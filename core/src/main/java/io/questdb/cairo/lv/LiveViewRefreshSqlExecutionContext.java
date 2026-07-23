/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cairo.lv;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.std.MemoryTracker;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Execution context used by {@link LiveViewRefreshJob} when compiling and running
 * the view's base SELECT during refresh. Pins the base table {@link TableReader}
 * for the duration of compile and cursor execution so SQL machinery's
 * {@code getReader} calls return a snapshot at a consistent transaction.
 */
public class LiveViewRefreshSqlExecutionContext extends SqlExecutionContextImpl {
    // Bound while no view is being refreshed. AtomicBooleanCircuitBreaker reads a null
    // flag as "cancelled", so the unbound state needs a real flag that is never set
    // rather than none at all.
    private static final AtomicBoolean NEVER_CANCELLED = new AtomicBoolean(false);

    private TableReader baseTableReader;
    private LiveViewInstance refreshingInstance;

    public LiveViewRefreshSqlExecutionContext(CairoEngine engine, int sharedQueryWorkerCount) {
        super(engine, sharedQueryWorkerCount);
        this.securityContext = AllowAllSecurityContext.INSTANCE;
        this.bindVariableService = new BindVariableServiceImpl(engine.getConfiguration());
    }

    /**
     * Live-view refresh must be a deterministic function of (base table contents, view
     * definition) so every refresh cycle converges on the same result - and, under
     * symmetric replica refresh, so every node does. {@code CREATE LIVE VIEW} already
     * rejects {@code now()}/{@code sysdate()}/{@code systimestamp()}/{@code rnd_*}/etc.
     * ({@code CairoEngine.createLiveView} arms the same guard while compiling the body),
     * but {@link LiveViewRefreshJob} recompiles the view's SELECT on its own
     * ({@code ensureCompiledFactory}), a path the CREATE gate never runs - a restart, for
     * instance, rebuilds the factory straight from the persisted definition. Forcing the
     * guard off here makes {@code FunctionParser} reject a non-deterministic function on
     * that recompile too, as defense in depth, mirroring
     * {@code MatViewRefreshSqlExecutionContext}.
     */
    @Override
    public boolean allowNonDeterministicFunctions() {
        return false;
    }

    public void clearReader() {
        this.baseTableReader = null;
    }

    /**
     * The cancellable breaker rather than the no-op default, mirroring
     * {@code MatViewRefreshSqlExecutionContext}. Two things trip it: the flag
     * {@link #ofRefreshingInstance} binds, which DROP and invalidation set, and
     * {@code CairoEngine.isClosing()}, which {@code ServerMain} raises through
     * {@code signalClose()} while the refresh workers are still running.
     * <p>
     * It matters because the live-view refresh scans are serial pulls over a
     * {@code PageFrameRecordCursorFactory}, and that cursor consults no breaker of its
     * own. The unlocalized rebuild in particular has no turn budget - it recomputes the
     * whole view in one call - so before this, a shutdown or a DROP issued against a view
     * over a large base waited out the entire scan.
     */
    @Override
    public @NotNull SqlExecutionCircuitBreaker getCircuitBreaker() {
        return getSimpleCircuitBreaker();
    }

    /**
     * Resolves to the tracker of the view being refreshed, so the anchored functions'
     * partition maps and ring buffers - which WindowRecordCursorFactory binds at cursor open -
     * allocate against the view that owns them. The lookup must be dynamic: the worker acquires
     * the tracker part-way through the cycle (when it builds the anchor window), so a value
     * snapshotted at cycle start would still be null and the maps would go untracked.
     * <p>
     * This getter is also what charges the cycle's TRANSIENT buffers to the view:
     * AbstractPageFrameRecordCursor binds whatever tracker the execution context returns into
     * the frame memory pool, which reaches RowGroupBuffers, so the parquet decode buffers of
     * the view's compiled SELECT are charged here alongside the persistent partition state.
     * They are freed at cursor close, so the accounting stays symmetric across cycles, but it
     * makes cairo.live.view.refresh.memory.limit.bytes a cap on the cycle's PEAK rather than on
     * the state the view retains between cycles. That is deliberate - the property bounds a
     * refresh, not a residue - and the limit must be sized to cover the transients: a breach
     * invalidates the view outright. See CairoConfiguration.getLiveViewRefreshMemoryLimitBytes().
     */
    @Override
    public @Nullable MemoryTracker getMemoryTracker() {
        return refreshingInstance != null ? refreshingInstance.getMemoryTracker() : super.getMemoryTracker();
    }

    @Override
    public TableReader getReader(TableToken tableToken, long version) {
        if (baseTableReader != null && tableToken.equals(baseTableReader.getTableToken())) {
            // Enforce the same staleness check as CairoEngine.checkReaderVersion. The LV
            // keeps its compiled factory across refresh cycles, so after a base-table
            // schema change that does not touch referenced columns (which leaves the
            // view valid by design) the factory's page-frame column mapping no longer
            // matches the reader's column layout. Serving the mismatched reader would
            // make the cursor read the wrong columns with the wrong strides - garbage
            // values at best, an out-of-bounds mmap read (SIGSEGV) at worst. Throwing
            // routes the refresh into LiveViewRefreshJob's recompile-and-recover path.
            // Unlike checkReaderVersion, the pinned reader must NOT be closed here: it
            // is owned by the refresh method's own try/finally.
            if (version > -1 && baseTableReader.getMetadataVersion() != version) {
                throw TableReferenceOutOfDateException.of(
                        tableToken,
                        tableToken.getTableId(),
                        baseTableReader.getMetadata().getTableId(),
                        version,
                        baseTableReader.getMetadataVersion()
                );
            }
            return getCairoEngine().getReaderAtTxn(baseTableReader, this);
        }
        return super.getReader(tableToken, version);
    }

    @Override
    public TableReader getReader(TableToken tableToken) {
        if (baseTableReader != null && tableToken.equals(baseTableReader.getTableToken())) {
            return getCairoEngine().getReaderAtTxn(baseTableReader, this);
        }
        return super.getReader(tableToken);
    }

    public boolean hasReader() {
        return baseTableReader != null;
    }

    public void of(TableReader baseTableReader) {
        this.baseTableReader = baseTableReader;
    }

    /**
     * Binds the view whose refresh cycle is running, or null to clear it. Also hands the
     * breaker that view's cancellation flag, so DROP and invalidation reach a scan already
     * in flight, and starts a fresh throttle window so the next consultation performs a
     * real check rather than riding out the previous cycle's count.
     */
    public void ofRefreshingInstance(@Nullable LiveViewInstance refreshingInstance) {
        this.refreshingInstance = refreshingInstance;
        setCancelledFlag(refreshingInstance != null ? refreshingInstance.getRefreshCancelledFlag() : NEVER_CANCELLED);
        getCircuitBreaker().resetTimer();
    }
}
