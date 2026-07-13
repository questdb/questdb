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
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.std.MemoryTracker;
import org.jetbrains.annotations.Nullable;

/**
 * Execution context used by {@link LiveViewRefreshJob} when compiling and running
 * the view's base SELECT during refresh. Pins the base table {@link TableReader}
 * for the duration of compile and cursor execution so SQL machinery's
 * {@code getReader} calls return a snapshot at a consistent transaction.
 */
public class LiveViewRefreshSqlExecutionContext extends SqlExecutionContextImpl {

    private TableReader baseTableReader;
    private LiveViewInstance refreshingInstance;

    public LiveViewRefreshSqlExecutionContext(CairoEngine engine, int sharedQueryWorkerCount) {
        super(engine, sharedQueryWorkerCount);
        this.securityContext = AllowAllSecurityContext.INSTANCE;
        this.bindVariableService = new BindVariableServiceImpl(engine.getConfiguration());
    }

    public void clearReader() {
        this.baseTableReader = null;
    }

    /**
     * Resolves to the tracker of the view being refreshed, so the anchored functions'
     * partition maps - which WindowRecordCursorFactory binds at cursor open - allocate
     * against the view that owns them. The lookup must be dynamic: the worker acquires the
     * tracker part-way through the cycle (when it builds the anchor window), so a value
     * snapshotted at cycle start would still be null and the maps would go untracked.
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
     * Binds the view whose refresh cycle is running, or null to clear it.
     */
    public void ofRefreshingInstance(@Nullable LiveViewInstance refreshingInstance) {
        this.refreshingInstance = refreshingInstance;
    }
}
