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

import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.RecordToRowCopier;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Runtime representation of a Phase 1 live view.
 * <p>
 * Replaces the prototype's merge-buffer / cold-path state with the
 * disk-only RFC 123 Phase 1 surface:
 * <ul>
 *     <li>Lifecycle is derived from registry visibility + {@link #stateReader}.invalid;
 *         see {@link LiveViewLifecycleState}.</li>
 *     <li>Reads route through the LV's own WAL-backed table via the standard
 *         {@code TableReader} machinery (delta plan task #3). The
 *         {@link DoubleBufferedTable} / {@link InMemoryTable} primitives stay in
 *         the package as scaffolding for the eventual seam_ts in-memory tier per
 *         RFC 123, but Phase 1 does not pin or mutate any in-mem buffer here.</li>
 *     <li>{@link LiveViewStateReader} mirrors the durable contents of {@code _lv.s} —
 *         {@code invalid}, {@code subscribeFromSeqTxn}, {@code lastProcessedSeqTxn},
 *         {@code appliedWatermark}, {@code lvConsumedSeqTxn}. The instance exposes the
 *         reader; refresh / lifecycle code rewrites the file via
 *         {@link io.questdb.cairo.lv.LiveViewState#append}.</li>
 *     <li>{@code dependencyColumnIndexes} captures base-table writer indexes the
 *         compiled SELECT depends on. {@code ApplyWal2TableJob}'s schema-change hook
 *         consults this set to decide whether a base-table column change forces
 *         {@code markInvalid}. Populated at CREATE.</li>
 * </ul>
 * <p>
 * V1 omits BACKFILLING / per-window-state Maps / TableWriter ownership — those land
 * in later phases. The {@code WalWriter} for live-view-internal apply is acquired
 * from the engine's WAL writer pool per FLUSH cycle in V1 (Phase 1 keeps things
 * stateless on the instance side).
 */
public class LiveViewInstance implements QuietCloseable {
    private final LiveViewDefinition definition;
    // Base-column names the SELECT depends on (filter inputs + window inputs +
    // designated ts). Populated at CREATE and lazily after restart on first refresh.
    // Used by ApplyWal2TableJob's schema-change hook to narrow invalidation: only
    // changes touching one of these columns mark the view INVALID.
    private final ObjList<String> dependencyColumnNames = new ObjList<>();
    private final AtomicBoolean refreshLatch = new AtomicBoolean(false);
    private final LiveViewStateReader stateReader = new LiveViewStateReader();
    // Cached compiled factory. Window functions carry per-row state, so refresh must
    // reuse the same factory across calls. Accessed only while the refresh latch is held.
    // Compiled anchor expression — evaluated per row against records shaped by the
    // live view's projected metadata (i.e. records emitted by WalSegmentRecordCursor).
    // Lazily built on first refresh after the live view's main SELECT has been
    // compiled. Phase 1: stashed but not yet consumed by the runtime.
    private Function anchorFunction;
    private RecordCursorFactory compiledFactory;
    private volatile boolean dropped;
    private volatile boolean isClosed;
    // Last refresh-worker tick wall-clock (micros). Used by catalogue / lag metrics.
    private volatile long lastRefreshTimeUs = Numbers.LONG_NULL;
    // Live-view's own table token. Populated at construction.
    private final TableToken liveViewToken;
    // Cached RecordToRowCopier (compiled bytecode bridging the SELECT cursor's record
    // shape to the LV's WalWriter row). Invalidated when the WalWriter's metadata version
    // moves past recordRowCopierMetadataVersion. Accessed only while the refresh latch is held.
    private RecordToRowCopier recordToRowCopier;
    private long recordRowCopierMetadataVersion = -1;

    public LiveViewInstance(LiveViewDefinition definition, TableToken liveViewToken) {
        this.definition = definition;
        this.liveViewToken = liveViewToken;
    }

    @Override
    public void close() {
        // Shutdown path only — called from CairoEngine.close after all workers stopped.
        dropped = true;
        if (!isClosed) {
            isClosed = true;
            compiledFactory = Misc.free(compiledFactory);
            anchorFunction = Misc.free(anchorFunction);
        }
    }

    public Function getAnchorFunction() {
        return anchorFunction;
    }

    public RecordCursorFactory getCompiledFactory() {
        return compiledFactory;
    }

    public LiveViewDefinition getDefinition() {
        return definition;
    }

    /**
     * Returns {@code true} if any of this view's dependency columns is missing from
     * the post-change writer metadata — i.e. the column was dropped or renamed away.
     * Callers use this to decide whether a base-table schema change must invalidate
     * the view. An empty dependency set returns {@code false} (defensive: we don't
     * know what the view reads, so we leave invalidation to the broader path).
     */
    public boolean dependsOnMissingColumn(@NotNull RecordMetadata baseMetadata) {
        if (dependencyColumnNames.size() == 0) {
            return false;
        }
        for (int i = 0, n = dependencyColumnNames.size(); i < n; i++) {
            if (baseMetadata.getColumnIndexQuiet(dependencyColumnNames.getQuick(i)) < 0) {
                return true;
            }
        }
        return false;
    }

    public ObjList<String> getDependencyColumnNames() {
        return dependencyColumnNames;
    }

    public CharSequence getInvalidationReason() {
        return stateReader.getInvalidationReason();
    }

    public long getLastProcessedSeqTxn() {
        return stateReader.getLastProcessedSeqTxn();
    }

    public long getLastRefreshTimeUs() {
        return lastRefreshTimeUs;
    }

    public LiveViewLifecycleState getLifecycleState() {
        return LiveViewLifecycleState.derive(
                !dropped && !isClosed,
                dropped,
                refreshLatch.get(),
                stateReader.isInvalid()
        );
    }

    public TableToken getLiveViewToken() {
        return liveViewToken;
    }

    public long getRecordRowCopierMetadataVersion() {
        return recordRowCopierMetadataVersion;
    }

    public RecordToRowCopier getRecordToRowCopier() {
        return recordToRowCopier;
    }

    public LiveViewStateReader getStateReader() {
        return stateReader;
    }

    /**
     * Records the in-memory state-mirroring fields from a freshly-loaded {@code _lv.s}
     * snapshot. Called at startup after the file is read.
     */
    public void initFromState(@NotNull LiveViewStateReader source) {
        stateReader.setInvalid(source.isInvalid());
        stateReader.setInvalidationReason(source.getInvalidationReason());
        stateReader.setInvalidationTimestampUs(source.getInvalidationTimestampUs());
        stateReader.setSubscribeFromSeqTxn(source.getSubscribeFromSeqTxn());
        stateReader.setLastProcessedSeqTxn(source.getLastProcessedSeqTxn());
        stateReader.setAppliedWatermark(source.getAppliedWatermark());
        stateReader.setLvConsumedSeqTxn(source.getLvConsumedSeqTxn());
    }

    public boolean isDropped() {
        return dropped;
    }

    public boolean isInvalid() {
        return stateReader.isInvalid();
    }

    /**
     * Writes invalidation fields into the in-memory state mirror. The caller is responsible
     * for rewriting {@code _lv.s} via {@link io.questdb.cairo.lv.LiveViewState#append} —
     * this method only updates the in-memory side.
     */
    public void markInvalid(@Nullable CharSequence reason, long invalidationTimestampUs) {
        stateReader.setInvalid(true);
        stateReader.setInvalidationReason(reason);
        stateReader.setInvalidationTimestampUs(invalidationTimestampUs);
    }

    public void markAsDropped() {
        dropped = true;
    }

    public void setAppliedWatermark(long appliedWatermark) {
        stateReader.setAppliedWatermark(appliedWatermark);
    }

    public void setAnchorFunction(Function function) {
        if (anchorFunction != function) {
            Misc.free(anchorFunction);
            anchorFunction = function;
        }
    }

    public void setCompiledFactory(RecordCursorFactory factory) {
        if (compiledFactory != factory) {
            Misc.free(compiledFactory);
            compiledFactory = factory;
        }
    }

    public void setLastProcessedSeqTxn(long seqTxn) {
        stateReader.setLastProcessedSeqTxn(seqTxn);
    }

    public void setLastRefreshTimeUs(long lastRefreshTimeUs) {
        this.lastRefreshTimeUs = lastRefreshTimeUs;
    }

    public void setLvConsumedSeqTxn(long lvConsumedSeqTxn) {
        stateReader.setLvConsumedSeqTxn(lvConsumedSeqTxn);
    }

    public void setRecordToRowCopier(RecordToRowCopier copier, long metadataVersion) {
        this.recordToRowCopier = copier;
        this.recordRowCopierMetadataVersion = metadataVersion;
    }

    public void setSubscribeFromSeqTxn(long subscribeFromSeqTxn) {
        stateReader.setSubscribeFromSeqTxn(subscribeFromSeqTxn);
    }

    public void tryCloseIfDropped() {
        if (!dropped) {
            return;
        }
        if (!refreshLatch.compareAndSet(false, true)) {
            // Refresh in flight; its finally hook retries.
            return;
        }
        try {
            if (!isClosed) {
                isClosed = true;
                compiledFactory = Misc.free(compiledFactory);
                anchorFunction = Misc.free(anchorFunction);
            }
        } finally {
            refreshLatch.set(false);
        }
    }

    public boolean tryLockForRefresh() {
        return refreshLatch.compareAndSet(false, true);
    }

    public void unlockAfterRefresh() {
        if (!refreshLatch.compareAndSet(true, false)) {
            throw new IllegalStateException("refresh latch is not held");
        }
    }
}
