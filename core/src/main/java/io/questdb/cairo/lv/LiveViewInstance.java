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
 *         {@code TableReader} machinery. A seam_ts in-memory tier for sub-FLUSH-cycle
 *         freshness is deferred to a later phase per RFC 123.</li>
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
    private final AtomicBoolean refreshLatch = new AtomicBoolean(false);
    private final LiveViewStateReader stateReader = new LiveViewStateReader();
    // Cached compiled factory. Window functions carry per-row state, so refresh must
    // reuse the same factory across calls. Accessed only while the refresh latch is held.
    // Compiled anchor expression — evaluated per row against records shaped by the
    // live view's projected metadata (i.e. records emitted by WalSegmentRecordCursor).
    // Lazily built on first refresh after the live view's main SELECT has been
    // compiled. Consumed by anchorWindow's per-row resetPartition dispatch.
    private Function anchorFunction;
    // Built once from anchorFunction + the compiled SELECT's window functions. Drives the
    // per-row resetPartition dispatch when the LV has an anchored named WINDOW.
    private LiveViewWindow anchorWindow;
    private RecordCursorFactory compiledFactory;
    private volatile boolean dropped;
    // Consecutive refresh-cycle failures since the last success. RFC 123 §"Flush"
    // budgets retries by both count (cairo.live.view.flush.retry.max) and elapsed
    // time (cairo.live.view.flush.retry.max.duration); on budget exhaustion the
    // refresh worker invalidates the view via the unified path. Mutated only on
    // the refresh-worker thread; not volatile because it isn't read elsewhere.
    private int flushRetryCount;
    // Wall-clock (micros) of the first failure in the current consecutive-failure
    // streak; Numbers.LONG_NULL when no streak is in progress. Same write-only
    // discipline as flushRetryCount.
    private long flushRetryStartUs = Numbers.LONG_NULL;
    // N=2 in-memory tier (RFC 123 §"In-memory tier"); lazily allocated on the
    // first refresh cycle after the LV's compiled factory + projected metadata
    // are known. Reads route through it via LiveViewRecordCursor (Phase 1b
    // Commit 4); the refresh worker drives the slow-path swap from
    // LiveViewRefreshJob. Null when no refresh has happened yet, or when the LV
    // was just constructed at startup.
    private volatile LiveViewInMemoryTier inMemoryTier;
    private volatile boolean isClosed;
    // Wall-clock (micros) of the most recent successful LV WAL commit. Used by
    // LiveViewRefreshJob to enforce FLUSH EVERY: a refresh that arrives within
    // flushEveryMicros of the previous commit is skipped, so high-rate base
    // ingestion produces batched commits at FLUSH EVERY cadence rather than one
    // commit per base notification.
    private volatile long lastFlushTimeUs = Numbers.LONG_NULL;
    // Last refresh-worker tick wall-clock (micros). Used by catalogue / lag metrics.
    private volatile long lastRefreshTimeUs = Numbers.LONG_NULL;
    // Live-view's own table token. Populated at construction.
    private final TableToken liveViewToken;
    // Cached RecordToRowCopier (compiled bytecode bridging the SELECT cursor's record
    // shape to the LV's WalWriter row). Invalidated when the WalWriter's metadata version
    // moves past recordRowCopierMetadataVersion. Accessed only while the refresh latch is held.
    private RecordToRowCopier recordToRowCopier;
    private long recordRowCopierMetadataVersion = -1;
    // Wall-clock (micros) when the in-mem tier's slow-path tryAcquireWrite first
    // observed both slots reader-pinned. Numbers.LONG_NULL when not stalled.
    // Cleared on the next successful acquire. Surfaces via
    // live_views().writer_stall_micros for operator visibility per RFC 123
    // §"Stall behavior".
    private volatile long writerStallStartUs = Numbers.LONG_NULL;

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
            anchorWindow = Misc.free(anchorWindow);
            anchorFunction = Misc.free(anchorFunction);
            inMemoryTier = Misc.free(inMemoryTier);
        }
    }

    public Function getAnchorFunction() {
        return anchorFunction;
    }

    public LiveViewWindow getAnchorWindow() {
        return anchorWindow;
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
        ObjList<String> deps = definition.getDependencyColumnNames();
        if (deps.size() == 0) {
            return false;
        }
        for (int i = 0, n = deps.size(); i < n; i++) {
            if (baseMetadata.getColumnIndexQuiet(deps.getQuick(i)) < 0) {
                return true;
            }
        }
        return false;
    }

    public ObjList<String> getDependencyColumnNames() {
        return definition.getDependencyColumnNames();
    }

    public int getFlushRetryCount() {
        return flushRetryCount;
    }

    public long getFlushRetryStartUs() {
        return flushRetryStartUs;
    }

    public LiveViewInMemoryTier getInMemoryTier() {
        return inMemoryTier;
    }

    public CharSequence getInvalidationReason() {
        return stateReader.getInvalidationReason();
    }

    public long getLastFlushTimeUs() {
        return lastFlushTimeUs;
    }

    public long getLastProcessedSeqTxn() {
        return stateReader.getLastProcessedSeqTxn();
    }

    public long getLastRefreshTimeUs() {
        return lastRefreshTimeUs;
    }

    public LiveViewLifecycleState getLifecycleState() {
        // A registered LiveViewInstance has, by definition, completed CREATE; the
        // CREATE-locked phase happens before registerView() is reached, and
        // registry-locked entries are in-memory only (no durable signal survives a
        // crash). So we never observe CREATING here and pass false for `locked` to
        // avoid conflating it with the unrelated refresh latch.
        return LiveViewLifecycleState.derive(
                !dropped && !isClosed,
                dropped,
                false,
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

    public long getWriterStallStartUs() {
        return writerStallStartUs;
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
        stateReader.setBackfillState(source.getBackfillState());
        stateReader.setBackfillTargetSeqTxn(source.getBackfillTargetSeqTxn());
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

    /**
     * Records a refresh-cycle failure. Increments the consecutive-failure counter
     * and stamps the start of the failure streak (used by the flush retry budget
     * in {@link io.questdb.cairo.lv.LiveViewRefreshJob}).
     */
    public void recordRefreshFailure(long nowUs) {
        if (flushRetryStartUs == Numbers.LONG_NULL) {
            flushRetryStartUs = nowUs;
        }
        flushRetryCount++;
    }

    /**
     * Resets the consecutive-failure counter and the streak start. Called after each
     * successful refresh cycle so the retry budget is per-streak, not lifetime.
     * <p>
     * Does <em>not</em> clear {@code writerStallStartUs}: stall is a property of
     * the in-mem tier's slot pinning, not of refresh-cycle success. A zero-row
     * cycle ({@code populateTier && appendedRows == 0}) skips
     * {@code publishToInMemoryTier} entirely, but if the slot remained pinned
     * by a long-running reader the stall is still happening; clearing here
     * would understate it. The clear lives on the populate-tier success
     * path in {@link io.questdb.cairo.lv.LiveViewRefreshJob#publishToInMemoryTier}
     * where we know the writer made tier progress (RFC 123 §"Stall behavior").
     */
    public void recordRefreshSuccess() {
        flushRetryCount = 0;
        flushRetryStartUs = Numbers.LONG_NULL;
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

    public void setAnchorWindow(LiveViewWindow window) {
        if (anchorWindow != window) {
            Misc.free(anchorWindow);
            anchorWindow = window;
        }
    }

    public void setCompiledFactory(RecordCursorFactory factory) {
        if (compiledFactory != factory) {
            Misc.free(compiledFactory);
            compiledFactory = factory;
        }
    }

    /**
     * Installs the in-memory tier. Single-shot — the tier is constructed once
     * on the first refresh cycle and lives for the LV's lifetime. Safe to call
     * with the existing tier passed back in (no-op); a different non-null tier
     * frees the old one first, mirroring {@link #setCompiledFactory}.
     */
    public void setInMemoryTier(LiveViewInMemoryTier tier) {
        if (inMemoryTier != tier) {
            Misc.free(inMemoryTier);
            inMemoryTier = tier;
        }
    }

    public void setLastFlushTimeUs(long lastFlushTimeUs) {
        this.lastFlushTimeUs = lastFlushTimeUs;
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

    public void setWriterStallStartUs(long writerStallStartUs) {
        this.writerStallStartUs = writerStallStartUs;
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
                anchorWindow = Misc.free(anchorWindow);
                anchorFunction = Misc.free(anchorFunction);
                inMemoryTier = Misc.free(inMemoryTier);
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
