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
import io.questdb.std.Os;
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
    private static final int HEAD_CHECKPOINT_LV_SEQ_TXN = 0;
    private static final int HEAD_CHECKPOINT_MAX_TS = 1;
    private static final int HEAD_CHECKPOINT_STATE_BYTES = 2;
    private static final long[] EMPTY_HEAD_CHECKPOINT = {Numbers.LONG_NULL, Numbers.LONG_NULL, 0L};
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
    // Snapshot-freeze gate. DatabaseCheckpointAgent sets this true before
    // copying an LV's file set and clears it afterwards; the refresh worker
    // observes the flag at the top of refreshInstance (after the latch +
    // dropped/invalid checks) and skips the cycle. The frozen lvSeqTxn at
    // the time of freeze is captured so post-restore consistency can be
    // asserted; the field is informational for tests and diagnostics.
    private volatile long freezeFrozenLvSeqTxn = Numbers.LONG_NULL;
    private volatile boolean freezeInProgress;
    // N=2 in-memory tier (RFC 123 §"In-memory tier"); lazily allocated on the
    // first refresh cycle after the LV's compiled factory + projected metadata
    // are known. Reads route through it via LiveViewRecordCursor (Phase 1b
    // Commit 4); the refresh worker drives the slow-path swap from
    // LiveViewRefreshJob. Null when no refresh has happened yet, or when the LV
    // was just constructed at startup.
    // Head-checkpoint metadata mirrored from the most recently committed
    // _checkpoints/<lvSeqTxn>.cp. Populated by the Phase 2a.4 flush-cycle
    // write hook (deferred) and consumed by the live_views() catalogue and
    // by the O3 head-hit / restart-restore decision paths.
    // <p>
    // The trio is packed into one immutable long[] published via volatile
    // store so the O3 head-hit lock-free reader always sees a consistent
    // (lvSeqTxn, maxTs, stateBytes) tuple; without the packing a reader
    // could observe a fresh lvSeqTxn paired with the prior maxTs.
    // Indexes: HEAD_CHECKPOINT_LV_SEQ_TXN / _MAX_TS / _STATE_BYTES.
    private volatile long[] headCheckpoint = EMPTY_HEAD_CHECKPOINT;
    private volatile LiveViewInMemoryTier inMemoryTier;
    private volatile boolean isClosed;
    // Restart-restore single-shot flag. The refresh worker flips it true on
    // the first cycle after CREATE / restart, regardless of whether a head
    // .cp was found - one attempt is the contract, no retries. Mutated only
    // under the refresh latch; volatile so the catalogue thread can read
    // the latest value without additional synchronisation.
    private volatile boolean checkpointRestoreAttempted;
    // Wall-clock (micros) of the most recent head-checkpoint commit. Numbers.LONG_NULL
    // until the first cycle that writes a .cp. The refresh worker compares
    // (nowUs - lastCheckpointWrittenUs) against
    // cairo.live.view.checkpoint.max.duration to decide whether the duration
    // trigger has fired this cycle. Mirrored as volatile because the catalogue
    // may surface it via live_views() later.
    private volatile long lastCheckpointWrittenUs = Numbers.LONG_NULL;
    // Wall-clock (micros) of the most recent successful LV WAL commit. Used by
    // LiveViewRefreshJob to enforce FLUSH EVERY: a refresh that arrives within
    // flushEveryMicros of the previous commit is skipped, so high-rate base
    // ingestion produces batched commits at FLUSH EVERY cadence rather than one
    // commit per base notification.
    private volatile long lastFlushTimeUs = Numbers.LONG_NULL;
    // Last refresh-worker tick wall-clock (micros). Used by catalogue / lag metrics.
    private volatile long lastRefreshTimeUs = Numbers.LONG_NULL;
    // Maximum base-row timestamp the refresh worker has observed so far, across
    // every cycle since startup or the last restore. Updated row-by-row by the
    // anchor-dispatch cursor. The refresh worker compares each incoming WAL
    // commit's min ts against this to detect out-of-order arrivals: a row with
    // ts strictly less than latestSeenTs means the commit needs the O3 replay
    // path instead of the append-only steady-state path.
    // <p>
    // Reset to {@link Numbers#LONG_NULL} on construction (a fresh LV has seen
    // no rows yet). On restart-restore, the value re-derives naturally from the
    // first post-restore commit; we deliberately don't persist this in
    // {@code _lv.s} because (a) the head .cp's maxTimestamp already plays the
    // gating role for O3 head-hit and (b) trailing this in {@code _lv.s} would
    // add a write per commit.
    private volatile long latestSeenTs = Numbers.LONG_NULL;
    // Live-view's own table token. Populated at construction.
    private final TableToken liveViewToken;
    // Reason string the refresh worker stashes here when a head-restore step
    // surfaces a "version too old" function snapshot. The worker holds the
    // refresh latch when populating this field; the same worker drains it
    // (consumes and clears) after releasing the latch and runs
    // engine.invalidateLiveView. The two-step is to avoid a deadlock: the
    // invalidate path parks on the instance monitor when a checkpoint freeze
    // is active, and the agent's startCheckpoint cannot complete its latch
    // handshake while the worker holds the refresh latch.
    private String pendingInvalidationReason;
    // Cached RecordToRowCopier (compiled bytecode bridging the SELECT cursor's record
    // shape to the LV's WalWriter row). Invalidated when the WalWriter's metadata version
    // moves past recordRowCopierMetadataVersion. Accessed only while the refresh latch is held.
    private RecordToRowCopier recordToRowCopier;
    private long recordRowCopierMetadataVersion = -1;
    // Live-view-row count applied since the most recent head-checkpoint commit.
    // The refresh worker compares this against cairo.live.view.checkpoint.rows
    // each cycle to decide whether the row-count trigger has fired. Mutated
    // only on the refresh-worker thread under the refresh latch; not volatile
    // because no other thread reads it.
    private long rowsSinceLastCheckpointWritten;
    // AND of every compiled window function's WindowFunction.supportsSnapshot().
    // Computed once on the first refresh cycle after the LV's compiled factory
    // is ready, then cached. False means the flush cycle emits no checkpoints
    // (every restart / O3 falls back to the head-miss replay path); the LV's
    // live_views().head_checkpoint_lv_seqtxn stays LONG_NULL for its lifetime.
    private volatile boolean snapshotCapability;
    private volatile boolean snapshotCapabilityComputed;
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

    /**
     * Accumulates {@code n} into {@link #rowsSinceLastCheckpointWritten}. Called
     * from the refresh worker after each successful LV WAL apply commit; the
     * counter resets when {@link #setHeadCheckpoint(long, long, long, long)}
     * stamps a fresh head.
     */
    public void addRowsSinceLastCheckpointWritten(long n) {
        rowsSinceLastCheckpointWritten += n;
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

    /**
     * Companion to {@link #startCheckpoint(long)}. Clears the freeze gate so
     * the refresh worker resumes on its next turn and wakes any thread blocked
     * in {@link #waitForUnfrozen()}. Idempotent.
     */
    public void endCheckpoint() {
        synchronized (this) {
            freezeInProgress = false;
            freezeFrozenLvSeqTxn = Numbers.LONG_NULL;
            notifyAll();
        }
    }

    /**
     * Non-monotonic restore of {@link #getLatestSeenTs()} used by the refresh
     * worker after an O3 detect + WAL rollback to revert any in-cycle bumps
     * the discarded rows applied. The snapshot must come from the cycle's
     * entry point. Bypassing the monotonic clamp is intentional and unsafe
     * in any other context, hence the explicit name.
     */
    public void forceSetLatestSeenTs(long ts) {
        latestSeenTs = ts;
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

    /**
     * @return the lvSeqTxn captured at {@link #startCheckpoint(long)}, or
     * {@link Numbers#LONG_NULL} when no freeze is in progress. Useful for tests
     * and post-restore consistency assertions.
     */
    public long getFreezeFrozenLvSeqTxn() {
        return freezeFrozenLvSeqTxn;
    }

    public long getHeadCheckpointLvSeqTxn() {
        return headCheckpoint[HEAD_CHECKPOINT_LV_SEQ_TXN];
    }

    public long getHeadCheckpointMaxTs() {
        return headCheckpoint[HEAD_CHECKPOINT_MAX_TS];
    }

    /**
     * Atomic read of the {@code (lvSeqTxn, maxTs)} pair the O3 head-hit
     * eligibility check needs. Returns a stable two-element array
     * {@code [lvSeqTxn, maxTs]} so callers cannot observe a torn pair across
     * a concurrent {@link #setHeadCheckpoint(long, long, long, long)}.
     */
    public long[] getHeadCheckpointSeqAndMaxTs() {
        final long[] local = headCheckpoint;
        return new long[]{local[HEAD_CHECKPOINT_LV_SEQ_TXN], local[HEAD_CHECKPOINT_MAX_TS]};
    }

    public long getHeadCheckpointStateBytes() {
        return headCheckpoint[HEAD_CHECKPOINT_STATE_BYTES];
    }

    public LiveViewInMemoryTier getInMemoryTier() {
        return inMemoryTier;
    }

    public CharSequence getInvalidationReason() {
        return stateReader.getInvalidationReason();
    }

    public long getLastCheckpointWrittenUs() {
        return lastCheckpointWrittenUs;
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

    /**
     * @return the highest base-row timestamp the refresh worker has fed
     * through the window pipeline since startup, or {@link Numbers#LONG_NULL}
     * if no row has been processed yet. The O3 detection path reads this to
     * decide whether an incoming commit is in-order.
     */
    public long getLatestSeenTs() {
        return latestSeenTs;
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

    public long getRowsSinceLastCheckpointWritten() {
        return rowsSinceLastCheckpointWritten;
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

    /**
     * @return true while a snapshot freeze is active for this view. Callers
     * that mutate {@code _lv.s} or advance any LV watermark MUST honour
     * this flag and back off until {@link #endCheckpoint()} clears it. The
     * refresh worker observes it at the top of its turn.
     */
    public boolean isFreezeInProgress() {
        return freezeInProgress;
    }

    /**
     * @return {@code true} once the refresh worker has attempted a head
     * checkpoint restore for this LV (whether the restore succeeded, found
     * no head, or failed on a corrupt file). Single-shot per LV lifetime.
     */
    public boolean isCheckpointRestoreAttempted() {
        return checkpointRestoreAttempted;
    }

    public boolean isInvalid() {
        return stateReader.isInvalid();
    }

    /**
     * @return the cached AND of every compiled window function's
     * {@code supportsSnapshot()}. Meaningful only when
     * {@link #isSnapshotCapabilityComputed()} returns {@code true}.
     */
    public boolean isSnapshotCapability() {
        return snapshotCapability;
    }

    /**
     * @return {@code true} once the first refresh cycle has evaluated
     * {@link #isSnapshotCapability()} from the compiled SELECT's window
     * functions. The refresh worker computes the AND exactly once per LV
     * lifetime, then routes every subsequent cycle through the cached value.
     */
    public boolean isSnapshotCapabilityComputed() {
        return snapshotCapabilityComputed;
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

    /**
     * Single-shot setter for {@link #isCheckpointRestoreAttempted()}.
     * The refresh worker calls this on the first cycle after the LV's
     * compiled factory becomes available, regardless of the restore
     * outcome.
     */
    public void setCheckpointRestoreAttempted() {
        this.checkpointRestoreAttempted = true;
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
    /**
     * Records a committed head checkpoint in one atomic store. Mirrors the
     * head metadata into the {@code live_views()} catalogue, resets the
     * cadence counter ({@link #rowsSinceLastCheckpointWritten} back to zero),
     * and stamps {@link #lastCheckpointWrittenUs}. Called by the flush-cycle
     * write hook after the {@code <lvSeqTxn>.cp.tmp} -> {@code <lvSeqTxn>.cp}
     * rename succeeds.
     * <p>
     * Passing {@code Numbers.LONG_NULL} for {@code lvSeqTxn} clears the head
     * (e.g. when the {@code .cp} is unlinked by a recovery sweep); cadence
     * counters reset too so the next eligible cycle writes a fresh head
     * immediately rather than waiting for the row-count trigger to re-fire.
     */
    public void setHeadCheckpoint(long lvSeqTxn, long maxTs, long stateBytes, long writtenUs) {
        // Publish the (lvSeqTxn, maxTs, stateBytes) trio atomically: build a
        // fresh immutable array and store the reference volatile. A reader
        // observing the new reference is guaranteed to see all three fields
        // from the same setHeadCheckpoint call, never a torn mix.
        this.headCheckpoint = new long[]{lvSeqTxn, maxTs, stateBytes};
        this.rowsSinceLastCheckpointWritten = 0;
        this.lastCheckpointWrittenUs = writtenUs;
    }

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

    /**
     * Monotonic update of {@link #getLatestSeenTs()}. Skips the store if
     * {@code ts <= latestSeenTs} so an O3 row (the very thing we want to
     * detect) does not retroactively lower the watermark. Called from the
     * anchor-dispatch cursor on every base row consumed by the refresh
     * worker; the only writer is the refresh-worker thread, so the read +
     * compare + write needs no extra synchronisation beyond the field's
     * own volatility for the catalogue / detection reader.
     */
    public void setLatestSeenTs(long ts) {
        if (ts > latestSeenTs) {
            latestSeenTs = ts;
        }
    }

    public void setLvConsumedSeqTxn(long lvConsumedSeqTxn) {
        stateReader.setLvConsumedSeqTxn(lvConsumedSeqTxn);
    }

    /**
     * Refresh-worker stash for a deferred invalidate (currently used by the
     * head-checkpoint restore path on a version-too-old function snapshot).
     * Worker calls this while holding the refresh latch; the caller of
     * {@code refreshInstance} drains via {@link #takePendingInvalidationReason}
     * after the latch is released and runs the engine-side invalidate.
     */
    public void setPendingInvalidationReason(String reason) {
        this.pendingInvalidationReason = reason;
    }

    public void setRecordToRowCopier(RecordToRowCopier copier, long metadataVersion) {
        this.recordToRowCopier = copier;
        this.recordRowCopierMetadataVersion = metadataVersion;
    }

    /**
     * Caches the AND of every compiled window function's
     * {@code supportsSnapshot()}, evaluated once after the LV's compiled
     * factory becomes available. Subsequent refresh cycles short-circuit on
     * {@link #isSnapshotCapabilityComputed()} and read the cached
     * {@link #isSnapshotCapability()} value. Setting writes both fields in
     * the right order so a concurrent reader on the catalogue thread never
     * observes {@code computed=true} with the default {@code capability=false}.
     */
    public void setSnapshotCapability(boolean value) {
        this.snapshotCapability = value;
        this.snapshotCapabilityComputed = true;
    }

    public void setSubscribeFromSeqTxn(long subscribeFromSeqTxn) {
        stateReader.setSubscribeFromSeqTxn(subscribeFromSeqTxn);
    }

    public void setWriterStallStartUs(long writerStallStartUs) {
        this.writerStallStartUs = writerStallStartUs;
    }

    /**
     * Marks the view frozen for the duration of a {@code DatabaseCheckpointAgent}
     * file copy. {@code frozenLvSeqTxn} is the {@code appliedWatermark} at the
     * time of freeze; recorded for diagnostics. Refresh-worker turns that
     * observe {@link #isFreezeInProgress()} short-circuit before mutating
     * {@code _lv.s} or advancing any LV watermark. The caller is responsible
     * for pairing this with a {@link #endCheckpoint()} after the copy completes.
     * <p>
     * After setting the flag the call takes and releases the refresh latch.
     * The CAS spins until any in-flight refresh turn releases the latch in
     * its finally block; this forces happens-before with the worker so that
     * (a) no refresh turn is still mutating {@code _lv.s} when the caller
     * proceeds with its copy, and (b) the worker's next call to
     * {@link #tryLockForRefresh()} observes {@code freezeInProgress=true}.
     */
    public void startCheckpoint(long frozenLvSeqTxn) {
        // Synchronize on the instance monitor while publishing the flag so any
        // invalidator inside synchronized(instance) on another thread either
        // (a) commits its rewrite before the agent's file copy begins, or
        // (b) observes freezeInProgress=true and parks via waitForUnfrozen().
        synchronized (this) {
            freezeFrozenLvSeqTxn = frozenLvSeqTxn;
            freezeInProgress = true;
        }
        while (!refreshLatch.compareAndSet(false, true)) {
            Os.pause();
        }
        refreshLatch.set(false);
    }

    public String takePendingInvalidationReason() {
        String reason = this.pendingInvalidationReason;
        this.pendingInvalidationReason = null;
        return reason;
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

    /**
     * Parks the calling thread on the instance monitor while
     * {@link #isFreezeInProgress()} is true. Must be invoked from within a
     * {@code synchronized(instance)} block; releases the monitor while waiting
     * and reacquires it before returning. {@link #endCheckpoint()} wakes
     * waiters once the freeze clears.
     * <p>
     * Out-of-band {@code _lv.s} mutators (engine-side invalidation paths) call
     * this at the top of their synchronized block so the snapshot agent's
     * file copy is not racing concurrent rewrites. The caller does not need
     * to recheck the flag after the call returns; the synchronized block
     * holds the monitor, so any subsequent {@code startCheckpoint} that
     * synchronizes on the same monitor will block until this work completes.
     */
    public void waitForUnfrozen() {
        assert Thread.holdsLock(this);
        while (freezeInProgress) {
            try {
                wait();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}
