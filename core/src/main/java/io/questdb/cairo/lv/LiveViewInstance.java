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
 * Runtime representation of a live view.
 * <p>
 * Replaces the prototype's merge-buffer / cold-path state with the disk-backed
 * surface:
 * <ul>
 *     <li>Lifecycle is derived from registry visibility + {@link #stateReader}.invalid;
 *         see {@link LiveViewLifecycleState}.</li>
 *     <li>Reads route through the LV's own WAL-backed table via the standard
 *         {@code TableReader} machinery. A seam_ts in-memory tier for sub-FLUSH-cycle
 *         freshness is deferred to a later phase.</li>
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
 * The {@code WalWriter} for live-view-internal apply is acquired from the engine's
 * WAL writer pool per FLUSH cycle rather than being owned by the instance.
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
    // In-memory count of base data-cursor rows the backfill sweep has consumed
    // so far - the skipRows() resume position for the next turn. Persists in
    // memory across in-process turns (window state persists with it), and is
    // re-seeded from the surviving .bcp on the first turn after a restart.
    // Numbers.LONG_NULL until the first backfill turn initialises it; 0 means
    // "swept nothing yet". Mutated under the refresh latch only.
    private long backfillDataOffset = Numbers.LONG_NULL;
    // Single-shot flag: the first backfill turn of the process restores window
    // state + data offset from the surviving .bcp (if any), then later turns
    // continue from the in-memory state. Mirrors checkpointRestoreAttempted for
    // the backfill path. Mutated under the refresh latch only.
    private boolean backfillResumeAttempted;
    // Skip-write floor for the backfill sweep: the LV table's on-disk row count
    // captured on the first turn of the process. Output rows whose position is
    // below it are already durable (deterministic recompute), so the sweep
    // recomputes them to advance window state but skips the WAL append. Spans
    // however many turns the catch-up needs; persists across turns (the per-turn
    // budget can split the catch-up). Mutated under the refresh latch only.
    private long backfillSkipWriteFloor;
    private RecordCursorFactory compiledFactory;
    // Cumulative count of coupled dedup-base refresh cycles that proved the base range
    // clean and took the cheap raw-WAL append path.
    // In-memory observability, reset to 0 on restart; bumped only on the refresh worker.
    // Volatile so a reader off the worker thread sees a current value.
    private volatile long dedupRawWalCleanCycles;
    private volatile boolean dropped;
    // Consecutive refresh-cycle failures since the last success. The flush retry
    // budget caps retries by both count (cairo.live.view.flush.retry.max) and elapsed
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
    // N=2 in-memory tier; lazily allocated on the
    // first refresh cycle after the LV's compiled factory + projected metadata
    // are known. Reads route through it via LiveViewRecordCursor; the refresh
    // worker drives the slow-path swap from
    // LiveViewRefreshJob. Null when no refresh has happened yet, or when the LV
    // was just constructed at startup.
    // Head-checkpoint metadata mirrored from the most recently committed
    // _checkpoints/<lvSeqTxn>.cp. Populated by the flush-cycle
    // write hook (deferred) and consumed by the live_views() catalogue and
    // by the O3 head-hit / restart-restore decision paths.
    // <p>
    // The trio is packed into one immutable long[] published via volatile
    // store so the O3 head-hit lock-free reader always sees a consistent
    // (lvSeqTxn, maxTs, stateBytes) tuple; without the packing a reader
    // could observe a fresh lvSeqTxn paired with the prior maxTs.
    // Indexes: HEAD_CHECKPOINT_LV_SEQ_TXN / _MAX_TS / _STATE_BYTES.
    private volatile long[] headCheckpoint = EMPTY_HEAD_CHECKPOINT;
    // Key (data-cursor row offset) of the current rolling backfill checkpoint
    // _checkpoints/<key>.bcp, or Numbers.LONG_NULL when none exists. Stamped by
    // the startup recovery sweep for a view loaded in BACKFILLING state, updated
    // each backfill turn after a fresh .bcp is written, and cleared when the
    // sweep completes. Volatile so the catalogue thread can read it; mutated
    // under the refresh latch.
    private volatile long headBackfillCpKey = Numbers.LONG_NULL;
    private volatile LiveViewInMemoryTier inMemoryTier;
    private volatile boolean isClosed;
    // Restart-restore single-shot flag. The refresh worker flips it true on
    // the first cycle after CREATE / restart, regardless of whether a head
    // .cp was found - one attempt is the contract, no retries. Mutated only
    // under the refresh latch; volatile so the catalogue thread can read
    // the latest value without additional synchronisation.
    private volatile boolean checkpointRestoreAttempted;
    // Set true only when a head .cp restore actually rehydrated the window state
    // (restoreFromHead returned true). Stays false when no head existed or the
    // restore failed and fell back to a head-miss replay. Distinguishes a real
    // restore from the replay fallback for observability and tests. Mutated only
    // under the refresh latch; volatile for the catalogue thread.
    private volatile boolean checkpointRestoreSucceeded;
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
    // Lead eligibility (cached, schema-derived). True when every output column is a
    // type the in-mem tier can store (see LiveViewInMemoryBuffer.isColumnTypeSupported:
    // fixed-width, SYMBOL via eager interning, and the variable-length STRING / BINARY /
    // VARCHAR / ARRAY types) and the output carries a designated timestamp, so the tier
    // can hold an un-flushed lead the refresh worker serves ahead of disk. False keeps
    // the tier a strict subset of disk - an output column of a type the tier does not
    // store (a non-persisted type such as INTERVAL). Computed once on the first refresh cycle after the
    // compiled factory is ready, then cached. Volatile so the catalogue thread can read
    // it without extra synchronisation; mutated only under the refresh latch.
    private volatile boolean leadEligible;
    private volatile boolean leadEligibilityComputed;
    // In-RAM lead row count: the number of output rows refreshed into the in-mem
    // tier but not yet flushed to the LV's on-disk table. Grows with each refresh
    // tick, reset to 0 at flush. Stamped onto the published slot so reads can serve
    // the lead on top of disk (size() = disk.size() + leadRowCount). In-RAM only
    // (recovered by replaying base WAL forward on restart); mutated under the
    // refresh latch only, like the other refresh-worker-only fields.
    private long leadRowCount;
    // Live-view's own table token. Populated at construction.
    private final TableToken liveViewToken;
    // Cumulative count of live-view rows produced over the LV's lifetime,
    // matching the MANIFEST.lvRowPosition field on every head checkpoint.
    // Initialised to 0 on construction. tryRestoreFromHead and the O3 head-hit
    // replay path stamp this from the manifest after a successful restore;
    // every subsequent {@link #addRowsSinceLastCheckpointWritten(long)} bumps
    // both this and the cadence counter so writes and restores stay aligned.
    // Mutated under the refresh latch only.
    private long lvRowsTotal;
    // Cumulative count of late O3 rows rejected because their timestamp fell below
    // viewLowerBoundTimestamp. Surfaced via live_views().o3_rejected_count. Bumped
    // only on the refresh-worker thread at the O3-detection step; volatile so the
    // catalogue query thread reads a current value. In-memory only - it resets to
    // 0 on restart (an observability signal, not durable state). Rows can only land
    // below the bound via the O3 path: in-WAL order guarantees
    // ts >= latestSeenTs >= viewLowerBoundTimestamp.
    private volatile long o3RejectedCount;
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
    // In-RAM refresh cursor: the highest base seqTxn whose rows have been refreshed
    // into the in-mem tier (the lead), which leads the flushed/applied point
    // ({@link #getLastProcessedSeqTxn()}) by the un-flushed lead. The refresh worker
    // drains base WAL from this point each tick; flush advances the applied point up
    // to it. In-RAM only (not persisted): on restart it re-initialises to the applied
    // point and drain-forward rebuilds the lead. LONG_NULL means "not initialised
    // yet" -> {@link #getRefreshedUpToSeqTxn()} falls back to the applied point.
    // Mutated under the refresh latch only.
    private long refreshedUpToSeqTxn = Numbers.LONG_NULL;
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
    // Set true when an O3 in-mem tier rebuild is skipped because both slots were
    // reader-pinned: the published slot then keeps its pre-O3 rows, which the O3
    // replay has since re-sequenced on disk. The flag forces the next normal
    // publish (LiveViewRefreshJob.publishToInMemoryTier) to drop the retained
    // rows instead of copying / appending onto them, so a read never serves the
    // stale pre-O3 rows re-stamped with a matching seqTxn. Cleared on any
    // successful publish / rebuild. Touched only on the refresh-worker thread
    // (rebuild + publish both run under the refresh latch), so a plain field
    // suffices - the latch's acquire/release supplies the happens-before edge,
    // same discipline as flushRetryCount.
    private boolean tierStale;
    // True only for a minimal stub registered by the catalogue load path when the
    // on-disk _lv / _lv.s carry an unsupported newer format version. Such an
    // instance has a null definition and default runtime state; getLifecycleState
    // reports VERSION_UNSUPPORTED and the catalogue surfaces only view_name /
    // view_status. Final, so it is safely published to the catalogue read thread.
    private final boolean versionUnsupported;
    // Wall-clock (micros) when the in-mem tier's slow-path tryAcquireWrite first
    // observed both slots reader-pinned. Numbers.LONG_NULL when not stalled.
    // Cleared on the next successful acquire. Surfaces via
    // live_views().writer_stall_micros for operator visibility.
    private volatile long writerStallStartUs = Numbers.LONG_NULL;

    public LiveViewInstance(LiveViewDefinition definition, TableToken liveViewToken) {
        this.definition = definition;
        this.liveViewToken = liveViewToken;
        this.versionUnsupported = false;
    }

    /**
     * Builds a minimal stub for a live view whose on-disk files carry an
     * unsupported newer format version. Carries only the token; the definition is
     * null and the runtime state stays at defaults. The catalogue surfaces it with
     * {@code view_status='version_unsupported'}; the refresh worker never runs
     * against it, and DROP LIVE VIEW removes it best-effort.
     */
    public LiveViewInstance(TableToken liveViewToken) {
        this.definition = null;
        this.liveViewToken = liveViewToken;
        this.versionUnsupported = true;
    }

    /**
     * Accumulates {@code n} into both {@link #rowsSinceLastCheckpointWritten}
     * (the cadence counter, which resets on each fresh head via
     * {@link #setHeadCheckpoint(long, long, long, long)}) and
     * {@link #lvRowsTotal} (the lifetime counter, which mirrors
     * {@code MANIFEST.lvRowPosition} and persists across restarts). Called
     * from the refresh worker after each successful LV WAL apply commit.
     */
    public void addRowsSinceLastCheckpointWritten(long n) {
        rowsSinceLastCheckpointWritten += n;
        lvRowsTotal += n;
    }

    /**
     * Increments the count of coupled dedup-base refresh cycles that proved the base
     * range clean (raw WAL == applied base) and took the cheap raw-WAL append path
     * instead of the applied-reader path. Bumped
     * only on the refresh-worker thread; in-memory observability that resets on restart.
     */
    public void bumpDedupRawWalCleanCycles() {
        dedupRawWalCleanCycles++;
    }

    /**
     * Accumulates {@code n} late O3 rows rejected for falling below
     * {@code viewLowerBoundTimestamp}. Called from the refresh worker at the
     * O3-detection step; the value is exposed via {@code live_views().o3_rejected_count}.
     */
    public void bumpO3RejectedCount(long n) {
        o3RejectedCount += n;
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

    public long getBackfillDataOffset() {
        return backfillDataOffset;
    }

    public long getBackfillSkipWriteFloor() {
        return backfillSkipWriteFloor;
    }

    public RecordCursorFactory getCompiledFactory() {
        return compiledFactory;
    }

    public long getDedupRawWalCleanCycles() {
        return dedupRawWalCleanCycles;
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

    /**
     * @return the persisted applied watermark (base seqTxn): the on-disk LV table
     * holds every base commit up to it. Delegates to {@link #stateReader}.
     */
    public long getAppliedWatermark() {
        return stateReader.getAppliedWatermark();
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

    public long getHeadBackfillCpKey() {
        return headBackfillCpKey;
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
     * @return the in-RAM lead row count (output rows refreshed into the tier but
     * not yet flushed to disk). See {@link #leadRowCount}.
     */
    public long getLeadRowCount() {
        return leadRowCount;
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
        if (versionUnsupported) {
            // Stub for an unloadable newer-schema view: its durable signals were
            // never read, so report the terminal state directly.
            return LiveViewLifecycleState.VERSION_UNSUPPORTED;
        }
        // A registered LiveViewInstance has, by definition, completed CREATE,
        // so CREATING is unreachable here. close() always flips `dropped` before
        // `isClosed`, so `!dropped && !isClosed` collapses to "not yet dropped"
        // and feeds the registryVisible signal.
        return LiveViewLifecycleState.derive(
                !dropped && !isClosed,
                stateReader.isInvalid(),
                stateReader.getBackfillState() == LiveViewState.BACKFILL_STATE_BACKFILLING
        );
    }

    public TableToken getLiveViewToken() {
        return liveViewToken;
    }

    /**
     * @return cumulative LV row count, matching the value persisted as
     * {@code MANIFEST.lvRowPosition} on the most recent head checkpoint.
     */
    public long getLvRowsTotal() {
        return lvRowsTotal;
    }

    public long getO3RejectedCount() {
        return o3RejectedCount;
    }

    public long getRecordRowCopierMetadataVersion() {
        return recordRowCopierMetadataVersion;
    }

    public RecordToRowCopier getRecordToRowCopier() {
        return recordToRowCopier;
    }

    /**
     * @return the in-RAM refresh cursor (highest base seqTxn drained into the tier
     * as lead). Falls back to {@link #getLastProcessedSeqTxn()} (the applied point)
     * when not yet initialised, so a fresh / restarted instance resumes refresh
     * from where disk left off. See {@link #refreshedUpToSeqTxn}.
     */
    public long getRefreshedUpToSeqTxn() {
        return refreshedUpToSeqTxn == Numbers.LONG_NULL ? getLastProcessedSeqTxn() : refreshedUpToSeqTxn;
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
     * @return {@code true} once the refresh worker has attempted to resume the
     * backfill sweep from the surviving {@code .bcp} on the first turn of this
     * process (whether a checkpoint was found or not). Single-shot per process;
     * later turns continue from the in-memory window state + data offset.
     */
    public boolean isBackfillResumeAttempted() {
        return backfillResumeAttempted;
    }

    /**
     * @return {@code true} once the refresh worker has attempted a head
     * checkpoint restore for this LV (whether the restore succeeded, found
     * no head, or failed on a corrupt file). Single-shot per LV lifetime.
     */
    public boolean isCheckpointRestoreAttempted() {
        return checkpointRestoreAttempted;
    }

    /**
     * @return {@code true} once a head-checkpoint restore for this LV actually
     * rehydrated the window state. Remains {@code false} when no head existed
     * or the restore failed and the LV fell back to a head-miss replay.
     */
    public boolean isCheckpointRestoreSucceeded() {
        return checkpointRestoreSucceeded;
    }

    public boolean isInvalid() {
        return stateReader.isInvalid();
    }

    /**
     * @return the cached lead eligibility (every output column is a type the in-mem
     * tier can store - fixed-width, SYMBOL, STRING, BINARY, VARCHAR or ARRAY - so the
     * tier may serve an un-flushed lead ahead of disk). Meaningful only when
     * {@link #isLeadEligibilityComputed()} returns {@code true}. See
     * {@link #leadEligible}.
     */
    public boolean isLeadEligible() {
        return leadEligible;
    }

    /**
     * @return {@code true} once the first refresh cycle has evaluated
     * {@link #isLeadEligible()} from the compiled SELECT's output schema. The
     * refresh worker computes it once per LV lifetime, then caches it.
     */
    public boolean isLeadEligibilityComputed() {
        return leadEligibilityComputed;
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
     * @return {@code true} when the published in-mem slot may still hold pre-O3
     * rows from a both-slots-pinned rebuild skip. While set, the next normal
     * publish drops the retained rows rather than carrying them forward. See
     * {@link #setTierStale(boolean)}.
     */
    public boolean isTierStale() {
        return tierStale;
    }

    /**
     * @return {@code true} for a minimal stub registered when the on-disk files
     * carry an unsupported newer format version (see the stub constructor).
     */
    public boolean isVersionUnsupported() {
        return versionUnsupported;
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
     * Records a written rolling backfill checkpoint: stamps the {@code .bcp}
     * key and the checkpoint write time. The backfill cadence keys off the
     * {@code .bcp} data offset delta (not {@link #rowsSinceLastCheckpointWritten},
     * which the steady head owns), so this does not touch the steady head
     * metadata or the steady cadence counter.
     */
    public void recordBackfillCheckpointWritten(long key, long writtenUs) {
        this.headBackfillCpKey = key;
        this.lastCheckpointWrittenUs = writtenUs;
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
     * where we know the writer made tier progress.
     */
    public void recordRefreshSuccess() {
        flushRetryCount = 0;
        flushRetryStartUs = Numbers.LONG_NULL;
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

    public void setAppliedWatermark(long appliedWatermark) {
        stateReader.setAppliedWatermark(appliedWatermark);
    }

    public void setBackfillDataOffset(long backfillDataOffset) {
        this.backfillDataOffset = backfillDataOffset;
    }

    public void setBackfillSkipWriteFloor(long backfillSkipWriteFloor) {
        this.backfillSkipWriteFloor = backfillSkipWriteFloor;
    }

    /**
     * Single-shot setter for {@link #isBackfillResumeAttempted()}. The refresh
     * worker calls this on the first backfill turn of the process, regardless
     * of whether a {@code .bcp} was found to resume from.
     */
    public void setBackfillResumeAttempted() {
        this.backfillResumeAttempted = true;
    }

    public void setBackfillState(byte backfillState) {
        stateReader.setBackfillState(backfillState);
    }

    public void setBackfillTargetSeqTxn(long backfillTargetSeqTxn) {
        stateReader.setBackfillTargetSeqTxn(backfillTargetSeqTxn);
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

    /**
     * Single-shot setter for {@link #isCheckpointRestoreSucceeded()}. The
     * refresh worker calls this only when {@code restoreFromHead} returned
     * true (the window state was rehydrated from the head .cp).
     */
    public void setCheckpointRestoreSucceeded() {
        this.checkpointRestoreSucceeded = true;
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
    public void setHeadBackfillCpKey(long key) {
        this.headBackfillCpKey = key;
    }

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

    /**
     * Caches the lead eligibility, evaluated once after the LV's compiled factory
     * becomes available. Writes the value before flipping the computed flag so a
     * concurrent catalogue reader never observes {@code computed=true} with the
     * default {@code eligible=false}. See {@link #leadEligible}.
     */
    public void setLeadEligible(boolean value) {
        this.leadEligible = value;
        this.leadEligibilityComputed = true;
    }

    /**
     * Sets the in-RAM lead row count. See {@link #leadRowCount}.
     */
    public void setLeadRowCount(long leadRowCount) {
        this.leadRowCount = leadRowCount;
    }

    public void setLvConsumedSeqTxn(long lvConsumedSeqTxn) {
        stateReader.setLvConsumedSeqTxn(lvConsumedSeqTxn);
    }

    /**
     * Re-stamps the cumulative LV row counter, used by the head-checkpoint
     * restore path to load the manifest's {@code lvRowPosition} so subsequent
     * incremental appends stack on top of the restored value.
     */
    public void setLvRowsTotal(long lvRowsTotal) {
        this.lvRowsTotal = lvRowsTotal;
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
     * Sets the in-RAM refresh cursor. See {@link #refreshedUpToSeqTxn}.
     */
    public void setRefreshedUpToSeqTxn(long refreshedUpToSeqTxn) {
        this.refreshedUpToSeqTxn = refreshedUpToSeqTxn;
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

    /**
     * Marks (or clears) the published in-mem slot as possibly carrying stale
     * pre-O3 rows after a both-slots-pinned rebuild skip. The refresh worker
     * sets it from {@code rebuildInMemoryTier} when the skip happens and clears
     * it on the next successful publish / rebuild. See {@link #isTierStale()}.
     */
    public void setTierStale(boolean tierStale) {
        this.tierStale = tierStale;
    }

    public void setWriterStallStartUs(long writerStallStartUs) {
        this.writerStallStartUs = writerStallStartUs;
    }

    /**
     * Marks the view frozen for the duration of a {@code DatabaseCheckpointAgent}
     * file copy. {@code frozenAppliedWatermark} is the {@code appliedWatermark}
     * at the time of freeze; recorded for diagnostics. Refresh-worker turns
     * that observe {@link #isFreezeInProgress()} short-circuit before mutating
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
    public void startCheckpoint(long frozenAppliedWatermark) {
        // Synchronize on the instance monitor while publishing the flag so any
        // invalidator inside synchronized(instance) on another thread either
        // (a) commits its rewrite before the agent's file copy begins, or
        // (b) observes freezeInProgress=true and parks via waitForUnfrozen().
        synchronized (this) {
            freezeFrozenLvSeqTxn = frozenAppliedWatermark;
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

    /**
     * Frees the refresh-worker-internal runtime state of an invalidated view -
     * {@link #compiledFactory}, {@link #anchorWindow}, {@link #anchorFunction},
     * and {@link #inMemoryTier} - so an INVALID view releases them promptly
     * rather than pinning them until DROP or shutdown. The view stays in the
     * registry and queryable: reads serve from the on-disk tier via {@code TableReader},
     * which consults none of these fields ({@code LiveViewRecordCursor} only
     * reads {@link #getInMemoryTier()}, and a null tier routes the cursor
     * disk-only), and a cursor that pinned an in-mem slot before invalidation
     * keeps it alive via the tier's deferred-close protocol.
     * <p>
     * {@link #isClosed} is deliberately NOT set: that flag drives registry
     * visibility and would flip the lifecycle to DROPPING, but an invalid view
     * must keep reporting INVALID.
     * <p>
     * Mirrors {@link #tryCloseIfDropped()}: CAS-acquires the refresh latch and
     * frees only when no refresh cycle is in flight. On CAS failure the caller
     * relies on the refresh worker's finally hook to retry once the in-flight
     * cycle completes. Idempotent - the freed fields become null, so repeat
     * calls (and a later {@code tryCloseIfDropped} / {@code close}) are no-ops.
     */
    public void tryFreeRuntimeStateIfInvalid() {
        if (isClosed || !stateReader.isInvalid()) {
            return;
        }
        if (!refreshLatch.compareAndSet(false, true)) {
            // Refresh in flight; the worker's finally hook retries.
            return;
        }
        try {
            compiledFactory = Misc.free(compiledFactory);
            anchorWindow = Misc.free(anchorWindow);
            anchorFunction = Misc.free(anchorFunction);
            inMemoryTier = Misc.free(inMemoryTier);
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
