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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.RecordToRowCopier;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTracker;
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
    private static final int HEAD_CHECKPOINT_BASE_SEQ_TXN = 3;
    private static final int HEAD_CHECKPOINT_LV_SEQ_TXN = 0;
    private static final int HEAD_CHECKPOINT_MAX_TS = 1;
    private static final int HEAD_CHECKPOINT_STATE_BYTES = 2;
    private static final long[] EMPTY_HEAD_CHECKPOINT = {Numbers.LONG_NULL, Numbers.LONG_NULL, 0L, Numbers.LONG_NULL};
    private final LiveViewDefinition definition;
    // Cancellation flag the refresh worker binds into its execution context's circuit
    // breaker for the duration of a cycle over this view. DROP and invalidation set it,
    // so a scan already inside the compiled cursor unwinds instead of running to
    // completion while the caller spins in fenceRefresh(). Terminal by construction -
    // both sources end the view's refreshing life - so nothing clears it.
    private final AtomicBoolean refreshCancelled = new AtomicBoolean(false);
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
    // Base seqTxn the deferred cycle waited on when it armed applyLagDeferUntilUs. The pre-latch
    // guard clears the floor early once the base applies past this point, so a caught-up view
    // converges without waiting out the wall-clock floor (which a frozen clock never crosses).
    // LONG_NULL when unarmed; volatile and armed before applyLagDeferUntilUs so a guard that sees
    // the floor also sees a published target.
    private volatile long applyLagDeferTargetSeqTxn = Numbers.LONG_NULL;
    // Wall-clock (micros) floor before which the refresh worker skips this view after a
    // cooperative apply-lag deferral, bounding the re-drain rate so it does not hot-spin the
    // window recompute while the transient lag clears. LONG_NULL until armed;
    // recordRefreshSuccess clears it back to LONG_NULL after a cycle drains cleanly so a
    // recovered view stops taking the clock-read branch. Volatile because refreshInstance reads
    // it pre-latch (a best-effort throttle) while another refresh worker may be writing it under
    // the latch: the volatile publishes a coherent, untorn value. The field only ever holds
    // LONG_NULL or a near-future floor, so a stale read costs at most one extra re-drain or a
    // one-tick defer, never a permanently wrong skip.
    private volatile long applyLagDeferUntilUs = Numbers.LONG_NULL;
    // Cumulative count of in-order (forward-append) base rows dropped because their
    // timestamp fell below viewLowerBoundTimestamp. Surfaced via
    // live_views().below_lower_bound_count. Complements o3RejectedCount, which
    // counts the same drops on the O3 replay path, so the two never overlap: a
    // given commit is diverted to exactly one path. Bumped only on the refresh
    // worker at the in-order drain; volatile so the catalogue query thread reads a
    // current value. In-memory only - resets to 0 on restart (an observability
    // signal, not durable state). Non-zero means back-dated / pre-CREATE data is
    // being silently excluded by the boundary; an earlier START FROM avoids it.
    private volatile long belowLowerBoundCount;
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
    // dropped/invalid checks) and skips the cycle. The frozen appliedWatermark
    // at the time of freeze is captured so post-restore consistency can be
    // asserted; the field is informational for tests and diagnostics.
    private volatile long freezeFrozenAppliedWatermark = Numbers.LONG_NULL;
    private volatile boolean freezeInProgress;
    // One-shot latch for the advisory log the refresh worker emits the first time
    // a view drops in-order rows below viewLowerBoundTimestamp. Keeps the "dropping
    // sub-floor rows" hint to a single line per process rather than one per drain.
    // Touched only on the refresh-worker thread under the refresh latch, like
    // tierStale, so a plain field suffices.
    private boolean hasWarnedBelowLowerBoundDrop;
    // N=2 in-memory tier; lazily allocated on the
    // first refresh cycle after the LV's compiled factory + projected metadata
    // are known. Reads route through it via LiveViewRecordCursor; the refresh
    // worker drives the slow-path swap from
    // LiveViewRefreshJob. Null when no refresh has happened yet, or when the LV
    // was just constructed at startup.
    // Head-checkpoint metadata mirrored from the newest logical boundary the
    // versioned checkpoint timeline holds: the seal stamps it beside the root it
    // appends, and the restart restore stamps it from the root it selected. The
    // live_views() catalogue reads it off the worker thread.
    // <p>
    // The tuple is packed into one immutable long[] published via volatile
    // store so an off-worker reader always sees a consistent
    // (lvSeqTxn, maxTs, stateBytes) tuple; without the packing a reader
    // could observe a fresh lvSeqTxn paired with the prior maxTs.
    // baseSeqTxn is the base commit the durable head covers: WalPurgeJob holds
    // the base WAL purge floor at it so the (baseSeqTxn, applied] range restart
    // recovery replays survives until a later checkpoint advances past it.
    // Indexes: HEAD_CHECKPOINT_LV_SEQ_TXN / _MAX_TS / _STATE_BYTES /
    // _BASE_SEQ_TXN.
    private volatile long[] headCheckpoint = EMPTY_HEAD_CHECKPOINT;
    // Base-WAL retention floor of the durable versioned checkpoint timeline:
    // the minimum normalizedBaseSeqTxn required by either valid A/B slot.
    // Published to this volatile mirror only after the superblock commit point,
    // and adopted from bounded timeline validation at startup. Recovery and
    // repair owners lower the floor for their pins before exposing them. The WAL
    // purge job min-combines it with the head arm. LONG_NULL means no usable
    // timeline generation currently requires WAL.
    private volatile long checkpointTimelineWalPurgeFloor = Numbers.LONG_NULL;
    // Elapsed wall-clock (micros) of the most recent restart restore from the
    // checkpoint timeline (select a root, restore its state, replay-to-applied).
    // Numbers.LONG_NULL until a restore runs, which is single-shot per LV
    // lifetime, so it stays NULL for a view that never restored. Mutated only on
    // the refresh worker under the refresh latch; volatile for the catalogue
    // thread. Surfaced via live_views().head_checkpoint_restore_micros.
    private volatile long headCheckpointRestoreMicros = Numbers.LONG_NULL;
    // Elapsed wall-clock (micros) of the most recent head-checkpoint write
    // (maybeWriteHeadCheckpoint: freeze the function state, append a logical
    // root, publish the timeline generation). Numbers.LONG_NULL until the first
    // root is sealed. Mutated only on the refresh worker under the refresh
    // latch; volatile for the catalogue thread. Surfaced via
    // live_views().head_checkpoint_write_micros.
    private volatile long headCheckpointWriteMicros = Numbers.LONG_NULL;
    private volatile LiveViewInMemoryTier inMemoryTier;
    private volatile boolean isClosed;
    // Per-view tracker for the persistent per-partition state: the anchor map (owned by
    // anchorWindow) and each anchored function's partition map (owned by compiledFactory).
    // That state outlives the cycle that built it, so the tracker's lifetime is the cached
    // state, not one refresh attempt. Acquired when the anchor window is built, released by
    // freeCachedRefreshState(). Null when the view has no anchored window; a limit of 0
    // (the default) accounts but never throws. Mutated only under the refresh latch.
    private MemoryTracker memoryTracker;
    // Restart-restore single-shot flag. The refresh worker flips it true on
    // the first cycle after CREATE / restart, regardless of whether a usable
    // timeline root was found - one attempt is the contract, no retries. Mutated only
    // under the refresh latch; volatile so the catalogue thread can read
    // the latest value without additional synchronisation.
    private volatile boolean checkpointRestoreAttempted;
    // Set true only when a timeline root restore actually rehydrated the window
    // state. Stays false when no usable root existed or the restore failed and
    // fell back to a from-base rebuild. Distinguishes a real
    // restore from the replay fallback for observability and tests. Mutated only
    // under the refresh latch; volatile for the catalogue thread.
    private volatile boolean checkpointRestoreSucceeded;
    // Wall-clock (micros) of the most recent head-checkpoint seal. Numbers.LONG_NULL
    // until the first cycle that seals a root. The refresh worker compares
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
    // The isLeadReconstruction() value the refresh worker observed on this view's
    // previous cycle, read and updated together with checkpointRestoreAttempted so it
    // is set exactly when a cycle actually reached the restore/reconcile block. A
    // true -> false transition marks an in-process promote: a demoted primary, or a
    // read-only replica that ran lead reconstruction (which burns
    // checkpointRestoreAttempted), has just become a writable primary on the SAME
    // instance, so the single-shot first-cycle restart recovery was already spent. The
    // first primary cycle uses that edge to re-arm that recovery (only when the applied
    // floor actually lags), which reconciles the floor up to disk truth AND rebuilds the
    // window accumulators / lead frontier from the applied tier, so it does not re-derive
    // (and forward-append duplicate) a base range the replica already materialised when
    // its trailing _lv.s persist failed. Mutated only under the refresh latch; volatile
    // for the catalogue thread.
    private volatile boolean lastRefreshLeadReconstruction;
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
    // {@code _lv.s} because (a) the newest root's maxTimestamp already plays the
    // gating role for O3 detection and (b) trailing this in {@code _lv.s} would
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
    // Read-only-replica O3 catch-up floor: the base seqTxn the LV table's applied watermark must reach
    // before the lead loop resumes staging after an out-of-order base commit reset it to cold start.
    // The primary handles an O3 base commit with o3Replay, which rewrites the on-disk symbol id space
    // and replicates the correction as an LV flush. Until that flush applies here (appliedWatermark >=
    // this seqTxn), a re-derive would re-intern a resequenced value at a fresh lead symbol id above the
    // still-lagging committed count -- a stranded id the disk never assigns, which breaks the read
    // path's symbol-table keyOf/valueOf agreement. Deferring staging until the disk catches up serves
    // reads disk-only meanwhile (correct, at worst one flush cycle stale). LONG_NULL means "not
    // waiting". In-RAM only; mutated under the refresh latch only.
    private long leadO3CatchupSeqTxn = Numbers.LONG_NULL;
    // Read-only-replica lead catch-up seam: the on-disk (LV table) maximum timestamp the lead
    // loop's window accumulators must reach before the drain resumes staging rows into the lead.
    // A replicated flush can advance the on-disk tier (and the applied watermark) past the point
    // the loop has computed -- the loop fell behind, or the LV WAL applied ahead of the base -- so
    // the accumulators (row_number(), running aggregates) trail disk over the (latestSeenTs,
    // diskMaxTs] band. reconcileLeadWithDisk arms this seam; drainAppliedBaseForLead drives the
    // accumulators over that band without staging it (a plain drain would re-stage rows disk
    // already holds, double-counting size()), then clears the seam once caught up. LONG_NULL means
    // "not catching up". In-RAM only; mutated under the refresh latch only.
    // Read-only-replica lead catch-up seam, tie quota: how many rows the on-disk (LV table) tier holds
    // AT leadReconcileSeamTs. The seam is a single timestamp, but output rows can tie it across the
    // flush boundary -- the primary flushed some of them and a later base commit produced more at the
    // same ts. Suppressing every tied row would drop the genuinely un-flushed ones from the replica's
    // result set until the next flush; staging every tied row would double-count the durable ones.
    // reconcileLeadWithDisk counts the durable ties off the LV reader when it arms the seam, and
    // drainAppliedBaseForLead suppresses exactly that many rows at the seam ts and stages the rest.
    // 0 when no seam is armed. In-RAM only; mutated under the refresh latch only.
    private long leadReconcileSeamDurableTies;
    private long leadReconcileSeamTs = Numbers.LONG_NULL;
    // Read-only-replica publish-stall back-off: a wall-clock retry floor armed when a lead publish
    // stalls (both in-mem tier slots reader-pinned, so a read-only replica cannot flush the lead).
    // scanForLaggingViews skips the view until the clock passes it, so the worker does not re-derive
    // every tick. LONG_NULL means no back-off pending. In-RAM only; mutated under the refresh latch only.
    private long leadRetryAfterUs = Numbers.LONG_NULL;
    // In-RAM lead row count: the number of output rows refreshed into the in-mem
    // tier but not yet flushed to the LV's on-disk table. Grows with each refresh
    // tick, reset to 0 at flush. Stamped onto the published slot so reads can serve
    // the lead on top of disk (size() = disk.size() + leadRowCount). In-RAM only
    // (recovered by replaying base WAL forward on restart); mutated under the
    // refresh latch only, like the other refresh-worker-only fields.
    private long leadRowCount;
    // Live-view's own table token. Populated at construction.
    private final TableToken liveViewToken;
    // Cumulative count of base rows the in-order drain physically visited while skipping the
    // sub-floor prefix through TimestampLowerBoundCursor. A wholly sub-floor commit is dropped
    // in O(1) (its max ts is below the boundary) without visiting any row, so this stays 0 for
    // it; a straddling commit contributes only its sub-floor prefix length. Diagnostic hook that
    // lets a test prove the O(1) skip: it counts row VISITS, unlike below_lower_bound_count which
    // counts row DROPS. Bumped only on the refresh worker at the in-order drain; volatile so a
    // reader off the worker thread sees a current value. In-memory only - resets to 0 on restart.
    private volatile long lowerBoundRowsScanned;
    // Cumulative count of live-view rows produced over the LV's lifetime,
    // matching the MANIFEST.lvRowPosition field on every head checkpoint.
    // Initialised to 0 on construction. The restart restore and the O3 resume
    // replay stamp this from the restored root after a successful restore;
    // every subsequent {@link #addRowsSinceLastCheckpointWritten(long)} bumps
    // both this and the cadence counter so writes and restores stay aligned.
    // Mutated under the refresh latch only.
    private long lvRowsTotal;
    // Cumulative count of live-view rows re-emitted by boundary-rebuild O3 replays
    // (o3HeadMissReplay - the full recompute from viewLowerBoundTimestamp). Surfaced
    // via live_views().o3_boundary_replay_rows; the residual-fallback counterpart to
    // o3ResumeReplayRows. This path is unbounded (O(view age)), so a growing value
    // flags late rows the timeline holds no boundary below - the case the resume
    // path cannot bound. Bumped only on the refresh-worker thread at replay
    // completion; volatile so the catalogue query thread reads a current value.
    // In-memory only - resets to 0 on restart (an observability signal, not durable
    // state). Disjoint from o3ResumeReplayRows: a given O3 replay bumps exactly one.
    private volatile long o3BoundaryReplayRows;
    // Cumulative count of late O3 rows rejected because their timestamp fell below
    // viewLowerBoundTimestamp. Surfaced via live_views().o3_rejected_count. Bumped
    // only on the refresh-worker thread at the O3-detection step; volatile so the
    // catalogue query thread reads a current value. In-memory only - it resets to
    // 0 on restart (an observability signal, not durable state). Rows can only land
    // below the bound via the O3 path: in-WAL order guarantees
    // ts >= latestSeenTs >= viewLowerBoundTimestamp.
    private volatile long o3RejectedCount;
    // Cumulative count of base rows the O3 replay paths (replayFromAnchor resume
    // and o3HeadMissReplay boundary rebuild) SCANNED - every base row the source
    // cursor pulled, including rows a WHERE filter dropped. Distinct from the
    // emit counters o3ResumeReplayRows / o3BoundaryReplayRows, which count only
    // rows re-emitted: with a filter, scan exceeds emit by the dropped rows; with
    // no filter the two are equal. The baseline scan-cost signal the localized O3
    // repair is designed to bound to [L, H). Bumped only on the refresh-worker
    // thread at replay completion; volatile so the catalogue query thread reads a
    // current value. In-memory only - resets to 0 on restart. Surfaced via
    // live_views().o3_replay_scan_rows.
    private volatile long o3ReplayScanRows;
    // Cumulative count of live-view rows re-emitted by bounded resume-from-anchor O3
    // replays (replayFromAnchor - the tail re-evaluation above the newest logical
    // boundary strictly below the change). Surfaced via
    // live_views().o3_resume_replay_rows; this is "the win" - the replay stays bounded
    // to the tail above the anchor rather than recomputing the whole view. Bumped only
    // on the refresh-worker thread at replay completion; volatile so the catalogue
    // query thread reads a current value. In-memory only - resets to 0 on restart (an
    // observability signal, not durable state). Disjoint from o3BoundaryReplayRows.
    private volatile long o3ResumeReplayRows;
    // Reason string the refresh worker stashes here when a head-restore step
    // surfaces a "version too old" function snapshot. The worker holds the
    // refresh latch when populating this field; the same worker drains it
    // (consumes and clears) after releasing the latch and runs
    // engine.invalidateLiveView. The two-step is to avoid a deadlock: the
    // invalidate path parks on the instance monitor when a checkpoint freeze
    // is active, and the agent's startCheckpoint cannot complete its latch
    // handshake while the worker holds the refresh latch.
    private String pendingInvalidationReason;
    // LV-WRITER space, not base space: the live-view writer's own seqTxn of an
    // out-of-order repair's REPLACE_RANGE block that committed but whose inline
    // apply did not land. LONG_NULL when nothing is outstanding. Refresh is blocked
    // behind reconciliation until such a replacement is known applied or not
    // applied: the repair's own bookkeeping (lvRowsTotal, every repaired root's
    // lvRowPosition, the suffix range-add) reads the materialised table, so a turn
    // that runs over an unapplied replacement derives its coordinates from a table
    // that does not hold the output. In-RAM only - a restart reconciles the same
    // window through LiveViewRefreshJob.reconcileAppliedFloorAfterRestart. Mutated
    // and read under the refresh latch.
    private long pendingReplacementLvSeqTxn = Numbers.LONG_NULL;
    // Cached RecordToRowCopier (compiled bytecode bridging the SELECT cursor's record
    // shape to the LV's WalWriter row). Invalidated when the WalWriter's metadata version
    // moves past recordRowCopierMetadataVersion. Accessed only while the refresh latch is held.
    private RecordToRowCopier recordToRowCopier;
    private long recordRowCopierMetadataVersion = -1;
    // Lifetime count of refresh cycles that threw, incremented once per entry into
    // LiveViewRefreshJob.handleRefreshFailure. Unlike flushRetryCount this is never reset, because
    // most refresh faults are invisible after the fact: the job self-heals a mid-drain fault by
    // recomputing the window from the applied base and calls recordRefreshSuccess(), which zeroes
    // flushRetryCount, so a view that faults on every cycle and recomputes its way back to the right
    // answer is indistinguishable from one that never faulted. Tests that mean to assert the
    // incremental path was actually exercised (rather than silently falling back to a full
    // recompute) assert this is zero. Written under the refresh latch, read from test threads.
    private volatile long refreshFaultCount;
    // In-RAM refresh cursor: the highest base seqTxn whose rows have been refreshed
    // into the in-mem tier (the lead), which leads the flushed/applied point
    // ({@link #getLastProcessedSeqTxn()}) by the un-flushed lead. The refresh worker
    // drains base WAL from this point each tick; flush advances the applied point up
    // to it. In-RAM only (not persisted): on restart it re-initialises to the applied
    // point and drain-forward rebuilds the lead. LONG_NULL means "not initialised
    // yet" -> {@link #getRefreshedUpToSeqTxn()} falls back to the applied point.
    // Mutated under the refresh latch only, but WalPurgeJob reads it off-latch to
    // compute the base WAL purge floor, so it must be volatile: a stale read there
    // only over-retains base WAL (the value is monotone and min-combined with the
    // flushed point), never under-retains, but the visibility must not rely on the
    // latch a purge run never takes.
    private volatile long refreshedUpToSeqTxn = Numbers.LONG_NULL;
    // Live-view-row count applied since the most recent head-checkpoint commit.
    // The refresh worker compares this against cairo.live.view.checkpoint.rows
    // each cycle to decide whether the row-count trigger has fired. Mutated
    // only on the refresh-worker thread under the refresh latch; not volatile
    // because no other thread reads it.
    private long rowsSinceLastCheckpointWritten;
    // Base-table reader pinned across the whole multi-turn seed sweep so every
    // turn reads one stable MVCC snapshot. Without it, re-opening the base at the
    // latest applied seqTxn each turn makes the positional skipRows() resume
    // unsound: an out-of-order base commit landing below the swept prefix between
    // turns shifts physical row positions, so the next turn's skipRows() lands on a
    // different set - silently dropping the back-dated row and re-feeding the old
    // boundary row (double-advancing the accumulators). Borrowed (not detached) so
    // any thread can release it via close(); held from the first sweep turn until
    // the sweep completes or the view is dropped/invalidated/closed. Null when no
    // sweep is in flight. Accessed under the refresh latch (and the latch-guarded
    // free hooks / shutdown).
    private TableReader seedBaseReader;
    // Base data-cursor row offset the newest seed boundary was sealed at, or
    // Numbers.LONG_NULL when the sweep has sealed none. Drives the seed
    // checkpoint cadence (the delta since this offset) and, after a restart,
    // starts out at the offset the restored root's generation carries.
    // Volatile so the catalogue thread can read it; mutated under the refresh
    // latch.
    private volatile long seedCheckpointDataOffset = Numbers.LONG_NULL;
    // Designated timestamp of the newest seed boundary, or Numbers.LONG_NULL
    // when the sweep has sealed none. The timeline is keyed on
    // (maxTimestamp, checkpointId) and refuses a boundary at or below its head,
    // so a turn whose batch does not carry the sweep past this timestamp seals
    // nothing. Volatile alongside the offset it pairs with; mutated under the
    // refresh latch.
    private volatile long seedCheckpointMaxTs = Numbers.LONG_NULL;
    // In-memory count of base data-cursor rows the seed sweep has consumed
    // so far - the skipRows() resume position for the next turn. Persists in
    // memory across in-process turns (window state persists with it), and is
    // re-seeded from the timeline's newest root on the first turn after a
    // restart. Numbers.LONG_NULL until the first seed turn initialises it; 0
    // means "swept nothing yet". Mutated under the refresh latch only.
    private long seedDataOffset = Numbers.LONG_NULL;
    // Single-shot flag: the first seed turn of the process restores window
    // state + data offset from the timeline's newest root (if it holds one),
    // then later turns continue from the in-memory state. Mirrors
    // checkpointRestoreAttempted for the seed path. Mutated under the refresh
    // latch only.
    private boolean seedResumeAttempted;
    // Skip-write floor for the seed sweep: the LV table's on-disk row count
    // captured on the first turn of the process. Output rows whose position is
    // below it are already durable (deterministic recompute), so the sweep
    // recomputes them to advance window state but skips the WAL append. Spans
    // however many turns the catch-up needs; persists across turns (the per-turn
    // budget can split the catch-up). Mutated under the refresh latch only.
    private long seedSkipWriteFloor;
    // The pinned snapshot's seqTxn, fixed for the whole sweep (see seedBaseReader).
    // The SEEDING -> ACTIVE handoff advances the watermarks to exactly this value
    // so the ACTIVE phase's incremental drain (with O3 detection) covers everything
    // committed after the snapshot from seedSweepSeqTxn + 1. LONG_NULL until the
    // sweep's first turn pins it; reset to LONG_NULL when the reader is released.
    private long seedSweepSeqTxn = Numbers.LONG_NULL;
    // AND of every compiled window function's WindowFunction.supportsCheckpointState().
    // Computed once on the first refresh cycle after the LV's compiled factory
    // is ready, then cached. False means the flush cycle emits no checkpoints
    // (every restart / O3 falls back to the head-miss replay path); the LV's
    // live_views().head_checkpoint_lv_seqtxn stays LONG_NULL for its lifetime.
    private volatile boolean snapshotCapability;
    private volatile boolean snapshotCapabilityComputed;
    // The localized out-of-order repair this view has parked between refresh
    // turns, or null when none is in flight. A repair whose replay spends its
    // turn budget stops at a complete timestamp group and leaves everything the
    // next turn continues from here: the pinned base snapshot, the live-view
    // writer holding the still-uncommitted replacement, the staged root versions,
    // and the window state it took aside. Nothing durable moved while it sits
    // here, so a reader still sees the pre-repair view.
    // Refresh for this view is blocked while it is set - every coordinate a turn
    // would derive reads a runtime that is mid-replay - and only the refresh job
    // the session names may continue it. Freed by the drop / invalidate /
    // shutdown hooks the same way the seed sweep's pinned base reader is.
    private LiveViewCheckpointRepairSession suspendedRepair;
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
    // Non-null only for a minimal stub registered by the catalogue load path when
    // the on-disk _lv / _lv.s could not be loaded (a too-new format version, or a
    // torn / corrupt file with no recoverable state). Such an instance has a null
    // definition and default runtime state; getLifecycleState reports this terminal
    // state (VERSION_UNSUPPORTED or STATE_UNREADABLE) and the catalogue surfaces only
    // view_name / view_status. Final, so it is safely published to the catalogue read
    // thread. Null for a normally-loaded instance.
    private final LiveViewLifecycleState stubState;
    // Wall-clock (micros) when the in-mem tier's slow-path tryAcquireWrite first
    // observed both slots reader-pinned. Numbers.LONG_NULL when not stalled.
    // Cleared on the next successful acquire. Surfaces via
    // live_views().writer_stall_micros for operator visibility.
    private volatile long writerStallStartUs = Numbers.LONG_NULL;

    public LiveViewInstance(LiveViewDefinition definition, TableToken liveViewToken) {
        this.definition = definition;
        this.liveViewToken = liveViewToken;
        this.stubState = null;
    }

    /**
     * Builds a minimal stub for a live view the catalogue load path could not fully
     * load: a too-new on-disk format ({@link LiveViewLifecycleState#VERSION_UNSUPPORTED})
     * or a torn / corrupt {@code _lv} / {@code _lv.s} with no recoverable state
     * ({@link LiveViewLifecycleState#STATE_UNREADABLE}). Carries only the token; the
     * definition is null and the runtime state stays at defaults. The catalogue surfaces
     * it with the matching {@code view_status}; the refresh worker never runs against it,
     * and DROP LIVE VIEW removes it best-effort.
     */
    public LiveViewInstance(TableToken liveViewToken, LiveViewLifecycleState stubState) {
        this.definition = null;
        this.liveViewToken = liveViewToken;
        this.stubState = stubState;
    }

    /**
     * Accumulates {@code n} into both {@link #rowsSinceLastCheckpointWritten}
     * (the cadence counter, which resets on each fresh head via
     * {@link #setHeadCheckpoint(long, long, long, long, long)}) and
     * {@link #lvRowsTotal} (the lifetime counter, which mirrors
     * {@code MANIFEST.lvRowPosition} and persists across restarts). Called
     * from the refresh worker after each successful LV WAL apply commit.
     */
    public void addRowsSinceLastCheckpointWritten(long n) {
        rowsSinceLastCheckpointWritten += n;
        lvRowsTotal += n;
    }

    /**
     * Accumulates {@code n} in-order (forward-append) base rows dropped for
     * falling below {@code viewLowerBoundTimestamp}. Called from the refresh
     * worker at the in-order drain; the value is exposed via
     * {@code live_views().below_lower_bound_count}. The O3 replay path counts
     * its own sub-floor drops via {@link #bumpO3RejectedCount(long)}; the two
     * never double-count the same row (a commit is diverted to one path only).
     */
    public void bumpBelowLowerBoundCount(long n) {
        belowLowerBoundCount += n;
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
     * Accumulates {@code n} base rows the in-order drain physically visited while skipping the
     * sub-floor prefix through {@code TimestampLowerBoundCursor}. A wholly sub-floor commit is
     * dropped in O(1) and contributes 0; a straddling commit contributes its sub-floor prefix
     * length. Counts row VISITS (work done), unlike {@link #bumpBelowLowerBoundCount(long)} which
     * counts row DROPS. Called from the refresh worker at the in-order drain.
     */
    public void bumpLowerBoundRowsScanned(long n) {
        lowerBoundRowsScanned += n;
    }

    /**
     * Accumulates {@code n} live-view rows re-emitted by a boundary-rebuild O3
     * replay (the full recompute from {@code viewLowerBoundTimestamp}). Called
     * from the refresh worker at replay completion; the value is exposed via
     * {@code live_views().o3_boundary_replay_rows}. Disjoint from
     * {@link #bumpO3ResumeReplayRows(long)} - a given O3 replay bumps one only.
     */
    public void bumpO3BoundaryReplayRows(long n) {
        o3BoundaryReplayRows += n;
    }

    /**
     * Accumulates {@code n} late O3 rows rejected for falling below
     * {@code viewLowerBoundTimestamp}. Called from the refresh worker at the
     * O3-detection step; the value is exposed via {@code live_views().o3_rejected_count}.
     */
    public void bumpO3RejectedCount(long n) {
        o3RejectedCount += n;
    }

    /**
     * Accumulates {@code n} base rows an O3 replay (resume or boundary) scanned -
     * every source row pulled, including rows a WHERE filter dropped. Called from
     * the refresh worker at replay completion; the value is exposed via
     * {@code live_views().o3_replay_scan_rows}. Distinct from the emit counters
     * {@link #bumpO3ResumeReplayRows(long)} / {@link #bumpO3BoundaryReplayRows(long)}:
     * with a filter it exceeds the emit count, without one it equals it.
     */
    public void bumpO3ReplayScanRows(long n) {
        o3ReplayScanRows += n;
    }

    /**
     * Accumulates {@code n} live-view rows re-emitted by a bounded
     * resume-from-anchor O3 replay (head-hit tail re-eval or bounded-miss resume
     * from an older sealed checkpoint). Called from the refresh worker at replay
     * completion; the value is exposed via {@code live_views().o3_resume_replay_rows}.
     * Disjoint from {@link #bumpO3BoundaryReplayRows(long)}.
     */
    public void bumpO3ResumeReplayRows(long n) {
        o3ResumeReplayRows += n;
    }

    /**
     * Trips the refresh cancellation flag this view's cycles run under, so a scan
     * already inside the compiled cursor throws on its next circuit-breaker check
     * instead of finishing. Called by DROP and by invalidation - the two events that
     * end the view's refreshing life - which is why the flag is never cleared: a view
     * past either one is refused at the top of {@code refreshInstance} anyway.
     * <p>
     * Safe from any thread, and it does not wait: the cancelled cycle unwinds through
     * its own error path, discarding an in-flight repair candidate with it. See
     * {@link #getRefreshCancelledFlag()}.
     */
    public void cancelRefresh() {
        refreshCancelled.set(true);
    }

    @Override
    public void close() {
        // Shutdown path only — called from CairoEngine.close after all workers stopped.
        dropped = true;
        if (!isClosed) {
            isClosed = true;
            discardSuspendedRepair();
            freeSeedBaseReader();
            freeCachedRefreshState();
        }
    }

    /**
     * Abandons a localized out-of-order repair parked between refresh turns,
     * releasing the pinned base snapshot, rolling the uncommitted replacement
     * back, unlinking the staged data segment and retiring the repair's durable
     * descriptor. The candidate is worthless once nothing will resume it, and the
     * files it staged have no other owner. Idempotent (null-safe).
     * <p>
     * The window state the replay had reached goes back to the pre-repair state the
     * session took aside, which is the state the untouched durable output belongs
     * to. Callers must therefore run before {@link #freeCachedRefreshState()}, and
     * under the refresh latch (or after the workers have stopped) so no turn is
     * driving those same window functions.
     */
    public void discardSuspendedRepair() {
        suspendedRepair = Misc.free(suspendedRepair);
    }

    /**
     * Companion to {@link #startCheckpoint(long)}. Clears the freeze gate so
     * the refresh worker resumes on its next turn and wakes any thread blocked
     * in {@link #waitForUnfrozen()}. Idempotent.
     */
    public void endCheckpoint() {
        synchronized (this) {
            freezeInProgress = false;
            freezeFrozenAppliedWatermark = Numbers.LONG_NULL;
            notifyAll();
        }
    }

    /**
     * Spin-acquires and releases the refresh latch, mirroring
     * {@link #startCheckpoint(long)} without the freeze gate: it waits out any
     * in-flight refresh turn and, via the CAS barrier, publishes state the caller
     * set beforehand to the worker's next {@link #tryLockForRefresh()}. DROP pairs
     * it with a prior {@link #markAsDropped()} so no worker is mid-commit and the
     * next under-latch recheck sees the drop before the table is torn down.
     */
    public void fenceRefresh() {
        while (!refreshLatch.compareAndSet(false, true)) {
            Os.pause();
        }
        refreshLatch.set(false);
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

    /**
     * Releases the base-table snapshot pinned across the seed sweep (see
     * {@link #getSeedBaseReader()}). The reader is borrowed, not detached, so
     * {@code close()} returns it to the pool from any thread. Idempotent (null-safe).
     * <p>
     * Callers must guarantee no concurrent sweep turn is reading from it: the sweep
     * completion / recompile call sites hold the refresh latch, the drop/invalidate
     * free hooks CAS the latch, and the engine-shutdown call site runs after the
     * refresh workers have stopped.
     */
    public void freeSeedBaseReader() {
        seedBaseReader = Misc.free(seedBaseReader);
        seedSweepSeqTxn = Numbers.LONG_NULL;
    }

    public Function getAnchorFunction() {
        return anchorFunction;
    }

    public LiveViewWindow getAnchorWindow() {
        return anchorWindow;
    }

    public long getApplyLagDeferTargetSeqTxn() {
        return applyLagDeferTargetSeqTxn;
    }

    public long getApplyLagDeferUntilUs() {
        return applyLagDeferUntilUs;
    }

    public long getBelowLowerBoundCount() {
        return belowLowerBoundCount;
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
     * Returns {@code true} if any of this view's dependency columns is either missing
     * from the post-change writer metadata — dropped or renamed away — or still
     * present under the same name but with a different {@code TYPE} than the view
     * compiled against. Callers use this to decide whether a base-table schema change
     * must invalidate the view.
     * <p>
     * The type check closes a memory-safety hole: the cached compiled factory derives
     * each column's stride from its compile-time type, so a referenced column whose
     * type changed (e.g. INT-&gt;LONG, LONG-&gt;INT, INT-&gt;VARCHAR) would otherwise
     * keep being read through the stale stride — wrong results on a widening change,
     * an out-of-bounds native read on a narrowing or fixed&lt;-&gt;var-size change.
     * <p>
     * An empty dependency set returns {@code false} (defensive: we don't know what the
     * view reads, so we leave invalidation to the broader path).
     */
    public boolean dependsOnMissingOrRetypedColumn(@NotNull RecordMetadata baseMetadata) {
        return findFirstMissingOrRetypedColumn(baseMetadata) != null;
    }

    /**
     * Returns the name of the first dependency column that is either missing from the
     * post-change writer metadata — dropped or renamed away — or still present under the
     * same name but with a different {@code TYPE} than the view compiled against, or
     * {@code null} when every dependency still resolves. Backs
     * {@link #dependsOnMissingOrRetypedColumn(RecordMetadata)} and lets the invalidation
     * site name the offending column in {@code invalidation_reason}.
     * <p>
     * Returns the interned dependency name straight from the definition, so it allocates
     * nothing on the schema-change path. An empty dependency set returns {@code null}
     * (defensive: with no known deps we leave invalidation to the broader
     * base-DROP/RENAME path).
     */
    public @Nullable String findFirstMissingOrRetypedColumn(@NotNull RecordMetadata baseMetadata) {
        ObjList<String> deps = definition.getDependencyColumnNames();
        // unreachable in practice for a normally-created view: a real view always
        // records its referenced base columns (locked by
        // LiveViewBaseDdlTest#testDependencyColumnSetIsNonEmpty). Defensive: with no
        // known deps we leave invalidation to the broader base-DROP/RENAME path.
        if (deps.size() == 0) {
            return null;
        }
        // The definition writer and reader keep the types list positionally
        // parallel to the names list (same count), so the two index together.
        IntList depTypes = definition.getDependencyColumnTypes();
        for (int i = 0, n = deps.size(); i < n; i++) {
            final String depName = deps.getQuick(i);
            final int columnIndex = baseMetadata.getColumnIndexQuiet(depName);
            if (columnIndex < 0) {
                return depName;
            }
            if (baseMetadata.getColumnType(columnIndex) != depTypes.getQuick(i)) {
                return depName;
            }
        }
        return null;
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
     * @return the {@code appliedWatermark} captured at {@link #startCheckpoint(long)},
     * or {@link Numbers#LONG_NULL} when no freeze is in progress. Useful for tests
     * and post-restore consistency assertions.
     */
    public long getFreezeFrozenAppliedWatermark() {
        return freezeFrozenAppliedWatermark;
    }

    public long getCheckpointTimelineWalPurgeFloor() {
        return checkpointTimelineWalPurgeFloor;
    }

    public long getHeadCheckpointBaseSeqTxn() {
        return headCheckpoint[HEAD_CHECKPOINT_BASE_SEQ_TXN];
    }

    public long getHeadCheckpointLvSeqTxn() {
        return headCheckpoint[HEAD_CHECKPOINT_LV_SEQ_TXN];
    }

    public long getHeadCheckpointMaxTs() {
        return headCheckpoint[HEAD_CHECKPOINT_MAX_TS];
    }

    /**
     * @return elapsed micros of the most recent restart restore-from-head, or
     * {@link Numbers#LONG_NULL} when this view has not restored. See
     * {@link #headCheckpointRestoreMicros}.
     */
    public long getHeadCheckpointRestoreMicros() {
        return headCheckpointRestoreMicros;
    }

    /**
     * Atomic read of the {@code (lvSeqTxn, maxTs)} pair the O3 head-hit
     * eligibility check needs. Returns a stable two-element array
     * {@code [lvSeqTxn, maxTs]} so callers cannot observe a torn pair across
     * a concurrent {@link #setHeadCheckpoint(long, long, long, long, long)}.
     */
    public long[] getHeadCheckpointSeqAndMaxTs() {
        final long[] local = headCheckpoint;
        return new long[]{local[HEAD_CHECKPOINT_LV_SEQ_TXN], local[HEAD_CHECKPOINT_MAX_TS]};
    }

    public long getHeadCheckpointStateBytes() {
        return headCheckpoint[HEAD_CHECKPOINT_STATE_BYTES];
    }

    /**
     * @return elapsed micros of the most recent head-checkpoint write, or
     * {@link Numbers#LONG_NULL} when this view has not written one. See
     * {@link #headCheckpointWriteMicros}.
     */
    public long getHeadCheckpointWriteMicros() {
        return headCheckpointWriteMicros;
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
     * @return the read-only-replica O3 catch-up floor (the base seqTxn the applied watermark must reach
     * before the lead loop resumes staging after an O3 reset), or {@link Numbers#LONG_NULL} when not
     * waiting. See {@link #leadO3CatchupSeqTxn}.
     */
    public long getLeadO3CatchupSeqTxn() {
        return leadO3CatchupSeqTxn;
    }

    /**
     * @return how many rows the LV's on-disk tier holds at {@link #getLeadReconcileSeamTs()} -- the
     * number of rows the drain must suppress at the seam ts before staging the rest as genuine lead.
     * 0 when no seam is armed. See {@link #leadReconcileSeamDurableTies}.
     */
    public long getLeadReconcileSeamDurableTies() {
        return leadReconcileSeamDurableTies;
    }

    /**
     * @return the read-only-replica lead catch-up seam (LV on-disk max ts the accumulators must
     * reach before staging resumes), or {@link Numbers#LONG_NULL} when not catching up. See
     * {@link #leadReconcileSeamTs}.
     */
    public long getLeadReconcileSeamTs() {
        return leadReconcileSeamTs;
    }

    /**
     * @return the read-only-replica publish-stall retry floor (wall-clock micros before which the lead
     * loop must not retry a both-slots-pinned publish), or {@link Numbers#LONG_NULL} when no back-off is
     * pending. See {@link #leadRetryAfterUs}.
     */
    public long getLeadRetryAfterUs() {
        return leadRetryAfterUs;
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
        if (stubState != null) {
            // Stub for an unloadable view (too-new format, or torn / corrupt state):
            // its durable signals were never read, so report the terminal state directly.
            return stubState;
        }
        // A registered LiveViewInstance has, by definition, completed CREATE,
        // so CREATING is unreachable here. close() always flips `dropped` before
        // `isClosed`, so `!dropped && !isClosed` collapses to "not yet dropped"
        // and feeds the registryVisible signal.
        return LiveViewLifecycleState.derive(
                !dropped && !isClosed,
                stateReader.isInvalid(),
                stateReader.getSeedState() == LiveViewState.SEED_STATE_SEEDING
        );
    }

    public TableToken getLiveViewToken() {
        return liveViewToken;
    }

    /**
     * @return cumulative base rows the in-order drain physically visited while skipping the
     * sub-floor prefix (row VISITS, not DROPS). Stays 0 for wholly sub-floor commits, which are
     * dropped in O(1). Diagnostic hook for tests; resets to 0 on restart.
     */
    public long getLowerBoundRowsScanned() {
        return lowerBoundRowsScanned;
    }

    /**
     * @return cumulative LV row count, matching the value persisted as
     * {@code MANIFEST.lvRowPosition} on the most recent head checkpoint.
     */
    public long getLvRowsTotal() {
        return lvRowsTotal;
    }

    /**
     * @return the per-view tracker charged for the anchor map and the anchored window
     * functions' partition maps, or null when the view has no anchored window yet. The
     * refresh worker binds it into the SQL execution context so the window cursor's
     * lazily created function maps allocate against it.
     */
    public @Nullable MemoryTracker getMemoryTracker() {
        return memoryTracker;
    }

    public long getO3BoundaryReplayRows() {
        return o3BoundaryReplayRows;
    }

    public long getO3RejectedCount() {
        return o3RejectedCount;
    }

    public long getO3ReplayScanRows() {
        return o3ReplayScanRows;
    }

    public long getO3ResumeReplayRows() {
        return o3ResumeReplayRows;
    }

    /**
     * @return the live-view-writer seqTxn of an out-of-order repair's replacement
     * that committed but did not apply, or {@link Numbers#LONG_NULL} when nothing
     * is outstanding. See {@link #pendingReplacementLvSeqTxn}.
     */
    public long getPendingReplacementLvSeqTxn() {
        return pendingReplacementLvSeqTxn;
    }

    public long getRecordRowCopierMetadataVersion() {
        return recordRowCopierMetadataVersion;
    }

    public RecordToRowCopier getRecordToRowCopier() {
        return recordToRowCopier;
    }

    /**
     * @return the flag the refresh worker binds into its circuit breaker while it holds
     * this view's refresh latch, so a cancelled cycle throws out of whatever scan it is
     * in. Set by {@link #cancelRefresh()} and never cleared. See {@link #refreshCancelled}.
     */
    public AtomicBoolean getRefreshCancelledFlag() {
        return refreshCancelled;
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

    /**
     * @return the lifetime count of refresh cycles that threw. See {@link #refreshFaultCount}: a
     * fault that the job self-heals leaves no other trace, so this is the only way a test can tell a
     * view that refreshed incrementally from one that faulted and recomputed itself back to the same
     * answer.
     */
    public long getRefreshFaultCount() {
        return refreshFaultCount;
    }

    public long getRowsSinceLastCheckpointWritten() {
        return rowsSinceLastCheckpointWritten;
    }

    public TableReader getSeedBaseReader() {
        return seedBaseReader;
    }

    /**
     * @return the base data-cursor row offset of the newest seed boundary, or
     * {@link Numbers#LONG_NULL} when the sweep has sealed none. See
     * {@link #seedCheckpointDataOffset}.
     */
    public long getSeedCheckpointDataOffset() {
        return seedCheckpointDataOffset;
    }

    /**
     * @return the designated timestamp of the newest seed boundary, or
     * {@link Numbers#LONG_NULL} when the sweep has sealed none. See
     * {@link #seedCheckpointMaxTs}.
     */
    public long getSeedCheckpointMaxTs() {
        return seedCheckpointMaxTs;
    }

    public long getSeedDataOffset() {
        return seedDataOffset;
    }

    public long getSeedSkipWriteFloor() {
        return seedSkipWriteFloor;
    }

    public long getSeedSweepSeqTxn() {
        return seedSweepSeqTxn;
    }

    public LiveViewStateReader getStateReader() {
        return stateReader;
    }

    /**
     * @return the localized out-of-order repair parked between refresh turns, or
     * null when none is in flight. See {@link LiveViewCheckpointRepairSession}.
     */
    public LiveViewCheckpointRepairSession getSuspendedRepair() {
        return suspendedRepair;
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
        stateReader.setSeedState(source.getSeedState());
        stateReader.setSeedTargetSeqTxn(source.getSeedTargetSeqTxn());
    }

    /**
     * @return {@code true} while a deferred invalidation reason is stashed (set
     * by the head-checkpoint restore path when it cannot recover a consistent
     * window state - a failed replay-to-applied, a failed dedup replay, or a
     * version-too-old snapshot). The refresh worker peeks this after the restore
     * to skip the refresh + flush for the turn (which would materialise the
     * inconsistent accumulators to disk), then drains and applies the reason via
     * {@link #takePendingInvalidationReason} outside the refresh latch.
     */
    public boolean hasPendingInvalidationReason() {
        return pendingInvalidationReason != null;
    }

    public boolean hasWarnedBelowLowerBoundDrop() {
        return hasWarnedBelowLowerBoundDrop;
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
     * @return the {@code isLeadReconstruction()} value the refresh worker observed on
     * this view's previous cycle. See {@link #lastRefreshLeadReconstruction}.
     */
    public boolean isLastRefreshLeadReconstruction() {
        return lastRefreshLeadReconstruction;
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
     * @return {@code true} once the refresh worker has attempted to resume the
     * seed sweep from the timeline's newest root on the first turn of this
     * process (whether a resume point was found or not). Single-shot per
     * process; later turns continue from the in-memory window state + data
     * offset.
     */
    public boolean isSeedResumeAttempted() {
        return seedResumeAttempted;
    }

    /**
     * @return the cached AND of every compiled window function's
     * {@code supportsCheckpointState()}. Meaningful only when
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
     * @return {@code true} for a minimal, definition-less stub registered when the
     * catalogue load path could not load the on-disk files (a too-new format version,
     * or a torn / corrupt {@code _lv} / {@code _lv.s} with no recoverable state). Such
     * a stub is visible in the catalogue and droppable but never refreshes. See the
     * stub constructor and {@link #getLifecycleState()}.
     */
    public boolean isStub() {
        return stubState != null;
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
     * Writes invalidation fields into the in-memory state mirror. The caller is responsible
     * for rewriting {@code _lv.s} via {@link io.questdb.cairo.lv.LiveViewState#append} —
     * this method only updates the in-memory side.
     */
    public void markInvalid(@Nullable CharSequence reason, long invalidationTimestampUs) {
        stateReader.setInvalidationReason(reason);
        stateReader.setInvalidationTimestampUs(invalidationTimestampUs);
        stateReader.setInvalid(true);
        // An invalid view never refreshes again, so a cycle still running over it is
        // producing output nothing will keep. Cut it short rather than let it finish.
        cancelRefresh();
    }

    public void markAsDropped() {
        dropped = true;
        cancelRefresh();
    }

    /**
     * DROP side of the checkpoint/drop handshake, the counterpart to
     * {@link #startCheckpoint(long)}. Marks the view dropped and then waits out any
     * in-progress {@code DatabaseCheckpointAgent} freeze, both under the instance
     * monitor so the two interlock:
     * <ul>
     *     <li>if this runs first, a later {@link #startCheckpoint(long)} observes
     *     {@code dropped} under the same monitor and refuses the freeze (returns
     *     {@code false}), so the agent skips the view;</li>
     *     <li>if a freeze is already published, this parks in {@link #waitForUnfrozen()}
     *     until the agent's {@link #endCheckpoint()} clears it.</li>
     * </ul>
     * Once it returns, no checkpoint file copy for this view can be in flight, so the
     * caller may safely tear the view's files down. This settles only the checkpoint
     * race; the caller must still {@link #fenceRefresh()} afterwards to quiesce the
     * refresh worker.
     */
    public void markDroppedAndAwaitCheckpoint() {
        synchronized (this) {
            dropped = true;
            // Trip the breaker before waiting on anything: the fenceRefresh() that follows
            // this call spins until the in-flight cycle releases the latch, and an
            // unlocalized rebuild scanning a large base holds it for as long as that scan
            // takes. Cancelling first makes that cycle unwind at its next breaker
            // consultation rather than at the end of its scan.
            cancelRefresh();
            waitForUnfrozen();
        }
    }

    /**
     * Prepares the view for a recompile after the base table's metadata version
     * drifted from the cached compiled factory (a schema change that does not
     * touch referenced columns - those invalidate the view instead). Frees the
     * compiled-SQL artifacts so the next factory use ({@code ensureCompiledFactory})
     * recompiles them against the base table's current metadata. Window state
     * accumulated in the old factory's functions is lost with it; the caller
     * must rebuild it (head-miss replay, seed resume, or restart-restore)
     * before resuming incremental processing. The in-memory tier is deliberately
     * kept: the view's own projection is unchanged and reads keep serving
     * through it.
     * <p>
     * Must be called on the refresh worker under the refresh latch.
     */
    public void prepareForBaseSchemaRecompile() {
        // Before anything is freed. A parked repair borrowed the very window functions and
        // anchor window below, both to replay through and to hold the pre-repair state its
        // overlay took aside; a session outliving them would restore into freed objects, and
        // a resumed turn would continue a half-finished replay through a factory rebuilt at
        // identity. The snapshot the candidate pinned is still fine; the runtime its replay
        // was standing in is not, so the candidate goes and a later turn replans.
        discardSuspendedRepair();
        // Frees only the compiled artifacts, keeping the in-memory tier AND the per-view tracker:
        // the tier stays queryable and stays charged to the tracker across the recompile, and the
        // rebuilt factory recharges the same tracker (so the refresh memory limit still accounts
        // the surviving tier). Freeing the tracker here would strand the still-charged tier.
        freeCompiledArtifacts();
        // Drop the cached row copier with the factory it was built for. Its cache key is
        // the LV's own WAL metadata version, which a base-side schema change never moves,
        // so it would otherwise survive the recompile and copy the new factory's records
        // through the old factory's column layout.
        recordToRowCopier = null;
        recordRowCopierMetadataVersion = -1;
    }

    /**
     * Stamps the elapsed micros of the restart restore-from-head that just ran.
     * Single-shot per LV lifetime, called by the refresh worker after
     * {@code tryRestoreFromHead} returns (regardless of outcome). See
     * {@link #headCheckpointRestoreMicros}.
     */
    public void recordCheckpointRestoreMicros(long durationUs) {
        this.headCheckpointRestoreMicros = durationUs;
    }

    /** Releases this primary's local timeline WAL-retention ownership. */
    public void clearCheckpointTimelineOwnership() {
        checkpointTimelineWalPurgeFloor = Numbers.LONG_NULL;
    }

    /**
     * Forgets the newest seed boundary, putting the seed cadence back on its
     * first-checkpoint trigger. Called when the sweep retires the boundaries it
     * sealed - on completion, and when a resume is abandoned for a re-sweep from
     * offset zero.
     */
    public void clearSeedCheckpoint() {
        this.seedCheckpointDataOffset = Numbers.LONG_NULL;
        this.seedCheckpointMaxTs = Numbers.LONG_NULL;
        this.lastCheckpointWrittenUs = Numbers.LONG_NULL;
    }

    /**
     * Publishes the WAL floor derived from a successfully committed timeline
     * generation, or adopts the same durable value during startup. Callers must
     * never pass a candidate computed before the superblock commit point.
     */
    public void recordCheckpointTimelineWalPurgeFloor(long walPurgeFloor) {
        if (walPurgeFloor < 0) {
            throw CairoException.critical(0)
                    .put("live view checkpoint timeline WAL floor must be non-negative");
        }
        checkpointTimelineWalPurgeFloor = walPurgeFloor;
    }

    /**
     * Stamps the elapsed micros of the head-checkpoint write that just completed.
     * Called by the refresh worker at the tail of {@code maybeWriteHeadCheckpoint}
     * after the timeline generation is published. See
     * {@link #headCheckpointWriteMicros}.
     */
    public void recordCheckpointWriteMicros(long durationUs) {
        this.headCheckpointWriteMicros = durationUs;
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
     * Records that a refresh cycle threw, whatever the job goes on to do about it (self-heal,
     * recompile, back off, or invalidate). Separate from {@link #recordRefreshFailure(long)}, which
     * several recovery paths deliberately never reach. See {@link #refreshFaultCount}.
     */
    public void recordRefreshFault() {
        refreshFaultCount++;
    }

    /**
     * Resets the consecutive-failure counter and the streak start. Called after each
     * successful refresh cycle so the retry budget is per-streak, not lifetime. Also
     * clears any armed apply-lag defer floor: a cycle that drained cleanly proves the
     * transient base-apply lag has passed, so the pre-latch throttle in
     * {@link io.questdb.cairo.lv.LiveViewRefreshJob#refreshInstance} should stop
     * short-circuiting this view.
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
        applyLagDeferUntilUs = Numbers.LONG_NULL;
        applyLagDeferTargetSeqTxn = Numbers.LONG_NULL;
    }

    /**
     * Records a sealed seed boundary: stamps the sweep's base data-cursor row
     * offset, the boundary timestamp, and the write time. The seed cadence keys
     * off the offset delta (not {@link #rowsSinceLastCheckpointWritten}, which
     * the steady head owns), so this does not touch the steady head metadata or
     * the steady cadence counter.
     * <p>
     * A resume passes {@link Numbers#LONG_NULL} for {@code writtenUs}: the
     * restored boundary was sealed by an earlier process, so the duration
     * trigger has no baseline until this one seals its own.
     */
    public void recordSeedCheckpointWritten(long dataOffset, long maxTs, long writtenUs) {
        this.seedCheckpointDataOffset = dataOffset;
        this.seedCheckpointMaxTs = maxTs;
        this.lastCheckpointWrittenUs = writtenUs;
    }

    /**
     * Clears the single-shot restart-restore flag so the refresh worker re-runs the
     * first-cycle recovery. Used only on an in-process promote whose applied floor lags
     * the LV table (a swallowed replica {@code _lv.s} persist): the recovery reconciles
     * the floor to disk truth and rebuilds the window state from the applied tier.
     * Mutated under the refresh latch only.
     */
    public void resetCheckpointRestoreAttempted() {
        checkpointRestoreAttempted = false;
    }

    /**
     * Re-arms the seed sweep's single-shot resume setup (see
     * {@link #isSeedResumeAttempted()}). Called by the refresh worker after
     * {@link #prepareForBaseSchemaRecompile()} on a SEEDING view so the next
     * sweep turn restores window state and the data offset from the timeline's
     * newest root against the recompiled factory, or re-sweeps from offset 0
     * behind the skip-write floor. Mutated under the refresh latch only.
     */
    public void resetSeedResumeAttempted() {
        seedResumeAttempted = false;
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

    public void setApplyLagDeferTargetSeqTxn(long applyLagDeferTargetSeqTxn) {
        this.applyLagDeferTargetSeqTxn = applyLagDeferTargetSeqTxn;
    }

    public void setApplyLagDeferUntilUs(long applyLagDeferUntilUs) {
        this.applyLagDeferUntilUs = applyLagDeferUntilUs;
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
     * refresh worker calls this only when the window state was rehydrated from
     * a checkpoint timeline root.
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
     * Records a committed head checkpoint in one atomic store. Mirrors the
     * head metadata into the {@code live_views()} catalogue, resets the
     * cadence counter ({@link #rowsSinceLastCheckpointWritten} back to zero),
     * and stamps {@link #lastCheckpointWrittenUs}. Called by the flush-cycle
     * write hook after the timeline generation carrying the new root is
     * published.
     * <p>
     * Passing {@code Numbers.LONG_NULL} for {@code lvSeqTxn} clears the head
     * (e.g. when an out-of-order change retires the timeline); cadence
     * counters reset too so the next eligible cycle seals a fresh root
     * immediately rather than waiting for the row-count trigger to re-fire.
     */
    public void setHeadCheckpoint(long lvSeqTxn, long baseSeqTxn, long maxTs, long stateBytes, long writtenUs) {
        // Publish the (lvSeqTxn, maxTs, stateBytes, baseSeqTxn) tuple atomically:
        // build a fresh immutable array and store the reference volatile. A reader
        // observing the new reference is guaranteed to see all fields from the same
        // setHeadCheckpoint call, never a torn mix.
        this.headCheckpoint = new long[]{lvSeqTxn, maxTs, stateBytes, baseSeqTxn};
        this.rowsSinceLastCheckpointWritten = 0;
        this.lastCheckpointWrittenUs = writtenUs;
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

    public void setLastRefreshLeadReconstruction(boolean leadReconstruction) {
        this.lastRefreshLeadReconstruction = leadReconstruction;
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
     * Sets the read-only-replica O3 catch-up floor. See {@link #leadO3CatchupSeqTxn}.
     */
    public void setLeadO3CatchupSeqTxn(long leadO3CatchupSeqTxn) {
        this.leadO3CatchupSeqTxn = leadO3CatchupSeqTxn;
    }

    /**
     * Sets the durable tie count at the lead catch-up seam. See {@link #leadReconcileSeamDurableTies}.
     */
    public void setLeadReconcileSeamDurableTies(long leadReconcileSeamDurableTies) {
        this.leadReconcileSeamDurableTies = leadReconcileSeamDurableTies;
    }

    /**
     * Sets the read-only-replica lead catch-up seam. See {@link #leadReconcileSeamTs}.
     */
    public void setLeadReconcileSeamTs(long leadReconcileSeamTs) {
        this.leadReconcileSeamTs = leadReconcileSeamTs;
    }

    /**
     * Hands the view its per-view {@link MemoryTracker}. The refresh worker acquires it
     * before the window machinery exists, so the maps allocate against it from their first
     * byte. See {@link #memoryTracker}.
     */
    public void setMemoryTracker(@Nullable MemoryTracker memoryTracker) {
        this.memoryTracker = memoryTracker;
    }

    /**
     * Sets the read-only-replica publish-stall retry floor. See {@link #leadRetryAfterUs}.
     */
    public void setLeadRetryAfterUs(long leadRetryAfterUs) {
        this.leadRetryAfterUs = leadRetryAfterUs;
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

    /**
     * Arms (or, with {@link Numbers#LONG_NULL}, clears) the reconciliation block a
     * repair leaves behind when its replacement committed without applying. See
     * {@link #pendingReplacementLvSeqTxn}.
     */
    public void setPendingReplacementLvSeqTxn(long lvSeqTxn) {
        this.pendingReplacementLvSeqTxn = lvSeqTxn;
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

    public void setSeedBaseReader(TableReader seedBaseReader) {
        this.seedBaseReader = seedBaseReader;
    }

    public void setSeedDataOffset(long seedDataOffset) {
        this.seedDataOffset = seedDataOffset;
    }

    /**
     * Single-shot setter for {@link #isSeedResumeAttempted()}. The refresh
     * worker calls this on the first seed turn of the process, regardless
     * of whether a resume point was found.
     */
    public void setSeedResumeAttempted() {
        this.seedResumeAttempted = true;
    }

    public void setSeedSkipWriteFloor(long seedSkipWriteFloor) {
        this.seedSkipWriteFloor = seedSkipWriteFloor;
    }

    public void setSeedState(byte seedState) {
        stateReader.setSeedState(seedState);
    }

    public void setSeedSweepSeqTxn(long seedSweepSeqTxn) {
        this.seedSweepSeqTxn = seedSweepSeqTxn;
    }

    public void setSeedTargetSeqTxn(long seedTargetSeqTxn) {
        stateReader.setSeedTargetSeqTxn(seedTargetSeqTxn);
    }

    /**
     * Caches the AND of every compiled window function's
     * {@code supportsCheckpointState()}, evaluated once after the LV's compiled
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
     * Parks a localized out-of-order repair that yielded on its turn budget, or
     * clears the one that has ended. Refresh for this view is blocked while a
     * session is parked, and only the refresh job the session names may continue
     * it. Called under the refresh latch.
     */
    public void setSuspendedRepair(@Nullable LiveViewCheckpointRepairSession suspendedRepair) {
        this.suspendedRepair = suspendedRepair;
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

    public void setWarnedBelowLowerBoundDrop() {
        this.hasWarnedBelowLowerBoundDrop = true;
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
     * <p>
     * Agent side of the checkpoint/drop handshake: returns {@code false} without
     * freezing when a concurrent DROP has already marked the view dropped (see
     * {@link #markDroppedAndAwaitCheckpoint()}). The caller must then skip the view.
     *
     * @return {@code true} if the freeze was published (pair with
     * {@link #endCheckpoint()}); {@code false} if the view is being dropped and the
     * caller must skip it (no {@code endCheckpoint()} is owed).
     */
    public boolean startCheckpoint(long frozenAppliedWatermark) {
        // Synchronize on the instance monitor while publishing the flag so any
        // invalidator inside synchronized(instance) on another thread either
        // (a) commits its rewrite before the agent's file copy begins, or
        // (b) observes freezeInProgress=true and parks via waitForUnfrozen().
        synchronized (this) {
            if (dropped) {
                // Checkpoint/drop handshake, agent side. A concurrent DROP LIVE VIEW has
                // already marked this instance dropped (under this same monitor, in
                // markDroppedAndAwaitCheckpoint) and is about to tear its files down.
                // Refuse the freeze so the agent skips the view instead of copying a
                // directory that is about to vanish - do NOT set freezeInProgress, or
                // the DROP would park forever waiting for an endCheckpoint the agent will
                // never issue for a view it skipped.
                return false;
            }
            freezeFrozenAppliedWatermark = frozenAppliedWatermark;
            freezeInProgress = true;
        }
        while (!refreshLatch.compareAndSet(false, true)) {
            Os.pause();
        }
        refreshLatch.set(false);
        return true;
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
                discardSuspendedRepair();
                freeSeedBaseReader();
                freeCachedRefreshState();
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
            discardSuspendedRepair();
            freeSeedBaseReader();
            freeCachedRefreshState();
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
     * waiters once the freeze clears. The wait is uninterruptible because returning
     * early would let the caller race its durable-state rewrite against the checkpoint
     * copy; the method restores interrupt status after the freeze clears.
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
        boolean isInterrupted = false;
        while (freezeInProgress) {
            try {
                wait();
            } catch (InterruptedException e) {
                isInterrupted = true;
            }
        }
        if (isInterrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Full-teardown free in the one order {@link #memoryTracker} tolerates. The in-memory tier
     * AND the compiled artifacts (the factory's per-partition function maps, the anchor map) all
     * charge the tracker, so all must release before it is closed. Closing the tracker with a
     * non-zero balance returns it to the pool dirty, and PerQueryMemoryTracker.init() then trips
     * its recycle assert in whichever unrelated query next acquires it. Every FULL teardown path
     * (drop, invalidate, runtime-state free) routes through here, so the order is stated once; a
     * base-schema recompile frees only the artifacts (see {@link #freeCompiledArtifacts}).
     */
    private void freeCachedRefreshState() {
        inMemoryTier = Misc.free(inMemoryTier);
        freeCompiledArtifacts();
        memoryTracker = Misc.free(memoryTracker);
    }

    /**
     * Frees the compiled-SQL artifacts that charge the per-view {@link #memoryTracker}: the
     * factory's per-partition function maps and the anchor window's anchor map. Does NOT free the
     * tracker or the in-memory tier, so {@link #prepareForBaseSchemaRecompile} can drop and
     * rebuild the factory while the tier keeps serving and the tracker keeps accounting the tier's
     * retained footprint (the next factory recharges the same tracker).
     */
    private void freeCompiledArtifacts() {
        compiledFactory = Misc.free(compiledFactory);
        anchorWindow = Misc.free(anchorWindow);
        anchorFunction = Misc.free(anchorFunction);
    }

}
