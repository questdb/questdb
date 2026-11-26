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

package io.questdb.cairo.mv;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.file.AppendableBlock;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.RecordToRowCopier;
import io.questdb.griffin.engine.table.SelectedRecord;
import io.questdb.std.Chars;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static io.questdb.TelemetryEvent.*;

/**
 * Mat view refresh state serves the purpose of synchronizing and coordinating
 * {@link MatViewRefreshJob}s.
 * <p>
 * The invalidation reason mirrors the last durable state and is retained while a full recovery is in
 * progress. The {@link #pendingInvalidationMarker} is a transient in-memory marker
 * with two facets: a deferred invalidation (reason plus optional base-table txn
 * provenance) and a pending full-refresh owner. Either facet can be present alone or
 * combined on one marker; publications merge facets keep-strongest. A lock-holder's
 * finalize wakes whichever facets remain after its hold, and each facet is cleared only
 * by the operation that consumes it (an invalid-state mint, or a covering or terminal
 * full refresh). It is distinct from the reader's durable invalidationReason.
 */
public class MatViewState implements QuietCloseable {
    // The base table id of a view state that carries none: a state that a previous version wrote, or a new view's
    // before its first refresh. A view with an unknown base id skips the base identity check.
    public static final int BASE_TABLE_ID_UNKNOWN = -1;
    // Cold-start gap-width threshold used when no scan/commit samples have
    // been observed yet. Expressed as a wall-clock duration in microseconds
    // and converted to the base table's timestamp unit at read time, so a
    // fresh us-base view and a fresh ns-base view both bias toward merging
    // gaps under the same wall-clock equivalent -- avoiding the 1000x
    // behavioural gap that a single ts-unit-valued constant would create
    // between us and ns bases for the one-refresh warmup window before the
    // EMA-derived threshold takes over.
    public static final long COLD_START_GAP_THRESHOLD_MICROS = 2_000_000L;
    public static final String MAT_VIEW_STATE_FILE_NAME = "_mv.s";
    // Base table identity and backlog: the id of the base table that the refresh position and the cached refresh
    // intervals belong to, followed by the backlog-lost marker, the cause of a lost backlog or null. Written after the
    // frozen-zone block; absent in state files that a previous version wrote, so readers must default the id to
    // BASE_TABLE_ID_UNKNOWN and the marker to null.
    public static final int MAT_VIEW_STATE_FORMAT_EXTRA_BASE_TABLE_MSG_TYPE = 5;
    // Frozen-zone runtime state: backfill frontier high-water mark + last published
    // boundary floor. Written after the intervals block; absent in state files
    // produced before the frozen-zone feature, so readers must default it.
    public static final int MAT_VIEW_STATE_FORMAT_EXTRA_FROZEN_MSG_TYPE = 4;
    public static final int MAT_VIEW_STATE_FORMAT_EXTRA_INTERVALS_MSG_TYPE = 3;
    public static final int MAT_VIEW_STATE_FORMAT_EXTRA_PERIOD_MSG_TYPE = 2;
    public static final int MAT_VIEW_STATE_FORMAT_EXTRA_TS_MSG_TYPE = 1;
    public static final int MAT_VIEW_STATE_FORMAT_MSG_TYPE = 0;
    // EMA smoothing inverse: new = (prev * (N-1) + sample) / N. Larger N is
    // smoother but slower to adapt. 8 gives ~6 samples to reach halfway to a
    // new steady state, which is a reasonable balance.
    static final int EMA_ALPHA_INV = 8;
    // Hard cap on incoming sample relative to current average, to prevent a
    // single outlier (GC pause, O3 partition rewrite) from poisoning the EMA
    // for the next several refreshes.
    static final int EMA_OUTLIER_MULTIPLIER = 5;
    static final int PENDING_TASK_RETRY_FULL_REFRESH = 1;
    static final int PENDING_TASK_RETRY_INCREMENTAL_REFRESH = 4;
    static final int PENDING_TASK_RETRY_INVALIDATION = 2;
    // Enables atomic ownership transfers of the pending-invalidation marker. A fresh marker object
    // identifies each reason publication, so a refresh can clear only the exact invalidation its
    // snapshot covered without erasing a newer publication carrying the same reason string.
    private static final AtomicReferenceFieldUpdater<MatViewState, Object> PENDING_INVALIDATION_MARKER_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(MatViewState.class, Object.class, "pendingInvalidationMarker");
    private static final AtomicIntegerFieldUpdater<MatViewState> PENDING_TASK_RETRY_FLAGS_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(MatViewState.class, "pendingTaskRetryFlags");
    // Enables an off-latch CAS on refreshRetryAfterMicros so MatViewTimerJob can clear only the
    // exact deadline it observed due, without clobbering a fresh backoff a concurrent under-latch
    // refresh may have just armed. See clearRefreshRetry(long).
    private static final AtomicLongFieldUpdater<MatViewState> REFRESH_RETRY_AFTER_UPDATER =
            AtomicLongFieldUpdater.newUpdater(MatViewState.class, "refreshRetryAfterMicros");
    // Backfill frontier high-water mark in the base table's timestamp driver units:
    // the largest boundary anchor (min(max(base_ts), now)) any accepted user backfill was
    // validated against. Advanced lock-free from WAL commit threads via
    // advanceBackfillFrontier; read by MatViewRefreshJob, which folds it into the refresh
    // boundary anchor so a retreating max(base_ts) cannot wipe an already-accepted
    // backfill. Long.MIN_VALUE until the first backfill is accepted. Persisted to the
    // _mv.s state file (block type MAT_VIEW_STATE_FORMAT_EXTRA_FROZEN_MSG_TYPE) by the
    // apply path when a backfill advances it, and restored on restart via initFromReader.
    // Persistence is required because max(view_ts) cannot protect a pre-first-refresh
    // backfill (the backfilled row IS the view's max), so without it a restart in the
    // bootstrap window would let a retreating max(base_ts) pull the boundary back over an
    // already-accepted backfill; see MatViewRefreshJob.findRefreshIntervals.
    private final AtomicLong backfillFrontier = new AtomicLong(Long.MIN_VALUE);
    // Used to avoid concurrent refresh runs.
    private final AtomicBoolean latch = new AtomicBoolean(false);
    // Serializes the writers of bareNameSwitches: the WAL apply threads of the view's base tables.
    private final Object bareNameSwitchesLock = new Object();
    // Serializes the writes of appliedDefinitionSeqTxn and hasDeferredDefinitionRefresh, so that a refresh judges a
    // definition pending and registers its request in one step against the raise of the applied seqTxn and the claim,
    // see requestDeferredDefinitionRefresh().
    private final Object definitionLock = new Object();
    // The highest pending definition seqTxn whose wait the refresh job logged, or -1, see claimDefinitionWaitLog().
    private final AtomicLong definitionWaitLoggedSeqTxn = new AtomicLong(-1);
    // The sequencer txn of the MAT_VIEW_DEFINITION event of the last ALTER MATERIALIZED VIEW ... AS that this node ran,
    // or of the newest one whose event the view's WAL held committed and unapplied when the node booted, or -1.
    // REFRESH ... FULL, INCREMENTAL and RANGE wait until the view's WAL is applied up to it, so that they run the new
    // SQL, and so do view ADD COLUMN, DROP COLUMN and ALTER ... AS, so that they check themselves against it. The
    // refresh job defers while the view's WAL has not applied or skipped the event, see isDefinitionPending(). Kept in
    // memory: the boot load finds an unapplied event again in the view's WAL; a promote's reload does not, see
    // CairoEngine.buildViewGraphs().
    private final AtomicLong pendingDefinitionSeqTxn = new AtomicLong(-1);
    // A full refresh task that lost the view lock left its owner's delivery to the lock holder: the
    // holder's release claims the handoff and enqueues the pending owner, even an owner the holder
    // itself deferred. See MatViewRefreshJob#tryLockForFullRefresh.
    private final AtomicBoolean pendingFullRefreshHandoff = new AtomicBoolean(false);
    // An incremental refresh that lost the view lock left its run to the lock holder: the holder's
    // release claims the request and enqueues one incremental refresh. See
    // MatViewRefreshJob#tryLockForIncrementalRefresh.
    private final AtomicBoolean pendingIncrementalRefresh = new AtomicBoolean(false);
    // Protected by this.latch.
    // Holds cached txn intervals read from WAL transactions (_event files) of the base table.
    // Lets WalPurgeJob to make progress and delete applied WAL segments of a base table without
    // having to wait for all dependent mat views to be refreshed.
    private final LongList refreshIntervals = new LongList();
    // Incremented each time there's a base table transaction(s).
    // Used by MatViewTimerJob to avoid queueing redundant WAL txn intervals caching tasks.
    private final AtomicLong refreshIntervalsSeq = new AtomicLong();
    // Incremented each time an incremental or full refresh task finishes, whether or not it refreshed
    // anything. The one exception is an incremental task that loses the view lock: the lock holder's release
    // enqueues the view's next incremental refresh, which increments it when it finishes. MatViewTimerJob
    // reads it to tell whether the refresh task it enqueued has left the queue.
    private final AtomicLong refreshSeq = new AtomicLong();
    private final MatViewTelemetryFacade telemetryFacade;
    // The sequencer txn up to which this node's WAL apply of the view has applied or skipped every definition event, or
    // -1: the WAL apply job raises it as it applies a MAT_VIEW_DEFINITION event or skips it as stale, and RESUME WAL
    // FROM TXN as it skips the view's WAL past one, see recordAppliedDefinitionSeqTxn(). It is not the writer txn of the
    // view's SeqTxnTracker: a republish resets that to -1 long after the event applied, and RESUME WAL FROM TXN moves it
    // only with the apply run that follows. Written under definitionLock, never lowered, kept in memory only.
    private volatile long appliedDefinitionSeqTxn = -1;
    // Exponential moving average of one REPLACE_RANGE commit, in nanoseconds.
    // Used together with the scan-sample/scan-range EMAs to derive a
    // timestamp-width threshold below which clustering two adjacent cached
    // refresh intervals is cheaper than paying for an extra commit.
    // Writes protected by this.latch; reads may be racy (used for display
    // and cost-model decisions, never for correctness).
    private volatile long avgCommitNanos;
    // Exponential moving average of the timestamp range width covered by a
    // single refresh iteration, in the base table's timestamp unit (us for
    // TIMESTAMP, ns for TIMESTAMP_NS). Paired with avgScanSampleNanos: the
    // cost model derives a scan-ns-per-ts-unit rate at read time
    // (avgScanSampleNanos / avgScanRangeTsUnits) rather than smoothing the
    // per-sample ratio, so a fast scan over a wide range (the common case
    // on TIMESTAMP_NS bases, where the two share magnitude) keeps its full
    // precision instead of flooring to zero. Same locking discipline as
    // avgCommitNanos.
    private volatile long avgScanRangeTsUnits;
    // Exponential moving average of the wall-clock duration of a single
    // refresh iteration's base-table scan, in nanoseconds. See
    // avgScanRangeTsUnits.
    private volatile long avgScanSampleNanos;
    // The cause of a lost backlog, or null while the view's backlog is tracked, see MatViewBacklog. An invalid view
    // keeps recording its base table's changes in its refresh-interval cache until the backlog is lost. Written under
    // this.latch and by the load, cleared when the view becomes valid; persisted with every view state event.
    private volatile String backlogLostReason;
    // The bare-name switches that the bare-name guard recorded on this view, see recordBareNameSwitch(): one entry per
    // name and base table. Writers publish a new list under bareNameSwitchesLock and never change a published one, so
    // RESUME reads it without a lock. Kept in memory only.
    private volatile ObjList<BareNameSwitch> bareNameSwitches = new ObjList<>();
    // The id of the base table that the refresh position and the cached refresh intervals belong to, or
    // BASE_TABLE_ID_UNKNOWN. Written under this.latch by incremental refresh (when unknown), FULL and RESUME, and
    // by the load; read under the latch by the base identity check of every refresh except FULL.
    private volatile int baseTableId = BASE_TABLE_ID_UNKNOWN;
    // What the column mapping that the last compile of the view's SQL built leaves out, for materialized_views():
    // null before the first such compile in this process and after one that failed. Refresh writes it under the
    // latch, view ADD COLUMN's check without it, and the catalogue reads it, so it is one volatile reference to an
    // immutable summary. Kept in memory only.
    private volatile MatViewColumnMapping.Summary columnMappingSummary;
    // Set once the owner store tears this state down (e.g. a role demote frees
    // the discarded store). A refresh worker that holds the latch across the
    // teardown consults this in the *refreshSuccess* park methods: rather than
    // parking the live native cursorFactory back into a discarded state (which
    // would leak it, the state is unreachable), it closes the factory it holds.
    // close() reads it under the latch to free the parked factory exactly once.
    private volatile boolean closed;
    // Protected by this.latch.
    private RecordCursorFactory cursorFactory;
    private long cursorFactoryExpiryPolicyVersion = -1;
    // Protected by this.latch.
    // The SQL text the cached factory was compiled from. ALTER MATERIALIZED VIEW ... AS replaces the view's SQL, and
    // a refresh that finds a factory of another SQL discards it, see acquireRecordFactory(String).
    private String cursorFactorySql;
    private volatile boolean dropped;
    // Set by a refresh that deferred on a pending definition, cleared by the claim that enqueues one incremental refresh
    // for every refresh that deferred since, see claimDeferredDefinitionRefresh(). Written under definitionLock.
    private volatile boolean hasDeferredDefinitionRefresh;
    private volatile boolean invalid;
    private volatile String invalidationReason;
    // Set under this.latch while REFRESH MATERIALIZED VIEW ... RESUME recovers the view. An invalidation published
    // while it is set is kept as a pending marker, as during a pending FULL refresh, so that the release of the
    // latch re-drives it once RESUME has made the view valid.
    private volatile boolean isResuming;
    // Protected by this.latch.
    // Whether the cached factory's SQL relies on generated column names, so the cached copier, and one rebuilt
    // for the factory after a view schema change, copies by position; see MatViewColumnMapping.
    private boolean isRecordCopyPositional;
    // Atomic token/txn pair covered by the last successful FULL refresh. This is intentionally
    // independent of lastRefreshBaseTxn: incremental and range refreshes cannot repair arbitrary
    // UPDATE, TRUNCATE, or schema invalidations even when they advance another watermark.
    private volatile FullRefreshCoverage lastFullRefreshCoverage;
    // Stands for the end of the last refreshed period of a period mat view, exclusive. Refreshes move it forward
    // as periods complete. An incremental refresh moves it back when the last complete period ends earlier, e.g.
    // after ALTER MATERIALIZED VIEW ... PERIOD or a clock step back.
    private volatile long lastPeriodHi = Numbers.LONG_NULL;
    // Stands for last successful incremental refresh base table reader txn.
    // Increases monotonically as long as mat view stays valid.
    private volatile long lastRefreshBaseTxn = -1;
    // The error of the last user REFRESH ... RANGE that failed, or null: a failed user RANGE leaves the view valid,
    // and materialized_views() reports its error as last_refresh_error until the next user RANGE succeeds. The
    // refresh job writes it under this.latch and the catalogue reads it, so it is one volatile reference to an
    // immutable string. Kept in memory only.
    private volatile String lastRefreshError;
    private volatile long lastRefreshFinishTimestampUs = Numbers.LONG_NULL;
    // Snapped REPLACE_RANGE.lo (the bucket floor) that the most recent
    // REFRESH LIMIT-bounded refresh tick computed and committed to. The
    // backfill validator and materialized_views().backfill_max_ts read this
    // so their accepted frozen zone never overlaps refresh's coverage. Stays
    // at LONG_NULL until the first refresh tick that runs under a non-zero
    // REFRESH LIMIT publishes a floor; from then on it advances monotonically
    // (modulo ALTER SET REFRESH LIMIT, which can move the floor in either
    // direction on the next refresh tick). Driver units (base table's
    // timestamp driver). Publishing the snapped floor -- not the raw anchor --
    // is what closes the SHRINK-LIMIT race: refresh's REPLACE_RANGE.lo is the
    // value that survives an ALTER, so the validator clamps to it directly
    // instead of re-applying the (now-different) LIMIT.
    private volatile long lastRefreshFrozenBoundaryFloor = Numbers.LONG_NULL;
    private volatile int lastRefreshFrozenBoundaryLimitHoursOrMonths;
    private volatile long lastRefreshStartTimestampUs = Numbers.LONG_NULL;
    @TestOnly
    private volatile Runnable onAlterValidatedForTesting;
    @TestOnly
    private volatile Runnable onBareNameSwitchCopiedForTesting;
    @TestOnly
    private volatile Runnable onBareNameSwitchRecordedForTesting;
    @TestOnly
    private volatile Runnable onDeferredDefinitionClaimForTesting;
    @TestOnly
    private volatile Runnable onDeferredDefinitionRequestForTesting;
    @TestOnly
    private volatile Runnable onDefinitionEventWrittenForTesting;
    @TestOnly
    private volatile Runnable onDefinitionWaitForTesting;
    @TestOnly
    private volatile Runnable onBeforeUnlockForTesting;
    @TestOnly
    private volatile Runnable onClearPendingFullRefreshForTesting;
    @TestOnly
    private volatile Runnable onPendingFullRefreshHandoffRequestForTesting;
    @TestOnly
    private volatile Runnable onPendingFullRefreshMarkerReadForTesting;
    @TestOnly
    private volatile Runnable onPendingIncrementalRefreshRequestForTesting;
    @TestOnly
    private volatile Runnable onPendingInvalidationMarkerReadForTesting;
    @TestOnly
    private volatile Runnable onResumeFlagSetForTesting;
    @TestOnly
    private volatile Runnable onResumeHoldingLockForTesting;
    @TestOnly
    private volatile Runnable onResumeLatchBusyForTesting;
    @TestOnly
    private volatile Runnable onResumeWalAppliedForTesting;
    @TestOnly
    private volatile Runnable onResumeWalWaitForTesting;
    // Single atomic marker for a deferred invalidation, collapsing the former
    // (pendingInvalidation, pendingInvalidationReason) two-volatile composite. Writers publish
    // off-latch before attempting the view latch; holders inspect the same atomic reference after
    // releasing it. One volatile reference makes every transition a single atomic write/read:
    //   null                             -> not pending
    //   PendingInvalidation(null, ..., owner) -> pending full-refresh reschedule
    //   PendingInvalidation(reason, ..., *)   -> pending invalidation, optionally also a full refresh
    // Every reason publication gets a distinct object identity, allowing a latch-holder to clear only
    // the exact marker it owns. Reason publications coalesce their base token/txn provenance atomically;
    // unknown or mismatched epochs dominate. A full-refresh publication gets a separate owner identity
    // that survives reason replacement and invalidation success, so terminal cleanup cannot erase a newer
    // full request.
    private volatile Object pendingInvalidationMarker;
    // Facets whose queue publication failed after their marker became authoritative. A refresh-job tick
    // claims these bits and retries only the failed facets, avoiding duplicate queue amplification when
    // one facet succeeded before another allocation failed.
    private volatile int pendingTaskRetryFlags;
    // Highest backfillFrontier value already written to the _mv.s state file. Lets the
    // backfill apply path skip a redundant state-file rewrite when no new backfill has
    // advanced the frontier since the last persist. Mutated only from the (single-threaded
    // per table) WAL apply path and seeded from disk in initFromReader.
    private long persistedBackfillFrontier = Long.MIN_VALUE;
    // Protected by this.latch.
    private long recordRowCopierMetadataVersion;
    // Protected by this.latch.
    private RecordToRowCopier recordToRowCopier;
    // Protected by this.latch.
    // Base table txn that corresponds to refreshIntervals.
    private volatile long refreshIntervalsBaseTxn = -1;
    // Wall-clock micros before which the view must not be refreshed after a transient failure
    // (e.g. base table reader pool exhausted). The view stays valid; MatViewTimerJob re-drives an
    // incremental refresh once the deadline elapses. Numbers.LONG_NULL means no pending retry.
    private volatile long refreshRetryAfterMicros = Numbers.LONG_NULL;
    // Number of consecutive transient refresh failures that were deferred without an intervening
    // success. MatViewRefreshJob invalidates the view once this exceeds the configured limit, which
    // releases base-table WAL retention. Mutated only under this.latch, so a plain increment is safe.
    private volatile int refreshRetryCount = 0;
    // Number of timers MatViewTimerJob currently holds for this view: the incremental refresh timer,
    // the period timer and the refresh intervals update timer, as the definition calls for.
    // materialized_views() publishes it as timers_registered. An immediate, non-period view
    // legitimately owns none, but a timer, period or manual view sitting at zero is scheduled by
    // nothing at all, and no other surface reveals that -- view_status keeps reporting 'valid'.
    // MatViewTimerJob is the only writer and SynchronizedJob serializes its ticks, so the
    // read-modify-write in addRegisteredTimers() needs no atomic.
    private volatile int registeredTimerCount;
    private volatile MatViewDefinition viewDefinition;

    public MatViewState(
            @NotNull MatViewDefinition viewDefinition,
            MatViewTelemetryFacade telemetryFacade
    ) {
        this.viewDefinition = viewDefinition;
        this.telemetryFacade = telemetryFacade;
    }

    public static void append(
            long lastRefreshTimestamp,
            long lastRefreshBaseTxn,
            boolean invalid,
            @Nullable CharSequence invalidationReason,
            long lastPeriodHi,
            @Nullable LongList refreshIntervals,
            long refreshIntervalsBaseTxn,
            long backfillFrontier,
            long frozenBoundaryFloor,
            int frozenBoundaryLimitHoursOrMonths,
            @NotNull BlockFileWriter writer
    ) {
        append(
                lastRefreshTimestamp,
                lastRefreshBaseTxn,
                invalid,
                invalidationReason,
                lastPeriodHi,
                refreshIntervals,
                refreshIntervalsBaseTxn,
                backfillFrontier,
                frozenBoundaryFloor,
                frozenBoundaryLimitHoursOrMonths,
                BASE_TABLE_ID_UNKNOWN,
                null,
                writer
        );
    }

    public static void append(
            long lastRefreshTimestamp,
            long lastRefreshBaseTxn,
            boolean invalid,
            @Nullable CharSequence invalidationReason,
            long lastPeriodHi,
            @Nullable LongList refreshIntervals,
            long refreshIntervalsBaseTxn,
            long backfillFrontier,
            long frozenBoundaryFloor,
            int frozenBoundaryLimitHoursOrMonths,
            int baseTableId,
            @Nullable CharSequence backlogLostReason,
            @NotNull BlockFileWriter writer
    ) {
        AppendableBlock block = writer.append();
        appendState(lastRefreshBaseTxn, invalid, invalidationReason, block);
        block.commit(MAT_VIEW_STATE_FORMAT_MSG_TYPE);
        block = writer.append();
        appendTs(lastRefreshTimestamp, block);
        block.commit(MAT_VIEW_STATE_FORMAT_EXTRA_TS_MSG_TYPE);
        block = writer.append();
        appendPeriodHi(lastPeriodHi, block);
        block.commit(MAT_VIEW_STATE_FORMAT_EXTRA_PERIOD_MSG_TYPE);
        block = writer.append();
        appendRefreshIntervals(refreshIntervals, refreshIntervalsBaseTxn, block);
        block.commit(MAT_VIEW_STATE_FORMAT_EXTRA_INTERVALS_MSG_TYPE);
        block = writer.append();
        appendFrozenZoneState(backfillFrontier, frozenBoundaryFloor, frozenBoundaryLimitHoursOrMonths, block);
        block.commit(MAT_VIEW_STATE_FORMAT_EXTRA_FROZEN_MSG_TYPE);
        block = writer.append();
        appendBaseTable(baseTableId, backlogLostReason, block);
        block.commit(MAT_VIEW_STATE_FORMAT_EXTRA_BASE_TABLE_MSG_TYPE);
        writer.commit();
    }

    // refreshState can be null, in this case "default" record will be written
    public static void append(@Nullable MatViewStateReader refreshState, @NotNull BlockFileWriter writer) {
        if (refreshState != null) {
            append(
                    refreshState.getLastRefreshTimestampUs(),
                    refreshState.getLastRefreshBaseTxn(),
                    refreshState.isInvalid(),
                    refreshState.getInvalidationReason(),
                    refreshState.getLastPeriodHi(),
                    refreshState.getRefreshIntervals(),
                    refreshState.getRefreshIntervalsBaseTxn(),
                    refreshState.getBackfillFrontier(),
                    refreshState.getFrozenBoundaryFloor(),
                    refreshState.getFrozenBoundaryLimitHoursOrMonths(),
                    refreshState.getBaseTableId(),
                    refreshState.getBacklogLostReason(),
                    writer
            );
        } else {
            append(
                    Numbers.LONG_NULL,
                    -1,
                    false,
                    null,
                    Numbers.LONG_NULL,
                    null,
                    -1,
                    Long.MIN_VALUE,
                    Numbers.LONG_NULL,
                    0,
                    writer
            );
        }
    }

    // kept public for tests
    public static void appendBaseTable(int baseTableId, @Nullable CharSequence backlogLostReason, @NotNull AppendableBlock block) {
        block.putInt(baseTableId);
        block.putStr(backlogLostReason);
    }

    // kept public for tests
    public static void appendFrozenZoneState(
            long backfillFrontier,
            long frozenBoundaryFloor,
            int frozenBoundaryLimitHoursOrMonths,
            @NotNull AppendableBlock block
    ) {
        block.putLong(backfillFrontier);
        block.putLong(frozenBoundaryFloor);
        block.putInt(frozenBoundaryLimitHoursOrMonths);
    }

    // kept public for tests
    public static void appendPeriodHi(long periodHi, @NotNull AppendableBlock block) {
        block.putLong(periodHi);
    }

    // kept public for tests
    public static void appendRefreshIntervals(
            @Nullable LongList refreshIntervals,
            long refreshIntervalsBaseTxn,
            @NotNull AppendableBlock block
    ) {
        block.putLong(refreshIntervalsBaseTxn);
        if (refreshIntervals != null) {
            block.putInt(refreshIntervals.size());
            for (int i = 0, n = refreshIntervals.size(); i < n; i++) {
                block.putLong(refreshIntervals.getQuick(i));
            }
        } else {
            block.putInt(-1);
        }
    }

    // kept public for tests
    public static void appendState(
            long lastRefreshBaseTxn,
            boolean invalid,
            @Nullable CharSequence invalidationReason,
            @NotNull AppendableBlock block
    ) {
        block.putBool(invalid);
        block.putLong(lastRefreshBaseTxn);
        block.putStr(invalidationReason);
    }

    // kept public for tests
    public static void appendTs(long lastRefreshTimestamp, @NotNull AppendableBlock block) {
        block.putLong(lastRefreshTimestamp);
    }

    /**
     * Folds a positive sample into a positive EMA, applying the outlier
     * multiplier cap and recovering safely from arithmetic overflow.
     * <p>
     * The pre-overflow form is
     * {@code (prev*(N-1) + min(sample, prev*K) + N/2) / N}. The {@code + N/2}
     * is a round-half-up bias: plain integer-floor division creates a fixed
     * point at {@code prev = 1} (cap is 5, so {@code (7 + capped)/8} is
     * always 0 or 1 and the EMA cannot grow regardless of sample size).
     * Rounding half-up lets a single large sample escape the trap and
     * leaves larger EMA values effectively unchanged (the half-unit bias
     * is negligible against typical six- and seven-digit averages).
     * <p>
     * For values near {@code Long.MAX_VALUE / 7} the {@code prev*(N-1)} step
     * wraps -- in that case we fall back to a 50/50 blend of {@code prev}
     * and the capped sample, which keeps the EMA monotonic in sign and
     * prevents a single outlier from latching the average to a corrupted
     * value.
     */
    private static long foldEma(long prev, long sample) {
        if (prev == 0) {
            return sample;
        }
        // Cap outlier samples relative to prev. multiplyExact guards against
        // overflow if prev itself is enormous; on overflow we treat the cap
        // as Long.MAX_VALUE (i.e. effectively no cap, which is fine because
        // we only cap when prev is small enough to make 5*prev finite).
        long cap;
        try {
            cap = Math.multiplyExact(prev, (long) EMA_OUTLIER_MULTIPLIER);
        } catch (ArithmeticException overflow) {
            cap = Long.MAX_VALUE;
        }
        final long capped = Math.min(sample, cap);
        // Standard EMA with round-half-up: (prev*(N-1) + capped + N/2) / N.
        // Guard multiply and additions; on overflow fall back to a 50/50
        // blend so a single outlier doesn't corrupt the average.
        try {
            final long weighted = Math.multiplyExact(prev, (long) (EMA_ALPHA_INV - 1));
            final long sum = Math.addExact(Math.addExact(weighted, capped), (long) (EMA_ALPHA_INV / 2));
            return sum / EMA_ALPHA_INV;
        } catch (ArithmeticException overflow) {
            return (prev / 2) + (capped / 2);
        }
    }

    /**
     * Computes {@code a * b / c} for positive operands, saturating to
     * {@code Long.MAX_VALUE / 2} on long overflow rather than wrapping.
     * Lossy on the divide-first paths but bounded; used to derive the
     * gap-merge threshold from the two scan EMAs.
     */
    private static long mulDivSaturating(long a, long b, long c) {
        try {
            return Math.multiplyExact(a, b) / c;
        } catch (ArithmeticException overflow) {
            if (b >= c) {
                try {
                    return Math.multiplyExact(a, b / c);
                } catch (ArithmeticException overflow2) {
                    return Long.MAX_VALUE / 2;
                }
            }
            final long ratio = a / c;
            if (ratio == 0) {
                // a < c and a * b still overflowed -- b is huge. True
                // answer is a * b / c < b but we can't reach it without
                // 128-bit math. Return 0 (the "merging disabled" sentinel)
                // rather than a wrong-by-orders-of-magnitude value.
                return 0L;
            }
            try {
                return Math.multiplyExact(ratio, b);
            } catch (ArithmeticException overflow2) {
                return Long.MAX_VALUE / 2;
            }
        }
    }

    /**
     * Adds {@code delta} to the number of timers {@link MatViewTimerJob} holds for this view: a
     * positive delta when it registers timers, a negative one when it removes or loses them. The
     * count clamps at zero, so a stray decrement cannot leave a negative reading behind in
     * {@code materialized_views()}.
     */
    public void addRegisteredTimers(int delta) {
        final int count = registeredTimerCount + delta;
        registeredTimerCount = Math.max(count, 0);
    }

    public RecordCursorFactory acquireRecordFactory() {
        assert latch.get();
        RecordCursorFactory factory = cursorFactory;
        cursorFactory = null;
        return factory;
    }

    /**
     * Takes the cached factory when it was compiled from the given SQL text, and discards it otherwise: ALTER
     * MATERIALIZED VIEW ... AS replaced the view's SQL since. The refresh that takes the factory compiles from that
     * SQL text, so the factory it caches next, the one it took or one it compiled, comes from it. The caller holds the
     * latch.
     */
    public RecordCursorFactory acquireRecordFactory(@NotNull String viewSql) {
        assert latch.get();
        RecordCursorFactory factory = cursorFactory;
        cursorFactory = null;
        if (factory != null && !Chars.equals(viewSql, cursorFactorySql)) {
            factory = Misc.free(factory);
        }
        cursorFactorySql = viewSql;
        return factory;
    }

    /**
     * Records the telemetry event of ALTER MATERIALIZED VIEW ... AS, once its definition event is in the view's WAL.
     */
    void alterSuccess(boolean isBaseTableChange) {
        telemetryFacade.store(MAT_VIEW_ALTER, viewDefinition.getMatViewToken(), lastRefreshBaseTxn, isBaseTableChange ? "base change" : null, 0);
    }

    public long getRecordFactoryExpiryPolicyVersion() {
        assert latch.get();
        return cursorFactoryExpiryPolicyVersion;
    }

    public void returnRecordFactory(
            RecordCursorFactory factory,
            RecordToRowCopier copier,
            long recordRowCopierMetadataVersion,
            long expiryPolicyVersion
    ) {
        assert latch.get();
        if (closed) {
            Misc.free(factory);
            return;
        }
        cursorFactory = factory;
        cursorFactoryExpiryPolicyVersion = expiryPolicyVersion;
        recordToRowCopier = copier;
        this.recordRowCopierMetadataVersion = recordRowCopierMetadataVersion;
    }

    boolean claimPendingFullRefreshHandoff() {
        // Reads before clearing, so that a claim with nothing pending writes nothing. The read is volatile,
        // so it still observes a handoff that a loser published before its final lock attempt.
        return pendingFullRefreshHandoff.get() && pendingFullRefreshHandoff.getAndSet(false);
    }

    /**
     * Claims the request of the refreshes that deferred on a pending definition, see
     * {@link #requestDeferredDefinitionRefresh()}, once no definition is pending: the caller enqueues one incremental
     * refresh, which reads the base table after the claim, and so covers every refresh that deferred before it. Returns
     * false without a request, and while a definition is pending: the claim that follows the apply or skip of its event
     * takes the request then.
     */
    boolean claimDeferredDefinitionRefresh() {
        // The read without the lock writes nothing when no refresh deferred. A request registered before a raise of the
        // applied seqTxn is visible to the claim that follows the raise, since the raise takes the lock too.
        if (!hasDeferredDefinitionRefresh) {
            return false;
        }
        final Runnable seam = onDeferredDefinitionClaimForTesting;
        if (seam != null) {
            onDeferredDefinitionClaimForTesting = null;
            seam.run();
        }
        synchronized (definitionLock) {
            // another claim may have taken the request since the read above
            if (!hasDeferredDefinitionRefresh || isDefinitionPending()) {
                return false;
            }
            hasDeferredDefinitionRefresh = false;
            return true;
        }
    }

    /**
     * Returns true once per pending definition: the refresh job logs the wait of the first refresh that meets it.
     */
    boolean claimDefinitionWaitLog() {
        final long pendingSeqTxn = pendingDefinitionSeqTxn.get();
        return definitionWaitLoggedSeqTxn.getAndAccumulate(pendingSeqTxn, Math::max) < pendingSeqTxn;
    }

    boolean claimPendingIncrementalRefresh() {
        // Reads before clearing, so that a claim with nothing pending writes nothing. The read is volatile,
        // so it still observes a request that a loser published before its final lock attempt.
        return pendingIncrementalRefresh.get() && pendingIncrementalRefresh.getAndSet(false);
    }

    int claimPendingTaskRetryFlags() {
        return PENDING_TASK_RETRY_FLAGS_UPDATER.getAndSet(this, 0);
    }

    /**
     * Clears the backlog-lost marker, for REFRESH ... FULL, which rebuilds the view from its base table and so has no
     * backlog to keep. The caller holds the latch.
     */
    void clearBacklogLost() {
        assert latch.get();
        this.backlogLostReason = null;
    }

    boolean clearPendingFullRefresh(Object expectedOwner) {
        assert latch.get();
        final Runnable onClear = onClearPendingFullRefreshForTesting;
        if (onClear != null) {
            onClearPendingFullRefreshForTesting = null;
            onClear.run();
        }
        while (true) {
            final Object marker = pendingInvalidationMarker;
            if (!(marker instanceof PendingInvalidation pending) || pending.fullRefreshOwner != expectedOwner) {
                return false;
            }
            final Object replacement = pending.reason != null
                    ? new PendingInvalidation(
                    pending.reason,
                    pending.invalidationBaseTableToken,
                    pending.invalidationBaseTxn,
                    pending.hasBaseTableChange,
                    pending.isInvalidationForced,
                    null
            )
                    : null;
            if (PENDING_INVALIDATION_MARKER_UPDATER.compareAndSet(this, marker, replacement)) {
                return true;
            }
        }
    }

    /**
     * Clears the marker only when it still identifies {@code expectedMarker}. The caller must hold
     * the view latch. A refresh uses this ownership check after reading a fixed base snapshot so it
     * cannot erase a newer invalidation that arrived during the refresh.
     */
    boolean clearPendingInvalidation(Object expectedMarker) {
        assert latch.get();
        if (!(expectedMarker instanceof PendingInvalidation pending)) {
            return false;
        }
        final Object replacement = pending.fullRefreshOwner != null
                ? new PendingInvalidation(null, null, Numbers.LONG_NULL, false, pending.fullRefreshOwner)
                : null;
        return PENDING_INVALIDATION_MARKER_UPDATER.compareAndSet(this, expectedMarker, replacement);
    }

    /**
     * Clears the pending-invalidation marker without touching the {@link #invalid} flag.
     * Test callers must hold the view latch.
     */
    @TestOnly
    public void clearPendingInvalidationForTesting() {
        assert latch.get();
        this.pendingInvalidationMarker = null;
    }

    boolean clearPendingInvalidationIfCoveredByLastFullRefresh() {
        assert latch.get();
        while (true) {
            final Object marker = pendingInvalidationMarker;
            if (!(marker instanceof PendingInvalidation pending) || pending.reason == null) {
                return true;
            }
            final FullRefreshCoverage coverage = lastFullRefreshCoverage;
            if (coverage == null
                    || pending.invalidationBaseTableToken == null
                    || pending.invalidationBaseTxn == Numbers.LONG_NULL
                    || !coverage.baseTableToken.equals(pending.invalidationBaseTableToken)
                    || pending.invalidationBaseTxn > coverage.baseTableTxn) {
                return false;
            }
            final Object replacement = pending.fullRefreshOwner != null
                    ? new PendingInvalidation(null, null, Numbers.LONG_NULL, false, pending.fullRefreshOwner)
                    : null;
            if (PENDING_INVALIDATION_MARKER_UPDATER.compareAndSet(this, marker, replacement)) {
                return true;
            }
        }
    }

    void clearPendingInvalidationReason() {
        assert latch.get();
        while (true) {
            final Object marker = pendingInvalidationMarker;
            if (!(marker instanceof PendingInvalidation pending) || pending.reason == null) {
                return;
            }
            final Object replacement = pending.fullRefreshOwner != null
                    ? new PendingInvalidation(null, null, Numbers.LONG_NULL, false, pending.fullRefreshOwner)
                    : null;
            if (PENDING_INVALIDATION_MARKER_UPDATER.compareAndSet(this, marker, replacement)) {
                return;
            }
        }
    }

    @Override
    public void close() {
        // Flag the state as torn down BEFORE attempting the free so a refresh
        // worker that currently holds the latch (and may still acquire/return
        // the factory) sees closed==true and closes the factory it holds at its
        // park/unlock path instead of parking the live native factory into this
        // discarded state. Then free the factory under the latch so the free
        // never races a latched reader. If a worker holds the latch right now,
        // tryLock fails and the worker frees on its way out (tryCloseIfClosed).
        closed = true;
        if (tryLock()) {
            try {
                cursorFactory = Misc.free(cursorFactory);
            } finally {
                unlock();
            }
        }
    }

    public long getAvgCommitNanos() {
        return avgCommitNanos;
    }

    public long getAvgScanRangeTsUnits() {
        return avgScanRangeTsUnits;
    }

    public long getAvgScanSampleNanos() {
        return avgScanSampleNanos;
    }

    /**
     * Returns the cause of a lost backlog, see {@link MatViewBacklog}, or null while the view's backlog is tracked or
     * the view is valid.
     */
    public @Nullable String getBacklogLostReason() {
        return backlogLostReason;
    }

    /**
     * Returns the bare-name switches recorded on the view, see {@link #recordBareNameSwitch}. The caller must not
     * change the list.
     */
    ObjList<BareNameSwitch> getBareNameSwitches() {
        return bareNameSwitches;
    }

    /**
     * Returns the id of the base table that the refresh position and the cached refresh intervals belong to, or
     * {@link #BASE_TABLE_ID_UNKNOWN} when the view's state carries none.
     */
    public int getBaseTableId() {
        return baseTableId;
    }

    /**
     * Returns what the column mapping that the last compile of the view's SQL built leaves out: the view columns the
     * SQL does not produce and the SQL's columns the view lacks. Returns null before the first such compile in this
     * process and after one that failed. Refresh compiles the SQL, and so does view ADD COLUMN's check. View DROP
     * COLUMN takes the column out of the mapping of the last compile, see
     * {@link MatViewColumnMapping#summarizeWithoutViewColumn}.
     */
    public @Nullable MatViewColumnMapping.Summary getColumnMappingSummary() {
        return columnMappingSummary;
    }

    /**
     * Returns the gap-width threshold below which two adjacent cached refresh
     * intervals should be merged into one cluster. Units match the base
     * table's timestamp column (us for TIMESTAMP, ns for TIMESTAMP_NS), the
     * same as the cached interval values themselves. Derived from the recent
     * moving averages: the threshold is the timestamp-range width whose scan
     * cost equals one extra REPLACE_RANGE commit -- below it, merging beats
     * splitting.
     * <p>
     * Returns the cold-start default ({@link #COLD_START_GAP_THRESHOLD_MICROS}
     * converted to the base table's ts-unit) when no samples have been
     * recorded yet. Returns 0 when the cost model says merging is never
     * worth it (commit cheaper than scanning one ts unit of gap);
     * {@link MatViewRefreshJob#clusterIntervals} interprets a zero threshold
     * as "gap-based merging disabled" because
     * {@link io.questdb.griffin.model.IntervalUtils#unionInPlace} already
     * guarantees adjacent intervals are at least one ts unit apart, so no gap
     * can satisfy {@code gap < 0}. Operators reading the catalogue see 0
     * instead of a misleading 1 that would suggest the cost model is active.
     */
    public long getCommitGapThresholdTsUnits() {
        final long commit = avgCommitNanos;
        final long sample = avgScanSampleNanos;
        final long range = avgScanRangeTsUnits;
        if (commit <= 0 || sample <= 0 || range <= 0) {
            // Cold-start: convert the micros-valued constant into the base
            // table's ts-unit. The driver is null only in test fixtures
            // that construct a bare MatViewDefinition; production callers
            // always have a bound driver. Fall back to the raw micros
            // value when absent so the catalogue stays deterministic.
            final TimestampDriver driver = viewDefinition.getBaseTableTimestampDriver();
            return driver != null
                    ? driver.fromMicros(COLD_START_GAP_THRESHOLD_MICROS)
                    : COLD_START_GAP_THRESHOLD_MICROS;
        }
        // threshold = commit_ns * range_ts_units / sample_ns
        return mulDivSaturating(commit, range, sample);
    }

    /**
     * Returns high boundary for all complete time periods up to which a period
     * mat view is refreshed.
     * <p>
     * A period view is a mat view that has REFRESH ... PERIOD (LENGTH ...) clause.
     * <p>
     * Each time when a period, e.g. a day, is complete, a range refresh is triggered
     * on the period mat view (unless it has manual refresh type). This range refresh
     * runs for the [lastPeriodHi, completePeriodHi) interval. If the refresh is successful,
     * completePeriodHi is stored as the new lastPeriodHi value.
     * <p>
     * An incremental refresh stores the end of the last complete period too. When that end
     * moves earlier, e.g. after ALTER MATERIALIZED VIEW ... PERIOD or a clock step back, the
     * incremental refresh moves lastPeriodHi back to it, so that the next range refresh covers
     * the rows in between.
     */
    public long getLastPeriodHi() {
        return lastPeriodHi;
    }

    /**
     * Returns base table txn read by the last incremental or full refresh.
     * Subsequent incremental refreshes should only refresh base table intervals
     * (think, slices of table partitions) that correspond to later txns.
     */
    public long getLastRefreshBaseTxn() {
        return lastRefreshBaseTxn;
    }

    /**
     * Returns the error of the last user {@code REFRESH ... RANGE} that failed, or null when the last one
     * succeeded or none failed in this process. A failed user RANGE leaves the view valid instead of invalidating
     * it, so this is where its error shows.
     */
    public @Nullable String getLastRefreshError() {
        return lastRefreshError;
    }

    public long getLastRefreshFinishTimestampUs() {
        return lastRefreshFinishTimestampUs;
    }

    @TestOnly
    public long getAppliedDefinitionSeqTxnForTesting() {
        return appliedDefinitionSeqTxn;
    }

    public long getBackfillFrontier() {
        return backfillFrontier.get();
    }

    public long getLastRefreshFrozenBoundaryFloor() {
        return lastRefreshFrozenBoundaryFloor;
    }

    public int getLastRefreshFrozenBoundaryLimitHoursOrMonths() {
        return lastRefreshFrozenBoundaryLimitHoursOrMonths;
    }

    /**
     * The highest {@link #getBackfillFrontier() backfill frontier} value already written to the
     * {@code _mv.s} state file. The apply path persists the frontier only when it has advanced
     * past this, so a stream of refresh-only applies pays no extra state-file write. Apply thread only.
     */
    public long getPersistedBackfillFrontier() {
        return persistedBackfillFrontier;
    }

    public long getLastRefreshStartTimestampUs() {
        return lastRefreshStartTimestampUs;
    }

    /**
     * Returns the sequencer txn of the definition event of the last ALTER MATERIALIZED VIEW ... AS that this node ran,
     * or of the newest one whose event the view's WAL held committed and unapplied when the node booted, or -1 when
     * there is none since the node started.
     */
    public long getPendingDefinitionSeqTxn() {
        return pendingDefinitionSeqTxn.get();
    }

    Object getPendingFullRefreshOwner(Object marker) {
        return marker instanceof PendingInvalidation pending ? pending.fullRefreshOwner : null;
    }

    @TestOnly
    public Object getPendingFullRefreshOwnerForTesting() {
        return getPendingFullRefreshOwner(pendingInvalidationMarker);
    }

    TableToken getPendingInvalidationBaseTableToken(Object marker) {
        return marker instanceof PendingInvalidation pending ? pending.invalidationBaseTableToken : null;
    }

    @TestOnly
    public TableToken getPendingInvalidationBaseTableTokenForTesting() {
        return getPendingInvalidationBaseTableToken(pendingInvalidationMarker);
    }

    long getPendingInvalidationBaseTxn(Object marker) {
        return marker instanceof PendingInvalidation pending ? pending.invalidationBaseTxn : Numbers.LONG_NULL;
    }

    @TestOnly
    public long getPendingInvalidationBaseTxnForTesting() {
        return getPendingInvalidationBaseTxn(pendingInvalidationMarker);
    }

    Object getPendingInvalidationMarker() {
        return pendingInvalidationMarker;
    }

    @TestOnly
    public String getPendingInvalidationReason() {
        return getPendingInvalidationReason(pendingInvalidationMarker);
    }

    String getPendingInvalidationReason(Object marker) {
        return marker instanceof PendingInvalidation pending ? pending.reason : null;
    }

    public long getRecordRowCopierMetadataVersion() {
        return recordRowCopierMetadataVersion;
    }

    // Returns the projection that the cached copier reads the query's record through, or null when the copier
    // reads it as it is, for an identity or positional column mapping.
    @TestOnly
    public @Nullable SelectedRecord getRecordProjectionForTesting() {
        return MatViewColumnMapping.getRecordProjection(recordToRowCopier);
    }

    public RecordToRowCopier getRecordToRowCopier() {
        return recordToRowCopier;
    }

    public LongList getRefreshIntervals() {
        return refreshIntervals;
    }

    public long getRefreshIntervalsBaseTxn() {
        return refreshIntervalsBaseTxn;
    }

    public long getRefreshIntervalsSeq() {
        return refreshIntervalsSeq.get();
    }

    public long getRefreshRetryAfterMicros() {
        return refreshRetryAfterMicros;
    }

    public int getRefreshRetryCount() {
        return refreshRetryCount;
    }

    public long getRefreshSeq() {
        return refreshSeq.get();
    }

    public int getRegisteredTimerCount() {
        return registeredTimerCount;
    }

    // The view definition may change at any time as a result of ALTER MATERIALIZED VIEW SET REFRESH.
    // Avoid making chained calls, e.g. viewState.getViewDefinition().getRefreshType().
    public @NotNull MatViewDefinition getViewDefinition() {
        return viewDefinition;
    }

    @TestOnly
    public boolean hasPendingFullRefreshHandoffForTesting() {
        return pendingFullRefreshHandoff.get();
    }

    @TestOnly
    public boolean hasPendingFullRefreshOwnerForTesting() {
        return getPendingFullRefreshOwner(pendingInvalidationMarker) != null;
    }

    @TestOnly
    public boolean hasPendingIncrementalRefreshForTesting() {
        return pendingIncrementalRefresh.get();
    }

    @TestOnly
    public boolean hasDeferredDefinitionRefreshForTesting() {
        return hasDeferredDefinitionRefresh;
    }

    /**
     * Returns whether the marker's reason stands for a base table change, which a publication names with the base
     * table's token and the change's sequencer txn, such as the bare-name guard's: true also when a merge with a
     * publication of another or no provenance dropped the token and txn, see
     * {@link #getPendingInvalidationBaseTableToken}.
     */
    boolean hasPendingInvalidationBaseTableChange(Object marker) {
        return marker instanceof PendingInvalidation pending && pending.hasBaseTableChange;
    }

    @TestOnly
    public boolean hasPendingInvalidationBaseTableChangeForTesting() {
        return hasPendingInvalidationBaseTableChange(pendingInvalidationMarker);
    }

    /**
     * Returns true only when the pending marker carries an invalidation reason. The refresh and
     * WAL-purge gates use this reason-only view: a full-refresh-only marker must not freeze a
     * valid view's refreshes -- those refreshes' post-release finalize is the redelivery channel
     * for a retained full-refresh owner. Use {@link #isPendingInvalidation()} when either facet
     * matters.
     */
    public boolean hasPendingInvalidationReason() {
        return pendingInvalidationMarker instanceof PendingInvalidation pending && pending.reason != null;
    }

    /**
     * Returns true when the view keeps a backlog that is not known to be lost: it has completed a refresh, its state
     * knows the id of its base table, and no lost cause is set. While such a view is invalid, it keeps recording its
     * base table's changes in its refresh-interval cache, and holds back base WAL purging at its cache position, see
     * {@link MatViewBacklog}.
     */
    public boolean hasTrackedBacklog() {
        return lastRefreshBaseTxn != -1 && baseTableId != BASE_TABLE_ID_UNKNOWN && backlogLostReason == null;
    }

    public void incrementRefreshIntervalsSeq() {
        refreshIntervalsSeq.incrementAndGet();
    }

    public void incrementRefreshSeq() {
        refreshSeq.incrementAndGet();
    }

    public void init() {
        telemetryFacade.store(MAT_VIEW_CREATE, viewDefinition.getMatViewToken(), Numbers.LONG_NULL, null, 0);
    }

    public void initFromReader(MatViewStateReader reader) {
        this.invalid = reader.isInvalid();
        this.invalidationReason = Chars.toString(reader.getInvalidationReason());
        this.lastRefreshBaseTxn = reader.getLastRefreshBaseTxn();
        this.lastRefreshFinishTimestampUs = reader.getLastRefreshTimestampUs();
        this.lastPeriodHi = reader.getLastPeriodHi();
        this.refreshIntervalsBaseTxn = reader.getRefreshIntervalsBaseTxn();
        refreshIntervals.clear();
        refreshIntervals.addAll(reader.getRefreshIntervals());
        // Restore the frozen-zone runtime state. Defaults (MIN_VALUE / LONG_NULL) when the
        // state file predates the frozen-zone feature or no backfill/refresh has persisted it.
        final long frontier = reader.getBackfillFrontier();
        this.backfillFrontier.set(frontier);
        this.persistedBackfillFrontier = frontier;
        this.lastRefreshFrozenBoundaryFloor = reader.getFrozenBoundaryFloor();
        this.lastRefreshFrozenBoundaryLimitHoursOrMonths = reader.getFrozenBoundaryLimitHoursOrMonths();
        this.baseTableId = reader.getBaseTableId();
        this.backlogLostReason = Chars.toString(reader.getBacklogLostReason());
    }

    public boolean isClosed() {
        return closed;
    }

    /**
     * Returns true when the last successful FULL refresh rebuilt the view from a snapshot of the base table that
     * includes the transaction: the view's rows read the base table as it was from that transaction on. The
     * caller holds the latch, under which FULL records its snapshot.
     */
    boolean isCoveredByLastFullRefresh(int baseTableId, long baseTableTxn) {
        assert latch.get();
        final FullRefreshCoverage coverage = lastFullRefreshCoverage;
        return coverage != null && coverage.baseTableToken.getTableId() == baseTableId && coverage.baseTableTxn >= baseTableTxn;
    }

    /**
     * Returns true while the definition event of an ALTER MATERIALIZED VIEW ... AS that this node ran, or that the
     * view's WAL held committed and unapplied when the node booted, waits in the view's WAL: this node's WAL apply of
     * the view has neither applied nor skipped it, see {@link #recordAppliedDefinitionSeqTxn(long)}. False when there
     * is no such event since the node started.
     */
    public boolean isDefinitionPending() {
        return appliedDefinitionSeqTxn < pendingDefinitionSeqTxn.get();
    }

    public boolean isDropped() {
        return dropped;
    }

    public boolean isInvalid() {
        return invalid;
    }

    public String getInvalidationReason() {
        assert latch.get();
        return invalidationReason;
    }

    /**
     * Returns the invalidation reason without the latch, for a statement that reports it: the reason a concurrent
     * holder writes may not match the invalid flag read before it.
     */
    public String getInvalidationReasonForReport() {
        return invalidationReason;
    }

    public boolean isLocked() {
        return latch.get();
    }

    boolean isPendingFullRefresh() {
        return isPendingFullRefresh(pendingInvalidationMarker);
    }

    boolean isPendingFullRefresh(Object marker) {
        return marker instanceof PendingInvalidation pending && pending.fullRefreshOwner != null;
    }

    boolean isPendingFullRefreshOwner(Object expectedOwner) {
        return getPendingFullRefreshOwner(pendingInvalidationMarker) == expectedOwner;
    }

    @TestOnly
    public boolean isPendingInvalidation() {
        return pendingInvalidationMarker != null;
    }

    boolean isPendingInvalidationForced(Object marker) {
        return marker instanceof PendingInvalidation pending && pending.isInvalidationForced;
    }

    @TestOnly
    public boolean isPendingInvalidationForcedForTesting() {
        return isPendingInvalidationForced(pendingInvalidationMarker);
    }

    public boolean isRecordCopyPositional() {
        return isRecordCopyPositional;
    }

    /**
     * Returns true while REFRESH MATERIALIZED VIEW ... RESUME holds the view latch to recover the view.
     */
    public boolean isResuming() {
        return isResuming;
    }

    /**
     * Returns true if the view is not currently inside a transient-refresh backoff window,
     * i.e. it is eligible to be refreshed now. See {@link #scheduleRefreshRetry(long)}.
     */
    public boolean isRefreshDue(long nowMicros) {
        final long retryAfter = refreshRetryAfterMicros;
        return retryAfter == Numbers.LONG_NULL || nowMicros >= retryAfter;
    }

    public void markAsDropped() {
        dropped = true;
        telemetryFacade.store(MAT_VIEW_DROP, viewDefinition.getMatViewToken(), Numbers.LONG_NULL, null, 0);
    }

    public void markAsInvalid(CharSequence invalidationReason) {
        if (!invalid) {
            telemetryFacade.store(MAT_VIEW_INVALIDATE, viewDefinition.getMatViewToken(), Numbers.LONG_NULL, invalidationReason, 0);
        }
        this.invalid = true;
        this.invalidationReason = Chars.toString(invalidationReason);
    }

    Object markAsPendingFullRefreshAndGetOwner() {
        final Object fullRefreshOwner = new Object();
        while (true) {
            final Object marker = pendingInvalidationMarker;
            final Runnable onMarkerRead = onPendingFullRefreshMarkerReadForTesting;
            if (onMarkerRead != null) {
                onPendingFullRefreshMarkerReadForTesting = null;
                onMarkerRead.run();
            }
            final PendingInvalidation pending = marker instanceof PendingInvalidation p ? p : null;
            final Object replacement = pending != null
                    ? new PendingInvalidation(
                    pending.reason,
                    pending.invalidationBaseTableToken,
                    pending.invalidationBaseTxn,
                    pending.hasBaseTableChange,
                    pending.isInvalidationForced,
                    fullRefreshOwner
            )
                    : new PendingInvalidation(null, null, Numbers.LONG_NULL, false, fullRefreshOwner);
            if (PENDING_INVALIDATION_MARKER_UPDATER.compareAndSet(this, marker, replacement)) {
                return fullRefreshOwner;
            }
        }
    }

    @TestOnly
    public void markAsPendingFullRefreshForTesting() {
        markAsPendingFullRefreshAndGetOwner();
    }

    @TestOnly
    public void markAsPendingInvalidation(String invalidationReason) {
        markAsPendingInvalidationAndGetMarker(invalidationReason);
    }

    Object markAsPendingInvalidationAndGetMarker(String invalidationReason) {
        return markAsPendingInvalidationAndGetMarker(invalidationReason, null, Numbers.LONG_NULL, true);
    }

    Object markAsPendingInvalidationAndGetMarker(
            String invalidationReason,
            TableToken invalidationBaseTableToken,
            long invalidationBaseTxn,
            boolean isInvalidationForced
    ) {
        if (invalidationReason == null) {
            throw new IllegalArgumentException("invalidation reason must not be null");
        }
        while (true) {
            final Object marker = pendingInvalidationMarker;
            final Runnable onMarkerRead = onPendingInvalidationMarkerReadForTesting;
            if (onMarkerRead != null) {
                onPendingInvalidationMarkerReadForTesting = null;
                onMarkerRead.run();
            }
            final PendingInvalidation pending = marker instanceof PendingInvalidation p ? p : null;
            final Object fullRefreshOwner = pending != null ? pending.fullRefreshOwner : null;
            TableToken combinedBaseTableToken = invalidationBaseTableToken;
            long combinedBaseTxn = invalidationBaseTxn;
            boolean isCombinedForced = isInvalidationForced;
            // A publication that names a base table stands for a change of it. Sticky across merges: a merge with a
            // publication of another or no provenance drops the token and txn, but RESUME must still tell that the
            // reason may stand for a base table change, see hasPendingInvalidationBaseTableChange().
            boolean hasCombinedBaseTableChange = invalidationBaseTableToken != null;
            if (pending != null && pending.reason != null) {
                isCombinedForced |= pending.isInvalidationForced;
                hasCombinedBaseTableChange |= pending.hasBaseTableChange;
                if (combinedBaseTableToken == null
                        || combinedBaseTxn == Numbers.LONG_NULL
                        || pending.invalidationBaseTableToken == null
                        || pending.invalidationBaseTxn == Numbers.LONG_NULL
                        || !combinedBaseTableToken.equals(pending.invalidationBaseTableToken)) {
                    combinedBaseTableToken = null;
                    combinedBaseTxn = Numbers.LONG_NULL;
                } else {
                    combinedBaseTxn = Math.max(combinedBaseTxn, pending.invalidationBaseTxn);
                }
            } else if (combinedBaseTableToken == null || combinedBaseTxn == Numbers.LONG_NULL) {
                combinedBaseTableToken = null;
                combinedBaseTxn = Numbers.LONG_NULL;
            }
            final Object replacement = new PendingInvalidation(
                    invalidationReason,
                    combinedBaseTableToken,
                    combinedBaseTxn,
                    hasCombinedBaseTableChange,
                    isCombinedForced,
                    fullRefreshOwner
            );
            if (PENDING_INVALIDATION_MARKER_UPDATER.compareAndSet(this, marker, replacement)) {
                return replacement;
            }
        }
    }

    @TestOnly
    public void markAsPendingInvalidationForTesting(
            String invalidationReason,
            TableToken invalidationBaseTableToken,
            long invalidationBaseTxn,
            boolean isInvalidationForced
    ) {
        markAsPendingInvalidationAndGetMarker(
                invalidationReason,
                invalidationBaseTableToken,
                invalidationBaseTxn,
                isInvalidationForced
        );
    }

    public void markAsValid() {
        this.invalid = false;
        this.invalidationReason = null;
        // A valid view has no backlog; it starts a tracked one when it goes invalid.
        this.backlogLostReason = null;
        this.refreshRetryAfterMicros = Numbers.LONG_NULL;
        this.refreshRetryCount = 0;
    }

    /**
     * Records that the view's backlog is lost, with its cause, see {@link MatViewBacklog}. The view stops tracking its
     * base table's changes, and REFRESH MATERIALIZED VIEW ... RESUME refuses it until SKIP BACKLOG is given. The
     * caller holds the latch and persists the marker with the next view state event.
     */
    void markBacklogLost(@NotNull String cause) {
        assert latch.get();
        this.backlogLostReason = cause;
    }

    /**
     * Clears the pending transient-refresh retry deadline <em>iff</em> it still equals
     * {@code observedDeadline}, marking the view eligible for refresh now. This is an off-latch CAS:
     * {@link MatViewTimerJob} passes the deadline it observed due, so if a concurrent under-latch
     * refresh re-armed a fresher backoff (a different deadline) in the meantime, the CAS fails and
     * the fresh deadline is left intact (that re-arm queued its own RETRY heap entry to re-drive the
     * view later). Keeps the consecutive-failure counter so a re-driven refresh that fails again
     * still counts toward the retry limit. {@link MatViewTimerJob} calls this just before
     * re-enqueueing a refresh and only enqueues when this returns true.
     *
     * @param observedDeadline the retry deadline the caller observed due
     * @return true if this call cleared the deadline; false if it was concurrently changed
     */
    public boolean clearRefreshRetry(long observedDeadline) {
        return REFRESH_RETRY_AFTER_UPDATER.compareAndSet(this, observedDeadline, Numbers.LONG_NULL);
    }

    /**
     * Bumps and returns the consecutive transient-failure counter. Called under this.latch when a
     * refresh is deferred after a retriable error.
     */
    public int incrementRefreshRetryCount() {
        return ++refreshRetryCount;
    }

    /**
     * Resets the deferred-refresh retry state after a successful refresh: clears both the pending
     * retry deadline and the consecutive-failure counter that caps retries before invalidation.
     */
    public void resetRefreshRetry() {
        this.refreshRetryAfterMicros = Numbers.LONG_NULL;
        this.refreshRetryCount = 0;
    }

    /**
     * Schedules a deferred incremental refresh retry after a transient failure (e.g. base table
     * reader pool exhausted) instead of invalidating the view. The view stays valid in the meantime;
     * {@link MatViewTimerJob} re-drives an incremental refresh once {@code retryAfterMicros} elapses.
     */
    public void scheduleRefreshRetry(long retryAfterMicros) {
        this.refreshRetryAfterMicros = retryAfterMicros;
    }

    public void rangeRefreshSuccess(
            RecordCursorFactory factory,
            RecordToRowCopier copier,
            long recordRowCopierMetadataVersion,
            long expiryPolicyVersion,
            long refreshFinishedTimestampUs,
            long refreshTriggeredTimestampUs,
            long periodHi
    ) {
        assert latch.get();
        if (closed) {
            // The owner store was torn down (e.g. a demote) while this worker held the latch. Parking
            // the live native factory into a discarded state would leak it (the state is unreachable),
            // so close it here instead. The remaining bookkeeping is harmless on a discarded state.
            Misc.free(factory);
            return;
        }
        this.cursorFactory = factory;
        this.cursorFactoryExpiryPolicyVersion = expiryPolicyVersion;
        this.recordToRowCopier = copier;
        this.recordRowCopierMetadataVersion = recordRowCopierMetadataVersion;
        this.lastRefreshFinishTimestampUs = refreshFinishedTimestampUs;
        this.lastPeriodHi = periodHi;
        telemetryFacade.store(
                MAT_VIEW_REFRESH_SUCCESS,
                viewDefinition.getMatViewToken(),
                -1,
                null,
                refreshFinishedTimestampUs - refreshTriggeredTimestampUs
        );
    }

    /**
     * Records a range refresh that recomputed nothing and committed only the period it reached: a period refresh
     * whose period the partition clip emptied. A range refresh leaves the refresh position and the cached refresh
     * intervals to the incremental refreshes, so this keeps both, as {@link #rangeRefreshSuccess} does.
     */
    public void rangeRefreshSuccessNoRows(
            long refreshFinishedTimestampUs,
            long refreshTriggeredTimestampUs,
            long periodHi
    ) {
        assert latch.get();
        this.lastRefreshFinishTimestampUs = refreshFinishedTimestampUs;
        this.lastPeriodHi = periodHi;
        telemetryFacade.store(
                MAT_VIEW_REFRESH_SUCCESS,
                viewDefinition.getMatViewToken(),
                -1,
                null,
                refreshFinishedTimestampUs - refreshTriggeredTimestampUs
        );
    }

    /**
     * Folds a sampled REPLACE_RANGE commit latency into the rolling average used
     * for {@link #getCommitGapThresholdTsUnits()}. Samples in nanoseconds.
     * <p>
     * Samples larger than {@value #EMA_OUTLIER_MULTIPLIER}x the current average
     * are capped before folding, so a single GC pause or O3 partition rewrite
     * doesn't contaminate the cost model for the next several refreshes.
     */
    public void recordCommitNanos(long sampleNanos) {
        assert latch.get();
        if (sampleNanos <= 0) {
            return;
        }
        avgCommitNanos = foldEma(avgCommitNanos, sampleNanos);
    }

    /**
     * Records that a column change of the base table, at the base table's transaction, switches the bare select-list
     * name of the view's SQL between a column and a call of the zero-argument function of that name, see
     * {@link MatViewBareNameGuard}. One entry per name, compared without regard to case, and base table keeps the
     * highest transaction: a FULL refresh that covers it covers the lower ones. Nothing removes an entry; a FULL
     * refresh from a snapshot that includes its transaction covers it, see {@link #isCoveredByLastFullRefresh}. A
     * refresh that commits a refresh position below the transaction publishes the guard's invalidation again, see
     * {@code MatViewRefreshJob.publishBareNameSwitchAboveRefreshPosition()}.
     */
    void recordBareNameSwitch(CharSequence name, int baseTableId, long baseTableTxn) {
        synchronized (bareNameSwitchesLock) {
            final ObjList<BareNameSwitch> switches = bareNameSwitches;
            final ObjList<BareNameSwitch> updatedSwitches = new ObjList<>(switches.size() + 1);
            long switchTxn = baseTableTxn;
            for (int i = 0, n = switches.size(); i < n; i++) {
                final BareNameSwitch bareNameSwitch = switches.getQuick(i);
                if (bareNameSwitch.baseTableId == baseTableId && Chars.equalsIgnoreCase(bareNameSwitch.name, name)) {
                    switchTxn = Math.max(switchTxn, bareNameSwitch.baseTableTxn);
                } else {
                    updatedSwitches.add(bareNameSwitch);
                }
            }
            updatedSwitches.add(new BareNameSwitch(Chars.toString(name), baseTableId, switchTxn));
            final Runnable onCopied = onBareNameSwitchCopiedForTesting;
            if (onCopied != null) {
                onBareNameSwitchCopiedForTesting = null;
                onCopied.run();
            }
            bareNameSwitches = updatedSwitches;
        }
    }

    /**
     * Records that this node's WAL apply of the view applied or skipped every definition event up to the sequencer txn:
     * the WAL apply job as it applies a MAT_VIEW_DEFINITION event or skips it as stale, and RESUME WAL FROM TXN as it
     * skips the view's WAL past it, see {@link #isDefinitionPending()}. The caller then claims the request of the
     * refreshes that deferred on the event, see {@link #claimDeferredDefinitionRefresh()}. The record only grows.
     */
    public void recordAppliedDefinitionSeqTxn(long seqTxn) {
        synchronized (definitionLock) {
            if (seqTxn > appliedDefinitionSeqTxn) {
                appliedDefinitionSeqTxn = seqTxn;
            }
        }
    }

    void recordFullRefreshSuccess(TableToken baseTableToken, long baseTableTxn) {
        assert latch.get();
        lastFullRefreshCoverage = new FullRefreshCoverage(baseTableToken, baseTableTxn);
    }

    /**
     * Records the sequencer txn of the definition event that ALTER MATERIALIZED VIEW ... AS wrote, or that the boot
     * load found committed and unapplied in the view's WAL, see {@link #getPendingDefinitionSeqTxn()}. The record only
     * grows.
     */
    public void recordPendingDefinitionSeqTxn(long seqTxn) {
        pendingDefinitionSeqTxn.accumulateAndGet(seqTxn, Math::max);
    }

    /**
     * Folds a sampled base-table scan into the two rolling scan averages
     * (wall-clock duration and timestamp range width) used by the gap-merge
     * cost model. {@code rangeTsUnits} is the timestamp range width that
     * the cursor scanned (i.e. {@code iteratorHi - iteratorLo}) in the base
     * table's timestamp unit (us for TIMESTAMP, ns for TIMESTAMP_NS), not
     * row counts. Ignores zero/negative inputs but folds every other
     * sample: storing each EMA in its natural unit lets the threshold
     * derivation recover the rate at read time without losing precision to
     * the per-sample integer division that bites fast-scan-wide-range
     * workloads (the common case on TIMESTAMP_NS bases).
     * <p>
     * Samples larger than {@value #EMA_OUTLIER_MULTIPLIER}x the current
     * average are capped before folding.
     */
    public void recordScanMetrics(long sampleNanos, long rangeTsUnits) {
        assert latch.get();
        if (sampleNanos <= 0 || rangeTsUnits <= 0) {
            return;
        }
        avgScanSampleNanos = foldEma(avgScanSampleNanos, sampleNanos);
        avgScanRangeTsUnits = foldEma(avgScanRangeTsUnits, rangeTsUnits);
    }

    /**
     * Records a user {@code REFRESH ... RANGE} that failed. The view stays valid, with its rows, refresh position and
     * refresh timestamps, since RANGE never moves the refresh position and commits only complete replace ranges;
     * the error shows in {@link #getLastRefreshError()} until the next user RANGE succeeds.
     */
    public void rangeRefreshFail(CharSequence errorMessage) {
        assert latch.get();
        this.lastRefreshError = Chars.toString(errorMessage);
        telemetryFacade.store(MAT_VIEW_REFRESH_FAIL, viewDefinition.getMatViewToken(), Numbers.LONG_NULL, errorMessage, 0);
    }

    // A user REFRESH ... RANGE that succeeded clears the error of the one that failed before it.
    public void rangeRefreshSucceeded() {
        assert latch.get();
        this.lastRefreshError = null;
    }

    public void refreshFail(long refreshTimestamp, CharSequence errorMessage) {
        assert latch.get();
        this.lastRefreshFinishTimestampUs = refreshTimestamp;
        markAsInvalid(errorMessage);
        // Drop any pending transient-refresh backoff and the consecutive-failure counter: a view that
        // a non-retriable failure has just invalidated must not keep a stale retry deadline or count.
        // The view may have been deferred before this failure (e.g. it was re-driven by a base commit
        // through the isRefreshDue gate, which does not pre-clear the deadline), so the fields are not
        // necessarily already cleared here. markAsValid clears them again on recovery.
        resetRefreshRetry();
        telemetryFacade.store(MAT_VIEW_REFRESH_FAIL, viewDefinition.getMatViewToken(), Numbers.LONG_NULL, errorMessage, 0);
    }

    /**
     * Clears the rolling commit and scan latency averages, returning the cost
     * model to its cold-start state. Used by {@code REFRESH MATERIALIZED VIEW
     * ... STATS} when an operator wants to force the auto-tune to re-learn
     * (e.g. after a workload shape change or a hardware migration).
     */
    public void refreshStats() {
        assert latch.get();
        this.avgCommitNanos = 0;
        this.avgScanSampleNanos = 0;
        this.avgScanRangeTsUnits = 0;
    }

    public void refreshSuccess(
            RecordCursorFactory factory,
            RecordToRowCopier copier,
            long recordRowCopierMetadataVersion,
            long expiryPolicyVersion,
            long refreshFinishedTimestamp,
            long refreshTriggeredTimestamp,
            long baseTableTxn,
            long periodHi
    ) {
        assert latch.get();
        markAsValid();
        if (closed) {
            // The owner store was torn down (e.g. a demote) while this worker held the latch. Parking
            // the live native factory into a discarded state would leak it (the state is unreachable),
            // so close it here instead. The remaining bookkeeping is harmless on a discarded state.
            Misc.free(factory);
            return;
        }
        this.cursorFactory = factory;
        this.cursorFactoryExpiryPolicyVersion = expiryPolicyVersion;
        this.recordToRowCopier = copier;
        this.recordRowCopierMetadataVersion = recordRowCopierMetadataVersion;
        this.lastRefreshFinishTimestampUs = refreshFinishedTimestamp;
        this.lastRefreshBaseTxn = baseTableTxn;
        this.lastPeriodHi = periodHi;
        // Successful incremental refresh means that cached intervals were applied and should be evicted.
        this.refreshIntervalsBaseTxn = -1;
        refreshIntervals.clear();
        telemetryFacade.store(
                MAT_VIEW_REFRESH_SUCCESS,
                viewDefinition.getMatViewToken(),
                baseTableTxn,
                null,
                refreshFinishedTimestamp - refreshTriggeredTimestamp
        );
    }

    public void refreshSuccessNoRows(
            long refreshFinishedTimestampUs,
            long refreshTriggeredTimestampUs,
            long baseTableTxn,
            long periodHi
    ) {
        assert latch.get();
        markAsValid();
        this.lastRefreshFinishTimestampUs = refreshFinishedTimestampUs;
        this.lastRefreshBaseTxn = baseTableTxn;
        this.lastPeriodHi = periodHi;
        // Successful incremental refresh means that cached intervals were applied and should be evicted.
        this.refreshIntervalsBaseTxn = -1;
        refreshIntervals.clear();
        telemetryFacade.store(
                MAT_VIEW_REFRESH_SUCCESS,
                viewDefinition.getMatViewToken(),
                baseTableTxn,
                null,
                refreshFinishedTimestampUs - refreshTriggeredTimestampUs
        );
    }

    /**
     * Returns true when a definition is pending, see {@link #isDefinitionPending()}, after it registered the request of
     * the refresh that asks: the refresh then defers without writing, and the claim that follows the apply or skip of
     * the definition event enqueues one incremental refresh for it, see {@link #claimDeferredDefinitionRefresh()}.
     * Returns false when no definition is pending, and the refresh goes ahead. The check and the registration are one
     * step under the lock that the raise of the applied seqTxn and the claim take, so a refresh that goes ahead leaves no
     * request, and only a claim that enqueues a refresh clears one, whatever number of refreshes, claims and ALTERs
     * meet.
     */
    boolean requestDeferredDefinitionRefresh() {
        if (!isDefinitionPending()) {
            return false;
        }
        final Runnable seam = onDeferredDefinitionRequestForTesting;
        if (seam != null) {
            onDeferredDefinitionRequestForTesting = null;
            seam.run();
        }
        synchronized (definitionLock) {
            if (!isDefinitionPending()) {
                return false;
            }
            hasDeferredDefinitionRefresh = true;
            return true;
        }
    }

    void requestPendingFullRefreshHandoff() {
        final Runnable onRequest = onPendingFullRefreshHandoffRequestForTesting;
        if (onRequest != null) {
            onPendingFullRefreshHandoffRequestForTesting = null;
            onRequest.run();
        }
        pendingFullRefreshHandoff.set(true);
    }

    void requestPendingIncrementalRefresh() {
        final Runnable onRequest = onPendingIncrementalRefreshRequestForTesting;
        if (onRequest != null) {
            onPendingIncrementalRefreshRequestForTesting = null;
            onRequest.run();
        }
        pendingIncrementalRefresh.set(true);
    }

    void requestPendingTaskRetry(int retryFlags) {
        while (true) {
            final int currentFlags = pendingTaskRetryFlags;
            if (PENDING_TASK_RETRY_FLAGS_UPDATER.compareAndSet(this, currentFlags, currentFlags | retryFlags)) {
                return;
            }
        }
    }

    /**
     * Makes the view valid again after REFRESH MATERIALIZED VIEW ... RESUME persisted its new state: the refresh
     * position, the last refreshed period and the base table id that the state event carried. Catching up keeps the
     * cached refresh intervals, the view's backlog, for its next incremental refresh; a jump clears them. Either way
     * the backlog-lost marker goes. The caller holds the latch.
     */
    void resumeSuccess(
            long lastRefreshBaseTxn,
            long lastPeriodHi,
            int baseTableId,
            boolean isCatchUp,
            @NotNull CharSequence mode
    ) {
        assert latch.get();
        markAsValid();
        this.lastRefreshBaseTxn = lastRefreshBaseTxn;
        this.lastPeriodHi = lastPeriodHi;
        this.baseTableId = baseTableId;
        if (!isCatchUp) {
            this.refreshIntervalsBaseTxn = -1;
            refreshIntervals.clear();
        }
        telemetryFacade.store(MAT_VIEW_RESUME, viewDefinition.getMatViewToken(), lastRefreshBaseTxn, mode, 0);
    }

    void runAlterValidatedSeamForTesting() {
        final Runnable seam = onAlterValidatedForTesting;
        if (seam != null) {
            onAlterValidatedForTesting = null;
            seam.run();
        }
    }

    void runBareNameSwitchRecordedSeamForTesting() {
        final Runnable seam = onBareNameSwitchRecordedForTesting;
        if (seam != null) {
            onBareNameSwitchRecordedForTesting = null;
            seam.run();
        }
    }

    void runDefinitionEventWrittenSeamForTesting() {
        final Runnable seam = onDefinitionEventWrittenForTesting;
        if (seam != null) {
            onDefinitionEventWrittenForTesting = null;
            seam.run();
        }
    }

    void runDefinitionWaitSeamForTesting() {
        final Runnable seam = onDefinitionWaitForTesting;
        if (seam != null) {
            onDefinitionWaitForTesting = null;
            seam.run();
        }
    }

    void runResumeFlagSetSeamForTesting() {
        final Runnable seam = onResumeFlagSetForTesting;
        if (seam != null) {
            onResumeFlagSetForTesting = null;
            seam.run();
        }
    }

    void runResumeHoldingLockSeamForTesting() {
        final Runnable seam = onResumeHoldingLockForTesting;
        if (seam != null) {
            onResumeHoldingLockForTesting = null;
            seam.run();
        }
    }

    void runResumeLatchBusySeamForTesting() {
        final Runnable seam = onResumeLatchBusyForTesting;
        if (seam != null) {
            onResumeLatchBusyForTesting = null;
            seam.run();
        }
    }

    void runResumeWalAppliedSeamForTesting() {
        final Runnable seam = onResumeWalAppliedForTesting;
        if (seam != null) {
            onResumeWalAppliedForTesting = null;
            seam.run();
        }
    }

    void runResumeWalWaitSeamForTesting() {
        final Runnable seam = onResumeWalWaitForTesting;
        if (seam != null) {
            onResumeWalWaitForTesting = null;
            seam.run();
        }
    }

    /**
     * Sets the id of the base table that the refresh position and the cached refresh intervals belong to. The
     * caller holds the latch and persists the id with the next view state event.
     */
    public void setBaseTableId(int baseTableId) {
        assert latch.get();
        this.baseTableId = baseTableId;
    }

    // Keeps the summary of a column mapping that a compile of the view's SQL built, or null after one that failed, see
    // getColumnMappingSummary().
    public void setColumnMappingSummary(@Nullable MatViewColumnMapping.Summary columnMappingSummary) {
        this.columnMappingSummary = columnMappingSummary;
    }

    public void setLastPeriodHi(long lastPeriodHi) {
        this.lastPeriodHi = lastPeriodHi;
    }

    public void setLastRefreshBaseTableTxn(long txn) {
        lastRefreshBaseTxn = txn;
    }

    public void setLastRefreshFrozenBoundaryFloor(long floor, int limitHoursOrMonths) {
        assert latch.get();
        lastRefreshFrozenBoundaryFloor = floor;
        lastRefreshFrozenBoundaryLimitHoursOrMonths = limitHoursOrMonths;
    }

    /**
     * Records that the {@code _mv.s} state file now holds {@code frontier} as the persisted
     * backfill frontier. Called by the WAL apply path right after it (re)writes the state file.
     * Apply thread only.
     */
    public void setPersistedBackfillFrontier(long frontier) {
        persistedBackfillFrontier = frontier;
    }

    /**
     * Advance the backfill frontier high-water mark to {@code anchor} (a monotonic max;
     * lower values are ignored). Called from the WAL apply path
     * ({@link io.questdb.cairo.wal.ApplyWal2TableJob#reconstructBackfillFrontier}) after a user
     * backfill txn has been committed, with the boundary anchor derived from the committed row
     * ({@code bucketEnd + LIMIT}). It is deliberately NOT advanced by the commit-time validator,
     * which is a pre-commit hook: advancing before the txn is sealed could leave this no-rollback
     * max permanently over-advanced if the commit fails. The refresh job folds this value into
     * its boundary anchor (alongside {@code max(base_ts)} and {@code max(view_ts)}), so once a
     * backfill has been accepted as frozen, a later {@code max(base_ts)} retreat (base partition
     * drop / re-ingestion) cannot pull the boundary back over it -- the case {@code max(view_ts)}
     * alone cannot cover because a backfilled row is older than the view's materialised frontier.
     * <p>
     * Lock-free CAS max: the apply path advances it while the refresh job reads it concurrently.
     */
    public void advanceBackfillFrontier(long anchor) {
        long current;
        while (anchor > (current = backfillFrontier.get())) {
            if (backfillFrontier.compareAndSet(current, anchor)) {
                break;
            }
        }
    }

    public void setLastRefreshStartTimestampUs(long timestampUs) {
        lastRefreshStartTimestampUs = timestampUs;
    }

    public void setLastRefreshTimestampUs(long timestampUs) {
        this.lastRefreshFinishTimestampUs = timestampUs;
    }

    /**
     * Test seam: runs once, in ALTER MATERIALIZED VIEW ... AS, between the validation of the new SQL and the write of
     * its definition event.
     */
    @TestOnly
    public void setOnAlterValidatedForTesting(Runnable onAlterValidatedForTesting) {
        this.onAlterValidatedForTesting = onAlterValidatedForTesting;
    }

    /**
     * Test seam: runs once in {@link #recordBareNameSwitch}, under the lock that serializes its writers, after it built
     * the updated copy of the view's bare-name switches and before it publishes the copy. Tests use it to hold the
     * bare-name guard of one base table there while the guard of another base table records a switch on the same view.
     * One-shot: the seam clears itself before firing.
     */
    @TestOnly
    public void setOnBareNameSwitchCopiedForTesting(Runnable onBareNameSwitchCopiedForTesting) {
        this.onBareNameSwitchCopiedForTesting = onBareNameSwitchCopiedForTesting;
    }

    /**
     * Test seam: runs once in the bare-name guard, after it recorded the bare-name switches of a base column change on
     * the view, see {@link MatViewBareNameGuard}: in its check before the change applies, before it reads the view's
     * resuming and invalid flags; in its check after the change, before it reads the view's refresh position. Tests use
     * it to run RESUME or a refresh in that window. One-shot: the seam clears itself before firing.
     */
    @TestOnly
    public void setOnBareNameSwitchRecordedForTesting(Runnable onBareNameSwitchRecordedForTesting) {
        this.onBareNameSwitchRecordedForTesting = onBareNameSwitchRecordedForTesting;
    }

    /**
     * Test seam: runs once in {@code claimDeferredDefinitionRefresh}, after the claim read a request without the lock and
     * before it takes the lock. Tests use it to run another claim in that window. One-shot: the seam clears itself before
     * firing.
     */
    @TestOnly
    public void setOnDeferredDefinitionClaimForTesting(Runnable onDeferredDefinitionClaimForTesting) {
        this.onDeferredDefinitionClaimForTesting = onDeferredDefinitionClaimForTesting;
    }

    /**
     * Test seam: runs once in {@code requestDeferredDefinitionRefresh}, after a refresh found a definition pending and
     * before it checks again and registers its request under the lock. Tests use it to apply or skip the definition
     * event in that window. One-shot: the seam clears itself before firing.
     */
    @TestOnly
    public void setOnDeferredDefinitionRequestForTesting(Runnable onDeferredDefinitionRequestForTesting) {
        this.onDeferredDefinitionRequestForTesting = onDeferredDefinitionRequestForTesting;
    }

    /**
     * Test seam: runs once in ALTER MATERIALIZED VIEW ... AS, after the statement wrote its definition event, and, with
     * a base change, released the view latch, and before it records the event's seqTxn, see
     * {@link #getPendingDefinitionSeqTxn()}. Tests use it to run a refresh in that window. One-shot: the seam clears
     * itself before firing.
     */
    @TestOnly
    public void setOnDefinitionEventWrittenForTesting(Runnable onDefinitionEventWrittenForTesting) {
        this.onDefinitionEventWrittenForTesting = onDefinitionEventWrittenForTesting;
    }

    /**
     * Test seam: runs once, while a REFRESH or a view ADD COLUMN, DROP COLUMN or ALTER ... AS waits for the definition
     * event of an ALTER MATERIALIZED VIEW ... AS to apply, see {@link MatViewAlter#awaitPendingDefinition} and
     * {@link MatViewAlter#awaitPendingDefinitionOrRefuse}.
     */
    @TestOnly
    public void setOnDefinitionWaitForTesting(Runnable onDefinitionWaitForTesting) {
        this.onDefinitionWaitForTesting = onDefinitionWaitForTesting;
    }

    /**
     * Test seam: runs once at the start of {@link #unlock()}, while the caller still holds the lock. Tests use
     * it to race a refresh that loses the lock against the holder's release.
     * One-shot: the seam clears itself before firing.
     */
    @TestOnly
    public void setOnBeforeUnlockForTesting(Runnable onBeforeUnlockForTesting) {
        this.onBeforeUnlockForTesting = onBeforeUnlockForTesting;
    }

    /**
     * Test seam: runs once at the start of {@code clearPendingFullRefresh}, before it reads the pending
     * marker. Tests use it to race a marker replacement against the clear.
     * One-shot: the seam clears itself before firing.
     */
    @TestOnly
    public void setOnClearPendingFullRefreshForTesting(Runnable onClearPendingFullRefreshForTesting) {
        this.onClearPendingFullRefreshForTesting = onClearPendingFullRefreshForTesting;
    }

    /**
     * Test seam: runs once at the start of {@code requestPendingFullRefreshHandoff}, after a full refresh lost
     * its first lock attempt and before it publishes its handoff. Tests use it to release the lock in that
     * window. One-shot: the seam clears itself before firing.
     */
    @TestOnly
    public void setOnPendingFullRefreshHandoffRequestForTesting(Runnable onPendingFullRefreshHandoffRequestForTesting) {
        this.onPendingFullRefreshHandoffRequestForTesting = onPendingFullRefreshHandoffRequestForTesting;
    }

    /**
     * Test seam: runs once inside {@code markAsPendingFullRefreshAndGetOwner}'s CAS loop, after it reads
     * the pending marker but before it builds the replacement. Tests use it to race a concurrent marker
     * update against the CAS.
     * One-shot: the seam clears itself before firing.
     */
    @TestOnly
    public void setOnPendingFullRefreshMarkerReadForTesting(Runnable onPendingFullRefreshMarkerReadForTesting) {
        this.onPendingFullRefreshMarkerReadForTesting = onPendingFullRefreshMarkerReadForTesting;
    }

    /**
     * Test seam: runs once at the start of {@code requestPendingIncrementalRefresh}, after a refresh lost its
     * first lock attempt and before it publishes its request. Tests use it to release the lock in that window.
     * One-shot: the seam clears itself before firing.
     */
    @TestOnly
    public void setOnPendingIncrementalRefreshRequestForTesting(Runnable onPendingIncrementalRefreshRequestForTesting) {
        this.onPendingIncrementalRefreshRequestForTesting = onPendingIncrementalRefreshRequestForTesting;
    }

    /**
     * Test seam: runs once inside {@code markAsPendingInvalidationAndGetMarker}'s CAS loop, after it reads
     * the pending marker but before it builds the replacement. Tests use it to race a concurrent marker
     * update against the CAS.
     * One-shot: the seam clears itself before firing.
     */
    @TestOnly
    public void setOnPendingInvalidationMarkerReadForTesting(Runnable onPendingInvalidationMarkerReadForTesting) {
        this.onPendingInvalidationMarkerReadForTesting = onPendingInvalidationMarkerReadForTesting;
    }

    /**
     * Test seam: runs once while REFRESH MATERIALIZED VIEW ... RESUME holds the view latch, right after it set the
     * view's resuming flag and before it reads the view's bare-name switches, see {@link MatViewResume}. Tests use it
     * to make the bare-name guard record a switch in that window. One-shot: the seam clears itself before firing.
     */
    @TestOnly
    public void setOnResumeFlagSetForTesting(Runnable onResumeFlagSetForTesting) {
        this.onResumeFlagSetForTesting = onResumeFlagSetForTesting;
    }

    /**
     * Test seam: runs once while REFRESH MATERIALIZED VIEW ... RESUME holds the view latch, right after it took it
     * and before it checks the view again. Tests use it to make an invalidation, a FULL refresh request, a demote or
     * a base table replacement arrive while RESUME holds the latch. One-shot: the seam clears itself before firing.
     */
    @TestOnly
    public void setOnResumeHoldingLockForTesting(Runnable onResumeHoldingLockForTesting) {
        this.onResumeHoldingLockForTesting = onResumeHoldingLockForTesting;
    }

    /**
     * Test seam: runs once when REFRESH MATERIALIZED VIEW ... RESUME finds the view latch held by another holder,
     * before it polls the latch again. Tests use it to release the latch, or to cancel the statement, at a known
     * point of the wait. One-shot: the seam clears itself before firing.
     */
    @TestOnly
    public void setOnResumeLatchBusyForTesting(Runnable onResumeLatchBusyForTesting) {
        this.onResumeLatchBusyForTesting = onResumeLatchBusyForTesting;
    }

    /**
     * Test seam: runs once after REFRESH MATERIALIZED VIEW ... RESUME found the view's WAL applied and before it
     * tries the view latch. Tests use it to commit a view WAL transaction in that window. One-shot: the seam clears
     * itself before firing.
     */
    @TestOnly
    public void setOnResumeWalAppliedForTesting(Runnable onResumeWalAppliedForTesting) {
        this.onResumeWalAppliedForTesting = onResumeWalAppliedForTesting;
    }

    /**
     * Test seam: runs once when REFRESH MATERIALIZED VIEW ... RESUME finds the view's WAL not applied yet, before it
     * checks the statement's timeout and waits. Tests use it to drop the view, or to expire the statement's timeout,
     * while RESUME waits. One-shot: the seam clears itself before firing.
     */
    @TestOnly
    public void setOnResumeWalWaitForTesting(Runnable onResumeWalWaitForTesting) {
        this.onResumeWalWaitForTesting = onResumeWalWaitForTesting;
    }

    // Records how the copier built from the next cached factory copies, see isRecordCopyPositional.
    public void setRecordCopyPositional(boolean isRecordCopyPositional) {
        assert latch.get();
        this.isRecordCopyPositional = isRecordCopyPositional;
    }

    public void setRefreshIntervals(LongList refreshIntervals) {
        this.refreshIntervals.clear();
        this.refreshIntervals.addAll(refreshIntervals);
    }

    public void setRefreshIntervalsBaseTxn(long refreshIntervalsBaseTxn) {
        this.refreshIntervalsBaseTxn = refreshIntervalsBaseTxn;
    }

    /**
     * Test-only seeder for the rolling averages. Lets tests drive the
     * clustering cost model into a known regime without having to predict the
     * exact scan-time of a warmup refresh on the running hardware. Production
     * callers must use the {@code recordCommitNanos} / {@code recordScanMetrics}
     * pair instead.
     */
    @TestOnly
    public void setRefreshMetricsForTesting(long commitNanos, long scanSampleNanos, long scanRangeTsUnits) {
        assert latch.get();
        this.avgCommitNanos = Math.max(0, commitNanos);
        this.avgScanSampleNanos = Math.max(0, scanSampleNanos);
        this.avgScanRangeTsUnits = Math.max(0, scanRangeTsUnits);
    }

    /**
     * Sets or clears the resuming flag, see {@link #isResuming()}. The caller holds the latch.
     */
    void setResuming(boolean isResuming) {
        assert latch.get();
        this.isResuming = isResuming;
    }

    public void setViewDefinition(MatViewDefinition viewDefinition) {
        this.viewDefinition = viewDefinition;
    }

    public void tryCloseIfClosed() {
        // Companion of close() for the latch-race: if the owner store called close() while a refresh
        // worker held the latch, close()'s own tryLock failed and the factory the worker may have
        // parked (or left in place) is still live. The worker calls this right after it unlocks so the
        // discarded state's native factory is freed exactly once, off the teardown thread.
        if (closed && tryLock()) {
            try {
                cursorFactory = Misc.free(cursorFactory);
            } finally {
                unlock();
            }
        }
    }

    public void tryCloseIfDropped() {
        if (dropped && tryLock()) {
            try {
                closed = true;
                cursorFactory = Misc.free(cursorFactory);
            } finally {
                unlock();
            }
        }
    }

    public boolean tryLock() {
        return latch.compareAndSet(false, true);
    }

    public void unlock() {
        final Runnable onBeforeUnlock = onBeforeUnlockForTesting;
        if (onBeforeUnlock != null) {
            onBeforeUnlockForTesting = null;
            onBeforeUnlock.run();
        }
        if (!latch.compareAndSet(true, false)) {
            throw new IllegalStateException("cannot unlock, not locked");
        }
    }

    // A base column change that switches a bare select-list name of the view's SQL, see recordBareNameSwitch().
    static final class BareNameSwitch {
        final int baseTableId;
        final long baseTableTxn;
        // the name as the view's SQL wrote it when the bare-name guard recorded the switch
        final String name;

        private BareNameSwitch(String name, int baseTableId, long baseTableTxn) {
            this.name = name;
            this.baseTableId = baseTableId;
            this.baseTableTxn = baseTableTxn;
        }
    }

    private static final class FullRefreshCoverage {
        private final TableToken baseTableToken;
        private final long baseTableTxn;

        private FullRefreshCoverage(TableToken baseTableToken, long baseTableTxn) {
            this.baseTableToken = baseTableToken;
            this.baseTableTxn = baseTableTxn;
        }
    }

    private static final class PendingInvalidation {
        private final Object fullRefreshOwner;
        // Whether a reason publication merged into this marker carried a base table change, a base token and txn,
        // also when the merge lost them.
        private final boolean hasBaseTableChange;
        private final TableToken invalidationBaseTableToken;
        private final long invalidationBaseTxn;
        private final boolean isInvalidationForced;
        private final String reason;

        // A marker without a base table change, such as one that carries a full refresh owner only.
        private PendingInvalidation(
                String reason,
                TableToken invalidationBaseTableToken,
                long invalidationBaseTxn,
                boolean isInvalidationForced,
                Object fullRefreshOwner
        ) {
            this(reason, invalidationBaseTableToken, invalidationBaseTxn, false, isInvalidationForced, fullRefreshOwner);
        }

        private PendingInvalidation(
                String reason,
                TableToken invalidationBaseTableToken,
                long invalidationBaseTxn,
                boolean hasBaseTableChange,
                boolean isInvalidationForced,
                Object fullRefreshOwner
        ) {
            this.fullRefreshOwner = fullRefreshOwner;
            this.hasBaseTableChange = hasBaseTableChange;
            this.invalidationBaseTableToken = invalidationBaseTableToken;
            this.invalidationBaseTxn = invalidationBaseTxn;
            this.isInvalidationForced = isInvalidationForced;
            this.reason = reason;
        }
    }
}
