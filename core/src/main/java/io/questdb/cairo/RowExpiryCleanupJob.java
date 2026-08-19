/*******************************************************************************
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

package io.questdb.cairo;

import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.griffin.ExpiryValidationResult;
import io.questdb.griffin.RecordToRowCopier;
import io.questdb.griffin.RecordToRowCopierUtils;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SynchronizedJob;
import io.questdb.mp.WorkerPool;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.CharSequenceLongHashMap;
import io.questdb.std.Chars;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

import static io.questdb.cairo.wal.WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE;

/**
 * Primary-only background job that reclaims storage for materialized views carrying an {@code EXPIRE ROWS}
 * policy (EXPIRE ROWS is materialized-view-only). The read-time filter (see {@code SqlParser}) already hides
 * expired rows from every query, so this job is <b>best-effort</b>: correctness never depends on it, it only
 * frees disk space. For each policied view it processes non-active <b>logical</b> partitions and reclaims via
 * a sequencer-fenced {@link WalWriter#commitWithParamsIfSeqTxn} with {@code WAL_DEDUP_MODE_REPLACE_RANGE}:
 * <ul>
 *     <li>a fully-expired partition is wiped by an <i>empty</i> REPLACE_RANGE (a pure delete that removes the
 *         whole logical partition), and</li>
 *     <li>a partially-expired partition is compacted by replacing its timestamp range with the surviving
 *         (non-expired) rows.</li>
 * </ul>
 * {@code ALTER TABLE ... DROP PARTITION} is deliberately NOT used: it is rejected for materialized views
 * ("cannot modify materialized view") and is replicated as re-compiled SQL, so it cannot run on a view.
 * REPLACE_RANGE is a data operation that works on a view's WAL writer and replicates as an ordinary WAL
 * transaction. The commit writes a new partition version and switches atomically, so a crash mid-cleanup
 * cannot lose surviving rows (the original partition stays intact until the single commit).
 * <p>
 * <b>Logical (not physical) partitions:</b> O3 can split one logical day into several physical partitions.
 * REPLACE_RANGE operates on the whole logical day, so this job collapses physical splits into their logical
 * partition and acts per logical day. Survivor totals come from the tx file ({@code getPartitionSize}, no
 * column mapping), so parquet partitions are handled too without reading raw column memory.
 * <p>
 * <b>Active-partition protection (mirrors TTL):</b> the newest (active) logical partition is never reclaimed —
 * including any earlier physical split that shares its logical day. Expired rows there stay hidden by the read
 * filter and are reclaimed once that day ages out of the active slot.
 * <p>
 * <b>Read-filter interaction:</b> the cleanup runs on its own execution context with the read-time row-expiry
 * filter DISABLED ({@code setExpiryReadFilterEnabled(false)}), so the survivor query expresses a scalar WHEN
 * predicate's keep set explicitly without being re-wrapped. Structural KEEP and raw window policies skip
 * physical cleanup because later refreshes can make a physically deleted fallback visible again.
 * <p>
 * <b>Concurrency (reclamation never deletes a row a concurrent writer back-filled):</b> the only writers to a
 * policied view are this job and the materialized-view refresh job, and BOTH guard every view write with the
 * per-view {@link MatViewState#tryLock()}. {@link #cleanupTable} takes that same lock for the whole sweep, so
 * cleanup and refresh are mutually exclusive — no back-fill can land between the survivor scan and the
 * REPLACE_RANGE commit. If a refresh holds the lock, cleanup DEFERS to a later sweep (it is idempotent and on
 * its own CLEANUP EVERY cadence). As defense-in-depth (and for the degenerate case where no view state exists,
 * e.g. materialized views disabled), each destructive commit ALSO gates on the SEQUENCER TRANSACTION: it
 * commits only when the table was fully applied at sweep start and the sequencer txn has not advanced beyond
 * the cleanup's own commits. The read filter stays authoritative for VISIBILITY regardless, so an expired row
 * is never shown even when reclamation is deferred.
 * <p>
 * A <i>bounds</i> wipe of a logical partition lying wholly below a designated-timestamp threshold ({@code ts <
 * T}) needs no survivor scan: every row there is expired.
 */
public class RowExpiryCleanupJob extends SynchronizedJob implements Closeable {
    private static final int ACTION_DROP = 1;
    private static final int ACTION_REPLACE = 2;
    private static final int ACTION_SKIP = 0;
    private static final int ACTION_UNKNOWN = -1; // bounds were not decisive; fall back to the survivor scan
    // Ceiling for the per-table failure backoff (10 minutes): a persistently failing sweep keeps
    // retrying, but never more often than this once the exponential backoff has grown to the cap.
    private static final long FAILURE_BACKOFF_CAP_MICROS = 600_000_000L;
    private static final long GLOBAL_CHECK_INTERVAL_MICROS = 1_000_000L;
    private static final Log LOG = LogFactory.getLog(RowExpiryCleanupJob.class);
    private static final long NO_LAST_RUN = Long.MIN_VALUE;
    private final BytecodeAssembler asm = new BytecodeAssembler();
    private final MicrosecondClock clock;
    private final EntityColumnFilter columnFilter = new EntityColumnFilter();
    // Per-tick snapshot of policied objects, collected under the metadata-cache read lock and then
    // processed AFTER the lock is released (never hold the cache lock during cleanup).
    private final LongList discoveredCleanupIntervals = new LongList();
    private final ObjList<String> discoveredPredicates = new ObjList<>();
    private final ObjList<TableToken> discoveredTokens = new ObjList<>();
    private final CairoEngine engine;
    // Current failure backoff per table: a failing cleanup doubles it (from one global tick up to
    // FAILURE_BACKOFF_CAP_MICROS) so a persistently failing sweep cannot re-run its full heavy work
    // on every global tick. A successful or deferred sweep removes the entry.
    private final CharSequenceLongHashMap failureBackoffMicros = new CharSequenceLongHashMap(4, 0.5, NO_LAST_RUN);
    private final CharSequenceLongHashMap lastRunByTable = new CharSequenceLongHashMap(4, 0.5, NO_LAST_RUN);
    private final double minExpiredFraction;
    // Per-cleanup snapshot of one object's non-active LOGICAL partitions.
    private final LongList partitionContentGenerations = new LongList();
    private final LongList partitionFloors = new LongList();
    private final LongList partitionNextFloors = new LongList();
    private final LongList partitionRowCounts = new LongList();
    private final CharSequenceLongHashMap scalarPartitionGenerations = new CharSequenceLongHashMap(16, 0.5, NO_LAST_RUN);
    private final StringSink scalarPartitionKey = new StringSink();
    private long lastExpiryPolicyVersion = -1;
    private long nextDiscoveryDeadlineMicros = NO_LAST_RUN;
    private long policyDiscoveryCount;
    private long scalarPartitionScanCount;
    private boolean isLastCleanupFailed;
    private SqlExecutionContextImpl sqlExecutionContext;

    public static boolean assignToPool(WorkerPool workerPool, CairoEngine engine) {
        if (!engine.getConfiguration().isMatViewRowExpiryCleanupEnabled()) {
            return false;
        }
        final RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine);
        workerPool.assign(job);
        workerPool.freeOnExit(job);
        return true;
    }

    public RowExpiryCleanupJob(CairoEngine engine) {
        this.engine = engine;
        final CairoConfiguration configuration = engine.getConfiguration();
        this.clock = configuration.getMicrosecondClock();
        this.minExpiredFraction = configuration.getMatViewRowExpiryCleanupMinExpiredFraction();
        this.sqlExecutionContext = new SqlExecutionContextImpl(engine, 1);
        this.sqlExecutionContext.with(
                configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                // The cleanup binds $1/$2 (the partition range) into the compiled-once survivor count/select
                // factories, so it needs a bind-variable service on its own execution context.
                new BindVariableServiceImpl(configuration),
                null
        );
        // The cleanup computes survivors from its own authoritative keep-filter; disable the read-time
        // row-expiry filter on this context so the survivor query is not ALSO wrapped by it (which would
        // be redundant, and would couple physical deletion to any read-filter change).
        this.sqlExecutionContext.setExpiryReadFilterEnabled(false);
    }

    /**
     * Physically reclaims expired rows from a single policied object (a fully-expired partition is a pure-delete wipe; a partial one is compacted by copying its survivors).
     * Snapshots non-active LOGICAL partition totals from a reader, classifies each as DROP/REPLACE/SKIP
     * via the keep-filter, then compacts via REPLACE_RANGE and batch-drops fully expired partitions.
     * <p>
     * Reclamation requires <b>monotonic</b> expiry: a row classified as expired now must stay expired. A
     * non-monotonic policy, including every structural KEEP/window mode, returns early WITHOUT reclaiming.
     * The read filter stays authoritative for visibility, so such a policy is query-correct but accrues
     * physical residue until a full refresh. See {@link SqlCompiler#isExpiryCleanupReclaiming}.
     */
    public boolean cleanupTable(TableToken tableToken, String predicate) {
        isLastCleanupFailed = false;
        // Serialize with the materialized-view refresh job. Both this job and refresh write the view through
        // its WAL writer, and a refresh O3 back-fill into a non-active partition landing between our survivor
        // scan and our REPLACE_RANGE commit could otherwise be deleted. Refresh guards EVERY view write with
        // MatViewState#tryLock(); we take the SAME per-view lock for the whole sweep, so cleanup and refresh
        // are mutually exclusive — no two writers sequence the view concurrently, which closes the otherwise
        // best-effort commit-window race entirely. If a refresh holds the lock we DEFER to a later sweep
        // (cleanup is idempotent and on its own CLEANUP EVERY cadence).
        final MatViewState viewState = engine.getMatViewStateStore().getViewState(tableToken);
        if (viewState != null) {
            if (viewState.isDropped() || viewState.isPendingInvalidation() || viewState.isInvalid()) {
                return false; // view being torn down / not in a refreshable state; skip
            }
            if (!viewState.tryLock()) {
                LOG.debug().$("deferred row-expiry cleanup; view busy [table=").$safe(tableToken.getTableName()).I$();
                return false;
            }
            try {
                return cleanupTable0(tableToken, predicate);
            } finally {
                viewState.unlock();
            }
        }
        // No view state (materialized views disabled, so no policied views exist in practice): fall back to
        // the per-commit sequencer-txn gate inside cleanupTable0 (still correct, best-effort).
        return cleanupTable0(tableToken, predicate);
    }

    private boolean cleanupTable0(TableToken tableToken, String predicate) {
        final String tableName = tableToken.getTableName();
        // Structural KEEP and raw window policies can reveal an older row after a later materialized-view
        // refresh removes the current winner. Preserve their physical history until cleanup has a
        // deletion-aware rebuild mechanism. The check reads the encoded policy alone, so it runs before this
        // sweep borrows a reader or a compiler; the monotonicity half of the same rule is applied below.
        if (RowExpiryUtil.isStructuralPolicy(predicate)) {
            return false;
        }

        // Freeze now() once per scalar-policy sweep, BEFORE any survivor query runs (the bounds threshold
        // and the per-partition count/select). A scalar WHEN predicate may reference now();
        // without this its survivor query would evaluate now() against an uninitialised clock and diverge from
        // the authoritative read filter (which freezes now() per query), risking deletion of visible rows.
        initNow();

        partitionContentGenerations.clear();
        partitionFloors.clear();
        partitionNextFloors.clear();
        partitionRowCounts.clear();

        final String timestampColumnName;
        // The designated timestamp column's type; partition floors are in this column's native unit, and the
        // survivor queries' $1/$2 range binds carry the same type so no unit conversion is applied to them.
        final int timestampType;
        // Fast-path threshold for a "<ts> < T"/"<ts> <= T" predicate, in the unit of the designated timestamp
        // column. The job classifies each partition from its [floor, nextFloor) bounds and does no survivor
        // scan. LONG_NULL means there is no such threshold, and the job scans.
        long timestampThreshold = Numbers.LONG_NULL;
        // Whether this policy is monotonic, i.e. physically safe to reclaim (a row expired now stays expired).
        // A non-monotonic policy (e.g. "ts > now()") would have cleanup delete a row a later read must show;
        // for such a policy we skip reclamation entirely and let the read filter enforce retention.
        boolean isCleanupMonotonic = false;
        // Whether the predicate is clock-free (deterministic). The SKIP generation cache is keyed only by
        // partition content, so it may memoize a SKIP verdict only when that verdict cannot change as time
        // passes -- i.e. for a deterministic predicate. A monotonic clock threshold (e.g.
        // "ts < dateadd('d', -30, now())") is excluded even though reclamation under it is safe: its per-row
        // verdict advances with now(), so a still-live partition cached now must be re-scanned as it ages.
        boolean isPredicateDeterministic = false;
        // The applied sequencer txn this sweep's reader reflects; the destructive-commit gate is baselined on
        // it (not a fresh txnTracker.getSeqTxn() after the reader closes) so the predicate and the gate share
        // one snapshot -- see the authoritative-predicate re-read inside the reader block.
        long readerSeqTxn = 0;

        // Snapshot non-active LOGICAL partitions. Totals come from the tx file (no column mapping, so
        // parquet partitions are fine), and physical O3 splits of the same logical day are collapsed
        // into one entry so DROP/REPLACE act on the whole day (see class javadoc). The newest (active)
        // logical partition — and any earlier split that shares its logical day — is never touched.
        try (TableReader reader = engine.getReader(tableToken)) {
            final TableReaderMetadata metadata = reader.getMetadata();
            final int timestampIndex = metadata.getTimestampIndex();
            if (timestampIndex < 0) {
                return false; // non-timestamp table; nothing to expire (should not happen for a WAL table)
            }
            timestampColumnName = metadata.getColumnName(timestampIndex);
            timestampType = metadata.getColumnType(timestampIndex);

            // Tie the predicate and the seqTxn baseline to ONE consistent snapshot: this reader. `predicate`
            // was snapshotted at sweep-start discovery (runSerially), well before this reader opened. If an
            // ALTER SET/DROP EXPIRE applied in between, the reader reflects the NEW policy, so classifying with
            // the stale discovery predicate could physically delete rows the current policy keeps -- and a
            // passthrough view stores its own copy, so the read filter cannot resurrect them. Re-read the
            // authoritative predicate here and DEFER the view this sweep if it changed (the next tick
            // re-discovers it). A policy change that applies AFTER this reader opened advances the sequencer
            // past reader.getSeqTxn(), so the per-commit gate below -- baselined on that same reader txn --
            // catches that window too.
            final String authoritativePredicate = metadata.getExpiryPredicate();
            if (authoritativePredicate == null || !authoritativePredicate.equals(predicate)) {
                return false;
            }
            readerSeqTxn = reader.getSeqTxn();

            final int partitionCount = reader.getPartitionCount();
            // Active-partition protection: with < 2 partitions there is only the active partition.
            if (partitionCount < 2) {
                return false;
            }
            final TxReader txReader = reader.getTxFile();
            final long activeLogicalFloor = txReader.getLogicalPartitionTimestamp(
                    reader.getPartitionTimestampByIndex(partitionCount - 1));
            long prevLogicalFloor = Numbers.LONG_NULL;
            for (int i = 0, n = partitionCount - 1; i < n; i++) {
                final long physicalPartitionTimestamp = reader.getPartitionTimestampByIndex(i);
                final long logicalFloor = txReader.getLogicalPartitionTimestamp(physicalPartitionTimestamp);
                if (logicalFloor == activeLogicalFloor) {
                    continue; // an earlier split of the active logical day; leave the whole active day alone
                }
                final long size = txReader.getPartitionSize(i);
                if (size <= 0) {
                    continue;
                }
                if (logicalFloor != prevLogicalFloor) {
                    prevLogicalFloor = logicalFloor;
                    partitionFloors.add(logicalFloor);
                    partitionNextFloors.add(txReader.getNextLogicalPartitionTimestamp(logicalFloor));
                    partitionRowCounts.add(size);
                    partitionContentGenerations.add(mixPartitionGeneration(
                            physicalPartitionTimestamp,
                            txReader.getPartitionNameTxn(i),
                            size
                    ));
                } else {
                    // Another physical split of the current logical partition: accumulate its rows and fold
                    // its persisted name transaction into the logical partition's content generation.
                    final int last = partitionRowCounts.size() - 1;
                    partitionRowCounts.setQuick(last, partitionRowCounts.getQuick(last) + size);
                    partitionContentGenerations.setQuick(last, mixPartitionGeneration(
                            partitionContentGenerations.getQuick(last),
                            txReader.getPartitionNameTxn(i),
                            size
                    ));
                }
            }

            // Resolve the cleanup-safety (monotonicity) gate, whether the predicate is clock-free, and the
            // fast-path timestamp threshold once per table (now() was already frozen above). The gate
            // authoritatively decides whether physical reclamation is safe; the clock-free flag decides
            // whether the SKIP generation cache is sound; only a scalar "<ts> < T" WHEN predicate additionally
            // has a bounds threshold.
            if (partitionFloors.size() > 0) {
                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    // One classification pass yields both the monotonicity gate and the clock-free flag.
                    final ExpiryValidationResult classification =
                            compiler.validateExpiryPredicateOnMetadata(sqlExecutionContext, metadata, predicate, 0);
                    isCleanupMonotonic = classification.isMonotonic();
                    isPredicateDeterministic = classification.isDeterministic();
                    timestampThreshold = compiler.expiryTimestampThreshold(
                            sqlExecutionContext, metadata, predicate, timestampColumnName);
                } catch (SqlException e) {
                    // A predicate that fails to classify is treated as non-monotonic: skip reclamation and let
                    // the read filter stay authoritative (mirrors isExpiryCleanupReclaiming's swallow-and-skip).
                    return false;
                }
            }
        }

        // Non-monotonic policy (e.g. a now()-referencing predicate that does not reduce to a "<ts> < T"
        // threshold, like "ts > now()"): the background job must NOT physically delete rows it might have to
        // show again as time advances. The read filter stays authoritative for correctness; here we simply
        // skip disk reclamation for this policy. Monotonic clock-free predicates and "ts < now()"-style
        // thresholds proceed normally.
        if (!RowExpiryUtil.isReclaimingPolicy(predicate, isCleanupMonotonic)) {
            return false;
        }

        boolean isWorkDone = false;
        // Enable the SKIP generation cache only when the bounds fast path is unavailable AND the predicate is
        // clock-free. The cache key is partition content alone, so for a clock-based threshold the same data
        // yields a different expiry verdict as now() advances; caching a SKIP there would suppress the later
        // re-scan that must reclaim the partition once it ages past the threshold.
        final boolean isScalarGenerationCacheEnabled = timestampThreshold == Numbers.LONG_NULL && isPredicateDeterministic;
        // A REPLACE rewrites every surviving row of a partition, so the job does one only when a large
        // enough fraction of that partition is expired. A clock threshold moves through a partition across
        // many sweeps. With daily partitions and the default cleanup interval of 1 hour, the partition that
        // holds the threshold is partly expired at EVERY sweep. Without the minimum fraction, the job copies
        // the survivors of that partition about 11 times before it drops the partition. With the minimum
        // fraction at the default 0.5, each copy is about one half of the previous copy, and the total is
        // about one copy for each row. The expired rows that stay on disk are not more than one partition,
        // and the read filter hides them.
        // A deterministic (clock-free) predicate has no minimum fraction. Its result for a row cannot change
        // as time passes, so only a refresh that back-fills rows into a partition can raise the expired
        // fraction of that partition. One REPLACE clears the partition, and each later sweep classifies it
        // as SKIP until the next back-fill (the generation cache stores that result when there is no bounds
        // threshold). The job copies such a partition one time for each back-fill, and not one time for each
        // sweep. A minimum fraction here would keep expired rows on disk for an unlimited time, because
        // nothing can raise the expired fraction of a partition that no writer touches. As a result, the two
        // flags are mutually exclusive.
        final boolean isCompactionGated = !isPredicateDeterministic && minExpiredFraction > 0;
        if (scalarPartitionGenerations.size() > 16_384) {
            scalarPartitionGenerations.clear();
        }
        // Concurrency model. A REPLACE or count-DROP must never physically delete a row a concurrent writer
        // back-filled into a non-active partition since the survivor scan. On a WAL table the scan reads only
        // APPLIED state, so a committed-but-unapplied back-fill is invisible to a recount; we instead gate on
        // the sequencer transaction: require the table FULLY APPLIED at the start (so the scan is complete)
        // and UNCHANGED-BY-OTHERS through each destructive commit (our own commits are tracked in
        // expectedSeqTxn). A non-WAL table writes synchronously, so the survivor recount is authoritative
        // there. Every destructive path, including a bounds-based DROP, uses the same conditional sequencer
        // allocation because a concurrent policy change can make rows below the old threshold visible again.
        final boolean isWal = tableToken.isWal();
        if (isWal && !engine.getTableSequencerAPI().isTxnTrackerInitialised(tableToken)) {
            engine.getTableSequencerAPI().initTxnTracker(
                    tableToken,
                    readerSeqTxn,
                    engine.getTableSequencerAPI().lastTxn(tableToken)
            );
        }
        final SeqTxnTracker txnTracker = isWal ? engine.getTableSequencerAPI().getTxnTracker(tableToken) : null;
        // Baseline on the reader's own applied txn (readerSeqTxn), NOT a fresh txnTracker.getSeqTxn() here: the
        // survivor scan and the authoritative predicate both came from that reader snapshot, so any policy
        // change or write that applied after the reader opened advances the sequencer past this baseline and
        // trips the per-commit gate below (the mid-sweep window). A fresh getSeqTxn() would instead adopt
        // the post-reader sequencer state and could let a stale-predicate wipe commit.
        long expectedSeqTxn = isWal ? readerSeqTxn : 0;
        // For WAL, only attempt racy reclamation when the writer is still caught up to the reader snapshot (no
        // apply since it opened that the survivor scan would miss). Non-WAL is always allowed (synchronous,
        // recount-checked).
        final boolean racyOpsAllowed = !isWal || txnTracker.getWriterTxn() == expectedSeqTxn;

        // Decide and act (reader closed). All reclamation is via REPLACE_RANGE on the WAL writer: a fully
        // expired partition is wiped (empty REPLACE_RANGE = pure delete), a partially expired one is compacted
        // to its survivors. DROP PARTITION via SQL is NOT used — it is rejected for materialized views ("cannot
        // modify materialized view") and every policied object is a WAL mat view. The survivor count() and
        // SELECT * are compiled ONCE per sweep with $1/$2 bind variables for the partition range and rebound
        // per partition; both compile lazily, so a pure bounds-wipe sweep compiles neither.
        final String keepFilter = RowExpiryUtil.buildRowExpiryKeepFilter(predicate);
        final String quotedTable = RowExpiryUtil.quoteIdentifier(tableName);
        final String quotedTimestamp = RowExpiryUtil.quoteIdentifier(timestampColumnName);
        final String tail = " WHERE (" + keepFilter + ") AND " + quotedTimestamp
                + " >= $1 AND " + quotedTimestamp + " < $2";
        final String countSql = "SELECT count() FROM " + quotedTable + tail;
        final String selectSql = "SELECT * FROM " + quotedTable + tail;

        SqlCompiler cleanupCompiler = null;
        RecordCursorFactory countFactory = null;
        RecordCursorFactory selectFactory = null;
        RecordToRowCopier survivorCopier = null;
        int selectTsIndex = -1;
        WalWriter walWriter = null;
        // Bound every survivor query on this sweep to the MAT_VIEW_REFRESH per-workload memory budget.
        // A bound tracker makes an oversized scalar survivor query trip the configured limit and DEFER to a
        // later sweep. EXPIRE ROWS is materialized-view-only, so cleanup shares the mat-view refresh budget,
        // exactly as MatViewRefreshJob binds its own tracker before running inner SQL.
        final MemoryTracker memoryTracker = engine.getMemoryTrackerProvider().acquire(
                sqlExecutionContext.getSecurityContext(),
                tableToken.getTableId(),
                MemoryTrackerWorkload.MAT_VIEW_REFRESH
        );
        sqlExecutionContext.setMemoryTracker(memoryTracker);
        try {
            for (int i = 0, n = partitionFloors.size(); i < n; i++) {
                final long floorTs = partitionFloors.getQuick(i);
                final long nextFloorTs = partitionNextFloors.getQuick(i);
                final long rowCount = partitionRowCounts.getQuick(i);
                try {
                    // Fast path: classify by the partition's [floor, nextFloor) bounds vs the threshold,
                    // with no scan. Only when the bounds straddle the threshold (or there is no threshold)
                    // do we fall back to the authoritative survivor count scan.
                    final int boundsAction = fastClassifyByBounds(timestampThreshold, floorTs, nextFloorTs);
                    if (racyOpsAllowed && boundsAction == ACTION_DROP) {
                        // Bounds wipe: the whole logical partition is below the designated-ts threshold, so
                        // every row (incl. any concurrent back-fill) is expired. Reclaim with a no-scan empty
                        // REPLACE_RANGE (pure delete of the range).
                        if (walWriter == null) {
                            walWriter = engine.getWalWriter(tableToken);
                        }
                        // The sequencer atomically gates and allocates this transaction. A concurrent local or
                        // replicated producer advances the authoritative order and makes the cleanup commit defer.
                        if (commitWithFence(walWriter, floorTs, nextFloorTs, txnTracker, expectedSeqTxn)) {
                            expectedSeqTxn++; // our accepted commit advanced the sequencer by exactly one txn
                            isWorkDone = true;
                            LOG.info().$("reclaimed fully-expired partition [table=").$safe(tableName)
                                    .$(", partitionTs=").$ts(floorTs).I$();
                        } else {
                            LOG.info().$("deferred expired-rows partition wipe; table changed concurrently [table=")
                                    .$safe(tableName).$(", partitionTs=").$ts(floorTs).I$();
                            // Fence rejected: an external txn advanced the sequencer past our reader-snapshot
                            // baseline. expectedSeqTxn is intentionally not re-read (a fresh baseline could
                            // commit a stale-predicate wipe), and sequencer txns only move forward, so every
                            // remaining partition is a certain rejection. Stop the sweep instead of paying a
                            // count scan + survivor copy + event fsync + openNewSegment for each one; the next
                            // sweep opens a fresh reader and re-baselines.
                            break;
                        }
                    } else if (racyOpsAllowed && boundsAction == ACTION_UNKNOWN) {
                        if (isScalarGenerationCacheEnabled
                                && isScalarPartitionGenerationCurrent(tableToken, predicate, i)) {
                            continue;
                        }
                        final int action;
                        if (countFactory == null) {
                            if (cleanupCompiler == null) {
                                cleanupCompiler = engine.getSqlCompiler();
                            }
                            bindPartitionRange(timestampType, floorTs, nextFloorTs); // declare $1/$2 types before compiling
                            countFactory = cleanupCompiler.compile(countSql, sqlExecutionContext).getRecordCursorFactory();
                        }
                        action = classifyPartition(countFactory, timestampType, floorTs, nextFloorTs, rowCount, isCompactionGated);
                        if (action == ACTION_DROP) {
                            // Fully expired -> no-scan empty REPLACE_RANGE wipe, gated on the sequencer txn so a
                            // row a concurrent writer back-filled since the survivor scan is never deleted.
                            if (walWriter == null) {
                                walWriter = engine.getWalWriter(tableToken);
                            }
                            if (commitWithFence(walWriter, floorTs, nextFloorTs, txnTracker, expectedSeqTxn)) {
                                expectedSeqTxn++; // our accepted commit advanced the sequencer by exactly one txn
                                isWorkDone = true;
                                LOG.info().$("reclaimed fully-expired partition [table=").$safe(tableName)
                                        .$(", partitionTs=").$ts(floorTs).I$();
                            } else {
                                LOG.info().$("deferred expired-rows partition wipe; table changed concurrently [table=")
                                        .$safe(tableName).$(", partitionTs=").$ts(floorTs).I$();
                                // Fence rejected; every remaining partition would reject the same way. Stop the
                                // sweep (see the bounds-wipe branch above); the next sweep re-baselines.
                                break;
                            }
                        } else if (action == ACTION_REPLACE) {
                            if (walWriter == null) {
                                walWriter = engine.getWalWriter(tableToken);
                            }
                            if (selectFactory == null) {
                                if (cleanupCompiler == null) {
                                    cleanupCompiler = engine.getSqlCompiler();
                                }
                                bindPartitionRange(timestampType, floorTs, nextFloorTs);
                                // Build the factory into a local and assign the field ONLY after the copier
                                // is built: if generateCopier() throws, selectFactory must stay null so the
                                // next partition rebuilds it cleanly (otherwise a non-null factory with a null
                                // copier would NPE in replacePartition), and the local is freed so it can't leak.
                                final RecordCursorFactory f = cleanupCompiler.compile(selectSql, sqlExecutionContext).getRecordCursorFactory();
                                try {
                                    selectTsIndex = f.getMetadata().getColumnIndex(timestampColumnName);
                                    columnFilter.of(f.getMetadata().getColumnCount());
                                    survivorCopier = RecordToRowCopierUtils.generateCopier(asm,
                                            f.getMetadata(), walWriter.getMetadata(), columnFilter, engine.getConfiguration());
                                } catch (Throwable th) {
                                    Misc.free(f);
                                    throw th;
                                }
                                selectFactory = f;
                            }
                            if (replacePartition(selectFactory, survivorCopier, selectTsIndex, walWriter,
                                    tableName, timestampType, floorTs, nextFloorTs, txnTracker, expectedSeqTxn)) {
                                expectedSeqTxn++; // our REPLACE commit advanced the sequencer by exactly one txn
                                isWorkDone = true;
                            } else {
                                // Fence rejected; every remaining partition would reject too. Stop the sweep
                                // (replacePartition already logged the deferral); the next sweep re-baselines.
                                break;
                            }
                        } else if (isScalarGenerationCacheEnabled) {
                            rememberScalarPartitionGeneration(tableToken, predicate, i);
                        }
                    }
                    // ACTION_SKIP, or a WAL table not caught up -> defer this partition to a later sweep.
                } catch (Throwable th) {
                    // A REPLACE that failed mid-append leaves uncommitted rows in the (reused) writer.
                    // Free it so those rows are rolled back on close and cannot be committed into the
                    // NEXT partition's REPLACE_RANGE (which would resurrect them outside the deleted
                    // range). A fresh writer is acquired on the next REPLACE.
                    walWriter = Misc.free(walWriter);
                    isLastCleanupFailed = true;
                    LOG.error().$("row-expiry partition cleanup failed [table=").$safe(tableName)
                            .$(", partitionTs=").$ts(floorTs)
                            .$(", msg=").$safe(th.getMessage())
                            .I$();
                }
            }
        } finally {
            try {
                // Free the survivor-scan factories and compiler before releasing the memory tracker: the
                // provider pool re-inits the tracker and asserts used == 0, so all memory charged to it must
                // be released first. The WAL writer goes last, because for a dropped table returnToPool()
                // returns false and WalWriter.close() runs doClose(), whose sequencer notification and file
                // I/O can throw CairoException; Misc.free only catches IOException, so a throw here must not
                // strand the memory-tracker release in the inner finally below.
                selectFactory = Misc.free(selectFactory);
                countFactory = Misc.free(countFactory);
                cleanupCompiler = Misc.free(cleanupCompiler);
                walWriter = Misc.free(walWriter);
            } finally {
                // Always release the sweep's memory tracker: clear the context slot, then recycle the
                // tracker to the provider pool so its used-bytes counter resets before the next sweep binds
                // a fresh one.
                sqlExecutionContext.setMemoryTracker(null);
                memoryTracker.close();
            }
        }
        return isWorkDone;
    }

    @Override
    public void close() {
        sqlExecutionContext = Misc.free(sqlExecutionContext);
    }

    @TestOnly
    public long getPolicyDiscoveryCount() {
        return policyDiscoveryCount;
    }

    @TestOnly
    public long getScalarPartitionScanCount() {
        return scalarPartitionScanCount;
    }

    @TestOnly
    public boolean runNow() {
        return runSerially();
    }

    @Override
    protected boolean runSerially() {
        final long nowMicros = clock.getTicks();
        final long expiryPolicyVersion = engine.getMetadataCache().getExpiryPolicyVersion();
        if (expiryPolicyVersion == lastExpiryPolicyVersion
                && nextDiscoveryDeadlineMicros != NO_LAST_RUN
                && nowMicros < nextDiscoveryDeadlineMicros) {
            return false;
        }
        lastExpiryPolicyVersion = expiryPolicyVersion;

        // Skip discovery when no table carries an EXPIRE ROWS policy (and the metadata
        // cache has finished hydrating, so that signal is trustworthy).
        if (!engine.getMetadataCache().mayHaveExpiryPolicy()) {
            if (lastRunByTable.size() > 0) {
                lastRunByTable.clear();
                failureBackoffMicros.clear();
            }
            nextDiscoveryDeadlineMicros = nowMicros + GLOBAL_CHECK_INTERVAL_MICROS;
            return false;
        }

        // Discover policied objects via the metadata cache. Snapshot (token, predicate, interval) under
        // the read lock, then RELEASE the lock before doing ANY cleanup — cleanup borrows readers/writers
        // and compiles SQL, none of which may run while holding the cache lock.
        discoveredTokens.clear();
        discoveredPredicates.clear();
        discoveredCleanupIntervals.clear();
        // One pass over the metadata cache, collecting only the policied tables' (token, predicate,
        // interval) — instead of snapshotting and re-looking-up the entire table registry every tick.
        // Snapshot under the read lock, then RELEASE it before ANY cleanup: cleanup borrows readers/writers
        // and compiles SQL, none of which may run while holding the cache lock.
        policyDiscoveryCount++;
        try (MetadataCacheReader metadataRO = engine.getMetadataCache().readLock()) {
            metadataRO.collectPoliciedTables(discoveredTokens, discoveredPredicates, discoveredCleanupIntervals);
        }
        if (discoveredTokens.size() == 0 && engine.getMetadataCache().mayHaveExpiryPolicy()) {
            // A metadata writer can publish a pending policy ID before the corresponding table has entered
            // the startup cache. Resolve that one-time incomplete-cache state authoritatively; subsequent
            // discovery uses the published active-table snapshot and remains O(number of policies).
            engine.getMetadataCache().hydrateAllTables();
            try (MetadataCacheReader metadataRO = engine.getMetadataCache().readLock()) {
                metadataRO.collectPoliciedTables(discoveredTokens, discoveredPredicates, discoveredCleanupIntervals);
            }
        }

        if (discoveredTokens.size() == 0) {
            if (lastRunByTable.size() > 0) {
                lastRunByTable.clear();
                failureBackoffMicros.clear();
            }
            nextDiscoveryDeadlineMicros = nowMicros + GLOBAL_CHECK_INTERVAL_MICROS;
            return false;
        }

        // Bound the throttle map over a long process lifetime: a dropped/renamed view never removes its
        // entry, so if it has accumulated far more entries than there are live policied views, reset it. The
        // throttle is best-effort (cleanup is idempotent and globally rate-limited), so the only effect is one
        // extra sweep per live view this round before each CLEANUP EVERY cadence re-applies.
        if (lastRunByTable.size() > 4 * discoveredTokens.size()) {
            lastRunByTable.clear();
            failureBackoffMicros.clear();
        }

        boolean isWorkDone = false;
        long nextDeadlineMicros = Long.MAX_VALUE;
        for (int i = 0, n = discoveredTokens.size(); i < n; i++) {
            final TableToken tableToken = discoveredTokens.getQuick(i);
            final String predicate = discoveredPredicates.getQuick(i);
            long cleanupIntervalMicros = discoveredCleanupIntervals.getQuick(i);
            if (cleanupIntervalMicros <= 0) {
                // Only reachable from a truncated/legacy _meta (DDL always stores a positive default); fall
                // back to the default cadence so a degenerate interval does not sweep on every global tick.
                cleanupIntervalMicros = RowExpiryUtil.DEFAULT_CLEANUP_INTERVAL_MICROS;
            }
            final CharSequence tableKey = tableToken.getTableName();

            // A table in a failure streak is throttled by its current backoff instead of the
            // CLEANUP EVERY cadence, so retries neither hammer every global tick nor wait a
            // full (possibly hours-long) cadence for a transient failure to clear.
            final long lastRun = lastRunByTable.get(tableKey);
            final long backoffMicros = failureBackoffMicros.get(tableKey);
            final long requiredGapMicros = backoffMicros != NO_LAST_RUN ? backoffMicros : cleanupIntervalMicros;
            if (lastRun != NO_LAST_RUN && nowMicros - lastRun < requiredGapMicros) {
                nextDeadlineMicros = Math.min(nextDeadlineMicros, lastRun + requiredGapMicros);
                continue;
            }

            // Defensive primary-only/safety guard: the object may have been dropped/renamed since the
            // snapshot, or the policy removed. Skip if the token no longer resolves; wrap per-object work
            // so one bad table cannot kill the whole sweep.
            if (engine.getTableTokenIfExists(tableToken.getTableName()) == null) {
                continue;
            }
            try {
                if (cleanupTable(tableToken, predicate)) {
                    isWorkDone = true;
                    // Advisory: reclaiming from an AGGREGATING (non-passthrough) view is best-effort - a later
                    // incremental/full refresh can regenerate reclaimed rows from base rows that still exist.
                    // Surface it so operators can align base-table retention with the EXPIRE ROWS horizon.
                    final MatViewDefinition def = engine.getDependentViewGraph().getViewDefinition(tableToken);
                    if (def != null && !def.isPassthrough()) {
                        LOG.advisory().$("reclaimed expired rows from an aggregating materialized view; a later refresh may regenerate them - align base-table retention with the EXPIRE ROWS horizon [view=")
                                .$safe(tableKey).I$();
                    }
                }
            } catch (Throwable th) {
                isLastCleanupFailed = true;
                LOG.error().$("row-expiry cleanup failed [table=").$safe(tableKey)
                        .$(", msg=").$safe(th.getMessage())
                        .I$();
            }
            if (isLastCleanupFailed) {
                // Exponential backoff: the first failure retries after one global tick, each further
                // failure doubles the wait up to FAILURE_BACKOFF_CAP_MICROS.
                final long prevBackoffMicros = failureBackoffMicros.get(tableKey);
                final long nextBackoffMicros = prevBackoffMicros == NO_LAST_RUN
                        ? GLOBAL_CHECK_INTERVAL_MICROS
                        : Math.min(prevBackoffMicros * 2, FAILURE_BACKOFF_CAP_MICROS);
                final String key = Chars.toString(tableKey);
                failureBackoffMicros.put(key, nextBackoffMicros);
                lastRunByTable.put(key, nowMicros);
                nextDeadlineMicros = Math.min(nextDeadlineMicros, nowMicros + nextBackoffMicros);
            } else {
                failureBackoffMicros.remove(tableKey);
                lastRunByTable.put(Chars.toString(tableKey), nowMicros);
                nextDeadlineMicros = Math.min(nextDeadlineMicros, nowMicros + cleanupIntervalMicros);
            }
        }
        nextDiscoveryDeadlineMicros = nextDeadlineMicros == Long.MAX_VALUE
                ? nowMicros + GLOBAL_CHECK_INTERVAL_MICROS
                : nextDeadlineMicros;
        return isWorkDone;
    }

    /**
     * Classifies a logical partition from its {@code [floorTs, nextFloorTs)} bounds against the fast-path
     * timestamp threshold T (from a {@code <ts> < T}/{@code <ts> <= T} predicate), with no row scan:
     * <ul>
     *   <li>{@code nextFloorTs <= T}: every row has {@code ts < nextFloorTs <= T}, so all expired -> DROP;</li>
     *   <li>{@code floorTs > T}: every row has {@code ts >= floorTs > T}, so none expired -> SKIP.</li>
     * </ul>
     * Both rules fire only when the WHOLE partition range is decisively on one side of T, so a partition
     * holding any live row is never dropped. Returns {@link #ACTION_UNKNOWN} when there is no threshold or
     * the bounds straddle T, so the caller falls back to the authoritative survivor count scan.
     */
    private static int fastClassifyByBounds(long timestampThreshold, long floorTs, long nextFloorTs) {
        if (timestampThreshold == Numbers.LONG_NULL) {
            return ACTION_UNKNOWN;
        }
        if (nextFloorTs <= timestampThreshold) {
            return ACTION_DROP;
        }
        if (floorTs > timestampThreshold) {
            return ACTION_SKIP;
        }
        return ACTION_UNKNOWN;
    }

    private int classifyPartition(
            RecordCursorFactory countFactory,
            int timestampType,
            long floorTs,
            long nextFloorTs,
            long rowCount,
            boolean isCompactionGated
    ) throws SqlException {
        scalarPartitionScanCount++;
        // Classify with a read-only count() scan, then copy only REPLACE partitions. This is deliberately
        // NOT folded into the copy: for an arbitrary predicate the class of a partition is unknown until it
        // is scanned, and the common case (a fully-live recent partition -> SKIP) is decided here with a cheap
        // read-only scan and no WAL writes. Folding the count into the copy would instead append every survivor
        // to the WAL writer and then roll back the SKIP partitions — turning the common fully-live partition
        // from a read-only scan into a scan plus discarded WAL write I/O. Only the (few) expired partitions
        // pay for a second scan.
        final long survivors = countSurvivors(countFactory, timestampType, floorTs, nextFloorTs);
        // survivors == 0 -> fully expired (DROP/wipe); 0 < survivors < rowCount -> partially expired (REPLACE
        // compacts to survivors); survivors == rowCount -> nothing expired (SKIP). rowCount is the reader
        // snapshot, so only act when something is clearly expired.
        if (survivors == 0) {
            return ACTION_DROP;
        }
        if (survivors >= rowCount) {
            return ACTION_SKIP;
        }
        // The partition is partly expired. When isCompactionGated is true, the job compacts the partition
        // only when the expired rows are a large enough fraction of it. Below that fraction the expired rows
        // stay on disk, the read filter hides them, and a later sweep removes them in one pass, when more of
        // the partition is expired.
        return !isCompactionGated || rowCount - survivors >= minExpiredFraction * rowCount
                ? ACTION_REPLACE
                : ACTION_SKIP;
    }

    // Binds $1 = partition floor (inclusive), $2 = next floor (exclusive). The interval optimiser prunes to
    // exactly this logical partition (an interval forward scan), so the count/select factories are compiled
    // ONCE per sweep and merely rebound per partition rather than re-parsed + re-codegen'd from a literal.
    // The floors are in the designated timestamp column's native unit, so the bind variables are typed with
    // that column's type (timestampType): a TIMESTAMP_NS column's nano floors bind as nanos, keeping the
    // survivor scan's interval in the same unit as the REPLACE_RANGE commit. Typing them as micros would
    // make the interval evaluation re-scale the already-nano floors and fail every NS survivor query.
    private void bindPartitionRange(int timestampType, long floorTs, long nextFloorTs) throws SqlException {
        final BindVariableService bind = sqlExecutionContext.getBindVariableService();
        bind.setTimestampWithType(0, timestampType, floorTs);
        bind.setTimestampWithType(1, timestampType, nextFloorTs);
    }

    private boolean commitWithFence(
            WalWriter walWriter,
            long floorTs,
            long nextFloorTs,
            SeqTxnTracker txnTracker,
            long expectedSeqTxn
    ) {
        if (txnTracker == null) {
            walWriter.commitWithParams(floorTs, nextFloorTs, WAL_DEDUP_MODE_REPLACE_RANGE);
            return true;
        }
        return walWriter.commitWithParamsIfSeqTxn(
                expectedSeqTxn,
                floorTs,
                nextFloorTs,
                WAL_DEDUP_MODE_REPLACE_RANGE
        );
    }

    private long countSurvivors(RecordCursorFactory countFactory, int timestampType, long floorTs, long nextFloorTs) throws SqlException {
        bindPartitionRange(timestampType, floorTs, nextFloorTs);
        try (RecordCursor cursor = countFactory.getCursor(sqlExecutionContext)) {
            if (cursor.hasNext()) {
                return cursor.getRecord().getLong(0);
            }
        }
        return 0;
    }

    private void initNow() {
        sqlExecutionContext.initNow();
    }

    private boolean isScalarPartitionGenerationCurrent(TableToken tableToken, String predicate, int partitionIndex) {
        scalarPartitionKey.clear();
        scalarPartitionKey.put(tableToken.getDirName()).putAscii(':').put(predicate).putAscii(':')
                .put(partitionFloors.getQuick(partitionIndex));
        return scalarPartitionGenerations.get(scalarPartitionKey)
                == partitionContentGenerations.getQuick(partitionIndex);
    }

    private static long mixPartitionGeneration(long seed, long nameTxn, long rowCount) {
        long generation = seed * 31 + nameTxn;
        return generation * 31 + rowCount;
    }

    private void rememberScalarPartitionGeneration(TableToken tableToken, String predicate, int partitionIndex) {
        scalarPartitionKey.clear();
        scalarPartitionKey.put(tableToken.getDirName()).putAscii(':').put(predicate).putAscii(':')
                .put(partitionFloors.getQuick(partitionIndex));
        scalarPartitionGenerations.put(
                Chars.toString(scalarPartitionKey),
                partitionContentGenerations.getQuick(partitionIndex)
        );
    }

    private boolean replacePartition(
            RecordCursorFactory selectFactory,
            RecordToRowCopier copier,
            int cursorTimestampIndex,
            WalWriter walWriter,
            String tableName,
            int timestampType,
            long floorTs,
            long nextFloorTs,
            SeqTxnTracker txnTracker,
            long expectedSeqTxn
    ) throws SqlException {
        // selectFactory + copier + cursorTimestampIndex are compiled/built ONCE per sweep and rebound here
        // per partition (the SQL uses $1/$2 bind variables for the partition range). The copier is safe to
        // reuse across partitions because the concurrency gate below defers on ANY concurrent transaction,
        // so a structural ALTER cannot change the column layout mid-sweep without the REPLACE deferring.
        bindPartitionRange(timestampType, floorTs, nextFloorTs);
        long appended = 0;
        try (RecordCursor cursor = selectFactory.getCursor(sqlExecutionContext)) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                final long timestamp = record.getTimestamp(cursorTimestampIndex);
                final TableWriter.Row row = walWriter.newRow(timestamp);
                copier.copy(sqlExecutionContext, record, row);
                row.append();
                appended++;
            }
        }
        // The sequencer atomically validates that no external transaction followed the survivor-scan baseline
        // and allocates this REPLACE transaction in the same ordering decision. WalWriter prepares and syncs
        // the event first, then rolls the unsequenced event and appended survivors back when the fence rejects.
        // An empty survivor set is a legitimate fully-expired partition and commits as a pure-delete range.
        if (!commitWithFence(walWriter, floorTs, nextFloorTs, txnTracker, expectedSeqTxn)) {
            LOG.info().$("deferred expired-rows compaction; table changed concurrently [table=").$safe(tableName)
                    .$(", partitionTs=").$ts(floorTs).I$();
            return false;
        }
        LOG.info().$(appended == 0 ? "reclaimed fully-expired partition [table=" : "compacted expired-rows partition [table=")
                .$safe(tableName).$(", partitionTs=").$ts(floorTs).I$();
        return true;
    }
}
