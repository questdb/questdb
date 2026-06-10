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

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.griffin.RecordToRowCopier;
import io.questdb.griffin.RecordToRowCopierUtils;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.CharSequenceLongHashMap;
import io.questdb.std.Chars;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.str.StringSink;

import java.io.Closeable;

import static io.questdb.cairo.wal.WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE;

/**
 * Primary-only background job that reclaims storage for materialized views carrying an {@code EXPIRE ROWS}
 * policy (EXPIRE ROWS is materialized-view-only). The read-time filter (see {@code SqlParser}) already hides
 * expired rows from every query, so this job is <b>best-effort</b>: correctness never depends on it, it only
 * frees disk space. For each policied view it processes non-active <b>logical</b> partitions and reclaims via
 * {@link WalWriter#commitWithParams} with {@code WAL_DEDUP_MODE_REPLACE_RANGE}:
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
 * filter DISABLED ({@code setExpiryReadFilterEnabled(false)}), so the survivor query expresses the keep set
 * explicitly — a CASE keep-filter for a WHEN predicate, or {@code LATEST ON} for KEEP LATEST — without being
 * re-wrapped.
 * <p>
 * <b>Concurrency (reclamation never deletes a row a concurrent writer back-filled):</b> survivors are computed
 * on a separate handle, so a non-expired row could be back-filled into a non-active partition between the
 * survivor scan and the commit. On a WAL table a recount cannot detect this — the reader sees only APPLIED
 * state, while the back-fill may be committed-but-not-yet-applied. The job therefore gates on the SEQUENCER
 * TRANSACTION: it attempts reclamation only when the table is fully applied at the start (so the survivor scan
 * is complete) and the sequencer txn has not advanced beyond the cleanup's own commits by the moment of each
 * commit; otherwise it DEFERS to the next sweep. The trade-off is that a concurrently-written view defers
 * reclamation until a quiescent sweep. The read filter stays authoritative for VISIBILITY, so an expired row
 * is never shown even when reclamation is deferred.
 * <p>
 * A <i>bounds</i> wipe of a logical partition lying wholly below a designated-timestamp threshold ({@code ts <
 * T}) needs no survivor scan: every row there is expired. KEEP LATEST has no such threshold and always uses
 * the survivor-count scan.
 */
public class RowExpiryCleanupJob extends SynchronizedJob implements Closeable {
    private static final int ACTION_DROP = 1;
    private static final int ACTION_REPLACE = 2;
    private static final int ACTION_SKIP = 0;
    private static final int ACTION_UNKNOWN = -1; // bounds were not decisive; fall back to the survivor scan
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
    private final CharSequenceLongHashMap lastRunByTable = new CharSequenceLongHashMap(4, 0.5, NO_LAST_RUN);
    // Per-cleanup snapshot of one object's non-active LOGICAL partitions.
    private final LongList partitionFloors = new LongList();
    private final LongList partitionNextFloors = new LongList();
    private final LongList partitionRowCounts = new LongList();
    private long lastGlobalCheckMicros = NO_LAST_RUN;
    private SqlExecutionContextImpl sqlExecutionContext;

    public RowExpiryCleanupJob(CairoEngine engine) {
        this.engine = engine;
        final CairoConfiguration configuration = engine.getConfiguration();
        this.clock = configuration.getMicrosecondClock();
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
     * Physically reclaims expired rows from a single policied object, in place (no copy, no populate).
     * Snapshots non-active LOGICAL partition totals from a reader, classifies each as DROP/REPLACE/SKIP
     * via the keep-filter, then compacts via REPLACE_RANGE and batch-drops fully expired partitions.
     */
    public boolean cleanupTable(TableToken tableToken, String predicate, long cleanupIntervalMicros) {
        final String tableName = tableToken.getTableName();
        // KEEP LATEST (relative) policies reclaim via a different survivor query (the global latest-per-key
        // set, intersected with each partition) and have no "<ts> < T" bounds fast-path; a plain WHEN
        // predicate uses the CASE keep-filter plus the bounds fast-path.
        final boolean keepLatest = RowExpiryUtil.isKeepLatest(predicate);
        final boolean window = RowExpiryUtil.isKeepBy(predicate) || RowExpiryUtil.isWindow(predicate);

        partitionFloors.clear();
        partitionNextFloors.clear();
        partitionRowCounts.clear();

        final String timestampColumnName;
        // For a window/keep-by survivor query: the quoted base column list, so the synthetic keep column is
        // projected away (built from the reader metadata while it is open).
        String windowColsCsv = null;
        // Fast-path threshold for a "<ts> < T"/"<ts> <= T" predicate: lets each partition be classified by
        // its [floor, nextFloor) bounds with no survivor scan (LONG_NULL = not applicable -> scan instead).
        long timestampThreshold = Numbers.LONG_NULL;

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
            if (window) {
                windowColsCsv = buildQuotedColumnList(metadata);
            }

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
                final long logicalFloor = txReader.getLogicalPartitionTimestamp(reader.getPartitionTimestampByIndex(i));
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
                } else {
                    // another physical split of the current logical partition: accumulate its rows
                    final int last = partitionRowCounts.size() - 1;
                    partitionRowCounts.setQuick(last, partitionRowCounts.getQuick(last) + size);
                }
            }

            // Resolve the fast-path timestamp threshold once per table (now() must be initialised first).
            // Only a scalar "<ts> < T" WHEN predicate has a bounds threshold; KEEP LATEST / window modes
            // always fall to the survivor-count scan.
            if (!keepLatest && !window && partitionFloors.size() > 0) {
                initNow();
                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    timestampThreshold = compiler.expiryTimestampThresholdMicros(
                            sqlExecutionContext, metadata, predicate, timestampColumnName);
                }
            }
        }

        boolean workDone = false;
        // Concurrency model. A REPLACE or count-DROP must never physically delete a row a concurrent writer
        // back-filled into a non-active partition since the survivor scan. On a WAL table the scan reads only
        // APPLIED state, so a committed-but-unapplied back-fill is invisible to a recount; we instead gate on
        // the sequencer transaction: require the table FULLY APPLIED at the start (so the scan is complete)
        // and UNCHANGED-BY-OTHERS through each destructive commit (our own commits are tracked in
        // expectedSeqTxn). A non-WAL table writes synchronously, so the survivor recount is authoritative
        // there. A bounds-based DROP is always safe (every row below T is expired) and is never gated.
        final boolean isWal = tableToken.isWal();
        final SeqTxnTracker txnTracker = isWal ? engine.getTableSequencerAPI().getTxnTracker(tableToken) : null;
        long expectedSeqTxn = isWal ? txnTracker.getSeqTxn() : 0;
        // For WAL, only attempt racy reclamation when the writer has caught up to the sequencer (no pending
        // apply that the survivor scan would miss). Non-WAL is always allowed (synchronous, recount-checked).
        final boolean racyOpsAllowed = !isWal || txnTracker.getWriterTxn() == expectedSeqTxn;

        // Decide and act (reader closed). All reclamation is via REPLACE_RANGE on the WAL writer: a fully
        // expired partition is wiped (empty REPLACE_RANGE = pure delete), a partially expired one is compacted
        // to its survivors. DROP PARTITION via SQL is NOT used — it is rejected for materialized views ("cannot
        // modify materialized view") and every policied object is a WAL mat view. The survivor count() and
        // SELECT * are compiled ONCE per sweep with $1/$2 bind variables for the partition range and rebound
        // per partition; both compile lazily, so a pure bounds-wipe sweep compiles neither.
        final String countSql;
        final String selectSql;
        if (keepLatest) {
            // Survivors = the global latest row per key (LATEST ON over the WHOLE view), intersected with the
            // partition range by the OUTER predicate (LATEST ON cannot share a level with WHERE). The cleanup
            // context disables the read filter, so the reference resolves to the raw view, not re-wrapped.
            final String latestSource = "(SELECT * FROM \"" + tableName + "\" LATEST ON \"" + timestampColumnName
                    + "\" PARTITION BY " + RowExpiryUtil.keepLatestKeys(predicate) + ")";
            final String tail = " WHERE \"" + timestampColumnName + "\" >= $1 AND \"" + timestampColumnName + "\" < $2";
            countSql = "SELECT count() FROM " + latestSource + tail;
            selectSql = "SELECT * FROM " + latestSource + tail;
        } else if (window) {
            // Window/keep-by: survivors are the NOT-expired rows per the window keep-filter, computed over the
            // WHOLE view (inner projection) and intersected with the partition range by the OUTER predicate.
            // Base columns are enumerated so the synthetic keep column is projected away for the REPLACE copier.
            final String windowPred = RowExpiryUtil.windowPredicate(predicate, timestampColumnName);
            final String source = "(SELECT *, CASE WHEN (" + windowPred + ") THEN false ELSE true END "
                    + RowExpiryUtil.KEEP_COLUMN + " FROM \"" + tableName + "\")";
            final String tail = " WHERE " + RowExpiryUtil.KEEP_COLUMN + " AND \"" + timestampColumnName
                    + "\" >= $1 AND \"" + timestampColumnName + "\" < $2";
            countSql = "SELECT count() FROM " + source + tail;
            selectSql = "SELECT " + windowColsCsv + " FROM " + source + tail;
        } else {
            final String keepFilter = RowExpiryUtil.buildRowExpiryKeepFilter(predicate);
            final String tail = " WHERE (" + keepFilter + ") AND \"" + timestampColumnName
                    + "\" >= $1 AND \"" + timestampColumnName + "\" < $2";
            countSql = "SELECT count() FROM \"" + tableName + "\"" + tail;
            selectSql = "SELECT * FROM \"" + tableName + "\"" + tail;
        }
        SqlCompiler cleanupCompiler = null;
        RecordCursorFactory countFactory = null;
        RecordCursorFactory selectFactory = null;
        RecordToRowCopier survivorCopier = null;
        int selectTsIndex = -1;
        WalWriter walWriter = null;
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
                        walWriter.commitWithParams(floorTs, nextFloorTs, WAL_DEDUP_MODE_REPLACE_RANGE);
                        expectedSeqTxn++; // our commit advanced the sequencer by exactly one txn
                        workDone = true;
                        LOG.info().$("reclaimed fully-expired partition [table=").$safe(tableName)
                                .$(", partitionTs=").$ts(floorTs).I$();
                    } else if (racyOpsAllowed && boundsAction == ACTION_UNKNOWN) {
                        if (countFactory == null) {
                            if (cleanupCompiler == null) {
                                cleanupCompiler = engine.getSqlCompiler();
                            }
                            bindPartitionRange(floorTs, nextFloorTs); // declare $1/$2 types before compiling
                            countFactory = cleanupCompiler.compile(countSql, sqlExecutionContext).getRecordCursorFactory();
                        }
                        // ACTION_REPLACE covers both a partially expired partition (compact to survivors) and a
                        // fully expired one (zero survivors -> empty REPLACE_RANGE wipe); both go through the
                        // gated replacePartition below.
                        if (classifyPartition(countFactory, floorTs, nextFloorTs, rowCount) == ACTION_REPLACE) {
                            if (walWriter == null) {
                                walWriter = engine.getWalWriter(tableToken);
                            }
                            if (selectFactory == null) {
                                if (cleanupCompiler == null) {
                                    cleanupCompiler = engine.getSqlCompiler();
                                }
                                bindPartitionRange(floorTs, nextFloorTs);
                                selectFactory = cleanupCompiler.compile(selectSql, sqlExecutionContext).getRecordCursorFactory();
                                selectTsIndex = selectFactory.getMetadata().getColumnIndex(timestampColumnName);
                                columnFilter.of(selectFactory.getMetadata().getColumnCount());
                                survivorCopier = RecordToRowCopierUtils.generateCopier(asm,
                                        selectFactory.getMetadata(), walWriter.getMetadata(), columnFilter, engine.getConfiguration());
                            }
                            if (replacePartition(selectFactory, survivorCopier, selectTsIndex, walWriter,
                                    tableName, floorTs, nextFloorTs, txnTracker, expectedSeqTxn)) {
                                expectedSeqTxn++; // our REPLACE commit advanced the sequencer by exactly one txn
                                workDone = true;
                            }
                        }
                    }
                    // ACTION_SKIP, or a WAL table not caught up -> defer this partition to a later sweep.
                } catch (Throwable th) {
                    // A REPLACE that failed mid-append leaves uncommitted rows in the (reused) writer.
                    // Free it so those rows are rolled back on close and cannot be committed into the
                    // NEXT partition's REPLACE_RANGE (which would resurrect them outside the deleted
                    // range). A fresh writer is acquired on the next REPLACE.
                    walWriter = Misc.free(walWriter);
                    LOG.error().$("row-expiry partition cleanup failed [table=").$safe(tableName)
                            .$(", partitionTs=").$(floorTs)
                            .$(", msg=").$safe(th.getMessage())
                            .I$();
                }
            }
        } finally {
            walWriter = Misc.free(walWriter);
            selectFactory = Misc.free(selectFactory);
            countFactory = Misc.free(countFactory);
            cleanupCompiler = Misc.free(cleanupCompiler);
        }
        return workDone;
    }

    @Override
    public void close() {
        sqlExecutionContext = Misc.free(sqlExecutionContext);
    }

    @Override
    protected boolean runSerially() {
        final long nowMicros = clock.getTicks();
        // Global throttle: the worker pool ticks this job continuously, but discovery only needs an
        // occasional sweep. Per-object cadence is then enforced by each policy's CLEANUP EVERY interval.
        if (lastGlobalCheckMicros != NO_LAST_RUN && nowMicros - lastGlobalCheckMicros < GLOBAL_CHECK_INTERVAL_MICROS) {
            return false;
        }
        lastGlobalCheckMicros = nowMicros;

        // Skip the whole-database scan when no table carries an EXPIRE ROWS policy (and the metadata
        // cache has finished hydrating, so that signal is trustworthy).
        if (!engine.getMetadataCache().mayHaveExpiryPolicy()) {
            if (lastRunByTable.size() > 0) {
                lastRunByTable.clear();
            }
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
        try (MetadataCacheReader metadataRO = engine.getMetadataCache().readLock()) {
            metadataRO.collectPoliciedTables(discoveredTokens, discoveredPredicates, discoveredCleanupIntervals);
        }

        if (discoveredTokens.size() == 0) {
            if (lastRunByTable.size() > 0) {
                lastRunByTable.clear();
            }
            return false;
        }

        boolean workDone = false;
        for (int i = 0, n = discoveredTokens.size(); i < n; i++) {
            final TableToken tableToken = discoveredTokens.getQuick(i);
            final String predicate = discoveredPredicates.getQuick(i);
            final long cleanupIntervalMicros = discoveredCleanupIntervals.getQuick(i);
            final CharSequence tableKey = tableToken.getTableName();

            final long lastRun = lastRunByTable.get(tableKey);
            if (lastRun != NO_LAST_RUN && nowMicros - lastRun < cleanupIntervalMicros) {
                continue;
            }

            // Defensive primary-only/safety guard: the object may have been dropped/renamed since the
            // snapshot, or the policy removed. Skip if the token no longer resolves; wrap per-object work
            // so one bad table cannot kill the whole sweep.
            if (engine.getTableTokenIfExists(tableToken.getTableName()) == null) {
                continue;
            }
            try {
                if (cleanupTable(tableToken, predicate, cleanupIntervalMicros)) {
                    workDone = true;
                }
            } catch (Throwable th) {
                LOG.error().$("row-expiry cleanup failed [table=").$safe(tableKey)
                        .$(", msg=").$safe(th.getMessage())
                        .I$();
            }
            lastRunByTable.put(Chars.toString(tableKey), nowMicros);
        }
        return workDone;
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

    private int classifyPartition(RecordCursorFactory countFactory, long floorTs, long nextFloorTs, long rowCount) throws SqlException {
        // Classify with a read-only count() scan, then copy only REPLACE partitions. This is deliberately
        // NOT folded into the copy: for an arbitrary predicate the class of a partition is unknown until it
        // is scanned, and the common case (a fully-live recent partition -> SKIP) is decided here with a cheap
        // read-only scan and no WAL writes. Folding the count into the copy would instead append every survivor
        // to the WAL writer and then roll back the SKIP partitions — turning the common fully-live partition
        // from a read-only scan into a scan plus discarded WAL write I/O. Only the (few) expired partitions
        // pay for a second scan.
        final long survivors = countSurvivors(countFactory, floorTs, nextFloorTs);
        // survivors == 0 -> fully expired (REPLACE wipes it); 0 < survivors < rowCount -> partially expired
        // (REPLACE compacts to survivors); survivors == rowCount -> nothing expired (SKIP). rowCount is the
        // reader snapshot, so only act when something is clearly expired.
        return survivors < rowCount ? ACTION_REPLACE : ACTION_SKIP;
    }

    // Binds $1 = partition floor (inclusive), $2 = next floor (exclusive). The interval optimiser prunes to
    // exactly this logical partition (an interval forward scan), so the count/select factories are compiled
    // ONCE per sweep and merely rebound per partition rather than re-parsed + re-codegen'd from a literal.
    private void bindPartitionRange(long floorTs, long nextFloorTs) throws SqlException {
        final BindVariableService bind = sqlExecutionContext.getBindVariableService();
        bind.setTimestamp(0, floorTs);
        bind.setTimestamp(1, nextFloorTs);
    }

    private long countSurvivors(RecordCursorFactory countFactory, long floorTs, long nextFloorTs) throws SqlException {
        bindPartitionRange(floorTs, nextFloorTs);
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

    /** Quoted, comma-separated base column list (for the window survivor query's outer projection). */
    private static String buildQuotedColumnList(TableReaderMetadata metadata) {
        final StringSink sink = new StringSink();
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            if (i > 0) {
                sink.putAscii(',');
            }
            sink.putAscii('"').put(metadata.getColumnName(i)).putAscii('"');
        }
        return sink.toString();
    }

    private boolean replacePartition(
            RecordCursorFactory selectFactory,
            RecordToRowCopier copier,
            int cursorTimestampIndex,
            WalWriter walWriter,
            String tableName,
            long floorTs,
            long nextFloorTs,
            SeqTxnTracker txnTracker,
            long expectedSeqTxn
    ) throws SqlException {
        // selectFactory + copier + cursorTimestampIndex are compiled/built ONCE per sweep and rebound here
        // per partition (the SQL uses $1/$2 bind variables for the partition range). The copier is safe to
        // reuse across partitions because the concurrency gate below defers on ANY concurrent transaction,
        // so a structural ALTER cannot change the column layout mid-sweep without the REPLACE deferring.
        bindPartitionRange(floorTs, nextFloorTs);
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
        // Concurrency gate: commit the REPLACE only if NO external transaction was sequenced since the
        // survivor scan's baseline (our own prior commits are accounted for in expectedSeqTxn). Unlike a
        // reader-based recount, the sequencer txn reflects committed-but-not-yet-applied writes too, so a
        // concurrent back-fill into this partition cannot slip through unseen. Residual: a write sequenced
        // between this check and the commit just below — a sub-microsecond window with no I/O in between.
        // An empty survivor set is a legitimate fully-expired partition: the gate proves nothing was
        // back-filled into the range, so it is committed as a pure-delete REPLACE_RANGE (DROP PARTITION via
        // SQL is rejected for materialized views).
        if (txnTracker.getSeqTxn() != expectedSeqTxn) {
            walWriter.rollback();
            LOG.info().$("deferred expired-rows compaction; table changed concurrently [table=").$safe(tableName)
                    .$(", partitionTs=").$ts(floorTs).I$();
            return false;
        }
        // Atomically replace [floorTs, nextFloorTs) with exactly the surviving rows appended above (none ->
        // the whole range is deleted).
        walWriter.commitWithParams(floorTs, nextFloorTs, WAL_DEDUP_MODE_REPLACE_RANGE);
        LOG.info().$(appended == 0 ? "reclaimed fully-expired partition [table=" : "compacted expired-rows partition [table=")
                .$safe(tableName).$(", partitionTs=").$ts(floorTs).I$();
        return true;
    }
}
