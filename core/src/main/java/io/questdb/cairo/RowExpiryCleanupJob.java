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
 * Primary-only background job that reclaims storage for tables and materialized views carrying an
 * {@code EXPIRE ROWS} policy. The read-time filter (see {@code SqlParser}) already hides expired rows
 * from every query, so this job is <b>best-effort</b>: correctness never depends on it, it only frees
 * disk space. For each policied object it processes non-active <b>logical</b> partitions and either:
 * <ul>
 *     <li>drops partitions whose rows are all expired ({@code ALTER TABLE ... DROP PARTITION LIST}), or</li>
 *     <li>compacts partially expired partitions by atomically replacing the partition's timestamp range
 *         with the surviving (non-expired) rows via {@link WalWriter#commitWithParams} using
 *         {@code WAL_DEDUP_MODE_REPLACE_RANGE}.</li>
 * </ul>
 * The REPLACE_RANGE commit writes a new partition version and switches atomically on commit, so a crash
 * mid-cleanup cannot lose surviving rows (the original partition stays intact until the single commit).
 * <p>
 * <b>Logical (not physical) partitions:</b> O3 can split one logical day into several physical
 * partitions. {@code DROP PARTITION} and the {@code ts IN '<day>'} interval both operate on the whole
 * logical day, so this job collapses physical splits into their logical partition and acts per logical
 * day. Acting per physical split would let a DROP of one split wipe a sibling split's surviving rows,
 * and a split's directory name is not even a valid interval literal. Survivor totals come from the tx
 * file ({@code getPartitionSize}, no column mapping), so parquet partitions are handled too without
 * reading raw column memory.
 * <p>
 * <b>Active-partition protection (mirrors TTL):</b> the newest (active) logical partition is never
 * dropped or replaced — including any earlier physical split that shares its logical day. Expired rows
 * there stay hidden by the read filter and are reclaimed once that day ages out of the active slot.
 * <p>
 * <b>Read-filter interaction:</b> the survivor/count SQL references the policied table by name, so the
 * read filter ALSO wraps it. This is idempotent with the explicit keep-filter
 * ({@code NOT(pred) AND NOT(pred)} == {@code NOT(pred)}), so results stay correct.
 * <p>
 * <b>Concurrency (a REPLACE / count-DROP never deletes a row a concurrent writer back-filled):</b>
 * survivors are computed on a separate handle from any concurrent writer, so a non-expired row could be
 * back-filled into a non-active partition between the survivor scan and the destructive commit. On a WAL
 * table a recount cannot detect this — the reader sees only APPLIED state, while the back-fill may be
 * committed-but-not-yet-applied (a lower sequencer txn than the cleanup's REPLACE, hence overwritten on
 * apply). The job therefore gates on the SEQUENCER TRANSACTION: it proceeds with REPLACE / count-DROP only
 * when the table is fully applied at the start (so the survivor scan is complete) and the sequencer txn has
 * not advanced beyond the cleanup's own commits by the moment of each destructive commit; otherwise it
 * DEFERS to the next sweep. The sequencer txn reflects committed-but-unapplied writes, so this closes the
 * window down to a sub-microsecond residual between the final check and the commit (no I/O in between).
 * The trade-off is that on a concurrently-written table REPLACE / count-DROP defer until a quiescent sweep.
 * A non-WAL table writes synchronously, so a survivor recount is authoritative and is used instead.
 * <p>
 * A <i>bounds-based</i> DROP of a logical partition lying wholly below a designated-timestamp threshold is
 * unconditionally safe and never gated: every row there, including any concurrent back-fill, necessarily
 * satisfies {@code ts < T} and so is expired — this is the primary reclamation for time-based expiry under
 * load. Either way the read filter stays authoritative for VISIBILITY, so an expired row is never shown
 * even when physical reclamation is deferred.
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
    private final LongList countDropFloors = new LongList();
    private final LongList countDropNextFloors = new LongList();
    // Per-tick snapshot of policied objects, collected under the metadata-cache read lock and then
    // processed AFTER the lock is released (never hold the cache lock during cleanup).
    private final LongList discoveredCleanupIntervals = new LongList();
    private final ObjList<String> discoveredPredicates = new ObjList<>();
    private final ObjList<TableToken> discoveredTokens = new ObjList<>();
    private final LongList dropFloors = new LongList();
    private final CairoEngine engine;
    private final CharSequenceLongHashMap lastRunByTable = new CharSequenceLongHashMap(4, 0.5, NO_LAST_RUN);
    // Per-cleanup snapshot of one object's non-active LOGICAL partitions.
    private final LongList partitionFloors = new LongList();
    private final LongList partitionNextFloors = new LongList();
    private final LongList partitionRowCounts = new LongList();
    private final StringSink sqlSink = new StringSink();
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
        // KEEP LATEST (relative) policies are read-filter-authoritative for now; physical cleanup of the
        // latest-per-key set is deferred (a follow-up). Skip so the scalar keep-filter below is never built
        // from the encoded policy string.
        if (RowExpiryUtil.isKeepLatest(predicate)) {
            return false;
        }
        final String tableName = tableToken.getTableName();
        final String keepFilter = RowExpiryUtil.buildRowExpiryKeepFilter(predicate);

        partitionFloors.clear();
        partitionNextFloors.clear();
        partitionRowCounts.clear();

        final int partitionBy;
        final int timestampType;
        final String timestampColumnName;
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
            partitionBy = reader.getPartitionedBy();
            timestampType = metadata.getTimestampType();
            timestampColumnName = metadata.getColumnName(timestampIndex);

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
            if (partitionFloors.size() > 0) {
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

        // Decide and act (reader closed). REPLACE_RANGE first (needs a WAL writer), then batch-drop. The
        // survivor count() and the SELECT * are compiled ONCE per sweep with $1/$2 bind variables for the
        // partition range [floor, nextFloor) (an interval scan) and rebound per partition — no per-partition
        // re-parse/re-codegen. Both compile lazily, so a pure bounds-DROP sweep compiles nothing.
        dropFloors.clear();
        countDropFloors.clear();
        countDropNextFloors.clear();
        final String countSql = "SELECT count() FROM \"" + tableName + "\" WHERE (" + keepFilter
                + ") AND \"" + timestampColumnName + "\" >= $1 AND \"" + timestampColumnName + "\" < $2";
        final String selectSql = "SELECT * FROM \"" + tableName + "\" WHERE (" + keepFilter
                + ") AND \"" + timestampColumnName + "\" >= $1 AND \"" + timestampColumnName + "\" < $2";
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
                    if (boundsAction == ACTION_DROP) {
                        // Bounds DROP: the whole logical partition is below the designated-ts threshold, so
                        // every row — including any concurrent back-fill — is expired. Always safe; not gated.
                        dropFloors.add(floorTs);
                        workDone = true;
                    } else if (boundsAction == ACTION_UNKNOWN && racyOpsAllowed) {
                        if (countFactory == null) {
                            if (cleanupCompiler == null) {
                                cleanupCompiler = engine.getSqlCompiler();
                            }
                            bindPartitionRange(floorTs, nextFloorTs); // declare $1/$2 types before compiling
                            countFactory = cleanupCompiler.compile(countSql, sqlExecutionContext).getRecordCursorFactory();
                        }
                        final int action = classifyPartition(countFactory, floorTs, nextFloorTs, rowCount);
                        if (action == ACTION_DROP) {
                            // Count-based DROP (custom predicate): defer to the gated recheck below so a
                            // partition that received a non-expired row since the scan is not dropped.
                            countDropFloors.add(floorTs);
                            countDropNextFloors.add(nextFloorTs);
                        } else if (action == ACTION_REPLACE && isWal) {
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
                        // non-WAL REPLACE is unsupported (no WAL writer); that partition waits for a bounds-DROP.
                    }
                    // boundsAction == ACTION_SKIP, or a WAL table not caught up -> defer this partition.
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

            // Done with REPLACEs: release the WAL writer before the (read-only) count-DROP recheck.
            walWriter = Misc.free(walWriter);

            // Count-DROP: drop a fully-expired partition only if a concurrent write cannot have added a
            // non-expired row since classification. WAL: no EXTERNAL txn since the baseline (our own commits
            // are accounted for in expectedSeqTxn). Non-WAL: a synchronous recount of survivors == 0 (the
            // count factory was compiled above when this floor was classified).
            for (int i = 0, n = countDropFloors.size(); i < n; i++) {
                final long floorTs = countDropFloors.getQuick(i);
                try {
                    final boolean safe = isWal
                            ? txnTracker.getSeqTxn() == expectedSeqTxn
                            : countSurvivors(countFactory, floorTs, countDropNextFloors.getQuick(i)) == 0;
                    if (safe) {
                        dropFloors.add(floorTs);
                        workDone = true;
                    } else {
                        LOG.info().$("deferred expired-rows partition drop; table changed concurrently [table=")
                                .$safe(tableName).$(", partitionTs=").$ts(floorTs).I$();
                    }
                } catch (Throwable th) {
                    LOG.error().$("row-expiry partition drop recheck failed [table=").$safe(tableName)
                            .$(", partitionTs=").$ts(floorTs).$(", msg=").$safe(th.getMessage()).I$();
                }
            }
        } finally {
            walWriter = Misc.free(walWriter);
            selectFactory = Misc.free(selectFactory);
            countFactory = Misc.free(countFactory);
            cleanupCompiler = Misc.free(cleanupCompiler);
        }

        if (dropFloors.size() > 0) {
            workDone |= dropPartitions(tableName, timestampType, partitionBy, dropFloors);
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
        // is scanned, and the common cases (a fully-live recent partition -> SKIP, a fully-expired old one
        // -> DROP) are decided here with a cheap read-only scan and no WAL writes. Folding the count into
        // the copy would instead append every survivor to the WAL writer and then roll back the SKIP
        // partitions — turning the common fully-live partition from a read-only scan into a scan plus
        // discarded WAL write I/O. Only the (few) partially-expired partitions pay for a second scan.
        final long survivors = countSurvivors(countFactory, floorTs, nextFloorTs);
        if (survivors == 0) {
            return ACTION_DROP;
        }
        // rowCount is from the reader snapshot; only act when something is clearly expired.
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

    private boolean dropPartitions(
            String tableName,
            int timestampType,
            int partitionBy,
            LongList floors
    ) {
        sqlSink.clear();
        sqlSink.putAscii("ALTER TABLE \"").put(tableName).putAscii("\" DROP PARTITION LIST ");
        for (int i = 0, n = floors.size(); i < n; i++) {
            if (i > 0) {
                sqlSink.putAscii(", ");
            }
            sqlSink.putAscii("'");
            PartitionBy.setSinkForPartition(sqlSink, timestampType, partitionBy, floors.getQuick(i));
            sqlSink.putAscii("'");
        }
        try {
            executeSql(sqlSink);
            LOG.info().$("dropped ").$(floors.size()).$(" fully-expired partitions [table=").$safe(tableName).I$();
            return true;
        } catch (Throwable th) {
            LOG.error().$("failed to drop expired partitions [table=").$safe(tableName)
                    .$(", msg=").$safe(th.getMessage()).I$();
            return false;
        }
    }

    private void executeSql(CharSequence sql) throws SqlException {
        initNow();
        engine.execute(sql, sqlExecutionContext, null);
    }

    private void initNow() {
        sqlExecutionContext.initNow();
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
        // Also defends against an empty survivor set (would otherwise REPLACE the range away entirely).
        if (appended == 0 || txnTracker.getSeqTxn() != expectedSeqTxn) {
            walWriter.rollback();
            LOG.info().$("deferred expired-rows compaction; table changed concurrently [table=").$safe(tableName)
                    .$(", partitionTs=").$ts(floorTs).I$();
            return false;
        }
        // Atomically replace [floorTs, nextFloorTs) with exactly the surviving rows appended above.
        walWriter.commitWithParams(floorTs, nextFloorTs, WAL_DEDUP_MODE_REPLACE_RANGE);
        LOG.info().$("compacted expired-rows partition [table=").$safe(tableName)
                .$(", partitionTs=").$ts(floorTs).I$();
        return true;
    }
}
