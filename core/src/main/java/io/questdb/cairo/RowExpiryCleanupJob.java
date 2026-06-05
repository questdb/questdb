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
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.RecordToRowCopier;
import io.questdb.griffin.RecordToRowCopierUtils;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
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
import io.questdb.std.ObjHashSet;
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
 * <b>Best-effort window (concurrency):</b> survivors are read just before the REPLACE_RANGE commit, on a
 * separate handle from any concurrent writer. For a designated-timestamp predicate this is safe — a row
 * back-filled into the replaced (past) range is itself expired, so removing it is correct. For a CUSTOM
 * predicate, a NON-expired row back-filled into a logical partition between the survivor read and the
 * commit CAN be removed (the read filter does not hide such a row). This is the accepted best-effort
 * window: the job only reclaims space, and the hiding of genuinely-expired rows (the correctness
 * guarantee) is unaffected.
 */
public class RowExpiryCleanupJob extends SynchronizedJob implements Closeable {
    private static final int ACTION_DROP = 1;
    private static final int ACTION_REPLACE = 2;
    private static final int ACTION_SKIP = 0;
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
    private final LongList dropFloors = new LongList();
    private final CairoEngine engine;
    private final CharSequenceLongHashMap lastRunByTable = new CharSequenceLongHashMap(4, 0.5, NO_LAST_RUN);
    // Per-cleanup snapshot of one object's non-active LOGICAL partitions.
    private final LongList partitionFloors = new LongList();
    private final LongList partitionNextFloors = new LongList();
    private final LongList partitionRowCounts = new LongList();
    private final StringSink partitionSink = new StringSink();
    private final StringSink sqlSink = new StringSink();
    private final ObjHashSet<TableToken> tableTokenBucket = new ObjHashSet<>();
    private long lastGlobalCheckMicros = NO_LAST_RUN;
    private SqlExecutionContext sqlExecutionContext;

    public RowExpiryCleanupJob(CairoEngine engine) {
        this.engine = engine;
        final CairoConfiguration configuration = engine.getConfiguration();
        this.clock = configuration.getMicrosecondClock();
        this.sqlExecutionContext = new SqlExecutionContextImpl(engine, 1);
        ((SqlExecutionContextImpl) this.sqlExecutionContext).with(
                configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                null,
                null
        );
    }

    /**
     * Physically reclaims expired rows from a single policied object, in place (no copy, no populate).
     * Snapshots non-active LOGICAL partition totals from a reader, classifies each as DROP/REPLACE/SKIP
     * via the keep-filter, then compacts via REPLACE_RANGE and batch-drops fully expired partitions.
     */
    public boolean cleanupTable(TableToken tableToken, String predicate, long cleanupIntervalMicros) {
        final String tableName = tableToken.getTableName();
        final String keepFilter = RowExpiryUtil.buildRowExpiryKeepFilter(predicate);

        partitionFloors.clear();
        partitionNextFloors.clear();
        partitionRowCounts.clear();

        final int partitionBy;
        final int timestampType;
        final String timestampColumnName;

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
        }

        boolean workDone = false;
        // Decide and act (reader closed). REPLACE_RANGE first (needs a WAL writer), then batch-drop.
        dropFloors.clear();
        WalWriter walWriter = null;
        RecordToRowCopier copier = null;
        try {
            for (int i = 0, n = partitionFloors.size(); i < n; i++) {
                final long floorTs = partitionFloors.getQuick(i);
                final long nextFloorTs = partitionNextFloors.getQuick(i);
                final long rowCount = partitionRowCounts.getQuick(i);
                try {
                    final int action = classifyPartition(tableName, keepFilter, timestampColumnName,
                            timestampType, partitionBy, floorTs, rowCount);
                    if (action == ACTION_DROP) {
                        dropFloors.add(floorTs);
                        workDone = true;
                    } else if (action == ACTION_REPLACE) {
                        if (walWriter == null) {
                            walWriter = engine.getWalWriter(tableToken);
                        }
                        copier = replacePartition(tableName, keepFilter, timestampColumnName,
                                timestampType, partitionBy, floorTs, nextFloorTs, walWriter, copier);
                        workDone = true;
                    }
                } catch (Throwable th) {
                    LOG.error().$("row-expiry partition cleanup failed [table=").$(tableName)
                            .$(", partitionTs=").$(floorTs)
                            .$(", msg=").$(th.getMessage())
                            .I$();
                }
            }
        } finally {
            walWriter = Misc.free(walWriter);
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
        tableTokenBucket.clear();
        engine.getTableTokens(tableTokenBucket, false);
        final ObjList<TableToken> tokens = tableTokenBucket.getList();
        try (MetadataCacheReader metadataRO = engine.getMetadataCache().readLock()) {
            for (int i = 0, n = tokens.size(); i < n; i++) {
                final TableToken token = tokens.getQuick(i);
                if (token == null || token.isView()) {
                    // plain (non-materialized) views have no _meta and cannot carry an EXPIRE ROWS policy
                    continue;
                }
                final CairoTable table = metadataRO.getTable(token);
                if (table == null) {
                    continue;
                }
                final String predicate = table.getExpiryPredicate();
                if (predicate == null || predicate.isEmpty()) {
                    continue;
                }
                discoveredTokens.add(token);
                discoveredPredicates.add(predicate);
                discoveredCleanupIntervals.add(table.getExpiryCleanupIntervalMicros());
            }
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
                LOG.error().$("row-expiry cleanup failed [table=").$(tableKey)
                        .$(", msg=").$(th.getMessage())
                        .I$();
            }
            lastRunByTable.put(Chars.toString(tableKey), nowMicros);
        }
        return workDone;
    }

    private int classifyPartition(
            String tableName,
            String keepFilter,
            String timestampColumnName,
            int timestampType,
            int partitionBy,
            long floorTs,
            long rowCount
    ) throws SqlException {
        final long survivors = countSurvivors(tableName, keepFilter, timestampColumnName, timestampType, partitionBy, floorTs);
        if (survivors == 0) {
            return ACTION_DROP;
        }
        // rowCount is from the reader snapshot; only act when something is clearly expired.
        return survivors < rowCount ? ACTION_REPLACE : ACTION_SKIP;
    }

    private long countSurvivors(
            String tableName,
            String keepFilter,
            String timestampColumnName,
            int timestampType,
            int partitionBy,
            long floorTs
    ) throws SqlException {
        partitionSink.clear();
        PartitionBy.setSinkForPartition(partitionSink, timestampType, partitionBy, floorTs);
        sqlSink.clear();
        sqlSink.putAscii("SELECT count() FROM \"").put(tableName).putAscii("\" WHERE (").put(keepFilter)
                .putAscii(") AND \"").put(timestampColumnName).putAscii("\" IN '").put(partitionSink).putAscii("'");
        initNow();
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            try (RecordCursorFactory factory = compiler.compile(sqlSink, sqlExecutionContext).getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    if (cursor.hasNext()) {
                        return cursor.getRecord().getLong(0);
                    }
                }
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
            LOG.info().$("dropped ").$(floors.size()).$(" fully-expired partitions [table=").$(tableName).I$();
            return true;
        } catch (Throwable th) {
            LOG.error().$("failed to drop expired partitions [table=").$(tableName)
                    .$(", msg=").$(th.getMessage()).I$();
            return false;
        }
    }

    private void executeSql(CharSequence sql) throws SqlException {
        initNow();
        engine.execute(sql, sqlExecutionContext, null);
    }

    private void initNow() {
        ((SqlExecutionContextImpl) sqlExecutionContext).initNow();
    }

    private RecordToRowCopier replacePartition(
            String tableName,
            String keepFilter,
            String timestampColumnName,
            int timestampType,
            int partitionBy,
            long floorTs,
            long nextFloorTs,
            WalWriter walWriter,
            RecordToRowCopier copier
    ) throws SqlException {
        partitionSink.clear();
        PartitionBy.setSinkForPartition(partitionSink, timestampType, partitionBy, floorTs);
        sqlSink.clear();
        sqlSink.putAscii("SELECT * FROM \"").put(tableName).putAscii("\" WHERE (").put(keepFilter)
                .putAscii(") AND \"").put(timestampColumnName).putAscii("\" IN '").put(partitionSink).putAscii("'");
        initNow();
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            try (RecordCursorFactory factory = compiler.compile(sqlSink, sqlExecutionContext).getRecordCursorFactory()) {
                if (copier == null) {
                    columnFilter.of(factory.getMetadata().getColumnCount());
                    copier = RecordToRowCopierUtils.generateCopier(
                            asm,
                            factory.getMetadata(),
                            walWriter.getMetadata(),
                            columnFilter,
                            engine.getConfiguration()
                    );
                }
                final int cursorTimestampIndex = factory.getMetadata().getColumnIndex(timestampColumnName);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final Record record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        final long timestamp = record.getTimestamp(cursorTimestampIndex);
                        final TableWriter.Row row = walWriter.newRow(timestamp);
                        copier.copy(sqlExecutionContext, record, row);
                        row.append();
                    }
                }
            }
        }
        // Atomically replace [floorTs, nextFloorTs) with exactly the surviving rows appended above.
        walWriter.commitWithParams(floorTs, nextFloorTs, WAL_DEDUP_MODE_REPLACE_RANGE);
        LOG.info().$("compacted expired-rows partition [table=").$(tableName)
                .$(", partition=").$(partitionSink).I$();
        return copier;
    }
}
