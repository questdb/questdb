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
import io.questdb.cairo.vm.api.MemoryCR;
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
 * disk space. For each policied object it scans non-active partitions and either:
 * <ul>
 *     <li>drops partitions whose rows are all expired ({@code ALTER TABLE ... DROP PARTITION LIST}), or</li>
 *     <li>compacts partially expired partitions by atomically replacing the partition's timestamp range
 *         with the surviving (non-expired) rows via {@link WalWriter#commitWithParams} using
 *         {@code WAL_DEDUP_MODE_REPLACE_RANGE}.</li>
 * </ul>
 * The REPLACE_RANGE commit writes a new partition version and switches atomically on commit, so a crash
 * mid-cleanup cannot lose surviving rows (the original partition stays intact until the single commit).
 * <p>
 * <b>Active-partition protection (mirrors TTL):</b> the newest (active) partition is never dropped or
 * replaced. TTL guards with a {@code getPartitionCount() < 2} style check; we replicate that intent by
 * processing only partition indices {@code [0, partitionCount - 1)}. Expired rows in the active
 * partition stay hidden by the read filter and are reclaimed once that partition ages out of the active
 * slot. Skipping it avoids fighting concurrent writers and avoids REPLACE_RANGE on the hot partition.
 * <p>
 * <b>Read-filter interaction:</b> the survivor/count SQL references the policied table by name, so the
 * read filter ALSO wraps it. This is idempotent with the explicit keep-filter
 * ({@code NOT(pred) AND NOT(pred)} = {@code NOT(pred)}), so results stay correct; we rely on that
 * idempotency rather than disabling the read filter.
 * <p>
 * Note on concurrency: cleanup decisions are taken from a reader snapshot, so a row written into an
 * already-expired range between the scan and the commit may be removed. For the common designated-timestamp
 * case this is correct by definition (such a row is itself expired); CUSTOM predicates accept this small
 * window (and the read filter would have hidden such a row anyway).
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
    private final LongList partitionFloors = new LongList();
    private final LongList partitionMaxTs = new LongList();
    private final LongList partitionMinTs = new LongList();
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

    /**
     * Physically reclaims expired rows from a single policied object, in place (no copy, no populate).
     * Snapshots non-active partition stats from a reader, classifies each as DROP/REPLACE/SKIP, then
     * compacts via REPLACE_RANGE and batch-drops fully expired partitions.
     */
    public boolean cleanupTable(TableToken tableToken, String predicate, long cleanupIntervalMicros) {
        final String tableName = tableToken.getTableName();
        final String keepFilter = RowExpiryUtil.buildRowExpiryKeepFilter(predicate);

        partitionFloors.clear();
        partitionNextFloors.clear();
        partitionMinTs.clear();
        partitionMaxTs.clear();
        partitionRowCounts.clear();

        final int partitionBy;
        final int timestampType;
        final long nowInTableUnits;
        final String timestampColumnName;
        final boolean timestampCompare;

        // Snapshot the object's per-partition stats (cheap: per-partition min/max from the sorted
        // designated timestamp). Only NON-ACTIVE partitions are considered: the newest partition is the
        // active one and is left untouched (mirrors TTL's getPartitionCount() < 2 guard).
        try (TableReader reader = engine.getReader(tableToken)) {
            final TableReaderMetadata metadata = reader.getMetadata();
            final int timestampIndex = metadata.getTimestampIndex();
            if (timestampIndex < 0) {
                return false; // non-timestamp table; nothing to expire (should not happen for a WAL table)
            }
            partitionBy = reader.getPartitionedBy();
            timestampType = metadata.getTimestampType();
            timestampColumnName = metadata.getColumnName(timestampIndex);
            nowInTableUnits = ColumnType.getTimestampDriver(timestampType).fromMicros(clock.getTicks());
            timestampCompare = isTimestampCompare(predicate, timestampColumnName);

            final int partitionCount = reader.getPartitionCount();
            // Active-partition protection: never touch the newest partition. With < 2 partitions there is
            // nothing but the active partition, so there is nothing to reclaim here.
            if (partitionCount < 2) {
                return false;
            }
            final int lastNonActive = partitionCount - 1; // exclusive upper bound => skips active partition
            for (int i = 0; i < lastNonActive; i++) {
                final long rowCount = reader.openPartition(i);
                if (rowCount <= 0) {
                    continue;
                }
                final int columnBase = reader.getColumnBase(i);
                final MemoryCR tsColumn = reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, timestampIndex));
                // Designated timestamp is stored sorted ascending, so first/last rows are the partition min/max.
                partitionMinTs.add(tsColumn.getLong(0));
                partitionMaxTs.add(tsColumn.getLong((rowCount - 1) * 8L));
                final long floorTs = reader.getPartitionTimestampByIndex(i);
                partitionFloors.add(floorTs);
                partitionNextFloors.add(reader.getTxFile().getNextLogicalPartitionTimestamp(floorTs));
                partitionRowCounts.add(rowCount);
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
                final long minTs = partitionMinTs.getQuick(i);
                final long maxTs = partitionMaxTs.getQuick(i);
                final long rowCount = partitionRowCounts.getQuick(i);
                try {
                    final int action = classifyPartition(timestampCompare, tableName, keepFilter,
                            timestampColumnName, timestampType, partitionBy, floorTs, minTs, maxTs, rowCount, nowInTableUnits);
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

    private int classifyPartition(
            boolean timestampCompare,
            String tableName,
            String keepFilter,
            String timestampColumnName,
            int timestampType,
            int partitionBy,
            long floorTs,
            long minTs,
            long maxTs,
            long rowCount,
            long nowInTableUnits
    ) throws SqlException {
        if (timestampCompare) {
            // Fast path: classification straight from the partition's designated-timestamp range, no SQL.
            if (maxTs < nowInTableUnits) {
                return ACTION_DROP;
            }
            return minTs < nowInTableUnits ? ACTION_REPLACE : ACTION_SKIP;
        }
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

    /**
     * Detects whether {@code predicate} is an "older than now" comparison against the designated
     * timestamp column — one of {@code <ts> < now()}, {@code <ts> <= now()}, {@code now() > <ts>},
     * {@code now() >= <ts>} — AND the referenced column is exactly the designated timestamp. Only then
     * is the partition min/max fast path valid (it relies on the sorted designated timestamp). Any other
     * predicate falls back to the correct, slower CUSTOM path via {@link #countSurvivors}.
     * <p>
     * Mirrors {@code ExpiringViewDefinition.detectTimestampCompareColumn} from the expiring-view draft.
     */
    private static boolean isTimestampCompare(String predicate, String timestampColumnName) {
        final String column = parseOlderThanNowColumn(predicate);
        return column != null && Chars.equalsIgnoreCase(column, timestampColumnName);
    }

    private static String parseOlderThanNowColumn(String predicateSql) {
        final String p = predicateSql.trim();
        final String lower = p.toLowerCase();
        String columnName = null;
        if (lower.endsWith("< now()") || lower.endsWith("<now()")
                || lower.endsWith("<= now()") || lower.endsWith("<=now()")) {
            columnName = p.substring(0, p.lastIndexOf('<')).trim();
        } else if (lower.startsWith("now()")) {
            final String rest = p.substring(5).trim();
            if (rest.startsWith(">=")) {
                columnName = rest.substring(2).trim();
            } else if (rest.startsWith(">")) {
                columnName = rest.substring(1).trim();
            }
        }
        if (columnName == null || columnName.isEmpty() || columnName.indexOf(' ') >= 0) {
            return null;
        }
        return columnName;
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
