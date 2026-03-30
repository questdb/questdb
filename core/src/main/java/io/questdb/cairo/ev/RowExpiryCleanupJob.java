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

package io.questdb.cairo.ev;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.CharSequenceLongHashMap;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.ObjObjHashMap;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.str.StringSink;

import java.io.Closeable;

public class RowExpiryCleanupJob extends SynchronizedJob implements Closeable {
    private static final Log LOG = LogFactory.getLog(RowExpiryCleanupJob.class);
    private static final long NO_LAST_RUN = Long.MIN_VALUE;
    private static final String STAGING_TABLE_PREFIX = "ev_staging_";
    private final MicrosecondClock clock;
    private final CairoEngine engine;
    private final ObjObjHashMap<CharSequence, PartitionExpiryCache> expiryCaches = new ObjObjHashMap<>();
    // Store partition timestamps (not indices) so they remain valid after reader is closed
    private final LongList fullyExpiredPartitionTimestamps = new LongList();
    private final CharSequenceLongHashMap lastRunByView = new CharSequenceLongHashMap(4, 0.5, NO_LAST_RUN);
    private final LongList partiallyExpiredPartitionTimestamps = new LongList();
    private final ObjList<ExpiringViewDefinition> siblingViews = new ObjList<>();
    private final StringSink sqlSink = new StringSink();
    private SqlExecutionContext sqlExecutionContext;

    public RowExpiryCleanupJob(CairoEngine engine) {
        this.engine = engine;
        this.clock = engine.getConfiguration().getMicrosecondClock();
        final CairoConfiguration configuration = engine.getConfiguration();
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
        final ConcurrentHashMap<ExpiringViewDefinition> registry = engine.getExpiringViewRegistry();
        if (registry.isEmpty()) {
            return false;
        }

        final long now = clock.getTicks();
        boolean workDone = false;

        for (CharSequence viewName : registry.keySet()) {
            final ExpiringViewDefinition definition = registry.get(viewName);
            if (definition == null) {
                continue;
            }

            final long lastRun = lastRunByView.get(viewName);
            if (lastRun != NO_LAST_RUN && now - lastRun < definition.getCleanupIntervalMicros()) {
                continue;
            }

            try {
                boolean viewWorkDone;
                if (definition.isTimestampCompare()) {
                    viewWorkDone = runTimestampCompareCleanup(definition, now);
                } else {
                    viewWorkDone = runCustomCleanup(definition, now);
                }
                if (viewWorkDone) {
                    workDone = true;
                }
                lastRunByView.put(viewName, now);
            } catch (Throwable th) {
                LOG.error().$("cleanup failed [view=").$(viewName)
                        .$(", msg=").$(th.getMessage())
                        .I$();
            }
        }
        return workDone;
    }

    private void classifyTimestampComparePartitions(
            ExpiringViewDefinition definition,
            TableToken baseTableToken,
            long now
    ) {
        fullyExpiredPartitionTimestamps.clear();
        partiallyExpiredPartitionTimestamps.clear();

        final int expiryColIdx = definition.getExpiryColumnIndex();
        final String viewName = definition.getViewToken().getTableName();

        PartitionExpiryCache cache = expiryCaches.get(viewName);
        if (cache == null) {
            cache = new PartitionExpiryCache();
            expiryCaches.put(viewName, cache);
        }

        try (TableReader reader = engine.getReader(baseTableToken)) {
            final int partitionCount = reader.getPartitionCount();
            cache.ensureCapacity(partitionCount);

            for (int i = 0; i < partitionCount; i++) {
                final long rowCount = reader.openPartition(i);
                if (rowCount <= 0) {
                    continue;
                }

                long minExpiry;
                long maxExpiry;

                final long nameTxn = reader.getTxFile().getPartitionNameTxn(i);
                if (cache.isValid(i, nameTxn)) {
                    minExpiry = cache.getMinExpiry(i);
                    maxExpiry = cache.getMaxExpiry(i);
                } else {
                    final int columnBase = reader.getColumnBase(i);
                    final int primaryIdx = TableReader.getPrimaryColumnIndex(columnBase, expiryColIdx);
                    final MemoryCR column = reader.getColumn(primaryIdx);
                    minExpiry = column.getLong(0);
                    maxExpiry = column.getLong((rowCount - 1) * 8);
                    cache.set(i, minExpiry, maxExpiry, nameTxn);
                }

                final long partitionTs = reader.getPartitionTimestampByIndex(i);
                if (maxExpiry < now) {
                    fullyExpiredPartitionTimestamps.add(partitionTs);
                } else if (minExpiry < now) {
                    partiallyExpiredPartitionTimestamps.add(partitionTs);
                }
            }
        }
    }

    private boolean compactPartitionsViaSql(
            ExpiringViewDefinition definition,
            String baseTableName,
            int partitionBy,
            LongList partitionTimestamps,
            String notExpiredPredicate
    ) {
        if (partitionTimestamps.size() == 0) {
            return false;
        }

        final String stagingTableName = STAGING_TABLE_PREFIX + definition.getViewToken().getTableName();
        boolean workDone = false;

        try {
            // Create staging table with same schema, non-WAL, non-partitioned
            sqlSink.clear();
            sqlSink.putAscii("CREATE TABLE IF NOT EXISTS '").put(stagingTableName)
                    .putAscii("' AS (SELECT * FROM ").put(baseTableName)
                    .putAscii(" WHERE 1=0)");
            executeSql(sqlSink);

            for (int i = 0, n = partitionTimestamps.size(); i < n; i++) {
                final long partitionTs = partitionTimestamps.getQuick(i);
                final String partitionStr = formatPartitionTimestamp(partitionBy, partitionTs);

                try {
                    // Insert non-expired rows into staging
                    sqlSink.clear();
                    sqlSink.putAscii("INSERT INTO '").put(stagingTableName)
                            .putAscii("' SELECT * FROM ").put(baseTableName)
                            .putAscii(" WHERE ").put(notExpiredPredicate)
                            .putAscii(" AND ts IN '").put(partitionStr).putAscii("'");
                    executeSql(sqlSink);

                    // Drop the partition
                    sqlSink.clear();
                    sqlSink.putAscii("ALTER TABLE ").put(baseTableName)
                            .putAscii(" DROP PARTITION LIST '").put(partitionStr).putAscii("'");
                    executeSql(sqlSink);

                    // Reinsert from staging
                    sqlSink.clear();
                    sqlSink.putAscii("INSERT INTO ").put(baseTableName)
                            .putAscii(" SELECT * FROM '").put(stagingTableName).putAscii("'");
                    executeSql(sqlSink);

                    // Truncate staging for next partition
                    sqlSink.clear();
                    sqlSink.putAscii("TRUNCATE TABLE '").put(stagingTableName).putAscii("'");
                    executeSql(sqlSink);

                    workDone = true;
                    LOG.info().$("compacted partition [view=").$(definition.getViewToken().getTableName())
                            .$(", partition=").$(partitionStr)
                            .$(']').$();
                } catch (Throwable th) {
                    LOG.error().$("partition compaction failed [view=").$(definition.getViewToken().getTableName())
                            .$(", partition=").$(partitionStr)
                            .$(", msg=").$(th.getMessage())
                            .I$();
                }
            }
        } finally {
            try {
                sqlSink.clear();
                sqlSink.putAscii("DROP TABLE IF EXISTS '").put(stagingTableName).putAscii("'");
                executeSql(sqlSink);
            } catch (Throwable th) {
                LOG.error().$("failed to drop staging table [table=").$(stagingTableName)
                        .$(", msg=").$(th.getMessage())
                        .I$();
            }
        }
        return workDone;
    }

    private long countExpiredRows(String baseTableName, String predicate, String partitionStr) {
        sqlSink.clear();
        sqlSink.putAscii("SELECT count() FROM ").put(baseTableName)
                .putAscii(" WHERE (").put(predicate).putAscii(")")
                .putAscii(" AND ts IN '").put(partitionStr).putAscii("'");

        ((SqlExecutionContextImpl) sqlExecutionContext).initNow();
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            try (RecordCursorFactory factory = compiler.compile(sqlSink, sqlExecutionContext).getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    if (cursor.hasNext()) {
                        return cursor.getRecord().getLong(0);
                    }
                }
            }
        } catch (SqlException e) {
            throw new RuntimeException(e);
        }
        return 0;
    }

    private boolean dropFullPartitions(
            ExpiringViewDefinition definition,
            String baseTableName,
            int partitionBy,
            LongList partitionTimestamps
    ) {
        if (partitionTimestamps.size() == 0) {
            return false;
        }

        sqlSink.clear();
        sqlSink.putAscii("ALTER TABLE ").put(baseTableName).putAscii(" DROP PARTITION LIST ");

        for (int i = 0, n = partitionTimestamps.size(); i < n; i++) {
            if (i > 0) {
                sqlSink.putAscii(", ");
            }
            sqlSink.putAscii("'");
            PartitionBy.setSinkForPartition(sqlSink, ColumnType.TIMESTAMP, partitionBy, partitionTimestamps.getQuick(i));
            sqlSink.putAscii("'");
        }

        try {
            executeSql(sqlSink);
            LOG.info().$("dropped ").$(partitionTimestamps.size()).$(" expired partitions [view=")
                    .$(definition.getViewToken().getTableName())
                    .$(", table=").$(baseTableName)
                    .$(']').$();
            return true;
        } catch (Throwable th) {
            LOG.error().$("failed to drop partitions [view=").$(definition.getViewToken().getTableName())
                    .$(", msg=").$(th.getMessage())
                    .I$();
            return false;
        }
    }

    private void executeSql(CharSequence sql) {
        try {
            ((SqlExecutionContextImpl) sqlExecutionContext).initNow();
            engine.execute(sql, sqlExecutionContext, null);
        } catch (SqlException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * For multi-view scenarios: a partition can only be fully dropped if ALL views
     * on the same base table agree it is fully expired. Partitions that are only
     * expired for this view but not siblings are moved to compaction instead.
     */
    private void filterPartitionsForMultiView(
            ExpiringViewDefinition definition,
            int partitionBy
    ) {
        engine.getExpiringViewsByBaseTable(definition.getBaseTableName(), siblingViews);
        if (siblingViews.size() <= 1) {
            return; // No siblings, nothing to filter
        }

        final LongList demoted = new LongList();
        for (int i = 0, n = fullyExpiredPartitionTimestamps.size(); i < n; i++) {
            final long partTs = fullyExpiredPartitionTimestamps.getQuick(i);
            final String partitionStr = formatPartitionTimestamp(partitionBy, partTs);

            boolean allAgree = true;
            for (int s = 0, sn = siblingViews.size(); s < sn; s++) {
                final ExpiringViewDefinition sibling = siblingViews.getQuick(s);
                if (sibling == definition) {
                    continue;
                }
                try {
                    long siblingExpiredCount = countExpiredRows(
                            definition.getBaseTableName(),
                            sibling.getExpiryPredicateSql(),
                            partitionStr
                    );
                    // We need to know total row count for this partition
                    long totalCount = countTotalRows(definition.getBaseTableName(), partitionStr);
                    if (siblingExpiredCount < totalCount) {
                        allAgree = false;
                        break;
                    }
                } catch (Throwable th) {
                    // If we can't verify, be safe and don't drop
                    allAgree = false;
                    break;
                }
            }
            if (!allAgree) {
                demoted.add(partTs);
            }
        }

        // Move demoted partitions from fully expired to partially expired
        for (int i = 0, n = demoted.size(); i < n; i++) {
            final long ts = demoted.getQuick(i);
            fullyExpiredPartitionTimestamps.remove(ts);
            partiallyExpiredPartitionTimestamps.add(ts);
        }
    }

    private long countTotalRows(String baseTableName, String partitionStr) {
        sqlSink.clear();
        sqlSink.putAscii("SELECT count() FROM ").put(baseTableName)
                .putAscii(" WHERE ts IN '").put(partitionStr).putAscii("'");

        ((SqlExecutionContextImpl) sqlExecutionContext).initNow();
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            try (RecordCursorFactory factory = compiler.compile(sqlSink, sqlExecutionContext).getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    if (cursor.hasNext()) {
                        return cursor.getRecord().getLong(0);
                    }
                }
            }
        } catch (SqlException e) {
            throw new RuntimeException(e);
        }
        return 0;
    }

    /**
     * Builds a NOT-expired predicate that keeps rows not considered expired by ALL views
     * on the same base table. When multiple views share a table, a row can only be removed
     * if every view agrees it is expired.
     */
    private String buildIntersectedNotExpiredPredicate(ExpiringViewDefinition definition) {
        engine.getExpiringViewsByBaseTable(definition.getBaseTableName(), siblingViews);
        if (siblingViews.size() <= 1) {
            return "NOT (" + definition.getExpiryPredicateSql() + ")";
        }
        // Build: NOT (pred1 AND pred2 AND ... AND predN)
        // This keeps any row that ANY view still considers non-expired
        final StringBuilder sb = new StringBuilder();
        sb.append("NOT (");
        boolean first = true;
        for (int i = 0, n = siblingViews.size(); i < n; i++) {
            if (!first) {
                sb.append(" AND ");
            }
            sb.append('(').append(siblingViews.getQuick(i).getExpiryPredicateSql()).append(')');
            first = false;
        }
        sb.append(')');
        return sb.toString();
    }

    private static String formatPartitionTimestamp(int partitionBy, long partitionTs) {
        final StringSink sink = new StringSink();
        PartitionBy.setSinkForPartition(sink, ColumnType.TIMESTAMP, partitionBy, partitionTs);
        return sink.toString();
    }

    private boolean runCustomCleanup(ExpiringViewDefinition definition, long now) {
        final String baseTableName = definition.getBaseTableName();
        final TableToken baseTableToken = engine.getTableTokenIfExists(baseTableName);
        if (baseTableToken == null) {
            LOG.info().$("base table no longer exists [view=").$(definition.getViewToken().getTableName())
                    .$(", table=").$(baseTableName).$(']').$();
            return false;
        }

        final String predicate = definition.getExpiryPredicateSql();
        fullyExpiredPartitionTimestamps.clear();
        partiallyExpiredPartitionTimestamps.clear();

        int partitionBy;

        try (TableReader reader = engine.getReader(baseTableToken)) {
            final int partitionCount = reader.getPartitionCount();
            if (partitionCount == 0) {
                return false;
            }

            partitionBy = reader.getPartitionedBy();

            for (int i = 0; i < partitionCount; i++) {
                final long rowCount = reader.openPartition(i);
                if (rowCount <= 0) {
                    continue;
                }

                final long partitionTs = reader.getPartitionTimestampByIndex(i);
                final String partitionStr = formatPartitionTimestamp(partitionBy, partitionTs);

                try {
                    long expiredCount = countExpiredRows(baseTableName, predicate, partitionStr);
                    if (expiredCount >= rowCount) {
                        fullyExpiredPartitionTimestamps.add(partitionTs);
                    } else if (expiredCount > 0) {
                        partiallyExpiredPartitionTimestamps.add(partitionTs);
                    }
                } catch (Throwable th) {
                    LOG.error().$("failed to count expired rows [view=").$(definition.getViewToken().getTableName())
                            .$(", partition=").$(partitionStr)
                            .$(", msg=").$(th.getMessage())
                            .I$();
                }
            }
        }

        // Phase 7: multi-view intersection — demote partitions that sibling views still need
        filterPartitionsForMultiView(definition, partitionBy);

        boolean workDone = false;

        if (fullyExpiredPartitionTimestamps.size() > 0) {
            workDone = dropFullPartitions(definition, baseTableName, partitionBy, fullyExpiredPartitionTimestamps);
        }

        if (partiallyExpiredPartitionTimestamps.size() > 0) {
            final String notExpiredPredicate = buildIntersectedNotExpiredPredicate(definition);
            workDone |= compactPartitionsViaSql(definition, baseTableName, partitionBy, partiallyExpiredPartitionTimestamps, notExpiredPredicate);
        }

        return workDone;
    }

    private boolean runTimestampCompareCleanup(ExpiringViewDefinition definition, long now) {
        final String baseTableName = definition.getBaseTableName();
        final TableToken baseTableToken = engine.getTableTokenIfExists(baseTableName);
        if (baseTableToken == null) {
            LOG.info().$("base table no longer exists [view=").$(definition.getViewToken().getTableName())
                    .$(", table=").$(baseTableName).$(']').$();
            return false;
        }

        if (definition.getExpiryColumnIndex() < 0) {
            return false;
        }

        // Step 1: Scan partitions and classify by expiry stats
        classifyTimestampComparePartitions(definition, baseTableToken, now);

        if (fullyExpiredPartitionTimestamps.size() == 0 && partiallyExpiredPartitionTimestamps.size() == 0) {
            return false;
        }

        int partitionBy;
        try (TableReader reader = engine.getReader(baseTableToken)) {
            partitionBy = reader.getPartitionedBy();
        }

        // Phase 7: multi-view intersection — demote partitions that sibling views still need
        filterPartitionsForMultiView(definition, partitionBy);

        boolean workDone = false;

        // Step 2: Drop fully expired partitions
        if (fullyExpiredPartitionTimestamps.size() > 0) {
            workDone = dropFullPartitions(definition, baseTableName, partitionBy, fullyExpiredPartitionTimestamps);
        }

        // Step 3: Compact partially expired partitions
        if (partiallyExpiredPartitionTimestamps.size() > 0) {
            final String notExpiredPredicate = buildIntersectedNotExpiredPredicate(definition);
            workDone |= compactPartitionsViaSql(definition, baseTableName, partitionBy, partiallyExpiredPartitionTimestamps, notExpiredPredicate);
        }

        return workDone;
    }
}
