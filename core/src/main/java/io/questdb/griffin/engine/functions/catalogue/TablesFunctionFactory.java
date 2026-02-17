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

package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoTable;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.MetadataCacheReader;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.pool.RecentWriteTracker;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;

public class TablesFunctionFactory implements FunctionFactory {
    private static final int DEDUP_NAME_COLUMN = 5;
    private static final int DESIGNATED_TIMESTAMP_COLUMN = 2;
    private static final int DIRECTORY_NAME_COLUMN = 9;
    // Base columns (0-11) - pre-existing, keep names
    private static final int ID_COLUMN = 0;
    private static final int IS_MAT_VIEW_COLUMN = 8;
    private static final int MAX_UNCOMMITTED_ROWS_COLUMN = 10;
    private static final RecordMetadata METADATA;
    private static final int O3_MAX_LAG_COLUMN = 11;
    private static final int PARTITION_BY_COLUMN = 3;
    // Replica metrics (39-44) - replica_* prefix
    private static final int REPLICA_BATCH_COUNT_COLUMN = 39;
    private static final int REPLICA_BATCH_SIZE_MAX_COLUMN = 43;
    private static final int REPLICA_BATCH_SIZE_P50_COLUMN = 40;
    private static final int REPLICA_BATCH_SIZE_P90_COLUMN = 41;
    private static final int REPLICA_BATCH_SIZE_P99_COLUMN = 42;
    private static final int REPLICA_MORE_PENDING_COLUMN = 44;
    private static final int TABLE_LAST_WRITE_TIMESTAMP_COLUMN = 17;
    private static final int TABLE_MAX_TIMESTAMP_COLUMN = 16;
    private static final int TABLE_MEMORY_PRESSURE_COLUMN = 19;
    private static final int TABLE_MERGE_RATE_COUNT_COLUMN = 25;
    private static final int TABLE_MERGE_RATE_MAX_COLUMN = 29;
    private static final int TABLE_MERGE_RATE_P50_COLUMN = 26;
    private static final int TABLE_MERGE_RATE_P90_COLUMN = 27;
    private static final int TABLE_MERGE_RATE_P99_COLUMN = 28;
    private static final int TABLE_MIN_TIMESTAMP_COLUMN = 15;
    private static final int TABLE_NAME = 1;
    private static final int TABLE_ROW_COUNT_COLUMN = 14;
    // Table metrics (12-29) - table_* prefix
    private static final int TABLE_SUSPENDED_COLUMN = 12;
    private static final int TABLE_TXN_COLUMN = 18;
    private static final int TABLE_TYPE_COLUMN = 13;
    private static final int TABLE_WRITE_AMP_COUNT_COLUMN = 20;
    private static final int TABLE_WRITE_AMP_MAX_COLUMN = 24;
    private static final int TABLE_WRITE_AMP_P50_COLUMN = 21;
    private static final int TABLE_WRITE_AMP_P90_COLUMN = 22;
    private static final int TABLE_WRITE_AMP_P99_COLUMN = 23;
    private static final int TTL_UNIT_COLUMN = 7;
    private static final int TTL_VALUE_COLUMN = 6;
    private static final int WAL_DEDUP_ROW_COUNT_COLUMN = 31;
    private static final int WAL_ENABLED_COLUMN = 4;
    private static final int WAL_MAX_TIMESTAMP_COLUMN = 33;
    // WAL metrics (30-38) - wal_* prefix
    private static final int WAL_PENDING_ROW_COUNT_COLUMN = 30;
    private static final int WAL_TXN_COLUMN = 32;
    private static final int WAL_TX_COUNT_COLUMN = 34;
    private static final int WAL_TX_SIZE_MAX_COLUMN = 38;
    private static final int WAL_TX_SIZE_P50_COLUMN = 35;
    private static final int WAL_TX_SIZE_P90_COLUMN = 36;
    private static final int WAL_TX_SIZE_P99_COLUMN = 37;

    public static String getTtlUnit(int ttl) {
        if (ttl == 0) {
            return "HOUR";
        }
        if (ttl < 0) {
            return -ttl % 12 != 0 ? "MONTH" : "YEAR";
        }
        if (ttl % 24 != 0) {
            return "HOUR";
        }
        ttl /= 24;
        return ttl % 7 != 0 ? "DAY" : "WEEK";
    }

    public static int getTtlValue(int ttl) {
        if (ttl == 0) {
            return 0;
        }
        if (ttl > 0) {
            if (ttl % 24 != 0) {
                return ttl;
            }
            ttl /= 24;
            return (ttl % 7 == 0) ? (ttl / 7) : ttl;
        }
        ttl = -ttl;
        return (ttl % 12 == 0) ? (ttl / 12) : ttl;
    }

    @Override
    public String getSignature() {
        return "tables()";
    }

    @Override
    public boolean isRuntimeConstant() {
        return true;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        return new CursorFunction(new TablesCursorFactory()) {
            @Override
            public boolean isRuntimeConstant() {
                return true;
            }
        };
    }

    public static class TablesCursorFactory extends AbstractRecordCursorFactory {
        public static final Log LOG = LogFactory.getLog(TablesCursorFactory.class);
        private final TablesRecordCursor cursor;
        private final CharSequenceObjHashMap<CairoTable> tableCache = new CharSequenceObjHashMap<>();
        private long tableCacheVersion = -1;

        public TablesCursorFactory() {
            super(METADATA);
            cursor = new TablesRecordCursor(tableCache);
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            final CairoEngine engine = executionContext.getCairoEngine();
            try (MetadataCacheReader metadataRO = engine.getMetadataCache().readLock()) {
                tableCacheVersion = metadataRO.snapshot(tableCache, tableCacheVersion);
            }
            cursor.of(engine.getRecentWriteTracker(), engine.getTableSequencerAPI());
            cursor.toTop();
            return cursor;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.type("tables()");
        }

        @Override
        protected void _close() {
            cursor.close();
        }

        private static class TablesRecordCursor implements NoRandomAccessRecordCursor {
            private final TableListRecord record = new TableListRecord();
            private final CharSequenceObjHashMap<CairoTable> tableCache;
            private int iteratorIdx = -1;
            private int iteratorLim;
            private RecentWriteTracker recentWriteTracker;
            private TableSequencerAPI tableSequencerAPI;

            public TablesRecordCursor(CharSequenceObjHashMap<CairoTable> tableCache) {
                this.tableCache = tableCache;
                this.iteratorLim = tableCache.size() - 1;
            }

            @Override
            public void close() {
            }

            @Override
            public Record getRecord() {
                return record;
            }

            @Override
            public boolean hasNext() {
                if (iteratorIdx < iteratorLim) {
                    record.of(tableCache.getAt(++iteratorIdx), recentWriteTracker, tableSequencerAPI);
                    return true;
                }
                return false;
            }

            public void of(RecentWriteTracker recentWriteTracker, TableSequencerAPI tableSequencerAPI) {
                this.recentWriteTracker = recentWriteTracker;
                this.tableSequencerAPI = tableSequencerAPI;
                // can is refreshed every time cursor is refreshed
                this.iteratorLim = tableCache.size() - 1;
            }

            @Override
            public long preComputedStateSize() {
                return 0;
            }

            @Override
            public long size() {
                return iteratorLim + 1;
            }

            @Override
            public void toTop() {
                iteratorIdx = -1;
            }

            private static class TableListRecord implements Record {
                private StringSink lazyStringSink = null;
                private CairoTable table;
                private TableSequencerAPI tableSequencerAPI;
                private TimestampDriver timestampDriver;
                private RecentWriteTracker.WriteStats writeStats;

                @Override
                public boolean getBool(int col) {
                    return switch (col) {
                        case WAL_ENABLED_COLUMN -> table.isWalEnabled();
                        case DEDUP_NAME_COLUMN -> table.hasDedup();
                        case IS_MAT_VIEW_COLUMN -> table.getTableToken().isMatView();
                        case TABLE_SUSPENDED_COLUMN ->
                                table.isWalEnabled() && tableSequencerAPI.isSuspended(table.getTableToken());
                        case REPLICA_MORE_PENDING_COLUMN -> writeStats != null && writeStats.isReplicaMorePending();
                        default -> false;
                    };
                }

                @Override
                public char getChar(int col) {
                    if (col == TABLE_TYPE_COLUMN) {
                        if (table.getTableToken().isMatView()) {
                            return 'M';
                        } else if (table.getTableToken().isView()) {
                            return 'V';
                        } else {
                            return 'T';
                        }
                    }
                    return 0;
                }

                @Override
                public double getDouble(int col) {
                    if (writeStats == null) {
                        return 0.0;
                    }
                    return switch (col) {
                        case TABLE_WRITE_AMP_P50_COLUMN -> writeStats.getWriteAmplificationP50();
                        case TABLE_WRITE_AMP_P90_COLUMN -> writeStats.getWriteAmplificationP90();
                        case TABLE_WRITE_AMP_P99_COLUMN -> writeStats.getWriteAmplificationP99();
                        case TABLE_WRITE_AMP_MAX_COLUMN -> writeStats.getWriteAmplificationMax();
                        default -> Double.NaN;
                    };
                }

                @Override
                public int getInt(int col) {
                    if (col == ID_COLUMN) {
                        return table.getId();
                    }
                    if (col == TTL_VALUE_COLUMN) {
                        return getTtlValue(table.getTtlHoursOrMonths());
                    }
                    if (col == TABLE_MEMORY_PRESSURE_COLUMN) {
                        if (!table.isWalEnabled()) {
                            return Numbers.INT_NULL;
                        }
                        SeqTxnTracker tracker = tableSequencerAPI.getTxnTracker(table.getTableToken());
                        return tracker.getMemPressureControl().getMemoryPressureLevel();
                    }
                    assert col == MAX_UNCOMMITTED_ROWS_COLUMN;
                    return table.getMaxUncommittedRows();
                }

                @Override
                public long getLong(int col) {
                    if (col == O3_MAX_LAG_COLUMN) {
                        return table.getO3MaxLag();
                    }
                    if (writeStats == null) {
                        return col == WAL_PENDING_ROW_COUNT_COLUMN || col == WAL_DEDUP_ROW_COUNT_COLUMN
                                || col == WAL_TX_COUNT_COLUMN || col == WAL_TX_SIZE_P50_COLUMN
                                || col == WAL_TX_SIZE_P90_COLUMN || col == WAL_TX_SIZE_P99_COLUMN
                                || col == WAL_TX_SIZE_MAX_COLUMN || col == TABLE_WRITE_AMP_COUNT_COLUMN
                                || col == TABLE_MERGE_RATE_COUNT_COLUMN || col == TABLE_MERGE_RATE_P50_COLUMN
                                || col == TABLE_MERGE_RATE_P90_COLUMN || col == TABLE_MERGE_RATE_P99_COLUMN
                                || col == TABLE_MERGE_RATE_MAX_COLUMN
                                || col == REPLICA_BATCH_COUNT_COLUMN || col == REPLICA_BATCH_SIZE_P50_COLUMN
                                || col == REPLICA_BATCH_SIZE_P90_COLUMN || col == REPLICA_BATCH_SIZE_P99_COLUMN
                                || col == REPLICA_BATCH_SIZE_MAX_COLUMN ? 0 : Numbers.LONG_NULL;
                    }
                    return switch (col) {
                        case TABLE_ROW_COUNT_COLUMN -> writeStats.getRowCount();
                        case TABLE_TXN_COLUMN -> writeStats.getWriterTxn();
                        case WAL_TXN_COLUMN -> writeStats.getSequencerTxn();
                        case WAL_PENDING_ROW_COUNT_COLUMN -> writeStats.getWalRowCount();
                        case WAL_DEDUP_ROW_COUNT_COLUMN -> writeStats.getDedupRowCount();
                        case WAL_TX_COUNT_COLUMN -> writeStats.getTxnCount();
                        case WAL_TX_SIZE_P50_COLUMN -> writeStats.getTxnSizeP50();
                        case WAL_TX_SIZE_P90_COLUMN -> writeStats.getTxnSizeP90();
                        case WAL_TX_SIZE_P99_COLUMN -> writeStats.getTxnSizeP99();
                        case WAL_TX_SIZE_MAX_COLUMN -> writeStats.getTxnSizeMax();
                        case TABLE_WRITE_AMP_COUNT_COLUMN -> writeStats.getWriteAmplificationCount();
                        case TABLE_MERGE_RATE_COUNT_COLUMN -> writeStats.getMergeThroughputCount();
                        case TABLE_MERGE_RATE_P50_COLUMN -> writeStats.getMergeThroughputP50();
                        case TABLE_MERGE_RATE_P90_COLUMN -> writeStats.getMergeThroughputP90();
                        case TABLE_MERGE_RATE_P99_COLUMN -> writeStats.getMergeThroughputP99();
                        case TABLE_MERGE_RATE_MAX_COLUMN -> writeStats.getMergeThroughputMax();
                        case REPLICA_BATCH_COUNT_COLUMN -> writeStats.getBatchCount();
                        case REPLICA_BATCH_SIZE_P50_COLUMN -> writeStats.getBatchSizeP50();
                        case REPLICA_BATCH_SIZE_P90_COLUMN -> writeStats.getBatchSizeP90();
                        case REPLICA_BATCH_SIZE_P99_COLUMN -> writeStats.getBatchSizeP99();
                        case REPLICA_BATCH_SIZE_MAX_COLUMN -> writeStats.getBatchSizeMax();
                        default -> Numbers.LONG_NULL;
                    };
                }

                @Override
                public CharSequence getStrA(int col) {
                    return switch (col) {
                        case TABLE_NAME -> table.getTableName();
                        case PARTITION_BY_COLUMN -> table.getPartitionByName();
                        case TTL_UNIT_COLUMN -> getTtlUnit(table.getTtlHoursOrMonths());
                        case DESIGNATED_TIMESTAMP_COLUMN -> table.getTimestampName();
                        case DIRECTORY_NAME_COLUMN -> {
                            if (table.isSoftLink()) {
                                if (lazyStringSink == null) {
                                    lazyStringSink = new StringSink();
                                }
                                lazyStringSink.clear();
                                lazyStringSink.put(table.getDirectoryName()).put(" (->)");
                                yield lazyStringSink;
                            }
                            yield table.getDirectoryName();
                        }
                        default -> null;
                    };
                }

                @Override
                public CharSequence getStrB(int col) {
                    return getStrA(col);
                }

                @Override
                public int getStrLen(int col) {
                    return TableUtils.lengthOf(getStrA(col));
                }

                @Override
                public long getTimestamp(int col) {
                    if (writeStats == null) {
                        return Numbers.LONG_NULL;
                    }
                    return switch (col) {
                        case TABLE_MIN_TIMESTAMP_COLUMN -> timestampDriver.toMicros(writeStats.getTableMinTimestamp());
                        case TABLE_MAX_TIMESTAMP_COLUMN -> timestampDriver.toMicros(writeStats.getTableMaxTimestamp());
                        case WAL_MAX_TIMESTAMP_COLUMN -> timestampDriver.toMicros(writeStats.getLastWalTimestamp());
                        // table_last_write_timestamp is always in microseconds (system clock)
                        case TABLE_LAST_WRITE_TIMESTAMP_COLUMN -> writeStats.getTimestamp();
                        default -> Numbers.LONG_NULL;
                    };
                }

                private void of(CairoTable table, RecentWriteTracker recentWriteTracker, TableSequencerAPI tableSequencerAPI) {
                    this.table = table;
                    this.tableSequencerAPI = tableSequencerAPI;
                    this.writeStats = recentWriteTracker.getWriteStats(table.getTableToken());
                    this.timestampDriver = ColumnType.getTimestampDriver(table.getTimestampType());
                }
            }
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        // Base columns (0-11) - pre-existing, keep names
        metadata.add(new TableColumnMetadata("id", ColumnType.INT));                              // 0
        metadata.add(new TableColumnMetadata("table_name", ColumnType.STRING));                   // 1
        metadata.add(new TableColumnMetadata("designatedTimestamp", ColumnType.STRING));          // 2
        metadata.add(new TableColumnMetadata("partitionBy", ColumnType.STRING));                  // 3
        metadata.add(new TableColumnMetadata("walEnabled", ColumnType.BOOLEAN));                  // 4
        metadata.add(new TableColumnMetadata("dedup", ColumnType.BOOLEAN));                       // 5
        metadata.add(new TableColumnMetadata("ttlValue", ColumnType.INT));                        // 6
        metadata.add(new TableColumnMetadata("ttlUnit", ColumnType.STRING));                      // 7
        metadata.add(new TableColumnMetadata("matView", ColumnType.BOOLEAN));                     // 8
        metadata.add(new TableColumnMetadata("directoryName", ColumnType.STRING));                // 9
        metadata.add(new TableColumnMetadata("maxUncommittedRows", ColumnType.INT));              // 10
        metadata.add(new TableColumnMetadata("o3MaxLag", ColumnType.LONG));                       // 11
        // Table metrics (12-29) - table_* prefix
        metadata.add(new TableColumnMetadata("table_suspended", ColumnType.BOOLEAN));             // 12
        metadata.add(new TableColumnMetadata("table_type", ColumnType.CHAR));                     // 13
        metadata.add(new TableColumnMetadata("table_row_count", ColumnType.LONG));                // 14
        metadata.add(new TableColumnMetadata("table_min_timestamp", ColumnType.TIMESTAMP));       // 15
        metadata.add(new TableColumnMetadata("table_max_timestamp", ColumnType.TIMESTAMP));       // 16
        metadata.add(new TableColumnMetadata("table_last_write_timestamp", ColumnType.TIMESTAMP));// 17
        metadata.add(new TableColumnMetadata("table_txn", ColumnType.LONG));                      // 18
        metadata.add(new TableColumnMetadata("table_memory_pressure_level", ColumnType.INT));     // 19
        metadata.add(new TableColumnMetadata("table_write_amp_count", ColumnType.LONG));          // 20
        metadata.add(new TableColumnMetadata("table_write_amp_p50", ColumnType.DOUBLE));          // 21
        metadata.add(new TableColumnMetadata("table_write_amp_p90", ColumnType.DOUBLE));          // 22
        metadata.add(new TableColumnMetadata("table_write_amp_p99", ColumnType.DOUBLE));          // 23
        metadata.add(new TableColumnMetadata("table_write_amp_max", ColumnType.DOUBLE));          // 24
        metadata.add(new TableColumnMetadata("table_merge_rate_count", ColumnType.LONG));         // 25
        metadata.add(new TableColumnMetadata("table_merge_rate_p50", ColumnType.LONG));           // 26
        metadata.add(new TableColumnMetadata("table_merge_rate_p90", ColumnType.LONG));           // 27
        metadata.add(new TableColumnMetadata("table_merge_rate_p99", ColumnType.LONG));           // 28
        metadata.add(new TableColumnMetadata("table_merge_rate_max", ColumnType.LONG));           // 29
        // WAL metrics (30-38) - wal_* prefix
        metadata.add(new TableColumnMetadata("wal_pending_row_count", ColumnType.LONG));          // 30
        metadata.add(new TableColumnMetadata("wal_dedup_row_count_since_start", ColumnType.LONG)); // 31
        metadata.add(new TableColumnMetadata("wal_txn", ColumnType.LONG));                        // 32
        metadata.add(new TableColumnMetadata("wal_max_timestamp", ColumnType.TIMESTAMP));         // 33
        metadata.add(new TableColumnMetadata("wal_tx_count", ColumnType.LONG));                   // 34
        metadata.add(new TableColumnMetadata("wal_tx_size_p50", ColumnType.LONG));                // 35
        metadata.add(new TableColumnMetadata("wal_tx_size_p90", ColumnType.LONG));                // 36
        metadata.add(new TableColumnMetadata("wal_tx_size_p99", ColumnType.LONG));                // 37
        metadata.add(new TableColumnMetadata("wal_tx_size_max", ColumnType.LONG));                // 38
        // Replica metrics (39-44) - replica_* prefix
        metadata.add(new TableColumnMetadata("replica_batch_count", ColumnType.LONG));            // 39
        metadata.add(new TableColumnMetadata("replica_batch_size_p50", ColumnType.LONG));         // 40
        metadata.add(new TableColumnMetadata("replica_batch_size_p90", ColumnType.LONG));         // 41
        metadata.add(new TableColumnMetadata("replica_batch_size_p99", ColumnType.LONG));         // 42
        metadata.add(new TableColumnMetadata("replica_batch_size_max", ColumnType.LONG));         // 43
        metadata.add(new TableColumnMetadata("replica_more_pending", ColumnType.BOOLEAN));        // 44
        METADATA = metadata;
    }
}
