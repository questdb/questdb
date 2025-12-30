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
    private static final int DEDUPE_ROW_COUNT_COLUMN = 12;
    private static final int DEDUP_NAME_COLUMN = 6;
    private static final int DESIGNATED_TIMESTAMP_COLUMN = 2;
    private static final int DIRECTORY_NAME_COLUMN = 17;
    private static final int ID_COLUMN = 0;
    private static final int IS_MAT_VIEW_COLUMN = 9;
    private static final int LAST_WAL_TIMESTAMP_COLUMN = 16;
    private static final int LAST_WRITE_TIMESTAMP_COLUMN = 13;
    private static final int MAX_UNCOMMITTED_ROWS_COLUMN = 34;
    private static final int MEMORY_PRESSURE_LEVEL_COLUMN = 18;
    private static final int MERGE_THROUGHPUT_COUNT_COLUMN = 29;
    private static final int MERGE_THROUGHPUT_MAX_COLUMN = 33;
    private static final int MERGE_THROUGHPUT_P50_COLUMN = 30;
    private static final int MERGE_THROUGHPUT_P90_COLUMN = 31;
    private static final int MERGE_THROUGHPUT_P99_COLUMN = 32;
    private static final RecordMetadata METADATA;
    private static final int O3_MAX_LAG_COLUMN = 35;
    private static final int PARTITION_BY_COLUMN = 3;
    private static final int PENDING_ROW_COUNT_COLUMN = 11;
    private static final int ROW_COUNT_COLUMN = 10;
    private static final int SEQUENCER_TXN_COLUMN = 15;
    private static final int SUSPENDED_COLUMN = 5;
    private static final int TABLE_NAME = 1;
    private static final int TTL_UNIT_COLUMN = 8;
    private static final int TTL_VALUE_COLUMN = 7;
    private static final int TXN_COUNT_COLUMN = 19;
    private static final int TXN_SIZE_MAX_COLUMN = 23;
    private static final int TXN_SIZE_P50_COLUMN = 20;
    private static final int TXN_SIZE_P90_COLUMN = 21;
    private static final int TXN_SIZE_P99_COLUMN = 22;
    private static final int WAL_ENABLED_COLUMN = 4;
    private static final int WRITE_AMPLIFICATION_COUNT_COLUMN = 24;
    private static final int WRITE_AMPLIFICATION_MAX_COLUMN = 28;
    private static final int WRITE_AMPLIFICATION_P50_COLUMN = 25;
    private static final int WRITE_AMPLIFICATION_P90_COLUMN = 26;
    private static final int WRITE_AMPLIFICATION_P99_COLUMN = 27;
    private static final int WRITER_TXN_COLUMN = 14;

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
                private RecentWriteTracker recentWriteTracker;
                private CairoTable table;
                private TableSequencerAPI tableSequencerAPI;

                @Override
                public boolean getBool(int col) {
                    return switch (col) {
                        case WAL_ENABLED_COLUMN -> table.isWalEnabled();
                        case DEDUP_NAME_COLUMN -> table.hasDedup();
                        case IS_MAT_VIEW_COLUMN -> table.getTableToken().isMatView();
                        case SUSPENDED_COLUMN ->
                                table.isWalEnabled() && tableSequencerAPI.isSuspended(table.getTableToken());
                        default -> false;
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
                    if (col == MEMORY_PRESSURE_LEVEL_COLUMN) {
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
                public double getDouble(int col) {
                    return switch (col) {
                        case WRITE_AMPLIFICATION_P50_COLUMN ->
                                recentWriteTracker.getWriteAmplificationP50(table.getTableToken());
                        case WRITE_AMPLIFICATION_P90_COLUMN ->
                                recentWriteTracker.getWriteAmplificationP90(table.getTableToken());
                        case WRITE_AMPLIFICATION_P99_COLUMN ->
                                recentWriteTracker.getWriteAmplificationP99(table.getTableToken());
                        case WRITE_AMPLIFICATION_MAX_COLUMN ->
                                recentWriteTracker.getWriteAmplificationMax(table.getTableToken());
                        default -> Double.NaN;
                    };
                }

                @Override
                public long getLong(int col) {
                    return switch (col) {
                        case O3_MAX_LAG_COLUMN -> table.getO3MaxLag();
                        case ROW_COUNT_COLUMN -> recentWriteTracker.getRowCount(table.getTableToken());
                        case WRITER_TXN_COLUMN -> recentWriteTracker.getWriterTxn(table.getTableToken());
                        case SEQUENCER_TXN_COLUMN -> recentWriteTracker.getSequencerTxn(table.getTableToken());
                        case PENDING_ROW_COUNT_COLUMN -> recentWriteTracker.getWalRowCount(table.getTableToken());
                        case DEDUPE_ROW_COUNT_COLUMN -> recentWriteTracker.getDedupRowCount(table.getTableToken());
                        case TXN_COUNT_COLUMN -> recentWriteTracker.getTxnCount(table.getTableToken());
                        case TXN_SIZE_P50_COLUMN -> recentWriteTracker.getTxnSizeP50(table.getTableToken());
                        case TXN_SIZE_P90_COLUMN -> recentWriteTracker.getTxnSizeP90(table.getTableToken());
                        case TXN_SIZE_P99_COLUMN -> recentWriteTracker.getTxnSizeP99(table.getTableToken());
                        case TXN_SIZE_MAX_COLUMN -> recentWriteTracker.getTxnSizeMax(table.getTableToken());
                        case WRITE_AMPLIFICATION_COUNT_COLUMN ->
                                recentWriteTracker.getWriteAmplificationCount(table.getTableToken());
                        case MERGE_THROUGHPUT_COUNT_COLUMN ->
                                recentWriteTracker.getMergeThroughputCount(table.getTableToken());
                        case MERGE_THROUGHPUT_P50_COLUMN ->
                                recentWriteTracker.getMergeThroughputP50(table.getTableToken());
                        case MERGE_THROUGHPUT_P90_COLUMN ->
                                recentWriteTracker.getMergeThroughputP90(table.getTableToken());
                        case MERGE_THROUGHPUT_P99_COLUMN ->
                                recentWriteTracker.getMergeThroughputP99(table.getTableToken());
                        case MERGE_THROUGHPUT_MAX_COLUMN ->
                                recentWriteTracker.getMergeThroughputMax(table.getTableToken());
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
                    return switch (col) {
                        case LAST_WRITE_TIMESTAMP_COLUMN -> recentWriteTracker.getWriteTimestamp(table.getTableToken());
                        case LAST_WAL_TIMESTAMP_COLUMN -> recentWriteTracker.getLastWalTimestamp(table.getTableToken());
                        default -> Numbers.LONG_NULL;
                    };
                }

                private void of(CairoTable table, RecentWriteTracker recentWriteTracker, TableSequencerAPI tableSequencerAPI) {
                    this.table = table;
                    this.recentWriteTracker = recentWriteTracker;
                    this.tableSequencerAPI = tableSequencerAPI;
                }
            }
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("id", ColumnType.INT));                        // 0
        metadata.add(new TableColumnMetadata("table_name", ColumnType.STRING));             // 1
        metadata.add(new TableColumnMetadata("designatedTimestamp", ColumnType.STRING));    // 2
        metadata.add(new TableColumnMetadata("partitionBy", ColumnType.STRING));            // 3
        metadata.add(new TableColumnMetadata("walEnabled", ColumnType.BOOLEAN));            // 4
        metadata.add(new TableColumnMetadata("suspended", ColumnType.BOOLEAN));             // 5
        metadata.add(new TableColumnMetadata("dedup", ColumnType.BOOLEAN));                 // 6
        metadata.add(new TableColumnMetadata("ttlValue", ColumnType.INT));                  // 7
        metadata.add(new TableColumnMetadata("ttlUnit", ColumnType.STRING));                // 8
        metadata.add(new TableColumnMetadata("matView", ColumnType.BOOLEAN));               // 9
        metadata.add(new TableColumnMetadata("rowCount", ColumnType.LONG));                 // 10
        metadata.add(new TableColumnMetadata("pendingRowCount", ColumnType.LONG));          // 11
        metadata.add(new TableColumnMetadata("dedupeRowCount", ColumnType.LONG));           // 12
        metadata.add(new TableColumnMetadata("lastWriteTimestamp", ColumnType.TIMESTAMP));  // 13
        metadata.add(new TableColumnMetadata("writerTxn", ColumnType.LONG));                // 14
        metadata.add(new TableColumnMetadata("sequencerTxn", ColumnType.LONG));             // 15
        metadata.add(new TableColumnMetadata("lastWalTimestamp", ColumnType.TIMESTAMP));    // 16
        metadata.add(new TableColumnMetadata("directoryName", ColumnType.STRING));          // 17
        metadata.add(new TableColumnMetadata("memoryPressureLevel", ColumnType.INT));       // 18
        metadata.add(new TableColumnMetadata("txnCount", ColumnType.LONG));                 // 19
        metadata.add(new TableColumnMetadata("txnSizeP50", ColumnType.LONG));               // 20
        metadata.add(new TableColumnMetadata("txnSizeP90", ColumnType.LONG));               // 21
        metadata.add(new TableColumnMetadata("txnSizeP99", ColumnType.LONG));               // 22
        metadata.add(new TableColumnMetadata("txnSizeMax", ColumnType.LONG));               // 23
        metadata.add(new TableColumnMetadata("writeAmplificationCount", ColumnType.LONG));  // 24
        metadata.add(new TableColumnMetadata("writeAmplificationP50", ColumnType.DOUBLE));  // 25
        metadata.add(new TableColumnMetadata("writeAmplificationP90", ColumnType.DOUBLE));  // 26
        metadata.add(new TableColumnMetadata("writeAmplificationP99", ColumnType.DOUBLE));  // 27
        metadata.add(new TableColumnMetadata("writeAmplificationMax", ColumnType.DOUBLE));  // 28
        metadata.add(new TableColumnMetadata("mergeThroughputCount", ColumnType.LONG));     // 29
        metadata.add(new TableColumnMetadata("mergeThroughputP50", ColumnType.LONG));       // 30
        metadata.add(new TableColumnMetadata("mergeThroughputP90", ColumnType.LONG));       // 31
        metadata.add(new TableColumnMetadata("mergeThroughputP99", ColumnType.LONG));       // 32
        metadata.add(new TableColumnMetadata("mergeThroughputMax", ColumnType.LONG));       // 33
        metadata.add(new TableColumnMetadata("maxUncommittedRows", ColumnType.INT));        // 34
        metadata.add(new TableColumnMetadata("o3MaxLag", ColumnType.LONG));                 // 35
        METADATA = metadata;
    }
}
