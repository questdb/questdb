/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;

public class TablesFunctionFactory implements FunctionFactory {
    private static final int DEDUP_NAME_COLUMN = 8;
    private static final int DESIGNATED_TIMESTAMP_COLUMN = 2;
    private static final int DIRECTORY_NAME_COLUMN = 7;
    private static final int ID_COLUMN = 0;
    private static final int IS_MAT_VIEW_COLUMN = 11;
    private static final int MAX_UNCOMMITTED_ROWS_COLUMN = 4;
    private static final RecordMetadata METADATA;
    private static final int O3_MAX_LAG_COLUMN = 5;
    private static final int PARTITION_BY_COLUMN = 3;
    private static final int TABLE_NAME = 1;
    private static final int TTL_UNIT_COLUMN = 10;
    private static final int TTL_VALUE_COLUMN = 9;
    private static final int WAL_ENABLED_COLUMN = 6;

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

            public TablesRecordCursor(CharSequenceObjHashMap<CairoTable> tableCache) {
                this.tableCache = tableCache;
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
                if (iteratorIdx < tableCache.size() - 1) {
                    record.of(tableCache.getAt(++iteratorIdx));
                    return true;
                }
                return false;
            }

            @Override
            public long size() {
                return -1;
            }

            @Override
            public void toTop() {
                iteratorIdx = -1;
            }

            private static class TableListRecord implements Record {
                private StringSink lazyStringSink = null;
                private CairoTable table;

                @Override
                public boolean getBool(int col) {
                    switch (col) {
                        case WAL_ENABLED_COLUMN:
                            return table.isWalEnabled();
                        case DEDUP_NAME_COLUMN:
                            return table.hasDedup();
                        case IS_MAT_VIEW_COLUMN:
                            return table.getTableToken().isMatView();
                        default:
                            return false;
                    }
                }

                @Override
                public int getInt(int col) {
                    if (col == ID_COLUMN) {
                        return table.getId();
                    }
                    if (col == TTL_VALUE_COLUMN) {
                        return getTtlValue(table.getTtlHoursOrMonths());
                    }
                    assert col == MAX_UNCOMMITTED_ROWS_COLUMN;
                    return table.getMaxUncommittedRows();
                }

                @Override
                public long getLong(int col) {
                    assert col == O3_MAX_LAG_COLUMN;
                    return table.getO3MaxLag();
                }

                @Override
                public CharSequence getStrA(int col) {
                    switch (col) {
                        case TABLE_NAME:
                            return table.getTableName();
                        case PARTITION_BY_COLUMN:
                            return table.getPartitionByName();
                        case TTL_UNIT_COLUMN:
                            return getTtlUnit(table.getTtlHoursOrMonths());
                        case DESIGNATED_TIMESTAMP_COLUMN:
                            return table.getTimestampName();
                        case DIRECTORY_NAME_COLUMN:
                            if (table.isSoftLink()) {
                                if (lazyStringSink == null) {
                                    lazyStringSink = new StringSink();
                                }
                                lazyStringSink.clear();
                                lazyStringSink.put(table.getDirectoryName()).put(" (->)");
                                return lazyStringSink;
                            }
                            return table.getDirectoryName();
                        default:
                            return null;
                    }
                }

                @Override
                public CharSequence getStrB(int col) {
                    return getStrA(col);
                }

                @Override
                public int getStrLen(int col) {
                    return TableUtils.lengthOf(getStrA(col));
                }

                private void of(CairoTable table) {
                    this.table = table;
                }
            }
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("id", ColumnType.INT));
        metadata.add(new TableColumnMetadata("table_name", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("designatedTimestamp", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("partitionBy", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("maxUncommittedRows", ColumnType.INT));
        metadata.add(new TableColumnMetadata("o3MaxLag", ColumnType.LONG));
        metadata.add(new TableColumnMetadata("walEnabled", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("directoryName", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("dedup", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("ttlValue", ColumnType.INT));
        metadata.add(new TableColumnMetadata("ttlUnit", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("matView", ColumnType.BOOLEAN));
        METADATA = metadata;
    }
}
