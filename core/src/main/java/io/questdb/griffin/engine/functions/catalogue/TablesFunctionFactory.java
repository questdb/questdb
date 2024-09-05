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

import io.questdb.cairo.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.IntList;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;

public class TablesFunctionFactory implements FunctionFactory {
    private static final int DEDUP_NAME_COLUMN = 8;
    private static final int DESIGNATED_TIMESTAMP_COLUMN = 2;
    private static final int DIRECTORY_NAME_COLUMN = 7;
    private static final int ID_COLUMN = 0;
    private static final int MAX_UNCOMMITTED_ROWS_COLUMN = 4;
    private static final int O3_MAX_LAG_COLUMN = 5;
    private static final RecordMetadata METADATA;
    private static final int PARTITION_BY_COLUMN = 3;
    private static final int TABLE_NAME = 1;
    private static final int WAL_ENABLED_COLUMN = 6;

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
        public static final String TABLE_NAME_COLUMN_NAME = "table_name";
        public static final TableColumnMetadata TABLE_NAME_COLUMN_META = new TableColumnMetadata(TABLE_NAME_COLUMN_NAME, ColumnType.STRING);
        private final TablesRecordCursor cursor = new TablesRecordCursor();

        public TablesCursorFactory() {
            super(METADATA);
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            cursor.of(executionContext);
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
            private final ObjHashSet<TableToken> tableBucket = new ObjHashSet<>();
            private SqlExecutionContext executionContext;
            private int tableIndex = -1;

            @Override
            public void close() {
                tableIndex = -1;
            }

            @Override
            public Record getRecord() {
                return record;
            }

            @Override
            public boolean hasNext() {
                tableIndex++;
                int n = tableBucket.size();
                for (; tableIndex < n; tableIndex++) {
                    if (record.open(tableBucket.get(tableIndex))) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public long size() {
                return -1;
            }

            @Override
            public void toTop() {
                tableIndex = -1;
            }

            private void of(SqlExecutionContext executionContext) {
                this.executionContext = executionContext;
                executionContext.getCairoEngine().getTableTokens(tableBucket, false);
                toTop();
            }

            private class TableListRecord implements Record {
                private StringSink lazyStringSink = null;
                private CairoTable table;

                @Override
                public boolean getBool(int col) {
                    switch (col) {
                        case WAL_ENABLED_COLUMN:
                            return table.getWalEnabled();
                        case DEDUP_NAME_COLUMN:
                            return table.getIsDedup();
                        default:
                            return false;
                    }
                }

                @Override
                public int getInt(int col) {
                    if (col == ID_COLUMN) {
                        return table.getId();
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
                        case DESIGNATED_TIMESTAMP_COLUMN:
                            return table.getTimestampName();
                        case DIRECTORY_NAME_COLUMN:
                            if (table.getIsSoftLink()) {
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
                    CharSequence str = getStrA(col);
                    return str != null ? str.length() : -1;
                }

                private boolean open(TableToken tableToken) {
                    table = executionContext.getCairoEngine().metadataCacheGetVisibleTable(tableToken);
                    return table != null;
                }
            }
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("id", ColumnType.INT));
        metadata.add(TablesCursorFactory.TABLE_NAME_COLUMN_META);
        metadata.add(new TableColumnMetadata("designatedTimestamp", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("partitionBy", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("maxUncommittedRows", ColumnType.INT));
        metadata.add(new TableColumnMetadata("o3MaxLag", ColumnType.LONG));
        metadata.add(new TableColumnMetadata("walEnabled", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("directoryName", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("dedup", ColumnType.BOOLEAN));
        METADATA = metadata;
    }
}
