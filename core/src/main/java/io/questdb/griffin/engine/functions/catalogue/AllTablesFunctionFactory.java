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

public class AllTablesFunctionFactory implements FunctionFactory {
    private static final RecordMetadata METADATA;

    @Override
    public String getSignature() {
        return "all_tables()";
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
        return new CursorFunction(new AllTablesCursorFactory()) {
            @Override
            public boolean isRuntimeConstant() {
                return true;
            }
        };
    }

    public static class AllTablesCursorFactory extends AbstractRecordCursorFactory {
        public static final Log LOG = LogFactory.getLog(AllTablesCursorFactory.class);
        public static final String TABLE_NAME_COLUMN_NAME = "table_name";
        public static final TableColumnMetadata TABLE_NAME_COLUMN_META = new TableColumnMetadata(TABLE_NAME_COLUMN_NAME, ColumnType.STRING);
        private final AllTablesRecordCursor cursor = new AllTablesRecordCursor();

        public AllTablesCursorFactory() {
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
            sink.type("all_tables()");
        }

        @Override
        protected void _close() {
            cursor.close();
        }

        private static class AllTablesRecordCursor implements NoRandomAccessRecordCursor {
            private final AllTablesRecord record = new AllTablesRecord();
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

            private class AllTablesRecord implements Record {
                private CairoTable table;

                @Override
                public CharSequence getStrA(int col) {
                    return table.getTableName();
                }

                @Override
                public CharSequence getStrB(int col) {
                    return getStrA(col);
                }

                @Override
                public int getStrLen(int col) {
                    return getStrA(col).length();
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
        metadata.add(AllTablesCursorFactory.TABLE_NAME_COLUMN_META);
        METADATA = metadata;
    }
}
