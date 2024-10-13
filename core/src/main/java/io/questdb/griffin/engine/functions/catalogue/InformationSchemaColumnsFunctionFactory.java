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
import io.questdb.griffin.engine.table.ShowColumnsRecordCursorFactory;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

import static io.questdb.griffin.engine.functions.catalogue.TablesFunctionFactory.TablesCursorFactory;
import static io.questdb.griffin.engine.table.ShowColumnsRecordCursorFactory.ShowColumnsCursor;

public class InformationSchemaColumnsFunctionFactory implements FunctionFactory {
    public static final RecordMetadata METADATA;
    public static final String SIGNATURE = "information_schema.columns()";

    @Override
    public String getSignature() {
        return SIGNATURE;
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
        return new CursorFunction(new ColumnsCursorFactory());
    }

    private static class ColumnsCursorFactory extends AbstractRecordCursorFactory {
        private final ColumnRecordCursor cursor = new ColumnRecordCursor();

        private ColumnsCursorFactory() {
            super(METADATA);
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            return cursor.of(executionContext);
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.type(SIGNATURE);
        }

        private static class ColumnRecordCursor implements NoRandomAccessRecordCursor {
            private final AllTablesFunctionFactory.AllTablesCursorFactory allTablesCursorFactory;
            private final ColumnsRecord record = new ColumnsRecord();
            private final ShowColumnsCursor showColumnsCursor = new ShowColumnsCursor();
            private RecordCursor allTablesCursor;
            private int columIdx;
            private SqlExecutionContext executionContext;
            private CharSequence tableName;

            private ColumnRecordCursor() {
                allTablesCursorFactory = new AllTablesFunctionFactory.AllTablesCursorFactory();
            }

            @Override
            public void close() {
                Misc.free(allTablesCursor);
                Misc.free(allTablesCursorFactory);
                Misc.free(showColumnsCursor);
                executionContext = null;
                tableName = null;
                columIdx = -1;
            }

            @Override
            public Record getRecord() {
                return record;
            }

            @Override
            public boolean hasNext() {
                boolean hasNext = false;
                if (columIdx == -1 && (hasNext = allTablesCursor.hasNext())) {
                    tableName = allTablesCursor.getRecord().getStrA(1);
                    showColumnsCursor.of(executionContext, tableName);
                }

                if (!hasNext && columIdx == -1) {
                    return false;
                }

                if (showColumnsCursor.hasNext()) {
                    Record rec = showColumnsCursor.getRecord();
                    CharSequence columnName = rec.getStrA(ShowColumnsRecordCursorFactory.N_NAME_COL);
                    CharSequence dataType = rec.getStrA(ShowColumnsRecordCursorFactory.N_TYPE_COL);
                    columIdx++;
                    record.of(tableName, columIdx, columnName, dataType);
                    return true;
                }
                columIdx = -1;
                Misc.free(showColumnsCursor);
                return hasNext();
            }

            @Override
            public long size() {
                return -1;
            }

            @Override
            public void toTop() {
                columIdx = -1;
                allTablesCursor.toTop();
            }

            private ColumnRecordCursor of(SqlExecutionContext sqlExecutionContext) {
                executionContext = sqlExecutionContext;
                allTablesCursor = allTablesCursorFactory.getCursor(executionContext);
                toTop();
                return this;
            }

            private static class ColumnsRecord implements Record {
                private CharSequence columnName;
                private CharSequence dataType;
                private int ordinalPosition;
                private CharSequence tableName;

                @Override
                public int getInt(int col) {
                    return ordinalPosition;
                }

                @Override
                public CharSequence getStrA(int col) {
                    switch (col) {
                        case 0:
                            return tableName;
                        case 2:
                            return columnName;
                        case 3:
                            return dataType;
                    }
                    return null;
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

                private void of(CharSequence tableName, int ordinalPosition, CharSequence columnName, CharSequence dataType) {
                    this.tableName = tableName;
                    this.ordinalPosition = ordinalPosition;
                    this.columnName = columnName;
                    this.dataType = dataType;
                }
            }
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(TablesCursorFactory.TABLE_NAME_COLUMN_META);
        metadata.add(new TableColumnMetadata("ordinal_position", ColumnType.INT));
        metadata.add(new TableColumnMetadata("column_name", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("data_type", ColumnType.STRING));
        METADATA = metadata;
    }
}
