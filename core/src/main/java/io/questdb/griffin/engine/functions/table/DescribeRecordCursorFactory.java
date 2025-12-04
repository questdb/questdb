/// *******************************************************************************
/// *     ___                  _   ____  ____
/// *    / _ \ _   _  ___  ___| |_|  _ \| __ )
/// *   | | | | | | |/ _ \/ __| __| | | |  _ \
/// *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
/// *    \__\_\\__,_|\___||___/\__|____/|____/
/// *
/// *  Copyright (c) 2014-2019 Appsicle
/// *  Copyright (c) 2019-2024 QuestDB
/// *
/// *  Licensed under the Apache License, Version 2.0 (the "License");
/// *  you may not use this file except in compliance with the License.
/// *  You may obtain a copy of the License at
/// *
/// *  http://www.apache.org/licenses/LICENSE-2.0
/// *
/// *  Unless required by applicable law or agreed to in writing, software
/// *  distributed under the License is distributed on an "AS IS" BASIS,
/// *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// *  See the License for the specific language governing permissions and
/// *  limitations under the License.
/// *
/// ******************************************************************************/
//
//package io.questdb.griffin.engine.functions.table;
//
//import io.questdb.cairo.AbstractRecordCursorFactory;
//import io.questdb.cairo.CairoColumn;
//import io.questdb.cairo.CairoEngine;
//import io.questdb.cairo.CairoTable;
//import io.questdb.cairo.ColumnType;
//import io.questdb.cairo.GenericRecordMetadata;
//import io.questdb.cairo.MetadataCacheReader;
//import io.questdb.cairo.TableColumnMetadata;
//import io.questdb.cairo.TableUtils;
//import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
//import io.questdb.cairo.sql.Record;
//import io.questdb.cairo.sql.RecordCursor;
//import io.questdb.cairo.sql.RecordMetadata;
//import io.questdb.griffin.PlanSink;
//import io.questdb.griffin.SqlExecutionContext;
//import io.questdb.griffin.engine.functions.catalogue.Constants;
//import io.questdb.griffin.engine.functions.catalogue.InformationSchemaColumnsFunctionFactory;
//import io.questdb.griffin.engine.functions.catalogue.InformationSchemaQuestDBColumnsFunctionFactory;
//import io.questdb.std.CharSequenceObjHashMap;
//
//import javax.management.Descriptor;
//import java.util.function.IntFunction;
//
//public class DescribeRecordCursorFactory extends AbstractRecordCursorFactory{
//    static RecordMetadata METADATA = InformationSchemaColumnsFunctionFactory.METADATA;
//    public static final String SIGNATURE = "describe()";
//    DescribeRecordCursor cursor = new DescribeRecordCursor();
//    final RecordMetadata childMetadata;
//
//    DescribeRecordCursorFactory(RecordMetadata childMetadata, CharSequence tableName) {
//            super(METADATA);
//            this.childMetadata = childMetadata;
//        }
//
//        @Override
//        public RecordCursor getCursor(SqlExecutionContext executionContext) {
//            cursor.toTop();
//            return cursor;
//        }
//
//        @Override
//        public boolean recordCursorSupportsRandomAccess() {
//            return false;
//        }
//
//        @Override
//        public void toPlan(PlanSink sink) {
//            sink.type(SIGNATURE);
//        }
//
//        private static class DescribeRecordCursor implements NoRandomAccessRecordCursor {
//            int pos;
//            ColumnsRecord record;
//
//            public DescribeRecordCursor() {};
//
//            @Override
//            public void close() {
//                pos = -1;
//            }
//
//            @Override
//            public io.questdb.cairo.sql.Record getRecord() {
//                return record;
//            }
//
//            @Override
//            public boolean hasNext() {
//                do {
//                    if (table == null && !nextTable()) {
//                        return false;
//                    }
//                    // we have a table
//                    if (columnIdx < table.getColumnCount() - 1) {
//                        columnIdx++;
//                        CairoColumn column = table.getColumnQuiet(columnIdx);
//                        record.of(table.getTableName(), columnIdx, column.getName(), typeToName.apply(column.getType()));
//                        return true;
//                    } else {
//                        columnIdx = -1;
//                        table = null;
//                    }
//                } while (true);
//
//                record.of(ta)
//            }
//
//            @Override
//            public long preComputedStateSize() {
//                return pos;
//            }
//
//            @Override
//            public long size() {
//                childMetadata.getColumnCount();
//            }
//
//            @Override
//            public void toTop() {
//                pos = -1;
//            }
//
//            private static class ColumnsRecord implements Record {
//                private CharSequence columnName;
//                private CharSequence dataType;
//                private int ordinalPosition;
//                private CharSequence tableName;
//
//                @Override
//                public int getInt(int col) {
//                    return ordinalPosition;
//                }
//
//                @Override
//                public CharSequence getStrA(int col) {
//                    switch (col) {
//                        case 0:
//                            // table_catalog
//                            return Constants.DB_NAME;
//                        case 1:
//                            // table_schema
//                            return Constants.PUBLIC_SCHEMA;
//                        case 2:
//                            return tableName;
//                        case 3:
//                            return columnName;
//                        case 5:
//                            // column_default
//                            return null;
//                        case 6:
//                            // is_nullable
//                            return "yes";
//                        case 7:
//                            return dataType;
//                    }
//                    return null;
//                }
//
//                @Override
//                public CharSequence getStrB(int col) {
//                    return getStrA(col);
//                }
//
//                @Override
//                public int getStrLen(int col) {
//                    return TableUtils.lengthOf(getStrA(col));
//                }
//
//                private void of(CharSequence tableName, int ordinalPosition, CharSequence columnName, CharSequence dataType) {
//                    this.tableName = tableName;
//                    this.ordinalPosition = ordinalPosition;
//                    this.columnName = columnName;
//                    this.dataType = dataType;
//                }
//            }
//        }
//
//    static {
//            GenericRecordMetadata metadata = new GenericRecordMetadata();
//            metadata.add(new TableColumnMetadata("name", ColumnType.VARCHAR));
//            metadata.add(new TableColumnMetadata("type", ColumnType.VARCHAR));
//    }
//}
//
//
