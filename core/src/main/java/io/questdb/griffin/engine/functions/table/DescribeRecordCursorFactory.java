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

package io.questdb.griffin.engine.functions.table;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlUtil;
import org.jetbrains.annotations.NotNull;

public class DescribeRecordCursorFactory extends AbstractRecordCursorFactory {
    public static final String SIGNATURE = "describe()";
    private static final RecordMetadata METADATA;
    private final RecordMetadata childMetadata;
    private final DescribeRecordCursor cursor = new DescribeRecordCursor();

    DescribeRecordCursorFactory(@NotNull RecordMetadata childMetadata) {
        super(METADATA);
        this.childMetadata = childMetadata;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        cursor.of(childMetadata);
        cursor.toTop();
        return cursor;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type(SIGNATURE);
    }

    private static class DescribeRecordCursor implements NoRandomAccessRecordCursor {
        final DescribeRecord record = new DescribeRecord();
        RecordMetadata childMetadata;
        int pos;

        @Override
        public void close() {
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            if (columnIndex == 2) {
                return SqlUtil.ColumnTypeSymbolTable.INSTANCE;
            } else {
                throw new UnsupportedOperationException();
            }
        }

        @Override
        public boolean hasNext() {
            if (childMetadata.hasColumn(++pos)) {
                record.of(pos, childMetadata.getColumnName(pos), childMetadata.getColumnType(pos));
                return true;
            }
            return false;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return new SqlUtil.ColumnTypeSymbolTable();
        }

        public void of(@NotNull RecordMetadata childMetadata) {
            this.childMetadata = childMetadata;
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public long size() {
            return childMetadata.getColumnCount();
        }

        @Override
        public void toTop() {
            pos = -1;
        }

        private static class DescribeRecord implements Record {
            private CharSequence name;
            private int position;
            private int type;

            @Override
            public int getInt(int col) {
                return switch (col) {
                    case 2 -> type;
                    case 0 -> position;
                    default -> throw new UnsupportedOperationException();
                };
            }

            @Override
            public long getRowId() {
                return position;
            }

            @Override
            public CharSequence getStrA(int col) {
                if (col == 1) {
                    return name;
                }
                throw new UnsupportedOperationException();
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
            public CharSequence getSymA(int col) {
                if (col == 2) {
                    return SqlUtil.ColumnTypeSymbolTable.INSTANCE.valueOf(type);
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public CharSequence getSymB(int col) {
                return getSymA(col);
            }

            private void of(int position, CharSequence name, int type) {
                this.position = position;
                this.name = name;
                this.type = type;
            }
        }
    }

    static {
        GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("ordinal_position", ColumnType.INT));
        metadata.add(new TableColumnMetadata("column_name", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("data_type", ColumnType.SYMBOL, null, true, true, ColumnType.NULL));
        METADATA = metadata;
    }
}
