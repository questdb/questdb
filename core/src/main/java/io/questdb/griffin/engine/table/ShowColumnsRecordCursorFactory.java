/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
package io.questdb.griffin.engine.table;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlExecutionContext;

public class ShowColumnsRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final RecordMetadata METADATA;
    private static final int N_NAME_COL = 0;
    private static final int N_TYPE_COL = 1;
    private static final int N_INDEXED_COL = 2;
    private static final int N_INDEX_BLOCK_CAPACITY_COL = 3;
    private static final int N_SYMBOL_CACHED_COL = 4;
    private static final int N_SYMBOL_CAPACITY_COL = 5;
    private static final int N_DESIGNATED_COL = 6;
    private final ShowColumnsCursor cursor = new ShowColumnsCursor();
    private final CharSequence tableName;
    public ShowColumnsRecordCursorFactory(CharSequence tableName) {
        super(METADATA);
        this.tableName = tableName.toString();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        return cursor.of(executionContext);
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    private class ShowColumnsCursor implements RecordCursor {
        private final ShowColumnsRecord record = new ShowColumnsRecord();
        private TableReader reader;
        private int columnIndex;

        @Override
        public void close() {
            if (null != reader) {
                reader.close();
                reader = null;
            }
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            columnIndex++;
            if (columnIndex < reader.getMetadata().getColumnCount()) {
                return true;
            }
            columnIndex--;
            return false;
        }

        @Override
        public Record getRecordB() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void toTop() {
            columnIndex = -1;
        }

        @Override
        public long size() {
            return -1;
        }

        private ShowColumnsCursor of(SqlExecutionContext executionContext) {
            reader = executionContext.getCairoEngine().getReader(executionContext.getCairoSecurityContext(), tableName);
            toTop();
            return this;
        }

        public class ShowColumnsRecord implements Record {
            @Override
            public boolean getBool(int col) {
                if (col == N_INDEXED_COL) {
                    return reader.getMetadata().isColumnIndexed(columnIndex);
                }
                if (col == N_SYMBOL_CACHED_COL) {
                    if (ColumnType.isSymbol(reader.getMetadata().getColumnType(columnIndex))) {
                        return reader.getSymbolMapReader(columnIndex).isCached();
                    } else {
                        return false;
                    }
                }
                if (col == N_DESIGNATED_COL) {
                    return reader.getMetadata().getTimestampIndex() == columnIndex;
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public int getInt(int col) {
                if (col == N_INDEX_BLOCK_CAPACITY_COL) {
                    return reader.getMetadata().getIndexValueBlockCapacity(columnIndex);
                }
                if (col == N_SYMBOL_CAPACITY_COL) {
                    if (ColumnType.isSymbol(reader.getMetadata().getColumnType(columnIndex))) {
                        return reader.getSymbolMapReader(columnIndex).getSymbolCapacity();
                    } else {
                        return 0;
                    }
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public CharSequence getStr(int col) {
                if (col == N_NAME_COL) {
                    return reader.getMetadata().getColumnName(columnIndex);
                }
                if (col == N_TYPE_COL) {
                    return ColumnType.nameOf(reader.getMetadata().getColumnType(columnIndex));
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public CharSequence getStrB(int col) {
                return getStr(col);
            }

            @Override
            public int getStrLen(int col) {
                return getStr(col).length();
            }
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("column", 1, ColumnType.STRING));
        metadata.add(new TableColumnMetadata("type", 2, ColumnType.STRING));
        metadata.add(new TableColumnMetadata("indexed", 3, ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("indexBlockCapacity", 4, ColumnType.INT));
        metadata.add(new TableColumnMetadata("symbolCached", 5, ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("symbolCapacity", 6, ColumnType.INT));
        metadata.add(new TableColumnMetadata("designated", 7, ColumnType.BOOLEAN));
        METADATA = metadata;
    }
}
