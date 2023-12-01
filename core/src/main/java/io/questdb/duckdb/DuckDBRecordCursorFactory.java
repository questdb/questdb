/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.duckdb;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.vm.NullMemoryMR;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.std.Rows;
import io.questdb.std.Unsafe;

import static io.questdb.cairo.sql.DataFrameCursorFactory.ORDER_ANY;

public class DuckDBRecordCursorFactory implements RecordCursorFactory {
    private final DuckDBConnection connection;
    private final DuckDBPreparedStatement statement;
    private final RecordCursorImpl cursor;
    private final DuckDBPageFrameCursor pageFrameCursor;
    private final RecordMetadata metadata;

    public DuckDBRecordCursorFactory(DuckDBPreparedStatement statement, DuckDBConnection connection) {
        this.connection = connection;
        this.statement = statement;
        this.cursor = new RecordCursorImpl();
        this.pageFrameCursor = new DuckDBPageFrameCursor();
        this.metadata = buildMetadata(statement);
    }

    private static RecordMetadata buildMetadata(DuckDBPreparedStatement statement) {
        final int columnCount = (int) statement.getColumnCount();
        if (columnCount == 0) {
            return null;
        }

        GenericRecordMetadata metadata = new GenericRecordMetadata();
        for (int i = 0; i < columnCount; i++) {
            int type = statement.getColumnLogicalType(i);
            int questType = DuckDB.getQdbColumnType(type);
            if (questType == ColumnType.TIMESTAMP) {
                metadata.setTimestampIndex(i);
            }
            metadata.add(new TableColumnMetadata(statement.getColumnName(i).toString(), questType));
        }
        return metadata;
    }

    @Override
    public void close() {
        Misc.free(cursor);
        Misc.free(statement);
        Misc.free(connection);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        pageFrameCursor.of(statement);
        cursor.of(pageFrameCursor);
        return cursor;
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    public static class RecordCursorImpl implements RecordCursor {
        private final RecordImpl record = new RecordImpl();
        private PageFrameCursor pageFrameCursor;
        private PageFrame currentPageFrame;

        public void of(PageFrameCursor pageFrameCursor) {
            this.pageFrameCursor = pageFrameCursor;
            this.currentPageFrame = pageFrameCursor.next();
        }

        @Override
        public void close() {
            Misc.free(pageFrameCursor);
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public Record getRecordB() {
            return record;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return pageFrameCursor.getSymbolTable(columnIndex);
        }

        @Override
        public boolean hasNext() {
            if (currentPageFrame == null) {
                return false;
            }
            record.incrementPageRowIndex();
            if (record.getPageRowIndex() >= currentPageFrame.getPartitionHi()) {
                record.setPageRowIndex(-1);
                currentPageFrame = pageFrameCursor.next();
                return currentPageFrame != null && currentPageFrame.getPartitionHi() != 0;
            }
            return true;
        }

        @Override
        public void recordAt(Record record, long atRowId) {

        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        public void toTop() {
            pageFrameCursor.toTop();
            currentPageFrame = pageFrameCursor.next();
        }

        public class RecordImpl implements Record {
            private long pageRowIndex = -1;

            @Override
            public boolean getBool(int columnIndex) {
                final long address = currentPageFrame.getPageAddress(columnIndex);
                if (address == 0) {
                    return NullMemoryMR.INSTANCE.getBool(0);
                }
                return Unsafe.getUnsafe().getByte(address + pageRowIndex * Byte.BYTES) == 1;
            }

            @Override
            public byte getByte(int columnIndex) {
                final long address = currentPageFrame.getPageAddress(columnIndex);
                if (address == 0) {
                    return NullMemoryMR.INSTANCE.getByte(0);
                }
                return Unsafe.getUnsafe().getByte(address + pageRowIndex * Byte.BYTES);
            }

            @Override
            public char getChar(int columnIndex) {
                final long address = currentPageFrame.getPageAddress(columnIndex);
                if (address == 0) {
                    return NullMemoryMR.INSTANCE.getChar(0);
                }
                return Unsafe.getUnsafe().getChar(address + pageRowIndex * Character.BYTES);
            }

            @Override
            public double getDouble(int columnIndex) {
                final long address = currentPageFrame.getPageAddress(columnIndex);
                if (address == 0) {
                    return NullMemoryMR.INSTANCE.getDouble(0);
                }
                return Unsafe.getUnsafe().getDouble(address + pageRowIndex * Double.BYTES);
            }

            @Override
            public float getFloat(int columnIndex) {
                final long address = currentPageFrame.getPageAddress(columnIndex);
                if (address == 0) {
                    return NullMemoryMR.INSTANCE.getFloat(0);
                }
                return Unsafe.getUnsafe().getFloat(address + pageRowIndex * Float.BYTES);
            }

            @Override
            public int getInt(int columnIndex) {
                final long address = currentPageFrame.getPageAddress(columnIndex);
                if (address == 0) {
                    return NullMemoryMR.INSTANCE.getInt(0);
                }
                return Unsafe.getUnsafe().getInt(address + pageRowIndex * Integer.BYTES);
            }

            @Override
            public long getLong(int columnIndex) {
                final long address = currentPageFrame.getPageAddress(columnIndex);
                if (address == 0) {
                    return NullMemoryMR.INSTANCE.getLong(0);
                }
                return Unsafe.getUnsafe().getLong(address + pageRowIndex * Long.BYTES);
            }

            @Override
            public long getRowId() {
                return Rows.toRowID(currentPageFrame.getPartitionIndex(), pageRowIndex);
            }

            public long getPageRowIndex() {
                return pageRowIndex;
            }

            @Override
            public short getShort(int columnIndex) {
                final long address = currentPageFrame.getPageAddress(columnIndex);
                if (address == 0) {
                    return NullMemoryMR.INSTANCE.getShort(0);
                }
                return Unsafe.getUnsafe().getShort(address + pageRowIndex * Short.BYTES);
            }

            public void incrementPageRowIndex() {
                this.pageRowIndex++;
            }

            public void setPageRowIndex(long pageRowIndex) {
                this.pageRowIndex = pageRowIndex;
            }
        }
    }
}
