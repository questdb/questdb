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

import io.questdb.cairo.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.vm.NullMemoryMR;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.*;
import io.questdb.std.str.CharSinkBase;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.DirectUtf8StringZ;
import org.jetbrains.annotations.Nullable;

public class DuckDBRecordCursorFactory implements RecordCursorFactory {
    private final long statement;
    private final RecordCursorImpl cursor;
    private final DuckDBPageFrameCursor pageFrameCursor;
    private final RecordMetadata metadata;

    // used for a quack function
    public DuckDBRecordCursorFactory(long stmt) {
        this(true, stmt, null);
    }

    // used for a hybrid tables
    public DuckDBRecordCursorFactory(long stmt, TableReader reader) {
        this(false, stmt, reader);
    }

    public DuckDBRecordCursorFactory(boolean duckNative, long stmt, @Nullable TableReader reader) {
        this.metadata = (reader == null) ? buildMetadata(stmt) : validateMetadata(stmt, reader.getMetadata());
        this.statement = stmt;
        this.cursor = new RecordCursorImpl(duckNative, reader);
        this.pageFrameCursor = new DuckDBPageFrameCursor();
    }

    public static RecordMetadata buildMetadata(long statement) {
        final int columnCount = (int)DuckDB.preparedGetColumnCount(statement);
        if (columnCount == 0) {
            return null;
        }

        DirectUtf8StringZ utf8String = new DirectUtf8StringZ();
        GenericRecordMetadata metadata = new GenericRecordMetadata();
        for (int i = 0; i < columnCount; i++) {
            int type = DuckDB.decodeLogicalTypeId(DuckDB.preparedGetColumnTypes(statement, i));
            int questType = DuckDB.getQdbColumnType(type);
            if (questType == ColumnType.TIMESTAMP) {
                metadata.setTimestampIndex(i);
            }
            long name = DuckDB.preparedGetColumnName(statement, i);
            metadata.add(new TableColumnMetadata(utf8String.of(name).toString(), questType));
        }

        return metadata;
    }

    public static RecordMetadata validateMetadata(long statement, RecordMetadata metadata) {
        if (checkAllTypesMatched(statement, metadata)) {
            return metadata;
        }
        return null; // TODO: throw exception
    }

    private static boolean checkAllTypesMatched(long statement, RecordMetadata metadata) {
        final int columnCount = (int)DuckDB.preparedGetColumnCount(statement);
        if (columnCount == 0 || metadata == null || metadata.getColumnCount() > columnCount) {
            return false;
        }

        for (int i = 0, columns = metadata.getColumnCount(); i < columns; i++) {
            int duckLogicalTypeId = DuckDB.decodeLogicalTypeId(DuckDB.preparedGetColumnTypes(statement, i));
            int questType = ColumnType.tagOf(metadata.getColumnType(i));
            if (!isTypeMatched(questType, duckLogicalTypeId)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isTypeMatched(int questType, int duckType) {
        switch (questType) {
            case ColumnType.BOOLEAN:
            case ColumnType.BYTE:
            case ColumnType.GEOBYTE:
                return duckType == DuckDB.COLUMN_TYPE_TINYINT;
            case ColumnType.CHAR:
            case ColumnType.SHORT:
            case ColumnType.GEOSHORT:
                return duckType == DuckDB.COLUMN_TYPE_SMALLINT;
            case ColumnType.INT:
            case ColumnType.GEOINT:
            case ColumnType.IPv4:
            case ColumnType.SYMBOL:
                return duckType == DuckDB.COLUMN_TYPE_INTEGER;
            case ColumnType.LONG:
            case ColumnType.DATE:
            case ColumnType.TIMESTAMP:
            case ColumnType.GEOLONG:
                return duckType == DuckDB.COLUMN_TYPE_BIGINT;
            case ColumnType.LONG128:
            case ColumnType.UUID:
                return duckType == DuckDB.COLUMN_TYPE_UUID;
            case ColumnType.FLOAT:
                return duckType == DuckDB.COLUMN_TYPE_FLOAT;
            case ColumnType.DOUBLE:
                return duckType == DuckDB.COLUMN_TYPE_DOUBLE;
            case ColumnType.STRING:
                return duckType == DuckDB.COLUMN_TYPE_VARCHAR;
            case ColumnType.BINARY:
            case ColumnType.LONG256:
                return duckType == DuckDB.COLUMN_TYPE_BLOB;
            default:
                return false;
        }
    }

    @Override
    public void close() {
        Misc.free(cursor);
        Misc.free(pageFrameCursor);
        DuckDB.preparedDestroy(statement);
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
        private final AbstractRecord record;
        private final TableReader reader;
        private DuckDBPageFrameCursor pageFrameCursor;
        private DuckDBPageFrameCursor.PageFrameImpl currentPageFrame;

        public RecordCursorImpl(boolean duckNative, @Nullable TableReader reader) {
            this.reader = reader;
            if (duckNative) {
                this.record = new DuckNativeRecord();
            } else {
                this.record = new QuestNativeRecord();
            }
        }

        public void of(DuckDBPageFrameCursor pageFrameCursor) {
            assert pageFrameCursor != null;
            this.pageFrameCursor = pageFrameCursor;
            this.currentPageFrame = pageFrameCursor.next();
            this.record.setPageRowIndex(-1);
        }

        @Override
        public void close() {
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public Record getRecordB() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            if (reader == null) {
                return pageFrameCursor.getSymbolTable(columnIndex);
            } else {
                return reader.getSymbolTable(columnIndex);
            }
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            if (reader == null) {
                return pageFrameCursor.newSymbolTable(columnIndex);
            } else {
                return reader.newSymbolTable(columnIndex);
            }
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
            throw new UnsupportedOperationException();
        }

        @Override
        public long size() {
            return -1L;
        }

        @Override
        public void toTop() {
            pageFrameCursor.toTop(); // reevaluate query !!!
            currentPageFrame = pageFrameCursor.next();
        }

        public abstract class AbstractRecord implements Record {
            protected long pageRowIndex = -1;

            @Override
            public long getRowId() {
                return Rows.toRowID(currentPageFrame.getPartitionIndex(), pageRowIndex);
            }

            public long getPageRowIndex() {
                return pageRowIndex;
            }

            public void incrementPageRowIndex() {
                this.pageRowIndex++;
            }

            public void setPageRowIndex(long pageRowIndex) {
                this.pageRowIndex = pageRowIndex;
            }
        }

        public class DuckNativeRecord extends AbstractRecord {
            @Override
            public boolean getBool(int columnIndex) {
                final long address = currentPageFrame.getPageAddress(columnIndex);
                if (address == 0) {
                    return NullMemoryMR.INSTANCE.getBool(0);
                }
                long validityAddress = currentPageFrame.getValidityMaskAddress(columnIndex);
                if (!DuckDB.validityRowIsValid(validityAddress, pageRowIndex)) {
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
                long validityAddress = currentPageFrame.getValidityMaskAddress(columnIndex);
                if (!DuckDB.validityRowIsValid(validityAddress, pageRowIndex)) {
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
                long validityAddress = currentPageFrame.getValidityMaskAddress(columnIndex);
                if (!DuckDB.validityRowIsValid(validityAddress, pageRowIndex)) {
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
                long validityAddress = currentPageFrame.getValidityMaskAddress(columnIndex);
                if (!DuckDB.validityRowIsValid(validityAddress, pageRowIndex)) {
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
                long validityAddress = currentPageFrame.getValidityMaskAddress(columnIndex);
                if (!DuckDB.validityRowIsValid(validityAddress, pageRowIndex)) {
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
                long validityAddress = currentPageFrame.getValidityMaskAddress(columnIndex);
                if (!DuckDB.validityRowIsValid(validityAddress, pageRowIndex)) {
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
                long validityAddress = currentPageFrame.getValidityMaskAddress(columnIndex);
                if (!DuckDB.validityRowIsValid(validityAddress, pageRowIndex)) {
                    return NullMemoryMR.INSTANCE.getLong(0);
                }
                return Unsafe.getUnsafe().getLong(address + pageRowIndex * Long.BYTES);
            }

            @Override
            public short getShort(int columnIndex) {
                final long address = currentPageFrame.getPageAddress(columnIndex);
                if (address == 0) {
                    return NullMemoryMR.INSTANCE.getShort(0);
                }
                long validityAddress = currentPageFrame.getValidityMaskAddress(columnIndex);
                if (!DuckDB.validityRowIsValid(validityAddress, pageRowIndex)) {
                    return NullMemoryMR.INSTANCE.getShort(0);
                }
                return Unsafe.getUnsafe().getShort(address + pageRowIndex * Short.BYTES);
            }
        }

        public class QuestNativeRecord extends AbstractRecord {
            private final DirectUtf8String utf8String = new DirectUtf8String();
            private final DirectBinarySequence binarySequence = new DirectBinarySequence();
            private final Long256Impl long256A = new Long256Impl();
            private final Long256Impl long256B = new Long256Impl();

            @Override
            public boolean getBool(int columnIndex) {
                final long address = currentPageFrame.getPageAddress(columnIndex);
                if (address == 0) {
                    return false;
                }
                return Unsafe.getUnsafe().getByte(address + pageRowIndex * Byte.BYTES) == 1;
            }

            @Override
            public byte getByte(int columnIndex) {
                final long address = currentPageFrame.getPageAddress(columnIndex);
                if (address == 0) {
                    return 0;
                }
                return Unsafe.getUnsafe().getByte(address + pageRowIndex * Byte.BYTES);
            }

            @Override
            public char getChar(int columnIndex) {
                final long address = currentPageFrame.getPageAddress(columnIndex);
                if (address == 0) {
                    return 0;
                }
                return Unsafe.getUnsafe().getChar(address + pageRowIndex * Character.BYTES);
            }

            @Override
            public double getDouble(int columnIndex) {
                final long address = currentPageFrame.getPageAddress(columnIndex);
                if (address == 0) {
                    return Double.NaN;
                }
                return Unsafe.getUnsafe().getDouble(address + pageRowIndex * Double.BYTES);
            }

            @Override
            public float getFloat(int columnIndex) {
                final long address = currentPageFrame.getPageAddress(columnIndex);
                if (address == 0) {
                    return Float.NaN;
                }
                return Unsafe.getUnsafe().getFloat(address + pageRowIndex * Float.BYTES);
            }

            @Override
            public int getInt(int columnIndex) {
                final long address = currentPageFrame.getPageAddress(columnIndex);
                if (address == 0) {
                    return Numbers.INT_NaN;
                }
                return Unsafe.getUnsafe().getInt(address + pageRowIndex * Integer.BYTES);
            }

            @Override
            public long getLong(int columnIndex) {
                final long address = currentPageFrame.getPageAddress(columnIndex);
                if (address == 0) {
                    return Numbers.LONG_NaN;
                }
                return Unsafe.getUnsafe().getLong(address + pageRowIndex * Long.BYTES);
            }

            @Override
            public short getShort(int columnIndex) {
                final long address = currentPageFrame.getPageAddress(columnIndex);
                if (address == 0) {
                    return 0;
                }
                return Unsafe.getUnsafe().getShort(address + pageRowIndex * Short.BYTES);
            }

            @Override
            public byte getGeoByte(int columnIndex) {
                final long address = currentPageFrame.getPageAddress(columnIndex);
                if (address == 0) {
                    return GeoHashes.BYTE_NULL;
                }
                return Unsafe.getUnsafe().getByte(address + pageRowIndex * Byte.BYTES);
            }

            @Override
            public int getGeoInt(int columnIndex) {
                final long address = currentPageFrame.getPageAddress(columnIndex);
                if (address == 0) {
                    return GeoHashes.INT_NULL;
                }
                return Unsafe.getUnsafe().getInt(address + pageRowIndex * Integer.BYTES);
            }

            @Override
            public long getGeoLong(int columnIndex) {
                final long address = currentPageFrame.getPageAddress(columnIndex);
                if (address == 0) {
                    return GeoHashes.NULL;
                }
                return Unsafe.getUnsafe().getLong(address + pageRowIndex * Long.BYTES);
            }

            @Override
            public short getGeoShort(int columnIndex) {
                final long address = currentPageFrame.getPageAddress(columnIndex);
                if (address == 0) {
                    return GeoHashes.SHORT_NULL;
                }
                return Unsafe.getUnsafe().getShort(address + pageRowIndex * Short.BYTES);
            }

            @Override
            public int getIPv4(int columnIndex) {
                final long address = currentPageFrame.getPageAddress(columnIndex);
                if (address == 0) {
                    return Numbers.IPv4_NULL;
                }
                return Unsafe.getUnsafe().getInt(address + pageRowIndex * Integer.BYTES);
            }

            @Override
            public long getLong128Hi(int columnIndex) {
                final long address = currentPageFrame.getPageAddress(columnIndex);
                if (address == 0) {
                    return Numbers.LONG_NaN;
                }
                return Unsafe.getUnsafe().getLong(address + pageRowIndex * Long128.BYTES + Long.BYTES);
            }

            @Override
            public long getLong128Lo(int columnIndex) {
                final long address = currentPageFrame.getPageAddress(columnIndex);
                if (address == 0) {
                    return Numbers.LONG_NaN;
                }
                return Unsafe.getUnsafe().getLong(address + pageRowIndex * Long128.BYTES);
            }

            @Override
            public void getLong256(int columnIndex, CharSinkBase<?> sink) {
                Long256 long256 = getLong256A(columnIndex);
                Numbers.appendLong256(long256.getLong0(), long256.getLong1(), long256.getLong2(), long256.getLong3(), sink);
            }

            private void updateLong256(int columnIndex, Long256 long256) {
                final long baseAddress = currentPageFrame.getPageAddress(columnIndex);
                if (baseAddress == 0) {
                    long256.setAll(Numbers.LONG_NaN, Numbers.LONG_NaN, Numbers.LONG_NaN, Numbers.LONG_NaN);
                } else {
                    long recordAddress = baseAddress + pageRowIndex * 16; // sizeof(value)
                    int length = Unsafe.getUnsafe().getInt(recordAddress);
                    assert length == 32;
                    long address = Unsafe.getUnsafe().getLong(recordAddress + 8);
                    long256.setAll(
                            Unsafe.getUnsafe().getLong(address),
                            Unsafe.getUnsafe().getLong(address + Long.BYTES),
                            Unsafe.getUnsafe().getLong(address + Long.BYTES * 2),
                            Unsafe.getUnsafe().getLong(address + Long.BYTES * 3)
                    );
                }
            }

            @Override
            public Long256 getLong256A(int columnIndex) {
                updateLong256(columnIndex, long256A);
                return long256A;
            }

            @Override
            public Long256 getLong256B(int columnIndex) {
                updateLong256(columnIndex, long256B);
                return long256B;
            }

            @Override
            public CharSequence getStr(int columnIndex) { // TODO: null handling
                //	union {
                //		struct {
                //			uint32_t length;
                //			char prefix[4];
                //			char *ptr;
                //		} pointer;
                //		struct {
                //			uint32_t length;
                //			char inlined[12];
                //		} inlined;
                //	} value;
                final long address = currentPageFrame.getPageAddress(columnIndex);
                if (address == 0) {
                    return null;
                }
                long recordAddress = address + pageRowIndex * 16; // sizeof(value)
                int length = Unsafe.getUnsafe().getInt(recordAddress);
                long dataAddress = getVarlenAddress(recordAddress, length);
                utf8String.of(dataAddress, dataAddress + length);
                return utf8String.toString(); // Convert for now
            }

            @Override
            public CharSequence getStrB(int columnIndex) {
               return getStr(columnIndex);
            }

            @Override
            public int getStrLen(int columnIndex) {
                final long address = currentPageFrame.getPageAddress(columnIndex);
                if (address == 0) {
                    return 0;
                }
                long recordAddress = address + pageRowIndex * 16; // sizeof(value)
                return Unsafe.getUnsafe().getInt(recordAddress);
            }

            @Override
            public BinarySequence getBin(int columnIndex) {
                final long address = currentPageFrame.getPageAddress(columnIndex);
                if (address == 0) {
                    return null;
                }
                long recordAddress = address + pageRowIndex * 16; // sizeof(value)
                int length = Unsafe.getUnsafe().getInt(recordAddress);
                long dataAddress = getVarlenAddress(recordAddress, length);
                binarySequence.of(dataAddress, length);
                return binarySequence;
            }

            private long getVarlenAddress(long recordAddress, int length) {
                if (length <= 12) {
                    return recordAddress + Integer.BYTES;
                } else {
                    return Unsafe.getUnsafe().getLong(recordAddress + 8);
                }
            }

            @Override
            public long getBinLen(int columnIndex) {
                return getStrLen(columnIndex);
            }

            @Override
            public CharSequence getSym(int columnIndex) {
                final int index = getInt(columnIndex);
                if (index == Numbers.INT_NaN || reader == null) {
                    return null;
                }
                return reader.getSymbolMapReader(columnIndex).valueOf(index);
            }

            @Override
            public CharSequence getSymB(int columnIndex) {
                final int index = getInt(columnIndex);
                if (index == Numbers.INT_NaN || reader == null) {
                    return null;
                }
                return reader.getSymbolMapReader(columnIndex).valueBOf(index);
            }

        }
    }
}
