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

package io.questdb.griffin.engine.table.parquet;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.std.Chars;
import io.questdb.std.DirectIntList;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import io.questdb.std.Os;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.DirectString;

public class PartitionDecoder implements QuietCloseable {
    private static final long COLUMNS_PTR_OFFSET;
    private static final long COLUMN_COUNT_OFFSET;
    private final static long COLUMN_IDS_OFFSET;
    private static final long COLUMN_RECORD_NAME_PTR_OFFSET;
    private static final long COLUMN_RECORD_NAME_SIZE_OFFSET;
    private static final long COLUMN_RECORD_TYPE_OFFSET;
    private static final long COLUMN_STRUCT_SIZE;
    private static final long ROW_COUNT_OFFSET;
    private static final long ROW_GROUP_COUNT_OFFSET;
    private static final long ROW_GROUP_SIZES_PTR_OFFSET;
    private final ObjectPool<DirectString> directStringPool = new ObjectPool<>(DirectString::new, 16);
    private final Metadata metadata = new Metadata();
    private long columnsPtr;
    private long fileAddr; // mmapped parquet file's address
    private long fileSize; // mmapped parquet file's size
    private long ptr;
    private long rowGroupSizesPtr;

    public static boolean decodeNoNeedToDecodeFlag(long encodedIndex) {
        return (encodedIndex & 1) == 1;
    }

    public static int decodeRowGroupIndex(long encodedIndex) {
        return (int) ((encodedIndex >> 1) - 1);
    }

    @Override
    public void close() {
        destroy();
        fileAddr = 0;
        fileSize = 0;
    }

    public int decodeRowGroup(
            RowGroupBuffers rowGroupBuffers,
            DirectIntList columns, // contains [parquet_column_index, column_type] pairs
            int rowGroupIndex,
            int rowLo, // low row index within the row group, inclusive
            int rowHi // high row index within the row group, exclusive
    ) {
        assert ptr != 0;
        return decodeRowGroup( // throws CairoException on error
                ptr,
                rowGroupBuffers.ptr(),
                columns.getAddress(),
                (int) (columns.size() >>> 1),
                rowGroupIndex,
                rowLo,
                rowHi
        );
    }

    /**
     * Searches for the row group holding the given timestamp.
     * Scan direction is always {@link Vect#BIN_SEARCH_SCAN_DOWN}.
     * <p>
     * The row group index can be calculated on the returned value
     * via a {@link #decodeRowGroupIndex(long)} call. In case if the located
     * position is at the end of a row group or between two row groups, the
     * {@link #decodeNoNeedToDecodeFlag(long)} method will return true.
     *
     * @param timestamp            timestamp value to search for
     * @param rowLo                row lo, inclusive
     * @param rowHi                row hi, inclusive
     * @param timestampColumnIndex timestamp column index within the Parquet file
     * @return encoded row group index and "no need to decode" flag
     */
    public long findRowGroupByTimestamp(
            long timestamp,
            long rowLo,
            long rowHi,
            int timestampColumnIndex
    ) {
        assert ptr != 0;
        return findRowGroupByTimestamp( // throws CairoException on error
                ptr,
                timestamp,
                rowLo,
                rowHi,
                timestampColumnIndex
        );
    }

    public long getFileAddr() {
        return fileAddr;
    }

    public long getFileSize() {
        assert fileSize > 0 || fileAddr == 0;
        return fileSize;
    }

    public Metadata metadata() {
        assert ptr != 0;
        return metadata;
    }

    public void of(long addr, long fileSize, int memoryTag) {
        assert addr != 0;
        assert fileSize > 0;
        destroy();
        this.fileAddr = addr;
        this.fileSize = fileSize;
        final long allocator = Unsafe.getNativeAllocator(memoryTag);
        ptr = create(allocator, addr, fileSize); // throws CairoException on error
        columnsPtr = Unsafe.getUnsafe().getLong(ptr + COLUMNS_PTR_OFFSET);
        rowGroupSizesPtr = Unsafe.getUnsafe().getLong(ptr + ROW_GROUP_SIZES_PTR_OFFSET);
        metadata.init();
    }

    public void readRowGroupStats(
            RowGroupStatBuffers statBuffers,
            DirectIntList columns,
            int rowGroupIndex
    ) {
        assert ptr != 0;
        readRowGroupStats( // throws CairoException on error
                ptr,
                statBuffers.ptr(),
                columns.getAddress(),
                (int) (columns.size() >>> 1),
                rowGroupIndex
        );
    }

    private static native long columnCountOffset();

    private static native long columnIdsOffset();

    private static native long columnRecordNamePtrOffset();

    private static native long columnRecordNameSizeOffset();

    private static native long columnRecordSize();

    private static native long columnRecordTypeOffset();

    private static native long columnsPtrOffset();

    private static native long create(long allocator, long addr, long fileSize) throws CairoException;

    private static native int decodeRowGroup(
            long decoderPtr,
            long rowGroupBuffersPtr,
            long columnsPtr,
            int columnCount,
            int rowGroup,
            int rowLo,
            int rowHi
    ) throws CairoException;

    private static native void destroy(long impl);

    private static native long findRowGroupByTimestamp(
            long decoderPtr,
            long rowLo,
            long rowHi,
            long timestamp,
            int timestampColumnIndex
    );

    private static native long readRowGroupStats(
            long decoderPtr,
            long statBuffersPtr,
            long columnsPtr,
            int columnCount,
            int rowGroup
    ) throws CairoException;

    private static native long rowCountOffset();

    private static native long rowGroupCountOffset();

    private static native long rowGroupSizesPtrOffset();

    private void destroy() {
        if (ptr != 0) {
            destroy(ptr);
            ptr = 0;
            columnsPtr = 0;
            rowGroupSizesPtr = 0;
        }
    }

    public class Metadata {
        private final ObjList<DirectString> columnNames = new ObjList<>();

        public int columnCount() {
            return Unsafe.getUnsafe().getInt(ptr + COLUMN_COUNT_OFFSET);
        }

        public int columnId(int columnIndex) {
            return Unsafe.getUnsafe().getInt(columnsPtr + columnIndex * COLUMN_STRUCT_SIZE + COLUMN_IDS_OFFSET);
        }

        public CharSequence columnName(int columnIndex) {
            return columnNames.getQuick(columnIndex);
        }

        public void copyTo(GenericRecordMetadata metadata, boolean treatSymbolsAsVarchar) {
            metadata.clear();
            final int columnCount = columnCount();
            for (int i = 0; i < columnCount; i++) {
                final String columnName = Chars.toString(columnName(i));
                final int columnType = getColumnType(i);
                if (ColumnType.isSymbol(columnType)) {
                    if (treatSymbolsAsVarchar) {
                        metadata.add(new TableColumnMetadata(columnName, ColumnType.VARCHAR));
                    } else {
                        metadata.add(new TableColumnMetadata(
                                columnName,
                                columnType,
                                false,
                                64,
                                true,
                                null
                        ));
                    }
                } else {
                    metadata.add(new TableColumnMetadata(columnName, columnType));
                }
            }
        }

        public int getColumnType(int columnIndex) {
            return Unsafe.getUnsafe().getInt(columnsPtr + columnIndex * COLUMN_STRUCT_SIZE + COLUMN_RECORD_TYPE_OFFSET);
        }

        public long rowCount() {
            return Unsafe.getUnsafe().getLong(ptr + ROW_COUNT_OFFSET);
        }

        public int rowGroupCount() {
            return Unsafe.getUnsafe().getInt(ptr + ROW_GROUP_COUNT_OFFSET);
        }

        public int rowGroupSize(int rowGroupIndex) {
            return Unsafe.getUnsafe().getInt(rowGroupSizesPtr + 4L * rowGroupIndex);
        }

        private void init() {
            columnNames.clear();
            directStringPool.clear();

            final long columnCount = columnCount();
            long currentColumnPtr = columnsPtr;
            for (long i = 0; i < columnCount; i++) {
                DirectString str = directStringPool.next();
                int len = Unsafe.getUnsafe().getInt(currentColumnPtr + COLUMN_RECORD_NAME_SIZE_OFFSET);
                long colNamePtr = Unsafe.getUnsafe().getLong(currentColumnPtr + COLUMN_RECORD_NAME_PTR_OFFSET);
                str.of(colNamePtr, len);
                columnNames.add(str);
                currentColumnPtr += COLUMN_STRUCT_SIZE;
            }
        }
    }

    static {
        Os.init();

        COLUMN_COUNT_OFFSET = columnCountOffset();
        COLUMNS_PTR_OFFSET = columnsPtrOffset();
        ROW_COUNT_OFFSET = rowCountOffset();
        COLUMN_STRUCT_SIZE = columnRecordSize();
        COLUMN_RECORD_TYPE_OFFSET = columnRecordTypeOffset();
        COLUMN_RECORD_NAME_SIZE_OFFSET = columnRecordNameSizeOffset();
        COLUMN_RECORD_NAME_PTR_OFFSET = columnRecordNamePtrOffset();
        ROW_GROUP_SIZES_PTR_OFFSET = rowGroupSizesPtrOffset();
        ROW_GROUP_COUNT_OFFSET = rowGroupCountOffset();
        COLUMN_IDS_OFFSET = columnIdsOffset();
    }
}
