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
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.DirectString;

public class PartitionDecoder implements QuietCloseable {
    private static final long COLUMNS_PTR_OFFSET;
    private static final long COLUMN_COUNT_OFFSET;
    private final static long COLUMN_IDS_OFFSET;
    private static final long COLUMN_RECORD_NAME_PTR_OFFSET;
    private static final long COLUMN_RECORD_NAME_SIZE_OFFSET;
    private static final long COLUMN_RECORD_TYPE_OFFSET;
    private static final long COLUMN_STRUCT_SIZE;
    private static final Log LOG = LogFactory.getLog(PartitionDecoder.class);
    private static final long ROW_COUNT_OFFSET;
    private static final long ROW_GROUP_COUNT_OFFSET;
    private static final long ROW_GROUP_SIZES_PTR_OFFSET;
    private final ObjectPool<DirectString> directStringPool = new ObjectPool<>(DirectString::new, 16);
    private final Metadata metadata = new Metadata();
    private long columnsPtr;
    private long fd; // kept around for logging purposes
    private long ptr;
    private long rowGroupSizesPtr;

    @Override
    public void close() {
        destroy();
        fd = -1;
    }

    public int decodeRowGroup(
            RowGroupBuffers rowGroupBuffers,
            DirectIntList columns, // contains [parquet_column_index, column_type] pairs
            int rowGroupIndex,
            int rowLo, // low row index within the row group, inclusive
            int rowHi // high row index within the row group, exclusive
    ) {
        assert ptr != 0;
        return decodeRowGroup(  // throws CairoException on error
                ptr,
                rowGroupBuffers.ptr(),
                columns.getAddress(),
                (int) (columns.size() >>> 1),
                rowGroupIndex,
                rowLo,
                rowHi
        );
    }

    public long getFd() {
        return fd;
    }

    public Metadata getMetadata() {
        assert ptr != 0;
        return metadata;
    }

    public void getRowGroupStats(
            RowGroupStatBuffers rowGroupStatBuffers,
            DirectIntList columns,
            int rowGroupIndex
    ) {
        assert ptr != 0;
        getRowGroupStats(  // throws CairoException on error
                ptr,
                rowGroupStatBuffers.ptr(),
                columns.getAddress(),
                (int) (columns.size() >>> 1),
                rowGroupIndex
        );
    }

    public void of(long fd) {
        of(fd, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
    }

    public void of(long fd, int memoryTag) {
        destroy();
        this.fd = fd;
        final long allocator = Unsafe.getNativeAllocator(memoryTag);
        ptr = create(allocator, Files.toOsFd(fd));  // throws CairoException on error
        columnsPtr = Unsafe.getUnsafe().getLong(ptr + COLUMNS_PTR_OFFSET);
        rowGroupSizesPtr = Unsafe.getUnsafe().getLong(ptr + ROW_GROUP_SIZES_PTR_OFFSET);
        metadata.init();
    }

    private static native long columnCountOffset();

    private static native long columnIdsOffset();

    private static native long columnRecordNamePtrOffset();

    private static native long columnRecordNameSizeOffset();

    private static native long columnRecordSize();

    private static native long columnRecordTypeOffset();

    private static native long columnsPtrOffset();

    private static native long create(long allocator, int fd) throws CairoException;

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

    private static native long getRowGroupStats(
            long decoderPtr,
            long rowGroupStatBuffersPtr,
            long requestedColumnsPtr,
            int requestedColumnCount,
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
                if (ColumnType.isSymbol(columnType) && treatSymbolsAsVarchar) {
                    metadata.add(new TableColumnMetadata(columnName, ColumnType.VARCHAR));
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
