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

package io.questdb.griffin.engine.table.parquet;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.DirectIntList;
import io.questdb.std.DirectLongList;
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
    private static final Log LOG = LogFactory.getLog(PartitionDecoder.class);
    private static final long ROW_COUNT_OFFSET;
    private static final long ROW_GROUP_COUNT_OFFSET;
    private static final long ROW_GROUP_SIZES_PTR_OFFSET;
    private static final long TIMESTAMP_INDEX_OFFSET;
    private final ObjectPool<DirectString> directStringPool = new ObjectPool<>(DirectString::new, 16);
    private final Metadata metadata = new Metadata();
    private long columnsPtr;
    private long decodeContextPtr;
    private long fileAddr; // mmapped parquet file's address
    private long fileSize; // mmapped parquet file's size
    private boolean owned;
    private long ptr;
    private long rowGroupSizesPtr;

    public static native long createDecodeContext(long addr, long fileSize);

    public static boolean decodeNoNeedToDecodeFlag(long encodedIndex) {
        return (encodedIndex & 1) == 1;
    }

    public static int decodeRowGroupIndex(long encodedIndex) {
        return (int) ((encodedIndex >> 1) - 1);
    }

    public static native void destroyDecodeContext(long decodeContextPtr);

    public boolean canSkipRowGroup(
            int rowGroupIndex,
            DirectLongList filters // contains [parquet_column_index, ColumnFilterValues] pairs
    ) {
        assert ptr != 0;
        return canSkipRowGroup(
                ptr,
                rowGroupIndex,
                fileAddr,
                fileSize,
                filters.getAddress(),
                (int) (filters.size() / 3)
        );
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
        if (decodeContextPtr == 0) {
            // lazy init
            decodeContextPtr = createDecodeContext(fileAddr, fileSize);
        }
        return decodeRowGroup( // throws CairoException on error
                ptr,
                decodeContextPtr,
                rowGroupBuffers.ptr(),
                columns.getAddress(),
                (int) (columns.size() >>> 1),
                rowGroupIndex,
                rowLo,
                rowHi
        );
    }

    public void decodeRowGroupWithRowFilter(
            RowGroupBuffers rowGroupBuffers,
            int columnOffset,
            DirectIntList columns, // contains [parquet_column_index, column_type] pairs
            int rowGroupIndex,
            int rowLo, // low row index within the row group, inclusive
            int rowHi, // high row index within the row group, exclusive
            DirectLongList filteredRows
    ) {
        assert ptr != 0;
        if (decodeContextPtr == 0) {
            // lazy init
            decodeContextPtr = createDecodeContext(fileAddr, fileSize);
        }
        decodeRowGroupWithRowFilter(
                ptr,
                decodeContextPtr,
                rowGroupBuffers.ptr(),
                columnOffset,
                columns.getAddress(),
                (int) (columns.size() >>> 1),
                rowGroupIndex,
                rowLo,
                rowHi,
                filteredRows.getAddress(),
                filteredRows.size()
        );
    }

    public void decodeRowGroupWithRowFilterFillNulls(
            RowGroupBuffers rowGroupBuffers,
            int columnOffset,
            DirectIntList columns, // contains [parquet_column_index, column_type] pairs
            int rowGroupIndex,
            int rowLo, // low row index within the row group, inclusive
            int rowHi, // high row index within the row group, exclusive
            DirectLongList filteredRows
    ) {
        assert ptr != 0;
        if (decodeContextPtr == 0) {
            // lazy init
            decodeContextPtr = createDecodeContext(fileAddr, fileSize);
        }
        decodeRowGroupWithRowFilterFillNulls(
                ptr,
                decodeContextPtr,
                rowGroupBuffers.ptr(),
                columnOffset,
                columns.getAddress(),
                (int) (columns.size() >>> 1),
                rowGroupIndex,
                rowLo,
                rowHi,
                filteredRows.getAddress(),
                filteredRows.size()
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
     * @param timestamp      timestamp value to search for
     * @param rowLo          row lo, inclusive
     * @param rowHi          row hi, inclusive
     * @param timestampIndex timestamp column index within the Parquet file
     * @return encoded row group index and "no need to decode" flag
     */
    public long findRowGroupByTimestamp(
            long timestamp,
            long rowLo,
            long rowHi,
            int timestampIndex
    ) {
        assert ptr != 0;
        return findRowGroupByTimestamp( // throws CairoException on error
                ptr,
                timestamp,
                rowLo,
                rowHi,
                timestampIndex
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
        this.owned = true;
        this.fileAddr = addr;
        this.fileSize = fileSize;
        final long allocator = Unsafe.getNativeAllocator(memoryTag);
        ptr = create(allocator, addr, fileSize); // throws CairoException on error
        columnsPtr = Unsafe.getUnsafe().getLong(ptr + COLUMNS_PTR_OFFSET);
        rowGroupSizesPtr = Unsafe.getUnsafe().getLong(ptr + ROW_GROUP_SIZES_PTR_OFFSET);
        metadata.init();
    }

    /**
     * Creates a non-owning shallow copy that shares native metadata with the source decoder.
     * <p>
     * This instance will reference the same native {@code ParquetDecoder} (via {@code ptr}) as
     * {@code other}, but will create its own {@code DecodeContext} lazily when decoding.
     * <p>
     * <b>Lifetime requirement:</b> The source decoder ({@code other}) must remain open and valid
     * for the entire lifetime of this instance. Closing or reinitializing {@code other} while
     * this instance is in use will result in use-after-free of native memory.
     * <p>
     * Typical usage: {@code PageFrameMemoryPool} creates thread-local decoders that copy metadata
     * from a shared decoder owned by {@code TableReader}. The TableReader must outlive all
     * worker threads using the copied decoders.
     *
     * @param other the source decoder whose metadata will be shared (must remain valid)
     */
    public void of(PartitionDecoder other) {
        this.fileAddr = other.fileAddr;
        this.fileSize = other.fileSize;
        this.ptr = other.ptr;
        if (decodeContextPtr != 0) {
            destroyDecodeContext(decodeContextPtr);
            decodeContextPtr = 0;
        }
        this.columnsPtr = other.columnsPtr;
        this.rowGroupSizesPtr = other.rowGroupSizesPtr;
        this.metadata.of(other.metadata);
        owned = false;
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

    private static native boolean canSkipRowGroup(
            long decoderPtr,
            int rowGroupIndex,
            long filePtr,
            long fileSize,
            long filtersPtr,
            int filterCount
    ) throws CairoException;

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
            long decodeContextPtr,
            long rowGroupBuffersPtr,
            long columnsPtr,
            int columnCount,
            int rowGroup,
            int rowLo,
            int rowHi
    ) throws CairoException;

    private static native void decodeRowGroupWithRowFilter(
            long decoderPtr,
            long decodeContextPtr,
            long rowGroupBuffersPtr,
            int columnOffset,
            long columnsPtr,
            int columnCount,
            int rowGroup,
            int rowLo,
            int rowHi,
            long filteredRowsPtr,
            long filteredRowsSize
    ) throws CairoException;

    private static native void decodeRowGroupWithRowFilterFillNulls(
            long decoderPtr,
            long decodeContextPtr,
            long rowGroupBuffersPtr,
            int columnOffset,
            long columnsPtr,
            int columnCount,
            int rowGroup,
            int rowLo,
            int rowHi,
            long filteredRowsPtr,
            long filteredRowsSize
    ) throws CairoException;

    private static native void destroy(long impl);

    private static native long findRowGroupByTimestamp(
            long decoderPtr,
            long rowLo,
            long rowHi,
            long timestamp,
            int timestampIndex
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

    private static native long timestampIndexOffset();

    private void destroy() {
        if (owned && ptr != 0) {
            destroy(ptr);
            ptr = 0;
            columnsPtr = 0;
            rowGroupSizesPtr = 0;
        }
        if (decodeContextPtr != 0) {
            destroyDecodeContext(decodeContextPtr);
            decodeContextPtr = 0;
        }
    }

    /**
     * Note: low-level parquet metadata tracks all columns (unsupported are tagged as Undefined),
     * but higher-level GenericRecordMetadata produced via copyToSansUnsupported() omits them.
     */
    public class Metadata {
        private final ObjList<DirectString> columnNames = new ObjList<>();

        /**
         * Copies supported columns into the provided metadata, skipping any with Undefined type.
         * If treatSymbolsAsVarchar is true, symbol columns are exposed as VARCHAR.
         */
        public void copyToSansUnsupported(GenericRecordMetadata dest, boolean treatSymbolsAsVarchar) {
            dest.clear();
            final int timestampIndex = getTimestampIndex();
            int copyTimestampIndex = -1;
            for (int i = 0, n = getColumnCount(); i < n; i++) {
                final String columnName = Chars.toString(getColumnName(i));
                final int columnType = getColumnType(i);

                if (ColumnType.isUndefined(columnType)) {
                    LOG.info().$("unsupported column type, skipping [column=").$(columnName).I$();
                    continue;
                }

                if (ColumnType.isSymbol(columnType)) {
                    if (treatSymbolsAsVarchar) {
                        dest.add(new TableColumnMetadata(columnName, ColumnType.VARCHAR));
                    } else {
                        dest.add(new TableColumnMetadata(
                                columnName,
                                columnType,
                                false,
                                64,
                                true,
                                null
                        ));
                    }
                } else {
                    dest.add(new TableColumnMetadata(columnName, columnType));
                    // Determine designated timestamp's index within the copy.
                    if (ColumnType.isTimestamp(columnType) && i == timestampIndex) {
                        copyTimestampIndex = dest.getColumnCount() - 1;
                    }
                }
            }

            dest.setTimestampIndex(copyTimestampIndex);
        }

        public int getColumnCount() {
            return Unsafe.getUnsafe().getInt(ptr + COLUMN_COUNT_OFFSET);
        }

        public int getColumnId(int columnIndex) {
            return Unsafe.getUnsafe().getInt(columnsPtr + columnIndex * COLUMN_STRUCT_SIZE + COLUMN_IDS_OFFSET);
        }

        public int getColumnIndex(CharSequence name) {
            assert ptr != 0;
            for (int i = 0, n = columnNames.size(); i < n; i++) {
                if (Chars.equals(columnNames.getQuick(i), name)) {
                    return i;
                }
            }
            return -1;
        }

        public CharSequence getColumnName(int columnIndex) {
            return columnNames.getQuick(columnIndex);
        }

        public int getColumnType(int columnIndex) {
            return Unsafe.getUnsafe().getInt(columnsPtr + columnIndex * COLUMN_STRUCT_SIZE + COLUMN_RECORD_TYPE_OFFSET);
        }

        public long getRowCount() {
            return Unsafe.getUnsafe().getLong(ptr + ROW_COUNT_OFFSET);
        }

        public int getRowGroupCount() {
            return Unsafe.getUnsafe().getInt(ptr + ROW_GROUP_COUNT_OFFSET);
        }

        public int getRowGroupSize(int rowGroupIndex) {
            return Unsafe.getUnsafe().getInt(rowGroupSizesPtr + 4L * rowGroupIndex);
        }

        public int getTimestampIndex() {
            // The value is stored as Option<NonMaxU32> on the Rust side,
            // so we need to apply bitwise not to get the actual value.
            // None is mapped to ~0 which is u32::max or -1_i32.
            return ~Unsafe.getUnsafe().getInt(ptr + TIMESTAMP_INDEX_OFFSET);
        }

        private void init() {
            columnNames.clear();
            directStringPool.clear();

            final long columnCount = getColumnCount();
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

        void of(Metadata other) {
            this.columnNames.clear();
            this.columnNames.addAll(other.columnNames);
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
        TIMESTAMP_INDEX_OFFSET = timestampIndexOffset();
        COLUMN_IDS_OFFSET = columnIdsOffset();
    }
}
