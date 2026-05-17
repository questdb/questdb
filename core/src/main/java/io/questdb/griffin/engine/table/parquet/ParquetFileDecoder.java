/*+*****************************************************************************
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
import io.questdb.cairo.IndexType;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.griffin.engine.table.ParquetRowGroupFilter;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.DirectIntList;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import io.questdb.std.Os;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.DirectString;


public class ParquetFileDecoder implements ParquetDecoder, ParquetRowGroupSkipper, QuietCloseable {
    private static final long COLUMNS_PTR_OFFSET;
    private static final long COLUMN_COUNT_OFFSET;
    private final static long COLUMN_IDS_OFFSET;
    private static final long COLUMN_RECORD_NAME_PTR_OFFSET;
    private static final long COLUMN_RECORD_NAME_SIZE_OFFSET;
    private static final long COLUMN_RECORD_TYPE_OFFSET;
    private static final long COLUMN_STRUCT_SIZE;
    private static final Log LOG = LogFactory.getLog(ParquetFileDecoder.class);
    private static final long ROW_COUNT_OFFSET;
    private static final long ROW_GROUP_COUNT_OFFSET;
    private static final long ROW_GROUP_SIZES_PTR_OFFSET;
    private static final long SORTED_COLUMNS_COUNT_OFFSET;
    private static final long SORTED_COLUMNS_PTR_OFFSET;
    private static final long SORTED_COLUMN_DESCENDING_OFFSET;
    private static final long SORTED_COLUMN_INDEX_OFFSET;
    private static final long SORTED_COLUMN_STRUCT_SIZE;
    private static final long TIMESTAMP_INDEX_OFFSET;
    private static final long UNUSED_BYTES_OFFSET;
    private final ObjectPool<DirectString> directStringPool = new ObjectPool<>(DirectString::new, 16);
    private final Metadata metadata = new Metadata();
    private long columnsPtr;
    // Volatile so the double-checked read in decodeRowGroup* and the synchronized
    // write in ensureDecodeContext are properly published across threads. Workers
    // call decodeRowGroup concurrently on a single decoder; without this guarantee
    // each could observe decodeContextPtr == 0 and race to allocate, leaking one
    // context per race.
    private volatile long decodeContextPtr;
    private long fileAddr; // mmapped parquet file's address
    private long fileSize; // mmapped parquet file's size
    private boolean owned;
    private long ptr;
    private long rowGroupSizesPtr;
    // Address of the native SortingColumnMeta[] (NULL when sorted_columns is empty).
    // Read lazily in Metadata.getSortedColumnIndex/Descending; populated alongside
    // columnsPtr/rowGroupSizesPtr in of().
    private long sortedColumnsPtr;

    public static native long createDecodeContext(long addr, long fileSize);

    public static native void destroyDecodeContext(long decodeContextPtr);

    /**
     * Check if a row group can be skipped based on min/max statistics and bloom filter conditions.
     * <p>
     * Filter list format: 3 longs per filter
     * - Long 0: encoded(column_index, count, op) - lower 32 bits: column_index, next 24 bits: count, upper 8 bits: op
     * - Long 1: values_ptr
     * - Long 2: column_type
     *
     * @param rowGroupIndex the row group index to check
     * @param filters       filter descriptors: [encoded(col_idx, count, op), ptr, column_type] per filter
     * @param filterBufEnd  exclusive end address of the filter values buffer, used for native bounds checking
     * @return true if the row group can be safely skipped
     */
    @Override
    public boolean canSkipRowGroup(int rowGroupIndex, DirectLongList filters, long filterBufEnd) {
        assert ptr != 0;
        assert filters.size() % ParquetRowGroupFilter.LONGS_PER_FILTER == 0;
        return canSkipRowGroup(
                ptr,
                rowGroupIndex,
                fileAddr,
                fileSize,
                filters.getAddress(),
                (int) (filters.size() / ParquetRowGroupFilter.LONGS_PER_FILTER),
                filterBufEnd
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
        ensureDecodeContext();
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
        ensureDecodeContext();
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
        ensureDecodeContext();
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

    @Override
    public int getColumnCount() {
        return metadata.getColumnCount();
    }

    @Override
    public int getColumnId(int columnIndex) {
        return metadata.getColumnId(columnIndex);
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
        columnsPtr = Unsafe.getLong(ptr + COLUMNS_PTR_OFFSET);
        rowGroupSizesPtr = Unsafe.getLong(ptr + ROW_GROUP_SIZES_PTR_OFFSET);
        sortedColumnsPtr = Unsafe.getLong(ptr + SORTED_COLUMNS_PTR_OFFSET);
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
    public void of(ParquetFileDecoder other) {
        this.fileAddr = other.fileAddr;
        this.fileSize = other.fileSize;
        this.ptr = other.ptr;
        if (decodeContextPtr != 0) {
            destroyDecodeContext(decodeContextPtr);
            decodeContextPtr = 0;
        }
        this.columnsPtr = other.columnsPtr;
        this.rowGroupSizesPtr = other.rowGroupSizesPtr;
        this.sortedColumnsPtr = other.sortedColumnsPtr;
        this.metadata.of(other.metadata);
        owned = false;
    }

    public boolean rowGroupColumnHasEncoding(int rowGroupIndex, int columnIndex, int encoding) {
        assert ptr != 0;
        return rowGroupColumnHasEncoding(ptr, rowGroupIndex, columnIndex, encoding);
    }

    public long rowGroupMaxTimestamp(int rowGroupIndex, int timestampColumnIndex) {
        assert ptr != 0;
        return rowGroupMaxTimestamp(ptr, fileAddr, fileSize, rowGroupIndex, timestampColumnIndex);
    }

    public long rowGroupMinTimestamp(int rowGroupIndex, int timestampColumnIndex) {
        assert ptr != 0;
        return rowGroupMinTimestamp(ptr, fileAddr, fileSize, rowGroupIndex, timestampColumnIndex);
    }

    private static native boolean canSkipRowGroup(
            long decoderPtr,
            int rowGroupIndex,
            long filePtr,
            long fileSize,
            long filtersPtr,
            int filterCount,
            long filterBufEnd
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
            long fileAddr,
            long fileSize,
            long timestamp,
            long rowLo,
            long rowHi,
            int timestampIndex
    ) throws CairoException;

    private static native long rowCountOffset();

    private static native boolean rowGroupColumnHasEncoding(
            long decoderPtr,
            int rowGroupIndex,
            int columnIndex,
            int encoding
    ) throws CairoException;

    private static native long rowGroupCountOffset();

    private static native long rowGroupMaxTimestamp(
            long decoderPtr,
            long fileAddr,
            long fileSize,
            int rowGroupIndex,
            int timestampColumnIndex
    ) throws CairoException;

    private static native long rowGroupMinTimestamp(
            long decoderPtr,
            long fileAddr,
            long fileSize,
            int rowGroupIndex,
            int timestampColumnIndex
    ) throws CairoException;

    private static native long rowGroupSizesPtrOffset();

    private static native long sortedColumnDescendingOffset();

    private static native long sortedColumnIndexOffset();

    private static native long sortedColumnRecordSize();

    private static native long sortedColumnsCountOffset();

    private static native long sortedColumnsPtrOffset();

    private static native long timestampIndexOffset();

    private static native long unusedBytesOffset();

    /**
     * Initialises the decode context lazily and safely under concurrent callers.
     * The double-checked read on a volatile field plus the synchronized write
     * means workers calling {@code decodeRowGroup*} concurrently on a freshly
     * opened decoder can no longer race to allocate two contexts.
     */
    private synchronized void ensureDecodeContextLocked() {
        if (decodeContextPtr == 0) {
            decodeContextPtr = createDecodeContext(fileAddr, fileSize);
        }
    }

    private void ensureDecodeContext() {
        if (decodeContextPtr == 0) {
            ensureDecodeContextLocked();
        }
    }

    private void destroy() {
        if (owned && ptr != 0) {
            destroy(ptr);
            ptr = 0;
            columnsPtr = 0;
            rowGroupSizesPtr = 0;
            sortedColumnsPtr = 0;
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
        // Scratch buffer for parquet-index -> output-index translation in
        // copyToSansUnsupported. Reused across files so the call doesn't
        // allocate per invocation. Sized in copyToSansUnsupported.
        private final IntList parquetToOutputIndex = new IntList();

        /**
         * Copies supported columns into the provided metadata, skipping any with Undefined type.
         * If treatSymbolsAsVarchar is true, symbol columns are exposed as VARCHAR.
         */
        public void copyToSansUnsupported(GenericRecordMetadata dest, boolean treatSymbolsAsVarchar) {
            dest.clear();
            final int timestampIndex = getTimestampIndex();
            int copyTimestampIndex = -1;
            // Translate parquet column indices to output (post-skip) indices so
            // the sort-order map below can be applied. Sized to the input width;
            // entries for unsupported (skipped) columns stay at -1 and are not
            // propagated.
            final int sourceCount = getColumnCount();
            parquetToOutputIndex.clear();
            parquetToOutputIndex.setPos(sourceCount);
            for (int i = 0; i < sourceCount; i++) {
                parquetToOutputIndex.setQuick(i, -1);
            }
            for (int i = 0; i < sourceCount; i++) {
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
                                IndexType.NONE,
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
                parquetToOutputIndex.setQuick(i, dest.getColumnCount() - 1);
            }

            dest.setTimestampIndex(copyTimestampIndex);

            // Carry parquet `sorting_columns` metadata through to the output
            // metadata so the optimiser can elide redundant ORDER BY clauses.
            // Only propagate columns that survived the skip pass above; an
            // unsupported sort column has no place in dest, so it's ignored.
            final int sortCount = getSortedColumnsCount();
            for (int rank = 0; rank < sortCount; rank++) {
                final int parquetIdx = getSortedColumnParquetIndex(rank);
                if (parquetIdx < 0 || parquetIdx >= sourceCount) {
                    continue;
                }
                final int outputIdx = parquetToOutputIndex.getQuick(parquetIdx);
                if (outputIdx >= 0) {
                    dest.setColumnOrderBy(outputIdx, isSortedColumnDescending(rank) ? -1 : 1);
                }
            }
        }

        public int getColumnCount() {
            return Unsafe.getInt(ptr + COLUMN_COUNT_OFFSET);
        }

        public int getColumnId(int columnIndex) {
            return Unsafe.getInt(columnsPtr + columnIndex * COLUMN_STRUCT_SIZE + COLUMN_IDS_OFFSET);
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
            return Unsafe.getInt(columnsPtr + columnIndex * COLUMN_STRUCT_SIZE + COLUMN_RECORD_TYPE_OFFSET);
        }

        public long getRowCount() {
            return Unsafe.getLong(ptr + ROW_COUNT_OFFSET);
        }

        public int getRowGroupCount() {
            return Unsafe.getInt(ptr + ROW_GROUP_COUNT_OFFSET);
        }

        public int getRowGroupSize(int rowGroupIndex) {
            return Unsafe.getInt(rowGroupSizesPtr + 4L * rowGroupIndex);
        }

        /**
         * Returns the order direction the file is sorted on for the given
         * parquet column index, or 0 if the file does not declare a sort on
         * that column.
         * <p>
         * Returns +1 (ASC) or -1 (DESC) when the file's {@code sorting_columns}
         * metadata names this column at any rank, and the same entry holds
         * consistently across every row group.
         * <p>
         * The optimiser consumes this to elide redundant {@code ORDER BY col}
         * clauses. Callers must hold a parquet-side column index, not the
         * projected/output index.
         */
        public int getColumnOrderBy(int parquetColumnIndex) {
            final int n = getSortedColumnsCount();
            for (int i = 0; i < n; i++) {
                if (getSortedColumnParquetIndex(i) == parquetColumnIndex) {
                    return isSortedColumnDescending(i) ? -1 : 1;
                }
            }
            return 0;
        }

        /**
         * Parquet column index of the i-th sorted column, in the order the
         * file declares sort precedence (rank 0 = primary key, rank 1 = tie
         * breaker, ...). Bounds-checked at debug time only.
         */
        public int getSortedColumnParquetIndex(int rank) {
            assert rank >= 0 && rank < getSortedColumnsCount();
            return Unsafe.getInt(sortedColumnsPtr + rank * SORTED_COLUMN_STRUCT_SIZE + SORTED_COLUMN_INDEX_OFFSET);
        }

        /**
         * Count of columns the file claims as sorted CONSISTENTLY across every
         * row group. Zero when the file has no {@code sorting_columns}
         * metadata or when sort is inconsistent across row groups.
         */
        public int getSortedColumnsCount() {
            return Unsafe.getInt(ptr + SORTED_COLUMNS_COUNT_OFFSET);
        }

        public int getTimestampIndex() {
            // The value is stored as Option<NonMaxU32> on the Rust side,
            // so we need to apply bitwise not to get the actual value.
            // None is mapped to ~0 which is u32::max or -1_i32.
            return ~Unsafe.getInt(ptr + TIMESTAMP_INDEX_OFFSET);
        }

        public boolean isSortedColumnDescending(int rank) {
            assert rank >= 0 && rank < getSortedColumnsCount();
            return Unsafe.getByte(sortedColumnsPtr + rank * SORTED_COLUMN_STRUCT_SIZE + SORTED_COLUMN_DESCENDING_OFFSET) != 0;
        }

        public long getUnusedBytes() {
            return Unsafe.getLong(ptr + UNUSED_BYTES_OFFSET);
        }

        private void init() {
            columnNames.clear();
            directStringPool.clear();

            final long columnCount = getColumnCount();
            long currentColumnPtr = columnsPtr;
            for (long i = 0; i < columnCount; i++) {
                DirectString str = directStringPool.next();
                int len = Unsafe.getInt(currentColumnPtr + COLUMN_RECORD_NAME_SIZE_OFFSET);
                long colNamePtr = Unsafe.getLong(currentColumnPtr + COLUMN_RECORD_NAME_PTR_OFFSET);
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
        SORTED_COLUMNS_COUNT_OFFSET = sortedColumnsCountOffset();
        SORTED_COLUMNS_PTR_OFFSET = sortedColumnsPtrOffset();
        SORTED_COLUMN_STRUCT_SIZE = sortedColumnRecordSize();
        SORTED_COLUMN_INDEX_OFFSET = sortedColumnIndexOffset();
        SORTED_COLUMN_DESCENDING_OFFSET = sortedColumnDescendingOffset();
        TIMESTAMP_INDEX_OFFSET = timestampIndexOffset();
        UNUSED_BYTES_OFFSET = unusedBytesOffset();
        COLUMN_IDS_OFFSET = columnIdsOffset();
    }
}
