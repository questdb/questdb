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

package io.questdb.cairo;

import io.questdb.griffin.engine.table.ParquetRowGroupFilter;
import io.questdb.griffin.engine.table.parquet.ParquetRowGroupSkipper;
import io.questdb.std.DirectLongList;
import io.questdb.std.Os;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Utf8s;

/**
 * Zero-allocation reader for _pm parquet metadata files.
 * Reads directly from mmaped memory via Unsafe offset arithmetic.
 * <p>
 * Implements {@link ParquetRowGroupSkipper} for filter-pushdown row group
 * pruning. The first call to {@link #canSkipRowGroup} lazily allocates a
 * native handle that caches the parsed {@code _pm} header/footer; the
 * handle is reused across all subsequent skip calls and freed by
 * {@link #close()} / {@link #clear()}.
 * <p>
 * <b>Lifecycle contract:</b> Callers MUST invoke {@link #close()} (or
 * {@link #clear()}) BEFORE munmapping the underlying {@code _pm} file. The
 * native handle borrows from the mmap and reading after unmap is undefined
 * behaviour. {@code ShowPartitionsRecordCursorFactory.closeParquetMeta()}
 * is the reference pattern: clear, then munmap.
 * <p>
 * <b>Thread safety:</b> Not thread-safe per instance. The lazy native
 * handle initialization is racy if two threads enter {@link #canSkipRowGroup}
 * concurrently. Each worker thread must hold its own reader instance.
 * <p>
 * Binary format (little-endian):
 * <pre>
 * HEADER (24 bytes fixed):
 *   [0]  FORMAT_VERSION    u32
 *   [4]  FEATURE_FLAGS     u64
 *   [12] DESIGNATED_TS     i32
 *   [16] SORTING_COL_CNT   u32
 *   [20] COLUMN_COUNT      u32
 *   [24..] column descriptors (32B each), sorting columns (4B each), name strings
 *   [..] header feature sections (if any feature flags set)
 *
 * ROW GROUP BLOCK (8-byte aligned, per row group):
 *   [0]  NUM_ROWS        u64
 *   [8..] column chunks (64B each), then optional out-of-line stats
 *
 * FOOTER (offset derived from trailer at end of file):
 *   [0]  PARQUET_FOOTER_OFFSET   u64
 *   [8]  PARQUET_FOOTER_LENGTH   u32
 *   [12] ROW_GROUP_COUNT         u32
 *   [16] UNUSED_BYTES            u64
 *   [24..] row group entries (4B each, u32 block offset >> 3)
 *   [..]  feature sections (gated by header FEATURE_FLAGS)
 *   [..]  CRC32                  u32
 *   [..]  FOOTER_LENGTH          u32  (total bytes from footer start through CRC)
 * </pre>
 */
public class ParquetMetaFileReader implements ParquetRowGroupSkipper, QuietCloseable {

    // Row group block offsets are stored right-shifted by this amount
    private static final int BLOCK_ALIGNMENT_SHIFT = 3;
    // Column descriptor layout (32B each, starting at header offset 24)
    private static final int COL_DESC_COL_TYPE_OFF = 12;
    private static final int COL_DESC_ID_OFF = 8;
    private static final int COL_DESC_NAME_LENGTH_OFF = 24;
    private static final int COL_DESC_NAME_OFFSET_OFF = 0;
    private static final int COLUMN_CHUNK_MAX_STAT_OFF = 56;
    private static final int COLUMN_CHUNK_MIN_STAT_OFF = 48;
    // Column chunk layout (64B per chunk, starting at row group block offset + 8)
    private static final int COLUMN_CHUNK_SIZE = 64;
    private static final int COLUMN_CHUNK_STAT_FLAGS_OFF = 2;
    private static final int COLUMN_DESCRIPTOR_SIZE = 32;
    private static final int FOOTER_FIXED_SIZE = 32;
    private static final int FOOTER_PARQUET_FOOTER_LENGTH_OFF = 8;
    // Footer offsets (relative to footer start)
    private static final int FOOTER_PARQUET_FOOTER_OFFSET_OFF = 0;
    private static final int FOOTER_PREV_FOOTER_OFFSET_OFF = 24;
    private static final int FOOTER_ROW_GROUP_COUNT_OFF = 12;
    // Footer trailer size (appended after CRC)
    private static final int FOOTER_TRAILER_SIZE = 4;
    private static final int FOOTER_UNUSED_BYTES_OFF = 16;
    // Header offsets (new layout: footer_offset(8) + feature_flags(8) + dts(4) + sorting(4) + col_count(4) + reserved(4))
    private static final int HEADER_FOOTER_OFFSET_OFF = 0;
    private static final long HEADER_FEATURE_FLAGS_OFF = 8;
    private static final int HEADER_DESIGNATED_TS_OFF = 16;
    private static final int HEADER_COLUMN_COUNT_OFF = 24;
    private static final int HEADER_FIXED_SIZE = 32;
    // Feature flag bits 32-63 are required: unknown bits must cause rejection.
    private static final long REQUIRED_FEATURE_MASK = 0xFFFF_FFFF_0000_0000L;
    private long addr;
    private final DirectUtf8String flyweightColName = new DirectUtf8String();
    private int columnCount;
    private long fileSize;
    private long footerAddr;
    // Lazily allocated native handle to a JniParquetMetaReader. Created on
    // the first canSkipRowGroup call and freed by clear()/close().
    private long nativeReaderPtr;
    private int rowGroupCount;
    private long totalRowCount;

    @Override
    public boolean canSkipRowGroup(int rowGroupIndex, DirectLongList filters, long filterBufEnd) {
        assert addr != 0;
        assert filters.size() % ParquetRowGroupFilter.LONGS_PER_FILTER == 0;
        if (nativeReaderPtr == 0) {
            nativeReaderPtr = createNativeReader(addr, fileSize);
        }
        return canSkipRowGroup0(
                nativeReaderPtr,
                rowGroupIndex,
                filters.getAddress(),
                (int) (filters.size() / ParquetRowGroupFilter.LONGS_PER_FILTER),
                filterBufEnd
        );
    }

    public void clear() {
        if (nativeReaderPtr != 0) {
            destroyNativeReader(nativeReaderPtr);
            nativeReaderPtr = 0;
        }
        this.addr = 0;
        this.fileSize = 0;
        this.footerAddr = 0;
        this.columnCount = 0;
        this.rowGroupCount = 0;
        this.totalRowCount = 0;
    }

    /**
     * Releases the native reader handle but preserves in-memory state
     * (addr, fileSize, column/row group counts). This allows long-lived
     * owners to defensively close without breaking subsequent accessor
     * reads or canSkipRowGroup calls (which lazily reallocate the handle).
     * Use {@link #clear()} for a full reset.
     */
    @Override
    public void close() {
        if (nativeReaderPtr != 0) {
            destroyNativeReader(nativeReaderPtr);
            nativeReaderPtr = 0;
        }
    }

    public long getAddr() {
        return addr;
    }

    public long getChunkMaxStat(int rowGroupIndex, int columnIndex) {
        assert rowGroupIndex >= 0 && rowGroupIndex < rowGroupCount;
        assert columnIndex >= 0 && columnIndex < columnCount;
        return Unsafe.getUnsafe().getLong(columnChunkAddr(rowGroupIndex, columnIndex) + COLUMN_CHUNK_MAX_STAT_OFF);
    }

    public long getChunkMinStat(int rowGroupIndex, int columnIndex) {
        assert rowGroupIndex >= 0 && rowGroupIndex < rowGroupCount;
        assert columnIndex >= 0 && columnIndex < columnCount;
        return Unsafe.getUnsafe().getLong(columnChunkAddr(rowGroupIndex, columnIndex) + COLUMN_CHUNK_MIN_STAT_OFF);
    }

    public int getChunkStatFlags(int rowGroupIndex, int columnIndex) {
        assert rowGroupIndex >= 0 && rowGroupIndex < rowGroupCount;
        assert columnIndex >= 0 && columnIndex < columnCount;
        return Unsafe.getUnsafe().getByte(columnChunkAddr(rowGroupIndex, columnIndex) + COLUMN_CHUNK_STAT_FLAGS_OFF) & 0xFF;
    }

    public int getColumnCount() {
        return columnCount;
    }

    public int getColumnId(int columnIndex) {
        return Unsafe.getUnsafe().getInt(columnDescriptorAddr(columnIndex) + COL_DESC_ID_OFF);
    }

    /**
     * Finds a column by name (linear scan). Returns -1 if not found.
     */
    public int getColumnIndex(CharSequence name) {
        for (int i = 0; i < columnCount; i++) {
            if (Utf8s.equalsUtf16(name, getColumnName(i))) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Returns the column name for the given column index as a flyweight
     * over the mmaped _pm data. The returned reference is reused across
     * calls — callers must not hold it past the next call.
     */
    public DirectUtf8String getColumnName(int columnIndex) {
        long descAddr = columnDescriptorAddr(columnIndex);
        long nameOffset = Unsafe.getUnsafe().getLong(descAddr + COL_DESC_NAME_OFFSET_OFF);
        int nameLength = Unsafe.getUnsafe().getInt(descAddr + COL_DESC_NAME_LENGTH_OFF);
        long nameAddr = addr + nameOffset;
        return flyweightColName.of(nameAddr, nameAddr + nameLength, true);
    }

    public int getColumnType(int columnIndex) {
        return Unsafe.getUnsafe().getInt(columnDescriptorAddr(columnIndex) + COL_DESC_COL_TYPE_OFF);
    }

    /**
     * Returns the column index of the designated timestamp, or -1 if none.
     */
    public int getDesignatedTimestampColumnIndex() {
        return Unsafe.getUnsafe().getInt(addr + HEADER_DESIGNATED_TS_OFF);
    }

    public long getFileSize() {
        return fileSize;
    }

    /**
     * Derives the parquet file size from the _pm footer metadata.
     * parquetFileSize = PARQUET_FOOTER_OFFSET + PARQUET_FOOTER_LENGTH + 8
     * (4B parquet footer length field + 4B PAR1 magic)
     */
    public long getParquetFileSize() {
        long parquetFooterOffset = Unsafe.getUnsafe().getLong(footerAddr + FOOTER_PARQUET_FOOTER_OFFSET_OFF);
        int parquetFooterLength = Unsafe.getUnsafe().getInt(footerAddr + FOOTER_PARQUET_FOOTER_LENGTH_OFF);
        return parquetFooterOffset + Integer.toUnsignedLong(parquetFooterLength) + 8;
    }

    /**
     * Returns the total number of rows across all row groups (cached, O(1)).
     */
    public long getPartitionRowCount() {
        return totalRowCount;
    }

    /**
     * Alias for {@link #getPartitionRowCount()}.
     */
    public long getRowCount() {
        return getPartitionRowCount();
    }

    public int getRowGroupCount() {
        return rowGroupCount;
    }

    /**
     * Returns the maximum timestamp from the specified row group's column chunk.
     * Reads the inline max_stat (i64) at offset 56 within the column chunk.
     *
     * @param rowGroupIndex        zero-based row group index
     * @param timestampColumnIndex column index of the designated timestamp
     * @return max timestamp value (epoch micros)
     */
    public long getRowGroupMaxTimestamp(int rowGroupIndex, int timestampColumnIndex) {
        assert rowGroupIndex >= 0 && rowGroupIndex < rowGroupCount;
        assert timestampColumnIndex >= 0 && timestampColumnIndex < columnCount;
        long chunkAddr = columnChunkAddr(rowGroupIndex, timestampColumnIndex);
        return Unsafe.getUnsafe().getLong(chunkAddr + COLUMN_CHUNK_MAX_STAT_OFF);
    }

    /**
     * Returns the minimum timestamp from the specified row group's column chunk.
     * Reads the inline min_stat (i64) at offset 48 within the column chunk.
     *
     * @param rowGroupIndex        zero-based row group index
     * @param timestampColumnIndex column index of the designated timestamp
     * @return min timestamp value (epoch micros)
     */
    public long getRowGroupMinTimestamp(int rowGroupIndex, int timestampColumnIndex) {
        assert rowGroupIndex >= 0 && rowGroupIndex < rowGroupCount;
        assert timestampColumnIndex >= 0 && timestampColumnIndex < columnCount;
        long chunkAddr = columnChunkAddr(rowGroupIndex, timestampColumnIndex);
        return Unsafe.getUnsafe().getLong(chunkAddr + COLUMN_CHUNK_MIN_STAT_OFF);
    }

    /**
     * Returns the number of rows in the specified row group.
     *
     * @param rowGroupIndex zero-based row group index
     * @return NUM_ROWS (u64) from the row group block header
     */
    public long getRowGroupSize(int rowGroupIndex) {
        assert rowGroupIndex >= 0 && rowGroupIndex < rowGroupCount;
        return Unsafe.getUnsafe().getLong(rowGroupBlockAddr(rowGroupIndex));
    }

    /**
     * Alias for {@link #getDesignatedTimestampColumnIndex()}.
     */
    public int getTimestampIndex() {
        return getDesignatedTimestampColumnIndex();
    }

    /**
     * Returns the accumulated dead bytes in the parquet file tracked by the _pm footer.
     */
    public long getUnusedBytes() {
        return Unsafe.getUnsafe().getLong(footerAddr + FOOTER_UNUSED_BYTES_OFF);
    }

    public boolean isOpen() {
        return addr != 0;
    }

    /**
     * Initializes (or reinitializes) the reader with the given mmap address, file size,
     * and parquet file size (MVCC token from _txn field 3).
     * <p>
     * The footer is located via the header's footer_offset field. If the latest footer's
     * parquet file size exceeds parquetFileSize, the reader walks the prev_footer_offset
     * chain to find the matching footer for this _txn snapshot.
     * <p>
     * Calls {@link #clear()} first so that any previously allocated native
     * handle from a prior {@code of()} call is released before storing the
     * new state.
     *
     * @param addr            base address of the mmaped _pm file
     * @param fileSize        actual size of the mmaped _pm file in bytes
     * @param parquetFileSize parquet file size from _txn field 3, used as MVCC version token
     * @throws CairoException if the format version is unsupported or the file is too small
     */
    public void of(long addr, long fileSize, long parquetFileSize) {
        clear();
        if (fileSize < HEADER_FIXED_SIZE) {
            throw CairoException.critical(0)
                    .put("pm file too small [fileSize=").put(fileSize).put(']');
        }

        // Read footer_offset from the header. A loadFence ensures subsequent
        // reads of the footer data observe the bytes the writer committed
        // before patching this field.
        long footerOffset = Unsafe.getUnsafe().getLong(addr + HEADER_FOOTER_OFFSET_OFF);
        Unsafe.getUnsafe().loadFence();
        if (footerOffset < 0 || footerOffset >= fileSize) {
            throw CairoException.critical(0)
                    .put("invalid _pm header footer_offset [offset=").put(footerOffset)
                    .put(", fileSize=").put(fileSize)
                    .put(']');
        }

        // Walk the prev_footer_offset chain to find the footer matching parquetFileSize.
        long currentOffset = footerOffset;
        while (true) {
            long currentAddr = addr + currentOffset;
            long pqFooterOffset = Unsafe.getUnsafe().getLong(currentAddr + FOOTER_PARQUET_FOOTER_OFFSET_OFF);
            int pqFooterLength = Unsafe.getUnsafe().getInt(currentAddr + FOOTER_PARQUET_FOOTER_LENGTH_OFF);
            long derivedPqSize = pqFooterOffset + Integer.toUnsignedLong(pqFooterLength) + 8;
            if (derivedPqSize == parquetFileSize) {
                break;
            }
            long prevOffset = Unsafe.getUnsafe().getLong(currentAddr + FOOTER_PREV_FOOTER_OFFSET_OFF);
            if (prevOffset == 0) {
                throw CairoException.critical(0)
                        .put("no _pm footer found for parquet size [parquetFileSize=").put(parquetFileSize)
                        .put(']');
            }
            currentOffset = prevOffset;
        }

        // Use local variables for all validation. Fields are only assigned
        // at the very end so that a validation failure leaves isOpen()==false,
        // preventing double-munmap in callers that check isOpen() in catch blocks.
        long footerAddr = addr + currentOffset;
        int columnCount = Unsafe.getUnsafe().getInt(addr + HEADER_COLUMN_COUNT_OFF);
        long headerEndOffset = HEADER_FIXED_SIZE + (long) columnCount * COLUMN_DESCRIPTOR_SIZE;
        if (headerEndOffset > fileSize) {
            throw CairoException.critical(0)
                    .put("invalid _pm columnCount [count=").put(columnCount)
                    .put(", fileSize=").put(fileSize)
                    .put(']');
        }
        int rowGroupCount = Unsafe.getUnsafe().getInt(footerAddr + FOOTER_ROW_GROUP_COUNT_OFF);

        // Read footer_length from the trailer following this footer.
        // The trailer is at: footerAddr + footer_length_through_crc.
        // We find footer_length by reading it from the end of the footer region.
        // For a footer at currentOffset, its trailer is right after CRC.
        // We need to compute the end of this footer. Read the trailer: at
        // the end of this footer's data region.
        // Since the footer might not be at the end of the file (chain-walk),
        // we validate using the base footer size.
        final long baseFooterLength = FOOTER_FIXED_SIZE + (long) rowGroupCount * Integer.BYTES + Integer.BYTES;
        // Read the actual footer_length_through_crc from the trailer.
        long trailerAddr = footerAddr + baseFooterLength;
        // For footers with feature sections, the trailer is after the feature sections + CRC.
        // We use the approach of reading footer_length from the 4 bytes after the CRC.
        // But we don't know the CRC position without footer_length. Instead, for the
        // chain-walk case we must use the base footer check.
        long featureFlags = Unsafe.getUnsafe().getLong(addr + HEADER_FEATURE_FLAGS_OFF);
        long unknownRequired = featureFlags & REQUIRED_FEATURE_MASK;
        if (unknownRequired != 0) {
            throw CairoException.critical(0)
                    .put("unsupported required _pm feature flags [flags=0x")
                    .put(Long.toHexString(unknownRequired))
                    .put(']');
        }

        long rowCount = 0;
        for (int i = 0; i < rowGroupCount; i++) {
            long entryAddr = footerAddr + FOOTER_FIXED_SIZE + (long) i * 4;
            int stored = Unsafe.getUnsafe().getInt(entryAddr);
            rowCount += Unsafe.getUnsafe().getLong(addr + (Integer.toUnsignedLong(stored) << BLOCK_ALIGNMENT_SHIFT));
        }

        // All validations passed — commit state.
        this.addr = addr;
        this.fileSize = fileSize;
        this.footerAddr = footerAddr;
        this.columnCount = columnCount;
        this.rowGroupCount = rowGroupCount;
        this.totalRowCount = rowCount;
    }

    private static native boolean canSkipRowGroup0(
            long ptr,
            int rowGroupIndex,
            long filtersPtr,
            int filterCount,
            long filterBufEnd
    );

    private static native long createNativeReader(long addr, long fileSize);

    private static native void destroyNativeReader(long ptr);

    /**
     * Computes the absolute memory address of a column chunk within a row group block.
     * Column chunks start at offset 8 (after NUM_ROWS) and are 64 bytes each.
     */
    private long columnChunkAddr(int rowGroupIndex, int columnIndex) {
        return rowGroupBlockAddr(rowGroupIndex) + 8 + (long) columnIndex * COLUMN_CHUNK_SIZE;
    }

    /**
     * Computes the absolute memory address of a column descriptor in the header.
     * Descriptors start at offset 24 (after fixed header) and are 32 bytes each.
     */
    private long columnDescriptorAddr(int columnIndex) {
        assert columnIndex >= 0 && columnIndex < columnCount;
        return addr + HEADER_FIXED_SIZE + (long) columnIndex * COLUMN_DESCRIPTOR_SIZE;
    }

    /**
     * Computes the absolute memory address of a row group block.
     * Reads the footer entry for the given row group index and applies the <<3 shift.
     */
    private long rowGroupBlockAddr(int rowGroupIndex) {
        long entryAddr = footerAddr + FOOTER_FIXED_SIZE + (long) rowGroupIndex * 4;
        int stored = Unsafe.getUnsafe().getInt(entryAddr);
        return addr + (Integer.toUnsignedLong(stored) << BLOCK_ALIGNMENT_SHIFT);
    }

    static {
        Os.init();
    }
}
