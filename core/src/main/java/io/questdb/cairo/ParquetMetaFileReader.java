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
    private static final int COLUMN_CHUNK_MAX_STAT_OFF = 56;
    private static final int COLUMN_CHUNK_MIN_STAT_OFF = 48;
    // Column chunk layout (64B per chunk, starting at row group block offset + 8)
    private static final int COLUMN_CHUNK_SIZE = 64;
    private static final int COLUMN_CHUNK_STAT_FLAGS_OFF = 2;
    // Column descriptor layout (32B each, starting at header offset 24)
    private static final int COLUMN_DESCRIPTOR_SIZE = 32;
    private static final int COL_DESC_COL_TYPE_OFF = 12;
    private static final int COL_DESC_ID_OFF = 8;
    private static final int COL_DESC_NAME_LENGTH_OFF = 24;
    private static final int COL_DESC_NAME_OFFSET_OFF = 0;
    private static final int EXPECTED_FORMAT_VERSION = 1;
    private static final int FOOTER_FIXED_SIZE = 24;
    private static final int FOOTER_PARQUET_FOOTER_LENGTH_OFF = 8;
    // Footer offsets (relative to footer start)
    private static final int FOOTER_PARQUET_FOOTER_OFFSET_OFF = 0;
    private static final int FOOTER_ROW_GROUP_COUNT_OFF = 12;
    // Footer trailer size (appended after CRC)
    private static final int FOOTER_TRAILER_SIZE = 4;
    private static final int FOOTER_UNUSED_BYTES_OFF = 16;
    private static final int HEADER_COLUMN_COUNT_OFF = 20;
    private static final int HEADER_DESIGNATED_TS_OFF = 12;
    private static final int HEADER_FIXED_SIZE = 24;
    // Header offsets
    private static final int HEADER_FORMAT_VERSION_OFF = 0;
    private final DirectUtf8String flyweightColName = new DirectUtf8String();
    private long addr;
    private int columnCount;
    private long fileSize;
    private long footerAddr;
    // Lazily allocated native handle to a JniParquetMetaReader. Created on
    // the first canSkipRowGroup call and freed by clear()/close().
    private long nativeReaderPtr;
    private int rowGroupCount;

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
        close();
        this.addr = 0;
        this.fileSize = 0;
        this.footerAddr = 0;
        this.columnCount = 0;
        this.rowGroupCount = 0;
    }

    /**
     * Releases the lazily-allocated native reader handle, if any. Does not
     * touch the in-memory header/footer state — accessors keep returning
     * whatever was last loaded. This makes {@code close()} safe to call
     * defensively on long-lived field instances that get re-{@code of()}-ed
     * for new partitions.
     * <p>
     * Use {@link #clear()} to also wipe the in-memory state when fully
     * tearing down the reader.
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
        return Unsafe.getUnsafe().getLong(columnChunkAddr(rowGroupIndex, columnIndex) + COLUMN_CHUNK_MAX_STAT_OFF);
    }

    public long getChunkMinStat(int rowGroupIndex, int columnIndex) {
        return Unsafe.getUnsafe().getLong(columnChunkAddr(rowGroupIndex, columnIndex) + COLUMN_CHUNK_MIN_STAT_OFF);
    }

    public int getChunkStatFlags(int rowGroupIndex, int columnIndex) {
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

    // ── Column descriptor accessors ──────────────────────────────────────

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
     * Returns the total number of rows across all row groups.
     */
    public long getPartitionRowCount() {
        long total = 0;
        for (int i = 0; i < rowGroupCount; i++) {
            total += Unsafe.getUnsafe().getLong(rowGroupBlockAddr(i));
        }
        return total;
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
     * Initializes (or reinitializes) the reader with the given mmap address and file size.
     * The footer offset is derived from the 4-byte trailer at the end of the file.
     * <p>
     * Calls {@link #clear()} first so that any previously allocated native
     * handle from a prior {@code of()} call is released before storing the
     * new state.
     *
     * @param addr     base address of the mmaped _pm file
     * @param fileSize size of the mmaped file in bytes
     * @throws CairoException if the format version is unsupported or the file is too small
     */
    public void of(long addr, long fileSize) {
        clear();
        if (fileSize < FOOTER_TRAILER_SIZE + FOOTER_FIXED_SIZE) {
            throw CairoException.critical(0)
                    .put("pm file too small [fileSize=").put(fileSize).put(']');
        }

        // Read footer length from the trailer (last 4 bytes of the file).
        int footerLength = Unsafe.getUnsafe().getInt(addr + fileSize - FOOTER_TRAILER_SIZE);
        long footerOffset = fileSize - FOOTER_TRAILER_SIZE - Integer.toUnsignedLong(footerLength);
        if (footerOffset < 0 || footerOffset >= fileSize - FOOTER_TRAILER_SIZE) {
            throw CairoException.critical(0)
                    .put("invalid _pm footer offset [offset=").put(footerOffset)
                    .put(", fileSize=").put(fileSize)
                    .put(']');
        }

        int formatVersion = Unsafe.getUnsafe().getInt(addr + HEADER_FORMAT_VERSION_OFF);
        if (formatVersion != EXPECTED_FORMAT_VERSION) {
            throw CairoException.critical(0)
                    .put("unsupported _pm format version [version=").put(formatVersion)
                    .put(", expected=").put(EXPECTED_FORMAT_VERSION)
                    .put(']');
        }

        this.addr = addr;
        this.fileSize = fileSize;
        this.footerAddr = addr + footerOffset;
        this.columnCount = Unsafe.getUnsafe().getInt(addr + HEADER_COLUMN_COUNT_OFF);
        this.rowGroupCount = Unsafe.getUnsafe().getInt(this.footerAddr + FOOTER_ROW_GROUP_COUNT_OFF);

        final long baseFooterLength = FOOTER_FIXED_SIZE + (long) rowGroupCount * Integer.BYTES + Integer.BYTES;
        final long footerLengthUnsigned = Integer.toUnsignedLong(footerLength);
        if (footerLengthUnsigned < baseFooterLength) {
            throw CairoException.critical(0)
                    .put("invalid _pm footer length [footerLength=").put(footerLengthUnsigned)
                    .put(", min=").put(baseFooterLength)
                    .put(']');
        }
        final long footerExtra = footerLengthUnsigned - baseFooterLength;
        if (footerExtra != 0) {
            throw CairoException.critical(0)
                    .put("unexpected _pm footer feature bytes [bytes=").put(footerExtra)
                    .put(']');
        }
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
