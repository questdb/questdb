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

import io.questdb.std.Unsafe;

/**
 * Zero-allocation reader for _pm parquet metadata files.
 * Reads directly from mmaped memory via Unsafe offset arithmetic.
 * <p>
 * Binary format (little-endian):
 * <pre>
 * HEADER (16 bytes fixed):
 *   [0]  FORMAT_VERSION  u32
 *   [4]  DESIGNATED_TS   i32
 *   [8]  SORTING_COL_CNT u32
 *   [12] COLUMN_COUNT    u32
 *   [16..] column descriptors (32B each), sorting columns (4B each), name strings
 *
 * ROW GROUP BLOCK (8-byte aligned, per row group):
 *   [0]  NUM_ROWS        u64
 *   [8..] column chunks (64B each), then optional out-of-line stats
 *
 * FOOTER (offset derived from trailer at end of file):
 *   [0]  PARQUET_FOOTER_OFFSET u64
 *   [8]  PARQUET_FOOTER_LENGTH u32
 *   [12] ROW_GROUP_COUNT       u32
 *   [16..] row group entries (4B each, u32 block offset >> 3)
 *   [..]  CRC32                u32
 *   [..]  FOOTER_LENGTH        u32  (total bytes from footer start through CRC)
 * </pre>
 */
public class ParquetMetaFileReader {

    private static final int EXPECTED_FORMAT_VERSION = 1;

    // Header offsets
    private static final int HEADER_FORMAT_VERSION_OFF = 0;
    private static final int HEADER_DESIGNATED_TS_OFF = 4;
    private static final int HEADER_COLUMN_COUNT_OFF = 12;

    // Footer offsets (relative to footer start)
    private static final int FOOTER_PARQUET_FOOTER_OFFSET_OFF = 0;
    private static final int FOOTER_PARQUET_FOOTER_LENGTH_OFF = 8;
    private static final int FOOTER_FIXED_SIZE = 16;
    private static final int FOOTER_ROW_GROUP_COUNT_OFF = 12;

    // Footer trailer size (appended after CRC)
    private static final int FOOTER_TRAILER_SIZE = 4;

    // Row group block offsets are stored right-shifted by this amount
    private static final int BLOCK_ALIGNMENT_SHIFT = 3;

    // Column chunk layout (64B per chunk, starting at row group block offset + 8)
    private static final int COLUMN_CHUNK_SIZE = 64;
    private static final int COLUMN_CHUNK_MIN_STAT_OFF = 48;
    private static final int COLUMN_CHUNK_MAX_STAT_OFF = 56;

    private long addr;
    private long fileSize;
    private long footerAddr;
    private int columnCount;
    private int rowGroupCount;

    /**
     * Initializes (or reinitializes) the reader with the given mmap address and file size.
     * The footer offset is derived from the 4-byte trailer at the end of the file.
     *
     * @param addr     base address of the mmaped _pm file
     * @param fileSize size of the mmaped file in bytes
     * @throws CairoException if the format version is unsupported or the file is too small
     */
    public void of(long addr, long fileSize) {
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
    }

    public void clear() {
        this.addr = 0;
        this.fileSize = 0;
        this.footerAddr = 0;
        this.columnCount = 0;
        this.rowGroupCount = 0;
    }

    public long getAddr() {
        return addr;
    }

    public int getColumnCount() {
        return columnCount;
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
     * Returns the column index of the designated timestamp, or -1 if none.
     */
    public int getDesignatedTimestampColumnIndex() {
        return Unsafe.getUnsafe().getInt(addr + HEADER_DESIGNATED_TS_OFF);
    }

    public boolean isOpen() {
        return addr != 0;
    }

    /**
     * Computes the absolute memory address of a column chunk within a row group block.
     * Column chunks start at offset 8 (after NUM_ROWS) and are 64 bytes each.
     */
    private long columnChunkAddr(int rowGroupIndex, int columnIndex) {
        return rowGroupBlockAddr(rowGroupIndex) + 8 + (long) columnIndex * COLUMN_CHUNK_SIZE;
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
}
