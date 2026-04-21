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
import io.questdb.log.Log;
import io.questdb.std.DirectLongList;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.LPSZ;
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
 * HEADER (32 bytes fixed):
 *   [0]  PARQUET_META_FILE_SIZE  u64  (total committed file size; patched last as MVCC commit signal)
 *   [8]  FEATURE_FLAGS           u64
 *   [16] DESIGNATED_TS           i32
 *   [20] SORTING_COL_CNT         u32
 *   [24] COLUMN_COUNT            u32
 *   [28] RESERVED                u32
 *   [32..] column descriptors (32B each), sorting columns (4B each), name strings
 *   [..] header feature sections (if any feature flags set)
 *
 * ROW GROUP BLOCK (8-byte aligned, per row group):
 *   [0]  NUM_ROWS        u64
 *   [8..] column chunks (64B each), then optional out-of-line stats
 *
 * FOOTER (offset derived from trailer at PARQUET_META_FILE_SIZE - 4):
 *   [0]  PARQUET_FOOTER_OFFSET          u64
 *   [8]  PARQUET_FOOTER_LENGTH          u32
 *   [12] ROW_GROUP_COUNT                u32
 *   [16] UNUSED_BYTES                   u64
 *   [24] PREV_PARQUET_META_FILE_SIZE    u64  (0 if first snapshot; walk back via trailer at prev_size - 4)
 *   [32] FOOTER_FEATURE_FLAGS           u64  (per-footer flags; distinct from header FEATURE_FLAGS)
 *   [40..] row group entries (4B each, u32 block offset >> 3)
 *   [..]  feature sections (gated by header FEATURE_FLAGS or FOOTER_FEATURE_FLAGS)
 *   [..]  CRC32                         u32
 *   [..]  FOOTER_LENGTH                 u32  (total bytes from footer start through CRC)
 * </pre>
 * <p>
 * Callers must never read the `_pm` file size from the filesystem (via
 * {@code ff.length()} or similar). Instead, read the committed
 * {@code PARQUET_META_FILE_SIZE} via
 * {@link #readParquetMetaFileSize(FilesFacade, LPSZ)}, map that many
 * bytes, then call {@link #of(long, long, long)} passing the same size
 * as {@code parquetMetaFileSize}. The filesystem size may include bytes
 * from an in-progress, unpublished append and is not a valid commit
 * boundary — only {@code PARQUET_META_FILE_SIZE} is.
 */
public class ParquetMetaFileReader implements ParquetRowGroupSkipper, QuietCloseable {

    // Row group block offsets are stored right-shifted by this amount
    private static final int BLOCK_ALIGNMENT_SHIFT = 3;
    private static final int COLUMN_CHUNK_MAX_STAT_OFF = 56;
    private static final int COLUMN_CHUNK_MIN_STAT_OFF = 48;
    // Column chunk layout (64B per chunk, starting at row group block offset + 8)
    private static final int COLUMN_CHUNK_SIZE = 64;
    private static final int COLUMN_CHUNK_STAT_FLAGS_OFF = 2;
    private static final int COLUMN_DESCRIPTOR_SIZE = 32;
    // Column descriptor layout (32B each, starting at header offset 24)
    private static final int COL_DESC_COL_TYPE_OFF = 12;
    private static final int COL_DESC_ID_OFF = 8;
    private static final int COL_DESC_NAME_LENGTH_OFF = 24;
    private static final int COL_DESC_NAME_OFFSET_OFF = 0;
    private static final int FOOTER_FEATURE_FLAGS_OFF = 32;
    private static final int FOOTER_FIXED_SIZE = 40;
    private static final int FOOTER_PARQUET_FOOTER_LENGTH_OFF = 8;
    // Footer offsets (relative to footer start)
    private static final int FOOTER_PARQUET_FOOTER_OFFSET_OFF = 0;
    private static final int FOOTER_PREV_PARQUET_META_FILE_SIZE_OFF = 24;
    private static final int FOOTER_ROW_GROUP_COUNT_OFF = 12;
    // Footer trailer size (appended after CRC)
    private static final int FOOTER_TRAILER_SIZE = 4;
    private static final int FOOTER_UNUSED_BYTES_OFF = 16;
    private static final int HEADER_COLUMN_COUNT_OFF = 24;
    private static final int HEADER_DESIGNATED_TS_OFF = 16;
    private static final int HEADER_FEATURE_FLAGS_OFF = 8;
    // Stat flag bits within the column chunk stat_flags byte at COLUMN_CHUNK_STAT_FLAGS_OFF.
    // Layout mirrors the Rust writer (see parquet_metadata::types::StatFlags):
    //   bit 0 MIN_PRESENT, bit 1 MIN_INLINED, bit 2 MIN_EXACT,
    //   bit 3 MAX_PRESENT, bit 4 MAX_INLINED, bit 5 MAX_EXACT.
    // Reading the 8-byte inline stat at COLUMN_CHUNK_MIN_STAT_OFF / COLUMN_CHUNK_MAX_STAT_OFF is
    // only meaningful when both PRESENT and INLINED are set for that side.
    private static final int STAT_FLAG_MAX_INLINED = 1 << 4;
    private static final int STAT_FLAG_MAX_PRESENT = 1 << 3;
    private static final int STAT_FLAG_MIN_INLINED = 1 << 1;
    private static final int STAT_FLAG_MIN_PRESENT = 1;
    public static final int HEADER_FIXED_SIZE = 32;
    // Header offsets (layout: parquet_meta_file_size(8) + feature_flags(8) + dts(4) + sorting(4) + col_count(4) + reserved(4))
    public static final int HEADER_PARQUET_META_FILE_SIZE_OFF = 0;
    // Feature flag bits 32-63 are required: unknown bits must cause rejection.
    private static final long OPTIONAL_FEATURE_MASK = 0x0000_0000_FFFF_FFFFL;
    private static final long REQUIRED_FEATURE_MASK = 0xFFFF_FFFF_0000_0000L;
    private final DirectUtf8String flyweightColName = new DirectUtf8String();
    private long addr;
    private int columnCount;
    private long fileSize;
    private long footerAddr;
    // Lazily allocated native handle to a JniParquetMetaReader. Created on
    // the first canSkipRowGroup call and freed by clear()/close().
    private long nativeReaderPtr;
    private int rowGroupCount;
    private long totalRowCount;

    /**
     * Reads the committed {@code PARQUET_META_FILE_SIZE} field from a {@code _pm} file
     * without mapping the whole file. Callers that manage their own mapping
     * (e.g. via {@link io.questdb.cairo.vm.api.MemoryCMR}) use this to size the
     * full mapping. Never calls {@code ff.length()} — the filesystem size is
     * not a valid commit boundary.
     * <p>
     * Returns {@code -1} if the file is missing, unreadable, or the header
     * holds an implausible {@code parquet_meta_file_size} (too small to contain a
     * header + trailer). This mirrors {@link FilesFacade#length} semantics:
     * non-positive return means "can't be used", and callers use that as a
     * "regenerate" or "missing" signal with a plain {@code <= 0} check
     * instead of catching exceptions. Propagates unrecoverable errors
     * (e.g. native allocation failure) as-is.
     *
     * @param ff   files facade
     * @param path path to the {@code _pm} file
     * @return the committed {@code parquet_meta_file_size} stored in the header, or
     * {@code -1} if the file cannot be opened or yields an invalid value
     */
    public static long readParquetMetaFileSize(FilesFacade ff, LPSZ path) {
        final long fd = ff.openRO(path);
        if (fd < 0) {
            return -1;
        }
        try {
            final long scratch = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
            try {
                if (ff.read(fd, scratch, 8, 0) != 8) {
                    return -1;
                }
                final long parquetMetaFileSize = Unsafe.getUnsafe().getLong(scratch);
                Unsafe.getUnsafe().loadFence();
                if (parquetMetaFileSize < HEADER_FIXED_SIZE + FOOTER_TRAILER_SIZE) {
                    return -1;
                }
                return parquetMetaFileSize;
            } finally {
                Unsafe.free(scratch, 8, MemoryTag.NATIVE_DEFAULT);
            }
        } finally {
            ff.close(fd);
        }
    }

    /**
     * Maps a {@code _pm} file in two stages: first the fixed header prefix,
     * then the full committed file size read from {@code PARQUET_META_FILE_SIZE} at
     * offset 0. Never calls {@code ff.length()} — the filesystem size is not
     * a valid commit boundary.
     * <p>
     * The returned address is mmaped at the committed size ({@code PARQUET_META_FILE_SIZE}).
     * The caller must later {@code ff.munmap(addr, size, memoryTag)} with the
     * same size, which is available via {@link #getFileSize()} after calling
     * {@link #of(long, long)} on the address.
     *
     * @param ff        files facade
     * @param path      path to the {@code _pm} file (full, null-terminated)
     * @param log       logger for open-RO diagnostics
     * @param memoryTag memory tag bucket for the final mapping
     * @return the address of the mmaped {@code _pm} file, sized to the header's
     * {@code PARQUET_META_FILE_SIZE}
     * @throws CairoException if the file is missing, cannot be opened, or
     *                        contains an invalid {@code PARQUET_META_FILE_SIZE}
     */
    public static long mapParquetMeta(FilesFacade ff, LPSZ path, Log log, int memoryTag) {
        final long fd = TableUtils.openRO(ff, path, log);
        try {
            final long headerAddr = ff.mmap(fd, HEADER_FIXED_SIZE, 0, Files.MAP_RO, memoryTag);
            if (headerAddr == FilesFacade.MAP_FAILED) {
                throw CairoException.critical(ff.errno())
                        .put("could not mmap _pm header [file=").put(path).put(']');
            }
            final long parquetMetaFileSize;
            try {
                parquetMetaFileSize = Unsafe.getUnsafe().getLong(headerAddr + HEADER_PARQUET_META_FILE_SIZE_OFF);
                Unsafe.getUnsafe().loadFence();
            } finally {
                ff.munmap(headerAddr, HEADER_FIXED_SIZE, memoryTag);
            }
            if (parquetMetaFileSize < HEADER_FIXED_SIZE + FOOTER_TRAILER_SIZE) {
                throw CairoException.critical(0)
                        .put("invalid _pm parquet_meta_file_size [file=").put(path)
                        .put(", parquetMetaFileSize=").put(parquetMetaFileSize)
                        .put(']');
            }
            final long addr = ff.mmap(fd, parquetMetaFileSize, 0, Files.MAP_RO, memoryTag);
            if (addr == FilesFacade.MAP_FAILED) {
                throw CairoException.critical(ff.errno())
                        .put("could not mmap _pm [file=").put(path)
                        .put(", parquetMetaFileSize=").put(parquetMetaFileSize)
                        .put(']');
            }
            return addr;
        } finally {
            ff.close(fd);
        }
    }

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
     * Returns the native reader handle, allocating it lazily on first call.
     * The handle caches the parsed {@code _pm} header / footer / feature-flag
     * layout so repeated JNI calls (filter pruning AND row-group decode) avoid
     * reparsing. Freed by {@link #clear()} / {@link #close()}.
     */
    public long getOrCreateNativeReaderPtr() {
        if (nativeReaderPtr == 0) {
            nativeReaderPtr = createNativeReader(addr, fileSize);
        }
        return nativeReaderPtr;
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
     * <p>
     * Assumes the column chunk has both {@code MAX_PRESENT} and {@code MAX_INLINED}
     * bits set in its {@code stat_flags} byte. The designated-timestamp column is
     * always written this way by the QuestDB writer, so this is an invariant. The
     * assertion catches any violation in tests / CI (runs with {@code -ea}) without
     * imposing branch overhead in production.
     *
     * @param rowGroupIndex        zero-based row group index
     * @param timestampColumnIndex column index of the designated timestamp
     * @return max timestamp value (epoch micros)
     */
    public long getRowGroupMaxTimestamp(int rowGroupIndex, int timestampColumnIndex) {
        assert rowGroupIndex >= 0 && rowGroupIndex < rowGroupCount;
        assert timestampColumnIndex >= 0 && timestampColumnIndex < columnCount;
        long chunkAddr = columnChunkAddr(rowGroupIndex, timestampColumnIndex);
        assert (Unsafe.getUnsafe().getByte(chunkAddr + COLUMN_CHUNK_STAT_FLAGS_OFF) & (STAT_FLAG_MAX_PRESENT | STAT_FLAG_MAX_INLINED))
                == (STAT_FLAG_MAX_PRESENT | STAT_FLAG_MAX_INLINED)
                : "max_stat absent or not inlined for row group " + rowGroupIndex + ", column " + timestampColumnIndex;
        return Unsafe.getUnsafe().getLong(chunkAddr + COLUMN_CHUNK_MAX_STAT_OFF);
    }

    /**
     * Returns the minimum timestamp from the specified row group's column chunk.
     * Reads the inline min_stat (i64) at offset 48 within the column chunk.
     * <p>
     * Assumes the column chunk has both {@code MIN_PRESENT} and {@code MIN_INLINED}
     * bits set in its {@code stat_flags} byte. The designated-timestamp column is
     * always written this way by the QuestDB writer, so this is an invariant. The
     * assertion catches any violation in tests / CI (runs with {@code -ea}) without
     * imposing branch overhead in production.
     *
     * @param rowGroupIndex        zero-based row group index
     * @param timestampColumnIndex column index of the designated timestamp
     * @return min timestamp value (epoch micros)
     */
    public long getRowGroupMinTimestamp(int rowGroupIndex, int timestampColumnIndex) {
        assert rowGroupIndex >= 0 && rowGroupIndex < rowGroupCount;
        assert timestampColumnIndex >= 0 && timestampColumnIndex < columnCount;
        long chunkAddr = columnChunkAddr(rowGroupIndex, timestampColumnIndex);
        assert (Unsafe.getUnsafe().getByte(chunkAddr + COLUMN_CHUNK_STAT_FLAGS_OFF) & (STAT_FLAG_MIN_PRESENT | STAT_FLAG_MIN_INLINED))
                == (STAT_FLAG_MIN_PRESENT | STAT_FLAG_MIN_INLINED)
                : "min_stat absent or not inlined for row group " + rowGroupIndex + ", column " + timestampColumnIndex;
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
     * Initializes (or reinitializes) the reader over a {@code _pm} file
     * that has already been mmapped at its committed size.
     * {@code parquetMetaFileSize} is the anchor for the MVCC chain walk
     * and doubles as the upper bound on every dereference — the caller
     * must pass the size it actually mapped with, derived from
     * {@link #readParquetMetaFileSize}.
     * <p>
     * Locates the trailer at {@code parquetMetaFileSize - 4} and derives
     * the footer offset as
     * {@code parquetMetaFileSize - 4 - footer_length}. If the latest
     * footer's derived parquet file size does not equal
     * {@code parquetFileSize}, walks the MVCC chain via each footer's
     * {@code prev_parquet_meta_file_size} until a match is found; each
     * step re-applies the size-then-trailer indirection so its location
     * is re-validated through its own trailer.
     * <p>
     * Calls {@link #clear()} first so that any previously allocated
     * native handle from a prior {@code of()} call is released before
     * storing the new state.
     *
     * @param addr                 base address of the mmaped {@code _pm}
     *                             file
     * @param parquetMetaFileSize  size the caller mapped with; must equal
     *                             the committed {@code PARQUET_META_FILE_SIZE}
     *                             observed at map time
     * @param parquetFileSize      parquet file size from {@code _txn} field
     *                             3, used as MVCC version token; pass
     *                             {@link Long#MAX_VALUE} to disable MVCC
     *                             matching
     * @throws CairoException if the format is unsupported, corrupt, or no
     *                        footer matches {@code parquetFileSize}
     */
    public void of(long addr, long parquetMetaFileSize, long parquetFileSize) {
        clear();

        // The caller's parquetMetaFileSize is the anchor for the MVCC walk
        // and also the upper bound on every dereference. It must equal the
        // size the caller actually mapped — derived from its own read of
        // the committed PARQUET_META_FILE_SIZE via
        // readParquetMetaFileSize(). We do NOT re-read offset 0 here: a
        // newer value produced by a concurrent writer would correspond to
        // parquet file sizes newer than the caller's _txn snapshot
        // (parquetFileSize), so the MVCC walk would reject those footers
        // anyway, and trusting the re-read would open a TOCTOU window
        // whose dereferences would go out of the mapping.
        if (parquetMetaFileSize < HEADER_FIXED_SIZE + FOOTER_TRAILER_SIZE) {
            throw CairoException.critical(0)
                    .put("invalid _pm parquet_meta_file_size [parquetMetaFileSize=").put(parquetMetaFileSize).put(']');
        }

        // Walk the MVCC chain. Each step: read the trailer at `currentSize - 4`
        // to get the footer length, derive the footer offset, then check the
        // parquet file size. If it doesn't match, read
        // `prev_parquet_meta_file_size` from the current footer and repeat.
        //
        // Long.MAX_VALUE is a sentinel: use the latest footer without MVCC
        // matching. In practice K stays small because O3PartitionJob
        // triggers a full _pm rewrite when the unused-bytes ratio exceeds
        // the configured threshold (default 50%) or absolute unused bytes
        // exceed the limit (default 1 GB).
        long currentSize = parquetMetaFileSize;
        long currentOffset;
        long currentFooterLength;
        while (true) {
            currentFooterLength = Integer.toUnsignedLong(
                    Unsafe.getUnsafe().getInt(addr + currentSize - FOOTER_TRAILER_SIZE));
            currentOffset = currentSize - FOOTER_TRAILER_SIZE - currentFooterLength;
            if (currentOffset < HEADER_FIXED_SIZE || currentOffset >= currentSize) {
                throw CairoException.critical(0)
                        .put("invalid _pm footer offset [footerLength=").put(currentFooterLength)
                        .put(", parquetMetaFileSize=").put(currentSize)
                        .put(']');
            }
            if (parquetFileSize == Long.MAX_VALUE) {
                break;
            }

            long currentAddr = addr + currentOffset;
            long pqFooterOffset = Unsafe.getUnsafe().getLong(currentAddr + FOOTER_PARQUET_FOOTER_OFFSET_OFF);
            int pqFooterLength = Unsafe.getUnsafe().getInt(currentAddr + FOOTER_PARQUET_FOOTER_LENGTH_OFF);
            long derivedPqSize = pqFooterOffset + Integer.toUnsignedLong(pqFooterLength) + 8;
            if (derivedPqSize == parquetFileSize) {
                break;
            }
            long prevSize = Unsafe.getUnsafe().getLong(currentAddr + FOOTER_PREV_PARQUET_META_FILE_SIZE_OFF);
            if (prevSize <= 0 || prevSize >= currentSize) {
                throw CairoException.critical(CairoException.STALE_PARQUET_METADATA)
                        .put("no _pm footer found for parquet size [parquetFileSize=").put(parquetFileSize)
                        .put(']');
            }
            currentSize = prevSize;
        }

        // Use local variables for all validation. Fields are only assigned
        // at the very end so that a validation failure leaves isOpen()==false,
        // preventing double-munmap in callers that check isOpen() in catch blocks.
        long footerAddr = addr + currentOffset;
        int columnCount = Unsafe.getUnsafe().getInt(addr + HEADER_COLUMN_COUNT_OFF);
        long headerEndOffset = HEADER_FIXED_SIZE + (long) columnCount * COLUMN_DESCRIPTOR_SIZE;
        if (headerEndOffset > parquetMetaFileSize) {
            throw CairoException.critical(0)
                    .put("invalid _pm columnCount [count=").put(columnCount)
                    .put(", parquetMetaFileSize=").put(parquetMetaFileSize)
                    .put(']');
        }
        int rowGroupCount = Unsafe.getUnsafe().getInt(footerAddr + FOOTER_ROW_GROUP_COUNT_OFF);
        final long baseFooterLength = FOOTER_FIXED_SIZE + (long) rowGroupCount * Integer.BYTES + Integer.BYTES;
        if (currentOffset + baseFooterLength > parquetMetaFileSize) {
            throw CairoException.critical(0)
                    .put("invalid _pm footer length [rowGroupCount=").put(rowGroupCount)
                    .put(", footerOffset=").put(currentOffset)
                    .put(", parquetMetaFileSize=").put(parquetMetaFileSize)
                    .put(']');
        }
        long featureFlags = Unsafe.getUnsafe().getLong(addr + HEADER_FEATURE_FLAGS_OFF);
        long unknownRequired = featureFlags & REQUIRED_FEATURE_MASK;
        if (unknownRequired != 0) {
            throw CairoException.critical(0)
                    .put("unsupported required _pm feature flags [flags=0x")
                    .put(Long.toHexString(unknownRequired))
                    .put(']');
        }
        long footerFeatureFlags = Unsafe.getUnsafe().getLong(footerAddr + FOOTER_FEATURE_FLAGS_OFF);
        long unknownRequiredFooter = footerFeatureFlags & REQUIRED_FEATURE_MASK;
        if (unknownRequiredFooter != 0) {
            throw CairoException.critical(0)
                    .put("unsupported required _pm footer feature flags [flags=0x")
                    .put(Long.toHexString(unknownRequiredFooter))
                    .put(']');
        }

        // Cross-validate actual footer size from the selected footer's trailer
        // against the expected base size. Extra bytes without feature flags
        // to justify them indicate corruption. Both header and footer
        // optional flag bits can attach sections, so either set is enough.
        // The check applies to whichever footer the MVCC walk settled on —
        // each step reads its own trailer, so this covers every footer in
        // the chain.
        // baseFooterLength already includes CRC (Integer.BYTES at the end).
        // The trailer's footer_length covers from footer start through CRC.
        long knownOptionalFeatureFlags = featureFlags & OPTIONAL_FEATURE_MASK;
        long knownOptionalFooterFeatureFlags = footerFeatureFlags & OPTIONAL_FEATURE_MASK;
        if (knownOptionalFeatureFlags == 0
                && knownOptionalFooterFeatureFlags == 0
                && currentFooterLength != baseFooterLength) {
            throw CairoException.critical(0)
                    .put("unexpected _pm footer feature bytes [expected=").put(baseFooterLength)
                    .put(", actual=").put(currentFooterLength)
                    .put(']');
        }

        long minBlockSize = 8 + (long) columnCount * COLUMN_CHUNK_SIZE;
        long rowCount = 0;
        for (int i = 0; i < rowGroupCount; i++) {
            long entryAddr = footerAddr + FOOTER_FIXED_SIZE + (long) i * 4;
            int stored = Unsafe.getUnsafe().getInt(entryAddr);
            long blockOffset = Integer.toUnsignedLong(stored) << BLOCK_ALIGNMENT_SHIFT;
            if (blockOffset + minBlockSize > parquetMetaFileSize) {
                throw CairoException.critical(0)
                        .put("invalid _pm row group block offset [rowGroup=").put(i)
                        .put(", offset=").put(blockOffset)
                        .put(", parquetMetaFileSize=").put(parquetMetaFileSize)
                        .put(']');
            }
            rowCount += Unsafe.getUnsafe().getLong(addr + blockOffset);
        }

        // All validations passed — commit state.
        this.addr = addr;
        this.fileSize = parquetMetaFileSize;
        this.footerAddr = footerAddr;
        this.columnCount = columnCount;
        this.rowGroupCount = rowGroupCount;
        this.totalRowCount = rowCount;
    }

    /**
     * Writes the total row count (i64) at {@code destAddr} and the partition
     * squash tracker (i64) at {@code destAddr + 8}. The squash tracker is
     * {@code -1} when the {@code _pm} header has no {@code SQUASH_TRACKER}
     * feature section. Caller must provide a 16-byte buffer.
     * <p>
     * Enterprise callers use this to retrieve both values in a single JNI
     * round trip.
     *
     * @param destAddr address of a 16-byte buffer to receive the two longs
     * @throws CairoException on malformed {@code _pm} data
     */
    public void readPartitionMeta(long destAddr) {
        assert addr != 0;
        readPartitionMeta0(addr, fileSize, destAddr);
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

    private static native void readPartitionMeta0(long pmAddr, long pmSize, long destAddr);

    /**
     * Computes the absolute memory address of a column chunk within a row group block.
     * Column chunks start at offset 8 (after NUM_ROWS) and are 64 bytes each.
     */
    private long columnChunkAddr(int rowGroupIndex, int columnIndex) {
        return rowGroupBlockAddr(rowGroupIndex) + 8 + (long) columnIndex * COLUMN_CHUNK_SIZE;
    }

    /**
     * Computes the absolute memory address of a column descriptor in the header.
     * Descriptors start at offset 32 (after fixed header) and are 32 bytes each.
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
