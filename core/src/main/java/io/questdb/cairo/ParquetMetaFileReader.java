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
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;

/**
 * Zero-allocation flyweight reader for _pm parquet metadata files.
 * Reads directly from caller-owned mmaped memory via Unsafe offset arithmetic.
 * <p>
 * Implements {@link ParquetRowGroupSkipper} for filter-pushdown row group
 * pruning. The first call to {@link #canSkipRowGroup} lazily allocates a
 * native handle that caches the parsed {@code _pm} header/footer; the
 * handle is reused across all subsequent skip calls and freed by
 * {@link #clear()}.
 * <p>
 * <b>Ownership:</b> The reader does NOT own the underlying {@code _pm} mmap.
 * The caller mmaps the file (typically via {@link #openAndMapRO(FilesFacade, LPSZ, ParquetMetaFileReader)}
 * or its own {@link io.questdb.cairo.vm.api.MemoryCMR}), passes the address
 * to {@link #of(long, long)}, and is responsible for the matching
 * {@link FilesFacade#munmap}. {@link #clear()} only releases the lazily
 * allocated native handle and zeros the reader's fields; it does NOT
 * munmap.
 * <p>
 * <b>Lifecycle contract:</b> Callers MUST invoke {@link #clear()} BEFORE
 * munmapping the underlying {@code _pm} file. The native handle borrows
 * from the mmap and reading after unmap is undefined behaviour.
 * {@code ShowPartitionsRecordCursorFactory.closeParquetMeta()} is the
 * reference pattern: clear, then munmap.
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
 * Callers shouldn't read the `_pm` file size from the filesystem (via {@code ff.length()} or similar) when another
 * writer might modify the file simultaneously. Instead, read the committed {@code PARQUET_META_FILE_SIZE} via
 * {@link #readParquetMetaFileSize(FilesFacade, LPSZ)}, map that many bytes, then call {@link #of(long, long)} passing the same size
 * as {@code parquetMetaFileSize}. The filesystem size may include bytes from an in-progress, unpublished append and is not a valid commit
 * boundary -- only {@code PARQUET_META_FILE_SIZE} is. You may also use {@link #openAndMapRO(FilesFacade, LPSZ, ParquetMetaFileReader)} to
 * open and map the file in one call. Note that after calling it, you need to call {@link #resolveFooter(long)} before accessing any
 * fields other than {@code addr} or {@code fileSize}.
 */
public class ParquetMetaFileReader implements ParquetRowGroupSkipper {

    public static final int HEADER_FIXED_SIZE = 32;
    // Header offsets (layout: parquet_meta_file_size(8) + feature_flags(8) + dts(4) + sorting(4) + col_count(4) + reserved(4))
    public static final int HEADER_PARQUET_META_FILE_SIZE_OFF = 0;
    // Row group block offsets are stored right-shifted by this amount
    private static final int BLOCK_ALIGNMENT_SHIFT = 3;
    private static final int COLUMN_CHUNK_BYTE_RANGE_START_OFF = 16;
    private static final int COLUMN_CHUNK_MAX_STAT_OFF = 56;
    private static final int COLUMN_CHUNK_MIN_STAT_OFF = 48;
    // Column chunk layout (64B per chunk, starting at row group block offset + 8)
    private static final int COLUMN_CHUNK_SIZE = 64;
    private static final int COLUMN_CHUNK_STAT_FLAGS_OFF = 2;
    private static final int COLUMN_CHUNK_TOTAL_COMPRESSED_OFF = 24;
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
    // Feature flag bits 32-63 are required: unknown bits must cause rejection.
    private static final long OPTIONAL_FEATURE_MASK = 0x0000_0000_FFFF_FFFFL;
    // Trailing bytes after a parquet file's footer body: 4-byte footer length + 4-byte PAR1 magic.
    private static final int PARQUET_TRAILER_SIZE = 8;
    private static final long REQUIRED_FEATURE_MASK = 0xFFFF_FFFF_0000_0000L;
    // Each row group block starts with an 8-byte NUM_ROWS u64 prefix; column chunks follow.
    private static final int ROW_GROUP_BLOCK_HEADER_SIZE = 8;
    // Each row group entry in the footer is a 4-byte u32 (block offset >> BLOCK_ALIGNMENT_SHIFT).
    private static final int ROW_GROUP_ENTRY_SIZE = 4;
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
    private final DirectUtf8String flyweightColName = new DirectUtf8String();
    private long addr;
    // CRC32 verification result for the currently bound _pm mapping. Set
    // true after a successful verifyChecksum0 call so subsequent opens
    // (resolveFooter and onward) skip re-verification. Reset by clear().
    private boolean checksumVerified;
    private int columnCount;
    private long fileSize;
    private long footerAddr;
    // Lazily allocated native handle to a JniParquetMetaReader. Created on
    // the first canSkipRowGroup call and freed by clear().
    private long nativeReaderPtr;
    private int rowGroupCount;

    /**
     * Single-open helper: opens the {@code _pm} file once, reads the
     * committed {@code parquet_meta_file_size} at offset 0, mmaps the file
     * at that size, calls {@code reader.of(addr, size)}, and closes the fd
     * before returning. The mmap survives the fd close. This is a one-open
     * replacement for the {@link #readParquetMetaFileSize} plus
     * {@link TableUtils#mapRO} plus {@link #of} sequence, used on the
     * partition-scan path where the per-partition syscall count matters.
     * <p>
     * Returns the mmap address, or {@code 0} if the file is missing,
     * unreadable, or holds an implausible {@code parquet_meta_file_size}
     * (in which case the reader is left cleared via {@link #clear()}).
     * <p>
     * The caller owns the returned mapping. After use:
     * <ul>
     *   <li>capture {@link #getFileSize()} BEFORE calling {@link #clear()}
     *       (clear zeros the field);</li>
     *   <li>call {@link #clear()} to release the native handle and zero the
     *       reader's fields;</li>
     *   <li>call {@code ff.munmap(addr, size, MemoryTag.MMAP_PARQUET_METADATA_READER)}
     *       to release the mapping. {@code clear()} must run before munmap;
     *       the native handle borrows from the mapping.</li>
     * </ul>
     *
     * @param ff     files facade
     * @param path   path to the {@code _pm} file
     * @param reader reader to bind the mapping to
     * @return the mapping address, or {@code 0} if the file is missing/invalid
     * @throws CairoException if the header claims a size larger than the file
     */
    public static long openAndMapRO(FilesFacade ff, LPSZ path, ParquetMetaFileReader reader) {
        // The reader is left cleared on failure (no addr, no fileSize, no
        // native handle), so the caller can use the return value alone as
        // the success/failure signal without inspecting reader state.
        reader.clear();
        final long fd = ff.openRO(path);
        if (fd < 0) {
            return 0;
        }
        try {
            final long parquetMetaFileSize = ff.readNonNegativeLong(fd, 0);
            if (parquetMetaFileSize < HEADER_FIXED_SIZE + FOOTER_TRAILER_SIZE) {
                return 0;
            }
            // Reject header-claimed sizes that exceed the actual file length.
            // Mapping more bytes than the file holds and reading past EOF would
            // SIGBUS the JVM; corruption / partial write / stale-header bugs
            // must surface as a clear error rather than crash the process.
            final long actualFileSize = ff.length(fd);
            if (parquetMetaFileSize > actualFileSize) {
                throw CairoException.critical(0)
                        .put("invalid _pm parquet_meta_file_size exceeds file length [parquetMetaFileSize=")
                        .put(parquetMetaFileSize)
                        .put(", actualFileSize=").put(actualFileSize)
                        .put(", path=").put(path).put(']');
            }
            final long addr = TableUtils.mapRO(ff, fd, parquetMetaFileSize, MemoryTag.MMAP_PARQUET_METADATA_READER);
            try {
                reader.of(addr, parquetMetaFileSize);
            } catch (Throwable t) {
                ff.munmap(addr, parquetMetaFileSize, MemoryTag.MMAP_PARQUET_METADATA_READER);
                throw t;
            }
            return addr;
        } finally {
            ff.close(fd);
        }
    }

    /**
     * Reads the committed {@code PARQUET_META_FILE_SIZE} field from a {@code _pm} file
     * without mapping the whole file. Callers that manage their own mapping
     * (e.g. via {@link io.questdb.cairo.vm.api.MemoryCMR}) use this to size the
     * full mapping. Never calls {@code ff.length()} -- the filesystem size is
     * not a valid commit boundary.
     * <p>
     * Returns {@code -1} if the file is missing, unreadable, or the header
     * holds an implausible {@code parquet_meta_file_size} (too small to contain a
     * header + trailer). This mirrors {@link FilesFacade#length} semantics:
     * non-positive return means "can't be used", and callers use that as a
     * "regenerate" or "missing" signal with a plain {@code <= 0} check
     * instead of catching exceptions. Propagates unrecoverable errors
     * (e.g. native allocation failure) as-is.
     * <p>
     * Throws {@link CairoException} when the header-claimed size exceeds the
     * actual file length: callers would otherwise mmap past EOF and SIGBUS
     * the JVM on the first read of the trailing region.
     *
     * @param ff   files facade
     * @param path path to the {@code _pm} file
     * @return the committed {@code parquet_meta_file_size} stored in the header, or
     * {@code -1} if the file cannot be opened or yields an invalid value
     * @throws CairoException if the header claims a size larger than the file
     */
    public static long readParquetMetaFileSize(FilesFacade ff, LPSZ path) {
        final long fd = ff.openRO(path);
        if (fd < 0) {
            return -1;
        }
        try {
            final long parquetMetaFileSize = ff.readNonNegativeLong(fd, 0);
            if (parquetMetaFileSize < HEADER_FIXED_SIZE + FOOTER_TRAILER_SIZE) {
                return -1;
            }
            // Reject header-claimed sizes that exceed the actual file length.
            // Mapping more bytes than the file holds and reading past EOF would
            // SIGBUS the JVM; corruption / partial write / stale-header bugs
            // must surface as a clear error rather than crash the process.
            final long actualFileSize = ff.length(fd);
            if (parquetMetaFileSize > actualFileSize) {
                throw CairoException.critical(0)
                        .put("invalid _pm parquet_meta_file_size exceeds file length [parquetMetaFileSize=")
                        .put(parquetMetaFileSize)
                        .put(", actualFileSize=").put(actualFileSize)
                        .put(", path=").put(path).put(']');
            }
            return parquetMetaFileSize;
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
        this.checksumVerified = false;
    }

    public long getAddr() {
        return addr;
    }

    /**
     * Returns the absolute byte offset of the column chunk inside the
     * parquet file, as recorded in the {@code _pm} metadata.
     */
    public long getChunkByteRangeStart(int rowGroupIndex, int columnIndex) {
        assert rowGroupIndex >= 0 && rowGroupIndex < rowGroupCount;
        assert columnIndex >= 0 && columnIndex < columnCount;
        return Unsafe.getUnsafe().getLong(columnChunkAddr(rowGroupIndex, columnIndex) + COLUMN_CHUNK_BYTE_RANGE_START_OFF);
    }

    public long getChunkMaxStat(int rowGroupIndex, int columnIndex) {
        assert rowGroupIndex >= 0 && rowGroupIndex < rowGroupCount;
        assert columnIndex >= 0 && columnIndex < columnCount;
        long chunkAddr = columnChunkAddr(rowGroupIndex, columnIndex);
        assert (Unsafe.getByte(chunkAddr + COLUMN_CHUNK_STAT_FLAGS_OFF) & (STAT_FLAG_MAX_PRESENT | STAT_FLAG_MAX_INLINED))
                == (STAT_FLAG_MAX_PRESENT | STAT_FLAG_MAX_INLINED)
                : "max_stat absent or not inlined for row group " + rowGroupIndex + ", column " + columnIndex;
        return Unsafe.getLong(chunkAddr + COLUMN_CHUNK_MAX_STAT_OFF);
    }

    public long getChunkMinStat(int rowGroupIndex, int columnIndex) {
        assert rowGroupIndex >= 0 && rowGroupIndex < rowGroupCount;
        assert columnIndex >= 0 && columnIndex < columnCount;
        long chunkAddr = columnChunkAddr(rowGroupIndex, columnIndex);
        assert (Unsafe.getByte(chunkAddr + COLUMN_CHUNK_STAT_FLAGS_OFF) & (STAT_FLAG_MIN_PRESENT | STAT_FLAG_MIN_INLINED))
                == (STAT_FLAG_MIN_PRESENT | STAT_FLAG_MIN_INLINED)
                : "min_stat absent or not inlined for row group " + rowGroupIndex + ", column " + columnIndex;
        return Unsafe.getLong(chunkAddr + COLUMN_CHUNK_MIN_STAT_OFF);
    }

    public int getChunkStatFlags(int rowGroupIndex, int columnIndex) {
        assert rowGroupIndex >= 0 && rowGroupIndex < rowGroupCount;
        assert columnIndex >= 0 && columnIndex < columnCount;
        return Unsafe.getByte(columnChunkAddr(rowGroupIndex, columnIndex) + COLUMN_CHUNK_STAT_FLAGS_OFF) & 0xFF;
    }

    /**
     * Returns the compressed byte length of the column chunk, as recorded
     * in the {@code _pm} metadata.
     */
    public long getChunkTotalCompressed(int rowGroupIndex, int columnIndex) {
        assert rowGroupIndex >= 0 && rowGroupIndex < rowGroupCount;
        assert columnIndex >= 0 && columnIndex < columnCount;
        return Unsafe.getUnsafe().getLong(columnChunkAddr(rowGroupIndex, columnIndex) + COLUMN_CHUNK_TOTAL_COMPRESSED_OFF);
    }

    public int getColumnCount() {
        return columnCount;
    }

    public int getColumnId(int columnIndex) {
        return Unsafe.getInt(columnDescriptorAddr(columnIndex) + COL_DESC_ID_OFF);
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
        long nameOffset = Unsafe.getLong(descAddr + COL_DESC_NAME_OFFSET_OFF);
        int nameLength = Unsafe.getInt(descAddr + COL_DESC_NAME_LENGTH_OFF);
        long nameAddr = addr + nameOffset;
        return flyweightColName.of(nameAddr, nameAddr + nameLength, true);
    }

    public int getColumnType(int columnIndex) {
        return Unsafe.getInt(columnDescriptorAddr(columnIndex) + COL_DESC_COL_TYPE_OFF);
    }

    /**
     * Returns the column index of the designated timestamp, or -1 if none.
     */
    public int getDesignatedTimestampColumnIndex() {
        return Unsafe.getInt(addr + HEADER_DESIGNATED_TS_OFF);
    }

    public long getFileSize() {
        return fileSize;
    }

    /**
     * Returns the native reader handle, allocating it lazily on first call.
     * The handle caches the parsed {@code _pm} header / footer / feature-flag
     * layout so repeated JNI calls (filter pruning AND row-group decode) avoid
     * reparsing. Freed by {@link #clear()}.
     */
    public long getOrCreateNativeReaderPtr() {
        if (nativeReaderPtr == 0) {
            nativeReaderPtr = createNativeReader(addr, fileSize);
        }
        return nativeReaderPtr;
    }

    /**
     * Derives the parquet file size from the _pm footer metadata.
     * parquetFileSize = PARQUET_FOOTER_OFFSET + PARQUET_FOOTER_LENGTH + PARQUET_TRAILER_SIZE
     * (4B parquet footer length field + 4B PAR1 magic)
     */
    public long getParquetFileSize() {
        long parquetFooterOffset = Unsafe.getLong(footerAddr + FOOTER_PARQUET_FOOTER_OFFSET_OFF);
        int parquetFooterLength = Unsafe.getInt(footerAddr + FOOTER_PARQUET_FOOTER_LENGTH_OFF);
        return parquetFooterOffset + Integer.toUnsignedLong(parquetFooterLength) + PARQUET_TRAILER_SIZE;
    }

    /**
     * Returns the total number of rows across all row groups, summed on
     * demand. Cost is O(rowGroupCount); called on the parquet attach path
     * only. Requires {@link #resolveFooter(long)} to have run first.
     */
    public long getPartitionRowCount() {
        long total = 0;
        for (int i = 0; i < rowGroupCount; i++) {
            total += Unsafe.getLong(rowGroupBlockAddr(i));
        }
        return total;
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
        assert (Unsafe.getByte(chunkAddr + COLUMN_CHUNK_STAT_FLAGS_OFF) & (STAT_FLAG_MAX_PRESENT | STAT_FLAG_MAX_INLINED))
                == (STAT_FLAG_MAX_PRESENT | STAT_FLAG_MAX_INLINED)
                : "max_stat absent or not inlined for row group " + rowGroupIndex + ", column " + timestampColumnIndex;
        return Unsafe.getLong(chunkAddr + COLUMN_CHUNK_MAX_STAT_OFF);
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
        assert (Unsafe.getByte(chunkAddr + COLUMN_CHUNK_STAT_FLAGS_OFF) & (STAT_FLAG_MIN_PRESENT | STAT_FLAG_MIN_INLINED))
                == (STAT_FLAG_MIN_PRESENT | STAT_FLAG_MIN_INLINED)
                : "min_stat absent or not inlined for row group " + rowGroupIndex + ", column " + timestampColumnIndex;
        return Unsafe.getLong(chunkAddr + COLUMN_CHUNK_MIN_STAT_OFF);
    }

    /**
     * Returns the number of rows in the specified row group.
     *
     * @param rowGroupIndex zero-based row group index
     * @return NUM_ROWS (u64) from the row group block header
     */
    public long getRowGroupSize(int rowGroupIndex) {
        assert rowGroupIndex >= 0 && rowGroupIndex < rowGroupCount;
        return Unsafe.getLong(rowGroupBlockAddr(rowGroupIndex));
    }

    /**
     * Returns the accumulated dead bytes in the parquet file tracked by the _pm footer.
     */
    public long getUnusedBytes() {
        return Unsafe.getLong(footerAddr + FOOTER_UNUSED_BYTES_OFF);
    }

    public boolean isOpen() {
        return addr != 0 && footerAddr != 0;
    }

    /**
     * Binds the reader to a {@code _pm} file already mmapped at its committed
     * size. Pure setter: stores {@code addr} and {@code parquetMetaFileSize}
     * after a sanity check, clears any prior state, and returns. Does not
     * walk the MVCC chain and does not validate the footer.
     * <p>
     * Callers must follow with {@link #resolveFooter(long)} before using any
     * accessor other than {@link #getAddr()} / {@link #getFileSize()};
     * {@link #isOpen()} stays {@code false} until the footer is resolved.
     *
     * @param addr                base address of the mmaped {@code _pm}
     *                            file
     * @param parquetMetaFileSize size the caller mapped with; must equal
     *                            the committed {@code PARQUET_META_FILE_SIZE}
     *                            observed at map time
     * @throws CairoException if {@code parquetMetaFileSize} is too small to
     *                        contain a header + trailer
     */
    public void of(long addr, long parquetMetaFileSize) {
        clear();
        // Reject a null mapping address up front. Subsequent Unsafe reads
        // would dereference offsets from a null base pointer (e.g.
        // resolveFooter reads addr + currentSize - 4) and SIGSEGV the
        // JVM. Callers that pass addr == 0 typically forgot to check
        // openAndMapRO's return code.
        if (addr == 0) {
            throw CairoException.critical(0)
                    .put("invalid _pm mapping address [addr=0]");
        }
        if (parquetMetaFileSize < HEADER_FIXED_SIZE + FOOTER_TRAILER_SIZE) {
            throw CairoException.critical(0)
                    .put("invalid _pm parquet_meta_file_size [parquetMetaFileSize=").put(parquetMetaFileSize).put(']');
        }
        this.addr = addr;
        this.fileSize = parquetMetaFileSize;
    }

    /**
     * Shallow-copies the resolved state of {@code other} into this reader
     * without re-walking the MVCC chain or re-validating the layout. The
     * underlying mmap is NOT copied — this reader borrows the same
     * {@code addr} and must not outlive the mapping {@code other} points
     * at. The native handle is not shared; it will be lazily allocated on
     * first use (freed independently by {@link #clear()}).
     *
     * @param other a reader whose state has already been resolved via
     *              {@link #resolveFooter(long)}
     */
    public void of(ParquetMetaFileReader other) {
        clear();
        this.addr = other.addr;
        this.fileSize = other.fileSize;
        this.footerAddr = other.footerAddr;
        this.columnCount = other.columnCount;
        this.rowGroupCount = other.rowGroupCount;
        this.checksumVerified = other.checksumVerified;
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

    /**
     * Locates the footer that matches {@code parquetFileSize} via the MVCC
     * chain walk, validates the header and footer, and populates
     * {@code footerAddr}, {@code columnCount}, and {@code rowGroupCount}.
     * Must be called after {@link #of(long, long)} and before any accessor
     * beyond {@link #getAddr()} / {@link #getFileSize()}.
     * <p>
     * Starting from {@code fileSize}, reads the trailer at
     * {@code currentSize - 4} to derive the footer offset. If the latest
     * footer's derived parquet file size does not equal
     * {@code parquetFileSize}, walks back via each footer's
     * {@code prev_parquet_meta_file_size}; each step re-applies the
     * size-then-trailer indirection so its location is re-validated through
     * its own trailer.
     *
     * @param parquetFileSize parquet file size from {@code _txn} field 3,
     *                        used as MVCC version token; pass
     *                        {@link Long#MAX_VALUE} to take the latest
     *                        footer without MVCC matching
     * @return false if the parquet footer couldn't be found
     * @throws CairoException if the format is unsupported or corrupt
     */
    public boolean resolveFooter(long parquetFileSize) {
        final long addr = this.addr;
        final long parquetMetaFileSize = this.fileSize;

        // Verify the CRC32 once per open before trusting any byte of the
        // file. Single-bit disk rot or RAM corruption otherwise passes the
        // structural bound checks and is served as authoritative metadata
        // steering SQL row-group pruning and cold-storage byte-range reads.
        // The check parses the file once; the cached flag stops re-verifying
        // on subsequent canSkipRowGroup calls. verifyChecksum0 throws
        // CairoException on mismatch, null pointer, or unparseable file.
        if (!checksumVerified) {
            verifyChecksum0(addr, parquetMetaFileSize);
            checksumVerified = true;
        }

        // Walk the MVCC chain. Each step: read the trailer at
        // `currentSize - 4` to get the footer length, derive the footer
        // offset, then check the parquet file size. If it doesn't match,
        // read `prev_parquet_meta_file_size` from the current footer and
        // repeat.
        //
        // Long.MAX_VALUE is a sentinel: use the latest footer without
        // MVCC matching. In practice K stays small because
        // O3PartitionJob triggers a full _pm rewrite when the
        // unused-bytes ratio exceeds the configured threshold (default
        // 50%) or absolute unused bytes exceed the limit (default 1 GB).
        long currentSize = parquetMetaFileSize;
        long currentOffset;
        long currentFooterLength;
        while (true) {
            currentFooterLength = Integer.toUnsignedLong(
                    Unsafe.getInt(addr + currentSize - FOOTER_TRAILER_SIZE));
            currentOffset = currentSize - FOOTER_TRAILER_SIZE - currentFooterLength;
            // Bound checks use currentSize (the snapshot's committed size)
            // not parquetMetaFileSize (the mapping size). Intermediate
            // footers in the MVCC chain only own bytes up to their own
            // currentSize; using the mapping size would let a corrupted
            // footer steer reads into a later snapshot's bytes. The
            // FOOTER_FIXED_SIZE guard ensures the subsequent fixed-field
            // reads (parquet footer offset/length, prev_size, footer
            // feature flags) all fall within currentSize.
            if (currentOffset < HEADER_FIXED_SIZE
                    || currentOffset + FOOTER_FIXED_SIZE > currentSize) {
                throw CairoException.critical(0)
                        .put("invalid _pm footer offset [footerLength=").put(currentFooterLength)
                        .put(", parquetMetaFileSize=").put(currentSize)
                        .put(']');
            }
            if (parquetFileSize == Long.MAX_VALUE) {
                break;
            }

            long currentAddr = addr + currentOffset;
            long pqFooterOffset = Unsafe.getLong(currentAddr + FOOTER_PARQUET_FOOTER_OFFSET_OFF);
            int pqFooterLength = Unsafe.getInt(currentAddr + FOOTER_PARQUET_FOOTER_LENGTH_OFF);
            long derivedPqSize = pqFooterOffset + Integer.toUnsignedLong(pqFooterLength) + PARQUET_TRAILER_SIZE;
            if (derivedPqSize == parquetFileSize) {
                break;
            }
            long prevSize = Unsafe.getLong(currentAddr + FOOTER_PREV_PARQUET_META_FILE_SIZE_OFF);
            // Reject prevSize values too small to hold a header +
            // trailer. The next iteration dereferences addr + prevSize
            // - FOOTER_TRAILER_SIZE, which for prevSize <
            // FOOTER_TRAILER_SIZE would land before the mapping start.
            if (prevSize < HEADER_FIXED_SIZE + FOOTER_TRAILER_SIZE || prevSize >= currentSize) {
                return false;
            }
            currentSize = prevSize;
        }

        // Use local variables for all validation. Fields are only
        // assigned at the very end so that a validation failure leaves
        // isOpen()==false, preventing double-munmap in callers that
        // check isOpen() in catch blocks.
        //
        // Read columnCount / rowGroupCount as u32 (writer emits Rust u32).
        // A signed-int interpretation lets bit-31-set values become negative,
        // making the (long) columnCount * COLUMN_DESCRIPTOR_SIZE arithmetic
        // negative and silently passing the headerEndOffset bound check;
        // downstream reads would then land at attacker-controlled offsets.
        // Reject values that overflow Java int because they cannot be stored
        // in this reader's int fields and would overflow downstream sizing
        // arithmetic.
        long footerAddr = addr + currentOffset;
        long columnCountLong = Integer.toUnsignedLong(Unsafe.getInt(addr + HEADER_COLUMN_COUNT_OFF));
        if (columnCountLong > Integer.MAX_VALUE) {
            throw CairoException.critical(0)
                    .put("invalid _pm columnCount [count=").put(columnCountLong)
                    .put(", parquetMetaFileSize=").put(parquetMetaFileSize)
                    .put(']');
        }
        int columnCount = (int) columnCountLong;
        long headerEndOffset = HEADER_FIXED_SIZE + columnCountLong * COLUMN_DESCRIPTOR_SIZE;
        if (headerEndOffset > parquetMetaFileSize) {
            throw CairoException.critical(0)
                    .put("invalid _pm columnCount [count=").put(columnCount)
                    .put(", parquetMetaFileSize=").put(parquetMetaFileSize)
                    .put(']');
        }
        long rowGroupCountLong = Integer.toUnsignedLong(Unsafe.getInt(footerAddr + FOOTER_ROW_GROUP_COUNT_OFF));
        if (rowGroupCountLong > Integer.MAX_VALUE) {
            throw CairoException.critical(0)
                    .put("invalid _pm rowGroupCount [count=").put(rowGroupCountLong)
                    .put(", footerOffset=").put(currentOffset)
                    .put(", parquetMetaFileSize=").put(parquetMetaFileSize)
                    .put(']');
        }
        int rowGroupCount = (int) rowGroupCountLong;
        final long baseFooterLength = FOOTER_FIXED_SIZE + rowGroupCountLong * ROW_GROUP_ENTRY_SIZE + Integer.BYTES;
        if (currentOffset + baseFooterLength > parquetMetaFileSize) {
            throw CairoException.critical(0)
                    .put("invalid _pm footer length [rowGroupCount=").put(rowGroupCount)
                    .put(", footerOffset=").put(currentOffset)
                    .put(", parquetMetaFileSize=").put(parquetMetaFileSize)
                    .put(']');
        }
        // Validate designated timestamp column index. The writer emits -1
        // when no DTS is set, otherwise a 0-based column index. Any other
        // value (e.g. a sentinel from corruption or a stale-format file)
        // would feed downstream readers an out-of-range index that
        // silently aliases unrelated columns or reads past the descriptor
        // table.
        int dtsIndex = Unsafe.getInt(addr + HEADER_DESIGNATED_TS_OFF);
        if (dtsIndex < -1 || dtsIndex >= columnCount) {
            throw CairoException.critical(0)
                    .put("invalid _pm designated timestamp column index [dtsIndex=").put(dtsIndex)
                    .put(", columnCount=").put(columnCount)
                    .put(']');
        }
        long featureFlags = Unsafe.getLong(addr + HEADER_FEATURE_FLAGS_OFF);
        long unknownRequired = featureFlags & REQUIRED_FEATURE_MASK;
        if (unknownRequired != 0) {
            throw CairoException.critical(0)
                    .put("unsupported required _pm feature flags [flags=0x")
                    .put(Long.toHexString(unknownRequired))
                    .put(']');
        }
        long footerFeatureFlags = Unsafe.getLong(footerAddr + FOOTER_FEATURE_FLAGS_OFF);
        long unknownRequiredFooter = footerFeatureFlags & REQUIRED_FEATURE_MASK;
        if (unknownRequiredFooter != 0) {
            throw CairoException.critical(0)
                    .put("unsupported required _pm footer feature flags [flags=0x")
                    .put(Long.toHexString(unknownRequiredFooter))
                    .put(']');
        }

        // Cross-validate actual footer size from the selected footer's
        // trailer against the expected base size. Extra bytes without
        // feature flags to justify them indicate corruption. Both
        // header and footer optional flag bits can attach sections, so
        // either set is enough. The check applies to whichever footer
        // the MVCC walk settled on — each step reads its own trailer,
        // so this covers every footer in the chain. baseFooterLength
        // already includes CRC (Integer.BYTES at the end). The
        // trailer's footer_length covers from footer start through CRC.
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

        long minBlockSize = ROW_GROUP_BLOCK_HEADER_SIZE + (long) columnCount * COLUMN_CHUNK_SIZE;
        for (int i = 0; i < rowGroupCount; i++) {
            long entryAddr = footerAddr + FOOTER_FIXED_SIZE + (long) i * ROW_GROUP_ENTRY_SIZE;
            int stored = Unsafe.getInt(entryAddr);
            long blockOffset = Integer.toUnsignedLong(stored) << BLOCK_ALIGNMENT_SHIFT;
            if (blockOffset + minBlockSize > parquetMetaFileSize) {
                throw CairoException.critical(0)
                        .put("invalid _pm row group block offset [rowGroup=").put(i)
                        .put(", offset=").put(blockOffset)
                        .put(", parquetMetaFileSize=").put(parquetMetaFileSize)
                        .put(']');
            }
        }

        // Validate column descriptor name pointers. Each descriptor stores a
        // (nameOffset, nameLength) pair pointing into the same mapping.
        // Without validation, getColumnName would build a flyweight over an
        // arbitrary memory region (UB) and even nameOffset+nameLength could
        // wrap. Reject offsets that land in the fixed header / column
        // descriptor area, and pairs whose end exceeds the committed size.
        // nameLength is read as int and treated as unsigned.
        for (int i = 0; i < columnCount; i++) {
            long descAddr = addr + HEADER_FIXED_SIZE + (long) i * COLUMN_DESCRIPTOR_SIZE;
            long nameOffset = Unsafe.getLong(descAddr + COL_DESC_NAME_OFFSET_OFF);
            long nameLength = Integer.toUnsignedLong(Unsafe.getInt(descAddr + COL_DESC_NAME_LENGTH_OFF));
            // nameOffset is signed long from disk; reject negatives explicitly.
            // nameOffset + nameLength can overflow when nameOffset is large;
            // compare nameLength against the remaining space instead of
            // computing the sum.
            if (nameOffset < headerEndOffset
                    || nameOffset > parquetMetaFileSize
                    || nameLength > parquetMetaFileSize - nameOffset) {
                throw CairoException.critical(0)
                        .put("invalid _pm column name pointer [columnIndex=").put(i)
                        .put(", nameOffset=").put(nameOffset)
                        .put(", nameLength=").put(nameLength)
                        .put(", headerEndOffset=").put(headerEndOffset)
                        .put(", parquetMetaFileSize=").put(parquetMetaFileSize)
                        .put(']');
            }
        }

        // All validations passed — commit state.
        this.footerAddr = footerAddr;
        this.columnCount = columnCount;
        this.rowGroupCount = rowGroupCount;
        return true;
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

    private static native void readPartitionMeta0(long parquetMetaAddr, long parquetMetaSize, long destAddr);

    /**
     * Verifies the CRC32 stored in the {@code _pm} footer.
     * Throws {@link CairoException} on mismatch, null pointer, or any
     * structural error encountered while parsing the file.
     */
    private static native void verifyChecksum0(long addr, long fileSize);

    /**
     * Computes the absolute memory address of a column chunk within a row group block.
     * Column chunks start after the row group block header (NUM_ROWS) and are 64 bytes each.
     */
    private long columnChunkAddr(int rowGroupIndex, int columnIndex) {
        return rowGroupBlockAddr(rowGroupIndex) + ROW_GROUP_BLOCK_HEADER_SIZE + (long) columnIndex * COLUMN_CHUNK_SIZE;
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
        long entryAddr = footerAddr + FOOTER_FIXED_SIZE + (long) rowGroupIndex * ROW_GROUP_ENTRY_SIZE;
        int stored = Unsafe.getInt(entryAddr);
        return addr + (Integer.toUnsignedLong(stored) << BLOCK_ALIGNMENT_SHIFT);
    }

    static {
        Os.init();
    }
}
