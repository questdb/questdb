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

import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Zip;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.LPSZ;

/**
 * Memory-mapped reader for the {@code _im} covering-index metadata file,
 * format version 3, the sidecar to {@code <col>.pidx.parquet}. It mirrors the
 * Rust {@code IndexMetaReader} in {@code qdb-parquet-meta}: the two
 * implementations validate the same fields in the same order and resolve the
 * same section offsets, and must stay in lock step.
 * <p>
 * {@code _im} deliberately reuses {@code _pm}'s 32-byte column descriptor,
 * row group block and 64-byte column chunk structures byte for byte, so the
 * descriptor and chunk field offsets below are
 * {@link ParquetMetaFileReader}'s, unchanged. Only the header and the
 * index-specific sections differ.
 * <p>
 * Binary format (little-endian):
 * <pre>
 * HEADER (128 bytes fixed):
 *   [0]  IM_FILE_SIZE          u64  (total committed file size; patched last as the commit signal,
 *                                    and the only field outside the CRC)
 *   [8]  IM_MAGIC              u64  (0x0300584449424451, the bytes QDBIDX\0\3)
 *   [16] FEATURE_FLAGS         u64  (bits 32-63 are required: unknown bits must cause rejection)
 *   [24] FORMAT_VERSION        u32  (3)
 *   [28] PAYLOAD_KIND          u32  (0 = row per posting, 1 = row per key)
 *   [32] COLUMN_COUNT          u32
 *   [36] INDEX_RG_COUNT        u32
 *   [40] DATA_RG_COUNT         u32
 *   [44] KEY_SPACE_SIZE        u32  (exclusive upper bound on key ids, not a distinct-key count)
 *   [48] KEY_ID_COLUMN         i32
 *   [52] ROW_ID_COLUMN         i32  (-1 under PAYLOAD_KIND 1)
 *   [56] INDEX_SECTIONS_OFFSET u64  (absolute offset of RG_BLOCK_OFFSET, 8-byte aligned)
 *   [64] PIDX_FOOTER_OFFSET    u64  (where the index parquet's own footer starts)
 *   [72] PIDX_FOOTER_LENGTH    u32  (length of that footer)
 *   [76] FIRST_COVER_COLUMN    u32  (descriptor index of cover slot 0)
 *   [80] RESERVED              48B  (zero means "absent", so a later writer may spend it)
 *
 *   [128..] column descriptors (32B each), then the UTF-8 name blob padded to 8 bytes
 *
 * ROW GROUP BLOCK (8-byte aligned, one per index row group, located via RG_BLOCK_OFFSET):
 *   [0]  NUM_ROWS  u64
 *   [8..] column chunks (64B each), then the out-of-line stat region
 *
 * INDEX SECTIONS (at INDEX_SECTIONS_OFFSET, each 8-byte aligned and padded up):
 *   RG_BLOCK_OFFSET   u32 x INDEX_RG_COUNT        block byte offset from file start, &gt;&gt; 3
 *   RG_FIRST_KEY      u32 x (INDEX_RG_COUNT + 1)  smallest key id per row group, plus a
 *                                                 KEY_SPACE_SIZE sentinel
 *   RG_ROW_ID_MIN     i64 x INDEX_RG_COUNT        smallest row id per row group
 *   RG_ROW_ID_MAX     i64 x INDEX_RG_COUNT        largest row id per row group
 *   DATA_RG_BOUNDARY  i64 x (DATA_RG_COUNT + 1)   cumulative data.parquet row counts
 *   CRC32             u32  over [8, IM_FILE_SIZE - 4)
 * </pre>
 * <p>
 * The row-id zone maps are unconditional: under {@code PAYLOAD_KIND 1} there
 * is no {@code row_id} column at all, so a reader taking the range from that
 * column's chunk statistics would have no time pruning for that payload.
 * <p>
 * A query's required cover columns are cover slots - ordinals into this
 * index's own {@code INCLUDE} list - not writer indices, and
 * {@link #getCoverColumnIndex(int)} is the only correct way to resolve one.
 * {@link #getColumnIndexById(int)} resolves a writer index instead; the two
 * spaces are different and confusing them silently reads the wrong column.
 * <p>
 * {@code INDEX_SECTIONS_OFFSET} is read from the header, never derived: a
 * row group block's size depends on the length of its out-of-line stat
 * region, which is recorded nowhere, and deriving the offset backwards from
 * the CRC would give the two reader implementations two chains of inferences
 * to keep in step instead of one value to compare.
 * <p>
 * <b>Ownership:</b> {@link #openAndMapRO(FilesFacade, LPSZ, IndexMetaFileReader)}
 * leaves the reader owning the mapping, released by {@link #close()}; the file
 * descriptor is closed before that method returns. {@link #ofAddress(long, long)}
 * binds to a buffer the caller owns; {@link #close()} then only zeroes the
 * reader's fields.
 * <p>
 * <b>Thread safety:</b> not thread-safe per instance.
 * <p>
 * Callers must not size the mapping from the filesystem (via
 * {@code ff.length()} or similar): only the committed {@code IM_FILE_SIZE} in
 * the header is a valid commit boundary, and the filesystem length may include
 * bytes of an in-progress, unpublished append.
 */
public class IndexMetaFileReader implements QuietCloseable {

    public static final int IM_HEADER_SIZE = 128;
    public static final int IM_TRAILER_SIZE = 4;
    /**
     * {@link #getRowGroupRangeForKey(int)} for a key outside the covered key
     * space. Both packed bounds decode to {@code -1}, which is what
     * {@link #getRowGroupLoForKey(int)} and {@link #getRowGroupHiForKey(int)}
     * answer for such a key.
     */
    public static final long KEY_ABSENT = -1L;
    // Index sections and row group blocks start on this boundary, which is what
    // lets RG_BLOCK_OFFSET store a byte offset right-shifted by 3 in a u32.
    private static final int BLOCK_ALIGNMENT = 8;
    private static final int BLOCK_ALIGNMENT_SHIFT = 3;
    // Column chunk layout (64B per chunk, starting at row group block offset + 8),
    // identical to _pm's; see ParquetMetaFileReader.
    private static final int COLUMN_CHUNK_BYTE_RANGE_START_OFF = 16;
    private static final int COLUMN_CHUNK_CODEC_OFF = 0;
    private static final int COLUMN_CHUNK_DISTINCT_COUNT_OFF = 40;
    private static final int COLUMN_CHUNK_ENCODINGS_OFF = 1;
    private static final int COLUMN_CHUNK_MAX_STAT_OFF = 56;
    private static final int COLUMN_CHUNK_MIN_STAT_OFF = 48;
    private static final int COLUMN_CHUNK_NULL_COUNT_OFF = 32;
    private static final int COLUMN_CHUNK_NUM_VALUES_OFF = 8;
    private static final int COLUMN_CHUNK_SIZE = 64;
    private static final int COLUMN_CHUNK_STAT_FLAGS_OFF = 2;
    private static final int COLUMN_CHUNK_STAT_SIZES_OFF = 3;
    private static final int COLUMN_CHUNK_TOTAL_COMPRESSED_OFF = 24;
    private static final int COLUMN_DESCRIPTOR_SIZE = 32;
    // Column descriptor layout (32B each, starting right after the header),
    // identical to _pm's; see ParquetMetaFileReader.
    private static final int COL_DESC_COL_TYPE_OFF = 12;
    private static final int COL_DESC_FIXED_BYTE_LEN_OFF = 20;
    private static final int COL_DESC_FLAGS_OFF = 16;
    private static final int COL_DESC_ID_OFF = 8;
    private static final int COL_DESC_MAX_DEF_LEVEL_OFF = 30;
    private static final int COL_DESC_MAX_REP_LEVEL_OFF = 29;
    private static final int COL_DESC_NAME_LENGTH_OFF = 24;
    private static final int COL_DESC_NAME_OFFSET_OFF = 0;
    private static final int COL_DESC_PHYSICAL_TYPE_OFF = 28;
    // First byte covered by the CRC; IM_FILE_SIZE at offset 0 is excluded
    // because the writer patches it last as the commit signal.
    private static final int IM_CRC_AREA_OFF = 8;
    private static final int IM_FORMAT_VERSION = 4;
    // The bytes QDBIDX\0\3 at offset 8. Disambiguates _im from _pm, which
    // carries FEATURE_FLAGS at the same offset, and its version byte is what
    // keeps a v2 file from being read as a v3 one.
    private static final long IM_MAGIC = 0x0300_5844_4942_4451L;
    // One index row per key: there is no row_id column and ROW_ID_COLUMN is -1.
    private static final int IM_PAYLOAD_ROW_PER_KEY = 1;
    // One index row per posting, carrying a row_id column.
    private static final int IM_PAYLOAD_ROW_PER_POSTING = 0;
    private static final int OFF_COLUMN_COUNT = 32;
    private static final int OFF_DATA_RG_COUNT = 40;
    private static final int OFF_FEATURE_FLAGS = 16;
    private static final int OFF_FIRST_COVER_COLUMN = 76;
    private static final int OFF_FORMAT_VERSION = 24;
    private static final int OFF_IM_FILE_SIZE = 0;
    private static final int OFF_IM_MAGIC = 8;
    private static final int OFF_INDEX_RG_COUNT = 36;
    private static final int OFF_INDEX_SECTIONS_OFFSET = 56;
    private static final int OFF_KEY_ID_COLUMN = 48;
    // Total KEY_ROW_OFFSET entries, taken from the reserved area at format
    // version 4: the per-group directories are variable length, so the
    // section's size is not derivable from the counts already in the header.
    private static final int OFF_KEY_DIR_ENTRY_COUNT = 80;
    private static final int OFF_KEY_SPACE_SIZE = 44;
    private static final int OFF_PAYLOAD_KIND = 28;
    private static final int OFF_PIDX_FOOTER_LENGTH = 72;
    private static final int OFF_PIDX_FOOTER_OFFSET = 64;
    private static final int OFF_ROW_ID_COLUMN = 52;
    // The 4-byte footer length plus the PAR1 magic that follow a parquet
    // footer, exactly as _pm derives data.parquet's committed size.
    private static final int PIDX_FOOTER_TRAILER_SIZE = 8;
    // Feature flag bits 32-63 are required: unknown bits must cause rejection.
    private static final long REQUIRED_FEATURE_MASK = 0xFFFF_FFFF_0000_0000L;
    // Each row group block starts with an 8-byte NUM_ROWS u64 prefix.
    private static final int ROW_GROUP_BLOCK_HEADER_SIZE = 8;
    // Each RG_BLOCK_OFFSET entry is a u32 holding a byte offset >> 3.
    private static final int ROW_GROUP_ENTRY_SIZE = 4;
    // Stat flag bits within the column chunk stat_flags byte, mirroring the
    // Rust StatFlags: bit 0 MIN_PRESENT, bit 1 MIN_INLINED, bit 2 MIN_EXACT,
    // bit 3 MAX_PRESENT, bit 4 MAX_INLINED, bit 5 MAX_EXACT.
    private static final int STAT_FLAG_MAX_INLINED = 1 << 4;
    private static final int STAT_FLAG_MAX_PRESENT = 1 << 3;
    private static final int STAT_FLAG_MIN_INLINED = 1 << 1;
    private static final int STAT_FLAG_MIN_PRESENT = 1;
    private final DirectUtf8String flyweightColName = new DirectUtf8String();
    private long addr;
    private int columnCount;
    private long dataBoundaryOffset;
    private int keyDirEntryCount;
    private long keyRowOffsetOffset;
    private long rgKeyDirBaseOffset;
    private int dataRowGroupCount;
    private long featureFlags;
    private FilesFacade ff;
    private int firstCoverColumn;
    private int indexRowGroupCount;
    private int keyIdColumn;
    private int keySpaceSize;
    // Size of the mapping this reader owns, 0 when the buffer belongs to the caller.
    private long mappedSize;
    // End of the column descriptors, and so the lowest offset a name string or
    // a row group block may start at.
    private long namesStart;
    private int payloadKind;
    private int pidxFooterLength;
    private long pidxFooterOffset;
    // The header's INDEX_SECTIONS_OFFSET, validated at bind time. It doubles as
    // the exclusive upper bound of the row group block region.
    private long rgBlockOffsetOffset;
    private long rgFirstKeyOffset;
    private long rgRowIdMaxOffset;
    private long rgRowIdMinOffset;
    private int rowIdColumn;
    // Committed IM_FILE_SIZE the reader is bound to, never the filesystem length.
    private long size;

    /**
     * Single-open helper: opens the {@code _im} file, reads the committed
     * {@code IM_FILE_SIZE} at offset 0 with a pread, maps exactly that many
     * bytes and binds the reader to the mapping. The file descriptor is closed
     * before this method returns - the mapping outlives it - so the reader owns
     * only the mapping, released by {@link #close()}.
     * <p>
     * Returns {@code 0}, leaving the reader cleared, when the file is missing,
     * unreadable, or not yet committed. Throws when the file is present but
     * malformed.
     *
     * @param ff     files facade
     * @param path   path to the {@code _im} file
     * @param reader reader to bind the mapping to
     * @return the mapping address, or {@code 0} when there is nothing to read
     * @throws CairoException if the file is corrupt or the header claims a size
     *                        larger than the file
     */
    public static long openAndMapRO(FilesFacade ff, LPSZ path, IndexMetaFileReader reader) {
        // The reader is left cleared on failure, so the caller can use the
        // return value alone as the success/failure signal.
        reader.clear();
        final long fd = ff.openRO(path);
        if (fd < 0) {
            return 0;
        }
        try {
            // Read IM_FILE_SIZE with a pread and validate it against the file
            // length BEFORE mapping anything. mmap() does not check the length
            // against the file, so mapping a fixed header off a shorter file
            // succeeds and the first read lands on a page past EOF, raising
            // SIGBUS - which kills the JVM instead of throwing. A crash between
            // the writer's creat() and its first write leaves exactly such a
            // zero-length file. This mirrors ParquetMetaFileReader.openAndMapRO,
            // whose ordering is the point of the guard.
            final long imFileSize = ff.readNonNegativeLong(fd, 0);
            if (imFileSize <= 0) {
                // 0: IM_FILE_SIZE is patched last as the commit signal, so an
                // unpatched zero means "not committed yet" - a normal state, not
                // corruption. Negative: the pread could not return 8 bytes, so
                // the file is shorter than the size field and holds nothing to
                // read either way.
                return 0;
            }
            if (imFileSize < IM_HEADER_SIZE + IM_TRAILER_SIZE) {
                throw CairoException.critical(0)
                        .put("invalid _im IM_FILE_SIZE [imFileSize=").put(imFileSize)
                        .put(", path=").put(path).put(']');
            }
            // The filesystem length never bounds the mapping - only IM_FILE_SIZE
            // does - but a header claiming more bytes than the file holds would
            // make the reads below run past EOF and SIGBUS the JVM, so corruption
            // must surface as a clear error instead.
            final long actualFileSize = ff.length(fd);
            if (imFileSize > actualFileSize) {
                throw CairoException.critical(0)
                        .put("invalid _im IM_FILE_SIZE exceeds file length [imFileSize=").put(imFileSize)
                        .put(", actualFileSize=").put(actualFileSize)
                        .put(", path=").put(path).put(']');
            }
            final long addr = TableUtils.mapRO(ff, fd, imFileSize, MemoryTag.MMAP_PARQUET_METADATA_READER);
            try {
                reader.ofAddress(addr, imFileSize);
            } catch (Throwable th) {
                reader.clear();
                ff.munmap(addr, imFileSize, MemoryTag.MMAP_PARQUET_METADATA_READER);
                throw th;
            }
            // Transfer mapping ownership only once the reader is bound and
            // validated: until then the catch above owns the cleanup.
            reader.ff = ff;
            reader.mappedSize = imFileSize;
            return addr;
        } finally {
            // The mapping survives the descriptor, so the reader never retains
            // one: Phase 2 holds a reader per indexed column per partition and
            // each retained descriptor would cost a live fd.
            ff.close(fd);
        }
    }

    /**
     * Releases the mapping when this reader owns it (see {@link #openAndMapRO})
     * and zeroes the reader's fields. Safe to call repeatedly and safe to call
     * on a reader that was never bound.
     */
    public void clear() {
        if (mappedSize != 0) {
            ff.munmap(addr, mappedSize, MemoryTag.MMAP_PARQUET_METADATA_READER);
            mappedSize = 0;
        }
        ff = null;
        addr = 0;
        size = 0;
        featureFlags = 0;
        namesStart = 0;
        rgBlockOffsetOffset = 0;
        rgFirstKeyOffset = 0;
        rgRowIdMinOffset = 0;
        rgRowIdMaxOffset = 0;
        dataBoundaryOffset = 0;
        rgKeyDirBaseOffset = 0;
        keyRowOffsetOffset = 0;
        keyDirEntryCount = 0;
        columnCount = 0;
        indexRowGroupCount = 0;
        dataRowGroupCount = 0;
        keySpaceSize = 0;
        keyIdColumn = 0;
        rowIdColumn = 0;
        firstCoverColumn = 0;
        pidxFooterOffset = 0;
        pidxFooterLength = 0;
        payloadKind = 0;
    }

    @Override
    public void close() {
        clear();
    }

    /**
     * Base address of the bound buffer. Zero when the reader is not bound.
     */
    public long getAddr() {
        return addr;
    }

    /**
     * Byte offset of the {@code column}'s chunk within {@code rowGroup},
     * relative to the start of {@code <col>.pidx.parquet}.
     */
    public long getChunkByteRangeStart(int rowGroup, int column) {
        return Unsafe.getLong(columnChunkAddr(rowGroup, column) + COLUMN_CHUNK_BYTE_RANGE_START_OFF);
    }

    /**
     * Compression codec the chunk's pages are compressed with, as the raw
     * codec byte: 0 uncompressed, 1 snappy, 2 gzip, 3 lzo, 4 brotli, 5 lz4,
     * 6 zstd, 7 lz4 raw.
     */
    public int getChunkCodec(int rowGroup, int column) {
        return Unsafe.getByte(columnChunkAddr(rowGroup, column) + COLUMN_CHUNK_CODEC_OFF) & 0xFF;
    }

    /**
     * Distinct value count of the chunk. Meaningful only when the
     * {@code DISTINCT_COUNT_PRESENT} bit of {@link #getChunkStatFlags} is set.
     */
    public long getChunkDistinctCount(int rowGroup, int column) {
        return Unsafe.getLong(columnChunkAddr(rowGroup, column) + COLUMN_CHUNK_DISTINCT_COUNT_OFF);
    }

    /**
     * Bitmask of the encodings present in the chunk: bit 0 PLAIN,
     * bit 1 RLE_DICTIONARY, bit 2 DELTA_BINARY_PACKED,
     * bit 3 DELTA_LENGTH_BYTE_ARRAY, bit 4 DELTA_BYTE_ARRAY,
     * bit 5 BYTE_STREAM_SPLIT. Page headers carry the per-page encoding; this
     * is the union over the chunk.
     */
    public int getChunkEncodings(int rowGroup, int column) {
        return Unsafe.getByte(columnChunkAddr(rowGroup, column) + COLUMN_CHUNK_ENCODINGS_OFF) & 0xFF;
    }

    /**
     * Inline maximum statistic of the chunk. Valid only when
     * {@link #hasChunkMaxStat} and {@link #isChunkMaxStatInline} both hold;
     * otherwise use {@link #getChunkMaxStatAddr} and
     * {@link #getChunkMaxStatLength}.
     */
    public long getChunkMaxStat(int rowGroup, int column) {
        final long chunkAddr = columnChunkAddr(rowGroup, column);
        assert (Unsafe.getByte(chunkAddr + COLUMN_CHUNK_STAT_FLAGS_OFF) & (STAT_FLAG_MAX_PRESENT | STAT_FLAG_MAX_INLINED))
                == (STAT_FLAG_MAX_PRESENT | STAT_FLAG_MAX_INLINED)
                : "max_stat absent or not inlined for row group " + rowGroup + ", column " + column;
        return Unsafe.getLong(chunkAddr + COLUMN_CHUNK_MAX_STAT_OFF);
    }

    /**
     * Address of the chunk's out-of-line maximum statistic within the row
     * group block's out-of-line region. Valid only when
     * {@link #hasChunkMaxStat} holds and {@link #isChunkMaxStatInline} does
     * not.
     *
     * @throws CairoException if the stat reference points outside the block
     */
    public long getChunkMaxStatAddr(int rowGroup, int column) {
        final long chunkAddr = columnChunkAddr(rowGroup, column);
        assert (Unsafe.getByte(chunkAddr + COLUMN_CHUNK_STAT_FLAGS_OFF) & (STAT_FLAG_MAX_PRESENT | STAT_FLAG_MAX_INLINED))
                == STAT_FLAG_MAX_PRESENT
                : "max_stat absent or inlined for row group " + rowGroup + ", column " + column;
        return outOfLineStatAddr(rowGroup, column, Unsafe.getLong(chunkAddr + COLUMN_CHUNK_MAX_STAT_OFF));
    }

    /**
     * Byte length of the chunk's out-of-line maximum statistic. Valid under
     * the same condition as {@link #getChunkMaxStatAddr}.
     */
    public int getChunkMaxStatLength(int rowGroup, int column) {
        return (int) (Unsafe.getLong(columnChunkAddr(rowGroup, column) + COLUMN_CHUNK_MAX_STAT_OFF) & 0xFFFFL);
    }

    /**
     * Declared byte width of the chunk's inline maximum statistic, from the
     * high nibble of STAT_SIZES.
     */
    public int getChunkMaxStatSize(int rowGroup, int column) {
        return (Unsafe.getByte(columnChunkAddr(rowGroup, column) + COLUMN_CHUNK_STAT_SIZES_OFF) >> 4) & 0x0F;
    }

    /**
     * Inline minimum statistic of the chunk. Valid only when
     * {@link #hasChunkMinStat} and {@link #isChunkMinStatInline} both hold;
     * otherwise use {@link #getChunkMinStatAddr} and
     * {@link #getChunkMinStatLength}.
     */
    public long getChunkMinStat(int rowGroup, int column) {
        final long chunkAddr = columnChunkAddr(rowGroup, column);
        assert (Unsafe.getByte(chunkAddr + COLUMN_CHUNK_STAT_FLAGS_OFF) & (STAT_FLAG_MIN_PRESENT | STAT_FLAG_MIN_INLINED))
                == (STAT_FLAG_MIN_PRESENT | STAT_FLAG_MIN_INLINED)
                : "min_stat absent or not inlined for row group " + rowGroup + ", column " + column;
        return Unsafe.getLong(chunkAddr + COLUMN_CHUNK_MIN_STAT_OFF);
    }

    /**
     * Address of the chunk's out-of-line minimum statistic within the row
     * group block's out-of-line region. Valid only when
     * {@link #hasChunkMinStat} holds and {@link #isChunkMinStatInline} does
     * not.
     *
     * @throws CairoException if the stat reference points outside the block
     */
    public long getChunkMinStatAddr(int rowGroup, int column) {
        final long chunkAddr = columnChunkAddr(rowGroup, column);
        assert (Unsafe.getByte(chunkAddr + COLUMN_CHUNK_STAT_FLAGS_OFF) & (STAT_FLAG_MIN_PRESENT | STAT_FLAG_MIN_INLINED))
                == STAT_FLAG_MIN_PRESENT
                : "min_stat absent or inlined for row group " + rowGroup + ", column " + column;
        return outOfLineStatAddr(rowGroup, column, Unsafe.getLong(chunkAddr + COLUMN_CHUNK_MIN_STAT_OFF));
    }

    /**
     * Byte length of the chunk's out-of-line minimum statistic. Valid under
     * the same condition as {@link #getChunkMinStatAddr}.
     */
    public int getChunkMinStatLength(int rowGroup, int column) {
        return (int) (Unsafe.getLong(columnChunkAddr(rowGroup, column) + COLUMN_CHUNK_MIN_STAT_OFF) & 0xFFFFL);
    }

    /**
     * Declared byte width of the chunk's inline minimum statistic, from the
     * low nibble of STAT_SIZES.
     */
    public int getChunkMinStatSize(int rowGroup, int column) {
        return Unsafe.getByte(columnChunkAddr(rowGroup, column) + COLUMN_CHUNK_STAT_SIZES_OFF) & 0x0F;
    }

    /**
     * Null count of the chunk. When it equals {@link #getChunkNumValues} the
     * chunk is entirely null and the reader can materialise nulls without
     * fetching or decoding anything.
     */
    public long getChunkNullCount(int rowGroup, int column) {
        return Unsafe.getLong(columnChunkAddr(rowGroup, column) + COLUMN_CHUNK_NULL_COUNT_OFF);
    }

    public long getChunkNumValues(int rowGroup, int column) {
        return Unsafe.getLong(columnChunkAddr(rowGroup, column) + COLUMN_CHUNK_NUM_VALUES_OFF);
    }

    /**
     * Raw STAT_FLAGS byte of the chunk: bit 0 MIN_PRESENT, bit 1 MIN_INLINED,
     * bit 2 MIN_EXACT, bit 3 MAX_PRESENT, bit 4 MAX_INLINED, bit 5 MAX_EXACT,
     * bit 6 DISTINCT_COUNT_PRESENT, bit 7 NULL_COUNT_PRESENT.
     */
    public int getChunkStatFlags(int rowGroup, int column) {
        return Unsafe.getByte(columnChunkAddr(rowGroup, column) + COLUMN_CHUNK_STAT_FLAGS_OFF) & 0xFF;
    }

    /**
     * Compressed byte length of the chunk. Together with
     * {@link #getChunkByteRangeStart} this is the range to fetch from cold
     * storage; contiguous row groups coalesce into one request.
     */
    public long getChunkTotalCompressed(int rowGroup, int column) {
        return Unsafe.getLong(columnChunkAddr(rowGroup, column) + COLUMN_CHUNK_TOTAL_COMPRESSED_OFF);
    }

    public int getColumnCount() {
        return columnCount;
    }

    /**
     * Byte width of a {@code FIXED_LEN_BYTE_ARRAY} column, {@code 0} for every
     * other physical type. This is the only record of the width: a
     * {@code UUID}, {@code LONG256} or {@code VARCHAR} covered column is a
     * fixed-length byte array whose width lives nowhere else in {@code _im},
     * and the point of the format is to decode index bytes without reading the
     * parquet footer.
     */
    public int getColumnFixedByteLen(int column) {
        return Unsafe.getInt(columnDescriptorAddr(column) + COL_DESC_FIXED_BYTE_LEN_OFF);
    }

    /**
     * Raw FLAGS bitfield of the column descriptor.
     */
    public int getColumnFlags(int column) {
        return Unsafe.getInt(columnDescriptorAddr(column) + COL_DESC_FLAGS_OFF);
    }

    /**
     * The column's QuestDB writer index, or {@code -1} for the synthetic
     * {@code key_id} and {@code row_id} columns. Writer indices are used
     * rather than positional table indices because they survive
     * {@code DROP COLUMN}.
     */
    public int getColumnId(int column) {
        return Unsafe.getInt(columnDescriptorAddr(column) + COL_DESC_ID_OFF);
    }

    /**
     * Index of the column whose descriptor ID matches the given QuestDB
     * writer index, or {@code -1} when no column matches. This is how a
     * query's required cover columns become a parquet column projection.
     * <p>
     * A negative ID is not a lookup key and always misses: {@code -1} is the
     * descriptor sentinel for the synthetic {@code key_id} and {@code row_id}
     * columns, and matching it here would hand back the first synthetic column
     * instead. Those two are reached through {@link #getKeyIdColumn()} and
     * {@link #getRowIdColumn()}, which is the only sanctioned route. This
     * matches {@link ParquetMetaFileReader#getColumnIndexById(int)}, and the
     * Rust reader's {@code column_index_by_id} makes the same call.
     */
    public int getColumnIndexById(int id) {
        if (id < 0) {
            return -1;
        }
        for (int i = 0; i < columnCount; i++) {
            if (getColumnId(i) == id) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Maximum definition level of the column, {@code 0} when the column is
     * required. A page decoder needs it to size the definition level data it
     * must skip or decode, which is the other half of decoding a chunk without
     * the parquet footer. Named after
     * {@link ParquetMetaFileReader#getColumnMaxDefLevel(int)}.
     */
    public int getColumnMaxDefLevel(int column) {
        return Unsafe.getByte(columnDescriptorAddr(column) + COL_DESC_MAX_DEF_LEVEL_OFF) & 0xFF;
    }

    /**
     * Maximum repetition level of the column, {@code 0} for the flat schemas
     * the index writer produces. Kept because a page decoder must know whether
     * repetition levels are present at all.
     */
    public int getColumnMaxRepLevel(int column) {
        return Unsafe.getByte(columnDescriptorAddr(column) + COL_DESC_MAX_REP_LEVEL_OFF) & 0xFF;
    }

    /**
     * Returns the column name as a flyweight over the mapped {@code _im}
     * data. The returned reference is reused across calls - callers must not
     * hold it past the next call. The name range was bounds-checked when the
     * reader was bound.
     */
    public DirectUtf8String getColumnName(int column) {
        final long descAddr = columnDescriptorAddr(column);
        final long nameAddr = addr + Unsafe.getLong(descAddr + COL_DESC_NAME_OFFSET_OFF);
        final int nameLength = Unsafe.getInt(descAddr + COL_DESC_NAME_LENGTH_OFF);
        return flyweightColName.of(nameAddr, nameAddr + nameLength, true);
    }

    /**
     * Parquet physical type of the column, as the raw descriptor byte.
     */
    public int getColumnPhysicalType(int column) {
        return Unsafe.getByte(columnDescriptorAddr(column) + COL_DESC_PHYSICAL_TYPE_OFF) & 0xFF;
    }

    /**
     * QuestDB column type of the column.
     */
    public int getColumnType(int column) {
        return Unsafe.getInt(columnDescriptorAddr(column) + COL_DESC_COL_TYPE_OFF);
    }

    /**
     * Descriptor index of cover slot {@code slot}.
     * <p>
     * A query's required cover columns are <b>cover slots</b> - ordinals into
     * this index's own {@code INCLUDE} list, the {@code n} in the native
     * {@code <col>.pc{n}} - not writer indices, and the two spaces are easy to
     * confuse: a writer index passed here resolves to some other covered
     * column or misses entirely, with no error either way. Descriptor order is
     * the synthetic columns first, then the covered columns in cover-slot
     * order, so the mapping is positional and bounded by
     * {@link #getColumnCount()}.
     * <p>
     * The addition is unsigned on both terms, so neither a negative slot nor a
     * {@code FIRST_COVER_COLUMN} near the top of the u32 range wraps into
     * range. This is the Java spelling of the Rust reader's
     * {@code cover_column_index}, which rejects the same slots.
     *
     * @throws CairoException if the slot is outside
     *                        {@code [0, COLUMN_COUNT - FIRST_COVER_COLUMN)}
     */
    public int getCoverColumnIndex(int slot) {
        final long index = Integer.toUnsignedLong(firstCoverColumn) + Integer.toUnsignedLong(slot);
        if (index >= columnCount) {
            throw CairoException.critical(0)
                    .put("_im cover slot out of range [slot=").put(slot)
                    .put(", firstCoverColumn=").put(Integer.toUnsignedLong(firstCoverColumn))
                    .put(", columnCount=").put(columnCount).put(']');
        }
        return (int) index;
    }

    /**
     * Cumulative row count at {@code data.parquet} row group boundary
     * {@code i}. There are {@code getDataRowGroupCount() + 1} entries, the
     * first is {@code 0} and the array is non-decreasing, so a binary search
     * over it maps a row id to the data row groups a non-covering query must
     * read.
     *
     * @throws CairoException if {@code i} is outside
     *                        {@code [0, getDataRowGroupCount()]}
     */
    public long getDataRowGroupBoundary(int i) {
        // A real check rather than an assert: assertions are off in
        // production and the index reaches an address computation. The Rust
        // reader's data_row_group_boundary returns an error for the same index.
        if (i < 0 || i > dataRowGroupCount) {
            throw CairoException.critical(0)
                    .put("_im data boundary index out of range [index=").put(i)
                    .put(", dataRowGroupCount=").put(dataRowGroupCount).put(']');
        }
        return Unsafe.getLong(addr + dataBoundaryOffset + (long) i * Long.BYTES);
    }

    public int getDataRowGroupCount() {
        return dataRowGroupCount;
    }

    public long getFeatureFlags() {
        return featureFlags;
    }

    /**
     * Committed {@code IM_FILE_SIZE} the reader is bound to.
     */
    public long getFileSize() {
        return size;
    }

    /**
     * Descriptor index of cover slot {@code 0}, from which
     * {@link #getCoverColumnIndex(int)} resolves every slot. FIRST_COVER_COLUMN
     * is a u32 on disk and is returned here as a raw {@code int}; the reader
     * does not validate it at open, exactly as the Rust reader does not - the
     * writer enforces the ordering, and every read of it is bounds-checked
     * against COLUMN_COUNT at the point of use.
     */
    public int getFirstCoverColumn() {
        return firstCoverColumn;
    }

    public int getIndexRowGroupCount() {
        return indexRowGroupCount;
    }

    /**
     * The header's {@code INDEX_SECTIONS_OFFSET}: the absolute file offset of
     * the first index section, {@code RG_BLOCK_OFFSET}.
     */
    public long getIndexSectionsOffset() {
        return rgBlockOffsetOffset;
    }

    /**
     * Index of the synthetic {@code key_id} column in the descriptors.
     */
    public int getKeyIdColumn() {
        return keyIdColumn;
    }

    /**
     * The <b>exclusive upper bound on key ids</b> - the native reader's
     * {@code keyCountIncludingNulls} - and emphatically not a count of the
     * distinct keys present. Occupancy is sparse: a partition holding keys
     * {@code {5, 900, 12_000}} has a key space of at least {@code 12_001}, and
     * a distinct-key count of {@code 3} would make every key at or above it
     * report as absent with no error anywhere.
     * <p>
     * KEY_SPACE_SIZE is a u32 on disk and is returned here as a raw
     * {@code int}: a value above {@code 2^31} reads back negative, where the
     * Rust reader returns a {@code u32}. Compare it with
     * {@link Integer#compareUnsigned(int, int)}, as the key lookup does.
     * Unreachable with real symbol keys.
     */
    public int getKeySpaceSize() {
        return keySpaceSize;
    }

    /**
     * {@code 0} = row per posting, {@code 1} = row per key.
     */
    public int getPayloadKind() {
        return payloadKind;
    }

    /**
     * Committed size of {@code <col>.pidx.<indexTxn>.parquet}, derived exactly
     * as {@code _pm} derives the data parquet's: the footer offset and length
     * plus the 4-byte footer length and the {@code PAR1} magic. Recording it in
     * {@code _im} is what lets cold-storage upload, orphan validation and the
     * standard-statistics oracle path work without an {@code ff.length()} call.
     *
     * @throws CairoException if the recorded footer range does not describe an
     *                        addressable file
     */
    public long getPidxFileSize() {
        final long footerLength = Integer.toUnsignedLong(pidxFooterLength);
        // PIDX_FOOTER_OFFSET is a u64: at or above 2^63 it reads back negative
        // here, and no parquet file reaches that offset. The Rust reader's
        // pidx_file_size rejects the same range - it bounds its u64 sum by
        // i64::MAX for this reason - so a file either yields the same size in
        // both readers or is an error in both. An unusable value has to be an
        // error rather than a plausible, wrong size: cold-storage upload and
        // orphan validation both consume it as a long.
        if (pidxFooterOffset < 0
                || pidxFooterOffset > Long.MAX_VALUE - footerLength - PIDX_FOOTER_TRAILER_SIZE) {
            throw CairoException.critical(0)
                    .put("_im PIDX footer range overflows [pidxFooterOffset=").put(pidxFooterOffset)
                    .put(", pidxFooterLength=").put(footerLength).put(']');
        }
        return pidxFooterOffset + footerLength + PIDX_FOOTER_TRAILER_SIZE;
    }

    /**
     * Length of {@code <col>.pidx.<indexTxn>.parquet}'s own parquet footer.
     * PIDX_FOOTER_LENGTH is a u32 on disk and is returned here as a raw
     * {@code int}; {@link #getPidxFileSize()} widens it without sign extension.
     */
    public int getPidxFooterLength() {
        return pidxFooterLength;
    }

    /**
     * Byte offset in {@code <col>.pidx.<indexTxn>.parquet} where its own
     * parquet footer starts.
     */
    public long getPidxFooterOffset() {
        return pidxFooterOffset;
    }

    /**
     * The smallest key id present in index row group {@code i}. Index
     * {@code getIndexRowGroupCount()} is the sentinel and equals
     * {@link #getKeySpaceSize()}.
     * <p>
     * RG_FIRST_KEY holds u32 entries and one is returned here as a raw
     * {@code int}: a key above {@code 2^31} reads back negative, where the Rust
     * reader returns a {@code u32}. Compare it with
     * {@link Integer#compareUnsigned(int, int)}, as the key lookup does.
     * Unreachable with real symbol keys.
     *
     * @throws CairoException if {@code i} is outside
     *                        {@code [0, getIndexRowGroupCount()]}
     */
    public int getRowGroupFirstKey(int i) {
        // A real check rather than an assert: assertions are off in
        // production and the index reaches an address computation. The Rust
        // reader's row_group_first_key returns an error for the same index.
        if (i < 0 || i > indexRowGroupCount) {
            throw CairoException.critical(0)
                    .put("_im first key index out of range [index=").put(i)
                    .put(", indexRowGroupCount=").put(indexRowGroupCount).put(']');
        }
        return firstKeyAt(i);
    }

    /**
     * Index of the last index row group that can hold {@code key}, or
     * {@code -1} when {@code key} is outside the covered key space. Together
     * with {@link #getRowGroupLoForKey(int)} this is the inclusive row group
     * range the key's postings live in, and it is contiguous, so the key's
     * postings and covered values are one byte range per column. A caller that
     * wants both bounds should ask {@link #getRowGroupRangeForKey(int)} once
     * rather than call this and its companion.
     */
    public int getRowGroupHiForKey(int key) {
        return Numbers.decodeHighInt(getRowGroupRangeForKey(key));
    }

    /**
     * Index of the first index row group that can hold {@code key}, or
     * {@code -1} when {@code key} is outside the covered key space: at or past
     * {@code KEY_SPACE_SIZE}, below the first row group's first key, or when
     * the file holds no index row groups at all.
     */
    public int getRowGroupLoForKey(int key) {
        return Numbers.decodeLowInt(getRowGroupRangeForKey(key));
    }

    /**
     * NUM_ROWS of the index row group's block, located through
     * RG_BLOCK_OFFSET.
     *
     * @throws CairoException if {@code rowGroup} is outside
     *                        {@code [0, getIndexRowGroupCount())}, or the block
     *                        offset points outside the block region
     */
    public long getRowGroupNumRows(int rowGroup) {
        return Unsafe.getLong(rowGroupBlockAddr(rowGroup));
    }

    /**
     * The row range {@code [lo, hi)} within {@code rowGroup} holding
     * {@code key}, packed with {@link Numbers#encodeLowHighInts(int, int)}, or
     * {@link #KEY_ABSENT} when the group holds no row for it.
     * <p>
     * Format version 4's replacement for decoding the group's whole
     * {@code key_id} column and binary searching it. That probe cost 2.7 ms on
     * a 100k-row group and was paid once per key looked up, which made read
     * cost scale with the number of distinct keys a query touched rather than
     * with the rows it returned.
     * <p>
     * A group's directory covers only the key ids it actually holds, so its
     * length comes from the next group's base - or, for the last group, from
     * the header's entry count.
     */
    public long getKeyRowRangeInGroup(int rowGroup, int key) {
        if (rowGroup < 0 || rowGroup >= getIndexRowGroupCount()) {
            return KEY_ABSENT;
        }
        final int firstKey = getRowGroupFirstKey(rowGroup);
        if (key < firstKey) {
            return KEY_ABSENT;
        }
        final long base = Unsafe.getInt(addr + rgKeyDirBaseOffset + (long) rowGroup * Integer.BYTES) & 0xFFFFFFFFL;
        final long end = rowGroup + 1 < getIndexRowGroupCount()
                ? Unsafe.getInt(addr + rgKeyDirBaseOffset + (long) (rowGroup + 1) * Integer.BYTES) & 0xFFFFFFFFL
                : keyDirEntryCount;
        final long idx = base + (key - firstKey);
        // The terminator is the last entry, so a key needs idx and idx+1 both
        // inside this group's slice.
        if (idx + 1 >= end) {
            return KEY_ABSENT;
        }
        final long at = addr + keyRowOffsetOffset + idx * Integer.BYTES;
        final int lo = Unsafe.getInt(at);
        final int hi = Unsafe.getInt(at + Integer.BYTES);
        return lo >= hi ? KEY_ABSENT : Numbers.encodeLowHighInts(lo, hi);
    }

    /**
     * The inclusive index row group range holding {@code key}, packed with
     * {@link Numbers#encodeLowHighInts(int, int)}: the low bound in the low
     * int, the high bound in the high int. {@link #KEY_ABSENT} when
     * {@code key} is outside the covered key space, which decodes to
     * {@code -1} for both bounds.
     * <p>
     * This is the lookup hot path, and answering both bounds from one pass is
     * the reason RG_FIRST_KEY is a dense u32 array rather than a stride over
     * the 64-byte column chunks: a lookup should touch a couple of cache
     * lines, not one per row group. {@link #getRowGroupLoForKey(int)} and
     * {@link #getRowGroupHiForKey(int)} delegate here rather than searching
     * again. Mirrors the Rust {@code row_group_range_for_key}.
     */
    public long getRowGroupRangeForKey(int key) {
        // The comparison is unsigned: KEY_SPACE_SIZE is a u32, so a key read
        // back negative in Java is above it rather than below zero. It bounds
        // key *ids*, not the count of distinct keys present, so a sparse key
        // set reaches the search for every one of its keys.
        if (Integer.compareUnsigned(key, keySpaceSize) >= 0 || indexRowGroupCount == 0) {
            return KEY_ABSENT;
        }
        // Lower bound: the first row group whose first key is at or above
        // key. Bounded at INDEX_RG_COUNT, so the sentinel is never read.
        int lo = 0;
        int hi = indexRowGroupCount;
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (Integer.compareUnsigned(firstKeyAt(mid), key) < 0) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        final int rgLo;
        if (lo < indexRowGroupCount && firstKeyAt(lo) == key) {
            rgLo = lo;
        } else if (lo == 0) {
            // The key sorts below the first row group's first key, so no row
            // group can hold it.
            return KEY_ABSENT;
        } else {
            // No row group starts at key, so it is packed inside the one
            // before the lower bound.
            rgLo = lo - 1;
        }
        // Upper bound: the first row group whose first key is strictly above
        // key. A first key above key is also at or above it, so the upper
        // bound is never below the lower bound and the search starts there
        // instead of at zero.
        int upperLo = lo;
        int upperHi = indexRowGroupCount;
        while (upperLo < upperHi) {
            final int mid = (upperLo + upperHi) >>> 1;
            if (Integer.compareUnsigned(firstKeyAt(mid), key) <= 0) {
                upperLo = mid + 1;
            } else {
                upperHi = mid;
            }
        }
        // rgLo is a valid row group, so at least one first key is at or below
        // key and the subtraction stays in range.
        return Numbers.encodeLowHighInts(rgLo, upperLo - 1);
    }

    /**
     * The largest row id present in index row group {@code i}. Recorded
     * unconditionally, for the same reason as
     * {@link #getRowGroupRowIdMin(int)}.
     *
     * @throws CairoException if {@code i} is outside
     *                        {@code [0, getIndexRowGroupCount())}
     */
    public long getRowGroupRowIdMax(int i) {
        return rowIdZoneMap(rgRowIdMaxOffset, i, "max");
    }

    /**
     * The smallest row id present in index row group {@code i}.
     * <p>
     * Recorded unconditionally, including under payload kind {@code 1}, where
     * the row ids are an opaque blob and there is no {@code row_id} column to
     * take the range from: a reader that fell back to the chunk statistics
     * would have no time pruning at all for that payload. Under payload kind
     * {@code 0} the writer cross-checks it against those statistics, so the
     * fast path has an independent oracle.
     *
     * @throws CairoException if {@code i} is outside
     *                        {@code [0, getIndexRowGroupCount())}
     */
    public long getRowGroupRowIdMin(int i) {
        return rowIdZoneMap(rgRowIdMinOffset, i, "min");
    }

    /**
     * Index of the synthetic {@code row_id} column in the descriptors, or
     * {@code -1} under payload kind {@code 1}.
     */
    public int getRowIdColumn() {
        return rowIdColumn;
    }

    public boolean hasChunkMaxStat(int rowGroup, int column) {
        return (getChunkStatFlags(rowGroup, column) & STAT_FLAG_MAX_PRESENT) != 0;
    }

    public boolean hasChunkMinStat(int rowGroup, int column) {
        return (getChunkStatFlags(rowGroup, column) & STAT_FLAG_MIN_PRESENT) != 0;
    }

    /**
     * True when the chunk's maximum statistic fits in the 8 inline bytes.
     * Otherwise the field holds an {@code (offset << 16) | length} reference
     * into the row group block's out-of-line region.
     */
    public boolean isChunkMaxStatInline(int rowGroup, int column) {
        return (getChunkStatFlags(rowGroup, column) & STAT_FLAG_MAX_INLINED) != 0;
    }

    /**
     * True when the chunk's minimum statistic fits in the 8 inline bytes.
     * Otherwise the field holds an {@code (offset << 16) | length} reference
     * into the row group block's out-of-line region.
     */
    public boolean isChunkMinStatInline(int rowGroup, int column) {
        return (getChunkStatFlags(rowGroup, column) & STAT_FLAG_MIN_INLINED) != 0;
    }

    public boolean isOpen() {
        return addr != 0;
    }

    /**
     * Binds the reader to an {@code _im} buffer the caller owns: validates the
     * header, verifies the CRC32 and resolves the section offsets. The reader
     * does not take ownership of {@code addr}, so {@link #close()} will not
     * release it.
     *
     * @param addr base address of the buffer
     * @param size number of readable bytes at {@code addr}; must be at least
     *             the committed {@code IM_FILE_SIZE} stored in the header
     * @throws CairoException if the buffer does not hold a valid {@code _im} file
     */
    public void ofAddress(long addr, long size) {
        clear();
        if (addr == 0) {
            throw CairoException.critical(0).put("invalid _im mapping address [addr=0]");
        }
        if (size < IM_HEADER_SIZE + IM_TRAILER_SIZE) {
            throw CairoException.critical(0)
                    .put("invalid _im buffer size [size=").put(size).put(']');
        }
        final long imFileSize = Unsafe.getLong(addr + OFF_IM_FILE_SIZE);
        if (imFileSize < IM_HEADER_SIZE + IM_TRAILER_SIZE || imFileSize > size) {
            throw CairoException.critical(0)
                    .put("invalid _im IM_FILE_SIZE [imFileSize=").put(imFileSize)
                    .put(", size=").put(size).put(']');
        }
        this.addr = addr;
        // The committed IM_FILE_SIZE, not the caller's buffer length, bounds
        // every read below - exactly as the Rust reader bounds itself.
        this.size = imFileSize;
        try {
            parse();
        } catch (Throwable th) {
            // A reader that failed to bind must not go on claiming to be open:
            // its column count, row group count and section offsets are all
            // still zero, so every accessor would answer nonsense. There is
            // nothing to release either way - mappedSize is 0 at this point,
            // and the buffer belongs to the caller - so this is about the
            // object not lying about its state. The Rust reader cannot reach
            // this state at all: its constructor returns a Result.
            clear();
            throw th;
        }
    }

    /**
     * Rounds a section size up to the 8-byte boundary the next section starts
     * on. The Java spelling of the Rust reader's {@code aligned_footprint}.
     */
    private static long alignUp(long size) {
        return (size + BLOCK_ALIGNMENT - 1) & ~((long) BLOCK_ALIGNMENT - 1);
    }

    private static int crc32(long address, long len) {
        int crc = 0;
        long remaining = len;
        while (remaining > 0) {
            final int chunkSize = (int) Math.min(Integer.MAX_VALUE, remaining);
            crc = Zip.crc32(crc, address, chunkSize);
            address += chunkSize;
            remaining -= chunkSize;
        }
        return crc;
    }

    /**
     * Address of a column chunk within a row group block: chunks start after
     * the block's NUM_ROWS prefix and are 64 bytes each.
     */
    private long columnChunkAddr(int rowGroup, int column) {
        // A real check rather than an assert: assertions are off in
        // production, and an out-of-range column here is an address hundreds
        // of megabytes past a mapping of IM_FILE_SIZE bytes - a SIGSEGV that
        // kills the JVM instead of throwing. The analogous index in
        // ParquetMetaFileReader comes from QuestDB metadata, but a caller
        // reaches the synthetic columns here through KEY_ID_COLUMN and
        // ROW_ID_COLUMN, which come off the file. The Rust reader's
        // column_chunk returns an error for the same index.
        if (column < 0 || column >= columnCount) {
            throw CairoException.critical(0)
                    .put("_im column index out of range [column=").put(column)
                    .put(", columnCount=").put(columnCount).put(']');
        }
        return rowGroupBlockAddr(rowGroup) + ROW_GROUP_BLOCK_HEADER_SIZE + (long) column * COLUMN_CHUNK_SIZE;
    }

    /**
     * Address of a column descriptor: descriptors start right after the fixed
     * header and are 32 bytes each.
     */
    private long columnDescriptorAddr(int column) {
        // A real check rather than an assert, for the reason spelled out on
        // columnChunkAddr: assertions are off in production and the index
        // reaches an address computation. The Rust reader's column_descriptor
        // returns an error for the same index.
        if (column < 0 || column >= columnCount) {
            throw CairoException.critical(0)
                    .put("_im column index out of range [column=").put(column)
                    .put(", columnCount=").put(columnCount).put(']');
        }
        return addr + IM_HEADER_SIZE + (long) column * COLUMN_DESCRIPTOR_SIZE;
    }

    private int firstKeyAt(int i) {
        return Unsafe.getInt(addr + rgFirstKeyOffset + (long) i * Integer.BYTES);
    }

    /**
     * Resolves an {@code (offset << 16) | length} out-of-line stat reference
     * to an address, bounding it by the block's out-of-line region: from the
     * end of the last column chunk to the end of <b>this block's own extent</b>,
     * which is where the Rust reader's {@code out_of_line_stat} bounds it too.
     * <p>
     * Bounding it only by the start of the index sections would let a stat in
     * one row group address bytes belonging to the next - legal-looking, and
     * silently wrong, since stats drive query pruning.
     */
    private long outOfLineStatAddr(int rowGroup, int column, long encoded) {
        final long regionStart = rowGroupBlockAddr(rowGroup) + ROW_GROUP_BLOCK_HEADER_SIZE
                + (long) columnCount * COLUMN_CHUNK_SIZE;
        final long regionSize = addr + rowGroupBlockEnd(rowGroup) - regionStart;
        final long statOffset = encoded >>> 16;
        final long statLength = encoded & 0xFFFFL;
        if (statOffset > regionSize || statLength > regionSize - statOffset) {
            throw CairoException.critical(0)
                    .put("_im out of line stat out of bounds [rowGroup=").put(rowGroup)
                    .put(", column=").put(column)
                    .put(", statOffset=").put(statOffset)
                    .put(", statLength=").put(statLength)
                    .put(", regionSize=").put(regionSize).put(']');
        }
        return regionStart + statOffset;
    }

    /**
     * Validates the header and the CRC32, then validates the header's
     * {@code INDEX_SECTIONS_OFFSET} and resolves the five index sections
     * forward from it. The arithmetic mirrors {@code IndexMetaReader::new} in
     * {@code qdb-parquet-meta} step for step; {@link #alignUp(long)} is the
     * Java spelling of its {@code aligned_footprint}. Any drift here shifts
     * every section after the one that moved.
     */
    private void parse() {
        final long addr = this.addr;
        final long size = this.size;

        final long magic = Unsafe.getLong(addr + OFF_IM_MAGIC);
        if (magic != IM_MAGIC) {
            throw CairoException.critical(0)
                    .put("bad _im IM_MAGIC [magic=0x").put(Long.toHexString(magic))
                    .put(", expected=0x").put(Long.toHexString(IM_MAGIC)).put(']');
        }
        final int version = Unsafe.getInt(addr + OFF_FORMAT_VERSION);
        if (version != IM_FORMAT_VERSION) {
            throw CairoException.critical(0)
                    .put("unsupported _im FORMAT_VERSION [version=").put(version)
                    .put(", expected=").put(IM_FORMAT_VERSION).put(']');
        }
        final long featureFlags = Unsafe.getLong(addr + OFF_FEATURE_FLAGS);
        final long unknownRequired = featureFlags & REQUIRED_FEATURE_MASK;
        if (unknownRequired != 0) {
            throw CairoException.critical(0)
                    .put("unsupported required _im FEATURE_FLAGS [flags=0x")
                    .put(Long.toHexString(unknownRequired)).put(']');
        }

        // Nothing below this point may be trusted until the CRC agrees.
        final long crcEnd = size - IM_TRAILER_SIZE;
        final int storedCrc = Unsafe.getInt(addr + crcEnd);
        final int computedCrc = crc32(addr + IM_CRC_AREA_OFF, crcEnd - IM_CRC_AREA_OFF);
        if (storedCrc != computedCrc) {
            throw CairoException.critical(0)
                    .put("_im CRC32 mismatch [stored=").put(storedCrc)
                    .put(", computed=").put(computedCrc).put(']');
        }

        final int columnCount = readCount(addr + OFF_COLUMN_COUNT, "COLUMN_COUNT");
        final int indexRowGroupCount = readCount(addr + OFF_INDEX_RG_COUNT, "INDEX_RG_COUNT");
        final int dataRowGroupCount = readCount(addr + OFF_DATA_RG_COUNT, "DATA_RG_COUNT");

        // The header records where the index sections start; a reader never
        // derives it. What it must do is validate the value against everything
        // else the header claims, with every step checked: the counts and the
        // offset come straight off a file that may be crafted.
        final long indexSectionsOffset = Unsafe.getLong(addr + OFF_INDEX_SECTIONS_OFFSET);
        // Each section starts 8-byte aligned, so the first one must be too.
        if ((indexSectionsOffset & (BLOCK_ALIGNMENT - 1)) != 0) {
            throw CairoException.critical(0)
                    .put("_im INDEX_SECTIONS_OFFSET is not 8 byte aligned [offset=")
                    .put(indexSectionsOffset).put(']');
        }
        // A u64 at or above 2^63 reads back negative here. The five sections
        // are at least 32 bytes, so an offset at or past the CRC leaves them
        // nowhere to fit; rejecting both up front also keeps every sum below
        // from overflowing.
        if (indexSectionsOffset < 0 || indexSectionsOffset > crcEnd) {
            throw truncated(indexSectionsOffset, columnCount, indexRowGroupCount, dataRowGroupCount);
        }
        // The sections start at or after the column descriptors and the name
        // strings they point at.
        final long namesStart = IM_HEADER_SIZE + (long) columnCount * COLUMN_DESCRIPTOR_SIZE;
        if (namesStart > indexSectionsOffset) {
            throw truncated(indexSectionsOffset, columnCount, indexRowGroupCount, dataRowGroupCount);
        }

        // The five sections, sized from the header counts, each padded up so
        // the next starts 8-byte aligned, must fit ahead of the CRC.
        //
        // This bound runs BEFORE the descriptor loop below, and the order is
        // load-bearing rather than incidental:
        // namesStart <= indexSectionsOffset <= sectionsEnd <= crcEnd is what
        // puts the descriptors inside the mapping. Bounding the name entries
        // first reads descriptor bytes that a file truncated anywhere between
        // the header and the end of the descriptors does not have. Clamping
        // COLUMN_COUNT would not fix it: the descriptors can also be cut short
        // with the count untouched, which is exactly what a torn write leaves
        // behind. The Rust reader orders these two the same way, so both
        // accept and reject the same files.
        final long rgFirstKeyOffset = indexSectionsOffset + alignUp((long) indexRowGroupCount * Integer.BYTES);
        final long rgRowIdMinOffset = rgFirstKeyOffset + alignUp((indexRowGroupCount + 1L) * Integer.BYTES);
        // The row-id zone maps are unconditional - row per key has no row_id
        // column to derive them from - and are i64 arrays, so each footprint is
        // already a multiple of 8.
        final long rowIdBytes = alignUp((long) indexRowGroupCount * Long.BYTES);
        final long rgRowIdMaxOffset = rgRowIdMinOffset + rowIdBytes;
        final long dataBoundaryOffset = rgRowIdMaxOffset + rowIdBytes;
        // RG_KEY_DIR_BASE then KEY_ROW_OFFSET. A group's directory covers only
        // the key ids it holds, so its length is not derivable from
        // RG_FIRST_KEY: consecutive bases give it, and KEY_DIR_ENTRY_COUNT
        // gives the last group's.
        final long rgKeyDirBaseOffset = dataBoundaryOffset + alignUp((dataRowGroupCount + 1L) * Long.BYTES);
        final long keyRowOffsetOffset = rgKeyDirBaseOffset + alignUp((long) indexRowGroupCount * Integer.BYTES);
        final int keyDirEntryCount = Unsafe.getInt(addr + OFF_KEY_DIR_ENTRY_COUNT);
        if (keyDirEntryCount < 0) {
            throw truncated(indexSectionsOffset, columnCount, indexRowGroupCount, dataRowGroupCount);
        }
        final long sectionsEnd = keyRowOffsetOffset + alignUp((long) keyDirEntryCount * Integer.BYTES);
        // Slack between the end of DATA_RG_BOUNDARY and the CRC is permitted,
        // so this is a bound and not equality: a writer may pad, and a reader
        // that demanded exactness would reject files the other reader accepts.
        if (sectionsEnd > crcEnd) {
            throw truncated(indexSectionsOffset, columnCount, indexRowGroupCount, dataRowGroupCount);
        }

        // Descriptors are in bounds now, so their name entries can be read to
        // bound the end of the name blob. Doing it here rather than on first
        // access is what makes both implementations reject the same files.
        for (int i = 0; i < columnCount; i++) {
            final long descAddr = addr + IM_HEADER_SIZE + (long) i * COLUMN_DESCRIPTOR_SIZE;
            final long nameOffset = Unsafe.getLong(descAddr + COL_DESC_NAME_OFFSET_OFF);
            final long nameLength = Integer.toUnsignedLong(Unsafe.getInt(descAddr + COL_DESC_NAME_LENGTH_OFF));
            // nameOffset is a u64: at or above 2^63 it reads back negative and
            // fails the lower bound rather than being sign-extended into an
            // address. Comparing the length against the space that remains
            // keeps the end of the name from wrapping.
            if (nameOffset < namesStart || nameOffset > indexSectionsOffset
                    || nameLength > indexSectionsOffset - nameOffset) {
                throw CairoException.critical(0)
                        .put("invalid _im column name pointer [column=").put(i)
                        .put(", nameOffset=").put(nameOffset)
                        .put(", nameLength=").put(nameLength)
                        .put(", namesStart=").put(namesStart)
                        .put(", indexSectionsOffset=").put(indexSectionsOffset).put(']');
            }
        }

        // The header's column selectors are trusted all the way to an address
        // computation: KEY_ID_COLUMN is the only sanctioned route to the
        // synthetic key_id column, so a caller passes it straight to a column
        // chunk accessor. Validating them here, at open, is what keeps that
        // call safe, and the Rust reader validates them at the same point.
        final int payloadKind = Unsafe.getInt(addr + OFF_PAYLOAD_KIND);
        if (payloadKind != IM_PAYLOAD_ROW_PER_POSTING && payloadKind != IM_PAYLOAD_ROW_PER_KEY) {
            throw CairoException.critical(0)
                    .put("unknown _im PAYLOAD_KIND [payloadKind=").put(payloadKind).put(']');
        }
        final int keyIdColumn = Unsafe.getInt(addr + OFF_KEY_ID_COLUMN);
        if (keyIdColumn < 0 || keyIdColumn >= columnCount) {
            throw CairoException.critical(0)
                    .put("_im KEY_ID_COLUMN out of range [keyIdColumn=").put(keyIdColumn)
                    .put(", columnCount=").put(columnCount).put(']');
        }
        // ROW_ID_COLUMN is -1 exactly under row per key: that payload has no
        // row id column at all, and row per posting prunes by time through the
        // chunk stats of the column this names. Any other negative value is
        // rejected under both kinds - it is neither the sentinel nor an index.
        final int rowIdColumn = Unsafe.getInt(addr + OFF_ROW_ID_COLUMN);
        final boolean rowIdColumnValid = payloadKind == IM_PAYLOAD_ROW_PER_KEY
                ? rowIdColumn == -1
                : rowIdColumn >= 0 && rowIdColumn < columnCount;
        if (!rowIdColumnValid) {
            throw CairoException.critical(0)
                    .put("_im ROW_ID_COLUMN is invalid [rowIdColumn=").put(rowIdColumn)
                    .put(", payloadKind=").put(payloadKind)
                    .put(", columnCount=").put(columnCount).put(']');
        }

        // A block's extent comes from the next entry of RG_BLOCK_OFFSET, so
        // the array must ascend: an entry that does not leaves a block with an
        // empty or inverted extent and makes every out-of-line stat bound
        // derived from it meaningless. Rejecting here rather than on first
        // access is what makes every later extent computation trustworthy, and
        // the Rust reader rejects the same files at the same point.
        //
        // The other three per-block predicates run here too, in the same pass.
        // Deferring them to first access lets a crafted file open and answer
        // key lookups, KEY_SPACE_SIZE, boundaries and descriptors for an index
        // whose blocks are all unreachable: the caller gets a row group range
        // it can never resolve, and only discovers it several calls later.
        final long minBlockSize = ROW_GROUP_BLOCK_HEADER_SIZE + (long) columnCount * COLUMN_CHUNK_SIZE;
        for (int i = 0; i < indexRowGroupCount; i++) {
            // An entry is a u32 count of 8-byte units, so the shift is exact in
            // a long and the extent arithmetic below cannot wrap.
            final long entry = Integer.toUnsignedLong(
                    Unsafe.getInt(addr + indexSectionsOffset + (long) i * ROW_GROUP_ENTRY_SIZE));
            final long start = entry << BLOCK_ALIGNMENT_SHIFT;
            final long end;
            if (i + 1 < indexRowGroupCount) {
                // The ascent is checked one entry ahead rather than one behind,
                // because the next entry is also this block's end: comparing
                // backwards would report an inverted extent as a bounds failure
                // a row group earlier, and the ascent message names the real
                // defect.
                final long next = Integer.toUnsignedLong(
                        Unsafe.getInt(addr + indexSectionsOffset + (long) (i + 1) * ROW_GROUP_ENTRY_SIZE));
                if (next <= entry) {
                    throw CairoException.critical(0)
                            .put("_im RG_BLOCK_OFFSET entries must ascend [rowGroup=").put(i + 1)
                            .put(", entry=").put(next)
                            .put(", previous=").put(entry).put(']');
                }
                end = next << BLOCK_ALIGNMENT_SHIFT;
            } else {
                end = indexSectionsOffset;
            }
            // A block starting before the descriptors end overlaps the header
            // or the descriptors; one ending past INDEX_SECTIONS_OFFSET reads
            // the key directory as column chunks. Both are addresses, not
            // decode failures, so they are rejected rather than resolved.
            // start > end is reachable only for the last block, whose end is
            // INDEX_SECTIONS_OFFSET rather than the next entry; without it the
            // subtraction below would wrap and pass the size check.
            if (start < namesStart || end > indexSectionsOffset || start > end) {
                throw CairoException.critical(0)
                        .put("_im row group block extent is outside the block region [rowGroup=").put(i)
                        .put(", start=").put(start)
                        .put(", end=").put(end)
                        .put(", namesStart=").put(namesStart)
                        .put(", indexSectionsOffset=").put(indexSectionsOffset).put(']');
            }
            // The extent must hold NUM_ROWS and one chunk per column.
            if (end - start < minBlockSize) {
                throw CairoException.critical(0)
                        .put("_im row group block extent is below the bytes its column chunks need [rowGroup=").put(i)
                        .put(", start=").put(start)
                        .put(", end=").put(end)
                        .put(", minBlockSize=").put(minBlockSize)
                        .put(", columnCount=").put(columnCount).put(']');
            }
        }

        this.featureFlags = featureFlags;
        this.payloadKind = payloadKind;
        this.columnCount = columnCount;
        this.indexRowGroupCount = indexRowGroupCount;
        this.dataRowGroupCount = dataRowGroupCount;
        this.keySpaceSize = Unsafe.getInt(addr + OFF_KEY_SPACE_SIZE);
        this.keyIdColumn = keyIdColumn;
        this.rowIdColumn = rowIdColumn;
        this.firstCoverColumn = Unsafe.getInt(addr + OFF_FIRST_COVER_COLUMN);
        this.pidxFooterOffset = Unsafe.getLong(addr + OFF_PIDX_FOOTER_OFFSET);
        this.pidxFooterLength = Unsafe.getInt(addr + OFF_PIDX_FOOTER_LENGTH);
        this.namesStart = namesStart;
        this.rgBlockOffsetOffset = indexSectionsOffset;
        this.rgFirstKeyOffset = rgFirstKeyOffset;
        this.rgRowIdMinOffset = rgRowIdMinOffset;
        this.rgRowIdMaxOffset = rgRowIdMaxOffset;
        this.dataBoundaryOffset = dataBoundaryOffset;
        this.rgKeyDirBaseOffset = rgKeyDirBaseOffset;
        this.keyRowOffsetOffset = keyRowOffsetOffset;
        this.keyDirEntryCount = keyDirEntryCount;
    }

    /**
     * Reads a u32 header count. Values above {@link Integer#MAX_VALUE} cannot
     * describe a mappable file and would make the section arithmetic negative,
     * so they are rejected rather than sign-extended.
     */
    private int readCount(long fieldAddr, String field) {
        final long count = Integer.toUnsignedLong(Unsafe.getInt(fieldAddr));
        if (count > Integer.MAX_VALUE) {
            throw CairoException.critical(0)
                    .put("invalid _im ").put(field)
                    .put(" [count=").put(count).put(']');
        }
        return (int) count;
    }

    /**
     * Address of an index row group's block, read from RG_BLOCK_OFFSET and
     * shifted back by 3. The block must start at or after the descriptors,
     * end at or before the index sections, and be wide enough for its chunks:
     * the same window the Rust reader hands to its
     * {@code RowGroupBlockReader}.
     * <p>
     * {@link #parse()} enforces all three predicates over every entry at open,
     * so this repeats them for the one block it is about to address rather than
     * discovering them - the Rust reader's {@code row_group_block_extent}
     * repeats its own bound for the same reason.
     */
    private long rowGroupBlockAddr(int rowGroup) {
        // A real check rather than an assert: with assertions off the entry
        // read below comes from outside RG_BLOCK_OFFSET, so the "offset" is
        // whatever bytes happen to follow, and the bounds applied to it prove
        // nothing. The Rust reader's row_group_block_extent returns an error
        // for the same index.
        if (rowGroup < 0 || rowGroup >= indexRowGroupCount) {
            throw CairoException.critical(0)
                    .put("_im row group index out of range [rowGroup=").put(rowGroup)
                    .put(", indexRowGroupCount=").put(indexRowGroupCount).put(']');
        }
        final long offset = rowGroupBlockOffset(rowGroup);
        final long end = rowGroupBlockEnd(rowGroup);
        final long minBlockSize = ROW_GROUP_BLOCK_HEADER_SIZE + (long) columnCount * COLUMN_CHUNK_SIZE;
        if (offset < namesStart || end > rgBlockOffsetOffset || end - offset < minBlockSize) {
            throw CairoException.critical(0)
                    .put("invalid _im row group block offset [rowGroup=").put(rowGroup)
                    .put(", offset=").put(offset)
                    .put(", end=").put(end)
                    .put(", namesStart=").put(namesStart)
                    .put(", indexSectionsOffset=").put(rgBlockOffsetOffset).put(']');
        }
        return addr + offset;
    }

    /**
     * Exclusive end of an index row group's block. Block {@code i} runs to
     * block {@code i + 1} and the last block runs to INDEX_SECTIONS_OFFSET,
     * which is the extent the Rust reader's {@code row_group_block_extent}
     * computes. RG_BLOCK_OFFSET was checked to ascend when the reader was
     * bound, so this is above the block's own offset for every row group but
     * the last, whose end {@link #rowGroupBlockAddr(int)} checks.
     */
    private long rowGroupBlockEnd(int rowGroup) {
        return rowGroup + 1 < indexRowGroupCount ? rowGroupBlockOffset(rowGroup + 1) : rgBlockOffsetOffset;
    }

    /**
     * Byte offset of an index row group's block from the start of the file:
     * its RG_BLOCK_OFFSET entry, a u32 holding the offset shifted right by 3.
     */
    private long rowGroupBlockOffset(int rowGroup) {
        final int stored = Unsafe.getInt(addr + rgBlockOffsetOffset + (long) rowGroup * ROW_GROUP_ENTRY_SIZE);
        return Integer.toUnsignedLong(stored) << BLOCK_ALIGNMENT_SHIFT;
    }

    /**
     * Reads one entry of RG_ROW_ID_MIN or RG_ROW_ID_MAX. Both arrays have
     * exactly {@code INDEX_RG_COUNT} entries - there is no sentinel, unlike
     * RG_FIRST_KEY - and the bound is a real check rather than an assert
     * because assertions are off in production and the index reaches an
     * address computation. The Rust reader's {@code row_id_zone_map} returns an
     * error for the same index.
     */
    private long rowIdZoneMap(long sectionOffset, int i, CharSequence which) {
        if (i < 0 || i >= indexRowGroupCount) {
            throw CairoException.critical(0)
                    .put("_im row id ").put(which)
                    .put(" index out of range [index=").put(i)
                    .put(", indexRowGroupCount=").put(indexRowGroupCount).put(']');
        }
        return Unsafe.getLong(addr + sectionOffset + (long) i * Long.BYTES);
    }

    private CairoException truncated(long indexSectionsOffset, int columnCount, int indexRowGroupCount, int dataRowGroupCount) {
        return CairoException.critical(0)
                .put("_im sections do not fit [indexSectionsOffset=").put(indexSectionsOffset)
                .put(", columnCount=").put(columnCount)
                .put(", indexRowGroupCount=").put(indexRowGroupCount)
                .put(", dataRowGroupCount=").put(dataRowGroupCount)
                .put(", size=").put(size).put(']');
    }
}
