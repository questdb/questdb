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
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Zip;
import io.questdb.std.str.LPSZ;

/**
 * Memory-mapped reader for the {@code _im} covering-index metadata file, the
 * sidecar to {@code <col>.pidx.parquet}. It mirrors the Rust
 * {@code IndexMetaReader} in {@code qdb-parquet-meta}: the two implementations
 * derive their section offsets from the same header fields and must stay in
 * lock step.
 * <p>
 * Binary format (little-endian):
 * <pre>
 * HEADER (48 bytes fixed):
 *   [0]  IM_FILE_SIZE        u64  (total committed file size; patched last as the commit signal)
 *   [8]  FEATURE_FLAGS       u64  (bits 32-63 are required: unknown bits must cause rejection)
 *   [16] FORMAT_VERSION      u32
 *   [20] PAYLOAD_KIND        u32
 *   [24] INDEX_RG_COUNT      u32
 *   [28] DATA_RG_COUNT       u32
 *   [32] INDEX_COLUMN_COUNT  u32
 *   [36] KEY_COUNT           u32
 *   [40] RESERVED            u64
 *
 * SECTIONS (in file order, each derived from the header counts):
 *   RG_FIRST_KEY      u32 x (INDEX_RG_COUNT + 1)   first key of each index row group, plus a
 *                                                  KEY_COUNT sentinel; padded to an 8-byte boundary
 *   RG_ROW_ID_MIN     i64 x INDEX_RG_COUNT
 *   RG_ROW_ID_MAX     i64 x INDEX_RG_COUNT
 *   DATA_RG_BOUNDARY  i64 x (DATA_RG_COUNT + 1)
 *   RG_COL_RANGE      (u64 offset, u64 length) x INDEX_RG_COUNT x INDEX_COLUMN_COUNT
 *   CRC32             u32  over [8, IM_FILE_SIZE - 4)
 * </pre>
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

    public static final int IM_HEADER_SIZE = 48;
    public static final int IM_TRAILER_SIZE = 4;
    // First byte covered by the CRC; IM_FILE_SIZE at offset 0 is excluded
    // because the writer patches it last as the commit signal.
    private static final int IM_CRC_AREA_OFF = 8;
    private static final int IM_FORMAT_VERSION = 1;
    private static final int OFF_DATA_RG_COUNT = 28;
    private static final int OFF_FEATURE_FLAGS = 8;
    private static final int OFF_FORMAT_VERSION = 16;
    private static final int OFF_IM_FILE_SIZE = 0;
    private static final int OFF_INDEX_COLUMN_COUNT = 32;
    private static final int OFF_INDEX_RG_COUNT = 24;
    private static final int OFF_KEY_COUNT = 36;
    private static final int OFF_PAYLOAD_KIND = 20;
    // Feature flag bits 32-63 are required: unknown bits must cause rejection.
    private static final long REQUIRED_FEATURE_MASK = 0xFFFF_FFFF_0000_0000L;
    private long addr;
    private long colRangeOffset;
    private long dataBoundaryOffset;
    private int dataRowGroupCount;
    private FilesFacade ff;
    private int indexColumnCount;
    private int indexRowGroupCount;
    private int keyCount;
    // Size of the mapping this reader owns, 0 when the buffer belongs to the caller.
    private long mappedSize;
    private int payloadKind;
    private long rgFirstKeyOffset;
    private long rowIdMaxOffset;
    private long rowIdMinOffset;
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
        rgFirstKeyOffset = 0;
        rowIdMinOffset = 0;
        rowIdMaxOffset = 0;
        dataBoundaryOffset = 0;
        colRangeOffset = 0;
        indexRowGroupCount = 0;
        dataRowGroupCount = 0;
        indexColumnCount = 0;
        keyCount = 0;
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
     * Length in bytes of the {@code column}'s chunk within {@code rowGroup}.
     */
    public long getColumnByteRangeLength(int rowGroup, int column) {
        return Unsafe.getLong(addr + columnByteRangeOffset(rowGroup, column) + Long.BYTES);
    }

    /**
     * Byte offset of the {@code column}'s chunk within {@code rowGroup},
     * relative to the start of the index parquet file.
     */
    public long getColumnByteRangeOffset(int rowGroup, int column) {
        return Unsafe.getLong(addr + columnByteRangeOffset(rowGroup, column));
    }

    /**
     * Row id boundary {@code i} of the data partition the index was built
     * over. There are {@code getDataRowGroupCount() + 1} boundaries.
     */
    public long getDataRowGroupBoundary(int i) {
        assert i >= 0 && i <= dataRowGroupCount;
        return Unsafe.getLong(addr + dataBoundaryOffset + (long) i * Long.BYTES);
    }

    public int getDataRowGroupCount() {
        return dataRowGroupCount;
    }

    /**
     * Committed {@code IM_FILE_SIZE} the reader is bound to.
     */
    public long getFileSize() {
        return size;
    }

    public int getIndexColumnCount() {
        return indexColumnCount;
    }

    public int getIndexRowGroupCount() {
        return indexRowGroupCount;
    }

    public int getKeyCount() {
        return keyCount;
    }

    public int getPayloadKind() {
        return payloadKind;
    }

    /**
     * Index of the last index row group that can hold {@code key}, or
     * {@code -1} when {@code key} is outside the covered key space. Together
     * with {@link #getRowGroupLoForKey(int)} this is the inclusive row group
     * range the key's postings live in.
     */
    public int getRowGroupHiForKey(int key) {
        if (getRowGroupLoForKey(key) < 0) {
            return -1;
        }
        // A valid lo implies at least one row group whose first key is <= key,
        // so the upper bound is at least 1 and the subtraction stays in range.
        return upperBound(key) - 1;
    }

    /**
     * Index of the first index row group that can hold {@code key}, or
     * {@code -1} when {@code key} is outside the covered key space: at or past
     * {@code KEY_COUNT}, below the first row group's first key, or when the
     * file holds no index row groups at all.
     */
    public int getRowGroupLoForKey(int key) {
        if (Integer.compareUnsigned(key, keyCount) >= 0 || indexRowGroupCount == 0) {
            return -1;
        }
        final int lo = lowerBound(key);
        if (lo < indexRowGroupCount && firstKeyAt(lo) == key) {
            return lo;
        }
        if (lo == 0) {
            return -1;
        }
        return lo - 1;
    }

    /**
     * Largest data row id held by {@code rowGroup}, from the zone map.
     */
    public long getRowIdMax(int rowGroup) {
        assert rowGroup >= 0 && rowGroup < indexRowGroupCount;
        return Unsafe.getLong(addr + rowIdMaxOffset + (long) rowGroup * Long.BYTES);
    }

    /**
     * Smallest data row id held by {@code rowGroup}, from the zone map.
     */
    public long getRowIdMin(int rowGroup) {
        assert rowGroup >= 0 && rowGroup < indexRowGroupCount;
        return Unsafe.getLong(addr + rowIdMinOffset + (long) rowGroup * Long.BYTES);
    }

    public boolean isOpen() {
        return addr != 0;
    }

    /**
     * Binds the reader to an {@code _im} buffer the caller owns: validates the
     * header, verifies the CRC32 and computes the section offsets. The reader
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
        parse();
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

    private long columnByteRangeOffset(int rowGroup, int column) {
        assert rowGroup >= 0 && rowGroup < indexRowGroupCount;
        assert column >= 0 && column < indexColumnCount;
        return colRangeOffset + ((long) rowGroup * indexColumnCount + column) * 2 * Long.BYTES;
    }

    private int firstKeyAt(int i) {
        return Unsafe.getInt(addr + rgFirstKeyOffset + (long) i * Integer.BYTES);
    }

    /**
     * Index of the first row group whose first key is greater than or equal to
     * {@code key}; {@code indexRowGroupCount} when there is none.
     */
    private int lowerBound(int key) {
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
        return lo;
    }

    /**
     * Validates the header and the CRC32, then derives the five section
     * offsets. The arithmetic mirrors {@code IndexMetaReader::new} in
     * {@code qdb-parquet-meta} field for field; {@code (afterKeys + 7) & ~7L}
     * is the Java spelling of Rust's {@code next_multiple_of(8)}. Any drift
     * here shifts every section after the key directory.
     */
    private void parse() {
        final long addr = this.addr;
        final long size = this.size;

        final int version = Unsafe.getInt(addr + OFF_FORMAT_VERSION);
        if (version != IM_FORMAT_VERSION) {
            throw CairoException.critical(0)
                    .put("unsupported _im FORMAT_VERSION [version=").put(version)
                    .put(", expected=").put(IM_FORMAT_VERSION).put(']');
        }
        final long unknownRequired = Unsafe.getLong(addr + OFF_FEATURE_FLAGS) & REQUIRED_FEATURE_MASK;
        if (unknownRequired != 0) {
            throw CairoException.critical(0)
                    .put("unsupported required _im FEATURE_FLAGS [flags=0x")
                    .put(Long.toHexString(unknownRequired)).put(']');
        }
        final long crcEnd = size - IM_TRAILER_SIZE;
        final int storedCrc = Unsafe.getInt(addr + crcEnd);
        final int computedCrc = crc32(addr + IM_CRC_AREA_OFF, crcEnd - IM_CRC_AREA_OFF);
        if (storedCrc != computedCrc) {
            throw CairoException.critical(0)
                    .put("_im CRC32 mismatch [stored=").put(storedCrc)
                    .put(", computed=").put(computedCrc).put(']');
        }

        this.indexRowGroupCount = readCount(addr + OFF_INDEX_RG_COUNT, "INDEX_RG_COUNT");
        this.dataRowGroupCount = readCount(addr + OFF_DATA_RG_COUNT, "DATA_RG_COUNT");
        this.indexColumnCount = readCount(addr + OFF_INDEX_COLUMN_COUNT, "INDEX_COLUMN_COUNT");
        this.rgFirstKeyOffset = IM_HEADER_SIZE;
        final long afterKeys = rgFirstKeyOffset + (indexRowGroupCount + 1L) * Integer.BYTES;
        this.rowIdMinOffset = (afterKeys + 7) & ~7L;
        this.rowIdMaxOffset = rowIdMinOffset + indexRowGroupCount * (long) Long.BYTES;
        this.dataBoundaryOffset = rowIdMaxOffset + indexRowGroupCount * (long) Long.BYTES;
        this.colRangeOffset = dataBoundaryOffset + (dataRowGroupCount + 1L) * Long.BYTES;
        // Same test as the Rust reader's
        //   needed = col_range_off + rg_count * col_count * 16 + IM_TRAILER_SIZE > end
        // rearranged into a division so the u32 x u32 x 16 product cannot
        // overflow a signed long on a corrupt header.
        final long colRangeEntries = (long) indexRowGroupCount * indexColumnCount;
        if (colRangeOffset + IM_TRAILER_SIZE > size
                || colRangeEntries > (size - colRangeOffset - IM_TRAILER_SIZE) / (2L * Long.BYTES)) {
            throw CairoException.critical(0)
                    .put("_im file truncated [colRangeOffset=").put(colRangeOffset)
                    .put(", indexRowGroupCount=").put(indexRowGroupCount)
                    .put(", indexColumnCount=").put(indexColumnCount)
                    .put(", size=").put(size).put(']');
        }

        this.keyCount = Unsafe.getInt(addr + OFF_KEY_COUNT);
        this.payloadKind = Unsafe.getInt(addr + OFF_PAYLOAD_KIND);
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
     * Index of the first row group whose first key is strictly greater than
     * {@code key}; {@code indexRowGroupCount} when there is none.
     */
    private int upperBound(int key) {
        int lo = 0;
        int hi = indexRowGroupCount;
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (Integer.compareUnsigned(firstKeyAt(mid), key) <= 0) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }
}
