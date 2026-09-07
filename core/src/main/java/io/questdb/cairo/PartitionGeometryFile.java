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

package io.questdb.cairo;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Mutable;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

import java.io.Closeable;

/**
 * Codec and I/O for {@code _geometry.<generation>}, the per-PHYSICAL-PARTITION geometry file that lives inside the
 * partition's own directory.
 */
public class PartitionGeometryFile implements Closeable, Mutable {
    public static final int HEADER_SIZE = 56;
    public static final int MAGIC = 0x4D4F4547; // 'G','E','O','M'
    // Below TxReader's PARTITION_GEOMETRY_OFFSET_MASK's own reach (24 bits of 8-byte units = 128MB), on purpose:
    // PartitionGeometry.publish rotates to a fresh generation once a record would cross this, leaving headroom under
    public static final long MAX_FILE_SIZE = 100L * 1024 * 1024;
    public static final int PIECE_SIZE = 32;
    public static final int HEADER_OFFSET_CHECKSUM_64 = 32;
    public static final int HEADER_OFFSET_LAST_WRITE_MICROS_64 = 40;
    public static final int HEADER_OFFSET_LIVE_ROWS_64 = 24;
    public static final int HEADER_OFFSET_MAGIC_32 = 0;
    public static final int HEADER_OFFSET_PHYSICAL_ROWS_64 = 16;
    public static final int HEADER_OFFSET_PIECE_COUNT_32 = 4;
    public static final int HEADER_OFFSET_SEQ_TXN_64 = 48;
    public static final int HEADER_OFFSET_WRITER_TXN_64 = 8;
    public static final int PIECE_OFFSET_ROW_COUNT_64 = 24;
    public static final int PIECE_OFFSET_ROW_OFFSET_64 = 16;
    public static final int PIECE_OFFSET_TS_HI_64 = 8;
    public static final int PIECE_OFFSET_TS_LO_64 = 0;
    // A record can never be larger than this. 44-bit row counts and a handful of pieces per physical
    // partition make the real size a few hundred bytes; the cap only stops a corrupt pieceCount from
    // asking for an absurd allocation.
    private static final int MAX_PIECE_COUNT = 1 << 20;
    private static final Log LOG = LogFactory.getLog(PartitionGeometryFile.class);
    private final int memoryTag;
    private long buf;
    private long bufCapacity;
    private int pieceCount;

    /**
     * {@code memoryTag} should name the owning subsystem - {@code TableReader}, {@code TableWriter} or an
     * O3 job's own tag - not {@link MemoryTag#NATIVE_TABLE_READER} by default, so a leak or a memory-usage
     * report attributes this buffer to whoever actually holds it.
     */
    public PartitionGeometryFile(int memoryTag) {
        this.memoryTag = memoryTag;
    }

    public static long recordSize(int pieceCount) {
        return HEADER_SIZE + (long) PIECE_SIZE * pieceCount;
    }

    /**
     * Starts building a record in the scratch buffer.
     * @param seqTxn the partition's last-modifying seqTxn, or -1 when unknown (non-WAL table)
     */
    public void beginRecord(long writerTxn, long seqTxn, int expectedPieceCount) {
        ensureCapacity(recordSize(Math.max(expectedPieceCount, 1)));
        pieceCount = 0;
        Unsafe.getUnsafe().putInt(buf + HEADER_OFFSET_MAGIC_32, MAGIC);
        Unsafe.getUnsafe().putLong(buf + HEADER_OFFSET_WRITER_TXN_64, writerTxn);
        Unsafe.getUnsafe().putLong(buf + HEADER_OFFSET_SEQ_TXN_64, seqTxn);
        Unsafe.getUnsafe().putLong(buf + HEADER_OFFSET_PHYSICAL_ROWS_64, 0);
        Unsafe.getUnsafe().putLong(buf + HEADER_OFFSET_LIVE_ROWS_64, 0);
        // The scratch buffer is reused across records and ensureCapacity carries its old contents
        // forward, so a field left unset here inherits the previous record's value and the checksum
        // then blesses it.
        Unsafe.getUnsafe().putLong(buf + HEADER_OFFSET_LAST_WRITE_MICROS_64, 0);
    }

    public void addPiece(long tsLo, long tsHi, long rowOffset, long rowCount) {
        ensureCapacity(recordSize(pieceCount + 1));
        final long p = buf + HEADER_SIZE + (long) PIECE_SIZE * pieceCount;
        Unsafe.getUnsafe().putLong(p + PIECE_OFFSET_TS_LO_64, tsLo);
        Unsafe.getUnsafe().putLong(p + PIECE_OFFSET_TS_HI_64, tsHi);
        Unsafe.getUnsafe().putLong(p + PIECE_OFFSET_ROW_OFFSET_64, rowOffset);
        Unsafe.getUnsafe().putLong(p + PIECE_OFFSET_ROW_COUNT_64, rowCount);
        pieceCount++;
    }

    /**
     * Appends the record built since {@link #beginRecord(long, long, int)} at {@code offset} of {@code
     * <partitionDir>/_geometry.<generation>}, creating the file when it does not exist, and syncs it per {@code
     * commitMode}.
     */
    public long append(FilesFacade ff, Path partitionDir, int generation, long offset, int commitMode) {
        final long size = recordSize(pieceCount);
        Unsafe.getUnsafe().putInt(buf + HEADER_OFFSET_PIECE_COUNT_32, pieceCount);
        Unsafe.getUnsafe().putLong(buf + HEADER_OFFSET_CHECKSUM_64, checksum(buf, pieceCount));

        final int dirLen = partitionDir.size();
        long fd = -1;
        try {
            fd = TableUtils.openFileRWOrFail(ff, geometryFileName(partitionDir, generation), CairoConfiguration.O_NONE);
            if (ff.write(fd, buf, size, offset) != size) {
                throw CairoException.critical(ff.errno())
                        .put("could not append partition geometry [path=").put(partitionDir)
                        .put(", offset=").put(offset)
                        .put(", size=").put(size)
                        .put(']');
            }
            if (commitMode != CommitMode.NOSYNC) {
                ff.fsync(fd);
            }
        } finally {
            if (fd > -1) {
                ff.close(fd);
            }
            partitionDir.trimTo(dirLen);
        }
        return size;
    }

    @Override
    public void clear() {
        pieceCount = 0;
    }

    @Override
    public void close() {
        if (buf != 0) {
            Unsafe.free(buf, bufCapacity, memoryTag);
            buf = 0;
            bufCapacity = 0;
        }
        pieceCount = 0;
    }

    public long getLastWriteMicros() {
        return Unsafe.getUnsafe().getLong(buf + HEADER_OFFSET_LAST_WRITE_MICROS_64);
    }

    public long getLiveRows() {
        return Unsafe.getUnsafe().getLong(buf + HEADER_OFFSET_LIVE_ROWS_64);
    }

    public long getPhysicalRows() {
        return Unsafe.getUnsafe().getLong(buf + HEADER_OFFSET_PHYSICAL_ROWS_64);
    }

    public int getPieceCount() {
        return pieceCount;
    }

    public long getPieceRowCount(int index) {
        return getPieceLong(index, PIECE_OFFSET_ROW_COUNT_64);
    }

    public long getPieceRowOffset(int index) {
        return getPieceLong(index, PIECE_OFFSET_ROW_OFFSET_64);
    }

    public long getPieceTimestampHi(int index) {
        return getPieceLong(index, PIECE_OFFSET_TS_HI_64);
    }

    public long getPieceTimestampLo(int index) {
        return getPieceLong(index, PIECE_OFFSET_TS_LO_64);
    }

    public long getRecordSize() {
        return recordSize(pieceCount);
    }

    public long getSeqTxn() {
        return Unsafe.getUnsafe().getLong(buf + HEADER_OFFSET_SEQ_TXN_64);
    }

    public long getWriterTxn() {
        return Unsafe.getUnsafe().getLong(buf + HEADER_OFFSET_WRITER_TXN_64);
    }

    /**
     * Reads the record at {@code offset} of {@code <partitionDir>/_geometry.<generation>} into the scratch buffer.
     */
    public void read(FilesFacade ff, Path partitionDir, int generation, long offset) {
        final int dirLen = partitionDir.size();
        long fd = -1;
        try {
            fd = TableUtils.openRO(ff, geometryFileName(partitionDir, generation), LOG);
            ensureCapacity(HEADER_SIZE);
            if (ff.read(fd, buf, HEADER_SIZE, offset) != HEADER_SIZE) {
                throw CairoException.critical(ff.errno())
                        .put("could not read partition geometry header [path=").put(partitionDir)
                        .put(", offset=").put(offset)
                        .put(']');
            }
            final int magic = Unsafe.getUnsafe().getInt(buf + HEADER_OFFSET_MAGIC_32);
            final int count = Unsafe.getUnsafe().getInt(buf + HEADER_OFFSET_PIECE_COUNT_32);
            if (magic != MAGIC || count < 1 || count > MAX_PIECE_COUNT) {
                throw CairoException.critical(0)
                        .put("invalid partition geometry record [path=").put(partitionDir)
                        .put(", offset=").put(offset)
                        .put(", magic=").put(magic)
                        .put(", pieceCount=").put(count)
                        .put(']');
            }
            final long size = recordSize(count);
            ensureCapacity(size);
            final long tail = size - HEADER_SIZE;
            if (ff.read(fd, buf + HEADER_SIZE, tail, offset + HEADER_SIZE) != tail) {
                throw CairoException.critical(ff.errno())
                        .put("could not read partition geometry pieces [path=").put(partitionDir)
                        .put(", offset=").put(offset)
                        .put(", pieceCount=").put(count)
                        .put(']');
            }
            final long stored = Unsafe.getUnsafe().getLong(buf + HEADER_OFFSET_CHECKSUM_64);
            if (stored != checksum(buf, count)) {
                throw CairoException.critical(0)
                        .put("partition geometry checksum mismatch [path=").put(partitionDir)
                        .put(", offset=").put(offset)
                        .put(']');
            }
            pieceCount = count;
        } finally {
            if (fd > -1) {
                ff.close(fd);
            }
            partitionDir.trimTo(dirLen);
        }
    }

    public void setLastWriteMicros(long micros) {
        Unsafe.getUnsafe().putLong(buf + HEADER_OFFSET_LAST_WRITE_MICROS_64, micros);
    }

    public void setLiveRows(long liveRows) {
        Unsafe.getUnsafe().putLong(buf + HEADER_OFFSET_LIVE_ROWS_64, liveRows);
    }

    public void setPhysicalRows(long physicalRows) {
        Unsafe.getUnsafe().putLong(buf + HEADER_OFFSET_PHYSICAL_ROWS_64, physicalRows);
    }

    private static long checksum(long addr, int pieceCount) {
        // Deliberately skips HEADER_OFFSET_CHECKSUM_64 itself. A cheap 64-bit mix is enough: this is
        // belt-and-braces against a torn sync, not a defence against a hostile writer.
        long h = 0xcbf29ce484222325L;
        h = mix(h, Unsafe.getUnsafe().getInt(addr + HEADER_OFFSET_MAGIC_32));
        h = mix(h, pieceCount);
        h = mix(h, Unsafe.getUnsafe().getLong(addr + HEADER_OFFSET_WRITER_TXN_64));
        h = mix(h, Unsafe.getUnsafe().getLong(addr + HEADER_OFFSET_PHYSICAL_ROWS_64));
        h = mix(h, Unsafe.getUnsafe().getLong(addr + HEADER_OFFSET_LIVE_ROWS_64));
        // The loop below starts at HEADER_SIZE, so a header field added after the checksum word is NOT
        // covered by it and has to be mixed in by hand.
        h = mix(h, Unsafe.getUnsafe().getLong(addr + HEADER_OFFSET_LAST_WRITE_MICROS_64));
        h = mix(h, Unsafe.getUnsafe().getLong(addr + HEADER_OFFSET_SEQ_TXN_64));
        for (long p = addr + HEADER_SIZE, lim = p + (long) PIECE_SIZE * pieceCount; p < lim; p += Long.BYTES) {
            h = mix(h, Unsafe.getUnsafe().getLong(p));
        }
        return h;
    }

    private static LPSZ geometryFileName(Path partitionDir, int generation) {
        return partitionDir.concat(TableUtils.PARTITION_GEOMETRY_FILE_NAME).put('.').put(generation).$();
    }

    private static long mix(long h, long v) {
        h ^= v;
        h *= 0x100000001b3L;
        return h;
    }

    private void ensureCapacity(long capacity) {
        if (bufCapacity >= capacity) {
            return;
        }
        final long newCapacity = Math.max(capacity, Math.max(bufCapacity * 2, HEADER_SIZE + 8L * PIECE_SIZE));
        final long newBuf = Unsafe.malloc(newCapacity, memoryTag);
        if (buf != 0) {
            Vect.memcpy(newBuf, buf, bufCapacity);
            Unsafe.free(buf, bufCapacity, memoryTag);
        }
        buf = newBuf;
        bufCapacity = newCapacity;
    }

    private long getPieceLong(int index, int fieldOffset) {
        assert index > -1 && index < pieceCount;
        return Unsafe.getUnsafe().getLong(buf + HEADER_SIZE + (long) PIECE_SIZE * index + fieldOffset);
    }
}
