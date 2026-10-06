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

package io.questdb.cairo.frm.file;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.frm.FrameColumn;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.dFile;

public class ContiguousFileFixFrameColumn implements FrameColumn {
    public static final int MEMORY_TAG = MemoryTag.MMAP_TABLE_WRITER;
    private static final Log LOG = LogFactory.getLog(ContiguousFileFixFrameColumn.class);
    protected final FilesFacade ff;
    private final int fileOpts;
    private final boolean mixedIOFlag;
    private final ColumnWriteBuffer writeBuffer = new ColumnWriteBuffer();
    // Introduce a flag to avoid double close, which will lead to very serious consequences.
    protected boolean closed;
    // The least length this column knows its file to have. Every write checks its end against this number instead of
    // asking the file system, and a reserve() ahead of a plan makes the whole plan's writes pass the check without a
    // single fallocate or fstat. Learnt from the file once per open, see isAllocatedBytesKnown.
    private long allocatedBytes;
    private int columnIndex;
    private long columnTop;
    private int columnType;
    private long fd = -1;
    // False until the first write of this open asked the file for its length; allocatedBytes means nothing before.
    private boolean isAllocatedBytesKnown;
    private boolean isReadOnly;
    // See setReadWindow: the top getColumnTop() reports is capped here, while columnTop stays the file's own.
    private long logicalRowHi = Long.MAX_VALUE;
    // One mapping of the file from its byte 0. Read-only columns map the rows they are asked for; writable ones map
    // everything allocated, and grow the same mapping as the file grows, so the writes of a plan - and of the plans
    // after it, while the frame is cached - share one mapping rather than mapping and unmapping each its own slice.
    private long mapAddr;
    private long mapRowHi;
    private long mapSize;
    private RecycleBin<FrameColumn> recycleBin;
    private int shl;

    public ContiguousFileFixFrameColumn(CairoConfiguration configuration) {
        this.ff = configuration.getFilesFacade();
        this.fileOpts = configuration.getWriterFileOpenOpts();
        this.mixedIOFlag = configuration.isWriterMixedIOEnabled();
    }

    @Override
    public void addTop(long value) {
        assert value >= 0;
        columnTop += value;
    }

    @Override
    public void append(long appendOffsetRowCount, FrameColumn sourceColumn, long sourceLo, long sourceHi, int commitMode) {
        final int sourceStorageType = sourceColumn.getStorageType();
        if (sourceStorageType != COLUMN_CONTIGUOUS_FILE && sourceStorageType != COLUMN_MEMORY) {
            throw new UnsupportedOperationException();
        }

        // The source's mapping is asked for by logical row, top included.
        final long sourceRowHi = sourceHi;
        // Each side offsets by its OWN column top: a column whose data starts at a top does not hold the rows below it.
        sourceLo -= sourceColumn.getColumnTop();
        sourceHi -= sourceColumn.getColumnTop();
        appendOffsetRowCount -= columnTop;

        assert sourceLo >= 0;
        assert sourceHi >= 0;
        assert appendOffsetRowCount >= 0;

        if (sourceHi <= sourceLo) {
            return;
        }

        final long size = (sourceHi - sourceLo) << shl;
        final long srcOffset = sourceLo << shl;
        final long dstOffset = appendOffsetRowCount << shl;

        if (mixedIOFlag) {
            // reserve() allocated the plan's full extent before this positioned write; mixed I/O needs no target
            // mapping. Only a file source has an fd to copy from, so only it takes the kernel's fd-to-fd path.
            assertWriteReserved(dstOffset + size);
            if (sourceStorageType == COLUMN_CONTIGUOUS_FILE) {
                copyFromFile(sourceColumn, srcOffset, dstOffset, size);
            } else if (sourceColumn.isTimestampIndex()) {
                writeFromTimestampIndex(sourceColumn.getContiguousDataAddr(sourceHi), sourceLo, sourceHi, dstOffset);
            } else {
                ColumnWriteBuffer.write(ff, fd, sourceColumn.getContiguousDataAddr(sourceHi) + srcOffset, size, dstOffset);
            }
            if (commitMode != CommitMode.NOSYNC) {
                ff.fsync(fd);
            }
            return;
        }

        // Either source hands its rows over as an address of its row 0: a memory source already is one, and a file
        // source maps itself - once for the whole extent when its frame keeps it open, so a plan does not map it per
        // action.
        final long dstAddress = mapWritable(dstOffset + size) + dstOffset;
        if (sourceColumn.isTimestampIndex()) {
            // The designated timestamp of an O3 frame arrives as the 16-bytes-per-row sorted INDEX
            // rather than as a column, so its rows are de-interleaved out of the index instead of
            // copied.
            Vect.copyFromTimestampIndex(sourceColumn.getContiguousDataAddr(sourceHi), sourceLo, sourceHi - 1, dstAddress);
        } else {
            final long srcAddress = sourceStorageType == COLUMN_CONTIGUOUS_FILE
                    ? sourceColumn.getContiguousDataAddr(sourceRowHi)
                    : sourceColumn.getContiguousDataAddr(sourceHi);
            Vect.memcpy(dstAddress, srcAddress + srcOffset, size);
        }

        if (commitMode != CommitMode.NOSYNC) {
            TableUtils.msync(ff, dstAddress, size, commitMode == CommitMode.ASYNC);
        }
    }

    @Override
    public void merge(
            long appendOffsetRowCount,
            FrameColumn sourceColumn1,
            long source1Lo,
            long source1Hi,
            FrameColumn sourceColumn2,
            long source2Lo,
            long source2Hi,
            long mergeIndexAddr,
            long mergeIndexRows,
            int commitMode
    ) {
        // The target offsets by its OWN column top, exactly as append does: a row below the top is not in
        // the file at all, so the top is the difference between the row a caller names and the row the file
        // holds, and it is the column that knows it. Each SOURCE does the same in rowZeroAddr below.
        appendOffsetRowCount -= columnTop;

        assert appendOffsetRowCount >= 0;
        // Not an equality: a deduplicating commit drops rows, so the index is SHORTER than both sides added together.
        assert mergeIndexRows <= (source1Hi - source1Lo) + (source2Hi - source2Lo);

        final long size = mergeIndexRows << shl;

        // The shuffle picks rows by the ABSOLUTE row id the merge index carries, so each source is
        // addressed from ITS row 0 and the index does the rest. The designated timestamp reads neither
        // source: the merge index was built out of both sides' timestamps and already holds the answer.
        final boolean isTimestamp = sourceColumn2.isTimestampIndex();
        // Only the DATA side can carry a column top.
        final long src1Top = isTimestamp ? 0 : sourceColumn1.getColumnTop();
        final boolean readsBelowTop = source1Lo < source1Hi && source1Lo < src1Top;
        final long src1Address = isTimestamp ? 0
                : readsBelowTop
                  // UNBIASED: the file's first stored row IS logical row src1Top, and the kernel does the
                  // subtraction itself.
                  ? sourceColumn1.getContiguousDataAddr(source1Hi)
                  : rowZeroAddr(sourceColumn1, source1Lo, source1Hi);
        final long src2Address = isTimestamp ? 0 : rowZeroAddr(sourceColumn2, source2Lo, source2Hi);
        final long dstAddress = mapWritable((appendOffsetRowCount << shl) + size) + (appendOffsetRowCount << shl);
        long nullValueAddress = 0;
        try {
            if (isTimestamp) {
                Vect.oooCopyIndex(mergeIndexAddr, mergeIndexRows, dstAddress);
            } else if (readsBelowTop) {
                // One element wide, holding this type's NULL pattern - one kernel per width covers every
                // fixed type that way.
                nullValueAddress = Unsafe.malloc(1L << shl, MemoryTag.NATIVE_O3);
                TableUtils.setNull(columnType, nullValueAddress, 1);
                mergeShuffleWithTop(
                        src1Address,
                        src2Address,
                        dstAddress,
                        mergeIndexAddr,
                        mergeIndexRows,
                        src1Top,
                        nullValueAddress,
                        shl
                );
            } else {
                mergeShuffle(src1Address, src2Address, dstAddress, mergeIndexAddr, mergeIndexRows, shl);
            }
            if (commitMode != CommitMode.NOSYNC) {
                TableUtils.msync(ff, dstAddress, size, commitMode == CommitMode.ASYNC);
            }
        } finally {
            if (nullValueAddress != 0) {
                Unsafe.free(nullValueAddress, 1L << shl, MemoryTag.NATIVE_O3);
            }
        }
    }

    private long rowZeroAddr(FrameColumn column, long lo, long hi) {
        if (lo >= hi) {
            return 0;
        }
        final long top = column.getColumnTop();
        if (lo < top) {
            throw CairoException.critical(0).put("merge reads below a column top [column=").put(columnIndex)
                    .put(", rowLo=").put(lo)
                    .put(", columnTop=").put(top)
                    .put(']');
        }
        return column.getContiguousDataAddr(hi) - (top << shl);
    }

    private static void mergeShuffle(long src1, long src2, long dst, long mergeIndexAddr, long rows, int shl) {
        switch (shl) {
            case 0 -> Vect.mergeShuffle8Bit(src1, src2, dst, mergeIndexAddr, rows);
            case 1 -> Vect.mergeShuffle16Bit(src1, src2, dst, mergeIndexAddr, rows);
            case 2 -> Vect.mergeShuffle32Bit(src1, src2, dst, mergeIndexAddr, rows);
            case 3 -> Vect.mergeShuffle64Bit(src1, src2, dst, mergeIndexAddr, rows);
            case 4 -> Vect.mergeShuffle128Bit(src1, src2, dst, mergeIndexAddr, rows);
            case 5 -> Vect.mergeShuffle256Bit(src1, src2, dst, mergeIndexAddr, rows);
            default ->
                    throw CairoException.critical(0).put("unsupported column width for merge [shl=").put(shl).put(']');
        }
    }

    /**
     * The column-top aware counterpart of {@link #mergeShuffle}.
     */
    private static void mergeShuffleWithTop(
            long src1,
            long src2,
            long dst,
            long mergeIndexAddr,
            long rows,
            long srcDataTop,
            long pNullValue,
            int shl
    ) {
        switch (shl) {
            case 0 -> Vect.mergeShuffle8BitWithTop(src1, src2, dst, mergeIndexAddr, rows, srcDataTop, pNullValue);
            case 1 -> Vect.mergeShuffle16BitWithTop(src1, src2, dst, mergeIndexAddr, rows, srcDataTop, pNullValue);
            case 2 -> Vect.mergeShuffle32BitWithTop(src1, src2, dst, mergeIndexAddr, rows, srcDataTop, pNullValue);
            case 3 -> Vect.mergeShuffle64BitWithTop(src1, src2, dst, mergeIndexAddr, rows, srcDataTop, pNullValue);
            case 4 -> Vect.mergeShuffle128BitWithTop(src1, src2, dst, mergeIndexAddr, rows, srcDataTop, pNullValue);
            case 5 -> Vect.mergeShuffle256BitWithTop(src1, src2, dst, mergeIndexAddr, rows, srcDataTop, pNullValue);
            default ->
                    throw CairoException.critical(0).put("unsupported column width for merge [shl=").put(shl).put(']');
        }
    }

    @Override
    public void appendNulls(long rowCount, long sourceColumnTop, int commitMode) {
        rowCount -= columnTop;
        assert rowCount >= 0;
        assert sourceColumnTop >= 0;

        if (sourceColumnTop > 0) {
            final long mappedAddress = mapWritable((rowCount + sourceColumnTop) << shl) + (rowCount << shl);
            TableUtils.setNull(columnType, mappedAddress, sourceColumnTop);
            if (commitMode != CommitMode.NOSYNC) {
                TableUtils.msync(ff, mappedAddress, sourceColumnTop << shl, commitMode == CommitMode.ASYNC);
            }
        }
    }

    @Override
    public void close() {
        if (!closed) {
            if (mapAddr != 0) {
                ff.munmap(mapAddr, mapSize, MEMORY_TAG);
                mapAddr = 0;
                mapSize = 0;
            }
            if (fd > -1) {
                ff.close(fd);
                fd = -1;
            }
            writeBuffer.close();
            closed = true;

            if (recycleBin != null && !recycleBin.isClosed()) {
                recycleBin.put(this);
            }
        }
    }

    @Override
    public int getColumnIndex() {
        return columnIndex;
    }

    @Override
    public long getColumnTop() {
        return Math.min(columnTop, logicalRowHi);
    }

    @Override
    public int getColumnType() {
        return columnType;
    }

    @Override
    public long getContiguousAuxAddr(long rowHi) {
        return 0;
    }

    @Override
    public long getContiguousDataAddr(long rowHi) {
        if (rowHi <= columnTop) {
            // No data
            return 0;
        }

        mapAllRows(rowHi);
        return mapAddr;
    }

    @Override
    public long getPrimaryFd() {
        return fd;
    }

    @Override
    public long getSecondaryFd() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getStorageType() {
        return COLUMN_CONTIGUOUS_FILE;
    }

    public void ofRO(Path partitionPath, CharSequence columnName, long columnTxn, int columnType, long columnTop, int columnIndex, boolean isEmpty) {
        assert fd == -1;
        int plen = 0;

        try {
            of(columnType, columnTop, columnIndex);
            // Set whether or not there is a file to open: a pooled column must not carry its previous owner's mode.
            this.isReadOnly = true;

            if (!isEmpty) {
                plen = partitionPath.size();
                dFile(partitionPath, columnName, columnTxn);
                this.fd = TableUtils.openRO(ff, partitionPath.$(), LOG);
            }
        } catch (Throwable e) {
            close();
            throw e;
        } finally {
            if (!isEmpty) {
                partitionPath.trimTo(plen);
            }
        }
    }

    public void ofRW(Path partitionPath, CharSequence columnName, long columnTxn, int columnType, long columnTop, int columnIndex) {
        assert fd == -1;
        int plen = partitionPath.size();
        allocatedBytes = 0;
        isAllocatedBytesKnown = false;

        try {
            // Negative col top means column does not exist in the partition.
            // Create it.
            of(columnType, columnTop, columnIndex);
            dFile(partitionPath, columnName, columnTxn);
            this.fd = TableUtils.openRW(ff, partitionPath.$(), LOG, fileOpts);
            this.isReadOnly = false;
        } catch (Throwable e) {
            close();
            throw e;
        } finally {
            if (plen != 0) {
                partitionPath.trimTo(plen);
            }
        }
    }

    @Override
    public void reserve(long rowLo, long rowHi, long dataBytes, boolean isMerging) {
        // Fixed width: the rows alone say how long the file gets, whatever is written into them.
        final long rows = rowHi - columnTop;
        if (rows > 0) {
            final long size = rows << shl;
            if (mixedIOFlag) {
                ensureAllocated(size);
            } else {
                mapWritable(size);
            }
        }
    }

    @Override
    public void setReadWindow(long logicalRowHi, long mapRowHi) {
        this.logicalRowHi = logicalRowHi;
        this.mapRowHi = mapRowHi;
    }

    public void setRecycleBin(RecycleBin<FrameColumn> recycleBin) {
        assert this.recycleBin == null;
        this.recycleBin = recycleBin;
    }

    /**
     * Writable columns only. Makes the file at least {@code size} bytes long and the column's one mapping cover all
     * of it, and returns the address of the file's byte 0. A no-op when a previous call - a {@link #reserve} ahead of
     * the plan, typically - already got that far; otherwise one fallocate and one map or remap, which grows the same
     * mapping instead of adding another. The mapping can move, so an address taken before a call is stale after it.
     */
    protected long mapWritable(long size) {
        assert !isReadOnly;
        ensureAllocated(size);
        if (mapSize < allocatedBytes) {
            mapAddr = mapAddr == 0
                    ? TableUtils.mapRWNoAlloc(ff, fd, allocatedBytes, 0, MEMORY_TAG)
                    : TableUtils.mremap(ff, fd, mapAddr, mapSize, allocatedBytes, Files.MAP_RW, MEMORY_TAG);
            mapSize = allocatedBytes;
        }
        return mapAddr;
    }

    private void copyFromFile(FrameColumn sourceColumn, long srcOffset, long dstOffset, long size) {
        final long sourceFd = sourceColumn.getPrimaryFd();
        if (ff.copyData(sourceFd, fd, srcOffset, dstOffset, size) != size) {
            throw CairoException.critical(ff.errno()).put("Cannot copy data [fd=").put(fd)
                    .put(", destOffset=").put(dstOffset)
                    .put(", size=").put(size)
                    .put(", fileSize=").put(ff.length(fd))
                    .put(", srcFd=").put(sourceFd)
                    .put(", srcOffset=").put(srcOffset)
                    .put(", srcFileSize=").put(ff.length(sourceFd))
                    .put(", columnIndex=").put(columnIndex)
                    .put(", dstColumnTop=").put(columnTop)
                    .put(", srcColumnTop=").put(sourceColumn.getColumnTop())
                    .put(']');
        }
    }

    /**
     * Makes the file at least {@code size} bytes long. A no-op when a previous call already grew it that far: the file
     * only ever grows, and only through this column, so the length it reached is the length it still has. The first
     * call of an open asks the file its length, so a file already long enough is not allocated again, and every call
     * after allocates only past that length.
     */
    private void ensureAllocated(long size) {
        if (size > allocatedBytes) {
            if (!isAllocatedBytesKnown) {
                // Positioned writes may have grown the file past what was asked for so far.
                allocatedBytes = Math.max(allocatedBytes, ff.length(fd));
                isAllocatedBytesKnown = true;
                if (size <= allocatedBytes) {
                    return;
                }
            }
            size = Files.ceilPageSize(size);
            // Only the growth: the file already holds allocatedBytes, and allocating from 0 would cost every extent
            // the file has, not just the new ones.
            if (!ff.allocate(fd, allocatedBytes, size)) {
                throw CairoException.critical(ff.errno()).put("No space left [size=").put(size).put(", fd=").put(fd).put(']');
            }
            allocatedBytes = size;
        }
    }

    /**
     * Verifies that reserve() allocated the whole plan before its first positioned write.
     */
    private void assertWriteReserved(long fileOffsetHi) {
        assert isAllocatedBytesKnown;
        assert fileOffsetHi <= allocatedBytes : "positioned write exceeds reservation [column=" + columnIndex
                + ", writeHi=" + fileOffsetHi + ", allocated=" + allocatedBytes + ']';
    }

    /**
     * De-interleaves the timestamps of rows {@code [lo, hi)} out of an O3 sort index into the write buffer and writes
     * them at {@code dstOffset}, a buffer's worth at a time.
     */
    private void writeFromTimestampIndex(long indexAddr, long lo, long hi, long dstOffset) {
        final long rowsPerChunk = ColumnWriteBuffer.MAX_SIZE >> shl;
        final long buffer = writeBuffer.reserve((hi - lo) << shl);
        for (long chunkLo = lo; chunkLo < hi; chunkLo += rowsPerChunk) {
            final long chunkHi = Math.min(chunkLo + rowsPerChunk, hi);
            Vect.copyFromTimestampIndex(indexAddr, chunkLo, chunkHi - 1, buffer);
            ColumnWriteBuffer.write(ff, fd, buffer, (chunkHi - chunkLo) << shl, dstOffset + ((chunkLo - lo) << shl));
        }
    }

    private void mapAllRows(long rowHi) {
        if (!isReadOnly) {
            // Writable columns are not used yet, can be easily implemented if needed
            throw new UnsupportedOperationException("Cannot map writable column");
        }

        final long newMemSize = (Math.max(rowHi, mapRowHi) - columnTop) << shl;
        if (newMemSize <= mapSize) {
            // The mapping already covers these rows.
            return;
        }

        // Grow. A kept-open column serves one piece after another, and a later piece - or the next commit's plan,
        // while the frame is cached - can reach higher than the first did. The file only grows at its tail and every
        // caller takes the address afresh after this call, so the one mapping is remapped bigger.
        mapAddr = mapAddr == 0
                ? TableUtils.mapRO(ff, fd, newMemSize, MEMORY_TAG)
                : TableUtils.mremap(ff, fd, mapAddr, mapSize, newMemSize, Files.MAP_RO, MEMORY_TAG);
        mapSize = newMemSize;
    }

    private void of(int columnType, long columnTop, int columnIndex) {
        this.shl = ColumnType.pow2SizeOf(columnType);
        this.columnType = columnType;
        this.columnTop = columnTop;
        this.columnIndex = columnIndex;
        this.closed = false;
        // A pooled column must not carry the previous owner's window into this open.
        this.logicalRowHi = Long.MAX_VALUE;
        this.mapRowHi = 0;
    }
}
