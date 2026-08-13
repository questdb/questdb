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
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Vect;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.dFile;

public class ContiguousFileFixFrameColumn implements FrameColumn {
    public static final int MEMORY_TAG = MemoryTag.MMAP_TABLE_WRITER;
    private static final Log LOG = LogFactory.getLog(ContiguousFileFixFrameColumn.class);
    protected final FilesFacade ff;
    private final int fileOpts;
    private final boolean mixedIOFlag;
    // Introduce a flag to avoid double close, which will lead to very serious consequences.
    protected boolean closed;
    private int columnIndex;
    private long columnTop;
    private int columnType;
    private long fd = -1;
    private boolean isReadOnly;
    private long mapAddr;
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
        if (sourceColumn.getStorageType() == COLUMN_CONTIGUOUS_FILE) {
            sourceLo -= sourceColumn.getColumnTop();
            sourceHi -= sourceColumn.getColumnTop();
            appendOffsetRowCount -= columnTop;

            assert sourceLo >= 0;
            assert sourceHi >= 0;
            assert appendOffsetRowCount >= 0;

            if (sourceHi > 0) {
                long sourceFd = sourceColumn.getPrimaryFd();
                long size = (sourceHi - sourceLo) << shl;
                TableUtils.allocateDiskSpaceToPage(ff, fd, (appendOffsetRowCount << shl) + size);
                if (mixedIOFlag) {
                    if (ff.copyData(sourceFd, fd, sourceLo << shl, appendOffsetRowCount << shl, size) != size) {
                        throw CairoException.critical(ff.errno()).put("Cannot copy data [fd=").put(fd)
                                .put(", destOffset=").put(appendOffsetRowCount << shl)
                                .put(", size=").put(size)
                                .put(", fileSize=").put(ff.length(fd))
                                .put(", srcFd=").put(sourceFd)
                                .put(", srcOffset=").put(sourceLo << shl)
                                .put(", srcFileSize=").put(ff.length(sourceFd))
                                .put(']');
                    }
                    if (commitMode != CommitMode.NOSYNC) {
                        ff.fsync(fd);
                    }
                } else {
                    long srcAddress = 0;
                    long dstAddress = 0;
                    try {
                        srcAddress = TableUtils.mapAppendColumnBuffer(ff, sourceFd, sourceLo << shl, size, false, MEMORY_TAG);
                        dstAddress = TableUtils.mapAppendColumnBuffer(ff, fd, appendOffsetRowCount << shl, size, true, MEMORY_TAG);

                        Vect.memcpy(dstAddress, srcAddress, size);

                        if (commitMode != CommitMode.NOSYNC) {
                            TableUtils.msync(ff, dstAddress, size, commitMode == CommitMode.ASYNC);
                        }
                    } finally {
                        if (srcAddress != 0) {
                            TableUtils.mapAppendColumnBufferRelease(ff, srcAddress, sourceLo << shl, size, MEMORY_TAG);
                        }
                        if (dstAddress != 0) {
                            TableUtils.mapAppendColumnBufferRelease(ff, dstAddress, appendOffsetRowCount << shl, size, MEMORY_TAG);
                        }
                    }
                }
            }
        } else if (sourceColumn.getStorageType() == COLUMN_MEMORY) {
            // The O3 buffers. They are already in timestamp order by the time a partition task sees them,
            // so the slice goes down as one run - the same contiguous copy the per-column O3 path makes for
            // a pure-O3 block.
            appendOffsetRowCount -= columnTop;
            assert sourceLo >= 0;
            assert appendOffsetRowCount >= 0;

            if (sourceHi > sourceLo) {
                final long size = (sourceHi - sourceLo) << shl;
                TableUtils.allocateDiskSpaceToPage(ff, fd, (appendOffsetRowCount << shl) + size);
                long dstAddress = 0;
                try {
                    dstAddress = TableUtils.mapAppendColumnBuffer(ff, fd, appendOffsetRowCount << shl, size, true, MEMORY_TAG);
                    if (sourceColumn.isTimestampIndex()) {
                        // The designated timestamp arrives as the 16-bytes-per-row sorted INDEX rather than
                        // as a column, so its rows are de-interleaved out of the index instead of copied.
                        Vect.copyFromTimestampIndex(sourceColumn.getContiguousDataAddr(sourceHi), sourceLo, sourceHi - 1, dstAddress);
                    } else {
                        Vect.memcpy(dstAddress, sourceColumn.getContiguousDataAddr(sourceHi) + (sourceLo << shl), size);
                    }
                    if (commitMode != CommitMode.NOSYNC) {
                        TableUtils.msync(ff, dstAddress, size, commitMode == CommitMode.ASYNC);
                    }
                } finally {
                    if (dstAddress != 0) {
                        TableUtils.mapAppendColumnBufferRelease(ff, dstAddress, appendOffsetRowCount << shl, size, MEMORY_TAG);
                    }
                }
            }
        } else {
            throw new UnsupportedOperationException();
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
        assert (source1Hi - source1Lo) + (source2Hi - source2Lo) == mergeIndexRows;

        final long size = mergeIndexRows << shl;
        TableUtils.allocateDiskSpaceToPage(ff, fd, (appendOffsetRowCount << shl) + size);

        // The shuffle picks rows by the ABSOLUTE row id the merge index carries, so each source is
        // addressed from ITS row 0 and the index does the rest. The designated timestamp reads neither
        // source: the merge index was built out of both sides' timestamps and already holds the answer.
        final boolean isTimestamp = sourceColumn2.isTimestampIndex();
        final long src1Address = isTimestamp ? 0 : rowZeroAddr(sourceColumn1, source1Lo, source1Hi);
        final long src2Address = isTimestamp ? 0 : rowZeroAddr(sourceColumn2, source2Lo, source2Hi);
        long dstAddress = 0;
        try {
            dstAddress = TableUtils.mapAppendColumnBuffer(ff, fd, appendOffsetRowCount << shl, size, true, MEMORY_TAG);
            if (isTimestamp) {
                Vect.oooCopyIndex(mergeIndexAddr, mergeIndexRows, dstAddress);
            } else {
                mergeShuffle(src1Address, src2Address, dstAddress, mergeIndexAddr, mergeIndexRows, shl);
            }
            if (commitMode != CommitMode.NOSYNC) {
                TableUtils.msync(ff, dstAddress, size, commitMode == CommitMode.ASYNC);
            }
        } finally {
            if (dstAddress != 0) {
                TableUtils.mapAppendColumnBufferRelease(ff, dstAddress, appendOffsetRowCount << shl, size, MEMORY_TAG);
            }
        }
    }

    /**
     * The address the source's row 0 WOULD be at, which is what the merge index's absolute row ids address.
     * <p>
     * A column whose data starts at a top does not hold the rows below it, so its mapping begins that many
     * rows in and the base steps back by the same amount. That leaves the returned address pointing outside
     * the mapping, which is safe only because no row below the top is ever read - and that is exactly what
     * the check below enforces.
     */
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

    /**
     * The O3 merge kernels, picked by column width. These are the same routines the per-column O3 copy path
     * uses; what changes here is only who calls them and with what - two frame columns and an index, rather
     * than a task carrying two dozen scalars.
     */
    private static void mergeShuffle(long src1, long src2, long dst, long mergeIndexAddr, long rows, int shl) {
        switch (shl) {
            case 0 -> Vect.mergeShuffle8Bit(src1, src2, dst, mergeIndexAddr, rows);
            case 1 -> Vect.mergeShuffle16Bit(src1, src2, dst, mergeIndexAddr, rows);
            case 2 -> Vect.mergeShuffle32Bit(src1, src2, dst, mergeIndexAddr, rows);
            case 3 -> Vect.mergeShuffle64Bit(src1, src2, dst, mergeIndexAddr, rows);
            case 4 -> Vect.mergeShuffle128Bit(src1, src2, dst, mergeIndexAddr, rows);
            case 5 -> Vect.mergeShuffle256Bit(src1, src2, dst, mergeIndexAddr, rows);
            default -> throw CairoException.critical(0).put("unsupported column width for merge [shl=").put(shl).put(']');
        }
    }

    @Override
    public void appendNulls(long rowCount, long sourceColumnTop, int commitMode) {
        rowCount -= columnTop;
        assert rowCount >= 0;
        assert sourceColumnTop >= 0;

        if (sourceColumnTop > 0) {
            TableUtils.allocateDiskSpaceToPage(ff, fd, (rowCount + sourceColumnTop) << shl);
            long mappedAddress = TableUtils.mapAppendColumnBuffer(ff, fd, rowCount << shl, sourceColumnTop << shl, true, MEMORY_TAG);
            try {
                TableUtils.setNull(columnType, mappedAddress, sourceColumnTop);
                if (commitMode != CommitMode.NOSYNC) {
                    TableUtils.msync(ff, mappedAddress, sourceColumnTop << shl, commitMode == CommitMode.ASYNC);
                }
            } finally {
                TableUtils.mapAppendColumnBufferRelease(ff, mappedAddress, rowCount << shl, sourceColumnTop << shl, MEMORY_TAG);
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
        return columnTop;
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

            if (!isEmpty) {
                plen = partitionPath.size();
                dFile(partitionPath, columnName, columnTxn);
                this.fd = TableUtils.openRO(ff, partitionPath.$(), LOG);
                this.isReadOnly = true;
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

    public void setRecycleBin(RecycleBin<FrameColumn> recycleBin) {
        assert this.recycleBin == null;
        this.recycleBin = recycleBin;
    }

    private void mapAllRows(long rowHi) {
        if (!isReadOnly) {
            // Writable columns are not used yet, can be easily implemented if needed
            throw new UnsupportedOperationException("Cannot map writable column");
        }

        long newMemSize = (rowHi - columnTop) << shl;
        if (mapSize > 0) {
            if (mapSize <= newMemSize) {
                // Already mapped to same or bigger size
                return;
            }

            // We can handle remaps, but so far there was no case for it.
            throw new UnsupportedOperationException("Remap not supported for frame columns yet");
        }

        mapSize = newMemSize;
        if (newMemSize > 0) {
            mapAddr = TableUtils.mapRO(ff, fd, mapSize, MEMORY_TAG);
        }
    }

    private void of(int columnType, long columnTop, int columnIndex) {
        this.shl = ColumnType.pow2SizeOf(columnType);
        this.columnType = columnType;
        this.columnTop = columnTop;
        this.columnIndex = columnIndex;
        this.closed = false;
    }
}
