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
import io.questdb.cairo.ColumnTypeDriver;
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
import static io.questdb.cairo.TableUtils.iFile;

public class ContiguousFileVarFrameColumn implements FrameColumn {
    private static final Log LOG = LogFactory.getLog(ContiguousFileFixFrameColumn.class);
    private static final int MEMORY_TAG = MemoryTag.MMAP_TABLE_WRITER;
    private final FilesFacade ff;
    private final int fileOpts;
    private final boolean mixedIOFlag;
    private long appendOffsetRowCount = -1;
    private long auxFd = -1;
    private long auxMapAddr;
    private long auxMapSize;
    private boolean closed = false;
    private int columnIndex;
    private long columnTop;
    private int columnType;
    private ColumnTypeDriver columnTypeDriver;
    private long dataAppendOffsetBytes = -1;
    private long dataFd = -1;
    private long dataMapAddr;
    private long dataMapSize;
    private boolean isReadOnly;
    private RecycleBin<FrameColumn> recycleBin;

    public ContiguousFileVarFrameColumn(CairoConfiguration configuration) {
        this.ff = configuration.getFilesFacade();
        this.fileOpts = configuration.getWriterFileOpenOpts();
        this.mixedIOFlag = configuration.isWriterMixedIOEnabled();
    }

    @Override
    public void addTop(long value) {
        this.columnTop += value;
    }

    @Override
    public void append(long appendOffsetRowCount, FrameColumn sourceColumn, long sourceLo, long sourceHi, int commitMode) {
        final int sourceStorageType = sourceColumn.getStorageType();
        if (sourceStorageType != COLUMN_CONTIGUOUS_FILE && sourceStorageType != COLUMN_MEMORY) {
            throw new UnsupportedOperationException();
        }

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

        // A file source hands its aux vector over as a mapping of its own and has it released afterwards; a
        // memory source is already addressable, so it maps nothing. That is the whole difference between the
        // two, and everything below reads one address either way.
        final boolean isSourceMapped = sourceStorageType == COLUMN_CONTIGUOUS_FILE;
        // sourceHi is exclusive, so this covers every entry the copy reads.
        final long srcAuxMapSize = isSourceMapped ? columnTypeDriver.getAuxVectorSize(sourceHi) : 0;
        final long srcAuxAddr = isSourceMapped
                ? TableUtils.mapAppendColumnBuffer(ff, sourceColumn.getSecondaryFd(), 0, srcAuxMapSize, false, MEMORY_TAG)
                : sourceColumn.getContiguousAuxAddr(sourceHi);
        try {
            final long targetDataOffset = getDataAppendOffsetBytes(appendOffsetRowCount);
            final long srcDataOffset = columnTypeDriver.getDataVectorOffset(srcAuxAddr, sourceLo);
            assert (sourceLo == 0 && srcDataOffset == 0) || (sourceLo > 0 && srcDataOffset >= columnTypeDriver.getDataVectorMinEntrySize() && srcDataOffset < 1L << 40);
            final long srcDataSize = columnTypeDriver.getDataVectorSize(srcAuxAddr, sourceLo, sourceHi - 1);

            if (srcDataSize > 0) {
                assert srcDataSize < 1L << 40;
                TableUtils.allocateDiskSpaceToPage(ff, dataFd, targetDataOffset + srcDataSize);
                appendData(sourceColumn, isSourceMapped, sourceHi, srcDataOffset, srcDataSize, targetDataOffset, commitMode);
            }

            final long dstAuxOffset = columnTypeDriver.getAuxVectorOffset(appendOffsetRowCount);
            final long dstAuxSize = columnTypeDriver.getAuxVectorSize(sourceHi - sourceLo);
            TableUtils.allocateDiskSpaceToPage(ff, auxFd, dstAuxOffset + dstAuxSize);
            long dstAuxAddr = 0;
            try {
                dstAuxAddr = TableUtils.mapAppendColumnBuffer(ff, auxFd, dstAuxOffset, dstAuxSize, true, MEMORY_TAG);
                columnTypeDriver.shiftCopyAuxVector(
                        srcDataOffset - targetDataOffset,
                        srcAuxAddr,
                        sourceLo,
                        sourceHi - 1, // inclusive
                        dstAuxAddr,
                        dstAuxSize
                );
                if (commitMode != CommitMode.NOSYNC) {
                    TableUtils.msync(ff, dstAuxAddr, dstAuxSize, commitMode == CommitMode.ASYNC);
                }
            } finally {
                if (dstAuxAddr != 0) {
                    TableUtils.mapAppendColumnBufferRelease(ff, dstAuxAddr, dstAuxOffset, dstAuxSize, MEMORY_TAG);
                }
            }

            this.appendOffsetRowCount = appendOffsetRowCount + (sourceHi - sourceLo);
            this.dataAppendOffsetBytes = targetDataOffset + srcDataSize;
        } finally {
            if (isSourceMapped) {
                TableUtils.mapAppendColumnBufferRelease(ff, srcAuxAddr, 0, srcAuxMapSize, MEMORY_TAG);
            }
        }
    }

    /**
     * Copies one contiguous run of the source's DATA vector to {@code targetDataOffset} in this column's data file.
     */
    private void appendData(
            FrameColumn sourceColumn,
            boolean isSourceMapped,
            long sourceHi,
            long srcDataOffset,
            long srcDataSize,
            long targetDataOffset,
            int commitMode
    ) {
        final long sourceFd = sourceColumn.getPrimaryFd();
        if (isSourceMapped && mixedIOFlag) {
            if (ff.copyData(sourceFd, dataFd, srcDataOffset, targetDataOffset, srcDataSize) != srcDataSize) {
                throw CairoException.critical(ff.errno()).put("Cannot copy data [fd=").put(dataFd)
                        .put(", destOffset=").put(targetDataOffset)
                        .put(", size=").put(srcDataSize)
                        .put(", fileSize=").put(ff.length(dataFd))
                        .put(", srcFd=").put(sourceFd)
                        .put(", srcOffset=").put(srcDataOffset)
                        .put(", srcFileSize=").put(ff.length(sourceFd))
                        .put(']');
            }
            if (commitMode != CommitMode.NOSYNC) {
                ff.fsync(dataFd);
            }
            return;
        }

        long srcDataAddress = 0;
        long dstDataAddress = 0;
        try {
            srcDataAddress = isSourceMapped
                    ? TableUtils.mapAppendColumnBuffer(ff, sourceFd, srcDataOffset, srcDataSize, false, MEMORY_TAG)
                    : sourceColumn.getContiguousDataAddr(sourceHi) + srcDataOffset;
            dstDataAddress = TableUtils.mapAppendColumnBuffer(ff, dataFd, targetDataOffset, srcDataSize, true, MEMORY_TAG);

            Vect.memcpy(dstDataAddress, srcDataAddress, srcDataSize);

            if (commitMode != CommitMode.NOSYNC) {
                TableUtils.msync(ff, dstDataAddress, srcDataSize, commitMode == CommitMode.ASYNC);
            }
        } finally {
            if (isSourceMapped && srcDataAddress != 0) {
                TableUtils.mapAppendColumnBufferRelease(ff, srcDataAddress, srcDataOffset, srcDataSize, MEMORY_TAG);
            }
            if (dstDataAddress != 0) {
                TableUtils.mapAppendColumnBufferRelease(ff, dstDataAddress, targetDataOffset, srcDataSize, MEMORY_TAG);
            }
        }
    }

    @Override
    public void appendNulls(long rowCount, long sourceColumnTop, int commitMode) {
        rowCount -= columnTop;
        assert rowCount >= 0;

        if (sourceColumnTop > 0) {
            long targetDataOffset = getDataAppendOffsetBytes(rowCount);
            long srcDataSize = sourceColumnTop * columnTypeDriver.getDataVectorMinEntrySize();
            if (srcDataSize > 0) {
                TableUtils.allocateDiskSpaceToPage(ff, dataFd, targetDataOffset + srcDataSize);

                // Set nulls in variable file
                long targetDataMemAddr = TableUtils.mapAppendColumnBuffer(ff, dataFd, targetDataOffset, srcDataSize, true, MEMORY_TAG);
                try {
                    columnTypeDriver.setDataVectorEntriesToNull(targetDataMemAddr, sourceColumnTop);

                    if (commitMode != CommitMode.NOSYNC) {
                        TableUtils.msync(ff, targetDataMemAddr, srcDataSize, commitMode == CommitMode.ASYNC);
                    }
                } finally {
                    TableUtils.mapAppendColumnBufferRelease(ff, targetDataMemAddr, targetDataOffset, srcDataSize, MEMORY_TAG);
                }

                // Cache the new data append offset
                this.appendOffsetRowCount = rowCount + sourceColumnTop;
                this.dataAppendOffsetBytes = targetDataOffset + srcDataSize;
            }

            // Set pointers to nulls
            long srcAuxSize = columnTypeDriver.getAuxVectorSize(sourceColumnTop);
            long dstAuxOffset = columnTypeDriver.getAuxVectorSize(rowCount);
            TableUtils.allocateDiskSpaceToPage(ff, auxFd, dstAuxOffset + srcAuxSize);
            long targetAuxMemAddr = TableUtils.mapAppendColumnBuffer(ff, auxFd, dstAuxOffset, srcAuxSize, true, MEMORY_TAG);
            try {
                // We need to write pointer to nulls in aux vector.
                // If the destination is empty (0 rows) and we need to write 1 null
                // then we need to write value -1 to offset 0 (targetDataOffset) in data vector
                // and value 4 (targetDataOffset + columnTypeDriver.getDataVectorMinEntrySize()) at offset 8 (dstAuxOffset) at aux vector.
                columnTypeDriver.setPartAuxVectorNull(
                        targetAuxMemAddr,
                        targetDataOffset + columnTypeDriver.getDataVectorMinEntrySize(),
                        sourceColumnTop
                );
                if (commitMode != CommitMode.NOSYNC) {
                    TableUtils.msync(ff, targetAuxMemAddr, srcAuxSize, commitMode == CommitMode.ASYNC);
                }
            } finally {
                TableUtils.mapAppendColumnBufferRelease(ff, targetAuxMemAddr, dstAuxOffset, srcAuxSize, MEMORY_TAG);
            }
        }
    }

    @Override
    public void close() {
        if (!closed) {
            if (auxMapAddr != 0) {
                ff.munmap(auxMapAddr, auxMapSize, MEMORY_TAG);
                auxMapAddr = 0;
                auxMapSize = 0;
            }

            if (dataMapAddr != 0) {
                ff.munmap(dataMapAddr, dataMapSize, MEMORY_TAG);
                dataMapAddr = 0;
                dataMapSize = 0;
            }

            if (auxFd != -1) {
                ff.close(auxFd);
                auxFd = -1;
            }
            if (dataFd != -1) {
                ff.close(dataFd);
                dataFd = -1;
            }
            closed = true;

            if (recycleBin != null && !recycleBin.isClosed()) {
                appendOffsetRowCount = 0;
                dataAppendOffsetBytes = 0;
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
        if (rowHi <= columnTop) {
            return 0;
        }

        mapAllRows(rowHi);
        return auxMapAddr;
    }

    @Override
    public long getContiguousDataAddr(long rowHi) {
        if (rowHi <= columnTop) {
            return 0;
        }

        mapAllRows(rowHi);
        return dataMapAddr;
    }

    @Override
    public long getPrimaryFd() {
        return dataFd;
    }

    @Override
    public long getSecondaryFd() {
        return auxFd;
    }

    @Override
    public int getStorageType() {
        return COLUMN_CONTIGUOUS_FILE;
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
        // The target offsets by its OWN column top, exactly as append does; each SOURCE does the same in
        // rowZeroAuxAddr below.
        appendOffsetRowCount -= columnTop;

        assert appendOffsetRowCount >= 0;
        // Not an equality: a deduplicating commit drops rows, so the index is SHORTER than both sides added together.
        assert mergeIndexRows <= (source1Hi - source1Lo) + (source2Hi - source2Lo);

        if (mergeIndexRows == 0) {
            return;
        }

        // Only the DATA side can carry a column top - the O3 side is a batch this commit is writing now, so
        // every row of it exists. When the slice reaches below that top the top-aware kernel takes the top
        // directly and emits this type's NULL for the rows underneath.
        final long src1Top = sourceColumn1.getColumnTop();
        final boolean readsBelowTop = source1Lo < source1Hi && source1Lo < src1Top;
        // Both vectors are addressed from ROW 0 and BYTE 0 of their own column: the merge index carries absolute row
        // ids, and an aux entry carries an absolute data offset, so neither side is relative to the slice being read.
        final long src1AuxAddr = readsBelowTop
                ? sourceColumn1.getContiguousAuxAddr(source1Hi)
                : rowZeroAuxAddr(sourceColumn1, source1Lo, source1Hi);
        final long src2AuxAddr = rowZeroAuxAddr(sourceColumn2, source2Lo, source2Hi);
        final long src1DataAddr = source1Lo < source1Hi ? sourceColumn1.getContiguousDataAddr(source1Hi) : 0;
        final long src2DataAddr = source2Lo < source2Hi ? sourceColumn2.getContiguousDataAddr(source2Hi) : 0;

        // Every row of both slices is written out exactly once and carries its own bytes with it, so the merged image
        // is as long as the two slices put together - the interleaving moves bytes around but creates none.
        final long belowTopRows = readsBelowTop ? Math.min(source1Hi, src1Top) - source1Lo : 0;
        final long dataSize = (readsBelowTop
                ? sourceDataSize(src1AuxAddr, Math.max(source1Lo, src1Top) - src1Top, source1Hi - src1Top)
                  + belowTopRows * columnTypeDriver.getDataVectorMinEntrySize()
                : sourceDataSize(src1AuxAddr, source1Lo, source1Hi))
                + sourceDataSize(src2AuxAddr, source2Lo, source2Hi);
        final long targetDataOffset = getDataAppendOffsetBytes(appendOffsetRowCount);
        final long dstAuxOffset = columnTypeDriver.getAuxVectorOffset(appendOffsetRowCount);
        final long dstAuxSize = columnTypeDriver.getAuxVectorSize(mergeIndexRows);

        TableUtils.allocateDiskSpaceToPage(ff, auxFd, dstAuxOffset + dstAuxSize);
        if (dataSize > 0) {
            TableUtils.allocateDiskSpaceToPage(ff, dataFd, targetDataOffset + dataSize);
        }

        long dstAuxAddr = 0;
        long dstDataAddr = 0;
        try {
            dstAuxAddr = TableUtils.mapAppendColumnBuffer(ff, auxFd, dstAuxOffset, dstAuxSize, true, MEMORY_TAG);
            if (dataSize > 0) {
                dstDataAddr = TableUtils.mapAppendColumnBuffer(ff, dataFd, targetDataOffset, dataSize, true, MEMORY_TAG);
            }
            // The kernel writes ABSOLUTE data offsets into the aux entries and addresses its writes by the same value,
            // so it is handed the address byte 0 of the data file would be at.
            final long dstDataBase = dstDataAddr != 0 ? dstDataAddr - targetDataOffset : 0;
            if (readsBelowTop) {
                columnTypeDriver.o3ColumnMergeWithTop(
                        mergeIndexAddr,
                        mergeIndexRows,
                        src1Top,
                        src1AuxAddr,
                        src1DataAddr,
                        src2AuxAddr,
                        src2DataAddr,
                        dstAuxAddr,
                        dstDataBase,
                        targetDataOffset
                );
            } else {
                columnTypeDriver.o3ColumnMerge(
                        mergeIndexAddr,
                        mergeIndexRows,
                        src1AuxAddr,
                        src1DataAddr,
                        src2AuxAddr,
                        src2DataAddr,
                        dstAuxAddr,
                        dstDataBase,
                        targetDataOffset
                );
            }

            if (commitMode != CommitMode.NOSYNC) {
                TableUtils.msync(ff, dstAuxAddr, dstAuxSize, commitMode == CommitMode.ASYNC);
                if (dstDataAddr != 0) {
                    TableUtils.msync(ff, dstDataAddr, dataSize, commitMode == CommitMode.ASYNC);
                }
            }
        } finally {
            if (dstAuxAddr != 0) {
                TableUtils.mapAppendColumnBufferRelease(ff, dstAuxAddr, dstAuxOffset, dstAuxSize, MEMORY_TAG);
            }
            if (dstDataAddr != 0) {
                TableUtils.mapAppendColumnBufferRelease(ff, dstDataAddr, targetDataOffset, dataSize, MEMORY_TAG);
            }
        }

        this.appendOffsetRowCount = appendOffsetRowCount + mergeIndexRows;
        this.dataAppendOffsetBytes = targetDataOffset + dataSize;
    }

    public void ofRO(Path partitionPath, CharSequence columnName, long columnTxn, int columnType, long columnTop, int columnIndex, boolean isEmpty) {
        assert auxFd == -1;
        closed = false;
        int plen = partitionPath.size();

        try {
            this.columnType = columnType;
            this.columnTypeDriver = ColumnType.getDriver(columnType);
            this.columnTop = columnTop;
            this.columnIndex = columnIndex;
            this.appendOffsetRowCount = -1;

            if (!isEmpty) {
                dFile(partitionPath, columnName, columnTxn);
                this.dataFd = TableUtils.openRO(ff, partitionPath.$(), LOG);
                partitionPath.trimTo(plen);
                iFile(partitionPath, columnName, columnTxn);
                this.auxFd = TableUtils.openRO(ff, partitionPath.$(), LOG);
            }
            this.isReadOnly = true;
        } catch (Exception e) {
            close();
            throw e;
        } finally {
            partitionPath.trimTo(plen);
        }
    }

    public void ofRW(Path partitionPath, CharSequence columnName, long columnTxn, int columnType, long columnTop, int columnIndex) {
        assert auxFd == -1;
        closed = false;
        int plen = partitionPath.size();

        try {
            // Negative col top means column does not exist in the partition.
            // Create it.
            this.columnType = columnType;
            this.columnTypeDriver = ColumnType.getDriver(columnType);
            this.columnTop = columnTop;
            this.columnIndex = columnIndex;
            this.appendOffsetRowCount = -1;

            dFile(partitionPath, columnName, columnTxn);
            this.dataFd = TableUtils.openRW(ff, partitionPath.$(), LOG, fileOpts);
            partitionPath.trimTo(plen);
            iFile(partitionPath, columnName, columnTxn);
            this.auxFd = TableUtils.openRW(ff, partitionPath.$(), LOG, fileOpts);
            this.isReadOnly = false;
        } catch (Throwable e) {
            close();
            throw e;
        } finally {
            partitionPath.trimTo(plen);
        }
    }

    public void setRecycleBin(RecycleBin<FrameColumn> recycleBin) {
        assert this.recycleBin == null;
        this.recycleBin = recycleBin;
    }

    private long getDataAppendOffsetBytes(long appendOffsetRowCount) {
        // cache repeated calls to this method provided the append offset row count is the same
        if (this.appendOffsetRowCount != appendOffsetRowCount) {
            dataAppendOffsetBytes = columnTypeDriver.getDataVectorSizeAtFromFd(ff, auxFd, appendOffsetRowCount - 1);
            this.appendOffsetRowCount = appendOffsetRowCount;
        }
        return dataAppendOffsetBytes;
    }

    private void mapAllRows(long rowHi) {
        if (!isReadOnly) {
            // Writable columns are not used yet, can be easily implemented if needed
            throw new UnsupportedOperationException("Cannot map writable column");
        }

        long newAuxMemSize = columnTypeDriver.getAuxVectorSize(rowHi - columnTop);
        if (auxMapSize > 0) {
            if (auxMapSize <= newAuxMemSize) {
                // Already mapped to same or bigger size
                return;
            }

            // We can handle remaps, but so far there was no case for it.
            throw new UnsupportedOperationException("Remap not supported for frame columns yet");
        }

        auxMapSize = newAuxMemSize;
        if (newAuxMemSize > 0) {
            auxMapAddr = TableUtils.mapRO(ff, auxFd, auxMapSize, 0, MEMORY_TAG);
        }

        dataMapSize = columnTypeDriver.getDataVectorSize(auxMapAddr, 0, rowHi - columnTop - 1);
        if (dataMapSize > 0) {
            dataMapAddr = TableUtils.mapRO(ff, dataFd, dataMapSize, 0, MEMORY_TAG);
        }
    }

    /**
     * The address the source's row 0 WOULD be at in its AUX vector, which is what the merge index's absolute row ids
     * address.
     */
    private long rowZeroAuxAddr(FrameColumn column, long lo, long hi) {
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
        return column.getContiguousAuxAddr(hi) - columnTypeDriver.getAuxVectorOffset(top);
    }

    private long sourceDataSize(long auxAddr, long lo, long hi) {
        return lo < hi ? columnTypeDriver.getDataVectorSize(auxAddr, lo, hi - 1) : 0;
    }
}
