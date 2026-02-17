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
        if (sourceColumn.getStorageType() != COLUMN_CONTIGUOUS_FILE) {
            throw new UnsupportedOperationException();
        }
        sourceLo -= sourceColumn.getColumnTop();
        sourceHi -= sourceColumn.getColumnTop();
        appendOffsetRowCount -= columnTop;

        assert sourceHi >= 0;
        assert sourceLo >= 0;
        assert appendOffsetRowCount >= 0;

        if (sourceHi > 0) {
            final long targetDataOffset = getDataAppendOffsetBytes(appendOffsetRowCount);

            // Map source offset file, it will be used to copy data from anyway.
            // sourceHi is exclusive
            long srcAuxMemSize = columnTypeDriver.getAuxVectorSize(sourceHi);

            final long srcAuxMemAddr = TableUtils.mapAppendColumnBuffer(
                    ff,
                    sourceColumn.getSecondaryFd(),
                    0,
                    srcAuxMemSize,
                    false,
                    MEMORY_TAG
            );

            try {
                long srcDataOffset = columnTypeDriver.getDataVectorOffset(srcAuxMemAddr, sourceLo);
                assert (sourceLo == 0 && srcDataOffset == 0) || (sourceLo > 0 && srcDataOffset >= columnTypeDriver.getDataVectorMinEntrySize() && srcDataOffset < 1L << 40);
                long srcDataSize = columnTypeDriver.getDataVectorSize(srcAuxMemAddr, sourceLo, sourceHi - 1);
                if (srcDataSize > 0) {
                    assert srcDataSize < 1L << 40;
                    TableUtils.allocateDiskSpaceToPage(ff, dataFd, targetDataOffset + srcDataSize);
                    if (mixedIOFlag) {
                        if (ff.copyData(sourceColumn.getPrimaryFd(), dataFd, srcDataOffset, targetDataOffset, srcDataSize) != srcDataSize) {
                            throw CairoException.critical(ff.errno()).put("Cannot copy data [fd=").put(dataFd)
                                    .put(", destOffset=").put(targetDataOffset)
                                    .put(", size=").put(srcDataSize)
                                    .put(", fileSize=").put(ff.length(dataFd))
                                    .put(", srcFd=").put(sourceColumn.getPrimaryFd())
                                    .put(", srcOffset=").put(srcDataOffset)
                                    .put(", srcFileSize=").put(ff.length(sourceColumn.getPrimaryFd()))
                                    .put(']');
                        }

                        if (commitMode != CommitMode.NOSYNC) {
                            ff.fsync(dataFd);
                        }
                    } else {
                        long srcDataAddress = 0;
                        long dstDataAddress = 0;
                        try {
                            srcDataAddress = TableUtils.mapAppendColumnBuffer(ff, sourceColumn.getPrimaryFd(), srcDataOffset, srcDataSize, false, MEMORY_TAG);
                            dstDataAddress = TableUtils.mapAppendColumnBuffer(ff, dataFd, targetDataOffset, srcDataSize, true, MEMORY_TAG);

                            Vect.memcpy(dstDataAddress, srcDataAddress, srcDataSize);

                            if (commitMode != CommitMode.NOSYNC) {
                                TableUtils.msync(ff, dstDataAddress, srcDataSize, commitMode == CommitMode.ASYNC);
                            }
                        } finally {
                            if (srcDataAddress != 0) {
                                TableUtils.mapAppendColumnBufferRelease(ff, srcDataAddress, srcDataOffset, srcDataSize, MEMORY_TAG);
                            }
                            if (dstDataAddress != 0) {
                                TableUtils.mapAppendColumnBufferRelease(ff, dstDataAddress, targetDataOffset, srcDataSize, MEMORY_TAG);
                            }
                        }
                    }
                }

                final long dstAuxOffset = columnTypeDriver.getAuxVectorOffset(appendOffsetRowCount);
                TableUtils.allocateDiskSpaceToPage(ff, auxFd, dstAuxOffset + srcAuxMemSize);
                final long dstAuxAddr = TableUtils.mapAppendColumnBuffer(ff, auxFd, dstAuxOffset, srcAuxMemSize, true, MEMORY_TAG);
                try {
                    columnTypeDriver.shiftCopyAuxVector(
                            srcDataOffset - targetDataOffset,
                            srcAuxMemAddr,
                            sourceLo,
                            sourceHi - 1, // inclusive
                            dstAuxAddr,
                            srcAuxMemSize
                    );

                    if (commitMode != CommitMode.NOSYNC) {
                        TableUtils.msync(ff, dstAuxAddr, srcAuxMemSize, commitMode == CommitMode.ASYNC);
                    }
                } finally {
                    TableUtils.mapAppendColumnBufferRelease(ff, dstAuxAddr, dstAuxOffset, srcAuxMemSize, MEMORY_TAG);
                }

                this.appendOffsetRowCount = appendOffsetRowCount + (sourceHi - sourceLo);
                this.dataAppendOffsetBytes = targetDataOffset + srcDataSize;
            } finally {
                TableUtils.mapAppendColumnBufferRelease(
                        ff,
                        srcAuxMemAddr,
                        0,
                        srcAuxMemSize,
                        MEMORY_TAG
                );
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
}
