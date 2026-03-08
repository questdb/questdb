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
        } else {
            throw new UnsupportedOperationException();
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
