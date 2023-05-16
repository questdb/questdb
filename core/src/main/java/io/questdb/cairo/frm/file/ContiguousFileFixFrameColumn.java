/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.*;
import io.questdb.cairo.frm.FrameColumn;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.dFile;

public class ContiguousFileFixFrameColumn implements FrameColumn {
    public static final int MEMORY_TAG = MemoryTag.MMAP_TABLE_WRITER;
    private static final Log LOG = LogFactory.getLog(ContiguousFileFixFrameColumn.class);
    protected final FilesFacade ff;
    private final long fileOpts;
    private int columnIndex;
    private long columnTop;
    private int columnType;
    private int fd = -1;
    private RecycleBin<ContiguousFileFixFrameColumn> recycleBin;
    private int shl;

    public ContiguousFileFixFrameColumn(CairoConfiguration configuration) {
        this.ff = configuration.getFilesFacade();
        this.fileOpts = configuration.getWriterFileOpenOpts();
    }

    @Override
    public void addTop(long value) {
        assert value >= 0;
        columnTop += value;
    }

    @Override
    public void append(long offset, FrameColumn sourceColumn, long sourceLo, long sourceHi, int commitMode) {
        if (sourceColumn.getStorageType() == COLUMN_CONTIGUOUS_FILE) {
            sourceLo -= sourceColumn.getColumnTop();
            sourceHi -= sourceColumn.getColumnTop();
            offset -= columnTop;

            assert sourceLo >= 0;
            assert sourceHi >= 0;
            assert offset >= 0;

            if (sourceHi > 0) {
                int sourceFd = sourceColumn.getPrimaryFd();
                long length = sourceHi << shl;
                TableUtils.allocateDiskSpaceToPage(ff, fd, (offset + sourceHi) << shl);
                ff.fadvise(sourceFd, sourceLo << shl, length, Files.POSIX_FADV_SEQUENTIAL);
                if (ff.copyData(sourceFd, fd, sourceLo << shl, offset << shl, length) != length) {
                    throw CairoException.critical(ff.errno()).put("Cannot copy data [fd=").put(fd)
                            .put(", destOffset=").put(offset << shl)
                            .put(", size=").put(length)
                            .put(", fileSize=").put(ff.length(fd))
                            .put(", srcFd=").put(sourceFd)
                            .put(", srcOffset=").put(sourceLo << shl)
                            .put(", srcFileSize=").put(ff.length(sourceFd))
                            .put(']');
                }
                if (commitMode != CommitMode.NOSYNC) {
                    ff.fsync(fd);
                }
            }
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public void appendNulls(long offset, long count, int commitMode) {
        offset -= columnTop;
        assert offset >= 0;
        assert count >= 0;

        if (count > 0) {
            TableUtils.allocateDiskSpaceToPage(ff, fd, (offset + count) << shl);
            long mappedAddress = TableUtils.mapAppendColumnBuffer(ff, fd, offset << shl, count << shl, true, MEMORY_TAG);
            try {
                TableUtils.setNull(columnType, mappedAddress, count);
            } finally {
                TableUtils.mapAppendColumnBufferRelease(ff, mappedAddress, offset << shl, count << shl, MEMORY_TAG);
            }
            if (commitMode != CommitMode.NOSYNC) {
                ff.fsync(fd);
            }
        }
    }

    @Override
    public void close() {
        if (fd > -1) {
            ff.close(fd);
            fd = -1;
        }
        if (!recycleBin.isClosed()) {
            recycleBin.put(this);
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
    public int getPrimaryFd() {
        return fd;
    }

    @Override
    public int getSecondaryFd() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getStorageType() {
        return COLUMN_CONTIGUOUS_FILE;
    }

    public void ofRO(Path partitionPath, CharSequence columnName, long columnTxn, int columnType, long columnTop, int columnIndex) {
        assert fd == -1;
        of(columnType, columnTop, columnIndex);

        if (columnTop >= 0) {
            int plen = partitionPath.length();
            try {
                dFile(partitionPath, columnName, columnTxn);
                this.fd = TableUtils.openRO(ff, partitionPath.$(), LOG);
            } finally {
                partitionPath.trimTo(plen);
            }
        } else {
            // Column does not exist in the partition, don't try to open the file
            this.columnTop = -columnTop;
        }
    }

    public void ofRW(Path partitionPath, CharSequence columnName, long columnTxn, int columnType, long columnTop, int columnIndex) {
        assert fd == -1;
        // Negative col top means column does not exist in the partition.
        // Create it.
        columnTop = Math.abs(columnTop);
        of(columnType, columnTop, columnIndex);

        int plen = partitionPath.length();
        try {
            dFile(partitionPath, columnName, columnTxn);
            this.fd = TableUtils.openRW(ff, partitionPath.$(), LOG, fileOpts);
        } finally {
            partitionPath.trimTo(plen);
        }
    }

    public void setPool(RecycleBin<ContiguousFileFixFrameColumn> recycleBin) {
        assert this.recycleBin == null;
        this.recycleBin = recycleBin;
    }

    private void of(int columnType, long columnTop, int columnIndex) {
        this.shl = ColumnType.pow2SizeOf(columnType);
        this.columnType = columnType;
        this.columnTop = columnTop;
        this.columnIndex = columnIndex;
    }
}
