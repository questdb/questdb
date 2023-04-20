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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.frm.FrameColumn;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.dFile;
import static io.questdb.cairo.TableUtils.iFile;

public class ContinuousFileVarFrameColumn implements FrameColumn {
    private static final Log LOG = LogFactory.getLog(ContinuousFileFixFrameColumn.class);
    private static final int MEMORY_TAG = MemoryTag.MMAP_TABLE_WRITER;
    private final FilesFacade ff;
    private final long fileOpts;
    private int columnIndex;
    private long columnTop;
    private int columnType;
    private int fixedFd = -1;
    private Pool<ContinuousFileVarFrameColumn> pool;
    private long varAppendOffset = -1;
    private int varFd = -1;
    private long varLength = -1;

    public ContinuousFileVarFrameColumn(FilesFacade ff, long fileOpts) {
        this.ff = ff;
        this.fileOpts = fileOpts;
    }

    @Override
    public void append(long offset, FrameColumn sourceColumn, long sourceOffset, long sourceSize) {
        if (sourceColumn.getStorageType() != COLUMN_CONTINUOUS_FILE) {
            throw new UnsupportedOperationException();
        }

        sourceOffset -= sourceColumn.getColumnTop();
        sourceSize -= sourceColumn.getColumnTop();
        offset -= columnTop;

        assert sourceSize >= 0;
        assert sourceOffset >= 0;
        assert offset >= 0;

        if (sourceSize > 0) {

            long varOffset = getVarOffset(offset);

            // Map source offset file, it will be used to copy data from anyway.
            long srcFixMapSize = (sourceOffset + sourceSize + 1) * Long.BYTES;
            final long srcFixAddr = TableUtils.mapAppendColumnBuffer(
                    ff,
                    sourceColumn.getSecondaryFd(),
                    sourceOffset * Long.BYTES,
                    srcFixMapSize,
                    false,
                    MEMORY_TAG
            );

            try {
                long varSrcOffset = Unsafe.getUnsafe().getLong(srcFixAddr);
                assert (sourceOffset == 0 && varSrcOffset == 0) || (sourceOffset > 0 && varSrcOffset > 0 && varSrcOffset < 1L << 40);
                long copySize = Unsafe.getUnsafe().getLong(srcFixAddr + sourceSize * Long.BYTES) - varSrcOffset;
                assert copySize > 0 && copySize < 1L << 40;

                if (!ff.truncate(varFd, varOffset + copySize)) {
                    throw CairoException.critical(ff.errno()).put("Cannot set variable file size [fd=").put(varFd).put(", size=").put(varOffset + copySize).put(']');
                }
                if (ff.copyData(sourceColumn.getPrimaryFd(), varFd, varSrcOffset, varOffset, copySize) != copySize) {
                    throw CairoException.critical(ff.errno()).put("Cannot copy data [fd=").put(varFd)
                            .put(", destOffset=").put(varOffset)
                            .put(", size=").put(copySize)
                            .put(", fileSize").put(ff.length(varFd))
                            .put(", srcFd=").put(sourceColumn.getPrimaryFd())
                            .put(", srcOffset=").put(varSrcOffset)
                            .put(", srcFileSize=").put(ff.length(sourceColumn.getPrimaryFd()))
                            .put(']');
                }

                long fixedOffset = offset * Long.BYTES;
                long fixedSize = (sourceSize - sourceOffset + 1) * Long.BYTES;
                if (!ff.truncate(fixedFd, fixedOffset + fixedSize)) {
                    throw CairoException.critical(ff.errno())
                            .put("Cannot set variable file fixed size to [fd=").put(varFd)
                            .put(", size=").put(varOffset + copySize).put(']');
                }

                long fixAddr = TableUtils.mapAppendColumnBuffer(ff, fixedFd, fixedOffset, fixedSize, true, MEMORY_TAG);
                try {
                    Vect.shiftCopyFixedSizeColumnData(
                            varSrcOffset - varOffset,
                            srcFixAddr,
                            0,
                            sourceSize,
                            fixAddr
                    );
                } finally {
                    TableUtils.mapAppendColumnBufferRelease(ff, fixAddr, fixedOffset, fixedSize, MEMORY_TAG);
                }

                varAppendOffset = offset + (sourceSize - sourceOffset);
                varLength = varOffset + copySize;
            } finally {
                TableUtils.mapAppendColumnBufferRelease(ff, srcFixAddr, sourceOffset * Long.BYTES, srcFixMapSize, MEMORY_TAG);
            }
        }
    }

    @Override
    public void appendNulls(long offset, long count) {
        offset -= columnTop;
        assert offset >= 0;

        if (count > 0) {

            long fixedOffset = (offset + 1) * Long.BYTES;
            long varOffset = getVarOffset(offset);
            long appendVarSize = count * (ColumnType.isString(columnType) ? Integer.BYTES : Long.BYTES);

            if (!ff.truncate(varFd, varOffset + appendVarSize)) {
                throw CairoException.critical(ff.errno()).put("Cannot set variable file size [fd=").put(varFd).put(", size=").put(varOffset + appendVarSize).put(']');
            }

            // Set nulls in variable file
            long varAddr = TableUtils.mapAppendColumnBuffer(ff, varFd, varOffset, appendVarSize, true, MEMORY_TAG);
            try {
                Vect.memset(varAddr, appendVarSize, -1);
            } finally {
                TableUtils.mapAppendColumnBufferRelease(ff, varAddr, varOffset, appendVarSize, MEMORY_TAG);
            }

            // Set pointers to nulls
            long fixedSize = count * Long.BYTES;
            if (!ff.truncate(fixedFd, fixedOffset + fixedSize)) {
                throw CairoException.critical(ff.errno()).put("Cannot set variable file size [fd=").put(varFd).put(", size=").put(varOffset + appendVarSize).put(']');
            }
            long fixAddr = TableUtils.mapAppendColumnBuffer(ff, fixedFd, fixedOffset, fixedSize, true, MEMORY_TAG);
            try {
                if (ColumnType.isString(columnType)) {
                    Vect.setVarColumnRefs32Bit(fixAddr, varOffset, count);
                } else {
                    Vect.setVarColumnRefs64Bit(fixAddr, varOffset, count);
                }
            } finally {
                TableUtils.mapAppendColumnBufferRelease(ff, fixAddr, fixedOffset, fixedSize, MEMORY_TAG);
            }

            varAppendOffset = offset + count;
            varLength = varOffset + appendVarSize;
        }
    }

    @Override
    public void close() {
        if (fixedFd != -1) {
            ff.close(fixedFd);
            fixedFd = -1;
        }
        if (varFd != -1) {
            ff.close(varFd);
            varFd = -1;
        }

        if (pool != null && !pool.isClosed()) {
            varAppendOffset = 0;
            varLength = 0;
            pool.put(this);
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
    public long getPrimaryAddress() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getPrimaryFd() {
        return varFd;
    }

    @Override
    public long getSecondaryAddress() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getSecondaryFd() {
        return fixedFd;
    }

    @Override
    public int getStorageType() {
        return COLUMN_CONTINUOUS_FILE;
    }

    public void ofRO(Path partitionPath, CharSequence columnName, long columnTxn, int columnType, long columnTop, int columnIndex) {
        assert fixedFd == -1;
        this.columnType = columnType;
        this.columnTop = columnTop;
        this.columnIndex = columnIndex;

        int plen = partitionPath.length();
        try {
            if (columnTop >= 0) {
                dFile(partitionPath, columnName, columnTxn);
                this.varFd = TableUtils.openRO(ff, partitionPath.$(), LOG);

                partitionPath.trimTo(plen);
                iFile(partitionPath, columnName, columnTxn);
                this.fixedFd = TableUtils.openRO(ff, partitionPath.$(), LOG);
            } else {
                // Column does not exist in the partition, don't try to open the file
                this.columnTop = -columnTop;
            }
        } finally {
            partitionPath.trimTo(plen);
        }
    }

    public void ofRW(Path partitionPath, CharSequence columnName, long columnTxn, int columnType, long columnTop, int columnIndex) {
        assert fixedFd == -1;
        // Negative col top means column does not exist in the partition.
        // Create it.
        columnTop = Math.abs(columnTop);
        this.columnType = columnType;
        this.columnTop = columnTop;
        this.columnIndex = columnIndex;

        int plen = partitionPath.length();
        try {
            dFile(partitionPath, columnName, columnTxn);
            this.varFd = TableUtils.openRW(ff, partitionPath.$(), LOG, fileOpts);

            partitionPath.trimTo(plen);
            iFile(partitionPath, columnName, columnTxn);
            this.fixedFd = TableUtils.openRW(ff, partitionPath.$(), LOG, fileOpts);
        } finally {
            partitionPath.trimTo(plen);
        }
    }

    @Override
    public void setAddTop(long value) {
        this.columnTop += value;
    }

    public void setPool(Pool<ContinuousFileVarFrameColumn> pool) {
        assert this.pool == null;
        this.pool = pool;
    }

    private long getVarOffset(long offset) {
        if (varAppendOffset != offset) {
            varLength = readVarOffset(fixedFd, offset);
            varAppendOffset = offset;
            return varLength;
        } else {
            return varLength;
        }
    }

    private long readVarOffset(int fixedFd, long offset) {
        long fixedOffset = offset * Long.BYTES;
        long varOffset = offset > 0 ? ff.readNonNegativeLong(fixedFd, fixedOffset) : 0;
        if (varOffset < 0 || varOffset > 1L << 40 || (offset > 0 && varOffset == 0)) {
            throw CairoException.critical(ff.errno())
                    .put("Invalid variable file length offset read from offset file [fixedFd=").put(fixedFd)
                    .put(", offset=").put(fixedOffset)
                    .put(", fileLen=").put(ff.length(fixedFd))
                    .put(", result=").put(varOffset)
                    .put(']');
        }
        return varOffset;
    }
}
