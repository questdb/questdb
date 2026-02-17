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

package io.questdb.cairo.wal;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypeConverter;
import io.questdb.cairo.ColumnTypeDriver;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.griffin.SymbolMapWriterLite;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Transient;
import io.questdb.std.Vect;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.TableUtils.*;

public class CopyWalSegmentUtils {
    private static final Log LOG = LogFactory.getLog(CopyWalSegmentUtils.class);
    private static final int MEMORY_TAG = MemoryTag.MMAP_TABLE_WAL_WRITER;

    public static void rollColumnToSegment(
            FilesFacade ff,
            int options,
            MemoryMA primaryColumn,
            MemoryMA secondaryColumn,
            @Transient Path walPath,
            int newSegment,
            CharSequence columnName,
            int columnType,
            long startRowNumber,
            long rowCount,
            SegmentColumnRollSink columnRollSink,
            int commitMode,
            int newColumnType,
            @Nullable SymbolTable symbolTable,
            @Nullable SymbolMapWriterLite symbolMapWriter
    ) {
        Path newSegPath = Path.PATH.get().of(walPath).slash().put(newSegment);
        int setPathRoot = newSegPath.size();
        long primaryFd = openRW(ff, dFile(newSegPath, columnName, COLUMN_NAME_TXN_NONE), LOG, options);
        columnRollSink.setDestPrimaryFd(primaryFd);

        long secondaryFd;
        if (ColumnType.isVarSize(newColumnType)) {
            secondaryFd = openRW(ff, iFile(newSegPath.trimTo(setPathRoot), columnName, COLUMN_NAME_TXN_NONE), LOG, options);
        } else {
            secondaryFd = -1;
        }
        columnRollSink.setDestSecondaryFd(secondaryFd);

        boolean success;
        if (columnType == newColumnType) {
            if (ColumnType.isVarSize(columnType)) {
                success = copyVarSizeFiles(
                        ff,
                        columnType,
                        primaryColumn,
                        secondaryColumn,
                        primaryFd,
                        secondaryFd,
                        startRowNumber,
                        rowCount,
                        columnRollSink,
                        commitMode
                );
            } else if (columnType > 0) {
                success = copyFixLenFile(
                        ff,
                        primaryColumn,
                        primaryFd,
                        startRowNumber,
                        rowCount,
                        columnType,
                        columnRollSink,
                        commitMode
                );
            } else {
                success = copyTimestampFile(
                        ff,
                        primaryColumn,
                        primaryFd,
                        startRowNumber,
                        rowCount,
                        columnRollSink,
                        commitMode
                );
            }
        } else {
            try {
                long srcFixFd;
                long srcVarFd;

                if (ColumnType.isVarSize(columnType)) {
                    srcFixFd = secondaryColumn.getFd();
                    srcVarFd = primaryColumn.getFd();
                } else {
                    srcFixFd = primaryColumn.getFd();
                    srcVarFd = -1;
                }

                long dstFixFd;
                long dstVarFd;

                if (ColumnType.isVarSize(newColumnType)) {
                    dstFixFd = secondaryFd;
                    dstVarFd = primaryFd;
                } else {
                    dstFixFd = primaryFd;
                    dstVarFd = -1;
                }

                success = ColumnTypeConverter.convertColumn(
                        startRowNumber,
                        rowCount,
                        columnType,
                        srcFixFd,
                        srcVarFd,
                        symbolTable,
                        newColumnType,
                        dstFixFd,
                        dstVarFd,
                        symbolMapWriter,
                        ff,
                        primaryColumn.getExtendSegmentSize(),
                        columnRollSink
                );
                if (commitMode != CommitMode.NOSYNC) {
                    ff.fsync(srcFixFd);
                    ff.fsync(srcVarFd);
                    ff.fsync(dstFixFd);
                    ff.fsync(dstVarFd);
                }

            } catch (Throwable th) {
                LOG.critical().$("Failed to convert column [name=").$(newSegPath).$(", error=").$(th).I$();
                success = false;
            }
        }

        if (!success) {
            throw CairoException.critical(ff.errno()).put("failed to copy column file to new segment" +
                            " [path=").put(newSegPath)
                    .put(", column=").put(columnName)
                    .put(", errno=").put(ff.errno())
                    .put(", startRowNumber=").put(startRowNumber)
                    .put(", rowCount=").put(rowCount)
                    .put(", columnType=").put(columnType)
                    .put(", newColumnType=").put(newColumnType).put("]");
        }
    }

    private static boolean copyFixLenFile(
            FilesFacade ff,
            MemoryMA primaryColumn,
            long primaryFd,
            long rowOffset,
            long rowCount,
            int columnType,
            SegmentColumnRollSink columnRollSink,
            int commitMode
    ) {
        int shl = ColumnType.pow2SizeOf(columnType);
        assert shl > -1;
        long offset = rowOffset << shl;
        long length = rowCount << shl;

        boolean success = ff.copyData(primaryColumn.getFd(), primaryFd, offset, length) == length;
        if (success) {
            columnRollSink.setSrcOffsets(offset, -1);
            columnRollSink.setDestSizes(length, -1);
            if (commitMode != CommitMode.NOSYNC) {
                ff.fsync(primaryFd);
            }
        }
        return success;
    }

    private static boolean copyTimestampFile(
            FilesFacade ff,
            MemoryMA primaryColumn,
            long primaryFd,
            long rowOffset,
            long rowCount,
            SegmentColumnRollSink columnRollSink,
            int commitMode
    ) {
        // Designated timestamp column is written as 2 long values
        if (!copyFixLenFile(ff, primaryColumn, primaryFd, rowOffset, rowCount, ColumnType.LONG128, columnRollSink, commitMode)) {
            return false;
        }
        long size = rowCount << 4;
        long srcDataTimestampAddr = TableUtils.mapRW(ff, primaryFd, size, MEMORY_TAG);
        try {
            Vect.flattenIndex(srcDataTimestampAddr, rowCount);
            if (commitMode != CommitMode.NOSYNC) {
                ff.msync(srcDataTimestampAddr, size, commitMode == CommitMode.ASYNC);
            }
        } finally {
            ff.munmap(srcDataTimestampAddr, size, MEMORY_TAG);
        }
        return true;
    }

    private static boolean copyVarSizeFiles(
            FilesFacade ff,
            int columnType,
            MemoryMA dataMem,
            MemoryMA auxMem,
            long primaryFd,
            long secondaryFd,
            long startRowNumber,
            long rowCount,
            SegmentColumnRollSink columnRollSink,
            int commitMode
    ) {
        ColumnTypeDriver columnTypeDriver = ColumnType.getDriver(columnType);
        final long auxMemSize = columnTypeDriver.getAuxVectorSize(startRowNumber + rowCount);
        final long auxMemAddr = TableUtils.mapRW(ff, auxMem.getFd(), auxMemSize, MEMORY_TAG);
        try {
            final long dataStartOffset = columnTypeDriver.getDataVectorOffset(auxMemAddr, startRowNumber);
            assert dataStartOffset >= 0;
            final long dataSize = columnTypeDriver.getDataVectorSize(auxMemAddr, startRowNumber, startRowNumber + rowCount - 1);

            boolean success = dataSize == 0 || ff.copyData(dataMem.getFd(), primaryFd, dataStartOffset, dataSize) == dataSize;
            if (!success) {
                return false;
            }

            if (commitMode != CommitMode.NOSYNC) {
                ff.fsync(primaryFd);
            }

            final long newAuxMemSize = columnTypeDriver.getAuxVectorSize(rowCount);
            final long newAuxMemAddr = TableUtils.mapRW(ff, secondaryFd, newAuxMemSize, MEMORY_TAG);
            try {
                ff.madvise(newAuxMemAddr, newAuxMemSize, Files.POSIX_MADV_RANDOM);

                columnTypeDriver.shiftCopyAuxVector(
                        dataStartOffset,
                        auxMemAddr,
                        startRowNumber,
                        startRowNumber + rowCount - 1, // inclusive
                        newAuxMemAddr,
                        newAuxMemSize
                );

                columnRollSink.setSrcOffsets(dataStartOffset, columnTypeDriver.getAuxVectorSize(startRowNumber));
                columnRollSink.setDestSizes(dataSize, newAuxMemSize);

                if (commitMode != CommitMode.NOSYNC) {
                    ff.msync(newAuxMemAddr, newAuxMemSize, commitMode == CommitMode.ASYNC);
                }
            } finally {
                ff.munmap(newAuxMemAddr, newAuxMemSize, MEMORY_TAG);
            }
            return true;
        } finally {
            ff.munmap(auxMemAddr, auxMemSize, MEMORY_TAG);
        }
    }
}
