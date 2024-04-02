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

package io.questdb.cairo.wal;

import io.questdb.cairo.*;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.*;
import static io.questdb.cairo.wal.WalWriter.NEW_COL_RECORD_SIZE;

public class CopyWalSegmentUtils {
    private static final Log LOG = LogFactory.getLog(CopyWalSegmentUtils.class);
    private static final int MEMORY_TAG = MemoryTag.MMAP_TABLE_WAL_WRITER;

    public static void rollColumnToSegment(
            FilesFacade ff,
            long options,
            MemoryMA primaryColumn,
            MemoryMA secondaryColumn,
            @Transient Path walPath,
            int newSegment,
            CharSequence columnName,
            int columnType,
            long startRowNumber,
            long rowCount,
            LongList newColumnFiles,
            int columnIndex,
            int commitMode
    ) {
        Path newSegPath = Path.PATH.get().of(walPath).slash().put(newSegment);
        int setPathRoot = newSegPath.size();
        dFile(newSegPath, columnName, COLUMN_NAME_TXN_NONE);
        int primaryFd = openRW(ff, newSegPath, LOG, options);
        newColumnFiles.setQuick(columnIndex * NEW_COL_RECORD_SIZE, primaryFd);

        int secondaryFd;
        if (ColumnType.isVarSize(columnType)) {
            iFile(newSegPath.trimTo(setPathRoot), columnName, COLUMN_NAME_TXN_NONE);
            secondaryFd = openRW(ff, newSegPath, LOG, options);
            newColumnFiles.setQuick(columnIndex * NEW_COL_RECORD_SIZE + 3, secondaryFd);
        } else {
            secondaryFd = -1;
        }

        boolean success;
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
                    newColumnFiles,
                    columnIndex,
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
                    newColumnFiles,
                    columnIndex,
                    commitMode
            );
        } else {
            success = copyTimestampFile(
                    ff,
                    primaryColumn,
                    primaryFd,
                    startRowNumber,
                    rowCount,
                    newColumnFiles,
                    columnIndex,
                    commitMode
            );
        }

        if (!success) {
            throw CairoException.critical(ff.errno()).put("failed to copy column file to new segment" +
                            " [path=").put(newSegPath)
                    .put(", column=").put(columnName)
                    .put(", startRowNumber=").put(startRowNumber)
                    .put(", rowCount=").put(rowCount)
                    .put(", columnType=").put(columnType).put("]");
        }
    }

    private static boolean copyFixLenFile(
            FilesFacade ff,
            MemoryMA primaryColumn,
            int primaryFd,
            long rowOffset,
            long rowCount,
            int columnType,
            LongList newOffsets,
            int columnIndex,
            int commitMode
    ) {
        int shl = ColumnType.pow2SizeOf(columnType);
        long offset = rowOffset << shl;
        long length = rowCount << shl;

        boolean success = ff.copyData(primaryColumn.getFd(), primaryFd, offset, length) == length;
        if (success) {
            newOffsets.setQuick(columnIndex * NEW_COL_RECORD_SIZE + 1, offset);
            newOffsets.setQuick(columnIndex * NEW_COL_RECORD_SIZE + 2, length);
        }
        if (commitMode != CommitMode.NOSYNC) {
            ff.fsync(primaryFd);
        }
        return success;
    }

    private static boolean copyTimestampFile(
            FilesFacade ff,
            MemoryMA primaryColumn,
            int primaryFd,
            long rowOffset,
            long rowCount,
            LongList newOffsets,
            int columnIndex,
            int commitMode
    ) {
        // Designated timestamp column is written as 2 long values
        if (!copyFixLenFile(ff, primaryColumn, primaryFd, rowOffset, rowCount, ColumnType.LONG128, newOffsets, columnIndex, commitMode)) {
            return false;
        }
        long size = rowCount << 4;
        long srcDataTimestampAddr = TableUtils.mapRW(ff, primaryFd, size, MEMORY_TAG);
        Vect.flattenIndex(srcDataTimestampAddr, rowCount);
        if (commitMode != CommitMode.NOSYNC) {
            ff.msync(srcDataTimestampAddr, size, commitMode == CommitMode.ASYNC);
        }
        ff.munmap(srcDataTimestampAddr, size, MEMORY_TAG);
        return true;
    }

    private static boolean copyVarSizeFiles(
            FilesFacade ff,
            int columnType,
            MemoryMA dataMem,
            MemoryMA auxMem,
            int primaryFd,
            int secondaryFd,
            long startRowNumber,
            long rowCount,
            LongList newOffsets,
            int columnIndex,
            int commitMode
    ) {
        ColumnTypeDriver columnTypeDriver = ColumnType.getDriver(columnType);
        final long auxMemSize = columnTypeDriver.getAuxVectorSize(startRowNumber + rowCount);
        final long auxMemAddr = TableUtils.mapRW(ff, auxMem.getFd(), auxMemSize, MEMORY_TAG);
        try {
            final long dataStartOffset = columnTypeDriver.getDataVectorOffset(auxMemAddr, startRowNumber);
            final long dataSize = columnTypeDriver.getDataVectorSize(auxMemAddr, startRowNumber, startRowNumber + rowCount - 1);

            boolean success = dataSize == 0 || ff.copyData(dataMem.getFd(), primaryFd, dataStartOffset, dataSize) == dataSize;
            if (!success) {
                return false;
            }

            if (commitMode != CommitMode.NOSYNC) {
                ff.fsync(primaryFd);
            }

            newOffsets.setQuick(columnIndex * NEW_COL_RECORD_SIZE + 1, dataStartOffset);
            newOffsets.setQuick(columnIndex * NEW_COL_RECORD_SIZE + 2, dataSize);

            final long newAuxMemSize = columnTypeDriver.getAuxVectorSize(rowCount);
            final long newAuxMemAddr = TableUtils.mapRW(ff, secondaryFd, newAuxMemSize, MEMORY_TAG);
            ff.madvise(newAuxMemAddr, newAuxMemSize, Files.POSIX_MADV_RANDOM);

            columnTypeDriver.shiftCopyAuxVector(
                    dataStartOffset,
                    auxMemAddr,
                    startRowNumber,
                    startRowNumber + rowCount - 1, // inclusive
                    newAuxMemAddr,
                    newAuxMemSize
            );

            newOffsets.setQuick(columnIndex * NEW_COL_RECORD_SIZE + 4, columnTypeDriver.getAuxVectorOffset(startRowNumber + 1));
            newOffsets.setQuick(columnIndex * NEW_COL_RECORD_SIZE + 5, newAuxMemSize);

            if (commitMode != CommitMode.NOSYNC) {
                ff.msync(newAuxMemAddr, newAuxMemSize, commitMode == CommitMode.ASYNC);
            }
            // All in memory calls, no need to unmap in finally
            ff.munmap(newAuxMemAddr, newAuxMemSize, MEMORY_TAG);
            return true;
        } finally {
            ff.munmap(auxMemAddr, auxMemSize, MEMORY_TAG);
        }
    }
}
