/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import static io.questdb.cairo.wal.WalColFirstWriter.NEW_COL_RECORD_SIZE;

public class CopyWalSegmentUtils {
    private static final Log LOG = LogFactory.getLog(CopyWalSegmentUtils.class);
    private static final int MEMORY_TAG = MemoryTag.MMAP_TABLE_WAL_WRITER;

    // column-first
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
        int primaryFd = -1;
        int secondaryFd = -1;
        boolean success;

        try {
            dFile(newSegPath, columnName, COLUMN_NAME_TXN_NONE);
            primaryFd = openRW(ff, newSegPath, LOG, options);
            newColumnFiles.setQuick(columnIndex * NEW_COL_RECORD_SIZE, primaryFd);

            if (ColumnType.isVarSize(columnType)) {
                iFile(newSegPath.trimTo(setPathRoot), columnName, COLUMN_NAME_TXN_NONE);
                secondaryFd = openRW(ff, newSegPath, LOG, options);
                newColumnFiles.setQuick(columnIndex * NEW_COL_RECORD_SIZE + 3, secondaryFd);
            }

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
        } catch (Throwable th) {
            success = false;
            LOG.critical()
                    .$("col-first WAL copy failed [path=").$(walPath)
                    .$(", error=").$(th.getMessage())
                    .I$();
        }

        if (!success) {
            throw CairoException.critical(ff.errno()).put("failed to copy column WAL data to new segment [path=").put(newSegPath)
                    .put(", column=").put(columnName)
                    .put(", startRowNumber=").put(startRowNumber)
                    .put(", rowCount=").put(rowCount)
                    .put(", columnType=").put(columnType)
                    .put(']');
        }
    }

    public static int rollRowFirstToSegment(
            FilesFacade ff,
            long options,
            WalWriterMetadata metadata,
            MemoryMA rowMem,
            @Transient Path walPath,
            int newSegment,
            long startOffset,
            long size,
            int commitMode
    ) {
        Path newSegPath = Path.PATH.get().of(walPath).slash().put(newSegment).concat(WAL_SEGMENT_FILE_NAME).$();

        final long alignedOffset = Files.floorPageSize(startOffset);
        final long alignedExtraSize = startOffset - alignedOffset;
        int fd = -1;
        long srcRowDataAddr = 0;
        long destRowDataAddr = 0;
        try {
            fd = openRW(ff, newSegPath, LOG, options);

            srcRowDataAddr = TableUtils.mapRO(ff, rowMem.getFd(), size + alignedExtraSize, alignedOffset, MEMORY_TAG);
            destRowDataAddr = TableUtils.mapRW(ff, fd, size, MEMORY_TAG);
            ff.madvise(destRowDataAddr, size, Files.POSIX_MADV_RANDOM);

            final int timestampIndex = metadata.getTimestampIndex();

            final long srcAddr = srcRowDataAddr + alignedExtraSize;
            long destRowId = 0;
            for (long offset = 0; offset < size; ) {
                final int columnIndex = Unsafe.getUnsafe().getInt(srcAddr + offset);
                Unsafe.getUnsafe().putInt(destRowDataAddr + offset, columnIndex);
                offset += Integer.BYTES;
                if (columnIndex == WalRowFirstWriter.NEW_ROW_SEPARATOR) {
                    destRowId++;
                    continue;
                }
                if (columnIndex == timestampIndex) {
                    // Designated timestamp column is written as 2 long values
                    Unsafe.getUnsafe().putLong(destRowDataAddr + offset, Unsafe.getUnsafe().getLong(srcAddr + offset));
                    Unsafe.getUnsafe().putLong(destRowDataAddr + offset + 8, destRowId);
                    offset += 2 * Long.BYTES;
                } else {
                    final int columnType = metadata.getColumnType(columnIndex);
                    final int columnSize = ColumnType.sizeOf(columnType);
                    if (columnSize > 0) {
                        // fixed-size column
                        Vect.memcpy(destRowDataAddr + offset, srcAddr + offset, columnSize);
                        offset += columnSize;
                    } else {
                        // var-size column
                        switch (ColumnType.tagOf(columnType)) {
                            case ColumnType.VARCHAR:
                                final int varcharSize = VarcharTypeDriver.getPlainValueSize(srcAddr + offset);
                                Unsafe.getUnsafe().putInt(destRowDataAddr + offset, varcharSize);
                                if (varcharSize == NULL_LEN) {
                                    Unsafe.getUnsafe().putInt(destRowDataAddr + offset, NULL_LEN);
                                    offset += Integer.BYTES;
                                } else {
                                    final long bytes = Integer.BYTES + varcharSize;
                                    Vect.memcpy(destRowDataAddr + offset, srcAddr + offset, bytes);
                                    offset += bytes;
                                }
                                break;
                            case ColumnType.STRING:
                                final int stringLen = Unsafe.getUnsafe().getInt(srcAddr + offset);
                                if (stringLen == NULL_LEN) {
                                    Unsafe.getUnsafe().putInt(destRowDataAddr + offset, NULL_LEN);
                                    offset += Integer.BYTES;
                                } else {
                                    final long bytes = Integer.BYTES + 2L * stringLen;
                                    Vect.memcpy(destRowDataAddr + offset, srcAddr + offset, bytes);
                                    offset += bytes;
                                }
                                break;
                            case ColumnType.BINARY:
                                final int binSize = Unsafe.getUnsafe().getInt(srcAddr + offset);
                                if (binSize == NULL_LEN) {
                                    Unsafe.getUnsafe().putLong(destRowDataAddr + offset, NULL_LEN);
                                    offset += Long.BYTES;
                                } else {
                                    final long bytes = Long.BYTES + binSize;
                                    Vect.memcpy(destRowDataAddr + offset, srcAddr + offset, bytes);
                                    offset += bytes;
                                }
                                break;
                            default:
                                throw CairoException.nonCritical()
                                        .put("Column type ").put(ColumnType.nameOf(columnType))
                                        .put(" not supported by WAL writer");
                        }
                    }
                }
            }

            if (commitMode != CommitMode.NOSYNC) {
                ff.msync(destRowDataAddr, size, commitMode == CommitMode.ASYNC);
            }
            return fd;
        } catch (Throwable th) {
            ff.close(fd);
            LOG.critical()
                    .$("row-first WAL copy failed [path=").$(walPath)
                    .$(", error=").$(th.getMessage())
                    .I$();
            throw CairoException.critical(ff.errno()).put("failed to copy row-first WAL data to new segment [path=").put(newSegPath)
                    .put(", path=").put(walPath)
                    .put(", startOffset=").put(startOffset)
                    .put(", size=").put(size)
                    .put(']');
        } finally {
            ff.munmap(srcRowDataAddr, size + alignedExtraSize, MEMORY_TAG);
            ff.munmap(destRowDataAddr, size, MEMORY_TAG);
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
        long size = rowCount << shl;

        boolean success = ff.copyData(primaryColumn.getFd(), primaryFd, offset, size) == size;
        if (success) {
            newOffsets.setQuick(columnIndex * NEW_COL_RECORD_SIZE + 1, offset);
            newOffsets.setQuick(columnIndex * NEW_COL_RECORD_SIZE + 2, size);
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
        final long size = rowCount << 4;
        final long destTimestampDataAddr = TableUtils.mapRW(ff, primaryFd, size, MEMORY_TAG);
        ff.madvise(destTimestampDataAddr, size, Files.POSIX_MADV_RANDOM);
        try {
            Vect.flattenIndex(destTimestampDataAddr, rowCount);
            if (commitMode != CommitMode.NOSYNC) {
                ff.msync(destTimestampDataAddr, size, commitMode == CommitMode.ASYNC);
            }
            return true;
        } finally {
            ff.munmap(destTimestampDataAddr, size, MEMORY_TAG);
        }
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
        final long auxMemAddr = TableUtils.mapRO(ff, auxMem.getFd(), auxMemSize, MEMORY_TAG);
        long newAuxMemAddr = 0;
        long newAuxMemSize = 0;
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

            newAuxMemSize = columnTypeDriver.getAuxVectorSize(rowCount);
            newAuxMemAddr = TableUtils.mapRW(ff, secondaryFd, newAuxMemSize, MEMORY_TAG);
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
            return true;
        } finally {
            ff.munmap(auxMemAddr, auxMemSize, MEMORY_TAG);
            ff.munmap(newAuxMemAddr, newAuxMemSize, MEMORY_TAG);
        }
    }
}
