/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.*;
import static io.questdb.cairo.wal.WalWriter.*;

public class CopySegmentFileJob {
    private static final Log LOG = LogFactory.getLog(WalWriter.class);
    private static final int MEMORY_TAG = MemoryTag.MMAP_TABLE_WAL_WRITER;

    public static void rollColumnToSegment(
            FilesFacade ff,
            long options,
            MemoryMA primaryColumn,
            MemoryMA secondaryColumn,
            @Transient Path walPath,
            long newSegment,
            CharSequence columnName,
            int columnType,
            long rowOffset,
            long rowCount,
            LongList newColumnFiles,
            int columnIndex
    ) {
        Path newSegPath = Path.PATH.get().of(walPath).slash().put(newSegment);
        int setPathRoot = newSegPath.length();
        dFile(newSegPath, columnName, COLUMN_NAME_TXN_NONE);
        long primaryFd = openRW(ff, newSegPath, LOG, options);
        long secondaryFd;
        if (ColumnType.isVariableLength(columnType)) {
            iFile(newSegPath.trimTo(setPathRoot), columnName, COLUMN_NAME_TXN_NONE);
            secondaryFd = openRW(ff, newSegPath, LOG, options);
        } else {
            secondaryFd = -1;
        }

        boolean success;
        if (ColumnType.isVariableLength(columnType)) {
            success = copyVarLenFile(ff, primaryColumn, secondaryColumn, primaryFd, secondaryFd,  rowOffset, rowCount, newColumnFiles, columnIndex);
        } else if (columnType > 0) {
            success = copyFixLenFile(ff, primaryColumn, primaryFd, rowOffset, rowCount, columnType, newColumnFiles, columnIndex);
        } else {
            success = copyTimestampFile(ff, primaryColumn, primaryFd, rowOffset, rowCount, newColumnFiles, columnIndex);
        }

        if (!success) {
            throw CairoException.critical(ff.errno()).put("failed to copy column file to new segment" +
                            " [path=").put(newSegPath)
                    .put(", column=").put(columnName)
                    .put(", rowOffset=").put(rowOffset)
                    .put(", rowCount=").put(rowCount)
                    .put(", columnType=").put(columnType).put("]");
        }
    }

    private static boolean copyVarLenFile(FilesFacade ff, MemoryMA primaryColumn, MemoryMA secondaryColumn, long primaryFd, long secondaryFd, long rowOffset, long rowCount, LongList newOffsets, int columnIndex) {
        long indexMapSize = (rowOffset + rowCount + 1) * Long.BYTES;
        long srcIndexAddr = TableUtils.mapRW(ff, secondaryColumn.getFd(), indexMapSize, MEMORY_TAG);
        try {
            long varStart = Unsafe.getUnsafe().getLong(srcIndexAddr + rowOffset * Long.BYTES);
            long varEnd = Unsafe.getUnsafe().getLong(srcIndexAddr + (rowOffset + rowCount) * Long.BYTES);
            long varCopyLen = varEnd - varStart;
            boolean success = ff.copyData(primaryColumn.getFd(), primaryFd, varStart, varCopyLen) == varCopyLen;
            if (!success) {
                return false;
            }
            newOffsets.setQuick(columnIndex * NEW_COL_RECORD_SIZE, primaryFd);
            newOffsets.setQuick(columnIndex * NEW_COL_RECORD_SIZE + 1, varStart);
            newOffsets.setQuick(columnIndex * NEW_COL_RECORD_SIZE + 2, varCopyLen);

            long indexLen = (rowCount + 1) * Long.BYTES;
            long dstIndexAddr = TableUtils.mapRW(ff, secondaryFd, indexLen, MEMORY_TAG);
            try {
                Vect.shiftCopyFixedSizeColumnData(varStart, srcIndexAddr, rowOffset, rowOffset + rowCount, dstIndexAddr);
                newOffsets.setQuick(columnIndex * NEW_COL_RECORD_SIZE + 3, secondaryFd);
                newOffsets.setQuick(columnIndex * NEW_COL_RECORD_SIZE + 4, (rowOffset + 1) * Long.BYTES);
                newOffsets.setQuick(columnIndex * NEW_COL_RECORD_SIZE + 5, indexLen);
                return true;
            } finally {
                ff.munmap(dstIndexAddr, indexLen, MEMORY_TAG);
            }
        } finally {
            ff.munmap(srcIndexAddr, indexMapSize, MEMORY_TAG);
        }
    }

    private static boolean copyTimestampFile(FilesFacade ff, MemoryMA primaryColumn, long primaryFd, long rowOffset, long rowCount, LongList newOffsets, int columnIndex) {
        // Designated timestamp column is written as 2 long values
        if (!copyFixLenFile(ff, primaryColumn, primaryFd, rowOffset, rowCount, ColumnType.LONG128, newOffsets, columnIndex)) {
            return false;
        }
        long size = rowCount << 4;
        long srcDataTimestampAddr = TableUtils.mapRW(ff, primaryFd, size, MEMORY_TAG);
        try {
            Vect.flattenIndex(srcDataTimestampAddr, rowCount);
        } finally {
            ff.munmap(srcDataTimestampAddr, size, MEMORY_TAG);
        }
        return true;
    }

    private static boolean copyFixLenFile(FilesFacade ff, MemoryMA primaryColumn, long primaryFd, long rowOffset, long rowCount, int columnType, LongList newOffsets, int columnIndex) {
        int shl = ColumnType.pow2SizeOf(columnType);
        long offset = rowOffset << shl;
        long length = rowCount << shl;

        boolean success = ff.copyData(primaryColumn.getFd(), primaryFd, offset, length) == length;
        if (success) {
            newOffsets.setQuick(columnIndex * NEW_COL_RECORD_SIZE, primaryFd);
            newOffsets.setQuick(columnIndex * NEW_COL_RECORD_SIZE + 1, offset);
            newOffsets.setQuick(columnIndex * NEW_COL_RECORD_SIZE + 2, length);
        } else {
            ff.close(primaryFd);
        }
        return success;
    }
}
