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

package io.questdb.cairo;

import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;

import static io.questdb.cairo.TableWriter.getPrimaryColumnIndex;
import static io.questdb.cairo.TableWriter.getSecondaryColumnIndex;

@SuppressWarnings("unused")
public class DebugUtils {
    public static final Log LOG = LogFactory.getLog(DebugUtils.class);

    // For debugging purposes
    public static boolean checkAscendingTimestamp(FilesFacade ff, long size, int fd) {
        if (size > 0) {
            long buffer = TableUtils.mapAppendColumnBuffer(ff, fd, 0, size * Long.BYTES, false, MemoryTag.MMAP_DEFAULT);
            try {
                long ts = Long.MIN_VALUE;
                for (int i = 0; i < size; i++) {
                    long nextTs = Unsafe.getUnsafe().getLong(buffer + (long) i * Long.BYTES);
                    if (nextTs < ts) {
                        return false;
                    }
                    ts = nextTs;
                }
            } finally {
                TableUtils.mapAppendColumnBufferRelease(ff, buffer, 0, size * Long.BYTES, MemoryTag.MMAP_DEFAULT);
            }
        }
        return true;
    }

    public static boolean isSparseVarCol(long colRowCount, long iAddr, long dAddr) {
        for (int row = 0; row < colRowCount; row++) {
            long offset = Unsafe.getUnsafe().getLong(iAddr + (long) row * Long.BYTES);
            long iLen = Unsafe.getUnsafe().getLong(iAddr + (long) (row + 1) * Long.BYTES) - offset;
            int dLen = Unsafe.getUnsafe().getInt(dAddr + offset);
            int dStorageLen = dLen > 0 ? dLen * 2 + 4 : 4;
            if (iLen != dStorageLen) {
                // Swiss cheese hole in var col file
                return true;
            }
        }
        return false;
    }

    // Useful debugging method
    public static boolean reconcileColumnTops(int partitionsSlotSize, LongList openPartitionInfo, ColumnVersionReader columnVersionReader, TableReader reader) {
        int partitionCount = reader.getPartitionCount();
        for (int p = 0; p < partitionCount; p++) {
            long partitionRowCount = reader.getPartitionRowCount(p);
            if (partitionRowCount != -1) {
                long partitionTimestamp = openPartitionInfo.getQuick(p * partitionsSlotSize);
                for (int c = 0; c < reader.getColumnCount(); c++) {
                    long colTop = Math.min(reader.getColumnTop(reader.getColumnBase(p), c), partitionRowCount);
                    long columnTopRaw = columnVersionReader.getColumnTop(partitionTimestamp, c);
                    long columnTop = Math.min(columnTopRaw == -1 ? partitionRowCount : columnTopRaw, partitionRowCount);
                    if (columnTop != colTop) {
                        LOG.criticalW().$("failed to reconcile column top [partition=").$ts(partitionTimestamp)
                                .$(", column=").$(c)
                                .$(", expected=").$(columnTop)
                                .$(", actual=").$(colTop).$(']').
                                $();
                        return false;
                    }
                }
            }
        }
        return true;
    }

    // Useful debugging method
    public static boolean reconcileVarLenCol(
            FilesFacade ff,
            RecordMetadata metadata,
            long partitionTimestamp,
            ColumnVersionReader columnVersionReader,
            ObjList<MemoryMA> columns,
            long rowCount
    ) {
        if (rowCount == 0) {
            return true;
        }

        for (int col = 0; col < metadata.getColumnCount(); col++) {
            if (metadata.getColumnType(col) == ColumnType.STRING) {
                // map column files
                long colTop = columnVersionReader.getColumnTop(partitionTimestamp, col);
                long colRowCount = rowCount - colTop;
                if (colRowCount == 0) {
                    continue;
                }

                MemoryMA dCol = columns.get(getPrimaryColumnIndex(col));
                MemoryMA iCol = columns.get(getSecondaryColumnIndex(col));

                long iAddrMap = TableUtils.mapAppendColumnBuffer(ff, iCol.getFd(), 0, (colRowCount + 1) * Long.BYTES, false, MemoryTag.MMAP_DEFAULT);
                long iAddr = Math.abs(iAddrMap);

                long dSize = Unsafe.getUnsafe().getLong(iAddr + colRowCount * Long.BYTES);
                long dAddrMap = TableUtils.mapAppendColumnBuffer(ff, dCol.getFd(), 0, dSize, false, MemoryTag.MMAP_DEFAULT);
                long dAddr = Math.abs(dAddrMap);

                try {
                    if (isSparseVarCol(colRowCount, iAddr, dAddr)) {
                        return false;
                    }
                } finally {
                    TableUtils.mapAppendColumnBufferRelease(ff, dAddrMap, 0, dSize, MemoryTag.MMAP_DEFAULT);
                    TableUtils.mapAppendColumnBufferRelease(ff, iAddrMap, 0, (colRowCount + 1) * Long.BYTES, MemoryTag.MMAP_DEFAULT);
                }
            }
        }
        return true;
    }
}
