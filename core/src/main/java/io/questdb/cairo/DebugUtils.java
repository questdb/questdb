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

package io.questdb.cairo;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;

@SuppressWarnings("unused")
public class DebugUtils {
    public static final Log LOG = LogFactory.getLog(DebugUtils.class);

    // For debugging purposes
    public static boolean checkAscendingTimestamp(FilesFacade ff, long size, long fd) {
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

    public static boolean isSparseVarCol(long colRowCount, long iAddr, long dAddr, int colType) {
        if (colType == ColumnType.STRING || colType == ColumnType.BINARY) {
            for (int row = 0; row < colRowCount; row++) {
                long offset = Unsafe.getUnsafe().getLong(iAddr + (long) row * Long.BYTES);
                long iLen = Unsafe.getUnsafe().getLong(iAddr + (long) (row + 1) * Long.BYTES) - offset;
                long dLen = ColumnType.isString(colType) ? Unsafe.getUnsafe().getInt(dAddr + offset) : Unsafe.getUnsafe().getLong(dAddr + offset);
                int lenLen = ColumnType.isString(colType) ? 4 : 8;
                long dataLen = ColumnType.isString(colType) ? dLen * 2 : dLen;
                long dStorageLen = dLen > 0 ? dataLen + lenLen : lenLen;
                if (iLen != dStorageLen) {
                    // Swiss cheese hole in var col file
                    return true;
                }
            }
            return false;
        } else if (colType == ColumnType.VARCHAR) {
            ColumnTypeDriver driver = ColumnType.getDriver(colType);
            long lastSizeInDataVector = 0;
            for (int row = 0; row < colRowCount; row++) {
                long offset = driver.getDataVectorOffset(iAddr, row);
                if (offset != lastSizeInDataVector) {
                    // Swiss cheese hole in var col file
                    return true;
                }
                lastSizeInDataVector = driver.getDataVectorSizeAt(iAddr, row);
            }
            return false;
        } else {
            throw new AssertionError("Not a var col");
        }
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
                        LOG.critical().$("failed to reconcile column top [partition=").$ts(partitionTimestamp)
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

    static void assertO3IndexSorted(long indexAddr, long indexSize) {
        long lastTs = Long.MIN_VALUE;
        for (long i = 0; i < indexSize; i++) {
            long ts = Unsafe.getUnsafe().getLong(indexAddr + 16 * i);
            long rowId = Unsafe.getUnsafe().getLong(indexAddr + 16 * i + 8);
            assert ts >= lastTs : String.format("ts %,d lastTs %,d rowId %,d", ts, lastTs, rowId);
            lastTs = ts;
        }
    }

    static void assertTimestampColumnSorted(long columnAddr, long columnSize) {
        long lastTs = Long.MIN_VALUE;
        for (long i = 0; i < columnSize; i++) {
            long ts = Unsafe.getUnsafe().getLong(columnAddr + 8 * i);
            assert ts >= lastTs : String.format("ts %,d lastTs %,d", ts, lastTs);
            lastTs = ts;
        }
    }

    static void logO3Index(long indexAddr, long indexSize, long tailLen) {
        long start = Math.max(0, indexSize - tailLen);
        for (long i = start; i < indexSize; i++) {
            long ts = Unsafe.getUnsafe().getLong(indexAddr + 16 * i);
            long rowId = Unsafe.getUnsafe().getLong(indexAddr + 16 * i + 8);
            LOG.info().$("index [").$(i).$("] = ").$ts(ts).$(", ts=").$(ts).$(", rowId=").$(rowId).$();
        }
    }

    static void logTimestampColumn(long colAddr, long colSize, long tailLen) {
        long start = Math.max(0, colSize - tailLen);
        for (long i = start; i < colSize; i++) {
            long ts = Unsafe.getUnsafe().getLong(colAddr + 8 * i);
            LOG.info().$("ts_col [").$(i).$("] = ").$ts(ts).$(", ts=").$(ts).$();
        }
    }
}
