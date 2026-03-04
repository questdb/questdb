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

package io.questdb.cairo;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

/**
 * Generates null bitmap (.n) files from sentinel values in .d data files.
 * Used for lazy migration of pre-bitmap partitions so that all column types
 * have a correct .n file for bitmap-based null detection.
 */
public class NullBitmapMigrator {
    private static final Log LOG = LogFactory.getLog(NullBitmapMigrator.class);

    /**
     * Ensures a .n bitmap file exists for the given column partition.
     * If .n already exists with correct size, this is a no-op.
     * If .n is missing or truncated, generates it from sentinel values in .d.
     *
     * @param ff         file system facade
     * @param nFilePath  path to the .n file
     * @param columnType the column type (determines which sentinel to scan for)
     * @param dataAddr   memory-mapped address of the .d file data (after column top)
     * @param rowCount   total number of rows in the partition
     * @param columnTop  number of rows at top with no data (added column)
     */
    public static void ensureNullBitmap(
            FilesFacade ff,
            LPSZ nFilePath,
            int columnType,
            long dataAddr,
            long rowCount,
            long columnTop
    ) {
        long expectedBitmapSize = (rowCount + 7) >> 3;
        if (ff.exists(nFilePath) && ff.length(nFilePath) >= expectedBitmapSize) {
            return;
        }

        LOG.info().$("generating null bitmap [path=").$(nFilePath)
                .$(", columnType=").$(ColumnType.nameOf(columnType))
                .$(", rowCount=").$(rowCount)
                .$(", columnTop=").$(columnTop)
                .I$();

        long bitmapAddr = Unsafe.malloc(expectedBitmapSize, MemoryTag.NATIVE_DEFAULT);
        try {
            Vect.memset(bitmapAddr, expectedBitmapSize, 0);

            for (long row = 0; row < columnTop; row++) {
                setBit(bitmapAddr, row);
            }

            long dataRowCount = rowCount - columnTop;
            if (dataRowCount > 0 && dataAddr != 0) {
                scanForSentinels(columnType, dataAddr, bitmapAddr, columnTop, dataRowCount);
            }

            // Write to a temp file first, then rename atomically to avoid
            // corruption when concurrent TableReaders migrate the same partition.
            LPSZ tmpPath = Path.getThreadLocal2("").of(nFilePath).put(".tmp").$();
            writeNFile(ff, tmpPath, bitmapAddr, expectedBitmapSize);
            if (ff.rename(tmpPath, nFilePath) != Files.FILES_RENAME_OK) {
                // Rename failed — another reader probably won the race. Clean up temp file
                // and verify the final .n file is valid.
                ff.remove(tmpPath);
                if (!ff.exists(nFilePath) || ff.length(nFilePath) < expectedBitmapSize) {
                    throw CairoException.critical(ff.errno())
                            .put("failed to create null bitmap [path=").put(nFilePath).put(']');
                }
            }
        } finally {
            Unsafe.free(bitmapAddr, expectedBitmapSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void scanForSentinels(int columnType, long dataAddr, long bitmapAddr, long columnTop, long dataRowCount) {
        int tag = ColumnType.tagOf(columnType);
        switch (tag) {
            case ColumnType.BOOLEAN:
            case ColumnType.BYTE:
                // These types historically had no null support (no sentinel).
                // All values are valid — bitmap stays all-zeros.
                break;
            case ColumnType.SHORT:
                if (ColumnType.isUInt16(columnType)) {
                    // UINT16 is a new type — never had sentinel-based data.
                    break;
                }
                // Signed SHORT historically had no null support either.
                break;
            case ColumnType.CHAR:
                scanChar(dataAddr, bitmapAddr, columnTop, dataRowCount);
                break;
            case ColumnType.INT:
                if (ColumnType.isUInt32(columnType)) {
                    break;
                }
                scanInt(dataAddr, bitmapAddr, columnTop, dataRowCount);
                break;
            case ColumnType.IPv4:
                scanIPv4(dataAddr, bitmapAddr, columnTop, dataRowCount);
                break;
            case ColumnType.LONG:
                if (ColumnType.isUInt64(columnType)) {
                    break;
                }
                scanLong(dataAddr, bitmapAddr, columnTop, dataRowCount);
                break;
            case ColumnType.DATE:
            case ColumnType.TIMESTAMP:
                scanLong(dataAddr, bitmapAddr, columnTop, dataRowCount);
                break;
            case ColumnType.FLOAT:
                scanFloat(dataAddr, bitmapAddr, columnTop, dataRowCount);
                break;
            case ColumnType.DOUBLE:
                scanDouble(dataAddr, bitmapAddr, columnTop, dataRowCount);
                break;
            case ColumnType.SYMBOL:
                scanSymbol(dataAddr, bitmapAddr, columnTop, dataRowCount);
                break;
            default:
                // UUID, LONG256, variable-size types, etc. — not applicable
                break;
        }
    }

    private static void scanChar(long dataAddr, long bitmapAddr, long columnTop, long dataRowCount) {
        for (long i = 0; i < dataRowCount; i++) {
            char value = Unsafe.getUnsafe().getChar(dataAddr + i * Character.BYTES);
            if (value == 0) {
                setBit(bitmapAddr, columnTop + i);
            }
        }
    }

    private static void scanDouble(long dataAddr, long bitmapAddr, long columnTop, long dataRowCount) {
        for (long i = 0; i < dataRowCount; i++) {
            double value = Unsafe.getUnsafe().getDouble(dataAddr + i * Double.BYTES);
            if (Double.isNaN(value)) {
                setBit(bitmapAddr, columnTop + i);
            }
        }
    }

    private static void scanFloat(long dataAddr, long bitmapAddr, long columnTop, long dataRowCount) {
        for (long i = 0; i < dataRowCount; i++) {
            float value = Unsafe.getUnsafe().getFloat(dataAddr + i * Float.BYTES);
            if (Float.isNaN(value)) {
                setBit(bitmapAddr, columnTop + i);
            }
        }
    }

    private static void scanIPv4(long dataAddr, long bitmapAddr, long columnTop, long dataRowCount) {
        for (long i = 0; i < dataRowCount; i++) {
            int value = Unsafe.getUnsafe().getInt(dataAddr + i * Integer.BYTES);
            if (value == Numbers.IPv4_NULL) {
                setBit(bitmapAddr, columnTop + i);
            }
        }
    }

    private static void scanInt(long dataAddr, long bitmapAddr, long columnTop, long dataRowCount) {
        for (long i = 0; i < dataRowCount; i++) {
            int value = Unsafe.getUnsafe().getInt(dataAddr + i * Integer.BYTES);
            if (value == Numbers.INT_NULL) {
                setBit(bitmapAddr, columnTop + i);
            }
        }
    }

    private static void scanLong(long dataAddr, long bitmapAddr, long columnTop, long dataRowCount) {
        for (long i = 0; i < dataRowCount; i++) {
            long value = Unsafe.getUnsafe().getLong(dataAddr + i * Long.BYTES);
            if (value == Numbers.LONG_NULL) {
                setBit(bitmapAddr, columnTop + i);
            }
        }
    }

    private static void scanSymbol(long dataAddr, long bitmapAddr, long columnTop, long dataRowCount) {
        for (long i = 0; i < dataRowCount; i++) {
            int value = Unsafe.getUnsafe().getInt(dataAddr + i * Integer.BYTES);
            if (value == Numbers.INT_NULL) {
                setBit(bitmapAddr, columnTop + i);
            }
        }
    }

    private static void setBit(long bitmapAddr, long row) {
        long byteOffset = row >> 3;
        int bitIndex = (int) (row & 7);
        long addr = bitmapAddr + byteOffset;
        byte current = Unsafe.getUnsafe().getByte(addr);
        Unsafe.getUnsafe().putByte(addr, (byte) (current | (1 << bitIndex)));
    }

    private static void writeNFile(FilesFacade ff, LPSZ nFilePath, long bitmapAddr, long bitmapSize) {
        long fd = ff.openRW(nFilePath, CairoConfiguration.O_NONE);
        if (fd < 0) {
            throw CairoException.critical(ff.errno()).put("could not create null bitmap file [path=").put(nFilePath).put(']');
        }
        try {
            if (!ff.truncate(fd, bitmapSize)) {
                throw CairoException.critical(ff.errno()).put("could not truncate null bitmap file [path=").put(nFilePath)
                        .put(", size=").put(bitmapSize).put(']');
            }
            long written = ff.write(fd, bitmapAddr, bitmapSize, 0);
            if (written != bitmapSize) {
                throw CairoException.critical(ff.errno()).put("could not write null bitmap file [path=").put(nFilePath)
                        .put(", expected=").put(bitmapSize)
                        .put(", written=").put(written).put(']');
            }
        } finally {
            ff.close(fd);
        }
    }
}
