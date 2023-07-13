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

import io.questdb.std.*;

import java.io.Closeable;

/**
 * This class is used to store addresses of columns to pass to C deduplication routines.
 * The data structure has to match dedup_column struct in dedup.cpp
 */
public class DedupColumnCommitAddresses implements Closeable {
    // The data structure in below offsets has to match dedup_column struct in dedup.cpp
    private static final long COL_TYPE_32 = 0L;
    private static final long VAL_SIZE_32 = COL_TYPE_32 + 4L;
    private static final long COL_TOP_64 = VAL_SIZE_32 + 4L;
    private static final long COL_DATA_64 = COL_TOP_64 + 8L;
    private static final long O3_DATA_64 = COL_DATA_64 + 8L;
    private static final long RESERVED1 = O3_DATA_64 + 8L;
    private static final long RESERVED2 = RESERVED1 + 8L;
    private static final long RESERVED3 = RESERVED2 + 8L;
    private static final long NULL_VAL_256 = RESERVED3 + 8L;
    private static final int RECORD_BYTES = (int) (NULL_VAL_256 + 32L);
    // The data structure in above offsets has to match dedup_column struct in dedup.cpp
    
    private PagedDirectLongList addresses;
    private int columnCount;

    public long allocateBlock() {
        if (columnCount == 0) {
            return -1;
        }
        return addresses.allocateBlock();
    }

    public void clear(long dedupColSinkAddr) {
        Vect.memset(dedupColSinkAddr, (long) columnCount * RECORD_BYTES, 0);
    }

    public void clear() {
        if (addresses != null) {
            addresses.clear();
        }
    }

    @Override
    public void close() {
        addresses = Misc.free(addresses);
    }

    public long getAddress(long dedupCommitAddr) {
        return dedupCommitAddr;
    }

    public long getColReserved1(long dedupBlockAddress, int keyIndex) {
        return Unsafe.getUnsafe().getLong(dedupBlockAddress + (long) keyIndex * RECORD_BYTES + RESERVED1);
    }

    public long getColReserved2(long dedupBlockAddress, int keyIndex) {
        return Unsafe.getUnsafe().getLong(dedupBlockAddress + (long) keyIndex * RECORD_BYTES + RESERVED2);
    }

    public long getColReserved3(long dedupBlockAddress, int keyIndex) {
        return Unsafe.getUnsafe().getLong(dedupBlockAddress + (long) keyIndex * RECORD_BYTES + RESERVED3);
    }

    public int getColumnCount() {
        return columnCount;
    }

    public void setArrayValues(
            long dedupCommitAddr,
            int dedupKeyIndex,
            int columnType,
            int valueSizeBytes,
            long columnTop,
            long columnDataAddress,
            long o3DataAddress,
            long reserved1,
            long reserved2,
            long reserved3
    ) {
        long addr = dedupCommitAddr + (long) dedupKeyIndex * RECORD_BYTES;
        Unsafe.getUnsafe().putInt(addr + COL_TYPE_32, columnType);
        Unsafe.getUnsafe().putInt(addr + VAL_SIZE_32, valueSizeBytes);
        Unsafe.getUnsafe().putLong(addr + COL_TOP_64, columnTop);
        Unsafe.getUnsafe().putLong(addr + COL_DATA_64, columnDataAddress);
        Unsafe.getUnsafe().putLong(addr + O3_DATA_64, o3DataAddress);
        Unsafe.getUnsafe().putLong(addr + RESERVED1, reserved1);
        Unsafe.getUnsafe().putLong(addr + RESERVED2, reserved2);
        Unsafe.getUnsafe().putLong(addr + RESERVED3, reserved3);
        Unsafe.getUnsafe().putLong(addr + NULL_VAL_256, TableUtils.getNullLong(columnType, 0));
        Unsafe.getUnsafe().putLong(addr + NULL_VAL_256 + 8, TableUtils.getNullLong(columnType, 1));
        Unsafe.getUnsafe().putLong(addr + NULL_VAL_256 + 16, TableUtils.getNullLong(columnType, 2));
        Unsafe.getUnsafe().putLong(addr + NULL_VAL_256 + 24, TableUtils.getNullLong(columnType, 3));
    }

    public void setDedupColumnCount(int dedupColumnCount) {
        if (dedupColumnCount > 0) {
            if (addresses == null) {
                addresses = new PagedDirectLongList(MemoryTag.NATIVE_O3);
            } else {
                addresses.clear();
            }
            int longsPerBlock = RECORD_BYTES / Long.BYTES;
            addresses.setBlockSize(dedupColumnCount * longsPerBlock);
            this.columnCount = dedupColumnCount;
        }
    }

    static {
        assert RECORD_BYTES % Long.BYTES == 0;
    }
}
