/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin.engine.table;

import io.questdb.std.Unsafe;

public final class LatestByArguments {
    private static final long KEY_LO_OFFSET = 0*8;
    private static final long KEY_HI_OFFSET = 1*8;
    private static final long ROWS_ADDRESS_OFFSET = 2*8;
    private static final long ROWS_CAPACITY_OFFSET = 3*8;
    private static final long ROWS_SIZE_OFFSET = 4*8;
    public static final long MEMORY_SIZE = 5*8;

    public static long allocateMemory() {
        return Unsafe.calloc(MEMORY_SIZE);
    }

    public static void releaseMemory(long address) {
        Unsafe.free(address, MEMORY_SIZE);
    }

    public static long allocateMemoryArray(int elements) {
        return Unsafe.calloc(MEMORY_SIZE * elements);
    }

    public static void releaseMemoryArray(long address, int elements) {
        Unsafe.free(address, MEMORY_SIZE * elements);
    }

    public static long getKeyLo(long address) {
        return Unsafe.getUnsafe().getLong(address + KEY_LO_OFFSET);
    }
    public static long getKeyHi(long address) {
        return Unsafe.getUnsafe().getLong(address + KEY_HI_OFFSET);
    }
    public static long getRowsAddress(long address) {
        return Unsafe.getUnsafe().getLong(address + ROWS_ADDRESS_OFFSET);
    }
    public static long getRowsCapacity(long address) {
        return Unsafe.getUnsafe().getLong(address + ROWS_CAPACITY_OFFSET);
    }
    public static long getRowsSize(long address) {
        return Unsafe.getUnsafe().getLong(address + ROWS_SIZE_OFFSET);
    }
    public static void setKeyLo(long address, long lo) {
        Unsafe.getUnsafe().putLong(address + KEY_LO_OFFSET, lo);
    }
    public static void setKeyHi(long address, long up) {
        Unsafe.getUnsafe().putLong(address + KEY_HI_OFFSET, up);
    }
    public static void setRowsAddress(long address, long addr) {
        Unsafe.getUnsafe().putLong(address + ROWS_ADDRESS_OFFSET, addr);
    }
    public static void setRowsCapacity(long address, long cap) {
        Unsafe.getUnsafe().putLong(address + ROWS_CAPACITY_OFFSET, cap);
    }
    public static void setRowsSize(long address, long size) {
        Unsafe.getUnsafe().putLong(address + ROWS_SIZE_OFFSET, size);
    }
}