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

package io.questdb.std;

import io.questdb.cairo.ColumnTypes;

public final class Rosti {

    public static final int FAKE_ALLOC_SIZE = 1024;

    public static native long alloc(long pKeyTypes, int keyTypeCount, long capacity);

    public static long alloc(ColumnTypes types, long capacity) {
        final int columnCount = types.getColumnCount();
        final long mem = Unsafe.malloc(Integer.BYTES * columnCount);
        try {
            long p = mem;
            for (int i = 0; i < columnCount; i++) {
                Unsafe.getUnsafe().putInt(p, types.getColumnType(i));
                p += Integer.BYTES;
            }
            // this is not an exact size of memory allocated for Rosti, but this is useful to
            // track that we free these maps
            Unsafe.recordMemAlloc(FAKE_ALLOC_SIZE);
            return alloc(mem, columnCount, Numbers.ceilPow2(capacity) - 1);
        } finally {
            Unsafe.free(mem, Integer.BYTES * columnCount);
        }
    }

    public static void free(long pRosti) {
        free0(pRosti);
        Unsafe.recordMemAlloc(-FAKE_ALLOC_SIZE);
    }

    private static native void free0(long pRosti);

    public static native void clear(long pRosti);

    public static native void keyedIntSumDouble(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native void keyedIntSumDoubleMerge(long pRostiA, long pRostiB, int valueOffset);

    public static long getCtrl(long pRosti) {
        return Unsafe.getUnsafe().getLong(pRosti);
    }

    public static long getSlots(long pRosti) {
        return Unsafe.getUnsafe().getLong(pRosti + Long.BYTES);
    }

    public static long getSize(long pRosti) {
        return Unsafe.getUnsafe().getLong(pRosti + 2 * Long.BYTES);
    }

    public static long getSlotShift(long pRosti) {
        return Unsafe.getUnsafe().getLong(pRosti + 5 * Long.BYTES);
    }

    public static long getValueOffsets(long pRosti) {
        return Unsafe.getUnsafe().getLong(pRosti + 7 * Long.BYTES);
    }

    public static void printRosti(long pRosti) {
        final long slots = getSlots(pRosti);
        final long shift = getSlotShift(pRosti);
        long ctrl = getCtrl(pRosti);
        final long start = ctrl;
        long count = getSize(pRosti);
        while (count > 0) {
            byte b = Unsafe.getUnsafe().getByte(ctrl);
            if ((b & 0x80) == 0) {
                long p = slots + ((ctrl - start) << shift);

//                System.out.println("offset = " + (((ctrl - start) << shift)));
                System.out.println(Unsafe.getUnsafe().getInt(p) + " -> " + Unsafe.getUnsafe().getDouble(p + 4));
//                System.out.println(b + " at " + (ctrl-start));
                count--;
            }
            ctrl++;
        }
    }
}
