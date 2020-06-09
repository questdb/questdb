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

    // sum double
    public static native void keyedIntSumDouble(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native void keyedIntSumZero(long pRosti, long pKeys, long count, int valueOffset);

    public static native void keyedIntSumDoubleMerge(long pRostiA, long pRostiB, int valueOffset);

    public static native void keyedIntSumDoubleWrapUp(long pRosti, int valueOffset, double valueAtNull, long valueAtNullCount);

    public static native void keyedIntAvgDoubleSetNull(long pRosti, int valueOffset);

    // ksum double
    public static native void keyedIntKSumDouble(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native void keyedIntKSumDoubleMerge(long pRostiA, long pRostiB, int valueOffset);

    public static native void keyedIntKSumDoubleSetNull(long pRosti, int valueOffset);

    // nsum double
    public static native void keyedIntNSumDouble(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native void keyedIntNSumDoubleMerge(long pRostiA, long pRostiB, int valueOffset);

    public static native void keyedIntNSumDoubleSetNull(long pRosti, int valueOffset);

    // max double
    public static native void keyedIntMinDouble(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native void keyedIntMinDoubleMerge(long pRostiA, long pRostiB, int valueOffset);

    public static native void keyedIntMinDoubleSetNull(long pRosti, int valueOffset);

    // min double
    public static native void keyedIntMaxDouble(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native void keyedIntMaxDoubleMerge(long pRostiA, long pRostiB, int valueOffset);

    public static native void keyedIntMaxDoubleSetNull(long pRosti, int valueOffset);

    // sum int
    public static native void keyedIntSumInt(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native void keyedIntSumIntMerge(long pRostiA, long pRostiB, int valueOffset);

    public static native void keyedIntSumIntSetNull(long pRosti, int valueOffset);

    // avg int
    public static native void keyedIntAvgIntSetNull(long pRosti, int valueOffset);

    // min int
    public static native void keyedIntMinInt(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native void keyedIntMinIntMerge(long pRostiA, long pRostiB, int valueOffset);

    public static native void keyedIntMinIntSetNull(long pRosti, int valueOffset);

    // max int
    public static native void keyedIntMaxInt(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native void keyedIntMaxIntMerge(long pRostiA, long pRostiB, int valueOffset);

    // sum long
    public static native void keyedIntSumLong(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native void keyedIntSumLongMerge(long pRostiA, long pRostiB, int valueOffset);

    public static native void keyedIntSumLongSetNull(long pRosti, int valueOffset);

    // avg long
    public static native void keyedIntAvgLongSetNull(long pRosti, int valueOffset);

    // min long
    public static native void keyedIntMinLong(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native void keyedIntMinLongMerge(long pRostiA, long pRostiB, int valueOffset);

    public static native void keyedIntMinLongSetNull(long pRosti, int valueOffset);

    // max long
    public static native void keyedIntMaxLong(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native void keyedIntMaxLongMerge(long pRostiA, long pRostiB, int valueOffset);

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

    public static long getInitialValuesSlot(long pRosti) {
        return Unsafe.getUnsafe().getLong(pRosti + 8 * Long.BYTES);
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
                System.out.println(Unsafe.getUnsafe().getInt(p) + " -> " + Unsafe.getUnsafe().getDouble(p + 12));
//                System.out.println(b + " at " + (ctrl-start));
                count--;
            }
            ctrl++;
        }
    }

    public static long getInitialValueSlot(long pRosti, int columnIndex) {
        return getInitialValuesSlot(pRosti) + Unsafe.getUnsafe().getInt(getValueOffsets(pRosti) + columnIndex * Integer.BYTES);
    }
}
