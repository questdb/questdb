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
        final long mem = Unsafe.malloc(4L * columnCount, MemoryTag.NATIVE_DEFAULT);
        try {
            long p = mem;
            for (int i = 0; i < columnCount; i++) {
                Unsafe.getUnsafe().putInt(p, types.getColumnType(i));
                p += Integer.BYTES;
            }
            // this is not an exact size of memory allocated for Rosti, but this is useful to
            // track that we free these maps
            Unsafe.recordMemAlloc(FAKE_ALLOC_SIZE, MemoryTag.NATIVE_DEFAULT);
            return alloc(mem, columnCount, Numbers.ceilPow2(capacity) - 1);
        } finally {
            Unsafe.free(mem, 4L * columnCount, MemoryTag.NATIVE_DEFAULT);
        }
    }

    public static void free(long pRosti) {
        free0(pRosti);
        Unsafe.recordMemAlloc(-FAKE_ALLOC_SIZE, MemoryTag.NATIVE_DEFAULT);
    }

    private static native void free0(long pRosti);

    public static native void clear(long pRosti);

    public static native void keyedIntDistinct(long pRosti, long pKeys, long count);

    public static native void keyedHourDistinct(long pRosti, long pKeys, long count);

    public static native void keyedHourCount(long pRosti, long pKeys, long count, int valueOffset);

    public static native void keyedIntCount(long pRosti, long pKeys, long count, int valueOffset);

    public static native void keyedIntCountMerge(long pRostiA, long pRostiB, int valueOffset);

    // sum double
    public static native void keyedIntSumDouble(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native void keyedHourSumDouble(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native void keyedIntSumDoubleMerge(long pRostiA, long pRostiB, int valueOffset);

    public static native void keyedIntSumDoubleWrapUp(long pRosti, int valueOffset, double valueAtNull, long valueAtNullCount);

    public static native void keyedIntAvgDoubleWrapUp(long pRosti, int valueOffset, double valueAtNull, long valueAtNullCount);

    // ksum double
    public static native void keyedIntKSumDouble(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native void keyedHourKSumDouble(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native void keyedIntKSumDoubleMerge(long pRostiA, long pRostiB, int valueOffset);

    public static native void keyedIntKSumDoubleWrapUp(long pRosti, int valueOffset, double valueAtNull, long valueAtNullCount);

    // nsum double
    public static native void keyedIntNSumDouble(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native void keyedHourNSumDouble(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native void keyedIntNSumDoubleMerge(long pRostiA, long pRostiB, int valueOffset);

    public static native void keyedIntNSumDoubleWrapUp(long pRosti, int valueOffset, double valueAtNull, long valueAtNullCount, double valueAtNullC);

    // min double
    public static native void keyedIntMinDouble(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native void keyedHourMinDouble(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native void keyedIntMinDoubleMerge(long pRostiA, long pRostiB, int valueOffset);

    public static native void keyedIntMinDoubleWrapUp(long pRosti, int valueOffset, double valueAtNull);

    // max double
    public static native void keyedIntMaxDouble(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native void keyedHourMaxDouble(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native void keyedIntMaxDoubleMerge(long pRostiA, long pRostiB, int valueOffset);

    public static native void keyedIntMaxDoubleWrapUp(long pRosti, int valueOffset, double valueAtNull);

    // sum int
    public static native void keyedIntSumInt(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native void keyedHourSumInt(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native void keyedIntSumIntMerge(long pRostiA, long pRostiB, int valueOffset);

    // min int
    public static native void keyedIntMinInt(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native void keyedHourMinInt(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native void keyedIntMinIntMerge(long pRostiA, long pRostiB, int valueOffset);

    public static native void keyedIntMinIntWrapUp(long pRosti, int valueOffset, int valueAtNull);

    // max int
    public static native void keyedIntMaxInt(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native void keyedHourMaxInt(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native void keyedIntMaxIntMerge(long pRostiA, long pRostiB, int valueOffset);

    public static native void keyedIntMaxIntWrapUp(long pRosti, int valueOffset, int valueAtNull);

    // sum long
    public static native void keyedIntSumLong(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native void keyedHourSumLong(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native void keyedIntSumLongMerge(long pRostiA, long pRostiB, int valueOffset);

    public static native void keyedIntSumLongWrapUp(long pRosti, int valueOffset, long valueAtNull, long valueAtNullCount);

    // avg long
    public static native void keyedIntAvgLongWrapUp(long pRosti, int valueOffset, double valueAtNull, long valueAtNullCount);

    // min long
    public static native void keyedIntMinLong(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native void keyedHourMinLong(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native void keyedIntMinLongMerge(long pRostiA, long pRostiB, int valueOffset);

    public static native void keyedIntMinLongWrapUp(long pRosti, int valueOffset, long valueAtNull);

    // max long
    public static native void keyedIntMaxLong(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native void keyedHourMaxLong(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native void keyedIntMaxLongMerge(long pRostiA, long pRostiB, int valueOffset);

    public static native void keyedIntMaxLongWrapUp(long pRosti, int valueOffset, long valueAtNull);

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
                System.out.println(Unsafe.getUnsafe().getInt(p) + " -> " + Unsafe.getUnsafe().getDouble(p + 12));
                count--;
            }
            ctrl++;
        }
    }

    public static long getInitialValueSlot(long pRosti, int columnIndex) {
        return getInitialValuesSlot(pRosti) + Unsafe.getUnsafe().getInt(getValueOffsets(pRosti) + columnIndex * 4L);
    }
}
