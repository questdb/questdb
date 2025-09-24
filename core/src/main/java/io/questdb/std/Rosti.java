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

package io.questdb.std;

import io.questdb.cairo.ColumnTypes;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.std.Numbers.hexDigits;

public final class Rosti {

    public static long alloc(ColumnTypes types, long capacity) {
        // min capacity that works on all platforms is 16  
        assert capacity >= 16;

        final int columnCount = types.getColumnCount();
        final long mem = Unsafe.malloc(4L * columnCount, MemoryTag.NATIVE_ROSTI);
        try {
            long p = mem;
            for (int i = 0; i < columnCount; i++) {
                Unsafe.getUnsafe().putInt(p, types.getColumnType(i));
                p += Integer.BYTES;
            }
            // this is not an exact size of memory allocated for Rosti, but this is useful to
            // track that we free these maps
            long pRosti = alloc(mem, columnCount, Numbers.ceilPow2(capacity) - 1);
            if (pRosti != 0) {
                Unsafe.recordMemAlloc(getAllocMemory(pRosti), MemoryTag.NATIVE_ROSTI);
            }
            return pRosti;
        } finally {
            Unsafe.free(mem, 4L * columnCount, MemoryTag.NATIVE_ROSTI);
        }
    }

    public static native void clear(long pRosti);

    //turns on normal allocation inside rosti
    @TestOnly
    public static native void disableOOMOnMalloc();

    //triggers OOM on next allocation happening inside rosti
    @TestOnly
    public static native void enableOOMOnMalloc();

    public static void free(long pRosti) {
        long size = getAllocMemory(pRosti);
        free0(pRosti);
        Unsafe.recordMemAlloc(-size, MemoryTag.NATIVE_ROSTI);
    }

    public static native long getAllocMemory(long pRosti);

    public static long getCapacity(long pRosti) {
        return Unsafe.getUnsafe().getLong(pRosti + 3 * Long.BYTES);
    }

    public static long getCtrl(long pRosti) {
        return Unsafe.getUnsafe().getLong(pRosti);
    }

    public static long getInitialValueSlot(long pRosti, int columnIndex) {
        return getInitialValuesSlot(pRosti) + Unsafe.getUnsafe().getInt(getValueOffsets(pRosti) + columnIndex * 4L);
    }

    public static long getInitialValuesSlot(long pRosti) {
        return Unsafe.getUnsafe().getLong(pRosti + 8 * Long.BYTES);
    }

    public static long getSize(long pRosti) {
        return Unsafe.getUnsafe().getLong(pRosti + 2 * Long.BYTES);
    }

    public static long getSlotShift(long pRosti) {
        return Unsafe.getUnsafe().getLong(pRosti + 5 * Long.BYTES);
    }

    public static long getSlotSize(long pRosti) {
        return Unsafe.getUnsafe().getLong(pRosti + 4 * Long.BYTES);
    }

    public static long getSlots(long pRosti) {
        return Unsafe.getUnsafe().getLong(pRosti + Long.BYTES);
    }

    public static long getValueOffsets(long pRosti) {
        return Unsafe.getUnsafe().getLong(pRosti + 7 * Long.BYTES);
    }

    public static native boolean keyedIntAvgDoubleWrapUp(long pRosti, int valueOffset, double valueAtNull, long valueAtNullCount);

    public static native boolean keyedIntAvgLongLongWrapUp(long pRosti, int valueOffset, double valueAtNull, long valueAtNullCount);

    // avg long
    public static native boolean keyedIntAvgLongWrapUp(long pRosti, int valueOffset, double valueAtNull, long valueAtNullCount);

    public static native boolean keyedIntCount(long pRosti, long pKeys, long count, int valueOffset);

    public static native boolean keyedIntCountDouble(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native boolean keyedIntCountInt(long pRosti, long pKeys, long pInt, long count, int valueOffset);

    public static native boolean keyedIntCountLong(long pRosti, long pKeys, long pLong, long count, int valueOffset);

    public static native boolean keyedIntCountMerge(long pRostiA, long pRostiB, int valueOffset);

    public static native boolean keyedIntCountWrapUp(long pRosti, int valueOffset, long valueAtNull);

    public static native boolean keyedIntDistinct(long pRosti, long pKeys, long count);

    // ksum double
    public static native boolean keyedIntKSumDouble(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native boolean keyedIntKSumDoubleMerge(long pRostiA, long pRostiB, int valueOffset);

    public static native boolean keyedIntKSumDoubleWrapUp(long pRosti, int valueOffset, double valueAtNull, long valueAtNullCount);

    // max double
    public static native boolean keyedIntMaxDouble(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native boolean keyedIntMaxDoubleMerge(long pRostiA, long pRostiB, int valueOffset);

    public static native boolean keyedIntMaxDoubleWrapUp(long pRosti, int valueOffset, double valueAtNull);

    // max int
    public static native boolean keyedIntMaxInt(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native boolean keyedIntMaxIntMerge(long pRostiA, long pRostiB, int valueOffset);

    public static native boolean keyedIntMaxIntWrapUp(long pRosti, int valueOffset, int valueAtNull);

    // max long
    public static native boolean keyedIntMaxLong(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native boolean keyedIntMaxLongMerge(long pRostiA, long pRostiB, int valueOffset);

    public static native boolean keyedIntMaxLongWrapUp(long pRosti, int valueOffset, long valueAtNull);

    // max short
    public static native boolean keyedIntMaxShort(long pRosti, long pKeys, long pShort, long count, int valueOffset);

    public static native boolean keyedIntMaxShortWrapUp(long pRosti, int valueOffset, int accumulatedValue);

    // min double
    public static native boolean keyedIntMinDouble(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native boolean keyedIntMinDoubleMerge(long pRostiA, long pRostiB, int valueOffset);

    public static native boolean keyedIntMinDoubleWrapUp(long pRosti, int valueOffset, double valueAtNull);

    // min int
    public static native boolean keyedIntMinInt(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native boolean keyedIntMinIntMerge(long pRostiA, long pRostiB, int valueOffset);

    public static native boolean keyedIntMinIntWrapUp(long pRosti, int valueOffset, int valueAtNull);

    // min long
    public static native boolean keyedIntMinLong(long pRosti, long pKeys, long pLong, long count, int valueOffset);

    public static native boolean keyedIntMinLongMerge(long pRostiA, long pRostiB, int valueOffset);

    public static native boolean keyedIntMinLongWrapUp(long pRosti, int valueOffset, long valueAtNull);

    // min short
    public static native boolean keyedIntMinShort(long pRosti, long pKeys, long pShort, long count, int valueOffset);

    public static native boolean keyedIntMinShortWrapUp(long pRosti, int valueOffset, long accumulatedValue);

    // nsum double
    public static native boolean keyedIntNSumDouble(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native boolean keyedIntNSumDoubleMerge(long pRostiA, long pRostiB, int valueOffset);

    public static native boolean keyedIntNSumDoubleWrapUp(long pRosti, int valueOffset, double valueAtNull, long valueAtNullCount, double valueAtNullC);

    // sum double
    public static native boolean keyedIntSumDouble(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native boolean keyedIntSumDoubleMerge(long pRostiA, long pRostiB, int valueOffset);

    public static native boolean keyedIntSumDoubleWrapUp(long pRosti, int valueOffset, double valueAtNull, long valueAtNullCount);

    // sum int
    public static native boolean keyedIntSumInt(long pRosti, long pKeys, long pInt, long count, int valueOffset);

    public static native boolean keyedIntSumIntMerge(long pRostiA, long pRostiB, int valueOffset);

    // sum long
    public static native boolean keyedIntSumLong(long pRosti, long pKeys, long pLong, long count, int valueOffset);

    public static native boolean keyedIntSumLong256(long pRosti, long pKeys, long pLong, long count, int valueOffset);

    public static native boolean keyedIntSumLong256Merge(long pRostiA, long pRostiB, int valueOffset);

    public static native boolean keyedIntSumLong256WrapUp(long pRosti, int valueOffset, long v0, long v1, long v2, long v3, long valueAtNullCount);

    public static native boolean keyedIntSumLongLong(long pRosti, long pKeys, long pLong, long count, int valueOffset);

    public static native boolean keyedIntSumLongLongMerge(long pRostiA, long pRostiB, int valueOffset);

    public static native boolean keyedIntSumLongMerge(long pRostiA, long pRostiB, int valueOffset);

    public static native boolean keyedIntSumLongWrapUp(long pRosti, int valueOffset, long valueAtNull, long valueAtNullCount);

    // sum short
    public static native boolean keyedIntSumShort(long pRosti, long pKeys, long pShort, long count, int valueOffset);

    public static native boolean keyedIntSumShortLong(long pRosti, long pKeys, long pLong, long count, int valueOffset);

    //returns true if rosti is set to trigger OOM on  allocation
    public static native boolean keyedMicroHourCount(long pRosti, long pKeys, long count, int valueOffset);

    public static native boolean keyedMicroHourCountDouble(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native boolean keyedMicroHourCountInt(long pRosti, long pKeys, long pInt, long count, int valueOffset);

    public static native boolean keyedMicroHourCountLong(long pRosti, long pKeys, long pLong, long count, int valueOffset);

    public static native boolean keyedMicroHourDistinct(long pRosti, long pKeys, long count);

    public static native boolean keyedMicroHourKSumDouble(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native boolean keyedMicroHourMaxDouble(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native boolean keyedMicroHourMaxInt(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native boolean keyedMicroHourMaxLong(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native boolean keyedMicroHourMaxShort(long pRosti, long pKeys, long pShort, long count, int valueOffset);

    public static native boolean keyedMicroHourMinDouble(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native boolean keyedMicroHourMinInt(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native boolean keyedMicroHourMinLong(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native boolean keyedMicroHourMinShort(long pRosti, long pKeys, long pShort, long count, int valueOffset);

    public static native boolean keyedMicroHourNSumDouble(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native boolean keyedMicroHourSumDouble(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native boolean keyedMicroHourSumInt(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native boolean keyedMicroHourSumLong(long pRosti, long pKeys, long pLong, long count, int valueOffset);

    // sum long256
    public static native boolean keyedMicroHourSumLong256(long pRosti, long pKeys, long pLong256, long count, int valueOffset);

    public static native boolean keyedMicroHourSumLongLong(long pRosti, long pKeys, long pLong, long count, int valueOffset);

    public static native boolean keyedMicroHourSumShort(long pRosti, long pKeys, long pShort, long count, int valueOffset);

    public static native boolean keyedMicroHourSumShortLong(long pRosti, long pKeys, long pLong, long count, int valueOffset);

    public static native boolean keyedNanoHourCount(long pRosti, long pKeys, long count, int valueOffset);

    public static native boolean keyedNanoHourCountDouble(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native boolean keyedNanoHourCountInt(long pRosti, long pKeys, long pInt, long count, int valueOffset);

    public static native boolean keyedNanoHourCountLong(long pRosti, long pKeys, long pLong, long count, int valueOffset);

    public static native boolean keyedNanoHourDistinct(long pRosti, long pKeys, long count);

    public static native boolean keyedNanoHourKSumDouble(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native boolean keyedNanoHourMaxDouble(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native boolean keyedNanoHourMaxInt(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native boolean keyedNanoHourMaxLong(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native boolean keyedNanoHourMaxShort(long pRosti, long pKeys, long pShort, long count, int valueOffset);

    public static native boolean keyedNanoHourMinDouble(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native boolean keyedNanoHourMinInt(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native boolean keyedNanoHourMinLong(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native boolean keyedNanoHourMinShort(long pRosti, long pKeys, long pShort, long count, int valueOffset);

    public static native boolean keyedNanoHourNSumDouble(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native boolean keyedNanoHourSumDouble(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native boolean keyedNanoHourSumInt(long pRosti, long pKeys, long pDouble, long count, int valueOffset);

    public static native boolean keyedNanoHourSumLong(long pRosti, long pKeys, long pLong, long count, int valueOffset);

    // sum long256
    public static native boolean keyedNanoHourSumLong256(long pRosti, long pKeys, long pLong256, long count, int valueOffset);

    public static native boolean keyedNanoHourSumLongLong(long pRosti, long pKeys, long pLong, long count, int valueOffset);

    public static native boolean keyedNanoHourSumShort(long pRosti, long pKeys, long pShort, long count, int valueOffset);

    public static native boolean keyedNanoHourSumShortLong(long pRosti, long pKeys, long pLong, long count, int valueOffset);

    public static void printRosti(long pRosti) {
        final long slots = getSlots(pRosti);
        final long shift = getSlotShift(pRosti);
        long ctrl = getCtrl(pRosti);
        final long start = ctrl;
        long count = getSize(pRosti);
        System.out.println("size=" + count);
        System.out.println("capacity=" + getCapacity(pRosti));
        System.out.println("slot size=" + getSlotSize(pRosti));
        System.out.println("slot shift=" + getSlotShift(pRosti));
        System.out.print("initial slot=");
        long initialSlot = getInitialValuesSlot(pRosti);
        for (long i = 0, n = getSlotSize(pRosti); i < n; i++) {

            byte b = Unsafe.getUnsafe().getByte(initialSlot + i);
            final int v;
            if (b < 0) {
                v = 256 + b;
            } else {
                v = b;
            }

            if (v < 0x10) {
                System.out.print('0');
                System.out.print(hexDigits[b]);
            } else {
                System.out.print(hexDigits[v / 0x10]);
                System.out.print(hexDigits[v % 0x10]);
            }

            System.out.print(' ');
        }
        System.out.println();
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

    public static boolean reset(long pRosti, int size) {
        long oldSize = Rosti.getAllocMemory(pRosti);
        boolean success = reset0(pRosti, Numbers.ceilPow2(size) - 1);
        updateMemoryUsage(pRosti, oldSize);
        return success;
    }

    public static void updateMemoryUsage(long pRosti, long oldSize) {
        long newSize = Rosti.getAllocMemory(pRosti);
        Unsafe.recordMemAlloc(newSize - oldSize, MemoryTag.NATIVE_ROSTI);
    }

    private static native long alloc(long pKeyTypes, int keyTypeCount, long capacity);

    private static native void free0(long pRosti);

    //clears and shrinks to given size
    private static native boolean reset0(long pRosti, int size);
}
