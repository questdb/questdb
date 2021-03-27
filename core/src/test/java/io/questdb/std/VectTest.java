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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class VectTest {

    private static final Rnd rnd = new Rnd();

    @Before
    public void setUp() {
        rnd.reset();
    }

    @Test
    public void testMergeFourSameSize() {
        final int count = 1_000_000;
        long indexPtr1 = seedAndSort(count);
        long indexPtr2 = seedAndSort(count);
        long indexPtr3 = seedAndSort(count);
        long indexPtr4 = seedAndSort(count);

        long struct = Unsafe.malloc(Long.BYTES * 8);
        Unsafe.getUnsafe().putLong(struct, indexPtr1);
        Unsafe.getUnsafe().putLong(struct + Long.BYTES, count);

        Unsafe.getUnsafe().putLong(struct + 2 * Long.BYTES, indexPtr2);
        Unsafe.getUnsafe().putLong(struct + 3 * Long.BYTES, count);

        Unsafe.getUnsafe().putLong(struct + 4 * Long.BYTES, indexPtr3);
        Unsafe.getUnsafe().putLong(struct + 5 * Long.BYTES, count);

        Unsafe.getUnsafe().putLong(struct + 6 * Long.BYTES, indexPtr4);
        Unsafe.getUnsafe().putLong(struct + 7 * Long.BYTES, count);
        try {
            long merged = Vect.mergeLongIndexesAsc(struct, 4);
            assertIndexAsc(count * 4, merged);
            Vect.freeMergedIndex(merged);
        } finally {
            Unsafe.free(indexPtr1, count * 2 * Long.BYTES);
            Unsafe.free(indexPtr2, count * 2 * Long.BYTES);
            Unsafe.free(indexPtr3, count * 2 * Long.BYTES);
            Unsafe.free(indexPtr4, count * 2 * Long.BYTES);
            Unsafe.free(struct, Long.BYTES * 8);
        }
    }

    @Test
    public void testMergeOne() {
        long indexPtr = seedAndSort(150);
        try {
            assertIndexAsc(150, indexPtr);
        } finally {
            Unsafe.free(indexPtr, 150 * 2 * Long.BYTES);
        }
    }

    @Test
    public void testMergeThreeDifferentSizes() {
        final int count1 = 1_000_000;
        final int count2 = 500_000;
        final int count3 = 750_000;
        long indexPtr1 = seedAndSort(count1);
        long indexPtr2 = seedAndSort(count2);
        long indexPtr3 = seedAndSort(count3);

        long struct = Unsafe.malloc(Long.BYTES * 6);
        Unsafe.getUnsafe().putLong(struct, indexPtr1);
        Unsafe.getUnsafe().putLong(struct + Long.BYTES, count1);
        Unsafe.getUnsafe().putLong(struct + 2 * Long.BYTES, indexPtr2);
        Unsafe.getUnsafe().putLong(struct + 3 * Long.BYTES, count2);
        Unsafe.getUnsafe().putLong(struct + 4 * Long.BYTES, indexPtr3);
        Unsafe.getUnsafe().putLong(struct + 5 * Long.BYTES, count3);
        try {
            long merged = Vect.mergeLongIndexesAsc(struct, 3);
            assertIndexAsc(count1 + count2 + count3, merged);
            Vect.freeMergedIndex(merged);
        } finally {
            Unsafe.free(indexPtr1, count1 * 2 * Long.BYTES);
            Unsafe.free(indexPtr2, count2 * 2 * Long.BYTES);
            Unsafe.free(indexPtr3, count3 * 2 * Long.BYTES);
            Unsafe.free(struct, Long.BYTES * 6);
        }
    }

    @Test
    public void testMergeTwoDifferentSizesAB() {
        final int count1 = 1_000_000;
        final int count2 = 500_000;
        long indexPtr1 = seedAndSort(count1);
        long indexPtr2 = seedAndSort(count2);

        long struct = Unsafe.malloc(Long.BYTES * 4);
        Unsafe.getUnsafe().putLong(struct, indexPtr1);
        Unsafe.getUnsafe().putLong(struct + Long.BYTES, count1);
        Unsafe.getUnsafe().putLong(struct + 2 * Long.BYTES, indexPtr2);
        Unsafe.getUnsafe().putLong(struct + 3 * Long.BYTES, count2);
        try {
            long merged = Vect.mergeLongIndexesAsc(struct, 2);
            assertIndexAsc(count1 + count2, merged);
            Vect.freeMergedIndex(merged);
        } finally {
            Unsafe.free(indexPtr1, count1 * 2 * Long.BYTES);
            Unsafe.free(indexPtr2, count2 * 2 * Long.BYTES);
            Unsafe.free(struct, Long.BYTES * 4);
        }
    }

    @Test
    public void testMergeTwoDifferentSizesBA() {
        final int count1 = 1_000_000;
        final int count2 = 2_000_000;
        long indexPtr1 = seedAndSort(count1);
        long indexPtr2 = seedAndSort(count2);

        long struct = Unsafe.malloc(Long.BYTES * 4);
        Unsafe.getUnsafe().putLong(struct, indexPtr1);
        Unsafe.getUnsafe().putLong(struct + Long.BYTES, count1);
        Unsafe.getUnsafe().putLong(struct + 2 * Long.BYTES, indexPtr2);
        Unsafe.getUnsafe().putLong(struct + 3 * Long.BYTES, count2);
        try {
            long merged = Vect.mergeLongIndexesAsc(struct, 2);
            assertIndexAsc(count1 + count2, merged);
            Vect.freeMergedIndex(merged);
        } finally {
            Unsafe.free(indexPtr1, count1 * 2 * Long.BYTES);
            Unsafe.free(indexPtr2, count2 * 2 * Long.BYTES);
            Unsafe.free(struct, Long.BYTES * 4);
        }
    }

    @Test
    public void testMergeTwoSameSize() {
        final int count = 1_000_000;
        long indexPtr1 = seedAndSort(count);
        long indexPtr2 = seedAndSort(count);

        long struct = Unsafe.malloc(Long.BYTES * 4);
        Unsafe.getUnsafe().putLong(struct, indexPtr1);
        Unsafe.getUnsafe().putLong(struct + Long.BYTES, count);
        Unsafe.getUnsafe().putLong(struct + 2 * Long.BYTES, indexPtr2);
        Unsafe.getUnsafe().putLong(struct + 3 * Long.BYTES, count);
        try {
            long merged = Vect.mergeLongIndexesAsc(struct, 2);
            assertIndexAsc(count * 2, merged);
            Vect.freeMergedIndex(merged);
        } finally {
            Unsafe.free(indexPtr1, count * 2 * Long.BYTES);
            Unsafe.free(indexPtr2, count * 2 * Long.BYTES);
            Unsafe.free(struct, Long.BYTES * 4);
        }
    }

    @Test
    public void testMergeZero() {
        Assert.assertEquals(0, Vect.mergeLongIndexesAsc(0, 0));
    }

    @Test
    public void testSort1M() {
        testSort(1_000_000);
    }

    @Test
    public void testSortFour() {
        for (int i = 0; i < 100; i++) {
            testSort(4);
        }
    }

    @Test
    public void testSortOne() {
        final long indexAddr = Unsafe.malloc(2 * Long.BYTES);
        try {
            seedMem(1, indexAddr);
            long expected = Unsafe.getUnsafe().getLong(indexAddr);
            Vect.sortLongIndexAscInPlace(indexAddr, 1);
            Assert.assertEquals(expected, Unsafe.getUnsafe().getLong(indexAddr));
        } finally {
            Unsafe.free(indexAddr, 2 * Long.BYTES);
        }
    }

    @Test
    public void testSetMemoryVanillaLong() {
        int[] sizes = new int[]{0, 1, 3, 4, 5, 7, 9, 15, 20, 1024 * 1024 - 1, 1024 * 1024, 1024 * 1024 + 1, 2_000_000, 10_000_000};
        long[] values = new long[]{-1, 0, 1, Long.MIN_VALUE, Long.MAX_VALUE, 0xabcd};
        long buffSize = sizes[sizes.length - 1] * Long.BYTES;
        long buffer = Unsafe.getUnsafe().allocateMemory(buffSize);

        try {
            for (int size : sizes) {
                for (long val : values) {
                    Vect.setMemoryLong(buffer, val, size);
                    for (int i = 0; i < size; i++) {
                        long actual = Unsafe.getUnsafe().getLong(buffer + i * Long.BYTES);
                        if (val != actual) {
                            Assert.assertEquals("Failed to set for size=" + size + ", value=" + val + ", pos=" + i, val, actual);
                        }
                    }
                }
            }
        } finally {
            Unsafe.free(buffer, buffSize);
        }
    }

    @Test
    public void testSetMemoryVanillaDouble() {
        int[] sizes = new int[]{0, 1, 3, 4, 5, 7, 15, 20, 1024 * 1024, 1024 * 1024 + 1, 2_000_000, 10_000_000};
        double[] values = new double[]{-1, 0, 1, Double.MIN_VALUE, Double.MAX_VALUE, Double.NaN, 1.0023455};
        int typeBytes = Double.BYTES;
        long buffSize = sizes[sizes.length - 1] * typeBytes;
        long buffer = Unsafe.getUnsafe().allocateMemory(buffSize);

        try {
            for (int size : sizes) {
                for (double val : values) {
                    Vect.setMemoryDouble(buffer, val, size);
                    for (int i = 0; i < size; i++) {
                        double actual = Unsafe.getUnsafe().getDouble(buffer + i * typeBytes);
                        if (val != actual && !(Double.isNaN(val) && Double.isNaN(actual))) {
                            Assert.assertEquals("Failed to set for size=" + size + ", value=" + val + ", pos=" + i, val, actual, 1E-24);
                        }
                    }
                }
            }
        } finally {
            Unsafe.free(buffer, buffSize);
        }
    }

    @Test
    public void testSetMemoryVanillaFloat() {
        int[] sizes = new int[]{0, 1, 3, 4, 5, 6, 7, 8, 10, 12, 15, 19, 1024 * 1024, 1024 * 1024 + 1, 2_000_000, 10_000_000};
        float[] values = new float[]{-1, 0, 1, Float.MIN_VALUE, Float.MAX_VALUE, Float.NaN, 1.0023455f};
        int typeBytes = Float.BYTES;
        long buffSize = sizes[sizes.length - 1] * typeBytes;
        long buffer = Unsafe.getUnsafe().allocateMemory(buffSize);

        try {
            for (int size : sizes) {
                for (float val : values) {
                    Vect.setMemoryFloat(buffer, val, size);
                    for (int i = 0; i < size; i++) {
                        float actual = Unsafe.getUnsafe().getFloat(buffer + i * typeBytes);
                        if (val != actual && !(Float.isNaN(val) && Float.isNaN(actual))) {
                            Assert.assertEquals("Failed to set for size=" + size + ", value=" + val + ", pos=" + i, val, actual, 1E-20);
                        }
                    }
                }
            }
        } finally {
            Unsafe.free(buffer, buffSize);
        }
    }

    @Test
    public void testSetMemoryVanillaInt() {
        int[] sizes = new int[]{0, 1, 3, 4, 5, 6, 7, 8, 10, 12, 15, 19, 1024 * 1024, 1024 * 1024 + 1, 2_000_000, 10_000_000};
        int[] values = new int[]{-1, 0, 1, Integer.MIN_VALUE, Integer.MAX_VALUE, 0xabcd};
        int typeBytes = Integer.BYTES;
        long buffSize = sizes[sizes.length - 1] * typeBytes;
        long buffer = Unsafe.getUnsafe().allocateMemory(buffSize);

        try {
            for (int size : sizes) {
                for (int val : values) {
                    Vect.setMemoryInt(buffer, val, size);
                    for (int i = 0; i < size; i++) {
                        long actual = Unsafe.getUnsafe().getInt(buffer + i * typeBytes);
                        if (val != actual) {
                            Assert.assertEquals("Failed to set for size=" + size + ", value=" + val + ", pos=" + i, val, actual);
                        }
                    }
                }
            }
        } finally {
            Unsafe.free(buffer, buffSize);
        }
    }

    @Test
    public void testSetMemoryVanillaShort() {
        int[] sizes =  new int[]{ 1, 3, 4, 5, 6, 7, 8, 10, 12, 15, 19, 1024 * 1024, 1024 * 1024 + 1, 2_000_000, 10_000_000};
        int[] offsetBytes = new int[]{4};
        int maxOffset = 16;

        short[] values = new short[]{-1, 0, 1, Short.MIN_VALUE, Short.MAX_VALUE, 0xabc};
        int typeBytes = Short.BYTES;
        long buffSize = sizes[sizes.length - 1] * typeBytes + maxOffset;
        long buffer = Unsafe.getUnsafe().allocateMemory(buffSize);

        try {
            for (int offset: offsetBytes) {
                for (int size : sizes) {
                    for (short val : values) {
                        Vect.setMemoryShort(buffer + offset, val, size);
                        for (int i = 0; i < size; i++) {
                            short actual = Unsafe.getUnsafe().getShort(buffer + offset + i * typeBytes);
                            if (val != actual) {
                                Assert.assertEquals("Failed to set for size=" + size + ", value=" + val + ", pos=" + i, val, actual);
                            }
                        }
                    }
                }
            }
        } finally {
            Unsafe.free(buffer, buffSize);
        }
    }

    @Test
    public void testResuffleInt64() {
        int[] sizes = new int[]{0, 1, 3, 4, 5, 1024 * 1024 + 2};
        int typeBytes = Long.BYTES;
        int maxSize = sizes[sizes.length - 1];
        long buffSize = maxSize * typeBytes;

        int indexBuffSize = maxSize * Long.BYTES * 2;
        long index = Unsafe.getUnsafe().allocateMemory(indexBuffSize);
        long dst = Unsafe.getUnsafe().allocateMemory(buffSize);
        long src = Unsafe.getUnsafe().allocateMemory(buffSize);

        for(int i = 0; i < maxSize; i++) {
            long offset = (2 * i + 1) * Long.BYTES;
            long expected = (i % 2 == 0) ? i + 1 : i - 1;
            Unsafe.getUnsafe().putLong(index + offset, expected);
        }

        for (int i = 0; i < maxSize; i++) {
            Unsafe.getUnsafe().putLong(src + i * Long.BYTES, i);
        }

        try {
            for (int size : sizes) {
                    Vect.indexReshuffle64Bit(src, dst, index, size);

                    for (int i = 0; i < size; i++) {
                        long actual = Unsafe.getUnsafe().getLong(dst + i * typeBytes);
                        long expected = (i % 2 == 0) ? i + 1 : i - 1;
                        if (expected != actual) {
                            Assert.assertEquals("Failed to init reshuffle size=" + size
                                    + ", expected=" + expected + ", pos=" + i, expected, actual);
                        }
                    }
            }
        } finally {
            Unsafe.free(index, indexBuffSize);
            Unsafe.free(src, buffSize);
            Unsafe.free(dst, buffSize);
        }
    }

    private void assertIndexAsc(int count, long indexAddr) {
        long v = Unsafe.getUnsafe().getLong(indexAddr);
        for (int i = 1; i < count; i++) {
            long next = Unsafe.getUnsafe().getLong(indexAddr + i * 2L * Long.BYTES);
            if (next < v) {
                System.out.println("wtf?: " + next + " < " + v);
            }
            Assert.assertTrue(next >= v);
            v = next;
        }
    }

    private long seedAndSort(int count) {
        final long indexAddr = Unsafe.malloc(count * 2L * Long.BYTES);
        seedMem(count, indexAddr);
        Vect.sortLongIndexAscInPlace(indexAddr, count);
        return indexAddr;
    }

    private void seedMem(int count, long p) {
        for (int i = 0; i < count; i++) {
            final long z = rnd.nextPositiveLong();
            Unsafe.getUnsafe().putLong(p + i * 2L * Long.BYTES, z);
            Unsafe.getUnsafe().putLong(p + i * 2L * Long.BYTES + 8, i);
        }
    }

    private void testSort(int count) {
        final int size = count * 2 * Long.BYTES;
        final long indexAddr = Unsafe.malloc(size);
        try {
            seedMem(count, indexAddr);
            Vect.sortLongIndexAscInPlace(indexAddr, count);
            assertIndexAsc(count, indexAddr);
        } finally {
            Unsafe.free(indexAddr, size);
        }
    }

    static {
        Os.init();
    }
}
