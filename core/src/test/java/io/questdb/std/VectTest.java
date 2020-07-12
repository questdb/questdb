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

    static {
        Os.init();
    }

    @Before
    public void setUp() {
        rnd.reset();
    }

    private long seedAndSort(int count) {
        final long indexAddr = Unsafe.malloc(count * 2 * Long.BYTES);
        seedMem(count, indexAddr);
        Vect.sortLongIndexAscInPlace(indexAddr, count);
        return indexAddr;
    }

    private void seedMem(int count, long p) {
        for (int i = 0; i < count; i++) {
            long z = rnd.nextPositiveLong();
            Unsafe.getUnsafe().putLong(p + i * 2 * Long.BYTES, z);
            Unsafe.getUnsafe().putLong(p + i * 2 * Long.BYTES + 8, i);
        }
    }

    @Test
    public void testMergeZero() {
        Assert.assertEquals(0, Vect.mergeLongIndexesAsc(0, 0));
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
    public void testMergeThreeDifferentSizesAB() {
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
    public void testSortFour() {
        testSort(4);
    }

    @Test
    public void testSort1M() {
        testSort(1_000_000);
    }

    private void testSort(int count) {
        final int size = count * 2 * Long.BYTES;
        final long indexAddr = Unsafe.malloc(size);
        try {
            seedMem(1, indexAddr);
            Vect.sortLongIndexAscInPlace(indexAddr, count);
            assertIndexAsc(count, indexAddr);
        } finally {
            Unsafe.free(indexAddr, size);
        }
    }

    private void assertIndexAsc(int count, long indexAddr) {
        long v = Unsafe.getUnsafe().getLong(indexAddr);
        for (int i = 1; i < count; i++) {
            long next = Unsafe.getUnsafe().getLong(indexAddr + i * 2 * Long.BYTES);
            Assert.assertTrue(next >= v);
            v = next;
        }
    }
}
