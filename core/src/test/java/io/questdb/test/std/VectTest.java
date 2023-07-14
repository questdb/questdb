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

package io.questdb.test.std;

import io.questdb.cairo.BinarySearch;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DedupColumnCommitAddresses;
import io.questdb.std.*;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.cairo.AbstractIntervalDataFrameCursor.SCAN_UP;
import static io.questdb.cairo.BinarySearch.SCAN_DOWN;

public class VectTest {

    private Rnd rnd = new Rnd();

    @Before
    public void setUp() {
        rnd.reset();
    }

    @Test
    public void testBinarySearchIndexT() {
        int count = 1000;
        final int size = count * 2 * Long.BYTES;
        final long addr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);

        try {
            // 0,0,0,2,2,2,4,4,4 ...
            for (int i = 0; i < count; i++) {
                long value = (i / 3) * 2;
                Unsafe.getUnsafe().putLong(addr + i * 2 * Long.BYTES, value);
            }

            // Existing
            Assert.assertEquals(2, Vect.binarySearchIndexT(addr, 0, 0, count - 1, SCAN_DOWN));
            Assert.assertEquals(0, Vect.binarySearchIndexT(addr, 0, 0, count - 1, SCAN_UP));

            // Non-existing
            Assert.assertEquals(3, -Vect.binarySearchIndexT(addr, 1, 0, count - 1, SCAN_DOWN) - 1);
            Assert.assertEquals(3, -Vect.binarySearchIndexT(addr, 1, 0, count - 1, SCAN_UP) - 1);

            // Generalize
            for (int i = 0; i < count / 3; i++) {
                int existingValue = i * 2;
                Assert.assertEquals(i * 3 + 2, Vect.binarySearchIndexT(addr, existingValue, 0, count - 1, SCAN_DOWN));
                Assert.assertEquals(i * 3, Vect.binarySearchIndexT(addr, existingValue, 0, count - 1, SCAN_UP));

                int nonExisting = i * 2 + 1;
                Assert.assertEquals(i * 3 + 3, -Vect.binarySearchIndexT(addr, nonExisting, 0, count - 1, SCAN_DOWN) - 1);
                Assert.assertEquals(i * 3 + 3, -Vect.binarySearchIndexT(addr, nonExisting, 0, count - 1, SCAN_UP) - 1);
            }
        } finally {
            Unsafe.free(addr, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testBoundedBinarySearchIndexT() {
        int count = 1000;
        final int size = count * 2 * Long.BYTES;
        final long addr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);

        try {
            // 0,0,0,2,2,2,4,4,4 ...
            for (int i = 0; i < count; i++) {
                long value = (i / 3) * 2;
                Unsafe.getUnsafe().putLong(addr + i * 2 * Long.BYTES, value);
            }

            // Existing
            Assert.assertEquals(2, Vect.boundedBinarySearchIndexT(addr, 0, 0, count - 1, SCAN_DOWN));
            Assert.assertEquals(0, Vect.boundedBinarySearchIndexT(addr, 0, 0, count - 1, SCAN_UP));

            // Non-existing
            Assert.assertEquals(2, Vect.boundedBinarySearchIndexT(addr, 1, 0, count - 1, SCAN_DOWN));
            Assert.assertEquals(2, Vect.boundedBinarySearchIndexT(addr, 1, 0, count - 1, SCAN_UP));

            // Generalize
            for (int i = 0; i < count / 3; i++) {
                int existingValue = i * 2;
                Assert.assertEquals(i * 3 + 2, Vect.boundedBinarySearchIndexT(addr, existingValue, 0, count - 1, SCAN_DOWN));
                Assert.assertEquals(i * 3, Vect.boundedBinarySearchIndexT(addr, existingValue, 0, count - 1, SCAN_UP));

                int nonExisting = i * 2 + 1;
                Assert.assertEquals(i * 3 + 2, Vect.boundedBinarySearchIndexT(addr, nonExisting, 0, count - 1, SCAN_DOWN));
                Assert.assertEquals(i * 3 + 2, Vect.boundedBinarySearchIndexT(addr, nonExisting, 0, count - 1, SCAN_UP));
            }
        } finally {
            Unsafe.free(addr, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDedupWithKey() {
        Rnd rnd = TestUtils.generateRandom(null);
        int indexLen = rnd.nextInt(100_000);

        int keyCount = 1 + rnd.nextInt(3);
        assert keyCount > 0 && keyCount < 5;

        ObjList<DirectLongList> keys = new ObjList<>();
        int keyMax = 1 + rnd.nextInt(4);

        try (DirectLongList index = new DirectLongList(indexLen * 2L, MemoryTag.NATIVE_DEFAULT)) {
            for (int i = 0; i < keyCount; i++) {
                keys.add(new DirectLongList(indexLen, MemoryTag.NATIVE_DEFAULT));
            }

            LongHashSet distinctKeys = new LongHashSet();
            LongList tsAndKey = new LongList();

            // Generate data
            int tsVal = 1000;
            for (int i = 0; i < indexLen; i++) {
                if (rnd.nextDouble() < 0.1) {
                    tsVal += rnd.nextInt(50);
                }
                index.add(tsVal);
                index.add(i);

                // Encode keys in 4 byte integer
                int combinedKey = 0;
                for (int k = 0; k < keyCount; k++) {
                    int keyVal = rnd.nextInt(keyMax);
                    keys.get(k).add(keyVal);
                    combinedKey = combinedKey << 8;
                    combinedKey += keyVal;
                }
                distinctKeys.add(Numbers.encodeLowHighInts(tsVal, combinedKey));
                tsAndKey.add(Numbers.encodeLowHighInts(combinedKey, tsVal));
            }

            // Squash keys from longs to ints
            for (int k = 0; k < keyCount; k++) {
                DirectLongList keyList = keys.get(k);
                for (int i = 0; i < indexLen / 2 + 1; i++) {
                    int high = 2 * i + 1 < indexLen ? (int) getIndexChecked(keyList, 2L * i + 1) : 0;
                    int low = 2 * i < indexLen ? (int) getIndexChecked(keyList, 2L * i) : 0;
                    keyList.set(i, Numbers.encodeLowHighInts(low, high));
                }
            }

            try (DedupColumnCommitAddresses colBuffs = new DedupColumnCommitAddresses()) {
                try (DirectLongList copy = new DirectLongList(indexLen * 2L, MemoryTag.NATIVE_DEFAULT)) {

                    colBuffs.setDedupColumnCount(keyCount);
                    long dedupColBuffPtr = colBuffs.allocateBlock();
                    for (int k = 0; k < keyCount; k++) {
                        colBuffs.setArrayValues(
                                dedupColBuffPtr,
                                k,
                                ColumnType.SYMBOL,
                                4,
                                0,
                                keys.get(k).getAddress(),
                                0L,
                                0L,
                                0L,
                                0L
                        );
                    }
                    copy.setPos(indexLen * 2L);

                    long dedupCount = Vect.dedupSortedTimestampIndexIntKeysChecked(
                            index.getAddress(),
                            indexLen,
                            index.getAddress(),
                            copy.getAddress(),
                            keyCount,
                            colBuffs.getAddress(dedupColBuffPtr)
                    );
                    if (distinctKeys.size() == indexLen && dedupCount == -2) {
                        // No duplicates detected and that's correct. Assert that index is not messed up.
                        dedupCount = indexLen;
                    } else {
                        Assert.assertEquals(distinctKeys.size(), dedupCount);
                    }

                    // assert indexes of the rows are distinct and in correct range
                    boolean[] indexFound = new boolean[(int) indexLen];
                    tsAndKey.sort();
                    int expectedValueIndex = -1;
                    long expectedValue = -1;

                    for (int i = 0; i < dedupCount; i++) {
                        expectedValueIndex = getNextDistinctIndex(tsAndKey, expectedValueIndex, expectedValue);
                        expectedValue = tsAndKey.get(expectedValueIndex);

                        long rowIndex = getIndexChecked(index, i * 2L + 1L);
                        Assert.assertTrue("row index not in expected range", rowIndex >= 0 && rowIndex < indexLen);
                        Assert.assertFalse("row index is not distinct", indexFound[(int) rowIndex]);
                        indexFound[(int) rowIndex] = true;

                        Assert.assertEquals(Numbers.decodeHighInt(expectedValue), getIndexChecked(index, i * 2L));
                        int expectedCombinedKey = Numbers.decodeLowInt(expectedValue);

                        int combinedKey = 0;
                        for (int k = 0; k < keyCount; k++) {
                            DirectLongList keyList = keys.get(k);
                            Assert.assertTrue("key index not in expected range", rowIndex >= 0 && rowIndex / 2L < keyList.size());
                            long keyLong = getIndexChecked(keyList, rowIndex / 2L);
                            int key = rowIndex % 2 == 0 ? Numbers.decodeLowInt(keyLong) : Numbers.decodeHighInt(keyLong);
                            Assert.assertTrue(key < 256);
                            combinedKey = combinedKey << 8;
                            combinedKey += key;
                        }

                        Assert.assertEquals(expectedCombinedKey, combinedKey);
                    }
                }
            }
        } finally {
            Misc.freeObjList(keys);
        }
    }

    @Test
    public void testMemeq() {
        int maxSize = 1024 * 1024;
        int[] sizes = {16, 1024, maxSize};
        long a = Unsafe.malloc(maxSize, MemoryTag.NATIVE_DEFAULT);
        long b = Unsafe.malloc(maxSize, MemoryTag.NATIVE_DEFAULT);

        try {
            for (int i = 0; i < maxSize; i += Integer.BYTES) {
                Unsafe.getUnsafe().putInt(a + i, i);
                Unsafe.getUnsafe().putInt(b + i, i);
            }

            for (int size : sizes) {
                Assert.assertTrue(Vect.memeq(a, b, size));
            }

            Unsafe.getUnsafe().putInt(b, -1);

            for (int size : sizes) {
                Assert.assertFalse(Vect.memeq(a, b, size));
            }
        } finally {
            Unsafe.free(a, maxSize, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(b, maxSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testMemmove() {
        int maxSize = 1024 * 1024;
        int[] sizes = {1024, 4096, maxSize};
        int buffSize = 1024 + 4096 + maxSize;
        long from = Unsafe.malloc(buffSize, MemoryTag.NATIVE_DEFAULT);
        long to = Unsafe.malloc(maxSize, MemoryTag.NATIVE_DEFAULT);

        try {
            // initialize from buffer
            // with 1, 4, 8, 12 ... integers
            for (int i = 0; i < buffSize; i += Integer.BYTES) {
                Unsafe.getUnsafe().putInt(from + i, i);
            }

            int offset = 0;
            for (int size : sizes) {
                // move next portion of from into to
                Vect.memmove(to, from + offset, size);

                for (int i = 0; i < size; i += Integer.BYTES) {
                    int actual = Unsafe.getUnsafe().getInt(to + i);
                    Assert.assertEquals(i + offset, actual);
                }

                offset += size;
            }
        } finally {
            Unsafe.free(from, buffSize, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(to, maxSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testMergeDedupIndex() {
        int srcLen = 10;
        try (DirectLongList src = new DirectLongList(srcLen, MemoryTag.NATIVE_DEFAULT)) {
            int indexLen = 10;
            try (DirectLongList index = new DirectLongList(indexLen * 2, MemoryTag.NATIVE_DEFAULT)) {
                try (DirectLongList dest = new DirectLongList((srcLen + indexLen) * 2, MemoryTag.NATIVE_DEFAULT)) {
                    src.setPos(srcLen);
                    for (int i = 0; i < srcLen; i++) {
                        src.set(i, (i + 1) * 10);
                    }
                    Assert.assertEquals("{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}", src.toString());

                    index.setPos(indexLen * 2);
                    for (int i = 0; i < indexLen * 2; i += 2) {
                        index.set(i, (i + 1) * 10L);
                        index.set(i + 1, i / 2);
                    }
                    Assert.assertEquals("{10, 0, 30, 1, 50, 2, 70, 3, 90, 4, 110, 5, 130, 6, 150, 7, 170, 8, 190, 9}", index.toString());

                    long mergedCount = Vect.mergeDedupTimestampWithLongIndexAsc(
                            src.getAddress(),
                            0,
                            srcLen - 1,
                            index.getAddress(),
                            0,
                            indexLen - 1,
                            dest.getAddress()
                    );
                    dest.setPos(mergedCount * 2);
                    Assert.assertEquals("10 0:i, 20 1:s, 30 1:i, 40 3:s, 50 2:i, 60 5:s, 70 3:i, 80 7:s, 90 4:i, 100 9:s, 110 5:i, 130 6:i, 150 7:i, 170 8:i, 190 9:i", printMergeIndex(dest));
                }
            }
        }
    }

    @Test
    public void testMergeDedupIndexEmptyIndex() {
        int srcLen = 10;
        try (DirectLongList src = new DirectLongList(srcLen, MemoryTag.NATIVE_DEFAULT)) {
            int indexLen = 0;
            try (DirectLongList index = new DirectLongList(indexLen * 2, MemoryTag.NATIVE_DEFAULT)) {
                try (DirectLongList dest = new DirectLongList((srcLen + indexLen) * 2, MemoryTag.NATIVE_DEFAULT)) {
                    src.setPos(srcLen);
                    for (int i = 0; i < srcLen; i++) {
                        src.set(i, (i + 1) * 10);
                    }
                    Assert.assertEquals("{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}", src.toString());

                    index.setPos(indexLen * 2);
                    Assert.assertEquals("{}", index.toString());

                    long mergedCount = Vect.mergeDedupTimestampWithLongIndexAsc(
                            src.getAddress(),
                            0,
                            srcLen - 1,
                            index.getAddress(),
                            0,
                            indexLen - 1,
                            dest.getAddress()
                    );
                    dest.setPos(mergedCount * 2);
                    Assert.assertEquals("10 0:s, 20 1:s, 30 2:s, 40 3:s, 50 4:s, 60 5:s, 70 6:s, 80 7:s, 90 8:s, 100 9:s", printMergeIndex(dest));
                }
            }
        }
    }

    @Test
    public void testMergeDedupIndexEmptySrc() {
        int srcLen = 10000;
        Rnd rnd = TestUtils.generateRandom(null);
        try (DirectLongList src = new DirectLongList(srcLen, MemoryTag.NATIVE_DEFAULT)) {
            int indexLen = 20000;
            try (DirectLongList index = new DirectLongList(indexLen * 2, MemoryTag.NATIVE_DEFAULT)) {
                try (DirectLongList dest = new DirectLongList((srcLen + indexLen) * 2, MemoryTag.NATIVE_DEFAULT)) {

                    long lastTs = 0;
                    for (int i = 0; i < srcLen; i++) {
                        lastTs += 1 + rnd.nextLong(1_000L);
                        src.add(lastTs);
                    }

                    lastTs = 1;
                    for (int i = 0; i < indexLen * 2; i += 2) {
                        while (src.binarySearch(lastTs, BinarySearch.SCAN_UP) >= 0) {
                            lastTs += rnd.nextLong(1_000L);
                        }
                        index.add(lastTs);
                        lastTs += 1 + rnd.nextLong(1_000L);
                        index.add(rnd.nextPositiveLong());
                    }

                    long mergedCount = Vect.mergeDedupTimestampWithLongIndexAsc(
                            src.getAddress(),
                            0,
                            srcLen - 1,
                            index.getAddress(),
                            0,
                            indexLen - 1,
                            dest.getAddress()
                    );
                    // Assert no dups found
                    Assert.assertEquals(srcLen + indexLen, mergedCount);
                    dest.setPos(mergedCount * 2);

                    int timestampIndexSize = (srcLen + indexLen) * Long.BYTES * 2;
                    long ptr = Unsafe.malloc(timestampIndexSize, MemoryTag.NATIVE_O3);
                    Vect.mergeTwoLongIndexesAsc(
                            src.getAddress(),
                            0,
                            srcLen,
                            index.getAddress(),
                            indexLen,
                            ptr
                    );

                    try {
                        // Assert memory equal
                        assertEqualLongs(
                                ptr,
                                dest.getAddress(),
                                timestampIndexSize / Long.BYTES
                        );
                    } finally {
                        Unsafe.free(ptr, timestampIndexSize, MemoryTag.NATIVE_O3);
                    }
                }
            }
        }
    }

    @Test
    public void testMergeDedupIndexRepeated() {
        int srcLen = 10;
        try (DirectLongList src = new DirectLongList(srcLen, MemoryTag.NATIVE_DEFAULT)) {
            int indexLen = 5;
            try (DirectLongList index = new DirectLongList(indexLen * 2, MemoryTag.NATIVE_DEFAULT)) {
                try (DirectLongList dest = new DirectLongList((srcLen + indexLen) * 2, MemoryTag.NATIVE_DEFAULT)) {
                    src.setPos(srcLen);
                    for (int i = 0; i < srcLen; i++) {
                        src.set(i, (i + 1) / 2 * 2 * 10);
                    }
                    Assert.assertEquals("{0, 20, 20, 40, 40, 60, 60, 80, 80, 100}", src.toString());

                    index.setPos(indexLen * 2);
                    for (int i = 0; i < indexLen * 2; i += 2) {
                        index.set(i, (4 + i / 2 * 2) * 10);
                        index.set(i + 1, i / 2);
                    }
                    Assert.assertEquals("{40, 0, 60, 1, 80, 2, 100, 3, 120, 4}", index.toString());

                    long mergedCount = Vect.mergeDedupTimestampWithLongIndexAsc(
                            src.getAddress(),
                            0,
                            srcLen - 1,
                            index.getAddress(),
                            0,
                            indexLen - 1,
                            dest.getAddress()
                    );
                    dest.setPos(mergedCount * 2);
                    Assert.assertEquals("0 0:s, 20 1:s, 20 2:s, 40 0:i, 40 0:i, 60 1:i, 60 1:i, 80 2:i, 80 2:i, 100 3:i, 120 4:i", printMergeIndex(dest));
                }
            }
        }
    }

    @Test
    public void testMergeDedupIndexWithKey() {
        try (DirectLongList src = new DirectLongList(100, MemoryTag.NATIVE_DEFAULT);
             DirectLongList srcDedupCol = new DirectLongList(100, MemoryTag.NATIVE_DEFAULT)) {
            try (DirectLongList index = new DirectLongList(100, MemoryTag.NATIVE_DEFAULT);
                 DirectLongList indexDedupCol = new DirectLongList(100, MemoryTag.NATIVE_DEFAULT)) {
                try (DirectLongList dest = new DirectLongList(100, MemoryTag.NATIVE_DEFAULT)) {
                    src.add(10);
                    src.add(20);
                    src.add(20);
                    src.add(30);
                    src.add(40);
                    src.add(40);
                    src.add(50);
                    srcDedupCol.add(Numbers.encodeLowHighInts(1, 0));
                    srcDedupCol.add(Numbers.encodeLowHighInts(0, 1));
                    srcDedupCol.add(Numbers.encodeLowHighInts(0, 1));
                    srcDedupCol.add(Numbers.encodeLowHighInts(0, 1));
                    Assert.assertEquals("10:1, 20:0, 20:0, 30:1, 40:0, 40:1, 50:0", printTsWithDedupKey(src, srcDedupCol));

                    index.add(10);
                    index.add(0);
                    index.add(20);
                    index.add(1);
                    index.add(30);
                    index.add(2);
                    index.add(40);
                    index.add(3);
                    indexDedupCol.add(Numbers.encodeLowHighInts(0, 0));
                    indexDedupCol.add(Numbers.encodeLowHighInts(0, 0));
                    Assert.assertEquals("10:0, 20:0, 30:0, 40:0", printTsIndexWithDedupKey(index, indexDedupCol));

                    try (DedupColumnCommitAddresses colBuffs = new DedupColumnCommitAddresses()) {
                        colBuffs.setDedupColumnCount(1);
                        long address = colBuffs.allocateBlock();
                        colBuffs.setArrayValues(
                                address,
                                0,
                                ColumnType.SYMBOL,
                                4,
                                0,
                                srcDedupCol.getAddress(),
                                indexDedupCol.getAddress(),
                                0L,
                                0L,
                                0L
                        );

                        dest.setPos(index.size() + src.size() * 2);
                        long mergedCount = Vect.mergeDedupTimestampWithLongIndexIntKeys(
                                src.getAddress(),
                                0,
                                src.size() - 1,
                                index.getAddress(),
                                0,
                                index.size() / 2 - 1,
                                dest.getAddress(),
                                1,
                                colBuffs.getAddress(address)
                        );
                        dest.setPos(mergedCount * 2);
                        Assert.assertEquals("10 0:s, 10 0:i, 20 1:i, 20 1:i, 30 3:s, 30 2:i, 40 3:i, 40 5:s, 50 6:s", printMergeIndex(dest));
                    }
                }
            }
        }
    }

    @Test
    public void testMergeFourSameSize() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int count = 1_000_000;
            long indexPtr1 = seedAndSort(count);
            long indexPtr2 = seedAndSort(count);
            long indexPtr3 = seedAndSort(count);
            long indexPtr4 = seedAndSort(count);

            long struct = Unsafe.malloc(Long.BYTES * 8, MemoryTag.NATIVE_DEFAULT);
            Unsafe.getUnsafe().putLong(struct, indexPtr1);
            Unsafe.getUnsafe().putLong(struct + Long.BYTES, count);

            Unsafe.getUnsafe().putLong(struct + 2 * Long.BYTES, indexPtr2);
            Unsafe.getUnsafe().putLong(struct + 3 * Long.BYTES, count);

            Unsafe.getUnsafe().putLong(struct + 4 * Long.BYTES, indexPtr3);
            Unsafe.getUnsafe().putLong(struct + 5 * Long.BYTES, count);

            Unsafe.getUnsafe().putLong(struct + 6 * Long.BYTES, indexPtr4);
            Unsafe.getUnsafe().putLong(struct + 7 * Long.BYTES, count);

            long targetSize = 4 * count * 2L * Long.BYTES;
            long targetAddr = Unsafe.malloc(targetSize, MemoryTag.NATIVE_DEFAULT);

            try {
                Vect.mergeLongIndexesAsc(struct, 4, targetAddr);
                assertIndexAsc(count * 4, targetAddr);
            } finally {
                Unsafe.free(indexPtr1, count * 2 * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(indexPtr2, count * 2 * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(indexPtr3, count * 2 * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(indexPtr4, count * 2 * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(struct, Long.BYTES * 8, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(targetAddr, targetSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testMergeOne() {
        long indexPtr = seedAndSort(150);
        try {
            assertIndexAsc(150, indexPtr);
        } finally {
            Unsafe.free(indexPtr, 150 * 2 * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
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

        long struct = Unsafe.malloc(Long.BYTES * 6, MemoryTag.NATIVE_DEFAULT);
        Unsafe.getUnsafe().putLong(struct, indexPtr1);
        Unsafe.getUnsafe().putLong(struct + Long.BYTES, count1);
        Unsafe.getUnsafe().putLong(struct + 2 * Long.BYTES, indexPtr2);
        Unsafe.getUnsafe().putLong(struct + 3 * Long.BYTES, count2);
        Unsafe.getUnsafe().putLong(struct + 4 * Long.BYTES, indexPtr3);
        Unsafe.getUnsafe().putLong(struct + 5 * Long.BYTES, count3);

        long targetSize = (count1 + count2 + count3) * 2L * Long.BYTES;
        long targetAddr = Unsafe.malloc(targetSize, MemoryTag.NATIVE_DEFAULT);

        try {
            Vect.mergeLongIndexesAsc(struct, 3, targetAddr);
            assertIndexAsc(count1 + count2 + count3, targetAddr);
        } finally {
            Unsafe.free(indexPtr1, count1 * 2 * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(indexPtr2, count2 * 2 * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(indexPtr3, count3 * 2 * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(struct, Long.BYTES * 6, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(targetAddr, targetSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testMergeTwoDifferentSizesAB() {
        final int count1 = 1_000_000;
        final int count2 = 500_000;
        long indexPtr1 = seedAndSort(count1);
        long indexPtr2 = seedAndSort(count2);

        long struct = Unsafe.malloc(Long.BYTES * 4, MemoryTag.NATIVE_DEFAULT);
        Unsafe.getUnsafe().putLong(struct, indexPtr1);
        Unsafe.getUnsafe().putLong(struct + Long.BYTES, count1);
        Unsafe.getUnsafe().putLong(struct + 2 * Long.BYTES, indexPtr2);
        Unsafe.getUnsafe().putLong(struct + 3 * Long.BYTES, count2);

        long targetSize = (count1 + count2) * 2L * Long.BYTES;
        long targetAddr = Unsafe.malloc(targetSize, MemoryTag.NATIVE_DEFAULT);

        try {
            Vect.mergeLongIndexesAsc(struct, 2, targetAddr);
            assertIndexAsc(count1 + count2, targetAddr);
        } finally {
            Unsafe.free(indexPtr1, count1 * 2 * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(indexPtr2, count2 * 2 * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(struct, Long.BYTES * 4, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(targetAddr, targetSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testMergeTwoDifferentSizesBA() {
        final int count1 = 1_000_000;
        final int count2 = 2_000_000;
        long indexPtr1 = seedAndSort(count1);
        long indexPtr2 = seedAndSort(count2);

        long struct = Unsafe.malloc(Long.BYTES * 4, MemoryTag.NATIVE_DEFAULT);
        Unsafe.getUnsafe().putLong(struct, indexPtr1);
        Unsafe.getUnsafe().putLong(struct + Long.BYTES, count1);
        Unsafe.getUnsafe().putLong(struct + 2 * Long.BYTES, indexPtr2);
        Unsafe.getUnsafe().putLong(struct + 3 * Long.BYTES, count2);

        long targetSize = (count1 + count2) * 2L * Long.BYTES;
        long targetAddr = Unsafe.malloc(targetSize, MemoryTag.NATIVE_DEFAULT);

        try {
            Vect.mergeLongIndexesAsc(struct, 2, targetAddr);
            assertIndexAsc(count1 + count2, targetAddr);
        } finally {
            Unsafe.free(indexPtr1, count1 * 2 * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(indexPtr2, count2 * 2 * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(struct, Long.BYTES * 4, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(targetAddr, targetSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testMergeTwoSameSize() {
        final int count = 1_000_000;
        long indexPtr1 = seedAndSort(count);
        long indexPtr2 = seedAndSort(count);

        long struct = Unsafe.malloc(Long.BYTES * 4, MemoryTag.NATIVE_DEFAULT);
        Unsafe.getUnsafe().putLong(struct, indexPtr1);
        Unsafe.getUnsafe().putLong(struct + Long.BYTES, count);
        Unsafe.getUnsafe().putLong(struct + 2 * Long.BYTES, indexPtr2);
        Unsafe.getUnsafe().putLong(struct + 3 * Long.BYTES, count);

        long targetSize = (2 * count) * 2L * Long.BYTES;
        long targetAddr = Unsafe.malloc(targetSize, MemoryTag.NATIVE_DEFAULT);

        try {
            Vect.mergeLongIndexesAsc(struct, 2, targetAddr);
            assertIndexAsc(count * 2, targetAddr);
        } finally {
            Unsafe.free(indexPtr1, count * 2 * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(indexPtr2, count * 2 * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(struct, Long.BYTES * 4, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(targetAddr, targetSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testMergeZero() {
        try {
            Vect.mergeLongIndexesAsc(0, 0, 0);
            Assert.fail();
        } catch (IllegalArgumentException e) {
            TestUtils.assertContains(e.getMessage(), "Count of indexes to merge should at least be 2.");
        }
    }

    @Test
    public void testQuickSort1M() {
        rnd = TestUtils.generateRandom(null);
        testQuickSort(1_000_000);
    }

    @Test
    public void testReshuffleInt64() {
        int[] sizes = new int[]{0, 1, 3, 4, 5, 1024 * 1024 + 2};
        int typeBytes = Long.BYTES;
        int maxSize = sizes[sizes.length - 1];
        long buffSize = maxSize * typeBytes;

        int indexBuffSize = maxSize * Long.BYTES * 2;
        long index = Unsafe.malloc(indexBuffSize, MemoryTag.NATIVE_DEFAULT);
        long dst = Unsafe.malloc(buffSize, MemoryTag.NATIVE_DEFAULT);
        long src = Unsafe.malloc(buffSize, MemoryTag.NATIVE_DEFAULT);

        for (int i = 0; i < maxSize; i++) {
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
            Unsafe.free(index, indexBuffSize, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(src, buffSize, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(dst, buffSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testSetMemoryVanillaDouble() {
        int[] sizes = new int[]{0, 1, 3, 4, 5, 7, 15, 20, 1024 * 1024, 1024 * 1024 + 1, 2_000_000, 10_000_000};
        double[] values = new double[]{-1, 0, 1, Double.MIN_VALUE, Double.MAX_VALUE, Double.NaN, 1.0023455};
        int typeBytes = Double.BYTES;
        long buffSize = sizes[sizes.length - 1] * typeBytes;
        long buffer = Unsafe.malloc(buffSize, MemoryTag.NATIVE_DEFAULT);

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
            Unsafe.free(buffer, buffSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testSetMemoryVanillaFloat() {
        int[] sizes = new int[]{0, 1, 3, 4, 5, 6, 7, 8, 10, 12, 15, 19, 1024 * 1024, 1024 * 1024 + 1, 2_000_000, 10_000_000};
        float[] values = new float[]{-1, 0, 1, Float.MIN_VALUE, Float.MAX_VALUE, Float.NaN, 1.0023455f};
        int typeBytes = Float.BYTES;
        long buffSize = sizes[sizes.length - 1] * typeBytes;
        long buffer = Unsafe.malloc(buffSize, MemoryTag.NATIVE_DEFAULT);

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
            Unsafe.free(buffer, buffSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testSetMemoryVanillaInt() {
        int[] sizes = new int[]{0, 1, 3, 4, 5, 6, 7, 8, 10, 12, 15, 19, 1024 * 1024, 1024 * 1024 + 1, 2_000_000, 10_000_000};
        int[] values = new int[]{-1, 0, 1, Integer.MIN_VALUE, Integer.MAX_VALUE, 0xabcd};
        int typeBytes = Integer.BYTES;
        long buffSize = sizes[sizes.length - 1] * typeBytes;
        long buffer = Unsafe.malloc(buffSize, MemoryTag.NATIVE_DEFAULT);

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
            Unsafe.free(buffer, buffSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testSetMemoryVanillaLong() {
        int[] sizes = new int[]{0, 1, 3, 4, 5, 7, 9, 15, 20, 1024 * 1024 - 1, 1024 * 1024, 1024 * 1024 + 1, 2_000_000, 10_000_000};
        long[] values = new long[]{-1, 0, 1, Long.MIN_VALUE, Long.MAX_VALUE, 0xabcd};
        long buffSize = sizes[sizes.length - 1] * Long.BYTES;
        long buffer = Unsafe.malloc(buffSize, MemoryTag.NATIVE_DEFAULT);

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
            Unsafe.free(buffer, buffSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testSetMemoryVanillaShort() {
        int[] sizes = new int[]{1, 3, 4, 5, 6, 7, 8, 10, 12, 15, 19, 1024 * 1024, 1024 * 1024 + 1, 2_000_000, 10_000_000};
        int[] offsetBytes = new int[]{4};
        int maxOffset = 16;

        short[] values = new short[]{-1, 0, 1, Short.MIN_VALUE, Short.MAX_VALUE, 0xabc};
        int typeBytes = Short.BYTES;
        long buffSize = sizes[sizes.length - 1] * typeBytes + maxOffset;
        long buffer = Unsafe.malloc(buffSize, MemoryTag.NATIVE_DEFAULT);

        try {
            for (int offset : offsetBytes) {
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
            Unsafe.free(buffer, buffSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testSort1M() {
        rnd = TestUtils.generateRandom(null);
        testSort(10_000_000);
    }

    @Test
    public void testSortAB10M() {
        rnd = TestUtils.generateRandom(null);
        int split = rnd.nextInt(10_000_000);
        testSortAB(10_000_000 - split, split);
    }

    @Test
    public void testSortAEmptyA() {
        testSortAB(0, 1_000);
    }

    @Test
    public void testSortAEmptyB() {
        testSortAB(1_000, 0);
    }

    @Test
    public void testSortFour() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            for (int i = 0; i < 100; i++) {
                testSort(4);
            }
        });
    }

    @Test
    public void testSortOne() {
        final long indexAddr = Unsafe.malloc(2 * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            seedMem2Longs(1, indexAddr);
            long expected = Unsafe.getUnsafe().getLong(indexAddr);
            Vect.sortLongIndexAscInPlace(indexAddr, 1);
            Assert.assertEquals(expected, Unsafe.getUnsafe().getLong(indexAddr));
        } finally {
            Unsafe.free(indexAddr, 2 * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static long getIndexChecked(DirectLongList keyList, long p) {
        Assert.assertTrue("key index not in expected range", p >= 0 && p < keyList.size());
        return keyList.get(p);
    }

    private static String printMergeIndex(DirectLongList dest) {
        StringSink sink = new StringSink();
        for (int i = 0; i < dest.size(); i += 2) {
            long bit_index = getIndexChecked(dest, i + 1);
            char bit = bit_index < 0 ? 's' : 'i';
            long index = bit_index & ~(1L << 63);
            if (i > 0) {
                sink.put(", ");
            }
            sink.put(getIndexChecked(dest, i)).put(' ').put(index).put(':').put(bit);
        }
        return sink.toString();
    }

    private void assertEqualLongs(long expected, long actual, int longCount) {
        for (int i = 0; i < longCount; i++) {
            if (Unsafe.getUnsafe().getLong(expected + i * 8L) != Unsafe.getUnsafe().getLong(actual + i * 8L)) {
                Assert.assertEquals("Longs at " + i + " are not equal", Unsafe.getUnsafe().getLong(expected + i * 8L), Unsafe.getUnsafe().getLong(actual + i * 8L));
            }
        }
    }

    private void assertIndexAsc(int count, long indexAddr) {
        long v = Unsafe.getUnsafe().getLong(indexAddr);
        for (int i = 1; i < count; i++) {
            long next = Unsafe.getUnsafe().getLong(indexAddr + i * 2L * Long.BYTES);
            Assert.assertTrue(next >= v);
            v = next;
        }
    }

    private void assertIndexAsc(int count, long indexAddr, long initialAddrA, long initialAddrB) {
        long v = Unsafe.getUnsafe().getLong(indexAddr);
        for (int i = 1; i < count; i++) {
            long ts = Unsafe.getUnsafe().getLong(indexAddr + i * 2L * Long.BYTES);
            long idx = Unsafe.getUnsafe().getLong(indexAddr + i * 2L * Long.BYTES + Long.BYTES);
            Assert.assertTrue(ts >= v);
            if (idx < 0) {
                Assert.assertEquals(ts, Unsafe.getUnsafe().getLong(initialAddrA + idx * Long.BYTES));
            } else {
                idx = (idx << 1) >> 1;
                Assert.assertEquals(ts, Unsafe.getUnsafe().getLong(initialAddrB + idx * 2L * Long.BYTES));
            }

            v = ts;
        }
    }

    private int getNextDistinctIndex(LongList sortedList, int nextIndex, long expectedValue) {
        //noinspection StatementWithEmptyBody
        while (sortedList.get(++nextIndex) == expectedValue) ;
        return nextIndex;
    }

    private String printTsIndexWithDedupKey(DirectLongList tsIndex, DirectLongList key) {
        StringSink sink = new StringSink();
        for (int i = 0; i < tsIndex.size() / 2; i++) {
            if (i > 0) {
                sink.put(", ");
            }

            long tsVal = getIndexChecked(tsIndex, i * 2L);
            long value = getIndexChecked(key, i / 2);
            int keyVal = i % 2 == 1 ? Numbers.decodeHighInt(value) : Numbers.decodeLowInt(value);
            sink.put(tsVal).put(':').put(keyVal);
        }
        return sink.toString();
    }

    private String printTsWithDedupKey(DirectLongList ts, DirectLongList key) {
        StringSink sink = new StringSink();
        for (int i = 0; i < ts.size(); i++) {
            if (i > 0) {
                sink.put(", ");
            }

            long tsVal = getIndexChecked(ts, i);
            long value = getIndexChecked(key, i / 2);
            int keyVal = i % 2 == 1 ? Numbers.decodeHighInt(value) : Numbers.decodeLowInt(value);
            sink.put(tsVal).put(':').put(keyVal);
        }
        return sink.toString();
    }

    private long seedAndSort(int count) {
        final long indexAddr = Unsafe.malloc(count * 2L * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        seedMem2Longs(count, indexAddr);
        Vect.sortLongIndexAscInPlace(indexAddr, count);
        return indexAddr;
    }

    private void seedMem1Long(int count, long p) {
        for (int i = 0; i < count; i++) {
            final long z = rnd.nextPositiveLong();
            Unsafe.getUnsafe().putLong(p + (long) i * Long.BYTES, z);
        }
    }

    private void seedMem2Longs(int count, long p) {
        for (int i = 0; i < count; i++) {
            final long z = rnd.nextPositiveLong();
            Unsafe.getUnsafe().putLong(p + i * 2L * Long.BYTES, z);
            Unsafe.getUnsafe().putLong(p + i * 2L * Long.BYTES + 8, i);
        }
    }

    private void testQuickSort(int count) {
        final int size = count * 2 * Long.BYTES;
        final long indexAddr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            seedMem2Longs(count, indexAddr);
            Vect.quickSortLongIndexAscInPlace(indexAddr, count);
            assertIndexAsc(count, indexAddr);
        } finally {
            Unsafe.free(indexAddr, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void testSort(int count) {
        final int size = count * 2 * Long.BYTES;
        final long indexAddr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            seedMem2Longs(count, indexAddr);
            Vect.sortLongIndexAscInPlace(indexAddr, count);
            assertIndexAsc(count, indexAddr);
        } finally {
            Unsafe.free(indexAddr, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void testSortAB(int aCount, int bCount) {
        final int sizeA = aCount * 2 * Long.BYTES;
        final int sizeB = bCount * 2 * Long.BYTES;

        final int resultSize = sizeA + sizeB;
        final long aAddr = Unsafe.malloc(resultSize, MemoryTag.NATIVE_DEFAULT);
        final long bAddr = Unsafe.malloc(sizeB, MemoryTag.NATIVE_DEFAULT);
        final long cpyAddr = Unsafe.malloc(resultSize, MemoryTag.NATIVE_DEFAULT);

        try {
            seedMem1Long(aCount, aAddr);
            seedMem2Longs(bCount, bAddr);

            final long aAddrCopy = Unsafe.malloc(sizeA, MemoryTag.NATIVE_DEFAULT);
            final long bAddrCopy = Unsafe.malloc(sizeB, MemoryTag.NATIVE_DEFAULT);
            Vect.memcpy(aAddrCopy, aAddr, sizeA);
            Vect.memcpy(bAddrCopy, bAddr, sizeB);

            Vect.radixSortABLongIndexAsc(aAddr, aCount, bAddr, bCount, aAddr, cpyAddr);
            assertIndexAsc(aCount + bCount, aAddr, aAddrCopy, bAddrCopy);
        } finally {
            Unsafe.free(aAddr, resultSize, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(bAddr, sizeB, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(cpyAddr, resultSize, MemoryTag.NATIVE_DEFAULT);
        }
    }


    static {
        Os.init();
    }
}
