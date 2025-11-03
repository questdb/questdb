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

package io.questdb.test.std;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DedupColumnCommitAddresses;
import io.questdb.cairo.TableWriterSegmentCopyInfo;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.vm.MemoryCMARWImpl;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.network.Net;
import io.questdb.std.Chars;
import io.questdb.std.DirectLongList;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongHashSet;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static io.questdb.std.Vect.BIN_SEARCH_SCAN_DOWN;
import static io.questdb.std.Vect.BIN_SEARCH_SCAN_UP;

public class VectFuzzTest {

    @ClassRule
    public static final TemporaryFolder temp = new TemporaryFolder();
    private Rnd rnd = new Rnd();

    @Before
    public void setUp() {
        rnd.reset();
    }

    @Test
    public void testAggregateBoundary() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            abstract class TestCase {
                final long sizeBytes;

                TestCase(long sizeBytes) {
                    this.sizeBytes = sizeBytes;
                }

                abstract void run(long ptr, long count);
            }

            final int nMax = 1024;
            final TestCase[] testCases = new TestCase[]{
                    new TestCase(Short.BYTES) {
                        @Override
                        void run(long ptr, long count) {
                            if (count == 0) {
                                Assert.assertEquals("short sum, count: 0", Numbers.LONG_NULL, Vect.sumShort(ptr, 0));
                                Assert.assertEquals("short min, count: 0", Numbers.INT_NULL, Vect.minShort(ptr, 0));
                                Assert.assertEquals("short max, count: 0", Numbers.INT_NULL, Vect.maxShort(ptr, 0));
                            } else {
                                Assert.assertEquals("short sum, count: " + count, 0, Vect.sumShort(ptr, count));
                                Assert.assertEquals("short min, count: " + count, 0, Vect.minShort(ptr, count));
                                Assert.assertEquals("short max, count: " + count, 0, Vect.maxShort(ptr, count));
                            }
                        }
                    },
                    new TestCase(Integer.BYTES) {
                        @Override
                        void run(long ptr, long count) {
                            if (count == 0) {
                                Assert.assertEquals("int sum, count: 0", Numbers.LONG_NULL, Vect.sumInt(ptr, 0));
                                Assert.assertEquals("int min, count: 0", Numbers.INT_NULL, Vect.minInt(ptr, 0));
                                Assert.assertEquals("int max, count: 0", Numbers.INT_NULL, Vect.maxInt(ptr, 0));
                                Assert.assertEquals("int count, count: 0", 0, Vect.countInt(ptr, 0));
                            } else {
                                Assert.assertEquals("int sum, count: " + count, 0, Vect.sumInt(ptr, count));
                                Assert.assertEquals("int min, count: " + count, 0, Vect.minInt(ptr, count));
                                Assert.assertEquals("int max, count: " + count, 0, Vect.maxInt(ptr, count));
                                Assert.assertEquals("int count, count: " + count, count, Vect.countInt(ptr, count));
                            }
                        }
                    },
                    new TestCase(Long.BYTES) {
                        @Override
                        void run(long ptr, long count) {
                            if (count == 0) {
                                Assert.assertEquals("long sum, count: 0", Numbers.LONG_NULL, Vect.sumLong(ptr, 0));
                                Assert.assertEquals("long min, count: 0", Numbers.LONG_NULL, Vect.minLong(ptr, 0));
                                Assert.assertEquals("long max, count: 0", Numbers.LONG_NULL, Vect.maxLong(ptr, 0));
                                Assert.assertEquals("long count, count: 0", 0, Vect.countLong(ptr, 0));
                            } else {
                                Assert.assertEquals("long sum, count: " + count, 0, Vect.sumLong(ptr, count));
                                Assert.assertEquals("long min, count: " + count, 0, Vect.minLong(ptr, count));
                                Assert.assertEquals("long max, count: " + count, 0, Vect.maxLong(ptr, count));
                                Assert.assertEquals("long count, count: " + count, count, Vect.countLong(ptr, count));
                            }
                        }
                    },
                    new TestCase(Double.BYTES) {
                        @Override
                        void run(long ptr, long count) {
                            if (count == 0) {
                                Assert.assertTrue("double sum, count: 0", Double.isNaN(Vect.sumDouble(ptr, 0)));
                                Assert.assertTrue("double Kahan sum, count: 0", Double.isNaN(Vect.sumDoubleKahan(ptr, 0)));
                                Assert.assertTrue("double Neumaier sum, count: 0", Double.isNaN(Vect.sumDoubleNeumaier(ptr, 0)));
                                Assert.assertTrue("double min, count: 0", Double.isNaN(Vect.minDouble(ptr, 0)));
                                Assert.assertTrue("double max, count: 0", Double.isNaN(Vect.maxDouble(ptr, 0)));
                                Assert.assertEquals("double count, count: 0", 0, Vect.countDouble(ptr, 0));
                            } else {
                                Assert.assertEquals("double sum, count: " + count, 0.0, Vect.sumDouble(ptr, count), 0.001);
                                Assert.assertEquals("double Kahan sum, count: " + count, 0.0, Vect.sumDoubleKahan(ptr, count), 0.001);
                                Assert.assertEquals("double Neumaier sum, count: " + count, 0.0, Vect.sumDoubleNeumaier(ptr, count), 0.001);
                                Assert.assertEquals("double min, count: " + count, 0.0, Vect.minDouble(ptr, count), 0.001);
                                Assert.assertEquals("double max, count: " + count, 0.0, Vect.maxDouble(ptr, count), 0.001);
                                Assert.assertEquals("double count, count: " + count, count, Vect.countDouble(ptr, count));
                            }
                        }
                    }
            };

            for (TestCase testCase : testCases) {
                for (int i = 0; i < nMax; i++) {
                    final long size = i * testCase.sizeBytes;
                    final long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
                    Vect.memset(ptr, size, 0);
                    try {
                        testCase.run(ptr, i);
                    } finally {
                        Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
                    }
                }
            }
        });
    }

    @Test
    public void testBinarySearchIndexT() throws Exception {
        TestUtils.assertMemoryLeak(() -> {

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
                Assert.assertEquals(2, Vect.binarySearchIndexT(addr, 0, 0, count - 1, BIN_SEARCH_SCAN_DOWN));
                Assert.assertEquals(0, Vect.binarySearchIndexT(addr, 0, 0, count - 1, BIN_SEARCH_SCAN_UP));

                // Non-existing
                Assert.assertEquals(3, -Vect.binarySearchIndexT(addr, 1, 0, count - 1, BIN_SEARCH_SCAN_DOWN) - 1);
                Assert.assertEquals(3, -Vect.binarySearchIndexT(addr, 1, 0, count - 1, BIN_SEARCH_SCAN_UP) - 1);

                // Generalize
                for (int i = 0; i < count / 3; i++) {
                    int existingValue = i * 2;
                    Assert.assertEquals(i * 3 + 2, Vect.binarySearchIndexT(addr, existingValue, 0, count - 1, BIN_SEARCH_SCAN_DOWN));
                    Assert.assertEquals(i * 3, Vect.binarySearchIndexT(addr, existingValue, 0, count - 1, BIN_SEARCH_SCAN_UP));

                    int nonExisting = i * 2 + 1;
                    Assert.assertEquals(i * 3 + 3, -Vect.binarySearchIndexT(addr, nonExisting, 0, count - 1, BIN_SEARCH_SCAN_DOWN) - 1);
                    Assert.assertEquals(i * 3 + 3, -Vect.binarySearchIndexT(addr, nonExisting, 0, count - 1, BIN_SEARCH_SCAN_UP) - 1);
                }
            } finally {
                Unsafe.free(addr, size, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testBoundedBinarySearchIndexT() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
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
                Assert.assertEquals(2, Vect.boundedBinarySearchIndexT(addr, 0, 0, count - 1, BIN_SEARCH_SCAN_DOWN));
                Assert.assertEquals(0, Vect.boundedBinarySearchIndexT(addr, 0, 0, count - 1, BIN_SEARCH_SCAN_UP));

                // Non-existing
                Assert.assertEquals(2, Vect.boundedBinarySearchIndexT(addr, 1, 0, count - 1, BIN_SEARCH_SCAN_DOWN));
                Assert.assertEquals(2, Vect.boundedBinarySearchIndexT(addr, 1, 0, count - 1, BIN_SEARCH_SCAN_UP));

                // Generalize
                for (int i = 0; i < count / 3; i++) {
                    int existingValue = i * 2;
                    Assert.assertEquals(i * 3 + 2, Vect.boundedBinarySearchIndexT(addr, existingValue, 0, count - 1, BIN_SEARCH_SCAN_DOWN));
                    Assert.assertEquals(i * 3, Vect.boundedBinarySearchIndexT(addr, existingValue, 0, count - 1, BIN_SEARCH_SCAN_UP));

                    int nonExisting = i * 2 + 1;
                    Assert.assertEquals(i * 3 + 2, Vect.boundedBinarySearchIndexT(addr, nonExisting, 0, count - 1, BIN_SEARCH_SCAN_DOWN));
                    Assert.assertEquals(i * 3 + 2, Vect.boundedBinarySearchIndexT(addr, nonExisting, 0, count - 1, BIN_SEARCH_SCAN_UP));
                }
            } finally {
                Unsafe.free(addr, size, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testDedupWithKey() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
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
                            long addr = DedupColumnCommitAddresses.setColValues(
                                    dedupColBuffPtr,
                                    k,
                                    ColumnType.SYMBOL,
                                    4,
                                    0
                            );

                            DedupColumnCommitAddresses.setColAddressValues(
                                    addr,
                                    keys.get(k).getAddress(),
                                    0L,
                                    0L
                            );

                            DedupColumnCommitAddresses.setO3DataAddressValues(
                                    addr,
                                    0L,
                                    0L,
                                    0L
                            );

                            DedupColumnCommitAddresses.setReservedValuesSet2(
                                    addr,
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
                                DedupColumnCommitAddresses.getAddress(dedupColBuffPtr)
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
                                Assert.assertTrue("key index not in expected range", rowIndex / 2L < keyList.size());
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
        });
    }

    @Test
    public void testMemeq() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
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
        });
    }

    @Test
    public void testMemmove() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
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
        });
    }

    @Test
    public void testMergeDedupIndex() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int srcLen = 10;
            try (DirectLongList src = new DirectLongList(srcLen, MemoryTag.NATIVE_DEFAULT)) {
                int indexLen = 10;
                try (DirectLongList index = new DirectLongList(indexLen * 2, MemoryTag.NATIVE_DEFAULT)) {
                    try (DirectLongList dest = new DirectLongList((srcLen + indexLen) * 2, MemoryTag.NATIVE_DEFAULT)) {
                        src.setPos(srcLen);
                        for (int i = 0; i < srcLen; i++) {
                            src.set(i, (i + 1) * 10);
                        }
                        Assert.assertEquals("[10, 20, 30, 40, 50, 60, 70, 80, 90, 100]", src.toString());

                        index.setPos(indexLen * 2);
                        for (int i = 0; i < indexLen * 2; i += 2) {
                            index.set(i, (i + 1) * 10L);
                            index.set(i + 1, i / 2);
                        }
                        Assert.assertEquals("[10, 0, 30, 1, 50, 2, 70, 3, 90, 4, 110, 5, 130, 6, 150, 7, 170, 8, 190, 9]", index.toString());

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
        });
    }

    @Test
    public void testMergeDedupIndexEmptyIndex() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int srcLen = 10;
            try (DirectLongList src = new DirectLongList(srcLen, MemoryTag.NATIVE_DEFAULT)) {
                int indexLen = 0;
                try (DirectLongList index = new DirectLongList(0, MemoryTag.NATIVE_DEFAULT)) {
                    try (DirectLongList dest = new DirectLongList((srcLen + indexLen) * 2, MemoryTag.NATIVE_DEFAULT)) {
                        src.setPos(srcLen);
                        for (int i = 0; i < srcLen; i++) {
                            src.set(i, (i + 1) * 10);
                        }
                        Assert.assertEquals("[10, 20, 30, 40, 50, 60, 70, 80, 90, 100]", src.toString());

                        index.setPos(0);
                        Assert.assertEquals("[]", index.toString());

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
        });
    }

    @Test
    public void testMergeDedupIndexEmptySrc() throws Exception {
        TestUtils.assertMemoryLeak(() -> {

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
                            while (src.binarySearch(lastTs, BIN_SEARCH_SCAN_UP) >= 0) {
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
        });
    }

    @Test
    public void testMergeDedupIndexRepeated() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int srcLen = 10;
            try (DirectLongList src = new DirectLongList(srcLen, MemoryTag.NATIVE_DEFAULT)) {
                int indexLen = 5;
                try (DirectLongList index = new DirectLongList(indexLen * 2, MemoryTag.NATIVE_DEFAULT)) {
                    try (DirectLongList dest = new DirectLongList((srcLen + indexLen) * 2, MemoryTag.NATIVE_DEFAULT)) {
                        src.setPos(srcLen);
                        for (int i = 0; i < srcLen; i++) {
                            src.set(i, (i + 1) / 2 * 2 * 10);
                        }
                        Assert.assertEquals("[0, 20, 20, 40, 40, 60, 60, 80, 80, 100]", src.toString());

                        index.setPos(indexLen * 2);
                        for (int i = 0; i < indexLen * 2; i += 2) {
                            index.set(i, (4 + i / 2 * 2) * 10);
                            index.set(i + 1, i / 2);
                        }
                        Assert.assertEquals("[40, 0, 60, 1, 80, 2, 100, 3, 120, 4]", index.toString());

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
        });
    }

    @Test
    public void testMergeDedupIndexWithKey() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
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

                            long addr = DedupColumnCommitAddresses.setColValues(
                                    address,
                                    0,
                                    ColumnType.SYMBOL,
                                    4,
                                    0
                            );

                            DedupColumnCommitAddresses.setColAddressValues(addr, srcDedupCol.getAddress());
                            DedupColumnCommitAddresses.setO3DataAddressValues(addr, indexDedupCol.getAddress());

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
                                    DedupColumnCommitAddresses.getAddress(address)
                            );
                            dest.setPos(mergedCount * 2);
                            Assert.assertEquals("10 0:s, 10 0:i, 20 1:i, 20 1:i, 30 3:s, 30 2:i, 40 3:i, 40 5:s, 50 6:s", printMergeIndex(dest));
                        }
                    }
                }
            }
        });
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
    public void testMergeOne() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long indexPtr = seedAndSort(150);
            try {
                assertIndexAsc(150, indexPtr);
            } finally {
                Unsafe.free(indexPtr, 150 * 2 * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testMergeThreeDifferentSizes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
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
        });
    }

    @Test
    public void testMergeTwoDifferentSizesAB() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
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
        });
    }

    @Test
    public void testMergeTwoDifferentSizesBA() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
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
        });
    }

    @Test
    public void testMergeTwoSameSize() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
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
        });
    }

    @Test
    public void testMergeZero() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                Vect.mergeLongIndexesAsc(0, 0, 0);
                Assert.fail();
            } catch (IllegalArgumentException e) {
                TestUtils.assertContains(e.getMessage(), "Count of indexes to merge should at least be 2.");
            }
        });
    }

    @Test
    public void testOooMergeCopyStrColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int rowCount = 10_000;
            FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            long pageSize = Files.PAGE_SIZE;
            try (Path dataPathA = new Path().of(temp.newFile().getAbsolutePath());
                 Path auxPathA = new Path().of(temp.newFile().getAbsolutePath());
                 Path dataPathB = new Path().of(temp.newFile().getAbsolutePath());
                 Path auxPathB = new Path().of(temp.newFile().getAbsolutePath());
                 DirectLongList index = new DirectLongList(rowCount * 4, MemoryTag.NATIVE_DEFAULT);
                 MemoryCMARW dataMemA = new MemoryCMARWImpl(ff, dataPathA.$(), pageSize, -1, MemoryTag.NATIVE_DEFAULT, CairoConfiguration.O_NONE);
                 MemoryCMARW auxMemA = new MemoryCMARWImpl(ff, auxPathA.$(), pageSize, -1, MemoryTag.NATIVE_DEFAULT, CairoConfiguration.O_NONE);
                 MemoryCMARW dataMemB = new MemoryCMARWImpl(ff, dataPathB.$(), pageSize, -1, MemoryTag.NATIVE_DEFAULT, CairoConfiguration.O_NONE);
                 MemoryCMARW auxMemB = new MemoryCMARWImpl(ff, auxPathB.$(), pageSize, -1, MemoryTag.NATIVE_DEFAULT, CairoConfiguration.O_NONE)) {
                auxMemA.putLong(0);
                auxMemB.putLong(0);

                StringSink sink = new StringSink();
                int maxLen = 50; // exclusive
                int nullModA = 17;
                int nullModB = 23;
                for (int i = 0; i < rowCount; i++) {
                    int len = i % maxLen;

                    if (i % nullModA == 0) {
                        auxMemA.putLong(dataMemA.putStr(null));
                    } else {
                        sink.clear();
                        sink.repeat("a", len);
                        auxMemA.putLong(dataMemA.putStr(sink));
                    }

                    if (i % nullModB == 0) {
                        auxMemB.putLong(dataMemB.putStr(null));
                    } else {
                        sink.clear();
                        sink.repeat("b", len);
                        auxMemB.putLong(dataMemB.putStr(sink));
                    }
                    index.add(i * 2); // rowA synthetic timestamp
                    index.add(i); // rowA index

                    index.add(i * 2 + 1); // rowB synthetic timestamp
                    index.add(i | 1L << 63); // rowB index
                }

                String[] strings = strColAsStringArray(dataMemA, auxMemA, rowCount);
                for (int i = 0; i < rowCount; i++) {
                    if (i % nullModA == 0) {
                        Assert.assertNull(strings[i]);
                    } else {
                        sink.clear();
                        sink.repeat("a", i % maxLen);
                        TestUtils.assertEquals(sink, strings[i]);
                    }
                }

                strings = strColAsStringArray(dataMemB, auxMemB, rowCount);
                for (int i = 0; i < rowCount; i++) {
                    if (i % nullModB == 0) {
                        Assert.assertNull(strings[i]);
                    } else {
                        sink.clear();
                        sink.repeat("b", i % maxLen);
                        TestUtils.assertEquals(sink, strings[i]);
                    }
                }

                try (Path dataPathDest = new Path().of(temp.newFile().getAbsolutePath());
                     Path auxPathDest = new Path().of(temp.newFile().getAbsolutePath());
                     MemoryCMARW dataMemDest = new MemoryCMARWImpl(ff, dataPathDest.$(), pageSize, -1, MemoryTag.NATIVE_DEFAULT, CairoConfiguration.O_NONE);
                     MemoryCMARW auxMemDest = new MemoryCMARWImpl(ff, auxPathDest.$(), pageSize, -1, MemoryTag.NATIVE_DEFAULT, CairoConfiguration.O_NONE)) {

                    auxMemDest.extend(2 * rowCount * 8L + 8L);
                    dataMemDest.extend(dataMemA.getAppendOffset() + dataMemB.getAppendOffset());

                    Vect.oooMergeCopyStrColumn(index.getAddress(), 2 * rowCount,
                            auxMemA.addressOf(0), dataMemA.addressOf(0),
                            auxMemB.addressOf(0), dataMemB.addressOf(0),
                            auxMemDest.addressOf(0), dataMemDest.addressOf(0),
                            0);


                    strings = strColAsStringArray(dataMemDest, auxMemDest, rowCount * 2);
                    for (int i = 0; i < rowCount; i++) {
                        if (i % nullModB == 0) {
                            Assert.assertNull(strings[i * 2]);
                        } else {
                            sink.clear();
                            sink.repeat("b", i % maxLen);
                            TestUtils.assertEquals(sink, strings[i * 2]);
                        }

                        if (i % nullModA == 0) {
                            Assert.assertNull(strings[i * 2 + 1]);
                        } else {
                            sink.clear();
                            sink.repeat("a", i % maxLen);
                            TestUtils.assertEquals(sink, strings[i * 2 + 1]);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testOooMergeCopyVarcharColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {

            // todo: shuffle vectors
            int rowCount = 10_000;
            FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            long pageSize = Files.PAGE_SIZE;
            try (Path dataPathA = new Path().of(temp.newFile().getAbsolutePath());
                 Path auxPathA = new Path().of(temp.newFile().getAbsolutePath());
                 Path dataPathB = new Path().of(temp.newFile().getAbsolutePath());
                 Path auxPathB = new Path().of(temp.newFile().getAbsolutePath());
                 DirectLongList index = new DirectLongList(rowCount * 4, MemoryTag.NATIVE_DEFAULT);
                 MemoryCMARW dataMemA = new MemoryCMARWImpl(ff, dataPathA.$(), pageSize, -1, MemoryTag.NATIVE_DEFAULT, CairoConfiguration.O_NONE);
                 MemoryCMARW auxMemA = new MemoryCMARWImpl(ff, auxPathA.$(), pageSize, -1, MemoryTag.NATIVE_DEFAULT, CairoConfiguration.O_NONE);
                 MemoryCMARW dataMemB = new MemoryCMARWImpl(ff, dataPathB.$(), pageSize, -1, MemoryTag.NATIVE_DEFAULT, CairoConfiguration.O_NONE);
                 MemoryCMARW auxMemB = new MemoryCMARWImpl(ff, auxPathB.$(), pageSize, -1, MemoryTag.NATIVE_DEFAULT, CairoConfiguration.O_NONE)) {

                StringSink sink = new StringSink();
                Utf8StringSink utf8Sink = new Utf8StringSink();
                int maxLen = 50; // exclusive
                int nullModA = 17;
                int nullModB = 23;
                for (int i = 0; i < rowCount; i++) {
                    int len = i % maxLen;

                    if (i % nullModA == 0) {
                        VarcharTypeDriver.appendValue(auxMemA, dataMemA, null);
                    } else {
                        utf8Sink.clear();
                        utf8Sink.repeat('a', len);
                        VarcharTypeDriver.appendValue(auxMemA, dataMemA, utf8Sink);
                    }

                    if (i % nullModB == 0) {
                        VarcharTypeDriver.appendValue(auxMemB, dataMemB, null);
                    } else {
                        utf8Sink.clear();
                        utf8Sink.repeat('b', len);
                        VarcharTypeDriver.appendValue(auxMemB, dataMemB, utf8Sink);
                    }
                    index.add(i * 2); // rowA synthetic timestamp
                    index.add(i); // rowA index

                    index.add(i * 2 + 1); // rowB synthetic timestamp
                    index.add(i | 1L << 63); // rowB index
                }

                String[] strings = varcharColAsStringArray(dataMemA, auxMemA, rowCount);
                for (int i = 0; i < rowCount; i++) {
                    if (i % nullModA == 0) {
                        Assert.assertNull(strings[i]);
                    } else {
                        sink.clear();
                        sink.repeat("a", i % maxLen);
                        TestUtils.assertEquals(sink, strings[i]);
                    }
                }

                strings = varcharColAsStringArray(dataMemB, auxMemB, rowCount);
                for (int i = 0; i < rowCount; i++) {
                    if (i % nullModB == 0) {
                        Assert.assertNull(strings[i]);
                    } else {
                        sink.clear();
                        sink.repeat("b", i % maxLen);
                        TestUtils.assertEquals(sink, strings[i]);
                    }
                }

                try (Path dataPathDest = new Path().of(temp.newFile().getAbsolutePath());
                     Path auxPathDest = new Path().of(temp.newFile().getAbsolutePath());
                     MemoryCMARW dataMemDest = new MemoryCMARWImpl(ff, dataPathDest.$(), pageSize, -1, MemoryTag.NATIVE_DEFAULT, CairoConfiguration.O_NONE);
                     MemoryCMARW auxMemDest = new MemoryCMARWImpl(ff, auxPathDest.$(), pageSize, -1, MemoryTag.NATIVE_DEFAULT, CairoConfiguration.O_NONE)) {

                    auxMemDest.extend(2 * rowCount * 16L);
                    dataMemDest.extend(dataMemA.getAppendOffset() + dataMemB.getAppendOffset());

                    Vect.oooMergeCopyVarcharColumn(index.getAddress(), 2 * rowCount,
                            auxMemA.addressOf(0), dataMemA.addressOf(0),
                            auxMemB.addressOf(0), dataMemB.addressOf(0),
                            auxMemDest.addressOf(0), dataMemDest.addressOf(0),
                            0);

                    strings = varcharColAsStringArray(dataMemDest, auxMemDest, rowCount * 2);
                    for (int i = 0; i < rowCount; i++) {
                        if (i % nullModB == 0) {
                            Assert.assertNull(strings[i * 2]);
                        } else {
                            sink.clear();
                            sink.repeat("b", i % maxLen);
                            TestUtils.assertEquals(sink, strings[i * 2]);
                        }

                        if (i % nullModA == 0) {
                            Assert.assertNull(strings[i * 2 + 1]);
                        } else {
                            sink.clear();
                            sink.repeat("a", i % maxLen);
                            TestUtils.assertEquals(sink, strings[i * 2 + 1]);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testQuickSort1M() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            rnd = TestUtils.generateRandom(null);
            testQuickSort(1 + rnd.nextInt(1_000_000));
        });
    }

    @Test
    public void testReshuffleInt64() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
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
        });
    }

    @Test
    public void testSetMemoryVanillaDouble() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
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
        });
    }

    @Test
    public void testSetMemoryVanillaFloat() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
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
        });
    }

    @Test
    public void testSetMemoryVanillaInt() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
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
        });
    }

    @Test
    public void testSetMemoryVanillaLong() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
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
        });
    }

    @Test
    public void testSetMemoryVanillaShort() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
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
        });
    }

    @Test
    public void testSort1M() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            rnd = TestUtils.generateRandom(null);
            testSort(10_000_000);
        });
    }

    @Test
    public void testSort1Segment1Commit() throws Exception {
        Rnd rnd = TestUtils.generateRandom(null);
        TestUtils.assertMemoryLeak(() -> {
            int segmentCount = 1;
            int rowsPerCommit = 1 + rnd.nextInt(10000);
            int commits = 1;
            long increment = Math.min((long) (Micros.DAY_MICROS * rnd.nextDouble()), (1L << 50) / rowsPerCommit / segmentCount);

            testSortManySegments(
                    rnd.nextLong(123124512354523L),
                    increment,
                    rowsPerCommit,
                    commits,
                    segmentCount,
                    false,
                    0
            );
        });
    }

    @Test
    public void testSort1Segment1CommitSameTimestamp() throws Exception {
        Rnd rnd = TestUtils.generateRandom(null);
        TestUtils.assertMemoryLeak(() -> {
            int segmentCount = 1;
            int rowsPerCommit = 1 + rnd.nextInt(1000);
            int commits = 1;
            long increment = 0;

            testSortManySegments(
                    rnd.nextLong(123124512354523L),
                    increment,
                    rowsPerCommit,
                    commits,
                    segmentCount,
                    false,
                    -1
            );
        });
    }

    @Test
    public void testSortAB10M() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            rnd = TestUtils.generateRandom(null);
            int split = rnd.nextInt(10_000_000);
            testSortAB(
                    10_000_000 - split,
                    split,
                    -rnd.nextLong(Micros.DAY_MICROS * 365 * 10),
                    rnd.nextLong(Micros.DAY_MICROS * 365 * 10),
                    0
            );
        });
    }

    @Test
    public void testSortABEquals() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            rnd = TestUtils.generateRandom(null);
            int split = rnd.nextInt(100);
            long min = rnd.nextLong(Micros.DAY_MICROS * 365 * 10);
            testSortAB(
                    100 - split,
                    split,
                    min,
                    // Set min == max, e.g. all timestamp to be equal
                    min,
                    0
            );
        });
    }

    @Test
    public void testSortABMinGreaterThanMax() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            rnd = TestUtils.generateRandom(null);
            int split = 10;
            testSortAB(
                    50 - split,
                    split,
                    101,
                    100,
                    -1
            );
        });
    }

    @Test
    public void testSortABMinMaxDifferenceOverflow() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            rnd = TestUtils.generateRandom(null);
            int split = 10;
            testSortAB(
                    50 - split,
                    split,
                    -100,
                    Long.MAX_VALUE - 80,
                    -2
            );
        });
    }

    @Test
    public void testSortABSmallDifferenceEqual() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            rnd = TestUtils.generateRandom(null);
            int split = rnd.nextInt(100);
            long min = rnd.nextLong(Micros.DAY_MICROS * 365 * 10);
            long max = min + rnd.nextInt(260);
            testSortAB(
                    100 - split,
                    split,
                    min,
                    max,
                    0
            );
        });
    }

    @Test
    public void testSortAEmptyA() throws Exception {
        TestUtils.assertMemoryLeak(() ->
                testSortAB(
                        10_000,
                        0,
                        -rnd.nextLong(Micros.DAY_MICROS * 365 * 10),
                        rnd.nextLong(Micros.DAY_MICROS * 365 * 10),
                        0
                )
        );
    }

    @Test
    public void testSortAEmptyB() throws Exception {
        TestUtils.assertMemoryLeak(() ->
                testSortAB(
                        1_000,
                        0,
                        -rnd.nextLong(Micros.DAY_MICROS * 365 * 10),
                        rnd.nextLong(Micros.DAY_MICROS * 365 * 10),
                        0
                )
        );
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
    public void testSortManySegments() throws Exception {
        Rnd rnd = TestUtils.generateRandom(null);
        TestUtils.assertMemoryLeak(() -> {
            int segmentCount = 1 + rnd.nextInt(200);
            int rowsPerCommit = 1 + rnd.nextInt(10000);
            int commits = Math.min(rnd.nextInt(1000), Math.max(1, 1_000_000 / rowsPerCommit / segmentCount));
            long increment = Math.min((long) (Micros.DAY_MICROS * rnd.nextDouble()), (1L << 50) / rowsPerCommit / segmentCount);

            testSortManySegments(
                    rnd.nextLong(123124512354523L),
                    increment,
                    rowsPerCommit,
                    commits,
                    segmentCount,
                    false,
                    0
            );
        });
    }

    @Test
    public void testSortManySegmentsLowNumbers() throws Exception {
        Rnd rnd = TestUtils.generateRandom(null);
        TestUtils.assertMemoryLeak(() -> {
            int segmentCount = 1 + rnd.nextInt(30);
            int rowsPerCommit = 1 + rnd.nextInt(10);
            int commits = 1 + rnd.nextInt(10);
            long increment = Math.min((long) (1 + rnd.nextDouble()), (1L << 50) / rowsPerCommit / segmentCount);

            testSortManySegments(
                    rnd.nextLong(123124512354523L),
                    increment,
                    rowsPerCommit,
                    commits,
                    segmentCount,
                    false,
                    0
            );
        });
    }

    @Test
    public void testSortManySegmentsWithLag() throws Exception {
        Rnd rnd = TestUtils.generateRandom(null);
        TestUtils.assertMemoryLeak(() -> {
            int segmentCount = 1 + rnd.nextInt(200);
            int rowsPerCommit = 1 + rnd.nextInt(10000);

            int commits = 1 + Math.min(rnd.nextInt(1000), 1_000_000 / (rowsPerCommit * segmentCount));
            long increment = Math.min((long) (Micros.DAY_MICROS * rnd.nextDouble()), (1L << 50) / rowsPerCommit / segmentCount);

            testSortManySegments(
                    rnd.nextLong(123124512354523L),
                    increment,
                    rowsPerCommit,
                    commits,
                    segmentCount,
                    true,
                    0
            );
        });
    }

    @Test
    public void testSortOne() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long indexAddr = Unsafe.malloc(2 * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                seedMem2Longs(1, indexAddr);
                long expected = Unsafe.getUnsafe().getLong(indexAddr);
                Vect.sortLongIndexAscInPlace(indexAddr, 1);
                Assert.assertEquals(expected, Unsafe.getUnsafe().getLong(indexAddr));
            } finally {
                Unsafe.free(indexAddr, 2 * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        });
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

    private void assertIndexAsc(int count, long indexAddr, long initialAddrA, long initialAddrB, long min, long max) {
        long v = Unsafe.getUnsafe().getLong(indexAddr);
        Assert.assertTrue(v >= min && v <= max);
        for (int i = 1; i < count; i++) {
            long ts = Unsafe.getUnsafe().getLong(indexAddr + i * 2L * Long.BYTES);
            long idx = Unsafe.getUnsafe().getLong(indexAddr + i * 2L * Long.BYTES + Long.BYTES);
            Assert.assertTrue(ts >= min && ts <= max);
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

    private void seedMem1Long(int count, long p, long min, long max) {
        for (int i = 0; i < count; i++) {
            final long z = min + (max > min ? rnd.nextLong(max - min) : 0);
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

    private void seedMem2Longs(int count, long p, long min, long max) {
        for (int i = 0; i < count; i++) {
            final long z = min + (max > min ? rnd.nextLong(max - min) : 0);
            Unsafe.getUnsafe().putLong(p + i * 2L * Long.BYTES, z);
            Unsafe.getUnsafe().putLong(p + i * 2L * Long.BYTES + 8, i);
        }
    }

    private String[] strColAsStringArray(MemoryCMARW dataMemA, MemoryCMARW auxMemA, int rowCount) {
        String[] strings = new String[rowCount];
        for (int i = 0; i < rowCount; i++) {
            CharSequence cs = dataMemA.getStrA(auxMemA.getLong(i * 8L));
            strings[i] = Chars.toString(cs);
        }
        return strings;
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

    private void testSortAB(int aCount, int bCount, long min, long max, long errorResult) {
        final int sizeA = aCount * 2 * Long.BYTES;
        final int sizeB = bCount * 2 * Long.BYTES;

        final int resultSize = sizeA + sizeB;
        final long aAddr = Unsafe.malloc(resultSize, MemoryTag.NATIVE_DEFAULT);
        final long bAddr = Unsafe.malloc(sizeB, MemoryTag.NATIVE_DEFAULT);
        final long cpyAddr = Unsafe.malloc(resultSize, MemoryTag.NATIVE_DEFAULT);

        final long aAddrCopy = Unsafe.malloc(sizeA, MemoryTag.NATIVE_DEFAULT);
        final long bAddrCopy = Unsafe.malloc(sizeB, MemoryTag.NATIVE_DEFAULT);

        try {
            seedMem1Long(aCount, aAddr, min, max);
            seedMem2Longs(bCount, bAddr, min, max);

            Vect.memcpy(aAddrCopy, aAddr, sizeA);
            Vect.memcpy(bAddrCopy, bAddr, sizeB);

            long rowCount = Vect.radixSortABLongIndexAsc(aAddr, aCount, bAddr, bCount, aAddr, cpyAddr, min, max);
            if (errorResult < 0) {
                // The test expects an error
                Assert.assertEquals(errorResult, rowCount);
                return;
            }

            Assert.assertEquals(aCount + bCount, rowCount);
            assertIndexAsc(aCount + bCount, aAddr, aAddrCopy, bAddrCopy, min, max);
        } finally {
            Unsafe.free(aAddr, resultSize, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(bAddr, sizeB, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(cpyAddr, resultSize, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(aAddrCopy, sizeA, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(bAddrCopy, sizeB, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void testSortManySegments(long startTs, long tsIncrement, long rowsPerCommit, long commits, int segmentCount, boolean withLag, int expectedFailure) {
        // To simplify assertion, make lag rows same as very other segment
        long lagRows = withLag ? commits * rowsPerCommit : 0;
        long maxTs = startTs + tsIncrement * rowsPerCommit * commits;
        int tsBytes = (int) Math.ceil((Long.SIZE - Long.numberOfLeadingZeros(maxTs - startTs)) / 8.0);
        int oneForLag = withLag ? 1 : 0;
        int txnBytes = (int) Math.ceil((Long.SIZE - Long.numberOfLeadingZeros(commits * segmentCount - 1 + oneForLag)) / 8.0);

        Assume.assumeTrue(tsBytes + txnBytes <= 8);

        int segmentBytes = (int) Math.ceil((Long.SIZE - Long.numberOfLeadingZeros(segmentCount - 1 + oneForLag)) / 8.0);

        long rowsPerSegment = rowsPerCommit * commits;
        long totalRows = rowsPerSegment * segmentCount + lagRows;

        Assume.assumeTrue(totalRows < 2E6 && totalRows > 0);

        try (DirectLongList segmentAddresses = new DirectLongList(4, MemoryTag.NATIVE_DEFAULT)) {
            long allocationSize = totalRows * 3L * Long.BYTES + Long.BYTES;
            long buf1 = Unsafe.malloc(allocationSize, MemoryTag.NATIVE_DEFAULT);
            long buf2 = Unsafe.malloc(allocationSize, MemoryTag.NATIVE_DEFAULT);
            for (int s = 0; s < segmentCount; s++) {
                segmentAddresses.add(Unsafe.malloc(rowsPerSegment * 2L * Long.BYTES, MemoryTag.NATIVE_DEFAULT));
            }
            long lagBuf = Unsafe.malloc(lagRows * Long.BYTES, MemoryTag.NATIVE_DEFAULT);

            try (TableWriterSegmentCopyInfo segmentCopyInfo = new TableWriterSegmentCopyInfo()) {

                for (int s = 0; s < segmentCount; s++) {
                    long segmentAddr = segmentAddresses.get(s);
                    long ts = startTs;
                    for (int c = 0; c < commits; c++) {
                        for (int r = 0; r < rowsPerCommit; r++) {
                            Unsafe.getUnsafe().putLong(segmentAddr + (c * rowsPerCommit + r) * 2L * Long.BYTES, ts);
                            ts += tsIncrement;
                        }
                        segmentCopyInfo.addTxn((long) c * rowsPerCommit, c * segmentCount + s, rowsPerCommit, s, startTs, ts - tsIncrement);
                    }
                    segmentCopyInfo.addSegment(1, s, 0, commits * rowsPerCommit, false);
                }

                for (int lr = 0; lr < lagRows; lr++) {
                    Unsafe.getUnsafe().putLong(lagBuf + (long) lr * Long.BYTES, startTs + tsIncrement * lr);
                }

                long indexFormat = Vect.radixSortManySegmentsIndexAsc(
                        buf1,
                        buf2,
                        segmentAddresses.getAddress(),
                        segmentCount,
                        segmentCopyInfo.getTxnInfoAddress(),
                        commits * segmentCount,
                        rowsPerCommit,
                        lagBuf,
                        lagRows,
                        startTs,
                        maxTs,
                        totalRows,
                        Vect.SHUFFLE_INDEX_FORMAT
                );

                if (expectedFailure < 0) {
                    Assert.assertEquals(indexFormat, expectedFailure);
                    return;
                }
                Assert.assertTrue("Internal sort failure: " + indexFormat, Vect.isIndexSuccess(indexFormat));

                // Assert the data
                long lastTs = startTs;
                long segmentIdMask = (1L << (segmentBytes * 8)) - 1;
                for (long r = 0; r < totalRows; r++) {
                    long ts = Unsafe.getUnsafe().getLong(buf1 + r * 2L * Long.BYTES);
                    long idx = Unsafe.getUnsafe().getLong(buf1 + r * 2L * Long.BYTES + Long.BYTES);

                    if (ts < lastTs) {
                        Assert.fail("Micros are not in order, row=" + r + ", actual=" + ts + ", last=" + lastTs);
                    }
                    lastTs = ts;

                    int divider = segmentCount + (withLag ? 1 : 0);
                    long segmentId = idx & segmentIdMask;
                    long segmentRow = idx >> (segmentBytes * 8);

                    if (startTs + (r / divider) * tsIncrement != ts) {
                        Assert.assertEquals(Long.toString(r), startTs + (r / divider) * tsIncrement, ts);
                    }

                    if (withLag) {
                        if (r % divider == 0) {
                            // Lag rows should have segmentId as segmentCount, e.g. last virtual segment
                            Assert.assertEquals(segmentCount, segmentId);
                        } else {
                            Assert.assertEquals(r % divider - 1, segmentId);
                        }
                    } else {
                        Assert.assertEquals(r % divider, segmentId);
                    }
                    Assert.assertEquals(segmentRow, r / divider);

                }

            } finally {
                for (int s = 0; s < segmentCount; s++) {
                    Unsafe.free(segmentAddresses.get(s), rowsPerSegment * 2 * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
                Unsafe.free(lagBuf, lagRows * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(buf1, allocationSize, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(buf2, allocationSize, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    private String[] varcharColAsStringArray(MemoryCMARW dataMemA, MemoryCMARW auxMemA, int rowCount) {
        String[] strings = new String[rowCount];
        StringSink sink = new StringSink();
        for (int i = 0; i < rowCount; i++) {
            Utf8Sequence utf8Sequence = VarcharTypeDriver.getSplitValue(auxMemA, dataMemA, i, 1);
            if (utf8Sequence == null) {
                strings[i] = null;
            } else {
                sink.clear();
                sink.put(utf8Sequence);
                strings[i] = Chars.toString(sink);
            }
        }
        return strings;
    }

    static {
        Os.init();
        Net.init();
    }
}
