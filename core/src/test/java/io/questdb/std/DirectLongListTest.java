/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.cairo.BinarySearch;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class DirectLongListTest {

    private static final Log LOG = LogFactory.getLog(DirectLongListTest.class);

    @Test
    public void test128BitSort() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (DirectLongList list = new DirectLongList(256, MemoryTag.NATIVE_LONG_LIST)) {
                final int N = 100;
                for (int i = 0; i < N; ++i) {
                    list.add((100 - i - 1) / 10);
                    list.add((100 - i - 1));
                }

                Vect.sort128BitAscInPlace(list.getAddress(), list.size() / 2);

                for (int i = 0; i < 100; i++) {
                    Assert.assertEquals(i / 10, list.get(i * 2));
                    Assert.assertEquals(i, list.get(i * 2 + 1));
                }
            }
        });
    }

    @Test
    public void test128BitSortFuzzTest() throws Exception {
        long s0 = System.currentTimeMillis();
        long s1 = System.nanoTime();
        Rnd rnd = new Rnd(s0, s1);
        LOG.info().$("random seed : ").$(s0).$(", ").$(s1).$();

        int size = 1024 * 1024;
        int range = Integer.MAX_VALUE - 1;

        TestUtils.assertMemoryLeak(() -> {
            try (DirectLongList list = new DirectLongList(size, MemoryTag.NATIVE_LONG_LIST)) {
                long[] longList = new long[size];
                for (int i = 0; i < size; ++i) {
                    int rnd1 = Math.abs(rnd.nextInt() % (range));
                    int rnd2 = Math.abs(rnd.nextShort());

                    list.add(rnd1);
                    list.add(rnd2);

                    longList[i] = Numbers.encodeLowHighInts(rnd1, rnd2);
                    assert longList[i] >= 0;
                }

                Vect.sort128BitAscInPlace(list.getAddress(), list.size() / 2);
                Arrays.sort(longList);

                for (int i = 0; i < size; i++) {
                    int rnd1 = (int) list.get(2 * i);
                    int rnd2 = (int) list.get(2 * i + 1);

                    int expectedLow = Numbers.decodeLowInt(longList[i]);
                    int expectedHi = Numbers.decodeHighInt(longList[i]);

                    Assert.assertEquals(expectedLow, rnd1);
                    Assert.assertEquals(expectedHi, rnd2);
                }
            }
        });
    }

    @Test
    public void testAddList() {
        DirectLongList list = new DirectLongList(256, MemoryTag.NATIVE_LONG_LIST);
        DirectLongList list2 = new DirectLongList(256, MemoryTag.NATIVE_LONG_LIST);
        final int N = 100;
        for (int i = 0; i < N; ++i) {
            list.add(i);
            list2.add(N + i);
        }
        list.add(list2);
        Assert.assertEquals(256, list.getCapacity());
        Assert.assertEquals(2 * N, list.size());
        for (long i = 0; i < list.size(); ++i) {
            Assert.assertEquals(i, list.get(i));
        }
        list.close();
        list2.close();
    }

    @Test
    public void testAddListExpand() {
        DirectLongList list = new DirectLongList(128, MemoryTag.NATIVE_LONG_LIST);
        DirectLongList list2 = new DirectLongList(128, MemoryTag.NATIVE_LONG_LIST);
        final int N = 100;
        for (int i = 0; i < N; ++i) {
            list.add(i);
            list2.add(N + i);
        }
        list.add(list2);
        Assert.assertEquals(200, list.getCapacity()); //128 + 100 - 28
        Assert.assertEquals(2 * N, list.size());
        for (long i = 0; i < list.size(); ++i) {
            Assert.assertEquals(i, list.get(i));
        }
        list.close();
        list2.close();
    }

    @Test
    public void testBinarySearchFuzz() {
        final int N = 997; // prime
        final int skipRate = 4;
        final int dupeRate = 8;
        final int dupeCountBound = 4;
        for (int c = 0; c < N; c++) {
            for (int i = 0; i < skipRate; i++) {
                for (int j = 0; j < dupeRate; j++) {
                    for (int k = 1; k < dupeCountBound; k++) {
                        testBinarySearchFuzz0(c, i, j, k);
                    }
                }
            }
        }

        testBinarySearchFuzz0(1, 0, 1, 1024);
    }

    @Test
    public void testCapacityAndSize() {
        // use logger so that static memory allocation happens before our control measurement
        LOG.info().$("testCapacityAndSize").$();
        long expected = Unsafe.getMemUsed();
        DirectLongList list = new DirectLongList(1024, MemoryTag.NATIVE_LONG_LIST);
        Assert.assertEquals(1024, list.getCapacity());

        list.extend(2048);
        Assert.assertEquals(2048, list.getCapacity());
        // verify that extend also shrinks capacity
        list.extend(1024);
        Assert.assertEquals(1024, list.getCapacity());

        Assert.assertEquals(0, list.size());
        long addr = list.getAddress();
        Unsafe.getUnsafe().putLong(addr, 42);
        Assert.assertEquals(42, list.get(0));
        for (long i = 0; i < list.getCapacity(); ++i) {
            list.add(i);
        }
        for (long i = 0; i < list.size(); ++i) {
            Assert.assertEquals(i, list.get(i));
        }
        list.clear(0);
        Assert.assertEquals(0, list.size());
        for (long i = 0; i < list.getCapacity(); ++i) {
            Assert.assertEquals(0, list.get(i));
        }
        list.setPos(42);
        Assert.assertEquals(42, list.size());
        list.clear();
        Assert.assertEquals(0, list.size());
        list.close(); //release memory
        Assert.assertEquals(expected, Unsafe.getMemUsed());
    }

    @Test
    public void testResizeMemLeak() {
        // use logger so that static memory allocation happens before our control measurement
        LOG.info().$("testResizeMemLeak").$();
        long expected = Unsafe.getMemUsed();
        try (DirectLongList list = new DirectLongList(1024, MemoryTag.NATIVE_LONG_LIST)) {
            for (int i = 0; i < 1_000_000; i++) {
                list.add(i);
            }
        }
        Assert.assertEquals(expected, Unsafe.getMemUsed());
    }

    @Test
    public void testSearch() {
        DirectLongList list = new DirectLongList(256, MemoryTag.NATIVE_LONG_LIST);
        final int N = 100;
        for (int i = 0; i < N; ++i) {
            list.add(i);
        }
        Assert.assertEquals(N / 2, list.scanSearch(N / 2, 0, list.size()));
        Assert.assertEquals(N / 2, list.binarySearch(N / 2, BinarySearch.SCAN_UP));
        list.close();
    }

    @Test
    public void testToString() {
        DirectLongList list = new DirectLongList(1001, MemoryTag.NATIVE_LONG_LIST);
        final int N = 1000;
        for (int i = 0; i < N; ++i) {
            list.add(i);
        }
        String str1 = list.toString();
        list.add(1001);
        String str2 = list.toString();

        Assert.assertEquals(str1.substring(0, str1.length() - 1) + ", .. }", str2);
    }

    private void testBinarySearchFuzz0(int N, int skipRate, int dupeRate, int dupeCountBound) {
        final Rnd rnd = new Rnd();
        try (final DirectLongList list = new DirectLongList(N, MemoryTag.NATIVE_LONG_LIST)) {
            final IntList skipList = new IntList();
            for (int i = 0; i < N; i++) {
                // not skipping ?
                if (skipRate == 0 || rnd.nextInt(skipRate) != 0) {
                    list.add(i);
                    skipList.add(1);

                    boolean dupe = dupeRate > 0 && rnd.nextInt(dupeRate) == 0;
                    // duplicating value ?
                    if (dupe) {
                        int dupeCount = Math.abs(rnd.nextInt(dupeCountBound));
                        while (dupeCount-- > 0) {
                            list.add(i);
                        }
                    }
                } else {
                    skipList.add(0);
                }
            }

            // test scan UP
            final long M = list.size();

            for (int i = 0; i < N; i++) {
                long pos = list.binarySearch(i, BinarySearch.SCAN_UP);
                int skip = skipList.getQuick(i);

                // the value was skipped
                if (skip == 0) {
                    Assert.assertTrue(pos < 0);

                    pos = -pos - 1;
                    if (pos > 0) {
                        Assert.assertTrue(list.get(pos - 1) < i);
                    }

                    if (pos < M) {
                        Assert.assertTrue(list.get(pos) > i);
                    }
                } else {
                    Assert.assertTrue(pos > -1);
                    if (pos > 0) {
                        Assert.assertTrue(list.get(pos - 1) < i);
                    }
                    Assert.assertEquals(list.get(pos), i);
                }
            }

            for (int i = 0; i < N; i++) {
                long pos = list.binarySearch(i, BinarySearch.SCAN_DOWN);
                int skip = skipList.getQuick(i);

                // the value was skipped
                if (skip == 0) {
                    Assert.assertTrue(pos < 0);

                    pos = -pos - 1;

                    if (pos > 0) {
                        Assert.assertTrue(list.get(pos - 1) < i);
                    }

                    if (pos < M) {
                        Assert.assertTrue(list.get(pos) > i);
                    }
                } else {
                    Assert.assertTrue(pos > -1);
                    Assert.assertEquals(list.get(pos), i);
                    if (pos + 1 < M) {
                        Assert.assertTrue(list.get(pos + 1) > i);
                    }
                }
            }

            // search max value (greater than anything in the list)

            long pos = list.binarySearch(N, BinarySearch.SCAN_UP);
            Assert.assertTrue(pos < 0);

            pos = -pos - 1;
            Assert.assertEquals(pos, list.size());

            // search min value (less than anything in the list)

            pos = list.binarySearch(-1, BinarySearch.SCAN_UP);
            Assert.assertTrue(pos < 0);

            pos = -pos - 1;
            Assert.assertEquals(0, pos);
        }
    }
}