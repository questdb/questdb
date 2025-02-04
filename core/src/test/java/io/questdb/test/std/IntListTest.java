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

import io.questdb.std.IntList;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class IntListTest {

    @Test
    public void testAddAll() {
        IntList src = new IntList();
        for (int i = 0; i < 100; i++) {
            src.add(i);
        }

        IntList dst = new IntList();
        dst.clear();
        dst.addAll(src);

        Assert.assertEquals(dst, src);
    }

    @Test
    public void testBinarySearchFuzz() {
        final int N = 997; // prime
        final int skipRate = 4;
        for (int c = 0; c < N; c++) {
            for (int i = 0; i < skipRate; i++) {
                testBinarySearchFuzz0(c, i);
            }
        }

        testBinarySearchFuzz0(1, 0);
    }

    @Test
    public void testEquals() {
        final IntList list1 = new IntList();
        list1.add(1);
        list1.add(2);
        list1.add(3);

        // different order
        final IntList list2 = new IntList();
        list2.add(1);
        list2.add(3);
        list2.add(2);
        Assert.assertNotEquals(list1, list2);

        // longer
        final IntList list3 = new IntList();
        list3.add(1);
        list3.add(2);
        list3.add(3);
        list3.add(4);
        Assert.assertNotEquals(list1, list3);

        // shorter
        final IntList list4 = new IntList();
        list4.add(1);
        list4.add(2);
        Assert.assertNotEquals(list1, list4);

        // empty
        final IntList list5 = new IntList();
        Assert.assertNotEquals(list1, list5);

        // null
        Assert.assertNotEquals(list1, null);

        // equals
        final IntList list6 = new IntList();
        list6.add(1);
        list6.add(2);
        list6.add(3);
        Assert.assertEquals(list1, list6);
    }

    @Test
    public void testIndexOf() {
        IntList list = new IntList();
        for (int i = 100; i > -1; i--) {
            list.add(i);
        }

        for (int i = 100; i > -1; i--) {
            Assert.assertEquals(100 - i, list.indexOf(i, 0, 101));
        }
    }

    @Test
    public void testRestoreInitialCapacity() {
        final int N = 1000;
        IntList list = new IntList();
        int initialCapacity = list.capacity();

        for (int i = 0; i < N; i++) {
            list.add(i);
        }

        Assert.assertEquals(N, list.size());
        Assert.assertTrue(list.capacity() >= N);

        list.restoreInitialCapacity();
        Assert.assertEquals(0, list.size());
        Assert.assertEquals(initialCapacity, list.capacity());
    }

    @Test
    public void testSmoke() {
        IntList list = new IntList();
        for (int i = 0; i < 100; i++) {
            list.add(i);
        }
        Assert.assertEquals(100, list.size());
        Assert.assertTrue(list.capacity() >= 100);

        for (int i = 0; i < 100; i++) {
            Assert.assertEquals(i, list.getQuick(i));
            Assert.assertTrue(list.contains(i));
        }

        for (int i = 0; i < 100; i++) {
            list.remove(i);
        }
        Assert.assertEquals(0, list.size());
    }

    @Test
    public void testSortGroups() {
        Rnd rnd = TestUtils.generateRandom(null);
        int n = 1 + rnd.nextInt(20);
        sortGroupsRandom(n, rnd.nextInt(10000), rnd);
    }

    @Test
    public void testSortGroupsEqualElements() {
        Rnd rnd = TestUtils.generateRandom(null);
        int n = 1 + rnd.nextInt(20);
        checkSortGroupsEqualElements(n, rnd.nextInt(10000), rnd);
    }

    @Test
    public void testSortGroupsNotSupported() {
        IntList l = new IntList();

        l.add(1);
        try {
            l.sortGroups(2);
            Assert.fail();
        } catch (IllegalStateException e) {
            // expected
        }
        l.sortGroups(1);

        try {
            l.sortGroups(-1);
            Assert.fail();
        } catch (IllegalStateException e) {
            // expected
        }
    }

    private static void checkSortGroupsEqualElements(int n, int elements, Rnd rnd) {
        IntList list = new IntList(elements * n);
        int[][] arrays = new int[elements][];

        for (int i = 0; i < elements; i++) {
            arrays[i] = new int[n];
            int equalValue = rnd.nextInt(5);
            for (int j = 0; j < n; j++) {
                arrays[i][j] = equalValue;
                list.add(arrays[i][j]);
            }
        }

        sortAndCompare(n, elements, arrays, list);
    }

    private static void sortAndCompare(int n, int elements, int[][] arrays, IntList list) {
        Arrays.sort(arrays, (int[] a, int[] b) -> {
            for (int i = 0; i < n; i++) {
                int comparison = Integer.compare(a[i], b[i]);
                if (comparison != 0) {
                    return comparison;
                }
            }
            return 0;
        });

        list.sortGroups(n);

        for (int i = 0; i < elements; i++) {
            for (int j = 0; j < n; j++) {
                Assert.assertEquals(arrays[i][j], list.get(i * n + j));
            }
        }
    }

    private static void sortGroupsRandom(int n, int elements, Rnd rnd) {
        IntList list = new IntList(elements * n);
        int[][] arrays = new int[elements][];

        for (int i = 0; i < elements; i++) {
            arrays[i] = new int[n];
            for (int j = 0; j < n; j++) {
                arrays[i][j] = rnd.nextInt();
                list.add(arrays[i][j]);
            }
        }

        sortAndCompare(n, elements, arrays, list);
    }

    private void testBinarySearchFuzz0(int N, int skipRate) {
        final Rnd rnd = new Rnd();
        final IntList list = new IntList();
        final IntList skipList = new IntList();
        for (int i = 0; i < N; i++) {
            // not skipping ?
            if (skipRate == 0 || rnd.nextInt(skipRate) != 0) {
                list.add(i);
                skipList.add(1);
            } else {
                skipList.add(0);
            }
        }

        // test scan UP
        final int M = list.size();

        for (int i = 0; i < N; i++) {
            int pos = list.binarySearchUniqueList(i);
            int skip = skipList.getQuick(i);

            // the value was skipped
            if (skip == 0) {
                Assert.assertTrue(pos < 0);

                pos = -pos - 1;
                if (pos > 0) {
                    Assert.assertTrue(list.getQuick(pos - 1) < i);
                }

                if (pos < M) {
                    Assert.assertTrue(list.getQuick(pos) > i);
                }
            } else {
                Assert.assertTrue(pos > -1);
                if (pos > 0) {
                    Assert.assertTrue(list.getQuick(pos - 1) < i);
                }
                Assert.assertEquals(list.getQuick(pos), i);
            }
        }


        // search max value (greater than anything in the list)

        int pos = list.binarySearchUniqueList(N);
        Assert.assertTrue(pos < 0);

        pos = -pos - 1;
        Assert.assertEquals(pos, list.size());

        // search min value (less than anything in the list)

        pos = list.binarySearchUniqueList(-1);
        Assert.assertTrue(pos < 0);

        pos = -pos - 1;
        Assert.assertEquals(0, pos);
    }
}
