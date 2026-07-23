/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

import io.questdb.std.LongGroupSort;
import io.questdb.std.LongList;
import io.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class LongGroupSortTest {

    // The large-input tests double as complexity canaries: before LongGroupSort
    // switched to introsort (median-of-three pivot, sorted-input fast-path, heapsort
    // depth fallback), the fixed last-element pivot made each of these shapes take
    // O(D^2) comparisons - minutes of CPU at this size - while a healthy sort
    // finishes in milliseconds. Each canary carries a generous timeout, orders of
    // magnitude above the healthy runtime, that turns a complexity regression into a
    // prompt red failure instead of a CI hang. The adversarial-shape tests also
    // assert via LongGroupSort.getHeapSortCallCountForTesting() that the heapsort
    // fallback actually ran, so a regression that quietly disables the fallback
    // (e.g. a mis-set depth budget) fails even though the output stays sorted.
    private static final int LARGE_GROUP_COUNT = 1_000_000;
    private static final long LARGE_TEST_TIMEOUT_MS = 300_000;

    @Test(timeout = LARGE_TEST_TIMEOUT_MS)
    public void testAllEqualLarge() {
        LongList list = new LongList(2 * LARGE_GROUP_COUNT);
        for (int i = 0; i < LARGE_GROUP_COUNT; i++) {
            list.add(42L, 42L);
        }
        LongGroupSort.quickSort(2, list, 0, LARGE_GROUP_COUNT);
        assertSortedByFirstLong(list, 0, LARGE_GROUP_COUNT);
        Assert.assertEquals(2 * LARGE_GROUP_COUNT, list.size());
        Assert.assertEquals(42, list.getQuick(0));
        Assert.assertEquals(42, list.getQuick(2 * LARGE_GROUP_COUNT - 1));
    }

    @Test(timeout = LARGE_TEST_TIMEOUT_MS)
    public void testAlreadySortedLarge() {
        LongList list = new LongList(2 * LARGE_GROUP_COUNT);
        for (int i = 0; i < LARGE_GROUP_COUNT; i++) {
            list.add(10L * i, 10L * i + 5);
        }
        LongGroupSort.quickSort(2, list, 0, LARGE_GROUP_COUNT);
        for (int i = 0; i < LARGE_GROUP_COUNT; i++) {
            Assert.assertEquals(10L * i, list.getQuick(2 * i));
            Assert.assertEquals(10L * i + 5, list.getQuick(2 * i + 1));
        }
    }

    @Test
    public void testMatchesReferenceSortRandomized() {
        Rnd rnd = new Rnd();
        for (int iter = 0; iter < 200; iter++) {
            int n = 1 + rnd.nextInt(4);
            int groupCount = rnd.nextInt(300);
            LongList list = new LongList(groupCount * n);
            long[][] reference = new long[groupCount][n];
            for (int g = 0; g < groupCount; g++) {
                for (int k = 0; k < n; k++) {
                    // narrow value range makes ties on every key position common
                    long value = rnd.nextInt(8);
                    list.add(value);
                    reference[g][k] = value;
                }
            }
            Arrays.sort(reference, LongGroupSortTest::compareRows);

            LongGroupSort.quickSort(n, list, 0, groupCount);

            for (int g = 0; g < groupCount; g++) {
                for (int k = 0; k < n; k++) {
                    Assert.assertEquals(
                            "iteration " + iter + ", group " + g + ", key " + k,
                            reference[g][k],
                            list.getQuick(g * n + k)
                    );
                }
            }
        }
    }

    @Test(timeout = LARGE_TEST_TIMEOUT_MS)
    public void testOrganPipeLarge() {
        // organ-pipe input reliably exhausts the introsort depth budget at this size,
        // exercising the heapsort fallback
        LongList list = new LongList(2 * LARGE_GROUP_COUNT);
        for (int i = 0; i < LARGE_GROUP_COUNT; i++) {
            long lo = i < LARGE_GROUP_COUNT / 2 ? i : LARGE_GROUP_COUNT - i;
            list.add(lo, lo + 1);
        }
        long heapSortCallsBefore = LongGroupSort.getHeapSortCallCountForTesting();
        LongGroupSort.quickSort(2, list, 0, LARGE_GROUP_COUNT);
        Assert.assertTrue(
                "heapsort fallback did not run on organ-pipe input",
                LongGroupSort.getHeapSortCallCountForTesting() > heapSortCallsBefore
        );
        assertSortedByFirstLong(list, 0, LARGE_GROUP_COUNT);
        // 1..(D/2 - 1) appear twice, 0 and D/2 once each
        Assert.assertEquals(0, list.getQuick(0));
        Assert.assertEquals(1, list.getQuick(2));
        Assert.assertEquals(1, list.getQuick(4));
        Assert.assertEquals(LARGE_GROUP_COUNT / 2, list.getQuick(2 * LARGE_GROUP_COUNT - 2));
    }

    @Test(timeout = LARGE_TEST_TIMEOUT_MS)
    public void testReverseSortedLarge() {
        LongList list = new LongList(2 * LARGE_GROUP_COUNT);
        for (int i = LARGE_GROUP_COUNT; i > 0; i--) {
            list.add(10L * i, 10L * i + 5);
        }
        LongGroupSort.quickSort(2, list, 0, LARGE_GROUP_COUNT);
        for (int i = 0; i < LARGE_GROUP_COUNT; i++) {
            Assert.assertEquals(10L * (i + 1), list.getQuick(2 * i));
            Assert.assertEquals(10L * (i + 1) + 5, list.getQuick(2 * i + 1));
        }
    }

    @Test(timeout = LARGE_TEST_TIMEOUT_MS)
    public void testSawtoothDuplicateHeavy() {
        // 16 distinct keys across 100k groups; this shape also reaches the heapsort fallback
        final int groupCount = 100_000;
        LongList list = new LongList(2 * groupCount);
        for (int i = 0; i < groupCount; i++) {
            list.add(i % 16L, i);
        }
        long heapSortCallsBefore = LongGroupSort.getHeapSortCallCountForTesting();
        LongGroupSort.quickSort(2, list, 0, groupCount);
        Assert.assertTrue(
                "heapsort fallback did not run on sawtooth input",
                LongGroupSort.getHeapSortCallCountForTesting() > heapSortCallsBefore
        );
        assertSortedByFirstLong(list, 0, groupCount);
        for (int i = 0; i < groupCount; i++) {
            // each of the 16 keys occupies a contiguous run of groupCount / 16 groups
            Assert.assertEquals(i / (groupCount / 16), list.getQuick(2 * i));
        }
    }

    @Test
    public void testSortsRequestedRangeOnly() {
        LongList list = new LongList();
        list.add(90L, 91L);   // prefix, group 0
        list.add(70L, 71L);   // batch
        list.add(30L, 31L);
        list.add(50L, 51L);
        list.add(10L, 11L);   // suffix, group 4
        LongGroupSort.quickSort(2, list, 1, 4);
        Assert.assertEquals(90, list.getQuick(0));
        Assert.assertEquals(91, list.getQuick(1));
        Assert.assertEquals(30, list.getQuick(2));
        Assert.assertEquals(31, list.getQuick(3));
        Assert.assertEquals(50, list.getQuick(4));
        Assert.assertEquals(51, list.getQuick(5));
        Assert.assertEquals(70, list.getQuick(6));
        Assert.assertEquals(71, list.getQuick(7));
        Assert.assertEquals(10, list.getQuick(8));
        Assert.assertEquals(11, list.getQuick(9));
        Assert.assertEquals(10, list.size());
    }

    @Test
    public void testTieBreaksOnLessSignificantLong() {
        LongList list = new LongList();
        list.add(5L, 30L);
        list.add(5L, 10L);
        list.add(5L, 20L);
        LongGroupSort.quickSort(2, list, 0, 3);
        Assert.assertEquals(10, list.getQuick(1));
        Assert.assertEquals(20, list.getQuick(3));
        Assert.assertEquals(30, list.getQuick(5));
    }

    @Test
    public void testTinyInputs() {
        // empty
        LongList list = new LongList();
        LongGroupSort.quickSort(2, list, 0, 0);
        Assert.assertEquals(0, list.size());

        // single group
        list.add(7L, 8L);
        LongGroupSort.quickSort(2, list, 0, 1);
        Assert.assertEquals(7, list.getQuick(0));
        Assert.assertEquals(8, list.getQuick(1));

        // two groups, out of order
        list.add(1L, 2L);
        LongGroupSort.quickSort(2, list, 0, 2);
        Assert.assertEquals(1, list.getQuick(0));
        Assert.assertEquals(2, list.getQuick(1));
        Assert.assertEquals(7, list.getQuick(2));
        Assert.assertEquals(8, list.getQuick(3));

        // two groups, already in order
        LongGroupSort.quickSort(2, list, 0, 2);
        Assert.assertEquals(1, list.getQuick(0));
        Assert.assertEquals(7, list.getQuick(2));
    }

    private static void assertSortedByFirstLong(LongList list, int groupLo, int groupHi) {
        for (int g = groupLo + 1; g < groupHi; g++) {
            long prev = list.getQuick(2 * (g - 1));
            long curr = list.getQuick(2 * g);
            Assert.assertTrue("group " + g + ": " + prev + " > " + curr, prev <= curr);
        }
    }

    private static int compareRows(long[] a, long[] b) {
        for (int k = 0; k < a.length; k++) {
            int cmp = Long.compare(a[k], b[k]);
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }
}
