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

import io.questdb.std.IntGroupSort;
import io.questdb.std.IntList;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * IntGroupSort, through its only entry point {@link IntList#sortGroups(int)}: the int twin of
 * LongGroupSort's introsort. Besides the order, the tests count group comparisons (through
 * {@link IntGroupSort#getComparisonCountForTesting()}, which counts under -ea) and heapsort
 * fallbacks, so that a quadratic sort fails on the count rather than on a timeout. Both counters
 * are monotonic; the tests read deltas on their own thread.
 * <p>
 * Bound on the comparisons of one sort of n groups, read off the code: the fast-path pre-scan
 * takes at most n - 1; one partition of a range of length L takes at most 2L + 15 (12 for the
 * ninther, L + 2 for the two scans, L for the equal checks after the swaps, 1 for the i == j
 * check), ranges at one depth are disjoint and longer than 16, so a depth costs under 3n, over at
 * most D = 2 * (floor(log2 n) + 1) depths; insertion sort on ranges of at most 16 groups costs at
 * most 7.5 per group; heapsort at most 2L(1 + log2 L). So B(n) = (n - 1) + 3nD + 7.5n +
 * 2n(1 + log2 n).
 */
public class IntGroupSortTest {
    private static final int N = 20_000;

    @Test
    public void testAllEqualTakesFastPath() {
        final IntList list = new IntList(N);
        for (int i = 0; i < N; i++) {
            list.add(42);
        }
        assertComparisons(list, 1, N - 1);
        for (int i = 0; i < N; i++) {
            Assert.assertEquals(42, list.getQuick(i));
        }
    }

    @Test
    public void testAlreadySortedTakesFastPath() {
        final IntList list = new IntList(N);
        for (int i = 0; i < N; i++) {
            list.add(3 * i - 7);
        }
        assertComparisons(list, 1, N - 1);
        for (int i = 0; i < N; i++) {
            Assert.assertEquals(3 * i - 7, list.getQuick(i));
        }
    }

    @Test
    public void testDescendingWithTiesReversesCorrectly() {
        // non-strict descending input takes the reversal fast path
        final IntList list = new IntList();
        add2(list, 7, 70);
        add2(list, 7, 70);
        add2(list, 5, 50);
        add2(list, 5, 50);
        add2(list, 2, 20);
        list.sortGroups(2);
        assertList(list, 2, 20, 5, 50, 5, 50, 7, 70, 7, 70);
    }

    @Test
    public void testMatchesReferenceSortRandomized() {
        // group sizes 1-4 and a narrow value range: ties on every position, ranges past the
        // insertion sort, median-of-3 and ninther thresholds
        final Rnd rnd = TestUtils.generateRandom(null);
        for (int iter = 0; iter < 300; iter++) {
            final int n = 1 + rnd.nextInt(4);
            final int groupCount = rnd.nextInt(iter < 200 ? 300 : 3000);
            final IntList list = new IntList(groupCount * n);
            final int[][] reference = new int[groupCount][n];
            final int range = 1 + rnd.nextInt(16);
            for (int g = 0; g < groupCount; g++) {
                for (int k = 0; k < n; k++) {
                    final int value = rnd.nextInt(range) - range / 2;
                    list.add(value);
                    reference[g][k] = value;
                }
            }
            Arrays.sort(reference, IntGroupSortTest::compareRows);
            list.sortGroups(n);
            Assert.assertEquals(groupCount * n, list.size());
            for (int g = 0; g < groupCount; g++) {
                for (int k = 0; k < n; k++) {
                    Assert.assertEquals("iteration " + iter + ", group " + g + ", key " + k, reference[g][k], list.getQuick(g * n + k));
                }
            }
        }
    }

    @Test
    public void testMedianOfThreeKillerFallsBackToHeapsort() {
        // Musser's median-of-3-killer drives the sampled pivots into unbalanced splits; the depth
        // budget hands the degraded ranges to heapsort
        final IntList list = killer(65_536);
        final long heapSorts = IntGroupSort.getHeapSortCallCountForTesting();
        list.sortGroups(1);
        Assert.assertTrue(IntGroupSort.getHeapSortCallCountForTesting() > heapSorts);
        for (int i = 0; i < 65_536; i++) {
            Assert.assertEquals(i + 1, list.getQuick(i));
        }
    }

    @Test
    public void testMedianOfThreeKillerWithinBound() {
        final IntList list = killer(N);
        assertComparisonsAtMost(list, 1, bound(N));
        for (int i = 0; i < N; i++) {
            Assert.assertEquals(i + 1, list.getQuick(i));
        }
    }

    @Test
    public void testOrganPipeWithinBoundWithoutHeapsort() {
        final IntList list = new IntList(N);
        for (int i = 0; i < N; i++) {
            list.add(i < N / 2 ? i : N - i);
        }
        final long heapSorts = IntGroupSort.getHeapSortCallCountForTesting();
        assertComparisonsAtMost(list, 1, bound(N));
        Assert.assertEquals(heapSorts, IntGroupSort.getHeapSortCallCountForTesting());
        // 1..N/2 - 1 twice, 0 and N/2 once each
        Assert.assertEquals(0, list.getQuick(0));
        Assert.assertEquals(1, list.getQuick(1));
        Assert.assertEquals(1, list.getQuick(2));
        for (int i = 1; i < N - 1; i++) {
            Assert.assertEquals((i + 1) / 2, list.getQuick(i));
        }
        Assert.assertEquals(N / 2, list.getQuick(N - 1));
    }

    @Test
    public void testRandomWithinBound() {
        final Rnd rnd = TestUtils.generateRandom(null);
        final IntList list = new IntList(N);
        final int[] reference = new int[N];
        for (int i = 0; i < N; i++) {
            reference[i] = rnd.nextInt();
            list.add(reference[i]);
        }
        Arrays.sort(reference);
        assertComparisonsAtMost(list, 1, bound(N));
        for (int i = 0; i < N; i++) {
            Assert.assertEquals(reference[i], list.getQuick(i));
        }
    }

    @Test
    public void testReverseSortedTakesFastPath() {
        final IntList list = new IntList(N);
        for (int i = N; i > 0; i--) {
            list.add(i);
        }
        final long heapSorts = IntGroupSort.getHeapSortCallCountForTesting();
        assertComparisons(list, 1, N - 1);
        Assert.assertEquals(heapSorts, IntGroupSort.getHeapSortCallCountForTesting());
        for (int i = 0; i < N; i++) {
            Assert.assertEquals(i + 1, list.getQuick(i));
        }
    }

    @Test
    public void testSawtoothFallsBackToHeapsort() {
        // 16 distinct leading ints, the second makes every group distinct: the residual structure
        // exhausts the depth budget
        final int groupCount = 100_000;
        final IntList list = new IntList(2 * groupCount);
        for (int i = 0; i < groupCount; i++) {
            add2(list, i % 16, i);
        }
        final long heapSorts = IntGroupSort.getHeapSortCallCountForTesting();
        list.sortGroups(2);
        Assert.assertTrue(IntGroupSort.getHeapSortCallCountForTesting() > heapSorts);
        for (int i = 0; i < groupCount; i++) {
            final int lead = i / (groupCount / 16);
            Assert.assertEquals(lead, list.getQuick(2 * i));
            Assert.assertEquals(lead + 16 * (i % (groupCount / 16)), list.getQuick(2 * i + 1));
        }
    }

    @Test
    public void testShuffledDuplicatesAvoidHeapsort() {
        final Rnd rnd = TestUtils.generateRandom(null);
        final int groupCount = 100_000;
        final IntList list = new IntList(2 * groupCount);
        final int[] counts = new int[100];
        for (int i = 0; i < groupCount; i++) {
            final int key = rnd.nextInt(100);
            counts[key]++;
            add2(list, key, key);
        }
        final long heapSorts = IntGroupSort.getHeapSortCallCountForTesting();
        list.sortGroups(2);
        Assert.assertEquals(heapSorts, IntGroupSort.getHeapSortCallCountForTesting());
        int g = 0;
        for (int key = 0; key < 100; key++) {
            for (int c = 0; c < counts[key]; c++, g++) {
                Assert.assertEquals(key, list.getQuick(2 * g));
                Assert.assertEquals(key, list.getQuick(2 * g + 1));
            }
        }
    }

    @Test
    public void testSortGrowsListCapacityForItsStack() {
        // the stack lives in the list's spare capacity: a list filled to its capacity grows, and
        // the sort leaves its size and contents as they should be
        final IntList list = new IntList(64);
        final int[] reference = new int[64];
        for (int i = 0; i < 64; i++) {
            reference[i] = (i * 37) % 64;
            list.add(reference[i]);
        }
        Arrays.sort(reference);
        list.sortGroups(1);
        Assert.assertEquals(64, list.size());
        for (int i = 0; i < 64; i++) {
            Assert.assertEquals(reference[i], list.getQuick(i));
        }
    }

    @Test
    public void testTieBreaksOnLessSignificantInt() {
        final IntList list = new IntList();
        add2(list, 5, 30);
        add2(list, 5, 10);
        add2(list, 5, 20);
        add2(list, 4, 99);
        list.sortGroups(2);
        assertList(list, 4, 99, 5, 10, 5, 20, 5, 30);
    }

    @Test
    public void testTinyInputs() {
        final IntList list = new IntList();
        list.sortGroups(2);
        Assert.assertEquals(0, list.size());
        add2(list, 7, 8);
        list.sortGroups(2);
        assertList(list, 7, 8);
        add2(list, 1, 2);
        list.sortGroups(2);
        assertList(list, 1, 2, 7, 8);
        list.sortGroups(2);
        assertList(list, 1, 2, 7, 8);
    }

    private static void add2(IntList list, int a, int b) {
        list.add(a);
        list.add(b);
    }

    private static void assertComparisons(IntList list, int groupSize, long expected) {
        final long before = IntGroupSort.getComparisonCountForTesting();
        list.sortGroups(groupSize);
        Assert.assertEquals(expected, IntGroupSort.getComparisonCountForTesting() - before);
    }

    private static void assertComparisonsAtMost(IntList list, int groupSize, long bound) {
        final long before = IntGroupSort.getComparisonCountForTesting();
        list.sortGroups(groupSize);
        final long comparisons = IntGroupSort.getComparisonCountForTesting() - before;
        Assert.assertTrue("comparisons " + comparisons + " > " + bound, comparisons <= bound);
    }

    private static void assertList(IntList list, int... expected) {
        Assert.assertEquals(expected.length, list.size());
        for (int i = 0; i < expected.length; i++) {
            Assert.assertEquals("index " + i, expected[i], list.getQuick(i));
        }
    }

    // B(n) from the class javadoc
    private static long bound(int n) {
        final double log2 = Math.log(n) / Math.log(2);
        final int depth = 2 * ((31 - Integer.numberOfLeadingZeros(n)) + 1);
        return (long) ((n - 1) + 3.0 * n * depth + 7.5 * n + 2.0 * n * (1 + log2));
    }

    private static int compareRows(int[] a, int[] b) {
        for (int k = 0; k < a.length; k++) {
            final int cmp = Integer.compare(a[k], b[k]);
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    // Musser's median-of-3-killer permutation of 1..n (n even)
    private static IntList killer(int n) {
        final int k = n / 2;
        final int[] keys = new int[n];
        for (int i = 1; i <= k; i++) {
            if ((i & 1) == 1) {
                keys[i - 1] = i;
                keys[i] = k + i;
            }
            keys[k + i - 1] = 2 * i;
        }
        final IntList list = new IntList(n);
        for (int i = 0; i < n; i++) {
            list.add(keys[i]);
        }
        return list;
    }
}
