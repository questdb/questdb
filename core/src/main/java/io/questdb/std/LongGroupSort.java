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

package io.questdb.std;

import org.jetbrains.annotations.TestOnly;

/**
 * Sort a long array which is actually intrusively storing a group (tuple) of N longs.
 * The group is considered ordered (for sorting purposes) from most significant long to least significant.
 */
public class LongGroupSort {

    // Stack entries are (low, high, depthBudget) triples. quickSortImpl pushes the larger
    // partition first and processes the smaller one next, so the number of deferred entries
    // never exceeds log2(groupCount) + 1 <= 33 for any int-sized input: every consecutive
    // descent goes into a partition at most half the size of its parent, and each descent
    // level leaves at most one deferred sibling on the stack. 33 triples need 99 longs;
    // 128 leaves comfortable slack.
    private static final int STACK_CAPACITY = 128;
    // Test-only observability for the heapsort fallback. heapSort() increments the
    // counter on entry - a cold path reached only when a partition chain exhausts the
    // introsort depth budget - so the sort hot path pays nothing. Tests read the
    // counter on the same thread that ran the sort and assert on deltas, so a plain
    // non-volatile static suffices; the counter is monotonic and never reset.
    private static long heapSortCallCount;

    /**
     * Returns the number of heapsort-fallback activations since class load. The counter
     * is monotonic: capture it before a sort and assert on the delta afterwards.
     *
     * @return count of heapsort-fallback activations
     */
    @TestOnly
    public static long getHeapSortCallCountForTesting() {
        return heapSortCallCount;
    }

    /**
     * Sort a long list which is actually intrusively storing a group (tuple) of N longs.
     * Uses the unused capacity of the LongList as temporary stack space, so the sort
     * performs no allocations and cannot overflow the JVM stack.
     * <p>
     * The sort is an introsort: quicksort with median-of-three pivot selection, an O(count)
     * fast-path for already-sorted input, and a heapsort fallback once a partition chain
     * exceeds the 2*log2(count) depth budget. Worst-case cost is O(count * log(count))
     * group comparisons for every input shape, including sorted, reverse-sorted and
     * duplicate-heavy data.
     *
     * @param n        number of longs in a group
     * @param longList LongList containing the data to sort
     * @param groupLo  start group index (inclusive)
     * @param groupHi  end group index (exclusive)
     */
    public static void quickSort(int n, LongList longList, int groupLo, int groupHi) {
        assert groupHi >= groupLo;
        quickSortImpl(longList, groupLo, groupHi, n);
    }

    private static int compareGroups(long[] array, int i, int j, int n) {
        for (int k = 0; k < n; k++) {
            int comparison = Long.compare(array[i * n + k], array[j * n + k]);
            if (comparison != 0) {
                return comparison;
            }
        }
        return 0;
    }

    private static void heapSort(long[] array, int low, int high, int n) {
        heapSortCallCount++;
        int count = high - low;
        for (int i = (count >> 1) - 1; i >= 0; i--) {
            siftDown(array, low, i, count, n);
        }
        for (int end = count - 1; end > 0; end--) {
            swapGroups(array, low, low + end, n);
            siftDown(array, low, 0, end, n);
        }
    }

    private static int partition(long[] array, int low, int high, int n) {
        // Median-of-three pivot selection: order the groups at low, mid and high - 1 so
        // that the median lands at high - 1, where the Lomuto scan below expects the pivot.
        // A fixed-position pivot degrades to a worst-case split on sorted and
        // reverse-sorted input; the median of three splits both roughly in half.
        int pivotIndex = high - 1;
        int mid = (low + pivotIndex) >>> 1;
        if (compareGroups(array, mid, low, n) < 0) {
            swapGroups(array, mid, low, n);
        }
        if (compareGroups(array, pivotIndex, low, n) < 0) {
            swapGroups(array, pivotIndex, low, n);
        }
        if (compareGroups(array, mid, pivotIndex, n) < 0) {
            swapGroups(array, mid, pivotIndex, n);
        }

        int i = low - 1;
        for (int j = low; j < high; j++) {
            if (compareGroups(array, j, pivotIndex, n) < 0) {
                swapGroups(array, ++i, j, n);
            }
        }

        swapGroups(array, ++i, pivotIndex, n);
        return i;
    }

    private static void quickSortImpl(LongList longList, int low, int high, int n) {
        int stackStart = longList.size();
        int stackPos = stackStart;
        long[] array = longList.resetCapacityInternal(stackPos + STACK_CAPACITY);

        try {
            if (low + 1 >= high) {
                return;
            }

            // Fast-path: a single O(count) scan detects already-sorted input, the common
            // shape for ascending IN-list / OR-ed timestamp disjuncts. On unsorted input
            // the scan aborts at the first inversion.
            boolean isSorted = true;
            for (int g = low + 1; g < high; g++) {
                if (compareGroups(array, g - 1, g, n) > 0) {
                    isSorted = false;
                    break;
                }
            }
            if (isSorted) {
                return;
            }

            // Introsort depth budget: once a partition chain runs 2*ceil(log2(count))
            // levels deep without bottoming out, the pivots are pathological and we finish
            // that range with heapsort, capping the worst case at O(count * log(count)).
            long depth = 2L * (32 - Integer.numberOfLeadingZeros(high - low));
            array[stackPos++] = low;
            array[stackPos++] = high;
            array[stackPos++] = depth;

            while (stackPos > stackStart) {
                depth = array[--stackPos];
                high = (int) array[--stackPos];
                low = (int) array[--stackPos];

                if (low + 1 >= high) {
                    continue;
                }

                if (depth == 0) {
                    heapSort(array, low, high, n);
                    continue;
                }

                int pi = partition(array, low, high, n);
                depth--;

                // Push the larger partition first (defer it), the smaller one last
                // (process it next); this keeps the stack depth logarithmic.
                if (pi - low > high - pi - 1) {
                    array[stackPos++] = low;
                    array[stackPos++] = pi;
                    array[stackPos++] = depth;
                    array[stackPos++] = pi + 1;
                    array[stackPos++] = high;
                    array[stackPos++] = depth;
                } else {
                    array[stackPos++] = pi + 1;
                    array[stackPos++] = high;
                    array[stackPos++] = depth;
                    array[stackPos++] = low;
                    array[stackPos++] = pi;
                    array[stackPos++] = depth;
                }
            }
        } finally {
            longList.setPos(stackStart);
        }
    }

    private static void siftDown(long[] array, int low, int start, int count, int n) {
        int root = start;
        for (; ; ) {
            int child = 2 * root + 1;
            if (child >= count) {
                break;
            }
            if (child + 1 < count && compareGroups(array, low + child, low + child + 1, n) < 0) {
                child++;
            }
            if (compareGroups(array, low + root, low + child, n) < 0) {
                swapGroups(array, low + root, low + child, n);
                root = child;
            } else {
                break;
            }
        }
    }

    private static void swapGroups(long[] array, int i, int j, int n) {
        if (i != j) {
            for (int k = 0; k < n; k++) {
                long temp = array[i * n + k];
                array[i * n + k] = array[j * n + k];
                array[j * n + k] = temp;
            }
        }
    }
}
