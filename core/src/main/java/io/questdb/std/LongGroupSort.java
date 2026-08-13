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
    // Partitions at or below this group count are finished with insertion sort instead of
    // further partitioning. Insertion sort has no pivot-selection/scan overhead and is the
    // fastest comparison sort on tiny ranges; 16 is the classic introsort cutoff. It also
    // means typical interval lists (rarely more than a few dozen groups) never partition
    // at all.
    private static final int INSERTION_SORT_THRESHOLD = 16;
    // Ranges above this group count pick the pivot with a Tukey ninther (median of the
    // medians of three spaced triples) instead of a single median-of-three sample. The
    // ninther costs ~12 group comparisons, negligible against the O(len) partition scan
    // at this size, and is far harder to defeat with structured input (organ-pipe,
    // median-of-3-killer permutations). Typical interval lists never reach this size.
    private static final int NINTHER_THRESHOLD = 128;
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
     * The sort is an introsort: quicksort with sampled pivot selection (median-of-three,
     * or a Tukey ninther on large ranges) and Bentley-McIlroy fat-pivot (three-way)
     * partitioning, O(count) fast-paths for already-sorted and fully descending input,
     * insertion sort for ranges of up to 16 groups, and a heapsort fallback once a
     * partition chain exceeds the 2*log2(count) depth budget. Worst-case cost is
     * O(count * log(count)) group comparisons for every input shape. Three-way
     * partitioning places groups equal to the pivot in their final position in a single
     * pass, so duplicate-heavy input costs O(count * log(distinct)) rather than degrading
     * to the heapsort fallback.
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

    private static void insertionSort(long[] array, int low, int high, int n) {
        // Swap-based insertion sort keeps the no-allocation guarantee (a shift-based
        // variant would need an n-long scratch buffer for the group in flight). Ranges
        // here hold at most INSERTION_SORT_THRESHOLD groups, so the quadratic swap count
        // is capped and tiny.
        for (int i = low + 1; i < high; i++) {
            for (int j = i; j > low && compareGroups(array, j - 1, j, n) > 0; j--) {
                swapGroups(array, j - 1, j, n);
            }
        }
    }

    /**
     * Returns the index of the median group of the groups at indexes i, j and k.
     */
    private static int median3(long[] array, int i, int j, int k, int n) {
        return compareGroups(array, i, j, n) < 0
                ? (compareGroups(array, j, k, n) < 0 ? j : compareGroups(array, i, k, n) < 0 ? k : i)
                : (compareGroups(array, k, j, n) < 0 ? j : compareGroups(array, k, i, n) < 0 ? k : i);
    }

    /**
     * Bentley-McIlroy fat-pivot partitioning. A sampled median (median-of-three, or a
     * Tukey ninther for ranges above {@link #NINTHER_THRESHOLD} groups) is moved to low
     * and used as the pivot. A single pass splits the range into
     * three regions: groups less than, equal to and greater than the pivot. Groups equal
     * to the pivot are parked at both ends during the scan and vacuumed into the middle
     * afterwards, so a run of duplicates collapses in one pass instead of producing the
     * one-element-off splits a strict two-way scheme degrades to on duplicate-heavy input.
     *
     * @return packed region boundaries: the low int is the exclusive end of the less-than
     * region, the high int is the inclusive start of the greater-than region; groups in
     * between equal the pivot and are already in their final position
     */
    private static long partition(long[] array, int low, int high, int n) {
        // Sampled pivot selection: a fixed-position pivot degrades to a worst-case split
        // on sorted and reverse-sorted input; a sampled median splits both roughly in
        // half. Large ranges use the Tukey ninther for robustness against structured
        // input; the scan guards below keep both variants in bounds.
        int hi = high - 1;
        int mid = (low + hi) >>> 1;
        int len = high - low;
        if (len > NINTHER_THRESHOLD) {
            int eps = len / 8;
            int m1 = median3(array, low, low + eps, low + 2 * eps, n);
            int m2 = median3(array, mid - eps, mid, mid + eps, n);
            int m3 = median3(array, hi - 2 * eps, hi - eps, hi, n);
            swapGroups(array, low, median3(array, m1, m2, m3, n), n);
        } else {
            // Order the groups at low, mid and hi in place, then move the median (now at
            // mid) to low, where it serves as the pivot. Ordering all three - rather than
            // only selecting the median - additionally parks the largest of the sample at
            // hi, which empirically keeps the scans balanced on structured input.
            if (compareGroups(array, mid, low, n) < 0) {
                swapGroups(array, mid, low, n);
            }
            if (compareGroups(array, hi, mid, n) < 0) {
                swapGroups(array, hi, mid, n);
                if (compareGroups(array, mid, low, n) < 0) {
                    swapGroups(array, mid, low, n);
                }
            }
            swapGroups(array, low, mid, n);
        }

        // The pivot group stays at low for the whole scan: i and j stop on the first group
        // they see that is >= / <= the pivot, equal groups are parked at [low + 1, p] and
        // [q, hi]. Once the scans cross, the parked groups (and the pivot itself) are
        // swapped into the middle.
        int i = low;
        int j = hi + 1;
        int p = low;
        int q = hi + 1;
        for (; ; ) {
            while (compareGroups(array, ++i, low, n) < 0) {
                if (i == hi) {
                    break;
                }
            }
            while (compareGroups(array, low, --j, n) < 0) {
                if (j == low) {
                    break;
                }
            }
            if (i == j && compareGroups(array, i, low, n) == 0) {
                swapGroups(array, ++p, i, n);
            }
            if (i >= j) {
                break;
            }
            swapGroups(array, i, j, n);
            if (compareGroups(array, i, low, n) == 0) {
                swapGroups(array, ++p, i, n);
            }
            if (compareGroups(array, j, low, n) == 0) {
                swapGroups(array, --q, j, n);
            }
        }

        i = j + 1;
        for (int k = low; k <= p; k++) {
            swapGroups(array, k, j--, n);
        }
        for (int k = hi; k >= q; k--) {
            swapGroups(array, k, i++, n);
        }
        // [low, j + 1) < pivot, [j + 1, i) == pivot, [i, high) > pivot
        return Numbers.encodeLowHighInts(j + 1, i);
    }

    private static void quickSortImpl(LongList longList, int low, int high, int n) {
        int stackStart = longList.size();
        int stackPos = stackStart;
        long[] array = longList.resetCapacityInternal(stackPos + STACK_CAPACITY);

        try {
            if (low + 1 >= high) {
                return;
            }

            // Fast-paths: a single O(count) scan detects already-sorted input (the common
            // shape for ascending IN-list / OR-ed timestamp disjuncts) as well as fully
            // descending input (newest-first lists), which is fixed up with an O(count)
            // in-place reversal. The scan aborts as soon as both orderings are ruled out,
            // so unstructured input pays only a handful of comparisons.
            boolean ascending = true;
            boolean descending = true;
            for (int g = low + 1; g < high && (ascending || descending); g++) {
                int cmp = compareGroups(array, g - 1, g, n);
                if (cmp > 0) {
                    ascending = false;
                } else if (cmp < 0) {
                    descending = false;
                }
            }
            if (ascending) {
                return;
            }
            if (descending) {
                // Reversing a descending run yields ascending order. Groups that compare
                // equal swap only with each other, which is indistinguishable in the
                // output, so the non-strict descending check is safe.
                for (int g1 = low, g2 = high - 1; g1 < g2; g1++, g2--) {
                    swapGroups(array, g1, g2, n);
                }
                return;
            }

            // Small inputs skip the partitioning machinery entirely; this is the common
            // case for interval / disjunct lists, which rarely exceed a few dozen groups.
            if (high - low <= INSERTION_SORT_THRESHOLD) {
                insertionSort(array, low, high, n);
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

                if (high - low <= INSERTION_SORT_THRESHOLD) {
                    insertionSort(array, low, high, n);
                    continue;
                }

                if (depth == 0) {
                    heapSort(array, low, high, n);
                    continue;
                }

                long regions = partition(array, low, high, n);
                int ltHi = Numbers.decodeLowInt(regions);
                int gtLo = Numbers.decodeHighInt(regions);
                depth--;

                // Push the larger partition first (defer it), the smaller one last
                // (process it next); this keeps the stack depth logarithmic. Groups equal
                // to the pivot occupy [ltHi, gtLo) and are already in final position.
                if (ltHi - low > high - gtLo) {
                    array[stackPos++] = low;
                    array[stackPos++] = ltHi;
                    array[stackPos++] = depth;
                    array[stackPos++] = gtLo;
                    array[stackPos++] = high;
                    array[stackPos++] = depth;
                } else {
                    array[stackPos++] = gtLo;
                    array[stackPos++] = high;
                    array[stackPos++] = depth;
                    array[stackPos++] = low;
                    array[stackPos++] = ltHi;
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
