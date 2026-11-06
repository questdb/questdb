/*******************************************************************************
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
 * Off-heap double sort and selection utility. Both run an introsort-style quicksort
 * with Dutch National Flag (three-way) partitioning: elements equal to the pivot land
 * in their final position in one pass, so all-equal data costs O(N). The pivot is the
 * median of three samples, or a Tukey ninther on ranges above
 * {@link #NINTHER_THRESHOLD} elements. A partition chain that runs deeper than
 * 2*log2(N) levels finishes its range with heapsort, which bounds the worst case at
 * O(N log N) for every input shape. Sorting falls back to insertion sort for
 * subarrays smaller than {@link #INSERTION_SORT_THRESHOLD}.
 * <p>
 * Memory is accessed through raw native pointers via {@link Unsafe}. The caller
 * must ensure that {@code ptr + index * 8L} is valid for every index in
 * {@code [left, right]}. The values must not contain NaN.
 */
public final class DoubleSort {
    // radixSelect(): the bits a step counts by, and the range below which quickSelect() finishes
    private static final int RADIX_SELECT_BITS = 11;
    private static final long RADIX_SELECT_MIN_RANGE = 1024;
    // radixSelect(): a range above this size takes its first step by the order keys' highest bits
    private static final int RADIX_SELECT_WIDE_BITS = 16;
    private static final long RADIX_SELECT_WIDE_RANGE = 1 << 18;

    private static final int INSERTION_SORT_THRESHOLD = 47;
    // Ranges above this size pick the pivot with a Tukey ninther (median of the medians
    // of three spaced triples). A single median-of-three sample picks the minimum of a
    // rise-then-fall range, which made each partition pass strip only a few elements.
    private static final int NINTHER_THRESHOLD = 128;
    // Test-only observability for the heapsort fallback. heapSort() increments the
    // counter on entry, a cold path reached only when a partition chain exhausts the
    // depth budget. Tests read it on the thread that ran the sort and assert on deltas.
    private static long heapSortCallCount;

    private DoubleSort() {
    }

    /**
     * Returns the number of heapsort-fallback activations since class load. The counter
     * is monotonic: capture it before a sort or selection and assert on the delta afterwards.
     *
     * @return count of heapsort-fallback activations
     */
    @TestOnly
    public static long getHeapSortCallCountForTesting() {
        return heapSortCallCount;
    }

    /**
     * Selects the k-th smallest element of the range. After this method returns, the
     * element at index k is in its final sorted position, all elements before it are
     * less than or equal to it, and all elements after it are greater than or equal to it.
     * O(N) on average, O(N log N) worst case.
     *
     * @param ptr native address of element[0]
     * @param lo  the index of the first element, inclusive
     * @param hi  the index of the last element, inclusive
     * @param k   the index of the element to select, within {@code [lo, hi]}
     */
    public static void quickSelect(long ptr, long lo, long hi, long k) {
        int depth = depthBudget(hi - lo + 1);
        while (lo < hi) {
            if (depth == 0) {
                heapSort(ptr, lo, hi);
                return;
            }
            depth--;
            long lt = partition(ptr, lo, hi);
            long gt = equalRunEnd(ptr, lt, hi);
            if (k < lt) {
                hi = lt - 1;
            } else if (k > gt) {
                lo = gt + 1;
            } else {
                return; // k is in the equal partition
            }
        }
    }

    /**
     * The k-th smallest (0-based, k in [lo, hi]) of the finite doubles at [lo, hi] (inclusive), by radix
     * selection: each step counts the range's values by the 11 highest bits their order keys still differ
     * in (a large range's first step by their 16 highest bits), moves those of the k-th's bucket to the
     * range's front and narrows to them, until the range is small or its values are equal. The range holds
     * fewer than 2^31 values. Linear in the range whatever its distribution, many equal values
     * included. Reorders the elements; unlike {@link #quickSelect} it does not leave the k-th at index k.
     */
    public static double radixSelect(long ptr, long lo, long hi, long k) {
        return radixSelect(ptr, lo, hi, k, null);
    }

    /**
     * {@link #radixSelect}, and into {@code next[0]} the (k+1)-th smallest (k in [lo, hi)), found on the way:
     * the smallest value above the k-th's bucket when the k-th is its bucket's last, else within the
     * narrowed range.
     */
    public static double radixSelect(long ptr, long lo, long hi, long k, double[] next) {
        long l = lo;
        long h = hi + 1; // exclusive
        long kk = k - lo; // the rank within [l, h)
        // the smallest value above the narrowed range's: the (k+1)-th once the k-th is the range's last
        double above = Double.POSITIVE_INFINITY;
        if (h - l <= RADIX_SELECT_MIN_RANGE) {
            return finishSelect(ptr, l, h, kk, above, next);
        }
        // a large range's first step counts by the order keys' 16 highest bits, without first reading the
        // range for the bits its keys differ in; each later step counts by the 11 highest bits the narrowed
        // range's keys differ in, found as the previous step moved them
        final boolean isWide = h - l > RADIX_SELECT_WIDE_RANGE;
        int bits = isWide ? RADIX_SELECT_WIDE_BITS : RADIX_SELECT_BITS;
        int shift = 64 - bits;
        final int[] counts = new int[1 << (isWide ? RADIX_SELECT_WIDE_BITS : RADIX_SELECT_BITS)];
        if (!isWide) {
            final long first = orderKey(getDouble(ptr, l));
            long varying = 0;
            for (long i = l + 1; i < h; i++) {
                varying |= orderKey(getDouble(ptr, i)) ^ first;
            }
            if (varying == 0) {
                return equalRange(ptr, l, h, kk, above, next);
            }
            shift = Math.max(0, 63 - Long.numberOfLeadingZeros(varying) - (bits - 1));
        }
        while (true) {
            final long mask = (1L << bits) - 1;
            java.util.Arrays.fill(counts, 0, 1 << bits, 0);
            for (long i = l; i < h; i++) {
                counts[(int) ((orderKey(getDouble(ptr, i)) >>> shift) & mask)]++;
            }
            int bucket = 0;
            while (kk >= counts[bucket]) {
                kk -= counts[bucket++];
            }
            final boolean isLast = kk == counts[bucket] - 1;
            // the bucket's values to the range's front, and the smallest value above the bucket; the bits
            // the moved keys differ in are those set in some and clear in others
            long w = l;
            double minAbove = Double.POSITIVE_INFINITY;
            long anySet = 0;
            long allSet = -1;
            for (long i = l; i < h; i++) {
                final double v = getDouble(ptr, i);
                final long key = orderKey(v);
                final long digit = (key >>> shift) & mask;
                if (digit == bucket) {
                    if (i != w) {
                        putDouble(ptr, i, getDouble(ptr, w));
                        putDouble(ptr, w, v);
                    }
                    w++;
                    anySet |= key;
                    allSet &= key;
                } else if (isLast && digit > bucket && v < minAbove) {
                    minAbove = v;
                }
            }
            if (isLast) {
                above = Math.min(above, minAbove);
            }
            h = w;
            final long varying = anySet & ~allSet;
            if (varying == 0) {
                return equalRange(ptr, l, h, kk, above, next);
            }
            if (h - l <= RADIX_SELECT_MIN_RANGE) {
                return finishSelect(ptr, l, h, kk, above, next);
            }
            bits = RADIX_SELECT_BITS;
            shift = Math.max(0, 63 - Long.numberOfLeadingZeros(varying) - (bits - 1));
        }
    }

    // the k-th (rank kk) of the range [l, h) whose values are all equal
    private static double equalRange(long ptr, long l, long h, long kk, double above, double[] next) {
        final double v = getDouble(ptr, l);
        if (next != null) {
            next[0] = kk + 1 < h - l ? v : above;
        }
        return v;
    }

    // the k-th (rank kk) of the small range [l, h), by quickSelect
    private static double finishSelect(long ptr, long l, long h, long kk, double above, double[] next) {
        quickSelect(ptr, l, h - 1, l + kk);
        final double kth = getDouble(ptr, l + kk);
        if (next != null) {
            // quickSelect leaves the values after the k-th's index at least it
            double n = above;
            for (long i = l + kk + 1; i < h; i++) {
                n = Math.min(n, getDouble(ptr, i));
            }
            next[0] = n;
        }
        return kth;
    }

    /**
     * Selects several order statistics at once. Partitions only the ranges that contain
     * a requested index. After this method returns, the elements at all requested
     * indices are in their final sorted positions.
     *
     * @param ptr     native address of element[0]
     * @param lo      the index of the first element, inclusive
     * @param hi      the index of the last element, inclusive
     * @param indices indices to select, sorted in ascending order, each within {@code [lo, hi]}
     * @param from    start position in the indices array, inclusive
     * @param to      end position in the indices array, exclusive
     */
    public static void quickSelectMultiple(long ptr, long lo, long hi, int[] indices, int from, int to) {
        quickSelectMultiple(ptr, lo, hi, indices, from, to, depthBudget(hi - lo + 1));
    }

    /**
     * Sorts doubles stored in native memory in ascending order.
     *
     * @param ptr   native address of element[0]
     * @param left  the index of the first element, inclusive, to be sorted
     * @param right the index of the last element, inclusive, to be sorted
     */
    public static void sort(long ptr, long left, long right) {
        sort(ptr, left, right, depthBudget(right - left + 1));
    }

    // 2 * (floor(log2(length)) + 1) partition levels before the heapsort fallback
    private static int depthBudget(long length) {
        return length > 0 ? 2 * (64 - Long.numberOfLeadingZeros(length)) : 0;
    }

    // Returns the last index of the run of pivot-equal elements that partition() left
    // at [lt, ...]. Nothing right of lt is less than the pivot, so the run ends at the
    // first element greater than the pivot.
    private static long equalRunEnd(long ptr, long lt, long hi) {
        final double pivot = getDouble(ptr, lt);
        long gt = lt;
        while (gt < hi && !(getDouble(ptr, gt + 1) > pivot)) {
            gt++;
        }
        return gt;
    }

    // the double's bits as an unsigned order key, -0.0 as 0.0 (they compare equal)
    private static long orderKey(double v) {
        final long bits = Double.doubleToRawLongBits(v + 0.0);
        return bits ^ ((bits >> 63) | Long.MIN_VALUE);
    }

    private static double getDouble(long ptr, long index) {
        return Unsafe.getUnsafe().getDouble(ptr + index * 8L);
    }

    private static void heapSort(long ptr, long lo, long hi) {
        heapSortCallCount++;
        final long count = hi - lo + 1;
        for (long i = (count >> 1) - 1; i >= 0; i--) {
            siftDown(ptr, lo, i, count);
        }
        for (long end = count - 1; end > 0; end--) {
            swap(ptr, lo, lo + end);
            siftDown(ptr, lo, 0, end);
        }
    }

    private static void insertionSort(long ptr, long left, long right) {
        for (long i = left + 1; i <= right; i++) {
            double key = getDouble(ptr, i);
            long j = i - 1;
            while (j >= left && getDouble(ptr, j) > key) {
                putDouble(ptr, j + 1, getDouble(ptr, j));
                j--;
            }
            putDouble(ptr, j + 1, key);
        }
    }

    private static long medianOfThree(long ptr, long a, long b, long c) {
        double va = getDouble(ptr, a);
        double vb = getDouble(ptr, b);
        double vc = getDouble(ptr, c);
        if (va <= vb) {
            if (vb <= vc) {
                return b;
            } else {
                return va <= vc ? c : a;
            }
        } else {
            if (va <= vc) {
                return a;
            } else {
                return vb <= vc ? c : b;
            }
        }
    }

    /**
     * Dutch National Flag three-way partition of {@code [lo, hi]} (lo < hi) around a
     * sampled pivot. Afterwards {@code [lo, lt)} is less than the pivot, the pivot-equal
     * run starts at lt and ends at {@link #equalRunEnd}, and the rest is greater.
     *
     * @return lt, the index of the first pivot-equal element
     */
    private static long partition(long ptr, long lo, long hi) {
        final long mid = lo + (hi - lo) / 2;
        final long length = hi - lo + 1;
        final long pivotIndex;
        if (length > NINTHER_THRESHOLD) {
            final long eps = length / 8;
            pivotIndex = medianOfThree(
                    ptr,
                    medianOfThree(ptr, lo, lo + eps, lo + 2 * eps),
                    medianOfThree(ptr, mid - eps, mid, mid + eps),
                    medianOfThree(ptr, hi - 2 * eps, hi - eps, hi)
            );
        } else {
            pivotIndex = medianOfThree(ptr, lo, mid, hi);
        }
        final double pivot = getDouble(ptr, pivotIndex);

        //   [lo, lt)   < pivot
        //   [lt, i)   == pivot
        //   (gt, hi]   > pivot
        //   [i, gt]      unexamined
        long lt = lo;
        long gt = hi;
        long i = lo;
        while (i <= gt) {
            double val = getDouble(ptr, i);
            if (val < pivot) {
                swap(ptr, i, lt);
                lt++;
                i++;
            } else if (val > pivot) {
                swap(ptr, i, gt);
                gt--;
            } else {
                i++;
            }
        }
        return lt;
    }

    private static void putDouble(long ptr, long index, double value) {
        Unsafe.getUnsafe().putDouble(ptr + index * 8L, value);
    }

    private static void quickSelectMultiple(long ptr, long lo, long hi, int[] indices, int from, int to, int depth) {
        while (from < to && lo < hi) {
            if (depth == 0) {
                // sorting the range places every requested index in it
                heapSort(ptr, lo, hi);
                return;
            }
            depth--;
            long lt = partition(ptr, lo, hi);
            long gt = equalRunEnd(ptr, lt, hi);

            // Find which indices fall into each region: <lt, [lt..gt], >gt
            int splitLeft = from;
            while (splitLeft < to && indices[splitLeft] < lt) {
                splitLeft++;
            }

            // Skip indices in the equal partition [lt..gt], they are already in place
            int splitRight = splitLeft;
            while (splitRight < to && indices[splitRight] <= gt) {
                splitRight++;
            }

            boolean hasLeft = splitLeft > from;
            boolean hasRight = splitRight < to;

            if (hasLeft && hasRight) {
                // recurse into the smaller side, iterate on the larger to limit stack depth
                if ((lt - lo) <= (hi - gt)) {
                    quickSelectMultiple(ptr, lo, lt - 1, indices, from, splitLeft, depth);
                    lo = gt + 1;
                    from = splitRight;
                } else {
                    quickSelectMultiple(ptr, gt + 1, hi, indices, splitRight, to, depth);
                    hi = lt - 1;
                    to = splitLeft;
                }
            } else if (hasLeft) {
                hi = lt - 1;
                to = splitLeft;
            } else if (hasRight) {
                lo = gt + 1;
                from = splitRight;
            } else {
                break;
            }
        }
    }

    private static void siftDown(long ptr, long lo, long root, long count) {
        final double value = getDouble(ptr, lo + root);
        for (; ; ) {
            long child = 2 * root + 1;
            if (child >= count) {
                break;
            }
            if (child + 1 < count && getDouble(ptr, lo + child) < getDouble(ptr, lo + child + 1)) {
                child++;
            }
            final double childValue = getDouble(ptr, lo + child);
            if (value < childValue) {
                putDouble(ptr, lo + root, childValue);
                root = child;
            } else {
                break;
            }
        }
        putDouble(ptr, lo + root, value);
    }

    private static void sort(long ptr, long lo, long hi, int depth) {
        while (lo < hi) {
            if (hi - lo + 1 < INSERTION_SORT_THRESHOLD) {
                insertionSort(ptr, lo, hi);
                return;
            }
            if (depth == 0) {
                heapSort(ptr, lo, hi);
                return;
            }
            depth--;
            long lt = partition(ptr, lo, hi);
            long gt = equalRunEnd(ptr, lt, hi);
            // Now: [lo, lt) < pivot, [lt, gt] == pivot, (gt, hi] > pivot.
            // Recurse on the smaller side, iterate on the larger to limit stack depth.
            if (lt - lo < hi - gt) {
                sort(ptr, lo, lt - 1, depth);
                lo = gt + 1;
            } else {
                sort(ptr, gt + 1, hi, depth);
                hi = lt - 1;
            }
        }
    }

    private static void swap(long ptr, long i, long j) {
        double tmp = getDouble(ptr, i);
        putDouble(ptr, i, getDouble(ptr, j));
        putDouble(ptr, j, tmp);
    }
}
