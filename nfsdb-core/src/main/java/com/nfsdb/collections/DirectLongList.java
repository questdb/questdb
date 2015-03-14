/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.collections;

import com.nfsdb.utils.Rnd;
import com.nfsdb.utils.Unsafe;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class DirectLongList extends AbstractDirectList {
    /**
     * The maximum number of runs in merge sort.
     */
    private static final int MAX_RUN_COUNT = 67;
    /**
     * The maximum length of run in merge sort.
     */
    private static final int MAX_RUN_LENGTH = 33;
    /**
     * If the length of an array to be sorted is less than this
     * constant, Quicksort is used in preference to merge sort.
     */
    private static final int QUICKSORT_THRESHOLD = 286;
    /**
     * If the length of an array to be sorted is less than this
     * constant, insertion sort is used in preference to Quicksort.
     */
    private static final int INSERTION_SORT_THRESHOLD = 47;

    /**
     * If the length of a byte array to be sorted is greater than this
     * constant, counting sort is used in preference to insertion sort.
     */

    public DirectLongList() {
        super(3, 10);
    }

    public DirectLongList(long capacity) {
        super(3, capacity);
    }

    public DirectLongList(DirectLongList that) {
        super(3, that);
    }

    public void add(long x) {
        ensureCapacity();
        Unsafe.getUnsafe().putLong(pos, x);
        pos += 8;
    }

    public int binarySearch(long v) {
        int low = 0;
        int high = (int) ((pos - start) >> 3) - 1;

        while (low <= high) {

            if (high - low <= 64) {
                return scanSearch(v);
            }

            int mid = (low + high) >>> 1;
            long midVal = Unsafe.getUnsafe().getLong(start + (mid << 3));

            if (midVal < v)
                low = mid + 1;
            else if (midVal > v)
                high = mid - 1;
            else
                return mid;
        }
        return -(low + 1);
    }

    public long get(long p) {
        return Unsafe.getUnsafe().getLong(start + (p << 3));
    }

    public int scanSearch(long v) {
        int sz = size();
        for (int i = 0; i < sz; i++) {
            long f = get(i);
            if (f == v) {
                return i;
            }
            if (f > v) {
                return -(i + 1);
            }
        }
        return -(sz + 1);
    }

    public void set(long p, long v) {
        assert p >= 0 && p <= (limit - start) >> 3;
        Unsafe.getUnsafe().putLong(start + (p << 3), v);
    }

    public DirectLongList shuffle(Rnd rnd) {
        for (int i = 0, sz = size(); i < sz; i++) {
            swap(i, rnd.nextPositiveInt() & (sz - 1));
        }
        return this;
    }

    /**
     * Sorts the specified array.
     */
    public void sort() {
        sort(0, size() - 1);
    }

    /**
     * Sorts the specified range of the array.
     *
     * @param left  the index of the first element, inclusive, to be sorted
     * @param right the index of the last element, inclusive, to be sorted
     */
    public void sort(int left, int right) {
        // Use Quicksort on small arrays
        if (right - left < QUICKSORT_THRESHOLD) {
            sort(left, right, true);
            return;
        }

        /*
         * Index run[i] is the start of i-th run
         * (ascending or descending sequence).
         */
        int[] run = new int[MAX_RUN_COUNT + 1];
        int count = 0;
        run[0] = left;

        // Check if the array is nearly sorted
        for (int k = left; k < right; run[count] = k) {
            if (get(k) < get(k + 1)) { // ascending
                //noinspection StatementWithEmptyBody
                while (++k <= right && get(k - 1) <= get(k)) ;
            } else if (get(k) > get(k + 1)) { // descending
                //noinspection StatementWithEmptyBody
                while (++k <= right && get(k - 1) >= get(k)) ;
                for (int lo = run[count] - 1, hi = k; ++lo < --hi; ) {
                    swap(lo, hi);
                }
            } else { // equal
                for (int m = MAX_RUN_LENGTH; ++k <= right && get(k - 1) == get(k); ) {
                    if (--m == 0) {
                        sort(left, right, true);
                        return;
                    }
                }
            }

            /*
             * The array is not highly structured,
             * use Quicksort instead of merge sort.
             */
            if (++count == MAX_RUN_COUNT) {
                sort(left, right, true);
                return;
            }
        }

        // Check special cases
        if (run[count] == right++) { // The last run contains one element
            run[++count] = right;
        } else if (count == 1) { // The array is already sorted
            return;
        }

        /*
         * Create temporary array, which is used for merging.
         * Implementation note: variable "right" is increased by 1.
         */

        DirectLongList a;
        DirectLongList b;

        //long[] b;
        byte odd = 0;
        //noinspection StatementWithEmptyBody
        for (int n = 1; (n <<= 1) < count; odd ^= 1) ;

        if (odd == 0) {
            b = this;
            a = new DirectLongList(this.size());

            //noinspection StatementWithEmptyBody
            for (int i = left - 1; ++i < right; a.set(i, b.get(i))) ;
        } else {
            a = this;
            b = new DirectLongList(this.size());
        }

        // Merging
        for (int last; count > 1; count = last) {
            for (int k = (last = 0) + 2; k <= count; k += 2) {
                int hi = run[k], mi = run[k - 1];
                for (int i = run[k - 2], p = i, q = mi; i < hi; ++i) {
                    if (q >= hi || p < mi && get(p) <= get(q)) {
                        b.set(i, a.get(p++));
                    } else {
                        b.set(i, a.get(q++));
                    }
                }
                run[++last] = hi;
            }
            if ((count & 1) != 0) {
                //noinspection StatementWithEmptyBody
                for (int i = right, lo = run[count - 1]; --i >= lo; b.set(i, a.get(i))) ;
                run[++last] = right;
            }
            DirectLongList t = a;
            a = b;
            b = t;
        }
    }

    public DirectLongList subset(int lo, int hi) {
        DirectLongList that = new DirectLongList(hi - lo);
        Unsafe.getUnsafe().copyMemory(start + (lo << 3), that.start, (hi - lo) << 3);
        that.pos += (hi - lo) << 3;
        return that;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (int i = 0; i < size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(get(i));
        }
        sb.append("}");
        return sb.toString();
    }

    private void swap(int a, int b) {
        long tmp = Unsafe.getUnsafe().getLong(start + (a << 3));
        Unsafe.getUnsafe().copyMemory(start + (b << 3), start + (a << 3), 8);
        Unsafe.getUnsafe().putLong(start + (b << 3), tmp);

    }

    private void let(int a, int b) {
        Unsafe.getUnsafe().putLong(start + (a << 3), Unsafe.getUnsafe().getLong(start + (b << 3)));
    }

    /**
     * Sorts the specified range of the array by Dual-Pivot Quicksort.
     *
     * @param left     the index of the first element, inclusive, to be sorted
     * @param right    the index of the last element, inclusive, to be sorted
     * @param leftmost indicates if this part is the leftmost in the range
     */
    @SuppressFBWarnings({"CC_CYCLOMATIC_COMPLEXITY"})
    private void sort(int left, int right, boolean leftmost) {
        int length = right - left + 1;

        // Use insertion sort on tiny arrays
        if (length < INSERTION_SORT_THRESHOLD) {
            if (leftmost) {
                /*
                 * Traditional (without sentinel) insertion sort,
                 * optimized for server VM, is used in case of
                 * the leftmost part.
                 */
                for (int i = left, j = i; i < right; j = ++i) {
                    long ai = get(i + 1);
                    while (ai < get(j)) {
                        let(j + 1, j);
                        if (j-- == left) {
                            break;
                        }
                    }
                    set(j + 1, ai);
                }
            } else {
                /*
                 * Skip the longest ascending sequence.
                 */
                do {
                    if (left >= right) {
                        return;
                    }
                } while (get(++left) >= get(left - 1));

                /*
                 * Every element from adjoining part plays the role
                 * of sentinel, therefore this allows us to avoid the
                 * left range check on each iteration. Moreover, we use
                 * the more optimized algorithm, so called pair insertion
                 * sort, which is faster (in the context of Quicksort)
                 * than traditional implementation of insertion sort.
                 */
                for (int k = left; ++left <= right; k = ++left) {
                    long a1 = get(k), a2 = get(left);

                    if (a1 < a2) {
                        a2 = a1;
                        a1 = get(left);
                    }
                    while (a1 < get(--k)) {
                        let(k + 2, k);
                    }
                    set(++k + 1, a1);

                    while (a2 < get(--k)) {
                        let(k + 1, k);
                    }
                    set(k + 1, a2);
                }
                long last = get(right);

                while (last < get(--right)) {
                    let(right + 1, right);
                }
                set(right + 1, last);
            }
            return;
        }

        // Inexpensive approximation of length / 7
        int seventh = (length >> 3) + (length >> 6) + 1;

        /*
         * Sort five evenly spaced elements around (and including) the
         * center element in the range. These elements will be used for
         * pivot selection as described below. The choice for spacing
         * these elements was empirically determined to work well on
         * a wide variety of inputs.
         */
        int e3 = (left + right) >>> 1; // The midpoint
        int e2 = e3 - seventh;
        int e1 = e2 - seventh;
        int e4 = e3 + seventh;
        int e5 = e4 + seventh;

        // Sort these elements using insertion sort
        if (get(e2) < get(e1)) {
            swap(e2, e1);
        }

        if (get(e3) < get(e2)) {
            long t = get(e3);
            let(e3, e2);
            set(e2, t);
            if (t < get(e1)) {
                let(e2, e1);
                set(e1, t);
            }
        }
        if (get(e4) < get(e3)) {
            long t = get(e4);
            let(e4, e3);
            set(e3, t);
            if (t < get(e2)) {
                let(e3, e2);
                set(e2, t);
                if (t < get(e1)) {
                    let(e2, e1);
                    set(e1, t);
                }
            }
        }
        if (get(e5) < get(e4)) {
            long t = get(e5);
            let(e5, e4);
            set(e4, t);
            if (t < get(e3)) {
                let(e4, e3);
                set(e3, t);
                if (t < get(e2)) {
                    let(e3, e2);
                    set(e2, t);
                    if (t < get(e1)) {
                        let(e2, e1);
                        set(e1, t);
                    }
                }
            }
        }

        // Pointers
        int less = left;  // The index of the first element of center part
        int great = right; // The index before the first element of right part

        if (get(e1) != get(e2) && get(e2) != get(e3) && get(e3) != get(e4) && get(e4) != get(e5)) {
            /*
             * Use the second and fourth of the five sorted elements as pivots.
             * These values are inexpensive approximations of the first and
             * second terciles of the array. Note that pivot1 <= pivot2.
             */
            long pivot1 = get(e2);
            long pivot2 = get(e4);

            /*
             * The first and the last elements to be sorted are moved to the
             * locations formerly occupied by the pivots. When partitioning
             * is complete, the pivots are swapped back into their final
             * positions, and excluded from subsequent sorting.
             */
            let(e2, left);
            let(e4, right);

            /*
             * Skip elements, which are less or greater than pivot values.
             */
            //noinspection StatementWithEmptyBody
            while (get(++less) < pivot1) ;
            //noinspection StatementWithEmptyBody
            while (get(--great) > pivot2) ;

            /*
             * Partitioning:
             *
             *   left part           center part                   right part
             * +--------------------------------------------------------------+
             * |  < pivot1  |  pivot1 <= && <= pivot2  |    ?    |  > pivot2  |
             * +--------------------------------------------------------------+
             *               ^                          ^       ^
             *               |                          |       |
             *              less                        k     great
             *
             * Invariants:
             *
             *              all in (left, less)   < pivot1
             *    pivot1 <= all in [less, k)     <= pivot2
             *              all in (great, right) > pivot2
             *
             * Pointer k is the first index of ?-part.
             */
            outer:
            for (int k = less - 1; ++k <= great; ) {
                long ak = get(k);
                if (ak < pivot1) { // Move a[k] to left part
                    let(k, less);
                    /*
                     * Here and below we use "a[i] = b; i++;" instead
                     * of "a[i++] = b;" due to performance issue.
                     */
                    set(less, ak);
                    ++less;
                } else if (ak > pivot2) { // Move a[k] to right part
                    while (get(great) > pivot2) {
                        if (great-- == k) {
                            break outer;
                        }
                    }
                    if (get(great) < pivot1) { // a[great] <= pivot2
                        let(k, less);
                        let(less, great);
                        ++less;
                    } else { // pivot1 <= a[great] <= pivot2
                        let(k, great);
                    }
                    /*
                     * Here and below we use "a[i] = b; i--;" instead
                     * of "a[i--] = b;" due to performance issue.
                     */
                    set(great, ak);
                    --great;
                }
            }

            // Swap pivots into their final positions
            let(left, less - 1);
            set(less - 1, pivot1);
            let(right, great + 1);
            set(great + 1, pivot2);

            // Sort left and right parts recursively, excluding known pivots
            sort(left, less - 2, leftmost);
            sort(great + 2, right, false);

            /*
             * If center part is too large (comprises > 4/7 of the array),
             * swap internal pivot values to ends.
             */
            if (less < e1 && e5 < great) {
                /*
                 * Skip elements, which are equal to pivot values.
                 */
                while (get(less) == pivot1) {
                    ++less;
                }

                while (get(great) == pivot2) {
                    --great;
                }

                /*
                 * Partitioning:
                 *
                 *   left part         center part                  right part
                 * +----------------------------------------------------------+
                 * | == pivot1 |  pivot1 < && < pivot2  |    ?    | == pivot2 |
                 * +----------------------------------------------------------+
                 *              ^                        ^       ^
                 *              |                        |       |
                 *             less                      k     great
                 *
                 * Invariants:
                 *
                 *              all in (*,  less) == pivot1
                 *     pivot1 < all in [less,  k)  < pivot2
                 *              all in (great, *) == pivot2
                 *
                 * Pointer k is the first index of ?-part.
                 */
                outer:
                for (int k = less - 1; ++k <= great; ) {
                    long ak = get(k);
                    if (ak == pivot1) { // Move a[k] to left part
                        let(k, less);
                        set(less, ak);
                        ++less;
                    } else if (ak == pivot2) { // Move a[k] to right part
                        while (get(great) == pivot2) {
                            if (great-- == k) {
                                break outer;
                            }
                        }
                        if (get(great) == pivot1) { // a[great] < pivot2
                            let(k, less);
                            /*
                             * Even though a[great] equals to pivot1, the
                             * assignment a[less] = pivot1 may be incorrect,
                             * if a[great] and pivot1 are floating-point zeros
                             * of different signs. Therefore in float and
                             * double sorting methods we have to use more
                             * accurate assignment a[less] = a[great].
                             */
                            set(less, pivot1);
                            ++less;
                        } else { // pivot1 < a[great] < pivot2
                            let(k, great);
                        }
                        set(great, ak);
                        --great;
                    }
                }
            }

            // Sort center part recursively
            sort(less, great, false);

        } else { // Partitioning with one pivot
            /*
             * Use the third of the five sorted elements as pivot.
             * This value is inexpensive approximation of the median.
             */
            long pivot = get(e3);

            /*
             * Partitioning degenerates to the traditional 3-way
             * (or "Dutch National Flag") schema:
             *
             *   left part    center part              right part
             * +-------------------------------------------------+
             * |  < pivot  |   == pivot   |     ?    |  > pivot  |
             * +-------------------------------------------------+
             *              ^              ^        ^
             *              |              |        |
             *             less            k      great
             *
             * Invariants:
             *
             *   all in (left, less)   < pivot
             *   all in [less, k)     == pivot
             *   all in (great, right) > pivot
             *
             * Pointer k is the first index of ?-part.
             */
            for (int k = less; k <= great; ++k) {
                if (get(k) == pivot) {
                    continue;
                }
                long ak = get(k);
                if (ak < pivot) { // Move a[k] to left part
                    let(k, less);
                    set(less, ak);
                    ++less;
                } else { // a[k] > pivot - Move a[k] to right part
                    while (get(great) > pivot) {
                        --great;
                    }
                    if (get(great) < pivot) { // a[great] <= pivot
                        let(k, less);
                        let(less, great);
                        ++less;
                    } else { // a[great] == pivot
                        /*
                         * Even though a[great] equals to pivot, the
                         * assignment a[k] = pivot may be incorrect,
                         * if a[great] and pivot are floating-point
                         * zeros of different signs. Therefore in float
                         * and double sorting methods we have to use
                         * more accurate assignment a[k] = a[great].
                         */
                        set(k, pivot);
                    }
                    set(great, ak);
                    --great;
                }
            }

            /*
             * Sort left and right parts recursively.
             * All elements from center part are equal
             * and, therefore, already sorted.
             */
            sort(left, less - 1, leftmost);
            sort(great + 1, right, false);
        }
    }
}
