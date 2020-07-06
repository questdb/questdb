/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

public class LongSort {
    /**
     * If the length of an array to be sorted is less than this
     * constant, Quicksort is used in preference to merge sort.
     */
    private static final int QUICKSORT_THRESHOLD = 286;

    /**
     * The maximum length of run in merge sort.
     */
    private static final int MAX_RUN_LENGTH = 33;

    /**
     * The maximum number of runs in merge sort.
     */
    private static final int MAX_RUN_COUNT = 67;

    /**
     * If the length of an array to be sorted is less than this
     * constant, insertion sort is used in preference to Quicksort.
     */
    private static final int INSERTION_SORT_THRESHOLD = 47;

    /**
     * Sorts the specified range of the array.
     *
     * @param left  the index of the first element, inclusive, to be sorted
     * @param right the index of the last element, inclusive, to be sorted
     */
    public static void sort(LongVec vec, int left, int right) {
        // Use Quicksort on small arrays
        if (right - left < QUICKSORT_THRESHOLD) {
            sort(vec, left, right, true);
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
            if (vec.getQuick(k) < vec.getQuick(k + 1)) { // ascending
                //noinspection StatementWithEmptyBody
                while (++k <= right && vec.getQuick(k - 1) <= vec.getQuick(k)) ;
            } else if (vec.getQuick(k) > vec.getQuick(k + 1)) { // descending
                //noinspection StatementWithEmptyBody
                while (++k <= right && vec.getQuick(k - 1) >= vec.getQuick(k)) ;
                for (int lo = run[count] - 1, hi = k; ++lo < --hi; ) {
                    swap(vec, lo, hi);
                }
            } else { // equal
                for (int m = MAX_RUN_LENGTH; ++k <= right && vec.getQuick(k - 1) == vec.getQuick(k); ) {
                    if (--m == 0) {
                        sort(vec, left, right, true);
                        return;
                    }
                }
            }

            /*
             * The array is not highly structured,
             * use Quicksort instead of merge sort.
             */
            if (++count == MAX_RUN_COUNT) {
                sort(vec, left, right, true);
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

        LongVec a;
        LongVec b;

        byte odd = 0;
        //noinspection StatementWithEmptyBody
        for (int n = 1; (n <<= 1) < count; odd ^= 1) ;

        if (odd == 0) {
            b = vec;
            a = vec.newInstance();

            //noinspection StatementWithEmptyBody
            for (int i = left - 1; ++i < right; a.setQuick(i, b.getQuick(i))) ;
        } else {
            a = vec;
            b = vec.newInstance();
        }

        // Merging
        for (int last; count > 1; count = last) {
            for (int k = (last = 0) + 2; k <= count; k += 2) {
                int hi = run[k], mi = run[k - 1];
                for (int i = run[k - 2], p = i, q = mi; i < hi; ++i) {
                    if (q >= hi || p < mi && vec.getQuick(p) <= vec.getQuick(q)) {
                        b.setQuick(i, a.getQuick(p++));
                    } else {
                        b.setQuick(i, a.getQuick(q++));
                    }
                }
                run[++last] = hi;
            }
            if ((count & 1) != 0) {
                //noinspection StatementWithEmptyBody
                for (int i = right, lo = run[count - 1]; --i >= lo; b.setQuick(i, a.getQuick(i))) ;
                run[++last] = right;
            }
            LongVec t = a;
            a = b;
            b = t;
        }
    }

    /**
     * Sorts the specified range of the array by Dual-Pivot Quicksort.
     *
     * @param left     the index of the first element, inclusive, to be sorted
     * @param right    the index of the last element, inclusive, to be sorted
     * @param leftmost indicates if this part is the leftmost in the range
     */
    private static void sort(LongVec vec, int left, int right, boolean leftmost) {
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
                    long ai = vec.getQuick(i + 1);
                    while (ai < vec.getQuick(j)) {
                        let(vec, j + 1, j);
                        if (j-- == left) {
                            break;
                        }
                    }
                    vec.setQuick(j + 1, ai);
                }
            } else {
                /*
                 * Skip the longest ascending sequence.
                 */
                do {
                    if (left >= right) {
                        return;
                    }
                } while (vec.getQuick(++left) >= vec.getQuick(left - 1));

                /*
                 * Every element from adjoining part plays the role
                 * of sentinel, therefore this allows us to avoid the
                 * left range check on each iteration. Moreover, we use
                 * the more optimized algorithm, so called pair insertion
                 * sort, which is faster (in the context of Quicksort)
                 * than traditional implementation of insertion sort.
                 */
                for (int k = left; ++left <= right; k = ++left) {
                    long a1 = vec.getQuick(k), a2 = vec.getQuick(left);

                    if (a1 < a2) {
                        a2 = a1;
                        a1 = vec.getQuick(left);
                    }
                    while (a1 < vec.getQuick(--k)) {
                        let(vec, k + 2, k);
                    }
                    vec.setQuick(++k + 1, a1);

                    while (a2 < vec.getQuick(--k)) {
                        let(vec, k + 1, k);
                    }
                    vec.setQuick(k + 1, a2);
                }
                long last = vec.getQuick(right);

                while (last < vec.getQuick(--right)) {
                    let(vec, right + 1, right);
                }
                vec.setQuick(right + 1, last);
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
        if (vec.getQuick(e2) < vec.getQuick(e1)) {
            swap(vec, e2, e1);
        }

        if (vec.getQuick(e3) < vec.getQuick(e2)) {
            long t = vec.getQuick(e3);
            let(vec, e3, e2);
            vec.setQuick(e2, t);
            if (t < vec.getQuick(e1)) {
                let(vec, e2, e1);
                vec.setQuick(e1, t);
            }
        }
        if (vec.getQuick(e4) < vec.getQuick(e3)) {
            long t = vec.getQuick(e4);
            let(vec, e4, e3);
            vec.setQuick(e3, t);
            if (t < vec.getQuick(e2)) {
                let(vec, e3, e2);
                vec.setQuick(e2, t);
                if (t < vec.getQuick(e1)) {
                    let(vec, e2, e1);
                    vec.setQuick(e1, t);
                }
            }
        }
        if (vec.getQuick(e5) < vec.getQuick(e4)) {
            long t = vec.getQuick(e5);
            let(vec, e5, e4);
            vec.setQuick(e4, t);
            if (t < vec.getQuick(e3)) {
                let(vec, e4, e3);
                vec.setQuick(e3, t);
                if (t < vec.getQuick(e2)) {
                    let(vec, e3, e2);
                    vec.setQuick(e2, t);
                    if (t < vec.getQuick(e1)) {
                        let(vec, e2, e1);
                        vec.setQuick(e1, t);
                    }
                }
            }
        }

        // Pointers
        int less = left;  // The index of the first element of center part
        int great = right; // The index before the first element of right part

        if (vec.getQuick(e1) != vec.getQuick(e2) && vec.getQuick(e2) != vec.getQuick(e3) && vec.getQuick(e3) != vec.getQuick(e4) && vec.getQuick(e4) != vec.getQuick(e5)) {
            /*
             * Use the second and fourth of the five sorted elements as pivots.
             * These values are inexpensive approximations of the first and
             * second terciles of the array. Note that pivot1 <= pivot2.
             */
            long pivot1 = vec.getQuick(e2);
            long pivot2 = vec.getQuick(e4);

            /*
             * The first and the last elements to be sorted are moved to the
             * locations formerly occupied by the pivots. When partitioning
             * is complete, the pivots are swapped back into their final
             * positions, and excluded from subsequent sorting.
             */
            let(vec, e2, left);
            let(vec, e4, right);

            /*
             * Skip elements, which are less or greater than pivot values.
             */
            //noinspection StatementWithEmptyBody
            while (vec.getQuick(++less) < pivot1) ;
            //noinspection StatementWithEmptyBody
            while (vec.getQuick(--great) > pivot2) ;

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
                long ak = vec.getQuick(k);
                if (ak < pivot1) { // Move a[k] to left part
                    let(vec, k, less);
                    /*
                     * Here and below we use "a[i] = b; i++;" instead
                     * of "a[i++] = b;" due to performance issue.
                     */
                    vec.setQuick(less, ak);
                    ++less;
                } else if (ak > pivot2) { // Move a[k] to right part
                    while (vec.getQuick(great) > pivot2) {
                        if (great-- == k) {
                            break outer;
                        }
                    }
                    if (vec.getQuick(great) < pivot1) { // a[great] <= pivot2
                        let(vec, k, less);
                        let(vec, less, great);
                        ++less;
                    } else { // pivot1 <= a[great] <= pivot2
                        let(vec, k, great);
                    }
                    /*
                     * Here and below we use "a[i] = b; i--;" instead
                     * of "a[i--] = b;" due to performance issue.
                     */
                    vec.setQuick(great, ak);
                    --great;
                }
            }

            // Swap pivots into their final positions
            let(vec, left, less - 1);
            vec.setQuick(less - 1, pivot1);
            let(vec, right, great + 1);
            vec.setQuick(great + 1, pivot2);

            // Sort left and right parts recursively, excluding known pivots
            sort(vec, left, less - 2, leftmost);
            sort(vec, great + 2, right, false);

            /*
             * If center part is too large (comprises > 4/7 of the array),
             * swap internal pivot values to ends.
             */
            if (less < e1 && e5 < great) {
                /*
                 * Skip elements, which are equal to pivot values.
                 */
                while (vec.getQuick(less) == pivot1) {
                    ++less;
                }

                while (vec.getQuick(great) == pivot2) {
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
                    long ak = vec.getQuick(k);
                    if (ak == pivot1) { // Move a[k] to left part
                        let(vec, k, less);
                        vec.setQuick(less, ak);
                        ++less;
                    } else if (ak == pivot2) { // Move a[k] to right part
                        while (vec.getQuick(great) == pivot2) {
                            if (great-- == k) {
                                break outer;
                            }
                        }
                        if (vec.getQuick(great) == pivot1) { // a[great] < pivot2
                            let(vec, k, less);
                            /*
                             * Even though a[great] equals to pivot1, the
                             * assignment a[less] = pivot1 may be incorrect,
                             * if a[great] and pivot1 are floating-point zeros
                             * of different signs. Therefore in float and
                             * double sorting methods we have to use more
                             * accurate assignment a[less] = a[great].
                             */
                            vec.setQuick(less, pivot1);
                            ++less;
                        } else { // pivot1 < a[great] < pivot2
                            let(vec, k, great);
                        }
                        vec.setQuick(great, ak);
                        --great;
                    }
                }
            }

            // Sort center part recursively
            sort(vec, less, great, false);

        } else { // Partitioning with one pivot
            /*
             * Use the third of the five sorted elements as pivot.
             * This value is inexpensive approximation of the median.
             */
            long pivot = vec.getQuick(e3);

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
                if (vec.getQuick(k) == pivot) {
                    continue;
                }
                long ak = vec.getQuick(k);
                if (ak < pivot) { // Move a[k] to left part
                    let(vec, k, less);
                    vec.setQuick(less, ak);
                    ++less;
                } else { // a[k] > pivot - Move a[k] to right part
                    while (vec.getQuick(great) > pivot) {
                        --great;
                    }
                    if (vec.getQuick(great) < pivot) { // a[great] <= pivot
                        let(vec, k, less);
                        let(vec, less, great);
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
                        vec.setQuick(k, pivot);
                    }
                    vec.setQuick(great, ak);
                    --great;
                }
            }

            /*
             * Sort left and right parts recursively.
             * All elements from center part are equal
             * and, therefore, already sorted.
             */
            sort(vec, left, less - 1, leftmost);
            sort(vec, great + 1, right, false);
        }
    }

    private static void swap(LongVec vec, int a, int b) {
        long tmp = vec.getQuick(a);
        vec.setQuick(a, vec.getQuick(b));
        vec.setQuick(b, tmp);
    }

    private static void let(LongVec vec, int a, int b) {
        vec.setQuick(a, vec.getQuick(b));
    }
}
