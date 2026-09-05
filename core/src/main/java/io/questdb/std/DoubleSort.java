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

/**
 * Off-heap double quicksort utility using Dutch National Flag (three-way)
 * partitioning. Elements equal to the pivot are skipped, preventing O(N^2)
 * degradation on all-equal data. Falls back to insertion sort for subarrays
 * smaller than {@link #INSERTION_SORT_THRESHOLD}.
 * <p>
 * Memory is accessed through raw native pointers via {@link Unsafe}. The caller
 * must ensure that {@code ptr + index * 8L} is valid for every index in
 * {@code [left, right]}.
 */
public class DoubleSort {

    private static final int INSERTION_SORT_THRESHOLD = 47;

    /**
     * Sorts doubles stored in native memory in ascending order.
     *
     * @param ptr   native address of element[0]
     * @param left  the index of the first element, inclusive, to be sorted
     * @param right the index of the last element, inclusive, to be sorted
     */
    public static void sort(long ptr, long left, long right) {
        while (left < right) {
            long length = right - left + 1;

            if (length < INSERTION_SORT_THRESHOLD) {
                insertionSort(ptr, left, right);
                return;
            }

            // Median-of-three pivot selection
            long pivotIndex = medianOfThree(ptr, left, left + (right - left) / 2, right);
            double pivot = getDouble(ptr, pivotIndex);

            // Dutch National Flag three-way partition:
            //   [left, lt)   < pivot
            //   [lt, i)     == pivot
            //   [gt, right] > pivot
            //   [i, gt)      unexamined
            long lt = left;
            long gt = right;
            long i = left;

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

            // Now: [left, lt) < pivot, [lt, gt] == pivot, (gt, right] > pivot.
            // Recurse on the smaller side, iterate on the larger to limit stack depth.
            if (lt - left < right - gt) {
                sort(ptr, left, lt - 1);
                left = gt + 1;
            } else {
                sort(ptr, gt + 1, right);
                right = lt - 1;
            }
        }
    }

    private static double getDouble(long ptr, long index) {
        return Unsafe.getUnsafe().getDouble(ptr + index * 8L);
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

    private static void putDouble(long ptr, long index, double value) {
        Unsafe.getUnsafe().putDouble(ptr + index * 8L, value);
    }

    private static void swap(long ptr, long i, long j) {
        double tmp = getDouble(ptr, i);
        putDouble(ptr, i, getDouble(ptr, j));
        putDouble(ptr, j, tmp);
    }
}
