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

package io.questdb.griffin.engine.groupby;

import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

/**
 * Specialized flyweight double list used in {@link io.questdb.griffin.engine.functions.GroupByFunction}s.
 * <p>
 * Uses provided {@link GroupByAllocator} to allocate the underlying buffer. Grows the buffer when needed.
 * <p>
 * Buffer layout is the following:
 * <pre>
 * | capacity (in doubles) | size (in doubles) | double array |
 * +-----------------------+-------------------+--------------+
 * |       4 bytes         |      4 bytes      |      -       |
 * +-----------------------+-------------------+--------------+
 * </pre>
 */
public class GroupByDoubleList {
    private static final long HEADER_SIZE = 2 * Integer.BYTES;
    private static final int MIN_INITIAL_CAPACITY = 2;
    private static final long SIZE_OFFSET = Integer.BYTES;
    private final int initialCapacity;
    private final double noEntryValue;
    private GroupByAllocator allocator;
    private long ptr;

    public GroupByDoubleList(int initialCapacity, double noEntryValue) {
        this.initialCapacity = Math.max(initialCapacity, MIN_INITIAL_CAPACITY);
        this.noEntryValue = noEntryValue;
    }

    /**
     * Adds long to list;
     *
     * @param value to be added.
     */
    public void add(double value) {
        final int size = size();
        final int newSize = size + 1;
        checkCapacity(newSize);
        setValueAt(size, value);
        setSize(newSize);
    }

    public long add(GroupByDoubleList that) {
        return add(that, 0, that.size());
    }

    public long add(GroupByDoubleList that, int lo, int hi) {
        final int this_size = size();
        final int that_size = hi - lo;
        final int final_size = this_size + that_size;
        checkCapacity(final_size);
        Vect.memcpy(appendAddress(), that.addressOf(lo), that_size);
        setSize(final_size);
        return ptr();
    }

    public long addressOf(int index) {
        return ptr() + HEADER_SIZE + 8L * index;
    }

    public long appendAddress() {
        return addressOf(size());
    }

    public int capacity() {
        return ptr != 0 ? Unsafe.getUnsafe().getInt(ptr) : 0;
    }

    public void checkCapacity(int capacity) {
        if (capacity < 0) {
            throw new IllegalArgumentException("Negative capacity. Integer overflow may be?");
        }

        int l = capacity();
        if (capacity > l) {
            int newCapacity = Math.max(l << 1, capacity);
            final int oldSize = size();
            final int oldCapacity = capacity();

            long oldPtr = ptr;
            ptr = allocator.malloc(8L * newCapacity + HEADER_SIZE);
            zero(ptr, newCapacity);
            Unsafe.getUnsafe().putInt(ptr, newCapacity);
            Unsafe.getUnsafe().putInt(ptr + SIZE_OFFSET, oldSize);

            Vect.memcpy(ptr + HEADER_SIZE, oldPtr + HEADER_SIZE, 8L * oldCapacity);

            allocator.free(oldPtr, HEADER_SIZE + 8L * oldCapacity);
        }
    }

    public double get(int index) {
        if (index < size()) {
            return valueAt(index);
        }
        throw new ArrayIndexOutOfBoundsException(index);
    }

    public double getQuick(int index) {
        return valueAt(index);
    }

    public GroupByDoubleList of(long ptr) {
        if (ptr == 0) {
            this.ptr = allocator.malloc(HEADER_SIZE + 8L * initialCapacity);
            zero(this.ptr, initialCapacity);
            setCapacity(initialCapacity);
            setSize(0);
        } else {
            this.ptr = ptr;
        }
        return this;
    }

    public long ptr() {
        return ptr;
    }

    public void resetPtr() {
        ptr = 0;
    }

    public void setAllocator(GroupByAllocator allocator) {
        this.allocator = allocator;
    }

    public int size() {
        return ptr != 0 ? Unsafe.getUnsafe().getInt(ptr + SIZE_OFFSET) : 0;
    }

    private void setCapacity(int newCapacity) {
        Unsafe.getUnsafe().putInt(ptr, newCapacity);
    }

    private void setSize(int newSize) {
        Unsafe.getUnsafe().putInt(ptr + SIZE_OFFSET, newSize);
    }

    private void setValueAt(long index, double value) {
        Unsafe.getUnsafe().putDouble(ptr + HEADER_SIZE + 8L * index, value);
    }

    private double valueAt(long index) {
        return Unsafe.getUnsafe().getDouble(ptr + HEADER_SIZE + 8L * index);
    }

    private void zero(long ptr, int cap) {
        if (noEntryValue == 0) {
            // Vectorized fast path for zero default value.
            Vect.memset(ptr + HEADER_SIZE, 8L * cap, 0);
        } else {
            for (long p = ptr + HEADER_SIZE, lim = ptr + HEADER_SIZE + 8L * cap; p < lim; p += 8L) {
                Unsafe.getUnsafe().putDouble(p, noEntryValue);
            }
        }
    }

    /**
     * Sorts the specified range of the list using Dual-Pivot Quicksort.
     * The sort is performed in-place.
     *
     * @param lo the index of the first element, inclusive, to be sorted
     * @param hi the index of the last element, inclusive, to be sorted
     */
    public void sort(int lo, int hi) {
        if (lo < 0 || hi >= size()) {
            throw new ArrayIndexOutOfBoundsException("lo=" + lo + ", hi=" + hi + ", size=" + size());
        }
        if (lo < hi) {
            sort(lo, hi, true);
        }
    }

    /**
     * If the length of an array to be sorted is less than this
     * constant, insertion sort is used in preference to Quicksort.
     */
    private static final int INSERTION_SORT_THRESHOLD = 47;

    /**
     * Sorts the specified range of the list by Dual-Pivot Quicksort.
     *
     * @param left     the index of the first element, inclusive, to be sorted
     * @param right    the index of the last element, inclusive, to be sorted
     * @param leftmost indicates if this part is the leftmost in the range
     */
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
                    double ai = getQuick(i + 1);
                    while (ai < getQuick(j)) {
                        setValueAt(j + 1, getQuick(j));
                        if (j-- == left) {
                            break;
                        }
                    }
                    setValueAt(j + 1, ai);
                }
            } else {
                /*
                 * Skip the longest ascending sequence.
                 */
                do {
                    if (left >= right) {
                        return;
                    }
                } while (getQuick(++left) >= getQuick(left - 1));

                /*
                 * Every element from adjoining part plays the role
                 * of sentinel, therefore this allows us to avoid the
                 * left range check on each iteration. Moreover, we use
                 * the more optimized algorithm, so called pair insertion
                 * sort, which is faster (in the context of Quicksort)
                 * than traditional implementation of insertion sort.
                 */
                for (int k = left; ++left <= right; k = ++left) {
                    double a1 = getQuick(k), a2 = getQuick(left);

                    if (a1 < a2) {
                        a2 = a1;
                        a1 = getQuick(left);
                    }
                    while (a1 < getQuick(--k)) {
                        setValueAt(k + 2, getQuick(k));
                    }
                    setValueAt(++k + 1, a1);

                    while (a2 < getQuick(--k)) {
                        setValueAt(k + 1, getQuick(k));
                    }
                    setValueAt(k + 1, a2);
                }
                double last = getQuick(right);

                while (last < getQuick(--right)) {
                    setValueAt(right + 1, getQuick(right));
                }
                setValueAt(right + 1, last);
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
        if (getQuick(e2) < getQuick(e1)) {
            swap(e2, e1);
        }

        if (getQuick(e3) < getQuick(e2)) {
            double t = getQuick(e3);
            setValueAt(e3, getQuick(e2));
            setValueAt(e2, t);
            if (t < getQuick(e1)) {
                setValueAt(e2, getQuick(e1));
                setValueAt(e1, t);
            }
        }
        if (getQuick(e4) < getQuick(e3)) {
            double t = getQuick(e4);
            setValueAt(e4, getQuick(e3));
            setValueAt(e3, t);
            if (t < getQuick(e2)) {
                setValueAt(e3, getQuick(e2));
                setValueAt(e2, t);
                if (t < getQuick(e1)) {
                    setValueAt(e2, getQuick(e1));
                    setValueAt(e1, t);
                }
            }
        }
        if (getQuick(e5) < getQuick(e4)) {
            double t = getQuick(e5);
            setValueAt(e5, getQuick(e4));
            setValueAt(e4, t);
            if (t < getQuick(e3)) {
                setValueAt(e4, getQuick(e3));
                setValueAt(e3, t);
                if (t < getQuick(e2)) {
                    setValueAt(e3, getQuick(e2));
                    setValueAt(e2, t);
                    if (t < getQuick(e1)) {
                        setValueAt(e2, getQuick(e1));
                        setValueAt(e1, t);
                    }
                }
            }
        }

        // Pointers
        int less = left;  // The index of the first element of center part
        int great = right; // The index before the first element of right part

        if (getQuick(e1) != getQuick(e2) && getQuick(e2) != getQuick(e3) && getQuick(e3) != getQuick(e4) && getQuick(e4) != getQuick(e5)) {
            /*
             * Use the second and fourth of the five sorted elements as pivots.
             * These values are inexpensive approximations of the first and
             * second terciles of the array. Note that pivot1 <= pivot2.
             */
            double pivot1 = getQuick(e2);
            double pivot2 = getQuick(e4);

            /*
             * The first and the last elements to be sorted are moved to the
             * locations formerly occupied by the pivots. When partitioning
             * is complete, the pivots are swapped back into their final
             * positions, and excluded from subsequent sorting.
             */
            setValueAt(e2, getQuick(left));
            setValueAt(e4, getQuick(right));

            /*
             * Skip elements, which are less or greater than pivot values.
             */
            //noinspection StatementWithEmptyBody
            while (getQuick(++less) < pivot1) ;
            //noinspection StatementWithEmptyBody
            while (getQuick(--great) > pivot2) ;

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
                double ak = getQuick(k);
                if (ak < pivot1) { // Move a[k] to left part
                    setValueAt(k, getQuick(less));
                    /*
                     * Here and below we use "a[i] = b; i++;" instead
                     * of "a[i++] = b;" due to performance issue.
                     */
                    setValueAt(less, ak);
                    ++less;
                } else if (ak > pivot2) { // Move a[k] to right part
                    while (getQuick(great) > pivot2) {
                        if (great-- == k) {
                            break outer;
                        }
                    }
                    if (getQuick(great) < pivot1) { // a[great] <= pivot2
                        setValueAt(k, getQuick(less));
                        setValueAt(less, getQuick(great));
                        ++less;
                    } else { // pivot1 <= a[great] <= pivot2
                        setValueAt(k, getQuick(great));
                    }
                    /*
                     * Here and below we use "a[i] = b; i--;" instead
                     * of "a[i--] = b;" due to performance issue.
                     */
                    setValueAt(great, ak);
                    --great;
                }
            }

            // Swap pivots into their final positions
            setValueAt(left, getQuick(less - 1));
            setValueAt(less - 1, pivot1);
            setValueAt(right, getQuick(great + 1));
            setValueAt(great + 1, pivot2);

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
                while (getQuick(less) == pivot1) {
                    ++less;
                }

                while (getQuick(great) == pivot2) {
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
                    double ak = getQuick(k);
                    if (ak == pivot1) { // Move a[k] to left part
                        setValueAt(k, getQuick(less));
                        setValueAt(less, ak);
                        ++less;
                    } else if (ak == pivot2) { // Move a[k] to right part
                        while (getQuick(great) == pivot2) {
                            if (great-- == k) {
                                break outer;
                            }
                        }
                        if (getQuick(great) == pivot1) { // a[great] < pivot2
                            setValueAt(k, getQuick(less));
                            /*
                             * Even though a[great] equals to pivot1, the
                             * assignment a[less] = pivot1 may be incorrect,
                             * if a[great] and pivot1 are floating-point zeros
                             * of different signs. Therefore in float and
                             * double sorting methods we have to use more
                             * accurate assignment a[less] = a[great].
                             */
                            setValueAt(less, getQuick(great));
                            ++less;
                        } else { // pivot1 < a[great] < pivot2
                            setValueAt(k, getQuick(great));
                        }
                        setValueAt(great, ak);
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
            double pivot = getQuick(e3);

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
                if (getQuick(k) == pivot) {
                    continue;
                }
                double ak = getQuick(k);
                if (ak < pivot) { // Move a[k] to left part
                    setValueAt(k, getQuick(less));
                    setValueAt(less, ak);
                    ++less;
                } else { // a[k] > pivot - Move a[k] to right part
                    while (getQuick(great) > pivot) {
                        --great;
                    }
                    if (getQuick(great) < pivot) { // a[great] <= pivot
                        setValueAt(k, getQuick(less));
                        setValueAt(less, getQuick(great));
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
                        setValueAt(k, getQuick(great));
                    }
                    setValueAt(great, ak);
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

    private void swap(int a, int b) {
        double tmp = getQuick(a);
        setValueAt(a, getQuick(b));
        setValueAt(b, tmp);
    }

    /**
     * QuickSelect algorithm to find the k-th smallest element.
     * After this method returns, the element at index k will be in its final sorted position,
     * and all elements before k will be less than or equal to it, and all elements after k
     * will be greater than or equal to it.
     * <p>
     * This is more efficient than full sorting when only specific elements are needed.
     * Time complexity: O(n) average case, O(n²) worst case.
     *
     * @param lo the index of the first element, inclusive
     * @param hi the index of the last element, inclusive
     * @param k  the index of the element to select
     */
    public void quickSelect(int lo, int hi, int k) {
        if (lo < 0 || hi >= size() || k < lo || k > hi) {
            throw new ArrayIndexOutOfBoundsException("lo=" + lo + ", hi=" + hi + ", k=" + k + ", size=" + size());
        }
        if (lo < hi) {
            quickSelectImpl(lo, hi, k);
        }
    }

    /**
     * Optimized QuickSelect for multiple indices.
     * Partitions recursively only for the partitions containing required indices.
     * This avoids unnecessary work when selecting multiple percentiles.
     * <p>
     * The indices array must be sorted in ascending order.
     * After this method returns, all elements at the specified indices will be
     * in their final sorted positions.
     *
     * @param lo      the index of the first element, inclusive
     * @param hi      the index of the last element, inclusive
     * @param indices sorted array of indices to select
     * @param from    start index in the indices array
     * @param to      end index (exclusive) in the indices array
     */
    public void quickSelectMultiple(int lo, int hi, int[] indices, int from, int to) {
        if (from >= to || lo >= hi) {
            return;
        }

        // Find pivot and partition
        int pivotIndex = partition(lo, hi);

        // Find which indices fall into which partition
        int splitPoint = from;
        while (splitPoint < to && indices[splitPoint] < pivotIndex) {
            splitPoint++;
        }

        // Recursively select in left partition (elements < pivot)
        if (splitPoint > from) {
            quickSelectMultiple(lo, pivotIndex - 1, indices, from, splitPoint);
        }

        // Skip the pivot itself if it's one of our target indices
        int afterPivot = splitPoint;
        while (afterPivot < to && indices[afterPivot] == pivotIndex) {
            afterPivot++;
        }

        // Recursively select in right partition (elements > pivot)
        if (afterPivot < to) {
            quickSelectMultiple(pivotIndex + 1, hi, indices, afterPivot, to);
        }
    }

    /**
     * Internal QuickSelect implementation using the partition method.
     */
    private void quickSelectImpl(int lo, int hi, int k) {
        if (lo >= hi) {
            return;
        }

        int pivotIndex = partition(lo, hi);

        if (k < pivotIndex) {
            quickSelectImpl(lo, pivotIndex - 1, k);
        } else if (k > pivotIndex) {
            quickSelectImpl(pivotIndex + 1, hi, k);
        }
        // If k == pivotIndex, we're done - the element is in the correct position
    }

    /**
     * Partition method using median-of-three pivot selection.
     * Returns the final position of the pivot element.
     * <p>
     * After partitioning:
     * - All elements to the left of the pivot are <= pivot
     * - All elements to the right of the pivot are >= pivot
     *
     * @param lo the index of the first element, inclusive
     * @param hi the index of the last element, inclusive
     * @return the final index of the pivot element
     */
    private int partition(int lo, int hi) {
        // Use median-of-three for better pivot selection
        int mid = lo + (hi - lo) / 2;

        // Order lo, mid, hi
        if (getQuick(mid) < getQuick(lo)) {
            swap(lo, mid);
        }
        if (getQuick(hi) < getQuick(lo)) {
            swap(lo, hi);
        }
        if (getQuick(mid) < getQuick(hi)) {
            swap(mid, hi);
        }

        // Now hi is the median, use it as pivot
        double pivot = getQuick(hi);
        int i = lo - 1;

        for (int j = lo; j < hi; j++) {
            if (getQuick(j) <= pivot) {
                i++;
                swap(i, j);
            }
        }

        swap(i + 1, hi);
        return i + 1;
    }
}
