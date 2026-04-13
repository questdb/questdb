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

package io.questdb.griffin.engine.groupby;

import io.questdb.std.DoubleSort;
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
        final int thisSize = size();
        final int thatSize = hi - lo;
        final int finalSize = thisSize + thatSize;
        checkCapacity(finalSize);
        Vect.memcpy(appendAddress(), that.addressOf(lo), thatSize * 8L);
        setSize(finalSize);
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

            Vect.memcpy(ptr + HEADER_SIZE, oldPtr + HEADER_SIZE, 8L * oldSize);

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
     * Sorts the specified range of the list.
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
            DoubleSort.sort(addressOf(0), lo, hi);
        }
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
        while (from < to && lo < hi) {
            int pivotIndex = partition(lo, hi);

            // Find which indices fall into which partition
            int splitPoint = from;
            while (splitPoint < to && indices[splitPoint] < pivotIndex) {
                splitPoint++;
            }

            // Skip the pivot itself if it's one of our target indices
            int afterPivot = splitPoint;
            while (afterPivot < to && indices[afterPivot] == pivotIndex) {
                afterPivot++;
            }

            boolean hasLeft = splitPoint > from;
            boolean hasRight = afterPivot < to;

            if (hasLeft && hasRight) {
                // Recurse on smaller side, iterate on larger to limit stack depth
                if ((pivotIndex - lo) <= (hi - pivotIndex)) {
                    quickSelectMultiple(lo, pivotIndex - 1, indices, from, splitPoint);
                    lo = pivotIndex + 1;
                    from = afterPivot;
                } else {
                    quickSelectMultiple(pivotIndex + 1, hi, indices, afterPivot, to);
                    hi = pivotIndex - 1;
                    to = splitPoint;
                }
            } else if (hasLeft) {
                hi = pivotIndex - 1;
                to = splitPoint;
            } else if (hasRight) {
                lo = pivotIndex + 1;
                from = afterPivot;
            } else {
                break;
            }
        }
    }

    /**
     * Internal QuickSelect implementation using the partition method.
     * Iterative to avoid StackOverflowError on degenerate inputs (e.g. all-identical values).
     */
    private void quickSelectImpl(int lo, int hi, int k) {
        while (lo < hi) {
            int pivotIndex = partition(lo, hi);

            if (k < pivotIndex) {
                hi = pivotIndex - 1;
            } else if (k > pivotIndex) {
                lo = pivotIndex + 1;
            } else {
                break;
            }
        }
    }

    private void swap(int a, int b) {
        double tmp = getQuick(a);
        setValueAt(a, getQuick(b));
        setValueAt(b, tmp);
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
