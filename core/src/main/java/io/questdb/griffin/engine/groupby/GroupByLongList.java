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

package io.questdb.griffin.engine.groupby;

import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

/**
 * Specialized flyweight long list used in {@link io.questdb.griffin.engine.functions.GroupByFunction}s.
 * <p>
 * Uses provided {@link GroupByAllocator} to allocate the underlying buffer. Grows the buffer when needed.
 * <p>
 * Buffer layout is the following:
 * <pre>
 * | capacity (in longs) | size (in longs) | long array |
 * +---------------------+-----------------+------------+
 * |       4 bytes       |     4 bytes     |     -      |
 * +---------------------+-----------------+------------+
 * </pre>
 */
public class GroupByLongList {
    private static final long HEADER_SIZE = 2 * Integer.BYTES;
    private static final int INSERTION_SORT_THRESHOLD = 47;
    private static final int MIN_INITIAL_CAPACITY = 2;
    private static final long SIZE_OFFSET = Integer.BYTES;
    private final int initialCapacity;
    private final long noEntryValue;
    private GroupByAllocator allocator;
    private long ptr;

    public GroupByLongList(int initialCapacity, long noEntryValue) {
        this.initialCapacity = Math.max(initialCapacity, MIN_INITIAL_CAPACITY);
        this.noEntryValue = noEntryValue;
    }

    /**
     * Adds long to list;
     *
     * @param value to be added.
     */
    public void add(long value) {
        final int size = size();
        final int newSize = size + 1;
        checkCapacity(newSize);
        setValueAt(size, value);
        setSize(newSize);
    }

    public long add(GroupByLongList that) {
        return add(that, 0, that.size());
    }

    public long add(GroupByLongList that, int lo, int hi) {
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

    public long dataPtr() {
        return ptr + HEADER_SIZE;
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

            Vect.memcpy(ptr + HEADER_SIZE, oldPtr + HEADER_SIZE, oldSize * 8L);

            allocator.free(oldPtr, HEADER_SIZE + 8L * oldCapacity);
        }
    }

    public long get(long index) {
        if (index < size()) {
            return valueAt(index);
        }
        throw new ArrayIndexOutOfBoundsException((int) index);
    }

    public long getQuick(long index) {
        return valueAt(index);
    }

    public GroupByLongList of(long ptr) {
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

    /**
     * QuickSelect algorithm to find the k-th smallest element.
     * After this method returns, the element at index k will be in its final sorted position,
     * and all elements before k will be less than or equal to it, and all elements after k
     * will be greater than or equal to it.
     * <p>
     * This is more efficient than full sorting when only specific elements are needed.
     * Time complexity: O(n) average case, O(n^2) worst case.
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
        if (lo < 0 || hi >= size()) {
            throw new ArrayIndexOutOfBoundsException("lo=" + lo + ", hi=" + hi + ", size=" + size());
        }
        while (from < to && lo < hi) {
            long packed = partition3Way(lo, hi);
            int lt = (int) (packed >>> 32);
            int gt = (int) packed;

            // Find which indices fall into each region: <lt, [lt..gt], >gt
            int splitLeft = from;
            while (splitLeft < to && indices[splitLeft] < lt) {
                splitLeft++;
            }

            // Skip indices in the equal partition [lt..gt]
            int splitRight = splitLeft;
            while (splitRight < to && indices[splitRight] <= gt) {
                splitRight++;
            }

            boolean hasLeft = splitLeft > from;
            boolean hasRight = splitRight < to;

            if (hasLeft && hasRight) {
                if ((lt - lo) <= (hi - gt)) {
                    quickSelectMultiple(lo, lt - 1, indices, from, splitLeft);
                    lo = gt + 1;
                    from = splitRight;
                } else {
                    quickSelectMultiple(gt + 1, hi, indices, splitRight, to);
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

    public void resetPtr() {
        ptr = 0;
    }

    public void set(long index, long value) {
        setValueAt(index, value);
    }

    public void setAllocator(GroupByAllocator allocator) {
        this.allocator = allocator;
    }

    public int size() {
        return ptr != 0 ? Unsafe.getUnsafe().getInt(ptr + SIZE_OFFSET) : 0;
    }

    public void sort() {
        int size = size();
        if (size > 1) {
            sort(0, size - 1, true);
        }
    }

    public void sortAsUnsigned() {
        Vect.sortULongAscInPlace(ptr() + HEADER_SIZE, size());
    }

    /**
     * DNF three-way partition for quickSelectMultiple.
     * Returns packed lt|gt as a long: lt in upper 32 bits, gt in lower 32 bits.
     */
    private long partition3Way(int lo, int hi) {
        int mid = lo + (hi - lo) / 2;
        if (getQuick(mid) < getQuick(lo)) {
            swap(lo, mid);
        }
        if (getQuick(hi) < getQuick(lo)) {
            swap(lo, hi);
        }
        if (getQuick(mid) < getQuick(hi)) {
            swap(mid, hi);
        }
        long pivot = getQuick(hi);
        int lt = lo;
        int gt = hi;
        int i = lo;
        while (i <= gt) {
            long v = getQuick(i);
            if (v < pivot) {
                swap(lt, i);
                lt++;
                i++;
            } else if (v > pivot) {
                swap(i, gt);
                gt--;
            } else {
                i++;
            }
        }
        return ((long) lt << 32) | (gt & 0xFFFFFFFFL);
    }

    /**
     * Internal QuickSelect implementation using DNF three-way partition.
     * Iterative to avoid StackOverflowError on degenerate inputs.
     * Handles all-equal data in O(n) because the equal partition is skipped entirely.
     */
    private void quickSelectImpl(int lo, int hi, int k) {
        while (lo < hi) {
            long packed = partition3Way(lo, hi);
            int lt = (int) (packed >>> 32);
            int gt = (int) packed;

            if (k < lt) {
                hi = lt - 1;
            } else if (k > gt) {
                lo = gt + 1;
            } else {
                break; // k is in the equal partition
            }
        }
    }

    private void setCapacity(int newCapacity) {
        Unsafe.getUnsafe().putInt(ptr, newCapacity);
    }

    private void setSize(int newSize) {
        Unsafe.getUnsafe().putInt(ptr + SIZE_OFFSET, newSize);
    }

    private void setValueAt(long index, long value) {
        Unsafe.getUnsafe().putLong(ptr + HEADER_SIZE + 8L * index, value);
    }

    private void sort(int left, int right, boolean leftmost) {
        int length = right - left + 1;

        if (length < INSERTION_SORT_THRESHOLD) {
            if (leftmost) {
                for (int i = left, j = i; i < right; j = ++i) {
                    long ai = getQuick(i + 1);
                    while (ai < getQuick(j)) {
                        setValueAt(j + 1, getQuick(j));
                        if (j-- == left) {
                            break;
                        }
                    }
                    setValueAt(j + 1, ai);
                }
            } else {
                do {
                    if (left >= right) {
                        return;
                    }
                } while (getQuick(++left) >= getQuick(left - 1));

                for (int k = left; ++left <= right; k = ++left) {
                    long a1 = getQuick(k), a2 = getQuick(left);
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
                long last = getQuick(right);
                while (last < getQuick(--right)) {
                    setValueAt(right + 1, getQuick(right));
                }
                setValueAt(right + 1, last);
            }
            return;
        }

        int seventh = (length >> 3) + (length >> 6) + 1;

        int e3 = (left + right) >>> 1;
        int e2 = e3 - seventh;
        int e1 = e2 - seventh;
        int e4 = e3 + seventh;
        int e5 = e4 + seventh;

        if (getQuick(e2) < getQuick(e1)) {
            swap(e2, e1);
        }
        if (getQuick(e3) < getQuick(e2)) {
            long t = getQuick(e3);
            setValueAt(e3, getQuick(e2));
            setValueAt(e2, t);
            if (t < getQuick(e1)) {
                setValueAt(e2, getQuick(e1));
                setValueAt(e1, t);
            }
        }
        if (getQuick(e4) < getQuick(e3)) {
            long t = getQuick(e4);
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
            long t = getQuick(e5);
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

        int less = left;
        int great = right;

        if (getQuick(e1) != getQuick(e2) && getQuick(e2) != getQuick(e3) && getQuick(e3) != getQuick(e4) && getQuick(e4) != getQuick(e5)) {
            long pivot1 = getQuick(e2);
            long pivot2 = getQuick(e4);

            setValueAt(e2, getQuick(left));
            setValueAt(e4, getQuick(right));

            //noinspection StatementWithEmptyBody
            while (getQuick(++less) < pivot1) ;
            //noinspection StatementWithEmptyBody
            while (getQuick(--great) > pivot2) ;

            outer:
            for (int k = less - 1; ++k <= great; ) {
                long ak = getQuick(k);
                if (ak < pivot1) {
                    setValueAt(k, getQuick(less));
                    setValueAt(less, ak);
                    ++less;
                } else if (ak > pivot2) {
                    while (getQuick(great) > pivot2) {
                        if (great-- == k) {
                            break outer;
                        }
                    }
                    if (getQuick(great) < pivot1) {
                        setValueAt(k, getQuick(less));
                        setValueAt(less, getQuick(great));
                        ++less;
                    } else {
                        setValueAt(k, getQuick(great));
                    }
                    setValueAt(great, ak);
                    --great;
                }
            }

            setValueAt(left, getQuick(less - 1));
            setValueAt(less - 1, pivot1);
            setValueAt(right, getQuick(great + 1));
            setValueAt(great + 1, pivot2);

            sort(left, less - 2, leftmost);
            sort(great + 2, right, false);

            if (less < e1 && e5 < great) {
                while (getQuick(less) == pivot1) {
                    ++less;
                }
                while (getQuick(great) == pivot2) {
                    --great;
                }

                outer:
                for (int k = less - 1; ++k <= great; ) {
                    long ak = getQuick(k);
                    if (ak == pivot1) {
                        setValueAt(k, getQuick(less));
                        setValueAt(less, ak);
                        ++less;
                    } else if (ak == pivot2) {
                        while (getQuick(great) == pivot2) {
                            if (great-- == k) {
                                break outer;
                            }
                        }
                        if (getQuick(great) == pivot1) {
                            setValueAt(k, getQuick(less));
                            setValueAt(less, getQuick(great));
                            ++less;
                        } else {
                            setValueAt(k, getQuick(great));
                        }
                        setValueAt(great, ak);
                        --great;
                    }
                }
            }

            sort(less, great, false);
        } else {
            long pivot = getQuick(e3);

            for (int k = less; k <= great; ++k) {
                if (getQuick(k) == pivot) {
                    continue;
                }
                long ak = getQuick(k);
                if (ak < pivot) {
                    setValueAt(k, getQuick(less));
                    setValueAt(less, ak);
                    ++less;
                } else {
                    while (getQuick(great) > pivot) {
                        --great;
                    }
                    if (getQuick(great) < pivot) {
                        setValueAt(k, getQuick(less));
                        setValueAt(less, getQuick(great));
                        ++less;
                    } else {
                        setValueAt(k, getQuick(great));
                    }
                    setValueAt(great, ak);
                    --great;
                }
            }

            sort(left, less - 1, leftmost);
            sort(great + 1, right, false);
        }
    }

    private void swap(int a, int b) {
        long tmp = getQuick(a);
        setValueAt(a, getQuick(b));
        setValueAt(b, tmp);
    }

    private long valueAt(long index) {
        return Unsafe.getUnsafe().getLong(ptr + HEADER_SIZE + 8L * index);
    }

    private void zero(long ptr, int cap) {
        if (noEntryValue == 0) {
            // Vectorized fast path for zero default value.
            Vect.memset(ptr + HEADER_SIZE, 8L * cap, 0);
        } else {
            for (long p = ptr + HEADER_SIZE, lim = ptr + HEADER_SIZE + 8L * cap; p < lim; p += 8L) {
                Unsafe.getUnsafe().putLong(p, noEntryValue);
            }
        }
    }
}
