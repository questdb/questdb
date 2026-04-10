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

    public void set(long index, long value) {
        setValueAt(index, value);
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

    public void sort() {
        int size = size();
        if (size > 1) {
            sort(0, size - 1, true);
        }
    }

    public void sortAsUnsigned() {
        Vect.sortULongAscInPlace(ptr() + HEADER_SIZE, size());
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
