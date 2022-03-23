/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.cairo.BinarySearch;
import io.questdb.std.str.CharSink;

import java.util.Arrays;

public class LongList implements Mutable, LongVec {
    private static final int DEFAULT_ARRAY_SIZE = 16;
    private static final long DEFAULT_NO_ENTRY_VALUE = -1;
    private final long noEntryValue;
    private long[] data;
    private int pos = 0;

    public LongList() {
        this(DEFAULT_ARRAY_SIZE);
    }

    public LongList(int capacity) {
        this(capacity, DEFAULT_NO_ENTRY_VALUE);
    }

    public LongList(int capacity, long noEntryValue) {
        this.data = new long[capacity];
        this.noEntryValue = noEntryValue;
    }

    public LongList(LongList other) {
        this.data = new long[Math.max(other.size(), DEFAULT_ARRAY_SIZE)];
        setPos(other.size());
        System.arraycopy(other.data, 0, this.data, 0, pos);
        this.noEntryValue = other.noEntryValue;
    }

    public void add(long value) {
        ensureCapacity(pos + 1);
        data[pos++] = value;
    }

    public void add(long value0, long value1) {
        int n = pos;
        ensureCapacity(n + 2);
        data[n++] = value0;
        data[n++] = value1;
        pos = n;
    }

    public void add(long value0, long value1, long value2, long value3) {
        int n = pos;
        ensureCapacity(n + 4);
        data[n++] = value0;
        data[n++] = value1;
        data[n++] = value2;
        data[n++] = value3;
        pos = n;
    }

    public void add(long value0, long value1, long value2, long value3, long value4, long value5, long value6, long value7) {
        int n = pos;
        ensureCapacity(n + 8);
        data[n++] = value0;
        data[n++] = value1;
        data[n++] = value2;
        data[n++] = value3;
        data[n++] = value4;
        data[n++] = value5;
        data[n++] = value6;
        data[n++] = value7;
        pos = n;
    }

    public void add(LongList that) {
        add(that, 0, that.size());
    }

    public void add(LongList that, int lo, int hi) {
        int p = pos;
        int s = hi - lo;
        ensureCapacity(p + s);
        System.arraycopy(that.data, lo, this.data, p, s);
        pos += s;
    }

    public void add(int index, long element) {
        ensureCapacity(++pos);
        System.arraycopy(data, index, data, index + 1, pos - index - 1);
        data[index] = element;
    }

    public void arrayCopy(int srcPos, int dstPos, int length) {
        System.arraycopy(data, srcPos, data, dstPos, length);
    }

    public int binarySearch(long value, int scanDir) {

        // this is the same algorithm as implemented in C (util.h)
        // template<class T, class V>
        // inline int64_t binary_search(T *data, V value, int64_t low, int64_t high, int32_t scan_dir)
        // please ensure these implementations are in sync

        int low = 0;
        int high = pos - 1;
        while (high - low > 65) {
            final int mid = (low + high) / 2;
            final long midVal = data[mid];

            if (midVal < value) {
                low = mid;
            } else if (midVal > value) {
                high = mid - 1;
            } else {
                // In case of multiple equal values, find the first
                return scanDir == BinarySearch.SCAN_UP ?
                        scrollUp(mid, midVal) :
                        scrollDown(mid, high, midVal);
            }
        }
        return scanDir == BinarySearch.SCAN_UP ?
                scanUp(value, low, high + 1) :
                scanDown(value, low, high + 1);
    }

    public int binarySearchBlock(int shl, long value, int scanDir) {
        // Binary searches using 2^shl blocks
        // e.g. when shl == 2
        // this method treats 4 longs as 1 entry
        // taking first long for the comparisons
        // and ignoring the other 3 values.

        // This is useful when list is a dictionary where first long is a key
        // and subsequent X (1, 3, 7 etc.) values are the value of the dictionary.

        // this is the same algorithm as implemented in C (util.h)
        // template<class T, class V>
        // inline int64_t binary_search(T *data, V value, int64_t low, int64_t high, int32_t scan_dir)
        // please ensure these implementations are in sync

        int low = 0;
        int high = (pos - 1) >> shl;
        while (high - low > 65) {
            final int mid = (low + high) / 2;
            final long midVal = data[mid << shl];

            if (midVal < value) {
                low = mid;
            } else if (midVal > value) {
                high = mid - 1;
            } else {
                // In case of multiple equal values, find the first
                return scanDir == BinarySearch.SCAN_UP ?
                        scrollUpBlock(shl, mid, midVal) :
                        scrollDownBlock(shl, mid, high, midVal);
            }
        }
        return scanDir == BinarySearch.SCAN_UP ?
                scanUpBlock(shl, value, low, high + 1) :
                scanDownBlock(shl, value, low, high + 1);
    }

    public void clear() {
        pos = 0;
    }

    public void ensureCapacity(int capacity) {
        if (capacity < 0) {
            throw new IllegalArgumentException("Negative capacity. Integer overflow may be?");
        }

        int l = data.length;
        if (capacity > l) {
            int newCap = Math.max(l << 1, capacity);
            long[] buf = new long[newCap];
            System.arraycopy(data, 0, buf, 0, l);
            this.data = buf;
        }
    }

    public void erase() {
        pos = 0;
        Arrays.fill(data, noEntryValue);
    }

    public void extendAndSet(int index, long value) {
        ensureCapacity(index + 1);
        if (index >= pos) {
            pos = index + 1;
        }
        data[index] = value;
    }

    public void fill(int from, int to, long value) {
        Arrays.fill(data, from, to, value);
    }

    public long get(int index) {
        if (index < pos) {
            return data[index];
        }
        throw new ArrayIndexOutOfBoundsException(index);
    }

    public long getAndSetQuick(int index, long value) {
        long v = getQuick(index);
        data[index] = value;
        return v;
    }

    /**
     * Returns last element of the list or null if list is empty.
     *
     * @return last element of the list
     */
    public long getLast() {
        if (pos > 0) {
            return data[pos - 1];
        }
        return noEntryValue;
    }

    public void setLast(long value) {
        if (pos > 0) {
            data[pos - 1] = value;
        }
    }

    /**
     * Returns element at the specified position. This method does not do
     * bounds check and may cause memory corruption if index is out of bounds.
     * Instead the responsibility to check bounds is placed on application code,
     * which is often the case anyway, for example in indexed for() loop.
     *
     * @param index of the element
     * @return element at the specified position.
     */
    public long getQuick(int index) {
        return data[index];
    }

    public void setQuick(int index, long value) {
        assert index < pos;
        data[index] = value;
    }

    @Override
    public LongVec newInstance() {
        return new LongList(size());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        long hashCode = 1;
        for (int i = 0, n = pos; i < n; i++) {
            long v = getQuick(i);
            hashCode = 31 * hashCode + (v == noEntryValue ? 0 : v);
        }
        return (int) hashCode;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object that) {
        return this == that || that instanceof LongList && equals((LongList) that);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        final CharSink sb = Misc.getThreadLocalBuilder();

        sb.put('[');
        for (int i = 0, k = pos; i < k; i++) {
            if (i > 0) {
                sb.put(',');
            }
            sb.put(get(i));
        }
        sb.put(']');
        return sb.toString();
    }

    public void increment(int index) {
        data[index] = data[index] + 1;
    }

    public void insert(int index, int length) {
        ensureCapacity(pos + length);
        if (pos > index) {
            System.arraycopy(data, index, data, index + length, pos - index);
        }
        pos += length;
    }

    public void remove(long v) {
        int index = indexOf(v);
        if (index > -1) {
            removeIndex(index);
        }
    }

    public void removeIndex(int index) {
        if (pos < 1 || index >= pos) {
            return;
        }
        int move = pos - index - 1;
        if (move > 0) {
            System.arraycopy(data, index + 1, data, index, move);
        }
        data[--pos] = noEntryValue;
    }

    public void removeIndexBlock(int index, int slotSize) {
        if (pos < 1 || index >= pos) {
            return;
        }
        int move = pos - index - slotSize;
        if (move > 0) {
            System.arraycopy(data, index + slotSize, data, index, move);
        }
        pos -= slotSize;
        Arrays.fill(data, pos, pos + slotSize, noEntryValue);
    }

    public void seed(int capacity, long value) {
        ensureCapacity(capacity);
        pos = capacity;
        fill(0, capacity, value);
    }

    public void seed(int fromIndex, int count, long value) {
        int capacity = fromIndex + count;
        ensureCapacity(capacity);
        Arrays.fill(data, fromIndex, capacity, value);
    }

    public void set(int index, long element) {
        if (index < pos) {
            data[index] = element;
            return;
        }
        throw new ArrayIndexOutOfBoundsException(index);
    }

    public void setAll(int capacity, long value) {
        ensureCapacity(capacity);
        pos = capacity;
        Arrays.fill(data, value);
    }

    final public void setPos(int pos) {
        ensureCapacity(pos);
        this.pos = pos;
    }

    public int size() {
        return pos;
    }

    /**
     * Sorts the specified array.
     */
    public void sort() {
        LongSort.sort(this, 0, size() - 1);
    }

    public LongList subset(int lo, int hi) {
        int _hi = Math.min(hi, pos);
        LongList that = new LongList(_hi - lo);
        System.arraycopy(this.data, lo, that.data, 0, _hi - lo);
        that.pos = _hi - lo;
        return that;
    }

    public void zero(int value) {
        Arrays.fill(data, 0, pos, value);
    }

    private boolean equals(LongList that) {
        if (this.pos == that.pos) {
            for (int i = 0, n = pos; i < n; i++) {
                long lhs = this.getQuick(i);
                if (lhs == noEntryValue) {
                    return that.getQuick(i) == noEntryValue;
                } else if (lhs == that.getQuick(i)) {
                    return true;
                }
            }
        }
        return false;
    }

    public int indexOf(long o) {
        for (int i = 0, n = pos; i < n; i++) {
            if (o == getQuick(i)) {
                return i;
            }
        }
        return -1;
    }

    private int scanDown(long v, int low, int high) {
        for (int i = high - 1; i >= low; i--) {
            long that = data[i];
            if (that == v) {
                return i;
            }
            if (that < v) {
                return -(i + 2);
            }
        }
        return -(low + 1);
    }

    private int scanDownBlock(int shl, long v, int low, int high) {
        for (int i = high - 1; i >= low; i--) {
            long that = data[i << shl];
            if (that == v) {
                return i << shl;
            }
            if (that < v) {
                return -(((i + 1) << shl) + 1);
            }
        }
        return -((low << shl) + 1);
    }

    private int scanUp(long value, int low, int high) {
        for (int i = low; i < high; i++) {
            long that = data[i];
            if (that == value) {
                return i;
            }
            if (that > value) {
                return -(i + 1);
            }
        }
        return -(high + 1);
    }

    private int scanUpBlock(int shl, long value, int low, int high) {
        for (int i = low; i < high; i++) {
            long that = data[i << shl];
            if (that == value) {
                return i << shl;
            }
            if (that > value) {
                return -((i << shl) + 1);
            }
        }
        return -((high << shl) + 1);
    }

    private int scrollDown(int low, int high, long value) {
        do {
            if (low < high) {
                low++;
            } else {
                return low;
            }
        } while (data[low] == value);
        return low - 1;
    }

    private int scrollDownBlock(int shl, int low, int high, long value) {
        do {
            if (low < high) {
                low++;
            } else {
                return low << shl;
            }
        } while (data[low << shl] == value);
        return (low - 1) << shl;
    }

    private int scrollUp(int high, long value) {
        do {
            if (high > 0) {
                high--;
            } else {
                return 0;
            }
        } while (data[high] == value);
        return high + 1;
    }

    private int scrollUpBlock(int shl, int high, long value) {
        do {
            if (high > 0) {
                high--;
            } else {
                return 0;
            }
        } while (data[high << shl] == value);
        return (high + 1) << shl;
    }
}
