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

import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.Utf16Sink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.util.Arrays;

public class LongList implements Mutable, LongVec, Sinkable {
    private static final int DEFAULT_ARRAY_SIZE = 16;
    private static final long DEFAULT_NO_ENTRY_VALUE = -1L;
    private final int initialCapacity;
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
        this.initialCapacity = capacity;
        this.data = new long[capacity];
        this.noEntryValue = noEntryValue;
    }

    public LongList(LongList other) {
        this.initialCapacity = Math.max(other.size(), DEFAULT_ARRAY_SIZE);
        this.data = new long[initialCapacity];
        setPos(other.size());
        System.arraycopy(other.data, 0, this.data, 0, pos);
        this.noEntryValue = other.noEntryValue;
    }

    public LongList(long[] other) {
        this.initialCapacity = other.length;
        this.data = new long[initialCapacity];
        setPos(other.length);
        System.arraycopy(other, 0, this.data, 0, pos);
        this.noEntryValue = DEFAULT_NO_ENTRY_VALUE;
    }

    public void add(long value) {
        checkCapacity(pos + 1);
        data[pos++] = value;
    }

    public void add(long value0, long value1) {
        int n = pos;
        checkCapacity(n + 2);
        data[n++] = value0;
        data[n++] = value1;
        pos = n;
    }

    public void add(long value0, long value1, long value2, long value3) {
        int n = pos;
        checkCapacity(n + 4);
        data[n++] = value0;
        data[n++] = value1;
        data[n++] = value2;
        data[n++] = value3;
        pos = n;
    }

    public void add(long value0, long value1, long value2, long value3, long value4, long value5, long value6, long value7) {
        int n = pos;
        checkCapacity(n + 8);
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
        checkCapacity(p + s);
        System.arraycopy(that.data, lo, this.data, p, s);
        pos += s;
    }

    public void add(int index, long element) {
        checkCapacity(++pos);
        System.arraycopy(data, index, data, index + 1, pos - index - 1);
        data[index] = element;
    }

    public void addAll(LongList that) {
        int p = pos;
        int s = that.size();
        setPos(p + s);
        System.arraycopy(that.data, 0, this.data, p, s);
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
            final int mid = (low + high) >>> 1;
            final long midVal = data[mid];

            if (midVal < value) {
                low = mid;
            } else if (midVal > value) {
                high = mid - 1;
            } else {
                // In case of multiple equal values, find the first
                return scanDir == Vect.BIN_SEARCH_SCAN_UP
                        ? scrollUp(mid, midVal)
                        : scrollDown(mid, high, midVal);
            }
        }
        return scanDir == Vect.BIN_SEARCH_SCAN_UP
                ? scanUp(value, low, high + 1)
                : scanDown(value, low, high + 1);
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

        return binarySearchBlock(0, shl, value, scanDir);
    }

    public int binarySearchBlock(int offset, int shl, long value, int scanDir) {
        int low = offset >> shl;
        int high = (pos - 1) >> shl;
        while (high - low > 65) {
            final int mid = (low + high) >>> 1;
            final long midVal = data[mid << shl];

            if (midVal < value) {
                low = mid;
            } else if (midVal > value) {
                high = mid - 1;
            } else {
                // In case of multiple equal values, find the first
                return scanDir == Vect.BIN_SEARCH_SCAN_UP
                        ? scrollUpBlock(shl, mid, midVal)
                        : scrollDownBlock(shl, mid, high, midVal);
            }
        }
        return scanDir == Vect.BIN_SEARCH_SCAN_UP
                ? scanUpBlock(shl, value, low, high + 1)
                : scanDownBlock(shl, value, low, high + 1);
    }

    public int capacity() {
        return data.length;
    }

    public void checkCapacity(int capacity) {
        if (capacity < 0) {
            throw new IllegalArgumentException("Negative capacity. Integer overflow may be?");
        }

        int l = data.length;
        if (capacity > l) {
            int newCap = Math.max(l << 1, capacity);
            this.data = Arrays.copyOf(data, newCap);
        }
    }

    /**
     * Resets the size of this list to zero.
     * <p>
     * <strong>Does not overwrite the underlying array with empty values.</strong>
     * Use <code>clear(0)</code> to overwrite it.
     */
    public void clear() {
        pos = 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object that) {
        return this == that || that instanceof LongList && equals((LongList) that);
    }

    public void erase() {
        pos = 0;
        Arrays.fill(data, noEntryValue);
    }

    /**
     * Sets the value at index, extending the backing array if needed.
     * <p>
     * <strong>WARNING:</strong> does not initialize the newly revealed portion of
     * the backing array! This may reveal values that were never set, or were set
     * before calling <code>clear()</code>.
     */
    public void extendAndSet(int index, long value) {
        checkCapacity(index + 1);
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

    /**
     * Returns element at the specified position. This method does not do
     * bounds check and may cause memory corruption if index is out of bounds.
     * Instead, the responsibility to check bounds is placed on application code,
     * which is often the case anyway, for example in indexed for() loop.
     *
     * @param index of the element
     * @return element at the specified position.
     */
    public long getQuick(int index) {
        return data[index];
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

    public void increment(int index) {
        data[index] = data[index] + 1;
    }

    public int indexOf(long o) {
        for (int i = 0, n = pos; i < n; i++) {
            if (o == getQuick(i)) {
                return i;
            }
        }
        return -1;
    }

    public void insert(int index, int length) {
        checkCapacity(pos + length);
        if (pos > index) {
            System.arraycopy(data, index, data, index + length, pos - index);
        }
        pos += length;
    }

    public void insertFromSource(int index, LongList src, int srcLo, int srcHi) {
        assert index > -1 && index < pos + 1 && srcLo > -1 && srcHi - 1 < src.size();
        int len = srcHi - srcLo;
        if (len > 0) {
            insert(index, len);
            System.arraycopy(src.data, srcLo, data, index, len);
        }
    }

    @Override
    public LongVec newInstance() {
        LongList newList = new LongList(size());
        newList.setPos(pos);
        return newList;
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

    public void restoreInitialCapacity() {
        data = new long[initialCapacity];
        pos = 0;
    }

    public void reverse() {
        int n = size();
        for (int i = 0, m = n / 2; i < m; i++) {
            long tmp = data[i];
            data[i] = data[n - i - 1];
            data[n - i - 1] = tmp;
        }
    }

    public void seed(int capacity, long value) {
        checkCapacity(capacity);
        pos = capacity;
        fill(0, capacity, value);
    }

    public void seed(int fromIndex, int count, long value) {
        int capacity = fromIndex + count;
        checkCapacity(capacity);
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
        checkCapacity(capacity);
        pos = capacity;
        Arrays.fill(data, value);
    }

    public void setLast(long value) {
        if (pos > 0) {
            data[pos - 1] = value;
        }
    }

    public final void setPos(int pos) {
        checkCapacity(pos);
        this.pos = pos;
    }

    public void setQuick(int index, long value) {
        assert index < pos;
        data[index] = value;
    }

    @TestOnly
    public void shuffle(Rnd rnd, int sh) {
        // sh is a power of 2 to indicate number of
        // values stored per virtual "slot". E.g. if
        // we store two values at a time, we want to shuffle pairs
        int size = size() >> sh;
        for (int i = size; i > 1; i--) {
            swap(i - 1, rnd.nextInt(i), sh);
        }
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

    public void swap(int i, int j, int shl) {
        int k = 1 << shl;
        for (int k1 = 0; k1 < k; k1++) {
            final int ii = (i << shl) + k1;
            final int ji = (j << shl) + k1;
            final long jv = getQuick(ji);
            setQuick(ji, getQuick(ii));
            setQuick(ii, jv);
        }
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.putAscii('[');
        for (int i = 0, k = pos; i < k; i++) {
            if (i > 0) {
                sink.putAscii(',');
            }
            sink.put(get(i));
        }
        sink.putAscii(']');
    }

    public void toSinkSorted(@NotNull CharSink<?> sink) {
        LongList temp = new LongList(size());
        temp.addAll(this);
        temp.sort();
        temp.toSink(sink);
        temp.clear();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        final Utf16Sink sink = Misc.getThreadLocalSink();
        sink.putAscii('[');
        // Do not try to print too much, it can hang IntelliJ debugger.
        for (int i = 0, k = Math.min(pos, 100); i < k; i++) {
            if (i > 0) {
                sink.putAscii(',');
            }
            sink.put(get(i));
        }
        if (pos > 100) {
            sink.putAscii(", .. ");
        }
        sink.putAscii(']');
        return sink.toString();
    }

    public void zero(int value) {
        Arrays.fill(data, 0, pos, value);
    }

    private boolean equals(LongList that) {
        if (this.pos != that.pos) {
            return false;
        }
        if (this.noEntryValue != that.noEntryValue) {
            return false;
        }
        for (int i = 0, n = pos; i < n; i++) {
            if (this.getQuick(i) != that.getQuick(i)) {
                return false;
            }
        }
        return true;
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

    long[] resetCapacityInternal(int longCapacity) {
        checkCapacity(longCapacity);
        return data;
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
