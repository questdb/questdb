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

import io.questdb.std.str.CharSink;

import java.util.Arrays;

public class LongList implements Mutable, LongVec {
    private static final int DEFAULT_ARRAY_SIZE = 16;
    private static final long DEFAULT_NO_ENTRY_VALUE = -1;
    private final long noEntryValue;
    private long[] buffer;
    private int pos = 0;

    public LongList() {
        this(DEFAULT_ARRAY_SIZE);
    }

    public LongList(int capacity) {
        this(capacity, DEFAULT_NO_ENTRY_VALUE);
    }

    public LongList(int capacity, long noEntryValue) {
        this.buffer = new long[capacity];
        this.noEntryValue = noEntryValue;
    }

    public LongList(LongList other) {
        this.buffer = new long[Math.max(other.size(), DEFAULT_ARRAY_SIZE)];
        setPos(other.size());
        System.arraycopy(other.buffer, 0, this.buffer, 0, pos);
        this.noEntryValue = other.noEntryValue;
    }

    public void add(long value) {
        ensureCapacity(pos + 1);
        buffer[pos++] = value;
    }

    public void add(long value0, long value1) {
        int n = pos;
        ensureCapacity(n + 2);
        buffer[n++] = value0;
        buffer[n++] = value1;
        pos = n;
    }

    public void add(long value0, long value1, long value2, long value3) {
        int n = pos;
        ensureCapacity(n + 4);
        buffer[n++] = value0;
        buffer[n++] = value1;
        buffer[n++] = value2;
        buffer[n++] = value3;
        pos = n;
    }

    public void add(long value0, long value1, long value2, long value3, long value4, long value5, long value6, long value7) {
        int n = pos;
        ensureCapacity(n + 8);
        buffer[n++] = value0;
        buffer[n++] = value1;
        buffer[n++] = value2;
        buffer[n++] = value3;
        buffer[n++] = value4;
        buffer[n++] = value5;
        buffer[n++] = value6;
        buffer[n++] = value7;
        pos = n;
    }

    public void add(LongList that) {
        add(that, 0, that.size());
    }

    public void add(LongList that, int lo, int hi) {
        int p = pos;
        int s = hi - lo;
        ensureCapacity(p + s);
        System.arraycopy(that.buffer, lo, this.buffer, p, s);
        pos += s;
    }

    public void add(int index, long element) {
        ensureCapacity(++pos);
        System.arraycopy(buffer, index, buffer, index + 1, pos - index - 1);
        buffer[index] = element;
    }

    public void arrayCopy(int srcPos, int dstPos, int length) {
        System.arraycopy(buffer, srcPos, buffer, dstPos, length);
    }

    public int binarySearch(long v) {
        int low = 0;
        int high = pos;

        while (low < high) {

            if (high - low < 65) {
                return scanSearch(v, low, high);
            }

            int mid = (low + high - 1) >>> 1;
            long midVal = buffer[mid];

            if (midVal < v)
                low = mid + 1;
            else if (midVal > v)
                high = mid;
            else
                return mid;
        }
        return -(low + 1);
    }

    public int binarySearchBlock(int low, int high, int shift, long v) {
        // Binary searches using 2^shift blocks
        // e.g. when shift == 2
        // this method treats 4 longs as 1 entry
        // taking first long for the comparisons
        // and ignoring the other 3 values.

        // This is useful when list is a dictionary where first long is a key
        // and subsequent X (1, 3, 7 etc.) values are the value of the dictionary.

        // assert that scan interval is integer number of blocks
        assert (high - low) % (1 << shift) == 0;
        high = high >> shift;
        low = low >> shift;

        while (low < high) {
            if (high - low < 65) {
                return scanSearchBlock(v, low, high, shift);
            }

            int mid = (low + high - 1) / 2;
            long midVal = buffer[mid << shift];

            if (midVal < v)
                low = mid + 1;
            else if (midVal > v)
                high = mid;
            else
                return mid << shift;
        }
        return -((low << shift) + 1);
    }

    public void clear() {
        pos = 0;
    }

    public void ensureCapacity(int capacity) {
        if (capacity < 0) {
            throw new IllegalArgumentException("Negative capacity. Integer overflow may be?");
        }

        int l = buffer.length;
        if (capacity > l) {
            int newCap = Math.max(l << 1, capacity);
            long[] buf = new long[newCap];
            System.arraycopy(buffer, 0, buf, 0, l);
            this.buffer = buf;
        }
    }

    public void erase() {
        pos = 0;
        Arrays.fill(buffer, noEntryValue);
    }

    public void extendAndSet(int index, long value) {
        ensureCapacity(index + 1);
        if (index >= pos) {
            pos = index + 1;
        }
        buffer[index] = value;
    }

    public void fill(int from, int to, long value) {
        Arrays.fill(buffer, from, to, value);
    }

    public long get(int index) {
        if (index < pos) {
            return buffer[index];
        }
        throw new ArrayIndexOutOfBoundsException(index);
    }

    public long getAndSetQuick(int index, long value) {
        long v = getQuick(index);
        buffer[index] = value;
        return v;
    }

    /**
     * Returns last element of the list or null if list is empty.
     *
     * @return last element of the list
     */
    public long getLast() {
        if (pos > 0) {
            return buffer[pos - 1];
        }
        return noEntryValue;
    }

    public void setLast(long value) {
        if (pos > 0) {
            buffer[pos - 1] = value;
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
        assert index < pos;
        return buffer[index];
    }

    public void setQuick(int index, long value) {
        assert index < pos;
        buffer[index] = value;
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
        for (int i = 0, k = size(); i < k; i++) {
            if (i > 0) {
                sb.put(',');
            }
            sb.put(get(i));
        }
        sb.put(']');
        return sb.toString();
    }

    public void increment(int index) {
        buffer[index] = buffer[index] + 1;
    }

    public void insert(int index, int length) {
        ensureCapacity(pos + length);
        if (pos > index) {
            System.arraycopy(buffer, index, buffer, index + length, pos - index);
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
            System.arraycopy(buffer, index + 1, buffer, index, move);
        }
        buffer[--pos] = noEntryValue;
    }

    public void removeIndexBlock(int index, int slotSize) {
        if (pos < 1 || index >= pos) {
            return;
        }
        int move = pos - index - slotSize;
        if (move > 0) {
            System.arraycopy(buffer, index + slotSize, buffer, index, move);
        }
        pos -= slotSize;
        Arrays.fill(buffer, pos, pos + slotSize, noEntryValue);
    }

    public void seed(int capacity, long value) {
        ensureCapacity(capacity);
        pos = capacity;
        fill(0, capacity, value);
    }

    public void seed(int fromIndex, int count, long value) {
        int capacity = fromIndex + count;
        ensureCapacity(capacity);
        Arrays.fill(buffer, fromIndex, capacity, value);
    }

    public void set(int index, long element) {
        if (index < pos) {
            buffer[index] = element;
            return;
        }
        throw new ArrayIndexOutOfBoundsException(index);
    }

    public void setAll(int capacity, long value) {
        ensureCapacity(capacity);
        pos = capacity;
        Arrays.fill(buffer, value);
    }

    final public void setPos(int pos) {
        ensureCapacity(pos);
        this.pos = pos;
    }

    public void shuffle(Rnd rnd) {
        for (int i = 0, sz = size(); i < sz; i++) {
            swap(i, rnd.nextPositiveInt() & (sz - 1));
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
        System.arraycopy(this.buffer, lo, that.buffer, 0, _hi - lo);
        that.pos = _hi - lo;
        return that;
    }

    public void zero(int value) {
        Arrays.fill(buffer, 0, pos, value);
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

    private int indexOf(long o) {
        for (int i = 0, n = pos; i < n; i++) {
            if (o == getQuick(i)) {
                return i;
            }
        }
        return -1;
    }

    private int scanSearch(long v, int low, int high) {
        for (int i = low; i < high; i++) {
            long f = buffer[i];
            if (f == v) {
                return i;
            }
            if (f > v) {
                return -(i + 1);
            }
        }
        return -(high + 1);
    }

    private int scanSearchBlock(long v, int low, int high, int bitHint) {
        for (int i = low; i < high; i++) {
            int index = i << bitHint;
            long f = buffer[index];
            if (f == v) {
                return index;
            }
            if (f > v) {
                return -(index + 1);
            }
        }
        return -((high << bitHint) + 1);
    }

    private void swap(int a, int b) {
        long tmp = getQuick(a);
        setQuick(a, getQuick(b));
        setQuick(b, tmp);
    }
}