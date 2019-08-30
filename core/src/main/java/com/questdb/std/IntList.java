/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.std;

import com.questdb.std.str.CharSink;

import java.util.Arrays;

public class IntList implements Mutable {
    private static final int DEFAULT_ARRAY_SIZE = 16;
    private static final int noEntryValue = -1;
    private int[] buffer;
    private int pos = 0;

    public IntList() {
        this(DEFAULT_ARRAY_SIZE);
    }

    public IntList(int capacity) {
        this.buffer = new int[Math.max(capacity, DEFAULT_ARRAY_SIZE)];
    }

    public void add(int value) {
        ensureCapacity0(pos + 1);
        Unsafe.arrayPut(buffer, pos++, value);
    }

    public void add(int index, int element) {
        ensureCapacity(++pos);
        System.arraycopy(buffer, index, buffer, index + 1, pos - index - 1);
        Unsafe.arrayPut(buffer, index, element);
    }

    public void addAll(IntList that) {
        int p = pos;
        int s = that.size();
        ensureCapacity(p + s);
        System.arraycopy(that.buffer, 0, this.buffer, p, s);
    }

    public void arrayCopy(int srcPos, int dstPos, int length) {
        System.arraycopy(buffer, srcPos, buffer, dstPos, length);
    }

    public int binarySearch(int v) {
        int low = 0;
        int high = pos;

        while (low < high) {

            if (high - low < 65) {
                return scanSearch(v);
            }

            int mid = (low + high - 1) >>> 1;
            long midVal = Unsafe.arrayGet(buffer, mid);

            if (midVal < v)
                low = mid + 1;
            else if (midVal > v)
                high = mid;
            else
                return mid;
        }
        return -(low + 1);
    }

    public void clear() {
        pos = 0;
        // this is removed to improve performance
        // erase() method should be added if entire buffer has to be erased as well
//        Arrays.fill(buffer, noEntryValue);
    }

    public void clear(int capacity) {
        ensureCapacity(capacity);
        pos = 0;
        Arrays.fill(buffer, noEntryValue);
    }

    public void ensureCapacity(int capacity) {
        ensureCapacity0(capacity);
        pos = capacity;
    }

    public void extendAndSet(int index, int value) {
        ensureCapacity0(index + 1);
        if (index >= pos) {
            pos = index + 1;
        }
        Unsafe.arrayPut(buffer, index, value);
    }

    public int get(int index) {
        if (index < pos) {
            return Unsafe.arrayGet(buffer, index);
        }
        throw new ArrayIndexOutOfBoundsException(index);
    }

    public int getLast() {
        if (pos > 0) {
            return Unsafe.arrayGet(buffer, pos - 1);
        }
        return noEntryValue;
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
    public int getQuick(int index) {
        return Unsafe.arrayGet(buffer, index);
    }

    /**
     * Returns element at the specified position or null, if element index is
     * out of bounds. This is an alternative to throwing runtime exception or
     * doing preemptive check.
     *
     * @param index position of element
     * @return element at the specified position.
     */
    public int getQuiet(int index) {
        if (index < pos) {
            return Unsafe.arrayGet(buffer, index);
        }
        return noEntryValue;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        int hashCode = 1;
        for (int i = 0, n = pos; i < n; i++) {
            int v = getQuick(i);
            hashCode = 31 * hashCode + (v == noEntryValue ? 0 : v);
        }
        return hashCode;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object that) {
        return this == that || that instanceof IntList && equals((IntList) that);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        CharSink b = Misc.getThreadLocalBuilder();

        b.put('[');
        for (int i = 0, k = size(); i < k; i++) {
            if (i > 0) {
                b.put(',');
            }
            b.put(get(i));
        }
        b.put(']');
        return b.toString();
    }

    public void increment(int index) {
        Unsafe.arrayPut(buffer, index, Unsafe.arrayGet(buffer, index) + 1);
    }

    public void remove(int key) {
        for (int i = 0, n = size(); i < n; i++) {
            if (key == getQuick(i)) {
                removeIndex(i);
                return;
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    public void removeIndex(int index) {
        if (pos < 1 || index >= pos) {
            return;
        }
        int move = pos - index - 1;
        if (move > 0) {
            System.arraycopy(buffer, index + 1, buffer, index, move);
        }
        Unsafe.arrayPut(buffer, --pos, noEntryValue);
    }

    public void set(int index, int element) {
        if (index < pos) {
            Unsafe.arrayPut(buffer, index, element);
            return;
        }
        throw new ArrayIndexOutOfBoundsException(index);
    }

    public void setAll(int capacity, int value) {
        ensureCapacity0(capacity);
        pos = capacity;
        Arrays.fill(buffer, value);
    }

    public void setQuick(int index, int value) {
        Unsafe.arrayPut(buffer, index, value);
    }

    public int size() {
        return pos;
    }

    public void zero(int value) {
        Arrays.fill(buffer, 0, pos, value);
    }

    private void ensureCapacity0(int capacity) {
        int l = buffer.length;
        if (capacity > l) {
            int newCap = Math.max(l << 1, capacity);
            int[] buf = new int[newCap];
            System.arraycopy(buffer, 0, buf, 0, l);
            this.buffer = buf;
        }
    }

    private boolean equals(IntList that) {
        if (this.pos != that.pos) {
            return false;
        }

        for (int i = 0, n = pos; i < n; i++) {
            if (this.getQuick(i) != that.getQuick(i)) {
                return false;
            }
        }
        return true;
    }

    private int scanSearch(int v) {
        int sz = size();
        for (int i = 0; i < sz; i++) {
            long f = getQuick(i);
            if (f == v) {
                return i;
            }
            if (f > v) {
                return -(i + 1);
            }
        }
        return -(sz + 1);
    }

}