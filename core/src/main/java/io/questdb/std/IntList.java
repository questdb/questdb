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

import io.questdb.std.str.CharSink;

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
        buffer[pos++] = value;
    }

    public void addAll(IntList that) {
        int p = pos;
        int s = that.size();
        ensureCapacity(p + s);
        System.arraycopy(that.buffer, 0, this.buffer, p, s);
    }

    public int indexOf(int v, int low, int high) {
        assert high <= pos;

        for (int i = low; i < high; i++) {
            int f = buffer[i];
            if (f == v) {
                return i;
            }
        }
        return -1;
    }

    public void setPos(int capacity) {
        ensureCapacity(capacity);
        pos = capacity;
    }

    public void arrayCopy(int srcPos, int dstPos, int length) {
        System.arraycopy(buffer, srcPos, buffer, dstPos, length);
    }

    public int binarySearch(int v) {
        int low = 0;
        int high = pos;

        while (low < high) {

            if (high - low < 65) {
                return scanSearch(v, low, high);
            }

            int mid = (low + high - 1) >>> 1;
            int midVal = buffer[mid];

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

    public void insert(int index, int element) {
        ensureCapacity(++pos);
        System.arraycopy(buffer, index, buffer, index + 1, pos - index - 1);
        buffer[index] = element;
    }

    public void extendAndSet(int index, int value) {
        ensureCapacity0(index + 1);
        if (index >= pos) {
            pos = index + 1;
        }
        buffer[index] = value;
    }

    public int get(int index) {
        return getQuick(index);
    }

    public int getLast() {
        if (pos > 0) {
            return buffer[pos - 1];
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
        assert index < pos;
        return buffer[index];
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
            return buffer[index];
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
        buffer[index] = buffer[index] + 1;
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
        int index1 = --pos;
        buffer[index1] = noEntryValue;
    }

    public void set(int index, int element) {
        if (index < pos) {
            buffer[index] = element;
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
        assert index < pos;
        buffer[index] = value;
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

    private int scanSearch(int v, int low, int high) {
        for (int i = low; i < high; i++) {
            int f = buffer[i];
            if (f == v) {
                return i;
            }
            if (f > v) {
                return -(i + 1);
            }
        }
        return -(high + 1);
    }

}