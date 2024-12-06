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

package io.questdb.std;

import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.Utf16Sink;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

public class IntList implements Mutable, Sinkable {
    private static final int DEFAULT_ARRAY_SIZE = 16;
    private static final int NO_ENTRY_VALUE = -1;
    private final int initialCapacity;
    private int[] data;
    private int pos = 0;

    public IntList() {
        this(DEFAULT_ARRAY_SIZE);
    }

    public IntList(int capacity) {
        this.initialCapacity = capacity;
        this.data = new int[initialCapacity];
    }

    public void add(int value) {
        checkCapacity(pos + 1);
        data[pos++] = value;
    }

    public void addAll(IntList that) {
        int p = pos;
        int s = that.size();
        setPos(p + s);
        System.arraycopy(that.data, 0, this.data, p, s);
    }

    public void arrayCopy(int srcPos, int dstPos, int length) {
        System.arraycopy(data, srcPos, data, dstPos, length);
    }

    public int binarySearchUniqueList(int v) {
        int low = 0;
        int high = pos - 1;
        while (high - low > 65) {
            int mid = (low + high) >>> 1;
            int midVal = data[mid];

            if (midVal < v)
                low = mid + 1;
            else if (midVal > v)
                high = mid - 1;
            else {
                return mid;
            }
        }
        return scanSearch(v, low, high + 1);
    }

    public int capacity() {
        return data.length;
    }

    public void checkCapacity(int capacity) {
        int l = data.length;
        if (capacity > l) {
            int newCap = Math.max(l << 1, capacity);
            int[] buf = new int[newCap];
            System.arraycopy(data, 0, buf, 0, l);
            this.data = buf;
        }
    }

    public void clear() {
        pos = 0;
    }

    public void clear(int capacity) {
        checkCapacity(capacity);
        pos = 0;
        Arrays.fill(data, NO_ENTRY_VALUE);
    }

    public boolean contains(int value) {
        return indexOf(value, 0, pos) > -1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object that) {
        return this == that || that instanceof IntList && equals((IntList) that);
    }

    public void extendAndSet(int index, int value) {
        checkCapacity(index + 1);
        if (index >= pos) {
            pos = index + 1;
        }
        data[index] = value;
    }

    public int get(int index) {
        return getQuick(index);
    }

    public int getLast() {
        if (pos > 0) {
            return data[pos - 1];
        }
        return NO_ENTRY_VALUE;
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
    public int getQuick(int index) {
        assert index < pos;
        return data[index];
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        int hashCode = 1;
        for (int i = 0, n = pos; i < n; i++) {
            int v = getQuick(i);
            hashCode = 31 * hashCode + (v == NO_ENTRY_VALUE ? 0 : v);
        }
        return hashCode;
    }

    public void increment(int index) {
        data[index] = data[index] + 1;
    }

    public void increment(int index, int delta) {
        assert delta > -1;
        data[index] = data[index] + delta;
    }

    public int indexOf(int v, int low, int high) {
        assert high <= pos;

        for (int i = low; i < high; i++) {
            int f = data[i];
            if (f == v) {
                return i;
            }
        }
        return -1;
    }

    public void insert(int index, int element) {
        setPos(++pos);
        System.arraycopy(data, index, data, index + 1, pos - index - 1);
        data[index] = element;
    }

    // increment at index and return previous value
    public int postIncrement(int index) {
        final int prev = data[index];
        data[index] = prev + 1;
        return prev;
    }

    public void remove(int key) {
        for (int i = 0, n = size(); i < n; i++) {
            if (key == getQuick(i)) {
                removeIndex(i);
                return;
            }
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
        int index1 = --pos;
        data[index1] = NO_ENTRY_VALUE;
    }

    public void restoreInitialCapacity() {
        data = new int[initialCapacity];
        pos = 0;
    }

    public void set(int index, int element) {
        if (index < pos) {
            data[index] = element;
            return;
        }
        throw new ArrayIndexOutOfBoundsException(index);
    }

    public void setAll(int capacity, int value) {
        checkCapacity(capacity);
        pos = capacity;
        Arrays.fill(data, 0, pos, value);
    }

    public void setPos(int position) {
        checkCapacity(position);
        pos = position;
    }

    public void setQuick(int index, int value) {
        assert index < pos;
        data[index] = value;
    }

    public int size() {
        return pos;
    }

    /**
     * Sorts groups of N elements. The size of the group is specified by {@code groupSize}.
     * Comparison between groups is done by comparing the first element of each group, then
     * if the first elements are equial the second elements are compared and so on.
     *
     * @param groupSize size of the group
     */
    public void sortGroups(int groupSize) {
        if (groupSize > 0 && pos % groupSize == 0) {
            IntGroupSort.quickSort(groupSize, data, 0, pos / groupSize);
            return;
        }
        throw new IllegalStateException("sorting not supported for group size: " + groupSize + ", length: " + pos);
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.putAscii('[');
        for (int i = 0, k = size(); i < k; i++) {
            if (i > 0) {
                sink.putAscii(',');
            }
            sink.put(get(i));
        }
        sink.putAscii(']');
    }

    public void toSink(CharSink<?> sink, int exceptValue) {
        sink.putAscii('[');
        boolean pastFirst = false;
        for (int i = 0, k = size(); i < k; i++) {
            if (pastFirst) {
                sink.putAscii(',');
            }
            int val = get(i);
            if (val == exceptValue) {
                continue;
            }
            sink.put(val);
            pastFirst = true;
        }
        sink.putAscii(']');
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        Utf16Sink b = Misc.getThreadLocalSink();
        toSink(b);
        return b.toString();
    }

    public void zero(int value) {
        Arrays.fill(data, 0, pos, value);
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
            int f = data[i];
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
