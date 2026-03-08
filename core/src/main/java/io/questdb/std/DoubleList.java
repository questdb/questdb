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

import java.util.Arrays;

public class DoubleList implements Mutable, Sinkable {
    public static final double DEFAULT_NO_ENTRY_VALUE = -1L;
    private static final int DEFAULT_ARRAY_SIZE = 16;
    private final double noEntryValue;
    private double[] data;
    private int pos = 0;

    public DoubleList() {
        this(DEFAULT_ARRAY_SIZE);
    }

    public DoubleList(int capacity) {
        this(capacity, DEFAULT_NO_ENTRY_VALUE);
    }

    public DoubleList(int capacity, double noEntryValue) {
        this.data = new double[capacity];
        this.noEntryValue = noEntryValue;
    }

    public DoubleList(DoubleList other) {
        this.data = new double[Math.max(other.size(), DEFAULT_ARRAY_SIZE)];
        setPos(other.size());
        System.arraycopy(other.data, 0, this.data, 0, pos);
        this.noEntryValue = other.noEntryValue;
    }

    public DoubleList(double[] other) {
        this.data = new double[other.length];
        setPos(other.length);
        System.arraycopy(other, 0, this.data, 0, pos);
        this.noEntryValue = DEFAULT_NO_ENTRY_VALUE;
    }

    public void add(double value) {
        checkCapacity(pos + 1);
        data[pos++] = value;
    }

    public void add(int index, double element) {
        checkCapacity(++pos);
        System.arraycopy(data, index, data, index + 1, pos - index - 1);
        data[index] = element;
    }

    public int binarySearch(double value, int scanDir) {

        // this is the same algorithm as implemented in C (util.h)
        // template<class T, class V>
        // inline int64_t binary_search(T *data, V value, int64_t low, int64_t high, int32_t scan_dir)
        // please ensure these implementations are in sync

        int low = 0;
        int high = pos - 1;
        while (high - low > 65) {
            final int mid = (low + high) >>> 1;
            final double midVal = data[mid];
            int cmp = Numbers.compare(midVal, value);

            if (cmp < 0) {
                low = mid;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                // In case of multiple equal values, find the first
                return scanDir == Vect.BIN_SEARCH_SCAN_UP ?
                        scrollUp(mid, midVal) :
                        scrollDown(mid, high, midVal);
            }
        }
        return scanDir == Vect.BIN_SEARCH_SCAN_UP ?
                scanUp(value, low, high + 1) :
                scanDown(value, low, high + 1);
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

    @Override
    public boolean equals(Object that) {
        return this == that || that instanceof DoubleList && equals((DoubleList) that);
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
    public void extendAndSet(int index, double value) {
        checkCapacity(index + 1);
        if (index >= pos) {
            pos = index + 1;
        }
        data[index] = value;
    }

    public void fill(int from, int to, double value) {
        Arrays.fill(data, from, to, value);
    }

    public double get(int index) {
        if (index < pos) {
            return data[index];
        }
        throw new ArrayIndexOutOfBoundsException(index);
    }

    /**
     * Returns last element of the list or null if list is empty.
     *
     * @return last element of the list
     */
    public double getLast() {
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
    public double getQuick(int index) {
        return data[index];
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        long hashCode = 1;
        for (int i = 0, n = pos; i < n; i++) {
            double v = getQuick(i);
            hashCode = 31 * hashCode + (v == noEntryValue ? 0 : Double.doubleToLongBits(v));
        }
        return (int) hashCode;
    }

    public int indexOf(double o) {
        for (int i = 0, n = pos; i < n; i++) {
            if (Numbers.equals(o, getQuick(i))) {
                return i;
            }
        }
        return -1;
    }

    public void remove(double v) {
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

    public void reverse() {
        int n = size();
        for (int i = 0, m = n / 2; i < m; i++) {
            double tmp = data[i];
            data[i] = data[n - i - 1];
            data[n - i - 1] = tmp;
        }
    }

    public void set(int index, double element) {
        if (index < pos) {
            data[index] = element;
            return;
        }
        throw new ArrayIndexOutOfBoundsException(index);
    }

    public void setAll(int capacity, double value) {
        checkCapacity(capacity);
        pos = capacity;
        Arrays.fill(data, value);
    }

    public void setLast(double value) {
        if (pos > 0) {
            data[pos - 1] = value;
        }
    }

    public final void setPos(int pos) {
        checkCapacity(pos);
        this.pos = pos;
    }

    public void setQuick(int index, double value) {
        assert index < pos;
        data[index] = value;
    }

    public int size() {
        return pos;
    }

    public void sort() {
        Arrays.sort(data, 0, size());
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

    public void toSink(CharSink<?> sink, double exceptValue) {
        sink.putAscii('[');
        boolean pastFirst = false;
        for (int i = 0, k = size(); i < k; i++) {
            double val = get(i);
            if (Numbers.equals(val, exceptValue)) {
                continue;
            }
            if (pastFirst) {
                sink.putAscii(',');
            }
            sink.put(val);
            pastFirst = true;
        }
        sink.putAscii(']');
    }

    @Override
    public String toString() {
        final Utf16Sink sb = Misc.getThreadLocalSink();
        toSink(sb);
        return sb.toString();
    }

    private boolean equals(DoubleList that) {
        if (this.pos != that.pos) {
            return false;
        }
        if (Numbers.compare(this.noEntryValue, that.noEntryValue) != 0) {
            return false;
        }
        for (int i = 0, n = pos; i < n; i++) {
            if (Numbers.compare(this.getQuick(i), that.getQuick(i)) != 0) {
                return false;
            }
        }
        return true;
    }

    private int scanDown(double value, int low, int high) {
        for (int i = high - 1; i >= low; i--) {
            double that = data[i];
            int cmp = Numbers.compare(that, value);
            if (cmp == 0) {
                return i;
            }
            if (cmp < 0) {
                return -(i + 2);
            }
        }
        return -(low + 1);
    }

    private int scanUp(double value, int low, int high) {
        for (int i = low; i < high; i++) {
            double that = data[i];
            int cmp = Numbers.compare(that, value);
            if (cmp == 0) {
                return i;
            }
            if (cmp > 0) {
                return -(i + 1);
            }
        }
        return -(high + 1);
    }

    private int scrollDown(int low, int high, double value) {
        do {
            if (low < high) {
                low++;
            } else {
                return low;
            }
        } while (Numbers.compare(data[low], value) == 0);
        return low - 1;
    }

    private int scrollUp(int high, double value) {
        do {
            if (high > 0) {
                high--;
            } else {
                return 0;
            }
        } while (Numbers.compare(data[high], value) == 0);
        return high + 1;
    }
}
