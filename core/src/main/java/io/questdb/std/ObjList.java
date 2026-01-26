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
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Comparator;

public class ObjList<T> implements Mutable, Sinkable, ReadOnlyObjList<T> {
    private static final int DEFAULT_ARRAY_SIZE = 16;
    private T[] buffer;
    private int pos = 0;

    @SuppressWarnings("unchecked")
    public ObjList() {
        this.buffer = (T[]) new Object[DEFAULT_ARRAY_SIZE];
    }

    @SuppressWarnings("unchecked")
    public ObjList(ObjList<? extends T> other) {
        this.buffer = (T[]) new Object[Math.max(other.size(), DEFAULT_ARRAY_SIZE)];
        setPos(other.size());
        System.arraycopy(other.buffer, 0, this.buffer, 0, pos);
    }

    @SafeVarargs
    @SuppressWarnings("unchecked")
    public ObjList(T... other) {
        this.buffer = (T[]) new Object[Math.max(other.length, DEFAULT_ARRAY_SIZE)];
        setPos(other.length);
        System.arraycopy(other, 0, this.buffer, 0, pos);
    }

    @SuppressWarnings("unchecked")
    public ObjList(int capacity) {
        this.buffer = (T[]) new Object[Math.max(capacity, DEFAULT_ARRAY_SIZE)];
    }

    public void add(T value) {
        checkCapacity(pos + 1);
        buffer[pos++] = value;
    }

    public void addAll(ReadOnlyObjList<? extends T> that) {
        int n = that.size();
        checkCapacity(pos + n);
        for (int i = 0; i < n; i++) {
            buffer[pos++] = that.getQuick(i);
        }
    }

    public void addAll(ReadOnlyObjList<? extends T> that, int lo, int hi) {
        int n = hi - lo;
        checkCapacity(pos + n);
        for (int i = lo; i < hi; i++) {
            buffer[pos++] = that.getQuick(i);
        }
    }

    public void addReverseAll(ReadOnlyObjList<? extends T> that) {
        int n = that.size();
        checkCapacity(pos + n);
        for (int i = n - 1; i >= 0; i--) {
            buffer[pos++] = that.getQuick(i);
        }
    }

    @SuppressWarnings("unchecked")
    public void checkCapacity(int capacity) {
        int l = buffer.length;
        if (capacity > l) {
            int newCap = Math.max(l << 1, capacity);
            T[] buf = (T[]) new Object[newCap];
            System.arraycopy(buffer, 0, buf, 0, l);
            this.buffer = buf;
        }
    }

    @Override
    public void clear() {
        if (pos > 0) {
            Arrays.fill(buffer, null);
        }
        pos = 0;
    }

    public boolean contains(T value) {
        for (int i = 0, n = pos; i < n; i++) {
            T o = getQuick(i);
            if ((value == null && o == null) ||
                    (value != null && value.equals(o))) {
                return true;
            }
        }

        return false;
    }

    @Override
    public ObjList<T> copy() {
        return new ObjList<>(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object that) {
        return this == that || that instanceof ObjList && equals((ObjList<?>) that);
    }

    public void extendAndSet(int index, T value) {
        checkCapacity(index + 1);
        if (index >= pos) {
            pos = index + 1;
        }
        buffer[index] = value;
    }

    public void extendPos(int capacity) {
        checkCapacity(capacity);
        pos = Math.max(pos, capacity);
    }

    @Override
    public T get(int index) {
        if (index < pos) {
            return buffer[index];
        }
        throw new ArrayIndexOutOfBoundsException(index);
    }

    public T getAndSetQuick(int index, T value) {
        assert index < pos : "index out of bounds, " + index + " >= " + pos;
        T v = buffer[index];
        buffer[index] = value;
        return v;
    }

    /**
     * Returns last element of the list or null if list is empty.
     *
     * @return last element of the list
     */
    @Override
    public T getLast() {
        if (pos > 0) {
            return buffer[pos - 1];
        }
        return null;
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
    @Override
    public T getQuick(int index) {
        assert index < pos : "index out of bounds, " + index + " >= " + pos;
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
    @Override
    public T getQuiet(int index) {
        if (index < pos) {
            return buffer[index];
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        int hashCode = 1;
        for (int i = 0, n = pos; i < n; i++) {
            T o = getQuick(i);
            hashCode = 31 * hashCode + (o == null ? 0 : o.hashCode());
        }
        return hashCode;
    }

    @Override
    public int indexOf(Object o) {
        if (o == null) {
            return indexOfNull();
        } else {
            for (int i = 0, n = pos; i < n; i++) {
                if (o.equals(getQuick(i))) {
                    return i;
                }
            }
            return -1;
        }
    }

    public int indexOfNull() {
        for (int i = 0, n = pos; i < n; i++) {
            if (getQuick(i) == null) {
                return i;
            }
        }
        return -1;
    }

    public void insert(int index, int length, T defaultValue) {
        checkCapacity(pos + length);
        if (pos > index) {
            System.arraycopy(buffer, index, buffer, index + length, pos - index);
        }
        Arrays.fill(buffer, index, index + length, defaultValue);
        pos += length;
    }

    public void remove(int index) {
        if (pos < 1 || index >= pos) {
            return;
        }
        int move = pos - index - 1;
        if (move > 0) {
            System.arraycopy(buffer, index + 1, buffer, index, move);
        }
        buffer[--pos] = null;
    }

    public void remove(int from, int to) {
        assert from <= to : "start index is greater than end index, " + from + " > " + to;
        int move = pos - from - (to - from) - 1;
        if (move > 0) {
            System.arraycopy(buffer, to + 1, buffer, from, move);
        }
        pos = Math.max(0, pos - (to - from + 1));
        Arrays.fill(buffer, pos, buffer.length - 1, null);
    }

    public int remove(Object o) {
        if (pos == 0) {
            return -1;
        }
        int index = indexOf(o);
        if (index > -1) {
            remove(index);
            return index;
        }
        return -1;
    }

    public void set(int from, int to, T value) {
        Arrays.fill(buffer, from, Math.min(buffer.length, to), value);
    }

    public void set(int index, T value) {
        buffer[index] = value;
    }

    public void setAll(int count, T value) {
        checkCapacity(count);
        pos = count;
        Arrays.fill(buffer, value);
    }

    public void setPos(int newPos) {
        checkCapacity(newPos);
        pos = newPos;
    }

    public void setQuick(int index, T value) {
        assert index < pos : "index out of bounds, " + index + " >= " + pos;
        buffer[index] = value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int size() {
        return pos;
    }

    public void sort(Comparator<T> cmp) {
        sort(0, pos, cmp);
    }

    public void sort(int from, int to, Comparator<T> cmp) {
        Arrays.sort(buffer, from, to, cmp);
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        toSink(sink, 0, size());
    }

    public void toSink(CharSink<?> sink, int from) {
        toSink(sink, from, size());
    }

    public void toSink(CharSink<?> sink, int from, int to) {
        sink.putAscii('[');
        for (int i = from; i < to; i++) {
            if (i > from) {
                sink.putAscii(',');
            }
            T obj = getQuick(i);
            if (obj instanceof Sinkable) {
                sink.put((Sinkable) obj);
            } else if (obj == null) {
                sink.putAscii("null");
            } else {
                sink.put(obj.toString());
            }
        }
        sink.putAscii(']');
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();

        b.setLength(0);
        b.append('[');
        for (int i = 0, k = size(); i < k; i++) {
            if (i > 0) {
                b.append(',');
            }
            b.append(getQuick(i));
        }
        b.append(']');
        return b.toString();
    }

    private boolean equals(ObjList<?> that) {
        if (this.pos == that.pos) {
            for (int i = 0, n = pos; i < n; i++) {
                Object lhs = this.getQuick(i);
                if (lhs == null) {
                    if (that.getQuick(i) != null) {
                        return false;
                    }
                } else if (!lhs.equals(that.getQuick(i))) {
                    return false;
                }
            }

            return true;
        }
        return false;
    }
}
