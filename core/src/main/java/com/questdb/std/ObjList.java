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
import java.util.Comparator;

public class ObjList<T> implements Mutable, Sinkable {
    private static final int DEFAULT_ARRAY_SIZE = 16;
    private T[] buffer;
    private int pos = 0;

    @SuppressWarnings("unchecked")
    public ObjList() {
        this.buffer = (T[]) new Object[DEFAULT_ARRAY_SIZE];
    }

    @SuppressWarnings("unchecked")
    public ObjList(int capacity) {
        this.buffer = (T[]) new Object[Math.max(capacity, DEFAULT_ARRAY_SIZE)];
    }

    /**
     * {@inheritDoc}
     */
    public void add(T value) {
        ensureCapacity0(pos + 1);
        Unsafe.arrayPut(buffer, pos++, value);
    }

    public void addAll(ObjList<T> that) {
        int n = that.size();
        ensureCapacity0(pos + n);
        for (int i = 0; i < n; i++) {
            Unsafe.arrayPut(buffer, pos++, that.getQuick(i));
        }
    }

    /**
     * {@inheritDoc}
     */
    public void clear() {
        pos = 0;
        Arrays.fill(buffer, null);
    }

    public void extendAndSet(int index, T value) {
        ensureCapacity0(index + 1);
        if (index >= pos) {
            pos = index + 1;
        }
        Unsafe.arrayPut(buffer, index, value);
    }

    /**
     * {@inheritDoc}
     */
    public T get(int index) {
        if (index < pos) {
            return Unsafe.arrayGet(buffer, index);
        }
        throw new ArrayIndexOutOfBoundsException(index);
    }

    public T getAndSetQuick(int index, T value) {
        T v = Unsafe.arrayGet(buffer, index);
        Unsafe.arrayPut(buffer, index, value);
        return v;
    }

    /**
     * Returns last element of the list or null if list is empty.
     *
     * @return last element of the list
     */
    public T getLast() {
        if (pos > 0) {
            return Unsafe.arrayGet(buffer, pos - 1);
        }
        return null;
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
    public T getQuick(int index) {
        assert index < pos;
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
    public T getQuiet(int index) {
        if (index < pos) {
            return Unsafe.arrayGet(buffer, index);
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

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object that) {
        return this == that || that instanceof ObjList && equals((ObjList) that);
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

    /**
     * {@inheritDoc}
     */
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

    public void remove(int index) {
        if (pos < 1 || index >= pos) {
            return;
        }
        int move = pos - index - 1;
        if (move > 0) {
            System.arraycopy(buffer, index + 1, buffer, index, move);
        }
        Unsafe.arrayPut(buffer, --pos, null);
    }

    public void remove(int from, int to) {
        assert from <= to;
        int move = pos - from - (to - from) - 1;
        if (move > 0) {
            System.arraycopy(buffer, to + 1, buffer, from, move);
        }
        pos -= (to - from + 1);
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

    public void setAll(int capacity, T value) {
        ensureCapacity0(capacity);
        pos = capacity;
        Arrays.fill(buffer, value);
    }

    public void setPos(int capacity) {
        ensureCapacity0(capacity);
        pos = capacity;
    }

    public void setQuick(int index, T value) {
        Unsafe.arrayPut(buffer, index, value);
    }

    /**
     * {@inheritDoc}
     */
    public int size() {
        return pos;
    }

    public void sort(Comparator<T> cmp) {
        Arrays.sort(buffer, 0, pos, cmp);
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put('[');
        for (int i = 0, k = size(); i < k; i++) {
            if (i > 0) {
                sink.put(',');
            }
            T obj = getQuick(i);
            if (obj instanceof Sinkable) {
                sink.put((Sinkable) obj);
            } else if (obj == null) {
                sink.put("null");
            } else {
                sink.put(obj.toString());
            }
        }
        sink.put(']');
    }

    @SuppressWarnings("unchecked")
    private void ensureCapacity0(int capacity) {
        int l = buffer.length;
        if (capacity > l) {
            int newCap = Math.max(l << 1, capacity);
            T[] buf = (T[]) new Object[newCap];
            System.arraycopy(buffer, 0, buf, 0, l);
            this.buffer = buf;
        }
    }

    private boolean equals(ObjList that) {
        if (this.pos == that.pos) {
            for (int i = 0, n = pos; i < n; i++) {
                Object lhs = this.getQuick(i);
                if (lhs == null) {
                    return that.getQuick(i) == null;
                } else if (lhs.equals(that.getQuick(i))) {
                    return true;
                }
            }
        }
        return false;
    }

    private int indexOfNull() {
        for (int i = 0, n = pos; i < n; i++) {
            if (null == getQuick(i)) {
                return i;
            }
        }
        return -1;
    }
}