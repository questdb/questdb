/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.collections;

import com.nfsdb.utils.Unsafe;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Iterator;

public class ObjList<T> implements Mutable {
    private static final int DEFAULT_ARRAY_SIZE = 16;
    private final Iter iterator = new Iter();
    private T[] buffer;
    private int pos = 0;
    private StringBuilder toStringBuilder;

    @SuppressWarnings("unchecked")
    public ObjList() {
        this.buffer = (T[]) new Object[DEFAULT_ARRAY_SIZE];
    }

    @SuppressWarnings("unchecked")
    public ObjList(int capacity) {
        this.buffer = (T[]) new Object[capacity < DEFAULT_ARRAY_SIZE ? DEFAULT_ARRAY_SIZE : capacity];
    }

    /**
     * {@inheritDoc}
     */
    public void add(T value) {
        ensureCapacity0(pos + 1);
        Unsafe.arrayPut(buffer, pos++, value);
    }

    /**
     * {@inheritDoc}
     */
    public void clear() {
        pos = 0;
        Arrays.fill(buffer, null);
    }

    public void ensureCapacity(int capacity) {
        ensureCapacity0(capacity);
        pos = capacity;
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
        if (toStringBuilder == null) {
            toStringBuilder = new StringBuilder();
        }

        toStringBuilder.setLength(0);
        toStringBuilder.append('[');
        for (int i = 0, k = size(); i < k; i++) {
            if (i > 0) {
                toStringBuilder.append(',');
            }
            toStringBuilder.append(getQuick(i));
        }
        toStringBuilder.append(']');
        return toStringBuilder.toString();
    }

    /**
     * {@inheritDoc}
     */
    @NotNull
    public Iterator<T> iterator() {
        iterator.n = 0;
        return iterator;
    }

    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     */
    public boolean remove(Object o) {
        if (pos == 0) {
            return false;
        }
        int index = indexOf(o);
        if (index > -1) {
            remove(index);
            return true;
        }
        return false;
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

    /**
     * {@inheritDoc}
     */
    private int indexOf(Object o) {
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

    private int indexOfNull() {
        for (int i = 0, n = pos; i < n; i++) {
            if (null == getQuick(i)) {
                return i;
            }
        }
        return -1;
    }

    private class Iter implements Iterator<T> {
        private int n;

        @Override
        public boolean hasNext() {
            return n < pos;
        }

        @Override
        public T next() {
            return getQuick(n++);
        }

        @Override
        public void remove() {

        }
    }

}