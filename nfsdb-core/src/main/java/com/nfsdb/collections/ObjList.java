/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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

import com.nfsdb.utils.Numbers;
import com.nfsdb.utils.Unsafe;

import java.util.Arrays;

public class ObjList<T> {
    public static final int DEFAULT_ARRAY_SIZE = 16;
    private T[] buffer;
    private int pos = 0;
    private StringBuilder toStringBuilder;

    @SuppressWarnings("unchecked")
    public ObjList() {
        this.buffer = (T[]) new Object[DEFAULT_ARRAY_SIZE];
    }

    @SuppressWarnings("unchecked")
    public ObjList(int capacity) {
        this.buffer = (T[]) new Object[capacity < DEFAULT_ARRAY_SIZE ? DEFAULT_ARRAY_SIZE : Numbers.ceilPow2(capacity)];
    }

    public void add(T value) {
        if (pos == buffer.length) {
            extend(pos << 1);
        }
        Unsafe.arrayPut(buffer, pos++, value);
    }


    public void clear() {
        pos = 0;
        Arrays.fill(buffer, null);
    }

    public void ensureCapacity(int capacity) {
        if (capacity > buffer.length) {
            extend(capacity);
        }
        pos = capacity;
    }

    public void extendAndSet(int index, T value) {
        if (index >= buffer.length) {
            extend(Numbers.ceilPow2(index));
            pos = index + 1;
        } else if (index >= pos) {
            pos = index + 1;
        }

        setQuick(index, value);
    }

    @SuppressWarnings("unchecked")
    public T get(int index) {
        if (index < pos) {
            return Unsafe.arrayGet(buffer, index);
        }
        throw new ArrayIndexOutOfBoundsException(index);
    }

    public T getAndSet(int index, T value) {
        T v = Unsafe.arrayGet(buffer, index);
        Unsafe.arrayPut(buffer, index, value);
        return v;
    }

    @SuppressWarnings("unchecked")
    public T getLast() {
        if (pos > 0) {
            return Unsafe.arrayGet(buffer, pos - 1);
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public T getQuick(int index) {
        return Unsafe.arrayGet(buffer, index);
    }

    @SuppressWarnings("unchecked")
    public T getQuiet(int index) {
        if (index < pos) {
            return Unsafe.arrayGet(buffer, index);
        }
        return null;
    }

    public T remove(int index) {
        if (pos < 1 || index >= pos) {
            return null;
        }
        T v = Unsafe.arrayGet(buffer, index);
        int move = pos - index - 1;
        if (move > 0) {
            System.arraycopy(buffer, index + 1, buffer, index, move);
        }
        Unsafe.arrayPut(buffer, --pos, null);
        return v;
    }

    public boolean remove(T obj) {

        if (obj == null) {
            return false;
        }

        int p = pos;
        int index = -1;
        for (int i = 0; i < p; i++) {
            if (obj == Unsafe.arrayGet(buffer, i) || obj.equals(Unsafe.arrayGet(buffer, i))) {
                index = i;
                break;
            }
        }

        if (index > -1) {
            remove(index);
            return true;
        }

        return false;
    }

    public void setQuick(int index, T value) {
        Unsafe.arrayPut(buffer, index, value);
    }

    public int size() {
        return pos;
    }

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
            toStringBuilder.append(get(i));
        }
        toStringBuilder.append(']');
        return toStringBuilder.toString();
    }

    @SuppressWarnings("unchecked")
    private void extend(int capacity) {
        T[] buf = (T[]) new Object[capacity];
        System.arraycopy(buffer, 0, buf, 0, buffer.length);
        this.buffer = buf;
    }
}
