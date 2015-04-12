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

public class ObjList<T> {
    public static final int DEFAULT_ARRAY_SIZE = 16;
    private static final long OFFSET = Unsafe.getUnsafe().arrayBaseOffset(Object[].class);
    private static final long SCALE = Unsafe.getUnsafe().arrayIndexScale(Object[].class);
    private Object[] buffer;
    private int pos = 0;
    private StringBuilder toStringBuilder;

    public ObjList() {
        this.buffer = new Object[DEFAULT_ARRAY_SIZE];
    }

    public ObjList(int capacity) {
        this.buffer = new Object[capacity < DEFAULT_ARRAY_SIZE ? DEFAULT_ARRAY_SIZE : Numbers.ceilPow2(capacity)];
    }

    public void add(T value) {
        if (pos == buffer.length) {
            extend(pos << 1);
        }
        setQuick(pos++, value);
    }

    public void clear() {
        pos = 0;
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
            return (T) Unsafe.getUnsafe().getObject(buffer, OFFSET + index * SCALE);
        }
        throw new ArrayIndexOutOfBoundsException(index);
    }

    @SuppressWarnings("unchecked")
    public T getQuick(int index) {
        return (T) Unsafe.getUnsafe().getObject(buffer, OFFSET + index * SCALE);
    }

    @SuppressWarnings("unchecked")
    public T getQuiet(int index) {
        if (index < pos) {
            return (T) Unsafe.getUnsafe().getObject(buffer, OFFSET + index * SCALE);
        }
        return null;
    }

    public T setQuick(int index, T value) {
        T v = getQuick(index);
        Unsafe.getUnsafe().putObject(buffer, OFFSET + index * SCALE, value);
        return v;
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

    private void extend(int capacity) {
        Object[] buf = new Object[capacity];
        for (int i = 0, k = buffer.length; i < k; i++) {
            long p = OFFSET + i * SCALE;
            Unsafe.getUnsafe().putObject(buf, p, Unsafe.getUnsafe().getObject(buffer, p));
        }
        this.buffer = buf;
    }
}
