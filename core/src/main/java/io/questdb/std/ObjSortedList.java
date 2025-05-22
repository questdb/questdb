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

import org.jetbrains.annotations.NotNull;

import java.util.Comparator;

public class ObjSortedList<T> implements Mutable {
    private static final int DEFAULT_CAPACITY = 16;
    private final Comparator<T> comparator;
    private T[] buffer;
    private int size;

    @SuppressWarnings("unchecked")
    public ObjSortedList(Comparator<T> comparator) {
        this.comparator = comparator;
        this.buffer = (T[]) new Object[DEFAULT_CAPACITY];
        this.size = 0;
    }

    public void add(@NotNull T value) {
        int p = binSearch(value);
        if (size == buffer.length) {
            resize();
        }
        if (p < size) {
            System.arraycopy(buffer, p, buffer, p + 1, size - p);
        }
        buffer[p] = value;
        size++;
    }

    @Override
    public void clear() {
        for (int i = 0; i < size; i++) {
            buffer[i] = null;
        }
        size = 0;
    }

    public T get(int index) {
        return buffer[index];
    }

    public int getCapacity() {
        return buffer.length;
    }

    public T poll() {
        T v = buffer[0];
        if (size > 0) {
            buffer[0] = null;
            --size;
            System.arraycopy(buffer, 1, buffer, 0, size);
        }
        return v;
    }

    public void popMany(int n, ObjList<T> target) {
        if (size > 0) {
            n = Math.min(size, n);
            for (int i = 0; i < n; i++) {
                target.add(buffer[i]);
                buffer[i] = null;
            }
            size -= n;
            System.arraycopy(buffer, n, buffer, 0, size);
        }
    }

    public void remove(@NotNull T value) {
        int p = binSearch(value);
        for (int i = p - 1; i >= 0; i--) {
            if (buffer[i].equals(value)) {
                if (i == size - 1) {
                    buffer[i] = null;
                } else {
                    System.arraycopy(buffer, i + 1, buffer, i, size - i);
                }
                size--;
                break;
            }
            if (comparator.compare(buffer[i], value) != 0) {
                break;
            }
        }
    }

    public int size() {
        return size;
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private int binSearch(T value) {
        int low = 0;
        int high = size;

        while (high - low > 65) {
            int mid = (low + high - 1) >>> 1;
            T midVal = buffer[mid];

            final int c = comparator.compare(midVal, value);
            if (c < 0) {
                low = mid + 1;
            } else if (c > 0) {
                high = mid;
            } else {
                while (++mid < high && comparator.compare(buffer[mid], value) == 0) {
                }
                return mid;
            }
        }
        return scanSearch(value, low);
    }

    @SuppressWarnings("unchecked")
    private void resize() {
        T[] tmp = (T[]) new Object[buffer.length * 2];
        System.arraycopy(buffer, 0, tmp, 0, buffer.length);
        buffer = tmp;
    }

    private int scanSearch(T value, int low) {
        for (int i = low; i < size; i++) {
            if (comparator.compare(buffer[i], value) > 0) {
                return i;
            }
        }
        return size;
    }
}
