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

public class DirectLongLongAscList implements DirectLongLongSortedList {
    private final Cursor cursor = new Cursor();
    private final int memoryTag;
    private int capacity;
    private long ptr;
    private int size;

    public DirectLongLongAscList(int capacity, int memoryTag) {
        this.capacity = capacity;
        this.memoryTag = memoryTag;
        this.ptr = Unsafe.malloc(16L * capacity, memoryTag);
        this.size = 0;
    }

    @Override
    public void add(long index, long value) {
        // fast path
        if (size == capacity && Unsafe.getUnsafe().getLong(ptr + 16L * (size - 1)) <= value) {
            return;
        }
        // slow path
        int p = binSearch(value);
        if (p == capacity) {
            return;
        }
        if (p < capacity - 1) {
            Vect.memmove(ptr + 16L * (p + 1), ptr + 16L * p, 16L * (capacity - p - 1));
        }
        Unsafe.getUnsafe().putLong(ptr + 16L * p, value);
        Unsafe.getUnsafe().putLong(ptr + 16L * p + 8, index);
        size = Math.min(capacity, size + 1);
    }

    @Override
    public void clear() {
        size = 0;
        cursor.toTop();
    }

    @Override
    public void close() {
        if (ptr != 0) {
            ptr = Unsafe.free(ptr, 16L * capacity, memoryTag);
            size = 0;
        }
    }

    @Override
    public int getCapacity() {
        return capacity;
    }

    @Override
    public Cursor getCursor() {
        return cursor;
    }

    @Override
    public int getOrder() {
        return DirectLongLongSortedList.ASC_ORDER;
    }

    @Override
    public void reopen(int capacity) {
        if (ptr == 0) {
            ptr = Unsafe.malloc(16L * capacity, memoryTag);
            this.capacity = capacity;
            clear();
        }
    }

    @Override
    public void reopen() {
        if (ptr == 0) {
            ptr = Unsafe.malloc(16L * capacity, memoryTag);
            clear();
        }
    }

    @Override
    public int size() {
        return size;
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private int binSearch(long v) {
        int low = 0;
        int high = size;

        while (high - low > 65) {
            int mid = (low + high - 1) >>> 1;
            long midVal = Unsafe.getUnsafe().getLong(ptr + 16L * mid);

            if (midVal < v) {
                low = mid + 1;
            } else if (midVal > v) {
                high = mid;
            } else {
                while (++mid < high && Unsafe.getUnsafe().getLong(ptr + 16L * mid) == v) {
                }
                return mid;
            }
        }
        return scanSearch(v, low);
    }

    private int scanSearch(long v, int low) {
        for (int i = low; i < size; i++) {
            if (Unsafe.getUnsafe().getLong(ptr + 16L * i) > v) {
                return i;
            }
        }
        return size;
    }

    public class Cursor implements DirectLongLongSortedList.Cursor {
        private int pos = -1;

        @Override
        public boolean hasNext() {
            return ++pos < size;
        }

        @Override
        public long index() {
            return Unsafe.getUnsafe().getLong(ptr + 16L * pos + 8);
        }

        @Override
        public void toTop() {
            pos = -1;
        }

        @Override
        public long value() {
            return Unsafe.getUnsafe().getLong(ptr + 16L * pos);
        }
    }
}
