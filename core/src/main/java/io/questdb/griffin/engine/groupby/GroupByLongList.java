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

package io.questdb.griffin.engine.groupby;

import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

/**
 * Specialized flyweight long list used in {@link io.questdb.griffin.engine.functions.GroupByFunction}s.
 * <p>
 * Uses provided {@link GroupByAllocator} to allocate the underlying buffer. Grows the buffer when needed.
 * <p>
 * Buffer layout is the following:
 * <pre>
 * | capacity (in longs) | size (in longs) | long array |
 * +---------------------+-----------------+------------+
 * |       4 bytes       |     4 bytes     |     -      |
 * +---------------------+-----------------+------------+
 * </pre>
 */
public class GroupByLongList {
    private static final long HEADER_SIZE = 2 * Integer.BYTES;
    private static final long SIZE_OFFSET = Integer.BYTES;
    private final int initialCapacity;
    private GroupByAllocator allocator;
    private long ptr;

    public GroupByLongList(int initialCapacity) {
        this.initialCapacity = initialCapacity;
    }

    public void add(long value) {
        int newSize = size() + 1;
        checkCapacity(newSize);
        set(newSize - 1, value);
        Unsafe.getUnsafe().putInt(ptr + SIZE_OFFSET, newSize);
    }

    public int capacity() {
        return ptr != 0 ? Unsafe.getUnsafe().getInt(ptr) : 0;
    }

    public void checkCapacity(int capacity) {
        if (capacity < 0) {
            throw new IllegalArgumentException("Negative capacity. Integer overflow may be?");
        }

        final int oldCapacity = capacity();

        long oldPtr = ptr;
        if (capacity > oldCapacity) {
            final int newCapacity = Math.max(oldCapacity << 1, capacity);
            ptr = allocator.realloc(oldPtr, 8L * oldCapacity + HEADER_SIZE, 8L * newCapacity + HEADER_SIZE);
            Vect.memset(ptr + HEADER_SIZE + 8L * oldCapacity, 8L * (newCapacity - oldCapacity), 0);
            Unsafe.getUnsafe().putInt(ptr, newCapacity);
        }
    }

    public long dataPtr() {
        return ptr + HEADER_SIZE;
    }

    public long get(long index) {
        return Unsafe.getUnsafe().getLong(ptr + HEADER_SIZE + 8L * index);
    }

    public GroupByLongList of(long ptr) {
        if (ptr == 0) {
            this.ptr = allocator.malloc(HEADER_SIZE + 8L * initialCapacity);
            Vect.memset(this.ptr + HEADER_SIZE, 8L * initialCapacity, 0);
            Unsafe.getUnsafe().putInt(this.ptr, initialCapacity);
            Unsafe.getUnsafe().putInt(this.ptr + SIZE_OFFSET, 0);
        } else {
            this.ptr = ptr;
        }
        return this;
    }

    public long ptr() {
        return ptr;
    }

    public void resetPtr() {
        ptr = 0;
    }

    public void set(long index, long value) {
        Unsafe.getUnsafe().putLong(ptr + HEADER_SIZE + 8L * index, value);
    }

    public void setAllocator(GroupByAllocator allocator) {
        this.allocator = allocator;
    }

    public int size() {
        return ptr != 0 ? Unsafe.getUnsafe().getInt(ptr + SIZE_OFFSET) : 0;
    }
}
