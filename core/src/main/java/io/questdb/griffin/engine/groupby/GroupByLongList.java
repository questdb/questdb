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
    private static final int MIN_INITIAL_CAPACITY = 2;
    private static final long SIZE_OFFSET = Integer.BYTES;
    private final int initialCapacity;
    private final long noEntryValue;
    private GroupByAllocator allocator;
    private long ptr;

    public GroupByLongList(int initialCapacity, long noEntryValue) {
        this.initialCapacity = Math.max(initialCapacity, MIN_INITIAL_CAPACITY);
        this.noEntryValue = noEntryValue;
    }

    /**
     * Adds long to list;
     *
     * @param value to be added.
     */
    public void add(long value) {
        final int size = size();
        final int newSize = size + 1;
        checkCapacity(newSize);
        setValueAt(size, value);
        setSize(newSize);
    }

    public long add(GroupByLongList that) {
        return add(that, 0, that.size());
    }

    public long add(GroupByLongList that, int lo, int hi) {
        final int this_size = size();
        final int that_size = hi - lo;
        final int final_size = this_size + that_size;
        checkCapacity(final_size);
        Vect.memcpy(appendAddress(), that.addressOf(lo), that_size);
        setSize(final_size);
        return ptr();
    }

    public long addressOf(int index) {
        return ptr() + HEADER_SIZE + 8L * index;
    }

    public long appendAddress() {
        return addressOf(size());
    }

    public int capacity() {
        return ptr != 0 ? Unsafe.getUnsafe().getInt(ptr) : 0;
    }

    public void checkCapacity(int capacity) {
        if (capacity < 0) {
            throw new IllegalArgumentException("Negative capacity. Integer overflow may be?");
        }

        int l = capacity();
        if (capacity > l) {
            int newCapacity = Math.max(l << 1, capacity);
            final int oldSize = size();
            final int oldCapacity = capacity();

            long oldPtr = ptr;
            ptr = allocator.malloc(8L * newCapacity + HEADER_SIZE);
            zero(ptr, newCapacity);
            Unsafe.getUnsafe().putInt(ptr, newCapacity);
            Unsafe.getUnsafe().putInt(ptr + SIZE_OFFSET, oldSize);

            Vect.memcpy(ptr + HEADER_SIZE, oldPtr + HEADER_SIZE, oldSize * 8L);

            allocator.free(oldPtr, HEADER_SIZE + 8L * oldCapacity);
        }
    }

    public long get(int index) {
        if (index < size()) {
            return valueAt(index);
        }
        throw new ArrayIndexOutOfBoundsException(index);
    }

    public long getQuick(int index) {
        return valueAt(index);
    }

    public GroupByLongList of(long ptr) {
        if (ptr == 0) {
            this.ptr = allocator.malloc(HEADER_SIZE + 8L * initialCapacity);
            zero(this.ptr, initialCapacity);
            setCapacity(initialCapacity);
            setSize(0);
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

    public void setAllocator(GroupByAllocator allocator) {
        this.allocator = allocator;
    }

    public int size() {
        return ptr != 0 ? Unsafe.getUnsafe().getInt(ptr + SIZE_OFFSET) : 0;
    }

    public void sortAsUnsigned() {
        Vect.sortULongAscInPlace(ptr() + HEADER_SIZE, size());
    }

    private void setCapacity(int newCapacity) {
        Unsafe.getUnsafe().putInt(ptr, newCapacity);
    }

    private void setSize(int newSize) {
        Unsafe.getUnsafe().putInt(ptr + SIZE_OFFSET, newSize);
    }

    private void setValueAt(long index, long value) {
        Unsafe.getUnsafe().putLong(ptr + HEADER_SIZE + 8L * index, value);
    }

    private long valueAt(long index) {
        return Unsafe.getUnsafe().getLong(ptr + HEADER_SIZE + 8L * index);
    }

    private void zero(long ptr, int cap) {
        if (noEntryValue == 0) {
            // Vectorized fast path for zero default value.
            Vect.memset(ptr + HEADER_SIZE, 8L * cap, 0);
        } else {
            for (long p = ptr + HEADER_SIZE, lim = ptr + HEADER_SIZE + 8L * cap; p < lim; p += 8L) {
                Unsafe.getUnsafe().putLong(p, noEntryValue);
            }
        }
    }
}
