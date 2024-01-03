/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.CairoException;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;

/**
 * Specialized flyweight hash set used in {@link io.questdb.griffin.engine.functions.GroupByFunction}s.
 * <p>
 * Uses provided {@link GroupByAllocator} to allocate the underlying buffer. Grows the buffer when needed.
 * <p>
 * Buffer layout is the following:
 * <pre>
 * | capacity (in longs) | size (in longs) | size limit (in longs) | padding | long array |
 * +---------------------+-----------------+-----------------------+---------+------------+
 * |       4 bytes       |     4 bytes     |       4 bytes         | 4 bytes |     -      |
 * +---------------------+-----------------+-----------------------+---------+------------+
 * </pre>
 */
public class GroupByLongHashSet {
    private static final long HEADER_SIZE = 4 * Integer.BYTES;
    private static final int MIN_INITIAL_CAPACITY = 16;
    private static final long SIZE_LIMIT_OFFSET = 2 * Integer.BYTES;
    private static final long SIZE_OFFSET = Integer.BYTES;
    private final int initialCapacity;
    private final double loadFactor;
    private final long noKeyValue;
    private GroupByAllocator allocator;
    private int mask;
    private long ptr;

    public GroupByLongHashSet(int initialCapacity, double loadFactor, long noKeyValue) {
        if (loadFactor <= 0d || loadFactor >= 1d) {
            throw new IllegalArgumentException("0 < loadFactor < 1");
        }
        this.noKeyValue = noKeyValue;
        this.initialCapacity = Numbers.ceilPow2((int) (Math.max(initialCapacity, MIN_INITIAL_CAPACITY) / loadFactor));
        this.loadFactor = loadFactor;
    }

    /**
     * Adds key to hash set preserving key uniqueness.
     *
     * @param key key to be added.
     * @return false if key is already in the set and true otherwise.
     */
    public boolean add(long key) {
        int index = keyIndex(key);
        if (index < 0) {
            return false;
        }
        addAt(index, key);
        return true;
    }

    public void addAt(int index, long key) {
        setKeyAt(index, key);
        int size = size();
        int sizeLimit = sizeLimit();
        Unsafe.getUnsafe().putInt(ptr + SIZE_OFFSET, ++size);
        if (size >= sizeLimit) {
            rehash(size, sizeLimit);
        }
    }

    public int capacity() {
        return ptr != 0 ? Unsafe.getUnsafe().getInt(ptr) : 0;
    }

    public long keyAt(int index) {
        return Unsafe.getUnsafe().getLong(ptr + HEADER_SIZE + 8L * index);
    }

    public int keyIndex(long key) {
        int hashCode = Long.hashCode(key);
        int index = hashCode & mask;
        long k = keyAt(index);
        if (k == noKeyValue) {
            return index;
        }
        if (key == k) {
            return -index - 1;
        }
        return probe(key, index);
    }

    public GroupByLongHashSet of(long ptr) {
        if (ptr == 0) {
            this.ptr = allocator.malloc(8L * initialCapacity + HEADER_SIZE);
            zero(this.ptr, initialCapacity);
            Unsafe.getUnsafe().putInt(this.ptr, initialCapacity);
            Unsafe.getUnsafe().putInt(this.ptr + SIZE_OFFSET, 0);
            Unsafe.getUnsafe().putInt(this.ptr + SIZE_LIMIT_OFFSET, (int) (initialCapacity * loadFactor));
            mask = initialCapacity - 1;
        } else {
            this.ptr = ptr;
            mask = capacity() - 1;
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

    public int sizeLimit() {
        return ptr != 0 ? Unsafe.getUnsafe().getInt(ptr + SIZE_LIMIT_OFFSET) : 0;
    }

    private int probe(long key, int index) {
        do {
            index = (index + 1) & mask;
            long k = keyAt(index);
            if (k == noKeyValue) {
                return index;
            }
            if (key == k) {
                return -index - 1;
            }
        } while (true);
    }

    private void rehash(int size, int sizeLimit) {
        int oldCapacity = capacity();
        int newCapacity = oldCapacity << 1;
        if (newCapacity < oldCapacity) {
            throw CairoException.nonCritical().put("hash set capacity overflow");
        }

        long oldPtr = ptr;
        ptr = allocator.malloc(8L * newCapacity + HEADER_SIZE);
        zero(ptr, newCapacity);
        Unsafe.getUnsafe().putInt(ptr, newCapacity);
        Unsafe.getUnsafe().putInt(ptr + SIZE_OFFSET, size);
        Unsafe.getUnsafe().putInt(ptr + SIZE_LIMIT_OFFSET, sizeLimit << 1);
        mask = newCapacity - 1;

        for (long p = oldPtr + HEADER_SIZE, lim = oldPtr + HEADER_SIZE + 8L * oldCapacity; p < lim; p += 8L) {
            long key = Unsafe.getUnsafe().getLong(p);
            if (key != noKeyValue) {
                int index = keyIndex(key);
                setKeyAt(index, key);
            }
        }
    }

    private void setKeyAt(int index, long key) {
        Unsafe.getUnsafe().putLong(ptr + HEADER_SIZE + 8L * index, key);
    }

    private void zero(long ptr, int cap) {
        for (long p = ptr + HEADER_SIZE, lim = ptr + HEADER_SIZE + 8L * cap; p < lim; p += 8L) {
            Unsafe.getUnsafe().putLong(p, noKeyValue);
        }
    }
}
