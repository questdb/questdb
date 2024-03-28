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
import io.questdb.std.Hash;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

/**
 * Specialized flyweight hash set used in {@link io.questdb.griffin.engine.functions.GroupByFunction}s.
 * <p>
 * Uses provided {@link GroupByAllocatorImpl} to allocate the underlying buffer. Grows the buffer when needed.
 * <p>
 * Buffer layout is the following:
 * <pre>
 * | capacity (in ints) | size (in ints) | size limit (in ints) | int array |
 * +--------------------+----------------+----------------------+-----------+
 * |       4 bytes      |     4 bytes    |       4 bytes        |     -     |
 * +--------------------+----------------+----------------------+-----------+
 * </pre>
 */
public class GroupByIntHashSet {
    private static final long HEADER_SIZE = 3 * Integer.BYTES;
    private static final int MIN_INITIAL_CAPACITY = 16;
    private static final long SIZE_LIMIT_OFFSET = 2 * Integer.BYTES;
    private static final long SIZE_OFFSET = Integer.BYTES;
    private final int initialCapacity;
    private final double loadFactor;
    private final int noKeyValue;
    private GroupByAllocator allocator;
    private long mask;
    private long ptr;

    public GroupByIntHashSet(int initialCapacity, double loadFactor, int noKeyValue) {
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
    public boolean add(int key) {
        long index = keyIndex(key);
        if (index < 0) {
            return false;
        }
        addAt(index, key);
        return true;
    }

    public void addAt(long index, int key) {
        setKeyAt(index, key);
        int size = size();
        int sizeLimit = sizeLimit();
        Unsafe.getUnsafe().putInt(ptr + SIZE_OFFSET, ++size);
        if (size >= sizeLimit) {
            rehash(capacity() << 1, sizeLimit << 1);
        }
    }

    public int capacity() {
        return ptr != 0 ? Unsafe.getUnsafe().getInt(ptr) : 0;
    }

    public int keyAt(long index) {
        return Unsafe.getUnsafe().getInt(ptr + HEADER_SIZE + 4L * index);
    }

    public long keyIndex(int key) {
        long hashCode = Hash.hashInt64(key);
        long index = hashCode & mask;
        int k = keyAt(index);
        if (k == noKeyValue) {
            return index;
        }
        if (key == k) {
            return -index - 1;
        }
        return probe(key, index);
    }

    public void merge(GroupByIntHashSet srcSet) {
        final int size = size();
        // Math.max is here for overflow protection.
        final int newSize = Math.max(size + srcSet.size(), size);
        final int sizeLimit = sizeLimit();
        if (sizeLimit < newSize) {
            int newSizeLimit = sizeLimit;
            int newCapacity = capacity();
            while (newSizeLimit < newSize) {
                newSizeLimit *= 2;
                newCapacity *= 2;
            }
            rehash(newCapacity, newSizeLimit);
        }

        for (long p = srcSet.ptr + HEADER_SIZE, lim = srcSet.ptr + HEADER_SIZE + 4L * srcSet.capacity(); p < lim; p += 4L) {
            int val = Unsafe.getUnsafe().getInt(p);
            if (val != noKeyValue) {
                final long index = keyIndex(val);
                if (index >= 0) {
                    addAt(index, val);
                }
            }
        }
    }

    public GroupByIntHashSet of(long ptr) {
        if (ptr == 0) {
            this.ptr = allocator.malloc(HEADER_SIZE + 4L * initialCapacity);
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

    private long probe(int key, long index) {
        do {
            index = (index + 1) & mask;
            int k = keyAt(index);
            if (k == noKeyValue) {
                return index;
            }
            if (key == k) {
                return -index - 1;
            }
        } while (true);
    }

    private void rehash(int newCapacity, int newSizeLimit) {
        if (newCapacity < 0) {
            throw CairoException.nonCritical().put("set capacity overflow");
        }

        final int oldSize = size();
        final int oldCapacity = capacity();

        long oldPtr = ptr;
        ptr = allocator.malloc(HEADER_SIZE + 4L * newCapacity);
        zero(ptr, newCapacity);
        Unsafe.getUnsafe().putInt(ptr, newCapacity);
        Unsafe.getUnsafe().putInt(ptr + SIZE_OFFSET, oldSize);
        Unsafe.getUnsafe().putInt(ptr + SIZE_LIMIT_OFFSET, newSizeLimit);
        mask = newCapacity - 1;

        for (long p = oldPtr + HEADER_SIZE, lim = oldPtr + HEADER_SIZE + 4L * oldCapacity; p < lim; p += 4L) {
            int key = Unsafe.getUnsafe().getInt(p);
            if (key != noKeyValue) {
                long index = keyIndex(key);
                setKeyAt(index, key);
            }
        }

        allocator.free(oldPtr, HEADER_SIZE + 4L * oldCapacity);
    }

    private void setKeyAt(long index, int key) {
        Unsafe.getUnsafe().putInt(ptr + HEADER_SIZE + 4L * index, key);
    }

    private void zero(long ptr, int cap) {
        if (noKeyValue == 0) {
            // Vectorized fast path for zero default value.
            Vect.memset(ptr + HEADER_SIZE, 4L * cap, 0);
        } else {
            for (long p = ptr + HEADER_SIZE, lim = ptr + HEADER_SIZE + 4L * cap; p < lim; p += 4L) {
                Unsafe.getUnsafe().putInt(p, noKeyValue);
            }
        }
    }
}
