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
 * | capacity (in long256s) | size (in long256s) | size limit (in long256s) | padding | long256 array |
 * +------------------------+--------------------+--------------------------+---------+---------------+
 * |        4 bytes         |       4 bytes      |         4 bytes          | 4 bytes |       -       |
 * +------------------------+--------------------+--------------------------+---------+---------------+
 * </pre>
 */
public class GroupByLong256HashSet {
    private static final long HEADER_SIZE = 4 * Integer.BYTES;
    private static final int MIN_INITIAL_CAPACITY = 16;
    private static final long SIZE_LIMIT_OFFSET = 2 * Integer.BYTES;
    private static final long SIZE_OFFSET = Integer.BYTES;
    private final int initialCapacity;
    private final double loadFactor;
    private final long noKeyValue;
    private GroupByAllocator allocator;
    private long mask;
    private long ptr;

    public GroupByLong256HashSet(int initialCapacity, double loadFactor, long noKeyValue) {
        if (loadFactor <= 0d || loadFactor >= 1d) {
            throw new IllegalArgumentException("0 < loadFactor < 1");
        }
        this.initialCapacity = Numbers.ceilPow2((int) (Math.max(initialCapacity, MIN_INITIAL_CAPACITY) / loadFactor));
        this.loadFactor = loadFactor;
        this.noKeyValue = noKeyValue;
    }

    /**
     * Adds key to hash set preserving key uniqueness.
     *
     * @param k0 lowest 64 bits of key to be added.
     * @param k1 second lowest 64 bits of key to be added.
     * @param k2 second highest 64 bits of key to be added.
     * @param k3 highest 64 bits of key to be added.
     * @return false if key is already in the set and true otherwise.
     */
    public boolean add(long k0, long k1, long k2, long k3) {
        long index = keyIndex(k0, k1, k2, k3);
        if (index < 0) {
            return false;
        }
        addAt(index, k0, k1, k2, k3);
        return true;
    }

    public void addAt(long index, long k0, long k1, long k2, long k3) {
        setKeyAt(index, k0, k1, k2, k3);
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

    public long keyAddrAt(long index) {
        return ptr + HEADER_SIZE + 32L * index;
    }

    public long keyIndex(long k0, long k1, long k2, long k3) {
        long hashCode = Hash.hashLong256_64(k0, k1, k2, k3);
        long index = hashCode & mask;
        long p = keyAddrAt(index);
        long k0Key = Unsafe.getUnsafe().getLong(p);
        long k1Key = Unsafe.getUnsafe().getLong(p + 8L);
        long k2Key = Unsafe.getUnsafe().getLong(p + 16L);
        long k3Key = Unsafe.getUnsafe().getLong(p + 24L);
        if (k0Key == noKeyValue && k1Key == noKeyValue && k2Key == noKeyValue && k3Key == noKeyValue) {
            return index;
        }
        if (k0Key == k0 && k1Key == k1 && k2Key == k2 && k3Key == k3) {
            return -index - 1;
        }
        return probe(k0, k1, k2, k3, index);
    }

    public void merge(GroupByLong256HashSet srcSet) {
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

        for (long p = srcSet.ptr + HEADER_SIZE, lim = srcSet.ptr + HEADER_SIZE + 32L * srcSet.capacity(); p < lim; p += 32L) {
            long k0 = Unsafe.getUnsafe().getLong(p);
            long k1 = Unsafe.getUnsafe().getLong(p + 8L);
            long k2 = Unsafe.getUnsafe().getLong(p + 16L);
            long k3 = Unsafe.getUnsafe().getLong(p + 24L);
            if (k0 != noKeyValue || k1 != noKeyValue || k2 != noKeyValue || k3 != noKeyValue) {
                final long index = keyIndex(k0, k1, k2, k3);
                if (index >= 0) {
                    addAt(index, k0, k1, k2, k3);
                }
            }
        }
    }

    public GroupByLong256HashSet of(long ptr) {
        if (ptr == 0) {
            this.ptr = allocator.malloc(HEADER_SIZE + 32L * initialCapacity);
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

    private long probe(long k0, long k1, long k2, long k3, long index) {
        do {
            index = (index + 1) & mask;
            long p = keyAddrAt(index);
            long k0Key = Unsafe.getUnsafe().getLong(p);
            long k1Key = Unsafe.getUnsafe().getLong(p + 8L);
            long k2Key = Unsafe.getUnsafe().getLong(p + 16L);
            long k3Key = Unsafe.getUnsafe().getLong(p + 24L);
            if (k0Key == noKeyValue && k1Key == noKeyValue && k2Key == noKeyValue && k3Key == noKeyValue) {
                return index;
            }
            if (k0Key == k0 && k1Key == k1 && k2Key == k2 && k3Key == k3) {
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
        ptr = allocator.malloc(32L * newCapacity + HEADER_SIZE);
        zero(ptr, newCapacity);
        Unsafe.getUnsafe().putInt(ptr, newCapacity);
        Unsafe.getUnsafe().putInt(ptr + SIZE_OFFSET, oldSize);
        Unsafe.getUnsafe().putInt(ptr + SIZE_LIMIT_OFFSET, newSizeLimit);
        mask = newCapacity - 1;

        for (long p = oldPtr + HEADER_SIZE, lim = oldPtr + HEADER_SIZE + 32L * oldCapacity; p < lim; p += 32L) {
            long k0 = Unsafe.getUnsafe().getLong(p);
            long k1 = Unsafe.getUnsafe().getLong(p + 8L);
            long k2 = Unsafe.getUnsafe().getLong(p + 16L);
            long k3 = Unsafe.getUnsafe().getLong(p + 24L);
            if (k0 != noKeyValue || k1 != noKeyValue || k2 != noKeyValue || k3 != noKeyValue) {
                long index = keyIndex(k0, k1, k2, k3);
                setKeyAt(index, k0, k1, k2, k3);
            }
        }

        allocator.free(oldPtr, HEADER_SIZE + 32L * oldCapacity);
    }

    private void setKeyAt(long index, long k0, long k1, long k2, long k3) {
        long p = keyAddrAt(index);
        Unsafe.getUnsafe().putLong(p, k0);
        Unsafe.getUnsafe().putLong(p + 8L, k1);
        Unsafe.getUnsafe().putLong(p + 16L, k2);
        Unsafe.getUnsafe().putLong(p + 24L, k3);
    }

    private void zero(long ptr, int cap) {
        if (noKeyValue == 0) {
            // Vectorized fast path for zero default value.
            Vect.memset(ptr + HEADER_SIZE, 32L * cap, 0);
        } else {
            for (long p = ptr + HEADER_SIZE, lim = ptr + HEADER_SIZE + 32L * cap; p < lim; p += 32L) {
                Unsafe.getUnsafe().putLong(p, noKeyValue);
                Unsafe.getUnsafe().putLong(p + 8L, noKeyValue);
                Unsafe.getUnsafe().putLong(p + 16L, noKeyValue);
                Unsafe.getUnsafe().putLong(p + 24L, noKeyValue);
            }
        }
    }
}
