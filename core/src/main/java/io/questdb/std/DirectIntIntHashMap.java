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

import io.questdb.cairo.CairoException;


public class DirectIntIntHashMap implements Mutable, QuietCloseable {
    private static final int MIN_INITIAL_CAPACITY = 4;
    private final int initialCapacity;
    private final double loadFactor;
    private final int memoryTag;
    private final int noKeyValue;
    private int capacity;
    private int free;
    private long mask;
    private long ptr;

    public DirectIntIntHashMap(int initialCapacity, double loadFactor, int noKeyValue, int memoryTag) {
        if (loadFactor <= 0d || loadFactor >= 1d) {
            throw new IllegalArgumentException("0 < loadFactor < 1");
        }
        this.noKeyValue = noKeyValue;
        this.initialCapacity = Numbers.ceilPow2((int) (Math.max(initialCapacity, MIN_INITIAL_CAPACITY) / loadFactor));
        this.loadFactor = loadFactor;
        this.memoryTag = memoryTag;
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

    public void addAt(long index, int key, int value) {
        putAt(index, key, value);
        if (--free == 0) {
            rehash(capacity() << 1, sizeLimit << 1);
        }
    }

    public int capacity() {
        return capacity;
    }

    @Override
    public void clear() {
        // TODO
    }

    @Override
    public void close() {
        // TODO
    }

    public int keyAt(long index) {
        return Unsafe.getUnsafe().getInt(ptr + (index << 3));
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

    public void of(long ptr) {
        if (ptr == 0) {
            this.ptr = Unsafe.malloc(8L * initialCapacity, memoryTag);
            zero(this.ptr, initialCapacity);
            Unsafe.getUnsafe().putInt(this.ptr, initialCapacity);
            Unsafe.getUnsafe().putInt(this.ptr + SIZE_OFFSET, 0);
            Unsafe.getUnsafe().putInt(this.ptr + SIZE_LIMIT_OFFSET, (int) (initialCapacity * loadFactor));
            mask = initialCapacity - 1;
        } else {
            this.ptr = ptr;
            mask = capacity() - 1;
        }
    }

    public void restoreInitialCapacity() {
        // TODO
    }

    public int size() {
        return capacity - free;
    }

    private long probe(int key, long index) {
        final long index0 = index;
        do {
            index = (index + 1) & mask;
            int k = keyAt(index);
            if (k == noKeyValue) {
                return index;
            }
            if (key == k) {
                return -index - 1;
            }
        } while (index != index0);

        throw CairoException.critical(0).put("corrupt int hash set");
    }

    private void putAt(long index, int key, int value) {
        final long p = ptr + (index << 3);
        Unsafe.getUnsafe().putInt(p, key);
        Unsafe.getUnsafe().putInt(p + 4, value);
    }

    private void rehash(int newCapacity, int newSizeLimit) {
        if (newCapacity < 0) {
            throw CairoException.nonCritical().put("int hash set capacity overflow");
        }

        final int oldSize = size();
        final int oldCapacity = capacity();

        long oldPtr = ptr;
        ptr = Unsafe.malloc(8L * newCapacity, memoryTag);
        zero(ptr, newCapacity);
        Unsafe.getUnsafe().putInt(ptr, newCapacity);
        Unsafe.getUnsafe().putInt(ptr + SIZE_OFFSET, oldSize);
        Unsafe.getUnsafe().putInt(ptr + SIZE_LIMIT_OFFSET, newSizeLimit);
        mask = newCapacity - 1;

        for (long p = oldPtr, lim = oldPtr + 8L * oldCapacity; p < lim; p += 8L) {
            int key = Unsafe.getUnsafe().getInt(p);
            if (key != noKeyValue) {
                long index = keyIndex(key);
                putAt(index, key);
            }
        }

        Unsafe.free(oldPtr, HEADER_SIZE + 4L * oldCapacity, memoryTag);
    }

    private void zero(long ptr, int cap) {
        if (noKeyValue == 0) {
            // Vectorized fast path for zero default value.
            Vect.memset(ptr, 8L * cap, 0);
        } else {
            // Otherwise, clean up only keys.
            for (long p = ptr, lim = ptr + 8L * cap; p < lim; p += 8L) {
                Unsafe.getUnsafe().putInt(p, noKeyValue);
            }
        }
    }
}
