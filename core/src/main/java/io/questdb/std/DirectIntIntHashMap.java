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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.Reopenable;


public class DirectIntIntHashMap implements Mutable, QuietCloseable, Reopenable {
    private static final int MIN_INITIAL_CAPACITY = 4;
    private final int initialCapacity;
    private final double loadFactor;
    private final int memoryTag;
    private final int noEntryKey;
    private final int noEntryValue;
    private int capacity;
    private int free;
    private long mask;
    private long ptr;
    private int size;

    public DirectIntIntHashMap(int initialCapacity, double loadFactor, int noEntryKey, int noEntryValue, int memoryTag) {
        if (loadFactor <= 0d || loadFactor >= 1d) {
            throw new IllegalArgumentException("0 < loadFactor < 1");
        }
        this.noEntryKey = noEntryKey;
        this.noEntryValue = noEntryValue;
        this.loadFactor = loadFactor;
        this.memoryTag = memoryTag;
        this.initialCapacity = this.capacity = Numbers.ceilPow2((int) (Math.max(initialCapacity, MIN_INITIAL_CAPACITY) / loadFactor));
        this.size = 0;
        this.free = (int) (capacity * loadFactor);
        this.mask = capacity - 1;
        this.ptr = Unsafe.malloc(8L * capacity, memoryTag);
        zero();
    }

    public int capacity() {
        return capacity;
    }

    @Override
    public void clear() {
        free = (int) (capacity * loadFactor);
        size = 0;
        zero();
    }

    @Override
    public void close() {
        if (ptr != 0) {
            ptr = Unsafe.free(ptr, 8L * capacity, memoryTag);
            capacity = 0;
            free = 0;
            size = 0;
        }
    }

    public boolean excludes(int key) {
        return keyIndex(key) > -1;
    }

    public int get(int key) {
        return valueAt(keyIndex(key));
    }

    public int keyAt(long index) {
        return Unsafe.getUnsafe().getInt(ptr + (index << 3));
    }

    public long keyIndex(int key) {
        long hashCode = Hash.fastHashInt64(key);
        long index = hashCode & mask;
        int k = keyAt(index);
        if (k == noEntryKey) {
            return index;
        }
        if (key == k) {
            return -index - 1;
        }
        return probe(key, index);
    }

    public void put(int key, int value) {
        putAt(keyIndex(key), key, value);
    }

    public void putAt(long index, int key, int value) {
        if (index < 0) {
            Unsafe.getUnsafe().putInt(ptr + ((-index - 1) << 3) + 4, value);
        } else {
            putAt0(index, key, value);
            size++;
            if (--free == 0) {
                rehash(capacity() << 1);
            }
        }
    }

    @Override
    public void reopen() {
        if (ptr == 0) {
            restoreInitialCapacity();
        }
    }

    public void restoreInitialCapacity() {
        if (ptr == 0 || capacity != initialCapacity) {
            final long oldCapacity = capacity;
            capacity = initialCapacity;
            mask = capacity - 1;
            if (ptr == 0) {
                ptr = Unsafe.malloc(8L * capacity, memoryTag);
            } else {
                ptr = Unsafe.realloc(ptr, 8L * oldCapacity, 8L * capacity, memoryTag);
            }
        }

        clear();
    }

    public int size() {
        return size;
    }

    public int valueAt(long index) {
        return index < 0 ? Unsafe.getUnsafe().getInt(ptr + ((-index - 1) << 3) + 4) : noEntryValue;
    }

    private long probe(int key, long index) {
        final long index0 = index;
        do {
            index = (index + 1) & mask;
            int k = keyAt(index);
            if (k == noEntryKey) {
                return index;
            }
            if (key == k) {
                return -index - 1;
            }
        } while (index != index0);

        throw CairoException.critical(0).put("corrupt int-int hash table");
    }

    private void putAt0(long index, int key, int value) {
        final long p = ptr + (index << 3);
        Unsafe.getUnsafe().putInt(p, key);
        Unsafe.getUnsafe().putInt(p + 4, value);
    }

    private void rehash(int newCapacity) {
        if (newCapacity < 0) {
            throw CairoException.nonCritical().put("int-int hash table capacity overflow");
        }

        final int oldCapacity = capacity;

        capacity = newCapacity;
        mask = newCapacity - 1;
        free += (int) ((newCapacity - oldCapacity) * loadFactor);
        long oldPtr = ptr;
        ptr = Unsafe.malloc(8L * newCapacity, memoryTag);
        zero();

        for (long p = oldPtr, lim = oldPtr + 8L * oldCapacity; p < lim; p += 8L) {
            int key = Unsafe.getUnsafe().getInt(p);
            if (key != noEntryKey) {
                long hashCode = Hash.fastHashInt64(key);
                long index = hashCode & mask;
                while (keyAt(index) != noEntryKey) {
                    index = (index + 1) & mask;
                }

                int value = Unsafe.getUnsafe().getInt(p + 4);
                putAt0(index, key, value);
            }
        }

        Unsafe.free(oldPtr, 8L * oldCapacity, memoryTag);
    }

    private void zero() {
        if (noEntryKey == 0) {
            // Vectorized fast path for zero default value.
            Vect.memset(ptr, 8L * capacity, 0);
        } else {
            // Otherwise, clean up only keys.
            for (long p = ptr, lim = ptr + 8L * capacity; p < lim; p += 8L) {
                Unsafe.getUnsafe().putInt(p, noEntryKey);
            }
        }
    }
}
