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

/**
 * Specialized off-heap hash table that stores int keys and multiple long values.
 * Designed for storing GroupByLongList pointers in window join operations.
 * <p>
 * Memory layout per entry:
 * <pre>
 * | key (4 bytes) | value1 (8 bytes) | value2 (8 bytes) | ... | valueN (8 bytes) |
 * +---------------+------------------+------------------+-----+------------------+
 * |     int       |       long       |       long       | ... |       long       |
 * +---------------+------------------+------------------+-----+------------------+
 * </pre>
 * <p>
 * This allows storing multiple related long values (e.g., rowIds pointer, timestamps pointer,
 * rowLos value) for each int key, optimized for GROUP BY operations with GroupByAllocator.
 */
public class DirectIntMultiLongHashMap implements Mutable, QuietCloseable, Reopenable {
    private static final int MIN_INITIAL_CAPACITY = 4;
    private final long entrySize; // 4 bytes (key) + valueCount * 8 bytes (values)
    private final int initialCapacity;
    private final double loadFactor;
    private final int memoryTag;
    private final int noEntryKey;
    private final long noEntryValue;
    private final int valueCount;
    private int capacity;
    private int free;
    private long mask;
    private long ptr;
    private int size;

    /**
     * Creates a new DirectIntMultiLongHashMap.
     *
     * @param initialCapacity initial capacity of the hash table
     * @param loadFactor      load factor for the hash table (should be between 0 and 1)
     * @param noEntryKey      value representing an empty key
     * @param noEntryValue    value representing an empty value
     * @param valueCount      number of long values to store per key
     * @param memoryTag       memory tag for tracking allocations
     */
    public DirectIntMultiLongHashMap(int initialCapacity, double loadFactor, int noEntryKey, long noEntryValue, int valueCount, int memoryTag) {
        if (loadFactor <= 0d || loadFactor >= 1d) {
            throw new IllegalArgumentException("0 < loadFactor < 1");
        }
        if (valueCount <= 0) {
            throw new IllegalArgumentException("valueCount must be positive");
        }
        this.noEntryKey = noEntryKey;
        this.noEntryValue = noEntryValue;
        this.valueCount = valueCount;
        this.entrySize = 4 + 8L * valueCount;
        this.loadFactor = loadFactor;
        this.memoryTag = memoryTag;
        this.initialCapacity = this.capacity = Numbers.ceilPow2((int) (Math.max(initialCapacity, MIN_INITIAL_CAPACITY) / loadFactor));
        this.size = 0;
        this.free = (int) (capacity * loadFactor);
        this.mask = capacity - 1;
        this.ptr = Unsafe.malloc(entrySize * capacity, memoryTag);
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
            ptr = Unsafe.free(ptr, entrySize * capacity, memoryTag);
            capacity = 0;
            free = 0;
            size = 0;
        }
    }

    public boolean excludes(int key) {
        return keyIndex(key) > -1;
    }

    /**
     * Gets a specific long value for the given key.
     *
     * @param key        the key to look up
     * @param valueIndex index of the value (0 to valueCount-1)
     * @return the long value at the specified index, or 0 if key not found
     */
    public long get(int key, int valueIndex) {
        if (valueIndex < 0 || valueIndex >= valueCount) {
            throw new IllegalArgumentException("valueIndex out of bounds: " + valueIndex);
        }
        return valueAt(keyIndex(key), valueIndex);
    }

    public int getValueCount() {
        return valueCount;
    }

    public int keyAt(long index) {
        return Unsafe.getUnsafe().getInt(ptr + index * entrySize);
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

    public long ptr() {
        return ptr;
    }

    /**
     * Puts a single value at the specified index for the given key.
     *
     * @param key        the key
     * @param valueIndex index of the value to set (0 to valueCount-1)
     * @param value      the value to set
     */
    public void put(int key, int valueIndex, long value) {
        if (valueIndex < 0 || valueIndex >= valueCount) {
            throw new IllegalArgumentException("valueIndex out of bounds: " + valueIndex);
        }
        putAt(keyIndex(key), key, valueIndex, value);
    }

    /**
     * Puts all values for the given key.
     *
     * @param key    the key
     * @param values array of values to set (must have length >= valueCount)
     */
    public void putAll(int key, long[] values) {
        if (values.length < valueCount) {
            throw new IllegalArgumentException("values array too small");
        }
        putAllAt(keyIndex(key), key, values);
    }

    public void putAllAt(long index, int key, long[] values) {
        if (index < 0) {
            long entryPtr = ptr + (-index - 1) * entrySize;
            for (int i = 0; i < valueCount; i++) {
                Unsafe.getUnsafe().putLong(entryPtr + 4 + 8L * i, values[i]);
            }
        } else {
            putAllAt0(index, key, values);
            size++;
            if (--free == 0) {
                rehash(capacity() << 1);
            }
        }
    }

    public void putAt(long index, int key, int valueIndex, long value) {
        if (valueIndex < 0 || valueIndex >= valueCount) {
            throw new IllegalArgumentException("valueIndex out of bounds: " + valueIndex);
        }
        if (index < 0) {
            Unsafe.getUnsafe().putLong(ptr + (-index - 1) * entrySize + 4 + 8L * valueIndex, value);
        } else {
            long entryPtr = ptr + index * entrySize;
            Unsafe.getUnsafe().putInt(entryPtr, key);
            for (int i = 0; i < valueCount; i++) {
                Unsafe.getUnsafe().putLong(entryPtr + 4 + 8L * i, 0L);
            }
            Unsafe.getUnsafe().putLong(entryPtr + 4 + 8L * valueIndex, value);
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
                ptr = Unsafe.malloc(entrySize * capacity, memoryTag);
            } else {
                ptr = Unsafe.realloc(ptr, entrySize * oldCapacity, entrySize * capacity, memoryTag);
            }
        }

        clear();
    }

    public int size() {
        return size;
    }

    public long valueAt(long index, int valueIndex) {
        if (valueIndex < 0 || valueIndex >= valueCount) {
            throw new IllegalArgumentException("valueIndex out of bounds: " + valueIndex);
        }
        return index < 0 ? Unsafe.getUnsafe().getLong(ptr + (-index - 1) * entrySize + 4 + 8L * valueIndex) : noEntryValue;
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

        throw CairoException.critical(0).put("corrupt int multi-long hash map");
    }

    private void putAllAt0(long index, int key, long[] values) {
        final long entryPtr = ptr + index * entrySize;
        Unsafe.getUnsafe().putInt(entryPtr, key);
        for (int i = 0; i < valueCount; i++) {
            Unsafe.getUnsafe().putLong(entryPtr + 4 + 8L * i, values[i]);
        }
    }

    private void rehash(int newCapacity) {
        if (newCapacity < 0) {
            throw CairoException.nonCritical().put("int multi-long hash map capacity overflow");
        }

        final int oldCapacity = capacity;

        capacity = newCapacity;
        mask = newCapacity - 1;
        free += (int) ((newCapacity - oldCapacity) * loadFactor);
        long oldPtr = ptr;
        ptr = Unsafe.malloc(entrySize * newCapacity, memoryTag);
        zero();

        for (long p = oldPtr, lim = oldPtr + entrySize * oldCapacity; p < lim; p += entrySize) {
            int key = Unsafe.getUnsafe().getInt(p);
            if (key != noEntryKey) {
                long hashCode = Hash.fastHashInt64(key);
                long index = hashCode & mask;
                while (keyAt(index) != noEntryKey) {
                    index = (index + 1) & mask;
                }

                long entryPtr = ptr + index * entrySize;
                Unsafe.getUnsafe().putInt(entryPtr, key);
                // Copy all values
                for (long o = 4, oLim = 4 + 8L * valueCount; o < oLim; o += 8) {
                    Unsafe.getUnsafe().putLong(entryPtr + o, Unsafe.getUnsafe().getLong(p + o));
                }
            }
        }

        Unsafe.free(oldPtr, entrySize * oldCapacity, memoryTag);
    }

    private void zero() {
        if (noEntryKey == 0) {
            Vect.memset(ptr, entrySize * capacity, 0);
        } else {
            for (long p = ptr, lim = ptr + entrySize * capacity; p < lim; p += entrySize) {
                Unsafe.getUnsafe().putInt(p, noEntryKey);
            }
        }
    }
}