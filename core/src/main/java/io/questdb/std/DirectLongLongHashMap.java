/*+*****************************************************************************
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


public class DirectLongLongHashMap implements Mutable, QuietCloseable, Reopenable {
    private static final int MIN_INITIAL_CAPACITY = 4;
    private final int initialCapacity;
    private final double loadFactor;
    private final int memoryTag;
    private final long noEntryKey;
    private final long noEntryValue;
    private int capacity;
    private int free;
    private long mask;
    private long ptr;
    private int size;

    public DirectLongLongHashMap(int initialCapacity, double loadFactor, long noEntryKey, long noEntryValue, int memoryTag) {
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
        this.ptr = Unsafe.malloc(16L * capacity, memoryTag);
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
            ptr = Unsafe.free(ptr, 16L * capacity, memoryTag);
            capacity = 0;
            free = 0;
            size = 0;
        }
    }

    public boolean excludes(long key) {
        return keyIndex(key) > -1;
    }

    public long get(long key) {
        return valueAt(keyIndex(key));
    }

    public long keyAtRaw(long index) {
        return Unsafe.getLong(ptr + 16 * index);
    }

    public long keyIndex(long key) {
        long hashCode = Hash.hashLong64(key);
        long index = hashCode & mask;
        long k = keyAtRaw(index);
        if (k == noEntryKey) {
            return index;
        }
        if (key == k) {
            return -index - 1;
        }
        return probe(key, index);
    }

    public void put(long key, long value) {
        putAt(keyIndex(key), key, value);
    }

    public void putAt(long index, long key, long value) {
        if (index < 0) {
            Unsafe.putLong(ptr + 16 * (-index - 1) + 8, value);
        } else {
            putAt0(index, key, value);
            size++;
            if (--free == 0) {
                try {
                    rehash(capacity() << 1);
                } catch (CairoException e) {
                    free = 1;
                    throw e;
                }
            }
        }
    }

    public boolean putIfAbsent(long key, long value) {
        long index = keyIndex(key);
        if (index > -1) {
            putAt(index, key, value);
            return true;
        }
        return false;
    }

    public void removeAt(long index) {
        if (index < 0) {
            long from = -index - 1;
            erase(from);
            free++;
            size--;

            // After we have freed up a slot, consider non-empty keys directly below.
            // They may have been a direct hit but because directly hit slot wasn't
            // empty these keys would have moved.
            // After slot is freed these keys require re-hash.
            from = (from + 1) & mask;
            for (
                    long key = keyAtRaw(from);
                    key != noEntryKey;
                    from = (from + 1) & mask, key = keyAtRaw(from)
            ) {
                long hashCode = Hash.hashLong64(key);
                long idealHit = hashCode & mask;
                if (idealHit != from) {
                    long to;
                    if (keyAtRaw(idealHit) != noEntryKey) {
                        to = probe(key, idealHit);
                    } else {
                        to = idealHit;
                    }

                    if (to > -1) {
                        move(from, to);
                    }
                }
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
            long newPtr;
            if (ptr == 0) {
                newPtr = Unsafe.malloc(16L * initialCapacity, memoryTag);
            } else {
                newPtr = Unsafe.realloc(ptr, 16L * oldCapacity, 16L * initialCapacity, memoryTag);
            }
            ptr = newPtr;
            capacity = initialCapacity;
            mask = capacity - 1;
        }

        clear();
    }

    public int size() {
        return size;
    }

    public long valueAt(long index) {
        return index < 0 ? Unsafe.getLong(ptr + 16 * (-index - 1) + 8) : noEntryValue;
    }

    public long valueAtRaw(long index) {
        return Unsafe.getLong(ptr + 16 * index + 8);
    }

    private void erase(long index) {
        Unsafe.putLong(ptr + 16 * index, noEntryKey);
    }

    private void move(long from, long to) {
        final long fromPtr = ptr + 16 * from;
        final long toPtr = ptr + 16 * to;
        Unsafe.putLong(toPtr, Unsafe.getLong(fromPtr));
        Unsafe.putLong(toPtr + 8, Unsafe.getLong(fromPtr + 8));
        erase(from);
    }

    private long probe(long key, long index) {
        final long index0 = index;
        do {
            index = (index + 1) & mask;
            long k = keyAtRaw(index);
            if (k == noEntryKey) {
                return index;
            }
            if (key == k) {
                return -index - 1;
            }
        } while (index != index0);

        throw CairoException.critical(0).put("corrupt long-long hash table");
    }

    private void putAt0(long index, long key, long value) {
        final long p = ptr + 16 * index;
        Unsafe.putLong(p, key);
        Unsafe.putLong(p + 8, value);
    }

    private void rehash(int newCapacity) {
        if (newCapacity < 0) {
            throw CairoException.nonCritical().put("long-long hash table capacity overflow");
        }

        final int oldCapacity = capacity;
        long newPtr = Unsafe.malloc(16L * newCapacity, memoryTag);

        long oldPtr = ptr;
        ptr = newPtr;
        capacity = newCapacity;
        mask = newCapacity - 1;
        free += (int) ((newCapacity - oldCapacity) * loadFactor);
        zero();

        for (long p = oldPtr, lim = oldPtr + 16L * oldCapacity; p < lim; p += 16L) {
            long key = Unsafe.getLong(p);
            if (key != noEntryKey) {
                long hashCode = Hash.hashLong64(key);
                long index = hashCode & mask;
                while (keyAtRaw(index) != noEntryKey) {
                    index = (index + 1) & mask;
                }

                long value = Unsafe.getLong(p + 8);
                putAt0(index, key, value);
            }
        }

        Unsafe.free(oldPtr, 16L * oldCapacity, memoryTag);
    }

    private void zero() {
        if (noEntryKey == 0) {
            // Vectorized fast path for zero default value.
            Vect.memset(ptr, 16L * capacity, 0);
        } else {
            // Otherwise, clean up only keys.
            for (long p = ptr, lim = ptr + 16L * capacity; p < lim; p += 16L) {
                Unsafe.putLong(p, noEntryKey);
            }
        }
    }
}
