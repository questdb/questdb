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

import static io.questdb.std.MapUtil.shouldMoveToFillGap;


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
        return Unsafe.getUnsafe().getLong(ptr + 16 * index);
    }

    public long keyIndex(long key) {
        long hashCode = Hash.fastHashLong64(key);
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
            Unsafe.getUnsafe().putLong(ptr + 16 * (-index - 1) + 8, value);
        } else {
            putAt0(index, key, value);
            size++;
            if (--free == 0) {
                rehash(capacity() << 1);
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
            compactProbeSequence(from);
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
                ptr = Unsafe.malloc(16L * capacity, memoryTag);
            } else {
                ptr = Unsafe.realloc(ptr, 16L * oldCapacity, 16L * capacity, memoryTag);
            }
        }

        clear();
    }

    public int size() {
        return size;
    }

    public long valueAt(long index) {
        return index < 0 ? Unsafe.getUnsafe().getLong(ptr + 16 * (-index - 1) + 8) : noEntryValue;
    }

    public long valueAtRaw(long index) {
        return Unsafe.getUnsafe().getLong(ptr + 16 * index + 8);
    }

    private void erase(long index) {
        Unsafe.getUnsafe().putLong(ptr + 16 * index, noEntryKey);
    }

    private void move(long from, long to) {
        final long fromPtr = ptr + 16 * from;
        final long toPtr = ptr + 16 * to;
        Unsafe.getUnsafe().putLong(toPtr, Unsafe.getUnsafe().getLong(fromPtr));
        Unsafe.getUnsafe().putLong(toPtr + 8, Unsafe.getUnsafe().getLong(fromPtr + 8));
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

    /**
     * When a slot is freed, we examine the non-empty entries that follow it.
     * Some of them may have originally hashed to this slot but were displaced
     * because it was occupied. Once the slot becomes free, such entries
     * may need to be moved backward to preserve correct lookup semantics.
     */
    private void compactProbeSequence(long deletedPosition) {
        long gapPos = deletedPosition;
        long scanPos = (gapPos + 1) & mask;

        // Scan forward until we hit an empty slot (end of probe sequence)
        for (long key = keyAtRaw(scanPos);
             key != noEntryKey;
             scanPos = (scanPos + 1) & mask, key = keyAtRaw(scanPos)) {

            long idealPos = Hash.fastHashLong64(key) & mask;

            if (shouldMoveToFillGap(scanPos, idealPos, gapPos)) {
                move(scanPos, gapPos);
                gapPos = scanPos;
            }
        }
    }

    private void putAt0(long index, long key, long value) {
        final long p = ptr + 16 * index;
        Unsafe.getUnsafe().putLong(p, key);
        Unsafe.getUnsafe().putLong(p + 8, value);
    }

    private void rehash(int newCapacity) {
        if (newCapacity < 0) {
            throw CairoException.nonCritical().put("long-long hash table capacity overflow");
        }

        final int oldCapacity = capacity;

        capacity = newCapacity;
        mask = newCapacity - 1;
        free += (int) ((newCapacity - oldCapacity) * loadFactor);
        long oldPtr = ptr;
        ptr = Unsafe.malloc(16L * newCapacity, memoryTag);
        zero();

        for (long p = oldPtr, lim = oldPtr + 16L * oldCapacity; p < lim; p += 16L) {
            long key = Unsafe.getUnsafe().getLong(p);
            if (key != noEntryKey) {
                long hashCode = Hash.fastHashLong64(key);
                long index = hashCode & mask;
                while (keyAtRaw(index) != noEntryKey) {
                    index = (index + 1) & mask;
                }

                long value = Unsafe.getUnsafe().getLong(p + 8);
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
                Unsafe.getUnsafe().putLong(p, noEntryKey);
            }
        }
    }
}
