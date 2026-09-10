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
import org.jetbrains.annotations.Nullable;


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
    // Per-workload native memory tracker bound by the owning cursor at workload start.
    // Null when no per-query limit applies; all Unsafe.{malloc,realloc,free} calls
    // degrade to the global-only overloads in that case.
    @Nullable
    private MemoryTracker memoryTracker;
    private long ptr;
    private int size;

    public DirectIntIntHashMap(int initialCapacity, double loadFactor, int noEntryKey, int noEntryValue, int memoryTag) {
        this(initialCapacity, loadFactor, noEntryKey, noEntryValue, memoryTag, true);
    }

    public DirectIntIntHashMap(int initialCapacity, double loadFactor, int noEntryKey, int noEntryValue, int memoryTag, boolean openOnInit) {
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
        if (openOnInit) {
            this.ptr = Unsafe.malloc(8L * capacity, memoryTag, memoryTracker);
            zero();
        }
        // else: ptr stays 0; the first reopen() allocates the directory under
        // whatever MemoryTracker is bound at that time.
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
            ptr = Unsafe.free(ptr, 8L * capacity, memoryTag, memoryTracker);
            capacity = 0;
            free = 0;
            mask = 0;
            size = 0;
        }
        // The block is gone, so the tracker that charged it carries no debt for this map any more.
        // Dropping the reference keeps a later free - one that runs after the pooled tracker was
        // recycled by another workload - on the global counter, where it cannot corrupt someone
        // else's total.
        memoryTracker = null;
    }

    public boolean excludes(int key) {
        return keyIndex(key) > -1;
    }

    public int get(int key) {
        return valueAt(keyIndex(key));
    }

    public boolean isOpen() {
        return ptr != 0;
    }

    public int keyAt(long index) {
        return Unsafe.getInt(ptr + (index << 3));
    }

    public long keyIndex(int key) {
        long hashCode = Hash.hashInt64(key);
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
            Unsafe.putInt(ptr + ((-index - 1) << 3) + 4, value);
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
                newPtr = Unsafe.malloc(8L * initialCapacity, memoryTag, memoryTracker);
            } else {
                newPtr = Unsafe.realloc(ptr, 8L * oldCapacity, 8L * initialCapacity, memoryTag, memoryTracker);
            }
            ptr = newPtr;
            capacity = initialCapacity;
            mask = capacity - 1;
        }

        clear();
    }

    /**
     * Binds the per-workload {@link MemoryTracker} that every subsequent allocation charges. A
     * {@code null} tracker degrades the map to global-only accounting.
     * <p>
     * Rebinding releases the live block first: a block has to be freed under the tracker that
     * charged it, or the two counters drift apart and the per-query limit stops holding. Callers
     * therefore bind at workload start, immediately before {@link #reopen()}, when the map is empty.
     */
    public void setMemoryTracker(@Nullable MemoryTracker tracker) {
        if (tracker != memoryTracker) {
            close();
            memoryTracker = tracker;
        }
    }

    public int size() {
        return size;
    }

    public int valueAt(long index) {
        return index < 0 ? Unsafe.getInt(ptr + ((-index - 1) << 3) + 4) : noEntryValue;
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
        Unsafe.putInt(p, key);
        Unsafe.putInt(p + 4, value);
    }

    private void rehash(int newCapacity) {
        if (newCapacity < 0) {
            throw CairoException.nonCritical().put("int-int hash table capacity overflow");
        }

        final int oldCapacity = capacity;
        long newPtr = Unsafe.malloc(8L * newCapacity, memoryTag, memoryTracker);

        long oldPtr = ptr;
        ptr = newPtr;
        capacity = newCapacity;
        mask = newCapacity - 1;
        free += (int) ((newCapacity - oldCapacity) * loadFactor);
        zero();

        for (long p = oldPtr, lim = oldPtr + 8L * oldCapacity; p < lim; p += 8L) {
            int key = Unsafe.getInt(p);
            if (key != noEntryKey) {
                long hashCode = Hash.hashInt64(key);
                long index = hashCode & mask;
                while (keyAt(index) != noEntryKey) {
                    index = (index + 1) & mask;
                }

                int value = Unsafe.getInt(p + 4);
                putAt0(index, key, value);
            }
        }

        Unsafe.free(oldPtr, 8L * oldCapacity, memoryTag, memoryTracker);
    }

    private void zero() {
        if (ptr == 0) {
            // Lazy-open (openOnInit == false) leaves capacity sized while ptr is still 0.
            // reopen() zeroes the directory it allocates, so there is nothing to do here.
            return;
        }
        if (noEntryKey == 0) {
            // Vectorized fast path for zero default value.
            Vect.memset(ptr, 8L * capacity, 0);
        } else {
            // Otherwise, clean up only keys.
            for (long p = ptr, lim = ptr + 8L * capacity; p < lim; p += 8L) {
                Unsafe.putInt(p, noEntryKey);
            }
        }
    }
}
