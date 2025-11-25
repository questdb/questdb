/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \|_| |_| | |_) |
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

import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

/**
 * Direct (off-heap) long hash set without sentinel value conflicts.
 */
public class DirectLongHashSet implements Closeable, Mutable, Sinkable {
    public static final double DEFAULT_LOAD_FACTOR = 0.4;
    private static final int MIN_CAPACITY = 16;
    private final double loadFactor;
    private final int memoryTag;
    private int capacity;
    private int free;
    private boolean hasZero;
    private int mask;
    private long memLimit;
    private long memStart;
    private int size;

    public DirectLongHashSet(int capacity) {
        this(capacity, DEFAULT_LOAD_FACTOR, MemoryTag.NATIVE_DEFAULT);
    }

    public DirectLongHashSet(int capacity, int memoryTag) {
        this(capacity, DEFAULT_LOAD_FACTOR, memoryTag);
    }

    public DirectLongHashSet(int capacity, double loadFactor, int memoryTag) {
        if (loadFactor <= 0d || loadFactor >= 1d) {
            throw new IllegalArgumentException("0 < loadFactor < 1");
        }

        this.loadFactor = loadFactor;
        this.memoryTag = memoryTag;
        this.capacity = Math.max(Numbers.ceilPow2((int) (capacity / loadFactor)), MIN_CAPACITY);
        this.mask = this.capacity - 1;
        this.free = (int) (this.capacity * loadFactor);
        this.size = 0;
        this.hasZero = false;

        long sizeBytes = (long) this.capacity * Long.BYTES;
        this.memStart = Unsafe.malloc(sizeBytes, memoryTag);
        this.memLimit = memStart + sizeBytes;
        Vect.memset(memStart, sizeBytes, 0);
    }

    public boolean add(long key) {
        if (key == 0) {
            return addZero();
        }
        return addNonZero(key);
    }

    public int capacity() {
        return capacity;
    }

    @Override
    public void clear() {
        if (memStart != 0) {
            Vect.memset(memStart, memLimit - memStart, 0);
            size = 0;
            free = (int) (capacity * loadFactor);
            hasZero = false;
        }
    }

    @Override
    public void close() {
        if (memStart != 0) {
            Unsafe.free(memStart, memLimit - memStart, memoryTag);
            memStart = memLimit = 0;
            size = 0;
            free = 0;
            hasZero = false;
        }
    }

    public boolean contains(long key) {
        if (key == 0) {
            return hasZero;
        }
        return keyIndex(key) < 0;
    }

    public boolean excludes(long key) {
        return !contains(key);
    }

    public int keyIndex(long key) {
        if (key == 0) {
            return hasZero ? -1 : 0;
        }

        long index = Hash.hashLong64(key) & mask;
        long addr = memStart + (index * Long.BYTES);

        for (; ; ) {
            long k = Unsafe.getUnsafe().getLong(addr);
            if (k == 0) {
                return (int) index;
            } else if (k == key) {
                return (int) -(index + 1);
            }
            index = (index + 1) & mask;
            addr = memStart + (index * Long.BYTES);
        }
    }

    public int remove(long key) {
        if (key == 0) {
            if (hasZero) {
                hasZero = false;
                return 0;
            }
            return -1;
        }

        long index = Hash.hashLong64(key) & mask;
        long addr = memStart + (index * Long.BYTES);

        for (; ; ) {
            long k = Unsafe.getUnsafe().getLong(addr);
            if (k == 0) {
                return -1;
            } else if (k == key) {
                removeAt(addr, index);
                return (int) index;
            }
            index = (index + 1) & mask;
            addr = memStart + (index * Long.BYTES);
        }
    }

    public int size() {
        return size + (hasZero ? 1 : 0);
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        int totalSize = size();
        if (totalSize == 0) {
            sink.putAscii("[]");
            return;
        }

        LongList temp = new LongList(totalSize);

        if (hasZero) {
            temp.add(0);
        }

        for (long addr = memStart; addr < memLimit; addr += Long.BYTES) {
            long key = Unsafe.getUnsafe().getLong(addr);
            if (key != 0) {
                temp.add(key);
            }
        }

        temp.sort();
        temp.toSink(sink);
        temp.clear();
    }

    private boolean addNonZero(long key) {
        long index = Hash.hashLong64(key) & mask;
        long addr = memStart + (index * Long.BYTES);

        for (; ; ) {
            long k = Unsafe.getUnsafe().getLong(addr);
            if (k == 0) {
                Unsafe.getUnsafe().putLong(addr, key);
                size++;
                if (--free == 0) {
                    rehash();
                }
                return true;
            } else if (k == key) {
                return false;
            }
            index = (index + 1) & mask;
            addr = memStart + (index * Long.BYTES);
        }
    }

    private boolean addZero() {
        if (hasZero) {
            return false;
        }
        hasZero = true;
        return true;
    }

    private boolean canMoveBack(long properIndex, long currentIndex, long emptyIndex) {
        if (properIndex <= currentIndex) {
            return properIndex <= emptyIndex && emptyIndex < currentIndex;
        } else {
            return properIndex <= emptyIndex || emptyIndex < currentIndex;
        }
    }

    private void rehash() {
        int newCapacity = capacity * 2;
        long newSizeBytes = (long) newCapacity * Long.BYTES;
        long newMemStart = Unsafe.malloc(newSizeBytes, memoryTag);
        Vect.memset(newMemStart, newSizeBytes, 0);

        int newMask = newCapacity - 1;

        for (long addr = memStart; addr < memLimit; addr += Long.BYTES) {
            long key = Unsafe.getUnsafe().getLong(addr);
            if (key == 0) {
                continue;
            }

            long newIndex = Hash.hashLong64(key) & newMask;
            long newAddr = newMemStart + (newIndex * Long.BYTES);

            while (Unsafe.getUnsafe().getLong(newAddr) != 0) {
                newIndex = (newIndex + 1) & newMask;
                newAddr = newMemStart + (newIndex * Long.BYTES);
            }
            Unsafe.getUnsafe().putLong(newAddr, key);
        }

        Unsafe.free(memStart, memLimit - memStart, memoryTag);
        memStart = newMemStart;
        memLimit = newMemStart + newSizeBytes;
        capacity = newCapacity;
        mask = newMask;
        free = (int) ((capacity - size) * loadFactor);
    }

    private void removeAt(long addr, long index) {
        Unsafe.getUnsafe().putLong(addr, 0);
        size--;

        long nextIndex = (index + 1) & mask;
        long nextAddr = memStart + (nextIndex * Long.BYTES);

        for (; ; ) {
            long nextKey = Unsafe.getUnsafe().getLong(nextAddr);
            if (nextKey == 0) {
                break;
            }

            long properIndex = Hash.hashLong64(nextKey) & mask;
            if (properIndex != nextIndex) {
                if (canMoveBack(properIndex, nextIndex, index)) {
                    Unsafe.getUnsafe().putLong(addr, nextKey);
                    Unsafe.getUnsafe().putLong(nextAddr, 0);
                    addr = nextAddr;
                    index = nextIndex;
                }
            }

            nextIndex = (nextIndex + 1) & mask;
            nextAddr = memStart + (nextIndex * Long.BYTES);
        }
    }
}
