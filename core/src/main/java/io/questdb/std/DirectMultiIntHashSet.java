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
import io.questdb.griffin.engine.LimitOverflowException;
import org.jetbrains.annotations.Nullable;

/**
 * Fixed-width integer tuples. The first integer must not be -1.
 */
public class DirectMultiIntHashSet implements Mutable, QuietCloseable, Reopenable {
    private final int initialCapacity;
    private final long keySize;
    private final double loadFactor;
    private final int maxResizes;
    private int capacity;
    private long keyAddress;
    private long memStart;
    private @Nullable MemoryTracker memoryTracker;
    private int resizeCount;
    private int size;
    private int sizeLimit;

    public DirectMultiIntHashSet(int keyColumnCount, int keyCapacity, double loadFactor, int maxResizes) {
        if (keyColumnCount < 1 || keyCapacity < 0 || !(loadFactor > 0 && loadFactor < 1) || maxResizes < 0) {
            throw new IllegalArgumentException("invalid integer set configuration");
        }
        double slots = Math.max(16, Math.ceil(keyCapacity / loadFactor));
        if (slots > Numbers.MAX_SAFE_INT_POW_2) {
            throw CairoException.nonCritical().put("integer set capacity overflow");
        }
        this.initialCapacity = Numbers.ceilPow2((int) slots);
        this.keySize = (long) keyColumnCount * Integer.BYTES;
        this.loadFactor = loadFactor;
        this.maxResizes = maxResizes;
    }

    public boolean add() {
        assert memStart != 0;
        assert Unsafe.getInt(keyAddress) != -1;
        long hash = Hash.hashMem64(keyAddress, keySize);
        long address = find(keyAddress, hash, memStart, capacity);
        if (Unsafe.getInt(address) != -1) {
            return false;
        }
        if (size == sizeLimit) {
            rehash();
            address = find(keyAddress, hash, memStart, capacity);
        }
        Unsafe.copyMemory(keyAddress, address, keySize);
        size++;
        return true;
    }

    public int capacity() {
        return capacity;
    }

    @Override
    public void clear() {
        if (memStart != 0) {
            Vect.memset(memStart, (long) capacity * keySize, -1);
        }
        size = 0;
    }

    @Override
    public void close() {
        if (memStart != 0) {
            memStart = Unsafe.free(memStart, allocationSize(capacity), MemoryTag.NATIVE_UNORDERED_MAP, memoryTracker);
        }
        keyAddress = 0;
        capacity = 0;
        size = 0;
        sizeLimit = 0;
        memoryTracker = null;
    }

    public long getKeyAddress() {
        return keyAddress;
    }

    @Override
    public void reopen() {
        if (memStart == 0) {
            long address = Unsafe.malloc(allocationSize(initialCapacity), MemoryTag.NATIVE_UNORDERED_MAP, memoryTracker);
            memStart = address;
            capacity = initialCapacity;
            keyAddress = address + (long) capacity * keySize;
            sizeLimit = Math.max(1, (int) (capacity * loadFactor));
            resizeCount = 0;
            clear();
        }
    }

    public void setMemoryTracker(@Nullable MemoryTracker memoryTracker) {
        assert memStart == 0 || this.memoryTracker == memoryTracker;
        this.memoryTracker = memoryTracker;
    }

    public int size() {
        return size;
    }

    private long allocationSize(int capacity) {
        try {
            return Math.multiplyExact((long) capacity + 1, keySize);
        } catch (ArithmeticException e) {
            throw CairoException.nonCritical().put("integer set capacity overflow");
        }
    }

    private boolean equals(long left, long right) {
        long offset = 0;
        for (; offset + Long.BYTES <= keySize; offset += Long.BYTES) {
            if (Unsafe.getLong(left + offset) != Unsafe.getLong(right + offset)) {
                return false;
            }
        }
        return offset == keySize || Unsafe.getInt(left + offset) == Unsafe.getInt(right + offset);
    }

    private long find(long key, long hash, long start, int slotCount) {
        long index = hash & (slotCount - 1);
        while (true) {
            long address = start + index * keySize;
            if (Unsafe.getInt(address) == -1 || equals(address, key)) {
                return address;
            }
            index = (index + 1) & (slotCount - 1);
        }
    }

    private void rehash() {
        if (resizeCount == maxResizes) {
            throw LimitOverflowException.instance().put("limit of ").put(maxResizes).put(" resizes exceeded in integer set");
        }
        if (capacity >= Numbers.MAX_SAFE_INT_POW_2) {
            throw CairoException.nonCritical().put("integer set capacity overflow");
        }
        int newCapacity = capacity << 1;
        long newAddress = Unsafe.malloc(allocationSize(newCapacity), MemoryTag.NATIVE_UNORDERED_MAP, memoryTracker);
        Vect.memset(newAddress, (long) newCapacity * keySize, -1);
        for (long address = memStart; address < keyAddress; address += keySize) {
            if (Unsafe.getInt(address) != -1) {
                long target = find(address, Hash.hashMem64(address, keySize), newAddress, newCapacity);
                Unsafe.copyMemory(address, target, keySize);
            }
        }
        long newKeyAddress = newAddress + (long) newCapacity * keySize;
        Unsafe.copyMemory(keyAddress, newKeyAddress, keySize);
        Unsafe.free(memStart, allocationSize(capacity), MemoryTag.NATIVE_UNORDERED_MAP, memoryTracker);
        memStart = newAddress;
        keyAddress = newKeyAddress;
        capacity = newCapacity;
        sizeLimit = Math.max(1, (int) (capacity * loadFactor));
        resizeCount++;
    }
}
