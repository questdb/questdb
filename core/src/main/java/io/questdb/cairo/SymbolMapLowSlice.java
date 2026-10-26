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

package io.questdb.cairo;

import io.questdb.std.MemoryTag;
import io.questdb.std.NativeMemoryBudget;
import io.questdb.std.Numbers;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.DirectString;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Values of the keys below {@link #MAX_KEYS} of one dictionary version, each copied on first read
 * into a slot of one cache line: the length plus one, then up to {@link #MAX_INLINE_CHARS} chars.
 * A zero header marks a key that is not copied yet. Reads are lock-free; copies and growth take a
 * try-lock that never waits. Published slots never change and retired arrays stay allocated until
 * close, so views survive later copies. Every array is charged to the shared budget, and a refused
 * array stops admission for good.
 */
final class SymbolMapLowSlice implements QuietCloseable {
    static final int BUSY = 3;
    static final int INSERTED = 0;
    // (SLOT_SIZE - Integer.BYTES) / Character.BYTES
    static final int MAX_INLINE_CHARS = 30;
    static final int MAX_KEYS = 4096;
    static final int NOT_INSERTED = 1;
    static final int STOPPED = 2;
    // The array header takes the first slot, so the slots start on a cache line.
    private static final int CAPACITY_OFFSET = 0;
    private static final int MIN_CAPACITY = 64;
    private static final int RAW_ADDRESS_OFFSET = 8;
    private static final int RAW_SIZE_OFFSET = 16;
    private static final int RETIRED_OFFSET = 24;
    private static final int SLOT_SHIFT = 6;
    private static final int SLOT_SIZE = 1 << SLOT_SHIFT;
    private final NativeMemoryBudget budget;
    private final AtomicBoolean lock = new AtomicBoolean();
    private volatile long allocatedBytes;
    private volatile long array;
    private volatile boolean isFilling = true;
    private volatile int size;

    SymbolMapLowSlice(NativeMemoryBudget budget) {
        this.budget = budget;
    }

    @TestOnly
    public long allocatedBytes() {
        return allocatedBytes;
    }

    public int capacity() {
        final long a = array;
        return a == 0 ? 0 : Unsafe.getInt(a + CAPACITY_OFFSET);
    }

    @Override
    public void close() {
        isFilling = false;
        long a = array;
        array = 0;
        while (a != 0) {
            final long next = Unsafe.getLong(a + RETIRED_OFFSET);
            final long bytes = Unsafe.getLong(a + RAW_SIZE_OFFSET);
            Unsafe.free(Unsafe.getLong(a + RAW_ADDRESS_OFFSET), bytes, MemoryTag.NATIVE_SYMBOL_CACHE);
            budget.release(bytes);
            a = next;
        }
        allocatedBytes = 0;
        size = 0;
    }

    public @Nullable DirectString get(int key, @NotNull DirectString view) {
        assert key >= 0 && key < MAX_KEYS;
        final long a = array;
        if (a == 0 || key >= Unsafe.getInt(a + CAPACITY_OFFSET)) {
            return null;
        }
        final long slot = slotAddress(a, key);
        final int header = Unsafe.getIntVolatile(null, slot);
        return header > 0 ? view.of(slot + Integer.BYTES, header - 1) : null;
    }

    public boolean isFilling() {
        return isFilling;
    }

    public int size() {
        return size;
    }

    /**
     * Returns BUSY while another caller holds the lock, and STOPPED only to the caller whose copy
     * stops admission.
     */
    public int tryPut(int key, int symbolCount, long charsAddr, int len) {
        assert key >= 0 && key < MAX_KEYS && key < symbolCount && len >= 0 && len <= MAX_INLINE_CHARS;
        if (!isFilling) {
            return NOT_INSERTED;
        }
        if (lock.get() || !lock.compareAndSet(false, true)) {
            return BUSY;
        }
        try {
            if (!isFilling) {
                return NOT_INSERTED;
            }
            long a = array;
            if (a == 0 || key >= Unsafe.getInt(a + CAPACITY_OFFSET)) {
                a = grow(a, key, symbolCount);
                if (a == 0) {
                    isFilling = false;
                    return STOPPED;
                }
            }
            final long slot = slotAddress(a, key);
            if (Unsafe.getInt(slot) != 0) {
                return NOT_INSERTED;
            }
            if (len > 0) {
                Vect.memcpy(slot + Integer.BYTES, charsAddr, (long) len * Character.BYTES);
            }
            // Publishes the chars copied above.
            Unsafe.putOrderedInt(null, slot, len + 1);
            //noinspection NonAtomicOperationOnVolatileField
            size++;
            return INSERTED;
        } finally {
            lock.set(false);
        }
    }

    private static long slotAddress(long array, int key) {
        return array + ((long) (key + 1) << SLOT_SHIFT);
    }

    private long grow(long current, int key, int symbolCount) {
        final int capacity = current == 0 ? 0 : Unsafe.getInt(current + CAPACITY_OFFSET);
        final int newCapacity = current == 0 && key < MIN_CAPACITY
                ? MIN_CAPACITY
                : Math.max(MIN_CAPACITY, Numbers.ceilPow2(Math.min(symbolCount, MAX_KEYS)));
        // the header slot, the slots and the slack that aligns them to a cache line
        final long bytes = (newCapacity + 2L) * SLOT_SIZE;
        if (!budget.tryReserve(bytes)) {
            return 0;
        }
        final long raw;
        try {
            raw = Unsafe.malloc(bytes, MemoryTag.NATIVE_SYMBOL_CACHE);
        } catch (Throwable e) {
            budget.release(bytes);
            if (e instanceof CairoException ce && ce.isOutOfMemory()) {
                return 0;
            }
            throw e;
        }
        final long a = (raw + SLOT_SIZE - 1) & -SLOT_SIZE;
        Vect.memset(a, (newCapacity + 1L) * SLOT_SIZE, 0);
        if (current != 0) {
            Vect.memcpy(a + SLOT_SIZE, current + SLOT_SIZE, (long) capacity * SLOT_SIZE);
        }
        Unsafe.putInt(a + CAPACITY_OFFSET, newCapacity);
        Unsafe.putLong(a + RAW_ADDRESS_OFFSET, raw);
        Unsafe.putLong(a + RAW_SIZE_OFFSET, bytes);
        Unsafe.putLong(a + RETIRED_OFFSET, current);
        //noinspection NonAtomicOperationOnVolatileField
        allocatedBytes += bytes;
        // Publishes the header and the copied slots.
        array = a;
        return a;
    }
}
