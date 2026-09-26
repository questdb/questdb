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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.std.Hash;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Numbers;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.Nullable;

/**
 * One key table of {@link IntHashJoinBuild}: linear probing at a maximum load of 1/2, eight-byte
 * slots of an INT key and the key's chain head, zero marking an unused slot. A serial build fills
 * one table; a parallel build fills one per hash partition, each on whichever thread builds that
 * partition, so a table binds the circuit breaker of the thread that opens it.
 */
final class IntHashJoinKeyTable implements QuietCloseable {
    static final int MAX_SLOTS = 1 << 30;
    static final int SLOT_SIZE = 8;
    // Rehashing is not bounded by a row; it checks the breaker once per MiB it touches.
    private static final int KEY_SLOTS_PER_CHECK = (int) (HashJoinBuffer.COPY_CHUNK_SIZE / SLOT_SIZE);
    private static final long MAX_BUFFER_SIZE = 1L << 48;
    // Read directly by the build and its frozen snapshots.
    final HashJoinBuffer slots = new HashJoinBuffer(MAX_BUFFER_SIZE);
    int keyCount;
    int slotCount;
    private SqlExecutionCircuitBreaker circuitBreaker;

    static long findSlot(long base, int slots, int key) {
        int index = (int) Hash.hashInt64(key) & (slots - 1);
        long address = base + (long) index * SLOT_SIZE;
        while (Unsafe.getInt(address + 4) != 0 && Unsafe.getInt(address) != key) {
            index = (index + 1) & (slots - 1);
            address = base + (long) index * SLOT_SIZE;
        }
        return address;
    }

    /**
     * The slot this key's row goes into: the key's own slot, or the empty one a new key takes.
     * A new key that would pass the maximum load grows the table first. The caller writes the slot.
     */
    long claim(int key) {
        long slot = findSlot(slots.address, slotCount, key);
        if (Unsafe.getInt(slot + 4) == 0 && keyCount == slotCount / 2) {
            if (slotCount == MAX_SLOTS) {
                throw CairoException.nonCritical().put("hash join build capacity overflow");
            }
            grow(slotCount * 2);
            slot = findSlot(slots.address, slotCount, key);
        }
        return slot;
    }

    @Override
    public void close() {
        slots.close();
        keyCount = slotCount = 0;
        circuitBreaker = null;
    }

    /** Allocated native bytes. */
    long getSizeInBytes() {
        return slots.capacity;
    }

    /**
     * Binds the execution and the calling thread's circuit breaker, which growth checks, and
     * allocates this many empty slots, a power of two. The caller's failure path closes the table.
     */
    void open(@Nullable MemoryTracker memoryTracker, SqlExecutionCircuitBreaker circuitBreaker, int slotCount) {
        this.circuitBreaker = circuitBreaker;
        slots.of(memoryTracker, circuitBreaker);
        circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
        slots.allocate((long) slotCount * SLOT_SIZE, true);
        this.slotCount = slotCount;
    }

    /**
     * Grows the table so that this many distinct keys fit without a rehash. A count past the largest
     * table is not an error here: the keys it bounds may still fit, so growth decides that.
     */
    void reserve(long keyCount) {
        final int slots = Numbers.ceilPow2(2 * (int) Math.min(keyCount, MAX_SLOTS / 2));
        if (slots > slotCount) {
            grow(slots);
        }
    }

    // Rehashes into a table of the given power-of-two slot count, larger than the current one.
    private void grow(int newSlotCount) {
        // Separate destination keeps both allocations charged throughout rehashing.
        final HashJoinBuffer dest = slots.scratch();
        try {
            dest.allocate((long) newSlotCount * SLOT_SIZE, true);
            for (int i = 0, n = slotCount; i < n; i++) {
                if ((i & (KEY_SLOTS_PER_CHECK - 1)) == 0) {
                    circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
                }
                long src = slots.address + (long) i * SLOT_SIZE;
                int value = Unsafe.getInt(src + 4);
                if (value != 0) {
                    int key = Unsafe.getInt(src);
                    int index = (int) Hash.hashInt64(key) & (newSlotCount - 1);
                    long target = dest.address + (long) index * SLOT_SIZE;
                    while (Unsafe.getInt(target + 4) != 0) {
                        index = (index + 1) & (newSlotCount - 1);
                        target = dest.address + (long) index * SLOT_SIZE;
                    }
                    Unsafe.putInt(target, key);
                    Unsafe.putInt(target + 4, value);
                }
            }
            slots.take(dest);
            slotCount = newSlotCount;
        } finally {
            dest.close();
        }
    }
}
