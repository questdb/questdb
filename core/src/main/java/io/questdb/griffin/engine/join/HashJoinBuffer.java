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
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

/**
 * Growable native buffer of a hash join build, charged to the execution's memory tracker
 * under {@link MemoryTag#NATIVE_JOIN_MAP}. {@link IntHashJoinBuild} uses it for its key
 * table and {@link HashJoinRowHeap} for its payload rows.
 * <p>
 * Growth allocates the destination before it frees the source, so both allocations stay
 * charged while the copy runs and a breached limit leaves the buffer holding the block it
 * had. Loops that are not bounded by a row - the clear on allocation and the copy on
 * growth - check the circuit breaker once per MiB they touch.
 * <p>
 * The growth destination is a dedicated scratch buffer that the constructor allocates, so
 * growing allocates no Java object. A scratch buffer holds memory only inside one growth;
 * the caller closes it on both paths.
 */
final class HashJoinBuffer implements Closeable {
    static final long COPY_CHUNK_SIZE = 1024 * 1024;
    private final long maxCapacity;
    @Nullable
    private final HashJoinBuffer scratch;
    // Read directly by the builds and their frozen snapshots, as the hot loops read a slot
    // or a row address without a call.
    long address;
    long capacity;
    private SqlExecutionCircuitBreaker circuitBreaker;
    @Nullable
    private MemoryTracker memoryTracker;

    HashJoinBuffer(long maxCapacity) {
        this(maxCapacity, true);
    }

    private HashJoinBuffer(long maxCapacity, boolean growable) {
        this.maxCapacity = maxCapacity;
        this.scratch = growable ? new HashJoinBuffer(maxCapacity, false) : null;
    }

    /** Allocates the initial block. Clearing checks the breaker once per MiB. */
    void allocate(long size, boolean clear) {
        address = Unsafe.malloc(size, MemoryTag.NATIVE_JOIN_MAP, memoryTracker);
        capacity = size;
        if (clear) {
            for (long offset = 0; offset < size; offset += COPY_CHUNK_SIZE) {
                circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
                Vect.memset(address + offset, Math.min(size - offset, COPY_CHUNK_SIZE), 0);
            }
        }
    }

    /** Releases the block and unbinds the execution, so a later use fails loudly. */
    @Override
    public void close() {
        free();
        memoryTracker = null;
        circuitBreaker = null;
    }

    /**
     * Grows the buffer to hold at least {@code required} bytes, doubling and copying what
     * it holds. A buffer with no block yet takes {@code initialCapacity} as its floor.
     */
    void ensure(long required, long initialCapacity) {
        if (required > maxCapacity || required < 0) {
            throw CairoException.nonCritical().put("hash join build buffer overflow");
        }
        if (required <= capacity) {
            return;
        }
        final HashJoinBuffer dest = scratch();
        try {
            dest.allocate(Math.max(required, Math.min(maxCapacity, Math.max(initialCapacity, capacity * 2))), false);
            for (long offset = 0; offset < capacity; offset += COPY_CHUNK_SIZE) {
                circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
                Unsafe.copyMemory(address + offset, dest.address + offset, Math.min(capacity - offset, COPY_CHUNK_SIZE));
            }
            take(dest);
        } finally {
            dest.close();
        }
    }

    /** Binds the execution that charges and cancels this buffer's allocations. */
    void of(@Nullable MemoryTracker memoryTracker, SqlExecutionCircuitBreaker circuitBreaker) {
        this.memoryTracker = memoryTracker;
        this.circuitBreaker = circuitBreaker;
    }

    /**
     * The growth destination, bound to this buffer's execution. The caller allocates it,
     * fills it, hands it to {@link #take(HashJoinBuffer)} and closes it on both paths.
     */
    HashJoinBuffer scratch() {
        assert scratch != null;
        scratch.of(memoryTracker, circuitBreaker);
        return scratch;
    }

    /** Frees this buffer's block and adopts the one the caller filled. */
    void take(HashJoinBuffer other) {
        free();
        address = other.address;
        capacity = other.capacity;
        other.address = other.capacity = 0;
    }

    private void free() {
        if (address != 0) {
            address = Unsafe.free(address, capacity, MemoryTag.NATIVE_JOIN_MAP, memoryTracker);
            capacity = 0;
        }
    }
}
