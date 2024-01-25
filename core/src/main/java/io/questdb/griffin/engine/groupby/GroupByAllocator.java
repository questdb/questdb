/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.std.ThreadLocal;
import io.questdb.std.*;
import io.questdb.std.bytes.Bytes;
import org.jetbrains.annotations.TestOnly;

/**
 * Specialized thread-safe allocator used in
 * {@link io.questdb.griffin.engine.functions.GroupByFunction}s that require
 * additional off-heap state, e.g. min(str) or count_distinct().
 * <p>
 * free method is best-effort, i.e. the only way to free all memory is to close
 * the allocator. This is fine for GROUP BY functions since they start small and
 * grow their state as power of 2.
 * <p>
 * The allocator pre-touches the memory to avoid minor page faults on later
 * memory access.
 * <p>
 * The purpose of this allocator is to amortize the cost of frequent alloc/free
 * calls.
 */
public class GroupByAllocator implements QuietCloseable {
    private final ObjList<Arena> arenas = new ObjList<>(); // protected by lock
    private final long defaultChunkSize;
    private final Object lock = new Object();
    private final long maxChunkSize;
    private final ThreadLocal<Arena> tlArena = new ThreadLocal<>(this::newArena);

    public GroupByAllocator(CairoConfiguration configuration) {
        this.defaultChunkSize = configuration.getGroupByAllocatorDefaultChunkSize();
        this.maxChunkSize = configuration.getGroupByAllocatorMaxChunkSize();
    }

    /**
     * Returns allocated chunks total, in bytes. This method is not thread-safe
     * and shouldn't be called concurrently with any alloc/free calls.
     */
    @TestOnly
    public long allocated() {
        long allocated = 0;
        for (int i = 0, n = arenas.size(); i < n; i++) {
            allocated += arenas.getQuick(i).allocated();
        }
        return allocated;
    }

    public long calloc(long size) {
        return tlArena.get().calloc(size);
    }

    /**
     * This method is not thread-safe and shouldn't be called concurrently
     * with any alloc/free calls.
     */
    @Override
    public void close() {
        for (int i = 0, n = arenas.size(); i < n; i++) {
            arenas.getQuick(i).close();
        }
    }

    /**
     * Best-effort free memory operation. The memory shouldn't be used after it was called.
     */
    public void free(long ptr, long size) {
        tlArena.get().free(ptr, size);
    }

    public long malloc(long size) {
        return tlArena.get().malloc(size);
    }

    public long realloc(long ptr, long oldSize, long newSize) {
        return tlArena.get().realloc(ptr, oldSize, newSize);
    }

    private Arena newArena() {
        final Arena arena = new Arena();
        synchronized (lock) {
            arenas.add(arena);
        }
        return arena;
    }

    private class Arena implements QuietCloseable {
        // Holds <ptr, size> pairs.
        private final LongLongHashMap chunks = new LongLongHashMap();
        private long allocated;
        private long lim;
        private long ptr;

        // Allocated chunks total (bytes).
        public long allocated() {
            return allocated;
        }

        public long calloc(long size) {
            if (size > maxChunkSize) {
                throw CairoException.nonCritical().put("too large allocation requested: ").put(size);
            }

            if (ptr + size <= lim) {
                long allocatedPtr = ptr;
                ptr = Bytes.align8b(allocatedPtr + size);
                return allocatedPtr;
            }

            long allocatedPtr;
            long chunkSize;
            if (size < defaultChunkSize) {
                chunkSize = defaultChunkSize;
                allocatedPtr = Unsafe.malloc(chunkSize, MemoryTag.NATIVE_GROUP_BY_FUNCTION, true);
                Vect.memset(allocatedPtr, chunkSize, 0);
            } else {
                chunkSize = size;
                allocatedPtr = Unsafe.calloc(chunkSize, MemoryTag.NATIVE_GROUP_BY_FUNCTION, true);
            }
            chunks.put(allocatedPtr, chunkSize);
            allocated += chunkSize;
            ptr = Bytes.align8b(allocatedPtr + size);
            lim = allocatedPtr + chunkSize;
            return allocatedPtr;
        }

        @Override
        public void close() {
            for (int i = 0, n = chunks.capacity(); i < n; i++) {
                long ptr = chunks.keyAtRaw(i);
                if (ptr != -1) {
                    long size = chunks.valueAtRaw(i);
                    Unsafe.free(ptr, size, MemoryTag.NATIVE_GROUP_BY_FUNCTION);
                }
            }
            chunks.restoreInitialCapacity();
            allocated = 0;
            ptr = lim = 0;
        }

        // Best-effort free operation.
        public void free(long ptr, long size) {
            if (size < defaultChunkSize) {
                // We don't free small allocations.
                return;
            }
            int index = chunks.keyIndex(ptr);
            if (index < 0) {
                long chunkSize = chunks.valueAt(index);
                if (size == chunkSize) {
                    // We're lucky! We can free the whole chunk.
                    Unsafe.free(ptr, chunkSize, MemoryTag.NATIVE_GROUP_BY_FUNCTION);
                    chunks.removeAt(index);
                    allocated -= chunkSize;
                    if (this.ptr == Bytes.align8b(ptr + chunkSize)) {
                        this.ptr = lim = 0;
                    }
                }
            }
        }

        public long malloc(long size) {
            if (size > maxChunkSize) {
                throw CairoException.nonCritical().put("too large allocation requested: ").put(size);
            }

            if (ptr + size <= lim) {
                long allocatedPtr = ptr;
                ptr = Bytes.align8b(allocatedPtr + size);
                return allocatedPtr;
            }

            long chunkSize = Math.max(size, defaultChunkSize);
            long allocatedPtr = Unsafe.malloc(chunkSize, MemoryTag.NATIVE_GROUP_BY_FUNCTION, true);
            chunks.put(allocatedPtr, chunkSize);
            allocated += chunkSize;
            ptr = Bytes.align8b(allocatedPtr + size);
            lim = allocatedPtr + chunkSize;
            return allocatedPtr;
        }

        public long realloc(long ptr, long oldSize, long newSize) {
            if (newSize > maxChunkSize) {
                throw CairoException.nonCritical().put("too large allocation requested: ").put(newSize);
            }

            assert oldSize < newSize;
            if (this.ptr == Bytes.align8b(ptr + oldSize)) {
                // Potential fast path:
                // we've just allocated this memory, so maybe we don't need to do anything?
                if (ptr + newSize <= lim) {
                    // Great, we can simply use the remaining part of the chunk.
                    this.ptr = Bytes.align8b(ptr + newSize);
                    return ptr;
                }
            }

            if (oldSize >= defaultChunkSize) {
                // Check another potential fast path:
                // maybe we can reallocate the whole chunk?
                int index = chunks.keyIndex(ptr);
                if (index < 0) {
                    long chunkSize = chunks.valueAt(index);
                    if (chunkSize == oldSize) {
                        // Nice, we can reallocate the whole chunk.
                        long chunkPtr = Unsafe.realloc(ptr, chunkSize, newSize, MemoryTag.NATIVE_GROUP_BY_FUNCTION, true);
                        allocated += newSize - chunkSize;
                        chunks.removeAt(index);
                        chunks.put(chunkPtr, newSize);
                        lim = chunkPtr + newSize;
                        this.ptr = Bytes.align8b(lim);
                        return chunkPtr;
                    }
                }
            }

            // Slow path.
            long allocatedPtr = malloc(newSize);
            Vect.memcpy(allocatedPtr, ptr, oldSize);
            return allocatedPtr;
        }
    }
}
