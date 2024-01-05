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
import io.questdb.std.ThreadLocal;
import io.questdb.std.*;
import io.questdb.std.bytes.Bytes;

/**
 * Specialized thread-safe allocator used in
 * {@link io.questdb.griffin.engine.functions.GroupByFunction}s that require
 * additional off-heap state, e.g. min(str) or count_distinct().
 * <p>
 * Does not free memory until closed. This is fine for GROUP BY functions since
 * they grow their state as power of 2. In practice this means that the memory
 * overhead will be around 2x.
 * <p>
 * The purpose of this allocator is to amortize the cost of frequent alloc/free
 * calls.
 */
public class GroupByAllocator implements QuietCloseable {
    private final ObjList<Arena> arenas = new ObjList<>(); // protected by lock
    private final int defaultChunkSize;
    private final Object lock = new Object();
    private final ThreadLocal<Arena> tlArena = new ThreadLocal<>(this::newArena);

    public GroupByAllocator(CairoConfiguration configuration) {
        this.defaultChunkSize = configuration.getGroupByAllocatorChunkSize();
    }

    // Useful for debugging.
    @SuppressWarnings("unused")
    public long allocated() {
        long allocated = 0;
        synchronized (lock) {
            for (int i = 0, n = arenas.size(); i < n; i++) {
                allocated += arenas.getQuick(i).allocated();
            }
        }
        return allocated;
    }

    @Override
    public void close() {
        synchronized (lock) {
            for (int i = 0, n = arenas.size(); i < n; i++) {
                arenas.getQuick(i).close();
            }
        }
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
        private final LongList chunkPtrs = new LongList();
        private final IntList chunkSizes = new IntList();
        private int allocatedChunks;
        private long ptr;

        // Useful for debugging.
        @SuppressWarnings("unused")
        public long allocated() {
            long allocated = 0;
            for (int i = 0; i < allocatedChunks; i++) {
                allocated += chunkSizes.getQuick(i);
            }
            return allocated;
        }

        @Override
        public void close() {
            for (int i = 0; i < allocatedChunks; i++) {
                Unsafe.free(chunkPtrs.getQuick(i), chunkSizes.getQuick(i), MemoryTag.NATIVE_GROUP_BY_FUNCTION);
            }
            chunkPtrs.restoreInitialCapacity();
            chunkSizes.restoreInitialCapacity();
            allocatedChunks = 0;
            ptr = 0;
        }

        public long malloc(long size) {
            assert size < Integer.MAX_VALUE;
            long chunkLim = allocatedChunks > 0 ? chunkPtrs.getQuick(allocatedChunks - 1) + chunkSizes.getQuick(allocatedChunks - 1) : 0;
            if (ptr + size <= chunkLim) {
                long allocatedPtr = ptr;
                ptr = Bytes.align8b(allocatedPtr + size);
                return allocatedPtr;
            }

            int chunkSize = (int) Math.max(size, defaultChunkSize);
            long allocatedPtr = Unsafe.malloc(chunkSize, MemoryTag.NATIVE_GROUP_BY_FUNCTION);
            chunkPtrs.extendAndSet(allocatedChunks, allocatedPtr);
            chunkSizes.extendAndSet(allocatedChunks, chunkSize);
            allocatedChunks++;
            ptr = Bytes.align8b(allocatedPtr + size);
            return allocatedPtr;
        }

        public long realloc(long ptr, long oldSize, long newSize) {
            assert newSize < Integer.MAX_VALUE;
            assert oldSize < newSize;
            if (ptr == this.ptr - oldSize) {
                // Potential fast path for realloc:
                // we've just allocated this memory, so maybe we don't need to do anything?
                assert allocatedChunks > 0;
                long chunkPtr = chunkPtrs.getQuick(allocatedChunks - 1);
                int chunkSize = chunkSizes.getQuick(allocatedChunks - 1);
                if (ptr + newSize <= chunkPtr + chunkSize) {
                    // Great, we can simply use remaining part of the chunk.
                    this.ptr = Bytes.align8b(ptr + newSize);
                    return ptr;
                } else if (ptr == chunkPtr) {
                    // Nice, we can reallocate the whole chunk.
                    chunkPtr = Unsafe.realloc(chunkPtr, chunkSize, newSize, MemoryTag.NATIVE_GROUP_BY_FUNCTION);
                    chunkPtrs.setQuick(allocatedChunks - 1, chunkPtr);
                    chunkSizes.setQuick(allocatedChunks - 1, (int) newSize);
                    this.ptr = Bytes.align8b(chunkPtr + newSize);
                    return chunkPtr;
                }
            }
            long allocatedPtr = malloc(newSize);
            Vect.memcpy(allocatedPtr, ptr, oldSize);
            return allocatedPtr;
        }
    }
}
