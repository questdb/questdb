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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.CairoException;
import io.questdb.std.DirectLongLongHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.bytes.Bytes;

/**
 * Thread-unsafe allocator implementation.
 * <p>
 * free() method is best-effort, i.e. the only way to free all memory is to close
 * the allocator. This is fine for GROUP BY functions since they start small and
 * grow their state as power of 2.
 * <p>
 * The purpose of this allocator is to amortize the cost of frequent alloc()/free()
 * calls. This comes at the cost of memory fragmentation.
 */
public class FastGroupByAllocator implements GroupByAllocator {
    private final boolean aligned;
    // Holds <ptr, size> pairs.
    private final DirectLongLongHashMap chunks;
    private final long defaultChunkSize;
    private final long maxChunkSize;
    private long allocated;
    private long lim;
    private long ptr;

    public FastGroupByAllocator(long defaultChunkSize, long maxChunkSize) {
        this(defaultChunkSize, maxChunkSize, true);
    }

    public FastGroupByAllocator(long defaultChunkSize, long maxChunkSize, boolean aligned) {
        this.defaultChunkSize = defaultChunkSize;
        this.maxChunkSize = maxChunkSize;
        this.aligned = aligned;
        this.chunks = new DirectLongLongHashMap(8, 0.5, 0, 0, MemoryTag.NATIVE_GROUP_BY_FUNCTION);
    }

    // Allocated chunks total (bytes).
    @Override
    public long allocated() {
        return allocated;
    }

    @Override
    public void clear() {
        _close();
        chunks.restoreInitialCapacity();
    }

    @Override
    public void close() {
        _close();
        Misc.free(chunks);
    }

    @Override
    public void free(long ptr, long size) {
        if (size < defaultChunkSize) {
            // We don't free small allocations.
            return;
        }
        long index = chunks.keyIndex(ptr);
        if (index < 0) {
            long chunkSize = chunks.valueAt(index);
            if (size == chunkSize) {
                // We're lucky! We can free the whole chunk.
                Unsafe.free(ptr, chunkSize, MemoryTag.NATIVE_GROUP_BY_FUNCTION);
                chunks.removeAt(index);
                allocated -= chunkSize;
                if (this.ptr == alignMaybe(ptr + chunkSize)) {
                    this.ptr = lim = 0;
                }
            }
        }
    }

    @Override
    public long malloc(long size) {
        if (size > maxChunkSize) {
            throw CairoException.nonCritical().put("too large allocation requested: ").put(size);
        }

        if (ptr + size <= lim) {
            long allocatedPtr = ptr;
            ptr = alignMaybe(allocatedPtr + size);
            return allocatedPtr;
        }

        long chunkSize = Math.max(size, defaultChunkSize);
        long allocatedPtr = Unsafe.malloc(chunkSize, MemoryTag.NATIVE_GROUP_BY_FUNCTION);
        chunks.put(allocatedPtr, chunkSize);
        allocated += chunkSize;
        ptr = alignMaybe(allocatedPtr + size);
        lim = allocatedPtr + chunkSize;
        return allocatedPtr;
    }

    @Override
    public long realloc(long ptr, long oldSize, long newSize) {
        if (newSize > maxChunkSize) {
            throw CairoException.nonCritical().put("too large allocation requested: ").put(newSize);
        }
        if (newSize <= oldSize) {
            return ptr;
        }

        if (this.ptr == alignMaybe(ptr + oldSize)) {
            // Potential fast path:
            // we've just allocated this memory, so maybe we don't need to do anything?
            if (ptr + newSize <= lim) {
                // Great, we can simply use the remaining part of the chunk.
                this.ptr = alignMaybe(ptr + newSize);
                return ptr;
            }
        }

        if (oldSize >= defaultChunkSize) {
            // Check another potential fast path:
            // maybe we can reallocate the whole chunk?
            long index = chunks.keyIndex(ptr);
            if (index < 0) {
                long chunkSize = chunks.valueAt(index);
                if (chunkSize == oldSize) {
                    // Nice, we can reallocate the whole chunk.
                    long chunkPtr = Unsafe.realloc(ptr, chunkSize, newSize, MemoryTag.NATIVE_GROUP_BY_FUNCTION);
                    allocated += newSize - chunkSize;
                    chunks.removeAt(index);
                    chunks.put(chunkPtr, newSize);
                    lim = chunkPtr + newSize;
                    this.ptr = alignMaybe(lim);
                    return chunkPtr;
                }
            }
        }

        // Slow path.
        long allocatedPtr = malloc(newSize);
        Vect.memcpy(allocatedPtr, ptr, oldSize);
        return allocatedPtr;
    }

    @Override
    public void reopen() {
        chunks.reopen();
    }

    private void _close() {
        for (int i = 0, n = chunks.capacity(); i < n; i++) {
            long ptr = chunks.keyAtRaw(i);
            if (ptr != 0) {
                long size = chunks.valueAtRaw(i);
                Unsafe.free(ptr, size, MemoryTag.NATIVE_GROUP_BY_FUNCTION);
            }
        }
        allocated = 0;
        ptr = lim = 0;
    }

    private long alignMaybe(long ptr) {
        return aligned ? Bytes.align8b(ptr) : ptr;
    }
}
