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

import io.questdb.cairo.CairoException;
import io.questdb.std.LongLongHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.bytes.Bytes;

/**
 * Thread-unsafe allocator implementation.
 */
public class GroupByAllocatorArena implements GroupByAllocator {
    // Holds <ptr, size> pairs.
    private final LongLongHashMap chunks = new LongLongHashMap();
    private final long defaultChunkSize;
    private final long maxChunkSize;
    private long allocated;
    private long lim;
    private long ptr;

    public GroupByAllocatorArena(long defaultChunkSize, long maxChunkSize) {
        this.defaultChunkSize = defaultChunkSize;
        this.maxChunkSize = maxChunkSize;
    }

    // Allocated chunks total (bytes).
    @Override
    public long allocated() {
        return allocated;
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

    @Override
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

    @Override
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
        long allocatedPtr = Unsafe.malloc(chunkSize, MemoryTag.NATIVE_GROUP_BY_FUNCTION);
        chunks.put(allocatedPtr, chunkSize);
        allocated += chunkSize;
        ptr = Bytes.align8b(allocatedPtr + size);
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
                    long chunkPtr = Unsafe.realloc(ptr, chunkSize, newSize, MemoryTag.NATIVE_GROUP_BY_FUNCTION);
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
