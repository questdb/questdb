/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
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

package io.questdb.griffin.engine.groupby;

import io.questdb.std.ObjList;
import io.questdb.std.ThreadLocal;
import org.jetbrains.annotations.TestOnly;

/**
 * Thread-safe allocator implementation. Uses thread-local {@link GroupByAllocatorArena}s.
 * <p>
 * free() method is best-effort, i.e. the only way to free all memory is to close
 * the allocator. This is fine for GROUP BY functions since they start small and
 * grow their state as power of 2.
 * <p>
 * The purpose of this allocator is to amortize the cost of frequent alloc()/free()
 * calls.
 */
public class GroupByAllocatorImpl implements GroupByAllocator {
    private final ObjList<GroupByAllocatorArena> arenas = new ObjList<>(); // protected by lock
    private final long defaultChunkSize;
    private final Object lock = new Object();
    private final long maxChunkSize;
    private final ThreadLocal<GroupByAllocatorArena> tlArena = new ThreadLocal<>(this::newArena);

    public GroupByAllocatorImpl(long defaultChunkSize, long maxChunkSize) {
        this.defaultChunkSize = defaultChunkSize;
        this.maxChunkSize = maxChunkSize;
    }

    /**
     * Returns allocated chunks total, in bytes. This method is not thread-safe
     * and shouldn't be called concurrently with any alloc/free calls.
     */
    @TestOnly
    @Override
    public long allocated() {
        long allocated = 0;
        for (int i = 0, n = arenas.size(); i < n; i++) {
            allocated += arenas.getQuick(i).allocated();
        }
        return allocated;
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
    @Override
    public void free(long ptr, long size) {
        tlArena.get().free(ptr, size);
    }

    @Override
    public long malloc(long size) {
        return tlArena.get().malloc(size);
    }

    @Override
    public long realloc(long ptr, long oldSize, long newSize) {
        return tlArena.get().realloc(ptr, oldSize, newSize);
    }

    private GroupByAllocatorArena newArena() {
        final GroupByAllocatorArena arena = new GroupByAllocatorArena(defaultChunkSize, maxChunkSize);
        synchronized (lock) {
            arenas.add(arena);
        }
        return arena;
    }
}
