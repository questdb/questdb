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

/**
 * OSS {@link MemoryTracker} implementation: one tracker per workload
 * invocation, pooled across acquisitions. The native {@code {used, limit}}
 * block is allocated once on construction and reused across pool returns;
 * {@code acquire()} resets {@code used = 0} and writes the workload-appropriate
 * limit to the existing block. Native alloc/free only happens on a cold-path
 * pool miss or when the owning provider is closed at engine shutdown.
 */
public final class PerQueryMemoryTracker extends MemoryTracker {

    private final long nativeAddress;
    private final PerQueryMemoryTrackerProvider provider;
    private long queryId;
    private MemoryTrackerWorkload workload;

    PerQueryMemoryTracker(PerQueryMemoryTrackerProvider provider) {
        this.provider = provider;
        this.nativeAddress = Unsafe.malloc(Unsafe.MEMORY_TRACKER_BLOCK_SIZE, MemoryTag.NATIVE_MEMORY_TRACKER);
        // The counters must start zeroed: used = 0, limit = 0.
        Unsafe.getUnsafe().putLong(nativeAddress + Unsafe.MEMORY_TRACKER_USED_OFFSET, 0L);
        Unsafe.getUnsafe().putLong(nativeAddress + Unsafe.MEMORY_TRACKER_LIMIT_OFFSET, 0L);
    }

    @Override
    public void close() {
        provider.release(this);
    }

    @Override
    public long getLimit() {
        return Unsafe.getLongVolatile(nativeAddress + Unsafe.MEMORY_TRACKER_LIMIT_OFFSET);
    }

    @Override
    public long getQueryId() {
        return queryId;
    }

    @Override
    public long getUsed() {
        return Unsafe.getLongVolatile(nativeAddress + Unsafe.MEMORY_TRACKER_USED_OFFSET);
    }

    @Override
    public MemoryTrackerWorkload getWorkload() {
        return workload;
    }

    @Override
    public long nativeAddress() {
        return nativeAddress;
    }

    /**
     * Initializes the tracker for a new workload invocation: resets the
     * native counter to {@code 0} and stores the workload-appropriate limit.
     */
    void init(long queryId, MemoryTrackerWorkload workload, long limit) {
        this.queryId = queryId;
        this.workload = workload;
        Unsafe.putLongVolatile(nativeAddress + Unsafe.MEMORY_TRACKER_USED_OFFSET, 0L);
        Unsafe.putLongVolatile(nativeAddress + Unsafe.MEMORY_TRACKER_LIMIT_OFFSET, limit);
    }

    /**
     * Releases all native memory owned by this tracker: the
     * {@code {used, limit}} block and every per-tag Rust allocator block. Called
     * by the provider when the pooled tracker is finally disposed (engine
     * shutdown or pool-capacity overflow).
     */
    void destroy() {
        freeNativeAllocators();
        Unsafe.free(nativeAddress, Unsafe.MEMORY_TRACKER_BLOCK_SIZE, MemoryTag.NATIVE_MEMORY_TRACKER);
    }
}
