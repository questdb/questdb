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
import io.questdb.mp.CarrierIdentity;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks native memory charged to one bounded workload. The first 16 bytes of
 * the native block are the stable OSS {@code {used, limit}} ABI. Enterprise
 * may initialize the versioned Resource Group tail in the same cache line.
 *
 * <p>Plain OSS trackers publish every allocation synchronously. A Resource
 * Group tracker batches a signed delta in OS-thread-local state owned by
 * libquestdbr, shared by Java and native allocations, and publishes it when
 * the adaptive threshold or an execution boundary is reached.</p>
 */
public abstract class MemoryTracker implements Closeable {

    private static final AtomicInteger RESOURCE_MEMORY_ACTIVE_TRACKER_COUNT = new AtomicInteger();
    private static final long RESOURCE_MEMORY_CONTEXT_COUNT_OFFSET = 48;
    private static final long RESOURCE_MEMORY_GENERATION_OFFSET = 56;
    private static final long RESOURCE_MEMORY_GROUP_OFFSET = 32;
    private static final long RESOURCE_MEMORY_MAGIC = 0x51444252474D454DL;
    private static final long RESOURCE_MEMORY_MAGIC_OFFSET = 16;
    private static final long RESOURCE_MEMORY_MAX_UNPUBLISHED_BYTES = 64 * 1024;
    private static final long RESOURCE_MEMORY_PROCESS_OFFSET = 40;
    private static final long RESOURCE_MEMORY_THRESHOLD_OFFSET = 24;
    private static final int SCOPE_GROUP = 3;
    private static final int SCOPE_PROCESS = 2;
    private static final int SCOPE_QUERY = 1;

    // Covered-index buffers are released by a reusable reduce-task pool after
    // the owning query has ended. Their outstanding charge is reconciled at
    // query close so a pooled tracker cannot be credited by a later owner.
    private final AtomicLong coveredBytes = new AtomicLong();
    // One Rust QdbAllocator per native memory tag, created lazily and retained
    // for the lifetime of this pooled tracker.
    private final long[] nativeAllocators = new long[MemoryTag.SIZE - MemoryTag.NATIVE_DEFAULT];
    private long nativeAddress;
    private long resourceMemoryGeneration;

    protected MemoryTracker() {
        nativeAddress = Unsafe.malloc(Unsafe.MEMORY_TRACKER_BLOCK_SIZE, MemoryTag.NATIVE_MEMORY_TRACKER);
        Vect.memset(nativeAddress, Unsafe.MEMORY_TRACKER_BLOCK_SIZE, 0);
    }

    public final void addCoveredBytes(long delta) {
        if (delta != 0) {
            coveredBytes.addAndGet(delta);
        }
    }

    @Override
    public abstract void close();

    /**
     * Publishes and detaches the Resource Group delta owned by the current
     * carrier. Execution-segment completion and carrier shutdown call this
     * method. The disabled fast path is one volatile read.
     */
    public static void detachResourceMemoryCurrentThread() {
        if (RESOURCE_MEMORY_ACTIVE_TRACKER_COUNT.get() != 0) {
            CarrierIdentity.detachMemoryTracker(0, 0);
        }
    }

    public final long getLimit() {
        return Unsafe.getLongVolatile(nativeAddress + Unsafe.MEMORY_TRACKER_LIMIT_OFFSET);
    }

    public abstract long getQueryId();

    /**
     * Returns the currently published usage. Resource Group trackers may have
     * bounded carrier-local deltas that are not visible until the next publish
     * boundary.
     */
    public final long getUsed() {
        return Math.max(Unsafe.getLongVolatile(nativeAddress + Unsafe.MEMORY_TRACKER_USED_OFFSET), 0);
    }

    public abstract MemoryTrackerWorkload getWorkload();

    public final long nativeAddress() {
        return nativeAddress;
    }

    /**
     * Publishes the current carrier's Resource Group delta without detaching
     * it. Cooperative circuit-breaker polls call this method.
     */
    public static void publishResourceMemoryCurrentThread() {
        if (RESOURCE_MEMORY_ACTIVE_TRACKER_COUNT.get() != 0) {
            CarrierIdentity.publishMemoryTracker();
        }
    }

    public final void reconcileCovered() {
        final long covered = coveredBytes.getAndSet(0);
        if (covered != 0) {
            release(covered);
        }
    }

    protected final void clearResourceMemory() {
        final long base = nativeAddress;
        if (Unsafe.getLongVolatile(base + RESOURCE_MEMORY_MAGIC_OFFSET) != RESOURCE_MEMORY_MAGIC) {
            throw new IllegalStateException("Resource Group memory tracker is not configured");
        }
        Unsafe.putLongVolatile(base + RESOURCE_MEMORY_MAGIC_OFFSET, 0);
        Unsafe.putLongVolatile(base + RESOURCE_MEMORY_CONTEXT_COUNT_OFFSET, 0);
        Unsafe.putLongVolatile(base + RESOURCE_MEMORY_THRESHOLD_OFFSET, 0);
        Unsafe.putLongVolatile(base + RESOURCE_MEMORY_GROUP_OFFSET, 0);
        Unsafe.putLongVolatile(base + RESOURCE_MEMORY_PROCESS_OFFSET, 0);
        if (RESOURCE_MEMORY_ACTIVE_TRACKER_COUNT.decrementAndGet() < 0) {
            throw new IllegalStateException("Resource Group memory tracker counter underflow");
        }
    }

    /**
     * Finalizes this Resource Group binding. Every execution segment must have
     * detached before the tracker can be recycled.
     */
    protected final void closeResourceMemory() {
        final long base = nativeAddress;
        if (Unsafe.getLongVolatile(base + RESOURCE_MEMORY_MAGIC_OFFSET) != RESOURCE_MEMORY_MAGIC) {
            return;
        }
        CarrierIdentity.detachMemoryTracker(base, Unsafe.getLongVolatile(base + RESOURCE_MEMORY_GENERATION_OFFSET));
        final long contextCount = Unsafe.getLongVolatile(base + RESOURCE_MEMORY_CONTEXT_COUNT_OFFSET);
        if (contextCount != 0) {
            throw new IllegalStateException(
                    "cannot close Resource Group memory tracker with unpublished carrier contexts=" + contextCount
            );
        }
        final long used = Unsafe.getLongVolatile(base + Unsafe.MEMORY_TRACKER_USED_OFFSET);
        if (used != 0) {
            throw new IllegalStateException("cannot close Resource Group memory tracker with retained native memory=" + used);
        }
    }

    protected final void configureResourceMemory(long groupAddress, long processAddress) {
        if (groupAddress == 0 || processAddress == 0) {
            throw new IllegalArgumentException("Resource Group memory node addresses must be non-zero");
        }
        final long base = nativeAddress;
        Unsafe.putLongVolatile(base + RESOURCE_MEMORY_CONTEXT_COUNT_OFFSET, 0);
        Unsafe.putLongVolatile(base + RESOURCE_MEMORY_GROUP_OFFSET, groupAddress);
        Unsafe.putLongVolatile(base + RESOURCE_MEMORY_PROCESS_OFFSET, processAddress);
        Unsafe.putLongVolatile(
                base + RESOURCE_MEMORY_THRESHOLD_OFFSET,
                calculateUnpublishedThreshold(base, groupAddress, processAddress)
        );
        Unsafe.putLongVolatile(base + RESOURCE_MEMORY_GENERATION_OFFSET, ++resourceMemoryGeneration);
        Unsafe.putLongVolatile(base + RESOURCE_MEMORY_MAGIC_OFFSET, RESOURCE_MEMORY_MAGIC);
        RESOURCE_MEMORY_ACTIVE_TRACKER_COUNT.incrementAndGet();
    }

    /**
     * Releases all native memory owned by this tracker: the counter block and
     * every per-tag Rust allocator. A destroyed tracker ignores further
     * reservations and releases.
     */
    protected final void destroyNativeBlock() {
        freeNativeAllocators();
        nativeAddress = Unsafe.free(nativeAddress, Unsafe.MEMORY_TRACKER_BLOCK_SIZE, MemoryTag.NATIVE_MEMORY_TRACKER);
    }

    protected final void updateResourceMemoryLimit(long expectedGroupAddress, long limit) {
        if (expectedGroupAddress == 0 || limit < 0) {
            throw new IllegalArgumentException("invalid Resource Group memory limit update");
        }
        final long base = nativeAddress;
        if (Unsafe.getLongVolatile(base + RESOURCE_MEMORY_MAGIC_OFFSET) != RESOURCE_MEMORY_MAGIC
                || Unsafe.getLongVolatile(base + RESOURCE_MEMORY_GROUP_OFFSET) != expectedGroupAddress) {
            return;
        }
        final long processAddress = Unsafe.getLongVolatile(base + RESOURCE_MEMORY_PROCESS_OFFSET);
        if (processAddress == 0) {
            throw new IllegalStateException("Resource Group memory tracker has incomplete hierarchy");
        }
        Unsafe.putLongVolatile(base + Unsafe.MEMORY_TRACKER_LIMIT_OFFSET, limit);
        Unsafe.putLongVolatile(
                base + RESOURCE_MEMORY_THRESHOLD_OFFSET,
                calculateUnpublishedThreshold(base, expectedGroupAddress, processAddress)
        );
    }

    final synchronized long getOrCreateNativeAllocator(int memoryTag) {
        assert memoryTag >= MemoryTag.NATIVE_DEFAULT;
        final int idx = memoryTag - MemoryTag.NATIVE_DEFAULT;
        long addr = nativeAllocators[idx];
        if (addr == 0) {
            addr = Unsafe.constructTrackerNativeAllocator(this, memoryTag);
            nativeAllocators[idx] = addr;
        }
        return addr;
    }

    final void release(long bytes) {
        final long base = nativeAddress;
        if (bytes <= 0 || base == 0) {
            return;
        }
        if (Unsafe.getLongVolatile(base + RESOURCE_MEMORY_MAGIC_OFFSET) == RESOURCE_MEMORY_MAGIC) {
            CarrierIdentity.creditMemoryTracker(base, bytes);
            return;
        }
        final long usedAddress = base + Unsafe.MEMORY_TRACKER_USED_OFFSET;
        final long previous = Unsafe.getUnsafe().getAndAddLong(null, usedAddress, -bytes);
        if (previous < bytes) {
            assert false : "memory tracker underflow [used=" + (previous - bytes) + ", size=" + bytes + ']';
            Unsafe.getUnsafe().getAndAddLong(null, usedAddress, bytes - previous);
        }
    }

    final void reserve(long bytes, int memoryTag) {
        final long base = nativeAddress;
        if (bytes <= 0 || base == 0) {
            return;
        }
        if (Unsafe.getLongVolatile(base + RESOURCE_MEMORY_MAGIC_OFFSET) == RESOURCE_MEMORY_MAGIC) {
            final int scope = CarrierIdentity.chargeMemoryTracker(base, bytes);
            if (scope != 0) {
                throwLimitExceeded(scope, bytes, memoryTag);
            }
            return;
        }
        final long limit = getLimit();
        final long used = getUsed();
        if (limit != 0 && used + bytes > limit) {
            throwLimitExceeded("query", limit, used, bytes, memoryTag);
        }
        Unsafe.getUnsafe().getAndAddLong(null, base + Unsafe.MEMORY_TRACKER_USED_OFFSET, bytes);
    }

    private static long calculateUnpublishedThreshold(long base, long groupAddress, long processAddress) {
        long narrowestLimit = Long.MAX_VALUE;
        final long queryLimit = Unsafe.getLongVolatile(base + Unsafe.MEMORY_TRACKER_LIMIT_OFFSET);
        final long groupLimit = Unsafe.getLongVolatile(groupAddress + Unsafe.MEMORY_TRACKER_LIMIT_OFFSET);
        final long processLimit = Unsafe.getLongVolatile(processAddress + Unsafe.MEMORY_TRACKER_LIMIT_OFFSET);
        if (queryLimit > 0) {
            narrowestLimit = queryLimit;
        }
        if (groupLimit > 0) {
            narrowestLimit = Math.min(narrowestLimit, groupLimit);
        }
        if (processLimit > 0) {
            narrowestLimit = Math.min(narrowestLimit, processLimit);
        }
        if (narrowestLimit == Long.MAX_VALUE) {
            return RESOURCE_MEMORY_MAX_UNPUBLISHED_BYTES;
        }
        return Math.max(1, Math.min(RESOURCE_MEMORY_MAX_UNPUBLISHED_BYTES, narrowestLimit / 1024));
    }

    private synchronized void freeNativeAllocators() {
        for (int i = 0; i < nativeAllocators.length; i++) {
            if (nativeAllocators[i] != 0) {
                Unsafe.freeTrackerNativeAllocator(nativeAllocators[i]);
                nativeAllocators[i] = 0;
            }
        }
    }

    private void throwLimitExceeded(int scope, long bytes, int memoryTag) {
        final long base = nativeAddress;
        final String scopeName;
        final long address;
        switch (scope) {
            case SCOPE_QUERY -> {
                scopeName = "query";
                address = base;
            }
            case SCOPE_PROCESS -> {
                scopeName = "process";
                address = Unsafe.getLongVolatile(base + RESOURCE_MEMORY_PROCESS_OFFSET);
            }
            case SCOPE_GROUP -> {
                scopeName = "group";
                address = Unsafe.getLongVolatile(base + RESOURCE_MEMORY_GROUP_OFFSET);
            }
            default -> throw new IllegalStateException("Resource Group memory tracker has incomplete hierarchy");
        }
        throwLimitExceeded(
                scopeName,
                Unsafe.getLongVolatile(address + Unsafe.MEMORY_TRACKER_LIMIT_OFFSET),
                Math.max(Unsafe.getLongVolatile(address + Unsafe.MEMORY_TRACKER_USED_OFFSET), 0),
                bytes,
                memoryTag
        );
    }

    private void throwLimitExceeded(String scope, long limit, long used, long bytes, int memoryTag) {
        throw CairoException.nonCritical().setOutOfMemory(true)
                .put("query memory limit exceeded [workload=").put(getWorkload().name())
                .put(", queryId=").put(getQueryId())
                .put(", scope=").put(scope)
                .put(", limit=").put(limit)
                .put(", used=").put(used)
                .put(", size=").put(bytes)
                .put(", memoryTag=").put(memoryTag)
                .put(']');
    }
}
