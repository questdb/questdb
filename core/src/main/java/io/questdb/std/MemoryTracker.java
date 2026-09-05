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
 * Group tracker batches a signed delta in carrier-local state and publishes it
 * when the adaptive threshold or an execution boundary is reached. This keeps
 * the successful hot path allocation-free and free of shared atomics while
 * bounding temporarily unpublished usage.</p>
 */
public abstract class MemoryTracker implements Closeable {

    private static final AtomicInteger RESOURCE_MEMORY_ACTIVE_TRACKER_COUNT = new AtomicInteger();
    private static final long RESOURCE_MEMORY_CONTEXT_COUNT_OFFSET = 48;
    protected static final long RESOURCE_MEMORY_FLAG_ENFORCE = 1;
    private static final long RESOURCE_MEMORY_FLAG_MODE_MASK = 3;
    protected static final long RESOURCE_MEMORY_FLAG_SHADOW = 2;
    private static final int RESOURCE_MEMORY_FLAG_THRESHOLD_SHIFT = 2;
    private static final long RESOURCE_MEMORY_FLAGS_OFFSET = 24;
    private static final long RESOURCE_MEMORY_GENERATION_OFFSET = 56;
    private static final long RESOURCE_MEMORY_GROUP_OFFSET = 32;
    private static final long RESOURCE_MEMORY_MAGIC = 0x51444252474D454DL;
    private static final long RESOURCE_MEMORY_MAGIC_OFFSET = 16;
    private static final long RESOURCE_MEMORY_MAX_UNPUBLISHED_BYTES = 64 * 1024;
    private static final long RESOURCE_MEMORY_PROCESS_OFFSET = 40;
    private static final CarrierLocal<ResourceMemoryThreadState> RESOURCE_MEMORY_THREAD_STATE =
            new CarrierLocal<>(ResourceMemoryThreadState::new);

    // Covered-index buffers are released by a reusable reduce-task pool after
    // the owning query has ended. Their outstanding charge is reconciled at
    // query close so a pooled tracker cannot be credited by a later owner.
    private final AtomicLong coveredBytes = new AtomicLong();
    // One Rust QdbAllocator per native memory tag, created lazily and retained
    // for the lifetime of this pooled tracker.
    private final long[] nativeAllocators = new long[MemoryTag.SIZE - MemoryTag.NATIVE_DEFAULT];
    private long resourceMemoryGeneration;

    public final void addCoveredBytes(long delta) {
        if (delta != 0) {
            coveredBytes.addAndGet(delta);
        }
    }

    @Override
    public abstract void close();

    /**
     * Publishes and detaches Java and qdbr native state owned by the current
     * carrier. Execution-segment completion and carrier shutdown call this
     * method. The disabled fast path is one volatile read.
     */
    public static void detachResourceMemoryCurrentThread() {
        if (RESOURCE_MEMORY_ACTIVE_TRACKER_COUNT.get() == 0) {
            return;
        }
        final ResourceMemoryThreadState state = RESOURCE_MEMORY_THREAD_STATE.getIfPresent();
        try {
            if (state != null) {
                state.detach();
            }
        } finally {
            CarrierIdentity.detachMemoryTracker();
        }
    }

    public abstract long getLimit();

    public abstract long getQueryId();

    /**
     * Returns the currently published usage. Resource Group trackers may have
     * bounded carrier-local deltas that are not visible until the next publish
     * boundary.
     */
    public abstract long getUsed();

    public abstract MemoryTrackerWorkload getWorkload();

    public abstract long nativeAddress();

    /**
     * Publishes Java and qdbr native deltas without detaching the current
     * carrier. Cooperative circuit-breaker polls call this method.
     */
    public static void publishResourceMemoryCurrentThread() {
        if (RESOURCE_MEMORY_ACTIVE_TRACKER_COUNT.get() == 0) {
            return;
        }
        final ResourceMemoryThreadState state = RESOURCE_MEMORY_THREAD_STATE.getIfPresent();
        try {
            if (state != null) {
                state.publish();
            }
        } finally {
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
        final long base = nativeAddress();
        if (Unsafe.getLongVolatile(base + RESOURCE_MEMORY_MAGIC_OFFSET) != RESOURCE_MEMORY_MAGIC) {
            throw new IllegalStateException("Resource Group memory tracker is not configured");
        }
        Unsafe.putLongVolatile(base + RESOURCE_MEMORY_MAGIC_OFFSET, 0);
        Unsafe.putLongVolatile(base + RESOURCE_MEMORY_CONTEXT_COUNT_OFFSET, 0);
        Unsafe.putLongVolatile(base + RESOURCE_MEMORY_FLAGS_OFFSET, 0);
        Unsafe.putLongVolatile(base + RESOURCE_MEMORY_GROUP_OFFSET, 0);
        Unsafe.putLongVolatile(base + RESOURCE_MEMORY_PROCESS_OFFSET, 0);
        final int activeTrackerCount = RESOURCE_MEMORY_ACTIVE_TRACKER_COUNT.decrementAndGet();
        if (activeTrackerCount < 0) {
            RESOURCE_MEMORY_ACTIVE_TRACKER_COUNT.incrementAndGet();
            throw new IllegalStateException("Resource Group memory tracker counter underflow");
        }
    }

    protected final void configureResourceMemory(long groupAddress, long processAddress, boolean enforce) {
        if (groupAddress == 0 || processAddress == 0) {
            throw new IllegalArgumentException("Resource Group memory node addresses must be non-zero");
        }
        final long base = nativeAddress();
        resourceMemoryGeneration = resourceMemoryGeneration == Long.MAX_VALUE
                ? 1
                : resourceMemoryGeneration + 1;
        Unsafe.putLongVolatile(base + RESOURCE_MEMORY_CONTEXT_COUNT_OFFSET, 0);
        Unsafe.putLongVolatile(base + RESOURCE_MEMORY_GROUP_OFFSET, groupAddress);
        Unsafe.putLongVolatile(base + RESOURCE_MEMORY_PROCESS_OFFSET, processAddress);
        Unsafe.putLongVolatile(
                base + RESOURCE_MEMORY_FLAGS_OFFSET,
                resourceMemoryFlags(base, groupAddress, processAddress, enforce)
        );
        Unsafe.putLongVolatile(base + RESOURCE_MEMORY_GENERATION_OFFSET, resourceMemoryGeneration);
        Unsafe.putLongVolatile(base + RESOURCE_MEMORY_MAGIC_OFFSET, RESOURCE_MEMORY_MAGIC);
        final int activeTrackerCount = RESOURCE_MEMORY_ACTIVE_TRACKER_COUNT.incrementAndGet();
        if (activeTrackerCount < 1) {
            RESOURCE_MEMORY_ACTIVE_TRACKER_COUNT.decrementAndGet();
            Unsafe.putLongVolatile(base + RESOURCE_MEMORY_MAGIC_OFFSET, 0);
            throw new IllegalStateException("Resource Group memory tracker counter overflow");
        }
    }

    protected final synchronized void freeNativeAllocators() {
        for (int i = 0; i < nativeAllocators.length; i++) {
            if (nativeAllocators[i] != 0) {
                Unsafe.freeTrackerNativeAllocator(nativeAllocators[i]);
                nativeAllocators[i] = 0;
            }
        }
    }

    /**
     * Finalizes this Resource Group binding. Every execution segment must have
     * detached before the tracker can be recycled.
     */
    protected final void closeResourceMemory() {
        final long base = nativeAddress();
        if (Unsafe.getLongVolatile(base + RESOURCE_MEMORY_MAGIC_OFFSET) != RESOURCE_MEMORY_MAGIC) {
            return;
        }
        final long generation = Unsafe.getLongVolatile(base + RESOURCE_MEMORY_GENERATION_OFFSET);
        detachCurrentBinding(base, generation);
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

    protected final void updateResourceMemoryLimit(long expectedGroupAddress, long limit) {
        if (expectedGroupAddress == 0 || limit < 0) {
            throw new IllegalArgumentException("invalid Resource Group memory limit update");
        }
        final long base = nativeAddress();
        if (!isExpectedResourceMemoryBinding(base, expectedGroupAddress)) {
            return;
        }
        final long processAddress = Unsafe.getLongVolatile(base + RESOURCE_MEMORY_PROCESS_OFFSET);
        if (processAddress == 0) {
            throw new IllegalStateException("Resource Group memory tracker has incomplete hierarchy");
        }
        final long flags = Unsafe.getLongVolatile(base + RESOURCE_MEMORY_FLAGS_OFFSET);
        final long mode = flags & RESOURCE_MEMORY_FLAG_MODE_MASK;
        if (mode != RESOURCE_MEMORY_FLAG_ENFORCE && mode != RESOURCE_MEMORY_FLAG_SHADOW) {
            throw new IllegalStateException("Resource Group memory tracker has invalid flags: " + flags);
        }
        Unsafe.putLongVolatile(base + Unsafe.MEMORY_TRACKER_LIMIT_OFFSET, limit);
        Unsafe.putLongVolatile(
                base + RESOURCE_MEMORY_FLAGS_OFFSET,
                resourceMemoryFlags(
                        base,
                        expectedGroupAddress,
                        processAddress,
                        mode == RESOURCE_MEMORY_FLAG_ENFORCE
                )
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
        if (bytes <= 0) {
            return;
        }
        final long base = nativeAddress();
        if (base == 0) {
            return;
        }
        if (Unsafe.getLongVolatile(base + RESOURCE_MEMORY_MAGIC_OFFSET) == RESOURCE_MEMORY_MAGIC) {
            final long generation = Unsafe.getLongVolatile(base + RESOURCE_MEMORY_GENERATION_OFFSET);
            final long groupAddress = Unsafe.getLongVolatile(base + RESOURCE_MEMORY_GROUP_OFFSET);
            final long processAddress = Unsafe.getLongVolatile(base + RESOURCE_MEMORY_PROCESS_OFFSET);
            validateResourceBinding(base, generation, groupAddress, processAddress);
            RESOURCE_MEMORY_THREAD_STATE.get().release(
                    this,
                    base,
                    generation,
                    groupAddress,
                    processAddress,
                    bytes,
                    resourceMemoryThreshold(Unsafe.getLongVolatile(base + RESOURCE_MEMORY_FLAGS_OFFSET))
            );
        } else {
            creditExact(base, bytes);
        }
    }

    final void reserve(long bytes, int memoryTag) {
        if (bytes <= 0) {
            return;
        }
        final long base = nativeAddress();
        if (base == 0) {
            return;
        }
        if (Unsafe.getLongVolatile(base + RESOURCE_MEMORY_MAGIC_OFFSET) != RESOURCE_MEMORY_MAGIC) {
            reserveExact(base, bytes, memoryTag);
            return;
        }
        final long generation = Unsafe.getLongVolatile(base + RESOURCE_MEMORY_GENERATION_OFFSET);
        final long groupAddress = Unsafe.getLongVolatile(base + RESOURCE_MEMORY_GROUP_OFFSET);
        final long processAddress = Unsafe.getLongVolatile(base + RESOURCE_MEMORY_PROCESS_OFFSET);
        validateResourceBinding(base, generation, groupAddress, processAddress);
        RESOURCE_MEMORY_THREAD_STATE.get().reserve(
                this,
                base,
                generation,
                groupAddress,
                processAddress,
                bytes,
                memoryTag,
                resourceMemoryThreshold(Unsafe.getLongVolatile(base + RESOURCE_MEMORY_FLAGS_OFFSET))
        );
    }

    private static long addPublished(long address, long delta) {
        final long usedAddress = address + Unsafe.MEMORY_TRACKER_USED_OFFSET;
        while (true) {
            final long used = Unsafe.getLongVolatile(usedAddress);
            final long next = used + delta;
            if (((used ^ next) & (delta ^ next)) < 0) {
                throw new IllegalStateException("Resource Group memory counter overflow");
            }
            if (Unsafe.getUnsafe().compareAndSwapLong(null, usedAddress, used, next)) {
                return used;
            }
        }
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
        final long scaled = Math.min(RESOURCE_MEMORY_MAX_UNPUBLISHED_BYTES, narrowestLimit / 1024);
        return scaled > 0 ? Long.highestOneBit(scaled) : 1;
    }

    private static long creditExact(long address, long bytes) {
        final long usedAddress = address + Unsafe.MEMORY_TRACKER_USED_OFFSET;
        final long previous = Unsafe.getUnsafe().getAndAddLong(null, usedAddress, -bytes);
        final long used = previous - bytes;
        assert previous >= bytes : "memory tracker underflow [used=" + used + ", size=" + bytes + ']';
        if (previous < bytes) {
            Unsafe.getUnsafe().getAndAddLong(null, usedAddress, bytes - previous);
            return Math.max(previous, 0);
        }
        return previous;
    }

    private static void decrementContextCount(long base) {
        final long address = base + RESOURCE_MEMORY_CONTEXT_COUNT_OFFSET;
        final long previous = Unsafe.getUnsafe().getAndAddLong(null, address, -1);
        if (previous < 1) {
            Unsafe.getUnsafe().getAndAddLong(null, address, 1);
            throw new IllegalStateException("Resource Group memory context counter underflow");
        }
    }

    private static void detachCurrentBinding(long base, long generation) {
        final ResourceMemoryThreadState state = RESOURCE_MEMORY_THREAD_STATE.getIfPresent();
        try {
            if (state != null) {
                state.detachIf(base, generation);
            }
        } finally {
            CarrierIdentity.detachMemoryTracker(base, generation);
        }
    }

    private static void incrementContextCount(long base) {
        final long address = base + RESOURCE_MEMORY_CONTEXT_COUNT_OFFSET;
        final long previous = Unsafe.getUnsafe().getAndAddLong(null, address, 1);
        if (previous < 0 || previous == Long.MAX_VALUE) {
            Unsafe.getUnsafe().getAndAddLong(null, address, -1);
            throw new IllegalStateException("Resource Group memory context counter overflow");
        }
    }

    private static boolean isExpectedResourceMemoryBinding(long base, long expectedGroupAddress) {
        return Unsafe.getLongVolatile(base + RESOURCE_MEMORY_MAGIC_OFFSET) == RESOURCE_MEMORY_MAGIC
                && Unsafe.getLongVolatile(base + RESOURCE_MEMORY_GROUP_OFFSET) == expectedGroupAddress;
    }

    private static boolean isResourceBindingValid(
            long base,
            long generation,
            long groupAddress,
            long processAddress
    ) {
        return Unsafe.getLongVolatile(base + RESOURCE_MEMORY_MAGIC_OFFSET) == RESOURCE_MEMORY_MAGIC
                && Unsafe.getLongVolatile(base + RESOURCE_MEMORY_GENERATION_OFFSET) == generation
                && Unsafe.getLongVolatile(base + RESOURCE_MEMORY_GROUP_OFFSET) == groupAddress
                && Unsafe.getLongVolatile(base + RESOURCE_MEMORY_PROCESS_OFFSET) == processAddress;
    }

    private static long publishedUsed(long address) {
        return Math.max(Unsafe.getLongVolatile(address + Unsafe.MEMORY_TRACKER_USED_OFFSET), 0);
    }

    private void publishBoundaryDelta(
            long base,
            long generation,
            long groupAddress,
            long processAddress,
            long delta
    ) {
        if (delta == 0 || !isResourceBindingValid(base, generation, groupAddress, processAddress)) {
            return;
        }
        if (delta == Long.MIN_VALUE) {
            throw new IllegalStateException("Resource Group memory counter overflow");
        }
        final long rollbackDelta = -delta;
        addPublished(base, delta);
        boolean processPublished = false;
        try {
            addPublished(processAddress, delta);
            processPublished = true;
            addPublished(groupAddress, delta);
        } catch (Throwable th) {
            if (processPublished) {
                addPublished(processAddress, rollbackDelta);
            }
            addPublished(base, rollbackDelta);
            throw th;
        }
    }

    private void publishEnforcedDelta(
            long base,
            long generation,
            long groupAddress,
            long processAddress,
            long delta,
            long requestedBytes,
            int memoryTag
    ) {
        if (!isResourceBindingValid(base, generation, groupAddress, processAddress)) {
            throw new IllegalStateException("Resource Group memory tracker binding changed during allocation");
        }
        final long flags = Unsafe.getLongVolatile(base + RESOURCE_MEMORY_FLAGS_OFFSET);
        final long mode = flags & RESOURCE_MEMORY_FLAG_MODE_MASK;
        if (mode == RESOURCE_MEMORY_FLAG_SHADOW) {
            publishBoundaryDelta(base, generation, groupAddress, processAddress, delta);
            return;
        }
        if (mode != RESOURCE_MEMORY_FLAG_ENFORCE) {
            throw new IllegalStateException("Resource Group memory tracker has invalid flags: " + flags);
        }
        if (!tryAddPublished(base, delta, true)) {
            throwLimitExceeded(memoryTag, "query", base, requestedBytes);
        }
        if (!tryAddPublished(processAddress, delta, true)) {
            addPublished(base, -delta);
            throwLimitExceeded(memoryTag, "process", processAddress, requestedBytes);
        }
        if (!tryAddPublished(groupAddress, delta, true)) {
            addPublished(processAddress, -delta);
            addPublished(base, -delta);
            throwLimitExceeded(memoryTag, "group", groupAddress, requestedBytes);
        }
    }

    private void reserveExact(long address, long bytes, int memoryTag) {
        final long limit = Unsafe.getLongVolatile(address + Unsafe.MEMORY_TRACKER_LIMIT_OFFSET);
        final long used = publishedUsed(address);
        if ((limit == 0 || used <= limit && bytes <= limit - used)
                && used <= Long.MAX_VALUE - bytes) {
            Unsafe.getUnsafe().getAndAddLong(null, address + Unsafe.MEMORY_TRACKER_USED_OFFSET, bytes);
            return;
        }
        throw CairoException.nonCritical().setOutOfMemory(true)
                .put("query memory limit exceeded [workload=").put(getWorkload().name())
                .put(", queryId=").put(getQueryId())
                .put(", limit=").put(limit)
                .put(", used=").put(used)
                .put(", size=").put(bytes)
                .put(", memoryTag=").put(memoryTag)
                .put(']');
    }

    private static long resourceMemoryFlags(
            long base,
            long groupAddress,
            long processAddress,
            boolean enforce
    ) {
        final long mode = enforce ? RESOURCE_MEMORY_FLAG_ENFORCE : RESOURCE_MEMORY_FLAG_SHADOW;
        return mode
                | (calculateUnpublishedThreshold(base, groupAddress, processAddress)
                << RESOURCE_MEMORY_FLAG_THRESHOLD_SHIFT);
    }

    private static long resourceMemoryThreshold(long flags) {
        final long threshold = flags >>> RESOURCE_MEMORY_FLAG_THRESHOLD_SHIFT;
        if (threshold < 1
                || threshold > RESOURCE_MEMORY_MAX_UNPUBLISHED_BYTES
                || (threshold & (threshold - 1)) != 0) {
            throw new IllegalStateException("Resource Group memory tracker has invalid unpublished threshold: " + threshold);
        }
        return threshold;
    }

    private void throwLimitExceeded(int memoryTag, CharSequence scope, long address, long bytes) {
        final long limit = Unsafe.getLongVolatile(address + Unsafe.MEMORY_TRACKER_LIMIT_OFFSET);
        final long used = publishedUsed(address);
        throw CairoException.nonCritical().setOutOfMemory(true)
                .put("query memory limit exceeded [workload=").put(getWorkload().name())
                .put(", queryId=").put(getQueryId())
                .put(", scope=").put(scope)
                .put(", reason=limit")
                .put(", limit=").put(limit)
                .put(", used=").put(used)
                .put(", size=").put(bytes)
                .put(", memoryTag=").put(memoryTag)
                .put(']');
    }

    private static boolean tryAddPublished(long address, long delta, boolean enforce) {
        final long usedAddress = address + Unsafe.MEMORY_TRACKER_USED_OFFSET;
        final long limit = Unsafe.getLongVolatile(address + Unsafe.MEMORY_TRACKER_LIMIT_OFFSET);
        while (true) {
            final long used = Unsafe.getLongVolatile(usedAddress);
            final long next = used + delta;
            if (((used ^ next) & (delta ^ next)) < 0 || (enforce && limit > 0 && next > limit)) {
                return false;
            }
            if (Unsafe.getUnsafe().compareAndSwapLong(null, usedAddress, used, next)) {
                return true;
            }
        }
    }

    private static void validateResourceBinding(
            long base,
            long generation,
            long groupAddress,
            long processAddress
    ) {
        if (generation == 0 || groupAddress == 0 || processAddress == 0
                || !isResourceBindingValid(base, generation, groupAddress, processAddress)) {
            throw new IllegalStateException("Resource Group memory tracker has incomplete hierarchy");
        }
    }

    private static final class ResourceMemoryThreadState {
        private long delta;
        private long generation;
        private long groupAddress;
        private MemoryTracker owner;
        private long processAddress;
        private long trackerAddress;

        private void bind(
                MemoryTracker owner,
                long trackerAddress,
                long generation,
                long groupAddress,
                long processAddress
        ) {
            if (this.trackerAddress == trackerAddress && this.generation == generation) {
                return;
            }
            detach();
            validateResourceBinding(trackerAddress, generation, groupAddress, processAddress);
            this.generation = generation;
            this.groupAddress = groupAddress;
            this.owner = owner;
            this.processAddress = processAddress;
            this.trackerAddress = trackerAddress;
            incrementContextCount(trackerAddress);
            if (!isResourceBindingValid(trackerAddress, generation, groupAddress, processAddress)) {
                decrementContextCount(trackerAddress);
                clear();
                throw new IllegalStateException("Resource Group memory tracker changed while binding carrier state");
            }
        }

        private void clear() {
            delta = 0;
            generation = 0;
            groupAddress = 0;
            owner = null;
            processAddress = 0;
            trackerAddress = 0;
        }

        private void detach() {
            if (trackerAddress == 0) {
                return;
            }
            try {
                publish();
            } finally {
                if (isResourceBindingValid(trackerAddress, generation, groupAddress, processAddress)) {
                    decrementContextCount(trackerAddress);
                }
                clear();
            }
        }

        private void detachIf(long trackerAddress, long generation) {
            if (this.trackerAddress == trackerAddress && this.generation == generation) {
                detach();
            }
        }

        private void publish() {
            if (delta == 0) {
                return;
            }
            if (isResourceBindingValid(trackerAddress, generation, groupAddress, processAddress)) {
                owner.publishBoundaryDelta(trackerAddress, generation, groupAddress, processAddress, delta);
            }
            delta = 0;
        }

        private void release(
                MemoryTracker owner,
                long trackerAddress,
                long generation,
                long groupAddress,
                long processAddress,
                long bytes,
                long threshold
        ) {
            bind(owner, trackerAddress, generation, groupAddress, processAddress);
            final long next = Math.subtractExact(delta, bytes);
            if (next <= -threshold) {
                owner.publishBoundaryDelta(trackerAddress, generation, groupAddress, processAddress, next);
                delta = 0;
            } else {
                delta = next;
            }
        }

        private void reserve(
                MemoryTracker owner,
                long trackerAddress,
                long generation,
                long groupAddress,
                long processAddress,
                long bytes,
                int memoryTag,
                long threshold
        ) {
            bind(owner, trackerAddress, generation, groupAddress, processAddress);
            final long previous = delta;
            final long next = Math.addExact(previous, bytes);
            if (next >= threshold) {
                try {
                    owner.publishEnforcedDelta(
                            trackerAddress,
                            generation,
                            groupAddress,
                            processAddress,
                            next,
                            bytes,
                            memoryTag
                    );
                    delta = 0;
                } catch (Throwable th) {
                    delta = previous;
                    throw th;
                }
            } else {
                delta = next;
            }
        }
    }
}
