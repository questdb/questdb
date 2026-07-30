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

package io.questdb.mp.continuation;

import io.questdb.std.ObjList;
import io.questdb.std.Os;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicInteger;

public final class FiberPool {
    private final @Nullable Runnable beforeWaitFireForTesting;
    private final AtomicInteger createdCount = new AtomicInteger();
    private final FiberRing freeList;
    private final AtomicInteger inFlightWaitRegistrationCount = new AtomicInteger();
    private volatile boolean isClosed;
    private final ObjList<Fiber> liveFibers = new ObjList<>();
    private final int maxLive;
    private final int maxRetained;
    private final AtomicInteger retainedCount = new AtomicInteger();
    private final AtomicInteger retiredCount = new AtomicInteger();
    private final FiberRuntime runtime;

    FiberPool(
            int maxRetained,
            int maxLive,
            FiberRuntime runtime,
            @Nullable Runnable beforeWaitFireForTesting
    ) {
        if (maxRetained < 1) {
            throw new IllegalArgumentException("maxRetained must be positive");
        }
        if (maxLive < maxRetained) {
            throw new IllegalArgumentException("maxLive must not be less than maxRetained");
        }
        if (runtime == null) {
            throw new IllegalArgumentException("fiber pool requires a runtime");
        }
        this.beforeWaitFireForTesting = beforeWaitFireForTesting;
        this.freeList = new FiberRing(maxRetained);
        this.maxLive = maxLive;
        this.maxRetained = maxRetained;
        this.runtime = runtime;
    }

    public int getCreatedCount() {
        return createdCount.get();
    }

    public synchronized int getLiveCount() {
        return liveFibers.size();
    }

    public synchronized int getParkedCount() {
        int count = 0;
        for (int i = 0, n = liveFibers.size(); i < n; i++) {
            if (liveFibers.getQuick(i).getExecutionState() == Fiber.EXECUTION_WAITING) {
                count++;
            }
        }
        return count;
    }

    public int getRetainedCount() {
        return retainedCount.get();
    }

    public int getRetiredCount() {
        return retiredCount.get();
    }

    public void release(Fiber fiber) {
        if (fiber.isDone()) {
            onRetired(fiber);
            return;
        }
        if (!isClosed) {
            if (retainedCount.incrementAndGet() <= maxRetained) {
                freeList.put(fiber);
                runtime.signalCapacity();
                if (isClosed) {
                    drainFreeList();
                }
                return;
            }
            retainedCount.decrementAndGet();
        }
        retire(fiber);
    }

    public Fiber tryAcquire() {
        if (isClosed) {
            throw new IllegalStateException("fiber pool is closed");
        }
        final Fiber fiber = freeList.tryDequeue();
        if (fiber != null) {
            retainedCount.decrementAndGet();
            if (!isClosed) {
                fiber.reserve();
                return fiber;
            }
            retire(fiber);
            throw new IllegalStateException("fiber pool is closed");
        }
        return tryAcquireSlow();
    }

    synchronized void beginQuiesce() {
        if (isClosed) {
            return;
        }
        isClosed = true;
        drainFreeList();
        for (int i = liveFibers.size() - 1; i >= 0; i--) {
            final Fiber fiber = liveFibers.getQuick(i);
            if (!fiber.isDone() && !fiber.isReserved()) {
                retire(fiber);
            }
        }
        drainFreeList();
    }

    boolean beginWaitArm() {
        return runtime.acquireAdmission();
    }

    void clearRegistry() {
        synchronized (this) {
            if (liveFibers.size() != 0) {
                throw new IllegalStateException("fiber registry is not empty");
            }
            liveFibers.clear();
        }
    }

    void endWaitArm() {
        runtime.releaseAdmission();
    }

    void enqueue(Fiber fiber) {
        runtime.enqueue(fiber);
    }

    FiberRuntime getRuntime() {
        return runtime;
    }

    boolean hasAvailableFiber() {
        if (isClosed) {
            return false;
        }
        if (freeList.depth() > 0) {
            return true;
        }
        synchronized (this) {
            return !isClosed && liveFibers.size() < maxLive;
        }
    }

    boolean hasInFlightWaitRegistrations() {
        return inFlightWaitRegistrationCount.get() > 0;
    }

    void onRetired(Fiber fiber) {
        if (!fiber.completeRetirement()) {
            throw new IllegalStateException("fiber retirement is not scheduled");
        }
        unregisterFiber(fiber);
        retiredCount.incrementAndGet();
        runtime.signalCapacity();
    }

    void onWaitRegistrationAcquired() {
        inFlightWaitRegistrationCount.incrementAndGet();
    }

    void onWaitRegistrationReleased() {
        while (true) {
            final int count = inFlightWaitRegistrationCount.get();
            if (count < 1) {
                throw new IllegalStateException("fiber wait registration is not in flight");
            }
            if (inFlightWaitRegistrationCount.compareAndSet(count, count - 1)) {
                return;
            }
        }
    }

    void retireAfterDriverFailure(Fiber fiber) throws Throwable {
        fiber.beginRetirement();
        Throwable failure = null;
        try {
            fiber.prepareDriverFailure();
        } catch (Throwable th) {
            failure = th;
        }
        fiber.markRetired();
        try {
            onRetired(fiber);
        } catch (Throwable th) {
            if (failure == null) {
                failure = th;
            } else if (failure != th) {
                failure.addSuppressed(th);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private void drainFreeList() {
        Fiber fiber;
        while ((fiber = freeList.tryDequeue()) != null) {
            retainedCount.decrementAndGet();
            retire(fiber);
        }
    }

    private void retire(Fiber fiber) {
        fiber.beginRetirement();
        fiber.prepareShutdown();
    }

    private synchronized Fiber tryAcquireSlow() {
        while (true) {
            if (isClosed) {
                throw new IllegalStateException("fiber pool is closed");
            }
            final Fiber retainedFiber = freeList.tryDequeue();
            if (retainedFiber != null) {
                retainedCount.decrementAndGet();
                retainedFiber.reserve();
                return retainedFiber;
            }
            if (liveFibers.size() < maxLive) {
                final Fiber fiber = new Fiber(this, beforeWaitFireForTesting);
                fiber.setRegistryIndex(liveFibers.size());
                liveFibers.add(fiber);
                createdCount.incrementAndGet();
                fiber.reserve();
                return fiber;
            }
            // release() bumps retainedCount before the lock-free put, so a positive count with an
            // empty dequeue is a publication in flight, not saturation
            if (retainedCount.get() == 0) {
                return null;
            }
            Os.pause();
        }
    }

    private synchronized void unregisterFiber(Fiber fiber) {
        final int index = fiber.getRegistryIndex();
        final int lastIndex = liveFibers.size() - 1;
        if (index < 0 || index > lastIndex || liveFibers.getQuick(index) != fiber) {
            throw new IllegalStateException("fiber is not registered");
        }
        final Fiber lastFiber = liveFibers.popLast();
        if (index < lastIndex) {
            liveFibers.setQuick(index, lastFiber);
            lastFiber.setRegistryIndex(index);
        }
        fiber.setRegistryIndex(-1);
    }
}
