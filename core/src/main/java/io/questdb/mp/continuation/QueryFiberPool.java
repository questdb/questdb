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

package io.questdb.mp.continuation;

import io.questdb.mp.ConcurrentQueue;
import io.questdb.mp.Queue;
import io.questdb.mp.ValueHolder;
import io.questdb.std.CarrierLocal;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.QuietCloseable;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Bounded MPMC pool of {@link QueryFiber}s, shared by all workers of a worker pool:
 * a fiber frozen on one worker resumes on a peer, so the free-list cannot be
 * per-worker. A fiber is held only while a task is RUNNING on it or frozen in a wait
 * inside it; tasks parked on their sink (socket write, credit) hold no fiber. The
 * steady-state pool size therefore converges to the concurrent-run + concurrent-wait
 * high-water mark, which is small and bounded.
 *
 * <p>{@link #acquire()} never blocks admission: past the pooled high-water it simply
 * creates a fresh fiber -- a spike degrades to the worker loop's historical
 * fresh-continuation-per-park allocation instead of queueing or failing.
 * {@link #release(QueryFiber)} pools up to {@code maxPooled} live fibers and retires
 * the surplus by driving them to completion, releasing their native stack chunks
 * deterministically. The {@link #getCreatedCount()} / {@link #getRetiredCount()} /
 * {@link #getPooledCount()} counters make spikes observable to callers -- there is no
 * silent cap.
 *
 * <p>{@link #close()} marks every fiber ever created for shutdown and retires the
 * free ones. Fibers frozen in a wait at close time are owned by their waiters; the
 * engine's waiter-shutdown path resumes them, their bodies observe the flag on thaw
 * and complete, and whoever runs them last releases them back here for retirement
 * accounting.
 */
public final class QueryFiberPool implements QuietCloseable {
    // All fibers ever created, so close() can flag shutdown on fibers that are
    // currently out of the free-list (mounted or frozen in a waiter). Guarded by
    // this; touched only on the cold create/close paths.
    private final ObjList<QueryFiber> allFibers = new ObjList<>();
    private final AtomicInteger createdCount = new AtomicInteger();
    private final Queue<FiberHolder> freeList = ConcurrentQueue.createConcurrentQueue(FiberHolder::new);
    private final int maxPooled;
    private final AtomicInteger pooledCount = new AtomicInteger();
    private final ContinuationSink resumeSink;
    private final AtomicInteger retiredCount = new AtomicInteger();
    private final CarrierLocal<FiberHolder> scratch = CarrierLocal.withInitial(FiberHolder::new);
    private volatile boolean isClosed;

    public QueryFiberPool(int maxPooled, ContinuationSink resumeSink) {
        assert maxPooled > 0;
        this.maxPooled = maxPooled;
        this.resumeSink = resumeSink;
    }

    /**
     * Pops a free fiber, or creates a fresh one when the free-list is empty. Never
     * blocks and never fails admission; creation past the high-water is the
     * intentional spike-degradation path.
     */
    public QueryFiber acquire() {
        final FiberHolder h = scratch.get();
        if (freeList.tryDequeue(h)) {
            pooledCount.decrementAndGet();
            final QueryFiber fiber = h.fiber;
            h.fiber = null;
            return fiber;
        }
        return createFiber();
    }

    @Override
    public void close() {
        isClosed = true;
        synchronized (this) {
            for (int i = 0, n = allFibers.size(); i < n; i++) {
                allFibers.getQuick(i).shutdown();
            }
        }
        drainFreeList();
    }

    /**
     * Approximate count of fibers currently out of the pool -- mounted on a carrier
     * or frozen in a wait. Launch throttles use it as a soft bound; the underlying
     * counters are updated independently, so transient off-by-one readings under
     * concurrency are expected and harmless.
     */
    public int getBusyCount() {
        return createdCount.get() - pooledCount.get() - retiredCount.get();
    }

    public int getCreatedCount() {
        return createdCount.get();
    }

    public int getMaxPooled() {
        return maxPooled;
    }

    public int getPooledCount() {
        return pooledCount.get();
    }

    public int getRetiredCount() {
        return retiredCount.get();
    }

    /**
     * Transition-mode launch: claims the task's gate (IDLE -&gt; ENQUEUED -&gt;
     * RUNNING), assigns it to a fiber and pushes the fiber onto the fibers' resume
     * sink. When the sink is a worker pool's {@code ContinuationQueue}, a worker
     * dequeues the fiber and mounts it exactly like any parked continuation --
     * launching needs no scheduler drain and no changes to the launch site's
     * threading. Returns {@code false} when the task is already scheduled, running
     * or terminal.
     */
    public boolean launch(QueryTask task) {
        if (!task.tryEnqueue() || !task.tryClaimRun()) {
            return false;
        }
        final QueryFiber fiber = acquire();
        fiber.assign(task);
        fiber.scheduleResume();
        return true;
    }

    /**
     * Returns a fiber after its {@code run()} returned with a free-yield (or with
     * the body completed). Pools it up to the high-water; retires the surplus, a
     * completed body, or anything arriving after {@link #close()}.
     */
    public void release(QueryFiber fiber) {
        if (fiber.isDone()) {
            retiredCount.incrementAndGet();
            return;
        }
        if (!isClosed) {
            if (pooledCount.incrementAndGet() <= maxPooled) {
                final FiberHolder h = scratch.get();
                h.fiber = fiber;
                freeList.enqueue(h);
                h.fiber = null;
                if (isClosed) {
                    // close() may have swept the free-list before our enqueue
                    // landed; sweep again so no shutdown-flagged fiber lingers
                    // pooled. Idempotent against a concurrent close's own drain.
                    drainFreeList();
                }
                return;
            }
            pooledCount.decrementAndGet();
        }
        retire(fiber);
    }

    private QueryFiber createFiber() {
        final QueryFiber fiber = new QueryFiber(this, resumeSink);
        createdCount.incrementAndGet();
        synchronized (this) {
            allFibers.add(fiber);
        }
        return fiber;
    }

    private void drainFreeList() {
        final FiberHolder h = scratch.get();
        while (freeList.tryDequeue(h)) {
            pooledCount.decrementAndGet();
            final QueryFiber fiber = h.fiber;
            h.fiber = null;
            retire(fiber);
        }
    }

    private void retire(QueryFiber fiber) {
        fiber.shutdown();
        // The fiber sits at its free-yield (or was never run); one mount drives the
        // body to observe the flag and return. Spin through the benign mount race
        // if a registering carrier is still transiently holding it.
        while (!fiber.isDone()) {
            try {
                fiber.run();
            } catch (IllegalStateException e) {
                Os.pause();
            }
        }
        retiredCount.incrementAndGet();
    }

    private static final class FiberHolder implements ValueHolder<FiberHolder> {
        QueryFiber fiber;

        @Override
        public void clear() {
            fiber = null;
        }

        @Override
        public void copyTo(FiberHolder dest) {
            dest.fiber = fiber;
        }
    }
}
