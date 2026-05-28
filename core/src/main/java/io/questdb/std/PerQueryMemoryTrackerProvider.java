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

import io.questdb.cairo.SecurityContext;
import io.questdb.mp.ConcurrentPool;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * OSS {@link MemoryTrackerProvider}: hands out one
 * {@link PerQueryMemoryTracker} per workload invocation. Trackers are pooled
 * across invocations so the steady state performs no native alloc/free per
 * workload start. The pool is unbounded unless {@code poolCapacity > 0}, in
 * which case trackers returned past the cap are destroyed instead of pooled.
 * <p>
 * The provider's {@link #close()} drains the pool and releases every retained
 * native block. It is invoked from {@code CairoEngine.close()}.
 */
public final class PerQueryMemoryTrackerProvider implements MemoryTrackerProvider {

    private final AtomicInteger pooled = new AtomicInteger();
    private final ConcurrentPool<PerQueryMemoryTracker> pool = new ConcurrentPool<>();
    private final int poolCapacity;
    private final long limitMatViewRefresh;
    private final long limitQuery;
    private final long limitWalApply;
    private volatile boolean closed;

    /**
     * @param limitQuery           byte limit for {@link MemoryTrackerWorkload#QUERY};
     *                             {@code 0} means unlimited.
     * @param limitMatViewRefresh  byte limit for
     *                             {@link MemoryTrackerWorkload#MAT_VIEW_REFRESH};
     *                             {@code 0} means unlimited.
     * @param limitWalApply        byte limit for {@link MemoryTrackerWorkload#WAL_APPLY};
     *                             {@code 0} means unlimited.
     * @param poolCapacity         optional cap on retained pooled trackers;
     *                             {@code 0} means unbounded.
     */
    public PerQueryMemoryTrackerProvider(long limitQuery, long limitMatViewRefresh, long limitWalApply, int poolCapacity) {
        assert limitQuery >= 0;
        assert limitMatViewRefresh >= 0;
        assert limitWalApply >= 0;
        assert poolCapacity >= 0;
        this.limitQuery = limitQuery;
        this.limitMatViewRefresh = limitMatViewRefresh;
        this.limitWalApply = limitWalApply;
        this.poolCapacity = poolCapacity;
    }

    @Override
    public @NotNull MemoryTracker acquire(@NotNull SecurityContext securityContext, long queryId, @NotNull MemoryTrackerWorkload workload) {
        assert !closed : "acquire() after close()";
        PerQueryMemoryTracker tracker = pool.pop();
        if (tracker != null) {
            pooled.decrementAndGet();
        } else {
            tracker = new PerQueryMemoryTracker(this);
        }
        tracker.init(queryId, workload, limitFor(workload));
        return tracker;
    }

    @Override
    public void close() {
        closed = true;
        PerQueryMemoryTracker tracker;
        while ((tracker = pool.pop()) != null) {
            pooled.decrementAndGet();
            tracker.destroy();
        }
    }

    @TestOnly
    public int getPooledCount() {
        return pooled.get();
    }

    void release(PerQueryMemoryTracker tracker) {
        if (closed) {
            tracker.destroy();
            return;
        }
        if (poolCapacity > 0 && pooled.get() >= poolCapacity) {
            tracker.destroy();
            return;
        }
        pool.push(tracker);
        pooled.incrementAndGet();
    }

    private long limitFor(MemoryTrackerWorkload workload) {
        switch (workload) {
            case QUERY:
                return limitQuery;
            case MAT_VIEW_REFRESH:
                return limitMatViewRefresh;
            case WAL_APPLY:
                return limitWalApply;
            default:
                throw new AssertionError("unknown workload: " + workload);
        }
    }
}
