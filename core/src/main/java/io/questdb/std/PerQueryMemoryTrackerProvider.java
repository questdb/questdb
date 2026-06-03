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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.SecurityContext;
import io.questdb.mp.ConcurrentPool;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * OSS {@link MemoryTrackerProvider}: hands out one {@link PerQueryMemoryTracker}
 * per workload invocation. Trackers are pooled across invocations so the steady
 * state performs no native alloc/free per workload start. The pool is unbounded;
 * a tracker's native {@code {used, limit}} block is freed only when the provider
 * closes (engine shutdown), never on the per-workload release path. That keeps
 * cross-thread readers of the block -- the {@code query_activity} view's
 * {@code memory_used} / {@code memory_limit} columns - free of use-after-free:
 * a concurrently released tracker's block stays mapped (and may be reused by the
 * next acquisition), so a racing read is at worst stale, never freed underneath
 * the reader. Engine shutdown joins every worker thread before closing the
 * provider, so no reader is live when the pool is finally drained.
 * <p>
 * {@link #acquire} reads the workload's configured byte limit from the
 * {@link CairoConfiguration} on every call rather than caching it at
 * construction, so a dynamic configuration reload that changes a limit applies
 * to every tracker acquired after the reload. An in-flight tracker keeps the
 * limit it was initialized with.
 * <p>
 * The provider's {@link #close()} drains the pool and releases every retained
 * native block. It is invoked from {@code CairoEngine.close()}.
 */
public final class PerQueryMemoryTrackerProvider implements MemoryTrackerProvider {
    private final CairoConfiguration configuration;
    private final ConcurrentPool<PerQueryMemoryTracker> pool = new ConcurrentPool<>();
    private final AtomicInteger pooled = new AtomicInteger();
    private volatile boolean closed;

    public PerQueryMemoryTrackerProvider(@NotNull CairoConfiguration configuration) {
        this.configuration = configuration;
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
    public void clear() {
        drainPool();
    }

    @Override
    public void close() {
        closed = true;
        drainPool();
    }

    @TestOnly
    public int getPooledCount() {
        return pooled.get();
    }

    private void drainPool() {
        PerQueryMemoryTracker tracker;
        while ((tracker = pool.pop()) != null) {
            pooled.decrementAndGet();
            tracker.destroy();
        }
    }

    private long limitFor(MemoryTrackerWorkload workload) {
        return switch (workload) {
            case QUERY -> configuration.getQueryMemoryLimitBytes();
            case MAT_VIEW_REFRESH -> configuration.getMatViewRefreshMemoryLimitBytes();
            case WAL_APPLY -> configuration.getWalApplyMemoryLimitBytes();
        };
    }

    void release(PerQueryMemoryTracker tracker) {
        if (closed) {
            tracker.destroy();
            return;
        }
        pool.push(tracker);
        pooled.incrementAndGet();
    }
}
