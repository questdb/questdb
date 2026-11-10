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
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.ConcurrentPool;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

/**
 * OSS {@link MemoryTrackerProvider}: hands out one {@link PerQueryMemoryTracker}
 * per workload invocation. Trackers are pooled across invocations so the steady
 * state performs no native alloc/free per workload start. The pool is unbounded;
 * a tracker's native {@code {used, limit}} block is freed only when the provider
 * closes (engine shutdown), never on the per-workload release path. That keeps
 * cross-thread readers of the block (the {@code query_activity} view's
 * {@code memory_used} / {@code memory_limit} columns) free of use-after-free:
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
public class PerQueryMemoryTrackerProvider implements MemoryTrackerProvider {
    private static final Log LOG = LogFactory.getLog(PerQueryMemoryTrackerProvider.class);
    private final CairoConfiguration configuration;
    private final ConcurrentPool<PerQueryMemoryTracker> pool = new ConcurrentPool<>();
    private volatile boolean closed;

    public PerQueryMemoryTrackerProvider(@NotNull CairoConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public @NotNull MemoryTracker acquire(@NotNull SecurityContext securityContext, long queryId, @NotNull MemoryTrackerWorkload workload) {
        assert !closed : "acquire() after close()";
        // A subclass may consult the security context. Resolve before taking ownership of native memory.
        final long limit = limitFor(securityContext, workload);
        PerQueryMemoryTracker tracker;
        do {
            tracker = pool.pop();
        } while (tracker != null && isDirty(tracker, "acquire"));
        if (tracker == null) {
            tracker = new PerQueryMemoryTracker(this);
        }
        tracker.init(queryId, workload, limit);
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
        return pool.count();
    }

    // A tracker that still has native memory charged to it must not be reused, or the next workload
    // would inherit the charge. It must not be freed either: the allocations still point to its
    // native block and Rust allocators, and their frees would write through freed memory. Such a
    // tracker is dropped, leaking its native block, and the failure is logged rather than thrown,
    // so that the workload which runs next is not failed for a leak it did not cause.
    private static boolean isDirty(PerQueryMemoryTracker tracker, String stage) {
        tracker.reconcileCovered();
        final long used = tracker.getUsed();
        if (used == 0) {
            return false;
        }
        final long queryId = tracker.getQueryId();
        final MemoryTrackerWorkload workload = tracker.getWorkload();
        LOG.critical().$("tracker dirty on ").$(stage)
                .$(", leaking its native block to avoid a use-after-free [used=").$(used)
                .$(", queryId=").$(queryId)
                .$(", workload=").$(workload)
                .I$();
        return true;
    }

    private void drainPool() {
        PerQueryMemoryTracker tracker;
        while ((tracker = pool.pop()) != null) {
            tracker.destroy();
        }
    }

    /**
     * Resolves a limit for a new workload invocation. Existing trackers retain their acquired limit.
     */
    protected long limitFor(@NotNull SecurityContext securityContext, @NotNull MemoryTrackerWorkload workload) {
        return switch (workload) {
            case QUERY -> configuration.getQueryMemoryLimitBytes();
            case MAT_VIEW_REFRESH -> configuration.getMatViewRefreshMemoryLimitBytes();
            case WAL_APPLY -> configuration.getWalApplyMemoryLimitBytes();
            case LIVE_VIEW_REFRESH -> configuration.getLiveViewRefreshMemoryLimitBytes();
        };
    }

    void release(PerQueryMemoryTracker tracker) {
        releaseAfterClosedCheck(tracker, closed);
    }

    void releaseAfterClosedCheck(PerQueryMemoryTracker tracker, boolean isClosedAtCheck) {
        if (isDirty(tracker, "release")) {
            return;
        }
        if (isClosedAtCheck) {
            tracker.destroy();
        } else {
            pool.push(tracker);
            // close() may have drained after the initial closed read but before this push. In that
            // interleaving the releasing thread owns the final drain; otherwise close() observes the push.
            if (closed) {
                drainPool();
            }
        }
    }
}
