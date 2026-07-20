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

package io.questdb.griffin;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.SecurityContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Worker;
import io.questdb.std.CarrierLocal;
import io.questdb.std.Chars;
import io.questdb.std.ConcurrentLongHashMap;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerProvider;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.WeakMutableObjectPool;
import io.questdb.std.datetime.Clock;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A concurrent registry of running sql commands.
 */
public class QueryRegistry {
    private static final Log LOG = LogFactory.getLog(QueryRegistry.class);
    private final Clock clock;
    private final AtomicLong idSeq = new AtomicLong();
    private final ConcurrentLongHashMap<Entry> registry = new ConcurrentLongHashMap<>();
    private final CarrierLocal<WeakMutableObjectPool<Entry>> tlQueryPool;

    private volatile Listener listener;

    public QueryRegistry(CairoConfiguration configuration) {
        this.clock = configuration.getMicrosecondClock();
        tlQueryPool = new CarrierLocal<>(() -> new WeakMutableObjectPool<>(Entry::new, configuration.getQueryRegistryPoolSize()));
    }

    /**
     * Cancels command with given id.
     * Cancellation is not immediate and depends on how often the running command checks circuit breaker.
     * Cancelling commands issued by other users is allowed for admin user only.
     *
     * @param queryId          id of query to cancel, must be non-negative
     * @param executionContext execution context
     * @return true if query was found in registry and cancelled, otherwise false
     * @throws CairoException when user doesn't have permission to cancel the query or query is executed in WAL Apply job.
     */
    public boolean cancel(long queryId, SqlExecutionContext executionContext) throws CairoException {
        SecurityContext securityContext = executionContext.getSecurityContext();
        if (!securityContext.isQueryCancellationAllowed()) {
            throw CairoException.nonCritical().put("Query cancellation is disabled");
        }

        Entry entry = registry.get(queryId);
        if (entry != null) {
            final CharSequence cancellerPrincipal = securityContext.getPrincipal();
            if (!entry.beginCancel(queryId)) {
                LOG.info().$("query not found in registry [id=").$(queryId).I$();
                return false;
            }

            // While the entry is in the CANCELLING state the query owner busy-waits
            // in retire(), so keep the guarded sections minimal: snapshot the fields
            // the permission checks need, then run the checks outside the guard.
            // SecurityContext methods are extension points and may be slow, so the
            // canceller principal is already captured before acquiring the guard.
            final boolean isAdminRequired;
            final boolean isWAL;
            try {
                isAdminRequired = !Chars.equals(entry.principal, cancellerPrincipal);
                isWAL = entry.isWAL;
            } finally {
                entry.activate(queryId);
            }

            if (isAdminRequired) {
                // only a SQL Engine admin can cancel other user's queries
                securityContext.authorizeSqlEngineAdmin();
            }
            if (isWAL) {
                throw CairoException.nonCritical().put("query applied in WAL job can't be cancelled [id=").put(queryId).put(']');
            }

            // Re-acquire the guard for the actual cancellation. This fails if the
            // query finished while the checks ran, in which case we report "not found".
            if (entry.beginCancel(queryId)) {
                try {
                    entry.cancel();
                    entry.changedAtNs = clock.getTicks();
                    entry.state = Entry.State.CANCELLED;
                    // Log inside the guard: it reads entry.query, which a concurrent
                    // register() would overwrite once the entry is released and recycled.
                    // The chars are copied into the async log buffer synchronously, so the
                    // owner's retire() busy-wait stays short.
                    LOG.info().$("cancelling query [user=").$(cancellerPrincipal).$(",queryId=").$(queryId).$(",sql=").$(entry.query).I$();
                    return true;
                } finally {
                    entry.activate(queryId);
                }
            }
        }

        LOG.info().$("query not found in registry [id=").$(queryId).I$();
        return false;
    }

    /**
     * Returns the registry entry for the given query id, or null when not found.
     * Reads of the returned entry are best-effort: the entry may be concurrently
     * retired and recycled for another query while the caller reads its fields.
     * Callers that need a stable view should copy the fields they need and then
     * validate the entry lifecycle before using the copy.
     *
     * @param id id of the query to look up
     * @return entry for the given query id, or null
     */
    public Entry getEntry(long id) {
        return registry.get(id);
    }

    /**
     * Copy ids of currently running sql commands to target list.
     * List is cleared before adding ids.
     *
     * @param target - list to copy ids to
     */
    public void getEntryIds(@NotNull LongList target) {
        target.clear();

        ConcurrentLongHashMap.KeyIterator<Entry> iterator = registry.keySet().iterator();

        while (iterator.hasNext()) {
            target.add(iterator.next());
        }
    }

    /**
     * Add given command to registry.
     *
     * @param query            - query text
     * @param executionContext - execution context
     * @return non-negative id assigned to given query. It may be used to look query up in registry.
     */
    public long register(CharSequence query, SqlExecutionContext executionContext) {
        final long queryId = idSeq.getAndIncrement();
        final Entry e = tlQueryPool.get().pop();
        // Just in case something messed the cached Entry
        // while it was in the pool, like late query cancel()
        // clean the object before using.
        e.clear();

        e.registeredAtNs = clock.getTicks();
        e.changedAtNs = e.registeredAtNs;
        e.state = Entry.State.ACTIVE;

        if (executionContext.containsSecret()) {
            e.query.put("<SECRET>");
        } else {
            // we shouldn't copy text in case of sensitive queries
            e.query.put(query);
        }

        final Thread thread = Thread.currentThread();
        if (thread instanceof Worker worker) {
            e.workerId = worker.getWorkerId();
            e.poolName = worker.getPoolName();
        }
        e.isWAL = executionContext.isWalApplication();
        e.principal = executionContext.getSecurityContext().getPrincipal();

        // Acquire a per-workload memory tracker for this workload, or inherit an
        // outer one. Inheritance is allowed only when the bound tracker belongs to
        // a non-QUERY background workload: a mat-view refresh or WAL apply job
        // binds its own tracker on a dedicated execution context before running
        // inner SQL, and that inner SQL must charge the background workload's
        // budget. Such nesting is strictly LIFO and single-threaded.
        //
        // A QUERY tracker already on the context is NOT inherited. Concurrent PG
        // named portals share one SqlExecutionContext and are siblings, not
        // nested: a suspended portal leaves its tracker bound, and inheriting it
        // would conflate the two portals' accounting and -- once the first
        // portal's cursor closes and recycles that tracker to the pool -- corrupt
        // an unrelated query's counter. Each top-level QUERY gets its own tracker.
        //
        // Acquire a tracker even when the QUERY limit is 0 (unlimited) so that
        // accounting stays on and query_activity.memory_used reports live usage
        // for every query. A null tracker when unlimited would save one atomic
        // on the tracked allocation path, at the cost of that observability.
        final MemoryTracker outerTracker = executionContext.getMemoryTracker();
        if (outerTracker == null || outerTracker.getWorkload() == MemoryTrackerWorkload.QUERY) {
            final MemoryTrackerProvider provider = executionContext.getCairoEngine().getMemoryTrackerProvider();
            final MemoryTracker tracker = provider.acquire(
                    executionContext.getSecurityContext(),
                    queryId,
                    MemoryTrackerWorkload.QUERY
            );
            executionContext.setMemoryTracker(tracker);
            e.memoryTracker = tracker;
        }

        try {
            // the volatile lifecycle store also publishes the plain field writes
            // above (including the memory tracker) to threads that look the entry
            // up via the registry
            e.activate(queryId);
            registry.put(queryId, e);

            Listener listener = this.listener;
            if (listener != null) {
                listener.onRegister(query, queryId, executionContext);
            }
        } catch (Throwable th) {
            // registry.put() can OOM mid-rehash. register() runs outside the
            // caller's try/finally, so unregister() never fires here -- release the
            // just-acquired tracker (else its native blocks leak during the very OOM
            // the feature bounds), drop the partial entry, and retire the Entry
            // before recycling it.
            registry.remove(queryId);
            if (e.memoryTracker != null) {
                // Restore the prior tracker only if the slot is still ours; a
                // concurrently-suspended sibling portal may have rebound it.
                if (executionContext.getMemoryTracker() == e.memoryTracker) {
                    executionContext.setMemoryTracker(outerTracker);
                }
                e.memoryTracker.close();
                e.memoryTracker = null;
            }
            if (e.retire(queryId)) {
                tlQueryPool.get().push(e);
            } else {
                LOG.error().$("query lifecycle mismatch on register rollback [id=").$(queryId).I$();
            }
            throw th;
        }

        executionContext.setCancelledFlag(e.cancelled);
        return queryId;
    }

    @TestOnly
    public void setListener(Listener listener) {
        this.listener = listener;
    }

    /**
     * Remove query with given id from registry.
     *
     * @param queryId          - id of query to remove
     * @param executionContext - execution context
     */
    public void unregister(long queryId, SqlExecutionContext executionContext) {
        if (queryId < 0) {
            // likely because query was already unregistered
            return;
        }

        // Remove shared AtomicBoolean from execution context CircuitBreaker
        // before returning Entry to the pool
        executionContext.setCancelledFlag(null);
        final Entry e = registry.remove(queryId);
        if (e != null) {
            // Release the per-workload memory tracker if this register() call
            // acquired it. A null e.memoryTracker means the registration was
            // nested under an outer workload that owns the tracker; in that
            // case we must not touch the context's tracker reference.
            if (e.memoryTracker != null) {
                // Clear the context slot only if it still points at our tracker. A
                // concurrently-suspended sibling portal (sharing this context) may
                // have rebound the slot to its own tracker after us; nulling it then
                // would strand that sibling. Out-of-order portal close makes this
                // conditional necessary -- see the inheritance note in register().
                if (executionContext.getMemoryTracker() == e.memoryTracker) {
                    executionContext.setMemoryTracker(null);
                }
                e.memoryTracker.close();
                e.memoryTracker = null;
            }
            // Retire the entry to guard pooled reuse: this waits out an in-flight
            // canceller and only recycles the entry once it owns the lifecycle word.
            if (e.retire(queryId)) {
                tlQueryPool.get().push(e);
            } else {
                LOG.error().$("query lifecycle mismatch [id=").$(queryId).I$();
            }
        } else {
            // this might happen if query was cancelled
            LOG.error().$("query to unregister not found [id=").$(queryId).I$();
        }
    }

    public interface Listener {
        void onRegister(CharSequence query, long queryId, SqlExecutionContext executionContext);
    }

    /**
     * Pooled, reusable descriptor of a registered query. The volatile lifecycle
     * word packs the owning query id and a state:
     * <pre>
     * IDLE -&gt; ACTIVE -&gt; (CANCELLING -&gt; ACTIVE)* -&gt; RETIRED -&gt; IDLE
     * </pre>
     * register() activates the entry for a query id before publishing it in the
     * registry. cancel() briefly holds CANCELLING while it mutates the entry,
     * then releases it back to ACTIVE. unregister() moves ACTIVE to RETIRED,
     * waiting out an in-flight canceller, and pushes the entry to the pool,
     * where clear() resets it to IDLE. Because the query id is part of the CAS
     * word, a stale canceller holding a recycled entry cannot transition it.
     * <p>
     * The byte state field is separate from the lifecycle word despite the
     * overlapping vocabulary: it carries the informational status
     * (idle/active/cancelled) that query_activity() renders via State.getText(),
     * and it never synchronizes anything. cancel() writes it while holding the
     * CANCELLING guard, so query_activity() snapshots observe it consistently
     * with the rest of the entry.
     */
    public static class Entry implements Mutable {
        private static final long LIFECYCLE_IDLE = -1;
        private static final long LIFECYCLE_OFFSET = Unsafe.getFieldOffset(Entry.class, "lifecycle");
        private static final long LIFECYCLE_STATE_ACTIVE = 0;
        private static final long LIFECYCLE_STATE_CANCELLING = 1;
        private static final long LIFECYCLE_STATE_RETIRED = 2;

        private final AtomicBoolean cancelled = new AtomicBoolean();
        private final StringSink query = new StringSink();
        private long changedAtNs;
        private boolean isWAL;
        // Packs query id and state into one CAS word to guard pooled Entry reuse.
        // The id occupies bits 2-63, so the usable id space is 2^62; idSeq starts
        // at 0 on every server start and cannot realistically reach that.
        private volatile long lifecycle = LIFECYCLE_IDLE;
        // Non-null only when this register() call acquired the tracker. Nested
        // registrations that inherit an outer tracker leave this null so that
        // the matching unregister() does not touch the context's tracker.
        //
        // query_activity reads this cross-thread (unsynchronized) via
        // getMemoryUsed / getMemoryLimit. Like the other Entry columns the read
        // is best-effort: under register/unregister churn the Entry can be
        // recycled to another query between the reader resolving it and reading
        // the column, so a row may briefly report a different query's bytes. The
        // read is never unsafe: the tracker's native block outlives release
        // (freed only when the provider closes), so a stale read returns a
        // valid-but-wrong number, never a fault.
        private MemoryTracker memoryTracker;
        private CharSequence poolName;
        private CharSequence principal;
        private long registeredAtNs;
        private byte state;
        private long workerId;

        public static boolean isActiveLifecycle(long queryId, long lifecycle) {
            return lifecycle == lifecycle(queryId, LIFECYCLE_STATE_ACTIVE);
        }

        private static long lifecycle(long queryId, long state) {
            return (queryId << 2) | state;
        }

        public void cancel() {
            cancelled.set(true);
        }

        @Override
        public void clear() {
            query.clear();
            registeredAtNs = 0;
            changedAtNs = 0;
            cancelled.set(false);
            memoryTracker = null;
            poolName = null;
            workerId = -1;
            principal = null;
            state = State.IDLE;
            isWAL = false;
            lifecycle = LIFECYCLE_IDLE;
        }

        public AtomicBoolean getCancelled() {
            return cancelled;
        }

        public long getChangedAtNs() {
            return changedAtNs;
        }

        public long getLifecycle() {
            return lifecycle;
        }

        // For query_activity: the per-query limit, or NULL when no tracker is
        // bound (nested registration) or the limit is 0 (unlimited). Best-effort
        // cross-thread read; see the memoryTracker field.
        public long getMemoryLimit() {
            final MemoryTracker t = memoryTracker;
            final long limit = t != null ? t.getLimit() : 0;
            return limit != 0 ? limit : Numbers.LONG_NULL;
        }

        // For query_activity: bytes charged to the per-query tracker, or NULL
        // when no tracker is bound (nested registration). Best-effort
        // cross-thread read; see the memoryTracker field.
        public long getMemoryUsed() {
            final MemoryTracker t = memoryTracker;
            return t != null ? t.getUsed() : Numbers.LONG_NULL;
        }

        public CharSequence getPoolName() {
            return poolName;
        }

        public CharSequence getPrincipal() {
            return principal;
        }

        public StringSink getQuery() {
            return query;
        }

        public long getRegisteredAtNs() {
            return registeredAtNs;
        }

        public byte getState() {
            return state;
        }

        public long getWorkerId() {
            return workerId;
        }

        public boolean isWAL() {
            return isWAL;
        }

        /**
         * Moves the entry into the ACTIVE lifecycle state for the given query id.
         * register() calls this to publish the entry before inserting it into the
         * registry; cancel() calls it to release the CANCELLING state.
         */
        private void activate(long queryId) {
            lifecycle = lifecycle(queryId, LIFECYCLE_STATE_ACTIVE);
        }

        private boolean beginCancel(long queryId) {
            return transitionFromActive(queryId, LIFECYCLE_STATE_CANCELLING);
        }

        private boolean retire(long queryId) {
            return transitionFromActive(queryId, LIFECYCLE_STATE_RETIRED);
        }

        private boolean transitionFromActive(long queryId, long targetState) {
            final long active = lifecycle(queryId, LIFECYCLE_STATE_ACTIVE);
            final long cancelling = lifecycle(queryId, LIFECYCLE_STATE_CANCELLING);
            final long target = lifecycle(queryId, targetState);
            while (true) {
                final long current = lifecycle;
                if (current == active) {
                    if (Unsafe.cas(this, LIFECYCLE_OFFSET, active, target)) {
                        return true;
                    }
                    continue;
                }
                if (current == cancelling) {
                    // a canceller owns the entry, wait for it to finish
                    Os.pause();
                    continue;
                }
                return false;
            }
        }

        public static class State {
            public static final byte ACTIVE = 2;
            public static final byte CANCELLED = (byte) (ACTIVE + 1);
            public static final byte IDLE = 1;

            private State() {
            }

            public static String getText(byte state) {
                return switch (state) {
                    case IDLE -> "idle";
                    case ACTIVE -> "active";
                    case CANCELLED -> "cancelled";
                    default -> "unknown state";
                };
            }
        }
    }
}
