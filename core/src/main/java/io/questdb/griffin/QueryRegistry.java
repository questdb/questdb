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
import io.questdb.mp.CarrierIdentity;
import io.questdb.mp.ConcurrentPool;
import io.questdb.mp.Worker;
import io.questdb.mp.continuation.CancellationBinding;
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberCancellationSignal;
import io.questdb.mp.continuation.FiberDispatchContext;
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
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.Clock;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A concurrent registry of running sql commands.
 */
public class QueryRegistry {
    private static final String DEFERRED_QUERY_TEXT = "<PENDING>";
    private static final Log LOG = LogFactory.getLog(QueryRegistry.class);
    private static final String SECRET_QUERY_TEXT = "<SECRET>";
    private final Clock clock;
    private final AtomicLong idSeq = new AtomicLong();
    private final CarrierLocal<EntryHolder> localQueryPool = new CarrierLocal<>(EntryHolder::new);
    private final ConcurrentPool<Entry> queryPool = new ConcurrentPool<>();
    private final int queryPoolCapacity;
    private final ConcurrentLongHashMap<Entry> registry = new ConcurrentLongHashMap<>();

    private volatile Listener listener;

    public QueryRegistry(CairoConfiguration configuration) {
        this.clock = configuration.getMicrosecondClock();
        this.queryPoolCapacity = configuration.getQueryRegistryPoolSize();
        for (int i = 0; i < queryPoolCapacity; i++) {
            queryPool.push(new Entry());
        }
    }

    /**
     * Cancels command with given id.
     * Running commands observe cancellation at their next circuit-breaker check. A fiber
     * registered on the command's cancellation signal wakes immediately.
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
                    // Log inside the guard: it reads entry.queryText, which a concurrent
                    // register() would overwrite once the entry is released and recycled.
                    // The chars are copied into the async log buffer synchronously, so the
                    // owner's retire() busy-wait stays short.
                    LOG.info().$("cancelling query [user=").$(cancellerPrincipal).$(",queryId=").$(queryId).$(",sql=").$(entry.queryText).I$();
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

    @TestOnly
    public int getPoolSize() {
        return queryPool.count();
    }

    /**
     * Returns the Resource Group ID captured by the active execution lease, or SQL NULL when the
     * owner is absent or the engine did not attach a Resource Group lease. The double lifecycle
     * check prevents a pooled entry recycled concurrently for another query from leaking its
     * identity into this query.
     */
    public long getResourceGroupId(long queryId) {
        final Entry entry = registry.get(queryId);
        if (entry == null || !Entry.isActiveLifecycle(queryId, entry.lifecycle)) {
            return Numbers.LONG_NULL;
        }
        final QuietCloseable lease = entry.executionLease;
        final long groupId = lease instanceof SqlExecutionLease sqlExecutionLease
                ? sqlExecutionLease.getResourceGroupId()
                : Numbers.LONG_NULL;
        Unsafe.loadFence();
        return Entry.isActiveLifecycle(queryId, entry.lifecycle) ? groupId : Numbers.LONG_NULL;
    }

    /**
     * Returns the immutable Resource Group name captured by the active execution lease, or null
     * when the owner is absent or the engine did not attach a Resource Group lease.
     */
    public @Nullable CharSequence getResourceGroupName(long queryId) {
        final Entry entry = registry.get(queryId);
        if (entry == null || !Entry.isActiveLifecycle(queryId, entry.lifecycle)) {
            return null;
        }
        final QuietCloseable lease = entry.executionLease;
        final CharSequence groupName = lease instanceof SqlExecutionLease sqlExecutionLease
                ? sqlExecutionLease.getResourceGroupName()
                : null;
        Unsafe.loadFence();
        return Entry.isActiveLifecycle(queryId, entry.lifecycle) ? groupName : null;
    }

    /**
     * Mounts an existing protocol owner for another executable segment.
     */
    public void mountOwner(long queryId, SqlExecutionContext executionContext) {
        final Entry entry = getOwnerEntry(queryId, executionContext);
        final QuietCloseable executionLease = entry.executionLease;
        if (executionLease instanceof SqlExecutionLease lease) {
            lease.mount();
        }
        final MemoryTracker memoryTracker = entry.memoryTracker;
        if (memoryTracker != null) {
            executionContext.setMemoryTracker(memoryTracker);
        }
    }

    /**
     * Publishes a protocol owner's query text after secret classification. The first publication
     * wins; retries therefore never mutate a StringSink already visible to query_activity().
     */
    public void publishOwnerQuery(long queryId, CharSequence query, boolean containsSecret) {
        if (queryId < 0) {
            return;
        }
        final Entry entry = registry.get(queryId);
        if (entry == null || !entry.publishQuery(queryId, query, containsSecret)) {
            throw new IllegalStateException("query registry owner is no longer active [id=" + queryId + ']');
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
        final long contextOwnerId = executionContext.getQueryRegistryOwnerId();
        if (contextOwnerId > -1) {
            return retainOwner(contextOwnerId, executionContext);
        }
        final FiberDispatchContext dispatchContext = Fiber.captureDispatchContext();
        if (dispatchContext != null) {
            final long ownerId = dispatchContext.getQueryRegistryOwnerId();
            if (ownerId > -1) {
                final long retainedOwnerId = tryRetainDispatchedOwner(ownerId, executionContext);
                if (retainedOwnerId > -1) {
                    return retainedOwnerId;
                }
            }
        }
        return register0(query, executionContext, false);
    }

    /**
     * Registers a protocol-owned SQL execution before compilation. Its text remains hidden until
     * {@link #publishOwnerQuery(long, CharSequence, boolean)} is called after the compiler has
     * classified secret-bearing statements.
     */
    public long registerOwner(CharSequence query, SqlExecutionContext executionContext) {
        return register0(query, executionContext, true);
    }

    @TestOnly
    public void setListener(Listener listener) {
        this.listener = listener;
    }

    /**
     * Unmounts a protocol owner between executable segments.
     */
    public void unmountOwner(long queryId, SqlExecutionContext executionContext) {
        final Entry entry = getOwnerEntry(queryId, executionContext);
        Throwable cleanupFailure = null;
        try {
            MemoryTracker.detachResourceMemoryCurrentThread();
        } catch (Throwable th) {
            cleanupFailure = th;
        }
        final QuietCloseable executionLease = entry.executionLease;
        if (executionLease instanceof SqlExecutionLease lease) {
            try {
                lease.unmount();
            } catch (Throwable th) {
                if (cleanupFailure == null) {
                    cleanupFailure = th;
                } else if (cleanupFailure != th) {
                    cleanupFailure.addSuppressed(th);
                }
            }
        }
        final MemoryTracker memoryTracker = entry.memoryTracker;
        try {
            if (memoryTracker != null && executionContext.getMemoryTracker() == memoryTracker) {
                executionContext.setMemoryTracker(null);
            }
        } catch (Throwable th) {
            if (cleanupFailure == null) {
                cleanupFailure = th;
            } else if (cleanupFailure != th) {
                cleanupFailure.addSuppressed(th);
            }
        }
        CairoException.rethrowCleanupFailure(cleanupFailure);
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

        final Entry e = registry.get(queryId);
        if (e != null) {
            final int releaseResult = e.release(queryId, executionContext);
            if (releaseResult == Entry.RELEASE_RETAINED) {
                return;
            }
            if (releaseResult != Entry.RELEASE_FINAL) {
                LOG.error().$("query lifecycle mismatch [id=").$(queryId).I$();
                return;
            }
            Throwable cleanupFailure = null;
            boolean detached = false;
            try {
                detached = registry.remove(queryId, e);
                if (!detached) {
                    throw new IllegalStateException("query registry could not detach retired entry [id=" + queryId + ']');
                }
            } catch (Throwable th) {
                cleanupFailure = th;
            }
            try {
                clearStaleSignalBinding(e.previousCancelledBinding);
            } catch (Throwable th) {
                cleanupFailure = appendCleanupFailure(cleanupFailure, th);
            }
            try {
                clearStaleSignalBinding(e.previousSimpleCancelledBinding);
            } catch (Throwable th) {
                cleanupFailure = appendCleanupFailure(cleanupFailure, th);
            }
            try {
                executionContext.restoreCancelledFlag(
                        e.cancelled,
                        e.previousCancelledBinding,
                        e.previousSimpleCancelledBinding
                );
            } catch (Throwable th) {
                cleanupFailure = appendCleanupFailure(cleanupFailure, th);
            }
            // Release the per-workload memory tracker if this register() call
            // acquired it. A null e.memoryTracker means the registration was
            // nested under an outer workload that owns the tracker; in that
            // case we must not touch the context's tracker reference.
            final MemoryTracker memoryTracker = e.memoryTracker;
            if (memoryTracker != null) {
                // Clear the context slot only if it still points at our tracker. A
                // concurrently-suspended sibling portal (sharing this context) may
                // have rebound the slot to its own tracker after us; nulling it then
                // would strand that sibling. Out-of-order portal close makes this
                // conditional necessary -- see the inheritance note in register().
                try {
                    if (executionContext.getMemoryTracker() == memoryTracker) {
                        executionContext.setMemoryTracker(null);
                    }
                } catch (Throwable th) {
                    cleanupFailure = appendCleanupFailure(cleanupFailure, th);
                }
                try {
                    memoryTracker.close();
                } catch (Throwable th) {
                    cleanupFailure = appendCleanupFailure(cleanupFailure, th);
                } finally {
                    e.memoryTracker = null;
                }
            }
            final QuietCloseable executionLease = e.executionLease;
            if (executionLease != null) {
                try {
                    executionLease.close();
                } catch (Throwable th) {
                    cleanupFailure = appendCleanupFailure(cleanupFailure, th);
                } finally {
                    e.executionLease = null;
                }
            }
            if (detached) {
                try {
                    recycle(e);
                } catch (Throwable th) {
                    cleanupFailure = appendCleanupFailure(cleanupFailure, th);
                }
            }
            CairoException.rethrowCleanupFailure(cleanupFailure);
        } else {
            // this might happen if query was cancelled
            LOG.error().$("query to unregister not found [id=").$(queryId).I$();
        }
    }

    private static Throwable appendCleanupFailure(@Nullable Throwable primary, Throwable failure) {
        if (primary == null) {
            return failure;
        }
        suppressCleanupFailure(primary, failure);
        return primary;
    }

    private static void clearStaleSignalBinding(CancellationBinding binding) {
        final AtomicBoolean flag = binding.getFlag();
        if (flag instanceof FiberCancellationSignal signal
                && signal.getGeneration() != binding.getGeneration(flag)) {
            binding.clear();
        }
    }

    private static void suppressCleanupFailure(Throwable primary, Throwable failure) {
        if (primary != failure) {
            try {
                primary.addSuppressed(failure);
            } catch (Throwable ignored) {
                // Preserve forward cleanup progress when suppression itself cannot allocate.
            }
        }
    }

    private Entry acquireEntry() {
        final int carrierId = CarrierIdentity.current();
        if (carrierId >= 0) {
            final EntryHolder localPool = localQueryPool.get(carrierId);
            final Entry entry = localPool.entry;
            if (entry != null) {
                localPool.entry = null;
                return entry;
            }
        }
        final Entry entry = queryPool.pop();
        return entry != null ? entry : new Entry();
    }

    private Entry getOwnerEntry(long queryId, SqlExecutionContext executionContext) {
        if (queryId < 0) {
            throw new IllegalArgumentException("query registry owner ID must be non-negative");
        }
        final Entry entry = registry.get(queryId);
        if (entry == null
                || !Entry.isActiveLifecycle(queryId, entry.lifecycle)
                || !entry.protocolOwner
                || entry.executionContext != executionContext) {
            throw new IllegalStateException("query registry owner is no longer active [id=" + queryId + ']');
        }
        return entry;
    }

    private void recycle(Entry entry) {
        entry.clear();
        final int carrierId = CarrierIdentity.current();
        if (carrierId >= 0) {
            final EntryHolder localPool = localQueryPool.get(carrierId);
            if (localPool.entry == null) {
                localPool.entry = entry;
                return;
            }
        }
        queryPool.tryPush(entry, queryPoolCapacity);
    }

    private long register0(CharSequence query, SqlExecutionContext executionContext, boolean deferQueryText) {
        final long queryId = idSeq.getAndIncrement();
        final Entry e = acquireEntry();
        // Just in case something messed the cached Entry
        // while it was in the pool, like late query cancel()
        // clean the object before using.
        e.clear();

        e.registeredAtNs = clock.getTicks();
        e.changedAtNs = e.registeredAtNs;
        e.state = Entry.State.ACTIVE;

        if (deferQueryText) {
            e.queryText = DEFERRED_QUERY_TEXT;
        } else if (executionContext.containsSecret()) {
            e.queryText = SECRET_QUERY_TEXT;
        } else {
            // we shouldn't copy text in case of sensitive queries
            e.query.put(query);
            e.queryText = e.query;
        }

        final Worker worker = Worker.current();
        if (worker != null) {
            e.workerId = worker.getWorkerId();
            e.poolName = worker.getPoolName();
        }
        e.isWAL = executionContext.isWalApplication();
        e.principal = executionContext.getSecurityContext().getPrincipal();
        e.executionContext = executionContext;
        e.protocolOwner = deferQueryText;
        e.referenceCount = 1;

        final MemoryTracker outerTracker = executionContext.getMemoryTracker();

        boolean isCancellationBound = false;
        try {
            // Publish the descriptor before admission so a queued query can be
            // cancelled through the registry.
            e.activate(queryId);
            registry.put(queryId, e);

            executionContext.copyCancelledFlagsTo(e.previousCancelledBinding, e.previousSimpleCancelledBinding);
            executionContext.setCancelledFlag(e.cancelled, e.cancelledGeneration);
            isCancellationBound = true;
            e.executionLease = executionContext.getCairoEngine().onSqlExecutionRegistered(
                    queryId,
                    executionContext,
                    e.cancelled,
                    e.cancelledGeneration
            );

            // Acquire a per-workload memory tracker only after an optional engine admission hook
            // has committed. A queued Enterprise query therefore owns its registry descriptor and
            // cancellation signal, but no query tracker or active memory budget. OSS returns no
            // execution lease and reaches this block without any additional branch or allocation.
            //
            // Inheritance is allowed only when the bound tracker belongs to a non-QUERY background
            // workload. A QUERY tracker already on the context is not inherited because concurrent
            // PG named portals are siblings that share one SqlExecutionContext.
            if (outerTracker == null || outerTracker.getWorkload() == MemoryTrackerWorkload.QUERY) {
                MemoryTracker tracker = e.executionLease instanceof SqlExecutionLease sqlExecutionLease
                        ? sqlExecutionLease.getMemoryTracker()
                        : null;
                if (tracker == null) {
                    final MemoryTrackerProvider provider = executionContext.getCairoEngine().getMemoryTrackerProvider();
                    tracker = provider.acquire(
                            executionContext.getSecurityContext(),
                            queryId,
                            MemoryTrackerWorkload.QUERY
                    );
                }
                executionContext.setMemoryTracker(tracker);
                e.memoryTracker = tracker;
            }

            // Registration listeners observe a fully initialized execution: admission has
            // committed, cancellation is bound, and any query memory tracker is installed.
            Listener listener = this.listener;
            if (listener != null) {
                listener.onRegister(deferQueryText ? DEFERRED_QUERY_TEXT : query, queryId, executionContext);
            }
        } catch (Throwable th) {
            // registry.put() can OOM mid-rehash. register() runs outside the
            // caller's try/finally, so unregister() never fires here -- release the
            // just-acquired tracker (else its native blocks leak during the very OOM
            // the feature bounds), drop the partial entry, and retire the Entry
            // before recycling it.
            boolean detached = false;
            try {
                detached = registry.remove(queryId, e) || registry.get(queryId) != e;
                if (!detached) {
                    suppressCleanupFailure(
                            th,
                            new IllegalStateException("query registry rollback could not detach entry [id=" + queryId + ']')
                    );
                }
            } catch (Throwable cleanupFailure) {
                suppressCleanupFailure(th, cleanupFailure);
            }
            if (isCancellationBound) {
                try {
                    clearStaleSignalBinding(e.previousCancelledBinding);
                } catch (Throwable cleanupFailure) {
                    suppressCleanupFailure(th, cleanupFailure);
                }
                try {
                    clearStaleSignalBinding(e.previousSimpleCancelledBinding);
                } catch (Throwable cleanupFailure) {
                    suppressCleanupFailure(th, cleanupFailure);
                }
                try {
                    executionContext.restoreCancelledFlag(
                            e.cancelled,
                            e.previousCancelledBinding,
                            e.previousSimpleCancelledBinding
                    );
                } catch (Throwable cleanupFailure) {
                    suppressCleanupFailure(th, cleanupFailure);
                }
            }
            final MemoryTracker memoryTracker = e.memoryTracker;
            if (memoryTracker != null) {
                // Restore the prior tracker only if the slot is still ours; a
                // concurrently-suspended sibling portal may have rebound it.
                try {
                    if (executionContext.getMemoryTracker() == memoryTracker) {
                        executionContext.setMemoryTracker(outerTracker);
                    }
                } catch (Throwable cleanupFailure) {
                    suppressCleanupFailure(th, cleanupFailure);
                }
                try {
                    memoryTracker.close();
                } catch (Throwable cleanupFailure) {
                    suppressCleanupFailure(th, cleanupFailure);
                } finally {
                    e.memoryTracker = null;
                }
            }
            final QuietCloseable executionLease = e.executionLease;
            if (executionLease != null) {
                try {
                    executionLease.close();
                } catch (Throwable cleanupFailure) {
                    suppressCleanupFailure(th, cleanupFailure);
                } finally {
                    e.executionLease = null;
                }
            }
            boolean retired = false;
            try {
                retired = e.retire(queryId);
            } catch (Throwable cleanupFailure) {
                suppressCleanupFailure(th, cleanupFailure);
            }
            if (detached && retired) {
                try {
                    recycle(e);
                } catch (Throwable cleanupFailure) {
                    suppressCleanupFailure(th, cleanupFailure);
                }
            } else if (!retired) {
                try {
                    LOG.error().$("query lifecycle mismatch on register rollback [id=").$(queryId).I$();
                } catch (Throwable cleanupFailure) {
                    suppressCleanupFailure(th, cleanupFailure);
                }
            }
            throw th;
        }
        return queryId;
    }

    private long retainOwner(long ownerId, SqlExecutionContext executionContext) {
        final Entry owner = registry.get(ownerId);
        if (owner != null && owner.protocolOwner && owner.executionContext == executionContext) {
            if (owner.retain(ownerId)) {
                return ownerId;
            }
            throw new IllegalStateException("query registry owner is no longer active [id=" + ownerId + ']');
        }
        throw new IllegalStateException("query registry owner does not match execution context [id=" + ownerId + ']');
    }

    private long tryRetainDispatchedOwner(long ownerId, SqlExecutionContext executionContext) {
        final Entry owner = registry.get(ownerId);
        if (owner != null
                && Entry.isActiveLifecycle(ownerId, owner.lifecycle)
                && (!owner.protocolOwner || owner.executionContext != executionContext)) {
            // Dispatch identity propagates through nested work, including SYSTEM SQL using its
            // own execution context. Only the protocol context that created an owner may retain
            // it. Ordinary query owners and nested contexts receive independent registry entries.
            return -1;
        }
        return retainOwner(ownerId, executionContext);
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
        private static final int RELEASE_FINAL = 1;
        private static final int RELEASE_MISMATCH = -1;
        private static final int RELEASE_RETAINED = 0;
        private final FiberCancellationSignal cancelled = new FiberCancellationSignal();
        private final CancellationBinding previousCancelledBinding = new CancellationBinding();
        private final CancellationBinding previousSimpleCancelledBinding = new CancellationBinding();
        private final StringSink query = new StringSink();
        private long cancelledGeneration;
        private long changedAtNs;
        private SqlExecutionContext executionContext;
        private volatile QuietCloseable executionLease;
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
        private boolean protocolOwner;
        private volatile CharSequence queryText = query;
        private int referenceCount;
        private long registeredAtNs;
        private byte state;
        private long workerId;

        public static boolean isActiveLifecycle(long queryId, long lifecycle) {
            return lifecycle == lifecycle(queryId, LIFECYCLE_STATE_ACTIVE);
        }

        public void cancel() {
            cancelled.cancel(cancelledGeneration);
        }

        @Override
        public void clear() {
            query.clear();
            registeredAtNs = 0;
            changedAtNs = 0;
            cancelledGeneration = cancelled.reopen();
            executionContext = null;
            executionLease = null;
            memoryTracker = null;
            poolName = null;
            previousCancelledBinding.clear();
            previousSimpleCancelledBinding.clear();
            protocolOwner = false;
            queryText = query;
            referenceCount = 0;
            workerId = -1;
            principal = null;
            state = State.IDLE;
            isWAL = false;
            lifecycle = LIFECYCLE_IDLE;
        }

        public AtomicBoolean getCancelled() {
            return cancelled;
        }

        public long getCancelledGeneration() {
            return cancelledGeneration;
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

        public CharSequence getQuery() {
            return queryText;
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

        private static long lifecycle(long queryId, long state) {
            return (queryId << 2) | state;
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

        private boolean publishQuery(long queryId, CharSequence queryText, boolean containsSecret) {
            if (!beginCancel(queryId)) {
                return false;
            }
            try {
                if (this.queryText != DEFERRED_QUERY_TEXT) {
                    return true;
                }
                if (containsSecret) {
                    this.queryText = SECRET_QUERY_TEXT;
                } else {
                    query.clear();
                    query.put(queryText);
                    // Volatile publication happens only after the reusable sink is complete, so
                    // query_activity() can never race a mutation of the visible buffer.
                    this.queryText = query;
                }
                return true;
            } finally {
                activate(queryId);
            }
        }

        private int release(long queryId, SqlExecutionContext executionContext) {
            if (!beginCancel(queryId)) {
                return RELEASE_MISMATCH;
            }
            boolean reactivate = true;
            try {
                if (this.executionContext != executionContext || referenceCount < 1) {
                    return RELEASE_MISMATCH;
                }
                referenceCount--;
                if (referenceCount > 0) {
                    return RELEASE_RETAINED;
                }
                lifecycle = lifecycle(queryId, LIFECYCLE_STATE_RETIRED);
                reactivate = false;
                return RELEASE_FINAL;
            } finally {
                if (reactivate) {
                    activate(queryId);
                }
            }
        }

        private boolean retain(long queryId) {
            if (!beginCancel(queryId)) {
                return false;
            }
            try {
                if (referenceCount < 1) {
                    return false;
                }
                if (referenceCount == Integer.MAX_VALUE) {
                    throw new IllegalStateException("query registry owner reference count exhausted [id=" + queryId + ']');
                }
                referenceCount++;
                return true;
            } finally {
                activate(queryId);
            }
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

    private static class EntryHolder {
        private Entry entry;

        private EntryHolder() {
        }
    }
}
