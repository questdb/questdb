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

package io.questdb.test.griffin;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.griffin.QueryRegistry;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Exercises the QueryRegistry.Entry lifecycle protocol that guards pooled
 * Entry reuse against late cancel() calls racing unregister().
 */
public class QueryRegistryLifecycleTest extends AbstractCairoTest {

    /**
     * Cancelling an id that is no longer registered returns false. The entry is
     * already gone from the registry map, so cancel() short-circuits at the
     * registry lookup before it ever reaches the lifecycle guard. This covers
     * the plain registry-miss path; the lifecycle guard itself is covered by
     * {@link #testRecycledEntryReportsStaleLifecycle()} and
     * {@link #testStaleCancellerCannotTouchRecycledEntry()}.
     */
    @Test
    public void testCancelReturnsFalseForUnregisteredId() throws Exception {
        assertMemoryLeak(() -> {
            final QueryRegistry registry = engine.getQueryRegistry();
            try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE)) {
                final long queryId = registry.register("SELECT 1", context);
                registry.unregister(queryId, context);
                Assert.assertFalse(registry.cancel(queryId, context));
            }
        });
    }

    @Test
    public void testCancelSetsCancelledFlagAndState() throws Exception {
        assertMemoryLeak(() -> {
            final QueryRegistry registry = engine.getQueryRegistry();
            try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE)) {
                final long queryId = registry.register("SELECT 1", context);
                final QueryRegistry.Entry entry = registry.getEntry(queryId);
                Assert.assertNotNull(entry);
                Assert.assertFalse(entry.getCancelled().get());

                Assert.assertTrue(registry.cancel(queryId, context));
                Assert.assertTrue(entry.getCancelled().get());
                Assert.assertEquals(QueryRegistry.Entry.State.CANCELLED, entry.getState());

                // cancelling an already cancelled, still registered query succeeds again
                Assert.assertTrue(registry.cancel(queryId, context));

                registry.unregister(queryId, context);
                Assert.assertNull(registry.getEntry(queryId));
            }
        });
    }

    @Test
    public void testDeniedCrossUserCancelReactivatesEntry() throws Exception {
        assertMemoryLeak(() -> {
            final QueryRegistry registry = engine.getQueryRegistry();
            try (
                    SqlExecutionContextImpl ownerContext = new SqlExecutionContextImpl(engine, 1).with(new PrincipalSecurityContext("owner"));
                    SqlExecutionContextImpl deniedContext = new SqlExecutionContextImpl(engine, 1).with(new DenyingSqlEngineAdminSecurityContext("intruder"));
                    SqlExecutionContextImpl adminContext = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE)
            ) {
                final long queryId = registry.register("SELECT owner", ownerContext);
                final QueryRegistry.Entry entry = registry.getEntry(queryId);
                Assert.assertNotNull(entry);
                try {
                    try {
                        registry.cancel(queryId, deniedContext);
                        Assert.fail("expected admin authorization failure");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "Access denied for intruder [SQL ENGINE ADMIN]");
                    }
                    assertActive(queryId, entry);

                    Assert.assertTrue(registry.cancel(queryId, adminContext));
                    Assert.assertTrue(entry.getCancelled().get());
                } finally {
                    if (registry.getEntry(queryId) != null) {
                        registry.unregister(queryId, ownerContext);
                    }
                }
                Assert.assertNull(registry.getEntry(queryId));
            }
        });
    }

    @Test
    public void testPhaseTwoCancelReturnsFalseWhenQueryFinishesDuringChecks() throws Exception {
        assertMemoryLeak(() -> {
            final QueryRegistry registry = engine.getQueryRegistry();
            final CountDownLatch cancellerInAuthorize = new CountDownLatch(1);
            final CountDownLatch releaseCanceller = new CountDownLatch(1);
            final CountDownLatch cancellerDone = new CountDownLatch(1);
            final AtomicBoolean cancelResult = new AtomicBoolean(true);
            final AtomicReference<Throwable> fault = new AtomicReference<>();

            try (
                    SqlExecutionContextImpl ownerContext = new SqlExecutionContextImpl(engine, 1).with(new PrincipalSecurityContext("owner"));
                    SqlExecutionContextImpl cancelContext = new SqlExecutionContextImpl(engine, 1).with(
                            new BlockingSqlEngineAdminSecurityContext("admin", cancellerInAuthorize, releaseCanceller)
                    )
            ) {
                final long queryId = registry.register("SELECT owner", ownerContext);
                final QueryRegistry.Entry entry = registry.getEntry(queryId);
                Assert.assertNotNull(entry);

                final Thread cancellerThread = new Thread(() -> {
                    try {
                        cancelResult.set(registry.cancel(queryId, cancelContext));
                    } catch (Throwable t) {
                        fault.compareAndSet(null, t);
                    } finally {
                        cancellerDone.countDown();
                    }
                }, "query_registry_phase_two_canceller");

                cancellerThread.start();
                try {
                    Assert.assertTrue("canceller did not reach admin authorization", cancellerInAuthorize.await(5, TimeUnit.SECONDS));
                    assertActive(queryId, entry);

                    registry.unregister(queryId, ownerContext);
                    Assert.assertNull(registry.getEntry(queryId));
                } finally {
                    releaseCanceller.countDown();
                    Assert.assertTrue("canceller did not finish", cancellerDone.await(5, TimeUnit.SECONDS));
                    cancellerThread.join(5_000);
                }
                Assert.assertFalse("canceller thread hung", cancellerThread.isAlive());
                if (fault.get() != null) {
                    throw new AssertionError("canceller failed", fault.get());
                }
                Assert.assertFalse(cancelResult.get());
            }
        });
    }

    /**
     * After an entry is unregistered and the pooled object is reused for a new
     * query, its lifecycle word no longer matches the old query id. This is the
     * invariant the whole fix relies on: a stale canceller (or a
     * query_activity() snapshot) holding the recycled entry sees
     * isActiveLifecycle() return false for the old id and rejects it, while the
     * new id is reported active. Single-threaded, so the recycle is
     * deterministic - register() pops the very same entry back from the
     * thread-local pool.
     */
    @Test
    public void testRecycledEntryReportsStaleLifecycle() throws Exception {
        assertMemoryLeak(() -> {
            final QueryRegistry registry = engine.getQueryRegistry();
            try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE)) {
                final long oldId = registry.register("SELECT old", context);
                final QueryRegistry.Entry entry = registry.getEntry(oldId);
                Assert.assertNotNull(entry);
                Assert.assertTrue(QueryRegistry.Entry.isActiveLifecycle(oldId, entry.getLifecycle()));

                registry.unregister(oldId, context);

                final long newId = registry.register("SELECT new", context);
                Assert.assertNotEquals(oldId, newId);
                // the thread-local pool hands the very same Entry object back
                Assert.assertSame(entry, registry.getEntry(newId));

                // the recycled entry is active for the new id and stale for the old one
                Assert.assertTrue(QueryRegistry.Entry.isActiveLifecycle(newId, entry.getLifecycle()));
                Assert.assertFalse(QueryRegistry.Entry.isActiveLifecycle(oldId, entry.getLifecycle()));

                registry.unregister(newId, context);
            }
        });
    }

    @Test
    public void testRegisterRollbackRetiresEntryWhenListenerThrows() throws Exception {
        assertMemoryLeak(() -> {
            final QueryRegistry registry = engine.getQueryRegistry();
            try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE)) {
                final RuntimeException boom = new RuntimeException("listener boom");
                final AtomicLong rolledBackId = new AtomicLong(-1);
                final AtomicReference<QueryRegistry.Entry> rolledBackEntry = new AtomicReference<>();

                Assert.assertNull(context.getMemoryTracker());
                registry.setListener((query, queryId, executionContext) -> {
                    // register() has already published the entry in the registry when
                    // the listener runs, so capture it, then fail the registration.
                    rolledBackId.set(queryId);
                    rolledBackEntry.set(registry.getEntry(queryId));
                    throw boom;
                });
                try {
                    registry.register("SELECT rollback", context);
                    Assert.fail("expected the listener failure to propagate");
                } catch (RuntimeException e) {
                    Assert.assertSame(boom, e);
                } finally {
                    registry.setListener(null);
                }

                final long oldId = rolledBackId.get();
                final QueryRegistry.Entry entry = rolledBackEntry.get();
                Assert.assertTrue(oldId >= 0);
                Assert.assertNotNull(entry);
                // rollback dropped the entry from the registry and released the
                // per-query tracker it had bound on the context.
                Assert.assertNull(registry.getEntry(oldId));
                Assert.assertNull(context.getMemoryTracker());

                // rollback retired the entry: the next register() pops the very same
                // Entry back from the thread-local pool, now active for the new id only.
                final long newId = registry.register("SELECT after rollback", context);
                Assert.assertNotEquals(oldId, newId);
                Assert.assertSame(entry, registry.getEntry(newId));
                Assert.assertTrue(QueryRegistry.Entry.isActiveLifecycle(newId, entry.getLifecycle()));
                Assert.assertFalse(QueryRegistry.Entry.isActiveLifecycle(oldId, entry.getLifecycle()));

                registry.unregister(newId, context);
            }
        });
    }

    @Test
    public void testSlowCancellerPrincipalDoesNotBlockUnregister() throws Exception {
        assertMemoryLeak(() -> {
            final QueryRegistry registry = engine.getQueryRegistry();
            final CountDownLatch principalRequested = new CountDownLatch(1);
            final CountDownLatch releasePrincipal = new CountDownLatch(1);
            final CountDownLatch unregisterDone = new CountDownLatch(1);
            final CountDownLatch cancellerDone = new CountDownLatch(1);
            final AtomicReference<Throwable> fault = new AtomicReference<>();

            try (
                    SqlExecutionContextImpl ownerContext = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE);
                    SqlExecutionContextImpl cancelContext = new SqlExecutionContextImpl(engine, 1).with(
                            new BlockingPrincipalSecurityContext(principalRequested, releasePrincipal)
                    )
            ) {
                final long queryId = registry.register("SELECT slow principal", ownerContext);
                final Thread cancellerThread = new Thread(() -> {
                    try {
                        Assert.assertFalse(registry.cancel(queryId, cancelContext));
                    } catch (Throwable t) {
                        fault.compareAndSet(null, t);
                    } finally {
                        cancellerDone.countDown();
                    }
                }, "query_registry_slow_principal_canceller");
                final Thread unregisterThread = new Thread(() -> {
                    try {
                        registry.unregister(queryId, ownerContext);
                    } catch (Throwable t) {
                        fault.compareAndSet(null, t);
                    } finally {
                        unregisterDone.countDown();
                    }
                }, "query_registry_unregister");

                cancellerThread.start();
                try {
                    Assert.assertTrue("canceller did not request principal", principalRequested.await(5, TimeUnit.SECONDS));
                    unregisterThread.start();
                    Assert.assertTrue(
                            "unregister waited for canceller principal lookup",
                            unregisterDone.await(5, TimeUnit.SECONDS)
                    );
                } finally {
                    releasePrincipal.countDown();
                    unregisterThread.join(5_000);
                    Assert.assertTrue("canceller did not finish", cancellerDone.await(5, TimeUnit.SECONDS));
                    cancellerThread.join(5_000);
                }
                Assert.assertFalse("unregister thread hung", unregisterThread.isAlive());
                Assert.assertFalse("canceller thread hung", cancellerThread.isAlive());
                if (fault.get() != null) {
                    throw new AssertionError("worker thread failed", fault.get());
                }
                Assert.assertNull(registry.getEntry(queryId));
            }
        });
    }

    /**
     * Reproduces the pooled-entry reuse race: a canceller looks an entry up in
     * the registry, the owner unregisters the query and the entry is recycled
     * for a new query, then the stale canceller proceeds. Without the lifecycle
     * guard the stale canceller sets the cancelled flag of the new, unrelated
     * query.
     * <p>
     * Cancellers only ever target even query ids, so a cancelled flag observed
     * on an odd-id query proves a stale canceller touched a recycled entry.
     */
    @Test
    public void testStaleCancellerCannotTouchRecycledEntry() throws Exception {
        assertMemoryLeak(() -> {
            final QueryRegistry registry = engine.getQueryRegistry();
            final int producerCount = 4;
            final int cancellerCount = 2;
            final int iterations = 10_000;

            final AtomicLongArray liveCancellableIds = new AtomicLongArray(producerCount);
            for (int i = 0; i < producerCount; i++) {
                liveCancellableIds.set(i, -1);
            }
            final AtomicInteger runningProducers = new AtomicInteger(producerCount);
            final AtomicLong cancelAttempts = new AtomicLong();
            final AtomicReference<Throwable> fault = new AtomicReference<>();
            final CyclicBarrier startBarrier = new CyclicBarrier(producerCount + cancellerCount);
            final ObjList<Thread> threads = new ObjList<>();

            for (int p = 0; p < producerCount; p++) {
                final int slot = p;
                final Thread thread = new Thread(() -> {
                    try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE)) {
                        startBarrier.await();
                        for (int i = 0; i < iterations && fault.get() == null; i++) {
                            final long queryId = registry.register("SELECT " + slot, context);
                            final QueryRegistry.Entry entry = registry.getEntry(queryId);
                            final AtomicBoolean cancelledFlag = entry.getCancelled();
                            final boolean isCancellable = (queryId & 1) == 0;
                            if (isCancellable) {
                                liveCancellableIds.set(slot, queryId);
                            }
                            // work window for cancellers to race against
                            for (int j = 0; j < 20; j++) {
                                if (!isCancellable && cancelledFlag.get()) {
                                    throw new AssertionError("stale canceller cancelled query " + queryId);
                                }
                                Os.pause();
                            }
                            if (isCancellable) {
                                liveCancellableIds.set(slot, -1);
                            }
                            registry.unregister(queryId, context);
                            if (!isCancellable && cancelledFlag.get()) {
                                throw new AssertionError("stale canceller cancelled query " + queryId + " around unregister");
                            }
                        }
                    } catch (Throwable t) {
                        fault.compareAndSet(null, t);
                    } finally {
                        runningProducers.decrementAndGet();
                    }
                });
                threads.add(thread);
            }

            for (int c = 0; c < cancellerCount; c++) {
                final int seed = c;
                final Thread thread = new Thread(() -> {
                    try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE)) {
                        startBarrier.await();
                        int slot = seed;
                        while (runningProducers.get() > 0 && fault.get() == null) {
                            slot = (slot + 1) % producerCount;
                            final long queryId = liveCancellableIds.get(slot);
                            if (queryId < 0) {
                                Os.pause();
                                continue;
                            }
                            // target even ids only; cancel may legitimately win or
                            // lose the race against unregister, both return values
                            // are valid here
                            registry.cancel(queryId, context);
                            cancelAttempts.incrementAndGet();
                        }
                    } catch (Throwable t) {
                        fault.compareAndSet(null, t);
                    }
                });
                threads.add(thread);
            }

            for (int i = 0, n = threads.size(); i < n; i++) {
                threads.getQuick(i).start();
            }
            for (int i = 0, n = threads.size(); i < n; i++) {
                threads.getQuick(i).join(120_000);
                Assert.assertFalse("worker thread hung", threads.getQuick(i).isAlive());
            }

            if (fault.get() != null) {
                throw new AssertionError("worker thread failed", fault.get());
            }
            Assert.assertTrue("cancellers never raced a live query", cancelAttempts.get() > 0);
        });
    }

    @Test
    public void testSuccessfulCancelReadsCancellerPrincipalOnce() throws Exception {
        assertMemoryLeak(() -> {
            final QueryRegistry registry = engine.getQueryRegistry();
            final AtomicInteger principalReads = new AtomicInteger();
            try (
                    SqlExecutionContextImpl ownerContext = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE);
                    SqlExecutionContextImpl cancelContext = new SqlExecutionContextImpl(engine, 1).with(
                            new SingleReadPrincipalSecurityContext("admin", principalReads)
                    )
            ) {
                final long queryId = registry.register("SELECT one principal read", ownerContext);
                try {
                    Assert.assertTrue(registry.cancel(queryId, cancelContext));
                    Assert.assertEquals(1, principalReads.get());
                } finally {
                    if (registry.getEntry(queryId) != null) {
                        registry.unregister(queryId, ownerContext);
                    }
                }
            }
        });
    }

    @Test
    public void testWalCancelReactivatesEntryBeforeThrowing() throws Exception {
        assertMemoryLeak(() -> {
            final QueryRegistry registry = engine.getQueryRegistry();
            try (
                    SqlExecutionContextImpl walContext = newWalContext("wal-owner");
                    SqlExecutionContextImpl adminContext = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE)
            ) {
                final long queryId = registry.register("SELECT wal", walContext);
                final QueryRegistry.Entry entry = registry.getEntry(queryId);
                Assert.assertNotNull(entry);
                try {
                    try {
                        registry.cancel(queryId, adminContext);
                        Assert.fail("expected WAL cancel failure");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "query applied in WAL job can't be cancelled [id=" + queryId + "]");
                    }
                    assertActive(queryId, entry);

                } finally {
                    if (registry.getEntry(queryId) != null) {
                        registry.unregister(queryId, walContext);
                    }
                }
                Assert.assertNull(registry.getEntry(queryId));
            }
        });
    }

    private static void assertActive(long queryId, QueryRegistry.Entry entry) {
        Assert.assertTrue(QueryRegistry.Entry.isActiveLifecycle(queryId, entry.getLifecycle()));
        Assert.assertEquals(QueryRegistry.Entry.State.ACTIVE, entry.getState());
        Assert.assertFalse(entry.getCancelled().get());
    }

    private SqlExecutionContextImpl newWalContext(String principal) {
        return new SqlExecutionContextImpl(engine, 1) {
            @Override
            public boolean isWalApplication() {
                return true;
            }
        }.with(new PrincipalSecurityContext(principal));
    }

    private static class BlockingPrincipalSecurityContext extends AllowAllSecurityContext {
        private final CountDownLatch entered;
        private final CountDownLatch release;

        private BlockingPrincipalSecurityContext(CountDownLatch entered, CountDownLatch release) {
            this.entered = entered;
            this.release = release;
        }

        @Override
        public CharSequence getPrincipal() {
            entered.countDown();
            TestUtils.await(release);
            return AllowAllSecurityContext.INSTANCE.getPrincipal();
        }
    }

    private static class BlockingSqlEngineAdminSecurityContext extends PrincipalSecurityContext {
        private final CountDownLatch entered;
        private final CountDownLatch release;

        private BlockingSqlEngineAdminSecurityContext(String principal, CountDownLatch entered, CountDownLatch release) {
            super(principal);
            this.entered = entered;
            this.release = release;
        }

        @Override
        public void authorizeSqlEngineAdmin() {
            entered.countDown();
            TestUtils.await(release);
        }
    }

    private static class DenyingSqlEngineAdminSecurityContext extends PrincipalSecurityContext {
        private DenyingSqlEngineAdminSecurityContext(String principal) {
            super(principal);
        }

        @Override
        public void authorizeSqlEngineAdmin() {
            throw CairoException.authorization().put("Access denied for ").put(getPrincipal()).put(" [SQL ENGINE ADMIN]");
        }
    }

    private static class PrincipalSecurityContext extends AllowAllSecurityContext {
        private final String principal;

        private PrincipalSecurityContext(String principal) {
            this.principal = principal;
        }

        @Override
        public String getPrincipal() {
            return principal;
        }
    }

    private static class SingleReadPrincipalSecurityContext extends PrincipalSecurityContext {
        private final AtomicInteger reads;

        private SingleReadPrincipalSecurityContext(String principal, AtomicInteger reads) {
            super(principal);
            this.reads = reads;
        }

        @Override
        public String getPrincipal() {
            final int count = reads.incrementAndGet();
            if (count > 1) {
                throw new AssertionError("principal read more than once");
            }
            return super.getPrincipal();
        }
    }
}
