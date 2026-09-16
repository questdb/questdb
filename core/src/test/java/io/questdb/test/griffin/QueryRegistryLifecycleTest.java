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

import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.griffin.QueryRegistry;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.mp.CarrierIdentity;
import io.questdb.mp.continuation.FiberCancellationSignal;
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

public class QueryRegistryLifecycleTest extends AbstractCairoTest {

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
    public void testCancellationBindingSurvivesSiblingUnregister() throws Exception {
        assertMemoryLeak(() -> {
            final QueryRegistry registry = engine.getQueryRegistry();
            try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE)) {
                context.setUseSimpleCircuitBreaker(true);
                final AtomicBoolean originalCancelled = context.getCircuitBreaker().getCancelledFlag();
                long queryIdA = -1;
                long queryIdB = -1;
                try {
                    queryIdA = registry.register("portal A", context);
                    final QueryRegistry.Entry entryA = registry.getEntry(queryIdA);
                    queryIdB = registry.register("portal B", context);
                    final QueryRegistry.Entry entryB = registry.getEntry(queryIdB);
                    Assert.assertNotNull(entryA);
                    Assert.assertNotNull(entryB);
                    final AtomicBoolean cancelledA = entryA.getCancelled();
                    final AtomicBoolean cancelledB = entryB.getCancelled();

                    Assert.assertNotSame(cancelledA, cancelledB);
                    context.setCancelledFlag(cancelledA);
                    registry.unregister(queryIdB, context);
                    Assert.assertSame(cancelledA, context.getCircuitBreaker().getCancelledFlag());

                    Assert.assertTrue(registry.cancel(queryIdA, context));
                    Assert.assertTrue(cancelledA.get());
                    Assert.assertFalse(cancelledB.get());

                    registry.unregister(queryIdA, context);
                    Assert.assertSame(originalCancelled, context.getCircuitBreaker().getCancelledFlag());
                } finally {
                    if (queryIdB > -1 && registry.getEntry(queryIdB) != null) {
                        registry.unregister(queryIdB, context);
                    }
                    if (queryIdA > -1 && registry.getEntry(queryIdA) != null) {
                        registry.unregister(queryIdA, context);
                    }
                }
            }
        });
    }

    @Test
    public void testCancellationBindingSurvivesThreeDeepMiddleUnregister() throws Exception {
        assertMemoryLeak(() -> {
            final QueryRegistry registry = engine.getQueryRegistry();
            try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE)) {
                context.setUseSimpleCircuitBreaker(true);
                long queryIdA = -1;
                long queryIdB = -1;
                long queryIdC = -1;
                try {
                    queryIdA = registry.register("portal A", context);
                    queryIdB = registry.register("portal B", context);
                    queryIdC = registry.register("portal C", context);
                    final QueryRegistry.Entry entryC = registry.getEntry(queryIdC);
                    Assert.assertNotNull(entryC);
                    final AtomicBoolean cancelledC = entryC.getCancelled();
                    Assert.assertSame(cancelledC, context.getCircuitBreaker().getCancelledFlag());

                    registry.unregister(queryIdB, context);

                    Assert.assertSame(cancelledC, context.getCircuitBreaker().getCancelledFlag());
                    Assert.assertTrue(registry.cancel(queryIdC, context));
                    Assert.assertTrue(context.getCircuitBreaker().checkIfTripped());
                } finally {
                    if (queryIdC > -1 && registry.getEntry(queryIdC) != null) {
                        registry.unregister(queryIdC, context);
                    }
                    if (queryIdB > -1 && registry.getEntry(queryIdB) != null) {
                        registry.unregister(queryIdB, context);
                    }
                    if (queryIdA > -1 && registry.getEntry(queryIdA) != null) {
                        registry.unregister(queryIdA, context);
                    }
                }
            }
        });
    }

    @Test
    public void testCancellationBindingRestoredAfterNestedUnregister() throws Exception {
        assertMemoryLeak(() -> {
            final QueryRegistry registry = engine.getQueryRegistry();
            try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE)) {
                context.setUseSimpleCircuitBreaker(true);
                final long outerQueryId = registry.register("outer", context);
                final QueryRegistry.Entry outerEntry = registry.getEntry(outerQueryId);
                final long innerQueryId = registry.register("inner", context);
                final QueryRegistry.Entry innerEntry = registry.getEntry(innerQueryId);
                Assert.assertNotNull(outerEntry);
                Assert.assertNotNull(innerEntry);
                Assert.assertSame(innerEntry.getCancelled(), context.getCircuitBreaker().getCancelledFlag());

                registry.unregister(innerQueryId, context);

                Assert.assertSame(outerEntry.getCancelled(), context.getCircuitBreaker().getCancelledFlag());
                Assert.assertFalse(context.getCircuitBreaker().checkIfTripped());
                Assert.assertTrue(registry.cancel(outerQueryId, context));
                Assert.assertTrue(context.getCircuitBreaker().checkIfTripped());
                registry.unregister(outerQueryId, context);
            }
        });
    }

    @Test
    public void testCancellationSignalIsReusedAcrossEntryLifecycles() throws Exception {
        assertMemoryLeak(() -> {
            final QueryRegistry registry = newSingleEntryRegistry();
            try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE)) {
                context.setUseSimpleCircuitBreaker(true);
                final long oldQueryId = registry.register("SELECT old", context);
                final QueryRegistry.Entry entry = registry.getEntry(oldQueryId);
                Assert.assertNotNull(entry);
                final FiberCancellationSignal signal = (FiberCancellationSignal) entry.getCancelled();
                final long oldGeneration = entry.getCancelledGeneration();
                final AtomicBooleanCircuitBreaker staleBreaker = new AtomicBooleanCircuitBreaker(engine);
                staleBreaker.setCancelledFlag(signal, oldGeneration);

                registry.unregister(oldQueryId, context);

                final long newQueryId = registry.register("SELECT new", context);
                final QueryRegistry.Entry reusedEntry = registry.getEntry(newQueryId);
                Assert.assertSame(entry, reusedEntry);
                Assert.assertSame(signal, reusedEntry.getCancelled());
                Assert.assertNotEquals(oldGeneration, reusedEntry.getCancelledGeneration());
                Assert.assertTrue(signal.isCancelled(oldGeneration));
                Assert.assertFalse(signal.isCancelled(reusedEntry.getCancelledGeneration()));

                staleBreaker.cancel();
                Assert.assertTrue(staleBreaker.checkIfTripped());
                Assert.assertFalse(signal.isCancelled(reusedEntry.getCancelledGeneration()));

                context.clearCancelledFlag(signal, oldGeneration);
                Assert.assertSame(signal, context.getCircuitBreaker().getCancelledFlag());
                Assert.assertTrue(registry.cancel(newQueryId, context));
                Assert.assertTrue(signal.isCancelled(reusedEntry.getCancelledGeneration()));

                registry.unregister(newQueryId, context);
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
    public void testEntryClearAllocatesNoJavaHeap() throws Exception {
        try (TestUtils.ThreadMetricsScope<com.sun.management.ThreadMXBean> scope = TestUtils.threadAllocationScope()) {
            final com.sun.management.ThreadMXBean threadMXBean = scope.getBean();
            assertMemoryLeak(() -> {
                final QueryRegistry.Entry entry = new QueryRegistry.Entry();
                for (int i = 0; i < 10_000; i++) {
                    entry.clear();
                }

                long minAllocatedBytes = Long.MAX_VALUE;
                for (int round = 0; round < 5; round++) {
                    final long allocatedBefore = threadMXBean.getCurrentThreadAllocatedBytes();
                    for (int i = 0; i < 100_000; i++) {
                        entry.clear();
                    }
                    minAllocatedBytes = Math.min(
                            minAllocatedBytes,
                            threadMXBean.getCurrentThreadAllocatedBytes() - allocatedBefore
                    );
                }
                Assert.assertEquals(0, minAllocatedBytes);
            });
        }
    }

    @Test
    public void testEntryPoolDoesNotExceedConfiguredSize() throws Exception {
        assertMemoryLeak(() -> {
            final QueryRegistry registry = newSingleEntryRegistry();
            try (
                    SqlExecutionContextImpl contextA = new SqlExecutionContextImpl(engine, 1)
                            .with(AllowAllSecurityContext.INSTANCE);
                    SqlExecutionContextImpl contextB = new SqlExecutionContextImpl(engine, 1)
                            .with(AllowAllSecurityContext.INSTANCE)
            ) {
                final long queryIdA = registry.register("SELECT A", contextA);
                final long queryIdB = registry.register("SELECT B", contextB);
                registry.unregister(queryIdA, contextA);
                registry.unregister(queryIdB, contextB);
                Assert.assertEquals(1, registry.getPoolSize());
            }
        });
    }

    @Test
    public void testEntryReturnsToCarrierLocalPool() throws Exception {
        assertMemoryLeak(() -> {
            Assert.assertEquals(CarrierIdentity.UNBOUND, CarrierIdentity.current());
            CarrierIdentity.bind();
            try {
                final QueryRegistry registry = newSingleEntryRegistry();
                try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE)) {
                    final long oldId = registry.register("SELECT old", context);
                    final QueryRegistry.Entry entry = registry.getEntry(oldId);
                    Assert.assertNotNull(entry);
                    Assert.assertEquals(0, registry.getPoolSize());

                    registry.unregister(oldId, context);

                    Assert.assertEquals(0, registry.getPoolSize());
                    final long newId = registry.register("SELECT new", context);
                    Assert.assertSame(entry, registry.getEntry(newId));
                    registry.unregister(newId, context);
                }
            } finally {
                CarrierIdentity.unbind();
            }
        });
    }

    @Test
    public void testEntryReturnsToSharedPoolAcrossThreads() throws Exception {
        assertMemoryLeak(() -> {
            final QueryRegistry registry = newSingleEntryRegistry();
            final AtomicReference<Throwable> fault = new AtomicReference<>();
            try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE)) {
                final long oldId = registry.register("SELECT old", context);
                final QueryRegistry.Entry entry = registry.getEntry(oldId);
                Assert.assertNotNull(entry);

                final Thread unregisterThread = new Thread(() -> {
                    try {
                        registry.unregister(oldId, context);
                    } catch (Throwable th) {
                        fault.set(th);
                    }
                }, "query_registry_unregister_migrated");
                unregisterThread.start();
                unregisterThread.join(5_000);
                Assert.assertFalse("unregister thread hung", unregisterThread.isAlive());
                if (fault.get() != null) {
                    throw new AssertionError("unregister failed", fault.get());
                }

                final long newId = registry.register("SELECT new", context);
                Assert.assertSame(entry, registry.getEntry(newId));
                registry.unregister(newId, context);
            }
        });
    }

    @Test
    public void testEntrySpillsFromCarrierLocalPoolToSharedPool() throws Exception {
        assertMemoryLeak(() -> {
            Assert.assertEquals(CarrierIdentity.UNBOUND, CarrierIdentity.current());
            CarrierIdentity.bind();
            try {
                final QueryRegistry registry = newSingleEntryRegistry();
                try (
                        SqlExecutionContextImpl contextA = new SqlExecutionContextImpl(engine, 1)
                                .with(AllowAllSecurityContext.INSTANCE);
                        SqlExecutionContextImpl contextB = new SqlExecutionContextImpl(engine, 1)
                                .with(AllowAllSecurityContext.INSTANCE);
                        SqlExecutionContextImpl contextC = new SqlExecutionContextImpl(engine, 1)
                                .with(AllowAllSecurityContext.INSTANCE)
                ) {
                    final long queryIdA = registry.register("SELECT A", contextA);
                    final QueryRegistry.Entry entryA = registry.getEntry(queryIdA);
                    final long queryIdB = registry.register("SELECT B", contextB);
                    final QueryRegistry.Entry entryB = registry.getEntry(queryIdB);
                    final long queryIdC = registry.register("SELECT C", contextC);
                    Assert.assertNotNull(entryA);
                    Assert.assertNotNull(entryB);

                    registry.unregister(queryIdA, contextA);
                    registry.unregister(queryIdB, contextB);
                    registry.unregister(queryIdC, contextC);

                    Assert.assertEquals(1, registry.getPoolSize());
                    final long reusedLocalId = registry.register("SELECT local", contextA);
                    Assert.assertSame(entryA, registry.getEntry(reusedLocalId));
                    Assert.assertEquals(1, registry.getPoolSize());
                    final long reusedSharedId = registry.register("SELECT shared", contextB);
                    Assert.assertSame(entryB, registry.getEntry(reusedSharedId));
                    Assert.assertEquals(0, registry.getPoolSize());
                    registry.unregister(reusedLocalId, contextA);
                    registry.unregister(reusedSharedId, contextB);
                    Assert.assertEquals(1, registry.getPoolSize());
                }
            } finally {
                CarrierIdentity.unbind();
            }
        });
    }

    @Test
    public void testMigratedEntryReturnsToCurrentCarrier() throws Exception {
        assertMemoryLeak(() -> {
            Assert.assertEquals(CarrierIdentity.UNBOUND, CarrierIdentity.current());
            CarrierIdentity.bind();
            try {
                final QueryRegistry registry = newSingleEntryRegistry();
                final AtomicReference<Throwable> fault = new AtomicReference<>();
                try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE)) {
                    final long oldId = registry.register("SELECT old", context);
                    final QueryRegistry.Entry entry = registry.getEntry(oldId);
                    Assert.assertNotNull(entry);

                    final Thread migratedCarrier = new Thread(() -> {
                        try {
                            Assert.assertEquals(CarrierIdentity.UNBOUND, CarrierIdentity.current());
                            CarrierIdentity.bind();
                            try {
                                registry.unregister(oldId, context);
                                Assert.assertEquals(0, registry.getPoolSize());
                                final long migratedId = registry.register("SELECT migrated", context);
                                Assert.assertSame(entry, registry.getEntry(migratedId));
                                registry.unregister(migratedId, context);
                            } finally {
                                CarrierIdentity.unbind();
                            }
                        } catch (Throwable th) {
                            fault.set(th);
                        }
                    }, "query_registry_migrated_carrier");
                    boolean isMigratedCarrierFinished = false;
                    try {
                        migratedCarrier.start();
                        migratedCarrier.join(5_000);
                        isMigratedCarrierFinished = !migratedCarrier.isAlive();
                    } finally {
                        if (migratedCarrier.isAlive()) {
                            migratedCarrier.interrupt();
                            migratedCarrier.join(5_000);
                        }
                    }
                    Assert.assertTrue("migrated carrier did not finish", isMigratedCarrierFinished);
                    Assert.assertFalse("migrated carrier survived cleanup", migratedCarrier.isAlive());
                    if (fault.get() != null) {
                        throw new AssertionError("migrated carrier failed", fault.get());
                    }

                    Assert.assertEquals(0, registry.getPoolSize());
                    final long sourceId = registry.register("SELECT source", context);
                    Assert.assertNotSame(entry, registry.getEntry(sourceId));
                    registry.unregister(sourceId, context);
                }
            } finally {
                CarrierIdentity.unbind();
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
                boolean isCancellerFinished = false;
                try {
                    Assert.assertTrue("canceller did not reach admin authorization", cancellerInAuthorize.await(5, TimeUnit.SECONDS));
                    assertActive(queryId, entry);

                    registry.unregister(queryId, ownerContext);
                    Assert.assertNull(registry.getEntry(queryId));
                } finally {
                    // Cleanup only: an assertion here would throw past the join and strand the
                    // canceller, which parks in TestUtils.await and would outlive the test in the
                    // shared fork. countDown is what releases it, so record whether that alone
                    // finished it - asserting after the interrupt would pass for a canceller that
                    // only exited because it was interrupted (TestUtils.await swallows the
                    // InterruptedException). Waiting first also keeps the happy path from
                    // interrupting a cancel() in flight; the interrupt covers a worker parked
                    // somewhere the latch does not reach.
                    releaseCanceller.countDown();
                    isCancellerFinished = cancellerDone.await(5, TimeUnit.SECONDS);
                    if (cancellerThread.isAlive()) {
                        cancellerThread.interrupt();
                    }
                    cancellerThread.join(5_000);
                }
                Assert.assertTrue("canceller did not finish", isCancellerFinished);
                Assert.assertFalse("canceller thread hung", cancellerThread.isAlive());
                if (fault.get() != null) {
                    throw new AssertionError("canceller failed", fault.get());
                }
                Assert.assertFalse(cancelResult.get());
            }
        });
    }

    @Test
    public void testRecycledEntryReportsStaleLifecycle() throws Exception {
        assertMemoryLeak(() -> {
            final QueryRegistry registry = newSingleEntryRegistry();
            try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE)) {
                final long oldId = registry.register("SELECT old", context);
                final QueryRegistry.Entry entry = registry.getEntry(oldId);
                Assert.assertNotNull(entry);
                Assert.assertTrue(QueryRegistry.Entry.isActiveLifecycle(oldId, entry.getLifecycle()));

                registry.unregister(oldId, context);

                final long newId = registry.register("SELECT new", context);
                Assert.assertNotEquals(oldId, newId);
                // the pool hands the very same Entry object back
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
            final QueryRegistry registry = newSingleEntryRegistry();
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
                // Entry back from the pool, now active for the new id only.
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
                boolean isCancellerFinished = false;
                try {
                    Assert.assertTrue("canceller did not request principal", principalRequested.await(5, TimeUnit.SECONDS));
                    unregisterThread.start();
                    Assert.assertTrue(
                            "unregister waited for canceller principal lookup",
                            unregisterDone.await(5, TimeUnit.SECONDS)
                    );
                } finally {
                    // Cleanup only, both threads - see the note in
                    // testPhaseTwoCancelReturnsFalseWhenQueryFinishesDuringChecks. unregisterDone
                    // is not awaited here: the thread may never have started, and nothing asserts
                    // on it - joining it is enough.
                    releasePrincipal.countDown();
                    isCancellerFinished = cancellerDone.await(5, TimeUnit.SECONDS);
                    if (unregisterThread.isAlive()) {
                        unregisterThread.interrupt();
                    }
                    if (cancellerThread.isAlive()) {
                        cancellerThread.interrupt();
                    }
                    unregisterThread.join(5_000);
                    cancellerThread.join(5_000);
                }
                Assert.assertTrue("canceller did not finish", isCancellerFinished);
                Assert.assertFalse("unregister thread hung", unregisterThread.isAlive());
                Assert.assertFalse("canceller thread hung", cancellerThread.isAlive());
                if (fault.get() != null) {
                    throw new AssertionError("worker thread failed", fault.get());
                }
                Assert.assertNull(registry.getEntry(queryId));
            }
        });
    }

    @Test
    public void testStaleCancellerCannotTouchRecycledEntry() throws Exception {
        assertMemoryLeak(() -> {
            final QueryRegistry registry = newSingleEntryRegistry();
            final CountDownLatch entryLookedUp = new CountDownLatch(1);
            final CountDownLatch releaseCanceller = new CountDownLatch(1);
            final AtomicBoolean cancelResult = new AtomicBoolean(true);
            final AtomicReference<Throwable> fault = new AtomicReference<>();

            try (
                    SqlExecutionContextImpl ownerContext = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE);
                    // cancel() reads the canceller's principal immediately after it looks the
                    // entry up and before it calls beginCancel(), so blocking inside getPrincipal()
                    // parks the canceller in exactly the window this test needs. The context belongs
                    // to this test alone, unlike an engine-wide hook a stranded test could leave
                    // behind to wedge every later cancel() in the fork.
                    SqlExecutionContextImpl cancelContext = new SqlExecutionContextImpl(engine, 1).with(
                            new BlockingPrincipalSecurityContext(entryLookedUp, releaseCanceller)
                    )
            ) {
                final long oldId = registry.register("SELECT old", ownerContext);
                final QueryRegistry.Entry oldEntry = registry.getEntry(oldId);
                Assert.assertNotNull(oldEntry);

                final Thread cancellerThread = new Thread(() -> {
                    try {
                        cancelResult.set(registry.cancel(oldId, cancelContext));
                    } catch (Throwable t) {
                        fault.compareAndSet(null, t);
                    }
                }, "query_registry_stale_canceller");

                long newId = -1;
                try {
                    // Start the canceller inside the try so the finally always releases the latch,
                    // even if Thread.start() throws (a loaded fork can fail to create a thread).
                    cancellerThread.start();
                    Assert.assertTrue("canceller did not look up the old entry", entryLookedUp.await(5, TimeUnit.SECONDS));

                    registry.unregister(oldId, ownerContext);
                    newId = registry.register("SELECT new", ownerContext);
                    final QueryRegistry.Entry newEntry = registry.getEntry(newId);
                    Assert.assertSame(oldEntry, newEntry);

                    releaseCanceller.countDown();
                    cancellerThread.join(5_000);
                    Assert.assertFalse("stale canceller thread hung", cancellerThread.isAlive());
                    if (fault.get() != null) {
                        throw new AssertionError("stale canceller failed", fault.get());
                    }
                    Assert.assertFalse(cancelResult.get());
                    Assert.assertFalse("stale canceller touched the recycled entry", newEntry.getCancelled().get());
                } finally {
                    releaseCanceller.countDown();
                    if (cancellerThread.isAlive()) {
                        cancellerThread.interrupt();
                        cancellerThread.join(5_000);
                    }
                    if (newId >= 0 && registry.getEntry(newId) != null) {
                        registry.unregister(newId, ownerContext);
                    } else if (registry.getEntry(oldId) != null) {
                        registry.unregister(oldId, ownerContext);
                    }
                }
                Assert.assertFalse("stale canceller thread survived cleanup", cancellerThread.isAlive());
            }
        });
    }

    @Test
    public void testConcurrentCancelCannotTouchRecycledEntry() throws Exception {
        assertMemoryLeak(() -> {
            final QueryRegistry registry = engine.getQueryRegistry();
            final int producerCount = 4;
            final int cancellerCount = 2;
            final int iterations = 10_000;
            final long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);

            final AtomicLongArray liveCancellableIds = new AtomicLongArray(producerCount);
            for (int i = 0; i < producerCount; i++) {
                liveCancellableIds.set(i, -1);
            }
            final AtomicInteger runningProducers = new AtomicInteger(producerCount);
            final AtomicLong cancelAttempts = new AtomicLong();
            final AtomicReference<Throwable> fault = new AtomicReference<>();
            final AtomicBoolean isStopped = new AtomicBoolean();
            final CyclicBarrier startBarrier = new CyclicBarrier(producerCount + cancellerCount);
            final ObjList<Thread> threads = new ObjList<>();

            for (int p = 0; p < producerCount; p++) {
                final int slot = p;
                final Thread thread = new Thread(() -> {
                    try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE)) {
                        startBarrier.await();
                        for (int i = 0; i < iterations && fault.get() == null && !isStopped.get() && System.nanoTime() - deadlineNanos < 0; i++) {
                            final long queryId = registry.register("SELECT " + slot, context);
                            final QueryRegistry.Entry entry = registry.getEntry(queryId);
                            final FiberCancellationSignal cancelledFlag = (FiberCancellationSignal) entry.getCancelled();
                            final long cancelledGeneration = entry.getCancelledGeneration();
                            final boolean isCancellable = (queryId & 1) == 0;
                            if (isCancellable) {
                                liveCancellableIds.set(slot, queryId);
                            }
                            // work window for cancellers to race against
                            for (int j = 0; j < 20; j++) {
                                if (!isCancellable && cancelledFlag.isCancelled(cancelledGeneration)) {
                                    throw new AssertionError("stale canceller cancelled query " + queryId);
                                }
                                Os.pause();
                            }
                            if (isCancellable) {
                                liveCancellableIds.set(slot, -1);
                            }
                            registry.unregister(queryId, context);
                        }
                    } catch (Throwable t) {
                        fault.compareAndSet(null, t);
                    } finally {
                        runningProducers.decrementAndGet();
                    }
                }, "query_registry_producer_" + p);
                threads.add(thread);
            }

            for (int c = 0; c < cancellerCount; c++) {
                final int seed = c;
                final Thread thread = new Thread(() -> {
                    try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE)) {
                        startBarrier.await();
                        int slot = seed;
                        while (runningProducers.get() > 0 && fault.get() == null && !isStopped.get()) {
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
                }, "query_registry_canceller_" + c);
                threads.add(thread);
            }

            final long joinDeadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
            try {
                for (int i = 0, n = threads.size(); i < n; i++) {
                    threads.getQuick(i).start();
                }
                for (int i = 0, n = threads.size(); i < n; i++) {
                    final long remainingMillis = TimeUnit.NANOSECONDS.toMillis(joinDeadlineNanos - System.nanoTime());
                    if (remainingMillis > 0) {
                        threads.getQuick(i).join(remainingMillis);
                    }
                }
            } finally {
                isStopped.set(true);
                for (int i = 0, n = threads.size(); i < n; i++) {
                    final Thread thread = threads.getQuick(i);
                    if (thread.isAlive()) {
                        thread.interrupt();
                    }
                }
                for (int i = 0, n = threads.size(); i < n; i++) {
                    threads.getQuick(i).join(5_000);
                }
            }

            for (int i = 0, n = threads.size(); i < n; i++) {
                Assert.assertFalse("worker thread hung: " + threads.getQuick(i).getName(), threads.getQuick(i).isAlive());
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
    public void testUnboundThreadUsesOnlySharedPool() throws Exception {
        assertMemoryLeak(() -> {
            Assert.assertEquals(CarrierIdentity.UNBOUND, CarrierIdentity.current());
            final QueryRegistry registry = newSingleEntryRegistry();
            try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE)) {
                final long oldId = registry.register("SELECT old", context);
                final QueryRegistry.Entry entry = registry.getEntry(oldId);
                Assert.assertNotNull(entry);
                Assert.assertEquals(0, registry.getPoolSize());

                registry.unregister(oldId, context);

                Assert.assertEquals(1, registry.getPoolSize());
                final long newId = registry.register("SELECT new", context);
                Assert.assertSame(entry, registry.getEntry(newId));
                registry.unregister(newId, context);
                Assert.assertEquals(1, registry.getPoolSize());
            }
        });
    }

    @Test
    public void testUnregisterRestoresEachBreakersOwnBinding() throws Exception {
        assertMemoryLeak(() -> {
            final QueryRegistry registry = engine.getQueryRegistry();
            try (
                    NetworkSqlExecutionCircuitBreaker networkCircuitBreaker = new NetworkSqlExecutionCircuitBreaker(
                            engine,
                            engine.getConfiguration().getCircuitBreakerConfiguration()
                    );
                    SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1)
            ) {
                context.with(AllowAllSecurityContext.INSTANCE, null, null, -1, networkCircuitBreaker);
                Assert.assertNull(networkCircuitBreaker.getCancelledFlag());
                context.setUseSimpleCircuitBreaker(true);
                final AtomicBoolean simpleOwnFlag = context.getCircuitBreaker().getCancelledFlag();
                Assert.assertNotNull(simpleOwnFlag);

                // UPDATE registers under the simple breaker and unregisters after switching back
                final long queryId = registry.register("UPDATE t SET x = 1", context);
                context.setUseSimpleCircuitBreaker(false);
                registry.unregister(queryId, context);

                Assert.assertNull(networkCircuitBreaker.getCancelledFlag());
                Assert.assertSame(simpleOwnFlag, context.getSimpleCircuitBreaker().getCancelledFlag());

                // a PG CancelRequest between statements must not latch the simple breaker's flag
                networkCircuitBreaker.cancel();
                networkCircuitBreaker.clearCancelSentinel();
                Assert.assertFalse(simpleOwnFlag.get());
                networkCircuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
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

    @Test
    public void testZeroSharedCapacityUsesOneCarrierLocalEntry() throws Exception {
        assertMemoryLeak(() -> {
            Assert.assertEquals(CarrierIdentity.UNBOUND, CarrierIdentity.current());
            final QueryRegistry registry = newZeroCapacityRegistry();
            final QueryRegistry.Entry retainedEntry;
            try (
                    SqlExecutionContextImpl contextA = new SqlExecutionContextImpl(engine, 1)
                            .with(AllowAllSecurityContext.INSTANCE);
                    SqlExecutionContextImpl contextB = new SqlExecutionContextImpl(engine, 1)
                            .with(AllowAllSecurityContext.INSTANCE)
            ) {
                CarrierIdentity.bind();
                try {
                    final long queryIdA = registry.register("SELECT A", contextA);
                    retainedEntry = registry.getEntry(queryIdA);
                    final long queryIdB = registry.register("SELECT B", contextB);
                    final QueryRegistry.Entry overflowEntry = registry.getEntry(queryIdB);
                    Assert.assertNotSame(retainedEntry, overflowEntry);

                    registry.unregister(queryIdA, contextA);
                    registry.unregister(queryIdB, contextB);

                    Assert.assertEquals(0, registry.getPoolSize());
                    final long reusedId = registry.register("SELECT reused", contextA);
                    Assert.assertSame(retainedEntry, registry.getEntry(reusedId));
                    final long freshId = registry.register("SELECT fresh", contextB);
                    Assert.assertNotSame(retainedEntry, registry.getEntry(freshId));
                    Assert.assertNotSame(overflowEntry, registry.getEntry(freshId));
                    Assert.assertEquals(0, registry.getPoolSize());
                    registry.unregister(reusedId, contextA);
                    registry.unregister(freshId, contextB);
                    Assert.assertEquals(0, registry.getPoolSize());
                } finally {
                    CarrierIdentity.unbind();
                }

                Assert.assertEquals(CarrierIdentity.UNBOUND, CarrierIdentity.current());
                final long unboundId = registry.register("SELECT unbound", contextA);
                Assert.assertNotSame(retainedEntry, registry.getEntry(unboundId));
                Assert.assertEquals(0, registry.getPoolSize());
                registry.unregister(unboundId, contextA);
                Assert.assertEquals(0, registry.getPoolSize());
            }
        });
    }

    private static void assertActive(long queryId, QueryRegistry.Entry entry) {
        Assert.assertTrue(QueryRegistry.Entry.isActiveLifecycle(queryId, entry.getLifecycle()));
        Assert.assertEquals(QueryRegistry.Entry.State.ACTIVE, entry.getState());
        Assert.assertFalse(entry.getCancelled().get());
    }

    private QueryRegistry newSingleEntryRegistry() {
        return new QueryRegistry(new CairoConfigurationWrapper(configuration) {
            @Override
            public int getQueryRegistryPoolSize() {
                return 1;
            }
        });
    }

    private SqlExecutionContextImpl newWalContext(String principal) {
        return new SqlExecutionContextImpl(engine, 1) {
            @Override
            public boolean isWalApplication() {
                return true;
            }
        }.with(new PrincipalSecurityContext(principal));
    }

    private QueryRegistry newZeroCapacityRegistry() {
        return new QueryRegistry(new CairoConfigurationWrapper(configuration) {
            @Override
            public int getQueryRegistryPoolSize() {
                return 0;
            }
        });
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
