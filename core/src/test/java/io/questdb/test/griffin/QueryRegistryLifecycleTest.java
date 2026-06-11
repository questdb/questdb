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

import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.griffin.QueryRegistry;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
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

    @Test
    public void testCancelReturnsFalseForRetiredId() throws Exception {
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
}
