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

package io.questdb.test.cairo.mv;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.mv.MatViewTimerJob;
import io.questdb.cairo.mv.MatViewTimerTask;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.mp.ConcurrentQueue;
import io.questdb.mp.Queue;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@RunWith(Parameterized.class)
public class MatViewTimerJobTest extends AbstractCairoTest {
    private final TestTimestampType timestampType;

    public MatViewTimerJobTest(TestTimestampType timestampType) {
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<TestTimestampType> testParams() {
        return List.of(TestTimestampType.MICRO, TestTimestampType.NANO);
    }

    @Test
    public void testAddAfterDropRemoveBeforeGraphRemoval() throws Exception {
        testRegistrationAfterDrop(false, false);
    }

    @Test
    public void testDroppedStateDoesNotRegisterTimers() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable(engine, sqlExecutionContext);
            execute("CREATE MATERIALIZED VIEW mv REFRESH EVERY 1m DEFERRED START '1970-01-02' AS SELECT ts, last(price) price FROM base SAMPLE BY 1h");
            final TableToken token = engine.verifyTableName("mv");
            final MatViewState state = engine.getMatViewStateStore().getViewState(token);
            state.markAsDropped();
            final MatViewTimerJob timer = new MatViewTimerJob(engine);
            timer.run();
            Assert.assertEquals(0, timer.getTimerCount());
            engine.getMatViewTimerQueue().enqueue(new MatViewTimerTask().ofUpdate(token));
            timer.run();
            Assert.assertEquals(0, timer.getTimerCount());
            Assert.assertEquals(0, state.getRegisteredTimerCount());
        });
    }

    @Test
    public void testExpiryDiscardsDroppedStateTimers() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable(engine, sqlExecutionContext);
            execute("CREATE MATERIALIZED VIEW mv REFRESH MANUAL AS SELECT ts, last(price) price FROM base SAMPLE BY 1h");
            final MatViewTimerJob timer = new MatViewTimerJob(engine);
            timer.run();
            final MatViewState state = engine.getMatViewStateStore().getViewState(engine.verifyTableName("mv"));
            Assert.assertEquals(1, state.getRegisteredTimerCount());
            state.markAsDropped();
            currentMicros += Micros.DAY_MICROS;
            timer.run();
            Assert.assertEquals(0, timer.getTimerCount());
            Assert.assertEquals(0, state.getRegisteredTimerCount());
        });
    }

    @Test
    public void testExpiryDiscardsMissingStateBeforeRemovePublication() throws Exception {
        assertMemoryLeak(() -> {
            final GatedQueue queue = new GatedQueue();
            try (CairoEngine testEngine = createEngine(queue);
                 SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(testEngine, 1)) {
                testEngine.load();
                createBaseTable(testEngine, context);
                testEngine.execute("CREATE MATERIALIZED VIEW mv REFRESH EVERY 1m DEFERRED PERIOD (LENGTH 1h) AS SELECT ts, last(price) price FROM base SAMPLE BY 1h", context);
                final TableToken token = testEngine.verifyTableName("mv");
                final MatViewTimerJob timer = new MatViewTimerJob(testEngine);
                timer.run();
                Assert.assertEquals(3, timer.getTimerCount());
                final Gates gates = new Gates(true);
                queue.gates = gates;
                final AtomicReference<Throwable> error = new AtomicReference<>();
                final Thread drop = dropThread(testEngine, error);
                try {
                    drop.start();
                    await(gates.removeEntered);
                    Assert.assertNull(testEngine.getMatViewStateStore().getViewState(token));
                    Assert.assertNotNull(testEngine.getDependentViewGraph().getViewDefinition(token));
                    currentMicros += Micros.DAY_MICROS;
                    timer.run();
                    Assert.assertEquals("missing-state expiry must retire all timer types", 0, timer.getTimerCount());
                } finally {
                    gates.allowRemove.countDown();
                    TestUtils.joinThreads(drop);
                    assertNoThreadError(error);
                }
                timer.run();
                currentMicros += Micros.DAY_MICROS;
                Assert.assertFalse(timer.run());
            }
        });
    }

    @Test
    public void testInvalidStateRetainsTimersThroughRecovery() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable(engine, sqlExecutionContext);
            execute("CREATE MATERIALIZED VIEW mv REFRESH EVERY 1m DEFERRED PERIOD (LENGTH 1h) AS SELECT ts, last(price) price FROM base SAMPLE BY 1h");
            final TableToken token = engine.verifyTableName("mv");
            final MatViewState state = engine.getMatViewStateStore().getViewState(token);
            final MatViewTimerJob timer = new MatViewTimerJob(engine);

            // Pending invalidation suppresses dispatch, not registration or the next deadline.
            state.markAsPendingInvalidation("test invalidation");
            timer.run();
            Assert.assertEquals(3, timer.getTimerCount());
            currentMicros += Micros.DAY_MICROS;
            timer.run();
            Assert.assertEquals(3, timer.getTimerCount());
            Assert.assertEquals(3, state.getRegisteredTimerCount());

            Assert.assertTrue(state.tryLock());
            try {
                state.clearPendingInvalidationForTesting();
                state.markAsInvalid("test invalidation");
            } finally {
                state.unlock();
            }
            engine.getMatViewTimerQueue().enqueue(new MatViewTimerTask().ofUpdate(token));
            timer.run();
            Assert.assertEquals(3, timer.getTimerCount());
            currentMicros += Micros.DAY_MICROS;
            timer.run();
            Assert.assertEquals(3, timer.getTimerCount());
            Assert.assertEquals(3, state.getRegisteredTimerCount());

            execute("INSERT INTO base VALUES ('1970-01-01T01:00:00', 42)");
            drainWalAndMatViewQueues();
            execute("REFRESH MATERIALIZED VIEW mv FULL");
            drainWalAndMatViewQueues();
            Assert.assertFalse(state.isInvalid());
            assertQuery("SELECT price FROM mv").expectSize().noLeakCheck().returns("price\n42.0\n");

            // The retained schedule must pick up later data without another ADD or UPDATE.
            execute("INSERT INTO base VALUES ('1970-01-01T02:00:00', 43)");
            drainWalAndMatViewQueues();
            currentMicros += Micros.DAY_MICROS;
            timer.run();
            drainWalAndMatViewQueues();
            Assert.assertEquals(3, timer.getTimerCount());
            Assert.assertEquals(3, state.getRegisteredTimerCount());
            assertQuery("SELECT price FROM mv ORDER BY ts").expectSize().noLeakCheck().returns("price\n42.0\n43.0\n");
        });
    }

    @Test
    public void testUpdateDroppedStateDoesNotRegisterTimers() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable(engine, sqlExecutionContext);
            execute("CREATE MATERIALIZED VIEW mv REFRESH EVERY 1m DEFERRED START '1970-01-02' AS SELECT ts, last(price) price FROM base SAMPLE BY 1h");
            final TableToken token = engine.verifyTableName("mv");
            final MatViewState state = engine.getMatViewStateStore().getViewState(token);
            final Queue<MatViewTimerTask> queue = engine.getMatViewTimerQueue();
            final MatViewTimerTask delayedAdd = new MatViewTimerTask();
            Assert.assertTrue(queue.tryDequeue(delayedAdd));
            Assert.assertEquals(MatViewTimerTask.ADD, delayedAdd.getOperation());
            Assert.assertEquals(token, delayedAdd.getMatViewToken());
            final MatViewTimerJob timer = new MatViewTimerJob(engine);
            Assert.assertEquals(0, timer.getTimerCount());
            Assert.assertEquals(0, state.getRegisteredTimerCount());

            // With no timers to remove, UPDATE makes addTimers the first getter of dropped state.
            state.markAsDropped();
            queue.enqueue(new MatViewTimerTask().ofUpdate(token));
            timer.run();
            Assert.assertEquals("UPDATE must reject non-null dropped state", 0, timer.getTimerCount());
            Assert.assertEquals(0, state.getRegisteredTimerCount());
        });
    }

    @Test
    public void testUpdateAfterCompletedDrop() throws Exception {
        testRegistrationAfterDrop(true, true);
    }

    @Test
    public void testUpdateAfterDropRemoveBeforeGraphRemoval() throws Exception {
        testRegistrationAfterDrop(true, false);
    }

    private static void assertNoThreadError(AtomicReference<Throwable> error) {
        if (error.get() != null) {
            throw new AssertionError("lifecycle thread failed", error.get());
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            Assert.assertTrue("timer lifecycle coordination timeout", latch.await(10, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private static Thread dropThread(CairoEngine testEngine, AtomicReference<Throwable> error) {
        return new Thread(() -> {
            try (SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(testEngine, 1)) {
                testEngine.execute("DROP MATERIALIZED VIEW mv", context);
            } catch (Throwable th) {
                error.compareAndSet(null, th);
            } finally {
                Path.clearThreadLocals();
            }
        }, "mat-view-drop");
    }

    private void createBaseTable(CairoEngine testEngine, SqlExecutionContext context) throws Exception {
        currentMicros = Micros.MINUTE_MICROS;
        testEngine.execute("CREATE TABLE base (ts " + timestampType.getTypeName() + ", price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL", context);
    }

    private CairoEngine createEngine(GatedQueue queue) throws Exception {
        return new CairoEngine(new DefaultTestCairoConfiguration(temp.newFolder().getAbsolutePath()) {
            @Override
            public MicrosecondClock getMicrosecondClock() {
                return () -> currentMicros;
            }
        }) {
            @Override
            protected Queue<MatViewTimerTask> createMatViewTimerQueue() {
                return queue;
            }
        };
    }

    private void testRegistrationAfterDrop(boolean isUpdate, boolean isDropCompletedBeforeTick) throws Exception {
        assertMemoryLeak(() -> {
            final GatedQueue queue = new GatedQueue();
            try (CairoEngine testEngine = createEngine(queue);
                 SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(testEngine, 1)) {
                testEngine.load();
                createBaseTable(testEngine, context);
                final MatViewTimerJob timer = new MatViewTimerJob(testEngine);
                TableToken previousToken = null;
                for (int episode = 0; episode < 3; episode++) {
                    testEngine.execute("CREATE MATERIALIZED VIEW mv REFRESH " + (isUpdate ? "IMMEDIATE" : "EVERY 1m DEFERRED START '1970-01-02'") + " AS SELECT ts, last(price) price FROM base SAMPLE BY 1h", context);
                    final TableToken token = testEngine.verifyTableName("mv");
                    if (previousToken != null) {
                        Assert.assertNotEquals(previousToken.getDirName(), token.getDirName());
                    }
                    final MatViewTimerTask delayedAdd = new MatViewTimerTask();
                    if (!isUpdate) {
                        // Hold CREATE's real ADD, just as a publisher paused after installing state would.
                        Assert.assertTrue(queue.tryDequeue(delayedAdd));
                        Assert.assertEquals(MatViewTimerTask.ADD, delayedAdd.getOperation());
                    }
                    drainWalAndMatViewQueues(testEngine);
                    timer.run();
                    Assert.assertEquals(0, timer.getTimerCount());
                    final Gates gates = new Gates(false);
                    queue.gates = gates;
                    final AtomicReference<Throwable> error = new AtomicReference<>();
                    final Thread apply = new Thread(() -> {
                        try {
                            drainWalQueue(testEngine);
                        } catch (Throwable th) {
                            error.compareAndSet(null, th);
                        } finally {
                            Path.clearThreadLocals();
                        }
                    }, "mat-view-alter-apply");
                    final Thread drop = dropThread(testEngine, error);
                    try {
                        if (isUpdate) {
                            testEngine.execute("ALTER MATERIALIZED VIEW mv SET REFRESH EVERY 1m START '1970-01-02';", context);
                            apply.start();
                            await(gates.updateEntered);
                        }
                        drop.start();
                        await(gates.removePublished);
                        Assert.assertNull(testEngine.getMatViewStateStore().getViewState(token));
                        Assert.assertNotNull(testEngine.getDependentViewGraph().getViewDefinition(token));
                        // Consume REMOVE before the stale registration reaches the consumer.
                        timer.run();
                        if (isUpdate) {
                            gates.allowUpdate.countDown();
                            await(gates.updatePublished);
                        } else {
                            queue.enqueue(delayedAdd);
                        }
                        if (isDropCompletedBeforeTick) {
                            gates.allowRemove.countDown();
                            TestUtils.joinThreads(drop);
                            assertNoThreadError(error);
                            Assert.assertNull(testEngine.getDependentViewGraph().getViewDefinition(token));
                        }
                        timer.run();
                        Assert.assertEquals("stale registration must not retain timers", 0, timer.getTimerCount());
                    } finally {
                        gates.allowUpdate.countDown();
                        gates.allowRemove.countDown();
                        try {
                            TestUtils.joinThreads(apply, drop);
                            assertNoThreadError(error);
                        } finally {
                            queue.gates = null;
                        }
                    }
                    for (int tick = 0; tick < 3; tick++) {
                        currentMicros += Micros.MINUTE_MICROS;
                        Assert.assertFalse(timer.run());
                        Assert.assertEquals(0, timer.getTimerCount());
                    }
                    previousToken = token;
                }
            }
        });
    }

    private static class GatedQueue implements Queue<MatViewTimerTask> {
        private final Queue<MatViewTimerTask> delegate = ConcurrentQueue.createConcurrentQueue(MatViewTimerTask.ITEM_FACTORY);
        private volatile Gates gates;

        @Override
        public void clear() {
            delegate.clear();
        }

        @Override
        public void enqueue(MatViewTimerTask task) {
            final Gates active = gates;
            if (active != null && task.getOperation() == MatViewTimerTask.UPDATE) {
                active.updateEntered.countDown();
                await(active.allowUpdate);
                delegate.enqueue(task);
                active.updatePublished.countDown();
            } else if (active != null && task.getOperation() == MatViewTimerTask.REMOVE) {
                active.removeEntered.countDown();
                if (active.isRemoveBlockedBeforePublication) {
                    await(active.allowRemove);
                }
                delegate.enqueue(task);
                active.removePublished.countDown();
                if (!active.isRemoveBlockedBeforePublication) {
                    await(active.allowRemove);
                }
            } else {
                delegate.enqueue(task);
            }
        }

        @Override
        public boolean tryDequeue(MatViewTimerTask target) {
            return delegate.tryDequeue(target);
        }
    }

    private static class Gates {
        private final CountDownLatch allowRemove = new CountDownLatch(1);
        private final CountDownLatch allowUpdate = new CountDownLatch(1);
        private final boolean isRemoveBlockedBeforePublication;
        private final CountDownLatch removeEntered = new CountDownLatch(1);
        private final CountDownLatch removePublished = new CountDownLatch(1);
        private final CountDownLatch updateEntered = new CountDownLatch(1);
        private final CountDownLatch updatePublished = new CountDownLatch(1);

        private Gates(boolean isRemoveBlockedBeforePublication) {
            this.isRemoveBlockedBeforePublication = isRemoveBlockedBeforePublication;
        }
    }
}
