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

package io.questdb.test.cairo.sql.async;

import io.questdb.MessageBus;
import io.questdb.Metrics;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.async.QueryParallelFiberDispatcher;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.groupby.GroupByLongTopKJob;
import io.questdb.griffin.engine.groupby.GroupByMergeShardJob;
import io.questdb.griffin.engine.orderby.LongTopKRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncGroupByRecordCursorFactory;
import io.questdb.griffin.engine.table.GroupByShardingContext;
import io.questdb.griffin.engine.table.LatestByAllIndexedRecordCursorFactory;
import io.questdb.mp.MCSequence;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.mp.continuation.SuspensionScope;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.tasks.GroupByLongTopKTask;
import io.questdb.tasks.GroupByMergeShardTask;
import io.questdb.tasks.LatestByTask;
import io.questdb.test.AbstractTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class QueryParallelFiberDispatcherTest extends AbstractTest {

    @Test
    public void testAdapterCreationFailureReleasesFiberReservation() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root);
            try (CairoEngine engine = new CairoEngine(configuration)) {
                final FiberRuntime runtime = new FiberRuntime(1);
                final QueryParallelFiberDispatcher dispatcher = new QueryParallelFiberDispatcher(
                        engine,
                        engine.getMessageBus(),
                        runtime
                );
                try {
                    final MessageBus messageBus = engine.getMessageBus();
                    final AtomicBooleanCircuitBreaker circuitBreaker = new AtomicBooleanCircuitBreaker(engine);
                    final AtomicInteger startedCounter = new AtomicInteger();
                    final SOUnboundedCountDownLatch doneLatch = new SOUnboundedCountDownLatch();
                    doneLatch.reset();

                    final MPSequence pubSeq = messageBus.getGroupByMergeShardPubSeq();
                    final long cursor = pubSeq.next();
                    Assert.assertTrue(cursor > -1);
                    final GroupByMergeShardTask task = messageBus.getGroupByMergeShardQueue().get(cursor);
                    task.of(circuitBreaker, startedCounter, doneLatch, null, 0);
                    pubSeq.done(cursor);

                    final RuntimeException injected = new RuntimeException("injected adapter creation failure");
                    dispatcher.setBeforeMergeShardTaskCreationForTesting(() -> {
                        throw injected;
                    });
                    try {
                        dispatcher.consumeMergeShard(-1);
                        Assert.fail("expected injected adapter creation failure");
                    } catch (RuntimeException th) {
                        Assert.assertSame(injected, th);
                    }
                    Assert.assertEquals(0, runtime.getOutstandingTaskCount());
                    final Fiber fiber = runtime.tryReserveFiber();
                    Assert.assertNotNull(fiber);
                    runtime.releaseReservedFiber(fiber, fiber.getReservationEpoch());
                } finally {
                    Misc.free(dispatcher);
                    if (runtime.getOutstandingTaskCount() == 0) {
                        closeRuntime(runtime);
                    }
                }
            }
        });
    }

    @Test
    public void testAllParallelConsumersRunAsFibers() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public long getGroupByParallelTopKThreshold() {
                    return 0;
                }

                @Override
                public int getGroupByShardingThreshold() {
                    return 1;
                }

                @Override
                public int getSqlPageFrameMaxRows() {
                    return 128;
                }

                @Override
                public int getSqlPageFrameMinRows() {
                    return 1;
                }

                @Override
                public int getVectorAggregateQueueCapacity() {
                    return 2;
                }
            };
            final TestWorkerPool pool = new TestWorkerPool(
                    "query-parallel-fiber-test",
                    4,
                    Metrics.DISABLED,
                    WorkerPoolMode.FIBER_HOST
            );
            TestUtils.execute(
                    pool,
                    (engine, compiler, executionContext) -> {
                        final QueryParallelFiberDispatcher dispatcher =
                                engine.getMessageBus().getQueryParallelFiberDispatcher();
                        Assert.assertNotNull(dispatcher);

                        engine.execute(
                                "create table latest_tab as (" +
                                        "select x id, ('k' || (x % 64))::symbol sym, " +
                                        "timestamp_sequence(0, 1_000_000) ts from long_sequence(4096)" +
                                        "), index(sym) timestamp(ts) partition by day",
                                executionContext
                        );
                        runAsFiberOwner(engine, "query-owner-latest-by-test", () -> Assert.assertEquals(
                                64,
                                drain(compiler, executionContext, "select * from latest_tab latest on ts partition by sym")
                        ));
                        Assert.assertTrue(dispatcher.getLatestByCreatedTaskCount() > 0);

                        engine.execute(
                                "create table vector_tab as (" +
                                        "select (x % 17)::int k, x v from long_sequence(262_144)" +
                                        ")",
                                executionContext
                        );
                        runAsFiberOwner(engine, "query-owner-vector-test", () -> Assert.assertEquals(
                                17,
                                drain(compiler, executionContext, "select k, sum(v) from vector_tab")
                        ));
                        Assert.assertTrue(dispatcher.getVectorAggregateCreatedTaskCount() > 0);
                        assertFiberOwnerParks(engine, compiler, executionContext);
                        assertSameRuntimeOwnerRunsLocally(pool, engine, compiler, executionContext, dispatcher);

                        engine.execute(
                                "create table group_tab as (" +
                                        "select timestamp_sequence(0, 1_000_000) ts, " +
                                        "'k' || (x % 64) key, x::double price, x quantity " +
                                        "from long_sequence(4096)" +
                                        ") timestamp(ts) partition by day",
                                executionContext
                        );
                        runAsFiberOwner(engine, "query-owner-group-test", () -> Assert.assertEquals(
                                10,
                                drain(
                                        compiler,
                                        executionContext,
                                        "select quantity, max(price) from group_tab order by quantity asc limit 10"
                                )
                        ));
                        Assert.assertTrue(dispatcher.getMergeShardCreatedTaskCount() > 0);
                        Assert.assertTrue(dispatcher.getLongTopKCreatedTaskCount() > 0);
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testLatestByOwnerHelpsWithoutConsumers() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public int getLatestByQueueCapacity() {
                    return 1;
                }

                @Override
                public int getSqlPageFrameMaxRows() {
                    return 128;
                }

                @Override
                public int getSqlPageFrameMinRows() {
                    return 1;
                }
            };
            final WorkerPoolConfiguration poolConfiguration = new WorkerPoolConfiguration() {
                @Override
                public Metrics getMetrics() {
                    return Metrics.DISABLED;
                }

                @Override
                public String getPoolName() {
                    return "zero-consumer-latest-by-test";
                }

                @Override
                public int getWorkerCount() {
                    return 0;
                }

                @Override
                public WorkerPoolMode getWorkerPoolMode() {
                    return WorkerPoolMode.FIBER_HOST;
                }
            };
            final TestWorkerPool pool = new TestWorkerPool(poolConfiguration);
            try (CairoEngine engine = new CairoEngine(configuration);
                 SqlCompiler compiler = engine.getSqlCompiler();
                 SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine, 2)) {
                TestUtils.setupWorkerPool(pool, engine);
                pool.start(LOG);
                final QueryParallelFiberDispatcher dispatcher = engine.getMessageBus().getQueryParallelFiberDispatcher();
                Assert.assertNotNull(dispatcher);
                engine.execute(
                        """
                                CREATE TABLE tab AS (
                                  SELECT (x * 1_000_000L)::timestamp ts, ('s' || (x % 64))::symbol sym, x v
                                  FROM long_sequence(512)
                                ), INDEX(sym) TIMESTAMP(ts)""",
                        executionContext
                );
                try (RecordCursorFactory factory = compiler.compile("SELECT * FROM tab LATEST ON ts PARTITION BY sym", executionContext)
                        .getRecordCursorFactory()) {
                    assertInTree(factory, LatestByAllIndexedRecordCursorFactory.class);
                }

                final MessageBus messageBus = engine.getMessageBus();
                final MCSequence subSeq = messageBus.getLatestBySubSeq();
                final long consumedBefore = subSeq.current();
                final AtomicReference<Throwable> ownerFailure = new AtomicReference<>();
                final CountDownLatch ownerDone = new CountDownLatch(1);
                final AtomicInteger rowCount = new AtomicInteger();
                final Thread owner = new Thread(() -> {
                    try (RecordCursorFactory factory = engine.select("SELECT * FROM tab LATEST ON ts PARTITION BY sym", executionContext);
                         RecordCursor cursor = factory.getCursor(executionContext)) {
                        while (cursor.hasNext()) {
                            rowCount.incrementAndGet();
                        }
                    } catch (Throwable th) {
                        ownerFailure.set(th);
                    } finally {
                        ownerDone.countDown();
                    }
                }, "latest-by-owner");
                owner.setDaemon(true);
                boolean completed = false;
                boolean publishedTaskConsumed = false;
                InterruptedException interrupted = null;
                owner.start();
                try {
                    completed = ownerDone.await(10, TimeUnit.SECONDS);
                    publishedTaskConsumed = subSeq.current() > consumedBefore;
                } catch (InterruptedException e) {
                    interrupted = e;
                } finally {
                    if (ownerDone.getCount() > 0) {
                        executionContext.getCircuitBreaker().cancel();
                        final RingQueue<LatestByTask> queue = messageBus.getLatestByQueue();
                        while (ownerDone.getCount() > 0) {
                            final long cursor = subSeq.next();
                            if (cursor > -1) {
                                try {
                                    queue.get(cursor).run();
                                } catch (Throwable th) {
                                    ownerFailure.compareAndSet(null, th);
                                } finally {
                                    subSeq.done(cursor);
                                }
                            } else {
                                Os.pause();
                            }
                        }
                    }
                    while (owner.isAlive()) {
                        try {
                            owner.join();
                        } catch (InterruptedException e) {
                            if (interrupted == null) {
                                interrupted = e;
                            }
                            executionContext.getCircuitBreaker().cancel();
                        }
                    }
                    if (interrupted != null) {
                        Thread.currentThread().interrupt();
                    }
                }
                if (interrupted != null) {
                    throw interrupted;
                }
                Assert.assertTrue("latest-by owner did not finish", completed);
                Assert.assertTrue("owner did not consume a published latest-by task", publishedTaskConsumed);
                Assert.assertEquals(64, rowCount.get());
                Assert.assertNull(ownerFailure.get());
                Assert.assertEquals(0, dispatcher.getLatestByCreatedTaskCount());
            } finally {
                pool.haltAndAssertCleanForTest(WorkerPool.DEFAULT_HALT_TIMEOUT_NANOS);
            }
        });
    }

    @Test
    public void testMergeShardOwnerHelpsWithoutConsumers() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public int getGroupByShardingThreshold() {
                    return 1;
                }

                @Override
                public int getGroupByMergeShardQueueCapacity() {
                    return 1;
                }

                @Override
                public int getSqlPageFrameMaxRows() {
                    return 128;
                }

                @Override
                public int getSqlPageFrameMinRows() {
                    return 1;
                }
            };
            final WorkerPoolConfiguration poolConfiguration = new WorkerPoolConfiguration() {
                @Override
                public Metrics getMetrics() {
                    return Metrics.DISABLED;
                }

                @Override
                public String getPoolName() {
                    return "zero-consumer-merge-shard-test";
                }

                @Override
                public int getWorkerCount() {
                    return 0;
                }

                @Override
                public WorkerPoolMode getWorkerPoolMode() {
                    return WorkerPoolMode.FIBER_HOST;
                }
            };
            final TestWorkerPool pool = new TestWorkerPool(poolConfiguration);
            try (CairoEngine engine = new CairoEngine(configuration);
                 SqlCompiler compiler = engine.getSqlCompiler();
                 SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine, 1)) {
                TestUtils.setupWorkerPool(pool, engine);
                pool.start(LOG);
                final QueryParallelFiberDispatcher dispatcher = engine.getMessageBus().getQueryParallelFiberDispatcher();
                Assert.assertNotNull(dispatcher);
                engine.execute(
                        "create table tab as (select ('k' || x) key, x v from long_sequence(128))",
                        executionContext
                );
                try (RecordCursorFactory factory = compiler.compile("select key, max(v) m from tab", executionContext)
                        .getRecordCursorFactory()) {
                    assertInTree(factory, AsyncGroupByRecordCursorFactory.class);
                }

                final MessageBus messageBus = engine.getMessageBus();
                final MCSequence subSeq = messageBus.getGroupByMergeShardSubSeq();
                final long consumedBefore = subSeq.current();
                final AtomicReference<Throwable> ownerFailure = new AtomicReference<>();
                final CountDownLatch ownerDone = new CountDownLatch(1);
                final AtomicInteger rowCount = new AtomicInteger();
                final Thread owner = new Thread(() -> {
                    try (RecordCursorFactory factory = engine.select("select key, max(v) m from tab", executionContext);
                         RecordCursor cursor = factory.getCursor(executionContext)) {
                        while (cursor.hasNext()) {
                            rowCount.incrementAndGet();
                        }
                    } catch (Throwable th) {
                        ownerFailure.set(th);
                    } finally {
                        ownerDone.countDown();
                    }
                }, "merge-shard-owner");
                owner.setDaemon(true);
                boolean completed = false;
                boolean publishedTaskConsumed = false;
                InterruptedException interrupted = null;
                owner.start();
                try {
                    completed = ownerDone.await(10, TimeUnit.SECONDS);
                    publishedTaskConsumed = subSeq.current() > consumedBefore;
                } catch (InterruptedException e) {
                    interrupted = e;
                } finally {
                    if (ownerDone.getCount() > 0) {
                        executionContext.getCircuitBreaker().cancel();
                        final RingQueue<GroupByMergeShardTask> queue = messageBus.getGroupByMergeShardQueue();
                        while (ownerDone.getCount() > 0) {
                            final long cursor = subSeq.next();
                            if (cursor > -1) {
                                final GroupByMergeShardTask task = queue.get(cursor);
                                final GroupByShardingContext context = task.getShardingContext();
                                try {
                                    GroupByMergeShardJob.run(-1, task, subSeq, cursor, context);
                                } catch (Throwable th) {
                                    ownerFailure.compareAndSet(null, th);
                                }
                            } else {
                                Os.pause();
                            }
                        }
                    }
                    while (owner.isAlive()) {
                        try {
                            owner.join();
                        } catch (InterruptedException e) {
                            if (interrupted == null) {
                                interrupted = e;
                            }
                            executionContext.getCircuitBreaker().cancel();
                        }
                    }
                    if (interrupted != null) {
                        Thread.currentThread().interrupt();
                    }
                }
                if (interrupted != null) {
                    throw interrupted;
                }
                Assert.assertTrue("merge-shard owner did not finish", completed);
                Assert.assertTrue("owner did not consume a published merge shard", publishedTaskConsumed);
                Assert.assertEquals(128, rowCount.get());
                Assert.assertNull(ownerFailure.get());
                Assert.assertEquals(0, dispatcher.getMergeShardCreatedTaskCount());
            } finally {
                pool.haltAndAssertCleanForTest(WorkerPool.DEFAULT_HALT_TIMEOUT_NANOS);
            }
        });
    }

    @Test
    public void testLongTopKOwnerHelpsWithoutConsumers() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public int getGroupByShardingThreshold() {
                    return 1;
                }

                @Override
                public int getGroupByMergeShardQueueCapacity() {
                    return 1;
                }

                @Override
                public int getGroupByTopKQueueCapacity() {
                    return 1;
                }

                @Override
                public long getGroupByParallelTopKThreshold() {
                    return 0;
                }

                @Override
                public int getSqlPageFrameMaxRows() {
                    return 128;
                }

                @Override
                public int getSqlPageFrameMinRows() {
                    return 1;
                }
            };
            final WorkerPoolConfiguration poolConfiguration = new WorkerPoolConfiguration() {
                @Override
                public Metrics getMetrics() {
                    return Metrics.DISABLED;
                }

                @Override
                public String getPoolName() {
                    return "zero-consumer-long-top-k-test";
                }

                @Override
                public int getWorkerCount() {
                    return 0;
                }

                @Override
                public WorkerPoolMode getWorkerPoolMode() {
                    return WorkerPoolMode.FIBER_HOST;
                }
            };
            final TestWorkerPool pool = new TestWorkerPool(poolConfiguration);
            try (CairoEngine engine = new CairoEngine(configuration);
                 SqlCompiler compiler = engine.getSqlCompiler();
                 SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine, 1)) {
                TestUtils.setupWorkerPool(pool, engine);
                pool.start(LOG);
                final QueryParallelFiberDispatcher dispatcher =
                        engine.getMessageBus().getQueryParallelFiberDispatcher();
                Assert.assertNotNull(dispatcher);
                engine.execute(
                        "create table tab as (select ('k' || x) key, x v from long_sequence(128))",
                        executionContext
                );
                try (RecordCursorFactory factory = compiler.compile(
                        "select key, max(v) m from tab order by m desc limit 10",
                        executionContext
                ).getRecordCursorFactory()) {
                    assertInTree(factory, LongTopKRecordCursorFactory.class);
                    assertInTree(factory, AsyncGroupByRecordCursorFactory.class);
                }

                final MessageBus messageBus = engine.getMessageBus();
                final MCSequence subSeq = messageBus.getGroupByLongTopKSubSeq();
                final long consumedBefore = subSeq.current();
                final AtomicReference<Throwable> ownerFailure = new AtomicReference<>();
                final CountDownLatch ownerDone = new CountDownLatch(1);
                final AtomicInteger rowCount = new AtomicInteger();
                final AtomicBoolean exactOutput = new AtomicBoolean(true);
                final Thread owner = new Thread(() -> {
                    try (RecordCursorFactory factory = engine.select(
                            "select key, max(v) m from tab order by m desc limit 10",
                            executionContext
                    );
                         RecordCursor cursor = factory.getCursor(executionContext)) {
                        while (cursor.hasNext()) {
                            final int row = rowCount.getAndIncrement();
                            exactOutput.compareAndSet(
                                    true,
                                    row < 10
                                            && ("k" + (128 - row)).contentEquals(cursor.getRecord().getStrA(0))
                                            && cursor.getRecord().getLong(1) == 128 - row
                            );
                        }
                    } catch (Throwable th) {
                        ownerFailure.set(th);
                    } finally {
                        ownerDone.countDown();
                    }
                }, "long-top-k-owner");
                owner.setDaemon(true);
                boolean completed = false;
                boolean publishedTaskConsumed = false;
                InterruptedException interrupted = null;
                owner.start();
                try {
                    completed = ownerDone.await(10, TimeUnit.SECONDS);
                    publishedTaskConsumed = subSeq.current() > consumedBefore;
                } catch (InterruptedException e) {
                    interrupted = e;
                } finally {
                    if (ownerDone.getCount() > 0) {
                        executionContext.getCircuitBreaker().cancel();
                        final RingQueue<GroupByMergeShardTask> mergeQueue = messageBus.getGroupByMergeShardQueue();
                        final RingQueue<GroupByLongTopKTask> topKQueue = messageBus.getGroupByLongTopKQueue();
                        final MCSequence mergeSubSeq = messageBus.getGroupByMergeShardSubSeq();
                        while (ownerDone.getCount() > 0) {
                            final long mergeCursor = mergeSubSeq.next();
                            if (mergeCursor > -1) {
                                final GroupByMergeShardTask task = mergeQueue.get(mergeCursor);
                                final GroupByShardingContext context = task.getShardingContext();
                                try {
                                    GroupByMergeShardJob.run(-1, task, mergeSubSeq, mergeCursor, context);
                                } catch (Throwable th) {
                                    ownerFailure.compareAndSet(null, th);
                                }
                            }
                            final long topKCursor = subSeq.next();
                            if (topKCursor > -1) {
                                final GroupByLongTopKTask task = topKQueue.get(topKCursor);
                                try {
                                    GroupByLongTopKJob.run(-1, task, subSeq, topKCursor, task.getAtom());
                                } catch (Throwable th) {
                                    ownerFailure.compareAndSet(null, th);
                                }
                            }
                            if (mergeCursor < 0 && topKCursor < 0) {
                                Os.pause();
                            }
                        }
                    }
                    while (owner.isAlive()) {
                        try {
                            owner.join();
                        } catch (InterruptedException e) {
                            if (interrupted == null) {
                                interrupted = e;
                            }
                            executionContext.getCircuitBreaker().cancel();
                        }
                    }
                    if (interrupted != null) {
                        Thread.currentThread().interrupt();
                    }
                }
                if (interrupted != null) {
                    throw interrupted;
                }
                Assert.assertTrue("long top-K owner did not finish", completed);
                Assert.assertTrue("owner did not consume a published long top-K task", publishedTaskConsumed);
                Assert.assertTrue(exactOutput.get());
                Assert.assertEquals(10, rowCount.get());
                Assert.assertNull(ownerFailure.get());
                Assert.assertEquals(0, dispatcher.getLongTopKCreatedTaskCount());
            } finally {
                pool.haltAndAssertCleanForTest(WorkerPool.DEFAULT_HALT_TIMEOUT_NANOS);
            }
        });
    }

    @Test
    public void testQuiesceDrainFailsParkedLatestByOwner() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root);
            try (
                    CairoEngine engine = new CairoEngine(configuration);
                    SqlCompiler compiler = engine.getSqlCompiler();
                    SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)
            ) {
                engine.execute(
                        "create table latest_tab as (" +
                                "select x id, ('k' || (x % 64))::symbol sym, " +
                                "timestamp_sequence(0, 1_000_000) ts from long_sequence(4096)" +
                                "), index(sym) timestamp(ts) partition by day",
                        executionContext
                );
                assertOwnerCancelledByQuiesceDrain(
                        engine,
                        compiler,
                        executionContext,
                        "owner-latest-by-quiesce",
                        "select * from latest_tab latest on ts partition by sym",
                        LatestByAllIndexedRecordCursorFactory.class
                );
            }
        });
    }

    @Test
    public void testQuiesceDrainFailsParkedVectorAggregateOwner() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root);
            try (
                    CairoEngine engine = new CairoEngine(configuration);
                    SqlCompiler compiler = engine.getSqlCompiler();
                    SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)
            ) {
                engine.execute(
                        "create table vector_tab as (select (x % 17)::int k, x v from long_sequence(65_536))",
                        executionContext
                );
                assertOwnerCancelledByQuiesceDrain(
                        engine,
                        compiler,
                        executionContext,
                        "owner-vector-quiesce",
                        "select k, sum(v) from vector_tab",
                        io.questdb.griffin.engine.groupby.vect.GroupByRecordCursorFactory.class
                );
            }
        });
    }

    @Test
    public void testQuiesceDrainsPublishedTaskWithActivePublisher() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root);
            try (CairoEngine engine = new CairoEngine(configuration)) {
                final FiberRuntime runtime = new FiberRuntime(1);
                final QueryParallelFiberDispatcher dispatcher = new QueryParallelFiberDispatcher(
                        engine,
                        engine.getMessageBus(),
                        runtime
                );
                try {
                    final MessageBus messageBus = engine.getMessageBus();
                    final AtomicBooleanCircuitBreaker circuitBreaker = new AtomicBooleanCircuitBreaker(engine);
                    final AtomicInteger startedCounter = new AtomicInteger();
                    final SOUnboundedCountDownLatch doneLatch = new SOUnboundedCountDownLatch();
                    doneLatch.reset();

                    Assert.assertTrue(dispatcher.tryAcquirePublication());
                    try {
                        final MPSequence pubSeq = messageBus.getGroupByMergeShardPubSeq();
                        final long cursor = pubSeq.next();
                        Assert.assertTrue(cursor > -1);
                        final GroupByMergeShardTask task = messageBus.getGroupByMergeShardQueue().get(cursor);
                        task.of(circuitBreaker, startedCounter, doneLatch, null, 0);
                        pubSeq.done(cursor);

                        dispatcher.beginQuiesce();
                        dispatcher.progressQuiesce();

                        Assert.assertTrue(circuitBreaker.checkIfTripped());
                        Assert.assertEquals(1, startedCounter.get());
                        Assert.assertTrue(doneLatch.done(1));
                        Assert.assertFalse(dispatcher.isQuiesced());
                    } finally {
                        dispatcher.releasePublication();
                    }

                    dispatcher.progressQuiesce();
                    Assert.assertTrue(dispatcher.isQuiesced());
                } finally {
                    closeRuntime(runtime);
                    Misc.free(dispatcher);
                }
            }
        });
    }

    private static void assertFiberOwnerParks(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext executionContext
    ) throws Exception {
        final FiberRuntime runtime = runAsFiberOwner(
                engine,
                "query-owner-fiber-test",
                () -> Assert.assertEquals(17, drain(compiler, executionContext, "select k, sum(v) from vector_tab"))
        );
        Assert.assertTrue(runtime.getMountCount() > 1);
    }

    // A fiber owner parks on the dispatcher instead of stealing, so every published task is left
    // for the query pool to consume. An ordinary owner helps out and can drain the queue itself,
    // which makes the dispatcher task counters racy.
    private static FiberRuntime runAsFiberOwner(
            CairoEngine engine,
            String poolName,
            FiberOwnerBody body
    ) throws Exception {
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final CountDownLatch finished = new CountDownLatch(1);
        final AtomicBoolean launched = new AtomicBoolean();
        try (TestWorkerPool ownerPool = new TestWorkerPool(
                poolName,
                1,
                Metrics.DISABLED,
                WorkerPoolMode.FIBER_HOST
        )) {
            final FiberRuntime runtime = ownerPool.getFiberRuntime();
            final FiberTask task = new FiberTask() {
                @Override
                protected void onDone() {
                    finished.countDown();
                }

                @Override
                protected void onError(Throwable th) {
                    error.compareAndSet(null, th);
                    finished.countDown();
                }

                @Override
                protected boolean runStep() {
                    SuspensionScope.enterTimerShards(engine.getTimerShards());
                    try {
                        body.run();
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                    return true;
                }
            };
            ownerPool.assign(_ -> {
                if (launched.compareAndSet(false, true)) {
                    final LaunchResult result = runtime.launch(task);
                    if (result != LaunchResult.LAUNCHED) {
                        error.set(new AssertionError("owner fiber launch failed [result=" + result + ']'));
                        finished.countDown();
                    }
                    return true;
                }
                return false;
            });
            ownerPool.start(LOG);
            Assert.assertTrue(finished.await(10, TimeUnit.SECONDS));
            ownerPool.haltAndAssertCleanForTest(WorkerPool.DEFAULT_HALT_TIMEOUT_NANOS);
            if (error.get() != null) {
                throw new AssertionError(error.get());
            }
            return runtime;
        }
    }

    @FunctionalInterface
    private interface FiberOwnerBody {
        void run() throws Exception;
    }

    private static long drain(
            SqlCompiler compiler,
            SqlExecutionContext executionContext,
            CharSequence sql
    ) throws Exception {
        try (RecordCursorFactory factory = compiler.compile(sql, executionContext).getRecordCursorFactory();
             RecordCursor cursor = factory.getCursor(executionContext)) {
            long count = 0;
            while (cursor.hasNext()) {
                count++;
            }
            return count;
        }
    }

    private static void assertSameRuntimeOwnerRunsLocally(
            TestWorkerPool queryPool,
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext executionContext,
            QueryParallelFiberDispatcher dispatcher
    ) throws Exception {
        final int createdTaskCount = dispatcher.getVectorAggregateCreatedTaskCount();
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final CountDownLatch finished = new CountDownLatch(1);
        final FiberTask task = new FiberTask() {
            @Override
            protected void onDone() {
                finished.countDown();
            }

            @Override
            protected void onError(Throwable th) {
                error.compareAndSet(null, th);
            }

            @Override
            protected boolean runStep() {
                SuspensionScope.enterTimerShards(engine.getTimerShards());
                try {
                    Assert.assertEquals(17, drain(compiler, executionContext, "select k, sum(v) from vector_tab"));
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
                return true;
            }
        };
        Assert.assertSame(LaunchResult.LAUNCHED, queryPool.getFiberRuntime().launch(task));
        Assert.assertTrue(finished.await(10, TimeUnit.SECONDS));
        if (error.get() != null) {
            throw new AssertionError(error.get());
        }
        Assert.assertEquals(createdTaskCount, dispatcher.getVectorAggregateCreatedTaskCount());
    }

    // The owner runs as a fiber of its own runtime, so it parks on the dispatcher instead of
    // stealing. With no consumers, its published tasks sit in the queue until the quiesce drain
    // aborts them; the owner must then fail the query instead of returning a partial result.
    private static void assertOwnerCancelledByQuiesceDrain(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext executionContext,
            String poolName,
            String sql,
            Class<?> expectedFactoryClass
    ) throws Exception {
        try (RecordCursorFactory factory = compiler.compile(sql, executionContext).getRecordCursorFactory()) {
            TestUtils.assertFactoryInTree(factory, expectedFactoryClass);
        }
        final FiberRuntime dispatcherRuntime = new FiberRuntime(1);
        final QueryParallelFiberDispatcher dispatcher = new QueryParallelFiberDispatcher(
                engine,
                engine.getMessageBus(),
                dispatcherRuntime
        );
        try {
            engine.getMessageBus().setQueryParallelFiberDispatcher(dispatcher);
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final CountDownLatch finished = new CountDownLatch(1);
            final AtomicBoolean launched = new AtomicBoolean();
            final AtomicBoolean isWorkerGated = new AtomicBoolean();
            final CountDownLatch ownerParked = new CountDownLatch(1);
            final CountDownLatch drainComplete = new CountDownLatch(1);
            try (TestWorkerPool ownerPool = new TestWorkerPool(
                    poolName,
                    1,
                    Metrics.DISABLED,
                    WorkerPoolMode.FIBER_HOST
            )) {
                final FiberRuntime ownerRuntime = ownerPool.getFiberRuntime();
                final FiberTask task = new FiberTask() {
                    @Override
                    protected void onDone() {
                        finished.countDown();
                    }

                    @Override
                    protected void onError(Throwable th) {
                        failure.compareAndSet(null, th);
                        finished.countDown();
                    }

                    @Override
                    protected boolean runStep() {
                        SuspensionScope.enterTimerShards(engine.getTimerShards());
                        try {
                            drain(compiler, executionContext, sql);
                        } catch (RuntimeException | Error e) {
                            throw e;
                        } catch (Exception e) {
                            throw new AssertionError(e);
                        }
                        return true;
                    }
                };
                // The single worker both launches the owner and, once the owner has published and
                // parked, blocks in this job so nothing can mount the owner while the test thread
                // drains the dispatcher. The owner resumes only after the drain aborted its tasks.
                ownerPool.assign(_ -> {
                    if (launched.compareAndSet(false, true)) {
                        final LaunchResult result = ownerRuntime.launch(task);
                        if (result != LaunchResult.LAUNCHED) {
                            failure.set(new AssertionError("owner fiber launch failed [result=" + result + ']'));
                            finished.countDown();
                        }
                        return true;
                    }
                    if (isWorkerGated.compareAndSet(false, ownerRuntime.getParkedFiberCount() == 1)) {
                        return false;
                    }
                    ownerParked.countDown();
                    try {
                        if (!drainComplete.await(10, TimeUnit.SECONDS)) {
                            failure.compareAndSet(null, new AssertionError("dispatcher drain timed out"));
                            finished.countDown();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return true;
                });
                ownerPool.start(LOG);
                Assert.assertTrue("owner fiber did not park", ownerParked.await(10, TimeUnit.SECONDS));

                dispatcher.beginQuiesce();
                dispatcher.progressQuiesce();
                drainComplete.countDown();

                final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (!finished.await(10, TimeUnit.MILLISECONDS)) {
                    Assert.assertTrue("owner did not finish after quiesce drain", System.nanoTime() < deadline);
                    dispatcher.progressQuiesce();
                }
                ownerPool.haltAndAssertCleanForTest(WorkerPool.DEFAULT_HALT_TIMEOUT_NANOS);
            }

            final Throwable th = failure.get();
            Assert.assertNotNull("query with aborted tasks must fail, not return a partial result", th);
            Assert.assertTrue(String.valueOf(th), th instanceof CairoException);
            final CairoException e = (CairoException) th;
            TestUtils.assertContains(e.getFlyweightMessage(), "cancelled by user");
            Assert.assertEquals(SqlExecutionCircuitBreaker.STATE_CANCELLED, e.getInterruptionReason());
        } finally {
            closeRuntime(dispatcherRuntime);
            Misc.free(dispatcher);
        }
    }

    private static void closeRuntime(FiberRuntime runtime) {
        runtime.beginQuiesce();
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (runtime.state() != FiberRuntimeState.CLOSED && System.nanoTime() < deadline) {
            runtime.drain(8);
        }
        Assert.assertTrue(runtime.awaitClosed(deadline));
        runtime.closeAfterDrained();
    }

    private static void assertInTree(RecordCursorFactory factory, Class<?> expected) {
        RecordCursorFactory f = factory;
        while (f != null) {
            if (expected.isInstance(f)) {
                return;
            }
            f = f.getBaseFactory();
        }
        Assert.fail("expected " + expected.getSimpleName() + " in the factory tree, but top was " + factory.getClass().getName());
    }
}
