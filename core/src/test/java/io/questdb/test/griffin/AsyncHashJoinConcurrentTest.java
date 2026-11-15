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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.test.TestLatchedCounterFunctionFactory;
import io.questdb.griffin.engine.table.AsyncHashJoinAtom;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.HashJoinBuildStage;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.test.griffin.ParallelHashJoinTestUtils.*;

/**
 * The parallel hash join under concurrency, on thread workers and on fiber workers, over inputs of
 * Parquet and native partitions: queries on many threads that saturate the shared query workers and
 * their queues, and executions whose owner thread runs tasks of its own sequences while the workers
 * hold theirs. A latch holds the workers inside the probe reducer, or inside a round of a
 * parallel build, until the owner ran a task of the same execution itself, so the interleaving does
 * not depend on timing. Every result compares with the ordinary plan, in order.
 */
public class AsyncHashJoinConcurrentTest extends AbstractCairoTest {
    private static final Log LOG = LogFactory.getLog(AsyncHashJoinConcurrentTest.class);
    // A LEFT join whose ON residual reads d alone, which the build runs over d's rows: the stage hands
    // d's chains on, one tuple per probe row, so no frame continues past a cap of one tuple.
    private static final String PUSHED = "SELECT t.k, t.v, d.w, d.s FROM t LEFT JOIN d ON t.k = d.k AND d.w % 3 = 0";
    private static final String ATOM = AsyncHashJoinAtom.class.getName();
    private static final String STAGE = HashJoinBuildStage.class.getName();
    private static final StackWalker STACK_WALKER = StackWalker.getInstance();
    private static final int WORKER_COUNT = 4;

    @After
    public void resetLatchedCounter() {
        TestLatchedCounterFunctionFactory.reset(null);
    }

    @Test
    public void testConcurrentQueriesSaturateFiberWorkers() throws Exception {
        assertConcurrentQueries(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testConcurrentQueriesSaturateThreadWorkers() throws Exception {
        assertConcurrentQueries(WorkerPoolMode.LEGACY);
    }

    @Test
    public void testConcurrentSmallInterpretedFilterBuildsOnFiberWorkers() throws Exception {
        assertConcurrentSmallInterpretedFilterBuilds(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testConcurrentSmallInterpretedFilterBuildsOnThreadWorkers() throws Exception {
        assertConcurrentSmallInterpretedFilterBuilds(WorkerPoolMode.LEGACY);
    }

    @Test
    public void testOwnerRunsProbeTasksWhileWorkersHoldTheirsOnFiberWorkers() throws Exception {
        assertOwnerRunsTasksWhileWorkersHoldTheirs(WorkerPoolMode.FIBER_HOST, false);
    }

    @Test
    public void testOwnerRunsProbeTasksWhileWorkersHoldTheirsOnThreadWorkers() throws Exception {
        assertOwnerRunsTasksWhileWorkersHoldTheirs(WorkerPoolMode.LEGACY, false);
    }

    @Test
    public void testOwnerRunsRoundTasksWhileWorkersHoldTheirsOnFiberWorkers() throws Exception {
        assertOwnerRunsTasksWhileWorkersHoldTheirs(WorkerPoolMode.FIBER_HOST, true);
    }

    @Test
    public void testOwnerRunsRoundTasksWhileWorkersHoldTheirsOnThreadWorkers() throws Exception {
        assertOwnerRunsTasksWhileWorkersHoldTheirs(WorkerPoolMode.LEGACY, true);
    }

    private static boolean isOnStack(String className, String methodName) {
        return STACK_WALKER.walk(frames -> frames.anyMatch(frame -> frame.getClassName().equals(className) && frame.getMethodName().equals(methodName)));
    }

    private static SqlExecutionContextImpl newContext(CairoEngine db) {
        final SqlExecutionContextImpl context = new SqlExecutionContextImpl(db, WORKER_COUNT)
                .with(AllowAllSecurityContext.INSTANCE, null, null, -1, null);
        context.setParallelHashJoinEnabled(true);
        return context;
    }

    private void assertConcurrentQueries(WorkerPoolMode mode) throws Exception {
        // Small queues, which the concurrent queries fill, so that their owners steal and reduce
        // locally, and builds in rounds of many partitions.
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 4);
        setProperty(PropertyKey.CAIRO_UNORDERED_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 4);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 0);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 64);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1);
        assertMemoryLeak(() -> TestUtils.execute(new TestWorkerPool(WORKER_COUNT, mode), (db, compiler, ignore) -> {
            final String[] queries = {
                    "SELECT t.k, t.v, d.w, d.s FROM t LEFT JOIN d ON t.k = d.k",
                    "SELECT t.k, t.v, d.w, d.s FROM t LEFT JOIN d ON t.s = d.s AND d.w > t.v % 50",
                    "SELECT t.k, t.v, d.w FROM t JOIN d ON t.l = d.l WHERE t.ts IN '2024-01-01' AND d.w > t.v % 7",
                    "SELECT d.w, t.v FROM d JOIN t ON d.k = t.k",
                    "SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k ORDER BY t.ts DESC LIMIT 300",
                    "SELECT t.k, t.v, d.w FROM (SELECT * FROM t WHERE v % 3 = 0) t LEFT JOIN (SELECT * FROM d WHERE w % 2 = 0) d ON t.k = d.k LIMIT -50",
                    PUSHED,
            };
            final ObjList<String> expected = new ObjList<>();
            try (SqlExecutionContextImpl context = newContext(db)) {
                createTables(db, context);
                for (String sql : queries) {
                    context.changePageFrameSizes(128, 128);
                    expected.add(ordinary(sql, context));
                }
            }
            final int threadCount = 6;
            final int iterations = 12;
            final CyclicBarrier start = new CyclicBarrier(threadCount);
            final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
            final AtomicInteger operatorRuns = new AtomicInteger();
            final ObjList<Thread> threads = new ObjList<>();
            for (int i = 0; i < threadCount; i++) {
                final int threadIndex = i;
                final Thread thread = new Thread(() -> {
                    try (SqlExecutionContextImpl context = newContext(db)) {
                        start.await(30, TimeUnit.SECONDS);
                        for (int n = 0; n < iterations; n++) {
                            final int q = (threadIndex + n) % queries.length;
                            context.changePageFrameSizes(128, 128);
                            try (RecordCursorFactory factory = db.select(queries[q], context)) {
                                final AsyncHashJoinRecordCursorFactory join = find(factory);
                                Assert.assertTrue(queries[q], plan(factory, context).contains("Hash Join workers:"));
                                Assert.assertEquals(queries[q], expected.getQuick(q), result(factory, context));
                                if (join != null) {
                                    Assert.assertEquals(0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                                    Assert.assertEquals(0, join.getBuildRoundAtom().getPerWorkerLocks().getAcquiredSlotCount());
                                }
                                if (queries[q].equals(PUSHED)) {
                                    Assert.assertNotNull(join);
                                    Assert.assertEquals(PUSHED, 0, join.getAtom().getContinuationCount());
                                }
                                operatorRuns.incrementAndGet();
                            }
                        }
                    } catch (Throwable th) {
                        errors.add(th);
                    }
                });
                threads.add(thread);
                thread.start();
            }
            for (int i = 0; i < threadCount; i++) {
                threads.getQuick(i).join();
            }
            for (Throwable th : errors) {
                LOG.error().$("concurrent query failed: ").$(th).$();
            }
            Assert.assertTrue(errors.isEmpty() ? "" : errors.peek().toString(), errors.isEmpty());
            Assert.assertEquals(threadCount * iterations, operatorRuns.get());
        }, configuration, LOG));
    }

    // Queries on many threads whose builds hold fewer rows than the configured 300_000, over two day
    // partitions of d, one of them Parquet, with filters that the JIT does not compile: every build
    // runs in rounds, its frame tasks among the other queries' tasks on the shared workers.
    private void assertConcurrentSmallInterpretedFilterBuilds(WorkerPoolMode mode) throws Exception {
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 4);
        setProperty(PropertyKey.CAIRO_UNORDERED_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 4);
        assertMemoryLeak(() -> TestUtils.execute(new TestWorkerPool(WORKER_COUNT, mode), (db, compiler, ignore) -> {
            final String[] queries = {
                    "SELECT t.k, t.v, d.w FROM t LEFT JOIN (SELECT * FROM d WHERE s::STRING LIKE 's1%') d ON t.k = d.k",
                    "SELECT t.k, t.v, d.w, d.s FROM t JOIN (SELECT * FROM d WHERE s::STRING ~ 's[2-4]') d ON t.l = d.l",
                    "SELECT t.k, t.v, d.w FROM t JOIN (SELECT * FROM d WHERE w::STRING LIKE '%7%') d ON t.s = d.s",
                    "SELECT t.k, t.v, d.w FROM t LEFT JOIN (SELECT * FROM d WHERE s::STRING LIKE '%3') d ON t.k = d.k ORDER BY t.ts DESC LIMIT 300",
            };
            final ObjList<String> expected = new ObjList<>();
            try (SqlExecutionContextImpl context = newContext(db)) {
                createTables(db, context);
                for (String sql : queries) {
                    expected.add(ordinary(sql, context));
                }
            }
            final int threadCount = 6;
            final int iterations = 8;
            final CyclicBarrier start = new CyclicBarrier(threadCount);
            final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
            final AtomicInteger roundBuilds = new AtomicInteger();
            final ObjList<Thread> threads = new ObjList<>();
            for (int i = 0; i < threadCount; i++) {
                final int threadIndex = i;
                final Thread thread = new Thread(() -> {
                    try (SqlExecutionContextImpl context = newContext(db)) {
                        start.await(30, TimeUnit.SECONDS);
                        for (int n = 0; n < iterations; n++) {
                            final int q = (threadIndex + n) % queries.length;
                            try (RecordCursorFactory factory = db.select(queries[q], context)) {
                                final AsyncHashJoinRecordCursorFactory join = find(factory);
                                Assert.assertNotNull(queries[q], join);
                                Assert.assertEquals(queries[q], expected.getQuick(q), result(factory, context));
                                try (RecordCursor cursor = factory.getCursor(context)) {
                                    Assert.assertTrue(queries[q], cursor.hasNext());
                                    Assert.assertTrue(queries[q], join.getAtom().getStage(0).isBuiltInRounds());
                                }
                                Assert.assertEquals(0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                                Assert.assertEquals(0, join.getBuildRoundAtom().getPerWorkerLocks().getAcquiredSlotCount());
                                roundBuilds.incrementAndGet();
                            }
                        }
                    } catch (Throwable th) {
                        errors.add(th);
                    }
                });
                threads.add(thread);
                thread.start();
            }
            for (int i = 0; i < threadCount; i++) {
                threads.getQuick(i).join();
            }
            for (Throwable th : errors) {
                LOG.error().$("concurrent query failed: ").$(th).$();
            }
            Assert.assertTrue(errors.isEmpty() ? "" : errors.peek().toString(), errors.isEmpty());
            Assert.assertEquals(threadCount * iterations, roundBuilds.get());
        }, configuration, LOG));
    }

    private void assertOwnerRunsTasksWhileWorkersHoldTheirs(WorkerPoolMode mode, boolean isRound) throws Exception {
        // Room for more frames, and round tasks, in flight than there are workers, so that tasks wait
        // in the queue while the workers hold theirs.
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 32);
        setProperty(PropertyKey.CAIRO_UNORDERED_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 32);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 32);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 0);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 64);
        assertMemoryLeak(() -> TestUtils.execute(new TestWorkerPool(WORKER_COUNT, mode), (db, compiler, ignore) -> {
            try (SqlExecutionContextImpl context = newContext(db)) {
                createTables(db, context);
                // The filter of the probe input runs in the probe reducer, the filter of the build
                // input in the build's first round.
                final String sql = isRound
                        ? "SELECT t.k, t.v, d.w, d.s FROM t LEFT JOIN (SELECT * FROM d WHERE test_latched_counter()) d ON t.k = d.k"
                        : "SELECT t.k, t.v, d.w, d.s FROM (SELECT * FROM t WHERE test_latched_counter()) t LEFT JOIN d ON t.k = d.k";
                final String reducerClass = isRound ? STAGE : ATOM;
                final String reducer = isRound ? "partitionFrame" : "probeFrame";
                context.changePageFrameSizes(64, 64);
                final String expected = ordinary(sql, context);
                final Thread owner = Thread.currentThread();
                final AtomicBoolean isArmed = new AtomicBoolean();
                // One pair per execution: a worker counts the first down once it holds a task, and the
                // owner counts the second down once it runs a task while a worker holds one.
                final AtomicReference<CountDownLatch> workerHeldRef = new AtomicReference<>();
                final AtomicReference<CountDownLatch> ownerRanRef = new AtomicReference<>();
                final AtomicInteger heldWorkers = new AtomicInteger();
                final AtomicBoolean isTimedOut = new AtomicBoolean();
                final TestLatchedCounterFunctionFactory.Callback callback = new TestLatchedCounterFunctionFactory.Callback() {
                    @Override
                    public boolean onGet(Record rec, int count) {
                        final CountDownLatch workerHeld = workerHeldRef.get();
                        final CountDownLatch ownerRan = ownerRanRef.get();
                        if (isArmed.get() && ownerRan.getCount() > 0 && isOnStack(reducerClass, reducer)) {
                            try {
                                if (Thread.currentThread() == owner) {
                                    // The owner runs a task of its own sequence while a worker holds one.
                                    if (!workerHeld.await(30, TimeUnit.SECONDS)) {
                                        isTimedOut.set(true);
                                    }
                                    ownerRan.countDown();
                                } else {
                                    // A worker holds its task, and its slot, until the owner ran one.
                                    heldWorkers.incrementAndGet();
                                    workerHeld.countDown();
                                    if (!ownerRan.await(30, TimeUnit.SECONDS)) {
                                        isTimedOut.set(true);
                                    }
                                }
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new AssertionError(e);
                            }
                        }
                        return true;
                    }
                };
                TestLatchedCounterFunctionFactory.reset(callback);
                try (RecordCursorFactory factory = db.select(sql, context)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(join);
                    for (int run = 0; run < 2; run++) {
                        final CountDownLatch ownerRan = new CountDownLatch(1);
                        workerHeldRef.set(new CountDownLatch(1));
                        ownerRanRef.set(ownerRan);
                        heldWorkers.set(0);
                        isArmed.set(true);
                        context.changePageFrameSizes(64, 64);
                        Assert.assertEquals(expected, result(factory, context));
                        isArmed.set(false);
                        Assert.assertFalse("the owner and a worker did not hold tasks at once", isTimedOut.get());
                        Assert.assertEquals(0, ownerRan.getCount());
                        // Only a task of the reducer counts the latches down, so the build ran in
                        // rounds when the reducer is the round's.
                        Assert.assertTrue(heldWorkers.get() > 0);
                        Assert.assertEquals(0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                        Assert.assertEquals(0, join.getBuildRoundAtom().getPerWorkerLocks().getAcquiredSlotCount());
                    }
                }
            }
        }, configuration, LOG));
    }

    // t spans three days and d two; the first day of each is Parquet, so both inputs mix Parquet
    // and native frames.
    private static void createTables(CairoEngine db, SqlExecutionContextImpl context) throws Exception {
        db.execute("""
                CREATE TABLE t AS (
                    SELECT (x % 400)::INT k, x v, (x % 300)::LONG l, ('s' || (x % 250))::SYMBOL s,
                        timestamp_sequence('2024-01-01', 40_000_000) ts
                    FROM long_sequence(6_000)
                ) TIMESTAMP(ts) PARTITION BY DAY
                """, context);
        db.execute("""
                CREATE TABLE d AS (
                    SELECT (x % 350)::INT k, x w, (x % 300)::LONG l, ('s' || (x % 200))::SYMBOL s,
                        timestamp_sequence('2024-01-01', 60_000_000) dts
                    FROM long_sequence(2_000)
                ) TIMESTAMP(dts) PARTITION BY DAY
                """, context);
        db.execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'", context);
        db.execute("ALTER TABLE d CONVERT PARTITION TO PARQUET LIST '2024-01-01'", context);
    }
}
