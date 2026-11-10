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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.test.TestLatchedCounterFunctionFactory;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.HashJoinBuildStage;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.sql.async.SlotGatedWorkStealingStrategy;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.test.griffin.ParallelHashJoinTestUtils.*;

/**
 * The parallel hash join's concurrent small builds and its count of rows on shared query workers that
 * run on threads and on fibers. Two small builds run at once: each task of the round holds its
 * thread until the other task runs too, which only threads that build the stages at once let pass.
 * count(*) and negative {@code LIMIT}s count on count-only tasks that the workers run, and a count that
 * follows some rows meets the tuple-carrying tasks that the workers ran before it. Every result
 * compares with the ordinary plan, and every slot is back after each query, a failed one included.
 */
public class AsyncHashJoinConcurrentBuildsWorkersTest extends AbstractCairoTest {
    private static final Log LOG = LogFactory.getLog(AsyncHashJoinConcurrentBuildsWorkersTest.class);
    private static final String STAGE = HashJoinBuildStage.class.getName();
    // Three small stages over t: a duplicate INT key, a duplicate LONG key that stages into a map and a
    // translated SYMBOL key of one row per text.
    private static final String STAR = "SELECT t.k, t.v, d.w, d2.w2, d3.w3 FROM t LEFT JOIN d ON t.k = d.k"
            + " LEFT JOIN d2 ON t.l = d2.l LEFT JOIN d3 ON t.s = d3.s";
    private static final StackWalker STACK_WALKER = StackWalker.getInstance();
    private static final int WORKER_COUNT = 4;

    @Override
    @Before
    public void setUp() {
        factoryProvider = SlotGatedWorkStealingStrategy.newFactoryProvider();
        super.setUp();
    }

    @After
    public void tearDownWorkers() {
        TestLatchedCounterFunctionFactory.reset(null);
    }

    @Test
    public void testConcurrentBuildsOnFiberWorkers() throws Exception {
        assertConcurrentBuilds(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testConcurrentBuildsOnThreadWorkers() throws Exception {
        assertConcurrentBuilds(WorkerPoolMode.LEGACY);
    }

    @Test
    public void testCountsOnFiberWorkers() throws Exception {
        assertCounts(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testCountsOnThreadWorkers() throws Exception {
        assertCounts(WorkerPoolMode.LEGACY);
    }

    private static void assertConcurrentBuilds(WorkerPoolMode mode) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, Long.MAX_VALUE);
        execute(mode, (db, context) -> {
            // Each stage's build filter holds the first thread that runs it until the other stage's
            // task runs too: the two builds must run at once to get past it.
            final String sql = "SELECT t.k, t.v, a.w, b.w2 FROM t LEFT JOIN (SELECT * FROM d WHERE test_latched_counter()) a ON t.k = a.k"
                    + " LEFT JOIN (SELECT * FROM d2 WHERE test_latched_counter()) b ON t.l = b.l";
            final String expected = ordinary(sql, context);
            final AtomicBoolean isArmed = new AtomicBoolean();
            final AtomicBoolean isTimedOut = new AtomicBoolean();
            final Set<Thread> buildThreads = ConcurrentHashMap.newKeySet();
            final AtomicReference<CountDownLatch> bothStarted = new AtomicReference<>();
            final TestLatchedCounterFunctionFactory.Callback callback = new TestLatchedCounterFunctionFactory.Callback() {
                @Override
                public boolean onGet(Record rec, int count) {
                    if (isArmed.get() && isOnStack("buildStage") && buildThreads.add(Thread.currentThread())) {
                        final CountDownLatch latch = bothStarted.get();
                        latch.countDown();
                        try {
                            if (!latch.await(30, TimeUnit.SECONDS)) {
                                isTimedOut.set(true);
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
            context.setParallelHashJoinEnabled(true);
            try (RecordCursorFactory factory = db.select(sql, context)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                TestUtils.assertContains(plan(factory, context), "stages: 2");
                for (int run = 0; run < 2; run++) {
                    final long rounds = join.getBuildRoundAtom().getConcurrentBuildRoundCount();
                    // The owner waits until a worker took a slot of the round.
                    final CountDownLatch roundSlot = new CountDownLatch(1);
                    join.getBuildRoundAtom().getPerWorkerLocks().setTestAcquireLatch(roundSlot);
                    buildThreads.clear();
                    bothStarted.set(new CountDownLatch(2));
                    isArmed.set(true);
                    try {
                        Assert.assertEquals(expected, result(factory, context));
                    } finally {
                        isArmed.set(false);
                        join.getBuildRoundAtom().getPerWorkerLocks().setTestAcquireLatch(null);
                    }
                    Assert.assertFalse("the two builds did not run at once", isTimedOut.get());
                    Assert.assertEquals(2, buildThreads.size());
                    Assert.assertEquals(0, roundSlot.getCount());
                    Assert.assertEquals(rounds + 1, join.getBuildRoundAtom().getConcurrentBuildRoundCount());
                    assertSlotsBack(join);
                }
            }
            // Limits from a few bytes up fail the concurrent builds, and the probes after them, at
            // their allocations in turn, on the workers and the owner; each failure leaves every slot
            // back, and the first limit that admits the query reads the rows.
            final String starExpected = ordinary(STAR, context);
            context.setParallelHashJoinEnabled(true);
            try (RecordCursorFactory factory = db.select(STAR, context)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                int failures = 0;
                for (long limit = 4; ; limit *= 2) {
                    setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, limit);
                    try {
                        Assert.assertEquals(starExpected, result(factory, context));
                        break;
                    } catch (CairoException e) {
                        Assert.assertTrue(e.getMessage(), e.isOutOfMemory());
                        failures++;
                    }
                    assertSlotsBack(join);
                }
                Assert.assertTrue(failures > 0);
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0);
                Assert.assertEquals(starExpected, result(factory, context));
            }
        });
    }

    private static void assertCounts(WorkerPoolMode mode) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 4);
        execute(mode, (db, context) -> {
            context.setParallelHashJoinGroupByEnabled(false);
            for (String sql : new String[]{
                    STAR,
                    "SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k",
                    // A residual, which the workers run per pair, and a JIT compiled probe filter.
                    "SELECT t.k, t.v, d.w, d3.w3 FROM t LEFT JOIN d ON t.k = d.k AND d.w > t.v % 1_000 LEFT JOIN d3 ON t.s = d3.s WHERE t.v > 10",
            }) {
                final String expectedRows = ordinary(sql, context);
                // The planner gives a negative LIMIT over a filtered probe input to the input's filter,
                // which the operator then does not take.
                final String[] queries = sql.contains("WHERE t.v")
                        ? new String[]{"SELECT count(*) FROM (" + sql + ")"}
                        : new String[]{"SELECT count(*) FROM (" + sql + ")", sql + " LIMIT -7", sql + " LIMIT -20, -10"};
                for (String query : queries) {
                    final String expected = ordinary(query, context);
                    context.setParallelHashJoinEnabled(true);
                    try (RecordCursorFactory factory = db.select(query, context)) {
                        final AsyncHashJoinRecordCursorFactory join = find(factory);
                        Assert.assertNotNull(query, join);
                        // The owner waits until a worker took a probe slot: the count's tasks run there.
                        final CountDownLatch probeSlot = new CountDownLatch(1);
                        join.getAtom().getPerWorkerLocks().setTestAcquireLatch(probeSlot);
                        try {
                            Assert.assertEquals(query, expected, result(factory, context));
                        } finally {
                            join.getAtom().getPerWorkerLocks().setTestAcquireLatch(null);
                        }
                        Assert.assertEquals(0, probeSlot.getCount());
                        assertSlotsBack(join);
                    }
                }
                // A count after some rows meets the tuples that the workers wrote before it began.
                context.setParallelHashJoinEnabled(true);
                try (RecordCursorFactory factory = db.select(sql, context)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(sql, join);
                    final long rowCount = ordinaryRowCount(db, context, sql);
                    try (RecordCursor cursor = factory.getCursor(context)) {
                        for (int i = 0; i < 1_000; i++) {
                            Assert.assertTrue(cursor.hasNext());
                        }
                        final RecordCursor.Counter counter = new RecordCursor.Counter();
                        cursor.calculateSize(context.getCircuitBreaker(), counter);
                        Assert.assertEquals(sql, rowCount - 1_000, counter.get());
                        Assert.assertTrue(sql, join.getCountedTupleFrameCount() > 0);
                        Assert.assertTrue(sql, join.getCountOnlyFrameCount() > 0);
                        cursor.toTop();
                        long again = 0;
                        while (cursor.hasNext()) {
                            again++;
                        }
                        Assert.assertEquals(sql, rowCount, again);
                    }
                    Assert.assertEquals(sql, expectedRows, result(factory, context));
                    assertSlotsBack(join);
                }
            }
        });
    }

    private static void assertSlotsBack(AsyncHashJoinRecordCursorFactory join) {
        Assert.assertEquals(0, join.getBuildRoundAtom().getPerWorkerLocks().getAcquiredSlotCount());
        Assert.assertEquals(0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
    }

    private static void execute(WorkerPoolMode mode, Code code) throws Exception {
        assertMemoryLeak(() -> TestUtils.execute(new TestWorkerPool(WORKER_COUNT, mode), (db, compiler, ignore) -> {
            Assert.assertEquals(mode == WorkerPoolMode.FIBER_HOST, db.getMessageBus().getPageFrameReduceDispatcher() != null);
            try (
                    SqlExecutionContextImpl context = new SqlExecutionContextImpl(db, WORKER_COUNT)
                            .with(AllowAllSecurityContext.INSTANCE, null, null, -1, null)
            ) {
                db.execute("""
                        CREATE TABLE t AS (
                            SELECT
                                (x % 500)::INT k,
                                x v,
                                (x % 300)::LONG l,
                                ('s' || (x % 400))::SYMBOL s,
                                timestamp_sequence('2024-01-01', 10_000_000) ts
                            FROM long_sequence(20_000)
                        ) TIMESTAMP(ts) PARTITION BY DAY
                        """, context);
                db.execute("CREATE TABLE d AS (SELECT (x % 450)::INT k, x w FROM long_sequence(3_000))", context);
                db.execute("CREATE TABLE d2 AS (SELECT (x % 300)::LONG l, x w2 FROM long_sequence(1_000))", context);
                db.execute("CREATE TABLE d3 AS (SELECT ('s' || (x * 2))::SYMBOL s, x w3 FROM long_sequence(300))", context);
                context.changePageFrameSizes(256, 256);
                code.run(db, context);
            }
        }, configuration, LOG));
    }

    private static boolean isOnStack(String methodName) {
        return STACK_WALKER.walk(frames -> frames.anyMatch(frame -> frame.getClassName().equals(STAGE) && frame.getMethodName().equals(methodName)));
    }

    // The rows of the query's ordinary plan.
    private static long ordinaryRowCount(CairoEngine db, SqlExecutionContextImpl context, String sql) throws Exception {
        context.setParallelHashJoinEnabled(false);
        try (RecordCursorFactory factory = db.select(sql, context); RecordCursor cursor = factory.getCursor(context)) {
            Assert.assertNull(find(factory));
            long count = 0;
            while (cursor.hasNext()) {
                count++;
            }
            return count;
        } finally {
            context.setParallelHashJoinEnabled(true);
        }
    }

    @FunctionalInterface
    private interface Code {
        void run(CairoEngine db, SqlExecutionContextImpl context) throws Exception;
    }
}
