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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.test.TestLatchedCounterFunctionFactory;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.HashJoinBuildStage;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.std.Rows;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.sql.async.SlotGatedWorkStealingStrategy;
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
 * Two small builds whose filters run interpreted over many frames build in rounds of their own.
 * count(*) and negative {@code LIMIT}s count on count-only tasks that the workers run, and a count that
 * follows some rows meets the tuple-carrying tasks that the workers ran before it. The pass that
 * indexes the builds' chain lengths for a count runs on the owner, with the owner's breaker, also
 * while a worker still joins a frame over the same builds. Every result compares with the ordinary
 * plan, and every slot is back after each query, a failed one included.
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

    @Test
    public void testIndexPassChecksTheOwnersBreakerOnFiberWorkers() throws Exception {
        assertIndexPassBreaker(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testIndexPassChecksTheOwnersBreakerOnThreadWorkers() throws Exception {
        assertIndexPassBreaker(WorkerPoolMode.LEGACY);
    }

    @Test
    public void testIndexPassRunsBesideATupleTaskOnFiberWorkers() throws Exception {
        assertIndexPassBesideTupleTask(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testIndexPassRunsBesideATupleTaskOnThreadWorkers() throws Exception {
        assertIndexPassBesideTupleTask(WorkerPoolMode.LEGACY);
    }

    @Test
    public void testSmallFilteredBuildsInRoundsOnFiberWorkers() throws Exception {
        assertSmallFilteredBuildsInRounds(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testSmallFilteredBuildsInRoundsOnThreadWorkers() throws Exception {
        assertSmallFilteredBuildsInRounds(WorkerPoolMode.LEGACY);
    }

    private static void assertConcurrentBuilds(WorkerPoolMode mode) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, Long.MAX_VALUE);
        execute(mode, (db, context) -> {
            // Each stage's build filter holds the first thread that runs it until the other stage's
            // task runs too: the two builds must run at once to get past it. A filter that the JIT
            // does not compile builds a stage in rounds of its own over more than one frame, so frames
            // of 4_096 rows hold d's 3_000 rows and d2's 1_000 in one each, and the stages stay small.
            context.changePageFrameSizes(4_096, 4_096);
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
            context.changePageFrameSizes(256, 256);
            final String starExpected = ordinary(STAR, context);
            context.setParallelHashJoinEnabled(true);
            try (RecordCursorFactory factory = db.select(STAR, context)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                final int failures = assertMemoryLimitSweep(
                        starExpected,
                        limit -> setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, limit),
                        () -> result(factory, context),
                        null,
                        () -> {
                            assertSlotsBack(join);
                        }
                );
                Assert.assertTrue(failures > 0);
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

    // A count that follows a row indexes the chain lengths of d's duplicate keys while a worker still
    // joins a frame whose task the cursor dispatched for the rows: the probe filter holds that task
    // until the pass makes its first check, and the task then walks the chains whose links the pass
    // reads. The owner waits for the workers rather than run the tasks itself, so that a worker runs
    // the held task. The count, and the rows read again after toTop(), equal the ordinary plan's.
    private static void assertIndexPassBesideTupleTask(WorkerPoolMode mode) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 4);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WORK_STEALING_THRESHOLD, 1);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WORK_STEALING_SPIN_TIMEOUT, TimeUnit.SECONDS.toNanos(30));
        execute(mode, (db, context) -> {
            final String sql = "SELECT t.k, t.v, d.w FROM (SELECT * FROM t WHERE test_latched_counter()) t LEFT JOIN d ON t.k = d.k";
            final String expected = ordinary(sql, context);
            final long rowCount = ordinaryRowCount(db, context, sql);
            final Thread owner = Thread.currentThread();
            final AtomicBoolean isArmed = new AtomicBoolean();
            final AtomicBoolean isHeld = new AtomicBoolean();
            final AtomicBoolean isTimedOut = new AtomicBoolean();
            final CountDownLatch held = new CountDownLatch(1);
            final CountDownLatch released = new CountDownLatch(1);
            TestLatchedCounterFunctionFactory.reset(new TestLatchedCounterFunctionFactory.Callback() {
                @Override
                public boolean onGet(Record rec, int count) {
                    // A worker's probe filter over a frame after the first, which the first window holds.
                    if (isArmed.get() && Thread.currentThread() != owner && Rows.toPartitionIndex(rec.getRowId()) > 0
                            && isHeld.compareAndSet(false, true)) {
                        held.countDown();
                        try {
                            if (!released.await(30, TimeUnit.SECONDS)) {
                                isTimedOut.set(true);
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError(e);
                        }
                    }
                    return true;
                }
            });
            context.setParallelHashJoinEnabled(true);
            try (RecordCursorFactory factory = db.select(sql, context)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                final ChainLengthIndexPassBreaker breaker = new ChainLengthIndexPassBreaker(db, false, released::countDown);
                final SqlExecutionCircuitBreaker previous = context.getCircuitBreaker();
                context.with(breaker);
                isArmed.set(true);
                try (RecordCursor cursor = factory.getCursor(context)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertTrue("no worker held a frame's task", held.await(30, TimeUnit.SECONDS));
                    final RecordCursor.Counter counter = new RecordCursor.Counter();
                    cursor.calculateSize(breaker, counter);
                    Assert.assertEquals(rowCount - 1, counter.get());
                    // The count met the tuples of the frames of the first window, the held one among them.
                    Assert.assertTrue(join.getCountedTupleFrameCount() > 0);
                    cursor.toTop();
                    long again = 0;
                    while (cursor.hasNext()) {
                        again++;
                    }
                    Assert.assertEquals(rowCount, again);
                } finally {
                    isArmed.set(false);
                    released.countDown();
                    context.with(previous);
                }
                Assert.assertFalse("the pass did not release the held task", isTimedOut.get());
                Assert.assertEquals(1, breaker.getPassCheckCount());
                Assert.assertTrue(breaker.isPassOnlyOn(owner));
                assertSlotsBack(join);
                Assert.assertEquals(expected, result(factory, context));
            }
        });
    }

    // d and d2 repeat their keys, so their stages hand their chains on, and both are small, so the
    // workers build them at once, in a round of concurrent builds, each task with a copy of the
    // context's breaker, which is not thread safe. A count indexes their chain lengths on the owner,
    // and the pass checks the context's breaker itself, once per stage here, from the top, when the
    // workers then run the count-only tasks that read the index, and after rows whose tuples the
    // workers wrote.
    private static void assertIndexPassBreaker(WorkerPoolMode mode) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 4);
        execute(mode, (db, context) -> {
            final String sql = "SELECT t.k, t.v, d.w, d2.w2 FROM t JOIN d ON t.k = d.k JOIN d2 ON t.l = d2.l";
            final long rowCount = ordinaryRowCount(db, context, sql);
            context.setParallelHashJoinEnabled(true);
            try (RecordCursorFactory factory = db.select(sql, context)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                for (int prefix : new int[]{0, 1_000}) {
                    final ChainLengthIndexPassBreaker breaker = new ChainLengthIndexPassBreaker(db, false, null);
                    final long rounds = join.getBuildRoundAtom().getConcurrentBuildRoundCount();
                    // From the top, the owner waits until a worker took a probe slot: the count-only
                    // tasks run there.
                    final CountDownLatch probeSlot = new CountDownLatch(1);
                    if (prefix == 0) {
                        join.getAtom().getPerWorkerLocks().setTestAcquireLatch(probeSlot);
                    }
                    final SqlExecutionCircuitBreaker previous = context.getCircuitBreaker();
                    context.with(breaker);
                    try (RecordCursor cursor = factory.getCursor(context)) {
                        for (int i = 0; i < prefix; i++) {
                            Assert.assertTrue(cursor.hasNext());
                        }
                        final RecordCursor.Counter counter = new RecordCursor.Counter();
                        cursor.calculateSize(breaker, counter);
                        Assert.assertEquals(sql + ", after " + prefix + " rows", rowCount - prefix, counter.get());
                        Assert.assertTrue(join.getCountOnlyFrameCount() > 0);
                    } finally {
                        context.with(previous);
                        join.getAtom().getPerWorkerLocks().setTestAcquireLatch(null);
                    }
                    if (prefix == 0) {
                        Assert.assertEquals(0, probeSlot.getCount());
                    }
                    Assert.assertEquals(rounds + 1, join.getBuildRoundAtom().getConcurrentBuildRoundCount());
                    Assert.assertEquals(2, breaker.getPassCheckCount());
                    Assert.assertTrue(breaker.isPassOnlyOn(Thread.currentThread()));
                    assertSlotsBack(join);
                }
            }
        });
    }

    // d's 3_000 rows and d2's 1_000 span frames of 256 rows, and each stage's filter runs interpreted,
    // so each stage builds in rounds of its own, one after the other, rather than in a round of
    // concurrent builds.
    private static void assertSmallFilteredBuildsInRounds(WorkerPoolMode mode) throws Exception {
        // No build copies its payload, so that each build runs two rounds.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MIN_PROBE_RATIO, "1000000");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_PARALLEL_MIN_PROBE_RATIO, "1000000");
        execute(mode, (db, context) -> {
            final String sql = "SELECT t.k, t.v, a.w, b.w2 FROM t LEFT JOIN (SELECT * FROM d WHERE w::STRING LIKE '%7%') a ON t.k = a.k"
                    + " LEFT JOIN (SELECT * FROM d2 WHERE w2::STRING LIKE '%3%') b ON t.l = b.l";
            final String expected = ordinary(sql, context);
            context.setParallelHashJoinEnabled(true);
            try (RecordCursorFactory factory = db.select(sql, context)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                for (int run = 0; run < 2; run++) {
                    final long concurrentRounds = join.getBuildRoundAtom().getConcurrentBuildRoundCount();
                    final long rounds0 = join.getAtom().getStage(0).getRoundCount();
                    final long rounds1 = join.getAtom().getStage(1).getRoundCount();
                    // The owner waits until a worker took a slot of the build's rounds.
                    final CountDownLatch roundSlot = new CountDownLatch(1);
                    join.getBuildRoundAtom().getPerWorkerLocks().setTestAcquireLatch(roundSlot);
                    try (RecordCursor cursor = factory.getCursor(context)) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertTrue(join.getAtom().getStage(0).isBuiltInRounds());
                        Assert.assertTrue(join.getAtom().getStage(1).isBuiltInRounds());
                    } finally {
                        join.getBuildRoundAtom().getPerWorkerLocks().setTestAcquireLatch(null);
                    }
                    Assert.assertEquals(0, roundSlot.getCount());
                    Assert.assertEquals(concurrentRounds, join.getBuildRoundAtom().getConcurrentBuildRoundCount());
                    Assert.assertEquals(rounds0 + 2, join.getAtom().getStage(0).getRoundCount());
                    Assert.assertEquals(rounds1 + 2, join.getAtom().getStage(1).getRoundCount());
                    Assert.assertEquals(expected, result(factory, context));
                    assertSlotsBack(join);
                }
            }
        });
    }

    private static void assertSlotsBack(AsyncHashJoinRecordCursorFactory join) {
        Assert.assertEquals(0, join.getBuildRoundAtom().getPerWorkerLocks().getAcquiredSlotCount());
        Assert.assertEquals(0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
    }

    private static void execute(WorkerPoolMode mode, WorkersCode code) throws Exception {
        assertMemoryLeak(() -> executeOnWorkers(mode, WORKER_COUNT, null, configuration, LOG, (db, context) -> {
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
        }));
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
}
