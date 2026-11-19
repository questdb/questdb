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
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.test.TestLatchedCounterFunctionFactory;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.test.griffin.ParallelHashJoinTestUtils.*;

/**
 * A probe task of the parallel hash join stops within 4,096 rows of a frame once its execution was
 * cancelled on another thread, by the cursor's close() or by another task's failure, rather than at
 * the end of its frame: every loop over a frame's rows, a task's or a count's, over the frame's rows
 * or over the rows that its probe filter kept, reads the execution's state at every multiple of
 * 4,096 of its index.
 * <p>
 * The probe table t holds 8 frames of 16,384 rows in one partition; each of its rows meets one row
 * of d. A LEFT join's ON residual calls test_latched_counter() once per probe row, on the thread
 * that joins it, which reads the row's value v off the joined record. A worker that reaches the
 * first row that a task of frame 2 joins waits there until the execution was cancelled, then joins
 * on: it calls the residual for 4,095 more rows, those up to its next check, and stops. A task
 * without the check would join to its frame's end, as a stage checks the breaker every 16,384 pairs,
 * the page frame's rows. The owner of the execution never waits: when it joins that row itself, the
 * execution runs again.
 */
public class AsyncHashJoinTaskCancellationTest extends AbstractCairoTest {
    private static final int FRAME_COUNT = 8;
    private static final int FRAME_ROWS = 16_384;
    // The rows that a task joins after the row it waited at: up to the next multiple of 4,096.
    private static final long ROWS_AFTER_PAUSE = 4_095;
    private static final int ROW_COUNT = FRAME_COUNT * FRAME_ROWS;
    private static final String JOIN = "SELECT t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND (test_latched_counter() OR t.v < d.w)";
    private static final Log LOG = LogFactory.getLog(AsyncHashJoinTaskCancellationTest.class);
    private static final int MAX_ATTEMPTS = 50;
    // An INNER join of one stage that walks no matches, whose tasks no residual interrupts.
    private static final String PLAIN_JOIN = "SELECT t.v, d.w FROM t JOIN d ON t.k = d.k";
    private static final long WAIT_NANOS = TimeUnit.SECONDS.toNanos(60);
    private static final int WORKER_COUNT = 4;

    @After
    public void tearDownCancellation() {
        TestLatchedCounterFunctionFactory.reset(null);
        restoreSteppers();
    }

    @Test
    public void testCancelledExecutionLeavesOtherQueriesWholeOnFiberWorkers() throws Exception {
        assertCancelledExecutionLeavesOtherQueriesWhole(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testCancelledExecutionLeavesOtherQueriesWholeOnThreadWorkers() throws Exception {
        assertCancelledExecutionLeavesOtherQueriesWhole(WorkerPoolMode.LEGACY);
    }

    @Test
    public void testCloseStopsRunningTasksOnFiberWorkers() throws Exception {
        assertCloseStopsRunningTasks(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testCloseStopsRunningTasksOnThreadWorkers() throws Exception {
        assertCloseStopsRunningTasks(WorkerPoolMode.LEGACY);
    }

    @Test
    public void testConcurrentEarlyClosesOnFiberWorkers() throws Exception {
        assertConcurrentEarlyCloses(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testConcurrentEarlyClosesOnThreadWorkers() throws Exception {
        assertConcurrentEarlyCloses(WorkerPoolMode.LEGACY);
    }

    @Test
    public void testFailedTaskStopsRunningCountTasksOnFiberWorkers() throws Exception {
        assertFailedTaskStopsRunningCountTasks(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testFailedTaskStopsRunningCountTasksOnThreadWorkers() throws Exception {
        assertFailedTaskStopsRunningCountTasks(WorkerPoolMode.LEGACY);
    }

    // The value of the first row that a task of the frame joins: in the scan's order, of the rows that
    // the probe filter, which keeps the even values, kept, if it runs.
    private static long firstRowOf(int frame, boolean isFiltered, boolean isDescending) {
        if (isDescending) {
            // A backward scan reads the frames from the table's end, each from its last row.
            return ROW_COUNT - (long) frame * FRAME_ROWS;
        }
        return (long) frame * FRAME_ROWS + (isFiltered ? 2 : 1);
    }

    private static String joinSql(boolean isFiltered, boolean isDescending) {
        return JOIN + (isFiltered ? " WHERE t.v % 2 = 0" : "") + (isDescending ? " ORDER BY t.ts DESC" : "");
    }

    private static SqlExecutionContextImpl newContext(CairoEngine db) {
        final SqlExecutionContextImpl context = new SqlExecutionContextImpl(db, WORKER_COUNT)
                .with(AllowAllSecurityContext.INSTANCE, null, null, -1, null);
        context.setParallelHashJoinEnabled(true);
        context.changePageFrameSizes(FRAME_ROWS, FRAME_ROWS);
        return context;
    }

    // The rows of the cursor, in order, folded into a hash of their values, and their count.
    private static long[] readAll(RecordCursor cursor) {
        long hash = 0;
        long rows = 0;
        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            hash = hash * 31 + record.getLong(0);
            hash = hash * 31 + record.getLong(1);
            rows++;
        }
        return new long[]{hash, rows};
    }

    private static long[] readAll(RecordCursorFactory factory, SqlExecutionContext context) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(context)) {
            return readAll(cursor);
        }
    }

    private static void createTables(CairoEngine db, SqlExecutionContext context) throws SqlException {
        // v, the first column of the joined record, tells a probe row and its frame.
        db.execute("""
                CREATE TABLE t AS (
                    SELECT x v, (x % 100)::INT k, timestamp_sequence('2024-01-01', 1_000_000) ts
                    FROM long_sequence(131_072)
                ) TIMESTAMP(ts) PARTITION BY MONTH
                """, context);
        db.execute("CREATE TABLE d AS (SELECT (x - 1)::INT k, x * 10 w FROM long_sequence(100))", context);
        db.execute("CREATE TABLE e AS (SELECT (x * 2)::INT k, x * 1_000 z FROM long_sequence(50))", context);
    }

    // Each loop over a frame's rows, a count's, over the frame's rows and over the rows its probe
    // filter kept, forward and backward, on the stages' hidden stepper copies and on the shared
    // template: once a worker holds frame 2's task, the task of frame 3 fails, whose reduce job
    // cancels the execution; the paused task stops 4,095 rows later, and the count fails with the
    // task's error. The factory counts every row again.
    private void assertFailedTaskStopsRunningCountTasks(WorkerPoolMode mode) throws Exception {
        setUpExecution();
        assertMemoryLeak(() -> executeOnWorkers(mode, WORKER_COUNT, null, configuration, LOG, (db, context) -> {
            context.setParallelHashJoinEnabled(true);
            context.changePageFrameSizes(FRAME_ROWS, FRAME_ROWS);
            createTables(db, context);
            final Router router = new Router();
            TestLatchedCounterFunctionFactory.reset(router);
            for (boolean isCopy : new boolean[]{true, false}) {
                for (boolean isFiltered : new boolean[]{false, true}) {
                    for (boolean isDescending : new boolean[]{false, true}) {
                        final String sql = joinSql(isFiltered, isDescending);
                        final String label = sql + ", copies: " + isCopy;
                        final long expectedCount = ordinaryRowCount(sql, context);
                        Assert.assertEquals(label, isFiltered ? ROW_COUNT / 2 : ROW_COUNT, expectedCount);
                        useSteppers(isCopy);
                        try (RecordCursorFactory factory = db.select(sql, context)) {
                            final AsyncHashJoinRecordCursorFactory join = find(factory);
                            Assert.assertNotNull(label, join);
                            int attempts = 0;
                            Pause pause;
                            do {
                                Assert.assertTrue(label + ": the owner joined the row at every attempt", ++attempts <= MAX_ATTEMPTS);
                                pause = new Pause(join, firstRowOf(2, isFiltered, isDescending), Thread.currentThread(), null);
                                pause.failAt(firstRowOf(3, isFiltered, isDescending));
                                router.pause = pause;
                                try (RecordCursor cursor = factory.getCursor(context)) {
                                    cursor.calculateSize(context.getCircuitBreaker(), new RecordCursor.Counter());
                                    Assert.fail(label + ": expected the failure of frame 3's task");
                                } catch (CairoException e) {
                                    TestUtils.assertContains(e.getFlyweightMessage(), "injected failure at " + firstRowOf(3, isFiltered, isDescending));
                                }
                                Assert.assertTrue(label, pause.awaitDecided());
                            } while (!pause.isPaused());
                            pause.assertStoppedAtTheNextCheck(label);
                            Assert.assertEquals(label, isCopy, join.getStepperClass(0).isHidden());
                            Assert.assertEquals(label, 0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                            Assert.assertEquals(label, 0, db.getBusyReaderCount());
                            router.pause = null;
                            try (RecordCursor cursor = factory.getCursor(context)) {
                                final RecordCursor.Counter counter = new RecordCursor.Counter();
                                cursor.calculateSize(context.getCircuitBreaker(), counter);
                                Assert.assertEquals(label, expectedCount, counter.get());
                            }
                        } finally {
                            router.pause = null;
                            restoreSteppers();
                        }
                    }
                }
            }
        }));
    }

    // A worker of an execution that close() cancelled waits, once it saw the cancellation, until
    // another thread ran a query of its own to the end on the same workers: the other query's tasks
    // read their own execution's state, which no other execution's cancellation reaches, and return
    // every row, in order, as the ordinary plan does. Then the paused task stops 4,095 rows later.
    private void assertCancelledExecutionLeavesOtherQueriesWhole(WorkerPoolMode mode) throws Exception {
        setUpExecution();
        assertMemoryLeak(() -> executeOnWorkers(mode, WORKER_COUNT, null, configuration, LOG, (db, context) -> {
            context.setParallelHashJoinEnabled(true);
            context.changePageFrameSizes(FRAME_ROWS, FRAME_ROWS);
            createTables(db, context);
            final String other = PLAIN_JOIN + " WHERE t.v % 5 <> 0";
            final long[] expectedOther;
            try (SqlExecutionContextImpl otherContext = newContext(db)) {
                otherContext.setParallelHashJoinEnabled(false);
                try (RecordCursorFactory factory = db.select(other, otherContext)) {
                    expectedOther = readAll(factory, otherContext);
                }
            }
            Assert.assertEquals(ROW_COUNT - ROW_COUNT / 5, expectedOther[1]);
            final String sql = joinSql(false, false);
            final Router router = new Router();
            TestLatchedCounterFunctionFactory.reset(router);
            final long[] expected;
            try (SqlExecutionContextImpl ordinaryContext = newContext(db)) {
                ordinaryContext.setParallelHashJoinEnabled(false);
                try (RecordCursorFactory factory = db.select(sql, ordinaryContext)) {
                    expected = readAll(factory, ordinaryContext);
                }
            }
            Assert.assertEquals(ROW_COUNT, expected[1]);
            try (RecordCursorFactory factory = db.select(sql, context)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
                final AtomicLong otherRows = new AtomicLong(-1);
                int attempts = 0;
                Pause pause;
                do {
                    Assert.assertTrue("the owner joined the row at every attempt", ++attempts <= MAX_ATTEMPTS);
                    final CountDownLatch otherDone = new CountDownLatch(1);
                    // Runs on the paused worker, once it saw the cancellation.
                    final Runnable runOther = () -> {
                        final Thread thread = new Thread(() -> {
                            try (SqlExecutionContextImpl otherContext = newContext(db)) {
                                try (RecordCursorFactory otherFactory = db.select(other, otherContext)) {
                                    final AsyncHashJoinRecordCursorFactory otherJoin = find(otherFactory);
                                    Assert.assertNotNull(otherJoin);
                                    final long[] rows;
                                    try (RecordCursor otherCursor = otherFactory.getCursor(otherContext)) {
                                        rows = readAll(otherCursor);
                                        // Its execution ran to the end uncancelled, while the paused one's
                                        // stays cancelled.
                                        Assert.assertTrue(otherJoin.isExecutionActive());
                                        Assert.assertFalse(join.isExecutionActive());
                                    }
                                    Assert.assertEquals(expectedOther[1], rows[1]);
                                    Assert.assertEquals(expectedOther[0], rows[0]);
                                    otherRows.set(rows[1]);
                                }
                            } catch (Throwable th) {
                                errors.add(th);
                            } finally {
                                otherDone.countDown();
                            }
                        });
                        thread.start();
                        try {
                            if (!otherDone.await(60, TimeUnit.SECONDS)) {
                                errors.add(new AssertionError("the other query did not finish"));
                            }
                            thread.join();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            errors.add(e);
                        }
                    };
                    pause = new Pause(join, firstRowOf(2, false, false), Thread.currentThread(), runOther);
                    router.pause = pause;
                    try (RecordCursor cursor = factory.getCursor(context)) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertTrue(pause.awaitDecided());
                        Assert.assertEquals(FRAME_COUNT, join.getDispatchedFrameCount());
                    }
                } while (!pause.isPaused());
                Assert.assertTrue(errors.isEmpty() ? "" : errors.peek().toString(), errors.isEmpty());
                Assert.assertEquals(expectedOther[1], otherRows.get());
                pause.assertStoppedAtTheNextCheck(sql);
                Assert.assertEquals(0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                router.pause = null;
                final long[] actual = readAll(factory, context);
                Assert.assertEquals(expected[1], actual[1]);
                Assert.assertEquals(expected[0], actual[0]);
            }
            Assert.assertEquals(0, db.getBusyReaderCount());
        }));
    }

    // Each loop over a frame's rows, a task's, over the frame's rows and over the rows its probe
    // filter kept, forward and backward, on the stages' hidden stepper copies and on the shared
    // template: the cursor reads its first row and closes while a worker holds frame 2's task, whose
    // close() cancels; the paused task stops 4,095 rows later, close() returns once it did, and the
    // factory reads every row again.
    private void assertCloseStopsRunningTasks(WorkerPoolMode mode) throws Exception {
        setUpExecution();
        assertMemoryLeak(() -> executeOnWorkers(mode, WORKER_COUNT, null, configuration, LOG, (db, context) -> {
            context.setParallelHashJoinEnabled(true);
            context.changePageFrameSizes(FRAME_ROWS, FRAME_ROWS);
            createTables(db, context);
            final Router router = new Router();
            TestLatchedCounterFunctionFactory.reset(router);
            for (boolean isCopy : new boolean[]{true, false}) {
                for (boolean isFiltered : new boolean[]{false, true}) {
                    for (boolean isDescending : new boolean[]{false, true}) {
                        final String sql = joinSql(isFiltered, isDescending);
                        final String label = sql + ", copies: " + isCopy;
                        final String expected = ordinary(sql, context);
                        useSteppers(isCopy);
                        try (RecordCursorFactory factory = db.select(sql, context)) {
                            final AsyncHashJoinRecordCursorFactory join = find(factory);
                            Assert.assertNotNull(label, join);
                            int attempts = 0;
                            Pause pause;
                            do {
                                Assert.assertTrue(label + ": the owner joined the row at every attempt", ++attempts <= MAX_ATTEMPTS);
                                pause = new Pause(join, firstRowOf(2, isFiltered, isDescending), Thread.currentThread(), null);
                                router.pause = pause;
                                try (RecordCursor cursor = factory.getCursor(context)) {
                                    Assert.assertTrue(label, cursor.hasNext());
                                    Assert.assertTrue(label, pause.awaitDecided());
                                    Assert.assertEquals(label, FRAME_COUNT, join.getDispatchedFrameCount());
                                    Assert.assertTrue(label, join.isExecutionActive());
                                }
                                Assert.assertFalse(label, join.isExecutionActive());
                            } while (!pause.isPaused());
                            pause.assertStoppedAtTheNextCheck(label);
                            Assert.assertEquals(label, isCopy, join.getStepperClass(0).isHidden());
                            Assert.assertEquals(label, 0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                            Assert.assertEquals(label, 0, db.getBusyReaderCount());
                            // The next execution reads every row, as the ordinary plan does.
                            router.pause = null;
                            Assert.assertEquals(label, expected, result(factory, context));
                        } finally {
                            router.pause = null;
                            restoreSteppers();
                        }
                    }
                }
            }
        }));
    }

    // Threads that share the query workers each run, over factories they reuse, short reads that
    // close while their tasks join frames, full reads, counts, and reads that go back to the top
    // halfway: no execution that another one's cancellation could reach loses a row, every read
    // returns the ordinary plan's rows in order, and no execution leaves a slot or a reader behind.
    private void assertConcurrentEarlyCloses(WorkerPoolMode mode) throws Exception {
        setUpExecution();
        // Small queues, which the threads fill, so that owners also join frames themselves.
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 4);
        assertMemoryLeak(() -> executeOnWorkers(mode, WORKER_COUNT, null, configuration, LOG, (db, context) -> {
            context.setParallelHashJoinEnabled(true);
            context.changePageFrameSizes(FRAME_ROWS, FRAME_ROWS);
            createTables(db, context);
            final String[] queries = {
                    PLAIN_JOIN,
                    "SELECT t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w > t.v % 500",
                    PLAIN_JOIN + " WHERE t.v % 3 = 0",
                    PLAIN_JOIN + " ORDER BY t.ts DESC",
                    "SELECT t.v, e.z FROM t JOIN d ON t.k = d.k LEFT JOIN e ON t.k = e.k",
            };
            final long[][] expected = new long[queries.length][];
            try (SqlExecutionContextImpl ordinaryContext = newContext(db)) {
                ordinaryContext.setParallelHashJoinEnabled(false);
                for (int q = 0; q < queries.length; q++) {
                    try (RecordCursorFactory factory = db.select(queries[q], ordinaryContext)) {
                        expected[q] = readAll(factory, ordinaryContext);
                    }
                }
            }
            final Rnd seeds = TestUtils.generateRandom(LOG);
            final int threadCount = 6;
            final int iterations = 24;
            final CyclicBarrier start = new CyclicBarrier(threadCount);
            final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
            final AtomicInteger earlyCloses = new AtomicInteger();
            final AtomicInteger fullReads = new AtomicInteger();
            final ObjList<Thread> threads = new ObjList<>();
            for (int i = 0; i < threadCount; i++) {
                final long seed0 = seeds.nextLong();
                final long seed1 = seeds.nextLong();
                final Thread thread = new Thread(() -> {
                    final Rnd rnd = new Rnd(seed0, seed1);
                    final ObjList<RecordCursorFactory> factories = new ObjList<>();
                    try (SqlExecutionContextImpl threadContext = newContext(db)) {
                        for (String sql : queries) {
                            factories.add(db.select(sql, threadContext));
                        }
                        start.await(60, TimeUnit.SECONDS);
                        for (int n = 0; n < iterations; n++) {
                            final int q = rnd.nextInt(queries.length);
                            final String label = queries[q] + ", seeds: " + seed0 + "L, " + seed1 + "L, iteration " + n;
                            final RecordCursorFactory factory = factories.getQuick(q);
                            final AsyncHashJoinRecordCursorFactory join = findAll(factory, threadContext).getQuick(0);
                            switch (rnd.nextInt(4)) {
                                case 0 -> {
                                    // A short read that leaves frames in flight, then a close.
                                    final long rows = 1 + rnd.nextInt((int) Math.min(3 * FRAME_ROWS, expected[q][1] - 1));
                                    try (RecordCursor cursor = factory.getCursor(threadContext)) {
                                        for (long r = 0; r < rows; r++) {
                                            Assert.assertTrue(label, cursor.hasNext());
                                        }
                                    }
                                    Assert.assertFalse(label, join.isExecutionActive());
                                    earlyCloses.incrementAndGet();
                                }
                                case 1 -> {
                                    final long[] actual = readAll(factory, threadContext);
                                    Assert.assertEquals(label, expected[q][1], actual[1]);
                                    Assert.assertEquals(label, expected[q][0], actual[0]);
                                    fullReads.incrementAndGet();
                                }
                                case 2 -> {
                                    try (RecordCursor cursor = factory.getCursor(threadContext)) {
                                        final RecordCursor.Counter counter = new RecordCursor.Counter();
                                        cursor.calculateSize(threadContext.getCircuitBreaker(), counter);
                                        Assert.assertEquals(label, expected[q][1], counter.get());
                                    }
                                }
                                default -> {
                                    // Half a frame, then back to the top, and every row.
                                    try (RecordCursor cursor = factory.getCursor(threadContext)) {
                                        for (long r = 0, rows = 1 + rnd.nextInt(FRAME_ROWS); r < rows; r++) {
                                            Assert.assertTrue(label, cursor.hasNext());
                                        }
                                        cursor.toTop();
                                        final long[] actual = readAll(cursor);
                                        Assert.assertEquals(label, expected[q][1], actual[1]);
                                        Assert.assertEquals(label, expected[q][0], actual[0]);
                                    }
                                }
                            }
                            Assert.assertEquals(label, 0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                        }
                    } catch (Throwable th) {
                        errors.add(th);
                    } finally {
                        for (int f = 0, m = factories.size(); f < m; f++) {
                            factories.getQuick(f).close();
                        }
                    }
                });
                threads.add(thread);
                thread.start();
            }
            for (int i = 0; i < threadCount; i++) {
                threads.getQuick(i).join();
            }
            for (Throwable th : errors) {
                LOG.error().$("concurrent read failed: ").$(th).$();
            }
            Assert.assertTrue(errors.isEmpty() ? "" : errors.peek().toString(), errors.isEmpty());
            Assert.assertTrue("early closes: " + earlyCloses.get(), earlyCloses.get() > 0);
            Assert.assertTrue("full reads: " + fullReads.get(), fullReads.get() > 0);
            Assert.assertEquals(0, db.getBusyReaderCount());
            // Every factory of the threads' closed; one more execution on a fresh one is whole.
            try (RecordCursorFactory factory = db.select(queries[0], context)) {
                final long[] actual = readAll(factory, context);
                Assert.assertEquals(expected[0][1], actual[1]);
                Assert.assertEquals(expected[0][0], actual[0]);
            }
        }));
    }

    private void setUpExecution() {
        // Room for every frame in flight at once, so that the first row dispatches them all.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 2 * FRAME_COUNT);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 2 * FRAME_COUNT);
    }

    /**
     * Pauses the worker that joins the probe row of the given value until the execution was
     * cancelled, and counts the residual's calls that the worker makes after it resumed. The
     * execution's owner never pauses: when it joins the row itself, nothing pauses, and the test runs
     * the execution again. With {@link #failAt(long)}, the thread that joins the row of that value
     * fails its task, once the pause was decided, which cancels the execution.
     */
    private static final class Pause {
        private final AtomicLong callsAfterResume = new AtomicLong();
        private final CountDownLatch decided = new CountDownLatch(1);
        private final AsyncHashJoinRecordCursorFactory join;
        @Nullable
        private final Runnable onCancelSeen;
        private final Thread owner;
        private final long value;
        private long failValue = -1;
        private volatile boolean isResumed;
        private volatile boolean isTimedOut;
        private volatile Thread pausedThread;

        Pause(AsyncHashJoinRecordCursorFactory join, long value, Thread owner, @Nullable Runnable onCancelSeen) {
            this.join = join;
            this.value = value;
            this.owner = owner;
            this.onCancelSeen = onCancelSeen;
        }

        // Fails the task that joins the row of the given value, once the pause was decided.
        void failAt(long failValue) {
            this.failValue = failValue;
        }

        boolean onGet(Record rec) {
            final Thread current = Thread.currentThread();
            if (isResumed) {
                if (current == pausedThread) {
                    callsAfterResume.incrementAndGet();
                }
                return true;
            }
            final long v = rec.getLong(0);
            if (v == failValue) {
                try {
                    if (!decided.await(60, TimeUnit.SECONDS)) {
                        isTimedOut = true;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                throw CairoException.nonCritical().put("injected failure at ").put(v);
            }
            if (v != value) {
                return true;
            }
            if (current == owner) {
                decided.countDown();
                return true;
            }
            pausedThread = current;
            decided.countDown();
            final long deadline = System.nanoTime() + WAIT_NANOS;
            while (join.isExecutionActive()) {
                if (System.nanoTime() > deadline) {
                    isTimedOut = true;
                    break;
                }
                Os.pause();
            }
            if (onCancelSeen != null) {
                onCancelSeen.run();
            }
            isResumed = true;
            return true;
        }

        void assertStoppedAtTheNextCheck(String label) {
            Assert.assertFalse(label + ": the execution was not cancelled", isTimedOut);
            Assert.assertEquals(label, ROWS_AFTER_PAUSE, callsAfterResume.get());
        }

        boolean awaitDecided() throws InterruptedException {
            return decided.await(60, TimeUnit.SECONDS);
        }

        boolean isPaused() {
            return pausedThread != null;
        }
    }

    /**
     * The callback that the residual's function takes as its query compiles: it hands each call on
     * to the pause of the current attempt, none between attempts.
     */
    private static final class Router implements TestLatchedCounterFunctionFactory.Callback {
        private volatile Pause pause;

        @Override
        public boolean onGet(Record rec, int count) {
            final Pause current = pause;
            return current == null || current.onGet(rec);
        }
    }
}
