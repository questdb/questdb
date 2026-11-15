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
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.test.TestLatchedCounterFunctionFactory;
import io.questdb.griffin.engine.table.AsyncHashJoinAtom;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.HashJoinBuildStage;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Rows;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.test.griffin.ParallelHashJoinTestUtils.*;

/**
 * The parallel hash join's failure paths, each followed by a check that the failure released
 * everything it held and that the same factory, and the tables, still work: cancellation inside
 * each round of a parallel build, inside a probe task and inside a continuation chunk, and inside
 * the build of an execution between one that walks a stage's matches and one that hands the stage's
 * chains on, which leaves the next execution to decide afresh; a small
 * build's filter that runs interpreted and fails in the first round of the build's rounds; per-query
 * memory limit breaches in the build, the symbol key translator, the payload copy, a probe task,
 * a continuation, the consumer's Parquet output reader and the pass that indexes the builds' chain
 * lengths for a count that follows some rows; failing file operations on the build
 * input's, the probe input's and a later stage's build input's native and Parquet frames; and a
 * later stage's table dropped and created again under a cached stacked factory.
 */
public class AsyncHashJoinFailureTest extends AbstractCairoTest {
    private static final String STAGE = HashJoinBuildStage.class.getName();
    private static final String ATOM = AsyncHashJoinAtom.class.getName();
    // A single stage, a stack, a stack whose first join runs an ON residual on the workers, and a
    // stack whose first join swaps its inputs: build_tab, the smaller one, is its left input, so the
    // execution hashes it and streams probe_tab, and the later stage's key reads build_tab's column
    // through the first stage's payload. Every query probes probe_tab, and every stack's later stage
    // hashes later_tab.
    private static final String[] PROBE_AND_LATER_STAGE_QUERIES = {
            "SELECT t.v, b.w FROM probe_tab t LEFT JOIN build_tab b ON t.k = b.k",
            "SELECT t.v, b.w, l.w2 FROM probe_tab t LEFT JOIN build_tab b ON t.k = b.k JOIN later_tab l ON t.l = l.l",
            "SELECT t.v, b.w, l.w2 FROM probe_tab t LEFT JOIN build_tab b ON t.k = b.k AND b.w > t.v % 9 LEFT JOIN later_tab l ON t.l = l.l",
            "SELECT b.w, t.v, l.w2 FROM build_tab b JOIN probe_tab t ON b.k = t.k LEFT JOIN later_tab l ON b.l = l.l",
    };
    private static final String READER = "io.questdb.griffin.engine.table.HashJoinBuildFrames$Reader";
    private static final StackWalker STACK_WALKER = StackWalker.getInstance();

    @Override
    @Before
    public void setUp() {
        super.setUp();
        sqlExecutionContext.setParallelHashJoinEnabled(true);
    }

    @After
    public void tearDownFailures() {
        TestLatchedCounterFunctionFactory.reset(null);
        sqlExecutionContext.restoreToDefaultPageFrameSizes();
    }

    @Test
    public void testCancellationInsideEachBuildRound() throws Exception {
        // Every build runs in rounds, of many partitions, and copies its payload in a round too.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 0);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 16);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_PARALLEL_MIN_PROBE_RATIO, "0");
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(32, 32);
            // An INT key, and a LONG key that the build stages into its map.
            for (String key : new String[]{"k", "lk"}) {
                // The first round sorts each build frame's rows into partitions: the build's filter
                // trips the breaker from inside a task of that round, and the round's tasks after it
                // find the query cancelled before they run.
                final String filtered = "SELECT t.v, d.w FROM t LEFT JOIN (SELECT * FROM d WHERE test_latched_counter()) d ON t." + key + " = d." + key;
                final String expectedFiltered = ordinary(filtered, sqlExecutionContext);
                // The function takes the callback when it compiles; the callback trips the breaker
                // only once armed.
                final AtomicBooleanCircuitBreaker breaker = new AtomicBooleanCircuitBreaker(engine);
                final AtomicBoolean isArmed = new AtomicBoolean();
                final AtomicBoolean isInsideRound = new AtomicBoolean();
                final TestLatchedCounterFunctionFactory.Callback callback = new TestLatchedCounterFunctionFactory.Callback() {
                    @Override
                    public boolean onGet(Record rec, int count) {
                        if (isArmed.get() && count == 40) {
                            isInsideRound.set(isOnStack(STAGE, "partitionFrame"));
                            breaker.cancel();
                        }
                        return true;
                    }
                };
                TestLatchedCounterFunctionFactory.reset(callback);
                try (RecordCursorFactory factory = select(filtered)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(join);
                    TestUtils.assertContains(plan(factory, sqlExecutionContext), "buildFilter: test_latched_counter()");
                    // Restarts the count.
                    TestLatchedCounterFunctionFactory.reset(callback);
                    isArmed.set(true);
                    assertCancelled(factory, breaker, filtered);
                    isArmed.set(false);
                    Assert.assertTrue(isInsideRound.get());
                    // The build's 300 rows went through the filter only up to the cancelled task.
                    Assert.assertTrue(String.valueOf(TestLatchedCounterFunctionFactory.getCount()), TestLatchedCounterFunctionFactory.getCount() < 300);
                    assertReused(factory, join, filtered, expectedFiltered);
                }
                // The second round fills each partition's region of the heap and its key table, and
                // the third copies the payload of each build frame's rows.
                final String sql = "SELECT t.v, t.ts, d.w, d.s FROM t LEFT JOIN d ON t." + key + " = d." + key;
                final String expected = ordinary(sql, sqlExecutionContext);
                for (String round : new String[]{"buildPartition", "copyFramePayload"}) {
                    try (RecordCursorFactory factory = select(sql)) {
                        final AsyncHashJoinRecordCursorFactory join = find(factory);
                        Assert.assertNotNull(join);
                        final MethodCheckCircuitBreaker roundBreaker = new MethodCheckCircuitBreaker(engine, STAGE, round, 1);
                        assertCancelled(factory, roundBreaker, sql + ", " + round);
                        Assert.assertTrue(round, roundBreaker.getChecks() > 0);
                        assertReused(factory, join, sql, expected);
                    }
                }
            }
        });
    }

    @Test
    public void testCancellationInsideBuildBetweenWalkingAndDeferringExecutions() throws Exception {
        // Every build runs in rounds, of many partitions, and a cap of one tuple, which a task lifts to
        // its frame's rows, leaves the rest of a frame with more pairs than rows to a continuation.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 0);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 16);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(32, 32);
            // test_latched_counter() reads no column, so it leads the residual's conjuncts of the build
            // alone. An execution whose build meets a failure of it gives the conjuncts up and walks d's
            // matches under the whole residual, past the cap; any other one keeps d's rows of
            // w % 3 = 0, which repeat each key, and hands their chains on, one tuple per probe row.
            final String sql = "SELECT t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND test_latched_counter() AND d.w % 3 = 0";
            final AtomicBoolean isFailing = new AtomicBoolean();
            TestLatchedCounterFunctionFactory.reset(new TestLatchedCounterFunctionFactory.Callback() {
                @Override
                public boolean onGet(Record rec, int count) {
                    if (isFailing.get() && isOnStack(STAGE, "applyAbandonableBuildOnFilter")) {
                        throw CairoException.nonCritical().put("test value failure");
                    }
                    return true;
                }
            });
            final String expected = ordinary(sql, sqlExecutionContext);
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                TestUtils.assertContains(plan(factory, sqlExecutionContext), "buildOnFilter: (test_latched_counter() and w%3=0)");
                // Before each execution, one that the breaker cancels inside its build's second round,
                // after the first ran the conjuncts: it decides nothing, and the next execution decides
                // afresh, after an execution that walked or one that handed its chains on.
                for (boolean isWalking : new boolean[]{true, false, true}) {
                    final MethodCheckCircuitBreaker breaker = new MethodCheckCircuitBreaker(engine, STAGE, "buildPartition", 1);
                    assertCancelled(factory, breaker, sql);
                    Assert.assertTrue(breaker.getChecks() > 0);
                    isFailing.set(isWalking);
                    try {
                        assertReused(factory, join, sql, expected);
                    } finally {
                        isFailing.set(false);
                    }
                    Assert.assertEquals(sql, isWalking, join.getAtom().getContinuationCount() > 0);
                }
            }
        });
    }

    @Test
    public void testCancellationInsideProbeTaskAndContinuation() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // Frames of eight rows, whose tasks check the breaker every eight pairs, and a cap of one
            // tuple, which leaves the rest of a frame with more pairs than rows to a continuation.
            sqlExecutionContext.changePageFrameSizes(8, 8);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 1);
            // The residual reads both inputs, so the operator runs it for each pair on the workers,
            // and its first operand counts the pairs.
            final String[] queries = {
                    "SELECT t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND (test_latched_counter() OR d.w = t.v)",
                    "SELECT t.v, d.w FROM t JOIN d ON t.k = d.k WHERE t.ts IN '2024-01-01' AND (test_latched_counter() OR d.w = t.v)",
            };
            for (String sql : queries) {
                final String expected = ordinary(sql, sqlExecutionContext);
                for (String method : new String[]{"probeFrame", "continueFrame"}) {
                    // The function takes the callback when it compiles; the callback trips the
                    // breaker only once armed.
                    final AtomicBooleanCircuitBreaker breaker = new AtomicBooleanCircuitBreaker(engine);
                    final AtomicBoolean isArmed = new AtomicBoolean();
                    final AtomicInteger cancelledAt = new AtomicInteger(-1);
                    final TestLatchedCounterFunctionFactory.Callback callback = new TestLatchedCounterFunctionFactory.Callback() {
                        @Override
                        public boolean onGet(Record rec, int count) {
                            // The first pair that the task, or a continuation chunk, runs the
                            // residual over trips the breaker.
                            if (isArmed.get() && cancelledAt.get() < 0 && isOnStack(ATOM, method)
                                    && (method.equals("continueFrame") || !isOnStack(ATOM, "continueFrame"))) {
                                cancelledAt.set(count);
                                breaker.cancel();
                            }
                            return true;
                        }
                    };
                    TestLatchedCounterFunctionFactory.reset(callback);
                    try (RecordCursorFactory factory = select(sql)) {
                        final AsyncHashJoinRecordCursorFactory join = find(factory);
                        Assert.assertNotNull(join);
                        TestUtils.assertContains(plan(factory, sqlExecutionContext), "test_latched_counter()");
                        TestLatchedCounterFunctionFactory.reset(callback);
                        isArmed.set(true);
                        assertCancelled(factory, breaker, sql + ", " + method);
                        isArmed.set(false);
                        Assert.assertTrue(method, cancelledAt.get() > 0);
                        // The stepper observes the cancellation within the eight pairs that follow.
                        Assert.assertTrue(method + ": " + TestLatchedCounterFunctionFactory.getCount(), TestLatchedCounterFunctionFactory.getCount() < cancelledAt.get() + 8);
                        Assert.assertEquals(method, method.equals("continueFrame") ? 1 : 0, join.getAtom().getContinuationCount());
                        assertReused(factory, join, sql, expected);
                        Assert.assertTrue(join.getAtom().getContinuationCount() > 0);
                    }
                }
            }
        });
    }

    @Test
    public void testFileFailuresOnNativeBuildFrames() throws Exception {
        final AtomicBoolean isArmed = new AtomicBoolean();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (isArmed.get() && Utf8s.containsAscii(name, "build_tab") && Utf8s.endsWithAscii(name, "w.d")) {
                    return -1;
                }
                return super.openRO(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            createFileTables();
            // The LEFT join hashes build_tab; the INNER join, whose left input is the smaller one,
            // hashes build_tab as its left input; the build's column file fails to open in both.
            for (String sql : new String[]{
                    "SELECT t.v, b.w FROM probe_tab t LEFT JOIN build_tab b ON t.k = b.k",
                    "SELECT b.w, t.v FROM build_tab b JOIN probe_tab t ON b.k = t.k",
            }) {
                assertFileFailure(sql, isArmed, "could not open");
            }
        });
    }

    @Test
    public void testFileFailuresOnParquetBuildFrames() throws Exception {
        final AtomicBoolean isArmed = new AtomicBoolean();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (isArmed.get() && Utf8s.containsAscii(name, "build_tab") && Utf8s.endsWithAscii(name, ".parquet")) {
                    return -1;
                }
                return super.openRO(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            createFileTables();
            execute("ALTER TABLE build_tab CONVERT PARTITION TO PARQUET WHERE ts >= 0");
            for (String sql : new String[]{
                    "SELECT t.v, b.w FROM probe_tab t LEFT JOIN build_tab b ON t.k = b.k",
                    "SELECT b.w, t.v FROM build_tab b JOIN probe_tab t ON b.k = t.k",
            }) {
                assertFileFailure(sql, isArmed, "could not open");
            }
        });
    }

    @Test
    public void testFileFailuresOnProbeAndLaterStageInputs() throws Exception {
        final TableFilesFacade ff = new TableFilesFacade();
        assertMemoryLeak(ff, () -> {
            createFileTables();
            execute("CREATE TABLE later_tab AS (SELECT (x % 5)::LONG l, x w2, timestamp_sequence('2024-01-01', 1_800_000_000) ts FROM long_sequence(100)) TIMESTAMP(ts) PARTITION BY DAY");
            sqlExecutionContext.changePageFrameSizes(16, 16);
            for (boolean isParquet : new boolean[]{false, true}) {
                if (isParquet) {
                    execute("ALTER TABLE probe_tab CONVERT PARTITION TO PARQUET WHERE ts >= 0");
                    execute("ALTER TABLE later_tab CONVERT PARTITION TO PARQUET WHERE ts >= 0");
                }
                // The probe's partitions open once every build is done, and a later stage's build
                // input opens once stage 0 holds its build cursor and the probe's scan.
                for (String[] target : new String[][]{{"probe_tab", isParquet ? ".parquet" : "v.d"}, {"later_tab", isParquet ? ".parquet" : "w2.d"}}) {
                    for (boolean isMmap : new boolean[]{false, true}) {
                        for (String sql : PROBE_AND_LATER_STAGE_QUERIES) {
                            if (target[0].equals("later_tab") && !sql.contains("later_tab")) {
                                continue;
                            }
                            ff.target(target[0], target[1], isMmap);
                            assertTableFileFailure(sql, ff, target[0], isMmap);
                        }
                    }
                }
            }
            // The swapped stack swapped, so its probe was probe_tab.
            try (RecordCursorFactory factory = select(PROBE_AND_LATER_STAGE_QUERIES[3])) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertTrue(join.getAtom().isSwapped());
                }
            }
            // The tables still work on their own.
            assertQuery("SELECT count() FROM probe_tab").noLeakCheck().noRandomAccess().expectSize().returns("count\n400\n");
            assertQuery("SELECT count() FROM later_tab").noLeakCheck().noRandomAccess().expectSize().returns("count\n100\n");
        });
    }

    @Test
    public void testCloseDropsFramesThatNoWorkerStarted() throws Exception {
        // A window of 32 frames of 16 rows; two workers each hold a frame past the first one inside
        // the probe filter, so the frames behind them wait in the queue, until the consumer, which
        // read the first row, closes its cursor and waits for the frames in flight. The frames that
        // no worker started by then never run the probe filter.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 32);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 64);
        for (WorkerPoolMode mode : WorkerPoolMode.values()) {
            assertMemoryLeak(() -> TestUtils.execute(new TestWorkerPool(2, mode), (db, compiler, ignore) -> {
                try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(db, 2)
                        .with(AllowAllSecurityContext.INSTANCE, null, null, -1, null)) {
                    context.setParallelHashJoinEnabled(true);
                    context.changePageFrameSizes(16, 16);
                    // 64 frames of t, all in one partition, so that a row id's partition index is
                    // its frame's index.
                    final String t = "t_" + mode.name().toLowerCase();
                    final String d = "d_" + mode.name().toLowerCase();
                    db.execute("CREATE TABLE " + t + " AS (SELECT (x % 50)::INT k, x v, timestamp_sequence('2024-01-01', 1_000_000) ts"
                            + " FROM long_sequence(1_024)) TIMESTAMP(ts) PARTITION BY DAY", context);
                    db.execute("CREATE TABLE " + d + " AS (SELECT (x % 40)::INT k, x w FROM long_sequence(300))", context);
                    final String sql = "SELECT t.v, d.w FROM " + t + " t JOIN " + d + " d ON t.k = d.k WHERE (test_latched_counter() OR t.v < 0)";
                    final Thread owner = Thread.currentThread();
                    final Set<Integer> startedFrames = ConcurrentHashMap.newKeySet();
                    final AtomicBoolean isClosing = new AtomicBoolean();
                    final AtomicInteger startsAfterClose = new AtomicInteger();
                    final AtomicInteger heldWorkers = new AtomicInteger();
                    TestLatchedCounterFunctionFactory.reset(new TestLatchedCounterFunctionFactory.Callback() {
                        @Override
                        public boolean onGet(Record rec, int count) {
                            final int frame = Rows.toPartitionIndex(rec.getRowId());
                            if (startedFrames.add(frame) && isClosing.get()) {
                                startsAfterClose.incrementAndGet();
                            }
                            if (frame > 0 && Thread.currentThread() != owner && !isClosing.get()) {
                                // The worker holds its frame until the consumer's close() cancelled the
                                // frames and waits for those in flight.
                                heldWorkers.incrementAndGet();
                                while (!isClosing.get()) {
                                    if (isAwaitingFrames(owner)) {
                                        isClosing.set(true);
                                    } else {
                                        Thread.onSpinWait();
                                    }
                                }
                            }
                            return true;
                        }
                    });
                    try (RecordCursorFactory factory = db.select(sql, context)) {
                        final AsyncHashJoinRecordCursorFactory join = find(factory);
                        Assert.assertNotNull(join);
                        TestUtils.assertContains(plan(factory, context), "filter: (test_latched_counter() or v<0)");
                        try (RecordCursor cursor = factory.getCursor(context)) {
                            Assert.assertTrue(cursor.hasNext());
                            Assert.assertEquals(64, join.getProbeFrameCount());
                            // Both workers hold a frame, with the rest of the window queued behind them.
                            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
                            while (heldWorkers.get() < 2) {
                                Assert.assertTrue("the workers took no frame", System.nanoTime() < deadline);
                                Thread.onSpinWait();
                            }
                        }
                        // The workers held their frames, and only the frame that a worker took before
                        // the cancellation, after the first one, started once the consumer closed.
                        Assert.assertEquals(2, heldWorkers.get());
                        Assert.assertTrue("frames started after close: " + startsAfterClose.get(), startsAfterClose.get() <= 1);
                        Assert.assertTrue("frames started: " + startedFrames.size(), startedFrames.size() < 32);
                        assertReleased(join, db);
                        TestLatchedCounterFunctionFactory.reset(null);
                        Assert.assertEquals(ordinary(sql, context), result(factory, context));
                    } finally {
                        TestLatchedCounterFunctionFactory.reset(null);
                    }
                }
            }, configuration, LOG));
        }
    }

    @Test
    public void testLengthFailuresOnParquetProbeBuildAndLaterStageFiles() throws Exception {
        // The length of a Parquet partition's data file, which the reader checks before it maps the
        // file, fails for the probe, for the build, and for a later stage's build input.
        final TableFilesFacade ff = new TableFilesFacade();
        assertMemoryLeak(ff, () -> {
            createFileTables();
            execute("CREATE TABLE later_tab AS (SELECT (x % 5)::LONG l, x w2, timestamp_sequence('2024-01-01', 1_800_000_000) ts FROM long_sequence(100)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("ALTER TABLE probe_tab CONVERT PARTITION TO PARQUET WHERE ts >= 0");
            execute("ALTER TABLE build_tab CONVERT PARTITION TO PARQUET WHERE ts >= 0");
            execute("ALTER TABLE later_tab CONVERT PARTITION TO PARQUET WHERE ts >= 0");
            sqlExecutionContext.changePageFrameSizes(16, 16);
            for (String table : new String[]{"probe_tab", "build_tab", "later_tab"}) {
                for (String sql : PROBE_AND_LATER_STAGE_QUERIES) {
                    if (table.equals("later_tab") && !sql.contains("later_tab")) {
                        continue;
                    }
                    ff.targetLength(table, ".parquet");
                    final String expected = ordinary(sql, sqlExecutionContext);
                    try (RecordCursorFactory factory = select(sql)) {
                        final AsyncHashJoinRecordCursorFactory join = find(factory);
                        Assert.assertNotNull(sql, join);
                        for (int run = 0; run < 2; run++) {
                            // The readers open the partitions anew, through the failing facade.
                            engine.releaseAllReaders();
                            ff.arm();
                            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                //noinspection StatementWithEmptyBody
                                while (cursor.hasNext()) {
                                }
                                Assert.fail("expected the length of " + table + "'s Parquet file to fail: " + sql);
                            } catch (CairoException e) {
                                TestUtils.assertContains(e.getFlyweightMessage(), "parquet partition data file missing");
                                TestUtils.assertContains(e.getFlyweightMessage(), table);
                            } finally {
                                ff.disarm();
                            }
                            Assert.assertTrue(sql, ff.getFailures() > 0);
                            assertReleased(join);
                            Assert.assertEquals(sql, expected, result(factory, sqlExecutionContext));
                        }
                    }
                }
            }
            // The tables still work on their own.
            assertQuery("SELECT count() FROM probe_tab").noLeakCheck().noRandomAccess().expectSize().returns("count\n400\n");
            assertQuery("SELECT count() FROM build_tab").noLeakCheck().noRandomAccess().expectSize().returns("count\n200\n");
            assertQuery("SELECT count() FROM later_tab").noLeakCheck().noRandomAccess().expectSize().returns("count\n100\n");
        });
    }

    @Test
    public void testInterpretedBuildFilterFailsInRounds() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // bf holds 30 rows over three days, far fewer than the configured 300_000. p * 1000 does
            // not fit DECIMAL(4,0) for the one row with p = 10, and p * 100 fits for every row. The
            // filter runs interpreted, so the build runs in rounds, whose first round filters the
            // frames: the round fails there, before it partitions any row.
            execute("""
                    CREATE TABLE bf AS (
                        SELECT (x % 40)::INT k, CASE WHEN x = 17 THEN 10 ELSE x % 10 END p, timestamp_sequence('2024-01-01', 8_640_000_000) ts
                        FROM long_sequence(30)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            for (String join : new String[]{" JOIN ", " LEFT JOIN "}) {
                final String sql = "SELECT t.k, t.v, b.p FROM t" + join + "(SELECT * FROM bf WHERE (p * $1)::DECIMAL(4,0) > 0::DECIMAL(4,0)) b ON t.k = b.k";
                bindVariableService.clear();
                bindVariableService.setLong(0, 1_000);
                final String expected = error(sql, false);
                TestUtils.assertContains(expected, "inconvertible value: 10000 [LONG -> DECIMAL(4,0)]");
                try (RecordCursorFactory factory = select(sql)) {
                    final AsyncHashJoinRecordCursorFactory asyncJoin = find(factory);
                    Assert.assertNotNull(sql, asyncJoin);
                    TestUtils.assertContains(plan(factory, sqlExecutionContext), "buildFilter: ");
                    final HashJoinBuildStage stage = asyncJoin.getAtom().getStage(0);
                    for (int run = 0; run < 2; run++) {
                        final long rounds = stage.getRoundCount();
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            //noinspection StatementWithEmptyBody
                            while (cursor.hasNext()) {
                            }
                            Assert.fail("expected the build filter to fail: " + sql);
                        } catch (CairoException | ImplicitCastException e) {
                            Assert.assertEquals(sql, expected, e.getPosition() + ": " + e.getFlyweightMessage());
                        }
                        Assert.assertEquals(sql, rounds + 1, stage.getRoundCount());
                        assertReleased(asyncJoin);
                    }
                    // The same factory, with a value that every row passes, builds in rounds and reads
                    // the ordinary plan's rows.
                    bindVariableService.setLong(0, 100);
                    Assert.assertEquals(sql, ordinary(sql, sqlExecutionContext), result(factory, sqlExecutionContext));
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertTrue(stage.isBuiltInRounds());
                    }
                }
            }
            assertQuery("SELECT count() FROM bf").noLeakCheck().noRandomAccess().expectSize().returns("count\n30\n");
        });
    }

    @Test
    public void testLaterStageTableRecreatedUnderCachedStack() throws Exception {
        assertMemoryLeak(() -> {
            createFileTables();
            execute("CREATE TABLE later_tab AS (SELECT (x % 5)::LONG l, x w2 FROM long_sequence(100))");
            final String sql = PROBE_AND_LATER_STAGE_QUERIES[1];
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                Assert.assertEquals(2, join.getSpec().getStageCount());
                Assert.assertEquals(ordinary(sql, sqlExecutionContext), result(factory, sqlExecutionContext));
                execute("DROP TABLE later_tab");
                execute("CREATE TABLE later_tab AS (SELECT (x % 5)::LONG l, -x w2 FROM long_sequence(10))");
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    cursor.hasNext();
                    Assert.fail("expected a stale table reference");
                } catch (TableReferenceOutOfDateException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "later_tab");
                }
                // Stage 0 had adopted its build scan, and the probe's sequence its scan, when the
                // later stage's scan failed to open; the failure released them with the rest.
                assertReleased(join);
            }
            // A new compilation reads the new table.
            final String expected = ordinary(sql, sqlExecutionContext);
            TestUtils.assertContains(expected, "-1");
            try (RecordCursorFactory factory = select(sql)) {
                Assert.assertNotNull(find(factory));
                Assert.assertEquals(expected, result(factory, sqlExecutionContext));
            }
        });
    }

    @Test
    public void testMemoryLimitInIndexPassOfCountAfterRows() throws Exception {
        // A count that follows some rows, as a page of an HTTP request with count=true does, holds the
        // builds and the frames whose tuples it dispatched for the rows, and then indexes the chain
        // lengths of the builds whose stages hand their chains on, 4 bytes a build row. A per-query
        // memory limit one byte short of what the indexes take fails the count inside the pass, and
        // the failure releases everything. A count from the top opens no ring entry, so it may fit
        // under a limit that the rows do not.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 2);
        // A ring budget below a quarter of the limit below, which leaves the ring the same budget, and
        // the same window, as it has without a limit.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TUPLE_RING_MAX_SIZE, 512);
        assertMemoryLeak(() -> {
            createTables();
            // m and d2 repeat their keys: 8 and 15 rows.
            execute("CREATE TABLE m AS (SELECT CASE WHEN x <= 3 THEN 1 WHEN x = 4 THEN 2 WHEN x <= 6 THEN 3 ELSE x - 3 END::INT k, x w FROM long_sequence(8))");
            execute("CREATE TABLE d2 AS (SELECT 1 + x % 5 lk, x * 10 w2 FROM long_sequence(15))");
            sqlExecutionContext.changePageFrameSizes(4, 4);
            final String sql = "SELECT t.v, m.w, d2.w2 FROM t JOIN m ON t.k = m.k JOIN d2 ON t.lk = d2.lk";
            final String expected = ordinary(sql, sqlExecutionContext);
            final long rowCount = ordinaryRowCount(sql);
            final int prefix = 3;
            final long indexBytes = (8 + 15) * Integer.BYTES;
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                // Without a limit: the memory that the execution holds once it read the rows, and the
                // window of frames that it dispatched for them.
                final long used;
                final int dispatchLimit;
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    for (int i = 0; i < prefix; i++) {
                        Assert.assertTrue(cursor.hasNext());
                    }
                    final MemoryTracker tracker = sqlExecutionContext.getMemoryTracker();
                    Assert.assertNotNull(tracker);
                    used = tracker.getUsed();
                    dispatchLimit = join.getAtom().getRing().getDispatchLimit();
                    final RecordCursor.Counter counter = new RecordCursor.Counter();
                    cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                    Assert.assertEquals(rowCount - prefix, counter.get());
                }
                Assert.assertEquals(2, dispatchLimit);
                final MemoryTracker tracker;
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, used + indexBytes - 1);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    for (int i = 0; i < prefix; i++) {
                        Assert.assertTrue(cursor.hasNext());
                    }
                    tracker = sqlExecutionContext.getMemoryTracker();
                    Assert.assertNotNull(tracker);
                    // The rows took the same memory, and the same window, under the limit.
                    Assert.assertEquals(used, tracker.getUsed());
                    Assert.assertEquals(dispatchLimit, join.getAtom().getRing().getDispatchLimit());
                    try {
                        cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), new RecordCursor.Counter());
                        Assert.fail("expected the count to breach the limit inside the pass");
                    } catch (CairoException e) {
                        Assert.assertTrue(e.getMessage(), e.isOutOfMemory());
                        TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                        Assert.assertTrue(isThrownIn(e, "io.questdb.griffin.engine.join.HashJoinRowHeap", "indexChainLengths"));
                    }
                } finally {
                    setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0);
                }
                assertReleased(join);
                Assert.assertEquals(0, tracker.getUsed());
                // The same factory counts the rows, and reads them, without the limit.
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    for (int i = 0; i < prefix; i++) {
                        Assert.assertTrue(cursor.hasNext());
                    }
                    final RecordCursor.Counter counter = new RecordCursor.Counter();
                    cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                    Assert.assertEquals(rowCount - prefix, counter.get());
                }
                Assert.assertEquals(expected, result(factory, sqlExecutionContext));
            }
        });
    }

    @Test
    public void testMemoryLimitInParquetOutputReader() throws Exception {
        // Row groups of 20 rows, whose VARCHAR payload, of 250 characters a row, the consumer's output
        // reader decodes, with the whole Parquet decode budget, as it returns the rows; the payload
        // copy is off, and no probe task reads the VARCHAR.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 20);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MAX_SIZE, 0);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT ((x * 53) % 200 + 1)::INT k, x v, timestamp_sequence('2024-01-01', 60_000_000) ts " +
                    "FROM long_sequence(400)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE d AS (SELECT x::INT k, x w, (rpad('v' || x, 250, 'x'))::VARCHAR vc, " +
                    "timestamp_sequence('2024-01-01', 432_000_000) dts FROM long_sequence(200)) TIMESTAMP(dts) PARTITION BY DAY");
            execute("INSERT INTO d VALUES (-1, -1, 'last', '2024-02-01')");
            execute("ALTER TABLE d CONVERT PARTITION TO PARQUET WHERE dts < '2024-02-01'");
            sqlExecutionContext.changePageFrameSizes(64, 64);
            final String sql = "SELECT t.v, d.w, d.vc FROM t LEFT JOIN d ON t.k = d.k";
            final String expected = ordinary(sql, sqlExecutionContext);
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                final HashJoinBuildStage stage = join.getAtom().getStage(0);
                long fitting = 1024;
                while (!isFitting(factory, join, fitting, expected)) {
                    fitting *= 2;
                }
                final long step = Math.max(64, fitting / 1_000);
                int outputReader = 0;
                int other = 0;
                for (long limit = step; ; limit += step) {
                    setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, limit);
                    try {
                        Assert.assertEquals(expected, result(factory, sqlExecutionContext));
                        Assert.assertFalse(stage.isPayloadCopied());
                        break;
                    } catch (CairoException e) {
                        Assert.assertTrue(e.getMessage(), e.isOutOfMemory());
                        TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                        if (isOutputReaderDecode(e)) {
                            outputReader++;
                        } else {
                            other++;
                        }
                    } finally {
                        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0);
                    }
                    // Each failure released everything the execution held.
                    assertReleased(join);
                }
                Assert.assertTrue("output reader " + outputReader + ", other " + other, outputReader > 0);
                Assert.assertEquals(expected, result(factory, sqlExecutionContext));
            }
            assertQuery("SELECT count() FROM d").noLeakCheck().noRandomAccess().expectSize().returns("count\n201\n");
        });
    }

    @Test
    public void testMemoryLimitInEachPhaseInRounds() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 0);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 256);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_PARALLEL_MIN_PROBE_RATIO, "0");
        assertMemoryLimitInEachPhase(true);
    }

    @Test
    public void testMemoryLimitInEachPhaseOnOwner() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, Long.MAX_VALUE);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MIN_PROBE_RATIO, "0");
        assertMemoryLimitInEachPhase(false);
    }

    private static void assertCancelled(RecordCursorFactory factory, SqlExecutionCircuitBreaker breaker, String label) throws SqlException {
        final SqlExecutionCircuitBreaker previous = sqlExecutionContext.getCircuitBreaker();
        ((SqlExecutionContextImpl) sqlExecutionContext).with(breaker);
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            //noinspection StatementWithEmptyBody
            while (cursor.hasNext()) {
            }
            Assert.fail("expected cancellation: " + label);
        } catch (CairoException e) {
            Assert.assertTrue(label + ": " + e.getMessage(), e.isCancellation());
            TestUtils.assertContains(e.getFlyweightMessage(), "cancelled by user");
        } finally {
            ((SqlExecutionContextImpl) sqlExecutionContext).with(previous);
        }
        Assert.assertEquals(label, 0, engine.getBusyReaderCount());
    }

    // A breach of the consumer's output reader: its decode of a Parquet row group of the build, on
    // the consumer's thread, outside every probe task, build and payload copy.
    private static boolean isOutputReaderDecode(Throwable e) {
        boolean isReader = false;
        for (StackTraceElement frame : e.getStackTrace()) {
            final String className = frame.getClassName();
            if (className.equals(ATOM) || className.equals(STAGE)) {
                return false;
            }
            if (className.equals(READER) && frame.getMethodName().equals("record")) {
                isReader = true;
            }
        }
        return isReader;
    }

    // Nothing of the failed execution stays behind: no build of any stage, no slot, no ring byte and
    // no busy reader.
    private static void assertReleased(AsyncHashJoinRecordCursorFactory join, CairoEngine db) {
        for (int k = 0, n = join.getSpec().getStageCount(); k < n; k++) {
            Assert.assertNull("stage " + k, join.getAtom().getStage(k).getFrozenBuild());
        }
        Assert.assertEquals(0, join.getAtom().getRing().getAllocatedBytes());
        Assert.assertEquals(0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
        Assert.assertEquals(0, db.getBusyReaderCount());
    }

    // Whether the thread waits, inside a cursor's close(), for the frames that workers still reduce.
    private static boolean isAwaitingFrames(Thread thread) {
        for (StackTraceElement frame : thread.getStackTrace()) {
            if (frame.getClassName().equals(PageFrameSequence.class.getName()) && frame.getMethodName().equals("await")) {
                return true;
            }
        }
        return false;
    }

    private static void assertReleased(AsyncHashJoinRecordCursorFactory join) {
        for (int k = 0, n = join.getSpec().getStageCount(); k < n; k++) {
            Assert.assertNull("stage " + k, join.getAtom().getStage(k).getFrozenBuild());
        }
        Assert.assertEquals(0, join.getAtom().getRing().getAllocatedBytes());
        Assert.assertEquals(0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
        Assert.assertEquals(0, join.getBuildRoundAtom().getPerWorkerLocks().getAcquiredSlotCount());
        Assert.assertEquals(0, engine.getBusyReaderCount());
    }

    private void assertTableFileFailure(String sql, TableFilesFacade ff, String table, boolean isMmap) throws Exception {
        final String label = sql + ", " + table + (isMmap ? ", mmap" : ", open");
        final String expected = ordinary(sql, sqlExecutionContext);
        try (RecordCursorFactory factory = select(sql)) {
            final AsyncHashJoinRecordCursorFactory join = find(factory);
            Assert.assertNotNull(label, join);
            for (int run = 0; run < 2; run++) {
                // The readers open the partitions anew, through the failing facade.
                engine.releaseAllReaders();
                ff.arm();
                final RecordCursor cursor = factory.getCursor(sqlExecutionContext);
                try {
                    //noinspection StatementWithEmptyBody
                    while (cursor.hasNext()) {
                    }
                    Assert.fail("expected the table's files to fail: " + label);
                } catch (CairoException e) {
                    if (isMmap) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "could not mmap");
                    } else {
                        TestUtils.assertContains(e.getFlyweightMessage(), "could not open");
                        TestUtils.assertContains(e.getFlyweightMessage(), table);
                    }
                    // The failure released everything before the caller closes the cursor.
                    assertReleased(join);
                } finally {
                    ff.disarm();
                    cursor.close();
                }
                Assert.assertTrue(label, ff.getFailures() > 0);
                assertReleased(join);
                Assert.assertEquals(label, expected, result(factory, sqlExecutionContext));
            }
        }
    }

    private void assertFileFailure(String sql, AtomicBoolean isArmed, String error) throws Exception {
        final String expected = ordinary(sql, sqlExecutionContext);
        try (RecordCursorFactory factory = select(sql)) {
            final AsyncHashJoinRecordCursorFactory join = find(factory);
            Assert.assertNotNull(sql, join);
            for (int run = 0; run < 2; run++) {
                // The readers open the build's partitions anew, through the failing facade.
                engine.releaseAllReaders();
                isArmed.set(true);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    //noinspection StatementWithEmptyBody
                    while (cursor.hasNext()) {
                    }
                    Assert.fail("expected the build's frames to fail: " + sql);
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), error);
                    TestUtils.assertContains(e.getFlyweightMessage(), "build_tab");
                } finally {
                    isArmed.set(false);
                }
                Assert.assertNull(join.getAtom().getStage(0).getFrozenBuild());
                Assert.assertEquals(0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                Assert.assertEquals(0, engine.getBusyReaderCount());
                Assert.assertEquals(sql, expected, result(factory, sqlExecutionContext));
            }
        }
        // The tables still work on their own.
        assertQuery("SELECT count() FROM build_tab").noLeakCheck().noRandomAccess().expectSize().returns("count\n200\n");
    }

    // The failure released every slot, reader and build, and the same factory reads the ordinary
    // plan's rows again.
    private static void assertReused(RecordCursorFactory factory, AsyncHashJoinRecordCursorFactory join, String sql, String expected) throws SqlException {
        Assert.assertEquals(0, join.getBuildRoundAtom().getPerWorkerLocks().getAcquiredSlotCount());
        Assert.assertEquals(0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
        Assert.assertNull(join.getAtom().getStage(0).getFrozenBuild());
        Assert.assertEquals(sql, expected, result(factory, sqlExecutionContext));
    }

    // The probe's SYMBOL dictionary holds 5_001 texts, which the translator's cache is sized for.
    // The build holds 600 rows of the text 'hot' and 100 of 'warm', and t holds one row of each in
    // every frame of 64 rows, at positions 40 and 10. A task that reaches the cap of 64 tuples on
    // the frame's warm row leaves the frame to a continuation, whose first chunk ends on its hot row
    // and so needs more tuples than the task did.
    private static void createLimitTables() throws Exception {
        execute("""
                CREATE TABLE t AS (
                    SELECT x v,
                        (CASE WHEN x % 64 = 41 THEN 'hot' WHEN x % 64 = 11 THEN 'warm' ELSE 'k' || (x % 5_000) END)::SYMBOL s,
                        timestamp_sequence('2024-01-01', 1_000_000) ts
                    FROM long_sequence(3_200)
                ) TIMESTAMP(ts) PARTITION BY DAY
                """);
        execute("""
                CREATE TABLE d AS (
                    SELECT x w, (CASE WHEN x <= 600 THEN 'hot' WHEN x <= 700 THEN 'warm' ELSE 'k' || (x % 50) END)::SYMBOL s, x::INT k
                    FROM long_sequence(2_000)
                )
                """);
    }

    private static void createFileTables() throws Exception {
        execute("CREATE TABLE probe_tab AS (SELECT (x % 9)::INT k, (x % 5)::LONG l, x v, timestamp_sequence('2024-01-01', 600_000_000) ts FROM long_sequence(400)) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE build_tab AS (SELECT (x % 7)::INT k, (x % 6)::LONG l, x w, timestamp_sequence('2024-01-01', 1_800_000_000) ts FROM long_sequence(200)) TIMESTAMP(ts) PARTITION BY DAY");
    }

    private static void createTables() throws Exception {
        execute("""
                CREATE TABLE t AS (
                    SELECT (x % 50)::INT k, (x % 50)::LONG lk, x v, timestamp_sequence('2024-01-01', 60_000_000) ts
                    FROM long_sequence(400)
                ) TIMESTAMP(ts) PARTITION BY HOUR
                """);
        execute("""
                CREATE TABLE d AS (
                    SELECT (x % 40)::INT k, (x % 40)::LONG lk, x w, ('s' || (x % 7))::SYMBOL s
                    FROM long_sequence(300)
                )
                """);
    }

    private static boolean isOnStack(String className, String methodName) {
        return STACK_WALKER.walk(frames -> frames.anyMatch(frame -> frame.getClassName().equals(className) && frame.getMethodName().equals(methodName)));
    }

    // The rows of the query's ordinary plan.
    private static long ordinaryRowCount(String sql) throws SqlException {
        sqlExecutionContext.setParallelHashJoinEnabled(false);
        try (RecordCursorFactory factory = select(sql); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Assert.assertNull(find(factory));
            long count = 0;
            while (cursor.hasNext()) {
                count++;
            }
            return count;
        } finally {
            sqlExecutionContext.setParallelHashJoinEnabled(true);
        }
    }

    // The error that reading the query's rows ends with, under the operator or under the ordinary plan.
    private static String error(String sql, boolean isParallel) throws SqlException {
        sqlExecutionContext.setParallelHashJoinEnabled(isParallel);
        try (RecordCursorFactory factory = select(sql)) {
            Assert.assertEquals(sql, isParallel, find(factory) != null);
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                //noinspection StatementWithEmptyBody
                while (cursor.hasNext()) {
                }
                Assert.fail("expected the filter to fail: " + sql);
            } catch (CairoException | ImplicitCastException e) {
                return e.getPosition() + ": " + e.getFlyweightMessage();
            }
        } finally {
            sqlExecutionContext.setParallelHashJoinEnabled(true);
        }
        return null;
    }

    // True when the method was on the stack where the exception was thrown.
    private static boolean isThrownIn(Throwable e, String className, String methodName) {
        for (StackTraceElement frame : e.getStackTrace()) {
            if (frame.getClassName().equals(className) && frame.getMethodName().equals(methodName)) {
                return true;
            }
        }
        return false;
    }

    // The phase of a parallel hash join's execution that threw the exception, by its stack.
    private static String phase(Throwable e) {
        String phase = "other";
        for (StackTraceElement frame : e.getStackTrace()) {
            final String method = frame.getMethodName();
            if (frame.getClassName().equals(ATOM) && method.equals("continueFrame")) {
                return "continuation";
            }
            if (frame.getClassName().equals(STAGE)) {
                switch (method) {
                    case "bindSymbolKeyTranslation" -> phase = "translator";
                    case "maybeCopyPayload" -> phase = "copy";
                    case "build" -> phase = phase.equals("other") ? "build" : phase;
                    default -> {
                    }
                }
            }
            if (frame.getClassName().endsWith("AsyncHashJoinRecordCursor") && method.equals("fetchNextFrame")) {
                return "probe";
            }
        }
        return phase;
    }

    private void assertMemoryLimitInEachPhase(boolean isInRounds) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 1);
        assertMemoryLeak(() -> {
            createLimitTables();
            sqlExecutionContext.changePageFrameSizes(64, 64);
            // A translated SYMBOL key, a residual that makes the operator walk every pair on the
            // workers, and a payload of fixed-size columns, which the build copies.
            final String sql = "SELECT t.v, t.s, d.w, d.k FROM t LEFT JOIN d ON t.s = d.s AND d.w > t.v - 1_000_000";
            final String expected = ordinary(sql, sqlExecutionContext);
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                TestUtils.assertContains(plan(factory, sqlExecutionContext), "symbolKeyJoin: true");
                final HashJoinBuildStage stage = join.getAtom().getStage(0);
                // The first power of two that fits sizes the sweep's steps.
                long fitting = 1024;
                while (!isFitting(factory, join, fitting, expected)) {
                    fitting *= 2;
                }
                final long step = Math.max(64, fitting / 2_000);
                int build = 0;
                int translator = 0;
                int copy = 0;
                int probe = 0;
                int continuation = 0;
                for (long limit = step; ; limit += step) {
                    setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, limit);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        //noinspection StatementWithEmptyBody
                        while (cursor.hasNext()) {
                        }
                        Assert.assertTrue(stage.isPayloadCopied());
                        Assert.assertEquals(isInRounds, stage.isBuiltInRounds());
                        Assert.assertTrue(join.getAtom().getContinuationCount() > 0);
                        break;
                    } catch (CairoException e) {
                        Assert.assertTrue(e.getMessage(), e.isOutOfMemory());
                        TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                        switch (phase(e)) {
                            case "build" -> build++;
                            case "translator" -> translator++;
                            case "copy" -> copy++;
                            case "probe" -> probe++;
                            case "continuation" -> continuation++;
                            default -> throw new AssertionError("a breach outside the join's phases", e);
                        }
                    }
                    // Each failure released everything the execution held.
                    Assert.assertNull(stage.getFrozenBuild());
                    Assert.assertEquals(0, join.getAtom().getRing().getAllocatedBytes());
                    Assert.assertEquals(0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                    Assert.assertEquals(0, join.getBuildRoundAtom().getPerWorkerLocks().getAcquiredSlotCount());
                    Assert.assertEquals(0, engine.getBusyReaderCount());
                }
                final String counts = "build " + build + ", translator " + translator + ", copy " + copy + ", probe " + probe + ", continuation " + continuation;
                Assert.assertTrue(counts, build > 0);
                Assert.assertTrue(counts, translator > 0);
                Assert.assertTrue(counts, copy > 0);
                Assert.assertTrue(counts, probe > 0);
                Assert.assertTrue(counts, continuation > 0);
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0);
                Assert.assertEquals(expected, result(factory, sqlExecutionContext));
            }
            assertQuery("SELECT count() FROM d").noLeakCheck().noRandomAccess().expectSize().returns("count\n2000\n");
        });
    }

    private boolean isFitting(RecordCursorFactory factory, AsyncHashJoinRecordCursorFactory join, long limit, String expected) throws SqlException {
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, limit);
        try {
            Assert.assertEquals(expected, result(factory, sqlExecutionContext));
            return true;
        } catch (CairoException e) {
            Assert.assertTrue(e.getMessage(), e.isOutOfMemory());
            Assert.assertEquals(0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
            return false;
        } finally {
            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0);
        }
    }

    /**
     * Fails, while armed, the opening, or the mapping, of the files of one table whose names end
     * with the target suffix; a mapping fails for files that it opened while armed.
     */
    private static class TableFilesFacade extends TestFilesFacadeImpl {
        private final AtomicInteger failures = new AtomicInteger();
        private final Set<Long> targetFds = ConcurrentHashMap.newKeySet();
        private volatile boolean isArmed;
        private volatile boolean isLength;
        private volatile boolean isMmap;
        private volatile String suffix;
        private volatile String table;

        @Override
        public long length(long fd) {
            if (isArmed && isLength && targetFds.contains(fd)) {
                failures.incrementAndGet();
                return -1;
            }
            return super.length(fd);
        }

        @Override
        public long length(LPSZ name) {
            if (isArmed && isLength && Utf8s.containsAscii(name, table) && Utf8s.endsWithAscii(name, suffix)) {
                failures.incrementAndGet();
                return -1;
            }
            return super.length(name);
        }

        @Override
        public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
            if (isArmed && isMmap && targetFds.contains(fd)) {
                failures.incrementAndGet();
                return FilesFacade.MAP_FAILED;
            }
            return super.mmap(fd, len, offset, flags, memoryTag);
        }

        @Override
        public long openRO(LPSZ name) {
            final boolean isTarget = isArmed && Utf8s.containsAscii(name, table) && Utf8s.endsWithAscii(name, suffix);
            if (isTarget && !isMmap && !isLength) {
                failures.incrementAndGet();
                return -1;
            }
            final long fd = super.openRO(name);
            if (isTarget && fd > -1) {
                targetFds.add(fd);
            }
            return fd;
        }

        void arm() {
            failures.set(0);
            targetFds.clear();
            isArmed = true;
        }

        void disarm() {
            isArmed = false;
            targetFds.clear();
        }

        int getFailures() {
            return failures.get();
        }

        void target(String table, String suffix, boolean isMmap) {
            this.table = table;
            this.suffix = suffix;
            this.isMmap = isMmap;
            this.isLength = false;
        }

        // Fails the length of the table's files whose names end with the suffix, by name or by the
        // descriptor that opened them.
        void targetLength(String table, String suffix) {
            this.table = table;
            this.suffix = suffix;
            this.isMmap = false;
            this.isLength = true;
        }
    }

    /**
     * Counts the breaker checks that threads make inside one method, on any thread, and cancels
     * the query at the given one. A breaker of a class of its own is thread safe, so the workers
     * consult it directly rather than through a copy.
     */
    private static class MethodCheckCircuitBreaker extends AtomicBooleanCircuitBreaker {
        private final AtomicInteger checks = new AtomicInteger();
        private final String className;
        private final String methodName;
        private final int reachedCheck;

        private MethodCheckCircuitBreaker(CairoEngine engine, String className, String methodName, int reachedCheck) {
            super(engine);
            this.className = className;
            this.methodName = methodName;
            this.reachedCheck = reachedCheck;
        }

        @Override
        public boolean checkIfTripped() {
            onCheck();
            return super.checkIfTripped();
        }

        public int getChecks() {
            return checks.get();
        }

        @Override
        public int getState() {
            onCheck();
            return super.getState();
        }

        @Override
        public void statefulThrowExceptionIfTrippedNoThrottle() {
            onCheck();
            super.statefulThrowExceptionIfTrippedNoThrottle();
        }

        private void onCheck() {
            if (isOnStack(className, methodName) && checks.incrementAndGet() == reachedCheck) {
                cancel();
            }
        }
    }
}
