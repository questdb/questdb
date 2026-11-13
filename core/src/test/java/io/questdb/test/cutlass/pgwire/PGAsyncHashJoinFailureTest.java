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

package io.questdb.test.cutlass.pgwire;

import io.questdb.Metrics;
import io.questdb.PropertyKey;
import io.questdb.cairo.sql.Record;
import io.questdb.cutlass.pgwire.DefaultPGCircuitBreakerRegistry;
import io.questdb.cutlass.pgwire.PGServer;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.test.TestLatchedCounterFunctionFactory;
import io.questdb.griffin.engine.table.AsyncHashJoinAtom;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.HashJoinBuildStage;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.mp.continuation.Fiber;
import io.questdb.std.Os;
import io.questdb.std.str.StringSink;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.postgresql.PGConnection;
import org.postgresql.util.PSQLException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

/**
 * Drives the parallel hash join's failure and owner paths through PG wire with the real pgjdbc
 * driver: a CancelRequest while a latched function holds the execution in each of its phases, on a
 * thread owner with a LEGACY query pool and on a fiber owner with a FIBER_HOST query pool;
 * statement timeouts in the build and the probe; an owner mounted on a fiber of the network pool
 * through every phase, with a FIBER_HOST, a LEGACY or no query pool; clients that drop their
 * connection while a portal holds the join's cursor mid-stream; and concurrent connections that run
 * one cached stacked statement with their own bind values. Every result equals the rows that a
 * server whose connections run the ordinary plan returns, in order.
 */
public class PGAsyncHashJoinFailureTest extends BasePGTest {
    private static final String ATOM = AsyncHashJoinAtom.class.getName();
    // [w2 lower bound, v + w upper bound]: the bounds of the later stage's ON residual and of the
    // first join's post-join filter, the last of which selects no row.
    private static final long[][] BINDS = {{5, 200}, {0, 100_000}, {30, 500}, {10, 50}, {0, 0}};
    // A stack whose first join swaps: d, the smaller input, is its left input, so each execution
    // hashes d and streams f, and the later stage reads d's key l through the first stage's
    // payload. Bind variables sit in the later stage's ON residual and in the first join's post-join
    // filter.
    private static final String BOUND_STACK = "SELECT d.w, f.v, d2.w2 FROM d JOIN f ON d.k = f.k LEFT JOIN d2 ON d.l = d2.l AND d2.w2 > ? "
            + "WHERE f.v + d.w < ?";
    private static final String FACTORY = AsyncHashJoinRecordCursorFactory.class.getName();
    // The owner's rounds, the probe tasks, continuations, LIMITs and count-only counts, on a fiber.
    private static final String[] FIBER_QUERIES = {
            "SELECT f.ts, f.k, f.v, d1.w FROM f LEFT JOIN d1 ON f.k = d1.k",
            "SELECT f.k, f.v, d1.w, d2.w2, d3.s3 FROM f LEFT JOIN d1 ON f.k = d1.k JOIN d2 ON f.l = d2.l LEFT JOIN d3 ON f.sym = d3.sym",
            "SELECT d1.w, f.v, f.k FROM d1 JOIN f ON d1.k = f.k",
            "SELECT d3.s3, f.v, d2.w2 FROM d3 JOIN f ON d3.sym = f.sym LEFT JOIN d2 ON d3.l = d2.l",
            "SELECT f.k, f.v, d1.w, d2.w2 FROM f LEFT JOIN d1 ON f.k = d1.k JOIN d2 ON f.l = d2.l LIMIT 7",
            "SELECT f.k, f.v, d1.w, d2.w2 FROM f LEFT JOIN d1 ON f.k = d1.k JOIN d2 ON f.l = d2.l LIMIT -9",
            "SELECT count(*) FROM (SELECT f.k, d1.w, d2.w2 FROM f LEFT JOIN d1 ON f.k = d1.k JOIN d2 ON f.l = d2.l)",
            "SELECT f.k, f.v, d1.w FROM f LEFT JOIN d1 ON f.k = d1.k AND d1.w > f.v % 50",
            "SELECT f.k, f.v, d1.w, d3.s3 FROM f LEFT JOIN d1 ON f.k = d1.k LEFT JOIN d3 ON f.sym = d3.sym WHERE d1.w > f.v % 7 OR d1.w IS NULL ORDER BY f.ts DESC",
            // d1's filter runs in the build's rounds, which record whether the owner ran on its fiber.
            "SELECT f.k, f.v, d1.w FROM f LEFT JOIN (SELECT * FROM d1 WHERE test_latched_counter()) d1 ON f.k = d1.k",
            // The residual runs in the continuation chunks, which record whether the owner ran on its fiber.
            "SELECT f.k, f.v, d1.w FROM f LEFT JOIN d1 ON f.k = d1.k AND (test_latched_counter() OR d1.w = f.v)",
    };
    private static final String STAGE = HashJoinBuildStage.class.getName();
    private static final StackWalker STACK_WALKER = StackWalker.getInstance();
    private static final String STAR = "SELECT f.k, f.v, d1.w, d2.w2 FROM f LEFT JOIN d1 ON f.k = d1.k JOIN d2 ON f.l = d2.l";
    private static final int WORKER_COUNT = 4;
    private static volatile TestLatchedCounterFunctionFactory.Callback current;
    // test_latched_counter() takes the callback when it compiles, and the server keeps the compiled
    // factories, so the callback that the factories take delegates to the one a test installs.
    private static final TestLatchedCounterFunctionFactory.Callback DELEGATE = new TestLatchedCounterFunctionFactory.Callback() {
        @Override
        public boolean onGet(Record rec, int count) {
            final TestLatchedCounterFunctionFactory.Callback callback = current;
            return callback == null || callback.onGet(rec, count);
        }
    };
    // Where each cancellation lands: the SQL, the count at which the latch may hold, the frame that
    // must be on the latched thread's stack, and the plan attribute that shows where the function runs.
    private static final Site[] SITES = {
            // A small build's filter, which the owner runs alone.
            new Site("build on the owner",
                    "SELECT f.k, f.v, d0.w0 FROM f LEFT JOIN (SELECT * FROM d0 WHERE test_latched_counter()) d0 ON f.k = d0.k",
                    100, () -> isOnStack(STAGE, "buildOnOwner"), "buildFilter: test_latched_counter()", false),
            // A large build's filter, which runs in the first round of the build's rounds.
            new Site("build in rounds",
                    "SELECT f.k, f.v, d1.w FROM f LEFT JOIN (SELECT * FROM d1 WHERE test_latched_counter()) d1 ON f.k = d1.k",
                    300, () -> isOnStack(STAGE, "partitionFrame"), "buildFilter: test_latched_counter()", false),
            // A later stage's filter, inside one task of the round of the two small builds.
            new Site("concurrent small build",
                    "SELECT f.k, f.v, d0.w0, d2.w2 FROM f LEFT JOIN d0 ON f.k = d0.k JOIN (SELECT * FROM d2 WHERE test_latched_counter()) d2 ON f.l = d2.l",
                    50, () -> isOnStack(STAGE, "buildStage"), "buildFilter: test_latched_counter()", false),
            // The probe filter, which the operator took from the probe's scan.
            new Site("probe filter",
                    "SELECT f.k, f.v, d1.w, d2.w2 FROM (SELECT * FROM f WHERE test_latched_counter()) f LEFT JOIN d1 ON f.k = d1.k JOIN d2 ON f.l = d2.l",
                    700, () -> isOnStack(FACTORY, "probeFrame"), "filter: test_latched_counter()", false),
            // An ON residual, which a probe task runs for each pair: the function's conjunct reads both
            // inputs, so that no part of it runs in the build.
            new Site("ON residual",
                    "SELECT f.k, f.v, d1.w FROM f LEFT JOIN d1 ON f.k = d1.k AND d1.w + f.v > 0 AND (test_latched_counter() OR d1.w = f.v)",
                    700, () -> isOnStack(FACTORY, "probeFrame") && !isOnStack(ATOM, "continueFrame"), "onFilter:", false),
            // A post-join filter, which a probe task runs for each pair.
            new Site("post-join filter",
                    "SELECT f.k, f.v, d1.w FROM f JOIN d1 ON f.k = d1.k WHERE d1.w + f.v > 0 AND (test_latched_counter() OR d1.w = f.v)",
                    700, () -> isOnStack(FACTORY, "probeFrame") && !isOnStack(ATOM, "continueFrame"), "postJoinFilter:", false),
            // A continuation chunk on the owner: with a cap of one tuple, the rest of every frame with
            // more pairs than one goes to continuations.
            new Site("continuation",
                    "SELECT f.k, f.v, d1.w FROM f LEFT JOIN d1 ON f.k = d1.k AND d1.w > f.v % 50 AND (test_latched_counter() OR d1.w = f.v)",
                    50, () -> isOnStack(ATOM, "continueFrame"), "onFilter:", true),
            // The consumer's expansion of the chains that a hot key leaves to it: the first 20 rows of f
            // meet 300 rows of hot and 3 or 4 of d0 each, and the projection's function runs for each.
            new Site("consumer chain expansion",
                    "SELECT f.v, d0.w0, hot.hw, test_latched_counter() c FROM f LEFT JOIN d0 ON f.k = d0.k JOIN hot ON f.h = hot.h",
                    50, () -> !isOnStack(FACTORY, "probeFrame") && !isOnStack(ATOM, "continueFrame"), "stages: 2", false),
    };

    @Override
    @Before
    public void setUp() {
        // Frames of 16 to 64 rows, so that a latch leaves frames, and breaker checks, after it, and a
        // capped task leaves the rest of a frame of duplicate matches to a continuation; builds of
        // 500 rows or more build in rounds of partitions of 64 rows.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 16);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 64);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 500);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 64);
        super.setUp();
    }

    @After
    public void tearDownLatch() {
        current = null;
        TestLatchedCounterFunctionFactory.reset(null);
        AsyncHashJoinRecordCursorFactory.setStepperWarmUpRowsForTesting(AsyncHashJoinAtom.STEPPER_WARM_UP_ROWS);
    }

    @Test
    public void testCancelRequestInEachPhaseOnFiberOwner() throws Exception {
        assertCancelledInEachPhase(true, WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testCancelRequestInEachPhaseOnThreadOwner() throws Exception {
        assertCancelledInEachPhase(false, WorkerPoolMode.LEGACY);
    }

    // Each connection runs the same SQL with its own sequence of bind values, so the server's select
    // cache hands the cached factory from one connection to the next, with its warm-up count of probe
    // rows and its per-execution orientation, while the query pool's workers run the frames of several
    // executions at once. The low warm-up threshold makes the stages switch to their hidden stepper
    // copies early in the run.
    @Test
    public void testConcurrentConnectionsRunTheSameCachedStack() throws Exception {
        assertMemoryLeak(() -> {
            createBindTables();
            final String[] expected = new String[BINDS.length];
            withServer(false, false, WorkerPoolMode.LEGACY, port -> {
                try (
                        Connection connection = getConnection(Mode.EXTENDED, port, true, 1);
                        PreparedStatement statement = connection.prepareStatement(BOUND_STACK)
                ) {
                    for (int i = 0; i < BINDS.length; i++) {
                        bind(statement, BINDS[i]);
                        expected[i] = read(statement);
                    }
                }
            });
            // The bounds select different rows, and the last one none.
            Assert.assertTrue(expected[0].length() > 0 && expected[1].length() > expected[0].length());
            TestUtils.assertEquals("w[BIGINT],v[BIGINT],w2[BIGINT]\n", expected[4]);
            AsyncHashJoinRecordCursorFactory.setStepperWarmUpRowsForTesting(2_000);
            withServer(true, false, WorkerPoolMode.LEGACY, port -> {
                final int connectionCount = 4;
                final int iterationCount = 15;
                final CyclicBarrier barrier = new CyclicBarrier(connectionCount);
                final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
                final Thread[] threads = new Thread[connectionCount];
                for (int t = 0; t < connectionCount; t++) {
                    final int threadIndex = t;
                    threads[t] = new Thread(() -> {
                        try (
                                Connection connection = getConnection(Mode.EXTENDED, port, threadIndex % 2 == 0, 1);
                                PreparedStatement statement = connection.prepareStatement(BOUND_STACK)
                        ) {
                            barrier.await(30, TimeUnit.SECONDS);
                            for (int i = 0; i < iterationCount; i++) {
                                final int b = (threadIndex + i) % BINDS.length;
                                bind(statement, BINDS[b]);
                                TestUtils.assertEquals(Arrays.toString(BINDS[b]), expected[b], read(statement));
                            }
                        } catch (Throwable th) {
                            errors.add(th);
                            barrier.reset();
                        }
                    }, "pg-client-" + t);
                    threads[t].start();
                }
                for (Thread thread : threads) {
                    thread.join();
                }
                for (Throwable error : errors) {
                    throw new AssertionError(error);
                }
                try (Connection connection = getConnection(Mode.EXTENDED, port, true, 1)) {
                    final String plan = explain(connection, BOUND_STACK, BINDS[0]);
                    Assert.assertTrue(plan, plan.contains("Hash Join workers:"));
                    Assert.assertTrue(plan, plan.contains("stages: 2"));
                    Assert.assertTrue(plan, plan.contains("inputSwap:"));
                }
            });
        });
    }

    // Four connections run one cached stack whose first join swaps while its left input is the
    // smaller one, while another connection inserts rows into that input, past the size of the right
    // one, so that the cached factory's orientation flips between executions. The inserted rows match
    // no key, so every execution returns the same rows, in the order of the orientation that its
    // snapshot gives: the ordinary plan's order before the insert, or after it.
    @Test
    public void testSwapFlipsWhileConnectionsShareTheCachedStack() throws Exception {
        assertMemoryLeak(() -> {
            final String sql = "SELECT l.v, r.w, e.ew FROM l JOIN r ON l.k = r.k LEFT JOIN e ON l.k = e.k";
            final String createL = "CREATE TABLE l AS (SELECT (x % 30)::INT k, x v FROM long_sequence(100))";
            final String insertL = "INSERT INTO l SELECT (-1 - x)::INT, -x FROM long_sequence(100)";
            execute(createL);
            execute("CREATE TABLE r AS (SELECT (x % 40)::INT k, x w FROM long_sequence(150))");
            execute("CREATE TABLE e AS (SELECT (x % 20)::INT k, x ew FROM long_sequence(25))");
            // The ordinary plan's rows: l, the smaller input, hashed, then, after the insert, r.
            final String[] expected = new String[2];
            withServer(false, false, WorkerPoolMode.LEGACY, port -> {
                try (
                        Connection connection = getConnection(Mode.EXTENDED, port, true, 1);
                        PreparedStatement statement = connection.prepareStatement(sql)
                ) {
                    expected[0] = read(statement);
                    execute(insertL);
                    expected[1] = read(statement);
                }
            });
            Assert.assertNotEquals(expected[0], expected[1]);
            Assert.assertEquals(sortedLines(expected[0]), sortedLines(expected[1]));
            Assert.assertTrue(expected[0].length() > 1_000);
            execute("DROP TABLE l");
            execute(createL);
            withServer(true, false, WorkerPoolMode.LEGACY, port -> {
                final int connectionCount = 4;
                final int iterationsBefore = 3;
                final int iterationsAfter = 3;
                final CyclicBarrier barrier = new CyclicBarrier(connectionCount + 1);
                final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
                final int[] afterCounts = new int[connectionCount];
                final Thread[] threads = new Thread[connectionCount];
                for (int t = 0; t < connectionCount; t++) {
                    final int threadIndex = t;
                    threads[t] = new Thread(() -> {
                        try (
                                Connection connection = getConnection(Mode.EXTENDED, port, threadIndex % 2 == 0, 1);
                                PreparedStatement statement = connection.prepareStatement(sql)
                        ) {
                            // Before the insert, every execution hashes l.
                            for (int i = 0; i < iterationsBefore; i++) {
                                TestUtils.assertEquals(expected[0], read(statement));
                            }
                            barrier.await(60, TimeUnit.SECONDS);
                            // While the insert runs, an execution's snapshot holds l before or after it,
                            // and once a connection read the rows after it, it never reads the rows
                            // before it again.
                            boolean isAfter = false;
                            for (int i = 0; i < iterationsAfter || !isAfter; i++) {
                                Assert.assertTrue("the rows after the insert never came", i < 10_000);
                                final String rows = read(statement);
                                if (rows.equals(expected[1])) {
                                    isAfter = true;
                                    afterCounts[threadIndex]++;
                                } else {
                                    Assert.assertFalse("read the rows before the insert after the rows after it", isAfter);
                                    TestUtils.assertEquals(expected[0], rows);
                                }
                            }
                        } catch (Throwable th) {
                            errors.add(th);
                            barrier.reset();
                        }
                    }, "pg-client-" + t);
                    threads[t].start();
                }
                try (Connection connection = getConnection(Mode.EXTENDED, port, true, 1)) {
                    barrier.await(60, TimeUnit.SECONDS);
                    try (Statement statement = connection.createStatement()) {
                        statement.execute(insertL);
                    }
                } finally {
                    for (Thread thread : threads) {
                        thread.join(TimeUnit.SECONDS.toMillis(120));
                    }
                }
                for (Throwable error : errors) {
                    throw new AssertionError(error);
                }
                for (int t = 0; t < connectionCount; t++) {
                    Assert.assertTrue(afterCounts[t] > 0);
                }
                try (Connection connection = getConnection(Mode.EXTENDED, port, true, 1)) {
                    final String plan = explain(connection, sql, null);
                    Assert.assertTrue(plan, plan.contains("Hash Join workers:"));
                    Assert.assertTrue(plan, plan.contains("inputSwap:"));
                }
            });
        });
    }

    @Test
    public void testDisconnectMidStreamOnFiberOwner() throws Exception {
        assertDisconnectMidStream(true, WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testDisconnectMidStreamOnThreadOwner() throws Exception {
        assertDisconnectMidStream(false, WorkerPoolMode.LEGACY);
    }

    @Test
    public void testFiberOwnerWithFiberQueryPool() throws Exception {
        assertFiberOwner(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testFiberOwnerWithLegacyQueryPool() throws Exception {
        assertFiberOwner(WorkerPoolMode.LEGACY);
    }

    @Test
    public void testFiberOwnerWithoutQueryPool() throws Exception {
        assertFiberOwner(null);
    }

    @Test
    public void testStatementTimeoutDuringBuild() throws Exception {
        assertTimedOut(SITES[1]);
    }

    @Test
    public void testStatementTimeoutDuringProbe() throws Exception {
        assertTimedOut(SITES[3]);
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(30, TimeUnit.SECONDS)) {
                throw new AssertionError("latch timed out");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private static void bind(PreparedStatement statement, long[] values) throws SQLException {
        statement.setLong(1, values[0]);
        statement.setLong(2, values[1]);
    }

    private static String explain(Connection connection, String sql, @Nullable long[] values) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("EXPLAIN " + sql)) {
            if (values != null) {
                bind(statement, values);
            }
            try (ResultSet resultSet = statement.executeQuery()) {
                final StringBuilder plan = new StringBuilder();
                while (resultSet.next()) {
                    plan.append(resultSet.getString(1)).append('\n');
                }
                return plan.toString();
            }
        }
    }

    private static boolean isOnStack(String className, String methodName) {
        return STACK_WALKER.walk(frames -> frames.anyMatch(frame -> frame.getClassName().equals(className) && frame.getMethodName().equals(methodName)));
    }

    private static String sortedLines(String text) {
        final String[] lines = text.split("\n");
        Arrays.sort(lines);
        return String.join("\n", lines);
    }

    private static String read(PreparedStatement statement) throws SQLException {
        final StringSink rows = new StringSink();
        try (ResultSet resultSet = statement.executeQuery()) {
            printToSink(rows, resultSet, null);
        }
        return rows.toString();
    }

    private void assertCancelledInEachPhase(boolean isFiber, WorkerPoolMode queryPoolMode) throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String[] expected = new String[SITES.length];
            withServer(false, isFiber, queryPoolMode, port -> {
                try (Connection connection = getConnection(Mode.EXTENDED, port, true, 1)) {
                    for (int i = 0; i < SITES.length; i++) {
                        try (PreparedStatement statement = connection.prepareStatement(SITES[i].sql)) {
                            expected[i] = read(statement);
                        }
                        Assert.assertFalse(SITES[i].name, explain(connection, SITES[i].sql, null).contains("Hash Join workers:"));
                    }
                }
            });
            TestLatchedCounterFunctionFactory.reset(DELEGATE);
            withServer(true, isFiber, queryPoolMode, port -> {
                for (int i = 0; i < SITES.length; i++) {
                    final Site site = SITES[i];
                    if (site.isCapped) {
                        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1);
                    }
                    try (
                            Connection connection = getConnection(Mode.EXTENDED, port, true, 1);
                            PreparedStatement statement = connection.prepareStatement(site.sql)
                    ) {
                        final String plan = explain(connection, site.sql, null);
                        Assert.assertTrue(site.name + '\n' + plan, plan.contains("Hash Join workers:"));
                        Assert.assertTrue(site.name + '\n' + plan, plan.contains(site.planAttribute));
                        final AtomicBoolean isLatched = new AtomicBoolean();
                        final CountDownLatch reached = new CountDownLatch(1);
                        final CountDownLatch cancelSent = new CountDownLatch(1);
                        TestLatchedCounterFunctionFactory.reset(DELEGATE);
                        // Holds the first thread that reaches the site, once, until the cancellation
                        // went out, so the execution after it runs freely.
                        current = new TestLatchedCounterFunctionFactory.Callback() {
                            @Override
                            public boolean onGet(Record rec, int count) {
                                if (count >= site.latchCount && !isLatched.get() && site.isAtSite.getAsBoolean() && isLatched.compareAndSet(false, true)) {
                                    reached.countDown();
                                    await(cancelSent);
                                }
                                return true;
                            }
                        };
                        final AtomicBoolean isCancelSent = new AtomicBoolean();
                        final Thread canceller = new Thread(() -> {
                            try {
                                if (reached.await(30, TimeUnit.SECONDS)) {
                                    ((PGConnection) connection).cancelQuery();
                                    isCancelSent.set(true);
                                }
                            } catch (Throwable th) {
                                LOG.error().$("cancellation failed [error=").$(th).I$();
                            } finally {
                                cancelSent.countDown();
                            }
                        }, "pg-canceller");
                        canceller.start();
                        try {
                            read(statement);
                            Assert.fail("expected the cancellation of " + site.name);
                        } catch (PSQLException e) {
                            TestUtils.assertContains(e.getMessage(), "cancelled by user");
                        } finally {
                            canceller.join();
                        }
                        Assert.assertTrue(site.name, isCancelSent.get());
                        current = null;
                        // The connection and the statement stay usable, and the next execution
                        // returns the ordinary plan's rows.
                        TestUtils.assertEquals(site.name, expected[i], read(statement));
                    } finally {
                        if (site.isCapped) {
                            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, (String) null);
                        }
                    }
                    TestUtils.assertEventually(() -> Assert.assertEquals(site.name, 0, engine.getBusyReaderCount()));
                }
            });
        });
    }

    private void assertDisconnectMidStream(boolean isFiber, WorkerPoolMode queryPoolMode) throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String[] queries = {
                    STAR,
                    // A swapped stack: d1 is smaller than f.
                    "SELECT d1.w, f.v, d2.w2 FROM d1 JOIN f ON d1.k = f.k LEFT JOIN d2 ON f.l = d2.l",
                    // Continuations: a hot key's chains, which the consumer expands.
                    "SELECT f.v, d0.w0, hot.hw FROM f LEFT JOIN d0 ON f.k = d0.k JOIN hot ON f.h = hot.h",
            };
            final String[] expected = new String[queries.length];
            withServer(false, isFiber, queryPoolMode, port -> {
                try (Connection connection = getConnection(Mode.EXTENDED, port, true, 1)) {
                    for (int i = 0; i < queries.length; i++) {
                        try (PreparedStatement statement = connection.prepareStatement(queries[i])) {
                            expected[i] = read(statement);
                        }
                    }
                }
            });
            withServer(true, isFiber, queryPoolMode, port -> {
                for (int i = 0; i < queries.length; i++) {
                    for (int run = 0; run < 3; run++) {
                        final Connection connection = getConnection(Mode.EXTENDED, port, true, 1);
                        connection.setAutoCommit(false);
                        final PreparedStatement statement = connection.prepareStatement(queries[i]);
                        statement.setFetchSize(7);
                        final ResultSet resultSet = statement.executeQuery();
                        for (int r = 0; r < 20 + run * 9 && resultSet.next(); r++) {
                            resultSet.getLong(1);
                        }
                        // Drops the connection while the server's portal holds the cursor mid-stream.
                        connection.abort(Runnable::run);
                    }
                    // The server closes the dropped connections' cursors, which release their readers.
                    TestUtils.assertEventually(() -> Assert.assertEquals(0, engine.getBusyReaderCount()));
                    try (
                            Connection connection = getConnection(Mode.EXTENDED, port, true, 1);
                            PreparedStatement statement = connection.prepareStatement(queries[i])
                    ) {
                        Assert.assertTrue(explain(connection, queries[i], null).contains("Hash Join workers:"));
                        TestUtils.assertEquals(queries[i], expected[i], read(statement));
                    }
                }
            });
        });
    }

    private void assertFiberOwner(@Nullable WorkerPoolMode queryPoolMode) throws Exception {
        // Every frame with more pairs than one goes to continuations, builds of 200 rows or more
        // build in rounds, and the smaller ones of a stack build at once.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 200);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 4);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE f AS (SELECT (x % 97)::INT k, x v, (x % 41)::LONG l, ('s' || (x % 13))::SYMBOL sym, timestamp_sequence('2024-01-01', 60_000_000) ts FROM long_sequence(5_000)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE d1 AS (SELECT (x % 90)::INT k, x w FROM long_sequence(400))");
            execute("CREATE TABLE d2 AS (SELECT (x % 41)::LONG l, x w2, (x % 97)::INT k FROM long_sequence(120))");
            execute("CREATE TABLE d3 AS (SELECT ('s' || (x % 11))::SYMBOL sym, 'v' || x s3, (x % 41)::LONG l FROM long_sequence(30))");
            // The probe reads one Parquet partition.
            execute("ALTER TABLE f CONVERT PARTITION TO PARQUET LIST '2024-01-02'");
            final StringSink expected = new StringSink();
            final StringSink actual = new StringSink();
            withServer(false, true, queryPoolMode, port -> readFiberQueries(port, expected, false));
            final AtomicBoolean isOwnerRoundOnFiber = new AtomicBoolean();
            final AtomicBoolean isOwnerRoundOffFiber = new AtomicBoolean();
            final AtomicBoolean isRound = new AtomicBoolean();
            final AtomicBoolean isContinuationOnFiber = new AtomicBoolean();
            final AtomicBoolean isContinuationOffFiber = new AtomicBoolean();
            TestLatchedCounterFunctionFactory.reset(DELEGATE);
            current = new TestLatchedCounterFunctionFactory.Callback() {
                @Override
                public boolean onGet(Record rec, int count) {
                    if (isOnStack(STAGE, "partitionFrame")) {
                        isRound.set(true);
                    }
                    // The owner runs the continuation chunks on its slot.
                    if (isOnStack(ATOM, "continueFrame")) {
                        if (Fiber.isMounted()) {
                            isContinuationOnFiber.set(true);
                        } else {
                            isContinuationOffFiber.set(true);
                        }
                    }
                    // The owner runs a round's task itself from within buildInRounds().
                    if (isOnStack(STAGE, "buildInRounds")) {
                        if (Fiber.isMounted()) {
                            isOwnerRoundOnFiber.set(true);
                        } else {
                            isOwnerRoundOffFiber.set(true);
                        }
                    }
                    return true;
                }
            };
            withServer(true, true, queryPoolMode, port -> readFiberQueries(port, actual, true));
            Assert.assertTrue(expected.length() > 0);
            TestUtils.assertEquals(expected, actual);
            Assert.assertTrue(isRound.get());
            Assert.assertTrue(isContinuationOnFiber.get());
            Assert.assertFalse(isContinuationOffFiber.get());
            // The owner never runs a round's task off its fiber, and without a pool it runs them all.
            Assert.assertFalse(isOwnerRoundOffFiber.get());
            if (queryPoolMode == null) {
                Assert.assertTrue(isOwnerRoundOnFiber.get());
            }
        });
    }

    private void assertTimedOut(Site site) throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String[] expected = new String[1];
            withServer(false, false, null, port -> {
                try (
                        Connection connection = getConnection(Mode.EXTENDED, port, true, 1);
                        PreparedStatement statement = connection.prepareStatement(site.sql)
                ) {
                    expected[0] = read(statement);
                }
            });
            final AtomicBoolean isLatched = new AtomicBoolean();
            TestLatchedCounterFunctionFactory.reset(DELEGATE);
            // The latch outlasts the statement's timeout once, at the site.
            current = new TestLatchedCounterFunctionFactory.Callback() {
                @Override
                public boolean onGet(Record rec, int count) {
                    if (count >= site.latchCount && !isLatched.get() && site.isAtSite.getAsBoolean() && isLatched.compareAndSet(false, true)) {
                        Os.sleep(1_000);
                    }
                    return true;
                }
            };
            withServer(true, false, null, port -> {
                try (
                        Connection connection = getConnection(port, false, true, 500);
                        PreparedStatement statement = connection.prepareStatement(site.sql)
                ) {
                    try {
                        read(statement);
                        Assert.fail("expected the timeout of " + site.name);
                    } catch (PSQLException e) {
                        TestUtils.assertContains(e.getMessage(), "timeout, query aborted");
                    }
                    Assert.assertTrue(isLatched.get());
                    // The next execution runs within the timeout.
                    TestUtils.assertEquals(site.name, expected[0], read(statement));
                }
                TestUtils.assertEventually(() -> Assert.assertEquals(0, engine.getBusyReaderCount()));
            });
        });
    }

    // f holds 1_000 rows over ten days; d holds 300 rows, six for each of f's keys 0 to 49; d2 holds
    // one row for each of d's keys l but 0 and 10.
    private void createBindTables() throws Exception {
        execute("CREATE TABLE f AS (SELECT (x % 60)::INT k, x v, timestamp_sequence('2024-01-01', 864_000_000) ts FROM long_sequence(1_000)) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE d AS (SELECT (x % 50)::INT k, x w, (x % 11)::LONG l FROM long_sequence(300))");
        execute("CREATE TABLE d2 AS (SELECT x::LONG l, x * 5 w2 FROM long_sequence(9))");
    }

    private void createTables() throws Exception {
        execute("CREATE TABLE f AS (SELECT (x % 97)::INT k, x v, (x % 41)::LONG l, CASE WHEN x <= 20 THEN 0 ELSE x::INT END h, " +
                "timestamp_sequence('2024-01-01', 60_000_000) ts FROM long_sequence(6_000)) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE d0 AS (SELECT (x % 90)::INT k, x w0 FROM long_sequence(300))");
        execute("CREATE TABLE d1 AS (SELECT (x % 90)::INT k, x w FROM long_sequence(900))");
        execute("CREATE TABLE d2 AS (SELECT (x % 41)::LONG l, x w2 FROM long_sequence(120))");
        execute("CREATE TABLE hot AS (SELECT 0 h, x hw FROM long_sequence(300))");
    }

    private void readFiberQueries(int port, StringSink sink, boolean isParallel) throws Exception {
        try (Connection connection = getConnection(Mode.EXTENDED, port, true, 1)) {
            for (String sql : FIBER_QUERIES) {
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    // Three executions of one factory.
                    for (int pass = 0; pass < 3; pass++) {
                        sink.put(sql).put('\n').put(read(statement));
                    }
                }
                final String plan = explain(connection, sql, null);
                Assert.assertEquals(plan, isParallel, plan.contains("Hash Join workers:"));
            }
        }
    }

    /**
     * Runs the code against a PG server whose connections run the parallel hash join or not, on
     * fibers of the network pool when asked, with a query pool of their own when asked; without one
     * the owner runs every task.
     */
    private void withServer(boolean isParallel, boolean isFiber, @Nullable WorkerPoolMode queryPoolMode, ServerCode code) throws Exception {
        final Port0PGConfiguration configuration = new Port0PGConfiguration() {
            @Override
            public int getWorkerCount() {
                return 2;
            }

            @Override
            public boolean isFiberEnabled() {
                return isFiber;
            }
        };
        final TestWorkerPool queryPool = queryPoolMode == null
                ? null
                : new TestWorkerPool("query", WORKER_COUNT, Metrics.DISABLED, queryPoolMode);
        try {
            if (queryPool != null) {
                WorkerPoolUtils.setupQueryJobs(queryPool, engine, true);
                queryPool.start(LOG);
            }
            final TestWorkerPool networkPool = new TestWorkerPool(configuration);
            Assert.assertEquals(isFiber, networkPool.isFiberHost());
            try (
                    PGServer server = createPGWireServer(
                            configuration,
                            engine,
                            networkPool,
                            new DefaultPGCircuitBreakerRegistry(configuration, engine.getConfiguration()),
                            () -> {
                                final SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, WORKER_COUNT);
                                context.setParallelHashJoinEnabled(isParallel);
                                return context;
                            }
                    );
                    WorkerPool ignore = networkPool
            ) {
                Assert.assertNotNull(server);
                networkPool.start(LOG);
                for (int i = 0; i < 60_000 && !server.isListening(); i++) {
                    Os.sleep(1);
                }
                code.run(server.getPort());
            }
        } finally {
            if (queryPool != null) {
                queryPool.halt();
            }
        }
    }

    @FunctionalInterface
    private interface ServerCode {
        void run(int port) throws Exception;
    }

    private record Site(String name, String sql, int latchCount, BooleanSupplier isAtSite, String planAttribute, boolean isCapped) {
    }
}
