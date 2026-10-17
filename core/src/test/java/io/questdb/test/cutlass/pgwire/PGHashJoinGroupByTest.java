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
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.mp.continuation.Fiber;
import io.questdb.std.Os;
import io.questdb.std.str.StringSink;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.postgresql.PGConnection;
import org.postgresql.util.PSQLException;

import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.questdb.test.tools.TestUtils.assertContains;

/**
 * Drives the fused hash join GROUP BY through PG wire with the real pgjdbc driver: named
 * statements whose bind values flip the build choice, the parameter types that Describe reports,
 * concurrent connections over one cached query, CancelRequest and statement timeouts during the
 * build and the probe, and owners that run on a fiber of the network pool.
 */
public class PGHashJoinGroupByTest extends BasePGTest {
    // [from, to, sym]: one day of f selects 100 rows against the 300 rows of d, so these bind
    // values flip which input the INNER join builds, and the last one selects no row of f.
    private static final String[][] BINDS = {
            {"2024-01-01T00:00", "2024-01-02T00:00", "a"},
            {"2024-01-01T00:00", "2024-01-11T00:00", "a"},
            {"2024-01-05T00:00", "2024-01-07T00:00", "b"},
            {"2024-01-02T00:00", "2024-01-06T00:00", "b"},
            {"2025-01-01T00:00", "2025-01-02T00:00", "a"}
    };
    // SQL with d, the table with fewer rows, first in join order, so the INNER join builds the
    // input that the ordinary plan compiles first.
    private static final String BUILD_FIRST_SQL = """
            SELECT d.g, count(*) n, sum(f.v) v, sum(d.w) w FROM d JOIN f ON d.k = f.k
            WHERE f.ts >= ? AND f.ts < ? AND d.sym = ? ORDER BY d.g""";
    private static final String BUILD_LATCHED_SQL = """
            SELECT d.g, count(*) n, sum(f.v) v, sum(d.w) w
            FROM f LEFT JOIN (SELECT * FROM d WHERE test_latched_counter()) d ON f.k = d.k
            ORDER BY d.g""";
    private static final String[] EXPECTED = {
            """
                    g[INTEGER],n[BIGINT],v[BIGINT],w[DOUBLE]
                    0,38,1908,2919.0
                    1,38,1960,2840.0
                    2,39,1956,2888.0
                    3,38,1946,2808.0
                    4,39,1944,2857.0
                    5,38,1932,2776.0
                    6,40,2034,2997.0
                    """,
            """
                    g[INTEGER],n[BIGINT],v[BIGINT],w[DOUBLE]
                    0,353,176418,27174.0
                    1,353,176980,26150.0
                    2,369,184476,27473.0
                    3,353,176846,26433.0
                    4,369,184374,27772.0
                    5,353,176712,26716.0
                    6,370,185274,28242.0
                    """,
            """
                    g[INTEGER],n[BIGINT],v[BIGINT],w[DOUBLE]
                    0,67,33473,4896.5
                    1,71,35373,5271.5
                    2,67,33447,5033.5
                    3,71,35355,5412.5
                    4,67,33421,5170.5
                    5,70,34936,5383.0
                    6,67,33395,5132.5
                    """,
            """
                    g[INTEGER],n[BIGINT],v[BIGINT],w[DOUBLE]
                    0,138,41588,10094.0
                    1,146,43912,10776.0
                    2,138,41534,10267.0
                    3,146,43874,10957.0
                    4,138,41480,10440.0
                    5,145,43735,10967.5
                    6,139,41927,10598.5
                    """,
            """
                    g[INTEGER],n[BIGINT],v[BIGINT],w[DOUBLE]
                    """
    };
    private static final String LEFT_EXPECTED = """
            g[INTEGER],n[BIGINT],v[BIGINT],w[DOUBLE]
            null,160,80720,null
            0,706,352885,53147.5
            1,722,360927,53438.5
            2,722,360809,53729.5
            3,722,360691,54020.5
            4,722,360573,54311.5
            5,723,361456,54773.0
            6,723,361339,55239.5
            """;
    private static final String PROBE_LATCHED_SQL = """
            SELECT d.g, count(*) n, sum(f.v) v, sum(d.w) w
            FROM (SELECT * FROM f WHERE test_latched_counter()) f LEFT JOIN d ON f.k = d.k
            ORDER BY d.g""";
    // EXPECTED, plus the rows of f whose key no row of d with the symbol matches.
    private static final String[] RIGHT_EXPECTED = {
            """
                    g[INTEGER],n[BIGINT],v[BIGINT],w[DOUBLE]
                    null,55,2770,null
                    0,38,1908,2919.0
                    1,38,1960,2840.0
                    2,39,1956,2888.0
                    3,38,1946,2808.0
                    4,39,1944,2857.0
                    5,38,1932,2776.0
                    6,40,2034,2997.0
                    """,
            """
                    g[INTEGER],n[BIGINT],v[BIGINT],w[DOUBLE]
                    null,580,290320,null
                    0,353,176418,27174.0
                    1,353,176980,26150.0
                    2,369,184476,27473.0
                    3,353,176846,26433.0
                    4,369,184374,27772.0
                    5,353,176712,26716.0
                    6,370,185274,28242.0
                    """,
            """
                    g[INTEGER],n[BIGINT],v[BIGINT],w[DOUBLE]
                    null,120,60200,null
                    0,67,33473,4896.5
                    1,71,35373,5271.5
                    2,67,33447,5033.5
                    3,71,35355,5412.5
                    4,67,33421,5170.5
                    5,70,34936,5383.0
                    6,67,33395,5132.5
                    """,
            """
                    g[INTEGER],n[BIGINT],v[BIGINT],w[DOUBLE]
                    null,235,70525,null
                    0,138,41588,10094.0
                    1,146,43912,10776.0
                    2,138,41534,10267.0
                    3,146,43874,10957.0
                    4,138,41480,10440.0
                    5,145,43735,10967.5
                    6,139,41927,10598.5
                    """,
            """
                    g[INTEGER],n[BIGINT],v[BIGINT],w[DOUBLE]
                    """
    };
    // A RIGHT join builds the table before it, here d with its own WHERE clause, and probes f.
    private static final String RIGHT_SQL = """
            SELECT d.g, count(*) n, sum(f.v) v, sum(d.w) w FROM (SELECT * FROM d WHERE sym = ?) d RIGHT JOIN f ON d.k = f.k
            WHERE f.ts >= ? AND f.ts < ? ORDER BY d.g""";
    private static final String SQL = """
            SELECT d.g, count(*) n, sum(f.v) v, sum(d.w) w FROM f JOIN d ON f.k = d.k
            WHERE f.ts >= ? AND f.ts < ? AND d.sym = ? ORDER BY d.g""";
    private static final int WORKER_COUNT = 4;

    @Override
    @Before
    public void setUp() {
        // Frames of up to 50 rows give f 20 frames and d 6, so a latch inside the build or the
        // probe leaves frames, and circuit breaker checks, after it.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 50);
        super.setUp();
    }

    @Test
    public void testCancelRequestDuringBuild() throws Exception {
        assertCancelled(BUILD_LATCHED_SQL, 120, false);
    }

    @Test
    public void testCancelRequestDuringParallelBuild() throws Exception {
        assertCancelled(BUILD_LATCHED_SQL, 120, true);
    }

    @Test
    public void testCancelRequestDuringProbe() throws Exception {
        assertCancelled(PROBE_LATCHED_SQL, 400, false);
    }

    @Test
    public void testCancelRequestDuringProbeAfterParallelBuild() throws Exception {
        assertCancelled(PROBE_LATCHED_SQL, 400, true);
    }

    // Each connection runs the same SQL with its own sequence of bind values, so the server's
    // select cache hands the cached factories from one connection to the next while the query
    // pool's workers run the frames of several executions at once.
    @Test
    public void testConcurrentConnectionsRunTheSameFusedQuery() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            withServer(true, false, WorkerPoolMode.LEGACY, port -> {
                final int connectionCount = 4;
                final int iterationCount = 20;
                final CyclicBarrier barrier = new CyclicBarrier(connectionCount);
                final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
                final Thread[] threads = new Thread[connectionCount];
                for (int t = 0; t < connectionCount; t++) {
                    final int threadIndex = t;
                    threads[t] = new Thread(() -> {
                        final StringSink localSink = new StringSink();
                        try (
                                Connection connection = getConnection(Mode.EXTENDED, port, threadIndex % 2 == 0, 1);
                                PreparedStatement statement = connection.prepareStatement(SQL)
                        ) {
                            barrier.await(30, TimeUnit.SECONDS);
                            for (int i = 0; i < iterationCount; i++) {
                                final int bind = (threadIndex + i) % BINDS.length;
                                bindTyped(statement, BINDS[bind]);
                                try (ResultSet resultSet = statement.executeQuery()) {
                                    localSink.clear();
                                    printToSink(localSink, resultSet, null);
                                    TestUtils.assertEquals(Arrays.toString(BINDS[bind]), EXPECTED[bind], localSink);
                                }
                            }
                        } catch (Throwable th) {
                            errors.add(th);
                            // reset() releases the threads that wait on the barrier, and the timed
                            // await bounds the wait of any thread that reaches it later.
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
                    assertFused(explain(connection, SQL, PGHashJoinGroupByTest::bindTyped, BINDS[0]), true);
                }
            });
        });
    }

    // A dispatcher on the fiber query pool lets the owner's fiber park between the round's tasks,
    // the canPark branches of UnorderedPageFrameSequence.dispatchAndAwait().
    @Test
    public void testFiberOwnerBuildsInRoundsWithFiberQueryPool() throws Exception {
        assertFiberOwnerBuildsInRounds(WorkerPoolMode.FIBER_HOST);
    }

    // The query pool is not a fiber host, so no dispatcher parks the owner: the owner's fiber
    // spins on the round's latch while the pool's threads run the round tasks.
    @Test
    public void testFiberOwnerBuildsInRoundsWithLegacyQueryPool() throws Exception {
        assertFiberOwnerBuildsInRounds(WorkerPoolMode.LEGACY);
    }

    // Without a query pool, the owner's fiber runs every task of every round itself.
    @Test
    public void testFiberOwnerBuildsInRoundsWithoutQueryPool() throws Exception {
        assertFiberOwnerBuildsInRounds(null);
    }

    @Test
    public void testNamedStatementRebindsCastTextTimestamps() throws Exception {
        assertNamedStatement(
                """
                        SELECT d.g, count(*) n, sum(f.v) v, sum(d.w) w FROM f JOIN d ON f.k = d.k
                        WHERE f.ts >= ?::TIMESTAMP AND f.ts < ?::TIMESTAMP AND d.sym = ? ORDER BY d.g""",
                (statement, values) -> {
                    statement.setString(1, values[0] + ":00.000000Z");
                    statement.setString(2, values[1] + ":00.000000Z");
                    statement.setString(3, values[2]);
                },
                EXPECTED,
                true
        );
    }

    // setObject() of a LocalDateTime sends the TIMESTAMP type, so the analysis sees typed bind
    // variables and compiles both orientations of the INNER join.
    @Test
    public void testNamedStatementRebindsTypedTimestamps() throws Exception {
        assertNamedStatement(SQL, PGHashJoinGroupByTest::bindTyped, EXPECTED, true);
    }

    // setTimestamp() leaves the parameter type unspecified, since pgjdbc cannot tell TIMESTAMP
    // from TIMESTAMPTZ. The bind variables sit in the inputs' own WHERE clauses, so the analysis
    // leaves their types to the compile of each input, which infers them as the ordinary plan does.
    @Test
    public void testNamedStatementRebindsUnspecifiedTimestamps() throws Exception {
        assertNamedStatement(SQL, PGHashJoinGroupByTest::bindUnspecified, EXPECTED, true);
    }

    // A RIGHT join always builds the input that comes first in join order, and keeps one orientation.
    @Test
    public void testNamedStatementRebindsUnspecifiedTimestampsInRightJoin() throws Exception {
        assertNamedStatement(RIGHT_SQL, PGHashJoinGroupByTest::bindUnspecifiedRight, RIGHT_EXPECTED, false);
    }

    // The analysis accepts an INNER join that builds d, the input that comes first in join order,
    // and leaves the unspecified types to the compile of each input. pgjdbc gives each ? its own
    // parameter, which sits in one input, so the compile order decides no type here;
    // HashJoinGroupByCandidateTest and HashJoinGroupBySemanticTest pin that order with a bind
    // variable that both inputs read.
    @Test
    public void testNamedStatementRebindsUnspecifiedTimestampsWhenBuildComesFirst() throws Exception {
        assertNamedStatement(BUILD_FIRST_SQL, PGHashJoinGroupByTest::bindUnspecified, EXPECTED, true);
    }

    // Describe compiles the statement with every parameter type unspecified. The fused analysis
    // and the build choice's second compile must leave the types that the ordinary plan infers.
    @Test
    public void testParameterDescriptionMatchesOrdinaryPlan() throws Exception {
        final String[] sqls = {
                SQL,
                BUILD_FIRST_SQL,
                RIGHT_SQL,
                """
                        SELECT d.g, count(*) n FROM f JOIN d ON f.k = d.k
                        WHERE f.ts >= ?::TIMESTAMP AND f.ts < ?::TIMESTAMP AND d.sym = ? ORDER BY d.g""",
                "SELECT d.g, sum(f.v + ?) v, avg(d.w * ?) w FROM f JOIN d ON f.k = d.k WHERE d.g = ? ORDER BY d.g",
                "SELECT d.g, count(*) n FROM f LEFT JOIN d ON f.k = d.k AND d.w > ? WHERE f.v < ? ORDER BY d.g",
                "SELECT d.g, count(*) n FROM f RIGHT JOIN d ON f.k = d.k AND f.v > ? ORDER BY d.g",
                "SELECT d.sym, count(*) n FROM f JOIN d ON f.k = d.k WHERE f.ts IN ? AND d.sym <> ? ORDER BY d.sym"
        };
        assertMemoryLeak(() -> {
            createTables();
            final String[] descriptions = new String[2];
            for (int i = 0; i < 2; i++) {
                final boolean isFused = i == 0;
                final StringBuilder description = new StringBuilder();
                withServer(isFused, false, null, port -> {
                    try (Connection connection = getConnection(Mode.EXTENDED, port, true, 1)) {
                        for (String sql : sqls) {
                            description.append(sql).append('\n').append(describe(connection, sql)).append('\n');
                        }
                        // The described statements compile the plans that the enabled path chose:
                        // SQL, BUILD_FIRST_SQL and RIGHT_SQL with their timestamps unspecified, and
                        // sqls[4] with typed parameters.
                        assertFused(explain(connection, SQL, PGHashJoinGroupByTest::bindUnspecified, BINDS[0]), isFused);
                        assertFused(explain(connection, BUILD_FIRST_SQL, PGHashJoinGroupByTest::bindUnspecified, BINDS[0]), isFused);
                        assertFused(explain(connection, RIGHT_SQL, PGHashJoinGroupByTest::bindUnspecifiedRight, BINDS[0]), isFused);
                        assertFused(explain(connection, sqls[4], (statement, values) -> {
                            statement.setLong(1, 1);
                            statement.setDouble(2, 2.0);
                            statement.setInt(3, 3);
                        }, null), isFused);
                    }
                });
                descriptions[i] = description.toString();
            }
            Assert.assertEquals(descriptions[1], descriptions[0]);
        });
    }

    @Test
    public void testStatementTimeoutDuringBuild() throws Exception {
        assertTimedOut(BUILD_LATCHED_SQL, 120);
    }

    @Test
    public void testStatementTimeoutDuringProbe() throws Exception {
        assertTimedOut(PROBE_LATCHED_SQL, 400);
    }

    private static void assertFused(String plan, boolean isFused) {
        Assert.assertEquals(plan, isFused, plan.contains("Hash Join Group By"));
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

    private static void bindTyped(PreparedStatement statement, String[] values) throws SQLException {
        statement.setObject(1, LocalDateTime.parse(values[0]));
        statement.setObject(2, LocalDateTime.parse(values[1]));
        statement.setString(3, values[2]);
    }

    // setTimestamp() leaves the parameter type unspecified.
    private static void bindUnspecified(PreparedStatement statement, String[] values) throws SQLException {
        statement.setTimestamp(1, Timestamp.from(Instant.parse(values[0] + ":00Z")));
        statement.setTimestamp(2, Timestamp.from(Instant.parse(values[1] + ":00Z")));
        statement.setString(3, values[2]);
    }

    // RIGHT_SQL takes the symbol first.
    private static void bindUnspecifiedRight(PreparedStatement statement, String[] values) throws SQLException {
        statement.setString(1, values[2]);
        statement.setTimestamp(2, Timestamp.from(Instant.parse(values[0] + ":00Z")));
        statement.setTimestamp(3, Timestamp.from(Instant.parse(values[1] + ":00Z")));
    }

    private static String describe(Connection connection, String sql) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            final ParameterMetaData metaData = statement.getParameterMetaData();
            final StringBuilder types = new StringBuilder();
            for (int i = 1, n = metaData.getParameterCount(); i <= n; i++) {
                types.append(i > 1 ? "," : "").append(metaData.getParameterTypeName(i));
            }
            return types.toString();
        }
    }

    private static String explain(Connection connection, String sql, @Nullable Binder binder, String[] values) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("EXPLAIN " + sql)) {
            if (binder != null) {
                binder.bind(statement, values);
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

    private static boolean isOnStack(StackTraceElement[] stack, String methodName) {
        for (StackTraceElement frame : stack) {
            if (methodName.equals(frame.getMethodName())) {
                return true;
            }
        }
        return false;
    }

    private void assertCancelled(String sql, int latchCount, boolean isParallelBuild) throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            if (isParallelBuild) {
                useParallelBuild();
            }
            withServer(true, false, null, port -> {
                final AtomicBoolean isLatched = new AtomicBoolean();
                final CountDownLatch reached = new CountDownLatch(1);
                final CountDownLatch cancelSent = new CountDownLatch(1);
                TestLatchedCounterFunctionFactory.reset(new TestLatchedCounterFunctionFactory.Callback() {
                    @Override
                    public boolean onGet(Record rec, int count) {
                        // Blocks once, so the execution after the cancellation runs freely.
                        if (count >= latchCount && isLatched.compareAndSet(false, true)) {
                            reached.countDown();
                            await(cancelSent);
                        }
                        return true;
                    }
                });
                try (
                        Connection connection = getConnection(Mode.EXTENDED, port, true, 1);
                        PreparedStatement statement = connection.prepareStatement(sql)
                ) {
                    assertFused(explain(connection, sql, null, null), true);
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
                        statement.executeQuery().close();
                        Assert.fail("expected the cancellation of " + sql);
                    } catch (PSQLException e) {
                        assertContains(e.getMessage(), "cancelled by user");
                    } finally {
                        canceller.join();
                    }
                    Assert.assertTrue(isCancelSent.get());
                    // The connection stays usable and the next execution builds anew.
                    try (ResultSet resultSet = statement.executeQuery()) {
                        assertResultSet(sql, LEFT_EXPECTED, sink, resultSet);
                    }
                } finally {
                    TestLatchedCounterFunctionFactory.reset(null);
                }
            });
        });
    }

    private void assertFiberOwnerBuildsInRounds(@Nullable WorkerPoolMode queryPoolMode) throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            useParallelBuild();
            setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 4);
            withServer(true, true, queryPoolMode, port -> {
                final Set<String> buildThreads = ConcurrentHashMap.newKeySet();
                final AtomicBoolean isOwnerBuild = new AtomicBoolean();
                final AtomicBoolean isOwnerRoundOnFiber = new AtomicBoolean();
                final AtomicBoolean isOwnerRoundOffFiber = new AtomicBoolean();
                TestLatchedCounterFunctionFactory.reset(new TestLatchedCounterFunctionFactory.Callback() {
                    @Override
                    public boolean onGet(Record rec, int count) {
                        final StackTraceElement[] stack = Thread.currentThread().getStackTrace();
                        buildThreads.add(Thread.currentThread().getName());
                        if (isOnStack(stack, "buildOnOwner")) {
                            isOwnerBuild.set(true);
                        }
                        // The owner runs a round task itself from within buildInRounds().
                        if (isOnStack(stack, "buildInRounds")) {
                            if (Fiber.isMounted()) {
                                isOwnerRoundOnFiber.set(true);
                            } else {
                                isOwnerRoundOffFiber.set(true);
                            }
                        }
                        return true;
                    }
                });
                try (
                        Connection connection = getConnection(Mode.EXTENDED, port, true, 1);
                        PreparedStatement statement = connection.prepareStatement(BUILD_LATCHED_SQL)
                ) {
                    assertFused(explain(connection, BUILD_LATCHED_SQL, null, null), true);
                    for (int i = 0; i < 3; i++) {
                        try (ResultSet resultSet = statement.executeQuery()) {
                            assertResultSet(BUILD_LATCHED_SQL, LEFT_EXPECTED, sink, resultSet);
                        }
                    }
                } finally {
                    TestLatchedCounterFunctionFactory.reset(null);
                }
                final String label = "build threads: " + buildThreads;
                Assert.assertFalse(label, buildThreads.isEmpty());
                Assert.assertFalse(label, isOwnerBuild.get());
                // The owner never runs a round task off its fiber.
                Assert.assertFalse(label, isOwnerRoundOffFiber.get());
                if (queryPoolMode == null) {
                    Assert.assertTrue(label, isOwnerRoundOnFiber.get());
                }
            });
        });
    }

    private void assertNamedStatement(String sql, Binder binder, String[] expected, boolean isBuildChoice) throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (boolean isFused : new boolean[]{true, false}) {
                withServer(isFused, false, null, port -> {
                    for (boolean isBinary : new boolean[]{true, false}) {
                        // A prepare threshold of 1 makes pgjdbc parse a named statement once and
                        // bind it again on every later execution.
                        try (
                                Connection connection = getConnection(Mode.EXTENDED, port, isBinary, 1);
                                PreparedStatement statement = connection.prepareStatement(sql)
                        ) {
                            for (int pass = 0; pass < 2; pass++) {
                                for (int i = 0; i < BINDS.length; i++) {
                                    binder.bind(statement, BINDS[i]);
                                    try (ResultSet resultSet = statement.executeQuery()) {
                                        assertResultSet(Arrays.toString(BINDS[i]), expected[i], sink, resultSet);
                                    }
                                }
                            }
                            final String plan = explain(connection, sql, binder, BINDS[0]);
                            assertFused(plan, isFused);
                            Assert.assertEquals(plan, isFused && isBuildChoice, plan.contains("Hash Join Group By Build Choice"));
                        }
                    }
                });
            }
        });
    }

    private void assertTimedOut(String sql, int latchCount) throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            withServer(true, false, null, port -> {
                final AtomicBoolean isLatched = new AtomicBoolean();
                TestLatchedCounterFunctionFactory.reset(new TestLatchedCounterFunctionFactory.Callback() {
                    @Override
                    public boolean onGet(Record rec, int count) {
                        if (count >= latchCount && isLatched.compareAndSet(false, true)) {
                            Os.sleep(1_000);
                        }
                        return true;
                    }
                });
                try (
                        Connection connection = getConnection(port, false, true, 500);
                        PreparedStatement statement = connection.prepareStatement(sql)
                ) {
                    try {
                        statement.executeQuery().close();
                        Assert.fail("expected the timeout of " + sql);
                    } catch (PSQLException e) {
                        assertContains(e.getMessage(), "timeout, query aborted");
                    }
                    Assert.assertTrue(isLatched.get());
                    // The next execution runs within the timeout.
                    try (ResultSet resultSet = statement.executeQuery()) {
                        assertResultSet(sql, LEFT_EXPECTED, sink, resultSet);
                    }
                } finally {
                    TestLatchedCounterFunctionFactory.reset(null);
                }
            });
        });
    }

    // f holds 100 rows a day for ten days, with keys 0..59; d holds 300 rows, six for each of the
    // keys 0..49, so the keys 50..59 of f find no match.
    private void createTables() throws Exception {
        execute("CREATE TABLE f (ts TIMESTAMP, k INT, v LONG) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE d (k INT, g INT, sym SYMBOL, w DOUBLE)");
        execute("INSERT INTO f SELECT timestamp_sequence('2024-01-01', 864_000_000), (x % 60)::INT, x FROM long_sequence(1_000)");
        execute("""
                INSERT INTO d SELECT (x % 50)::INT, (x % 7)::INT, CASE WHEN x % 2 = 0 THEN 'a' ELSE 'b' END, x * 0.5
                FROM long_sequence(300)""");
    }

    // Builds every key on the query workers, over as many hash partitions as the rows allow.
    private void useParallelBuild() {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 0);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 1);
    }

    /**
     * Runs the code against a PG server whose connections enable or disable the fused path. The
     * server's network pool hosts the connections on fibers when asked, and an optional query pool
     * of its own runs the parallel tasks; without one the owner runs every task.
     */
    private void withServer(
            boolean isFused,
            boolean isFiber,
            @Nullable WorkerPoolMode queryPoolMode,
            ServerCode code
    ) throws Exception {
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
                                context.setParallelHashJoinGroupByEnabled(isFused);
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
    private interface Binder {
        void bind(PreparedStatement statement, String[] values) throws SQLException;
    }

    @FunctionalInterface
    private interface ServerCode {
        void run(int port) throws Exception;
    }
}
