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
import io.questdb.griffin.engine.table.HashJoinBuildStage;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.std.Os;
import io.questdb.std.str.StringSink;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Drives the parallel hash join through PG wire with the real pgjdbc driver: named statements whose
 * bind variables sit in a LEFT join's ON residual, in the probe input's WHERE clause and in a
 * post-join filter, and a LIKE pattern over a small build of several frames, whose filter the build's
 * rounds run on the workers, each executed many times with new values, in text and binary transfer, against
 * a server whose connections run the operator and one whose connections run the ordinary plan, and
 * a walking join that the driver fetches seven rows at a time while the query workers join the
 * frames ahead of the one the server's cursor holds. The two servers return the same rows in the
 * same order.
 */
public class PGAsyncHashJoinTest extends BasePGTest {
    // [lo, hi]: the values each execution binds.
    private static final long[][] BINDS = {{3, 40}, {0, 90}, {7, 12}, {100, 200}, {3, 40}};
    private static final String[] QUERIES = {
            // A bind variable in the ON residual and one in the probe's WHERE clause.
            "SELECT f.ts, f.k, f.v, d.w, d.sym FROM f LEFT JOIN d ON f.k = d.k AND d.w > ? WHERE f.v < ?",
            // Both in the WHERE clause: one filters the build, the other is a post-join filter.
            "SELECT f.ts, f.k, f.v, d.w FROM f JOIN d ON f.k = d.k WHERE d.w >= ? AND f.v + d.w < ? AND f.ts IN '2024-01-01'",
            // A residual over both inputs and a bound post-join filter of a LEFT join.
            "SELECT f.k, f.v, d.w FROM f LEFT JOIN d ON f.k = d.k AND d.w + ? > f.v WHERE d.w IS NULL OR d.w < ? ORDER BY f.ts DESC",
    };
    private static final StackWalker STACK_WALKER = StackWalker.getInstance();
    private static final String STAGE = HashJoinBuildStage.class.getName();
    private static final int WORKER_COUNT = 4;

    @Test
    public void testNamedStatementWithBuildLikePattern() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE f AS (SELECT (x % 9)::INT k, x v, timestamp_sequence('2024-01-01', 1_800_000_000) ts FROM long_sequence(120)) TIMESTAMP(ts) PARTITION BY DAY");
            // dp holds 120 rows over three days, far fewer than the configured 300_000, and its scan's
            // filter, which the JIT does not compile, filters its frames in the build's rounds, as the
            // filter's test_latched_counter() records.
            execute("""
                    CREATE TABLE dp AS (
                        SELECT (x % 7)::INT k, x w, CASE WHEN x % 3 = 0 THEN 'bar' || x ELSE 'foo' || x END::VARCHAR vch,
                            timestamp_sequence('2024-01-01', 2_160_000_000) ts
                        FROM long_sequence(120)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            final String sql = "SELECT f.ts, f.k, f.v, d.w, d.vch FROM f JOIN (SELECT * FROM dp WHERE vch LIKE ? AND test_latched_counter()) d ON f.k = d.k";
            // Patterns that keep some rows, every row and none, executed twice over.
            final String[] patterns = {"%a%", "%", "zzz%", "bar1%"};
            final StringSink expected = new StringSink();
            final StringSink actual = new StringSink();
            withServer(false, null, port -> readPatterns(port, sql, patterns, expected, false));
            final AtomicInteger roundCalls = new AtomicInteger();
            final AtomicInteger ownerCalls = new AtomicInteger();
            // The function takes the callback when the statement compiles.
            TestLatchedCounterFunctionFactory.reset(new TestLatchedCounterFunctionFactory.Callback() {
                @Override
                public boolean onGet(Record rec, int count) {
                    if (isOnStack("partitionFrame")) {
                        roundCalls.incrementAndGet();
                    } else if (isOnStack("buildOnOwner")) {
                        ownerCalls.incrementAndGet();
                    }
                    return true;
                }
            });
            try {
                withServer(true, null, port -> readPatterns(port, sql, patterns, actual, true));
            } finally {
                TestLatchedCounterFunctionFactory.reset(null);
            }
            Assert.assertTrue(expected.length() > 0);
            TestUtils.assertEquals(expected, actual);
            Assert.assertTrue(roundCalls.get() > 0);
            Assert.assertEquals(0, ownerCalls.get());
        });
    }

    @Test
    public void testNamedStatementsWithBindVariablesInOnAndWhere() throws Exception {
        assertNamedStatements(null);
    }

    @Test
    public void testNamedStatementsWithBindVariablesOnQueryWorkers() throws Exception {
        assertNamedStatements(WorkerPoolMode.LEGACY);
    }

    @Test
    public void testNamedStatementsWithLimitsThatSwitchSymbolKeyTranslation() throws Exception {
        assertMemoryLeak(() -> {
            // fs.sym holds 300,000 texts, so the join translates its key through one shared cache when it
            // reads at least ceil(300,000 / 256) = 1,172 probe rows first, and through each worker's own
            // cache otherwise. A LIMIT of 1,171 or 10 reads fewer, a LIMIT of 1,172 or of the March
            // partition's 1,000 rows not, so each execution of a statement may switch.
            execute("CREATE TABLE fs (sym SYMBOL CAPACITY 524288, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO fs SELECT 'k' || x, x, timestamp_sequence('2024-01-01', 1_000_000) FROM long_sequence(300_000)");
            execute("INSERT INTO fs SELECT 'k' || ((x - 1) % 900 + 1), x, timestamp_sequence('2024-03-01', 1_000_000) FROM long_sequence(1_000)");
            execute("CREATE TABLE ds AS (SELECT ('k' || x)::SYMBOL sym, x w FROM long_sequence(1_000))");
            final String[] queries = {
                    "SELECT fs.ts, fs.sym, ds.w FROM fs JOIN ds ON fs.sym = ds.sym LIMIT ?",
                    "SELECT fs.ts, fs.sym, ds.w FROM fs LEFT JOIN ds ON fs.sym = ds.sym WHERE fs.ts IN '2024-03-01' LIMIT ?",
            };
            final long[][] limits = {{1_171, 1_172, 1_171}, {10, 1_000_000, 10}};
            final StringSink expected = new StringSink();
            final StringSink actual = new StringSink();
            withServer(false, WorkerPoolMode.LEGACY, port -> readLimits(port, queries, limits, expected, false));
            withServer(true, WorkerPoolMode.LEGACY, port -> readLimits(port, queries, limits, actual, true));
            // Each execution of the first statement returns its LIMIT's rows, twice per transfer format.
            Assert.assertTrue(countLines(expected) > 2 * 2 * (1_171 + 1_172 + 1_171));
            TestUtils.assertEquals(expected, actual);
        });
    }

    @Test
    public void testWalkingJoinReadSevenRowsAtATime() throws Exception {
        assertMemoryLeak(() -> {
            // f8 holds 96 probe rows, twelve frames of eight rows, and each meets the three rows of its
            // key in d3, every one of which the residual keeps. pgjdbc fetches seven rows per Execute,
            // so the server's consumer holds a frame across round trips while the query workers join
            // the frames after it: a window of three frames sends the frame two past the one the
            // consumer takes out as it takes it. The connections take the configured frame sizes, and
            // a task's cap of tuples stays at a million, which no frame reaches.
            setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 8);
            setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 8);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1_000_000);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 3);
            execute("CREATE TABLE f8 AS (SELECT (x % 4)::INT k, x v, timestamp_sequence('2024-01-01', 60_000_000) ts FROM long_sequence(96)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE d3 AS (SELECT (x % 4)::INT k, x * 100 w FROM long_sequence(12))");
            final String sql = "SELECT f8.v, d3.w FROM f8 LEFT JOIN d3 ON f8.k = d3.k AND d3.w > f8.v";
            final StringSink expected = new StringSink();
            final StringSink actual = new StringSink();
            withServer(false, WorkerPoolMode.LEGACY, port -> readInChunks(port, sql, expected, false));
            withServer(true, WorkerPoolMode.LEGACY, port -> readInChunks(port, sql, actual, true));
            // Each of the 96 probe rows yields three rows, in binary and text transfer, twice each.
            Assert.assertEquals(2 * 2 * 96 * 3, countLines(expected));
            TestUtils.assertEquals(expected, actual);
        });
    }

    private static String explain(Connection connection, String sql, long[] values) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("EXPLAIN " + sql)) {
            statement.setLong(1, values[0]);
            statement.setLong(2, values[1]);
            try (ResultSet resultSet = statement.executeQuery()) {
                final StringBuilder plan = new StringBuilder();
                while (resultSet.next()) {
                    plan.append(resultSet.getString(1)).append('\n');
                }
                return plan.toString();
            }
        }
    }

    private static long countLines(CharSequence text) {
        long lines = 0;
        for (int i = 0, n = text.length(); i < n; i++) {
            if (text.charAt(i) == '\n') {
                lines++;
            }
        }
        return lines;
    }

    private static boolean isOnStack(String methodName) {
        return STACK_WALKER.walk(frames -> frames.anyMatch(frame -> frame.getClassName().equals(STAGE) && frame.getMethodName().equals(methodName)));
    }

    private void assertNamedStatements(@Nullable WorkerPoolMode queryPoolMode) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE f AS (SELECT (x % 9)::INT k, x v, timestamp_sequence('2024-01-01', 1_800_000_000) ts FROM long_sequence(120)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE d AS (SELECT CASE WHEN x % 10 = 0 THEN NULL ELSE (x % 7)::INT END k, x w, ('s' || (x % 3))::SYMBOL sym FROM long_sequence(40))");
            // The rows of every query and bind set, read through the ordinary plan.
            final StringSink expected = new StringSink();
            final StringSink actual = new StringSink();
            withServer(false, queryPoolMode, port -> read(port, expected, false));
            withServer(true, queryPoolMode, port -> read(port, actual, true));
            Assert.assertTrue(expected.length() > 0);
            TestUtils.assertEquals(expected, actual);
        });
    }

    // Executes every query's named statement with every bind set, twice over, in binary and text
    // transfer, and appends the rows to the sink.
    private void read(int port, StringSink sink, boolean isParallel) throws Exception {
        final StringSink rows = new StringSink();
        for (boolean isBinary : new boolean[]{true, false}) {
            for (String sql : QUERIES) {
                // A prepare threshold of 1 makes pgjdbc parse a named statement once and bind it again
                // on every later execution.
                try (
                        Connection connection = getConnection(Mode.EXTENDED, port, isBinary, 1);
                        PreparedStatement statement = connection.prepareStatement(sql)
                ) {
                    for (int pass = 0; pass < 2; pass++) {
                        for (long[] values : BINDS) {
                            statement.setLong(1, values[0]);
                            statement.setLong(2, values[1]);
                            try (ResultSet resultSet = statement.executeQuery()) {
                                rows.clear();
                                printToSink(rows, resultSet, null);
                            }
                            sink.put(sql).put(' ').put(values[0]).put(',').put(values[1]).put('\n').put(rows);
                        }
                    }
                    final String plan = explain(connection, sql, BINDS[0]);
                    Assert.assertEquals(plan, isParallel, plan.contains("Hash Join workers:"));
                }
            }
        }
    }

    // Executes each query's named statement with each of its LIMITs, twice over, in binary and text
    // transfer, and appends the rows to the sink.
    private void readLimits(int port, String[] queries, long[][] limits, StringSink sink, boolean isParallel) throws Exception {
        final StringSink rows = new StringSink();
        for (boolean isBinary : new boolean[]{true, false}) {
            for (int q = 0; q < queries.length; q++) {
                // A prepare threshold of 1 makes pgjdbc parse a named statement once and bind it again
                // on every later execution.
                try (
                        Connection connection = getConnection(Mode.EXTENDED, port, isBinary, 1);
                        PreparedStatement statement = connection.prepareStatement(queries[q])
                ) {
                    for (int pass = 0; pass < 2; pass++) {
                        for (long limit : limits[q]) {
                            statement.setLong(1, limit);
                            try (ResultSet resultSet = statement.executeQuery()) {
                                rows.clear();
                                printToSink(rows, resultSet, null);
                            }
                            sink.put(queries[q]).put(' ').put(limit).put('\n').put(rows);
                        }
                    }
                    try (PreparedStatement explain = connection.prepareStatement("EXPLAIN " + queries[q])) {
                        explain.setLong(1, limits[q][0]);
                        try (ResultSet resultSet = explain.executeQuery()) {
                            final StringBuilder plan = new StringBuilder();
                            while (resultSet.next()) {
                                plan.append(resultSet.getString(1)).append('\n');
                            }
                            Assert.assertEquals(plan.toString(), isParallel, plan.indexOf("Hash Join workers:") > -1);
                            Assert.assertTrue(plan.toString(), plan.indexOf("symbolKeyJoin: true") > -1);
                        }
                    }
                }
            }
        }
    }

    // Executes the query's prepared statement twice, in binary and text transfer, fetching seven rows
    // per Execute, and appends the rows to the sink.
    private void readInChunks(int port, String sql, StringSink sink, boolean isParallel) throws Exception {
        for (boolean isBinary : new boolean[]{true, false}) {
            try (Connection connection = getConnection(Mode.EXTENDED, port, isBinary, 1)) {
                // pgjdbc fetches in chunks only outside autocommit.
                connection.setAutoCommit(false);
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setFetchSize(7);
                    for (int execution = 0; execution < 2; execution++) {
                        try (ResultSet resultSet = statement.executeQuery()) {
                            Assert.assertEquals(7, resultSet.getFetchSize());
                            while (resultSet.next()) {
                                sink.put(resultSet.getLong(1)).put(',').put(resultSet.getLong(2)).put('\n');
                            }
                        }
                    }
                }
                try (PreparedStatement explain = connection.prepareStatement("EXPLAIN " + sql)) {
                    try (ResultSet resultSet = explain.executeQuery()) {
                        final StringBuilder plan = new StringBuilder();
                        while (resultSet.next()) {
                            plan.append(resultSet.getString(1)).append('\n');
                        }
                        Assert.assertEquals(plan.toString(), isParallel, plan.indexOf("Hash Join workers:") > -1);
                    }
                }
                connection.commit();
            }
        }
    }

    // Executes the query's named statement with every pattern, twice over, in binary and text
    // transfer, and appends the rows to the sink.
    private void readPatterns(int port, String sql, String[] patterns, StringSink sink, boolean isParallel) throws Exception {
        final StringSink rows = new StringSink();
        for (boolean isBinary : new boolean[]{true, false}) {
            // A prepare threshold of 1 makes pgjdbc parse a named statement once and bind it again on
            // every later execution.
            try (
                    Connection connection = getConnection(Mode.EXTENDED, port, isBinary, 1);
                    PreparedStatement statement = connection.prepareStatement(sql)
            ) {
                for (int pass = 0; pass < 2; pass++) {
                    for (String pattern : patterns) {
                        statement.setString(1, pattern);
                        try (ResultSet resultSet = statement.executeQuery()) {
                            rows.clear();
                            printToSink(rows, resultSet, null);
                        }
                        sink.put(pattern).put('\n').put(rows);
                    }
                }
                try (PreparedStatement explain = connection.prepareStatement("EXPLAIN " + sql)) {
                    explain.setString(1, patterns[0]);
                    try (ResultSet resultSet = explain.executeQuery()) {
                        final StringBuilder plan = new StringBuilder();
                        while (resultSet.next()) {
                            plan.append(resultSet.getString(1)).append('\n');
                        }
                        Assert.assertEquals(plan.toString(), isParallel, plan.indexOf("Hash Join workers:") > -1);
                        Assert.assertEquals(plan.toString(), isParallel, plan.indexOf("buildFilter: ") > -1);
                    }
                }
            }
        }
    }

    /**
     * Runs the code against a PG server whose connections run the parallel hash join or not, with a
     * query pool of their own when asked; without one the owner runs every task.
     */
    private void withServer(boolean isParallel, @Nullable WorkerPoolMode queryPoolMode, ServerCode code) throws Exception {
        final Port0PGConfiguration configuration = new Port0PGConfiguration() {
            @Override
            public int getWorkerCount() {
                return 2;
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
}
