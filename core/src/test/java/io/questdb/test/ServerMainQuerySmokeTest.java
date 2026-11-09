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

package io.questdb.test;

import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.Numbers;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.postgresql.util.PSQLException;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.test.cutlass.pgwire.BasePGTest.assertResultSet;
import static io.questdb.test.tools.TestUtils.assertContains;
import static io.questdb.test.tools.TestUtils.unchecked;

@RunWith(Parameterized.class)
public class ServerMainQuerySmokeTest extends AbstractBootstrapTest {
    private static final long FUZZ_HANG_ROUND_NANOS = 5_000_000_000L;
    private static final int FUZZ_KEY_COUNT = 100;
    private static final int FUZZ_QUERIES_PER_CLIENT = 10;
    private static final long FUZZ_ROW_COUNT = 500_000;
    private static final StringSink sink = new StringSink();
    private final boolean convertToParquet;

    public ServerMainQuerySmokeTest(boolean convertToParquet) {
        this.convertToParquet = convertToParquet;
    }

    @Parameterized.Parameters(name = "parquet={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {true},
                {false},
        });
    }

    @Before
    public void setUp() {
        super.setUp();
        unchecked(() -> createDummyConfiguration(
                // Force enable parallel GROUP BY and filter for smoke tests.
                PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED + "=true",
                PropertyKey.CAIRO_SQL_PARALLEL_FILTER_ENABLED + "=true",
                PropertyKey.SHARED_WORKER_COUNT + "=4",
                PropertyKey.PG_WORKER_COUNT + "=4",
                PropertyKey.PG_SELECT_CACHE_ENABLED + "=true",
                PropertyKey.CAIRO_SQL_PARALLEL_WORK_STEALING_THRESHOLD + "=1",
                PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD + "=100",
                PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE + "=100",
                PropertyKey.QUERY_TIMEOUT + "=150000",
                // JIT doesn't support ARM, and we want exec plans to be the same.
                PropertyKey.CAIRO_SQL_JIT_MODE + "=off",
                PropertyKey.DEBUG_ENABLE_TEST_FACTORIES + "=true"
        ));
        dbPath.parent().$();
    }

    @Test
    public void testBindVariableFilterWithBatchedFetch() throws Exception {
        Assume.assumeFalse(convertToParquet);
        createDummyConfiguration(
                PropertyKey.CAIRO_SQL_PARALLEL_FILTER_ENABLED + "=true",
                PropertyKey.CAIRO_SQL_JIT_MODE + "=off",
                PropertyKey.PG_WORKER_COUNT + "=2",
                PropertyKey.SHARED_QUERY_WORKER_COUNT + "=1",
                PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT + "=2",
                PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY + "=32"
        );
        final Rnd rnd = TestUtils.generateRandom(LOG);
        try (ServerMain serverMain = new ServerMain(getServerMainArgs())) {
            serverMain.start();
            createFuzzTable();
            try (Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES)) {
                conn.setAutoCommit(false);
                for (int i = 0; i < 100; i++) {
                    final int key = 1 + rnd.nextInt(FUZZ_KEY_COUNT - 1);
                    assertRows(
                            conn,
                            null,
                            50 + rnd.nextInt(450),
                            "SELECT x FROM tab WHERE key = ?",
                            "k" + key,
                            key,
                            FUZZ_KEY_COUNT,
                            (FUZZ_ROW_COUNT - key) / FUZZ_KEY_COUNT + 1
                    );
                }
            }
        }
    }

    @Test
    public void testParallelFilterOomError() throws Exception {
        Assume.assumeFalse(convertToParquet);
        try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
            serverMain.start();
            try (Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES)) {
                try (Statement stat = conn.createStatement()) {
                    stat.execute(
                            "create table x as (" +
                                    " select timestamp_sequence(0, 1000000) ts, x" +
                                    " from long_sequence(10000000)" +
                                    ") timestamp(ts);"
                    );
                }

                final String expected = """
                        count()[BIGINT]
                        9999999
                        """;
                try (PreparedStatement stmt = conn.prepareStatement("select count() from x where x != 1")) {
                    // Set RSS limit, so that the SELECT will fail with OOM.
                    // The limit should be high enough to let worker threads fail on reduce.
                    Unsafe.setRssMemLimit(39 * Numbers.SIZE_1MB);
                    try (ResultSet rs = stmt.executeQuery()) {
                        sink.clear();
                        assertResultSet(expected, sink, rs);
                        Assert.fail();
                    } catch (PSQLException e) {
                        assertContains(e.getMessage(), "global RSS memory limit exceeded");
                    }

                    // Remove the limit, this time the query should succeed.
                    Unsafe.setRssMemLimit(0);
                    try (ResultSet rs = stmt.executeQuery()) {
                        sink.clear();
                        assertResultSet(expected, sink, rs);
                    }
                } finally {
                    Unsafe.setRssMemLimit(0);
                }
            }
        }
    }

    @Test
    public void testParallelGroupByOomError() throws Exception {
        Assume.assumeFalse(convertToParquet);
        try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
            serverMain.start();
            try (Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES)) {
                try (Statement stat = conn.createStatement()) {
                    stat.execute(
                            "create table x as (" +
                                    " select timestamp_sequence(0, 1000000) ts, x::varchar as x" +
                                    " from long_sequence(10000000)" +
                                    ") timestamp(ts);"
                    );
                }

                final String expected = """
                        count()[BIGINT]
                        10000000
                        """;
                try (PreparedStatement stmt = conn.prepareStatement("select count() from (select x from x group by x)")) {
                    // Set RSS limit, so that the SELECT will fail with OOM.
                    // The limit should be high enough to let worker threads fail on reduce.
                    Unsafe.setRssMemLimit(39 * Numbers.SIZE_1MB);
                    try (ResultSet rs = stmt.executeQuery()) {
                        sink.clear();
                        assertResultSet(expected, sink, rs);
                        Assert.fail();
                    } catch (PSQLException e) {
                        assertContains(e.getMessage(), "global RSS memory limit exceeded");
                    }

                    // Remove the limit, this time the query should succeed.
                    Unsafe.setRssMemLimit(0);
                    try (ResultSet rs = stmt.executeQuery()) {
                        sink.clear();
                        assertResultSet(expected, sink, rs);
                    }
                } finally {
                    Unsafe.setRssMemLimit(0);
                }
            }
        }
    }

    @Test
    public void testParallelQueryFuzzOverPgWireAndHttp() throws Exception {
        Assume.assumeFalse(convertToParquet);
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final int roundCount = Boolean.getBoolean("questdb.fuzz.nightly") ? 20 : 2;
        for (int round = 0; round < roundCount; round++) {
            final boolean isHangShape = round == 0;
            final boolean isPgFiberEnabled = isHangShape || rnd.nextBoolean();
            final boolean isHttpFiberEnabled = isHangShape || rnd.nextBoolean();
            final boolean isQueryFiberEnabled = isHangShape || rnd.nextBoolean();
            final boolean isJitEnabled = !isHangShape && rnd.nextBoolean();
            final boolean isParallelGroupByEnabled = isHangShape || rnd.nextBoolean();
            final boolean isParallelTopKEnabled = isHangShape || rnd.nextBoolean();
            final int pgWorkerCount = isHangShape ? 2 + rnd.nextInt(2) : 1 + rnd.nextInt(3);
            final int httpWorkerCount = 1 + rnd.nextInt(isHangShape ? 2 : 3);
            final int clientsPerWorker = isHangShape ? 4 : 2 + rnd.nextInt(3);
            final int queryWorkerCount = isHangShape ? 1 : 1 + rnd.nextInt(2);
            final int shardCount = isHangShape ? 2 : 1 + rnd.nextInt(2);
            final int queueCapacity = isHangShape ? 32 : 4 << rnd.nextInt(4);
            final int dispatchLimit = !isHangShape && rnd.nextBoolean() ? 1 + rnd.nextInt(64) : 0;
            final int stealingThreshold = !isHangShape && rnd.nextBoolean() ? 1 + rnd.nextInt(16) : 0;
            final String config = "round=" + round
                    + ", pgFiber=" + isPgFiberEnabled
                    + ", httpFiber=" + isHttpFiberEnabled
                    + ", queryFiber=" + isQueryFiberEnabled
                    + ", jit=" + isJitEnabled
                    + ", parallelGroupBy=" + isParallelGroupByEnabled
                    + ", parallelTopK=" + isParallelTopKEnabled
                    + ", pgWorkers=" + pgWorkerCount
                    + ", httpWorkers=" + httpWorkerCount
                    + ", clientsPerWorker=" + clientsPerWorker
                    + ", queryWorkers=" + queryWorkerCount
                    + ", shards=" + shardCount
                    + ", queueCapacity=" + queueCapacity
                    + ", dispatchLimit=" + dispatchLimit
                    + ", stealingThreshold=" + stealingThreshold;
            LOG.info().$("parallel query fuzz [").$(config).I$();
            final List<String> properties = new ArrayList<>(List.of(
                    PropertyKey.CAIRO_SQL_PARALLEL_FILTER_ENABLED + "=true",
                    PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED + "=" + isParallelGroupByEnabled,
                    PropertyKey.CAIRO_SQL_PARALLEL_TOP_K_ENABLED + "=" + isParallelTopKEnabled,
                    PropertyKey.CAIRO_SQL_JIT_MODE + "=" + (isJitEnabled ? "on" : "off"),
                    PropertyKey.PG_WORKER_COUNT + "=" + pgWorkerCount,
                    PropertyKey.PG_WORKER_FIBER_ENABLED + "=" + isPgFiberEnabled,
                    PropertyKey.HTTP_WORKER_COUNT + "=" + httpWorkerCount,
                    PropertyKey.HTTP_WORKER_FIBER_ENABLED + "=" + isHttpFiberEnabled,
                    PropertyKey.SHARED_QUERY_WORKER_COUNT + "=" + queryWorkerCount,
                    PropertyKey.SHARED_QUERY_WORKER_FIBER_ENABLED + "=" + isQueryFiberEnabled,
                    PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT + "=" + shardCount,
                    PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY + "=" + queueCapacity
            ));
            if (dispatchLimit > 0) {
                properties.add(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT + "=" + dispatchLimit);
            }
            if (stealingThreshold > 0) {
                properties.add(PropertyKey.CAIRO_SQL_PARALLEL_WORK_STEALING_THRESHOLD + "=" + stealingThreshold);
            }
            createDummyConfiguration(properties.toArray(new String[0]));
            final HttpClient httpClient = HttpClient.newHttpClient();
            try (ServerMain serverMain = new ServerMain(getServerMainArgs())) {
                serverMain.start();
                if (round == 0) {
                    createFuzzTable();
                }
                final int pgClientCount = pgWorkerCount * clientsPerWorker;
                final int clientCount = pgClientCount + httpWorkerCount * clientsPerWorker;
                final long deadlineNanos = System.nanoTime() + (isHangShape ? FUZZ_HANG_ROUND_NANOS : 0);
                final CountDownLatch doneLatch = new CountDownLatch(clientCount);
                final AtomicReference<Throwable> failure = new AtomicReference<>();
                for (int i = 0; i < clientCount; i++) {
                    final boolean isPgClient = i < pgClientCount;
                    final Rnd clientRnd = new Rnd(rnd.nextLong(), rnd.nextLong());
                    final int fetchSize = isPgClient && clientRnd.nextBoolean() ? 50 + clientRnd.nextInt(450) : 0;
                    final Thread client = new Thread(() -> {
                        try (Connection conn = isPgClient ? DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES) : null) {
                            if (conn != null && fetchSize > 0) {
                                conn.setAutoCommit(false);
                            }
                            for (int q = 0; q < FUZZ_QUERIES_PER_CLIENT || System.nanoTime() < deadlineNanos; q++) {
                                assertFuzzQuery(conn, httpClient, fetchSize, clientRnd, isHangShape);
                            }
                        } catch (Throwable th) {
                            failure.compareAndSet(null, th);
                        } finally {
                            doneLatch.countDown();
                        }
                    });
                    client.setDaemon(true);
                    client.start();
                }
                if (!doneLatch.await(65, TimeUnit.SECONDS)) {
                    TestListener.dumpThreadStacks();
                    Assert.fail("queries stalled [" + config + ']');
                }
                if (failure.get() != null) {
                    throw new AssertionError("query failed [" + config + ']', failure.get());
                }
            } finally {
                httpClient.shutdownNow();
            }
        }
    }

    @Test
    public void testServerMainGlobalQueryCacheSmokeTest() {
        Assume.assumeFalse(convertToParquet);

        // Verify that global cache is correctly synchronized, so that
        // no record cursor factory is used by multiple threads concurrently.
        record TestCase(String query, String expectedResult) {
        }
        final int nQueries = 10;
        final TestCase[] testCases = new TestCase[nQueries];
        for (int i = 0; i < nQueries; i++) {
            testCases[i] = new TestCase(
                    "select owners owners_" + i + " from test_owner_counter();",
                    "owners_" + i + "[INTEGER]\n" +
                            "1\n"
            );
        }

        try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
            serverMain.start();

            final int nThreads = 4;
            final int nIterations = 1000;

            final CyclicBarrier startBarrier = new CyclicBarrier(nThreads);
            final SOCountDownLatch doneLatch = new SOCountDownLatch(nThreads);
            final AtomicInteger errors = new AtomicInteger();
            for (int t = 0; t < nThreads; t++) {
                final int threadId = t;
                new Thread(() -> {
                    final Rnd rnd = new Rnd(threadId, threadId);
                    final StringSink sink = new StringSink();
                    try {
                        startBarrier.await();

                        try (Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES)) {
                            for (int i = 0; i < nIterations; i++) {
                                final TestCase testCase = testCases[rnd.nextInt(nQueries)];
                                try (ResultSet rs = conn.prepareStatement(testCase.query).executeQuery()) {
                                    sink.clear();
                                    assertResultSet(testCase.expectedResult, sink, rs);
                                }
                            }
                        }
                    } catch (Throwable th) {
                        errors.incrementAndGet();
                        th.printStackTrace(System.out);
                    } finally {
                        doneLatch.countDown();
                    }
                }).start();
            }

            doneLatch.await();

            Assert.assertEquals(0, errors.get());
        }
    }

    @Test
    public void testServerMainParallelFilterLoadTest() throws Exception {
        testServerMainParallelQueryLoadTest(
                "CREATE TABLE tab as (" +
                        "  select (x * 864000000)::timestamp ts, ('k' || (x % 5))::symbol key, x:: double price, x::long quantity from long_sequence(10000)" +
                        ") timestamp (ts) PARTITION BY DAY",
                convertToParquet ? "ALTER TABLE tab CONVERT PARTITION TO PARQUET WHERE ts >= 0" : null,
                "SELECT /*+ ENABLE_PRE_TOUCH(tab) */ * FROM tab WHERE key = 'k3' LIMIT 10",
                """
                        QUERY PLAN[VARCHAR]
                        Async Filter workers: 4
                          limit: 10
                          filter: key='k3' [pre-touch]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """,
                """
                        ts[TIMESTAMP],key[VARCHAR],price[DOUBLE],quantity[BIGINT]
                        1970-01-01 00:43:12.0,k3,3.0,3
                        1970-01-01 01:55:12.0,k3,8.0,8
                        1970-01-01 03:07:12.0,k3,13.0,13
                        1970-01-01 04:19:12.0,k3,18.0,18
                        1970-01-01 05:31:12.0,k3,23.0,23
                        1970-01-01 06:43:12.0,k3,28.0,28
                        1970-01-01 07:55:12.0,k3,33.0,33
                        1970-01-01 09:07:12.0,k3,38.0,38
                        1970-01-01 10:19:12.0,k3,43.0,43
                        1970-01-01 11:31:12.0,k3,48.0,48
                        """
        );
    }

    @Test
    public void testServerMainParallelFilterSmokeTest() throws Exception {
        Assume.assumeFalse(convertToParquet);
        // Verify that circuit breaker checks don't have weird bugs unseen in fast tests.
        try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
            serverMain.start();
            try (Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES)) {
                try (Statement statement = conn.createStatement()) {
                    statement.execute("create table x as (select x % 10000 l from long_sequence(1000000));");
                }

                String query = "select count() from (select * from x where l = 42);";
                String expected = """
                        count()[BIGINT]
                        100
                        """;
                try (ResultSet rs = conn.prepareStatement(query).executeQuery()) {
                    sink.clear();
                    assertResultSet(expected, sink, rs);
                }
            }
        }
    }

    @Test
    public void testServerMainParallelGroupBySmokeTest1() throws Exception {
        Assume.assumeFalse(convertToParquet);
        // Verify that circuit breaker checks don't have weird bugs unseen in fast tests.
        try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
            serverMain.start();
            try (Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES)) {
                try (Statement statement = conn.createStatement()) {
                    statement.execute("create table x as (select x % 10000 l1, x % 1000 l2 from long_sequence(1000000));");
                }

                String query = "select count_distinct(l1), count_distinct(l2) from x;";
                String expected = """
                        count_distinct(l1)[BIGINT],count_distinct(l2)[BIGINT]
                        10000,1000
                        """;
                try (ResultSet rs = conn.prepareStatement(query).executeQuery()) {
                    sink.clear();
                    assertResultSet(expected, sink, rs);
                }
            }
        }
    }

    @Test
    public void testServerMainParallelGroupBySmokeTest2() throws Exception {
        Assume.assumeFalse(convertToParquet);
        // Verify that circuit breaker checks don't have weird bugs unseen in fast tests.
        try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
            serverMain.start();
            try (Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES)) {
                try (Statement statement = conn.createStatement()) {
                    statement.execute("create table x as (select 'k' || (x % 3) k, x % 10000 l from long_sequence(1000000));");
                }

                String query = "select k, count_distinct(l) from x order by k;";
                String expected = """
                        k[VARCHAR],count_distinct(l)[BIGINT]
                        k0,10000
                        k1,10000
                        k2,10000
                        """;
                try (ResultSet rs = conn.prepareStatement(query).executeQuery()) {
                    sink.clear();
                    assertResultSet(expected, sink, rs);
                }
            }
        }
    }

    @Test
    public void testServerMainParallelKeyedRostiGroupByLoadTest() throws Exception {
        testServerMainParallelQueryLoadTest(
                "CREATE TABLE tab as (" +
                        "  select (x * 864000000)::timestamp ts, ('k' || (x % 5))::symbol key, x:: double price, x::long quantity from long_sequence(10000)" +
                        ") timestamp (ts) PARTITION BY DAY",
                convertToParquet ? "ALTER TABLE tab CONVERT PARTITION TO PARQUET WHERE ts >= 0" : null,
                "SELECT key, min(quantity), max(quantity) FROM tab ORDER BY key DESC",
                """
                        QUERY PLAN[VARCHAR]
                        Encode sort light
                          keys: [key desc]
                            GroupBy vectorized: true workers: 4
                              keys: [key]
                              values: [min(quantity),max(quantity)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tab
                        """,
                """
                        key[VARCHAR],min(quantity)[BIGINT],max(quantity)[BIGINT]
                        k4,4,9999
                        k3,3,9998
                        k2,2,9997
                        k1,1,9996
                        k0,5,10000
                        """
        );
    }

    @Test
    public void testServerMainParallelNonKeyedRostiGroupByLoadTest() throws Exception {
        testServerMainParallelQueryLoadTest(
                "CREATE TABLE tab as (" +
                        "  select (x * 864000000)::timestamp ts, ('k' || (x % 5))::symbol key, x:: double price, x::long quantity from long_sequence(10000)" +
                        ") timestamp (ts) PARTITION BY DAY",
                convertToParquet ? "ALTER TABLE tab CONVERT PARTITION TO PARQUET WHERE ts >= 0" : null,
                "SELECT min(quantity), max(quantity) FROM tab",
                """
                        QUERY PLAN[VARCHAR]
                        Async Group By workers: 4
                          vectorized: true
                          values: [min(quantity),max(quantity)]
                          filter: null
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """,
                """
                        min(quantity)[BIGINT],max(quantity)[BIGINT]
                        1,10000
                        """
        );
    }

    @Test
    public void testServerMainParallelShardedGroupByLoadTest1() throws Exception {
        testServerMainParallelQueryLoadTest(
                "CREATE TABLE tab as (" +
                        "  select (x * 864000000)::timestamp ts, ('k' || (x % 101))::symbol key, x:: double price, x::long quantity from long_sequence(10000)" +
                        ") timestamp (ts) PARTITION BY DAY",
                convertToParquet ? "ALTER TABLE tab CONVERT PARTITION TO PARQUET WHERE ts >= 0" : null,
                "SELECT day_of_week(ts) day, key, vwap(price, quantity) FROM tab GROUP BY day, key ORDER BY day, key LIMIT 10",
                """
                        QUERY PLAN[VARCHAR]
                        Encode sort light lo: 10
                          keys: [day, key]
                            VirtualRecord
                              functions: [day,key,vwap(price, quantity)]
                                Async Group By workers: 4
                                  keys: [day,key]
                                  keyFunctions: [day_of_week(ts)]
                                  values: [vwap(price,quantity)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tab
                        """,
                """
                        day[INTEGER],key[VARCHAR],vwap(price, quantity)[DOUBLE]
                        1,k0,6624.171717171717
                        1,k1,6624.8468153184685
                        1,k10,6612.932687914096
                        1,k100,6623.496749024707
                        1,k11,6613.610770065386
                        1,k12,6281.67243296272
                        1,k13,6598.965586726309
                        1,k14,6599.64534842185
                        1,k15,6600.325238210883
                        1,k16,6601.005256016568
                        """
        );
    }

    @Test
    public void testServerMainParallelShardedGroupByLoadTest2() throws Exception {
        testServerMainParallelQueryLoadTest(
                "CREATE TABLE tab as (" +
                        "  select (x * 864000000)::timestamp ts, ('k' || (x % 101))::symbol key, x::long x from long_sequence(10000)" +
                        ") timestamp (ts) PARTITION BY DAY",
                convertToParquet ? "ALTER TABLE tab CONVERT PARTITION TO PARQUET WHERE ts >= 0" : null,
                "SELECT key, count_distinct(x) FROM tab ORDER BY key LIMIT 10",
                """
                        QUERY PLAN[VARCHAR]
                        Encode sort light lo: 10
                          keys: [key]
                            Async Group By workers: 4
                              keys: [key]
                              values: [count_distinct(x)]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tab
                        """,
                """
                        key[VARCHAR],count_distinct(x)[BIGINT]
                        k0,99
                        k1,100
                        k10,99
                        k100,99
                        k11,99
                        k12,99
                        k13,99
                        k14,99
                        k15,99
                        k16,99
                        """
        );
    }

    private static void assertFuzzQuery(@Nullable Connection conn, HttpClient httpClient, int fetchSize, Rnd rnd, boolean isHangShape) throws Exception {
        final int key = 1 + rnd.nextInt(FUZZ_KEY_COUNT - 1);
        final int otherKey = 1 + (key + rnd.nextInt(FUZZ_KEY_COUNT - 2)) % (FUZZ_KEY_COUNT - 1);
        final long matchCount = (FUZZ_ROW_COUNT - key) / FUZZ_KEY_COUNT + 1;
        final long lastMatch = key + FUZZ_KEY_COUNT * (matchCount - 1);
        final long limit = 1 + rnd.nextInt((int) (2 * matchCount));
        final long limitedCount = Math.min(limit, matchCount);
        final String keyFilter = "SELECT x FROM tab WHERE key = 'k" + key + "'";
        switch (isHangShape ? (rnd.nextInt(4) == 0 ? 0 : 8) : rnd.nextInt(9)) {
            case 0 -> {
                final long lowerBound = rnd.nextInt((int) (FUZZ_ROW_COUNT - FUZZ_KEY_COUNT));
                assertKeyedRows(
                        conn,
                        httpClient,
                        fetchSize,
                        "SELECT key, x FROM (SELECT * FROM tab WHERE key IN ('k" + key + "', 'k" + otherKey
                                + "') AND x > " + lowerBound + ") LATEST ON ts PARTITION BY key",
                        "k" + key,
                        new long[]{FUZZ_ROW_COUNT - FUZZ_KEY_COUNT + key},
                        "k" + otherKey,
                        new long[]{FUZZ_ROW_COUNT - FUZZ_KEY_COUNT + otherKey}
                );
            }
            case 1 ->
                    assertRows(conn, httpClient, fetchSize, keyFilter + " LIMIT " + limit, null, key, FUZZ_KEY_COUNT, limitedCount);
            case 2 -> assertRows(
                    conn,
                    httpClient,
                    fetchSize,
                    keyFilter + " LIMIT -" + limit,
                    null,
                    lastMatch - FUZZ_KEY_COUNT * (limitedCount - 1),
                    FUZZ_KEY_COUNT,
                    limitedCount
            );
            case 3 ->
                    assertRows(conn, httpClient, fetchSize, keyFilter + " ORDER BY ts DESC LIMIT " + limit, null, lastMatch, -FUZZ_KEY_COUNT, limitedCount);
            case 4 ->
                    assertRows(conn, httpClient, fetchSize, keyFilter + " ORDER BY x DESC LIMIT " + limit, null, lastMatch, -FUZZ_KEY_COUNT, limitedCount);
            case 5 -> {
                final long lo = rnd.nextInt((int) (FUZZ_ROW_COUNT - 2_000));
                final long hi = lo + 1 + rnd.nextInt(2_000);
                assertRows(conn, httpClient, fetchSize, "SELECT x FROM tab WHERE x > " + lo + " AND x <= " + hi, null, lo + 1, 1, hi - lo);
            }
            case 6 ->
                    assertRows(conn, httpClient, fetchSize, "SELECT count() FROM tab WHERE key = 'k" + key + "'", null, matchCount, 0, 1);
            case 7 -> {
                final long otherMatchCount = (FUZZ_ROW_COUNT - otherKey) / FUZZ_KEY_COUNT + 1;
                assertKeyedRows(
                        conn,
                        httpClient,
                        fetchSize,
                        "SELECT key, count(), sum(x) FROM tab WHERE key IN ('k" + key + "', 'k" + otherKey + "') GROUP BY key",
                        "k" + key,
                        new long[]{matchCount, matchCount * key + FUZZ_KEY_COUNT * matchCount * (matchCount - 1) / 2},
                        "k" + otherKey,
                        new long[]{otherMatchCount, otherMatchCount * otherKey + FUZZ_KEY_COUNT * otherMatchCount * (otherMatchCount - 1) / 2}
                );
            }
            default -> {
                final boolean isBind = conn != null && rnd.nextBoolean();
                assertRows(
                        conn,
                        httpClient,
                        fetchSize,
                        isBind ? "SELECT x FROM tab WHERE key = ?" : keyFilter,
                        isBind ? "k" + key : null,
                        key,
                        FUZZ_KEY_COUNT,
                        matchCount
                );
            }
        }
    }

    private static void assertKeyedRows(
            @Nullable Connection conn,
            HttpClient httpClient,
            int fetchSize,
            String sql,
            String keyA,
            long[] valuesA,
            String keyB,
            long[] valuesB
    ) throws Exception {
        if (conn == null) {
            final String body = exec(httpClient, sql);
            final String rowA = toJsonRow(keyA, valuesA);
            final String rowB = toJsonRow(keyB, valuesB);
            Assert.assertTrue(
                    sql + " returned " + body,
                    body.contains("\"dataset\":[" + rowA + "," + rowB + "],\"count\":2")
                            || body.contains("\"dataset\":[" + rowB + "," + rowA + "],\"count\":2")
            );
            return;
        }
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            if (fetchSize > 0) {
                stmt.setFetchSize(fetchSize);
            }
            boolean isASeen = false;
            boolean isBSeen = false;
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    final String key = rs.getString(1);
                    final long[] expected;
                    if (keyA.equals(key) && !isASeen) {
                        isASeen = true;
                        expected = valuesA;
                    } else if (keyB.equals(key) && !isBSeen) {
                        isBSeen = true;
                        expected = valuesB;
                    } else {
                        throw new AssertionError(sql + " returned unexpected key " + key);
                    }
                    for (int c = 0; c < expected.length; c++) {
                        Assert.assertEquals(sql, expected[c], rs.getLong(c + 2));
                    }
                }
            }
            Assert.assertTrue(sql, isASeen && isBSeen);
        }
    }

    private static void assertRows(
            @Nullable Connection conn,
            @Nullable HttpClient httpClient,
            int fetchSize,
            String sql,
            @Nullable String bindKey,
            long first,
            long step,
            long count
    ) throws Exception {
        if (conn == null) {
            final StringBuilder expected = new StringBuilder("\"dataset\":[");
            for (long i = 0; i < count; i++) {
                if (i > 0) {
                    expected.append(',');
                }
                expected.append('[').append(first + step * i).append(']');
            }
            expected.append("],\"count\":").append(count);
            assertContains(exec(httpClient, sql), expected.toString());
            return;
        }
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            if (bindKey != null) {
                stmt.setString(1, bindKey);
            }
            if (fetchSize > 0) {
                stmt.setFetchSize(fetchSize);
            }
            long row = 0;
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    Assert.assertEquals(sql + " row " + row, first + step * row, rs.getLong(1));
                    row++;
                }
            }
            Assert.assertEquals(sql, count, row);
        }
    }

    private static void createFuzzTable() throws Exception {
        try (
                Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                Statement statement = conn.createStatement()
        ) {
            statement.execute("""
                    CREATE TABLE tab AS (
                        SELECT (x * 34_560_000)::timestamp AS ts, ('k' || (x % 100))::symbol AS key, x
                        FROM long_sequence(500_000)
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
        }
    }

    private static String exec(HttpClient httpClient, String sql) throws Exception {
        final HttpRequest request = HttpRequest.newBuilder(URI.create(
                        "http://127.0.0.1:" + HTTP_PORT + "/exec?query=" + URLEncoder.encode(sql, StandardCharsets.UTF_8)
                ))
                .timeout(Duration.ofSeconds(120))
                .build();
        final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        Assert.assertEquals(sql + " returned " + response.body(), 200, response.statusCode());
        return response.body();
    }

    private static String toJsonRow(String key, long[] values) {
        final StringBuilder row = new StringBuilder("[\"").append(key).append('"');
        for (long value : values) {
            row.append(',').append(value);
        }
        return row.append(']').toString();
    }

    private void testServerMainParallelQueryLoadTest(
            String ddl,
            String ddl2,
            String query,
            String expectedPlan,
            String expectedResult
    ) throws Exception {
        // Here we're verifying that adaptive work stealing doesn't lead to deadlocks.
        try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
            serverMain.start();

            try (Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES)) {
                try (Statement statement = conn.createStatement()) {
                    statement.execute(ddl);
                }

                if (ddl2 != null) {
                    try (Statement statement = conn.createStatement()) {
                        statement.execute(ddl2);
                    }
                }

                try (ResultSet rs = conn.prepareStatement("EXPLAIN " + query).executeQuery()) {
                    sink.clear();
                    assertResultSet(expectedPlan, sink, rs);
                }
            }

            final int nThreads = 6;
            // Fewer iterations on slow CI runners; 6 oversubscribed threads still stress work stealing.
            final int nIterations = Os.isLinux() ? 80 : 30;

            final CyclicBarrier startBarrier = new CyclicBarrier(nThreads);
            final SOCountDownLatch doneLatch = new SOCountDownLatch(nThreads);
            final AtomicInteger errors = new AtomicInteger();
            for (int t = 0; t < nThreads; t++) {
                new Thread(() -> {
                    final StringSink sink = new StringSink();
                    try {
                        startBarrier.await();

                        try (Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES)) {
                            for (int i = 0; i < nIterations; i++) {
                                try (ResultSet rs = conn.prepareStatement(query).executeQuery()) {
                                    sink.clear();
                                    assertResultSet(expectedResult, sink, rs);
                                }
                            }
                        }
                    } catch (Throwable th) {
                        errors.incrementAndGet();
                        th.printStackTrace(System.out);
                    } finally {
                        doneLatch.countDown();
                    }
                }).start();
            }

            doneLatch.await();
            Assert.assertEquals(0, errors.get());
        }
    }
}
