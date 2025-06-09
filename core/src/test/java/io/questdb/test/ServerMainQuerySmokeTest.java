/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.postgresql.util.PSQLException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.test.cutlass.pgwire.BasePGTest.assertResultSet;
import static io.questdb.test.tools.TestUtils.assertContains;
import static io.questdb.test.tools.TestUtils.unchecked;

@RunWith(Parameterized.class)
public class ServerMainQuerySmokeTest extends AbstractBootstrapTest {
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

                final String expected = "count[BIGINT]\n" +
                        "9999999\n";
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

                final String expected = "count[BIGINT]\n" +
                        "10000000\n";
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
    public void testServerMainGlobalQueryCacheSmokeTest() {
        Assume.assumeFalse(convertToParquet);

        // Verify that global cache is correctly synchronized, so that
        // no record cursor factory is used by multiple threads concurrently.
        class TestCase {
            final String expectedResult;
            final String query;

            TestCase(String query, String expectedResult) {
                this.query = query;
                this.expectedResult = expectedResult;
            }
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
                "SELECT * FROM tab WHERE key = 'k3' LIMIT 10",
                "QUERY PLAN[VARCHAR]\n" +
                        "Async Filter workers: 4\n" +
                        "  limit: 10\n" +
                        "  filter: key='k3' [pre-touch]\n" +
                        "    PageFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n",
                "ts[TIMESTAMP],key[VARCHAR],price[DOUBLE],quantity[BIGINT]\n" +
                        "1970-01-01 00:43:12.0,k3,3.0,3\n" +
                        "1970-01-01 01:55:12.0,k3,8.0,8\n" +
                        "1970-01-01 03:07:12.0,k3,13.0,13\n" +
                        "1970-01-01 04:19:12.0,k3,18.0,18\n" +
                        "1970-01-01 05:31:12.0,k3,23.0,23\n" +
                        "1970-01-01 06:43:12.0,k3,28.0,28\n" +
                        "1970-01-01 07:55:12.0,k3,33.0,33\n" +
                        "1970-01-01 09:07:12.0,k3,38.0,38\n" +
                        "1970-01-01 10:19:12.0,k3,43.0,43\n" +
                        "1970-01-01 11:31:12.0,k3,48.0,48\n"
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
                String expected = "count[BIGINT]\n" +
                        "100\n";
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
                String expected = "count_distinct[BIGINT],count_distinct1[BIGINT]\n" +
                        "10000,1000\n";
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
                String expected = "k[VARCHAR],count_distinct[BIGINT]\n" +
                        "k0,10000\n" +
                        "k1,10000\n" +
                        "k2,10000\n";
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
                "QUERY PLAN[VARCHAR]\n" +
                        "Sort light\n" +
                        "  keys: [key desc]\n" +
                        "    GroupBy vectorized: true workers: 4\n" +
                        "      keys: [key]\n" +
                        "      values: [min(quantity),max(quantity)]\n" +
                        "        PageFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: tab\n",
                "key[VARCHAR],min[BIGINT],max[BIGINT]\n" +
                        "k4,4,9999\n" +
                        "k3,3,9998\n" +
                        "k2,2,9997\n" +
                        "k1,1,9996\n" +
                        "k0,5,10000\n"
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
                "QUERY PLAN[VARCHAR]\n" +
                        "GroupBy vectorized: true workers: 4\n" +
                        "  values: [min(quantity),max(quantity)]\n" +
                        "    PageFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n",
                "min[BIGINT],max[BIGINT]\n" +
                        "1,10000\n"
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
                "QUERY PLAN[VARCHAR]\n" +
                        "Sort light lo: 10\n" +
                        "  keys: [day, key]\n" +
                        "    VirtualRecord\n" +
                        "      functions: [day,key,vwap]\n" +
                        "        Async Group By workers: 4\n" +
                        "          keys: [day,key]\n" +
                        "          values: [vwap(price,quantity)]\n" +
                        "          filter: null\n" +
                        "            PageFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: tab\n",
                "day[INTEGER],key[VARCHAR],vwap[DOUBLE]\n" +
                        "1,k0,6624.171717171717\n" +
                        "1,k1,6624.8468153184685\n" +
                        "1,k10,6612.932687914096\n" +
                        "1,k100,6623.496749024707\n" +
                        "1,k11,6613.610770065386\n" +
                        "1,k12,6281.67243296272\n" +
                        "1,k13,6598.965586726309\n" +
                        "1,k14,6599.64534842185\n" +
                        "1,k15,6600.325238210883\n" +
                        "1,k16,6601.005256016568\n"
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
                "QUERY PLAN[VARCHAR]\n" +
                        "Sort light lo: 10\n" +
                        "  keys: [key]\n" +
                        "    Async Group By workers: 4\n" +
                        "      keys: [key]\n" +
                        "      values: [count_distinct(x)]\n" +
                        "      filter: null\n" +
                        "        PageFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: tab\n",
                "key[VARCHAR],count_distinct[BIGINT]\n" +
                        "k0,99\n" +
                        "k1,100\n" +
                        "k10,99\n" +
                        "k100,99\n" +
                        "k11,99\n" +
                        "k12,99\n" +
                        "k13,99\n" +
                        "k14,99\n" +
                        "k15,99\n" +
                        "k16,99\n"
        );
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
            final int nIterations = 80;

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
