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
import io.questdb.jit.JitUtil;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.test.cutlass.pgwire.BasePGTest.assertResultSet;
import static io.questdb.test.tools.TestUtils.unchecked;

/**
 * E2E work stealing stress test.
 */
public class ServerMainWorkStealingTest extends AbstractBootstrapTest {
    private static final long FUZZ_HANG_ROUND_NANOS = 5_000_000_000L;
    private static final int FUZZ_KEY_COUNT = 100;
    private static final int FUZZ_QUERIES_PER_CLIENT = 10;
    private static final long FUZZ_ROW_COUNT = 500_000;

    @Before
    public void setUp() {
        super.setUp();
        unchecked(() -> createDummyConfiguration(
                // Force enable parallel GROUP BY and filter.
                PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED + "=true",
                PropertyKey.CAIRO_SQL_PARALLEL_FILTER_ENABLED + "=true",
                // Use only single worker thread to maximize chances of work stealing.
                PropertyKey.SHARED_WORKER_COUNT + "=1",
                PropertyKey.PG_WORKER_COUNT + "=8",
                PropertyKey.PG_SELECT_CACHE_ENABLED + "=true",
                // Maximize contention over the reduce queue.
                PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT + "=1",
                PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY + "=32",
                // Make the only shared worker sleepy.
                PropertyKey.SHARED_WORKER_YIELD_THRESHOLD + "=1",
                PropertyKey.SHARED_WORKER_NAP_THRESHOLD + "=2",
                PropertyKey.SHARED_WORKER_SLEEP_THRESHOLD + "=3",
                PropertyKey.CIRCUIT_BREAKER_THROTTLE + "=5",
                PropertyKey.QUERY_TIMEOUT + "=150000"
        ));
        dbPath.parent().$();
    }

    @Test
    public void testBindVariableFilterWithBatchedFetch() throws Exception {
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
    public void testParallelQueryFuzzOverPgWireAndHttp() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final int roundCount = Boolean.getBoolean("questdb.fuzz.nightly") ? 20 : 2;
        for (int round = 0; round < roundCount; round++) {
            final boolean isHangShape = round == 0;
            final boolean isPgFiberEnabled = isHangShape || rnd.nextBoolean();
            final boolean isHttpFiberEnabled = isHangShape || rnd.nextBoolean();
            final boolean isQueryFiberEnabled = isPgFiberEnabled || isHttpFiberEnabled || rnd.nextBoolean();
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
    public void testTwoFilteredScansOverHttp() throws Exception {
        final String joinQuery = "SELECT t.k FROM t JOIN s ON s.k = t.k WHERE t.vt IS NULL AND s.ct = 'u'";
        final String unionAllQuery = "SELECT sum(k) FROM (SELECT k FROM s WHERE ct = 'u' UNION ALL SELECT k FROM t WHERE vt IS NULL)";
        assertMemoryLeak(() -> {
            try (ServerMain serverMain = new ServerMain(getServerMainArgs())) {
                serverMain.start();

                try (
                        Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                        Statement statement = conn.createStatement()
                ) {
                    statement.execute("""
                            CREATE TABLE t AS (
                                SELECT x AS k, NULL::TIMESTAMP AS vt, timestamp_sequence(0, 86_400_000_000) AS ts
                                FROM long_sequence(40)
                            ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                            """);
                    statement.execute("CREATE TABLE s AS (SELECT k, 'u' AS ct FROM t WHERE k % 10 = 7)");
                    assertPlan(statement, joinQuery, """
                            QUERY PLAN[VARCHAR]
                            SelectedRecord
                                Hash Join Light
                                  condition: s.k=t.k
                                    Async JIT Filter workers: 1
                                      filter: null=vt
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                                    Hash
                                        Async JIT Filter workers: 1
                                          filter: ct=u
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: s
                            """);
                    assertPlan(statement, unionAllQuery, """
                            QUERY PLAN[VARCHAR]
                            GroupBy vectorized: false
                              values: [sum(k)]
                                Union All
                                    SelectedRecord
                                        Async JIT Filter workers: 1
                                          filter: ct=u
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: s
                                    SelectedRecord
                                        Async JIT Filter workers: 1
                                          filter: null=vt
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: t
                            """);
                }

                assertExecReturns(joinQuery, "\"dataset\":[[7],[17],[27],[37]],\"count\":4");
                assertExecReturns(unionAllQuery, "\"dataset\":[[908]],\"count\":1");
            }
        });
    }

    @Test
    public void testWorkStealing() throws Exception {
        try (ServerMain serverMain = new ServerMain(getServerMainArgs())) {
            serverMain.start();

            try (Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES)) {
                try (Statement statement = conn.createStatement()) {
                    statement.execute(
                            "CREATE TABLE tab as ( " +
                                    "  select (x * 86400000)::timestamp ts, ('k' || (x % 5))::symbol key, x::double price, x::long quantity " +
                                    "  from long_sequence(10000) " +
                                    ") timestamp (ts) PARTITION BY DAY;"
                    );
                }
            }

            final int nThreads = 8;
            final int nIterations = 50;

            final String query = "SELECT * FROM tab WHERE key = 'k3' LIMIT 10;";
            final String expectedResult = """
                    ts[TIMESTAMP],key[VARCHAR],price[DOUBLE],quantity[BIGINT]
                    1970-01-01 00:04:19.2,k3,3.0,3
                    1970-01-01 00:11:31.2,k3,8.0,8
                    1970-01-01 00:18:43.2,k3,13.0,13
                    1970-01-01 00:25:55.2,k3,18.0,18
                    1970-01-01 00:33:07.2,k3,23.0,23
                    1970-01-01 00:40:19.2,k3,28.0,28
                    1970-01-01 00:47:31.2,k3,33.0,33
                    1970-01-01 00:54:43.2,k3,38.0,38
                    1970-01-01 01:01:55.2,k3,43.0,43
                    1970-01-01 01:09:07.2,k3,48.0,48
                    """;

            final CyclicBarrier startBarrier = new CyclicBarrier(nThreads);
            final SOCountDownLatch doneLatch = new SOCountDownLatch(nThreads);
            final AtomicInteger errors = new AtomicInteger();
            for (int t = 0; t < nThreads; t++) {
                new Thread(() -> {
                    final StringSink sink = new StringSink();
                    try {
                        startBarrier.await();

                        for (int i = 0; i < nIterations; i++) {
                            try (
                                    Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                                    PreparedStatement stmt = conn.prepareStatement(query);
                                    ResultSet rs = stmt.executeQuery()
                            ) {
                                sink.clear();
                                assertResultSet(expectedResult, sink, rs);
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

    private static void assertExecReturns(String query, String expectedResult) throws Exception {
        final HttpRequest request = HttpRequest.newBuilder(URI.create(
                        "http://127.0.0.1:" + HTTP_PORT + "/exec?query=" + URLEncoder.encode(query, StandardCharsets.UTF_8)
                ))
                .timeout(Duration.ofSeconds(10))
                .build();
        final HttpClient client = HttpClient.newHttpClient();
        for (int i = 0; i < 3; i++) {
            final HttpResponse<String> response;
            try {
                response = client.send(request, HttpResponse.BodyHandlers.ofString());
            } catch (HttpTimeoutException e) {
                throw new AssertionError("query stalled [run=" + i + ", query=" + query + ']', e);
            }
            Assert.assertEquals(200, response.statusCode());
            TestUtils.assertContains(response.body(), expectedResult);
        }
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
            case 1 -> assertRows(conn, httpClient, fetchSize, keyFilter + " LIMIT " + limit, null, key, FUZZ_KEY_COUNT, limitedCount);
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
            case 3 -> assertRows(conn, httpClient, fetchSize, keyFilter + " ORDER BY ts DESC LIMIT " + limit, null, lastMatch, -FUZZ_KEY_COUNT, limitedCount);
            case 4 -> assertRows(conn, httpClient, fetchSize, keyFilter + " ORDER BY x DESC LIMIT " + limit, null, lastMatch, -FUZZ_KEY_COUNT, limitedCount);
            case 5 -> {
                final long lo = rnd.nextInt((int) (FUZZ_ROW_COUNT - 2_000));
                final long hi = lo + 1 + rnd.nextInt(2_000);
                assertRows(conn, httpClient, fetchSize, "SELECT x FROM tab WHERE x > " + lo + " AND x <= " + hi, null, lo + 1, 1, hi - lo);
            }
            case 6 -> assertRows(conn, httpClient, fetchSize, "SELECT count() FROM tab WHERE key = 'k" + key + "'", null, matchCount, 0, 1);
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

    private static void assertPlan(Statement statement, String query, String expectedPlan) throws Exception {
        try (ResultSet rs = statement.executeQuery("EXPLAIN " + query)) {
            assertResultSet(
                    JitUtil.isJitSupported() ? expectedPlan : expectedPlan.replace("Async JIT", "Async"),
                    new StringSink(),
                    rs
            );
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
            TestUtils.assertContains(exec(httpClient, sql), expected.toString());
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
}
