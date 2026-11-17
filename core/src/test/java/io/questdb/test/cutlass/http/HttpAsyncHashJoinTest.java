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

package io.questdb.test.cutlass.http;

import io.questdb.Metrics;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.http.client.Fragment;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.test.TestFaultFunctionFactory;
import io.questdb.griffin.engine.functions.test.TestLatchedCounterFunctionFactory;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.test.AbstractTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Drives the parallel hash join through the HTTP endpoints: /exec pages with {@code limit=lo,hi}
 * and {@code count=true}, which counts the rows after a page with {@code calculateSize()}; a send
 * buffer so small that every response resumes once the client read the last chunk; /exp CSV and
 * Parquet exports of a star; {@code CANCEL QUERY} from another connection while the build or the
 * probe is latched; a client that drops its connection mid-response; and one cached factory over
 * requests while INSERTs flip the first join's swap rule. Every response equals the one that a
 * server whose engine runs the ordinary plan returns for the same request.
 */
public class HttpAsyncHashJoinTest extends AbstractTest {
    // [query, query parameters as name=value pairs separated by '&', or empty]
    private static final String[][] PAGED_REQUESTS = {
            // A stack of an INNER and a LEFT join and a SYMBOL key.
            {"SELECT f.k, f.v, d1.w, d2.w2, d3.s3 FROM f LEFT JOIN d1 ON f.k = d1.k JOIN d2 ON f.l = d2.l LEFT JOIN d3 ON f.sym = d3.sym", ""},
            {"SELECT f.k, f.v, d1.w, d2.w2, d3.s3 FROM f LEFT JOIN d1 ON f.k = d1.k JOIN d2 ON f.l = d2.l LEFT JOIN d3 ON f.sym = d3.sym", "limit=1,7"},
            {"SELECT f.k, f.v, d1.w, d2.w2, d3.s3 FROM f LEFT JOIN d1 ON f.k = d1.k JOIN d2 ON f.l = d2.l LEFT JOIN d3 ON f.sym = d3.sym", "limit=5,20&count=true"},
            {"SELECT f.k, f.v, d1.w, d2.w2, d3.s3 FROM f LEFT JOIN d1 ON f.k = d1.k JOIN d2 ON f.l = d2.l LEFT JOIN d3 ON f.sym = d3.sym", "limit=33_000,40_000&count=true"},
            // A swapped stack: d1 is smaller than f, and the later key reads d1's column.
            {"SELECT d1.w, f.v, d2.w2 FROM d1 JOIN f ON d1.k = f.k LEFT JOIN d2 ON d1.l = d2.l", "limit=1,7&count=true"},
            {"SELECT d1.w, f.v, d2.w2 FROM d1 JOIN f ON d1.k = f.k LEFT JOIN d2 ON d1.l = d2.l", "count=true"},
            // A residual that leaves the rest of every frame with fan-out to continuations.
            {"SELECT f.k, f.v, d1.w FROM f LEFT JOIN d1 ON f.k = d1.k AND d1.w > f.v % 50", "limit=3,9&count=true"},
            {"SELECT f.k, f.v, d1.w FROM f LEFT JOIN d1 ON f.k = d1.k AND d1.w > f.v % 50", "limit=13_000,14_000"},
            // The chains of a hot key, which the consumer expands, under a page and a count.
            {"SELECT f.v, d1.w, hot.hw FROM f JOIN hot ON f.h = hot.h LEFT JOIN d1 ON f.k = d1.k", "limit=10,30&count=true"},
            // A LIMIT and a negative LIMIT of the query, under a page of the response.
            {"SELECT f.k, f.v, d1.w, d2.w2 FROM f LEFT JOIN d1 ON f.k = d1.k JOIN d2 ON f.l = d2.l LIMIT 100", "limit=90,120&count=true"},
            {"SELECT f.k, f.v, d1.w, d2.w2 FROM f LEFT JOIN d1 ON f.k = d1.k JOIN d2 ON f.l = d2.l LIMIT -50", "limit=1,10&count=true"},
            // A post-filtered star in descending order.
            {"SELECT f.k, f.v, d1.w, d3.s3 FROM f LEFT JOIN d1 ON f.k = d1.k LEFT JOIN d3 ON f.sym = d3.sym WHERE d1.w > f.v % 7 OR d1.w IS NULL ORDER BY f.ts DESC", "limit=2,8&count=true"},
            // A many-to-many INNER join over d1's ten rows per key, counted after a page, as the web
            // console asks, and under a window function, whose cursor hands the count to the join.
            {"SELECT f.v, d1.w FROM f JOIN d1 ON f.k = d1.k", "limit=0,1000&count=true"},
            {"SELECT f.v, d1.w, row_number() OVER () rn FROM f JOIN d1 ON f.k = d1.k", "limit=1,10&count=true"},
    };
    private static final String STAR = PAGED_REQUESTS[0][0];

    @After
    public void resetLatch() {
        TestLatchedCounterFunctionFactory.reset(null);
    }

    @Test
    public void testCachedFactoryFollowsTheSwapRuleAcrossRequests() throws Exception {
        final String sql = "SELECT d.w, f.v FROM d JOIN f ON d.k = f.k";
        // The INSERTs make d, which starts smaller than f, larger than f, and then smaller again.
        final String[] inserts = {
                "INSERT INTO d SELECT (x % 7)::INT, x + 1_000 FROM long_sequence(300)",
                "INSERT INTO f SELECT (x % 9)::INT, x + 1_000 FROM long_sequence(600)",
        };
        final ObjList<String> expected = new ObjList<>();
        final ObjList<String> actual = new ObjList<>();
        for (boolean isParallel : new boolean[]{false, true}) {
            final ObjList<String> responses = isParallel ? actual : expected;
            newTester().run(newConfiguration(isParallel, 0), (engine, context) -> {
                engine.execute("DROP TABLE IF EXISTS f");
                engine.execute("DROP TABLE IF EXISTS d");
                engine.execute("CREATE TABLE f AS (SELECT (x % 9)::INT k, x v FROM long_sequence(200))");
                engine.execute("CREATE TABLE d AS (SELECT (x % 7)::INT k, x w FROM long_sequence(100))");
                try (TestHttpClient client = new TestHttpClient()) {
                    // One connection, so each request finds the factory that the request before it
                    // returned to the select cache.
                    client.setKeepConnection(true);
                    responses.add(get(client, "/exec", sql, ""));
                    for (String insert : inserts) {
                        engine.execute(insert);
                        responses.add(get(client, "/exec", sql, ""));
                        responses.add(get(client, "/exec", sql, "limit=3,8&count=true"));
                    }
                    final String plan = get(client, "/exec", "EXPLAIN " + sql, "");
                    Assert.assertEquals(plan, isParallel, plan.contains("Async Hash Join workers:"));
                    Assert.assertEquals(plan, isParallel, plan.contains("inputSwap:"));
                }
            });
        }
        assertResponses(expected, actual);
    }

    @Test
    public void testCancelQueryWhileBuildIsLatched() throws Exception {
        assertCancelQuery("SELECT f.k, f.v, d1.w FROM f LEFT JOIN (SELECT * FROM d1 WHERE test_latched_counter()) d1 ON f.k = d1.k", 200);
    }

    @Test
    public void testCancelQueryWhileProbeIsLatched() throws Exception {
        assertCancelQuery("SELECT f.k, f.v, d1.w, d2.w2 FROM (SELECT * FROM f WHERE test_latched_counter()) f LEFT JOIN d1 ON f.k = d1.k JOIN d2 ON f.l = d2.l", 700);
    }

    @Test
    public void testClientDisconnectsMidResponse() throws Exception {
        final String[] expected = new String[1];
        newTester().run(newConfiguration(false, 0), (engine, context) -> {
            createTables(engine);
            try (TestHttpClient client = new TestHttpClient()) {
                expected[0] = get(client, "/exec", STAR, "");
            }
        });
        newTester().withSendBufferSize(512).run(newConfiguration(true, 0), (engine, context) -> {
            try (HttpClient httpClient = io.questdb.cutlass.http.client.HttpClientFactory.newPlainTextInstance()) {
                for (int run = 0; run < 3; run++) {
                    final HttpClient.Request request = httpClient.newRequest("localhost", 9001);
                    request.GET().url("/exec").query("query", STAR);
                    final HttpClient.ResponseHeaders response = request.send();
                    response.await();
                    // Reads a chunk or two of the response, while the server's cursor holds the rest.
                    final Fragment fragment = response.getResponse().recv();
                    Assert.assertNotNull(fragment);
                    httpClient.disconnect();
                }
            }
            // The server closes the dropped connections' cursors, which release their readers.
            TestUtils.assertEventually(() -> Assert.assertEquals(0, engine.getBusyReaderCount()));
            try (TestHttpClient client = new TestHttpClient()) {
                Assert.assertEquals(expected[0], get(client, "/exec", STAR, ""));
            }
        });
    }

    @Test
    public void testExecPagesAndCountsWithSmallSendBuffer() throws Exception {
        assertPagedRequests(512);
    }

    @Test
    public void testExecPagesAndCounts() throws Exception {
        assertPagedRequests(-1);
    }

    @Test
    public void testExecPagesWithoutCountBoundTheFirstWindow() throws Exception {
        // [query, query parameters, the rows that the join's first window was cut for]. A page
        // without a row count reads the rows up to its last one and one more; a count, no page or
        // a join that is not the query's rows leaves the window as it is.
        final Object[][] requests = {
                {STAR, "limit=1,7", 8L},
                {STAR, "limit=1,7&count=true", Long.MAX_VALUE},
                {STAR, "", Long.MAX_VALUE},
                {STAR, "limit=7", 8L},
                {STAR, "limit=0", 2L},
                {STAR, "limit=3_000,3_010", 3_011L},
                // A page that starts past its end reads the rows it skips, its first row and one more.
                {STAR, "limit=21,5", 22L},
                {STAR, "limit=9223372036854775807,9223372036854775806", Long.MAX_VALUE},
                {STAR + " ORDER BY f.v DESC", "limit=1,7", Long.MAX_VALUE},
                {"SELECT * FROM (" + STAR + ") LIMIT 50", "limit=1,7", Long.MAX_VALUE},
                {STAR, "limit=2,4", 5L},
        };
        final ObjList<String> expected = new ObjList<>();
        final ObjList<String> actual = new ObjList<>();
        for (boolean isParallel : new boolean[]{false, true}) {
            final ObjList<String> responses = isParallel ? actual : expected;
            newTester().run(newConfiguration(isParallel, 0), (engine, context) -> withQueryPool(engine, () -> {
                createTables(engine);
                try (TestHttpClient client = new TestHttpClient()) {
                    // One connection, whose context serves every request.
                    client.setKeepConnection(true);
                    for (Object[] request : requests) {
                        AsyncHashJoinRecordCursorFactory.takeLastFirstWindowRowsForTesting();
                        responses.add(get(client, "/exec", (String) request[0], (String) request[1]));
                        final long firstWindowRows = AsyncHashJoinRecordCursorFactory.takeLastFirstWindowRowsForTesting();
                        Assert.assertEquals(request[0] + "&" + request[1], isParallel ? (long) request[2] : -1L, firstWindowRows);
                    }
                    // A page whose query fails to open its cursor after the join opened its own: the
                    // join took the page's rows, and the next request on the connection, without a
                    // page, finds no trace of them.
                    final String failing = "SELECT f.k, d1.w, test_fault() x FROM f JOIN d1 ON f.k = d1.k";
                    TestFaultFunctionFactory.armToFailAfterInits(0);
                    try {
                        TestUtils.assertContains(get(client, "/exec", failing, "limit=5", null), "test_fault: injected init failure");
                    } finally {
                        TestFaultFunctionFactory.disarm();
                    }
                    Assert.assertEquals(isParallel ? 6L : -1L, AsyncHashJoinRecordCursorFactory.takeLastFirstWindowRowsForTesting());
                    responses.add(get(client, "/exec", failing, ""));
                    Assert.assertEquals(isParallel ? Long.MAX_VALUE : -1L, AsyncHashJoinRecordCursorFactory.takeLastFirstWindowRowsForTesting());
                    final String plan = get(client, "/exec", "EXPLAIN " + STAR, "");
                    Assert.assertEquals(plan, isParallel, plan.contains("Hash Join workers:"));
                }
            }));
        }
        assertResponses(expected, actual);
    }

    @Test
    public void testExportCsvAndParquet() throws Exception {
        final ObjList<String> expected = new ObjList<>();
        final ObjList<String> actual = new ObjList<>();
        for (boolean isParallel : new boolean[]{false, true}) {
            final ObjList<String> responses = isParallel ? actual : expected;
            newTester().run(newConfiguration(isParallel, 1), (engine, context) -> {
                createTables(engine);
                try (TestHttpClient client = new TestHttpClient()) {
                    client.setKeepConnection(true);
                    responses.add(get(client, "/exp", STAR, ""));
                    responses.add(get(client, "/exp", PAGED_REQUESTS[4][0], ""));
                    responses.add(get(client, "/exp", PAGED_REQUESTS[6][0], "limit=3,90"));
                    // A Parquet export of the star, compared byte for byte.
                    final Utf8StringSink parquet = new Utf8StringSink();
                    client.toSink0("/exp", STAR, parquet, null, null, null, params("fmt=parquet"), "200");
                    Assert.assertTrue(parquet.size() > 1_000);
                    final byte[] bytes = new byte[parquet.size()];
                    for (int i = 0; i < bytes.length; i++) {
                        bytes[i] = parquet.byteAt(i);
                    }
                    Assert.assertEquals("PAR1", new String(bytes, 0, 4, java.nio.charset.StandardCharsets.US_ASCII));
                    responses.add(java.util.HexFormat.of().formatHex(bytes));
                }
            });
        }
        assertResponses(expected, actual);
    }

    private static void assertResponses(ObjList<String> expected, ObjList<String> actual) {
        Assert.assertEquals(expected.size(), actual.size());
        for (int i = 0, n = expected.size(); i < n; i++) {
            TestUtils.assertEquals("response " + i, expected.getQuick(i), actual.getQuick(i));
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(60, TimeUnit.SECONDS)) {
                throw new AssertionError("latch timed out");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    // f holds 6_000 rows over five days, with keys 0 to 96 and its first two rows on the hot key
    // that every row of hot holds; d1 holds 900 rows, d2 120 and d3 30.
    private static void createTables(CairoEngine engine) throws Exception {
        engine.execute("DROP TABLE IF EXISTS f");
        engine.execute("DROP TABLE IF EXISTS d1");
        engine.execute("DROP TABLE IF EXISTS d2");
        engine.execute("DROP TABLE IF EXISTS d3");
        engine.execute("DROP TABLE IF EXISTS hot");
        engine.execute("CREATE TABLE f AS (SELECT (x % 97)::INT k, x v, (x % 41)::LONG l, ('s' || (x % 13))::SYMBOL sym, " +
                "CASE WHEN x <= 2 THEN 0 ELSE x::INT END h, timestamp_sequence('2024-01-01', 60_000_000) ts FROM long_sequence(6_000)) TIMESTAMP(ts) PARTITION BY DAY");
        engine.execute("CREATE TABLE d1 AS (SELECT (x % 90)::INT k, x w, (x % 41)::LONG l FROM long_sequence(900))");
        engine.execute("CREATE TABLE d2 AS (SELECT (x % 41)::LONG l, x w2 FROM long_sequence(120))");
        engine.execute("CREATE TABLE d3 AS (SELECT ('s' || (x % 11))::SYMBOL sym, 'v' || x s3 FROM long_sequence(30))");
        engine.execute("CREATE TABLE hot AS (SELECT 0 h, x hw FROM long_sequence(300))");
        engine.execute("ALTER TABLE f CONVERT PARTITION TO PARQUET LIST '2024-01-02'");
    }

    private static String get(TestHttpClient client, String url, String sql, String params) {
        return get(client, url, sql, params, "200");
    }

    private static String get(TestHttpClient client, String url, String sql, String params, @Nullable String status) {
        final Utf8StringSink sink = new Utf8StringSink();
        client.toSink0(url, sql, sink, null, null, null, params(params), status);
        return sink.toString();
    }

    // The query parameters of name=value pairs separated by '&', whose values may group digits with
    // underscores.
    private static CharSequenceObjHashMap<String> params(String params) {
        final CharSequenceObjHashMap<String> queryParams = new CharSequenceObjHashMap<>();
        if (!params.isEmpty()) {
            for (String param : params.split("&")) {
                final int eq = param.indexOf('=');
                queryParams.put(param.substring(0, eq), param.substring(eq + 1).replace("_", ""));
            }
        }
        return queryParams;
    }

    // The engine's configuration: frames of 16 to 64 rows, the parallel hash join on or off, and the
    // given cap of tuples per probe task, 0 for the default.
    private static DefaultTestCairoConfiguration newConfiguration(boolean isParallel, int taskOutputMaxRows) {
        return new DefaultTestCairoConfiguration(root) {
            @Override
            public int getSqlPageFrameMaxRows() {
                return 64;
            }

            @Override
            public int getSqlPageFrameMinRows() {
                return 16;
            }

            @Override
            public int getSqlParallelHashJoinTaskOutputMaxRows() {
                return taskOutputMaxRows > 0 ? taskOutputMaxRows : super.getSqlParallelHashJoinTaskOutputMaxRows();
            }

            @Override
            public boolean isSqlParallelHashJoinEnabled() {
                return isParallel;
            }

            @Override
            public boolean mangleTableDirNames() {
                return false;
            }
        };
    }

    // A server with two network workers, so that one serves a request while the other's query waits.
    private static HttpQueryTestBuilder newTester() {
        return getSimpleTester().withWorkerCount(2);
    }

    private void assertCancelQuery(String sql, int latchCount) throws Exception {
        final String[] expected = new String[1];
        newTester().run(newConfiguration(false, 0), (engine, context) -> {
            createTables(engine);
            try (TestHttpClient client = new TestHttpClient()) {
                expected[0] = get(client, "/exec", sql, "");
            }
        });
        final AtomicBoolean isLatched = new AtomicBoolean();
        final CountDownLatch reached = new CountDownLatch(1);
        final CountDownLatch cancelled = new CountDownLatch(1);
        TestLatchedCounterFunctionFactory.reset(new TestLatchedCounterFunctionFactory.Callback() {
            @Override
            public boolean onGet(Record rec, int count) {
                // Holds the execution once, until the cancellation went out.
                if (count >= latchCount && isLatched.compareAndSet(false, true)) {
                    reached.countDown();
                    await(cancelled);
                }
                return true;
            }
        });
        newTester().run(newConfiguration(true, 0), (engine, context) -> withQueryPool(engine, () -> {
            final AtomicReference<String> response = new AtomicReference<>();
            final AtomicReference<Throwable> error = new AtomicReference<>();
            final Thread requester = new Thread(() -> {
                try (TestHttpClient client = new TestHttpClient()) {
                    response.set(get(client, "/exec", sql, "", null));
                } catch (Throwable th) {
                    error.set(th);
                } finally {
                    reached.countDown();
                }
            }, "http-requester");
            requester.start();
            try {
                await(reached);
                Assert.assertNull(error.get());
                final long queryId = queryId(engine, context, sql);
                try (TestHttpClient client = new TestHttpClient()) {
                    final String cancel = get(client, "/exec", "CANCEL QUERY " + queryId, "");
                    TestUtils.assertContains(cancel, "\"ddl\":\"OK\"");
                }
            } finally {
                cancelled.countDown();
                requester.join();
            }
            Assert.assertNull(error.get());
            TestUtils.assertContains(response.get(), "cancelled by user");
            TestUtils.assertEventually(() -> Assert.assertEquals(0, engine.getBusyReaderCount()));
            // The next request runs the operator, and returns the ordinary plan's rows.
            try (TestHttpClient client = new TestHttpClient()) {
                final String plan = get(client, "/exec", "EXPLAIN " + sql, "");
                Assert.assertTrue(plan, plan.contains("Async Hash Join workers:"));
                Assert.assertEquals(expected[0], get(client, "/exec", sql, ""));
            }
        }));
    }

    private void assertPagedRequests(int sendBufferSize) throws Exception {
        final ObjList<String> expected = new ObjList<>();
        final ObjList<String> actual = new ObjList<>();
        for (boolean isParallel : new boolean[]{false, true}) {
            final ObjList<String> responses = isParallel ? actual : expected;
            final HttpQueryTestBuilder tester = newTester();
            if (isParallel && sendBufferSize > 0) {
                tester.withSendBufferSize(sendBufferSize);
            }
            // A cap of one tuple per task leaves the rest of the frames with fan-out to continuations.
            tester.run(newConfiguration(isParallel, 1), (engine, context) -> withQueryPool(engine, () -> {
                createTables(engine);
                try (TestHttpClient client = new TestHttpClient()) {
                    // One connection, so that the requests after the first of a query reuse its
                    // cached factory.
                    client.setKeepConnection(true);
                    for (String[] request : PAGED_REQUESTS) {
                        final String plan = get(client, "/exec", "EXPLAIN " + request[0], "");
                        Assert.assertEquals(request[0] + '\n' + plan, isParallel, plan.contains("Hash Join workers:"));
                        responses.add(get(client, "/exec", request[0], request[1]));
                    }
                }
            }));
        }
        assertResponses(expected, actual);
        // The pages hold rows, and the counts count past them.
        TestUtils.assertContains(expected.getQuick(2), "\"count\":");
        TestUtils.assertContains(expected.getQuick(3), "\"dataset\":[[");
    }

    private long queryId(CairoEngine engine, SqlExecutionContext context, String sql) throws Exception {
        try (RecordCursorFactory factory = engine.select("SELECT query_id FROM query_activity() WHERE query = '" + sql.replace("'", "''") + "'", context);
             RecordCursor cursor = factory.getCursor(context)) {
            Assert.assertTrue(cursor.hasNext());
            return cursor.getRecord().getLong(0);
        }
    }

    // Runs the code with a query pool of two workers that runs the operator's tasks next to the owner.
    private void withQueryPool(CairoEngine engine, @Nullable ThrowingRunnable code) throws Exception {
        final TestWorkerPool queryPool = new TestWorkerPool("query", 2, Metrics.DISABLED, WorkerPoolMode.LEGACY);
        try {
            WorkerPoolUtils.setupQueryJobs(queryPool, engine, true);
            queryPool.start(LOG);
            if (code != null) {
                code.run();
            }
        } finally {
            queryPool.halt();
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}
