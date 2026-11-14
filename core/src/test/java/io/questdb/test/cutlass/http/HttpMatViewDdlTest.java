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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.test.AbstractTest;
import io.questdb.test.QueryAssertion;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.concurrent.TimeUnit;

/**
 * REFRESH MATERIALIZED VIEW ... RESUME and ALTER MATERIALIZED VIEW ... AS over HTTP /exec: a statement that succeeds
 * returns the DDL acknowledgement, a refused one returns the error and position a direct compile of the same statement
 * gives and changes nothing, and one whose statement timeout expires while it waits for the view's latch refuses with
 * "retry later" and changes nothing. The view takes the same statement once the latch is free.
 */
public class HttpMatViewDdlTest extends AbstractTest {
    private static final String DDL_OK = "{\"ddl\":\"OK\"}";
    private static final String VIEW_SQL = "SELECT ts, sym, sum(price) AS price FROM base SAMPLE BY 1h";
    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(10 * 60 * 1000, TimeUnit.MILLISECONDS)
            .withLookingForStuckThread(true)
            .build();

    @Test
    public void testAlterAsBaseChangeTimesOutWaitingForLatch() throws Exception {
        // A base change waits for the view's latch and refuses once the statement timeout expires. The statement is the
        // connection's first: /exec reads the Statement-Timeout header of a pooled connection's first request only
        // (https://github.com/questdb/questdb/issues/4409).
        TestUtils.assertMemoryLeak(() -> getSimpleTester().run((engine, context) -> {
            try (ExecClient client = new ExecClient()) {
                createBaseAndView(engine, context);
                final TableToken viewToken = engine.verifyTableName("mv");
                engine.execute("CREATE TABLE base2 (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL", context);
                engine.execute("INSERT INTO base2 VALUES ('b', 7.0, '2024-01-04T00:10:00.000000Z')", context);
                drainWalAndMatViewQueues(engine);
                final String baseChangeSql = "ALTER MATERIALIZED VIEW mv WITH BASE base2 AS (SELECT ts, sym, sum(price) AS price FROM base2 SAMPLE BY 1h)";
                final long viewSeqTxn = viewSeqTxn(engine, viewToken);
                final MatViewState state = engine.getMatViewStateStore().getViewState(viewToken);
                Assert.assertTrue(state.tryLock());
                try {
                    client.assertExecContains("materialized view is busy, retry later [view=mv]", baseChangeSql, 100);
                } finally {
                    MatViewRefreshJob.finalizeAndUnlock(engine, engine.getMatViewStateStore(), viewToken, state, false);
                }
                Assert.assertEquals(viewSeqTxn, viewSeqTxn(engine, viewToken));
                assertQuery(engine, context, "SELECT view_status, base_table_name FROM materialized_views()")
                        .noRandomAccess()
                        .returns("view_status\tbase_table_name\nvalid\tbase\n");

                // with the latch free, the base change goes through and leaves the view invalid until RESUME
                client.assertExec(DDL_OK, baseChangeSql);
                drainWalAndMatViewQueues(engine);
                assertQuery(engine, context, "SELECT view_status, invalidation_reason, base_table_name FROM materialized_views()")
                        .noRandomAccess()
                        .returns("view_status\tinvalidation_reason\tbase_table_name\ninvalid\tbase table changed\tbase2\n");
                client.assertExec(DDL_OK, "REFRESH MATERIALIZED VIEW mv RESUME SKIP BACKLOG");
                drainWalAndMatViewQueues(engine);
                engine.execute("INSERT INTO base2 VALUES ('b', 8.0, '2024-01-05T00:10:00.000000Z')", context);
                drainWalAndMatViewQueues(engine);
                assertQuery(engine, context, "mv ORDER BY ts")
                        .timestamp("ts")
                        .expectSize()
                        .returns("""
                                ts\tsym\tprice
                                2024-01-01T00:00:00.000000Z\ta\t1.0
                                2024-01-02T00:00:00.000000Z\ta\t2.0
                                2024-01-05T00:00:00.000000Z\tb\t8.0
                                """);
            }
        }));
    }

    @Test
    public void testAlterAsOverExec() throws Exception {
        TestUtils.assertMemoryLeak(() -> getSimpleTester().run((engine, context) -> {
            try (ExecClient client = new ExecClient()) {
                createBaseAndView(engine, context);
                final TableToken viewToken = engine.verifyTableName("mv");
                final String newSql = "SELECT ts, sym, sum(price) * 2 AS price FROM base SAMPLE BY 1h";
                client.assertExec(DDL_OK, "ALTER MATERIALIZED VIEW mv AS (" + newSql + ")");
                drainWalAndMatViewQueues(engine);
                assertQuery(engine, context, "SELECT view_status, view_sql FROM materialized_views()")
                        .noRandomAccess()
                        .returns("view_status\tview_sql\nvalid\t" + newSql + '\n');
                engine.execute("INSERT INTO base VALUES ('a', 5.0, '2024-01-03T00:10:00.000000Z')", context);
                drainWalAndMatViewQueues(engine);
                assertQuery(engine, context, "mv ORDER BY ts")
                        .timestamp("ts")
                        .expectSize()
                        .returns("""
                                ts\tsym\tprice
                                2024-01-01T00:00:00.000000Z\ta\t1.0
                                2024-01-02T00:00:00.000000Z\ta\t2.0
                                2024-01-03T00:00:00.000000Z\ta\t10.0
                                """);

                // the same-position rule refuses a SQL that puts another column at price's position
                final long viewSeqTxn = viewSeqTxn(engine, viewToken);
                final String refusedSql = "ALTER MATERIALIZED VIEW mv AS (SELECT ts, sym, sum(price) AS amount FROM base SAMPLE BY 1h)";
                client.assertExec(expectedError(engine, context, refusedSql, "materialized view column does not match the query column at the same position"), refusedSql);
                Assert.assertEquals(viewSeqTxn, viewSeqTxn(engine, viewToken));
                assertQuery(engine, context, "SELECT view_status, view_sql FROM materialized_views()")
                        .noRandomAccess()
                        .returns("view_status\tview_sql\nvalid\t" + newSql + '\n');
            }
        }));
    }

    @Test
    public void testResumeOverExec() throws Exception {
        TestUtils.assertMemoryLeak(() -> getSimpleTester().run((engine, context) -> {
            try (ExecClient client = new ExecClient()) {
                createBaseAndView(engine, context);
                final TableToken viewToken = engine.verifyTableName("mv");
                engine.getMatViewStateStore().enqueueInvalidate(viewToken, "test invalidation");
                drainWalAndMatViewQueues(engine);
                engine.execute("INSERT INTO base VALUES ('a', 3.0, '2024-01-03T00:10:00.000000Z')", context);
                drainWalAndMatViewQueues(engine);
                assertQuery(engine, context, "SELECT view_status, invalidation_reason FROM materialized_views()")
                        .noRandomAccess()
                        .returns("view_status\tinvalidation_reason\ninvalid\ttest invalidation\n");

                // RESUME waits for the view's latch and refuses once the statement timeout expires. The statement is the
                // connection's first: /exec reads the Statement-Timeout header of a pooled connection's first request
                // only (https://github.com/questdb/questdb/issues/4409).
                final long viewSeqTxn = viewSeqTxn(engine, viewToken);
                final MatViewState state = engine.getMatViewStateStore().getViewState(viewToken);
                Assert.assertTrue(state.tryLock());
                try {
                    client.assertExecContains("materialized view is busy, retry later [view=mv]", "REFRESH MATERIALIZED VIEW mv RESUME", 100);
                } finally {
                    MatViewRefreshJob.finalizeAndUnlock(engine, engine.getMatViewStateStore(), viewToken, state, false);
                }
                Assert.assertEquals(viewSeqTxn, viewSeqTxn(engine, viewToken));
                assertQuery(engine, context, "SELECT view_status, invalidation_reason FROM materialized_views()")
                        .noRandomAccess()
                        .returns("view_status\tinvalidation_reason\ninvalid\ttest invalidation\n");

                // with the latch free, RESUME catches the view up on the row it missed
                client.assertExec(DDL_OK, "REFRESH MATERIALIZED VIEW mv RESUME");
                drainWalAndMatViewQueues(engine);
                assertQuery(engine, context, "SELECT view_status, invalidation_reason FROM materialized_views()")
                        .noRandomAccess()
                        .returns("view_status\tinvalidation_reason\nvalid\t\n");
                assertQuery(engine, context, "mv ORDER BY ts")
                        .timestamp("ts")
                        .expectSize()
                        .returns("""
                                ts\tsym\tprice
                                2024-01-01T00:00:00.000000Z\ta\t1.0
                                2024-01-02T00:00:00.000000Z\ta\t2.0
                                2024-01-03T00:00:00.000000Z\ta\t3.0
                                """);

                // a replaced base loses the backlog, which plain RESUME refuses and RESUME SKIP BACKLOG drops
                engine.execute("DROP TABLE base", context);
                engine.execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL", context);
                engine.execute("INSERT INTO base VALUES ('b', 4.0, '2024-01-04T00:10:00.000000Z')", context);
                drainWalAndMatViewQueues(engine);
                final String resumeSql = "REFRESH MATERIALIZED VIEW mv RESUME";
                final long invalidSeqTxn = viewSeqTxn(engine, viewToken);
                client.assertExec(expectedError(engine, context, resumeSql, "materialized view backlog is lost [reason=base table replaced]"), resumeSql);
                Assert.assertEquals(invalidSeqTxn, viewSeqTxn(engine, viewToken));
                client.assertExec(DDL_OK, "REFRESH MATERIALIZED VIEW mv RESUME SKIP BACKLOG");
                drainWalAndMatViewQueues(engine);
                assertQuery(engine, context, "SELECT view_status, invalidation_reason FROM materialized_views()")
                        .noRandomAccess()
                        .returns("view_status\tinvalidation_reason\nvalid\t\n");
                engine.execute("INSERT INTO base VALUES ('b', 5.0, '2024-01-05T00:10:00.000000Z')", context);
                drainWalAndMatViewQueues(engine);
                assertQuery(engine, context, "mv WHERE ts >= '2024-01-04' ORDER BY ts")
                        .timestamp("ts")
                        .returns("""
                                ts\tsym\tprice
                                2024-01-05T00:00:00.000000Z\tb\t5.0
                                """);
            }
        }));
    }

    // The test server harness already wraps each test in assertMemoryLeak().
    private static QueryAssertion assertQuery(CairoEngine engine, SqlExecutionContext context, String sql) {
        return new QueryAssertion(engine, context, () -> {
        }, sql).noLeakCheck();
    }

    // One base row a day for two days, and an IMMEDIATE view of hourly sums refreshed from them.
    private static void createBaseAndView(CairoEngine engine, SqlExecutionContext context) throws SqlException {
        // the test server's engine skips the startup load, which creates the materialized view state store
        engine.load();
        engine.execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL", context);
        engine.execute("""
                INSERT INTO base VALUES
                ('a', 1.0, '2024-01-01T00:10:00.000000Z'),
                ('a', 2.0, '2024-01-02T00:10:00.000000Z')
                """, context);
        drainWalAndMatViewQueues(engine);
        engine.execute("CREATE MATERIALIZED VIEW mv AS (" + VIEW_SQL + ") PARTITION BY DAY", context);
        drainWalAndMatViewQueues(engine);
    }

    // The JSON error /exec returns for the statement: the message and the position a direct compile gives.
    private static String expectedError(CairoEngine engine, SqlExecutionContext context, String sql, String messagePrefix) {
        try {
            engine.execute(sql, context);
            Assert.fail("a direct compile must refuse: " + sql);
            return null;
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), messagePrefix);
            return "{\"query\":\"" + sql + "\",\"error\":\"" + e.getFlyweightMessage() + "\",\"position\":" + e.getPosition() + '}';
        }
    }

    private static long viewSeqTxn(CairoEngine engine, TableToken viewToken) {
        return engine.getTableSequencerAPI().lastTxn(viewToken);
    }

    // Sends statements to /exec, with or without a Statement-Timeout header.
    private static class ExecClient extends TestHttpClient {
        private final Utf8StringSink response = new Utf8StringSink();

        void assertExec(String expected, String sql) {
            exec(sql, 0);
            TestUtils.assertEquals(expected, response.toString());
        }

        void assertExecContains(String expected, String sql, long timeoutMillis) {
            exec(sql, timeoutMillis);
            TestUtils.assertContains(response.toString(), "\"error\":\"" + expected);
        }

        private void exec(String sql, long timeoutMillis) {
            try {
                final HttpClient.Request request = getHttpClient().newRequest("localhost", 9001);
                request.GET().url("/exec").query("query", sql);
                if (timeoutMillis > 0) {
                    request.header("Statement-Timeout", Long.toString(timeoutMillis));
                }
                reqToSink0(request, response, null, null, null);
            } finally {
                disconnect();
            }
        }
    }
}
