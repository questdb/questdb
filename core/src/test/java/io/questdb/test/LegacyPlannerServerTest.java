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

import io.questdb.Bootstrap;
import io.questdb.ServerMain;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.wal.QdbrWalLocker;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.griffin.BatchCallback;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.LegacyPlanner;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlCompilerFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.legacy.griffin.LegacyPlannerImpl;
import io.questdb.metrics.Counter;
import io.questdb.std.Chars;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.cutlass.http.TestHttpClient;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class LegacyPlannerServerTest extends AbstractBootstrapTest {
    private static final String[] CACHING = {
            "http.query.cache.enabled=true",
            "pg.select.cache.enabled=true",
            "metrics.enabled=true"
    };
    private static final String INSERT_SQL = "INSERT INTO c VALUES (1)";
    // one HTTP worker, so that QWP egress, whose select cache each worker keeps, serves every query from one cache
    private static final String[] RACING = {
            "http.query.cache.enabled=true",
            "http.worker.count=1",
            "pg.insert.cache.enabled=true",
            "pg.select.cache.enabled=true"
    };
    // the legacy planner projects the join through a SelectedRecord and names the column without its table
    private static final String LEGACY_PLAN = "functions: [x+1]";
    private static final String NEW_PLAN = "functions: [a.x+1]";
    private static final String PLAN_SQL = "EXPLAIN SELECT a.x + 1 FROM a JOIN b ON a.x = b.x";
    private static final String SELECT_SQL = "SELECT x FROM a";

    @Override
    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testHttpExportSelectCacheKeepsPlannersApart() throws Exception {
        createDummyConfiguration(CACHING);
        assertMemoryLeak(() -> {
            try (ServerMain serverMain = startServer(); TestHttpClient client = new TestHttpClient()) {
                client.setKeepConnection(true);
                final Counter hits = serverMain.getEngine().getMetrics().jsonQueryMetrics().cacheHitCounter();
                final long hitsBefore = hits.getValue();
                Assert.assertEquals("\"x\"\r\n1\r\n", httpGet(client, "/exp", SELECT_SQL));
                reloadPlanner(serverMain, true, CACHING);
                httpGet(client, "/exp", SELECT_SQL);
                Assert.assertEquals(0, hits.getValue() - hitsBefore);
                Assert.assertEquals("\"x\"\r\n1\r\n", httpGet(client, "/exp", SELECT_SQL));
                Assert.assertEquals(1, hits.getValue() - hitsBefore);
                reloadPlanner(serverMain, false, CACHING);
                httpGet(client, "/exp", SELECT_SQL);
                Assert.assertEquals(2, hits.getValue() - hitsBefore);
            }
        });
    }

    @Test
    public void testHttpExportRecompileReturnsToCompilingPlanner() throws Exception {
        final String sql = "SELECT x FROM d";
        createDummyConfiguration(RACING);
        assertMemoryLeak(() -> {
            final SwitchingCompilerFactory compilers = new SwitchingCompilerFactory(sql);
            try (ServerMain serverMain = startServer(compilers); TestHttpClient client = new TestHttpClient()) {
                client.setKeepConnection(true);
                serverMain.getEngine().execute("CREATE TABLE d (x INT)");
                serverMain.getEngine().execute("INSERT INTO d VALUES (7)");
                Assert.assertEquals("\"x\"\r\n7\r\n", httpGet(client, "/exp", sql));
                compilers.assertCompiles(1, 0);
                // the cached plan goes stale, and the reload lands between its lookup and the recompile
                serverMain.getEngine().execute("ALTER TABLE d ADD COLUMN y INT");
                compilers.reloadBeforeNextCompile(serverMain, true);
                Assert.assertEquals("\"x\"\r\n7\r\n", httpGet(client, "/exp", sql));
                compilers.assertCompiles(2, 1);
                Assert.assertEquals("\"x\"\r\n7\r\n", httpGet(client, "/exp", sql));
                compilers.assertCompiles(2, 1);
                reloadPlanner(serverMain, false, RACING);
                Assert.assertEquals("\"x\"\r\n7\r\n", httpGet(client, "/exp", sql));
                compilers.assertCompiles(3, 1);
            }
        });
    }

    @Test
    public void testHttpExportReturnsToCompilingPlanner() throws Exception {
        assertReturnsToCompilingPlanner(SELECT_SQL, (serverMain, client) -> Assert.assertEquals("\"x\"\r\n1\r\n", httpGet(client, "/exp", SELECT_SQL)));
    }

    @Test
    public void testHttpSelectCacheKeepsPlannersApart() throws Exception {
        createDummyConfiguration(CACHING);
        assertMemoryLeak(() -> {
            try (ServerMain serverMain = startServer(); TestHttpClient client = new TestHttpClient()) {
                // one connection runs the requests in order, so each returns its factory to the cache before the next
                client.setKeepConnection(true);
                final Counter hits = serverMain.getEngine().getMetrics().jsonQueryMetrics().cacheHitCounter();
                final long hitsBefore = hits.getValue();
                httpGet(client, "/exec", SELECT_SQL);
                reloadPlanner(serverMain, true, CACHING);
                httpGet(client, "/exec", SELECT_SQL);
                Assert.assertEquals(0, hits.getValue() - hitsBefore);
                httpGet(client, "/exec", SELECT_SQL);
                Assert.assertEquals(1, hits.getValue() - hitsBefore);
                reloadPlanner(serverMain, false, CACHING);
                httpGet(client, "/exec", SELECT_SQL);
                Assert.assertEquals(2, hits.getValue() - hitsBefore);
            }
        });
    }

    @Test
    public void testHttpSelectReturnsToCompilingPlanner() throws Exception {
        assertReturnsToCompilingPlanner(SELECT_SQL, (serverMain, client) -> TestUtils.assertContains(httpGet(client, "/exec", SELECT_SQL), "\"dataset\":[[1]]"));
    }

    @Test
    public void testPgInsertReturnsToCompilingPlanner() throws Exception {
        // the insert cache belongs to the connection, so every run goes through one
        assertReturnsToCompilingPlanner(INSERT_SQL, new ServerAction() {
            private Connection conn;

            @Override
            public void close() {
                TestUtils.unchecked(() -> {
                    if (conn != null) {
                        conn.close();
                    }
                });
            }

            @Override
            public void run(ServerMain serverMain, TestHttpClient httpClient) throws Exception {
                if (conn == null) {
                    conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                }
                try (Statement stmt = conn.createStatement()) {
                    Assert.assertEquals(1, stmt.executeUpdate(INSERT_SQL));
                }
            }
        });
    }

    @Test
    public void testPgPreparedStatementOpenAcrossReloadReturnsToItsPlanner() throws Exception {
        createDummyConfiguration(CACHING);
        assertMemoryLeak(() -> {
            try (ServerMain serverMain = startServer()) {
                final Counter hits = serverMain.getEngine().getMetrics().pgWireMetrics().selectCacheHitCounter();
                reloadPlanner(serverMain, true, CACHING);
                try (Connection conn = preparingConnection(); PreparedStatement stmt = conn.prepareStatement(SELECT_SQL)) {
                    assertSelect(stmt);
                    assertSelect(stmt);
                    reloadPlanner(serverMain, false, CACHING);
                    assertSelect(stmt);
                }
                final long hitsBefore = hits.getValue();
                pgSelect();
                Assert.assertEquals(0, hits.getValue() - hitsBefore);
                reloadPlanner(serverMain, true, CACHING);
                pgSelect();
                Assert.assertEquals(1, hits.getValue() - hitsBefore);
            }
        });
    }

    @Test
    public void testPgPreparedStatementSelectCacheKeepsPlannersApart() throws Exception {
        createDummyConfiguration(CACHING);
        assertMemoryLeak(() -> {
            try (ServerMain serverMain = startServer()) {
                final Counter hits = serverMain.getEngine().getMetrics().pgWireMetrics().selectCacheHitCounter();
                reloadPlanner(serverMain, true, CACHING);
                try (Connection conn = preparingConnection(); PreparedStatement stmt = conn.prepareStatement(SELECT_SQL)) {
                    for (int i = 0; i < 3; i++) {
                        assertSelect(stmt);
                    }
                }
                reloadPlanner(serverMain, false, CACHING);
                final long hitsBefore = hits.getValue();
                pgSelect();
                Assert.assertEquals(0, hits.getValue() - hitsBefore);
                reloadPlanner(serverMain, true, CACHING);
                pgSelect();
                Assert.assertEquals(1, hits.getValue() - hitsBefore);
            }
        });
    }

    @Test
    public void testPgSelectCacheKeepsPlannersApart() throws Exception {
        createDummyConfiguration(CACHING);
        assertMemoryLeak(() -> {
            try (ServerMain serverMain = startServer()) {
                // pgwire returns an executed statement to the cache at Sync, before the client sees its result
                final Counter hits = serverMain.getEngine().getMetrics().pgWireMetrics().selectCacheHitCounter();
                final long hitsBefore = hits.getValue();
                pgSelect();
                reloadPlanner(serverMain, true, CACHING);
                pgSelect();
                Assert.assertEquals(0, hits.getValue() - hitsBefore);
                pgSelect();
                Assert.assertEquals(1, hits.getValue() - hitsBefore);
                reloadPlanner(serverMain, false, CACHING);
                pgSelect();
                Assert.assertEquals(2, hits.getValue() - hitsBefore);
            }
        });
    }

    @Test
    public void testPgSelectReturnsToCompilingPlanner() throws Exception {
        assertReturnsToCompilingPlanner(SELECT_SQL, (serverMain, client) -> pgSelect());
    }

    @Test
    public void testPgSimpleQueryReturnsToCompilingPlanner() throws Exception {
        createDummyConfiguration(RACING);
        assertMemoryLeak(() -> {
            final SwitchingCompilerFactory compilers = new SwitchingCompilerFactory(SELECT_SQL);
            try (ServerMain serverMain = startServer(compilers)) {
                // the reload lands between the batch, which compiles with the new planner, and its cache lookup
                compilers.reloadBeforeNextCompile(serverMain, true);
                pgSimpleSelect();
                compilers.assertCompiles(1, 0);
                reloadPlanner(serverMain, false, RACING);
                pgSimpleSelect();
                compilers.assertCompiles(1, 0);
                reloadPlanner(serverMain, true, RACING);
                pgSimpleSelect();
                compilers.assertCompiles(2, 0);
            }
        });
    }

    @Test
    public void testQwpEgressSelectReturnsToCompilingPlanner() throws Exception {
        assertReturnsToCompilingPlanner(SELECT_SQL, new ServerAction() {
            private QwpQueryClient client;

            @Override
            public void close() {
                if (client != null) {
                    client.close();
                }
            }

            @Override
            public void run(ServerMain serverMain, TestHttpClient httpClient) {
                if (client == null) {
                    client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";");
                    client.connect();
                }
                Assert.assertEquals(1, qwpRowCount(client));
            }
        });
    }

    @Test
    public void testReloadSwitchesPlanner() throws Exception {
        assertMemoryLeak(() -> {
            try (ServerMain serverMain = startServer(); TestHttpClient client = new TestHttpClient()) {
                assertPlanners(client, NEW_PLAN);
                reloadPlanner(serverMain, true);
                assertPlanners(client, LEGACY_PLAN);
                reloadPlanner(serverMain, false);
                assertPlanners(client, NEW_PLAN);
            }
        });
    }

    @Test
    public void testServerPropertySelectsPlannerAtStartup() throws Exception {
        createDummyConfiguration("cairo.sql.legacy.planner.enabled=true");
        assertMemoryLeak(() -> {
            try (ServerMain ignored = startServer(); TestHttpClient client = new TestHttpClient()) {
                assertPlanners(client, LEGACY_PLAN);
            }
        });
    }

    // A reload of the switch lands between the cache lookup of the statement, under the new planner's key, and its
    // compile, which the legacy planner then serves. The statement returns to the legacy planner's key, so the next
    // run under the legacy planner reuses it, and a run under the new planner compiles it again.
    private static void assertReturnsToCompilingPlanner(String sql, ServerAction action) throws Exception {
        createDummyConfiguration(RACING);
        assertMemoryLeak(() -> {
            final SwitchingCompilerFactory compilers = new SwitchingCompilerFactory(sql);
            try (
                    ServerMain serverMain = startServer(compilers);
                    TestHttpClient client = new TestHttpClient();
                    ServerAction ignored = action
            ) {
                // one connection runs the requests in order, so each returns its factory to the cache before the next
                client.setKeepConnection(true);
                serverMain.getEngine().execute("CREATE TABLE c (x INT)");
                compilers.reloadBeforeNextCompile(serverMain, true);
                action.run(serverMain, client);
                compilers.assertCompiles(1, 1);
                action.run(serverMain, client);
                compilers.assertCompiles(1, 1);
                reloadPlanner(serverMain, false, RACING);
                action.run(serverMain, client);
                compilers.assertCompiles(2, 1);
            }
        });
    }

    private static void assertPlan(String expectedPlan, CharSequence plan) {
        TestUtils.assertContains(plan, expectedPlan);
        TestUtils.assertNotContains(plan, LEGACY_PLAN.equals(expectedPlan) ? NEW_PLAN : LEGACY_PLAN);
    }

    private static void assertPlanners(TestHttpClient client, String expectedPlan) throws Exception {
        assertPlan(expectedPlan, httpGet(client, "/exec", PLAN_SQL));
        assertPlan(expectedPlan, httpGet(client, "/exp", PLAN_SQL));
        try (Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES); Statement stmt = conn.createStatement()) {
            final StringBuilder plan = new StringBuilder();
            try (ResultSet rs = stmt.executeQuery(PLAN_SQL)) {
                while (rs.next()) {
                    plan.append(rs.getString(1)).append('\n');
                }
            }
            assertPlan(expectedPlan, plan);
        }
    }

    private static void assertSelect(PreparedStatement stmt) throws Exception {
        try (ResultSet rs = stmt.executeQuery()) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(1, rs.getInt(1));
            Assert.assertFalse(rs.next());
        }
    }

    private static String httpGet(TestHttpClient client, String url, String sql) {
        final HttpClient.Request request = client.getHttpClient().newRequest("localhost", HTTP_PORT);
        request.GET().url(url).query("query", sql);
        final HttpClient.ResponseHeaders response = request.send();
        response.await();
        final Utf8StringSink sink = new Utf8StringSink();
        response.getResponse().copyTextTo(sink);
        Assert.assertEquals(sink.toString(), "200", Utf8s.toString(response.getStatusCode()));
        return sink.toString();
    }

    private static void pgSelect() throws Exception {
        try (
                Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(SELECT_SQL)
        ) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(1, rs.getInt(1));
            Assert.assertFalse(rs.next());
        }
    }

    private static void pgSimpleSelect() throws Exception {
        final Properties properties = new Properties();
        properties.putAll(PG_CONNECTION_PROPERTIES);
        properties.setProperty("preferQueryMode", "simple");
        try (
                Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, properties);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(SELECT_SQL)
        ) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(1, rs.getInt(1));
            Assert.assertFalse(rs.next());
        }
    }

    // a server-side statement from the first run, so that every later run reuses the executed statement
    private static Connection preparingConnection() throws Exception {
        final Properties properties = new Properties();
        properties.putAll(PG_CONNECTION_PROPERTIES);
        properties.setProperty("prepareThreshold", "1");
        return DriverManager.getConnection(PG_CONNECTION_URI, properties);
    }

    private static void reloadPlanner(ServerMain serverMain, boolean isLegacy, String... extra) throws Exception {
        final String[] properties = new String[extra.length + 1];
        System.arraycopy(extra, 0, properties, 0, extra.length);
        properties[extra.length] = "cairo.sql.legacy.planner.enabled=" + isLegacy;
        createDummyConfiguration(properties);
        Assert.assertTrue(serverMain.getEngine().getConfigReloader().reload());
        Assert.assertEquals(isLegacy, serverMain.getEngine().getConfiguration().isSqlLegacyPlannerEnabled());
    }

    private static long qwpRowCount(QwpQueryClient client) {
        final long[] rowCount = {-1};
        client.execute(SELECT_SQL, new QwpColumnBatchHandler() {
            @Override
            public void onBatch(QwpColumnBatch batch) {
            }

            @Override
            public void onEnd(long totalRows) {
                rowCount[0] = totalRows;
            }

            @Override
            public void onError(byte status, String message) {
                Assert.fail("egress error: " + message);
            }
        });
        return rowCount[0];
    }

    private static ServerMain start(ServerMain serverMain) throws Exception {
        try {
            serverMain.start();
            serverMain.getEngine().execute("CREATE TABLE a (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            serverMain.getEngine().execute("CREATE TABLE b (x INT, y INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            serverMain.getEngine().execute("INSERT INTO a VALUES (1, 0)");
            return serverMain;
        } catch (Throwable th) {
            serverMain.close();
            throw th;
        }
    }

    private static ServerMain startServer() throws Exception {
        return start(new ServerMain(getServerMainArgs()));
    }

    private static ServerMain startServer(SqlCompilerFactory compilerFactory) throws Exception {
        return start(new ServerMain(new Bootstrap(getServerMainArgs()) {
            @Override
            public CairoEngine newCairoEngine() {
                return new CairoEngine(getConfiguration().getCairoConfiguration(), new QdbrWalLocker(), true, getDurabilityFailureHandler()) {
                    @Override
                    public SqlCompilerFactory getSqlCompilerFactory() {
                        return compilerFactory;
                    }
                };
            }
        }));
    }

    @FunctionalInterface
    private interface ServerAction extends AutoCloseable {
        @Override
        default void close() {
        }

        void run(ServerMain serverMain, TestHttpClient client) throws Exception;
    }

    /**
     * Compilers that count the compiles of one SQL text, by either planner and by the legacy planner alone, and that
     * reload the planner switch once, at the next compile of the text: just before {@link SqlCompiler#compile} reads
     * the switch, or, in a batch, just before the callback's cache lookup, after the batch has read it.
     */
    private static final class SwitchingCompilerFactory implements SqlCompilerFactory {
        private final AtomicInteger compiles = new AtomicInteger();
        private final AtomicInteger legacyCompiles = new AtomicInteger();
        private final String sql;
        private volatile Runnable beforeNextCompile;

        private SwitchingCompilerFactory(String sql) {
            this.sql = sql;
        }

        @Override
        public SqlCompilerImpl getInstance(CairoEngine engine) {
            return new SqlCompilerImpl(engine) {
                @Override
                public @NotNull CompiledQuery compile(@NotNull CharSequence sqlText, @NotNull SqlExecutionContext executionContext) throws SqlException {
                    if (isCounted(sqlText)) {
                        runBeforeCompile();
                        compiles.incrementAndGet();
                    }
                    return super.compile(sqlText, executionContext);
                }

                @Override
                public void compileBatch(@NotNull CharSequence batchText, @NotNull SqlExecutionContext executionContext, BatchCallback batchCallback) throws Exception {
                    super.compileBatch(batchText, executionContext, new BatchCallback() {
                        @Override
                        public void postCompile(SqlCompiler compiler, CompiledQuery cq, CharSequence queryText) throws Exception {
                            if (isCounted(queryText)) {
                                compiles.incrementAndGet();
                            }
                            batchCallback.postCompile(compiler, cq, queryText);
                        }

                        @Override
                        public boolean preCompile(SqlCompiler compiler, CharSequence sqlText) {
                            if (isCounted(sqlText)) {
                                runBeforeCompile();
                            }
                            return batchCallback.preCompile(compiler, sqlText);
                        }
                    });
                }
            };
        }

        @Override
        public LegacyPlanner newLegacyPlanner(CairoEngine engine) {
            return new LegacyPlannerImpl(engine, legacyEngine -> new io.questdb.legacy.griffin.SqlCompilerImpl(legacyEngine) {
                @Override
                public @NotNull CompiledQuery compile(@NotNull CharSequence sqlText, @NotNull SqlExecutionContext executionContext) throws SqlException {
                    if (isCounted(sqlText)) {
                        legacyCompiles.incrementAndGet();
                    }
                    return super.compile(sqlText, executionContext);
                }
            });
        }

        private void assertCompiles(int expectedCompiles, int expectedLegacyCompiles) {
            Assert.assertEquals("compiles", expectedCompiles, compiles.get());
            Assert.assertEquals("legacy compiles", expectedLegacyCompiles, legacyCompiles.get());
        }

        private boolean isCounted(CharSequence sqlText) {
            return Chars.startsWith(sqlText, sql);
        }

        private void reloadBeforeNextCompile(ServerMain serverMain, boolean isLegacy) {
            beforeNextCompile = () -> TestUtils.unchecked(() -> reloadPlanner(serverMain, isLegacy, RACING));
        }

        private void runBeforeCompile() {
            final Runnable action = beforeNextCompile;
            if (action != null) {
                beforeNextCompile = null;
                action.run();
            }
        }
    }
}
