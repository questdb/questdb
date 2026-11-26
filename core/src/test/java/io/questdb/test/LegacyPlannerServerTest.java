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

import io.questdb.ServerMain;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.metrics.Counter;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.cutlass.http.TestHttpClient;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class LegacyPlannerServerTest extends AbstractBootstrapTest {
    private static final String[] CACHING = {
            "http.query.cache.enabled=true",
            "pg.select.cache.enabled=true",
            "metrics.enabled=true"
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

    private static ServerMain startServer() throws Exception {
        final ServerMain serverMain = new ServerMain(getServerMainArgs());
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
}
