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
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class LegacyPlannerServerTest extends AbstractBootstrapTest {
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
    public void testHttpExportHeaderSelectsPlanner() throws Exception {
        assertMemoryLeak(() -> {
            try (ServerMain ignored = startServer(); TestHttpClient client = new TestHttpClient()) {
                assertPlan(LEGACY_PLAN, httpGet(client, "/exp", PLAN_SQL, "true"));
                assertPlan(NEW_PLAN, httpGet(client, "/exp", PLAN_SQL, null));
            }
        });
    }

    @Test
    public void testHttpExportSelectCacheKeepsPlannersApart() throws Exception {
        createCachingConfiguration();
        assertMemoryLeak(() -> {
            try (ServerMain serverMain = startServer(); TestHttpClient client = new TestHttpClient()) {
                client.setKeepConnection(true);
                final Counter hits = serverMain.getEngine().getMetrics().jsonQueryMetrics().cacheHitCounter();
                final long hitsBefore = hits.getValue();
                Assert.assertEquals("\"x\"\r\n1\r\n", httpGet(client, "/exp", SELECT_SQL, null));
                httpGet(client, "/exp", SELECT_SQL, "true");
                Assert.assertEquals(0, hits.getValue() - hitsBefore);
                Assert.assertEquals("\"x\"\r\n1\r\n", httpGet(client, "/exp", SELECT_SQL, "true"));
                Assert.assertEquals(1, hits.getValue() - hitsBefore);
                httpGet(client, "/exp", SELECT_SQL, null);
                Assert.assertEquals(2, hits.getValue() - hitsBefore);
            }
        });
    }

    @Test
    public void testHttpHeaderAppliesToItsRequestOnly() throws Exception {
        assertMemoryLeak(() -> {
            try (ServerMain ignored = startServer(); TestHttpClient client = new TestHttpClient()) {
                client.setKeepConnection(true);
                assertPlan(LEGACY_PLAN, httpGet(client, "/exec", PLAN_SQL, "true"));
                assertPlan(NEW_PLAN, httpGet(client, "/exec", PLAN_SQL, null));
            }
        });
    }

    @Test
    public void testHttpHeaderOverridesServerProperty() throws Exception {
        createDummyConfiguration("cairo.sql.legacy.planner.enabled=true");
        assertMemoryLeak(() -> {
            try (ServerMain ignored = startServer(); TestHttpClient client = new TestHttpClient()) {
                assertPlan(LEGACY_PLAN, httpGet(client, "/exec", PLAN_SQL, null));
                assertPlan(NEW_PLAN, httpGet(client, "/exec", PLAN_SQL, "false"));
                assertPlan(LEGACY_PLAN, httpGet(client, "/exec", PLAN_SQL, "garbage"));
            }
        });
    }

    @Test
    public void testHttpHeaderSelectsPlanner() throws Exception {
        assertMemoryLeak(() -> {
            try (ServerMain ignored = startServer(); TestHttpClient client = new TestHttpClient()) {
                assertPlan(NEW_PLAN, httpGet(client, "/exec", PLAN_SQL, null));
                assertPlan(LEGACY_PLAN, httpGet(client, "/exec", PLAN_SQL, "true"));
                assertPlan(LEGACY_PLAN, httpGet(client, "/exec", PLAN_SQL, "TRUE"));
                assertPlan(NEW_PLAN, httpGet(client, "/exec", PLAN_SQL, "false"));
                assertPlan(NEW_PLAN, httpGet(client, "/exec", PLAN_SQL, "False"));
                assertPlan(NEW_PLAN, httpGet(client, "/exec", PLAN_SQL, ""));
                assertPlan(NEW_PLAN, httpGet(client, "/exec", PLAN_SQL, "1"));
            }
        });
    }

    @Test
    public void testHttpSelectCacheKeepsPlannersApart() throws Exception {
        createCachingConfiguration();
        assertMemoryLeak(() -> {
            try (ServerMain serverMain = startServer(); TestHttpClient client = new TestHttpClient()) {
                // one connection runs the requests in order, so each returns its factory to the cache before the next
                client.setKeepConnection(true);
                final Counter hits = serverMain.getEngine().getMetrics().jsonQueryMetrics().cacheHitCounter();
                final long hitsBefore = hits.getValue();
                httpGet(client, "/exec", SELECT_SQL, null);
                httpGet(client, "/exec", SELECT_SQL, "true");
                Assert.assertEquals(0, hits.getValue() - hitsBefore);
                httpGet(client, "/exec", SELECT_SQL, "true");
                Assert.assertEquals(1, hits.getValue() - hitsBefore);
                httpGet(client, "/exec", SELECT_SQL, null);
                Assert.assertEquals(2, hits.getValue() - hitsBefore);
            }
        });
    }

    @Test
    public void testPgOptionAppliesAlongsideStatementTimeout() throws Exception {
        assertMemoryLeak(() -> {
            try (ServerMain ignored = startServer()) {
                for (String options : new String[]{
                        "-c statement_timeout=1 -c legacy_planner=true",
                        "-c legacy_planner=true -c statement_timeout=1",
                        "-c  legacy_planner=true   -cstatement_timeout=1 ",
                        "-c unknown=1 -c legacy_planner=true -c statement_timeout=1"
                }) {
                    try (Connection conn = pgConnection(options); Statement stmt = conn.createStatement()) {
                        assertPlan(LEGACY_PLAN, pgPlan(stmt));
                        try {
                            stmt.executeQuery("SELECT sum(x) FROM long_sequence(10_000_000_000)");
                            Assert.fail("statement timeout ignored for options: " + options);
                        } catch (PSQLException e) {
                            TestUtils.assertContains(e.getMessage(), "timeout, query aborted");
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testPgOptionOverridesServerProperty() throws Exception {
        createDummyConfiguration("cairo.sql.legacy.planner.enabled=true");
        assertMemoryLeak(() -> {
            try (ServerMain ignored = startServer()) {
                assertPgPlan(LEGACY_PLAN, null);
                assertPgPlan(NEW_PLAN, "-c legacy_planner=false");
                assertPgPlan(LEGACY_PLAN, "-c legacy_planner=maybe");
            }
        });
    }

    @Test
    public void testPgOptionSelectsPlanner() throws Exception {
        assertMemoryLeak(() -> {
            try (ServerMain ignored = startServer()) {
                assertPgPlan(NEW_PLAN, null);
                assertPgPlan(LEGACY_PLAN, "-c legacy_planner=true");
                assertPgPlan(LEGACY_PLAN, "-c legacy_planner=True");
                assertPgPlan(NEW_PLAN, "-c legacy_planner=false");
                assertPgPlan(NEW_PLAN, "-c legacy_planner=");
                assertPgPlan(NEW_PLAN, "-c legacy_planner");
                assertPgPlan(NEW_PLAN, "-c statement_timeout=60000");
                assertPgPlan(LEGACY_PLAN, "-c statement_timeout=soon -c legacy_planner=true");
                assertPgPlan(LEGACY_PLAN, "-c legacy_planner=true -c");
                // a token that is no "-c" setting ends the options, so nothing after it applies
                assertPgPlan(NEW_PLAN, "x -c legacy_planner=true");
                assertPgPlan(NEW_PLAN, "-x legacy_planner=true");
                assertPgPlan(NEW_PLAN, "-");
                // a connection never inherits the planner of the one that used its context before
                assertPgPlan(LEGACY_PLAN, "-c legacy_planner=true");
                assertPgPlan(NEW_PLAN, null);
            }
        });
    }

    @Test
    public void testPgPreparedStatementSelectCacheKeepsPlannersApart() throws Exception {
        createCachingConfiguration();
        assertMemoryLeak(() -> {
            try (ServerMain serverMain = startServer()) {
                final Counter hits = serverMain.getEngine().getMetrics().pgWireMetrics().selectCacheHitCounter();
                final Properties properties = new Properties();
                properties.putAll(PG_CONNECTION_PROPERTIES);
                properties.setProperty("options", "-c legacy_planner=true");
                // a server-side statement from the first run, so that every later run reuses the executed statement
                properties.setProperty("prepareThreshold", "1");
                try (Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, properties);
                     PreparedStatement stmt = conn.prepareStatement(SELECT_SQL)) {
                    for (int i = 0; i < 3; i++) {
                        try (ResultSet rs = stmt.executeQuery()) {
                            Assert.assertTrue(rs.next());
                            Assert.assertEquals(1, rs.getInt(1));
                            Assert.assertFalse(rs.next());
                        }
                    }
                }
                final long hitsBefore = hits.getValue();
                pgSelect(null);
                Assert.assertEquals(0, hits.getValue() - hitsBefore);
                pgSelect("-c legacy_planner=true");
                Assert.assertEquals(1, hits.getValue() - hitsBefore);
            }
        });
    }

    @Test
    public void testPgSelectCacheKeepsPlannersApart() throws Exception {
        createCachingConfiguration();
        assertMemoryLeak(() -> {
            try (ServerMain serverMain = startServer()) {
                // pgwire returns an executed statement to the cache at Sync, before the client sees its result
                final Counter hits = serverMain.getEngine().getMetrics().pgWireMetrics().selectCacheHitCounter();
                final long hitsBefore = hits.getValue();
                pgSelect(null);
                pgSelect("-c legacy_planner=true");
                Assert.assertEquals(0, hits.getValue() - hitsBefore);
                pgSelect("-c legacy_planner=true");
                Assert.assertEquals(1, hits.getValue() - hitsBefore);
                pgSelect(null);
                Assert.assertEquals(2, hits.getValue() - hitsBefore);
            }
        });
    }

    private static void assertPgPlan(String expectedPlan, @Nullable String options) throws Exception {
        try (Connection conn = pgConnection(options); Statement stmt = conn.createStatement()) {
            assertPlan(expectedPlan, pgPlan(stmt));
        }
    }

    private static void assertPlan(String expectedPlan, CharSequence plan) {
        TestUtils.assertContains(plan, expectedPlan);
        TestUtils.assertNotContains(plan, LEGACY_PLAN.equals(expectedPlan) ? NEW_PLAN : LEGACY_PLAN);
    }

    private static void createCachingConfiguration() throws Exception {
        createDummyConfiguration(
                "http.query.cache.enabled=true",
                "pg.select.cache.enabled=true",
                "metrics.enabled=true"
        );
    }

    private static String httpGet(TestHttpClient client, String url, String sql, @Nullable String legacyPlanner) {
        final HttpClient.Request request = client.getHttpClient().newRequest("localhost", HTTP_PORT);
        request.GET().url(url).query("query", sql);
        if (legacyPlanner != null) {
            request.header("Legacy-Planner", legacyPlanner);
        }
        final HttpClient.ResponseHeaders response = request.send();
        response.await();
        final Utf8StringSink sink = new Utf8StringSink();
        response.getResponse().copyTextTo(sink);
        Assert.assertEquals(sink.toString(), "200", Utf8s.toString(response.getStatusCode()));
        return sink.toString();
    }

    private static Connection pgConnection(@Nullable String options) throws Exception {
        final Properties properties = new Properties();
        properties.putAll(PG_CONNECTION_PROPERTIES);
        if (options != null) {
            properties.setProperty("options", options);
        }
        return DriverManager.getConnection(PG_CONNECTION_URI, properties);
    }

    private static String pgPlan(Statement stmt) throws Exception {
        final StringBuilder plan = new StringBuilder();
        try (ResultSet rs = stmt.executeQuery(PLAN_SQL)) {
            while (rs.next()) {
                plan.append(rs.getString(1)).append('\n');
            }
        }
        return plan.toString();
    }

    private static void pgSelect(@Nullable String options) throws Exception {
        try (Connection conn = pgConnection(options); Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(SELECT_SQL)) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(1, rs.getInt(1));
            Assert.assertFalse(rs.next());
        }
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
