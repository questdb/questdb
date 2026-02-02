/*******************************************************************************
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
import io.questdb.BootstrapConfiguration;
import io.questdb.ConfigPropertyKey;
import io.questdb.ConfigReloader;
import io.questdb.DefaultBootstrapConfiguration;
import io.questdb.DefaultHttpClientConfiguration;
import io.questdb.DynamicPropServerConfiguration;
import io.questdb.FactoryProviderFactoryImpl;
import io.questdb.HttpClientConfiguration;
import io.questdb.Metrics;
import io.questdb.PropertyKey;
import io.questdb.ServerConfiguration;
import io.questdb.ServerMain;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientException;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.metrics.QueryTracingJob;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.test.cutlass.http.TestHttpClient;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;
import static org.junit.Assert.assertFalse;

public class DynamicPropServerConfigurationTest extends AbstractTest {
    private static final TestHttpClient testHttpClient = new TestHttpClient();
    private File serverConf;

    @AfterClass
    public static void tearDownStatic() {
        testHttpClient.close();
        AbstractTest.tearDownStatic();
        assert Unsafe.getMemUsedByTag(MemoryTag.NATIVE_HTTP_CONN) == 0;
    }

    @Before
    public void setUp() {
        Path serverConfPath = Paths.get(temp.getRoot().getAbsolutePath(), "dbRoot", "conf", "server.conf");
        try {
            Files.createDirectories(serverConfPath.getParent());
            serverConf = serverConfPath.toFile();
            Assert.assertTrue(serverConf.createNewFile());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertTrue(serverConf.exists());
    }

    @Test
    public void testAsOfJoinEvacuationThreshold() throws Exception {
        assertMemoryLeak(() -> {
            try (ServerMain serverMain = new ServerMain(getBootstrap())) {
                serverMain.start();

                try (FileWriter w = new FileWriter(serverConf)) {
                    w.write("cairo.sql.asof.join.evacuation.threshold=1000\n");
                }

                assertReloadConfigEventually();

                int threshold = serverMain.getConfiguration().getCairoConfiguration().getSqlAsOfJoinMapEvacuationThreshold();
                Assert.assertEquals(1000, threshold);
            }
        });
    }

    @Test
    public void testAsOfJoinShortCircuitCacheCapacity() throws Exception {
        assertMemoryLeak(() -> {
            try (ServerMain serverMain = new ServerMain(getBootstrap())) {
                serverMain.start();

                try (FileWriter w = new FileWriter(serverConf)) {
                    w.write("cairo.sql.asof.join.short.circuit.cache.capacity=1000\n");
                }

                assertReloadConfigEventually();

                int capacity = serverMain.getConfiguration().getCairoConfiguration().getSqlAsOfJoinShortCircuitCacheCapacity();
                Assert.assertEquals(1000, capacity);
            }
        });
    }

    @Test
    public void testConfigChangeListener() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicLong configChangedCalledCounter = new AtomicLong(0);
            try (ServerMain serverMain = new ServerMain(getBootstrap())) {
                serverMain.start();
                final var listener = new ConfigReloader.Listener() {
                    private final ConfigPropertyKey[] WATCHED_PROPERTIES = new ConfigPropertyKey[]{
                            PropertyKey.CAIRO_SQL_ASOF_JOIN_EVACUATION_THRESHOLD
                    };

                    @Override
                    public void configChanged() {
                        configChangedCalledCounter.incrementAndGet();
                    }

                    @Override
                    public @NotNull ConfigPropertyKey[] getWatchedConfigKeys() {
                        return WATCHED_PROPERTIES;
                    }
                };
                final long watchId = serverMain.getEngine().getConfigReloader().watch(listener);

                Assert.assertTrue("watchId should be non-negative", watchId >= 0);
                Assert.assertEquals(0, configChangedCalledCounter.get());

                // [1] First, reload config after changing a watched setting.
                try (FileWriter w = new FileWriter(serverConf)) {
                    w.write("cairo.sql.asof.join.evacuation.threshold=1000\n");
                }

                assertReloadConfigEventually();

                Assert.assertEquals(1, configChangedCalledCounter.get());

                final int threshold = serverMain.getConfiguration().getCairoConfiguration().getSqlAsOfJoinMapEvacuationThreshold();
                Assert.assertEquals(1000, threshold);

                // [2] Now again, reload config after changing a setting which is reloadable but not watched.
                try (FileWriter w = new FileWriter(serverConf)) {
                    w.write("cairo.sql.asof.join.evacuation.threshold=1000\n");
                    w.write("cairo.sql.asof.join.short.circuit.cache.capacity=4000\n");
                }

                assertReloadConfigEventually();

                final int capacity = serverMain.getConfiguration().getCairoConfiguration().getSqlAsOfJoinShortCircuitCacheCapacity();
                Assert.assertEquals(4000, capacity);

                // Note! The listener was _not_ notified. The `cairo.sql.asof.join.short.circuit.cache.capacity` is of no interest.
                Assert.assertEquals(1, configChangedCalledCounter.get());

                // [3] Now we test _removing_ keys. Should get notified.
                try (FileWriter w = new FileWriter(serverConf)) {
                    // Not going to `w.write("cairo.sql.asof.join.evacuation.threshold=1000\n");`, i.e. removed.
                    w.write("cairo.sql.asof.join.short.circuit.cache.capacity=4000\n");
                }
                assertReloadConfigEventually();
                Assert.assertEquals(2, configChangedCalledCounter.get());

                // [4] Finally, we test unregistering from config changes.
                // We should not get notified, despite adding back the key of interest.
                serverMain.getEngine().getConfigReloader().unwatch(watchId);
                try (FileWriter w = new FileWriter(serverConf)) {
                    w.write("cairo.sql.asof.join.evacuation.threshold=2000\n");
                }
                assertReloadConfigEventually();
                Assert.assertEquals(2, configChangedCalledCounter.get());

                // [5] Should we re-register, we'll get a different ID.
                final long watchId2 = serverMain.getEngine().getConfigReloader().watch(listener);
                Assert.assertNotEquals("re-register should return a new watchId", watchId, watchId2);
            }
        });
    }

    @Test
    public void testHttpConnectionLimitReload() throws Exception {
        assertMemoryLeak(() -> {
            try (FileWriter w = new FileWriter(serverConf)) {
                w.write("http.net.bind.to=0.0.0.0:9001\n");
                w.write("http.net.connection.limit=1\n");
            }

            try (ServerMain serverMain = new ServerMain(getBootstrap())) {
                serverMain.start();

                final Metrics metrics = serverMain.getEngine().getMetrics();
                final HttpClientConfiguration config = new DefaultHttpClientConfiguration() {
                    @Override
                    public int getTimeout() {
                        return 1000;
                    }
                };
                try (
                        HttpClient httpClient1 = HttpClientFactory.newPlainTextInstance(config);
                        TestHttpClient testHttpClient1 = new TestHttpClient(httpClient1)
                ) {
                    testHttpClient1.setKeepConnection(true);
                    testHttpClient1.assertGet(
                            "/exec",
                            "{\"query\":\"select 1;\",\"columns\":[{\"name\":\"1\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[1]],\"count\":1}",
                            "select 1;"
                    );

                    try (
                            HttpClient httpClient2 = HttpClientFactory.newPlainTextInstance(config);
                            TestHttpClient testHttpClient2 = new TestHttpClient(httpClient2)
                    ) {
                        testHttpClient2.assertGet(
                                "/exec",
                                "{\"query\":\"select 1;\",\"columns\":[{\"name\":\"1\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[1]],\"count\":1}",
                                "select 1;"
                        );
                        Assert.fail();
                    } catch (Exception ignore) {
                    }
                }

                try (FileWriter w = new FileWriter(serverConf)) {
                    w.write("http.net.bind.to=0.0.0.0:9001\n");
                    w.write("http.net.connection.limit=10\n");
                }

                TestUtils.assertEventually(() -> Assert.assertEquals(0, metrics.httpMetrics().connectionCountGauge().getValue()));
                TestUtils.assertEventually(() -> Assert.assertTrue(3 < metrics.httpMetrics().listenerStateChangeCounter().getValue()));

                assertReloadConfigEventually();

                // we should be able to open two connections eventually
                TestUtils.assertEventually(() -> {
                    try (
                            HttpClient httpClient1 = HttpClientFactory.newPlainTextInstance(config);
                            TestHttpClient testHttpClient1 = new TestHttpClient(httpClient1)
                    ) {
                        testHttpClient1.setKeepConnection(true);
                        testHttpClient1.assertGet(
                                "/exec",
                                "{\"query\":\"select 1;\",\"columns\":[{\"name\":\"1\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[1]],\"count\":1}",
                                "select 1;"
                        );

                        try (
                                HttpClient httpClient2 = HttpClientFactory.newPlainTextInstance(config);
                                TestHttpClient testHttpClient2 = new TestHttpClient(httpClient2)
                        ) {
                            testHttpClient2.assertGet(
                                    "/exec",
                                    "{\"query\":\"select 1;\",\"columns\":[{\"name\":\"1\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[1]],\"count\":1}",
                                    "select 1;"
                            );
                        }
                    }
                });
            }
        });
    }

    @Test
    public void testHttpRecvBufferSizeReload() throws Exception {
        assertMemoryLeak(() -> {
            try (FileWriter w = new FileWriter(serverConf)) {
                w.write("http.net.bind.to=0.0.0.0:9001\n");
                w.write("http.request.header.buffer.size=100\n");
                w.write("http.recv.buffer.size=100\n");
            }

            try (ServerMain serverMain = new ServerMain(getBootstrap())) {
                serverMain.start();

                final StringSink querySink = new StringSink();
                querySink.put("select length('");
                querySink.put(Chars.repeat("q", 150));
                querySink.put("');");
                final String query = querySink.toString();
                try {
                    testHttpClient.assertGet(
                            "/exec",
                            "",
                            query
                    );
                    Assert.fail();
                } catch (HttpClientException ignore) {
                }

                try (FileWriter w = new FileWriter(serverConf)) {
                    w.write("http.net.bind.to=0.0.0.0:9001\n");
                    w.write("http.request.header.buffer.size=300\n");
                    w.write("http.recv.buffer.size=300\n");
                }

                assertReloadConfigEventually();

                // second reload should not reload (no changes)
                assertReloadConfig(false);

                try {
                    testHttpClient.assertGet(
                            "/exec",
                            "{\"query\":\"select length('qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq');\",\"columns\":[{\"name\":\"length('qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[150]],\"count\":1}",
                            query
                    );
                    Assert.fail();
                } catch (HttpClientException ignore) {

                }

                try (FileWriter w = new FileWriter(serverConf)) {
                    w.write("http.net.bind.to=0.0.0.0:9001\n");
                    // add size for the second copy of the URL query
                    w.write("http.request.header.buffer.size=500\n");
                    w.write("http.recv.buffer.size=300\n");
                }

                assertReloadConfigEventually();

                // second reload should not reload (no changes)
                assertReloadConfig(false);

                testHttpClient.assertGet(
                        "/exec",
                        "{\"query\":\"select length('qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq');\",\"columns\":[{\"name\":\"length('qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[150]],\"count\":1}",
                        query
                );
            }
        });
    }

    @Test
    public void testHttpSendBufferSizeReload() throws Exception {
        assertMemoryLeak(() -> {
            try (FileWriter w = new FileWriter(serverConf)) {
                w.write("http.net.bind.to=0.0.0.0:9001\n");
                w.write("http.send.buffer.size=100\n");
                w.write("query.tracing.enabled=true\n");
            }

            try (ServerMain serverMain = new ServerMain(getBootstrap())) {
                serverMain.start();

                final String query = "select rpad('QuestDB', 150, '0');";
                try {
                    testHttpClient.assertGet(
                            "/exec",
                            "",
                            query
                    );
                    Assert.fail();
                } catch (HttpClientException ignore) {
                }

                try (FileWriter w = new FileWriter(serverConf)) {
                    w.write("http.net.bind.to=0.0.0.0:9001\n");
                    w.write("http.send.buffer.size=200\n");
                }

                assertReloadConfigEventually();

                testHttpClient.assertGet(
                        "/exec",
                        "{\"query\":\"select rpad('QuestDB', 150, '0');\",\"columns\":[{\"name\":\"rpad('QuestDB', 150, '0')\",\"type\":\"STRING\"}],\"timestamp\":-1,\"dataset\":[[\"QuestDB00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000\"]],\"count\":1}",
                        query
                );
            }
        });
    }

    @Test
    public void testPgNamedStatementLimitReloadWithChangedProp() throws Exception {
        assertMemoryLeak(() -> {
            try (ServerMain serverMain = new ServerMain(getBootstrap())) {
                serverMain.start();

                int namedStatementLimit = serverMain.getConfiguration().getPGWireConfiguration().getNamedStatementLimit();
                Assert.assertEquals(10_000, namedStatementLimit);

                try (FileWriter w = new FileWriter(serverConf)) {
                    w.write("pg.named.statement.limit=10\n");
                }

                assertReloadConfigEventually();

                namedStatementLimit = serverMain.getConfiguration().getPGWireConfiguration().getNamedStatementLimit();
                Assert.assertEquals(10, namedStatementLimit);
            }
        });
    }

    @Test
    public void testPgWireConnectionLimitReload() throws Exception {
        assertMemoryLeak(() -> {
            try (FileWriter w = new FileWriter(serverConf)) {
                w.write("pg.net.connection.limit=1\n");
            }

            try (ServerMain serverMain = new ServerMain(getBootstrap())) {
                serverMain.start();

                final Metrics metrics = serverMain.getEngine().getMetrics();

                try (Connection conn1 = getConnection("admin", "quest")) {
                    assertFalse(conn1.isClosed());

                    try {
                        try (Connection ignore = getConnection("admin", "quest")) {
                            Assert.fail();
                        }
                    } catch (Throwable ignore) {
                    }
                }

                try (FileWriter w = new FileWriter(serverConf)) {
                    w.write("pg.net.connection.limit=10\n");
                }

                TestUtils.assertEventually(() -> Assert.assertEquals(0, metrics.pgWireMetrics().connectionCountGauge().getValue()));
                TestUtils.assertEventually(() -> Assert.assertTrue(3 < metrics.pgWireMetrics().listenerStateChangeCounter().getValue()));

                // call the reload method directly instead of using the reload_config() SQL function
                // to avoid opening a PGWire connection;
                // there is no reliable way to wait until the server listener is re-registered
                serverMain.getEngine().getConfigReloader().reload();

                // while the configuration was reloaded, metrics must not be reset
                TestUtils.assertEventually(() -> Assert.assertTrue(3 < metrics.pgWireMetrics().listenerStateChangeCounter().getValue()));

                // we should be able to open two connections eventually
                TestUtils.assertEventually(() -> {
                    try (Connection conn1 = getConnection("admin", "quest")) {
                        assertFalse(conn1.isClosed());

                        try (Connection conn2 = getConnection("admin", "quest")) {
                            assertFalse(conn2.isClosed());
                        }
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        });
    }

    @Test
    public void testPgWireCredentialsReloadByDeletingProp() throws Exception {
        assertMemoryLeak(() -> {
            try (FileWriter w = new FileWriter(serverConf)) {
                w.write("pg.user=steven\n");
                w.write("pg.password=sklar\n");
            }

            try (ServerMain serverMain = new ServerMain(getBootstrap())) {
                serverMain.start();

                try (Connection conn = getConnection("steven", "sklar")) {
                    assertFalse(conn.isClosed());
                }

                // Overwrite file to remove props
                try (FileWriter w = new FileWriter(serverConf, false)) {
                    w.write("\n");
                }

                assertReloadConfig(true, "steven", "sklar");

                try (Connection conn = getConnection("admin", "quest")) {
                    assertFalse(conn.isClosed());
                }
            }
        });
    }

    @Test
    public void testPgWireCredentialsReloadWithChangedProp() throws Exception {
        assertMemoryLeak(() -> {
            try (FileWriter w = new FileWriter(serverConf)) {
                w.write("pg.user=steven\n");
                w.write("pg.password=sklar\n");
            }

            try (ServerMain serverMain = new ServerMain(getBootstrap())) {
                serverMain.start();

                try (Connection conn = getConnection("steven", "sklar")) {
                    assertFalse(conn.isClosed());
                }

                try (FileWriter w = new FileWriter(serverConf)) {
                    w.write("pg.user=nevets\n");
                    w.write("pg.password=ralks\n");
                }

                assertReloadConfig(true, "steven", "sklar");

                try (Connection conn = getConnection("nevets", "ralks")) {
                    assertFalse(conn.isClosed());
                }
                Assert.assertThrows(PSQLException.class, () -> getConnection("admin", "quest"));
            }
        });
    }

    @Test
    public void testPgWireCredentialsReloadWithChangedPropAfterRecreatedFile() throws Exception {
        assertMemoryLeak(() -> {
            try (ServerMain serverMain = new ServerMain(getBootstrap())) {
                serverMain.start();

                try (Connection conn = getConnection("admin", "quest")) {
                    assertFalse(conn.isClosed());
                }

                Assert.assertTrue(serverConf.delete());
                Assert.assertTrue(serverConf.createNewFile());

                try (FileWriter w = new FileWriter(serverConf)) {
                    w.write("pg.user=steven\n");
                    w.write("pg.password=sklar\n");
                }

                assertReloadConfigEventually();

                try (Connection conn = getConnection("steven", "sklar")) {
                    assertFalse(conn.isClosed());
                }

                Assert.assertThrows(PSQLException.class, () -> getConnection("admin", "quest"));
            }
        });
    }

    @Test
    public void testPgWireCredentialsReloadWithNewProp() throws Exception {
        assertMemoryLeak(() -> {
            try (ServerMain serverMain = new ServerMain(getBootstrap())) {
                serverMain.start();

                try (Connection conn = getConnection("admin", "quest")) {
                    assertFalse(conn.isClosed());
                }

                try (FileWriter w = new FileWriter(serverConf)) {
                    w.write("pg.user=steven\n");
                    w.write("pg.password=sklar\n");
                }

                assertReloadConfigEventually();

                try (Connection conn = getConnection("steven", "sklar")) {
                    assertFalse(conn.isClosed());
                }

                Assert.assertThrows(PSQLException.class, () -> getConnection("admin", "quest"));
            }
        });
    }

    @Test
    public void testPgWireRecvBufferSizeReload() throws Exception {
        assertMemoryLeak(() -> {
            try (FileWriter w = new FileWriter(serverConf)) {
                w.write("pg.recv.buffer.size=256\n");
            }

            try (ServerMain serverMain = new ServerMain(getBootstrap())) {
                serverMain.start();

                final StringSink sink = new StringSink();
                sink.put(Chars.repeat("q", 300));

                try (Connection conn = getConnection("admin", "quest")) {
                    try (PreparedStatement stmt = conn.prepareStatement("create table x (s varchar)")) {
                        stmt.execute();
                    }

                    try (PreparedStatement stmt = conn.prepareStatement("insert into x values (?)")) {
                        stmt.setString(1, sink.toString());
                        stmt.execute();
                        Assert.fail();
                    } catch (PSQLException ignore) {
                    }
                }

                try (FileWriter w = new FileWriter(serverConf)) {
                    w.write("pg.recv.buffer.size=512\n");
                }

                assertReloadConfigEventually();

                // now we should be able to insert
                try (
                        Connection conn = getConnection("admin", "quest");
                        PreparedStatement stmt = conn.prepareStatement("insert into x values (?)")
                ) {
                    stmt.setString(1, sink.toString());
                    stmt.execute();
                }
            }
        });
    }

    @Test
    public void testPgWireSendBufferSizeReload() throws Exception {
        assertMemoryLeak(() -> {
            try (FileWriter w = new FileWriter(serverConf)) {
                w.write("pg.send.buffer.size=256\n");
            }

            try (ServerMain serverMain = new ServerMain(getBootstrap())) {
                serverMain.start();

                final int len = 300;
                final String query = "select rpad('QuestDB', " + len + ", '0');";
                try (
                        Connection conn = getConnection("admin", "quest");
                        PreparedStatement stmt = conn.prepareStatement(query)
                ) {
                    try (ResultSet ignore = stmt.executeQuery()) {
                        Assert.fail();
                    } catch (PSQLException e) {
                        TestUtils.assertContains(e.getMessage(), "not enough space in send buffer");
                    }
                }

                try (FileWriter w = new FileWriter(serverConf)) {
                    w.write("pg.send.buffer.size=512\n");
                }

                assertReloadConfigEventually();

                try (
                        Connection conn = getConnection("admin", "quest");
                        PreparedStatement stmt = conn.prepareStatement(query);
                        ResultSet rs = stmt.executeQuery()
                ) {
                    Assert.assertTrue(rs.next());
                    Assert.assertEquals(len, rs.getString(1).length());
                }
            }
        });
    }

    @Test
    public void testQueryTracingReload() throws Exception {
        assertMemoryLeak(() -> {
            try (ServerMain serverMain = new ServerMain(getBootstrap())) {
                serverMain.start();

                String tableName = QueryTracingJob.TABLE_NAME;
                try (
                        Connection conn = getConnection("admin", "quest");
                        PreparedStatement queryTraceStmt = conn.prepareStatement(tableName)
                ) {
                    Runnable assertTableEmpty = () -> {
                        try (ResultSet rs = queryTraceStmt.executeQuery()) {
                            assertFalse("Query Trace table not empty, but query tracing is disabled", rs.next());
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    };

                    // this executes a query, and would trigger query tracing (if it was enabled) as a side effect
                    assertTableEmpty.run();
                    Os.sleep(1_000);
                    // by this time the query_trace table would most likely exist if tracing was enabled
                    assertTableEmpty.run();

                    try (FileWriter w = new FileWriter(serverConf)) {
                        w.write("query.tracing.enabled=true\n");
                    }
                    // This is also a query. With tracing now on, it triggers creating the query_trace table:
                    assertReloadConfigEventually();

                    int sleepMillis = 100;
                    while (true) {
                        Os.sleep(sleepMillis);
                        try (ResultSet rs = queryTraceStmt.executeQuery()) {
                            Assert.assertTrue(rs.next());
                            break;
                        } catch (AssertionError | SQLException e) {
                            if (sleepMillis >= 6400) {
                                throw e;
                            }
                            sleepMillis *= 2;
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testReloadDisabled() throws Exception {
        assertMemoryLeak(() -> {
            try (FileWriter w = new FileWriter(serverConf)) {
                w.write("pg.user=steven\n");
                w.write("pg.password=sklar\n");
                w.write("config.reload.enabled=false\n");
            }

            try (ServerMain serverMain = new ServerMain(getBootstrap())) {
                serverMain.start();

                try (Connection conn = getConnection("steven", "sklar")) {
                    assertFalse(conn.isClosed());
                }

                // overwrite file to remove props
                try (FileWriter w = new FileWriter(serverConf, false)) {
                    w.write("\n");
                }

                assertReloadConfig(false, "steven", "sklar");

                try (Connection conn = getConnection("steven", "sklar")) {
                    assertFalse(conn.isClosed());
                }
            }
        });
    }

    @Test
    public void testRemovalOfUnsupportedPropertyWontTriggerReload() throws Exception {
        assertMemoryLeak(() -> {
            try (FileWriter w = new FileWriter(serverConf)) {
                w.write("pg.user=steven\n");
                w.write("pg.password=sklar\n");
                w.write("cairo.legacy.string.column.type.default=true\n"); // non-default value
            }

            try (ServerMain serverMain = new ServerMain(getBootstrap())) {
                serverMain.start();

                Assert.assertTrue(serverMain.getConfiguration().getLineTcpReceiverConfiguration().isUseLegacyStringDefault());

                // remove unsupported property
                try (FileWriter w = new FileWriter(serverConf, false)) {
                    w.write("pg.user=steven\n");
                    w.write("pg.password=sklar\n");
                }

                assertReloadConfig(false, "steven", "sklar");
            }
        });
    }

    @Test
    public void testRemovedUnsupportedPropertyWontReturnToDefault() throws Exception {
        assertMemoryLeak(() -> {
            try (FileWriter w = new FileWriter(serverConf)) {
                w.write("pg.user=steven\n");
                w.write("pg.password=sklar\n");
                w.write("cairo.legacy.string.column.type.default=true\n"); // non-default value
            }

            try (ServerMain serverMain = new ServerMain(getBootstrap())) {
                serverMain.start();

                Assert.assertTrue(serverMain.getConfiguration().getLineTcpReceiverConfiguration().isUseLegacyStringDefault());

                // remove unsupported property and change some supported property
                try (FileWriter w = new FileWriter(serverConf, false)) {
                    w.write("pg.user=steven\n");
                    w.write("pg.password=foo\n");
                }

                assertReloadConfig(true, "steven", "sklar");

                // unsupported property should stay as it was before reload
                Assert.assertTrue(serverMain.getConfiguration().getLineTcpReceiverConfiguration().isUseLegacyStringDefault());
            }
        });
    }

    @Test
    public void testSqlJitMaxInListSizeThreshold() throws Exception {
        assertMemoryLeak(() -> {
            try (ServerMain serverMain = new ServerMain(getBootstrap())) {
                serverMain.start();

                int oldCapacity = serverMain.getConfiguration().getCairoConfiguration().getSqlJitMaxInListSizeThreshold();
                Assert.assertEquals(10, oldCapacity);

                try (FileWriter w = new FileWriter(serverConf)) {
                    w.write("cairo.sql.jit.max.in.list.size.threshold=1000\n");
                }

                assertReloadConfigEventually();

                int capacity = serverMain.getConfiguration().getCairoConfiguration().getSqlJitMaxInListSizeThreshold();
                Assert.assertEquals(1000, capacity);
            }
        });
    }

    @Test
    public void testUnknownPropertyAdditionIsIgnored() throws Exception {
        assertMemoryLeak(() -> {
            try (FileWriter w = new FileWriter(serverConf)) {
                w.write("query.tracing.enabled=false\n");
            }

            try (ServerMain serverMain = new ServerMain(getBootstrap())) {
                serverMain.start();

                try (FileWriter w = new FileWriter(serverConf, false)) {
                    w.write("query.tracing.enabled=true\n");
                    w.write("foo.bar=baz\n");
                }

                assertReloadConfig(true, "admin", "quest");

                Assert.assertTrue(serverMain.getConfiguration().getCairoConfiguration().isQueryTracingEnabled());
            }
        });
    }

    @Test
    public void testUnknownPropertyRemovalIsIgnored() throws Exception {
        assertMemoryLeak(() -> {
            try (FileWriter w = new FileWriter(serverConf)) {
                w.write("query.tracing.enabled=false\n");
                w.write("foo.bar=baz\n");
            }

            try (ServerMain serverMain = new ServerMain(getBootstrap())) {
                serverMain.start();

                try (FileWriter w = new FileWriter(serverConf, false)) {
                    w.write("query.tracing.enabled=true\n");
                }

                assertReloadConfig(true, "admin", "quest");

                Assert.assertTrue(serverMain.getConfiguration().getCairoConfiguration().isQueryTracingEnabled());
            }
        });
    }

    private static Connection getConnection(String user, String pass) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", user);
        properties.setProperty("password", pass);
        properties.setProperty("connectTimeout", "5");
        properties.setProperty("socketTimeout", "3");
        final String url = String.format("jdbc:postgresql://127.0.0.1:%d/qdb", 8812);
        return DriverManager.getConnection(url, properties);
    }

    private void assertReloadConfig(boolean expectedResult) throws SQLException {
        assertReloadConfig(expectedResult, "admin", "quest");
    }

    private void assertReloadConfig(boolean expectedResult, String user, String password) throws SQLException {
        try (
                Connection conn = getConnection(user, password);
                PreparedStatement stmt = conn.prepareStatement("select reload_config();");
                ResultSet rs = stmt.executeQuery()
        ) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(expectedResult, rs.getBoolean(1));
        }
    }

    private void assertReloadConfigEventually() throws SQLException {
        final long timeoutSeconds = 10;
        long maxSleepingTimeMillis = 1000;
        long nextSleepingTimeMillis = 10;
        long startTime = System.nanoTime();
        long deadline = startTime + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        for (; ; ) {
            try {
                assertReloadConfig(true);
                return;
            } catch (AssertionError error) {
                if (System.nanoTime() >= deadline) {
                    throw error;
                }
            }
            Os.sleep(nextSleepingTimeMillis);
            nextSleepingTimeMillis = Math.min(maxSleepingTimeMillis, nextSleepingTimeMillis << 1);
        }
    }

    private Bootstrap getBootstrap() {
        return new Bootstrap(
                getBootstrapConfig(),
                Bootstrap.getServerMainArgs(root)
        );
    }

    private BootstrapConfiguration getBootstrapConfig() {
        Map<String, String> envMap = new HashMap<>();
        envMap.put(PropertyKey.METRICS_ENABLED.getEnvVarName(), "true");
        envMap.put(PropertyKey.SHARED_WORKER_COUNT.getEnvVarName(), "1");
        envMap.put(PropertyKey.WAL_APPLY_WORKER_COUNT.getEnvVarName(), "1");
        return new DefaultBootstrapConfiguration() {
            @Override
            public ServerConfiguration getServerConfiguration(Bootstrap bootstrap) {
                try {
                    return new DynamicPropServerConfiguration(
                            bootstrap.getRootDirectory(),
                            bootstrap.loadProperties(),
                            envMap,
                            bootstrap.getLog(),
                            bootstrap.getBuildInformation(),
                            FilesFacadeImpl.INSTANCE,
                            bootstrap.getMicrosecondClock(),
                            FactoryProviderFactoryImpl.INSTANCE,
                            true
                    );
                } catch (Exception ex) {
                    Assert.fail(ex.getMessage());
                    return null;
                }
            }
        };
    }
}
