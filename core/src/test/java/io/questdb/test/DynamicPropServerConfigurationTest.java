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

import io.questdb.Bootstrap;
import io.questdb.BootstrapConfiguration;
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
import io.questdb.std.Chars;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.str.StringSink;
import io.questdb.test.cutlass.http.TestHttpClient;
import io.questdb.test.tools.TestUtils;
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

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class DynamicPropServerConfigurationTest extends AbstractTest {
    private File serverConf;

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

                assertReloadConfig(true);

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
                try (TestHttpClient testHttpClient = new TestHttpClient()) {
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

                assertReloadConfig(true);

                // second reload should not reload (no changes)
                assertReloadConfig(false);

                try (TestHttpClient testHttpClient = new TestHttpClient()) {
                    testHttpClient.assertGet(
                            "/exec",
                            "{\"query\":\"select length('qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq');\",\"columns\":[{\"name\":\"length\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[150]],\"count\":1}",
                            query
                    );
                }
            }
        });
    }

    @Test
    public void testHttpSendBufferSizeReload() throws Exception {
        assertMemoryLeak(() -> {
            try (FileWriter w = new FileWriter(serverConf)) {
                w.write("http.net.bind.to=0.0.0.0:9001\n");
                w.write("http.send.buffer.size=100\n");
            }

            try (ServerMain serverMain = new ServerMain(getBootstrap())) {
                serverMain.start();

                final String query = "select rpad('QuestDB', 150, '0');";
                try (TestHttpClient testHttpClient = new TestHttpClient()) {
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

                assertReloadConfig(true);

                try (TestHttpClient testHttpClient = new TestHttpClient()) {
                    testHttpClient.assertGet(
                            "/exec",
                            "{\"query\":\"select rpad('QuestDB', 150, '0');\",\"columns\":[{\"name\":\"rpad\",\"type\":\"STRING\"}],\"timestamp\":-1,\"dataset\":[[\"QuestDB00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000\"]],\"count\":1}",
                            query
                    );
                }
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

                assertReloadConfig(true);

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
                    Assert.assertFalse(conn1.isClosed());

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

                // while configuration was reloaded, metrics must not be reset
                TestUtils.assertEventually(() -> Assert.assertTrue(3 < metrics.pgWireMetrics().listenerStateChangeCounter().getValue()));

                // we should be able to open two connections eventually
                TestUtils.assertEventually(() -> {
                    try (Connection conn1 = getConnection("admin", "quest")) {
                        Assert.assertFalse(conn1.isClosed());

                        try (Connection conn2 = getConnection("admin", "quest")) {
                            Assert.assertFalse(conn2.isClosed());
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
                    Assert.assertFalse(conn.isClosed());
                }

                // Overwrite file to remove props
                try (FileWriter w = new FileWriter(serverConf, false)) {
                    w.write("\n");
                }

                assertReloadConfig(true, "steven", "sklar");

                try (Connection conn = getConnection("admin", "quest")) {
                    Assert.assertFalse(conn.isClosed());
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
                    Assert.assertFalse(conn.isClosed());
                }

                try (FileWriter w = new FileWriter(serverConf)) {
                    w.write("pg.user=nevets\n");
                    w.write("pg.password=ralks\n");
                }

                assertReloadConfig(true, "steven", "sklar");

                try (Connection conn = getConnection("nevets", "ralks")) {
                    Assert.assertFalse(conn.isClosed());
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
                    Assert.assertFalse(conn.isClosed());
                }

                Assert.assertTrue(serverConf.delete());
                Assert.assertTrue(serverConf.createNewFile());

                try (FileWriter w = new FileWriter(serverConf)) {
                    w.write("pg.user=steven\n");
                    w.write("pg.password=sklar\n");
                }

                assertReloadConfig(true);

                try (Connection conn = getConnection("steven", "sklar")) {
                    Assert.assertFalse(conn.isClosed());
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
                    Assert.assertFalse(conn.isClosed());
                }

                try (FileWriter w = new FileWriter(serverConf)) {
                    w.write("pg.user=steven\n");
                    w.write("pg.password=sklar\n");
                }

                assertReloadConfig(true);

                try (Connection conn = getConnection("steven", "sklar")) {
                    Assert.assertFalse(conn.isClosed());
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

                assertReloadConfig(true);

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

                assertReloadConfig(true);

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
                    Assert.assertFalse(conn.isClosed());
                }

                // overwrite file to remove props
                try (FileWriter w = new FileWriter(serverConf, false)) {
                    w.write("\n");
                }

                assertReloadConfig(false, "steven", "sklar");

                try (Connection conn = getConnection("steven", "sklar")) {
                    Assert.assertFalse(conn.isClosed());
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
