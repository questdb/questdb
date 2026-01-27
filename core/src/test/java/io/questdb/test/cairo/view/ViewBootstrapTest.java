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

package io.questdb.test.cairo.view;

import io.questdb.Bootstrap;
import io.questdb.DefaultHttpClientConfiguration;
import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.cairo.view.ViewState;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.cutlass.http.client.Fragment;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.cutlass.http.client.Response;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Misc;
import io.questdb.std.ThreadLocal;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.str.*;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.cutlass.pgwire.BasePGTest;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestMicroClock;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

import static io.questdb.client.Sender.PROTOCOL_VERSION_V2;
import static io.questdb.test.tools.TestUtils.*;
import static io.questdb.test.tools.TestUtils.assertEquals;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.junit.Assert.*;

public class ViewBootstrapTest extends AbstractBootstrapTest {
    private static final String TABLE1 = "table1";
    private static final String TABLE2 = "table2";
    private static final String VIEW1 = "view1";
    private static final String VIEW2 = "view2";
    private static final LogCapture capture = new LogCapture();
    private static final ThreadLocal<StringSink> tlSink = new ThreadLocal<>(StringSink::new);
    private ServerMain questdb;

    private static void assertExecRequest(
            HttpClient httpClient,
            String sql,
            String expectedHttpResponse
    ) {
        final HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
        request.GET().url("/exec").query("query", sql);
        assertHttpRequest(request, HTTP_OK, expectedHttpResponse);
    }

    private static void assertHttpRequest(
            HttpClient.Request request,
            int expectedHttpStatusCode,
            String expectedHttpResponse,
            String... expectedHeaders
    ) {
        try (HttpClient.ResponseHeaders responseHeaders = request.send()) {
            responseHeaders.await();

            assertEquals(String.valueOf(expectedHttpStatusCode), responseHeaders.getStatusCode());

            for (int i = 0; i < expectedHeaders.length; i += 2) {
                final Utf8Sequence value = responseHeaders.getHeader(new Utf8String(expectedHeaders[i]));
                assertTrue(Utf8s.equals(new Utf8String(expectedHeaders[i + 1]), value));
            }

            final StringSink sink = tlSink.get();

            Fragment fragment;
            final Response chunkedResponse = responseHeaders.getResponse();
            while ((fragment = chunkedResponse.recv()) != null) {
                Utf8s.utf8ToUtf16(fragment.lo(), fragment.hi(), sink);
            }

            TestUtils.assertEquals(expectedHttpResponse, sink);
            sink.clear();
        }
    }

    private static void assertSqlViaPG(String sql, String expectedResult) throws SQLException {
        try (
                final Connection connection = getPGConnection();
                final PreparedStatement stmt = connection.prepareStatement(sql);
                final ResultSet resultSet = stmt.executeQuery()
        ) {
            final StringSink sink = Misc.getThreadLocalSink();
            sink.clear();

            BasePGTest.assertResultSet(expectedResult, sink, resultSet);
        }
    }

    private static ServerMain createServerMain() {
        return new ServerMain(new Bootstrap(Bootstrap.getServerMainArgs(root)) {
            @Override
            public MicrosecondClock getMicrosecondClock() {
                return new TestMicroClock(1750345200000000L, 0L);
            }
        }) {
            @Override
            protected void setupViewJobs(WorkerPool workerPool, CairoEngine engine, int sharedWorkerCount) {
            }

            @Override
            protected void setupWalApplyJob(WorkerPool workerPool, CairoEngine engine, int sharedWorkerCount) {
            }
        };
    }

    private static void createTable(HttpClient httpClient, String tableName) {
        assertExecRequest(
                httpClient,
                "create table if not exists " + tableName +
                        " (ts timestamp, k symbol capacity 2048, k2 symbol capacity 512, v long)" +
                        " timestamp(ts) partition by day wal",
                "{\"ddl\":\"OK\"}"
        );
        for (int i = 0; i < 9; i++) {
            assertExecRequest(
                    httpClient,
                    "insert into " + tableName + " values (" + (i * 10000000) + ", 'k" + i + "', " + "'k2_" + i + "', " + i + ")",
                    "{\"dml\":\"OK\"}"
            );
        }
    }

    private static void createView(HttpClient httpClient, String viewName, String viewQuery) {
        assertExecRequest(
                httpClient,
                "create view " + viewName + " as (" + viewQuery + ")",
                "{\"ddl\":\"OK\"}"
        );
    }

    private static void executeViaPG(Connection conn, String sql, String... bindVars) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            for (int i = 0; i < bindVars.length; i++) {
                stmt.setString(i + 1, bindVars[i]);
            }
            stmt.execute();
        }
    }

    private static Connection getPGConnection() throws SQLException {
        return DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
    }

    private static void runSqlViaPG(String... sqls) throws SQLException {
        try (final Connection connection = getPGConnection()) {
            for (String sql : sqls) {
                executeViaPG(connection, sql);
            }
        }
    }

    @Before
    @Override
    public void setUp() {
        super.setUp();
        startQuestDB();
        capture.start();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        capture.stop();
        stopQuestDB();
        super.tearDown();
    }

    @Test
    public void testHttp() {
        try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
            createTable(httpClient, TABLE1);
            createTable(httpClient, TABLE2);
            drainWalQueue();

            final String query1 = "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 4";
            createView(httpClient, VIEW1, query1);
            drainWalQueue();

            final String query2 = "select ts, k2, max(v) as v_max from " + TABLE2 + " where v > 6";
            createView(httpClient, VIEW2, query2);
            drainWalQueue();

            assertExecRequest(
                    httpClient,
                    VIEW1 + " order by ts",
                    "{" +
                            "\"query\":\"view1 order by ts\"," +
                            "\"columns\":[{\"name\":\"ts\",\"type\":\"TIMESTAMP\"},{\"name\":\"k\",\"type\":\"SYMBOL\"},{\"name\":\"v_max\",\"type\":\"LONG\"}]," +
                            "\"timestamp\":0," +
                            "\"dataset\":[" +
                            "[\"1970-01-01T00:00:50.000000Z\",\"k5\",5]," +
                            "[\"1970-01-01T00:01:00.000000Z\",\"k6\",6]," +
                            "[\"1970-01-01T00:01:10.000000Z\",\"k7\",7]," +
                            "[\"1970-01-01T00:01:20.000000Z\",\"k8\",8]" +
                            "]," +
                            "\"count\":4" +
                            "}"
            );

            assertExecRequest(
                    httpClient,
                    VIEW2 + " order by ts",
                    "{" +
                            "\"query\":\"view2 order by ts\"," +
                            "\"columns\":[{\"name\":\"ts\",\"type\":\"TIMESTAMP\"},{\"name\":\"k2\",\"type\":\"SYMBOL\"},{\"name\":\"v_max\",\"type\":\"LONG\"}]," +
                            "\"timestamp\":0," +
                            "\"dataset\":[" +
                            "[\"1970-01-01T00:01:10.000000Z\",\"k2_7\",7]," +
                            "[\"1970-01-01T00:01:20.000000Z\",\"k2_8\",8]" +
                            "]," +
                            "\"count\":2" +
                            "}"
            );
        }

        // restart and check that the views are still working
        stopQuestDB();
        startQuestDB();

        try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
            assertExecRequest(
                    httpClient,
                    VIEW1 + " order by ts",
                    "{" +
                            "\"query\":\"view1 order by ts\"," +
                            "\"columns\":[{\"name\":\"ts\",\"type\":\"TIMESTAMP\"},{\"name\":\"k\",\"type\":\"SYMBOL\"},{\"name\":\"v_max\",\"type\":\"LONG\"}]," +
                            "\"timestamp\":0," +
                            "\"dataset\":[" +
                            "[\"1970-01-01T00:00:50.000000Z\",\"k5\",5]," +
                            "[\"1970-01-01T00:01:00.000000Z\",\"k6\",6]," +
                            "[\"1970-01-01T00:01:10.000000Z\",\"k7\",7]," +
                            "[\"1970-01-01T00:01:20.000000Z\",\"k8\",8]" +
                            "]," +
                            "\"count\":4" +
                            "}"
            );

            assertExecRequest(
                    httpClient,
                    VIEW2 + " order by ts",
                    "{" +
                            "\"query\":\"view2 order by ts\"," +
                            "\"columns\":[{\"name\":\"ts\",\"type\":\"TIMESTAMP\"},{\"name\":\"k2\",\"type\":\"SYMBOL\"},{\"name\":\"v_max\",\"type\":\"LONG\"}]," +
                            "\"timestamp\":0," +
                            "\"dataset\":[" +
                            "[\"1970-01-01T00:01:10.000000Z\",\"k2_7\",7]," +
                            "[\"1970-01-01T00:01:20.000000Z\",\"k2_8\",8]" +
                            "]," +
                            "\"count\":2" +
                            "}"
            );
        }
    }

    @Test
    public void testHttpOverIlpCannotIngestIntoView() throws Exception {
        runSqlViaPG(
                "create table prices (" +
                        "sym varchar, price double, ts timestamp" +
                        ") timestamp(ts) partition by day wal"
        );
        runSqlViaPG(
                "insert into prices values ('sym1', 1.12, 10000000)",
                "insert into prices values ('sym1', 1.33, 30000000)"
        );
        drainWalQueue();

        runSqlViaPG(
                "create view instruments as (select distinct sym from prices)"
        );
        drainWalQueue();
        drainViewQueue();

        try (Sender sender = Sender.builder("http::addr=localhost:" + HTTP_PORT).protocolVersion(PROTOCOL_VERSION_V2).build()) {
            sender.table("instruments")
                    .stringColumn("sym", "gbpusd")
                    .atNow();
            sender.flush();
            fail("exception expected");
        } catch (LineSenderException e) {
            assertContains(e.getMessage(), "cannot modify view");
        }
    }

    @Test
    public void testPGWire() throws SQLException {
        runSqlViaPG(
                "create table if not exists " + TABLE1 +
                        " (ts timestamp, k symbol capacity 2048, k2 symbol capacity 512, v long)" +
                        " timestamp(ts) partition by day wal",
                "create table if not exists " + TABLE2 +
                        " (ts timestamp, k symbol capacity 2048, k2 symbol capacity 512, v long)" +
                        " timestamp(ts) partition by day wal"
        );
        for (int i = 0; i < 9; i++) {
            runSqlViaPG(
                    "insert into " + TABLE1 + " values (" + (i * 10000000) + ", 'k" + i + "', " + "'k2_" + i + "', " + i + ")",
                    "insert into " + TABLE2 + " values (" + (i * 10000000) + ", 'k" + i + "', " + "'k2_" + i + "', " + i + ")"
            );
        }
        drainWalQueue();

        final String query1 = "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 4 order by ts";
        final String query2 = "select ts, k2, max(v) as v_max from " + TABLE2 + " where v > 6 order by ts";
        runSqlViaPG(
                "create view " + VIEW1 + " as (" + query1 + ")",
                "create view " + VIEW2 + " as (" + query2 + ")"
        );
        drainWalQueue();

        assertSqlViaPG(
                VIEW1,
                """
                        ts[TIMESTAMP],k[VARCHAR],v_max[BIGINT]
                        1970-01-01 00:00:50.0,k5,5
                        1970-01-01 00:01:00.0,k6,6
                        1970-01-01 00:01:10.0,k7,7
                        1970-01-01 00:01:20.0,k8,8
                        """
        );
        assertSqlViaPG(
                VIEW2,
                """
                        ts[TIMESTAMP],k2[VARCHAR],v_max[BIGINT]
                        1970-01-01 00:01:10.0,k2_7,7
                        1970-01-01 00:01:20.0,k2_8,8
                        """
        );

        // restart and check that the views are still working
        stopQuestDB();
        startQuestDB();

        assertSqlViaPG(
                VIEW1,
                """
                        ts[TIMESTAMP],k[VARCHAR],v_max[BIGINT]
                        1970-01-01 00:00:50.0,k5,5
                        1970-01-01 00:01:00.0,k6,6
                        1970-01-01 00:01:10.0,k7,7
                        1970-01-01 00:01:20.0,k8,8
                        """
        );
        assertSqlViaPG(
                VIEW2,
                """
                        ts[TIMESTAMP],k2[VARCHAR],v_max[BIGINT]
                        1970-01-01 00:01:10.0,k2_7,7
                        1970-01-01 00:01:20.0,k2_8,8
                        """
        );
    }

    @Test
    public void testTcpOverIlpCannotIngestIntoView() throws Exception {
        runSqlViaPG(
                "create table prices (" +
                        "sym varchar, price double, ts timestamp" +
                        ") timestamp(ts) partition by day wal"
        );
        runSqlViaPG(
                "insert into prices values ('sym1', 1.12, 10000000)",
                "insert into prices values ('sym1', 1.33, 30000000)"
        );
        drainWalQueue();

        runSqlViaPG(
                "create view instruments as (select distinct sym from prices)"
        );
        drainWalQueue();
        drainViewQueue();

        try (Sender sender = Sender.builder("tcp::addr=localhost:" + ILP_PORT).protocolVersion(PROTOCOL_VERSION_V2).build()) {
            sender.table("instruments")
                    .stringColumn("sym", "gbpusd")
                    .atNow();
            sender.flush();
        }

        // TCP disconnects the client when error is detected, and logs the error
        capture.waitFor("could not process line data 1 [table=instruments, msg=cannot modify view [view=instruments], errno=-1]");
    }

    @Test
    public void testViewStateAfterRestart() {
        final String query1 = "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 4";
        final String query2 = "select ts, k2, min(v) as v_min from " + TABLE2 + " where v > 6";

        try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
            createTable(httpClient, TABLE1);
            createTable(httpClient, TABLE2);
            drainWalQueue();

            createView(httpClient, VIEW1, query1);
            createView(httpClient, VIEW2, query2);
            drainWalQueue();
            drainViewQueue();

            final ViewState state1 = getViewState(VIEW1);
            final ViewState state2 = getViewState(VIEW2);

            assertViewState(VALID, state1);
            assertViewState(VALID, state2);

            final ViewDefinition definition1 = getViewDefinition(VIEW1);
            final ViewDefinition definition2 = getViewDefinition(VIEW2);

            assertNotNull(definition1);
            Assert.assertEquals(6, definition1.getViewToken().getTableId());
            Assert.assertEquals(VIEW1, definition1.getViewToken().getTableName());
            Assert.assertEquals(0, definition1.getSeqTxn());
            assertEquals(query1, definition1.getViewSql());
            Assert.assertEquals(1, definition1.getDependencies().size());
            assertTrue(definition1.getDependencies().contains(TABLE1));
            // Check that column dependencies are collected for view1 (ts, k, v from table1)
            Assert.assertEquals(3, definition1.getDependencies().get(TABLE1).size());
            Assert.assertTrue(definition1.getDependencies().get(TABLE1).contains("ts"));
            Assert.assertTrue(definition1.getDependencies().get(TABLE1).contains("k"));
            Assert.assertTrue(definition1.getDependencies().get(TABLE1).contains("v"));

            assertNotNull(definition2);
            Assert.assertEquals(7, definition2.getViewToken().getTableId());
            Assert.assertEquals(VIEW2, definition2.getViewToken().getTableName());
            Assert.assertEquals(0, definition2.getSeqTxn());
            assertEquals(query2, definition2.getViewSql());
            Assert.assertEquals(1, definition2.getDependencies().size());
            assertTrue(definition2.getDependencies().contains(TABLE2));
            // Check that column dependencies are collected for view2 (ts, k2, v from table2)
            Assert.assertEquals(3, definition2.getDependencies().get(TABLE2).size());
            Assert.assertTrue(definition2.getDependencies().get(TABLE2).contains("ts"));
            Assert.assertTrue(definition2.getDependencies().get(TABLE2).contains("k2"));
            Assert.assertTrue(definition2.getDependencies().get(TABLE2).contains("v"));

            assertExecRequest(
                    httpClient,
                    "views()",
                    "{" +
                            "\"query\":\"views()\"," +
                            "\"columns\":[" +
                            "{\"name\":\"view_name\",\"type\":\"STRING\"}," +
                            "{\"name\":\"view_sql\",\"type\":\"STRING\"}," +
                            "{\"name\":\"view_table_dir_name\",\"type\":\"STRING\"}," +
                            "{\"name\":\"invalidation_reason\",\"type\":\"STRING\"}," +
                            "{\"name\":\"view_status\",\"type\":\"STRING\"}," +
                            "{\"name\":\"view_status_update_time\",\"type\":\"TIMESTAMP\"}" +
                            "]," +
                            "\"timestamp\":-1," +
                            "\"dataset\":[" +
                            "[\"view2\",\"select ts, k2, min(v) as v_min from table2 where v > 6\",\"view2~7\",null,\"valid\",\"2025-06-19T15:00:00.000000Z\"]," +
                            "[\"view1\",\"select ts, k, max(v) as v_max from table1 where v > 4\",\"view1~6\",null,\"valid\",\"2025-06-19T15:00:00.000000Z\"]" +
                            "]," +
                            "\"count\":2" +
                            "}"
            );

            assertExecRequest(
                    httpClient,
                    "alter table " + TABLE1 + " drop column k",
                    "{\"ddl\":\"OK\"}"
            );
            drainWalQueue();
            drainViewQueue();

            assertViewState(INVALID, state1);
            assertViewState(VALID, state2);

            assertExecRequest(
                    httpClient,
                    "views()",
                    "{" +
                            "\"query\":\"views()\"," +
                            "\"columns\":[" +
                            "{\"name\":\"view_name\",\"type\":\"STRING\"}," +
                            "{\"name\":\"view_sql\",\"type\":\"STRING\"}," +
                            "{\"name\":\"view_table_dir_name\",\"type\":\"STRING\"}," +
                            "{\"name\":\"invalidation_reason\",\"type\":\"STRING\"}," +
                            "{\"name\":\"view_status\",\"type\":\"STRING\"}," +
                            "{\"name\":\"view_status_update_time\",\"type\":\"TIMESTAMP\"}" +
                            "]," +
                            "\"timestamp\":-1," +
                            "\"dataset\":[" +
                            "[\"view2\",\"select ts, k2, min(v) as v_min from table2 where v > 6\",\"view2~7\",null,\"valid\",\"2025-06-19T15:00:00.000000Z\"]," +
                            "[\"view1\",\"select ts, k, max(v) as v_max from table1 where v > 4\",\"view1~6\",\"Invalid column: k\",\"invalid\",\"2025-06-19T15:00:00.000000Z\"]" +
                            "]," +
                            "\"count\":2" +
                            "}"
            );
        }

        // restart, and assert that view state is the same
        stopQuestDB();
        startQuestDB();

        drainWalQueue();
        drainViewQueue();

        try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
            final ViewState state1 = getViewState(VIEW1);
            final ViewState state2 = getViewState(VIEW2);

            assertViewState(INVALID, state1);
            assertViewState(VALID, state2);

            final ViewDefinition definition1 = getViewDefinition(VIEW1);
            final ViewDefinition definition2 = getViewDefinition(VIEW2);

            assertNotNull(definition1);
            Assert.assertEquals(6, definition1.getViewToken().getTableId());
            Assert.assertEquals(VIEW1, definition1.getViewToken().getTableName());
            Assert.assertEquals(0, definition1.getSeqTxn());
            assertEquals(query1, definition1.getViewSql());
            Assert.assertEquals(1, definition1.getDependencies().size());
            assertTrue(definition1.getDependencies().contains(TABLE1));
            // Check that column dependencies are collected for view1 (ts, k, v from table1)
            Assert.assertEquals(3, definition1.getDependencies().get(TABLE1).size());
            Assert.assertTrue(definition1.getDependencies().get(TABLE1).contains("ts"));
            Assert.assertTrue(definition1.getDependencies().get(TABLE1).contains("k"));
            Assert.assertTrue(definition1.getDependencies().get(TABLE1).contains("v"));

            assertNotNull(definition2);
            Assert.assertEquals(7, definition2.getViewToken().getTableId());
            Assert.assertEquals(VIEW2, definition2.getViewToken().getTableName());
            Assert.assertEquals(0, definition2.getSeqTxn());
            assertEquals(query2, definition2.getViewSql());
            Assert.assertEquals(1, definition2.getDependencies().size());
            assertTrue(definition2.getDependencies().contains(TABLE2));
            // Check that column dependencies are collected for view2 (ts, k2, v from table2)
            Assert.assertEquals(3, definition2.getDependencies().get(TABLE2).size());
            Assert.assertTrue(definition2.getDependencies().get(TABLE2).contains("ts"));
            Assert.assertTrue(definition2.getDependencies().get(TABLE2).contains("k2"));
            Assert.assertTrue(definition2.getDependencies().get(TABLE2).contains("v"));

            assertExecRequest(
                    httpClient,
                    "views()",
                    "{" +
                            "\"query\":\"views()\"," +
                            "\"columns\":[" +
                            "{\"name\":\"view_name\",\"type\":\"STRING\"}," +
                            "{\"name\":\"view_sql\",\"type\":\"STRING\"}," +
                            "{\"name\":\"view_table_dir_name\",\"type\":\"STRING\"}," +
                            "{\"name\":\"invalidation_reason\",\"type\":\"STRING\"}," +
                            "{\"name\":\"view_status\",\"type\":\"STRING\"}," +
                            "{\"name\":\"view_status_update_time\",\"type\":\"TIMESTAMP\"}" +
                            "]," +
                            "\"timestamp\":-1," +
                            "\"dataset\":[" +
                            "[\"view2\",\"select ts, k2, min(v) as v_min from table2 where v > 6\",\"view2~7\",null,\"valid\",\"2025-06-19T15:00:00.000000Z\"]," +
                            "[\"view1\",\"select ts, k, max(v) as v_max from table1 where v > 4\",\"view1~6\",\"Invalid column: k\",\"invalid\",\"2025-06-19T15:00:00.000000Z\"]" +
                            "]," +
                            "\"count\":2" +
                            "}"
            );
        }

        // restart without tables.d file, and assert that view state is the same
        final CairoConfiguration configuration = questdb.getEngine().getConfiguration();
        try (Path path = new Path().of(configuration.getDbRoot()).concat("tables.d.0")) {
            configuration.getFilesFacade().remove(path.$());
        }

        stopQuestDB();
        startQuestDB();

        drainWalQueue();
        drainViewQueue();

        try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
            final ViewState state1 = getViewState(VIEW1);
            final ViewState state2 = getViewState(VIEW2);

            assertViewState(INVALID, state1);
            assertViewState(VALID, state2);

            final ViewDefinition definition1 = getViewDefinition(VIEW1);
            final ViewDefinition definition2 = getViewDefinition(VIEW2);

            assertNotNull(definition1);
            Assert.assertEquals(6, definition1.getViewToken().getTableId());
            Assert.assertEquals(VIEW1, definition1.getViewToken().getTableName());
            Assert.assertEquals(0, definition1.getSeqTxn());
            assertEquals(query1, definition1.getViewSql());
            Assert.assertEquals(1, definition1.getDependencies().size());
            assertTrue(definition1.getDependencies().contains(TABLE1));
            // Check that column dependencies are collected for view1 (ts, k, v from table1)
            Assert.assertEquals(3, definition1.getDependencies().get(TABLE1).size());
            Assert.assertTrue(definition1.getDependencies().get(TABLE1).contains("ts"));
            Assert.assertTrue(definition1.getDependencies().get(TABLE1).contains("k"));
            Assert.assertTrue(definition1.getDependencies().get(TABLE1).contains("v"));

            assertNotNull(definition2);
            Assert.assertEquals(7, definition2.getViewToken().getTableId());
            Assert.assertEquals(VIEW2, definition2.getViewToken().getTableName());
            Assert.assertEquals(0, definition2.getSeqTxn());
            assertEquals(query2, definition2.getViewSql());
            Assert.assertEquals(1, definition2.getDependencies().size());
            assertTrue(definition2.getDependencies().contains(TABLE2));
            // Check that column dependencies are collected for view2 (ts, k2, v from table2)
            Assert.assertEquals(3, definition2.getDependencies().get(TABLE2).size());
            Assert.assertTrue(definition2.getDependencies().get(TABLE2).contains("ts"));
            Assert.assertTrue(definition2.getDependencies().get(TABLE2).contains("k2"));
            Assert.assertTrue(definition2.getDependencies().get(TABLE2).contains("v"));

            assertExecRequest(
                    httpClient,
                    "views()",
                    "{" +
                            "\"query\":\"views()\"," +
                            "\"columns\":[" +
                            "{\"name\":\"view_name\",\"type\":\"STRING\"}," +
                            "{\"name\":\"view_sql\",\"type\":\"STRING\"}," +
                            "{\"name\":\"view_table_dir_name\",\"type\":\"STRING\"}," +
                            "{\"name\":\"invalidation_reason\",\"type\":\"STRING\"}," +
                            "{\"name\":\"view_status\",\"type\":\"STRING\"}," +
                            "{\"name\":\"view_status_update_time\",\"type\":\"TIMESTAMP\"}" +
                            "]," +
                            "\"timestamp\":-1," +
                            "\"dataset\":[" +
                            "[\"view2\",\"select ts, k2, min(v) as v_min from table2 where v > 6\",\"view2~7\",null,\"valid\",\"2025-06-19T15:00:00.000000Z\"]," +
                            "[\"view1\",\"select ts, k, max(v) as v_max from table1 where v > 4\",\"view1~6\",\"Invalid column: k\",\"invalid\",\"2025-06-19T15:00:00.000000Z\"]" +
                            "]," +
                            "\"count\":2" +
                            "}"
            );
        }
    }

    private void drainViewQueue() {
        drainWalAndViewQueues(questdb.getEngine());
    }

    private void drainWalQueue() {
        drainWalQueue(questdb.getEngine());
    }

    private ViewDefinition getViewDefinition(CharSequence viewName) {
        final CairoEngine engine = questdb.getEngine();
        final TableToken viewToken = engine.getTableTokenIfExists(viewName);
        return engine.getViewGraph().getViewDefinition(viewToken);
    }

    private ViewState getViewState(CharSequence viewName) {
        final CairoEngine engine = questdb.getEngine();
        final TableToken viewToken = engine.getTableTokenIfExists(viewName);
        return engine.getViewStateStore().getViewState(viewToken);
    }

    private void startQuestDB() {
        unchecked(() -> createDummyConfiguration(
                PropertyKey.DEV_MODE_ENABLED + "=true",
                PropertyKey.CAIRO_WAL_ENABLED_DEFAULT + "=true"
        ));
        questdb = createServerMain();
        questdb.start();
        questdb.awaitStartup();
    }

    private void stopQuestDB() {
        questdb = Misc.free(questdb);
    }
}
