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

import io.questdb.DefaultHttpClientConfiguration;
import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.cutlass.http.client.Fragment;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.cutlass.http.client.Response;
import io.questdb.std.Misc;
import io.questdb.std.ThreadLocal;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.cutlass.pgwire.BasePGTest;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static io.questdb.test.tools.TestUtils.*;
import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.junit.Assert.assertTrue;

public class ViewBootstrapTest extends AbstractBootstrapTest {
    private static final String TABLE1 = "table1";
    private static final String TABLE2 = "table2";
    private static final String VIEW1 = "view1";
    private static final String VIEW2 = "view2";
    private static final ThreadLocal<StringSink> tlSink = new ThreadLocal<>(StringSink::new);
    private boolean isViewEnabled = true;
    private ServerMain questdb;

    @Before
    @Override
    public void setUp() {
        super.setUp();
        startQuestDB();
    }

    @After
    @Override
    public void tearDown() throws Exception {
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
                    VIEW1,
                    HTTP_OK,
                    "{" +
                            "\"query\":\"view1\"," +
                            "\"columns\":[{\"name\":\"ts\",\"type\":\"TIMESTAMP\"},{\"name\":\"k\",\"type\":\"SYMBOL\"},{\"name\":\"v_max\",\"type\":\"LONG\"}]," +
                            "\"timestamp\":-1," +
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
                    VIEW2,
                    HTTP_OK,
                    "{" +
                            "\"query\":\"view2\"," +
                            "\"columns\":[{\"name\":\"ts\",\"type\":\"TIMESTAMP\"},{\"name\":\"k2\",\"type\":\"SYMBOL\"},{\"name\":\"v_max\",\"type\":\"LONG\"}]," +
                            "\"timestamp\":-1," +
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
                    VIEW1,
                    HTTP_OK,
                    "{" +
                            "\"query\":\"view1\"," +
                            "\"columns\":[{\"name\":\"ts\",\"type\":\"TIMESTAMP\"},{\"name\":\"k\",\"type\":\"SYMBOL\"},{\"name\":\"v_max\",\"type\":\"LONG\"}]," +
                            "\"timestamp\":-1," +
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
                    VIEW2,
                    HTTP_OK,
                    "{" +
                            "\"query\":\"view2\"," +
                            "\"columns\":[{\"name\":\"ts\",\"type\":\"TIMESTAMP\"},{\"name\":\"k2\",\"type\":\"SYMBOL\"},{\"name\":\"v_max\",\"type\":\"LONG\"}]," +
                            "\"timestamp\":-1," +
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

        final String query1 = "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 4";
        final String query2 = "select ts, k2, max(v) as v_max from " + TABLE2 + " where v > 6";
        runSqlViaPG(
                "create view " + VIEW1 + " as (" + query1 + ")",
                "create view " + VIEW2 + " as (" + query2 + ")"
        );
        drainWalQueue();

        assertSqlViaPG(
                VIEW1,
                "ts[TIMESTAMP],k[VARCHAR],v_max[BIGINT]\n" +
                        "1970-01-01 00:00:50.0,k5,5\n" +
                        "1970-01-01 00:01:00.0,k6,6\n" +
                        "1970-01-01 00:01:10.0,k7,7\n" +
                        "1970-01-01 00:01:20.0,k8,8\n"
        );
        assertSqlViaPG(
                VIEW2,
                "ts[TIMESTAMP],k2[VARCHAR],v_max[BIGINT]\n" +
                        "1970-01-01 00:01:10.0,k2_7,7\n" +
                        "1970-01-01 00:01:20.0,k2_8,8\n"
        );

        // restart and check that the views are still working
        stopQuestDB();
        startQuestDB();

        assertSqlViaPG(
                VIEW1,
                "ts[TIMESTAMP],k[VARCHAR],v_max[BIGINT]\n" +
                        "1970-01-01 00:00:50.0,k5,5\n" +
                        "1970-01-01 00:01:00.0,k6,6\n" +
                        "1970-01-01 00:01:10.0,k7,7\n" +
                        "1970-01-01 00:01:20.0,k8,8\n"
        );
        assertSqlViaPG(
                VIEW2,
                "ts[TIMESTAMP],k2[VARCHAR],v_max[BIGINT]\n" +
                        "1970-01-01 00:01:10.0,k2_7,7\n" +
                        "1970-01-01 00:01:20.0,k2_8,8\n"
        );
    }

    @Test
    public void testViewsAreDisabled() throws SQLException {
        try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
            createTable(httpClient, TABLE1);
            drainWalQueue();

            final String query1 = "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 4";
            createView(httpClient, VIEW1, query1);
            drainWalQueue();

            assertExecRequest(
                    httpClient,
                    VIEW1,
                    HTTP_OK,
                    "{" +
                            "\"query\":\"view1\"," +
                            "\"columns\":[{\"name\":\"ts\",\"type\":\"TIMESTAMP\"},{\"name\":\"k\",\"type\":\"SYMBOL\"},{\"name\":\"v_max\",\"type\":\"LONG\"}]," +
                            "\"timestamp\":-1," +
                            "\"dataset\":[" +
                            "[\"1970-01-01T00:00:50.000000Z\",\"k5\",5]," +
                            "[\"1970-01-01T00:01:00.000000Z\",\"k6\",6]," +
                            "[\"1970-01-01T00:01:10.000000Z\",\"k7\",7]," +
                            "[\"1970-01-01T00:01:20.000000Z\",\"k8\",8]" +
                            "]," +
                            "\"count\":4" +
                            "}"
            );
        }

        // restart with views disabled
        stopQuestDB();

        isViewEnabled = false;
        startQuestDB();

        // existing view still readable
        assertSqlViaPG(
                VIEW1,
                "ts[TIMESTAMP],k[VARCHAR],v_max[BIGINT]\n" +
                        "1970-01-01 00:00:50.0,k5,5\n" +
                        "1970-01-01 00:01:00.0,k6,6\n" +
                        "1970-01-01 00:01:10.0,k7,7\n" +
                        "1970-01-01 00:01:20.0,k8,8\n"
        );

        // cannot create new view via PG
        final String query2 = "select ts, k2, max(v) as v_max from " + TABLE1 + " where v > 6";
        assertSqlFailureViaPG(
                "create view " + VIEW2 + " as (" + query2 + ")",
                "views are disabled"
        );

        // cannot create new view via HTTP
        try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
            assertExecRequest(
                    httpClient,
                    "create view " + VIEW2 + " as (" + query2 + ")",
                    HTTP_BAD_REQUEST,
                    "{" +
                            "\"query\":\"create view view2 as (select ts, k2, max(v) as v_max from table1 where v > 6)\"," +
                            "\"error\":\"views are disabled\"," +
                            "\"position\":0" +
                            "}"
            );
        }
    }

    private static void assertExecRequest(
            HttpClient httpClient,
            String sql,
            int expectedHttpStatusCode,
            String expectedHttpResponse
    ) {
        final HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
        request.GET().url("/exec").query("query", sql);
        assertHttpRequest(request, expectedHttpStatusCode, expectedHttpResponse);
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

    private static void assertSqlFailureViaPG(String sql, String expectedErrorMessage) {
        try {
            runSqlViaPG(sql);
        } catch (SQLException e) {
            assertContains(e.getMessage(), expectedErrorMessage);
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
        return ServerMain.create(root);
    }

    private static void createTable(HttpClient httpClient, String tableName) {
        assertExecRequest(
                httpClient,
                "create table if not exists " + tableName +
                        " (ts timestamp, k symbol capacity 2048, k2 symbol capacity 512, v long)" +
                        " timestamp(ts) partition by day wal",
                HTTP_OK,
                "{\"ddl\":\"OK\"}"
        );
        for (int i = 0; i < 9; i++) {
            assertExecRequest(
                    httpClient,
                    "insert into " + tableName + " values (" + (i * 10000000) + ", 'k" + i + "', " + "'k2_" + i + "', " + i + ")",
                    HTTP_OK,
                    "{\"dml\":\"OK\"}"
            );
        }
    }

    private static void createView(HttpClient httpClient, String viewName, String viewQuery) {
        assertExecRequest(
                httpClient,
                "create view " + viewName + " as (" + viewQuery + ")",
                HTTP_OK,
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

    private void drainWalQueue() {
        drainWalQueue(questdb.getEngine());
    }

    private void startQuestDB() {
        unchecked(() -> createDummyConfiguration(
                PropertyKey.DEV_MODE_ENABLED + "=true",
                PropertyKey.CAIRO_VIEW_ENABLED + "=" + isViewEnabled,
                PropertyKey.CAIRO_WAL_ENABLED_DEFAULT + "=true"
        ));
        questdb = createServerMain();
        questdb.start();
    }

    private void stopQuestDB() {
        questdb = Misc.free(questdb);
    }
}
