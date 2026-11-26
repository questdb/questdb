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

import io.questdb.Bootstrap;
import io.questdb.DynamicPropServerConfiguration;
import io.questdb.FactoryProviderImpl;
import io.questdb.PropBootstrapConfiguration;
import io.questdb.PropertyKey;
import io.questdb.ServerConfiguration;
import io.questdb.cairo.TableWriter;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.cutlass.http.WaitProcessor;
import io.questdb.cutlass.qwp.codec.QwpEgressMsgKind;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.server.egress.QwpEgressMetrics;
import io.questdb.cutlass.qwp.websocket.WebSocketOpcode;
import io.questdb.log.Log;
import io.questdb.network.NetworkFacade;
import io.questdb.network.PlainSocket;
import io.questdb.network.PlainSocketFactory;
import io.questdb.network.SocketFactory;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.cutlass.qwp.QwpWireTestFixtures;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

/**
 * A client that sends the close connection option reads the response until the
 * connection ends, so the HTTP servers close the connection once they have sent
 * the response, and say so in its Connection header (RFC 9112, 9.6).
 */
public class HttpConnectionCloseTest extends AbstractBootstrapTest {
    private static final String CONNECTION_CLOSE = "Connection: close\r\n";
    private static final String DATE = "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n";
    private static final String MULTIPART_BOUNDARY = "------------------------27d997ca93d2689d";
    private static final String SELECT_ONE_JSON = """
            {"query":"SELECT 1","columns":[{"name":"1","type":"INT"}],"timestamp":-1,"dataset":[[1]],"count":1}""";

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testConnectionClosedAfterParkedResponse() throws Exception {
        // the server sends the response a few bytes at a time and parks between sends, so
        // the request completes when the last send resumes, not when it is first served
        final String json = longSequenceJson("100", 100);
        assertMemoryLeak(() -> {
            try (TestServerMain ignore = startWithEnvVariables(
                    PropertyKey.DEBUG_HTTP_FORCE_SEND_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), "16",
                    PropertyKey.DEBUG_HTTP_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), "5"
            )) {
                assertExchangesThenClose(
                        HTTP_PORT,
                        longSequenceRequest("100", ""), jsonResponse("", json),
                        longSequenceRequest("100", CONNECTION_CLOSE), jsonResponse(CONNECTION_CLOSE, json)
                );
            }
        });
    }

    @Test
    public void testConnectionClosedAfterParkedRetriedResponse() throws Exception {
        // the attempt that completes the request sends the response a few bytes at a time and parks
        // between sends, and the connection closes once the last send has resumed
        assertRetriedRequestClosesConnection(PlainSocketFactory.INSTANCE, 4);
    }

    @Test
    public void testConnectionClosedAfterRequestReadInSamePass() throws Exception {
        // The next request arrives while the server still sends the long response before it, a
        // fragment at a time. Once that response is out, the server reads the next request in the
        // same pass, answers it in one send, and closes the connection right after.
        final String longJson = longSequenceJson("10_000", 10_000);
        assertMemoryLeak(() -> {
            try (
                    TestServerMain ignore = startWithEnvVariables(
                            PropertyKey.DEBUG_HTTP_FORCE_SEND_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), "512"
                    );
                    Socket socket = new Socket("127.0.0.1", HTTP_PORT)
            ) {
                socket.setSoTimeout(10_000);
                final OutputStream out = socket.getOutputStream();
                final InputStream in = socket.getInputStream();
                out.write(longSequenceRequest("10_000", "").getBytes(StandardCharsets.US_ASCII));
                // the first byte of the long response shows the server has read its request
                Assert.assertEquals('H', in.read());
                out.write(getExec("SELECT%201", CONNECTION_CLOSE).getBytes(StandardCharsets.US_ASCII));

                final String longHeaders = "H" + QwpWireTestFixtures.readHttpHeaders(in);
                Assert.assertEquals(jsonResponse("", longJson), longHeaders + QwpWireTestFixtures.readChunkedBody(in, longHeaders));
                final String headers = QwpWireTestFixtures.readHttpHeaders(in);
                Assert.assertEquals(jsonResponse(CONNECTION_CLOSE, SELECT_ONE_JSON), headers + QwpWireTestFixtures.readChunkedBody(in, headers));
                assertClosedByServer(in);
            }
        });
    }

    @Test
    public void testConnectionClosedAfterResponse() throws Exception {
        // each request on a connection of its own, through each way the server reads a request
        assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startWithEnvVariables()) {
                assertEachRequestClosesConnection(serverMain);
            }
        });
    }

    @Test
    public void testConnectionClosedAfterRetriedRequest() throws Exception {
        assertRetriedRequestClosesConnection(PlainSocketFactory.INSTANCE, Integer.MAX_VALUE);
    }

    @Test
    public void testConnectionClosedAfterRetriedRequestOnceTlsSessionSentResponse() throws Exception {
        // the attempt that completes the request runs on the thread that retries requests, and the
        // connection waits there for the session to send the response before it closes
        assertRetriedRequestClosesConnection(TlsSessionSocket::new, Integer.MAX_VALUE);
    }

    @Test
    public void testConnectionClosedOnceTlsSessionSentResponse() throws Exception {
        // An encrypted socket reports a send complete while its TLS session still holds records of
        // it. The server closes a connection only once the session has sent them all, so the client
        // gets each response whole, also one longer than the session can hold, and one that follows
        // responses the session sent while the connection stayed open.
        final String json = longSequenceJson("10_000", 10_000);
        assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startWithHttpSocketFactory(TlsSessionSocket::new)) {
                assertEachRequestClosesConnection(serverMain);
                assertExchangesThenClose(
                        HTTP_PORT,
                        getExec("SELECT%201", ""), jsonResponse("", SELECT_ONE_JSON),
                        ilpWithContentLength("kept_open v=1i\n", ""), noContentResponse(""),
                        longSequenceRequest("10_000", CONNECTION_CLOSE), jsonResponse(CONNECTION_CLOSE, json)
                );
            }
        });
    }

    @Test
    public void testConnectionClosedOnceTlsSessionSentResponseWithoutKeepAlive() throws Exception {
        // a server that keeps no connection alive also waits for the session to send the response
        final String json = longSequenceJson("10_000", 10_000);
        assertMemoryLeak(() -> {
            try (TestServerMain ignore = startWithHttpSocketFactory(
                    TlsSessionSocket::new,
                    PropertyKey.HTTP_SERVER_KEEP_ALIVE.getEnvVarName(), "false"
            )) {
                assertExchangesThenClose(HTTP_PORT, longSequenceRequest("10_000", ""), jsonResponse(CONNECTION_CLOSE, json));
                assertExchangesThenClose(HTTP_PORT, ilpWithContentLength("no_keep_alive v=1i\n", ""), noContentResponse());
            }
        });
    }

    @Test
    public void testConnectionClosedWhenTlsSessionFailsToSend() throws Exception {
        // The session fails to send the records it holds, as it does once the peer has gone. The
        // server ends the connection then, rather than waiting until the socket takes them.
        final int responseLength = jsonResponse(CONNECTION_CLOSE, longSequenceJson("10_000", 10_000)).length();
        assertMemoryLeak(() -> {
            try (
                    TestServerMain ignore = startWithHttpSocketFactory(FailingTlsSessionSocket::new);
                    Socket socket = new Socket("127.0.0.1", HTTP_PORT)
            ) {
                socket.setSoTimeout(10_000);
                socket.getOutputStream().write(longSequenceRequest("10_000", CONNECTION_CLOSE).getBytes(StandardCharsets.US_ASCII));
                final InputStream in = socket.getInputStream();
                int received = 0;
                try {
                    while (in.read() > -1) {
                        received++;
                    }
                } catch (SocketTimeoutException e) {
                    Assert.fail("the server kept the connection open after the session failed to send");
                }
                // the client gets what the sends wrote, and the session lost the rest
                Assert.assertTrue("received " + received + " bytes", received > 0);
                Assert.assertTrue("received " + received + " of " + responseLength + " bytes", received < responseLength);
            }
        });
    }

    @Test
    public void testConnectionClosedWithoutKeepAlive() throws Exception {
        // a server that keeps no connection alive closes every one, and says so once
        assertMemoryLeak(() -> {
            try (TestServerMain ignore = startWithEnvVariables(PropertyKey.HTTP_SERVER_KEEP_ALIVE.getEnvVarName(), "false")) {
                assertExchangesThenClose(HTTP_PORT, getExec("SELECT%201", ""), jsonResponse(CONNECTION_CLOSE, SELECT_ONE_JSON));
                assertExchangesThenClose(HTTP_PORT, getExec("SELECT%201", CONNECTION_CLOSE), jsonResponse(CONNECTION_CLOSE, SELECT_ONE_JSON));
            }
        });
    }

    @Test
    public void testConnectionKeptOpenUntilRequestAsksToClose() throws Exception {
        assertMemoryLeak(() -> {
            try (TestServerMain ignore = startWithEnvVariables()) {
                assertExchangesThenClose(
                        HTTP_PORT,
                        getExec("SELECT%201", ""), jsonResponse("", SELECT_ONE_JSON),
                        getExec("SELECT%201", "Connection: keep-alive\r\n"), jsonResponse("", SELECT_ONE_JSON),
                        ilpWithContentLength("kept_open v=1i\n", ""), noContentResponse(""),
                        getExec("SELECT%201", "Connection: Keep-Alive, Close\r\n"), jsonResponse(CONNECTION_CLOSE, SELECT_ONE_JSON)
                );
            }
        });
    }

    @Test
    public void testUpgradeServesWebSocketDespiteCloseOption() throws Exception {
        // the 101 answers the upgrade request, and the close option of HTTP does not apply
        // to the WebSocket protocol the connection switches to
        assertMemoryLeak(() -> {
            try (
                    TestServerMain ignore = startWithEnvVariables();
                    Socket socket = new Socket("127.0.0.1", HTTP_PORT)
            ) {
                socket.setSoTimeout(10_000);
                final OutputStream out = socket.getOutputStream();
                final InputStream in = socket.getInputStream();
                out.write(("GET /read/v1 HTTP/1.1\r\n" +
                        "Host: localhost:" + HTTP_PORT + "\r\n" +
                        "Upgrade: websocket\r\n" +
                        "Connection: Upgrade, close\r\n" +
                        "Sec-WebSocket-Key: " + QwpWireTestFixtures.WEBSOCKET_KEY + "\r\n" +
                        "Sec-WebSocket-Version: 13\r\n" +
                        "\r\n").getBytes(StandardCharsets.US_ASCII));
                final String response = QwpWireTestFixtures.readHttpHeaders(in);
                Assert.assertTrue(response, response.startsWith("HTTP/1.1 101 Switching Protocols\r\n"));

                // SERVER_INFO is pushed on connect, and a query gets its result
                QwpWireTestFixtures.readServerFrame(in);
                out.write(QwpWireTestFixtures.maskedFrame(WebSocketOpcode.BINARY, QwpWireTestFixtures.buildQueryRequest(1L, "SELECT 1")));
                out.flush();
                byte kind;
                int frames = 0;
                do {
                    final byte[] frame = QwpWireTestFixtures.readServerFrame(in);
                    Assert.assertTrue("truncated QWP message", frame.length > QwpConstants.HEADER_SIZE);
                    kind = frame[QwpConstants.HEADER_SIZE];
                    Assert.assertTrue("no RESULT_END after " + frames + " frames", ++frames < 32);
                } while (kind != QwpEgressMsgKind.RESULT_END);
            }
        });
    }

    @Test
    public void testWebSocketsServedWithoutKeepAlive() throws Exception {
        // a connection that switched to WebSocket belongs to that protocol, and a server that
        // keeps no HTTP connection alive keeps it open
        assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_SERVER_KEEP_ALIVE.getEnvVarName(), "false",
                    PropertyKey.METRICS_ENABLED.getEnvVarName(), "true"
            )) {
                assertWebSocketsServed(serverMain);
            }
        });
    }

    @Test
    public void testWebSocketsServedWithoutKeepAliveAfterParkedUpgrade() throws Exception {
        // the server sends the 101 a few bytes at a time and parks between sends, so the
        // connection switches to WebSocket when the last send resumes
        assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_SERVER_KEEP_ALIVE.getEnvVarName(), "false",
                    PropertyKey.DEBUG_HTTP_FORCE_SEND_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), "16",
                    PropertyKey.METRICS_ENABLED.getEnvVarName(), "true"
            )) {
                assertWebSocketsServed(serverMain);
            }
        });
    }

    // the server ends the connection after the response: the stream ends rather than
    // blocking until the read timeout
    private static void assertClosedByServer(InputStream in) throws Exception {
        try {
            Assert.assertEquals("the server sent more than the response", -1, in.read());
        } catch (SocketTimeoutException e) {
            Assert.fail("the server kept the connection open after the response to a request that asked to close it");
        }
    }

    // sends each kind of request on a connection of its own, one for each way the server reads a
    // request, and checks that the server closes the connection after the whole response
    private static void assertEachRequestClosesConnection(TestServerMain serverMain) throws Exception {
        // a request without a body
        assertExchangesThenClose(HTTP_PORT, getExec("SELECT%201", CONNECTION_CLOSE), jsonResponse(CONNECTION_CLOSE, SELECT_ONE_JSON));
        // a body with Content-Length, and a chunked body
        assertExchangesThenClose(
                HTTP_PORT,
                ilpWithContentLength("close_ilp v=1i\n", "Connection: Close\r\n"),
                noContentResponse()
        );
        assertExchangesThenClose(
                HTTP_PORT,
                ilpChunked("close_ilp v=2i\n", "Connection: keep-alive, close\r\n"),
                noContentResponse()
        );
        // a multipart body
        assertExchangesThenClose(
                HTTP_PORT,
                csvImport("TE, close"),
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        DATE +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        CONNECTION_CLOSE +
                        "\r\n" +
                        "{\"status\":\"OK\",\"location\":\"close_import\",\"rowsRejected\":0,\"rowsImported\":1,\"header\":true,\"partitionBy\":\"NONE\",\"columns\":[{\"name\":\"x\",\"type\":\"INT\",\"size\":4,\"errors\":0}]}"
        );
        // a rejected request, with a body of each kind
        final String methodNotAllowed = "HTTP/1.1 405 Method Not Allowed\r\n" +
                "Server: questDB/1.0\r\n" +
                DATE +
                "Transfer-Encoding: chunked\r\n" +
                "Content-Type: text/plain; charset=utf-8\r\n" +
                CONNECTION_CLOSE +
                "\r\n" +
                "Method POST not supported\r\n";
        assertExchangesThenClose(
                HTTP_PORT,
                "POST /exec?query=SELECT%201 HTTP/1.1\r\n" +
                        "Host: localhost:" + HTTP_PORT + "\r\n" +
                        "Connection: close\r\n" +
                        "Content-Length: 1\r\n" +
                        "\r\n" +
                        "x",
                methodNotAllowed
        );
        assertExchangesThenClose(
                HTTP_PORT,
                "POST /exec?query=SELECT%201 HTTP/1.1\r\n" +
                        "Host: localhost:" + HTTP_PORT + "\r\n" +
                        "Connection: close\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "\r\n" +
                        "1\r\nx\r\n0\r\n\r\n",
                methodNotAllowed
        );
        // the min HTTP server
        assertExchangesThenClose(
                HTTP_MIN_PORT,
                "GET /status HTTP/1.1\r\nHost: localhost:" + HTTP_MIN_PORT + "\r\nConnection: close\r\n\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        DATE +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: text/plain\r\n" +
                        CONNECTION_CLOSE +
                        "\r\n" +
                        "Status: Healthy"
        );

        // the server closed each connection after it had handled the request in full
        serverMain.assertSql("SELECT x FROM close_import", "x\n1\n");
        TestUtils.assertEventually(() -> serverMain.assertSql("SELECT v FROM close_ilp ORDER BY v", "v\n1\n2\n"));
    }

    // sends each request on one connection once the response to the one before it has arrived,
    // checks each response, and checks that the server closes the connection after the last one
    private static void assertExchangesThenClose(int port, String... requestsAndResponses) throws Exception {
        try (Socket socket = new Socket("127.0.0.1", port)) {
            socket.setSoTimeout(10_000);
            final OutputStream out = socket.getOutputStream();
            final InputStream in = socket.getInputStream();
            for (int i = 0; i < requestsAndResponses.length; i += 2) {
                Assert.assertEquals(
                        requestsAndResponses[i],
                        requestsAndResponses[i + 1],
                        QwpWireTestFixtures.exchange(out, in, requestsAndResponses[i])
                );
            }
            assertClosedByServer(in);
        }
    }

    // a request that waits for a busy table writer completes on a later attempt, and the
    // connection closes once that attempt has sent the response
    private static void assertRetriedRequestClosesConnection(SocketFactory socketFactory, int sendFragmentationChunkSize) throws Exception {
        final HttpQueryTestBuilder builder = new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withForceSendFragmentationChunkSize(sendFragmentationChunkSize)
                .withSocketFactory(socketFactory)
                .withTelemetry(false);
        builder.run((engine, sqlExecutionContext) -> {
            engine.execute("CREATE TABLE tab (x LONG)", sqlExecutionContext);
            final WaitProcessor waitProcessor = builder.getHttpServer().getWaitProcessor();
            try (Socket socket = new Socket("127.0.0.1", 9001)) {
                socket.setSoTimeout(10_000);
                final InputStream in = socket.getInputStream();
                final long reschedules = waitProcessor.getRescheduleCount();
                try (TableWriter ignore = engine.getWriter(engine.verifyTableName("tab"), "test")) {
                    socket.getOutputStream().write(
                            getExec("INSERT%20INTO%20tab%20VALUES%20(1)", "localhost:9001", CONNECTION_CLOSE).getBytes(StandardCharsets.US_ASCII)
                    );
                    // the held writer turns the INSERT into a busy-writer retry
                    TestUtils.assertEventually(() -> Assert.assertTrue(waitProcessor.getRescheduleCount() > reschedules));
                }
                final String headers = QwpWireTestFixtures.readHttpHeaders(in);
                Assert.assertEquals(jsonResponse(CONNECTION_CLOSE, "{\"dml\":\"OK\"}"), headers + QwpWireTestFixtures.readChunkedBody(in, headers));
                assertClosedByServer(in);
            }
            assertExchangesThenClose(
                    9001,
                    getExec("SELECT%20x%20FROM%20tab", "localhost:9001", CONNECTION_CLOSE),
                    jsonResponse(CONNECTION_CLOSE, "{\"query\":\"SELECT x FROM tab\",\"columns\":[{\"name\":\"x\",\"type\":\"LONG\"}],\"timestamp\":-1,\"dataset\":[[1]],\"count\":1}")
            );
        });
    }

    // a query client runs queries and a sender writes rows over QWP WebSockets, and the server
    // releases each connection once its client closes it
    private static void assertWebSocketsServed(TestServerMain serverMain) throws Exception {
        final QwpEgressMetrics metrics = serverMain.getEngine().getMetrics().qwpEgressMetrics();
        try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";failover=off;")) {
            client.connect();
            // the second query runs on the connection that the first one kept open
            for (int i = 1; i <= 2; i++) {
                final StringBuilder result = new StringBuilder();
                client.execute("SELECT " + i + " x", new QwpColumnBatchHandler() {
                    @Override
                    public void onBatch(QwpColumnBatch batch) {
                        for (int row = 0, n = batch.getRowCount(); row < n; row++) {
                            result.append(batch.getIntValue(0, row)).append('\n');
                        }
                    }

                    @Override
                    public void onEnd(long totalRows) {
                        result.append("rows: ").append(totalRows);
                    }

                    @Override
                    public void onError(byte status, String message) {
                        result.append("error: ").append(message);
                    }
                });
                Assert.assertEquals(i + "\nrows: 1", result.toString());
            }
            Assert.assertEquals(1, metrics.connectionCountGauge().getValue());
        }
        TestUtils.assertEventually(() -> Assert.assertEquals(0, metrics.connectionCountGauge().getValue()));

        serverMain.execute("CREATE TABLE ws_rows (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        // close() waits for the server to acknowledge the row
        try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + HTTP_PORT + ";close_flush_timeout_millis=10000;")) {
            sender.table("ws_rows").longColumn("v", 42).at(1_000_000_000_000L, ChronoUnit.MICROS);
        }
        serverMain.awaitTable("ws_rows");
        serverMain.assertSql("SELECT v FROM ws_rows", "v\n42\n");
    }

    private static String csvImport(String connection) {
        final String body = "--" + MULTIPART_BOUNDARY + "\r\n" +
                "Content-Disposition: form-data; name=\"schema\"; filename=\"schema.json\"\r\n" +
                "Content-Type: application/octet-stream\r\n" +
                "\r\n" +
                "[{\"name\":\"x\",\"type\":\"INT\"}]\r\n" +
                "--" + MULTIPART_BOUNDARY + "\r\n" +
                "Content-Disposition: form-data; name=\"data\"; filename=\"close.csv\"\r\n" +
                "Content-Type: application/octet-stream\r\n" +
                "\r\n" +
                "x\r\n" +
                "1\r\n" +
                "--" + MULTIPART_BOUNDARY + "--";
        return "POST /imp?fmt=json&forceHeader=true&delimiter=%2C&name=close_import HTTP/1.1\r\n" +
                "Host: localhost:" + HTTP_PORT + "\r\n" +
                "Connection: " + connection + "\r\n" +
                "Content-Type: multipart/form-data; boundary=" + MULTIPART_BOUNDARY + "\r\n" +
                "Content-Length: " + body.length() + "\r\n" +
                "\r\n" +
                body;
    }

    private static String getExec(String query, String headers) {
        return getExec(query, "localhost:" + HTTP_PORT, headers);
    }

    private static String getExec(String query, String host, String headers) {
        return "GET /exec?query=" + query + " HTTP/1.1\r\n" +
                "Host: " + host + "\r\n" +
                headers +
                "\r\n";
    }

    private static String ilpChunked(String lines, String headers) {
        return "POST /write HTTP/1.1\r\n" +
                "Host: localhost:" + HTTP_PORT + "\r\n" +
                headers +
                "Transfer-Encoding: chunked\r\n" +
                "\r\n" +
                Integer.toHexString(lines.length()) + "\r\n" +
                lines + "\r\n" +
                "0\r\n" +
                "\r\n";
    }

    private static String ilpWithContentLength(String lines, String headers) {
        return "POST /write HTTP/1.1\r\n" +
                "Host: localhost:" + HTTP_PORT + "\r\n" +
                headers +
                "Content-Length: " + lines.length() + "\r\n" +
                "\r\n" +
                lines;
    }

    // the response to a query on /exec, as exchange() returns it: the header block and the decoded body
    private static String jsonResponse(String connectionHeader, String json) {
        return "HTTP/1.1 200 OK\r\n" +
                "Server: questDB/1.0\r\n" +
                DATE +
                "Transfer-Encoding: chunked\r\n" +
                "Content-Type: application/json; charset=utf-8\r\n" +
                connectionHeader +
                "Keep-Alive: timeout=5, max=10000\r\n" +
                "\r\n" +
                json;
    }

    // the JSON of SELECT x FROM long_sequence(rows), with the row count written as in the query
    private static String longSequenceJson(String rows, int count) {
        final StringBuilder dataset = new StringBuilder();
        for (int i = 1; i <= count; i++) {
            dataset.append(i > 1 ? ",[" : "[").append(i).append(']');
        }
        return "{\"query\":\"SELECT x FROM long_sequence(" + rows + ")\",\"columns\":[{\"name\":\"x\",\"type\":\"LONG\"}],\"timestamp\":-1,\"dataset\":["
                + dataset + "],\"count\":" + count + "}";
    }

    private static String longSequenceRequest(String rows, String headers) {
        return getExec("SELECT%20x%20FROM%20long_sequence%28" + rows + "%29", headers);
    }

    private static String noContentResponse() {
        return noContentResponse(CONNECTION_CLOSE);
    }

    private static String noContentResponse(String connectionHeader) {
        return "HTTP/1.1 204 OK\r\n" +
                "Server: questDB/1.0\r\n" +
                DATE +
                connectionHeader +
                "\r\n";
    }

    // starts the server with HTTP sockets of the given factory, on both HTTP servers
    private static TestServerMain startWithHttpSocketFactory(SocketFactory socketFactory, String... envs) {
        final Map<String, String> env = new HashMap<>(System.getenv());
        for (int i = 0; i < envs.length; i += 2) {
            env.put(envs[i], envs[i + 1]);
        }
        env.put(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED.getEnvVarName(), "false");
        final TestServerMain serverMain = new TestServerMain(new Bootstrap(
                new PropBootstrapConfiguration() {
                    @Override
                    public Map<String, String> getEnv() {
                        return env;
                    }

                    @Override
                    public ServerConfiguration getServerConfiguration(Bootstrap bootstrap) throws Exception {
                        return new DynamicPropServerConfiguration(
                                bootstrap.getRootDirectory(),
                                bootstrap.loadProperties(),
                                getEnv(),
                                bootstrap.getLog(),
                                bootstrap.getBuildInformation(),
                                FilesFacadeImpl.INSTANCE,
                                bootstrap.getMicrosecondClock(),
                                (configuration, engine, freeOnExit) -> new FactoryProviderImpl(configuration) {
                                    @Override
                                    public @NotNull SocketFactory getHttpMinSocketFactory() {
                                        return socketFactory;
                                    }

                                    @Override
                                    public @NotNull SocketFactory getHttpSocketFactory() {
                                        return socketFactory;
                                    }
                                }
                        );
                    }
                },
                getServerMainArgs()
        ));
        try {
            serverMain.start();
            return serverMain;
        } catch (Throwable th) {
            Misc.free(serverMain, th);
            throw th;
        }
    }

    // a session that fails to send what it holds, as one does once the peer has gone
    private static class FailingTlsSessionSocket extends TlsSessionSocket {
        private FailingTlsSessionSocket(NetworkFacade nf, Log log) {
            super(nf, log);
        }

        @Override
        public int tlsIO(int readinessFlags) {
            return (readinessFlags & WRITE_FLAG) != 0 ? -1 : 0;
        }
    }

    // Holds back part of everything it sends, as the TLS session of an encrypted socket does:
    // send() takes the bytes into the session and reports them sent, but writes no more than
    // WRITE_SIZE bytes of the session to the network, and only tlsIO() writes the rest. What
    // the session still holds when the socket closes is lost.
    private static class TlsSessionSocket extends PlainSocket {
        private static final int SESSION_CAPACITY = 64 * 1024;
        private static final int WRITE_SIZE = 64;
        private long session;
        private int sessionSize;

        private TlsSessionSocket(NetworkFacade nf, Log log) {
            super(nf, log);
        }

        @Override
        public void close() {
            session = Unsafe.free(session, SESSION_CAPACITY, MemoryTag.NATIVE_DEFAULT);
            sessionSize = 0;
            super.close();
        }

        @Override
        public void of(long fd) {
            super.of(fd);
            if (session == 0) {
                session = Unsafe.malloc(SESSION_CAPACITY, MemoryTag.NATIVE_DEFAULT);
            }
        }

        @Override
        public int send(long bufferPtr, int bufferLen) {
            final int taken = Math.min(bufferLen, SESSION_CAPACITY - sessionSize);
            Vect.memcpy(session + sessionSize, bufferPtr, taken);
            sessionSize += taken;
            return writeSession() < 0 ? -1 : taken;
        }

        @Override
        public int tlsIO(int readinessFlags) {
            return (readinessFlags & WRITE_FLAG) != 0 && writeSession() < 0 ? -1 : 0;
        }

        @Override
        public boolean wantsTlsWrite() {
            return sessionSize > 0;
        }

        private int writeSession() {
            if (sessionSize == 0) {
                return 0;
            }
            final int written = super.send(session, Math.min(sessionSize, WRITE_SIZE));
            if (written > 0) {
                sessionSize -= written;
                Vect.memmove(session, session + written, sessionSize);
            }
            return written;
        }
    }
}
