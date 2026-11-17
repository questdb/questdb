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

import io.questdb.PropertyKey;
import io.questdb.cairo.TableWriter;
import io.questdb.cutlass.http.WaitProcessor;
import io.questdb.cutlass.qwp.codec.QwpEgressMsgKind;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.websocket.WebSocketOpcode;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.cutlass.qwp.QwpWireTestFixtures;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;

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
        final StringBuilder dataset = new StringBuilder();
        for (int i = 1; i <= 100; i++) {
            dataset.append(i > 1 ? ",[" : "[").append(i).append(']');
        }
        final String json = "{\"query\":\"SELECT x FROM long_sequence(100)\",\"columns\":[{\"name\":\"x\",\"type\":\"LONG\"}],\"timestamp\":-1,\"dataset\":["
                + dataset + "],\"count\":100}";
        assertMemoryLeak(() -> {
            try (TestServerMain ignore = startWithEnvVariables(
                    PropertyKey.DEBUG_HTTP_FORCE_SEND_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), "16",
                    PropertyKey.DEBUG_HTTP_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), "5"
            )) {
                final String request = getExec("SELECT%20x%20FROM%20long_sequence%28100%29", "");
                final String requestToClose = getExec("SELECT%20x%20FROM%20long_sequence%28100%29", CONNECTION_CLOSE);
                assertExchangesThenClose(
                        HTTP_PORT,
                        request, jsonResponse("", json),
                        requestToClose, jsonResponse(CONNECTION_CLOSE, json)
                );
            }
        });
    }

    @Test
    public void testConnectionClosedAfterRequestReadInSamePass() throws Exception {
        // The next request arrives while the server still sends the long response before it, a
        // fragment at a time. Once that response is out, the server reads the next request in the
        // same pass, answers it in one send, and closes the connection right after.
        final String longQuery = "SELECT x FROM long_sequence(10000)";
        final StringBuilder dataset = new StringBuilder();
        for (int i = 1; i <= 10_000; i++) {
            dataset.append(i > 1 ? ",[" : "[").append(i).append(']');
        }
        final String longJson = "{\"query\":\"" + longQuery + "\",\"columns\":[{\"name\":\"x\",\"type\":\"LONG\"}],\"timestamp\":-1,\"dataset\":["
                + dataset + "],\"count\":10000}";
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
                out.write(getExec("SELECT%20x%20FROM%20long_sequence%2810000%29", "").getBytes(StandardCharsets.US_ASCII));
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
        });
    }

    @Test
    public void testConnectionClosedAfterRetriedRequest() throws Exception {
        // a request that waits for a busy table writer completes on a later attempt,
        // and the connection closes once that attempt has sent the response
        final HttpQueryTestBuilder builder = new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
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

    // the server ends the connection after the response: the stream ends rather than
    // blocking until the read timeout
    private static void assertClosedByServer(InputStream in) throws Exception {
        try {
            Assert.assertEquals("the server sent more than the response", -1, in.read());
        } catch (SocketTimeoutException e) {
            Assert.fail("the server kept the connection open after the response to a request that asked to close it");
        }
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
}
