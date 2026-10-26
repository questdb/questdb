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
import io.questdb.PropBootstrapConfiguration;
import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.client.DefaultHttpClientConfiguration;
import io.questdb.client.cutlass.http.client.HttpClient;
import io.questdb.client.cutlass.http.client.HttpClientFactory;
import io.questdb.client.network.NetworkFacade;
import io.questdb.client.network.NetworkFacadeImpl;
import io.questdb.client.std.Unsafe;
import io.questdb.client.std.str.Utf8String;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.HttpURLConnection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static io.questdb.cutlass.http.HttpConstants.HEADER_IF_NONE_MATCH;
import static java.net.HttpURLConnection.*;

public class WebConsoleLoadingTest extends AbstractBootstrapTest {
    private static final String TEST_PAYLOAD = "<html><body><p>Dummy Web Console</p></body></html>";

    private long indexFileLastModified;

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> {
            createDummyConfiguration();
            indexFileLastModified = createDummyWebConsole();
        });
        dbPath.parent().$();
    }

    @Test
    public void testEmptyRedirectFraming() throws Exception {
        testEmptyRedirectFraming("", Integer.MAX_VALUE);
        testEmptyRedirectFraming("/", Integer.MAX_VALUE);
        testEmptyRedirectFraming("/context", Integer.MAX_VALUE);
        testEmptyRedirectFraming("", 1);
        testEmptyRedirectFraming("/context", 1);
    }

    @Test
    public void testEmptyRedirectFramingWithoutKeepAlive() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final RecordingHttpClientNetwork network = new RecordingHttpClientNetwork();
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_SERVER_KEEP_ALIVE.getEnvVarName(), "false"
            ); HttpClient httpClient = network.newClient()) {
                final HttpClient.ResponseHeaders redirect = httpClient.newRequest("localhost", HTTP_PORT).GET().url("/").send();
                redirect.await();
                network.assertEmptyResponse(redirect, HTTP_MOVED_PERM);
                Assert.assertEquals("/index.html", redirect.getHeader(new Utf8String("Location")).toString());
                Assert.assertEquals("close", redirect.getHeader(new Utf8String("Connection")).toString());
            }
        });
    }

    @Test
    public void testHttpContextPathExecEndpoint() throws Exception {
        testHttpContextPathExecEndpoint("");
        testHttpContextPathExecEndpoint("/");
        testHttpContextPathExecEndpoint("/context");
    }

    @Test
    public void testWebConsoleLoadsWhenPgDisabled() throws Exception {
        testWebConsoleLoads("", false);
        testWebConsoleLoads("/", false);
        testWebConsoleLoads("/context", false);
    }

    @Test
    public void testWebConsoleLoadsWhenPgEnabled() throws Exception {
        testWebConsoleLoads("", true);
        testWebConsoleLoads("/", true);
        testWebConsoleLoads("/context", true);
    }

    @Test
    public void testWebConsoleRootRedirects() throws Exception {
        testWebConsoleRootRedirects("");
        testWebConsoleRootRedirects("/");
        testWebConsoleRootRedirects("/context");
    }

    private void assertRequest(HttpClient.Request request, int responseCode, String expectedResponse) {
        try (HttpClient.ResponseHeaders responseHeaders = request.send()) {
            responseHeaders.await();
            TestUtils.assertEquals(String.valueOf(responseCode), responseHeaders.getStatusCode().asAsciiCharSequence());
            ClientHttpUtils.assertChunkedBody(responseHeaders, expectedResponse);
        }
    }

    private void assertRequest(
            HttpClient httpClient,
            String contextPath,
            String resource,
            boolean cachedResponse,
            int expectedResponseCode,
            String expectedResponse
    ) {
        final HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
        request.GET().url(contextPath + resource);
        if (cachedResponse) {
            request.header(HEADER_IF_NONE_MATCH.toString(), "\"" + indexFileLastModified + "\"");
        }
        assertRequest(request, expectedResponseCode, expectedResponse);
    }

    private void followRedirect(HttpClient httpClient, RecordingHttpClientNetwork network, String path, String expectedLocation) {
        network.clearReceived();
        final HttpClient.ResponseHeaders redirect = httpClient.newRequest("localhost", HTTP_PORT).GET().url(path).query("test", "1").send();
        redirect.await();
        network.assertEmptyResponse(redirect, HTTP_MOVED_PERM);
        Assert.assertEquals(expectedLocation, redirect.getHeader(new Utf8String("Location")).toString());

        // Closing ResponseHeaders disconnects the client, so consume and follow without closing it.
        network.clearReceived();
        final HttpClient.ResponseHeaders page = httpClient.newRequest("localhost", HTTP_PORT).GET().url(expectedLocation).send();
        page.await();
        TestUtils.assertEquals("200", page.getStatusCode().asAsciiCharSequence());
        Assert.assertEquals(TEST_PAYLOAD.length(), page.getContentLength());
        ClientHttpUtils.assertChunkedBody(page, TEST_PAYLOAD);
        network.assertStatusLine(HTTP_OK);
        Assert.assertEquals(1, network.connectCount);
    }

    private void testEmptyRedirectFraming(String contextPath, int sendFragmentSize) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String normalizedContext = "/".equals(contextPath) ? "" : contextPath;
            final String pagePath = normalizedContext + "/index.html";
            final RecordingHttpClientNetwork network = new RecordingHttpClientNetwork();
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_CONTEXT_WEB_CONSOLE.getEnvVarName(), contextPath,
                    PropertyKey.DEBUG_HTTP_FORCE_SEND_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), Integer.toString(sendFragmentSize),
                    PropertyKey.HTTP_REDIRECT_COUNT.getEnvVarName(), "1",
                    "QDB_HTTP_REDIRECT_1", "/custom -> " + pagePath
            ); HttpClient httpClient = network.newClient()) {
                followRedirect(httpClient, network, normalizedContext + "/", pagePath + "?test=1");

                network.clearReceived();
                final HttpClient.ResponseHeaders cached = httpClient.newRequest("localhost", HTTP_PORT).GET().url(pagePath)
                        .header(HEADER_IF_NONE_MATCH.toString(), "\"" + indexFileLastModified + "\"").send();
                cached.await();
                network.assertEmptyResponse(cached, HTTP_NOT_MODIFIED);

                network.clearReceived();
                final HttpClient.ResponseHeaders query = httpClient.newRequest("localhost", HTTP_PORT).GET()
                        .url(normalizedContext + "/exec").query("query", "SELECT 1").send();
                query.await();
                Assert.assertTrue(query.isChunked());
                ClientHttpUtils.assertChunkedBody(query,
                        "{\"query\":\"SELECT 1\",\"columns\":[{\"name\":\"1\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[1]],\"count\":1}");
                network.assertStatusLine(HTTP_OK);

                network.clearReceived();
                final HttpClient.ResponseHeaders ping = httpClient.newRequest("localhost", HTTP_PORT).GET().url("/ping").send();
                ping.await();
                network.assertEmptyResponse(ping, HTTP_NO_CONTENT);
                Assert.assertNotNull(ping.getHeader(new Utf8String("X-Influxdb-Version")));

                followRedirect(httpClient, network, "/custom", pagePath + "?test=1");
                followRedirect(httpClient, network, normalizedContext, pagePath + "?test=1");
                Assert.assertEquals(1, network.connectCount);
            }
        });
    }

    private void testHttpContextPathExecEndpoint(String contextPathWebConsole) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_CONTEXT_WEB_CONSOLE.getEnvVarName(), contextPathWebConsole
            )) {
                serverMain.start();

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    final HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
                    request.GET().url(contextPathWebConsole + "/exec").query("query", "select 1");
                    assertRequest(
                            request,
                            HttpURLConnection.HTTP_OK,
                            "{\"query\":\"select 1\",\"columns\":[{\"name\":\"1\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[1]],\"count\":1}"
                    );
                }
            }
        });
    }

    private void testWebConsoleLoads(String contextPath, boolean pgEnabled) throws Exception {
        final Bootstrap bootstrap = new Bootstrap(
                new PropBootstrapConfiguration() {
                    @Override
                    public Map<String, String> getEnv() {
                        final Map<String, String> env = new HashMap<>(super.getEnv());
                        env.put(PropertyKey.PG_ENABLED.getEnvVarName(), Boolean.toString(pgEnabled));
                        env.put(PropertyKey.HTTP_CONTEXT_WEB_CONSOLE.getEnvVarName(), contextPath);
                        return Collections.unmodifiableMap(env);
                    }

                    @Override
                    public boolean useSite() {
                        return false;
                    }
                },
                getServerMainArgs()
        );

        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = new ServerMain(bootstrap)) {
                serverMain.start();

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    assertRequest(httpClient, contextPath, "/index.html", true, HTTP_NOT_MODIFIED, "");
                    assertRequest(httpClient, contextPath, "/index.html", false, HTTP_OK, TEST_PAYLOAD);
                }
            }
        });
    }

    private void testWebConsoleRootRedirects(String contextPath) throws Exception {
        final Bootstrap bootstrap = new Bootstrap(
                new PropBootstrapConfiguration() {
                    @Override
                    public Map<String, String> getEnv() {
                        final Map<String, String> env = new HashMap<>(super.getEnv());
                        env.put(PropertyKey.HTTP_CONTEXT_WEB_CONSOLE.getEnvVarName(), contextPath);
                        return Collections.unmodifiableMap(env);
                    }

                    @Override
                    public boolean useSite() {
                        return false;
                    }
                },
                getServerMainArgs()
        );

        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = new ServerMain(bootstrap)) {
                serverMain.start();

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    assertRequest(httpClient, contextPath, "", true, HTTP_MOVED_PERM, "");
                    assertRequest(httpClient, contextPath, "", false, HTTP_MOVED_PERM, "");
                }
            }
        });
    }

    static class RecordingHttpClientNetwork extends NetworkFacadeImpl {
        private final StringBuilder received = new StringBuilder();
        private int connectCount;

        @Override
        public int connectAddrInfo(int fd, long addrInfo) {
            connectCount++;
            return super.connectAddrInfo(fd, addrInfo);
        }

        @Override
        public int recvRaw(int fd, long buffer, int bufferLen) {
            // Do not let the client's header parser discard unexpected bytes after the terminator.
            final int count = super.recvRaw(fd, buffer, Math.min(bufferLen, 1));
            if (count > 0) {
                received.append((char) (Unsafe.getUnsafe().getByte(buffer) & 0xff));
            }
            return count;
        }

        void assertEmptyResponse(HttpClient.ResponseHeaders response, int status) {
            TestUtils.assertEquals(Integer.toString(status), response.getStatusCode().asAsciiCharSequence());
            final boolean isLengthForbidden = status == HTTP_NO_CONTENT || status == HTTP_NOT_MODIFIED;
            Assert.assertEquals("empty responses need status-appropriate framing", isLengthForbidden ? -1 : 0, response.getContentLength());
            Assert.assertNull(response.getHeader(new Utf8String("Transfer-Encoding")));
            Assert.assertNull(response.getHeader(new Utf8String("Content-Type")));
            Assert.assertFalse(response.isChunked());
            ClientHttpUtils.assertChunkedBody(response, "");
            assertStatusLine(status);
            final String wire = received.toString();
            Assert.assertEquals(wire, wire.length() - 4, wire.indexOf("\r\n\r\n"));
            Assert.assertEquals(wire, isLengthForbidden ? 0 : 1, wire.split("Content-Length:", -1).length - 1);
        }

        void assertSingleConnection() {
            Assert.assertEquals(1, connectCount);
        }

        void assertStatusLine(int status) {
            Assert.assertTrue(received.toString(), received.toString().startsWith("HTTP/1.1 " + status + " "));
        }

        void clearReceived() {
            received.setLength(0);
        }

        HttpClient newClient() {
            return HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration() {
                @Override
                public NetworkFacade getNetworkFacade() {
                    return RecordingHttpClientNetwork.this;
                }

                @Override
                public int getTimeout() {
                    return 10_000;
                }
            });
        }
    }

}
