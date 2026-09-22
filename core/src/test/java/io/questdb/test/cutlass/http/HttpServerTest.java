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

import io.questdb.DefaultFactoryProvider;
import io.questdb.DefaultHttpClientConfiguration;
import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpRequestHandler;
import io.questdb.cutlass.http.HttpRequestHandlerFactory;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.cutlass.http.HttpServer;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.PlainSocketFactory;
import io.questdb.std.ObjHashSet;
import io.questdb.std.QuietCloseable;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.mp.TestWorkerPool;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.test.tools.TestUtils.assertEquals;
import static io.questdb.test.tools.TestUtils.assertEventually;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.junit.Assert.assertTrue;

public class HttpServerTest extends AbstractBootstrapTest {
    private static final String SUCCESS = "Success";
    private static final Utf8String SUCCESS_UTF8 = new Utf8String(SUCCESS);

    @Test
    public void testHttpServerBind() {
        try (HttpServerMock httpServer = new HttpServerMock(1, 9001)) {
            httpServer.registerEndpoint(requestHeader -> new HttpRequestProcessor() {
                @Override
                public void onRequestComplete(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException {
                    context.simpleResponse().sendStatusTextContent(HTTP_OK, SUCCESS_UTF8, null);
                }

                @Override
                public void resumeSend(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException {
                    context.simpleResponse().sendStatusTextContent(HTTP_OK, SUCCESS_UTF8, null);
                }
            });
            httpServer.start();

            try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                assertExecRequest(httpClient, "/service", 200, "Success\r\n");
                assertExecRequest(httpClient, "/noHandler", 400, "No request handler for URI: /noHandler\r\n");
            }
        }
    }

    @Test
    public void testRebaseWalOverHttpExec() throws Exception {
        createDummyConfiguration();
        assertMemoryLeak(() -> {
            try (
                    ServerMain main = startWithEnvVariables(
                            // REBASE WAL requires hard suspension to actually block writes.
                            PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED.getEnvVarName(), "true"
                    );
                    TestHttpClient httpClient = new TestHttpClient()
            ) {
                ddl(httpClient, main, "create table t (ts timestamp, x int) timestamp(ts) partition by day wal");
                dml(httpClient, main, "insert into t values" +
                        " ('2024-01-01T00:00:00.000000Z', 1)," +
                        " ('2024-01-02T00:00:00.000000Z', 2)," +
                        " ('2024-01-03T00:00:00.000000Z', 3)");

                // Wait for the WAL to apply before suspending so the count is deterministic.
                assertEventually(() -> count(httpClient, main, 3));

                // Rebase requires the table to be hard-suspended first.
                ddl(httpClient, main, "alter table t suspend wal");
                // The line under test: this used to NPE at response dispatch over HTTP.
                ddl(httpClient, main, "alter table t rebase wal");

                // Applied data is preserved and the rebased table is live and writable over HTTP.
                assertEventually(() -> count(httpClient, main, 3));
                dml(httpClient, main, "insert into t values ('2024-01-04T00:00:00.000000Z', 4)");
                assertEventually(() -> count(httpClient, main, 4));
            }
        });
    }

    private static void count(TestHttpClient httpClient, ServerMain main, int expected) {
        exec(
                httpClient,
                main,
                "{\"query\":\"select count() from t\",\"columns\":[{\"name\":\"count\",\"type\":\"LONG\"}],\"timestamp\":-1,\"dataset\":[[" + expected + "]],\"count\":1}",
                "select count() from t"
        );
    }

    private static void ddl(TestHttpClient httpClient, ServerMain main, String sql) {
        exec(httpClient, main, "{\"ddl\":\"OK\"}", sql);
    }

    private static void dml(TestHttpClient httpClient, ServerMain main, String sql) {
        exec(httpClient, main, "{\"dml\":\"OK\"}", sql);
    }

    private static void exec(TestHttpClient httpClient, ServerMain main, String expectedResponse, String sql) {
        httpClient.assertGet(
                "/exec",
                expectedResponse,
                sql,
                "localhost",
                main.getHttpServerPort(),
                null,
                null,
                null
        );
    }

    private void assertExecRequest(
            HttpClient httpClient,
            String endpoint,
            int expectedHttpStatusCode,
            String expectedHttpResponse
    ) {
        final HttpClient.Request request = httpClient.newRequest("localhost", 9001);
        request.GET().url(endpoint).query("query", "select 1");
        assertHttpRequest(request, true, expectedHttpStatusCode, expectedHttpResponse);
    }

    private void assertHttpRequest(
            HttpClient.Request request,
            boolean isChunked,
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
            Assert.assertEquals(isChunked, responseHeaders.isChunked());
            HttpUtils.assertChunkedBody(responseHeaders, expectedHttpResponse);
        }
    }

    private static class HttpServerMock implements QuietCloseable {
        private final static Log LOG = LogFactory.getLog(HttpServerMock.class);
        private final HttpServer httpServer;
        private final WorkerPool workerPool;

        public HttpServerMock(int workerCount, int port) {
            final String baseDir = root;
            final HttpServerConfigurationBuilder serverConfigBuilder = new HttpServerConfigurationBuilder();
            final DefaultHttpServerConfiguration httpConfiguration = serverConfigBuilder
                    .withBaseDir(baseDir)
                    .withFactoryProvider(DefaultFactoryProvider.INSTANCE)
                    .withPort(port)
                    .build(new DefaultTestCairoConfiguration(root));

            final int cpuAvailable = max(Runtime.getRuntime().availableProcessors() / 2, 1);
            workerPool = new TestWorkerPool(min(workerCount, cpuAvailable));
            httpServer = new HttpServer(httpConfiguration, workerPool, PlainSocketFactory.INSTANCE);
        }

        @Override
        public void close() {
            workerPool.close();
            httpServer.close();
        }

        public void start() {
            workerPool.start(LOG);
        }

        private void registerEndpoint(HttpRequestHandler requestHandler) {
            assert requestHandler != null;
            httpServer.bind(new HttpRequestHandlerFactory() {
                @Override
                public ObjHashSet<String> getUrls() {
                    return new ObjHashSet<>() {{
                        add("/service");
                    }};
                }

                @Override
                public HttpRequestHandler newInstance() {
                    return requestHandler;
                }
            });
        }
    }
}
