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

package io.questdb.test.cutlass.http;

import io.questdb.DefaultFactoryProvider;
import io.questdb.DefaultHttpClientConfiguration;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpChunkedResponse;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpRequestHandler;
import io.questdb.cutlass.http.HttpRequestHandlerFactory;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.cutlass.http.HttpServer;
import io.questdb.cutlass.http.client.Fragment;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.cutlass.http.client.Response;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.PlainSocketFactory;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.ThreadLocal;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.test.tools.TestUtils.assertEquals;
import static java.lang.Math.min;
import static org.junit.Assert.assertTrue;

public class HttpServerTest extends AbstractTest {
    private static final ThreadLocal<StringSink> tlSink = new ThreadLocal<>(StringSink::new);

    @Test
    public void testHttpServerBind() {
        try (HttpServerMock httpServer = new HttpServerMock(1, 9001)) {
            httpServer.registerEndpoint("/service", new HttpRequestHandler() {
                @Override
                public HttpRequestProcessor getProcessor(HttpRequestHeader requestHeader) {
                    return new HttpRequestProcessor() {
                        @Override
                        public void onRequestComplete(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException {
                            final HttpChunkedResponse response = context.getChunkedResponse();
                            response.status(200, "text/plain");
                            response.sendHeader();
                            response.putAscii("Success");
                            response.sendChunk(true);
                        }
                    };
                }
            });
            httpServer.start();

            try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                assertExecRequest(httpClient, "/service", 200, "Success");
                assertExecRequest(httpClient, "/noHandler", 400, "No request handler for URI: /noHandler\r\n");
            }
        }
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

            final StringSink sink = tlSink.get();

            Fragment fragment;
            final Response chunkedResponse = responseHeaders.getResponse();
            while ((fragment = chunkedResponse.recv()) != null) {
                Utf8s.utf8ToUtf16(fragment.lo(), fragment.hi(), sink);
            }

            if (expectedHttpResponse != null) {
                TestUtils.assertEquals(expectedHttpResponse, sink);
            }
            sink.clear();
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

            final int cpuAvailable = Runtime.getRuntime().availableProcessors() / 2;
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

        private void registerEndpoint(String uri, HttpRequestHandler requestHandler) {
            assert requestHandler != null;
            httpServer.bind(new HttpRequestHandlerFactory() {
                @Override
                public ObjList<String> getUrls() {
                    return new ObjList<>(uri);
                }

                @Override
                public HttpRequestHandler newInstance() {
                    return requestHandler;
                }
            });
        }
    }
}
