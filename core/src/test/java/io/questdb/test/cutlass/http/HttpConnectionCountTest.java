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

import io.questdb.DefaultHttpClientConfiguration;
import io.questdb.ServerConfiguration;
import io.questdb.ServerMain;
import io.questdb.cairo.CairoEngine;
import io.questdb.client.Sender;
import io.questdb.cutlass.Services;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.cutlass.http.HttpRequestProcessorFactory;
import io.questdb.cutlass.http.HttpServer;
import io.questdb.cutlass.http.client.Fragment;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.cutlass.http.client.Response;
import io.questdb.cutlass.http.processors.JsonQueryProcessor;
import io.questdb.cutlass.http.processors.LineHttpProcessorImpl;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.WorkerPool;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.QueryPausedException;
import io.questdb.std.Chars;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.PropertyKey.*;
import static io.questdb.test.tools.TestUtils.assertMemoryLeak;
import static io.questdb.test.tools.TestUtils.unchecked;
import static java.net.HttpURLConnection.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class HttpConnectionCountTest extends AbstractBootstrapTest {
    private static final String EXEC_TEST_URI = "/exec-test";
    private static final String EXEC_URI = "/exec";
    private static final String ILP_PATH = "/write";
    private static final String ILP_TEST_PATH = "/write-test";
    private static final String PING_PATH = "/ping";

    @Before
    public void setUp() {
        super.setUp();
    }

    @Test
    public void testConnectionLimit() throws Exception {
        final int numOfThreads = 12;
        final long jsonQueryConnLimit = 8;
        final long ilpConnLimit = numOfThreads - jsonQueryConnLimit;

        unchecked(() -> createDummyConfiguration(
                METRICS_ENABLED + "=true",
                HTTP_WORKER_COUNT + "=" + numOfThreads,
                HTTP_NET_CONNECTION_LIMIT + "=" + (int) (numOfThreads * 1.5),
                HTTP_JSON_QUERY_CONNECTION_LIMIT + "=" + jsonQueryConnLimit,
                HTTP_ILP_CONNECTION_LIMIT + "=" + ilpConnLimit
        ));

        assertMemoryLeak(() -> {
            final Rnd rnd = new Rnd();

            try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
                serverMain.start();

                final CyclicBarrier start = new CyclicBarrier(numOfThreads);
                final SOCountDownLatch end = new SOCountDownLatch(numOfThreads);
                final AtomicInteger errorCount = new AtomicInteger();
                final ObjList<String> errorMessages = new ObjList<>();
                final ConcurrentMap<Integer, ConnectionType> connectionTypes = new ConcurrentHashMap<>();

                for (int i = 0; i < numOfThreads; i++) {
                    final int threadIndex = i;
                    new Thread(() -> {
                        await(start);
                        try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration());
                             HttpClient.ResponseHeaders responseHeaders = httpClient.getResponseHeaders()) {
                            for (int j = 0; j < 300; j++) {
                                final ConnectionType prevConnectionType = connectionTypes.get(threadIndex);
                                final ConnectionType connectionType = (j % 30 == 0) && rnd.nextBoolean() ? ConnectionType.QUERY : ConnectionType.ILP;
                                if (connectionType != prevConnectionType) {
                                    connectionTypes.put(threadIndex, connectionType);
                                    if (prevConnectionType != null) {
                                        responseHeaders.close();
                                    }
                                }

                                if (connectionType == ConnectionType.QUERY) {
                                    sendExecRequest(httpClient, EXEC_URI, "select " + j);
                                    assertResponse(responseHeaders, HTTP_OK,
                                            "{\"query\":\"select " + j + "\",\"columns\":[{\"name\":\"" + j + "\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[" + j + "]],\"count\":1}"
                                    );
                                } else {
                                    sendIlpRequest(httpClient, "tab col1=" + threadIndex + " " + (threadIndex * 1000000 + j));
                                    assertResponse(responseHeaders, HTTP_NO_CONTENT, "");
                                }
                            }
                        } catch (Throwable e) {
                            if (!Chars.contains(e.getMessage(), "exceeded connection limit [name=json_queries_connections")
                                    && !Chars.contains(e.getMessage(), "exceeded connection limit [name=line_http_connections")) {
                                errorCount.incrementAndGet();
                                errorMessages.add(e.getMessage());
                                LOG.error().$("Error executing query [thread=").$(threadIndex).$(", error=").$(e.getMessage()).$(']').$();
                            }
                        }
                        end.countDown();
                    }).start();
                }

                end.await();

                for (int i = 0; i < errorMessages.size(); i++) {
                    LOG.error().$("Error ").$(errorMessages.get(i)).$();
                }
                assertEquals(0, errorCount.get());
            }
        });
    }

    @Test
    public void testIlpConnectionLimit() throws Exception {
        unchecked(() -> createDummyConfiguration(
                METRICS_ENABLED + "=true",
                HTTP_WORKER_COUNT + "=6",
                HTTP_ILP_CONNECTION_LIMIT + "=4"
        ));

        final int numOfThreads = 4;
        final CyclicBarrier limitBarrier = new CyclicBarrier(numOfThreads + 1);
        final CyclicBarrier endBarrier = new CyclicBarrier(numOfThreads + 1);

        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = new ServerMain(getServerMainArgs()) {
                @Override
                protected Services services() {
                    return new Services() {
                        @Override
                        public @Nullable HttpServer createHttpServer(ServerConfiguration configuration, CairoEngine cairoEngine, WorkerPool workerPool, int sharedWorkerCount) {
                            HttpServer server = super.createHttpServer(configuration, cairoEngine, workerPool, sharedWorkerCount);
                            if (server != null) {
                                server.bind(new HttpRequestProcessorFactory() {
                                    @Override
                                    public ObjList<String> getUrls() {
                                        return new ObjList<>(ILP_TEST_PATH);
                                    }

                                    @Override
                                    public HttpRequestProcessor newInstance() {
                                        return new LineHttpProcessorImpl(
                                                cairoEngine,
                                                configuration.getHttpServerConfiguration().getRecvBufferSize(),
                                                configuration.getHttpServerConfiguration().getSendBufferSize(),
                                                configuration.getHttpServerConfiguration().getLineHttpProcessorConfiguration()
                                        ) {
                                            @Override
                                            public void onRequestComplete(
                                                    HttpConnectionContext context
                                            ) throws PeerDisconnectedException, PeerIsSlowToReadException {
                                                super.onRequestComplete(context);

                                                await(limitBarrier);
                                                await(endBarrier);
                                            }
                                        };
                                    }
                                });
                            }
                            return server;
                        }
                    };
                }
            }) {
                serverMain.start();

                final AtomicInteger errorCount = new AtomicInteger();
                final SOCountDownLatch end = new SOCountDownLatch(numOfThreads);
                for (int i = 0; i < numOfThreads; i++) {
                    final int threadIndex = i;
                    new Thread(() -> {
                        try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                                .address("localhost:" + serverMain.getHttpServerPort())
                                .httpPath(ILP_TEST_PATH)
                                .build()
                        ) {
                            sender.table("tab").longColumn("col", 1).atNow();
                        } catch (Throwable e) {
                            errorCount.incrementAndGet();
                            LOG.error().$("Error while ingesting [thread=").$(threadIndex).$(", error=").$(e.getMessage()).$(']').$();
                        }
                        end.countDown();
                    }).start();
                }

                limitBarrier.await();
                // at this point limit has been reached

                // ilp over http, sender should fail with soft limit breach
                try (final HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration() {
                    @Override
                    public boolean fixBrokenConnection() {
                        return false;
                    }
                })) {
                    final HttpClient.ResponseHeaders responseHeaders = sendIlpRequest(httpClient, "tab col=1i 1000000");
                    assertResponse(responseHeaders, HTTP_BAD_REQUEST, "exceeded connection limit [name=line_http_connections, numOfConnections=5, connectionLimit=4]\r\n");

                    // test that the connection cannot be used anymore
                    try {
                        assertIlpRequest(httpClient, "tab col=3i 2000000", HTTP_BAD_REQUEST, "exceeded connection limit [name=line_http_connections, numOfConnections=5, connectionLimit=4]\r\n");
                        fail("Exception expected");
                    } catch (Exception e) {
                        TestUtils.assertContains(e.getMessage(), "peer disconnect");
                    }
                } catch (Throwable e) {
                    errorCount.incrementAndGet();
                    LOG.error().$("Error while ingesting [error=").$(e.getMessage()).$(']').$();
                }

                // wait for the rejected connection to be closed to avoid race in the next assert
                while (serverMain.getEngine().getMetrics().lineMetrics().httpConnectionCountGauge().getValue() > numOfThreads) {
                    Os.sleep(50);
                }

                // ilp ping, should fail with soft limit breach
                try (final HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration() {
                    @Override
                    public boolean fixBrokenConnection() {
                        return false;
                    }
                })) {
                    final HttpClient.ResponseHeaders responseHeaders = sendPingRequest(httpClient);
                    assertResponse(responseHeaders, HTTP_BAD_REQUEST, "exceeded connection limit [name=line_http_connections, numOfConnections=5, connectionLimit=4]\r\n");

                    // test that the connection cannot be used anymore
                    try {
                        assertPingRequest(httpClient, HTTP_BAD_REQUEST, "exceeded connection limit [name=line_http_connections, numOfConnections=5, connectionLimit=4]\r\n");
                        fail("Exception expected");
                    } catch (Exception e) {
                        TestUtils.assertContains(e.getMessage(), "peer disconnect");
                    }
                } catch (Throwable e) {
                    errorCount.incrementAndGet();
                    LOG.error().$("Error while ingesting [error=").$(e.getMessage()).$(']').$();
                }

                // http query, should be able to connect
                try (final HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    assertExecRequest(httpClient, EXEC_URI, "select 1", HTTP_OK,
                            "{\"query\":\"select 1\",\"columns\":[{\"name\":\"1\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[1]],\"count\":1}"
                    );
                } catch (Throwable e) {
                    errorCount.incrementAndGet();
                    LOG.error().$("Error executing query [error=").$(e.getMessage()).$(']').$();
                }

                // let threads finish
                endBarrier.await();
                end.await();
                assertEquals(0, errorCount.get());
            }
        });
    }

    @Test
    public void testQueryConnectionLimit() throws Exception {
        unchecked(() -> createDummyConfiguration(
                METRICS_ENABLED + "=true",
                HTTP_WORKER_COUNT + "=6",
                HTTP_JSON_QUERY_CONNECTION_LIMIT + "=4"
        ));

        final int numOfThreads = 4;
        final CyclicBarrier limitBarrier = new CyclicBarrier(numOfThreads + 1);
        final CyclicBarrier endBarrier = new CyclicBarrier(numOfThreads + 1);

        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = new ServerMain(getServerMainArgs()) {
                @Override
                protected Services services() {
                    return new Services() {
                        @Override
                        public @Nullable HttpServer createHttpServer(ServerConfiguration configuration, CairoEngine cairoEngine, WorkerPool workerPool, int sharedWorkerCount) {
                            HttpServer server = super.createHttpServer(configuration, cairoEngine, workerPool, sharedWorkerCount);
                            if (server != null) {
                                server.bind(new HttpRequestProcessorFactory() {
                                    @Override
                                    public ObjList<String> getUrls() {
                                        return new ObjList<>(EXEC_TEST_URI);
                                    }

                                    @Override
                                    public HttpRequestProcessor newInstance() {
                                        return new JsonQueryProcessor(
                                                configuration.getHttpServerConfiguration().getJsonQueryProcessorConfiguration(),
                                                cairoEngine,
                                                workerPool.getWorkerCount(),
                                                sharedWorkerCount
                                        ) {
                                            @Override
                                            public void onRequestComplete(
                                                    HttpConnectionContext context
                                            ) throws PeerDisconnectedException, PeerIsSlowToReadException, QueryPausedException {
                                                super.onRequestComplete(context);

                                                await(limitBarrier);
                                                await(endBarrier);
                                            }
                                        };
                                    }
                                });
                            }
                            return server;
                        }
                    };
                }
            }) {
                serverMain.start();

                final AtomicInteger errorCount = new AtomicInteger();
                final SOCountDownLatch end = new SOCountDownLatch(numOfThreads);
                for (int i = 0; i < numOfThreads; i++) {
                    final int threadIndex = i;
                    new Thread(() -> {
                        try (final HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                            assertExecRequest(httpClient, EXEC_TEST_URI, "select 1", HTTP_OK,
                                    "{\"query\":\"select 1\",\"columns\":[{\"name\":\"1\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[1]],\"count\":1}"
                            );
                        } catch (Throwable e) {
                            errorCount.incrementAndGet();
                            LOG.error().$("Error executing query [thread=").$(threadIndex).$(", error=").$(e.getMessage()).$(']').$();
                        }
                        end.countDown();
                    }).start();
                }

                limitBarrier.await();
                // at this point limit has been reached

                // http query, should fail with soft limit breach
                try (final HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration() {
                    @Override
                    public boolean fixBrokenConnection() {
                        return false;
                    }
                })) {
                    final HttpClient.ResponseHeaders responseHeaders = sendExecRequest(httpClient, EXEC_URI, "select 2");
                    assertResponse(responseHeaders, HTTP_BAD_REQUEST, "exceeded connection limit [name=json_queries_connections, numOfConnections=5, connectionLimit=4]\r\n");

                    // test that the connection cannot be used anymore
                    try {
                        assertExecRequest(httpClient, EXEC_URI, "select 3", HTTP_BAD_REQUEST, "exceeded connection limit [name=json_queries_connections, numOfConnections=5, connectionLimit=4]\r\n");
                        fail("Exception expected");
                    } catch (Exception e) {
                        TestUtils.assertContains(e.getMessage(), "peer disconnect");
                    }
                } catch (Throwable e) {
                    errorCount.incrementAndGet();
                    LOG.error().$("Error executing query [error=").$(e.getMessage()).$(']').$();
                }

                // ilp over http, sender should be able to connect
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + serverMain.getHttpServerPort())
                        .build()
                ) {
                    sender.table("tab").longColumn("col", 1).atNow();
                    sender.flush();
                } catch (Throwable e) {
                    errorCount.incrementAndGet();
                    LOG.error().$("Sender error [error=").$(e.getMessage()).$(']').$();
                }

                // ilp ping, should be able to connect
                try (final HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    assertPingRequest(httpClient, HTTP_NO_CONTENT, "");
                } catch (Throwable e) {
                    errorCount.incrementAndGet();
                    LOG.error().$("Error executing ping request [error=").$(e.getMessage()).$(']').$();
                }

                // let threads finish
                endBarrier.await();
                end.await();
                assertEquals(0, errorCount.get());
            }
        });
    }

    private static void await(CyclicBarrier barrier) {
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            throw new RuntimeException(e);
        }
    }

    private void assertExecRequest(
            HttpClient httpClient,
            String uri,
            String sql,
            int expectedHttpStatusCode,
            String expectedHttpResponse
    ) {
        try (final HttpClient.ResponseHeaders responseHeaders = sendExecRequest(httpClient, uri, sql)) {
            assertResponse(responseHeaders, expectedHttpStatusCode, expectedHttpResponse);
        }
    }

    private void assertIlpRequest(
            HttpClient httpClient,
            String line,
            int expectedHttpStatusCode,
            String expectedHttpResponse
    ) {
        try (final HttpClient.ResponseHeaders responseHeaders = sendIlpRequest(httpClient, line)) {
            assertResponse(responseHeaders, expectedHttpStatusCode, expectedHttpResponse);
        }
    }

    private void assertPingRequest(
            HttpClient httpClient,
            int expectedHttpStatusCode,
            String expectedHttpResponse
    ) {
        try (final HttpClient.ResponseHeaders responseHeaders = sendPingRequest(httpClient)) {
            assertResponse(responseHeaders, expectedHttpStatusCode, expectedHttpResponse);
        }
    }

    private void assertResponse(
            HttpClient.ResponseHeaders responseHeaders,
            int expectedHttpStatusCode,
            String expectedHttpResponse
    ) {
        responseHeaders.clear();
        responseHeaders.await();

        final Utf8StringSink sink = new Utf8StringSink();

        Fragment fragment;
        final Response response = responseHeaders.getResponse();
        while ((fragment = response.recv()) != null) {
            Utf8s.strCpy(fragment.lo(), fragment.hi(), sink);
        }

        TestUtils.assertEquals(expectedHttpResponse, sink.toString());
        sink.clear();

        TestUtils.assertEquals(String.valueOf(expectedHttpStatusCode), responseHeaders.getStatusCode());
    }

    private HttpClient.ResponseHeaders sendExecRequest(
            HttpClient httpClient,
            String uri,
            String sql
    ) {
        final HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
        return request.GET().url(uri).query("query", sql).send();
    }

    private HttpClient.ResponseHeaders sendIlpRequest(
            HttpClient httpClient,
            String line
    ) {
        final HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
        return request.POST().url(ILP_PATH).withContent().put(line).send();
    }

    private HttpClient.ResponseHeaders sendPingRequest(
            HttpClient httpClient
    ) {
        final HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
        return request.GET().url(PING_PATH).send();
    }

    private enum ConnectionType {
        QUERY, ILP
    }
}
