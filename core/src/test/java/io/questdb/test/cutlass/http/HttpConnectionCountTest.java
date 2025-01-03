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
import io.questdb.cutlass.http.processors.LineHttpProcessor;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.WorkerPool;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.QueryPausedException;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.PropertyKey.*;
import static io.questdb.test.tools.TestUtils.*;
import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class HttpConnectionCountTest extends AbstractBootstrapTest {
    private static final String EXEC_TEST_URI = "/exec-test";
    private static final String EXEC_URI = "/exec";
    private static final String ILP_PATH = "/write";
    private static final String ILP_TEST_PATH = "/write-test";

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
                                    public String getUrl() {
                                        return ILP_TEST_PATH;
                                    }

                                    @Override
                                    public HttpRequestProcessor newInstance() {
                                        return new LineHttpProcessor(
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
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + serverMain.getHttpServerPort())
                        .httpPath(ILP_PATH)
                        .build()
                ) {
                    sender.table("tab").longColumn("col", 2).atNow();
                    try {
                        sender.flush();
                        fail("sender should fail with soft limit breach");
                    } catch (LineSenderException e) {
                        assertContains(e.getMessage(), "Could not flush buffer: exceeded HTTP connection soft limit [name=line_http_connections]");
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
                HTTP_QUERY_CONNECTION_LIMIT + "=4"
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
                                    public String getUrl() {
                                        return EXEC_TEST_URI;
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
                try (final HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    assertExecRequest(httpClient, EXEC_URI, "select 2", HTTP_BAD_REQUEST,
                            "exceeded HTTP connection soft limit [name=json_http_connections]\r\n"
                    );
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
        final HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
        request.GET().url(uri).query("query", sql);
        try (HttpClient.ResponseHeaders responseHeaders = request.send()) {
            responseHeaders.await();

            TestUtils.assertEquals(String.valueOf(expectedHttpStatusCode), responseHeaders.getStatusCode());

            final Utf8StringSink sink = new Utf8StringSink();

            Fragment fragment;
            final Response response = responseHeaders.getResponse();
            while ((fragment = response.recv()) != null) {
                Utf8s.strCpy(fragment.lo(), fragment.hi(), sink);
            }

            TestUtils.assertEquals(expectedHttpResponse, sink.toString());
            sink.clear();
        }
    }
}
