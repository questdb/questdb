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

import io.questdb.cutlass.http.client.Fragment;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.cutlass.http.client.Response;
import io.questdb.cutlass.http.processors.PrometheusMetricsProcessor;
import io.questdb.metrics.Counter;
import io.questdb.metrics.CounterWithOneLabel;
import io.questdb.metrics.CounterWithTwoLabels;
import io.questdb.metrics.LongGauge;
import io.questdb.metrics.MetricsRegistry;
import io.questdb.metrics.MetricsRegistryImpl;
import io.questdb.metrics.Target;
import io.questdb.network.DefaultIODispatcherConfiguration;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.ObjList;
import io.questdb.std.str.BorrowableUtf8Sink;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class MetricsIODispatcherTest {

    // This number should be lower than the maximum IODispatcher connection limit,
    // which by default is 64.
    private static final int PARALLEL_REQUESTS = 16;
    private static final String prometheusRequest = "GET /metrics HTTP/1.1\r\n" +
            "Host: localhost:9003\r\n" +
            "User-Agent: Prometheus/2.22.0\r\n" +
            "Accept: application/openmetrics-text; version=0.0.1,text/plain;version=0.0.4;q=0.5,*/*;q=0.1\r\n" +
            "Accept-Encoding: gzip\r\n" +
            "X-Prometheus-Scrape-Timeout-Seconds: 10.000000\r\n" +
            "\r\n";

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(10 * 60 * 1000, TimeUnit.MILLISECONDS)
            .withLookingForStuckThread(true)
            .build();

    @Test
    public void testFewMetricsBigBuffers() throws Exception {
        // In this scenario there are few metrics and the send and tcp send buffers are large.
        // This will cover the code that handles sending a single chunk from `onRequestComplete`.
        testPrometheusScenario(100, 1024 * 1024, 1024 * 1024, 1);
    }

    @Test
    @Ignore
    public void testFewMetricsBigBuffersPar() throws Exception {
        testPrometheusScenario(100, 1024 * 1024, 1024 * 1024, PARALLEL_REQUESTS);
    }

    @Test
    @Ignore
    public void testLotsOfConnections() throws Exception {
        // In this scenario we want to test pool reuse.
        // This is dependent on thread scheduling so some runs may not achieve
        // the desired coverage, but by repeating the number of requests we can
        // increase the chances of hitting the desired `push()` and `pop()` pool method call
        // sequences.
        testPrometheusScenario(10, 1024, 1024 * 1024, PARALLEL_REQUESTS, 1, 100);
    }

    @Test
    public void testMultiChunkResponse() throws Exception {
        // In this scenario, the metrics response is larger than the chunk size (256 bytes),
        // but fits comfortably in the tcp send buffer (1MiB).
        // This will stress the code that handles sending multiple chunks from `onRequestComplete`.
        testPrometheusScenario(100, 1024 * 1024, 256, 1);
    }

    @Test
    @Ignore
    public void testMultiChunkResponsePar() throws Exception {
        testPrometheusScenario(100, 1024 * 1024, 256, PARALLEL_REQUESTS);
    }

    @Test
    public void testMultipleChunksPeerIsSlowToRead() throws Exception {
        // In this scenario, the metrics response is larger than the chunk size (256 bytes),
        // and is also larger than the tcp send buffer (1KiB).
        // This will stress the code that handles sending multiple chunks from both `onRequestComplete` and
        // `resumeSend`.
        testPrometheusScenario(10_000, 1024, 256, 1);
    }

    @Test
    @Ignore
    public void testMultipleChunksPeerIsSlowToReadPar() throws Exception {
        testPrometheusScenario(10_000, 1024, 256, PARALLEL_REQUESTS);
    }

    @Test
    public void testPeerIsSlowToRead() throws Exception {
        // In this scenario, the metrics response is smaller than the chunk size (1MiB),
        // but larger than the tcp send buffer (1KiB).
        // This will cause `onRequestComplete` to raise `PeerIsSlowToReadException` and
        // will stress the code that handles resending the same chunk multiple times from `resumeSend`.
        testPrometheusScenario(10_000, 1024, 1024 * 1024, 1);
    }

    @Test
    public void testPeerIsSlowToReadPar() throws Exception {
        testPrometheusScenario(10_000, 1024, 1024 * 1024, PARALLEL_REQUESTS);
    }

    @Test
    public void testPrometheusTextFormat() throws Exception {
        TestMetrics metrics = new TestMetrics(new MetricsRegistryImpl());

        new HttpMinTestBuilder()
                .withTempFolder(temp)
                .withScrappable(metrics)
                .run((engine, sqlExecutionContext) -> {
                    metrics.markQueryStart();
                    metrics.markSyntaxError();
                    metrics.markInsertCancelled();

                    String expectedResponse = "HTTP/1.1 200 OK\r\n" +
                            "Server: questDB/1.0\r\n" +
                            "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                            "Transfer-Encoding: chunked\r\n" +
                            "Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n" +
                            "\r\n" +
                            "02f0\r\n" +
                            "# TYPE questdb_test_json_queries_total counter\n" +
                            "questdb_test_json_queries_total 1\n" +
                            "\n" +
                            "# TYPE questdb_test_json_queries_failed_total counter\n" +
                            "questdb_test_json_queries_failed_total{reason=\"cancelled\"} 0\n" +
                            "questdb_test_json_queries_failed_total{reason=\"syntax_error\"} 1\n" +
                            "\n" +
                            "# TYPE questdb_test_compiled_json_queries_failed_total counter\n" +
                            "questdb_test_compiled_json_queries_failed_total{type=\"insert\",reason=\"cancelled\"} 1\n" +
                            "questdb_test_compiled_json_queries_failed_total{type=\"insert\",reason=\"syntax_error\"} 0\n" +
                            "questdb_test_compiled_json_queries_failed_total{type=\"select\",reason=\"cancelled\"} 0\n" +
                            "questdb_test_compiled_json_queries_failed_total{type=\"select\",reason=\"syntax_error\"} 0\n" +
                            "\n" +
                            "# TYPE questdb_test_json_queries_running gauge\n" +
                            "questdb_test_json_queries_running 1\n" +
                            "\n" +
                            "\r\n" +
                            "00\r\n" +
                            "\r\n";

                    new SendAndReceiveRequestBuilder()
                            .withNetworkFacade(NetworkFacadeImpl.INSTANCE)
                            .execute(prometheusRequest, expectedResponse);
                });
    }

    private static HttpQueryTestBuilder.HttpClientCode buildClientCode(
            int parallelRequestBatches,
            int repeatedConnections,
            HttpQueryTestBuilder.HttpClientCode makeRequest
    ) {
        final HttpQueryTestBuilder.HttpClientCode repeatedRequest = (engine, sqlExecutionContext) -> {
            for (int i = 0; i < repeatedConnections; i++) {
                makeRequest.run(engine, sqlExecutionContext);
            }
        };
        // Parallel request batches.
        return parallelizeRequests(parallelRequestBatches, repeatedRequest);
    }

    private static HttpQueryTestBuilder.HttpClientCode parallelizeRequests(
            int parallelRequests,
            HttpQueryTestBuilder.HttpClientCode makeRequest
    ) {
        assert parallelRequests > 0;
        if (parallelRequests == 1) {
            return makeRequest;
        }
        return (engine, sqlExecutionContext) -> {
            final ExecutorService execSvc = Executors.newCachedThreadPool();
            final ObjList<Future<Void>> futures = new ObjList<>(parallelRequests);
            for (int index = 0; index < parallelRequests; index++) {
                futures.add(execSvc.submit(() -> {
                    makeRequest.run(engine, sqlExecutionContext);
                    return null;
                }));
            }

            for (int index = 0; index < parallelRequests; index++) {
                try {
                    futures.getQuick(index).get();
                } catch (ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    private void testPrometheusScenario(int metricCount, int tcpSndBufSize, int sendBufferSize, int parallelRequestBatches) throws Exception {
        testPrometheusScenario(metricCount, tcpSndBufSize, sendBufferSize, parallelRequestBatches, 5, 5);
    }

    private void testPrometheusScenario(int metricCount, int tcpSndBufSize, int sendBufferSize, int parallelRequestBatches, int repeatedRequests, int repeatedConnections) throws Exception {
        final int workerCount = Math.max(2, Math.min(parallelRequestBatches, 6));
        final PrometheusMetricsProcessor.RequestStatePool pool = new PrometheusMetricsProcessor.RequestStatePool(workerCount);

        Assert.assertEquals(0, pool.size());

        MetricsRegistry metrics = new MetricsRegistryImpl();
        for (int i = 0; i < metricCount; i++) {
            metrics.newCounter("testMetrics" + i).add(i);
        }
        StringBuilder expectedResponse = new StringBuilder();
        for (int i = 0; i < metricCount; i++) {
            expectedResponse.append("# TYPE questdb_testMetrics").append(i).append("_total counter").append("\n");
            expectedResponse.append("questdb_testMetrics").append(i).append("_total ").append(i).append("\n").append("\n");
        }

        final HttpQueryTestBuilder.HttpClientCode makeRequest = (engine, sqlExecutionContext) -> {
            try (HttpClient client = HttpClientFactory.newPlainTextInstance()) {
                if (parallelRequestBatches == 1) {
                    Assert.assertTrue("pool.size() > 1: " + pool.size(), pool.size() <= 1);
                }

                final StringSink utf16Sink = new StringSink();

                // Repeated requests over the same connection.
                // This is to stress out the RequestState pooling logic.
                for (int i = 0; i < repeatedRequests; i++) {
                    try (
                            HttpClient.ResponseHeaders response = client.newRequest("localhost", DefaultIODispatcherConfiguration.INSTANCE.getBindPort())
                                    .GET()
                                    .url("/metrics")
                                    .send()
                    ) {
                        response.await(15_000);
                        utf16Sink.clear();
                        utf16Sink.put(response.getStatusCode());
                        TestUtils.assertEquals("200", utf16Sink);
                        Assert.assertTrue(pool.size() <= parallelRequestBatches);

                        Assert.assertTrue(response.isChunked());
                        Response chunkedResponse = response.getResponse();

                        utf16Sink.clear();
                        Fragment fragment;
                        while ((fragment = chunkedResponse.recv(5_000)) != null) {
                            Utf8s.utf8ToUtf16(fragment.lo(), fragment.hi(), utf16Sink);
                        }
                        TestUtils.assertEquals(expectedResponse, utf16Sink);
                    }
                }

                if (parallelRequestBatches == 1) {
                    Assert.assertTrue("pool.size() > 1: " + pool.size(), pool.size() <= 1);
                }
            }
        };

        // Repeat each connection `repeatedConnections` times, for each parallel request batch.
        // This is to stress out the RequestState pooling logic.
        final HttpQueryTestBuilder.HttpClientCode clientCode = buildClientCode(parallelRequestBatches, repeatedConnections, makeRequest);
        new HttpMinTestBuilder()
                .withTempFolder(temp)
                .withScrappable(metrics)
                .withTcpSndBufSize(tcpSndBufSize)
                .withSendBufferSize(sendBufferSize)
                .withWorkerCount(workerCount)
                .withPrometheusPool(pool)
                .run(clientCode);

        Assert.assertEquals(0, pool.size());
    }

    private static class TestMetrics implements Target {
        private static final short INSERT = 0;
        private static final short QUERY_CANCELLED = 0;
        private static final CharSequence[] QUERY_TYPE_ID_TO_NAME = new CharSequence[2];
        private static final CharSequence[] REASON_ID_TO_NAME = new CharSequence[2];
        private static final short SELECT = 1;
        private static final short SYNTAX_ERROR = 1;
        private final CounterWithTwoLabels failedCompiledQueriesCounter;
        private final CounterWithOneLabel failedQueriesCounter;
        private final MetricsRegistry metricsRegistry;
        private final Counter queriesCounter;
        private final LongGauge runningQueries;

        public TestMetrics(MetricsRegistry metricsRegistry) {
            this.queriesCounter = metricsRegistry.newCounter("test_json_queries");
            this.failedQueriesCounter = metricsRegistry.newCounter("test_json_queries_failed", "reason", REASON_ID_TO_NAME);
            this.failedCompiledQueriesCounter = metricsRegistry.newCounter("test_compiled_json_queries_failed",
                    "type", QUERY_TYPE_ID_TO_NAME,
                    "reason", REASON_ID_TO_NAME
            );
            this.runningQueries = metricsRegistry.newLongGauge("test_json_queries_running");
            this.metricsRegistry = metricsRegistry;
        }

        public void markInsertCancelled() {
            failedCompiledQueriesCounter.inc(INSERT, QUERY_CANCELLED);
        }

        public void markQueryStart() {
            runningQueries.inc();
            queriesCounter.inc();
        }

        public void markSyntaxError() {
            failedQueriesCounter.inc(SYNTAX_ERROR);
        }

        @Override
        public void scrapeIntoPrometheus(@NotNull BorrowableUtf8Sink sink) {
            metricsRegistry.scrapeIntoPrometheus(sink);
        }

        static {
            REASON_ID_TO_NAME[QUERY_CANCELLED] = "cancelled";
            REASON_ID_TO_NAME[SYNTAX_ERROR] = "syntax_error";

            QUERY_TYPE_ID_TO_NAME[INSERT] = "insert";
            QUERY_TYPE_ID_TO_NAME[SELECT] = "select";
        }
    }
}
