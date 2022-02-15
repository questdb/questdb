/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cutlass.http;

import io.questdb.metrics.*;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.str.CharSink;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class MetricsIODispatcherTest {

    private static final String prometheusRequest = "GET /metrics HTTP/1.1\r\n" +
            "Host: localhost:9003\r\n" +
            "User-Agent: Prometheus/2.22.0\r\n" +
            "Accept: application/openmetrics-text; version=0.0.1,text/plain;version=0.0.4;q=0.5,*/*;q=0.1\r\n" +
            "Accept-Encoding: gzip\r\n" +
            "X-Prometheus-Scrape-Timeout-Seconds: 10.000000\r\n" +
            "\r\n";

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testPrometheusTextFormat() throws Exception {
        TestMetrics metrics = new TestMetrics(new MetricsRegistryImpl());

        new HttpMinTestBuilder()
                .withTempFolder(temp)
                .withScrapable(metrics)
                .run(engine -> {
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
                            .withExpectDisconnect(false)
                            .withPrintOnly(false)
                            .withRequestCount(1)
                            .withPauseBetweenSendAndReceive(0)
                            .execute(prometheusRequest, expectedResponse);
                });
    }

    private static class TestMetrics implements Scrapable {
        private static final short QUERY_CANCELLED = 0;
        private static final short SYNTAX_ERROR = 1;
        private static final CharSequence[] REASON_ID_TO_NAME = new CharSequence[2];
        private static final short INSERT = 0;
        private static final short SELECT = 1;
        private static final CharSequence[] QUERY_TYPE_ID_TO_NAME = new CharSequence[2];
        private final Counter queriesCounter;
        private final CounterWithOneLabel failedQueriesCounter;
        private final CounterWithTwoLabels failedCompiledQueriesCounter;
        private final Gauge runningQueries;
        private final MetricsRegistry metricsRegistry;

        public TestMetrics(MetricsRegistry metricsRegistry) {
            this.queriesCounter = metricsRegistry.newCounter("test_json_queries");
            this.failedQueriesCounter = metricsRegistry.newCounter("test_json_queries_failed", "reason", REASON_ID_TO_NAME);
            this.failedCompiledQueriesCounter = metricsRegistry.newCounter("test_compiled_json_queries_failed",
                    "type", QUERY_TYPE_ID_TO_NAME,
                    "reason", REASON_ID_TO_NAME
            );
            this.runningQueries = metricsRegistry.newGauge("test_json_queries_running");
            this.metricsRegistry = metricsRegistry;
        }

        public void markQueryStart() {
            runningQueries.inc();
            queriesCounter.inc();
        }

        public void markInsertCancelled() {
            failedCompiledQueriesCounter.inc(INSERT, QUERY_CANCELLED);
        }

        public void markSyntaxError() {
            failedQueriesCounter.inc(SYNTAX_ERROR);
        }

        @Override
        public void scrapeIntoPrometheus(CharSink sink) {
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
