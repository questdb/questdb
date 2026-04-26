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

package io.questdb.test.cutlass.qwp;

import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.cutlass.qwp.server.egress.QwpEgressMetrics;
import io.questdb.griffin.CompiledQuery;
import io.questdb.std.Os;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * End-to-end tests for the QWP egress Prometheus metrics: per-connection
 * gauge, query counters, batch/row counters, and the Prometheus scrape output
 * format. Tests exercise the egress endpoint with metrics enabled, run
 * representative workloads, and assert the counters advance as expected.
 */
public class QwpEgressMetricsScrapeTest extends AbstractBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testBytesZstdSavedCounterAdvancesWithCompression() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    "QDB_METRICS_ENABLED", "true")) {
                // Repetitive LONG payload compresses well under zstd, so savings
                // are positive on every compressed batch.
                serverMain.execute("CREATE TABLE zstd_metric(x LONG, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO zstd_metric "
                        + "SELECT 1, x::TIMESTAMP FROM long_sequence(50_000)");
                serverMain.awaitTable("zstd_metric");
                QwpEgressMetrics metrics = serverMain.getEngine().getMetrics().qwpEgressMetrics();
                long savedBefore = metrics.bytesCompressedSavedCounter().getValue();

                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";compression=zstd;")) {
                    client.connect();
                    client.execute("SELECT x FROM zstd_metric", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("must succeed: " + message);
                        }
                    });
                }

                long savedDelta = metrics.bytesCompressedSavedCounter().getValue() - savedBefore;
                Assert.assertTrue("zstd must save bytes on a repetitive payload; saved=" + savedDelta,
                        savedDelta > 0);
            }
        });
    }

    @Test
    public void testBatchAndRowCountersAdvanceWithLoad() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    "QDB_METRICS_ENABLED", "true")) {
                serverMain.execute("CREATE TABLE batch_metric(x LONG, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO batch_metric "
                        + "SELECT x, x::TIMESTAMP FROM long_sequence(50_000)");
                serverMain.awaitTable("batch_metric");
                QwpEgressMetrics metrics = serverMain.getEngine().getMetrics().qwpEgressMetrics();
                long batchesBefore = metrics.batchesSentCount();
                long rowsBefore = metrics.rowsStreamedCounter().getValue();
                long bytesBefore = metrics.bytesSentCounter().getValue();

                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT x FROM batch_metric", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("must succeed: " + message);
                        }
                    });
                }

                long batchesDelta = metrics.batchesSentCount() - batchesBefore;
                long rowsDelta = metrics.rowsStreamedCounter().getValue() - rowsBefore;
                long bytesDelta = metrics.bytesSentCounter().getValue() - bytesBefore;
                Assert.assertTrue("at least one batch must be counted", batchesDelta >= 1);
                Assert.assertEquals("rows_streamed must match row count", 50_000, rowsDelta);
                Assert.assertTrue("bytes_sent must advance", bytesDelta > 0);
            }
        });
    }

    @Test
    public void testConnectionGaugeTracksOpenClose() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    "QDB_METRICS_ENABLED", "true")) {
                QwpEgressMetrics metrics = serverMain.getEngine().getMetrics().qwpEgressMetrics();
                Assert.assertEquals("start at 0 with no connections", 0, metrics.connectionCountGauge().getValue());

                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    // client.connect() returns once handshake completes; the server
                    // increment may be on a different thread, so give it a moment.
                    long deadline = System.nanoTime() + 5_000_000_000L;
                    while (metrics.connectionCountGauge().getValue() == 0 && System.nanoTime() < deadline) {
                        Os.pause();
                    }
                    Assert.assertEquals("one active connection", 1, metrics.connectionCountGauge().getValue());
                }

                // Close must bring the gauge back to 0.
                long deadline = System.nanoTime() + 5_000_000_000L;
                while (metrics.connectionCountGauge().getValue() != 0 && System.nanoTime() < deadline) {
                    Os.pause();
                }
                Assert.assertEquals("gauge returns to 0 after close", 0, metrics.connectionCountGauge().getValue());
            }
        });
    }

    @Test
    public void testDisabledMetricsAreNoOp() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    "QDB_METRICS_ENABLED", "false")) {
                serverMain.execute("CREATE TABLE nometric(x LONG, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO nometric VALUES (1, 1::TIMESTAMP)");
                serverMain.awaitTable("nometric");

                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT x FROM nometric", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("must succeed: " + message);
                        }
                    });
                }

                // When metrics are disabled, the registry is a no-op; counters stay at 0 regardless of activity.
                QwpEgressMetrics metrics = serverMain.getEngine().getMetrics().qwpEgressMetrics();
                Assert.assertEquals(0, metrics.queriesStartedCount());
                Assert.assertEquals(0, metrics.rowsStreamedCounter().getValue());
            }
        });
    }

    @Test
    public void testErroredCounterAdvancesOnQueryError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    "QDB_METRICS_ENABLED", "true")) {
                QwpEgressMetrics metrics = serverMain.getEngine().getMetrics().qwpEgressMetrics();
                long erroredBefore = metrics.queriesErroredCounter().getValue();

                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    final String[] msg = {null};
                    client.execute("SELECT * FROM no_such_table", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            Assert.fail("no batch expected");
                        }

                        @Override
                        public void onEnd(long totalRows) {
                            Assert.fail("no end expected");
                        }

                        @Override
                        public void onError(byte s, String m) {
                            msg[0] = m;
                        }
                    });
                    Assert.assertNotNull(msg[0]);
                }
                Assert.assertTrue("errored counter must advance",
                        metrics.queriesErroredCounter().getValue() > erroredBefore);
            }
        });
    }

    @Test
    public void testExecDoneAndStartedCountersAdvance() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    "QDB_METRICS_ENABLED", "true")) {
                QwpEgressMetrics metrics = serverMain.getEngine().getMetrics().qwpEgressMetrics();
                long startedBefore = metrics.queriesStartedCount();

                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    // DDL -> EXEC_DONE
                    final short[] op = {-1};
                    client.execute("CREATE TABLE exec_done_metric(x LONG)", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("must succeed: " + message);
                        }

                        @Override
                        public void onExecDone(short opType, long rowsAffected) {
                            op[0] = opType;
                        }
                    });
                    Assert.assertEquals(CompiledQuery.CREATE_TABLE, op[0]);
                }
                Assert.assertEquals("queries_started counter advances by 1",
                        startedBefore + 1, metrics.queriesStartedCount());
            }
        });
    }

    @Test
    public void testScrapeOutputExposesQwpEgressCounters() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    "QDB_METRICS_ENABLED", "true")) {
                serverMain.execute("CREATE TABLE scrape_t(x LONG, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO scrape_t VALUES (1, 1::TIMESTAMP), (2, 2::TIMESTAMP)");
                serverMain.awaitTable("scrape_t");

                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT x FROM scrape_t", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("must succeed: " + message);
                        }
                    });
                }

                // Scrape the Prometheus output via the metrics registry and assert
                // the egress counters are present with their questdb_ prefix.
                try (DirectUtf8Sink sink = new DirectUtf8Sink(16 * 1024)) {
                    serverMain.getEngine().getMetrics().scrapeIntoPrometheus(sink);
                    String scrape = sink.toString();
                    Assert.assertTrue("qwp_egress_queries_started must appear",
                            scrape.contains("qwp_egress_queries_started"));
                    Assert.assertTrue("qwp_egress_batches_sent must appear",
                            scrape.contains("qwp_egress_batches_sent"));
                    Assert.assertTrue("qwp_egress_bytes_sent must appear",
                            scrape.contains("qwp_egress_bytes_sent"));
                    Assert.assertTrue("qwp_egress_rows_streamed must appear",
                            scrape.contains("qwp_egress_rows_streamed"));
                    Assert.assertTrue("qwp_egress_connections must appear",
                            scrape.contains("qwp_egress_connections"));
                    Assert.assertTrue("qwp_egress_cache_reset_dict must appear",
                            scrape.contains("qwp_egress_cache_reset_dict"));
                    Assert.assertTrue("qwp_egress_cache_reset_schemas must appear",
                            scrape.contains("qwp_egress_cache_reset_schemas"));
                }
            }
        });
    }
}
