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
import io.questdb.client.std.str.DirectUtf8Sequence;
import io.questdb.cutlass.qwp.server.egress.QwpEgressMetrics;
import io.questdb.cutlass.qwp.server.egress.QwpEgressProcessorState;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * End-to-end tests for the QWP egress {@code CACHE_RESET} frame and the soft
 * caps that trigger it. Sets tight caps per test via
 * {@link QwpEgressProcessorState#defaultMaxDictEntriesOverrideForTest},
 * {@link QwpEgressProcessorState#defaultMaxDictHeapBytesOverrideForTest}, and
 * {@link QwpEgressProcessorState#defaultMaxSchemasOverrideForTest}, restores
 * them in {@code @After} so subsequent tests start clean, and asserts both the
 * observable client behaviour (dict resolves across the reset, subsequent
 * queries decode normally) and the server-side metrics counters.
 */
public class QwpEgressCacheResetWireTest extends AbstractBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @After
    public void restoreCaps() {
        QwpEgressProcessorState.defaultMaxDictEntriesOverrideForTest = -1;
        QwpEgressProcessorState.defaultMaxDictHeapBytesOverrideForTest = -1;
        QwpEgressProcessorState.defaultMaxSchemasOverrideForTest = -1;
    }

    /**
     * Dict entry cap at 4: issue queries that pull 8 distinct symbol values
     * across the connection, verify the server ships {@code CACHE_RESET}
     * between queries and the metric counter advances, and the client still
     * resolves every symbol correctly even though the dict has been flushed
     * server-side mid-connection.
     */
    @Test
    public void testCacheResetFiresOnDictEntryCap() throws Exception {
        QwpEgressProcessorState.defaultMaxDictEntriesOverrideForTest = 4;
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    "QDB_METRICS_ENABLED", "true")) {
                serverMain.execute("CREATE TABLE cap_entries(sym SYMBOL, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("""
                        INSERT INTO cap_entries VALUES
                            ('a', 1::TIMESTAMP), ('b', 2::TIMESTAMP),
                            ('c', 3::TIMESTAMP), ('d', 4::TIMESTAMP),
                            ('e', 5::TIMESTAMP), ('f', 6::TIMESTAMP),
                            ('g', 7::TIMESTAMP), ('h', 8::TIMESTAMP)
                        """);
                serverMain.awaitTable("cap_entries");
                QwpEgressMetrics metrics = serverMain.getEngine().getMetrics().qwpEgressMetrics();
                long dictResetsBefore = metrics.cacheResetDictCount();

                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();

                    // Query 1: 4 symbols, exactly at cap. CACHE_RESET fires
                    // after RESULT_END when computeCacheResetMask reports dict
                    // size >= cap.
                    final java.util.Set<String> q1 = new java.util.HashSet<>();
                    client.execute("SELECT sym FROM cap_entries LIMIT 4", collectInto(q1));
                    Assert.assertEquals(java.util.Set.of("a", "b", "c", "d"), q1);

                    // Query 2: reads 4 new symbols. On a reset-naive server the
                    // dict would still contain a..d, so the new symbols would
                    // get ids 4..7. With the reset, the dict starts fresh --
                    // either way the client must resolve every symbol.
                    final java.util.Set<String> q2 = new java.util.HashSet<>();
                    client.execute("SELECT sym FROM cap_entries WHERE sym IN ('e','f','g','h')", collectInto(q2));
                    Assert.assertEquals(java.util.Set.of("e", "f", "g", "h"), q2);

                    // Query 3: reads EVERY symbol, including ones from both
                    // sides of the reset. The client's dict has been flushed
                    // between batches, so every symbol needs to appear in the
                    // delta section of q3's first batch.
                    final java.util.Set<String> q3 = new java.util.HashSet<>();
                    client.execute("SELECT sym FROM cap_entries", collectInto(q3));
                    Assert.assertEquals(java.util.Set.of("a", "b", "c", "d", "e", "f", "g", "h"), q3);
                }

                Assert.assertTrue("dict CACHE_RESET must have fired at least once",
                        metrics.cacheResetDictCount() > dictResetsBefore);
            }
        });
    }

    /**
     * Dict heap cap at 8 bytes: three 4-byte symbol strings (total 12 bytes)
     * will trip the cap. Tests that the heap-bytes-based trigger fires
     * independently of the entry-count trigger.
     */
    @Test
    public void testCacheResetFiresOnDictHeapCap() throws Exception {
        QwpEgressProcessorState.defaultMaxDictHeapBytesOverrideForTest = 8;
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    "QDB_METRICS_ENABLED", "true")) {
                serverMain.execute("CREATE TABLE cap_heap(sym SYMBOL, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("""
                        INSERT INTO cap_heap VALUES
                            ('quad', 1::TIMESTAMP),
                            ('pent', 2::TIMESTAMP),
                            ('hexo', 3::TIMESTAMP)
                        """);
                serverMain.awaitTable("cap_heap");
                QwpEgressMetrics metrics = serverMain.getEngine().getMetrics().qwpEgressMetrics();
                long before = metrics.cacheResetDictCount();

                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    java.util.Set<String> got = new java.util.HashSet<>();
                    client.execute("SELECT sym FROM cap_heap", collectInto(got));
                    Assert.assertEquals(java.util.Set.of("quad", "pent", "hexo"), got);
                    // Re-run to trigger at least one post-query reset.
                    java.util.Set<String> got2 = new java.util.HashSet<>();
                    client.execute("SELECT sym FROM cap_heap", collectInto(got2));
                    Assert.assertEquals(java.util.Set.of("quad", "pent", "hexo"), got2);
                }
                Assert.assertTrue("heap-bytes cap must trigger at least one dict reset",
                        metrics.cacheResetDictCount() > before);
            }
        });
    }

    /**
     * Schemas cap at 2: run three queries with distinct column shapes and
     * assert {@code CACHE_RESET} with the {@code RESET_MASK_SCHEMAS} bit fires.
     */
    @Test
    public void testCacheResetFiresOnSchemaCap() throws Exception {
        QwpEgressProcessorState.defaultMaxSchemasOverrideForTest = 2;
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    "QDB_METRICS_ENABLED", "true")) {
                serverMain.execute("CREATE TABLE cap_schemas(a LONG, b INT, c DOUBLE, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO cap_schemas VALUES (1, 2, 3.0, 1::TIMESTAMP)");
                serverMain.awaitTable("cap_schemas");
                QwpEgressMetrics metrics = serverMain.getEngine().getMetrics().qwpEgressMetrics();
                long before = metrics.cacheResetSchemasCount();

                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();

                    // Three distinct shapes -> cap of 2 trips on the 2nd or 3rd
                    // query. We don't care which, only that a schema reset
                    // fires somewhere in the sequence.
                    final int[] batches = {0};
                    client.execute("SELECT a FROM cap_schemas", noopHandler(batches));
                    client.execute("SELECT b FROM cap_schemas", noopHandler(batches));
                    client.execute("SELECT c FROM cap_schemas", noopHandler(batches));
                    Assert.assertTrue("at least one RESULT_BATCH must deliver", batches[0] > 0);
                }
                Assert.assertTrue("schema cap must trigger at least one schema reset",
                        metrics.cacheResetSchemasCount() > before);
            }
        });
    }

    /**
     * Under default caps, no {@code CACHE_RESET} must fire for a small
     * workload. Pins the contract that the reset path is quiescent in normal
     * operation.
     */
    @Test
    public void testCacheResetDoesNotFireUnderDefaults() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    "QDB_METRICS_ENABLED", "true")) {
                serverMain.execute("CREATE TABLE quiet(sym SYMBOL, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO quiet VALUES ('x', 1::TIMESTAMP)");
                serverMain.awaitTable("quiet");
                QwpEgressMetrics metrics = serverMain.getEngine().getMetrics().qwpEgressMetrics();
                long dictBefore = metrics.cacheResetDictCount();
                long schemasBefore = metrics.cacheResetSchemasCount();

                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    java.util.Set<String> got = new java.util.HashSet<>();
                    for (int i = 0; i < 5; i++) {
                        got.clear();
                        client.execute("SELECT sym FROM quiet", collectInto(got));
                        Assert.assertEquals(java.util.Set.of("x"), got);
                    }
                }
                Assert.assertEquals("no dict reset expected under defaults",
                        dictBefore, metrics.cacheResetDictCount());
                Assert.assertEquals("no schemas reset expected under defaults",
                        schemasBefore, metrics.cacheResetSchemasCount());
            }
        });
    }

    /**
     * After a reset the connection must remain healthy: follow-up queries
     * succeed and decode their data correctly. Distinct from the previous
     * tests in that this exercises a reset AND then a large follow-up query
     * that relies on a freshly populated dict.
     */
    @Test
    public void testConnectionStaysHealthyAfterReset() throws Exception {
        QwpEgressProcessorState.defaultMaxDictEntriesOverrideForTest = 2;
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    "QDB_METRICS_ENABLED", "true")) {
                serverMain.execute("CREATE TABLE mixed(sym SYMBOL, n LONG, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("""
                        INSERT INTO mixed
                        SELECT CASE WHEN x % 5 = 0 THEN 'alpha' WHEN x % 5 = 1 THEN 'beta'
                                    WHEN x % 5 = 2 THEN 'gamma' WHEN x % 5 = 3 THEN 'delta'
                                    ELSE 'epsilon' END,
                               x,
                               x::TIMESTAMP
                        FROM long_sequence(10_000)
                        """);
                serverMain.awaitTable("mixed");

                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();

                    // Query 1: trips dict cap (5 distinct symbols, cap=2).
                    final int[] rows1 = {0};
                    final java.util.Set<String> set1 = new java.util.HashSet<>();
                    client.execute("SELECT sym FROM mixed", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                DirectUtf8Sequence v = batch.getStrA(0, r);
                                Assert.assertNotNull(v);
                                set1.add(v.toString());
                                rows1[0]++;
                            }
                        }

                        @Override
                        public void onEnd(long total) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("q1 must succeed: " + message);
                        }
                    });
                    Assert.assertEquals(10_000, rows1[0]);
                    Assert.assertEquals(java.util.Set.of("alpha", "beta", "gamma", "delta", "epsilon"), set1);

                    // Query 2: server has reset its dict; client must still
                    // resolve every symbol correctly.
                    final int[] rows2 = {0};
                    final java.util.Set<String> set2 = new java.util.HashSet<>();
                    client.execute("SELECT sym FROM mixed", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                DirectUtf8Sequence v = batch.getStrA(0, r);
                                Assert.assertNotNull(v);
                                set2.add(v.toString());
                                rows2[0]++;
                            }
                        }

                        @Override
                        public void onEnd(long total) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("q2 after reset must succeed: " + message);
                        }
                    });
                    Assert.assertEquals(10_000, rows2[0]);
                    Assert.assertEquals(set1, set2);
                }
            }
        });
    }

    /**
     * Reset only triggers at query boundaries -- a stream that runs over many
     * batches and hits the cap mid-stream keeps using its in-flight dict until
     * the stream ends. Verifies in-flight queries aren't disrupted by caps
     * crossed mid-batch.
     */
    @Test
    public void testNoResetMidStream() throws Exception {
        QwpEgressProcessorState.defaultMaxDictEntriesOverrideForTest = 3;
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    "QDB_METRICS_ENABLED", "true")) {
                serverMain.execute("CREATE TABLE mid(sym SYMBOL, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                // Enough rows to force multiple batches; many unique symbols
                // so the dict grows past the cap.
                serverMain.execute("""
                        INSERT INTO mid
                        SELECT 'sym_' || x::VARCHAR,
                               x::TIMESTAMP
                        FROM long_sequence(50_000)
                        """);
                serverMain.awaitTable("mid");

                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    final int[] rows = {0};
                    final java.util.Set<String> uniq = new java.util.HashSet<>();
                    client.execute("SELECT sym FROM mid", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                DirectUtf8Sequence v = batch.getStrA(0, r);
                                Assert.assertNotNull("in-flight row " + rows[0] + " must resolve", v);
                                uniq.add(v.toString());
                                rows[0]++;
                            }
                        }

                        @Override
                        public void onEnd(long total) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("mid-stream reset should not disrupt the query: " + message);
                        }
                    });
                    Assert.assertEquals(50_000, rows[0]);
                    Assert.assertEquals(50_000, uniq.size());
                }
            }
        });
    }

    private static QwpColumnBatchHandler collectInto(java.util.Set<String> sink) {
        return new QwpColumnBatchHandler() {
            @Override
            public void onBatch(QwpColumnBatch batch) {
                for (int r = 0; r < batch.getRowCount(); r++) {
                    DirectUtf8Sequence v = batch.getStrA(0, r);
                    if (v != null) {
                        sink.add(v.toString());
                    }
                }
            }

            @Override
            public void onEnd(long total) {
            }

            @Override
            public void onError(byte status, String message) {
                Assert.fail("query must succeed: " + message);
            }
        };
    }

    private static QwpColumnBatchHandler noopHandler(int[] batchCounter) {
        return new QwpColumnBatchHandler() {
            @Override
            public void onBatch(QwpColumnBatch batch) {
                batchCounter[0]++;
            }

            @Override
            public void onEnd(long total) {
            }

            @Override
            public void onError(byte status, String message) {
                Assert.fail("query must succeed: " + message);
            }
        };
    }
}
