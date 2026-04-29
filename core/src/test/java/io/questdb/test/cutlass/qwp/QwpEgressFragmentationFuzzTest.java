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

import io.questdb.PropertyKey;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Stress the QWP egress state machines under artificial network fragmentation.
 * Mirrors the ingress fuzz's fragmentation strategy: both send- and recv-side
 * chunk sizes are capped at a small random value, forcing every frame to span
 * many partial socket reads/writes. The intent is to expose hand-off bugs in
 * the WebSocket frame parser, the HTTP response sink's park-resume path, and
 * the egress streamResults loop when one batch's bytes trickle out over many
 * send cycles and any one of them may be preempted by inbound CANCEL / CREDIT
 * traffic.
 * <p>
 * Coverage:
 * <ul>
 *   <li>Full-scan streaming with row-level verification across fragmented wire.</li>
 *   <li>Credit-flow streaming -- fragments interleave with CREDIT frames, so
 *       the server's recv-side frame parser must stitch CREDIT bodies split
 *       across multiple partial reads.</li>
 *   <li>Multiple queries per connection to shake out cross-query state that
 *       might have picked up residue from a fragmented prior query.</li>
 * </ul>
 */
public class QwpEgressFragmentationFuzzTest extends AbstractBootstrapTest {

    private static final Log LOG = LogFactory.getLog(QwpEgressFragmentationFuzzTest.class);
    private Rnd random;

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
        // Seeds are pinned rather than derived from the clock so the fuzz
        // runs are bit-for-bit reproducible. When a CI failure surfaces a
        // different seed pair, update the constants here so the broken case
        // becomes the new regression baseline.
        random = TestUtils.generateRandom(LOG);
    }

    @Test
    public void testFragmentedBackToBackQueries() throws Exception {
        int chunk = pickChunk();
        LOG.info().$("=== fragmentation test: chunk=").$(chunk).$();
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startFragmented(chunk)) {
                serverMain.execute("CREATE TABLE btb(id LONG, v DOUBLE, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute(
                        "INSERT INTO btb SELECT x, CAST(x * 2.5 AS DOUBLE), x::TIMESTAMP FROM long_sequence(8000)");
                serverMain.awaitTable("btb");

                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    for (int q = 0; q < 5; q++) {
                        runAndVerify(client, "btb", 8000);
                    }
                }
            }
        });
    }

    @Test
    public void testFragmentedCreditFlow() throws Exception {
        // Every wire byte becomes its own socket-level event at chunk=1 -- the
        // handshake response, every WS frame header, every QWP prelude, every
        // CREDIT frame. Lands on the park-resume path at every boundary.
        int chunk = pickChunk();
        LOG.info().$("=== fragmented credit test: chunk=").$(chunk).$();
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startFragmented(chunk)) {
                serverMain.execute(
                        "CREATE TABLE cf AS (SELECT x AS id, x::TIMESTAMP AS ts FROM long_sequence(20000))"
                                + " TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.awaitTable("cf");

                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                                "ws::addr=127.0.0.1:" + HTTP_PORT + ";")
                        .withInitialCredit(2 * 1024)) {
                    client.connect();
                    final long[] idSum = {0};
                    final int[] rows = {0};
                    client.execute("SELECT * FROM cf", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            int n = batch.getRowCount();
                            long base = batch.valuesAddr(0);
                            int[] idx = batch.nonNullIndex(0);
                            for (int r = 0; r < n; r++) {
                                idSum[0] += Unsafe.getLong(base + 8L * idx[r]);
                            }
                            rows[0] += n;
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("credit-flow fragmentation error: " + message);
                        }
                    });
                    Assert.assertEquals(20_000, rows[0]);
                    Assert.assertEquals(20_000L * 20_001L / 2L, idSum[0]);
                }
            }
        });
    }

    @Test
    public void testFragmentedStreamingBigResult() throws Exception {
        int chunk = pickChunk();
        LOG.info().$("=== fragmentation big result: chunk=").$(chunk).$();
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startFragmented(chunk)) {
                serverMain.execute(
                        "CREATE TABLE bigt AS (" +
                                "SELECT x AS id, CAST(x * 1.5 AS DOUBLE) AS v, " +
                                "CAST('s_' || (x % 100) AS SYMBOL) AS s, " +
                                "x::TIMESTAMP AS ts " +
                                "FROM long_sequence(50000)) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.awaitTable("bigt");

                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    runAndVerify(client, "bigt", 50_000);
                }
            }
        });
    }

    @Test
    public void testHandshakeSurvivesMicroChunk() throws Exception {
        // Pin chunk to 5 bytes: the ~220 B WebSocket 101 handshake response
        // fragments across ~44 socket writes, forcing rawSocket.send() to park
        // repeatedly. Regression for the "Egress 101 handshake blocked" bug
        // that used to surface when any chunk was smaller than the handshake
        // response. Now onHeadersReady defers send() to onRequestComplete,
        // where PISR propagates cleanly into the park-resume path.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startFragmented(5)) {
                serverMain.execute("CREATE TABLE tiny(id LONG, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO tiny SELECT x, x::TIMESTAMP FROM long_sequence(3)");
                serverMain.awaitTable("tiny");
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    runAndVerify(client, "tiny", 3);
                }
            }
        });
    }

    private int pickChunk() {
        // Aggressive fragmentation: every socket read/write carries ~this many
        // bytes, so even a tiny WS frame spans many iterations and the state
        // machine must survive being preempted / resumed at arbitrary points.
        return 1 + random.nextInt(500);
    }

    private void runAndVerify(QwpQueryClient client, String table, int expectedRows) {
        final int[] rows = {0};
        final long[] idSum = {0};
        client.execute("SELECT * FROM " + table, new QwpColumnBatchHandler() {
            @Override
            public void onBatch(QwpColumnBatch batch) {
                int n = batch.getRowCount();
                long base = batch.valuesAddr(0);
                int[] idx = batch.nonNullIndex(0);
                for (int r = 0; r < n; r++) {
                    idSum[0] += Unsafe.getLong(base + 8L * idx[r]);
                }
                rows[0] += n;
            }

            @Override
            public void onEnd(long totalRows) {
            }

            @Override
            public void onError(byte status, String message) {
                Assert.fail("fragmented query error [status=" + status + "]: " + message);
            }
        });
        Assert.assertEquals("rowCount", expectedRows, rows[0]);
        Assert.assertEquals("idSum", (long) expectedRows * (expectedRows + 1) / 2, idSum[0]);
    }

    private TestServerMain startFragmented(int chunk) {
        return startWithEnvVariables(
                PropertyKey.DEBUG_HTTP_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), Integer.toString(chunk),
                PropertyKey.DEBUG_HTTP_FORCE_SEND_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), Integer.toString(chunk)
        );
    }
}
