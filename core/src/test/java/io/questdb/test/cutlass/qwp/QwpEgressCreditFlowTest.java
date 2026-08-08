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
import io.questdb.std.Unsafe;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Coverage for credit-based flow control on QWP egress.
 * <p>
 * The feature is byte-budgeted: the client advertises {@code initial_credit} in
 * its QUERY_REQUEST, the server streams at most that many result-payload bytes
 * before parking, and the client's I/O thread auto-replenishes by each batch's
 * size after the handler releases it.
 * <p>
 * Tests cover:
 * <ul>
 *   <li>Credit = 0 (default) - unbounded back-compat path.</li>
 *   <li>Tiny credit over many batches - the query still completes and every row
 *       arrives under client-driven replenishment.</li>
 *   <li>Credit below one batch - forces a suspend/resume round per batch,
 *       exercising the suspend -> CREDIT -> resume loop on the server.</li>
 *   <li>Mixed credit across queries on one connection - credit state resets
 *       between queries.</li>
 * </ul>
 */
public class QwpEgressCreditFlowTest extends AbstractQwpBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
        // setUpFragmentationChunks (superclass @Before) draws sendChunk/recvChunk from
        // [1, 500]. A 1-byte draw streams this class's multi-MB payloads one byte per
        // dispatcher round-trip - millions of them, blowing the global timeout on slow
        // CI (the mac-other hang). Floor it: still fragments every batch, and credit
        // cycling tracks the credit budget not chunk size, so no coverage is lost.
        sendChunk = Math.max(sendChunk, 64);
        recvChunk = Math.max(recvChunk, 64);
    }

    @Test
    public void testCreditFlowCompletesLargeQuery() throws Exception {
        // Initial credit is tiny (4 KiB) relative to the 500k-row payload
        // (several MB). The client's auto-replenish per batch must keep the
        // pipe moving to completion.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startFragmented()) {
                serverMain.execute(
                        "CREATE TABLE big AS (SELECT x AS id, CAST(x * 1.5 AS DOUBLE) AS v, " +
                                "x::TIMESTAMP AS ts " +
                                "FROM long_sequence(500000)) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.awaitTable("big");

                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                                "ws::addr=127.0.0.1:" + HTTP_PORT + ";")
                        .withInitialCredit(4 * 1024)) {
                    client.connect();

                    final long[] idSum = {0};
                    final int[] rows = {0};
                    client.execute("SELECT * FROM big", new QwpColumnBatchHandler() {
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
                            Assert.fail("credit-flow query error: " + message);
                        }
                    });

                    Assert.assertEquals(500_000, rows[0]);
                    // sum(1..500000) = 500000*500001/2
                    Assert.assertEquals(500_000L * 500_001L / 2L, idSum[0]);
                }
            }
        });
    }

    @Test
    public void testCreditFlowTinyCreditLotsOfRoundTrips() throws Exception {
        // Credit below the smallest batch - the server suspends after every batch
        // and waits for the client to replenish, exercising the CREDIT -> wake ->
        // emit -> suspend cycle hardest.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startFragmented()) {
                serverMain.execute(
                        "CREATE TABLE tiny AS (SELECT x AS id, x::TIMESTAMP AS ts FROM long_sequence(50000))"
                                + " TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.awaitTable("tiny");

                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                                "ws::addr=127.0.0.1:" + HTTP_PORT + ";")
                        .withInitialCredit(1)) {
                    client.connect();

                    final int[] rows = {0};
                    client.execute("SELECT * FROM tiny", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            rows[0] += batch.getRowCount();
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("credit-flow query error: " + message);
                        }
                    });
                    Assert.assertEquals(50_000, rows[0]);
                }
            }
        });
    }

    @Test
    public void testCreditFlowZeroMeansUnbounded() throws Exception {
        // Sanity: default initial_credit (0) is the unbounded back-compat path.
        // Query runs without client-side CREDIT emission; server never checks.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startFragmented()) {
                serverMain.execute(
                        "CREATE TABLE un AS (SELECT x AS id, x::TIMESTAMP AS ts FROM long_sequence(10000))"
                                + " TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.awaitTable("un");

                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    final int[] rows = {0};
                    client.execute("SELECT * FROM un", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            rows[0] += batch.getRowCount();
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("unbounded query error: " + message);
                        }
                    });
                    Assert.assertEquals(10_000, rows[0]);
                }
            }
        });
    }

    @Test
    public void testMultipleQueriesOnSameConnectionMixedCredit() throws Exception {
        // The connection must keep working across queries with different credit
        // configs - exercises credit-field reset in beginStreaming/endStreaming.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startFragmented()) {
                serverMain.execute("CREATE TABLE a(id LONG, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO a SELECT x, x::TIMESTAMP FROM long_sequence(3000)");
                serverMain.awaitTable("a");

                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                                "ws::addr=127.0.0.1:" + HTTP_PORT + ";")
                        .withInitialCredit(8 * 1024)) {
                    client.connect();
                    for (int q = 0; q < 5; q++) {
                        final int queryIndex = q;
                        final int[] rows = {0};
                        client.execute("SELECT * FROM a", new QwpColumnBatchHandler() {
                            @Override
                            public void onBatch(QwpColumnBatch batch) {
                                rows[0] += batch.getRowCount();
                            }

                            @Override
                            public void onEnd(long totalRows) {
                            }

                            @Override
                            public void onError(byte status, String message) {
                                Assert.fail("query " + queryIndex + " error: " + message);
                            }
                        });
                        Assert.assertEquals(3000, rows[0]);
                    }
                }
            }
        });
    }

}
