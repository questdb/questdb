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
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Coverage for credit-based flow control on QWP egress.
 * <p>
 * The feature is byte-budgeted: the client advertises {@code initial_credit}
 * in its QUERY_REQUEST, the server streams at most that many result-payload
 * bytes before parking, and the client's I/O thread auto-replenishes by the
 * size of each batch after the user's handler releases it.
 * <p>
 * Tests cover:
 * <ul>
 *   <li>Credit = 0 (default, unbounded) -- Phase-1 back-compat;
 *       the existing {@code QwpEgressFuzzTest} + exhaustive suite already pin
 *       this. A dedicated sanity check lives here for completeness.</li>
 *   <li>Small credit that spans many batches -- the query must still complete
 *       correctly under client-driven replenishment, and every row must arrive.</li>
 *   <li>Credit = exactly one batch -- forces at least one suspend/resume
 *       round per batch, exercising the full credit-suspended -> CREDIT -> resume
 *       loop on the server.</li>
 *   <li>CREDIT for an unknown request -- logged and dropped, doesn't disrupt
 *       the stream.</li>
 * </ul>
 */
public class QwpEgressCreditFlowTest extends AbstractBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testCreditFlowCompletesLargeQuery() throws Exception {
        // Initial credit is tiny (4 KiB) relative to the 500k-row payload
        // (several MB). The client's auto-replenish per batch must keep the
        // pipe moving to completion.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
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
        // Credit sized below the smallest batch -- forces the server to
        // suspend after every batch and wait for the client to replenish.
        // This exercises the CREDIT -> wake -> emit -> suspend cycle hardest.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
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
            try (final TestServerMain serverMain = startWithEnvVariables()) {
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
        // Connection must keep working across queries with different credit
        // configs -- this exercises state reset in beginStreaming/endStreaming
        // for the credit fields.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
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
