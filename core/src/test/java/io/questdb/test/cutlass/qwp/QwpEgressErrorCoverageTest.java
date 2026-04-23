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
import io.questdb.griffin.CompiledQuery;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * End-to-end error-path coverage for egress. Complements the server-side
 * request-decoder hardening ({@code QwpEgressRequestDecoderTest}) and the
 * client-side result-decoder hardening ({@code QwpResultBatchDecoderHardeningTest})
 * with scenarios that fall out of normal user operation: runtime DDL failures,
 * type mismatches, peer disconnects mid-stream, and handshake-level rejections.
 */
public class QwpEgressErrorCoverageTest extends AbstractBootstrapTest {

    // STATUS_PARSE_ERROR (0x05) and STATUS_INTERNAL_ERROR (0x06) from the
    // egress wire. Copied as literals so this test doesn't drag in the
    // server-side module.
    private static final byte STATUS_INTERNAL_ERROR = 0x06;
    private static final byte STATUS_PARSE_ERROR = 0x05;

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testAlterAddColumnThatAlreadyExists() throws Exception {
        // Adding a column with a name that's already on the table is a
        // classic user mistake. The compiler rejects it; we want to see the
        // rejection routed via QUERY_ERROR rather than killing the connection.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE dup(x LONG, y DOUBLE, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    final String[] errorMsg = {null};
                    final byte[] errorStatus = {0};
                    client.execute("ALTER TABLE dup ADD COLUMN y DOUBLE",
                            failIfSuccess(errorStatus, errorMsg));
                    Assert.assertNotNull("expected error for duplicate column", errorMsg[0]);
                    Assert.assertEquals(STATUS_PARSE_ERROR, errorStatus[0]);
                    // After the error, the connection is still usable: the next
                    // query goes through. Catches state corruption in the
                    // handleQueryRequest error arm.
                    final short[] opType = {-1};
                    client.execute("INSERT INTO dup VALUES (1, 2.0, 1::TIMESTAMP)", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            Assert.fail("unexpected batch");
                        }

                        @Override
                        public void onEnd(long totalRows) {
                            Assert.fail("unexpected end");
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("follow-up INSERT should succeed, got: " + message);
                        }

                        @Override
                        public void onExecDone(short ot, long rowsAffected) {
                            opType[0] = ot;
                        }
                    });
                    Assert.assertEquals(CompiledQuery.INSERT, opType[0]);
                }
            }
        });
    }

    @Test
    public void testDropNonExistentTable() throws Exception {
        // DROP of a table that doesn't exist should be a clean QUERY_ERROR, not
        // a crash or silent success. Covers the CairoException path through
        // Operation.execute on DDL.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain ignored = startWithEnvVariables()) {
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    final String[] errorMsg = {null};
                    final byte[] errorStatus = {0};
                    client.execute("DROP TABLE does_not_exist",
                            failIfSuccess(errorStatus, errorMsg));
                    Assert.assertNotNull(errorMsg[0]);
                    Assert.assertEquals(STATUS_PARSE_ERROR, errorStatus[0]);
                    // Message points at the bad table name rather than a generic failure.
                    Assert.assertTrue(
                            "error should mention table name, got: " + errorMsg[0],
                            errorMsg[0].toLowerCase().contains("does_not_exist")
                                    || errorMsg[0].toLowerCase().contains("not exist")
                    );
                }
            }
        });
    }

    @Test
    public void testInsertTypeMismatch() throws Exception {
        // Inserting a STRING into a LONG column. Depending on where the cast
        // fails -- compile time (SqlException -> PARSE_ERROR) or runtime
        // (CairoException -> INTERNAL_ERROR) -- the status byte differs. What
        // matters to the client is that the error surfaces cleanly and the
        // connection stays usable. Accept either code and then run a
        // follow-up query to confirm liveness.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE strict(x LONG, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    final String[] errorMsg = {null};
                    final byte[] errorStatus = {0};
                    client.execute("INSERT INTO strict VALUES ('not_a_long', 1::TIMESTAMP)",
                            failIfSuccess(errorStatus, errorMsg));
                    Assert.assertNotNull(errorMsg[0]);
                    Assert.assertTrue(
                            "expected PARSE_ERROR (0x05) or INTERNAL_ERROR (0x06), got 0x"
                                    + Integer.toHexString(errorStatus[0] & 0xFF),
                            errorStatus[0] == STATUS_PARSE_ERROR
                                    || errorStatus[0] == STATUS_INTERNAL_ERROR
                    );
                    // Connection is still usable for a valid insert after the error.
                    final short[] opType = {-1};
                    client.execute("INSERT INTO strict VALUES (42, 2::TIMESTAMP)", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            Assert.fail("unexpected batch");
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("follow-up INSERT should succeed: " + message);
                        }

                        @Override
                        public void onExecDone(short ot, long ra) {
                            opType[0] = ot;
                        }
                    });
                    Assert.assertEquals(CompiledQuery.INSERT, opType[0]);
                }
            }
        });
    }

    @Test
    public void testPeerDisconnectMidStreamDoesNotCrashServer() throws Exception {
        // Client opens a big streaming query, then closes before consuming all
        // batches. Server observes PeerDisconnectedException / ServerDisconnect
        // partway through; must release the cursor / factory without leaking and
        // must still serve a fresh connection afterwards.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE big(x LONG, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                // 50k rows guarantees multi-batch streaming (MAX_ROWS_PER_BATCH = 4096).
                serverMain.execute("INSERT INTO big SELECT x, x::TIMESTAMP FROM long_sequence(50000)");
                serverMain.awaitTable("big");

                // Client #1: close mid-stream.
                final CountDownLatch firstBatchSeen = new CountDownLatch(1);
                final Thread[] workerRef = new Thread[1];
                try (QwpQueryClient c1 = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)) {
                    c1.connect();
                    // Run the query on a worker thread so this thread can close
                    // the client mid-callback. Without that, close() can't run
                    // while execute() is parked inside onBatch.
                    Thread worker = new Thread(() -> {
                        try {
                            c1.execute("SELECT x FROM big ORDER BY x", new QwpColumnBatchHandler() {
                                @Override
                                public void onBatch(QwpColumnBatch batch) {
                                    // Signal that the first batch has arrived so the
                                    // test thread can close() and guarantee we are
                                    // mid-stream rather than racing timer-based sleep.
                                    firstBatchSeen.countDown();
                                    // Block inside the first batch callback so close()
                                    // fires while the server is mid-stream.
                                    try {
                                        Thread.sleep(30_000);
                                    } catch (InterruptedException ie) {
                                        Thread.currentThread().interrupt();
                                    }
                                }

                                @Override
                                public void onEnd(long totalRows) {
                                }

                                @Override
                                public void onError(byte status, String message) {
                                    // Expected once we close the client.
                                }
                            });
                        } catch (Throwable ignored) {
                            // Close propagates as an error / interrupt; swallow.
                        }
                    });
                    workerRef[0] = worker;
                    worker.start();
                    Assert.assertTrue(
                            "first RESULT_BATCH did not arrive within 30s -- server may be stalled",
                            firstBatchSeen.await(30, TimeUnit.SECONDS)
                    );
                    // Leaves try-with-resources here, triggering close() which
                    // interrupts the worker and shuts the I/O thread.
                }
                // Join the worker so stray threads do not outlive the memory-leak
                // check. It may still be parked in Thread.sleep(30_000) after the
                // client close; interrupt() unblocks it so join() completes promptly.
                workerRef[0].interrupt();
                workerRef[0].join(5_000);
                Assert.assertFalse("disconnect worker must not outlive the test",
                        workerRef[0].isAlive());

                // Client #2: fresh connection to the same server must work
                // normally. Catches server-state corruption triggered by the
                // first client's mid-stream abort.
                final long[] sum = {0};
                final int[] rows = {0};
                try (QwpQueryClient c2 = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    c2.connect();
                    c2.execute("SELECT x FROM big WHERE x <= 100", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                sum[0] += batch.getLongValue(0, r);
                                rows[0]++;
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("follow-up query failed: " + message);
                        }
                    });
                }
                Assert.assertEquals(100, rows[0]);
                Assert.assertEquals(100L * 101 / 2, sum[0]);
            }
        });
    }

    @Test
    public void testUpdateMatchesZeroRows() throws Exception {
        // UPDATE with a predicate that matches nothing should succeed with
        // rowsAffected = 0, not error out. Verifies the UPDATE code path
        // returns zero-rows as a first-class success.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                // Non-WAL: UPDATE's rowsAffected on WAL reports WAL-segment count, not
                // logical rows, which would break the zero-match assertion below.
                serverMain.execute("CREATE TABLE zu(ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY");
                serverMain.execute(
                        "INSERT INTO zu SELECT CAST((x - 1) * 1_000_000L AS TIMESTAMP), x " +
                                "FROM long_sequence(10)"
                );
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    final short[] opType = {-1};
                    final long[] rowsAffected = {-1L};
                    // x is always in [1, 10]; WHERE x > 1000 matches nothing.
                    client.execute("UPDATE zu SET x = 0 WHERE x > 1000", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            Assert.fail("unexpected batch");
                        }

                        @Override
                        public void onEnd(long totalRows) {
                            Assert.fail("unexpected end");
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("unexpected error: " + message);
                        }

                        @Override
                        public void onExecDone(short ot, long ra) {
                            opType[0] = ot;
                            rowsAffected[0] = ra;
                        }
                    });
                    Assert.assertEquals(CompiledQuery.UPDATE, opType[0]);
                    Assert.assertEquals(0L, rowsAffected[0]);
                }
            }
        });
    }

    @Test
    public void testWrongWebSocketVersionRejected() throws Exception {
        // Sec-WebSocket-Version: 8 is not supported (only 13 is). Mirrors
        // the equivalent ingress test; the egress endpoint shares the same
        // handshake validation path but there's no dedicated egress test for
        // this case before now.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain ignored = startWithEnvVariables()) {
                byte[] keyBytes = new byte[16];
                for (int i = 0; i < 16; i++) {
                    keyBytes[i] = (byte) (i * 31);
                }
                String wsKey = Base64.getEncoder().encodeToString(keyBytes);
                try (Socket socket = new Socket("127.0.0.1", HTTP_PORT)) {
                    socket.setSoTimeout(5_000);
                    String request = "GET /read/v1 HTTP/1.1\r\n" +
                            "Host: 127.0.0.1:" + HTTP_PORT + "\r\n" +
                            "Upgrade: websocket\r\n" +
                            "Connection: Upgrade\r\n" +
                            "Sec-WebSocket-Key: " + wsKey + "\r\n" +
                            "Sec-WebSocket-Version: 8\r\n" +
                            "\r\n";
                    socket.getOutputStream().write(request.getBytes(StandardCharsets.UTF_8));
                    socket.getOutputStream().flush();
                    BufferedReader reader = new BufferedReader(
                            new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                    String statusLine = reader.readLine();
                    Assert.assertNotNull("server must reply, not silently close", statusLine);
                    Assert.assertFalse(
                            "expected non-101 response for Sec-WebSocket-Version: 8, got: " + statusLine,
                            statusLine.contains("101")
                    );
                }
            }
        });
    }

    /**
     * Handler that fails the test on any success callback and captures the
     * error status + message produced by {@link QwpColumnBatchHandler#onError}.
     */
    private static QwpColumnBatchHandler failIfSuccess(byte[] statusOut, String[] messageOut) {
        return new QwpColumnBatchHandler() {
            @Override
            public void onBatch(QwpColumnBatch batch) {
                Assert.fail("unexpected batch");
            }

            @Override
            public void onEnd(long totalRows) {
                Assert.fail("unexpected end");
            }

            @Override
            public void onError(byte status, String message) {
                statusOut[0] = status;
                messageOut[0] = message;
            }

            @Override
            public void onExecDone(short opType, long rowsAffected) {
                Assert.fail("unexpected execDone");
            }
        };
    }
}
