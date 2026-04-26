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

import io.questdb.cairo.CairoException;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.cutlass.qwp.server.egress.QwpEgressUpgradeProcessor;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Coverage for the two egress-specific {@code QUERY_ERROR} status codes:
 * {@code STATUS_CANCELLED} and {@code STATUS_LIMIT_EXCEEDED}.
 * <p>
 * The unit tests exercise {@code QwpEgressUpgradeProcessor.mapErrorStatus} for
 * every {@code CairoException} classification that should surface as a QWP
 * status (authorization, cancellation, interruption, OOM, generic).
 * <p>
 * The e2e tests exercise the CANCEL frame plumbing:
 * <ul>
 *   <li>Cancel after completion is a no-op on the next query.</li>
 *   <li>Cancel from a side thread mid-unbounded-stream does not corrupt the
 *       connection. Note: for unbounded-credit streams the server is typically
 *       WRITE-parked on the dispatcher, so the CANCEL frame is only observed
 *       after the query has already completed -- this test asserts connection
 *       health post-race, not the status code.</li>
 *   <li>Cancel with no query in flight is silently dropped.</li>
 *   <li>Mid-stream cancel under credit-based flow control DOES surface as
 *       {@code onError(STATUS_CANCELLED, ...)}. Under credit flow the server
 *       parks cooperatively between batches (fd re-registered for READ), so
 *       CANCEL is dispatched before the next {@code streamResults} re-entry.</li>
 * </ul>
 * Mid-stream {@code STATUS_CANCELLED} delivery for default unbounded-credit
 * streams is tracked as a Phase-2 item (dispatcher dual-registration for
 * read+write during streaming).
 */
public class QwpEgressCancelTest extends AbstractBootstrapTest {

    // Status codes (copies; avoids dragging the server-side enum into the test path).
    private static final byte STATUS_CANCELLED = 0x0A;
    private static final byte STATUS_INTERNAL_ERROR = 0x06;
    private static final byte STATUS_LIMIT_EXCEEDED = 0x0B;
    private static final byte STATUS_PARSE_ERROR = 0x05;
    private static final byte STATUS_SECURITY_ERROR = 0x08;

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testCancelAfterResultEndIsIgnored() throws Exception {
        // Cancel fired after the query has already completed must not corrupt
        // the connection: a follow-up query on the same client must run cleanly.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE q1(id LONG, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO q1 VALUES (1, 1::TIMESTAMP), (2, 2::TIMESTAMP), (3, 3::TIMESTAMP)");
                serverMain.awaitTable("q1");

                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();

                    int[] q1Rows = {0};
                    client.execute("SELECT * FROM q1", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            q1Rows[0] += batch.getRowCount();
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("q1 error: " + message);
                        }
                    });
                    Assert.assertEquals(3, q1Rows[0]);
                    // currentRequestId has been cleared by execute()'s finally block,
                    // so cancel() is a no-op: no frame gets sent.
                    client.cancel();

                    // Follow-up query works as expected.
                    int[] q2Rows = {0};
                    client.execute("SELECT * FROM q1", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            q2Rows[0] += batch.getRowCount();
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("q2 error: " + message);
                        }
                    });
                    Assert.assertEquals(3, q2Rows[0]);
                }
            }
        });
    }

    @Test
    public void testCancelFromIdleConnectionDoesNotDisruptNextQuery() throws Exception {
        // Calls cancel() from a background thread with random timing while a
        // small query runs. Under the current single-op dispatcher, mid-stream
        // CANCEL is consumed only after the query completes; verify that it
        // still doesn't break subsequent queries on the same connection.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE small(id LONG, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO small SELECT x, x::TIMESTAMP FROM long_sequence(1000)");
                serverMain.awaitTable("small");

                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();

                    // Fire a cancel from a side thread at a jittered moment.
                    Thread canceler = new Thread(() -> {
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        }
                        client.cancel();
                    }, "cancel-racer");
                    canceler.setDaemon(true);
                    canceler.start();

                    int[] rows1 = {0};
                    boolean[] errored = {false};
                    client.execute("SELECT * FROM small", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            rows1[0] += batch.getRowCount();
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            // Tolerated: cancel may have landed.
                            errored[0] = true;
                        }
                    });
                    canceler.join(2_000);
                    // Either the cancel raced in and surfaced as onError, or the
                    // full result streamed before the cancel was observed. Any
                    // partial non-zero row count with no error would indicate a
                    // truncated stream, which must not happen silently.
                    Assert.assertTrue(
                            "rows=" + rows1[0] + ", errored=" + errored[0],
                            errored[0] || rows1[0] == 1000
                    );

                    // Follow-up query must succeed cleanly, proving the cancel
                    // didn't corrupt the connection or streaming state.
                    int[] rows2 = {0};
                    client.execute("SELECT * FROM small", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            rows2[0] += batch.getRowCount();
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("follow-up query failed: " + message);
                        }
                    });
                    Assert.assertEquals(1000, rows2[0]);
                }
            }
        });
    }

    @Test
    public void testCancelWithNoInFlightQueryIsNoOp() throws Exception {
        // cancel() must tolerate being called when no query is active. The
        // client's currentRequestId is -1 in that window, so cancel() short-circuits
        // before sending anything.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE mini(x INT, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO mini VALUES (1, 1::TIMESTAMP)");
                serverMain.awaitTable("mini");

                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    // No query in flight.
                    client.cancel(); // must not throw

                    int[] rows = {0};
                    client.execute("SELECT * FROM mini", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            rows[0] += batch.getRowCount();
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("query error: " + message);
                        }
                    });
                    Assert.assertEquals(1, rows[0]);
                }
            }
        });
    }

    @Test
    public void testMapErrorStatusAuthorization() {
        CairoException ce = CairoException.authorization().put("denied");
        Assert.assertEquals(STATUS_SECURITY_ERROR, QwpEgressUpgradeProcessor.mapErrorStatus(ce));
    }

    @Test
    public void testMapErrorStatusCancellation() {
        // Cancellation takes precedence over interruption (the cancellation
        // helpers set both flags; we must not misclassify as LIMIT_EXCEEDED).
        CairoException ce = CairoException.queryCancelled();
        Assert.assertTrue(ce.isCancellation());
        Assert.assertTrue(ce.isInterruption());
        Assert.assertEquals(STATUS_CANCELLED, QwpEgressUpgradeProcessor.mapErrorStatus(ce));
    }

    @Test
    public void testMapErrorStatusGenericCairo() {
        CairoException ce = CairoException.nonCritical().put("something broke");
        Assert.assertEquals(STATUS_INTERNAL_ERROR, QwpEgressUpgradeProcessor.mapErrorStatus(ce));
    }

    @Test
    public void testMapErrorStatusInterruptionWithoutCancellation() {
        // A bare interruption (e.g. timeout) surfaces as LIMIT_EXCEEDED.
        CairoException ce = CairoException.queryTimedOut();
        Assert.assertTrue(ce.isInterruption());
        Assert.assertFalse(ce.isCancellation());
        Assert.assertEquals(STATUS_LIMIT_EXCEEDED, QwpEgressUpgradeProcessor.mapErrorStatus(ce));
    }

    @Test
    public void testMapErrorStatusOutOfMemory() {
        CairoException ce = CairoException.nonCritical().put("oom").setOutOfMemory(true);
        Assert.assertTrue(ce.isOutOfMemory());
        Assert.assertEquals(STATUS_LIMIT_EXCEEDED, QwpEgressUpgradeProcessor.mapErrorStatus(ce));
    }

    @Test
    public void testMapErrorStatusPlainException() {
        Assert.assertEquals(STATUS_INTERNAL_ERROR,
                QwpEgressUpgradeProcessor.mapErrorStatus(new RuntimeException("boom")));
    }

    @Test
    public void testMapErrorStatusQwpParseException() {
        // Client-initiated protocol parse errors (bad bind type, truncated frame)
        // must surface as STATUS_PARSE_ERROR, not STATUS_INTERNAL_ERROR.
        QwpParseException e = QwpParseException.instance(
                QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("truncated frame");
        Assert.assertEquals(STATUS_PARSE_ERROR, QwpEgressUpgradeProcessor.mapErrorStatus(e));
    }

    @Test
    public void testMapErrorStatusSqlException() {
        io.questdb.griffin.SqlException sx = io.questdb.griffin.SqlException.$(0, "bad sql");
        Assert.assertEquals(STATUS_PARSE_ERROR, QwpEgressUpgradeProcessor.mapErrorStatus(sx));
    }

    @Test
    public void testMidStreamCancelUnderCreditFlowSurfacesStatusCancelled() throws Exception {
        // Under credit-based flow control the server parks between batches waiting
        // for a CREDIT frame. That park is a cooperative yield -- the dispatcher
        // re-registers the fd for READ while the stream is suspended, so a CANCEL
        // frame sent by the client IS observed before the next batch. This is the
        // one path where mid-stream cancel surfaces as STATUS_CANCELLED under the
        // current single-op-per-fd dispatcher (the Phase-2 limitation applies to
        // default unbounded-credit streams where the server is WRITE-parked).
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE big AS (SELECT x AS id, x::TIMESTAMP AS ts "
                        + "FROM long_sequence(500_000)) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.awaitTable("big");

                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                                "ws::addr=127.0.0.1:" + HTTP_PORT + ";")
                        .withInitialCredit(1024)) {
                    client.connect();

                    final int[] batchCount = {0};
                    final int[] rows = {0};
                    final byte[] observedStatus = {-1};
                    final String[] observedMessage = {null};
                    final boolean[] endSeen = {false};
                    client.execute("SELECT * FROM big", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            rows[0] += batch.getRowCount();
                            batchCount[0]++;
                            if (batchCount[0] == 1) {
                                // Issue cancel after receiving the first batch. The auto-release
                                // on return from this callback flushes a CREDIT; the I/O thread's
                                // next loop iteration then drains pendingCancel and sends CANCEL.
                                // The server may still deliver one or two more batches before it
                                // sees the flag on its next streamResults re-entry, but the stream
                                // must end in STATUS_CANCELLED rather than RESULT_END.
                                client.cancel();
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                            endSeen[0] = true;
                        }

                        @Override
                        public void onError(byte status, String message) {
                            observedStatus[0] = status;
                            observedMessage[0] = message;
                        }
                    });

                    Assert.assertFalse("stream completed instead of cancelling", endSeen[0]);
                    Assert.assertEquals(
                            "expected STATUS_CANCELLED, got status=" + observedStatus[0]
                                    + ", message=" + observedMessage[0],
                            STATUS_CANCELLED, observedStatus[0]);
                    Assert.assertTrue("rows streamed before cancel=" + rows[0],
                            rows[0] > 0 && rows[0] < 500_000);
                }
            }
        });
    }
}
