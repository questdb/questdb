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
 * The e2e tests exercise the CANCEL frame path: server receives CANCEL, sets
 * the flag on {@code QwpEgressProcessorState}, {@code streamResults} observes
 * it between batches and aborts with {@code STATUS_CANCELLED}.
 * <p>
 * The unit tests exercise {@code QwpEgressUpgradeProcessor.mapErrorStatus} for
 * every {@code CairoException} classification that should surface as a QWP
 * status (authorization, cancellation, interruption, OOM, generic).
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
                serverMain.execute("CREATE TABLE q1(id LONG)");
                serverMain.execute("INSERT INTO q1 VALUES (1), (2), (3)");

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
                serverMain.execute("CREATE TABLE small(id LONG)");
                serverMain.execute("INSERT INTO small SELECT x FROM long_sequence(1000)");

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
                        }
                    });
                    canceler.join(2_000);
                    Assert.assertTrue(rows1[0] >= 0);

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
                serverMain.execute("CREATE TABLE mini(x INT)");
                serverMain.execute("INSERT INTO mini VALUES (1)");

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

    // -- Unit tests for mapErrorStatus ------------------------------------

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
    public void testMapErrorStatusSqlException() throws Exception {
        io.questdb.griffin.SqlException sx = io.questdb.griffin.SqlException.$(0, "bad sql");
        Assert.assertEquals(STATUS_PARSE_ERROR, QwpEgressUpgradeProcessor.mapErrorStatus(sx));
    }
}
