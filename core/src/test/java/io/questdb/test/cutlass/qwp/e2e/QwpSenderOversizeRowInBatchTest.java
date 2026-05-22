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

package io.questdb.test.cutlass.qwp.e2e;

import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.cutlass.http.DefaultHttpContextConfiguration;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.HttpRequestHandlerFactory;
import io.questdb.cutlass.http.HttpServer;
import io.questdb.cutlass.qwp.server.QwpWebSocketHttpProcessor;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.PlainSocketFactory;
import io.questdb.std.ObjHashSet;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.net.ServerSocket;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;

/**
 * End-to-end red test for the "row too large" producer-side guard inside
 * {@code QwpWebSocketSender.sendRow}. The current guard is conditioned on
 * {@code pendingRowCount == 1}, so it inspects only the very first row of
 * a batch. Once a small row has accumulated, every subsequent row -- no
 * matter how large -- escapes the guard and is silently appended to the
 * pending batch. The auto-flush threshold then enqueues an oversize WS
 * frame that the server closes with {@code ws-close[1009 Message Too Big]},
 * surfacing the failure asynchronously rather than as the clean
 * {@code "row too large for server batch cap"} pre-flight exception the
 * guard was designed to produce.
 *
 * <p>This test:
 * <ol>
 *   <li>Stands up one QWP server with a tight HTTP recv buffer (128 KB),
 *       so the advertised {@code X-QWP-Max-Batch-Size} is ~131 KB.</li>
 *   <li>Sends a small row (raw bytes well under the cap), then a 600 KB
 *       row (raw bytes well over the cap) to the same table.</li>
 *   <li>Asserts the producer thread throws
 *       {@code "row too large for server batch cap"} on the oversize row's
 *       {@code at()} call.</li>
 * </ol>
 *
 * <p>With the bug, the producer guard does not fire on row 2, so the
 * {@code at()} call returns normally; the assertion that an exception
 * with that message was thrown fails. With a per-row delta guard the
 * exception fires synchronously and the test turns green.
 */
public class QwpSenderOversizeRowInBatchTest extends AbstractCairoTest {

    private static final Log LOG = LogFactory.getLog(QwpSenderOversizeRowInBatchTest.class);
    // 5 rows x 30 KB raw column data = 150 KB, comfortably above the
    // 131058 cap while each individual row stays far below it.
    private static final int BATCH_ROW_COUNT = 5;
    // 128 KB recv buffer  -> advertised cap = 128 * 1024 - 14 = 131058 bytes.
    private static final int RECV_BUFFER_SMALL_BYTES = 128 * 1024;
    private static final int ROW_CHUNK_BYTES = 30 * 1024;
    // 600 KB payload comfortably exceeds the 131058 cap with margin for
    // per-column metadata.
    private static final int ROW_PAYLOAD_SIZE_BYTES = 600_000;

    @Test
    public void testOversizeBatchFlushTripsProducerGuard() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int port = pickFreePort();
            QwpSidecar server = new QwpSidecar(port, RECV_BUFFER_SMALL_BYTES);
            try {
                server.start();

                // Disable byte-based auto-flush so the user-driven batch
                // grows past the server cap before the explicit flush().
                // High row/interval thresholds keep the row-count and
                // interval triggers out of the way. Each row's raw bytes
                // are well under the per-row guard, so the only protection
                // left is a flush-time wire-size check -- which today's
                // flushPendingRows does not have.
                String config = "ws::addr=127.0.0.1:" + port
                        + ";auto_flush_rows=10000"
                        + ";auto_flush_bytes=off"
                        + ";auto_flush_interval=60000;";

                try (Sender sender = Sender.builder(config).build()) {
                    char[] chunkChars = new char[ROW_CHUNK_BYTES];
                    Arrays.fill(chunkChars, 'x');
                    String chunk = new String(chunkChars);

                    // 5 rows x 30 KB = 150 KB raw, comfortably over the
                    // sidecar's ~131 KB cap. Each individual row is far
                    // below the cap so the per-row guard cannot help.
                    for (int i = 0; i < BATCH_ROW_COUNT; i++) {
                        sender.table("oversize_batch_t")
                                .stringColumn("payload", chunk)
                                .at(1_000L * (i + 1), ChronoUnit.MICROS);
                    }

                    LineSenderException thrown = null;
                    try {
                        sender.flush();
                    } catch (LineSenderException e) {
                        thrown = e;
                    }

                    Assert.assertNotNull(
                            "expected flush() to refuse a batch whose wire size exceeds"
                                    + " serverMaxBatchSize",
                            thrown);
                    String msg = thrown.getMessage();
                    Assert.assertNotNull(msg);
                    Assert.assertTrue(
                            "expected 'batch too large for server batch cap' message,"
                                    + " got: " + msg,
                            msg.contains("batch too large for server batch cap"));
                }
            } finally {
                server.stop();
                drainWalQueue();
                engine.releaseInactive();
            }
        });
    }

    @Test
    public void testOversizeRowInsideBatchTripsProducerGuard() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int port = pickFreePort();
            QwpSidecar server = new QwpSidecar(port, RECV_BUFFER_SMALL_BYTES);
            try {
                server.start();

                // High auto_flush_rows and auto_flush_bytes so the two
                // rows accumulate into a single batch on the client. The
                // producer guard is the only thing standing between the
                // oversize row and a wire-level rejection -- exactly the
                // condition the test is targeting.
                String config = "ws::addr=127.0.0.1:" + port
                        + ";auto_flush_rows=10000"
                        + ";auto_flush_bytes=" + (ROW_PAYLOAD_SIZE_BYTES * 2)
                        + ";auto_flush_interval=60000;";

                try (Sender sender = Sender.builder(config).build()) {
                    // Row 1: tiny. pendingRowCount becomes 1, the existing
                    // pendingRowCount==1 guard inspects this row and
                    // (correctly) lets it through.
                    sender.table("oversize_in_batch_t")
                            .longColumn("v", 1L)
                            .at(1_000L, ChronoUnit.MICROS);

                    // Row 2: a 600 KB string payload, well above the
                    // sidecar's ~131 KB cap. A per-row delta guard MUST
                    // fire on this row's at() call. Today, the guard is
                    // gated on pendingRowCount==1 (now 2), so the row
                    // commits silently and the test sees no exception.
                    char[] payloadChars = new char[ROW_PAYLOAD_SIZE_BYTES];
                    Arrays.fill(payloadChars, 'x');
                    String payload = new String(payloadChars);

                    LineSenderException thrown = null;
                    try {
                        sender.table("oversize_in_batch_t")
                                .stringColumn("payload", payload)
                                .at(2_000L, ChronoUnit.MICROS);
                    } catch (LineSenderException e) {
                        thrown = e;
                    }

                    Assert.assertNotNull(
                            "expected producer-side guard to reject the oversize second row of the batch",
                            thrown);
                    String msg = thrown.getMessage();
                    Assert.assertNotNull(msg);
                    Assert.assertTrue(
                            "expected 'row too large for server batch cap' message,"
                                    + " got: " + msg,
                            msg.contains("row too large for server batch cap"));
                }
            } finally {
                server.stop();
                drainWalQueue();
                engine.releaseInactive();
            }
        });
    }

    private static int pickFreePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            s.setReuseAddress(true);
            return s.getLocalPort();
        }
    }

    /**
     * Minimal QWP-only {@link HttpServer} with a configurable HTTP recv
     * buffer size so the test can stand up a peer whose advertised
     * {@code X-QWP-Max-Batch-Size} is deterministically tight. Shares the
     * test's {@code engine}.
     */
    private static final class QwpSidecar {
        private final int port;
        private final int recvBufferSize;
        private TestWorkerPool pool;
        private boolean running;
        private HttpServer server;

        QwpSidecar(int port, int recvBufferSize) {
            this.port = port;
            this.recvBufferSize = recvBufferSize;
        }

        void start() throws SqlException {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(
                    configuration,
                    new DefaultHttpContextConfiguration()
            ) {
                @Override
                public int getBindPort() {
                    return port;
                }

                @Override
                public int getRecvBufferSize() {
                    return recvBufferSize;
                }
            };
            pool = new TestWorkerPool(1);
            server = new HttpServer(httpConfig, pool, PlainSocketFactory.INSTANCE);
            server.bind(new HttpRequestHandlerFactory() {
                @Override
                public ObjHashSet<String> getUrls() {
                    return httpConfig.getContextPathQWP();
                }

                @Override
                public QwpWebSocketHttpProcessor newInstance() {
                    return new QwpWebSocketHttpProcessor(engine, httpConfig);
                }
            });
            // Intentionally skip setupWriterJobs: the test only exercises
            // the wire layer (handshake cap advertisement + client-side
            // guard) and does not need WAL apply behind it.
            pool.start(LOG);
            running = true;
        }

        void stop() {
            if (!running) {
                return;
            }
            running = false;
            try {
                pool.halt();
            } catch (Throwable t) {
                LOG.error().$("worker pool halt failed").$(t).$();
            }
            try {
                server.close();
            } catch (Throwable t) {
                LOG.error().$("server close failed").$(t).$();
            }
            server = null;
            pool = null;
        }
    }
}
