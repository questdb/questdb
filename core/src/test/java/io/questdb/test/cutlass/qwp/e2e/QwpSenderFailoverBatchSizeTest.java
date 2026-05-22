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
import io.questdb.client.SenderConnectionEvent;
import io.questdb.client.SenderConnectionListener;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.cutlass.http.DefaultHttpContextConfiguration;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.HttpRequestHandlerFactory;
import io.questdb.cutlass.http.HttpServer;
import io.questdb.cutlass.qwp.server.QwpIngressHttpProcessor;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.PlainSocketFactory;
import io.questdb.std.ObjHashSet;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.ServerSocket;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * End-to-end coverage for the QWP ingress client's handling of the server's
 * advertised {@code X-QWP-Max-Batch-Size} across a mid-stream failover to a
 * peer with a tighter cap -- the rolling-upgrade scenario the
 * {@code QwpWebSocketSender.applyServerBatchSizeLimit} Javadoc explicitly
 * promises to handle.
 *
 * <p>The server formula in {@code QwpWebSocketUpgradeProcessor} is
 * {@code advertised = min(http.recv.buffer.size - 14, 16 MB)}. Standing up
 * two minimal QWP {@link HttpServer}s on the same {@code engine}, one with a
 * roomy recv buffer and one with a tight recv buffer, gives them
 * deterministically different advertised caps. The test then:
 *
 * <ol>
 *   <li>Points a single {@link Sender} at the address list {@code A,B},
 *       lets the initial connect land on A, and proves the wire is healthy
 *       with a small row.</li>
 *   <li>Stops A so the I/O thread observes the wire failure and the
 *       reconnect loop walks to B -- a real mid-stream failover.</li>
 *   <li>Pushes a single row whose raw bytes sit between the two caps:
 *       safe under A's, oversize under B's.</li>
 * </ol>
 *
 * <p>With the bug (current client code), the sender never refreshes its
 * cached {@code serverMaxBatchSize} on reconnect, so the post-failover
 * state still reflects A's roomy cap. The producer-thread "row too large"
 * guard in {@code QwpWebSocketSender.sendRow} therefore does NOT fire, and
 * the oversize row travels to B's wire only to be rejected downstream as
 * a {@code ws-close[1009 Message Too Big]} -- a fuzzy error path
 * unrelated to the cap.
 *
 * <p>With the fix, the cap is refreshed by the time the I/O thread
 * installs the new client, so the producer guard fires synchronously with
 * a clear {@code "row too large for server batch cap"} message citing
 * B's tight cap. This test asserts that precise message, so it is red
 * today and turns green when the fix lands.
 */
public class QwpSenderFailoverBatchSizeTest extends AbstractCairoTest {

    private static final Log LOG = LogFactory.getLog(QwpSenderFailoverBatchSizeTest.class);
    // 2 MB recv buffer  -> advertised cap = 2 * 1024 * 1024 - 14 = 2097138 bytes.
    private static final int RECV_BUFFER_LARGE_BYTES = 2 * 1024 * 1024;
    // 128 KB recv buffer -> advertised cap = 128 * 1024 - 14 = 131058 bytes.
    private static final int RECV_BUFFER_SMALL_BYTES = 128 * 1024;
    // 600 KB payload comfortably exceeds B's 131058 cap and fits under A's
    // 2097138 cap, with margin for per-column metadata.
    private static final int ROW_PAYLOAD_SIZE_BYTES = 600_000;
    // Server's send buffer; QwpSidecar does not override getSendBufferSize, so
    // the default from DefaultIODispatcherConfiguration applies.
    private static final int SEND_BUFFER_SIZE = 131_072;
    private int recvChunk;
    private int sendChunk;

    @Before
    public void setUpFragmentation() {
        Rnd rnd = TestUtils.generateRandom(LOG);
        // Chunk ranges are [1, bufferSize]. Use the smaller of the two
        // sidecars' recv buffers so the same chunk is legal for both.
        recvChunk = 1 + rnd.nextInt(RECV_BUFFER_SMALL_BYTES);
        sendChunk = 1 + rnd.nextInt(SEND_BUFFER_SIZE);
        LOG.info().$("QwpSenderFailoverBatchSizeTest fragmentation recvChunk=").$(recvChunk)
                .$(", sendChunk=").$(sendChunk).$();
    }

    @Test
    public void testReconnectToTighterCapRefreshesServerMaxBatchSize() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int portA = pickFreePort();
            int portB = pickFreePort();

            QwpSidecar serverA = new QwpSidecar(portA, RECV_BUFFER_LARGE_BYTES, recvChunk, sendChunk);
            QwpSidecar serverB = new QwpSidecar(portB, RECV_BUFFER_SMALL_BYTES, recvChunk, sendChunk);
            try {
                serverA.start();
                serverB.start();

                CapturingListener listener = new CapturingListener();
                String config = "ws::addr=127.0.0.1:" + portA + ",127.0.0.1:" + portB
                        + ";reconnect_max_duration_millis=15000"
                        + ";reconnect_initial_backoff_millis=50"
                        + ";reconnect_max_backoff_millis=200"
                        + ";auto_flush_rows=1;";

                try (Sender sender = Sender.builder(config)
                        .connectionListener(listener)
                        .build()) {
                    Assert.assertTrue("CONNECTED must fire on initial connect",
                            listener.awaitKind(SenderConnectionEvent.Kind.CONNECTED, 5_000));
                    SenderConnectionEvent connected = listener.firstOf(SenderConnectionEvent.Kind.CONNECTED);
                    Assert.assertNotNull(connected);
                    Assert.assertEquals("initial CONNECTED must land on server A (first endpoint)",
                            portA, connected.getPort());

                    // Confirm A's wire works end-to-end before we kill it.
                    sender.table("failover_cap_t")
                            .longColumn("v", 1L)
                            .at(1_000L, ChronoUnit.MICROS);
                    sender.flush();

                    // Stop A. The I/O thread surfaces the wire failure on its
                    // next send/poll; the reconnect loop then walks to B.
                    serverA.stop();

                    // Drive producer-side traffic until FAILED_OVER lands.
                    // Wire glitches during reconnect are expected and ignored.
                    long deadlineNanos = System.nanoTime() + 15_000L * 1_000_000L;
                    long row = 2;
                    while (listener.countOf() == 0
                            && System.nanoTime() < deadlineNanos) {
                        try {
                            sender.table("failover_cap_t")
                                    .longColumn("v", row)
                                    .at(1_000L * row, ChronoUnit.MICROS);
                        } catch (LineSenderException ignored) {
                            // mid-reconnect surface error; keep trying
                        }
                        row++;
                        Os.sleep(20);
                    }
                    Assert.assertTrue("FAILED_OVER must fire after server A is stopped",
                            listener.awaitKind(SenderConnectionEvent.Kind.FAILED_OVER, 1_000));
                    SenderConnectionEvent failedOver = listener.firstOf(SenderConnectionEvent.Kind.FAILED_OVER);
                    Assert.assertNotNull(failedOver);
                    Assert.assertEquals("FAILED_OVER must land on server B (the only remaining endpoint)",
                            portB, failedOver.getPort());

                    // Now push a single row whose raw column bytes sit between
                    // the two endpoint caps. The "row too large" guard in
                    // sendRow must fire IFF serverMaxBatchSize was refreshed
                    // to B's tight cap on the mid-stream reconnect.
                    char[] payloadChars = new char[ROW_PAYLOAD_SIZE_BYTES];
                    Arrays.fill(payloadChars, 'x');
                    String payload = new String(payloadChars);

                    LineSenderException thrown = null;
                    try {
                        sender.table("failover_cap_t")
                                .stringColumn("payload", payload)
                                .at(2_000_000L, ChronoUnit.MICROS);
                        sender.flush();
                    } catch (LineSenderException e) {
                        thrown = e;
                    }
                    Assert.assertNotNull(
                            "client must reject a row whose raw bytes exceed the new endpoint's serverMaxBatchSize",
                            thrown);
                    String msg = thrown.getMessage();
                    Assert.assertNotNull(msg);
                    Assert.assertTrue(
                            "expected the producer-side 'row too large for server batch cap' guard message"
                                    + " citing server B's tight cap, got: " + msg,
                            msg.contains("row too large for server batch cap"));
                }
                // The QWP processor on each sidecar acquired WAL writers to
                // ingest the warm-up / failover-driving rows. Without a WAL
                // apply job those segments stay un-drained and the pooled
                // writers keep file descriptors open. serverB's sidecar is
                // still running here (serverA was already stopped above), so
                // stop it first to halt its worker pool and return the WAL
                // writer to the pool; otherwise releaseInactive() races
                // serverB's worker thread and may skip a still-checked-out
                // writer, leaking fds. Then drain explicitly and release
                // inactive pool entries so assertMemoryLeak doesn't see them
                // as a leak.
                serverB.stop();
                drainWalQueue();
                engine.releaseInactive();
            } finally {
                serverA.stop();
                serverB.stop();
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
     * Minimal QWP-only {@link HttpServer} wrapper modeled on
     * {@link RestartableQwpServer}, but with a configurable HTTP recv buffer
     * size so the test can stand up two peers that advertise distinct
     * {@code X-QWP-Max-Batch-Size} caps. Shares the test's {@code engine}.
     */
    private static final class QwpSidecar {
        private final int forceRecvFragmentationChunkSize;
        private final int forceSendFragmentationChunkSize;
        private final int port;
        private final int recvBufferSize;
        private TestWorkerPool pool;
        private boolean running;
        private HttpServer server;

        QwpSidecar(int port, int recvBufferSize, int forceRecvFragmentationChunkSize, int forceSendFragmentationChunkSize) {
            this.port = port;
            this.recvBufferSize = recvBufferSize;
            this.forceRecvFragmentationChunkSize = forceRecvFragmentationChunkSize;
            this.forceSendFragmentationChunkSize = forceSendFragmentationChunkSize;
        }

        void start() throws SqlException {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(
                    configuration,
                    new DefaultHttpContextConfiguration() {
                        @Override
                        public int getForceRecvFragmentationChunkSize() {
                            return forceRecvFragmentationChunkSize;
                        }

                        @Override
                        public int getForceSendFragmentationChunkSize() {
                            return forceSendFragmentationChunkSize;
                        }
                    }
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
                public QwpIngressHttpProcessor newInstance() {
                    return new QwpIngressHttpProcessor(engine, httpConfig);
                }
            });
            // Intentionally skip setupWriterJobs: this test only exercises the
            // wire layer (handshake cap advertisement + client-side guard), and
            // two co-existing sidecars on the same engine would collide on the
            // singleton column-purge writer acquisition.
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

    /**
     * Thread-safe collector of {@link SenderConnectionEvent}. The QWP
     * dispatcher invokes the listener on its dedicated thread, so the
     * collection must be concurrency-safe; events are sparse enough that
     * {@link CopyOnWriteArrayList} is the right shape.
     */
    private static final class CapturingListener implements SenderConnectionListener {
        private final List<SenderConnectionEvent> events = new CopyOnWriteArrayList<>();
        private final Set<SenderConnectionEvent.Kind> seen = EnumSet.noneOf(SenderConnectionEvent.Kind.class);
        private final Object seenLock = new Object();

        @Override
        public void onEvent(@NotNull SenderConnectionEvent event) {
            events.add(event);
            synchronized (seenLock) {
                seen.add(event.getKind());
                seenLock.notifyAll();
            }
        }

        boolean awaitKind(SenderConnectionEvent.Kind kind, long timeoutMillis) throws InterruptedException {
            long deadline = System.nanoTime() + timeoutMillis * 1_000_000L;
            synchronized (seenLock) {
                while (!seen.contains(kind)) {
                    long remainingMs = (deadline - System.nanoTime()) / 1_000_000L;
                    if (remainingMs <= 0) {
                        return false;
                    }
                    seenLock.wait(remainingMs);
                }
                return true;
            }
        }

        int countOf() {
            int n = 0;
            for (SenderConnectionEvent e : events) {
                if (e.getKind() == SenderConnectionEvent.Kind.FAILED_OVER) {
                    n++;
                }
            }
            return n;
        }

        SenderConnectionEvent firstOf(SenderConnectionEvent.Kind kind) {
            for (SenderConnectionEvent e : events) {
                if (e.getKind() == kind) {
                    return e;
                }
            }
            return null;
        }
    }
}
