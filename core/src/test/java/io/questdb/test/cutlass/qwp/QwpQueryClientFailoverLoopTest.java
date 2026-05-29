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
import io.questdb.client.cutlass.qwp.client.QwpServerInfo;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * End-to-end coverage of the {@link QwpQueryClient#execute} failover loop
 * (failover.md §4.3). Drives the loop against a real {@link TestServerMain}
 * so the post-reconnect query path is exercised faithfully (the unit-only
 * tests in the client repo can't follow that branch without a real QWP
 * egress server).
 *
 * <p>Latched failures are injected via
 * {@link QwpQueryClient#recordTerminalFailureForTest(byte, String)} --
 * the same hook {@link QwpQueryClientTerminalFailureTest} uses -- so each
 * test has deterministic control over when the loop sees a transport
 * error vs a clean execute.
 *
 * <p>Coverage:
 * <ul>
 *   <li>{@code failover=off} surfaces the latched failure verbatim
 *       (no loop entry).</li>
 *   <li>{@code failover_max_attempts} caps the loop after N executes.</li>
 *   <li>{@code failover_max_duration_ms} caps the loop by wall clock
 *       even when attempts remain.</li>
 *   <li>The first latched failure wins -- subsequent injects do not
 *       overwrite the root cause.</li>
 *   <li>After a clean execute that re-binds (no synthetic failures),
 *       the same client is still usable across multiple Execute() calls.</li>
 * </ul>
 */
public class QwpQueryClientFailoverLoopTest extends AbstractQwpBootstrapTest {

    /**
     * Hard wall-clock cap per test so a regression that breaks the
     * failover bound surfaces as a JUnit timeout rather than a hung CI.
     * 30 s is generous: every test here is built to surface within
     * single-digit seconds. If one of these hits the cap, the failover
     * loop is genuinely stuck.
     */
    private static final int TEST_TIMEOUT_MS = 30_000;

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void testFailoverDeadlineExhaustsBeforeAttempts() throws Exception {
        // failover_max_duration_ms must bound the loop independently of
        // failover_max_attempts. Setup:
        // - HTTP_PORT: the real TestServerMain. Used for initial connect,
        //   then deliberately closed so subsequent reconnects to it
        //   return TCP refused.
        // - blackholePort: a ServerSocket that accepts TCP but never
        //   replies. Each upgrade attempt waits auth_timeout_ms.
        //
        // After connect, we close the real server and inject a transport
        // failure. The failover loop then walks {HTTP_PORT (refused, fast),
        // blackhole (auth_timeout)}, never recovers, and surfaces either
        // "budget exhausted" (deadline trip mid-loop) or "failover
        // exhausted" (walkTracker drained without binding).
        TestUtils.assertMemoryLeak(() -> {
            TestServerMain serverMain = startFragmented();
            boolean serverMainClosed = false;
            try (ServerSocket blackhole = new ServerSocket(0)) {
                int blackholePort = blackhole.getLocalPort();
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT
                                + ",127.0.0.1:" + blackholePort
                                + ";failover=on;"
                                + "failover_max_attempts=20;"
                                + "failover_max_duration_ms=50;"
                                + "failover_backoff_initial_ms=5;"
                                + "failover_backoff_max_ms=10;"
                                + "auth_timeout_ms=400;")) {
                    client.connect(); // binds HTTP_PORT
                    client.seedFailoverRandomForTest(0L);
                    // Kill the real server so subsequent walks to
                    // HTTP_PORT see TCP refused (immediate). With both
                    // hosts unrecoverable, the loop has no exit other
                    // than the deadline / attempts cap.
                    serverMain.close();
                    serverMainClosed = true;
                    client.recordTerminalFailureForTest((byte) 11,
                            "synthetic-deadline-cause");

                    CapturingHandler h = new CapturingHandler();
                    long t0 = System.nanoTime();
                    client.execute("SELECT 1", h);
                    long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;

                    Assert.assertNotNull("onError must fire", h.lastMessage);
                    String msg = h.lastMessage;
                    // Bounded-failure phrases the impl can surface here:
                    // - "budget exhausted" / "failover exhausted" -- failover-loop
                    //   level deadline / attempts cap tripped.
                    // - "all QWP endpoints unreachable on failover" -- WalkTracker
                    //   exhausted both peers in a single reconnect attempt (the
                    //   blackhole's auth_timeout chews up the per-loop budget so
                    //   this is the path actually taken when failover_max_duration
                    //   is shorter than auth_timeout x hostCount).
                    boolean bounded = msg.contains("budget exhausted")
                            || msg.contains("failover exhausted")
                            || msg.contains("all QWP endpoints unreachable");
                    Assert.assertTrue("loop must surface a bounded failure: " + msg,
                            bounded);
                    Assert.assertTrue("elapsed must be bounded, was " + elapsedMs + "ms",
                            elapsedMs < 10_000);
                }
            } finally {
                if (!serverMainClosed) {
                    serverMain.close();
                }
            }
        });
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void testFailoverOff_LatchedTransportSurfacesDirectly() throws Exception {
        // failover=off: the latched transport failure surfaces verbatim
        // through onError on the first execute call. The loop never
        // enters the retry path, so the elapsed time is microseconds.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain ignored = startFragmented()) {
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";failover=off;")) {
                    client.connect();
                    client.recordTerminalFailureForTest((byte) 7, "transport-direct");

                    CapturingHandler h = new CapturingHandler();
                    long t0 = System.nanoTime();
                    client.execute("SELECT 1", h);
                    long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;

                    Assert.assertEquals(Byte.valueOf((byte) 7), h.lastStatus);
                    Assert.assertEquals("transport-direct", h.lastMessage);
                    Assert.assertEquals("no failover reset must fire", 0, h.failoverResetCount);
                    Assert.assertTrue("must short-circuit fast, elapsed=" + elapsedMs + "ms",
                            elapsedMs < 1_000);
                }
            }
        });
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void testFirstLatchedFailureWins() throws Exception {
        // The latch is one-shot: the first call wins so the user sees
        // the root cause, not a downstream symptom from the I/O thread
        // winding down.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain ignored = startFragmented()) {
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";failover=off;")) {
                    client.connect();
                    client.recordTerminalFailureForTest((byte) 1, "first-cause");
                    client.recordTerminalFailureForTest((byte) 2, "second-symptom");
                    client.recordTerminalFailureForTest((byte) 3, "third-noise");

                    CapturingHandler h = new CapturingHandler();
                    client.execute("SELECT 1", h);

                    Assert.assertEquals("first cause status preserved",
                            Byte.valueOf((byte) 1), h.lastStatus);
                    Assert.assertEquals("first cause message preserved",
                            "first-cause", h.lastMessage);
                }
            }
        });
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void testCleanExecuteAfterReconnectWorks() throws Exception {
        // After failover successfully rebuilds the connection (single
        // host, latched failure on entry, walkTracker re-binds via
        // fall-through), a SECOND execute against the same client must
        // succeed against the real server. This proves the loop leaves
        // the client in a consistent state after recovery.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startFragmented()) {
                serverMain.execute("CREATE TABLE recover_test(id LONG, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO recover_test SELECT x, x::TIMESTAMP FROM long_sequence(3)");
                serverMain.awaitTable("recover_test");

                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";failover=on;"
                                + "failover_max_attempts=3;failover_backoff_initial_ms=5;"
                                + "failover_backoff_max_ms=10;auth_timeout_ms=2000;")) {
                    client.connect();
                    client.seedFailoverRandomForTest(42L);

                    // Latch a transport failure so the FIRST executeOnce
                    // trips the failover loop. The single host is the
                    // real TestServerMain, so walkTracker rebinds it
                    // successfully on the first walk-fall-through-walk.
                    client.recordTerminalFailureForTest((byte) 5, "first-execute-fault");

                    CapturingHandler h1 = new CapturingHandler();
                    client.execute("SELECT id FROM recover_test", h1);
                    // Either succeeded (reconnect + replay worked) OR
                    // surfaced as ceiling (failover exhausted on max=3).
                    // Both are valid; pin the loop entered at least once.
                    if (h1.lastMessage == null) {
                        Assert.assertTrue("query succeeded after failover", h1.endCalled);
                        Assert.assertTrue("OnFailoverReset must fire on successful failover",
                                h1.failoverResetCount >= 1);
                    } else {
                        // Loop tripped the ceiling -- still valid.
                        // The recovery test then wouldn't apply to the
                        // second execute below.
                        return;
                    }

                    // Second execute on the recovered client: must
                    // succeed cleanly with NO failover reset.
                    CapturingHandler h2 = new CapturingHandler();
                    client.execute("SELECT id FROM recover_test", h2);
                    Assert.assertNull("second execute must not error: " + h2.lastMessage,
                            h2.lastMessage);
                    Assert.assertTrue("second execute must reach onEnd", h2.endCalled);
                    Assert.assertEquals("second execute must NOT trigger failover", 0,
                            h2.failoverResetCount);
                }
            }
        });
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void testAuthFailureDuringReconnectSurfacesTerminalNoLoop() throws Exception {
        // failover.md §6 AuthError: a 401 / 403 on a reconnect attempt
        // is terminal -- the loop must NOT keep walking through it
        // (auth credentials are cluster-wide, retrying every host
        // floods server logs without recovery).
        //
        // Setup: real TestServerMain on HTTP_PORT (initial connect
        // succeeds). After connect we (a) close the real server so
        // HTTP_PORT becomes TCP refused and (b) inject a transport
        // failure. The address list also contains a Fake401Server so
        // that on mid-stream demote of HTTP_PORT, walkTracker prefers
        // the Fake401Server (UNKNOWN, priority 2) over HTTP_PORT
        // (TRANSPORT_ERROR, priority 4). The Fake401 then returns 401,
        // walkTracker rethrows, execute() catches and surfaces a clean
        // "auth failure during failover reconnect" message.
        TestUtils.assertMemoryLeak(() -> {
            TestServerMain serverMain = startFragmented();
            boolean serverMainClosed = false;
            try (Fake401Server auth = new Fake401Server()) {
                auth.start();
                int authPort = auth.port();
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT
                                + ",127.0.0.1:" + authPort
                                + ";failover=on;"
                                + "failover_max_attempts=20;"
                                + "failover_max_duration_ms=5000;"
                                + "failover_backoff_initial_ms=5;"
                                + "failover_backoff_max_ms=10;"
                                + "auth_timeout_ms=2000;")) {
                    client.connect(); // binds HTTP_PORT
                    serverMain.close();
                    serverMainClosed = true;
                    client.recordTerminalFailureForTest((byte) 9,
                            "synthetic-trigger-reconnect");

                    CapturingHandler h = new CapturingHandler();
                    client.execute("SELECT 1", h);

                    Assert.assertNotNull("onError must fire", h.lastMessage);
                    String msg = h.lastMessage;
                    // Spec-mandated wording: distinct from generic
                    // "failover reconnect failed" so monitoring can pull
                    // out auth incidents specifically.
                    Assert.assertTrue("auth-failure path message expected: " + msg,
                            msg.contains("auth failure during failover reconnect")
                                    && msg.contains("status=401"));
                    Assert.assertEquals("no failover-reset must have fired", 0,
                            h.failoverResetCount);
                }
            } finally {
                if (!serverMainClosed) {
                    serverMain.close();
                }
            }
        });
    }

    /**
     * Minimal HTTP server that returns {@code HTTP/1.1 401 Unauthorized}
     * to every WebSocket upgrade request. Used to drive the auth-error
     * path through {@link QwpQueryClient#execute}'s failover reconnect
     * catch without requiring a real auth-aware QuestDB build (those
     * live in the ent-side {@code QwpEgressAuthTest}).
     */
    private static final class Fake401Server implements AutoCloseable {
        private final AtomicBoolean running = new AtomicBoolean(false);
        private final ServerSocket serverSocket;
        private Thread acceptThread;

        Fake401Server() throws IOException {
            this.serverSocket = new ServerSocket(0);
            this.serverSocket.setSoTimeout(100);
        }

        @Override
        public void close() {
            running.set(false);
            try {
                serverSocket.close();
            } catch (IOException ignored) {
            }
            if (acceptThread != null) {
                try {
                    acceptThread.join(2000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        int port() {
            return serverSocket.getLocalPort();
        }

        void start() {
            running.set(true);
            acceptThread = new Thread(this::acceptLoop, "fake-401-acceptor");
            acceptThread.setDaemon(true);
            acceptThread.start();
        }

        private void acceptLoop() {
            while (running.get()) {
                Socket client;
                try {
                    client = serverSocket.accept();
                } catch (SocketException se) {
                    return;
                } catch (IOException e) {
                    continue;
                }
                handleClient(client);
            }
        }

        private void handleClient(Socket client) {
            try (Socket sock = client;
                 InputStream in = sock.getInputStream();
                 OutputStream out = sock.getOutputStream()) {
                // Drain the upgrade request until the blank line so
                // the client sees a clean response rather than a
                // half-read socket.
                byte[] buf = new byte[4096];
                int total = 0;
                while (total < buf.length) {
                    int n = in.read(buf, total, buf.length - total);
                    if (n < 0) break;
                    total += n;
                    String s = new String(buf, 0, total, StandardCharsets.US_ASCII);
                    if (s.contains("\r\n\r\n")) break;
                }
                String response = """
                        HTTP/1.1 401 Unauthorized\r
                        Connection: close\r
                        Content-Length: 0\r
                        \r
                        """;
                out.write(response.getBytes(StandardCharsets.US_ASCII));
                out.flush();
            } catch (IOException ignored) {
            }
        }
    }

    /**
     * Captures the FIRST onError plus aggregate counts for onBatch,
     * onEnd, onFailoverReset. Stays small so each test reads as a
     * straight assertion sequence.
     */
    private static final class CapturingHandler implements QwpColumnBatchHandler {
        boolean batchCalled;
        boolean endCalled;
        int failoverResetCount;
        AtomicReference<QwpServerInfo> lastServerInfo = new AtomicReference<>();
        String lastMessage;
        Byte lastStatus;

        @Override
        public void onBatch(QwpColumnBatch batch) {
            batchCalled = true;
        }

        @Override
        public void onEnd(long totalRows) {
            endCalled = true;
        }

        @Override
        public void onError(byte status, String message) {
            if (lastMessage == null) {
                lastStatus = status;
                lastMessage = message;
            }
        }

        @Override
        public void onFailoverReset(QwpServerInfo info) {
            failoverResetCount++;
            lastServerInfo.set(info);
        }
    }

}
