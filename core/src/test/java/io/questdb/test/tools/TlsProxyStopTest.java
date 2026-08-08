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

package io.questdb.test.tools;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TlsProxyStopTest {
    private static final int ITERATIONS = 20;
    private static final String KEYSTORE = "/keystore/server.keystore";
    private static final char[] KEYSTORE_PASSWORD = "questdb".toCharArray();
    private static final long STOP_TIMEOUT_MS = 30_000;

    /**
     * {@link TlsProxy#stop()} used to join the acceptor thread while holding the proxy monitor.
     * The acceptor takes that same monitor right after it dialled the backend, and
     * {@link Thread#interrupt()} cannot break monitor entry, so a connection sitting in that
     * window wedged {@code stop()} forever. Each iteration runs a connector thread that dials the
     * proxy port in a loop -- measured behaviour is one successful dial per iteration -- then
     * calls {@code stop()} on a third thread and asserts that thread finishes.
     * <p>
     * {@code SSLServerSocket.accept()} returns before any TLS handshake runs, so a plain TCP
     * connect is enough to drive the acceptor into the window -- the client never has to speak
     * TLS.
     * <p>
     * A latch, not a sleep, coordinates the two: {@code stop()} runs only after the connector's
     * {@code connect()} has returned. That return means the TCP handshake completed and the
     * connection sits in the proxy's listen backlog; it does NOT mean the acceptor has already
     * accepted it and dialled the backend. The scheduler decides who wins that race, which is
     * why the backend-dial check below aggregates over all iterations.
     * <p>
     * The 30s budget is deliberately generous and must not be "tightened": the regression is an
     * UNBOUNDED hang, not a slow path, so any budget separates broken from fixed. A tight one
     * only buys false failures on a loaded machine.
     */
    @Test
    public void testStopReturnsWhileConnectionsArrive() throws Exception {
        try (ServerSocket backend = new ServerSocket(0, 50, InetAddress.getLoopbackAddress())) {
            AtomicBoolean isBackendRunning = new AtomicBoolean(true);
            AtomicInteger backendDials = new AtomicInteger();
            Thread backendAcceptor = new Thread(() -> {
                while (isBackendRunning.get()) {
                    try {
                        backend.accept().close();
                        backendDials.incrementAndGet();
                    } catch (IOException e) {
                        return;
                    }
                }
            }, "tls-proxy-test-backend");
            backendAcceptor.setDaemon(true);
            backendAcceptor.start();

            try {
                for (int i = 0; i < ITERATIONS; i++) {
                    TlsProxy proxy = new TlsProxy("127.0.0.1", backend.getLocalPort(), KEYSTORE, KEYSTORE_PASSWORD);
                    int port = proxy.start();

                    AtomicBoolean isConnecting = new AtomicBoolean(true);
                    CountDownLatch firstConnectLatch = new CountDownLatch(1);
                    Thread connector = new Thread(() -> {
                        while (isConnecting.get()) {
                            try (Socket socket = new Socket()) {
                                socket.connect(new InetSocketAddress("127.0.0.1", port), 200);
                                firstConnectLatch.countDown();
                            } catch (IOException e) {
                                // the proxy is gone, nothing left to dial
                                firstConnectLatch.countDown();
                                return;
                            }
                        }
                    }, "tls-proxy-test-connector");
                    connector.setDaemon(true);
                    connector.start();
                    firstConnectLatch.await();

                    Thread stopper = new Thread(proxy::stop, "tls-proxy-test-stopper");
                    stopper.setDaemon(true);
                    stopper.start();
                    stopper.join(STOP_TIMEOUT_MS);
                    boolean isStopperAlive = stopper.isAlive();

                    isConnecting.set(false);
                    connector.join(STOP_TIMEOUT_MS);
                    Assert.assertFalse("TlsProxy.stop() did not return within " + STOP_TIMEOUT_MS
                            + "ms at iteration " + i, isStopperAlive);
                }
            } finally {
                isBackendRunning.set(false);
                backend.close();
                backendAcceptor.join(STOP_TIMEOUT_MS);
            }
            // This counts backend dials across ALL iterations and must never become a
            // per-iteration assertion: the acceptor reaches the backend in only some iterations
            // (measured 12-14 of 20), so a per-iteration check would be flaky by construction.
            Assert.assertTrue("the acceptor never dialled the backend across " + ITERATIONS
                    + " iterations, so the test no longer exercises the monitor window it targets",
                    backendDials.get() > 0);
        }
    }
}
