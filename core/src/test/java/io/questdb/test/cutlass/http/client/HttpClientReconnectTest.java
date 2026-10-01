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

package io.questdb.test.cutlass.http.client;

import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.cutlass.http.client.Response;
import io.questdb.std.Os;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * End-to-end coverage of {@link HttpClient}'s keep-alive connection management,
 * which rides on {@code NetworkFacade.testConnection} before each reuse. The raw
 * {@link ServerSocket} fixture controls the wire exactly, so the two sides of the
 * probe's contract are pinned:
 * <ul>
 *     <li>a server FIN hiding behind an unread buffered byte must force a transparent
 *         reconnect (the shape a {@code recv(MSG_PEEK)} probe cannot see);</li>
 *     <li>an idle, live connection must be reused -- a probe that misreads liveness
 *         would silently double connection churn.</li>
 * </ul>
 */
public class HttpClientReconnectTest {

    @Test
    public void testFinBehindBufferedDataForcesReconnect() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (ServerSocket server = new ServerSocket(0)) {
                final int port = server.getLocalPort();
                final AtomicInteger connections = new AtomicInteger();
                final AtomicReference<Throwable> serverFailure = new AtomicReference<>();
                final CountDownLatch firstResponseConsumed = new CountDownLatch(1);
                final CountDownLatch finSent = new CountDownLatch(1);

                Thread serverThread = new Thread(() -> {
                    try {
                        try (Socket first = server.accept()) {
                            connections.incrementAndGet();
                            readRequest(first.getInputStream());
                            writeResponse(first.getOutputStream());
                            Assert.assertTrue(
                                    "client never consumed the first response",
                                    firstResponseConsumed.await(10, TimeUnit.SECONDS)
                            );
                            // A stray unread byte, then FIN on close: the exact shape that
                            // masks the hangup from a peek-based probe.
                            OutputStream out = first.getOutputStream();
                            out.write('X');
                            out.flush();
                        }
                        finSent.countDown();
                        try (Socket second = server.accept()) {
                            connections.incrementAndGet();
                            readRequest(second.getInputStream());
                            writeResponse(second.getOutputStream());
                        }
                    } catch (Throwable t) {
                        serverFailure.set(t);
                        finSent.countDown();
                    }
                }, "http-client-reconnect-server");
                serverThread.setDaemon(true);
                serverThread.start();

                try (HttpClient client = HttpClientFactory.newPlainTextInstance()) {
                    exchange(client, port);
                    firstResponseConsumed.countDown();
                    Assert.assertTrue(
                            "server never sent the trailing byte and FIN",
                            finSent.await(10, TimeUnit.SECONDS)
                    );
                    // Let the stray byte and the FIN land in the client's receive buffer
                    // before the reuse probe runs.
                    Os.sleep(200);
                    exchange(client, port);
                }

                serverThread.join(10_000);
                if (serverFailure.get() != null) {
                    throw new AssertionError("server fixture failed", serverFailure.get());
                }
                Assert.assertFalse("server fixture thread did not terminate", serverThread.isAlive());
                Assert.assertEquals(
                        "client must detect the FIN behind the buffered byte and reconnect",
                        2, connections.get()
                );
            }
        });
    }

    @Test
    public void testLiveConnectionIsReused() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (ServerSocket server = new ServerSocket(0)) {
                final int port = server.getLocalPort();
                final AtomicInteger connections = new AtomicInteger();
                final AtomicReference<Throwable> serverFailure = new AtomicReference<>();

                Thread serverThread = new Thread(() -> {
                    try (Socket conn = server.accept()) {
                        connections.incrementAndGet();
                        readRequest(conn.getInputStream());
                        writeResponse(conn.getOutputStream());
                        readRequest(conn.getInputStream());
                        writeResponse(conn.getOutputStream());
                    } catch (Throwable t) {
                        serverFailure.set(t);
                    }
                }, "http-client-reuse-server");
                serverThread.setDaemon(true);
                serverThread.start();

                try (HttpClient client = HttpClientFactory.newPlainTextInstance()) {
                    exchange(client, port);
                    exchange(client, port);
                }

                serverThread.join(10_000);
                if (serverFailure.get() != null) {
                    throw new AssertionError("server fixture failed", serverFailure.get());
                }
                Assert.assertFalse("server fixture thread did not terminate", serverThread.isAlive());
                Assert.assertEquals(
                        "client must reuse the live keep-alive connection",
                        1, connections.get()
                );
            }
        });
    }

    private static void exchange(HttpClient client, int port) {
        HttpClient.Request request = client.newRequest("localhost", port);
        request.GET().url("/ping");
        HttpClient.ResponseHeaders responseHeaders = request.send();
        responseHeaders.await();
        Response response = responseHeaders.getResponse();
        //noinspection StatementWithEmptyBody
        while (response.recv() != null) {
        }
    }

    private static void readRequest(InputStream in) throws Exception {
        int matched = 0;
        while (matched < 4) {
            int b = in.read();
            Assert.assertNotEquals("unexpected end of stream while reading request", -1, b);
            if ((matched % 2 == 0 && b == '\r') || (matched % 2 == 1 && b == '\n')) {
                matched++;
            } else {
                matched = b == '\r' ? 1 : 0;
            }
        }
    }

    private static void writeResponse(OutputStream out) throws Exception {
        out.write("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok".getBytes(StandardCharsets.US_ASCII));
        out.flush();
    }
}
