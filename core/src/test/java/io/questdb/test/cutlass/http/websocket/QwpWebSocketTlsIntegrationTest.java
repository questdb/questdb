/*******************************************************************************
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

package io.questdb.test.cutlass.http.websocket;

import io.questdb.cutlass.qwp.websocket.WebSocketCloseCode;
import io.questdb.std.Os;
import org.junit.Assert;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * TLS integration tests for ILP v4 WebSocket server functionality.
 * These tests verify WebSocket upgrade and message flow over TLS
 * using JDK's built-in WebSocket client.
 */
public class QwpWebSocketTlsIntegrationTest extends AbstractWebSocketTest {

    private static final int TLS_TEST_PORT = 19300 + (int) (Os.currentTimeMicros() % 100);

    // ==================== TLS CONNECTION TESTS ====================

    @Test
    public void testTlsWebSocketUpgradeSuccess() throws Exception {
        AtomicBoolean connected = new AtomicBoolean(false);
        CountDownLatch serverConnectLatch = new CountDownLatch(1);

        try (TestWebSocketServer server = new TestWebSocketServer(TLS_TEST_PORT, new TestWebSocketServer.WebSocketServerHandler() {
            @Override
            public void onConnect(TestWebSocketServer.ClientHandler client) {
                connected.set(true);
                serverConnectLatch.countDown();
            }
        }, true)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            Assert.assertTrue("Server should have TLS enabled", server.isTlsEnabled());

            CountDownLatch clientConnectLatch = new CountDownLatch(1);
            WebSocket ws = connectTlsClient(TLS_TEST_PORT, clientConnectLatch);

            Assert.assertTrue("TLS connection should succeed", clientConnectLatch.await(5, TimeUnit.SECONDS));
            Assert.assertTrue("Server should receive connection", serverConnectLatch.await(5, TimeUnit.SECONDS));
            Assert.assertTrue("Server connected flag should be set", connected.get());

            ws.sendClose(WebSocket.NORMAL_CLOSURE, "done").join();
        }
    }

    @Test
    public void testTlsBinaryMessageEcho() throws Exception {
        AtomicReference<byte[]> receivedMessage = new AtomicReference<>();
        CountDownLatch messageLatch = new CountDownLatch(1);

        try (TestWebSocketServer server = new TestWebSocketServer(TLS_TEST_PORT + 1, new TestWebSocketServer.WebSocketServerHandler() {
            @Override
            public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
                receivedMessage.set(data);
                // Echo back the message
                try {
                    client.sendBinary(data);
                } catch (java.io.IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }, true)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            AtomicReference<byte[]> echoedMessage = new AtomicReference<>();
            CountDownLatch connectLatch = new CountDownLatch(1);

            HttpClient httpClient = createTrustAllHttpClient();

            WebSocket.Listener listener = new WebSocket.Listener() {
                private ByteBuffer binaryBuffer;

                @Override
                public void onOpen(WebSocket webSocket) {
                    connectLatch.countDown();
                    webSocket.request(1);
                }

                @Override
                public CompletionStage<?> onBinary(WebSocket webSocket, ByteBuffer data, boolean last) {
                    if (binaryBuffer == null) {
                        binaryBuffer = ByteBuffer.allocate(data.remaining());
                    }
                    if (binaryBuffer.remaining() < data.remaining()) {
                        ByteBuffer newBuffer = ByteBuffer.allocate(binaryBuffer.position() + data.remaining());
                        binaryBuffer.flip();
                        newBuffer.put(binaryBuffer);
                        binaryBuffer = newBuffer;
                    }
                    binaryBuffer.put(data);

                    if (last) {
                        binaryBuffer.flip();
                        byte[] bytes = new byte[binaryBuffer.remaining()];
                        binaryBuffer.get(bytes);
                        echoedMessage.set(bytes);
                        messageLatch.countDown();
                        binaryBuffer = null;
                    }
                    webSocket.request(1);
                    return null;
                }

                @Override
                public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
                    webSocket.request(1);
                    return null;
                }

                @Override
                public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
                    return null;
                }

                @Override
                public void onError(WebSocket webSocket, Throwable error) {
                }
            };

            WebSocket ws = httpClient.newWebSocketBuilder()
                    .buildAsync(URI.create("wss://localhost:" + (TLS_TEST_PORT + 1)), listener)
                    .join();
            Assert.assertTrue("TLS connection should succeed", connectLatch.await(5, TimeUnit.SECONDS));

            // Send binary message
            byte[] testData = "cpu,host=server01 usage=95.5 1234567890\n".getBytes(StandardCharsets.UTF_8);
            ws.sendBinary(ByteBuffer.wrap(testData), true).join();

            Assert.assertTrue("Should receive echo", messageLatch.await(5, TimeUnit.SECONDS));
            Assert.assertArrayEquals("Echoed message should match", testData, echoedMessage.get());
            Assert.assertArrayEquals("Server should receive message", testData, receivedMessage.get());

            ws.sendClose(WebSocket.NORMAL_CLOSURE, "done");
        }
    }

    @Test
    public void testTlsMultipleBinaryMessages() throws Exception {
        int numMessages = 50;
        CountDownLatch messageLatch = new CountDownLatch(numMessages);
        AtomicInteger messageCount = new AtomicInteger(0);

        try (TestWebSocketServer server = new TestWebSocketServer(TLS_TEST_PORT + 2, new TestWebSocketServer.WebSocketServerHandler() {
            @Override
            public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
                messageCount.incrementAndGet();
                messageLatch.countDown();
            }
        }, true)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            CountDownLatch connectLatch = new CountDownLatch(1);
            WebSocket ws = connectTlsClient(TLS_TEST_PORT + 2, connectLatch);
            Assert.assertTrue("TLS connection should succeed", connectLatch.await(5, TimeUnit.SECONDS));

            // Send multiple messages
            for (int i = 0; i < numMessages; i++) {
                byte[] msg = ("message-" + i + "\n").getBytes(StandardCharsets.UTF_8);
                ws.sendBinary(ByteBuffer.wrap(msg), true).join();
            }

            Assert.assertTrue("Should receive all messages over TLS", messageLatch.await(10, TimeUnit.SECONDS));
            Assert.assertEquals("All messages should be received", numMessages, messageCount.get());

            ws.sendClose(WebSocket.NORMAL_CLOSURE, "done");
        }
    }

    @Test
    public void testTlsLargeBinaryMessage() throws Exception {
        AtomicReference<byte[]> receivedMessage = new AtomicReference<>();
        CountDownLatch messageLatch = new CountDownLatch(1);

        try (TestWebSocketServer server = new TestWebSocketServer(TLS_TEST_PORT + 3, new TestWebSocketServer.WebSocketServerHandler() {
            @Override
            public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
                receivedMessage.set(data);
                messageLatch.countDown();
            }
        }, true)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            CountDownLatch connectLatch = new CountDownLatch(1);
            WebSocket ws = connectTlsClient(TLS_TEST_PORT + 3, connectLatch);
            Assert.assertTrue("TLS connection should succeed", connectLatch.await(5, TimeUnit.SECONDS));

            // Send large binary message (32KB)
            byte[] testData = new byte[32768];
            for (int i = 0; i < testData.length; i++) {
                testData[i] = (byte) (i % 256);
            }
            ws.sendBinary(ByteBuffer.wrap(testData), true).join();

            Assert.assertTrue("Should receive large message over TLS", messageLatch.await(10, TimeUnit.SECONDS));
            Assert.assertArrayEquals("Large message should be received correctly", testData, receivedMessage.get());

            ws.sendClose(WebSocket.NORMAL_CLOSURE, "done");
        }
    }

    @Test
    public void testTlsClientInitiatedClose() throws Exception {
        AtomicBoolean closed = new AtomicBoolean(false);
        AtomicInteger closeCode = new AtomicInteger(-1);
        CountDownLatch closeLatch = new CountDownLatch(1);

        try (TestWebSocketServer server = new TestWebSocketServer(TLS_TEST_PORT + 4, new TestWebSocketServer.WebSocketServerHandler() {
            @Override
            public void onClose(TestWebSocketServer.ClientHandler client, int code, String reason) {
                closed.set(true);
                closeCode.set(code);
                closeLatch.countDown();
            }
        }, true)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            CountDownLatch connectLatch = new CountDownLatch(1);
            WebSocket ws = connectTlsClient(TLS_TEST_PORT + 4, connectLatch);
            Assert.assertTrue("TLS connection should succeed", connectLatch.await(5, TimeUnit.SECONDS));

            // Close with normal code
            ws.sendClose(WebSocketCloseCode.NORMAL_CLOSURE, "Test close").join();

            Assert.assertTrue("Server should receive close over TLS", closeLatch.await(5, TimeUnit.SECONDS));
            Assert.assertTrue("Close should be recorded", closed.get());
        }
    }

    @Test
    public void testTlsMultipleClientConnections() throws Exception {
        AtomicInteger connectionCount = new AtomicInteger(0);

        try (TestWebSocketServer server = new TestWebSocketServer(TLS_TEST_PORT + 5, new TestWebSocketServer.WebSocketServerHandler() {
            @Override
            public void onConnect(TestWebSocketServer.ClientHandler client) {
                connectionCount.incrementAndGet();
            }
        }, true)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            int numClients = 3;
            CountDownLatch[] latches = new CountDownLatch[numClients];
            WebSocket[] clients = new WebSocket[numClients];

            for (int i = 0; i < numClients; i++) {
                latches[i] = new CountDownLatch(1);
                clients[i] = connectTlsClient(TLS_TEST_PORT + 5, latches[i]);
            }

            // Wait for all connections
            for (int i = 0; i < numClients; i++) {
                Assert.assertTrue("TLS client " + i + " should connect", latches[i].await(5, TimeUnit.SECONDS));
            }

            Assert.assertEquals("All TLS clients should connect", numClients, connectionCount.get());

            // Close all clients
            for (WebSocket client : clients) {
                client.sendClose(WebSocket.NORMAL_CLOSURE, "done");
            }
        }
    }

    // ==================== HELPER METHODS ====================

    private WebSocket connectTlsClient(int port, CountDownLatch latch) throws Exception {
        HttpClient httpClient = createTrustAllHttpClient();

        WebSocket.Listener listener = new WebSocket.Listener() {
            @Override
            public void onOpen(WebSocket webSocket) {
                latch.countDown();
                webSocket.request(1);
            }

            @Override
            public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
                webSocket.request(1);
                return null;
            }

            @Override
            public CompletionStage<?> onBinary(WebSocket webSocket, ByteBuffer data, boolean last) {
                webSocket.request(1);
                return null;
            }

            @Override
            public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
                return null;
            }

            @Override
            public void onError(WebSocket webSocket, Throwable error) {
                latch.countDown();
            }
        };

        return httpClient.newWebSocketBuilder()
                .buildAsync(URI.create("wss://localhost:" + port), listener)
                .join();
    }

    private HttpClient createTrustAllHttpClient() throws Exception {
        SSLContext sslContext = createTrustAllContext();
        return HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .sslContext(sslContext)
                .build();
    }

    private SSLContext createTrustAllContext() throws Exception {
        TrustManager[] trustAllCerts = new TrustManager[]{
                new X509TrustManager() {
                    public X509Certificate[] getAcceptedIssuers() {
                        return new X509Certificate[0];
                    }

                    public void checkClientTrusted(X509Certificate[] certs, String authType) {}

                    public void checkServerTrusted(X509Certificate[] certs, String authType) {}
                }
        };

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
        return sslContext;
    }
}
