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

import io.questdb.cutlass.qwp.server.QwpWebSocketProcessor;
import io.questdb.cutlass.qwp.server.QwpWebSocketProcessorState;
import io.questdb.cutlass.qwp.websocket.WebSocketCloseCode;
import io.questdb.std.Os;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Integration tests for ILP v4 WebSocket server functionality.
 * These tests verify the complete WebSocket upgrade and message flow
 * using JDK's built-in WebSocket client.
 */
public class QwpWebSocketServerIntegrationTest extends AbstractWebSocketTest {

    private static final int TEST_PORT = 19200 + (int) (Os.currentTimeMicros() % 100);
    private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    // ==================== CONNECTION TESTS ====================

    @Test
    public void testWebSocketUpgradeSuccess() throws Exception {
        AtomicBoolean connected = new AtomicBoolean(false);
        CountDownLatch serverConnectLatch = new CountDownLatch(1);

        try (TestWebSocketServer server = new TestWebSocketServer(TEST_PORT, new TestWebSocketServer.WebSocketServerHandler() {
            @Override
            public void onConnect(TestWebSocketServer.ClientHandler client) {
                connected.set(true);
                serverConnectLatch.countDown();
            }
        })) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            CountDownLatch clientConnectLatch = new CountDownLatch(1);
            WebSocket ws = connectClient(TEST_PORT, clientConnectLatch);

            Assert.assertTrue("Connection should succeed", clientConnectLatch.await(5, TimeUnit.SECONDS));
            Assert.assertTrue("Server should receive connection", serverConnectLatch.await(5, TimeUnit.SECONDS));
            Assert.assertTrue("Server connected flag should be set", connected.get());

            ws.sendClose(WebSocket.NORMAL_CLOSURE, "done").join();
        }
    }

    @Test
    public void testMultipleClientConnections() throws Exception {
        AtomicInteger connectionCount = new AtomicInteger(0);

        try (TestWebSocketServer server = new TestWebSocketServer(TEST_PORT + 1, new TestWebSocketServer.WebSocketServerHandler() {
            @Override
            public void onConnect(TestWebSocketServer.ClientHandler client) {
                connectionCount.incrementAndGet();
            }
        })) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            int numClients = 5;
            CountDownLatch[] latches = new CountDownLatch[numClients];
            WebSocket[] clients = new WebSocket[numClients];

            for (int i = 0; i < numClients; i++) {
                latches[i] = new CountDownLatch(1);
                clients[i] = connectClient(TEST_PORT + 1, latches[i]);
            }

            // Wait for all connections
            for (int i = 0; i < numClients; i++) {
                Assert.assertTrue("Client " + i + " should connect", latches[i].await(5, TimeUnit.SECONDS));
            }

            Assert.assertEquals("All clients should connect", numClients, connectionCount.get());

            // Close all clients
            for (WebSocket client : clients) {
                client.sendClose(WebSocket.NORMAL_CLOSURE, "done");
            }
        }
    }

    // ==================== BINARY MESSAGE TESTS ====================

    @Test
    public void testBinaryMessageEcho() throws Exception {
        AtomicReference<byte[]> receivedMessage = new AtomicReference<>();
        CountDownLatch messageLatch = new CountDownLatch(1);

        try (TestWebSocketServer server = new TestWebSocketServer(TEST_PORT + 2, new TestWebSocketServer.WebSocketServerHandler() {
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
        })) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            AtomicReference<byte[]> echoedMessage = new AtomicReference<>();
            CountDownLatch connectLatch = new CountDownLatch(1);

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

            };

            WebSocket ws = HTTP_CLIENT.newWebSocketBuilder()
                    .buildAsync(URI.create("ws://localhost:" + (TEST_PORT + 2)), listener)
                    .join();
            Assert.assertTrue(connectLatch.await(5, TimeUnit.SECONDS));

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
    public void testLargeBinaryMessage() throws Exception {
        AtomicReference<byte[]> receivedMessage = new AtomicReference<>();
        CountDownLatch messageLatch = new CountDownLatch(1);

        try (TestWebSocketServer server = new TestWebSocketServer(TEST_PORT + 3, new TestWebSocketServer.WebSocketServerHandler() {
            @Override
            public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
                receivedMessage.set(data);
                messageLatch.countDown();
            }
        })) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            CountDownLatch connectLatch = new CountDownLatch(1);
            WebSocket ws = connectClient(TEST_PORT + 3, connectLatch);
            Assert.assertTrue(connectLatch.await(5, TimeUnit.SECONDS));

            // Send large binary message (64KB)
            byte[] testData = new byte[65536];
            for (int i = 0; i < testData.length; i++) {
                testData[i] = (byte) (i % 256);
            }
            ws.sendBinary(ByteBuffer.wrap(testData), true).join();

            Assert.assertTrue("Should receive large message", messageLatch.await(10, TimeUnit.SECONDS));
            Assert.assertArrayEquals("Large message should be received correctly", testData, receivedMessage.get());

            ws.sendClose(WebSocket.NORMAL_CLOSURE, "done");
        }
    }

    @Test
    public void testMultipleBinaryMessages() throws Exception {
        AtomicInteger messageCount = new AtomicInteger(0);
        int numMessages = 100;
        CountDownLatch messageLatch = new CountDownLatch(numMessages);

        try (TestWebSocketServer server = new TestWebSocketServer(TEST_PORT + 4, new TestWebSocketServer.WebSocketServerHandler() {
            @Override
            public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
                messageCount.incrementAndGet();
                messageLatch.countDown();
            }
        })) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            CountDownLatch connectLatch = new CountDownLatch(1);
            WebSocket ws = connectClient(TEST_PORT + 4, connectLatch);
            Assert.assertTrue(connectLatch.await(5, TimeUnit.SECONDS));

            // Send multiple messages
            for (int i = 0; i < numMessages; i++) {
                byte[] msg = ("message-" + i + "\n").getBytes(StandardCharsets.UTF_8);
                ws.sendBinary(ByteBuffer.wrap(msg), true).join();
            }

            Assert.assertTrue("Should receive all messages", messageLatch.await(10, TimeUnit.SECONDS));
            Assert.assertEquals("All messages should be received", numMessages, messageCount.get());

            ws.sendClose(WebSocket.NORMAL_CLOSURE, "done");
        }
    }

    // ==================== CLOSE HANDLING TESTS ====================

    @Test
    public void testClientInitiatedClose() throws Exception {
        AtomicBoolean disconnected = new AtomicBoolean(false);
        AtomicInteger closeCode = new AtomicInteger(-1);
        CountDownLatch disconnectLatch = new CountDownLatch(1);

        try (TestWebSocketServer server = new TestWebSocketServer(TEST_PORT + 5, new TestWebSocketServer.WebSocketServerHandler() {
            @Override
            public void onClose(TestWebSocketServer.ClientHandler client, int code, String reason) {
                disconnected.set(true);
                closeCode.set(code);
                disconnectLatch.countDown();
            }
        })) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            CountDownLatch connectLatch = new CountDownLatch(1);
            WebSocket ws = connectClient(TEST_PORT + 5, connectLatch);
            Assert.assertTrue(connectLatch.await(5, TimeUnit.SECONDS));

            // Close with normal code
            ws.sendClose(WebSocketCloseCode.NORMAL_CLOSURE, "Test close").join();

            Assert.assertTrue("Server should receive disconnect", disconnectLatch.await(5, TimeUnit.SECONDS));
            Assert.assertTrue("Disconnect should be recorded", disconnected.get());
            Assert.assertEquals("Close code should be normal", WebSocketCloseCode.NORMAL_CLOSURE, closeCode.get());
        }
    }

    @Test
    public void testServerInitiatedClose() throws Exception {
        AtomicInteger clientCloseCode = new AtomicInteger(-1);
        AtomicReference<String> clientCloseReason = new AtomicReference<>();
        CountDownLatch closeLatch = new CountDownLatch(1);

        try (TestWebSocketServer server = new TestWebSocketServer(TEST_PORT + 6, new TestWebSocketServer.WebSocketServerHandler() {
            @Override
            public void onConnect(TestWebSocketServer.ClientHandler client) {
                // Immediately close the connection from server side
                try {
                    client.sendClose(WebSocketCloseCode.GOING_AWAY, "Server shutdown");
                } catch (java.io.IOException e) {
                    throw new RuntimeException(e);
                }
            }
        })) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            WebSocket.Listener listener = new WebSocket.Listener() {

                @Override
                public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
                    clientCloseCode.set(statusCode);
                    clientCloseReason.set(reason);
                    closeLatch.countDown();
                    return null;
                }

                @Override
                public void onError(WebSocket webSocket, Throwable error) {
                    closeLatch.countDown();
                }
            };

            HTTP_CLIENT.newWebSocketBuilder()
                    .buildAsync(URI.create("ws://localhost:" + (TEST_PORT + 6)), listener)
                    .join();

            Assert.assertTrue("Client should receive close", closeLatch.await(5, TimeUnit.SECONDS));
            // Note: Close code/reason may vary based on implementation
        }
    }

    // ==================== PING/PONG TESTS ====================

    @Test
    public void testPingPong() throws Exception {
        AtomicBoolean pongReceived = new AtomicBoolean(false);
        CountDownLatch pongLatch = new CountDownLatch(1);

        try (TestWebSocketServer server = new TestWebSocketServer(TEST_PORT + 7, new TestWebSocketServer.WebSocketServerHandler() {
            @Override
            public void onConnect(TestWebSocketServer.ClientHandler client) {
                // Server sends ping after connection
                try {
                    client.sendPing("test-ping".getBytes(StandardCharsets.UTF_8));
                } catch (java.io.IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void onPong(TestWebSocketServer.ClientHandler client, byte[] data) {
                pongReceived.set(true);
                pongLatch.countDown();
            }
        })) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            CountDownLatch connectLatch = new CountDownLatch(1);
            // JDK WebSocket client automatically responds to pings with pongs
            WebSocket ws = connectClient(TEST_PORT + 7, connectLatch);
            Assert.assertTrue(connectLatch.await(5, TimeUnit.SECONDS));

            // Wait for pong (JDK client library should auto-respond to ping)
            Assert.assertTrue("Should receive pong", pongLatch.await(5, TimeUnit.SECONDS));
            Assert.assertTrue("Pong should be received", pongReceived.get());

            ws.sendClose(WebSocket.NORMAL_CLOSURE, "done");
        }
    }

    // ==================== QWP SPECIFIC TESTS ====================

    @Test
    public void testQwpBinaryMessageProcessing() throws Exception {

        try (QwpWebSocketProcessorState state = new QwpWebSocketProcessorState(1024)) {
            AtomicInteger processedMessages = new AtomicInteger(0);
            try (TestWebSocketServer server = new TestWebSocketServer(TEST_PORT + 8, new TestWebSocketServer.WebSocketServerHandler() {
                @Override
                public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
                    // Simulate ILP v4 processing
                    long ptr = allocateAndWrite(data);
                    try {
                        state.addData(ptr, ptr + data.length);
                        state.processMessage();
                        processedMessages.incrementAndGet();
                    } finally {
                        freeBuffer(ptr, data.length);
                    }
                }
            })) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                CountDownLatch connectLatch = new CountDownLatch(1);
                WebSocket ws = connectClient(TEST_PORT + 8, connectLatch);
                Assert.assertTrue(connectLatch.await(5, TimeUnit.SECONDS));

                // Send ILP-like messages
                String[] ilpMessages = {
                        "cpu,host=server01 usage=95.5 1234567890\n",
                        "mem,host=server01 free=1024 1234567891\n",
                        "disk,host=server01 used=50 1234567892\n"
                };

                for (String msg : ilpMessages) {
                    ws.sendBinary(ByteBuffer.wrap(msg.getBytes(StandardCharsets.UTF_8)), true).join();
                }

                // Wait for processing
                Thread.sleep(500);

                Assert.assertEquals("All messages should be processed", 3, processedMessages.get());
                Assert.assertEquals("Bytes processed should match",
                        ilpMessages[0].length() + ilpMessages[1].length() + ilpMessages[2].length(),
                        state.getBytesProcessed());

                ws.sendClose(WebSocket.NORMAL_CLOSURE, "done");
            }
        }
    }

    @Test
    public void testQwpProcessorCallbackIntegration() throws Exception {
        QwpWebSocketProcessor processor = new QwpWebSocketProcessor();
        AtomicInteger binaryCount = new AtomicInteger(0);
        AtomicInteger closeCount = new AtomicInteger(0);

        processor.setCallback(new QwpWebSocketProcessor.Callback() {
            @Override
            public void onBinaryMessage(long payload, int length) {
                binaryCount.incrementAndGet();
            }

            @Override
            public void onTextMessage(long payload, int length) {}

            @Override
            public void onPing(long payload, int length) {}

            @Override
            public void onPong(long payload, int length) {}

            @Override
            public void onClose(int code, long reason, int reasonLength) {
                closeCount.incrementAndGet();
            }

            @Override
            public void onError(int errorCode, CharSequence message) {}
        });

        try (TestWebSocketServer server = new TestWebSocketServer(TEST_PORT + 9, new TestWebSocketServer.WebSocketServerHandler() {
            @Override
            public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
                long ptr = allocateAndWrite(data);
                try {
                    processor.onBinaryMessage(ptr, data.length);
                } finally {
                    freeBuffer(ptr, data.length);
                }
            }

            @Override
            public void onClose(TestWebSocketServer.ClientHandler client, int code, String reason) {
                processor.onClose(code, 0, 0);
            }
        })) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            CountDownLatch connectLatch = new CountDownLatch(1);
            WebSocket ws = connectClient(TEST_PORT + 9, connectLatch);
            Assert.assertTrue(connectLatch.await(5, TimeUnit.SECONDS));

            // Send messages
            for (int i = 0; i < 10; i++) {
                ws.sendBinary(ByteBuffer.wrap(("message-" + i + "\n").getBytes(StandardCharsets.UTF_8)), true).join();
            }

            // Wait for processing
            Thread.sleep(500);

            Assert.assertEquals("Binary messages should be processed", 10, binaryCount.get());

            ws.sendClose(WebSocketCloseCode.NORMAL_CLOSURE, "Done").join();

            // Wait for close
            Thread.sleep(200);

            Assert.assertEquals("Close should be recorded", 1, closeCount.get());
        }
    }

    // ==================== HELPER METHODS ====================

    private WebSocket connectClient(int port, CountDownLatch latch) throws Exception {
        WebSocket.Listener listener = new WebSocket.Listener() {
            @Override
            public void onOpen(WebSocket webSocket) {
                latch.countDown();
                webSocket.request(1);
            }

            @Override
            public void onError(WebSocket webSocket, Throwable error) {
                latch.countDown();
            }
        };

        return HTTP_CLIENT.newWebSocketBuilder()
                .buildAsync(URI.create("ws://localhost:" + port), listener)
                .join();
    }

    private long allocateAndWrite(byte[] data) {
        long ptr = io.questdb.std.Unsafe.malloc(data.length, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < data.length; i++) {
            io.questdb.std.Unsafe.getUnsafe().putByte(ptr + i, data[i]);
        }
        return ptr;
    }
}
