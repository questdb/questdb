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
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Integration tests for WebSocket implementation using JDK's built-in WebSocket client.
 * These tests verify end-to-end WebSocket communication between the JDK client
 * and our WebSocket server implementation.
 *
 * <p>All tests use proper synchronization (latches, polling) instead of fixed
 * sleeps to ensure reliability on slow CI environments.
 */
public class WebSocketIntegrationTest extends AbstractWebSocketTest {
    private static final Log LOG = LogFactory.getLog(WebSocketIntegrationTest.class);
    private static final int TEST_PORT = 19876;
    private static final long TIMEOUT_SECONDS = 30;  // Generous timeout for slow CI

    private TestWebSocketServer server;
    private RecordingServerHandler serverHandler;
    private HttpClient httpClient;

    @Before
    public void setUp() {
        serverHandler = new RecordingServerHandler();
        server = new TestWebSocketServer(TEST_PORT, serverHandler);
        httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(TIMEOUT_SECONDS))
                .build();
        try {
            server.start();
            Assert.assertTrue("Server failed to start", server.awaitStart(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        } catch (Exception e) {
            throw new RuntimeException("Failed to start test server", e);
        }
    }

    @After
    public void tearDown() {
        if (server != null) {
            server.close();
        }
    }

    // ==================== CONNECTION TESTS ====================

    @Test
    public void testBasicConnection() throws Exception {
        CountDownLatch connectLatch = new CountDownLatch(1);
        CountDownLatch closeLatch = new CountDownLatch(1);

        WebSocket ws = connectClient(connectLatch, closeLatch);

        Assert.assertTrue("Connection timeout", connectLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

        ws.sendClose(WebSocket.NORMAL_CLOSURE, "done").join();
        Assert.assertTrue("Close timeout", closeLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    }

    @Test
    public void testMultipleConnections() throws Exception {
        int numClients = 5;
        List<WebSocket> clients = new ArrayList<>();
        CountDownLatch allConnected = new CountDownLatch(numClients);

        try {
            for (int i = 0; i < numClients; i++) {
                WebSocket ws = connectClient(allConnected, new CountDownLatch(1));
                clients.add(ws);
            }

            Assert.assertTrue("Not all clients connected",
                    allConnected.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            Assert.assertEquals(numClients, server.getActiveConnectionCount());
        } finally {
            for (WebSocket client : clients) {
                client.sendClose(WebSocket.NORMAL_CLOSURE, "done");
            }
        }
    }

    @Test
    public void testReconnection() throws Exception {
        for (int i = 0; i < 3; i++) {
            CountDownLatch connectLatch = new CountDownLatch(1);
            CountDownLatch closeLatch = new CountDownLatch(1);

            WebSocket ws = connectClient(connectLatch, closeLatch);

            Assert.assertTrue("Connection timeout on attempt " + i,
                    connectLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

            ws.sendClose(WebSocket.NORMAL_CLOSURE, "done").join();
            Assert.assertTrue("Close timeout on attempt " + i,
                    closeLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        }

        Assert.assertEquals(3, server.getConnectionCount());
    }

    // ==================== BINARY MESSAGE TESTS ====================

    @Test
    public void testSendBinaryMessage() throws Exception {
        CountDownLatch connectLatch = new CountDownLatch(1);
        CountDownLatch messageLatch = new CountDownLatch(1);
        serverHandler.binaryMessageLatch = messageLatch;

        WebSocket ws = connectClient(connectLatch, new CountDownLatch(1));
        Assert.assertTrue(connectLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

        try {
            byte[] data = {0x01, 0x02, 0x03, 0x04, 0x05};
            ws.sendBinary(ByteBuffer.wrap(data), true).join();

            Assert.assertTrue("Message not received", messageLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            Assert.assertEquals(1, serverHandler.binaryMessages.size());
            Assert.assertArrayEquals(data, serverHandler.binaryMessages.get(0));
        } finally {
            ws.sendClose(WebSocket.NORMAL_CLOSURE, "done");
        }
    }

    @Test
    public void testSendLargeBinaryMessage() throws Exception {
        CountDownLatch connectLatch = new CountDownLatch(1);
        CountDownLatch messageLatch = new CountDownLatch(1);
        serverHandler.binaryMessageLatch = messageLatch;

        WebSocket ws = connectClient(connectLatch, new CountDownLatch(1));
        Assert.assertTrue(connectLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

        try {
            // Send 100KB of data
            byte[] data = new byte[100 * 1024];
            new Random(42).nextBytes(data);
            ws.sendBinary(ByteBuffer.wrap(data), true).join();

            Assert.assertTrue("Message not received", messageLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            Assert.assertEquals(1, serverHandler.binaryMessages.size());
            Assert.assertArrayEquals(data, serverHandler.binaryMessages.get(0));
        } finally {
            ws.sendClose(WebSocket.NORMAL_CLOSURE, "done");
        }
    }

    @Test
    public void testSendMultipleBinaryMessages() throws Exception {
        int numMessages = 100;
        CountDownLatch connectLatch = new CountDownLatch(1);
        CountDownLatch messageLatch = new CountDownLatch(numMessages);
        serverHandler.binaryMessageLatch = messageLatch;

        WebSocket ws = connectClient(connectLatch, new CountDownLatch(1));
        Assert.assertTrue(connectLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

        try {
            for (int i = 0; i < numMessages; i++) {
                byte[] data = new byte[]{(byte) i, (byte) (i + 1), (byte) (i + 2)};
                ws.sendBinary(ByteBuffer.wrap(data), true).join();
            }

            Assert.assertTrue("Not all messages received",
                    messageLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            Assert.assertEquals(numMessages, serverHandler.binaryMessages.size());
        } finally {
            ws.sendClose(WebSocket.NORMAL_CLOSURE, "done");
        }
    }

    @Test
    public void testReceiveBinaryMessage() throws Exception {
        CountDownLatch connectLatch = new CountDownLatch(1);
        CountDownLatch messageLatch = new CountDownLatch(1);
        CountDownLatch serverReadyLatch = new CountDownLatch(1);
        serverHandler.connectLatch = serverReadyLatch;
        List<byte[]> received = new CopyOnWriteArrayList<>();

        WebSocket ws = createClientWithBinaryHandler(connectLatch, new CountDownLatch(1), messageLatch, received);
        Assert.assertTrue(connectLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        Assert.assertTrue("Server not ready", serverReadyLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

        try {
            // Server sends message to client
            byte[] data = {0x0A, 0x0B, 0x0C, 0x0D, 0x0E};
            serverHandler.lastClient.sendBinary(data);

            Assert.assertTrue("Message receive timeout", messageLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            Assert.assertEquals(1, received.size());
            Assert.assertArrayEquals(data, received.get(0));
        } finally {
            ws.sendClose(WebSocket.NORMAL_CLOSURE, "done");
        }
    }

    // ==================== TEXT MESSAGE TESTS ====================

    @Test
    public void testSendTextMessage() throws Exception {
        CountDownLatch connectLatch = new CountDownLatch(1);
        CountDownLatch messageLatch = new CountDownLatch(1);
        serverHandler.textMessageLatch = messageLatch;

        WebSocket ws = connectClient(connectLatch, new CountDownLatch(1));
        Assert.assertTrue(connectLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

        try {
            String message = "Hello, WebSocket!";
            ws.sendText(message, true).join();

            Assert.assertTrue("Message not received", messageLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            Assert.assertEquals(1, serverHandler.textMessages.size());
            Assert.assertEquals(message, serverHandler.textMessages.get(0));
        } finally {
            ws.sendClose(WebSocket.NORMAL_CLOSURE, "done");
        }
    }

    @Test
    public void testSendUnicodeTextMessage() throws Exception {
        CountDownLatch connectLatch = new CountDownLatch(1);
        CountDownLatch messageLatch = new CountDownLatch(1);
        serverHandler.textMessageLatch = messageLatch;

        WebSocket ws = connectClient(connectLatch, new CountDownLatch(1));
        Assert.assertTrue(connectLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

        try {
            String message = "Hello 你好 مرحبا 🎉 Привет";
            ws.sendText(message, true).join();

            Assert.assertTrue("Message not received", messageLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            Assert.assertEquals(1, serverHandler.textMessages.size());
            Assert.assertEquals(message, serverHandler.textMessages.get(0));
        } finally {
            ws.sendClose(WebSocket.NORMAL_CLOSURE, "done");
        }
    }

    @Test
    public void testReceiveTextMessage() throws Exception {
        CountDownLatch connectLatch = new CountDownLatch(1);
        CountDownLatch messageLatch = new CountDownLatch(1);
        CountDownLatch serverReadyLatch = new CountDownLatch(1);
        serverHandler.connectLatch = serverReadyLatch;
        AtomicReference<String> received = new AtomicReference<>();

        WebSocket ws = createClientWithTextHandler(connectLatch, new CountDownLatch(1), messageLatch, received);
        Assert.assertTrue(connectLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        Assert.assertTrue("Server not ready", serverReadyLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

        try {
            String message = "Server response!";
            serverHandler.lastClient.sendText(message);

            Assert.assertTrue("Message receive timeout", messageLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            Assert.assertEquals(message, received.get());
        } finally {
            ws.sendClose(WebSocket.NORMAL_CLOSURE, "done");
        }
    }

    // ==================== PING/PONG TESTS ====================

    @Test
    public void testPingPong() throws Exception {
        CountDownLatch connectLatch = new CountDownLatch(1);
        CountDownLatch pongLatch = new CountDownLatch(1);
        AtomicReference<ByteBuffer> receivedPong = new AtomicReference<>();

        WebSocket ws = createClientWithPongHandler(connectLatch, new CountDownLatch(1), pongLatch, receivedPong);
        Assert.assertTrue(connectLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

        try {
            // Send ping
            byte[] pingData = {0x01, 0x02, 0x03, 0x04};
            ws.sendPing(ByteBuffer.wrap(pingData)).join();

            // Wait for pong
            Assert.assertTrue("Pong timeout", pongLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            Assert.assertNotNull(receivedPong.get());

            byte[] pongData = new byte[receivedPong.get().remaining()];
            receivedPong.get().get(pongData);
            Assert.assertArrayEquals(pingData, pongData);
        } finally {
            ws.sendClose(WebSocket.NORMAL_CLOSURE, "done");
        }
    }

    @Test
    public void testServerPing() throws Exception {
        CountDownLatch connectLatch = new CountDownLatch(1);
        CountDownLatch pingLatch = new CountDownLatch(1);
        CountDownLatch serverReadyLatch = new CountDownLatch(1);
        CountDownLatch pongReceivedLatch = new CountDownLatch(1);
        serverHandler.connectLatch = serverReadyLatch;
        serverHandler.pongLatch = pongReceivedLatch;
        AtomicReference<ByteBuffer> receivedPing = new AtomicReference<>();

        WebSocket ws = createClientWithPingHandler(connectLatch, new CountDownLatch(1), pingLatch, receivedPing);
        Assert.assertTrue(connectLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        Assert.assertTrue("Server not ready", serverReadyLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

        try {
            byte[] pingData = {0x05, 0x06, 0x07, 0x08};
            serverHandler.lastClient.sendPing(pingData);

            Assert.assertTrue("Ping receive timeout", pingLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            Assert.assertNotNull(receivedPing.get());

            // Server should receive pong (JDK WebSocket auto-responds to pings)
            Assert.assertTrue("Pong not received by server",
                    pongReceivedLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            Assert.assertEquals(1, serverHandler.pongCount.get());
        } finally {
            ws.sendClose(WebSocket.NORMAL_CLOSURE, "done");
        }
    }

    // ==================== CLOSE TESTS ====================

    @Test
    public void testClientInitiatedClose() throws Exception {
        CountDownLatch connectLatch = new CountDownLatch(1);
        CountDownLatch closeLatch = new CountDownLatch(1);
        CountDownLatch serverCloseLatch = new CountDownLatch(1);
        serverHandler.closeLatch = serverCloseLatch;

        WebSocket ws = connectClient(connectLatch, closeLatch);
        Assert.assertTrue(connectLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

        try {
            ws.sendClose(WebSocketCloseCode.NORMAL_CLOSURE, "Test close").join();

            Assert.assertTrue("Close timeout", closeLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            Assert.assertTrue("Server close not received",
                    serverCloseLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

            Assert.assertEquals(1, serverHandler.closeCount.get());
            Assert.assertEquals(WebSocketCloseCode.NORMAL_CLOSURE, serverHandler.lastCloseCode);
        } finally {
            if (!ws.isOutputClosed()) {
                ws.sendClose(WebSocket.NORMAL_CLOSURE, "done");
            }
        }
    }

    @Test
    public void testServerInitiatedClose() throws Exception {
        CountDownLatch connectLatch = new CountDownLatch(1);
        CountDownLatch closeLatch = new CountDownLatch(1);
        CountDownLatch serverReadyLatch = new CountDownLatch(1);
        serverHandler.connectLatch = serverReadyLatch;
        AtomicInteger closeCode = new AtomicInteger(-1);
        AtomicReference<String> closeReason = new AtomicReference<>();

        WebSocket ws = createClientWithCloseHandler(connectLatch, closeLatch, closeCode, closeReason);
        Assert.assertTrue(connectLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        Assert.assertTrue("Server not ready", serverReadyLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

        try {
            serverHandler.lastClient.sendClose(WebSocketCloseCode.GOING_AWAY, "Server closing");

            Assert.assertTrue("Close timeout", closeLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            Assert.assertEquals(WebSocketCloseCode.GOING_AWAY, closeCode.get());
            Assert.assertEquals("Server closing", closeReason.get());
        } finally {
            if (!ws.isOutputClosed()) {
                ws.sendClose(WebSocket.NORMAL_CLOSURE, "done");
            }
        }
    }

    @Test
    public void testCloseWithDifferentCodes() throws Exception {
        // Java HttpClient only allows certain close codes from clients (per RFC 6455).
        // Codes like 1002 (Protocol Error), 1003 (Unsupported Data), 1011 (Internal Error)
        // are server-only and will throw IllegalArgumentException.
        // Only test with client-allowed codes.
        int[] closeCodes = {
                WebSocketCloseCode.NORMAL_CLOSURE,
                WebSocketCloseCode.GOING_AWAY
        };

        for (int expectedCode : closeCodes) {
            serverHandler.clear();
            CountDownLatch connectLatch = new CountDownLatch(1);
            CountDownLatch closeLatch = new CountDownLatch(1);
            CountDownLatch serverCloseLatch = new CountDownLatch(1);
            serverHandler.closeLatch = serverCloseLatch;

            WebSocket ws = connectClient(connectLatch, closeLatch);
            Assert.assertTrue("Connect timeout for code " + expectedCode,
                    connectLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

            ws.sendClose(expectedCode, "Test " + expectedCode).join();

            Assert.assertTrue("Close timeout for code " + expectedCode,
                    closeLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            Assert.assertTrue("Server close not received for code " + expectedCode,
                    serverCloseLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

            Assert.assertEquals("Wrong close code", expectedCode, serverHandler.lastCloseCode);
        }
    }

    // ==================== STRESS TESTS ====================

    @Test
    public void testHighThroughput() throws Exception {
        int numMessages = 1000;
        CountDownLatch connectLatch = new CountDownLatch(1);
        CountDownLatch messageLatch = new CountDownLatch(numMessages);
        serverHandler.binaryMessageLatch = messageLatch;

        WebSocket ws = connectClient(connectLatch, new CountDownLatch(1));
        Assert.assertTrue(connectLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

        try {
            for (int i = 0; i < numMessages; i++) {
                byte[] data = ("message-" + i).getBytes(StandardCharsets.UTF_8);
                ws.sendBinary(ByteBuffer.wrap(data), true).join();
            }

            Assert.assertTrue("Not all messages received",
                    messageLatch.await(TIMEOUT_SECONDS * 2, TimeUnit.SECONDS));
            Assert.assertEquals(numMessages, serverHandler.binaryMessages.size());
        } finally {
            ws.sendClose(WebSocket.NORMAL_CLOSURE, "done");
        }
    }

    @Test
    public void testConcurrentClients() throws Exception {
        int numClients = 10;
        int messagesPerClient = 50;
        CountDownLatch allConnected = new CountDownLatch(numClients);
        CountDownLatch allMessagesSent = new CountDownLatch(numClients);
        int expectedTotal = numClients * messagesPerClient;
        CountDownLatch allMessagesReceived = new CountDownLatch(expectedTotal);
        serverHandler.binaryMessageLatch = allMessagesReceived;

        List<WebSocket> clients = new ArrayList<>();

        try {
            for (int i = 0; i < numClients; i++) {
                final int clientId = i;

                WebSocket.Listener listener = new WebSocket.Listener() {
                    @Override
                    public void onOpen(WebSocket webSocket) {
                        allConnected.countDown();
                        webSocket.request(1);
                        // Send messages in a separate thread
                        new Thread(() -> {
                            try {
                                for (int j = 0; j < messagesPerClient; j++) {
                                    webSocket.sendBinary(
                                            ByteBuffer.wrap(("client-" + clientId + "-msg-" + j).getBytes()),
                                            true
                                    ).join();
                                }
                            } finally {
                                allMessagesSent.countDown();
                            }
                        }).start();
                    }

                };

                WebSocket ws = httpClient.newWebSocketBuilder()
                        .buildAsync(URI.create("ws://localhost:" + TEST_PORT + "/"), listener)
                        .join();
                clients.add(ws);
            }

            Assert.assertTrue("Not all clients connected",
                    allConnected.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            Assert.assertTrue("Not all messages sent",
                    allMessagesSent.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            Assert.assertTrue("Not all messages received",
                    allMessagesReceived.await(TIMEOUT_SECONDS * 2, TimeUnit.SECONDS));

            Assert.assertEquals("Not all messages received",
                    expectedTotal, serverHandler.binaryMessages.size());
        } finally {
            for (WebSocket client : clients) {
                client.sendClose(WebSocket.NORMAL_CLOSURE, "done");
            }
        }
    }

    @Test
    public void testBidirectionalCommunication() throws Exception {
        int numRoundTrips = 100;
        CountDownLatch connectLatch = new CountDownLatch(1);
        CountDownLatch allReceived = new CountDownLatch(numRoundTrips);
        List<String> clientReceived = new CopyOnWriteArrayList<>();

        // Set up echo handler on server
        serverHandler.echoMode = true;

        WebSocket.Listener listener = new WebSocket.Listener() {
            private StringBuilder textBuffer = new StringBuilder();

            @Override
            public void onOpen(WebSocket webSocket) {
                connectLatch.countDown();
                webSocket.request(1);
            }

            @Override
            public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
                textBuffer.append(data);
                if (last) {
                    clientReceived.add(textBuffer.toString());
                    allReceived.countDown();
                    textBuffer = new StringBuilder();
                }
                webSocket.request(1);
                return null;
            }

        };

        WebSocket ws = httpClient.newWebSocketBuilder()
                .buildAsync(URI.create("ws://localhost:" + TEST_PORT + "/"), listener)
                .join();
        Assert.assertTrue(connectLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

        try {
            // Send messages
            for (int i = 0; i < numRoundTrips; i++) {
                ws.sendText("echo-" + i, true).join();
            }

            Assert.assertTrue("Not all echoes received",
                    allReceived.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            Assert.assertEquals(numRoundTrips, clientReceived.size());

            // Verify order
            for (int i = 0; i < numRoundTrips; i++) {
                Assert.assertEquals("echo-" + i, clientReceived.get(i));
            }
        } finally {
            ws.sendClose(WebSocket.NORMAL_CLOSURE, "done");
        }
    }

    // ==================== EDGE CASE TESTS ====================

    @Test
    public void testEmptyBinaryMessage() throws Exception {
        CountDownLatch connectLatch = new CountDownLatch(1);
        CountDownLatch messageLatch = new CountDownLatch(1);
        serverHandler.binaryMessageLatch = messageLatch;

        WebSocket ws = connectClient(connectLatch, new CountDownLatch(1));
        Assert.assertTrue(connectLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

        try {
            ws.sendBinary(ByteBuffer.wrap(new byte[0]), true).join();

            Assert.assertTrue("Message not received", messageLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            Assert.assertEquals(1, serverHandler.binaryMessages.size());
            Assert.assertEquals(0, serverHandler.binaryMessages.get(0).length);
        } finally {
            ws.sendClose(WebSocket.NORMAL_CLOSURE, "done");
        }
    }

    @Test
    public void testEmptyTextMessage() throws Exception {
        CountDownLatch connectLatch = new CountDownLatch(1);
        CountDownLatch messageLatch = new CountDownLatch(1);
        serverHandler.textMessageLatch = messageLatch;

        WebSocket ws = connectClient(connectLatch, new CountDownLatch(1));
        Assert.assertTrue(connectLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

        try {
            ws.sendText("", true).join();

            Assert.assertTrue("Message not received", messageLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            Assert.assertEquals(1, serverHandler.textMessages.size());
            Assert.assertEquals("", serverHandler.textMessages.get(0));
        } finally {
            ws.sendClose(WebSocket.NORMAL_CLOSURE, "done");
        }
    }

    @Test
    public void testRapidConnectDisconnect() throws Exception {
        for (int i = 0; i < 10; i++) {
            CountDownLatch connectLatch = new CountDownLatch(1);
            CountDownLatch closeLatch = new CountDownLatch(1);

            WebSocket ws = connectClient(connectLatch, closeLatch);
            Assert.assertTrue("Connect timeout on iteration " + i,
                    connectLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

            // Immediately close
            ws.sendClose(WebSocket.NORMAL_CLOSURE, "done").join();
            Assert.assertTrue("Close timeout on iteration " + i,
                    closeLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testVeryLargeBinaryMessage() throws Exception {
        CountDownLatch connectLatch = new CountDownLatch(1);
        CountDownLatch messageLatch = new CountDownLatch(1);
        serverHandler.binaryMessageLatch = messageLatch;

        WebSocket ws = connectClient(connectLatch, new CountDownLatch(1));
        Assert.assertTrue(connectLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

        try {
            // Send 1MB of data
            byte[] data = new byte[1024 * 1024];
            new Random(42).nextBytes(data);
            ws.sendBinary(ByteBuffer.wrap(data), true).join();

            Assert.assertTrue("Message not received", messageLatch.await(TIMEOUT_SECONDS * 2, TimeUnit.SECONDS));
            Assert.assertEquals(1, serverHandler.binaryMessages.size());
            Assert.assertArrayEquals(data, serverHandler.binaryMessages.get(0));
        } finally {
            ws.sendClose(WebSocket.NORMAL_CLOSURE, "done");
        }
    }

    @Test
    public void testManySmallMessages() throws Exception {
        int numMessages = 5000;
        CountDownLatch connectLatch = new CountDownLatch(1);
        CountDownLatch messageLatch = new CountDownLatch(numMessages);
        serverHandler.binaryMessageLatch = messageLatch;

        WebSocket ws = connectClient(connectLatch, new CountDownLatch(1));
        Assert.assertTrue(connectLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

        try {
            for (int i = 0; i < numMessages; i++) {
                ws.sendBinary(ByteBuffer.wrap(new byte[]{(byte) i}), true).join();
            }

            Assert.assertTrue("Not all messages received",
                    messageLatch.await(TIMEOUT_SECONDS * 2, TimeUnit.SECONDS));
            Assert.assertEquals(numMessages, serverHandler.binaryMessages.size());
        } finally {
            ws.sendClose(WebSocket.NORMAL_CLOSURE, "done");
        }
    }

    @Test
    public void testInterleavedTextAndBinary() throws Exception {
        int numPairs = 100;
        CountDownLatch connectLatch = new CountDownLatch(1);
        CountDownLatch textLatch = new CountDownLatch(numPairs);
        CountDownLatch binaryLatch = new CountDownLatch(numPairs);
        serverHandler.textMessageLatch = textLatch;
        serverHandler.binaryMessageLatch = binaryLatch;

        WebSocket ws = connectClient(connectLatch, new CountDownLatch(1));
        Assert.assertTrue(connectLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

        try {
            for (int i = 0; i < numPairs; i++) {
                ws.sendText("text-" + i, true).join();
                ws.sendBinary(ByteBuffer.wrap(("binary-" + i).getBytes(StandardCharsets.UTF_8)), true).join();
            }

            Assert.assertTrue("Not all text messages received",
                    textLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            Assert.assertTrue("Not all binary messages received",
                    binaryLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

            Assert.assertEquals(numPairs, serverHandler.textMessages.size());
            Assert.assertEquals(numPairs, serverHandler.binaryMessages.size());
        } finally {
            ws.sendClose(WebSocket.NORMAL_CLOSURE, "done");
        }
    }

    @Test
    public void testMultiplePingPong() throws Exception {
        int numPings = 5;
        CountDownLatch connectLatch = new CountDownLatch(1);
        CountDownLatch pongLatch = new CountDownLatch(numPings);
        AtomicInteger pongCount = new AtomicInteger(0);

        WebSocket.Listener listener = new WebSocket.Listener() {
            @Override
            public void onOpen(WebSocket webSocket) {
                connectLatch.countDown();
                webSocket.request(1);
            }

            @Override
            public CompletionStage<?> onPong(WebSocket webSocket, ByteBuffer message) {
                pongCount.incrementAndGet();
                pongLatch.countDown();
                webSocket.request(1);
                return null;
            }

        };

        WebSocket ws = httpClient.newWebSocketBuilder()
                .buildAsync(URI.create("ws://localhost:" + TEST_PORT + "/"), listener)
                .join();
        Assert.assertTrue(connectLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

        try {
            // Send multiple pings
            for (int i = 0; i < numPings; i++) {
                ws.sendPing(ByteBuffer.wrap(new byte[]{(byte) i})).join();
            }

            Assert.assertTrue("Not all pongs received", pongLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            Assert.assertEquals(numPings, pongCount.get());
        } finally {
            ws.sendClose(WebSocket.NORMAL_CLOSURE, "done");
        }
    }

    @Test
    public void testServerBroadcastToMultipleClients() throws Exception {
        int numClients = 5;
        List<WebSocket> clients = new ArrayList<>();
        CountDownLatch allConnected = new CountDownLatch(numClients);
        CountDownLatch allReceived = new CountDownLatch(numClients);
        CountDownLatch serverClientsReady = new CountDownLatch(numClients);
        List<String> receivedMessages = new CopyOnWriteArrayList<>();

        // Enable tracking of all clients
        serverHandler.trackAllClients = true;
        serverHandler.connectLatch = serverClientsReady;

        try {
            for (int i = 0; i < numClients; i++) {
                WebSocket.Listener listener = new WebSocket.Listener() {
                    private StringBuilder textBuffer = new StringBuilder();

                    @Override
                    public void onOpen(WebSocket webSocket) {
                        allConnected.countDown();
                        webSocket.request(1);
                    }

                    @Override
                    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
                        textBuffer.append(data);
                        if (last) {
                            receivedMessages.add(textBuffer.toString());
                            allReceived.countDown();
                            textBuffer = new StringBuilder();
                        }
                        webSocket.request(1);
                        return null;
                    }

                };

                WebSocket ws = httpClient.newWebSocketBuilder()
                        .buildAsync(URI.create("ws://localhost:" + TEST_PORT + "/"), listener)
                        .join();
                clients.add(ws);
            }

            Assert.assertTrue("Not all clients connected",
                    allConnected.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            Assert.assertTrue("Server clients not ready",
                    serverClientsReady.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

            // Server broadcasts to all clients
            String broadcastMessage = "Broadcast!";
            for (TestWebSocketServer.ClientHandler handler : serverHandler.allClients) {
                handler.sendText(broadcastMessage);
            }

            Assert.assertTrue("Not all clients received broadcast",
                    allReceived.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            Assert.assertEquals(numClients, receivedMessages.size());

            for (String msg : receivedMessages) {
                Assert.assertEquals(broadcastMessage, msg);
            }
        } finally {
            for (WebSocket client : clients) {
                client.sendClose(WebSocket.NORMAL_CLOSURE, "done");
            }
        }
    }

    @Test
    public void testCloseWithoutCode() throws Exception {
        CountDownLatch connectLatch = new CountDownLatch(1);
        CountDownLatch closeLatch = new CountDownLatch(1);

        WebSocket ws = connectClient(connectLatch, closeLatch);
        Assert.assertTrue(connectLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

        // Close without explicit code - use normal closure
        ws.sendClose(WebSocket.NORMAL_CLOSURE, "").join();
        Assert.assertTrue("Close timeout", closeLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    }

    @Test
    public void testMaxPingPayload() throws Exception {
        CountDownLatch connectLatch = new CountDownLatch(1);
        CountDownLatch pongLatch = new CountDownLatch(1);
        AtomicReference<ByteBuffer> receivedPong = new AtomicReference<>();

        WebSocket ws = createClientWithPongHandler(connectLatch, new CountDownLatch(1), pongLatch, receivedPong);
        Assert.assertTrue(connectLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

        try {
            // Max control frame payload is 125 bytes
            byte[] pingData = new byte[125];
            for (int i = 0; i < pingData.length; i++) {
                pingData[i] = (byte) i;
            }

            ws.sendPing(ByteBuffer.wrap(pingData)).join();

            Assert.assertTrue("Pong timeout", pongLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            Assert.assertNotNull(receivedPong.get());

            byte[] pongData = new byte[receivedPong.get().remaining()];
            receivedPong.get().get(pongData);
            Assert.assertArrayEquals(pingData, pongData);
        } finally {
            ws.sendClose(WebSocket.NORMAL_CLOSURE, "done");
        }
    }

    // ==================== HELPER METHODS ====================

    private WebSocket connectClient(CountDownLatch connectLatch, CountDownLatch closeLatch) throws Exception {
        WebSocket.Listener listener = new WebSocket.Listener() {
            @Override
            public void onOpen(WebSocket webSocket) {
                connectLatch.countDown();
                webSocket.request(1);
            }

            @Override
            public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
                closeLatch.countDown();
                return null;
            }

            @Override
            public void onError(WebSocket webSocket, Throwable error) {
                LOG.error().$("Client error: ").$(error).$();
            }
        };

        return httpClient.newWebSocketBuilder()
                .buildAsync(URI.create("ws://localhost:" + TEST_PORT + "/"), listener)
                .join();
    }

    private WebSocket createClientWithBinaryHandler(
            CountDownLatch connectLatch,
            CountDownLatch closeLatch,
            CountDownLatch messageLatch,
            List<byte[]> received
    ) throws Exception {
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
                    received.add(bytes);
                    messageLatch.countDown();
                    binaryBuffer = null;
                }
                webSocket.request(1);
                return null;
            }

            @Override
            public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
                closeLatch.countDown();
                return null;
            }

        };

        return httpClient.newWebSocketBuilder()
                .buildAsync(URI.create("ws://localhost:" + TEST_PORT + "/"), listener)
                .join();
    }

    private WebSocket createClientWithTextHandler(
            CountDownLatch connectLatch,
            CountDownLatch closeLatch,
            CountDownLatch messageLatch,
            AtomicReference<String> received
    ) throws Exception {
        WebSocket.Listener listener = new WebSocket.Listener() {
            private StringBuilder textBuffer = new StringBuilder();

            @Override
            public void onOpen(WebSocket webSocket) {
                connectLatch.countDown();
                webSocket.request(1);
            }

            @Override
            public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
                textBuffer.append(data);
                if (last) {
                    received.set(textBuffer.toString());
                    messageLatch.countDown();
                    textBuffer = new StringBuilder();
                }
                webSocket.request(1);
                return null;
            }

            @Override
            public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
                closeLatch.countDown();
                return null;
            }

        };

        return httpClient.newWebSocketBuilder()
                .buildAsync(URI.create("ws://localhost:" + TEST_PORT + "/"), listener)
                .join();
    }

    private WebSocket createClientWithPongHandler(
            CountDownLatch connectLatch,
            CountDownLatch closeLatch,
            CountDownLatch pongLatch,
            AtomicReference<ByteBuffer> receivedPong
    ) throws Exception {
        WebSocket.Listener listener = new WebSocket.Listener() {
            @Override
            public void onOpen(WebSocket webSocket) {
                connectLatch.countDown();
                webSocket.request(1);
            }

            @Override
            public CompletionStage<?> onPong(WebSocket webSocket, ByteBuffer message) {
                // Copy the buffer since it may be reused
                ByteBuffer copy = ByteBuffer.allocate(message.remaining());
                copy.put(message);
                copy.flip();
                receivedPong.set(copy);
                pongLatch.countDown();
                webSocket.request(1);
                return null;
            }

            @Override
            public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
                closeLatch.countDown();
                return null;
            }

        };

        return httpClient.newWebSocketBuilder()
                .buildAsync(URI.create("ws://localhost:" + TEST_PORT + "/"), listener)
                .join();
    }

    private WebSocket createClientWithPingHandler(
            CountDownLatch connectLatch,
            CountDownLatch closeLatch,
            CountDownLatch pingLatch,
            AtomicReference<ByteBuffer> receivedPing
    ) throws Exception {
        WebSocket.Listener listener = new WebSocket.Listener() {
            @Override
            public void onOpen(WebSocket webSocket) {
                connectLatch.countDown();
                webSocket.request(1);
            }

            @Override
            public CompletionStage<?> onPing(WebSocket webSocket, ByteBuffer message) {
                // JDK WebSocket automatically responds with pong
                ByteBuffer copy = ByteBuffer.allocate(message.remaining());
                copy.put(message);
                copy.flip();
                receivedPing.set(copy);
                pingLatch.countDown();
                webSocket.request(1);
                return null;
            }

            @Override
            public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
                closeLatch.countDown();
                return null;
            }

        };

        return httpClient.newWebSocketBuilder()
                .buildAsync(URI.create("ws://localhost:" + TEST_PORT + "/"), listener)
                .join();
    }

    private WebSocket createClientWithCloseHandler(
            CountDownLatch connectLatch,
            CountDownLatch closeLatch,
            AtomicInteger closeCode,
            AtomicReference<String> closeReason
    ) throws Exception {
        WebSocket.Listener listener = new WebSocket.Listener() {
            @Override
            public void onOpen(WebSocket webSocket) {
                connectLatch.countDown();
                webSocket.request(1);
            }

            @Override
            public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
                closeCode.set(statusCode);
                closeReason.set(reason);
                closeLatch.countDown();
                return null;
            }

        };

        return httpClient.newWebSocketBuilder()
                .buildAsync(URI.create("ws://localhost:" + TEST_PORT + "/"), listener)
                .join();
    }

    /**
     * Server handler that records all events for verification.
     * Uses latches for synchronization instead of sleeps.
     */
    private static class RecordingServerHandler implements TestWebSocketServer.WebSocketServerHandler {
        final List<byte[]> binaryMessages = new CopyOnWriteArrayList<>();
        final List<String> textMessages = new CopyOnWriteArrayList<>();
        final List<TestWebSocketServer.ClientHandler> allClients = new CopyOnWriteArrayList<>();
        final AtomicInteger pingCount = new AtomicInteger();
        final AtomicInteger pongCount = new AtomicInteger();
        final AtomicInteger closeCount = new AtomicInteger();
        final AtomicInteger connectCount = new AtomicInteger();
        volatile int lastCloseCode = -1;
        volatile String lastCloseReason;
        volatile TestWebSocketServer.ClientHandler lastClient;
        volatile boolean echoMode = false;
        volatile boolean trackAllClients = false;

        // Latches for synchronization
        volatile CountDownLatch connectLatch;
        volatile CountDownLatch binaryMessageLatch;
        volatile CountDownLatch textMessageLatch;
        volatile CountDownLatch closeLatch;
        volatile CountDownLatch pongLatch;

        @Override
        public void onConnect(TestWebSocketServer.ClientHandler client) {
            connectCount.incrementAndGet();
            lastClient = client;
            if (trackAllClients) {
                allClients.add(client);
            }
            if (connectLatch != null) {
                connectLatch.countDown();
            }
        }

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            binaryMessages.add(data);
            if (binaryMessageLatch != null) {
                binaryMessageLatch.countDown();
            }
        }

        @Override
        public void onTextMessage(TestWebSocketServer.ClientHandler client, String text) {
            textMessages.add(text);
            if (textMessageLatch != null) {
                textMessageLatch.countDown();
            }
            if (echoMode) {
                try {
                    client.sendText(text);
                } catch (Exception e) {
                    // Ignore
                }
            }
        }

        @Override
        public void onPing(TestWebSocketServer.ClientHandler client, byte[] data) {
            pingCount.incrementAndGet();
        }

        @Override
        public void onPong(TestWebSocketServer.ClientHandler client, byte[] data) {
            pongCount.incrementAndGet();
            if (pongLatch != null) {
                pongLatch.countDown();
            }
        }

        @Override
        public void onClose(TestWebSocketServer.ClientHandler client, int code, String reason) {
            closeCount.incrementAndGet();
            lastCloseCode = code;
            lastCloseReason = reason;
            if (closeLatch != null) {
                closeLatch.countDown();
            }
        }

        @Override
        public void onError(TestWebSocketServer.ClientHandler client, Exception e) {
            LOG.error().$("Server handler error: ").$(e).$();
        }

        void clear() {
            binaryMessages.clear();
            textMessages.clear();
            allClients.clear();
            pingCount.set(0);
            pongCount.set(0);
            closeCount.set(0);
            lastCloseCode = -1;
            lastCloseReason = null;
            echoMode = false;
            trackAllClients = false;
            connectLatch = null;
            binaryMessageLatch = null;
            textMessageLatch = null;
            closeLatch = null;
            pongLatch = null;
        }
    }
}
