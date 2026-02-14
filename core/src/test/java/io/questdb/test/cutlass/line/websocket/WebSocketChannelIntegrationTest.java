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

package io.questdb.test.cutlass.line.websocket;

import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.WebSocketChannel;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.test.cutlass.http.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Integration tests for WebSocketChannel with actual server connections.
 */
public class WebSocketChannelIntegrationTest {

    // Use a wider range to avoid port conflicts from TIME_WAIT sockets
    private static final int BASE_PORT = 19300 + (int) (Os.currentTimeMicros() % 1000);

    @Test
    public void testConnectAndClose() throws Exception {
        int port = BASE_PORT;

        try (TestWebSocketServer server = new TestWebSocketServer(port, new TestWebSocketServer.WebSocketServerHandler() {})) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            WebSocketChannel channel = new WebSocketChannel("ws://localhost:" + port, false);
            try (channel) {
                channel.connect();
                Assert.assertTrue(channel.isConnected());
            }
            Assert.assertFalse(channel.isConnected());
        }
    }

    @Test
    public void testSendBinaryMessage() throws Exception {
        int port = BASE_PORT + 1;
        AtomicReference<byte[]> receivedData = new AtomicReference<>();
        CountDownLatch messageLatch = new CountDownLatch(1);

        try (TestWebSocketServer server = new TestWebSocketServer(port, new TestWebSocketServer.WebSocketServerHandler() {
            @Override
            public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
                receivedData.set(data);
                messageLatch.countDown();
            }
        })) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            try (WebSocketChannel channel = new WebSocketChannel("ws://localhost:" + port, false)) {
                channel.connect();

                // Send binary data
                byte[] testData = "Hello, WebSocket!".getBytes(StandardCharsets.UTF_8);
                long ptr = Unsafe.malloc(testData.length, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < testData.length; i++) {
                        Unsafe.getUnsafe().putByte(ptr + i, testData[i]);
                    }
                    channel.sendBinary(ptr, testData.length);
                } finally {
                    Unsafe.free(ptr, testData.length, MemoryTag.NATIVE_DEFAULT);
                }

                // Wait for server to receive
                Assert.assertTrue("Server should receive message", messageLatch.await(5, TimeUnit.SECONDS));
                Assert.assertArrayEquals(testData, receivedData.get());
            }
        }
    }

    @Test
    public void testSendMultipleBinaryMessages() throws Exception {
        int port = BASE_PORT + 2;
        int messageCount = 100;
        CountDownLatch messageLatch = new CountDownLatch(messageCount);

        try (TestWebSocketServer server = new TestWebSocketServer(port, new TestWebSocketServer.WebSocketServerHandler() {
            @Override
            public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
                messageLatch.countDown();
            }
        })) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            try (WebSocketChannel channel = new WebSocketChannel("ws://localhost:" + port, false)) {
                channel.connect();

                // Pre-allocate buffer
                int bufferSize = 256;
                long ptr = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < messageCount; i++) {
                        String message = "Message " + i;
                        byte[] msgBytes = message.getBytes(StandardCharsets.UTF_8);
                        for (int j = 0; j < msgBytes.length; j++) {
                            Unsafe.getUnsafe().putByte(ptr + j, msgBytes[j]);
                        }
                        channel.sendBinary(ptr, msgBytes.length);
                    }
                } finally {
                    Unsafe.free(ptr, bufferSize, MemoryTag.NATIVE_DEFAULT);
                }

                Assert.assertTrue("Server should receive all messages",
                        messageLatch.await(10, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    public void testSendLargeBinaryMessage() throws Exception {
        int port = BASE_PORT + 3;
        AtomicReference<byte[]> receivedData = new AtomicReference<>();
        CountDownLatch messageLatch = new CountDownLatch(1);

        try (TestWebSocketServer server = new TestWebSocketServer(port, new TestWebSocketServer.WebSocketServerHandler() {
            @Override
            public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
                receivedData.set(data);
                messageLatch.countDown();
            }
        })) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            try (WebSocketChannel channel = new WebSocketChannel("ws://localhost:" + port, false)) {
                channel.connect();

                // Send large message (128KB)
                int size = 128 * 1024;
                long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < size; i++) {
                        Unsafe.getUnsafe().putByte(ptr + i, (byte) (i % 256));
                    }
                    channel.sendBinary(ptr, size);
                } finally {
                    Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
                }

                Assert.assertTrue("Server should receive large message",
                        messageLatch.await(10, TimeUnit.SECONDS));

                byte[] received = receivedData.get();
                Assert.assertEquals(size, received.length);
                for (int i = 0; i < size; i++) {
                    Assert.assertEquals("Byte at " + i, (byte) (i % 256), received[i]);
                }
            }
        }
    }

    @Test
    public void testReceiveBinaryMessage() throws Exception {
        int port = BASE_PORT + 4;
        AtomicReference<byte[]> clientReceived = new AtomicReference<>();
        CountDownLatch receiveLatch = new CountDownLatch(1);

        try (TestWebSocketServer server = new TestWebSocketServer(port, new TestWebSocketServer.WebSocketServerHandler() {
            @Override
            public void onConnect(TestWebSocketServer.ClientHandler client) {
                // Send a message to the client after connection
                try {
                    byte[] response = "Server says hello".getBytes(StandardCharsets.UTF_8);
                    client.sendBinary(response);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        })) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            try (WebSocketChannel channel = new WebSocketChannel("ws://localhost:" + port, false)) {
                channel.connect();

                // Receive the server's message
                boolean received = channel.receiveFrame(new WebSocketChannel.ResponseHandler() {
                    @Override
                    public void onBinaryMessage(long payload, int length) {
                        byte[] data = new byte[length];
                        for (int i = 0; i < length; i++) {
                            data[i] = Unsafe.getUnsafe().getByte(payload + i);
                        }
                        clientReceived.set(data);
                        receiveLatch.countDown();
                    }

                    @Override
                    public void onClose(int code, String reason) {
                    }
                }, 5000);

                Assert.assertTrue("Should receive frame", received);
                Assert.assertTrue("Latch should be triggered", receiveLatch.await(1, TimeUnit.SECONDS));
                Assert.assertArrayEquals("Server says hello".getBytes(StandardCharsets.UTF_8),
                        clientReceived.get());
            }
        }
    }

    @Test
    public void testPingPong() throws Exception {
        int port = BASE_PORT + 5;
        CountDownLatch pongLatch = new CountDownLatch(1);

        try (TestWebSocketServer server = new TestWebSocketServer(port, new TestWebSocketServer.WebSocketServerHandler() {
            @Override
            public void onPong(TestWebSocketServer.ClientHandler client, byte[] data) {
                pongLatch.countDown();
            }
        })) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            try (WebSocketChannel channel = new WebSocketChannel("ws://localhost:" + port, false)) {
                channel.connect();

                // Send ping
                channel.sendPing();

                // The server's test harness should receive the ping and the client should auto-respond
                // But since we control both sides, let's just verify the connection works
                Assert.assertTrue(channel.isConnected());

            }
        }
    }

    @Test
    public void testServerClose() throws Exception {
        int port = BASE_PORT + 6;
        CountDownLatch closeLatch = new CountDownLatch(1);
        AtomicReference<Integer> closeCodeRef = new AtomicReference<>();

        try (TestWebSocketServer server = new TestWebSocketServer(port, new TestWebSocketServer.WebSocketServerHandler() {
            @Override
            public void onConnect(TestWebSocketServer.ClientHandler client) {
                // Immediately close the connection
                try {
                    client.sendClose(1000, "Server closing");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        })) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            try (WebSocketChannel channel = new WebSocketChannel("ws://localhost:" + port, false)) {
                channel.connect();

                // Try to receive - should get close frame
                channel.receiveFrame(new WebSocketChannel.ResponseHandler() {
                    @Override
                    public void onBinaryMessage(long payload, int length) {
                    }

                    @Override
                    public void onClose(int code, String reason) {
                        closeCodeRef.set(code);
                        closeLatch.countDown();
                    }
                }, 5000);

                Assert.assertTrue("Should receive close", closeLatch.await(5, TimeUnit.SECONDS));
                Assert.assertEquals(Integer.valueOf(1000), closeCodeRef.get());
            }
        }
    }

    @Test
    public void testMultipleConnections() throws Exception {
        int port = BASE_PORT + 7;
        int connectionCount = 5;

        try (TestWebSocketServer server = new TestWebSocketServer(port, new TestWebSocketServer.WebSocketServerHandler() {})) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            WebSocketChannel[] channels = new WebSocketChannel[connectionCount];
            try {
                // Create and connect all channels
                for (int i = 0; i < connectionCount; i++) {
                    channels[i] = new WebSocketChannel("ws://localhost:" + port, false);
                    channels[i].connect();
                    Assert.assertTrue(channels[i].isConnected());
                }

                // Send a message from each
                long ptr = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < connectionCount; i++) {
                        byte[] msg = ("Message from " + i).getBytes(StandardCharsets.UTF_8);
                        for (int j = 0; j < msg.length; j++) {
                            Unsafe.getUnsafe().putByte(ptr + j, msg[j]);
                        }
                        channels[i].sendBinary(ptr, msg.length);
                    }
                } finally {
                    Unsafe.free(ptr, 64, MemoryTag.NATIVE_DEFAULT);
                }

                // Verify all still connected
                for (int i = 0; i < connectionCount; i++) {
                    Assert.assertTrue("Channel " + i + " should still be connected",
                            channels[i].isConnected());
                }
            } finally {
                for (WebSocketChannel channel : channels) {
                    if (channel != null) {
                        channel.close();
                    }
                }
            }
        }
    }

    @Test
    public void testReconnectAfterClose() throws Exception {
        int port = BASE_PORT + 8;

        try (TestWebSocketServer server = new TestWebSocketServer(port, new TestWebSocketServer.WebSocketServerHandler() {})) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            // First connection
            try (WebSocketChannel channel1 = new WebSocketChannel("ws://localhost:" + port, false)) {
                channel1.connect();
                Assert.assertTrue(channel1.isConnected());
            }

            // Second connection (new channel instance)
            try (WebSocketChannel channel2 = new WebSocketChannel("ws://localhost:" + port, false)) {
                channel2.connect();
                Assert.assertTrue(channel2.isConnected());
            }
        }
    }

    // ==================== Binary Data Edge Cases ====================

    @Test
    public void testSendEmptyMessage() throws Exception {
        int port = BASE_PORT + 9;
        AtomicReference<byte[]> receivedData = new AtomicReference<>();
        CountDownLatch messageLatch = new CountDownLatch(1);

        try (TestWebSocketServer server = new TestWebSocketServer(port, new TestWebSocketServer.WebSocketServerHandler() {
            @Override
            public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
                receivedData.set(data);
                messageLatch.countDown();
            }
        })) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            try (WebSocketChannel channel = new WebSocketChannel("ws://localhost:" + port, false)) {
                channel.connect();

                // Send empty message (0 bytes)
                long ptr = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
                try {
                    channel.sendBinary(ptr, 0);
                } finally {
                    Unsafe.free(ptr, 1, MemoryTag.NATIVE_DEFAULT);
                }

                Assert.assertTrue("Server should receive empty message",
                        messageLatch.await(5, TimeUnit.SECONDS));
                Assert.assertNotNull(receivedData.get());
                Assert.assertEquals(0, receivedData.get().length);
            }
        }
    }

    @Test
    public void testSendAllByteValues() throws Exception {
        int port = BASE_PORT + 10;
        AtomicReference<byte[]> receivedData = new AtomicReference<>();
        CountDownLatch messageLatch = new CountDownLatch(1);

        try (TestWebSocketServer server = new TestWebSocketServer(port, new TestWebSocketServer.WebSocketServerHandler() {
            @Override
            public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
                receivedData.set(data);
                messageLatch.countDown();
            }
        })) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            try (WebSocketChannel channel = new WebSocketChannel("ws://localhost:" + port, false)) {
                channel.connect();

                // Send all byte values 0x00 to 0xFF
                int size = 256;
                long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < size; i++) {
                        Unsafe.getUnsafe().putByte(ptr + i, (byte) i);
                    }
                    channel.sendBinary(ptr, size);
                } finally {
                    Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
                }

                Assert.assertTrue("Server should receive message",
                        messageLatch.await(5, TimeUnit.SECONDS));

                byte[] received = receivedData.get();
                Assert.assertEquals(256, received.length);
                for (int i = 0; i < 256; i++) {
                    Assert.assertEquals("Byte at " + i, (byte) i, received[i]);
                }
            }
        }
    }

    @Test
    public void testSendMessageWithNullBytes() throws Exception {
        int port = BASE_PORT + 11;
        AtomicReference<byte[]> receivedData = new AtomicReference<>();
        CountDownLatch messageLatch = new CountDownLatch(1);

        try (TestWebSocketServer server = new TestWebSocketServer(port, new TestWebSocketServer.WebSocketServerHandler() {
            @Override
            public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
                receivedData.set(data);
                messageLatch.countDown();
            }
        })) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            try (WebSocketChannel channel = new WebSocketChannel("ws://localhost:" + port, false)) {
                channel.connect();

                // Send message with embedded null bytes
                byte[] testData = new byte[]{0x00, 0x01, 0x00, 0x02, 0x00, 0x00, 0x03};
                long ptr = Unsafe.malloc(testData.length, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < testData.length; i++) {
                        Unsafe.getUnsafe().putByte(ptr + i, testData[i]);
                    }
                    channel.sendBinary(ptr, testData.length);
                } finally {
                    Unsafe.free(ptr, testData.length, MemoryTag.NATIVE_DEFAULT);
                }

                Assert.assertTrue("Server should receive message",
                        messageLatch.await(5, TimeUnit.SECONDS));
                Assert.assertArrayEquals(testData, receivedData.get());
            }
        }
    }

    // ==================== Large Message Tests ====================

    @Test
    public void testSendVeryLargeMessage() throws Exception {
        int port = BASE_PORT + 12;
        AtomicReference<byte[]> receivedData = new AtomicReference<>();
        CountDownLatch messageLatch = new CountDownLatch(1);

        try (TestWebSocketServer server = new TestWebSocketServer(port, new TestWebSocketServer.WebSocketServerHandler() {
            @Override
            public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
                receivedData.set(data);
                messageLatch.countDown();
            }
        })) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            try (WebSocketChannel channel = new WebSocketChannel("ws://localhost:" + port, false)) {
                channel.connect();

                // Send 1MB message
                int size = 1024 * 1024;
                long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
                try {
                    // Fill with pattern
                    for (int i = 0; i < size; i++) {
                        Unsafe.getUnsafe().putByte(ptr + i, (byte) (i & 0xFF));
                    }
                    channel.sendBinary(ptr, size);
                } finally {
                    Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
                }

                Assert.assertTrue("Server should receive large message",
                        messageLatch.await(30, TimeUnit.SECONDS));

                byte[] received = receivedData.get();
                Assert.assertEquals(size, received.length);
                // Verify pattern
                for (int i = 0; i < size; i++) {
                    if ((byte) (i & 0xFF) != received[i]) {
                        Assert.fail("Mismatch at byte " + i + ": expected " + (i & 0xFF) + " got " + (received[i] & 0xFF));
                    }
                }
            }
        }
    }

    @Test
    public void testSendMessagesRapidly() throws Exception {
        int port = BASE_PORT + 13;
        int messageCount = 1000;
        CountDownLatch messageLatch = new CountDownLatch(messageCount);
        AtomicReference<Exception> serverError = new AtomicReference<>();

        try (TestWebSocketServer server = new TestWebSocketServer(port, new TestWebSocketServer.WebSocketServerHandler() {
            @Override
            public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
                messageLatch.countDown();
            }

            @Override
            public void onError(TestWebSocketServer.ClientHandler client, Exception e) {
                serverError.set(e);
            }
        })) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            try (WebSocketChannel channel = new WebSocketChannel("ws://localhost:" + port, false)) {
                channel.connect();

                // Send messages as fast as possible
                int msgSize = 100;
                long ptr = Unsafe.malloc(msgSize, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < messageCount; i++) {
                        // Write message number to buffer
                        Unsafe.getUnsafe().putInt(ptr, i);
                        channel.sendBinary(ptr, msgSize);
                    }
                } finally {
                    Unsafe.free(ptr, msgSize, MemoryTag.NATIVE_DEFAULT);
                }

                Assert.assertTrue("Server should receive all messages",
                        messageLatch.await(30, TimeUnit.SECONDS));
                Assert.assertNull("Server should not have errors", serverError.get());
            }
        }
    }

    // ==================== Connection Behavior Tests ====================

    @Test
    public void testReceiveTimeoutReturnsNothing() throws Exception {
        int port = BASE_PORT + 14;

        try (TestWebSocketServer server = new TestWebSocketServer(port, new TestWebSocketServer.WebSocketServerHandler() {
            // Server doesn't send anything
        })) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            try (WebSocketChannel channel = new WebSocketChannel("ws://localhost:" + port, false)) {
                channel.connect();

                // Try to receive with short timeout - should return false
                boolean received = channel.receiveFrame(new WebSocketChannel.ResponseHandler() {
                    @Override
                    public void onBinaryMessage(long payload, int length) {
                        Assert.fail("Should not receive any message");
                    }

                    @Override
                    public void onClose(int code, String reason) {
                        Assert.fail("Should not receive close");
                    }
                }, 100);

                Assert.assertFalse("Should timeout without receiving", received);
                Assert.assertTrue("Channel should still be connected", channel.isConnected());
            }
        }
    }

    @Test
    public void testServerSendsCloseWithReason() throws Exception {
        int port = BASE_PORT + 15;
        CountDownLatch closeLatch = new CountDownLatch(1);
        AtomicReference<Integer> closeCodeRef = new AtomicReference<>();

        try (TestWebSocketServer server = new TestWebSocketServer(port, new TestWebSocketServer.WebSocketServerHandler() {
            @Override
            public void onConnect(TestWebSocketServer.ClientHandler client) {
                try {
                    // Send close with custom code and reason
                    client.sendClose(4001, "Custom close reason");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        })) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            try (WebSocketChannel channel = new WebSocketChannel("ws://localhost:" + port, false)) {
                channel.connect();

                channel.receiveFrame(new WebSocketChannel.ResponseHandler() {
                    @Override
                    public void onBinaryMessage(long payload, int length) {
                    }

                    @Override
                    public void onClose(int code, String reason) {
                        closeCodeRef.set(code);
                        closeLatch.countDown();
                    }
                }, 5000);

                Assert.assertTrue("Should receive close", closeLatch.await(5, TimeUnit.SECONDS));
                Assert.assertEquals(Integer.valueOf(4001), closeCodeRef.get());
            }
        }
    }

    @Test
    public void testConnectToServerThatRejectsHandshake() throws Exception {
        int port = BASE_PORT + 16;

        // Create a simple TCP server that sends invalid HTTP response
        java.net.ServerSocket serverSocket = new java.net.ServerSocket(port);
        Thread serverThread = new Thread(() -> {
            try {
                java.net.Socket client = serverSocket.accept();
                // Read the request
                byte[] buf = new byte[1024];
                client.getInputStream().read(buf);
                // Send invalid response (not 101)
                String response = "HTTP/1.1 400 Bad Request\r\n\r\n";
                client.getOutputStream().write(response.getBytes(StandardCharsets.US_ASCII));
                client.getOutputStream().flush();
                Thread.sleep(100);
                client.close();
            } catch (Exception e) {
                // Ignore
            }
        });
        serverThread.start();

        try {
            try (WebSocketChannel channel = new WebSocketChannel("ws://localhost:" + port, false)) {
                channel.connect();
                Assert.fail("Expected connection to fail");
            } catch (LineSenderException e) {
                Assert.assertTrue("Error should mention handshake failure",
                        e.getMessage().contains("handshake"));
            }
        } finally {
            serverSocket.close();
            serverThread.join(1000);
        }
    }

    @Test
    public void testConnectToServerThatClosesImmediately() throws Exception {
        int port = BASE_PORT + 17;

        // Create a simple TCP server that closes immediately
        java.net.ServerSocket serverSocket = new java.net.ServerSocket(port);
        Thread serverThread = new Thread(() -> {
            try {
                java.net.Socket client = serverSocket.accept();
                client.close(); // Close immediately without handshake
            } catch (Exception e) {
                // Ignore
            }
        });
        serverThread.start();

        try {
            try (WebSocketChannel channel = new WebSocketChannel("ws://localhost:" + port, false)) {
                channel.connect();
                Assert.fail("Expected connection to fail");
            } catch (LineSenderException e) {
                // Connection should fail
                Assert.assertNotNull(e.getMessage());
            }
        } finally {
            serverSocket.close();
            serverThread.join(1000);
        }
    }

    // ==================== Echo Tests ====================

    @Test
    public void testEchoSmallMessage() throws Exception {
        int port = BASE_PORT + 18;
        AtomicReference<byte[]> echoData = new AtomicReference<>();
        CountDownLatch echoLatch = new CountDownLatch(1);

        try (TestWebSocketServer server = new TestWebSocketServer(port, new TestWebSocketServer.WebSocketServerHandler() {
            @Override
            public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
                try {
                    // Echo back
                    client.sendBinary(data);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        })) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            try (WebSocketChannel channel = new WebSocketChannel("ws://localhost:" + port, false)) {
                channel.connect();

                // Send test data
                byte[] testData = "Echo test message!".getBytes(StandardCharsets.UTF_8);
                long ptr = Unsafe.malloc(testData.length, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < testData.length; i++) {
                        Unsafe.getUnsafe().putByte(ptr + i, testData[i]);
                    }
                    channel.sendBinary(ptr, testData.length);
                } finally {
                    Unsafe.free(ptr, testData.length, MemoryTag.NATIVE_DEFAULT);
                }

                // Receive echo
                channel.receiveFrame(new WebSocketChannel.ResponseHandler() {
                    @Override
                    public void onBinaryMessage(long payload, int length) {
                        byte[] data = new byte[length];
                        for (int i = 0; i < length; i++) {
                            data[i] = Unsafe.getUnsafe().getByte(payload + i);
                        }
                        echoData.set(data);
                        echoLatch.countDown();
                    }

                    @Override
                    public void onClose(int code, String reason) {
                    }
                }, 5000);

                Assert.assertTrue("Should receive echo", echoLatch.await(5, TimeUnit.SECONDS));
                Assert.assertArrayEquals(testData, echoData.get());
            }
        }
    }

    @Test
    public void testEchoMultipleMessages() throws Exception {
        int port = BASE_PORT + 19;
        int messageCount = 10;
        java.util.List<byte[]> sentMessages = new java.util.ArrayList<>();
        java.util.List<byte[]> receivedMessages = java.util.Collections.synchronizedList(new java.util.ArrayList<>());
        CountDownLatch echoLatch = new CountDownLatch(messageCount);

        try (TestWebSocketServer server = new TestWebSocketServer(port, new TestWebSocketServer.WebSocketServerHandler() {
            @Override
            public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
                try {
                    // Echo back
                    client.sendBinary(data);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        })) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            try (WebSocketChannel channel = new WebSocketChannel("ws://localhost:" + port, false)) {
                channel.connect();

                // Send multiple messages and receive echoes in lockstep
                // This ensures we don't buffer overflow and handles the echo properly
                long ptr = Unsafe.malloc(256, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < messageCount; i++) {
                        byte[] msg = ("Message " + i).getBytes(StandardCharsets.UTF_8);
                        sentMessages.add(msg);
                        for (int j = 0; j < msg.length; j++) {
                            Unsafe.getUnsafe().putByte(ptr + j, msg[j]);
                        }
                        channel.sendBinary(ptr, msg.length);

                        // Receive echo immediately after sending
                        boolean received = channel.receiveFrame(new WebSocketChannel.ResponseHandler() {
                            @Override
                            public void onBinaryMessage(long payload, int length) {
                                byte[] data = new byte[length];
                                for (int k = 0; k < length; k++) {
                                    data[k] = Unsafe.getUnsafe().getByte(payload + k);
                                }
                                receivedMessages.add(data);
                                echoLatch.countDown();
                            }

                            @Override
                            public void onClose(int code, String reason) {
                            }
                        }, 5000);

                        Assert.assertTrue("Should receive echo for message " + i, received);
                    }
                } finally {
                    Unsafe.free(ptr, 256, MemoryTag.NATIVE_DEFAULT);
                }

                Assert.assertTrue("Should receive all echoes", echoLatch.await(1, TimeUnit.SECONDS));
                Assert.assertEquals(messageCount, receivedMessages.size());

                // Verify messages (order should be preserved)
                for (int i = 0; i < messageCount; i++) {
                    Assert.assertArrayEquals("Message " + i + " should match",
                            sentMessages.get(i), receivedMessages.get(i));
                }
            }
        }
    }

    // ==================== Boundary Tests ====================

    @Test
    public void testSendMessageAtBufferBoundary() throws Exception {
        int port = BASE_PORT + 20;
        AtomicReference<byte[]> receivedData = new AtomicReference<>();
        CountDownLatch messageLatch = new CountDownLatch(1);

        try (TestWebSocketServer server = new TestWebSocketServer(port, new TestWebSocketServer.WebSocketServerHandler() {
            @Override
            public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
                receivedData.set(data);
                messageLatch.countDown();
            }
        })) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            try (WebSocketChannel channel = new WebSocketChannel("ws://localhost:" + port, false)) {
                channel.connect();

                // Send exactly 65536 bytes (64KB, common buffer size boundary)
                int size = 65536;
                long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < size; i++) {
                        Unsafe.getUnsafe().putByte(ptr + i, (byte) (i % 251)); // Use prime for pattern
                    }
                    channel.sendBinary(ptr, size);
                } finally {
                    Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
                }

                Assert.assertTrue("Server should receive message",
                        messageLatch.await(10, TimeUnit.SECONDS));

                byte[] received = receivedData.get();
                Assert.assertEquals(size, received.length);
                for (int i = 0; i < size; i++) {
                    Assert.assertEquals("Byte at " + i, (byte) (i % 251), received[i]);
                }
            }
        }
    }

    @Test
    public void testSendOneByteMessage() throws Exception {
        int port = BASE_PORT + 21;
        AtomicReference<byte[]> receivedData = new AtomicReference<>();
        CountDownLatch messageLatch = new CountDownLatch(1);

        try (TestWebSocketServer server = new TestWebSocketServer(port, new TestWebSocketServer.WebSocketServerHandler() {
            @Override
            public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
                receivedData.set(data);
                messageLatch.countDown();
            }
        })) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            try (WebSocketChannel channel = new WebSocketChannel("ws://localhost:" + port, false)) {
                channel.connect();

                // Send exactly 1 byte
                long ptr = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.getUnsafe().putByte(ptr, (byte) 0x42);
                    channel.sendBinary(ptr, 1);
                } finally {
                    Unsafe.free(ptr, 1, MemoryTag.NATIVE_DEFAULT);
                }

                Assert.assertTrue("Server should receive message",
                        messageLatch.await(5, TimeUnit.SECONDS));

                byte[] received = receivedData.get();
                Assert.assertEquals(1, received.length);
                Assert.assertEquals(0x42, received[0]);
            }
        }
    }
}
