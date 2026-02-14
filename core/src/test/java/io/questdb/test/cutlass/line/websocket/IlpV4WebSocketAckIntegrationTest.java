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

import io.questdb.client.cutlass.ilpv4.client.IlpV4WebSocketSender;
import io.questdb.client.cutlass.ilpv4.client.WebSocketResponse;
import io.questdb.cutlass.ilpv4.websocket.WebSocketCloseCode;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Os;
import io.questdb.test.cutlass.http.websocket.AbstractWebSocketTest;
import io.questdb.test.cutlass.http.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Integration tests for ILP v4 WebSocket ACK delivery mechanism.
 * These tests verify that the InFlightWindow and ACK responses work correctly end-to-end.
 */
public class IlpV4WebSocketAckIntegrationTest extends AbstractWebSocketTest {

    private static final Log LOG = LogFactory.getLog(IlpV4WebSocketAckIntegrationTest.class);
    private static final int TEST_PORT = 19500 + (int) (Os.currentTimeMicros() % 100);

    /**
     * Test that ACKs are properly sent and received for a single message.
     * Uses async mode to enable ACK handling via InFlightWindow.
     */
    @Test
    public void testSingleMessageAck() throws Exception {
        AckingServerHandler handler = new AckingServerHandler();

        try (TestWebSocketServer server = new TestWebSocketServer(TEST_PORT, handler)) {
            server.start();
            Assert.assertTrue("Server failed to start", server.awaitStart(5, TimeUnit.SECONDS));

            // Use async mode with no auto-flush (manual flush only)
            try (IlpV4WebSocketSender sender = IlpV4WebSocketSender.connectAsync(
                    "localhost", TEST_PORT, false, 0, 0, 0)) {

                // Send one row
                sender.table("test")
                        .longColumn("value", 42)
                        .atNow();

                // Flush and wait for ACK
                sender.flush();

                // Verify server received the message
                Assert.assertTrue("Server should receive message",
                        handler.awaitMessages(1, 5, TimeUnit.SECONDS));
                Assert.assertEquals("Should receive 1 message", 1, handler.getMessageCount());

                // Verify ACK was sent
                Assert.assertEquals("Should send 1 ACK", 1, handler.getAckCount());
            }
        }
    }

    /**
     * Test that multiple messages are all ACKed correctly.
     * Uses async mode to enable ACK handling via InFlightWindow.
     */
    @Test
    public void testMultipleMessagesAck() throws Exception {
        AckingServerHandler handler = new AckingServerHandler();

        try (TestWebSocketServer server = new TestWebSocketServer(TEST_PORT + 1, handler)) {
            server.start();
            Assert.assertTrue("Server failed to start", server.awaitStart(5, TimeUnit.SECONDS));

            // Use async mode with no auto-flush (manual flush only)
            try (IlpV4WebSocketSender sender = IlpV4WebSocketSender.connectAsync(
                    "localhost", TEST_PORT + 1, false, 0, 0, 0)) {

                // Send multiple rows
                int numRows = 10;
                for (int i = 0; i < numRows; i++) {
                    sender.table("test")
                            .longColumn("value", i)
                            .at(i * 1000L, ChronoUnit.MICROS);
                }

                // Flush and wait for all ACKs
                sender.flush();

                // Verify server received all messages
                Assert.assertTrue("Server should receive messages",
                        handler.awaitMessages(1, 5, TimeUnit.SECONDS)); // At least 1 batch

                // Verify ACKs were sent
                Assert.assertTrue("Should send at least 1 ACK", handler.getAckCount() >= 1);

                LOG.info().$("Received ").$(handler.getMessageCount()).$(" messages, sent ")
                        .$(handler.getAckCount()).$(" ACKs").$();
            }
        }
    }

    /**
     * Test that ACKs work with auto-flush triggered by row count.
     * Uses autoFlushRows=5 so we can send rows and verify the auto-flush triggers.
     */
    @Test
    public void testAutoFlushWithAck() throws Exception {
        SequenceTrackingHandler handler = new SequenceTrackingHandler();

        try (TestWebSocketServer server = new TestWebSocketServer(TEST_PORT + 2, handler)) {
            server.start();
            Assert.assertTrue("Server failed to start", server.awaitStart(5, TimeUnit.SECONDS));

            // Use async mode with autoFlushRows=5
            try (IlpV4WebSocketSender sender = IlpV4WebSocketSender.connectAsync(
                    "localhost", TEST_PORT + 2, false,
                    5, 0, 0)) { // autoFlushRows=5, no byte/interval limits

                // Send 10 rows - should trigger 2 auto-flushes (at row 5 and row 10)
                for (int i = 0; i < 10; i++) {
                    sender.table("test")
                            .longColumn("value", i)
                            .at(i * 1000L, ChronoUnit.MICROS);
                }

                // Final flush to ensure everything is sent and ACKed
                sender.flush();

                // Allow some time for server to process and send ACKs
                Thread.sleep(200);

                // Verify sequences were tracked
                LOG.info().$("Received ").$(handler.receivedSequences.size()).$(" sequences: ")
                        .$(handler.receivedSequences).$();

                // Should have received at least 2 batches (2 auto-flushes)
                Assert.assertTrue("Should receive at least 2 batches", handler.receivedSequences.size() >= 2);
                Assert.assertEquals("ACK count should match sequence count",
                        handler.receivedSequences.size(), handler.getAckCount());
            }
        }
    }

    /**
     * Test error response handling.
     * Uses async mode to enable ACK/error response handling via InFlightWindow.
     */
    @Test
    public void testErrorResponse() throws Exception {
        ErroringServerHandler handler = new ErroringServerHandler(0); // Error on 1st message

        try (TestWebSocketServer server = new TestWebSocketServer(TEST_PORT + 3, handler)) {
            server.start();
            Assert.assertTrue("Server failed to start", server.awaitStart(5, TimeUnit.SECONDS));

            boolean errorCaught = false;
            // Use async mode with no auto-flush (manual flush only)
            try (IlpV4WebSocketSender sender = IlpV4WebSocketSender.connectAsync(
                    "localhost", TEST_PORT + 3, false, 0, 0, 0)) {

                // Send a row - should fail on server side
                sender.table("test")
                        .longColumn("value", 42)
                        .atNow();

                sender.flush();
            } catch (Exception e) {
                errorCaught = true;
                LOG.info().$("Expected error caught: ").$(e.getMessage()).$();
                Assert.assertTrue("Error should mention server error or failed batch",
                        e.getMessage().contains("Server error") ||
                        e.getMessage().contains("failed") ||
                        e.getMessage().contains("PARSE_ERROR") ||
                        e.getMessage().contains("Batch"));
            }

            Assert.assertTrue("Should have caught an error", errorCaught);
        }
    }

    /**
     * Test that flush blocks until ACK is received.
     * Uses async mode to enable ACK handling via InFlightWindow.
     */
    @Test
    public void testFlushBlocksUntilAcked() throws Exception {
        final long DELAY_MS = 300; // 300ms delay before ACK
        DelayedAckHandler handler = new DelayedAckHandler(DELAY_MS);

        // Use a unique port offset based on test method to avoid conflicts
        int port = TEST_PORT + 10;
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue("Server failed to start", server.awaitStart(5, TimeUnit.SECONDS));

            // Use async mode with no auto-flush (manual flush only)
            try (IlpV4WebSocketSender sender = IlpV4WebSocketSender.connectAsync(
                    "localhost", port, false, 0, 0, 0)) {

                // Send a message
                sender.table("test")
                        .longColumn("value", 42)
                        .atNow();

                // Measure flush time - should block until ACK
                long startTime = System.currentTimeMillis();
                sender.flush();
                long duration = System.currentTimeMillis() - startTime;

                // Flush should have waited for the delayed ACK (allow 50% margin)
                Assert.assertTrue("Flush should have waited for ACK (took " + duration + "ms, expected >= " + (DELAY_MS / 2) + "ms)",
                        duration >= DELAY_MS / 2);

                LOG.info().$("Flush waited ").$(duration).$("ms for ACK").$();
            }
        }
    }

    /**
     * Test multiple sequential flushes with ACKs.
     * Uses async mode to enable ACK handling via InFlightWindow.
     */
    @Test
    public void testMultipleFlushesWithAcks() throws Exception {
        AckingServerHandler handler = new AckingServerHandler();

        try (TestWebSocketServer server = new TestWebSocketServer(TEST_PORT + 5, handler)) {
            server.start();
            Assert.assertTrue("Server failed to start", server.awaitStart(5, TimeUnit.SECONDS));

            // Use async mode with no auto-flush (manual flush only)
            try (IlpV4WebSocketSender sender = IlpV4WebSocketSender.connectAsync(
                    "localhost", TEST_PORT + 5, false, 0, 0, 0)) {

                int numFlushes = 3;
                for (int i = 0; i < numFlushes; i++) {
                    // Send data
                    sender.table("test")
                            .longColumn("batch", i)
                            .longColumn("value", i * 100)
                            .at(i * 1000000L, ChronoUnit.MICROS);

                    // Flush and wait for ACK
                    sender.flush();

                    LOG.debug().$("Completed flush ").$(i + 1).$();
                }

                // Verify all ACKs were received
                Assert.assertEquals("Should receive all ACKs", numFlushes, handler.getAckCount());
                LOG.info().$("All ").$(numFlushes).$(" flushes completed with ACKs").$();
            }
        }
    }

    @Test
    public void testAsyncFlushFailsFastOnServerClose() throws Exception {
        ClosingServerHandler handler = new ClosingServerHandler();
        int port = TEST_PORT + 20;

        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue("Server failed to start", server.awaitStart(5, TimeUnit.SECONDS));

            boolean errorCaught = false;
            long start = System.currentTimeMillis();
            try (IlpV4WebSocketSender sender = IlpV4WebSocketSender.connectAsync(
                    "localhost", port, false, 0, 0, 0)) {
                sender.table("test")
                        .longColumn("value", 1)
                        .atNow();
                sender.flush();
            } catch (Exception e) {
                errorCaught = true;
                Assert.assertTrue(
                        e.getMessage().contains("closed")
                                || e.getMessage().contains("Error in send queue")
                                || e.getMessage().contains("failed")
                );
            }

            long duration = System.currentTimeMillis() - start;
            Assert.assertTrue("Expected async close error", errorCaught);
            Assert.assertTrue("Flush should fail quickly on close [duration=" + duration + "ms]", duration < 10_000);
        }
    }

    @Test
    public void testAsyncFlushFailsFastOnInvalidAckPayload() throws Exception {
        InvalidAckPayloadHandler handler = new InvalidAckPayloadHandler();
        int port = TEST_PORT + 21;

        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue("Server failed to start", server.awaitStart(5, TimeUnit.SECONDS));

            boolean errorCaught = false;
            long start = System.currentTimeMillis();
            try (IlpV4WebSocketSender sender = IlpV4WebSocketSender.connectAsync(
                    "localhost", port, false, 0, 0, 0)) {
                sender.table("test")
                        .longColumn("value", 1)
                        .atNow();
                sender.flush();
            } catch (Exception e) {
                errorCaught = true;
                Assert.assertTrue(
                        e.getMessage().contains("Invalid ACK response payload")
                                || e.getMessage().contains("Error in send queue")
                );
            }

            long duration = System.currentTimeMillis() - start;
            Assert.assertTrue("Expected invalid ACK error", errorCaught);
            Assert.assertTrue("Flush should fail quickly on invalid ACK [duration=" + duration + "ms]", duration < 10_000);
        }
    }

    @Test
    public void testSyncFlushFailsOnInvalidAckPayload() throws Exception {
        InvalidAckPayloadHandler handler = new InvalidAckPayloadHandler();
        int port = TEST_PORT + 22;

        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue("Server failed to start", server.awaitStart(5, TimeUnit.SECONDS));

            boolean errorCaught = false;
            long start = System.currentTimeMillis();
            try (IlpV4WebSocketSender sender = IlpV4WebSocketSender.connect("localhost", port, false)) {
                sender.table("test")
                        .longColumn("value", 7)
                        .atNow();
                sender.flush();
            } catch (Exception e) {
                errorCaught = true;
                Assert.assertTrue(
                        e.getMessage().contains("Invalid ACK response payload")
                                || e.getMessage().contains("Failed to parse ACK response")
                );
            }

            long duration = System.currentTimeMillis() - start;
            Assert.assertTrue("Expected invalid ACK error in sync mode", errorCaught);
            Assert.assertTrue("Sync invalid ACK path should fail quickly [duration=" + duration + "ms]", duration < 10_000);
        }
    }

    @Test
    public void testSyncFlushIgnoresPingAndWaitsForAck() throws Exception {
        final long ackDelayMs = 300;
        PingThenDelayedAckHandler handler = new PingThenDelayedAckHandler(ackDelayMs);
        int port = TEST_PORT + 23;

        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue("Server failed to start", server.awaitStart(5, TimeUnit.SECONDS));

            try (IlpV4WebSocketSender sender = IlpV4WebSocketSender.connect("localhost", port, false)) {
                sender.table("test")
                        .longColumn("value", 11)
                        .atNow();

                long start = System.currentTimeMillis();
                sender.flush();
                long duration = System.currentTimeMillis() - start;

                Assert.assertTrue("Flush returned too early [duration=" + duration + "ms]", duration >= ackDelayMs / 2);
            }
        }
    }

    // ==================== SERVER HANDLERS ====================

    /**
     * Server handler that sends ACKs for each received message.
     */
    private static class AckingServerHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final AtomicInteger messageCount = new AtomicInteger(0);
        private final AtomicInteger ackCount = new AtomicInteger(0);
        private final AtomicLong nextSequence = new AtomicLong(0);
        private final CountDownLatch messageLatch = new CountDownLatch(1);

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            int count = messageCount.incrementAndGet();
            long sequence = nextSequence.getAndIncrement();

            LOG.debug().$("Server received message ").$(count).$(", sending ACK for seq ").$(sequence).$();

            // Send ACK response
            try {
                byte[] ackResponse = createAckResponse(sequence);
                client.sendBinary(ackResponse);
                ackCount.incrementAndGet();
            } catch (IOException e) {
                LOG.error().$("Failed to send ACK: ").$(e).$();
            }

            messageLatch.countDown();
        }

        public int getMessageCount() {
            return messageCount.get();
        }

        public int getAckCount() {
            return ackCount.get();
        }

        public boolean awaitMessages(int count, long timeout, TimeUnit unit) throws InterruptedException {
            return messageLatch.await(timeout, unit);
        }
    }

    /**
     * Server handler that tracks received sequence numbers.
     */
    private static class SequenceTrackingHandler implements TestWebSocketServer.WebSocketServerHandler {
        final CopyOnWriteArrayList<Long> receivedSequences = new CopyOnWriteArrayList<>();
        private final AtomicInteger ackCount = new AtomicInteger(0);
        private final AtomicLong nextSequence = new AtomicLong(0);

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            long sequence = nextSequence.getAndIncrement();
            receivedSequences.add(sequence);

            LOG.debug().$("Server tracking sequence ").$(sequence).$();

            // Send ACK response
            try {
                byte[] ackResponse = createAckResponse(sequence);
                client.sendBinary(ackResponse);
                ackCount.incrementAndGet();
            } catch (IOException e) {
                LOG.error().$("Failed to send ACK: ").$(e).$();
            }
        }

        public int getAckCount() {
            return ackCount.get();
        }
    }

    /**
     * Server handler that sends an error response for a specific message.
     */
    private static class ErroringServerHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final int errorOnMessage;
        private final AtomicInteger messageCount = new AtomicInteger(0);
        private final AtomicLong nextSequence = new AtomicLong(0);

        ErroringServerHandler(int errorOnMessage) {
            this.errorOnMessage = errorOnMessage;
        }

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            int count = messageCount.getAndIncrement();
            long sequence = nextSequence.getAndIncrement();

            try {
                if (count == errorOnMessage) {
                    LOG.info().$("Server sending ERROR for message ").$(count).$();
                    byte[] errorResponse = createErrorResponse(sequence,
                            WebSocketResponse.STATUS_PARSE_ERROR, "Test error");
                    client.sendBinary(errorResponse);
                } else {
                    LOG.debug().$("Server sending ACK for message ").$(count).$();
                    byte[] ackResponse = createAckResponse(sequence);
                    client.sendBinary(ackResponse);
                }
            } catch (IOException e) {
                LOG.error().$("Failed to send response: ").$(e).$();
            }
        }
    }

    /**
     * Server handler that delays ACKs to test blocking behavior.
     */
    private static class DelayedAckHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final long delayMs;
        private final AtomicLong nextSequence = new AtomicLong(0);

        DelayedAckHandler(long delayMs) {
            this.delayMs = delayMs;
        }

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            long sequence = nextSequence.getAndIncrement();

            LOG.debug().$("Server delaying ACK by ").$(delayMs).$("ms").$();

            // Delay before sending ACK
            new Thread(() -> {
                try {
                    Thread.sleep(delayMs);
                    byte[] ackResponse = createAckResponse(sequence);
                    client.sendBinary(ackResponse);
                    LOG.debug().$("Server sent delayed ACK for seq ").$(sequence).$();
                } catch (Exception e) {
                    LOG.error().$("Failed to send delayed ACK: ").$(e).$();
                }
            }).start();
        }
    }

    private static class ClosingServerHandler implements TestWebSocketServer.WebSocketServerHandler {
        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            try {
                client.sendClose(WebSocketCloseCode.GOING_AWAY, "bye");
            } catch (IOException e) {
                LOG.error().$("Failed to send close frame: ").$(e).$();
            }
        }
    }

    private static class InvalidAckPayloadHandler implements TestWebSocketServer.WebSocketServerHandler {
        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            try {
                client.sendBinary(new byte[]{1, 2, 3});
            } catch (IOException e) {
                LOG.error().$("Failed to send invalid payload: ").$(e).$();
            }
        }
    }

    private static class PingThenDelayedAckHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final long delayMs;
        private final AtomicLong nextSequence = new AtomicLong(0);

        private PingThenDelayedAckHandler(long delayMs) {
            this.delayMs = delayMs;
        }

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            long sequence = nextSequence.getAndIncrement();
            try {
                client.sendPing(new byte[]{42});
            } catch (IOException e) {
                LOG.error().$("Failed to send ping: ").$(e).$();
            }

            new Thread(() -> {
                try {
                    Thread.sleep(delayMs);
                    client.sendBinary(createAckResponse(sequence));
                } catch (Exception e) {
                    LOG.error().$("Failed to send delayed ACK: ").$(e).$();
                }
            }).start();
        }
    }

    // ==================== HELPER METHODS ====================

    /**
     * Creates a binary ACK response using WebSocketResponse format.
     * Format: status (1 byte) + sequence (8 bytes little-endian)
     */
    private static byte[] createAckResponse(long sequence) {
        byte[] response = new byte[WebSocketResponse.MIN_RESPONSE_SIZE];

        // Status OK (0)
        response[0] = WebSocketResponse.STATUS_OK;

        // Sequence (little-endian)
        response[1] = (byte) (sequence & 0xFF);
        response[2] = (byte) ((sequence >> 8) & 0xFF);
        response[3] = (byte) ((sequence >> 16) & 0xFF);
        response[4] = (byte) ((sequence >> 24) & 0xFF);
        response[5] = (byte) ((sequence >> 32) & 0xFF);
        response[6] = (byte) ((sequence >> 40) & 0xFF);
        response[7] = (byte) ((sequence >> 48) & 0xFF);
        response[8] = (byte) ((sequence >> 56) & 0xFF);

        return response;
    }

    /**
     * Creates a binary error response using WebSocketResponse format.
     * Format: status (1 byte) + sequence (8 bytes) + error length (2 bytes) + error message
     */
    private static byte[] createErrorResponse(long sequence, byte status, String errorMessage) {
        byte[] msgBytes = errorMessage.getBytes();
        byte[] response = new byte[WebSocketResponse.MIN_RESPONSE_SIZE + 2 + msgBytes.length];

        // Status
        response[0] = status;

        // Sequence (little-endian)
        response[1] = (byte) (sequence & 0xFF);
        response[2] = (byte) ((sequence >> 8) & 0xFF);
        response[3] = (byte) ((sequence >> 16) & 0xFF);
        response[4] = (byte) ((sequence >> 24) & 0xFF);
        response[5] = (byte) ((sequence >> 32) & 0xFF);
        response[6] = (byte) ((sequence >> 40) & 0xFF);
        response[7] = (byte) ((sequence >> 48) & 0xFF);
        response[8] = (byte) ((sequence >> 56) & 0xFF);

        // Error message length (little-endian)
        response[9] = (byte) (msgBytes.length & 0xFF);
        response[10] = (byte) ((msgBytes.length >> 8) & 0xFF);

        // Error message
        System.arraycopy(msgBytes, 0, response, 11, msgBytes.length);

        return response;
    }
}
