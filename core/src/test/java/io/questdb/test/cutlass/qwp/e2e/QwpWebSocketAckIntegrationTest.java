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

package io.questdb.test.cutlass.qwp.e2e;

import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.cutlass.qwp.websocket.WebSocketCloseCode;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Os;
import io.questdb.test.AbstractTest;
import io.questdb.test.cutlass.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Integration tests for QWP v1 WebSocket ACK delivery mechanism.
 * These tests verify that the InFlightWindow and ACK responses work correctly end-to-end.
 */
public class QwpWebSocketAckIntegrationTest extends AbstractTest {

    private static final Log LOG = LogFactory.getLog(QwpWebSocketAckIntegrationTest.class);
    private static final int TEST_PORT = 19500 + (int) (Os.currentTimeMicros() % 100);

    @Test
    public void testAsyncFlushFailsFastOnInvalidAckPayload() throws Exception {
        InvalidAckPayloadHandler handler = new InvalidAckPayloadHandler();
        int port = TEST_PORT + 21;

        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue("Server failed to start", server.awaitStart(5, TimeUnit.SECONDS));

            boolean errorCaught = false;
            long start = System.currentTimeMillis();
            try (QwpWebSocketSender sender = QwpWebSocketSender.connectAsync(
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
    public void testAsyncFlushFailsFastOnServerClose() throws Exception {
        ClosingServerHandler handler = new ClosingServerHandler();
        int port = TEST_PORT + 20;

        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue("Server failed to start", server.awaitStart(5, TimeUnit.SECONDS));

            boolean errorCaught = false;
            long start = System.currentTimeMillis();
            try (QwpWebSocketSender sender = QwpWebSocketSender.connectAsync(
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
            try (QwpWebSocketSender sender = QwpWebSocketSender.connectAsync(
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

    @Test
    public void testSyncFlushFailsOnInvalidAckPayload() throws Exception {
        InvalidAckPayloadHandler handler = new InvalidAckPayloadHandler();
        int port = TEST_PORT + 22;

        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue("Server failed to start", server.awaitStart(5, TimeUnit.SECONDS));

            boolean errorCaught = false;
            long start = System.currentTimeMillis();
            try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", port, false)) {
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

            try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", port, false)) {
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

}
