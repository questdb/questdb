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

import io.questdb.cutlass.qwp.server.WebSocketConnectionContext;
import io.questdb.cutlass.qwp.websocket.WebSocketCloseCode;
import io.questdb.cutlass.qwp.websocket.WebSocketOpcode;
import io.questdb.cutlass.qwp.websocket.WebSocketProcessor;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Comprehensive tests for WebSocket connection context.
 * Tests cover state management, receive handling, send handling,
 * close handshake, buffer management, and resource cleanup.
 */
public class WebSocketConnectionContextTest extends AbstractWebSocketTest {

    // ==================== STATE MANAGEMENT TESTS ====================

    @Test
    public void testBufferCleanupOnClose() {
        long memBefore = Unsafe.getMemUsed();

        WebSocketConnectionContext ctx = createContext();
        // Use the context
        byte[] frame = createMaskedFrame(WebSocketOpcode.BINARY, new byte[1000]);
        ctx.getRecvBuffer().write(frame);

        ctx.close();

        long memAfter = Unsafe.getMemUsed();

        // Memory should be fully released
        Assert.assertEquals(memBefore, memAfter);
    }

    @Test
    public void testClearResetsCloseState() {
        try (WebSocketConnectionContext ctx = createContext()) {
            ctx.initiateClose(WebSocketCloseCode.NORMAL_CLOSURE, "Test");
            Assert.assertTrue(ctx.isClosing());

            ctx.clear();

            Assert.assertFalse(ctx.isClosing());
            Assert.assertEquals(WebSocketConnectionContext.STATE_OPEN, ctx.getState());
        }
    }

    @Test
    public void testClearResetsFragmentState() {
        try (WebSocketConnectionContext ctx = createContext()) {
            MockWebSocketProcessor processor = new MockWebSocketProcessor();
            // Start fragmented message
            byte[] frag = createMaskedFrame(WebSocketOpcode.BINARY, new byte[]{1, 2}, false);
            ctx.getRecvBuffer().write(frag);
            ctx.handleRead(processor);

            // Clear should reset fragment state
            ctx.clear();

            // New message should work
            byte[] frame = createMaskedFrame(WebSocketOpcode.BINARY, new byte[]{3, 4}, true);
            ctx.getRecvBuffer().write(frame);
            ctx.handleRead(processor);

            Assert.assertEquals(1, processor.binaryMessages.size());
        }
    }

    @Test
    public void testCloseHandshakeClientInitiated() {
        try (WebSocketConnectionContext ctx = createContext()) {
            MockWebSocketProcessor processor = new MockWebSocketProcessor();
            // Client sends close
            byte[] closeFrame = createMaskedFrame(WebSocketOpcode.CLOSE, new byte[]{0x03, (byte) 0xE8}); // code 1000
            ctx.getRecvBuffer().write(closeFrame);
            ctx.handleRead(processor);

            Assert.assertTrue(ctx.hasPendingCloseResponse());
            Assert.assertEquals(WebSocketConnectionContext.STATE_CLOSING, ctx.getState());
            Assert.assertEquals(1, processor.closeCount);
            Assert.assertEquals(1000, processor.lastCloseCode);
        }
    }

    @Test
    public void testCloseHandshakeServerInitiated() {
        try (WebSocketConnectionContext ctx = createContext()) {
            // Server initiates close
            ctx.initiateClose(WebSocketCloseCode.GOING_AWAY, "Going away");

            Assert.assertTrue(ctx.isCloseFrameSent());
            Assert.assertEquals(WebSocketConnectionContext.STATE_CLOSING, ctx.getState());

            // Client responds with close
            ctx.onCloseFrameReceived(WebSocketCloseCode.GOING_AWAY);

            Assert.assertEquals(WebSocketConnectionContext.STATE_CLOSED, ctx.getState());
        }
    }

    // ==================== RECEIVE HANDLING TESTS ====================

    @Test
    public void testCloseWithEmptyPayload() {
        try (WebSocketConnectionContext ctx = createContext()) {
            MockWebSocketProcessor processor = new MockWebSocketProcessor();
            // Close frame with no body is valid
            byte[] closeFrame = createMaskedFrame(WebSocketOpcode.CLOSE, new byte[0]);
            ctx.getRecvBuffer().write(closeFrame);
            ctx.handleRead(processor);

            Assert.assertEquals(1, processor.closeCount);
            Assert.assertEquals(-1, processor.lastCloseCode);  // No code provided
        }
    }

    @Test
    public void testCloseWithReason() {
        try (WebSocketConnectionContext ctx = createContext()) {
            MockWebSocketProcessor processor = new MockWebSocketProcessor();
            String reason = "Test reason";
            byte[] reasonBytes = reason.getBytes();
            byte[] payload = new byte[2 + reasonBytes.length];
            payload[0] = 0x03;  // 1000 >> 8
            payload[1] = (byte) 0xE8;  // 1000 & 0xFF
            System.arraycopy(reasonBytes, 0, payload, 2, reasonBytes.length);

            byte[] closeFrame = createMaskedFrame(WebSocketOpcode.CLOSE, payload);
            ctx.getRecvBuffer().write(closeFrame);
            ctx.handleRead(processor);

            Assert.assertEquals(1, processor.closeCount);
            Assert.assertEquals(1000, processor.lastCloseCode);
            Assert.assertEquals(reason, processor.lastCloseReason);
        }
    }

    @Test
    public void testContextClear() {
        try (WebSocketConnectionContext ctx = createContext()) {
            MockWebSocketProcessor processor = new MockWebSocketProcessor();
            // First use
            byte[] frame1 = createMaskedFrame(WebSocketOpcode.BINARY, new byte[]{1, 2});
            ctx.getRecvBuffer().write(frame1);
            ctx.handleRead(processor);
            Assert.assertEquals(1, processor.binaryMessages.size());

            // Clear for reuse
            ctx.clear();
            processor.clear();

            // Second use
            byte[] frame2 = createMaskedFrame(WebSocketOpcode.BINARY, new byte[]{3, 4});
            ctx.getRecvBuffer().write(frame2);
            ctx.handleRead(processor);
            Assert.assertEquals(1, processor.binaryMessages.size());
            Assert.assertArrayEquals(new byte[]{3, 4}, processor.binaryMessages.get(0));
        }
    }

    @Test
    public void testDefaultMaxMessageSize() {
        try (WebSocketConnectionContext ctx = createContext()) {
            // Default should be a reasonable value
            Assert.assertTrue(ctx.getMaxMessageSize() > 0);
        }
    }

    @Test
    public void testHasPendingSend() {
        try (WebSocketConnectionContext ctx = createContext()) {
            Assert.assertFalse(ctx.hasPendingSend());

            ctx.sendBinaryFrame(new byte[]{1, 2, 3}, 0, 3);

            Assert.assertTrue(ctx.hasPendingSend());
        }
    }

    @Test
    public void testInitialState() {
        try (WebSocketConnectionContext ctx = createContext()) {
            Assert.assertEquals(WebSocketConnectionContext.STATE_OPEN, ctx.getState());
            Assert.assertFalse(ctx.isClosing());
            Assert.assertFalse(ctx.isClosed());
        }
    }

    @Test
    public void testMultiplePingsGenerateMultiplePongs() {
        try (WebSocketConnectionContext ctx = createContext()) {
            MockWebSocketProcessor processor = new MockWebSocketProcessor();
            // Send multiple pings in sequence
            byte[] ping1 = createMaskedFrame(WebSocketOpcode.PING, new byte[]{1});
            byte[] ping2 = createMaskedFrame(WebSocketOpcode.PING, new byte[]{2});
            byte[] ping3 = createMaskedFrame(WebSocketOpcode.PING, new byte[]{3});

            ctx.getRecvBuffer().write(ping1);
            ctx.getRecvBuffer().write(ping2);
            ctx.getRecvBuffer().write(ping3);
            ctx.handleRead(processor);

            // All three pings should be processed
            Assert.assertEquals(3, processor.pingCount);

            // All three pongs should be in send buffer
            Assert.assertTrue(ctx.hasPendingSend());

            // Verify we have 3 pong frames in the buffer (each pong is 3 bytes: header + 1 byte payload)
            byte[] sendData = ctx.getSendBuffer().toByteArray();
            Assert.assertTrue("Expected at least 9 bytes for 3 pong frames", sendData.length >= 9);
        }
    }

    // ==================== BUFFER MANAGEMENT TESTS ====================

    @Test
    public void testPingDuringClose() {
        try (WebSocketConnectionContext ctx = createContext()) {
            MockWebSocketProcessor processor = new MockWebSocketProcessor();
            ctx.onCloseFrameReceived(WebSocketCloseCode.NORMAL_CLOSURE);

            // Ping during close handshake should still be handled
            byte[] ping = createMaskedFrame(WebSocketOpcode.PING, new byte[]{1, 2});
            ctx.getRecvBuffer().write(ping);
            ctx.handleRead(processor);

            // Pong is sent immediately to send buffer
            Assert.assertTrue(ctx.hasPendingSend());
        }
    }

    @Test
    public void testPongSentImmediatelyAfterPing() {
        try (WebSocketConnectionContext ctx = createContext()) {
            MockWebSocketProcessor processor = new MockWebSocketProcessor();
            Assert.assertFalse(ctx.hasPendingSend());

            byte[] ping = createMaskedFrame(WebSocketOpcode.PING, new byte[]{1, 2, 3, 4});
            ctx.getRecvBuffer().write(ping);
            ctx.handleRead(processor);

            // Pong should be immediately in send buffer
            Assert.assertTrue(ctx.hasPendingSend());
        }
    }

    @Test
    public void testReceiveBufferGrowth() {
        // Small initial buffer
        try (WebSocketConnectionContext ctx = createContext(256)) {
            MockWebSocketProcessor processor = new MockWebSocketProcessor();
            // Send message larger than initial buffer
            byte[] largePayload = new byte[1024];
            for (int i = 0; i < largePayload.length; i++) {
                largePayload[i] = (byte) (i & 0xFF);
            }

            byte[] frame = createMaskedFrame(WebSocketOpcode.BINARY, largePayload);
            ctx.getRecvBuffer().write(frame);
            ctx.handleRead(processor);

            Assert.assertEquals(1, processor.binaryMessages.size());
            Assert.assertArrayEquals(largePayload, processor.binaryMessages.get(0));
        }
    }

    @Test
    public void testReceiveCloseAfterCloseSent() {
        try (WebSocketConnectionContext ctx = createContext()) {
            MockWebSocketProcessor processor = new MockWebSocketProcessor();
            ctx.initiateClose(WebSocketCloseCode.NORMAL_CLOSURE, "Normal");

            // Receive close response
            byte[] closeFrame = createMaskedFrame(WebSocketOpcode.CLOSE, new byte[]{0x03, (byte) 0xE8});
            ctx.getRecvBuffer().write(closeFrame);
            ctx.handleRead(processor);

            Assert.assertEquals(WebSocketConnectionContext.STATE_CLOSED, ctx.getState());
        }
    }

    // ==================== SEND HANDLING TESTS ====================

    @Test
    public void testReceiveDataAfterCloseSent() {
        try (WebSocketConnectionContext ctx = createContext()) {
            MockWebSocketProcessor processor = new MockWebSocketProcessor();
            ctx.initiateClose(WebSocketCloseCode.NORMAL_CLOSURE, "Normal");

            // Receive data after close sent (should be ignored per RFC)
            byte[] frame = createMaskedFrame(WebSocketOpcode.BINARY, new byte[]{1, 2, 3});
            ctx.getRecvBuffer().write(frame);
            ctx.handleRead(processor);

            // Data messages should be ignored during close
            Assert.assertEquals(0, processor.binaryMessages.size());
        }
    }

    @Test
    public void testReceiveEmptyFrame() {
        try (WebSocketConnectionContext ctx = createContext()) {
            MockWebSocketProcessor processor = new MockWebSocketProcessor();
            byte[] frame = createMaskedFrame(WebSocketOpcode.BINARY, new byte[0]);
            ctx.getRecvBuffer().write(frame);
            ctx.handleRead(processor);

            Assert.assertEquals(1, processor.binaryMessages.size());
            Assert.assertEquals(0, processor.binaryMessages.get(0).length);
        }
    }

    @Test
    public void testReceiveFragmentedMessage() {
        try (WebSocketConnectionContext ctx = createContext()) {
            MockWebSocketProcessor processor = new MockWebSocketProcessor();
            // Fragment 1: BINARY, FIN=0
            byte[] frag1 = createMaskedFrame(WebSocketOpcode.BINARY, new byte[]{1, 2, 3}, false);
            ctx.getRecvBuffer().write(frag1);
            ctx.handleRead(processor);

            Assert.assertEquals(0, processor.binaryMessages.size());  // Not complete yet

            // Fragment 2: CONTINUATION, FIN=0
            byte[] frag2 = createMaskedFrame(WebSocketOpcode.CONTINUATION, new byte[]{4, 5, 6}, false);
            ctx.getRecvBuffer().write(frag2);
            ctx.handleRead(processor);

            Assert.assertEquals(0, processor.binaryMessages.size());

            // Fragment 3: CONTINUATION, FIN=1
            byte[] frag3 = createMaskedFrame(WebSocketOpcode.CONTINUATION, new byte[]{7, 8, 9}, true);
            ctx.getRecvBuffer().write(frag3);
            ctx.handleRead(processor);

            Assert.assertEquals(1, processor.binaryMessages.size());
            Assert.assertArrayEquals(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9},
                    processor.binaryMessages.get(0));
        }
    }

    @Test
    public void testReceiveMultipleFramesInOneRead() {
        try (WebSocketConnectionContext ctx = createContext()) {
            MockWebSocketProcessor processor = new MockWebSocketProcessor();
            byte[] frame1 = createMaskedFrame(WebSocketOpcode.BINARY, new byte[]{1, 2});
            byte[] frame2 = createMaskedFrame(WebSocketOpcode.BINARY, new byte[]{3, 4});

            ctx.getRecvBuffer().write(frame1);
            ctx.getRecvBuffer().write(frame2);
            ctx.handleRead(processor);

            Assert.assertEquals(2, processor.binaryMessages.size());
            Assert.assertArrayEquals(new byte[]{1, 2}, processor.binaryMessages.get(0));
            Assert.assertArrayEquals(new byte[]{3, 4}, processor.binaryMessages.get(1));
        }
    }

    @Test
    public void testReceivePartialFrame() {
        try (WebSocketConnectionContext ctx = createContext()) {
            MockWebSocketProcessor processor = new MockWebSocketProcessor();
            byte[] payload = new byte[50];
            for (int i = 0; i < payload.length; i++) {
                payload[i] = (byte) i;
            }
            byte[] frame = createMaskedFrame(WebSocketOpcode.BINARY, payload);

            // Write only first 30 bytes
            ctx.getRecvBuffer().write(frame, 0, 30);
            ctx.handleRead(processor);

            Assert.assertEquals(0, processor.binaryMessages.size());  // Incomplete

            // Write remaining bytes
            ctx.getRecvBuffer().write(frame, 30, frame.length - 30);
            ctx.handleRead(processor);

            Assert.assertEquals(1, processor.binaryMessages.size());
            Assert.assertArrayEquals(payload, processor.binaryMessages.get(0));
        }
    }

    @Test
    public void testReceivePingDuringFragment() {
        try (WebSocketConnectionContext ctx = createContext()) {
            MockWebSocketProcessor processor = new MockWebSocketProcessor();
            // Start fragmented message
            byte[] frag1 = createMaskedFrame(WebSocketOpcode.BINARY, new byte[]{1, 2}, false);
            ctx.getRecvBuffer().write(frag1);
            ctx.handleRead(processor);

            // Receive ping in the middle
            byte[] ping = createMaskedFrame(WebSocketOpcode.PING, new byte[]{(byte) 0xAA, (byte) 0xBB}, true);
            ctx.getRecvBuffer().write(ping);
            ctx.handleRead(processor);

            Assert.assertEquals(1, processor.pingCount);
            // Pong is sent immediately to send buffer (not buffered separately)
            Assert.assertTrue(ctx.hasPendingSend());

            // Complete fragmented message
            byte[] frag2 = createMaskedFrame(WebSocketOpcode.CONTINUATION, new byte[]{3, 4}, true);
            ctx.getRecvBuffer().write(frag2);
            ctx.handleRead(processor);

            Assert.assertEquals(1, processor.binaryMessages.size());
            Assert.assertArrayEquals(new byte[]{1, 2, 3, 4}, processor.binaryMessages.get(0));
        }
    }

    // ==================== CLOSE HANDLING EDGE CASES ====================

    @Test
    public void testReceivePongFrame() {
        try (WebSocketConnectionContext ctx = createContext()) {
            MockWebSocketProcessor processor = new MockWebSocketProcessor();
            byte[] pong = createMaskedFrame(WebSocketOpcode.PONG, new byte[]{1, 2, 3, 4});
            ctx.getRecvBuffer().write(pong);
            ctx.handleRead(processor);

            Assert.assertEquals(1, processor.pongCount);
        }
    }

    @Test
    public void testReceiveSingleBinaryFrame() {
        try (WebSocketConnectionContext ctx = createContext()) {
            MockWebSocketProcessor processor = new MockWebSocketProcessor();
            byte[] data = {0x01, 0x02, 0x03, 0x04, 0x05};
            byte[] frame = createMaskedFrame(WebSocketOpcode.BINARY, data);
            ctx.getRecvBuffer().write(frame);

            ctx.handleRead(processor);

            Assert.assertEquals(1, processor.binaryMessages.size());
            Assert.assertArrayEquals(data, processor.binaryMessages.get(0));
        }
    }

    @Test
    public void testReceiveSingleTextFrame() {
        try (WebSocketConnectionContext ctx = createContext()) {
            MockWebSocketProcessor processor = new MockWebSocketProcessor();
            byte[] data = "Hello".getBytes();
            byte[] frame = createMaskedFrame(WebSocketOpcode.TEXT, data);
            ctx.getRecvBuffer().write(frame);

            ctx.handleRead(processor);

            Assert.assertEquals(1, processor.textMessages.size());
            Assert.assertArrayEquals(data, processor.textMessages.get(0));
        }
    }

    @Test
    public void testRejectContinuationWithoutStart() {
        try (WebSocketConnectionContext ctx = createContext()) {
            MockWebSocketProcessor processor = new MockWebSocketProcessor();
            // Continuation frame without preceding data frame
            byte[] frame = createMaskedFrame(WebSocketOpcode.CONTINUATION, new byte[]{1, 2, 3}, true);
            ctx.getRecvBuffer().write(frame);
            ctx.handleRead(processor);

            Assert.assertTrue(processor.hasError);
            Assert.assertEquals(WebSocketCloseCode.PROTOCOL_ERROR, processor.lastErrorCode);
        }
    }

    @Test
    public void testRejectFragmentedControlFrame() {
        try (WebSocketConnectionContext ctx = createContext()) {
            MockWebSocketProcessor processor = new MockWebSocketProcessor();
            // Ping with FIN=0 (fragmented control frame - not allowed)
            byte[] mask = {0x12, 0x34, 0x56, 0x78};
            long buf = allocateBuffer(16);
            try {
                writeBytes(buf, (byte) 0x09, (byte) 0x80);  // PING, no FIN, masked, 0 length
                // Add mask key
                for (int i = 0; i < 4; i++) {
                    writeByte(buf, 2 + i, mask[i]);
                }
                byte[] frame = readBytes(buf, 6);
                ctx.getRecvBuffer().write(frame);
                ctx.handleRead(processor);

                Assert.assertTrue(processor.hasError);
                Assert.assertEquals(WebSocketCloseCode.PROTOCOL_ERROR, processor.lastErrorCode);
            } finally {
                freeBuffer(buf, 16);
            }
        }
    }

    // ==================== PROTOCOL ERROR TESTS ====================

    @Test
    public void testRejectNestedFragmentation() {
        try (WebSocketConnectionContext ctx = createContext()) {
            MockWebSocketProcessor processor = new MockWebSocketProcessor();
            // Start binary fragmented message
            byte[] frag1 = createMaskedFrame(WebSocketOpcode.BINARY, new byte[]{1, 2}, false);
            ctx.getRecvBuffer().write(frag1);
            ctx.handleRead(processor);

            // Try to start another fragmented message (not allowed)
            byte[] frag2 = createMaskedFrame(WebSocketOpcode.TEXT, new byte[]{3, 4}, false);
            ctx.getRecvBuffer().write(frag2);
            ctx.handleRead(processor);

            Assert.assertTrue(processor.hasError);
            Assert.assertEquals(WebSocketCloseCode.PROTOCOL_ERROR, processor.lastErrorCode);
        }
    }

    @Test
    public void testRejectOversizeFragmentedMessage() {
        WebSocketConnectionContext ctx = createContext();
        try (ctx) {
            ctx.setMaxMessageSize(100);
            MockWebSocketProcessor processor = new MockWebSocketProcessor();
            // Send fragments that together exceed limit
            byte[] frag1 = createMaskedFrame(WebSocketOpcode.BINARY, new byte[60], false);
            ctx.getRecvBuffer().write(frag1);
            ctx.handleRead(processor);

            byte[] frag2 = createMaskedFrame(WebSocketOpcode.CONTINUATION, new byte[60], true);  // Total 120 > 100
            ctx.getRecvBuffer().write(frag2);
            ctx.handleRead(processor);

            Assert.assertTrue(processor.hasError);
            Assert.assertEquals(WebSocketCloseCode.MESSAGE_TOO_BIG, processor.lastErrorCode);
        }
    }

    @Test
    public void testRejectOversizeMessage() {
        WebSocketConnectionContext ctx = createContext();
        try (ctx) {
            ctx.setMaxMessageSize(1024);
            MockWebSocketProcessor processor = new MockWebSocketProcessor();
            byte[] oversizePayload = new byte[2048];
            byte[] frame = createMaskedFrame(WebSocketOpcode.BINARY, oversizePayload);
            ctx.getRecvBuffer().write(frame);

            ctx.handleRead(processor);

            Assert.assertTrue(processor.hasError);
            Assert.assertEquals(WebSocketCloseCode.MESSAGE_TOO_BIG, processor.lastErrorCode);
        }
    }

    @Test
    public void testRejectReservedOpcode() {
        try (WebSocketConnectionContext ctx = createContext()) {
            MockWebSocketProcessor processor = new MockWebSocketProcessor();
            // Reserved opcode 3
            byte[] mask = {0x12, 0x34, 0x56, 0x78};
            long buf = allocateBuffer(16);
            try {
                writeBytes(buf, (byte) 0x83, (byte) 0x80);  // FIN + opcode 3, masked, 0 length
                for (int i = 0; i < 4; i++) {
                    writeByte(buf, 2 + i, mask[i]);
                }
                byte[] frame = readBytes(buf, 6);
                ctx.getRecvBuffer().write(frame);
                ctx.handleRead(processor);

                Assert.assertTrue(processor.hasError);
                Assert.assertEquals(WebSocketCloseCode.PROTOCOL_ERROR, processor.lastErrorCode);
            } finally {
                freeBuffer(buf, 16);
            }
        }
    }

    @Test
    public void testRejectUnmaskedClientFrame() {
        try (WebSocketConnectionContext ctx = createContext()) {
            MockWebSocketProcessor processor = new MockWebSocketProcessor();
            // Create unmasked frame (server mode expects masked frames)
            long buf = allocateBuffer(16);
            try {
                writeBytes(buf, (byte) 0x82, (byte) 0x01, (byte) 0xFF);
                byte[] unmaskedFrame = readBytes(buf, 3);
                ctx.getRecvBuffer().write(unmaskedFrame);
                ctx.handleRead(processor);

                Assert.assertTrue(processor.hasError);
                Assert.assertEquals(WebSocketCloseCode.PROTOCOL_ERROR, processor.lastErrorCode);
            } finally {
                freeBuffer(buf, 16);
            }
        }
    }

    // ==================== RESOURCE MANAGEMENT TESTS ====================

    @Test
    public void testSendBinaryFrame() {
        try (WebSocketConnectionContext ctx = createContext()) {
            byte[] data = {0x01, 0x02, 0x03, 0x04, 0x05};
            ctx.sendBinaryFrame(data, 0, data.length);

            byte[] sent = ctx.getSendBuffer().toByteArray();
            Assert.assertTrue(sent.length > data.length);  // Header added

            // Verify it's a valid binary frame
            Assert.assertEquals((byte) 0x82, sent[0]);  // FIN + BINARY
            Assert.assertEquals((byte) data.length, sent[1]);  // length
        }
    }

    @Test
    public void testSendCloseFrame() {
        try (WebSocketConnectionContext ctx = createContext()) {
            ctx.sendCloseFrame(WebSocketCloseCode.NORMAL_CLOSURE, "Normal closure");

            byte[] sent = ctx.getSendBuffer().toByteArray();
            Assert.assertEquals((byte) 0x88, sent[0]);  // FIN + CLOSE

            // Verify close code (big-endian)
            Assert.assertEquals((byte) 0x03, sent[2]);  // 1000 >> 8
            Assert.assertEquals((byte) 0xE8, sent[3]);  // 1000 & 0xFF
        }
    }

    @Test
    public void testSendLargeFrame() {
        try (WebSocketConnectionContext ctx = createContext()) {
            byte[] data = new byte[100000];
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte) (i & 0xFF);
            }

            ctx.sendBinaryFrame(data, 0, data.length);

            byte[] sent = ctx.getSendBuffer().toByteArray();
            // Large frame uses 8-byte extended length (10 byte header total)
            Assert.assertTrue(sent.length >= data.length + 10);
        }
    }

    @Test
    public void testSendPingFrame() {
        try (WebSocketConnectionContext ctx = createContext()) {
            byte[] pingData = {0x01, 0x02, 0x03, 0x04};
            ctx.sendPingFrame(pingData, 0, pingData.length);

            byte[] sent = ctx.getSendBuffer().toByteArray();
            Assert.assertEquals((byte) 0x89, sent[0]);  // FIN + PING
            Assert.assertEquals((byte) pingData.length, sent[1]);
        }
    }

    // ==================== PENDING OPERATIONS TESTS ====================

    @Test
    public void testSendPongFrame() {
        try (WebSocketConnectionContext ctx = createContext()) {
            byte[] pingData = {0x01, 0x02, 0x03, 0x04};
            ctx.sendPongFrame(pingData, 0, pingData.length);

            byte[] sent = ctx.getSendBuffer().toByteArray();
            Assert.assertEquals((byte) 0x8A, sent[0]);  // FIN + PONG
            Assert.assertEquals((byte) pingData.length, sent[1]);
        }
    }

    @Test
    public void testSendTextFrame() {
        try (WebSocketConnectionContext ctx = createContext()) {
            byte[] data = "Hello World".getBytes();
            ctx.sendTextFrame(data, 0, data.length);

            byte[] sent = ctx.getSendBuffer().toByteArray();
            Assert.assertTrue(sent.length > data.length);

            // Verify it's a valid text frame
            Assert.assertEquals((byte) 0x81, sent[0]);  // FIN + TEXT
        }
    }

    @Test
    public void testSetMaxMessageSize() {
        try (WebSocketConnectionContext ctx = createContext()) {
            ctx.setMaxMessageSize(1024);
            Assert.assertEquals(1024, ctx.getMaxMessageSize());

            ctx.setMaxMessageSize(0);  // Disable limit
            Assert.assertEquals(0, ctx.getMaxMessageSize());
        }
    }

    // ==================== CONFIGURATION TESTS ====================

    @Test
    public void testStateTransitionToClosed() {
        try (WebSocketConnectionContext ctx = createContext()) {
            ctx.initiateClose(WebSocketCloseCode.NORMAL_CLOSURE, "Normal");
            ctx.onCloseFrameReceived(WebSocketCloseCode.NORMAL_CLOSURE);

            Assert.assertEquals(WebSocketConnectionContext.STATE_CLOSED, ctx.getState());
            Assert.assertTrue(ctx.isClosed());
        }
    }

    @Test
    public void testStateTransitionToClosing() {
        try (WebSocketConnectionContext ctx = createContext()) {
            ctx.initiateClose(WebSocketCloseCode.NORMAL_CLOSURE, "Normal");

            Assert.assertEquals(WebSocketConnectionContext.STATE_CLOSING, ctx.getState());
            Assert.assertTrue(ctx.isClosing());
            Assert.assertFalse(ctx.isClosed());
        }
    }

    // ==================== HELPER METHODS ====================

    private WebSocketConnectionContext createContext() {
        return createContext(65536);
    }

    private WebSocketConnectionContext createContext(int bufferSize) {
        return new WebSocketConnectionContext(bufferSize);
    }

    /**
     * Mock processor that records all received messages and events.
     */
    private static class MockWebSocketProcessor implements WebSocketProcessor {
        final List<byte[]> binaryMessages = new ArrayList<>();
        final List<byte[]> textMessages = new ArrayList<>();
        int closeCount = 0;
        boolean hasError = false;
        int lastCloseCode = -1;
        String lastCloseReason = null;
        int lastErrorCode = 0;
        CharSequence lastErrorMessage = null;
        int pingCount = 0;
        int pongCount = 0;

        @Override
        public void onBinaryMessage(long payload, int length) {
            byte[] data = new byte[length];
            for (int i = 0; i < length; i++) {
                data[i] = Unsafe.getUnsafe().getByte(payload + i);
            }
            binaryMessages.add(data);
        }

        @Override
        public void onClose(int code, long reason, int reasonLength) {
            closeCount++;
            lastCloseCode = code;
            if (reasonLength > 0) {
                byte[] reasonBytes = new byte[reasonLength];
                for (int i = 0; i < reasonLength; i++) {
                    reasonBytes[i] = Unsafe.getUnsafe().getByte(reason + i);
                }
                lastCloseReason = new String(reasonBytes);
            }
        }

        @Override
        public void onError(int errorCode, CharSequence message) {
            hasError = true;
            lastErrorCode = errorCode;
            lastErrorMessage = message;
        }

        @Override
        public void onPing(long payload, int length) {
            pingCount++;
        }

        @Override
        public void onPong(long payload, int length) {
            pongCount++;
        }

        @Override
        public void onTextMessage(long payload, int length) {
            byte[] data = new byte[length];
            for (int i = 0; i < length; i++) {
                data[i] = Unsafe.getUnsafe().getByte(payload + i);
            }
            textMessages.add(data);
        }

        void clear() {
            binaryMessages.clear();
            textMessages.clear();
            pingCount = 0;
            pongCount = 0;
            closeCount = 0;
            lastCloseCode = -1;
            lastCloseReason = null;
            hasError = false;
            lastErrorCode = 0;
            lastErrorMessage = null;
        }
    }
}
