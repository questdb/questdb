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
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Integration tests for ILP v4 WebSocket processor and state working together.
 * These tests verify the complete flow from receiving WebSocket messages
 * to processing ILP v4 data.
 */
public class QwpWebSocketProcessorIntegrationTest extends AbstractWebSocketTest {

    // ==================== BASIC INTEGRATION TESTS ====================

    @Test
    public void testBytesProcessedNotResetOnClear() {
        try (QwpWebSocketProcessorState state = new QwpWebSocketProcessorState(1024)) {
            QwpWebSocketProcessor processor = new QwpWebSocketProcessor();
            processor.setCallback(new StateAccumulatingCallback(state));

            byte[] data = new byte[100];
            long ptr = allocateAndWrite(data);
            try {
                processor.onBinaryMessage(ptr, data.length);
                state.processMessage();
            } finally {
                freeBuffer(ptr, data.length);
            }
            Assert.assertEquals(100, state.getBytesProcessed());

            // Clear should not reset bytes processed
            state.clear();
            Assert.assertEquals(100, state.getBytesProcessed());
        }
    }

    @Test
    public void testBytesProcessedTracking() {
        try (QwpWebSocketProcessorState state = new QwpWebSocketProcessorState(1024)) {
            QwpWebSocketProcessor processor = new QwpWebSocketProcessor();
            processor.setCallback(new StateAccumulatingCallback(state));

            Assert.assertEquals(0, state.getBytesProcessed());

            // First batch
            byte[] data1 = new byte[100];
            long ptr1 = allocateAndWrite(data1);
            try {
                processor.onBinaryMessage(ptr1, data1.length);
                state.processMessage();
            } finally {
                freeBuffer(ptr1, data1.length);
            }
            Assert.assertEquals(100, state.getBytesProcessed());

            // Second batch
            byte[] data2 = new byte[150];
            long ptr2 = allocateAndWrite(data2);
            try {
                processor.onBinaryMessage(ptr2, data2.length);
                state.processMessage();
            } finally {
                freeBuffer(ptr2, data2.length);
            }
            Assert.assertEquals(250, state.getBytesProcessed());

            // Third batch
            byte[] data3 = new byte[75];
            long ptr3 = allocateAndWrite(data3);
            try {
                processor.onBinaryMessage(ptr3, data3.length);
                state.processMessage();
            } finally {
                freeBuffer(ptr3, data3.length);
            }
            Assert.assertEquals(325, state.getBytesProcessed());
        }
    }

    @Test
    public void testCloseCallbackWithState() {
        try (QwpWebSocketProcessorState ignored = new QwpWebSocketProcessorState(1024)) {
            QwpWebSocketProcessor processor = new QwpWebSocketProcessor();
            CloseTrackingCallback callback = new CloseTrackingCallback();
            processor.setCallback(callback);

            processor.onClose(WebSocketCloseCode.NORMAL_CLOSURE, 0, 0);

            Assert.assertTrue(callback.closeCalled);
            Assert.assertEquals(WebSocketCloseCode.NORMAL_CLOSURE, callback.closeCode);
        }
    }

    // ==================== ERROR HANDLING TESTS ====================

    @Test
    public void testCloseWithReasonMessage() {
        QwpWebSocketProcessor processor = new QwpWebSocketProcessor();
        CloseTrackingCallback callback = new CloseTrackingCallback();
        processor.setCallback(callback);

        byte[] reason = "Server shutdown".getBytes(StandardCharsets.UTF_8);
        long ptr = allocateAndWrite(reason);
        try {
            processor.onClose(WebSocketCloseCode.GOING_AWAY, ptr, reason.length);

            Assert.assertTrue(callback.closeCalled);
            Assert.assertEquals(WebSocketCloseCode.GOING_AWAY, callback.closeCode);
            Assert.assertEquals(reason.length, callback.reasonLength);
        } finally {
            freeBuffer(ptr, reason.length);
        }
    }

    @Test
    public void testErrorCallbackSetsState() {
        try (QwpWebSocketProcessorState state = new QwpWebSocketProcessorState(1024)) {
            QwpWebSocketProcessor processor = new QwpWebSocketProcessor();
            processor.setCallback(new ErrorSettingCallback(state));

            // Trigger error callback
            processor.onError(WebSocketCloseCode.INVALID_PAYLOAD_DATA, "Invalid ILP format");

            // Verify error was set
            Assert.assertFalse(state.isOk());
            Assert.assertEquals("Invalid ILP format", state.getErrorMessage());
        }
    }

    @Test
    public void testErrorResponseFlow() {
        try (QwpWebSocketProcessorState state = new QwpWebSocketProcessorState(1024)) {
            // Simulate bad data that triggers error response
            state.setErrorResponse(WebSocketCloseCode.INVALID_PAYLOAD_DATA, "Invalid line format");

            Assert.assertTrue(state.hasResponse());
            Assert.assertFalse(state.isResponseSuccess());
            Assert.assertEquals(WebSocketCloseCode.INVALID_PAYLOAD_DATA, state.getResponseErrorCode());
            Assert.assertEquals("Invalid line format", state.getResponseErrorMessage());
        }
    }

    // ==================== RESPONSE HANDLING TESTS ====================

    @Test
    public void testErrorStateIgnoresData() {
        try (QwpWebSocketProcessorState state = new QwpWebSocketProcessorState(1024)) {
            QwpWebSocketProcessor processor = new QwpWebSocketProcessor();
            processor.setCallback(new StateAccumulatingCallback(state));

            // Set error state
            state.setError("Parse error at position 10");
            Assert.assertFalse(state.isOk());

            // Try to send data
            byte[] data = "should be ignored".getBytes(StandardCharsets.UTF_8);
            long ptr = allocateAndWrite(data);
            try {
                processor.onBinaryMessage(ptr, data.length);

                // Data should be ignored when in error state
                Assert.assertEquals(0, state.getBufferPosition());
            } finally {
                freeBuffer(ptr, data.length);
            }
        }
    }

    @Test
    public void testLargeMessageHandling() {
        try (QwpWebSocketProcessorState state = new QwpWebSocketProcessorState(1024)) {
            QwpWebSocketProcessor processor = new QwpWebSocketProcessor();
            processor.setCallback(new StateAccumulatingCallback(state));

            // Send 1MB of data
            byte[] data = new byte[1024 * 1024];
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte) (i % 256);
            }

            long ptr = allocateAndWrite(data);
            try {
                processor.onBinaryMessage(ptr, data.length);

                Assert.assertEquals(data.length, state.getBufferPosition());
                Assert.assertTrue(state.getBufferCapacity() >= data.length);

                // Verify data integrity
                long bufAddr = state.getBufferAddress();
                for (int i = 0; i < data.length; i++) {
                    Assert.assertEquals("Mismatch at index " + i, data[i],
                            Unsafe.getUnsafe().getByte(bufAddr + i));
                }
            } finally {
                freeBuffer(ptr, data.length);
            }
        }
    }

    // ==================== CLOSE HANDLING TESTS ====================

    @Test
    public void testManySmallMessages() {
        try (QwpWebSocketProcessorState state = new QwpWebSocketProcessorState(1024)) {
            QwpWebSocketProcessor processor = new QwpWebSocketProcessor();
            processor.setCallback(new StateAccumulatingCallback(state));

            int numMessages = 1000;
            int messageSize = 50;
            long totalBytes = 0;

            for (int i = 0; i < numMessages; i++) {
                byte[] data = new byte[messageSize];
                for (int j = 0; j < messageSize; j++) {
                    data[j] = (byte) ((i + j) % 256);
                }

                long ptr = allocateAndWrite(data);
                try {
                    processor.onBinaryMessage(ptr, data.length);
                } finally {
                    freeBuffer(ptr, data.length);
                }

                // Process every 10 messages
                if ((i + 1) % 10 == 0) {
                    state.processMessage();
                }
                totalBytes += messageSize;
            }

            // Process remaining
            state.processMessage();

            Assert.assertEquals(totalBytes, state.getBytesProcessed());
        }
    }

    @Test
    public void testMultipleMessagesAccumulate() {
        try (QwpWebSocketProcessorState state = new QwpWebSocketProcessorState(1024)) {
            QwpWebSocketProcessor processor = new QwpWebSocketProcessor();
            processor.setCallback(new StateAccumulatingCallback(state));

            // Send multiple messages
            byte[] msg1 = "cpu,host=server01 usage=95.5 1234567890\n".getBytes(StandardCharsets.UTF_8);
            byte[] msg2 = "mem,host=server01 free=1024 1234567891\n".getBytes(StandardCharsets.UTF_8);
            byte[] msg3 = "disk,host=server01 used=50 1234567892\n".getBytes(StandardCharsets.UTF_8);

            long ptr1 = allocateAndWrite(msg1);
            long ptr2 = allocateAndWrite(msg2);
            long ptr3 = allocateAndWrite(msg3);

            try {
                processor.onBinaryMessage(ptr1, msg1.length);
                Assert.assertEquals(msg1.length, state.getBufferPosition());

                processor.onBinaryMessage(ptr2, msg2.length);
                Assert.assertEquals(msg1.length + msg2.length, state.getBufferPosition());

                processor.onBinaryMessage(ptr3, msg3.length);
                int totalLength = msg1.length + msg2.length + msg3.length;
                Assert.assertEquals(totalLength, state.getBufferPosition());
            } finally {
                freeBuffer(ptr1, msg1.length);
                freeBuffer(ptr2, msg2.length);
                freeBuffer(ptr3, msg3.length);
            }
        }
    }

    // ==================== PING/PONG TESTS ====================

    @Test
    public void testPingPongCallbacks() {
        QwpWebSocketProcessor processor = new QwpWebSocketProcessor();
        PingPongTrackingCallback callback = new PingPongTrackingCallback();
        processor.setCallback(callback);

        byte[] pingData = new byte[]{1, 2, 3, 4};
        long ptr = allocateAndWrite(pingData);
        try {
            processor.onPing(ptr, pingData.length);
            Assert.assertEquals(1, callback.pingCount);

            processor.onPong(ptr, pingData.length);
            Assert.assertEquals(1, callback.pongCount);
        } finally {
            freeBuffer(ptr, pingData.length);
        }
    }

    // ==================== METRICS TRACKING TESTS ====================

    @Test
    public void testProcessMessageAfterAccumulation() {
        try (QwpWebSocketProcessorState state = new QwpWebSocketProcessorState(1024)) {
            QwpWebSocketProcessor processor = new QwpWebSocketProcessor();
            processor.setCallback(new StateAccumulatingCallback(state));

            byte[] data = "cpu,host=server01 usage=95.5 1234567890\n".getBytes(StandardCharsets.UTF_8);
            long ptr = allocateAndWrite(data);

            try {
                processor.onBinaryMessage(ptr, data.length);
                Assert.assertEquals(data.length, state.getBufferPosition());
                Assert.assertEquals(0, state.getBytesProcessed());

                // Process the message
                state.processMessage();

                // Buffer position should be reset, but bytes processed should be updated
                Assert.assertEquals(0, state.getBufferPosition());
                Assert.assertEquals(data.length, state.getBytesProcessed());
            } finally {
                freeBuffer(ptr, data.length);
            }
        }
    }

    @Test
    public void testProcessorWithStateCallback() {
        try (QwpWebSocketProcessorState state = new QwpWebSocketProcessorState(1024)) {
            QwpWebSocketProcessor processor = new QwpWebSocketProcessor();
            processor.setCallback(new StateAccumulatingCallback(state));

            // Simulate receiving binary data
            byte[] data = "cpu,host=server01 usage=95.5 1234567890\n".getBytes(StandardCharsets.UTF_8);
            long ptr = allocateAndWrite(data);
            try {
                processor.onBinaryMessage(ptr, data.length);

                // Verify data was accumulated
                Assert.assertEquals(data.length, state.getBufferPosition());

                // Verify data content
                long bufAddr = state.getBufferAddress();
                for (int i = 0; i < data.length; i++) {
                    Assert.assertEquals("Mismatch at index " + i, data[i],
                            Unsafe.getUnsafe().getByte(bufAddr + i));
                }
            } finally {
                freeBuffer(ptr, data.length);
            }
        }
    }

    // ==================== LARGE DATA TESTS ====================

    @Test
    public void testRapidMessageProcessing() {
        try (QwpWebSocketProcessorState state = new QwpWebSocketProcessorState(256)) {
            QwpWebSocketProcessor processor = new QwpWebSocketProcessor();
            processor.setCallback(new StateAccumulatingCallback(state));

            // Rapidly accumulate and process
            for (int round = 0; round < 100; round++) {
                byte[] data = ("round-" + round + "\n").getBytes(StandardCharsets.UTF_8);
                long ptr = allocateAndWrite(data);
                try {
                    processor.onBinaryMessage(ptr, data.length);
                } finally {
                    freeBuffer(ptr, data.length);
                }

                // Process
                state.processMessage();
                Assert.assertEquals(0, state.getBufferPosition());
                Assert.assertTrue(state.isOk());
            }
        }
    }

    @Test
    public void testRecoverFromError() {
        try (QwpWebSocketProcessorState state = new QwpWebSocketProcessorState(1024)) {
            QwpWebSocketProcessor processor = new QwpWebSocketProcessor();
            processor.setCallback(new StateAccumulatingCallback(state));

            // Set error
            state.setError("Temporary error");
            Assert.assertFalse(state.isOk());

            // Clear to recover
            state.clear();
            Assert.assertTrue(state.isOk());

            // Should accept data again
            byte[] data = "new data".getBytes(StandardCharsets.UTF_8);
            long ptr = allocateAndWrite(data);
            try {
                processor.onBinaryMessage(ptr, data.length);
                Assert.assertEquals(data.length, state.getBufferPosition());
            } finally {
                freeBuffer(ptr, data.length);
            }
        }
    }

    // ==================== CONCURRENT ACCESS SIMULATION ====================

    @Test
    public void testSuccessResponseFlow() {
        try (QwpWebSocketProcessorState state = new QwpWebSocketProcessorState(1024)) {
            QwpWebSocketProcessor processor = new QwpWebSocketProcessor();
            processor.setCallback(new StateAccumulatingCallback(state));

            // Accumulate and process
            byte[] data = "cpu,host=server01 usage=95.5 1234567890\n".getBytes(StandardCharsets.UTF_8);
            long ptr = allocateAndWrite(data);
            try {
                processor.onBinaryMessage(ptr, data.length);
            } finally {
                freeBuffer(ptr, data.length);
            }

            // Process and set success response
            state.processMessage();
            state.setSuccessResponse();

            Assert.assertTrue(state.hasResponse());
            Assert.assertTrue(state.isResponseSuccess());

            // Consume response
            state.consumeResponse();
            Assert.assertFalse(state.hasResponse());
        }
    }

    // ==================== CALLBACK IMPLEMENTATIONS ====================

    private long allocateAndWrite(byte[] data) {
        long ptr = Unsafe.malloc(data.length, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < data.length; i++) {
            Unsafe.getUnsafe().putByte(ptr + i, data[i]);
        }
        return ptr;
    }

    /**
     * Callback that tracks close events.
     */
    private static class CloseTrackingCallback implements QwpWebSocketProcessor.Callback {
        boolean closeCalled = false;
        int closeCode = -1;
        int reasonLength = 0;

        @Override
        public void onBinaryMessage(long payload, int length) {
        }

        @Override
        public void onClose(int code, long reason, int reasonLength) {
            this.closeCalled = true;
            this.closeCode = code;
            this.reasonLength = reasonLength;
        }

        @Override
        public void onError(int errorCode, CharSequence message) {
        }

        @Override
        public void onPing(long payload, int length) {
        }

        @Override
        public void onPong(long payload, int length) {
        }

        @Override
        public void onTextMessage(long payload, int length) {
        }
    }

    /**
     * Callback that sets error state on error.
     */
    private static class ErrorSettingCallback implements QwpWebSocketProcessor.Callback {
        private final QwpWebSocketProcessorState state;

        ErrorSettingCallback(QwpWebSocketProcessorState state) {
            this.state = state;
        }

        @Override
        public void onBinaryMessage(long payload, int length) {
        }

        @Override
        public void onClose(int code, long reason, int reasonLength) {
        }

        @Override
        public void onError(int errorCode, CharSequence message) {
            state.setError(message != null ? message.toString() : "Unknown error");
        }

        @Override
        public void onPing(long payload, int length) {
        }

        @Override
        public void onPong(long payload, int length) {
        }

        @Override
        public void onTextMessage(long payload, int length) {
        }
    }

    /**
     * Callback that tracks ping/pong events.
     */
    private static class PingPongTrackingCallback implements QwpWebSocketProcessor.Callback {
        int pingCount = 0;
        List<byte[]> pingPayloads = new ArrayList<>();
        int pongCount = 0;
        List<byte[]> pongPayloads = new ArrayList<>();

        @Override
        public void onBinaryMessage(long payload, int length) {
        }

        @Override
        public void onClose(int code, long reason, int reasonLength) {
        }

        @Override
        public void onError(int errorCode, CharSequence message) {
        }

        @Override
        public void onPing(long payload, int length) {
            pingCount++;
            if (length > 0) {
                byte[] data = new byte[length];
                for (int i = 0; i < length; i++) {
                    data[i] = Unsafe.getUnsafe().getByte(payload + i);
                }
                pingPayloads.add(data);
            }
        }

        @Override
        public void onPong(long payload, int length) {
            pongCount++;
            if (length > 0) {
                byte[] data = new byte[length];
                for (int i = 0; i < length; i++) {
                    data[i] = Unsafe.getUnsafe().getByte(payload + i);
                }
                pongPayloads.add(data);
            }
        }

        @Override
        public void onTextMessage(long payload, int length) {
        }
    }

    // ==================== HELPER METHODS ====================

    /**
     * Callback that accumulates binary data into the state.
     */
    private static class StateAccumulatingCallback implements QwpWebSocketProcessor.Callback {
        private final QwpWebSocketProcessorState state;

        StateAccumulatingCallback(QwpWebSocketProcessorState state) {
            this.state = state;
        }

        @Override
        public void onBinaryMessage(long payload, int length) {
            state.addData(payload, payload + length);
        }

        @Override
        public void onClose(int code, long reason, int reasonLength) {
        }

        @Override
        public void onError(int errorCode, CharSequence message) {
        }

        @Override
        public void onPing(long payload, int length) {
        }

        @Override
        public void onPong(long payload, int length) {
        }

        @Override
        public void onTextMessage(long payload, int length) {
            // ILP v4 ignores text messages
        }
    }
}
