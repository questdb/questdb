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
import io.questdb.cutlass.qwp.websocket.WebSocketCloseCode;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests for ILP v4 WebSocket processor.
 */
public class QwpWebSocketProcessorTest extends AbstractWebSocketTest {

    // ==================== PROCESSOR CREATION TESTS ====================

    @Test
    public void testProcessorCreation() {
        QwpWebSocketProcessor processor = new QwpWebSocketProcessor();
        Assert.assertNotNull(processor);
    }

    // ==================== MESSAGE HANDLING TESTS ====================

    @Test
    public void testOnBinaryMessageCallback() {
        RecordingCallback callback = new RecordingCallback();
        QwpWebSocketProcessor processor = new QwpWebSocketProcessor();
        processor.setCallback(callback);

        // Send a simple binary message
        byte[] data = new byte[]{1, 2, 3, 4};
        long ptr = allocateAndWrite(data);
        try {
            processor.onBinaryMessage(ptr, data.length);
            Assert.assertEquals(1, callback.binaryMessageCount);
        } finally {
            freeBuffer(ptr, data.length);
        }
    }

    @Test
    public void testOnTextMessageIgnored() {
        RecordingCallback callback = new RecordingCallback();
        QwpWebSocketProcessor processor = new QwpWebSocketProcessor();
        processor.setCallback(callback);

        // Text messages should be ignored (ILP v4 is binary only)
        byte[] data = "hello".getBytes(StandardCharsets.UTF_8);
        long ptr = allocateAndWrite(data);
        try {
            processor.onTextMessage(ptr, data.length);
            // Text messages are ignored - no error should be raised
            Assert.assertEquals(0, callback.errorCount);
        } finally {
            freeBuffer(ptr, data.length);
        }
    }

    @Test
    public void testOnPingCallback() {
        RecordingCallback callback = new RecordingCallback();
        QwpWebSocketProcessor processor = new QwpWebSocketProcessor();
        processor.setCallback(callback);

        byte[] data = new byte[]{1, 2};
        long ptr = allocateAndWrite(data);
        try {
            processor.onPing(ptr, data.length);
            Assert.assertEquals(1, callback.pingCount);
        } finally {
            freeBuffer(ptr, data.length);
        }
    }

    @Test
    public void testOnPongCallback() {
        RecordingCallback callback = new RecordingCallback();
        QwpWebSocketProcessor processor = new QwpWebSocketProcessor();
        processor.setCallback(callback);

        byte[] data = new byte[]{1, 2};
        long ptr = allocateAndWrite(data);
        try {
            processor.onPong(ptr, data.length);
            Assert.assertEquals(1, callback.pongCount);
        } finally {
            freeBuffer(ptr, data.length);
        }
    }

    @Test
    public void testOnCloseCallback() {
        RecordingCallback callback = new RecordingCallback();
        QwpWebSocketProcessor processor = new QwpWebSocketProcessor();
        processor.setCallback(callback);

        byte[] reason = "test close".getBytes(StandardCharsets.UTF_8);
        long ptr = allocateAndWrite(reason);
        try {
            processor.onClose(WebSocketCloseCode.NORMAL_CLOSURE, ptr, reason.length);
            Assert.assertEquals(1, callback.closeCount);
            Assert.assertEquals(WebSocketCloseCode.NORMAL_CLOSURE, callback.lastCloseCode);
        } finally {
            freeBuffer(ptr, reason.length);
        }
    }

    @Test
    public void testOnErrorCallback() {
        RecordingCallback callback = new RecordingCallback();
        QwpWebSocketProcessor processor = new QwpWebSocketProcessor();
        processor.setCallback(callback);

        processor.onError(WebSocketCloseCode.PROTOCOL_ERROR, "test error");
        Assert.assertEquals(1, callback.errorCount);
        Assert.assertEquals(WebSocketCloseCode.PROTOCOL_ERROR, callback.lastErrorCode);
    }

    // ==================== EMPTY MESSAGE TESTS ====================

    @Test
    public void testEmptyBinaryMessage() {
        RecordingCallback callback = new RecordingCallback();
        QwpWebSocketProcessor processor = new QwpWebSocketProcessor();
        processor.setCallback(callback);

        // Empty message should be handled gracefully
        processor.onBinaryMessage(0, 0);
        // Should not crash, may or may not record depending on implementation
    }

    @Test
    public void testEmptyPingMessage() {
        RecordingCallback callback = new RecordingCallback();
        QwpWebSocketProcessor processor = new QwpWebSocketProcessor();
        processor.setCallback(callback);

        processor.onPing(0, 0);
        Assert.assertEquals(1, callback.pingCount);
    }

    // ==================== NULL CALLBACK TESTS ====================

    @Test
    public void testNullCallbackDoesNotCrash() {
        QwpWebSocketProcessor processor = new QwpWebSocketProcessor();
        // No callback set

        byte[] data = new byte[]{1, 2, 3};
        long ptr = allocateAndWrite(data);
        try {
            // Should not throw NullPointerException
            processor.onBinaryMessage(ptr, data.length);
            processor.onPing(ptr, data.length);
            processor.onPong(ptr, data.length);
            processor.onClose(1000, ptr, data.length);
            processor.onError(1002, "error");
        } finally {
            freeBuffer(ptr, data.length);
        }
    }

    // ==================== MULTIPLE MESSAGE TESTS ====================

    @Test
    public void testMultipleBinaryMessages() {
        RecordingCallback callback = new RecordingCallback();
        QwpWebSocketProcessor processor = new QwpWebSocketProcessor();
        processor.setCallback(callback);

        for (int i = 0; i < 10; i++) {
            byte[] data = new byte[]{(byte) i};
            long ptr = allocateAndWrite(data);
            try {
                processor.onBinaryMessage(ptr, data.length);
            } finally {
                freeBuffer(ptr, data.length);
            }
        }

        Assert.assertEquals(10, callback.binaryMessageCount);
    }

    // ==================== HELPER METHODS ====================

    private long allocateAndWrite(byte[] data) {
        long ptr = Unsafe.malloc(data.length, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < data.length; i++) {
            Unsafe.getUnsafe().putByte(ptr + i, data[i]);
        }
        return ptr;
    }

    // ==================== CLOSE CODE TESTS ====================

    @Test
    public void testCloseWithNormalClosure() {
        RecordingCallback callback = new RecordingCallback();
        QwpWebSocketProcessor processor = new QwpWebSocketProcessor();
        processor.setCallback(callback);

        processor.onClose(WebSocketCloseCode.NORMAL_CLOSURE, 0, 0);
        Assert.assertEquals(WebSocketCloseCode.NORMAL_CLOSURE, callback.lastCloseCode);
    }

    @Test
    public void testCloseWithGoingAway() {
        RecordingCallback callback = new RecordingCallback();
        QwpWebSocketProcessor processor = new QwpWebSocketProcessor();
        processor.setCallback(callback);

        processor.onClose(WebSocketCloseCode.GOING_AWAY, 0, 0);
        Assert.assertEquals(WebSocketCloseCode.GOING_AWAY, callback.lastCloseCode);
    }

    @Test
    public void testCloseWithProtocolError() {
        RecordingCallback callback = new RecordingCallback();
        QwpWebSocketProcessor processor = new QwpWebSocketProcessor();
        processor.setCallback(callback);

        processor.onClose(WebSocketCloseCode.PROTOCOL_ERROR, 0, 0);
        Assert.assertEquals(WebSocketCloseCode.PROTOCOL_ERROR, callback.lastCloseCode);
    }

    @Test
    public void testCloseWithReason() {
        RecordingCallback callback = new RecordingCallback();
        QwpWebSocketProcessor processor = new QwpWebSocketProcessor();
        processor.setCallback(callback);

        byte[] reason = "server shutdown".getBytes(StandardCharsets.UTF_8);
        long ptr = allocateAndWrite(reason);
        try {
            processor.onClose(WebSocketCloseCode.GOING_AWAY, ptr, reason.length);
            Assert.assertEquals(1, callback.closeCount);
            Assert.assertEquals(reason.length, callback.lastReasonLength);
        } finally {
            freeBuffer(ptr, reason.length);
        }
    }

    // ==================== ERROR HANDLING TESTS ====================

    @Test
    public void testErrorWithMessage() {
        RecordingCallback callback = new RecordingCallback();
        QwpWebSocketProcessor processor = new QwpWebSocketProcessor();
        processor.setCallback(callback);

        String errorMessage = "Invalid ILP message format";
        processor.onError(WebSocketCloseCode.INVALID_PAYLOAD_DATA, errorMessage);

        Assert.assertEquals(1, callback.errorCount);
        Assert.assertEquals(WebSocketCloseCode.INVALID_PAYLOAD_DATA, callback.lastErrorCode);
        Assert.assertEquals(errorMessage, callback.lastErrorMessage);
    }

    @Test
    public void testErrorWithNullMessage() {
        RecordingCallback callback = new RecordingCallback();
        QwpWebSocketProcessor processor = new QwpWebSocketProcessor();
        processor.setCallback(callback);

        processor.onError(WebSocketCloseCode.INTERNAL_ERROR, null);

        Assert.assertEquals(1, callback.errorCount);
        Assert.assertEquals(WebSocketCloseCode.INTERNAL_ERROR, callback.lastErrorCode);
    }

    // ==================== LARGE MESSAGE TESTS ====================

    @Test
    public void testLargeBinaryMessage() {
        RecordingCallback callback = new RecordingCallback();
        QwpWebSocketProcessor processor = new QwpWebSocketProcessor();
        processor.setCallback(callback);

        // 64KB message
        byte[] data = new byte[65536];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }

        long ptr = allocateAndWrite(data);
        try {
            processor.onBinaryMessage(ptr, data.length);
            Assert.assertEquals(1, callback.binaryMessageCount);
            Assert.assertArrayEquals(data, callback.binaryMessages.get(0));
        } finally {
            freeBuffer(ptr, data.length);
        }
    }

    // ==================== CALLBACK REPLACEMENT TESTS ====================

    @Test
    public void testCallbackReplacement() {
        RecordingCallback callback1 = new RecordingCallback();
        RecordingCallback callback2 = new RecordingCallback();
        QwpWebSocketProcessor processor = new QwpWebSocketProcessor();

        processor.setCallback(callback1);

        byte[] data = new byte[]{1};
        long ptr = allocateAndWrite(data);
        try {
            processor.onBinaryMessage(ptr, data.length);
            Assert.assertEquals(1, callback1.binaryMessageCount);
            Assert.assertEquals(0, callback2.binaryMessageCount);

            processor.setCallback(callback2);
            processor.onBinaryMessage(ptr, data.length);
            Assert.assertEquals(1, callback1.binaryMessageCount);
            Assert.assertEquals(1, callback2.binaryMessageCount);
        } finally {
            freeBuffer(ptr, data.length);
        }
    }

    @Test
    public void testClearCallback() {
        RecordingCallback callback = new RecordingCallback();
        QwpWebSocketProcessor processor = new QwpWebSocketProcessor();
        processor.setCallback(callback);

        byte[] data = new byte[]{1};
        long ptr = allocateAndWrite(data);
        try {
            processor.onBinaryMessage(ptr, data.length);
            Assert.assertEquals(1, callback.binaryMessageCount);

            // Clear callback
            processor.setCallback(null);
            processor.onBinaryMessage(ptr, data.length);
            // Count should not increase
            Assert.assertEquals(1, callback.binaryMessageCount);
        } finally {
            freeBuffer(ptr, data.length);
        }
    }

    // ==================== RECORDING CALLBACK ====================

    /**
     * A callback that records all events for testing.
     */
    private static class RecordingCallback implements QwpWebSocketProcessor.Callback {
        int binaryMessageCount = 0;
        int textMessageCount = 0;
        int pingCount = 0;
        int pongCount = 0;
        int closeCount = 0;
        int errorCount = 0;
        int lastCloseCode = -1;
        int lastReasonLength = 0;
        int lastErrorCode = -1;
        String lastErrorMessage = null;
        List<byte[]> binaryMessages = new ArrayList<>();

        @Override
        public void onBinaryMessage(long payload, int length) {
            binaryMessageCount++;
            byte[] data = new byte[length];
            for (int i = 0; i < length; i++) {
                data[i] = Unsafe.getUnsafe().getByte(payload + i);
            }
            binaryMessages.add(data);
        }

        @Override
        public void onTextMessage(long payload, int length) {
            textMessageCount++;
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
        public void onClose(int code, long reason, int reasonLength) {
            closeCount++;
            lastCloseCode = code;
            lastReasonLength = reasonLength;
        }

        @Override
        public void onError(int errorCode, CharSequence message) {
            errorCount++;
            lastErrorCode = errorCode;
            lastErrorMessage = message != null ? message.toString() : null;
        }
    }
}
