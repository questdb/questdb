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

import io.questdb.cutlass.qwp.websocket.WebSocketFrameParser;
import io.questdb.cutlass.qwp.websocket.WebSocketFrameWriter;
import io.questdb.cutlass.qwp.websocket.WebSocketOpcode;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

/**
 * Comprehensive tests for WebSocket frame writing.
 */
public class WebSocketFrameWriterTest extends AbstractWebSocketTest {

    @Test
    public void testWriteSmallFrame() {
        long buf = allocateBuffer(16);
        try {
            int written = WebSocketFrameWriter.writeHeader(buf, true,
                    WebSocketOpcode.BINARY, 5, false);

            Assert.assertEquals(2, written);
            Assert.assertEquals((byte) 0x82, Unsafe.getUnsafe().getByte(buf));
            Assert.assertEquals((byte) 0x05, Unsafe.getUnsafe().getByte(buf + 1));
        } finally {
            freeBuffer(buf, 16);
        }
    }

    @Test
    public void testWriteMediumFrame() {
        long buf = allocateBuffer(16);
        try {
            int payloadLen = 1000;
            int written = WebSocketFrameWriter.writeHeader(buf, true,
                    WebSocketOpcode.BINARY, payloadLen, false);

            Assert.assertEquals(4, written);
            Assert.assertEquals((byte) 0x82, Unsafe.getUnsafe().getByte(buf));
            Assert.assertEquals((byte) 126, Unsafe.getUnsafe().getByte(buf + 1));
            Assert.assertEquals((byte) (payloadLen >> 8), Unsafe.getUnsafe().getByte(buf + 2));
            Assert.assertEquals((byte) (payloadLen & 0xFF), Unsafe.getUnsafe().getByte(buf + 3));
        } finally {
            freeBuffer(buf, 16);
        }
    }

    @Test
    public void testWriteLargeFrame() {
        long buf = allocateBuffer(16);
        try {
            long payloadLen = 100000L;
            int written = WebSocketFrameWriter.writeHeader(buf, true,
                    WebSocketOpcode.BINARY, payloadLen, false);

            Assert.assertEquals(10, written);
            Assert.assertEquals((byte) 0x82, Unsafe.getUnsafe().getByte(buf));
            Assert.assertEquals((byte) 127, Unsafe.getUnsafe().getByte(buf + 1));
            Assert.assertEquals(payloadLen,
                    Long.reverseBytes(Unsafe.getUnsafe().getLong(buf + 2)));
        } finally {
            freeBuffer(buf, 16);
        }
    }

    @Test
    public void testWriteTextFrame() {
        long buf = allocateBuffer(16);
        try {
            WebSocketFrameWriter.writeHeader(buf, true, WebSocketOpcode.TEXT, 10, false);

            Assert.assertEquals((byte) 0x81, Unsafe.getUnsafe().getByte(buf));
        } finally {
            freeBuffer(buf, 16);
        }
    }

    @Test
    public void testWriteCloseFrame() {
        long buf = allocateBuffer(16);
        try {
            WebSocketFrameWriter.writeHeader(buf, true, WebSocketOpcode.CLOSE, 2, false);

            Assert.assertEquals((byte) 0x88, Unsafe.getUnsafe().getByte(buf));
        } finally {
            freeBuffer(buf, 16);
        }
    }

    @Test
    public void testWritePingFrame() {
        long buf = allocateBuffer(16);
        try {
            WebSocketFrameWriter.writeHeader(buf, true, WebSocketOpcode.PING, 4, false);

            Assert.assertEquals((byte) 0x89, Unsafe.getUnsafe().getByte(buf));
        } finally {
            freeBuffer(buf, 16);
        }
    }

    @Test
    public void testWritePongFrame() {
        long buf = allocateBuffer(16);
        try {
            WebSocketFrameWriter.writeHeader(buf, true, WebSocketOpcode.PONG, 4, false);

            Assert.assertEquals((byte) 0x8A, Unsafe.getUnsafe().getByte(buf));
        } finally {
            freeBuffer(buf, 16);
        }
    }

    @Test
    public void testWriteNonFinalFrame() {
        long buf = allocateBuffer(16);
        try {
            WebSocketFrameWriter.writeHeader(buf, false, WebSocketOpcode.BINARY, 5, false);

            Assert.assertEquals((byte) 0x02, Unsafe.getUnsafe().getByte(buf));  // No FIN bit
        } finally {
            freeBuffer(buf, 16);
        }
    }

    @Test
    public void testWriteContinuationFrame() {
        long buf = allocateBuffer(16);
        try {
            WebSocketFrameWriter.writeHeader(buf, false, WebSocketOpcode.CONTINUATION, 5, false);

            Assert.assertEquals((byte) 0x00, Unsafe.getUnsafe().getByte(buf));
        } finally {
            freeBuffer(buf, 16);
        }
    }

    @Test
    public void testWriteFinalContinuationFrame() {
        long buf = allocateBuffer(16);
        try {
            WebSocketFrameWriter.writeHeader(buf, true, WebSocketOpcode.CONTINUATION, 5, false);

            Assert.assertEquals((byte) 0x80, Unsafe.getUnsafe().getByte(buf));
        } finally {
            freeBuffer(buf, 16);
        }
    }

    @Test
    public void testHeaderSizeCalculation() {
        Assert.assertEquals(2, WebSocketFrameWriter.headerSize(0, false));
        Assert.assertEquals(2, WebSocketFrameWriter.headerSize(125, false));
        Assert.assertEquals(4, WebSocketFrameWriter.headerSize(126, false));
        Assert.assertEquals(4, WebSocketFrameWriter.headerSize(65535, false));
        Assert.assertEquals(10, WebSocketFrameWriter.headerSize(65536, false));
        Assert.assertEquals(10, WebSocketFrameWriter.headerSize(1000000, false));

        // With masking
        Assert.assertEquals(6, WebSocketFrameWriter.headerSize(0, true));
        Assert.assertEquals(6, WebSocketFrameWriter.headerSize(125, true));
        Assert.assertEquals(8, WebSocketFrameWriter.headerSize(126, true));
        Assert.assertEquals(14, WebSocketFrameWriter.headerSize(65536, true));
    }

    @Test
    public void testWriteCloseFrameWithCode() {
        long buf = allocateBuffer(32);
        try {
            int headerLen = WebSocketFrameWriter.writeHeader(buf, true,
                    WebSocketOpcode.CLOSE, 2, false);
            WebSocketFrameWriter.writeClosePayload(buf + headerLen, 1000, null);

            Assert.assertEquals(2, headerLen);
            short code = Short.reverseBytes(Unsafe.getUnsafe().getShort(buf + 2));
            Assert.assertEquals(1000, code);
        } finally {
            freeBuffer(buf, 32);
        }
    }

    @Test
    public void testWriteCloseFrameWithCodeAndReason() {
        long buf = allocateBuffer(64);
        try {
            String reason = "Goodbye";
            byte[] reasonBytes = reason.getBytes(StandardCharsets.UTF_8);

            int headerLen = WebSocketFrameWriter.writeHeader(buf, true,
                    WebSocketOpcode.CLOSE, 2 + reasonBytes.length, false);
            int payloadLen = WebSocketFrameWriter.writeClosePayload(
                    buf + headerLen, 1001, reason);

            Assert.assertEquals(2 + reasonBytes.length, payloadLen);

            short code = Short.reverseBytes(Unsafe.getUnsafe().getShort(buf + headerLen));
            Assert.assertEquals(1001, code);

            byte[] writtenReason = readBytes(buf + headerLen + 2, reasonBytes.length);
            Assert.assertArrayEquals(reasonBytes, writtenReason);
        } finally {
            freeBuffer(buf, 64);
        }
    }

    @Test
    public void testWriteCompleteCloseFrame() {
        long buf = allocateBuffer(64);
        try {
            int totalLen = WebSocketFrameWriter.writeCloseFrame(buf, 1000, "Normal");

            // Verify header
            Assert.assertEquals((byte) 0x88, Unsafe.getUnsafe().getByte(buf));  // CLOSE

            // Parse what we wrote
            WebSocketFrameParser parser = new WebSocketFrameParser();
            int consumed = parser.parse(buf, buf + totalLen);

            Assert.assertEquals(totalLen, consumed);
            Assert.assertEquals(WebSocketOpcode.CLOSE, parser.getOpcode());
            Assert.assertTrue(parser.isFin());
        } finally {
            freeBuffer(buf, 64);
        }
    }

    @Test
    public void testWritePingFrameComplete() {
        long buf = allocateBuffer(32);
        try {
            byte[] payload = {0x01, 0x02, 0x03, 0x04};
            int totalLen = WebSocketFrameWriter.writePingFrame(buf, payload, 0, payload.length);

            // Verify header
            Assert.assertEquals((byte) 0x89, Unsafe.getUnsafe().getByte(buf));

            // Verify payload
            for (int i = 0; i < payload.length; i++) {
                Assert.assertEquals(payload[i], Unsafe.getUnsafe().getByte(buf + 2 + i));
            }

            Assert.assertEquals(2 + payload.length, totalLen);
        } finally {
            freeBuffer(buf, 32);
        }
    }

    @Test
    public void testWritePongFrameComplete() {
        long buf = allocateBuffer(32);
        try {
            byte[] payload = {0x01, 0x02, 0x03, 0x04};
            int totalLen = WebSocketFrameWriter.writePongFrame(buf, payload, 0, payload.length);

            // Verify header
            Assert.assertEquals((byte) 0x8A, Unsafe.getUnsafe().getByte(buf));

            // Verify payload
            for (int i = 0; i < payload.length; i++) {
                Assert.assertEquals(payload[i], Unsafe.getUnsafe().getByte(buf + 2 + i));
            }

            Assert.assertEquals(2 + payload.length, totalLen);
        } finally {
            freeBuffer(buf, 32);
        }
    }

    @Test
    public void testWritePongFrameFromMemory() {
        long buf = allocateBuffer(32);
        long payloadBuf = allocateBuffer(16);
        try {
            // Write ping payload to memory
            byte[] payload = {0x11, 0x22, 0x33, 0x44};
            writeBytes(payloadBuf, payload);

            int totalLen = WebSocketFrameWriter.writePongFrame(buf, payloadBuf, payload.length);

            // Verify header
            Assert.assertEquals((byte) 0x8A, Unsafe.getUnsafe().getByte(buf));

            // Verify payload was copied
            for (int i = 0; i < payload.length; i++) {
                Assert.assertEquals(payload[i], Unsafe.getUnsafe().getByte(buf + 2 + i));
            }

            Assert.assertEquals(2 + payload.length, totalLen);
        } finally {
            freeBuffer(buf, 32);
            freeBuffer(payloadBuf, 16);
        }
    }

    @Test
    public void testMaskPayload() {
        long buf = allocateBuffer(64);
        try {
            byte[] original = "Hello, WebSocket!".getBytes(StandardCharsets.UTF_8);
            int maskKey = 0x12345678;

            // Write original data
            for (int i = 0; i < original.length; i++) {
                Unsafe.getUnsafe().putByte(buf + i, original[i]);
            }

            // Mask it
            WebSocketFrameWriter.maskPayload(buf, original.length, maskKey);

            // Verify it's masked (different from original)
            byte[] masked = readBytes(buf, original.length);
            boolean allSame = true;
            for (int i = 0; i < original.length; i++) {
                if (original[i] != masked[i]) {
                    allSame = false;
                    break;
                }
            }
            Assert.assertFalse("Data should be masked", allSame);

            // Mask again to unmask (XOR is its own inverse)
            WebSocketFrameWriter.maskPayload(buf, original.length, maskKey);

            // Verify it's back to original
            byte[] unmasked = readBytes(buf, original.length);
            Assert.assertArrayEquals(original, unmasked);
        } finally {
            freeBuffer(buf, 64);
        }
    }

    @Test
    public void testMaskPayloadVaryingLengths() {
        // Test masking with various lengths to exercise different code paths
        for (int len : new int[]{1, 3, 4, 5, 7, 8, 9, 15, 16, 17, 31, 32, 100}) {
            long buf = allocateBuffer(128);
            try {
                byte[] original = new byte[len];
                for (int i = 0; i < len; i++) {
                    original[i] = (byte) (i + 1);
                }
                int maskKey = 0xDEADBEEF;

                // Write and mask
                for (int i = 0; i < len; i++) {
                    Unsafe.getUnsafe().putByte(buf + i, original[i]);
                }
                WebSocketFrameWriter.maskPayload(buf, len, maskKey);

                // Unmask
                WebSocketFrameWriter.maskPayload(buf, len, maskKey);

                // Verify
                byte[] result = readBytes(buf, len);
                Assert.assertArrayEquals("Failed for length " + len, original, result);
            } finally {
                freeBuffer(buf, 128);
            }
        }
    }

    @Test
    public void testRoundTrip() {
        // Write a frame, then parse it - verify consistency
        long buf = allocateBuffer(256);
        try {
            int payloadLen = 100;

            // Write
            int headerLen = WebSocketFrameWriter.writeHeader(buf, true,
                    WebSocketOpcode.BINARY, payloadLen, false);
            for (int i = 0; i < payloadLen; i++) {
                Unsafe.getUnsafe().putByte(buf + headerLen + i, (byte) i);
            }

            // Parse
            WebSocketFrameParser parser = new WebSocketFrameParser();
            int consumed = parser.parse(buf, buf + headerLen + payloadLen);

            Assert.assertEquals(headerLen + payloadLen, consumed);
            Assert.assertTrue(parser.isFin());
            Assert.assertEquals(WebSocketOpcode.BINARY, parser.getOpcode());
            Assert.assertEquals(payloadLen, parser.getPayloadLength());
        } finally {
            freeBuffer(buf, 256);
        }
    }

    @Test
    public void testRoundTripWithMasking() {
        long buf = allocateBuffer(256);
        try {
            byte[] payload = "Test payload data".getBytes(StandardCharsets.UTF_8);
            int maskKey = 0xABCDEF12;

            // Write header with mask
            int headerLen = WebSocketFrameWriter.writeHeader(buf, true,
                    WebSocketOpcode.BINARY, payload.length, maskKey);

            // Write and mask payload
            for (int i = 0; i < payload.length; i++) {
                Unsafe.getUnsafe().putByte(buf + headerLen + i, payload[i]);
            }
            WebSocketFrameWriter.maskPayload(buf + headerLen, payload.length, maskKey);

            // Parse
            WebSocketFrameParser parser = new WebSocketFrameParser();
            parser.setServerMode(true);  // Expect masked frames
            int consumed = parser.parse(buf, buf + headerLen + payload.length);

            Assert.assertEquals(headerLen + payload.length, consumed);
            Assert.assertTrue(parser.isMasked());
            Assert.assertEquals(maskKey, parser.getMaskKey());

            // Unmask and verify payload
            parser.unmaskPayload(buf + headerLen, payload.length);
            byte[] result = readBytes(buf + headerLen, payload.length);
            Assert.assertArrayEquals(payload, result);
        } finally {
            freeBuffer(buf, 256);
        }
    }

    @Test
    public void testWriteHeaderWithMaskKey() {
        long buf = allocateBuffer(32);
        try {
            int maskKey = 0x12345678;
            int headerLen = WebSocketFrameWriter.writeHeader(buf, true,
                    WebSocketOpcode.BINARY, 10, maskKey);

            // Should be 2 (basic header) + 4 (mask key) = 6
            Assert.assertEquals(6, headerLen);

            // Verify mask bit is set
            Assert.assertEquals((byte) (0x80 | 10), Unsafe.getUnsafe().getByte(buf + 1));

            // Verify mask key
            Assert.assertEquals(maskKey, Unsafe.getUnsafe().getInt(buf + 2));
        } finally {
            freeBuffer(buf, 32);
        }
    }

    @Test
    public void testWrite16BitLengthBoundary() {
        long buf = allocateBuffer(16);
        try {
            // Test boundary at 125/126
            int written125 = WebSocketFrameWriter.writeHeader(buf, true,
                    WebSocketOpcode.BINARY, 125, false);
            Assert.assertEquals(2, written125);

            int written126 = WebSocketFrameWriter.writeHeader(buf, true,
                    WebSocketOpcode.BINARY, 126, false);
            Assert.assertEquals(4, written126);
        } finally {
            freeBuffer(buf, 16);
        }
    }

    @Test
    public void testWrite64BitLengthBoundary() {
        long buf = allocateBuffer(16);
        try {
            // Test boundary at 65535/65536
            int written65535 = WebSocketFrameWriter.writeHeader(buf, true,
                    WebSocketOpcode.BINARY, 65535, false);
            Assert.assertEquals(4, written65535);

            int written65536 = WebSocketFrameWriter.writeHeader(buf, true,
                    WebSocketOpcode.BINARY, 65536, false);
            Assert.assertEquals(10, written65536);
        } finally {
            freeBuffer(buf, 16);
        }
    }

    @Test
    public void testWriteCloseFrameWithEmptyReason() {
        long buf = allocateBuffer(32);
        try {
            int totalLen = WebSocketFrameWriter.writeCloseFrame(buf, 1000, "");

            // Should have header (2) + code (2) = 4
            Assert.assertEquals(4, totalLen);

            // Verify the code
            short code = Short.reverseBytes(Unsafe.getUnsafe().getShort(buf + 2));
            Assert.assertEquals(1000, code);
        } finally {
            freeBuffer(buf, 32);
        }
    }

    @Test
    public void testWriteCloseFrameWithNullReason() {
        long buf = allocateBuffer(32);
        try {
            int totalLen = WebSocketFrameWriter.writeCloseFrame(buf, 1000, null);

            // Should have header (2) + code (2) = 4
            Assert.assertEquals(4, totalLen);
        } finally {
            freeBuffer(buf, 32);
        }
    }

    @Test
    public void testAllOpcodes() {
        long buf = allocateBuffer(16);
        try {
            // Verify all opcodes produce correct first byte
            int[] opcodes = {
                    WebSocketOpcode.CONTINUATION,
                    WebSocketOpcode.TEXT,
                    WebSocketOpcode.BINARY,
                    WebSocketOpcode.CLOSE,
                    WebSocketOpcode.PING,
                    WebSocketOpcode.PONG
            };
            byte[] expectedFirstBytes = {
                    (byte) 0x80,  // CONTINUATION + FIN
                    (byte) 0x81,  // TEXT + FIN
                    (byte) 0x82,  // BINARY + FIN
                    (byte) 0x88,  // CLOSE + FIN
                    (byte) 0x89,  // PING + FIN
                    (byte) 0x8A   // PONG + FIN
            };

            for (int i = 0; i < opcodes.length; i++) {
                WebSocketFrameWriter.writeHeader(buf, true, opcodes[i], 0, false);
                Assert.assertEquals("Opcode " + opcodes[i] + " first byte mismatch",
                        expectedFirstBytes[i], Unsafe.getUnsafe().getByte(buf));
            }
        } finally {
            freeBuffer(buf, 16);
        }
    }
}
