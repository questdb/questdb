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
import io.questdb.cutlass.qwp.websocket.WebSocketFrameParser;
import io.questdb.cutlass.qwp.websocket.WebSocketOpcode;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Random;

/**
 * Comprehensive tests for WebSocket frame parsing.
 * Tests MUST pass before implementation is considered complete.
 */
public class WebSocketFrameParserTest extends AbstractWebSocketTest {

    // ==================== FRAME STRUCTURE TESTS ====================

    @Test
    public void testParseMinimalFrame() {
        // Single byte payload, no mask (server->client style for testing)
        // 0x82 = FIN + BINARY, 0x01 = length 1, 0xFF = payload
        long buf = allocateBuffer(16);
        try {
            writeBytes(buf, (byte) 0x82, (byte) 0x01, (byte) 0xFF);

            WebSocketFrameParser parser = new WebSocketFrameParser();
            int consumed = parser.parse(buf, buf + 3);

            Assert.assertEquals(3, consumed);
            Assert.assertTrue(parser.isFin());
            Assert.assertEquals(WebSocketOpcode.BINARY, parser.getOpcode());
            Assert.assertEquals(1, parser.getPayloadLength());
            Assert.assertFalse(parser.isMasked());
        } finally {
            freeBuffer(buf, 16);
        }
    }

    @Test
    public void testParse7BitLength() {
        // Payload length 0-125 uses 7-bit length field
        for (int len = 0; len <= 125; len++) {
            long buf = allocateBuffer(256);
            try {
                writeBytes(buf, (byte) 0x82, (byte) len);
                // Write dummy payload
                for (int i = 0; i < len; i++) {
                    Unsafe.getUnsafe().putByte(buf + 2 + i, (byte) i);
                }

                WebSocketFrameParser parser = new WebSocketFrameParser();
                int consumed = parser.parse(buf, buf + 2 + len);

                Assert.assertEquals(2 + len, consumed);
                Assert.assertEquals(len, parser.getPayloadLength());
            } finally {
                freeBuffer(buf, 256);
            }
        }
    }

    @Test
    public void testParse16BitLength() {
        // Length 126 = use next 2 bytes as 16-bit length
        int payloadLen = 1000;
        long buf = allocateBuffer(payloadLen + 16);
        try {
            writeBytes(buf,
                    (byte) 0x82,           // FIN + BINARY
                    (byte) 126,            // 16-bit length follows
                    (byte) (payloadLen >> 8),   // Length high byte
                    (byte) (payloadLen & 0xFF)  // Length low byte
            );

            WebSocketFrameParser parser = new WebSocketFrameParser();
            int consumed = parser.parse(buf, buf + 4 + payloadLen);

            Assert.assertEquals(4 + payloadLen, consumed);
            Assert.assertEquals(payloadLen, parser.getPayloadLength());
        } finally {
            freeBuffer(buf, payloadLen + 16);
        }
    }

    @Test
    public void testParse64BitLength() {
        // Length 127 = use next 8 bytes as 64-bit length
        long payloadLen = 70000L;  // > 65535
        long buf = allocateBuffer((int) payloadLen + 16);
        try {
            Unsafe.getUnsafe().putByte(buf, (byte) 0x82);
            Unsafe.getUnsafe().putByte(buf + 1, (byte) 127);
            Unsafe.getUnsafe().putLong(buf + 2, Long.reverseBytes(payloadLen));

            WebSocketFrameParser parser = new WebSocketFrameParser();
            int consumed = parser.parse(buf, buf + 10 + payloadLen);

            Assert.assertEquals(10 + payloadLen, consumed);
            Assert.assertEquals(payloadLen, parser.getPayloadLength());
        } finally {
            freeBuffer(buf, (int) payloadLen + 16);
        }
    }

    // ==================== MASKING TESTS ====================

    @Test
    public void testParseMaskedFrame() {
        // Client->Server frames MUST be masked
        long buf = allocateBuffer(32);
        try {
            // "hello" masked with key 0x12345678
            byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
            byte[] maskKey = {0x12, 0x34, 0x56, 0x78};

            writeBytes(buf,
                    (byte) 0x82,                    // FIN + BINARY
                    (byte) (0x80 | payload.length), // MASK bit + length
                    maskKey[0], maskKey[1], maskKey[2], maskKey[3]
            );

            // Write masked payload
            for (int i = 0; i < payload.length; i++) {
                Unsafe.getUnsafe().putByte(buf + 6 + i,
                        (byte) (payload[i] ^ maskKey[i % 4]));
            }

            WebSocketFrameParser parser = new WebSocketFrameParser();
            parser.setServerMode(true);  // Expect masked frames
            int consumed = parser.parse(buf, buf + 6 + payload.length);

            Assert.assertEquals(6 + payload.length, consumed);
            Assert.assertTrue(parser.isMasked());
            Assert.assertEquals(0x78563412, parser.getMaskKey()); // Little-endian int
        } finally {
            freeBuffer(buf, 32);
        }
    }

    @Test
    public void testUnmaskPayload() {
        long buf = allocateBuffer(32);
        try {
            byte[] original = "hello".getBytes(StandardCharsets.UTF_8);
            byte[] maskKey = {0x12, 0x34, 0x56, 0x78};

            // Write masked data
            for (int i = 0; i < original.length; i++) {
                Unsafe.getUnsafe().putByte(buf + i,
                        (byte) (original[i] ^ maskKey[i % 4]));
            }

            WebSocketFrameParser parser = new WebSocketFrameParser();
            parser.setMaskKey(0x78563412);  // Little-endian
            parser.unmaskPayload(buf, original.length);

            byte[] result = readBytes(buf, original.length);
            Assert.assertArrayEquals(original, result);
        } finally {
            freeBuffer(buf, 32);
        }
    }

    @Test
    public void testUnmaskPayloadUnaligned() {
        // Test unmasking with lengths not divisible by 4
        for (int len = 1; len <= 20; len++) {
            long buf = allocateBuffer(32);
            try {
                byte[] original = new byte[len];
                for (int i = 0; i < len; i++) original[i] = (byte) (i + 1);
                byte[] maskKey = {0x37, 0x42, (byte) 0xAB, (byte) 0xCD};

                // Mask
                for (int i = 0; i < len; i++) {
                    Unsafe.getUnsafe().putByte(buf + i,
                            (byte) (original[i] ^ maskKey[i % 4]));
                }

                WebSocketFrameParser parser = new WebSocketFrameParser();
                parser.setMaskKey(0xCDAB4237);  // Little-endian of 0x3742ABCD
                parser.unmaskPayload(buf, len);

                byte[] result = readBytes(buf, len);
                Assert.assertArrayEquals("Failed for length " + len, original, result);
            } finally {
                freeBuffer(buf, 32);
            }
        }
    }

    @Test
    public void testUnmaskLargePayload() {
        // Test SIMD-optimizable path with large aligned payload
        int len = 4096;
        long buf = allocateBuffer(len);
        try {
            byte[] original = new byte[len];
            new Random(42).nextBytes(original);
            byte[] maskKey = {0x11, 0x22, 0x33, 0x44};

            for (int i = 0; i < len; i++) {
                Unsafe.getUnsafe().putByte(buf + i,
                        (byte) (original[i] ^ maskKey[i % 4]));
            }

            WebSocketFrameParser parser = new WebSocketFrameParser();
            parser.setMaskKey(0x44332211);  // Little-endian
            parser.unmaskPayload(buf, len);

            byte[] result = readBytes(buf, len);
            Assert.assertArrayEquals(original, result);
        } finally {
            freeBuffer(buf, len);
        }
    }

    // ==================== OPCODE TESTS ====================

    @Test
    public void testParseTextFrame() {
        long buf = allocateBuffer(16);
        try {
            writeBytes(buf, (byte) 0x81, (byte) 0x00);  // FIN + TEXT, empty

            WebSocketFrameParser parser = new WebSocketFrameParser();
            parser.parse(buf, buf + 2);

            Assert.assertEquals(WebSocketOpcode.TEXT, parser.getOpcode());
        } finally {
            freeBuffer(buf, 16);
        }
    }

    @Test
    public void testParseBinaryFrame() {
        long buf = allocateBuffer(16);
        try {
            writeBytes(buf, (byte) 0x82, (byte) 0x00);  // FIN + BINARY, empty

            WebSocketFrameParser parser = new WebSocketFrameParser();
            parser.parse(buf, buf + 2);

            Assert.assertEquals(WebSocketOpcode.BINARY, parser.getOpcode());
        } finally {
            freeBuffer(buf, 16);
        }
    }

    @Test
    public void testParseCloseFrame() {
        long buf = allocateBuffer(16);
        try {
            // Close with code 1000 (normal closure)
            writeBytes(buf,
                    (byte) 0x88,  // FIN + CLOSE
                    (byte) 0x02,  // Length 2 (just the code)
                    (byte) 0x03, (byte) 0xE8  // 1000 in big-endian
            );

            WebSocketFrameParser parser = new WebSocketFrameParser();
            parser.parse(buf, buf + 4);

            Assert.assertEquals(WebSocketOpcode.CLOSE, parser.getOpcode());
            Assert.assertEquals(2, parser.getPayloadLength());
        } finally {
            freeBuffer(buf, 16);
        }
    }

    @Test
    public void testParsePingFrame() {
        long buf = allocateBuffer(16);
        try {
            writeBytes(buf, (byte) 0x89, (byte) 0x04, (byte) 0x01, (byte) 0x02, (byte) 0x03, (byte) 0x04);

            WebSocketFrameParser parser = new WebSocketFrameParser();
            parser.parse(buf, buf + 6);

            Assert.assertEquals(WebSocketOpcode.PING, parser.getOpcode());
            Assert.assertEquals(4, parser.getPayloadLength());
        } finally {
            freeBuffer(buf, 16);
        }
    }

    @Test
    public void testParsePongFrame() {
        long buf = allocateBuffer(16);
        try {
            writeBytes(buf, (byte) 0x8A, (byte) 0x04, (byte) 0x01, (byte) 0x02, (byte) 0x03, (byte) 0x04);

            WebSocketFrameParser parser = new WebSocketFrameParser();
            parser.parse(buf, buf + 6);

            Assert.assertEquals(WebSocketOpcode.PONG, parser.getOpcode());
        } finally {
            freeBuffer(buf, 16);
        }
    }

    @Test
    public void testParseContinuationFrame() {
        long buf = allocateBuffer(16);
        try {
            writeBytes(buf, (byte) 0x00, (byte) 0x05, (byte) 0x01, (byte) 0x02, (byte) 0x03, (byte) 0x04, (byte) 0x05);

            WebSocketFrameParser parser = new WebSocketFrameParser();
            parser.parse(buf, buf + 7);

            Assert.assertEquals(WebSocketOpcode.CONTINUATION, parser.getOpcode());
            Assert.assertFalse(parser.isFin());
        } finally {
            freeBuffer(buf, 16);
        }
    }

    // ==================== FRAGMENTATION TESTS ====================

    @Test
    public void testParseFragmentedMessage() {
        long buf = allocateBuffer(64);
        try {
            // First fragment: opcode=TEXT, FIN=0
            writeBytes(buf, (byte) 0x01, (byte) 0x03, (byte) 'H', (byte) 'e', (byte) 'l');

            WebSocketFrameParser parser = new WebSocketFrameParser();
            int consumed = parser.parse(buf, buf + 5);

            Assert.assertEquals(5, consumed);
            Assert.assertFalse(parser.isFin());
            Assert.assertEquals(WebSocketOpcode.TEXT, parser.getOpcode());

            // Continuation: opcode=CONTINUATION, FIN=0
            parser.reset();
            writeBytes(buf, (byte) 0x00, (byte) 0x02, (byte) 'l', (byte) 'o');
            consumed = parser.parse(buf, buf + 4);

            Assert.assertEquals(4, consumed);
            Assert.assertFalse(parser.isFin());
            Assert.assertEquals(WebSocketOpcode.CONTINUATION, parser.getOpcode());

            // Final fragment: opcode=CONTINUATION, FIN=1
            parser.reset();
            writeBytes(buf, (byte) 0x80, (byte) 0x01, (byte) '!');
            consumed = parser.parse(buf, buf + 3);

            Assert.assertEquals(3, consumed);
            Assert.assertTrue(parser.isFin());
            Assert.assertEquals(WebSocketOpcode.CONTINUATION, parser.getOpcode());
        } finally {
            freeBuffer(buf, 64);
        }
    }

    @Test
    public void testControlFrameBetweenFragments() {
        // Control frames can appear between data fragments
        long buf = allocateBuffer(64);
        try {
            WebSocketFrameParser parser = new WebSocketFrameParser();

            // First data fragment
            writeBytes(buf, (byte) 0x01, (byte) 0x02, (byte) 'H', (byte) 'i');
            parser.parse(buf, buf + 4);
            Assert.assertFalse(parser.isFin());
            Assert.assertEquals(WebSocketOpcode.TEXT, parser.getOpcode());

            // Ping in the middle (control frame, FIN must be 1)
            parser.reset();
            writeBytes(buf, (byte) 0x89, (byte) 0x00);
            parser.parse(buf, buf + 2);
            Assert.assertTrue(parser.isFin());
            Assert.assertEquals(WebSocketOpcode.PING, parser.getOpcode());

            // Final data fragment
            parser.reset();
            writeBytes(buf, (byte) 0x80, (byte) 0x01, (byte) '!');
            parser.parse(buf, buf + 3);
            Assert.assertTrue(parser.isFin());
            Assert.assertEquals(WebSocketOpcode.CONTINUATION, parser.getOpcode());
        } finally {
            freeBuffer(buf, 64);
        }
    }

    // ==================== INCOMPLETE DATA TESTS ====================

    @Test
    public void testParseIncompleteHeader1Byte() {
        long buf = allocateBuffer(16);
        try {
            writeBytes(buf, (byte) 0x82);  // Only first byte

            WebSocketFrameParser parser = new WebSocketFrameParser();
            int consumed = parser.parse(buf, buf + 1);

            Assert.assertEquals(0, consumed);  // Need more data
            Assert.assertEquals(WebSocketFrameParser.STATE_NEED_MORE, parser.getState());
        } finally {
            freeBuffer(buf, 16);
        }
    }

    @Test
    public void testParseIncompleteHeader16BitLength() {
        long buf = allocateBuffer(16);
        try {
            writeBytes(buf, (byte) 0x82, (byte) 126, (byte) 0x01);  // Missing 2nd length byte

            WebSocketFrameParser parser = new WebSocketFrameParser();
            int consumed = parser.parse(buf, buf + 3);

            Assert.assertEquals(0, consumed);
            Assert.assertEquals(WebSocketFrameParser.STATE_NEED_MORE, parser.getState());
        } finally {
            freeBuffer(buf, 16);
        }
    }

    @Test
    public void testParseIncompleteHeader64BitLength() {
        long buf = allocateBuffer(16);
        try {
            writeBytes(buf, (byte) 0x82, (byte) 127, (byte) 0, (byte) 0, (byte) 0, (byte) 0);  // Only 4 of 8 length bytes

            WebSocketFrameParser parser = new WebSocketFrameParser();
            int consumed = parser.parse(buf, buf + 6);

            Assert.assertEquals(0, consumed);
        } finally {
            freeBuffer(buf, 16);
        }
    }

    @Test
    public void testParseIncompleteMaskKey() {
        long buf = allocateBuffer(16);
        try {
            writeBytes(buf, (byte) 0x82, (byte) 0x85, (byte) 0x12, (byte) 0x34);  // Only 2 of 4 mask bytes

            WebSocketFrameParser parser = new WebSocketFrameParser();
            parser.setServerMode(true);  // Expect masked frames
            int consumed = parser.parse(buf, buf + 4);

            Assert.assertEquals(0, consumed);
        } finally {
            freeBuffer(buf, 16);
        }
    }

    @Test
    public void testParseIncompletePayload() {
        long buf = allocateBuffer(16);
        try {
            writeBytes(buf, (byte) 0x82, (byte) 0x05, (byte) 0x01, (byte) 0x02);  // Only 2 of 5 payload bytes

            WebSocketFrameParser parser = new WebSocketFrameParser();
            int consumed = parser.parse(buf, buf + 4);

            // Parser should return header size, indicating header parsed but payload incomplete
            Assert.assertEquals(2, consumed);
            Assert.assertEquals(5, parser.getPayloadLength());
            Assert.assertEquals(WebSocketFrameParser.STATE_NEED_PAYLOAD, parser.getState());
        } finally {
            freeBuffer(buf, 16);
        }
    }

    @Test
    public void testParseMultipleFramesInBuffer() {
        long buf = allocateBuffer(32);
        try {
            // Two complete frames back-to-back
            writeBytes(buf,
                    (byte) 0x82, (byte) 0x02, (byte) 0x01, (byte) 0x02,  // Frame 1: binary, 2 bytes
                    (byte) 0x81, (byte) 0x03, (byte) 'a', (byte) 'b', (byte) 'c'  // Frame 2: text, 3 bytes
            );

            WebSocketFrameParser parser = new WebSocketFrameParser();

            // Parse first frame
            int consumed = parser.parse(buf, buf + 9);
            Assert.assertEquals(4, consumed);
            Assert.assertEquals(WebSocketOpcode.BINARY, parser.getOpcode());
            Assert.assertEquals(2, parser.getPayloadLength());

            // Parse second frame
            parser.reset();
            consumed = parser.parse(buf + 4, buf + 9);
            Assert.assertEquals(5, consumed);
            Assert.assertEquals(WebSocketOpcode.TEXT, parser.getOpcode());
            Assert.assertEquals(3, parser.getPayloadLength());
        } finally {
            freeBuffer(buf, 32);
        }
    }

    // ==================== ERROR CASES ====================

    @Test
    public void testRejectReservedBits() {
        long buf = allocateBuffer(16);
        try {
            // RSV1 bit set (0x40)
            writeBytes(buf, (byte) 0xC2, (byte) 0x00);

            WebSocketFrameParser parser = new WebSocketFrameParser();
            parser.parse(buf, buf + 2);

            Assert.assertEquals(WebSocketFrameParser.STATE_ERROR, parser.getState());
            Assert.assertEquals(WebSocketCloseCode.PROTOCOL_ERROR, parser.getErrorCode());
        } finally {
            freeBuffer(buf, 16);
        }
    }

    @Test
    public void testRejectRSV2Bit() {
        long buf = allocateBuffer(16);
        try {
            writeBytes(buf, (byte) 0xA2, (byte) 0x00);  // RSV2 set

            WebSocketFrameParser parser = new WebSocketFrameParser();
            parser.parse(buf, buf + 2);

            Assert.assertEquals(WebSocketFrameParser.STATE_ERROR, parser.getState());
        } finally {
            freeBuffer(buf, 16);
        }
    }

    @Test
    public void testRejectRSV3Bit() {
        long buf = allocateBuffer(16);
        try {
            writeBytes(buf, (byte) 0x92, (byte) 0x00);  // RSV3 set

            WebSocketFrameParser parser = new WebSocketFrameParser();
            parser.parse(buf, buf + 2);

            Assert.assertEquals(WebSocketFrameParser.STATE_ERROR, parser.getState());
        } finally {
            freeBuffer(buf, 16);
        }
    }

    @Test
    public void testRejectUnknownOpcode() {
        for (int opcode : new int[]{3, 4, 5, 6, 7, 0xB, 0xC, 0xD, 0xE, 0xF}) {
            long buf = allocateBuffer(16);
            try {
                writeBytes(buf, (byte) (0x80 | opcode), (byte) 0x00);

                WebSocketFrameParser parser = new WebSocketFrameParser();
                parser.parse(buf, buf + 2);

                Assert.assertEquals("Opcode " + opcode + " should be rejected",
                        WebSocketFrameParser.STATE_ERROR, parser.getState());
            } finally {
                freeBuffer(buf, 16);
            }
        }
    }

    @Test
    public void testRejectFragmentedControlFrame() {
        long buf = allocateBuffer(16);
        try {
            // Ping with FIN=0 (fragmented control frame - not allowed)
            writeBytes(buf, (byte) 0x09, (byte) 0x00);

            WebSocketFrameParser parser = new WebSocketFrameParser();
            parser.parse(buf, buf + 2);

            Assert.assertEquals(WebSocketFrameParser.STATE_ERROR, parser.getState());
            Assert.assertEquals(WebSocketCloseCode.PROTOCOL_ERROR, parser.getErrorCode());
        } finally {
            freeBuffer(buf, 16);
        }
    }

    @Test
    public void testRejectOversizeControlFrame() {
        long buf = allocateBuffer(256);
        try {
            // Ping with 126 byte payload (> 125 limit for control frames)
            writeBytes(buf, (byte) 0x89, (byte) 126, (byte) 0x00, (byte) 0x7E);

            WebSocketFrameParser parser = new WebSocketFrameParser();
            parser.parse(buf, buf + 4);

            Assert.assertEquals(WebSocketFrameParser.STATE_ERROR, parser.getState());
            Assert.assertEquals(WebSocketCloseCode.PROTOCOL_ERROR, parser.getErrorCode());
        } finally {
            freeBuffer(buf, 256);
        }
    }

    @Test
    public void testRejectUnmaskedClientFrame() {
        // When parser is in server mode, client frames MUST be masked
        long buf = allocateBuffer(16);
        try {
            writeBytes(buf, (byte) 0x82, (byte) 0x01, (byte) 0xFF);  // No mask bit

            WebSocketFrameParser parser = new WebSocketFrameParser();
            parser.setServerMode(true);  // Expect masked frames
            parser.parse(buf, buf + 3);

            Assert.assertEquals(WebSocketFrameParser.STATE_ERROR, parser.getState());
            Assert.assertEquals(WebSocketCloseCode.PROTOCOL_ERROR, parser.getErrorCode());
        } finally {
            freeBuffer(buf, 16);
        }
    }

    @Test
    public void testRejectMaskedServerFrame() {
        // When parser is in client mode, server frames MUST NOT be masked
        long buf = allocateBuffer(16);
        try {
            writeBytes(buf, (byte) 0x82, (byte) 0x81, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xFF);

            WebSocketFrameParser parser = new WebSocketFrameParser();
            parser.setServerMode(false);  // Client mode - expect unmasked
            parser.parse(buf, buf + 7);

            Assert.assertEquals(WebSocketFrameParser.STATE_ERROR, parser.getState());
        } finally {
            freeBuffer(buf, 16);
        }
    }

    @Test
    public void testRejectZeroLengthIn16BitField() {
        // Using 126 length indicator but value < 126 is not minimal encoding
        long buf = allocateBuffer(16);
        try {
            writeBytes(buf, (byte) 0x82, (byte) 126, (byte) 0x00, (byte) 0x05);  // Length 5 in 16-bit

            WebSocketFrameParser parser = new WebSocketFrameParser();
            parser.setStrictMode(true);
            parser.parse(buf, buf + 4);

            // Some implementations allow this, strict mode should reject
            Assert.assertEquals(WebSocketFrameParser.STATE_ERROR, parser.getState());
        } finally {
            freeBuffer(buf, 16);
        }
    }

    // ==================== EDGE CASES ====================

    @Test
    public void testParseEmptyPayload() {
        long buf = allocateBuffer(16);
        try {
            writeBytes(buf, (byte) 0x82, (byte) 0x00);

            WebSocketFrameParser parser = new WebSocketFrameParser();
            int consumed = parser.parse(buf, buf + 2);

            Assert.assertEquals(2, consumed);
            Assert.assertEquals(0, parser.getPayloadLength());
        } finally {
            freeBuffer(buf, 16);
        }
    }

    @Test
    public void testParseMaxControlFrameSize() {
        // Control frames can have up to 125 bytes payload
        long buf = allocateBuffer(256);
        try {
            Unsafe.getUnsafe().putByte(buf, (byte) 0x89);  // PING
            Unsafe.getUnsafe().putByte(buf + 1, (byte) 125);
            for (int i = 0; i < 125; i++) {
                Unsafe.getUnsafe().putByte(buf + 2 + i, (byte) i);
            }

            WebSocketFrameParser parser = new WebSocketFrameParser();
            int consumed = parser.parse(buf, buf + 127);

            Assert.assertEquals(127, consumed);
            Assert.assertEquals(125, parser.getPayloadLength());
            Assert.assertNotEquals(WebSocketFrameParser.STATE_ERROR, parser.getState());
        } finally {
            freeBuffer(buf, 256);
        }
    }

    @Test
    public void testParseEmptyBuffer() {
        long buf = allocateBuffer(16);
        try {
            WebSocketFrameParser parser = new WebSocketFrameParser();
            int consumed = parser.parse(buf, buf);  // Empty range

            Assert.assertEquals(0, consumed);
        } finally {
            freeBuffer(buf, 16);
        }
    }

    @Test
    public void testReset() {
        long buf = allocateBuffer(16);
        try {
            writeBytes(buf, (byte) 0x82, (byte) 0x02, (byte) 0x01, (byte) 0x02);

            WebSocketFrameParser parser = new WebSocketFrameParser();
            parser.parse(buf, buf + 4);

            Assert.assertEquals(WebSocketOpcode.BINARY, parser.getOpcode());
            Assert.assertEquals(2, parser.getPayloadLength());

            parser.reset();

            Assert.assertEquals(0, parser.getOpcode());
            Assert.assertEquals(0, parser.getPayloadLength());
            Assert.assertEquals(WebSocketFrameParser.STATE_HEADER, parser.getState());
        } finally {
            freeBuffer(buf, 16);
        }
    }

    // ==================== CLOSE FRAME PARSING ====================

    @Test
    public void testParseCloseFrameWithReason() {
        long buf = allocateBuffer(64);
        try {
            String reason = "Normal closure";
            byte[] reasonBytes = reason.getBytes(StandardCharsets.UTF_8);

            Unsafe.getUnsafe().putByte(buf, (byte) 0x88);  // CLOSE
            Unsafe.getUnsafe().putByte(buf + 1, (byte) (2 + reasonBytes.length));
            Unsafe.getUnsafe().putShort(buf + 2, Short.reverseBytes((short) 1000));
            for (int i = 0; i < reasonBytes.length; i++) {
                Unsafe.getUnsafe().putByte(buf + 4 + i, reasonBytes[i]);
            }

            WebSocketFrameParser parser = new WebSocketFrameParser();
            parser.parse(buf, buf + 4 + reasonBytes.length);

            Assert.assertEquals(WebSocketOpcode.CLOSE, parser.getOpcode());
            Assert.assertEquals(2 + reasonBytes.length, parser.getPayloadLength());
        } finally {
            freeBuffer(buf, 64);
        }
    }

    @Test
    public void testParseCloseFrameEmpty() {
        // Close frame with no body is valid
        long buf = allocateBuffer(16);
        try {
            writeBytes(buf, (byte) 0x88, (byte) 0x00);

            WebSocketFrameParser parser = new WebSocketFrameParser();
            parser.parse(buf, buf + 2);

            Assert.assertEquals(WebSocketOpcode.CLOSE, parser.getOpcode());
            Assert.assertEquals(0, parser.getPayloadLength());
        } finally {
            freeBuffer(buf, 16);
        }
    }

    @Test
    public void testRejectCloseFrameWith1BytePayload() {
        // Close frame payload must be 0 or >= 2 bytes (for status code)
        long buf = allocateBuffer(16);
        try {
            writeBytes(buf, (byte) 0x88, (byte) 0x01, (byte) 0x00);

            WebSocketFrameParser parser = new WebSocketFrameParser();
            parser.parse(buf, buf + 3);

            Assert.assertEquals(WebSocketFrameParser.STATE_ERROR, parser.getState());
            Assert.assertEquals(WebSocketCloseCode.PROTOCOL_ERROR, parser.getErrorCode());
        } finally {
            freeBuffer(buf, 16);
        }
    }

    // ==================== OPCODE UTILITY TESTS ====================

    @Test
    public void testOpcodeIsControlFrame() {
        Assert.assertFalse(WebSocketOpcode.isControlFrame(WebSocketOpcode.CONTINUATION));
        Assert.assertFalse(WebSocketOpcode.isControlFrame(WebSocketOpcode.TEXT));
        Assert.assertFalse(WebSocketOpcode.isControlFrame(WebSocketOpcode.BINARY));
        Assert.assertTrue(WebSocketOpcode.isControlFrame(WebSocketOpcode.CLOSE));
        Assert.assertTrue(WebSocketOpcode.isControlFrame(WebSocketOpcode.PING));
        Assert.assertTrue(WebSocketOpcode.isControlFrame(WebSocketOpcode.PONG));
    }

    @Test
    public void testOpcodeIsDataFrame() {
        Assert.assertTrue(WebSocketOpcode.isDataFrame(WebSocketOpcode.CONTINUATION));
        Assert.assertTrue(WebSocketOpcode.isDataFrame(WebSocketOpcode.TEXT));
        Assert.assertTrue(WebSocketOpcode.isDataFrame(WebSocketOpcode.BINARY));
        Assert.assertFalse(WebSocketOpcode.isDataFrame(WebSocketOpcode.CLOSE));
        Assert.assertFalse(WebSocketOpcode.isDataFrame(WebSocketOpcode.PING));
        Assert.assertFalse(WebSocketOpcode.isDataFrame(WebSocketOpcode.PONG));
    }

    @Test
    public void testOpcodeIsValid() {
        Assert.assertTrue(WebSocketOpcode.isValid(WebSocketOpcode.CONTINUATION));
        Assert.assertTrue(WebSocketOpcode.isValid(WebSocketOpcode.TEXT));
        Assert.assertTrue(WebSocketOpcode.isValid(WebSocketOpcode.BINARY));
        Assert.assertFalse(WebSocketOpcode.isValid(3));
        Assert.assertFalse(WebSocketOpcode.isValid(4));
        Assert.assertFalse(WebSocketOpcode.isValid(5));
        Assert.assertFalse(WebSocketOpcode.isValid(6));
        Assert.assertFalse(WebSocketOpcode.isValid(7));
        Assert.assertTrue(WebSocketOpcode.isValid(WebSocketOpcode.CLOSE));
        Assert.assertTrue(WebSocketOpcode.isValid(WebSocketOpcode.PING));
        Assert.assertTrue(WebSocketOpcode.isValid(WebSocketOpcode.PONG));
        Assert.assertFalse(WebSocketOpcode.isValid(0xB));
        Assert.assertFalse(WebSocketOpcode.isValid(0xF));
    }
}
