/*+*****************************************************************************
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

package io.questdb.test.cutlass.websocket;

import io.questdb.cutlass.qwp.websocket.WebSocketCloseCode;
import io.questdb.cutlass.qwp.websocket.WebSocketFrameParser;
import io.questdb.cutlass.qwp.websocket.WebSocketOpcode;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Random;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

/**
 * Tests for the server-side WebSocket frame parser.
 * The server parser expects masked frames (from clients)
 * and rejects unmasked frames.
 */
public class WebSocketFrameParserTest extends AbstractWebSocketTest {

    private static final byte[] MASK_KEY = {0x12, 0x34, 0x56, 0x78};

    @Test
    public void testControlFrameBetweenFragments() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(64);
            try {
                WebSocketFrameParser parser = new WebSocketFrameParser();

                // First data fragment (masked)
                byte[] frame1 = createMaskedFrame(WebSocketOpcode.TEXT, new byte[]{'H', 'i'}, false);
                writeBytes(buf, frame1);
                parser.parse(buf, buf + frame1.length);
                Assert.assertFalse(parser.isFin());
                Assert.assertEquals(WebSocketOpcode.TEXT, parser.getOpcode());

                // Ping in the middle (control frame, FIN must be 1, masked)
                parser.reset();
                byte[] frame2 = createMaskedFrame(WebSocketOpcode.PING, new byte[0], true);
                writeBytes(buf, frame2);
                parser.parse(buf, buf + frame2.length);
                Assert.assertTrue(parser.isFin());
                Assert.assertEquals(WebSocketOpcode.PING, parser.getOpcode());

                // Final data fragment (masked)
                parser.reset();
                byte[] frame3 = createMaskedFrame(WebSocketOpcode.CONTINUATION, new byte[]{'!'}, true);
                writeBytes(buf, frame3);
                parser.parse(buf, buf + frame3.length);
                Assert.assertTrue(parser.isFin());
                Assert.assertEquals(WebSocketOpcode.CONTINUATION, parser.getOpcode());
            } finally {
                freeBuffer(buf, 64);
            }
        });
    }

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

    @Test
    public void testParse16BitLength() throws Exception {
        assertMemoryLeak(() -> {
            int payloadLen = 1000;
            long buf = allocateBuffer(payloadLen + 16);
            try {
                byte[] payload = new byte[payloadLen];
                byte[] frame = createMaskedFrame(WebSocketOpcode.BINARY, payload);
                writeBytes(buf, frame);

                WebSocketFrameParser parser = new WebSocketFrameParser();
                int consumed = parser.parse(buf, buf + frame.length);

                Assert.assertEquals(frame.length, consumed);
                Assert.assertEquals(payloadLen, parser.getPayloadLength());
            } finally {
                freeBuffer(buf, payloadLen + 16);
            }
        });
    }

    @Test
    public void testParse64BitLength() throws Exception {
        assertMemoryLeak(() -> {
            int payloadLen = 70_000;
            long buf = allocateBuffer(payloadLen + 16);
            try {
                byte[] payload = new byte[payloadLen];
                byte[] frame = createMaskedFrame(WebSocketOpcode.BINARY, payload);
                writeBytes(buf, frame);

                WebSocketFrameParser parser = new WebSocketFrameParser();
                int consumed = parser.parse(buf, buf + frame.length);

                Assert.assertEquals(frame.length, consumed);
                Assert.assertEquals(payloadLen, parser.getPayloadLength());
            } finally {
                freeBuffer(buf, payloadLen + 16);
            }
        });
    }

    @Test
    public void testParse7BitLength() throws Exception {
        assertMemoryLeak(() -> {
            for (int len = 0; len <= 125; len++) {
                long buf = allocateBuffer(256);
                try {
                    byte[] payload = new byte[len];
                    for (int i = 0; i < len; i++) {
                        payload[i] = (byte) i;
                    }
                    byte[] frame = createMaskedFrame(WebSocketOpcode.BINARY, payload);
                    writeBytes(buf, frame);

                    WebSocketFrameParser parser = new WebSocketFrameParser();
                    int consumed = parser.parse(buf, buf + frame.length);

                    Assert.assertEquals(frame.length, consumed);
                    Assert.assertEquals(len, parser.getPayloadLength());
                } finally {
                    freeBuffer(buf, 256);
                }
            }
        });
    }

    @Test
    public void testParseBinaryFrame() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                byte[] frame = createMaskedFrame(WebSocketOpcode.BINARY, new byte[0]);
                writeBytes(buf, frame);

                WebSocketFrameParser parser = new WebSocketFrameParser();
                parser.parse(buf, buf + frame.length);

                Assert.assertEquals(WebSocketOpcode.BINARY, parser.getOpcode());
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testParseCloseFrame() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                // Close with code 1000 (normal closure)
                byte[] payload = {(byte) 0x03, (byte) 0xE8};
                byte[] frame = createMaskedFrame(WebSocketOpcode.CLOSE, payload);
                writeBytes(buf, frame);

                WebSocketFrameParser parser = new WebSocketFrameParser();
                parser.parse(buf, buf + frame.length);

                Assert.assertEquals(WebSocketOpcode.CLOSE, parser.getOpcode());
                Assert.assertEquals(2, parser.getPayloadLength());
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testParseCloseFrameEmpty() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                byte[] frame = createMaskedFrame(WebSocketOpcode.CLOSE, new byte[0]);
                writeBytes(buf, frame);

                WebSocketFrameParser parser = new WebSocketFrameParser();
                parser.parse(buf, buf + frame.length);

                Assert.assertEquals(WebSocketOpcode.CLOSE, parser.getOpcode());
                Assert.assertEquals(0, parser.getPayloadLength());
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testParseCloseFrameWithReason() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(64);
            try {
                String reason = "Normal closure";
                byte[] reasonBytes = reason.getBytes(StandardCharsets.UTF_8);
                byte[] payload = new byte[2 + reasonBytes.length];
                payload[0] = (byte) 0x03;
                payload[1] = (byte) 0xE8;  // 1000 in big-endian
                System.arraycopy(reasonBytes, 0, payload, 2, reasonBytes.length);

                byte[] frame = createMaskedFrame(WebSocketOpcode.CLOSE, payload);
                writeBytes(buf, frame);

                WebSocketFrameParser parser = new WebSocketFrameParser();
                parser.parse(buf, buf + frame.length);

                Assert.assertEquals(WebSocketOpcode.CLOSE, parser.getOpcode());
                Assert.assertEquals(2 + reasonBytes.length, parser.getPayloadLength());
            } finally {
                freeBuffer(buf, 64);
            }
        });
    }

    @Test
    public void testParseContinuationFrame() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                byte[] frame = createMaskedFrame(WebSocketOpcode.CONTINUATION,
                        new byte[]{0x01, 0x02, 0x03, 0x04, 0x05}, false);
                writeBytes(buf, frame);

                WebSocketFrameParser parser = new WebSocketFrameParser();
                parser.parse(buf, buf + frame.length);

                Assert.assertEquals(WebSocketOpcode.CONTINUATION, parser.getOpcode());
                Assert.assertFalse(parser.isFin());
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testParseEmptyBuffer() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                WebSocketFrameParser parser = new WebSocketFrameParser();
                int consumed = parser.parse(buf, buf);

                Assert.assertEquals(0, consumed);
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testParseEmptyPayload() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                byte[] frame = createMaskedFrame(WebSocketOpcode.BINARY, new byte[0]);
                writeBytes(buf, frame);

                WebSocketFrameParser parser = new WebSocketFrameParser();
                int consumed = parser.parse(buf, buf + frame.length);

                Assert.assertEquals(frame.length, consumed);
                Assert.assertEquals(0, parser.getPayloadLength());
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testParseFragmentedMessage() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(64);
            try {
                WebSocketFrameParser parser = new WebSocketFrameParser();

                // First fragment: opcode=TEXT, FIN=0
                byte[] frame1 = createMaskedFrame(WebSocketOpcode.TEXT, new byte[]{'H', 'e', 'l'}, false);
                writeBytes(buf, frame1);
                int consumed = parser.parse(buf, buf + frame1.length);

                Assert.assertEquals(frame1.length, consumed);
                Assert.assertFalse(parser.isFin());
                Assert.assertEquals(WebSocketOpcode.TEXT, parser.getOpcode());

                // Continuation: opcode=CONTINUATION, FIN=0
                parser.reset();
                byte[] frame2 = createMaskedFrame(WebSocketOpcode.CONTINUATION, new byte[]{'l', 'o'}, false);
                writeBytes(buf, frame2);
                consumed = parser.parse(buf, buf + frame2.length);

                Assert.assertEquals(frame2.length, consumed);
                Assert.assertFalse(parser.isFin());
                Assert.assertEquals(WebSocketOpcode.CONTINUATION, parser.getOpcode());

                // Final fragment: opcode=CONTINUATION, FIN=1
                parser.reset();
                byte[] frame3 = createMaskedFrame(WebSocketOpcode.CONTINUATION, new byte[]{'!'}, true);
                writeBytes(buf, frame3);
                consumed = parser.parse(buf, buf + frame3.length);

                Assert.assertEquals(frame3.length, consumed);
                Assert.assertTrue(parser.isFin());
                Assert.assertEquals(WebSocketOpcode.CONTINUATION, parser.getOpcode());
            } finally {
                freeBuffer(buf, 64);
            }
        });
    }

    @Test
    public void testParseIncompleteHeader16BitLength() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                // Masked frame with 16-bit length, but only provide 3 bytes (missing 2nd length byte)
                writeBytes(buf, (byte) 0x82, (byte) (0x80 | 126), (byte) 0x01);

                WebSocketFrameParser parser = new WebSocketFrameParser();
                int consumed = parser.parse(buf, buf + 3);

                Assert.assertEquals(0, consumed);
                Assert.assertEquals(WebSocketFrameParser.STATE_NEED_MORE, parser.getState());
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testParseIncompleteHeader1Byte() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                writeBytes(buf, (byte) 0x82);

                WebSocketFrameParser parser = new WebSocketFrameParser();
                int consumed = parser.parse(buf, buf + 1);

                Assert.assertEquals(0, consumed);
                Assert.assertEquals(WebSocketFrameParser.STATE_NEED_MORE, parser.getState());
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testParseIncompleteHeader64BitLength() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                // Masked frame with 64-bit length, but only provide 6 bytes (missing length bytes)
                writeBytes(buf, (byte) 0x82, (byte) (0x80 | 127), (byte) 0, (byte) 0, (byte) 0, (byte) 0);

                WebSocketFrameParser parser = new WebSocketFrameParser();
                int consumed = parser.parse(buf, buf + 6);

                Assert.assertEquals(0, consumed);
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testParseIncompleteMaskKey() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                writeBytes(buf, (byte) 0x82, (byte) 0x85, (byte) 0x12, (byte) 0x34);  // Only 2 of 4 mask bytes

                WebSocketFrameParser parser = new WebSocketFrameParser();
                int consumed = parser.parse(buf, buf + 4);

                Assert.assertEquals(0, consumed);
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testParseIncompletePayload() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                // Masked frame: FIN + BINARY, MASK + length 5, mask key, but only 2 payload bytes
                writeBytes(buf, (byte) 0x82, (byte) 0x85,
                        MASK_KEY[0], MASK_KEY[1], MASK_KEY[2], MASK_KEY[3],
                        (byte) 0x01, (byte) 0x02);

                WebSocketFrameParser parser = new WebSocketFrameParser();
                int consumed = parser.parse(buf, buf + 8);

                Assert.assertEquals(6, consumed);  // header (2) + mask key (4)
                Assert.assertEquals(5, parser.getPayloadLength());
                Assert.assertEquals(WebSocketFrameParser.STATE_NEED_PAYLOAD, parser.getState());
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testParseMaskedFrame() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(32);
            try {
                byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
                byte[] frame = createMaskedFrame(WebSocketOpcode.BINARY, payload, true, MASK_KEY);
                writeBytes(buf, frame);

                WebSocketFrameParser parser = new WebSocketFrameParser();
                int consumed = parser.parse(buf, buf + frame.length);

                Assert.assertEquals(frame.length, consumed);
                Assert.assertTrue(parser.isMasked());
                Assert.assertEquals(0x12345678, parser.getMaskKey());
            } finally {
                freeBuffer(buf, 32);
            }
        });
    }

    @Test
    public void testParseMaxControlFrameSize() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(256);
            try {
                byte[] payload = new byte[125];
                for (int i = 0; i < 125; i++) {
                    payload[i] = (byte) i;
                }
                byte[] frame = createMaskedFrame(WebSocketOpcode.PING, payload);
                writeBytes(buf, frame);

                WebSocketFrameParser parser = new WebSocketFrameParser();
                int consumed = parser.parse(buf, buf + frame.length);

                Assert.assertEquals(frame.length, consumed);
                Assert.assertEquals(125, parser.getPayloadLength());
                Assert.assertNotEquals(WebSocketFrameParser.STATE_ERROR, parser.getState());
            } finally {
                freeBuffer(buf, 256);
            }
        });
    }

    @Test
    public void testParseMultipleFramesInBuffer() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(64);
            try {
                byte[] frame1 = createMaskedFrame(WebSocketOpcode.BINARY, new byte[]{0x01, 0x02});
                byte[] frame2 = createMaskedFrame(WebSocketOpcode.TEXT, new byte[]{'a', 'b', 'c'});

                writeBytes(buf, frame1);
                for (int i = 0; i < frame2.length; i++) {
                    Unsafe.getUnsafe().putByte(buf + frame1.length + i, frame2[i]);
                }

                WebSocketFrameParser parser = new WebSocketFrameParser();

                int consumed = parser.parse(buf, buf + frame1.length + frame2.length);
                Assert.assertEquals(frame1.length, consumed);
                Assert.assertEquals(WebSocketOpcode.BINARY, parser.getOpcode());
                Assert.assertEquals(2, parser.getPayloadLength());

                parser.reset();
                consumed = parser.parse(buf + frame1.length, buf + frame1.length + frame2.length);
                Assert.assertEquals(frame2.length, consumed);
                Assert.assertEquals(WebSocketOpcode.TEXT, parser.getOpcode());
                Assert.assertEquals(3, parser.getPayloadLength());
            } finally {
                freeBuffer(buf, 64);
            }
        });
    }

    @Test
    public void testParsePingFrame() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                byte[] frame = createMaskedFrame(WebSocketOpcode.PING, new byte[]{0x01, 0x02, 0x03, 0x04});
                writeBytes(buf, frame);

                WebSocketFrameParser parser = new WebSocketFrameParser();
                parser.parse(buf, buf + frame.length);

                Assert.assertEquals(WebSocketOpcode.PING, parser.getOpcode());
                Assert.assertEquals(4, parser.getPayloadLength());
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testParsePongFrame() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                byte[] frame = createMaskedFrame(WebSocketOpcode.PONG, new byte[]{0x01, 0x02, 0x03, 0x04});
                writeBytes(buf, frame);

                WebSocketFrameParser parser = new WebSocketFrameParser();
                parser.parse(buf, buf + frame.length);

                Assert.assertEquals(WebSocketOpcode.PONG, parser.getOpcode());
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testParseTextFrame() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                byte[] frame = createMaskedFrame(WebSocketOpcode.TEXT, new byte[0]);
                writeBytes(buf, frame);

                WebSocketFrameParser parser = new WebSocketFrameParser();
                parser.parse(buf, buf + frame.length);

                Assert.assertEquals(WebSocketOpcode.TEXT, parser.getOpcode());
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testRejectCloseFrameWith1BytePayload() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                byte[] frame = createMaskedFrame(WebSocketOpcode.CLOSE, new byte[]{0x00});
                writeBytes(buf, frame);

                WebSocketFrameParser parser = new WebSocketFrameParser();
                parser.parse(buf, buf + frame.length);

                Assert.assertEquals(WebSocketFrameParser.STATE_ERROR, parser.getState());
                Assert.assertEquals(WebSocketCloseCode.PROTOCOL_ERROR, parser.getErrorCode());
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testRejectFragmentedControlFrame() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                // Ping with FIN=0 (fragmented control frame - not allowed)
                // RSV/opcode check and control-frame-fragmentation check happen before mask check
                writeBytes(buf, (byte) 0x09, (byte) 0x80);

                WebSocketFrameParser parser = new WebSocketFrameParser();
                parser.parse(buf, buf + 2);

                Assert.assertEquals(WebSocketFrameParser.STATE_ERROR, parser.getState());
                Assert.assertEquals(WebSocketCloseCode.PROTOCOL_ERROR, parser.getErrorCode());
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testRejectOversizeControlFrame() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(256);
            try {
                // Masked ping with 126-byte payload (> 125 limit for control frames)
                // byte0: FIN + PING, byte1: MASK + 126 (16-bit length), 2 length bytes, 4 mask bytes
                writeBytes(buf,
                        (byte) 0x89,           // FIN + PING
                        (byte) (0x80 | 126),   // MASK + 16-bit length follows
                        (byte) 0x00, (byte) 0x7E,  // 126 in big-endian
                        MASK_KEY[0], MASK_KEY[1], MASK_KEY[2], MASK_KEY[3]
                );

                WebSocketFrameParser parser = new WebSocketFrameParser();
                parser.parse(buf, buf + 8);

                Assert.assertEquals(WebSocketFrameParser.STATE_ERROR, parser.getState());
                Assert.assertEquals(WebSocketCloseCode.PROTOCOL_ERROR, parser.getErrorCode());
            } finally {
                freeBuffer(buf, 256);
            }
        });
    }

    @Test
    public void testRejectRSV2Bit() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                writeBytes(buf, (byte) 0xA2, (byte) 0x80);

                WebSocketFrameParser parser = new WebSocketFrameParser();
                parser.parse(buf, buf + 2);

                Assert.assertEquals(WebSocketFrameParser.STATE_ERROR, parser.getState());
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testRejectRSV3Bit() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                writeBytes(buf, (byte) 0x92, (byte) 0x80);

                WebSocketFrameParser parser = new WebSocketFrameParser();
                parser.parse(buf, buf + 2);

                Assert.assertEquals(WebSocketFrameParser.STATE_ERROR, parser.getState());
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testRejectReservedBits() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                writeBytes(buf, (byte) 0xC2, (byte) 0x80);

                WebSocketFrameParser parser = new WebSocketFrameParser();
                parser.parse(buf, buf + 2);

                Assert.assertEquals(WebSocketFrameParser.STATE_ERROR, parser.getState());
                Assert.assertEquals(WebSocketCloseCode.PROTOCOL_ERROR, parser.getErrorCode());
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testRejectUnknownOpcode() throws Exception {
        assertMemoryLeak(() -> {
            for (int opcode : new int[]{3, 4, 5, 6, 7, 0xB, 0xC, 0xD, 0xE, 0xF}) {
                long buf = allocateBuffer(16);
                try {
                    writeBytes(buf, (byte) (0x80 | opcode), (byte) 0x80);

                    WebSocketFrameParser parser = new WebSocketFrameParser();
                    parser.parse(buf, buf + 2);

                    Assert.assertEquals("Opcode " + opcode + " should be rejected",
                            WebSocketFrameParser.STATE_ERROR, parser.getState());
                } finally {
                    freeBuffer(buf, 16);
                }
            }
        });
    }

    @Test
    public void testRejectUnmaskedClientFrame() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                writeBytes(buf, (byte) 0x82, (byte) 0x01, (byte) 0xFF);  // No mask bit

                WebSocketFrameParser parser = new WebSocketFrameParser();
                parser.parse(buf, buf + 3);

                Assert.assertEquals(WebSocketFrameParser.STATE_ERROR, parser.getState());
                Assert.assertEquals(WebSocketCloseCode.PROTOCOL_ERROR, parser.getErrorCode());
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testRejectZeroLengthIn16BitField() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                // Masked frame with non-minimal 16-bit length encoding (value 5)
                writeBytes(buf,
                        (byte) 0x82,           // FIN + BINARY
                        (byte) (0x80 | 126),   // MASK + 16-bit length
                        (byte) 0x00, (byte) 0x05,  // Length 5 in 16-bit
                        MASK_KEY[0], MASK_KEY[1], MASK_KEY[2], MASK_KEY[3]
                );

                WebSocketFrameParser parser = new WebSocketFrameParser();
                parser.setStrictMode(true);
                parser.parse(buf, buf + 8);

                Assert.assertEquals(WebSocketFrameParser.STATE_ERROR, parser.getState());
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testReset() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                byte[] frame = createMaskedFrame(WebSocketOpcode.BINARY, new byte[]{0x01, 0x02});
                writeBytes(buf, frame);

                WebSocketFrameParser parser = new WebSocketFrameParser();
                parser.parse(buf, buf + frame.length);

                Assert.assertEquals(WebSocketOpcode.BINARY, parser.getOpcode());
                Assert.assertEquals(2, parser.getPayloadLength());

                parser.reset();

                Assert.assertEquals(0, parser.getOpcode());
                Assert.assertEquals(0, parser.getPayloadLength());
                Assert.assertEquals(WebSocketFrameParser.STATE_HEADER, parser.getState());
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testUnmaskLargePayload() throws Exception {
        assertMemoryLeak(() -> {
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
                parser.setMaskKey(0x11223344);
                parser.unmaskPayload(buf, len);

                byte[] result = readBytes(buf, len);
                Assert.assertArrayEquals(original, result);
            } finally {
                freeBuffer(buf, len);
            }
        });
    }

    @Test
    public void testUnmaskPayload() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(32);
            try {
                byte[] original = "hello".getBytes(StandardCharsets.UTF_8);
                byte[] maskKey = {0x12, 0x34, 0x56, 0x78};

                for (int i = 0; i < original.length; i++) {
                    Unsafe.getUnsafe().putByte(buf + i,
                            (byte) (original[i] ^ maskKey[i % 4]));
                }

                WebSocketFrameParser parser = new WebSocketFrameParser();
                parser.setMaskKey(0x12345678);
                parser.unmaskPayload(buf, original.length);

                byte[] result = readBytes(buf, original.length);
                Assert.assertArrayEquals(original, result);
            } finally {
                freeBuffer(buf, 32);
            }
        });
    }

    @Test
    public void testUnmaskPayloadUnaligned() throws Exception {
        assertMemoryLeak(() -> {
            for (int len = 1; len <= 20; len++) {
                long buf = allocateBuffer(32);
                try {
                    byte[] original = new byte[len];
                    for (int i = 0; i < len; i++) original[i] = (byte) (i + 1);
                    byte[] maskKey = {0x37, 0x42, (byte) 0xAB, (byte) 0xCD};

                    for (int i = 0; i < len; i++) {
                        Unsafe.getUnsafe().putByte(buf + i,
                                (byte) (original[i] ^ maskKey[i % 4]));
                    }

                    WebSocketFrameParser parser = new WebSocketFrameParser();
                    parser.setMaskKey(0x3742ABCD);
                    parser.unmaskPayload(buf, len);

                    byte[] result = readBytes(buf, len);
                    Assert.assertArrayEquals("Failed for length " + len, original, result);
                } finally {
                    freeBuffer(buf, 32);
                }
            }
        });
    }

}
