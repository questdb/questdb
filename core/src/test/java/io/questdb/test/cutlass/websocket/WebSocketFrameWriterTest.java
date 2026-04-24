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

import io.questdb.cutlass.qwp.websocket.WebSocketFrameWriter;
import io.questdb.cutlass.qwp.websocket.WebSocketOpcode;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

/**
 * Comprehensive tests for WebSocket frame writing.
 */
public class WebSocketFrameWriterTest extends AbstractWebSocketTest {

    @Test
    public void testAllOpcodes() throws Exception {
        assertMemoryLeak(() -> {
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
        });
    }

    @Test
    public void testHeaderSizeCalculation() {
        Assert.assertEquals(2, WebSocketFrameWriter.headerSize(0, false));
        Assert.assertEquals(2, WebSocketFrameWriter.headerSize(125, false));
        Assert.assertEquals(4, WebSocketFrameWriter.headerSize(126, false));
        Assert.assertEquals(4, WebSocketFrameWriter.headerSize(65_535, false));
        Assert.assertEquals(10, WebSocketFrameWriter.headerSize(65_536, false));
        Assert.assertEquals(10, WebSocketFrameWriter.headerSize(1_000_000, false));

        // With masking
        Assert.assertEquals(6, WebSocketFrameWriter.headerSize(0, true));
        Assert.assertEquals(6, WebSocketFrameWriter.headerSize(125, true));
        Assert.assertEquals(8, WebSocketFrameWriter.headerSize(126, true));
        Assert.assertEquals(14, WebSocketFrameWriter.headerSize(65_536, true));
    }

    @Test
    public void testWrite16BitLengthBoundary() throws Exception {
        assertMemoryLeak(() -> {
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
        });
    }

    @Test
    public void testWrite64BitLengthBoundary() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                // Test boundary at 65535/65536
                int written65535 = WebSocketFrameWriter.writeHeader(buf, true,
                        WebSocketOpcode.BINARY, 65_535, false);
                Assert.assertEquals(4, written65535);

                int written65536 = WebSocketFrameWriter.writeHeader(buf, true,
                        WebSocketOpcode.BINARY, 65_536, false);
                Assert.assertEquals(10, written65536);
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testWriteCloseFrame() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                WebSocketFrameWriter.writeHeader(buf, true, WebSocketOpcode.CLOSE, 2, false);

                Assert.assertEquals((byte) 0x88, Unsafe.getUnsafe().getByte(buf));
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testWriteCloseFrameRejectsSmallBuffer() throws Exception {
        assertMemoryLeak(() -> {
            // A close frame with null reason needs 4 bytes (2 header + 2 status code).
            // Verify that the bounds-checking overload refuses to write and does not
            // touch memory when the buffer is too small.
            int totalSize = 16;
            int bufferSize = 2; // too small for a 4-byte close frame
            long buf = allocateBuffer(totalSize);
            try {
                // Fill entire region with sentinel
                for (int i = 0; i < totalSize; i++) {
                    Unsafe.getUnsafe().putByte(buf + i, (byte) 0xAA);
                }

                int written = WebSocketFrameWriter.writeCloseFrame(buf, bufferSize, 1000);
                Assert.assertEquals(-1, written);

                // No bytes should have been touched
                for (int i = 0; i < totalSize; i++) {
                    Assert.assertEquals(
                            "writeCloseFrame wrote to buffer at offset " + i,
                            (byte) 0xAA,
                            Unsafe.getUnsafe().getByte(buf + i)
                    );
                }
            } finally {
                freeBuffer(buf, totalSize);
            }
        });
    }

    @Test
    public void testWriteCloseFrameWithoutReason() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(32);
            try {
                int totalLen = WebSocketFrameWriter.writeCloseFrame(buf, Integer.MAX_VALUE, 1000);

                // Should have header (2) + code (2) = 4
                Assert.assertEquals(4, totalLen);

                // Verify the code
                short code = Short.reverseBytes(Unsafe.getUnsafe().getShort(buf + 2));
                Assert.assertEquals(1000, code);
            } finally {
                freeBuffer(buf, 32);
            }
        });
    }

    @Test
    public void testWriteContinuationFrame() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                WebSocketFrameWriter.writeHeader(buf, false, WebSocketOpcode.CONTINUATION, 5, false);

                Assert.assertEquals((byte) 0x00, Unsafe.getUnsafe().getByte(buf));
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testWriteFinalContinuationFrame() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                WebSocketFrameWriter.writeHeader(buf, true, WebSocketOpcode.CONTINUATION, 5, false);

                Assert.assertEquals((byte) 0x80, Unsafe.getUnsafe().getByte(buf));
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testWriteLargeFrame() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                long payloadLen = 100_000L;
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
        });
    }

    @Test
    public void testWriteMediumFrame() throws Exception {
        assertMemoryLeak(() -> {
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
        });
    }

    @Test
    public void testWriteNonFinalFrame() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                WebSocketFrameWriter.writeHeader(buf, false, WebSocketOpcode.BINARY, 5, false);

                Assert.assertEquals((byte) 0x02, Unsafe.getUnsafe().getByte(buf));  // No FIN bit
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testWritePingFrame() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                WebSocketFrameWriter.writeHeader(buf, true, WebSocketOpcode.PING, 4, false);

                Assert.assertEquals((byte) 0x89, Unsafe.getUnsafe().getByte(buf));
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testWritePingFrameComplete() throws Exception {
        assertMemoryLeak(() -> {
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
        });
    }

    @Test
    public void testWritePongFrame() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                WebSocketFrameWriter.writeHeader(buf, true, WebSocketOpcode.PONG, 4, false);

                Assert.assertEquals((byte) 0x8A, Unsafe.getUnsafe().getByte(buf));
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testWritePongFrameFromMemory() throws Exception {
        assertMemoryLeak(() -> {
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
        });
    }

    @Test
    public void testWriteSmallFrame() throws Exception {
        assertMemoryLeak(() -> {
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
        });
    }

    @Test
    public void testWriteTextFrame() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                WebSocketFrameWriter.writeHeader(buf, true, WebSocketOpcode.TEXT, 10, false);

                Assert.assertEquals((byte) 0x81, Unsafe.getUnsafe().getByte(buf));
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }
}
