# WebSocket Support for ILPv4 Streaming

## Overview

This document describes the design for adding WebSocket support to QuestDB for streaming ILPv4 binary data.

**Development Philosophy**: Test-Driven Development (TDD) is mandatory. Every component must have comprehensive tests before implementation. No code merges without full test coverage.

### Core Principles

1. **Zero allocation on hot paths** - Pre-allocated buffers, object pools
2. **No third-party production dependencies** - Custom WebSocket implementation
3. **Tests first, always** - Write tests before implementation
4. **Third-party test clients allowed** - Use Java-WebSocket for integration tests

---

## Test Infrastructure Setup (Phase 0)

Before any implementation, set up test infrastructure.

### Test Dependencies

```xml
<!-- pom.xml - TEST SCOPE ONLY -->
<dependency>
    <groupId>org.java-websocket</groupId>
    <artifactId>Java-WebSocket</artifactId>
    <version>1.5.4</version>
    <scope>test</scope>
</dependency>
```

### Test Base Classes

```java
/**
 * Base class for WebSocket unit tests.
 * Provides memory management utilities for direct buffer testing.
 */
public abstract class AbstractWebSocketTest extends AbstractTest {

    protected long allocateBuffer(int size) {
        return Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
    }

    protected void freeBuffer(long address, int size) {
        Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
    }

    protected void writeBytes(long address, byte... bytes) {
        for (int i = 0; i < bytes.length; i++) {
            Unsafe.getUnsafe().putByte(address + i, bytes[i]);
        }
    }

    protected byte[] readBytes(long address, int length) {
        byte[] result = new byte[length];
        for (int i = 0; i < length; i++) {
            result[i] = Unsafe.getUnsafe().getByte(address + i);
        }
        return result;
    }
}

/**
 * Base class for WebSocket integration tests with real server.
 */
public abstract class AbstractWebSocketIntegrationTest extends AbstractBootstrapTest {

    protected WebSocketClient connectClient(int port, String path) throws Exception {
        return connectClient(port, path, new WebSocketClientAdapter());
    }

    protected WebSocketClient connectClient(int port, String path,
                                             WebSocketClientListener listener) throws Exception {
        URI uri = new URI("ws://localhost:" + port + path);
        TestWebSocketClient client = new TestWebSocketClient(uri, listener);
        client.connectBlocking(5, TimeUnit.SECONDS);
        return client;
    }
}
```

---

## Phase 1: WebSocket Frame Parser

### 1.1 Write Tests First

**File**: `core/src/test/java/io/questdb/test/cutlass/http/websocket/WebSocketFrameParserTest.java`

```java
package io.questdb.test.cutlass.http.websocket;

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
            int consumed = parser.parse(buf, buf + 6 + payload.length);

            Assert.assertEquals(6 + payload.length, consumed);
            Assert.assertTrue(parser.isMasked());
            Assert.assertEquals(0x12345678, parser.getMaskKey());
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
            parser.setMaskKey(0x12345678);
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
                parser.setMaskKey(0x3742ABCD);
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
            parser.setMaskKey(0x11223344);
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
            writeBytes(buf, (byte) 0x89, (byte) 0x04, 0x01, 0x02, 0x03, 0x04);

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
            writeBytes(buf, (byte) 0x8A, (byte) 0x04, 0x01, 0x02, 0x03, 0x04);

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
            writeBytes(buf, (byte) 0x00, (byte) 0x05, 0x01, 0x02, 0x03, 0x04, 0x05);

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
            writeBytes(buf, (byte) 0x01, (byte) 0x03, 'H', 'e', 'l');

            WebSocketFrameParser parser = new WebSocketFrameParser();
            int consumed = parser.parse(buf, buf + 5);

            Assert.assertEquals(5, consumed);
            Assert.assertFalse(parser.isFin());
            Assert.assertEquals(WebSocketOpcode.TEXT, parser.getOpcode());

            // Continuation: opcode=CONTINUATION, FIN=0
            parser.reset();
            writeBytes(buf, (byte) 0x00, (byte) 0x02, 'l', 'o');
            consumed = parser.parse(buf, buf + 4);

            Assert.assertEquals(4, consumed);
            Assert.assertFalse(parser.isFin());
            Assert.assertEquals(WebSocketOpcode.CONTINUATION, parser.getOpcode());

            // Final fragment: opcode=CONTINUATION, FIN=1
            parser.reset();
            writeBytes(buf, (byte) 0x80, (byte) 0x01, '!');
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
            writeBytes(buf, (byte) 0x01, (byte) 0x02, 'H', 'i');
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
            writeBytes(buf, (byte) 0x80, (byte) 0x01, '!');
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
            writeBytes(buf, (byte) 0x82, (byte) 127, 0, 0, 0, 0);  // Only 4 of 8 length bytes

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
            writeBytes(buf, (byte) 0x82, (byte) 0x85, 0x12, 0x34);  // Only 2 of 4 mask bytes

            WebSocketFrameParser parser = new WebSocketFrameParser();
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
            writeBytes(buf, (byte) 0x82, (byte) 0x05, 0x01, 0x02);  // Only 2 of 5 payload bytes

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
                (byte) 0x82, (byte) 0x02, 0x01, 0x02,  // Frame 1: binary, 2 bytes
                (byte) 0x81, (byte) 0x03, 'a', 'b', 'c'  // Frame 2: text, 3 bytes
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
            int consumed = parser.parse(buf, buf + 2);

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
            writeBytes(buf, (byte) 0x82, (byte) 0x81, 0x00, 0x00, 0x00, 0x00, (byte) 0xFF);

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
            writeBytes(buf, (byte) 0x82, (byte) 0x02, 0x01, 0x02);

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

    // ==================== ZERO ALLOCATION TESTS ====================

    @Test
    public void testNoAllocationDuringParse() {
        long buf = allocateBuffer(1024);
        try {
            // Prepare test frame
            writeBytes(buf, (byte) 0x82, (byte) 0x85,
                0x12, 0x34, 0x56, 0x78,  // mask
                0x01, 0x02, 0x03, 0x04, 0x05);

            WebSocketFrameParser parser = new WebSocketFrameParser();

            // Warmup
            parser.parse(buf, buf + 11);
            parser.reset();

            // Measure allocations
            long allocsBefore = TestUtils.getAllocatedBytes();

            for (int i = 0; i < 10000; i++) {
                parser.parse(buf, buf + 11);
                parser.unmaskPayload(buf + 6, 5);
                parser.reset();
            }

            long allocsAfter = TestUtils.getAllocatedBytes();
            Assert.assertEquals("Parsing should not allocate", allocsBefore, allocsAfter);
        } finally {
            freeBuffer(buf, 1024);
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
}
```

**File**: `core/src/test/java/io/questdb/test/cutlass/http/websocket/WebSocketFrameWriterTest.java`

```java
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
    public void testNoAllocationDuringWrite() {
        long buf = allocateBuffer(64);
        try {
            // Warmup
            for (int i = 0; i < 100; i++) {
                WebSocketFrameWriter.writeHeader(buf, true, WebSocketOpcode.BINARY, 1000, false);
            }

            long allocsBefore = TestUtils.getAllocatedBytes();

            for (int i = 0; i < 10000; i++) {
                WebSocketFrameWriter.writeHeader(buf, true, WebSocketOpcode.BINARY, 1000, false);
            }

            long allocsAfter = TestUtils.getAllocatedBytes();
            Assert.assertEquals("Writing should not allocate", allocsBefore, allocsAfter);
        } finally {
            freeBuffer(buf, 64);
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
}
```

### 1.2 Implementation

Only after ALL tests in 1.1 are written, implement:

- `WebSocketOpcode.java` - Opcode constants
- `WebSocketCloseCode.java` - Close code constants
- `WebSocketFrameParser.java` - Frame parser
- `WebSocketFrameWriter.java` - Frame writer

### 1.3 Verification

```bash
mvn -Dtest=WebSocketFrameParserTest,WebSocketFrameWriterTest test
```

All tests must pass before proceeding to Phase 2.

---

## Phase 2: WebSocket Handshake

### 2.1 Write Tests First

**File**: `core/src/test/java/io/questdb/test/cutlass/http/websocket/WebSocketHandshakeTest.java`

```java
public class WebSocketHandshakeTest extends AbstractWebSocketTest {

    // ==================== DETECTION TESTS ====================

    @Test
    public void testDetectWebSocketUpgrade() {
        MockHttpRequestHeader header = new MockHttpRequestHeader();
        header.setHeader("Upgrade", "websocket");
        header.setHeader("Connection", "Upgrade");
        header.setHeader("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==");
        header.setHeader("Sec-WebSocket-Version", "13");

        WebSocketHandshake handshake = new WebSocketHandshake();
        Assert.assertTrue(handshake.isWebSocketUpgrade(header));
    }

    @Test
    public void testDetectNonWebSocketRequest() {
        MockHttpRequestHeader header = new MockHttpRequestHeader();
        header.setHeader("Content-Type", "application/json");

        WebSocketHandshake handshake = new WebSocketHandshake();
        Assert.assertFalse(handshake.isWebSocketUpgrade(header));
    }

    @Test
    public void testDetectCaseInsensitiveUpgrade() {
        MockHttpRequestHeader header = new MockHttpRequestHeader();
        header.setHeader("Upgrade", "WebSocket");  // Mixed case
        header.setHeader("Connection", "upgrade");  // Lower case
        header.setHeader("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==");
        header.setHeader("Sec-WebSocket-Version", "13");

        WebSocketHandshake handshake = new WebSocketHandshake();
        Assert.assertTrue(handshake.isWebSocketUpgrade(header));
    }

    @Test
    public void testDetectConnectionWithMultipleValues() {
        MockHttpRequestHeader header = new MockHttpRequestHeader();
        header.setHeader("Upgrade", "websocket");
        header.setHeader("Connection", "keep-alive, Upgrade");  // Multiple values
        header.setHeader("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==");
        header.setHeader("Sec-WebSocket-Version", "13");

        WebSocketHandshake handshake = new WebSocketHandshake();
        Assert.assertTrue(handshake.isWebSocketUpgrade(header));
    }

    @Test
    public void testNotUpgradeWithoutConnectionHeader() {
        MockHttpRequestHeader header = new MockHttpRequestHeader();
        header.setHeader("Upgrade", "websocket");
        // Missing Connection header

        WebSocketHandshake handshake = new WebSocketHandshake();
        Assert.assertFalse(handshake.isWebSocketUpgrade(header));
    }

    @Test
    public void testNotUpgradeWithWrongConnectionValue() {
        MockHttpRequestHeader header = new MockHttpRequestHeader();
        header.setHeader("Upgrade", "websocket");
        header.setHeader("Connection", "keep-alive");  // Not "Upgrade"

        WebSocketHandshake handshake = new WebSocketHandshake();
        Assert.assertFalse(handshake.isWebSocketUpgrade(header));
    }

    // ==================== VALIDATION TESTS ====================

    @Test
    public void testValidHandshake() {
        MockHttpRequestHeader header = new MockHttpRequestHeader();
        header.setHeader("Upgrade", "websocket");
        header.setHeader("Connection", "Upgrade");
        header.setHeader("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==");
        header.setHeader("Sec-WebSocket-Version", "13");

        WebSocketHandshake handshake = new WebSocketHandshake();
        CharSequence error = handshake.validate(header);

        Assert.assertNull(error);
    }

    @Test
    public void testRejectMissingKey() {
        MockHttpRequestHeader header = new MockHttpRequestHeader();
        header.setHeader("Upgrade", "websocket");
        header.setHeader("Connection", "Upgrade");
        header.setHeader("Sec-WebSocket-Version", "13");
        // Missing Sec-WebSocket-Key

        WebSocketHandshake handshake = new WebSocketHandshake();
        CharSequence error = handshake.validate(header);

        Assert.assertNotNull(error);
        Assert.assertTrue(error.toString().contains("Sec-WebSocket-Key"));
    }

    @Test
    public void testRejectInvalidKeyLength() {
        MockHttpRequestHeader header = new MockHttpRequestHeader();
        header.setHeader("Upgrade", "websocket");
        header.setHeader("Connection", "Upgrade");
        header.setHeader("Sec-WebSocket-Key", "short");  // Not 16 bytes base64
        header.setHeader("Sec-WebSocket-Version", "13");

        WebSocketHandshake handshake = new WebSocketHandshake();
        CharSequence error = handshake.validate(header);

        Assert.assertNotNull(error);
    }

    @Test
    public void testRejectMissingVersion() {
        MockHttpRequestHeader header = new MockHttpRequestHeader();
        header.setHeader("Upgrade", "websocket");
        header.setHeader("Connection", "Upgrade");
        header.setHeader("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==");
        // Missing Sec-WebSocket-Version

        WebSocketHandshake handshake = new WebSocketHandshake();
        CharSequence error = handshake.validate(header);

        Assert.assertNotNull(error);
    }

    @Test
    public void testRejectUnsupportedVersion() {
        for (String version : new String[]{"0", "7", "8", "12", "14", "99"}) {
            MockHttpRequestHeader header = new MockHttpRequestHeader();
            header.setHeader("Upgrade", "websocket");
            header.setHeader("Connection", "Upgrade");
            header.setHeader("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==");
            header.setHeader("Sec-WebSocket-Version", version);

            WebSocketHandshake handshake = new WebSocketHandshake();
            CharSequence error = handshake.validate(header);

            Assert.assertNotNull("Version " + version + " should be rejected", error);
        }
    }

    @Test
    public void testAcceptVersion13() {
        MockHttpRequestHeader header = new MockHttpRequestHeader();
        header.setHeader("Upgrade", "websocket");
        header.setHeader("Connection", "Upgrade");
        header.setHeader("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==");
        header.setHeader("Sec-WebSocket-Version", "13");

        WebSocketHandshake handshake = new WebSocketHandshake();
        CharSequence error = handshake.validate(header);

        Assert.assertNull(error);
    }

    // ==================== ACCEPT KEY COMPUTATION TESTS ====================

    @Test
    public void testComputeAcceptKeyRfc6455Example() {
        // Test vector from RFC 6455 Section 1.3
        String clientKey = "dGhlIHNhbXBsZSBub25jZQ==";
        String expectedAccept = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";

        WebSocketHandshake handshake = new WebSocketHandshake();
        StringSink sink = new StringSink();
        handshake.computeAcceptKey(new DirectUtf8String().of(clientKey), sink);

        Assert.assertEquals(expectedAccept, sink.toString());
    }

    @Test
    public void testComputeAcceptKeyDifferentInputs() {
        // Test with various keys to ensure computation is correct
        String[][] testCases = {
            {"x3JJHMbDL1EzLkh9GBhXDw==", "HSmrc0sMlYUkAGmm5OPpG2HaGWk="},
            {"AQIDBAUGBwgJCgsMDQ4PEA==", "dLa5nSFVw3UYvRbH9GH3oXwRwfE="},
        };

        WebSocketHandshake handshake = new WebSocketHandshake();

        for (String[] testCase : testCases) {
            StringSink sink = new StringSink();
            handshake.computeAcceptKey(new DirectUtf8String().of(testCase[0]), sink);
            Assert.assertEquals(testCase[1], sink.toString());
        }
    }

    @Test
    public void testComputeAcceptKeyNoAllocation() {
        WebSocketHandshake handshake = new WebSocketHandshake();
        DirectUtf8String key = new DirectUtf8String().of("dGhlIHNhbXBsZSBub25jZQ==");
        StringSink sink = new StringSink();

        // Warmup
        for (int i = 0; i < 100; i++) {
            sink.clear();
            handshake.computeAcceptKey(key, sink);
        }

        long allocsBefore = TestUtils.getAllocatedBytes();

        for (int i = 0; i < 10000; i++) {
            sink.clear();
            handshake.computeAcceptKey(key, sink);
        }

        long allocsAfter = TestUtils.getAllocatedBytes();

        // Allow some tolerance for StringSink growth
        Assert.assertTrue("Should minimize allocations",
            allocsAfter - allocsBefore < 1000);
    }

    // ==================== RESPONSE WRITING TESTS ====================

    @Test
    public void testWriteUpgradeResponse() {
        long buf = allocateBuffer(512);
        try {
            MockRawSocket socket = new MockRawSocket(buf, 512);

            WebSocketHandshake handshake = new WebSocketHandshake();
            handshake.writeUpgradeResponse(
                socket,
                new DirectUtf8String().of("dGhlIHNhbXBsZSBub25jZQ=="),
                null  // No subprotocol
            );

            String response = socket.getWrittenString();

            Assert.assertTrue(response.startsWith("HTTP/1.1 101 Switching Protocols\r\n"));
            Assert.assertTrue(response.contains("Upgrade: websocket\r\n"));
            Assert.assertTrue(response.contains("Connection: Upgrade\r\n"));
            Assert.assertTrue(response.contains("Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n"));
            Assert.assertTrue(response.endsWith("\r\n\r\n"));
        } finally {
            freeBuffer(buf, 512);
        }
    }

    @Test
    public void testWriteUpgradeResponseWithProtocol() {
        long buf = allocateBuffer(512);
        try {
            MockRawSocket socket = new MockRawSocket(buf, 512);

            WebSocketHandshake handshake = new WebSocketHandshake();
            handshake.writeUpgradeResponse(
                socket,
                new DirectUtf8String().of("dGhlIHNhbXBsZSBub25jZQ=="),
                new DirectUtf8String().of("qwp")
            );

            String response = socket.getWrittenString();

            Assert.assertTrue(response.contains("Sec-WebSocket-Protocol: qwp\r\n"));
        } finally {
            freeBuffer(buf, 512);
        }
    }

    @Test
    public void testWriteUpgradeResponseNoAllocation() {
        long buf = allocateBuffer(512);
        try {
            MockRawSocket socket = new MockRawSocket(buf, 512);
            WebSocketHandshake handshake = new WebSocketHandshake();
            DirectUtf8String key = new DirectUtf8String().of("dGhlIHNhbXBsZSBub25jZQ==");

            // Warmup
            for (int i = 0; i < 100; i++) {
                socket.reset();
                handshake.writeUpgradeResponse(socket, key, null);
            }

            long allocsBefore = TestUtils.getAllocatedBytes();

            for (int i = 0; i < 1000; i++) {
                socket.reset();
                handshake.writeUpgradeResponse(socket, key, null);
            }

            long allocsAfter = TestUtils.getAllocatedBytes();

            Assert.assertEquals("Response writing should not allocate",
                allocsBefore, allocsAfter);
        } finally {
            freeBuffer(buf, 512);
        }
    }

    // ==================== ERROR RESPONSE TESTS ====================

    @Test
    public void testWriteBadRequestResponse() {
        long buf = allocateBuffer(512);
        try {
            MockRawSocket socket = new MockRawSocket(buf, 512);

            WebSocketHandshake handshake = new WebSocketHandshake();
            handshake.writeBadRequestResponse(socket, "Invalid WebSocket key");

            String response = socket.getWrittenString();

            Assert.assertTrue(response.startsWith("HTTP/1.1 400 Bad Request\r\n"));
        } finally {
            freeBuffer(buf, 512);
        }
    }

    @Test
    public void testWriteVersionNotSupportedResponse() {
        long buf = allocateBuffer(512);
        try {
            MockRawSocket socket = new MockRawSocket(buf, 512);

            WebSocketHandshake handshake = new WebSocketHandshake();
            handshake.writeVersionNotSupportedResponse(socket);

            String response = socket.getWrittenString();

            Assert.assertTrue(response.startsWith("HTTP/1.1 426 Upgrade Required\r\n"));
            Assert.assertTrue(response.contains("Sec-WebSocket-Version: 13\r\n"));
        } finally {
            freeBuffer(buf, 512);
        }
    }
}
```

**File**: `core/src/test/java/io/questdb/test/cutlass/http/websocket/WebSocketHandshakeIntegrationTest.java`

```java
/**
 * Integration tests using real WebSocket client.
 */
public class WebSocketHandshakeIntegrationTest extends AbstractWebSocketIntegrationTest {

    @Test
    public void testHandshakeWithJavaWebSocketClient() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain server = startWithEnvVariables()) {
                int port = server.getHttpServerPort();

                CountDownLatch connected = new CountDownLatch(1);
                CountDownLatch closed = new CountDownLatch(1);
                AtomicReference<ServerHandshake> handshakeRef = new AtomicReference<>();
                AtomicReference<Exception> errorRef = new AtomicReference<>();

                WebSocketClient client = new WebSocketClient(
                    new URI("ws://localhost:" + port + "/write/v4/ws")
                ) {
                    @Override
                    public void onOpen(ServerHandshake handshake) {
                        handshakeRef.set(handshake);
                        connected.countDown();
                    }

                    @Override
                    public void onMessage(String message) {}

                    @Override
                    public void onClose(int code, String reason, boolean remote) {
                        closed.countDown();
                    }

                    @Override
                    public void onError(Exception ex) {
                        errorRef.set(ex);
                        connected.countDown();
                    }
                };

                client.connect();

                Assert.assertTrue("Connection timeout",
                    connected.await(5, TimeUnit.SECONDS));
                Assert.assertNull("Connection error", errorRef.get());
                Assert.assertNotNull("Handshake received", handshakeRef.get());

                client.close();
                Assert.assertTrue("Close timeout",
                    closed.await(5, TimeUnit.SECONDS));
            }
        });
    }

    @Test
    public void testHandshakeWithSubprotocol() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain server = startWithEnvVariables()) {
                int port = server.getHttpServerPort();

                CountDownLatch connected = new CountDownLatch(1);
                AtomicReference<String> protocolRef = new AtomicReference<>();

                WebSocketClient client = new WebSocketClient(
                    new URI("ws://localhost:" + port + "/write/v4/ws"),
                    new Draft_6455(Collections.emptyList(),
                        Collections.singletonList(new Protocol("qwp")))
                ) {
                    @Override
                    public void onOpen(ServerHandshake handshake) {
                        protocolRef.set(
                            handshake.getFieldValue("Sec-WebSocket-Protocol"));
                        connected.countDown();
                    }

                    @Override
                    public void onMessage(String message) {}

                    @Override
                    public void onClose(int code, String reason, boolean remote) {}

                    @Override
                    public void onError(Exception ex) {
                        connected.countDown();
                    }
                };

                client.connect();

                Assert.assertTrue(connected.await(5, TimeUnit.SECONDS));
                Assert.assertEquals("qwp", protocolRef.get());

                client.close();
            }
        });
    }

    @Test
    public void testRejectInvalidVersion() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain server = startWithEnvVariables()) {
                int port = server.getHttpServerPort();

                // Send raw HTTP request with wrong WebSocket version
                try (Socket socket = new Socket("localhost", port)) {
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    BufferedReader in = new BufferedReader(
                        new InputStreamReader(socket.getInputStream()));

                    out.print("GET /write/v4/ws HTTP/1.1\r\n");
                    out.print("Host: localhost:" + port + "\r\n");
                    out.print("Upgrade: websocket\r\n");
                    out.print("Connection: Upgrade\r\n");
                    out.print("Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n");
                    out.print("Sec-WebSocket-Version: 8\r\n");  // Wrong version
                    out.print("\r\n");
                    out.flush();

                    String statusLine = in.readLine();
                    Assert.assertTrue(statusLine.contains("426"));

                    // Read headers to find Sec-WebSocket-Version
                    String line;
                    boolean foundVersion = false;
                    while ((line = in.readLine()) != null && !line.isEmpty()) {
                        if (line.startsWith("Sec-WebSocket-Version:")) {
                            Assert.assertTrue(line.contains("13"));
                            foundVersion = true;
                        }
                    }
                    Assert.assertTrue("Should include supported version", foundVersion);
                }
            }
        });
    }

    @Test
    public void testRejectNonWebSocketPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain server = startWithEnvVariables()) {
                int port = server.getHttpServerPort();

                CountDownLatch done = new CountDownLatch(1);
                AtomicBoolean rejected = new AtomicBoolean(false);

                WebSocketClient client = new WebSocketClient(
                    new URI("ws://localhost:" + port + "/nonexistent")
                ) {
                    @Override
                    public void onOpen(ServerHandshake handshake) {
                        done.countDown();
                    }

                    @Override
                    public void onMessage(String message) {}

                    @Override
                    public void onClose(int code, String reason, boolean remote) {
                        done.countDown();
                    }

                    @Override
                    public void onError(Exception ex) {
                        rejected.set(true);
                        done.countDown();
                    }
                };

                client.connect();

                Assert.assertTrue(done.await(5, TimeUnit.SECONDS));
                Assert.assertTrue("Should reject non-WebSocket path", rejected.get());
            }
        });
    }

    @Test
    public void testMultipleConcurrentHandshakes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain server = startWithEnvVariables()) {
                int port = server.getHttpServerPort();
                int numClients = 10;

                CountDownLatch allConnected = new CountDownLatch(numClients);
                AtomicInteger successCount = new AtomicInteger(0);
                List<WebSocketClient> clients = new ArrayList<>();

                for (int i = 0; i < numClients; i++) {
                    WebSocketClient client = new WebSocketClient(
                        new URI("ws://localhost:" + port + "/write/v4/ws")
                    ) {
                        @Override
                        public void onOpen(ServerHandshake handshake) {
                            successCount.incrementAndGet();
                            allConnected.countDown();
                        }

                        @Override
                        public void onMessage(String message) {}

                        @Override
                        public void onClose(int code, String reason, boolean remote) {}

                        @Override
                        public void onError(Exception ex) {
                            allConnected.countDown();
                        }
                    };
                    clients.add(client);
                }

                // Connect all clients concurrently
                for (WebSocketClient client : clients) {
                    client.connect();
                }

                Assert.assertTrue("All clients should connect",
                    allConnected.await(10, TimeUnit.SECONDS));
                Assert.assertEquals("All handshakes should succeed",
                    numClients, successCount.get());

                // Cleanup
                for (WebSocketClient client : clients) {
                    client.close();
                }
            }
        });
    }
}
```

### 2.2 Implementation

After ALL Phase 2 tests are written, implement:

- Modify `HttpHeaderParser` for WebSocket detection
- Implement `WebSocketHandshake`
- Add HTTP constants for WebSocket headers

### 2.3 Verification

```bash
mvn -Dtest=WebSocketHandshakeTest,WebSocketHandshakeIntegrationTest test
```

---

## Phase 3: WebSocket Connection Context

### 3.1 Write Tests First

**File**: `core/src/test/java/io/questdb/test/cutlass/http/websocket/WebSocketConnectionContextTest.java`

```java
public class WebSocketConnectionContextTest extends AbstractWebSocketTest {

    // ==================== STATE MANAGEMENT TESTS ====================

    @Test
    public void testInitialState() {
        WebSocketConnectionContext ctx = createContext();
        try {
            Assert.assertEquals(WebSocketConnectionContext.STATE_OPEN, ctx.getState());
            Assert.assertFalse(ctx.isClosing());
            Assert.assertFalse(ctx.isClosed());
        } finally {
            ctx.close();
        }
    }

    @Test
    public void testStateTransitionToClosing() {
        WebSocketConnectionContext ctx = createContext();
        try {
            ctx.initiateClose(1000, "Normal");

            Assert.assertEquals(WebSocketConnectionContext.STATE_CLOSING, ctx.getState());
            Assert.assertTrue(ctx.isClosing());
            Assert.assertFalse(ctx.isClosed());
        } finally {
            ctx.close();
        }
    }

    @Test
    public void testStateTransitionToClosed() {
        WebSocketConnectionContext ctx = createContext();
        try {
            ctx.initiateClose(1000, "Normal");
            ctx.onCloseFrameReceived(1000);

            Assert.assertEquals(WebSocketConnectionContext.STATE_CLOSED, ctx.getState());
            Assert.assertTrue(ctx.isClosed());
        } finally {
            ctx.close();
        }
    }

    @Test
    public void testCloseHandshakeClientInitiated() {
        WebSocketConnectionContext ctx = createContext();
        try {
            // Client sends close
            ctx.onCloseFrameReceived(1000);

            Assert.assertTrue(ctx.hasPendingCloseResponse());
            Assert.assertEquals(WebSocketConnectionContext.STATE_CLOSING, ctx.getState());

            // Server sends close response
            ctx.sendCloseFrame(1000, null);
            ctx.onCloseFrameSent();

            Assert.assertEquals(WebSocketConnectionContext.STATE_CLOSED, ctx.getState());
        } finally {
            ctx.close();
        }
    }

    @Test
    public void testCloseHandshakeServerInitiated() {
        WebSocketConnectionContext ctx = createContext();
        try {
            // Server initiates close
            ctx.initiateClose(1001, "Going away");

            Assert.assertTrue(ctx.isCloseFrameSent());
            Assert.assertEquals(WebSocketConnectionContext.STATE_CLOSING, ctx.getState());

            // Client responds with close
            ctx.onCloseFrameReceived(1001);

            Assert.assertEquals(WebSocketConnectionContext.STATE_CLOSED, ctx.getState());
        } finally {
            ctx.close();
        }
    }

    // ==================== RECEIVE HANDLING TESTS ====================

    @Test
    public void testReceiveSingleBinaryFrame() {
        WebSocketConnectionContext ctx = createContext();
        MockWebSocketProcessor processor = new MockWebSocketProcessor();
        try {
            // Simulate receiving a binary frame
            byte[] data = {0x01, 0x02, 0x03, 0x04, 0x05};
            simulateReceive(ctx, createMaskedFrame(WebSocketOpcode.BINARY, data));

            ctx.handleRead(processor);

            Assert.assertEquals(1, processor.getMessageCount());
            Assert.assertArrayEquals(data, processor.getLastMessageBytes());
        } finally {
            ctx.close();
        }
    }

    @Test
    public void testReceiveMultipleFramesInOneRead() {
        WebSocketConnectionContext ctx = createContext();
        MockWebSocketProcessor processor = new MockWebSocketProcessor();
        try {
            byte[] frame1 = createMaskedFrame(WebSocketOpcode.BINARY, new byte[]{1, 2});
            byte[] frame2 = createMaskedFrame(WebSocketOpcode.BINARY, new byte[]{3, 4});

            byte[] combined = new byte[frame1.length + frame2.length];
            System.arraycopy(frame1, 0, combined, 0, frame1.length);
            System.arraycopy(frame2, 0, combined, frame1.length, frame2.length);

            simulateReceive(ctx, combined);
            ctx.handleRead(processor);

            Assert.assertEquals(2, processor.getMessageCount());
        } finally {
            ctx.close();
        }
    }

    @Test
    public void testReceiveFragmentedMessage() {
        WebSocketConnectionContext ctx = createContext();
        MockWebSocketProcessor processor = new MockWebSocketProcessor();
        try {
            // Fragment 1: BINARY, FIN=0
            simulateReceive(ctx, createMaskedFrame(WebSocketOpcode.BINARY,
                new byte[]{1, 2, 3}, false));
            ctx.handleRead(processor);

            Assert.assertEquals(0, processor.getMessageCount());  // Not complete yet

            // Fragment 2: CONTINUATION, FIN=0
            simulateReceive(ctx, createMaskedFrame(WebSocketOpcode.CONTINUATION,
                new byte[]{4, 5, 6}, false));
            ctx.handleRead(processor);

            Assert.assertEquals(0, processor.getMessageCount());

            // Fragment 3: CONTINUATION, FIN=1
            simulateReceive(ctx, createMaskedFrame(WebSocketOpcode.CONTINUATION,
                new byte[]{7, 8, 9}, true));
            ctx.handleRead(processor);

            Assert.assertEquals(1, processor.getMessageCount());
            Assert.assertArrayEquals(
                new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9},
                processor.getLastMessageBytes());
        } finally {
            ctx.close();
        }
    }

    @Test
    public void testReceivePingDuringFragment() {
        WebSocketConnectionContext ctx = createContext();
        MockWebSocketProcessor processor = new MockWebSocketProcessor();
        try {
            // Start fragmented message
            simulateReceive(ctx, createMaskedFrame(WebSocketOpcode.BINARY,
                new byte[]{1, 2}, false));
            ctx.handleRead(processor);

            // Receive ping in the middle
            simulateReceive(ctx, createMaskedFrame(WebSocketOpcode.PING,
                new byte[]{0xAA, 0xBB}, true));
            ctx.handleRead(processor);

            Assert.assertEquals(1, processor.getPingCount());
            Assert.assertTrue(ctx.hasPendingPong());

            // Complete fragmented message
            simulateReceive(ctx, createMaskedFrame(WebSocketOpcode.CONTINUATION,
                new byte[]{3, 4}, true));
            ctx.handleRead(processor);

            Assert.assertEquals(1, processor.getMessageCount());
            Assert.assertArrayEquals(new byte[]{1, 2, 3, 4},
                processor.getLastMessageBytes());
        } finally {
            ctx.close();
        }
    }

    @Test
    public void testReceiveBufferGrowth() {
        WebSocketConnectionContext ctx = createContext(256);  // Small initial buffer
        MockWebSocketProcessor processor = new MockWebSocketProcessor();
        try {
            // Send message larger than initial buffer
            byte[] largePayload = new byte[1024];
            new Random(42).nextBytes(largePayload);

            simulateReceive(ctx, createMaskedFrame(WebSocketOpcode.BINARY, largePayload));
            ctx.handleRead(processor);

            Assert.assertEquals(1, processor.getMessageCount());
            Assert.assertArrayEquals(largePayload, processor.getLastMessageBytes());
        } finally {
            ctx.close();
        }
    }

    @Test
    public void testReceiveBufferCompaction() {
        WebSocketConnectionContext ctx = createContext(256);
        MockWebSocketProcessor processor = new MockWebSocketProcessor();
        try {
            // Fill buffer with partial read
            byte[] frame = createMaskedFrame(WebSocketOpcode.BINARY, new byte[50]);
            simulatePartialReceive(ctx, frame, 30);  // Only 30 bytes arrive

            ctx.handleRead(processor);
            Assert.assertEquals(0, processor.getMessageCount());  // Incomplete

            // Rest of data arrives
            simulateReceive(ctx, Arrays.copyOfRange(frame, 30, frame.length));
            ctx.handleRead(processor);

            Assert.assertEquals(1, processor.getMessageCount());
        } finally {
            ctx.close();
        }
    }

    @Test
    public void testRejectOversizeMessage() {
        WebSocketConnectionContext ctx = createContext();
        ctx.setMaxMessageSize(1024);
        MockWebSocketProcessor processor = new MockWebSocketProcessor();
        try {
            byte[] oversizePayload = new byte[2048];
            simulateReceive(ctx, createMaskedFrame(WebSocketOpcode.BINARY, oversizePayload));

            ctx.handleRead(processor);

            Assert.assertTrue(processor.hasError());
            Assert.assertEquals(WebSocketCloseCode.MESSAGE_TOO_BIG,
                processor.getLastErrorCode());
        } finally {
            ctx.close();
        }
    }

    @Test
    public void testRejectOversizeFragmentedMessage() {
        WebSocketConnectionContext ctx = createContext();
        ctx.setMaxMessageSize(100);
        MockWebSocketProcessor processor = new MockWebSocketProcessor();
        try {
            // Send fragments that together exceed limit
            simulateReceive(ctx, createMaskedFrame(WebSocketOpcode.BINARY,
                new byte[60], false));
            ctx.handleRead(processor);

            simulateReceive(ctx, createMaskedFrame(WebSocketOpcode.CONTINUATION,
                new byte[60], true));  // Total 120 > 100
            ctx.handleRead(processor);

            Assert.assertTrue(processor.hasError());
        } finally {
            ctx.close();
        }
    }

    // ==================== SEND HANDLING TESTS ====================

    @Test
    public void testSendBinaryFrame() {
        WebSocketConnectionContext ctx = createContext();
        MockSocket socket = (MockSocket) ctx.getSocket();
        try {
            byte[] data = {0x01, 0x02, 0x03, 0x04, 0x05};
            ctx.sendBinaryFrame(data, 0, data.length);
            ctx.flush();

            byte[] sent = socket.getSentData();
            WebSocketFrameParser parser = new WebSocketFrameParser();
            parser.parse(toAddress(sent), toAddress(sent) + sent.length);

            Assert.assertEquals(WebSocketOpcode.BINARY, parser.getOpcode());
            Assert.assertEquals(data.length, parser.getPayloadLength());
            Assert.assertFalse(parser.isMasked());  // Server->client not masked
        } finally {
            ctx.close();
        }
    }

    @Test
    public void testSendLargeFrame() {
        WebSocketConnectionContext ctx = createContext();
        MockSocket socket = (MockSocket) ctx.getSocket();
        try {
            byte[] data = new byte[100000];
            new Random(42).nextBytes(data);

            ctx.sendBinaryFrame(data, 0, data.length);
            ctx.flush();

            byte[] sent = socket.getSentData();
            Assert.assertTrue(sent.length > data.length);  // Header added
        } finally {
            ctx.close();
        }
    }

    @Test
    public void testSendCloseFrame() {
        WebSocketConnectionContext ctx = createContext();
        MockSocket socket = (MockSocket) ctx.getSocket();
        try {
            ctx.sendCloseFrame(1000, "Normal closure");
            ctx.flush();

            byte[] sent = socket.getSentData();
            WebSocketFrameParser parser = new WebSocketFrameParser();
            parser.parse(toAddress(sent), toAddress(sent) + sent.length);

            Assert.assertEquals(WebSocketOpcode.CLOSE, parser.getOpcode());
        } finally {
            ctx.close();
        }
    }

    @Test
    public void testSendPongFrame() {
        WebSocketConnectionContext ctx = createContext();
        MockSocket socket = (MockSocket) ctx.getSocket();
        try {
            byte[] pingData = {0x01, 0x02, 0x03, 0x04};
            ctx.sendPongFrame(pingData, 0, pingData.length);
            ctx.flush();

            byte[] sent = socket.getSentData();
            WebSocketFrameParser parser = new WebSocketFrameParser();
            parser.parse(toAddress(sent), toAddress(sent) + sent.length);

            Assert.assertEquals(WebSocketOpcode.PONG, parser.getOpcode());
            Assert.assertEquals(pingData.length, parser.getPayloadLength());
        } finally {
            ctx.close();
        }
    }

    @Test
    public void testSendBufferFlush() {
        WebSocketConnectionContext ctx = createContext();
        MockSocket socket = (MockSocket) ctx.getSocket();
        socket.setSlowMode(true);  // Simulate slow network
        try {
            byte[] data = new byte[1000];
            ctx.sendBinaryFrame(data, 0, data.length);

            Assert.assertTrue(ctx.hasPendingSend());

            // Flush until complete
            while (ctx.hasPendingSend()) {
                ctx.flush();
            }

            Assert.assertFalse(ctx.hasPendingSend());
            Assert.assertTrue(socket.getSentData().length > 0);
        } finally {
            ctx.close();
        }
    }

    @Test
    public void testSendBackpressure() {
        WebSocketConnectionContext ctx = createContext();
        MockSocket socket = (MockSocket) ctx.getSocket();
        socket.setSendBufferFull(true);
        try {
            byte[] data = new byte[10000];
            ctx.sendBinaryFrame(data, 0, data.length);

            Assert.assertTrue(ctx.hasPendingSend());
            Assert.assertThrows(PeerIsSlowToReadException.class, () -> ctx.flush());
        } finally {
            ctx.close();
        }
    }

    // ==================== CLOSE HANDLING EDGE CASES ====================

    @Test
    public void testReceiveAfterCloseSent() {
        WebSocketConnectionContext ctx = createContext();
        MockWebSocketProcessor processor = new MockWebSocketProcessor();
        try {
            ctx.initiateClose(1000, "Normal");

            // Receive data after close sent (should be ignored per RFC)
            simulateReceive(ctx, createMaskedFrame(WebSocketOpcode.BINARY,
                new byte[]{1, 2, 3}));
            ctx.handleRead(processor);

            // Data messages should be ignored during close
            Assert.assertEquals(0, processor.getMessageCount());
        } finally {
            ctx.close();
        }
    }

    @Test
    public void testReceiveCloseAfterCloseSent() {
        WebSocketConnectionContext ctx = createContext();
        try {
            ctx.initiateClose(1000, "Normal");

            // Receive close response
            simulateReceive(ctx, createMaskedFrame(WebSocketOpcode.CLOSE,
                new byte[]{0x03, (byte) 0xE8}));  // Code 1000

            MockWebSocketProcessor processor = new MockWebSocketProcessor();
            ctx.handleRead(processor);

            Assert.assertEquals(WebSocketConnectionContext.STATE_CLOSED, ctx.getState());
        } finally {
            ctx.close();
        }
    }

    @Test
    public void testPingDuringClose() {
        WebSocketConnectionContext ctx = createContext();
        MockWebSocketProcessor processor = new MockWebSocketProcessor();
        try {
            ctx.onCloseFrameReceived(1000);

            // Ping during close handshake should still be handled
            simulateReceive(ctx, createMaskedFrame(WebSocketOpcode.PING,
                new byte[]{1, 2}));
            ctx.handleRead(processor);

            Assert.assertTrue(ctx.hasPendingPong());
        } finally {
            ctx.close();
        }
    }

    // ==================== RESOURCE MANAGEMENT TESTS ====================

    @Test
    public void testBufferCleanupOnClose() {
        long memBefore = Unsafe.getMemUsed();

        WebSocketConnectionContext ctx = createContext();
        // Use the context
        simulateReceive(ctx, createMaskedFrame(WebSocketOpcode.BINARY, new byte[1000]));

        ctx.close();

        long memAfter = Unsafe.getMemUsed();

        // Memory should be fully released
        Assert.assertEquals(memBefore, memAfter);
    }

    @Test
    public void testContextReuse() {
        WebSocketConnectionContext ctx = createContext();
        MockWebSocketProcessor processor = new MockWebSocketProcessor();
        try {
            // First use
            simulateReceive(ctx, createMaskedFrame(WebSocketOpcode.BINARY, new byte[]{1, 2}));
            ctx.handleRead(processor);
            Assert.assertEquals(1, processor.getMessageCount());

            // Clear for reuse
            ctx.clear();
            processor.clear();

            // Second use
            simulateReceive(ctx, createMaskedFrame(WebSocketOpcode.BINARY, new byte[]{3, 4}));
            ctx.handleRead(processor);
            Assert.assertEquals(1, processor.getMessageCount());
            Assert.assertArrayEquals(new byte[]{3, 4}, processor.getLastMessageBytes());
        } finally {
            ctx.close();
        }
    }

    // ==================== ZERO ALLOCATION TESTS ====================

    @Test
    public void testNoAllocationSteadyState() {
        WebSocketConnectionContext ctx = createContext();
        MockWebSocketProcessor processor = new MockWebSocketProcessor();
        try {
            byte[] frame = createMaskedFrame(WebSocketOpcode.BINARY, new byte[100]);

            // Warmup
            for (int i = 0; i < 1000; i++) {
                simulateReceive(ctx, frame);
                ctx.handleRead(processor);
                processor.clear();
            }

            long allocsBefore = TestUtils.getAllocatedBytes();

            for (int i = 0; i < 10000; i++) {
                simulateReceive(ctx, frame);
                ctx.handleRead(processor);
                processor.clear();
            }

            long allocsAfter = TestUtils.getAllocatedBytes();

            Assert.assertEquals("Steady-state should not allocate",
                allocsBefore, allocsAfter);
        } finally {
            ctx.close();
        }
    }

    // ==================== HELPER METHODS ====================

    private WebSocketConnectionContext createContext() {
        return createContext(65536);
    }

    private WebSocketConnectionContext createContext(int bufferSize) {
        return new WebSocketConnectionContext(
            new MockSocketFactory(),
            NetworkFacadeImpl.INSTANCE,
            LOG,
            bufferSize
        );
    }

    private byte[] createMaskedFrame(int opcode, byte[] payload) {
        return createMaskedFrame(opcode, payload, true);
    }

    private byte[] createMaskedFrame(int opcode, byte[] payload, boolean fin) {
        // Implementation creates properly masked WebSocket frame
        // ...
    }

    private void simulateReceive(WebSocketConnectionContext ctx, byte[] data) {
        MockSocket socket = (MockSocket) ctx.getSocket();
        socket.setReceiveData(data);
    }
}
```

### 3.2 Implementation

After ALL Phase 3 tests pass, implement:

- `WebSocketConnectionContext`
- `WebSocketProcessor` interface

---

## Phase 4-7: Remaining Phases

Follow the same pattern:

1. **Write ALL tests first** for each phase
2. **Implement** only after tests are written
3. **Verify** all tests pass before proceeding

### Phase 4: ILPv4 WebSocket Processor Tests

```java
public class QwpWebSocketProcessorTest { /* 50+ tests */ }
public class QwpWebSocketProcessorStateTest { /* 30+ tests */ }
```

### Phase 5: Server Integration Tests

```java
public class WebSocketServerIntegrationTest { /* 30+ tests */ }
public class WebSocketTlsIntegrationTest { /* 15+ tests */ }
```

### Phase 6: End-to-End Tests

```java
public class QwpWebSocketE2ETest { /* 40+ tests */ }
public class QwpWebSocketStressTest { /* 20+ tests */ }
public class QwpWebSocketBenchmarkTest { /* 10+ tests */ }
```

### Phase 7: Client SDK Tests

```java
public class QwpWebSocketSenderTest { /* 40+ tests */ }
public class QwpWebSocketSenderIntegrationTest { /* 25+ tests */ }
```

---

## Test Coverage Requirements

### Minimum Coverage per Phase

| Phase | Unit Tests | Integration Tests | Total |
|-------|-----------|-------------------|-------|
| 1: Frame Parser | 50+ | 0 | 50+ |
| 2: Handshake | 25+ | 15+ | 40+ |
| 3: Connection Context | 40+ | 10+ | 50+ |
| 4: ILPv4 Processor | 35+ | 15+ | 50+ |
| 5: Server Integration | 10+ | 30+ | 40+ |
| 6: E2E | 0 | 60+ | 60+ |
| 7: Client SDK | 30+ | 25+ | 55+ |
| **Total** | **190+** | **155+** | **345+** |

### Test Categories

Each component must have tests for:

1. **Happy path** - Normal operation
2. **Edge cases** - Boundary conditions
3. **Error cases** - Invalid input, failures
4. **Concurrency** - Thread safety (where applicable)
5. **Resource management** - Memory leaks, cleanup
6. **Zero allocation** - No allocation on hot paths
7. **Performance** - Throughput and latency (integration)

---

## Continuous Integration

```yaml
# .github/workflows/websocket-tests.yml
name: WebSocket Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Set up JDK 11
        uses: actions/setup-java@v3
        with:
          java-version: '11'

      - name: Run WebSocket Unit Tests
        run: |
          mvn test -Dtest="io.questdb.test.cutlass.http.websocket.*Test"

      - name: Run WebSocket Integration Tests
        run: |
          mvn test -Dtest="io.questdb.test.cutlass.http.websocket.*IntegrationTest"

      - name: Verify Test Coverage
        run: |
          mvn jacoco:report
          # Fail if coverage < 90%
          python scripts/check-coverage.py --min 90 --package io.questdb.cutlass.http.websocket
```

---

## Implementation Checklist

For EACH phase:

- [ ] Write ALL unit tests
- [ ] Write ALL integration tests
- [ ] Verify tests compile (and fail, since implementation doesn't exist)
- [ ] Implement production code
- [ ] Run all tests - must pass
- [ ] Run allocation tests - must show zero allocation on hot paths
- [ ] Code review
- [ ] Merge

**No phase can begin until previous phase is 100% complete with all tests passing.**
