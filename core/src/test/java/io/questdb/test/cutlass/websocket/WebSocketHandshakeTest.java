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

package io.questdb.test.cutlass.websocket;

import io.questdb.cutlass.qwp.server.QwpWebSocketHttpProcessor;
import io.questdb.std.str.Utf8String;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

/**
 * Tests for WebSocket handshake processing.
 */
public class WebSocketHandshakeTest extends AbstractWebSocketTest {

    // ==================== UPGRADE HEADER TESTS ====================

    @Test
    public void testBadRequestResponseSize() {
        long buf = allocateBuffer(256);
        try {
            String reason = "Test reason";
            int written = QwpWebSocketHttpProcessor.writeBadRequestResponse(buf, reason);

            // Verify the response is properly terminated
            byte[] response = readBytes(buf, written);
            String responseStr = new String(response, StandardCharsets.US_ASCII);
            Assert.assertTrue(responseStr.contains("\r\n\r\n"));
            Assert.assertTrue(responseStr.endsWith(reason));
        } finally {
            freeBuffer(buf, 256);
        }
    }

    @Test
    public void testComputeAcceptKeyConsistent() {
        // Same key should always produce same accept value
        String clientKey = "x3JJHMbDL1EzLkh9GBhXDw==";

        String accept1 = QwpWebSocketHttpProcessor.computeAcceptKey(clientKey);
        String accept2 = QwpWebSocketHttpProcessor.computeAcceptKey(clientKey);
        String accept3 = QwpWebSocketHttpProcessor.computeAcceptKey(clientKey);

        Assert.assertEquals(accept1, accept2);
        Assert.assertEquals(accept2, accept3);
    }

    // ==================== CONNECTION HEADER TESTS ====================

    @Test
    public void testComputeAcceptKeyDifferentKeys() {
        // Different keys should produce different accept values
        String key1 = "dGhlIHNhbXBsZSBub25jZQ==";
        String key2 = "x3JJHMbDL1EzLkh9GBhXDw==";

        String accept1 = QwpWebSocketHttpProcessor.computeAcceptKey(key1);
        String accept2 = QwpWebSocketHttpProcessor.computeAcceptKey(key2);

        Assert.assertNotEquals(accept1, accept2);
    }

    @Test
    public void testComputeAcceptKeyKnownValues() {
        // Additional test vectors to verify SHA-1 computation
        // RFC 6455 test vector
        String acceptKey = QwpWebSocketHttpProcessor.computeAcceptKey("dGhlIHNhbXBsZSBub25jZQ==");
        Assert.assertEquals("s3pPLMBiTxaQ9kYGzzhZRbK+xOo=", acceptKey);

        // Verify different keys produce different results
        String acceptKey2 = QwpWebSocketHttpProcessor.computeAcceptKey("x3JJHMbDL1EzLkh9GBhXDw==");
        Assert.assertNotEquals(acceptKey, acceptKey2);
    }

    // ==================== VERSION HEADER TESTS ====================

    @Test
    public void testComputeAcceptKeyRFCExample() {
        // Test vector from RFC 6455
        String clientKey = "dGhlIHNhbXBsZSBub25jZQ==";
        String expectedAccept = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";

        String acceptKey = QwpWebSocketHttpProcessor.computeAcceptKey(clientKey);
        Assert.assertEquals(expectedAccept, acceptKey);
    }

    @Test
    public void testComputeAcceptKeyUtf8() {
        // Same test with Utf8Sequence
        Utf8String clientKey = new Utf8String("dGhlIHNhbXBsZSBub25jZQ==");
        String expectedAccept = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";

        String acceptKey = QwpWebSocketHttpProcessor.computeAcceptKey(clientKey);
        Assert.assertEquals(expectedAccept, acceptKey);
    }

    // ==================== KEY VALIDATION TESTS ====================

    @Test
    public void testConnectionHeaderCaseSensitivity() {
        // Connection header variations
        Assert.assertTrue(QwpWebSocketHttpProcessor.isConnectionUpgrade(new Utf8String("UPGRADE")));
        Assert.assertTrue(QwpWebSocketHttpProcessor.isConnectionUpgrade(new Utf8String("uPgRaDe")));
        Assert.assertTrue(QwpWebSocketHttpProcessor.isConnectionUpgrade(new Utf8String("keep-alive, UPGRADE, something")));
    }

    @Test
    public void testConnectionHeaderWithMultipleValues() {
        // Connection header can have multiple values
        Assert.assertTrue(QwpWebSocketHttpProcessor.isConnectionUpgrade(
                new Utf8String("keep-alive, Upgrade")));
        Assert.assertTrue(QwpWebSocketHttpProcessor.isConnectionUpgrade(
                new Utf8String("Upgrade, keep-alive")));
        Assert.assertTrue(QwpWebSocketHttpProcessor.isConnectionUpgrade(
                new Utf8String("Connection, Upgrade, keep-alive")));
    }

    // ==================== ACCEPT KEY COMPUTATION TESTS ====================

    @Test
    public void testIsConnectionUpgrade() {
        Assert.assertTrue(QwpWebSocketHttpProcessor.isConnectionUpgrade(new Utf8String("Upgrade")));
        Assert.assertTrue(QwpWebSocketHttpProcessor.isConnectionUpgrade(new Utf8String("upgrade")));
        Assert.assertTrue(QwpWebSocketHttpProcessor.isConnectionUpgrade(new Utf8String("UPGRADE")));
        Assert.assertTrue(QwpWebSocketHttpProcessor.isConnectionUpgrade(new Utf8String("keep-alive, Upgrade")));
        Assert.assertTrue(QwpWebSocketHttpProcessor.isConnectionUpgrade(new Utf8String("Upgrade, keep-alive")));
    }

    @Test
    public void testIsInvalidKey() {
        Assert.assertFalse(QwpWebSocketHttpProcessor.isValidKey(null));
        Assert.assertFalse(QwpWebSocketHttpProcessor.isValidKey(new Utf8String("")));
        Assert.assertFalse(QwpWebSocketHttpProcessor.isValidKey(new Utf8String("short")));
        Assert.assertFalse(QwpWebSocketHttpProcessor.isValidKey(new Utf8String("waytoolongforavalidbase64keyvalue==")));
        Assert.assertFalse(QwpWebSocketHttpProcessor.isValidKey(new Utf8String("invalid!chars!here!==")));
        Assert.assertFalse(QwpWebSocketHttpProcessor.isValidKey(new Utf8String("has spaces in it  ==")));
    }

    @Test
    public void testIsInvalidVersion() {
        Assert.assertFalse(QwpWebSocketHttpProcessor.isValidVersion(null));
        Assert.assertFalse(QwpWebSocketHttpProcessor.isValidVersion(new Utf8String("")));
        Assert.assertFalse(QwpWebSocketHttpProcessor.isValidVersion(new Utf8String("12")));
        Assert.assertFalse(QwpWebSocketHttpProcessor.isValidVersion(new Utf8String("14")));
        Assert.assertFalse(QwpWebSocketHttpProcessor.isValidVersion(new Utf8String("0")));
        Assert.assertFalse(QwpWebSocketHttpProcessor.isValidVersion(new Utf8String("8")));
        Assert.assertFalse(QwpWebSocketHttpProcessor.isValidVersion(new Utf8String("abc")));
        Assert.assertFalse(QwpWebSocketHttpProcessor.isValidVersion(new Utf8String("13a")));
        Assert.assertFalse(QwpWebSocketHttpProcessor.isValidVersion(new Utf8String("1 3")));
    }

    @Test
    public void testIsNotConnectionUpgrade() {
        Assert.assertFalse(QwpWebSocketHttpProcessor.isConnectionUpgrade(null));
        Assert.assertFalse(QwpWebSocketHttpProcessor.isConnectionUpgrade(new Utf8String("")));
        Assert.assertFalse(QwpWebSocketHttpProcessor.isConnectionUpgrade(new Utf8String("keep-alive")));
        Assert.assertFalse(QwpWebSocketHttpProcessor.isConnectionUpgrade(new Utf8String("close")));
    }

    // ==================== RESPONSE WRITING TESTS ====================

    @Test
    public void testIsNotWebSocketUpgrade() {
        Assert.assertFalse(QwpWebSocketHttpProcessor.isWebSocketUpgrade(null));
        Assert.assertFalse(QwpWebSocketHttpProcessor.isWebSocketUpgrade(new Utf8String("")));
        Assert.assertFalse(QwpWebSocketHttpProcessor.isWebSocketUpgrade(new Utf8String("http")));
        Assert.assertFalse(QwpWebSocketHttpProcessor.isWebSocketUpgrade(new Utf8String("websocket-extension")));
        Assert.assertFalse(QwpWebSocketHttpProcessor.isWebSocketUpgrade(new Utf8String("web")));
    }

    @Test
    public void testIsValidKey() {
        // Valid base64-encoded 16-byte keys (24 chars)
        Assert.assertTrue(QwpWebSocketHttpProcessor.isValidKey(new Utf8String("dGhlIHNhbXBsZSBub25jZQ==")));
        Assert.assertTrue(QwpWebSocketHttpProcessor.isValidKey(new Utf8String("x3JJHMbDL1EzLkh9GBhXDw==")));
        Assert.assertTrue(QwpWebSocketHttpProcessor.isValidKey(new Utf8String("AAAAAAAAAAAAAAAAAAAAAA==")));
        Assert.assertTrue(QwpWebSocketHttpProcessor.isValidKey(new Utf8String("0123456789ABCDEFGHIJ+/==")));
    }

    @Test
    public void testIsValidVersion() {
        Assert.assertTrue(QwpWebSocketHttpProcessor.isValidVersion(new Utf8String("13")));
    }

    // ==================== VALIDATION TESTS ====================

    @Test
    public void testIsWebSocketUpgrade() {
        Assert.assertTrue(QwpWebSocketHttpProcessor.isWebSocketUpgrade(new Utf8String("websocket")));
        Assert.assertTrue(QwpWebSocketHttpProcessor.isWebSocketUpgrade(new Utf8String("WebSocket")));
        Assert.assertTrue(QwpWebSocketHttpProcessor.isWebSocketUpgrade(new Utf8String("WEBSOCKET")));
        Assert.assertTrue(QwpWebSocketHttpProcessor.isWebSocketUpgrade(new Utf8String("WeBsOcKeT")));
    }

    @Test
    public void testKeyWithAllBase64Characters() {
        // Test key containing varied base64 characters
        // Use a valid 24-char base64 string with varied characters including +/
        Assert.assertTrue(QwpWebSocketHttpProcessor.isValidKey(new Utf8String("abcd+/0123456789ABCDEF==")));
    }

    @Test
    public void testKeyWithTrailingWhitespace() {
        // Keys should not have whitespace
        Assert.assertFalse(QwpWebSocketHttpProcessor.isValidKey(new Utf8String("dGhlIHNhbXBsZSBub25jZQ= "))); // Trailing space
        Assert.assertFalse(QwpWebSocketHttpProcessor.isValidKey(new Utf8String(" dGhlIHNhbXBsZSBub25jZQ="))); // Leading space
    }

    @Test
    public void testProtocolWithSpecialCharacters() {
        long buf = allocateBuffer(512);
        try {
            String acceptKey = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";
            String protocol = "qwp.streaming"; // Protocol with dots
            int written = QwpWebSocketHttpProcessor.writeResponseWithProtocol(buf, acceptKey, protocol);

            byte[] response = readBytes(buf, written);
            String responseStr = new String(response, StandardCharsets.US_ASCII);

            Assert.assertTrue(responseStr.contains("Sec-WebSocket-Protocol: " + protocol + "\r\n"));
        } finally {
            freeBuffer(buf, 512);
        }
    }

    @Test
    public void testResponseSize() {
        String acceptKey = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";
        int expectedSize = QwpWebSocketHttpProcessor.responseSize(acceptKey);

        long buf = allocateBuffer(256);
        try {
            int written = QwpWebSocketHttpProcessor.writeResponse(buf, acceptKey);
            Assert.assertEquals(expectedSize, written);
        } finally {
            freeBuffer(buf, 256);
        }
    }

    @Test
    public void testResponseSizeWithProtocol() {
        String acceptKey = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";
        String protocol = "qwp";
        int expectedSize = QwpWebSocketHttpProcessor.responseSizeWithProtocol(acceptKey, protocol);

        long buf = allocateBuffer(512);
        try {
            int written = QwpWebSocketHttpProcessor.writeResponseWithProtocol(buf, acceptKey, protocol);
            Assert.assertEquals(expectedSize, written);
        } finally {
            freeBuffer(buf, 512);
        }
    }

    @Test
    public void testThreadSafety() throws InterruptedException {
        // Test that accept key computation is thread-safe
        String clientKey = "dGhlIHNhbXBsZSBub25jZQ==";
        String expectedAccept = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";

        Thread[] threads = new Thread[10];
        boolean[] results = new boolean[10];

        for (int i = 0; i < 10; i++) {
            final int idx = i;
            threads[i] = new Thread(() -> {
                for (int j = 0; j < 100; j++) {
                    String accept = QwpWebSocketHttpProcessor.computeAcceptKey(clientKey);
                    if (!expectedAccept.equals(accept)) {
                        results[idx] = false;
                        return;
                    }
                }
                results[idx] = true;
            });
        }

        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();

        for (int i = 0; i < 10; i++) {
            Assert.assertTrue("Thread " + i + " failed", results[i]);
        }
    }

    @Test
    public void testUpgradeHeaderCaseSensitivity() {
        // Upgrade header variations
        Assert.assertTrue(QwpWebSocketHttpProcessor.isWebSocketUpgrade(new Utf8String("WEBSOCKET")));
        Assert.assertTrue(QwpWebSocketHttpProcessor.isWebSocketUpgrade(new Utf8String("wEbSoCkEt")));
    }

    @Test
    public void testValidateAllHeadersInvalid() {
        // All headers null
        String result = QwpWebSocketHttpProcessor.validate(null, null, null, null);
        Assert.assertNotNull(result);
    }

    // ==================== EDGE CASES ====================

    @Test
    public void testValidateInvalidConnection() {
        String result = QwpWebSocketHttpProcessor.validate(
                new Utf8String("websocket"),
                new Utf8String("close"),
                new Utf8String("dGhlIHNhbXBsZSBub25jZQ=="),
                new Utf8String("13")
        );
        Assert.assertNotNull(result);
        Assert.assertTrue(result.contains("Connection"));
    }

    @Test
    public void testValidateInvalidKey() {
        String result = QwpWebSocketHttpProcessor.validate(
                new Utf8String("websocket"),
                new Utf8String("Upgrade"),
                new Utf8String("invalid"),
                new Utf8String("13")
        );
        Assert.assertNotNull(result);
        Assert.assertTrue(result.contains("Key"));
    }

    @Test
    public void testValidateInvalidUpgrade() {
        String result = QwpWebSocketHttpProcessor.validate(
                new Utf8String("http"),
                new Utf8String("Upgrade"),
                new Utf8String("dGhlIHNhbXBsZSBub25jZQ=="),
                new Utf8String("13")
        );
        Assert.assertNotNull(result);
        Assert.assertTrue(result.contains("Upgrade"));
    }

    // ==================== ERROR RESPONSE TESTS ====================

    @Test
    public void testValidateInvalidVersion() {
        String result = QwpWebSocketHttpProcessor.validate(
                new Utf8String("websocket"),
                new Utf8String("Upgrade"),
                new Utf8String("dGhlIHNhbXBsZSBub25jZQ=="),
                new Utf8String("8")
        );
        Assert.assertNotNull(result);
        Assert.assertTrue(result.contains("version"));
    }

    @Test
    public void testValidateMissingConnection() {
        String result = QwpWebSocketHttpProcessor.validate(
                new Utf8String("websocket"),
                null,
                new Utf8String("dGhlIHNhbXBsZSBub25jZQ=="),
                new Utf8String("13")
        );
        Assert.assertNotNull(result);
        Assert.assertTrue(result.contains("Connection"));
    }

    @Test
    public void testValidateMissingKey() {
        String result = QwpWebSocketHttpProcessor.validate(
                new Utf8String("websocket"),
                new Utf8String("Upgrade"),
                null,
                new Utf8String("13")
        );
        Assert.assertNotNull(result);
        Assert.assertTrue(result.contains("Key"));
    }

    // ==================== PROTOCOL NEGOTIATION TESTS ====================

    @Test
    public void testValidateMissingUpgrade() {
        String result = QwpWebSocketHttpProcessor.validate(
                null,
                new Utf8String("Upgrade"),
                new Utf8String("dGhlIHNhbXBsZSBub25jZQ=="),
                new Utf8String("13")
        );
        Assert.assertNotNull(result);
        Assert.assertTrue(result.contains("Upgrade"));
    }

    @Test
    public void testValidateMissingVersion() {
        String result = QwpWebSocketHttpProcessor.validate(
                new Utf8String("websocket"),
                new Utf8String("Upgrade"),
                new Utf8String("dGhlIHNhbXBsZSBub25jZQ=="),
                null
        );
        Assert.assertNotNull(result);
        Assert.assertTrue(result.contains("version"));
    }

    @Test
    public void testValidateSuccess() {
        String result = QwpWebSocketHttpProcessor.validate(
                new Utf8String("websocket"),
                new Utf8String("Upgrade"),
                new Utf8String("dGhlIHNhbXBsZSBub25jZQ=="),
                new Utf8String("13")
        );
        Assert.assertNull(result);
    }

    @Test
    public void testVersionHeaderEdgeCases() {
        // Edge cases for version parsing
        Assert.assertFalse(QwpWebSocketHttpProcessor.isValidVersion(new Utf8String("1 3"))); // Space in middle
        Assert.assertFalse(QwpWebSocketHttpProcessor.isValidVersion(new Utf8String(" 13"))); // Leading space
        Assert.assertFalse(QwpWebSocketHttpProcessor.isValidVersion(new Utf8String("13 "))); // Trailing space
        Assert.assertFalse(QwpWebSocketHttpProcessor.isValidVersion(new Utf8String("-13"))); // Negative sign
        Assert.assertFalse(QwpWebSocketHttpProcessor.isValidVersion(new Utf8String("13.0"))); // Decimal
        Assert.assertFalse(QwpWebSocketHttpProcessor.isValidVersion(new Utf8String("1a3"))); // Letter in middle
    }

    // ==================== ADDITIONAL EDGE CASE TESTS ====================

    @Test
    public void testVersionNotSupportedResponseSize() {
        long buf = allocateBuffer(256);
        try {
            int written = QwpWebSocketHttpProcessor.writeVersionNotSupportedResponse(buf);

            // Verify the response is properly terminated
            byte[] response = readBytes(buf, written);
            String responseStr = new String(response, StandardCharsets.US_ASCII);
            Assert.assertTrue(responseStr.contains("\r\n\r\n"));
        } finally {
            freeBuffer(buf, 256);
        }
    }

    @Test
    public void testWriteBadRequestResponse() {
        long buf = allocateBuffer(256);
        try {
            String reason = "Invalid WebSocket key";
            int written = QwpWebSocketHttpProcessor.writeBadRequestResponse(buf, reason);

            byte[] response = readBytes(buf, written);
            String responseStr = new String(response, StandardCharsets.US_ASCII);

            Assert.assertTrue(responseStr.startsWith("HTTP/1.1 400 Bad Request\r\n"));
            Assert.assertTrue(responseStr.contains("Content-Type: text/plain\r\n"));
            Assert.assertTrue(responseStr.contains(reason));
        } finally {
            freeBuffer(buf, 256);
        }
    }

    @Test
    public void testWriteBadRequestResponseWithEmptyReason() {
        long buf = allocateBuffer(256);
        try {
            int written = QwpWebSocketHttpProcessor.writeBadRequestResponse(buf, "");

            byte[] response = readBytes(buf, written);
            String responseStr = new String(response, StandardCharsets.US_ASCII);

            Assert.assertTrue(responseStr.startsWith("HTTP/1.1 400 Bad Request\r\n"));
        } finally {
            freeBuffer(buf, 256);
        }
    }

    @Test
    public void testWriteResponse() {
        long buf = allocateBuffer(256);
        try {
            String acceptKey = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";
            int written = QwpWebSocketHttpProcessor.writeResponse(buf, acceptKey);

            byte[] response = readBytes(buf, written);
            String responseStr = new String(response, StandardCharsets.US_ASCII);

            Assert.assertTrue(responseStr.startsWith("HTTP/1.1 101 Switching Protocols\r\n"));
            Assert.assertTrue(responseStr.contains("Upgrade: websocket\r\n"));
            Assert.assertTrue(responseStr.contains("Connection: Upgrade\r\n"));
            Assert.assertTrue(responseStr.contains("Sec-WebSocket-Accept: " + acceptKey + "\r\n"));
            Assert.assertTrue(responseStr.endsWith("\r\n\r\n"));
        } finally {
            freeBuffer(buf, 256);
        }
    }

    @Test
    public void testWriteResponseComplete() {
        // Full end-to-end test
        String clientKey = "dGhlIHNhbXBsZSBub25jZQ==";
        String acceptKey = QwpWebSocketHttpProcessor.computeAcceptKey(clientKey);

        long buf = allocateBuffer(256);
        try {
            int written = QwpWebSocketHttpProcessor.writeResponse(buf, acceptKey);

            byte[] response = readBytes(buf, written);
            String responseStr = new String(response, StandardCharsets.US_ASCII);

            // Verify full response
            String expected = """
                    HTTP/1.1 101 Switching Protocols\r
                    Upgrade: websocket\r
                    Connection: Upgrade\r
                    Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r
                    \r
                    """;
            Assert.assertEquals(expected, responseStr);
        } finally {
            freeBuffer(buf, 256);
        }
    }

    @Test
    public void testWriteResponseWithEmptyProtocol() {
        long buf = allocateBuffer(256);
        try {
            String acceptKey = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";
            int written = QwpWebSocketHttpProcessor.writeResponseWithProtocol(buf, acceptKey, "");

            byte[] response = readBytes(buf, written);
            String responseStr = new String(response, StandardCharsets.US_ASCII);

            // Should not contain protocol header for empty protocol
            Assert.assertFalse(responseStr.contains("Sec-WebSocket-Protocol:"));
        } finally {
            freeBuffer(buf, 256);
        }
    }

    @Test
    public void testWriteResponseWithNullProtocol() {
        long buf = allocateBuffer(256);
        try {
            String acceptKey = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";
            int written = QwpWebSocketHttpProcessor.writeResponseWithProtocol(buf, acceptKey, null);

            byte[] response = readBytes(buf, written);
            String responseStr = new String(response, StandardCharsets.US_ASCII);

            // Should not contain protocol header
            Assert.assertFalse(responseStr.contains("Sec-WebSocket-Protocol:"));
        } finally {
            freeBuffer(buf, 256);
        }
    }

    @Test
    public void testWriteResponseWithProtocol() {
        long buf = allocateBuffer(512);
        try {
            String acceptKey = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";
            String protocol = "qwp";
            int written = QwpWebSocketHttpProcessor.writeResponseWithProtocol(buf, acceptKey, protocol);

            byte[] response = readBytes(buf, written);
            String responseStr = new String(response, StandardCharsets.US_ASCII);

            Assert.assertTrue(responseStr.startsWith("HTTP/1.1 101 Switching Protocols\r\n"));
            Assert.assertTrue(responseStr.contains("Sec-WebSocket-Accept: " + acceptKey + "\r\n"));
            Assert.assertTrue(responseStr.contains("Sec-WebSocket-Protocol: " + protocol + "\r\n"));
            Assert.assertTrue(responseStr.endsWith("\r\n\r\n"));
        } finally {
            freeBuffer(buf, 512);
        }
    }

    @Test
    public void testWriteVersionNotSupportedResponse() {
        long buf = allocateBuffer(256);
        try {
            int written = QwpWebSocketHttpProcessor.writeVersionNotSupportedResponse(buf);

            byte[] response = readBytes(buf, written);
            String responseStr = new String(response, StandardCharsets.US_ASCII);

            Assert.assertTrue(responseStr.startsWith("HTTP/1.1 426 Upgrade Required\r\n"));
            Assert.assertTrue(responseStr.contains("Sec-WebSocket-Version: 13\r\n"));
        } finally {
            freeBuffer(buf, 256);
        }
    }
}
