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

import io.questdb.cutlass.qwp.websocket.WebSocketHandshake;
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
    public void testIsWebSocketUpgrade() {
        Assert.assertTrue(WebSocketHandshake.isWebSocketUpgrade(new Utf8String("websocket")));
        Assert.assertTrue(WebSocketHandshake.isWebSocketUpgrade(new Utf8String("WebSocket")));
        Assert.assertTrue(WebSocketHandshake.isWebSocketUpgrade(new Utf8String("WEBSOCKET")));
        Assert.assertTrue(WebSocketHandshake.isWebSocketUpgrade(new Utf8String("WeBsOcKeT")));
    }

    @Test
    public void testIsNotWebSocketUpgrade() {
        Assert.assertFalse(WebSocketHandshake.isWebSocketUpgrade(null));
        Assert.assertFalse(WebSocketHandshake.isWebSocketUpgrade(new Utf8String("")));
        Assert.assertFalse(WebSocketHandshake.isWebSocketUpgrade(new Utf8String("http")));
        Assert.assertFalse(WebSocketHandshake.isWebSocketUpgrade(new Utf8String("websocket-extension")));
        Assert.assertFalse(WebSocketHandshake.isWebSocketUpgrade(new Utf8String("web")));
    }

    // ==================== CONNECTION HEADER TESTS ====================

    @Test
    public void testIsConnectionUpgrade() {
        Assert.assertTrue(WebSocketHandshake.isConnectionUpgrade(new Utf8String("Upgrade")));
        Assert.assertTrue(WebSocketHandshake.isConnectionUpgrade(new Utf8String("upgrade")));
        Assert.assertTrue(WebSocketHandshake.isConnectionUpgrade(new Utf8String("UPGRADE")));
        Assert.assertTrue(WebSocketHandshake.isConnectionUpgrade(new Utf8String("keep-alive, Upgrade")));
        Assert.assertTrue(WebSocketHandshake.isConnectionUpgrade(new Utf8String("Upgrade, keep-alive")));
    }

    @Test
    public void testIsNotConnectionUpgrade() {
        Assert.assertFalse(WebSocketHandshake.isConnectionUpgrade(null));
        Assert.assertFalse(WebSocketHandshake.isConnectionUpgrade(new Utf8String("")));
        Assert.assertFalse(WebSocketHandshake.isConnectionUpgrade(new Utf8String("keep-alive")));
        Assert.assertFalse(WebSocketHandshake.isConnectionUpgrade(new Utf8String("close")));
    }

    // ==================== VERSION HEADER TESTS ====================

    @Test
    public void testIsValidVersion() {
        Assert.assertTrue(WebSocketHandshake.isValidVersion(new Utf8String("13")));
    }

    @Test
    public void testIsInvalidVersion() {
        Assert.assertFalse(WebSocketHandshake.isValidVersion(null));
        Assert.assertFalse(WebSocketHandshake.isValidVersion(new Utf8String("")));
        Assert.assertFalse(WebSocketHandshake.isValidVersion(new Utf8String("12")));
        Assert.assertFalse(WebSocketHandshake.isValidVersion(new Utf8String("14")));
        Assert.assertFalse(WebSocketHandshake.isValidVersion(new Utf8String("0")));
        Assert.assertFalse(WebSocketHandshake.isValidVersion(new Utf8String("8")));
        Assert.assertFalse(WebSocketHandshake.isValidVersion(new Utf8String("abc")));
        Assert.assertFalse(WebSocketHandshake.isValidVersion(new Utf8String("13a")));
        Assert.assertFalse(WebSocketHandshake.isValidVersion(new Utf8String("1 3")));
    }

    // ==================== KEY VALIDATION TESTS ====================

    @Test
    public void testIsValidKey() {
        // Valid base64-encoded 16-byte keys (24 chars)
        Assert.assertTrue(WebSocketHandshake.isValidKey(new Utf8String("dGhlIHNhbXBsZSBub25jZQ==")));
        Assert.assertTrue(WebSocketHandshake.isValidKey(new Utf8String("x3JJHMbDL1EzLkh9GBhXDw==")));
        Assert.assertTrue(WebSocketHandshake.isValidKey(new Utf8String("AAAAAAAAAAAAAAAAAAAAAA==")));
        Assert.assertTrue(WebSocketHandshake.isValidKey(new Utf8String("0123456789ABCDEFGHIJ+/==")));
    }

    @Test
    public void testIsInvalidKey() {
        Assert.assertFalse(WebSocketHandshake.isValidKey(null));
        Assert.assertFalse(WebSocketHandshake.isValidKey(new Utf8String("")));
        Assert.assertFalse(WebSocketHandshake.isValidKey(new Utf8String("short")));
        Assert.assertFalse(WebSocketHandshake.isValidKey(new Utf8String("waytoolongforavalidbase64keyvalue==")));
        Assert.assertFalse(WebSocketHandshake.isValidKey(new Utf8String("invalid!chars!here!==")));
        Assert.assertFalse(WebSocketHandshake.isValidKey(new Utf8String("has spaces in it  ==")));
    }

    // ==================== ACCEPT KEY COMPUTATION TESTS ====================

    @Test
    public void testComputeAcceptKeyRFCExample() {
        // Test vector from RFC 6455
        String clientKey = "dGhlIHNhbXBsZSBub25jZQ==";
        String expectedAccept = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";

        String acceptKey = WebSocketHandshake.computeAcceptKey(clientKey);
        Assert.assertEquals(expectedAccept, acceptKey);
    }

    @Test
    public void testComputeAcceptKeyUtf8() {
        // Same test with Utf8Sequence
        Utf8String clientKey = new Utf8String("dGhlIHNhbXBsZSBub25jZQ==");
        String expectedAccept = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";

        String acceptKey = WebSocketHandshake.computeAcceptKey(clientKey);
        Assert.assertEquals(expectedAccept, acceptKey);
    }

    @Test
    public void testComputeAcceptKeyDifferentKeys() {
        // Different keys should produce different accept values
        String key1 = "dGhlIHNhbXBsZSBub25jZQ==";
        String key2 = "x3JJHMbDL1EzLkh9GBhXDw==";

        String accept1 = WebSocketHandshake.computeAcceptKey(key1);
        String accept2 = WebSocketHandshake.computeAcceptKey(key2);

        Assert.assertNotEquals(accept1, accept2);
    }

    @Test
    public void testComputeAcceptKeyConsistent() {
        // Same key should always produce same accept value
        String clientKey = "x3JJHMbDL1EzLkh9GBhXDw==";

        String accept1 = WebSocketHandshake.computeAcceptKey(clientKey);
        String accept2 = WebSocketHandshake.computeAcceptKey(clientKey);
        String accept3 = WebSocketHandshake.computeAcceptKey(clientKey);

        Assert.assertEquals(accept1, accept2);
        Assert.assertEquals(accept2, accept3);
    }

    // ==================== RESPONSE WRITING TESTS ====================

    @Test
    public void testWriteResponse() {
        long buf = allocateBuffer(256);
        try {
            String acceptKey = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";
            int written = WebSocketHandshake.writeResponse(buf, acceptKey);

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
    public void testResponseSize() {
        String acceptKey = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";
        int expectedSize = WebSocketHandshake.responseSize(acceptKey);

        long buf = allocateBuffer(256);
        try {
            int written = WebSocketHandshake.writeResponse(buf, acceptKey);
            Assert.assertEquals(expectedSize, written);
        } finally {
            freeBuffer(buf, 256);
        }
    }

    @Test
    public void testWriteResponseComplete() {
        // Full end-to-end test
        String clientKey = "dGhlIHNhbXBsZSBub25jZQ==";
        String acceptKey = WebSocketHandshake.computeAcceptKey(clientKey);

        long buf = allocateBuffer(256);
        try {
            int written = WebSocketHandshake.writeResponse(buf, acceptKey);

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

    // ==================== VALIDATION TESTS ====================

    @Test
    public void testValidateSuccess() {
        String result = WebSocketHandshake.validate(
                new Utf8String("websocket"),
                new Utf8String("Upgrade"),
                new Utf8String("dGhlIHNhbXBsZSBub25jZQ=="),
                new Utf8String("13")
        );
        Assert.assertNull(result);
    }

    @Test
    public void testValidateMissingUpgrade() {
        String result = WebSocketHandshake.validate(
                null,
                new Utf8String("Upgrade"),
                new Utf8String("dGhlIHNhbXBsZSBub25jZQ=="),
                new Utf8String("13")
        );
        Assert.assertNotNull(result);
        Assert.assertTrue(result.contains("Upgrade"));
    }

    @Test
    public void testValidateInvalidUpgrade() {
        String result = WebSocketHandshake.validate(
                new Utf8String("http"),
                new Utf8String("Upgrade"),
                new Utf8String("dGhlIHNhbXBsZSBub25jZQ=="),
                new Utf8String("13")
        );
        Assert.assertNotNull(result);
        Assert.assertTrue(result.contains("Upgrade"));
    }

    @Test
    public void testValidateMissingConnection() {
        String result = WebSocketHandshake.validate(
                new Utf8String("websocket"),
                null,
                new Utf8String("dGhlIHNhbXBsZSBub25jZQ=="),
                new Utf8String("13")
        );
        Assert.assertNotNull(result);
        Assert.assertTrue(result.contains("Connection"));
    }

    @Test
    public void testValidateInvalidConnection() {
        String result = WebSocketHandshake.validate(
                new Utf8String("websocket"),
                new Utf8String("close"),
                new Utf8String("dGhlIHNhbXBsZSBub25jZQ=="),
                new Utf8String("13")
        );
        Assert.assertNotNull(result);
        Assert.assertTrue(result.contains("Connection"));
    }

    @Test
    public void testValidateMissingKey() {
        String result = WebSocketHandshake.validate(
                new Utf8String("websocket"),
                new Utf8String("Upgrade"),
                null,
                new Utf8String("13")
        );
        Assert.assertNotNull(result);
        Assert.assertTrue(result.contains("Key"));
    }

    @Test
    public void testValidateInvalidKey() {
        String result = WebSocketHandshake.validate(
                new Utf8String("websocket"),
                new Utf8String("Upgrade"),
                new Utf8String("invalid"),
                new Utf8String("13")
        );
        Assert.assertNotNull(result);
        Assert.assertTrue(result.contains("Key"));
    }

    @Test
    public void testValidateMissingVersion() {
        String result = WebSocketHandshake.validate(
                new Utf8String("websocket"),
                new Utf8String("Upgrade"),
                new Utf8String("dGhlIHNhbXBsZSBub25jZQ=="),
                null
        );
        Assert.assertNotNull(result);
        Assert.assertTrue(result.contains("version"));
    }

    @Test
    public void testValidateInvalidVersion() {
        String result = WebSocketHandshake.validate(
                new Utf8String("websocket"),
                new Utf8String("Upgrade"),
                new Utf8String("dGhlIHNhbXBsZSBub25jZQ=="),
                new Utf8String("8")
        );
        Assert.assertNotNull(result);
        Assert.assertTrue(result.contains("version"));
    }

    // ==================== EDGE CASES ====================

    @Test
    public void testConnectionHeaderWithMultipleValues() {
        // Connection header can have multiple values
        Assert.assertTrue(WebSocketHandshake.isConnectionUpgrade(
                new Utf8String("keep-alive, Upgrade")));
        Assert.assertTrue(WebSocketHandshake.isConnectionUpgrade(
                new Utf8String("Upgrade, keep-alive")));
        Assert.assertTrue(WebSocketHandshake.isConnectionUpgrade(
                new Utf8String("Connection, Upgrade, keep-alive")));
    }

    @Test
    public void testKeyWithAllBase64Characters() {
        // Test key containing varied base64 characters
        // Use a valid 24-char base64 string with varied characters including +/
        Assert.assertTrue(WebSocketHandshake.isValidKey(new Utf8String("abcd+/0123456789ABCDEF==")));
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
                    String accept = WebSocketHandshake.computeAcceptKey(clientKey);
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

    // ==================== ERROR RESPONSE TESTS ====================

    @Test
    public void testWriteBadRequestResponse() {
        long buf = allocateBuffer(256);
        try {
            String reason = "Invalid WebSocket key";
            int written = WebSocketHandshake.writeBadRequestResponse(buf, reason);

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
            int written = WebSocketHandshake.writeBadRequestResponse(buf, "");

            byte[] response = readBytes(buf, written);
            String responseStr = new String(response, StandardCharsets.US_ASCII);

            Assert.assertTrue(responseStr.startsWith("HTTP/1.1 400 Bad Request\r\n"));
        } finally {
            freeBuffer(buf, 256);
        }
    }

    @Test
    public void testWriteVersionNotSupportedResponse() {
        long buf = allocateBuffer(256);
        try {
            int written = WebSocketHandshake.writeVersionNotSupportedResponse(buf);

            byte[] response = readBytes(buf, written);
            String responseStr = new String(response, StandardCharsets.US_ASCII);

            Assert.assertTrue(responseStr.startsWith("HTTP/1.1 426 Upgrade Required\r\n"));
            Assert.assertTrue(responseStr.contains("Sec-WebSocket-Version: 13\r\n"));
        } finally {
            freeBuffer(buf, 256);
        }
    }

    // ==================== PROTOCOL NEGOTIATION TESTS ====================

    @Test
    public void testWriteResponseWithProtocol() {
        long buf = allocateBuffer(512);
        try {
            String acceptKey = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";
            String protocol = "qwp";
            int written = WebSocketHandshake.writeResponseWithProtocol(buf, acceptKey, protocol);

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
    public void testWriteResponseWithNullProtocol() {
        long buf = allocateBuffer(256);
        try {
            String acceptKey = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";
            int written = WebSocketHandshake.writeResponseWithProtocol(buf, acceptKey, null);

            byte[] response = readBytes(buf, written);
            String responseStr = new String(response, StandardCharsets.US_ASCII);

            // Should not contain protocol header
            Assert.assertFalse(responseStr.contains("Sec-WebSocket-Protocol:"));
        } finally {
            freeBuffer(buf, 256);
        }
    }

    @Test
    public void testWriteResponseWithEmptyProtocol() {
        long buf = allocateBuffer(256);
        try {
            String acceptKey = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";
            int written = WebSocketHandshake.writeResponseWithProtocol(buf, acceptKey, "");

            byte[] response = readBytes(buf, written);
            String responseStr = new String(response, StandardCharsets.US_ASCII);

            // Should not contain protocol header for empty protocol
            Assert.assertFalse(responseStr.contains("Sec-WebSocket-Protocol:"));
        } finally {
            freeBuffer(buf, 256);
        }
    }

    @Test
    public void testResponseSizeWithProtocol() {
        String acceptKey = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";
        String protocol = "qwp";
        int expectedSize = WebSocketHandshake.responseSizeWithProtocol(acceptKey, protocol);

        long buf = allocateBuffer(512);
        try {
            int written = WebSocketHandshake.writeResponseWithProtocol(buf, acceptKey, protocol);
            Assert.assertEquals(expectedSize, written);
        } finally {
            freeBuffer(buf, 512);
        }
    }

    // ==================== ADDITIONAL EDGE CASE TESTS ====================

    @Test
    public void testBadRequestResponseSize() {
        long buf = allocateBuffer(256);
        try {
            String reason = "Test reason";
            int written = WebSocketHandshake.writeBadRequestResponse(buf, reason);

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
    public void testVersionNotSupportedResponseSize() {
        long buf = allocateBuffer(256);
        try {
            int written = WebSocketHandshake.writeVersionNotSupportedResponse(buf);

            // Verify the response is properly terminated
            byte[] response = readBytes(buf, written);
            String responseStr = new String(response, StandardCharsets.US_ASCII);
            Assert.assertTrue(responseStr.contains("\r\n\r\n"));
        } finally {
            freeBuffer(buf, 256);
        }
    }

    @Test
    public void testComputeAcceptKeyKnownValues() {
        // Additional test vectors to verify SHA-1 computation
        // RFC 6455 test vector
        String acceptKey = WebSocketHandshake.computeAcceptKey("dGhlIHNhbXBsZSBub25jZQ==");
        Assert.assertEquals("s3pPLMBiTxaQ9kYGzzhZRbK+xOo=", acceptKey);

        // Verify different keys produce different results
        String acceptKey2 = WebSocketHandshake.computeAcceptKey("x3JJHMbDL1EzLkh9GBhXDw==");
        Assert.assertNotEquals(acceptKey, acceptKey2);
    }

    @Test
    public void testValidateAllHeadersInvalid() {
        // All headers null
        String result = WebSocketHandshake.validate(null, null, null, null);
        Assert.assertNotNull(result);
    }

    @Test
    public void testConnectionHeaderCaseSensitivity() {
        // Connection header variations
        Assert.assertTrue(WebSocketHandshake.isConnectionUpgrade(new Utf8String("UPGRADE")));
        Assert.assertTrue(WebSocketHandshake.isConnectionUpgrade(new Utf8String("uPgRaDe")));
        Assert.assertTrue(WebSocketHandshake.isConnectionUpgrade(new Utf8String("keep-alive, UPGRADE, something")));
    }

    @Test
    public void testUpgradeHeaderCaseSensitivity() {
        // Upgrade header variations
        Assert.assertTrue(WebSocketHandshake.isWebSocketUpgrade(new Utf8String("WEBSOCKET")));
        Assert.assertTrue(WebSocketHandshake.isWebSocketUpgrade(new Utf8String("wEbSoCkEt")));
    }

    @Test
    public void testVersionHeaderEdgeCases() {
        // Edge cases for version parsing
        Assert.assertFalse(WebSocketHandshake.isValidVersion(new Utf8String("1 3"))); // Space in middle
        Assert.assertFalse(WebSocketHandshake.isValidVersion(new Utf8String(" 13"))); // Leading space
        Assert.assertFalse(WebSocketHandshake.isValidVersion(new Utf8String("13 "))); // Trailing space
        Assert.assertFalse(WebSocketHandshake.isValidVersion(new Utf8String("-13"))); // Negative sign
        Assert.assertFalse(WebSocketHandshake.isValidVersion(new Utf8String("13.0"))); // Decimal
        Assert.assertFalse(WebSocketHandshake.isValidVersion(new Utf8String("1a3"))); // Letter in middle
    }

    @Test
    public void testKeyWithTrailingWhitespace() {
        // Keys should not have whitespace
        Assert.assertFalse(WebSocketHandshake.isValidKey(new Utf8String("dGhlIHNhbXBsZSBub25jZQ= "))); // Trailing space
        Assert.assertFalse(WebSocketHandshake.isValidKey(new Utf8String(" dGhlIHNhbXBsZSBub25jZQ="))); // Leading space
    }

    @Test
    public void testProtocolWithSpecialCharacters() {
        long buf = allocateBuffer(512);
        try {
            String acceptKey = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";
            String protocol = "qwp.streaming"; // Protocol with dots
            int written = WebSocketHandshake.writeResponseWithProtocol(buf, acceptKey, protocol);

            byte[] response = readBytes(buf, written);
            String responseStr = new String(response, StandardCharsets.US_ASCII);

            Assert.assertTrue(responseStr.contains("Sec-WebSocket-Protocol: " + protocol + "\r\n"));
        } finally {
            freeBuffer(buf, 512);
        }
    }
}
