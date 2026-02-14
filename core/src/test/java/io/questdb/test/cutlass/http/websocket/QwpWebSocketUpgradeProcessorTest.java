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

import io.questdb.cutlass.qwp.server.QwpWebSocketUpgradeProcessor;
import io.questdb.cutlass.qwp.websocket.WebSocketHandshake;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8String;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

/**
 * Tests for ILP v4 WebSocket upgrade processor.
 */
public class QwpWebSocketUpgradeProcessorTest extends AbstractWebSocketTest {

    // ==================== PROCESSOR CREATION TESTS ====================
    // Note: Processor instantiation requires CairoEngine and configuration
    // Full processor tests are covered by integration tests (QwpWebSocketHandshakeTest, etc.)

    // ==================== HANDSHAKE RESPONSE TESTS ====================

    @Test
    public void testWriteHandshakeResponse() {
        // Given a valid WebSocket key
        Utf8String key = new Utf8String("dGhlIHNhbXBsZSBub25jZQ==");
        String expectedAccept = WebSocketHandshake.computeAcceptKey(key);

        // When we write the handshake response
        long bufferSize = 256;
        long buffer = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
        try {
            int written = QwpWebSocketUpgradeProcessor.writeHandshakeResponse(buffer, (int) bufferSize, key);

            // Then the response should be a valid HTTP 101 response
            Assert.assertTrue("Response should be written", written > 0);

            // Verify the response content
            byte[] responseBytes = new byte[written];
            for (int i = 0; i < written; i++) {
                responseBytes[i] = Unsafe.getUnsafe().getByte(buffer + i);
            }
            String response = new String(responseBytes, StandardCharsets.US_ASCII);

            Assert.assertTrue("Response should start with HTTP/1.1 101", response.startsWith("HTTP/1.1 101"));
            Assert.assertTrue("Response should contain Upgrade: websocket", response.contains("Upgrade: websocket"));
            Assert.assertTrue("Response should contain Connection: Upgrade", response.contains("Connection: Upgrade"));
            Assert.assertTrue("Response should contain Sec-WebSocket-Accept", response.contains("Sec-WebSocket-Accept: " + expectedAccept));
            Assert.assertTrue("Response should end with double CRLF", response.endsWith("\r\n\r\n"));
        } finally {
            Unsafe.free(buffer, bufferSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testWriteHandshakeResponseWithRfc6455TestVector() {
        // RFC 6455 test vector
        Utf8String key = new Utf8String("dGhlIHNhbXBsZSBub25jZQ==");
        String expectedAccept = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";

        long bufferSize = 256;
        long buffer = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
        try {
            int written = QwpWebSocketUpgradeProcessor.writeHandshakeResponse(buffer, (int) bufferSize, key);

            byte[] responseBytes = new byte[written];
            for (int i = 0; i < written; i++) {
                responseBytes[i] = Unsafe.getUnsafe().getByte(buffer + i);
            }
            String response = new String(responseBytes, StandardCharsets.US_ASCII);

            Assert.assertTrue("Response should contain RFC 6455 expected accept key",
                    response.contains("Sec-WebSocket-Accept: " + expectedAccept));
        } finally {
            Unsafe.free(buffer, bufferSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testWriteHandshakeResponseSize() {
        Utf8String key = new Utf8String("dGhlIHNhbXBsZSBub25jZQ==");

        long bufferSize = 256;
        long buffer = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
        try {
            int written = QwpWebSocketUpgradeProcessor.writeHandshakeResponse(buffer, (int) bufferSize, key);

            // Response should be within expected size range
            // HTTP/1.1 101 Switching Protocols\r\n
            // Upgrade: websocket\r\n
            // Connection: Upgrade\r\n
            // Sec-WebSocket-Accept: <28 chars>\r\n
            // \r\n
            // Total: approximately 100-150 bytes
            Assert.assertTrue("Response size should be reasonable", written > 80 && written < 200);
        } finally {
            Unsafe.free(buffer, bufferSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testWriteHandshakeResponseBufferTooSmall() {
        Utf8String key = new Utf8String("dGhlIHNhbXBsZSBub25jZQ==");

        // Very small buffer that can't fit the response
        long bufferSize = 10;
        long buffer = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
        try {
            int written = QwpWebSocketUpgradeProcessor.writeHandshakeResponse(buffer, (int) bufferSize, key);

            // Should return -1 or 0 to indicate failure
            Assert.assertTrue("Should indicate buffer too small", written <= 0);
        } finally {
            Unsafe.free(buffer, bufferSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== ERROR RESPONSE TESTS ====================

    @Test
    public void testWriteBadRequestResponse() {
        long bufferSize = 512;
        long buffer = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
        try {
            String reason = "Invalid WebSocket handshake";
            int written = QwpWebSocketUpgradeProcessor.writeBadRequestResponse(buffer, (int) bufferSize, reason);

            Assert.assertTrue("Response should be written", written > 0);

            byte[] responseBytes = new byte[written];
            for (int i = 0; i < written; i++) {
                responseBytes[i] = Unsafe.getUnsafe().getByte(buffer + i);
            }
            String response = new String(responseBytes, StandardCharsets.US_ASCII);

            Assert.assertTrue("Response should be 400 Bad Request", response.startsWith("HTTP/1.1 400"));
            Assert.assertTrue("Response should contain reason", response.contains(reason));
        } finally {
            Unsafe.free(buffer, bufferSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testWriteUpgradeRequiredResponse() {
        long bufferSize = 256;
        long buffer = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
        try {
            int written = QwpWebSocketUpgradeProcessor.writeUpgradeRequiredResponse(buffer, (int) bufferSize);

            Assert.assertTrue("Response should be written", written > 0);

            byte[] responseBytes = new byte[written];
            for (int i = 0; i < written; i++) {
                responseBytes[i] = Unsafe.getUnsafe().getByte(buffer + i);
            }
            String response = new String(responseBytes, StandardCharsets.US_ASCII);

            Assert.assertTrue("Response should be 426 Upgrade Required", response.startsWith("HTTP/1.1 426"));
            Assert.assertTrue("Response should contain Upgrade header", response.contains("Upgrade: websocket"));
            Assert.assertTrue("Response should contain WebSocket version", response.contains("Sec-WebSocket-Version: 13"));
        } finally {
            Unsafe.free(buffer, bufferSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== RESPONSE SIZE CALCULATION TESTS ====================

    @Test
    public void testHandshakeResponseSize() {
        Utf8String key = new Utf8String("dGhlIHNhbXBsZSBub25jZQ==");
        int expectedSize = QwpWebSocketUpgradeProcessor.handshakeResponseSize(key);

        Assert.assertTrue("Expected size should be positive", expectedSize > 0);

        // Verify actual size matches expected
        long bufferSize = 256;
        long buffer = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
        try {
            int actualSize = QwpWebSocketUpgradeProcessor.writeHandshakeResponse(buffer, (int) bufferSize, key);
            Assert.assertEquals("Actual size should match expected size", expectedSize, actualSize);
        } finally {
            Unsafe.free(buffer, bufferSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== MULTIPLE KEYS TESTS ====================

    @Test
    public void testWriteHandshakeResponseMultipleKeys() {
        String[] keys = {
                "dGhlIHNhbXBsZSBub25jZQ==",
                "x3JJHMbDL1EzLkh9GBhXDw==",
                "HSmrc0sMlYUkAGmm5OPpG2Hg==",
                "5GxkDbvI5gIpPm3iRrHJyA=="
        };

        for (String keyStr : keys) {
            Utf8String key = new Utf8String(keyStr);
            String expectedAccept = WebSocketHandshake.computeAcceptKey(key);

            long bufferSize = 256;
            long buffer = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
            try {
                int written = QwpWebSocketUpgradeProcessor.writeHandshakeResponse(buffer, (int) bufferSize, key);

                Assert.assertTrue("Response should be written for key: " + keyStr, written > 0);

                byte[] responseBytes = new byte[written];
                for (int i = 0; i < written; i++) {
                    responseBytes[i] = Unsafe.getUnsafe().getByte(buffer + i);
                }
                String response = new String(responseBytes, StandardCharsets.US_ASCII);

                Assert.assertTrue("Response should contain correct accept key for: " + keyStr,
                        response.contains("Sec-WebSocket-Accept: " + expectedAccept));
            } finally {
                Unsafe.free(buffer, bufferSize, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }
}
