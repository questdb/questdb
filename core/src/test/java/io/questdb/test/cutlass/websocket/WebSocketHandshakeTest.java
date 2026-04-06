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

import io.questdb.cutlass.qwp.server.QwpWebSocketHttpProcessor;
import io.questdb.std.str.Utf8String;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

/**
 * Tests for WebSocket handshake processing.
 */
public class WebSocketHandshakeTest extends AbstractWebSocketTest {

    @Test
    public void testComputeAcceptKeyConsistent() {
        // Same key should always produce same accept value
        String clientKey = "x3JJHMbDL1EzLkh9GBhXDw==";

        String accept1 = QwpWebSocketHttpProcessor.computeAcceptKey(new Utf8String(clientKey));
        String accept2 = QwpWebSocketHttpProcessor.computeAcceptKey(new Utf8String(clientKey));
        String accept3 = QwpWebSocketHttpProcessor.computeAcceptKey(new Utf8String(clientKey));

        Assert.assertEquals(accept1, accept2);
        Assert.assertEquals(accept2, accept3);
    }

    @Test
    public void testComputeAcceptKeyDifferentKeys() {
        // Different keys should produce different accept values
        String key1 = "dGhlIHNhbXBsZSBub25jZQ==";
        String key2 = "x3JJHMbDL1EzLkh9GBhXDw==";

        String accept1 = QwpWebSocketHttpProcessor.computeAcceptKey(new Utf8String(key1));
        String accept2 = QwpWebSocketHttpProcessor.computeAcceptKey(new Utf8String(key2));

        Assert.assertNotEquals(accept1, accept2);
    }

    @Test
    public void testComputeAcceptKeyKnownValues() {
        // Additional test vectors to verify SHA-1 computation
        // RFC 6455 test vector
        String acceptKey = QwpWebSocketHttpProcessor.computeAcceptKey(new Utf8String("dGhlIHNhbXBsZSBub25jZQ=="));
        Assert.assertEquals("s3pPLMBiTxaQ9kYGzzhZRbK+xOo=", acceptKey);

        // Verify different keys produce different results
        String acceptKey2 = QwpWebSocketHttpProcessor.computeAcceptKey(new Utf8String("x3JJHMbDL1EzLkh9GBhXDw=="));
        Assert.assertNotEquals(acceptKey, acceptKey2);
    }

    @Test
    public void testComputeAcceptKeyRFCExample() {
        // Test vector from RFC 6455
        String clientKey = "dGhlIHNhbXBsZSBub25jZQ==";
        String expectedAccept = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";

        String acceptKey = QwpWebSocketHttpProcessor.computeAcceptKey(new Utf8String(clientKey));
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
        // must not match "upgrade" as a substring of another token
        Assert.assertFalse(QwpWebSocketHttpProcessor.isConnectionUpgrade(new Utf8String("notupgrade")));
        Assert.assertFalse(QwpWebSocketHttpProcessor.isConnectionUpgrade(new Utf8String("upgradex")));
        Assert.assertFalse(QwpWebSocketHttpProcessor.isConnectionUpgrade(new Utf8String("xupgradex")));
        Assert.assertFalse(QwpWebSocketHttpProcessor.isConnectionUpgrade(new Utf8String("keep-alive, notupgrade")));
        Assert.assertFalse(QwpWebSocketHttpProcessor.isConnectionUpgrade(new Utf8String("preupgrade, keep-alive")));
    }

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
    public void testResponseSize() throws Exception {
        assertMemoryLeak(() -> {
            String acceptKey = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";
            int expectedSize = QwpWebSocketHttpProcessor.responseSize(acceptKey, 1);

            long buf = allocateBuffer(256);
            try {
                int written = QwpWebSocketHttpProcessor.writeResponse(buf, acceptKey, 1);
                Assert.assertEquals(expectedSize, written);
            } finally {
                freeBuffer(buf, 256);
            }
        });
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
                    String accept = QwpWebSocketHttpProcessor.computeAcceptKey(new Utf8String(clientKey));
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
    public void testVersionHeaderEdgeCases() {
        // Edge cases for version parsing
        Assert.assertFalse(QwpWebSocketHttpProcessor.isValidVersion(new Utf8String("1 3"))); // Space in middle
        Assert.assertFalse(QwpWebSocketHttpProcessor.isValidVersion(new Utf8String(" 13"))); // Leading space
        Assert.assertFalse(QwpWebSocketHttpProcessor.isValidVersion(new Utf8String("13 "))); // Trailing space
        Assert.assertFalse(QwpWebSocketHttpProcessor.isValidVersion(new Utf8String("-13"))); // Negative sign
        Assert.assertFalse(QwpWebSocketHttpProcessor.isValidVersion(new Utf8String("13.0"))); // Decimal
        Assert.assertFalse(QwpWebSocketHttpProcessor.isValidVersion(new Utf8String("1a3"))); // Letter in middle
    }

    @Test
    public void testWriteResponse() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(256);
            try {
                String acceptKey = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";
                int written = QwpWebSocketHttpProcessor.writeResponse(buf, acceptKey, 1);

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
        });
    }

    @Test
    public void testWriteResponseComplete() throws Exception {
        assertMemoryLeak(() -> {
            // Full end-to-end test
            String clientKey = "dGhlIHNhbXBsZSBub25jZQ==";
            String acceptKey = QwpWebSocketHttpProcessor.computeAcceptKey(new Utf8String(clientKey));

            long buf = allocateBuffer(256);
            try {
                int written = QwpWebSocketHttpProcessor.writeResponse(buf, acceptKey, 1);

                byte[] response = readBytes(buf, written);
                String responseStr = new String(response, StandardCharsets.US_ASCII);

                // Verify full response
                String expected = """
                        HTTP/1.1 101 Switching Protocols\r
                        Upgrade: websocket\r
                        Connection: Upgrade\r
                        Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r
                        X-QWP-Version: 1\r
                        \r
                        """;
                Assert.assertEquals(expected, responseStr);
            } finally {
                freeBuffer(buf, 256);
            }
        });
    }
}
