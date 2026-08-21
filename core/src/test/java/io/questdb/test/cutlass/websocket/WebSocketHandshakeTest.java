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

import io.questdb.cutlass.qwp.codec.DefaultQwpServerInfoProvider;
import io.questdb.cutlass.qwp.codec.QwpEgressMsgKind;
import io.questdb.cutlass.qwp.server.QwpIngressHttpProcessor;
import io.questdb.cutlass.qwp.server.QwpIngressUpgradeProcessor;
import io.questdb.cutlass.qwp.server.egress.QwpEgressUpgradeProcessor;
import io.questdb.std.str.Utf8String;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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

        String accept1 = new String(QwpIngressHttpProcessor.computeAcceptKey(new Utf8String(clientKey)), StandardCharsets.US_ASCII);
        String accept2 = new String(QwpIngressHttpProcessor.computeAcceptKey(new Utf8String(clientKey)), StandardCharsets.US_ASCII);
        String accept3 = new String(QwpIngressHttpProcessor.computeAcceptKey(new Utf8String(clientKey)), StandardCharsets.US_ASCII);

        Assert.assertEquals(accept1, accept2);
        Assert.assertEquals(accept2, accept3);
    }

    @Test
    public void testComputeAcceptKeyDifferentKeys() {
        // Different keys should produce different accept values
        String key1 = "dGhlIHNhbXBsZSBub25jZQ==";
        String key2 = "x3JJHMbDL1EzLkh9GBhXDw==";

        String accept1 = new String(QwpIngressHttpProcessor.computeAcceptKey(new Utf8String(key1)), StandardCharsets.US_ASCII);
        String accept2 = new String(QwpIngressHttpProcessor.computeAcceptKey(new Utf8String(key2)), StandardCharsets.US_ASCII);

        Assert.assertNotEquals(accept1, accept2);
    }

    @Test
    public void testComputeAcceptKeyKnownValues() {
        // Additional test vectors to verify SHA-1 computation
        // RFC 6455 test vector
        String acceptKey = new String(QwpIngressHttpProcessor.computeAcceptKey(new Utf8String("dGhlIHNhbXBsZSBub25jZQ==")), StandardCharsets.US_ASCII);
        Assert.assertEquals("s3pPLMBiTxaQ9kYGzzhZRbK+xOo=", acceptKey);

        // Verify different keys produce different results
        String acceptKey2 = new String(QwpIngressHttpProcessor.computeAcceptKey(new Utf8String("x3JJHMbDL1EzLkh9GBhXDw==")), StandardCharsets.US_ASCII);
        Assert.assertNotEquals(acceptKey, acceptKey2);
    }

    @Test
    public void testComputeAcceptKeyRFCExample() {
        // Test vector from RFC 6455
        String clientKey = "dGhlIHNhbXBsZSBub25jZQ==";
        String expectedAccept = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";

        String acceptKey = new String(QwpIngressHttpProcessor.computeAcceptKey(new Utf8String(clientKey)), StandardCharsets.US_ASCII);
        Assert.assertEquals(expectedAccept, acceptKey);
    }

    @Test
    public void testComputeAcceptKeyUtf8() {
        // Same test with Utf8Sequence
        Utf8String clientKey = new Utf8String("dGhlIHNhbXBsZSBub25jZQ==");
        String expectedAccept = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";

        String acceptKey = new String(QwpIngressHttpProcessor.computeAcceptKey(clientKey), StandardCharsets.US_ASCII);
        Assert.assertEquals(expectedAccept, acceptKey);
    }

    @Test
    public void testConnectionHeaderCaseSensitivity() {
        // Connection header variations
        Assert.assertTrue(QwpIngressHttpProcessor.isConnectionUpgrade(new Utf8String("UPGRADE")));
        Assert.assertTrue(QwpIngressHttpProcessor.isConnectionUpgrade(new Utf8String("uPgRaDe")));
        Assert.assertTrue(QwpIngressHttpProcessor.isConnectionUpgrade(new Utf8String("keep-alive, UPGRADE, something")));
    }

    @Test
    public void testConnectionHeaderWithMultipleValues() {
        // Connection header can have multiple values
        Assert.assertTrue(QwpIngressHttpProcessor.isConnectionUpgrade(
                new Utf8String("keep-alive, Upgrade")));
        Assert.assertTrue(QwpIngressHttpProcessor.isConnectionUpgrade(
                new Utf8String("Upgrade, keep-alive")));
        Assert.assertTrue(QwpIngressHttpProcessor.isConnectionUpgrade(
                new Utf8String("Connection, Upgrade, keep-alive")));
    }

    @Test
    public void testIsConnectionUpgrade() {
        Assert.assertTrue(QwpIngressHttpProcessor.isConnectionUpgrade(new Utf8String("Upgrade")));
        Assert.assertTrue(QwpIngressHttpProcessor.isConnectionUpgrade(new Utf8String("upgrade")));
        Assert.assertTrue(QwpIngressHttpProcessor.isConnectionUpgrade(new Utf8String("UPGRADE")));
        Assert.assertTrue(QwpIngressHttpProcessor.isConnectionUpgrade(new Utf8String("keep-alive, Upgrade")));
        Assert.assertTrue(QwpIngressHttpProcessor.isConnectionUpgrade(new Utf8String("Upgrade, keep-alive")));
    }

    @Test
    public void testIsInvalidKey() {
        Assert.assertFalse(QwpIngressHttpProcessor.isValidKey(null));
        Assert.assertFalse(QwpIngressHttpProcessor.isValidKey(new Utf8String("")));
        Assert.assertFalse(QwpIngressHttpProcessor.isValidKey(new Utf8String("short")));
        Assert.assertFalse(QwpIngressHttpProcessor.isValidKey(new Utf8String("waytoolongforavalidbase64keyvalue==")));
        Assert.assertFalse(QwpIngressHttpProcessor.isValidKey(new Utf8String("invalid!chars!here!==")));
        Assert.assertFalse(QwpIngressHttpProcessor.isValidKey(new Utf8String("has spaces in it  ==")));
    }

    @Test
    public void testIsInvalidVersion() {
        Assert.assertFalse(QwpIngressHttpProcessor.isValidVersion(null));
        Assert.assertFalse(QwpIngressHttpProcessor.isValidVersion(new Utf8String("")));
        Assert.assertFalse(QwpIngressHttpProcessor.isValidVersion(new Utf8String("12")));
        Assert.assertFalse(QwpIngressHttpProcessor.isValidVersion(new Utf8String("14")));
        Assert.assertFalse(QwpIngressHttpProcessor.isValidVersion(new Utf8String("0")));
        Assert.assertFalse(QwpIngressHttpProcessor.isValidVersion(new Utf8String("8")));
        Assert.assertFalse(QwpIngressHttpProcessor.isValidVersion(new Utf8String("abc")));
        Assert.assertFalse(QwpIngressHttpProcessor.isValidVersion(new Utf8String("13a")));
        Assert.assertFalse(QwpIngressHttpProcessor.isValidVersion(new Utf8String("1 3")));
    }

    @Test
    public void testIsNotConnectionUpgrade() {
        Assert.assertFalse(QwpIngressHttpProcessor.isConnectionUpgrade(null));
        Assert.assertFalse(QwpIngressHttpProcessor.isConnectionUpgrade(new Utf8String("")));
        Assert.assertFalse(QwpIngressHttpProcessor.isConnectionUpgrade(new Utf8String("keep-alive")));
        Assert.assertFalse(QwpIngressHttpProcessor.isConnectionUpgrade(new Utf8String("close")));
        // must not match "upgrade" as a substring of another token
        Assert.assertFalse(QwpIngressHttpProcessor.isConnectionUpgrade(new Utf8String("notupgrade")));
        Assert.assertFalse(QwpIngressHttpProcessor.isConnectionUpgrade(new Utf8String("upgradex")));
        Assert.assertFalse(QwpIngressHttpProcessor.isConnectionUpgrade(new Utf8String("xupgradex")));
        Assert.assertFalse(QwpIngressHttpProcessor.isConnectionUpgrade(new Utf8String("keep-alive, notupgrade")));
        Assert.assertFalse(QwpIngressHttpProcessor.isConnectionUpgrade(new Utf8String("preupgrade, keep-alive")));
    }

    @Test
    public void testIsNotWebSocketUpgrade() {
        Assert.assertFalse(QwpIngressHttpProcessor.isWebSocketUpgrade(null));
        Assert.assertFalse(QwpIngressHttpProcessor.isWebSocketUpgrade(new Utf8String("")));
        Assert.assertFalse(QwpIngressHttpProcessor.isWebSocketUpgrade(new Utf8String("http")));
        Assert.assertFalse(QwpIngressHttpProcessor.isWebSocketUpgrade(new Utf8String("websocket-extension")));
        Assert.assertFalse(QwpIngressHttpProcessor.isWebSocketUpgrade(new Utf8String("web")));
    }

    @Test
    public void testIsValidKey() {
        // Valid base64-encoded 16-byte keys (24 chars)
        Assert.assertTrue(QwpIngressHttpProcessor.isValidKey(new Utf8String("dGhlIHNhbXBsZSBub25jZQ==")));
        Assert.assertTrue(QwpIngressHttpProcessor.isValidKey(new Utf8String("x3JJHMbDL1EzLkh9GBhXDw==")));
        Assert.assertTrue(QwpIngressHttpProcessor.isValidKey(new Utf8String("AAAAAAAAAAAAAAAAAAAAAA==")));
        Assert.assertTrue(QwpIngressHttpProcessor.isValidKey(new Utf8String("0123456789ABCDEFGHIJ+/==")));
    }

    @Test
    public void testIsValidVersion() {
        Assert.assertTrue(QwpIngressHttpProcessor.isValidVersion(new Utf8String("13")));
    }

    @Test
    public void testIsWebSocketUpgrade() {
        Assert.assertTrue(QwpIngressHttpProcessor.isWebSocketUpgrade(new Utf8String("websocket")));
        Assert.assertTrue(QwpIngressHttpProcessor.isWebSocketUpgrade(new Utf8String("WebSocket")));
        Assert.assertTrue(QwpIngressHttpProcessor.isWebSocketUpgrade(new Utf8String("WEBSOCKET")));
        Assert.assertTrue(QwpIngressHttpProcessor.isWebSocketUpgrade(new Utf8String("WeBsOcKeT")));
    }

    @Test
    public void testContainsWebSocketProtocol() {
        Utf8String durableAck = QwpIngressHttpProcessor.WEBSOCKET_PROTOCOL_QWP_DURABLE_ACK;
        Assert.assertTrue(QwpIngressHttpProcessor.containsWebSocketProtocol(
                new Utf8String("questdb.qwp.durable-ack.v1"), durableAck));
        Assert.assertTrue(QwpIngressHttpProcessor.containsWebSocketProtocol(
                new Utf8String("application.v1, questdb.qwp.durable-ack.v1\t"), durableAck));
        Assert.assertFalse(QwpIngressHttpProcessor.containsWebSocketProtocol(
                new Utf8String("application.v1,questdb.qwp.durable-ack.v10"), durableAck));
        Assert.assertFalse(QwpIngressHttpProcessor.containsWebSocketProtocol(
                new Utf8String("QUESTDB.QWP.DURABLE-ACK.V1"), durableAck));
    }

    @Test
    public void testKeyWithAllBase64Characters() {
        // Test key containing varied base64 characters
        // Use a valid 24-char base64 string with varied characters including +/
        Assert.assertTrue(QwpIngressHttpProcessor.isValidKey(new Utf8String("abcd+/0123456789ABCDEF==")));
    }

    @Test
    public void testKeyWithTrailingWhitespace() {
        // Keys should not have whitespace
        Assert.assertFalse(QwpIngressHttpProcessor.isValidKey(new Utf8String("dGhlIHNhbXBsZSBub25jZQ= "))); // Trailing space
        Assert.assertFalse(QwpIngressHttpProcessor.isValidKey(new Utf8String(" dGhlIHNhbXBsZSBub25jZQ="))); // Leading space
    }

    @Test
    public void testResponseSize() throws Exception {
        assertMemoryLeak(() -> {
            byte[] acceptKey = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=".getBytes(StandardCharsets.US_ASCII);
            int expectedSize = QwpIngressHttpProcessor.responseSize(acceptKey, 1);

            long buf = allocateBuffer(256);
            try {
                int written = QwpIngressHttpProcessor.writeResponse(buf, acceptKey, 1);
                Assert.assertEquals(expectedSize, written);
            } finally {
                freeBuffer(buf, 256);
            }
        });
    }

    @Test
    public void testResponseSizeWithMaxBatchSize() throws Exception {
        assertMemoryLeak(() -> {
            byte[] acceptKey = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=".getBytes(StandardCharsets.US_ASCII);
            int maxBatchSize = 16 * 1024 * 1024;
            byte[] maxBatchSizeBytes = Integer.toString(maxBatchSize).getBytes(StandardCharsets.US_ASCII);
            int expectedSize = QwpIngressHttpProcessor.responseSize(
                    acceptKey, 1, null, false, null, maxBatchSizeBytes);

            long buf = allocateBuffer(512);
            try {
                int written = QwpIngressHttpProcessor.writeResponse(
                        buf, acceptKey, 1, null, false, null, maxBatchSizeBytes);
                Assert.assertEquals(expectedSize, written);

                String response = new String(readBytes(buf, written), StandardCharsets.US_ASCII);
                Assert.assertTrue("expected X-QWP-Max-Batch-Size header, got: " + response,
                        response.contains("X-QWP-Max-Batch-Size: " + maxBatchSize + "\r\n"));
                Assert.assertTrue(response.endsWith("\r\n\r\n"));
            } finally {
                freeBuffer(buf, 512);
            }
        });
    }

    @Test
    public void testResponseWithDurableAckWebSocketProtocol() throws Exception {
        assertMemoryLeak(() -> {
            byte[] acceptKey = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=".getBytes(StandardCharsets.US_ASCII);
            int expectedSize = QwpIngressHttpProcessor.responseSize(
                    acceptKey, 1, null, true, null, null, null, true);

            long buf = allocateBuffer(512);
            try {
                int written = QwpIngressHttpProcessor.writeResponse(
                        buf, acceptKey, 1, null, true, null, null, null, true);
                Assert.assertEquals(expectedSize, written);

                String response = new String(readBytes(buf, written), StandardCharsets.US_ASCII);
                Assert.assertTrue(response.contains("X-QWP-Durable-Ack: enabled\r\n"));
                Assert.assertTrue(response.contains(
                        "Sec-WebSocket-Protocol: questdb.qwp.durable-ack.v1\r\n"));
            } finally {
                freeBuffer(buf, 512);
            }
        });
    }

    @Test
    public void testBrowserIngressServerInfoFrame() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                int written = QwpIngressUpgradeProcessor.writeBrowserServerInfoFrame(
                        buf,
                        1_048_576
                );
                Assert.assertEquals(7, written);
                byte[] frame = readBytes(buf, written);
                Assert.assertEquals((byte) 0x82, frame[0]);
                Assert.assertEquals(5, frame[1]);
                Assert.assertEquals(1, frame[2]);
                Assert.assertEquals(0, frame[3]);
                Assert.assertEquals(0, frame[4]);
                Assert.assertEquals(16, frame[5]);
                Assert.assertEquals(0, frame[6]);
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testBrowserEgressServerInfoCompressionTrailer() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(256);
            try {
                int written = QwpEgressUpgradeProcessor.writeServerInfoFrame(
                        buf,
                        256,
                        (byte) 1,
                        DefaultQwpServerInfoProvider.INSTANCE,
                        0,
                        true,
                        (byte) 1,
                        (byte) 3
                );
                byte[] frame = readBytes(buf, written);
                Assert.assertEquals((byte) 0x82, frame[0]);
                int capabilities = ByteBuffer.wrap(frame)
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .getInt(24);
                Assert.assertNotEquals(0, capabilities & QwpEgressMsgKind.CAP_COMPRESSION);
                Assert.assertEquals(1, frame[written - 2]);
                Assert.assertEquals(3, frame[written - 1]);
            } finally {
                freeBuffer(buf, 256);
            }
        });
    }

    @Test
    public void testResponseWithRotatedSessionCookie() throws Exception {
        assertMemoryLeak(() -> {
            byte[] acceptKey = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=".getBytes(StandardCharsets.US_ASCII);
            byte[] cookieValue = "qs1_rotated; HttpOnly; Path=/; SameSite=Strict; Max-Age=2592000"
                    .getBytes(StandardCharsets.US_ASCII);
            int expectedSize = QwpIngressHttpProcessor.responseSize(
                    acceptKey, 1, null, false, null, null, cookieValue);

            long buf = allocateBuffer(512);
            try {
                int written = QwpIngressHttpProcessor.writeResponse(
                        buf, acceptKey, 1, null, false, null, null, cookieValue);
                Assert.assertEquals(expectedSize, written);

                String response = new String(readBytes(buf, written), StandardCharsets.US_ASCII);
                Assert.assertTrue(response.contains(
                        "Set-Cookie: qdb_session=qs1_rotated; HttpOnly; Path=/; SameSite=Strict; Max-Age=2592000\r\n"
                ));
                Assert.assertTrue(response.endsWith("\r\n\r\n"));
            } finally {
                freeBuffer(buf, 512);
            }
        });
    }

    @Test
    public void testWriteResponseOmitsMaxBatchSizeWhenAbsent() throws Exception {
        assertMemoryLeak(() -> {
            byte[] acceptKey = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=".getBytes(StandardCharsets.US_ASCII);

            long buf = allocateBuffer(256);
            try {
                int written = QwpIngressHttpProcessor.writeResponse(
                        buf, acceptKey, 1, null, false, null, null);

                String response = new String(readBytes(buf, written), StandardCharsets.US_ASCII);
                Assert.assertFalse("did not expect X-QWP-Max-Batch-Size header, got: " + response,
                        response.contains("X-QWP-Max-Batch-Size"));
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
                    String accept = new String(QwpIngressHttpProcessor.computeAcceptKey(new Utf8String(clientKey)), StandardCharsets.US_ASCII);
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
        Assert.assertTrue(QwpIngressHttpProcessor.isWebSocketUpgrade(new Utf8String("WEBSOCKET")));
        Assert.assertTrue(QwpIngressHttpProcessor.isWebSocketUpgrade(new Utf8String("wEbSoCkEt")));
    }

    @Test
    public void testVersionHeaderEdgeCases() {
        // Edge cases for version parsing
        Assert.assertFalse(QwpIngressHttpProcessor.isValidVersion(new Utf8String("1 3"))); // Space in middle
        Assert.assertFalse(QwpIngressHttpProcessor.isValidVersion(new Utf8String(" 13"))); // Leading space
        Assert.assertFalse(QwpIngressHttpProcessor.isValidVersion(new Utf8String("13 "))); // Trailing space
        Assert.assertFalse(QwpIngressHttpProcessor.isValidVersion(new Utf8String("-13"))); // Negative sign
        Assert.assertFalse(QwpIngressHttpProcessor.isValidVersion(new Utf8String("13.0"))); // Decimal
        Assert.assertFalse(QwpIngressHttpProcessor.isValidVersion(new Utf8String("1a3"))); // Letter in middle
    }

    @Test
    public void testWriteResponse() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(256);
            try {
                String acceptKeyStr = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";
                byte[] acceptKey = acceptKeyStr.getBytes(StandardCharsets.US_ASCII);
                int written = QwpIngressHttpProcessor.writeResponse(buf, acceptKey, 1);

                byte[] response = readBytes(buf, written);
                String responseStr = new String(response, StandardCharsets.US_ASCII);

                Assert.assertTrue(responseStr.startsWith("HTTP/1.1 101 Switching Protocols\r\n"));
                Assert.assertTrue(responseStr.contains("Upgrade: websocket\r\n"));
                Assert.assertTrue(responseStr.contains("Connection: Upgrade\r\n"));
                Assert.assertTrue(responseStr.contains("Sec-WebSocket-Accept: " + acceptKeyStr + "\r\n"));
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
            byte[] acceptKey = QwpIngressHttpProcessor.computeAcceptKey(new Utf8String(clientKey));

            long buf = allocateBuffer(256);
            try {
                int written = QwpIngressHttpProcessor.writeResponse(buf, acceptKey, 1);

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
