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

package io.questdb.test.cutlass.qwp.e2e;

import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.websocket.WebSocketCloseCode;
import io.questdb.cutlass.qwp.websocket.WebSocketOpcode;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;

/**
 * Tests for WebSocket upgrade handshake and protocol-level frame handling on the
 * /write/v4 endpoint. Verifies upgrade negotiation, close code normalization per
 * RFC 6455, and that the server rejects fragmented frames with a clear error
 * instead of silently dropping data.
 */
public class QwpWebSocketProtocolTest extends AbstractQwpWebSocketTest {

    private static final String WEBSOCKET_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

    @Test
    public void testCloseFrameEchoesValidCode() throws Exception {
        sendCloseAndAssertResponseCode(createClosePayload(1001), 1001);
    }

    @Test
    public void testCloseFrameExtensionCode2000RespondsWithProtocolError() throws Exception {
        sendCloseAndAssertResponseCode(createClosePayload(2000), WebSocketCloseCode.PROTOCOL_ERROR);
    }

    @Test
    public void testCloseFrameExtensionCode2999RespondsWithProtocolError() throws Exception {
        sendCloseAndAssertResponseCode(createClosePayload(2999), WebSocketCloseCode.PROTOCOL_ERROR);
    }

    @Test
    public void testCloseFrameNoPayloadRespondsWithNormalClosure() throws Exception {
        sendCloseAndAssertResponseCode(new byte[0], WebSocketCloseCode.NORMAL_CLOSURE);
    }

    @Test
    public void testCloseFrameReservedCode1004RespondsWithProtocolError() throws Exception {
        sendCloseAndAssertResponseCode(createClosePayload(1004), WebSocketCloseCode.PROTOCOL_ERROR);
    }

    @Test
    public void testCloseFrameReservedCode1005RespondsWithNormalClosure() throws Exception {
        sendCloseAndAssertResponseCode(createClosePayload(1005), WebSocketCloseCode.NORMAL_CLOSURE);
    }

    @Test
    public void testCloseFrameReservedCode1006RespondsWithNormalClosure() throws Exception {
        sendCloseAndAssertResponseCode(createClosePayload(1006), WebSocketCloseCode.NORMAL_CLOSURE);
    }

    @Test
    public void testCloseFrameReservedCode1015RespondsWithNormalClosure() throws Exception {
        sendCloseAndAssertResponseCode(createClosePayload(1015), WebSocketCloseCode.NORMAL_CLOSURE);
    }

    @Test
    public void testContinuationFrameRejected() throws Exception {
        // A standalone continuation frame (opcode 0x00, FIN=1) simulates a proxy
        // forwarding a continuation fragment without the server ever seeing the
        // initial frame.
        sendFrameAndAssertCloseResponse(
                WebSocketOpcode.CONTINUATION,
                "orphaned continuation data".getBytes(StandardCharsets.UTF_8),
                true,
                WebSocketCloseCode.PROTOCOL_ERROR,
                "fragment"
        );
    }

    @Test
    public void testFragmentedBinaryFrameRejected() throws Exception {
        // A binary frame with FIN=0 is what a proxy sends when it splits a large
        // binary message.
        sendFrameAndAssertCloseResponse(
                WebSocketOpcode.BINARY,
                "first fragment of a split message".getBytes(StandardCharsets.UTF_8),
                false,
                WebSocketCloseCode.PROTOCOL_ERROR,
                "fragment"
        );
    }

    @Test
    public void testStatusCodesSynchronizedBetweenServerAndClient() {
        assertClientDecodesStatus(QwpConstants.STATUS_OK, "OK");
        assertClientDecodesStatus(QwpConstants.STATUS_PARSE_ERROR, "PARSE_ERROR");
        assertClientDecodesStatus(QwpConstants.STATUS_SCHEMA_MISMATCH, "SCHEMA_MISMATCH");
        assertClientDecodesStatus(QwpConstants.STATUS_WRITE_ERROR, "WRITE_ERROR");
        assertClientDecodesStatus(QwpConstants.STATUS_SECURITY_ERROR, "SECURITY_ERROR");
        assertClientDecodesStatus(QwpConstants.STATUS_INTERNAL_ERROR, "INTERNAL_ERROR");
    }

    @Test
    public void testTextFrameRejectedWithUnsupportedData() throws Exception {
        sendFrameAndAssertCloseResponse(
                WebSocketOpcode.TEXT,
                "hello".getBytes(StandardCharsets.UTF_8),
                true,
                WebSocketCloseCode.UNSUPPORTED_DATA,
                "text"
        );
    }

    @Test
    public void testVersionNegotiationDefaultsToOneWhenHeaderAbsent() throws Exception {
        runInContext((port) -> {
            byte[] keyBytes = new byte[16];
            for (int i = 0; i < 16; i++) {
                keyBytes[i] = (byte) (Math.random() * 256);
            }
            String wsKey = Base64.getEncoder().encodeToString(keyBytes);

            try (Socket socket = new Socket("localhost", port)) {
                socket.setSoTimeout(5000);
                OutputStream out = socket.getOutputStream();

                // No X-QWP-Max-Version header
                String request = "GET /write/v4 HTTP/1.1\r\n" +
                        "Host: localhost:" + port + "\r\n" +
                        "Upgrade: websocket\r\n" +
                        "Connection: Upgrade\r\n" +
                        "Sec-WebSocket-Key: " + wsKey + "\r\n" +
                        "Sec-WebSocket-Version: 13\r\n" +
                        "\r\n";

                out.write(request.getBytes(StandardCharsets.UTF_8));
                out.flush();

                String response = readHttpResponse(socket);
                Assert.assertTrue("Should be 101", response.startsWith("HTTP/1.1 101"));
                Assert.assertTrue("Should contain X-QWP-Version: 1",
                        response.toLowerCase().contains("x-qwp-version: 1"));
            }
        });
    }

    @Test
    public void testVersionNegotiationReturnsServerMax() throws Exception {
        runInContext((port) -> {
            byte[] keyBytes = new byte[16];
            for (int i = 0; i < 16; i++) {
                keyBytes[i] = (byte) (Math.random() * 256);
            }
            String wsKey = Base64.getEncoder().encodeToString(keyBytes);

            try (Socket socket = new Socket("localhost", port)) {
                socket.setSoTimeout(5000);
                OutputStream out = socket.getOutputStream();

                // Client claims high version — server caps to its max
                String request = "GET /write/v4 HTTP/1.1\r\n" +
                        "Host: localhost:" + port + "\r\n" +
                        "Upgrade: websocket\r\n" +
                        "Connection: Upgrade\r\n" +
                        "Sec-WebSocket-Key: " + wsKey + "\r\n" +
                        "Sec-WebSocket-Version: 13\r\n" +
                        "X-QWP-Max-Version: 99\r\n" +
                        "\r\n";

                out.write(request.getBytes(StandardCharsets.UTF_8));
                out.flush();

                String response = readHttpResponse(socket);
                Assert.assertTrue("Should be 101", response.startsWith("HTTP/1.1 101"));
                Assert.assertTrue("Should contain X-QWP-Version: 1",
                        response.toLowerCase().contains("x-qwp-version: 1"));
            }
        });
    }

    @Test
    public void testVersionNegotiationWithClientId() throws Exception {
        runInContext((port) -> {
            byte[] keyBytes = new byte[16];
            for (int i = 0; i < 16; i++) {
                keyBytes[i] = (byte) (Math.random() * 256);
            }
            String wsKey = Base64.getEncoder().encodeToString(keyBytes);

            try (Socket socket = new Socket("localhost", port)) {
                socket.setSoTimeout(5000);
                OutputStream out = socket.getOutputStream();

                String request = "GET /write/v4 HTTP/1.1\r\n" +
                        "Host: localhost:" + port + "\r\n" +
                        "Upgrade: websocket\r\n" +
                        "Connection: Upgrade\r\n" +
                        "Sec-WebSocket-Key: " + wsKey + "\r\n" +
                        "Sec-WebSocket-Version: 13\r\n" +
                        "X-QWP-Max-Version: 1\r\n" +
                        "X-QWP-Client-Id: test/1.0\r\n" +
                        "\r\n";

                out.write(request.getBytes(StandardCharsets.UTF_8));
                out.flush();

                String response = readHttpResponse(socket);
                Assert.assertTrue("Should be 101", response.startsWith("HTTP/1.1 101"));
                Assert.assertTrue("Should contain X-QWP-Version: 1",
                        response.toLowerCase().contains("x-qwp-version: 1"));
            }
        });
    }

    @Test
    public void testWebSocketUpgradeSucceeds() throws Exception {
        runInContext((port) -> {
            byte[] keyBytes = new byte[16];
            for (int i = 0; i < 16; i++) {
                keyBytes[i] = (byte) (Math.random() * 256);
            }
            String wsKey = Base64.getEncoder().encodeToString(keyBytes);

            try (Socket socket = new Socket("localhost", port)) {
                socket.setSoTimeout(5000);
                OutputStream out = socket.getOutputStream();

                String request = "GET /write/v4 HTTP/1.1\r\n" +
                        "Host: localhost:" + port + "\r\n" +
                        "Upgrade: websocket\r\n" +
                        "Connection: Upgrade\r\n" +
                        "Sec-WebSocket-Key: " + wsKey + "\r\n" +
                        "Sec-WebSocket-Version: 13\r\n" +
                        "\r\n";

                out.write(request.getBytes(StandardCharsets.UTF_8));
                out.flush();

                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));

                String statusLine = reader.readLine();
                Assert.assertNotNull("Should receive response", statusLine);
                Assert.assertTrue("Should be 101 Switching Protocols: " + statusLine,
                        statusLine.contains("101"));

                String line;
                String acceptKey = null;
                boolean hasUpgradeHeader = false;
                boolean hasConnectionHeader = false;
                boolean hasQwpVersionHeader = false;

                while ((line = reader.readLine()) != null && !line.isEmpty()) {
                    if (line.toLowerCase().startsWith("sec-websocket-accept:")) {
                        acceptKey = line.substring(line.indexOf(':') + 1).trim();
                    } else if (line.toLowerCase().startsWith("upgrade:")) {
                        hasUpgradeHeader = line.toLowerCase().contains("websocket");
                    } else if (line.toLowerCase().startsWith("connection:")) {
                        hasConnectionHeader = line.toLowerCase().contains("upgrade");
                    } else if (line.toLowerCase().startsWith("x-qwp-version:")) {
                        hasQwpVersionHeader = true;
                    }
                }

                Assert.assertTrue("Should have Upgrade: websocket header", hasUpgradeHeader);
                Assert.assertTrue("Should have Connection: Upgrade header", hasConnectionHeader);
                Assert.assertNotNull("Should have Sec-WebSocket-Accept header", acceptKey);
                Assert.assertEquals("Accept key should match", computeAcceptKey(wsKey), acceptKey);
                Assert.assertTrue("Should have X-QWP-Version header", hasQwpVersionHeader);
            }
        });
    }

    @Test
    public void testWebSocketUpgradeWithMissingKeyFails() throws Exception {
        runInContext((port) -> {
            try (Socket socket = new Socket("localhost", port)) {
                socket.setSoTimeout(5000);
                OutputStream out = socket.getOutputStream();

                String request = "GET /write/v4 HTTP/1.1\r\n" +
                        "Host: localhost:" + port + "\r\n" +
                        "Upgrade: websocket\r\n" +
                        "Connection: Upgrade\r\n" +
                        "Sec-WebSocket-Version: 13\r\n" +
                        "\r\n";

                out.write(request.getBytes(StandardCharsets.UTF_8));
                out.flush();

                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));

                String statusLine = reader.readLine();
                Assert.assertNotNull("Should receive response", statusLine);
                Assert.assertFalse("Should not upgrade without key: " + statusLine,
                        statusLine.contains("101"));
            }
        });
    }

    @Test
    public void testWebSocketUpgradeWithNonGetMethodFails() throws Exception {
        // RFC 6455 Section 4.1: the handshake must use GET. The HTTP request
        // validator rejects non-GET methods with 405 before the WebSocket
        // handshake validation runs.
        runInContext((port) -> {
            byte[] keyBytes = new byte[16];
            for (int i = 0; i < 16; i++) {
                keyBytes[i] = (byte) (Math.random() * 256);
            }
            String wsKey = Base64.getEncoder().encodeToString(keyBytes);

            try (Socket socket = new Socket("localhost", port)) {
                socket.setSoTimeout(5000);
                OutputStream out = socket.getOutputStream();

                String request = "POST /write/v4 HTTP/1.1\r\n" +
                        "Host: localhost:" + port + "\r\n" +
                        "Upgrade: websocket\r\n" +
                        "Connection: Upgrade\r\n" +
                        "Sec-WebSocket-Key: " + wsKey + "\r\n" +
                        "Sec-WebSocket-Version: 13\r\n" +
                        "Content-Length: 0\r\n" +
                        "\r\n";

                out.write(request.getBytes(StandardCharsets.UTF_8));
                out.flush();

                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));

                String statusLine = reader.readLine();
                Assert.assertNotNull("Should receive response", statusLine);
                Assert.assertFalse("Should not upgrade with POST: " + statusLine,
                        statusLine.contains("101"));
            }
        });
    }

    @Test
    public void testWebSocketUpgradeWithWrongVersionFails() throws Exception {
        runInContext((port) -> {
            byte[] keyBytes = new byte[16];
            for (int i = 0; i < 16; i++) {
                keyBytes[i] = (byte) (Math.random() * 256);
            }
            String wsKey = Base64.getEncoder().encodeToString(keyBytes);

            try (Socket socket = new Socket("localhost", port)) {
                socket.setSoTimeout(5000);
                OutputStream out = socket.getOutputStream();

                String request = "GET /write/v4 HTTP/1.1\r\n" +
                        "Host: localhost:" + port + "\r\n" +
                        "Upgrade: websocket\r\n" +
                        "Connection: Upgrade\r\n" +
                        "Sec-WebSocket-Key: " + wsKey + "\r\n" +
                        "Sec-WebSocket-Version: 8\r\n" +
                        "\r\n";

                out.write(request.getBytes(StandardCharsets.UTF_8));
                out.flush();

                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));

                String statusLine = reader.readLine();
                Assert.assertNotNull("Should receive response", statusLine);
                Assert.assertFalse("Should not upgrade with wrong version: " + statusLine,
                        statusLine.contains("101"));
            }
        });
    }

    /**
     * Constructs a raw binary response payload with the given status byte and
     * sequence=1, then verifies {@link WebSocketResponse#readFrom} decodes it
     * to the expected status name.
     */
    private static void assertClientDecodesStatus(byte statusByte, String expectedName) {
        // Build a raw response: status (1) + sequence (8) + [error: msgLen (2) + msg]
        int payloadSize;
        byte[] errorMsg = "test error".getBytes(StandardCharsets.UTF_8);
        if (statusByte == QwpConstants.STATUS_OK) {
            payloadSize = 9; // status + sequence only
        } else {
            payloadSize = 11 + errorMsg.length; // status + sequence + msgLen + msg
        }

        long ptr = Unsafe.malloc(payloadSize, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putByte(ptr, statusByte);
            Unsafe.putLong(ptr + 1, 1L); // sequence = 1
            if (statusByte != QwpConstants.STATUS_OK) {
                Unsafe.putShort(ptr + 9, (short) errorMsg.length);
                for (int i = 0; i < errorMsg.length; i++) {
                    Unsafe.putByte(ptr + 11 + i, errorMsg[i]);
                }
            }

            WebSocketResponse response = new WebSocketResponse();
            boolean parsed = response.readFrom(ptr, payloadSize);
            Assert.assertTrue("readFrom should succeed for status 0x"
                    + Integer.toHexString(statusByte & 0xFF), parsed);
            Assert.assertEquals(
                    "Status byte 0x" + Integer.toHexString(statusByte & 0xFF)
                            + " should decode to " + expectedName,
                    expectedName, response.getStatusName()
            );
        } finally {
            Unsafe.free(ptr, payloadSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    /**
     * Reads a WebSocket frame from the input stream and asserts it is a CLOSE
     * frame with the expected status code and optional reason substring.
     */
    private static void assertCloseFrame(InputStream in, int expectedCode, String expectedReasonSubstring) throws Exception {
        int byte0 = in.read();
        Assert.assertNotEquals("Server should send a CLOSE frame, not close the TCP connection", -1, byte0);

        int opcode = byte0 & 0x0F;
        Assert.assertEquals("Expected CLOSE frame opcode", WebSocketOpcode.CLOSE, opcode);

        boolean fin = (byte0 & 0x80) != 0;
        Assert.assertTrue("CLOSE frame must have FIN=1", fin);

        int byte1 = in.read();
        Assert.assertNotEquals(-1, byte1);

        boolean masked = (byte1 & 0x80) != 0;
        Assert.assertFalse("Server frames must not be masked", masked);

        int payloadLen = byte1 & 0x7F;
        Assert.assertTrue("CLOSE frame should have at least 2 bytes for the status code", payloadLen >= 2);

        byte[] payload = new byte[payloadLen];
        int totalRead = 0;
        while (totalRead < payloadLen) {
            int n = in.read(payload, totalRead, payloadLen - totalRead);
            Assert.assertNotEquals("Unexpected end of stream while reading CLOSE payload", -1, n);
            totalRead += n;
        }

        int closeCode = ((payload[0] & 0xFF) << 8) | (payload[1] & 0xFF);
        Assert.assertEquals("Unexpected close code in response", expectedCode, closeCode);

        if (expectedReasonSubstring != null && payloadLen > 2) {
            String reason = new String(payload, 2, payloadLen - 2, StandardCharsets.UTF_8);
            Assert.assertTrue(
                    "Close reason should contain '" + expectedReasonSubstring + "', got: " + reason,
                    reason.toLowerCase().contains(expectedReasonSubstring.toLowerCase())
            );
        }
    }

    private static String computeAcceptKey(String key) throws Exception {
        String combined = key + WEBSOCKET_GUID;
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] hash = sha1.digest(combined.getBytes(StandardCharsets.UTF_8));
        return Base64.getEncoder().encodeToString(hash);
    }

    /**
     * Creates a 2-byte big-endian close frame payload for the given status code.
     */
    private static byte[] createClosePayload(int code) {
        return new byte[]{(byte) ((code >> 8) & 0xFF), (byte) (code & 0xFF)};
    }

    /**
     * Creates a masked WebSocket frame (client-to-server frames must be masked
     * per RFC 6455).
     */
    private static byte[] createMaskedFrame(int opcode, byte[] payload, boolean fin) {
        byte[] maskKey = {0x12, 0x34, 0x56, 0x78};
        int payloadLen = payload.length;
        int headerLen = (payloadLen <= 125) ? 6 : (payloadLen <= 65_535) ? 8 : 14;

        byte[] frame = new byte[headerLen + payloadLen];
        int offset = 0;

        frame[offset++] = (byte) ((fin ? 0x80 : 0x00) | (opcode & 0x0F));

        if (payloadLen <= 125) {
            frame[offset++] = (byte) (0x80 | payloadLen);
        } else if (payloadLen <= 65_535) {
            frame[offset++] = (byte) (0x80 | 126);
            frame[offset++] = (byte) ((payloadLen >> 8) & 0xFF);
            frame[offset++] = (byte) (payloadLen & 0xFF);
        } else {
            frame[offset++] = (byte) (0x80 | 127);
            for (int i = 7; i >= 0; i--) {
                frame[offset++] = (byte) (((long) payloadLen >> (i * 8)) & 0xFF);
            }
        }

        System.arraycopy(maskKey, 0, frame, offset, 4);
        offset += 4;

        for (int i = 0; i < payloadLen; i++) {
            frame[offset + i] = (byte) (payload[i] ^ maskKey[i % 4]);
        }

        return frame;
    }

    /**
     * Performs the WebSocket upgrade handshake over the given socket and
     * verifies the server responds with 101 Switching Protocols.
     */
    private static void performWebSocketHandshake(Socket socket, int port) throws Exception {
        OutputStream out = socket.getOutputStream();
        InputStream in = socket.getInputStream();

        byte[] keyBytes = new byte[16];
        for (int i = 0; i < 16; i++) {
            keyBytes[i] = (byte) (i + 1);
        }
        String wsKey = Base64.getEncoder().encodeToString(keyBytes);

        String request = "GET /write/v4 HTTP/1.1\r\n" +
                "Host: localhost:" + port + "\r\n" +
                "Upgrade: websocket\r\n" +
                "Connection: Upgrade\r\n" +
                "Sec-WebSocket-Key: " + wsKey + "\r\n" +
                "Sec-WebSocket-Version: 13\r\n" +
                "\r\n";

        out.write(request.getBytes(StandardCharsets.UTF_8));
        out.flush();

        // Read the HTTP response byte-by-byte to avoid buffering past
        // the header boundary into the WebSocket frame data.
        StringBuilder response = new StringBuilder();
        while (true) {
            int b = in.read();
            Assert.assertNotEquals("Unexpected end of stream during handshake", -1, b);
            response.append((char) b);
            // Detect \r\n\r\n (end of HTTP headers)
            int len = response.length();
            if (len >= 4
                    && response.charAt(len - 4) == '\r' && response.charAt(len - 3) == '\n'
                    && response.charAt(len - 2) == '\r' && response.charAt(len - 1) == '\n') {
                break;
            }
        }

        String responseStr = response.toString();
        Assert.assertTrue(
                "Expected 101 Switching Protocols, got: " + responseStr.split("\r\n")[0],
                responseStr.startsWith("HTTP/1.1 101")
        );
    }

    private static String readHttpResponse(Socket socket) throws Exception {
        InputStream in = socket.getInputStream();
        StringBuilder response = new StringBuilder();
        while (true) {
            int b = in.read();
            Assert.assertNotEquals("Unexpected end of stream", -1, b);
            response.append((char) b);
            int len = response.length();
            if (len >= 4
                    && response.charAt(len - 4) == '\r' && response.charAt(len - 3) == '\n'
                    && response.charAt(len - 2) == '\r' && response.charAt(len - 1) == '\n') {
                break;
            }
        }
        return response.toString();
    }

    private void sendCloseAndAssertResponseCode(byte[] closePayload, int expectedResponseCode) throws Exception {
        sendFrameAndAssertCloseResponse(WebSocketOpcode.CLOSE, closePayload, true, expectedResponseCode, null);
    }

    /**
     * Sends a WebSocket frame and asserts the server responds with a CLOSE
     * frame carrying the expected code and optional reason substring.
     */
    private void sendFrameAndAssertCloseResponse(int opcode, byte[] payload, boolean fin, int expectedCode, String expectedReasonSubstring) throws Exception {
        runInContext((port) -> {
            try (Socket socket = new Socket("localhost", port)) {
                socket.setSoTimeout(5000);
                performWebSocketHandshake(socket, port);

                OutputStream out = socket.getOutputStream();
                InputStream in = socket.getInputStream();

                byte[] frame = createMaskedFrame(opcode, payload, fin);
                out.write(frame);
                out.flush();

                assertCloseFrame(in, expectedCode, expectedReasonSubstring);
            }
        });
    }
}
