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

import io.questdb.PropertyKey;
import io.questdb.cutlass.qwp.websocket.WebSocketCloseCode;
import io.questdb.cutlass.qwp.websocket.WebSocketOpcode;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
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
 * /write/v4 endpoint. Verifies upgrade negotiation, and that the server rejects
 * fragmented frames with a clear error instead of silently dropping data.
 */
public class QwpWebSocketProtocolTest extends AbstractBootstrapTest {

    private static final String WEBSOCKET_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

    @Override
    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testContinuationFrameRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Socket socket = new Socket("localhost", httpPort)) {
                    socket.setSoTimeout(5000);
                    performWebSocketHandshake(socket, httpPort);

                    OutputStream out = socket.getOutputStream();
                    InputStream in = socket.getInputStream();

                    // Send a standalone continuation frame (opcode 0x00, FIN=1).
                    // This simulates a proxy forwarding a continuation fragment
                    // without the server ever seeing the initial frame.
                    byte[] payload = "orphaned continuation data".getBytes(StandardCharsets.UTF_8);
                    byte[] frame = createMaskedFrame(WebSocketOpcode.CONTINUATION, payload, true);
                    out.write(frame);
                    out.flush();

                    // The server should respond with a CLOSE frame (1002 Protocol Error).
                    assertCloseFrameWithProtocolError(in);
                }
            }
        });
    }

    @Test
    public void testFragmentedBinaryFrameRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Socket socket = new Socket("localhost", httpPort)) {
                    socket.setSoTimeout(5000);
                    performWebSocketHandshake(socket, httpPort);

                    OutputStream out = socket.getOutputStream();
                    InputStream in = socket.getInputStream();

                    // Send a binary frame with FIN=0 (start of a fragmented message).
                    // This is what a proxy does when it splits a large binary message.
                    byte[] payload = "first fragment of a split message".getBytes(StandardCharsets.UTF_8);
                    byte[] frame = createMaskedFrame(WebSocketOpcode.BINARY, payload, false);
                    out.write(frame);
                    out.flush();

                    // The server should respond with a CLOSE frame (1002 Protocol Error).
                    assertCloseFrameWithProtocolError(in);
                }
            }
        });
    }

    @Test
    public void testWebSocketUpgradeSucceeds() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                // Generate a random WebSocket key
                byte[] keyBytes = new byte[16];
                for (int i = 0; i < 16; i++) {
                    keyBytes[i] = (byte) (Math.random() * 256);
                }
                String wsKey = Base64.getEncoder().encodeToString(keyBytes);

                // Send WebSocket upgrade request
                try (Socket socket = new Socket("localhost", httpPort)) {
                    socket.setSoTimeout(5000);
                    OutputStream out = socket.getOutputStream();

                    String request = "GET /write/v4 HTTP/1.1\r\n" +
                            "Host: localhost:" + httpPort + "\r\n" +
                            "Upgrade: websocket\r\n" +
                            "Connection: Upgrade\r\n" +
                            "Sec-WebSocket-Key: " + wsKey + "\r\n" +
                            "Sec-WebSocket-Version: 13\r\n" +
                            "\r\n";

                    out.write(request.getBytes(StandardCharsets.UTF_8));
                    out.flush();

                    // Read response
                    BufferedReader reader = new BufferedReader(
                            new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));

                    String statusLine = reader.readLine();
                    Assert.assertNotNull("Should receive response", statusLine);
                    Assert.assertTrue("Should be 101 Switching Protocols: " + statusLine,
                            statusLine.contains("101"));

                    // Read headers
                    String line;
                    String acceptKey = null;
                    boolean hasUpgradeHeader = false;
                    boolean hasConnectionHeader = false;

                    while ((line = reader.readLine()) != null && !line.isEmpty()) {
                        if (line.toLowerCase().startsWith("sec-websocket-accept:")) {
                            acceptKey = line.substring(line.indexOf(':') + 1).trim();
                        } else if (line.toLowerCase().startsWith("upgrade:")) {
                            hasUpgradeHeader = line.toLowerCase().contains("websocket");
                        } else if (line.toLowerCase().startsWith("connection:")) {
                            hasConnectionHeader = line.toLowerCase().contains("upgrade");
                        }
                    }

                    Assert.assertTrue("Should have Upgrade: websocket header", hasUpgradeHeader);
                    Assert.assertTrue("Should have Connection: Upgrade header", hasConnectionHeader);
                    Assert.assertNotNull("Should have Sec-WebSocket-Accept header", acceptKey);

                    // Verify the accept key
                    String expectedAccept = computeAcceptKey(wsKey);
                    Assert.assertEquals("Accept key should match", expectedAccept, acceptKey);
                }
            }
        });
    }

    @Test
    public void testWebSocketUpgradeWithMissingKeyFails() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Socket socket = new Socket("localhost", httpPort)) {
                    socket.setSoTimeout(5000);
                    OutputStream out = socket.getOutputStream();

                    // Missing Sec-WebSocket-Key
                    String request = "GET /write/v4 HTTP/1.1\r\n" +
                            "Host: localhost:" + httpPort + "\r\n" +
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
                    // Should NOT be 101 - missing key
                    Assert.assertFalse("Should not upgrade without key: " + statusLine,
                            statusLine.contains("101"));
                }
            }
        });
    }

    @Test
    public void testWebSocketUpgradeWithWrongVersionFails() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                byte[] keyBytes = new byte[16];
                for (int i = 0; i < 16; i++) {
                    keyBytes[i] = (byte) (Math.random() * 256);
                }
                String wsKey = Base64.getEncoder().encodeToString(keyBytes);

                try (Socket socket = new Socket("localhost", httpPort)) {
                    socket.setSoTimeout(5000);
                    OutputStream out = socket.getOutputStream();

                    // Wrong version (8 instead of 13)
                    String request = "GET /write/v4 HTTP/1.1\r\n" +
                            "Host: localhost:" + httpPort + "\r\n" +
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
                    // Should NOT be 101 - wrong version
                    Assert.assertFalse("Should not upgrade with wrong version: " + statusLine,
                            statusLine.contains("101"));
                }
            }
        });
    }

    /**
     * Reads a WebSocket frame from the input stream and asserts it is a CLOSE
     * frame with status code 1002 (Protocol Error) and a reason containing
     * "fragmented".
     */
    private static void assertCloseFrameWithProtocolError(InputStream in) throws Exception {
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
        Assert.assertEquals(
                "Server should reject fragmented frames with PROTOCOL_ERROR (1002)",
                WebSocketCloseCode.PROTOCOL_ERROR,
                closeCode
        );

        if (payloadLen > 2) {
            String reason = new String(payload, 2, payloadLen - 2, StandardCharsets.UTF_8);
            Assert.assertTrue(
                    "Close reason should mention fragmentation for easy diagnosis, got: " + reason,
                    reason.toLowerCase().contains("fragment")
            );
        }
    }

    /**
     * Creates a masked WebSocket frame (client-to-server frames must be masked
     * per RFC 6455).
     */
    private static byte[] createMaskedFrame(int opcode, byte[] payload, boolean fin) {
        byte[] maskKey = {0x12, 0x34, 0x56, 0x78};
        int payloadLen = payload.length;
        int headerLen = (payloadLen <= 125) ? 6 : (payloadLen <= 65535) ? 8 : 14;

        byte[] frame = new byte[headerLen + payloadLen];
        int offset = 0;

        frame[offset++] = (byte) ((fin ? 0x80 : 0x00) | (opcode & 0x0F));

        if (payloadLen <= 125) {
            frame[offset++] = (byte) (0x80 | payloadLen);
        } else if (payloadLen <= 65535) {
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
    private static void performWebSocketHandshake(Socket socket, int httpPort) throws Exception {
        OutputStream out = socket.getOutputStream();
        InputStream in = socket.getInputStream();

        byte[] keyBytes = new byte[16];
        for (int i = 0; i < 16; i++) {
            keyBytes[i] = (byte) (i + 1);
        }
        String wsKey = Base64.getEncoder().encodeToString(keyBytes);

        String request = "GET /write/v4 HTTP/1.1\r\n" +
                "Host: localhost:" + httpPort + "\r\n" +
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

    /**
     * Computes the expected Sec-WebSocket-Accept value.
     */
    private String computeAcceptKey(String key) throws Exception {
        String combined = key + WEBSOCKET_GUID;
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] hash = sha1.digest(combined.getBytes(StandardCharsets.UTF_8));
        return Base64.getEncoder().encodeToString(hash);
    }
}
