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

package io.questdb.test.cutlass.qwp;

import io.questdb.cutlass.qwp.codec.QwpEgressMsgKind;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpVarint;
import io.questdb.cutlass.qwp.websocket.WebSocketOpcode;
import org.junit.Assert;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

/**
 * Raw-wire building blocks for QWP tests that must drive the WebSocket byte
 * stream directly (e.g. disconnect-while-parked scenarios that a managed
 * client's close-during-execute contract forbids, or browser-shaped upgrades
 * that carry cookies a managed client will not send).
 * <p>
 * Public because the Enterprise test tree drives the same wire from
 * {@code com.questdb.acl} and {@code com.questdb.cutlass.tls}; keep every
 * helper here rather than re-deriving the byte layouts per module.
 */
public final class QwpWireTestFixtures {
    /**
     * RFC 6455 handshake nonce. A public protocol value, not a secret; this
     * repository's {@code .gitleaks.toml} exempts it through the blanket
     * {@code (^|/)src/test/} path allowlist rather than a per-value rule.
     */
    public static final String WEBSOCKET_KEY = "AQIDBAUGBwgJCgsMDQ4PEA==";

    private QwpWireTestFixtures() {
    }

    /**
     * Asserts that {@code message} is a well-formed QWP v1 message whose first
     * payload byte is {@code expectedKind}, including that the header's declared
     * payload length matches the bytes actually present.
     */
    public static void assertQwpMessageKind(byte[] message, byte expectedKind) {
        Assert.assertTrue("QWP message is too short", message.length > QwpConstants.HEADER_SIZE);
        ByteBuffer header = ByteBuffer.wrap(message).order(ByteOrder.LITTLE_ENDIAN);
        Assert.assertEquals(QwpConstants.MAGIC_MESSAGE, header.getInt(QwpConstants.HEADER_OFFSET_MAGIC));
        Assert.assertEquals(QwpConstants.VERSION, message[QwpConstants.HEADER_OFFSET_VERSION]);
        Assert.assertEquals(
                message.length - QwpConstants.HEADER_SIZE,
                header.getInt(QwpConstants.HEADER_OFFSET_PAYLOAD_LENGTH)
        );
        Assert.assertEquals(expectedKind, message[QwpConstants.HEADER_SIZE]);
    }

    /**
     * Builds a browser-shaped WebSocket upgrade request: a real browser always
     * sends {@code Origin} and cannot attach {@code X-QWP-*} headers, so every
     * QWP browser test drives this exact shape.
     *
     * @param authority    value for {@code Host}, and the authority the
     *                     {@code Origin} is built from, so the request is
     *                     same-origin by construction
     * @param originScheme {@code http} or {@code https}
     * @param extraHeaders already-formatted {@code Name: value\r\n} lines
     *                     (cookies, credentials), or empty for none
     */
    public static String browserUpgradeRequest(String path, String authority, String originScheme, String extraHeaders) {
        return "GET " + path + " HTTP/1.1\r\n"
                + "Host: " + authority + "\r\n"
                + "Origin: " + originScheme + "://" + authority + "\r\n"
                + "Upgrade: websocket\r\n"
                + "Connection: Upgrade\r\n"
                + "Sec-WebSocket-Key: " + WEBSOCKET_KEY + "\r\n"
                + "Sec-WebSocket-Version: 13\r\n"
                + extraHeaders
                + "\r\n";
    }

    /**
     * msg_kind(1) + request_id(8 LE) + additional_bytes(varint).
     */
    public static byte[] buildCreditFrame(long requestId, long additionalBytes) {
        byte[] p = new byte[1 + 8 + 10];
        int i = 0;
        p[i++] = QwpEgressMsgKind.CREDIT;
        for (int s = 0; s < 8; s++) {
            p[i++] = (byte) (requestId >>> (8 * s));
        }
        i = QwpVarint.encode(p, i, additionalBytes);
        return Arrays.copyOf(p, i);
    }

    public static byte[] buildQueryRequest(long requestId, String sql) {
        return buildQueryRequest(requestId, sql, 0);
    }

    /**
     * msg_kind(1) + request_id(8 LE) + sql_len(varint) + sql + initial_credit(varint,
     * 0 = unbounded) + bind_count(0). SQL must be short enough for a single-byte
     * length varint.
     */
    public static byte[] buildQueryRequest(long requestId, String sql, long initialCredit) {
        byte[] sqlBytes = sql.getBytes(StandardCharsets.UTF_8);
        Assert.assertTrue("helper supports single-byte varint SQL lengths only", sqlBytes.length < 128);
        byte[] p = new byte[1 + 8 + 1 + sqlBytes.length + 10 + 1];
        int i = 0;
        p[i++] = QwpEgressMsgKind.QUERY_REQUEST;
        for (int s = 0; s < 8; s++) {
            p[i++] = (byte) (requestId >>> (8 * s));
        }
        p[i++] = (byte) sqlBytes.length;
        System.arraycopy(sqlBytes, 0, p, i, sqlBytes.length);
        i += sqlBytes.length;
        i = QwpVarint.encode(p, i, initialCredit);
        p[i++] = 0x00;
        return Arrays.copyOf(p, i);
    }

    /**
     * Builds a complete single-row QWP ingress message for a table shaped
     * {@code (value long, ts timestamp) timestamp(ts)}: header + one table
     * block carrying one {@code value} column and the designated timestamp,
     * which is written with an empty column name per the v1 layout.
     */
    public static byte[] encodeSingleLongRow(String tableName, long value, long timestampMicros) {
        byte[] tableNameBytes = tableName.getBytes(StandardCharsets.UTF_8);
        byte[] valueColumnName = "value".getBytes(StandardCharsets.UTF_8);
        byte[] payload = new byte[64 + tableNameBytes.length + valueColumnName.length];
        int i = 0;
        i = QwpVarint.encode(payload, i, tableNameBytes.length);
        System.arraycopy(tableNameBytes, 0, payload, i, tableNameBytes.length);
        i += tableNameBytes.length;
        i = QwpVarint.encode(payload, i, 1); // row count
        i = QwpVarint.encode(payload, i, 2); // column count
        i = QwpVarint.encode(payload, i, valueColumnName.length);
        System.arraycopy(valueColumnName, 0, payload, i, valueColumnName.length);
        i += valueColumnName.length;
        payload[i++] = QwpConstants.TYPE_LONG;
        i = QwpVarint.encode(payload, i, 0); // designated timestamp has an empty column name
        payload[i++] = QwpConstants.TYPE_TIMESTAMP;
        ByteBuffer payloadBuffer = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
        payload[i++] = 0; // value column has no nulls
        payloadBuffer.putLong(i, value);
        i += Long.BYTES;
        payload[i++] = 0; // timestamp column has no nulls
        payload[i++] = 0; // uncompressed timestamp encoding
        payloadBuffer.putLong(i, timestampMicros);
        i += Long.BYTES;

        byte[] message = new byte[QwpConstants.HEADER_SIZE + i];
        ByteBuffer messageBuffer = ByteBuffer.wrap(message).order(ByteOrder.LITTLE_ENDIAN);
        messageBuffer.putInt(QwpConstants.HEADER_OFFSET_MAGIC, QwpConstants.MAGIC_MESSAGE);
        message[QwpConstants.HEADER_OFFSET_VERSION] = QwpConstants.VERSION;
        message[QwpConstants.HEADER_OFFSET_FLAGS] = QwpConstants.FLAG_GORILLA;
        message[QwpConstants.HEADER_OFFSET_TABLE_COUNT] = 1;
        messageBuffer.putInt(QwpConstants.HEADER_OFFSET_PAYLOAD_LENGTH, i);
        System.arraycopy(payload, 0, message, QwpConstants.HEADER_SIZE, i);
        return message;
    }

    /**
     * Wraps {@code payload} in a masked client-to-server frame (FIN set) of the given
     * opcode. Client frames must be masked per RFC 6455.
     */
    public static byte[] maskedFrame(int opcode, byte[] payload) {
        byte[] maskKey = {0x12, 0x34, 0x56, 0x78};
        int payloadLen = payload.length;
        int headerLen = (payloadLen <= 125) ? 6 : (payloadLen <= 65_535) ? 8 : 14;
        byte[] frame = new byte[headerLen + payloadLen];
        int offset = 0;
        frame[offset++] = (byte) (0x80 | (opcode & 0x0F));
        if (payloadLen <= 125) {
            frame[offset++] = (byte) (0x80 | payloadLen);
        } else if (payloadLen <= 65_535) {
            frame[offset++] = (byte) (0x80 | 126);
            frame[offset++] = (byte) ((payloadLen >> 8) & 0xFF);
            frame[offset++] = (byte) (payloadLen & 0xFF);
        } else {
            frame[offset++] = (byte) (0x80 | 127);
            for (int b = 7; b >= 0; b--) {
                frame[offset++] = (byte) (((long) payloadLen >> (b * 8)) & 0xFF);
            }
        }
        System.arraycopy(maskKey, 0, frame, offset, 4);
        offset += 4;
        for (int b = 0; b < payloadLen; b++) {
            frame[offset + b] = (byte) (payload[b] ^ maskKey[b % 4]);
        }
        return frame;
    }

    /**
     * Performs the WebSocket upgrade against the egress read endpoint and reads
     * exactly up to the {@code \r\n\r\n} header boundary, leaving any pushed QWP
     * frames (SERVER_INFO first) unconsumed in the stream.
     */
    public static void performReadHandshake(Socket socket) throws Exception {
        performReadHandshake(socket, "");
    }

    /**
     * Upgrades the read endpoint with an optional query string, so a test can
     * drive the browser-only URL carriers a browser WebSocket must use in
     * place of the {@code X-QWP-*} headers it cannot set.
     *
     * @param query leading {@code ?} included, or empty for none
     */
    public static void performReadHandshake(Socket socket, String query) throws Exception {
        performHandshake(socket, "/read/v1", query);
    }

    /**
     * Upgrades the write endpoint with an optional query string. The ingress
     * counterpart of {@link #performReadHandshake(Socket, String)}, for the
     * browser-only URL carriers a browser WebSocket must use in place of the
     * {@code X-QWP-*} headers it cannot set.
     *
     * @param query leading {@code ?} included, or empty for none
     */
    public static void performWriteHandshake(Socket socket, String query) throws Exception {
        performHandshake(socket, "/write/v4", query);
    }

    /**
     * Reads an HTTP response up to and including the {@code \r\n\r\n} header
     * boundary and returns it as US-ASCII, leaving the body -- or, on a
     * successful upgrade, the pushed WebSocket frames -- unconsumed in the
     * stream.
     * <p>
     * A stream that ends early returns what arrived rather than failing here:
     * the caller's assertion on the status line reports the truncated response,
     * which localises a rejected or half-written upgrade better than an
     * end-of-stream failure inside this helper would. The 16 KiB ceiling bounds
     * a server that never terminates the header block.
     */
    public static String readHttpHeaders(InputStream in) throws Exception {
        ByteArrayOutputStream headers = new ByteArrayOutputStream();
        int matched = 0;
        while (headers.size() < 16_384 && matched < 4) {
            int value = in.read();
            if (value < 0) {
                break;
            }
            headers.write(value);
            if (value == (matched == 0 || matched == 2 ? '\r' : '\n')) {
                matched++;
            } else {
                matched = value == '\r' ? 1 : 0;
            }
        }
        return headers.toString(StandardCharsets.US_ASCII);
    }

    /**
     * Reads one unmasked server-to-client WebSocket frame and returns its payload.
     * Blocks until the frame is fully received (bounded by the socket's SO_TIMEOUT).
     */
    public static byte[] readServerFrame(InputStream in) throws Exception {
        int b0 = readByte(in);
        Assert.assertNotEquals("unexpected fragmented server frame", 0, b0 & 0x80);
        Assert.assertEquals("server must reply with a BINARY frame, not opcode 0x" + Integer.toHexString(b0 & 0x0F),
                WebSocketOpcode.BINARY, b0 & 0x0F);
        int b1 = readByte(in);
        Assert.assertEquals("server frames must not be masked", 0, b1 & 0x80);
        long payloadLen = b1 & 0x7F;
        if (payloadLen == 126) {
            payloadLen = ((long) readByte(in) << 8) | readByte(in);
        } else if (payloadLen == 127) {
            payloadLen = 0;
            for (int i = 0; i < 8; i++) {
                payloadLen = (payloadLen << 8) | readByte(in);
            }
        }
        Assert.assertTrue("unreasonable server frame size: " + payloadLen, payloadLen >= 0 && payloadLen < (1 << 26));
        byte[] payload = new byte[(int) payloadLen];
        int read = 0;
        while (read < payload.length) {
            int n = in.read(payload, read, payload.length - read);
            Assert.assertNotEquals("unexpected end of stream inside a server frame", -1, n);
            read += n;
        }
        return payload;
    }

    private static void performHandshake(Socket socket, String path, String query) throws Exception {
        OutputStream out = socket.getOutputStream();
        InputStream in = socket.getInputStream();

        byte[] keyBytes = new byte[16];
        for (int i = 0; i < 16; i++) {
            keyBytes[i] = (byte) (i + 1);
        }
        String wsKey = Base64.getEncoder().encodeToString(keyBytes);

        String request = "GET " + path + query + " HTTP/1.1\r\n" +
                "Host: localhost\r\n" +
                "Upgrade: websocket\r\n" +
                "Connection: Upgrade\r\n" +
                "Sec-WebSocket-Key: " + wsKey + "\r\n" +
                "Sec-WebSocket-Version: 13\r\n" +
                "\r\n";
        out.write(request.getBytes(StandardCharsets.UTF_8));
        out.flush();

        String response = readHttpHeaders(in);
        Assert.assertTrue(
                "Expected 101 Switching Protocols, got: <<<" + response + ">>>",
                response.startsWith("HTTP/1.1 101")
        );
    }

    private static int readByte(InputStream in) throws Exception {
        int b = in.read();
        Assert.assertNotEquals("unexpected end of stream while reading a server frame", -1, b);
        return b;
    }
}
