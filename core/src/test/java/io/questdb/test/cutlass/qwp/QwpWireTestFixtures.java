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
import io.questdb.cutlass.qwp.protocol.QwpVarint;
import io.questdb.cutlass.qwp.websocket.WebSocketOpcode;
import org.junit.Assert;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

/**
 * Raw-wire building blocks for QWP egress tests that must drive the WebSocket
 * byte stream directly (e.g. disconnect-while-parked scenarios that a managed
 * client's close-during-execute contract forbids). Shared by
 * {@link QwpEgressBootstrapTest} and {@link QwpEgressQueryFlagsResetWireTest}.
 */
final class QwpWireTestFixtures {

    private QwpWireTestFixtures() {
    }

    /**
     * msg_kind(1) + request_id(8 LE) + additional_bytes(varint).
     */
    static byte[] buildCreditFrame(long requestId, long additionalBytes) {
        byte[] p = new byte[1 + 8 + 10];
        int i = 0;
        p[i++] = QwpEgressMsgKind.CREDIT;
        for (int s = 0; s < 8; s++) {
            p[i++] = (byte) (requestId >>> (8 * s));
        }
        i = QwpVarint.encode(p, i, additionalBytes);
        return Arrays.copyOf(p, i);
    }

    static byte[] buildQueryRequest(long requestId, String sql) {
        return buildQueryRequest(requestId, sql, 0);
    }

    /**
     * msg_kind(1) + request_id(8 LE) + sql_len(varint) + sql + initial_credit(varint,
     * 0 = unbounded) + bind_count(0). SQL must be short enough for a single-byte
     * length varint.
     */
    static byte[] buildQueryRequest(long requestId, String sql, long initialCredit) {
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
     * Wraps {@code payload} in a masked client-to-server frame (FIN set) of the given
     * opcode. Client frames must be masked per RFC 6455.
     */
    static byte[] maskedFrame(int opcode, byte[] payload) {
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
    static void performReadHandshake(Socket socket) throws Exception {
        OutputStream out = socket.getOutputStream();
        InputStream in = socket.getInputStream();

        byte[] keyBytes = new byte[16];
        for (int i = 0; i < 16; i++) {
            keyBytes[i] = (byte) (i + 1);
        }
        String wsKey = Base64.getEncoder().encodeToString(keyBytes);

        String request = "GET /read/v1 HTTP/1.1\r\n" +
                "Host: localhost\r\n" +
                "Upgrade: websocket\r\n" +
                "Connection: Upgrade\r\n" +
                "Sec-WebSocket-Key: " + wsKey + "\r\n" +
                "Sec-WebSocket-Version: 13\r\n" +
                "\r\n";
        out.write(request.getBytes(StandardCharsets.UTF_8));
        out.flush();

        StringBuilder response = new StringBuilder();
        while (true) {
            int b = in.read();
            Assert.assertNotEquals("Unexpected end of stream during handshake", -1, b);
            response.append((char) b);
            int len = response.length();
            if (len >= 4
                    && response.charAt(len - 4) == '\r' && response.charAt(len - 3) == '\n'
                    && response.charAt(len - 2) == '\r' && response.charAt(len - 1) == '\n') {
                break;
            }
        }
        Assert.assertTrue(
                "Expected 101 Switching Protocols, got: " + response.toString().split("\r\n")[0],
                response.toString().startsWith("HTTP/1.1 101")
        );
    }

    /**
     * Reads one unmasked server-to-client WebSocket frame and returns its payload.
     * Blocks until the frame is fully received (bounded by the socket's SO_TIMEOUT).
     */
    static byte[] readServerFrame(InputStream in) throws Exception {
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

    private static int readByte(InputStream in) throws Exception {
        int b = in.read();
        Assert.assertNotEquals("unexpected end of stream while reading a server frame", -1, b);
        return b;
    }
}
