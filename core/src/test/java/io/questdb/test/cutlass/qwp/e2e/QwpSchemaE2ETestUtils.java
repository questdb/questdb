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

import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketClientFactory;
import io.questdb.client.cutlass.http.client.WebSocketFrameHandler;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaProtocol;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaResponse;
import io.questdb.cutlass.qwp.protocol.QwpMessageCursor;
import io.questdb.cutlass.qwp.protocol.QwpTableBlockCursor;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8Sequence;
import org.junit.Assert;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Wire-level helpers shared by the schema-aware QWP E2E tests. They drive the
 * pinned client's {@link WebSocketClient} directly, so a test controls exactly
 * which frames reach the server.
 */
final class QwpSchemaE2ETestUtils {

    private QwpSchemaE2ETestUtils() {
    }

    static void assertUtf8(String expected, Utf8Sequence actual) {
        Assert.assertNotNull(actual);
        byte[] bytes = expected.getBytes(StandardCharsets.UTF_8);
        Assert.assertEquals(bytes.length, actual.size());
        for (int i = 0; i < bytes.length; i++) {
            Assert.assertEquals("byte " + i, bytes[i], actual.byteAt(i));
        }
    }

    /**
     * Asserts that {@code actual} holds exactly {@code expected}, or is null when
     * {@code expected} is null.
     */
    static void assertUtf8(String caseId, byte[] expected, Utf8Sequence actual) {
        if (expected == null) {
            Assert.assertNull(caseId, actual);
            return;
        }
        Assert.assertNotNull(caseId, actual);
        Assert.assertEquals(caseId, expected.length, actual.size());
        for (int i = 0; i < expected.length; i++) {
            Assert.assertEquals(caseId + " byte " + i, expected[i], actual.byteAt(i));
        }
    }

    /**
     * Upgrades {@code client} on {@code port} without requesting schema support.
     */
    static void connectLegacyClient(WebSocketClient client, int port) {
        client.connect("127.0.0.1", port);
        client.upgrade("/write/v4", null);
        Assert.assertFalse(client.isQwpSchemaEnabled());
    }

    /**
     * Returns a client upgraded on {@code port} without requesting schema support.
     */
    static WebSocketClient connectLegacyClient(int port) {
        WebSocketClient client = WebSocketClientFactory.newPlainTextInstance();
        boolean success = false;
        try {
            connectLegacyClient(client, port);
            success = true;
            return client;
        } finally {
            if (!success) {
                client.close();
            }
        }
    }

    /**
     * Returns a client upgraded on {@code port} that negotiated schema support.
     */
    static WebSocketClient connectSchemaClient(int port) {
        WebSocketClient client = WebSocketClientFactory.newPlainTextInstance();
        boolean success = false;
        try {
            client.requestQwpSchema();
            client.connect("127.0.0.1", port);
            client.upgrade("/write/v4", null);
            Assert.assertTrue(client.isQwpSchemaEnabled());
            success = true;
            return client;
        } finally {
            if (!success) {
                client.close();
            }
        }
    }

    /**
     * Sends a DESCRIBE for {@code tableName} and returns the reply, which must
     * answer {@code requestId}.
     */
    static QwpSchemaResponse describe(WebSocketClient client, long requestId, String tableName) {
        byte[] request = QwpSchemaProtocol.encodeDescribe(requestId, tableName);
        long address = Unsafe.malloc(request.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < request.length; i++) {
                Unsafe.putByte(address + i, request[i]);
            }
            client.sendBinary(address, request.length);
        } finally {
            Unsafe.free(address, request.length, MemoryTag.NATIVE_DEFAULT);
        }
        AtomicReference<QwpSchemaResponse> response = new AtomicReference<>();
        Assert.assertTrue(client.receiveFrame(new WebSocketFrameHandler() {
            @Override
            public void onBinaryMessage(long payloadPtr, int payloadLen) {
                response.set(QwpSchemaProtocol.decodeResponse(payloadPtr, payloadLen));
            }

            @Override
            public void onClose(int code, String reason) {
                Assert.fail("unexpected close [code=" + code + ", reason=" + reason + ']');
            }
        }, 5_000));
        Assert.assertNotNull(response.get());
        Assert.assertEquals(requestId, response.get().getRequestId());
        return response.get();
    }

    /**
     * As {@link #describe}, and the table must exist.
     */
    static QwpSchemaResponse describeKnown(WebSocketClient client, long requestId, String tableName) {
        QwpSchemaResponse response = describe(client, requestId, tableName);
        Assert.assertEquals(QwpSchemaProtocol.RESULT_KNOWN, response.getResult());
        return response;
    }

    /**
     * Decodes the single table block of an encoded QWP message.
     */
    static QwpTableBlockCursor parseSingleTable(QwpWebSocketEncoder encoder, int length) throws Exception {
        QwpMessageCursor message = new QwpMessageCursor();
        message.of(encoder.getBuffer().getBufferPtr(), length, new ObjList<>());
        Assert.assertTrue(message.hasNextTable());
        QwpTableBlockCursor table = message.nextTable();
        Assert.assertFalse(message.hasNextTable());
        return table;
    }

    /**
     * Receives one ACK or error frame. It decodes schema feedback when the client
     * negotiated schema support, as the sender does.
     */
    static WebSocketResponse receiveResponse(WebSocketClient client) {
        AtomicReference<WebSocketResponse> response = new AtomicReference<>();
        Assert.assertTrue(client.receiveFrame(new WebSocketFrameHandler() {
            @Override
            public void onBinaryMessage(long payloadPtr, int payloadLen) {
                WebSocketResponse parsed = new WebSocketResponse();
                Assert.assertTrue(parsed.readFrom(payloadPtr, payloadLen, client.isQwpSchemaEnabled()));
                response.set(parsed);
            }

            @Override
            public void onClose(int code, String reason) {
                Assert.fail("unexpected close [code=" + code + ", reason=" + reason + ']');
            }
        }, 5_000));
        Assert.assertNotNull(response.get());
        return response.get();
    }
}
