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
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaProtocol;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaResponse;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicReference;

public class QwpSchemaDiscoveryE2ETest extends AbstractQwpWebSocketTest {

    @Test
    public void testPendingDataAckAfterCompletedSchemaResponse() throws Exception {
        assertPendingDataAckAfterSchema(24);
    }

    @Test
    public void testPendingDataAckAfterResumedSchemaResponse() throws Exception {
        assertPendingDataAckAfterSchema(1);
    }

    private void assertPendingDataAckAfterSchema(int forceSendChunk) throws Exception {
        runInContext(port -> {
            execute("create table schema_ack (val long, ts timestamp) timestamp(ts) partition by day wal");
            try (Socket socket = new Socket("127.0.0.1", port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = new QwpTableBuffer("schema_ack")) {
                socket.setSoTimeout(5_000);
                performSchemaHandshake(socket);
                table.getOrCreateColumn("val", QwpConstants.TYPE_LONG, true).addLong(7);
                table.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(1_000_000_000_000L);
                table.nextRow();
                int dataLength = encoder.encode(table);
                byte[] data = copyBytes(encoder.getBuffer().getBufferPtr(), dataLength);
                byte[] first = maskedFrame(data);
                byte[] second = maskedFrame(QwpSchemaProtocol.encodeDescribe(51, "missing_schema_ack"));
                byte[] pipeline = new byte[first.length + second.length];
                System.arraycopy(first, 0, pipeline, 0, first.length);
                System.arraycopy(second, 0, pipeline, first.length, second.length);
                OutputStream out = socket.getOutputStream();
                out.write(pipeline);
                out.flush();
                QwpSchemaResponse schema = decodeSchema(readServerFrame(socket.getInputStream()));
                Assert.assertEquals(51, schema.getRequestId());
                Assert.assertEquals(QwpSchemaProtocol.RESULT_MISSING, schema.getResult());
                Assert.assertEquals(0, decodeAckSequence(readServerFrame(socket.getInputStream())));

                out.write(maskedFrame(QwpSchemaProtocol.encodeDescribe(52, "missing_schema_ack")));
                out.flush();
                QwpSchemaResponse followUp = decodeSchema(readServerFrame(socket.getInputStream()));
                Assert.assertEquals(52, followUp.getRequestId());
                Assert.assertEquals(QwpSchemaProtocol.RESULT_MISSING, followUp.getResult());
            }
            drainWalQueue();
            assertQuery("select val from schema_ack").noLeakCheck().expectSize().returns("val\n7\n");
        }, 65_536, Integer.MAX_VALUE, forceSendChunk);
    }

    @Test
    public void testDescribeDoesNotConsumeSequenceOrCommitDeferredData() throws Exception {
        runInContext(port -> {
            execute("create table schema_deferred (val long, ts timestamp) timestamp(ts) partition by day wal");
            try (WebSocketClient client = WebSocketClientFactory.newPlainTextInstance();
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = new QwpTableBuffer("schema_deferred")) {
                client.requestQwpSchema();
                client.connect("127.0.0.1", port);
                client.upgrade("/write/v4", null);

                table.getOrCreateColumn("val", QwpConstants.TYPE_LONG, true).addLong(42);
                table.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(1_000_000_000_000L);
                table.nextRow();
                encoder.setDeferCommit(true);
                int deferredLength = encoder.encode(table);
                client.sendBinary(encoder.getBuffer().getBufferPtr(), deferredLength);

                QwpSchemaResponse schema = describe(client, 41, "schema_deferred");
                Assert.assertEquals(QwpSchemaProtocol.RESULT_KNOWN, schema.getResult());
                drainWalQueue();
                assertQuery("select count() from schema_deferred").noLeakCheck().expectSize().noRandomAccess().returns("count\n0\n");

                encoder.getBuffer().reset();
                encoder.setDeferCommit(false);
                encoder.writeHeader(0, 0);
                client.sendBinary(encoder.getBuffer().getBufferPtr(), encoder.getBuffer().getPosition());
                WebSocketResponse ack = new WebSocketResponse();
                AtomicReference<Long> ackSequence = new AtomicReference<>();
                Assert.assertTrue(client.receiveFrame(new WebSocketFrameHandler() {
                    @Override
                    public void onBinaryMessage(long payloadPtr, int payloadLen) {
                        if (ack.readFrom(payloadPtr, payloadLen, true) && ack.isSuccess()) {
                            ackSequence.set(ack.getSequence());
                        }
                    }

                    @Override
                    public void onClose(int code, String reason) {
                        Assert.fail("unexpected close [code=" + code + ", reason=" + reason + ']');
                    }
                }, 5_000));
                Assert.assertEquals(Long.valueOf(1), ackSequence.get());
                Assert.assertTrue(ack.hasSchemaUpdates());
                Assert.assertEquals(1, ack.getSchemaUpdateCount());
                Assert.assertEquals("schema_deferred", ack.getSchemaUpdateTableName(0));
            }
            drainWalQueue();
            assertQuery("select val from schema_deferred").noLeakCheck().expectSize().returns("val\n42\n");
        });
    }

    @Test
    public void testKnownAndMissingSchemasOverNegotiatedWire() throws Exception {
        runInContext(port -> {
            execute("create table 'schema_таблица' (id long, gone int, ts timestamp) timestamp(ts) partition by day wal");
            execute("alter table 'schema_таблица' drop column gone");
            try (WebSocketClient client = WebSocketClientFactory.newPlainTextInstance()) {
                client.requestQwpSchema();
                client.connect("127.0.0.1", port);
                client.upgrade("/write/v4", null);
                Assert.assertTrue(client.isQwpSchemaEnabled());

                QwpSchemaResponse known = describe(client, 1, "schema_таблица");
                Assert.assertEquals(QwpSchemaProtocol.RESULT_KNOWN, known.getResult());
                Assert.assertTrue(known.hasSchema());
                Assert.assertEquals(2, known.getColumnCount());
                Assert.assertEquals("id", known.getColumnName(0));
                Assert.assertEquals(io.questdb.cairo.ColumnType.LONG, known.getColumnType(0));
                Assert.assertEquals("ts", known.getColumnName(1));
                Assert.assertEquals(io.questdb.cairo.ColumnType.TIMESTAMP, known.getColumnType(1));
                Assert.assertEquals(1, known.getDesignatedIndex());

                execute("alter table 'schema_таблица' add column amount decimal(10,2)");
                QwpSchemaResponse changed = describe(client, 2, "schema_таблица");
                Assert.assertEquals(3, changed.getColumnCount());
                Assert.assertEquals(known.getTableId(), changed.getTableId());
                Assert.assertTrue(changed.getMetadataVersion() > known.getMetadataVersion());
                Assert.assertEquals("amount", changed.getColumnName(2));
                Assert.assertEquals(io.questdb.cairo.ColumnType.getDecimalType(10, 2), changed.getColumnType(2));

                QwpSchemaResponse missing = describe(client, 3, "does_not_exist");
                Assert.assertEquals(QwpSchemaProtocol.RESULT_MISSING, missing.getResult());
                Assert.assertFalse(missing.hasSchema());
                Assert.assertNull(engine.getTableTokenIfExists("does_not_exist"));
            }
        });
    }

    @Test
    public void testUnnegotiatedControlClosesWithoutDataAck() throws Exception {
        runInContext(port -> {
            try (WebSocketClient client = WebSocketClientFactory.newPlainTextInstance()) {
                client.connect("127.0.0.1", port);
                client.upgrade("/write/v4", null);
                Assert.assertFalse(client.isQwpSchemaEnabled());
                byte[] request = QwpSchemaProtocol.encodeDescribe(1, "missing");
                send(client, request);
                int[] closeCode = {-1};
                Assert.assertTrue(client.receiveFrame(new WebSocketFrameHandler() {
                    @Override
                    public void onBinaryMessage(long payloadPtr, int payloadLen) {
                        Assert.fail("unnegotiated control must not receive a data ACK");
                    }

                    @Override
                    public void onClose(int code, String reason) {
                        closeCode[0] = code;
                    }
                }, 5_000));
                Assert.assertEquals(1002, closeCode[0]);
            }
        });
    }

    @Test
    public void testTruncatedNegotiatedControlIsProtocolError() throws Exception {
        runInContext(port -> {
            try (WebSocketClient client = WebSocketClientFactory.newPlainTextInstance()) {
                client.requestQwpSchema();
                client.connect("127.0.0.1", port);
                client.upgrade("/write/v4", null);
                send(client, Arrays.copyOf(QwpSchemaProtocol.encodeDescribe(1, "missing"), 6));
                int[] closeCode = {-1};
                Assert.assertTrue(client.receiveFrame(new WebSocketFrameHandler() {
                    @Override
                    public void onBinaryMessage(long payloadPtr, int payloadLen) {
                        Assert.fail("malformed control must not receive a data ACK");
                    }

                    @Override
                    public void onClose(int code, String reason) {
                        closeCode[0] = code;
                    }
                }, 5_000));
                Assert.assertEquals(1002, closeCode[0]);
            }
        });
    }

    private static QwpSchemaResponse describe(WebSocketClient client, long requestId, String tableName) {
        byte[] request = QwpSchemaProtocol.encodeDescribe(requestId, tableName);
        long address = Unsafe.malloc(request.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < request.length; i++) {
                Unsafe.getUnsafe().putByte(address + i, request[i]);
            }
            client.sendBinary(address, request.length);
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
            Assert.assertEquals(requestId, response.get().getRequestId());
            return response.get();
        } finally {
            Unsafe.free(address, request.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void send(WebSocketClient client, byte[] request) {
        long address = Unsafe.malloc(request.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < request.length; i++) {
                Unsafe.getUnsafe().putByte(address + i, request[i]);
            }
            client.sendBinary(address, request.length);
        } finally {
            Unsafe.free(address, request.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static byte[] copyBytes(long address, int length) {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) bytes[i] = Unsafe.getUnsafe().getByte(address + i);
        return bytes;
    }

    private static QwpSchemaResponse decodeSchema(byte[] payload) {
        long address = Unsafe.malloc(payload.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < payload.length; i++) Unsafe.getUnsafe().putByte(address + i, payload[i]);
            return QwpSchemaProtocol.decodeResponse(address, payload.length);
        } finally {
            Unsafe.free(address, payload.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static long decodeAckSequence(byte[] payload) {
        long address = Unsafe.malloc(payload.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < payload.length; i++) Unsafe.getUnsafe().putByte(address + i, payload[i]);
            WebSocketResponse response = new WebSocketResponse();
            Assert.assertTrue(response.readFrom(address, payload.length, true));
            Assert.assertTrue(response.isSuccess());
            Assert.assertTrue(response.hasSchemaUpdates());
            Assert.assertEquals("schema_ack", response.getSchemaUpdateTableName(0));
            return response.getSequence();
        } finally {
            Unsafe.free(address, payload.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static byte[] maskedFrame(byte[] payload) {
        byte[] mask = {1, 2, 3, 4};
        int header = payload.length <= 125 ? 6 : 8;
        byte[] frame = new byte[header + payload.length];
        int p = 0;
        frame[p++] = (byte) 0x82;
        if (payload.length <= 125) {
            frame[p++] = (byte) (0x80 | payload.length);
        } else {
            frame[p++] = (byte) 0xfe;
            frame[p++] = (byte) (payload.length >>> 8);
            frame[p++] = (byte) payload.length;
        }
        System.arraycopy(mask, 0, frame, p, 4);
        p += 4;
        for (int i = 0; i < payload.length; i++) frame[p + i] = (byte) (payload[i] ^ mask[i & 3]);
        return frame;
    }

    private static void performSchemaHandshake(Socket socket) throws Exception {
        String key = Base64.getEncoder().encodeToString(new byte[16]);
        String request = "GET /write/v4 HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                + "Sec-WebSocket-Key: " + key + "\r\nSec-WebSocket-Version: 13\r\nX-QWP-Max-Version: 1\r\n"
                + "X-QWP-Request-Schema: true\r\n\r\n";
        socket.getOutputStream().write(request.getBytes(StandardCharsets.US_ASCII));
        socket.getOutputStream().flush();
        StringBuilder response = new StringBuilder();
        InputStream in = socket.getInputStream();
        while (!response.toString().endsWith("\r\n\r\n")) {
            int b = in.read();
            Assert.assertNotEquals(-1, b);
            response.append((char) b);
        }
        Assert.assertTrue(response.toString().startsWith("HTTP/1.1 101"));
        Assert.assertTrue(response.toString().contains("X-QWP-Schema: enabled"));
    }

    private static byte[] readServerFrame(InputStream in) throws Exception {
        Assert.assertEquals(0x82, in.read());
        int b1 = in.read();
        Assert.assertEquals(0, b1 & 0x80);
        int length = b1 & 0x7f;
        if (length == 126) length = (in.read() << 8) | in.read();
        byte[] payload = new byte[length];
        int offset = 0;
        while (offset < length) {
            int n = in.read(payload, offset, length - offset);
            Assert.assertTrue(n > 0);
            offset += n;
        }
        return payload;
    }

}
