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
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaProtocol;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaResponse;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaBinding;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpMessageCursor;
import io.questdb.cutlass.qwp.protocol.QwpTableBlockCursor;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

public class QwpSchemaFeedbackEvolutionE2ETest extends AbstractQwpWebSocketTest {

    @Test
    public void testRealLoopCoordinatesDescribeFeedbackAndRebind() throws Exception {
        execute("create table feedback_coordinator (n int, ts timestamp) timestamp(ts) partition by day wal");
        runInContext(port -> {
            java.io.File sfRoot = temp.newFolder("qwp-schema-coordinator");
            String slot = new java.io.File(sfRoot, "default").getAbsolutePath();
            try (WebSocketClient client = connectCoordinator(port);
                 CursorSendEngine cursorEngine = new CursorSendEngine(slot, 1 << 20);
                 CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                         client,
                         cursorEngine,
                         0,
                         CursorWebSocketSendLoop.DEFAULT_PARK_NANOS,
                         null,
                         1,
                         4,
                         false
                 );
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = new QwpTableBuffer("feedback_coordinator")) {
                loop.start();
                Assert.assertEquals(-1, cursorEngine.publishedFsn());
                Assert.assertEquals(-1, cursorEngine.ackedFsn());

                QwpSchemaResponse initial = loop.resolveSchema("feedback_coordinator", 5_000);
                Assert.assertTrue(initial.getRequestId() > 0);
                Assert.assertEquals(io.questdb.cairo.ColumnType.INT, initial.getColumnType(0));
                Assert.assertEquals(-1, cursorEngine.publishedFsn());
                Assert.assertEquals(-1, cursorEngine.ackedFsn());

                QwpSchemaBinding oldBinding = new QwpSchemaBinding(table, initial);
                oldBinding.longColumn("n", 42);
                table.nextRow();
                int firstLength = encoder.encodeSchema(table);
                assertColumnWire(
                        copy(encoder, firstLength),
                        QwpConstants.TYPE_INT,
                        initial.getTableId(),
                        initial.getMetadataVersion()
                );
                Assert.assertEquals(0, cursorEngine.appendBlocking(encoder.getBuffer().getBufferPtr(), firstLength));
                TestUtils.assertEventually(() -> Assert.assertEquals(0, cursorEngine.ackedFsn()), 10);

                execute("alter table feedback_coordinator alter column n type long");
                table.reset();
                oldBinding.longColumn("n", 43);
                table.nextRow();
                encoder.getBuffer().reset();
                int staleLength = encoder.encodeSchema(table);
                assertColumnWire(
                        copy(encoder, staleLength),
                        QwpConstants.TYPE_INT,
                        initial.getTableId(),
                        initial.getMetadataVersion()
                );
                Assert.assertEquals(1, cursorEngine.appendBlocking(encoder.getBuffer().getBufferPtr(), staleLength));
                TestUtils.assertEventually(() -> Assert.assertEquals(1, cursorEngine.ackedFsn()), 10);

                QwpSchemaResponse updated = loop.resolveSchema("feedback_coordinator", 5_000);
                Assert.assertEquals(0, updated.getRequestId());
                Assert.assertEquals(io.questdb.cairo.ColumnType.LONG, updated.getColumnType(0));
                Assert.assertEquals(initial.getTableId(), updated.getTableId());
                Assert.assertTrue(updated.getMetadataVersion() > initial.getMetadataVersion());

                table.clear();
                QwpSchemaBinding currentBinding = new QwpSchemaBinding(table, updated);
                currentBinding.longColumn("n", (long) Integer.MAX_VALUE + 1);
                table.nextRow();
                encoder.getBuffer().reset();
                int currentLength = encoder.encodeSchema(table);
                byte[] currentBytes = copy(encoder, currentLength);
                assertColumnWire(
                        currentBytes,
                        QwpConstants.TYPE_LONG,
                        updated.getTableId(),
                        updated.getMetadataVersion()
                );
                Assert.assertEquals(2, cursorEngine.appendBlocking(encoder.getBuffer().getBufferPtr(), currentLength));
                TestUtils.assertEventually(() -> Assert.assertEquals(2, cursorEngine.ackedFsn()), 10);

                long publishedBeforeRefresh = cursorEngine.publishedFsn();
                long ackedBeforeRefresh = cursorEngine.ackedFsn();
                QwpSchemaResponse refreshed = loop.refreshSchema("feedback_coordinator", 5_000);
                Assert.assertTrue(refreshed.getRequestId() > 0);
                Assert.assertEquals(updated.getTableId(), refreshed.getTableId());
                Assert.assertEquals(updated.getMetadataVersion(), refreshed.getMetadataVersion());
                Assert.assertEquals(publishedBeforeRefresh, cursorEngine.publishedFsn());
                Assert.assertEquals(ackedBeforeRefresh, cursorEngine.ackedFsn());
            }
            drainWalQueue();
            assertQuery("select n from feedback_coordinator order by n").noLeakCheck()
                    .expectSize().returns("n\n42\n43\n2147483648\n");
        }, 65_536, Integer.MAX_VALUE, 1);
    }

    @Test
    public void testStaleIntBytesYieldLongSnapshotAndNewTypedLongFrame() throws Exception {
        execute("create table feedback_evolution (n int, ts timestamp) timestamp(ts) partition by day wal");
        runInContext(port -> {
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = new QwpTableBuffer("feedback_evolution")) {
                QwpSchemaBinding oldBinding = new QwpSchemaBinding(
                        table, describe(client, 1, "feedback_evolution"));
                oldBinding.longColumn("n", 42);
                table.nextRow();
                int oldLength = encoder.encodeSchema(table);
                byte[] oldBytes = copy(encoder, oldLength);
                assertColumnWire(
                        oldBytes, QwpConstants.TYPE_INT, oldBinding.getTableId(), oldBinding.getMetadataVersion());

                execute("alter table feedback_evolution alter column n type long");
                sendBytes(client, oldBytes);
                WebSocketResponse staleAck = receive(client);
                Assert.assertTrue(staleAck.isSuccess());
                Assert.assertEquals(1, staleAck.getSchemaUpdateCount());
                Assert.assertEquals("feedback_evolution", staleAck.getSchemaUpdateTableName(0));
                QwpSchemaResponse current = staleAck.getSchemaUpdate(0);
                Assert.assertEquals(0, current.getRequestId());
                Assert.assertEquals(QwpSchemaProtocol.RESULT_KNOWN, current.getResult());
                Assert.assertEquals(io.questdb.cairo.ColumnType.LONG, current.getColumnType(0));

                encoder.getBuffer().reset();
                int reencodedLength = encoder.encodeSchema(table);
                Assert.assertArrayEquals(oldBytes, copy(encoder, reencodedLength));

                table.clear();
                Assert.assertThrows(IllegalStateException.class, () -> oldBinding.longColumn("n", 43));
                QwpSchemaBinding currentBinding = new QwpSchemaBinding(table, current);
                currentBinding.longColumn("n", (long) Integer.MAX_VALUE + 1);
                table.nextRow();
                encoder.getBuffer().reset();
                int currentLength = encoder.encodeSchema(table);
                byte[] currentBytes = copy(encoder, currentLength);
                assertColumnWire(
                        currentBytes, QwpConstants.TYPE_LONG,
                        currentBinding.getTableId(), currentBinding.getMetadataVersion());
                assertColumnWire(
                        oldBytes, QwpConstants.TYPE_INT, oldBinding.getTableId(), oldBinding.getMetadataVersion());
                sendBytes(client, currentBytes);
                WebSocketResponse matchingAck = receive(client);
                Assert.assertTrue(matchingAck.isSuccess());
                Assert.assertFalse(matchingAck.hasSchemaUpdates());
                Assert.assertFalse(matchingAck.isSchemaInvalidation());
            }
            drainWalQueue();
            assertQuery("select n from feedback_evolution order by n").noLeakCheck()
                    .expectSize().returns("n\n42\n2147483648\n");
        });
    }

    private static void assertColumnWire(
            byte[] bytes,
            int expectedType,
            int expectedTableId,
            long expectedMetadataVersion
    ) throws Exception {
        long address = Unsafe.malloc(bytes.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < bytes.length; i++) {
                Unsafe.putByte(address + i, bytes[i]);
            }
            Assert.assertEquals(QwpConstants.FLAG_SCHEMA,
                    Unsafe.getByte(address + QwpConstants.HEADER_OFFSET_FLAGS) & QwpConstants.FLAG_SCHEMA);
            QwpMessageCursor cursor = new QwpMessageCursor();
            cursor.of(address, bytes.length, new ObjList<>());
            Assert.assertTrue(cursor.hasNextTable());
            QwpTableBlockCursor table = cursor.nextTable();
            Assert.assertTrue(table.hasKnownSchemaIdentity());
            Assert.assertEquals(expectedTableId, table.getSchemaTableId());
            Assert.assertEquals(expectedMetadataVersion, table.getSchemaMetadataVersion());
            Assert.assertEquals(expectedType, table.getColumnDef(0).getTypeCode());
            Assert.assertFalse(cursor.hasNextTable());
        } finally {
            Unsafe.free(address, bytes.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static WebSocketClient connect(int port) {
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

    private static WebSocketClient connectCoordinator(int port) {
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

    private static byte[] copy(QwpWebSocketEncoder encoder, int length) {
        byte[] bytes = new byte[length];
        long address = encoder.getBuffer().getBufferPtr();
        for (int i = 0; i < length; i++) {
            bytes[i] = Unsafe.getByte(address + i);
        }
        return bytes;
    }

    private static QwpSchemaResponse describe(WebSocketClient client, long requestId, String tableName) {
        byte[] request = QwpSchemaProtocol.encodeDescribe(requestId, tableName);
        sendBytes(client, request);
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

    private static WebSocketResponse receive(WebSocketClient client) {
        AtomicReference<WebSocketResponse> received = new AtomicReference<>();
        Assert.assertTrue(client.receiveFrame(new WebSocketFrameHandler() {
            @Override
            public void onBinaryMessage(long payloadPtr, int payloadLen) {
                WebSocketResponse response = new WebSocketResponse();
                Assert.assertTrue(response.readFrom(payloadPtr, payloadLen, true));
                received.set(response);
            }

            @Override
            public void onClose(int code, String reason) {
                Assert.fail("unexpected close [code=" + code + ", reason=" + reason + ']');
            }

        }, 5_000));
        Assert.assertNotNull(received.get());
        return received.get();
    }

    private static void sendBytes(WebSocketClient client, byte[] bytes) {
        long address = Unsafe.malloc(bytes.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < bytes.length; i++) {
                Unsafe.putByte(address + i, bytes[i]);
            }
            client.sendBinary(address, bytes.length);
        } finally {
            Unsafe.free(address, bytes.length, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
