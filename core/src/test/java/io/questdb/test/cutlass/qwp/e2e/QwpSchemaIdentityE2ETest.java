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

import io.questdb.client.Sender;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketClientFactory;
import io.questdb.client.cutlass.http.client.WebSocketFrameHandler;
import io.questdb.client.cutlass.qwp.client.GlobalSymbolDictionary;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaProtocol;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaResponse;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.cairo.TableToken;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

public class QwpSchemaIdentityE2ETest extends AbstractQwpWebSocketTest {

    @Test
    public void testMixedLegacyAndSchemaFramesReplayFromStoreAndForward() throws Exception {
        runInContext(port -> {
            execute("create table schema_sf_replay (n long, ts timestamp) timestamp(ts) partition by day wal");
            java.io.File sfRoot = temp.newFolder("qwp-schema-mixed-sf");
            String slot = new java.io.File(sfRoot, "default").getAbsolutePath();
            try (CursorSendEngine cursorEngine = new CursorSendEngine(slot, 1 << 20);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer legacy = longTable("schema_sf_replay", 1);
                 QwpTableBuffer extended = longTable("schema_sf_replay", 2)) {
                int legacyLength = encoder.encode(legacy);
                Assert.assertEquals(0, Unsafe.getByte(encoder.getBuffer().getBufferPtr() + QwpConstants.HEADER_OFFSET_FLAGS) & QwpConstants.FLAG_SCHEMA);
                Assert.assertEquals(0, cursorEngine.appendBlocking(encoder.getBuffer().getBufferPtr(), legacyLength));
                int schemaLength = encoder.encodeSchema(extended, -1, -1);
                Assert.assertEquals(QwpConstants.FLAG_SCHEMA, Unsafe.getByte(encoder.getBuffer().getBufferPtr() + QwpConstants.HEADER_OFFSET_FLAGS) & QwpConstants.FLAG_SCHEMA);
                Assert.assertEquals(1, cursorEngine.appendBlocking(encoder.getBuffer().getBufferPtr(), schemaLength));
            }

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port
                    + ";sf_dir=" + sfRoot.getAbsolutePath()
                    + ";sender_id=default;sf_max_segment_bytes=1m;")) {
                Assert.assertTrue(sender.drain(10_000));
            }
            drainWalQueue();
            assertQuery("select n from schema_sf_replay order by n").noLeakCheck().returnsOnce("n\n1\n2\n");
        });
    }

    @Test
    public void testKnownStaleAndDropRecreateIdentitiesAreDescriptive() throws Exception {
        runInContext(port -> {
            execute("create table schema_identity (n long, ts timestamp) timestamp(ts) partition by day wal");
            try (WebSocketClient client = connect(port); QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                QwpSchemaResponse schema = describe(client, 1, "schema_identity");
                sendLong(client, encoder, "schema_identity", 1, schema.getTableId(), schema.getMetadataVersion(), 0, false);
                execute("alter table schema_identity add column later int");
                QwpSchemaResponse changed = describe(client, 2, "schema_identity");
                Assert.assertTrue(changed.getMetadataVersion() > schema.getMetadataVersion());
                WebSocketResponse staleFeedback = sendLong(client, encoder, "schema_identity", 2, schema.getTableId(), schema.getMetadataVersion(), 1, true);
                Assert.assertEquals(changed.getMetadataVersion(), staleFeedback.getSchemaUpdate(0).getMetadataVersion());
                Assert.assertEquals(changed.getTableId(), staleFeedback.getSchemaUpdate(0).getTableId());
                drainWalQueue();
                assertQuery("select n from schema_identity order by n").noLeakCheck().returnsOnce("n\n1\n2\n");

                execute("drop table schema_identity");
                execute("create table schema_identity (n long, ts timestamp) timestamp(ts) partition by day wal");
                TableToken recreated = engine.getTableTokenIfExists("schema_identity");
                Assert.assertNotNull(recreated);
                Assert.assertNotEquals(schema.getTableId(), recreated.getTableId());
                WebSocketResponse recreatedFeedback = sendLong(client, encoder, "schema_identity", 3, schema.getTableId(), schema.getMetadataVersion(), 2, true);
                Assert.assertEquals(recreated.getTableId(), recreatedFeedback.getSchemaUpdate(0).getTableId());
            }
            drainWalQueue();
            assertQuery("select n from schema_identity").noLeakCheck().returnsOnce("n\n3\n");
        });
    }

    @Test
    public void testUnknownIdentityAutoCreatesAndMultiTableMessageMixesIdentities() throws Exception {
        runInContext(port -> {
            execute("create table schema_known_multi (n long, ts timestamp) timestamp(ts) partition by day wal");
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer known = longTable("schema_known_multi", 10);
                 QwpTableBuffer unknown = longTable("schema_unknown_multi", 20)) {
                QwpSchemaResponse schema = describe(client, 2, "schema_known_multi");
                encoder.beginSchemaMessage(2, new GlobalSymbolDictionary(), -1, -1);
                encoder.addSchemaTable(known, schema.getTableId(), schema.getMetadataVersion());
                encoder.addSchemaTable(unknown, -1, -1);
                int length = encoder.finishMessage();
                client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                assertOk(client, 0, true);
            }
            drainWalQueue();
            assertQuery("select n from schema_known_multi").noLeakCheck().returnsOnce("n\n10\n");
            assertQuery("select n from schema_unknown_multi").noLeakCheck().returnsOnce("n\n20\n");
        });
    }

    @Test
    public void testNegotiatedConnectionStillAcceptsLegacyData() throws Exception {
        runInContext(port -> {
            execute("create table schema_legacy_on_new (n long, ts timestamp) timestamp(ts) partition by day wal");
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = longTable("schema_legacy_on_new", 7)) {
                int length = encoder.encode(table);
                Assert.assertEquals(0, Unsafe.getByte(encoder.getBuffer().getBufferPtr() + QwpConstants.HEADER_OFFSET_FLAGS) & QwpConstants.FLAG_SCHEMA);
                client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                assertOk(client, 0, true);
            }
            drainWalQueue();
            assertQuery("select n from schema_legacy_on_new").noLeakCheck().returnsOnce("n\n7\n");
        });
    }

    @Test
    public void testUnnegotiatedSchemaFrameClosesWithoutAckOrIngestion() throws Exception {
        runInContext(port -> {
            execute("create table schema_unnegotiated (n long, ts timestamp) timestamp(ts) partition by day wal");
            try (WebSocketClient client = WebSocketClientFactory.newPlainTextInstance();
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = longTable("schema_unnegotiated", 9)) {
                client.connect("127.0.0.1", port);
                client.upgrade("/write/v4", null);
                int length = encoder.encodeSchema(table, -1, -1);
                client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                int[] closeCode = {-1};
                Assert.assertTrue(client.receiveFrame(new WebSocketFrameHandler() {
                    @Override
                    public void onBinaryMessage(long payloadPtr, int payloadLen) {
                        Assert.fail("unnegotiated schema data must not receive an ACK");
                    }

                    @Override
                    public void onClose(int code, String reason) {
                        closeCode[0] = code;
                    }
                }, 5_000));
                Assert.assertEquals(1002, closeCode[0]);
            }
            drainWalQueue();
            assertQuery("select count() from schema_unnegotiated").noLeakCheck().returnsOnce("count\n0\n");
        });
    }

    private static WebSocketResponse assertOk(WebSocketClient client, long sequence, boolean expectFeedback) {
        WebSocketResponse response = new WebSocketResponse();
        Assert.assertTrue(client.receiveFrame(new WebSocketFrameHandler() {
            @Override
            public void onBinaryMessage(long payloadPtr, int payloadLen) {
                Assert.assertTrue(response.readFrom(payloadPtr, payloadLen, true));
            }

            @Override
            public void onClose(int code, String reason) {
                Assert.fail("unexpected close [code=" + code + ", reason=" + reason + ']');
            }
        }, 5_000));
        Assert.assertTrue(response.getErrorMessage(), response.isSuccess());
        Assert.assertEquals(sequence, response.getSequence());
        Assert.assertEquals(expectFeedback, response.hasSchemaUpdates());
        Assert.assertFalse(response.isSchemaInvalidation());
        if (expectFeedback) {
            Assert.assertEquals(1, response.getSchemaUpdateCount());
            Assert.assertEquals(QwpSchemaProtocol.RESULT_KNOWN, response.getSchemaUpdate(0).getResult());
        }
        return response;
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
            Assert.assertNotNull(response.get());
            Assert.assertEquals(requestId, response.get().getRequestId());
            Assert.assertEquals(QwpSchemaProtocol.RESULT_KNOWN, response.get().getResult());
            return response.get();
        } finally {
            Unsafe.free(address, request.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static QwpTableBuffer longTable(String tableName, long value) {
        QwpTableBuffer table = new QwpTableBuffer(tableName);
        table.getOrCreateColumn("n", QwpConstants.TYPE_LONG, true).addLong(value);
        table.nextRow();
        return table;
    }

    private static WebSocketResponse sendLong(WebSocketClient client, QwpWebSocketEncoder encoder, String tableName, long value, int tableId, long metadataVersion, long sequence, boolean expectFeedback) {
        try (QwpTableBuffer table = longTable(tableName, value)) {
            int length = encoder.encodeSchema(table, tableId, metadataVersion);
            client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
            return assertOk(client, sequence, expectFeedback);
        }
    }
}
