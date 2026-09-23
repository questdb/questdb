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
import io.questdb.client.cutlass.qwp.client.GlobalSymbolDictionary;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaProtocol;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaResponse;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

public class QwpSchemaFeedbackE2ETest extends AbstractQwpWebSocketTest {

    @Test
    public void testDeferredTwoTableCommitReturnsBothUpdates() throws Exception {
        execute("create table feedback_deferred_a (n long, ts timestamp) timestamp(ts) partition by day wal");
        execute("create table feedback_deferred_b (n long, ts timestamp) timestamp(ts) partition by day wal");
        runInContext(port -> {
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer a = longTable("feedback_deferred_a", 1);
                 QwpTableBuffer b = longTable("feedback_deferred_b", 2)) {
                encoder.setDeferCommit(true);
                encoder.beginSchemaMessage(2, new GlobalSymbolDictionary(), -1, -1);
                encoder.addSchemaTable(a, -1, -1);
                encoder.addSchemaTable(b, -1, -1);
                int length = encoder.finishMessage();
                client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                sendCommit(client, encoder);
                WebSocketResponse ack = receive(client);
                Assert.assertTrue(ack.isSuccess());
                Assert.assertEquals(1, ack.getSequence());
                Assert.assertEquals(2, ack.getSchemaUpdateCount());
                assertContainsUpdate(ack, "feedback_deferred_a");
                assertContainsUpdate(ack, "feedback_deferred_b");
            }
        });
    }

    @Test
    public void testDeferredFeedbackIsSampledAfterAlterAtResponseTime() throws Exception {
        execute("create table feedback_fresh (n long, ts timestamp) timestamp(ts) partition by day wal");
        runInContext(port -> {
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = longTable("feedback_fresh", 1)) {
                QwpSchemaResponse before = describe(client, 70, "feedback_fresh");
                encoder.setDeferCommit(true);
                sendSchema(client, encoder, table);
                QwpSchemaResponse queued = describe(client, 71, "feedback_fresh");
                Assert.assertEquals(before.getMetadataVersion(), queued.getMetadataVersion());
                execute("alter table feedback_fresh add column later long");
                QwpSchemaResponse after = describe(client, 72, "feedback_fresh");
                Assert.assertTrue(after.getMetadataVersion() > before.getMetadataVersion());
                Assert.assertEquals("later", after.getColumnName(2));
                sendCommit(client, encoder);
                WebSocketResponse ack = receive(client);
                Assert.assertEquals(1, ack.getSequence());
                Assert.assertEquals(1, ack.getSchemaUpdateCount());
                Assert.assertEquals(after.getMetadataVersion(), ack.getSchemaUpdate(0).getMetadataVersion());
                Assert.assertEquals("later", ack.getSchemaUpdate(0).getColumnName(2));
            }
        });
    }

    @Test
    public void testFragmentedAckThenNackKeepDistinctSchemaSets() throws Exception {
        execute("create table feedback_ack (n long, ts timestamp) timestamp(ts) partition by day wal");
        execute("create table feedback_nack (n long)");
        runInContext(port -> {
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer accepted = longTable("feedback_ack", 1);
                 QwpTableBuffer rejected = longTable("feedback_nack", 2)) {
                sendSchema(client, encoder, accepted);
                sendSchema(client, encoder, rejected);

                WebSocketResponse ack = receive(client);
                Assert.assertTrue(ack.isSuccess());
                Assert.assertEquals(0, ack.getSequence());
                Assert.assertEquals(1, ack.getSchemaUpdateCount());
                Assert.assertEquals("feedback_ack", ack.getSchemaUpdateTableName(0));

                WebSocketResponse nack = receive(client);
                Assert.assertFalse(nack.isSuccess());
                Assert.assertEquals(1, nack.getSequence());
                Assert.assertEquals(1, nack.getSchemaUpdateCount());
                Assert.assertEquals("feedback_nack", nack.getSchemaUpdateTableName(0));
            }
            drainWalQueue();
            assertQuery("select n from feedback_ack").noLeakCheck().expectSize().returns("n\n1\n");
            assertQuery("select count() from feedback_nack").noLeakCheck().expectSize().noRandomAccess().returns("count\n0\n");
        }, 65_536, Integer.MAX_VALUE, 1);
    }

    @Test
    public void testOversizedColumnCountPreservesMixedFeedback() throws Exception {
        // The designated timestamp adds one more column than the protocol limit.
        assertOversizedSnapshotPreservesMixedFeedback(QwpConstants.MAX_COLUMNS_PER_TABLE, 1_048_576);
    }

    @Test
    public void testOversizedSnapshotPreservesMixedFeedback() throws Exception {
        assertOversizedSnapshotPreservesMixedFeedback(20, 512);
    }

    @Test
    public void testOversizedSnapshotReturnsNamedResultOnNack() throws Exception {
        createWideTable("feedback_non_wal_wide", 20, false);
        runInContext(port -> {
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = longTable("feedback_non_wal_wide", 1)) {
                sendSchema(client, encoder, table);
                WebSocketResponse response = receive(client);
                Assert.assertFalse(response.isSuccess());
                Assert.assertEquals(0, response.getSequence());
                Assert.assertFalse(response.isSchemaInvalidation());
                Assert.assertEquals(1, response.getSchemaUpdateCount());
                assertContainsUpdate(response, "feedback_non_wal_wide", QwpSchemaProtocol.RESULT_TOO_LARGE);
            }
        }, 65_536, 65_536, 65_536, 512, null);
        assertQuery("SELECT count() FROM feedback_non_wal_wide").expectSize().noRandomAccess().returns("count\n0\n");
    }

    @Test
    public void testExactCapacityAckFallsBackToInvalidationWithoutLosingTables() throws Exception {
        runInContext(port -> {
            String tableA = repeat('a', 110);
            String tableB = repeat('b', 111);
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer a = longTable(tableA, 1);
                 QwpTableBuffer b = longTable(tableB, 2)) {
                encoder.beginSchemaMessage(2, new GlobalSymbolDictionary(), -1, -1);
                encoder.addSchemaTable(a, -1, -1);
                encoder.addSchemaTable(b, -1, -1);
                int length = encoder.finishMessage();
                client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                WebSocketResponse response = receive(client);
                Assert.assertTrue(response.isSuccess());
                Assert.assertTrue(response.isSchemaInvalidation());
                Assert.assertFalse(response.hasSchemaUpdates());
                Assert.assertEquals(2, response.getTableEntryCount());
                Assert.assertTrue(tableA.equals(response.getTableName(0)) || tableA.equals(response.getTableName(1)));
                Assert.assertTrue(tableB.equals(response.getTableName(0)) || tableB.equals(response.getTableName(1)));
            }
            drainWalQueue();
            assertQuery("select n from \"" + tableA + "\"").noLeakCheck().expectSize().returns("n\n1\n");
            assertQuery("select n from \"" + tableB + "\"").noLeakCheck().expectSize().returns("n\n2\n");
        }, 65_536, 65_536, 65_536, 256, null);
    }

    @Test
    public void testPreTableUpdateFailureReturnsCurrentAuthorizedSchemaOnNack() throws Exception {
        execute("create table feedback_non_wal (n long)");
        runInContext(port -> {
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = longTable("feedback_non_wal", 7)) {
                sendSchema(client, encoder, table);
                WebSocketResponse response = receive(client);
                Assert.assertFalse(response.isSuccess());
                Assert.assertEquals(0, response.getSequence());
                Assert.assertTrue(response.hasSchemaUpdates());
                Assert.assertEquals(1, response.getSchemaUpdateCount());
                Assert.assertEquals("feedback_non_wal", response.getSchemaUpdateTableName(0));
                Assert.assertEquals(QwpSchemaProtocol.RESULT_KNOWN, response.getSchemaUpdate(0).getResult());
                Assert.assertEquals("n", response.getSchemaUpdate(0).getColumnName(0));
            }
        }, 65_536, Integer.MAX_VALUE, 1);
        assertQuery("select count() from feedback_non_wal").noLeakCheck().expectSize().noRandomAccess().returns("count\n0\n");
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

    private static void assertContainsUpdate(WebSocketResponse response, String tableName) {
        assertContainsUpdate(response, tableName, QwpSchemaProtocol.RESULT_KNOWN);
    }

    private static void assertContainsUpdate(WebSocketResponse response, String tableName, int result) {
        for (int i = 0; i < response.getSchemaUpdateCount(); i++) {
            if (tableName.equals(response.getSchemaUpdateTableName(i))) {
                Assert.assertEquals(result, response.getSchemaUpdate(i).getResult());
                return;
            }
        }
        Assert.fail("missing schema feedback for " + tableName);
    }

    private static QwpSchemaResponse describe(WebSocketClient client, long requestId, String tableName) {
        byte[] request = QwpSchemaProtocol.encodeDescribe(requestId, tableName);
        long address = Unsafe.malloc(request.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < request.length; i++) {
                Unsafe.putByte(address + i, request[i]);
            }
            client.sendBinary(address, request.length);
            final QwpSchemaResponse[] response = new QwpSchemaResponse[1];
            Assert.assertTrue(client.receiveFrame(new WebSocketFrameHandler() {
                @Override
                public void onBinaryMessage(long payloadPtr, int payloadLen) {
                    response[0] = QwpSchemaProtocol.decodeResponse(payloadPtr, payloadLen);
                }

                @Override
                public void onClose(int code, String reason) {
                    Assert.fail("unexpected close [code=" + code + ", reason=" + reason + ']');
                }
            }, 5_000));
            Assert.assertNotNull(response[0]);
            Assert.assertEquals(requestId, response[0].getRequestId());
            Assert.assertEquals(QwpSchemaProtocol.RESULT_KNOWN, response[0].getResult());
            return response[0];
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

    private static WebSocketResponse receive(WebSocketClient client) {
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
        return response;
    }

    private static void sendCommit(WebSocketClient client, QwpWebSocketEncoder encoder) {
        encoder.getBuffer().reset();
        encoder.setDeferCommit(false);
        encoder.writeHeader(0, 0);
        client.sendBinary(encoder.getBuffer().getBufferPtr(), encoder.getBuffer().getPosition());
    }

    private static void sendSchema(WebSocketClient client, QwpWebSocketEncoder encoder, QwpTableBuffer table) {
        int length = encoder.encodeSchema(table, -1, -1);
        client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
    }

    private static String repeat(char c, int count) {
        StringBuilder sink = new StringBuilder(count);
        for (int i = 0; i < count; i++) {
            sink.append(c);
        }
        return sink.toString();
    }

    private void assertOversizedSnapshotPreservesMixedFeedback(int columnCount, int sendBufferSize) throws Exception {
        createWideTable("feedback_wide", columnCount, true);
        execute("CREATE TABLE feedback_small (n LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        runInContext(port -> {
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer wide = longTable("feedback_wide", 1);
                 QwpTableBuffer small = longTable("feedback_small", 2)) {
                for (int i = 0; i < 3; i++) {
                    encoder.beginSchemaMessage(2, new GlobalSymbolDictionary(), -1, -1);
                    encoder.addSchemaTable(i % 2 == 0 ? wide : small, -1, -1);
                    encoder.addSchemaTable(i % 2 == 0 ? small : wide, -1, -1);
                    int length = encoder.finishMessage();
                    client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                    WebSocketResponse response = receive(client);
                    Assert.assertTrue(response.isSuccess());
                    Assert.assertEquals(i, response.getSequence());
                    Assert.assertFalse(response.isSchemaInvalidation());
                    Assert.assertEquals(2, response.getSchemaUpdateCount());
                    assertContainsUpdate(response, "feedback_wide", QwpSchemaProtocol.RESULT_TOO_LARGE);
                    assertContainsUpdate(response, "feedback_small");
                }
                sendSchema(client, encoder, small);
                WebSocketResponse response = receive(client);
                Assert.assertTrue(response.isSuccess());
                Assert.assertEquals(3, response.getSequence());
                Assert.assertEquals(1, response.getSchemaUpdateCount());
                assertContainsUpdate(response, "feedback_small");
            }
            drainWalQueue();
            assertQuery("SELECT n FROM feedback_wide").noLeakCheck().expectSize().returns("n\n1\n1\n1\n");
            assertQuery("SELECT n FROM feedback_small").noLeakCheck().expectSize().returns("n\n2\n2\n2\n2\n");
        }, 65_536, 65_536, 65_536, sendBufferSize, null);
    }

    private void createWideTable(String tableName, int columnCount, boolean isWal) throws Exception {
        StringBuilder ddl = new StringBuilder("CREATE TABLE ").append(tableName).append(" (n LONG");
        for (int i = 1; i < columnCount; i++) {
            ddl.append(",column_name_").append(i).append("_abcdefghijklmnop LONG");
        }
        ddl.append(",ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY ").append(isWal ? "WAL" : "BYPASS WAL");
        execute(ddl);
    }
}
