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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.DenyAllSecurityContext;
import io.questdb.cairo.security.ReadOnlySecurityContext;
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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class QwpSchemaFeedbackAuthorizationE2ETest extends AbstractQwpWebSocketTest {

    @Test
    public void testDeniedWriteDoesNotExposeUnknownOrStaleTableMetadata() throws Exception {
        execute("create table feedback_acl_denied (n long, secret string, ts timestamp) timestamp(ts) partition by day wal");
        runInContext(port -> {
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer unknown = longTable("feedback_acl_unknown", 1)) {
                send(client, encoder, unknown, -1, -1);
                assertNoFeedback(receive(client));
            }
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer stale = longTable("feedback_acl_denied", 2)) {
                send(client, encoder, stale, 999_999, 999_999);
                assertInvalidation(receive(client));
            }
        }, ReadOnlySecurityContext.INSTANCE);
        assertQuery("select count() from feedback_acl_denied").noLeakCheck().expectSize().noRandomAccess().returns("count\n0\n");
    }

    @Test
    public void testRevocationAfterAuthorizedWriteReturnsNamelessInvalidation() throws Exception {
        execute("create table feedback_acl_revoked (n long, ts timestamp) timestamp(ts) partition by day wal");
        AtomicBoolean revoked = new AtomicBoolean();
        SecurityContext context = revocableWriteContext(revoked, null);
        runInContext(port -> {
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = longTable("feedback_acl_revoked", 3)) {
                encoder.setDeferCommit(true);
                send(client, encoder, table, -1, -1);
                Assert.assertEquals(QwpSchemaProtocol.RESULT_KNOWN,
                        describe(client, 31, "feedback_acl_revoked").getResult());
                revoked.set(true);
                sendCommit(client, encoder);
                assertInvalidation(receive(client));
            }
        }, context);
        drainWalQueue();
        assertQuery("select count() from feedback_acl_revoked").noLeakCheck().expectSize().noRandomAccess().returns("count\n0\n");
    }

    @Test
    public void testMixedAuthorizedFeedbackFallsBackToNamelessInvalidation() throws Exception {
        execute("create table feedback_acl_allowed (n long, ts timestamp) timestamp(ts) partition by day wal");
        execute("create table feedback_acl_later_denied (n long, secret string, ts timestamp) timestamp(ts) partition by day wal");
        AtomicBoolean revoked = new AtomicBoolean();
        SecurityContext context = revocableWriteContext(revoked, "feedback_acl_later_denied");
        runInContext(port -> {
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer allowed = longTable("feedback_acl_allowed", 4);
                 QwpTableBuffer denied = longTable("feedback_acl_later_denied", 5)) {
                encoder.setDeferCommit(true);
                encoder.beginSchemaMessage(2, new GlobalSymbolDictionary(), -1, -1);
                encoder.addSchemaTable(allowed, -1, -1);
                encoder.addSchemaTable(denied, -1, -1);
                int length = encoder.finishMessage();
                client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                Assert.assertEquals(QwpSchemaProtocol.RESULT_KNOWN,
                        describe(client, 32, "feedback_acl_allowed").getResult());
                revoked.set(true);
                sendCommit(client, encoder);
                assertInvalidation(receive(client));
            }
        }, context);
        drainWalQueue();
        assertQuery("select n from feedback_acl_allowed").noLeakCheck().expectSize().returns("n\n4\n");
        assertQuery("select count() from feedback_acl_later_denied").noLeakCheck().expectSize().noRandomAccess().returns("count\n0\n");
    }

    @Test
    public void testWriteOnlyContextReceivesKnownFeedbackWithoutSelect() throws Exception {
        execute("create table feedback_acl_write_only (n long, secret string, ts timestamp) timestamp(ts) partition by day wal");
        SecurityContext writeOnly = revocableWriteContext(new AtomicBoolean(), null);
        TableToken token = engine.verifyTableName("feedback_acl_write_only");
        try {
            writeOnly.authorizeSelectOnAnyColumn(token);
            Assert.fail("write-only fixture unexpectedly permits SELECT");
        } catch (CairoException expected) {
            Assert.assertTrue(expected.isAuthorizationError());
        }
        runInContext(port -> {
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = longTable("feedback_acl_write_only", 6)) {
                send(client, encoder, table, -1, -1);
                WebSocketResponse response = receive(client);
                Assert.assertTrue(response.isSuccess());
                Assert.assertTrue(response.hasSchemaUpdates());
                Assert.assertFalse(response.isSchemaInvalidation());
                Assert.assertEquals(1, response.getSchemaUpdateCount());
                Assert.assertEquals("feedback_acl_write_only", response.getSchemaUpdateTableName(0));
                Assert.assertEquals(QwpSchemaProtocol.RESULT_KNOWN, response.getSchemaUpdate(0).getResult());
                Assert.assertEquals("secret", response.getSchemaUpdate(0).getColumnName(1));
            }
        }, writeOnly);
        drainWalQueue();
        assertQuery("select n from feedback_acl_write_only").noLeakCheck().expectSize().returns("n\n6\n");
    }

    private static void assertInvalidation(WebSocketResponse response) {
        Assert.assertEquals(WebSocketResponse.STATUS_SECURITY_ERROR, response.getStatus());
        Assert.assertFalse(response.isSuccess());
        Assert.assertTrue(response.isSchemaInvalidation());
        Assert.assertFalse(response.hasSchemaUpdates());
        Assert.assertEquals(0, response.getSchemaUpdateCount());
    }

    private static void assertNoFeedback(WebSocketResponse response) {
        Assert.assertEquals(WebSocketResponse.STATUS_SECURITY_ERROR, response.getStatus());
        Assert.assertFalse(response.isSuccess());
        Assert.assertFalse(response.isSchemaInvalidation());
        Assert.assertFalse(response.hasSchemaUpdates());
        Assert.assertEquals(0, response.getSchemaUpdateCount());
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

    private static QwpTableBuffer longTable(String tableName, long value) {
        QwpTableBuffer table = new QwpTableBuffer(tableName);
        table.getOrCreateColumn("n", QwpConstants.TYPE_LONG, true).addLong(value);
        table.nextRow();
        return table;
    }

    private static QwpSchemaResponse describe(WebSocketClient client, long requestId, String tableName) {
        byte[] request = QwpSchemaProtocol.encodeDescribe(requestId, tableName);
        long address = Unsafe.malloc(request.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < request.length; i++) {
                Unsafe.putByte(address + i, request[i]);
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
            return response.get();
        } finally {
            Unsafe.free(address, request.length, MemoryTag.NATIVE_DEFAULT);
        }
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

    private static SecurityContext revocableWriteContext(AtomicBoolean revoked, String revokedTable) {
        return new DenyAllSecurityContext() {
            @Override
            public void authorizeHttp() {
            }

            @Override
            public void authorizeInsert(TableToken tableToken) {
                if (revoked.get() && (revokedTable == null || revokedTable.equalsIgnoreCase(tableToken.getTableName()))) {
                    throw CairoException.authorization().put("INSERT permission denied");
                }
            }

            @Override
            public void authorizeSelectOnAnyColumn(TableToken tableToken) {
                throw CairoException.authorization().put("SELECT permission denied");
            }
        };
    }

    private static void send(WebSocketClient client, QwpWebSocketEncoder encoder, QwpTableBuffer table, int tableId, long metadataVersion) {
        int length = encoder.encodeSchema(table, tableId, metadataVersion);
        client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
    }

    private static void sendCommit(WebSocketClient client, QwpWebSocketEncoder encoder) {
        encoder.getBuffer().reset();
        encoder.setDeferCommit(false);
        encoder.writeHeader(0, 0);
        client.sendBinary(encoder.getBuffer().getBufferPtr(), encoder.getBuffer().getPosition());
    }
}
