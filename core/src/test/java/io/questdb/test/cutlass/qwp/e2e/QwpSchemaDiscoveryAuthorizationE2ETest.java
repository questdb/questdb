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
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaProtocol;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaResponse;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

public class QwpSchemaDiscoveryAuthorizationE2ETest extends AbstractQwpWebSocketTest {
    @Test
    public void testInsertDeniedReturnsDeniedWithoutSchema() throws Exception {
        execute("create table schema_acl_denied (id long, ts timestamp) timestamp(ts) partition by day wal");
        runInContext(port -> {
            try (WebSocketClient client = connectSchemaClient(port)) {
                QwpSchemaResponse response = describe(client, 1, "schema_acl_denied");
                Assert.assertEquals(QwpSchemaProtocol.RESULT_DENIED, response.getResult());
                Assert.assertFalse(response.hasSchema());
                Assert.assertEquals(0, response.getColumnCount());
                Assert.assertEquals(-1, response.getTableId());
                Assert.assertEquals(-1, response.getMetadataVersion());
                Assert.assertEquals(-1, response.getDesignatedIndex());
            }
        }, ReadOnlySecurityContext.INSTANCE);
    }

    @Test
    public void testWriteOnlyContextCanDescribeIngestionSchema() throws Exception {
        execute("create table schema_acl_write_only (id long, ts timestamp) timestamp(ts) partition by day wal");
        SecurityContext writeOnly = new DenyAllSecurityContext() {
            @Override
            public void authorizeHttp() {
            }

            @Override
            public void authorizeInsert(TableToken tableToken) {
            }

            @Override
            public void authorizeSelectOnAnyColumn(TableToken tableToken) {
                throw CairoException.authorization().put("SELECT permission denied");
            }
        };
        TableToken token = engine.verifyTableName("schema_acl_write_only");
        try {
            writeOnly.authorizeSelectOnAnyColumn(token);
            Assert.fail("write-only fixture unexpectedly permits SELECT");
        } catch (CairoException expected) {
            Assert.assertTrue("SELECT denial must be classified as authorization", expected.isAuthorizationError());
            // Fixture precondition: schema discovery must not require SELECT.
        }

        runInContext(port -> {
            try (WebSocketClient client = connectSchemaClient(port)) {
                QwpSchemaResponse response = describe(client, 2, "schema_acl_write_only");
                Assert.assertEquals(QwpSchemaProtocol.RESULT_KNOWN, response.getResult());
                Assert.assertTrue(response.hasSchema());
                Assert.assertEquals(2, response.getColumnCount());
                Assert.assertEquals("id", response.getColumnName(0));
                Assert.assertEquals("ts", response.getColumnName(1));
            }
        }, writeOnly);
    }

    private static WebSocketClient connectSchemaClient(int port) {
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
            QwpSchemaResponse decoded = response.get();
            Assert.assertNotNull(decoded);
            Assert.assertEquals(requestId, decoded.getRequestId());
            return decoded;
        } finally {
            Unsafe.free(address, request.length, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
