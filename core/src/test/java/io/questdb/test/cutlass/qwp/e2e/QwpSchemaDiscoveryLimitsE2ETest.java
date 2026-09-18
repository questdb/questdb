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

import io.questdb.cairo.CairoEngine;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketClientFactory;
import io.questdb.client.cutlass.http.client.WebSocketFrameHandler;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaProtocol;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaResponse;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class QwpSchemaDiscoveryLimitsE2ETest extends AbstractQwpWebSocketTest {
    private static final AtomicBoolean READ_ONLY = new AtomicBoolean();

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractCairoTest.engineFactory = configuration -> new CairoEngine(configuration) {
            @Override
            public boolean isReadOnlyMode() {
                return READ_ONLY.get() || super.isReadOnlyMode();
            }
        };
        AbstractCairoTest.setUpStatic();
    }

    @After
    public void resetReadOnly() {
        READ_ONLY.set(false);
    }

    @Test
    public void testActiveColumnLimitReturnsTooLarge() throws Exception {
        StringBuilder ddl = new StringBuilder("create table schema_too_wide (");
        for (int i = 0; i < QwpSchemaProtocol.MAX_COLUMN_COUNT + 1; i++) {
            if (i > 0) {
                ddl.append(',');
            }
            ddl.append('c').append(i).append(" long");
        }
        ddl.append(')');
        execute(ddl);

        runInContext(port -> {
            try (WebSocketClient client = connectSchemaClient(port)) {
                QwpSchemaResponse response = describe(client, 1, "schema_too_wide");
                Assert.assertEquals(QwpSchemaProtocol.RESULT_TOO_LARGE, response.getResult());
                Assert.assertFalse(response.hasSchema());
            }
        });
    }

    @Test
    public void testConfiguredSendBufferReturnsTooLargeAndConnectionRemainsUsable() throws Exception {
        StringBuilder ddl = new StringBuilder("create table schema_large_response (");
        for (int i = 0; i < 20; i++) {
            if (i > 0) {
                ddl.append(',');
            }
            ddl.append("column_name_long_enough_").append(i).append(" long");
        }
        ddl.append(')');
        execute(ddl);
        execute("create table schema_small_response (x long)");

        runInContext(port -> {
            try (WebSocketClient client = connectSchemaClient(port)) {
                QwpSchemaResponse tooLarge = describe(client, 1, "schema_large_response");
                Assert.assertEquals(QwpSchemaProtocol.RESULT_TOO_LARGE, tooLarge.getResult());

                QwpSchemaResponse known = describe(client, 2, "schema_small_response");
                Assert.assertEquals(QwpSchemaProtocol.RESULT_KNOWN, known.getResult());
                Assert.assertEquals("x", known.getColumnName(0));
            }
        }, 65_536, recvChunk, sendChunk, 512, null);
    }

    @Test
    public void testLiveReadOnlyReturnsUnavailableAndConnectionRemainsUsable() throws Exception {
        execute("create table schema_role_change (x long)");

        runInContext(port -> {
            try (WebSocketClient client = connectSchemaClient(port)) {
                Assert.assertEquals(QwpSchemaProtocol.RESULT_KNOWN, describe(client, 1, "schema_role_change").getResult());

                READ_ONLY.set(true);
                Assert.assertEquals(QwpSchemaProtocol.RESULT_UNAVAILABLE, describe(client, 2, "schema_role_change").getResult());

                READ_ONLY.set(false);
                Assert.assertEquals(QwpSchemaProtocol.RESULT_KNOWN, describe(client, 3, "schema_role_change").getResult());
            }
        });
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
            return response.get();
        } finally {
            Unsafe.free(address, request.length, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
