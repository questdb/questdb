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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketClientFactory;
import io.questdb.client.cutlass.http.client.WebSocketFrameHandler;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaProtocol;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaResponse;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
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
    public void testAutoCachesTooLargeUntilFeedbackReportsShrink() throws Exception {
        StringBuilder ddl = new StringBuilder("create table schema_shrinks (");
        for (int i = 0; i < 20; i++) {
            ddl.append("column_name_long_enough_").append(i).append(" long, ");
        }
        ddl.append("ts timestamp) timestamp(ts) partition by day wal");
        execute(ddl);

        runInContext(port -> {
            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port
                    + ";schema_mode=auto;auto_flush_rows=2147483647;auto_flush_bytes=0;"
                    + "auto_flush_interval=2147483646;close_flush_timeout_millis=0;")) {
                QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                // Too wide for a 512-byte send buffer: AUTO writes legacy rows and
                // keeps the cached TOO_LARGE instead of asking on every batch.
                for (int i = 0; i < 3; i++) {
                    sender.table("schema_shrinks").longColumn("column_name_long_enough_0", i);
                    Assert.assertNull(ws.getTableBuffer("schema_shrinks").getSchemaBinding());
                    sender.atNow();
                    Assert.assertTrue(sender.drain(10_000));
                }
                for (int i = 1; i < 20; i++) {
                    execute("alter table schema_shrinks drop column column_name_long_enough_" + i);
                }
                // The next legacy frame sees the new version, so its ACK carries the
                // now-describable schema and replaces the cached TOO_LARGE.
                sender.table("schema_shrinks").longColumn("column_name_long_enough_0", 3).atNow();
                Assert.assertTrue(sender.drain(10_000));
                sender.table("schema_shrinks").longColumn("column_name_long_enough_0", 4);
                Assert.assertNotNull("shrunk table must bind its schema",
                        ws.getTableBuffer("schema_shrinks").getSchemaBinding());
                sender.atNow();
                Assert.assertTrue(sender.drain(10_000));
            }
            drainWalQueue();
            assertQuery("select column_name_long_enough_0 from schema_shrinks")
                    .noLeakCheck()
                    .expectSize()
                    .returns("column_name_long_enough_0\n0\n1\n2\n3\n4\n");
        }, 65_536, recvChunk, sendChunk, 512, null);
    }

    @Test
    public void testAutoSenderWritesLegacyRowsToTableWithLegacyColumnName() throws Exception {
        createLegacyColumnNameTable("legacy_column_name_ingest");

        runInContext(port -> {
            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port
                    + ";schema_mode=auto;auto_flush_rows=2147483647;auto_flush_bytes=0;"
                    + "auto_flush_interval=2147483646;close_flush_timeout_millis=0;")) {
                QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                for (int i = 0; i < 10; i++) {
                    sender.table("legacy_column_name_ingest").longColumn("x", i).atNow();
                }
                Assert.assertTrue(sender.drain(10_000));
                Assert.assertNull("a schema the client cannot decode must not bind",
                        ws.getTableBuffer("legacy_column_name_ingest").getSchemaBinding());
                // Before the fix, the client rejected the dashed name inside the schema
                // payload, dropped the ACK, and reconnected in a tight loop that replayed
                // the same rows on every connection.
                Assert.assertEquals(0, ws.getTotalReconnectAttempts());
            }
            drainWalQueue();
            assertQuery("select count(), sum(x) from legacy_column_name_ingest")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\tsum\n10\t45\n");
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
    public void testDescribeReturnsTooLargeForLegacyColumnName() throws Exception {
        createLegacyColumnNameTable("legacy_column_name_describe");

        runInContext(port -> {
            try (WebSocketClient client = connectSchemaClient(port)) {
                QwpSchemaResponse response = describe(client, 1, "legacy_column_name_describe");
                Assert.assertEquals(QwpSchemaProtocol.RESULT_TOO_LARGE, response.getResult());
                Assert.assertFalse(response.hasSchema());
            }
        });
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

    /**
     * QuestDB 6.0 to 6.2.0 accepted dashed ILP column names such as Telegraf's
     * user-agent, and upgrades keep them. TableModel writes the metadata directly,
     * skipping the SQL name check, which reproduces such a table.
     */
    private static void createLegacyColumnNameTable(String tableName) {
        TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY)
                .col("user-agent", ColumnType.VARCHAR)
                .col("x", ColumnType.LONG)
                .timestamp("ts")
                .wal();
        AbstractCairoTest.createTable(model);
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
