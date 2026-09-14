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

import io.questdb.client.LineSenderSchemaException;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketClientFactory;
import io.questdb.client.cutlass.http.client.WebSocketFrameHandler;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaProtocol;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaResponse;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaBinding;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpFixedWidthColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpMessageCursor;
import io.questdb.cutlass.qwp.protocol.QwpTableBlockCursor;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class QwpSchemaRowBufferE2ETest extends AbstractQwpWebSocketTest {
    private static final String UUID_VECTORS = "/io/questdb/client/cutlass/qwp/uuid-string-conformance.tsv";

    @Test
    public void testLocalFailureCancelsOnlyCurrentRowAndEncodesBinaryUuid() throws Exception {
        runInContext(port -> {
            execute("create table schema_rows (id uuid, n long, only_b long, extra boolean, ts timestamp) timestamp(ts) partition by day wal");
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = new QwpTableBuffer("schema_rows")) {
                QwpSchemaBinding binding = new QwpSchemaBinding(table, describe(client, 1, "schema_rows"));
                binding.stringColumn("id", "01234567-89ab-cdef-fedc-ba9876543210").longColumn("n", 11);
                table.nextRow();

                binding.longColumn("only_b", 99);
                LineSenderSchemaException failure = Assert.assertThrows(
                        LineSenderSchemaException.class,
                        () -> binding.stringColumn("id", "not-a-uuid")
                );
                Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, failure.getReason());
                Assert.assertFalse(failure.isRetryable());
                table.cancelCurrentRow();
                table.rollbackUncommittedColumns();

                binding.uuidColumn("id", 0xbb6d6bb9bd380a11L, 0xa0eebc999c0b4ef8L).longColumn("n", 33);
                table.nextRow();
                int length = encoder.encodeSchema(table);
                assertBinaryUuidFrame(encoder, length);
                client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                assertOk(client, 0);
            }
            drainWalQueue();
            assertQuery("select id, n, only_b, extra from schema_rows order by n")
                    .noLeakCheck()
                    .returnsOnce("id\tn\tonly_b\textra\n"
                            + "01234567-89ab-cdef-fedc-ba9876543210\t11\tnull\tfalse\n"
                            + "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\t33\tnull\tfalse\n");
        });
    }

    @Test
    public void testDuplicateFirstValueWinsBeforeCrossSetterConversion() throws Exception {
        runInContext(port -> {
            execute("create table schema_duplicate (id uuid, n long, ts timestamp) timestamp(ts) partition by day wal");
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = new QwpTableBuffer("schema_duplicate")) {
                QwpSchemaBinding binding = new QwpSchemaBinding(table, describe(client, 2, "schema_duplicate"));
                binding.uuidColumn("id", 0xfedcba9876543210L, 0x0123456789abcdefL)
                        .stringColumn("id", "malformed duplicate must not be parsed")
                        .longColumn("n", 1)
                        .longColumn("n", 2);
                table.nextRow();
                int length = encoder.encodeSchema(table);
                client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                assertOk(client, 0);
            }
            drainWalQueue();
            assertQuery("select id, n from schema_duplicate")
                    .noLeakCheck()
                    .returnsOnce("id\tn\n01234567-89ab-cdef-fedc-ba9876543210\t1\n");
        });
    }

    @Test
    public void testNullOmissionAndUnsupportedUsedColumns() throws Exception {
        runInContext(port -> {
            execute("create table schema_sparse (id uuid, n long, unsupported binary, ts timestamp) timestamp(ts) partition by day wal");
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = new QwpTableBuffer("schema_sparse")) {
                QwpSchemaBinding binding = new QwpSchemaBinding(table, describe(client, 3, "schema_sparse"));
                binding.stringColumn("id", null);
                table.nextRow();
                binding.longColumn("n", 7);
                table.nextRow();

                LineSenderSchemaException wrongTarget = Assert.assertThrows(
                        LineSenderSchemaException.class,
                        () -> binding.longColumn("id", 1)
                );
                Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, wrongTarget.getReason());
                table.cancelCurrentRow();
                table.rollbackUncommittedColumns();
                LineSenderSchemaException unsupported = Assert.assertThrows(
                        LineSenderSchemaException.class,
                        () -> binding.longColumn("unsupported", 1)
                );
                Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, unsupported.getReason());
                table.cancelCurrentRow();
                table.rollbackUncommittedColumns();

                int length = encoder.encodeSchema(table);
                client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                assertOk(client, 0);
            }
            drainWalQueue();
            assertQuery("select id, n, unsupported from schema_sparse where n = 7")
                    .noLeakCheck()
                    .returnsOnce("id\tn\tunsupported\n\t7\t\n");
            assertQuery("select id, n, unsupported from schema_sparse where n is null")
                    .noLeakCheck()
                    .returnsOnce("id\tn\tunsupported\n\tnull\t\n");
        });
    }

    @Test
    public void testUuidCorpusMatchesLegacyVarcharConversion() throws Exception {
        List<String[]> vectors = readVectors();
        runInContext(port -> {
            execute("create table schema_uuid_vectors (case_id long, value uuid, ts timestamp) timestamp(ts) partition by day wal");
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = new QwpTableBuffer("schema_uuid_vectors")) {
                QwpSchemaBinding binding = new QwpSchemaBinding(table, describe(client, 4, "schema_uuid_vectors"));
                int accepted = 0;
                for (String[] vector : vectors) {
                    String input = "<NULL>".equals(vector[1]) ? null : vector[1];
                    if ("<INVALID>".equals(vector[2])) {
                        LineSenderSchemaException ex = Assert.assertThrows(
                                vector[0], LineSenderSchemaException.class,
                                () -> binding.stringColumn("value", input)
                        );
                        Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, ex.getReason());
                        table.cancelCurrentRow();
                        table.rollbackUncommittedColumns();
                    } else {
                        binding.stringColumn("value", input).longColumn("case_id", accepted++);
                        table.nextRow();
                    }
                }
                int length = encoder.encodeSchema(table);
                client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                assertOk(client, 0);
            }
            drainWalQueue();
            assertQuery("select value from schema_uuid_vectors order by case_id")
                    .noLeakCheck()
                    .returnsOnce(expectedValidValues(vectors));

            execute("create table legacy_uuid_vectors (case_id long, value uuid, ts timestamp) timestamp(ts) partition by day wal");
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = new QwpTableBuffer("legacy_uuid_vectors")) {
                int accepted = 0;
                for (String[] vector : vectors) {
                    if ("<INVALID>".equals(vector[2])) {
                        continue;
                    }
                    table.getOrCreateColumn("case_id", QwpConstants.TYPE_LONG, true).addLong(accepted++);
                    QwpTableBuffer.ColumnBuffer value = table.getOrCreateColumn("value", QwpConstants.TYPE_VARCHAR, true);
                    if ("<NULL>".equals(vector[1])) {
                        value.addNull();
                    } else {
                        value.addString(vector[1]);
                    }
                    table.nextRow();
                }
                int length = encoder.encode(table);
                client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                assertOk(client, 0);
            }
            drainWalQueue();
            assertQuery("select value from legacy_uuid_vectors order by case_id")
                    .noLeakCheck()
                    .returnsOnce(expectedValidValues(vectors));

            // The production server converter must reject the same invalid corpus. Each
            // NACK gets its own stream because an ingress error terminates that sequence.
            int invalidTable = 0;
            for (String[] vector : vectors) {
                if (!"<INVALID>".equals(vector[2])) {
                    continue;
                }
                String tableName = "legacy_uuid_bad_" + invalidTable++;
                execute("create table " + tableName + " (value uuid, ts timestamp) timestamp(ts) partition by day wal");
                try (WebSocketClient client = connect(port);
                     QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                     QwpTableBuffer table = new QwpTableBuffer(tableName)) {
                    table.getOrCreateColumn("value", QwpConstants.TYPE_VARCHAR, true).addString(vector[1]);
                    table.nextRow();
                    int length = encoder.encode(table);
                    client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                    assertNack(client, vector[0]);
                }
            }
        });
    }

    private static void assertBinaryUuidFrame(QwpWebSocketEncoder encoder, int length) throws Exception {
        Assert.assertEquals(QwpConstants.FLAG_SCHEMA, Unsafe.getByte(encoder.getBuffer().getBufferPtr() + QwpConstants.HEADER_OFFSET_FLAGS) & QwpConstants.FLAG_SCHEMA);
        QwpMessageCursor message = new QwpMessageCursor();
        message.of(encoder.getBuffer().getBufferPtr(), length, new ObjList<>());
        Assert.assertTrue(message.hasNextTable());
        QwpTableBlockCursor table = message.nextTable();
        Assert.assertTrue(table.hasKnownSchemaIdentity());
        Assert.assertTrue(table.getSchemaTableId() >= 0);
        Assert.assertTrue(table.getSchemaMetadataVersion() >= 0);
        Assert.assertEquals(2, table.getRowCount());
        Assert.assertEquals(2, table.getColumnCount());
        Assert.assertEquals("id", table.getColumnDef(0).getName());
        Assert.assertEquals(QwpConstants.TYPE_UUID, table.getColumnDef(0).getTypeCode());
        Assert.assertEquals("n", table.getColumnDef(1).getName());
        Assert.assertEquals(QwpConstants.TYPE_LONG, table.getColumnDef(1).getTypeCode());
        Assert.assertTrue(table.hasNextRow());
        table.nextRow();
        QwpFixedWidthColumnCursor uuid = table.getFixedWidthColumn(0);
        Assert.assertEquals(0xfedcba9876543210L, uuid.getUuidLo());
        Assert.assertEquals(0x0123456789abcdefL, uuid.getUuidHi());
    }

    private static void assertNack(WebSocketClient client, String caseId) {
        WebSocketResponse response = receiveResponse(client);
        Assert.assertFalse(caseId, response.isSuccess());
        Assert.assertTrue(caseId + ": " + response.getErrorMessage(), response.getErrorMessage().contains("UUID"));
    }

    private static void assertOk(WebSocketClient client, long sequence) {
        WebSocketResponse response = receiveResponse(client);
        Assert.assertTrue(response.getErrorMessage(), response.isSuccess());
        Assert.assertEquals(sequence, response.getSequence());
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

    private static String expectedValidValues(List<String[]> vectors) {
        StringBuilder sink = new StringBuilder("value\n");
        for (String[] vector : vectors) {
            if (!"<INVALID>".equals(vector[2])) {
                if (!"<NULL>".equals(vector[2])) {
                    sink.append(vector[2]);
                }
                sink.append('\n');
            }
        }
        return sink.toString();
    }

    private static List<String[]> readVectors() throws Exception {
        List<String[]> vectors = new ArrayList<>();
        try (InputStream stream = QwpSchemaRowBufferE2ETest.class.getResourceAsStream(UUID_VECTORS)) {
            Assert.assertNotNull(stream);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (!line.startsWith("#")) {
                        String[] fields = line.split("\\t", -1);
                        Assert.assertEquals(line, 3, fields.length);
                        vectors.add(fields);
                    }
                }
            }
        }
        return vectors;
    }

    private static WebSocketResponse receiveResponse(WebSocketClient client) {
        WebSocketResponse response = new WebSocketResponse();
        Assert.assertTrue(client.receiveFrame(new WebSocketFrameHandler() {
            @Override
            public void onBinaryMessage(long payloadPtr, int payloadLen) {
                Assert.assertTrue(response.readFrom(payloadPtr, payloadLen, client.isQwpSchemaEnabled()));
            }

            @Override
            public void onClose(int code, String reason) {
                Assert.fail("unexpected close [code=" + code + ", reason=" + reason + ']');
            }
        }, 5_000));
        return response;
    }
}
