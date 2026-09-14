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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.client.LineSenderSchemaException;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketClientFactory;
import io.questdb.client.cutlass.http.client.WebSocketFrameHandler;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaBinding;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaProtocol;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaResponse;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.cutlass.qwp.protocol.QwpBooleanColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpFixedWidthColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpMessageCursor;
import io.questdb.cutlass.qwp.protocol.QwpStringColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpTableBlockCursor;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8Sequence;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class QwpSchemaBooleanE2ETest extends AbstractQwpWebSocketTest {
    private static final String BOOL_VECTORS = "/io/questdb/client/cutlass/qwp/boolean-to-target.tsv";
    private static final String STRING_VECTORS = "/io/questdb/client/cutlass/qwp/string-to-boolean.tsv";

    @Test
    public void testBooleanCorpusUsesExactTargetWireAndStoredValues() throws Exception {
        List<BoolVector> vectors = readBoolVectors();
        runInContext(port -> {
            for (Target target : Target.values()) {
                String tableName = "schema_bool_" + target.suffix;
                String legacyTable = "legacy_bool_" + target.suffix;
                createTable(tableName, target.sqlType);
                createTable(legacyTable, target.sqlType);
                List<BoolVector> selected = select(vectors, target);
                try (WebSocketClient client = connect(port);
                     QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                     QwpTableBuffer table = new QwpTableBuffer(tableName)) {
                    QwpSchemaBinding binding = new QwpSchemaBinding(table, describe(client, 100 + target.ordinal(), tableName));
                    for (int i = 0; i < selected.size(); i++) {
                        binding.longColumn("case_id", i).boolColumn("value", selected.get(i).input);
                        table.nextRow();
                    }
                    int length = encoder.encodeSchema(table);
                    assertBoolWire(encoder, length, target, selected);
                    client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                    assertResponse(client, true);
                }
                try (WebSocketClient client = connectLegacy(port);
                     QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                     QwpTableBuffer table = new QwpTableBuffer(legacyTable)) {
                    for (int i = 0; i < selected.size(); i++) {
                        table.getOrCreateColumn("case_id", QwpConstants.TYPE_LONG, true).addLong(i);
                        table.getOrCreateColumn("value", QwpConstants.TYPE_BOOLEAN, true)
                                .addBoolean(selected.get(i).input);
                        table.nextRow();
                    }
                    sendLegacy(client, encoder, table);
                }
                drainWalQueue();
                assertBoolStored(tableName, target, selected);
                assertBoolStored(legacyTable, target, selected);
            }
        });
    }

    @Test
    public void testStringBooleanGrammarMatchesLegacyServerParser() throws Exception {
        List<StringVector> vectors = readStringVectors();
        runInContext(port -> {
            String schemaTable = "schema_string_bool";
            String legacyTable = "legacy_string_bool";
            createTable(schemaTable, "boolean");
            createTable(legacyTable, "boolean");
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = new QwpTableBuffer(schemaTable)) {
                QwpSchemaBinding binding = new QwpSchemaBinding(table, describe(client, 200, schemaTable));
                int accepted = 0;
                for (StringVector vector : vectors) {
                    if (vector.invalid) {
                        LineSenderSchemaException error = Assert.assertThrows(vector.caseId,
                                LineSenderSchemaException.class, () -> binding.stringColumn("value", vector.input));
                        Assert.assertEquals(vector.caseId, LineSenderSchemaException.Reason.INVALID_VALUE, error.getReason());
                        table.cancelCurrentRow();
                        table.rollbackUncommittedColumns();
                    } else {
                        binding.longColumn("case_id", accepted++).stringColumn("value", vector.input);
                        table.nextRow();
                    }
                }
                int length = encoder.encodeSchema(table);
                QwpTableBlockCursor wire = parseSingleTable(encoder, length);
                Assert.assertEquals(QwpConstants.TYPE_BOOLEAN, wire.getColumnDef(1).getTypeCode());
                int row = 0;
                for (StringVector vector : vectors) {
                    if (!vector.invalid) {
                        Assert.assertTrue(vector.caseId, wire.hasNextRow());
                        wire.nextRow();
                        Assert.assertEquals(vector.caseId, vector.wireNull, wire.isColumnNull(1));
                        if (!vector.wireNull) {
                            Assert.assertEquals(vector.caseId, vector.value, wire.getBooleanColumn(1).getValue());
                        }
                        row++;
                    }
                }
                Assert.assertEquals(row, wire.getRowCount());
                client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                assertResponse(client, true);
            }

            int accepted = 0;
            for (StringVector vector : vectors) {
                if (!vector.invalid) {
                    try (WebSocketClient client = connectLegacy(port);
                         QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                         QwpTableBuffer table = new QwpTableBuffer(legacyTable)) {
                        table.getOrCreateColumn("case_id", QwpConstants.TYPE_LONG, true).addLong(accepted++);
                        table.getOrCreateColumn("value", QwpConstants.TYPE_VARCHAR, true).addString(vector.input);
                        table.nextRow();
                        sendLegacy(client, encoder, table);
                    }
                } else {
                    assertLegacyStringRejected(port, legacyTable, vector);
                }
            }
            drainWalQueue();
            assertBooleanRows(schemaTable, vectors);
            assertBooleanRows(legacyTable, vectors);
        });
    }

    @Test
    public void testNullablePackedBooleanCrossesBitmapBoundaries() throws Exception {
        runInContext(port -> {
            String tableName = "schema_bool_packed";
            createTable(tableName, "boolean");
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = new QwpTableBuffer(tableName)) {
                QwpSchemaBinding binding = new QwpSchemaBinding(table, describe(client, 300, tableName));
                for (int row = 0; row < 137; row++) {
                    binding.longColumn("case_id", row);
                    if (row % 5 == 0) {
                        binding.stringColumn("value", null);
                    } else if (row % 7 != 0) {
                        binding.boolColumn("value", (row & 1) != 0);
                    }
                    table.nextRow();
                }
                int length = encoder.encodeSchema(table);
                QwpTableBlockCursor wire = parseSingleTable(encoder, length);
                Assert.assertEquals(137, wire.getRowCount());
                Assert.assertEquals(QwpConstants.TYPE_BOOLEAN, wire.getColumnDef(1).getTypeCode());
                for (int row = 0; row < 137; row++) {
                    Assert.assertTrue(wire.hasNextRow());
                    wire.nextRow();
                    boolean missing = row % 5 == 0 || row % 7 == 0;
                    Assert.assertEquals("row " + row, missing, wire.isColumnNull(1));
                    if (!missing) {
                        Assert.assertEquals("row " + row, (row & 1) != 0, wire.getBooleanColumn(1).getValue());
                    }
                }
                Assert.assertFalse(wire.hasNextRow());
                client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                assertResponse(client, true);
            }
            drainWalQueue();
            try (RecordCursorFactory factory = select("select case_id, value from " + tableName + " order by case_id");
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                for (int row = 0; row < 137; row++) {
                    Assert.assertTrue("stored row " + row, cursor.hasNext());
                    Assert.assertEquals(row, record.getLong(0));
                    boolean missing = row % 5 == 0 || row % 7 == 0;
                    Assert.assertEquals("stored row " + row, !missing && (row & 1) != 0, record.getBool(1));
                }
                Assert.assertFalse(cursor.hasNext());
            }
        });
    }

    @Test
    public void testSchemaOmissionUsesTargetNullWhileLegacyBooleanUsesFalse() throws Exception {
        runInContext(port -> {
            execute("create table schema_bool_missing (i int, s string, ts timestamp) timestamp(ts) partition by day wal");
            execute("create table legacy_bool_missing (i int, s string, ts timestamp) timestamp(ts) partition by day wal");
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = new QwpTableBuffer("schema_bool_missing")) {
                QwpSchemaBinding binding = new QwpSchemaBinding(table, describe(client, 350, "schema_bool_missing"));
                binding.boolColumn("i", true).boolColumn("s", true);
                table.nextRow();
                table.nextRow();
                int length = encoder.encodeSchema(table);
                QwpTableBlockCursor wire = parseSingleTable(encoder, length);
                Assert.assertTrue(wire.hasNextRow());
                wire.nextRow();
                Assert.assertFalse(wire.isColumnNull(0));
                Assert.assertFalse(wire.isColumnNull(1));
                Assert.assertTrue(wire.hasNextRow());
                wire.nextRow();
                Assert.assertTrue(wire.isColumnNull(0));
                Assert.assertTrue(wire.isColumnNull(1));
                client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                assertResponse(client, true);
            }
            try (WebSocketClient client = connectLegacy(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = new QwpTableBuffer("legacy_bool_missing")) {
                // Legacy Sender boolean columns do not use a null bitmap, so omission appends false.
                table.getOrCreateColumn("i", QwpConstants.TYPE_BOOLEAN, false).addBoolean(true);
                table.getOrCreateColumn("s", QwpConstants.TYPE_BOOLEAN, false).addBoolean(true);
                table.nextRow();
                table.nextRow();
                sendLegacy(client, encoder, table);
            }
            drainWalQueue();
            assertQuery("select i, s, i is null ni, s is null ns from schema_bool_missing")
                    .noLeakCheck().returnsOnce("i\ts\tni\tns\n1\ttrue\tfalse\tfalse\nnull\t\ttrue\ttrue\n");
            assertQuery("select i, s, i is null ni, s is null ns from legacy_bool_missing")
                    .noLeakCheck().returnsOnce("i\ts\tni\tns\n1\ttrue\tfalse\tfalse\n0\tfalse\tfalse\tfalse\n");
        });
    }

    @Test
    public void testRollbackDuplicateAndUnsupportedSymbol() throws Exception {
        runInContext(port -> {
            execute("create table schema_bool_rows (value boolean, only_b long, unsupported symbol, ts timestamp) "
                    + "timestamp(ts) partition by day wal");
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = new QwpTableBuffer("schema_bool_rows")) {
                QwpSchemaBinding binding = new QwpSchemaBinding(table, describe(client, 400, "schema_bool_rows"));
                binding.boolColumn("value", true);
                table.nextRow();
                binding.longColumn("only_b", 42);
                LineSenderSchemaException error = Assert.assertThrows(LineSenderSchemaException.class,
                        () -> binding.stringColumn("value", "not-a-boolean"));
                Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, error.getReason());
                table.cancelCurrentRow();
                table.rollbackUncommittedColumns();
                binding.boolColumn("value", false).stringColumn("value", "still-invalid-but-duplicate");
                table.nextRow();
                LineSenderSchemaException unsupported = Assert.assertThrows(LineSenderSchemaException.class,
                        () -> binding.boolColumn("unsupported", true));
                Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, unsupported.getReason());
                table.cancelCurrentRow();
                table.rollbackUncommittedColumns();
                binding.boolColumn("value", true);
                table.nextRow();
                int length = encoder.encodeSchema(table);
                QwpTableBlockCursor wire = parseSingleTable(encoder, length);
                Assert.assertEquals(1, wire.getColumnCount());
                Assert.assertEquals(3, wire.getRowCount());
                client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                assertResponse(client, true);
            }
            drainWalQueue();
            assertQuery("select value, only_b, unsupported, unsupported is null u from schema_bool_rows")
                    .noLeakCheck().returnsOnce("value\tonly_b\tunsupported\tu\ntrue\tnull\t\ttrue\nfalse\tnull\t\ttrue\ntrue\tnull\t\ttrue\n");
        });
    }

    @Test
    public void testConvertedBooleanReplayFromStoreAndForward() throws Exception {
        runInContext(port -> {
            execute("create table schema_bool_replay (b boolean, n long, s varchar, ts timestamp) timestamp(ts) partition by day wal");
            File sfRoot = temp.newFolder("qwp-schema-bool-sf");
            String slot = new File(sfRoot, "default").getAbsolutePath();
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = new QwpTableBuffer("schema_bool_replay")) {
                QwpSchemaBinding binding = new QwpSchemaBinding(table, describe(client, 500, "schema_bool_replay"));
                binding.boolColumn("b", true).boolColumn("n", false).boolColumn("s", true);
                table.nextRow();
                int length = encoder.encodeSchema(table);
                QwpTableBlockCursor wire = parseSingleTable(encoder, length);
                Assert.assertEquals(QwpConstants.TYPE_BOOLEAN, wire.getColumnDef(0).getTypeCode());
                Assert.assertEquals(QwpConstants.TYPE_LONG, wire.getColumnDef(1).getTypeCode());
                Assert.assertEquals(QwpConstants.TYPE_VARCHAR, wire.getColumnDef(2).getTypeCode());
                Assert.assertTrue(wire.hasNextRow());
                wire.nextRow();
                Assert.assertTrue(wire.getBooleanColumn(0).getValue());
                Assert.assertEquals(0, wire.getFixedWidthColumn(1).getLong());
                assertUtf8("replay string", "true".getBytes(StandardCharsets.UTF_8), wire.getStringColumn(2).getUtf8Value());
                Assert.assertFalse(wire.hasNextRow());
                try (CursorSendEngine engine = new CursorSendEngine(slot, 1 << 20)) {
                    Assert.assertEquals(0, engine.appendBlocking(encoder.getBuffer().getBufferPtr(), length));
                }
            }
            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";sf_dir="
                    + sfRoot.getAbsolutePath() + ";sender_id=default;sf_max_segment_bytes=1m;")) {
                Assert.assertTrue(sender.drain(10_000));
            }
            drainWalQueue();
            assertQuery("select b, n, s from schema_bool_replay")
                    .noLeakCheck().returnsOnce("b\tn\ts\ntrue\t0\ttrue\n");
        });
    }

    private void assertBoolStored(String tableName, Target target, List<BoolVector> vectors) throws Exception {
        try (RecordCursorFactory factory = select("select case_id, value from " + tableName + " order by case_id");
             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Record record = cursor.getRecord();
            int row = 0;
            while (cursor.hasNext()) {
                BoolVector vector = vectors.get(row);
                Assert.assertEquals(row, record.getLong(0));
                target.assertRecord(vector, record);
                row++;
            }
            Assert.assertEquals(vectors.size(), row);
        }
    }


    private static void assertBoolWire(QwpWebSocketEncoder encoder, int length, Target target, List<BoolVector> vectors) throws Exception {
        QwpTableBlockCursor table = parseSingleTable(encoder, length);
        Assert.assertEquals(target.wireType, table.getColumnDef(1).getTypeCode());
        for (BoolVector vector : vectors) {
            Assert.assertTrue(table.hasNextRow());
            table.nextRow();
            Assert.assertFalse(table.isColumnNull(1));
            target.assertWire(vector, table);
        }
        Assert.assertFalse(table.hasNextRow());
    }

    private void assertBooleanRows(String tableName, List<StringVector> vectors) throws Exception {
        int expected = 0;
        try (RecordCursorFactory factory = select("select case_id, value from " + tableName + " order by case_id");
             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Record record = cursor.getRecord();
            for (StringVector vector : vectors) {
                if (!vector.invalid) {
                    Assert.assertTrue(vector.caseId, cursor.hasNext());
                    Assert.assertEquals(expected++, record.getLong(0));
                    Assert.assertEquals(vector.caseId, vector.value, record.getBool(1));
                }
            }
            Assert.assertFalse(cursor.hasNext());
        }
    }

    private void assertLegacyStringRejected(int port, String tableName, StringVector vector) throws Exception {
        try (WebSocketClient client = connectLegacy(port);
             QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
             QwpTableBuffer table = new QwpTableBuffer(tableName)) {
            table.getOrCreateColumn("value", io.questdb.client.cutlass.qwp.protocol.QwpConstants.TYPE_VARCHAR, true)
                    .addString(vector.input);
            table.nextRow();
            int length = encoder.encode(table);
            client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
            WebSocketResponse response = receiveResponse(client);
            Assert.assertFalse(response.isSuccess());
            Assert.assertEquals(WebSocketResponse.STATUS_SCHEMA_MISMATCH, response.getStatus());
            Assert.assertTrue(response.getErrorMessage(), response.getErrorMessage().contains("cannot parse boolean from string"));
        }
    }

    private static WebSocketClient connectLegacy(int port) {
        WebSocketClient client = WebSocketClientFactory.newPlainTextInstance();
        boolean success = false;
        try {
            client.connect("127.0.0.1", port);
            client.upgrade("/write/v4", null);
            Assert.assertFalse(client.isQwpSchemaEnabled());
            success = true;
            return client;
        } finally {
            if (!success) {
                client.close();
            }
        }
    }

    private static void sendLegacy(WebSocketClient client, QwpWebSocketEncoder encoder, QwpTableBuffer table) {
        int length = encoder.encode(table);
        Assert.assertEquals(0, Unsafe.getByte(encoder.getBuffer().getBufferPtr() + QwpConstants.HEADER_OFFSET_FLAGS)
                & QwpConstants.FLAG_SCHEMA);
        client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
        assertResponse(client, true);
    }

    private void createTable(String name, String type) throws Exception {
        execute("create table " + name + " (case_id long, value " + type + ", ts timestamp) timestamp(ts) partition by day wal");
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
            for (int i = 0; i < request.length; i++) Unsafe.putByte(address + i, request[i]);
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
        Assert.assertEquals(requestId, response.get().getRequestId());
        return response.get();
    }

    private static void assertResponse(WebSocketClient client, boolean success) {
        WebSocketResponse parsed = receiveResponse(client);
        Assert.assertEquals(parsed.getErrorMessage(), success, parsed.isSuccess());
    }

    private static WebSocketResponse receiveResponse(WebSocketClient client) {
        AtomicReference<WebSocketResponse> response = new AtomicReference<>();
        Assert.assertTrue(client.receiveFrame(new WebSocketFrameHandler() {
            @Override
            public void onBinaryMessage(long payloadPtr, int payloadLen) {
                WebSocketResponse parsed = new WebSocketResponse();
                Assert.assertTrue(parsed.readFrom(payloadPtr, payloadLen, true));
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

    private static QwpTableBlockCursor parseSingleTable(QwpWebSocketEncoder encoder, int length) throws Exception {
        QwpMessageCursor cursor = new QwpMessageCursor();
        cursor.of(encoder.getBuffer().getBufferPtr(), length, new ObjList<>());
        Assert.assertTrue(cursor.hasNextTable());
        QwpTableBlockCursor table = cursor.nextTable();
        Assert.assertFalse(cursor.hasNextTable());
        return table;
    }

    private static List<BoolVector> readBoolVectors() throws Exception {
        List<BoolVector> result = new ArrayList<>();
        try (InputStream stream = QwpSchemaBooleanE2ETest.class.getResourceAsStream(BOOL_VECTORS)) {
            Assert.assertNotNull(BOOL_VECTORS, stream);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.isEmpty() || line.charAt(0) == '#') continue;
                    String[] f = line.split("\\t", -1);
                    Assert.assertEquals(line, 6, f.length);
                    Target target = Target.valueOf(f[2]);
                    Assert.assertEquals(line, target.wireName, f[3]);
                    Assert.assertTrue(line, "TRUE".equals(f[1]) || "FALSE".equals(f[1]));
                    result.add(new BoolVector(f[0], "TRUE".equals(f[1]), target, f[4], f[5]));
                }
            }
        }
        Assert.assertEquals(18, result.size());
        for (Target target : Target.values()) Assert.assertEquals(target.name(), 2, select(result, target).size());
        return result;
    }

    private static List<StringVector> readStringVectors() throws Exception {
        List<StringVector> result = new ArrayList<>();
        try (InputStream stream = QwpSchemaBooleanE2ETest.class.getResourceAsStream(STRING_VECTORS)) {
            Assert.assertNotNull(STRING_VECTORS, stream);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.isEmpty() || line.charAt(0) == '#') continue;
                    String[] f = line.split("\\t", -1);
                    Assert.assertEquals(line, 4, f.length);
                    boolean invalid = "<INVALID>".equals(f[2]);
                    Assert.assertEquals(line, invalid, "<INVALID>".equals(f[3]));
                    String input = "<NULL>".equals(f[1]) ? null : utf16(f[1]);
                    boolean wireNull = "<NULL>".equals(f[2]);
                    boolean value = !wireNull && "1".equals(f[2]);
                    if (!invalid && !wireNull) Assert.assertTrue(line, "1".equals(f[2]) || "0".equals(f[2]));
                    if (!invalid) Assert.assertEquals(line, value ? "true" : "false", f[3]);
                    result.add(new StringVector(f[0], input, invalid, wireNull, value));
                }
            }
        }
        Assert.assertEquals("shared string corpus row count", 80, result.size());
        return result;
    }

    private static List<BoolVector> select(List<BoolVector> all, Target target) {
        List<BoolVector> result = new ArrayList<>();
        for (BoolVector vector : all) if (vector.target == target) result.add(vector);
        return result;
    }

    private static String utf16(String hex) {
        Assert.assertEquals(hex, 0, hex.length() & 3);
        char[] chars = new char[hex.length() / 4];
        for (int i = 0; i < chars.length; i++) chars[i] = (char) Integer.parseInt(hex.substring(i * 4, i * 4 + 4), 16);
        return new String(chars);
    }


    private enum Target {
        BOOLEAN("boolean", ColumnType.BOOLEAN, QwpConstants.TYPE_BOOLEAN, "BOOLEAN"),
        BYTE("byte", ColumnType.BYTE, QwpConstants.TYPE_BYTE, "BYTE"),
        SHORT("short", ColumnType.SHORT, QwpConstants.TYPE_SHORT, "SHORT"),
        INT("int", ColumnType.INT, QwpConstants.TYPE_INT, "INT"),
        LONG("long", ColumnType.LONG, QwpConstants.TYPE_LONG, "LONG"),
        FLOAT("float", ColumnType.FLOAT, QwpConstants.TYPE_FLOAT, "FLOAT"),
        DOUBLE("double", ColumnType.DOUBLE, QwpConstants.TYPE_DOUBLE, "DOUBLE"),
        STRING("string", ColumnType.STRING, QwpConstants.TYPE_VARCHAR, "VARCHAR"),
        VARCHAR("varchar", ColumnType.VARCHAR, QwpConstants.TYPE_VARCHAR, "VARCHAR");

        private final String sqlType;
        private final String suffix;
        private final byte wireType;
        private final String wireName;

        Target(String suffix, int sqlType, byte wireType, String wireName) {
            this.suffix = suffix;
            this.sqlType = ColumnType.nameOf(sqlType);
            this.wireType = wireType;
            this.wireName = wireName;
        }

        private void assertRecord(BoolVector v, Record r) {
            switch (this) {
                case BOOLEAN -> Assert.assertEquals(v.caseId, Boolean.parseBoolean(v.expectedSql), r.getBool(1));
                case BYTE -> Assert.assertEquals(v.caseId, Byte.parseByte(v.expectedSql), r.getByte(1));
                case SHORT -> Assert.assertEquals(v.caseId, Short.parseShort(v.expectedSql), r.getShort(1));
                case INT -> Assert.assertEquals(v.caseId, Integer.parseInt(v.expectedSql), r.getInt(1));
                case LONG -> Assert.assertEquals(v.caseId, Long.parseLong(v.expectedSql), r.getLong(1));
                case FLOAT -> Assert.assertEquals(v.caseId, Float.floatToRawIntBits(Float.parseFloat(v.expectedSql)), Float.floatToRawIntBits(r.getFloat(1)));
                case DOUBLE -> Assert.assertEquals(v.caseId, Double.doubleToRawLongBits(Double.parseDouble(v.expectedSql)), Double.doubleToRawLongBits(r.getDouble(1)));
                case STRING -> Assert.assertEquals(v.caseId, utf16(v.expectedSql), r.getStrA(1).toString());
                case VARCHAR -> Assert.assertEquals(v.caseId, utf16(v.expectedSql), r.getVarcharA(1).toString());
            }
        }

        private void assertWire(BoolVector v, QwpTableBlockCursor t) {
            switch (this) {
                case BOOLEAN -> Assert.assertEquals(v.caseId, "1".equals(v.expectedWire), t.getBooleanColumn(1).getValue());
                case BYTE, SHORT, INT, LONG -> Assert.assertEquals(v.caseId, Long.parseLong(v.expectedWire), t.getFixedWidthColumn(1).getLong());
                case FLOAT -> Assert.assertEquals(v.caseId, Float.floatToRawIntBits(Float.parseFloat(v.expectedWire)), Float.floatToRawIntBits((float) t.getFixedWidthColumn(1).getDouble()));
                case DOUBLE -> Assert.assertEquals(v.caseId, Double.doubleToRawLongBits(Double.parseDouble(v.expectedWire)), Double.doubleToRawLongBits(t.getFixedWidthColumn(1).getDouble()));
                case STRING, VARCHAR -> {
                    Utf8Sequence value = t.getStringColumn(1).getUtf8Value();
                    assertUtf8(v.caseId, bytes(v.expectedWire), value);
                }
            }
        }
    }

    private record BoolVector(String caseId, boolean input, Target target, String expectedWire, String expectedSql) {
    }

    private record StringVector(String caseId, String input, boolean invalid, boolean wireNull, boolean value) {
    }

    private static byte[] bytes(String hex) {
        Assert.assertEquals(hex, 0, hex.length() & 1);
        byte[] result = new byte[hex.length() / 2];
        for (int i = 0; i < result.length; i++) result[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
        return result;
    }

    private static void assertUtf8(String caseId, byte[] expected, Utf8Sequence actual) {
        Assert.assertNotNull(caseId, actual);
        Assert.assertEquals(caseId, expected.length, actual.size());
        for (int i = 0; i < expected.length; i++) Assert.assertEquals(caseId + " byte " + i, expected[i], actual.byteAt(i));
    }
}
