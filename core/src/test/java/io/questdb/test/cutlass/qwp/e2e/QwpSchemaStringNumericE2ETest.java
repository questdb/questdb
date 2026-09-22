/*+*****************************************************************************
 * Copyright (c) 2014-2019 Appsicle
 * Copyright (c) 2019-2026 QuestDB
 * Licensed under the Apache License, Version 2.0
 *******************************************************************************/
package io.questdb.test.cutlass.qwp.e2e;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.client.LineSenderSchemaException;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketClientFactory;
import io.questdb.client.cutlass.http.client.WebSocketFrameHandler;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaBinding;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaProtocol;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaResponse;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpFixedWidthColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpMessageCursor;
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
import java.util.Locale;
import java.util.concurrent.atomic.AtomicReference;

public class QwpSchemaStringNumericE2ETest extends AbstractQwpWebSocketTest {
    private static final String CORPUS = "/io/questdb/client/cutlass/qwp/string-to-numeric.tsv";

    @Test
    public void testSchemaStringNumericCorpusUsesExactTargetWireAndStoredValues() throws Exception {
        List<Vector> vectors = readVectors();
        runInContext(port -> {
            long requestId = 1;
            for (Target target : Target.values()) {
                List<Vector> selected = select(vectors, target);
                String tableName = "schema_string_" + target.name().toLowerCase(Locale.ROOT);
                String legacyTable = "legacy_string_" + target.name().toLowerCase(Locale.ROOT);
                execute("create table " + tableName + " (case_id long, value " + target.sqlType + ", ts timestamp) timestamp(ts) partition by day wal");
                execute("create table " + legacyTable + " (case_id long, value " + target.sqlType + ", ts timestamp) timestamp(ts) partition by day wal");
                try (WebSocketClient client = connectSchema(port);
                     QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                     QwpTableBuffer table = new QwpTableBuffer(tableName)) {
                    QwpSchemaResponse schema = describe(client, requestId++, tableName);
                    QwpSchemaBinding binding = new QwpSchemaBinding(table, schema);
                    int row = 0;
                    for (Vector vector : selected) {
                        binding.longColumn("case_id", row);
                        if (vector.invalid) {
                            LineSenderSchemaException error = Assert.assertThrows(vector.caseId, LineSenderSchemaException.class,
                                    () -> binding.stringColumn("value", vector.input));
                            Assert.assertEquals(vector.caseId, LineSenderSchemaException.Reason.INVALID_VALUE, error.getReason());
                            table.cancelCurrentRow();
                            table.rollbackUncommittedColumns();
                        } else {
                            binding.stringColumn("value", vector.input);
                            table.nextRow();
                            row++;
                        }
                    }
                    int length = encoder.encodeSchema(table);
                    assertWire(encoder, length, target, selected, schema);
                    client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                    Assert.assertTrue(receiveResponse(client).isSuccess());
                }
                sendLegacyAccepted(port, legacyTable, selected);
                drainWalQueue();
                assertStored(tableName, target, selected);
                assertStored(legacyTable, target, selected);
            }
        });
    }

    @Test
    public void testLegacyInvalidStringNumericCorpusIsRejectedWithoutRows() throws Exception {
        List<Vector> vectors = readVectors();
        runInContext(port -> {
            int rejected = 0;
            for (Vector vector : vectors) {
                if (!vector.invalid || vector.isAssertedLegacyWrap()) continue;
                String tableName = "legacy_string_invalid_" + rejected++;
                execute("create table " + tableName + " (value " + vector.target.sqlType + ", ts timestamp) timestamp(ts) partition by day wal");
                WebSocketResponse response;
                try (WebSocketClient client = WebSocketClientFactory.newPlainTextInstance();
                     QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                     QwpTableBuffer table = new QwpTableBuffer(tableName)) {
                    client.connect("127.0.0.1", port);
                    client.upgrade("/write/v4", null);
                    table.getOrCreateColumn("value", io.questdb.client.cutlass.qwp.protocol.QwpConstants.TYPE_VARCHAR, true).addString(vector.input);
                    table.nextRow();
                    int length = encoder.encode(table);
                    client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                    response = receiveResponse(client);
                }
                boolean logUtf8Failure = vector.input != null && vector.input.indexOf('\u00a0') >= 0;
                Assert.assertEquals(vector.caseId, logUtf8Failure ? WebSocketResponse.STATUS_INTERNAL_ERROR : WebSocketResponse.STATUS_SCHEMA_MISMATCH, response.getStatus());
                if (logUtf8Failure) {
                    Assert.assertTrue(vector.caseId + ": " + response.getErrorMessage(), response.getErrorMessage().contains("Invalid UTF-8"));
                } else {
                    Assert.assertTrue(vector.caseId + ": " + response.getErrorMessage(),
                            response.getErrorMessage().contains("cannot parse") || response.getErrorMessage().contains("out of range"));
                    Assert.assertTrue(vector.caseId + ": " + response.getErrorMessage(), response.getErrorMessage().contains("column=value"));
                }
                assertQuery("select count() from " + tableName).noLeakCheck().expectSize().noRandomAccess().returns("count\n0\n");
            }
            Assert.assertEquals(236, rejected);
        });
    }

    @Test
    public void testLegacyLongParserWrapCharacterization() throws Exception {
        String[] inputs = {"21000000000000000000", "-21000000000000000000", "25000000000000000000", "42000000000000000000", "63000000000000000000"};
        long[] expected = {2553255926290448384L, -2553255926290448384L, 6553255926290448384L, 5106511852580896768L, 7659767778871345152L};
        runInContext(port -> {
            for (int i = 0; i < inputs.length; i++) {
                String tableName = "legacy_long_wrap_" + i;
                execute("create table " + tableName + " (value long, ts timestamp) timestamp(ts) partition by day wal");
                try (WebSocketClient client = WebSocketClientFactory.newPlainTextInstance();
                     QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                     QwpTableBuffer table = new QwpTableBuffer(tableName)) {
                    client.connect("127.0.0.1", port);
                    client.upgrade("/write/v4", null);
                    table.getOrCreateColumn("value", io.questdb.client.cutlass.qwp.protocol.QwpConstants.TYPE_VARCHAR, true).addString(inputs[i]);
                    table.nextRow();
                    int length = encoder.encode(table);
                    client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                    Assert.assertTrue(inputs[i], receiveResponse(client).isSuccess());
                }
                drainWalQueue();
                assertQuery("select value from " + tableName).noLeakCheck().expectSize().returns("value\n" + expected[i] + "\n");
            }
        });
    }

    @Test
    public void testFailureRollbackAndDuplicateFirstWins() throws Exception {
        runInContext(port -> {
            execute("create table schema_string_rows (value byte, only_b long, ts timestamp) timestamp(ts) partition by day wal");
            try (WebSocketClient client = connectSchema(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = new QwpTableBuffer("schema_string_rows")) {
                QwpSchemaBinding binding = new QwpSchemaBinding(table, describe(client, 900, "schema_string_rows"));
                binding.stringColumn("value", "10");
                table.nextRow();
                binding.longColumn("only_b", 7);
                Assert.assertThrows(LineSenderSchemaException.class, () -> binding.stringColumn("value", "1.5"));
                table.cancelCurrentRow();
                table.rollbackUncommittedColumns();
                binding.stringColumn("value", "20").stringColumn("value", "invalid").doubleColumn("value", 20.5);
                table.nextRow();
                int length = encoder.encodeSchema(table);
                QwpMessageCursor message = new QwpMessageCursor();
                message.of(encoder.getBuffer().getBufferPtr(), length, new ObjList<>());
                QwpTableBlockCursor wire = message.nextTable();
                Assert.assertEquals(1, wire.getColumnCount());
                Assert.assertEquals("value", wire.getColumnDef(0).getName());
                Assert.assertEquals(QwpConstants.TYPE_BYTE, wire.getColumnDef(0).getTypeCode());
                client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                Assert.assertTrue(receiveResponse(client).isSuccess());
            }
            drainWalQueue();
            assertQuery("select value, only_b from schema_string_rows").noLeakCheck()
                    .expectSize().returns("value\tonly_b\n10\tnull\n20\tnull\n");
        });
    }

    @Test
    public void testParsedSentinelsAndExplicitNullAreStableAcrossOmissionBitmap() throws Exception {
        runInContext(port -> {
            long requestId = 950;
            for (Target target : Target.values()) {
                for (boolean withOmission : new boolean[]{false, true}) {
                    String tableName = "schema_string_null_" + target.name().toLowerCase(Locale.ROOT) + (withOmission ? "_bitmap" : "_plain");
                    execute("create table " + tableName + " (value " + target.sqlType + ", ts timestamp) timestamp(ts) partition by day wal");
                    try (WebSocketClient client = connectSchema(port);
                         QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                         QwpTableBuffer table = new QwpTableBuffer(tableName)) {
                        QwpSchemaBinding binding = new QwpSchemaBinding(table, describe(client, requestId++, tableName));
                        String text = switch (target) {
                            case BYTE, SHORT -> null;
                            case INT -> "-2147483648";
                            case LONG -> "-9223372036854775808";
                            case FLOAT, DOUBLE -> "NaN";
                        };
                        binding.stringColumn("value", text);
                        table.nextRow();
                        if (withOmission) table.nextRow();
                        int length = encoder.encodeSchema(table);
                        QwpMessageCursor message = new QwpMessageCursor();
                        message.of(encoder.getBuffer().getBufferPtr(), length, new ObjList<>());
                        QwpTableBlockCursor wire = message.nextTable();
                        QwpFixedWidthColumnCursor value = wire.getFixedWidthColumn(0);
                        boolean explicitNull = target == Target.BYTE || target == Target.SHORT;
                        Assert.assertEquals(explicitNull || withOmission, value.getNullBitmapAddress() != 0);
                        Assert.assertEquals(explicitNull ? 0 : 1, value.getValueCount());
                        Assert.assertTrue(wire.hasNextRow());
                        wire.nextRow();
                        Assert.assertEquals(explicitNull || !withOmission, wire.isColumnNull(0));
                        if (!explicitNull) target.assertSentinelBits(value);
                        if (withOmission) {
                            Assert.assertTrue(wire.hasNextRow());
                            wire.nextRow();
                            Assert.assertTrue(wire.isColumnNull(0));
                        }
                        client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                        Assert.assertTrue(receiveResponse(client).isSuccess());
                    }
                }
            }
            drainWalQueue();
            for (Target target : Target.values()) {
                for (boolean withOmission : new boolean[]{false, true}) {
                    String tableName = "schema_string_null_" + target.name().toLowerCase(Locale.ROOT) + (withOmission ? "_bitmap" : "_plain");
                    long rows = withOmission ? 2 : 1;
                    long present = target == Target.BYTE || target == Target.SHORT ? rows : 0;
                    assertQuery("select count() rows, count(value) present from " + tableName).noLeakCheck()
                            .expectSize().noRandomAccess().returns("rows\tpresent\n" + rows + '\t' + present + "\n");
                    if (target == Target.BYTE || target == Target.SHORT) {
                        assertQuery("select value, value=0 zero from " + tableName).noLeakCheck()
                                .expectSize().returns(withOmission ? "value\tzero\n0\ttrue\n0\ttrue\n" : "value\tzero\n0\ttrue\n");
                    }
                }
            }
        });
    }

    @Test
    public void testConvertedStringNumericValuesReplayFromStoreAndForward() throws Exception {
        runInContext(port -> {
            execute("create table schema_string_numeric_sf (i int, f float, d double, ts timestamp) timestamp(ts) partition by day wal");
            File sfRoot = temp.newFolder("qwp-schema-string-numeric-sf");
            String slot = new File(sfRoot, "default").getAbsolutePath();
            try (WebSocketClient client = connectSchema(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = new QwpTableBuffer("schema_string_numeric_sf")) {
                QwpSchemaBinding binding = new QwpSchemaBinding(table, describe(client, 901, "schema_string_numeric_sf"));
                binding.stringColumn("i", "42").stringColumn("f", "0.1d").stringColumn("d", "-0.0f");
                table.nextRow();
                int length = encoder.encodeSchema(table);
                QwpMessageCursor message = new QwpMessageCursor();
                message.of(encoder.getBuffer().getBufferPtr(), length, new ObjList<>());
                QwpTableBlockCursor wire = message.nextTable();
                Assert.assertEquals(QwpConstants.TYPE_INT, wire.getColumnDef(0).getTypeCode());
                Assert.assertEquals(QwpConstants.TYPE_FLOAT, wire.getColumnDef(1).getTypeCode());
                Assert.assertEquals(QwpConstants.TYPE_DOUBLE, wire.getColumnDef(2).getTypeCode());
                Assert.assertTrue(wire.hasNextRow());
                wire.nextRow();
                Assert.assertEquals(42, wire.getFixedWidthColumn(0).getLong());
                Assert.assertEquals(0x3dcccccd, Float.floatToRawIntBits((float) wire.getFixedWidthColumn(1).getDouble()));
                Assert.assertEquals(0x8000000000000000L, Double.doubleToRawLongBits(wire.getFixedWidthColumn(2).getDouble()));
                try (CursorSendEngine engine = new CursorSendEngine(slot, 1 << 20)) {
                    Assert.assertEquals(0, engine.appendBlocking(encoder.getBuffer().getBufferPtr(), length));
                }
            }
            try (io.questdb.client.Sender sender = io.questdb.client.Sender.fromConfig("ws::addr=localhost:" + port
                    + ";sf_dir=" + sfRoot.getAbsolutePath() + ";sender_id=default;sf_max_segment_bytes=1m;")) {
                Assert.assertTrue(sender.drain(10_000));
            }
            drainWalQueue();
            try (RecordCursorFactory factory = select("select i, f, d from schema_string_numeric_sf");
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(42, record.getInt(0));
                Assert.assertEquals(0x3dcccccd, Float.floatToRawIntBits(record.getFloat(1)));
                Assert.assertEquals(0x8000000000000000L, Double.doubleToRawLongBits(record.getDouble(2)));
                Assert.assertFalse(cursor.hasNext());
            }
        });
    }

    private void assertStored(String table, Target target, List<Vector> vectors) throws Exception {
        try (RecordCursorFactory factory = select("select case_id, value from " + table + " order by case_id");
             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Record record = cursor.getRecord();
            int row = 0;
            for (Vector vector : vectors) {
                if (!vector.invalid) {
                    Assert.assertTrue(vector.caseId, cursor.hasNext());
                    Assert.assertEquals(vector.caseId, row++, record.getLong(0));
                    target.assertStored(vector, record, 1);
                }
            }
            Assert.assertFalse(cursor.hasNext());
        }
    }

    private static void assertWire(QwpWebSocketEncoder encoder, int length, Target target, List<Vector> vectors, QwpSchemaResponse schema) throws Exception {
        QwpMessageCursor message = new QwpMessageCursor();
        message.of(encoder.getBuffer().getBufferPtr(), length, new ObjList<>());
        Assert.assertTrue(message.hasNextTable());
        QwpTableBlockCursor table = message.nextTable();
        Assert.assertFalse(message.hasNextTable());
        Assert.assertEquals(schema.getTableId(), table.getSchemaTableId());
        Assert.assertEquals(schema.getMetadataVersion(), table.getSchemaMetadataVersion());
        Assert.assertEquals(2, table.getColumnCount());
        Assert.assertEquals("value", table.getColumnDef(1).getName());
        Assert.assertEquals(target.wireType, table.getColumnDef(1).getTypeCode());
        QwpFixedWidthColumnCursor value = table.getFixedWidthColumn(1);
        int valueCount = 0;
        boolean hasNull = false;
        for (Vector vector : vectors)
            if (!vector.invalid) {
                hasNull |= vector.wireNull;
                if (!vector.wireNull) valueCount++;
            }
        Assert.assertEquals(hasNull, value.getNullBitmapAddress() != 0);
        Assert.assertEquals(valueCount, value.getValueCount());
        int rows = 0;
        for (Vector vector : vectors)
            if (!vector.invalid) {
                Assert.assertTrue(vector.caseId, table.hasNextRow());
                table.nextRow();
                Assert.assertEquals(vector.caseId, vector.wireNull, table.isColumnNull(1));
                if (!vector.wireNull) target.assertWire(vector, value);
                rows++;
            }
        Assert.assertEquals(rows, table.getRowCount());
        Assert.assertFalse(table.hasNextRow());
    }

    private static WebSocketClient connectSchema(int port) {
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
            if (!success) client.close();
        }
    }

    private static QwpSchemaResponse describe(WebSocketClient client, long requestId, String table) {
        byte[] request = QwpSchemaProtocol.encodeDescribe(requestId, table);
        long address = Unsafe.malloc(request.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < request.length; i++) Unsafe.putByte(address + i, request[i]);
            client.sendBinary(address, request.length);
        } finally {
            Unsafe.free(address, request.length, MemoryTag.NATIVE_DEFAULT);
        }
        AtomicReference<QwpSchemaResponse> result = new AtomicReference<>();
        Assert.assertTrue(client.receiveFrame(new WebSocketFrameHandler() {
            @Override
            public void onBinaryMessage(long ptr, int len) {
                result.set(QwpSchemaProtocol.decodeResponse(ptr, len));
            }

            @Override
            public void onClose(int code, String reason) {
                Assert.fail("unexpected close [code=" + code + ", reason=" + reason + ']');
            }
        }, 5_000));
        Assert.assertEquals(requestId, result.get().getRequestId());
        return result.get();
    }

    private static List<Vector> readVectors() throws Exception {
        List<Vector> result = new ArrayList<>();
        try (InputStream stream = QwpSchemaStringNumericE2ETest.class.getResourceAsStream(CORPUS)) {
            Assert.assertNotNull(CORPUS, stream);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.isEmpty() || line.charAt(0) == '#') continue;
                    String[] f = line.split("\\t", -1);
                    Assert.assertEquals(line, 7, f.length);
                    Target target = Target.valueOf(f[2]);
                    Assert.assertEquals(line, target.name(), f[3]);
                    boolean invalid = "INVALID".equals(f[6]);
                    Assert.assertTrue(line, invalid || "VALID".equals(f[6]));
                    Assert.assertEquals(line, invalid, "<INVALID>".equals(f[4]));
                    Assert.assertEquals(line, invalid, "<INVALID>".equals(f[5]));
                    result.add(new Vector(f[0], decodeUtf16(f[1]), target, f[4], f[5], invalid));
                }
            }
        }
        Assert.assertEquals(429, result.size());
        for (Target target : Target.values()) Assert.assertFalse(select(result, target).isEmpty());
        return result;
    }

    private static List<Vector> select(List<Vector> vectors, Target target) {
        List<Vector> result = new ArrayList<>();
        for (Vector vector : vectors) if (vector.target == target) result.add(vector);
        return result;
    }

    private static String decodeUtf16(String hex) {
        if ("<NULL>".equals(hex)) return null;
        Assert.assertEquals(hex, 0, hex.length() & 3);
        StringBuilder value = new StringBuilder(hex.length() / 4);
        for (int i = 0; i < hex.length(); i += 4) value.append((char) Integer.parseInt(hex.substring(i, i + 4), 16));
        return value.toString();
    }

    private static WebSocketResponse receiveResponse(WebSocketClient client) {
        AtomicReference<WebSocketResponse> result = new AtomicReference<>();
        Assert.assertTrue(client.receiveFrame(new WebSocketFrameHandler() {
            @Override
            public void onBinaryMessage(long ptr, int len) {
                WebSocketResponse response = new WebSocketResponse();
                Assert.assertTrue(response.readFrom(ptr, len, false));
                result.set(response);
            }

            @Override
            public void onClose(int code, String reason) {
                Assert.fail("unexpected close [code=" + code + ", reason=" + reason + ']');
            }
        }, 5_000));
        Assert.assertNotNull(result.get());
        return result.get();
    }

    private static void sendLegacyAccepted(int port, String tableName, List<Vector> vectors) throws Exception {
        try (WebSocketClient client = WebSocketClientFactory.newPlainTextInstance();
             QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
             QwpTableBuffer table = new QwpTableBuffer(tableName)) {
            client.connect("127.0.0.1", port);
            client.upgrade("/write/v4", null);
            Assert.assertFalse(client.isQwpSchemaEnabled());
            int row = 0;
            for (Vector vector : vectors) {
                if (!vector.invalid) {
                    table.getOrCreateColumn("case_id", QwpConstants.TYPE_LONG, false).addLong(row++);
                    table.getOrCreateColumn("value", QwpConstants.TYPE_VARCHAR, true).addString(vector.input);
                    table.nextRow();
                }
            }
            int length = encoder.encode(table);
            Assert.assertEquals(0, Unsafe.getByte(encoder.getBuffer().getBufferPtr()
                    + QwpConstants.HEADER_OFFSET_FLAGS) & QwpConstants.FLAG_SCHEMA);
            QwpMessageCursor cursor = new QwpMessageCursor();
            cursor.of(encoder.getBuffer().getBufferPtr(), length, new ObjList<>());
            Assert.assertTrue(cursor.hasNextTable());
            QwpTableBlockCursor wire = cursor.nextTable();
            Assert.assertFalse(cursor.hasNextTable());
            Assert.assertEquals(2, wire.getColumnCount());
            Assert.assertEquals("case_id", wire.getColumnDef(0).getName());
            Assert.assertEquals(QwpConstants.TYPE_LONG, wire.getColumnDef(0).getTypeCode());
            Assert.assertEquals("value", wire.getColumnDef(1).getName());
            Assert.assertEquals(QwpConstants.TYPE_VARCHAR, wire.getColumnDef(1).getTypeCode());
            int wireRow = 0;
            for (Vector vector : vectors) {
                if (!vector.invalid) {
                    Assert.assertTrue(vector.caseId, wire.hasNextRow());
                    wire.nextRow();
                    Assert.assertEquals(vector.caseId, wireRow++, wire.getFixedWidthColumn(0).getLong());
                    Assert.assertEquals(vector.caseId, vector.input == null, wire.isColumnNull(1));
                    if (vector.input != null) {
                        assertLegacyUtf8(vector.caseId, vector.input, wire.getStringColumn(1).getUtf8Value());
                    }
                }
            }
            Assert.assertEquals(wireRow, wire.getRowCount());
            Assert.assertFalse(wire.hasNextRow());
            client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
            Assert.assertTrue(receiveResponse(client).isSuccess());
        }
    }

    private static void assertLegacyUtf8(String caseId, String expected, Utf8Sequence actual) {
        byte[] bytes = expected.getBytes(StandardCharsets.UTF_8);
        Assert.assertNotNull(caseId, actual);
        Assert.assertEquals(caseId, bytes.length, actual.size());
        for (int i = 0; i < bytes.length; i++) {
            Assert.assertEquals(caseId + " byte " + i, bytes[i], actual.byteAt(i));
        }
    }

    private enum Target {
        BYTE(ColumnType.BYTE), SHORT(ColumnType.SHORT), INT(ColumnType.INT), LONG(ColumnType.LONG), FLOAT(ColumnType.FLOAT), DOUBLE(ColumnType.DOUBLE);
        private final String sqlType;
        private final byte wireType;

        Target(int type) {
            this.sqlType = ColumnType.nameOf(type);
            this.wireType = switch (type) {
                case ColumnType.BYTE -> QwpConstants.TYPE_BYTE;
                case ColumnType.SHORT -> QwpConstants.TYPE_SHORT;
                case ColumnType.INT -> QwpConstants.TYPE_INT;
                case ColumnType.LONG -> QwpConstants.TYPE_LONG;
                case ColumnType.FLOAT -> QwpConstants.TYPE_FLOAT;
                case ColumnType.DOUBLE -> QwpConstants.TYPE_DOUBLE;
                default -> throw new IllegalArgumentException();
            };
        }

        private void assertWire(Vector vector, QwpFixedWidthColumnCursor value) {
            long bits = Long.parseUnsignedLong(vector.expectedWire, 16);
            switch (this) {
                case BYTE -> Assert.assertEquals(vector.caseId, (byte) bits, value.getLong());
                case SHORT -> Assert.assertEquals(vector.caseId, (short) bits, value.getLong());
                case INT -> Assert.assertEquals(vector.caseId, (int) bits, value.getLong());
                case LONG -> Assert.assertEquals(vector.caseId, bits, value.getLong());
                case FLOAT ->
                        Assert.assertEquals(vector.caseId, (int) bits, Float.floatToRawIntBits((float) value.getDouble()));
                case DOUBLE -> Assert.assertEquals(vector.caseId, bits, Double.doubleToRawLongBits(value.getDouble()));
            }
        }

        private void assertStored(Vector vector, Record record, int column) {
            if ("NULL".equals(vector.expectedSql)) {
                switch (this) {
                    case INT -> Assert.assertEquals(vector.caseId, Integer.MIN_VALUE, record.getInt(column));
                    case LONG -> Assert.assertEquals(vector.caseId, Long.MIN_VALUE, record.getLong(column));
                    case FLOAT -> Assert.assertTrue(vector.caseId, Float.isNaN(record.getFloat(column)));
                    case DOUBLE -> Assert.assertTrue(vector.caseId, Double.isNaN(record.getDouble(column)));
                    default -> throw new AssertionError(vector.caseId);
                }
                return;
            }
            switch (this) {
                case BYTE ->
                        Assert.assertEquals(vector.caseId, Byte.parseByte(vector.expectedSql), record.getByte(column));
                case SHORT ->
                        Assert.assertEquals(vector.caseId, Short.parseShort(vector.expectedSql), record.getShort(column));
                case INT ->
                        Assert.assertEquals(vector.caseId, Integer.parseInt(vector.expectedSql), record.getInt(column));
                case LONG ->
                        Assert.assertEquals(vector.caseId, Long.parseLong(vector.expectedSql), record.getLong(column));
                case FLOAT ->
                        Assert.assertEquals(vector.caseId, (int) Long.parseUnsignedLong(vector.expectedSql.substring(5), 16), Float.floatToRawIntBits(record.getFloat(column)));
                case DOUBLE ->
                        Assert.assertEquals(vector.caseId, Long.parseUnsignedLong(vector.expectedSql.substring(5), 16), Double.doubleToRawLongBits(record.getDouble(column)));
            }
        }

        private void assertSentinelBits(QwpFixedWidthColumnCursor value) {
            switch (this) {
                case INT -> Assert.assertEquals(Integer.MIN_VALUE, value.getLong());
                case LONG -> Assert.assertEquals(Long.MIN_VALUE, value.getLong());
                case FLOAT -> Assert.assertEquals(0x7fc00000, Float.floatToRawIntBits((float) value.getDouble()));
                case DOUBLE -> Assert.assertEquals(0x7ff8000000000000L, Double.doubleToRawLongBits(value.getDouble()));
                default -> throw new AssertionError(this);
            }
        }
    }

    private static final class Vector {
        private final String caseId;
        private final String expectedSql;
        private final String expectedWire;
        private final String input;
        private final boolean invalid;
        private final Target target;
        private final boolean wireNull;

        private Vector(String caseId, String input, Target target, String expectedWire, String expectedSql, boolean invalid) {
            this.caseId = caseId;
            this.input = input;
            this.target = target;
            this.expectedWire = expectedWire;
            this.expectedSql = expectedSql;
            this.invalid = invalid;
            this.wireNull = "<NULL>".equals(expectedWire);
        }

        private boolean isAssertedLegacyWrap() {
            return target == Target.LONG && ("21000000000000000000".equals(input)
                    || "-21000000000000000000".equals(input)
                    || "25000000000000000000".equals(input)
                    || "42000000000000000000".equals(input)
                    || "63000000000000000000".equals(input));
        }
    }
}
