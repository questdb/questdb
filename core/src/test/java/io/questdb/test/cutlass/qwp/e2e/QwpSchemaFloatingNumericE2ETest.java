/*+*****************************************************************************
 * Copyright (c) 2014-2019 Appsicle
 * Copyright (c) 2019-2026 QuestDB
 * Licensed under the Apache License, Version 2.0
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
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class QwpSchemaFloatingNumericE2ETest extends AbstractQwpWebSocketTest {
    private static final String VECTORS = "/io/questdb/client/cutlass/qwp/floating-to-numeric.tsv";

    @Test
    public void testFloatingCorpusUsesExactTargetWireAndStoredValues() throws Exception {
        List<Vector> vectors = readVectors();
        runInContext(port -> {
            for (Input input : Input.values()) {
                for (Target target : Target.values()) {
                    List<Vector> selected = select(vectors, input, target);
                    String schemaTable = "schema_" + input.suffix + '_' + target.suffix;
                    String legacyTable = "legacy_" + input.suffix + '_' + target.suffix;
                    createTable(schemaTable, target);
                    createTable(legacyTable, target);
                    try (WebSocketClient client = connect(port);
                         QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                         QwpTableBuffer table = new QwpTableBuffer(schemaTable)) {
                        QwpSchemaBinding binding = new QwpSchemaBinding(table, describe(client, 100 + input.ordinal() * 10 + target.ordinal(), schemaTable));
                        int row = 0;
                        for (Vector vector : selected) {
                            if (vector.invalid) {
                                binding.longColumn("case_id", row);
                                LineSenderSchemaException error = Assert.assertThrows(vector.caseId,
                                        LineSenderSchemaException.class, () -> vector.append(binding));
                                Assert.assertEquals(vector.caseId, LineSenderSchemaException.Reason.INVALID_VALUE, error.getReason());
                                table.cancelCurrentRow();
                                table.rollbackUncommittedColumns();
                            } else {
                                binding.longColumn("case_id", row++);
                                vector.append(binding);
                                table.nextRow();
                            }
                        }
                        int length = encoder.encodeSchema(table);
                        assertWire(encoder, length, target, selected);
                        client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                        assertOk(client);
                    }
                    sendLegacyAccepted(port, legacyTable, selected);
                    drainWalQueue();
                    assertStored(schemaTable, target, selected);
                    assertStored(legacyTable, target, selected);
                }
            }
        });
    }

    @Test
    public void testFailureRollbackAndDuplicateFirstWins() throws Exception {
        runInContext(port -> {
            execute("create table schema_float_rows (v byte, only_b long, ts timestamp) timestamp(ts) partition by day wal");
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = new QwpTableBuffer("schema_float_rows")) {
                QwpSchemaBinding binding = new QwpSchemaBinding(table, describe(client, 300, "schema_float_rows"));
                binding.floatColumn("v", 10);
                table.nextRow();
                binding.longColumn("only_b", 7);
                Assert.assertThrows(LineSenderSchemaException.class, () -> binding.doubleColumn("v", 1.5));
                table.cancelCurrentRow();
                table.rollbackUncommittedColumns();
                binding.doubleColumn("v", 20).doubleColumn("v", 20.5).stringColumn("v", "invalid");
                table.nextRow();
                int length = encoder.encodeSchema(table);
                QwpTableBlockCursor wire = parseSingleTable(encoder, length);
                Assert.assertEquals(1, wire.getColumnCount());
                Assert.assertEquals(QwpConstants.TYPE_BYTE, wire.getColumnDef(0).getTypeCode());
                Assert.assertEquals(2, wire.getRowCount());
                client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                assertOk(client);
            }
            drainWalQueue();
            assertQuery("select v, only_b from schema_float_rows")
                    .noLeakCheck().returnsOnce("v\tonly_b\n10\tnull\n20\tnull\n");
        });
    }

    @Test
    public void testLegacyRejectsInvalidFloatingCorpusExceptApprovedLongClamp() throws Exception {
        List<Vector> vectors = readVectors();
        runInContext(port -> {
            int tableId = 0;
            for (Vector vector : vectors) {
                if (!vector.invalid || vector.isApprovedLegacyLongClamp()) continue;
                String tableName = "legacy_invalid_float_" + tableId++;
                createTable(tableName, vector.target);
                try (WebSocketClient client = WebSocketClientFactory.newPlainTextInstance();
                     QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                     QwpTableBuffer table = new QwpTableBuffer(tableName)) {
                    client.connect("127.0.0.1", port);
                    client.upgrade("/write/v4", null);
                    vector.append(table);
                    table.nextRow();
                    int length = encoder.encode(table);
                    client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                    WebSocketResponse response = receiveResponse(client);
                    Assert.assertEquals(vector.caseId, WebSocketResponse.STATUS_SCHEMA_MISMATCH, response.getStatus());
                    String message = response.getErrorMessage();
                    Assert.assertTrue(vector.caseId + ": " + message,
                            message.contains("loses precision") || message.contains("out of range"));
                    Assert.assertTrue(vector.caseId + ": " + message, message.contains(vector.target.wireName));
                }
                assertQuery("select count() from " + tableName).noLeakCheck().returnsOnce("count\n0\n");
            }
            Assert.assertEquals(104, tableId);
        });
    }

    @Test
    public void testNaNPayloadsAreTargetMissingWithAndWithoutOmission() throws Exception {
        runInContext(port -> {
            long requestId = 301;
            for (Input input : Input.values()) {
                for (Target target : Target.values()) {
                    for (boolean withOmission : new boolean[]{false, true}) {
                        String tableName = "schema_nan_" + input.suffix + '_' + target.suffix + (withOmission ? "_omitted" : "_plain");
                        createTable(tableName, target);
                        try (WebSocketClient client = connect(port);
                             QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                             QwpTableBuffer table = new QwpTableBuffer(tableName)) {
                            QwpSchemaBinding binding = new QwpSchemaBinding(table, describe(client, requestId++, tableName));
                            if (input == Input.FLOAT) binding.floatColumn("value", Float.intBitsToFloat(0xffc12345));
                            else binding.doubleColumn("value", Double.longBitsToDouble(0xfff8000000000042L));
                            table.nextRow();
                            if (withOmission) table.nextRow();
                            int length = encoder.encodeSchema(table);
                            QwpTableBlockCursor wire = parseSingleTable(encoder, length);
                            Assert.assertEquals(1, wire.getColumnCount());
                            Assert.assertEquals("value", wire.getColumnDef(0).getName());
                            Assert.assertEquals(target.wireType, wire.getColumnDef(0).getTypeCode());
                            QwpFixedWidthColumnCursor value = wire.getFixedWidthColumn(0);
                            Assert.assertTrue(value.getNullBitmapAddress() != 0);
                            Assert.assertEquals(0, value.getValueCount());
                            int expectedRows = withOmission ? 2 : 1;
                            for (int row = 0; row < expectedRows; row++) {
                                Assert.assertTrue(wire.hasNextRow());
                                wire.nextRow();
                                Assert.assertTrue(wire.isColumnNull(0));
                            }
                            Assert.assertFalse(wire.hasNextRow());
                            client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                            assertOk(client);
                        }
                    }
                }
            }
            drainWalQueue();
            for (Input input : Input.values()) {
                for (Target target : Target.values()) {
                    for (boolean withOmission : new boolean[]{false, true}) {
                        String tableName = "schema_nan_" + input.suffix + '_' + target.suffix + (withOmission ? "_omitted" : "_plain");
                        long rows = withOmission ? 2 : 1;
                        long nonNull = target == Target.BYTE || target == Target.SHORT ? rows : 0;
                        assertQuery("select count() rows, count(value) non_null from " + tableName)
                                .noLeakCheck().returnsOnce("rows\tnon_null\n" + rows + '\t' + nonNull + "\n");
                        if (target == Target.BYTE || target == Target.SHORT) {
                            StringBuilder expected = new StringBuilder("value\n");
                            for (int row = 0; row < rows; row++) expected.append("0\n");
                            assertQuery("select value from " + tableName).noLeakCheck().returnsOnce(expected.toString());
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testConvertedFloatingValuesReplayFromStoreAndForward() throws Exception {
        runInContext(port -> {
            execute("create table schema_float_replay (i int, f float, d double, ts timestamp) timestamp(ts) partition by day wal");
            File sfRoot = temp.newFolder("qwp-schema-float-sf");
            String slot = new File(sfRoot, "default").getAbsolutePath();
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = new QwpTableBuffer("schema_float_replay")) {
                QwpSchemaBinding binding = new QwpSchemaBinding(table, describe(client, 302, "schema_float_replay"));
                binding.doubleColumn("i", 42).doubleColumn("f", 0.1).floatColumn("d", -0.0f);
                table.nextRow();
                int length = encoder.encodeSchema(table);
                QwpTableBlockCursor wire = parseSingleTable(encoder, length);
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
            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";sf_dir=" + sfRoot.getAbsolutePath()
                    + ";sender_id=default;sf_max_segment_bytes=1m;")) {
                Assert.assertTrue(sender.drain(10_000));
            }
            drainWalQueue();
            try (RecordCursorFactory factory = select("select i, f, d from schema_float_replay");
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(42, record.getInt(0));
                Assert.assertEquals(Float.floatToRawIntBits((float) 0.1), Float.floatToRawIntBits(record.getFloat(1)));
                Assert.assertEquals(Double.doubleToRawLongBits(-0.0), Double.doubleToRawLongBits(record.getDouble(2)));
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
                    target.assertStored(vector, record);
                }
            }
            Assert.assertFalse(cursor.hasNext());
        }
    }

    private static void assertWire(QwpWebSocketEncoder encoder, int length, Target target, List<Vector> vectors) throws Exception {
        QwpTableBlockCursor table = parseSingleTable(encoder, length);
        Assert.assertEquals(target.wireType, table.getColumnDef(1).getTypeCode());
        QwpFixedWidthColumnCursor valueColumn = table.getFixedWidthColumn(1);
        int expectedValueCount = 0;
        boolean hasNull = false;
        for (Vector vector : vectors) {
            if (!vector.invalid) {
                hasNull |= vector.wireNull;
                if (!vector.wireNull) expectedValueCount++;
            }
        }
        Assert.assertEquals(hasNull, valueColumn.getNullBitmapAddress() != 0);
        Assert.assertEquals(expectedValueCount, valueColumn.getValueCount());
        int rows = 0;
        for (Vector vector : vectors) {
            if (!vector.invalid) {
                Assert.assertTrue(vector.caseId, table.hasNextRow());
                table.nextRow();
                Assert.assertEquals(vector.caseId, vector.wireNull, table.isColumnNull(1));
                if (!vector.wireNull) target.assertWire(vector, table);
                rows++;
            }
        }
        Assert.assertEquals(rows, table.getRowCount());
        Assert.assertFalse(table.hasNextRow());
    }

    private static void sendLegacyAccepted(int port, String table, List<Vector> vectors) throws Exception {
        try (WebSocketClient client = WebSocketClientFactory.newPlainTextInstance();
             QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
             QwpTableBuffer buffer = new QwpTableBuffer(table)) {
            client.connect("127.0.0.1", port);
            client.upgrade("/write/v4", null);
            Assert.assertFalse(client.isQwpSchemaEnabled());
            int row = 0;
            for (Vector vector : vectors) {
                if (!vector.invalid) {
                    buffer.getOrCreateColumn("case_id", QwpConstants.TYPE_LONG, false).addLong(row++);
                    vector.append(buffer);
                    buffer.nextRow();
                }
            }
            int length = encoder.encode(buffer);
            Assert.assertEquals(0, Unsafe.getByte(encoder.getBuffer().getBufferPtr()
                    + QwpConstants.HEADER_OFFSET_FLAGS) & QwpConstants.FLAG_SCHEMA);
            QwpTableBlockCursor wire = parseSingleTable(encoder, length);
            Assert.assertEquals(2, wire.getColumnCount());
            Assert.assertEquals("case_id", wire.getColumnDef(0).getName());
            Assert.assertEquals(QwpConstants.TYPE_LONG, wire.getColumnDef(0).getTypeCode());
            Assert.assertEquals("value", wire.getColumnDef(1).getName());
            Input input = vectors.get(0).input;
            Assert.assertEquals(input == Input.FLOAT ? QwpConstants.TYPE_FLOAT : QwpConstants.TYPE_DOUBLE,
                    wire.getColumnDef(1).getTypeCode());
            QwpFixedWidthColumnCursor value = wire.getFixedWidthColumn(1);
            Assert.assertEquals(0, value.getNullBitmapAddress());
            Assert.assertEquals(row, value.getValueCount());
            Assert.assertEquals(input == Input.FLOAT ? Float.BYTES : Double.BYTES, value.getValueSize());
            int wireRow = 0;
            for (Vector vector : vectors) {
                if (!vector.invalid) {
                    Assert.assertTrue(vector.caseId, wire.hasNextRow());
                    wire.nextRow();
                    Assert.assertEquals(vector.caseId, wireRow, wire.getFixedWidthColumn(0).getLong());
                    if (input == Input.FLOAT) {
                        Assert.assertEquals(vector.caseId, (int) vector.inputBits,
                                Unsafe.getInt(value.getValuesAddress() + (long) wireRow * Float.BYTES));
                    } else {
                        Assert.assertEquals(vector.caseId, vector.inputBits,
                                Unsafe.getLong(value.getValuesAddress() + (long) wireRow * Double.BYTES));
                    }
                    wireRow++;
                }
            }
            Assert.assertEquals(wireRow, wire.getRowCount());
            Assert.assertFalse(wire.hasNextRow());
            client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
            assertOk(client);
        }
    }

    private void createTable(String table, Target target) throws Exception {
        execute("create table " + table + " (case_id long, value " + target.sqlType + ", ts timestamp) timestamp(ts) partition by day wal");
    }

    private static List<Vector> readVectors() throws Exception {
        InputStream stream = QwpSchemaFloatingNumericE2ETest.class.getResourceAsStream(VECTORS);
        Assert.assertNotNull(VECTORS, stream);
        List<Vector> result = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty() || line.charAt(0) == '#') continue;
                String[] f = line.split("\\t", -1);
                Assert.assertEquals(line, 7, f.length);
                Input input = Input.valueOf(f[1]);
                Target target = Target.valueOf(f[3]);
                Assert.assertEquals(line, target.wireName, f[4]);
                boolean invalid = "<INVALID>".equals(f[5]);
                Assert.assertEquals(line, invalid, "<INVALID>".equals(f[6]));
                result.add(new Vector(f[0], input, f[2], target, f[5], f[6], invalid));
            }
        }
        Assert.assertEquals(350, result.size());
        for (Input input : Input.values()) for (Target target : Target.values()) Assert.assertFalse(select(result, input, target).isEmpty());
        return result;
    }

    private static List<Vector> select(List<Vector> all, Input input, Target target) {
        List<Vector> result = new ArrayList<>();
        for (Vector vector : all) if (vector.input == input && vector.target == target) result.add(vector);
        return result;
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

    private static void assertOk(WebSocketClient client) {
        AtomicReference<WebSocketResponse> result = new AtomicReference<>();
        Assert.assertTrue(client.receiveFrame(new WebSocketFrameHandler() {
            @Override
            public void onBinaryMessage(long ptr, int len) {
                WebSocketResponse response = new WebSocketResponse();
                Assert.assertTrue(response.readFrom(ptr, len, true));
                result.set(response);
            }

            @Override
            public void onClose(int code, String reason) {
                Assert.fail("unexpected close [code=" + code + ", reason=" + reason + ']');
            }
        }, 5_000));
        Assert.assertTrue(result.get().getErrorMessage(), result.get().isSuccess());
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

    private static QwpTableBlockCursor parseSingleTable(QwpWebSocketEncoder encoder, int length) throws Exception {
        QwpMessageCursor cursor = new QwpMessageCursor();
        cursor.of(encoder.getBuffer().getBufferPtr(), length, new ObjList<>());
        Assert.assertTrue(cursor.hasNextTable());
        QwpTableBlockCursor table = cursor.nextTable();
        Assert.assertFalse(cursor.hasNextTable());
        return table;
    }

    private enum Input {
        FLOAT("float"), DOUBLE("double");
        private final String suffix;
        Input(String suffix) { this.suffix = suffix; }
    }

    private enum Target {
        BYTE("byte", ColumnType.BYTE, QwpConstants.TYPE_BYTE, "BYTE"),
        SHORT("short", ColumnType.SHORT, QwpConstants.TYPE_SHORT, "SHORT"),
        INT("int", ColumnType.INT, QwpConstants.TYPE_INT, "INT"),
        LONG("long", ColumnType.LONG, QwpConstants.TYPE_LONG, "LONG"),
        FLOAT("float", ColumnType.FLOAT, QwpConstants.TYPE_FLOAT, "FLOAT"),
        DOUBLE("double", ColumnType.DOUBLE, QwpConstants.TYPE_DOUBLE, "DOUBLE");
        private final String sqlType;
        private final String suffix;
        private final byte wireType;
        private final String wireName;
        Target(String suffix, int type, byte wireType, String wireName) {
            this.suffix = suffix;
            this.sqlType = ColumnType.nameOf(type);
            this.wireType = wireType;
            this.wireName = wireName;
        }

        private void assertWire(Vector v, QwpTableBlockCursor table) {
            long bits = Long.parseUnsignedLong(v.expectedWire, 16);
            switch (this) {
                case BYTE -> Assert.assertEquals(v.caseId, (byte) bits, table.getFixedWidthColumn(1).getLong());
                case SHORT -> Assert.assertEquals(v.caseId, (short) bits, table.getFixedWidthColumn(1).getLong());
                case INT -> Assert.assertEquals(v.caseId, (int) bits, table.getFixedWidthColumn(1).getLong());
                case LONG -> Assert.assertEquals(v.caseId, bits, table.getFixedWidthColumn(1).getLong());
                case FLOAT -> Assert.assertEquals(v.caseId, (int) bits, Float.floatToRawIntBits((float) table.getFixedWidthColumn(1).getDouble()));
                case DOUBLE -> Assert.assertEquals(v.caseId, bits, Double.doubleToRawLongBits(table.getFixedWidthColumn(1).getDouble()));
            }
        }

        private void assertStored(Vector v, Record record) {
            if ("NULL".equals(v.expectedSql)) {
                switch (this) {
                    case INT -> Assert.assertEquals(v.caseId, Integer.MIN_VALUE, record.getInt(1));
                    case LONG -> Assert.assertEquals(v.caseId, Long.MIN_VALUE, record.getLong(1));
                    case FLOAT -> Assert.assertTrue(v.caseId, Float.isNaN(record.getFloat(1)));
                    case DOUBLE -> Assert.assertTrue(v.caseId, Double.isNaN(record.getDouble(1)));
                    default -> throw new AssertionError(v.caseId + " unexpected NULL target " + this);
                }
                return;
            }
            switch (this) {
                case BYTE -> Assert.assertEquals(v.caseId, Byte.parseByte(v.expectedSql), record.getByte(1));
                case SHORT -> Assert.assertEquals(v.caseId, Short.parseShort(v.expectedSql), record.getShort(1));
                case INT -> Assert.assertEquals(v.caseId, Integer.parseInt(v.expectedSql), record.getInt(1));
                case LONG -> Assert.assertEquals(v.caseId, Long.parseLong(v.expectedSql), record.getLong(1));
                case FLOAT -> Assert.assertEquals(v.caseId, (int) Long.parseUnsignedLong(v.expectedSql.substring(5), 16), Float.floatToRawIntBits(record.getFloat(1)));
                case DOUBLE -> Assert.assertEquals(v.caseId, Long.parseUnsignedLong(v.expectedSql.substring(5), 16), Double.doubleToRawLongBits(record.getDouble(1)));
            }
        }
    }

    private static final class Vector {
        private final String caseId;
        private final String expectedSql;
        private final String expectedWire;
        private final Input input;
        private final long inputBits;
        private final boolean invalid;
        private final Target target;
        private final boolean wireNull;

        private Vector(String caseId, Input input, String inputBits, Target target, String expectedWire, String expectedSql, boolean invalid) {
            this.caseId = caseId;
            this.input = input;
            this.inputBits = Long.parseUnsignedLong(inputBits, 16);
            this.target = target;
            this.expectedWire = expectedWire;
            this.expectedSql = expectedSql;
            this.invalid = invalid;
            this.wireNull = "<NULL>".equals(expectedWire);
        }

        private void append(QwpSchemaBinding binding) {
            if (input == Input.FLOAT) binding.floatColumn("value", Float.intBitsToFloat((int) inputBits));
            else binding.doubleColumn("value", Double.longBitsToDouble(inputBits));
        }

        private void append(QwpTableBuffer table) {
            byte type = input == Input.FLOAT
                    ? io.questdb.client.cutlass.qwp.protocol.QwpConstants.TYPE_FLOAT
                    : io.questdb.client.cutlass.qwp.protocol.QwpConstants.TYPE_DOUBLE;
            var column = table.getOrCreateColumn("value", type, false);
            if (input == Input.FLOAT) column.addFloat(Float.intBitsToFloat((int) inputBits));
            else column.addDouble(Double.longBitsToDouble(inputBits));
        }

        private boolean isApprovedLegacyLongClamp() {
            if (target != Target.LONG) return false;
            double value = input == Input.FLOAT ? Float.intBitsToFloat((int) inputBits) : Double.longBitsToDouble(inputBits);
            return value == 0x1.0p63;
        }
    }
}
