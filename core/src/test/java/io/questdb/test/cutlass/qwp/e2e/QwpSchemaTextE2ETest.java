/*+*****************************************************************************
 * Copyright (c) 2014-2019 Appsicle
 * Copyright (c) 2019-2026 QuestDB
 * Licensed under the Apache License, Version 2.0
 ******************************************************************************/
package io.questdb.test.cutlass.qwp.e2e;

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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpMessageCursor;
import io.questdb.cutlass.qwp.protocol.QwpStringColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpSymbolColumnCursor;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class QwpSchemaTextE2ETest extends AbstractQwpWebSocketTest {
    private static final String VECTORS = "/io/questdb/client/cutlass/qwp/text-to-text.tsv";

    @Test
    public void testNullOmissionRollbackAndCrossSetterDuplicateFirstWins() throws Exception {
        execute("create table schema_text_rows (s string, v varchar, y symbol, bad long, only_b long, "
                + "ts timestamp) timestamp(ts) partition by day wal");
        runInContext(port -> {
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = new QwpTableBuffer("schema_text_rows")) {
                QwpSchemaBinding binding = new QwpSchemaBinding(
                        table,
                        describe(client, 200, "schema_text_rows")
                );
                binding.stringColumn("s", "A-string").symbol("v", "A-varchar").stringColumn("y", "A-symbol");
                table.nextRow();
                binding.stringColumn("s", null).symbol("v", null).stringColumn("y", null);
                table.nextRow();
                table.nextRow();

                binding.longColumn("only_b", 99);
                LineSenderSchemaException error = Assert.assertThrows(
                        LineSenderSchemaException.class,
                        () -> binding.symbol("bad", "B")
                );
                Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, error.getReason());
                table.cancelCurrentRow();
                table.rollbackUncommittedColumns();

                binding.symbol("s", "C-string").stringColumn("s", "ignored")
                        .stringColumn("v", "C-varchar").symbol("v", "ignored")
                        .symbol("y", "C-symbol").stringColumn("y", "ignored");
                table.nextRow();

                int length = encoder.encodeSchema(table);
                QwpTableBlockCursor wire = parseSingleTable(encoder, length);
                Assert.assertEquals(4, wire.getRowCount());
                Assert.assertEquals("failed-row-only columns must be removed", 3, wire.getColumnCount());
                Assert.assertEquals(QwpConstants.TYPE_VARCHAR, wire.getColumnDef(0).getTypeCode());
                Assert.assertEquals(QwpConstants.TYPE_VARCHAR, wire.getColumnDef(1).getTypeCode());
                Assert.assertEquals(QwpConstants.TYPE_SYMBOL, wire.getColumnDef(2).getTypeCode());
                Assert.assertTrue(wire.hasNextRow());
                wire.nextRow();
                Assert.assertFalse(wire.isColumnNull(0));
                Assert.assertFalse(wire.isColumnNull(1));
                Assert.assertFalse(wire.isColumnNull(2));
                for (int row = 0; row < 2; row++) {
                    Assert.assertTrue(wire.hasNextRow());
                    wire.nextRow();
                    Assert.assertTrue(wire.isColumnNull(0));
                    Assert.assertTrue(wire.isColumnNull(1));
                    Assert.assertTrue(wire.isColumnNull(2));
                }
                Assert.assertTrue(wire.hasNextRow());
                wire.nextRow();
                Assert.assertFalse(wire.isColumnNull(0));
                Assert.assertFalse(wire.isColumnNull(1));
                Assert.assertFalse(wire.isColumnNull(2));
                Assert.assertFalse(wire.hasNextRow());
                client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                assertOk(client);
            }
            drainWalQueue();
            assertQuery("select s, v, y, s is null sn, v is null vn, y is null yn, bad, only_b "
                    + "from schema_text_rows")
                    .noLeakCheck().expectSize().returns("s\tv\ty\tsn\tvn\tyn\tbad\tonly_b\n"
                            + "A-string\tA-varchar\tA-symbol\tfalse\tfalse\tfalse\tnull\tnull\n"
                            + "\t\t\ttrue\ttrue\ttrue\tnull\tnull\n"
                            + "\t\t\ttrue\ttrue\ttrue\tnull\tnull\n"
                            + "C-string\tC-varchar\tC-symbol\tfalse\tfalse\tfalse\tnull\tnull\n");
        });
    }

    @Test
    public void testTargetTextAndSymbolReplayFromStoreAndForward() throws Exception {
        execute("create table schema_text_replay (text_value varchar, symbol_value symbol, ts timestamp) "
                + "timestamp(ts) partition by day wal");
        runInContext(port -> {
            File sfRoot = temp.newFolder("qwp-schema-text-sf");
            String slot = new File(sfRoot, "default").getAbsolutePath();
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = new QwpTableBuffer("schema_text_replay")) {
                QwpSchemaBinding binding = new QwpSchemaBinding(
                        table,
                        describe(client, 201, "schema_text_replay")
                );
                binding.stringColumn("text_value", "replay-\uD83D\uDE80")
                        .symbol("symbol_value", "symbol-\u20AC");
                table.nextRow();
                int length = encoder.encodeSchema(table);
                QwpTableBlockCursor wire = parseSingleTable(encoder, length);
                Assert.assertEquals(QwpConstants.TYPE_VARCHAR, wire.getColumnDef(0).getTypeCode());
                Assert.assertEquals(QwpConstants.TYPE_SYMBOL, wire.getColumnDef(1).getTypeCode());
                Assert.assertTrue(wire.hasNextRow());
                wire.nextRow();
                assertUtf8("replay text", "replay-\uD83D\uDE80".getBytes(StandardCharsets.UTF_8),
                        wire.getStringColumn(0).getUtf8Value());
                assertUtf8("replay symbol", "symbol-\u20AC".getBytes(StandardCharsets.UTF_8),
                        wire.getSymbolColumn(1).getSymbolUtf8());
                try (CursorSendEngine cursorEngine = new CursorSendEngine(slot, 1 << 20)) {
                    Assert.assertEquals(0, cursorEngine.appendBlocking(encoder.getBuffer().getBufferPtr(), length));
                }
            }
            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port
                    + ";sf_dir=" + sfRoot.getAbsolutePath()
                    + ";sender_id=default;sf_max_segment_bytes=1m;")) {
                Assert.assertTrue(sender.drain(10_000));
            }
            drainWalQueue();
            assertQuery("select text_value, symbol_value from schema_text_replay")
                    .noLeakCheck().expectSize().returns("text_value\tsymbol_value\nreplay-\uD83D\uDE80\tsymbol-\u20AC\n");
        });
    }

    @Test
    public void testTextCorpusUsesExactTargetWireAndStoredValues() throws Exception {
        List<Vector> vectors = readVectors();
        runInContext(port -> {
            for (Input input : Input.values()) {
                for (Target target : Target.values()) {
                    List<Vector> selected = select(vectors, input, target);
                    String tableName = "schema_text_" + input.suffix + '_' + target.suffix;
                    createTable(tableName, target);
                    try (WebSocketClient client = connect(port);
                         QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                         QwpTableBuffer table = new QwpTableBuffer(tableName)) {
                        QwpSchemaBinding binding = new QwpSchemaBinding(
                                table,
                                describe(client, 100 + input.ordinal() * 10L + target.ordinal(), tableName)
                        );
                        int row = 0;
                        for (Vector vector : selected) {
                            binding.longColumn("case_id", row++);
                            vector.append(binding);
                            table.nextRow();
                        }
                        int length = encoder.encodeSchema(table);
                        assertWire(encoder, length, target, selected);
                        client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                        assertOk(client);
                    }
                    drainWalQueue();
                    assertStored(tableName, target, selected);
                }
            }
        });
    }

    @Test
    public void testLegacySenderPreservesTheSameTextValues() throws Exception {
        List<Vector> vectors = readVectors();
        runInContext(port -> {
            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                for (Input input : Input.values()) {
                    for (Target target : Target.values()) {
                        List<Vector> selected = select(vectors, input, target);
                        String tableName = "legacy_text_" + input.suffix + '_' + target.suffix;
                        createTable(tableName, target);
                        int row = 0;
                        for (Vector vector : selected) {
                            sender.table(tableName).longColumn("case_id", row++);
                            if (input == Input.STRING) {
                                sender.stringColumn("value", vector.inputValue);
                            } else {
                                sender.symbol("value", vector.inputValue);
                            }
                            sender.atNow();
                        }
                    }
                }
                sender.flush();
            }
            drainWalQueue();
            for (Input input : Input.values()) {
                for (Target target : Target.values()) {
                    assertStored(
                            "legacy_text_" + input.suffix + '_' + target.suffix,
                            target,
                            select(vectors, input, target)
                    );
                }
            }
        });
    }

    private void assertStored(String tableName, Target target, List<Vector> vectors) throws Exception {
        try (RecordCursorFactory factory = select("select case_id, value from " + tableName + " order by case_id");
             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Record record = cursor.getRecord();
            int row = 0;
            while (cursor.hasNext()) {
                Vector vector = vectors.get(row);
                Assert.assertEquals(vector.caseId, row, record.getLong(0));
                if (target == Target.VARCHAR) {
                    assertUtf8(vector.caseId, vector.expectedUtf8, record.getVarcharA(1));
                } else {
                    CharSequence value = target == Target.STRING ? record.getStrA(1) : record.getSymA(1);
                    Assert.assertEquals(vector.caseId, vector.expectedValue, value == null ? null : value.toString());
                }
                row++;
            }
            Assert.assertEquals(vectors.size(), row);
        }
    }

    private void createTable(String tableName, Target target) throws Exception {
        execute("create table " + tableName + " (case_id long, value " + target.sqlType
                + ", ts timestamp) timestamp(ts) partition by day wal");
    }

    private static void assertWire(
            QwpWebSocketEncoder encoder,
            int length,
            Target target,
            List<Vector> vectors
    ) throws Exception {
        QwpTableBlockCursor table = parseSingleTable(encoder, length);
        Assert.assertTrue(table.hasKnownSchemaIdentity());
        Assert.assertEquals(target.wireType, table.getColumnDef(1).getTypeCode());
        Map<String, Integer> firstSymbolIndexByWireBytes = new HashMap<>();
        for (Vector vector : vectors) {
            Assert.assertTrue(vector.caseId, table.hasNextRow());
            table.nextRow();
            Assert.assertEquals(vector.caseId, vector.expectedValue == null, table.isColumnNull(1));
            if (target == Target.SYMBOL) {
                QwpSymbolColumnCursor symbol = table.getSymbolColumn(1);
                assertUtf8(vector.caseId, vector.expectedUtf8, symbol.getSymbolUtf8());
                if (vector.expectedUtf8 != null) {
                    Integer prior = firstSymbolIndexByWireBytes.putIfAbsent(
                            vector.expectedUtf8Hex,
                            symbol.getSymbolIndex()
                    );
                    if (prior != null) {
                        Assert.assertNotEquals(
                                "distinct raw UTF-16 keys may normalize to the same wire bytes",
                                prior.intValue(),
                                symbol.getSymbolIndex()
                        );
                    }
                }
            } else if (vector.expectedUtf8 != null) {
                QwpStringColumnCursor string = table.getStringColumn(1);
                assertUtf8(vector.caseId, vector.expectedUtf8, string.getUtf8Value());
            }
        }
        Assert.assertEquals(vectors.size(), table.getRowCount());
        Assert.assertFalse(table.hasNextRow());
    }

    private static void assertUtf8(String caseId, byte[] expected, Utf8Sequence actual) {
        if (expected == null) {
            Assert.assertNull(caseId, actual);
            return;
        }
        Assert.assertNotNull(caseId, actual);
        Assert.assertEquals(caseId, expected.length, actual.size());
        for (int i = 0; i < expected.length; i++) {
            Assert.assertEquals(caseId + " byte " + i, expected[i], actual.byteAt(i));
        }
    }

    private static List<Vector> readVectors() throws Exception {
        InputStream stream = QwpSchemaTextE2ETest.class.getResourceAsStream(VECTORS);
        Assert.assertNotNull(VECTORS, stream);
        List<Vector> vectors = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty() || line.charAt(0) == '#') {
                    continue;
                }
                String[] fields = line.split("\\t", -1);
                Assert.assertEquals(line, 7, fields.length);
                Input input = Input.valueOf(fields[1]);
                Target target = Target.valueOf(fields[2]);
                Assert.assertEquals(line, target.wireTypeName, fields[4]);
                String inputValue = "<NULL>".equals(fields[3]) ? null : utf16(fields[3]);
                byte[] expectedUtf8 = "<NULL>".equals(fields[5]) ? null : bytes(fields[5]);
                String expectedValue = "<NULL>".equals(fields[6]) ? null : utf16(fields[6]);
                Assert.assertEquals(line, inputValue == null, expectedUtf8 == null);
                Assert.assertEquals(line, inputValue == null, expectedValue == null);
                vectors.add(new Vector(fields[0], input, target, inputValue, expectedUtf8, fields[5], expectedValue));
            }
        }
        Assert.assertEquals("shared corpus row count", 90, vectors.size());
        for (Input input : Input.values()) {
            for (Target target : Target.values()) {
                Assert.assertEquals(input + " -> " + target, 15, select(vectors, input, target).size());
            }
        }
        return vectors;
    }

    private static List<Vector> select(List<Vector> vectors, Input input, Target target) {
        List<Vector> selected = new ArrayList<>();
        for (Vector vector : vectors) {
            if (vector.input == input && vector.target == target) {
                selected.add(vector);
            }
        }
        return selected;
    }

    private static byte[] bytes(String hex) {
        Assert.assertEquals(hex, 0, hex.length() & 1);
        byte[] bytes = new byte[hex.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
        }
        return bytes;
    }

    private static String utf16(String hex) {
        Assert.assertEquals(hex, 0, hex.length() & 3);
        char[] chars = new char[hex.length() / 4];
        for (int i = 0; i < chars.length; i++) {
            chars[i] = (char) Integer.parseInt(hex.substring(i * 4, i * 4 + 4), 16);
        }
        return new String(chars);
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
                Unsafe.putByte(address + i, request[i]);
            }
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
        Assert.assertNotNull(response.get());
        Assert.assertEquals(requestId, response.get().getRequestId());
        return response.get();
    }

    private static void assertOk(WebSocketClient client) {
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
        Assert.assertTrue(response.get().getErrorMessage(), response.get().isSuccess());
        Assert.assertEquals(0, response.get().getSequence());
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
        STRING("string"),
        SYMBOL("symbol");

        private final String suffix;

        Input(String suffix) {
            this.suffix = suffix;
        }
    }

    private enum Target {
        STRING("string", ColumnType.STRING, QwpConstants.TYPE_VARCHAR, "VARCHAR"),
        VARCHAR("varchar", ColumnType.VARCHAR, QwpConstants.TYPE_VARCHAR, "VARCHAR"),
        SYMBOL("symbol", ColumnType.SYMBOL, QwpConstants.TYPE_SYMBOL, "SYMBOL");

        private final String sqlType;
        private final String suffix;
        private final byte wireType;
        private final String wireTypeName;

        Target(String suffix, int sqlType, byte wireType, String wireTypeName) {
            this.suffix = suffix;
            this.sqlType = ColumnType.nameOf(sqlType);
            this.wireType = wireType;
            this.wireTypeName = wireTypeName;
        }
    }

    private static final class Vector {
        private final String caseId;
        private final byte[] expectedUtf8;
        private final String expectedUtf8Hex;
        private final String expectedValue;
        private final Input input;
        private final String inputValue;
        private final Target target;

        private Vector(
                String caseId,
                Input input,
                Target target,
                String inputValue,
                byte[] expectedUtf8,
                String expectedUtf8Hex,
                String expectedValue
        ) {
            this.caseId = caseId;
            this.input = input;
            this.target = target;
            this.inputValue = inputValue;
            this.expectedUtf8 = expectedUtf8;
            this.expectedUtf8Hex = expectedUtf8Hex;
            this.expectedValue = expectedValue;
        }

        private void append(QwpSchemaBinding binding) {
            if (input == Input.STRING) {
                binding.stringColumn("value", inputValue);
            } else {
                binding.symbol("value", inputValue);
            }
        }
    }
}
