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
import io.questdb.client.std.bytes.DirectByteSlice;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpMessageCursor;
import io.questdb.cutlass.qwp.protocol.QwpStringColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpTableBlockCursor;
import io.questdb.std.BinarySequence;
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

public class QwpSchemaBinaryE2ETest extends AbstractQwpWebSocketTest {
    private static final String VECTORS = "/io/questdb/client/cutlass/qwp/binary-input.tsv";

    @Test
    public void testBinaryCorpusUsesPinnedTargetWireAndExactStoredBytes() throws Exception {
        List<Vector> vectors = readVectors();
        execute("create table schema_binary_corpus (case_id long, value binary, ts timestamp) "
                + "timestamp(ts) partition by day wal");
        execute("create table legacy_binary_corpus (case_id long, value binary, ts timestamp) "
                + "timestamp(ts) partition by day wal");
        runInContext(port -> {
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = new QwpTableBuffer("schema_binary_corpus")) {
                QwpSchemaResponse schema = describe(client, 100, "schema_binary_corpus");
                QwpSchemaBinding binding = new QwpSchemaBinding(table, schema);
                int row = 0;
                for (Vector vector : vectors) {
                    binding.longColumn("case_id", row++);
                    vector.append(binding);
                    table.nextRow();
                }
                int length = encoder.encodeSchema(table);
                assertWire(encoder, length, schema, vectors);
                client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                assertOk(client);
            }
            for (Input input : Input.values()) {
                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                    for (int row = 0; row < vectors.size(); row++) {
                        Vector vector = vectors.get(row);
                        if (vector.input != input) {
                            continue;
                        }
                        sender.table("legacy_binary_corpus").longColumn("case_id", row);
                        if (input == Input.BINARY) {
                            sender.binaryColumn("value", vector.inputData);
                        } else {
                            sender.stringColumn("value", vector.inputText);
                        }
                        sender.atNow();
                    }
                    sender.flush();
                }
            }
            drainWalQueue();
            assertStored("schema_binary_corpus", vectors);
            assertStored("legacy_binary_corpus", vectors);
        });
    }

    @Test
    public void testBinaryOverloadsCopyInputAndRollbackFailedRows() throws Exception {
        execute("create table schema_binary_rows (case_id long, value binary, bad long, only_b binary, ts timestamp) "
                + "timestamp(ts) partition by day wal");
        runInContext(port -> {
            long ptr = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);
            try {
                try (WebSocketClient client = connect(port);
                     QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                     QwpTableBuffer table = new QwpTableBuffer("schema_binary_rows")) {
                    QwpSchemaResponse schema = describe(client, 101, "schema_binary_rows");
                    QwpSchemaBinding binding = new QwpSchemaBinding(table, schema);

                    byte[] array = {1, 2, 3};
                    binding.longColumn("case_id", 0);
                    binding.binaryColumn("value", array);
                    array[0] = 99;
                    table.nextRow();

                    putBytes(ptr, new byte[]{4, 5, 6, 7});
                    binding.longColumn("case_id", 99);
                    binding.binaryColumn("only_b", ptr, 4);
                    LineSenderSchemaException bad = Assert.assertThrows(
                            LineSenderSchemaException.class,
                            () -> binding.binaryColumn("bad", ptr, 4)
                    );
                    Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, bad.getReason());
                    table.cancelCurrentRow();
                    table.rollbackUncommittedColumns();

                    DirectByteSlice slice = new DirectByteSlice().of(ptr, 4);
                    binding.longColumn("case_id", 1);
                    binding.binaryColumn("value", slice);
                    putBytes(ptr, new byte[]{9, 9, 9, 9});
                    binding.binaryColumn("value", (byte[]) null); // duplicate is ignored before validation
                    table.nextRow();

                    LineSenderSchemaException nullArray = Assert.assertThrows(
                            LineSenderSchemaException.class,
                            () -> binding.binaryColumn("value", (byte[]) null)
                    );
                    Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, nullArray.getReason());
                    table.cancelCurrentRow();
                    table.rollbackUncommittedColumns();

                    binding.longColumn("case_id", 100);
                    LineSenderSchemaException nullSlice = Assert.assertThrows(
                            LineSenderSchemaException.class,
                            () -> binding.binaryColumn("value", (DirectByteSlice) null)
                    );
                    Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, nullSlice.getReason());
                    table.cancelCurrentRow();
                    table.rollbackUncommittedColumns();

                    binding.longColumn("case_id", 101);
                    LineSenderSchemaException nullPointer = Assert.assertThrows(
                            LineSenderSchemaException.class,
                            () -> binding.binaryColumn("value", 0, 1)
                    );
                    Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, nullPointer.getReason());
                    table.cancelCurrentRow();
                    table.rollbackUncommittedColumns();

                    binding.longColumn("case_id", 102);
                    LineSenderSchemaException negativeLength = Assert.assertThrows(
                            LineSenderSchemaException.class,
                            () -> binding.binaryColumn("value", ptr, -1)
                    );
                    Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, negativeLength.getReason());
                    table.cancelCurrentRow();
                    table.rollbackUncommittedColumns();

                    binding.longColumn("case_id", 2);
                    binding.stringColumn("value", null);
                    table.nextRow();
                    binding.longColumn("case_id", 3);
                    table.nextRow();
                    binding.longColumn("case_id", 4);
                    binding.binaryColumn("value", new byte[0]);
                    table.nextRow();

                    int length = encoder.encodeSchema(table);
                    QwpTableBlockCursor wire = parseSingleTable(encoder, length);
                    Assert.assertEquals(schema.getTableId(), wire.getSchemaTableId());
                    Assert.assertEquals(schema.getMetadataVersion(), wire.getSchemaMetadataVersion());
                    Assert.assertEquals(2, wire.getColumnCount());
                    Assert.assertEquals("case_id", wire.getColumnDef(0).getName());
                    Assert.assertEquals(QwpConstants.TYPE_LONG, wire.getColumnDef(0).getTypeCode());
                    Assert.assertEquals("value", wire.getColumnDef(1).getName());
                    Assert.assertEquals(QwpConstants.TYPE_BINARY, wire.getColumnDef(1).getTypeCode());
                    byte[][] expected = {{1, 2, 3}, {4, 5, 6, 7}, null, null, {}};
                    for (int row = 0; row < expected.length; row++) {
                        byte[] bytes = expected[row];
                        Assert.assertTrue(wire.hasNextRow());
                        wire.nextRow();
                        Assert.assertEquals(row, wire.getFixedWidthColumn(0).getLong());
                        Assert.assertEquals(bytes == null, wire.isColumnNull(1));
                        if (bytes != null) {
                            assertUtf8Bytes("wire row " + row, bytes, wire.getStringColumn(1).getUtf8Value());
                        }
                    }
                    Assert.assertFalse(wire.hasNextRow());
                    client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                    assertOk(client);
                }
            } finally {
                Unsafe.free(ptr, 4, MemoryTag.NATIVE_DEFAULT);
            }
            drainWalQueue();
            assertBinaryRows(
                    "schema_binary_rows",
                    new byte[][]{{1, 2, 3}, {4, 5, 6, 7}, null, null, {}}
            );
        });
    }

    @Test
    public void testNativeBinarySurvivesStoreAndForward() throws Exception {
        execute("create table schema_binary_replay (value binary, ts timestamp) timestamp(ts) partition by day wal");
        runInContext(port -> {
            File sfRoot = temp.newFolder("qwp-schema-binary-sf");
            String slot = new File(sfRoot, "default").getAbsolutePath();
            byte[] payload = new byte[256];
            for (int i = 0; i < payload.length; i++) {
                payload[i] = (byte) i;
            }
            long ptr = Unsafe.malloc(payload.length, MemoryTag.NATIVE_DEFAULT);
            try {
                putBytes(ptr, payload);
                try (WebSocketClient client = connect(port);
                     QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                     QwpTableBuffer table = new QwpTableBuffer("schema_binary_replay")) {
                    QwpSchemaResponse schema = describe(client, 102, "schema_binary_replay");
                    QwpSchemaBinding binding = new QwpSchemaBinding(table, schema);
                    binding.binaryColumn("value", ptr, payload.length);
                    table.nextRow();
                    int length = encoder.encodeSchema(table);
                    assertGorillaMessageFlag(encoder);
                    QwpTableBlockCursor wire = parseSingleTable(encoder, length);
                    Assert.assertEquals(schema.getTableId(), wire.getSchemaTableId());
                    Assert.assertEquals(schema.getMetadataVersion(), wire.getSchemaMetadataVersion());
                    Assert.assertEquals(QwpConstants.TYPE_BINARY, wire.getColumnDef(0).getTypeCode());
                    Assert.assertTrue(wire.hasNextRow());
                    wire.nextRow();
                    assertUtf8Bytes("pre-persistence wire", payload, wire.getStringColumn(0).getUtf8Value());
                    try (CursorSendEngine cursorEngine = new CursorSendEngine(slot, 1 << 20)) {
                        Assert.assertEquals(0, cursorEngine.appendBlocking(encoder.getBuffer().getBufferPtr(), length));
                    }
                }
            } finally {
                Unsafe.free(ptr, payload.length, MemoryTag.NATIVE_DEFAULT);
            }
            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port
                    + ";sf_dir=" + sfRoot.getAbsolutePath()
                    + ";sender_id=default;sf_max_segment_bytes=1m;")) {
                Assert.assertTrue(sender.drain(10_000));
            }
            drainWalQueue();
            try (RecordCursorFactory factory = select("select value from schema_binary_replay");
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(cursor.hasNext());
                BinarySequence value = cursor.getRecord().getBin(0);
                Assert.assertNotNull(value);
                Assert.assertEquals(256, value.length());
                for (int i = 0; i < 256; i++) {
                    Assert.assertEquals("byte " + i, (byte) i, value.byteAt(i));
                }
                Assert.assertFalse(cursor.hasNext());
            }
        });
    }

    @Test
    public void testThrowingStringAppendRollsBackExistingColumns() throws Exception {
        execute("create table legacy_throwing_varchar (case_id long, value varchar, ts timestamp) "
                + "timestamp(ts) partition by day wal");
        execute("create table schema_throwing_binary (case_id long, value binary, ts timestamp) "
                + "timestamp(ts) partition by day wal");
        runInContext(port -> {
            RuntimeException legacyFailure = new RuntimeException("synthetic UTF-16 append failure");
            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("legacy_throwing_varchar").longColumn("case_id", 0)
                        .stringColumn("value", "A").atNow();
                RuntimeException actual = Assert.assertThrows(
                        RuntimeException.class,
                        () -> sender.table("legacy_throwing_varchar").longColumn("case_id", 1)
                                .stringColumn("value", new ThrowingCharSequence(legacyFailure))
                );
                Assert.assertSame(legacyFailure, actual);
                sender.table("legacy_throwing_varchar").longColumn("case_id", 2)
                        .stringColumn("value", "C").atNow();
                sender.flush();
            }

            RuntimeException schemaFailure = new RuntimeException("synthetic schema UTF-16 append failure");
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = new QwpTableBuffer("schema_throwing_binary")) {
                QwpSchemaResponse schema = describe(client, 103, "schema_throwing_binary");
                QwpSchemaBinding binding = new QwpSchemaBinding(table, schema);
                binding.longColumn("case_id", 0).stringColumn("value", "A");
                table.nextRow();
                binding.longColumn("case_id", 1);
                RuntimeException actual = Assert.assertThrows(
                        RuntimeException.class,
                        () -> binding.stringColumn("value", new ThrowingCharSequence(schemaFailure))
                );
                Assert.assertSame(schemaFailure, actual);
                table.cancelCurrentRow();
                table.rollbackUncommittedColumns();
                binding.longColumn("case_id", 2).stringColumn("value", "C");
                table.nextRow();

                int length = encoder.encodeSchema(table);
                QwpTableBlockCursor wire = parseSingleTable(encoder, length);
                Assert.assertEquals(schema.getTableId(), wire.getSchemaTableId());
                Assert.assertEquals(schema.getMetadataVersion(), wire.getSchemaMetadataVersion());
                Assert.assertEquals(QwpConstants.TYPE_BINARY, wire.getColumnDef(1).getTypeCode());
                Assert.assertTrue(wire.hasNextRow());
                wire.nextRow();
                assertUtf8Bytes("schema A", new byte[]{'A'}, wire.getStringColumn(1).getUtf8Value());
                Assert.assertTrue(wire.hasNextRow());
                wire.nextRow();
                assertUtf8Bytes("schema C", new byte[]{'C'}, wire.getStringColumn(1).getUtf8Value());
                Assert.assertFalse(wire.hasNextRow());
                client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                assertOk(client);
            }

            drainWalQueue();
            assertQuery("select case_id, value from legacy_throwing_varchar order by case_id")
                    .noLeakCheck().expectSize().returns("case_id\tvalue\n0\tA\n2\tC\n");
            assertBinaryRows("schema_throwing_binary", new byte[][]{{'A'}, {'C'}}, new long[]{0, 2});
        });
    }

    private void assertStored(String tableName, List<Vector> vectors) throws Exception {
        try (RecordCursorFactory factory = select("select case_id, value from " + tableName + " order by case_id");
             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Record record = cursor.getRecord();
            int row = 0;
            while (cursor.hasNext()) {
                Vector vector = vectors.get(row);
                Assert.assertEquals(vector.caseId, row, record.getLong(0));
                BinarySequence actual = record.getBin(1);
                if (vector.expectedSql == null) {
                    Assert.assertNull(vector.caseId, actual);
                    Assert.assertEquals(vector.caseId, -1, record.getBinLen(1));
                } else {
                    Assert.assertNotNull(vector.caseId, actual);
                    Assert.assertEquals(vector.caseId, vector.expectedSql.length, actual.length());
                    for (int i = 0; i < vector.expectedSql.length; i++) {
                        Assert.assertEquals(vector.caseId + " byte " + i, vector.expectedSql[i], actual.byteAt(i));
                    }
                }
                row++;
            }
            Assert.assertEquals(vectors.size(), row);
        }
    }

    private void assertBinaryRows(String tableName, byte[][] expected) throws Exception {
        long[] caseIds = new long[expected.length];
        for (int i = 0; i < caseIds.length; i++) {
            caseIds[i] = i;
        }
        assertBinaryRows(tableName, expected, caseIds);
    }

    private void assertBinaryRows(String tableName, byte[][] expected, long[] caseIds) throws Exception {
        Assert.assertEquals(expected.length, caseIds.length);
        try (RecordCursorFactory factory = select("select case_id, value from " + tableName + " order by case_id");
             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Record record = cursor.getRecord();
            for (int row = 0; row < expected.length; row++) {
                Assert.assertTrue("missing row " + row, cursor.hasNext());
                Assert.assertEquals(caseIds[row], record.getLong(0));
                byte[] bytes = expected[row];
                BinarySequence actual = record.getBin(1);
                if (bytes == null) {
                    Assert.assertNull("row " + row, actual);
                    Assert.assertEquals(-1, record.getBinLen(1));
                } else {
                    Assert.assertNotNull("row " + row, actual);
                    Assert.assertEquals(bytes.length, actual.length());
                    for (int i = 0; i < bytes.length; i++) {
                        Assert.assertEquals("row " + row + " byte " + i, bytes[i], actual.byteAt(i));
                    }
                }
            }
            Assert.assertFalse(cursor.hasNext());
        }
    }

    private static void assertWire(
            QwpWebSocketEncoder encoder,
            int length,
            QwpSchemaResponse schema,
            List<Vector> vectors
    ) throws Exception {
        assertGorillaMessageFlag(encoder);
        QwpTableBlockCursor table = parseSingleTable(encoder, length);
        Assert.assertEquals(schema.getTableId(), table.getSchemaTableId());
        Assert.assertEquals(schema.getMetadataVersion(), table.getSchemaMetadataVersion());
        Assert.assertEquals(QwpConstants.TYPE_LONG, table.getColumnDef(0).getTypeCode());
        Assert.assertEquals(QwpConstants.TYPE_BINARY, table.getColumnDef(1).getTypeCode());
        for (Vector vector : vectors) {
            Assert.assertTrue(vector.caseId, table.hasNextRow());
            table.nextRow();
            Assert.assertEquals(vector.caseId, vector.expectedWire == null, table.isColumnNull(1));
            if (vector.expectedWire != null) {
                QwpStringColumnCursor binary = table.getStringColumn(1);
                assertUtf8Bytes(vector.caseId, vector.expectedWire, binary.getUtf8Value());
            }
        }
        Assert.assertEquals(vectors.size(), table.getRowCount());
        Assert.assertFalse(table.hasNextRow());
    }

    private static void assertGorillaMessageFlag(QwpWebSocketEncoder encoder) {
        Assert.assertEquals(
                QwpConstants.FLAG_GORILLA,
                Unsafe.getByte(encoder.getBuffer().getBufferPtr() + QwpConstants.HEADER_OFFSET_FLAGS)
                        & QwpConstants.FLAG_GORILLA
        );
    }

    private static void assertUtf8Bytes(String context, byte[] expected, io.questdb.std.str.Utf8Sequence actual) {
        Assert.assertNotNull(context, actual);
        Assert.assertEquals(context, expected.length, actual.size());
        for (int i = 0; i < expected.length; i++) {
            Assert.assertEquals(context + " byte " + i, expected[i], actual.byteAt(i));
        }
    }

    private static List<Vector> readVectors() throws Exception {
        InputStream stream = QwpSchemaBinaryE2ETest.class.getResourceAsStream(VECTORS);
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
                Assert.assertEquals(line, "BINARY", fields[3]);
                Assert.assertEquals(line, "VALID", fields[6]);
                byte[] inputData = "<NULL>".equals(fields[2]) ? null
                        : input == Input.BINARY ? bytes(fields[2]) : null;
                String inputText = input == Input.STRING && !"<NULL>".equals(fields[2]) ? utf16(fields[2]) : null;
                byte[] expectedWire = "<NULL>".equals(fields[4]) ? null : bytes(fields[4]);
                byte[] expectedSql = "<NULL>".equals(fields[5]) ? null : bytes(fields[5]);
                Assert.assertArrayEquals(line, expectedWire, expectedSql);
                vectors.add(new Vector(fields[0], input, inputData, inputText, expectedWire, expectedSql));
            }
        }
        Assert.assertEquals("shared corpus row count", 18, vectors.size());
        int binary = 0;
        int string = 0;
        for (Vector vector : vectors) {
            if (vector.input == Input.BINARY) {
                binary++;
            } else {
                string++;
            }
        }
        Assert.assertEquals("BINARY vectors", 4, binary);
        Assert.assertEquals("STRING vectors", 14, string);
        return vectors;
    }

    private static byte[] bytes(String hex) {
        Assert.assertEquals(hex, 0, hex.length() & 1);
        byte[] value = new byte[hex.length() / 2];
        for (int i = 0; i < value.length; i++) {
            value[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
        }
        return value;
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
            putBytes(address, request);
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
    }

    private static QwpTableBlockCursor parseSingleTable(QwpWebSocketEncoder encoder, int length) throws Exception {
        QwpMessageCursor cursor = new QwpMessageCursor();
        cursor.of(encoder.getBuffer().getBufferPtr(), length, new ObjList<>());
        Assert.assertTrue(cursor.hasNextTable());
        QwpTableBlockCursor table = cursor.nextTable();
        Assert.assertFalse(cursor.hasNextTable());
        return table;
    }

    private static void putBytes(long ptr, byte[] bytes) {
        for (int i = 0; i < bytes.length; i++) {
            Unsafe.putByte(ptr + i, bytes[i]);
        }
    }

    private enum Input {
        BINARY,
        STRING
    }

    private static final class Vector {
        private final String caseId;
        private final byte[] expectedSql;
        private final byte[] expectedWire;
        private final byte[] inputData;
        private final String inputText;
        private final Input input;

        private Vector(
                String caseId,
                Input input,
                byte[] inputData,
                String inputText,
                byte[] expectedWire,
                byte[] expectedSql
        ) {
            this.caseId = caseId;
            this.input = input;
            this.inputData = inputData;
            this.inputText = inputText;
            this.expectedWire = expectedWire;
            this.expectedSql = expectedSql;
        }

        private void append(QwpSchemaBinding binding) {
            if (input == Input.BINARY) {
                binding.binaryColumn("value", inputData);
            } else {
                binding.stringColumn("value", inputText);
            }
        }
    }

    private static final class ThrowingCharSequence implements CharSequence {
        private final RuntimeException failure;

        private ThrowingCharSequence(RuntimeException failure) {
            this.failure = failure;
        }

        @Override
        public char charAt(int index) {
            if (index == 0) {
                return 'B';
            }
            throw failure;
        }

        @Override
        public int length() {
            return 3;
        }

        @Override
        public CharSequence subSequence(int start, int end) {
            throw new UnsupportedOperationException();
        }
    }
}
