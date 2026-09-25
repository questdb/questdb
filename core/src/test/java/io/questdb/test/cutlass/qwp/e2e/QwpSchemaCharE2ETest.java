/*+*****************************************************************************
 * Copyright (c) 2014-2019 Appsicle
 * Copyright (c) 2019-2026 QuestDB
 * Licensed under the Apache License, Version 2.0
 *******************************************************************************/
package io.questdb.test.cutlass.qwp.e2e;

import io.questdb.client.LineSenderSchemaException;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketClientFactory;
import io.questdb.client.cutlass.http.client.WebSocketFrameHandler;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpFixedWidthColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpMessageCursor;
import io.questdb.cutlass.qwp.protocol.QwpStringColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpTableBlockCursor;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8Sequence;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class QwpSchemaCharE2ETest extends AbstractQwpWebSocketTest {
    private static final String CORPUS = "/io/questdb/client/cutlass/qwp/char-conversions.tsv";

    @Test
    public void testLegacyCharAndVarcharWireToCharWithOmission() throws Exception {
        runInContext(port -> {
            execute("create table legacy_char_native (case_id long, value char, ts timestamp) timestamp(ts) partition by day wal");
            execute("create table legacy_char_text (case_id long, value char, ts timestamp) timestamp(ts) partition by day wal");
            execute("create table legacy_char_native_plain (value char, ts timestamp) timestamp(ts) partition by day wal");
            execute("create table legacy_char_text_plain (value char, ts timestamp) timestamp(ts) partition by day wal");
            try (WebSocketClient client = connectLegacy(port); QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                try (QwpTableBuffer table = new QwpTableBuffer("legacy_char_native_plain")) {
                    table.getOrCreateColumn("value", QwpConstants.TYPE_CHAR, true).addShort((short) 0xd83d);
                    table.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(1);
                    table.nextRow();
                    int length = encoder.encode(table);
                    QwpTableBlockCursor wire = parseSingleTable(encoder, length);
                    Assert.assertEquals(2, wire.getColumnCount());
                    Assert.assertEquals(QwpConstants.TYPE_CHAR, wire.getColumnDef(0).getTypeCode());
                    QwpFixedWidthColumnCursor chars = wire.getFixedWidthColumn(0);
                    Assert.assertEquals(0, chars.getNullBitmapAddress());
                    Assert.assertTrue(wire.hasNextRow());
                    wire.nextRow();
                    Assert.assertFalse(wire.isColumnNull(0));
                    Assert.assertEquals(0xd83d, chars.getShort() & 0xffff);
                    Assert.assertFalse(wire.hasNextRow());
                    client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                    assertOk(client, 0);
                }
                try (QwpTableBuffer table = new QwpTableBuffer("legacy_char_text_plain")) {
                    table.getOrCreateColumn("value", QwpConstants.TYPE_VARCHAR, true).addString("€tail");
                    table.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(1);
                    table.nextRow();
                    int length = encoder.encode(table);
                    QwpTableBlockCursor wire = parseSingleTable(encoder, length);
                    Assert.assertEquals(2, wire.getColumnCount());
                    Assert.assertEquals(QwpConstants.TYPE_VARCHAR, wire.getColumnDef(0).getTypeCode());
                    Assert.assertTrue(wire.hasNextRow());
                    wire.nextRow();
                    Assert.assertFalse(wire.isColumnNull(0));
                    assertUtf8("€tail", wire.getStringColumn(0).getUtf8Value());
                    Assert.assertFalse(wire.hasNextRow());
                    client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                    assertOk(client, 1);
                }
                try (QwpTableBuffer table = new QwpTableBuffer("legacy_char_native")) {
                    QwpTableBuffer.ColumnBuffer ids = table.getOrCreateColumn("case_id", QwpConstants.TYPE_LONG, true);
                    QwpTableBuffer.ColumnBuffer values = table.getOrCreateColumn("value", QwpConstants.TYPE_CHAR, true);
                    QwpTableBuffer.ColumnBuffer timestamps = table.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP);
                    ids.addLong(0);
                    values.addShort((short) 'A');
                    timestamps.addLong(1);
                    table.nextRow();
                    ids.addLong(1);
                    values.addShort((short) 0);
                    timestamps.addLong(2);
                    table.nextRow();
                    ids.addLong(2);
                    timestamps.addLong(3);
                    table.nextRow();
                    int length = encoder.encode(table);
                    QwpTableBlockCursor wire = parseSingleTable(encoder, length);
                    Assert.assertEquals(3, wire.getColumnCount());
                    Assert.assertEquals(3, wire.getRowCount());
                    Assert.assertEquals(QwpConstants.TYPE_CHAR, wire.getColumnDef(1).getTypeCode());
                    QwpFixedWidthColumnCursor chars = wire.getFixedWidthColumn(1);
                    Assert.assertNotEquals(0, chars.getNullBitmapAddress());
                    Assert.assertEquals(2, chars.getValueCount());
                    assertRows(wire, new int[]{'A', 0, 0}, new boolean[]{false, false, true});
                    client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                    assertOk(client, 2);
                }
                try (QwpTableBuffer table = new QwpTableBuffer("legacy_char_text")) {
                    QwpTableBuffer.ColumnBuffer ids = table.getOrCreateColumn("case_id", QwpConstants.TYPE_LONG, true);
                    QwpTableBuffer.ColumnBuffer values = table.getOrCreateColumn("value", QwpConstants.TYPE_VARCHAR, true);
                    QwpTableBuffer.ColumnBuffer timestamps = table.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP);
                    ids.addLong(0);
                    values.addString("étail");
                    timestamps.addLong(1);
                    table.nextRow();
                    ids.addLong(1);
                    values.addString("😀");
                    timestamps.addLong(2);
                    table.nextRow();
                    ids.addLong(2);
                    values.addNull();
                    timestamps.addLong(3);
                    table.nextRow();
                    ids.addLong(3);
                    timestamps.addLong(4);
                    table.nextRow();
                    int length = encoder.encode(table);
                    QwpTableBlockCursor wire = parseSingleTable(encoder, length);
                    Assert.assertEquals(3, wire.getColumnCount());
                    Assert.assertEquals(QwpConstants.TYPE_VARCHAR, wire.getColumnDef(1).getTypeCode());
                    QwpStringColumnCursor strings = wire.getStringColumn(1);
                    Assert.assertTrue(wire.hasNextRow());
                    wire.nextRow();
                    assertUtf8("étail", strings.getUtf8Value());
                    Assert.assertTrue(wire.hasNextRow());
                    wire.nextRow();
                    assertUtf8("😀", strings.getUtf8Value());
                    Assert.assertTrue(wire.hasNextRow());
                    wire.nextRow();
                    Assert.assertTrue(wire.isColumnNull(1));
                    Assert.assertTrue(wire.hasNextRow());
                    wire.nextRow();
                    Assert.assertTrue(wire.isColumnNull(1));
                    Assert.assertFalse(wire.hasNextRow());
                    client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                    assertOk(client, 3);
                }
            }
            drainWalQueue();
            assertStored("legacy_char_native_plain", new int[]{0xd83d}, new boolean[]{false});
            assertStored("legacy_char_text_plain", new int[]{'€'}, new boolean[]{false});
            assertStored("legacy_char_native", new int[]{'A', 0, 0}, new boolean[]{false, true, true});
            assertStored("legacy_char_text", new int[]{'é', 0, 0, 0}, new boolean[]{false, true, true, true});
        });
    }

    @Test
    public void testPublicSenderCorpus() throws Exception {
        List<Vector> vectors = readVectors();
        Assert.assertEquals(23, vectors.size());
        runInContext(port -> {
            execute("create table schema_char_corpus (case_id long, value char, ts timestamp) timestamp(ts) partition by day wal");
            try (QwpWebSocketSender sender = connectWs(port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                int row = 0;
                for (Vector vector : vectors) {
                    sender.table("schema_char_corpus").longColumn("case_id", row);
                    if (vector.inputKind.equals("CHAR")) {
                        Assert.assertNotNull(vector.caseId, vector.value);
                        Assert.assertEquals(vector.caseId, 1, vector.value.length());
                        sender.charColumn("value", vector.value.charAt(0));
                    } else {
                        sender.stringColumn("value", vector.value);
                    }
                    sender.at(1_000_000L + row++, ChronoUnit.MICROS);
                }
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(fsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }
            drainWalQueue();
            try (RecordCursorFactory factory = select("select case_id, value, value is null from schema_char_corpus order by case_id");
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                for (int i = 0; i < vectors.size(); i++) {
                    Vector vector = vectors.get(i);
                    Assert.assertTrue(vector.caseId, cursor.hasNext());
                    Assert.assertEquals(vector.caseId, i, record.getLong(0));
                    Assert.assertEquals(vector.caseId, vector.expectedChar, record.getChar(1));
                    Assert.assertEquals(vector.caseId, vector.expectedNull, record.getBool(2));
                }
                Assert.assertFalse(cursor.hasNext());
            }
        });
    }

    @Test
    public void testPublicSenderDuplicateRollbackWrongTargetAndDesignatedGuard() throws Exception {
        runInContext(port -> {
            execute("create table schema_char_rows (value char, marker string, bad uuid, wrong long, ts timestamp) timestamp(ts) partition by day wal");
            try (QwpWebSocketSender sender = connectWs(port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                sender.table("schema_char_rows").stringColumn("value", null)
                        .binaryColumn("value", new byte[]{1})
                        .stringColumn("marker", "null-first").at(0, ChronoUnit.MICROS);
                sender.charColumn("value", 'A')
                        .stringColumn("value", "ignored duplicate")
                        .stringColumn("marker", "A").at(1, ChronoUnit.MICROS);
                sender.stringColumn("marker", "failed-B");
                LineSenderSchemaException invalid = Assert.assertThrows(LineSenderSchemaException.class,
                        () -> sender.stringColumn("bad", "not-a-uuid"));
                Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, invalid.getReason());
                sender.stringColumn("value", "C-tail").stringColumn("marker", "C").at(2, ChronoUnit.MICROS);
                LineSenderSchemaException wrong = Assert.assertThrows(LineSenderSchemaException.class,
                        () -> sender.charColumn("wrong", 'X'));
                Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, wrong.getReason());
                sender.stringColumn("marker", "failed-designated");
                LineSenderSchemaException designated = Assert.assertThrows(LineSenderSchemaException.class,
                        () -> sender.charColumn("ts", 'X'));
                Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, designated.getReason());
                sender.stringColumn("marker", "D").at(3, ChronoUnit.MICROS);
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(fsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }
            drainWalQueue();
            assertQuery("select value, marker, bad, wrong, ts from schema_char_rows order by ts")
                    .noLeakCheck().expectSize().timestamp("ts").returns("value\tmarker\tbad\twrong\tts\n\tnull-first\t\tnull\t1970-01-01T00:00:00.000000Z\nA\tA\t\tnull\t1970-01-01T00:00:00.000001Z\nC\tC\t\tnull\t1970-01-01T00:00:00.000002Z\n\tD\t\tnull\t1970-01-01T00:00:00.000003Z\n");
        });
    }

    @Test
    public void testMissingCharInferenceAndAckAdoption() throws Exception {
        runInContext(port -> {
            try (QwpWebSocketSender sender = connectWs(port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                sender.table("schema_char_inferred").charColumn("value", 'A').at(1, ChronoUnit.MICROS);
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
                sender.table("schema_char_inferred").stringColumn("value", "€tail").at(2, ChronoUnit.MICROS);
                fsn = sender.flushAndGetSequence();
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }
            drainWalQueue();
            assertQuery("select value, value is null n, timestamp from schema_char_inferred order by timestamp")
                    .noLeakCheck().expectSize().timestamp("timestamp").returns("value\tn\ttimestamp\nA\tfalse\t1970-01-01T00:00:00.000001Z\n€\tfalse\t1970-01-01T00:00:00.000002Z\n");
        });
    }

    private void assertStored(String table, int[] chars, boolean[] nulls) throws Exception {
        try (RecordCursorFactory factory = select("select value, value is null from " + table + " order by ts");
             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Record record = cursor.getRecord();
            for (int i = 0; i < chars.length; i++) {
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(chars[i], record.getChar(0));
                Assert.assertEquals(nulls[i], record.getBool(1));
            }
            Assert.assertFalse(cursor.hasNext());
        }
    }

    private static void assertRows(QwpTableBlockCursor wire, int[] chars, boolean[] nulls) throws Exception {
        QwpFixedWidthColumnCursor values = wire.getFixedWidthColumn(1);
        for (int i = 0; i < chars.length; i++) {
            Assert.assertTrue(wire.hasNextRow());
            wire.nextRow();
            Assert.assertEquals(nulls[i], wire.isColumnNull(1));
            if (!nulls[i]) Assert.assertEquals(chars[i], values.getShort() & 0xffff);
        }
        Assert.assertFalse(wire.hasNextRow());
    }

    private static void assertUtf8(String expected, Utf8Sequence actual) {
        Assert.assertNotNull(actual);
        String text = io.questdb.std.str.Utf8s.toString(actual);
        Assert.assertEquals(expected, text);
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
            if (!success) client.close();
        }
    }

    private static void assertOk(WebSocketClient client, long expectedSequence) {
        AtomicReference<WebSocketResponse> response = new AtomicReference<>();
        Assert.assertTrue(client.receiveFrame(new WebSocketFrameHandler() {
            @Override
            public void onBinaryMessage(long ptr, int len) {
                WebSocketResponse parsed = new WebSocketResponse();
                Assert.assertTrue(parsed.readFrom(ptr, len));
                response.set(parsed);
            }

            @Override
            public void onClose(int code, String reason) {
                Assert.fail("unexpected close " + code + ": " + reason);
            }
        }, 5_000));
        Assert.assertNotNull(response.get());
        Assert.assertTrue(response.get().getErrorMessage(), response.get().isSuccess());
        Assert.assertEquals(expectedSequence, response.get().getSequence());
    }

    private static QwpTableBlockCursor parseSingleTable(QwpWebSocketEncoder encoder, int length) throws Exception {
        Assert.assertEquals(0, Unsafe.getByte(encoder.getBuffer().getBufferPtr() + QwpConstants.HEADER_OFFSET_FLAGS)
                & QwpConstants.FLAG_SCHEMA);
        QwpMessageCursor cursor = new QwpMessageCursor();
        cursor.of(encoder.getBuffer().getBufferPtr(), length, new ObjList<>());
        Assert.assertTrue(cursor.hasNextTable());
        QwpTableBlockCursor table = cursor.nextTable();
        Assert.assertFalse(cursor.hasNextTable());
        return table;
    }

    private static List<Vector> readVectors() throws Exception {
        List<Vector> vectors = new ArrayList<>();
        try (InputStream in = QwpSchemaCharE2ETest.class.getResourceAsStream(CORPUS)) {
            Assert.assertNotNull(in);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                Assert.assertEquals("# case_id\tinput_kind\tutf16_hex\texpected_char\texpected_null", reader.readLine());
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] fields = line.split("\\t", -1);
                    Assert.assertEquals(line, 5, fields.length);
                    Assert.assertTrue(line, fields[1].equals("CHAR") || fields[1].equals("STRING"));
                    Assert.assertTrue(line, fields[4].equals("true") || fields[4].equals("false"));
                    String value = decode(fields[2]);
                    Assert.assertEquals(line, value == null, fields[2].equals("<NULL>"));
                    vectors.add(new Vector(fields[0], fields[1], value, Integer.parseInt(fields[3]), Boolean.parseBoolean(fields[4])));
                }
            }
        }
        return vectors;
    }

    private static String decode(String hex) {
        if (hex.equals("<NULL>")) return null;
        StringBuilder sink = new StringBuilder(hex.length() / 4);
        for (int i = 0; i < hex.length(); i += 4) sink.append((char) Integer.parseInt(hex.substring(i, i + 4), 16));
        return sink.toString();
    }

    private static class Vector {
        final String caseId;
        final String inputKind;
        final String value;
        final int expectedChar;
        final boolean expectedNull;

        Vector(String caseId, String inputKind, String value, int expectedChar, boolean expectedNull) {
            this.caseId = caseId;
            this.inputKind = inputKind;
            this.value = value;
            this.expectedChar = expectedChar;
            this.expectedNull = expectedNull;
        }
    }
}
