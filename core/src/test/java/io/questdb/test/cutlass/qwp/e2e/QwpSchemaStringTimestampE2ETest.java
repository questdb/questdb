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
import io.questdb.cutlass.qwp.protocol.QwpMessageCursor;
import io.questdb.cutlass.qwp.protocol.QwpStringColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpTableBlockCursor;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
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

public class QwpSchemaStringTimestampE2ETest extends AbstractQwpWebSocketTest {
    private static final String CORPUS = "/io/questdb/client/cutlass/qwp/string-to-timestamp.tsv";
    private static final String RUNTIME_VECTORS = "/io/questdb/client/cutlass/qwp/string-to-timestamp-runtime.tsv";

    @Test
    public void testDeterministicCorpusStoresExactRawEpochsAndRollsBackInvalidRows() throws Exception {
        List<Vector> vectors = readVectors(CORPUS);
        runInContext(port -> {
            for (Target target : Target.values()) {
                execute("create table " + target.table + " (case_id long, value " + target.sqlType
                        + ", marker string, ts timestamp) timestamp(ts) partition by day wal");
            }
            try (QwpWebSocketSender sender = connectWs(
                    port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                for (Target target : Target.values()) {
                    int caseId = 0;
                    for (Vector vector : vectors) {
                        if (vector.target != target) {
                            continue;
                        }
                        sender.table(target.table).longColumn("case_id", caseId);
                        if (vector.invalid) {
                            final CharSequence input = vector.input;
                            LineSenderSchemaException error = Assert.assertThrows(
                                    vector.caseId, LineSenderSchemaException.class,
                                    () -> sender.stringColumn("value", input));
                            Assert.assertEquals(vector.caseId,
                                    LineSenderSchemaException.Reason.INVALID_VALUE, error.getReason());
                        } else {
                            sender.stringColumn("value", vector.input);
                            if (vector.sqlNull) {
                                // Effective null is still the first write; unsupported duplicates are ignored.
                                sender.binaryColumn("value", new byte[]{1});
                            }
                            sender.at(1_000_000L + caseId++, ChronoUnit.MICROS);
                        }
                    }
                }
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(fsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }
            drainWalQueue();
            for (Target target : Target.values()) {
                assertDeterministicRows(target, vectors);
            }
        });
    }

    @Test
    public void testNamedDesignatedTimestampIsRejectedAndNextRowRecovers() throws Exception {
        runInContext(port -> {
            for (Target target : Target.values()) {
                String table = "schema_string_designated_" + target.suffix;
                execute("create table " + table + " (marker string, ts " + target.sqlType
                        + ") timestamp(ts) partition by day wal");
                try (QwpWebSocketSender sender = connectWs(
                        port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                    sender.table(table).stringColumn("marker", "failed");
                    LineSenderSchemaException error = Assert.assertThrows(
                            LineSenderSchemaException.class,
                            () -> sender.stringColumn("ts", "1970-01-01T00:00:00Z"));
                    Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, error.getReason());
                    sender.stringColumn("marker", "C").at(2, target.unit);
                    long fsn = sender.flushAndGetSequence();
                    Assert.assertTrue(fsn >= 0);
                    Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
                }
                drainWalQueue();
                assertQuery("select marker, cast(ts as long) ts from " + table)
                        .noLeakCheck().returnsOnce("marker\tts\nC\t2\n");
            }
        });
    }

    @Test
    public void testPartialInvalidRowIsCancelledAndNextRowNeedsNoReselection() throws Exception {
        runInContext(port -> {
            for (Target target : Target.values()) {
                String table = "schema_string_ts_rows_" + target.suffix;
                execute("create table " + table + " (value " + target.sqlType
                        + ", marker string, ts timestamp) timestamp(ts) partition by day wal");
                try (QwpWebSocketSender sender = connectWs(
                        port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                    sender.table(table).stringColumn("value", "1970-01-01T00:00:00Z")
                            .stringColumn("marker", "A").at(1, ChronoUnit.MICROS);
                    sender.stringColumn("marker", "failed-B");
                    LineSenderSchemaException error = Assert.assertThrows(
                            LineSenderSchemaException.class,
                            () -> sender.stringColumn("value", "not-a-timestamp"));
                    Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, error.getReason());
                    String message = error.getMessage();
                    Assert.assertTrue(message, message.contains(", table=" + table + ", column=value, inputType=STRING,"));
                    Assert.assertTrue(message, message.contains(target == Target.TIMESTAMP
                            ? "targetType=TIMESTAMP(8)" : "targetType=TIMESTAMP_NS(262152)"));
                    sender.stringColumn("value", "1970-01-01T00:00:00.000001Z")
                            .stringColumn("marker", "C").at(2, ChronoUnit.MICROS);
                    long fsn = sender.flushAndGetSequence();
                    Assert.assertTrue(fsn >= 0);
                    Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
                }
                drainWalQueue();
                long cValue = target == Target.TIMESTAMP ? 1 : 1_000;
                assertQuery("select marker, cast(value as long) value from " + table + " order by ts")
                        .noLeakCheck().returnsOnce("marker\tvalue\nA\t0\nC\t" + cValue + "\n");
            }
        });
    }

    @Test
    public void testRuntimeDependentCasesMatchCurrentServerParser() throws Exception {
        List<Vector> vectors = readRuntimeVectors();
        runInContext(port -> {
            for (Target target : Target.values()) {
                execute("create table " + target.table + "_runtime (case_id long, value " + target.sqlType
                        + ", ts timestamp) timestamp(ts) partition by day wal");
            }
            try (QwpWebSocketSender sender = connectWs(
                    port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                for (Target target : Target.values()) {
                    int caseId = 0;
                    for (Vector vector : vectors) {
                        if (vector.target == target) {
                            sender.table(target.table + "_runtime").longColumn("case_id", caseId);
                            if (vector.invalid) {
                                LineSenderSchemaException error = Assert.assertThrows(vector.caseId,
                                        LineSenderSchemaException.class,
                                        () -> sender.stringColumn("value", vector.input));
                                Assert.assertEquals(vector.caseId,
                                        LineSenderSchemaException.Reason.INVALID_VALUE, error.getReason());
                            } else {
                                sender.stringColumn("value", vector.input)
                                        .at(1_000_000L + caseId++, ChronoUnit.MICROS);
                            }
                        }
                    }
                }
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(fsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }
            drainWalQueue();
            for (Target target : Target.values()) {
                try (RecordCursorFactory factory = select("select case_id, value from " + target.table
                        + "_runtime order by case_id");
                     RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Record record = cursor.getRecord();
                    int row = 0;
                    for (Vector vector : vectors) {
                        if (vector.target == target && !vector.invalid) {
                            Assert.assertTrue(vector.caseId, cursor.hasNext());
                            Assert.assertEquals(row++, record.getLong(0));
                            Assert.assertEquals(vector.caseId, vector.expectedRaw, record.getTimestamp(1));
                        }
                    }
                    Assert.assertFalse(cursor.hasNext());
                }
                StringBuilder expectedNulls = new StringBuilder("case_id\tn\n");
                int row = 0;
                for (Vector vector : vectors) {
                    if (vector.target == target && !vector.invalid) {
                        expectedNulls.append(row++).append("\tfalse\n");
                    }
                }
                assertQuery("select case_id, value is null n from " + target.table
                        + "_runtime order by case_id").noLeakCheck().returnsOnce(expectedNulls.toString());
            }
        });
    }

    @Test
    public void testLegacyVarcharWireDistinguishesParsedSentinelFromBitmapNulls() throws Exception {
        runInContext(port -> {
            execute("create table legacy_string_ts_wire (value timestamp, ts timestamp) "
                    + "timestamp(ts) partition by day wal");
            try (WebSocketClient client = connectLegacy(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = new QwpTableBuffer("legacy_string_ts_wire")) {
                QwpTableBuffer.ColumnBuffer value = table.getOrCreateColumn(
                        "value", QwpConstants.TYPE_VARCHAR, true);
                value.addString("2024-01-02T03:04:05.1234Z");
                table.nextRow();
                value.addString("-292278-01-01 00:00:00.000Z");
                table.nextRow();
                value.addNull();
                table.nextRow();
                table.nextRow();
                int length = encoder.encode(table);
                assertRawLegacyVarcharLayout(encoder, length);
                Assert.assertEquals(0, Unsafe.getByte(encoder.getBuffer().getBufferPtr()
                        + QwpConstants.HEADER_OFFSET_FLAGS) & QwpConstants.FLAG_SCHEMA);
                QwpMessageCursor message = new QwpMessageCursor();
                message.of(encoder.getBuffer().getBufferPtr(), length, new ObjList<>());
                Assert.assertTrue(message.hasNextTable());
                QwpTableBlockCursor wire = message.nextTable();
                Assert.assertFalse(message.hasNextTable());
                Assert.assertEquals(4, wire.getRowCount());
                Assert.assertEquals(1, wire.getColumnCount());
                Assert.assertEquals("value", wire.getColumnDef(0).getName());
                Assert.assertEquals(QwpConstants.TYPE_VARCHAR, wire.getColumnDef(0).getTypeCode());
                QwpStringColumnCursor values = wire.getStringColumn(0);
                for (int row = 0; row < 4; row++) {
                    Assert.assertTrue(wire.hasNextRow());
                    wire.nextRow();
                    Assert.assertEquals(row >= 2, wire.isColumnNull(0));
                    if (row == 0) {
                        assertUtf8("2024-01-02T03:04:05.1234Z", values.getUtf8Value());
                    } else if (row == 1) {
                        assertUtf8("-292278-01-01 00:00:00.000Z", values.getUtf8Value());
                    }
                }
                Assert.assertFalse(wire.hasNextRow());
                client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                assertOk(client);
            }
            drainWalQueue();
            assertQuery("select cast(value as long) value, value is null n from legacy_string_ts_wire")
                    .noLeakCheck().returnsOnce("value\tn\n"
                            + "1704164645234000\tfalse\n"
                            + "null\ttrue\n"
                            + "null\ttrue\n"
                            + "null\ttrue\n");
        });
    }

    private static void assertRawLegacyVarcharLayout(QwpWebSocketEncoder encoder, int length) {
        String first = "2024-01-02T03:04:05.1234Z";
        String second = "-292278-01-01 00:00:00.000Z";
        byte[] firstBytes = first.getBytes(StandardCharsets.UTF_8);
        byte[] secondBytes = second.getBytes(StandardCharsets.UTF_8);
        RawReader reader = new RawReader(encoder.getBuffer().getBufferPtr(), length);
        Assert.assertEquals(0, Unsafe.getByte(encoder.getBuffer().getBufferPtr()
                + QwpConstants.HEADER_OFFSET_FLAGS) & QwpConstants.FLAG_SCHEMA);
        reader.skip(QwpConstants.HEADER_SIZE);
        Assert.assertEquals("legacy_string_ts_wire", reader.string());
        Assert.assertEquals(4, reader.varint());
        Assert.assertEquals(1, reader.varint());
        Assert.assertEquals("value", reader.string());
        Assert.assertEquals(QwpConstants.TYPE_VARCHAR, reader.u8());
        Assert.assertEquals(1, reader.u8());
        Assert.assertEquals(0x0c, reader.u8());
        Assert.assertEquals(0, reader.i32());
        Assert.assertEquals(firstBytes.length, reader.i32());
        Assert.assertEquals(firstBytes.length + secondBytes.length, reader.i32());
        reader.bytes(firstBytes);
        reader.bytes(secondBytes);
        Assert.assertEquals(length, reader.position);
    }

    private void assertDeterministicRows(Target target, List<Vector> vectors) throws Exception {
        try (RecordCursorFactory factory = select("select case_id, value from " + target.table + " order by case_id");
             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Record record = cursor.getRecord();
            int row = 0;
            for (Vector vector : vectors) {
                if (vector.target == target && !vector.invalid) {
                    Assert.assertTrue(vector.caseId, cursor.hasNext());
                    Assert.assertEquals(row++, record.getLong(0));
                    Assert.assertEquals(vector.caseId, vector.expectedRaw, record.getTimestamp(1));
                }
            }
            Assert.assertFalse(cursor.hasNext());
        }
        StringBuilder expected = new StringBuilder("case_id\tn\n");
        int row = 0;
        for (Vector vector : vectors) {
            if (vector.target == target && !vector.invalid) {
                expected.append(row++).append('\t').append(vector.sqlNull).append('\n');
            }
        }
        assertQuery("select case_id, value is null n from " + target.table + " order by case_id")
                .noLeakCheck().returnsOnce(expected.toString());
    }

    private static List<Vector> readVectors(String resource) throws Exception {
        InputStream stream = QwpSchemaStringTimestampE2ETest.class.getResourceAsStream(resource);
        Assert.assertNotNull(resource, stream);
        List<Vector> vectors = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            String line = reader.readLine();
            Assert.assertEquals("case_id\ttarget\tinput_kind\tinput\toutcome\texpected_raw\tfeature", line);
            while ((line = reader.readLine()) != null) {
                String[] fields = line.split("\\t", -1);
                Assert.assertEquals(line, 7, fields.length);
                Assert.assertTrue(line, "TEXT".equals(fields[2]) || "UTF16_HEX".equals(fields[2]));
                Assert.assertTrue(line, "VALID".equals(fields[4]) || "NULL".equals(fields[4]) || "INVALID".equals(fields[4]));
                Target target = Target.valueOf(fields[1]);
                String input = "UTF16_HEX".equals(fields[2]) ? decodeUtf16(fields[3])
                        : "<NULL>".equals(fields[3]) ? null : fields[3];
                boolean invalid = "INVALID".equals(fields[4]);
                boolean sqlNull = "NULL".equals(fields[4])
                        || (!invalid && "-9223372036854775808".equals(fields[5]));
                long expected = sqlNull ? Long.MIN_VALUE : invalid ? 0 : Long.parseLong(fields[5]);
                vectors.add(new Vector(fields[0], target, input, invalid, sqlNull, expected));
            }
        }
        Assert.assertEquals("deterministic corpus row count", 110, vectors.size());
        assertDeterministicCoverage(vectors);
        return vectors;
    }

    private static void assertDeterministicCoverage(List<Vector> vectors) {
        int valid = 0;
        int invalid = 0;
        int nulls = 0;
        int micros = 0;
        int nanos = 0;
        for (Vector vector : vectors) {
            if (vector.invalid) invalid++;
            else valid++;
            if (vector.sqlNull) nulls++;
            if (vector.target == Target.TIMESTAMP) micros++;
            else nanos++;
        }
        Assert.assertTrue("valid vectors", valid > 0);
        Assert.assertTrue("invalid vectors", invalid > 0);
        Assert.assertTrue("null vectors", nulls >= 2);
        Assert.assertTrue("micro vectors", micros > 0);
        Assert.assertTrue("nano vectors", nanos > 0);
    }

    private static List<Vector> readRuntimeVectors() throws Exception {
        InputStream stream = QwpSchemaStringTimestampE2ETest.class.getResourceAsStream(RUNTIME_VECTORS);
        Assert.assertNotNull(RUNTIME_VECTORS, stream);
        List<Vector> vectors = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            Assert.assertEquals("case_id\ttarget\tinput_kind\tinput\tfeature", reader.readLine());
            String line;
            while ((line = reader.readLine()) != null) {
                String[] fields = line.split("\\t", -1);
                Assert.assertEquals(line, 5, fields.length);
                Assert.assertEquals(line, Target.TIMESTAMP.name(), fields[1]);
                Assert.assertEquals(line, "TEXT", fields[2]);
                boolean invalid = false;
                long expected = 0;
                try {
                    expected = MicrosFormatUtils.parseTimestamp(fields[3]);
                } catch (NumericException e) {
                    invalid = true;
                }
                vectors.add(new Vector(fields[0] + "_micro", Target.TIMESTAMP, fields[3], invalid, false, expected));
                boolean nanoInvalid = invalid || expected > Long.MAX_VALUE / 1000
                        || expected < Long.MIN_VALUE / 1000;
                vectors.add(new Vector(fields[0] + "_nano", Target.TIMESTAMP_NANOS, fields[3], nanoInvalid,
                        false, nanoInvalid ? 0 : expected * 1000));
            }
        }
        Assert.assertEquals("seven runtime inputs exercise both targets", 14, vectors.size());
        return vectors;
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

    private static void assertUtf8(String expected, Utf8Sequence actual) {
        Assert.assertNotNull(actual);
        byte[] bytes = expected.getBytes(StandardCharsets.UTF_8);
        Assert.assertEquals(bytes.length, actual.size());
        for (int i = 0; i < bytes.length; i++) {
            Assert.assertEquals("byte " + i, bytes[i], actual.byteAt(i));
        }
    }

    private static void assertOk(WebSocketClient client) {
        AtomicReference<WebSocketResponse> response = new AtomicReference<>();
        Assert.assertTrue(client.receiveFrame(new WebSocketFrameHandler() {
            @Override
            public void onBinaryMessage(long payloadPtr, int payloadLen) {
                WebSocketResponse parsed = new WebSocketResponse();
                Assert.assertTrue(parsed.readFrom(payloadPtr, payloadLen));
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

    private static String decodeUtf16(String hex) {
        String[] units = hex.split(",", -1);
        StringBuilder sink = new StringBuilder(units.length);
        for (String unit : units) {
            sink.append((char) Integer.parseInt(unit, 16));
        }
        return sink.toString();
    }

    private enum Target {
        TIMESTAMP("micro", "timestamp", ChronoUnit.MICROS),
        TIMESTAMP_NANOS("nano", "timestamp_ns", ChronoUnit.NANOS);

        private final String suffix;
        private final String sqlType;
        private final String table;
        private final ChronoUnit unit;

        Target(String suffix, String sqlType, ChronoUnit unit) {
            this.suffix = suffix;
            this.sqlType = sqlType;
            this.table = "schema_string_ts_" + suffix;
            this.unit = unit;
        }
    }

    private static final class Vector {
        private final String caseId;
        private final long expectedRaw;
        private final String input;
        private final boolean invalid;
        private final boolean sqlNull;
        private final Target target;

        private Vector(String caseId, Target target, String input, boolean invalid, boolean sqlNull, long expectedRaw) {
            this.caseId = caseId;
            this.target = target;
            this.input = input;
            this.invalid = invalid;
            this.sqlNull = sqlNull;
            this.expectedRaw = expectedRaw;
        }
    }

    private static final class RawReader {
        private final long address;
        private final int limit;
        private int position;

        private RawReader(long address, int limit) {
            this.address = address;
            this.limit = limit;
        }

        private void bytes(byte[] expected) {
            Assert.assertTrue(position + expected.length <= limit);
            for (int i = 0; i < expected.length; i++) {
                Assert.assertEquals("raw byte " + i, expected[i], Unsafe.getByte(address + position++));
            }
        }

        private int i32() {
            Assert.assertTrue(position + Integer.BYTES <= limit);
            int value = Unsafe.getUnsafe().getInt(address + position);
            position += Integer.BYTES;
            return value;
        }

        private void skip(int count) {
            position += count;
            Assert.assertTrue(position <= limit);
        }

        private String string() {
            int length = varint();
            Assert.assertTrue(position + length <= limit);
            byte[] bytes = new byte[length];
            for (int i = 0; i < length; i++) {
                bytes[i] = Unsafe.getByte(address + position++);
            }
            return new String(bytes, StandardCharsets.UTF_8);
        }

        private int u8() {
            Assert.assertTrue(position < limit);
            return Unsafe.getByte(address + position++) & 0xff;
        }

        private int varint() {
            int value = 0;
            int shift = 0;
            int b;
            do {
                b = u8();
                value |= (b & 0x7f) << shift;
                shift += 7;
            } while ((b & 0x80) != 0);
            return value;
        }
    }
}
