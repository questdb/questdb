/*+*****************************************************************************
 * Copyright (c) 2014-2019 Appsicle
 * Copyright (c) 2019-2026 QuestDB
 * Licensed under the Apache License, Version 2.0
 *******************************************************************************/
package io.questdb.test.cutlass.qwp.e2e;

import io.questdb.client.LineSenderSchemaException;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaBinding;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaResponse;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpMessageCursor;
import io.questdb.cutlass.qwp.protocol.QwpTableBlockCursor;
import io.questdb.std.ObjList;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static io.questdb.test.cutlass.qwp.e2e.QwpSchemaE2ETestUtils.assertUtf8;
import static io.questdb.test.cutlass.qwp.e2e.QwpSchemaE2ETestUtils.connectSchemaClient;
import static io.questdb.test.cutlass.qwp.e2e.QwpSchemaE2ETestUtils.describe;

public class QwpSchemaFloatingTextConversionE2ETest extends AbstractQwpWebSocketTest {
    private static final String CORPUS = "/io/questdb/client/cutlass/qwp/floating-to-text.tsv";

    @Test
    public void testPublicSenderCorpusStoresExactTextAndTargetNulls() throws Exception {
        List<Vector> vectors = readVectors();
        runInContext(port -> {
            for (Input input : Input.values()) {
                for (Target target : Target.values()) {
                    execute("create table " + tableName(input, target) + " (case_id long, value " + target.sqlType
                            + ", ts timestamp) timestamp(ts) partition by day wal");
                }
            }
            try (QwpWebSocketSender sender = connectWs(
                    port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                for (Input input : Input.values()) {
                    for (Target target : Target.values()) {
                        int row = 0;
                        for (Vector vector : vectors) {
                            if (vector.input == input && vector.target == target) {
                                sender.table(tableName(input, target)).longColumn("case_id", row++);
                                vector.append(sender);
                                sender.at(1_000_000L + row, ChronoUnit.MICROS);
                            }
                        }
                    }
                }
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(fsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }
            drainWalQueue();
            for (Input input : Input.values()) {
                for (Target target : Target.values()) {
                    StringBuilder expected = new StringBuilder("case_id\tvalue\tn\n");
                    int row = 0;
                    for (Vector vector : vectors) {
                        if (vector.input == input && vector.target == target) {
                            expected.append(row++).append('\t');
                            if (vector.expected != null) {
                                expected.append(vector.expected);
                            }
                            expected.append('\t').append(vector.expected == null).append('\n');
                        }
                    }
                    assertQuery("select case_id, value, value is null n from " + tableName(input, target)
                            + " order by case_id").noLeakCheck().expectSize().returns(expected.toString());
                }
            }
        });
    }

    @Test
    public void testPublicSenderNaNFirstWriteAndPartialFailureRollback() throws Exception {
        runInContext(port -> {
            execute("create table schema_floating_text_rows (s string, v varchar, marker string, bad uuid, "
                    + "ts timestamp) timestamp(ts) partition by day wal");
            try (QwpWebSocketSender sender = connectWs(
                    port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                sender.table("schema_floating_text_rows")
                        .floatColumn("s", Float.intBitsToFloat(0x7fc00002))
                        .binaryColumn("s", new byte[]{1})
                        .doubleColumn("v", -0.0)
                        .at(1_000_000, ChronoUnit.MICROS);
                sender.stringColumn("marker", "failed-B");
                LineSenderSchemaException error = Assert.assertThrows(
                        LineSenderSchemaException.class,
                        () -> sender.doubleColumn("bad", 2.5)
                );
                Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, error.getReason());
                Assert.assertFalse(error.isRetryable());
                Assert.assertTrue(error.getMessage(), error.getMessage().contains("column=bad"));
                Assert.assertTrue(error.getMessage(), error.getMessage().contains("inputType=DOUBLE"));
                Assert.assertTrue(error.getMessage(), error.getMessage().contains("targetType=UUID"));
                sender.floatColumn("s", 0.1f).doubleColumn("v", 1e23)
                        .at(2_000_000, ChronoUnit.MICROS);
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(fsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }
            drainWalQueue();
            assertQuery("select s, v, marker, bad, s is null sn, ts from schema_floating_text_rows order by ts")
                    .noLeakCheck().expectSize().timestamp("ts").returns("s\tv\tmarker\tbad\tsn\tts\n"
                            + "\t-0.0\t\t\ttrue\t1970-01-01T00:00:01.000000Z\n"
                            + "0.10000000149011612\t1.0E23\t\t\tfalse\t1970-01-01T00:00:02.000000Z\n");
        });
    }

    @Test
    public void testNativeFloatGenerationSurvivesVarcharRebind() throws Exception {
        execute("create table schema_floating_text_rebind "
                + "(value float, marker varchar, ts timestamp) timestamp(ts) partition by day wal");
        runInContext(port -> {
            try (QwpWebSocketSender sender = connectWs(
                    port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                sender.table("schema_floating_text_rebind").floatColumn("value", 1.5f)
                        .stringColumn("marker", "A").at(1_000_000, ChronoUnit.MICROS);
                execute("alter table schema_floating_text_rebind drop column value");
                execute("alter table schema_floating_text_rebind add column value varchar");
                // The pending batch still validates against the pinned FLOAT target.
                LineSenderSchemaException invalid = Assert.assertThrows(
                        LineSenderSchemaException.class,
                        () -> sender.stringColumn("marker", "B").stringColumn("value", "not-a-number")
                );
                Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, invalid.getReason());
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(fsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
                // The stale frame's ACK carried the VARCHAR schema; the next batch
                // adopts it and formats the native float as text.
                sender.table("schema_floating_text_rebind").floatColumn("value", 0.1f)
                        .stringColumn("marker", "C").at(2_000_000, ChronoUnit.MICROS);
                fsn = sender.flushAndGetSequence();
                Assert.assertTrue(fsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }
            drainWalQueue();
            assertQuery("select value, marker, ts from schema_floating_text_rebind order by marker")
                    .noLeakCheck().expectSize().returns("value\tmarker\tts\n"
                            + "1.5\tA\t1970-01-01T00:00:01.000000Z\n"
                            + "0.10000000149011612\tC\t1970-01-01T00:00:02.000000Z\n");
        });
    }

    @Test
    public void testTargetTextWireReplaysFromStoreAndForward() throws Exception {
        runInContext(port -> {
            execute("create table schema_floating_text_sf (s string, v varchar, y symbol, n string, ts timestamp) "
                    + "timestamp(ts) partition by day wal");
            File sfRoot = temp.newFolder("qwp-schema-floating-text-sf");
            String slot = new File(sfRoot, "default").getAbsolutePath();
            try (WebSocketClient client = connectSchemaClient(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = new QwpTableBuffer("schema_floating_text_sf")) {
                QwpSchemaResponse schema = describe(client, 701, "schema_floating_text_sf");
                QwpSchemaBinding binding = new QwpSchemaBinding(table, schema);
                binding.floatColumn("s", 0.1f).doubleColumn("v", 1e23).doubleColumn("y", -0.0)
                        .floatColumn("n", Float.intBitsToFloat(0x7fc00002));
                table.nextRow();
                int length = encoder.encode(table);
                QwpMessageCursor message = new QwpMessageCursor();
                message.of(encoder.getBuffer().getBufferPtr(), length, new ObjList<>());
                Assert.assertTrue(message.hasNextTable());
                QwpTableBlockCursor wire = message.nextTable();
                Assert.assertFalse(message.hasNextTable());
                Assert.assertEquals(4, wire.getColumnCount());
                Assert.assertEquals(1, wire.getRowCount());
                Assert.assertEquals(QwpConstants.TYPE_VARCHAR, wire.getColumnDef(0).getTypeCode());
                Assert.assertEquals(QwpConstants.TYPE_VARCHAR, wire.getColumnDef(1).getTypeCode());
                Assert.assertEquals(QwpConstants.TYPE_SYMBOL, wire.getColumnDef(2).getTypeCode());
                Assert.assertEquals(QwpConstants.TYPE_VARCHAR, wire.getColumnDef(3).getTypeCode());
                Assert.assertTrue(wire.hasNextRow());
                wire.nextRow();
                assertUtf8("0.10000000149011612", wire.getStringColumn(0).getUtf8Value());
                assertUtf8("1.0E23", wire.getStringColumn(1).getUtf8Value());
                assertUtf8("-0.0", wire.getSymbolColumn(2).getSymbolUtf8());
                Assert.assertTrue(wire.isColumnNull(3));
                Assert.assertFalse(wire.hasNextRow());
                try (CursorSendEngine engine = new CursorSendEngine(slot, 1 << 20)) {
                    Assert.assertEquals(0, engine.appendBlocking(encoder.getBuffer().getBufferPtr(), length));
                }
            }
            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port
                    + ";sf_dir=" + sfRoot.getAbsolutePath()
                    + ";sender_id=default;sf_max_segment_bytes=1m;")) {
                Assert.assertTrue(sender.drain(10_000));
            }
            drainWalQueue();
            assertQuery("select s, v, y, n, n is null nn from schema_floating_text_sf")
                    .noLeakCheck().expectSize().returns("s\tv\ty\tn\tnn\n"
                            + "0.10000000149011612\t1.0E23\t-0.0\t\ttrue\n");
        });
    }

    private static List<Vector> readVectors() throws Exception {
        InputStream stream = QwpSchemaFloatingTextConversionE2ETest.class.getResourceAsStream(CORPUS);
        Assert.assertNotNull(CORPUS, stream);
        List<Vector> vectors = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty() || line.charAt(0) == '#') continue;
                String[] fields = line.split("\\t", -1);
                Assert.assertEquals(line, 7, fields.length);
                Input input = Input.valueOf(fields[1]);
                Target target = Target.valueOf(fields[3]);
                Assert.assertEquals(line, target.wireType, fields[4]);
                String expected = "<NULL>".equals(fields[6]) ? null : fields[6];
                Assert.assertEquals(line, expected == null, "<NULL>".equals(fields[5]));
                if (expected != null) Assert.assertEquals(line, fields[5], toHex(expected));
                vectors.add(new Vector(input, Long.parseUnsignedLong(fields[2], 16), target, expected));
            }
        }
        Assert.assertEquals(48, vectors.size());
        for (Input input : Input.values()) {
            for (Target target : Target.values()) {
                int count = 0;
                for (Vector vector : vectors) if (vector.input == input && vector.target == target) count++;
                int expectedCount = target == Target.STRING ? 11 : input == Input.FLOAT ? 6 : 7;
                Assert.assertEquals(input + " -> " + target, expectedCount, count);
            }
        }
        return vectors;
    }

    private static String tableName(Input input, Target target) {
        return "schema_" + input.name().toLowerCase(Locale.ROOT) + '_' + target.name().toLowerCase(Locale.ROOT);
    }

    private static String toHex(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        StringBuilder sink = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sink.append(Character.forDigit((b >>> 4) & 0xf, 16));
            sink.append(Character.forDigit(b & 0xf, 16));
        }
        return sink.toString();
    }

    private enum Input {FLOAT, DOUBLE}

    private enum Target {
        STRING("STRING", "VARCHAR"), VARCHAR("VARCHAR", "VARCHAR"), SYMBOL("SYMBOL", "SYMBOL");
        private final String sqlType;
        private final String wireType;

        Target(String sqlType, String wireType) {
            this.sqlType = sqlType;
            this.wireType = wireType;
        }
    }

    private static final class Vector {
        private final String expected;
        private final Input input;
        private final long rawBits;
        private final Target target;

        private Vector(Input input, long rawBits, Target target, String expected) {
            this.input = input;
            this.rawBits = rawBits;
            this.target = target;
            this.expected = expected;
        }

        private void append(QwpWebSocketSender sender) {
            if (input == Input.FLOAT) sender.floatColumn("value", Float.intBitsToFloat((int) rawBits));
            else sender.doubleColumn("value", Double.longBitsToDouble(rawBits));
        }
    }
}
