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
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpMessageCursor;
import io.questdb.cutlass.qwp.protocol.QwpTableBlockCursor;
import io.questdb.cutlass.qwp.protocol.QwpTimestampColumnCursor;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class QwpSchemaTimestampTextE2ETest extends AbstractQwpWebSocketTest {
    private static final String CORPUS = "/io/questdb/test/cutlass/qwp/e2e/timestamp-to-text.tsv";

    @Test
    public void testPublicSenderCorpusStoresExactTimestampText() throws Exception {
        List<Vector> vectors = readVectors();
        runInContext(port -> {
            for (Target target : Target.values()) {
                execute("create table schema_timestamp_text_" + target.name().toLowerCase(Locale.ROOT)
                        + " (case_id long, v " + target.sqlType
                        + ", ts timestamp) timestamp(ts) partition by day wal");
            }
            try (QwpWebSocketSender sender = connectWs(
                    port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                for (Target target : Target.values()) {
                    String tableName = "schema_timestamp_text_" + target.name().toLowerCase(Locale.ROOT);
                    long row = 0;
                    for (Vector vector : vectors) {
                        sender.table(tableName).longColumn("case_id", row);
                        if (vector.valid) {
                            vector.append(sender, "v");
                            sender.at(++row, ChronoUnit.MICROS);
                        } else {
                            LineSenderSchemaException error = Assert.assertThrows(
                                    LineSenderSchemaException.class,
                                    () -> vector.append(sender, "v")
                            );
                            Assert.assertEquals(vector.caseId, LineSenderSchemaException.Reason.INVALID_VALUE, error.getReason());
                            Assert.assertTrue(vector.caseId, error.getMessage().contains("column=v"));
                            Assert.assertTrue(vector.caseId, error.getMessage().contains("targetType=" + target.name()));
                        }
                    }
                }
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(fsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }
            drainWalQueue();
            for (Target target : Target.values()) {
                StringBuilder expected = new StringBuilder("case_id\tv\tn\n");
                long row = 0;
                for (Vector vector : vectors) {
                    if (vector.valid) {
                        expected.append(row++).append('\t').append(vector.expected).append("\tfalse\n");
                    }
                }
                assertQuery("select case_id, v, v is null n from schema_timestamp_text_"
                        + target.name().toLowerCase(Locale.ROOT) + " order by case_id")
                        .noLeakCheck().returnsOnce(expected.toString());
            }
        });
    }

    @Test
    public void testLegacyTimestampMinRemainsPresentWithAndWithoutBitmap() throws Exception {
        runInContext(port -> {
            for (Source source : Source.values()) {
                for (Target target : Target.values()) {
                    for (boolean bitmap : new boolean[]{false, true}) {
                        assertLegacyTimestampMin(port, source, target, bitmap);
                    }
                }
            }
        });
    }

    @Test
    public void testPublicSenderRejectsOverflowAndRollsBackWholeRow() throws Exception {
        runInContext(port -> {
            execute("create table schema_timestamp_text_rows "
                    + "(v varchar, marker string, ts timestamp) timestamp(ts) partition by day wal");
            try (QwpWebSocketSender sender = connectWs(
                    port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                sender.table("schema_timestamp_text_rows")
                        .timestampColumn("v", Long.MIN_VALUE, ChronoUnit.MICROS)
                        // Duplicate suppression precedes conversion, so this overflow is ignored.
                        .timestampColumn("v", Long.MAX_VALUE, ChronoUnit.DAYS)
                        .stringColumn("marker", "A")
                        .at(1_000_000, ChronoUnit.MICROS);

                sender.table("schema_timestamp_text_rows").stringColumn("marker", "failed-B");
                LineSenderSchemaException overflow = Assert.assertThrows(
                        LineSenderSchemaException.class,
                        () -> sender.timestampColumn("v", Long.MAX_VALUE, ChronoUnit.DAYS)
                );
                Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, overflow.getReason());
                Assert.assertFalse(overflow.isRetryable());
                Assert.assertTrue(overflow.getMessage(), overflow.getMessage().contains("column=v"));
                Assert.assertTrue(overflow.getMessage(), overflow.getMessage().contains("table=schema_timestamp_text_rows"));
                Assert.assertTrue(overflow.getMessage(), overflow.getMessage().contains("inputType=TIMESTAMP"));
                Assert.assertTrue(overflow.getMessage(), overflow.getMessage().contains("targetType=VARCHAR"));

                sender.table("schema_timestamp_text_rows").timestampColumn("v", 1, ChronoUnit.SECONDS)
                        .stringColumn("marker", "C")
                        .at(2_000_000, ChronoUnit.MICROS);
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(fsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }
            drainWalQueue();
            assertQuery("select v, v is null n, marker, ts from schema_timestamp_text_rows order by ts")
                    .noLeakCheck().returnsOnce("v\tn\tmarker\tts\n"
                            + "\tfalse\tA\t1970-01-01T00:00:01.000000Z\n"
                            + "1970-01-01T00:00:01.000Z\tfalse\tC\t1970-01-01T00:00:02.000000Z\n");
        });
    }

    @Test
    public void testPublicSenderRejectsNamedDesignatedTimestampAndNullInstant() throws Exception {
        runInContext(port -> {
            execute("create table schema_timestamp_text_guards "
                    + "(v string, marker string, ts timestamp) timestamp(ts) partition by day wal");
            try (QwpWebSocketSender sender = connectWs(
                    port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                sender.table("schema_timestamp_text_guards").stringColumn("marker", "failed-A");
                LineSenderSchemaException designated = Assert.assertThrows(
                        LineSenderSchemaException.class,
                        () -> sender.timestampColumn("ts", 1, ChronoUnit.MICROS)
                );
                Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, designated.getReason());
                Assert.assertTrue(designated.getMessage(), designated.getMessage().contains("column=ts"));

                sender.table("schema_timestamp_text_guards").stringColumn("marker", "failed-B");
                LineSenderSchemaException nullInstant = Assert.assertThrows(
                        LineSenderSchemaException.class,
                        () -> sender.timestampColumn("v", (Instant) null)
                );
                Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, nullInstant.getReason());
                Assert.assertTrue(nullInstant.getMessage(), nullInstant.getMessage().contains("column=v"));

                sender.table("schema_timestamp_text_guards").timestampColumn("v", 2, ChronoUnit.SECONDS)
                        .stringColumn("marker", "C")
                        .at(3_000_000, ChronoUnit.MICROS);
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(fsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }
            drainWalQueue();
            assertQuery("select v, marker, ts from schema_timestamp_text_guards")
                    .noLeakCheck().returnsOnce("v\tmarker\tts\n"
                            + "1970-01-01T00:00:02.000Z\tC\t1970-01-01T00:00:03.000000Z\n");
        });
    }

    private void assertLegacyTimestampMin(int port, Source source, Target target, boolean bitmap) throws Exception {
        String tableName = "legacy_ts_text_" + source.suffix + '_' + target.name().toLowerCase(Locale.ROOT)
                + (bitmap ? "_bitmap" : "_plain");
        execute("create table " + tableName + " (v " + target.sqlType
                + ", ts timestamp) timestamp(ts) partition by day wal");
        try (WebSocketClient client = WebSocketClientFactory.newPlainTextInstance();
             QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
             QwpTableBuffer table = new QwpTableBuffer(tableName)) {
            connectLegacy(client, port);
            table.getOrCreateColumn("v", source.wireType, true).addLong(Long.MIN_VALUE);
            table.nextRow();
            if (bitmap) {
                table.nextRow();
            }
            int length = encoder.encode(table);
            long frame = encoder.getBuffer().getBufferPtr();
            Assert.assertEquals(0, Unsafe.getByte(frame + QwpConstants.HEADER_OFFSET_FLAGS)
                    & QwpConstants.FLAG_SCHEMA);
            QwpMessageCursor message = new QwpMessageCursor();
            message.of(frame, length, new ObjList<>());
            Assert.assertTrue(message.hasNextTable());
            QwpTableBlockCursor wire = message.nextTable();
            Assert.assertFalse(message.hasNextTable());
            Assert.assertEquals(tableName, wire.getTableName());
            Assert.assertEquals(bitmap ? 2 : 1, wire.getRowCount());
            Assert.assertEquals(1, wire.getColumnCount());
            Assert.assertEquals("v", wire.getColumnDef(0).getName());
            Assert.assertEquals(source.wireType, wire.getColumnDef(0).getTypeCode());
            QwpTimestampColumnCursor values = wire.getTimestampColumn(0);
            Assert.assertEquals(1, values.getValueCount());
            Assert.assertEquals(bitmap, values.getNullBitmapAddress() != 0);
            Assert.assertTrue(values.supportsDirectAccess());
            Assert.assertEquals(Long.MIN_VALUE, Unsafe.getLong(values.getValuesAddress()));
            Assert.assertTrue(wire.hasNextRow());
            wire.nextRow();
            Assert.assertFalse("supplied timestamp MIN is present", wire.isColumnNull(0));
            Assert.assertEquals(Long.MIN_VALUE, values.getTimestamp());
            if (bitmap) {
                Assert.assertTrue(wire.hasNextRow());
                wire.nextRow();
                Assert.assertTrue("omitted timestamp is bitmap-null", wire.isColumnNull(0));
            }
            Assert.assertFalse(wire.hasNextRow());
            client.sendBinary(frame, length);
            assertOk(client);
        }
        drainWalQueue();
        String expectedValue = source == Source.MICRO ? "" : "1677-09-21T00:12:43.145Z";
        String expected = "v\tn\n" + expectedValue + "\tfalse\n";
        if (bitmap) {
            expected += "\ttrue\n";
        }
        assertQuery("select v, v is null n from " + tableName + " order by n")
                .noLeakCheck().returnsOnce(expected);
    }

    private static List<Vector> readVectors() throws Exception {
        InputStream stream = QwpSchemaTimestampTextE2ETest.class.getResourceAsStream(CORPUS);
        Assert.assertNotNull(CORPUS, stream);
        List<Vector> vectors = new ArrayList<>();
        int invalidCount = 0;
        int valueCount = 0;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            String header = reader.readLine();
            Assert.assertEquals("# case_id\tinput_kind\tunit\tvalue\tnanos\toutcome\texpected_text", header);
            String line;
            while ((line = reader.readLine()) != null) {
                String[] fields = line.split("\t", -1);
                Assert.assertEquals(line, 7, fields.length);
                String kind = fields[1];
                Assert.assertTrue(line, "LONG".equals(kind) || "INSTANT".equals(kind));
                boolean valid;
                if ("VALUE".equals(fields[5])) {
                    valid = true;
                    valueCount++;
                } else {
                    Assert.assertEquals(line, "INVALID", fields[5]);
                    valid = false;
                    invalidCount++;
                }
                String expected = "<EMPTY>".equals(fields[6]) ? "" : fields[6];
                if (valid) {
                    Assert.assertFalse(line, fields[6].isEmpty());
                } else {
                    Assert.assertTrue(line, fields[6].isEmpty());
                }
                vectors.add(new Vector(
                        fields[0],
                        kind,
                        fields[2].isEmpty() ? null : ChronoUnit.valueOf(fields[2]),
                        Long.parseLong(fields[3]),
                        fields[4].isEmpty() ? 0 : Integer.parseInt(fields[4]),
                        valid,
                        expected
                ));
            }
        }
        Assert.assertEquals(63, vectors.size());
        Assert.assertEquals(50, valueCount);
        Assert.assertEquals(13, invalidCount);
        return vectors;
    }

    private static void connectLegacy(WebSocketClient client, int port) throws Exception {
        client.connect("127.0.0.1", port);
        client.upgrade("/write/v4", null);
        Assert.assertFalse(client.isQwpSchemaEnabled());
    }

    private static void assertOk(WebSocketClient client) {
        AtomicReference<WebSocketResponse> response = new AtomicReference<>();
        Assert.assertTrue(client.receiveFrame(new WebSocketFrameHandler() {
            @Override
            public void onBinaryMessage(long ptr, int len) {
                WebSocketResponse value = new WebSocketResponse();
                Assert.assertTrue(value.readFrom(ptr, len));
                response.set(value);
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

    private enum Source {
        MICRO(QwpConstants.TYPE_TIMESTAMP, "micro"),
        NANO(QwpConstants.TYPE_TIMESTAMP_NANOS, "nano");

        private final String suffix;
        private final byte wireType;

        Source(byte wireType, String suffix) {
            this.wireType = wireType;
            this.suffix = suffix;
        }
    }

    private enum Target {
        STRING("string"),
        VARCHAR("varchar");

        private final String sqlType;

        Target(String sqlType) {
            this.sqlType = sqlType;
        }
    }

    private static final class Vector {
        private final String caseId;
        private final String expected;
        private final String kind;
        private final int nanos;
        private final ChronoUnit unit;
        private final boolean valid;
        private final long value;

        private Vector(
                String caseId,
                String kind,
                ChronoUnit unit,
                long value,
                int nanos,
                boolean valid,
                String expected
        ) {
            this.caseId = caseId;
            this.kind = kind;
            this.unit = unit;
            this.value = value;
            this.nanos = nanos;
            this.valid = valid;
            this.expected = expected;
        }

        private void append(QwpWebSocketSender sender, String column) {
            if ("LONG".equals(kind)) {
                sender.timestampColumn(column, value, unit);
            } else {
                sender.timestampColumn(column, Instant.ofEpochSecond(value, nanos));
            }
        }
    }
}
