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
import io.questdb.cutlass.qwp.protocol.QwpTableBlockCursor;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
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

public class QwpSchemaLongTimestampE2ETest extends AbstractQwpWebSocketTest {
    private static final String CORPUS = "/io/questdb/client/cutlass/qwp/long-to-timestamp.tsv";

    @Test
    public void testLegacyLongMinIsTimestampNullWithAndWithoutBitmap() throws Exception {
        runInContext(port -> {
            for (Target target : Target.values()) {
                for (boolean bitmap : new boolean[]{false, true}) {
                    String tableName = "legacy_long_" + target.suffix + (bitmap ? "_bitmap" : "_plain");
                    createTable(tableName, "value " + target.sqlType + ", ts timestamp");
                    try (WebSocketClient client = connectLegacy(port);
                         QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                         QwpTableBuffer table = new QwpTableBuffer(tableName)) {
                        table.getOrCreateColumn("value", QwpConstants.TYPE_LONG, true).addLong(Long.MIN_VALUE);
                        table.nextRow();
                        if (bitmap) {
                            table.nextRow();
                        }
                        int length = encoder.encode(table);
                        QwpTableBlockCursor wire = parseSingleTable(encoder, length);
                        Assert.assertEquals(QwpConstants.TYPE_LONG, wire.getColumnDef(0).getTypeCode());
                        QwpFixedWidthColumnCursor values = wire.getFixedWidthColumn(0);
                        Assert.assertEquals(bitmap, values.getNullBitmapAddress() != 0);
                        Assert.assertEquals(1, values.getValueCount());
                        Assert.assertTrue(wire.hasNextRow());
                        wire.nextRow();
                        Assert.assertEquals(Long.MIN_VALUE, values.getLong());
                        if (bitmap) {
                            Assert.assertFalse("supplied LONG_MIN is present on the source wire", wire.isColumnNull(0));
                            Assert.assertTrue(wire.hasNextRow());
                            wire.nextRow();
                            Assert.assertTrue("second row is omitted", wire.isColumnNull(0));
                        }
                        Assert.assertFalse(wire.hasNextRow());
                        client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                        assertOk(client);
                    }
                    drainWalQueue();
                    assertQuery("select cast(value as long) value, value is null n from " + tableName)
                            .noLeakCheck().returnsOnce(bitmap
                                    ? "value\tn\nnull\ttrue\nnull\ttrue\n"
                                    : "value\tn\nnull\ttrue\n");
                }
            }
        });
    }

    @Test
    public void testPublicSenderCorpusStoresRawTargetUnitCounts() throws Exception {
        List<Vector> vectors = readVectors();
        runInContext(port -> {
            for (Target target : Target.values()) {
                createTable(target.tableName, "case_id long, value " + target.sqlType + ", ts timestamp");
            }
            try (QwpWebSocketSender sender = connectWs(
                    port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                for (Target target : Target.values()) {
                    int caseId = 0;
                    for (Vector vector : vectors) {
                        if (vector.target == target) {
                            sender.table(target.tableName)
                                    .longColumn("case_id", caseId++)
                                    .longColumn("value", vector.input)
                                    .at(1_000_000L + caseId, ChronoUnit.MICROS);
                        }
                    }
                }
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(fsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }
            drainWalQueue();
            for (Target target : Target.values()) {
                assertStoredRawValues(target, vectors);
                StringBuilder expected = new StringBuilder("case_id\tvalue\tn\n");
                int caseId = 0;
                for (Vector vector : vectors) {
                    if (vector.target == target) {
                        expected.append(caseId++).append('\t')
                                .append(vector.sqlNull ? "null" : vector.expectedWire).append('\t')
                                .append(vector.sqlNull).append('\n');
                    }
                }
                assertQuery("select case_id, cast(value as long) value, value is null n from "
                        + target.tableName + " order by case_id")
                        .noLeakCheck().returnsOnce(expected.toString());
            }
        });
    }

    private void assertStoredRawValues(Target target, List<Vector> vectors) throws Exception {
        try (RecordCursorFactory factory = select("select case_id, value from " + target.tableName
                + " order by case_id");
             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Record record = cursor.getRecord();
            int caseId = 0;
            for (Vector vector : vectors) {
                if (vector.target == target) {
                    Assert.assertTrue(vector.caseId, cursor.hasNext());
                    Assert.assertEquals(vector.caseId, caseId++, record.getLong(0));
                    Assert.assertEquals(vector.caseId, vector.expectedWire, record.getTimestamp(1));
                }
            }
            Assert.assertFalse(cursor.hasNext());
        }
    }

    @Test
    public void testPublicSenderRollsBackInvalidRowAndRejectsNamedDesignatedTimestamp() throws Exception {
        runInContext(port -> {
            for (Target target : Target.values()) {
                String tableName = "schema_long_ts_rows_" + target.suffix;
                createTable(tableName, "value " + target.sqlType + ", marker string, bad uuid, ts "
                        + target.sqlType);
                try (QwpWebSocketSender sender = connectWs(
                        port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                    sender.table(tableName)
                            .longColumn("value", Long.MIN_VALUE)
                            .binaryColumn("value", new byte[]{1})
                            .stringColumn("marker", "A")
                            .at(1, target.unit);

                    sender.table(tableName).stringColumn("marker", "failed-B");
                    LineSenderSchemaException invalid = Assert.assertThrows(
                            LineSenderSchemaException.class,
                            () -> sender.longColumn("bad", 1)
                    );
                    Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, invalid.getReason());

                    sender.table(tableName).longColumn("value", Long.MAX_VALUE)
                            .stringColumn("marker", "C")
                            .at(2, target.unit);

                    sender.table(tableName).stringColumn("marker", "failed-designated");
                    LineSenderSchemaException designated = Assert.assertThrows(
                            LineSenderSchemaException.class,
                            () -> sender.longColumn("ts", 3)
                    );
                    Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, designated.getReason());

                    sender.table(tableName).stringColumn("marker", "D").at(4, target.unit);
                    long fsn = sender.flushAndGetSequence();
                    Assert.assertTrue(fsn >= 0);
                    Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
                }
                drainWalQueue();
                assertQuery("select marker, cast(value as long) value, value is null n, cast(ts as long) ts from "
                        + tableName + " order by ts")
                        .noLeakCheck().returnsOnce("marker\tvalue\tn\tts\n"
                                + "A\tnull\ttrue\t1\n"
                                + "C\t" + Long.MAX_VALUE + "\tfalse\t2\n"
                                + "D\tnull\ttrue\t4\n");
            }
        });
    }

    private void createTable(String tableName, String columns) throws Exception {
        execute("create table " + tableName + " (" + columns + ") timestamp(ts) partition by day wal");
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

    private static QwpTableBlockCursor parseSingleTable(QwpWebSocketEncoder encoder, int length) throws Exception {
        Assert.assertEquals(0, Unsafe.getByte(encoder.getBuffer().getBufferPtr()
                + QwpConstants.HEADER_OFFSET_FLAGS) & QwpConstants.FLAG_SCHEMA);
        QwpMessageCursor cursor = new QwpMessageCursor();
        cursor.of(encoder.getBuffer().getBufferPtr(), length, new ObjList<>());
        Assert.assertTrue(cursor.hasNextTable());
        QwpTableBlockCursor table = cursor.nextTable();
        Assert.assertFalse(cursor.hasNextTable());
        return table;
    }

    private static List<Vector> readVectors() throws Exception {
        InputStream stream = QwpSchemaLongTimestampE2ETest.class.getResourceAsStream(CORPUS);
        Assert.assertNotNull(CORPUS, stream);
        List<Vector> vectors = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty() || line.charAt(0) == '#') {
                    continue;
                }
                String[] fields = line.split("\\t", -1);
                Assert.assertEquals(line, 6, fields.length);
                Target target = Target.valueOf(fields[2]);
                Assert.assertEquals(line, target.wireName, fields[3]);
                long input = Long.parseLong(fields[1]);
                boolean sqlNull = Boolean.parseBoolean(fields[5]);
                Assert.assertEquals(line, input == Long.MIN_VALUE, sqlNull);
                long expectedWire = sqlNull ? Long.MIN_VALUE : Long.parseLong(fields[4]);
                Assert.assertEquals(line, input, expectedWire);
                vectors.add(new Vector(fields[0], input, target, expectedWire, sqlNull));
            }
        }
        Assert.assertEquals(12, vectors.size());
        for (Target target : Target.values()) {
            int count = 0;
            for (Vector vector : vectors) {
                if (vector.target == target) {
                    count++;
                }
            }
            Assert.assertEquals(target.name(), 6, count);
        }
        return vectors;
    }

    private enum Target {
        TIMESTAMP("micro", "timestamp", "TIMESTAMP", ChronoUnit.MICROS),
        TIMESTAMP_NANOS("nano", "timestamp_ns", "TIMESTAMP_NANOS", ChronoUnit.NANOS);

        private final String suffix;
        private final String sqlType;
        private final String tableName;
        private final ChronoUnit unit;
        private final String wireName;

        Target(String suffix, String sqlType, String wireName, ChronoUnit unit) {
            this.suffix = suffix;
            this.sqlType = sqlType;
            this.tableName = "schema_long_ts_" + suffix;
            this.unit = unit;
            this.wireName = wireName;
        }
    }

    private static final class Vector {
        private final String caseId;
        private final long expectedWire;
        private final long input;
        private final boolean sqlNull;
        private final Target target;

        private Vector(String caseId, long input, Target target, long expectedWire, boolean sqlNull) {
            this.caseId = caseId;
            this.input = input;
            this.target = target;
            this.expectedWire = expectedWire;
            this.sqlNull = sqlNull;
        }
    }
}
