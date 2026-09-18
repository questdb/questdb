/*+*****************************************************************************
 * Copyright (c) 2014-2019 Appsicle
 * Copyright (c) 2019-2026 QuestDB
 * Licensed under the Apache License, Version 2.0
 *******************************************************************************/
package io.questdb.test.cutlass.qwp.e2e;

import io.questdb.client.LineSenderSchemaException;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketClientFactory;
import io.questdb.client.cutlass.http.client.WebSocketFrameHandler;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
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

public class QwpSchemaFloatingDecimalE2ETest extends AbstractQwpWebSocketTest {
    private static final String CORPUS = "/io/questdb/client/cutlass/qwp/floating-to-decimal.tsv";
    private static final String HEADER = "# case_id\tsource_type\tinput_bits_hex\ttarget_type\t"
            + "target_precision\ttarget_scale\toutcome\texpected_ll_hex\texpected_lh_hex\t"
            + "expected_hl_hex\texpected_hh_hex\texpected_sql";

    @Test
    public void testMissingTableRetainsNativeFloatingTypes() throws Exception {
        runInContext(port -> {
            try (Sender sender = connectWs(
                    port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                sender.table("schema_floating_decimal_missing")
                        .floatColumn("f", 0.1f)
                        .doubleColumn("d", -0.0)
                        .atNow();
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(fsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }
            drainWalQueue();
            assertQuery("select \"column\", type from table_columns('schema_floating_decimal_missing') "
                    + "order by \"column\"")
                    .noLeakCheck().returnsOnce("column\ttype\n"
                            + "d\tDOUBLE\n"
                            + "f\tFLOAT\n"
                            + "timestamp\tTIMESTAMP\n");
        });
    }

    @Test
    public void testPublicSenderCorpusMatchesExactSqlAndNulls() throws Exception {
        List<Vector> vectors = readVectors();
        Assert.assertEquals(60, vectors.size());
        runInContext(port -> {
            try (QwpWebSocketSender sender = connectWs(
                    port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                for (int i = 0; i < vectors.size(); i++) {
                    Vector vector = vectors.get(i);
                    String table = "schema_floating_decimal_" + i;
                    execute("create table " + table + " (value " + vector.targetSqlType()
                            + ", ts timestamp) timestamp(ts) partition by day wal");
                    sender.table(table);
                    if (vector.invalid()) {
                        LineSenderSchemaException error = Assert.assertThrows(
                                vector.caseId, LineSenderSchemaException.class,
                                () -> vector.append(sender, "value"));
                        Assert.assertEquals(vector.caseId,
                                LineSenderSchemaException.Reason.INVALID_VALUE, error.getReason());
                    } else {
                        vector.append(sender, "value");
                        sender.at(i + 1L, ChronoUnit.MICROS);
                    }
                }
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(fsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }
            drainWalQueue();
            for (int i = 0; i < vectors.size(); i++) {
                Vector vector = vectors.get(i);
                String expected = vector.invalid() ? "value\n"
                        : vector.isNull() ? "value\n\n" : "value\n" + vector.expectedSql + '\n';
                assertQuery("select value from schema_floating_decimal_" + i + " order by ts")
                        .noLeakCheck().returnsOnce(expected);
            }
        });
    }

    @Test
    public void testPublicSenderRecoversInvalidRowAndPreservesFirstValue() throws Exception {
        runInContext(port -> {
            execute("create table schema_floating_decimal_recovery "
                    + "(value decimal(3,1), marker string, ts timestamp) timestamp(ts) partition by day wal");
            try (Sender sender = connectWs(
                    port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                sender.table("schema_floating_decimal_recovery")
                        .floatColumn("value", 1.0f)
                        .doubleColumn("value", 1.25)
                        .doubleColumn("value", Double.NEGATIVE_INFINITY)
                        .stringColumn("marker", "A")
                        .at(1, ChronoUnit.MICROS);

                sender.table("schema_floating_decimal_recovery").stringColumn("marker", "failed-B");
                LineSenderSchemaException error = Assert.assertThrows(
                        LineSenderSchemaException.class,
                        () -> sender.doubleColumn("value", 100.0));
                Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, error.getReason());
                Assert.assertFalse(error.getMessage(), error.isRetryable());
                Assert.assertTrue(error.getMessage(), error.getMessage().contains("column=value"));
                Assert.assertTrue(error.getMessage(), error.getMessage().contains("inputType=DOUBLE"));
                Assert.assertTrue(error.getMessage(), error.getMessage().contains("targetType=DECIMAL"));

                sender.floatColumn("value", -2.5f)
                        .stringColumn("marker", "C")
                        .at(2, ChronoUnit.MICROS);
                sender.doubleColumn("value", Double.longBitsToDouble(0x7ff8000000000042L))
                        .floatColumn("value", 1.25f)
                        .stringColumn("marker", "source-null")
                        .at(3, ChronoUnit.MICROS);
                sender.stringColumn("marker", "omitted")
                        .at(4, ChronoUnit.MICROS);
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(fsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }
            drainWalQueue();
            assertQuery("select value, value is null is_null, marker "
                    + "from schema_floating_decimal_recovery order by ts")
                    .noLeakCheck().returnsOnce("value\tis_null\tmarker\n"
                            + "1.0\tfalse\tA\n"
                            + "-2.5\tfalse\tC\n"
                            + "\ttrue\tsource-null\n"
                            + "\ttrue\tomitted\n");
        });
    }

    @Test
    public void testRawLegacyFrameKeepsNativeTagsAndServerConversionWithBitmap() throws Exception {
        runInContext(port -> {
            sendRawLegacy(port, "legacy_floating_decimal_plain", false);
            sendRawLegacy(port, "legacy_floating_decimal_bitmap", true);
            drainWalQueue();

            assertLegacyResult("legacy_floating_decimal_plain", false);
            assertLegacyResult("legacy_floating_decimal_bitmap", true);
        });
    }

    private void assertLegacyResult(String table, boolean bitmap) throws Exception {
        String expected = "case_id\tf\td\tf_null\td_null\n"
                + "0\t0.10000000149011612\t0.1\tfalse\tfalse\n"
                + "1\t\t\ttrue\ttrue\n";
        if (bitmap) {
            expected += "2\t\t\ttrue\ttrue\n";
        }
        assertQuery("select case_id, f, d, f is null f_null, d is null d_null from "
                + table + " order by case_id").noLeakCheck().returnsOnce(expected);
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

    private static QwpTableBlockCursor parseSingleTable(QwpWebSocketEncoder encoder, int length) throws Exception {
        QwpMessageCursor message = new QwpMessageCursor();
        message.of(encoder.getBuffer().getBufferPtr(), length, new ObjList<>());
        Assert.assertTrue(message.hasNextTable());
        QwpTableBlockCursor table = message.nextTable();
        Assert.assertFalse(message.hasNextTable());
        return table;
    }

    private static List<Vector> readVectors() throws Exception {
        List<Vector> vectors = new ArrayList<>();
        try (InputStream stream = QwpSchemaFloatingDecimalE2ETest.class.getResourceAsStream(CORPUS)) {
            Assert.assertNotNull(CORPUS, stream);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                Assert.assertEquals(HEADER, reader.readLine());
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] fields = line.split("\t", -1);
                    Assert.assertEquals(line, 12, fields.length);
                    vectors.add(new Vector(fields));
                }
            }
        }
        return vectors;
    }

    private static WebSocketResponse receiveResponse(WebSocketClient client) {
        WebSocketResponse response = new WebSocketResponse();
        Assert.assertTrue(client.receiveFrame(new WebSocketFrameHandler() {
            @Override
            public void onBinaryMessage(long payloadPtr, int payloadLen) {
                Assert.assertTrue(response.readFrom(payloadPtr, payloadLen));
            }

            @Override
            public void onClose(int code, String reason) {
                Assert.fail("unexpected close [code=" + code + ", reason=" + reason + ']');
            }
        }, 5_000));
        return response;
    }

    private void sendRawLegacy(int port, String tableName, boolean bitmap) throws Exception {
        execute("create table " + tableName
                + " (case_id long, f decimal(18,17), d decimal(2,1), ts timestamp) "
                + "timestamp(ts) partition by day wal");
        try (WebSocketClient client = connectLegacy(port);
             QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
             QwpTableBuffer table = new QwpTableBuffer(tableName)) {
            QwpTableBuffer.ColumnBuffer caseId = table.getOrCreateColumn("case_id", QwpConstants.TYPE_LONG, false);
            QwpTableBuffer.ColumnBuffer floatValue = table.getOrCreateColumn("f", QwpConstants.TYPE_FLOAT, true);
            QwpTableBuffer.ColumnBuffer doubleValue = table.getOrCreateColumn("d", QwpConstants.TYPE_DOUBLE, true);
            QwpTableBuffer.ColumnBuffer timestamp =
                    table.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP);

            caseId.addLong(0);
            floatValue.addFloat(Float.intBitsToFloat(0x3dcccccd));
            doubleValue.addDouble(Double.longBitsToDouble(0x3fb999999999999aL));
            timestamp.addLong(1);
            table.nextRow();

            caseId.addLong(1);
            floatValue.addFloat(Float.POSITIVE_INFINITY);
            doubleValue.addDouble(Double.longBitsToDouble(0x7ff8000000000042L));
            timestamp.addLong(2);
            table.nextRow();

            if (bitmap) {
                caseId.addLong(2);
                timestamp.addLong(3);
                table.nextRow();
            }

            int length = encoder.encode(table);
            long frame = encoder.getBuffer().getBufferPtr();
            Assert.assertEquals(0,
                    Unsafe.getByte(frame + QwpConstants.HEADER_OFFSET_FLAGS) & QwpConstants.FLAG_SCHEMA);
            QwpTableBlockCursor wire = parseSingleTable(encoder, length);
            Assert.assertEquals(4, wire.getColumnCount());
            Assert.assertEquals(QwpConstants.TYPE_LONG, wire.getColumnDef(0).getTypeCode());
            Assert.assertEquals(QwpConstants.TYPE_FLOAT, wire.getColumnDef(1).getTypeCode());
            Assert.assertEquals(QwpConstants.TYPE_DOUBLE, wire.getColumnDef(2).getTypeCode());
            Assert.assertEquals(QwpConstants.TYPE_TIMESTAMP, wire.getColumnDef(3).getTypeCode());
            QwpFixedWidthColumnCursor floatWire = wire.getFixedWidthColumn(1);
            QwpFixedWidthColumnCursor doubleWire = wire.getFixedWidthColumn(2);
            Assert.assertEquals(bitmap, floatWire.getNullBitmapAddress() != 0);
            Assert.assertEquals(bitmap, doubleWire.getNullBitmapAddress() != 0);
            Assert.assertEquals(2, floatWire.getValueCount());
            Assert.assertEquals(2, doubleWire.getValueCount());
            Assert.assertEquals(0x3dcccccd, Unsafe.getInt(floatWire.getValuesAddress()));
            Assert.assertEquals(0x7f800000, Unsafe.getInt(floatWire.getValuesAddress() + Float.BYTES));
            Assert.assertEquals(0x3fb999999999999aL, Unsafe.getLong(doubleWire.getValuesAddress()));
            Assert.assertEquals(0x7ff8000000000042L,
                    Unsafe.getLong(doubleWire.getValuesAddress() + Double.BYTES));
            if (bitmap) {
                Assert.assertEquals(0x04, Unsafe.getByte(floatWire.getNullBitmapAddress()) & 0xff);
                Assert.assertEquals(0x04, Unsafe.getByte(doubleWire.getNullBitmapAddress()) & 0xff);
            }

            Assert.assertTrue(wire.hasNextRow());
            wire.nextRow();
            Assert.assertFalse(wire.isColumnNull(1));
            Assert.assertFalse(wire.isColumnNull(2));
            Assert.assertTrue(wire.hasNextRow());
            wire.nextRow();
            Assert.assertFalse(wire.isColumnNull(1));
            Assert.assertEquals(!bitmap, wire.isColumnNull(2));
            if (bitmap) {
                Assert.assertTrue(wire.hasNextRow());
                wire.nextRow();
                Assert.assertTrue(wire.isColumnNull(1));
                Assert.assertTrue(wire.isColumnNull(2));
            }
            Assert.assertFalse(wire.hasNextRow());

            client.sendBinary(frame, length);
            WebSocketResponse response = receiveResponse(client);
            Assert.assertTrue(response.getErrorMessage(), response.isSuccess());
            Assert.assertEquals(0, response.getSequence());
        }
    }

    private static final class Vector {
        private final String caseId;
        private final String expectedSql;
        private final long inputBits;
        private final String outcome;
        private final String sourceType;
        private final int targetPrecision;
        private final int targetScale;
        private final String targetType;

        private Vector(String[] fields) {
            caseId = fields[0];
            sourceType = fields[1];
            Assert.assertTrue(sourceType.equals("FLOAT") || sourceType.equals("DOUBLE"));
            inputBits = Long.parseUnsignedLong(fields[2], 16);
            Assert.assertEquals(sourceType.equals("FLOAT") ? 8 : 16, fields[2].length());
            targetType = fields[3];
            targetPrecision = Integer.parseInt(fields[4]);
            targetScale = Integer.parseInt(fields[5]);
            outcome = fields[6];
            expectedSql = fields[11];
            Assert.assertTrue(outcome.equals("VALUE") || outcome.equals("INVALID") || outcome.equals("NULL"));
            Assert.assertEquals(expectedTargetType(targetPrecision), targetType);
            if (invalid()) {
                Assert.assertEquals("-", expectedSql);
            } else if (isNull()) {
                Assert.assertEquals("NULL", expectedSql);
            } else {
                Assert.assertNotEquals("-", expectedSql);
                Assert.assertNotEquals("NULL", expectedSql);
            }
        }

        private void append(Sender sender, String column) {
            if ("FLOAT".equals(sourceType)) {
                sender.floatColumn(column, Float.intBitsToFloat((int) inputBits));
            } else {
                sender.doubleColumn(column, Double.longBitsToDouble(inputBits));
            }
        }

        private boolean invalid() {
            return outcome.equals("INVALID");
        }

        private boolean isNull() {
            return outcome.equals("NULL");
        }

        private String targetSqlType() {
            return "decimal(" + targetPrecision + ',' + targetScale + ')';
        }

        private static String expectedTargetType(int precision) {
            if (precision <= 2) {
                return "DECIMAL8";
            }
            if (precision <= 4) {
                return "DECIMAL16";
            }
            if (precision <= 9) {
                return "DECIMAL32";
            }
            if (precision <= 18) {
                return "DECIMAL64";
            }
            if (precision <= 38) {
                return "DECIMAL128";
            }
            return "DECIMAL256";
        }
    }
}
