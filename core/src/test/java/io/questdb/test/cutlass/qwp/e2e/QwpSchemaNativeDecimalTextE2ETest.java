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
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer.ColumnBuffer;
import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal256;
import io.questdb.client.std.Decimal64;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpDecimalColumnCursor;
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

public class QwpSchemaNativeDecimalTextE2ETest extends AbstractQwpWebSocketTest {
    private static final String CORPUS = "/io/questdb/client/cutlass/qwp/native-decimal-to-text.tsv";
    private static final String HEADER = "# case_id\tsource_type\tsource_scale\tsource_ll_hex\t"
            + "source_lh_hex\tsource_hl_hex\tsource_hh_hex\ttarget_type\tinput_form\texpected_text";
    private static final String NINES_38 = "99999999999999999999999999999999999999";
    private static final String NINES_76 = "9999999999999999999999999999999999999999999999999999999999999999999999999999";

    @Test
    public void testPublicSenderCorpusStoresExactTextAndTargetTypes() throws Exception {
        List<Vector> vectors = readVectors();
        Assert.assertEquals(24, vectors.size());
        boolean[][] sourceTargets = new boolean[3][2];
        runInContext(port -> {
            for (Target target : Target.values()) {
                execute("create table " + target.tableName + " (seq long, value " + target.sqlType
                        + ", ts timestamp) timestamp(ts) partition by day wal");
            }
            try (QwpWebSocketSender sender = connectWs(
                    port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                for (Target target : Target.values()) {
                    int row = 0;
                    for (Vector vector : vectors) {
                        if (vector.target == target) {
                            sender.table(target.tableName).longColumn("seq", row);
                            vector.append(sender, "value");
                            sender.at(1_000_000L + row, ChronoUnit.MICROS);
                            sourceTargets[vector.sourceIndex()][target.ordinal()] = true;
                            row++;
                        }
                    }
                    Assert.assertEquals(target.name(), 12, row);
                }
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(fsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }
            drainWalQueue();
            for (Target target : Target.values()) {
                assertQuery("select type from table_columns('" + target.tableName
                        + "') where \"column\" = 'value'")
                        .noLeakCheck().returnsOnce("type\n" + target.sqlType + "\n");
                StringBuilder expected = new StringBuilder("seq\tvalue\tn\n");
                int row = 0;
                for (Vector vector : vectors) {
                    if (vector.target == target) {
                        expected.append(row++).append('\t');
                        if (!vector.isNull()) {
                            expected.append(vector.expectedText);
                        }
                        expected.append('\t').append(vector.isNull()).append('\n');
                    }
                }
                assertQuery("select seq, value, value is null n from " + target.tableName + " order by seq")
                        .noLeakCheck().returnsOnce(expected.toString());
            }
        });
        for (int source = 0; source < sourceTargets.length; source++) {
            for (int target = 0; target < sourceTargets[source].length; target++) {
                Assert.assertTrue("missing source/target pair " + source + '/' + target,
                        sourceTargets[source][target]);
            }
        }
    }

    @Test
    public void testPublicSenderUsesPerValueScalesAcrossMixedNativeWidthsAndRecoversRows() throws Exception {
        runInContext(port -> {
            execute("create table schema_native_decimal_text_rows "
                    + "(value varchar, marker string, bad uuid, ts timestamp) "
                    + "timestamp(ts) partition by day wal");
            Decimal64 wrappedScale = new Decimal64(123, 1);
            wrappedScale.setScale(257);
            try (QwpWebSocketSender sender = connectWs(
                    port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                sender.table("schema_native_decimal_text_rows")
                        .decimalColumn("value", Decimal64.NULL_VALUE)
                        .decimalColumn("value", new Decimal64(12_340, 2))
                        .binaryColumn("value", new byte[]{1})
                        .stringColumn("marker", "A")
                        .at(1, ChronoUnit.MICROS);

                sender.stringColumn("marker", "failed-B");
                LineSenderSchemaException error = Assert.assertThrows(
                        LineSenderSchemaException.class,
                        () -> sender.decimalColumn("bad", new Decimal64(1, 0))
                );
                Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, error.getReason());
                Assert.assertFalse(error.isRetryable());
                Assert.assertTrue(error.getMessage(), error.getMessage().contains("column=bad"));
                Assert.assertTrue(error.getMessage(), error.getMessage().contains("inputType=DECIMAL64"));
                Assert.assertTrue(error.getMessage(), error.getMessage().contains("targetType=UUID"));

                sender.decimalColumn("value", new Decimal64(-125, 3))
                        .stringColumn("marker", "C")
                        .at(2, ChronoUnit.MICROS);
                sender.stringColumn("marker", "omitted")
                        .at(3, ChronoUnit.MICROS);
                sender.decimalColumn("value", new Decimal256(
                                0x161bcca7119915b5L,
                                0x0764b4abe8652979L,
                                0x7775a5f171950fffL,
                                0xffffffffffffffffL,
                                76))
                        .stringColumn("marker", "long")
                        .at(4, ChronoUnit.MICROS);
                sender.decimalColumn("value", new Decimal64(0, 0))
                        .stringColumn("marker", "short")
                        .at(5, ChronoUnit.MICROS);
                sender.decimalColumn("value", new Decimal128(Long.MAX_VALUE, -1L))
                        .stringColumn("marker", "physical-128")
                        .at(6, ChronoUnit.MICROS);
                sender.decimalColumn("value", wrappedScale)
                        .stringColumn("marker", "wrapped-scale-257")
                        .at(7, ChronoUnit.MICROS);
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(fsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }
            drainWalQueue();
            assertQuery("select value, value is null n, marker "
                    + "from schema_native_decimal_text_rows order by ts")
                    .noLeakCheck().returnsOnce("value\tn\tmarker\n"
                            + "123.40\tfalse\tA\n"
                            + "-0.125\tfalse\tC\n"
                            + "\ttrue\tomitted\n"
                            + "0." + NINES_76 + "\tfalse\tlong\n"
                            + "0\tfalse\tshort\n"
                            + "170141183460469231731687303715884105727\tfalse\tphysical-128\n"
                            + "12.3\tfalse\twrapped-scale-257\n");
        });
    }

    @Test
    public void testRawLegacyNativeDecimalFrameUsesServerFormatterAndBitmapNulls() throws Exception {
        runInContext(port -> {
            String tableName = "legacy_native_decimal_text";
            execute("create table " + tableName + " (case_id long, "
                    + "d64_string string, d64_varchar varchar, "
                    + "d128_string string, d128_varchar varchar, "
                    + "d256_string string, d256_varchar varchar, "
                    + "ts timestamp) timestamp(ts) partition by day wal");
            try (WebSocketClient client = connectLegacy(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = new QwpTableBuffer(tableName)) {
                ColumnBuffer caseId = table.getOrCreateColumn("case_id", QwpConstants.TYPE_LONG, false);
                ColumnBuffer d64String = table.getOrCreateColumn("d64_string", QwpConstants.TYPE_DECIMAL64, true);
                ColumnBuffer d64Varchar = table.getOrCreateColumn("d64_varchar", QwpConstants.TYPE_DECIMAL64, true);
                ColumnBuffer d128String = table.getOrCreateColumn("d128_string", QwpConstants.TYPE_DECIMAL128, true);
                ColumnBuffer d128Varchar = table.getOrCreateColumn("d128_varchar", QwpConstants.TYPE_DECIMAL128, true);
                ColumnBuffer d256String = table.getOrCreateColumn("d256_string", QwpConstants.TYPE_DECIMAL256, true);
                ColumnBuffer d256Varchar = table.getOrCreateColumn("d256_varchar", QwpConstants.TYPE_DECIMAL256, true);
                ColumnBuffer timestamp = table.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP);

                caseId.addLong(0);
                d64String.addDecimal64(new Decimal64(12_340, 2));
                d64Varchar.addDecimal64(new Decimal64(-125, 3));
                d128String.addDecimal128(new Decimal128(
                        0x4b3b4ca85a86c47aL, 0x098a223fffffffffL, 0));
                d128Varchar.addDecimal128(new Decimal128(
                        0xb4c4b357a5793b85L, 0xf675ddc000000001L, 38));
                d256String.addDecimal256(new Decimal256(0, 0, 0, 12_340, 2));
                d256Varchar.addDecimal256(new Decimal256(
                        0xe9e43358ee66ea4aL,
                        0xf89b4b54179ad686L,
                        0x888a5a0e8e6af000L,
                        1,
                        76));
                timestamp.addLong(1);
                table.nextRow();

                caseId.addLong(1);
                timestamp.addLong(2);
                table.nextRow();

                assertBitmap(d64String, 2);
                assertBitmap(d64Varchar, 3);
                assertBitmap(d128String, 0);
                assertBitmap(d128Varchar, 38);
                assertBitmap(d256String, 2);
                assertBitmap(d256Varchar, 76);

                int length = encoder.encode(table);
                long frame = encoder.getBuffer().getBufferPtr();
                Assert.assertEquals(0,
                        Unsafe.getByte(frame + QwpConstants.HEADER_OFFSET_FLAGS) & QwpConstants.FLAG_SCHEMA);
                QwpTableBlockCursor wire = parseSingleTable(encoder, length);
                Assert.assertFalse(wire.hasKnownSchemaIdentity());
                Assert.assertEquals(8, wire.getColumnCount());
                Assert.assertEquals(2, wire.getRowCount());
                byte[] types = {
                        QwpConstants.TYPE_LONG,
                        QwpConstants.TYPE_DECIMAL64,
                        QwpConstants.TYPE_DECIMAL64,
                        QwpConstants.TYPE_DECIMAL128,
                        QwpConstants.TYPE_DECIMAL128,
                        QwpConstants.TYPE_DECIMAL256,
                        QwpConstants.TYPE_DECIMAL256,
                        QwpConstants.TYPE_TIMESTAMP
                };
                int[] scales = {-1, 2, 3, 0, 38, 2, 76, -1};
                for (int i = 0; i < types.length; i++) {
                    Assert.assertEquals("wire type " + i, types[i], wire.getColumnDef(i).getTypeCode());
                    if (scales[i] >= 0) {
                        Assert.assertEquals("wire scale " + i, scales[i], wire.getDecimalColumn(i).getScale() & 0xff);
                    }
                }

                Assert.assertTrue(wire.hasNextRow());
                wire.nextRow();
                Assert.assertEquals(0, wire.getFixedWidthColumn(0).getLong());
                Assert.assertEquals(12_340, wire.getDecimalColumn(1).getDecimal64());
                Assert.assertEquals(-125, wire.getDecimalColumn(2).getDecimal64());
                assertDecimal128(wire.getDecimalColumn(3),
                        0x4b3b4ca85a86c47aL, 0x098a223fffffffffL);
                assertDecimal128(wire.getDecimalColumn(4),
                        0xb4c4b357a5793b85L, 0xf675ddc000000001L);
                assertDecimal256(wire.getDecimalColumn(5), 0, 0, 0, 12_340);
                assertDecimal256(wire.getDecimalColumn(6),
                        0xe9e43358ee66ea4aL,
                        0xf89b4b54179ad686L,
                        0x888a5a0e8e6af000L,
                        1);
                Assert.assertEquals(1, wire.getTimestampColumn(7).getTimestamp());
                for (int i = 1; i <= 6; i++) {
                    Assert.assertFalse("row 0 column " + i, wire.isColumnNull(i));
                }

                Assert.assertTrue(wire.hasNextRow());
                wire.nextRow();
                Assert.assertEquals(1, wire.getFixedWidthColumn(0).getLong());
                Assert.assertEquals(2, wire.getTimestampColumn(7).getTimestamp());
                for (int i = 1; i <= 6; i++) {
                    Assert.assertTrue("row 1 column " + i, wire.isColumnNull(i));
                }
                Assert.assertFalse(wire.hasNextRow());

                client.sendBinary(frame, length);
                WebSocketResponse response = receiveResponse(client);
                Assert.assertTrue(response.getErrorMessage(), response.isSuccess());
                Assert.assertEquals(0, response.getSequence());
            }
            drainWalQueue();
            assertQuery("select d64_string, d64_varchar, d128_string, d128_varchar, "
                    + "d256_string, d256_varchar from " + tableName + " where case_id = 0")
                    .noLeakCheck().returnsOnce("d64_string\td64_varchar\td128_string\td128_varchar\t"
                            + "d256_string\td256_varchar\n"
                            + "123.40\t-0.125\t" + NINES_38 + "\t-0." + NINES_38
                            + "\t123.40\t-0." + NINES_76 + "\n");
            assertQuery("select case_id, "
                    + "d64_string is null n64s, d64_varchar is null n64v, "
                    + "d128_string is null n128s, d128_varchar is null n128v, "
                    + "d256_string is null n256s, d256_varchar is null n256v "
                    + "from " + tableName + " where case_id = 1")
                    .noLeakCheck().returnsOnce("case_id\tn64s\tn64v\tn128s\tn128v\tn256s\tn256v\n"
                            + "1\ttrue\ttrue\ttrue\ttrue\ttrue\ttrue\n");
        });
    }

    private static void assertBitmap(ColumnBuffer column, int scale) {
        Assert.assertEquals(scale, column.getDecimalScale() & 0xff);
        Assert.assertEquals(2, column.getSize());
        Assert.assertEquals(1, column.getValueCount());
        Assert.assertNotEquals(0, column.getNullBitmapAddress());
        Assert.assertEquals(0x02, Unsafe.getByte(column.getNullBitmapAddress()) & 0xff);
        Assert.assertFalse(column.isNull(0));
        Assert.assertTrue(column.isNull(1));
    }

    private static void assertDecimal128(QwpDecimalColumnCursor cursor, long hi, long lo) {
        Assert.assertEquals(hi, cursor.getDecimal128Hi());
        Assert.assertEquals(lo, cursor.getDecimal128Lo());
    }

    private static void assertDecimal256(
            QwpDecimalColumnCursor cursor, long hh, long hl, long lh, long ll
    ) {
        Assert.assertEquals(hh, cursor.getDecimal256Hh());
        Assert.assertEquals(hl, cursor.getDecimal256Hl());
        Assert.assertEquals(lh, cursor.getDecimal256Lh());
        Assert.assertEquals(ll, cursor.getDecimal256Ll());
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
        try (InputStream stream = QwpSchemaNativeDecimalTextE2ETest.class.getResourceAsStream(CORPUS)) {
            Assert.assertNotNull(CORPUS, stream);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                Assert.assertEquals(HEADER, reader.readLine());
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] fields = line.split("\\t", -1);
                    Assert.assertEquals(line, 10, fields.length);
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

    private enum Target {
        STRING("schema_native_decimal_string", "STRING"),
        VARCHAR("schema_native_decimal_varchar", "VARCHAR");

        private final String sqlType;
        private final String tableName;

        Target(String tableName, String sqlType) {
            this.tableName = tableName;
            this.sqlType = sqlType;
        }
    }

    private static final class Vector {
        private final String caseId;
        private final String expectedText;
        private final String inputForm;
        private final long[] limbs;
        private final int sourceScale;
        private final String sourceType;
        private final Target target;

        private Vector(String[] fields) {
            caseId = fields[0];
            sourceType = fields[1];
            sourceScale = Integer.parseInt(fields[2]);
            limbs = new long[]{hex(fields[3]), hex(fields[4]), hex(fields[5]), hex(fields[6])};
            target = Target.valueOf(fields[7]);
            inputForm = fields[8];
            expectedText = fields[9];
            Assert.assertTrue(caseId, inputForm.equals("VALUE")
                    || inputForm.equals("JAVA_NULL")
                    || inputForm.equals("NULL_VALUE"));
            Assert.assertEquals(caseId, isNull(), expectedText.equals("-"));
        }

        private void append(Sender sender, String column) {
            if (inputForm.equals("JAVA_NULL")) {
                switch (sourceType) {
                    case "DECIMAL64":
                        sender.decimalColumn(column, (Decimal64) null);
                        return;
                    case "DECIMAL128":
                        sender.decimalColumn(column, (Decimal128) null);
                        return;
                    case "DECIMAL256":
                        sender.decimalColumn(column, (Decimal256) null);
                        return;
                    default:
                        Assert.fail(caseId + ": " + sourceType);
                }
            }
            if (inputForm.equals("NULL_VALUE")) {
                switch (sourceType) {
                    case "DECIMAL64":
                        sender.decimalColumn(column, Decimal64.NULL_VALUE);
                        return;
                    case "DECIMAL128":
                        sender.decimalColumn(column, Decimal128.NULL_VALUE);
                        return;
                    case "DECIMAL256":
                        sender.decimalColumn(column, Decimal256.NULL_VALUE);
                        return;
                    default:
                        Assert.fail(caseId + ": " + sourceType);
                }
            }
            switch (sourceType) {
                case "DECIMAL64":
                    sender.decimalColumn(column, new Decimal64(limbs[0], sourceScale));
                    return;
                case "DECIMAL128":
                    sender.decimalColumn(column, new Decimal128(limbs[1], limbs[0], sourceScale));
                    return;
                case "DECIMAL256":
                    sender.decimalColumn(column,
                            new Decimal256(limbs[3], limbs[2], limbs[1], limbs[0], sourceScale));
                    return;
                default:
                    Assert.fail(caseId + ": " + sourceType);
            }
        }

        private static long hex(String value) {
            return value.equals("-") ? 0 : Long.parseUnsignedLong(value, 16);
        }

        private boolean isNull() {
            return !inputForm.equals("VALUE");
        }

        private int sourceIndex() {
            switch (sourceType) {
                case "DECIMAL64":
                    return 0;
                case "DECIMAL128":
                    return 1;
                case "DECIMAL256":
                    return 2;
                default:
                    throw new AssertionError(caseId + ": " + sourceType);
            }
        }
    }
}
