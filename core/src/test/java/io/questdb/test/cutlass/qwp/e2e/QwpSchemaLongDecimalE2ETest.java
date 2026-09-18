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
import io.questdb.cutlass.qwp.protocol.QwpFixedWidthColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpMessageCursor;
import io.questdb.cutlass.qwp.protocol.QwpTableBlockCursor;
import io.questdb.std.ObjList;
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

public class QwpSchemaLongDecimalE2ETest extends AbstractQwpWebSocketTest {
    private static final String CORPUS = "/io/questdb/client/cutlass/qwp/long-to-decimal.tsv";
    private static final String HEADER = "# case_id\tinput\ttarget_type\ttarget_precision\ttarget_scale\toutcome\texpected_ll_hex\texpected_lh_hex\texpected_hl_hex\texpected_hh_hex\texpected_sql";

    @Test
    public void testPublicSenderCorpusMatchesExactSql() throws Exception {
        List<Vector> vectors = readVectors();
        Assert.assertEquals(39, vectors.size());
        boolean[] targets = new boolean[6];
        runInContext(port -> {
            try (QwpWebSocketSender sender = connectWs(
                    port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                for (int i = 0; i < vectors.size(); i++) {
                    Vector vector = vectors.get(i);
                    targets[targetIndex(vector.targetType)] = true;
                    String table = "schema_long_decimal_" + i;
                    execute("create table " + table + " (value " + vector.targetSqlType()
                            + ", ts timestamp) timestamp(ts) partition by day wal");
                    sender.table(table);
                    if (vector.invalid()) {
                        LineSenderSchemaException error = Assert.assertThrows(
                                vector.caseId,
                                LineSenderSchemaException.class,
                                () -> sender.longColumn("value", vector.input)
                        );
                        Assert.assertEquals(vector.caseId, LineSenderSchemaException.Reason.INVALID_VALUE, error.getReason());
                        Assert.assertTrue(error.getMessage(), error.getMessage().contains("value"));
                    } else {
                        sender.longColumn("value", vector.input).at(i + 1L, ChronoUnit.MICROS);
                    }
                }
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }
            drainWalQueue();
            for (int i = 0; i < vectors.size(); i++) {
                Vector vector = vectors.get(i);
                String expected = vector.invalid() ? "value\n"
                        : vector.isNull() ? "value\n\n" : "value\n" + vector.expectedSql + '\n';
                assertQuery("select value from schema_long_decimal_" + i + " order by ts")
                        .noLeakCheck().returnsOnce(expected);
            }
        });
        for (int i = 0; i < targets.length; i++) {
            Assert.assertTrue("missing target " + i, targets[i]);
        }
    }

    @Test
    public void testPublicSenderInvalidRowRecoveryDuplicateNullAndOmission() throws Exception {
        runInContext(port -> {
            execute("create table schema_long_decimal_recovery "
                    + "(value decimal(2,1), marker string, ts timestamp) timestamp(ts) partition by day wal");
            try (QwpWebSocketSender sender = connectWs(
                    port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                sender.table("schema_long_decimal_recovery")
                        .longColumn("value", 1)
                        .longColumn("value", 10)
                        .stringColumn("marker", "A")
                        .at(1, ChronoUnit.MICROS);

                sender.table("schema_long_decimal_recovery").stringColumn("marker", "failed-B");
                LineSenderSchemaException error = Assert.assertThrows(
                        LineSenderSchemaException.class,
                        () -> sender.longColumn("value", 10)
                );
                Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, error.getReason());

                sender.table("schema_long_decimal_recovery").longColumn("value", -1)
                        .stringColumn("marker", "C")
                        .at(2, ChronoUnit.MICROS);
                sender.table("schema_long_decimal_recovery").longColumn("value", Long.MIN_VALUE)
                        .longColumn("value", 1)
                        .stringColumn("marker", "source-null")
                        .at(3, ChronoUnit.MICROS);
                sender.table("schema_long_decimal_recovery").stringColumn("marker", "omitted")
                        .at(4, ChronoUnit.MICROS);
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }
            drainWalQueue();
            assertQuery("select value, value is null as is_null, marker "
                    + "from schema_long_decimal_recovery order by ts")
                    .noLeakCheck().returnsOnce(
                            "value\tis_null\tmarker\n"
                                    + "1.0\tfalse\tA\n"
                                    + "-1.0\tfalse\tC\n"
                                    + "\ttrue\tsource-null\n"
                                    + "\ttrue\tomitted\n"
                    );
        });
    }

    @Test
    public void testRawLegacyLongMinRetainsBitmapDependentDecimalBehavior() throws Exception {
        runInContext(port -> {
            execute("create table legacy_long_decimal_plain "
                    + "(value decimal(19,0), ts timestamp) timestamp(ts) partition by day wal");
            sendLegacyLongMin(port, "legacy_long_decimal_plain", false, true);

            execute("create table legacy_long_decimal_bitmap_wide "
                    + "(value decimal(19,0), ts timestamp) timestamp(ts) partition by day wal");
            sendLegacyLongMin(port, "legacy_long_decimal_bitmap_wide", true, true);

            execute("create table legacy_long_decimal_bitmap_narrow "
                    + "(value decimal(18,0), ts timestamp) timestamp(ts) partition by day wal");
            sendLegacyLongMin(port, "legacy_long_decimal_bitmap_narrow", true, false);

            drainWalQueue();
            assertQuery("select value from legacy_long_decimal_plain")
                    .noLeakCheck().returnsOnce("value\n\n");
            assertQuery("select value from legacy_long_decimal_bitmap_wide")
                    .noLeakCheck().returnsOnce("value\n-9223372036854775808\n\n");
            assertQuery("select count() from legacy_long_decimal_bitmap_narrow")
                    .noLeakCheck().returnsOnce("count\n0\n");
        });
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
        try (InputStream stream = QwpSchemaLongDecimalE2ETest.class.getResourceAsStream(CORPUS)) {
            Assert.assertNotNull(CORPUS, stream);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                Assert.assertEquals(HEADER, reader.readLine());
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] fields = line.split("\t", -1);
                    Assert.assertEquals(line, 11, fields.length);
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

    private static void sendLegacyLongMin(
            int port,
            String tableName,
            boolean withOmission,
            boolean expectSuccess
    ) throws Exception {
        try (WebSocketClient client = connectLegacy(port);
             QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
             QwpTableBuffer table = new QwpTableBuffer(tableName)) {
            table.getOrCreateColumn("value", QwpConstants.TYPE_LONG, true).addLong(Long.MIN_VALUE);
            table.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(1);
            table.nextRow();
            if (withOmission) {
                table.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(2);
                table.nextRow();
            }
            int length = encoder.encode(table);
            QwpTableBlockCursor wireTable = parseSingleTable(encoder, length);
            Assert.assertEquals(2, wireTable.getColumnCount());
            Assert.assertEquals(QwpConstants.TYPE_LONG, wireTable.getColumnDef(0).getTypeCode());
            QwpFixedWidthColumnCursor wireValue = wireTable.getFixedWidthColumn(0);
            Assert.assertEquals(withOmission, wireValue.getNullBitmapAddress() != 0);
            Assert.assertTrue(wireTable.hasNextRow());
            wireTable.nextRow();
            Assert.assertEquals(Long.MIN_VALUE, wireValue.getLong());
            Assert.assertEquals(!withOmission, wireTable.isColumnNull(0));
            if (withOmission) {
                Assert.assertTrue(wireTable.hasNextRow());
                wireTable.nextRow();
                Assert.assertTrue(wireTable.isColumnNull(0));
            }
            Assert.assertFalse(wireTable.hasNextRow());

            client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
            WebSocketResponse response = receiveResponse(client);
            Assert.assertEquals(response.getErrorMessage(), expectSuccess, response.isSuccess());
            Assert.assertEquals(0, response.getSequence());
            if (!expectSuccess) {
                Assert.assertTrue(response.getErrorMessage(), response.getErrorMessage().contains("decimal"));
            }
        }
    }

    private static int targetIndex(String target) {
        switch (target) {
            case "DECIMAL8":
                return 0;
            case "DECIMAL16":
                return 1;
            case "DECIMAL32":
                return 2;
            case "DECIMAL64":
                return 3;
            case "DECIMAL128":
                return 4;
            default:
                return 5;
        }
    }

    private static final class Vector {
        private final String caseId;
        private final String expectedSql;
        private final long input;
        private final String outcome;
        private final int targetPrecision;
        private final int targetScale;
        private final String targetType;

        private Vector(String[] fields) {
            caseId = fields[0];
            input = Long.parseLong(fields[1]);
            targetType = fields[2];
            targetPrecision = Integer.parseInt(fields[3]);
            targetScale = Integer.parseInt(fields[4]);
            outcome = fields[5];
            expectedSql = fields[10];
            Assert.assertTrue(outcome.equals("VALUE") || outcome.equals("INVALID") || outcome.equals("NULL"));
            Assert.assertEquals(expectedTargetType(targetPrecision), targetType);
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
