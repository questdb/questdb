/*+*****************************************************************************
 * Copyright (c) 2014-2019 Appsicle
 * Copyright (c) 2019-2026 QuestDB
 * Licensed under the Apache License, Version 2.0
 *******************************************************************************/
package io.questdb.test.cutlass.qwp.e2e;

import io.questdb.client.LineSenderSchemaException;
import io.questdb.client.Sender;
import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal256;
import io.questdb.client.std.Decimal64;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketClientFactory;
import io.questdb.client.cutlass.http.client.WebSocketFrameHandler;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
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

public class QwpSchemaNativeDecimalE2ETest extends AbstractQwpWebSocketTest {
    private static final String CORPUS = "/io/questdb/client/cutlass/qwp/native-decimal-conversions.tsv";

    @Test
    public void testAllNativeDecimalPairsAndBoundaries() throws Exception {
        List<Vector> vectors = readVectors();
        Assert.assertEquals(60, vectors.size());
        boolean[][] pairs = new boolean[3][6];
        runInContext(port -> {
            try (QwpWebSocketSender sender = connectWs(port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                for (int i = 0; i < vectors.size(); i++) {
                    Vector v = vectors.get(i);
                    String table = "schema_decimal_" + i;
                    execute("create table " + table + " (value " + v.targetSqlType()
                            + ", ts timestamp) timestamp(ts) partition by day wal");
                    sender.table(table);
                    if (v.outcome.equals("INVALID")) {
                        LineSenderSchemaException error = Assert.assertThrows(
                                LineSenderSchemaException.class,
                                () -> append(sender, v)
                        );
                        Assert.assertEquals(v.caseId, LineSenderSchemaException.Reason.INVALID_VALUE, error.getReason());
                        Assert.assertTrue(error.getMessage(), error.getMessage().contains("value"));
                    } else {
                        append(sender, v);
                        sender.at(i + 1L, ChronoUnit.MICROS);
                    }
                    int source = sourceIndex(v.sourceType);
                    int target = targetIndex(v.targetType);
                    if (v.caseId.equals(v.sourceType.substring(7) + "_to_" + v.targetType.substring(7))) {
                        pairs[source][target] = true;
                    }
                }
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }
            drainWalQueue();
            for (int i = 0; i < vectors.size(); i++) {
                Vector v = vectors.get(i);
                String expected = v.outcome.equals("VALUE") ? "value\n" + v.expectedSql + "\n" : "value\n";
                assertQuery("select value from schema_decimal_" + i + " order by ts")
                        .noLeakCheck().returnsOnce(expected);
            }
        });
        for (int source = 0; source < 3; source++) {
            for (int target = 0; target < 6; target++) {
                Assert.assertTrue("missing pair " + source + '/' + target, pairs[source][target]);
            }
        }
    }

    @Test
    public void testNullNoopDuplicateAndPartialRowRecovery() throws Exception {
        runInContext(port -> {
            execute("create table schema_decimal_recovery (value decimal(2,1), marker string, ts timestamp) timestamp(ts) partition by day wal");
            try (QwpWebSocketSender sender = connectWs(port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                sender.table("schema_decimal_recovery")
                        .decimalColumn("value", Decimal64.NULL_VALUE)
                        .decimalColumn("value", Decimal128.NULL_VALUE)
                        .decimalColumn("value", Decimal256.NULL_VALUE)
                        .decimalColumn("value", new Decimal64(12, 1))
                        .stringColumn("marker", "A")
                        .at(1, ChronoUnit.MICROS);
                sender.table("schema_decimal_recovery")
                        .decimalColumn("value", (Decimal128) null)
                        .stringColumn("marker", "omitted")
                        .at(2, ChronoUnit.MICROS);
                sender.table("schema_decimal_recovery")
                        .decimalColumn("value", new Decimal64(12, 1))
                        .decimalColumn("value", new Decimal256(0, 0, 0, 100, 0))
                        .stringColumn("marker", "duplicate")
                        .at(3, ChronoUnit.MICROS);
                sender.table("schema_decimal_recovery").stringColumn("marker", "failed-B");
                LineSenderSchemaException error = Assert.assertThrows(
                        LineSenderSchemaException.class,
                        () -> sender.decimalColumn("value", new Decimal64(100, 0))
                );
                Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, error.getReason());
                sender.decimalColumn("value", new Decimal64(-12, 1))
                        .stringColumn("marker", "C")
                        .at(4, ChronoUnit.MICROS);
                sender.decimalColumn("marker", Decimal64.NULL_VALUE)
                        .decimalColumn("marker", (Decimal256) null);
                sender.stringColumn("marker", "failed-designated");
                LineSenderSchemaException designated = Assert.assertThrows(
                        LineSenderSchemaException.class,
                        () -> sender.decimalColumn("ts", new Decimal64(1, 0))
                );
                Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, designated.getReason());
                sender.decimalColumn("value", new Decimal64(0, 1))
                        .stringColumn("marker", "D")
                        .at(5, ChronoUnit.MICROS);
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }
            drainWalQueue();
            assertQuery("select value, value is null as is_null, marker from schema_decimal_recovery order by ts")
                    .noLeakCheck().returnsOnce(
                            "value\tis_null\tmarker\n"
                                    + "1.2\tfalse\tA\n"
                                    + "\ttrue\tomitted\n"
                                    + "1.2\tfalse\tduplicate\n"
                                    + "-1.2\tfalse\tC\n"
                                    + "0.0\tfalse\tD\n"
                    );
        });
    }

    @Test
    public void testRawLegacyDecimalSentinelTuplesWithAndWithoutBitmap() throws Exception {
        runInContext(port -> {
            assertRawSentinel(port, QwpConstants.TYPE_DECIMAL64, false, false);
            assertRawSentinel(port, QwpConstants.TYPE_DECIMAL64, true, false);
            assertRawSentinel(port, QwpConstants.TYPE_DECIMAL128, false, false);
            assertRawSentinel(port, QwpConstants.TYPE_DECIMAL128, true, false);
            assertRawSentinel(port, QwpConstants.TYPE_DECIMAL256, false, true);
            assertRawSentinel(port, QwpConstants.TYPE_DECIMAL256, true, true);
        });
    }

    private void assertRawSentinel(int port, byte wireType, boolean bitmap, boolean success) throws Exception {
        String suffix = QwpConstants.getTypeName(wireType).toLowerCase() + (bitmap ? "_bitmap" : "_plain");
        String tableName = "legacy_decimal_sentinel_" + suffix;
        String target = wireType == QwpConstants.TYPE_DECIMAL64 ? "decimal(18,0)"
                : wireType == QwpConstants.TYPE_DECIMAL128 ? "decimal(38,0)" : "decimal(76,0)";
        execute("create table " + tableName + " (value " + target + ", ts timestamp) timestamp(ts) partition by day wal");
        try (WebSocketClient client = connectLegacy(port);
             QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
             QwpTableBuffer table = new QwpTableBuffer(tableName)) {
            QwpTableBuffer.ColumnBuffer value = table.getOrCreateColumn("value", wireType, true);
            switch (wireType) {
                case QwpConstants.TYPE_DECIMAL64:
                    value.addDecimal64(new Decimal64(1, 0));
                    Unsafe.getUnsafe().putLong(value.getDataAddress(), Long.MIN_VALUE);
                    break;
                case QwpConstants.TYPE_DECIMAL128:
                    value.addDecimal128(new Decimal128(0, 1, 0));
                    Unsafe.getUnsafe().putLong(value.getDataAddress(), Long.MIN_VALUE);
                    Unsafe.getUnsafe().putLong(value.getDataAddress() + 8, 0);
                    break;
                default:
                    value.addDecimal256(new Decimal256(0, 0, 0, 1, 0));
                    Unsafe.getUnsafe().putLong(value.getDataAddress(), Long.MIN_VALUE);
                    Unsafe.getUnsafe().putLong(value.getDataAddress() + 8, 0);
                    Unsafe.getUnsafe().putLong(value.getDataAddress() + 16, 0);
                    Unsafe.getUnsafe().putLong(value.getDataAddress() + 24, 0);
                    break;
            }
            table.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(1);
            table.nextRow();
            if (bitmap) {
                table.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(2);
                table.nextRow();
            }
            int length = encoder.encode(table);
            client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
            assertResponse(client, success);
        }
        drainWalQueue();
        assertQuery("select count(), count(value) from " + tableName)
                .noLeakCheck().returnsOnce(success
                        ? (bitmap ? "count\tcount1\n2\t0\n" : "count\tcount1\n1\t0\n")
                        : "count\tcount1\n0\t0\n");
    }

    private static void append(Sender sender, Vector v) {
        switch (v.sourceType) {
            case "DECIMAL64":
                sender.decimalColumn("value", new Decimal64(v.ll, v.sourceScale));
                break;
            case "DECIMAL128": {
                Decimal128 value = new Decimal128();
                value.of(v.lh, v.ll, v.sourceScale);
                sender.decimalColumn("value", value);
                break;
            }
            case "DECIMAL256":
                sender.decimalColumn("value", new Decimal256(v.hh, v.hl, v.lh, v.ll, v.sourceScale));
                break;
            default:
                Assert.fail("unknown source type " + v.sourceType);
        }
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

    private static void assertResponse(WebSocketClient client, boolean success) {
        AtomicReference<WebSocketResponse> responseRef = new AtomicReference<>();
        Assert.assertTrue(client.receiveFrame(new WebSocketFrameHandler() {
            @Override
            public void onBinaryMessage(long ptr, int len) {
                WebSocketResponse response = new WebSocketResponse();
                Assert.assertTrue(response.readFrom(ptr, len));
                responseRef.set(response);
            }

            @Override
            public void onClose(int code, String reason) {
                Assert.fail("unexpected close " + code + ": " + reason);
            }
        }, 5_000));
        Assert.assertNotNull(responseRef.get());
        Assert.assertEquals(success, responseRef.get().isSuccess());
        Assert.assertEquals(0, responseRef.get().getSequence());
        if (!success) {
            Assert.assertTrue(responseRef.get().getErrorMessage(),
                    responseRef.get().getErrorMessage().contains("decimal"));
        }
    }

    private static List<Vector> readVectors() throws Exception {
        List<Vector> vectors = new ArrayList<>();
        try (InputStream in = QwpSchemaNativeDecimalE2ETest.class.getResourceAsStream(CORPUS)) {
            Assert.assertNotNull(CORPUS, in);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                String header = reader.readLine();
                Assert.assertNotNull(header);
                Assert.assertEquals(16, header.split("\\t", -1).length);
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] fields = line.split("\\t", -1);
                    Assert.assertEquals(line, 16, fields.length);
                    vectors.add(new Vector(fields));
                }
            }
        }
        return vectors;
    }

    private static int sourceIndex(String type) {
        return type.equals("DECIMAL64") ? 0 : type.equals("DECIMAL128") ? 1 : 2;
    }

    private static int targetIndex(String type) {
        switch (type) {
            case "DECIMAL8": return 0;
            case "DECIMAL16": return 1;
            case "DECIMAL32": return 2;
            case "DECIMAL64": return 3;
            case "DECIMAL128": return 4;
            default: return 5;
        }
    }

    private static long hex(String value) {
        return value.equals("-") ? 0 : Long.parseUnsignedLong(value, 16);
    }

    private static final class Vector {
        private final String caseId;
        private final long hh;
        private final long hl;
        private final long lh;
        private final long ll;
        private final String outcome;
        private final String expectedSql;
        private final int sourceScale;
        private final String sourceType;
        private final int targetPrecision;
        private final int targetScale;
        private final String targetType;

        private Vector(String[] fields) {
            caseId = fields[0];
            sourceType = fields[1];
            sourceScale = Integer.parseInt(fields[2]);
            ll = hex(fields[3]);
            lh = hex(fields[4]);
            hl = hex(fields[5]);
            hh = hex(fields[6]);
            targetType = fields[7];
            targetPrecision = Integer.parseInt(fields[8]);
            targetScale = Integer.parseInt(fields[9]);
            outcome = fields[10];
            expectedSql = fields[15];
            Assert.assertTrue(outcome.equals("VALUE") || outcome.equals("INVALID"));
        }

        private String targetSqlType() {
            return "decimal(" + targetPrecision + ',' + targetScale + ')';
        }
    }
}
