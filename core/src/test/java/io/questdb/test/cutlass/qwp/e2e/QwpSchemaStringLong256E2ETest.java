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
import io.questdb.cutlass.qwp.protocol.QwpTableBlockCursor;
import io.questdb.std.Long256;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8s;
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

public class QwpSchemaStringLong256E2ETest extends AbstractQwpWebSocketTest {
    private static final String CORPUS = "/io/questdb/client/cutlass/qwp/string-long256-conversions.tsv";

    @Test
    public void testLegacyVarcharToLong256ValueNullOmissionAndRejections() throws Exception {
        runInContext(port -> {
            execute("create table legacy_string_l256 (value long256, ts timestamp) timestamp(ts) partition by day wal");
            try (WebSocketClient client = connectLegacy(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = new QwpTableBuffer("legacy_string_l256")) {
                QwpTableBuffer.ColumnBuffer values = table.getOrCreateColumn("value", QwpConstants.TYPE_VARCHAR, true);
                QwpTableBuffer.ColumnBuffer timestamps = table.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP);
                values.addString("0x0123456789abcdeffedcba9876543210");
                timestamps.addLong(1);
                table.nextRow();
                values.addNull();
                timestamps.addLong(2);
                table.nextRow();
                timestamps.addLong(3);
                table.nextRow();
                int length = encoder.encode(table);
                QwpTableBlockCursor wire = parseSingleTable(encoder, length);
                Assert.assertEquals(2, wire.getColumnCount());
                Assert.assertEquals(QwpConstants.TYPE_VARCHAR, wire.getColumnDef(0).getTypeCode());
                Assert.assertTrue(wire.hasNextRow());
                wire.nextRow();
                Assert.assertEquals("0x0123456789abcdeffedcba9876543210", Utf8s.toString(wire.getStringColumn(0).getUtf8Value()));
                Assert.assertTrue(wire.hasNextRow());
                wire.nextRow();
                Assert.assertTrue(wire.isColumnNull(0));
                Assert.assertTrue(wire.hasNextRow());
                wire.nextRow();
                Assert.assertTrue(wire.isColumnNull(0));
                Assert.assertFalse(wire.hasNextRow());
                client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                assertResponse(client, true, 0, null);
            }
            drainWalQueue();
            assertQuery("select value, value is null n from legacy_string_l256 order by ts")
                    .noLeakCheck().expectSize().returns("value\tn\n0x0123456789abcdeffedcba9876543210\tfalse\n\ttrue\n\ttrue\n");

            assertLegacyRejected(port, "0x0", "odd_digits");
            assertLegacyRejected(port,
                    "0x8000000000000000800000000000000080000000000000008000000000000000",
                    "null_sentinel");
        });
    }

    @Test
    public void testPublicSenderCorpusStoresExactLimbsAndNullness() throws Exception {
        List<Vector> vectors = readVectors();
        runInContext(port -> {
            execute("create table schema_string_l256 (case_id long, value long256, ts timestamp) timestamp(ts) partition by day wal");
            try (QwpWebSocketSender sender = connectWs(port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                int row = 0;
                for (Vector vector : vectors) {
                    sender.table("schema_string_l256").longColumn("case_id", row);
                    if (vector.outcome.equals("VALUE")) {
                        sender.stringColumn("value", vector.input).at(1_000_000L + row++, ChronoUnit.MICROS);
                    } else if (vector.outcome.equals("NULL")) {
                        sender.stringColumn("value", null).at(1_000_000L + row++, ChronoUnit.MICROS);
                    } else {
                        LineSenderSchemaException error = Assert.assertThrows(LineSenderSchemaException.class,
                                () -> sender.stringColumn("value", vector.input));
                        Assert.assertEquals(vector.caseId, LineSenderSchemaException.Reason.INVALID_VALUE, error.getReason());
                    }
                }
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(fsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }
            drainWalQueue();
            try (RecordCursorFactory factory = select("select case_id, value, value is null from schema_string_l256 order by case_id");
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                int row = 0;
                for (Vector vector : vectors) {
                    if (!vector.outcome.equals("INVALID")) {
                        Assert.assertTrue(vector.caseId, cursor.hasNext());
                        Assert.assertEquals(vector.caseId, row++, record.getLong(0));
                        Assert.assertEquals(vector.caseId, vector.outcome.equals("NULL"), record.getBool(2));
                        if (vector.outcome.equals("VALUE")) {
                            Long256 value = record.getLong256A(1);
                            Assert.assertEquals(vector.caseId, vector.l0, value.getLong0());
                            Assert.assertEquals(vector.caseId, vector.l1, value.getLong1());
                            Assert.assertEquals(vector.caseId, vector.l2, value.getLong2());
                            Assert.assertEquals(vector.caseId, vector.l3, value.getLong3());
                        }
                    }
                }
                Assert.assertFalse(cursor.hasNext());
            }
        });
    }

    @Test
    public void testPublicSenderNullFirstDuplicateAndPartialRowRecovery() throws Exception {
        runInContext(port -> {
            execute("create table schema_string_l256_rows (value long256, marker string, ts timestamp) timestamp(ts) partition by day wal");
            try (QwpWebSocketSender sender = connectWs(port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                sender.table("schema_string_l256_rows").stringColumn("value", null)
                        .binaryColumn("value", new byte[]{1}).stringColumn("marker", "A").at(1, ChronoUnit.MICROS);
                sender.stringColumn("marker", "failed-B");
                LineSenderSchemaException invalid = Assert.assertThrows(LineSenderSchemaException.class,
                        () -> sender.stringColumn("value", "0x0"));
                Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, invalid.getReason());
                Assert.assertTrue(invalid.getMessage(), invalid.getMessage().contains("column=value"));
                Assert.assertTrue(invalid.getMessage(), invalid.getMessage().contains("targetType=LONG256"));
                sender.stringColumn("value", "0xff").stringColumn("marker", "C").at(2, ChronoUnit.MICROS);
                sender.stringColumn("marker", "failed-designated");
                LineSenderSchemaException designated = Assert.assertThrows(LineSenderSchemaException.class,
                        () -> sender.stringColumn("ts", "0xff"));
                Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, designated.getReason());
                sender.stringColumn("marker", "D").at(3, ChronoUnit.MICROS);
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }
            drainWalQueue();
            assertQuery("select value, marker, value is null n from schema_string_l256_rows order by ts")
                    .noLeakCheck().expectSize().returns("value\tmarker\tn\n\tA\ttrue\n0xff\tC\tfalse\n\tD\ttrue\n");
        });
    }

    private void assertLegacyRejected(int port, String input, String suffix) throws Exception {
        String tableName = "legacy_string_l256_bad_" + suffix;
        execute("create table " + tableName + " (value long256, ts timestamp) timestamp(ts) partition by day wal");
        try (WebSocketClient client = connectLegacy(port);
             QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
             QwpTableBuffer table = new QwpTableBuffer(tableName)) {
            table.getOrCreateColumn("value", QwpConstants.TYPE_VARCHAR, true).addString(input);
            table.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(1);
            table.nextRow();
            int length = encoder.encode(table);
            client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
            assertResponse(client, false, 0, "long256");
        }
        drainWalQueue();
        assertQuery("select count() from " + tableName).noLeakCheck().expectSize().noRandomAccess().returns("count\n0\n");
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

    private static void assertResponse(WebSocketClient client, boolean success, long sequence, String messagePart) {
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
        Assert.assertEquals(success, response.get().isSuccess());
        Assert.assertEquals(sequence, response.get().getSequence());
        if (messagePart != null)
            Assert.assertTrue(response.get().getErrorMessage(), response.get().getErrorMessage().contains(messagePart));
    }

    private static QwpTableBlockCursor parseSingleTable(QwpWebSocketEncoder encoder, int length) throws Exception {
        Assert.assertEquals(0, Unsafe.getByte(encoder.getBuffer().getBufferPtr() + QwpConstants.HEADER_OFFSET_FLAGS) & QwpConstants.FLAG_SCHEMA);
        QwpMessageCursor cursor = new QwpMessageCursor();
        cursor.of(encoder.getBuffer().getBufferPtr(), length, new ObjList<>());
        Assert.assertTrue(cursor.hasNextTable());
        QwpTableBlockCursor table = cursor.nextTable();
        Assert.assertFalse(cursor.hasNextTable());
        return table;
    }

    private static List<Vector> readVectors() throws Exception {
        List<Vector> vectors = new ArrayList<>();
        try (InputStream stream = QwpSchemaStringLong256E2ETest.class.getResourceAsStream(CORPUS)) {
            Assert.assertNotNull(CORPUS, stream);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                Assert.assertEquals("# case_id\tinput\toutcome\tl0_hex\tl1_hex\tl2_hex\tl3_hex", reader.readLine());
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] f = line.split("\\t", -1);
                    Assert.assertEquals(line, 7, f.length);
                    Assert.assertTrue(line, f[2].equals("VALUE") || f[2].equals("NULL") || f[2].equals("INVALID"));
                    if (f[2].equals("VALUE")) {
                        vectors.add(new Vector(f[0], f[1], f[2], hex(f[3]), hex(f[4]), hex(f[5]), hex(f[6])));
                    } else {
                        Assert.assertTrue(line, f[3].isEmpty() && f[4].isEmpty() && f[5].isEmpty() && f[6].isEmpty());
                        vectors.add(new Vector(f[0], f[1], f[2], 0, 0, 0, 0));
                    }
                }
            }
        }
        Assert.assertEquals(23, vectors.size());
        return vectors;
    }

    private static long hex(String value) {
        return Long.parseUnsignedLong(value, 16);
    }

    private static final class Vector {
        final String caseId, input, outcome;
        final long l0, l1, l2, l3;

        Vector(String caseId, String input, String outcome, long l0, long l1, long l2, long l3) {
            this.caseId = caseId;
            this.input = input;
            this.outcome = outcome;
            this.l0 = l0;
            this.l1 = l1;
            this.l2 = l2;
            this.l3 = l3;
        }
    }
}
