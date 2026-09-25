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
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.time.temporal.ChronoUnit;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class QwpSchemaTextSourceNullE2ETest extends AbstractQwpWebSocketTest {
    private static final long UUID_HI = 0x123e4567e89b12d3L;
    private static final long UUID_LO = 0xa456426614174000L;

    @Test
    public void testLegacyLongTextTargetsDependOnNullBitmapPresence() throws Exception {
        runInContext(port -> {
            String[] targets = {"STRING", "VARCHAR", "SYMBOL"};
            for (String target : targets) {
                assertLegacyLong(port, target, false);
                assertLegacyLong(port, target, true);
            }
        });
    }

    @Test
    public void testLegacyLong256TextTargetsDependOnNullBitmapPresence() throws Exception {
        runInContext(port -> {
            String[] targets = {"STRING", "VARCHAR"};
            for (String target : targets) {
                assertLegacyLong256(port, target, false);
                assertLegacyLong256(port, target, true);
            }
        });
    }

    @Test
    public void testLegacyUuidTextTargetsDependOnNullBitmapPresence() throws Exception {
        runInContext(port -> {
            String[] targets = {"STRING", "VARCHAR"};
            for (String target : targets) {
                assertLegacyUuid(port, target, false);
                assertLegacyUuid(port, target, true);
            }
        });
    }

    @Test
    public void testSchemaSenderRejectsUuidSymbolConversionAndRecoversWholeRow() throws Exception {
        runInContext(port -> {
            assertPublicUuidSymbolUnsupported(port, false);
            assertPublicUuidSymbolUnsupported(port, true);
        });
    }

    private void assertLegacyLong(int port, String target, boolean bitmap) throws Exception {
        String tableName = "legacy_long_" + target.toLowerCase(Locale.ROOT) + (bitmap ? "_bitmap" : "_plain");
        execute("create table " + tableName
                + " (case_id long, v " + target + ", ts timestamp) timestamp(ts) partition by day wal");
        long[] values = bitmap
                ? new long[]{Long.MIN_VALUE}
                : new long[]{0, -1, 1, Long.MIN_VALUE + 1, Long.MAX_VALUE, Long.MIN_VALUE};
        try (WebSocketClient client = WebSocketClientFactory.newPlainTextInstance();
             QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
             QwpTableBuffer table = new QwpTableBuffer(tableName)) {
            connectLegacy(client, port);
            for (int row = 0; row < (bitmap ? 2 : values.length); row++) {
                table.getOrCreateColumn("case_id", QwpConstants.TYPE_LONG, false).addLong(row);
                if (!bitmap || row == 0) {
                    table.getOrCreateColumn("v", QwpConstants.TYPE_LONG, true).addLong(values[row]);
                }
                table.nextRow();
            }
            int length = encoder.encode(table);
            QwpFixedWidthColumnCursor wire = assertFixedWire(
                    encoder, length, QwpConstants.TYPE_LONG, bitmap, bitmap ? 2 : values.length);
            Assert.assertEquals(values.length, wire.getValueCount());
            Assert.assertEquals(Long.BYTES, wire.getValueSize());
            for (int i = 0; i < values.length; i++) {
                Assert.assertEquals(values[i], Unsafe.getLong(wire.getValuesAddress() + (long) i * Long.BYTES));
            }
            client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
            assertOk(client);
        }
        drainWalQueue();
        String expected = bitmap
                ? "case_id\tv\tn\n0\tnull\tfalse\n1\t\ttrue\n"
                : "case_id\tv\tn\n0\t0\tfalse\n1\t-1\tfalse\n2\t1\tfalse\n"
                  + "3\t-9223372036854775807\tfalse\n4\t9223372036854775807\tfalse\n5\t\ttrue\n";
        assertQuery("select case_id, v, v is null n from " + tableName + " order by case_id")
                .noLeakCheck()
                .expectSize().returns(expected);
    }

    private void assertLegacyLong256(int port, String target, boolean bitmap) throws Exception {
        String tableName = "legacy_long256_" + target.toLowerCase(Locale.ROOT) + (bitmap ? "_bitmap" : "_plain");
        execute("create table " + tableName
                + " (case_id long, v " + target + ", ts timestamp) timestamp(ts) partition by day wal");
        try (WebSocketClient client = WebSocketClientFactory.newPlainTextInstance();
             QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
             QwpTableBuffer table = new QwpTableBuffer(tableName)) {
            connectLegacy(client, port);
            for (int row = 0; row < (bitmap ? 2 : 1); row++) {
                table.getOrCreateColumn("case_id", QwpConstants.TYPE_LONG, false).addLong(row);
                if (row == 0) {
                    table.getOrCreateColumn("v", QwpConstants.TYPE_LONG256, true)
                            .addLong256(Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE);
                }
                table.nextRow();
            }
            int length = encoder.encode(table);
            QwpFixedWidthColumnCursor wire = assertFixedWire(
                    encoder, length, QwpConstants.TYPE_LONG256, bitmap, bitmap ? 2 : 1);
            Assert.assertEquals(1, wire.getValueCount());
            Assert.assertEquals(4 * Long.BYTES, wire.getValueSize());
            for (int i = 0; i < 4; i++) {
                Assert.assertEquals(Long.MIN_VALUE, Unsafe.getLong(wire.getValuesAddress() + (long) i * Long.BYTES));
            }
            client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
            assertOk(client);
        }
        drainWalQueue();
        String expected = bitmap
                ? "case_id\tv\tn\n0\t\tfalse\n1\t\ttrue\n"
                : "case_id\tv\tn\n0\t\ttrue\n";
        assertQuery("select case_id, v, v is null n from " + tableName + " order by case_id")
                .noLeakCheck()
                .expectSize().returns(expected);
    }

    private void assertLegacyUuid(int port, String target, boolean bitmap) throws Exception {
        String tableName = "legacy_uuid_" + target.toLowerCase(Locale.ROOT) + (bitmap ? "_bitmap" : "_plain");
        execute("create table " + tableName
                + " (case_id long, v " + target + ", ts timestamp) timestamp(ts) partition by day wal");
        long[][] values = bitmap
                ? new long[][]{{Long.MIN_VALUE, Long.MIN_VALUE}}
                : new long[][]{
                {UUID_LO, UUID_HI},
                {Long.MIN_VALUE, 0},
                {0, Long.MIN_VALUE},
                {Long.MIN_VALUE, Long.MIN_VALUE}
        };
        try (WebSocketClient client = WebSocketClientFactory.newPlainTextInstance();
             QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
             QwpTableBuffer table = new QwpTableBuffer(tableName)) {
            connectLegacy(client, port);
            for (int row = 0; row < (bitmap ? 2 : values.length); row++) {
                table.getOrCreateColumn("case_id", QwpConstants.TYPE_LONG, false).addLong(row);
                if (!bitmap || row == 0) {
                    table.getOrCreateColumn("v", QwpConstants.TYPE_UUID, true)
                            .addUuid(values[row][1], values[row][0]);
                }
                table.nextRow();
            }
            int length = encoder.encode(table);
            QwpFixedWidthColumnCursor wire = assertFixedWire(
                    encoder, length, QwpConstants.TYPE_UUID, bitmap, bitmap ? 2 : values.length);
            Assert.assertEquals(values.length, wire.getValueCount());
            Assert.assertEquals(2 * Long.BYTES, wire.getValueSize());
            for (int i = 0; i < values.length; i++) {
                long address = wire.getValuesAddress() + (long) i * 2 * Long.BYTES;
                Assert.assertEquals(values[i][0], Unsafe.getLong(address));
                Assert.assertEquals(values[i][1], Unsafe.getLong(address + Long.BYTES));
            }
            client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
            assertOk(client);
        }
        drainWalQueue();
        String expected = bitmap
                ? "case_id\tv\tn\n0\t80000000-0000-0000-8000-000000000000\tfalse\n1\t\ttrue\n"
                : "case_id\tv\tn\n"
                  + "0\t123e4567-e89b-12d3-a456-426614174000\tfalse\n"
                  + "1\t00000000-0000-0000-8000-000000000000\tfalse\n"
                  + "2\t80000000-0000-0000-0000-000000000000\tfalse\n"
                  + "3\t\ttrue\n";
        assertQuery("select case_id, v, v is null n from " + tableName + " order by case_id")
                .noLeakCheck()
                .expectSize().returns(expected);
    }

    private void assertPublicUuidSymbolUnsupported(
            int port,
            boolean sentinel
    ) throws Exception {
        String tableName = "schema_uuid_symbol_" + (sentinel ? "sentinel" : "ordinary");
        execute("create table " + tableName
                + " (v symbol, marker string, ts timestamp) timestamp(ts) partition by day wal");
        try (QwpWebSocketSender sender = connectWs(
                port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
            sender.table(tableName).stringColumn("v", "A").at(1_000_000, ChronoUnit.MICROS);
            sender.table(tableName).stringColumn("marker", "failed-B");
            long lo = sentinel ? Long.MIN_VALUE : UUID_LO;
            long hi = sentinel ? Long.MIN_VALUE : UUID_HI;
            LineSenderSchemaException error = Assert.assertThrows(
                    LineSenderSchemaException.class,
                    () -> sender.uuidColumn("v", lo, hi)
            );
            Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, error.getReason());
            Assert.assertFalse(error.isRetryable());
            Assert.assertTrue(error.getMessage(), error.getMessage().contains("table=" + tableName));
            Assert.assertTrue(error.getMessage(), error.getMessage().contains("column=v"));
            Assert.assertTrue(error.getMessage(), error.getMessage().contains("inputType=UUID"));
            Assert.assertTrue(error.getMessage(), error.getMessage().contains("targetType=SYMBOL"));
            sender.stringColumn("v", "C").at(2_000_000, ChronoUnit.MICROS);
            long fsn = sender.flushAndGetSequence();
            Assert.assertTrue(fsn >= 0);
            Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
        }
        drainWalQueue();
        assertQuery("select v, marker, ts from " + tableName + " order by ts")
                .noLeakCheck()
                .expectSize().timestamp("ts").returns("v\tmarker\tts\nA\t\t1970-01-01T00:00:01.000000Z\n"
                        + "C\t\t1970-01-01T00:00:02.000000Z\n");
    }

    private static QwpFixedWidthColumnCursor assertFixedWire(
            QwpWebSocketEncoder encoder,
            int length,
            byte sourceType,
            boolean bitmap,
            int rowCount
    ) throws Exception {
        Assert.assertEquals(0, Unsafe.getByte(encoder.getBuffer().getBufferPtr()
                + QwpConstants.HEADER_OFFSET_FLAGS) & QwpConstants.FLAG_SCHEMA);
        QwpMessageCursor message = new QwpMessageCursor();
        message.of(encoder.getBuffer().getBufferPtr(), length, new ObjList<>());
        Assert.assertTrue(message.hasNextTable());
        QwpTableBlockCursor table = message.nextTable();
        Assert.assertFalse(message.hasNextTable());
        Assert.assertEquals(rowCount, table.getRowCount());
        Assert.assertEquals(2, table.getColumnCount());
        Assert.assertEquals("case_id", table.getColumnDef(0).getName());
        Assert.assertEquals(QwpConstants.TYPE_LONG, table.getColumnDef(0).getTypeCode());
        Assert.assertEquals("v", table.getColumnDef(1).getName());
        Assert.assertEquals(sourceType, table.getColumnDef(1).getTypeCode());
        QwpFixedWidthColumnCursor values = table.getFixedWidthColumn(1);
        if (bitmap) {
            Assert.assertNotEquals(0, values.getNullBitmapAddress());
            Assert.assertEquals(0x02, Unsafe.getByte(values.getNullBitmapAddress()) & 0xff);
        } else {
            Assert.assertEquals(0, values.getNullBitmapAddress());
        }
        for (int row = 0; row < rowCount; row++) {
            Assert.assertTrue(table.hasNextRow());
            table.nextRow();
            Assert.assertEquals(bitmap ? row == 1 : row == rowCount - 1, table.isColumnNull(1));
        }
        Assert.assertFalse(table.hasNextRow());
        return values;
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
                Assert.assertTrue(value.readFrom(ptr, len, false));
                response.set(value);
            }

            @Override
            public void onClose(int code, String reason) {
                Assert.fail("unexpected close [code=" + code + ", reason=" + reason + ']');
            }
        }, 5_000));
        Assert.assertNotNull(response.get());
        Assert.assertTrue(response.get().getErrorMessage(), response.get().isSuccess());
    }
}
