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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpGeoHashColumnCursor;
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

public class QwpSchemaStringGeoHashE2ETest extends AbstractQwpWebSocketTest {
    private static final String CORPUS = "/io/questdb/client/cutlass/qwp/string-geohash-conversions.tsv";

    @Test
    public void testLegacyVarcharAndNativeGeoHashBlockContexts() throws Exception {
        runInContext(port -> {
            execute("create table legacy_geo_text (value geohash(8b), ts timestamp) timestamp(ts) partition by day wal");
            try (WebSocketClient client = connectLegacy(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = new QwpTableBuffer("legacy_geo_text")) {
                QwpTableBuffer.ColumnBuffer value = table.getOrCreateColumn("value", QwpConstants.TYPE_VARCHAR, true);
                QwpTableBuffer.ColumnBuffer ts = table.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP);
                value.addString("zz");
                ts.addLong(1);
                table.nextRow();
                value.addString("");
                ts.addLong(2);
                table.nextRow();
                value.addNull();
                ts.addLong(3);
                table.nextRow();
                ts.addLong(4);
                table.nextRow();
                int length = encoder.encode(table);
                QwpTableBlockCursor wire = parseSingleTable(encoder, length);
                Assert.assertEquals(2, wire.getColumnCount());
                Assert.assertEquals(QwpConstants.TYPE_VARCHAR, wire.getColumnDef(0).getTypeCode());
                Assert.assertTrue(wire.hasNextRow());
                wire.nextRow();
                Assert.assertFalse(wire.isColumnNull(0));
                Assert.assertTrue(wire.hasNextRow());
                wire.nextRow();
                Assert.assertFalse("empty is source-present", wire.isColumnNull(0));
                Assert.assertTrue(wire.hasNextRow());
                wire.nextRow();
                Assert.assertTrue(wire.isColumnNull(0));
                Assert.assertTrue(wire.hasNextRow());
                wire.nextRow();
                Assert.assertTrue(wire.isColumnNull(0));
                Assert.assertFalse(wire.hasNextRow());
                client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                assertResponse(client, true, null);
            }
            drainWalQueue();
            assertGeoRows("legacy_geo_text", 8, new long[]{0xff, -1, -1, -1}, new boolean[]{false, true, true, true});
            for (int bits : new int[]{7, 16, 31, 32, 60}) {
                assertLegacyVarcharContext(port, bits);
            }

            for (int bits : new int[]{8, 16, 24, 32, 40, 48, 56}) {
                assertNativeMaxContext(port, bits, false);
                assertNativeMaxContext(port, bits, true);
            }
            assertLegacyInvalid(port, "a", 5, "invalid");
            assertLegacyInvalid(port, "zzzzzzzzzzz", 60, "short");
        });
    }

    @Test
    public void testPublicSenderAllPrecisionsKeepMaximumNonNull() throws Exception {
        runInContext(port -> {
            try (QwpWebSocketSender sender = connectWs(port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                for (int bits = 1; bits <= 60; bits++) {
                    String table = "schema_geo_bits_" + bits;
                    execute("create table " + table + " (value geohash(" + bits + "b), ts timestamp) timestamp(ts) partition by day wal");
                    sender.table(table).stringColumn("value", maxText(bits)).at(bits, ChronoUnit.MICROS);
                }
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(fsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
                for (int bits = 1; bits <= 60; bits++) {
                    sender.table("schema_geo_bits_" + bits).stringColumn("value", "").at(100 + bits, ChronoUnit.MICROS);
                    sender.table("schema_geo_bits_" + bits).stringColumn("value", null).at(200 + bits, ChronoUnit.MICROS);
                }
                fsn = sender.flushAndGetSequence();
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }
            drainWalQueue();
            for (int bits = 1; bits <= 60; bits++) {
                assertGeoRows("schema_geo_bits_" + bits, bits,
                        new long[]{mask(bits), -1, -1}, new boolean[]{false, true, true});
            }
        });
    }

    @Test
    public void testPublicSenderCorpusAndPartialRowRecovery() throws Exception {
        List<Vector> vectors = readVectors();
        runInContext(port -> {
            try (QwpWebSocketSender sender = connectWs(port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                for (Vector v : vectors) {
                    String table = "schema_geo_case_" + v.caseId;
                    execute("create table " + table + " (value geohash(" + v.bits + "b), marker string, ts timestamp) timestamp(ts) partition by day wal");
                    sender.table(table);
                    if (v.outcome.equals("INVALID")) {
                        LineSenderSchemaException error = Assert.assertThrows(LineSenderSchemaException.class,
                                () -> sender.stringColumn("value", v.input));
                        Assert.assertEquals(v.caseId, LineSenderSchemaException.Reason.INVALID_VALUE, error.getReason());
                    } else {
                        sender.stringColumn("value", v.input).stringColumn("marker", "ok").at(1, ChronoUnit.MICROS);
                    }
                }
                execute("create table schema_geo_recovery (value geohash(8b), marker string, ts timestamp) timestamp(ts) partition by day wal");
                sender.table("schema_geo_recovery").stringColumn("value", "00").stringColumn("marker", "A").at(1, ChronoUnit.MICROS);
                sender.table("schema_geo_recovery").stringColumn("marker", "failed-B");
                LineSenderSchemaException invalid = Assert.assertThrows(
                        LineSenderSchemaException.class,
                        () -> sender.stringColumn("value", "a")
                );
                Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, invalid.getReason());
                sender.table("schema_geo_recovery").stringColumn("value", "zz").stringColumn("marker", "C").at(2, ChronoUnit.MICROS);
                sender.table("schema_geo_recovery").stringColumn("marker", "failed-designated");
                LineSenderSchemaException designated = Assert.assertThrows(
                        LineSenderSchemaException.class,
                        () -> sender.stringColumn("ts", "0")
                );
                Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, designated.getReason());
                sender.table("schema_geo_recovery").stringColumn("marker", "D").at(3, ChronoUnit.MICROS);
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }
            drainWalQueue();
            for (Vector v : vectors) {
                if (!v.outcome.equals("INVALID")) {
                    assertGeoRows("schema_geo_case_" + v.caseId, v.bits,
                            new long[]{v.value}, new boolean[]{v.outcome.equals("NULL")});
                } else {
                    assertQuery("select count() from schema_geo_case_" + v.caseId).noLeakCheck().returnsOnce("count\n0\n");
                }
            }
            assertQuery("select marker from schema_geo_recovery order by ts")
                    .noLeakCheck().returnsOnce("marker\nA\nC\nD\n");
            assertGeoRows("schema_geo_recovery", 8,
                    new long[]{0, 0xff, -1}, new boolean[]{false, false, true});
        });
    }

    private void assertNativeMaxContext(int port, int bits, boolean bitmap) throws Exception {
        String tableName = "legacy_geo_native_" + bits + (bitmap ? "_bitmap" : "_plain");
        execute("create table " + tableName + " (value geohash(" + bits + "b), ts timestamp) timestamp(ts) partition by day wal");
        try (WebSocketClient client = connectLegacy(port);
             QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
             QwpTableBuffer table = new QwpTableBuffer(tableName)) {
            QwpTableBuffer.ColumnBuffer value = table.getOrCreateColumn("value", QwpConstants.TYPE_GEOHASH, true);
            QwpTableBuffer.ColumnBuffer ts = table.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP);
            value.addGeoHash(mask(bits), bits);
            ts.addLong(1);
            table.nextRow();
            // Raw native all-ones also models a hypothetical legacy local empty lowering.
            value.addGeoHash(-1, bits);
            ts.addLong(2);
            table.nextRow();
            if (bitmap) {
                ts.addLong(3);
                table.nextRow();
            }
            int length = encoder.encode(table);
            QwpTableBlockCursor wire = parseSingleTable(encoder, length);
            Assert.assertEquals(QwpConstants.TYPE_GEOHASH, wire.getColumnDef(0).getTypeCode());
            QwpGeoHashColumnCursor geo = wire.getGeoHashColumn(0);
            Assert.assertEquals(bits, geo.getPrecision());
            Assert.assertTrue(wire.hasNextRow());
            wire.nextRow();
            Assert.assertEquals(!bitmap, wire.isColumnNull(0));
            if (bitmap) Assert.assertEquals(mask(bits), geo.getGeoHash());
            Assert.assertTrue(wire.hasNextRow());
            wire.nextRow();
            Assert.assertEquals(!bitmap, wire.isColumnNull(0));
            if (bitmap) Assert.assertEquals(mask(bits), geo.getGeoHash());
            if (bitmap) {
                Assert.assertTrue(wire.hasNextRow());
                wire.nextRow();
                Assert.assertTrue(wire.isColumnNull(0));
            }
            Assert.assertFalse(wire.hasNextRow());
            client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
            assertResponse(client, true, null);
        }
        drainWalQueue();
        assertGeoRows(tableName, bits,
                bitmap ? new long[]{mask(bits), mask(bits), -1} : new long[]{-1, -1},
                bitmap ? new boolean[]{false, false, true} : new boolean[]{true, true});
    }

    private void assertLegacyVarcharContext(int port, int bits) throws Exception {
        String tableName = "legacy_geo_text_" + bits;
        execute("create table " + tableName + " (value geohash(" + bits + "b), ts timestamp) timestamp(ts) partition by day wal");
        try (WebSocketClient client = connectLegacy(port);
             QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
             QwpTableBuffer table = new QwpTableBuffer(tableName)) {
            QwpTableBuffer.ColumnBuffer value = table.getOrCreateColumn("value", QwpConstants.TYPE_VARCHAR, true);
            QwpTableBuffer.ColumnBuffer ts = table.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP);
            value.addString(maxText(bits));
            ts.addLong(1);
            table.nextRow();
            value.addString("");
            ts.addLong(2);
            table.nextRow();
            value.addNull();
            ts.addLong(3);
            table.nextRow();
            ts.addLong(4);
            table.nextRow();
            int length = encoder.encode(table);
            QwpTableBlockCursor wire = parseSingleTable(encoder, length);
            Assert.assertEquals(QwpConstants.TYPE_VARCHAR, wire.getColumnDef(0).getTypeCode());
            Assert.assertTrue(wire.hasNextRow());
            wire.nextRow();
            Assert.assertFalse(wire.isColumnNull(0));
            Assert.assertTrue(wire.hasNextRow());
            wire.nextRow();
            Assert.assertFalse("empty remains source-present", wire.isColumnNull(0));
            Assert.assertTrue(wire.hasNextRow());
            wire.nextRow();
            Assert.assertTrue(wire.isColumnNull(0));
            Assert.assertTrue(wire.hasNextRow());
            wire.nextRow();
            Assert.assertTrue(wire.isColumnNull(0));
            Assert.assertFalse(wire.hasNextRow());
            client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
            assertResponse(client, true, null);
        }
        drainWalQueue();
        assertGeoRows(tableName, bits, new long[]{mask(bits), -1, -1, -1}, new boolean[]{false, true, true, true});
    }

    private void assertLegacyInvalid(int port, String input, int bits, String suffix) throws Exception {
        String tableName = "legacy_geo_bad_" + suffix;
        execute("create table " + tableName + " (value geohash(" + bits + "b), ts timestamp) timestamp(ts) partition by day wal");
        try (WebSocketClient client = connectLegacy(port);
             QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
             QwpTableBuffer table = new QwpTableBuffer(tableName)) {
            table.getOrCreateColumn("value", QwpConstants.TYPE_VARCHAR, true).addString(input);
            table.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(1);
            table.nextRow();
            int length = encoder.encode(table);
            client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
            assertResponse(client, false, "geohash");
        }
        drainWalQueue();
        assertQuery("select count() from " + tableName).noLeakCheck().returnsOnce("count\n0\n");
    }

    private void assertGeoRows(String table, int bits, long[] values, boolean[] nulls) throws Exception {
        try (RecordCursorFactory factory = select("select value, value is null from " + table + " order by ts");
             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Record record = cursor.getRecord();
            int type = ColumnType.getGeoHashTypeWithBits(bits);
            for (int i = 0; i < values.length; i++) {
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(nulls[i], record.getBool(1));
                if (!nulls[i]) Assert.assertEquals(values[i], geoValue(record, type));
            }
            Assert.assertFalse(cursor.hasNext());
        }
    }

    private static long geoValue(Record record, int type) {
        switch (ColumnType.tagOf(type)) {
            case ColumnType.GEOBYTE:
                return record.getGeoByte(0) & 0xffL;
            case ColumnType.GEOSHORT:
                return record.getGeoShort(0) & 0xffffL;
            case ColumnType.GEOINT:
                return record.getGeoInt(0) & 0xffffffffL;
            default:
                return record.getGeoLong(0);
        }
    }

    private static long mask(int bits) {
        return (1L << bits) - 1;
    }

    private static String maxText(int bits) {
        return "zzzzzzzzzzzz".substring(0, (bits + 4) / 5);
    }

    private static WebSocketClient connectLegacy(int port) {
        WebSocketClient client = WebSocketClientFactory.newPlainTextInstance();
        boolean ok = false;
        try {
            client.connect("127.0.0.1", port);
            client.upgrade("/write/v4", null);
            Assert.assertFalse(client.isQwpSchemaEnabled());
            ok = true;
            return client;
        } finally {
            if (!ok) {
                client.close();
            }
        }
    }

    private static void assertResponse(WebSocketClient client, boolean success, String part) {
        AtomicReference<WebSocketResponse> ref = new AtomicReference<>();
        Assert.assertTrue(client.receiveFrame(new WebSocketFrameHandler() {
            @Override
            public void onBinaryMessage(long ptr, int len) {
                WebSocketResponse response = new WebSocketResponse();
                Assert.assertTrue(response.readFrom(ptr, len));
                ref.set(response);
            }

            @Override
            public void onClose(int code, String reason) {
                Assert.fail("unexpected close " + code + ": " + reason);
            }
        }, 5_000));
        Assert.assertNotNull(ref.get());
        Assert.assertEquals(success, ref.get().isSuccess());
        Assert.assertEquals(0, ref.get().getSequence());
        if (part != null) {
            Assert.assertTrue(ref.get().getErrorMessage(), ref.get().getErrorMessage().contains(part));
        }
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
        List<Vector> result = new ArrayList<>();
        try (InputStream in = QwpSchemaStringGeoHashE2ETest.class.getResourceAsStream(CORPUS)) {
            Assert.assertNotNull(CORPUS, in);
            try (BufferedReader r = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                Assert.assertEquals("# case_id\tinput\tbits\toutcome\tvalue_hex", r.readLine());
                String line;
                while ((line = r.readLine()) != null) {
                    String[] f = line.split("\\t", -1);
                    Assert.assertEquals(line, 5, f.length);
                    Assert.assertTrue(line, f[3].equals("VALUE") || f[3].equals("NULL") || f[3].equals("INVALID"));
                    int bits = Integer.parseInt(f[2]);
                    Assert.assertTrue(bits >= 1 && bits <= 60);
                    String input = f[1].equals("<NULL>") ? null : f[1].equals("<EMPTY>") ? "" : f[1];
                    long value = f[3].equals("VALUE") ? Long.parseUnsignedLong(f[4], 16) : -1;
                    Assert.assertEquals(line, f[3].equals("VALUE"), !f[4].isEmpty());
                    result.add(new Vector(f[0], input, bits, f[3], value));
                }
            }
        }
        Assert.assertEquals(18, result.size());
        return result;
    }

    private static final class Vector {
        final int bits;
        final String caseId;
        final String input;
        final String outcome;
        final long value;

        Vector(String caseId, String input, int bits, String outcome, long value) {
            this.caseId = caseId;
            this.input = input;
            this.bits = bits;
            this.outcome = outcome;
            this.value = value;
        }
    }
}
