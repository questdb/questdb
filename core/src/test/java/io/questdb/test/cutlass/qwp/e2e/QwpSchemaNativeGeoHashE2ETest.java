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
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaBinding;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaProtocol;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaResponse;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpGeoHashColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpMessageCursor;
import io.questdb.cutlass.qwp.protocol.QwpTableBlockCursor;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8Sequence;
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
import java.util.concurrent.atomic.AtomicReference;

public class QwpSchemaNativeGeoHashE2ETest extends AbstractQwpWebSocketTest {
    private static final String CORPUS = "/io/questdb/client/cutlass/qwp/native-geohash-conversions.tsv";

    @Test
    public void testMissingTableInfersSourcePrecisionAfterOmittedRow() throws Exception {
        runInContext(port -> {
            try (QwpWebSocketSender sender = connectWs(
                    port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                sender.table("schema_native_geo_infer").at(1_000_000, ChronoUnit.MICROS);
                sender.table("schema_native_geo_infer")
                        .geoHashColumn("value", "u33d")
                        .at(2_000_000, ChronoUnit.MICROS);
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(fsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }
            drainWalQueue();
            assertQuery("select type from table_columns('schema_native_geo_infer') where \"column\" = 'value'")
                    .noLeakCheck().noRandomAccess().returns("type\nGEOHASH(4c)\n");
            assertQuery("select value, value is null n from schema_native_geo_infer order by timestamp")
                    .noLeakCheck().expectSize().returns("value\tn\n\ttrue\nu33d\tfalse\n");
        });
    }

    @Test
    public void testPublicSenderCorpusStoresBinaryTextWithFixedWidth() throws Exception {
        List<Vector> vectors = readVectors();
        runInContext(port -> {
            for (Target target : Target.values()) {
                execute("create table " + target.tableName + " (case_id string, value " + target.sqlType
                        + ", ts timestamp) timestamp(ts) partition by day wal");
            }
            try (QwpWebSocketSender sender = connectWs(
                    port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                for (Target target : Target.values()) {
                    sender.table(target.tableName).stringColumn("case_id", "omitted")
                            .at(1_000_000L, ChronoUnit.MICROS);
                    long timestamp = 1_000_001L;
                    for (Vector vector : vectors) {
                        sender.table(target.tableName).stringColumn("case_id", vector.caseId)
                                .geoHashColumn("value", vector.inputBits, vector.precisionBits)
                                .at(timestamp++, ChronoUnit.MICROS);
                    }
                }
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(fsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }
            drainWalQueue();
            for (Target target : Target.values()) {
                StringBuilder expected = new StringBuilder("case_id\tvalue\tn\n")
                        .append("omitted\t\ttrue\n");
                for (Vector vector : vectors) {
                    expected.append(vector.caseId).append('\t').append(vector.expectedText)
                            .append("\tfalse\n");
                }
                assertQuery("select case_id, value, value is null n from " + target.tableName + " order by ts")
                        .noLeakCheck().expectSize().returns(expected.toString());
            }
        });
    }

    @Test
    public void testPublicSenderPreservesExactPrecisionAndRollsBackMismatch() throws Exception {
        runInContext(port -> {
            execute("create table schema_native_geo_rows "
                    + "(g20 geohash(4c), g8 geohash(8b), marker string, ts timestamp) "
                    + "timestamp(ts) partition by day wal");
            try (QwpWebSocketSender sender = connectWs(
                    port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                sender.table("schema_native_geo_rows")
                        .geoHashColumn("g20", "u33d")
                        .geoHashColumn("g8", 0xff, 8)
                        .stringColumn("marker", "A")
                        .at(1_000_000L, ChronoUnit.MICROS);
                sender.geoHashColumn("g20", 0x11111, 20)
                        .stringColumn("marker", "failed-B");
                LineSenderSchemaException error = Assert.assertThrows(
                        LineSenderSchemaException.class,
                        () -> sender.geoHashColumn("g8", 1, 7)
                );
                Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, error.getReason());
                sender.geoHashColumn("g20", 0x12345, 20)
                        .geoHashColumn("g8", 0, 8)
                        .stringColumn("marker", "C")
                        .at(2_000_000L, ChronoUnit.MICROS);
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(fsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }
            drainWalQueue();
            assertQuery("select g20, g8, marker, g8 is null n from schema_native_geo_rows order by ts")
                    .noLeakCheck().expectSize().returns("g20\tg8\tmarker\tn\n"
                            + "u33d\t11111111\tA\tfalse\n"
                            + "28u5\t00000000\tC\tfalse\n");
        });
    }

    @Test
    public void testTargetNativeWireReplaysFromStoreAndForward() throws Exception {
        runInContext(port -> {
            execute("create table schema_native_geo_sf (g geohash(4c), s string, v varchar, ts timestamp) "
                    + "timestamp(ts) partition by day wal");
            File sfRoot = temp.newFolder("qwp-schema-native-geohash-sf");
            String slot = new File(sfRoot, "default").getAbsolutePath();
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = new QwpTableBuffer("schema_native_geo_sf")) {
                QwpSchemaBinding binding = new QwpSchemaBinding(table, describe(client, 901, "schema_native_geo_sf"));
                binding.geoHashColumn("g", 0xabcde, 20)
                        .geoHashColumn("s", 0xabcde, 20)
                        .geoHashColumn("v", 0xabcde, 20);
                table.nextRow();
                table.nextRow();

                int length = encoder.encodeSchema(table);
                QwpMessageCursor message = new QwpMessageCursor();
                message.of(encoder.getBuffer().getBufferPtr(), length, new ObjList<>());
                Assert.assertTrue(message.hasNextTable());
                QwpTableBlockCursor wire = message.nextTable();
                Assert.assertFalse(message.hasNextTable());
                Assert.assertEquals(3, wire.getColumnCount());
                Assert.assertEquals(2, wire.getRowCount());
                Assert.assertEquals(QwpConstants.TYPE_GEOHASH, wire.getColumnDef(0).getTypeCode());
                Assert.assertEquals(QwpConstants.TYPE_VARCHAR, wire.getColumnDef(1).getTypeCode());
                Assert.assertEquals(QwpConstants.TYPE_VARCHAR, wire.getColumnDef(2).getTypeCode());
                QwpGeoHashColumnCursor geo = wire.getGeoHashColumn(0);
                Assert.assertEquals(20, geo.getPrecision());
                Assert.assertTrue(wire.hasNextRow());
                wire.nextRow();
                Assert.assertEquals(0xabcde, geo.getGeoHash());
                String expected = "10101011110011011110";
                assertUtf8(expected, wire.getStringColumn(1).getUtf8Value());
                assertUtf8(expected, wire.getStringColumn(2).getUtf8Value());
                Assert.assertTrue(wire.hasNextRow());
                wire.nextRow();
                Assert.assertTrue(wire.isColumnNull(0));
                Assert.assertTrue(wire.isColumnNull(1));
                Assert.assertTrue(wire.isColumnNull(2));
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
            assertQuery("select g, s, v, g is null gn, s is null sn, v is null vn from schema_native_geo_sf")
                    .noLeakCheck().expectSize().returns("g\ts\tv\tgn\tsn\tvn\n"
                            + "pg6y\t10101011110011011110\t10101011110011011110\tfalse\tfalse\tfalse\n"
                            + "\t\t\ttrue\ttrue\ttrue\n");
        });
    }

    private static void assertUtf8(String expected, Utf8Sequence actual) {
        Assert.assertNotNull(actual);
        byte[] bytes = expected.getBytes(StandardCharsets.UTF_8);
        Assert.assertEquals(bytes.length, actual.size());
        for (int i = 0; i < bytes.length; i++) {
            Assert.assertEquals("byte " + i, bytes[i], actual.byteAt(i));
        }
    }

    private static WebSocketClient connect(int port) {
        WebSocketClient client = WebSocketClientFactory.newPlainTextInstance();
        boolean success = false;
        try {
            client.requestQwpSchema();
            client.connect("127.0.0.1", port);
            client.upgrade("/write/v4", null);
            Assert.assertTrue(client.isQwpSchemaEnabled());
            success = true;
            return client;
        } finally {
            if (!success) {
                client.close();
            }
        }
    }

    private static QwpSchemaResponse describe(WebSocketClient client, long requestId, String tableName) {
        byte[] request = QwpSchemaProtocol.encodeDescribe(requestId, tableName);
        long address = Unsafe.malloc(request.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < request.length; i++) {
                Unsafe.putByte(address + i, request[i]);
            }
            client.sendBinary(address, request.length);
        } finally {
            Unsafe.free(address, request.length, MemoryTag.NATIVE_DEFAULT);
        }
        AtomicReference<QwpSchemaResponse> response = new AtomicReference<>();
        Assert.assertTrue(client.receiveFrame(new WebSocketFrameHandler() {
            @Override
            public void onBinaryMessage(long payloadPtr, int payloadLen) {
                response.set(QwpSchemaProtocol.decodeResponse(payloadPtr, payloadLen));
            }

            @Override
            public void onClose(int code, String reason) {
                Assert.fail("unexpected close [code=" + code + ", reason=" + reason + ']');
            }
        }, 5_000));
        Assert.assertNotNull(response.get());
        Assert.assertEquals(requestId, response.get().getRequestId());
        return response.get();
    }

    private static List<Vector> readVectors() throws Exception {
        InputStream stream = QwpSchemaNativeGeoHashE2ETest.class.getResourceAsStream(CORPUS);
        Assert.assertNotNull(CORPUS, stream);
        List<Vector> vectors = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            Assert.assertEquals("# case_id\tinput_bits\tprecision_bits\texpected_value_hex\texpected_text", reader.readLine());
            String line;
            while ((line = reader.readLine()) != null) {
                String[] fields = line.split("\t", -1);
                Assert.assertEquals(line, 5, fields.length);
                vectors.add(new Vector(
                        fields[0],
                        Long.parseLong(fields[1]),
                        Integer.parseInt(fields[2]),
                        fields[4]
                ));
            }
        }
        Assert.assertEquals("shared corpus row count", 15, vectors.size());
        return vectors;
    }

    private enum Target {
        STRING("STRING"),
        VARCHAR("VARCHAR");

        private final String sqlType;
        private final String tableName;

        Target(String sqlType) {
            this.sqlType = sqlType;
            this.tableName = "schema_native_geo_" + sqlType.toLowerCase(Locale.ROOT);
        }
    }

    private static final class Vector {
        private final String caseId;
        private final String expectedText;
        private final long inputBits;
        private final int precisionBits;

        private Vector(String caseId, long inputBits, int precisionBits, String expectedText) {
            this.caseId = caseId;
            this.inputBits = inputBits;
            this.precisionBits = precisionBits;
            this.expectedText = expectedText;
        }
    }
}
