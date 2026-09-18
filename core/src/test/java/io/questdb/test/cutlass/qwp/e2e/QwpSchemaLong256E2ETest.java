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
import io.questdb.cutlass.qwp.protocol.QwpFixedWidthColumnCursor;
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

public class QwpSchemaLong256E2ETest extends AbstractQwpWebSocketTest {
    private static final String CORPUS = "/io/questdb/client/cutlass/qwp/long256-conversions.tsv";

    @Test
    public void testMissingTableInfersLong256FromFirstNullValue() throws Exception {
        runInContext(port -> {
            try (QwpWebSocketSender sender = connectWs(
                    port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                sender.table("schema_long256_infer")
                        .long256Column("value", Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE)
                        .at(1_000_000, ChronoUnit.MICROS);
                sender.table("schema_long256_infer")
                        .long256Column("value", 1, 2, 3, 4)
                        .at(2_000_000, ChronoUnit.MICROS);
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(fsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }
            drainWalQueue();
            assertQuery("select type from table_columns('schema_long256_infer') where \"column\" = 'value'")
                    .noLeakCheck().returnsOnce("type\nLONG256\n");
            assertQuery("select value, value is null n from schema_long256_infer order by timestamp")
                    .noLeakCheck().returnsOnce("value\tn\n\ttrue\n"
                            + "0x04000000000000000300000000000000020000000000000001\tfalse\n");
        });
    }

    @Test
    public void testPublicSenderCorpusStoresTargetValuesWithBitmapContext() throws Exception {
        List<Vector> vectors = readVectors();
        runInContext(port -> {
            for (Target target : Target.values()) {
                execute("create table " + target.tableName + " (case_id string, value " + target.sqlType
                        + ", ts timestamp) timestamp(ts) partition by day wal");
            }
            try (QwpWebSocketSender sender = connectWs(
                    port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                for (Target target : Target.values()) {
                    // The omitted value creates a bitmap before the sentinel row is encoded.
                    sender.table(target.tableName).stringColumn("case_id", "omitted")
                            .at(1_000_000L, ChronoUnit.MICROS);
                    long timestamp = 1_000_001L;
                    for (Vector vector : vectors) {
                        sender.table(target.tableName).stringColumn("case_id", vector.caseId)
                                .long256Column("value", vector.l0, vector.l1, vector.l2, vector.l3)
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
                    expected.append(vector.caseId).append('\t');
                    if (!vector.isNull()) {
                        expected.append(vector.expectedText);
                    }
                    expected.append('\t').append(vector.isNull()).append('\n');
                }
                assertQuery("select case_id, value, value is null n from " + target.tableName + " order by ts")
                        .noLeakCheck().returnsOnce(expected.toString());
            }
        });
    }

    @Test
    public void testPublicSenderRollsBackPartialRejectedRow() throws Exception {
        runInContext(port -> {
            execute("create table schema_long256_rows "
                    + "(value long256, marker string, bad uuid, ts timestamp) timestamp(ts) partition by day wal");
            try (QwpWebSocketSender sender = connectWs(
                    port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                sender.table("schema_long256_rows")
                        .long256Column("value", 1, 2, 3, 4)
                        .stringColumn("marker", "A")
                        .at(1_000_000L, ChronoUnit.MICROS);
                sender.long256Column("value", 5, 6, 7, 8)
                        .stringColumn("marker", "failed-B");
                LineSenderSchemaException error = Assert.assertThrows(
                        LineSenderSchemaException.class,
                        () -> sender.long256Column("bad", 9, 10, 11, 12)
                );
                Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, error.getReason());
                sender.long256Column("value", 13, 14, 15, 16)
                        .stringColumn("marker", "C")
                        .at(2_000_000L, ChronoUnit.MICROS);
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(fsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }
            drainWalQueue();
            assertQuery("select value, marker, bad from schema_long256_rows order by ts")
                    .noLeakCheck().returnsOnce("value\tmarker\tbad\n"
                            + "0x04000000000000000300000000000000020000000000000001\tA\t\n"
                            + "0x10000000000000000f000000000000000e000000000000000d\tC\t\n");
        });
    }

    @Test
    public void testTargetWireReplaysFromStoreAndForward() throws Exception {
        runInContext(port -> {
            execute("create table schema_long256_sf (l long256, s string, v varchar, ts timestamp) "
                    + "timestamp(ts) partition by day wal");
            File sfRoot = temp.newFolder("qwp-schema-long256-sf");
            String slot = new File(sfRoot, "default").getAbsolutePath();
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = new QwpTableBuffer("schema_long256_sf")) {
                QwpSchemaBinding binding = new QwpSchemaBinding(table, describe(client, 801, "schema_long256_sf"));
                binding.long256Column("l", 1, 2, 3, 4)
                        .long256Column("s", 1, 2, 3, 4)
                        .long256Column("v", 1, 2, 3, 4);
                table.nextRow();
                binding.long256Column("l", Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE)
                        .long256Column("s", Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE)
                        .long256Column("v", Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE);
                table.nextRow();

                int length = encoder.encodeSchema(table);
                QwpMessageCursor message = new QwpMessageCursor();
                message.of(encoder.getBuffer().getBufferPtr(), length, new ObjList<>());
                Assert.assertTrue(message.hasNextTable());
                QwpTableBlockCursor wire = message.nextTable();
                Assert.assertFalse(message.hasNextTable());
                Assert.assertEquals(3, wire.getColumnCount());
                Assert.assertEquals(2, wire.getRowCount());
                Assert.assertEquals(QwpConstants.TYPE_LONG256, wire.getColumnDef(0).getTypeCode());
                Assert.assertEquals(QwpConstants.TYPE_VARCHAR, wire.getColumnDef(1).getTypeCode());
                Assert.assertEquals(QwpConstants.TYPE_VARCHAR, wire.getColumnDef(2).getTypeCode());
                Assert.assertTrue(wire.hasNextRow());
                wire.nextRow();
                QwpFixedWidthColumnCursor value = wire.getFixedWidthColumn(0);
                Assert.assertEquals(1, value.getLong256_0());
                Assert.assertEquals(2, value.getLong256_1());
                Assert.assertEquals(3, value.getLong256_2());
                Assert.assertEquals(4, value.getLong256_3());
                String expected = "0x04000000000000000300000000000000020000000000000001";
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
            assertQuery("select l, s, v, l is null ln, s is null sn, v is null vn from schema_long256_sf order by ts")
                    .noLeakCheck().returnsOnce("l\ts\tv\tln\tsn\tvn\n"
                            + "0x04000000000000000300000000000000020000000000000001\t"
                            + "0x04000000000000000300000000000000020000000000000001\t"
                            + "0x04000000000000000300000000000000020000000000000001\tfalse\tfalse\tfalse\n"
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
        InputStream stream = QwpSchemaLong256E2ETest.class.getResourceAsStream(CORPUS);
        Assert.assertNotNull(CORPUS, stream);
        List<Vector> vectors = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            Assert.assertEquals("# case_id\tinput_l0\tinput_l1\tinput_l2\tinput_l3\toutcome\texpected_text", reader.readLine());
            String line;
            while ((line = reader.readLine()) != null) {
                String[] fields = line.split("\\t", -1);
                Assert.assertEquals(line, 7, fields.length);
                Assert.assertTrue(line, "VALUE".equals(fields[5]) || "NULL".equals(fields[5]));
                Assert.assertEquals(line, "NULL".equals(fields[5]), "-".equals(fields[6]));
                vectors.add(new Vector(
                        fields[0],
                        Long.parseLong(fields[1]),
                        Long.parseLong(fields[2]),
                        Long.parseLong(fields[3]),
                        Long.parseLong(fields[4]),
                        fields[5],
                        fields[6]
                ));
            }
        }
        Assert.assertEquals("shared corpus row count", 8, vectors.size());
        return vectors;
    }

    private enum Target {
        LONG256("LONG256"),
        STRING("STRING"),
        VARCHAR("VARCHAR");

        private final String sqlType;
        private final String tableName;

        Target(String sqlType) {
            this.sqlType = sqlType;
            this.tableName = "schema_long256_" + sqlType.toLowerCase(Locale.ROOT);
        }
    }

    private static final class Vector {
        private final String caseId;
        private final String expectedText;
        private final long l0;
        private final long l1;
        private final long l2;
        private final long l3;
        private final String outcome;

        private Vector(String caseId, long l0, long l1, long l2, long l3, String outcome, String expectedText) {
            this.caseId = caseId;
            this.l0 = l0;
            this.l1 = l1;
            this.l2 = l2;
            this.l3 = l3;
            this.outcome = outcome;
            this.expectedText = expectedText;
        }

        private boolean isNull() {
            return "NULL".equals(outcome);
        }
    }
}
