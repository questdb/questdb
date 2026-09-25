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

public class QwpSchemaUuidTextE2ETest extends AbstractQwpWebSocketTest {
    private static final String CORPUS = "/io/questdb/client/cutlass/qwp/uuid-to-text.tsv";

    @Test
    public void testPublicSenderCorpusStoresCanonicalTextAndTargetNulls() throws Exception {
        List<Vector> vectors = readVectors();
        runInContext(port -> {
            for (Target target : Target.values()) {
                execute("create table " + target.tableName + " (case_id long, value " + target.sqlType
                        + ", ts timestamp) timestamp(ts) partition by day wal");
            }
            try (QwpWebSocketSender sender = connectWs(
                    port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                for (Target target : Target.values()) {
                    int row = 0;
                    for (Vector vector : vectors) {
                        if (vector.target == target) {
                            sender.table(target.tableName)
                                    .longColumn("case_id", row++)
                                    .uuidColumn("value", vector.lo, vector.hi)
                                    .at(1_000_000L + row, ChronoUnit.MICROS);
                        }
                    }
                }
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(fsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }
            drainWalQueue();
            for (Target target : Target.values()) {
                StringBuilder expected = new StringBuilder("case_id\tvalue\tn\n");
                int row = 0;
                for (Vector vector : vectors) {
                    if (vector.target == target) {
                        expected.append(row++).append('\t');
                        if (vector.expected != null) {
                            expected.append(vector.expected);
                        }
                        expected.append('\t').append(vector.expected == null).append('\n');
                    }
                }
                assertQuery("select case_id, value, value is null n from " + target.tableName + " order by case_id")
                        .noLeakCheck().expectSize().returns(expected.toString());
            }
        });
    }

    @Test
    public void testPublicSenderRollsBackPartialInvalidRow() throws Exception {
        runInContext(port -> {
            execute("create table schema_uuid_text_rows (s string, v varchar, marker string, bad long, "
                    + "ts timestamp) timestamp(ts) partition by day wal");
            try (QwpWebSocketSender sender = connectWs(
                    port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                sender.table("schema_uuid_text_rows")
                        .uuidColumn("s", 0xa456426614174000L, 0x123e4567e89b12d3L)
                        .uuidColumn("v", 0, Long.MIN_VALUE)
                        .at(1_000_000, ChronoUnit.MICROS);
                sender.stringColumn("marker", "failed-B");
                LineSenderSchemaException error = Assert.assertThrows(
                        LineSenderSchemaException.class,
                        () -> sender.uuidColumn("bad", 1, 2)
                );
                Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, error.getReason());
                Assert.assertFalse(error.isRetryable());
                Assert.assertTrue(error.getMessage(), error.getMessage().contains("column=bad"));
                Assert.assertTrue(error.getMessage(), error.getMessage().contains("inputType=UUID"));
                Assert.assertTrue(error.getMessage(), error.getMessage().contains("targetType=LONG"));
                sender.uuidColumn("s", Long.MIN_VALUE, Long.MIN_VALUE)
                        .uuidColumn("v", 81985529216486895L, -81985529216486896L)
                        .at(2_000_000, ChronoUnit.MICROS);
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(fsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }
            drainWalQueue();
            assertQuery("select s, v, marker, bad, s is null sn, v is null vn, ts "
                    + "from schema_uuid_text_rows order by ts")
                    .noLeakCheck().expectSize().timestamp("ts").returns("s\tv\tmarker\tbad\tsn\tvn\tts\n"
                            + "123e4567-e89b-12d3-a456-426614174000\t"
                            + "80000000-0000-0000-0000-000000000000\t\tnull\tfalse\tfalse\t"
                            + "1970-01-01T00:00:01.000000Z\n"
                            + "\tfedcba98-7654-3210-0123-456789abcdef\t\tnull\ttrue\tfalse\t"
                            + "1970-01-01T00:00:02.000000Z\n");
        });
    }

    @Test
    public void testTargetTextWireReplaysFromStoreAndForward() throws Exception {
        runInContext(port -> {
            execute("create table schema_uuid_text_sf (s string, v varchar, ts timestamp) "
                    + "timestamp(ts) partition by day wal");
            File sfRoot = temp.newFolder("qwp-schema-uuid-text-sf");
            String slot = new File(sfRoot, "default").getAbsolutePath();
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = new QwpTableBuffer("schema_uuid_text_sf")) {
                QwpSchemaBinding binding = new QwpSchemaBinding(table, describe(client, 601, "schema_uuid_text_sf"));
                binding.uuidColumn("s", 0xa456426614174000L, 0x123e4567e89b12d3L)
                        .uuidColumn("v", Long.MIN_VALUE, Long.MIN_VALUE);
                table.nextRow();
                int length = encoder.encodeSchema(table);
                QwpMessageCursor message = new QwpMessageCursor();
                message.of(encoder.getBuffer().getBufferPtr(), length, new ObjList<>());
                Assert.assertTrue(message.hasNextTable());
                QwpTableBlockCursor wire = message.nextTable();
                Assert.assertFalse(message.hasNextTable());
                Assert.assertEquals(2, wire.getColumnCount());
                Assert.assertEquals(1, wire.getRowCount());
                Assert.assertEquals(QwpConstants.TYPE_VARCHAR, wire.getColumnDef(0).getTypeCode());
                Assert.assertEquals(QwpConstants.TYPE_VARCHAR, wire.getColumnDef(1).getTypeCode());
                Assert.assertTrue(wire.hasNextRow());
                wire.nextRow();
                assertUtf8("123e4567-e89b-12d3-a456-426614174000", wire.getStringColumn(0).getUtf8Value());
                Assert.assertTrue(wire.isColumnNull(1));
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
            assertQuery("select s, v, v is null n from schema_uuid_text_sf")
                    .noLeakCheck().expectSize().returns("s\tv\tn\n"
                            + "123e4567-e89b-12d3-a456-426614174000\t\ttrue\n");
        });
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

    private static void assertUtf8(String expected, Utf8Sequence actual) {
        Assert.assertNotNull(actual);
        byte[] bytes = expected.getBytes(StandardCharsets.UTF_8);
        Assert.assertEquals(bytes.length, actual.size());
        for (int i = 0; i < bytes.length; i++) {
            Assert.assertEquals("byte " + i, bytes[i], actual.byteAt(i));
        }
    }

    private static List<Vector> readVectors() throws Exception {
        InputStream stream = QwpSchemaUuidTextE2ETest.class.getResourceAsStream(CORPUS);
        Assert.assertNotNull(CORPUS, stream);
        List<Vector> vectors = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty() || line.charAt(0) == '#') {
                    continue;
                }
                String[] fields = line.split("\\t", -1);
                Assert.assertEquals(line, 7, fields.length);
                Target target = Target.valueOf(fields[3]);
                Assert.assertEquals(line, "VARCHAR", fields[4]);
                long lo = Long.parseLong(fields[1]);
                long hi = Long.parseLong(fields[2]);
                String expected = "<NULL>".equals(fields[6]) ? null : fields[6];
                Assert.assertEquals(line, lo == Long.MIN_VALUE && hi == Long.MIN_VALUE,
                        "<NULL>".equals(fields[5]));
                Assert.assertEquals(line, lo == Long.MIN_VALUE && hi == Long.MIN_VALUE, expected == null);
                if (expected != null) {
                    Assert.assertEquals(line, toHex(expected.getBytes(StandardCharsets.UTF_8)), fields[5]);
                }
                vectors.add(new Vector(lo, hi, target, expected));
            }
        }
        Assert.assertEquals("shared corpus row count", 16, vectors.size());
        for (Target target : Target.values()) {
            int count = 0;
            for (Vector vector : vectors) {
                if (vector.target == target) {
                    count++;
                }
            }
            Assert.assertEquals(target.name(), 8, count);
        }
        return vectors;
    }

    private static String toHex(byte[] bytes) {
        StringBuilder sink = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            sink.append(Character.forDigit((value >>> 4) & 0xf, 16));
            sink.append(Character.forDigit(value & 0xf, 16));
        }
        return sink.toString();
    }

    private enum Target {
        STRING("STRING"),
        VARCHAR("VARCHAR");

        private final String sqlType;
        private final String tableName;

        Target(String sqlType) {
            this.sqlType = sqlType;
            this.tableName = "schema_uuid_" + sqlType.toLowerCase(Locale.ROOT);
        }
    }

    private static final class Vector {
        private final String expected;
        private final long hi;
        private final long lo;
        private final Target target;

        private Vector(long lo, long hi, Target target, String expected) {
            this.lo = lo;
            this.hi = hi;
            this.target = target;
            this.expected = expected;
        }
    }
}
