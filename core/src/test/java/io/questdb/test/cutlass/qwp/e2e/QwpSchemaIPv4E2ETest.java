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

public class QwpSchemaIPv4E2ETest extends AbstractQwpWebSocketTest {
    private static final String CORPUS = "/io/questdb/client/cutlass/qwp/ipv4-conversions.tsv";

    @Test
    public void testPublicSenderCorpusStoresTargetValuesAndRollsBackInvalidRows() throws Exception {
        List<Vector> vectors = readVectors();
        runInContext(port -> {
            for (Target target : Target.values()) {
                execute("create table " + target.tableName + " (case_id long, value " + target.sqlType
                        + ", ts timestamp) timestamp(ts) partition by day wal");
            }
            try (QwpWebSocketSender sender = connectWs(
                    port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                for (Target target : Target.values()) {
                    boolean duplicateChecked = false;
                    for (int row = 0; row < vectors.size(); row++) {
                        Vector vector = vectors.get(row);
                        sender.table(target.tableName).longColumn("case_id", row);
                        if (vector.invalid) {
                            LineSenderSchemaException error = Assert.assertThrows(
                                    LineSenderSchemaException.class,
                                    () -> append(sender, vector)
                            );
                            Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, error.getReason());
                            Assert.assertFalse(error.isRetryable());
                        } else {
                            append(sender, vector);
                            if (!duplicateChecked) {
                                sender.ipv4Column("value", "not-an-ip");
                                duplicateChecked = true;
                            }
                            sender.at(1_000_000L + row, ChronoUnit.MICROS);
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
                for (int row = 0; row < vectors.size(); row++) {
                    Vector vector = vectors.get(row);
                    if (!vector.invalid) {
                        expected.append(row).append('\t');
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
    public void testMissingTableInfersIPv4FromFirstNullValue() throws Exception {
        runInContext(port -> {
            try (QwpWebSocketSender sender = connectWs(
                    port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                sender.table("schema_ipv4_infer").ipv4Column("value", 0)
                        .at(1_000_000, ChronoUnit.MICROS);
                sender.table("schema_ipv4_infer").ipv4Column("value", "10.20.30.40")
                        .at(2_000_000, ChronoUnit.MICROS);
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(fsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }
            drainWalQueue();
            assertQuery("select type from table_columns('schema_ipv4_infer') where \"column\" = 'value'")
                    .noLeakCheck().noRandomAccess().returns("type\nIPv4\n");
            assertQuery("select value, value is null n from schema_ipv4_infer order by timestamp")
                    .noLeakCheck().expectSize().returns("value\tn\n\ttrue\n10.20.30.40\tfalse\n");
        });
    }

    @Test
    public void testTargetWireReplaysFromStoreAndForward() throws Exception {
        runInContext(port -> {
            execute("create table schema_ipv4_sf (ip IPv4, s string, v varchar, ts timestamp) "
                    + "timestamp(ts) partition by day wal");
            File sfRoot = temp.newFolder("qwp-schema-ipv4-sf");
            String slot = new File(sfRoot, "default").getAbsolutePath();
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = new QwpTableBuffer("schema_ipv4_sf")) {
                QwpSchemaBinding binding = new QwpSchemaBinding(table, describe(client, 701, "schema_ipv4_sf"));
                binding.ipv4Column("ip", 0xc0a80101)
                        .ipv4Column("s", 0x0a141e28)
                        .ipv4Column("v", "1.2.3.4");
                table.nextRow();
                binding.ipv4Column("ip", 0)
                        .ipv4Column("s", ".0.0.0.0.")
                        .ipv4Column("v", 0);
                table.nextRow();

                int length = encoder.encodeSchema(table);
                QwpMessageCursor message = new QwpMessageCursor();
                message.of(encoder.getBuffer().getBufferPtr(), length, new ObjList<>());
                Assert.assertTrue(message.hasNextTable());
                QwpTableBlockCursor wire = message.nextTable();
                Assert.assertFalse(message.hasNextTable());
                Assert.assertEquals(3, wire.getColumnCount());
                Assert.assertEquals(2, wire.getRowCount());
                Assert.assertEquals(QwpConstants.TYPE_IPV4, wire.getColumnDef(0).getTypeCode());
                Assert.assertEquals(QwpConstants.TYPE_VARCHAR, wire.getColumnDef(1).getTypeCode());
                Assert.assertEquals(QwpConstants.TYPE_VARCHAR, wire.getColumnDef(2).getTypeCode());
                Assert.assertTrue(wire.hasNextRow());
                wire.nextRow();
                Assert.assertEquals(0xc0a80101, wire.getFixedWidthColumn(0).getLong());
                assertUtf8("10.20.30.40", wire.getStringColumn(1).getUtf8Value());
                assertUtf8("1.2.3.4", wire.getStringColumn(2).getUtf8Value());
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
            assertQuery("select ip, s, v, ip is null ipn, s is null sn, v is null vn "
                    + "from schema_ipv4_sf order by ts")
                    .noLeakCheck().expectSize().returns("ip\ts\tv\tipn\tsn\tvn\n"
                            + "192.168.1.1\t10.20.30.40\t1.2.3.4\tfalse\tfalse\tfalse\n"
                            + "\t\t\ttrue\ttrue\ttrue\n");
        });
    }

    private static void append(QwpWebSocketSender sender, Vector vector) {
        if (vector.typed) {
            sender.ipv4Column("value", Integer.parseInt(vector.input));
        } else {
            sender.ipv4Column("value", vector.input);
        }
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
        InputStream stream = QwpSchemaIPv4E2ETest.class.getResourceAsStream(CORPUS);
        Assert.assertNotNull(CORPUS, stream);
        List<Vector> vectors = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty() || line.charAt(0) == '#') {
                    continue;
                }
                String[] fields = line.split("\\t", -1);
                Assert.assertEquals(line, 6, fields.length);
                boolean invalid = "INVALID".equals(fields[3]);
                String expected = "<NULL>".equals(fields[5]) ? null : fields[5];
                Assert.assertEquals(line, invalid, "-".equals(fields[5]));
                vectors.add(new Vector("INT".equals(fields[1]), fields[2], invalid, expected));
            }
        }
        Assert.assertEquals("shared corpus row count", 16, vectors.size());
        return vectors;
    }

    private enum Target {
        IPV4("IPv4"),
        STRING("STRING"),
        VARCHAR("VARCHAR");

        private final String sqlType;
        private final String tableName;

        Target(String sqlType) {
            this.sqlType = sqlType;
            this.tableName = "schema_ipv4_" + sqlType.toLowerCase(Locale.ROOT);
        }
    }

    private static final class Vector {
        private final String expected;
        private final String input;
        private final boolean invalid;
        private final boolean typed;

        private Vector(boolean typed, String input, boolean invalid, String expected) {
            this.typed = typed;
            this.input = input;
            this.invalid = invalid;
            this.expected = expected;
        }
    }
}
