/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test.cutlass.qwp.e2e;

import io.questdb.client.LineSenderSchemaException;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketClientFactory;
import io.questdb.client.cutlass.http.client.WebSocketFrameHandler;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaProtocol;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaResponse;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaBinding;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.cutlass.qwp.protocol.QwpColumnDef;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpFixedWidthColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpMessageCursor;
import io.questdb.cutlass.qwp.protocol.QwpTableBlockCursor;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class QwpSchemaLongNumericE2ETest extends AbstractQwpWebSocketTest {
    private static final String VECTORS = "/io/questdb/client/cutlass/qwp/long-to-numeric.tsv";

    @Test
    public void testLongConversionCorpusMatchesLegacyServerPath() throws Exception {
        List<Vector> vectors = readVectors();
        runInContext(port -> {
            for (Target target : Target.values()) {
                List<Vector> targetVectors = forTarget(vectors, target);
                String schemaTable = "schema_long_" + target.sqlName;
                String legacyTable = "legacy_long_" + target.sqlName;
                createNumericTable(schemaTable, target);
                createNumericTable(legacyTable, target);

                try (WebSocketClient client = connect(port);
                     QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                     QwpTableBuffer table = new QwpTableBuffer(schemaTable)) {
                    QwpSchemaBinding binding = new QwpSchemaBinding(
                            table, describe(client, 10 + target.ordinal(), schemaTable));
                    int accepted = 0;
                    for (Vector vector : targetVectors) {
                        if (vector.invalid()) {
                            LineSenderSchemaException ex = Assert.assertThrows(vector.caseId,
                                    LineSenderSchemaException.class,
                                    () -> binding.longColumn("value", vector.input));
                            Assert.assertEquals(vector.caseId, LineSenderSchemaException.Reason.INVALID_VALUE, ex.getReason());
                            Assert.assertFalse(ex.isRetryable());
                            table.cancelCurrentRow();
                            table.rollbackUncommittedColumns();
                        } else {
                            binding.longColumn("case_id", accepted++).longColumn("value", vector.input);
                            table.nextRow();
                        }
                    }
                    int length = encoder.encodeSchema(table);
                    assertTypedFrame(encoder, length, target, targetVectors);
                    client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                    assertOk(client, 0);
                }

                sendLegacyAccepted(port, legacyTable, targetVectors);
                sendLegacyRejected(port, legacyTable, targetVectors);
                drainWalQueue();
                String expected = expectedValues(targetVectors, target);
                assertQuery("select case_id, value from " + schemaTable + " order by case_id")
                        .noLeakCheck().returnsOnce(expected);
                assertQuery("select case_id, value from " + legacyTable + " order by case_id")
                        .noLeakCheck().returnsOnce(expected);
            }
        });
    }

    @Test
    public void testLongMinSchemaNullIsStableWhileLegacyDependsOnBlockBitmap() throws Exception {
        runInContext(port -> {
            for (Target target : Target.values()) {
                assertSchemaLongMin(port, target, false);
                assertSchemaLongMin(port, target, true);
                assertLegacyLongMin(port, target, false);
                assertLegacyLongMin(port, target, true);
            }
        });
    }

    @Test
    public void testOverflowRollsBackWholeRowAndDuplicateFirstValueWins() throws Exception {
        runInContext(port -> {
            execute("create table schema_long_rows (value byte, only_b long, ts timestamp) timestamp(ts) partition by day wal");
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = new QwpTableBuffer("schema_long_rows")) {
                QwpSchemaBinding binding = new QwpSchemaBinding(table, describe(client, 50, "schema_long_rows"));
                binding.longColumn("value", 10);
                table.nextRow();
                binding.longColumn("only_b", 99);
                LineSenderSchemaException overflow = Assert.assertThrows(
                        LineSenderSchemaException.class, () -> binding.longColumn("value", 128));
                Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, overflow.getReason());
                table.cancelCurrentRow();
                table.rollbackUncommittedColumns();

                binding.longColumn("value", 20)
                        .longColumn("value", 128)
                        .stringColumn("value", "not converted after duplicate")
                        .uuidColumn("value", 1, 2);
                table.nextRow();
                table.nextRow();

                int length = encoder.encodeSchema(table);
                QwpTableBlockCursor wireTable = parseSingleTable(encoder, length);
                Assert.assertTrue(wireTable.hasKnownSchemaIdentity());
                Assert.assertEquals(3, wireTable.getRowCount());
                Assert.assertEquals("value", wireTable.getColumnDef(0).getName());
                Assert.assertEquals(QwpConstants.TYPE_BYTE, wireTable.getColumnDef(0).getTypeCode());
                Assert.assertEquals("failed-row-only column must not remain in the encoded layout", 1, wireTable.getColumnCount());
                client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                assertOk(client, 0);
            }
            drainWalQueue();
            assertQuery("select value, only_b from schema_long_rows")
                    .noLeakCheck().returnsOnce("value\tonly_b\n10\tnull\n20\tnull\n0\tnull\n");
        });
    }

    @Test
    public void testExplicitStringNullAndOmissionAcrossNumericTargets() throws Exception {
        runInContext(port -> {
            execute("create table schema_long_nulls (b byte, s short, i int, l long, f float, d double, ts timestamp) "
                    + "timestamp(ts) partition by day wal");
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = new QwpTableBuffer("schema_long_nulls")) {
                QwpSchemaBinding binding = new QwpSchemaBinding(
                        table, describe(client, 60, "schema_long_nulls"));
                binding.longColumn("b", 1).longColumn("s", 1).longColumn("i", 1)
                        .longColumn("l", 1).longColumn("f", 1).longColumn("d", 1);
                table.nextRow();
                binding.stringColumn("b", null).stringColumn("s", null).stringColumn("i", null)
                        .stringColumn("l", null).stringColumn("f", null).stringColumn("d", null);
                table.nextRow();
                table.nextRow();
                int length = encoder.encodeSchema(table);
                client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                assertOk(client, 0);
            }
            drainWalQueue();
            assertQuery("select b, s, i, l, f, d from schema_long_nulls")
                    .noLeakCheck().returnsOnce("b\ts\ti\tl\tf\td\n"
                            + "1\t1\t1\t1\t1.0\t1.0\n"
                            + "0\t0\tnull\tnull\tnull\tnull\n"
                            + "0\t0\tnull\tnull\tnull\tnull\n");
        });
    }

    private void createNumericTable(String tableName, Target target) throws Exception {
        execute("create table " + tableName + " (case_id long, value " + target.sqlName
                + ", ts timestamp) timestamp(ts) partition by day wal");
    }

    private void assertLegacyLongMin(int port, Target target, boolean withOmission) throws Exception {
        String tableName = "legacy_min_" + target.sqlName + (withOmission ? "_bitmap" : "_plain");
        createNumericTable(tableName, target);
        try (WebSocketClient client = connect(port);
             QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
             QwpTableBuffer table = new QwpTableBuffer(tableName)) {
            table.getOrCreateColumn("value", QwpConstants.TYPE_LONG, true).addLong(Long.MIN_VALUE);
            table.nextRow();
            if (withOmission) {
                table.nextRow();
            }
            int length = encoder.encode(table);
            QwpTableBlockCursor wireTable = parseSingleTable(encoder, length);
            Assert.assertEquals(QwpConstants.TYPE_LONG, wireTable.getColumnDef(0).getTypeCode());
            QwpFixedWidthColumnCursor wireValue = wireTable.getFixedWidthColumn(0);
            Assert.assertEquals(withOmission, wireValue.getNullBitmapAddress() != 0);
            Assert.assertTrue(wireTable.hasNextRow());
            wireTable.nextRow();
            Assert.assertEquals(Long.MIN_VALUE, wireValue.getLong());
            if (withOmission) {
                Assert.assertFalse("present LONG_MIN must remain bitmap-non-null on the legacy wire",
                        wireTable.isColumnNull(0));
                Assert.assertTrue(wireTable.hasNextRow());
                wireTable.nextRow();
                Assert.assertTrue(wireTable.isColumnNull(0));
            }
            client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
            WebSocketResponse response = receiveResponse(client);
            if (withOmission && target.isNarrowInteger()) {
                Assert.assertFalse(target.name(), response.isSuccess());
                Assert.assertTrue(response.getErrorMessage(), response.getErrorMessage().contains("out of range"));
            } else {
                Assert.assertTrue(response.getErrorMessage(), response.isSuccess());
                Assert.assertEquals(0, response.getSequence());
            }
        }
        drainWalQueue();
        final String expected;
        if (withOmission && target.isNarrowInteger()) {
            expected = "value\n";
        } else if (withOmission && target == Target.FLOAT) {
            expected = "value\n-9.223372E18\nnull\n";
        } else if (withOmission && target == Target.DOUBLE) {
            expected = "value\n-9.223372036854776E18\nnull\n";
        } else {
            expected = missingValues(target, withOmission ? 2 : 1);
        }
        assertQuery("select value from " + tableName).noLeakCheck().returnsOnce(expected);
    }

    private void assertSchemaLongMin(int port, Target target, boolean withOmission) throws Exception {
        String tableName = "schema_min_" + target.sqlName + (withOmission ? "_bitmap" : "_plain");
        createNumericTable(tableName, target);
        try (WebSocketClient client = connect(port);
             QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
             QwpTableBuffer tableBuffer = new QwpTableBuffer(tableName)) {
            QwpSchemaBinding binding = new QwpSchemaBinding(
                    tableBuffer, describe(client, 100 + target.ordinal() * 2L + (withOmission ? 1 : 0), tableName));
            binding.longColumn("value", Long.MIN_VALUE);
            tableBuffer.nextRow();
            if (withOmission) {
                tableBuffer.nextRow();
            }
            int length = encoder.encodeSchema(tableBuffer);
            QwpTableBlockCursor table = parseSingleTable(encoder, length);
            Assert.assertTrue(table.hasKnownSchemaIdentity());
            Assert.assertEquals(target.wireType, table.getColumnDef(0).getTypeCode());
            Assert.assertTrue("schema conversion must encode target missing in the null bitmap",
                    table.getFixedWidthColumn(0).getNullBitmapAddress() != 0);
            int expectedRows = withOmission ? 2 : 1;
            Assert.assertEquals(expectedRows, table.getRowCount());
            for (int row = 0; row < expectedRows; row++) {
                Assert.assertTrue(table.hasNextRow());
                table.nextRow();
                Assert.assertTrue("schema mode must encode target missing independently of block layout",
                        table.isColumnNull(0));
            }
            Assert.assertFalse(table.hasNextRow());
            client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
            assertOk(client, 0);
        }
        drainWalQueue();
        assertQuery("select value from " + tableName).noLeakCheck()
                .returnsOnce(missingValues(target, withOmission ? 2 : 1));
    }

    private static String missingValues(Target target, int count) {
        StringBuilder sink = new StringBuilder("value\n");
        String value = target == Target.BYTE || target == Target.SHORT ? "0" : "null";
        for (int i = 0; i < count; i++) {
            sink.append(value).append('\n');
        }
        return sink.toString();
    }

    private static void assertTypedFrame(
            QwpWebSocketEncoder encoder,
            int length,
            Target target,
            List<Vector> vectors
    ) throws Exception {
        Assert.assertEquals(QwpConstants.FLAG_SCHEMA,
                Unsafe.getByte(encoder.getBuffer().getBufferPtr() + QwpConstants.HEADER_OFFSET_FLAGS)
                        & QwpConstants.FLAG_SCHEMA);
        QwpTableBlockCursor table = parseSingleTable(encoder, length);
        Assert.assertTrue(table.hasKnownSchemaIdentity());
        Assert.assertTrue(table.getSchemaTableId() >= 0);
        Assert.assertTrue(table.getSchemaMetadataVersion() >= 0);
        Assert.assertEquals(2, table.getColumnCount());
        QwpColumnDef caseId = table.getColumnDef(0);
        QwpColumnDef value = table.getColumnDef(1);
        Assert.assertEquals("case_id", caseId.getName());
        Assert.assertEquals(QwpConstants.TYPE_LONG, caseId.getTypeCode());
        Assert.assertEquals("value", value.getName());
        Assert.assertEquals(target.wireType, value.getTypeCode());

        int row = 0;
        for (Vector vector : vectors) {
            if (vector.invalid()) {
                continue;
            }
            Assert.assertTrue(vector.caseId, table.hasNextRow());
            table.nextRow();
            QwpFixedWidthColumnCursor cursor = table.getFixedWidthColumn(1);
            assertWireValue(vector, target, cursor);
            row++;
        }
        Assert.assertEquals(row, table.getRowCount());
        Assert.assertFalse(table.hasNextRow());
    }

    private static void assertWireValue(Vector vector, Target target, QwpFixedWidthColumnCursor cursor) {
        if (vector.nullValue()) {
            if (vector.input == Long.MIN_VALUE) {
                Assert.assertTrue(vector.caseId, cursor.isNull());
            } else {
                // INT_MIN is a non-null LONG source converted to the INT sentinel.
                // Its wire bitmap remains clear; QuestDB materializes it as SQL NULL.
                Assert.assertFalse(vector.caseId, cursor.isNull());
                Assert.assertEquals(vector.caseId, Integer.MIN_VALUE, cursor.getLong());
            }
            return;
        }
        Assert.assertFalse(vector.caseId, cursor.isNull());
        switch (target) {
            case BYTE, SHORT, INT, LONG -> Assert.assertEquals(vector.caseId,
                    Long.parseLong(vector.expected), cursor.getLong());
            case FLOAT -> Assert.assertEquals(vector.caseId,
                    (int) Long.parseUnsignedLong(vector.expected.substring(2), 16),
                    Float.floatToRawIntBits((float) cursor.getDouble()));
            case DOUBLE -> Assert.assertEquals(vector.caseId,
                    Long.parseUnsignedLong(vector.expected.substring(2), 16),
                    Double.doubleToRawLongBits(cursor.getDouble()));
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
                Unsafe.getUnsafe().putByte(address + i, request[i]);
            }
            client.sendBinary(address, request.length);
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
            Assert.assertEquals(QwpSchemaProtocol.RESULT_KNOWN, response.get().getResult());
            return response.get();
        } finally {
            Unsafe.free(address, request.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static String expectedValues(List<Vector> vectors, Target target) {
        StringBuilder sink = new StringBuilder("case_id\tvalue\n");
        int caseId = 0;
        for (Vector vector : vectors) {
            if (vector.invalid()) {
                continue;
            }
            sink.append(caseId++).append('\t');
            if (vector.nullValue()) {
                if (target == Target.BYTE || target == Target.SHORT) {
                    sink.append('0');
                } else {
                    sink.append("null");
                }
            } else if (target == Target.FLOAT) {
                sink.append(Float.intBitsToFloat((int) Long.parseUnsignedLong(vector.expected.substring(2), 16)));
            } else if (target == Target.DOUBLE) {
                sink.append(Double.longBitsToDouble(Long.parseUnsignedLong(vector.expected.substring(2), 16)));
            } else {
                sink.append(vector.expected);
            }
            sink.append('\n');
        }
        return sink.toString();
    }

    private static List<Vector> forTarget(List<Vector> vectors, Target target) {
        List<Vector> selected = new ArrayList<>();
        for (Vector vector : vectors) {
            if (vector.target == target) {
                selected.add(vector);
            }
        }
        Assert.assertFalse(target.name(), selected.isEmpty());
        return selected;
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
        try (InputStream stream = QwpSchemaLongNumericE2ETest.class.getResourceAsStream(VECTORS)) {
            Assert.assertNotNull(stream);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (!line.startsWith("#")) {
                        String[] fields = line.split("\\t", -1);
                        Assert.assertEquals(line, 4, fields.length);
                        vectors.add(new Vector(fields[0], Long.parseLong(fields[1]),
                                Target.valueOf(fields[2]), fields[3]));
                    }
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
                Assert.assertTrue(response.readFrom(payloadPtr, payloadLen, client.isQwpSchemaEnabled()));
            }

            @Override
            public void onClose(int code, String reason) {
                Assert.fail("unexpected close [code=" + code + ", reason=" + reason + ']');
            }
        }, 5_000));
        return response;
    }

    private static void assertOk(WebSocketClient client, long sequence) {
        WebSocketResponse response = receiveResponse(client);
        Assert.assertTrue(response.getErrorMessage(), response.isSuccess());
        Assert.assertEquals(sequence, response.getSequence());
    }

    private static void sendLegacyAccepted(int port, String tableName, List<Vector> vectors) {
        try (WebSocketClient client = connect(port);
             QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
             QwpTableBuffer table = new QwpTableBuffer(tableName)) {
            int accepted = 0;
            for (Vector vector : vectors) {
                if (!vector.invalid()) {
                    table.getOrCreateColumn("case_id", QwpConstants.TYPE_LONG, true).addLong(accepted++);
                    table.getOrCreateColumn("value", QwpConstants.TYPE_LONG, true).addLong(vector.input);
                    table.nextRow();
                }
            }
            int length = encoder.encode(table);
            client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
            assertOk(client, 0);
        }
    }

    private static void sendLegacyRejected(int port, String tableName, List<Vector> vectors) {
        int sequence = 0;
        for (Vector vector : vectors) {
            if (!vector.invalid()) {
                continue;
            }
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = new QwpTableBuffer(tableName)) {
                table.getOrCreateColumn("case_id", QwpConstants.TYPE_LONG, true).addLong(10_000 + sequence++);
                table.getOrCreateColumn("value", QwpConstants.TYPE_LONG, true).addLong(vector.input);
                table.nextRow();
                int length = encoder.encode(table);
                client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                WebSocketResponse response = receiveResponse(client);
                Assert.assertFalse(vector.caseId, response.isSuccess());
                Assert.assertTrue(vector.caseId + ": " + response.getErrorMessage(),
                        response.getErrorMessage().contains("out of range"));
            }
        }
    }

    private enum Target {
        BYTE("byte", QwpConstants.TYPE_BYTE),
        SHORT("short", QwpConstants.TYPE_SHORT),
        INT("int", QwpConstants.TYPE_INT),
        LONG("long", QwpConstants.TYPE_LONG),
        FLOAT("float", QwpConstants.TYPE_FLOAT),
        DOUBLE("double", QwpConstants.TYPE_DOUBLE);

        private final String sqlName;
        private final byte wireType;

        Target(String sqlName, byte wireType) {
            this.sqlName = sqlName;
            this.wireType = wireType;
        }

        private boolean isNarrowInteger() {
            return this == BYTE || this == SHORT || this == INT;
        }
    }

    private record Vector(String caseId, long input, Target target, String expected) {
        boolean invalid() {
            return "<INVALID>".equals(expected);
        }

        boolean nullValue() {
            return "<NULL>".equals(expected);
        }
    }
}
