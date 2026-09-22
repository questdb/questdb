/*+*****************************************************************************
 * Copyright (c) 2014-2019 Appsicle
 * Copyright (c) 2019-2026 QuestDB
 * Licensed under the Apache License, Version 2.0
 ******************************************************************************/
package io.questdb.test.cutlass.qwp.e2e;

import io.questdb.client.LineSenderSchemaException;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketClientFactory;
import io.questdb.client.cutlass.http.client.WebSocketFrameHandler;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaBinding;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaProtocol;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaResponse;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpMessageCursor;
import io.questdb.cutlass.qwp.protocol.QwpTableBlockCursor;
import io.questdb.cutlass.qwp.protocol.QwpTimestampColumnCursor;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class QwpSchemaTimestampE2ETest extends AbstractQwpWebSocketTest {
    private static final String INPUT_VECTORS = "/io/questdb/client/cutlass/qwp/timestamp-inputs.tsv";
    private static final String VECTORS = "/io/questdb/client/cutlass/qwp/timestamp-units.tsv";

    @Test
    public void testBroaderTimestampInputsUseTargetWireValues() throws Exception {
        List<InputVector> vectors = readInputVectors();
        runInContext(port -> {
            for (Target target : Target.values()) {
                List<InputVector> targetVectors = forInputTarget(vectors, target);
                String tableName = "schema_ts_inputs_" + target.suffix;
                createTable(tableName, target);
                try (WebSocketClient client = connect(port);
                     QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                     QwpTableBuffer table = new QwpTableBuffer(tableName)) {
                    QwpSchemaBinding binding = new QwpSchemaBinding(
                            table,
                            describe(client, 400 + target.ordinal(), tableName)
                    );
                    int row = 0;
                    for (InputVector vector : targetVectors) {
                        if (vector.invalid()) {
                            LineSenderSchemaException error = Assert.assertThrows(
                                    vector.caseId,
                                    LineSenderSchemaException.class,
                                    () -> vector.append(binding)
                            );
                            Assert.assertEquals(vector.caseId,
                                    LineSenderSchemaException.Reason.INVALID_VALUE,
                                    error.getReason());
                            table.cancelCurrentRow();
                            table.rollbackUncommittedColumns();
                        } else {
                            binding.longColumn("case_id", row++);
                            vector.append(binding);
                            table.nextRow();
                        }
                    }
                    int length = encoder.encodeSchema(table);
                    QwpTableBlockCursor wire = parseSingleTable(encoder, length);
                    Assert.assertTrue(wire.hasKnownSchemaIdentity());
                    Assert.assertEquals(target.wireType, wire.getColumnDef(1).getTypeCode());
                    QwpTimestampColumnCursor values = wire.getTimestampColumn(1);
                    int rows = 0;
                    for (InputVector vector : targetVectors) {
                        if (!vector.invalid()) {
                            Assert.assertTrue(vector.caseId, wire.hasNextRow());
                            wire.nextRow();
                            Assert.assertFalse(vector.caseId, wire.isColumnNull(1));
                            Assert.assertEquals(vector.caseId, vector.expectedWire, values.getTimestamp());
                            rows++;
                        }
                    }
                    Assert.assertEquals(rows, wire.getRowCount());
                    Assert.assertFalse(wire.hasNextRow());
                    client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                    assertOk(client);
                }
                drainWalQueue();
                assertQuery("select case_id, cast(value as long) value from " + tableName + " order by case_id")
                        .noLeakCheck().expectSize().returns(expectedInputRows(targetVectors));
            }
        });
    }

    @Test
    public void testLegacyNormalizedTimestampWireValuesRemainAccepted() throws Exception {
        execute("create table legacy_ts_inputs (wrapped timestamp_ns, instant_value timestamp_ns, ts timestamp) "
                + "timestamp(ts) partition by day wal");
        runInContext(port -> {
            try (WebSocketClient client = connectLegacy(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = new QwpTableBuffer("legacy_ts_inputs")) {
                // Raw-server characterization of the unchecked microsecond values already normalized
                // by the legacy Sender timestamp overloads.
                table.getOrCreateColumn("wrapped", QwpConstants.TYPE_TIMESTAMP, true).addLong(-1_000_000);
                table.getOrCreateColumn("instant_value", QwpConstants.TYPE_TIMESTAMP, true).addLong(0);
                table.nextRow();
                sendLegacy(client, encoder, table);
            }
            drainWalQueue();
            // Legacy normalizes both inputs to unchecked micros before server conversion.
            assertQuery("select cast(wrapped as long) wrapped, cast(instant_value as long) instant_value "
                    + "from legacy_ts_inputs")
                    .noLeakCheck().expectSize().returns("wrapped\tinstant_value\n-1000000000\t0\n");
        });
    }

    @Test
    public void testTimestampCorpusMatchesLegacyWireConversion() throws Exception {
        List<Vector> vectors = readVectors();
        runInContext(port -> {
            for (Target target : Target.values()) {
                List<Vector> targetVectors = forTarget(vectors, target);
                String schemaTable = "schema_ts_" + target.suffix;
                String legacyTable = "legacy_ts_" + target.suffix;
                createTable(schemaTable, target);
                createTable(legacyTable, target);

                try (WebSocketClient client = connect(port);
                     QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                     QwpTableBuffer table = new QwpTableBuffer(schemaTable)) {
                    QwpSchemaBinding binding = new QwpSchemaBinding(table, describe(client, 10 + target.ordinal(), schemaTable));
                    int caseId = 0;
                    for (Vector vector : targetVectors) {
                        if (vector.invalid()) {
                            LineSenderSchemaException error = Assert.assertThrows(vector.caseId,
                                    LineSenderSchemaException.class,
                                    () -> binding.timestampColumn("value", vector.input, vector.unit));
                            Assert.assertEquals(vector.caseId, LineSenderSchemaException.Reason.INVALID_VALUE, error.getReason());
                            table.cancelCurrentRow();
                            table.rollbackUncommittedColumns();
                        } else {
                            binding.longColumn("case_id", caseId++);
                            binding.timestampColumn("value", vector.input, vector.unit);
                            table.nextRow();
                        }
                    }
                    int length = encoder.encodeSchema(table);
                    assertTypedWire(encoder, length, target, targetVectors);
                    client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                    assertOk(client);
                }

                for (ChronoUnit unit : new ChronoUnit[]{ChronoUnit.MICROS, ChronoUnit.NANOS}) {
                    sendLegacyAccepted(port, legacyTable, target, targetVectors, unit);
                }
                sendLegacyRejected(port, legacyTable, targetVectors);
                drainWalQueue();
                String expected = expectedRows(targetVectors);
                assertQuery("select case_id, cast(value as long) value from " + schemaTable + " order by case_id")
                        .noLeakCheck().expectSize().returns(expected);
                assertQuery("select case_id, cast(value as long) value from " + legacyTable + " order by case_id")
                        .noLeakCheck().expectSize().returns(expected);
            }
        });
    }

    @Test
    public void testExplicitNullOmissionRollbackAndDuplicateFirstWins() throws Exception {
        execute("create table schema_ts_rows (u timestamp, n timestamp_ns, only_b long, ts timestamp) "
                + "timestamp(ts) partition by day wal");
        runInContext(port -> {
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = new QwpTableBuffer("schema_ts_rows")) {
                QwpSchemaBinding binding = new QwpSchemaBinding(table, describe(client, 50, "schema_ts_rows"));
                binding.timestampColumn("u", 1, ChronoUnit.MICROS)
                        .timestampColumn("n", 1, ChronoUnit.NANOS);
                table.nextRow();

                binding.stringColumn("u", null).stringColumn("n", null);
                table.nextRow();
                table.nextRow();

                binding.longColumn("only_b", 99);
                Assert.assertThrows(LineSenderSchemaException.class,
                        () -> binding.timestampColumn("n", 9_223_372_036_854_776L, ChronoUnit.MICROS));
                table.cancelCurrentRow();
                table.rollbackUncommittedColumns();

                binding.longColumn("only_b", 100);
                LineSenderSchemaException nullInstant = Assert.assertThrows(LineSenderSchemaException.class,
                        () -> binding.timestampColumn("n", (Instant) null));
                Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, nullInstant.getReason());
                table.cancelCurrentRow();
                table.rollbackUncommittedColumns();

                binding.timestampColumn("n", 2, ChronoUnit.MICROS)
                        .timestampColumn("n", Instant.MAX)
                        .timestampColumn("n", 9_223_372_036_854_776L, ChronoUnit.MICROS)
                        .stringColumn("n", "not parsed after duplicate")
                        .longColumn("n", 7);
                table.nextRow();

                int length = encoder.encodeSchema(table);
                QwpTableBlockCursor wire = parseSingleTable(encoder, length);
                Assert.assertEquals(4, wire.getRowCount());
                Assert.assertEquals("failed-row-only column must be rolled back", 2, wire.getColumnCount());
                Assert.assertEquals(QwpConstants.TYPE_TIMESTAMP, wire.getColumnDef(0).getTypeCode());
                Assert.assertEquals(QwpConstants.TYPE_TIMESTAMP_NANOS, wire.getColumnDef(1).getTypeCode());
                client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                assertOk(client);
            }
            drainWalQueue();
            assertQuery("select cast(u as long) u, cast(n as long) n, only_b from schema_ts_rows")
                    .noLeakCheck().expectSize().returns("u\tn\tonly_b\n"
                            + "1\t1\tnull\n"
                            + "null\tnull\tnull\n"
                            + "null\tnull\tnull\n"
                            + "null\t2000\tnull\n");
        });
    }

    @Test
    public void testLegacySuppliedLongMinIsIndependentOfTimestampNullBitmap() throws Exception {
        runInContext(port -> {
            for (Target target : Target.values()) {
                for (ChronoUnit unit : new ChronoUnit[]{ChronoUnit.MICROS, ChronoUnit.NANOS}) {
                    for (boolean withOmission : new boolean[]{false, true}) {
                        String name = "legacy_ts_min_" + target.suffix + '_'
                                + unit.name().toLowerCase() + (withOmission ? "_bitmap" : "_plain");
                        createTable(name, target);
                        try (WebSocketClient client = connect(port);
                             QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                             QwpTableBuffer table = new QwpTableBuffer(name)) {
                            byte sourceType = unit == ChronoUnit.NANOS
                                    ? QwpConstants.TYPE_TIMESTAMP_NANOS : QwpConstants.TYPE_TIMESTAMP;
                            table.getOrCreateColumn("case_id", QwpConstants.TYPE_LONG, true).addLong(0);
                            table.getOrCreateColumn("value", sourceType, true).addLong(Long.MIN_VALUE);
                            table.nextRow();
                            if (withOmission) {
                                table.nextRow();
                            }
                            int length = encoder.encode(table);
                            QwpTableBlockCursor wire = parseSingleTable(encoder, length);
                            Assert.assertEquals(sourceType, wire.getColumnDef(1).getTypeCode());
                            Assert.assertEquals(withOmission,
                                    wire.getTimestampColumn(1).getNullBitmapAddress() != 0);
                            Assert.assertTrue(wire.hasNextRow());
                            wire.nextRow();
                            Assert.assertFalse("supplied MIN must remain bitmap-present", wire.isColumnNull(1));
                            Assert.assertEquals(Long.MIN_VALUE, wire.getTimestampColumn(1).getTimestamp());
                            client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                            WebSocketResponse response = receive(client);
                            boolean overflow = unit == ChronoUnit.MICROS && target == Target.NANOS;
                            Assert.assertEquals(name, !overflow, response.isSuccess());
                            if (overflow) {
                                Assert.assertTrue(response.getErrorMessage(), response.getErrorMessage().contains("overflow"));
                            }
                        }
                        drainWalQueue();
                        String expected;
                        if (unit == ChronoUnit.MICROS && target == Target.NANOS) {
                            expected = "value\n";
                        } else if (unit == ChronoUnit.NANOS && target == Target.MICROS) {
                            expected = "value\n-9223372036854775\n" + (withOmission ? "null\n" : "");
                        } else {
                            expected = "value\nnull\n" + (withOmission ? "null\n" : "");
                        }
                        assertQuery("select cast(value as long) value from " + name)
                                .noLeakCheck().expectSize().returns(expected);
                    }
                }
            }
        });
    }

    @Test
    public void testSchemaSuppliedLongMinFollowsPairRulesWithAndWithoutOmission() throws Exception {
        runInContext(port -> {
            for (Target target : Target.values()) {
                for (ChronoUnit unit : new ChronoUnit[]{ChronoUnit.MICROS, ChronoUnit.NANOS}) {
                    for (boolean withOmission : new boolean[]{false, true}) {
                        String name = "schema_ts_min_" + target.suffix + '_'
                                + unit.name().toLowerCase() + (withOmission ? "_bitmap" : "_plain");
                        createTable(name, target);
                        try (WebSocketClient client = connect(port);
                             QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                             QwpTableBuffer table = new QwpTableBuffer(name)) {
                            QwpSchemaBinding binding = new QwpSchemaBinding(table, describe(client, 100, name));
                            if (unit == ChronoUnit.MICROS && target == Target.NANOS) {
                                Assert.assertThrows(LineSenderSchemaException.class,
                                        () -> binding.timestampColumn("value", Long.MIN_VALUE, unit));
                                table.cancelCurrentRow();
                                table.rollbackUncommittedColumns();
                                continue;
                            }
                            binding.longColumn("case_id", 0).timestampColumn("value", Long.MIN_VALUE, unit);
                            table.nextRow();
                            if (withOmission) {
                                table.nextRow();
                            }
                            int length = encoder.encodeSchema(table);
                            QwpTableBlockCursor wire = parseSingleTable(encoder, length);
                            Assert.assertEquals(target.wireType, wire.getColumnDef(1).getTypeCode());
                            Assert.assertEquals(withOmission,
                                    wire.getTimestampColumn(1).getNullBitmapAddress() != 0);
                            Assert.assertTrue(wire.hasNextRow());
                            wire.nextRow();
                            Assert.assertFalse(wire.isColumnNull(1));
                            long expected = unit == ChronoUnit.NANOS && target == Target.MICROS
                                    ? -9_223_372_036_854_775L : Long.MIN_VALUE;
                            Assert.assertEquals(expected, wire.getTimestampColumn(1).getTimestamp());
                            client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                            assertOk(client);
                        }
                        drainWalQueue();
                        String expectedSql;
                        if (unit == ChronoUnit.NANOS && target == Target.MICROS) {
                            expectedSql = "value\n-9223372036854775\n" + (withOmission ? "null\n" : "");
                        } else {
                            expectedSql = "value\nnull\n" + (withOmission ? "null\n" : "");
                        }
                        assertQuery("select cast(value as long) value from " + name)
                                .noLeakCheck().expectSize().returns(expectedSql);
                    }
                }
            }
        });
    }

    @Test
    public void testSchemaTimestampUsesGorillaEncodingOnSmoothValues() throws Exception {
        execute("create table schema_ts_gorilla (value timestamp, ts timestamp) timestamp(ts) partition by day wal");
        runInContext(port -> {
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = new QwpTableBuffer("schema_ts_gorilla")) {
                QwpSchemaBinding binding = new QwpSchemaBinding(table, describe(client, 200, "schema_ts_gorilla"));
                for (int i = 0; i < 16; i++) {
                    binding.timestampColumn("value", 1_000_000_123L + i * 1_000_000L, ChronoUnit.NANOS);
                    table.nextRow();
                }
                int length = encoder.encodeSchema(table);
                Assert.assertEquals(QwpConstants.FLAG_GORILLA,
                        Unsafe.getByte(encoder.getBuffer().getBufferPtr() + QwpConstants.HEADER_OFFSET_FLAGS)
                                & QwpConstants.FLAG_GORILLA);
                QwpTableBlockCursor wire = parseSingleTable(encoder, length);
                Assert.assertFalse("smooth timestamp values must exercise Gorilla decoding",
                        wire.getTimestampColumn(0).supportsDirectAccess());
                for (int i = 0; i < 16; i++) {
                    Assert.assertTrue(wire.hasNextRow());
                    wire.nextRow();
                    Assert.assertEquals(1_000_000L + i * 1_000L,
                            wire.getTimestampColumn(0).getTimestamp());
                }
                Assert.assertFalse(wire.hasNextRow());
                client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                assertOk(client);
            }
            drainWalQueue();
            StringBuilder expected = new StringBuilder("value\n");
            for (int i = 0; i < 16; i++) {
                expected.append(1_000_000L + i * 1_000L).append('\n');
            }
            assertQuery("select cast(value as long) value from schema_ts_gorilla")
                    .noLeakCheck().expectSize().returns(expected.toString());
        });
    }

    @Test
    public void testSchemaTimestampFrameReplaysFromStoreAndForward() throws Exception {
        execute("create table schema_ts_replay (value timestamp_ns, instant_value timestamp_ns, ts timestamp) "
                + "timestamp(ts) partition by day wal");
        runInContext(port -> {
            java.io.File sfRoot = temp.newFolder("qwp-schema-ts-sf");
            String slot = new java.io.File(sfRoot, "default").getAbsolutePath();
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = new QwpTableBuffer("schema_ts_replay")) {
                QwpSchemaBinding binding = new QwpSchemaBinding(table, describe(client, 300, "schema_ts_replay"));
                binding.timestampColumn("value", 1_001, ChronoUnit.MICROS);
                binding.timestampColumn("instant_value", Instant.ofEpochSecond(0, 123));
                table.nextRow();
                int length = encoder.encodeSchema(table);
                QwpTableBlockCursor wire = parseSingleTable(encoder, length);
                Assert.assertEquals(QwpConstants.TYPE_TIMESTAMP_NANOS, wire.getColumnDef(0).getTypeCode());
                Assert.assertEquals(QwpConstants.TYPE_TIMESTAMP_NANOS, wire.getColumnDef(1).getTypeCode());
                Assert.assertTrue(wire.hasNextRow());
                wire.nextRow();
                Assert.assertEquals(1_001_000, wire.getTimestampColumn(0).getTimestamp());
                Assert.assertEquals(123, wire.getTimestampColumn(1).getTimestamp());
                try (CursorSendEngine cursorEngine = new CursorSendEngine(slot, 1 << 20)) {
                    Assert.assertEquals(0, cursorEngine.appendBlocking(encoder.getBuffer().getBufferPtr(), length));
                }
            }
            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port
                    + ";sf_dir=" + sfRoot.getAbsolutePath()
                    + ";sender_id=default;sf_max_segment_bytes=1m;")) {
                Assert.assertTrue(sender.drain(10_000));
            }
            drainWalQueue();
            assertQuery("select cast(value as long) value, cast(instant_value as long) instant_value "
                    + "from schema_ts_replay")
                    .noLeakCheck().expectSize().returns("value\tinstant_value\n1001000\t123\n");
        });
    }

    private void createTable(String name, Target target) throws Exception {
        execute("create table " + name + " (case_id long, value " + target.sqlType
                + ", ts timestamp) timestamp(ts) partition by day wal");
    }

    private static void assertTypedWire(
            QwpWebSocketEncoder encoder,
            int length,
            Target target,
            List<Vector> vectors
    ) throws Exception {
        QwpTableBlockCursor table = parseSingleTable(encoder, length);
        Assert.assertTrue(table.hasKnownSchemaIdentity());
        Assert.assertEquals(target.wireType, table.getColumnDef(1).getTypeCode());
        QwpTimestampColumnCursor value = table.getTimestampColumn(1);
        int rows = 0;
        for (Vector vector : vectors) {
            if (vector.invalid()) {
                continue;
            }
            Assert.assertTrue(vector.caseId, table.hasNextRow());
            table.nextRow();
            Assert.assertFalse(vector.caseId, table.isColumnNull(1));
            Assert.assertEquals(vector.caseId, vector.expectedWire, value.getTimestamp());
            rows++;
        }
        Assert.assertEquals(rows, table.getRowCount());
        Assert.assertFalse(table.hasNextRow());
    }

    private static void sendLegacyAccepted(
            int port,
            String tableName,
            Target target,
            List<Vector> vectors,
            ChronoUnit unit
    ) throws Exception {
        try (WebSocketClient client = connectLegacy(port);
             QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
             QwpTableBuffer table = new QwpTableBuffer(tableName)) {
            byte sourceType = unit == ChronoUnit.NANOS
                    ? QwpConstants.TYPE_TIMESTAMP_NANOS : QwpConstants.TYPE_TIMESTAMP;
            int caseId = 0;
            for (Vector vector : vectors) {
                if (!vector.invalid() && vector.unit == unit) {
                    table.getOrCreateColumn("case_id", QwpConstants.TYPE_LONG, true).addLong(caseId);
                    table.getOrCreateColumn("value", sourceType, true).addLong(vector.input);
                    table.nextRow();
                }
                if (!vector.invalid()) {
                    caseId++;
                }
            }
            if (table.getRowCount() > 0) {
                int length = encoder.encode(table);
                QwpTableBlockCursor wire = parseSingleTable(encoder, length);
                Assert.assertEquals(sourceType, wire.getColumnDef(1).getTypeCode());
                sendLegacy(client, encoder, table, length);
            }
        }
    }

    private static void sendLegacyRejected(int port, String tableName, List<Vector> vectors) throws Exception {
        for (Vector vector : vectors) {
            if (!vector.invalid()) {
                continue;
            }
            try (WebSocketClient client = connectLegacy(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = new QwpTableBuffer(tableName)) {
                table.getOrCreateColumn("case_id", QwpConstants.TYPE_LONG, true).addLong(-1);
                table.getOrCreateColumn("value", QwpConstants.TYPE_TIMESTAMP, true).addLong(vector.input);
                table.nextRow();
                int length = encoder.encode(table);
                Assert.assertEquals(0, Unsafe.getByte(encoder.getBuffer().getBufferPtr() + QwpConstants.HEADER_OFFSET_FLAGS)
                        & QwpConstants.FLAG_SCHEMA);
                client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                WebSocketResponse response = receive(client);
                Assert.assertFalse(vector.caseId, response.isSuccess());
                Assert.assertTrue(vector.caseId + ": " + response.getErrorMessage(),
                        response.getErrorMessage().contains("overflow"));
            }
        }
    }

    private static String expectedRows(List<Vector> vectors) {
        StringBuilder sink = new StringBuilder("case_id\tvalue\n");
        int caseId = 0;
        for (Vector vector : vectors) {
            if (!vector.invalid()) {
                sink.append(caseId++).append('\t')
                        .append("NULL".equals(vector.expectedSql) ? "null" : vector.expectedSql).append('\n');
            }
        }
        return sink.toString();
    }

    private static String expectedInputRows(List<InputVector> vectors) {
        StringBuilder sink = new StringBuilder("case_id\tvalue\n");
        int caseId = 0;
        for (InputVector vector : vectors) {
            if (!vector.invalid()) {
                sink.append(caseId++).append('\t')
                        .append("NULL".equals(vector.expectedSql) ? "null" : vector.expectedSql).append('\n');
            }
        }
        return sink.toString();
    }

    private static List<InputVector> forInputTarget(List<InputVector> all, Target target) {
        List<InputVector> selected = new ArrayList<>();
        for (InputVector vector : all) {
            if (vector.target == target) {
                selected.add(vector);
            }
        }
        return selected;
    }

    private static List<Vector> forTarget(List<Vector> all, Target target) {
        List<Vector> selected = new ArrayList<>();
        for (Vector vector : all) {
            if (vector.target == target) {
                selected.add(vector);
            }
        }
        return selected;
    }

    private static List<Vector> readVectors() throws Exception {
        InputStream stream = QwpSchemaTimestampE2ETest.class.getResourceAsStream(VECTORS);
        Assert.assertNotNull(VECTORS, stream);
        List<Vector> vectors = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty() || line.charAt(0) == '#') {
                    continue;
                }
                String[] fields = line.split("\\t", -1);
                Assert.assertEquals(line, 6, fields.length);
                ChronoUnit unit = ChronoUnit.valueOf(fields[2]);
                Target target = Target.of(fields[3]);
                boolean invalid = "<INVALID>".equals(fields[4]);
                vectors.add(new Vector(fields[0], Long.parseLong(fields[1]), unit, target,
                        invalid, invalid ? 0 : Long.parseLong(fields[4]), fields[5]));
            }
        }
        return vectors;
    }

    private static List<InputVector> readInputVectors() throws Exception {
        InputStream stream = QwpSchemaTimestampE2ETest.class.getResourceAsStream(INPUT_VECTORS);
        Assert.assertNotNull(INPUT_VECTORS, stream);
        List<InputVector> vectors = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty() || line.charAt(0) == '#') {
                    continue;
                }
                String[] fields = line.split("\\t", -1);
                Assert.assertEquals(line, 7, fields.length);
                boolean instant = "INSTANT".equals(fields[1]);
                Assert.assertTrue(line, instant || "LONG".equals(fields[1]));
                ChronoUnit unit = instant ? null : ChronoUnit.valueOf(fields[3]);
                Target target = Target.of(fields[4]);
                boolean invalid = "<INVALID>".equals(fields[5]);
                Assert.assertEquals(line, invalid, "<INVALID>".equals(fields[6]));
                vectors.add(new InputVector(
                        fields[0],
                        instant,
                        fields[2],
                        unit,
                        target,
                        invalid,
                        invalid ? 0 : Long.parseLong(fields[5]),
                        fields[6]
                ));
            }
        }
        return vectors;
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

    private static void sendLegacy(WebSocketClient client, QwpWebSocketEncoder encoder, QwpTableBuffer table) {
        sendLegacy(client, encoder, table, encoder.encode(table));
    }

    private static void sendLegacy(
            WebSocketClient client,
            QwpWebSocketEncoder encoder,
            QwpTableBuffer table,
            int length
    ) {
        Assert.assertEquals(0, Unsafe.getByte(encoder.getBuffer().getBufferPtr() + QwpConstants.HEADER_OFFSET_FLAGS)
                & QwpConstants.FLAG_SCHEMA);
        client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
        assertOk(client);
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

    private static void assertOk(WebSocketClient client) {
        WebSocketResponse response = receive(client);
        Assert.assertTrue(response.getErrorMessage(), response.isSuccess());
        Assert.assertEquals(0, response.getSequence());
    }

    private static WebSocketResponse receive(WebSocketClient client) {
        AtomicReference<WebSocketResponse> response = new AtomicReference<>();
        Assert.assertTrue(client.receiveFrame(new WebSocketFrameHandler() {
            @Override
            public void onBinaryMessage(long payloadPtr, int payloadLen) {
                WebSocketResponse parsed = new WebSocketResponse();
                Assert.assertTrue(parsed.readFrom(payloadPtr, payloadLen, true));
                response.set(parsed);
            }

            @Override
            public void onClose(int code, String reason) {
                Assert.fail("unexpected close [code=" + code + ", reason=" + reason + ']');
            }
        }, 5_000));
        Assert.assertNotNull(response.get());
        return response.get();
    }

    private static QwpTableBlockCursor parseSingleTable(QwpWebSocketEncoder encoder, int length) throws Exception {
        QwpMessageCursor cursor = new QwpMessageCursor();
        cursor.of(encoder.getBuffer().getBufferPtr(), length, new ObjList<>());
        Assert.assertTrue(cursor.hasNextTable());
        QwpTableBlockCursor table = cursor.nextTable();
        Assert.assertFalse(cursor.hasNextTable());
        return table;
    }

    private enum Target {
        MICROS("micro", "timestamp", QwpConstants.TYPE_TIMESTAMP),
        NANOS("nano", "timestamp_ns", QwpConstants.TYPE_TIMESTAMP_NANOS);

        private final String sqlType;
        private final String suffix;
        private final byte wireType;

        Target(String suffix, String sqlType, byte wireType) {
            this.suffix = suffix;
            this.sqlType = sqlType;
            this.wireType = wireType;
        }

        private static Target of(String value) {
            if ("TIMESTAMP".equals(value)) {
                return MICROS;
            }
            if ("TIMESTAMP_NS".equals(value)) {
                return NANOS;
            }
            throw new AssertionError("unknown timestamp target: " + value);
        }
    }

    private static final class InputVector {
        private final String caseId;
        private final long expectedWire;
        private final String expectedSql;
        private final boolean instant;
        private final String input;
        private final boolean invalid;
        private final Target target;
        private final ChronoUnit unit;

        private InputVector(
                String caseId,
                boolean instant,
                String input,
                ChronoUnit unit,
                Target target,
                boolean invalid,
                long expectedWire,
                String expectedSql
        ) {
            this.caseId = caseId;
            this.instant = instant;
            this.input = input;
            this.unit = unit;
            this.target = target;
            this.invalid = invalid;
            this.expectedWire = expectedWire;
            this.expectedSql = expectedSql;
        }

        private void append(QwpSchemaBinding binding) {
            if (instant) {
                binding.timestampColumn("value", Instant.parse(input));
            } else {
                binding.timestampColumn("value", Long.parseLong(input), unit);
            }
        }

        private boolean invalid() {
            return invalid;
        }
    }

    private static final class Vector {
        private final String caseId;
        private final long expectedWire;
        private final String expectedSql;
        private final long input;
        private final boolean invalid;
        private final Target target;
        private final ChronoUnit unit;

        private Vector(
                String caseId,
                long input,
                ChronoUnit unit,
                Target target,
                boolean invalid,
                long expectedWire,
                String expectedSql
        ) {
            this.caseId = caseId;
            this.input = input;
            this.unit = unit;
            this.target = target;
            this.invalid = invalid;
            this.expectedWire = expectedWire;
            this.expectedSql = expectedSql;
        }

        private boolean invalid() {
            return invalid;
        }
    }
}
