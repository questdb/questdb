/*+*****************************************************************************
 * Copyright (c) 2014-2019 Appsicle
 * Copyright (c) 2019-2026 QuestDB
 * Licensed under the Apache License, Version 2.0
 *******************************************************************************/
package io.questdb.test.cutlass.qwp.e2e;

import io.questdb.client.LineSenderSchemaException;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
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

public class QwpSchemaStringDateE2ETest extends AbstractQwpWebSocketTest {
    private static final String CORPUS = "/io/questdb/client/cutlass/qwp/string-to-date.tsv";

    @Test
    public void testCorpusStoresExactRawMillisAndNullness() throws Exception {
        List<Vector> vectors = readVectors();
        runInContext(port -> {
            execute("create table schema_string_date (case_id long, value date, ts timestamp) "
                    + "timestamp(ts) partition by day wal");
            try (QwpWebSocketSender sender = connectWs(
                    port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                for (int i = 0; i < vectors.size(); i++) {
                    Vector vector = vectors.get(i);
                    sender.table("schema_string_date").longColumn("case_id", i);
                    if (vector.invalid) {
                        LineSenderSchemaException error = Assert.assertThrows(vector.caseId,
                                LineSenderSchemaException.class,
                                () -> sender.stringColumn("value", vector.input));
                        Assert.assertEquals(vector.caseId,
                                LineSenderSchemaException.Reason.INVALID_VALUE, error.getReason());
                    } else {
                        sender.stringColumn("value", vector.input);
                        if (vector.sqlNull) {
                            sender.binaryColumn("value", new byte[]{1});
                        }
                        sender.at(1_000_000L + i, ChronoUnit.MICROS);
                    }
                }
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(fsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }
            drainWalQueue();

            try (RecordCursorFactory factory = select(
                    "select case_id, value, value is null from schema_string_date order by case_id");
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                for (int i = 0; i < vectors.size(); i++) {
                    Vector vector = vectors.get(i);
                    if (!vector.invalid) {
                        Assert.assertTrue(vector.caseId, cursor.hasNext());
                        Assert.assertEquals(vector.caseId, i, record.getLong(0));
                        Assert.assertEquals(vector.caseId, vector.sqlNull, record.getBool(2));
                        Assert.assertEquals(vector.caseId, vector.expectedRaw, record.getDate(1));
                    }
                }
                Assert.assertFalse(cursor.hasNext());
            }
        });
    }

    @Test
    public void testPartialInvalidRowIsCancelledAndNextRowNeedsNoReselection() throws Exception {
        runInContext(port -> {
            execute("create table schema_string_date_rows (value date, marker string, ts timestamp) "
                    + "timestamp(ts) partition by day wal");
            try (QwpWebSocketSender sender = connectWs(
                    port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                sender.table("schema_string_date_rows").stringColumn("value", "1970-01-01")
                        .stringColumn("marker", "A").at(1, ChronoUnit.MICROS);
                sender.stringColumn("marker", "failed-B");
                LineSenderSchemaException error = Assert.assertThrows(LineSenderSchemaException.class,
                        () -> sender.stringColumn("value", "not-a-date"));
                Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, error.getReason());
                Assert.assertTrue(error.getMessage(), error.getMessage().contains("column=value"));
                Assert.assertTrue(error.getMessage(), error.getMessage().contains("targetType=DATE(7)"));
                sender.stringColumn("value", "1969-12-31").stringColumn("marker", "C")
                        .at(2, ChronoUnit.MICROS);
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }
            drainWalQueue();
            assertQuery("select marker, cast(value as long) value from schema_string_date_rows order by ts")
                    .noLeakCheck().expectSize().returns("marker\tvalue\nA\t0\nC\t-86400000\n");
        });
    }

    private static List<Vector> readVectors() throws Exception {
        InputStream stream = QwpSchemaStringDateE2ETest.class.getResourceAsStream(CORPUS);
        Assert.assertNotNull(CORPUS, stream);
        List<Vector> vectors = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            Assert.assertEquals("case_id\tinput_kind\tinput\toutcome\texpected_raw\tfeature", reader.readLine());
            String line;
            while ((line = reader.readLine()) != null) {
                String[] fields = line.split("\t", -1);
                Assert.assertEquals(line, 6, fields.length);
                Assert.assertTrue(line, "VALID".equals(fields[3])
                        || "NULL".equals(fields[3]) || "INVALID".equals(fields[3]));
                String input = decodeInput(fields[1], fields[2]);
                boolean invalid = "INVALID".equals(fields[3]);
                boolean sqlNull = "NULL".equals(fields[3]);
                long expected = sqlNull ? Long.MIN_VALUE : invalid ? 0 : Long.parseLong(fields[4]);
                vectors.add(new Vector(fields[0], input, invalid, sqlNull, expected));
            }
        }
        Assert.assertEquals(53, vectors.size());
        return vectors;
    }

    private static String decodeInput(String kind, String value) {
        Assert.assertTrue(kind, "TEXT".equals(kind) || "UTF16_HEX".equals(kind));
        if ("<NULL>".equals(value)) {
            return null;
        }
        if ("<EMPTY>".equals(value)) {
            return "";
        }
        if ("TEXT".equals(kind)) {
            return value;
        }
        String[] units = value.split(",");
        char[] chars = new char[units.length];
        for (int i = 0; i < units.length; i++) {
            chars[i] = (char) Integer.parseInt(units[i], 16);
        }
        return new String(chars);
    }

    private static final class Vector {
        private final String caseId;
        private final long expectedRaw;
        private final String input;
        private final boolean invalid;
        private final boolean sqlNull;

        private Vector(String caseId, String input, boolean invalid, boolean sqlNull, long expectedRaw) {
            this.caseId = caseId;
            this.input = input;
            this.invalid = invalid;
            this.sqlNull = sqlNull;
            this.expectedRaw = expectedRaw;
        }
    }
}
