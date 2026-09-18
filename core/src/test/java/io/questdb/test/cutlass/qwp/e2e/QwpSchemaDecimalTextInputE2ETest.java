/*+*****************************************************************************
 * Copyright (c) 2014-2019 Appsicle
 * Copyright (c) 2019-2026 QuestDB
 * Licensed under the Apache License, Version 2.0
 *******************************************************************************/
package io.questdb.test.cutlass.qwp.e2e;

import io.questdb.client.LineSenderSchemaException;
import io.questdb.client.Sender;
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

public class QwpSchemaDecimalTextInputE2ETest extends AbstractQwpWebSocketTest {
    private static final String CORPUS = "/io/questdb/client/cutlass/qwp/string-to-decimal.tsv";
    private static final String HEADER = "# case_id\tinput\ttarget_type\ttarget_precision\ttarget_scale\toutcome\texpected_ll_hex\texpected_lh_hex\texpected_hl_hex\texpected_hh_hex\texpected_sql";

    @Test
    public void testPublicSenderStoresExactValuesInEverySupportedTarget() throws Exception {
        List<TargetVector> vectors = readPrecisionMaxVectors();
        Assert.assertEquals(6, vectors.size());

        runInContext(port -> {
            for (int i = 0; i < vectors.size(); i++) {
                TargetVector vector = vectors.get(i);
                execute("CREATE TABLE schema_decimal_text_input_" + i
                        + " (value " + vector.targetSqlType + ", ts TIMESTAMP)"
                        + " TIMESTAMP(ts) PARTITION BY DAY WAL");
            }
            execute("CREATE TABLE schema_decimal_text_input_string"
                    + " (value STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE schema_decimal_text_input_varchar"
                    + " (value VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = connectWs(
                    port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                for (int i = 0; i < vectors.size(); i++) {
                    sender.table("schema_decimal_text_input_" + i)
                            .decimalColumn("value", vectors.get(i).input)
                            .at(i + 1L, ChronoUnit.MICROS);
                }
                sender.table("schema_decimal_text_input_string")
                        .decimalColumn("value", "  +00123.400m")
                        .at(10, ChronoUnit.MICROS);
                sender.table("schema_decimal_text_input_varchar")
                        .decimalColumn("value", "7.89E-3")
                        .at(11, ChronoUnit.MICROS);

                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(fsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }

            drainWalQueue();
            for (int i = 0; i < vectors.size(); i++) {
                TargetVector vector = vectors.get(i);
                String table = "schema_decimal_text_input_" + i;
                assertQuery("SELECT type FROM table_columns('" + table + "') WHERE \"column\" = 'value'")
                        .noLeakCheck().noRandomAccess().returns("type\n" + vector.targetSqlType + "\n");
                assertQuery("SELECT value FROM " + table)
                        .noLeakCheck().expectSize().returns("value\n" + vector.expectedSql + "\n");
            }
            assertQuery("SELECT value FROM schema_decimal_text_input_string")
                    .noLeakCheck().expectSize().returns("value\n123.400\n");
            assertQuery("SELECT value FROM schema_decimal_text_input_varchar")
                    .noLeakCheck().expectSize().returns("value\n0.00789\n");
        });
    }

    @Test
    public void testPublicSenderRollsBackInvalidRowAndPreservesNullAndOmissionSemantics() throws Exception {
        runInContext(port -> {
            execute("CREATE TABLE schema_decimal_text_input_rows "
                    + "(value DECIMAL(3,1), marker STRING, ts TIMESTAMP) "
                    + "TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = connectWs(
                    port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                sender.table("schema_decimal_text_input_rows")
                        .decimalColumn("value", "1.0")
                        .decimalColumn("value", "not-a-decimal")
                        .stringColumn("marker", "A")
                        .at(1, ChronoUnit.MICROS);

                sender.table("schema_decimal_text_input_rows")
                        .stringColumn("marker", "failed-B");
                LineSenderSchemaException error = Assert.assertThrows(
                        LineSenderSchemaException.class,
                        () -> sender.decimalColumn("value", "100.0")
                );
                Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, error.getReason());
                Assert.assertFalse(error.getMessage(), error.isRetryable());
                Assert.assertTrue(error.getMessage(), error.getMessage().contains("column=value"));
                Assert.assertTrue(error.getMessage(), error.getMessage().contains("inputType=DECIMAL256"));

                sender.table("schema_decimal_text_input_rows").decimalColumn("value", "-2.5")
                        .stringColumn("marker", "C")
                        .at(2, ChronoUnit.MICROS);
                sender.table("schema_decimal_text_input_rows").decimalColumn("value", "NaN")
                        .stringColumn("marker", "special-null")
                        .at(3, ChronoUnit.MICROS);
                sender.table("schema_decimal_text_input_rows").decimalColumn("value", (CharSequence) null)
                        .stringColumn("marker", "java-null")
                        .at(4, ChronoUnit.MICROS);
                sender.table("schema_decimal_text_input_rows").decimalColumn("value", "")
                        .stringColumn("marker", "empty")
                        .at(5, ChronoUnit.MICROS);
                sender.table("schema_decimal_text_input_rows").stringColumn("marker", "omitted")
                        .at(6, ChronoUnit.MICROS);

                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(fsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }

            drainWalQueue();
            assertQuery("SELECT value, value IS NULL is_null, marker "
                    + "FROM schema_decimal_text_input_rows ORDER BY ts")
                    .noLeakCheck().expectSize().returns(
                            "value\tis_null\tmarker\n"
                                    + "1.0\tfalse\tA\n"
                                    + "-2.5\tfalse\tC\n"
                                    + "\ttrue\tspecial-null\n"
                                    + "\ttrue\tjava-null\n"
                                    + "\ttrue\tempty\n"
                                    + "\ttrue\tomitted\n"
                    );
        });
    }

    @Test
    public void testPublicSenderMissingTablesInferFiniteAndSpecialDecimal256Scales() throws Exception {
        runInContext(port -> {
            try (Sender sender = connectWs(
                    port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                sender.table("schema_decimal_text_input_missing")
                        .decimalColumn("value", "123.4500m")
                        .at(1, ChronoUnit.MICROS);
                sender.table("schema_decimal_text_input_missing_special")
                        .decimalColumn("value", "-Infinity")
                        .at(2, ChronoUnit.MICROS);
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(fsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }

            drainWalQueue();
            assertQuery("SELECT type FROM table_columns('schema_decimal_text_input_missing') "
                    + "WHERE \"column\" = 'value'")
                    .noLeakCheck().noRandomAccess().returns("type\nDECIMAL(76,4)\n");
            assertQuery("SELECT value FROM schema_decimal_text_input_missing")
                    .noLeakCheck().expectSize().returns("value\n123.4500\n");
            assertQuery("SELECT type FROM table_columns('schema_decimal_text_input_missing_special') "
                    + "WHERE \"column\" = 'value'")
                    .noLeakCheck().noRandomAccess().returns("type\nDECIMAL(76,0)\n");
            assertQuery("SELECT value, value IS NULL is_null FROM schema_decimal_text_input_missing_special")
                    .noLeakCheck().expectSize().returns("value\tis_null\n\ttrue\n");
        });
    }

    private static List<TargetVector> readPrecisionMaxVectors() throws Exception {
        List<TargetVector> vectors = new ArrayList<>();
        try (InputStream stream = QwpSchemaDecimalTextInputE2ETest.class.getResourceAsStream(CORPUS)) {
            Assert.assertNotNull(CORPUS, stream);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                Assert.assertEquals(HEADER, reader.readLine());
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] fields = line.split("\t", -1);
                    Assert.assertEquals(line, 11, fields.length);
                    if (fields[0].endsWith("_precision_max")) {
                        Assert.assertEquals(fields[0], "VALUE", fields[5]);
                        vectors.add(new TargetVector(
                                fields[1],
                                "DECIMAL(" + fields[3] + ',' + fields[4] + ')',
                                fields[10]
                        ));
                    }
                }
            }
        }
        return vectors;
    }

    private static final class TargetVector {
        private final String expectedSql;
        private final String input;
        private final String targetSqlType;

        private TargetVector(String input, String targetSqlType, String expectedSql) {
            this.input = input;
            this.targetSqlType = targetSqlType;
            this.expectedSql = expectedSql;
        }
    }
}
