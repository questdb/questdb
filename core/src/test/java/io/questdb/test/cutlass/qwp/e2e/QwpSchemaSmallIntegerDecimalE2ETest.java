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

public class QwpSchemaSmallIntegerDecimalE2ETest extends AbstractQwpWebSocketTest {
    private static final String CORPUS = "/io/questdb/client/cutlass/qwp/small-integer-to-decimal.tsv";
    private static final String HEADER = "# case_id\tinput_type\tinput\ttarget_type\ttarget_precision\t"
            + "target_scale\toutcome\texpected_ll_hex\texpected_lh_hex\texpected_hl_hex\texpected_hh_hex\texpected_sql";

    @Test
    public void testPublicSenderCorpusMatchesExactSql() throws Exception {
        List<Vector> vectors = readVectors();
        runInContext(port -> {
            try (Sender sender = connectWs(
                    port,
                    0,
                    0,
                    TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L)
            )) {
                for (int i = 0; i < vectors.size(); i++) {
                    Vector vector = vectors.get(i);
                    String table = "schema_small_integer_decimal_" + i;
                    execute("create table " + table + " (value decimal(" + vector.precision + ','
                            + vector.scale + "), ts timestamp) timestamp(ts) partition by day wal");
                    sender.table(table);
                    if (vector.invalid()) {
                        LineSenderSchemaException error = Assert.assertThrows(
                                vector.caseId,
                                LineSenderSchemaException.class,
                                () -> append(sender, vector)
                        );
                        Assert.assertEquals(
                                vector.caseId,
                                LineSenderSchemaException.Reason.INVALID_VALUE,
                                error.getReason()
                        );
                    } else {
                        append(sender, vector);
                        sender.at(i + 1L, ChronoUnit.MICROS);
                    }
                }
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(fsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }

            drainWalQueue();
            for (int i = 0; i < vectors.size(); i++) {
                Vector vector = vectors.get(i);
                String expected = vector.invalid() ? "value\n"
                        : vector.isNull() ? "value\n\n" : "value\n" + vector.expectedSql + '\n';
                assertQuery("select value from schema_small_integer_decimal_" + i + " order by ts")
                        .noLeakCheck()
                        .expectSize().returns(expected);
            }
        });
    }

    @Test
    public void testPublicSenderRollsBackPartialRowAndKeepsCompletedRows() throws Exception {
        runInContext(port -> {
            execute("create table schema_small_integer_decimal_rows "
                    + "(value decimal(18,2), marker string, bad uuid, ts timestamp) "
                    + "timestamp(ts) partition by day wal");
            try (Sender sender = connectWs(
                    port,
                    0,
                    0,
                    TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L)
            )) {
                sender.table("schema_small_integer_decimal_rows")
                        .byteColumn("value", (byte) 42)
                        .shortColumn("value", (short) 99)
                        .stringColumn("marker", "A")
                        .at(1, ChronoUnit.MICROS);

                sender.stringColumn("marker", "failed-B").shortColumn("value", (short) 99);
                LineSenderSchemaException error = Assert.assertThrows(
                        LineSenderSchemaException.class,
                        () -> sender.intColumn("bad", 1)
                );
                Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, error.getReason());

                sender.shortColumn("value", (short) -7)
                        .stringColumn("marker", "C")
                        .at(2, ChronoUnit.MICROS);
                sender.intColumn("value", Integer.MIN_VALUE)
                        .stringColumn("marker", "source-null")
                        .at(3, ChronoUnit.MICROS);
                sender.stringColumn("marker", "omitted")
                        .at(4, ChronoUnit.MICROS);
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(fsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }

            drainWalQueue();
            assertQuery("select value, value is null n, marker "
                    + "from schema_small_integer_decimal_rows order by ts")
                    .noLeakCheck()
                    .expectSize().returns("value\tn\tmarker\n"
                            + "42.00\tfalse\tA\n"
                            + "-7.00\tfalse\tC\n"
                            + "\ttrue\tsource-null\n"
                            + "\ttrue\tomitted\n");
        });
    }

    private static void append(Sender sender, Vector vector) {
        switch (vector.source) {
            case BYTE -> sender.byteColumn("value", (byte) vector.input);
            case SHORT -> sender.shortColumn("value", (short) vector.input);
            case INT -> sender.intColumn("value", (int) vector.input);
        }
    }

    private static List<Vector> readVectors() throws Exception {
        List<Vector> vectors = new ArrayList<>();
        boolean[][] sourceTargets = new boolean[3][6];
        try (InputStream stream = QwpSchemaSmallIntegerDecimalE2ETest.class.getResourceAsStream(CORPUS)) {
            Assert.assertNotNull(CORPUS, stream);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                Assert.assertEquals(HEADER, reader.readLine());
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] fields = line.split("\t", -1);
                    Assert.assertEquals(line, 12, fields.length);
                    Vector vector = new Vector(fields);
                    sourceTargets[vector.source.ordinal()][targetIndex(vector.targetType)] = true;
                    vectors.add(vector);
                }
            }
        }
        Assert.assertEquals(30, vectors.size());
        for (int source = 0; source < sourceTargets.length; source++) {
            for (int target = 0; target < sourceTargets[source].length; target++) {
                Assert.assertTrue("missing source/target coverage " + source + '/' + target,
                        sourceTargets[source][target]);
            }
        }
        return vectors;
    }

    private static int targetIndex(String target) {
        return switch (target) {
            case "DECIMAL8" -> 0;
            case "DECIMAL16" -> 1;
            case "DECIMAL32" -> 2;
            case "DECIMAL64" -> 3;
            case "DECIMAL128" -> 4;
            default -> {
                Assert.assertEquals("DECIMAL256", target);
                yield 5;
            }
        };
    }

    private enum Source {
        BYTE,
        SHORT,
        INT
    }

    private static final class Vector {
        private final String caseId;
        private final String expectedSql;
        private final long input;
        private final String outcome;
        private final int precision;
        private final int scale;
        private final Source source;
        private final String targetType;

        private Vector(String[] fields) {
            caseId = fields[0];
            source = Source.valueOf(fields[1]);
            input = Long.parseLong(fields[2]);
            targetType = fields[3];
            precision = Integer.parseInt(fields[4]);
            scale = Integer.parseInt(fields[5]);
            outcome = fields[6];
            expectedSql = fields[11];
            Assert.assertTrue(outcome.equals("VALUE") || outcome.equals("INVALID") || outcome.equals("NULL"));
            Assert.assertEquals(storageType(precision), targetType);
        }

        private boolean invalid() {
            return outcome.equals("INVALID");
        }

        private boolean isNull() {
            return outcome.equals("NULL");
        }

        private static String storageType(int precision) {
            if (precision <= 2) {
                return "DECIMAL8";
            }
            if (precision <= 4) {
                return "DECIMAL16";
            }
            if (precision <= 9) {
                return "DECIMAL32";
            }
            if (precision <= 18) {
                return "DECIMAL64";
            }
            if (precision <= 38) {
                return "DECIMAL128";
            }
            return "DECIMAL256";
        }
    }
}
