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

public class QwpSchemaSmallIntegerTextE2ETest extends AbstractQwpWebSocketTest {
    private static final String CORPUS = "/io/questdb/client/cutlass/qwp/small-integer-to-text.tsv";

    @Test
    public void testPublicSenderCorpusStoresCanonicalTargetText() throws Exception {
        List<Vector> vectors = readVectors();
        runInContext(port -> {
            for (Target target : Target.values()) {
                execute("create table " + target.tableName + " (case_id long, value "
                        + target.sqlType + ", ts timestamp) timestamp(ts) partition by day wal");
            }
            try (Sender sender = connectWs(
                    port,
                    0,
                    0,
                    TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L)
            )) {
                for (Target target : Target.values()) {
                    long caseId = 0;
                    for (Vector vector : vectors) {
                        if (vector.target == target) {
                            sender.table(target.tableName).longColumn("case_id", caseId++);
                            append(sender, vector);
                            sender.atNow();
                        }
                    }
                }
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(fsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }

            drainWalQueue();
            for (Target target : Target.values()) {
                assertQuery("select case_id, value, value is null n from " + target.tableName
                        + " order by case_id")
                        .noLeakCheck()
                        .expectSize().returns(expectedValues(vectors, target));
            }
        });
    }

    @Test
    public void testPublicSenderRollsBackPartialRowAndReusesSymbol() throws Exception {
        runInContext(port -> {
            execute("create table schema_small_integer_text_rows (s string, v varchar, y symbol, "
                    + "marker string, bad uuid, ts timestamp) timestamp(ts) partition by day wal");
            try (Sender sender = connectWs(
                    port,
                    0,
                    0,
                    TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L)
            )) {
                sender.table("schema_small_integer_text_rows")
                        .byteColumn("s", Byte.MIN_VALUE)
                        .shortColumn("v", Short.MIN_VALUE)
                        .intColumn("y", 7)
                        .at(1_000_000, ChronoUnit.MICROS);

                sender.stringColumn("marker", "failed-B").byteColumn("s", (byte) 1);
                LineSenderSchemaException error = Assert.assertThrows(
                        LineSenderSchemaException.class,
                        () -> sender.intColumn("bad", 9)
                );
                Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, error.getReason());
                Assert.assertFalse(error.isRetryable());
                Assert.assertTrue(error.getMessage(), error.getMessage().contains("inputType=INT"));
                Assert.assertTrue(error.getMessage(), error.getMessage().contains("targetType=UUID"));

                sender.byteColumn("s", Byte.MAX_VALUE)
                        .shortColumn("v", Short.MAX_VALUE)
                        .intColumn("y", 7)
                        .at(2_000_000, ChronoUnit.MICROS);
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(fsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }

            drainWalQueue();
            assertQuery("select s, v, y, marker, bad, ts from schema_small_integer_text_rows order by ts")
                    .noLeakCheck()
                    .expectSize().timestamp("ts").returns("s\tv\ty\tmarker\tbad\tts\n"
                            + "-128\t-32768\t7\t\t\t1970-01-01T00:00:01.000000Z\n"
                            + "127\t32767\t7\t\t\t1970-01-01T00:00:02.000000Z\n");
        });
    }

    private static void append(Sender sender, Vector vector) {
        switch (vector.source) {
            case BYTE -> sender.byteColumn("value", (byte) vector.input);
            case SHORT -> sender.shortColumn("value", (short) vector.input);
            case INT -> sender.intColumn("value", (int) vector.input);
        }
    }

    private static String expectedValues(List<Vector> vectors, Target target) {
        StringBuilder sink = new StringBuilder("case_id\tvalue\tn\n");
        int caseId = 0;
        for (Vector vector : vectors) {
            if (vector.target == target) {
                sink.append(caseId++).append('\t');
                if (!vector.nullValue()) {
                    sink.append(vector.expected);
                }
                sink.append('\t').append(vector.nullValue()).append('\n');
            }
        }
        return sink.toString();
    }

    private static List<Vector> readVectors() throws Exception {
        List<Vector> vectors = new ArrayList<>();
        try (InputStream stream = QwpSchemaSmallIntegerTextE2ETest.class.getResourceAsStream(CORPUS)) {
            Assert.assertNotNull(stream);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (!line.startsWith("#")) {
                        String[] fields = line.split("\t", -1);
                        Assert.assertEquals(line, 6, fields.length);
                        String expected = "<NULL>".equals(fields[5]) ? null : fields[5];
                        if (expected != null) {
                            Assert.assertEquals(line, expected, utf8HexToString(fields[4]));
                        } else {
                            Assert.assertEquals(line, "<NULL>", fields[4]);
                        }
                        vectors.add(new Vector(
                                Source.valueOf(fields[1]),
                                Long.parseLong(fields[2]),
                                Target.valueOf(fields[3]),
                                expected
                        ));
                    }
                }
            }
        }
        Assert.assertEquals(30, vectors.size());
        for (Target target : Target.values()) {
            int count = 0;
            for (Vector vector : vectors) {
                if (vector.target == target) {
                    count++;
                }
            }
            Assert.assertEquals(target.name(), 10, count);
        }
        return vectors;
    }

    private static String utf8HexToString(String value) {
        byte[] bytes = new byte[value.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) Integer.parseInt(value.substring(i * 2, i * 2 + 2), 16);
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private enum Source {
        BYTE,
        SHORT,
        INT
    }

    private enum Target {
        STRING("schema_small_integer_string", "string"),
        VARCHAR("schema_small_integer_varchar", "varchar"),
        SYMBOL("schema_small_integer_symbol", "symbol");

        private final String sqlType;
        private final String tableName;

        Target(String tableName, String sqlType) {
            this.tableName = tableName;
            this.sqlType = sqlType;
        }
    }

    private record Vector(Source source, long input, Target target, String expected) {
        boolean nullValue() {
            return expected == null;
        }
    }
}
