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

import io.questdb.client.Sender;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class QwpSchemaIntegerTemporalE2ETest extends AbstractQwpWebSocketTest {
    private static final String VECTORS = "/io/questdb/client/cutlass/qwp/integer-temporal-conversions.tsv";

    @Test
    public void testPublicSenderConversionCorpusStoresRawTargetValues() throws Exception {
        List<Vector> vectors = readVectors();
        runInContext(port -> {
            for (Target target : Target.values()) {
                String tableName = "schema_integer_" + target.sqlName.toLowerCase();
                execute("create table " + tableName + " (case_id long, value " + target.sqlName
                        + ", ts timestamp) timestamp(ts) partition by day wal");
                List<Vector> targetVectors = forTarget(vectors, target);
                try (Sender sender = connectWs(
                        port,
                        0,
                        0,
                        TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L)
                )) {
                    long caseId = 0;
                    for (Vector vector : targetVectors) {
                        sender.table(tableName).longColumn("case_id", caseId++);
                        append(sender, vector);
                        sender.atNow();
                    }
                    long fsn = sender.flushAndGetSequence();
                    Assert.assertTrue(target.name(), fsn >= 0);
                    Assert.assertTrue(target.name(), sender.awaitAckedFsn(fsn, 10_000));
                }

                drainWalQueue();
                assertQuery("select case_id, cast(value as long) value, value is null n from "
                        + tableName + " order by case_id")
                        .noLeakCheck()
                        .returnsOnce(expectedValues(targetVectors));
            }
        });
    }

    private static void append(Sender sender, Vector vector) {
        switch (vector.source) {
            case BYTE -> sender.byteColumn("value", (byte) vector.input);
            case SHORT -> sender.shortColumn("value", (short) vector.input);
            case INT -> sender.intColumn("value", (int) vector.input);
            case LONG -> sender.longColumn("value", vector.input);
        }
    }

    private static String expectedValues(List<Vector> vectors) {
        StringBuilder sink = new StringBuilder("case_id\tvalue\tn\n");
        for (int i = 0, n = vectors.size(); i < n; i++) {
            Vector vector = vectors.get(i);
            sink.append(i).append('\t');
            if (vector.nullValue()) {
                sink.append("null\ttrue\n");
            } else {
                sink.append(vector.expected).append("\tfalse\n");
            }
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

    private static List<Vector> readVectors() throws Exception {
        List<Vector> vectors = new ArrayList<>();
        try (InputStream stream = QwpSchemaIntegerTemporalE2ETest.class.getResourceAsStream(VECTORS)) {
            Assert.assertNotNull(stream);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (!line.startsWith("#")) {
                        String[] fields = line.split("\t", -1);
                        Assert.assertEquals(line, 5, fields.length);
                        vectors.add(new Vector(
                                fields[0],
                                Source.valueOf(fields[1]),
                                Long.parseLong(fields[2]),
                                Target.valueOf(fields[3]),
                                fields[4]
                        ));
                    }
                }
            }
        }
        Assert.assertEquals(24, vectors.size());
        return vectors;
    }

    private enum Source {
        BYTE,
        SHORT,
        INT,
        LONG
    }

    private enum Target {
        DATE("DATE"),
        TIMESTAMP("TIMESTAMP"),
        TIMESTAMP_NS("TIMESTAMP_NS");

        private final String sqlName;

        Target(String sqlName) {
            this.sqlName = sqlName;
        }
    }

    private record Vector(String caseId, Source source, long input, Target target, String expected) {
        boolean nullValue() {
            return "<NULL>".equals(expected);
        }
    }
}
