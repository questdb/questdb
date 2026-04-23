/*******************************************************************************
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

package io.questdb.test.griffin.unnest;

import io.questdb.cairo.CairoException;
import io.questdb.griffin.engine.join.JsonUnnestSource;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Fuzz tests for JSON UNNEST. Uses typed array UNNEST as the correctness
 * oracle: identical data inserted as both DOUBLE[] and JSON VARCHAR must
 * produce identical SUM and COUNT results.
 */
public class JsonUnnestFuzzTest extends AbstractCairoTest {
    private static final int ITERATIONS = 100;
    private Rnd rnd;

    @Override
    @Before
    public void setUp() {
        rnd = TestUtils.generateRandom(LOG);
        super.setUp();
    }

    @Test
    public void testRandomTypesNoCrash() throws Exception {
        // Generate JSON objects with random field types and verify UNNEST
        // does not crash. Also verify re-running produces the same
        // result (printSql creates a fresh cursor factory each call).
        assertMemoryLeak(() -> {
            for (int iter = 0; iter < ITERATIONS; iter++) {
                int rows = rnd.nextInt(5) + 1;
                execute("DROP TABLE IF EXISTS t");
                execute("CREATE TABLE t (payload VARCHAR)");

                StringBuilder insertSql = new StringBuilder(
                        "INSERT INTO t VALUES "
                );
                for (int r = 0; r < rows; r++) {
                    if (r > 0) {
                        insertSql.append(", ");
                    }
                    int arrLen = rnd.nextInt(10);
                    StringBuilder json = new StringBuilder("[");
                    for (int j = 0; j < arrLen; j++) {
                        if (j > 0) {
                            json.append(',');
                        }
                        json.append("{\"d\":")
                                .append(rnd.nextInt(10_000) / 100.0)
                                .append(",\"n\":")
                                .append(rnd.nextLong(1_000_000))
                                .append(",\"s\":\"")
                                .append("str").append(rnd.nextInt(100))
                                .append("\",\"b\":")
                                .append(rnd.nextBoolean())
                                .append('}');
                    }
                    json.append(']');
                    insertSql.append("('")
                            .append(json)
                            .append("')");
                }
                execute(insertSql.toString());

                String query = "SELECT u.d, u.n, u.s, u.b "
                        + "FROM t, UNNEST("
                        + "t.payload COLUMNS("
                        + "d DOUBLE, n LONG, s VARCHAR, b BOOLEAN)"
                        + ") u";

                // First run
                printSql(query);
                String first = sink.toString();

                // Second run verifies deterministic output
                printSql(query);
                String second = sink.toString();

                TestUtils.assertEquals(
                        "consistency iteration=" + iter,
                        first,
                        second
                );
            }
        });
    }

    @Test
    public void testRandomTypesNoCrashErrorPaths() throws Exception {
        // Generate iterations with oversized JSON values to exercise
        // the overflow error path, and type mismatches to verify NULL
        // output (no crash).
        assertMemoryLeak(() -> {
            // Oversized JSON values (>4KB per field)
            for (int iter = 0; iter < 2; iter++) {
                execute("DROP TABLE IF EXISTS t");
                execute("CREATE TABLE t (payload VARCHAR)");

                String json = "[{\"s\":\"" + "x".repeat(JsonUnnestSource.DEFAULT_MAX_JSON_UNNEST_VALUE_SIZE + 100) + "\"}]";
                execute("INSERT INTO t VALUES ('" + json + "')");

                try {
                    printSql("SELECT u.s FROM t, UNNEST("
                            + "t.payload COLUMNS(s VARCHAR)) u");
                    Assert.fail("expected CairoException for oversized value");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "exceeds maximum size");
                }
            }

            // Type mismatch: string where DOUBLE expected -> NULL (no crash)
            for (int iter = 0; iter < 2; iter++) {
                execute("DROP TABLE IF EXISTS t");
                execute("CREATE TABLE t (payload VARCHAR)");

                execute("INSERT INTO t VALUES ('[{\"d\":\"not_a_number\"}]')");

                printSql("SELECT u.d FROM t, UNNEST("
                        + "t.payload COLUMNS(d DOUBLE)) u");
                String result = sink.toString();
                // simdjson returns error for string->double, so
                // getDouble returns NaN which prints as "null"
                TestUtils.assertContains(result, "null");
            }
        });
    }

    @Test
    public void testRowCountEquivalence() throws Exception {
        // count(*) via JSON UNNEST must equal count(*) via typed array UNNEST
        // for the same data.
        assertMemoryLeak(() -> {
            for (int iter = 0; iter < ITERATIONS; iter++) {
                int rows = rnd.nextInt(10) + 1;
                execute("DROP TABLE IF EXISTS t");
                execute("CREATE TABLE t (arr DOUBLE[], payload VARCHAR)");

                StringBuilder insertSql = new StringBuilder(
                        "INSERT INTO t VALUES "
                );
                for (int r = 0; r < rows; r++) {
                    if (r > 0) {
                        insertSql.append(", ");
                    }
                    int arrLen = rnd.nextInt(20);
                    StringBuilder arrLiteral = new StringBuilder("ARRAY[");
                    StringBuilder jsonArray = new StringBuilder("[");
                    for (int j = 0; j < arrLen; j++) {
                        if (j > 0) {
                            arrLiteral.append(", ");
                            jsonArray.append(',');
                        }
                        double val = rnd.nextInt(10_000) / 100.0;
                        arrLiteral.append(val);
                        jsonArray.append(val);
                    }
                    arrLiteral.append(']');
                    jsonArray.append(']');
                    if (arrLen == 0) {
                        arrLiteral.append("::DOUBLE[]");
                    }
                    insertSql.append("(")
                            .append(arrLiteral)
                            .append(", '")
                            .append(jsonArray)
                            .append("')");
                }
                execute(insertSql.toString());

                printSql(
                        "SELECT count() cnt "
                                + "FROM t, UNNEST(t.arr) u(val)"
                );
                String viaArray = sink.toString();

                printSql(
                        "SELECT count() cnt "
                                + "FROM t, UNNEST("
                                + "t.payload COLUMNS(val DOUBLE)"
                                + ") u"
                );
                String viaJson = sink.toString();

                TestUtils.assertEquals(
                        "row count iteration=" + iter
                                + " rows=" + rows,
                        viaArray,
                        viaJson
                );
            }
        });
    }

    @Test
    public void testSumEquivalence() throws Exception {
        // SUM via JSON UNNEST must equal SUM via typed array UNNEST for
        // the same underlying data. This is the primary correctness oracle.
        assertMemoryLeak(() -> {
            for (int iter = 0; iter < ITERATIONS; iter++) {
                int rows = rnd.nextInt(10) + 1;
                execute("DROP TABLE IF EXISTS t");
                execute("CREATE TABLE t (arr DOUBLE[], payload VARCHAR)");

                StringBuilder insertSql = new StringBuilder(
                        "INSERT INTO t VALUES "
                );
                for (int r = 0; r < rows; r++) {
                    if (r > 0) {
                        insertSql.append(", ");
                    }
                    int arrLen = rnd.nextInt(20);
                    StringBuilder arrLiteral = new StringBuilder("ARRAY[");
                    StringBuilder jsonArray = new StringBuilder("[");
                    for (int j = 0; j < arrLen; j++) {
                        if (j > 0) {
                            arrLiteral.append(", ");
                            jsonArray.append(',');
                        }
                        double val = rnd.nextInt(10_000) / 100.0;
                        arrLiteral.append(val);
                        jsonArray.append(val);
                    }
                    arrLiteral.append(']');
                    jsonArray.append(']');
                    if (arrLen == 0) {
                        arrLiteral.append("::DOUBLE[]");
                    }
                    insertSql.append("(")
                            .append(arrLiteral)
                            .append(", '")
                            .append(jsonArray)
                            .append("')");
                }
                execute(insertSql.toString());

                printSql(
                        "SELECT round(sum(val), 4) s "
                                + "FROM t, UNNEST(t.arr) u(val)"
                );
                String viaArray = sink.toString();

                printSql(
                        "SELECT round(sum(u.val), 4) s "
                                + "FROM t, UNNEST("
                                + "t.payload COLUMNS(val DOUBLE)"
                                + ") u"
                );
                String viaJson = sink.toString();

                TestUtils.assertEquals(
                        "sum equivalence iteration=" + iter
                                + " rows=" + rows,
                        viaArray,
                        viaJson
                );
            }
        });
    }
}
