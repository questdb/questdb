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

package io.questdb.test.griffin.engine.functions.array;

import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * Fuzz tests for array_build(). Verifies that array_build with scalar fillers
 * produces results equivalent to array[] literal construction, and that
 * array-copy operations preserve values correctly.
 */
public class BuildArrayFuzzTest extends AbstractCairoTest {
    private static final int ITERATIONS = 100;
    private Rnd rnd;

    @Override
    @Before
    public void setUp() {
        rnd = TestUtils.generateRandom(LOG);
        super.setUp();
    }

    @Test
    public void testArrayCopyIdentity() throws Exception {
        assertMemoryLeak(() -> {
            for (int i = 0; i < ITERATIONS; i++) {
                int size = rnd.nextInt(15) + 1;
                execute("DROP TABLE IF EXISTS t");
                execute("CREATE TABLE t AS (SELECT rnd_double_array(1, 0, 0, " + size + ") AS arr FROM long_sequence(5))");

                // array_build(1, arr, arr) should be identical to arr
                printSql("SELECT array_build(1, arr, arr) AS v FROM t");
                String copyResult = sink.toString();

                printSql("SELECT arr AS v FROM t");
                String origResult = sink.toString();

                TestUtils.assertEquals("identity copy iteration=" + i + " size=" + size, origResult, copyResult);
            }
        });
    }

    @Test
    public void testArrayCopyIdentity2d() throws Exception {
        assertMemoryLeak(() -> {
            for (int i = 0; i < ITERATIONS; i++) {
                int size = rnd.nextInt(10) + 1;
                execute("DROP TABLE IF EXISTS t");
                execute("CREATE TABLE t AS (SELECT rnd_double_array(1, 0, 0, " + size + ") AS a, rnd_double_array(1, 0, 0, " + size + ") AS b FROM long_sequence(5))");

                // array_build(2, a, a, b) should be identical to ARRAY[a, b]
                printSql("SELECT array_build(2, a, a, b) AS v FROM t");
                String buildResult = sink.toString();

                printSql("SELECT ARRAY[a, b] AS v FROM t");
                String refResult = sink.toString();

                TestUtils.assertEquals("2D identity copy iteration=" + i + " size=" + size, refResult, buildResult);
            }
        });
    }

    @Test
    public void testScalarFill1d() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT x AS id FROM long_sequence(1))");

            StringSink buildArraySql = new StringSink();
            StringSink referenceSql = new StringSink();
            for (int i = 0; i < ITERATIONS; i++) {
                int size = rnd.nextInt(20);
                double fillVal = rnd.nextDouble() * 200 - 100;

                // array_build(1, size, fillVal)
                buildArraySql.clear();
                buildArraySql.put("SELECT array_build(1, ").put(size).put(", ").put(fillVal).put(") AS v FROM t");

                // equivalent: array[fillVal, fillVal, ...]
                referenceSql.clear();
                referenceSql.put("SELECT ARRAY[");
                for (int j = 0; j < size; j++) {
                    if (j > 0) {
                        referenceSql.put(", ");
                    }
                    referenceSql.put(fillVal);
                }
                referenceSql.put("]::DOUBLE[] AS v FROM t");

                printSql(referenceSql);
                String expected = sink.toString();
                printSql(buildArraySql);
                String actual = sink.toString();
                TestUtils.assertEquals("iteration " + i + ": array_build(1, " + size + ", " + fillVal + ")", expected, actual);
            }
        });
    }

    @Test
    public void testScalarFill2d() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT x AS id FROM long_sequence(1))");

            StringSink buildArraySql = new StringSink();
            StringSink referenceSql = new StringSink();
            for (int i = 0; i < ITERATIONS; i++) {
                int size = rnd.nextInt(10);
                double fill1 = rnd.nextDouble() * 100;
                double fill2 = rnd.nextDouble() * 100;

                // array_build(2, size, fill1, fill2)
                buildArraySql.clear();
                buildArraySql.put("SELECT array_build(2, ").put(size)
                        .put(", ").put(fill1)
                        .put(", ").put(fill2)
                        .put(") AS v FROM t");

                // equivalent: array[ array[fill1 x size], array[fill2 x size] ]
                referenceSql.clear();
                referenceSql.put("SELECT ARRAY[ARRAY[");
                for (int j = 0; j < size; j++) {
                    if (j > 0) {
                        referenceSql.put(", ");
                    }
                    referenceSql.put(fill1);
                }
                referenceSql.put("], ARRAY[");
                for (int j = 0; j < size; j++) {
                    if (j > 0) {
                        referenceSql.put(", ");
                    }
                    referenceSql.put(fill2);
                }
                referenceSql.put("]] AS v FROM t");

                printSql(referenceSql);
                String expected = sink.toString();
                printSql(buildArraySql);
                String actual = sink.toString();
                TestUtils.assertEquals("iteration " + i, expected, actual);
            }
        });
    }

    @Test
    public void testScalarFillNd() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT x AS id FROM long_sequence(1))");

            StringSink buildArraySql = new StringSink();
            StringSink referenceSql = new StringSink();
            for (int i = 0; i < ITERATIONS; i++) {
                int nArrays = rnd.nextInt(5) + 2; // 2..6
                int size = rnd.nextInt(8);
                double[] fills = new double[nArrays];
                for (int k = 0; k < nArrays; k++) {
                    fills[k] = rnd.nextDouble() * 100;
                }

                // array_build(nArrays, size, fill0, fill1, ..., fillN-1)
                buildArraySql.clear();
                buildArraySql.put("SELECT array_build(").put(nArrays).put(", ").put(size);
                for (int k = 0; k < nArrays; k++) {
                    buildArraySql.put(", ").put(fills[k]);
                }
                buildArraySql.put(") AS v FROM t");

                // equivalent: ARRAY[ARRAY[fill0 x size], ARRAY[fill1 x size], ...]
                referenceSql.clear();
                referenceSql.put("SELECT ARRAY[");
                for (int k = 0; k < nArrays; k++) {
                    if (k > 0) {
                        referenceSql.put(", ");
                    }
                    referenceSql.put("ARRAY[");
                    for (int j = 0; j < size; j++) {
                        if (j > 0) {
                            referenceSql.put(", ");
                        }
                        referenceSql.put(fills[k]);
                    }
                    referenceSql.put(']');
                }
                referenceSql.put("] AS v FROM t");

                printSql(referenceSql);
                String expected = sink.toString();
                printSql(buildArraySql);
                String actual = sink.toString();
                TestUtils.assertEquals("iteration " + i + " nArrays=" + nArrays + " size=" + size, expected, actual);
            }
        });
    }
}
