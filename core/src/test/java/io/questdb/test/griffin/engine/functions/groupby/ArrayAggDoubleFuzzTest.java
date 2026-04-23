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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * Fuzz tests for array_agg(). Verifies that array_agg correctly collects
 * scalar double values into arrays, with varying group counts, row counts,
 * and NULL ratios.
 */
public class ArrayAggDoubleFuzzTest extends AbstractCairoTest {
    private static final int ITERATIONS = 100;
    private Rnd rnd;

    @Override
    @Before
    public void setUp() {
        rnd = TestUtils.generateRandom(LOG);
        super.setUp();
    }

    @Test
    public void testKeyedGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            for (int iter = 0; iter < ITERATIONS; iter++) {
                int numGroups = rnd.nextInt(5) + 1;
                int rowsPerGroup = rnd.nextInt(20) + 1;
                double nullRatio = rnd.nextDouble() * 0.3;

                execute("DROP TABLE IF EXISTS t");
                execute("CREATE TABLE t (grp INT, val DOUBLE)");

                StringBuilder insert = new StringBuilder("INSERT INTO t VALUES\n");
                // Build expected arrays per group
                StringBuilder[] expectedPerGroup = new StringBuilder[numGroups];
                for (int g = 0; g < numGroups; g++) {
                    expectedPerGroup[g] = new StringBuilder("[");
                }

                boolean first = true;
                for (int g = 0; g < numGroups; g++) {
                    for (int r = 0; r < rowsPerGroup; r++) {
                        if (!first) {
                            insert.append(",\n");
                        }
                        first = false;
                        boolean isNull = rnd.nextDouble() < nullRatio;
                        if (isNull) {
                            insert.append("(").append(g).append(", null)");
                            if (expectedPerGroup[g].length() > 1) {
                                expectedPerGroup[g].append(",");
                            }
                            expectedPerGroup[g].append("null");
                        } else {
                            double val = Math.round(rnd.nextDouble() * 1000.0) / 10.0;
                            insert.append("(").append(g).append(", ").append(val).append(")");
                            if (expectedPerGroup[g].length() > 1) {
                                expectedPerGroup[g].append(",");
                            }
                            expectedPerGroup[g].append(val);
                        }
                    }
                }
                execute(insert.toString());

                for (int g = 0; g < numGroups; g++) {
                    expectedPerGroup[g].append("]");
                }

                // Build expected output
                StringBuilder expected = new StringBuilder("grp\tarr\n");
                for (int g = 0; g < numGroups; g++) {
                    expected.append(g).append("\t").append(expectedPerGroup[g]).append("\n");
                }

                assertQueryNoLeakCheck(
                        expected.toString(),
                        "SELECT grp, array_agg(val) arr FROM t ORDER BY grp",
                        null,
                        true,
                        true
                );
            }
        });
    }

    @Test
    public void testNotKeyedGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            for (int iter = 0; iter < ITERATIONS; iter++) {
                int numRows = rnd.nextInt(30) + 1;
                double nullRatio = rnd.nextDouble() * 0.3;

                execute("DROP TABLE IF EXISTS t");
                execute("CREATE TABLE t (val DOUBLE)");

                StringBuilder insert = new StringBuilder("INSERT INTO t VALUES\n");
                StringBuilder expectedArray = new StringBuilder("[");

                for (int r = 0; r < numRows; r++) {
                    if (r > 0) {
                        insert.append(",\n");
                        expectedArray.append(",");
                    }
                    boolean isNull = rnd.nextDouble() < nullRatio;
                    if (isNull) {
                        insert.append("(null)");
                        expectedArray.append("null");
                    } else {
                        double val = Math.round(rnd.nextDouble() * 1000.0) / 10.0;
                        insert.append("(").append(val).append(")");
                        expectedArray.append(val);
                    }
                }
                execute(insert.toString());
                expectedArray.append("]");

                assertQueryNoLeakCheck(
                        "arr\n" + expectedArray + "\n",
                        "SELECT array_agg(val) arr FROM t",
                        null,
                        false,
                        true
                );
            }
        });
    }

    @Test
    public void testArrayInputConcatenation() throws Exception {
        assertMemoryLeak(() -> {
            for (int iter = 0; iter < ITERATIONS; iter++) {
                int numGroups = rnd.nextInt(5) + 1;
                int rowsPerGroup = rnd.nextInt(10) + 1;

                execute("DROP TABLE IF EXISTS t");
                execute("CREATE TABLE t (grp INT, arr DOUBLE[])");

                StringBuilder insert = new StringBuilder("INSERT INTO t VALUES\n");
                int[] expectedCounts = new int[numGroups];
                double[] expectedSums = new double[numGroups];
                boolean first = true;
                for (int g = 0; g < numGroups; g++) {
                    for (int r = 0; r < rowsPerGroup; r++) {
                        if (!first) {
                            insert.append(",\n");
                        }
                        first = false;
                        boolean isNull = rnd.nextDouble() < 0.2;
                        if (isNull) {
                            insert.append("(").append(g).append(", null)");
                        } else {
                            int arrLen = rnd.nextInt(5) + 1;
                            insert.append("(").append(g).append(", ARRAY[");
                            for (int i = 0; i < arrLen; i++) {
                                if (i > 0) {
                                    insert.append(",");
                                }
                                double val = rnd.nextInt(1000);
                                insert.append(val);
                                expectedSums[g] += val;
                            }
                            insert.append("])");
                            expectedCounts[g] += arrLen;
                        }
                    }
                }
                execute(insert.toString());

                // Verify element counts and sums match per group.
                // When all arrays in a group are null, array_agg returns null:
                // array_count(null) = 0, array_sum(null) = null.
                StringBuilder expected = new StringBuilder("grp\tcnt\tsum\n");
                for (int g = 0; g < numGroups; g++) {
                    expected.append(g).append("\t").append(expectedCounts[g]).append("\t");
                    if (expectedCounts[g] == 0) {
                        expected.append("null");
                    } else {
                        expected.append(expectedSums[g]);
                    }
                    expected.append("\n");
                }

                assertQueryNoLeakCheck(
                        expected.toString(),
                        "SELECT grp, array_count(array_agg(arr)) cnt, array_sum(array_agg(arr)) sum FROM t ORDER BY grp",
                        null,
                        true,
                        true
                );
            }
        });
    }

    @Test
    public void testParallelCountsMatch() throws Exception {
        assertMemoryLeak(() -> {
            for (int iter = 0; iter < ITERATIONS; iter++) {
                int numGroups = rnd.nextInt(5) + 1;
                int rowsPerGroup = rnd.nextInt(20) + 1;

                execute("DROP TABLE IF EXISTS t");
                execute("CREATE TABLE t (grp INT, val DOUBLE)");

                StringBuilder insert = new StringBuilder("INSERT INTO t VALUES\n");
                boolean first = true;
                for (int g = 0; g < numGroups; g++) {
                    for (int r = 0; r < rowsPerGroup; r++) {
                        if (!first) {
                            insert.append(",\n");
                        }
                        first = false;
                        double val = Math.round(rnd.nextDouble() * 1000.0) / 10.0;
                        insert.append("(").append(g).append(", ").append(val).append(")");
                    }
                }
                execute(insert.toString());

                StringBuilder expected = new StringBuilder("grp\tcnt\n");
                for (int g = 0; g < numGroups; g++) {
                    expected.append(g).append("\t").append(rowsPerGroup).append("\n");
                }

                assertQueryNoLeakCheck(
                        expected.toString(),
                        "SELECT grp, array_count(array_agg(val)) cnt FROM t ORDER BY grp",
                        null,
                        true,
                        true
                );
            }
        });
    }
}
