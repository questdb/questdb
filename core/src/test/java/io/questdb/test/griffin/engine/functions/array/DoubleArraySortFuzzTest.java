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

package io.questdb.test.griffin.engine.functions.array;

import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

public class DoubleArraySortFuzzTest extends AbstractCairoTest {
    private static final int ITERATIONS = 100;
    private Rnd rnd;

    @Override
    @Before
    public void setUp() {
        rnd = TestUtils.generateRandom(LOG);
        super.setUp();
    }

    @Test
    public void testNullsFirstVsLast() throws Exception {
        assertMemoryLeak(() -> {
            StringSink sql = new StringSink();
            for (int i = 0; i < ITERATIONS; i++) {
                int size = rnd.nextInt(20) + 1;
                double[] values = generateRandomArray(size);

                String arrayLiteral = toArrayLiteral(values);

                // count NaN in nulls_first result
                sql.clear();
                sql.put("SELECT array_sort(").put(arrayLiteral).put(", false, true) AS v FROM long_sequence(1)");
                printSql(sql);
                String nullsFirstResult = sink.toString();

                // count NaN in nulls_last result
                sql.clear();
                sql.put("SELECT array_sort(").put(arrayLiteral).put(", false, false) AS v FROM long_sequence(1)");
                printSql(sql);
                String nullsLastResult = sink.toString();

                // both should have the same number of NaN elements
                int nanCountFirst = countNulls(nullsFirstResult);
                int nanCountLast = countNulls(nullsLastResult);
                TestUtils.assertEquals(
                        "NaN count mismatch at iteration=" + i,
                        String.valueOf(nanCountFirst),
                        String.valueOf(nanCountLast)
                );
            }
        });
    }

    @Test
    public void testSortAscendingProperties() throws Exception {
        assertMemoryLeak(() -> {
            StringSink sql = new StringSink();
            for (int i = 0; i < ITERATIONS; i++) {
                int size = rnd.nextInt(30) + 1;
                double[] values = generateRandomArray(size);

                String arrayLiteral = toArrayLiteral(values);

                sql.clear();
                sql.put("SELECT array_sort(").put(arrayLiteral).put(") AS v FROM long_sequence(1)");
                printSql(sql);
                String result = sink.toString();

                double[] sorted = parseResultArray(result);

                // verify length preserved
                TestUtils.assertEquals(
                        "length mismatch at iteration=" + i,
                        String.valueOf(size),
                        String.valueOf(sorted.length)
                );

                // verify ascending order: non-NaN values are sorted, NaN at end
                boolean seenNaN = false;
                for (int j = 0; j < sorted.length; j++) {
                    if (Double.isNaN(sorted[j])) {
                        seenNaN = true;
                    } else {
                        if (seenNaN) {
                            throw new AssertionError("non-NaN after NaN at iteration=" + i + " index=" + j);
                        }
                        if (j > 0 && !Double.isNaN(sorted[j - 1]) && sorted[j] < sorted[j - 1]) {
                            throw new AssertionError("not ascending at iteration=" + i + " index=" + j);
                        }
                    }
                }

                // verify same multiset: sort both input and output, compare
                Arrays.sort(values);
                Arrays.sort(sorted);
                TestUtils.assertEquals(
                        "multiset mismatch at iteration=" + i,
                        Arrays.toString(values),
                        Arrays.toString(sorted)
                );
            }
        });
    }

    @Test
    public void testSortDescendingProperties() throws Exception {
        assertMemoryLeak(() -> {
            StringSink sql = new StringSink();
            for (int i = 0; i < ITERATIONS; i++) {
                int size = rnd.nextInt(30) + 1;
                double[] values = generateRandomArray(size);

                String arrayLiteral = toArrayLiteral(values);

                sql.clear();
                sql.put("SELECT array_sort(").put(arrayLiteral).put(", true) AS v FROM long_sequence(1)");
                printSql(sql);
                String result = sink.toString();

                double[] sorted = parseResultArray(result);

                // verify length preserved
                TestUtils.assertEquals(
                        "length mismatch at iteration=" + i,
                        String.valueOf(size),
                        String.valueOf(sorted.length)
                );

                // verify descending order: NaN at front, then descending non-NaN values
                boolean seenNonNaN = false;
                for (int j = 0; j < sorted.length; j++) {
                    if (!Double.isNaN(sorted[j])) {
                        seenNonNaN = true;
                        if (j > 0 && !Double.isNaN(sorted[j - 1]) && sorted[j] > sorted[j - 1]) {
                            throw new AssertionError("not descending at iteration=" + i + " index=" + j);
                        }
                    } else {
                        if (seenNonNaN) {
                            throw new AssertionError("NaN after non-NaN at iteration=" + i + " index=" + j);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testSortReverseEquivalence() throws Exception {
        assertMemoryLeak(() -> {
            StringSink sql = new StringSink();
            for (int i = 0; i < ITERATIONS; i++) {
                // use arrays without NaN so sort+reverse == sort desc
                int size = rnd.nextInt(20) + 1;
                double[] values = new double[size];
                for (int j = 0; j < size; j++) {
                    values[j] = rnd.nextDouble() * 200 - 100;
                }

                String arrayLiteral = toArrayLiteral(values);

                sql.clear();
                sql.put("SELECT array_sort(").put(arrayLiteral).put(", true, false) AS v FROM long_sequence(1)");
                printSql(sql);
                String descResult = sink.toString();

                sql.clear();
                sql.put("SELECT array_reverse(array_sort(").put(arrayLiteral).put(")) AS v FROM long_sequence(1)");
                printSql(sql);
                String reverseAscResult = sink.toString();

                TestUtils.assertEquals(
                        "sort desc != reverse(sort asc) at iteration=" + i,
                        descResult,
                        reverseAscResult
                );
            }
        });
    }

    @Test
    public void testSortRoundtrip() throws Exception {
        assertMemoryLeak(() -> {
            StringSink sql = new StringSink();
            for (int i = 0; i < ITERATIONS; i++) {
                int size = rnd.nextInt(20) + 1;
                double[] values = generateRandomArray(size);

                String arrayLiteral = toArrayLiteral(values);

                // sort once
                sql.clear();
                sql.put("SELECT array_sort(").put(arrayLiteral).put(") AS v FROM long_sequence(1)");
                printSql(sql);
                String firstSort = sink.toString();

                // sort twice - should be idempotent
                sql.clear();
                sql.put("SELECT array_sort(array_sort(").put(arrayLiteral).put(")) AS v FROM long_sequence(1)");
                printSql(sql);
                String secondSort = sink.toString();

                TestUtils.assertEquals(
                        "idempotence failed at iteration=" + i,
                        firstSort,
                        secondSort
                );
            }
        });
    }

    private int countNulls(String result) {
        int count = 0;
        int idx = 0;
        while ((idx = result.indexOf("null", idx)) >= 0) {
            count++;
            idx += 4;
        }
        return count;
    }

    private double[] generateRandomArray(int size) {
        double[] values = new double[size];
        for (int j = 0; j < size; j++) {
            if (rnd.nextInt(5) == 0) {
                values[j] = Double.NaN;
            } else {
                values[j] = rnd.nextDouble() * 200 - 100;
            }
        }
        return values;
    }

    private double[] parseResultArray(String result) {
        // result format: "v\n[1.0,2.0,null]\n"
        int bracketStart = result.indexOf('[');
        int bracketEnd = result.indexOf(']');
        if (bracketStart < 0 || bracketEnd < 0 || bracketEnd <= bracketStart + 1) {
            return new double[0];
        }
        String inner = result.substring(bracketStart + 1, bracketEnd);
        String[] parts = inner.split(",");
        double[] values = new double[parts.length];
        for (int i = 0; i < parts.length; i++) {
            String part = parts[i].trim();
            if ("null".equals(part) || "NaN".equals(part)) {
                values[i] = Double.NaN;
            } else if ("Infinity".equals(part)) {
                values[i] = Double.POSITIVE_INFINITY;
            } else if ("-Infinity".equals(part)) {
                values[i] = Double.NEGATIVE_INFINITY;
            } else {
                values[i] = Double.parseDouble(part);
            }
        }
        return values;
    }

    private String toArrayLiteral(double[] values) {
        StringSink sb = new StringSink();
        sb.put("ARRAY[");
        for (int j = 0; j < values.length; j++) {
            if (j > 0) {
                sb.put(", ");
            }
            if (Double.isNaN(values[j])) {
                sb.put("NaN");
            } else {
                sb.put(values[j]);
            }
        }
        sb.put(']');
        return sb.toString();
    }
}
