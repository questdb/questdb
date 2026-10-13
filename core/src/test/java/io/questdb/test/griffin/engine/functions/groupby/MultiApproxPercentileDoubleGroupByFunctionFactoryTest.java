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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.DirectArray;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class MultiApproxPercentileDoubleGroupByFunctionFactoryTest extends AbstractCairoTest {
    private static final String ALL_VARIANTS = allVariants("ARRAY[0.5]");

    @Test
    public void testAllVariantsEmptyAndNullGroups() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (k SYMBOL, d DOUBLE, l LONG)");
            // non-keyed over an empty table: the value only ever sees setEmpty()
            assertQuery("SELECT " + ALL_VARIANTS + " FROM t")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            d1\td4\tl1\tl4
                            null\tnull\tnull\tnull
                            """);

            execute("INSERT INTO t VALUES ('a', 1.0, 1), ('b', null, null), ('a', 1.0, 1), ('b', null, null)");
            // 'b' has a histogram, but no values were recorded into it
            assertQuery("SELECT k, " + ALL_VARIANTS + " FROM t ORDER BY k")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            k\td1\td4\tl1\tl4
                            a\t[1.0]\t[1.0]\t[1.0]\t[1.0]
                            b\tnull\tnull\tnull\tnull
                            """);
            assertQuery("SELECT " + ALL_VARIANTS + " FROM t WHERE k = 'b'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            d1\td4\tl1\tl4
                            null\tnull\tnull\tnull
                            """);
        });
    }

    @Test
    public void testAllVariantsSampleByFillPrevAlignToFirstObservation() throws Exception {
        // The keyed fill cursor seeds every key with setNull() before the first bucket. 'c' has no
        // rows until 03:00, so it must read as null rather than as the histogram of the first group.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (k SYMBOL, d DOUBLE, l LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                    ('a', 1.0, 1, '2024-01-01T00:00:00.000000Z'),
                    ('b', 1.0, 1, '2024-01-01T00:40:00.000000Z'),
                    ('a', 1.0, 1, '2024-01-01T02:00:00.000000Z'),
                    ('c', 1.0, 1, '2024-01-01T02:45:00.000000Z')
                    """);
            assertQuery("SELECT ts, k, " + ALL_VARIANTS + " FROM t SAMPLE BY 1h FILL(PREV) ALIGN TO FIRST OBSERVATION")
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("ts")
                    .returns("""
                            ts\tk\td1\td4\tl1\tl4
                            2024-01-01T00:00:00.000000Z\ta\t[1.0]\t[1.0]\t[1.0]\t[1.0]
                            2024-01-01T00:00:00.000000Z\tb\t[1.0]\t[1.0]\t[1.0]\t[1.0]
                            2024-01-01T00:00:00.000000Z\tc\tnull\tnull\tnull\tnull
                            2024-01-01T01:00:00.000000Z\ta\t[1.0]\t[1.0]\t[1.0]\t[1.0]
                            2024-01-01T01:00:00.000000Z\tb\t[1.0]\t[1.0]\t[1.0]\t[1.0]
                            2024-01-01T01:00:00.000000Z\tc\tnull\tnull\tnull\tnull
                            2024-01-01T02:00:00.000000Z\ta\t[1.0]\t[1.0]\t[1.0]\t[1.0]
                            2024-01-01T02:00:00.000000Z\tb\t[1.0]\t[1.0]\t[1.0]\t[1.0]
                            2024-01-01T02:00:00.000000Z\tc\t[1.0]\t[1.0]\t[1.0]\t[1.0]
                            """);
            assertQuery("SELECT ts, k, " + ALL_VARIANTS + " FROM t SAMPLE BY 1h FILL(NULL) ALIGN TO FIRST OBSERVATION")
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("ts")
                    .returns("""
                            ts\tk\td1\td4\tl1\tl4
                            2024-01-01T00:00:00.000000Z\ta\t[1.0]\t[1.0]\t[1.0]\t[1.0]
                            2024-01-01T00:00:00.000000Z\tb\t[1.0]\t[1.0]\t[1.0]\t[1.0]
                            2024-01-01T00:00:00.000000Z\tc\tnull\tnull\tnull\tnull
                            2024-01-01T01:00:00.000000Z\ta\tnull\tnull\tnull\tnull
                            2024-01-01T01:00:00.000000Z\tb\tnull\tnull\tnull\tnull
                            2024-01-01T01:00:00.000000Z\tc\tnull\tnull\tnull\tnull
                            2024-01-01T02:00:00.000000Z\ta\t[1.0]\t[1.0]\t[1.0]\t[1.0]
                            2024-01-01T02:00:00.000000Z\tb\tnull\tnull\tnull\tnull
                            2024-01-01T02:00:00.000000Z\tc\t[1.0]\t[1.0]\t[1.0]\t[1.0]
                            """);
        });
    }

    @Test
    public void testAllVariantsWindowJoinEmptyWindow() throws Exception {
        // WINDOW JOIN resets the value with setEmpty() for every master row. The 00:10 trade has no
        // price in its window, so it must read as null rather than as the histogram of the first row.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE prices (d DOUBLE, l LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO trades VALUES
                    (1.0, '2024-01-01T00:00:00.000000Z'),
                    (2.0, '2024-01-01T00:10:00.000000Z'),
                    (3.0, '2024-01-01T00:20:00.000000Z')
                    """);
            execute("""
                    INSERT INTO prices VALUES
                    (1.0, 1, '2024-01-01T00:00:30.000000Z'),
                    (1.0, 1, '2024-01-01T00:00:40.000000Z'),
                    (1.0, 1, '2024-01-01T00:20:10.000000Z')
                    """);
            assertQuery("""
                    SELECT t.ts,
                        approx_percentile(p.d, ARRAY[0.5]) d1, approx_percentile(p.d, ARRAY[0.5], 4) d4,
                        approx_percentile(p.l, ARRAY[0.5]) l1, approx_percentile(p.l, ARRAY[0.5], 4) l4
                    FROM trades t
                    WINDOW JOIN prices p
                    RANGE BETWEEN 1 MINUTE PRECEDING AND 1 MINUTE FOLLOWING
                    EXCLUDE PREVAILING
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\td1\td4\tl1\tl4
                            2024-01-01T00:00:00.000000Z\t[1.0]\t[1.0]\t[1.0]\t[1.0]
                            2024-01-01T00:10:00.000000Z\tnull\tnull\tnull\tnull
                            2024-01-01T00:20:00.000000Z\t[1.0]\t[1.0]\t[1.0]\t[1.0]
                            """);
        });
    }

    @Test
    public void testAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (x DOUBLE)");
            execute("INSERT INTO test VALUES (null), (null), (null)");
            assertQuery("SELECT approx_percentile(x, ARRAY[0.5, 0.95]) FROM test")
                    .noLeakCheck()
                    .returnsOnce("approx_percentile\nnull\n");
        });
    }

    @Test
    public void testAllSameValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT 5.0 x FROM long_sequence(100))");
            assertQuery("SELECT approx_percentile(x, ARRAY[0.25, 0.5, 0.75]) FROM test")
                    .noLeakCheck()
                    .returnsOnce("approx_percentile\n[5.0,5.0,5.0]\n");
        });
    }

    @Test
    public void testBasicMultiPercentile() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT CAST(x AS DOUBLE) x FROM long_sequence(100))");
            assertQuery("SELECT approx_percentile(x, ARRAY[0.5, 1.0]) FROM test")
                    .noLeakCheck()
                    .returnsOnce("approx_percentile\n[51.9375,103.9375]\n");
        });
    }

    @Test
    public void testBindVariablePercentiles() throws Exception {
        // bind variables are runtime constants, so the factories accept them as percentiles
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT x::DOUBLE d, x l FROM long_sequence(100))");
            final String expected = """
                    d1\td4\tl1\tl4
                    [51.9375,91.9375]\t[50.00189208984375,90.00384521484375]\t[51.0,91.0]\t[50.0,90.0]
                    """;
            assertQuery("SELECT " + allVariants("ARRAY[0.5, 0.9]") + " FROM t")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);

            // an array built from bind variables
            bindVariableService.clear();
            bindVariableService.setDouble(0, 0.5);
            bindVariableService.setDouble(1, 0.9);
            assertQuery("SELECT " + allVariants("ARRAY[$1, $2]") + " FROM t")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);

            // an array bind variable
            try (DirectArray percentiles = new DirectArray()) {
                percentiles.setType(ColumnType.encodeArrayType(ColumnType.DOUBLE, 1));
                percentiles.setDimLen(0, 2);
                percentiles.applyShape();
                percentiles.putDouble(0, 0.5);
                percentiles.putDouble(1, 0.9);
                bindVariableService.clear();
                bindVariableService.setArray(0, percentiles);
                assertQuery("SELECT " + allVariants("$1") + " FROM t")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns(expected);
            }
        });
    }

    @Test
    public void testEmptyArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT CAST(x AS DOUBLE) x FROM long_sequence(100))");
            assertQuery("SELECT approx_percentile(x, ARRAY[]) FROM test")
                    .noLeakCheck()
                    .returnsOnce("approx_percentile\n[]\n");
        });
    }

    @Test
    public void testEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (x DOUBLE)");
            assertQuery("SELECT approx_percentile(x, ARRAY[0.5, 0.95]) FROM test")
                    .noLeakCheck()
                    .returnsOnce("approx_percentile\nnull\n");
        });
    }

    @Test
    public void testGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (category SYMBOL, value DOUBLE)");
            execute("""
                    INSERT INTO test VALUES
                    ('A', 1.0), ('A', 2.0), ('A', 3.0), ('A', 4.0), ('A', 5.0),
                    ('B', 10.0), ('B', 20.0), ('B', 30.0), ('B', 40.0), ('B', 50.0)
                    """);
            String result = """
                    category\tapprox_percentile
                    A\t[3.0625,5.1875]
                    B\t[30.5,51.5]
                    """;
            assertQuery("SELECT category, approx_percentile(value, ARRAY[0.5, 1.0]) FROM test ORDER BY category")
                    .noLeakCheck()
                    .returnsOnce(result);
        });
    }

    @Test
    public void testInvalidPercentileInArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT x::double AS val FROM long_sequence(10))");
            try {
                assertQuery("SELECT approx_percentile(val, ARRAY[0.5, 1.5]::DOUBLE[]) FROM test")
                        .noLeakCheck()
                        .returnsOnce("");
                Assert.fail("Expected CairoException for invalid percentile");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "invalid percentile");
            }
        });
    }

    @Test
    public void testNegativePercentileInArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT x::double AS val, x AS lval FROM long_sequence(10))");
            try {
                assertQuery("SELECT approx_percentile(val, ARRAY[0.5, -1.5]::DOUBLE[]) FROM test")
                        .noLeakCheck()
                        .returnsOnce("");
                Assert.fail("Expected CairoException for invalid percentile");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "invalid percentile");
            }
            // a negative percentile within [-1, 0) is rejected too, as the scalar approx_percentile does
            final String sql = "SELECT approx_percentile(val, ARRAY[0.5, -0.25]) FROM test";
            assertExceptionNoLeakCheck(sql, sql.indexOf('['), "invalid percentile [expected=range(0.0, 1.0), actual=-0.25]");
            final String sqlLong = "SELECT approx_percentile(lval, ARRAY[0.5, -0.25], 3) FROM test";
            assertExceptionNoLeakCheck(sqlLong, sqlLong.indexOf('['), "invalid percentile [expected=range(0.0, 1.0), actual=-0.25]");
        });
    }

    @Test
    public void testInvalidNegativePrecision() throws Exception {
        assertException(
                "SELECT approx_percentile(x::DOUBLE, ARRAY[0.5], -1) FROM long_sequence(1)",
                48,
                "precision must be between 0 and 5"
        );
    }

    @Test
    public void testInvalidPrecision() throws Exception {
        assertException(
                "SELECT approx_percentile(x::DOUBLE, ARRAY[0.5], 6) FROM long_sequence(1)",
                48,
                "precision must be between 0 and 5"
        );
    }

    @Test
    public void testLongInputAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (x LONG)");
            execute("INSERT INTO test VALUES (null), (null), (null)");
            assertQuery("SELECT approx_percentile(x, ARRAY[0.5, 0.99]) FROM test")
                    .noLeakCheck()
                    .returnsOnce("approx_percentile\nnull\n");
        });
    }

    @Test
    public void testLongInputHighPrecision() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT x FROM long_sequence(1000))");
            assertQuery("SELECT approx_percentile(x, ARRAY[0.5, 0.95], 3) FROM test")
                    .noLeakCheck()
                    .returnsOnce("approx_percentile\n[500.0,950.0]\n");
        });
    }

    @Test
    public void testLongInputVariant() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT x FROM long_sequence(100))");
            assertQuery("SELECT approx_percentile(x, ARRAY[0.5, 1.0]) FROM test")
                    .noLeakCheck()
                    .returnsOnce("approx_percentile\n[51.0,103.0]\n");
        });
    }

    @Test
    public void testLongInputWithPrecision() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT x FROM long_sequence(1000))");
            assertQuery("SELECT approx_percentile(x, ARRAY[0.5, 0.95], 2) FROM test")
                    .noLeakCheck()
                    .returnsOnce("approx_percentile\n[501.0,951.0]\n");
        });
    }

    @Test
    public void testMultiResultsMatchIndividual() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT CAST(x AS DOUBLE) x FROM long_sequence(100))");

            String singleP50 = queryResult("SELECT approx_percentile(x, 0.5) FROM test");
            String singleP95 = queryResult("SELECT approx_percentile(x, 0.95) FROM test");
            String singleP100 = queryResult("SELECT approx_percentile(x, 1.0) FROM test");

            assertQuery("SELECT approx_percentile(x, ARRAY[0.5, 0.95, 1.0]) FROM test")
                    .noLeakCheck()
                    .returnsOnce("approx_percentile\n[" + singleP50 + "," + singleP95 + "," + singleP100 + "]\n");
        });
    }

    @Test
    public void testNonConstantPercentile() throws Exception {
        // getArray() evaluates the percentiles against the GROUP BY output record, which holds
        // no input columns, so percentiles that vary per row have no meaningful value
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (d DOUBLE, l LONG, arr DOUBLE[])");
            assertQuery("SELECT approx_percentile(d, arr) FROM t")
                    .noLeakCheck()
                    .fails(28, "percentile must be a constant or runtime constant");
            assertQuery("SELECT approx_percentile(d, arr, 4) FROM t")
                    .noLeakCheck()
                    .fails(28, "percentile must be a constant or runtime constant");
            assertQuery("SELECT approx_percentile(l, arr) FROM t")
                    .noLeakCheck()
                    .fails(28, "percentile must be a constant or runtime constant");
            assertQuery("SELECT approx_percentile(l, arr, 4) FROM t")
                    .noLeakCheck()
                    .fails(28, "percentile must be a constant or runtime constant");
            assertQuery("SELECT approx_percentile(d, ARRAY[d / 100]) FROM t")
                    .noLeakCheck()
                    .fails(33, "percentile must be a constant or runtime constant");
        });
    }

    @Test
    public void testPlanShowsPercentiles() throws Exception {
        // covers the DOUBLE and LONG functions, plain and packed
        assertQuery("SELECT " + allVariants("ARRAY[0.5, 0.9]") + " FROM t")
                .ddl("CREATE TABLE t (d DOUBLE, l LONG)")
                .assertsPlan("""
                        GroupBy vectorized: false
                          values: [approx_percentile(d,ARRAY[0.5,0.9]),approx_percentile(d,ARRAY[0.5,0.9]),approx_percentile(l,ARRAY[0.5,0.9]),approx_percentile(l,ARRAY[0.5,0.9])]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: t
                        """);
    }

    @Test
    public void testSinglePercentileInArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT CAST(x AS DOUBLE) x FROM long_sequence(100))");
            assertQuery("SELECT approx_percentile(x, ARRAY[0.5]) FROM test")
                    .noLeakCheck()
                    .returnsOnce("approx_percentile\n[51.9375]\n");
        });
    }

    @Test
    public void testSomeNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (x DOUBLE)");
            execute("INSERT INTO test VALUES (1.0), (null), (null), (null)");
            assertQuery("SELECT approx_percentile(x, ARRAY[0.5, 0.99]) FROM test")
                    .noLeakCheck()
                    .returnsOnce("approx_percentile\n[1.0,1.0]\n");
        });
    }

    @Test
    public void testThrowsOnNegativeDoubleValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (x DOUBLE)");
            execute("INSERT INTO test VALUES (1.0), (-1.0)");
            try {
                assertQuery("SELECT approx_percentile(x, ARRAY[0.5]) FROM test")
                        .noLeakCheck()
                        .returnsOnce("approx_percentile\n[1.0]\n");
                Assert.fail();
            } catch (CairoException ignore) {
            }
        });
    }

    @Test
    public void testWithHighPrecision() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT CAST(x AS DOUBLE) x FROM long_sequence(1000))");
            assertQuery("SELECT approx_percentile(x, ARRAY[0.5, 0.95], 3) FROM test")
                    .noLeakCheck()
                    .returnsOnce("approx_percentile\n[500.2490234375,950.4990234375]\n");
        });
    }

    @Test
    public void testWithPrecision() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT CAST(x AS DOUBLE) x FROM long_sequence(1000))");
            assertQuery("SELECT approx_percentile(x, ARRAY[0.5, 0.95], 2) FROM test")
                    .noLeakCheck()
                    .returnsOnce("approx_percentile\n[501.9921875,951.9921875]\n");
        });
    }

    // DOUBLE and LONG input, each with the default precision and with a precision above 2,
    // which selects the packed histogram implementation
    private static String allVariants(String percentiles) {
        return "approx_percentile(d, " + percentiles + ") d1, approx_percentile(d, " + percentiles + ", 4) d4, " +
                "approx_percentile(l, " + percentiles + ") l1, approx_percentile(l, " + percentiles + ", 4) l4";
    }

    private String queryResult(String query) throws Exception {
        try (var factory = select(query); var cursor = factory.getCursor(sqlExecutionContext)) {
            if (cursor.hasNext()) {
                return String.valueOf(cursor.getRecord().getDouble(0));
            }
        }
        return "null";
    }
}
