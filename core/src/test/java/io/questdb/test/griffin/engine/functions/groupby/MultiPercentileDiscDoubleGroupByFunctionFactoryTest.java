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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.DirectArray;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class MultiPercentileDiscDoubleGroupByFunctionFactoryTest extends AbstractCairoTest {

    private static final String txDdl = """
            CREATE TABLE tx_traffic (timestamp TIMESTAMP, value DOUBLE);
            """;

    private static final String txDml = """
            INSERT INTO tx_traffic (timestamp, value) VALUES
            ('2022-05-02T14:51:28Z', '10.978'),
            ('2022-05-02T14:51:58Z', '85.063'),
            ('2022-05-02T14:52:28Z', '148.83'),
            ('2022-05-02T14:52:58Z', '135.881'),
            ('2022-05-02T14:53:28Z', '47.674'),
            ('2022-05-02T14:53:58Z', '61.336'),
            ('2022-05-02T14:54:28Z', '9.651'),
            ('2022-05-02T14:54:58Z', '63.451'),
            ('2022-05-02T14:55:28Z', '8.618'),
            ('2022-05-02T14:55:58Z', '61.774'),
            ('2022-05-02T14:56:28Z', '8.1'),
            ('2022-05-02T14:56:58Z', '177.434'),
            ('2022-05-02T14:57:28Z', '12.955'),
            ('2022-05-02T14:57:58Z', '62.164'),
            ('2022-05-02T14:58:28Z', '161.288'),
            ('2022-05-02T14:58:58Z', '63.363'),
            ('2022-05-02T14:59:28Z', '10.228'),
            ('2022-05-02T14:59:58Z', '60.801'),
            ('2022-05-02T15:00:28Z', '14.053'),
            ('2022-05-02T15:00:58Z', '200.455'),
            ('2022-05-02T15:01:28Z', '406.977'),
            ('2022-05-02T15:01:58Z', '849.035'),
            ('2022-05-02T15:02:28Z', '14.674'),
            ('2022-05-02T15:02:58Z', '126.668'),
            ('2022-05-02T15:03:28Z', '217.782'),
            ('2022-05-02T15:03:58Z', '66.75'),
            ('2022-05-02T15:04:28Z', '39.856'),
            ('2022-05-02T15:04:58Z', '289.615'),
            ('2022-05-02T15:05:28Z', '182.917'),
            ('2022-05-02T15:05:58Z', '219.222'),
            ('2022-05-02T15:06:28Z', '125.86'),
            ('2022-05-02T15:06:58Z', '87.316'),
            ('2022-05-02T15:07:28Z', '191.085'),
            ('2022-05-02T15:07:58Z', '242.04'),
            ('2022-05-02T15:08:28Z', '224.195'),
            ('2022-05-02T15:08:58Z', '240.752'),
            ('2022-05-02T15:09:28Z', '70.349'),
            ('2022-05-02T15:09:58Z', '177.227'),
            ('2022-05-02T15:10:28Z', '184.439'),
            ('2022-05-02T15:10:58Z', '118.329'),
            ('2022-05-02T15:11:28Z', '8.316'),
            ('2022-05-02T15:11:58Z', '63.863'),
            ('2022-05-02T15:12:28Z', '13.639'),
            ('2022-05-02T15:12:58Z', '60.102'),
            ('2022-05-02T15:13:28Z', '47.509'),
            ('2022-05-02T15:13:58Z', '62.541'),
            ('2022-05-02T15:14:28Z', '11.976'),
            ('2022-05-02T15:14:58Z', '59.689'),
            ('2022-05-02T15:15:28Z', '8.939'),
            ('2022-05-02T15:15:58Z', '65.81');
            """;

    @Test
    public void testBindVariablePercentiles() throws Exception {
        // bind variables are runtime constants, so the factories accept them as percentiles
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT x::DOUBLE x FROM long_sequence(100))");
            final String expected = """
                    percentile_disc\tquantile_disc
                    [50.0,90.0]\t[50.0,90.0]
                    """;
            assertQuery("SELECT percentile_disc(x, ARRAY[0.5, 0.9]), quantile_disc(x, ARRAY[0.5, 0.9]) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);

            // an array built from bind variables
            bindVariableService.clear();
            bindVariableService.setDouble(0, 0.5);
            bindVariableService.setDouble(1, 0.9);
            assertQuery("SELECT percentile_disc(x, ARRAY[$1, $2]), quantile_disc(x, ARRAY[$1, $2]) FROM test")
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
                assertQuery("SELECT percentile_disc(x, $1), quantile_disc(x, $1) FROM test")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns(expected);
            }
        });
    }

    @Test
    public void testDescendingLargeBindArrayAcrossGroups() throws Exception {
        // A caller can bind thousands of percentiles in reverse order. Sorting their selected
        // indices must not do quadratic work for each group, and results retain input order.
        assertMemoryLeak(() -> {
            final int count = 2048;
            final int arrayType = ColumnType.encodeArrayType(ColumnType.DOUBLE, 1);
            try (DirectArray percentiles = new DirectArray()) {
                percentiles.setType(arrayType);
                percentiles.setDimLen(0, count);
                percentiles.applyShape();
                for (int i = 0; i < count; i++) {
                    percentiles.putDouble(i, (double) (count - i) / count);
                }
                bindVariableService.clear();
                bindVariableService.setArray(0, percentiles);
                try (RecordCursorFactory factory = select("SELECT k, percentile_disc(v, $1) disc, percentile_cont(v, $1) cont " +
                        "FROM (SELECT x::DOUBLE v, (x % 2)::INT k FROM long_sequence(4096)) GROUP BY k ORDER BY k");
                     RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    for (int k = 0; k < 2; k++) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(k, cursor.getRecord().getInt(0));
                        final ArrayView disc = cursor.getRecord().getArray(1, arrayType);
                        final ArrayView cont = cursor.getRecord().getArray(2, arrayType);
                        Assert.assertEquals(count, disc.getDimLen(0));
                        Assert.assertEquals(count, cont.getDimLen(0));
                        for (int i = 0; i < count; i++) {
                            double p = (double) (count - i) / count;
                            Assert.assertEquals(2.0 * (count - i) - (k == 1 ? 1 : 0), disc.getDouble(i), 0.0);
                            Assert.assertEquals((k == 0 ? 2 : 1) + (2.0 * count - 2) * p, cont.getDouble(i), 1e-9);
                        }
                    }
                    Assert.assertFalse(cursor.hasNext());
                }
            }
        });
    }

    @Test
    public void testInvalidPercentileInArray1() throws Exception {
        assertException(
                "SELECT percentile_disc(x::DOUBLE, ARRAY[0.98, 1.1]) FROM long_sequence(1)",
                39,
                "invalid percentile"
        );
    }

    @Test
    public void testInvalidPercentileInArray2() throws Exception {
        assertException(
                "SELECT percentile_disc(x::DOUBLE, ARRAY[0.98, -1.1]) FROM long_sequence(1)",
                39,
                "invalid percentile"
        );
    }

    @Test
    public void testInvalidPercentileInArrayMixed() throws Exception {
        assertException(
                "SELECT percentile_disc(x::DOUBLE, ARRAY[0.5, 0.95, 1.5, 0.98]) FROM long_sequence(1)",
                39,
                "invalid percentile"
        );
    }

    @Test
    public void testSlicedPercentiles() throws Exception {
        // a slice or sub-array shares the backing buffer of its parent array, starting at an
        // offset and possibly striding over it
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT x::DOUBLE x FROM long_sequence(100))");
            final String expected = """
                    p\tq
                    [50.0,90.0]\t[50.0,90.0]
                    """;
            for (String percentiles : new String[]{
                    "ARRAY[0.5, 0.9]",
                    "ARRAY[0.1, 0.5, 0.9, 0.99][2:4]",
                    "ARRAY[[0.1, 0.2], [0.5, 0.9]][2]",
                    "ARRAY[[0.1, 0.5], [0.2, 0.9]][1:, 2]",
                    // the slice leaves out the invalid percentile
                    "ARRAY[-1.0, 0.5, 0.9][2:4]"
            }) {
                assertQuery("SELECT percentile_disc(x, " + percentiles + ") p, quantile_disc(x, " + percentiles + ") q FROM test")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns(expected);
            }
            assertQuery("SELECT percentile_disc(x, ARRAY[0.1, 0.5][2:2]) p FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            p
                            []
                            """);
        });
    }

    @Test
    public void testSlicedPercentilesInvalidPercentileInSlice() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT x::DOUBLE x FROM long_sequence(100))");
            final String sql = "SELECT percentile_disc(x, ARRAY[0.5, -1.0, 0.9][2:4]) FROM test";
            assertExceptionNoLeakCheck(sql, sql.indexOf("[2:4]"), "invalid percentile [expected=range(0.0, 1.0), actual=-1.0]");
        });
    }

    @Test
    public void testPercentilesNotOneDimensional() throws Exception {
        // the result is always one-dimensional, so a deeper percentile list would lose its shape
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT x::DOUBLE x FROM long_sequence(100))");
            for (String function : new String[]{"percentile_disc", "quantile_disc"}) {
                String sql = "SELECT " + function + "(x, ARRAY[[0.1, 0.5], [0.9, 0.99]]) FROM test";
                assertExceptionNoLeakCheck(sql, sql.indexOf('['), "array is not one-dimensional");
                sql = "SELECT " + function + "(x, transpose(ARRAY[[0.1, 0.5], [0.9, 0.99]])) FROM test";
                assertExceptionNoLeakCheck(sql, sql.indexOf("transpose"), "array is not one-dimensional");
            }
        });
    }

    @Test
    public void testPercentilesNotOneDimensionalEmptyTable() throws Exception {
        // over an empty table the function never reads the percentile list, so only the
        // factory can reject it
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (x DOUBLE)");
            for (String function : new String[]{"percentile_disc", "quantile_disc"}) {
                final String sql = "SELECT " + function + "(x, ARRAY[[0.1, 0.5], [0.9, 0.99]]) FROM test";
                assertExceptionNoLeakCheck(sql, sql.indexOf('['), "array is not one-dimensional");
            }
        });
    }

    @Test
    public void testPercentilesNotOneDimensionalWeakDimsBindVariable() throws Exception {
        // PGWire declares array bind variables without their dimension count, so the factory
        // accepts them and the function checks the bound array when it computes the result
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT x::DOUBLE x FROM long_sequence(100))");
            final String sql = "SELECT percentile_disc(x, $1) FROM test";
            bindVariableService.clear();
            bindVariableService.define(0, ColumnType.encodeArrayTypeWithWeakDims(ColumnType.DOUBLE, true), 0);
            try (
                    RecordCursorFactory factory = select(sql);
                    DirectArray percentiles = new DirectArray()
            ) {
                percentiles.setType(ColumnType.encodeArrayType(ColumnType.DOUBLE, 2));
                percentiles.setDimLen(0, 2);
                percentiles.setDimLen(1, 2);
                percentiles.applyShape();
                percentiles.putDouble(0, 0.1);
                percentiles.putDouble(1, 0.5);
                percentiles.putDouble(2, 0.9);
                percentiles.putDouble(3, 0.99);
                // the bind variable pool hands the factory's variable back, as PGWire does on execute
                bindVariableService.clear();
                bindVariableService.setArray(0, percentiles);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    cursor.getRecord().getArray(0, ColumnType.encodeArrayType(ColumnType.DOUBLE, 1));
                    Assert.fail("expected 'array is not one-dimensional'");
                } catch (CairoException e) {
                    Assert.assertEquals(sql.indexOf('$'), e.getPosition());
                    TestUtils.assertContains(e.getFlyweightMessage(), "array is not one-dimensional");
                }
            }
        });
    }

    @Test
    public void testNullPercentiles() throws Exception {
        // a NULL percentile list fails like a NULL percentile does, rather than yielding []
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT x::DOUBLE x, (x % 2)::INT k FROM long_sequence(100))");
            for (String function : new String[]{"percentile_disc", "quantile_disc"}) {
                String sql = "SELECT " + function + "(x, NULL::DOUBLE[]) p FROM test";
                assertQuery(sql).noLeakCheck().fails(sql.indexOf("::"), "percentile array must not be NULL");
                sql = "SELECT k, " + function + "(x, NULL::DOUBLE[]) p FROM test";
                assertQuery(sql).noLeakCheck().fails(sql.indexOf("::"), "percentile array must not be NULL");
            }
        });
    }

    @Test
    public void testNullPercentilesBindVariable() throws Exception {
        // PGWire binds a NULL array parameter as an array bind variable without a value
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT x::DOUBLE x, (x % 2)::INT k FROM long_sequence(100))");
            final String sql = "SELECT k, percentile_disc(x, $1) p FROM test ORDER BY k";
            bindVariableService.clear();
            bindVariableService.define(0, ColumnType.encodeArrayType(ColumnType.DOUBLE, 1), 0);
            assertQuery(sql).noLeakCheck().fails(sql.indexOf('$'), "percentile array must not be NULL");

            try (DirectArray percentiles = new DirectArray()) {
                percentiles.setType(ColumnType.encodeArrayType(ColumnType.DOUBLE, 1));
                percentiles.setDimLen(0, 1);
                percentiles.applyShape();
                percentiles.putDouble(0, 0.5);
                bindVariableService.setArray(0, percentiles);
                assertQuery(sql)
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                k\tp
                                0\t[50.0]
                                1\t[49.0]
                                """);

                // PGWire re-binds the variable of the cached factory to NULL on the next execute
                try (RecordCursorFactory factory = select(sql)) {
                    bindVariableService.setArray(0, null);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        while (cursor.hasNext()) {
                            cursor.getRecord().getArray(1, ColumnType.encodeArrayType(ColumnType.DOUBLE, 1));
                        }
                        Assert.fail("expected 'percentile array must not be NULL'");
                    } catch (CairoException e) {
                        Assert.assertEquals(sql.indexOf('$'), e.getPosition());
                        TestUtils.assertContains(e.getFlyweightMessage(), "percentile array must not be NULL");
                    }
                }
            }
        });
    }

    @Test
    public void testNullPercentilesEmptyTable() throws Exception {
        // like a NULL percentile, a NULL list fails only when a group has values to compute
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (x DOUBLE)");
            execute("INSERT INTO test VALUES (null), (null)");
            for (String filter : new String[]{"WHERE false", ""}) {
                assertQuery("SELECT percentile_disc(x, NULL::DOUBLE[]) p FROM test " + filter)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                p
                                null
                                """);
            }
        });
    }

    @Test
    public void testSlicedPercentilesGroupByAndSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE test AS (
                        SELECT x::DOUBLE x, (x % 2)::INT k, timestamp_sequence('2024-01-01', 3_600_000_000L) ts
                        FROM long_sequence(100)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            for (String percentiles : new String[]{"ARRAY[0.5, 0.9]", "ARRAY[0.1, 0.5, 0.9, 0.99][2:4]"}) {
                assertQuery("SELECT k, percentile_disc(x, " + percentiles + ") p FROM test ORDER BY k")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                k\tp
                                0\t[50.0,90.0]
                                1\t[49.0,89.0]
                                """);
                assertQuery("SELECT ts, percentile_disc(x, " + percentiles + ") p FROM test SAMPLE BY 50h ALIGN TO FIRST OBSERVATION")
                        .noLeakCheck()
                        .noRandomAccess()
                        .timestamp("ts")
                        .returns("""
                                ts\tp
                                2024-01-01T00:00:00.000000Z\t[25.0,45.0]
                                2024-01-03T02:00:00.000000Z\t[75.0,95.0]
                                """);
            }
        });
    }

    @Test
    public void testSlicedPercentilesRuntimeConstant() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT x::DOUBLE x FROM long_sequence(100))");
            final String expected = """
                    p
                    [50.0,90.0]
                    """;
            assertQuery("DECLARE @p := ARRAY[0.1, 0.5, 0.9, 0.99] SELECT percentile_disc(x, @p[2:4]) p FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);

            try (DirectArray percentiles = new DirectArray()) {
                percentiles.setType(ColumnType.encodeArrayType(ColumnType.DOUBLE, 1));
                percentiles.setDimLen(0, 4);
                percentiles.applyShape();
                percentiles.putDouble(0, 0.1);
                percentiles.putDouble(1, 0.5);
                percentiles.putDouble(2, 0.9);
                percentiles.putDouble(3, 0.99);
                bindVariableService.clear();
                bindVariableService.setArray(0, percentiles);
                assertQuery("SELECT percentile_disc(x, ($1)[2:4]) p FROM test")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns(expected);
            }
        });
    }

    @Test
    public void testNegativePercentileInArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT x::DOUBLE x FROM long_sequence(100))");
            final String sql = "SELECT percentile_disc(x, ARRAY[0.5, -0.25]) FROM test";
            assertExceptionNoLeakCheck(sql, sql.indexOf('['), "invalid percentile [expected=range(0.0, 1.0), actual=-0.25]");
        });
    }

    @Test
    public void testPercentilesAtWholeNumberProduct() throws Exception {
        // 0.07 * 100 evaluates to 7.000000000000001; the 7th value's cumulative distribution,
        // 7 / 100, already reaches 0.07, so the result is 7, not 8
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT x::DOUBLE x FROM long_sequence(100))");
            assertQuery("SELECT percentile_disc(x, ARRAY[0.07, 0.14, 0.28, 0.55, 0.56]) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            percentile_disc
                            [7.0,14.0,28.0,55.0,56.0]
                            """);
        });
    }

    @Test
    public void testEmptyGroupSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT x::double AS val, timestamp_sequence('2024-01-01T00:00:00', 3600000000L) ts FROM long_sequence(3)) TIMESTAMP(ts)");
            assertQuery("SELECT percentile_disc(val, ARRAY[0.25, 0.75]::DOUBLE[]) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("percentile_disc\n" +
                            "[1.0,3.0]\n");
        });
    }

    @Test
    public void testMultiPercentileDiscGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (" +
                    "SELECT x % 2 AS category, cast(x AS DOUBLE) AS value FROM long_sequence(10)" +
                    ")");
            assertQuery("SELECT category, percentile_disc(value, ARRAY[0.25, 0.5, 0.75]) FROM test GROUP BY category ORDER BY category")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            category\tpercentile_disc
                            0\t[4.0,6.0,8.0]
                            1\t[3.0,5.0,7.0]
                            """);
        });
    }

    @Test
    public void testNullGroupBeforeNonNullGroup() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (grp SYMBOL, val DOUBLE)");
            execute("INSERT INTO test VALUES ('a', null), ('b', 1.0), ('b', 2.0), ('b', 3.0)");
            assertQuery("SELECT grp, percentile_disc(val, ARRAY[0.25, 0.75]::DOUBLE[]) FROM test GROUP BY grp ORDER BY grp")
                    .noLeakCheck()
                    .expectSize()
                    .returns("grp\tpercentile_disc\n" +
                            "a\tnull\n" +
                            "b\t[1.0,3.0]\n");
        });
    }

    @Test
    public void testMultiPercentileDiscGroupByWithAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (" +
                    "SELECT 1 AS category, null::DOUBLE AS value FROM long_sequence(5)" +
                    ")");
            assertQuery("SELECT category, percentile_disc(value, ARRAY[0.25, 0.5, 0.75]) FROM test GROUP BY category")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            category\tpercentile_disc
                            1\tnull
                            """);
        });
    }

    @Test
    public void testMultiPercentileDiscGroupByWithExtremePercentiles() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (" +
                    "SELECT x % 2 AS category, cast(x AS DOUBLE) AS value FROM long_sequence(10)" +
                    ")");
            // cat=0: 2, 4, 6, 8, 10 → [0, 0.5, 1.0] → [2.0, 6.0, 10.0]
            // cat=1: 1, 3, 5, 7, 9 → [0, 0.5, 1.0] → [1.0, 5.0, 9.0]
            assertQuery("SELECT category, percentile_disc(value, ARRAY[0.0, 0.5, 1.0]) FROM test GROUP BY category ORDER BY category")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            category\tpercentile_disc
                            0\t[2.0,6.0,10.0]
                            1\t[1.0,5.0,9.0]
                            """);
        });
    }

    @Test
    public void testMultiPercentileDiscGroupByWithMultipleColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (" +
                    "SELECT x % 2 AS cat1, x % 3 AS cat2, cast(x AS DOUBLE) AS value " +
                    "FROM long_sequence(12)" +
                    ")");
            // cat1=0, cat2=0: values 6, 12 (2 values)
            //   25th: ceil(2*0.25)-1 = 0 → 6.0
            //   50th: ceil(2*0.50)-1 = 0 → 6.0
            //   75th: ceil(2*0.75)-1 = 1 → 12.0
            // cat1=0, cat2=1: values 4, 10 (2 values)
            //   25th: ceil(2*0.25)-1 = 0 → 4.0
            //   50th: ceil(2*0.50)-1 = 0 → 4.0
            //   75th: ceil(2*0.75)-1 = 1 → 10.0
            // cat1=0, cat2=2: values 2, 8 (2 values)
            //   25th: ceil(2*0.25)-1 = 0 → 2.0
            //   50th: ceil(2*0.50)-1 = 0 → 2.0
            //   75th: ceil(2*0.75)-1 = 1 → 8.0
            // cat1=1, cat2=0: values 3, 9 (2 values)
            //   25th: ceil(2*0.25)-1 = 0 → 3.0
            //   50th: ceil(2*0.50)-1 = 0 → 3.0
            //   75th: ceil(2*0.75)-1 = 1 → 9.0
            // cat1=1, cat2=1: values 1, 7 (2 values)
            //   25th: ceil(2*0.25)-1 = 0 → 1.0
            //   50th: ceil(2*0.50)-1 = 0 → 1.0
            //   75th: ceil(2*0.75)-1 = 1 → 7.0
            // cat1=1, cat2=2: values 5, 11 (2 values)
            //   25th: ceil(2*0.25)-1 = 0 → 5.0
            //   50th: ceil(2*0.50)-1 = 0 → 5.0
            //   75th: ceil(2*0.75)-1 = 1 → 11.0
            assertQuery("SELECT cat1, cat2, percentile_disc(value, ARRAY[0.25, 0.5, 0.75]) " +
                    "FROM test GROUP BY cat1, cat2 ORDER BY cat1, cat2")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            cat1\tcat2\tpercentile_disc
                            0\t0\t[6.0,6.0,12.0]
                            0\t1\t[4.0,4.0,10.0]
                            0\t2\t[2.0,2.0,8.0]
                            1\t0\t[3.0,3.0,9.0]
                            1\t1\t[1.0,1.0,7.0]
                            1\t2\t[5.0,5.0,11.0]
                            """);
        });
    }

    @Test
    public void testMultiPercentileDiscGroupByWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (" +
                    "SELECT x % 2 AS category, " +
                    "CASE WHEN x % 4 = 0 THEN null ELSE cast(x AS DOUBLE) END AS value " +
                    "FROM long_sequence(10)" +
                    ")");
            // cat=0: 2, null, 6, null, 10 → non-null: 2, 6, 10 (3 values)
            //   25th: ceil(3*0.25)-1 = 0 → 2.0
            //   50th: ceil(3*0.50)-1 = 1 → 6.0
            //   75th: ceil(3*0.75)-1 = 2 → 10.0
            // cat=1: 1, 3, 5, 7, 9 (5 values)
            //   25th: ceil(5*0.25)-1 = 1 → 3.0
            //   50th: ceil(5*0.50)-1 = 2 → 5.0
            //   75th: ceil(5*0.75)-1 = 3 → 7.0
            assertQuery("SELECT category, percentile_disc(value, ARRAY[0.25, 0.5, 0.75]) FROM test GROUP BY category ORDER BY category")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            category\tpercentile_disc
                            0\t[2.0,6.0,10.0]
                            1\t[3.0,5.0,7.0]
                            """);
        });
    }

    @Test
    public void testMultiPercentileDiscGroupByWithSingleElement() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (" +
                    "SELECT x, cast(x AS DOUBLE) AS value FROM long_sequence(5)" +
                    ")");
            // Each group has one value, all percentiles should return that value
            assertQuery("SELECT x, percentile_disc(value, ARRAY[0.25, 0.5, 0.75]) FROM test GROUP BY x ORDER BY x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x\tpercentile_disc
                            1\t[1.0,1.0,1.0]
                            2\t[2.0,2.0,2.0]
                            3\t[3.0,3.0,3.0]
                            4\t[4.0,4.0,4.0]
                            5\t[5.0,5.0,5.0]
                            """);
        });
    }

    @Test
    public void testMultiPercentileDiscWithFineGrainedPercentiles() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT cast(x AS DOUBLE) value FROM long_sequence(100))");
            // Test with percentiles that have more decimal places
            final String expected = """
                    p
                    [10.0,25.0,34.0,50.0,67.0,75.0,90.0]
                    """;
            assertQuery("SELECT percentile_disc(value, ARRAY[0.1, 0.25, 0.333, 0.5, 0.667, 0.75, 0.9]) p FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);
            // the window function computes over all rows; LIMIT 1 trims only the output
            assertQuery("SELECT percentile_disc(value, ARRAY[0.1, 0.25, 0.333, 0.5, 0.667, 0.75, 0.9]) OVER () p FROM test LIMIT 1")
                    .noLeakCheck()
                    .returns(expected);
        });
    }

    @Test
    public void testMultiPercentileDiscWithNegativeValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT cast(x - 6 AS DOUBLE) value FROM long_sequence(10))");
            // Values: -5, -4, -3, -2, -1, 0, 1, 2, 3, 4
            // 25th: ceil(10*0.25)-1 = 2 → -3.0
            // 50th: ceil(10*0.50)-1 = 4 → -1.0
            // 75th: ceil(10*0.75)-1 = 7 → 2.0
            assertQuery("SELECT value, percentile_disc(value, ARRAY[0.25, 0.5, 0.75]) OVER () FROM test")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            value\tpercentile_disc
                            -5.0\t[-3.0,-1.0,2.0]
                            -4.0\t[-3.0,-1.0,2.0]
                            -3.0\t[-3.0,-1.0,2.0]
                            -2.0\t[-3.0,-1.0,2.0]
                            -1.0\t[-3.0,-1.0,2.0]
                            0.0\t[-3.0,-1.0,2.0]
                            1.0\t[-3.0,-1.0,2.0]
                            2.0\t[-3.0,-1.0,2.0]
                            3.0\t[-3.0,-1.0,2.0]
                            4.0\t[-3.0,-1.0,2.0]
                            """);
        });
    }

    @Test
    public void testMultiPercentileDiscWithSinglePercentileInArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT cast(x AS DOUBLE) value FROM long_sequence(10))");
            // Single element array should work like scalar version but return array
            assertQuery("SELECT value, percentile_disc(value, ARRAY[0.5]) OVER () FROM test")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            value\tpercentile_disc
                            1.0\t[5.0]
                            2.0\t[5.0]
                            3.0\t[5.0]
                            4.0\t[5.0]
                            5.0\t[5.0]
                            6.0\t[5.0]
                            7.0\t[5.0]
                            8.0\t[5.0]
                            9.0\t[5.0]
                            10.0\t[5.0]
                            """);
        });
    }

    @Test
    public void testMultiPercentileDiscWithVeryLargeArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT cast(x AS DOUBLE) value FROM long_sequence(10))");
            // Test with 20 percentiles
            final String percentiles = "ARRAY[" +
                    "0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5, " +
                    "0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1.0" +
                    "]";
            final String expected = """
                    p
                    [1.0,1.0,2.0,2.0,3.0,3.0,4.0,4.0,5.0,5.0,6.0,6.0,7.0,7.0,8.0,8.0,9.0,9.0,10.0,10.0]
                    """;
            assertQuery("SELECT percentile_disc(value, " + percentiles + ") p FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);
            // the window function computes over all rows; LIMIT 1 trims only the output
            assertQuery("SELECT percentile_disc(value, " + percentiles + ") OVER () p FROM test LIMIT 1")
                    .noLeakCheck()
                    .returns(expected);
        });
    }

    @Test
    public void testMultiplePercentilesAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (x LONG)");
            execute("INSERT INTO test VALUES (null), (null), (null)");
            assertQuery("SELECT percentile_disc(x, ARRAY[0.98, 0.95, 0.90, 0.50]) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            percentile_disc
                            null
                            """);
        });
    }

    @Test
    public void testMultiplePercentilesAllSameValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT 5.0 x FROM long_sequence(100))");
            assertQuery("SELECT percentile_disc(x, ARRAY[0.98, 0.95, 0.90, 0.50]) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("percentile_disc\n[5.0,5.0,5.0,5.0]\n");
        });
    }

    @Test
    public void testMultiplePercentilesAllZeros() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT 0.0 x FROM long_sequence(100))");
            assertQuery("SELECT percentile_disc(x, ARRAY[0.98, 0.95, 0.90, 0.50]) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("percentile_disc\n[0.0,0.0,0.0,0.0]\n");
        });
    }

    @Test
    public void testMultiplePercentilesDoubleValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT cast(x AS DOUBLE) x FROM long_sequence(100))");
            assertQuery("SELECT percentile_disc(x, ARRAY[0.98, 0.95, 0.90, 0.50]) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("percentile_disc\n[98.0,95.0,90.0,50.0]\n");
        });
    }

    @Test
    public void testMultiplePercentilesEmptyArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT cast(x AS DOUBLE) x FROM long_sequence(100))");
            assertQuery("SELECT percentile_disc(x, ARRAY[]) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("percentile_disc\n[]\n");
        });
    }

    @Test
    public void testMultiplePercentilesEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (x DOUBLE)");
            assertQuery("SELECT percentile_disc(x, ARRAY[0.98, 0.95, 0.90, 0.50]) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            percentile_disc
                            null
                            """);
        });
    }

    @Test
    public void testMultiplePercentilesFloatValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT cast(x AS FLOAT) x FROM long_sequence(100))");
            assertQuery("SELECT percentile_disc(x, ARRAY[0.98, 0.95, 0.90, 0.50]) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("percentile_disc\n[98.0,95.0,90.0,50.0]\n");
        });
    }

    @Test
    public void testMultiplePercentilesIncludingEdgeCases() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT cast(x AS DOUBLE) x FROM long_sequence(100))");
            assertQuery("SELECT percentile_disc(x, ARRAY[1.0, 0.98, 0.0, 0.50]) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("percentile_disc\n[100.0,98.0,1.0,50.0]\n");
        });
    }

    @Test
    public void testMultiplePercentilesLargeDataset() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT cast(x AS DOUBLE) x FROM long_sequence(10_000))");
            assertQuery("SELECT percentile_disc(x, ARRAY[0.98, 0.95, 0.90, 0.50]) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("percentile_disc\n[9800.0,9500.0,9000.0,5000.0]\n");
        });
    }

    @Test
    public void testMultiplePercentilesOrdering() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (x DOUBLE)");
            execute("INSERT INTO test VALUES (5.0), (1.0), (10.0), (3.0), (7.0)");
            // Sorted: 1, 3, 5, 7, 10
            // 0.98 -> ceil(5 * 0.98) - 1 = 4 -> 10.0
            // 0.95 -> ceil(5 * 0.95) - 1 = 4 -> 10.0
            // 0.90 -> ceil(5 * 0.90) - 1 = 4 -> 10.0
            // 0.50 -> ceil(5 * 0.50) - 1 = 2 -> 5.0
            assertQuery("SELECT percentile_disc(x, ARRAY[0.98, 0.95, 0.90, 0.50]) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            percentile_disc
                            [10.0,10.0,10.0,5.0]
                            """);
        });
    }

    @Test
    public void testMultiplePercentilesSinglePercentile() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT cast(x AS DOUBLE) x FROM long_sequence(100))");
            assertQuery("SELECT percentile_disc(x, ARRAY[0.50]) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("percentile_disc\n[50.0]\n");
        });
    }

    @Test
    public void testMultiplePercentilesSingleValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (x DOUBLE)");
            execute("INSERT INTO test VALUES (42.0)");
            assertQuery("SELECT percentile_disc(x, ARRAY[0.98, 0.95, 0.90, 0.50]) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            percentile_disc
                            [42.0,42.0,42.0,42.0]
                            """);
        });
    }

    @Test
    public void testMultiplePercentilesSomeNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (x DOUBLE)");
            execute("INSERT INTO test VALUES (1.0), (null), (null), (null)");
            assertQuery("SELECT percentile_disc(x, ARRAY[0.98, 0.95, 0.90, 0.50]) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            percentile_disc
                            [1.0,1.0,1.0,1.0]
                            """);
        });
    }

    @Test
    public void testMultiplePercentilesTwoValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (x DOUBLE)");
            execute("INSERT INTO test VALUES (10.0), (20.0)");
            assertQuery("SELECT percentile_disc(x, ARRAY[0.98, 0.95, 0.90, 0.50]) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            percentile_disc
                            [20.0,20.0,20.0,10.0]
                            """);
        });
    }

    @Test
    public void testMultiplePercentilesWithDuplicates() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (x DOUBLE)");
            execute("INSERT INTO test VALUES (1.0), (1.0), (2.0), (2.0), (3.0), (3.0)");
            assertQuery("SELECT percentile_disc(x, ARRAY[0.98, 0.95, 0.90, 0.50]) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            percentile_disc
                            [3.0,3.0,3.0,2.0]
                            """);
        });
    }

    @Test
    public void testMultiplePercentilesWithGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (category SYMBOL, value DOUBLE)");
            execute("INSERT INTO test VALUES ('A', 1.0), ('A', 2.0), ('A', 3.0), ('B', 10.0), ('B', 20.0), ('B', 30.0)");
            assertQuery("SELECT category, percentile_disc(value, ARRAY[0.98, 0.95, 0.90, 0.50]) FROM test ORDER BY category")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            category\tpercentile_disc
                            A\t[3.0,3.0,3.0,2.0]
                            B\t[30.0,30.0,30.0,20.0]
                            """);
        });
    }

    @Test
    public void testMultiplePercentilesWithKnownData() throws Exception {
        assertMemoryLeak(() -> {
            execute(txDdl);
            execute(txDml);
            assertQuery("SELECT percentile_disc(value, ARRAY[0.98, 0.95, 0.90, 0.50]) FROM tx_traffic")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            percentile_disc
                            [406.977,289.615,224.195,63.863]
                            """);
        });
    }

    @Test
    public void testMultiplePercentilesWithNegativeValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (x DOUBLE)");
            execute("INSERT INTO test VALUES (1.0), (-1.0), (5.0), (-5.0), (10.0), (-10.0)");
            assertQuery("SELECT percentile_disc(x, ARRAY[0.98, 0.95, 0.90, 0.50]) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            percentile_disc
                            [10.0,10.0,10.0,-1.0]
                            """);
        });
    }

    @Test
    public void testMultiplePercentilesWithPrecision() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT cast(x AS DOUBLE) x FROM long_sequence(1000))");
            assertQuery("SELECT percentile_disc(x, ARRAY[0.98, 0.95, 0.90, 0.50]) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("percentile_disc\n[980.0,950.0,900.0,500.0]\n");
        });
    }

    @Test
    public void testNonConstantPercentile() throws Exception {
        // getArray() evaluates the percentiles against the GROUP BY output record, which holds
        // no input columns, so percentiles that vary per row have no meaningful value
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (x DOUBLE, arr DOUBLE[])");
            assertQuery("SELECT percentile_disc(x, arr) FROM test")
                    .noLeakCheck()
                    .fails(26, "percentile must be a constant or runtime constant");
            assertQuery("SELECT percentile_disc(x, ARRAY[x / 100]) FROM test")
                    .noLeakCheck()
                    .fails(31, "percentile must be a constant or runtime constant");
            assertQuery("SELECT quantile_disc(x, arr) FROM test")
                    .noLeakCheck()
                    .fails(24, "percentile must be a constant or runtime constant");
        });
    }

    @Test
    public void testParallelGroupBy() throws Exception {
        // Small page frames spread the rows over 40 frames and only every tenth frame holds values,
        // so the workers regularly hand merge() an empty partial result on either side.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 100);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(pool, (engine, _, sqlExecutionContext) -> {
                engine.execute("""
                        CREATE TABLE tab AS (
                            SELECT CASE WHEN ((x - 1) / 100) % 10 = 0 THEN x::DOUBLE END v, (x % 4)::INT k
                            FROM long_sequence(4_000)
                        )""", sqlExecutionContext);
                assertQuery("SELECT percentile_disc(v, ARRAY[0.25, 0.5, 0.75]) p FROM tab")
                        .withEngine(engine)
                        .withContext(sqlExecutionContext)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .withPlanContaining("Async Group By workers: 4")
                        .returns("""
                                p
                                [100.0,1100.0,2100.0]
                                """);
                assertQuery("SELECT k, percentile_disc(v, ARRAY[0.25, 0.5, 0.75]) p FROM tab ORDER BY k")
                        .withEngine(engine)
                        .withContext(sqlExecutionContext)
                        .noLeakCheck()
                        .expectSize()
                        .withPlanContaining("Async Group By workers: 4")
                        .returns("""
                                k\tp
                                0\t[100.0,1100.0,2100.0]
                                1\t[97.0,1097.0,2097.0]
                                2\t[98.0,1098.0,2098.0]
                                3\t[99.0,1099.0,2099.0]
                                """);
            }, configuration, LOG);
        });
    }

    @Test
    public void testParallelGroupByNullPercentiles() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 100);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(pool, (engine, _, sqlExecutionContext) -> {
                engine.execute("CREATE TABLE tab AS (SELECT x::DOUBLE v, (x % 2)::INT k FROM long_sequence(1_000))", sqlExecutionContext);
                for (String sql : new String[]{
                        "SELECT percentile_disc(v, NULL::DOUBLE[]) p FROM tab",
                        "SELECT k, percentile_disc(v, NULL::DOUBLE[]) p FROM tab"
                }) {
                    assertQuery(sql)
                            .withEngine(engine)
                            .withContext(sqlExecutionContext)
                            .noLeakCheck()
                            .assertsPlanContaining("Async Group By workers: 4");
                    assertQuery(sql)
                            .withEngine(engine)
                            .withContext(sqlExecutionContext)
                            .noLeakCheck()
                            .fails(sql.indexOf("::"), "percentile array must not be NULL");
                }
            }, configuration, LOG);
        });
    }

    @Test
    public void testParallelGroupBySlicedPercentiles() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 100);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(pool, (engine, _, sqlExecutionContext) -> {
                engine.execute("CREATE TABLE tab AS (SELECT x::DOUBLE v, (x % 2)::INT k FROM long_sequence(1_000))", sqlExecutionContext);
                final String expected = """
                        k\tp
                        0\t[500.0,900.0]
                        1\t[499.0,899.0]
                        """;
                for (String percentiles : new String[]{"ARRAY[0.5, 0.9]", "ARRAY[0.1, 0.5, 0.9, 0.99][2:4]"}) {
                    assertQuery("SELECT k, percentile_disc(v, " + percentiles + ") p FROM tab ORDER BY k")
                            .withEngine(engine)
                            .withContext(sqlExecutionContext)
                            .noLeakCheck()
                            .expectSize()
                            .withPlanContaining("Async Group By workers: 4")
                            .returns(expected);
                }
            }, configuration, LOG);
        });
    }

    @Test
    public void testSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT x::DOUBLE AS val, timestamp_sequence('2024-01-01T00:00:00', 3600000000L) ts FROM long_sequence(5)) TIMESTAMP(ts)");
            assertQuery("SELECT ts, percentile_disc(val, ARRAY[0.5]) FROM test SAMPLE BY 1h")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("ts\tpercentile_disc\n" +
                            "2024-01-01T00:00:00.000000Z\t[1.0]\n" +
                            "2024-01-01T01:00:00.000000Z\t[2.0]\n" +
                            "2024-01-01T02:00:00.000000Z\t[3.0]\n" +
                            "2024-01-01T03:00:00.000000Z\t[4.0]\n" +
                            "2024-01-01T04:00:00.000000Z\t[5.0]\n");
        });
    }

    @Test
    public void testSampleByFill() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (k SYMBOL, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                    ('a', 1.0, '2024-01-01T00:00:00.000000Z'),
                    ('a', 3.0, '2024-01-01T00:20:00.000000Z'),
                    ('b', 10.0, '2024-01-01T00:40:00.000000Z'),
                    ('a', 5.0, '2024-01-01T03:00:00.000000Z'),
                    ('b', null, '2024-01-01T03:30:00.000000Z'),
                    ('c', 7.0, '2024-01-01T03:45:00.000000Z')
                    """);
            assertQuery("SELECT ts, percentile_disc(v, ARRAY[0.25, 0.5]) p FROM t SAMPLE BY 1h FILL(NULL)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("ts")
                    .returns("""
                            ts\tp
                            2024-01-01T00:00:00.000000Z\t[1.0,3.0]
                            2024-01-01T01:00:00.000000Z\tnull
                            2024-01-01T02:00:00.000000Z\tnull
                            2024-01-01T03:00:00.000000Z\t[5.0,5.0]
                            """);
            assertQuery("SELECT ts, percentile_disc(v, ARRAY[0.25, 0.5]) p FROM t SAMPLE BY 1h FILL(PREV)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("ts")
                    .returns("""
                            ts\tp
                            2024-01-01T00:00:00.000000Z\t[1.0,3.0]
                            2024-01-01T01:00:00.000000Z\t[1.0,3.0]
                            2024-01-01T02:00:00.000000Z\t[1.0,3.0]
                            2024-01-01T03:00:00.000000Z\t[5.0,5.0]
                            """);
            // ALIGN TO FIRST OBSERVATION runs the keyed fill cursors, which seed every key with setNull():
            // 'c' has no value until 03:00 and must stay null rather than borrow another key's list
            assertQuery("SELECT ts, k, percentile_disc(v, ARRAY[0.25, 0.5]) p FROM t SAMPLE BY 1h FILL(PREV) ALIGN TO FIRST OBSERVATION")
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("ts")
                    .returns("""
                            ts\tk\tp
                            2024-01-01T00:00:00.000000Z\ta\t[1.0,1.0]
                            2024-01-01T00:00:00.000000Z\tb\t[10.0,10.0]
                            2024-01-01T00:00:00.000000Z\tc\tnull
                            2024-01-01T01:00:00.000000Z\ta\t[1.0,1.0]
                            2024-01-01T01:00:00.000000Z\tb\t[10.0,10.0]
                            2024-01-01T01:00:00.000000Z\tc\tnull
                            2024-01-01T02:00:00.000000Z\ta\t[1.0,1.0]
                            2024-01-01T02:00:00.000000Z\tb\t[10.0,10.0]
                            2024-01-01T02:00:00.000000Z\tc\tnull
                            2024-01-01T03:00:00.000000Z\ta\t[5.0,5.0]
                            2024-01-01T03:00:00.000000Z\tb\tnull
                            2024-01-01T03:00:00.000000Z\tc\t[7.0,7.0]
                            """);
        });
    }

    @Test
    public void testSampleByFillUnsupportedModes() throws Exception {
        // an array result can neither be interpolated nor filled with a scalar constant
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            final String linearSql = "SELECT ts, percentile_disc(v, ARRAY[0.5]) FROM t SAMPLE BY 1h FILL(LINEAR)";
            assertExceptionNoLeakCheck(linearSql, linearSql.indexOf("percentile_disc"), "Unsupported interpolation type: DOUBLE[]");
            final String valueSql = "SELECT ts, percentile_disc(v, ARRAY[0.5]) FROM t SAMPLE BY 1h FILL(42)";
            assertExceptionNoLeakCheck(valueSql, valueSql.indexOf("42"), "fill value of type INT cannot fill column of type DOUBLE[]");
        });
    }

}
