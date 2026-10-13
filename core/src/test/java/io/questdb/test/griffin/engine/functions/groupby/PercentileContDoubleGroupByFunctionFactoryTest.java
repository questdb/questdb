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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class PercentileContDoubleGroupByFunctionFactoryTest extends AbstractCairoTest {

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
    public void test0thPercentileDoubleValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT cast(x AS DOUBLE) x FROM long_sequence(100))");
            assertQuery("SELECT percentile_cont(x, 0) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("percentile_cont\n1.0\n");
        });
    }

    @Test
    public void test100thPercentileDoubleValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT cast(x AS DOUBLE) x FROM long_sequence(100))");
            assertQuery("SELECT percentile_cont(x, 1.0) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("percentile_cont\n100.0\n");
        });
    }

    @Test
    public void test25thPercentileInterpolation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (x DOUBLE)");
            execute("INSERT INTO test VALUES (1.0), (2.0), (3.0), (4.0)");
            assertQuery("SELECT percentile_cont(x, 0.25) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("percentile_cont\n1.75\n");
        });
    }

    @Test
    public void test50thPercentileDoubleValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT cast(x AS DOUBLE) x FROM long_sequence(100))");
            assertQuery("SELECT percentile_cont(x, 0.5) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("percentile_cont\n50.5\n");
        });
    }

    @Test
    public void test50thPercentileFloatValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT cast(x AS FLOAT) x FROM long_sequence(100))");
            assertQuery("SELECT percentile_cont(x, 0.5) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("percentile_cont\n50.5\n");
        });
    }

    @Test
    public void test50thPercentileWithPrecision() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT cast(x AS DOUBLE) x FROM long_sequence(1000))");
            assertQuery("SELECT percentile_cont(x, 0.5) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("percentile_cont\n500.5\n");
        });
    }

    @Test
    public void test75thPercentileInterpolation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (x DOUBLE)");
            execute("INSERT INTO test VALUES (1.0), (2.0), (3.0), (4.0)");
            assertQuery("SELECT percentile_cont(x, 0.75) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            percentile_cont
                            3.25
                            """);
        });
    }

    @Test
    public void testCompareDiscVsCont() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (x DOUBLE)");
            execute("INSERT INTO test VALUES (1.0), (2.0), (3.0), (4.0), (5.0)");
            assertQuery("SELECT percentile_cont(x, 0.5), percentile_disc(x, 0.5) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            percentile_cont\tpercentile_disc
                            3.0\t3.0
                            """);
        });
    }

    @Test
    public void testCompareDiscVsContInterpolation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (x DOUBLE)");
            execute("INSERT INTO test VALUES (1.0), (2.0), (3.0), (4.0)");
            // For 4 values (indices 0-3):
            // percentile_cont(0.5): position = 0.5 * 3 = 1.5, interpolates between index 1 (2.0) and 2 (3.0) = 2.5
            // percentile_disc(0.5): ceil(4 * 0.5) - 1 = 1, returns value at index 1 = 2.0
            assertQuery("SELECT percentile_cont(x, 0.5), percentile_disc(x, 0.5) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            percentile_cont\tpercentile_disc
                            2.5\t2.0
                            """);
        });
    }

    @Test
    public void testInterpolationWithTwoValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (x DOUBLE)");
            execute("INSERT INTO test VALUES (10.0), (20.0)");
            assertQuery("SELECT percentile_cont(x, 0.5) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("percentile_cont\n15.0\n");
        });
    }

    @Test
    public void testInvalidPercentile1() throws Exception {
        assertException(
                "SELECT percentile_cont(x::DOUBLE, 1.1) FROM long_sequence(1)",
                34,
                "invalid percentile"
        );
    }

    @Test
    public void testInvalidPercentile2() throws Exception {
        assertException(
                "SELECT percentile_cont(x::DOUBLE, -1.1) FROM long_sequence(1)",
                34,
                "invalid percentile"
        );
    }

    @Test
    public void testInterpolationBetweenExtremes() throws Exception {
        // hi - lo overflows for these two values, but every interpolated value lies between them.
        // DuckDB returns the same three values; p75 is one ulp below max / 2 because
        // 0.75 * (hi - lo) rounds.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (x DOUBLE)");
            execute("INSERT INTO test VALUES (1.7976931348623157E308), (-1.7976931348623157E308)");
            assertQuery("SELECT percentile_cont(x, 0.5) p50, percentile_cont(x, 0.25) p25, percentile_cont(x, 0.75) p75 FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            p50\tp25\tp75
                            0.0\t-8.988465674311579E307\t8.988465674311578E307
                            """);
        });
    }

    @Test
    public void testNegativePercentile() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT cast(x AS DOUBLE) x FROM long_sequence(100))");
            assertExceptionNoLeakCheck(
                    "SELECT percentile_cont(x, -0.5) FROM test",
                    26,
                    "invalid percentile [expected=range(0.0, 1.0), actual=-0.5]"
            );
            assertExceptionNoLeakCheck(
                    "SELECT percentile_cont(x, -1.0) FROM test",
                    26,
                    "invalid percentile [expected=range(0.0, 1.0), actual=-1.0]"
            );
        });
    }

    @Test
    public void testNegativeZeroPercentile() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT cast(x AS DOUBLE) x FROM long_sequence(100))");
            assertQuery("SELECT percentile_cont(x, -0.0) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("percentile_cont\n1.0\n");
        });
    }

    @Test
    public void testNegativeValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (x DOUBLE)");
            execute("INSERT INTO test VALUES (1.0), (-1.0)");
            assertQuery("SELECT percentile_cont(x, 0.5) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            percentile_cont
                            0.0
                            """);
        });
    }

    @Test
    public void testNonConstantPercentile() throws Exception {
        // getDouble() evaluates the percentile against the GROUP BY output record, which holds
        // no input columns, so a percentile that varies per row has no meaningful value
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (x DOUBLE, p DOUBLE)");
            assertQuery("SELECT percentile_cont(x, p) FROM test")
                    .noLeakCheck()
                    .fails(26, "percentile must be a constant or runtime constant");
            assertQuery("SELECT quantile_cont(x, p) FROM test")
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
                assertQuery("SELECT percentile_cont(v, 0.25) p25, percentile_cont(v, 0.5) p50, percentile_cont(v, 0.75) p75 FROM tab")
                        .withEngine(engine)
                        .withContext(sqlExecutionContext)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .withPlanContaining("Async Group By workers: 4")
                        .returns("""
                                p25\tp50\tp75
                                775.75\t1550.5\t2325.25
                                """);
                assertQuery("SELECT k, percentile_cont(v, 0.25) p25, percentile_cont(v, 0.5) p50, percentile_cont(v, 0.75) p75 FROM tab ORDER BY k")
                        .withEngine(engine)
                        .withContext(sqlExecutionContext)
                        .noLeakCheck()
                        .expectSize()
                        .withPlanContaining("Async Group By workers: 4")
                        .returns("""
                                k\tp25\tp50\tp75
                                0\t778.0\t1552.0\t2326.0
                                1\t775.0\t1549.0\t2323.0
                                2\t776.0\t1550.0\t2324.0
                                3\t777.0\t1551.0\t2325.0
                                """);
            }, configuration, LOG);
        });
    }

    @Test
    public void testPercentileAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (x LONG)");
            execute("INSERT INTO test VALUES (null), (null), (null)");
            assertQuery("SELECT percentile_cont(x, 0.5) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            percentile_cont
                            null
                            """);
        });
    }

    @Test
    public void testPercentileAllSameValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT 5.0 x FROM long_sequence(100))");
            assertQuery("SELECT percentile_cont(x, 0.5) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("percentile_cont\n5.0\n");
        });
    }

    @Test
    public void testPercentileContGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (" +
                    "SELECT x % 2 AS category, cast(x AS DOUBLE) AS value FROM long_sequence(10)" +
                    ")");
            assertQuery("SELECT category, percentile_cont(value, 0.5) FROM test GROUP BY category ORDER BY category")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            category\tpercentile_cont
                            0\t6.0
                            1\t5.0
                            """);
        });
    }

    @Test
    public void testPercentileContGroupByWithInterpolation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (" +
                    "SELECT x % 2 AS category, cast(x AS DOUBLE) AS value FROM long_sequence(10)" +
                    ")");
            // cat=0: 2, 4, 6, 8, 10 → 75th percentile position = 0.75 * 4 = 3.0 → index 3 → 8.0
            // cat=1: 1, 3, 5, 7, 9 → 75th percentile position = 0.75 * 4 = 3.0 → index 3 → 7.0
            assertQuery("SELECT category, percentile_cont(value, 0.75) FROM test GROUP BY category ORDER BY category")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            category\tpercentile_cont
                            0\t8.0
                            1\t7.0
                            """);
        });
    }

    @Test
    public void testPercentileEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (x DOUBLE)");
            assertQuery("SELECT percentile_cont(x, 0.5) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            percentile_cont
                            null
                            """);
        });
    }

    @Test
    public void testPercentileSomeNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (x DOUBLE)");
            execute("INSERT INTO test VALUES (1.0), (null), (null), (null)");
            assertQuery("SELECT percentile_cont(x, 0.5) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            percentile_cont
                            1.0
                            """);
        });
    }

    @Test
    public void testPercentileWithPercentileBindVariable() throws Exception {
        bindVariableService.setDouble(0, 0.25);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT x::DOUBLE x FROM long_sequence(100))");
            assertQuery("SELECT percentile_cont(x, $1) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("percentile_cont\n25.75\n");
        });
    }

    @Test
    public void testPercentileWithUntypedBindVariable() throws Exception {
        // An untyped percentile bind variable matches the DOUBLE and DOUBLE[] overloads
        // at the same distance. function_list.txt pins the DOUBLE overload first, so the
        // compiler types the bind variable as DOUBLE rather than DOUBLE[].
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT x::DOUBLE x FROM long_sequence(10))");
            bindVariableService.clear();
            try (RecordCursorFactory factory = select("SELECT percentile_cont(x, $1), quantile_cont(x, $2) FROM test")) {
                Assert.assertEquals(ColumnType.DOUBLE, bindVariableService.getFunction(0).getType());
                Assert.assertEquals(ColumnType.DOUBLE, bindVariableService.getFunction(1).getType());
                bindVariableService.setDouble(0, 0.5);
                bindVariableService.setDouble(1, 0.25);
                assertFactory(factory)
                        .withContext(sqlExecutionContext)
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                percentile_cont\tquantile_cont
                                5.5\t3.25
                                """);
            }
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
            // 00:00 interpolates between 1.0 and 3.0, 03:00 between 5.0 and 7.0
            assertQuery("SELECT ts, percentile_cont(v, 0.25) p FROM t SAMPLE BY 1h FILL(PREV)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("ts")
                    .returns("""
                            ts\tp
                            2024-01-01T00:00:00.000000Z\t2.0
                            2024-01-01T01:00:00.000000Z\t2.0
                            2024-01-01T02:00:00.000000Z\t2.0
                            2024-01-01T03:00:00.000000Z\t5.5
                            """);
            assertQuery("SELECT ts, percentile_cont(v, 0.25) p FROM t SAMPLE BY 1h FILL(-1)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("ts")
                    .returns("""
                            ts\tp
                            2024-01-01T00:00:00.000000Z\t2.0
                            2024-01-01T01:00:00.000000Z\t-1.0
                            2024-01-01T02:00:00.000000Z\t-1.0
                            2024-01-01T03:00:00.000000Z\t5.5
                            """);
            // ALIGN TO FIRST OBSERVATION runs the keyed fill cursors, which seed every key with setNull()
            assertQuery("SELECT ts, k, percentile_cont(v, 0.25) p FROM t SAMPLE BY 1h FILL(PREV) ALIGN TO FIRST OBSERVATION")
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("ts")
                    .returns("""
                            ts\tk\tp
                            2024-01-01T00:00:00.000000Z\ta\t1.5
                            2024-01-01T00:00:00.000000Z\tb\t10.0
                            2024-01-01T00:00:00.000000Z\tc\tnull
                            2024-01-01T01:00:00.000000Z\ta\t1.5
                            2024-01-01T01:00:00.000000Z\tb\t10.0
                            2024-01-01T01:00:00.000000Z\tc\tnull
                            2024-01-01T02:00:00.000000Z\ta\t1.5
                            2024-01-01T02:00:00.000000Z\tb\t10.0
                            2024-01-01T02:00:00.000000Z\tc\tnull
                            2024-01-01T03:00:00.000000Z\ta\t5.0
                            2024-01-01T03:00:00.000000Z\tb\tnull
                            2024-01-01T03:00:00.000000Z\tc\t7.0
                            """);
        });
    }

    @Test
    public void testSampleByFillLinearNotSupported() throws Exception {
        // the list pointer in the map value cannot hold an interpolated value
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            final String sql = "SELECT ts, percentile_cont(v, 0.5) FROM t SAMPLE BY 1h FILL(LINEAR)";
            assertExceptionNoLeakCheck(sql, sql.indexOf("LINEAR"), "support for LINEAR fill is not yet implemented");
        });
    }

    @Test
    public void testSingleValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (x DOUBLE)");
            execute("INSERT INTO test VALUES (42.0)");
            assertQuery("SELECT percentile_cont(x, 0.5) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("percentile_cont\n42.0\n");
        });
    }

    @Test
    public void testWithKnownData() throws Exception {
        assertMemoryLeak(() -> {
            execute(txDdl);
            execute(txDml);
            assertQuery("SELECT percentile_cont(value, 0.95) FROM tx_traffic")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            percentile_cont
                            268.20624999999984
                            """);
        });
    }

    @Test
    public void testSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT x::DOUBLE AS val, timestamp_sequence('2024-01-01T00:00:00', 3600000000L) ts FROM long_sequence(5)) TIMESTAMP(ts)");
            assertQuery("SELECT ts, percentile_cont(val, 0.5) FROM test SAMPLE BY 1h")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("ts\tpercentile_cont\n" +
                            "2024-01-01T00:00:00.000000Z\t1.0\n" +
                            "2024-01-01T01:00:00.000000Z\t2.0\n" +
                            "2024-01-01T02:00:00.000000Z\t3.0\n" +
                            "2024-01-01T03:00:00.000000Z\t4.0\n" +
                            "2024-01-01T04:00:00.000000Z\t5.0\n");
        });
    }

}
