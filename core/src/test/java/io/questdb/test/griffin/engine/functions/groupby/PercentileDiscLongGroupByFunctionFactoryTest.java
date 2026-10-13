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
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class PercentileDiscLongGroupByFunctionFactoryTest extends AbstractCairoTest {

    private static final String txDdl = """
            CREATE TABLE tx_traffic (timestamp TIMESTAMP, value LONG);
            """;

    private static final String txDml = """
            INSERT INTO tx_traffic (timestamp, value) VALUES
            ('2022-05-02T14:51:28Z', '10'),
            ('2022-05-02T14:51:58Z', '85'),
            ('2022-05-02T14:52:28Z', '148'),
            ('2022-05-02T14:52:58Z', '135'),
            ('2022-05-02T14:53:28Z', '47'),
            ('2022-05-02T14:53:58Z', '61'),
            ('2022-05-02T14:54:28Z', '9'),
            ('2022-05-02T14:54:58Z', '63'),
            ('2022-05-02T14:55:28Z', '8'),
            ('2022-05-02T14:55:58Z', '61'),
            ('2022-05-02T14:56:28Z', '8'),
            ('2022-05-02T14:56:58Z', '177'),
            ('2022-05-02T14:57:28Z', '12'),
            ('2022-05-02T14:57:58Z', '62'),
            ('2022-05-02T14:58:28Z', '161'),
            ('2022-05-02T14:58:58Z', '63'),
            ('2022-05-02T14:59:28Z', '10'),
            ('2022-05-02T14:59:58Z', '60'),
            ('2022-05-02T15:00:28Z', '14'),
            ('2022-05-02T15:00:58Z', '200'),
            ('2022-05-02T15:01:28Z', '406'),
            ('2022-05-02T15:01:58Z', '849'),
            ('2022-05-02T15:02:28Z', '14'),
            ('2022-05-02T15:02:58Z', '126'),
            ('2022-05-02T15:03:28Z', '217'),
            ('2022-05-02T15:03:58Z', '66'),
            ('2022-05-02T15:04:28Z', '39'),
            ('2022-05-02T15:04:58Z', '289'),
            ('2022-05-02T15:05:28Z', '182'),
            ('2022-05-02T15:05:58Z', '219'),
            ('2022-05-02T15:06:28Z', '125'),
            ('2022-05-02T15:06:58Z', '87'),
            ('2022-05-02T15:07:28Z', '191'),
            ('2022-05-02T15:07:58Z', '242'),
            ('2022-05-02T15:08:28Z', '224'),
            ('2022-05-02T15:08:58Z', '240'),
            ('2022-05-02T15:09:28Z', '70'),
            ('2022-05-02T15:09:58Z', '177'),
            ('2022-05-02T15:10:28Z', '184'),
            ('2022-05-02T15:10:58Z', '118'),
            ('2022-05-02T15:11:28Z', '8'),
            ('2022-05-02T15:11:58Z', '63'),
            ('2022-05-02T15:12:28Z', '13'),
            ('2022-05-02T15:12:58Z', '60'),
            ('2022-05-02T15:13:28Z', '47'),
            ('2022-05-02T15:13:58Z', '62'),
            ('2022-05-02T15:14:28Z', '11'),
            ('2022-05-02T15:14:58Z', '59'),
            ('2022-05-02T15:15:28Z', '8'),
            ('2022-05-02T15:15:58Z', '65');
            """;

    @Test
    public void test0thPercentileLongValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT x FROM long_sequence(100))");
            assertQuery("SELECT percentile_disc(x, 0) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("percentile_disc\n1\n");
        });
    }

    @Test
    public void test100thPercentileLongValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT x FROM long_sequence(100))");
            assertQuery("SELECT percentile_disc(x, 1.0) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("percentile_disc\n100\n");
        });
    }

    @Test
    public void test50thPercentileIntValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT cast(x AS INT) x FROM long_sequence(100))");
            assertQuery("SELECT percentile_disc(x, 0.5) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("percentile_disc\n50\n");
        });
    }

    @Test
    public void test50thPercentileLongValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT x FROM long_sequence(100))");
            assertQuery("SELECT percentile_disc(x, 0.5) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("percentile_disc\n50\n");
        });
    }

    // test increasing level of precision
    @Test
    public void test50thPercentileWithPrecision() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT x FROM long_sequence(1000))");
            assertQuery("SELECT percentile_disc(x, 0.5) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("percentile_disc\n500\n");
        });
    }

    @Test
    public void testInvalidPercentile1() throws Exception {
        assertException(
                "SELECT percentile_disc(x, 1.1) FROM long_sequence(1)",
                26,
                "invalid percentile"
        );
    }

    @Test
    public void testInvalidPercentile2() throws Exception {
        assertException(
                "SELECT percentile_disc(x, -1.1) FROM long_sequence(1)",
                26,
                "invalid percentile"
        );
    }

    @Test
    public void testNegativePercentile() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT x FROM long_sequence(100))");
            assertExceptionNoLeakCheck(
                    "SELECT percentile_disc(x, -0.5) FROM test",
                    26,
                    "invalid percentile [expected=range(0.0, 1.0), actual=-0.5]"
            );
            assertExceptionNoLeakCheck(
                    "SELECT percentile_disc(x, -1.0) FROM test",
                    26,
                    "invalid percentile [expected=range(0.0, 1.0), actual=-1.0]"
            );
        });
    }

    @Test
    public void testNegativeZeroPercentile() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT x FROM long_sequence(100))");
            assertQuery("SELECT percentile_disc(x, -0.0) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("percentile_disc\n1\n");
        });
    }

    @Test
    public void testPercentileAtWholeNumberProduct() throws Exception {
        // 0.07 * 100 evaluates to 7.000000000000001; the 7th value's cumulative distribution,
        // 7 / 100, already reaches 0.07, so the result is 7, not 8
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT x FROM long_sequence(100))");
            assertQuery(
                    "SELECT percentile_disc(x, 0.07) p07, percentile_disc(x, 0.14) p14, percentile_disc(x, 0.28) p28, " +
                            "percentile_disc(x, 0.55) p55, percentile_disc(x, 0.56) p56 FROM test"
            )
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            p07\tp14\tp28\tp55\tp56
                            7\t14\t28\t55\t56
                            """);
        });
    }

    @Test
    public void testNegativeValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (x LONG)");
            execute("INSERT INTO test VALUES (1), (-1)");
            assertQuery("SELECT percentile_disc(x, 0.5) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            percentile_disc
                            -1
                            """);
        });
    }

    @Test
    public void testNonConstantPercentile() throws Exception {
        // getLong() evaluates the percentile against the GROUP BY output record, which holds
        // no input columns, so a percentile that varies per row has no meaningful value
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (x LONG, p DOUBLE)");
            assertQuery("SELECT percentile_disc(x, p) FROM test")
                    .noLeakCheck()
                    .fails(26, "percentile must be a constant or runtime constant");
            assertQuery("SELECT quantile_disc(x, p) FROM test")
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
                            SELECT CASE WHEN ((x - 1) / 100) % 10 = 0 THEN x END v, (x % 4)::INT k
                            FROM long_sequence(4_000)
                        )""", sqlExecutionContext);
                assertQuery("SELECT percentile_disc(v, 0.25) p25, percentile_disc(v, 0.5) p50, percentile_disc(v, 0.75) p75 FROM tab")
                        .withEngine(engine)
                        .withContext(sqlExecutionContext)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .withPlanContaining("Async Group By workers: 4")
                        .returns("""
                                p25\tp50\tp75
                                100\t1100\t2100
                                """);
                assertQuery("SELECT k, percentile_disc(v, 0.25) p25, percentile_disc(v, 0.5) p50, percentile_disc(v, 0.75) p75 FROM tab ORDER BY k")
                        .withEngine(engine)
                        .withContext(sqlExecutionContext)
                        .noLeakCheck()
                        .expectSize()
                        .withPlanContaining("Async Group By workers: 4")
                        .returns("""
                                k\tp25\tp50\tp75
                                0\t100\t1100\t2100
                                1\t97\t1097\t2097
                                2\t98\t1098\t2098
                                3\t99\t1099\t2099
                                """);
            }, configuration, LOG);
        });
    }

    @Test
    public void testPercentileAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (x LONG)");
            execute("INSERT INTO test VALUES (null), (null), (null)");
            assertQuery("SELECT percentile_disc(x, 0.5) FROM test")
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
    public void testPercentileAllSameValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT 5L x FROM long_sequence(100))");
            assertQuery("SELECT percentile_disc(x, 0.5) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("percentile_disc\n5\n");
        });
    }

    @Test
    public void testPercentileEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (x LONG)");
            assertQuery("SELECT percentile_disc(x, 0.5) FROM test")
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
    public void testPercentileSomeNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (x LONG)");
            execute("INSERT INTO test VALUES (1), (null), (null), (null)");
            assertQuery("SELECT percentile_disc(x, 0.5) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            percentile_disc
                            1
                            """);
        });
    }

    @Test
    public void testPercentileWithPercentileBindVariable() throws Exception {
        bindVariableService.setDouble(0, 0.5);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT 5L x FROM long_sequence(100))");
            assertQuery("SELECT percentile_disc(x, $1) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("percentile_disc\n5\n");
        });
    }

    @Test
    public void testNegativeValuesPacked() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (x LONG)");
            execute("INSERT INTO test VALUES (1), (-1)");
            assertQuery("SELECT percentile_disc(x, 0.5) FROM test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            percentile_disc
                            -1
                            """);
        });
    }

    @Test
    public void testQuickSelectGroupByCorrectness() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE grouped (grp SYMBOL, val LONG)");
            execute("""
                    INSERT INTO grouped (grp, val) VALUES
                    ('A', 10), ('A', 20), ('A', 30), ('A', 40), ('A', 50),
                    ('B', 100), ('B', 200), ('B', 300), ('B', 400), ('B', 500),
                    ('C', 1), ('C', 1), ('C', 1), ('C', 1), ('C', 1)
                    """);
            assertQuery("SELECT grp, percentile_disc(val, 0.5) FROM grouped ORDER BY grp")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            grp\tpercentile_disc
                            A\t30
                            B\t300
                            C\t1
                            """);
        });
    }

    @Test
    public void testSampleByFill() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (k SYMBOL, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                    ('a', 1, '2024-01-01T00:00:00.000000Z'),
                    ('a', 3, '2024-01-01T00:20:00.000000Z'),
                    ('b', 10, '2024-01-01T00:40:00.000000Z'),
                    ('a', 5, '2024-01-01T03:00:00.000000Z'),
                    ('b', null, '2024-01-01T03:30:00.000000Z'),
                    ('c', 7, '2024-01-01T03:45:00.000000Z')
                    """);
            assertQuery("SELECT ts, percentile_disc(v, 0.5) p FROM t SAMPLE BY 1h FILL(NULL)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("ts")
                    .returns("""
                            ts\tp
                            2024-01-01T00:00:00.000000Z\t3
                            2024-01-01T01:00:00.000000Z\tnull
                            2024-01-01T02:00:00.000000Z\tnull
                            2024-01-01T03:00:00.000000Z\t5
                            """);
            assertQuery("SELECT ts, percentile_disc(v, 0.5) p FROM t SAMPLE BY 1h FILL(PREV)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("ts")
                    .returns("""
                            ts\tp
                            2024-01-01T00:00:00.000000Z\t3
                            2024-01-01T01:00:00.000000Z\t3
                            2024-01-01T02:00:00.000000Z\t3
                            2024-01-01T03:00:00.000000Z\t5
                            """);
            assertQuery("SELECT ts, percentile_disc(v, 0.5) p FROM t SAMPLE BY 1h FILL(42)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("ts")
                    .returns("""
                            ts\tp
                            2024-01-01T00:00:00.000000Z\t3
                            2024-01-01T01:00:00.000000Z\t42
                            2024-01-01T02:00:00.000000Z\t42
                            2024-01-01T03:00:00.000000Z\t5
                            """);
            // ALIGN TO FIRST OBSERVATION runs the keyed fill cursors, which seed every key with setNull():
            // 'c' has no value until 03:00 and must stay null rather than borrow another key's list
            assertQuery("SELECT ts, k, percentile_disc(v, 0.5) p FROM t SAMPLE BY 1h FILL(PREV) ALIGN TO FIRST OBSERVATION")
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("ts")
                    .returns("""
                            ts\tk\tp
                            2024-01-01T00:00:00.000000Z\ta\t1
                            2024-01-01T00:00:00.000000Z\tb\t10
                            2024-01-01T00:00:00.000000Z\tc\tnull
                            2024-01-01T01:00:00.000000Z\ta\t1
                            2024-01-01T01:00:00.000000Z\tb\t10
                            2024-01-01T01:00:00.000000Z\tc\tnull
                            2024-01-01T02:00:00.000000Z\ta\t1
                            2024-01-01T02:00:00.000000Z\tb\t10
                            2024-01-01T02:00:00.000000Z\tc\tnull
                            2024-01-01T03:00:00.000000Z\ta\t5
                            2024-01-01T03:00:00.000000Z\tb\tnull
                            2024-01-01T03:00:00.000000Z\tc\t7
                            """);
        });
    }

    @Test
    public void testSampleByFillLinearNotSupported() throws Exception {
        // the list pointer in the map value cannot hold an interpolated value
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            final String sql = "SELECT ts, percentile_disc(v, 0.5) FROM t SAMPLE BY 1h FILL(LINEAR)";
            assertExceptionNoLeakCheck(sql, sql.indexOf("LINEAR"), "support for LINEAR fill is not yet implemented");
        });
    }

    @Test
    public void testWithKnownData() throws Exception {
        assertMemoryLeak(() -> {
            execute(txDdl);
            execute(txDml);
            assertQuery("SELECT percentile_disc(value, 0.95) FROM tx_traffic")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            percentile_disc
                            289
                            """);
        });
    }
}
