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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.cairo.CairoException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class ApproxPercentileLongGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testApprox0thPercentileLongValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select x from long_sequence(100))");
            assertQuery("select approx_percentile(x, 0) from test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("approx_percentile\n1.0\n");
        });
    }

    @Test
    public void testApprox100thPercentileLongValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select x from long_sequence(100))");
            assertQuery("select approx_percentile(x, 1.0) from test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("approx_percentile\n103.0\n");
        });
    }

    @Test
    public void testApprox50thPercentileIntValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as int) x from long_sequence(100))");
            assertQuery("select approx_percentile(x, 0.5) from test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("approx_percentile\n51.0\n");
        });
    }

    @Test
    public void testApprox50thPercentileLongValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select x from long_sequence(100))");
            assertQuery("select approx_percentile(x, 0.5) from test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("approx_percentile\n51.0\n");
        });
    }

    @Test
    public void testApprox50thPercentileLongValuesWith5() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select x from long_sequence(100))");
            assertQuery("select approx_percentile(x, 0.5) from test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("approx_percentile\n51.0\n");
        });
    }

    // test increasing level of precision
    @Test
    public void testApprox50thPercentileWithPrecision1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select x from long_sequence(1000))");
            assertQuery("select approx_percentile(x, 0.5, 1) from test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("approx_percentile\n511.0\n");
        });
    }

    @Test
    public void testApprox50thPercentileWithPrecision2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select x from long_sequence(1000))");
            assertQuery("select approx_percentile(x, 0.5, 2) from test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("approx_percentile\n501.0\n");
        });
    }

    @Test
    public void testApprox50thPercentileWithPrecision3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select x from long_sequence(1000))");
            assertQuery("select approx_percentile(x, 0.5, 3) from test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("approx_percentile\n500.0\n");
        });
    }

    @Test
    public void testApprox50thPercentileWithPrecision4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select x from long_sequence(1000))");
            assertQuery("select approx_percentile(x, 0.5, 4) from test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("approx_percentile\n500.0\n");
        });
    }

    @Test
    public void testApprox50thPercentileWithPrecision5() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select x from long_sequence(1000))");
            assertQuery("select approx_percentile(x, 0.5, 5) from test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("approx_percentile\n500.0\n");
        });
    }

    @Test
    public void testApproxPercentileAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x long)");
            execute("insert into test values (null), (null), (null)");
            assertQuery("select approx_percentile(x, 0.5) from test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            approx_percentile
                            null
                            """);
        });
    }

    @Test
    public void testApproxPercentileAllSameValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select 5 x from long_sequence(100))");
            assertQuery("select approx_percentile(x, 0.5) from test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("approx_percentile\n5.0\n");
        });
    }

    @Test
    public void testApproxPercentileEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x long)");
            assertQuery("select approx_percentile(x, 0.5) from test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            approx_percentile
                            null
                            """);
        });
    }

    @Test
    public void testApproxPercentilePackedAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x long)");
            execute("insert into test values (null), (null), (null)");
            assertQuery("select approx_percentile(x, 0.5, 5) from test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            approx_percentile
                            null
                            """);
        });
    }

    @Test
    public void testApproxPercentilePackedEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x long)");
            assertQuery("select approx_percentile(x, 0.5, 5) from test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            approx_percentile
                            null
                            """);
        });
    }

    @Test
    public void testApproxPercentilePackedWithPercentileBindVariable() throws Exception {
        bindVariableService.setDouble(0, 0.5);
        assertMemoryLeak(() -> {
            execute("create table test as (select 5 x from long_sequence(100))");
            assertQuery("select approx_percentile(x, $1, 5) from test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("approx_percentile\n5.0\n");
        });
    }

    @Test
    public void testApproxPercentileSampleByFillPrevAlignToFirstObservation() throws Exception {
        // The keyed fill cursor seeds every key with setNull() before the first bucket. 'c' has no
        // rows until 02:45, so it must read as null rather than as the histogram of the first key.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (k SYMBOL, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                    ('a', 1, '2024-01-01T00:00:00.000000Z'),
                    ('b', 1, '2024-01-01T00:40:00.000000Z'),
                    ('a', 1, '2024-01-01T02:00:00.000000Z'),
                    ('c', 1, '2024-01-01T02:45:00.000000Z')
                    """);
            // p and m use the default precision, p4 and m4 the packed histogram
            assertQuery("""
                    SELECT ts, k, approx_percentile(v, 0.5) p, approx_percentile(v, 0.5, 4) p4, approx_median(v) m, approx_median(v, 4) m4
                    FROM t
                    SAMPLE BY 1h FILL(PREV) ALIGN TO FIRST OBSERVATION
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("ts")
                    .returns("""
                            ts\tk\tp\tp4\tm\tm4
                            2024-01-01T00:00:00.000000Z\ta\t1.0\t1.0\t1.0\t1.0
                            2024-01-01T00:00:00.000000Z\tb\t1.0\t1.0\t1.0\t1.0
                            2024-01-01T00:00:00.000000Z\tc\tnull\tnull\tnull\tnull
                            2024-01-01T01:00:00.000000Z\ta\t1.0\t1.0\t1.0\t1.0
                            2024-01-01T01:00:00.000000Z\tb\t1.0\t1.0\t1.0\t1.0
                            2024-01-01T01:00:00.000000Z\tc\tnull\tnull\tnull\tnull
                            2024-01-01T02:00:00.000000Z\ta\t1.0\t1.0\t1.0\t1.0
                            2024-01-01T02:00:00.000000Z\tb\t1.0\t1.0\t1.0\t1.0
                            2024-01-01T02:00:00.000000Z\tc\t1.0\t1.0\t1.0\t1.0
                            """);
        });
    }

    @Test
    public void testApproxPercentileSomeNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x long)");
            execute("insert into test values (1), (null), (null), (null)");
            assertQuery("select approx_percentile(x, 0.5) from test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            approx_percentile
                            1.0
                            """);
        });
    }

    @Test
    public void testApproxPercentileWindowJoinEmptyWindow() throws Exception {
        // WINDOW JOIN resets the value with setEmpty() for every master row. The 00:10 trade has no
        // price in its window, so it must read as null rather than as the histogram of the first row.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE prices (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO trades VALUES
                    (1.0, '2024-01-01T00:00:00.000000Z'),
                    (2.0, '2024-01-01T00:10:00.000000Z'),
                    (3.0, '2024-01-01T00:20:00.000000Z')
                    """);
            execute("""
                    INSERT INTO prices VALUES
                    (1, '2024-01-01T00:00:30.000000Z'),
                    (1, '2024-01-01T00:00:40.000000Z'),
                    (1, '2024-01-01T00:20:10.000000Z')
                    """);
            assertQuery("""
                    SELECT t.ts, approx_percentile(p.v, 0.5) p, approx_percentile(p.v, 0.5, 4) p4, approx_median(p.v) m, approx_median(p.v, 4) m4
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
                            ts\tp\tp4\tm\tm4
                            2024-01-01T00:00:00.000000Z\t1.0\t1.0\t1.0\t1.0
                            2024-01-01T00:10:00.000000Z\tnull\tnull\tnull\tnull
                            2024-01-01T00:20:00.000000Z\t1.0\t1.0\t1.0\t1.0
                            """);
        });
    }

    @Test
    public void testApproxPercentileWithPercentileBindVariable() throws Exception {
        bindVariableService.setDouble(0, 0.5);
        assertMemoryLeak(() -> {
            execute("create table test as (select 5 x from long_sequence(100))");
            assertQuery("select approx_percentile(x, $1) from test")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("approx_percentile\n5.0\n");
        });
    }

    @Test
    public void testInvalidPercentile1() throws Exception {
        assertQuery("select approx_percentile(x, 1.1) from long_sequence(1)")
                .fails(7, "percentile must be between 0.0 and 1.0");
    }

    @Test
    public void testInvalidPercentile2() throws Exception {
        assertQuery("select approx_percentile(x, -1) from long_sequence(1)")
                .fails(7, "percentile must be between 0.0 and 1.0");
    }

    @Test
    public void testInvalidPercentile3() throws Exception {
        assertQuery("select approx_percentile(x, x) from long_sequence(1)")
                .fails(28, "percentile must be a constant");
    }

    @Test
    public void testInvalidPercentilePacked1() throws Exception {
        assertQuery("select approx_percentile(x, 1.1, 5) from long_sequence(1)")
                .fails(7, "percentile must be between 0.0 and 1.0");
    }

    @Test
    public void testInvalidPercentilePacked2() throws Exception {
        assertQuery("select approx_percentile(x, -1, 5) from long_sequence(1)")
                .fails(7, "percentile must be between 0.0 and 1.0");
    }

    @Test
    public void testInvalidPercentilePacked3() throws Exception {
        assertQuery("select approx_percentile(x, x, 5) from long_sequence(1)")
                .fails(28, "percentile must be a constant");
    }

    @Test
    public void testInvalidPrecision1() throws Exception {
        assertQuery("select approx_percentile(x, 0.5, 6) from long_sequence(1)")
                .fails(33, "precision must be between 0 and 5");
    }

    @Test
    public void testInvalidPrecision2() throws Exception {
        assertQuery("select approx_percentile(x, 0.5, -1) from long_sequence(1)")
                .fails(33, "precision must be between 0 and 5");
    }

    @Test
    public void testThrowsOnNegativeValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x long)");
            execute("insert into test values (1), (-1)");
            try {
                assertQuery("select approx_percentile(x, 0.5) from test")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                approx_percentile
                                1.0
                                """);
                Assert.fail();
            } catch (CairoException ignore) {
            }
        });
    }

    @Test
    public void testThrowsOnNegativeValuesPacked() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x long)");
            execute("insert into test values (1), (-1)");
            try {
                assertQuery("select approx_percentile(x, 0.5, 5) from test")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                approx_percentile
                                1.0
                                """);
                Assert.fail();
            } catch (CairoException ignore) {
            }
        });
    }
}
