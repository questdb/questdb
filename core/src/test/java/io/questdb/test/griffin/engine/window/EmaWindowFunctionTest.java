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

package io.questdb.test.griffin.engine.window;

import io.questdb.griffin.engine.functions.window.EmaDoubleWindowFunctionFactory;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class EmaWindowFunctionTest extends AbstractCairoTest {
    private final TestTimestampType timestampType;

    public EmaWindowFunctionTest() {
        this.timestampType = TestUtils.getTimestampType();
    }

    @Test
    public void testEmaAlphaBoundary() throws Exception {
        // Test avg() with alpha=1.0 (should just return current value)
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, val double) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab select x::timestamp, x from long_sequence(5)");

            // alpha=1 means EMA = 1*current + 0*previous = current
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\tval\tavg
                            1970-01-01T00:00:00.000001Z\t1.0\t1.0
                            1970-01-01T00:00:00.000002Z\t2.0\t2.0
                            1970-01-01T00:00:00.000003Z\t3.0\t3.0
                            1970-01-01T00:00:00.000004Z\t4.0\t4.0
                            1970-01-01T00:00:00.000005Z\t5.0\t5.0
                            """),
                    "select ts, val, avg(val, 'alpha', 1.0) over (order by ts) from tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testEmaAlphaMode() throws Exception {
        // Test avg() with alpha mode (direct smoothing factor)
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, val double) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab select x::timestamp, x from long_sequence(5)");

            // EMA with alpha=0.5:
            // row 1: ema = 1
            // row 2: ema = 0.5 * 2 + 0.5 * 1 = 1.5
            // row 3: ema = 0.5 * 3 + 0.5 * 1.5 = 2.25
            // row 4: ema = 0.5 * 4 + 0.5 * 2.25 = 3.125
            // row 5: ema = 0.5 * 5 + 0.5 * 3.125 = 4.0625
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\tval\tavg
                            1970-01-01T00:00:00.000001Z\t1.0\t1.0
                            1970-01-01T00:00:00.000002Z\t2.0\t1.5
                            1970-01-01T00:00:00.000003Z\t3.0\t2.25
                            1970-01-01T00:00:00.000004Z\t4.0\t3.125
                            1970-01-01T00:00:00.000005Z\t5.0\t4.0625
                            """),
                    "select ts, val, avg(val, 'alpha', 0.5) over (order by ts) from tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testEmaAlphaModeWithPartition() throws Exception {
        // Test avg() with alpha mode and partition by
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, val double) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab select x::timestamp, x%2, x from long_sequence(6)");

            // i=0: values 2,4,6 at ts 2,4,6
            //   row 1 (val=2): ema = 2
            //   row 2 (val=4): ema = 0.5 * 4 + 0.5 * 2 = 3
            //   row 3 (val=6): ema = 0.5 * 6 + 0.5 * 3 = 4.5
            // i=1: values 1,3,5 at ts 1,3,5
            //   row 1 (val=1): ema = 1
            //   row 2 (val=3): ema = 0.5 * 3 + 0.5 * 1 = 2
            //   row 3 (val=5): ema = 0.5 * 5 + 0.5 * 2 = 3.5
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tval\tavg
                            1970-01-01T00:00:00.000001Z\t1\t1.0\t1.0
                            1970-01-01T00:00:00.000002Z\t0\t2.0\t2.0
                            1970-01-01T00:00:00.000003Z\t1\t3.0\t2.0
                            1970-01-01T00:00:00.000004Z\t0\t4.0\t3.0
                            1970-01-01T00:00:00.000005Z\t1\t5.0\t3.5
                            1970-01-01T00:00:00.000006Z\t0\t6.0\t4.5
                            """),
                    "select ts, i, val, avg(val, 'alpha', 0.5) over (partition by i order by ts) from tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testEmaEmptyTable() throws Exception {
        // Test avg() with empty table
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, val double) timestamp(ts)", timestampType.getTypeName());

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\tval\tavg
                            """),
                    "select ts, val, avg(val, 'alpha', 0.5) over (order by ts) from tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testEmaExceptionAlphaMustBeBetween0And1() throws Exception {
        assertException(
                "select ts, val, avg(val, 'alpha', 1.5) over (order by ts) from tab",
                "create table tab (ts " + timestampType.getTypeName() + ", val double) timestamp(ts)",
                34,
                "alpha must be between 0 (exclusive) and 1 (inclusive)"
        );
    }

    @Test
    public void testEmaExceptionFramingNotSupported() throws Exception {
        assertException(
                "select ts, val, avg(val, 'alpha', 0.5) over (order by ts rows between 1 preceding and current row) from tab",
                "create table tab (ts " + timestampType.getTypeName() + ", val double) timestamp(ts)",
                16,
                "avg() does not support framing; remove ROWS/RANGE clause"
        );
    }

    @Test
    public void testEmaExceptionInfinityParameterValue() throws Exception {
        assertException(
                "select ts, val, avg(val, 'alpha', 1.0/0.0) over (order by ts) from tab",
                "create table tab (ts " + timestampType.getTypeName() + ", val double) timestamp(ts)",
                37,
                "parameter value must be a positive number"
        );
    }

    @Test
    public void testEmaExceptionInvalidKind() throws Exception {
        assertException(
                "select ts, val, avg(val, 'invalid', 0.5) over (order by ts) from tab",
                "create table tab (ts " + timestampType.getTypeName() + ", val double) timestamp(ts)",
                25,
                "invalid kind parameter: expected 'alpha', 'period', or a time unit (second, minute, hour, day, week)"
        );
    }

    @Test
    public void testEmaExceptionKindCannotBeNull() throws Exception {
        assertException(
                "select ts, val, avg(val, cast(null as string), 0.5) over (order by ts) from tab",
                "create table tab (ts " + timestampType.getTypeName() + ", val double) timestamp(ts)",
                25,
                "kind parameter cannot be null"
        );
    }

    @Test
    public void testEmaExceptionKindMustBeConstant() throws Exception {
        assertException(
                "select ts, kind, val, avg(val, kind, 0.5) over (order by ts) from tab",
                "create table tab (ts " + timestampType.getTypeName() + ", kind string, val double) timestamp(ts)",
                31,
                "kind parameter must be a constant"
        );
    }

    @Test
    public void testEmaExceptionNaNParameterValue() throws Exception {
        assertException(
                "select ts, val, avg(val, 'alpha', 0.0/0.0) over (order by ts) from tab",
                "create table tab (ts " + timestampType.getTypeName() + ", val double) timestamp(ts)",
                37,
                "parameter value must be a positive number"
        );
    }

    @Test
    public void testEmaExceptionNegativeParameterValue() throws Exception {
        assertException(
                "select ts, val, avg(val, 'period', -1) over (order by ts) from tab",
                "create table tab (ts " + timestampType.getTypeName() + ", val double) timestamp(ts)",
                35,
                "parameter value must be a positive number"
        );
    }

    @Test
    public void testEmaExceptionOrderByRequired() throws Exception {
        assertException(
                "select ts, val, avg(val, 'alpha', 0.5) over () from tab",
                "create table tab (ts " + timestampType.getTypeName() + ", val double) timestamp(ts)",
                16,
                "avg() requires ORDER BY"
        );
    }

    @Test
    public void testEmaExceptionParameterMustBeConstant() throws Exception {
        assertException(
                "select ts, val, avg(val, 'alpha', val) over (order by ts) from tab",
                "create table tab (ts " + timestampType.getTypeName() + ", val double) timestamp(ts)",
                34,
                "parameter value must be a constant"
        );
    }

    @Test
    public void testEmaExceptionZeroParameterValue() throws Exception {
        assertException(
                "select ts, val, avg(val, 'alpha', 0) over (order by ts) from tab",
                "create table tab (ts " + timestampType.getTypeName() + ", val double) timestamp(ts)",
                34,
                "parameter value must be a positive number"
        );
    }

    @Test
    public void testEmaExceptionTauTooSmall() throws Exception {
        // When value < 1, casting to long produces 0, which would make tau = 0
        assertException(
                "select ts, val, avg(val, 'microsecond', 0.5) over (order by ts) from tab",
                "create table tab (ts " + timestampType.getTypeName() + ", val double) timestamp(ts)",
                25,
                "time constant must be at least 1 unit in native timestamp precision"
        );
    }

    @Test
    public void testEmaExplainPlan() throws Exception {
        // Test that explain shows correct plan for avg() EMA function
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, val double) timestamp(ts)", timestampType.getTypeName());

            // Non-partitioned alpha mode
            assertSql(
                    """
                            QUERY PLAN
                            Window
                              functions: [avg(val, 'alpha', 0.5) over (rows between unbounded preceding and current row)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tab
                            """,
                    "explain select ts, val, avg(val, 'alpha', 0.5) over (order by ts) from tab"
            );

            // Partitioned period mode
            assertSql(
                    """
                            QUERY PLAN
                            Window
                              functions: [avg(val, 'period', 10.0) over (partition by [i] rows between unbounded preceding and current row)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tab
                            """,
                    "explain select ts, i, val, avg(val, 'period', 10) over (partition by i order by ts) from tab"
            );

            // Non-partitioned time-weighted mode
            assertSql(
                    """
                            QUERY PLAN
                            Window
                              functions: [avg(val, 'second', 1.0) over (rows between unbounded preceding and current row)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tab
                            """,
                    "explain select ts, val, avg(val, 'second', 1) over (order by ts) from tab"
            );

            // Partitioned time-weighted mode
            assertSql(
                    """
                            QUERY PLAN
                            Window
                              functions: [avg(val, 'minute', 5.0) over (partition by [i] rows between unbounded preceding and current row)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tab
                            """,
                    "explain select ts, i, val, avg(val, 'minute', 5) over (partition by i order by ts) from tab"
            );
        });
    }

    @Test
    public void testEmaFactoryIsWindow() {
        // Test that EmaDoubleWindowFunctionFactory is recognized as a window function
        EmaDoubleWindowFunctionFactory factory = new EmaDoubleWindowFunctionFactory();
        Assert.assertTrue(factory.isWindow());
    }

    @Test
    public void testEmaLargeDataset() throws Exception {
        // Test avg() with large dataset to verify memory handling
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, val double) timestamp(ts)", timestampType.getTypeName());
            // Create 1000 rows across 10 partitions
            execute("insert into tab select x::timestamp, x%10, x::double from long_sequence(1000)");

            // Just verify it runs without error and returns expected row count
            assertSql(
                    "count\n1000\n",
                    "select count(*) from (select ts, i, val, avg(val, 'alpha', 0.1) over (partition by i order by ts) from tab)"
            );
        });
    }

    @Test
    public void testEmaMultiplePartitionsWithNulls() throws Exception {
        // Test avg() with multiple partitions having various NULL patterns
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, val double) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab values (1::timestamp, 1, 10.0)");
            execute("insert into tab values (2::timestamp, 2, NULL)");
            execute("insert into tab values (3::timestamp, 1, NULL)");
            execute("insert into tab values (4::timestamp, 2, 20.0)");
            execute("insert into tab values (5::timestamp, 1, 30.0)");
            execute("insert into tab values (6::timestamp, 2, NULL)");

            // Partition 1: 10, NULL, 30 -> ema: 10, 10, 0.5*30+0.5*10=20
            // Partition 2: NULL, 20, NULL -> ema: NaN, 20, 20
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tval\tavg
                            1970-01-01T00:00:00.000001Z\t1\t10.0\t10.0
                            1970-01-01T00:00:00.000002Z\t2\tnull\tnull
                            1970-01-01T00:00:00.000003Z\t1\tnull\t10.0
                            1970-01-01T00:00:00.000004Z\t2\t20.0\t20.0
                            1970-01-01T00:00:00.000005Z\t1\t30.0\t20.0
                            1970-01-01T00:00:00.000006Z\t2\tnull\t20.0
                            """),
                    "select ts, i, val, avg(val, 'alpha', 0.5) over (partition by i order by ts) from tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testEmaNullHandling() throws Exception {
        // Test that avg handles NULL values correctly (should skip nulls and keep previous ema)
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, val double) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab values (1::timestamp, 10.0)");
            execute("insert into tab values (2::timestamp, NULL)");
            execute("insert into tab values (3::timestamp, 20.0)");
            execute("insert into tab values (4::timestamp, NULL)");
            execute("insert into tab values (5::timestamp, 30.0)");

            // EMA with alpha=0.5:
            // row 1 (val=10): ema = 10
            // row 2 (val=NULL): ema = 10 (keep previous)
            // row 3 (val=20): ema = 0.5 * 20 + 0.5 * 10 = 15
            // row 4 (val=NULL): ema = 15 (keep previous)
            // row 5 (val=30): ema = 0.5 * 30 + 0.5 * 15 = 22.5
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\tval\tavg
                            1970-01-01T00:00:00.000001Z\t10.0\t10.0
                            1970-01-01T00:00:00.000002Z\tnull\t10.0
                            1970-01-01T00:00:00.000003Z\t20.0\t15.0
                            1970-01-01T00:00:00.000004Z\tnull\t15.0
                            1970-01-01T00:00:00.000005Z\t30.0\t22.5
                            """),
                    "select ts, val, avg(val, 'alpha', 0.5) over (order by ts) from tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testEmaPartitionAllNulls() throws Exception {
        // Test avg() with partition where all values are NULL
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, val double) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab values (1::timestamp, 1, NULL)");
            execute("insert into tab values (2::timestamp, 1, NULL)");
            execute("insert into tab values (3::timestamp, 2, 10.0)");

            // Partition 1 has all NULLs -> EMA should be NaN
            // Partition 2 has a value -> EMA should be 10
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tval\tavg
                            1970-01-01T00:00:00.000001Z\t1\tnull\tnull
                            1970-01-01T00:00:00.000002Z\t1\tnull\tnull
                            1970-01-01T00:00:00.000003Z\t2\t10.0\t10.0
                            """),
                    "select ts, i, val, avg(val, 'alpha', 0.5) over (partition by i order by ts) from tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testEmaPartitionFirstRowNull() throws Exception {
        // Test avg() with partition where first row is NULL
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, val double) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab values (1::timestamp, 1, NULL)");
            execute("insert into tab values (2::timestamp, 1, 10.0)");
            execute("insert into tab values (3::timestamp, 1, 20.0)");

            // Partition 1: NULL, 10, 20
            // Row 1: val=NULL -> ema=NaN
            // Row 2: val=10, first valid -> ema=10
            // Row 3: val=20, alpha=0.5 -> ema = 0.5*20 + 0.5*10 = 15
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tval\tavg
                            1970-01-01T00:00:00.000001Z\t1\tnull\tnull
                            1970-01-01T00:00:00.000002Z\t1\t10.0\t10.0
                            1970-01-01T00:00:00.000003Z\t1\t20.0\t15.0
                            """),
                    "select ts, i, val, avg(val, 'alpha', 0.5) over (partition by i order by ts) from tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testEmaPeriodMode() throws Exception {
        // Test avg() with period mode (N-period EMA where alpha = 2 / (N + 1))
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, val double) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab select x::timestamp, x from long_sequence(5)");

            // EMA with period=3 means alpha = 2/(3+1) = 0.5 (same as alpha=0.5 test)
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\tval\tavg
                            1970-01-01T00:00:00.000001Z\t1.0\t1.0
                            1970-01-01T00:00:00.000002Z\t2.0\t1.5
                            1970-01-01T00:00:00.000003Z\t3.0\t2.25
                            1970-01-01T00:00:00.000004Z\t4.0\t3.125
                            1970-01-01T00:00:00.000005Z\t5.0\t4.0625
                            """),
                    "select ts, val, avg(val, 'period', 3) over (order by ts) from tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testEmaPeriodModeWithPartition() throws Exception {
        // Test avg() with period mode and partition by
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, val double) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab select x::timestamp, x%2, x from long_sequence(6)");

            // period=3 means alpha = 0.5 (same as alpha mode partition test)
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tval\tavg
                            1970-01-01T00:00:00.000001Z\t1\t1.0\t1.0
                            1970-01-01T00:00:00.000002Z\t0\t2.0\t2.0
                            1970-01-01T00:00:00.000003Z\t1\t3.0\t2.0
                            1970-01-01T00:00:00.000004Z\t0\t4.0\t3.0
                            1970-01-01T00:00:00.000005Z\t1\t5.0\t3.5
                            1970-01-01T00:00:00.000006Z\t0\t6.0\t4.5
                            """),
                    "select ts, i, val, avg(val, 'period', 3) over (partition by i order by ts) from tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testEmaPeriodOne() throws Exception {
        // Test avg() with period=1 (alpha = 2/(1+1) = 1.0)
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, val double) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab select x::timestamp, x from long_sequence(5)");

            // period=1 -> alpha=1 -> EMA equals current value
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\tval\tavg
                            1970-01-01T00:00:00.000001Z\t1.0\t1.0
                            1970-01-01T00:00:00.000002Z\t2.0\t2.0
                            1970-01-01T00:00:00.000003Z\t3.0\t3.0
                            1970-01-01T00:00:00.000004Z\t4.0\t4.0
                            1970-01-01T00:00:00.000005Z\t5.0\t5.0
                            """),
                    "select ts, val, avg(val, 'period', 1) over (order by ts) from tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testEmaResetBetweenQueries() throws Exception {
        // Test that EMA state resets properly between queries
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, val double) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab select x::timestamp, x from long_sequence(3)");

            // First query
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\tval\tavg
                            1970-01-01T00:00:00.000001Z\t1.0\t1.0
                            1970-01-01T00:00:00.000002Z\t2.0\t1.5
                            1970-01-01T00:00:00.000003Z\t3.0\t2.25
                            """),
                    "select ts, val, avg(val, 'alpha', 0.5) over (order by ts) from tab",
                    "ts",
                    false,
                    true
            );

            // Second query should produce same results (state was reset)
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\tval\tavg
                            1970-01-01T00:00:00.000001Z\t1.0\t1.0
                            1970-01-01T00:00:00.000002Z\t2.0\t1.5
                            1970-01-01T00:00:00.000003Z\t3.0\t2.25
                            """),
                    "select ts, val, avg(val, 'alpha', 0.5) over (order by ts) from tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testEmaSameTimestamps() throws Exception {
        // Test time-weighted avg() with identical timestamps (dt=0 -> alpha=1)
        // Using timestamp strings so test works with both MICRO and NANO precision
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, val double) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab values ('1970-01-01T00:00:01.000000Z'::timestamp, 10.0)");
            execute("insert into tab values ('1970-01-01T00:00:01.000000Z'::timestamp, 20.0)");
            execute("insert into tab values ('1970-01-01T00:00:01.000000Z'::timestamp, 30.0)");

            // All same timestamps -> dt=0 -> alpha=1 -> EMA just equals current value
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\tval\tavg
                            1970-01-01T00:00:01.000000Z\t10.0\t10.0
                            1970-01-01T00:00:01.000000Z\t20.0\t20.0
                            1970-01-01T00:00:01.000000Z\t30.0\t30.0
                            """),
                    "select ts, val, avg(val, 'second', 1) over (order by ts) from tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testEmaSingleRow() throws Exception {
        // Test avg() with single row
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, val double) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab values (1::timestamp, 42.0)");

            // Single row -> EMA equals the value itself
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\tval\tavg
                            1970-01-01T00:00:00.000001Z\t42.0\t42.0
                            """),
                    "select ts, val, avg(val, 'alpha', 0.5) over (order by ts) from tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testEmaTimeUnitDay() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, val double) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab values ('1970-01-01T00:00:00.000000Z'::timestamp, 10.0)");
            execute("insert into tab values ('1970-01-02T00:00:00.000000Z'::timestamp, 20.0)");

            // tau = 1 day, dt = 1 day, so alpha = 1 - exp(-1) ≈ 0.6321
            String expected = replaceTimestampSuffix("""
                    ts\tval\tavg
                    1970-01-01T00:00:00.000000Z\t10.0\t10.0
                    1970-01-02T00:00:00.000000Z\t20.0\t16.321205588285576
                    """);
            assertQueryNoLeakCheck(expected, "select ts, val, avg(val, 'day', 1) over (order by ts) from tab", "ts", false, true);
            assertQueryNoLeakCheck(expected, "select ts, val, avg(val, 'days', 1) over (order by ts) from tab", "ts", false, true);
            assertQueryNoLeakCheck(expected, "select ts, val, avg(val, 'DAY', 1) over (order by ts) from tab", "ts", false, true);
            assertQueryNoLeakCheck(expected, "select ts, val, avg(val, 'Days', 1) over (order by ts) from tab", "ts", false, true);
        });
    }

    @Test
    public void testEmaTimeUnitHour() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, val double) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab values ('1970-01-01T00:00:00.000000Z'::timestamp, 10.0)");
            execute("insert into tab values ('1970-01-01T01:00:00.000000Z'::timestamp, 20.0)");

            // tau = 1 hour, dt = 1 hour, so alpha = 1 - exp(-1) ≈ 0.6321
            String expected = replaceTimestampSuffix("""
                    ts\tval\tavg
                    1970-01-01T00:00:00.000000Z\t10.0\t10.0
                    1970-01-01T01:00:00.000000Z\t20.0\t16.321205588285576
                    """);
            assertQueryNoLeakCheck(expected, "select ts, val, avg(val, 'hour', 1) over (order by ts) from tab", "ts", false, true);
            assertQueryNoLeakCheck(expected, "select ts, val, avg(val, 'hours', 1) over (order by ts) from tab", "ts", false, true);
            assertQueryNoLeakCheck(expected, "select ts, val, avg(val, 'HOUR', 1) over (order by ts) from tab", "ts", false, true);
            assertQueryNoLeakCheck(expected, "select ts, val, avg(val, 'Hours', 1) over (order by ts) from tab", "ts", false, true);
        });
    }

    @Test
    public void testEmaTimeUnitMicrosecond() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, val double) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab values ('1970-01-01T00:00:00.000000Z'::timestamp, 10.0)");
            execute("insert into tab values ('1970-01-01T00:00:00.000001Z'::timestamp, 20.0)");

            // tau = 1 microsecond, dt = 1 microsecond, so alpha = 1 - exp(-1) ≈ 0.6321
            String expected = replaceTimestampSuffix("""
                    ts\tval\tavg
                    1970-01-01T00:00:00.000000Z\t10.0\t10.0
                    1970-01-01T00:00:00.000001Z\t20.0\t16.321205588285576
                    """);
            assertQueryNoLeakCheck(expected, "select ts, val, avg(val, 'microsecond', 1) over (order by ts) from tab", "ts", false, true);
            assertQueryNoLeakCheck(expected, "select ts, val, avg(val, 'microseconds', 1) over (order by ts) from tab", "ts", false, true);
            assertQueryNoLeakCheck(expected, "select ts, val, avg(val, 'MICROSECOND', 1) over (order by ts) from tab", "ts", false, true);
            assertQueryNoLeakCheck(expected, "select ts, val, avg(val, 'Microseconds', 1) over (order by ts) from tab", "ts", false, true);
        });
    }

    @Test
    public void testEmaTimeUnitMillisecond() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, val double) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab values ('1970-01-01T00:00:00.000Z'::timestamp, 10.0)");
            execute("insert into tab values ('1970-01-01T00:00:00.001Z'::timestamp, 20.0)");

            // tau = 1 millisecond, dt = 1 millisecond, so alpha = 1 - exp(-1) ≈ 0.6321
            String expected = replaceTimestampSuffix("""
                    ts\tval\tavg
                    1970-01-01T00:00:00.000000Z\t10.0\t10.0
                    1970-01-01T00:00:00.001000Z\t20.0\t16.321205588285576
                    """);
            assertQueryNoLeakCheck(expected, "select ts, val, avg(val, 'millisecond', 1) over (order by ts) from tab", "ts", false, true);
            assertQueryNoLeakCheck(expected, "select ts, val, avg(val, 'milliseconds', 1) over (order by ts) from tab", "ts", false, true);
            assertQueryNoLeakCheck(expected, "select ts, val, avg(val, 'MILLISECOND', 1) over (order by ts) from tab", "ts", false, true);
            assertQueryNoLeakCheck(expected, "select ts, val, avg(val, 'Milliseconds', 1) over (order by ts) from tab", "ts", false, true);
        });
    }

    @Test
    public void testEmaTimeUnitMinute() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, val double) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab values ('1970-01-01T00:00:00.000000Z'::timestamp, 10.0)");
            execute("insert into tab values ('1970-01-01T00:01:00.000000Z'::timestamp, 20.0)");

            // tau = 1 minute, dt = 1 minute, so alpha = 1 - exp(-1) ≈ 0.6321
            String expected = replaceTimestampSuffix("""
                    ts\tval\tavg
                    1970-01-01T00:00:00.000000Z\t10.0\t10.0
                    1970-01-01T00:01:00.000000Z\t20.0\t16.321205588285576
                    """);
            assertQueryNoLeakCheck(expected, "select ts, val, avg(val, 'minute', 1) over (order by ts) from tab", "ts", false, true);
            assertQueryNoLeakCheck(expected, "select ts, val, avg(val, 'minutes', 1) over (order by ts) from tab", "ts", false, true);
            assertQueryNoLeakCheck(expected, "select ts, val, avg(val, 'MINUTE', 1) over (order by ts) from tab", "ts", false, true);
            assertQueryNoLeakCheck(expected, "select ts, val, avg(val, 'Minutes', 1) over (order by ts) from tab", "ts", false, true);
        });
    }

    @Test
    public void testEmaTimeUnitSecond() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, val double) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab values ('1970-01-01T00:00:00.000000Z'::timestamp, 10.0)");
            execute("insert into tab values ('1970-01-01T00:00:01.000000Z'::timestamp, 20.0)");

            // tau = 1 second, dt = 1 second, so alpha = 1 - exp(-1) ≈ 0.6321
            String expected = replaceTimestampSuffix("""
                    ts\tval\tavg
                    1970-01-01T00:00:00.000000Z\t10.0\t10.0
                    1970-01-01T00:00:01.000000Z\t20.0\t16.321205588285576
                    """);
            assertQueryNoLeakCheck(expected, "select ts, val, avg(val, 'second', 1) over (order by ts) from tab", "ts", false, true);
            assertQueryNoLeakCheck(expected, "select ts, val, avg(val, 'seconds', 1) over (order by ts) from tab", "ts", false, true);
            assertQueryNoLeakCheck(expected, "select ts, val, avg(val, 'SECOND', 1) over (order by ts) from tab", "ts", false, true);
            assertQueryNoLeakCheck(expected, "select ts, val, avg(val, 'Seconds', 1) over (order by ts) from tab", "ts", false, true);
        });
    }

    @Test
    public void testEmaTimeUnitWeek() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, val double) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab values ('1970-01-01T00:00:00.000000Z'::timestamp, 10.0)");
            execute("insert into tab values ('1970-01-08T00:00:00.000000Z'::timestamp, 20.0)");

            // tau = 1 week, dt = 1 week, so alpha = 1 - exp(-1) ≈ 0.6321
            String expected = replaceTimestampSuffix("""
                    ts\tval\tavg
                    1970-01-01T00:00:00.000000Z\t10.0\t10.0
                    1970-01-08T00:00:00.000000Z\t20.0\t16.321205588285576
                    """);
            assertQueryNoLeakCheck(expected, "select ts, val, avg(val, 'week', 1) over (order by ts) from tab", "ts", false, true);
            assertQueryNoLeakCheck(expected, "select ts, val, avg(val, 'weeks', 1) over (order by ts) from tab", "ts", false, true);
            assertQueryNoLeakCheck(expected, "select ts, val, avg(val, 'WEEK', 1) over (order by ts) from tab", "ts", false, true);
            assertQueryNoLeakCheck(expected, "select ts, val, avg(val, 'Weeks', 1) over (order by ts) from tab", "ts", false, true);
        });
    }

    @Test
    public void testEmaTimeUnits() throws Exception {
        // Test that different time unit spellings work (singular and plural)
        // Note: testEmaTimeWeightedMode already tests the actual time-weighted computation with seconds
        // Using timestamp strings so test works with both MICRO and NANO precision
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, val double) timestamp(ts)", timestampType.getTypeName());
            // Create rows 1 second apart (same as testEmaTimeWeightedMode)
            execute("insert into tab values ('1970-01-01T00:00:01.000000Z'::timestamp, 10.0)");
            execute("insert into tab values ('1970-01-01T00:00:02.000000Z'::timestamp, 20.0)");

            // tau = 1 second, dt = 1 second, so alpha = 1 - exp(-1) ≈ 0.6321
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\tval\tavg
                            1970-01-01T00:00:01.000000Z\t10.0\t10.0
                            1970-01-01T00:00:02.000000Z\t20.0\t16.321205588285576
                            """),
                    "select ts, val, avg(val, 'second', 1) over (order by ts) from tab",
                    "ts",
                    false,
                    true
            );

            // Also test plural form
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\tval\tavg
                            1970-01-01T00:00:01.000000Z\t10.0\t10.0
                            1970-01-01T00:00:02.000000Z\t20.0\t16.321205588285576
                            """),
                    "select ts, val, avg(val, 'seconds', 1) over (order by ts) from tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testEmaTimeWeightedMode() throws Exception {
        // Test avg() with time-weighted mode
        // alpha = 1 - exp(-dt / tau) where tau = 1 second
        // Using timestamp strings so test works with both MICRO and NANO precision
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, val double) timestamp(ts)", timestampType.getTypeName());
            // Create rows 1 second apart
            execute("insert into tab values ('1970-01-01T00:00:01.000000Z'::timestamp, 10.0)");
            execute("insert into tab values ('1970-01-01T00:00:02.000000Z'::timestamp, 20.0)");
            execute("insert into tab values ('1970-01-01T00:00:03.000000Z'::timestamp, 30.0)");

            // tau = 1 second, dt = 1 second, so alpha = 1 - exp(-1) ≈ 0.6321
            // row 1: ema = 10
            // row 2: alpha = 1 - exp(-1) ≈ 0.6321; ema = 0.6321 * 20 + 0.3679 * 10 ≈ 16.321
            // row 3: alpha = 1 - exp(-1) ≈ 0.6321; ema = 0.6321 * 30 + 0.3679 * 16.321 ≈ 24.967
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\tval\tavg
                            1970-01-01T00:00:01.000000Z\t10.0\t10.0
                            1970-01-01T00:00:02.000000Z\t20.0\t16.321205588285576
                            1970-01-01T00:00:03.000000Z\t30.0\t24.967852755919452
                            """),
                    "select ts, val, avg(val, 'second', 1) over (order by ts) from tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testEmaTimeWeightedModeWithPartition() throws Exception {
        // Test avg() with time-weighted mode and partition by
        // Using timestamp strings so test works with both MICRO and NANO precision
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, val double) timestamp(ts)", timestampType.getTypeName());
            // Create rows for two partitions, 1 second apart within each partition
            execute("insert into tab values ('1970-01-01T00:00:01.000000Z'::timestamp, 0, 10.0)");
            execute("insert into tab values ('1970-01-01T00:00:02.000000Z'::timestamp, 1, 100.0)");
            execute("insert into tab values ('1970-01-01T00:00:03.000000Z'::timestamp, 0, 20.0)");
            execute("insert into tab values ('1970-01-01T00:00:04.000000Z'::timestamp, 1, 200.0)");

            // i=0: ts=1s val=10, ts=3s val=20 (dt=2s)
            //   row 1: ema = 10
            //   row 2: alpha = 1 - exp(-2) ≈ 0.8647; ema = 0.8647 * 20 + 0.1353 * 10 ≈ 18.647
            // i=1: ts=2s val=100, ts=4s val=200 (dt=2s)
            //   row 1: ema = 100
            //   row 2: alpha = 1 - exp(-2) ≈ 0.8647; ema = 0.8647 * 200 + 0.1353 * 100 ≈ 186.47
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tval\tavg
                            1970-01-01T00:00:01.000000Z\t0\t10.0\t10.0
                            1970-01-01T00:00:02.000000Z\t1\t100.0\t100.0
                            1970-01-01T00:00:03.000000Z\t0\t20.0\t18.646647167633873
                            1970-01-01T00:00:04.000000Z\t1\t200.0\t186.46647167633873
                            """),
                    "select ts, i, val, avg(val, 'second', 1) over (partition by i order by ts) from tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testEmaTimeWeightedNoPartitionNullKeepsPrevious() throws Exception {
        // Test time-weighted avg() without partition where NULL keeps previous EMA (L586)
        // Using timestamp strings so test works with both MICRO and NANO precision
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, val double) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab values ('1970-01-01T00:00:01.000000Z'::timestamp, 10.0)");
            execute("insert into tab values ('1970-01-01T00:00:02.000000Z'::timestamp, NULL)");
            execute("insert into tab values ('1970-01-01T00:00:03.000000Z'::timestamp, 20.0)");

            // Row 1: val=10 -> ema=10
            // Row 2: val=NULL -> ema=10 (keeps previous, L586 updates timestamp)
            // Row 3: val=20, dt=1s (from t=2s to t=3s), alpha=1-exp(-1)≈0.6321 -> ema≈16.32
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\tval\tavg
                            1970-01-01T00:00:01.000000Z\t10.0\t10.0
                            1970-01-01T00:00:02.000000Z\tnull\t10.0
                            1970-01-01T00:00:03.000000Z\t20.0\t16.321205588285576
                            """),
                    "select ts, val, avg(val, 'second', 1) over (order by ts) from tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testEmaTimeWeightedPartitionFirstRowNull() throws Exception {
        // Test time-weighted avg() with partition where first row is NULL (L391-394)
        // Using timestamp strings so test works with both MICRO and NANO precision
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, val double) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab values ('1970-01-01T00:00:01.000000Z'::timestamp, 1, NULL)");
            execute("insert into tab values ('1970-01-01T00:00:02.000000Z'::timestamp, 1, 10.0)");
            execute("insert into tab values ('1970-01-01T00:00:03.000000Z'::timestamp, 1, 20.0)");

            // Partition 1: NULL at t=1s, 10 at t=2s, 20 at t=3s
            // Row 1: val=NULL -> ema=NaN (L391-394)
            // Row 2: val=10, first valid value (L417) -> ema=10
            // Row 3: val=20, dt=1s, alpha=1-exp(-1)≈0.6321 -> ema≈16.32
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tval\tavg
                            1970-01-01T00:00:01.000000Z\t1\tnull\tnull
                            1970-01-01T00:00:02.000000Z\t1\t10.0\t10.0
                            1970-01-01T00:00:03.000000Z\t1\t20.0\t16.321205588285576
                            """),
                    "select ts, i, val, avg(val, 'second', 1) over (partition by i order by ts) from tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testEmaTimeWeightedPartitionLargeDataset() throws Exception {
        // Test time-weighted avg() with partition on larger dataset to ensure pass1 coverage (L448-449)
        // Using timestamp strings so test works with both MICRO and NANO precision
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, val double) timestamp(ts)", timestampType.getTypeName());
            // Create 100 rows across 5 partitions, 1 second apart
            execute("insert into tab select " +
                    "dateadd('s', x::int, '1970-01-01T00:00:00.000000Z'::timestamp), " +
                    "x % 5, " +
                    "x::double " +
                    "from long_sequence(100)");

            // Verify it runs and returns expected row count
            assertSql(
                    "count\n100\n",
                    "select count(*) from (select ts, i, val, avg(val, 'second', 1) over (partition by i order by ts) from tab)"
            );
        });
    }

    @Test
    public void testEmaTimeWeightedPartitionNullKeepsPrevious() throws Exception {
        // Test time-weighted avg() with partition where NULL keeps previous EMA (L425-426)
        // Using timestamp strings so test works with both MICRO and NANO precision
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, val double) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab values ('1970-01-01T00:00:01.000000Z'::timestamp, 1, 10.0)");
            execute("insert into tab values ('1970-01-01T00:00:02.000000Z'::timestamp, 1, NULL)");
            execute("insert into tab values ('1970-01-01T00:00:03.000000Z'::timestamp, 1, 20.0)");

            // Partition 1: 10 at t=1s, NULL at t=2s, 20 at t=3s
            // Row 1: val=10 -> ema=10
            // Row 2: val=NULL -> ema=10 (keeps previous, L425-426)
            // Row 3: val=20, dt=1s (from t=2s to t=3s), alpha=1-exp(-1)≈0.6321 -> ema≈16.32
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tval\tavg
                            1970-01-01T00:00:01.000000Z\t1\t10.0\t10.0
                            1970-01-01T00:00:02.000000Z\t1\tnull\t10.0
                            1970-01-01T00:00:03.000000Z\t1\t20.0\t16.321205588285576
                            """),
                    "select ts, i, val, avg(val, 'second', 1) over (partition by i order by ts) from tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testEmaTimeWeightedPartitionSameTimestamp() throws Exception {
        // Test time-weighted avg() with partition where timestamps are identical (dt=0 -> alpha=1) (L409)
        // Using timestamp strings so test works with both MICRO and NANO precision
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, val double) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab values ('1970-01-01T00:00:01.000000Z'::timestamp, 1, 10.0)");
            execute("insert into tab values ('1970-01-01T00:00:01.000000Z'::timestamp, 1, 20.0)");
            execute("insert into tab values ('1970-01-01T00:00:01.000000Z'::timestamp, 1, 30.0)");

            // All same timestamps in partition -> dt=0 -> alpha=1 -> EMA equals current value (L409)
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tval\tavg
                            1970-01-01T00:00:01.000000Z\t1\t10.0\t10.0
                            1970-01-01T00:00:01.000000Z\t1\t20.0\t20.0
                            1970-01-01T00:00:01.000000Z\t1\t30.0\t30.0
                            """),
                    "select ts, i, val, avg(val, 'second', 1) over (partition by i order by ts) from tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    private String replaceTimestampSuffix(String expected) {
        return timestampType == TestTimestampType.NANO ? expected.replaceAll("Z\t", "000Z\t").replaceAll("Z\n", "000Z\n") : expected;
    }
}
