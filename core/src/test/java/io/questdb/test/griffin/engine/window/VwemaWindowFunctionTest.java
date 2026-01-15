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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Tests for Volume-Weighted Exponential Moving Average (VWEMA) window function.
 */
public class VwemaWindowFunctionTest extends AbstractCairoTest {

    @Test
    public void testVwemaAlphaMode() throws Exception {
        // Test avg() VWEMA with alpha mode
        assertQuery(
                """
                        ts\tprice\tvolume\tvwema
                        2024-01-01T00:00:00.000000Z\t10.0\t100.0\t10.0
                        2024-01-01T00:00:01.000000Z\t20.0\t200.0\t16.666666666666668
                        2024-01-01T00:00:02.000000Z\t30.0\t300.0\t25.555555555555557
                        """,
                "select ts, price, volume, avg(price, 'alpha', 0.5, volume) over (order by ts) as vwema from tab",
                "create table tab as (" +
                        "select timestamp_sequence('2024-01-01', 1000000) as ts, " +
                        "(x * 10.0) as price, " +
                        "(x * 100.0) as volume " +
                        "from long_sequence(3)" +
                        ") timestamp(ts) partition by day",
                "ts",
                false,
                true
        );
    }

    @Test
    public void testVwemaAlphaModeExplainPlan() throws Exception {
        // VwemaOverUnboundedRowsFrameFunction
        execute("create table tab (ts timestamp, price double, volume double) timestamp(ts)");
        assertPlanNoLeakCheck(
                "select ts, price, volume, avg(price, 'alpha', 0.5, volume) over (order by ts) as vwema from tab",
                """
                        Window
                          functions: [avg(price, 'alpha', 0.5, volume) over (rows between unbounded preceding and current row)]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """
        );
    }

    @Test
    public void testVwemaAlphaModeNaNPrice() throws Exception {
        // L558 false via !isFinite(price) in VwemaOverUnboundedRowsFrameFunction
        assertQuery(
                """
                        ts\tprice\tvolume\tvwema
                        2024-01-01T00:00:00.000000Z\tnull\t100.0\tnull
                        2024-01-01T00:00:01.000000Z\t20.0\t200.0\t20.0
                        2024-01-01T00:00:02.000000Z\t30.0\t300.0\t26.0
                        """,
                "select ts, price, volume, avg(price, 'alpha', 0.5, volume) over (order by ts) as vwema from tab",
                "create table tab as (" +
                        "select timestamp_sequence('2024-01-01', 1000000) as ts, " +
                        "case when x = 1 then null::double else (x * 10.0) end as price, " +
                        "(x * 100.0) as volume " +
                        "from long_sequence(3)" +
                        ") timestamp(ts) partition by day",
                "ts",
                false,
                true
        );
    }

    @Test
    public void testVwemaAlphaModeNaNVolume() throws Exception {
        // L558 false via !isFinite(volume) in VwemaOverUnboundedRowsFrameFunction
        assertQuery(
                """
                        ts\tprice\tvolume\tvwema
                        2024-01-01T00:00:00.000000Z\t10.0\tnull\tnull
                        2024-01-01T00:00:01.000000Z\t20.0\t200.0\t20.0
                        2024-01-01T00:00:02.000000Z\t30.0\t300.0\t26.0
                        """,
                "select ts, price, volume, avg(price, 'alpha', 0.5, volume) over (order by ts) as vwema from tab",
                "create table tab as (" +
                        "select timestamp_sequence('2024-01-01', 1000000) as ts, " +
                        "(x * 10.0) as price, " +
                        "case when x = 1 then null::double else (x * 100.0) end as volume " +
                        "from long_sequence(3)" +
                        ") timestamp(ts) partition by day",
                "ts",
                false,
                true
        );
    }

    @Test
    public void testVwemaAlphaModeZeroVolume() throws Exception {
        // L558 false via volume <= 0 in VwemaOverUnboundedRowsFrameFunction
        assertQuery(
                """
                        ts\tprice\tvolume\tvwema
                        2024-01-01T00:00:00.000000Z\t10.0\t0.0\tnull
                        2024-01-01T00:00:01.000000Z\t20.0\t200.0\t20.0
                        2024-01-01T00:00:02.000000Z\t30.0\t300.0\t26.0
                        """,
                "select ts, price, volume, avg(price, 'alpha', 0.5, volume) over (order by ts) as vwema from tab",
                "create table tab as (" +
                        "select timestamp_sequence('2024-01-01', 1000000) as ts, " +
                        "(x * 10.0) as price, " +
                        "case when x = 1 then 0.0 else (x * 100.0) end as volume " +
                        "from long_sequence(3)" +
                        ") timestamp(ts) partition by day",
                "ts",
                false,
                true
        );
    }

    @Test
    public void testVwemaAlphaPartitionExplainPlan() throws Exception {
        // VwemaOverPartitionFunction
        execute("create table tab (ts timestamp, sym symbol, price double, volume double) timestamp(ts)");
        assertPlanNoLeakCheck(
                "select ts, sym, price, volume, avg(price, 'alpha', 0.5, volume) over (partition by sym order by ts) as vwema from tab",
                """
                        Window
                          functions: [avg(price, 'alpha', 0.5, volume) over (partition by [sym] rows between unbounded preceding and current row)]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """
        );
    }

    @Test
    public void testVwemaAlphaPartitionFirstRowNaNPrice() throws Exception {
        // L296 false via !isFinite(price) in VwemaOverPartitionFunction
        assertQuery(
                """
                        ts\tsym\tprice\tvolume\tvwema
                        2024-01-01T00:00:00.000000Z\tA\tnull\t100.0\tnull
                        2024-01-01T00:00:01.000000Z\tA\t20.0\t200.0\t20.0
                        """,
                "select ts, sym, price, volume, avg(price, 'alpha', 0.5, volume) over (partition by sym order by ts) as vwema from tab",
                "create table tab as (" +
                        "select timestamp_sequence('2024-01-01', 1000000) as ts, " +
                        "'A' as sym, " +
                        "case when x = 1 then null::double else 20.0 end as price, " +
                        "case when x = 1 then 100.0 else 200.0 end as volume " +
                        "from long_sequence(2)" +
                        ") timestamp(ts) partition by day",
                "ts",
                false,
                true
        );
    }

    @Test
    public void testVwemaAlphaPartitionFirstRowNaNVolume() throws Exception {
        // L296 false via !isFinite(volume) in VwemaOverPartitionFunction
        assertQuery(
                """
                        ts\tsym\tprice\tvolume\tvwema
                        2024-01-01T00:00:00.000000Z\tA\t10.0\tnull\tnull
                        2024-01-01T00:00:01.000000Z\tA\t20.0\t200.0\t20.0
                        """,
                "select ts, sym, price, volume, avg(price, 'alpha', 0.5, volume) over (partition by sym order by ts) as vwema from tab",
                "create table tab as (" +
                        "select timestamp_sequence('2024-01-01', 1000000) as ts, " +
                        "'A' as sym, " +
                        "case when x = 1 then 10.0 else 20.0 end as price, " +
                        "case when x = 1 then null::double else 200.0 end as volume " +
                        "from long_sequence(2)" +
                        ") timestamp(ts) partition by day",
                "ts",
                false,
                true
        );
    }

    @Test
    public void testVwemaAlphaPartitionFirstRowZeroVolume() throws Exception {
        // L296 false via volume <= 0 in VwemaOverPartitionFunction
        assertQuery(
                """
                        ts\tsym\tprice\tvolume\tvwema
                        2024-01-01T00:00:00.000000Z\tA\t10.0\t0.0\tnull
                        2024-01-01T00:00:01.000000Z\tA\t20.0\t200.0\t20.0
                        """,
                "select ts, sym, price, volume, avg(price, 'alpha', 0.5, volume) over (partition by sym order by ts) as vwema from tab",
                "create table tab as (" +
                        "select timestamp_sequence('2024-01-01', 1000000) as ts, " +
                        "'A' as sym, " +
                        "case when x = 1 then 10.0 else 20.0 end as price, " +
                        "case when x = 1 then 0.0 else 200.0 end as volume " +
                        "from long_sequence(2)" +
                        ") timestamp(ts) partition by day",
                "ts",
                false,
                true
        );
    }

    @Test
    public void testVwemaAlphaPartitionInvalidAfterValid() throws Exception {
        // L317 false via !isFinite(price) in VwemaOverPartitionFunction - keeps previous VWEMA
        assertQuery(
                """
                        ts\tsym\tprice\tvolume\tvwema
                        2024-01-01T00:00:00.000000Z\tA\t10.0\t100.0\t10.0
                        2024-01-01T00:00:01.000000Z\tA\tnull\t200.0\t10.0
                        2024-01-01T00:00:02.000000Z\tA\t30.0\t300.0\t25.0
                        """,
                "select ts, sym, price, volume, avg(price, 'alpha', 0.5, volume) over (partition by sym order by ts) as vwema from tab",
                "create table tab as (" +
                        "select timestamp_sequence('2024-01-01', 1000000) as ts, " +
                        "'A' as sym, " +
                        "case when x = 1 then 10.0 when x = 2 then null::double else 30.0 end as price, " +
                        "case when x = 1 then 100.0 when x = 2 then 200.0 else 300.0 end as volume " +
                        "from long_sequence(3)" +
                        ") timestamp(ts) partition by day",
                "ts",
                false,
                true
        );
    }

    @Test
    public void testVwemaAlphaPartitionNaNVolumeAfterValid() throws Exception {
        // L317 false via !isFinite(volume) in VwemaOverPartitionFunction - keeps previous VWEMA
        assertQuery(
                """
                        ts\tsym\tprice\tvolume\tvwema
                        2024-01-01T00:00:00.000000Z\tA\t10.0\t100.0\t10.0
                        2024-01-01T00:00:01.000000Z\tA\t20.0\tnull\t10.0
                        2024-01-01T00:00:02.000000Z\tA\t30.0\t300.0\t25.0
                        """,
                "select ts, sym, price, volume, avg(price, 'alpha', 0.5, volume) over (partition by sym order by ts) as vwema from tab",
                "create table tab as (" +
                        "select timestamp_sequence('2024-01-01', 1000000) as ts, " +
                        "'A' as sym, " +
                        "case when x = 1 then 10.0 when x = 2 then 20.0 else 30.0 end as price, " +
                        "case when x = 1 then 100.0 when x = 2 then null::double else 300.0 end as volume " +
                        "from long_sequence(3)" +
                        ") timestamp(ts) partition by day",
                "ts",
                false,
                true
        );
    }

    @Test
    public void testVwemaAlphaPartitionZeroVolumeAfterValid() throws Exception {
        // L317 false via volume <= 0 in VwemaOverPartitionFunction - keeps previous VWEMA
        assertQuery(
                """
                        ts\tsym\tprice\tvolume\tvwema
                        2024-01-01T00:00:00.000000Z\tA\t10.0\t100.0\t10.0
                        2024-01-01T00:00:01.000000Z\tA\t20.0\t0.0\t10.0
                        2024-01-01T00:00:02.000000Z\tA\t30.0\t300.0\t25.0
                        """,
                "select ts, sym, price, volume, avg(price, 'alpha', 0.5, volume) over (partition by sym order by ts) as vwema from tab",
                "create table tab as (" +
                        "select timestamp_sequence('2024-01-01', 1000000) as ts, " +
                        "'A' as sym, " +
                        "case when x = 1 then 10.0 when x = 2 then 20.0 else 30.0 end as price, " +
                        "case when x = 1 then 100.0 when x = 2 then 0.0 else 300.0 end as volume " +
                        "from long_sequence(3)" +
                        ") timestamp(ts) partition by day",
                "ts",
                false,
                true
        );
    }

    @Test
    public void testVwemaExceptionAlphaMustBeBetween0And1() throws Exception {
        assertException(
                "select ts, price, volume, avg(price, 'alpha', 1.5, volume) over (order by ts) from tab",
                "create table tab (ts timestamp, price double, volume double) timestamp(ts)",
                46,
                "alpha must be between 0 (exclusive) and 1 (inclusive)"
        );
    }

    @Test
    public void testVwemaExceptionFramingNotSupported() throws Exception {
        assertException(
                "select ts, price, volume, avg(price, 'alpha', 0.5, volume) over (order by ts rows between 1 preceding and current row) from tab",
                "create table tab (ts timestamp, price double, volume double) timestamp(ts)",
                26,
                "avg() does not support framing; remove ROWS/RANGE clause"
        );
    }

    @Test
    public void testVwemaExceptionInvalidKind() throws Exception {
        assertException(
                "select ts, price, volume, avg(price, 'invalid', 0.5, volume) over (order by ts) from tab",
                "create table tab (ts timestamp, price double, volume double) timestamp(ts)",
                37,
                "invalid kind parameter: expected 'alpha', 'period', or a time unit (second, minute, hour, day, week)"
        );
    }

    @Test
    public void testVwemaExceptionKindCannotBeNull() throws Exception {
        assertException(
                "select ts, price, volume, avg(price, cast(null as string), 0.5, volume) over (order by ts) from tab",
                "create table tab (ts timestamp, price double, volume double) timestamp(ts)",
                37,
                "kind parameter cannot be null"
        );
    }

    @Test
    public void testVwemaExceptionKindMustBeConstant() throws Exception {
        assertException(
                "select ts, kind, price, volume, avg(price, kind, 0.5, volume) over (order by ts) from tab",
                "create table tab (ts timestamp, kind string, price double, volume double) timestamp(ts)",
                43,
                "kind parameter must be a constant"
        );
    }

    @Test
    public void testVwemaExceptionNegativeParameterValue() throws Exception {
        assertException(
                "select ts, price, volume, avg(price, 'period', -1, volume) over (order by ts) from tab",
                "create table tab (ts timestamp, price double, volume double) timestamp(ts)",
                47,
                "parameter value must be a positive number"
        );
    }

    @Test
    public void testVwemaExceptionOrderByRequired() throws Exception {
        assertException(
                "select ts, price, volume, avg(price, 'alpha', 0.5, volume) over () from tab",
                "create table tab (ts timestamp, price double, volume double) timestamp(ts)",
                26,
                "avg() requires ORDER BY"
        );
    }

    @Test
    public void testVwemaExceptionParameterMustBeConstant() throws Exception {
        assertException(
                "select ts, price, volume, avg(price, 'alpha', price, volume) over (order by ts) from tab",
                "create table tab (ts timestamp, price double, volume double) timestamp(ts)",
                46,
                "parameter value must be a constant"
        );
    }

    @Test
    public void testVwemaExceptionZeroParameterValue() throws Exception {
        assertException(
                "select ts, price, volume, avg(price, 'alpha', 0, volume) over (order by ts) from tab",
                "create table tab (ts timestamp, price double, volume double) timestamp(ts)",
                46,
                "parameter value must be a positive number"
        );
    }

    // ========================= Edge Case Tests for VwemaOverPartitionFunction =========================

    @Test
    public void testVwemaPeriodMode() throws Exception {
        // Test avg() VWEMA with period mode (alpha = 2 / (period + 1))
        // With period=2, alpha = 2/3
        assertQuery(
                """
                        ts\tprice\tvolume\tvwema
                        2024-01-01T00:00:00.000000Z\t10.0\t100.0\t10.0
                        2024-01-01T00:00:01.000000Z\t20.0\t200.0\t18.0
                        2024-01-01T00:00:02.000000Z\t30.0\t300.0\t27.39130434782609
                        """,
                "select ts, price, volume, avg(price, 'period', 2, volume) over (order by ts) as vwema from tab",
                "create table tab as (" +
                        "select timestamp_sequence('2024-01-01', 1000000) as ts, " +
                        "(x * 10.0) as price, " +
                        "(x * 100.0) as volume " +
                        "from long_sequence(3)" +
                        ") timestamp(ts) partition by day",
                "ts",
                false,
                true
        );
    }

    @Test
    public void testVwemaTimeWeightedMode() throws Exception {
        // Test avg() VWEMA with time-weighted mode
        assertQuery(
                """
                        ts\tprice\tvolume\tvwema
                        2024-01-01T00:00:01.000000Z\t10.0\t100.0\t10.0
                        2024-01-01T00:00:02.000000Z\t20.0\t200.0\t17.74600326439436
                        2024-01-01T00:00:03.000000Z\t30.0\t300.0\t27.053175178756817
                        """,
                "select ts, price, volume, avg(price, 'second', 1, volume) over (order by ts) as vwema from tab",
                "create table tab as (" +
                        "select dateadd('s', x::int, '2024-01-01'::timestamp) as ts, " +
                        "(x * 10.0) as price, " +
                        "(x * 100.0) as volume " +
                        "from long_sequence(3)" +
                        ") timestamp(ts) partition by day",
                "ts",
                false,
                true
        );
    }

    @Test
    public void testVwemaTimeWeightedModeExplainPlan() throws Exception {
        // VwemaTimeWeightedOverUnboundedRowsFrameFunction
        execute("create table tab (ts timestamp, price double, volume double) timestamp(ts)");
        assertPlanNoLeakCheck(
                "select ts, price, volume, avg(price, 'second', 1, volume) over (order by ts) as vwema from tab",
                """
                        Window
                          functions: [avg(price, 'second', 1.0, volume) over (rows between unbounded preceding and current row)]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """
        );
    }

    @Test
    public void testVwemaTimeWeightedModeNaNPrice() throws Exception {
        // L662 false via !isFinite(price) in VwemaTimeWeightedOverUnboundedRowsFrameFunction
        assertQuery(
                """
                        ts\tprice\tvolume\tvwema
                        2024-01-01T00:00:01.000000Z\tnull\t100.0\tnull
                        2024-01-01T00:00:02.000000Z\t20.0\t200.0\t20.0
                        2024-01-01T00:00:03.000000Z\t30.0\t300.0\t27.20469155611041
                        """,
                "select ts, price, volume, avg(price, 'second', 1, volume) over (order by ts) as vwema from tab",
                "create table tab as (" +
                        "select dateadd('s', x::int, '2024-01-01'::timestamp) as ts, " +
                        "case when x = 1 then null::double else (x * 10.0) end as price, " +
                        "(x * 100.0) as volume " +
                        "from long_sequence(3)" +
                        ") timestamp(ts) partition by day",
                "ts",
                false,
                true
        );
    }

    @Test
    public void testVwemaTimeWeightedModeNaNVolume() throws Exception {
        // L662 false via !isFinite(volume) in VwemaTimeWeightedOverUnboundedRowsFrameFunction
        assertQuery(
                """
                        ts\tprice\tvolume\tvwema
                        2024-01-01T00:00:01.000000Z\t10.0\tnull\tnull
                        2024-01-01T00:00:02.000000Z\t20.0\t200.0\t20.0
                        2024-01-01T00:00:03.000000Z\t30.0\t300.0\t27.20469155611041
                        """,
                "select ts, price, volume, avg(price, 'second', 1, volume) over (order by ts) as vwema from tab",
                "create table tab as (" +
                        "select dateadd('s', x::int, '2024-01-01'::timestamp) as ts, " +
                        "(x * 10.0) as price, " +
                        "case when x = 1 then null::double else (x * 100.0) end as volume " +
                        "from long_sequence(3)" +
                        ") timestamp(ts) partition by day",
                "ts",
                false,
                true
        );
    }

    @Test
    public void testVwemaTimeWeightedModeSameTimestamp() throws Exception {
        // L669 true: Same timestamp (dt <= 0) - uses alpha = 1.0 in VwemaTimeWeightedOverUnboundedRowsFrameFunction
        assertQuery(
                """
                        ts\tprice\tvolume\tvwema
                        2024-01-01T00:00:00.000000Z\t10.0\t100.0\t10.0
                        2024-01-01T00:00:00.000000Z\t20.0\t200.0\t20.0
                        """,
                "select ts, price, volume, avg(price, 'second', 1, volume) over (order by ts) as vwema from tab",
                "create table tab as (" +
                        "select '2024-01-01'::timestamp as ts, " +
                        "case when x = 1 then 10.0 else 20.0 end as price, " +
                        "case when x = 1 then 100.0 else 200.0 end as volume " +
                        "from long_sequence(2)" +
                        ") timestamp(ts) partition by day",
                "ts",
                false,
                true
        );
    }

    // ========================= Edge Case Tests for VwemaTimeWeightedOverPartitionFunction =========================

    @Test
    public void testVwemaTimeWeightedModeZeroVolume() throws Exception {
        // L662 false via volume <= 0 in VwemaTimeWeightedOverUnboundedRowsFrameFunction
        assertQuery(
                """
                        ts\tprice\tvolume\tvwema
                        2024-01-01T00:00:01.000000Z\t10.0\t0.0\tnull
                        2024-01-01T00:00:02.000000Z\t20.0\t200.0\t20.0
                        2024-01-01T00:00:03.000000Z\t30.0\t300.0\t27.20469155611041
                        """,
                "select ts, price, volume, avg(price, 'second', 1, volume) over (order by ts) as vwema from tab",
                "create table tab as (" +
                        "select dateadd('s', x::int, '2024-01-01'::timestamp) as ts, " +
                        "(x * 10.0) as price, " +
                        "case when x = 1 then 0.0 else (x * 100.0) end as volume " +
                        "from long_sequence(3)" +
                        ") timestamp(ts) partition by day",
                "ts",
                false,
                true
        );
    }

    @Test
    public void testVwemaTimeWeightedPartitionConsecutiveInvalidRows() throws Exception {
        // L486 hasValue == 0: Multiple consecutive invalid rows return NaN
        assertQuery(
                """
                        ts\tsym\tprice\tvolume\tvwema
                        2024-01-01T00:00:00.000000Z\tA\tnull\t100.0\tnull
                        2024-01-01T00:00:01.000000Z\tA\tnull\t200.0\tnull
                        2024-01-01T00:00:02.000000Z\tA\t30.0\t300.0\t30.0
                        """,
                "select ts, sym, price, volume, avg(price, 'second', 1, volume) over (partition by sym order by ts) as vwema from tab",
                "create table tab as (" +
                        "select timestamp_sequence('2024-01-01', 1000000) as ts, " +
                        "'A' as sym, " +
                        "case when x <= 2 then null::double else 30.0 end as price, " +
                        "case when x = 1 then 100.0 when x = 2 then 200.0 else 300.0 end as volume " +
                        "from long_sequence(3)" +
                        ") timestamp(ts) partition by day",
                "ts",
                false,
                true
        );
    }

    @Test
    public void testVwemaTimeWeightedPartitionExplainPlan() throws Exception {
        // VwemaTimeWeightedOverPartitionFunction
        execute("create table tab (ts timestamp, sym symbol, price double, volume double) timestamp(ts)");
        assertPlanNoLeakCheck(
                "select ts, sym, price, volume, avg(price, 'second', 1, volume) over (partition by sym order by ts) as vwema from tab",
                """
                        Window
                          functions: [avg(price, 'second', 1.0, volume) over (partition by [sym] rows between unbounded preceding and current row)]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """
        );
    }

    @Test
    public void testVwemaTimeWeightedPartitionFirstRowNaNPrice() throws Exception {
        // L431 false via !isFinite(price): First row has invalid price (NaN)
        assertQuery(
                """
                        ts\tsym\tprice\tvolume\tvwema
                        2024-01-01T00:00:00.000000Z\tA\tnull\t100.0\tnull
                        2024-01-01T00:00:01.000000Z\tA\t20.0\t200.0\t20.0
                        """,
                "select ts, sym, price, volume, avg(price, 'second', 1, volume) over (partition by sym order by ts) as vwema from tab",
                "create table tab as (" +
                        "select timestamp_sequence('2024-01-01', 1000000) as ts, " +
                        "'A' as sym, " +
                        "case when x = 1 then null::double else 20.0 end as price, " +
                        "case when x = 1 then 100.0 else 200.0 end as volume " +
                        "from long_sequence(2)" +
                        ") timestamp(ts) partition by day",
                "ts",
                false,
                true
        );
    }

    @Test
    public void testVwemaTimeWeightedPartitionFirstRowNaNVolume() throws Exception {
        // L431 false via !isFinite(volume): First row has invalid volume (NaN)
        assertQuery(
                """
                        ts\tsym\tprice\tvolume\tvwema
                        2024-01-01T00:00:00.000000Z\tA\t10.0\tnull\tnull
                        2024-01-01T00:00:01.000000Z\tA\t20.0\t200.0\t20.0
                        """,
                "select ts, sym, price, volume, avg(price, 'second', 1, volume) over (partition by sym order by ts) as vwema from tab",
                "create table tab as (" +
                        "select timestamp_sequence('2024-01-01', 1000000) as ts, " +
                        "'A' as sym, " +
                        "case when x = 1 then 10.0 else 20.0 end as price, " +
                        "case when x = 1 then null::double else 200.0 end as volume " +
                        "from long_sequence(2)" +
                        ") timestamp(ts) partition by day",
                "ts",
                false,
                true
        );
    }

    @Test
    public void testVwemaTimeWeightedPartitionInvalidAfterValid() throws Exception {
        // L479-482: Invalid value after valid - keeps previous VWEMA
        assertQuery(
                """
                        ts\tsym\tprice\tvolume\tvwema
                        2024-01-01T00:00:00.000000Z\tA\t10.0\t100.0\t10.0
                        2024-01-01T00:00:01.000000Z\tA\tnull\t200.0\t10.0
                        2024-01-01T00:00:02.000000Z\tA\t30.0\t300.0\t26.750527686273102
                        """,
                "select ts, sym, price, volume, avg(price, 'second', 1, volume) over (partition by sym order by ts) as vwema from tab",
                "create table tab as (" +
                        "select timestamp_sequence('2024-01-01', 1000000) as ts, " +
                        "'A' as sym, " +
                        "case when x = 1 then 10.0 when x = 2 then null::double else 30.0 end as price, " +
                        "case when x = 1 then 100.0 when x = 2 then 200.0 else 300.0 end as volume " +
                        "from long_sequence(3)" +
                        ") timestamp(ts) partition by day",
                "ts",
                false,
                true
        );
    }

    @Test
    public void testVwemaTimeWeightedPartitionNaNVolumeAfterValid() throws Exception {
        // L451 false: NaN volume on subsequent row - keeps previous VWEMA
        assertQuery(
                """
                        ts\tsym\tprice\tvolume\tvwema
                        2024-01-01T00:00:00.000000Z\tA\t10.0\t100.0\t10.0
                        2024-01-01T00:00:01.000000Z\tA\t20.0\tnull\t10.0
                        2024-01-01T00:00:02.000000Z\tA\t30.0\t300.0\t26.750527686273102
                        """,
                "select ts, sym, price, volume, avg(price, 'second', 1, volume) over (partition by sym order by ts) as vwema from tab",
                "create table tab as (" +
                        "select timestamp_sequence('2024-01-01', 1000000) as ts, " +
                        "'A' as sym, " +
                        "case when x = 1 then 10.0 when x = 2 then 20.0 else 30.0 end as price, " +
                        "case when x = 1 then 100.0 when x = 2 then null::double else 300.0 end as volume " +
                        "from long_sequence(3)" +
                        ") timestamp(ts) partition by day",
                "ts",
                false,
                true
        );
    }

    @Test
    public void testVwemaTimeWeightedPartitionSameTimestamp() throws Exception {
        // L460: Same timestamp (dt <= 0) - uses alpha = 1.0 (full weight to new value)
        assertQuery(
                """
                        ts\tsym\tprice\tvolume\tvwema
                        2024-01-01T00:00:00.000000Z\tA\t10.0\t100.0\t10.0
                        2024-01-01T00:00:00.000000Z\tA\t20.0\t200.0\t20.0
                        """,
                "select ts, sym, price, volume, avg(price, 'second', 1, volume) over (partition by sym order by ts) as vwema from tab",
                "create table tab as (" +
                        "select '2024-01-01'::timestamp as ts, " +
                        "'A' as sym, " +
                        "case when x = 1 then 10.0 else 20.0 end as price, " +
                        "case when x = 1 then 100.0 else 200.0 end as volume " +
                        "from long_sequence(2)" +
                        ") timestamp(ts) partition by day",
                "ts",
                false,
                true
        );
    }

    // ========================= Explain Plan Tests =========================

    @Test
    public void testVwemaTimeWeightedPartitionZeroVolume() throws Exception {
        // L439-443 & L479-482: Zero volume is treated as invalid
        assertQuery(
                """
                        ts\tsym\tprice\tvolume\tvwema
                        2024-01-01T00:00:00.000000Z\tA\t10.0\t0.0\tnull
                        2024-01-01T00:00:01.000000Z\tA\t20.0\t200.0\t20.0
                        """,
                "select ts, sym, price, volume, avg(price, 'second', 1, volume) over (partition by sym order by ts) as vwema from tab",
                "create table tab as (" +
                        "select timestamp_sequence('2024-01-01', 1000000) as ts, " +
                        "'A' as sym, " +
                        "case when x = 1 then 10.0 else 20.0 end as price, " +
                        "case when x = 1 then 0.0 else 200.0 end as volume " +
                        "from long_sequence(2)" +
                        ") timestamp(ts) partition by day",
                "ts",
                false,
                true
        );
    }

    @Test
    public void testVwemaTimeWeightedPartitionZeroVolumeAfterValid() throws Exception {
        // L451 false: Zero volume on subsequent row - keeps previous VWEMA
        assertQuery(
                """
                        ts\tsym\tprice\tvolume\tvwema
                        2024-01-01T00:00:00.000000Z\tA\t10.0\t100.0\t10.0
                        2024-01-01T00:00:01.000000Z\tA\t20.0\t0.0\t10.0
                        2024-01-01T00:00:02.000000Z\tA\t30.0\t300.0\t26.750527686273102
                        """,
                "select ts, sym, price, volume, avg(price, 'second', 1, volume) over (partition by sym order by ts) as vwema from tab",
                "create table tab as (" +
                        "select timestamp_sequence('2024-01-01', 1000000) as ts, " +
                        "'A' as sym, " +
                        "case when x = 1 then 10.0 when x = 2 then 20.0 else 30.0 end as price, " +
                        "case when x = 1 then 100.0 when x = 2 then 0.0 else 300.0 end as volume " +
                        "from long_sequence(3)" +
                        ") timestamp(ts) partition by day",
                "ts",
                false,
                true
        );
    }

    @Test
    public void testVwemaTimeWeightedWithPartitionBy() throws Exception {
        // Test avg() VWEMA with time-weighted mode and partition by
        // This exercises VwemaTimeWeightedOverPartitionFunction
        assertQuery(
                """
                        ts\tsym\tprice\tvolume\tvwema
                        2024-01-01T00:00:00.000000Z\tA\t10.0\t100.0\t10.0
                        2024-01-01T00:00:01.000000Z\tB\t15.0\t150.0\t15.0
                        2024-01-01T00:00:02.000000Z\tA\t20.0\t200.0\t19.274211165042463
                        2024-01-01T00:00:03.000000Z\tB\t25.0\t250.0\t24.1415149749738
                        """,
                "select ts, sym, price, volume, avg(price, 'second', 1, volume) over (partition by sym order by ts) as vwema from tab",
                "create table tab as (" +
                        "select timestamp_sequence('2024-01-01', 1000000) as ts, " +
                        "case when x % 2 = 1 then 'A' else 'B' end as sym, " +
                        "case when x = 1 then 10.0 when x = 2 then 15.0 when x = 3 then 20.0 else 25.0 end as price, " +
                        "case when x = 1 then 100.0 when x = 2 then 150.0 when x = 3 then 200.0 else 250.0 end as volume " +
                        "from long_sequence(4)" +
                        ") timestamp(ts) partition by day",
                "ts",
                false,
                true
        );
    }

    @Test
    public void testVwemaWithPartitionBy() throws Exception {
        // Test avg() VWEMA with partition by
        assertQuery(
                """
                        ts\tsym\tprice\tvolume\tvwema
                        2024-01-01T00:00:00.000000Z\tA\t10.0\t100.0\t10.0
                        2024-01-01T00:00:01.000000Z\tB\t15.0\t150.0\t15.0
                        2024-01-01T00:00:02.000000Z\tA\t20.0\t200.0\t16.666666666666668
                        2024-01-01T00:00:03.000000Z\tB\t25.0\t250.0\t21.25
                        """,
                "select ts, sym, price, volume, avg(price, 'alpha', 0.5, volume) over (partition by sym order by ts) as vwema from tab",
                "create table tab as (" +
                        "select timestamp_sequence('2024-01-01', 1000000) as ts, " +
                        "case when x % 2 = 1 then 'A' else 'B' end as sym, " +
                        "case when x = 1 then 10.0 when x = 2 then 15.0 when x = 3 then 20.0 else 25.0 end as price, " +
                        "case when x = 1 then 100.0 when x = 2 then 150.0 when x = 3 then 200.0 else 250.0 end as volume " +
                        "from long_sequence(4)" +
                        ") timestamp(ts) partition by day",
                "ts",
                false,
                true
        );
    }
}
