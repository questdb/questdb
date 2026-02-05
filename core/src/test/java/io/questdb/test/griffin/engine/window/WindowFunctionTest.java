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

import io.questdb.PropertyKey;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.window.AvgDoubleWindowFunctionFactory;
import io.questdb.griffin.engine.functions.window.CountConstWindowFunctionFactory;
import io.questdb.griffin.engine.functions.window.CountDoubleWindowFunctionFactory;
import io.questdb.griffin.engine.functions.window.CountSymbolWindowFunctionFactory;
import io.questdb.griffin.engine.functions.window.CountVarcharWindowFunctionFactory;
import io.questdb.griffin.engine.functions.window.DenseRankFunctionFactory;
import io.questdb.griffin.engine.functions.window.FirstValueDoubleWindowFunctionFactory;
import io.questdb.griffin.engine.functions.window.KSumDoubleWindowFunctionFactory;
import io.questdb.griffin.engine.functions.window.LagDateFunctionFactory;
import io.questdb.griffin.engine.functions.window.LagDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.window.LagLongFunctionFactory;
import io.questdb.griffin.engine.functions.window.LagTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.window.LastValueDoubleWindowFunctionFactory;
import io.questdb.griffin.engine.functions.window.LeadDateFunctionFactory;
import io.questdb.griffin.engine.functions.window.LeadDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.window.LeadLongFunctionFactory;
import io.questdb.griffin.engine.functions.window.LeadTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.window.MaxDoubleWindowFunctionFactory;
import io.questdb.griffin.engine.functions.window.MinDoubleWindowFunctionFactory;
import io.questdb.griffin.engine.functions.window.RankFunctionFactory;
import io.questdb.griffin.engine.functions.window.RowNumberFunctionFactory;
import io.questdb.griffin.engine.functions.window.SumDoubleWindowFunctionFactory;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class WindowFunctionTest extends AbstractCairoTest {
    private static final List<String> FRAME_FUNCTIONS;
    private static final String[][] FRAME_FUNCTIONS_PARAMETER_COLUMN_NAME = new String[][]{
            {
                    "i", "j" // avg
            },
            {
                    "i", "j" // sum
            },
            {
                    "i", "j" // ksum
            },
            {
                    "i", "j", // first_value
            },
            {
                    "i", "j", // first_value ignore nulls
            },
            {
                    "i", "j", // first_value respect nulls
            },
            {
                    "*", "j", "s", "d", "c" // count
            },
            {
                    "j" // max
            },
            {
                    "j" // min
            },
            {
                    "i", "j", // last_value
            },
            {
                    "i", "j", // last_value ignore nulls
            },
            {
                    "i", "j", // last_value respect nulls
            }
    };
    private static final List<String> FRAME_TYPES = Arrays.asList("rows  ", "groups", "range ");
    private static final List<String> WINDOW_ONLY_FUNCTIONS;
    private final TestTimestampType timestampType;

    public WindowFunctionTest() {
        this.timestampType = TestUtils.getTimestampType();
    }

    @Test
    public void testAggregateFunctionInPartitionByFails() throws Exception {
        assertException(
                """
                        SELECT pickup_datetime, avg(total_amount) OVER (PARTITION BY avg(total_amount)
                          ORDER BY pickup_datetime
                          RANGE BETWEEN '7' PRECEDING AND CURRENT ROW) moving_average_1w
                        FROM trips
                        WHERE pickup_datetime >= '2018-12-30' and pickup_datetime <= '2018-12-31'
                        SAMPLE BY 1d""",
                "create table trips as " +
                        "(" +
                        "select" +
                        " rnd_double(42) total_amount," +
                        " timestamp_sequence(0, 100000000000) pickup_datetime" +
                        " from long_sequence(10)" +
                        ") timestamp(pickup_datetime) partition by day",
                24,
                "aggregate functions in partition by are not supported"
        );
    }

    @Test
    public void testCachedWindowFactoryMaintainsOrderOfRecordsWithSameTimestamp1() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table nodts_tab (ts #TIMESTAMP, val int)", timestampType.getTypeName());
            execute("insert into nodts_tab values (0, 1)");
            execute("insert into nodts_tab values (0, 2)");

            String noDtsResult = replaceTimestampSuffix("""
                    ts\tval\tavg\tcount\tcount1\tcount2\tcount3\tmax\tmin
                    1970-01-01T00:00:00.000000Z\t1\t1.0\t1\t1\t1\t1\t1\t1
                    1970-01-01T00:00:00.000000Z\t1\t1.0\t2\t2\t2\t2\t1\t1
                    1970-01-01T00:00:00.000000Z\t2\t1.3333333333333333\t3\t3\t3\t3\t2\t1
                    1970-01-01T00:00:00.000000Z\t2\t1.5\t4\t4\t4\t4\t2\t1
                    """, timestampType.getTypeName());

            assertQueryNoLeakCheck(
                    noDtsResult,
                    "SELECT T1.ts, T1.val, avg(T1.val) OVER (PARTITION BY 1=1 ORDER BY T1.ts), " +
                            "count(*) OVER (PARTITION BY 1=1 ORDER BY T1.ts), count(*) OVER (PARTITION BY 1=1 ORDER BY T1.ts), " +
                            "count(T1.val) OVER (PARTITION BY 1=1 ORDER BY T1.ts), count(*) OVER (PARTITION BY 1=1 ORDER BY T1.ts), " +
                            "max(T1.val) OVER (PARTITION BY 1=1 ORDER BY T1.ts), " +
                            "min(T1.val) OVER (PARTITION BY 1=1 ORDER BY T1.ts) " +
                            "FROM nodts_tab AS T1 " +
                            "CROSS JOIN nodts_tab AS T2",
                    null,
                    true,
                    true
            );

            assertQueryNoLeakCheck(
                    noDtsResult,
                    "SELECT T1.ts, T1.val, avg(T1.val) OVER (PARTITION BY 1=1 ORDER BY T1.ts desc), " +
                            "count(*) OVER (PARTITION BY 1=1 ORDER BY T1.ts), count(*) OVER (PARTITION BY 1=1 ORDER BY T1.ts), " +
                            "count(T1.val) OVER (PARTITION BY 1=1 ORDER BY T1.ts), count(*) OVER (PARTITION BY 1=1 ORDER BY T1.ts), " +
                            "max(T1.val) OVER (PARTITION BY 1=1 ORDER BY T1.ts desc), " +
                            "min(T1.val) OVER (PARTITION BY 1=1 ORDER BY T1.ts desc) " +
                            "FROM nodts_tab AS T1 " +
                            "CROSS JOIN nodts_tab AS T2",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testCachedWindowFactoryMaintainsOrderOfRecordsWithSameTimestamp2() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, val int) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab values (0, 1)");
            execute("insert into tab values (0, 1)");
            execute("insert into tab values (0, 2)");
            execute("insert into tab values (0, 2)");

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\tval\tavg\tcount\tmax\tmin
                            1970-01-01T00:00:00.000000Z\t1\t1.0\t1\t1\t1
                            1970-01-01T00:00:00.000000Z\t1\t1.0\t2\t1\t1
                            1970-01-01T00:00:00.000000Z\t2\t1.3333333333333333\t3\t2\t1
                            1970-01-01T00:00:00.000000Z\t2\t1.5\t4\t2\t1
                            """, timestampType.getTypeName()),
                    "SELECT ts, val, avg(val) OVER (PARTITION BY 1=1 ORDER BY ts), " +
                            "count(val) OVER (PARTITION BY 1=1 ORDER BY ts), " +
                            "max(val) OVER (PARTITION BY 1=1 ORDER BY ts), " +
                            "min(val) OVER (PARTITION BY 1=1 ORDER BY ts) " +
                            "FROM tab",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\tval\tavg\tcount\tmax\tmin
                            1970-01-01T00:00:00.000000Z\t2\t2.0\t1\t2\t2
                            1970-01-01T00:00:00.000000Z\t2\t2.0\t2\t2\t2
                            1970-01-01T00:00:00.000000Z\t1\t1.6666666666666667\t3\t2\t1
                            1970-01-01T00:00:00.000000Z\t1\t1.5\t4\t2\t1
                            """, timestampType.getTypeName()),
                    "SELECT ts, val, avg(val) OVER (PARTITION BY 1=1 ORDER BY ts DESC), " +
                            "count(val) OVER (PARTITION BY 1=1 ORDER BY ts DESC), " +
                            "max(val) OVER (PARTITION BY 1=1 ORDER BY ts DESC), " +
                            "min(val) OVER (PARTITION BY 1=1 ORDER BY ts DESC) " +
                            "FROM tab " +
                            "ORDER BY ts DESC",
                    "ts###desc",
                    false,
                    true
            );
        });
    }

    @Test
    public void testFirstValueDoubleOverPartitionedRangeWithLargeFrame() throws Exception {
        assertMemoryLeak(() -> {
            // Test similar to testMaxDoubleOverPartitionedRangeWithLargeFrame but for first_value(double)
            // This tests boundary conditions with large ranges that trigger buffer growth
            execute("create table tab (ts timestamp, i long, j long, s symbol, d double) timestamp(ts)");

            // Trigger per-partition buffers growth and free list usage
            execute("insert into tab select " +
                    "x::timestamp, " +
                    "x/10000, " +
                    "case when x % 3 = 0 THEN NULL ELSE x END, " +
                    "'k' || (x%5) ::symbol, " +
                    "x*2::double " +
                    "from long_sequence(39999)");

            // Trigger removal of rows below lo boundary AND resize of buffer
            execute("insert into tab select " +
                    "(100000+x)::timestamp, " +
                    "(100000+x)%4, " +
                    "case when x % 3 = 0 THEN NULL ELSE 100000 + x END, " +
                    "'k' || (x%20) ::symbol, " +
                    "x*2::double " +
                    "from long_sequence(360000)");

            // Test first_value(double) with window function over partitioned data with large frame
            // Test the last row of each partition to verify boundary conditions
            assertSql(
                    """
                            i\tts\tfirst_d_window
                            0\t1970-01-01T00:00:00.460000Z\t560000.0
                            1\t1970-01-01T00:00:00.459997Z\t559994.0
                            2\t1970-01-01T00:00:00.459998Z\t559996.0
                            3\t1970-01-01T00:00:00.459999Z\t559998.0
                            """,
                    "select i, ts, first_d_window from (" +
                            "select i, ts, " +
                            "first_value(d) over (partition by i order by ts range between 80000 preceding and current row) as first_d_window, " +
                            "row_number() over (partition by i order by ts desc) as rn " +
                            "from tab " +
                            "where i < 4" +
                            ") where rn = 1 " +
                            "order by i"
            );

            // Test with ignore nulls - should handle null values properly in large datasets
            assertSql(
                    """
                            i\tts\tfirst_d_ignore_nulls
                            0\t1970-01-01T00:00:00.460000Z\t560000.0
                            1\t1970-01-01T00:00:00.459997Z\t559994.0
                            2\t1970-01-01T00:00:00.459998Z\t559996.0
                            3\t1970-01-01T00:00:00.459999Z\t559998.0
                            """,
                    "select i, ts, first_d_ignore_nulls from (" +
                            "select i, ts, " +
                            "first_value(d) ignore nulls over (partition by i order by ts range between 80000 preceding and current row) as first_d_ignore_nulls, " +
                            "row_number() over (partition by i order by ts desc) as rn " +
                            "from tab " +
                            "where i < 4" +
                            ") where rn = 1 " +
                            "order by i"
            );

            // Cross-check: verify the first value in the window range
            assertSql(
                    """
                            i\tfirst_d
                            0\t560000.0
                            1\t559994.0
                            2\t559996.0
                            3\t559998.0
                            """,
                    """
                             select main_data.i, min(main_data.d) as first_d
                              from (
                                select data.d, data.i, data.ts
                                from (select i, max(ts) as max_ts from tab group by i) cnt
                                join tab data on cnt.i = data.i and data.ts >= (cnt.max_ts - 80000)
                              ) main_data
                              join (
                                select wd.i, min(wd.d) as min_d
                                from (
                                  select data.d, data.i
                                  from (select i, max(ts) as max_ts from tab group by i) cnt
                                  join tab data on cnt.i = data.i and data.ts >= (cnt.max_ts - 80000)
                                ) wd
                                group by wd.i
                              ) min_values on main_data.i = min_values.i and main_data.d = min_values.min_d
                              where main_data.i in (0,1,2,3)
                              group by main_data.i
                              order by main_data.i\
                            """
            );
        });
    }

    @Test
    public void testFrameFunctionDoesNotAcceptFollowingInNonDefaultFrameDefinition() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, j long, s symbol, d double, c VARCHAR) timestamp(ts)", timestampType.getTypeName());

            for (int i = 0, size = FRAME_FUNCTIONS.size(); i < size; i++) {
                String func = FRAME_FUNCTIONS.get(i);
                for (String column : FRAME_FUNCTIONS_PARAMETER_COLUMN_NAME[i]) {
                    assertExceptionNoLeakCheck(
                            "select #FUNCT_NAME over (partition by i rows between 10 following and 20 following) from tab".replace("#FUNCT_NAME", func).replace("#COLUMN", column),
                            73,
                            "frame start supports UNBOUNDED PRECEDING, _number_ PRECEDING and CURRENT ROW only"
                    );
                    assertExceptionNoLeakCheck(
                            "select #FUNCT_NAME over (partition by i rows between current row and 10 following) from tab".replace("#FUNCT_NAME", func).replace("#COLUMN", column),
                            89,
                            "frame end supports _number_ PRECEDING and CURRENT ROW only"
                    );
                }
            }
        });
    }

    @Test
    public void testFrameFunctionOverNonPartitionedRangeWithLargeFrame() throws Exception {
        assertMemoryLeak(() -> {
            //default buffer size holds 65k entries
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, j long, s symbol, d double, c VARCHAR) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab select x::timestamp, x/4, case when x % 3 = 0 THEN NULL ELSE x END, 'k' || (x%5) ::symbol, x*2::double, 'k' || x from long_sequence(40000)");
            //trigger removal of rows below lo boundary AND resize of buffer
            execute("insert into tab select (100000+x)::timestamp, x/4, case when x % 30 = 0 THEN NULL ELSE x END, 'k' || (x%5) ::symbol, x*2::double, 'k' || x from long_sequence(90000)");

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tavg\tsum\tfirst_value\tlast_value\tfirst_value_ignore_nulls\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                            1970-01-01T00:00:00.189996Z\t22499\t89996\t89995.5\t179991.0\t89995\t89996\t89995\t89996\t2\t2\t2\t2\t2\t89996\t89995
                            1970-01-01T00:00:00.189997Z\t22499\t89997\t89996.5\t179993.0\t89996\t89997\t89996\t89997\t2\t2\t2\t2\t2\t89997\t89996
                            1970-01-01T00:00:00.189998Z\t22499\t89998\t89997.5\t179995.0\t89997\t89998\t89997\t89998\t2\t2\t2\t2\t2\t89998\t89997
                            1970-01-01T00:00:00.189999Z\t22499\t89999\t89998.5\t179997.0\t89998\t89999\t89998\t89999\t2\t2\t2\t2\t2\t89999\t89998
                            1970-01-01T00:00:00.190000Z\t22500\tnull\t89999.0\t89999.0\t89999\tnull\t89999\t89999\t2\t1\t2\t2\t2\t89999\t89999
                            """),
                    "select * from (" +
                            "select ts, i, j, " +
                            "avg(j) over (order by ts range between 1 microsecond preceding and current row), " +
                            "sum(j) over (order by ts range between 1 microsecond preceding and current row), " +
                            "first_value(j) over (order by ts range between 1 microsecond preceding and current row), " +
                            "last_value(j) over (order by ts range between 1 microsecond preceding and current row), " +
                            "first_value(j) ignore nulls over (order by ts range between 1 microsecond preceding and current row), " +
                            "last_value(j) ignore nulls over (order by ts range between 1 microsecond preceding and current row), " +
                            "count(*) over (order by ts range between 1 microsecond preceding and current row), " +
                            "count(j) over (order by ts range between 1 microsecond preceding and current row), " +
                            "count(s) over (order by ts range between 1 microsecond preceding and current row), " +
                            "count(d) over (order by ts range between 1 microsecond preceding and current row), " +
                            "count(c) over (order by ts range between 1 microsecond preceding and current row), " +
                            "max(j) over (order by ts range between 1 microsecond preceding and current row), " +
                            "min(j) over (order by ts range between 1 microsecond preceding and current row) " +
                            "from tab), " +
                            " limit -5",
                    "ts",
                    false,
                    true,
                    false
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tavg\tsum\tfirst_value\tlast_value\tfirst_value_ignore_nulls\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                            1970-01-01T00:00:00.189996Z\t22499\t89996\t89495.68769389865\t8.654233E7\t88996\t89995\t88996\t89995\t1000\t967\t1000\t1000\t1000\t89995\t88996
                            1970-01-01T00:00:00.189997Z\t22499\t89997\t89496.72182006204\t8.654333E7\t88997\t89996\t88997\t89996\t1000\t967\t1000\t1000\t1000\t89996\t88997
                            1970-01-01T00:00:00.189998Z\t22499\t89998\t89497.75594622544\t8.654433E7\t88998\t89997\t88998\t89997\t1000\t967\t1000\t1000\t1000\t89997\t88998
                            1970-01-01T00:00:00.189999Z\t22499\t89999\t89498.79007238883\t8.654533E7\t88999\t89998\t88999\t89998\t1000\t967\t1000\t1000\t1000\t89998\t88999
                            1970-01-01T00:00:00.190000Z\t22500\tnull\t89499.82419855222\t8.654633E7\t89000\t89999\t89000\t89999\t1000\t967\t1000\t1000\t1000\t89999\t89000
                            """),
                    "select * from (" +
                            "select ts, i, j, " +
                            "avg(j) over (order by ts range between 1 millisecond preceding and 1 microsecond preceding), " +
                            "sum(j) over (order by ts range between 1 millisecond preceding and 1 microsecond preceding), " +
                            "first_value(j) over (order by ts range between 1 millisecond preceding and 1 microsecond preceding), " +
                            "last_value(j) over (order by ts range between 1 millisecond preceding and 1 microsecond preceding), " +
                            "first_value(j) ignore nulls over (order by ts range between 1 millisecond preceding and 1 microsecond preceding), " +
                            "last_value(j) ignore nulls over (order by ts range between 1 millisecond preceding and 1 microsecond preceding), " +
                            "count(*) over (order by ts range between 1 millisecond preceding and 1 microsecond preceding), " +
                            "count(j) over (order by ts range between 1 millisecond preceding and 1 microsecond preceding), " +
                            "count(s) over (order by ts range between 1 millisecond preceding and 1 microsecond preceding), " +
                            "count(d) over (order by ts range between 1 millisecond preceding and 1 microsecond preceding), " +
                            "count(c) over (order by ts range between 1 millisecond preceding and 1 microsecond preceding), " +
                            "max(j) over (order by ts range between 1 millisecond preceding and 1 microsecond preceding), " +
                            "min(j) over (order by ts range between 1 millisecond preceding and 1 microsecond preceding) " +
                            "from tab), " +
                            " limit -5",
                    "ts",
                    false,
                    true,
                    false
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tavg\tsum\tfirst_value\tlast_value\tfirst_value_ignore_nulls\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                            1970-01-01T00:00:00.189996Z\t22499\t89996\t20000.250009374882\t5.33346667E8\t1\t40000\t1\t40000\t40000\t26667\t40000\t40000\t40000\t40000\t1
                            1970-01-01T00:00:00.189997Z\t22499\t89997\t20000.250009374882\t5.33346667E8\t1\t40000\t1\t40000\t40000\t26667\t40000\t40000\t40000\t40000\t1
                            1970-01-01T00:00:00.189998Z\t22499\t89998\t20000.250009374882\t5.33346667E8\t1\t40000\t1\t40000\t40000\t26667\t40000\t40000\t40000\t40000\t1
                            1970-01-01T00:00:00.189999Z\t22499\t89999\t20000.250009374882\t5.33346667E8\t1\t40000\t1\t40000\t40000\t26667\t40000\t40000\t40000\t40000\t1
                            1970-01-01T00:00:00.190000Z\t22500\tnull\t20000.250009374882\t5.33346667E8\t1\t40000\t1\t40000\t40000\t26667\t40000\t40000\t40000\t40000\t1
                            """),
                    "select * from (" +
                            "select ts, i, j, " +
                            "avg(j) over (order by ts range between 1 day preceding and 100 millisecond preceding), " +
                            "sum(j) over (order by ts range between 1 day preceding and 100 millisecond preceding), " +
                            "first_value(j) over (order by ts range between 1 day preceding and 100 millisecond preceding), " +
                            "last_value(j) over (order by ts range between 1 day preceding and 100 millisecond preceding), " +
                            "first_value(j) ignore nulls over (order by ts range between 1 day preceding and 100 millisecond preceding), " +
                            "last_value(j) ignore nulls over (order by ts range between 1 day preceding and 100 millisecond preceding), " +
                            "count(*) over (order by ts range between 1 day preceding and 100 millisecond preceding), " +
                            "count(j) over (order by ts range between 1 day preceding and 100 millisecond preceding), " +
                            "count(s) over (order by ts range between 1 day preceding and 100 millisecond preceding), " +
                            "count(d) over (order by ts range between 1 day preceding and 100 millisecond preceding), " +
                            "count(c) over (order by ts range between 1 day preceding and 100 millisecond preceding), " +
                            "max(j) over (order by ts range between 1 day preceding and 100 millisecond preceding), " +
                            "min(j) over (order by ts range between 1 day preceding and 100 millisecond preceding) " +
                            "from tab), " +
                            " limit -5",
                    "ts",
                    false,
                    true,
                    false
            );

            execute("truncate table tab");
            // trigger buffer resize
            execute("insert into tab select (100000+x)::timestamp, x/4, case when x % 3 = 0 THEN NULL ELSE x END, 'k' || (x%5) ::symbol, x*2::double, 'k' || x from long_sequence(90000)");

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tavg\tsum\tfirst_value\tlast_value\tfirst_value_ignore_nulls\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                            1970-01-01T00:00:00.189996Z\t22499\t89996\t89496.0\t5.9783328E7\t88996\t89996\t88996\t89996\t1001\t668\t1001\t1001\t1001\t89996\t88996
                            1970-01-01T00:00:00.189997Z\t22499\tnull\t89496.74962518741\t5.9694332E7\t88997\tnull\t88997\t89996\t1001\t667\t1001\t1001\t1001\t89996\t88997
                            1970-01-01T00:00:00.189998Z\t22499\t89998\t89498.25037481259\t5.9695333E7\tnull\t89998\t88999\t89998\t1001\t667\t1001\t1001\t1001\t89998\t88999
                            1970-01-01T00:00:00.189999Z\t22499\t89999\t89499.0\t5.9785332E7\t88999\t89999\t88999\t89999\t1001\t668\t1001\t1001\t1001\t89999\t88999
                            1970-01-01T00:00:00.190000Z\t22500\tnull\t89499.74962518741\t5.9696333E7\t89000\tnull\t89000\t89999\t1001\t667\t1001\t1001\t1001\t89999\t89000
                            """),
                    "select * from (" +
                            "select ts, i, j, " +
                            "avg(j) over (order by ts range between 1 millisecond preceding and current row), " +
                            "sum(j) over (order by ts range between 1 millisecond preceding and current row), " +
                            "first_value(j) over (order by ts range between 1 millisecond preceding and current row), " +
                            "last_value(j) over (order by ts range between 1 millisecond preceding and current row), " +
                            "first_value(j) ignore nulls over (order by ts range between 1 millisecond preceding and current row), " +
                            "last_value(j) ignore nulls over (order by ts range between 1 millisecond preceding and current row), " +
                            "count(*) over (order by ts range between 1 millisecond preceding and current row), " +
                            "count(j) over (order by ts range between 1 millisecond preceding and current row), " +
                            "count(s) over (order by ts range between 1 millisecond preceding and current row), " +
                            "count(d) over (order by ts range between 1 millisecond preceding and current row), " +
                            "count(c) over (order by ts range between 1 millisecond preceding and current row), " +
                            "max(j) over (order by ts range between 1 millisecond preceding and current row), " +
                            "min(j) over (order by ts range between 1 millisecond preceding and current row) " +
                            "from tab), " +
                            " limit -5",
                    "ts",
                    false,
                    true,
                    false
            );
        });
    }

    @Test
    public void testFrameFunctionOverNonPartitionedRowsWithLargeFrame() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, j long, s symbol, d double, c VARCHAR) timestamp(ts)", timestampType.getTypeName());

            execute("insert into tab select x::timestamp, x/10000, case when x % 3 = 0 THEN NULL ELSE x END, 'k' || (x%10) ::symbol, x::double, 'k' || x from long_sequence(39999)");
            execute("insert into tab select (100000+x)::timestamp, (100000+x)%3, case when x % 3 = 0 THEN NULL ELSE 100000 + x END, 'k' || (x%10) ::symbol, x::double, 'k' || x from long_sequence(4*90000)");

            // cross-check with re-write using aggregate functions
            String expected = replaceTimestampSuffix("""
                    ts\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                    1970-01-01T00:00:00.460000Z\t419999.5\t2.2400253333E10\t380000.0\t380000.0\tnull\t459999.0\t80001\t53334\t80001\t80001\t80001\t459999.0\t380000.0
                    """);
            assertSql(
                    expected,
                    " select" +
                            " max(ts) as ts," +
                            " avg(j) as avg," +
                            " sum(j::double) as sum," +
                            " last(j::double) as first_value," +
                            " last_not_null(j::double) as first_value_ignore_nulls," +
                            " first(j::double) as last_value," +
                            " first_not_null(j::double) as last_value_ignore_nulls," +
                            " count(*) as count," +
                            " count(j::double) as count1," +
                            " count(s) as count2," +
                            " count(d) as count3," +
                            " count(c) as count4," +
                            " max(j::double) as max," +
                            " min(j::double) as min " +
                            "from " +
                            "(select" +
                            " ts, i, j, s, d, c," +
                            " row_number() over (order by ts desc) as rn" +
                            " from tab order by ts desc" +
                            ") " +
                            "where rn between 1 and 80001 "
            );

            String expected2 = replaceTimestampSuffix("""
                    ts\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                    1970-01-01T00:00:00.460000Z\t419999.5\t2.2400253333E10\t380000\t380000\tnull\t459999\t80001\t53334\t80001\t80001\t80001\t459999\t380000
                    """);
            assertQueryNoLeakCheck(
                    expected2,
                    "select * from (" +
                            "select * from " +
                            "(select ts, " +
                            "avg(j) over (order by ts rows between 80000 preceding and current row), " +
                            "sum(j) over (order by ts rows between 80000 preceding and current row), " +
                            "first_value(j) over (order by ts rows between 80000 preceding and current row), " +
                            "first_value(j) ignore nulls over (order by ts rows between 80000 preceding and current row), " +
                            "last_value(j) over (order by ts rows between 80000 preceding and current row), " +
                            "last_value(j) ignore nulls over (order by ts rows between 80000 preceding and current row), " +
                            "count(*) over (order by ts rows between 80000 preceding and current row), " +
                            "count(j) over (order by ts rows between 80000 preceding and current row), " +
                            "count(s) over (order by ts rows between 80000 preceding and current row), " +
                            "count(d) over (order by ts rows between 80000 preceding and current row), " +
                            "count(c) over (order by ts rows between 80000 preceding and current row), " +
                            "max(j) over (order by ts rows between 80000 preceding and current row), " +
                            "min(j) over (order by ts rows between 80000 preceding and current row) " +
                            "from tab) " +
                            "limit -1) ",
                    "ts",
                    false,
                    true,
                    false
            );
        });
    }

    @Test
    public void testFrameFunctionOverNonPartitionedRowsWithLargeFrameRandomData() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, j long, s symbol, d double, c VARCHAR) timestamp(ts)", timestampType.getTypeName());

            execute("insert into tab select x::timestamp, x/10000, case when x % 3 = 0 THEN NULL ELSE x END, 'k' || (x%5) ::symbol, x*2::double, 'k' || x from long_sequence(39999)");
            execute("insert into tab select (100000+x)::timestamp, rnd_long(1,10000,10), rnd_long(1,100000,10), 'k' || (x%5) ::symbol, x*2::double, 'k' || x from long_sequence(1000000)");

            // cross-check with re-write using aggregate functions
            String expected = replaceTimestampSuffix("""
                    ts\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                    1970-01-01T00:00:01.100000Z\t49980.066958378644\t3.815028491E9\t2073.0\t2073.0\t46392.0\t46392.0\t80001\t76331\t80001\t80001\t80001\t100000.0\t3.0
                    """);
            assertSql(
                    expected,
                    " select max(ts) as ts, avg(j) as avg, sum(j::double) as sum, last(j::double) as first_value," +
                            "last_not_null(j::double) as first_value_ignore_nulls, " +
                            "first(j::double) as last_value, first_not_null(j::double) as last_value_ignore_nulls, " +
                            "count(*) as count, count(j::double) as count1, count(s) as count2, count(d) as count3, count(c) as count4, " +
                            "max(j::double) as max, min(j::double) as min " +
                            "from " +
                            "( select ts, i, j, s, d, c, row_number() over (order by ts desc) as rn from tab order by ts desc) " +
                            "where rn between 1 and 80001 "
            );

            String expected2 = replaceTimestampSuffix("""
                    ts\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                    1970-01-01T00:00:01.100000Z\t49980.066958378644\t3.815028491E9\t2073\t2073\t46392\t46392\t80001\t76331\t80001\t80001\t80001\t100000\t3
                    """);

            assertQueryNoLeakCheck(
                    expected2,
                    "select * from (" +
                            "select * from (select ts, " +
                            "avg(j) over (order by ts rows between 80000 preceding and current row), " +
                            "sum(j) over (order by ts rows between 80000 preceding and current row), " +
                            "first_value(j) over (order by ts rows between 80000 preceding and current row), " +
                            "first_value(j) ignore nulls over (order by ts rows between 80000 preceding and current row), " +
                            "last_value(j) over (order by ts rows between 80000 preceding and current row), " +
                            "last_value(j) ignore nulls over (order by ts rows between 80000 preceding and current row), " +
                            "count(*) over (order by ts rows between 80000 preceding and current row), " +
                            "count(j) over (order by ts rows between 80000 preceding and current row), " +
                            "count(s) over (order by ts rows between 80000 preceding and current row), " +
                            "count(d) over (order by ts rows between 80000 preceding and current row), " +
                            "count(c) over (order by ts rows between 80000 preceding and current row), " +
                            "max(j) over (order by ts rows between 80000 preceding and current row), " +
                            "min(j) over (order by ts rows between 80000 preceding and current row) " +
                            "from tab) limit -1)",
                    "ts",
                    false,
                    true,
                    false
            );
        });
    }

    @Test
    public void testFrameFunctionOverPartitionedRangeWithLargeFrame() throws Exception {
        assertMemoryLeak(() -> {
            // default buffer size holds 65k entries in total, 32 per partition, see CairoConfiguration.getSqlWindowInitialRangeBufferSize()
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, j long, s symbol, d double, c VARCHAR) timestamp(ts)", timestampType.getTypeName());

            // trigger per-partition buffers growth and free list usage
            execute("insert into tab select x::timestamp, x/10000, case when x % 3 = 0 THEN NULL ELSE x END, 'k' || (x%5) ::symbol, x*2::double, 'k' || x from long_sequence(39999)");            // trigger removal of rows below lo boundary AND resize of buffer
            execute("insert into tab select (100000+x)::timestamp, (100000+x)%4, case when x % 3 = 0 THEN NULL ELSE 100000 + x END, 'k' || (x%20) ::symbol, x*2::double, 'k' || x from long_sequence(4*90000)");
            String expected = replaceTimestampSuffix("""
                    ts\ti\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                    1970-01-01T00:00:00.460000Z\t0\t419998.0\t5.600253332E9\t380000.0\t380000.0\tnull\t459996.0\t20001\t13334\t20001\t20001\t20001\t459996.0\t380000.0
                    1970-01-01T00:00:00.459997Z\t1\t419995.0\t5.60021333E9\t379997.0\t379997.0\tnull\t459993.0\t20001\t13334\t20001\t20001\t20001\t459993.0\t379997.0
                    1970-01-01T00:00:00.459998Z\t2\t419998.0\t5.600253332E9\t379998.0\t379998.0\t459998.0\t459998.0\t20001\t13334\t20001\t20001\t20001\t459998.0\t379998.0
                    1970-01-01T00:00:00.459999Z\t3\t420001.0\t5.600293334E9\tnull\t380003.0\t459999.0\t459999.0\t20001\t13334\t20001\t20001\t20001\t459999.0\t380003.0
                    """);

            // cross-check with re-write using aggregate functions
            assertSql(
                    expected,
                    "select max(ts) as ts, i, avg(j) as avg, sum(j::double) as sum, first(j::double) as first_value, " +
                            "first_not_null(j::double) as first_value_ignore_nulls, " +
                            "last(j::double) as last_value, last_not_null(j::double) as last_value_ignore_nulls, " +
                            "count(*) as count, count(j::double) as count1, count(s) as count2, count(d) as count3, count(c) as count4, " +
                            "max(j::double) as max, min(j::double) as min " +
                            "from (" +
                            "  select data.ts, data.i, data.j, data.s, data.d, data.c" +
                            "  from ( select i, max(ts) as max from tab group by i) cnt " +
                            (timestampType == TestTimestampType.MICRO ? "  join tab data on cnt.i = data.i and data.ts >= (cnt.max - 80000) " : "  join tab data on cnt.i = data.i and data.ts >= (cnt.max - 80000000) ") +
                            "  order by data.i, ts " +
                            ") " +
                            "group by i " +
                            "order by i"
            );

            String expected2 = replaceTimestampSuffix("""
                    ts\ti\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                    1970-01-01T00:00:00.460000Z\t0\t419998.0\t5.600253332E9\t380000\t380000\tnull\t459996\t20001\t13334\t20001\t20001\t20001\t459996\t380000
                    1970-01-01T00:00:00.459997Z\t1\t419995.0\t5.60021333E9\t379997\t379997\tnull\t459993\t20001\t13334\t20001\t20001\t20001\t459993\t379997
                    1970-01-01T00:00:00.459998Z\t2\t419998.0\t5.600253332E9\t379998\t379998\t459998\t459998\t20001\t13334\t20001\t20001\t20001\t459998\t379998
                    1970-01-01T00:00:00.459999Z\t3\t420001.0\t5.600293334E9\tnull\t380003\t459999\t459999\t20001\t13334\t20001\t20001\t20001\t459999\t380003
                    """);
            assertQueryNoLeakCheck(
                    expected2,
                    "select * from " +
                            "(select * from (select ts, i, " +
                            "avg(j) over (partition by i order by ts range between 80000 microseconds preceding and current row), " +
                            "sum(j) over (partition by i order by ts range between 80000 microseconds preceding and current row), " +
                            "first_value(j) over (partition by i order by ts range between 80000 microseconds preceding and current row), " +
                            "first_value(j) ignore nulls over (partition by i order by ts range between 80000 microseconds preceding and current row), " +
                            "last_value(j) over (partition by i order by ts range between 80000 microseconds preceding and current row), " +
                            "last_value(j) ignore nulls over (partition by i order by ts range between 80000 microseconds preceding and current row), " +
                            "count(*) over (partition by i order by ts range between 80000 microseconds preceding and current row), " +
                            "count(j) over (partition by i order by ts range between 80000 microseconds preceding and current row), " +
                            "count(s) over (partition by i order by ts range between 80000 microseconds preceding and current row), " +
                            "count(d) over (partition by i order by ts range between 80000 microseconds preceding and current row), " +
                            "count(c) over (partition by i order by ts range between 80000 microseconds preceding and current row), " +
                            "max(j) over (partition by i order by ts range between 80000 microseconds preceding and current row), " +
                            "min(j) over (partition by i order by ts range between 80000 microseconds preceding and current row) " +
                            "from tab" +
                            ") " +
                            "limit -4) " +
                            "order by i",
                    null,
                    true,
                    true,
                    false
            );
        });
    }

    @Test
    public void testFrameFunctionOverPartitionedRangeWithLargeFrameRandomData() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, j long, s symbol, d double, c VARCHAR) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab " +
                    "select (100000+x)::timestamp, " +
                    "rnd_long(1,20,10), " +
                    "rnd_long(1,1000,5), " +
                    "rnd_symbol('a', 'b', 'c', 'd'), " +
                    "rnd_long(1,1000,5)::double, " +
                    "rnd_varchar('aaa', 'vvvv', 'quest') " +
                    "from long_sequence(1000000)");

            String expected = replaceTimestampSuffix("""
                    ts\ti\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                    1970-01-01T00:00:01.099993Z\tnull\t500.195891634415\t1680158.0\t201.0\t201.0\t555.0\t555.0\t3664\t3359\t3664\t3377\t3664\t1000.0\t1.0
                    1970-01-01T00:00:01.099950Z\t1\t495.24524012503554\t1742768.0\t915.0\t915.0\t351.0\t351.0\t3845\t3519\t3845\t3520\t3845\t1000.0\t1.0
                    1970-01-01T00:00:01.099955Z\t2\t495.3698069046226\t1693174.0\t80.0\t80.0\t818.0\t818.0\t3781\t3418\t3781\t3475\t3781\t1000.0\t1.0
                    1970-01-01T00:00:01.099983Z\t3\t505.02330264672037\t1755461.0\t807.0\t807.0\t655.0\t655.0\t3786\t3476\t3786\t3452\t3786\t1000.0\t1.0
                    1970-01-01T00:00:01.099989Z\t4\t507.0198750709824\t1785724.0\t423.0\t423.0\t634.0\t634.0\t3834\t3522\t3834\t3528\t3834\t1000.0\t1.0
                    1970-01-01T00:00:01.099999Z\t5\t505.02770562770564\t1749921.0\t986.0\t986.0\t724.0\t724.0\t3786\t3465\t3786\t3467\t3786\t1000.0\t1.0
                    1970-01-01T00:00:01.099992Z\t6\t500.087528604119\t1748306.0\t455.0\t455.0\tnull\t319.0\t3847\t3496\t3847\t3565\t3847\t1000.0\t1.0
                    1970-01-01T00:00:01.100000Z\t7\t504.07134703196346\t1766266.0\t598.0\t598.0\t633.0\t633.0\t3810\t3504\t3810\t3517\t3810\t1000.0\t2.0
                    1970-01-01T00:00:01.099981Z\t8\t507.53068086298686\t1811377.0\t89.0\t89.0\t487.0\t487.0\t3894\t3569\t3894\t3612\t3894\t1000.0\t1.0
                    1970-01-01T00:00:01.099925Z\t9\t509.7903642099226\t1777639.0\t999.0\t999.0\t319.0\t319.0\t3789\t3487\t3789\t3441\t3789\t999.0\t1.0
                    1970-01-01T00:00:01.099947Z\t10\t499.44085417252035\t1777510.0\tnull\t122.0\t841.0\t841.0\t3878\t3559\t3878\t3564\t3878\t1000.0\t2.0
                    1970-01-01T00:00:01.099995Z\t11\t503.51796493245183\t1751739.0\t257.0\t257.0\t62.0\t62.0\t3819\t3479\t3819\t3506\t3819\t1000.0\t1.0
                    1970-01-01T00:00:01.099998Z\t12\t502.48197940503434\t1756677.0\t270.0\t270.0\t456.0\t456.0\t3820\t3496\t3820\t3498\t3820\t1000.0\t1.0
                    1970-01-01T00:00:01.099963Z\t13\t495.9894586894587\t1740923.0\t478.0\t478.0\t472.0\t472.0\t3825\t3510\t3825\t3484\t3825\t1000.0\t1.0
                    1970-01-01T00:00:01.099997Z\t14\t502.76085680751174\t1713409.0\t60.0\t60.0\t602.0\t602.0\t3691\t3408\t3691\t3399\t3691\t1000.0\t1.0
                    1970-01-01T00:00:01.099990Z\t15\t497.3836206896552\t1730895.0\t750.0\t750.0\t784.0\t784.0\t3796\t3480\t3796\t3475\t3796\t1000.0\t1.0
                    1970-01-01T00:00:01.099996Z\t16\t509.6849587716804\t1792562.0\t141.0\t141.0\t204.0\t204.0\t3826\t3517\t3826\t3517\t3826\t1000.0\t1.0
                    1970-01-01T00:00:01.099968Z\t17\t504.3433173212772\t1784871.0\t659.0\t659.0\t949.0\t949.0\t3855\t3539\t3855\t3522\t3855\t1000.0\t1.0
                    1970-01-01T00:00:01.099994Z\t18\t503.6875531613269\t1776506.0\t485.0\t485.0\t795.0\t795.0\t3860\t3527\t3860\t3518\t3860\t1000.0\t1.0
                    1970-01-01T00:00:01.099986Z\t19\t503.60588901472255\t1778736.0\t855.0\t855.0\t233.0\t233.0\t3845\t3532\t3845\t3542\t3845\t1000.0\t1.0
                    1970-01-01T00:00:01.099988Z\t20\t505.3122460824144\t1741306.0\t37.0\t37.0\t15.0\t15.0\t3767\t3446\t3767\t3443\t3767\t1000.0\t1.0
                    """);

            // cross-check with re-write using aggregate functions
            assertSql(
                    expected,
                    "select max(ts) as ts, i, avg(j) as avg, sum(j::double) as sum, first(j::double) as first_value, " +
                            "first_not_null(j::double) as first_value_ignore_nulls, " +
                            "last(j::double) as last_value, last_not_null(j::double) as last_value_ignore_nulls, " +
                            "count(*) as count, count(j::double) as count1, count(s) as count2, count(d) as count3, count(c) as count4, " +
                            "max(j::double) as max, min(j::double) as min " +
                            "from (" +
                            "  select data.ts, data.i, data.j, data.s, data.d, data.c" +
                            "  from (select i, max(ts) as max from tab group by i) cnt " +
                            (timestampType == TestTimestampType.MICRO ? "  join tab data on cnt.i = data.i and data.ts >= (cnt.max - 80000) " : "  join tab data on cnt.i = data.i and data.ts >= (cnt.max - 80000000) ") +
                            "  order by data.i, ts " +
                            ") " +
                            "group by i " +
                            "order by i "
            );

            String expected2 = replaceTimestampSuffix("""
                    ts\ti\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                    1970-01-01T00:00:01.099993Z\tnull\t500.195891634415\t1680158.0\t201\t201\t555\t555\t3664\t3359\t3664\t3377\t3664\t1000\t1
                    1970-01-01T00:00:01.099950Z\t1\t495.24524012503554\t1742768.0\t915\t915\t351\t351\t3845\t3519\t3845\t3520\t3845\t1000\t1
                    1970-01-01T00:00:01.099955Z\t2\t495.3698069046226\t1693174.0\t80\t80\t818\t818\t3781\t3418\t3781\t3475\t3781\t1000\t1
                    1970-01-01T00:00:01.099983Z\t3\t505.02330264672037\t1755461.0\t807\t807\t655\t655\t3786\t3476\t3786\t3452\t3786\t1000\t1
                    1970-01-01T00:00:01.099989Z\t4\t507.0198750709824\t1785724.0\t423\t423\t634\t634\t3834\t3522\t3834\t3528\t3834\t1000\t1
                    1970-01-01T00:00:01.099999Z\t5\t505.02770562770564\t1749921.0\t986\t986\t724\t724\t3786\t3465\t3786\t3467\t3786\t1000\t1
                    1970-01-01T00:00:01.099992Z\t6\t500.087528604119\t1748306.0\t455\t455\tnull\t319\t3847\t3496\t3847\t3565\t3847\t1000\t1
                    1970-01-01T00:00:01.100000Z\t7\t504.07134703196346\t1766266.0\t598\t598\t633\t633\t3810\t3504\t3810\t3517\t3810\t1000\t2
                    1970-01-01T00:00:01.099981Z\t8\t507.53068086298686\t1811377.0\t89\t89\t487\t487\t3894\t3569\t3894\t3612\t3894\t1000\t1
                    1970-01-01T00:00:01.099925Z\t9\t509.7903642099226\t1777639.0\t999\t999\t319\t319\t3789\t3487\t3789\t3441\t3789\t999\t1
                    1970-01-01T00:00:01.099947Z\t10\t499.44085417252035\t1777510.0\tnull\t122\t841\t841\t3878\t3559\t3878\t3564\t3878\t1000\t2
                    1970-01-01T00:00:01.099995Z\t11\t503.51796493245183\t1751739.0\t257\t257\t62\t62\t3819\t3479\t3819\t3506\t3819\t1000\t1
                    1970-01-01T00:00:01.099998Z\t12\t502.48197940503434\t1756677.0\t270\t270\t456\t456\t3820\t3496\t3820\t3498\t3820\t1000\t1
                    1970-01-01T00:00:01.099963Z\t13\t495.9894586894587\t1740923.0\t478\t478\t472\t472\t3825\t3510\t3825\t3484\t3825\t1000\t1
                    1970-01-01T00:00:01.099997Z\t14\t502.76085680751174\t1713409.0\t60\t60\t602\t602\t3691\t3408\t3691\t3399\t3691\t1000\t1
                    1970-01-01T00:00:01.099990Z\t15\t497.3836206896552\t1730895.0\t750\t750\t784\t784\t3796\t3480\t3796\t3475\t3796\t1000\t1
                    1970-01-01T00:00:01.099996Z\t16\t509.6849587716804\t1792562.0\t141\t141\t204\t204\t3826\t3517\t3826\t3517\t3826\t1000\t1
                    1970-01-01T00:00:01.099968Z\t17\t504.3433173212772\t1784871.0\t659\t659\t949\t949\t3855\t3539\t3855\t3522\t3855\t1000\t1
                    1970-01-01T00:00:01.099994Z\t18\t503.6875531613269\t1776506.0\t485\t485\t795\t795\t3860\t3527\t3860\t3518\t3860\t1000\t1
                    1970-01-01T00:00:01.099986Z\t19\t503.60588901472255\t1778736.0\t855\t855\t233\t233\t3845\t3532\t3845\t3542\t3845\t1000\t1
                    1970-01-01T00:00:01.099988Z\t20\t505.3122460824144\t1741306.0\t37\t37\t15\t15\t3767\t3446\t3767\t3443\t3767\t1000\t1
                    """);

            assertQueryNoLeakCheck(
                    expected2,
                    "select last(ts) as ts, " +
                            "i, " +
                            "last(avg) as avg, " +
                            "last(sum) as sum, " +
                            "last(first_value) as first_value, " +
                            "last(first_value_ignore_nulls) as first_value_ignore_nulls, " +
                            "last(last_value) as last_value, " +
                            "last(last_value_ignore_nulls) as last_value_ignore_nulls, " +
                            "last(count) as count, " +
                            "last(count1) as count1, " +
                            "last(count2) as count2, " +
                            "last(count3) as count3, " +
                            "last(count4) as count4, " +
                            "last(max) as max, " +
                            "last(min) as min " +
                            "from (  " +
                            "  select * from (" +
                            "    select ts, i, " +
                            "    avg(j) over (partition by i order by ts range between 80000 microseconds preceding and current row) avg, " +
                            "    sum(j) over (partition by i order by ts range between 80000 microseconds preceding and current row) sum, " +
                            "    first_value(j) over (partition by i order by ts range between 80000 microseconds preceding and current row) first_value, " +
                            "    first_value(j) ignore nulls over (partition by i order by ts range between 80000 microseconds preceding and current row) first_value_ignore_nulls, " +
                            "    last_value(j) over (partition by i order by ts range between 80000 microseconds preceding and current row) last_value, " +
                            "    last_value(j) ignore nulls over (partition by i order by ts range between 80000 microseconds preceding and current row) last_value_ignore_nulls, " +
                            "    count(*) over (partition by i order by ts range between 80000 microseconds preceding and current row) count, " +
                            "    count(j) over (partition by i order by ts range between 80000 microseconds preceding and current row) count1, " +
                            "    count(s) over (partition by i order by ts range between 80000 microseconds preceding and current row) count2, " +
                            "    count(d) over (partition by i order by ts range between 80000 microseconds preceding and current row) count3, " +
                            "    count(c) over (partition by i order by ts range between 80000 microseconds preceding and current row) count4, " +
                            "    max(j) over (partition by i order by ts range between 80000 microseconds preceding and current row) max, " +
                            "    min(j) over (partition by i order by ts range between 80000 microseconds preceding and current row) min " +
                            "    from tab ) " +
                            "  limit -100 )" +
                            "order by i",
                    null,
                    true,
                    true,
                    false
            );
        });
    }

    @Test
    public void testFrameFunctionOverPartitionedRowsWithLargeFrame() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, j long, s symbol, d double, c VARCHAR) timestamp(ts)", timestampType.getTypeName());

            execute("insert into tab select x::timestamp, x/10000, case when x % 3 = 0 THEN NULL ELSE x END, 'k' || (x%5) ::symbol, x*2::double, 'k' || x from long_sequence(39999)");
            execute("insert into tab select (100000+x)::timestamp, (100000+x)%4, case when x % 3 = 0 THEN NULL ELSE 100000+x END, 'k' || (x%20) ::symbol, x*2::double, 'k' || x from long_sequence(4*90000)");

            String expected = replaceTimestampSuffix("""
                    ts\ti\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                    1970-01-01T00:00:00.460000Z\t0\t299998.0\t1.6000093332E10\t140000.0\t140000.0\tnull\t459996.0\t80001\t53334\t80001\t80001\t80001\t459996.0\t140000.0
                    1970-01-01T00:00:00.459997Z\t1\t299995.0\t1.599993333E10\t139997.0\t139997.0\tnull\t459993.0\t80001\t53334\t80001\t80001\t80001\t459993.0\t139997.0
                    1970-01-01T00:00:00.459998Z\t2\t299998.0\t1.6000093332E10\t139998.0\t139998.0\t459998.0\t459998.0\t80001\t53334\t80001\t80001\t80001\t459998.0\t139998.0
                    1970-01-01T00:00:00.459999Z\t3\t300001.0\t1.6000253334E10\tnull\t140003.0\t459999.0\t459999.0\t80001\t53334\t80001\t80001\t80001\t459999.0\t140003.0
                    """);

            // cross-check with re-write using aggregate functions
            assertSql(
                    expected,
                    " select max(ts) as ts, i, avg(j::double) as avg, sum(j::double) as sum, last(j::double) as first_value, " +
                            "last_not_null(j::double) as first_value_ignore_nulls, first(j::double) as last_value, first_not_null(j::double) as last_value_ignore_nulls," +
                            "count(*) as count, count(j::double) as count1, count(s) as count2, " +
                            "count(d) as count3, count(c) as count4, max(j::double) as max, min(j::double) as min " +
                            "from " +
                            "( select ts, i, j, s, d, c, row_number() over (partition by i order by ts desc) as rn from tab order by ts desc) " +
                            "where rn between 1 and 80001 " +
                            "group by i " +
                            "order by i"
            );

            String expected2 = replaceTimestampSuffix("""
                    ts\ti\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                    1970-01-01T00:00:00.460000Z\t0\t299998.0\t1.6000093332E10\t140000\t140000\tnull\t459996\t80001\t53334\t80001\t80001\t80001\t459996\t140000
                    1970-01-01T00:00:00.459997Z\t1\t299995.0\t1.599993333E10\t139997\t139997\tnull\t459993\t80001\t53334\t80001\t80001\t80001\t459993\t139997
                    1970-01-01T00:00:00.459998Z\t2\t299998.0\t1.6000093332E10\t139998\t139998\t459998\t459998\t80001\t53334\t80001\t80001\t80001\t459998\t139998
                    1970-01-01T00:00:00.459999Z\t3\t300001.0\t1.6000253334E10\tnull\t140003\t459999\t459999\t80001\t53334\t80001\t80001\t80001\t459999\t140003
                    """);
            assertQueryNoLeakCheck(
                    expected2,
                    "select * from (" +
                            "select * from (select ts, i, " +
                            "avg(j) over (partition by i order by ts rows between 80000 preceding and current row), " +
                            "sum(j) over (partition by i order by ts rows between 80000 preceding and current row), " +
                            "first_value(j) over (partition by i order by ts rows between 80000 preceding and current row), " +
                            "first_value(j) ignore nulls over (partition by i order by ts rows between 80000 preceding and current row), " +
                            "last_value(j) over (partition by i order by ts rows between 80000 preceding and current row), " +
                            "last_value(j) ignore nulls over (partition by i order by ts rows between 80000 preceding and current row), " +
                            "count(*) over (partition by i order by ts rows between 80000 preceding and current row), " +
                            "count(j) over (partition by i order by ts rows between 80000 preceding and current row), " +
                            "count(s) over (partition by i order by ts rows between 80000 preceding and current row), " +
                            "count(d) over (partition by i order by ts rows between 80000 preceding and current row), " +
                            "count(c) over (partition by i order by ts rows between 80000 preceding and current row), " +
                            "max(j) over (partition by i order by ts rows between 80000 preceding and current row), " +
                            "min(j) over (partition by i order by ts rows between 80000 preceding and current row) " +
                            "from tab) limit -4) " +
                            "order by i",
                    null,
                    true,
                    true,
                    false
            );
        });
    }

    @Test
    public void testFrameFunctionOverRangeFrame() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab_big (ts #TIMESTAMP, i long, j long, s symbol, d double, c VARCHAR) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab_big select (x*1000000)::timestamp, x/4, case when x % 3 = 0 THEN NULL ELSE x%5 END, " +
                    "'k' || (x%5) ::symbol, x*2::double, 'k' || x  from long_sequence(10)");

            // tests when frame doesn't end on current row and time gaps between values are bigger than hi bound
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                            1970-01-01T00:00:01.000000Z\t0\t1\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:02.000000Z\t0\t2\t1.0\t1.0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1
                            1970-01-01T00:00:03.000000Z\t0\tnull\t1.5\t3.0\t1\t1\t2\t2\t2\t2\t2\t2\t2\t2\t1
                            1970-01-01T00:00:04.000000Z\t1\t4\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:05.000000Z\t1\t0\t4.0\t4.0\t4\t4\t4\t4\t1\t1\t1\t1\t1\t4\t4
                            1970-01-01T00:00:06.000000Z\t1\tnull\t2.0\t4.0\t4\t4\t0\t0\t2\t2\t2\t2\t2\t4\t0
                            1970-01-01T00:00:07.000000Z\t1\t2\t2.0\t4.0\t4\t4\tnull\t0\t3\t2\t3\t3\t3\t4\t0
                            1970-01-01T00:00:08.000000Z\t2\t3\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:09.000000Z\t2\tnull\t3.0\t3.0\t3\t3\t3\t3\t1\t1\t1\t1\t1\t3\t3
                            1970-01-01T00:00:10.000000Z\t2\t0\t3.0\t3.0\t3\t3\tnull\t3\t2\t1\t2\t2\t2\t3\t3
                            """),
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts range between unbounded preceding and 1 preceding), " +
                            "sum(j) over (partition by i order by ts range between unbounded preceding and 1 preceding), " +
                            "first_value(j) over (partition by i order by ts range between unbounded preceding and 1 preceding), " +
                            "first_value(j) ignore nulls over (partition by i order by ts range between unbounded preceding and 1 preceding), " +
                            "last_value(j) over (partition by i order by ts range between unbounded preceding and 1 preceding), " +
                            "last_value(j) ignore nulls over (partition by i order by ts range between unbounded preceding and 1 preceding), " +
                            "count(*) over (partition by i order by ts range between unbounded preceding and 1 preceding), " +
                            "count(j) over (partition by i order by ts range between unbounded preceding and 1 preceding), " +
                            "count(s) over (partition by i order by ts range between unbounded preceding and 1 preceding), " +
                            "count(d) over (partition by i order by ts range between unbounded preceding and 1 preceding), " +
                            "count(c) over (partition by i order by ts range between unbounded preceding and 1 preceding), " +
                            "max(j) over (partition by i order by ts range between unbounded preceding and 1 preceding), " +
                            "min(j) over (partition by i order by ts range between unbounded preceding and 1 preceding) " +
                            "from tab_big",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                            1970-01-01T00:00:10.000000Z\t2\t0\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:09.000000Z\t2\tnull\t0.0\t0.0\t0\t0\t0\t0\t1\t1\t1\t1\t1\t0\t0
                            1970-01-01T00:00:08.000000Z\t2\t3\t0.0\t0.0\t0\t0\tnull\t0\t2\t1\t2\t2\t2\t0\t0
                            1970-01-01T00:00:07.000000Z\t1\t2\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:06.000000Z\t1\tnull\t2.0\t2.0\t2\t2\t2\t2\t1\t1\t1\t1\t1\t2\t2
                            1970-01-01T00:00:05.000000Z\t1\t0\t2.0\t2.0\t2\t2\tnull\t2\t2\t1\t2\t2\t2\t2\t2
                            1970-01-01T00:00:04.000000Z\t1\t4\t1.0\t2.0\t2\t2\t0\t0\t3\t2\t3\t3\t3\t2\t0
                            1970-01-01T00:00:03.000000Z\t0\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:02.000000Z\t0\t2\tnull\tnull\tnull\tnull\tnull\tnull\t1\t0\t1\t1\t1\tnull\tnull
                            1970-01-01T00:00:01.000000Z\t0\t1\t2.0\t2.0\tnull\t2\t2\t2\t2\t1\t2\t2\t2\t2\t2
                            """),
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts desc range between unbounded preceding and 1 preceding), " +
                            "sum(j) over (partition by i order by ts desc range between unbounded preceding and 1 preceding), " +
                            "first_value(j) over (partition by i order by ts desc range between unbounded preceding and 1 preceding), " +
                            "first_value(j) ignore nulls over (partition by i order by ts desc range between unbounded preceding and 1 preceding), " +
                            "last_value(j) over (partition by i order by ts desc range between unbounded preceding and 1 preceding), " +
                            "last_value(j) ignore nulls over (partition by i order by ts desc range between unbounded preceding and 1 preceding), " +
                            "count(*) over (partition by i order by ts desc range between unbounded preceding and 1 preceding), " +
                            "count(j) over (partition by i order by ts desc range between unbounded preceding and 1 preceding), " +
                            "count(s) over (partition by i order by ts desc range between unbounded preceding and 1 preceding), " +
                            "count(d) over (partition by i order by ts desc range between unbounded preceding and 1 preceding), " +
                            "count(c) over (partition by i order by ts desc range between unbounded preceding and 1 preceding), " +
                            "max(j) over (partition by i order by ts desc range between unbounded preceding and 1 preceding), " +
                            "min(j) over (partition by i order by ts desc range between unbounded preceding and 1 preceding) " +
                            "from tab_big order by ts desc",
                    "ts###DESC",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                            1970-01-01T00:00:01.000000Z\t0\t1\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:02.000000Z\t0\t2\t1.0\t1.0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1
                            1970-01-01T00:00:03.000000Z\t0\tnull\t1.5\t3.0\t1\t1\t2\t2\t2\t2\t2\t2\t2\t2\t1
                            1970-01-01T00:00:04.000000Z\t1\t4\t1.5\t3.0\t1\t1\tnull\t2\t3\t2\t3\t3\t3\t2\t1
                            1970-01-01T00:00:05.000000Z\t1\t0\t2.3333333333333335\t7.0\t1\t1\t4\t4\t4\t3\t4\t4\t4\t4\t1
                            1970-01-01T00:00:06.000000Z\t1\tnull\t1.75\t7.0\t1\t1\t0\t0\t5\t4\t5\t5\t5\t4\t0
                            1970-01-01T00:00:07.000000Z\t1\t2\t1.75\t7.0\t1\t1\tnull\t0\t6\t4\t6\t6\t6\t4\t0
                            1970-01-01T00:00:08.000000Z\t2\t3\t1.8\t9.0\t1\t1\t2\t2\t7\t5\t7\t7\t7\t4\t0
                            1970-01-01T00:00:09.000000Z\t2\tnull\t2.0\t12.0\t1\t1\t3\t3\t8\t6\t8\t8\t8\t4\t0
                            1970-01-01T00:00:10.000000Z\t2\t0\t2.0\t12.0\t1\t1\tnull\t3\t9\t6\t9\t9\t9\t4\t0
                            """),
                    "select ts, i, j, " +
                            "avg(j) over (order by ts range between unbounded preceding and 1 preceding), " +
                            "sum(j) over (order by ts range between unbounded preceding and 1 preceding), " +
                            "first_value(j) over (order by ts range between unbounded preceding and 1 preceding), " +
                            "first_value(j) ignore nulls over (order by ts range between unbounded preceding and 1 preceding), " +
                            "last_value(j) over (order by ts range between unbounded preceding and 1 preceding), " +
                            "last_value(j) ignore nulls over (order by ts range between unbounded preceding and 1 preceding), " +
                            "count(*) over (order by ts range between unbounded preceding and 1 preceding), " +
                            "count(j) over (order by ts range between unbounded preceding and 1 preceding), " +
                            "count(s) over (order by ts range between unbounded preceding and 1 preceding), " +
                            "count(d) over (order by ts range between unbounded preceding and 1 preceding), " +
                            "count(c) over (order by ts range between unbounded preceding and 1 preceding), " +
                            "max(j) over (order by ts range between unbounded preceding and 1 preceding), " +
                            "min(j) over (order by ts range between unbounded preceding and 1 preceding) " +
                            "from tab_big",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                            1970-01-01T00:00:10.000000Z\t2\t0\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:09.000000Z\t2\tnull\t0.0\t0.0\t0\t0\t0\t0\t1\t1\t1\t1\t1\t0\t0
                            1970-01-01T00:00:08.000000Z\t2\t3\t0.0\t0.0\t0\t0\tnull\t0\t2\t1\t2\t2\t2\t0\t0
                            1970-01-01T00:00:07.000000Z\t1\t2\t1.5\t3.0\t0\t0\t3\t3\t3\t2\t3\t3\t3\t3\t0
                            1970-01-01T00:00:06.000000Z\t1\tnull\t1.6666666666666667\t5.0\t0\t0\t2\t2\t4\t3\t4\t4\t4\t3\t0
                            1970-01-01T00:00:05.000000Z\t1\t0\t1.6666666666666667\t5.0\t0\t0\tnull\t2\t5\t3\t5\t5\t5\t3\t0
                            1970-01-01T00:00:04.000000Z\t1\t4\t1.25\t5.0\t0\t0\t0\t0\t6\t4\t6\t6\t6\t3\t0
                            1970-01-01T00:00:03.000000Z\t0\tnull\t1.8\t9.0\t0\t0\t4\t4\t7\t5\t7\t7\t7\t4\t0
                            1970-01-01T00:00:02.000000Z\t0\t2\t1.8\t9.0\t0\t0\tnull\t4\t8\t5\t8\t8\t8\t4\t0
                            1970-01-01T00:00:01.000000Z\t0\t1\t1.8333333333333333\t11.0\t0\t0\t2\t2\t9\t6\t9\t9\t9\t4\t0
                            """),
                    "select ts, i, j, " +
                            "avg(j) over (order by ts desc range between unbounded preceding and 1 preceding), " +
                            "sum(j) over (order by ts desc range between unbounded preceding and 1 preceding), " +
                            "first_value(j) over (order by ts desc range between unbounded preceding and 1 preceding), " +
                            "first_value(j) ignore nulls over (order by ts desc range between unbounded preceding and 1 preceding), " +
                            "last_value(j) over (order by ts desc range between unbounded preceding and 1 preceding), " +
                            "last_value(j) ignore nulls over (order by ts desc range between unbounded preceding and 1 preceding), " +
                            "count(*) over (order by ts desc range between unbounded preceding and 1 preceding), " +
                            "count(j) over (order by ts desc range between unbounded preceding and 1 preceding), " +
                            "count(s) over (order by ts desc range between unbounded preceding and 1 preceding), " +
                            "count(d) over (order by ts desc range between unbounded preceding and 1 preceding), " +
                            "count(c) over (order by ts desc range between unbounded preceding and 1 preceding), " +
                            "max(j) over (order by ts desc range between unbounded preceding and 1 preceding), " +
                            "min(j) over (order by ts desc range between unbounded preceding and 1 preceding) " +
                            "from tab_big order by ts desc",
                    "ts###DESC",
                    false,
                    true
            );

            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, j long, s symbol, d double, c VARCHAR) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab select x::timestamp, x/4, case when x % 3 = 0 THEN NULL ELSE x%5 END, 'k' || (x%5) ::symbol, x::double, " +
                    "'k' || x  from long_sequence(7)");

            // tests for between X preceding and [Y preceding | current row]
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                            1970-01-01T00:00:00.000001Z\t0\t1\t1.8\t9.0\t1\t1\t2\t2\t7\t5\t7\t7\t7\t4\t0
                            1970-01-01T00:00:00.000002Z\t0\t2\t1.8\t9.0\t1\t1\t2\t2\t7\t5\t7\t7\t7\t4\t0
                            1970-01-01T00:00:00.000003Z\t0\tnull\t1.8\t9.0\t1\t1\t2\t2\t7\t5\t7\t7\t7\t4\t0
                            1970-01-01T00:00:00.000004Z\t1\t4\t1.8\t9.0\t1\t1\t2\t2\t7\t5\t7\t7\t7\t4\t0
                            1970-01-01T00:00:00.000005Z\t1\t0\t1.8\t9.0\t1\t1\t2\t2\t7\t5\t7\t7\t7\t4\t0
                            1970-01-01T00:00:00.000006Z\t1\tnull\t1.8\t9.0\t1\t1\t2\t2\t7\t5\t7\t7\t7\t4\t0
                            1970-01-01T00:00:00.000007Z\t1\t2\t1.8\t9.0\t1\t1\t2\t2\t7\t5\t7\t7\t7\t4\t0
                            """),
                    "select ts, i, j, " +
                            "avg(j) over (), " +
                            "sum(j) over (), " +
                            "first_value(j) over (), " +
                            "first_value(j) ignore nulls over (), " +
                            "last_value(j) over (), " +
                            "last_value(j) ignore nulls over (), " +
                            "count(*) over (), " +
                            "count(j) over (), " +
                            "count(s) over (), " +
                            "count(d) over (), " +
                            "count(c) over (), " +
                            "max(j) over (), " +
                            "min(j) over () " +
                            "from tab",
                    "ts",
                    true, // query is using cached window factory
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                            1970-01-01T00:00:00.000001Z\t0\t1\t1.5\t3.0\t1\t1\tnull\t2\t3\t2\t3\t3\t3\t2\t1
                            1970-01-01T00:00:00.000002Z\t0\t2\t1.5\t3.0\t1\t1\tnull\t2\t3\t2\t3\t3\t3\t2\t1
                            1970-01-01T00:00:00.000003Z\t0\tnull\t1.5\t3.0\t1\t1\tnull\t2\t3\t2\t3\t3\t3\t2\t1
                            1970-01-01T00:00:00.000004Z\t1\t4\t2.0\t6.0\t4\t4\t2\t2\t4\t3\t4\t4\t4\t4\t0
                            1970-01-01T00:00:00.000005Z\t1\t0\t2.0\t6.0\t4\t4\t2\t2\t4\t3\t4\t4\t4\t4\t0
                            1970-01-01T00:00:00.000006Z\t1\tnull\t2.0\t6.0\t4\t4\t2\t2\t4\t3\t4\t4\t4\t4\t0
                            1970-01-01T00:00:00.000007Z\t1\t2\t2.0\t6.0\t4\t4\t2\t2\t4\t3\t4\t4\t4\t4\t0
                            """),
                    "select ts, i, j, " +
                            "avg(j) over (partition by i), " +
                            "sum(j) over (partition by i), " +
                            "first_value(j) over (partition by i), " +
                            "first_value(j) ignore nulls over (partition by i), " +
                            "last_value(j) over (partition by i), " +
                            "last_value(j) ignore nulls over (partition by i), " +
                            "count(*) over (partition by i), " +
                            "count(j) over (partition by i), " +
                            "count(s) over (partition by i), " +
                            "count(d) over (partition by i), " +
                            "count(c) over (partition by i), " +
                            "max(j) over (partition by i), " +
                            "min(j) over (partition by i) " +
                            "from tab",
                    "ts",
                    true,//query is using cached window factory
                    true
            );

            // separate test for first_value() only to use it with non-caching factory
            // this variant doesn't need to scan whole partition (while sum() or avg() do)
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tfirst_value
                            1970-01-01T00:00:00.000001Z\t0\t1\t1
                            1970-01-01T00:00:00.000002Z\t0\t2\t1
                            1970-01-01T00:00:00.000003Z\t0\tnull\t1
                            1970-01-01T00:00:00.000004Z\t1\t4\t4
                            1970-01-01T00:00:00.000005Z\t1\t0\t4
                            1970-01-01T00:00:00.000006Z\t1\tnull\t4
                            1970-01-01T00:00:00.000007Z\t1\t2\t4
                            """),
                    "select ts, i, j, first_value(j) over (partition by i) " +
                            "from tab",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                            1970-01-01T00:00:00.000001Z\t0\t1\t1.0\t1.0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1
                            1970-01-01T00:00:00.000002Z\t0\t2\t1.5\t3.0\t1\t1\t2\t2\t2\t2\t2\t2\t2\t2\t1
                            1970-01-01T00:00:00.000003Z\t0\tnull\t2.0\t2.0\t2\t2\tnull\t2\t2\t1\t2\t2\t2\t2\t2
                            1970-01-01T00:00:00.000004Z\t1\t4\t4.0\t4.0\t4\t4\t4\t4\t1\t1\t1\t1\t1\t4\t4
                            1970-01-01T00:00:00.000005Z\t1\t0\t2.0\t4.0\t4\t4\t0\t0\t2\t2\t2\t2\t2\t4\t0
                            1970-01-01T00:00:00.000006Z\t1\tnull\t0.0\t0.0\t0\t0\tnull\t0\t2\t1\t2\t2\t2\t0\t0
                            1970-01-01T00:00:00.000007Z\t1\t2\t2.0\t2.0\tnull\t2\t2\t2\t2\t1\t2\t2\t2\t2\t2
                            """),
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts range between 1 microsecond preceding and current row), " +
                            "sum(j) over (partition by i order by ts range between 1 microsecond preceding and current row), " +
                            "first_value(j) over (partition by i order by ts range between 1 microsecond preceding and current row), " +
                            "first_value(j) ignore nulls over (partition by i order by ts range between 1 microsecond preceding and current row), " +
                            "last_value(j) over (partition by i order by ts range between 1 microsecond preceding and current row), " +
                            "last_value(j) ignore nulls over (partition by i order by ts range between 1 microsecond preceding and current row), " +
                            "count(*) over (partition by i order by ts range between 1 microsecond preceding and current row), " +
                            "count(j) over (partition by i order by ts range between 1 microsecond preceding and current row), " +
                            "count(s) over (partition by i order by ts range between 1 microsecond preceding and current row), " +
                            "count(d) over (partition by i order by ts range between 1 microsecond preceding and current row), " +
                            "count(c) over (partition by i order by ts range between 1 microsecond preceding and current row), " +
                            "max(j) over (partition by i order by ts range between 1 microsecond preceding and current row), " +
                            "min(j) over (partition by i order by ts range between 1 microsecond preceding and current row) " +
                            "from tab",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                            1970-01-01T00:00:00.000001Z\t0\t1\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000002Z\t0\t2\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000003Z\t0\tnull\t1.0\t1.0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1
                            1970-01-01T00:00:00.000004Z\t1\t4\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000005Z\t1\t0\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000006Z\t1\tnull\t4.0\t4.0\t4\t4\t4\t4\t1\t1\t1\t1\t1\t4\t4
                            1970-01-01T00:00:00.000007Z\t1\t2\t2.0\t4.0\t4\t4\t0\t0\t2\t2\t2\t2\t2\t4\t0
                            """),
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts rows between 4 preceding and 2 preceding), " +
                            "sum(j) over (partition by i order by ts rows between 4 preceding and 2 preceding), " +
                            "first_value(j) over (partition by i order by ts rows between 4 preceding and 2 preceding), " +
                            "first_value(j) ignore nulls over (partition by i order by ts rows between 4 preceding and 2 preceding), " +
                            "last_value(j) over (partition by i order by ts rows between 4 preceding and 2 preceding), " +
                            "last_value(j) ignore nulls over (partition by i order by ts rows between 4 preceding and 2 preceding), " +
                            "count(*) over (partition by i order by ts rows between 4 preceding and 2 preceding), " +
                            "count(j) over (partition by i order by ts rows between 4 preceding and 2 preceding), " +
                            "count(s) over (partition by i order by ts rows between 4 preceding and 2 preceding), " +
                            "count(d) over (partition by i order by ts rows between 4 preceding and 2 preceding), " +
                            "count(c) over (partition by i order by ts rows between 4 preceding and 2 preceding), " +
                            "max(j) over (partition by i order by ts rows between 4 preceding and 2 preceding), " +
                            "min(j) over (partition by i order by ts rows between 4 preceding and 2 preceding) " +
                            "from tab",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                            1970-01-01T00:00:00.000001Z\t0\t1\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000002Z\t0\t2\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000003Z\t0\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000004Z\t1\t4\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000005Z\t1\t0\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000006Z\t1\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000007Z\t1\t2\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            """),
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts rows between 20 preceding and 10 preceding), " +
                            "sum(j) over (partition by i order by ts rows between 20 preceding and 10 preceding), " +
                            "first_value(j) over (partition by i order by ts rows between 20 preceding and 10 preceding), " +
                            "first_value(j) ignore nulls over (partition by i order by ts rows between 20 preceding and 10 preceding), " +
                            "last_value(j) over (partition by i order by ts rows between 20 preceding and 10 preceding), " +
                            "last_value(j) ignore nulls over (partition by i order by ts rows between 20 preceding and 10 preceding), " +
                            "count(*) over (partition by i order by ts rows between 20 preceding and 10 preceding), " +
                            "count(j) over (partition by i order by ts rows between 20 preceding and 10 preceding), " +
                            "count(s) over (partition by i order by ts rows between 20 preceding and 10 preceding), " +
                            "count(d) over (partition by i order by ts rows between 20 preceding and 10 preceding), " +
                            "count(c) over (partition by i order by ts rows between 20 preceding and 10 preceding), " +
                            "max(j) over (partition by i order by ts rows between 20 preceding and 10 preceding), " +
                            "min(j) over (partition by i order by ts rows between 20 preceding and 10 preceding) " +
                            "from tab",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                            1970-01-01T00:00:00.000007Z\t1\t2\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000006Z\t1\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000005Z\t1\t0\t2.0\t2.0\t2\t2\t2\t2\t1\t1\t1\t1\t1\t2\t2
                            1970-01-01T00:00:00.000004Z\t1\t4\t2.0\t2.0\t2\t2\tnull\t2\t2\t1\t2\t2\t2\t2\t2
                            1970-01-01T00:00:00.000003Z\t0\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000002Z\t0\t2\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000001Z\t0\t1\tnull\tnull\tnull\tnull\tnull\tnull\t1\t0\t1\t1\t1\tnull\tnull
                            """),
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts desc range between 4 microseconds preceding and 2 microseconds preceding), " +
                            "sum(j) over (partition by i order by ts desc range between 4 microseconds preceding and 2 microseconds preceding), " +
                            "first_value(j) over (partition by i order by ts desc range between 4 microseconds preceding and 2 microseconds preceding), " +
                            "first_value(j) ignore nulls over (partition by i order by ts desc range between 4 microseconds preceding and 2 microseconds preceding), " +
                            "last_value(j) over (partition by i order by ts desc range between 4 microseconds preceding and 2 microseconds preceding), " +
                            "last_value(j) ignore nulls over (partition by i order by ts desc range between 4 microseconds preceding and 2 microseconds preceding), " +
                            "count(*) over (partition by i order by ts desc range between 4 microseconds preceding and 2 microseconds preceding), " +
                            "count(j) over (partition by i order by ts desc range between 4 microseconds preceding and 2 microseconds preceding), " +
                            "count(s) over (partition by i order by ts desc range between 4 microseconds preceding and 2 microseconds preceding), " +
                            "count(d) over (partition by i order by ts desc range between 4 microseconds preceding and 2 microseconds preceding), " +
                            "count(c) over (partition by i order by ts desc range between 4 microseconds preceding and 2 microseconds preceding), " +
                            "max(j) over (partition by i order by ts desc range between 4 microseconds preceding and 2 microseconds preceding), " +
                            "min(j) over (partition by i order by ts desc range between 4 microseconds preceding and 2 microseconds preceding) " +
                            "from tab order by ts desc",
                    "ts###DESC",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                            1970-01-01T00:00:00.000007Z\t1\t2\t2.0\t2.0\t2\t2\t2\t2\t1\t1\t1\t1\t1\t1\t2
                            1970-01-01T00:00:00.000006Z\t1\tnull\t2.0\t2.0\t2\t2\tnull\t2\t2\t1\t2\t2\t2\t1\t2
                            1970-01-01T00:00:00.000005Z\t1\t0\t1.0\t2.0\t2\t2\t0\t0\t3\t2\t3\t3\t3\t1\t0
                            1970-01-01T00:00:00.000004Z\t1\t4\t2.0\t6.0\t2\t2\t4\t4\t4\t3\t4\t4\t4\t1\t0
                            1970-01-01T00:00:00.000003Z\t0\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t1\t0\t1\t1\t1\t0\tnull
                            1970-01-01T00:00:00.000002Z\t0\t2\t2.0\t2.0\tnull\t2\t2\t2\t2\t1\t2\t2\t2\t0\t2
                            1970-01-01T00:00:00.000001Z\t0\t1\t1.5\t3.0\tnull\t2\t1\t1\t3\t2\t3\t3\t3\t0\t1
                            """),
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts desc range between 4 microseconds preceding and current row), " +
                            "sum(j) over (partition by i order by ts desc range between 4 microseconds preceding and current row), " +
                            "first_value(j) over (partition by i order by ts desc range between 4 microseconds preceding and current row), " +
                            "first_value(j) ignore nulls over (partition by i order by ts desc range between 4 microseconds preceding and current row), " +
                            "last_value(j) over (partition by i order by ts desc range between 4 microseconds preceding and current row), " +
                            "last_value(j) ignore nulls over (partition by i order by ts desc range between 4 microseconds preceding and current row), " +
                            "count(*) over (partition by i order by ts desc range between 4 microseconds preceding and current row), " +
                            "count(j) over (partition by i order by ts desc range between 4 microseconds preceding and current row), " +
                            "count(s) over (partition by i order by ts desc range between 4 microseconds preceding and current row), " +
                            "count(d) over (partition by i order by ts desc range between 4 microseconds preceding and current row), " +
                            "count(c) over (partition by i order by ts desc range between 4 microseconds preceding and current row), " +
                            "max(i) over (partition by i order by ts desc range between 4 microseconds preceding and current row), " +
                            "min(j) over (partition by i order by ts desc range between 4 microseconds preceding and current row) " +
                            "from tab order by ts desc",
                    "ts###DESC",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                            1970-01-01T00:00:00.000007Z\t1\t2\t2.0\t2.0\t2\t2\t2\t2\t1\t1\t1\t1\t1\t2\t2
                            1970-01-01T00:00:00.000006Z\t1\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t1\t0\t1\t1\t1\tnull\tnull
                            1970-01-01T00:00:00.000005Z\t1\t0\t0.0\t0.0\t0\t0\t0\t0\t1\t1\t1\t1\t1\t0\t0
                            1970-01-01T00:00:00.000004Z\t1\t4\t4.0\t4.0\t4\t4\t4\t4\t1\t1\t1\t1\t1\t4\t4
                            1970-01-01T00:00:00.000003Z\t0\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t1\t0\t1\t1\t1\tnull\tnull
                            1970-01-01T00:00:00.000002Z\t0\t2\t2.0\t2.0\t2\t2\t2\t2\t1\t1\t1\t1\t1\t2\t2
                            1970-01-01T00:00:00.000001Z\t0\t1\t1.0\t1.0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1
                            """),
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts desc range between 0 preceding and current row), " +
                            "sum(j) over (partition by i order by ts desc range between 0 preceding and current row), " +
                            "first_value(j) over (partition by i order by ts desc range between 0 preceding and current row), " +
                            "first_value(j) ignore nulls over (partition by i order by ts desc range between 0 preceding and current row), " +
                            "last_value(j) over (partition by i order by ts desc range between 0 preceding and current row), " +
                            "last_value(j) ignore nulls over (partition by i order by ts desc range between 0 preceding and current row), " +
                            "count(*) over (partition by i order by ts desc range between 0 preceding and current row), " +
                            "count(j) over (partition by i order by ts desc range between 0 preceding and current row), " +
                            "count(s) over (partition by i order by ts desc range between 0 preceding and current row), " +
                            "count(d) over (partition by i order by ts desc range between 0 preceding and current row), " +
                            "count(c) over (partition by i order by ts desc range between 0 preceding and current row), " +
                            "max(j) over (partition by i order by ts desc range between 0 preceding and current row), " +
                            "min(j) over (partition by i order by ts desc range between 0 preceding and current row) " +
                            "from tab " +
                            "order by ts desc",
                    "ts###DESC",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                            1970-01-01T00:00:00.000001Z\t0\t1\t1.0\t1.0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1
                            1970-01-01T00:00:00.000002Z\t0\t2\t2.0\t2.0\t2\t2\t2\t2\t1\t1\t1\t1\t1\t2\t2
                            1970-01-01T00:00:00.000003Z\t0\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t1\t0\t1\t1\t1\tnull\tnull
                            1970-01-01T00:00:00.000004Z\t1\t4\t4.0\t4.0\t4\t4\t4\t4\t1\t1\t1\t1\t1\t4\t4
                            1970-01-01T00:00:00.000005Z\t1\t0\t0.0\t0.0\t0\t0\t0\t0\t1\t1\t1\t1\t1\t0\t0
                            1970-01-01T00:00:00.000006Z\t1\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t1\t0\t1\t1\t1\tnull\tnull
                            1970-01-01T00:00:00.000007Z\t1\t2\t2.0\t2.0\t2\t2\t2\t2\t1\t1\t1\t1\t1\t2\t2
                            """),
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts asc range between 0 preceding and current row), " +
                            "sum(j) over (partition by i order by ts asc range between 0 preceding and current row), " +
                            "first_value(j) over (partition by i order by ts asc range between 0 preceding and current row), " +
                            "first_value(j) ignore nulls over (partition by i order by ts asc range between 0 preceding and current row), " +
                            "last_value(j) over (partition by i order by ts asc range between 0 preceding and current row), " +
                            "last_value(j) ignore nulls over (partition by i order by ts asc range between 0 preceding and current row), " +
                            "count(*) over (partition by i order by ts asc range between 0 preceding and current row), " +
                            "count(j) over (partition by i order by ts asc range between 0 preceding and current row), " +
                            "count(s) over (partition by i order by ts asc range between 0 preceding and current row), " +
                            "count(d) over (partition by i order by ts asc range between 0 preceding and current row), " +
                            "count(c) over (partition by i order by ts asc range between 0 preceding and current row), " +
                            "max(j) over (partition by i order by ts asc range between 0 preceding and current row), " +
                            "min(j) over (partition by i order by ts asc range between 0 preceding and current row) " +
                            "from tab",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                            1970-01-01T00:00:00.000001Z\t0\t1\t1.0\t1.0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1
                            1970-01-01T00:00:00.000002Z\t0\t2\t1.5\t3.0\t1\t1\t2\t2\t2\t2\t2\t2\t2\t2\t1
                            1970-01-01T00:00:00.000003Z\t0\tnull\t1.5\t3.0\t1\t1\tnull\t2\t3\t2\t3\t3\t3\t2\t1
                            1970-01-01T00:00:00.000004Z\t1\t4\t4.0\t4.0\t4\t4\t4\t4\t1\t1\t1\t1\t1\t4\t4
                            1970-01-01T00:00:00.000005Z\t1\t0\t2.0\t4.0\t4\t4\t0\t0\t2\t2\t2\t2\t2\t4\t0
                            1970-01-01T00:00:00.000006Z\t1\tnull\t2.0\t4.0\t4\t4\tnull\t0\t3\t2\t3\t3\t3\t4\t0
                            1970-01-01T00:00:00.000007Z\t1\t2\t2.0\t6.0\t4\t4\t2\t2\t4\t3\t4\t4\t4\t4\t0
                            """),
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts asc range between unbounded preceding and current row), " +
                            "sum(j) over (partition by i order by ts asc range between unbounded preceding and current row), " +
                            "first_value(j) over (partition by i order by ts asc range between unbounded preceding and current row), " +
                            "first_value(j) ignore nulls over (partition by i order by ts asc range between unbounded preceding and current row), " +
                            "last_value(j) over (partition by i order by ts asc range between unbounded preceding and current row), " +
                            "last_value(j) ignore nulls over (partition by i order by ts asc range between unbounded preceding and current row), " +
                            "count(*) over (partition by i order by ts asc range between unbounded preceding and current row), " +
                            "count(j) over (partition by i order by ts asc range between unbounded preceding and current row), " +
                            "count(s) over (partition by i order by ts asc range between unbounded preceding and current row), " +
                            "count(d) over (partition by i order by ts asc range between unbounded preceding and current row), " +
                            "count(c) over (partition by i order by ts asc range between unbounded preceding and current row), " +
                            "max(j) over (partition by i order by ts asc range between unbounded preceding and current row), " +
                            "min(j) over (partition by i order by ts asc range between unbounded preceding and current row) " +
                            "from tab",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                            1970-01-01T00:00:00.000007Z\t1\t2\t2.0\t2.0\t2\t2\t2\t2\t1\t1\t1\t1\t1\t2\t2
                            1970-01-01T00:00:00.000006Z\t1\tnull\t2.0\t2.0\t2\t2\tnull\t2\t2\t1\t2\t2\t2\t2\t2
                            1970-01-01T00:00:00.000005Z\t1\t0\t1.0\t2.0\t2\t2\t0\t0\t3\t2\t3\t3\t3\t2\t0
                            1970-01-01T00:00:00.000004Z\t1\t4\t2.0\t6.0\t2\t2\t4\t4\t4\t3\t4\t4\t4\t4\t0
                            1970-01-01T00:00:00.000003Z\t0\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t1\t0\t1\t1\t1\tnull\tnull
                            1970-01-01T00:00:00.000002Z\t0\t2\t2.0\t2.0\tnull\t2\t2\t2\t2\t1\t2\t2\t2\t2\t2
                            1970-01-01T00:00:00.000001Z\t0\t1\t1.5\t3.0\tnull\t2\t1\t1\t3\t2\t3\t3\t3\t2\t1
                            """),
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts desc range between unbounded preceding and current row), " +
                            "sum(j) over (partition by i order by ts desc range between unbounded preceding and current row), " +
                            "first_value(j) over (partition by i order by ts desc range between unbounded preceding and current row), " +
                            "first_value(j) ignore nulls over (partition by i order by ts desc range between unbounded preceding and current row), " +
                            "last_value(j) over (partition by i order by ts desc range between unbounded preceding and current row), " +
                            "last_value(j) ignore nulls over (partition by i order by ts desc range between unbounded preceding and current row), " +
                            "count(*) over (partition by i order by ts desc range between unbounded preceding and current row), " +
                            "count(j) over (partition by i order by ts desc range between unbounded preceding and current row), " +
                            "count(s) over (partition by i order by ts desc range between unbounded preceding and current row), " +
                            "count(d) over (partition by i order by ts desc range between unbounded preceding and current row), " +
                            "count(c) over (partition by i order by ts desc range between unbounded preceding and current row), " +
                            "max(j) over (partition by i order by ts desc range between unbounded preceding and current row), " +
                            "min(j) over (partition by i order by ts desc range between unbounded preceding and current row) " +
                            "from tab order by ts desc",
                    "ts###DESC",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                            1970-01-01T00:00:00.000001Z\t0\t1\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000002Z\t0\t2\t1.0\t1.0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1
                            1970-01-01T00:00:00.000003Z\t0\tnull\t1.5\t3.0\t1\t1\t2\t2\t2\t2\t2\t2\t2\t2\t1
                            1970-01-01T00:00:00.000004Z\t1\t4\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000005Z\t1\t0\t4.0\t4.0\t4\t4\t4\t4\t1\t1\t1\t1\t1\t4\t4
                            1970-01-01T00:00:00.000006Z\t1\tnull\t2.0\t4.0\t4\t4\t0\t0\t2\t2\t2\t2\t2\t4\t0
                            1970-01-01T00:00:00.000007Z\t1\t2\t2.0\t4.0\t4\t4\tnull\t0\t3\t2\t3\t3\t3\t4\t0
                            """),
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts asc range between unbounded preceding and 1 preceding), " +
                            "sum(j) over (partition by i order by ts asc range between unbounded preceding and 1 preceding), " +
                            "first_value(j) over (partition by i order by ts asc range between unbounded preceding and 1 preceding), " +
                            "first_value(j) ignore nulls over (partition by i order by ts asc range between unbounded preceding and 1 preceding), " +
                            "last_value(j) over (partition by i order by ts asc range between unbounded preceding and 1 preceding), " +
                            "last_value(j) ignore nulls over (partition by i order by ts asc range between unbounded preceding and 1 preceding), " +
                            "count(*) over (partition by i order by ts asc range between unbounded preceding and 1 preceding), " +
                            "count(j) over (partition by i order by ts asc range between unbounded preceding and 1 preceding), " +
                            "count(s) over (partition by i order by ts asc range between unbounded preceding and 1 preceding), " +
                            "count(d) over (partition by i order by ts asc range between unbounded preceding and 1 preceding), " +
                            "count(c) over (partition by i order by ts asc range between unbounded preceding and 1 preceding), " +
                            "max(j) over (partition by i order by ts asc range between unbounded preceding and 1 preceding), " +
                            "min(j) over (partition by i order by ts asc range between unbounded preceding and 1 preceding) " +
                            "from tab",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                            1970-01-01T00:00:00.000007Z\t1\t2\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000006Z\t1\tnull\t2.0\t2.0\t2\t2\t2\t2\t1\t1\t1\t1\t1\t2\t2
                            1970-01-01T00:00:00.000005Z\t1\t0\t2.0\t2.0\t2\t2\tnull\t2\t2\t1\t2\t2\t2\t2\t2
                            1970-01-01T00:00:00.000004Z\t1\t4\t1.0\t2.0\t2\t2\t0\t0\t3\t2\t3\t3\t3\t2\t0
                            1970-01-01T00:00:00.000003Z\t0\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000002Z\t0\t2\tnull\tnull\tnull\tnull\tnull\tnull\t1\t0\t1\t1\t1\tnull\tnull
                            1970-01-01T00:00:00.000001Z\t0\t1\t2.0\t2.0\tnull\t2\t2\t2\t2\t1\t2\t2\t2\t2\t2
                            """),
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts desc range between unbounded preceding and 1 preceding), " +
                            "sum(j) over (partition by i order by ts desc range between unbounded preceding and 1 preceding), " +
                            "first_value(j) over (partition by i order by ts desc range between unbounded preceding and 1 preceding), " +
                            "first_value(j) ignore nulls over (partition by i order by ts desc range between unbounded preceding and 1 preceding), " +
                            "last_value(j) over (partition by i order by ts desc range between unbounded preceding and 1 preceding), " +
                            "last_value(j) ignore nulls over (partition by i order by ts desc range between unbounded preceding and 1 preceding), " +
                            "count(*) over (partition by i order by ts desc range between unbounded preceding and 1 preceding), " +
                            "count(j) over (partition by i order by ts desc range between unbounded preceding and 1 preceding), " +
                            "count(s) over (partition by i order by ts desc range between unbounded preceding and 1 preceding), " +
                            "count(d) over (partition by i order by ts desc range between unbounded preceding and 1 preceding), " +
                            "count(c) over (partition by i order by ts desc range between unbounded preceding and 1 preceding), " +
                            "max(j) over (partition by i order by ts desc range between unbounded preceding and 1 preceding), " +
                            "min(j) over (partition by i order by ts desc range between unbounded preceding and 1 preceding) " +
                            "from tab " +
                            "order by ts desc",
                    "ts###DESC",
                    false,
                    true
            );

            // all nulls because values never enter the frame
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                            1970-01-01T00:00:00.000001Z\t0\t1\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000002Z\t0\t2\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000003Z\t0\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000004Z\t1\t4\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000005Z\t1\t0\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000006Z\t1\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000007Z\t1\t2\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            """),
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts asc range between unbounded preceding and 10 microseconds preceding), " +
                            "sum(j) over (partition by i order by ts asc range between unbounded preceding and 10 microseconds preceding), " +
                            "first_value(j) over (partition by i order by ts asc range between unbounded preceding and 10 microseconds preceding), " +
                            "first_value(j) ignore nulls over (partition by i order by ts asc range between unbounded preceding and 10 microseconds preceding), " +
                            "last_value(j) over (partition by i order by ts asc range between unbounded preceding and 10 microseconds preceding), " +
                            "last_value(j) ignore nulls over (partition by i order by ts asc range between unbounded preceding and 10 microseconds preceding), " +
                            "count(*) over (partition by i order by ts asc range between unbounded preceding and 10 microseconds preceding), " +
                            "count(j) over (partition by i order by ts asc range between unbounded preceding and 10 microseconds preceding), " +
                            "count(s) over (partition by i order by ts asc range between unbounded preceding and 10 microseconds preceding), " +
                            "count(d) over (partition by i order by ts asc range between unbounded preceding and 10 microseconds preceding), " +
                            "count(c) over (partition by i order by ts asc range between unbounded preceding and 10 microseconds preceding), " +
                            "max(j) over (partition by i order by ts asc range between unbounded preceding and 10 microseconds preceding), " +
                            "min(j) over (partition by i order by ts asc range between unbounded preceding and 10 microseconds preceding) " +
                            "from tab",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                            1970-01-01T00:00:00.000007Z\t1\t2\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000006Z\t1\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000005Z\t1\t0\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000004Z\t1\t4\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000003Z\t0\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000002Z\t0\t2\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000001Z\t0\t1\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            """),
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts desc range between unbounded preceding and 10 microseconds preceding), " +
                            "sum(j) over (partition by i order by ts desc range between unbounded preceding and 10 microseconds preceding), " +
                            "first_value(j) over (partition by i order by ts desc range between unbounded preceding and 10 microseconds preceding), " +
                            "first_value(j) ignore nulls over (partition by i order by ts desc range between unbounded preceding and 10 microseconds preceding), " +
                            "last_value(j) over (partition by i order by ts desc range between unbounded preceding and 10 microseconds preceding), " +
                            "last_value(j) ignore nulls over (partition by i order by ts desc range between unbounded preceding and 10 microseconds preceding), " +
                            "count(*) over (partition by i order by ts desc range between unbounded preceding and 10 microseconds preceding), " +
                            "count(j) over (partition by i order by ts desc range between unbounded preceding and 10 microseconds preceding), " +
                            "count(s) over (partition by i order by ts desc range between unbounded preceding and 10 microseconds preceding), " +
                            "count(d) over (partition by i order by ts desc range between unbounded preceding and 10 microseconds preceding), " +
                            "count(c) over (partition by i order by ts desc range between unbounded preceding and 10 microseconds preceding), " +
                            "max(j) over (partition by i order by ts desc range between unbounded preceding and 10 microseconds preceding), " +
                            "min(j) over (partition by i order by ts desc range between unbounded preceding and 10 microseconds preceding) " +
                            "from tab " +
                            "order by ts desc",
                    "ts###DESC",
                    false,
                    true
            );

            // with duplicate timestamp values (but still unique within partition)

            executeWithRewriteTimestamp("create table dups(ts #TIMESTAMP, i long, j long, s symbol, d double, c VARCHAR) timestamp(ts) partition by year", timestampType.getTypeName());
            execute("insert into dups select (x/2)::timestamp, x%2, case when x % 3 = 0 THEN NULL ELSE x%5 END, 'k' || (x%5) ::symbol, x*2::double," +
                    " 'k' || x from long_sequence(10)");

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\ts\td\tc
                            1970-01-01T00:00:00.000000Z\t1\t1\tk1\t2.0\tk1
                            1970-01-01T00:00:00.000001Z\t0\t2\tk2\t4.0\tk2
                            1970-01-01T00:00:00.000001Z\t1\tnull\tk3\t6.0\tk3
                            1970-01-01T00:00:00.000002Z\t0\t4\tk4\t8.0\tk4
                            1970-01-01T00:00:00.000002Z\t1\t0\tk0\t10.0\tk5
                            1970-01-01T00:00:00.000003Z\t0\tnull\tk1\t12.0\tk6
                            1970-01-01T00:00:00.000003Z\t1\t2\tk2\t14.0\tk7
                            1970-01-01T00:00:00.000004Z\t0\t3\tk3\t16.0\tk8
                            1970-01-01T00:00:00.000004Z\t1\tnull\tk4\t18.0\tk9
                            1970-01-01T00:00:00.000005Z\t0\t0\tk0\t20.0\tk10
                            """),
                    "select * from dups",
                    "ts",
                    true,
                    true
            );

            String dupResult = replaceTimestampSuffix("""
                    ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                    1970-01-01T00:00:00.000000Z\t1\t1\t1.0\t1.0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1
                    1970-01-01T00:00:00.000001Z\t0\t2\t2.0\t2.0\t2\t2\t2\t2\t1\t1\t1\t1\t1\t2\t2
                    1970-01-01T00:00:00.000001Z\t1\tnull\t1.0\t1.0\t1\t1\tnull\t1\t2\t1\t2\t2\t2\t1\t1
                    1970-01-01T00:00:00.000002Z\t0\t4\t3.0\t6.0\t2\t2\t4\t4\t2\t2\t2\t2\t2\t4\t2
                    1970-01-01T00:00:00.000002Z\t1\t0\t0.5\t1.0\t1\t1\t0\t0\t3\t2\t3\t3\t3\t1\t0
                    1970-01-01T00:00:00.000003Z\t0\tnull\t3.0\t6.0\t2\t2\tnull\t4\t3\t2\t3\t3\t3\t4\t2
                    1970-01-01T00:00:00.000003Z\t1\t2\t1.0\t3.0\t1\t1\t2\t2\t4\t3\t4\t4\t4\t2\t0
                    1970-01-01T00:00:00.000004Z\t0\t3\t3.0\t9.0\t2\t2\t3\t3\t4\t3\t4\t4\t4\t4\t2
                    1970-01-01T00:00:00.000004Z\t1\tnull\t1.0\t3.0\t1\t1\tnull\t2\t5\t3\t5\t5\t5\t2\t0
                    1970-01-01T00:00:00.000005Z\t0\t0\t2.25\t9.0\t2\t2\t0\t0\t5\t4\t5\t5\t5\t4\t0
                    """);

            assertQueryNoLeakCheck(
                    dupResult,
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts range between 4 microseconds preceding and current row), " +
                            "sum(j) over (partition by i order by ts range between 4 microseconds preceding and current row), " +
                            "first_value(j) over (partition by i order by ts range between 4 microseconds preceding and current row), " +
                            "first_value(j) ignore nulls over (partition by i order by ts range between 4 microseconds preceding and current row), " +
                            "last_value(j) over (partition by i order by ts range between 4 microseconds preceding and current row), " +
                            "last_value(j) ignore nulls over (partition by i order by ts range between 4 microseconds preceding and current row), " +
                            "count(*) over (partition by i order by ts range between 4 microseconds preceding and current row), " +
                            "count(j) over (partition by i order by ts range between 4 microseconds preceding and current row), " +
                            "count(s) over (partition by i order by ts range between 4 microseconds preceding and current row), " +
                            "count(d) over (partition by i order by ts range between 4 microseconds preceding and current row), " +
                            "count(c) over (partition by i order by ts range between 4 microseconds preceding and current row), " +
                            "max(j) over (partition by i order by ts range between 4 microseconds preceding and current row), " +
                            "min(j) over (partition by i order by ts range between 4 microseconds preceding and current row) " +
                            "from dups",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    dupResult,
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts range between 4 microseconds preceding and current row), " +
                            "sum(j) over (partition by i order by ts range between 4 microseconds preceding and current row), " +
                            "first_value(j) over (partition by i order by ts range between 4 microseconds preceding and current row), " +
                            "first_value(j) ignore nulls over (partition by i order by ts range between 4 microseconds preceding and current row), " +
                            "last_value(j) over (partition by i order by ts range between 4 microseconds preceding and current row), " +
                            "last_value(j) ignore nulls over (partition by i order by ts range between 4 microseconds preceding and current row), " +
                            "count(*) over (partition by i order by ts range between 4 microseconds preceding and current row), " +
                            "count(j) over (partition by i order by ts range between 4 microseconds preceding and current row), " +
                            "count(s) over (partition by i order by ts range between 4 microseconds preceding and current row), " +
                            "count(d) over (partition by i order by ts range between 4 microseconds preceding and current row), " +
                            "count(c) over (partition by i order by ts range between 4 microseconds preceding and current row), " +
                            "max(j) over (partition by i order by ts range between 4 microseconds preceding and current row), " +
                            "min(j) over (partition by i order by ts range between 4 microseconds preceding and current row) " +
                            "from dups " +
                            "order by ts",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    dupResult,
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts range between unbounded preceding and current row), " +
                            "sum(j) over (partition by i order by ts range between unbounded preceding and current row), " +
                            "first_value(j) over (partition by i order by ts range between unbounded preceding and current row), " +
                            "first_value(j) ignore nulls over (partition by i order by ts range between unbounded preceding and current row), " +
                            "last_value(j) over (partition by i order by ts range between unbounded preceding and current row), " +
                            "last_value(j) ignore nulls over (partition by i order by ts range between unbounded preceding and current row), " +
                            "count(*) over (partition by i order by ts range between unbounded preceding and current row), " +
                            "count(j) over (partition by i order by ts range between unbounded preceding and current row), " +
                            "count(s) over (partition by i order by ts range between unbounded preceding and current row), " +
                            "count(d) over (partition by i order by ts range between unbounded preceding and current row), " +
                            "count(c) over (partition by i order by ts range between unbounded preceding and current row), " +
                            "max(j) over (partition by i order by ts range between unbounded preceding and current row), " +
                            "min(j) over (partition by i order by ts range between unbounded preceding and current row) " +
                            "from dups " +
                            "order by ts",
                    "ts",
                    false,
                    true
            );

            String dupResult2 = replaceTimestampSuffix("""
                    ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                    1970-01-01T00:00:00.000005Z\t0\t0\t0.0\t0.0\t0\t0\t0\t0\t1\t1\t1\t1\t1\t0\t0
                    1970-01-01T00:00:00.000004Z\t1\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t1\t0\t1\t1\t1\tnull\tnull
                    1970-01-01T00:00:00.000004Z\t0\t3\t1.5\t3.0\t0\t0\t3\t3\t2\t2\t2\t2\t2\t3\t0
                    1970-01-01T00:00:00.000003Z\t1\t2\t2.0\t2.0\tnull\t2\t2\t2\t2\t1\t2\t2\t2\t2\t2
                    1970-01-01T00:00:00.000003Z\t0\tnull\t1.5\t3.0\t0\t0\tnull\t3\t3\t2\t3\t3\t3\t3\t0
                    1970-01-01T00:00:00.000002Z\t1\t0\t1.0\t2.0\tnull\t2\t0\t0\t3\t2\t3\t3\t3\t2\t0
                    1970-01-01T00:00:00.000002Z\t0\t4\t2.3333333333333335\t7.0\t0\t0\t4\t4\t4\t3\t4\t4\t4\t4\t0
                    1970-01-01T00:00:00.000001Z\t1\tnull\t1.0\t2.0\tnull\t2\tnull\t0\t4\t2\t4\t4\t4\t2\t0
                    1970-01-01T00:00:00.000001Z\t0\t2\t2.25\t9.0\t0\t0\t2\t2\t5\t4\t5\t5\t5\t4\t0
                    1970-01-01T00:00:00.000000Z\t1\t1\t1.0\t3.0\tnull\t2\t1\t1\t5\t3\t5\t5\t5\t2\t0
                    """);

            assertQueryNoLeakCheck(
                    dupResult2,
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts desc range between 4 microseconds preceding and current row), " +
                            "sum(j) over (partition by i order by ts desc range between 4 microseconds preceding and current row), " +
                            "first_value(j) over (partition by i order by ts desc range between 4 microseconds preceding and current row), " +
                            "first_value(j) ignore nulls over (partition by i order by ts desc range between 4 microseconds preceding and current row), " +
                            "last_value(j) over (partition by i order by ts desc range between 4 microseconds preceding and current row), " +
                            "last_value(j) ignore nulls over (partition by i order by ts desc range between 4 microseconds preceding and current row), " +
                            "count(*) over (partition by i order by ts desc range between 4 microseconds preceding and current row), " +
                            "count(j) over (partition by i order by ts desc range between 4 microseconds preceding and current row), " +
                            "count(s) over (partition by i order by ts desc range between 4 microseconds preceding and current row), " +
                            "count(d) over (partition by i order by ts desc range between 4 microseconds preceding and current row), " +
                            "count(c) over (partition by i order by ts desc range between 4 microseconds preceding and current row), " +
                            "max(j) over (partition by i order by ts desc range between 4 microseconds preceding and current row), " +
                            "min(j) over (partition by i order by ts desc range between 4 microseconds preceding and current row) " +
                            "from dups " +
                            "order by ts desc",
                    "ts###DESC",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    dupResult2,
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts desc range between unbounded preceding and current row), " +
                            "sum(j) over (partition by i order by ts desc range between unbounded preceding and current row), " +
                            "first_value(j) over (partition by i order by ts desc range between unbounded preceding and current row), " +
                            "first_value(j) ignore nulls over (partition by i order by ts desc range between unbounded preceding and current row), " +
                            "last_value(j) over (partition by i order by ts desc range between unbounded preceding and current row), " +
                            "last_value(j) ignore nulls over (partition by i order by ts desc range between unbounded preceding and current row), " +
                            "count(*) over (partition by i order by ts desc range between unbounded preceding and current row), " +
                            "count(j) over (partition by i order by ts desc range between unbounded preceding and current row), " +
                            "count(s) over (partition by i order by ts desc range between unbounded preceding and current row), " +
                            "count(d) over (partition by i order by ts desc range between unbounded preceding and current row), " +
                            "count(c) over (partition by i order by ts desc range between unbounded preceding and current row), " +
                            "max(j) over (partition by i order by ts desc range between unbounded preceding and current row), " +
                            "min(j) over (partition by i order by ts desc range between unbounded preceding and current row) " +
                            "from dups " +
                            "order by ts desc",
                    "ts###DESC",
                    false,
                    true
            );

            // with duplicate timestamp values (including ts duplicates within partition)
            executeWithRewriteTimestamp("create table dups2(ts #TIMESTAMP, i long, j long, n long, s symbol, d double, c VARCHAR) timestamp(ts) partition by year", timestampType.getTypeName());
            execute("insert into dups2 select (x/4)::timestamp, x%2, case when x % 3 = 0 THEN NULL ELSE x%5 END, x, 'k' || (x%5) ::symbol, x*2::double," +
                    " 'k' || x from long_sequence(10)");

            assertSql(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tn\ts\td\tc
                            1970-01-01T00:00:00.000000Z\t0\t2\t2\tk2\t4.0\tk2
                            1970-01-01T00:00:00.000001Z\t0\t4\t4\tk4\t8.0\tk4
                            1970-01-01T00:00:00.000001Z\t0\tnull\t6\tk1\t12.0\tk6
                            1970-01-01T00:00:00.000002Z\t0\t3\t8\tk3\t16.0\tk8
                            1970-01-01T00:00:00.000002Z\t0\t0\t10\tk0\t20.0\tk10
                            1970-01-01T00:00:00.000000Z\t1\t1\t1\tk1\t2.0\tk1
                            1970-01-01T00:00:00.000000Z\t1\tnull\t3\tk3\t6.0\tk3
                            1970-01-01T00:00:00.000001Z\t1\t0\t5\tk0\t10.0\tk5
                            1970-01-01T00:00:00.000001Z\t1\t2\t7\tk2\t14.0\tk7
                            1970-01-01T00:00:00.000002Z\t1\tnull\t9\tk4\t18.0\tk9
                            """),
                    "select * from dups2 order by i, n"
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                            1970-01-01T00:00:00.000000Z\t0\t2\t2.0\t2.0\t2\t2\t2\t2\t1\t1\t1\t1\t1\t2\t2
                            1970-01-01T00:00:00.000001Z\t0\t4\t4.0\t4.0\t4\t4\t4\t4\t1\t1\t1\t1\t1\t4\t4
                            1970-01-01T00:00:00.000001Z\t0\tnull\t4.0\t4.0\t4\t4\tnull\t4\t2\t1\t2\t2\t2\t4\t4
                            1970-01-01T00:00:00.000002Z\t0\t3\t3.0\t3.0\t3\t3\t3\t3\t1\t1\t1\t1\t1\t3\t3
                            1970-01-01T00:00:00.000002Z\t0\t0\t1.5\t3.0\t3\t3\t0\t0\t2\t2\t2\t2\t2\t3\t0
                            1970-01-01T00:00:00.000000Z\t1\t1\t1.0\t1.0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1
                            1970-01-01T00:00:00.000000Z\t1\tnull\t1.0\t1.0\t1\t1\tnull\t1\t2\t1\t2\t2\t2\t1\t1
                            1970-01-01T00:00:00.000001Z\t1\t0\t0.0\t0.0\t0\t0\t0\t0\t1\t1\t1\t1\t1\t0\t0
                            1970-01-01T00:00:00.000001Z\t1\t2\t1.0\t2.0\t0\t0\t2\t2\t2\t2\t2\t2\t2\t2\t0
                            1970-01-01T00:00:00.000002Z\t1\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t1\t0\t1\t1\t1\tnull\tnull
                            """),
                    "select ts, i, j, avg, sum, first_value, first_value_ignore_nulls, last_value, last_value_ignore_nulls, count, count1, count2, count3, count4, max, min " +
                            "from ( " +
                            "select ts, i, j, n, " +
                            "avg(j) over (partition by i order by ts range between 0 preceding and current row) as avg, " +
                            "sum(j) over (partition by i order by ts range between 0 preceding and current row) as sum, " +
                            "first_value(j) over (partition by i order by ts range between 0 preceding and current row) as first_value, " +
                            "first_value(j) ignore nulls over (partition by i order by ts range between 0 preceding and current row) as first_value_ignore_nulls, " +
                            "last_value(j) over (partition by i order by ts range between 0 preceding and current row) as last_value, " +
                            "last_value(j) ignore nulls over (partition by i order by ts range between 0 preceding and current row) as last_value_ignore_nulls, " +
                            "count(*) over (partition by i order by ts range between 0 preceding and current row) as count, " +
                            "count(j) over (partition by i order by ts range between 0 preceding and current row) as count1, " +
                            "count(s) over (partition by i order by ts range between 0 preceding and current row) as count2, " +
                            "count(d) over (partition by i order by ts range between 0 preceding and current row) as count3, " +
                            "count(c) over (partition by i order by ts range between 0 preceding and current row) as count4, " +
                            "max(j) over (partition by i order by ts range between 0 preceding and current row) as max, " +
                            "min(j) over (partition by i order by ts range between 0 preceding and current row) as min, " +
                            "from dups2 " +
                            "limit 10) " +
                            "order by i, n",
                    null,
                    true,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                            1970-01-01T00:00:00.000002Z\t1\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t1\t0\t1\t1\t1\tnull\tnull
                            1970-01-01T00:00:00.000001Z\t1\t2\t2.0\t2.0\t2\t2\t2\t2\t1\t1\t1\t1\t1\t2\t2
                            1970-01-01T00:00:00.000001Z\t1\t0\t1.0\t2.0\t2\t2\t0\t0\t2\t2\t2\t2\t2\t2\t0
                            1970-01-01T00:00:00.000000Z\t1\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t1\t0\t1\t1\t1\tnull\tnull
                            1970-01-01T00:00:00.000000Z\t1\t1\t1.0\t1.0\tnull\t1\t1\t1\t2\t1\t2\t2\t2\t1\t1
                            1970-01-01T00:00:00.000002Z\t0\t0\t0.0\t0.0\t0\t0\t0\t0\t1\t1\t1\t1\t1\t0\t0
                            1970-01-01T00:00:00.000002Z\t0\t3\t1.5\t3.0\t0\t0\t3\t3\t2\t2\t2\t2\t2\t3\t0
                            1970-01-01T00:00:00.000001Z\t0\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t1\t0\t1\t1\t1\tnull\tnull
                            1970-01-01T00:00:00.000001Z\t0\t4\t4.0\t4.0\tnull\t4\t4\t4\t2\t1\t2\t2\t2\t4\t4
                            1970-01-01T00:00:00.000000Z\t0\t2\t2.0\t2.0\t2\t2\t2\t2\t1\t1\t1\t1\t1\t2\t2
                            """),
                    "select ts, i, j, avg, sum, first_value, first_value_ignore_nulls, last_value, last_value_ignore_nulls, count, count1, count2, count3, count4, max, min from ( " +
                            "select ts, i, j, n, " +
                            "avg(j) over (partition by i order by ts desc range between 0 preceding and current row) as avg, " +
                            "sum(j) over (partition by i order by ts desc range between 0 preceding and current row) as sum, " +
                            "first_value(j) over (partition by i order by ts desc range between 0 preceding and current row) as first_value, " +
                            "first_value(j) ignore nulls over (partition by i order by ts desc range between 0 preceding and current row) as first_value_ignore_nulls, " +
                            "last_value(j) over (partition by i order by ts desc range between 0 preceding and current row) as last_value, " +
                            "last_value(j) ignore nulls over (partition by i order by ts desc range between 0 preceding and current row) as last_value_ignore_nulls, " +
                            "count(*) over (partition by i order by ts desc range between 0 preceding and current row) as count, " +
                            "count(j) over (partition by i order by ts desc range between 0 preceding and current row) as count1, " +
                            "count(s) over (partition by i order by ts desc range between 0 preceding and current row) as count2, " +
                            "count(d) over (partition by i order by ts desc range between 0 preceding and current row) as count3, " +
                            "count(c) over (partition by i order by ts desc range between 0 preceding and current row) as count4, " +
                            "max(j) over (partition by i order by ts desc range between 0 preceding and current row) as max, " +
                            "min(j) over (partition by i order by ts desc range between 0 preceding and current row) as min " +
                            "from dups2 " +
                            "order by ts " +
                            "desc limit 10) " +
                            "order by i desc, n desc",
                    null,
                    true,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                            1970-01-01T00:00:00.000000Z\t0\t2\t2.0\t2.0\t2\t2\t2\t2\t1\t1\t1\t1\t1\t2\t2
                            1970-01-01T00:00:00.000001Z\t0\t4\t3.0\t6.0\t2\t2\t4\t4\t2\t2\t2\t2\t2\t4\t2
                            1970-01-01T00:00:00.000001Z\t0\tnull\t3.0\t6.0\t2\t2\tnull\t4\t3\t2\t3\t3\t3\t4\t2
                            1970-01-01T00:00:00.000002Z\t0\t3\t3.5\t7.0\t4\t4\t3\t3\t3\t2\t3\t3\t3\t4\t3
                            1970-01-01T00:00:00.000002Z\t0\t0\t2.3333333333333335\t7.0\t4\t4\t0\t0\t4\t3\t4\t4\t4\t4\t0
                            1970-01-01T00:00:00.000000Z\t1\t1\t1.0\t1.0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1
                            1970-01-01T00:00:00.000000Z\t1\tnull\t1.0\t1.0\t1\t1\tnull\t1\t2\t1\t2\t2\t2\t1\t1
                            1970-01-01T00:00:00.000001Z\t1\t0\t0.5\t1.0\t1\t1\t0\t0\t3\t2\t3\t3\t3\t1\t0
                            1970-01-01T00:00:00.000001Z\t1\t2\t1.0\t3.0\t1\t1\t2\t2\t4\t3\t4\t4\t4\t2\t0
                            1970-01-01T00:00:00.000002Z\t1\tnull\t1.0\t2.0\t0\t0\tnull\t2\t3\t2\t3\t3\t3\t2\t0
                            """),
                    "select ts, i, j, avg, sum, first_value, first_value_ignore_nulls, last_value, last_value_ignore_nulls, count, count1, count2, count3, count4, max, min " +
                            "from ( " +
                            "select ts, i, j,n, " +
                            "avg(j) over (partition by i order by ts range between 1 microseconds preceding and current row) as avg, " +
                            "sum(j) over (partition by i order by ts range between 1 microseconds preceding and current row) as sum, " +
                            "first_value(j) over (partition by i order by ts range between 1 microseconds preceding and current row) as first_value, " +
                            "first_value(j) ignore nulls over (partition by i order by ts range between 1 microseconds preceding and current row) as first_value_ignore_nulls, " +
                            "last_value(j) over (partition by i order by ts range between 1 microseconds preceding and current row) as last_value, " +
                            "last_value(j) ignore nulls over (partition by i order by ts range between 1 microseconds preceding and current row) as last_value_ignore_nulls, " +
                            "count(*) over (partition by i order by ts range between 1 microseconds preceding and current row) as count, " +
                            "count(j) over (partition by i order by ts range between 1 microseconds preceding and current row) as count1, " +
                            "count(s) over (partition by i order by ts range between 1 microseconds preceding and current row) as count2, " +
                            "count(d) over (partition by i order by ts range between 1 microseconds preceding and current row) as count3, " +
                            "count(c) over (partition by i order by ts range between 1 microseconds preceding and current row) as count4, " +
                            "max(j) over (partition by i order by ts range between 1 microseconds preceding and current row) as max, " +
                            "min(j) over (partition by i order by ts range between 1 microseconds preceding and current row) as min " +
                            "from dups2 " +
                            "limit 10" +
                            ") order by i, n",
                    null,
                    true,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                            1970-01-01T00:00:00.000002Z\t1\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t1\t0\t1\t1\t1\tnull\tnull
                            1970-01-01T00:00:00.000001Z\t1\t2\t2.0\t2.0\tnull\t2\t2\t2\t2\t1\t2\t2\t2\t2\t2
                            1970-01-01T00:00:00.000001Z\t1\t0\t1.0\t2.0\tnull\t2\t0\t0\t3\t2\t3\t3\t3\t2\t0
                            1970-01-01T00:00:00.000000Z\t1\tnull\t1.0\t2.0\t2\t2\tnull\t0\t3\t2\t3\t3\t3\t2\t0
                            1970-01-01T00:00:00.000000Z\t1\t1\t1.0\t3.0\t2\t2\t1\t1\t4\t3\t4\t4\t4\t2\t0
                            1970-01-01T00:00:00.000002Z\t0\t0\t0.0\t0.0\t0\t0\t0\t0\t1\t1\t1\t1\t1\t0\t0
                            1970-01-01T00:00:00.000002Z\t0\t3\t1.5\t3.0\t0\t0\t3\t3\t2\t2\t2\t2\t2\t3\t0
                            1970-01-01T00:00:00.000001Z\t0\tnull\t1.5\t3.0\t0\t0\tnull\t3\t3\t2\t3\t3\t3\t3\t0
                            1970-01-01T00:00:00.000001Z\t0\t4\t2.3333333333333335\t7.0\t0\t0\t4\t4\t4\t3\t4\t4\t4\t4\t0
                            1970-01-01T00:00:00.000000Z\t0\t2\t3.0\t6.0\tnull\t4\t2\t2\t3\t2\t3\t3\t3\t4\t2
                            """),
                    "select ts, i, j, avg, sum, first_value, first_value_ignore_nulls, last_value, last_value_ignore_nulls, count, count1, count2, count3, count4, max, min " +
                            "from ( " +
                            "select ts, i, j,n, " +
                            "avg(j) over (partition by i order by ts desc range between 1 microseconds preceding and current row) as avg, " +
                            "sum(j) over (partition by i order by ts desc range between 1 microseconds preceding and current row) as sum, " +
                            "first_value(j) over (partition by i order by ts desc range between 1 microseconds preceding and current row) as first_value, " +
                            "first_value(j) ignore nulls over (partition by i order by ts desc range between 1 microseconds preceding and current row) as first_value_ignore_nulls, " +
                            "last_value(j) over (partition by i order by ts desc range between 1 microseconds preceding and current row) as last_value, " +
                            "last_value(j) ignore nulls over (partition by i order by ts desc range between 1 microseconds preceding and current row) as last_value_ignore_nulls, " +
                            "count(*) over (partition by i order by ts desc range between 1 microseconds preceding and current row) as count, " +
                            "count(j) over (partition by i order by ts desc range between 1 microseconds preceding and current row) as count1, " +
                            "count(s) over (partition by i order by ts desc range between 1 microseconds preceding and current row) as count2, " +
                            "count(d) over (partition by i order by ts desc range between 1 microseconds preceding and current row) as count3, " +
                            "count(c) over (partition by i order by ts desc range between 1 microseconds preceding and current row) as count4, " +
                            "max(j) over (partition by i order by ts desc range between 1 microseconds preceding and current row) as max, " +
                            "min(j) over (partition by i order by ts desc range between 1 microseconds preceding and current row) as min " +
                            "from dups2 " +
                            "order by ts " +
                            "desc limit 10" +
                            ") order by i desc, n desc",
                    null,
                    true,
                    true
            );

            String dupResult3 = replaceTimestampSuffix("""
                    ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                    1970-01-01T00:00:00.000000Z\t0\t2\t2.0\t2.0\t2\t2\t2\t2\t1\t1\t1\t1\t1\t2\t2
                    1970-01-01T00:00:00.000001Z\t0\t4\t3.0\t6.0\t2\t2\t4\t4\t2\t2\t2\t2\t2\t4\t2
                    1970-01-01T00:00:00.000001Z\t0\tnull\t3.0\t6.0\t2\t2\tnull\t4\t3\t2\t3\t3\t3\t4\t2
                    1970-01-01T00:00:00.000002Z\t0\t3\t3.0\t9.0\t2\t2\t3\t3\t4\t3\t4\t4\t4\t4\t2
                    1970-01-01T00:00:00.000002Z\t0\t0\t2.25\t9.0\t2\t2\t0\t0\t5\t4\t5\t5\t5\t4\t0
                    1970-01-01T00:00:00.000000Z\t1\t1\t1.0\t1.0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1
                    1970-01-01T00:00:00.000000Z\t1\tnull\t1.0\t1.0\t1\t1\tnull\t1\t2\t1\t2\t2\t2\t1\t1
                    1970-01-01T00:00:00.000001Z\t1\t0\t0.5\t1.0\t1\t1\t0\t0\t3\t2\t3\t3\t3\t1\t0
                    1970-01-01T00:00:00.000001Z\t1\t2\t1.0\t3.0\t1\t1\t2\t2\t4\t3\t4\t4\t4\t2\t0
                    1970-01-01T00:00:00.000002Z\t1\tnull\t1.0\t3.0\t1\t1\tnull\t2\t5\t3\t5\t5\t5\t2\t0
                    """);

            assertQueryNoLeakCheck(
                    dupResult3,
                    "select ts, i, j, avg, sum, first_value, first_value_ignore_nulls, last_value, last_value_ignore_nulls, count, count1, count2, count3, count4, max, min " +
                            "from ( " +
                            "select ts, i, j, n, " +
                            "avg(j) over (partition by i order by ts range between 4 microseconds preceding and current row) avg, " +
                            "sum(j) over (partition by i order by ts range between 4 microseconds preceding and current row) sum, " +
                            "first_value(j) over (partition by i order by ts range between 4 microseconds preceding and current row) first_value, " +
                            "first_value(j) ignore nulls over (partition by i order by ts range between 4 microseconds preceding and current row) first_value_ignore_nulls, " +
                            "last_value(j) over (partition by i order by ts range between 4 microseconds preceding and current row) last_value, " +
                            "last_value(j) ignore nulls over (partition by i order by ts range between 4 microseconds preceding and current row) last_value_ignore_nulls, " +
                            "count(*) over (partition by i order by ts range between 4 microseconds preceding and current row) count, " +
                            "count(j) over (partition by i order by ts range between 4 microseconds preceding and current row) count1, " +
                            "count(s) over (partition by i order by ts range between 4 microseconds preceding and current row) count2, " +
                            "count(d) over (partition by i order by ts range between 4 microseconds preceding and current row) count3, " +
                            "count(c) over (partition by i order by ts range between 4 microseconds preceding and current row) count4, " +
                            "max(j) over (partition by i order by ts range between 4 microseconds preceding and current row) max, " +
                            "min(j) over (partition by i order by ts range between 4 microseconds preceding and current row) min " +
                            "from dups2 " +
                            "order by ts " +
                            "limit 10" +
                            ") order by i, n",
                    null,
                    true,
                    true
            );

            assertQueryNoLeakCheck(
                    dupResult3,
                    "select ts, i, j,avg, sum, first_value, first_value_ignore_nulls, last_value, last_value_ignore_nulls, count, count1, count2, count3, count4, max, min " +
                            "from ( " +
                            "select ts, i, j, n, " +
                            "avg(j) over (partition by i order by ts range between unbounded preceding and current row) avg, " +
                            "sum(j) over (partition by i order by ts range between unbounded preceding and current row) sum, " +
                            "first_value(j) over (partition by i order by ts range between unbounded preceding and current row) first_value, " +
                            "first_value(j) ignore nulls over (partition by i order by ts range between unbounded preceding and current row) first_value_ignore_nulls, " +
                            "last_value(j) over (partition by i order by ts range between unbounded preceding and current row) last_value, " +
                            "last_value(j) ignore nulls over (partition by i order by ts range between unbounded preceding and current row) last_value_ignore_nulls, " +
                            "count(*) over (partition by i order by ts range between unbounded preceding and current row) count, " +
                            "count(j) over (partition by i order by ts range between unbounded preceding and current row) count1, " +
                            "count(s) over (partition by i order by ts range between unbounded preceding and current row) count2, " +
                            "count(d) over (partition by i order by ts range between unbounded preceding and current row) count3, " +
                            "count(c) over (partition by i order by ts range between unbounded preceding and current row) count4, " +
                            "max(j) over (partition by i order by ts range between unbounded preceding and current row) max, " +
                            "min(j) over (partition by i order by ts range between unbounded preceding and current row) min " +
                            "from dups2 " +
                            "order by ts " +
                            "limit 10" +
                            ") order by i, n",
                    null,
                    true,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                            1970-01-01T00:00:00.000000Z\t0\t2\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000001Z\t0\t4\t2.0\t2.0\t2\t2\t2\t2\t1\t1\t1\t1\t1\t2\t2
                            1970-01-01T00:00:00.000001Z\t0\tnull\t2.0\t2.0\t2\t2\t2\t2\t1\t1\t1\t1\t1\t2\t2
                            1970-01-01T00:00:00.000002Z\t0\t3\t3.0\t6.0\t2\t2\tnull\t4\t3\t2\t3\t3\t3\t4\t2
                            1970-01-01T00:00:00.000002Z\t0\t0\t3.0\t6.0\t2\t2\tnull\t4\t3\t2\t3\t3\t3\t4\t2
                            1970-01-01T00:00:00.000000Z\t1\t1\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000000Z\t1\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000001Z\t1\t0\t1.0\t1.0\t1\t1\tnull\t1\t2\t1\t2\t2\t2\t1\t1
                            1970-01-01T00:00:00.000001Z\t1\t2\t1.0\t1.0\t1\t1\tnull\t1\t2\t1\t2\t2\t2\t1\t1
                            1970-01-01T00:00:00.000002Z\t1\tnull\t1.0\t3.0\t1\t1\t2\t2\t4\t3\t4\t4\t4\t2\t0
                            """),
                    "select ts, i, j, avg, sum, first_value, first_value_ignore_nulls, last_value, last_value_ignore_nulls, count, count1, count2, count3, count4, max, min from ( " +
                            "select ts, i, j, n, " +
                            "avg(j) over (partition by i order by ts range between unbounded preceding and 1 microseconds preceding) avg, " +
                            "sum(j) over (partition by i order by ts range between unbounded preceding and 1 microseconds preceding) sum, " +
                            "first_value(j) over (partition by i order by ts range between unbounded preceding and 1 microseconds preceding) first_value, " +
                            "first_value(j) ignore nulls over (partition by i order by ts range between unbounded preceding and 1 microseconds preceding) first_value_ignore_nulls, " +
                            "last_value(j) over (partition by i order by ts range between unbounded preceding and 1 microseconds preceding) last_value, " +
                            "last_value(j) ignore nulls over (partition by i order by ts range between unbounded preceding and 1 microseconds preceding) last_value_ignore_nulls, " +
                            "count(*) over (partition by i order by ts range between unbounded preceding and 1 microseconds preceding) count, " +
                            "count(j) over (partition by i order by ts range between unbounded preceding and 1 microseconds preceding) count1, " +
                            "count(s) over (partition by i order by ts range between unbounded preceding and 1 microseconds preceding) count2, " +
                            "count(d) over (partition by i order by ts range between unbounded preceding and 1 microseconds preceding) count3, " +
                            "count(c) over (partition by i order by ts range between unbounded preceding and 1 microseconds preceding) count4, " +
                            "max(j) over (partition by i order by ts range between unbounded preceding and 1 microseconds preceding) max, " +
                            "min(j) over (partition by i order by ts range between unbounded preceding and 1 microseconds preceding) min " +
                            "from dups2 " +
                            "order by ts " +
                            "limit 10" +
                            ") order by i, n",
                    null,
                    true,
                    true
            );

            String dupResult4 = replaceTimestampSuffix("""
                    ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                    1970-01-01T00:00:00.000002Z\t1\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t1\t0\t1\t1\t1\tnull\tnull
                    1970-01-01T00:00:00.000001Z\t1\t2\t2.0\t2.0\tnull\t2\t2\t2\t2\t1\t2\t2\t2\t2\t2
                    1970-01-01T00:00:00.000001Z\t1\t0\t1.0\t2.0\tnull\t2\t0\t0\t3\t2\t3\t3\t3\t2\t0
                    1970-01-01T00:00:00.000000Z\t1\tnull\t1.0\t2.0\tnull\t2\tnull\t0\t4\t2\t4\t4\t4\t2\t0
                    1970-01-01T00:00:00.000000Z\t1\t1\t1.0\t3.0\tnull\t2\t1\t1\t5\t3\t5\t5\t5\t2\t0
                    1970-01-01T00:00:00.000002Z\t0\t0\t0.0\t0.0\t0\t0\t0\t0\t1\t1\t1\t1\t1\t0\t0
                    1970-01-01T00:00:00.000002Z\t0\t3\t1.5\t3.0\t0\t0\t3\t3\t2\t2\t2\t2\t2\t3\t0
                    1970-01-01T00:00:00.000001Z\t0\tnull\t1.5\t3.0\t0\t0\tnull\t3\t3\t2\t3\t3\t3\t3\t0
                    1970-01-01T00:00:00.000001Z\t0\t4\t2.3333333333333335\t7.0\t0\t0\t4\t4\t4\t3\t4\t4\t4\t4\t0
                    1970-01-01T00:00:00.000000Z\t0\t2\t2.25\t9.0\t0\t0\t2\t2\t5\t4\t5\t5\t5\t4\t0
                    """);

            assertQueryNoLeakCheck(
                    dupResult4,
                    "select ts,i,j,avg, sum, first_value, first_value_ignore_nulls, last_value, last_value_ignore_nulls, count, count1, count2, count3, count4, max, min " +
                            "from ( " +
                            "select ts, i, j, n, " +
                            "avg(j) over (partition by i order by ts desc range between 4 microseconds preceding and current row) avg, " +
                            "sum(j) over (partition by i order by ts desc range between 4 microseconds preceding and current row) sum, " +
                            "first_value(j) over (partition by i order by ts desc range between 4 microseconds preceding and current row) first_value, " +
                            "first_value(j) ignore nulls over (partition by i order by ts desc range between 4 microseconds preceding and current row) first_value_ignore_nulls, " +
                            "last_value(j) over (partition by i order by ts desc range between 4 microseconds preceding and current row) last_value, " +
                            "last_value(j) ignore nulls over (partition by i order by ts desc range between 4 microseconds preceding and current row) last_value_ignore_nulls, " +
                            "count(*) over (partition by i order by ts desc range between 4 microseconds preceding and current row) count, " +
                            "count(j) over (partition by i order by ts desc range between 4 microseconds preceding and current row) count1, " +
                            "count(s) over (partition by i order by ts desc range between 4 microseconds preceding and current row) count2, " +
                            "count(d) over (partition by i order by ts desc range between 4 microseconds preceding and current row) count3, " +
                            "count(c) over (partition by i order by ts desc range between 4 microseconds preceding and current row) count4, " +
                            "max(j) over (partition by i order by ts desc range between 4 microseconds preceding and current row) max, " +
                            "min(j) over (partition by i order by ts desc range between 4 microseconds preceding and current row) min " +
                            "from dups2 " +
                            "order by ts desc " +
                            "limit 10" +
                            ") order by i desc, n desc",
                    null,
                    true,
                    true
            );

            assertQueryNoLeakCheck(
                    dupResult4,
                    "select ts,i,j,avg, sum, first_value, first_value_ignore_nulls, last_value, last_value_ignore_nulls, count, count1, count2, count3, count4, max, min " +
                            "from ( " +
                            "select ts, i, j, n, " +
                            "avg(j) over (partition by i order by ts desc range between unbounded preceding and current row) avg, " +
                            "sum(j) over (partition by i order by ts desc range between unbounded preceding and current row) sum, " +
                            "first_value(j) over (partition by i order by ts desc range between unbounded preceding and current row) first_value, " +
                            "first_value(j) ignore nulls over (partition by i order by ts desc range between unbounded preceding and current row) first_value_ignore_nulls, " +
                            "last_value(j) over (partition by i order by ts desc range between unbounded preceding and current row) last_value, " +
                            "last_value(j) ignore nulls over (partition by i order by ts desc range between unbounded preceding and current row) last_value_ignore_nulls, " +
                            "count(*) over (partition by i order by ts desc range between unbounded preceding and current row) count, " +
                            "count(j) over (partition by i order by ts desc range between unbounded preceding and current row) count1, " +
                            "count(s) over (partition by i order by ts desc range between unbounded preceding and current row) count2, " +
                            "count(d) over (partition by i order by ts desc range between unbounded preceding and current row) count3, " +
                            "count(c) over (partition by i order by ts desc range between unbounded preceding and current row) count4, " +
                            "max(j) over (partition by i order by ts desc range between unbounded preceding and current row) max, " +
                            "min(j) over (partition by i order by ts desc range between unbounded preceding and current row) min " +
                            "from dups2 " +
                            "order by ts desc " +
                            "limit 10" +
                            ") order by i desc, n desc",
                    null,
                    true,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                            1970-01-01T00:00:00.000002Z\t1\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000001Z\t1\t2\tnull\tnull\tnull\tnull\tnull\tnull\t1\t0\t1\t1\t1\tnull\tnull
                            1970-01-01T00:00:00.000001Z\t1\t0\tnull\tnull\tnull\tnull\tnull\tnull\t1\t0\t1\t1\t1\tnull\tnull
                            1970-01-01T00:00:00.000000Z\t1\tnull\t1.0\t2.0\tnull\t2\t0\t0\t3\t2\t3\t3\t3\t2\t0
                            1970-01-01T00:00:00.000000Z\t1\t1\t1.0\t2.0\tnull\t2\t0\t0\t3\t2\t3\t3\t3\t2\t0
                            1970-01-01T00:00:00.000002Z\t0\t0\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000002Z\t0\t3\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000001Z\t0\tnull\t1.5\t3.0\t0\t0\t3\t3\t2\t2\t2\t2\t2\t3\t0
                            1970-01-01T00:00:00.000001Z\t0\t4\t1.5\t3.0\t0\t0\t3\t3\t2\t2\t2\t2\t2\t3\t0
                            1970-01-01T00:00:00.000000Z\t0\t2\t2.3333333333333335\t7.0\t0\t0\t4\t4\t4\t3\t4\t4\t4\t4\t0
                            """),
                    "select ts,i,j,avg, sum, first_value, first_value_ignore_nulls, last_value, last_value_ignore_nulls, count, count1, count2, count3, count4, max, min " +
                            "from ( " +
                            "select ts, i, j, n, " +
                            "avg(j) over (partition by i order by ts desc range between unbounded preceding and 1 microseconds preceding) avg, " +
                            "sum(j) over (partition by i order by ts desc range between unbounded preceding and 1 microseconds preceding) sum, " +
                            "first_value(j) over (partition by i order by ts desc range between unbounded preceding and 1 microseconds preceding) first_value, " +
                            "first_value(j) ignore nulls over (partition by i order by ts desc range between unbounded preceding and 1 microseconds preceding) first_value_ignore_nulls, " +
                            "last_value(j) over (partition by i order by ts desc range between unbounded preceding and 1 preceding) last_value, " +
                            "last_value(j) ignore nulls over (partition by i order by ts desc range between unbounded preceding and 1 microseconds preceding) last_value_ignore_nulls, " +
                            "count(*) over (partition by i order by ts desc range between unbounded preceding and 1 microseconds preceding) count, " +
                            "count(j) over (partition by i order by ts desc range between unbounded preceding and 1 microseconds preceding) count1, " +
                            "count(s) over (partition by i order by ts desc range between unbounded preceding and 1 microseconds preceding) count2, " +
                            "count(d) over (partition by i order by ts desc range between unbounded preceding and 1 microseconds preceding) count3, " +
                            "count(c) over (partition by i order by ts desc range between unbounded preceding and 1 microseconds preceding) count4, " +
                            "max(j) over (partition by i order by ts desc range between unbounded preceding and 1 microseconds preceding) max, " +
                            "min(j) over (partition by i order by ts desc range between unbounded preceding and 1 microseconds preceding) min " +
                            "from dups2 " +
                            "order by ts desc " +
                            "limit 10" +
                            ") order by i desc, n desc",
                    null,
                    true,
                    true
            );

            // table without designated timestamp
            executeWithRewriteTimestamp("create table nodts(ts #TIMESTAMP, i long, j long, s symbol, d double, c VARCHAR)", timestampType.getTypeName());
            execute("insert into nodts select (x/2)::timestamp, x%2, case when x % 3 = 0 THEN NULL ELSE x%5 END, 'k' || (x%5) ::symbol, x*2::double, 'k' || x from long_sequence(10)");
            // timestamp ascending order is declared using timestamp(ts) clause
            assertQueryNoLeakCheck(
                    dupResult,
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts range between 4 microseconds preceding and current row), " +
                            "sum(j) over (partition by i order by ts range between 4 microseconds preceding and current row), " +
                            "first_value(j) over (partition by i order by ts range between 4 microseconds preceding and current row), " +
                            "first_value(j) ignore nulls over (partition by i order by ts range between 4 microseconds preceding and current row), " +
                            "last_value(j) over (partition by i order by ts range between 4 microseconds preceding and current row), " +
                            "last_value(j) ignore nulls over (partition by i order by ts range between 4 microseconds preceding and current row), " +
                            "count(*) over (partition by i order by ts range between 4 microseconds preceding and current row), " +
                            "count(j) over (partition by i order by ts range between 4 microseconds preceding and current row), " +
                            "count(s) over (partition by i order by ts range between 4 microseconds preceding and current row), " +
                            "count(d) over (partition by i order by ts range between 4 microseconds preceding and current row), " +
                            "count(c) over (partition by i order by ts range between 4 microseconds preceding and current row), " +
                            "max(j) over (partition by i order by ts range between 4 microseconds preceding and current row), " +
                            "min(j) over (partition by i order by ts range between 4 microseconds preceding and current row) " +
                            "from nodts timestamp(ts)",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    dupResult,
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts range between unbounded preceding and current row), " +
                            "sum(j) over (partition by i order by ts range between unbounded preceding and current row), " +
                            "first_value(j) over (partition by i order by ts range between unbounded preceding and current row), " +
                            "first_value(j) ignore nulls over (partition by i order by ts range between unbounded preceding and current row), " +
                            "last_value(j) over (partition by i order by ts range between unbounded preceding and current row), " +
                            "last_value(j) ignore nulls over (partition by i order by ts range between unbounded preceding and current row), " +
                            "count(*) over (partition by i order by ts range between unbounded preceding and current row), " +
                            "count(j) over (partition by i order by ts range between unbounded preceding and current row), " +
                            "count(s) over (partition by i order by ts range between unbounded preceding and current row), " +
                            "count(d) over (partition by i order by ts range between unbounded preceding and current row), " +
                            "count(c) over (partition by i order by ts range between unbounded preceding and current row), " +
                            "max(j) over (partition by i order by ts range between unbounded preceding and current row), " +
                            "min(j) over (partition by i order by ts range between unbounded preceding and current row) " +
                            "from nodts timestamp(ts)",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                            1970-01-01T00:00:00.000001Z\t0\t1\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000002Z\t0\t2\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000003Z\t0\tnull\t1.0\t1.0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1
                            1970-01-01T00:00:00.000004Z\t1\t4\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000005Z\t1\t0\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000006Z\t1\tnull\t4.0\t4.0\t4\t4\t4\t4\t1\t1\t1\t1\t1\t4\t4
                            1970-01-01T00:00:00.000007Z\t1\t2\t2.0\t4.0\t4\t4\t0\t0\t2\t2\t2\t2\t2\t4\t0
                            """),
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts range between 1 second preceding and 2 microsecond preceding), " +
                            "sum(j) over (partition by i order by ts range between 1 second preceding and 2 microsecond preceding), " +
                            "first_value(j) over (partition by i order by ts range between 1 second preceding and 2 microsecond preceding), " +
                            "first_value(j) ignore nulls over (partition by i order by ts range between 1 second preceding and 2 microsecond preceding), " +
                            "last_value(j) over (partition by i order by ts range between 1 second preceding and 2 microsecond preceding), " +
                            "last_value(j) ignore nulls over (partition by i order by ts range between 1 second preceding and 2 microsecond preceding), " +
                            "count(*) over (partition by i order by ts range between 1 second preceding and 2 microsecond preceding), " +
                            "count(j) over (partition by i order by ts range between 1 second preceding and 2 microsecond preceding), " +
                            "count(s) over (partition by i order by ts range between 1 second preceding and 2 microsecond preceding), " +
                            "count(d) over (partition by i order by ts range between 1 second preceding and 2 microsecond preceding), " +
                            "count(c) over (partition by i order by ts range between 1 second preceding and 2 microsecond preceding), " +
                            "max(j) over (partition by i order by ts range between 1 second preceding and 2 microsecond preceding), " +
                            "min(j) over (partition by i order by ts range between 1 second preceding and 2 microsecond preceding) " +
                            "from tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testFrameFunctionOverRangeIsOnlySupportedOverDesignatedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            // table without designated timestamp
            executeWithRewriteTimestamp("create table nodts(ts #TIMESTAMP, i long, j long, s symbol, d double, c VARCHAR)", timestampType.getTypeName());

            //table with designated timestamp
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, j long, s symbol, d double, c VARCHAR, otherTs timestamp) timestamp(ts) partition by month", timestampType.getTypeName());

            for (int i = 0, size = FRAME_FUNCTIONS.size(); i < size; i++) {
                String func = FRAME_FUNCTIONS.get(i);
                for (String column : FRAME_FUNCTIONS_PARAMETER_COLUMN_NAME[i]) {
                    assertExceptionNoLeakCheck(
                            "select ts, i, j, #FUNCT_NAME over (order by ts range between 4 preceding and current row) from nodts".replace("#FUNCT_NAME", func).replace("#COLUMN", column),
                            61,
                            "RANGE is supported only for queries ordered by designated timestamp"
                    );

                    assertExceptionNoLeakCheck(
                            "select ts, i, j, #FUNCT_NAME over (partition by i order by ts range between 4 preceding and current row) from nodts".replace("#FUNCT_NAME", func).replace("#COLUMN", column),
                            76,
                            "RANGE is supported only for queries ordered by designated timestamp"
                    );

                    // while it's possible to declare ascending designated timestamp order, it's not possible to declare descending order
                    assertExceptionNoLeakCheck(
                            "select ts, i, j, #FUNCT_NAME over (partition by i order by ts desc range between 4 preceding and current row) from nodts timestamp(ts)".replace("#FUNCT_NAME", func).replace("#COLUMN", column),
                            76,
                            "RANGE is supported only for queries ordered by designated timestamp"
                    );

                    assertExceptionNoLeakCheck(
                            "select ts, i, j, #FUNCT_NAME over (partition by i order by j desc range between unbounded preceding and 10 microsecond preceding) ".replace("#FUNCT_NAME", func).replace("#COLUMN", column) +
                                    "from tab order by ts desc",
                            76,
                            "RANGE is supported only for queries ordered by designated timestamp"
                    );

                    assertExceptionNoLeakCheck(
                            "select ts, i, j, #FUNCT_NAME over (partition by i order by j range 10 microsecond preceding) from tab".replace("#FUNCT_NAME", func).replace("#COLUMN", column),
                            76,
                            "RANGE is supported only for queries ordered by designated timestamp"
                    );

                    // order by column_number doesn't work with in over clause so 1 is treated as integer constant
                    assertExceptionNoLeakCheck(
                            "select ts, i, j, #FUNCT_NAME over (partition by i order by 1 range 10 microsecond preceding) from tab".replace("#FUNCT_NAME", func).replace("#COLUMN", column),
                            76,
                            "RANGE is supported only for queries ordered by designated timestamp"
                    );

                    assertExceptionNoLeakCheck(
                            "select ts, i, j, #FUNCT_NAME over (partition by i order by ts+i range 10 microsecond preceding) from tab".replace("#FUNCT_NAME", func).replace("#COLUMN", column),
                            78,
                            "RANGE is supported only for queries ordered by designated timestamp"
                    );

                    assertExceptionNoLeakCheck(
                            "select ts, i, j, #FUNCT_NAME over (partition by i order by otherTs range 10 microsecond preceding) from tab".replace("#FUNCT_NAME", func).replace("#COLUMN", column),
                            76,
                            "RANGE is supported only for queries ordered by designated timestamp"
                    );

                    assertExceptionNoLeakCheck(
                            "select ts, i, j, #FUNCT_NAME over (partition by i order by ts range 10 microsecond preceding) from tab timestamp(otherTs)".replace("#FUNCT_NAME", func).replace("#COLUMN", column),
                            76,
                            "RANGE is supported only for queries ordered by designated timestamp"
                    );

                    assertExceptionNoLeakCheck(
                            "select ts, i, j, #FUNCT_NAME over (partition by i order by otherTs desc range 10 microsecond preceding) from tab timestamp(otherTs)".replace("#FUNCT_NAME", func).replace("#COLUMN", column),
                            76,
                            "RANGE is supported only for queries ordered by designated timestamp"
                    );
                }
            }
        });
    }

    @Test
    public void testFrameFunctionRejectsExclusionModesOtherThanDefault() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table xyz (a int, b int, c int, ts #TIMESTAMP) timestamp(ts)", timestampType.getTypeName());

            for (String function : FRAME_FUNCTIONS) {
                for (String exclusionMode : new String[]{"GROUP", "TIES"}) {
                    assertWindowException(
                            "select a,b, #FUNCT_NAME over (partition by b order by ts #FRAME UNBOUNDED PRECEDING EXCLUDE #mode) from xyz"
                                    .replace("#FUNCT_NAME", function)
                                    .replace("#COLUMN", "c")
                                    .replace("#mode", exclusionMode),
                            101,
                            "only EXCLUDE NO OTHERS and EXCLUDE CURRENT ROW exclusion modes are supported"
                    );

                    assertWindowException(
                            "select a,b, #FUNCT_NAME over (partition by b order by ts #FRAME BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE #mode) from xyz"
                                    .replace("#FUNCT_NAME", function)
                                    .replace("#COLUMN", "c")
                                    .replace("#mode", exclusionMode),
                            125,
                            "only EXCLUDE NO OTHERS and EXCLUDE CURRENT ROW exclusion modes are supported"
                    );
                }

                assertWindowException(
                        "select a,b, #FUNCT_NAME over (partition by b order by ts #FRAME BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) from xyz"
                                .replace("#FUNCT_NAME", function).replace("#COLUMN", "c"),
                        133,
                        "EXCLUDE CURRENT ROW not supported with UNBOUNDED FOLLOWING frame boundary"
                );
            }
        });
    }

    @Test
    public void testFrameFunctionRejectsFramesThatUseFollowing() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, j long, s symbol, d double, c VARCHAR) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab select x::timestamp, x/4, x%5, 'k' || (x%5) ::symbol, x*2::double, 'k' || x from long_sequence(7)");

            for (int i = 0, size = FRAME_FUNCTIONS.size(); i < size; i++) {
                String func = FRAME_FUNCTIONS.get(i);
                for (String column : FRAME_FUNCTIONS_PARAMETER_COLUMN_NAME[i]) {
                    assertExceptionNoLeakCheck(
                            "select ts, i, j, #FUNCT_NAME over (partition by i order by ts rows between 10 following and 20 following) from tab".replace("#FUNCT_NAME", func).replace("#COLUMN", column),
                            95,
                            "frame start supports UNBOUNDED PRECEDING, _number_ PRECEDING and CURRENT ROW only"
                    );
                    assertExceptionNoLeakCheck(
                            "select ts, i, j, #FUNCT_NAME over (partition by i order by ts rows between 10 preceding and 1 following) from tab".replace("#FUNCT_NAME", func).replace("#COLUMN", column),
                            111,
                            "frame end supports _number_ PRECEDING and CURRENT ROW only"
                    );
                    assertExceptionNoLeakCheck(
                            "select ts, i, j, #FUNCT_NAME over (partition by i order by ts rows between 10 preceding and unbounded following) from tab".replace("#FUNCT_NAME", func).replace("#COLUMN", column),
                            119, "frame end supports UNBOUNDED FOLLOWING only when frame start is UNBOUNDED PRECEDING"
                    );

                    assertExceptionNoLeakCheck(
                            "select ts, i, j, #FUNCT_NAME over (partition by i order by ts groups between 10 following and 20 following) from tab".replace("#FUNCT_NAME", func).replace("#COLUMN", column),
                            97,
                            "frame start supports UNBOUNDED PRECEDING, _number_ PRECEDING and CURRENT ROW only"
                    );
                    assertExceptionNoLeakCheck(
                            "select ts, i, j, #FUNCT_NAME over (partition by i order by ts groups between 10 preceding and 1 following) from tab".replace("#FUNCT_NAME", func).replace("#COLUMN", column),
                            113,
                            "frame end supports _number_ PRECEDING and CURRENT ROW only"
                    );
                    assertExceptionNoLeakCheck(
                            "select ts, i, j, #FUNCT_NAME over (partition by i order by ts groups between 10 preceding and unbounded following) from tab".replace("#FUNCT_NAME", func).replace("#COLUMN", column),
                            121,
                            "frame end supports UNBOUNDED FOLLOWING only when frame start is UNBOUNDED PRECEDING"
                    );

                    assertExceptionNoLeakCheck(
                            "select ts, i, j, #FUNCT_NAME over (partition by i order by ts range between 10 following and 20 following) from tab".replace("#FUNCT_NAME", func).replace("#COLUMN", column),
                            96,
                            "frame start supports UNBOUNDED PRECEDING, _number_ PRECEDING and CURRENT ROW only"
                    );
                    assertExceptionNoLeakCheck(
                            "select ts, i, j, #FUNCT_NAME over (partition by i order by ts range between 10 preceding and 1 following) from tab".replace("#FUNCT_NAME", func).replace("#COLUMN", column),
                            112,
                            "frame end supports _number_ PRECEDING and CURRENT ROW only"
                    );
                    assertExceptionNoLeakCheck(
                            "select ts, i, j, #FUNCT_NAME over (partition by i order by ts range between 10 preceding and unbounded following) from tab".replace("#FUNCT_NAME", func).replace("#COLUMN", column),
                            120,
                            "frame end supports UNBOUNDED FOLLOWING only when frame start is UNBOUNDED PRECEDING"
                    );
                }
            }
        });
    }

    @Test
    public void testFrameFunctionResolvesSymbolTables() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table  cpu ( hostname symbol, usage_system double )");
            execute("insert into cpu select rnd_symbol('A', 'B', 'C'), x from long_sequence(1000)");

            assertQueryNoLeakCheck(
                    """
                            hostname\tusage_system\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tmax\tmin
                            A\t1.0\t1.0\t1.0\t1.0\t1.0\t1.0\t1.0\t1\t1\t1.0\t1.0
                            A\t2.0\t1.5\t3.0\t1.0\t1.0\t2.0\t2.0\t2\t2\t2.0\t1.0
                            B\t3.0\t3.0\t3.0\t3.0\t3.0\t3.0\t3.0\t1\t1\t3.0\t3.0
                            C\t4.0\t4.0\t4.0\t4.0\t4.0\t4.0\t4.0\t1\t1\t4.0\t4.0
                            C\t5.0\t4.5\t9.0\t4.0\t4.0\t5.0\t5.0\t2\t2\t5.0\t4.0
                            C\t6.0\t5.0\t15.0\t4.0\t4.0\t6.0\t6.0\t3\t3\t6.0\t4.0
                            C\t7.0\t5.5\t22.0\t4.0\t4.0\t7.0\t7.0\t4\t4\t7.0\t4.0
                            B\t8.0\t5.5\t11.0\t3.0\t3.0\t8.0\t8.0\t2\t2\t8.0\t3.0
                            A\t9.0\t4.0\t12.0\t1.0\t1.0\t9.0\t9.0\t3\t3\t9.0\t1.0
                            B\t10.0\t7.0\t21.0\t3.0\t3.0\t10.0\t10.0\t3\t3\t10.0\t3.0
                            """,
                    "select hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname rows between 50 preceding and current row), " +
                            "sum(usage_system) over(partition by hostname rows between 50 preceding and current row), " +
                            "first_value(usage_system) over(partition by hostname rows between 50 preceding and current row), " +
                            "first_value(usage_system) ignore nulls over(partition by hostname rows between 50 preceding and current row), " +
                            "last_value(usage_system) over(partition by hostname rows between 50 preceding and current row), " +
                            "last_value(usage_system) ignore nulls over(partition by hostname rows between 50 preceding and current row), " +
                            "count(*) over(partition by hostname rows between 50 preceding and current row), " +
                            "count(usage_system) over(partition by hostname rows between 50 preceding and current row), " +
                            "max(usage_system) over(partition by hostname rows between 50 preceding and current row), " +
                            "min(usage_system) over(partition by hostname rows between 50 preceding and current row) " +
                            "from cpu " +
                            "limit 10",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testFrameFunctionResolvesSymbolTablesInPartitionByCachedWindow() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (sym symbol, i int);");
            execute("insert into x values ('aaa', NULL);");
            execute("insert into x values ('aaa', 1);");
            execute("insert into x values ('aaa', 2);");

            assertQueryNoLeakCheck(
                    """
                            sym\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tmax\tmin
                            aaa\t1.5\t3.0\tnull\t1\t2\t2\t2\t3\t2\t1
                            aaa\t1.5\t3.0\tnull\t1\t2\t2\t2\t3\t2\t1
                            aaa\t1.5\t3.0\tnull\t1\t2\t2\t2\t3\t2\t1
                            """,
                    "SELECT sym, " +
                            "avg(i) OVER(PARTITION BY sym LIKE '%aaa%'), " +
                            "sum(i) OVER(PARTITION BY sym LIKE '%aaa%'), " +
                            "first_value(i) OVER(PARTITION BY sym LIKE '%aaa%'), " +
                            "first_value(i) ignore nulls OVER(PARTITION BY sym LIKE '%aaa%'), " +
                            "last_value(i) OVER(PARTITION BY sym LIKE '%aaa%'), " +
                            "last_value(i) ignore nulls OVER(PARTITION BY sym LIKE '%aaa%'), " +
                            "count(i) OVER(PARTITION BY sym LIKE '%aaa%'), " +
                            "count(sym) OVER(PARTITION BY sym LIKE '%aaa%'), " +
                            "max(i) OVER(PARTITION BY sym LIKE '%aaa%'), " +
                            "min(i) OVER(PARTITION BY sym LIKE '%aaa%'), " +
                            "FROM x",
                    null,
                    true, // cached window factory
                    true
            );
        });
    }

    @Test
    public void testFrameFunctionResolvesSymbolTablesInPartitionByNonCachedWindow() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table x (sym symbol, i int, ts #TIMESTAMP) timestamp(ts) partition by day;", timestampType.getTypeName());
            execute("insert into x values ('aaa', NULL, '2023-11-09T00:00:00.000000');");
            execute("insert into x values ('aaa', 1, '2023-11-09T01:00:00.000000');");
            execute("insert into x values ('aaa', 2, '2023-11-09T02:00:00.000000');");

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\tsym\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tmax\tmin
                            2023-11-09T00:00:00.000000Z\taaa\tnull\tnull\tnull\tnull\tnull\tnull\t0\t1\tnull\tnull
                            2023-11-09T01:00:00.000000Z\taaa\t1.0\t1.0\tnull\t1\t1\t1\t1\t2\t1\t1
                            2023-11-09T02:00:00.000000Z\taaa\t1.5\t3.0\tnull\t1\t2\t2\t2\t3\t2\t1
                            """),
                    "SELECT ts, sym, " +
                            "avg(i) OVER(PARTITION BY sym LIKE '%aaa%' ORDER BY ts), " +
                            "sum(i) OVER(PARTITION BY sym LIKE '%aaa%' ORDER BY ts), " +
                            "first_value(i) OVER(PARTITION BY sym LIKE '%aaa%' ORDER BY ts), " +
                            "first_value(i) ignore nulls OVER(PARTITION BY sym LIKE '%aaa%' ORDER BY ts), " +
                            "last_value(i) OVER(PARTITION BY sym LIKE '%aaa%' ORDER BY ts), " +
                            "last_value(i) ignore nulls OVER(PARTITION BY sym LIKE '%aaa%' ORDER BY ts), " +
                            "count(i) OVER(PARTITION BY sym LIKE '%aaa%' ORDER BY ts), " +
                            "count(sym) OVER(PARTITION BY sym LIKE '%aaa%' ORDER BY ts), " +
                            "max(i) OVER(PARTITION BY sym LIKE '%aaa%' ORDER BY ts), " +
                            "min(i) OVER(PARTITION BY sym LIKE '%aaa%' ORDER BY ts), " +
                            "FROM x",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testFrameFunctionsDontSupportGroupFrames() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, j long, s symbol, d double, c VARCHAR) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab select x::timestamp, x/4, x%5, 'k' || (x%5) ::symbol, x*2::double, 'k' || x from long_sequence(7)");

            for (int i = 0, size = FRAME_FUNCTIONS.size(); i < size; i++) {
                String func = FRAME_FUNCTIONS.get(i);
                for (String column : FRAME_FUNCTIONS_PARAMETER_COLUMN_NAME[i]) {
                    assertExceptionNoLeakCheck(
                            "select ts, i, j, #FUNCT_NAME over (partition by i order by ts groups unbounded preceding) from tab".replace("#FUNCT_NAME", func).replace("#COLUMN", column),
                            17,
                            "function not implemented for given window parameters"
                    );
                }
            }
        });
    }

    @Test
    public void testFrameFunctionsOverRowsFrame() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, j long, d double, s symbol, c VARCHAR) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab select x::timestamp, x/4, case when x % 3 = 0 THEN NULL ELSE x%5 END, x%5, 'k' || (x%5) ::symbol, 'k' || x from long_sequence(7)");
            assertSql(
                    replaceTimestampSuffix("""
                            ts\ti\tj
                            1970-01-01T00:00:00.000001Z\t0\t1
                            1970-01-01T00:00:00.000002Z\t0\t2
                            1970-01-01T00:00:00.000003Z\t0\tnull
                            1970-01-01T00:00:00.000004Z\t1\t4
                            1970-01-01T00:00:00.000005Z\t1\t0
                            1970-01-01T00:00:00.000006Z\t1\tnull
                            1970-01-01T00:00:00.000007Z\t1\t2
                            """),
                    "select ts, i, j from tab"
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tmax\tmin
                            1970-01-01T00:00:00.000001Z\t0\t1\t1.0\t1.0\t1\t1\t1\t1\t1\t1\t1\t1\t1.0\t1.0
                            1970-01-01T00:00:00.000002Z\t0\t2\t1.5\t3.0\t1\t1\t2\t2\t2\t2\t2\t2\t2.0\t1.0
                            1970-01-01T00:00:00.000003Z\t0\tnull\t2.0\t6.0\t1\t1\tnull\t2\t3\t3\t3\t3\t3.0\t1.0
                            1970-01-01T00:00:00.000004Z\t1\t4\t2.5\t10.0\t1\t1\t4\t4\t4\t4\t4\t4\t4.0\t1.0
                            1970-01-01T00:00:00.000005Z\t1\t0\t2.0\t10.0\t1\t1\t0\t0\t5\t5\t5\t5\t4.0\t0.0
                            1970-01-01T00:00:00.000006Z\t1\tnull\t1.8333333333333333\t11.0\t1\t1\tnull\t0\t6\t6\t6\t6\t4.0\t0.0
                            1970-01-01T00:00:00.000007Z\t1\t2\t1.8571428571428572\t13.0\t1\t1\t2\t2\t7\t7\t7\t7\t4.0\t0.0
                            """),
                    "select ts, i, j, " +
                            "avg(d) over (order by ts rows unbounded preceding)," +
                            "sum(d) over (order by ts rows unbounded preceding), " +
                            "first_value(j) over (order by ts rows unbounded preceding), " +
                            "first_value(j) ignore nulls over (order by ts rows unbounded preceding), " +
                            "last_value(j) over (order by ts rows unbounded preceding), " +
                            "last_value(j) ignore nulls over (order by ts rows unbounded preceding), " +
                            "count(*) over (order by ts rows unbounded preceding), " +
                            "count(d) over (order by ts rows unbounded preceding), " +
                            "count(s) over (order by ts rows unbounded preceding), " +
                            "count(c) over (order by ts rows unbounded preceding), " +
                            "max(d) over (order by ts rows unbounded preceding), " +
                            "min(d) over (order by ts rows unbounded preceding) " +
                            "from tab",
                    "ts",
                    true,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tmax\tmin
                            1970-01-01T00:00:00.000001Z\t0\t1\t1.0\t1.0\tnull\t1\t1\t1\t2\t2\t2\t2\t1\t1
                            1970-01-01T00:00:00.000002Z\t0\t2\t1.5\t3.0\tnull\t1\t2\t2\t3\t3\t3\t3\t2\t1
                            1970-01-01T00:00:00.000003Z\t0\tnull\tnull\tnull\tnull\t1\tnull\tnull\t1\t1\t1\t1\tnull\tnull
                            1970-01-01T00:00:00.000004Z\t1\t4\t1.8\t9.0\tnull\t1\t4\t4\t7\t7\t7\t7\t4\t0
                            1970-01-01T00:00:00.000005Z\t1\t0\t1.0\t3.0\tnull\t1\t0\t0\t5\t5\t5\t5\t2\t0
                            1970-01-01T00:00:00.000006Z\t1\tnull\t1.5\t3.0\tnull\t1\tnull\t2\t4\t4\t4\t4\t2\t1
                            1970-01-01T00:00:00.000007Z\t1\t2\t1.25\t5.0\tnull\t1\t2\t2\t6\t6\t6\t6\t2\t0
                            """),
                    "select ts, i, j, " +
                            "avg(j) over (order by i, j rows unbounded preceding), " +
                            "sum(j) over (order by i, j rows unbounded preceding), " +
                            "first_value(j) over (order by i, j rows unbounded preceding), " +
                            "first_value(j) ignore nulls over (order by i, j rows unbounded preceding), " +
                            "last_value(j) over (order by i, j rows unbounded preceding), " +
                            "last_value(j) ignore nulls over (order by i, j rows unbounded preceding), " +
                            "count(*) over (order by i, j rows unbounded preceding), " +
                            "count(s) over (order by i, j rows unbounded preceding), " +
                            "count(d) over (order by i, j rows unbounded preceding), " +
                            "count(c) over (order by i, j rows unbounded preceding), " +
                            "max(j) over (order by i, j rows unbounded preceding), " +
                            "min(j) over (order by i, j rows unbounded preceding) " +
                            "from tab",
                    "ts",
                    true,//cached window factory
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tmax\tmin
                            1970-01-01T00:00:00.000001Z\t0\t1\t1.0\t1.0\t1\t1\t1\t1\t1\t1\t1\t1\t1.0\t1.0
                            1970-01-01T00:00:00.000002Z\t0\t2\t2.0\t2.0\t2\t2\t2\t2\t1\t1\t1\t1\t2.0\t2.0
                            1970-01-01T00:00:00.000003Z\t0\tnull\t3.0\t3.0\tnull\tnull\tnull\tnull\t1\t1\t1\t1\t3.0\t3.0
                            1970-01-01T00:00:00.000004Z\t1\t4\t4.0\t4.0\t4\t4\t4\t4\t1\t1\t1\t1\t4.0\t4.0
                            1970-01-01T00:00:00.000005Z\t1\t0\t0.0\t0.0\t0\t0\t0\t0\t1\t1\t1\t1\t0.0\t0.0
                            1970-01-01T00:00:00.000006Z\t1\tnull\t1.0\t1.0\tnull\tnull\tnull\tnull\t1\t1\t1\t1\t1.0\t1.0
                            1970-01-01T00:00:00.000007Z\t1\t2\t2.0\t2.0\t2\t2\t2\t2\t1\t1\t1\t1\t2.0\t2.0
                            """),
                    "select ts, i, j, " +
                            "avg(d) over (order by ts rows current row), " +
                            "sum(d) over (order by ts rows current row), " +
                            "first_value(j) over (order by ts rows current row), " +
                            "first_value(j) ignore nulls over (order by ts rows current row), " +
                            "last_value(j) over (order by ts rows current row), " +
                            "last_value(j) ignore nulls over (order by ts rows current row), " +
                            "count(*) over (order by ts rows current row), " +
                            "count(s) over (order by ts rows current row), " +
                            "count(d) over (order by ts rows current row), " +
                            "count(c) over (order by ts rows current row), " +
                            "max(d) over (order by ts rows current row), " +
                            "min(d) over (order by ts rows current row) " +
                            "from tab",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tmax\tmin
                            1970-01-01T00:00:00.000001Z\t0\t1\t1.0\t1.0\t1\t1\t1\t1\t1\t1\t1\t1\t1.0\t1.0
                            1970-01-01T00:00:00.000002Z\t0\t2\t2.0\t2.0\t2\t2\t2\t2\t1\t1\t1\t1\t2.0\t2.0
                            1970-01-01T00:00:00.000003Z\t0\tnull\t3.0\t3.0\tnull\tnull\tnull\tnull\t1\t1\t1\t1\t3.0\t3.0
                            1970-01-01T00:00:00.000004Z\t1\t4\t4.0\t4.0\t4\t4\t4\t4\t1\t1\t1\t1\t4.0\t4.0
                            1970-01-01T00:00:00.000005Z\t1\t0\t0.0\t0.0\t0\t0\t0\t0\t1\t1\t1\t1\t0.0\t0.0
                            1970-01-01T00:00:00.000006Z\t1\tnull\t1.0\t1.0\tnull\tnull\tnull\tnull\t1\t1\t1\t1\t1.0\t1.0
                            1970-01-01T00:00:00.000007Z\t1\t2\t2.0\t2.0\t2\t2\t2\t2\t1\t1\t1\t1\t2.0\t2.0
                            """),
                    "select ts, i, j, " +
                            "avg(d) over (order by ts desc rows current row), " +
                            "sum(d) over (order by ts desc rows current row), " +
                            "first_value(j) over (order by ts desc rows current row), " +
                            "first_value(j) ignore nulls over (order by ts desc rows current row), " +
                            "last_value(j) over (order by ts desc rows current row), " +
                            "last_value(j) ignore nulls over (order by ts desc rows current row), " +
                            "count(*) over (order by ts desc rows current row), " +
                            "count(s) over (order by ts desc rows current row), " +
                            "count(d) over (order by ts desc rows current row), " +
                            "count(c) over (order by ts desc rows current row), " +
                            "max(d) over (order by ts desc rows current row), " +
                            "min(d) over (order by ts desc rows current row) " +
                            "from tab",
                    "ts",
                    true, // cached window factory
                    true
            );

            assertSql(replaceTimestampSuffix("""
                    ts\ti\tj\td\ts\tc
                    1970-01-01T00:00:00.000001Z\t0\t1\t1.0\tk1\tk1
                    1970-01-01T00:00:00.000002Z\t0\t2\t2.0\tk2\tk2
                    1970-01-01T00:00:00.000003Z\t0\tnull\t3.0\tk3\tk3
                    1970-01-01T00:00:00.000004Z\t1\t4\t4.0\tk4\tk4
                    1970-01-01T00:00:00.000005Z\t1\t0\t0.0\tk0\tk5
                    1970-01-01T00:00:00.000006Z\t1\tnull\t1.0\tk1\tk6
                    1970-01-01T00:00:00.000007Z\t1\t2\t2.0\tk2\tk7
                    """), "tab");

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tmax\tmin
                            1970-01-01T00:00:00.000001Z\t0\t1\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000002Z\t0\t2\t1.0\t1.0\t1\t1\t1\t1\t1\t1\t1\t1\t1.0\t1.0
                            1970-01-01T00:00:00.000003Z\t0\tnull\t1.5\t3.0\t1\t1\t2\t2\t2\t2\t2\t2\t2.0\t1.0
                            1970-01-01T00:00:00.000004Z\t1\t4\t2.0\t6.0\t1\t1\tnull\t2\t3\t3\t3\t3\t3.0\t1.0
                            1970-01-01T00:00:00.000005Z\t1\t0\t2.5\t10.0\t1\t1\t4\t4\t4\t4\t4\t4\t4.0\t1.0
                            1970-01-01T00:00:00.000006Z\t1\tnull\t2.0\t10.0\t1\t1\t0\t0\t5\t5\t5\t5\t4.0\t0.0
                            1970-01-01T00:00:00.000007Z\t1\t2\t1.8333333333333333\t11.0\t1\t1\tnull\t0\t6\t6\t6\t6\t4.0\t0.0
                            """),
                    "select ts, i, j, " +
                            "avg(d) over (order by ts rows between unbounded preceding and 1 preceding), " +
                            "sum(d) over (order by ts rows between unbounded preceding and 1 preceding), " +
                            "first_value(j) over (order by ts rows between unbounded preceding and 1 preceding), " +
                            "first_value(j) ignore nulls over (order by ts rows between unbounded preceding and 1 preceding), " +
                            "last_value(j) over (order by ts rows between unbounded preceding and 1 preceding), " +
                            "last_value(j) ignore nulls over (order by ts rows between unbounded preceding and 1 preceding), " +
                            "count(*) over (order by ts rows between unbounded preceding and 1 preceding), " +
                            "count(s) over (order by ts rows between unbounded preceding and 1 preceding), " +
                            "count(d) over (order by ts rows between unbounded preceding and 1 preceding), " +
                            "count(c) over (order by ts rows between unbounded preceding and 1 preceding), " +
                            "max(d) over (order by ts rows between unbounded preceding and 1 preceding), " +
                            "min(d) over (order by ts rows between unbounded preceding and 1 preceding) " +
                            "from tab",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tmax\tmin
                            1970-01-01T00:00:00.000001Z\t0\t1\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000002Z\t0\t2\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000003Z\t0\tnull\t1.0\t1.0\t1\t1\t1\t1\t1\t1\t1\t1\t1.0\t1.0
                            1970-01-01T00:00:00.000004Z\t1\t4\t1.5\t3.0\t1\t1\t2\t2\t2\t2\t2\t2\t2.0\t1.0
                            1970-01-01T00:00:00.000005Z\t1\t0\t2.0\t6.0\t1\t1\tnull\t2\t3\t3\t3\t3\t3.0\t1.0
                            1970-01-01T00:00:00.000006Z\t1\tnull\t3.0\t9.0\t2\t2\t4\t4\t3\t3\t3\t3\t4.0\t2.0
                            1970-01-01T00:00:00.000007Z\t1\t2\t2.3333333333333335\t7.0\tnull\t4\t0\t0\t3\t3\t3\t3\t4.0\t0.0
                            """),
                    "select ts, i, j, " +
                            "avg(d) over (order by ts rows between 4 preceding and 2 preceding), " +
                            "sum(d) over (order by ts rows between 4 preceding and 2 preceding), " +
                            "first_value(j) over (order by ts rows between 4 preceding and 2 preceding), " +
                            "first_value(j) ignore nulls over (order by ts rows between 4 preceding and 2 preceding), " +
                            "last_value(j) over (order by ts rows between 4 preceding and 2 preceding), " +
                            "last_value(j) ignore nulls over (order by ts rows between 4 preceding and 2 preceding), " +
                            "count(*) over (order by ts rows between 4 preceding and 2 preceding), " +
                            "count(s) over (order by ts rows between 4 preceding and 2 preceding), " +
                            "count(d) over (order by ts rows between 4 preceding and 2 preceding), " +
                            "count(c) over (order by ts rows between 4 preceding and 2 preceding), " +
                            "max(d) over (order by ts rows between 4 preceding and 2 preceding), " +
                            "min(d) over (order by ts rows between 4 preceding and 2 preceding) " +
                            "from tab",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tmax\tmin
                            1970-01-01T00:00:00.000001Z\t0\t1\t2.3333333333333335\t7.0\t0\t0\tnull\t4\t3\t3\t3\t3\t4.0\t0.0
                            1970-01-01T00:00:00.000002Z\t0\t2\t1.6666666666666667\t5.0\tnull\t0\t4\t4\t3\t3\t3\t3\t4.0\t0.0
                            1970-01-01T00:00:00.000003Z\t0\tnull\t1.0\t3.0\t2\t2\t0\t0\t3\t3\t3\t3\t2.0\t0.0
                            1970-01-01T00:00:00.000004Z\t1\t4\t1.5\t3.0\t2\t2\tnull\t2\t2\t2\t2\t2\t2.0\t1.0
                            1970-01-01T00:00:00.000005Z\t1\t0\t2.0\t2.0\t2\t2\t2\t2\t1\t1\t1\t1\t2.0\t2.0
                            1970-01-01T00:00:00.000006Z\t1\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000007Z\t1\t2\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\tnull\tnull
                            """),
                    "select ts, i, j, " +
                            "avg(d) over (order by ts desc rows between 4 preceding and 2 preceding), " +
                            "sum(d) over (order by ts desc rows between 4 preceding and 2 preceding), " +
                            "first_value(j) over (order by ts desc rows between 4 preceding and 2 preceding), " +
                            "first_value(j) ignore nulls over (order by ts desc rows between 4 preceding and 2 preceding), " +
                            "last_value(j) over (order by ts desc rows between 4 preceding and 2 preceding), " +
                            "last_value(j) ignore nulls over (order by ts desc rows between 4 preceding and 2 preceding), " +
                            "count(*) over (order by ts desc rows between 4 preceding and 2 preceding), " +
                            "count(s) over (order by ts desc rows between 4 preceding and 2 preceding), " +
                            "count(d) over (order by ts desc rows between 4 preceding and 2 preceding), " +
                            "count(c) over (order by ts desc rows between 4 preceding and 2 preceding), " +
                            "max(d) over (order by ts desc rows between 4 preceding and 2 preceding), " +
                            "min(d) over (order by ts desc rows between 4 preceding and 2 preceding) " +
                            "from tab",
                    "ts",
                    true, //c ached window factory
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tmax\tmin
                            1970-01-01T00:00:00.000001Z\t0\t1\t1.8571428571428572\t13.0\t1\t1\t4\t4\t7\t7\t7\t7\t4.0\t0.0
                            1970-01-01T00:00:00.000002Z\t0\t2\t1.8571428571428572\t13.0\t1\t1\t4\t4\t7\t7\t7\t7\t4.0\t0.0
                            1970-01-01T00:00:00.000003Z\t0\tnull\t1.8571428571428572\t13.0\t1\t1\t4\t4\t7\t7\t7\t7\t4.0\t0.0
                            1970-01-01T00:00:00.000004Z\t1\t4\t1.8571428571428572\t13.0\t1\t1\t4\t4\t7\t7\t7\t7\t4.0\t0.0
                            1970-01-01T00:00:00.000005Z\t1\t0\t1.8571428571428572\t13.0\t1\t1\t4\t4\t7\t7\t7\t7\t4.0\t0.0
                            1970-01-01T00:00:00.000006Z\t1\tnull\t1.8571428571428572\t13.0\t1\t1\t4\t4\t7\t7\t7\t7\t4.0\t0.0
                            1970-01-01T00:00:00.000007Z\t1\t2\t1.8571428571428572\t13.0\t1\t1\t4\t4\t7\t7\t7\t7\t4.0\t0.0
                            """),
                    "select ts, i, j, " +
                            "avg(d) over (order by i rows between unbounded preceding and unbounded following), " +
                            "sum(d) over (order by i rows between unbounded preceding and unbounded following), " +
                            "first_value(j) over (order by i rows between unbounded preceding and unbounded following), " +
                            "first_value(j) ignore nulls over (order by i rows between unbounded preceding and unbounded following), " +
                            "last_value(j) over (order by i rows between unbounded preceding and unbounded following), " +
                            "last_value(j) ignore nulls over (order by i rows between unbounded preceding and unbounded following), " +
                            "count(*) over (order by i rows between unbounded preceding and unbounded following), " +
                            "count(s) over (order by i rows between unbounded preceding and unbounded following), " +
                            "count(d) over (order by i rows between unbounded preceding and unbounded following), " +
                            "count(c) over (order by i rows between unbounded preceding and unbounded following), " +
                            "max(d) over (order by i rows between unbounded preceding and unbounded following), " +
                            "min(d) over (order by i rows between unbounded preceding and unbounded following) " +
                            "from tab",
                    "ts",
                    true, // cached window factory
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tmax\tmin
                            1970-01-01T00:00:00.000001Z\t0\t1\t2.0\t6.0\t1.0\t1.0\t3.0\t3.0\t3\t3\t3\t3\t3.0\t1.0
                            1970-01-01T00:00:00.000002Z\t0\t2\t2.0\t6.0\t1.0\t1.0\t3.0\t3.0\t3\t3\t3\t3\t3.0\t1.0
                            1970-01-01T00:00:00.000003Z\t0\tnull\t2.0\t6.0\t1.0\t1.0\t3.0\t3.0\t3\t3\t3\t3\t3.0\t1.0
                            1970-01-01T00:00:00.000004Z\t1\t4\t1.75\t7.0\t4.0\t4.0\t2.0\t2.0\t4\t4\t4\t4\t4.0\t0.0
                            1970-01-01T00:00:00.000005Z\t1\t0\t1.75\t7.0\t4.0\t4.0\t2.0\t2.0\t4\t4\t4\t4\t4.0\t0.0
                            1970-01-01T00:00:00.000006Z\t1\tnull\t1.75\t7.0\t4.0\t4.0\t2.0\t2.0\t4\t4\t4\t4\t4.0\t0.0
                            1970-01-01T00:00:00.000007Z\t1\t2\t1.75\t7.0\t4.0\t4.0\t2.0\t2.0\t4\t4\t4\t4\t4.0\t0.0
                            """),
                    "select ts, i, j, " +
                            "avg(d) over (partition by i rows between unbounded preceding and unbounded following), " +
                            "sum(d) over (partition by i rows between unbounded preceding and unbounded following), " +
                            "first_value(d) over (partition by i rows between unbounded preceding and unbounded following), " +
                            "first_value(d) ignore nulls over (partition by i rows between unbounded preceding and unbounded following), " +
                            "last_value(d) over (partition by i rows between unbounded preceding and unbounded following), " +
                            "last_value(d) ignore nulls over (partition by i rows between unbounded preceding and unbounded following), " +
                            "count(*) over (partition by i rows between unbounded preceding and unbounded following), " +
                            "count(s) over (partition by i rows between unbounded preceding and unbounded following), " +
                            "count(d) over (partition by i rows between unbounded preceding and unbounded following), " +
                            "count(c) over (partition by i rows between unbounded preceding and unbounded following), " +
                            "max(d) over (partition by i rows between unbounded preceding and unbounded following), " +
                            "min(d) over (partition by i rows between unbounded preceding and unbounded following) " +
                            "from tab",
                    "ts",
                    true,//cached window factory
                    true
            );

            String rowsResult1 = replaceTimestampSuffix("""
                    ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tmax\tmin
                    1970-01-01T00:00:00.000001Z\t0\t1\t1.0\t1.0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1
                    1970-01-01T00:00:00.000002Z\t0\t2\t1.5\t3.0\t1\t1\t2\t2\t2\t2\t2\t2\t2\t1
                    1970-01-01T00:00:00.000003Z\t0\tnull\t1.5\t3.0\t1\t1\tnull\t2\t3\t3\t3\t3\t2\t1
                    1970-01-01T00:00:00.000004Z\t1\t4\t4.0\t4.0\t4\t4\t4\t4\t1\t1\t1\t1\t4\t4
                    1970-01-01T00:00:00.000005Z\t1\t0\t2.0\t4.0\t4\t4\t0\t0\t2\t2\t2\t2\t4\t0
                    1970-01-01T00:00:00.000006Z\t1\tnull\t2.0\t4.0\t4\t4\tnull\t0\t3\t3\t3\t3\t4\t0
                    1970-01-01T00:00:00.000007Z\t1\t2\t2.0\t6.0\t4\t4\t2\t2\t4\t4\t4\t4\t4\t0
                    """);

            assertQueryNoLeakCheck(
                    rowsResult1,
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts rows unbounded preceding), " +
                            "sum(j) over (partition by i order by ts rows unbounded preceding), " +
                            "first_value(j) over (partition by i order by ts rows unbounded preceding), " +
                            "first_value(j) ignore nulls over (partition by i order by ts rows unbounded preceding), " +
                            "last_value(j) over (partition by i order by ts rows unbounded preceding), " +
                            "last_value(j) ignore nulls over (partition by i order by ts rows unbounded preceding), " +
                            "count(*) over (partition by i order by ts rows unbounded preceding), " +
                            "count(s) over (partition by i order by ts rows unbounded preceding), " +
                            "count(d) over (partition by i order by ts rows unbounded preceding), " +
                            "count(c) over (partition by i order by ts rows unbounded preceding), " +
                            "max(j) over (partition by i order by ts rows unbounded preceding), " +
                            "min(j) over (partition by i order by ts rows unbounded preceding) " +
                            "from tab",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    rowsResult1,
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts rows unbounded preceding), " +
                            "sum(j) over (partition by i order by ts rows unbounded preceding), " +
                            "first_value(j) over (partition by i order by ts rows unbounded preceding), " +
                            "first_value(j) ignore nulls over (partition by i order by ts rows unbounded preceding), " +
                            "last_value(j) over (partition by i order by ts rows unbounded preceding), " +
                            "last_value(j) ignore nulls over (partition by i order by ts rows unbounded preceding), " +
                            "count(*) over (partition by i order by ts rows unbounded preceding), " +
                            "count(s) over (partition by i order by ts rows unbounded preceding), " +
                            "count(d) over (partition by i order by ts rows unbounded preceding), " +
                            "count(c) over (partition by i order by ts rows unbounded preceding), " +
                            "max(j) over (partition by i order by ts rows unbounded preceding), " +
                            "min(j) over (partition by i order by ts rows unbounded preceding) " +
                            "from tab",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    rowsResult1,
                    "select ts, i, j, " +
                            "avg(j) over (partition by i rows unbounded preceding), " +
                            "sum(j) over (partition by i rows unbounded preceding), " +
                            "first_value(j) over (partition by i rows unbounded preceding), " +
                            "first_value(j) ignore nulls over (partition by i rows unbounded preceding), " +
                            "last_value(j) over (partition by i rows unbounded preceding), " +
                            "last_value(j) ignore nulls over (partition by i rows unbounded preceding), " +
                            "count(*) over (partition by i rows unbounded preceding), " +
                            "count(s) over (partition by i rows unbounded preceding), " +
                            "count(d) over (partition by i rows unbounded preceding), " +
                            "count(c) over (partition by i rows unbounded preceding), " +
                            "max(j) over (partition by i rows unbounded preceding), " +
                            "min(j) over (partition by i rows unbounded preceding) " +
                            "from tab",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    rowsResult1,
                    "select ts, i, j, " +
                            "avg(j) over (partition by i rows between unbounded preceding and current row), " +
                            "sum(j) over (partition by i rows between unbounded preceding and current row), " +
                            "first_value(j) over (partition by i rows between unbounded preceding and current row), " +
                            "first_value(j) ignore nulls over (partition by i rows between unbounded preceding and current row), " +
                            "last_value(j) over (partition by i rows between unbounded preceding and current row), " +
                            "last_value(j) ignore nulls over (partition by i rows between unbounded preceding and current row), " +
                            "count(*) over (partition by i rows between unbounded preceding and current row), " +
                            "count(s) over (partition by i rows between unbounded preceding and current row), " +
                            "count(d) over (partition by i rows between unbounded preceding and current row), " +
                            "count(c) over (partition by i rows between unbounded preceding and current row), " +
                            "max(j) over (partition by i rows between unbounded preceding and current row), " +
                            "min(j) over (partition by i rows between unbounded preceding and current row) " +
                            "from tab",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    rowsResult1,
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts rows between 10 preceding and current row), " +
                            "sum(j) over (partition by i order by ts rows between 10 preceding and current row), " +
                            "first_value(j) over (partition by i order by ts rows between 10 preceding and current row), " +
                            "first_value(j) ignore nulls over (partition by i order by ts rows between 10 preceding and current row), " +
                            "last_value(j) over (partition by i order by ts rows between 10 preceding and current row), " +
                            "last_value(j) ignore nulls over (partition by i order by ts rows between 10 preceding and current row), " +
                            "count(*) over (partition by i order by ts rows between 10 preceding and current row), " +
                            "count(s) over (partition by i order by ts rows between 10 preceding and current row), " +
                            "count(d) over (partition by i order by ts rows between 10 preceding and current row), " +
                            "count(c) over (partition by i order by ts rows between 10 preceding and current row), " +
                            "max(j) over (partition by i order by ts rows between 10 preceding and current row), " +
                            "min(j) over (partition by i order by ts rows between 10 preceding and current row) " +
                            "from tab",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    rowsResult1,
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts rows between 3 preceding and current row), " +
                            "sum(j) over (partition by i order by ts rows between 3 preceding and current row), " +
                            "first_value(j) over (partition by i order by ts rows between 3 preceding and current row), " +
                            "first_value(j) ignore nulls over (partition by i order by ts rows between 3 preceding and current row), " +
                            "last_value(j) over (partition by i order by ts rows between 3 preceding and current row), " +
                            "last_value(j) ignore nulls over (partition by i order by ts rows between 3 preceding and current row), " +
                            "count(*) over (partition by i order by ts rows between 3 preceding and current row), " +
                            "count(s) over (partition by i order by ts rows between 3 preceding and current row), " +
                            "count(d) over (partition by i order by ts rows between 3 preceding and current row), " +
                            "count(c) over (partition by i order by ts rows between 3 preceding and current row), " +
                            "max(j) over (partition by i order by ts rows between 3 preceding and current row), " +
                            "min(j) over (partition by i order by ts rows between 3 preceding and current row) " +
                            "from tab",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                            1970-01-01T00:00:00.000001Z\t0\t1\t1.0\t1.0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1
                            1970-01-01T00:00:00.000002Z\t0\t2\t1.5\t3.0\t1\t1\t2\t2\t2\t2\t2\t2\t2\t2\t1
                            1970-01-01T00:00:00.000003Z\t0\tnull\t2.0\t2.0\t2\t2\tnull\t2\t2\t1\t2\t2\t2\t2\t2
                            1970-01-01T00:00:00.000004Z\t1\t4\t4.0\t4.0\t4\t4\t4\t4\t1\t1\t1\t1\t1\t4\t4
                            1970-01-01T00:00:00.000005Z\t1\t0\t2.0\t4.0\t4\t4\t0\t0\t2\t2\t2\t2\t2\t4\t0
                            1970-01-01T00:00:00.000006Z\t1\tnull\t0.0\t0.0\t0\t0\tnull\t0\t2\t1\t2\t2\t2\t0\t0
                            1970-01-01T00:00:00.000007Z\t1\t2\t2.0\t2.0\tnull\t2\t2\t2\t2\t1\t2\t2\t2\t2\t2
                            """),
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts rows between 1 preceding and current row), " +
                            "sum(j) over (partition by i order by ts rows between 1 preceding and current row), " +
                            "first_value(j) over (partition by i order by ts rows between 1 preceding and current row), " +
                            "first_value(j) ignore nulls over (partition by i order by ts rows between 1 preceding and current row), " +
                            "last_value(j) over (partition by i order by ts rows between 1 preceding and current row), " +
                            "last_value(j) ignore nulls over (partition by i order by ts rows between 1 preceding and current row), " +
                            "count(*) over (partition by i order by ts rows between 1 preceding and current row), " +
                            "count(j) over (partition by i order by ts rows between 1 preceding and current row), " +
                            "count(s) over (partition by i order by ts rows between 1 preceding and current row), " +
                            "count(d) over (partition by i order by ts rows between 1 preceding and current row), " +
                            "count(c) over (partition by i order by ts rows between 1 preceding and current row), " +
                            "max(j) over (partition by i order by ts rows between 1 preceding and current row), " +
                            "min(j) over (partition by i order by ts rows between 1 preceding and current row) " +
                            "from tab",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                            1970-01-01T00:00:00.000001Z\t0\t1\t1.0\t1.0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1
                            1970-01-01T00:00:00.000002Z\t0\t2\t1.5\t3.0\t1\t1\t2\t2\t2\t2\t2\t2\t2\t2\t1
                            1970-01-01T00:00:00.000003Z\t0\tnull\t1.5\t3.0\t1\t1\tnull\t2\t3\t2\t3\t3\t3\t2\t1
                            1970-01-01T00:00:00.000004Z\t1\t4\t4.0\t4.0\t4\t4\t4\t4\t1\t1\t1\t1\t1\t4\t4
                            1970-01-01T00:00:00.000005Z\t1\t0\t2.0\t4.0\t4\t4\t0\t0\t2\t2\t2\t2\t2\t4\t0
                            1970-01-01T00:00:00.000006Z\t1\tnull\t2.0\t4.0\t4\t4\tnull\t0\t3\t2\t3\t3\t3\t4\t0
                            1970-01-01T00:00:00.000007Z\t1\t2\t1.0\t2.0\t0\t0\t2\t2\t3\t2\t3\t3\t3\t2\t0
                            """),
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts rows between 2 preceding and current row), " +
                            "sum(j) over (partition by i order by ts rows between 2 preceding and current row), " +
                            "first_value(j) over (partition by i order by ts rows between 2 preceding and current row), " +
                            "first_value(j) ignore nulls over (partition by i order by ts rows between 2 preceding and current row), " +
                            "last_value(j) over (partition by i order by ts rows between 2 preceding and current row), " +
                            "last_value(j) ignore nulls over (partition by i order by ts rows between 2 preceding and current row), " +
                            "count(*) over (partition by i order by ts rows between 2 preceding and current row), " +
                            "count(j) over (partition by i order by ts rows between 2 preceding and current row), " +
                            "count(s) over (partition by i order by ts rows between 2 preceding and current row), " +
                            "count(d) over (partition by i order by ts rows between 2 preceding and current row), " +
                            "count(c) over (partition by i order by ts rows between 2 preceding and current row), " +
                            "max(j) over (partition by i order by ts rows between 2 preceding and current row), " +
                            "min(j) over (partition by i order by ts rows between 2 preceding and current row) " +
                            "from tab",
                    "ts",
                    false,
                    true
            );

            String result2 = replaceTimestampSuffix("""
                    ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                    1970-01-01T00:00:00.000001Z\t0\t1\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                    1970-01-01T00:00:00.000002Z\t0\t2\t1.0\t1.0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1
                    1970-01-01T00:00:00.000003Z\t0\tnull\t1.5\t3.0\t1\t1\t2\t2\t2\t2\t2\t2\t2\t2\t1
                    1970-01-01T00:00:00.000004Z\t1\t4\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                    1970-01-01T00:00:00.000005Z\t1\t0\t4.0\t4.0\t4\t4\t4\t4\t1\t1\t1\t1\t1\t4\t4
                    1970-01-01T00:00:00.000006Z\t1\tnull\t2.0\t4.0\t4\t4\t0\t0\t2\t2\t2\t2\t2\t4\t0
                    1970-01-01T00:00:00.000007Z\t1\t2\t0.0\t0.0\t0\t0\tnull\t0\t2\t1\t2\t2\t2\t0\t0
                    """);

            assertQueryNoLeakCheck(
                    result2,
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts rows between 2 preceding and 1 preceding), " +
                            "sum(j) over (partition by i order by ts rows between 2 preceding and 1 preceding), " +
                            "first_value(j) over (partition by i order by ts rows between 2 preceding and 1 preceding), " +
                            "first_value(j) ignore nulls over (partition by i order by ts rows between 2 preceding and 1 preceding), " +
                            "last_value(j) over (partition by i order by ts rows between 2 preceding and 1 preceding), " +
                            "last_value(j) ignore nulls over (partition by i order by ts rows between 2 preceding and 1 preceding), " +
                            "count(*) over (partition by i order by ts rows between 2 preceding and 1 preceding), " +
                            "count(j) over (partition by i order by ts rows between 2 preceding and 1 preceding), " +
                            "count(s) over (partition by i order by ts rows between 2 preceding and 1 preceding), " +
                            "count(d) over (partition by i order by ts rows between 2 preceding and 1 preceding), " +
                            "count(c) over (partition by i order by ts rows between 2 preceding and 1 preceding), " +
                            "max(j) over (partition by i order by ts rows between 2 preceding and 1 preceding), " +
                            "min(j) over (partition by i order by ts rows between 2 preceding and 1 preceding) " +
                            "from tab",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    result2,
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts rows between 2 preceding and 1 preceding exclude current row), " +
                            "sum(j) over (partition by i order by ts rows between 2 preceding and 1 preceding exclude current row), " +
                            "first_value(j) over (partition by i order by ts rows between 2 preceding and 1 preceding exclude current row), " +
                            "first_value(j) ignore nulls over (partition by i order by ts rows between 2 preceding and 1 preceding exclude current row), " +
                            "last_value(j) over (partition by i order by ts rows between 2 preceding and 1 preceding exclude current row), " +
                            "last_value(j) ignore nulls over (partition by i order by ts rows between 2 preceding and 1 preceding exclude current row), " +
                            "count(*) over (partition by i order by ts rows between 2 preceding and 1 preceding exclude current row), " +
                            "count(j) over (partition by i order by ts rows between 2 preceding and 1 preceding exclude current row), " +
                            "count(s) over (partition by i order by ts rows between 2 preceding and 1 preceding exclude current row), " +
                            "count(d) over (partition by i order by ts rows between 2 preceding and 1 preceding exclude current row), " +
                            "count(c) over (partition by i order by ts rows between 2 preceding and 1 preceding exclude current row), " +
                            "max(j) over (partition by i order by ts rows between 2 preceding and 1 preceding exclude current row), " +
                            "min(j) over (partition by i order by ts rows between 2 preceding and 1 preceding exclude current row) " +
                            "from tab",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    result2,
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts rows between 2 preceding and current row exclude current row), " +
                            "sum(j) over (partition by i order by ts rows between 2 preceding and current row exclude current row), " +
                            "first_value(j) over (partition by i order by ts rows between 2 preceding and current row exclude current row), " +
                            "first_value(j) ignore nulls over (partition by i order by ts rows between 2 preceding and current row exclude current row), " +
                            "last_value(j) over (partition by i order by ts rows between 2 preceding and current row exclude current row), " +
                            "last_value(j) ignore nulls over (partition by i order by ts rows between 2 preceding and current row exclude current row), " +
                            "count(*) over (partition by i order by ts rows between 2 preceding and current row exclude current row), " +
                            "count(j) over (partition by i order by ts rows between 2 preceding and current row exclude current row), " +
                            "count(s) over (partition by i order by ts rows between 2 preceding and current row exclude current row), " +
                            "count(d) over (partition by i order by ts rows between 2 preceding and current row exclude current row), " +
                            "count(c) over (partition by i order by ts rows between 2 preceding and current row exclude current row), " +
                            "max(j) over (partition by i order by ts rows between 2 preceding and current row exclude current row), " +
                            "min(j) over (partition by i order by ts rows between 2 preceding and current row exclude current row) " +
                            "from tab",
                    "ts",
                    false,
                    true
            );

            // partitions are smaller than 10 elements so avg is all nulls
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                            1970-01-01T00:00:00.000001Z\t0\t1\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000002Z\t0\t2\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000003Z\t0\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000004Z\t1\t4\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000005Z\t1\t0\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000006Z\t1\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            1970-01-01T00:00:00.000007Z\t1\t2\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                            """),
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts rows between 20 preceding and 10 preceding), " +
                            "sum(j) over (partition by i order by ts rows between 20 preceding and 10 preceding), " +
                            "first_value(j) over (partition by i order by ts rows between 20 preceding and 10 preceding), " +
                            "first_value(j) ignore nulls over (partition by i order by ts rows between 20 preceding and 10 preceding), " +
                            "last_value(j) over (partition by i order by ts rows between 20 preceding and 10 preceding), " +
                            "last_value(j) ignore nulls over (partition by i order by ts rows between 20 preceding and 10 preceding), " +
                            "count(*) over (partition by i order by ts rows between 20 preceding and 10 preceding), " +
                            "count(j) over (partition by i order by ts rows between 20 preceding and 10 preceding), " +
                            "count(s) over (partition by i order by ts rows between 20 preceding and 10 preceding), " +
                            "count(d) over (partition by i order by ts rows between 20 preceding and 10 preceding), " +
                            "count(c) over (partition by i order by ts rows between 20 preceding and 10 preceding), " +
                            "max(j) over (partition by i order by ts rows between 20 preceding and 10 preceding), " +
                            "min(j) over (partition by i order by ts rows between 20 preceding and 10 preceding) " +
                            "from tab",
                    "ts",
                    false,
                    true
            );

            String result3 = replaceTimestampSuffix("""
                    ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                    1970-01-01T00:00:00.000001Z\t0\t1\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                    1970-01-01T00:00:00.000002Z\t0\t2\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                    1970-01-01T00:00:00.000003Z\t0\tnull\t1.0\t1.0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1
                    1970-01-01T00:00:00.000004Z\t1\t4\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                    1970-01-01T00:00:00.000005Z\t1\t0\tnull\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull
                    1970-01-01T00:00:00.000006Z\t1\tnull\t4.0\t4.0\t4\t4\t4\t4\t1\t1\t1\t1\t1\t4\t4
                    1970-01-01T00:00:00.000007Z\t1\t2\t2.0\t4.0\t4\t4\t0\t0\t2\t2\t2\t2\t2\t4\t0
                    """);

            assertQueryNoLeakCheck(
                    result3,
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts rows between unbounded preceding and 2 preceding), " +
                            "sum(j) over (partition by i order by ts rows between unbounded preceding and 2 preceding), " +
                            "first_value(j) over (partition by i order by ts rows between unbounded preceding and 2 preceding), " +
                            "first_value(j) ignore nulls over (partition by i order by ts rows between unbounded preceding and 2 preceding), " +
                            "last_value(j) over (partition by i order by ts rows between unbounded preceding and 2 preceding), " +
                            "last_value(j) ignore nulls over (partition by i order by ts rows between unbounded preceding and 2 preceding), " +
                            "count(*) over (partition by i order by ts rows between unbounded preceding and 2 preceding), " +
                            "count(j) over (partition by i order by ts rows between unbounded preceding and 2 preceding), " +
                            "count(s) over (partition by i order by ts rows between unbounded preceding and 2 preceding), " +
                            "count(d) over (partition by i order by ts rows between unbounded preceding and 2 preceding), " +
                            "count(c) over (partition by i order by ts rows between unbounded preceding and 2 preceding), " +
                            "max(j) over (partition by i order by ts rows between unbounded preceding and 2 preceding), " +
                            "min(j) over (partition by i order by ts rows between unbounded preceding and 2 preceding) " +
                            "from tab",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    result3,
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts rows between 10000 preceding and 2 preceding), " +
                            "sum(j) over (partition by i order by ts rows between 10000 preceding and 2 preceding), " +
                            "first_value(j) over (partition by i order by ts rows between 10000 preceding and 2 preceding), " +
                            "first_value(j) ignore nulls over (partition by i order by ts rows between 10000 preceding and 2 preceding), " +
                            "last_value(j) over (partition by i order by ts rows between 10000 preceding and 2 preceding), " +
                            "last_value(j) ignore nulls over (partition by i order by ts rows between 10000 preceding and 2 preceding), " +
                            "count(*) over (partition by i order by ts rows between 10000 preceding and 2 preceding), " +
                            "count(j) over (partition by i order by ts rows between 10000 preceding and 2 preceding), " +
                            "count(s) over (partition by i order by ts rows between 10000 preceding and 2 preceding), " +
                            "count(d) over (partition by i order by ts rows between 10000 preceding and 2 preceding), " +
                            "count(c) over (partition by i order by ts rows between 10000 preceding and 2 preceding), " +
                            "max(j) over (partition by i order by ts rows between 10000 preceding and 2 preceding), " +
                            "min(j) over (partition by i order by ts rows between 10000 preceding and 2 preceding) " +
                            "from tab",
                    "ts",
                    false,
                    true
            );

            // here avg returns j as double because it processes current row only
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                            1970-01-01T00:00:00.000001Z\t0\t1\t1.0\t1.0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1
                            1970-01-01T00:00:00.000002Z\t0\t2\t2.0\t2.0\t2\t2\t2\t2\t1\t1\t1\t1\t1\t2\t2
                            1970-01-01T00:00:00.000003Z\t0\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t1\t0\t1\t1\t1\tnull\tnull
                            1970-01-01T00:00:00.000004Z\t1\t4\t4.0\t4.0\t4\t4\t4\t4\t1\t1\t1\t1\t1\t4\t4
                            1970-01-01T00:00:00.000005Z\t1\t0\t0.0\t0.0\t0\t0\t0\t0\t1\t1\t1\t1\t1\t0\t0
                            1970-01-01T00:00:00.000006Z\t1\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t1\t0\t1\t1\t1\tnull\tnull
                            1970-01-01T00:00:00.000007Z\t1\t2\t2.0\t2.0\t2\t2\t2\t2\t1\t1\t1\t1\t1\t2\t2
                            """),
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts rows current row), " +
                            "sum(j) over (partition by i order by ts rows current row), " +
                            "first_value(j) over (partition by i order by ts rows current row), " +
                            "first_value(j) ignore nulls over (partition by i order by ts rows current row), " +
                            "last_value(j) over (partition by i order by ts rows current row), " +
                            "last_value(j) ignore nulls over (partition by i order by ts rows current row), " +
                            "count(*) over (partition by i order by ts rows current row), " +
                            "count(j) over (partition by i order by ts rows current row), " +
                            "count(s) over (partition by i order by ts rows current row), " +
                            "count(d) over (partition by i order by ts rows current row), " +
                            "count(c) over (partition by i order by ts rows current row), " +
                            "max(j) over (partition by i order by ts rows current row), " +
                            "min(j) over (partition by i order by ts rows current row) " +
                            "from tab",
                    "ts",
                    false,
                    true
            );

            // test with dependencies not included on column list + column reorder + sort
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            avg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\tts\ti\tj
                            1.0\t1.0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1970-01-01T00:00:00.000001Z\t0\t1
                            1.5\t3.0\t1\t1\t2\t2\t2\t2\t2\t2\t2\t2\t1\t1970-01-01T00:00:00.000002Z\t0\t2
                            2.0\t2.0\t2\t2\tnull\t2\t2\t1\t2\t2\t2\t2\t2\t1970-01-01T00:00:00.000003Z\t0\tnull
                            4.0\t4.0\t4\t4\t4\t4\t1\t1\t1\t1\t1\t4\t4\t1970-01-01T00:00:00.000004Z\t1\t4
                            2.0\t4.0\t4\t4\t0\t0\t2\t2\t2\t2\t2\t4\t0\t1970-01-01T00:00:00.000005Z\t1\t0
                            0.0\t0.0\t0\t0\tnull\t0\t2\t1\t2\t2\t2\t0\t0\t1970-01-01T00:00:00.000006Z\t1\tnull
                            2.0\t2.0\tnull\t2\t2\t2\t2\t1\t2\t2\t2\t2\t2\t1970-01-01T00:00:00.000007Z\t1\t2
                            """),
                    "select avg(j) over (partition by i order by ts rows between 1 preceding and current row), " +
                            "sum(j) over (partition by i order by ts rows between 1 preceding and current row), " +
                            "first_value(j) over (partition by i order by ts rows between 1 preceding and current row), " +
                            "first_value(j) ignore nulls over (partition by i order by ts rows between 1 preceding and current row), " +
                            "last_value(j) over (partition by i order by ts rows between 1 preceding and current row), " +
                            "last_value(j) ignore nulls over (partition by i order by ts rows between 1 preceding and current row), " +
                            "count(*) over (partition by i order by ts rows between 1 preceding and current row), " +
                            "count(j) over (partition by i order by ts rows between 1 preceding and current row), " +
                            "count(s) over (partition by i order by ts rows between 1 preceding and current row), " +
                            "count(d) over (partition by i order by ts rows between 1 preceding and current row), " +
                            "count(c) over (partition by i order by ts rows between 1 preceding and current row), " +
                            "max(j) over (partition by i order by ts rows between 1 preceding and current row), " +
                            "min(j) over (partition by i order by ts rows between 1 preceding and current row), " +
                            "ts, i, j " +
                            "from tab",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    """
                            avg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\ti\tj
                            1.0\t1.0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t0\t1
                            1.5\t3.0\t1\t1\t2\t2\t2\t2\t2\t2\t2\t2\t1\t0\t2
                            2.0\t2.0\t2\t2\tnull\t2\t2\t1\t2\t2\t2\t2\t2\t0\tnull
                            4.0\t4.0\t4\t4\t4\t4\t1\t1\t1\t1\t1\t4\t4\t1\t4
                            2.0\t4.0\t4\t4\t0\t0\t2\t2\t2\t2\t2\t4\t0\t1\t0
                            0.0\t0.0\t0\t0\tnull\t0\t2\t1\t2\t2\t2\t0\t0\t1\tnull
                            2.0\t2.0\tnull\t2\t2\t2\t2\t1\t2\t2\t2\t2\t2\t1\t2
                            """,
                    "select avg(j) over (partition by i order by ts rows between 1 preceding and current row), " +
                            "sum(j) over (partition by i order by ts rows between 1 preceding and current row), " +
                            "first_value(j) over (partition by i order by ts rows between 1 preceding and current row), " +
                            "first_value(j) ignore nulls over (partition by i order by ts rows between 1 preceding and current row), " +
                            "last_value(j) over (partition by i order by ts rows between 1 preceding and current row), " +
                            "last_value(j) ignore nulls over (partition by i order by ts rows between 1 preceding and current row), " +
                            "count(*) over (partition by i order by ts rows between 1 preceding and current row), " +
                            "count(j) over (partition by i order by ts rows between 1 preceding and current row), " +
                            "count(s) over (partition by i order by ts rows between 1 preceding and current row), " +
                            "count(d) over (partition by i order by ts rows between 1 preceding and current row), " +
                            "count(c) over (partition by i order by ts rows between 1 preceding and current row), " +
                            "max(j) over (partition by i order by ts rows between 1 preceding and current row), " +
                            "min(j) over (partition by i order by ts rows between 1 preceding and current row), " +
                            "i, j " +
                            "from tab",
                    null,
                    false,
                    true
            );

            String result4 = """
                    avg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin
                    1.5\t3.0\t2\t2\t1\t1\t2\t2\t2\t2\t2\t2\t1
                    2.0\t2.0\tnull\t2\t2\t2\t2\t1\t2\t2\t2\t2\t2
                    null\tnull\tnull\tnull\tnull\tnull\t1\t0\t1\t1\t1\tnull\tnull
                    2.0\t4.0\t0\t0\t4\t4\t2\t2\t2\t2\t2\t4\t0
                    0.0\t0.0\tnull\t0\t0\t0\t2\t1\t2\t2\t2\t0\t0
                    2.0\t2.0\t2\t2\tnull\t2\t2\t1\t2\t2\t2\t2\t2
                    2.0\t2.0\t2\t2\t2\t2\t1\t1\t1\t1\t1\t2\t2
                    """;
            assertQueryNoLeakCheck(
                    result4,
                    "select avg(j) over (partition by i order by ts desc rows between 1 preceding and current row), " +
                            "sum(j) over (partition by i order by ts desc rows between 1 preceding and current row), " +
                            "first_value(j) over (partition by i order by ts desc rows between 1 preceding and current row), " +
                            "first_value(j) ignore nulls over (partition by i order by ts desc rows between 1 preceding and current row), " +
                            "last_value(j) over (partition by i order by ts desc rows between 1 preceding and current row), " +
                            "last_value(j) ignore nulls over (partition by i order by ts desc rows between 1 preceding and current row), " +
                            "count(*) over (partition by i order by ts desc rows between 1 preceding and current row), " +
                            "count(j) over (partition by i order by ts desc rows between 1 preceding and current row), " +
                            "count(s) over (partition by i order by ts desc rows between 1 preceding and current row), " +
                            "count(d) over (partition by i order by ts desc rows between 1 preceding and current row), " +
                            "count(c) over (partition by i order by ts desc rows between 1 preceding and current row), " +
                            "max(j) over (partition by i order by ts desc rows between 1 preceding and current row), " +
                            "min(j) over (partition by i order by ts desc rows between 1 preceding and current row) " +
                            "from tab",
                    null,
                    true, // cached window factory
                    true
            );

            assertQueryNoLeakCheck(
                    result4,
                    "select avg(j) over (partition by i order by ts desc rows between 1 preceding and current row), " +
                            "sum(j) over (partition by i order by ts desc rows between 1 preceding and current row), " +
                            "first_value(j) over (partition by i order by ts desc rows between 1 preceding and current row), " +
                            "first_value(j) ignore nulls over (partition by i order by ts desc rows between 1 preceding and current row), " +
                            "last_value(j) over (partition by i order by ts desc rows between 1 preceding and current row), " +
                            "last_value(j) ignore nulls over (partition by i order by ts desc rows between 1 preceding and current row), " +
                            "count(*) over (partition by i order by ts desc rows between 1 preceding and current row), " +
                            "count(j) over (partition by i order by ts desc rows between 1 preceding and current row), " +
                            "count(s) over (partition by i order by ts desc rows between 1 preceding and current row), " +
                            "count(d) over (partition by i order by ts desc rows between 1 preceding and current row), " +
                            "count(c) over (partition by i order by ts desc rows between 1 preceding and current row), " +
                            "max(j) over (partition by i order by ts desc rows between 1 preceding and current row), " +
                            "min(j) over (partition by i order by ts desc rows between 1 preceding and current row) " +
                            "from tab " +
                            "order by ts",
                    null,
                    true, // cached window factory
                    true
            );

            assertQueryNoLeakCheck(
                    """
                            avg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\ti\tj
                            null\tnull\tnull\tnull\tnull\tnull\t1\t0\t1\t1\t1\tnull\tnull\t0\tnull
                            1.0\t1.0\tnull\t1\t1\t1\t2\t1\t2\t2\t2\t1\t1\t0\t1
                            1.5\t3.0\t1\t1\t2\t2\t2\t2\t2\t2\t2\t2\t1\t0\t2
                            null\tnull\tnull\tnull\tnull\tnull\t1\t0\t1\t1\t1\tnull\tnull\t1\tnull
                            0.0\t0.0\tnull\t0\t0\t0\t2\t1\t2\t2\t2\t0\t0\t1\t0
                            1.0\t2.0\t0\t0\t2\t2\t2\t2\t2\t2\t2\t2\t0\t1\t2
                            3.0\t6.0\t2\t2\t4\t4\t2\t2\t2\t2\t2\t4\t2\t1\t4
                            """,
                    "select avg(j) over (partition by i order by j, i  desc rows between 1 preceding and current row), " +
                            "sum(j) over (partition by i order by j, i  desc rows between 1 preceding and current row), " +
                            "first_value(j) over (partition by i order by j, i  desc rows between 1 preceding and current row), " +
                            "first_value(j) ignore nulls over (partition by i order by j, i  desc rows between 1 preceding and current row), " +
                            "last_value(j) over (partition by i order by j, i  desc rows between 1 preceding and current row), " +
                            "last_value(j) ignore nulls over (partition by i order by j, i  desc rows between 1 preceding and current row), " +
                            "count(*) over (partition by i order by j, i  desc rows between 1 preceding and current row), " +
                            "count(j) over (partition by i order by j, i  desc rows between 1 preceding and current row), " +
                            "count(s) over (partition by i order by j, i  desc rows between 1 preceding and current row), " +
                            "count(d) over (partition by i order by j, i  desc rows between 1 preceding and current row), " +
                            "count(c) over (partition by i order by j, i  desc rows between 1 preceding and current row), " +
                            "max(j) over (partition by i order by j, i  desc rows between 1 preceding and current row), " +
                            "min(j) over (partition by i order by j, i  desc rows between 1 preceding and current row), " +
                            "i, j " +
                            "from tab " +
                            "order by i, j",
                    null,
                    true,
                    true
            );

            executeWithRewriteTimestamp("create table tab1 (ts #TIMESTAMP, i long, j long, d double, s symbol, c VARCHAR) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab1 select x::timestamp, x/13, case when x < 6 THEN NULL ELSE 1.0 END, x%5, 'k' || (x%5) ::symbol, 'k' || x from long_sequence(12)");

            assertQueryNoLeakCheck(
                    """
                            avg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\ti\tj
                            null\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\t0\t1
                            null\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\t0\t1
                            null\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\t0\t1
                            1.0\t1.0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t0\t1
                            1.0\t2.0\t1\t1\t1\t1\t2\t2\t2\t2\t2\t1\t1\t0\t1
                            1.0\t3.0\t1\t1\t1\t1\t3\t3\t3\t3\t3\t1\t1\t0\t1
                            1.0\t4.0\t1\t1\t1\t1\t4\t4\t4\t4\t4\t1\t1\t0\t1
                            1.0\t4.0\t1\t1\t1\t1\t4\t4\t4\t4\t4\t1\t1\t0\tnull
                            1.0\t4.0\t1\t1\t1\t1\t4\t4\t4\t4\t4\t1\t1\t0\tnull
                            1.0\t4.0\t1\t1\t1\t1\t4\t4\t4\t4\t4\t1\t1\t0\tnull
                            1.0\t3.0\t1\t1\tnull\t1\t4\t3\t4\t4\t4\t1\t1\t0\tnull
                            1.0\t2.0\t1\t1\tnull\t1\t4\t2\t4\t4\t4\t1\t1\t0\tnull
                            """,
                    "select avg(j) over (partition by i order by ts desc rows between 6 preceding and 3 preceding), " +
                            "sum(j) over (partition by i order by ts  desc rows between 6 preceding and 3 preceding), " +
                            "first_value(j) over (partition by i order by ts  desc rows between 6 preceding and 3 preceding), " +
                            "first_value(j) ignore nulls over (partition by i order by ts  desc rows between 6 preceding and 3 preceding), " +
                            "last_value(j) over (partition by i order by ts  desc rows between 6 preceding and 3 preceding), " +
                            "last_value(j) ignore nulls over (partition by i order by ts  desc rows between 6 preceding and 3 preceding), " +
                            "count(*) over (partition by i order by ts  desc rows between 6 preceding and 3 preceding), " +
                            "count(j) over (partition by i order by ts  desc rows between 6 preceding and 3 preceding), " +
                            "count(s) over (partition by i order by ts  desc rows between 6 preceding and 3 preceding), " +
                            "count(d) over (partition by i order by ts  desc rows between 6 preceding and 3 preceding), " +
                            "count(c) over (partition by i order by ts  desc rows between 6 preceding and 3 preceding), " +
                            "max(j) over (partition by i order by ts  desc rows between 6 preceding and 3 preceding), " +
                            "min(j) over (partition by i order by ts  desc rows between 6 preceding and 3 preceding), " +
                            "i, j " +
                            "from tab1 " +
                            "order by ts desc",
                    null,
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    """
                            avg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\tj
                            null\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\t1
                            null\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\t1
                            null\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\t1
                            1.0\t1.0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1
                            1.0\t2.0\t1\t1\t1\t1\t2\t2\t2\t2\t2\t1\t1\t1
                            1.0\t3.0\t1\t1\t1\t1\t3\t3\t3\t3\t3\t1\t1\t1
                            1.0\t4.0\t1\t1\t1\t1\t4\t4\t4\t4\t4\t1\t1\t1
                            1.0\t4.0\t1\t1\t1\t1\t4\t4\t4\t4\t4\t1\t1\tnull
                            1.0\t4.0\t1\t1\t1\t1\t4\t4\t4\t4\t4\t1\t1\tnull
                            1.0\t4.0\t1\t1\t1\t1\t4\t4\t4\t4\t4\t1\t1\tnull
                            1.0\t3.0\t1\t1\tnull\t1\t4\t3\t4\t4\t4\t1\t1\tnull
                            1.0\t2.0\t1\t1\tnull\t1\t4\t2\t4\t4\t4\t1\t1\tnull
                            """,
                    "select avg(j) over (order by ts desc rows between 6 preceding and 3 preceding), " +
                            "sum(j) over (order by ts  desc rows between 6 preceding and 3 preceding), " +
                            "first_value(j) over (order by ts  desc rows between 6 preceding and 3 preceding), " +
                            "first_value(j) ignore nulls over (order by ts  desc rows between 6 preceding and 3 preceding), " +
                            "last_value(j) over (order by ts  desc rows between 6 preceding and 3 preceding), " +
                            "last_value(j) ignore nulls over (order by ts  desc rows between 6 preceding and 3 preceding), " +
                            "count(*) over (order by ts  desc rows between 6 preceding and 3 preceding), " +
                            "count(j) over (order by ts  desc rows between 6 preceding and 3 preceding), " +
                            "count(s) over (order by ts  desc rows between 6 preceding and 3 preceding), " +
                            "count(d) over (order by ts  desc rows between 6 preceding and 3 preceding), " +
                            "count(c) over (order by ts  desc rows between 6 preceding and 3 preceding), " +
                            "max(j) over (order by ts  desc rows between 6 preceding and 3 preceding), " +
                            "min(j) over (order by ts  desc rows between 6 preceding and 3 preceding), " +
                            "j " +
                            "from tab1 " +
                            "order by ts desc",
                    null,
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    """
                            avg\tsum\tfirst_value\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\tj
                            1.0\t1.0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1
                            1.0\t2.0\t1\t1\t1\t2\t2\t2\t2\t2\t1\t1\t1
                            1.0\t3.0\t1\t1\t1\t3\t3\t3\t3\t3\t1\t1\t1
                            1.0\t4.0\t1\t1\t1\t4\t4\t4\t4\t4\t1\t1\t1
                            1.0\t5.0\t1\t1\t1\t5\t5\t5\t5\t5\t1\t1\t1
                            1.0\t6.0\t1\t1\t1\t6\t6\t6\t6\t6\t1\t1\t1
                            1.0\t7.0\t1\t1\t1\t7\t7\t7\t7\t7\t1\t1\t1
                            1.0\t7.0\t1\tnull\t1\t8\t7\t8\t8\t8\t1\t1\tnull
                            1.0\t7.0\t1\tnull\t1\t9\t7\t9\t9\t9\t1\t1\tnull
                            1.0\t7.0\t1\tnull\t1\t10\t7\t10\t10\t10\t1\t1\tnull
                            1.0\t7.0\t1\tnull\t1\t11\t7\t11\t11\t11\t1\t1\tnull
                            1.0\t7.0\t1\tnull\t1\t12\t7\t12\t12\t12\t1\t1\tnull
                            """,
                    "select avg(j) over (order by ts desc rows between unbounded preceding and current row), " +
                            "sum(j) over (order by ts  desc rows between unbounded preceding and current row), " +
                            "first_value(j) over (order by ts  desc rows between unbounded preceding and current row), " +
                            "last_value(j) over (order by ts  desc rows between unbounded preceding and current row), " +
                            "last_value(j) ignore nulls over (order by ts  desc rows between unbounded preceding and current row), " +
                            "count(*) over (order by ts  desc rows between unbounded preceding and current row), " +
                            "count(j) over (order by ts  desc rows between unbounded preceding and current row), " +
                            "count(s) over (order by ts  desc rows between unbounded preceding and current row), " +
                            "count(d) over (order by ts  desc rows between unbounded preceding and current row), " +
                            "count(c) over (order by ts  desc rows between unbounded preceding and current row), " +
                            "max(j) over (order by ts  desc rows between unbounded preceding and current row), " +
                            "min(j) over (order by ts  desc rows between unbounded preceding and current row), " +
                            "j " +
                            "from tab1 " +
                            "order by ts desc",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testFrameStartUnfollowingUnsupported() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, j long, s symbol, d double, c VARCHAR) timestamp(ts)", timestampType.getTypeName());

            for (int i = 0, size = FRAME_FUNCTIONS.size(); i < size; i++) {
                String func = FRAME_FUNCTIONS.get(i);
                for (String column : FRAME_FUNCTIONS_PARAMETER_COLUMN_NAME[i]) {
                    assertExceptionNoLeakCheck(
                            "select ts, i, j, #FUNCT_NAME over (partition by i order by ts range between 22 following and 3 following) from tab".replace("#FUNCT_NAME", func).replace("#COLUMN", column),
                            96,
                            "frame start supports UNBOUNDED PRECEDING, _number_ PRECEDING and CURRENT ROW only"
                    );
                }
            }
        });
    }

    @Test
    public void testImplicitCastExceptionInLag() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "CREATE TABLE 'trades' ( " +
                            " symbol SYMBOL, " +
                            " side SYMBOL, " +
                            " price DOUBLE, " +
                            " amount DOUBLE, " +
                            " timestamp #TIMESTAMP " +
                            ") timestamp(timestamp) PARTITION BY DAY;", timestampType.getTypeName()
            );
            execute("INSERT INTO trades VALUES ('ETH-USD', 'sell', 2615.54, 0.00044, '2022-03-08T18:03:57.609765Z');");

            assertExceptionNoLeakCheck(
                    "SELECT " +
                            "    timestamp, " +
                            "    price, " +
                            "    lag('timestamp') OVER (ORDER BY timestamp) AS previous_price " +
                            "FROM trades " +
                            "LIMIT 10;",
                    0,
                    "inconvertible value: `timestamp` [STRING -> DOUBLE]"
            );
        });
    }

    @Test
    public void testKSumCurrentRow() throws Exception {
        // Test ksum() over (rows current row) - KSumOverCurrentRowFunction
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, val double) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab select x::timestamp, x%5 from long_sequence(5)");

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\tval\tksum
                            1970-01-01T00:00:00.000001Z\t1.0\t1.0
                            1970-01-01T00:00:00.000002Z\t2.0\t2.0
                            1970-01-01T00:00:00.000003Z\t3.0\t3.0
                            1970-01-01T00:00:00.000004Z\t4.0\t4.0
                            1970-01-01T00:00:00.000005Z\t0.0\t0.0
                            """),
                    "select ts, val, ksum(val) over (order by ts rows current row) from tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testKSumEmptyFrame() throws Exception {
        // Test ksum() with empty frame (rowsHi < rowsLo) - L99
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, val double) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab values (1000000::timestamp, 1.0)");
            execute("insert into tab values (2000000::timestamp, 2.0)");

            // Frame "between 1 preceding and 2 preceding" is empty (hi < lo)
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\tval\tksum
                            1970-01-01T00:00:01.000000Z\t1.0\tnull
                            1970-01-01T00:00:02.000000Z\t2.0\tnull
                            """),
                    "select ts, val, ksum(val) over (order by ts rows between 1 preceding and 2 preceding) from tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testKSumPartitionAllNulls() throws Exception {
        // Test ksum() with partition where all values are NULL - L402
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, val double) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab values (1000000::timestamp, 1, NULL)");
            execute("insert into tab values (2000000::timestamp, 1, NULL)");
            execute("insert into tab values (3000000::timestamp, 2, 10.0)");

            // Partition 1 has all NULLs -> ksum should be NaN
            // Partition 2 has a value -> ksum should be 10
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tval\tksum
                            1970-01-01T00:00:01.000000Z\t1\tnull\tnull
                            1970-01-01T00:00:02.000000Z\t1\tnull\tnull
                            1970-01-01T00:00:03.000000Z\t2\t10.0\t10.0
                            """),
                    "select ts, i, val, ksum(val) over (partition by i) from tab",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testKSumPartitionedRangeFirstRowNull() throws Exception {
        // Test ksum() with partition by range frame where first row in partition is NULL
        // This tests lines 503-507 in KSumOverPartitionRangeFrameFunction
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, val double) timestamp(ts)", timestampType.getTypeName());
            // Insert in timestamp order - partition 1 starts with NULL
            execute("insert into tab values (1000000::timestamp, 1, NULL)");  // t=1s, partition 1, NULL
            execute("insert into tab values (2000000::timestamp, 1, 2.0)");   // t=2s, partition 1
            execute("insert into tab values (3000000::timestamp, 2, 10.0)");  // t=3s, partition 2 (first row)
            execute("insert into tab values (4000000::timestamp, 1, 3.0)");   // t=4s, partition 1
            execute("insert into tab values (5000000::timestamp, 2, 20.0)");  // t=5s, partition 2

            // Range of 10 seconds includes all rows in each partition
            // Partition 1 at t=1s: NULL -> null
            // Partition 1 at t=2s: NULL + 2 = 2
            // Partition 2 at t=3s: 10 -> 10
            // Partition 1 at t=4s: NULL + 2 + 3 = 5
            // Partition 2 at t=5s: 10 + 20 = 30
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tval\tksum
                            1970-01-01T00:00:01.000000Z\t1\tnull\tnull
                            1970-01-01T00:00:02.000000Z\t1\t2.0\t2.0
                            1970-01-01T00:00:03.000000Z\t2\t10.0\t10.0
                            1970-01-01T00:00:04.000000Z\t1\t3.0\t5.0
                            1970-01-01T00:00:05.000000Z\t2\t20.0\t30.0
                            """),
                    "select ts, i, val, ksum(val) over (partition by i order by ts range between 10 second preceding and current row) from tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testKSumPartitionedRangeFrame() throws Exception {
        // Test ksum() over (partition by x order by ts range between N preceding and current row) - KSumOverPartitionRangeFrameFunction
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, val double) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab select x::timestamp, x%2, x from long_sequence(6)");

            // range between 2 microseconds preceding and current row
            // i=0: ts=2,4,6 values=2,4,6
            //   ts=2: range [0,2] -> [2] = 2
            //   ts=4: range [2,4] -> [2,4] = 6
            //   ts=6: range [4,6] -> [4,6] = 10
            // i=1: ts=1,3,5 values=1,3,5
            //   ts=1: range [-1,1] -> [1] = 1
            //   ts=3: range [1,3] -> [1,3] = 4
            //   ts=5: range [3,5] -> [3,5] = 8
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tval\tksum
                            1970-01-01T00:00:00.000001Z\t1\t1.0\t1.0
                            1970-01-01T00:00:00.000002Z\t0\t2.0\t2.0
                            1970-01-01T00:00:00.000003Z\t1\t3.0\t4.0
                            1970-01-01T00:00:00.000004Z\t0\t4.0\t6.0
                            1970-01-01T00:00:00.000005Z\t1\t5.0\t8.0
                            1970-01-01T00:00:00.000006Z\t0\t6.0\t10.0
                            """),
                    "select ts, i, val, ksum(val) over (partition by i order by ts range between 2 microseconds preceding and current row) from tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testKSumPartitionedRangeFrameBufferExpansion() throws Exception {
        // Test ksum() with partitioned range frame that triggers buffer expansion (>32 rows)
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, val double) timestamp(ts)", timestampType.getTypeName());
            // Create 40 rows all in the same partition to exceed initial buffer size of 32
            execute("insert into tab select (x * 1000000)::timestamp, 1, x from long_sequence(40)");

            // Range of 100 seconds should include all rows, triggering buffer expansion
            // Sum of 1..40 = 40*41/2 = 820
            // Last row timestamp: 40 * 1000000 microseconds = 40 seconds
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tval\tksum
                            1970-01-01T00:00:40.000000Z\t1\t40.0\t820.0
                            """),
                    "select ts, i, val, ksum(val) over (partition by i order by ts range between 100 second preceding and current row) from tab limit -1",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testKSumPartitionedRangeFramePlan() throws Exception {
        // Test toPlan() method of KSumOverPartitionRangeFrameFunction
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, val double) timestamp(ts)", timestampType.getTypeName());

            // Range values depend on timestamp type (micros vs nanos)
            String tenSeconds = timestampType == TestTimestampType.MICRO ? "10000000" : "10000000000";
            String twoSeconds = timestampType == TestTimestampType.MICRO ? "2000000" : "2000000000";

            // Test bounded range frame plan
            assertSql(
                    "QUERY PLAN\n" +
                            "Window\n" +
                            "  functions: [ksum(val) over (partition by [i] range between " + tenSeconds + " preceding and current row)]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: tab\n",
                    "explain select ts, i, val, ksum(val) over (partition by i order by ts range between 10 second preceding and current row) from tab"
            );

            // Test unbounded preceding plan
            assertSql(
                    "QUERY PLAN\n" +
                            "Window\n" +
                            "  functions: [ksum(val) over (partition by [i] range between unbounded preceding and " + twoSeconds + " preceding)]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: tab\n",
                    "explain select ts, i, val, ksum(val) over (partition by i order by ts range between unbounded preceding and 2 second preceding) from tab"
            );
        });
    }

    @Test
    public void testKSumPartitionedRangeUnboundedPreceding() throws Exception {
        // Test ksum() with partition by and range between unbounded preceding and N preceding
        // This tests the else branch (frameLoBounded=false) in KSumOverPartitionRangeFrameFunction
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, val double) timestamp(ts)", timestampType.getTypeName());
            // Rows 1 second apart in partition 1
            execute("insert into tab values (1000000::timestamp, 1, 1.0)");  // t=1s
            execute("insert into tab values (2000000::timestamp, 1, 2.0)");  // t=2s
            execute("insert into tab values (3000000::timestamp, 1, 3.0)");  // t=3s
            execute("insert into tab values (4000000::timestamp, 1, 4.0)");  // t=4s

            // Frame: unbounded preceding to 2 second preceding
            // At t=1s: no rows >= 2s before -> NaN
            // At t=2s: no rows >= 2s before -> NaN
            // At t=3s: t=1s is 2s before (included) -> 1.0
            // At t=4s: t=1s is 3s before, t=2s is 2s before -> 1.0 + 2.0 = 3.0
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tval\tksum
                            1970-01-01T00:00:01.000000Z\t1\t1.0\tnull
                            1970-01-01T00:00:02.000000Z\t1\t2.0\tnull
                            1970-01-01T00:00:03.000000Z\t1\t3.0\t1.0
                            1970-01-01T00:00:04.000000Z\t1\t4.0\t3.0
                            """),
                    "select ts, i, val, ksum(val) over (partition by i order by ts range between unbounded preceding and 2 second preceding) from tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testKSumPartitionedRowsFrameAllNulls() throws Exception {
        // Test ksum() with partitioned rows frame where frame has all NULLs - L779
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, val double) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab values (1000000::timestamp, 1, NULL)");
            execute("insert into tab values (2000000::timestamp, 1, NULL)");
            execute("insert into tab values (3000000::timestamp, 1, 3.0)");

            // Sliding window of 1 preceding + current
            // At t=1s: window=[NULL] -> NaN
            // At t=2s: window=[NULL, NULL] -> NaN
            // At t=3s: window=[NULL, 3.0] -> 3.0
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tval\tksum
                            1970-01-01T00:00:01.000000Z\t1\tnull\tnull
                            1970-01-01T00:00:02.000000Z\t1\tnull\tnull
                            1970-01-01T00:00:03.000000Z\t1\t3.0\t3.0
                            """),
                    "select ts, i, val, ksum(val) over (partition by i order by ts rows between 1 preceding and current row) from tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testKSumPartitionedRowsFramePlan() throws Exception {
        // Test toPlan() method of KSumOverPartitionRowsFrameFunction - L847, L853
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, val double) timestamp(ts)", timestampType.getTypeName());

            // Test unbounded preceding with N preceding end (L847) - uses KSumOverPartitionRowsFrameFunction
            // "rows between unbounded preceding and 1 preceding" triggers frameLoBounded=false
            assertSql(
                    """
                            QUERY PLAN
                            Window
                              functions: [ksum(val) over (partition by [i] rows between unbounded preceding and 0 preceding)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tab
                            """,
                    "explain select ts, i, val, ksum(val) over (partition by i order by ts rows between unbounded preceding and 1 preceding) from tab"
            );

            // Test bounded preceding with N preceding end (L853)
            assertSql(
                    """
                            QUERY PLAN
                            Window
                              functions: [ksum(val) over (partition by [i] rows between 3 preceding and 0 preceding)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tab
                            """,
                    "explain select ts, i, val, ksum(val) over (partition by i order by ts rows between 3 preceding and 1 preceding) from tab"
            );
        });
    }

    @Test
    public void testKSumPartitionedSlidingRows() throws Exception {
        // Test ksum() over (partition by x order by ts rows between N preceding and current row) - KSumOverPartitionRowsFrameFunction
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, val double) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab select x::timestamp, x%2, x from long_sequence(8)");

            // i=0: values 2,4,6,8 with window size 2 (1 preceding + current)
            //   row 2: [2] = 2
            //   row 4: [2,4] = 6
            //   row 6: [4,6] = 10
            //   row 8: [6,8] = 14
            // i=1: values 1,3,5,7 with window size 2
            //   row 1: [1] = 1
            //   row 3: [1,3] = 4
            //   row 5: [3,5] = 8
            //   row 7: [5,7] = 12
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tval\tksum
                            1970-01-01T00:00:00.000001Z\t1\t1.0\t1.0
                            1970-01-01T00:00:00.000002Z\t0\t2.0\t2.0
                            1970-01-01T00:00:00.000003Z\t1\t3.0\t4.0
                            1970-01-01T00:00:00.000004Z\t0\t4.0\t6.0
                            1970-01-01T00:00:00.000005Z\t1\t5.0\t8.0
                            1970-01-01T00:00:00.000006Z\t0\t6.0\t10.0
                            1970-01-01T00:00:00.000007Z\t1\t7.0\t12.0
                            1970-01-01T00:00:00.000008Z\t0\t8.0\t14.0
                            """),
                    "select ts, i, val, ksum(val) over (partition by i order by ts rows between 1 preceding and current row) from tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testKSumPartitionedUnboundedPreceding() throws Exception {
        // Test ksum() over (partition by x order by ts rows unbounded preceding) - KSumOverUnboundedPartitionRowsFrameFunction
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, val double) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab select x::timestamp, x%2, x from long_sequence(6)");

            // i=0: values 2,4,6 -> cumulative: 2, 6, 12
            // i=1: values 1,3,5 -> cumulative: 1, 4, 9
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tval\tksum
                            1970-01-01T00:00:00.000001Z\t1\t1.0\t1.0
                            1970-01-01T00:00:00.000002Z\t0\t2.0\t2.0
                            1970-01-01T00:00:00.000003Z\t1\t3.0\t4.0
                            1970-01-01T00:00:00.000004Z\t0\t4.0\t6.0
                            1970-01-01T00:00:00.000005Z\t1\t5.0\t9.0
                            1970-01-01T00:00:00.000006Z\t0\t6.0\t12.0
                            """),
                    "select ts, i, val, ksum(val) over (partition by i order by ts rows unbounded preceding) from tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testKSumPrecision() throws Exception {
        // Test that ksum handles NULL values and basic summation correctly
        // Kahan summation improves precision for accumulated floating point errors
        // but extreme cases like 1e16 + 1 - 1e16 are beyond what Kahan can fully compensate
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table precision_tab (ts #TIMESTAMP, val double) timestamp(ts)", timestampType.getTypeName());

            // Test with NULL handling and basic precision
            execute("insert into precision_tab values (1::timestamp, 1.1)");
            execute("insert into precision_tab values (2::timestamp, 2.2)");
            execute("insert into precision_tab values (3::timestamp, NULL)");
            execute("insert into precision_tab values (4::timestamp, 3.3)");

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\tval\tksum
                            1970-01-01T00:00:00.000001Z\t1.1\t1.1
                            1970-01-01T00:00:00.000002Z\t2.2\t3.3000000000000003
                            1970-01-01T00:00:00.000003Z\tnull\t3.3000000000000003
                            1970-01-01T00:00:00.000004Z\t3.3\t6.6
                            """),
                    "select ts, val, ksum(val) over (order by ts rows unbounded preceding) from precision_tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testKSumRangeFrame() throws Exception {
        // Test ksum() over (order by ts range between N preceding and current row) - KSumOverRangeFrameFunction
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, val double) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab select x::timestamp, x from long_sequence(5)");

            // range between 2 microseconds preceding and current row
            // ts=1: range [-1,1] -> [1] = 1
            // ts=2: range [0,2] -> [1,2] = 3
            // ts=3: range [1,3] -> [1,2,3] = 6
            // ts=4: range [2,4] -> [2,3,4] = 9
            // ts=5: range [3,5] -> [3,4,5] = 12
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\tval\tksum
                            1970-01-01T00:00:00.000001Z\t1.0\t1.0
                            1970-01-01T00:00:00.000002Z\t2.0\t3.0
                            1970-01-01T00:00:00.000003Z\t3.0\t6.0
                            1970-01-01T00:00:00.000004Z\t4.0\t9.0
                            1970-01-01T00:00:00.000005Z\t5.0\t12.0
                            """),
                    "select ts, val, ksum(val) over (order by ts range between 2 microseconds preceding and current row) from tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testKSumRangeFrameBufferExpansion() throws Exception {
        // Test ksum() with non-partitioned range frame that triggers buffer expansion (>32 rows)
        // This tests lines 952-966 in KSumOverRangeFrameFunction
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, val double) timestamp(ts)", timestampType.getTypeName());
            // Create 40 rows, 1 second apart
            execute("insert into tab select (x * 1000000)::timestamp, x from long_sequence(40)");

            // Range of 100 seconds should include all rows, triggering buffer expansion
            // Sum of 1..40 = 40*41/2 = 820
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\tval\tksum
                            1970-01-01T00:00:40.000000Z\t40.0\t820.0
                            """),
                    "select ts, val, ksum(val) over (order by ts range between 100 second preceding and current row) from tab limit -1",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testKSumRangeFramePlan() throws Exception {
        // Test toPlan() method of KSumOverRangeFrameFunction
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, val double) timestamp(ts)", timestampType.getTypeName());

            // Range values depend on timestamp type (micros vs nanos)
            String tenSeconds = timestampType == TestTimestampType.MICRO ? "10000000" : "10000000000";
            String twoSeconds = timestampType == TestTimestampType.MICRO ? "2000000" : "2000000000";

            // Test bounded range with current row
            assertSql(
                    "QUERY PLAN\n" +
                            "Window\n" +
                            "  functions: [ksum(val) over (range between " + tenSeconds + " preceding and current row)]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: tab\n",
                    "explain select ts, val, ksum(val) over (order by ts range between 10 second preceding and current row) from tab"
            );

            // Test unbounded preceding with N preceding end
            assertSql(
                    "QUERY PLAN\n" +
                            "Window\n" +
                            "  functions: [ksum(val) over (range between unbounded preceding and " + twoSeconds + " preceding)]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: tab\n",
                    "explain select ts, val, ksum(val) over (order by ts range between unbounded preceding and 2 second preceding) from tab"
            );
        });
    }

    @Test
    public void testKSumRangeFrameUnboundedPreceding() throws Exception {
        // Test ksum() with non-partitioned range between unbounded preceding and N preceding
        // This tests lines 980-998 in KSumOverRangeFrameFunction (frameLoBounded=false branch)
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, val double) timestamp(ts)", timestampType.getTypeName());
            // Rows 1 second apart
            execute("insert into tab values (1000000::timestamp, 1.0)");  // t=1s
            execute("insert into tab values (2000000::timestamp, 2.0)");  // t=2s
            execute("insert into tab values (3000000::timestamp, 3.0)");  // t=3s
            execute("insert into tab values (4000000::timestamp, 4.0)");  // t=4s

            // Frame: unbounded preceding to 2 second preceding
            // At t=1s: no rows >= 2s before -> NaN
            // At t=2s: no rows >= 2s before -> NaN
            // At t=3s: t=1s is 2s before (included) -> 1.0
            // At t=4s: t=1s is 3s before, t=2s is 2s before -> 1.0 + 2.0 = 3.0
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\tval\tksum
                            1970-01-01T00:00:01.000000Z\t1.0\tnull
                            1970-01-01T00:00:02.000000Z\t2.0\tnull
                            1970-01-01T00:00:03.000000Z\t3.0\t1.0
                            1970-01-01T00:00:04.000000Z\t4.0\t3.0
                            """),
                    "select ts, val, ksum(val) over (order by ts range between unbounded preceding and 2 second preceding) from tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testKSumRowsBetweenPrecedingAndPreceding() throws Exception {
        // Test ksum() over (rows between N preceding and M preceding) - frame excludes current row
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, val double) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab select x::timestamp, x from long_sequence(6)");

            // rows between 3 preceding and 1 preceding (excludes current row)
            // row 1: frame is empty -> NaN
            // row 2: [1] = 1
            // row 3: [1,2] = 3
            // row 4: [1,2,3] = 6
            // row 5: [2,3,4] = 9
            // row 6: [3,4,5] = 12
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\tval\tksum
                            1970-01-01T00:00:00.000001Z\t1.0\tnull
                            1970-01-01T00:00:00.000002Z\t2.0\t1.0
                            1970-01-01T00:00:00.000003Z\t3.0\t3.0
                            1970-01-01T00:00:00.000004Z\t4.0\t6.0
                            1970-01-01T00:00:00.000005Z\t5.0\t9.0
                            1970-01-01T00:00:00.000006Z\t6.0\t12.0
                            """),
                    "select ts, val, ksum(val) over (order by ts rows between 3 preceding and 1 preceding) from tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testKSumRowsFramePlan() throws Exception {
        // Test toPlan() method of KSumOverRowsFrameFunction (non-partitioned)
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, val double) timestamp(ts)", timestampType.getTypeName());

            // Test bounded rows with current row
            assertSql(
                    """
                            QUERY PLAN
                            Window
                              functions: [ksum(val) over ( rows between 3 preceding and current row)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tab
                            """,
                    "explain select ts, val, ksum(val) over (order by ts rows between 3 preceding and current row) from tab"
            );

            // Test unbounded rows with N preceding end
            assertSql(
                    """
                            QUERY PLAN
                            Window
                              functions: [ksum(val) over ( rows between unbounded preceding and 0 preceding)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tab
                            """,
                    "explain select ts, val, ksum(val) over (order by ts rows between unbounded preceding and 1 preceding) from tab"
            );
        });
    }

    @Test
    public void testKSumWholePartitionWithRowsFrame() throws Exception {
        // Test ksum() over (partition by x rows between unbounded preceding and unbounded following)
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, val double) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab select x::timestamp, x%2, x from long_sequence(6)");

            // i=0: values 2,4,6 -> sum=12
            // i=1: values 1,3,5 -> sum=9
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tval\tksum
                            1970-01-01T00:00:00.000001Z\t1\t1.0\t9.0
                            1970-01-01T00:00:00.000002Z\t0\t2.0\t12.0
                            1970-01-01T00:00:00.000003Z\t1\t3.0\t9.0
                            1970-01-01T00:00:00.000004Z\t0\t4.0\t12.0
                            1970-01-01T00:00:00.000005Z\t1\t5.0\t9.0
                            1970-01-01T00:00:00.000006Z\t0\t6.0\t12.0
                            """),
                    "select ts, i, val, ksum(val) over (partition by i rows between unbounded preceding and unbounded following) from tab",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testKSumWholeResultSet() throws Exception {
        // Test ksum() over () - whole result set without partition - L274
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, val double) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab values (1000000::timestamp, 1.0)");
            execute("insert into tab values (2000000::timestamp, 2.0)");
            execute("insert into tab values (3000000::timestamp, 3.0)");

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\tval\tksum
                            1970-01-01T00:00:01.000000Z\t1.0\t6.0
                            1970-01-01T00:00:02.000000Z\t2.0\t6.0
                            1970-01-01T00:00:03.000000Z\t3.0\t6.0
                            """),
                    "select ts, val, ksum(val) over () from tab",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testKSumWindowFunction() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, j double, s symbol) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab select x::timestamp, x/4, x%5, 'k' || (x%3) ::symbol from long_sequence(10)");

            // Test ksum() over partition
            // i=0: j values are 1,2,3 -> sum=6
            // i=1: j values are 4,0,1,2 -> sum=7
            // i=2: j values are 3,4,0 -> sum=7
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tksum
                            1970-01-01T00:00:00.000001Z\t0\t1.0\t6.0
                            1970-01-01T00:00:00.000002Z\t0\t2.0\t6.0
                            1970-01-01T00:00:00.000003Z\t0\t3.0\t6.0
                            1970-01-01T00:00:00.000004Z\t1\t4.0\t7.0
                            1970-01-01T00:00:00.000005Z\t1\t0.0\t7.0
                            1970-01-01T00:00:00.000006Z\t1\t1.0\t7.0
                            1970-01-01T00:00:00.000007Z\t1\t2.0\t7.0
                            1970-01-01T00:00:00.000008Z\t2\t3.0\t7.0
                            1970-01-01T00:00:00.000009Z\t2\t4.0\t7.0
                            1970-01-01T00:00:00.000010Z\t2\t0.0\t7.0
                            """),
                    "select ts, i, j, ksum(j) over (partition by i) from tab",
                    "ts",
                    true,
                    true
            );

            // Test ksum() over unbounded preceding
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tksum
                            1970-01-01T00:00:00.000001Z\t0\t1.0\t1.0
                            1970-01-01T00:00:00.000002Z\t0\t2.0\t3.0
                            1970-01-01T00:00:00.000003Z\t0\t3.0\t6.0
                            1970-01-01T00:00:00.000004Z\t1\t4.0\t10.0
                            1970-01-01T00:00:00.000005Z\t1\t0.0\t10.0
                            1970-01-01T00:00:00.000006Z\t1\t1.0\t11.0
                            1970-01-01T00:00:00.000007Z\t1\t2.0\t13.0
                            1970-01-01T00:00:00.000008Z\t2\t3.0\t16.0
                            1970-01-01T00:00:00.000009Z\t2\t4.0\t20.0
                            1970-01-01T00:00:00.000010Z\t2\t0.0\t20.0
                            """),
                    "select ts, i, j, ksum(j) over (order by ts rows unbounded preceding) from tab",
                    "ts",
                    false,
                    true
            );

            // Test ksum() over sliding window
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tksum
                            1970-01-01T00:00:00.000001Z\t0\t1.0\t1.0
                            1970-01-01T00:00:00.000002Z\t0\t2.0\t3.0
                            1970-01-01T00:00:00.000003Z\t0\t3.0\t6.0
                            1970-01-01T00:00:00.000004Z\t1\t4.0\t9.0
                            1970-01-01T00:00:00.000005Z\t1\t0.0\t7.0
                            1970-01-01T00:00:00.000006Z\t1\t1.0\t5.0
                            1970-01-01T00:00:00.000007Z\t1\t2.0\t3.0
                            1970-01-01T00:00:00.000008Z\t2\t3.0\t6.0
                            1970-01-01T00:00:00.000009Z\t2\t4.0\t9.0
                            1970-01-01T00:00:00.000010Z\t2\t0.0\t7.0
                            """),
                    "select ts, i, j, ksum(j) over (order by ts rows between 2 preceding and current row) from tab",
                    "ts",
                    false,
                    true
            );

            // Test ksum() over () - whole result set
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tj\tksum
                            1970-01-01T00:00:00.000001Z\t0\t1.0\t20.0
                            1970-01-01T00:00:00.000002Z\t0\t2.0\t20.0
                            1970-01-01T00:00:00.000003Z\t0\t3.0\t20.0
                            1970-01-01T00:00:00.000004Z\t1\t4.0\t20.0
                            1970-01-01T00:00:00.000005Z\t1\t0.0\t20.0
                            1970-01-01T00:00:00.000006Z\t1\t1.0\t20.0
                            1970-01-01T00:00:00.000007Z\t1\t2.0\t20.0
                            1970-01-01T00:00:00.000008Z\t2\t3.0\t20.0
                            1970-01-01T00:00:00.000009Z\t2\t4.0\t20.0
                            1970-01-01T00:00:00.000010Z\t2\t0.0\t20.0
                            """),
                    "select ts, i, j, ksum(j) over () from tab",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testKSumWithNulls() throws Exception {
        // Test ksum with NULL values in various positions
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, val double) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab values (1::timestamp, 1, 1.0)");
            execute("insert into tab values (2::timestamp, 1, NULL)");
            execute("insert into tab values (3::timestamp, 1, 3.0)");
            execute("insert into tab values (4::timestamp, 2, NULL)");
            execute("insert into tab values (5::timestamp, 2, 5.0)");
            execute("insert into tab values (6::timestamp, 2, NULL)");

            // Test partitioned with NULLs
            // i=1: values 1, NULL, 3 -> sum=4
            // i=2: values NULL, 5, NULL -> sum=5
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tval\tksum
                            1970-01-01T00:00:00.000001Z\t1\t1.0\t4.0
                            1970-01-01T00:00:00.000002Z\t1\tnull\t4.0
                            1970-01-01T00:00:00.000003Z\t1\t3.0\t4.0
                            1970-01-01T00:00:00.000004Z\t2\tnull\t5.0
                            1970-01-01T00:00:00.000005Z\t2\t5.0\t5.0
                            1970-01-01T00:00:00.000006Z\t2\tnull\t5.0
                            """),
                    "select ts, i, val, ksum(val) over (partition by i) from tab",
                    "ts",
                    true,
                    true
            );

            // Test sliding window with NULLs
            // Window is "1 preceding and current row":
            // Row 1: val=1.0, window=[1.0] -> 1.0
            // Row 2: val=null, window=[1.0, null] -> 1.0
            // Row 3: val=3.0, window=[null, 3.0] -> 3.0
            // Row 4: val=null, window=[3.0, null] -> 3.0
            // Row 5: val=5.0, window=[null, 5.0] -> 5.0
            // Row 6: val=null, window=[5.0, null] -> 5.0
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ti\tval\tksum
                            1970-01-01T00:00:00.000001Z\t1\t1.0\t1.0
                            1970-01-01T00:00:00.000002Z\t1\tnull\t1.0
                            1970-01-01T00:00:00.000003Z\t1\t3.0\t3.0
                            1970-01-01T00:00:00.000004Z\t2\tnull\t3.0
                            1970-01-01T00:00:00.000005Z\t2\t5.0\t5.0
                            1970-01-01T00:00:00.000006Z\t2\tnull\t5.0
                            """),
                    "select ts, i, val, ksum(val) over (order by ts rows between 1 preceding and current row) from tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testLagException() throws Exception {
        executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, j long, d double, s symbol, c VARCHAR) timestamp(ts)", timestampType.getTypeName());
        assertExceptionNoLeakCheck(
                "select lag() over () from tab",
                7,
                "function `lag` requires arguments"
        );

        assertExceptionNoLeakCheck(
                "select lag(d, -1) over () from tab",
                14,
                "offset must be a positive integer"
        );

        assertExceptionNoLeakCheck(
                "select lag(d, 1, s) over () from tab",
                17,
                "default value must be can cast to double"
        );

        assertExceptionNoLeakCheck(
                "select lag(d, 1, d + 1, d) over () from tab",
                24,
                "too many arguments"
        );

        assertExceptionNoLeakCheck(
                "select lag(d, i, d + 1) over () from tab",
                14,
                "offset must be a constant"
        );

        assertExceptionNoLeakCheck(
                "select lag(d, 1, sum(d)) over () from tab",
                17,
                "default value can not be a window function"
        );

        assertExceptionNoLeakCheck(
                "select lag(d, 1) ignore over () from tab",
                24,
                "'nulls' expected after 'ignore'"
        );

        assertExceptionNoLeakCheck(
                "select lag(d, 1) respect over () from tab",
                25,
                "'nulls' expected after 'respect'"
        );

        assertExceptionNoLeakCheck(
                "select lag(d, 1) ignore null over () from tab",
                24,
                "'nulls' expected, not 'null'"
        );
    }

    @Test
    public void testLagLeadOver() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, j long, d double, s symbol, c VARCHAR, m date) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab select x::" + timestampType.getTypeName() + ", x/4, case when x % 3 = 0 THEN NULL ELSE x%5 END, " +
                    "case when x::double % 3 = 0 THEN NULL ELSE x::double%5 END, 'k' || (x%5) ::symbol, 'k' || x, x::date from long_sequence(7)");

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix1("""
                            ts\ti\tj\td\tlead\tlag\tlead_ignore_nulls\tlag_ignore_nulls\tlead1\tlag1\tlead2\tlag2
                            1970-01-01T00:00:00.000001Z\t0\t1\t1.0\t2\tnull\t2\tnull\t2\tnull\t1970-01-01T00:00:00.002Z\t
                            1970-01-01T00:00:00.000002Z\t0\t2\t2.0\tnull\t1\t4\t1\tnull\t1\t1970-01-01T00:00:00.003Z\t1970-01-01T00:00:00.001Z
                            1970-01-01T00:00:00.000003Z\t0\tnull\tnull\t4\t2\t4\t2\t4\t2\t1970-01-01T00:00:00.004Z\t1970-01-01T00:00:00.002Z
                            1970-01-01T00:00:00.000004Z\t1\t4\t4.0\t0\tnull\t0\t2\t0\tnull\t1970-01-01T00:00:00.005Z\t1970-01-01T00:00:00.003Z
                            1970-01-01T00:00:00.000005Z\t1\t0\t0.0\tnull\t4\t2\t4\tnull\t4\t1970-01-01T00:00:00.006Z\t1970-01-01T00:00:00.004Z
                            1970-01-01T00:00:00.000006Z\t1\tnull\tnull\t2\t0\t2\t0\t2\t0\t1970-01-01T00:00:00.007Z\t1970-01-01T00:00:00.005Z
                            1970-01-01T00:00:00.000007Z\t1\t2\t2.0\tnull\tnull\tnull\t0\tnull\tnull\t\t1970-01-01T00:00:00.006Z
                            """),
                    "select ts, i, j, d, " +
                            "lead(j) over (), " +
                            "lag(j) over (), " +
                            "lead(j) ignore nulls over (), " +
                            "lag(j) ignore nulls over (), " +
                            "lead(j) respect nulls over (), " +
                            "lag(j) respect nulls over (), " +
                            "lead(m) respect nulls over (), " +
                            "lag(m) respect nulls over (), " +
                            "from tab ",
                    "ts",
                    true,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix1("""
                            ts\ti\tj\td\tlead\tlag\tlead_ignore_nulls\tlag_ignore_nulls\tlead1\tlag1\tlead2\tlag2
                            1970-01-01T00:00:00.000001Z\t0\t1\t1.0\t2.0\tnull\t2.0\tnull\t2.0\tnull\t1970-01-01T00:00:00.002Z\t
                            1970-01-01T00:00:00.000002Z\t0\t2\t2.0\tnull\t1.0\t4.0\t1.0\tnull\t1.0\t1970-01-01T00:00:00.003Z\t1970-01-01T00:00:00.001Z
                            1970-01-01T00:00:00.000003Z\t0\tnull\tnull\t4.0\t2.0\t4.0\t2.0\t4.0\t2.0\t1970-01-01T00:00:00.004Z\t1970-01-01T00:00:00.002Z
                            1970-01-01T00:00:00.000004Z\t1\t4\t4.0\t0.0\tnull\t0.0\t2.0\t0.0\tnull\t1970-01-01T00:00:00.005Z\t1970-01-01T00:00:00.003Z
                            1970-01-01T00:00:00.000005Z\t1\t0\t0.0\tnull\t4.0\t2.0\t4.0\tnull\t4.0\t1970-01-01T00:00:00.006Z\t1970-01-01T00:00:00.004Z
                            1970-01-01T00:00:00.000006Z\t1\tnull\tnull\t2.0\t0.0\t2.0\t0.0\t2.0\t0.0\t1970-01-01T00:00:00.007Z\t1970-01-01T00:00:00.005Z
                            1970-01-01T00:00:00.000007Z\t1\t2\t2.0\tnull\tnull\tnull\t0.0\tnull\tnull\t\t1970-01-01T00:00:00.006Z
                            """),
                    "select ts, i, j, d, " +
                            "lead(d) over (), " +
                            "lag(d) over (), " +
                            "lead(d) ignore nulls over (), " +
                            "lag(d) ignore nulls over (), " +
                            "lead(d) respect nulls over (), " +
                            "lag(d) respect nulls over (), " +
                            "lead(m) respect nulls over (), " +
                            "lag(m) respect nulls over (), " +
                            "from tab ",
                    "ts",
                    true,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix1("""
                            ts\ti\tj\td\tlead\tlag\tlead_ignore_nulls\tlag_ignore_nulls\tlead1\tlag1\tlead2\tlag2
                            1970-01-01T00:00:00.000001Z\t0\t1\t1.0\t1970-01-01T00:00:00.000002Z\t\t1970-01-01T00:00:00.000002Z\t\t1970-01-01T00:00:00.000002Z\t\t1970-01-01T00:00:00.002Z\t
                            1970-01-01T00:00:00.000002Z\t0\t2\t2.0\t1970-01-01T00:00:00.000003Z\t1970-01-01T00:00:00.000001Z\t1970-01-01T00:00:00.000003Z\t1970-01-01T00:00:00.000001Z\t1970-01-01T00:00:00.000003Z\t1970-01-01T00:00:00.000001Z\t1970-01-01T00:00:00.003Z\t1970-01-01T00:00:00.001Z
                            1970-01-01T00:00:00.000003Z\t0\tnull\tnull\t1970-01-01T00:00:00.000004Z\t1970-01-01T00:00:00.000002Z\t1970-01-01T00:00:00.000004Z\t1970-01-01T00:00:00.000002Z\t1970-01-01T00:00:00.000004Z\t1970-01-01T00:00:00.000002Z\t1970-01-01T00:00:00.004Z\t1970-01-01T00:00:00.002Z
                            1970-01-01T00:00:00.000004Z\t1\t4\t4.0\t1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.000003Z\t1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.000003Z\t1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.000003Z\t1970-01-01T00:00:00.005Z\t1970-01-01T00:00:00.003Z
                            1970-01-01T00:00:00.000005Z\t1\t0\t0.0\t1970-01-01T00:00:00.000006Z\t1970-01-01T00:00:00.000004Z\t1970-01-01T00:00:00.000006Z\t1970-01-01T00:00:00.000004Z\t1970-01-01T00:00:00.000006Z\t1970-01-01T00:00:00.000004Z\t1970-01-01T00:00:00.006Z\t1970-01-01T00:00:00.004Z
                            1970-01-01T00:00:00.000006Z\t1\tnull\tnull\t1970-01-01T00:00:00.000007Z\t1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.000007Z\t1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.000007Z\t1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.007Z\t1970-01-01T00:00:00.005Z
                            1970-01-01T00:00:00.000007Z\t1\t2\t2.0\t\t1970-01-01T00:00:00.000006Z\t\t1970-01-01T00:00:00.000006Z\t\t1970-01-01T00:00:00.000006Z\t\t1970-01-01T00:00:00.006Z
                            """),
                    "select ts, i, j, d, " +
                            "lead(ts) over (), " +
                            "lag(ts) over (), " +
                            "lead(ts) ignore nulls over (), " +
                            "lag(ts) ignore nulls over (), " +
                            "lead(ts) respect nulls over (), " +
                            "lag(ts) respect nulls over (), " +
                            "lead(m) respect nulls over (), " +
                            "lag(m) respect nulls over (), " +
                            "from tab ",
                    "ts",
                    true,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix1("""
                            ts\ti\tj\td\tlead\tlag\tlead_ignore_nulls\tlag_ignore_nulls\tlead1\tlag1\tlead2\tlag2\tlead_ignore_nulls1\tlag_ignore_nulls1\tlead3\tlag3\tlead4\tlag4\tlead_ignore_nulls2\tlag_ignore_nulls2\tlead5\tlag5\tlead6\tlag6\tlead_ignore_nulls3\tlag_ignore_nulls3\tlead7\tlag7
                            1970-01-01T00:00:00.000001Z\t0\t1\t1.0\t1\t1\t1\t1\t1\t1\t1.0\t1.0\t1.0\t1.0\t1.0\t1.0\t1\t1970-01-01T00:00:00.000001Z\t1\t1970-01-01T00:00:00.000001Z\t1\t1970-01-01T00:00:00.000001Z\t1\t1970-01-01T00:00:00.001Z\t1\t1970-01-01T00:00:00.001Z\t1\t1970-01-01T00:00:00.001Z
                            1970-01-01T00:00:00.000002Z\t0\t2\t2.0\t2\t2\t2\t2\t2\t2\t2.0\t2.0\t2.0\t2.0\t2.0\t2.0\t2\t1970-01-01T00:00:00.000002Z\t2\t1970-01-01T00:00:00.000002Z\t2\t1970-01-01T00:00:00.000002Z\t2\t1970-01-01T00:00:00.002Z\t2\t1970-01-01T00:00:00.002Z\t2\t1970-01-01T00:00:00.002Z
                            1970-01-01T00:00:00.000003Z\t0\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t3\t1970-01-01T00:00:00.000003Z\t3\t1970-01-01T00:00:00.000003Z\t3\t1970-01-01T00:00:00.000003Z\t3\t1970-01-01T00:00:00.003Z\t3\t1970-01-01T00:00:00.003Z\t3\t1970-01-01T00:00:00.003Z
                            1970-01-01T00:00:00.000004Z\t1\t4\t4.0\t4\t4\t4\t4\t4\t4\t4.0\t4.0\t4.0\t4.0\t4.0\t4.0\t4\t1970-01-01T00:00:00.000004Z\t4\t1970-01-01T00:00:00.000004Z\t4\t1970-01-01T00:00:00.000004Z\t4\t1970-01-01T00:00:00.004Z\t4\t1970-01-01T00:00:00.004Z\t4\t1970-01-01T00:00:00.004Z
                            1970-01-01T00:00:00.000005Z\t1\t0\t0.0\t0\t0\t0\t0\t0\t0\t0.0\t0.0\t0.0\t0.0\t0.0\t0.0\t5\t1970-01-01T00:00:00.000005Z\t5\t1970-01-01T00:00:00.000005Z\t5\t1970-01-01T00:00:00.000005Z\t5\t1970-01-01T00:00:00.005Z\t5\t1970-01-01T00:00:00.005Z\t5\t1970-01-01T00:00:00.005Z
                            1970-01-01T00:00:00.000006Z\t1\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t6\t1970-01-01T00:00:00.000006Z\t6\t1970-01-01T00:00:00.000006Z\t6\t1970-01-01T00:00:00.000006Z\t6\t1970-01-01T00:00:00.006Z\t6\t1970-01-01T00:00:00.006Z\t6\t1970-01-01T00:00:00.006Z
                            1970-01-01T00:00:00.000007Z\t1\t2\t2.0\t2\t2\t2\t2\t2\t2\t2.0\t2.0\t2.0\t2.0\t2.0\t2.0\t7\t1970-01-01T00:00:00.000007Z\t7\t1970-01-01T00:00:00.000007Z\t7\t1970-01-01T00:00:00.000007Z\t7\t1970-01-01T00:00:00.007Z\t7\t1970-01-01T00:00:00.007Z\t7\t1970-01-01T00:00:00.007Z
                            """),
                    "select ts, i, j, d, " +
                            "lead(j, 0) over (), " +
                            "lag(j, 0) over (), " +
                            "lead(j, 0) ignore nulls over (), " +
                            "lag(j, 0) ignore nulls over (), " +
                            "lead(j, 0) respect nulls over (), " +
                            "lag(j, 0) respect nulls over (), " +
                            "lead(d, 0) over (), " +
                            "lag(d, 0) over (), " +
                            "lead(d, 0) ignore nulls over (), " +
                            "lag(d, 0) ignore nulls over (), " +
                            "lead(d, 0) respect nulls over (), " +
                            "lag(d, 0) respect nulls over (), " +
                            "lead(ts, 0) over (), " +
                            "lag(ts, 0) over (), " +
                            "lead(ts, 0) ignore nulls over (), " +
                            "lag(ts, 0) ignore nulls over (), " +
                            "lead(ts, 0) respect nulls over (), " +
                            "lag(ts, 0) respect nulls over (), " +
                            "lead(m, 0) over (), " +
                            "lag(m, 0) over (), " +
                            "lead(m, 0) ignore nulls over (), " +
                            "lag(m, 0) ignore nulls over (), " +
                            "lead(m, 0) respect nulls over (), " +
                            "lag(m, 0) respect nulls over (), " +
                            "from tab ",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix1("""
                            ts\ti\tj\td\tlead\tlag\tlead_ignore_nulls\tlag_ignore_nulls\tlead1\tlag1\tlead2\tlag2\tlead_ignore_nulls1\tlag_ignore_nulls1\tlead3\tlag3\tlead4\tlag4\tlead_ignore_nulls2\tlag_ignore_nulls2\tlead5\tlag5\tlead6\tlag6\tlead_ignore_nulls3\tlag_ignore_nulls3\tlead7\tlag7
                            1970-01-01T00:00:00.000001Z\t0\t1\t1.0\t4\tnull\t0\tnull\t4\tnull\t4.0\tnull\t0.0\tnull\t4.0\tnull\t1970-01-01T00:00:00.000004Z\t\t1970-01-01T00:00:00.000004Z\t\t1970-01-01T00:00:00.000004Z\t\t1970-01-01T00:00:00.004Z\t\t1970-01-01T00:00:00.004Z\t\t1970-01-01T00:00:00.004Z\t
                            1970-01-01T00:00:00.000002Z\t0\t2\t2.0\t0\tnull\t2\tnull\t0\tnull\t0.0\tnull\t2.0\tnull\t0.0\tnull\t1970-01-01T00:00:00.000005Z\t\t1970-01-01T00:00:00.000005Z\t\t1970-01-01T00:00:00.000005Z\t\t1970-01-01T00:00:00.005Z\t\t1970-01-01T00:00:00.005Z\t\t1970-01-01T00:00:00.005Z\t
                            1970-01-01T00:00:00.000003Z\t0\tnull\tnull\tnull\tnull\t2\tnull\tnull\tnull\tnull\tnull\t2.0\tnull\tnull\tnull\t1970-01-01T00:00:00.000006Z\t\t1970-01-01T00:00:00.000006Z\t\t1970-01-01T00:00:00.000006Z\t\t1970-01-01T00:00:00.006Z\t\t1970-01-01T00:00:00.006Z\t\t1970-01-01T00:00:00.006Z\t
                            1970-01-01T00:00:00.000004Z\t1\t4\t4.0\t2\t1\tnull\tnull\t2\t1\t2.0\t1.0\tnull\tnull\t2.0\t1.0\t1970-01-01T00:00:00.000007Z\t1970-01-01T00:00:00.000001Z\t1970-01-01T00:00:00.000007Z\t1970-01-01T00:00:00.000001Z\t1970-01-01T00:00:00.000007Z\t1970-01-01T00:00:00.000001Z\t1970-01-01T00:00:00.007Z\t1970-01-01T00:00:00.001Z\t1970-01-01T00:00:00.007Z\t1970-01-01T00:00:00.001Z\t1970-01-01T00:00:00.007Z\t1970-01-01T00:00:00.001Z
                            1970-01-01T00:00:00.000005Z\t1\t0\t0.0\tnull\t2\tnull\t1\tnull\t2\tnull\t2.0\tnull\t1.0\tnull\t2.0\t\t1970-01-01T00:00:00.000002Z\t\t1970-01-01T00:00:00.000002Z\t\t1970-01-01T00:00:00.000002Z\t\t1970-01-01T00:00:00.002Z\t\t1970-01-01T00:00:00.002Z\t\t1970-01-01T00:00:00.002Z
                            1970-01-01T00:00:00.000006Z\t1\tnull\tnull\tnull\tnull\tnull\t2\tnull\tnull\tnull\tnull\tnull\t2.0\tnull\tnull\t\t1970-01-01T00:00:00.000003Z\t\t1970-01-01T00:00:00.000003Z\t\t1970-01-01T00:00:00.000003Z\t\t1970-01-01T00:00:00.003Z\t\t1970-01-01T00:00:00.003Z\t\t1970-01-01T00:00:00.003Z
                            1970-01-01T00:00:00.000007Z\t1\t2\t2.0\tnull\t4\tnull\t2\tnull\t4\tnull\t4.0\tnull\t2.0\tnull\t4.0\t\t1970-01-01T00:00:00.000004Z\t\t1970-01-01T00:00:00.000004Z\t\t1970-01-01T00:00:00.000004Z\t\t1970-01-01T00:00:00.004Z\t\t1970-01-01T00:00:00.004Z\t\t1970-01-01T00:00:00.004Z
                            """),
                    "select ts, i, j, d, " +
                            "lead(j, 3) over (), " +
                            "lag(j, 3) over (), " +
                            "lead(j, 3) ignore nulls over (), " +
                            "lag(j, 3) ignore nulls over (), " +
                            "lead(j, 3) respect nulls over (), " +
                            "lag(j, 3) respect nulls over (), " +
                            "lead(d, 3) over (), " +
                            "lag(d, 3) over (), " +
                            "lead(d, 3) ignore nulls over (), " +
                            "lag(d, 3) ignore nulls over (), " +
                            "lead(d, 3) respect nulls over (), " +
                            "lag(d, 3) respect nulls over (), " +
                            "lead(ts, 3) over (), " +
                            "lag(ts, 3) over (), " +
                            "lead(ts, 3) ignore nulls over (), " +
                            "lag(ts, 3) ignore nulls over (), " +
                            "lead(ts, 3) respect nulls over (), " +
                            "lag(ts, 3) respect nulls over (), " +
                            "lead(m, 3) over (), " +
                            "lag(m, 3) over (), " +
                            "lead(m, 3) ignore nulls over (), " +
                            "lag(m, 3) ignore nulls over (), " +
                            "lead(m, 3) respect nulls over (), " +
                            "lag(m, 3) respect nulls over (), " +
                            "from tab ",
                    "ts",
                    true,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix1("""
                            ts\ti\tj\td\tlead\tlag\tlead_ignore_nulls\tlag_ignore_nulls\tlead1\tlag1\tlead2\tlag2\tlead_ignore_nulls1\tlag_ignore_nulls1\tlead3\tlag3\tlead4\tlag4\tlead_ignore_nulls2\tlag_ignore_nulls2\tlead5\tlag5\tlead6\tlag6\tlead_ignore_nulls3\tlag_ignore_nulls3\tlead7\tlag7
                            1970-01-01T00:00:00.000001Z\t0\t1\t1.0\tnull\t2\t4\t2\tnull\t2\tnull\t2.0\t4.0\t2.0\tnull\t2.0\t1970-01-01T00:00:00.000003Z\t1970-01-01T00:00:00.000011Z\t1970-01-01T00:00:00.000003Z\t1970-01-01T00:00:00.000011Z\t1970-01-01T00:00:00.000003Z\t1970-01-01T00:00:00.000011Z\t1970-01-01T00:00:00.003Z\t1970-01-01T00:00:00.001Z\t1970-01-01T00:00:00.003Z\t1970-01-01T00:00:00.001Z\t1970-01-01T00:00:00.003Z\t1970-01-01T00:00:00.001Z
                            1970-01-01T00:00:00.000002Z\t0\t2\t2.0\t4\t3\t0\t3\t4\t3\t4.0\t3.0\t0.0\t3.0\t4.0\t3.0\t1970-01-01T00:00:00.000004Z\t1970-01-01T00:00:00.000012Z\t1970-01-01T00:00:00.000004Z\t1970-01-01T00:00:00.000012Z\t1970-01-01T00:00:00.000004Z\t1970-01-01T00:00:00.000012Z\t1970-01-01T00:00:00.004Z\t1970-01-01T00:00:00.002Z\t1970-01-01T00:00:00.004Z\t1970-01-01T00:00:00.002Z\t1970-01-01T00:00:00.004Z\t1970-01-01T00:00:00.002Z
                            1970-01-01T00:00:00.000003Z\t0\tnull\tnull\t0\t1\t0\t1\t0\t1\t0.0\t1.0\t0.0\t1.0\t0.0\t1.0\t1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.000001Z\t1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.000001Z\t1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.000001Z\t1970-01-01T00:00:00.005Z\t1970-01-01T00:00:00.001Z\t1970-01-01T00:00:00.005Z\t1970-01-01T00:00:00.001Z\t1970-01-01T00:00:00.005Z\t1970-01-01T00:00:00.001Z
                            1970-01-01T00:00:00.000004Z\t1\t4\t4.0\tnull\t2\t2\t1\tnull\t2\tnull\t2.0\t2.0\t1.0\tnull\t2.0\t1970-01-01T00:00:00.000006Z\t1970-01-01T00:00:00.000002Z\t1970-01-01T00:00:00.000006Z\t1970-01-01T00:00:00.000002Z\t1970-01-01T00:00:00.000006Z\t1970-01-01T00:00:00.000002Z\t1970-01-01T00:00:00.006Z\t1970-01-01T00:00:00.002Z\t1970-01-01T00:00:00.006Z\t1970-01-01T00:00:00.002Z\t1970-01-01T00:00:00.006Z\t1970-01-01T00:00:00.002Z
                            1970-01-01T00:00:00.000005Z\t1\t0\t0.0\t2\tnull\t1\t2\t2\tnull\t2.0\tnull\t1.0\t2.0\t2.0\tnull\t1970-01-01T00:00:00.000007Z\t1970-01-01T00:00:00.000003Z\t1970-01-01T00:00:00.000007Z\t1970-01-01T00:00:00.000003Z\t1970-01-01T00:00:00.000007Z\t1970-01-01T00:00:00.000003Z\t1970-01-01T00:00:00.007Z\t1970-01-01T00:00:00.003Z\t1970-01-01T00:00:00.007Z\t1970-01-01T00:00:00.003Z\t1970-01-01T00:00:00.007Z\t1970-01-01T00:00:00.003Z
                            1970-01-01T00:00:00.000006Z\t1\tnull\tnull\tnull\t4\tnull\t4\tnull\t4\tnull\t4.0\tnull\t4.0\tnull\t4.0\t1970-01-01T00:00:00.000016Z\t1970-01-01T00:00:00.000004Z\t1970-01-01T00:00:00.000016Z\t1970-01-01T00:00:00.000004Z\t1970-01-01T00:00:00.000016Z\t1970-01-01T00:00:00.000004Z\t1970-01-01T00:00:00.006Z\t1970-01-01T00:00:00.004Z\t1970-01-01T00:00:00.006Z\t1970-01-01T00:00:00.004Z\t1970-01-01T00:00:00.006Z\t1970-01-01T00:00:00.004Z
                            1970-01-01T00:00:00.000007Z\t1\t2\t2.0\t3\t0\t3\t4\t3\t0\t3.0\t0.0\t3.0\t4.0\t3.0\t0.0\t1970-01-01T00:00:00.000017Z\t1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.000017Z\t1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.000017Z\t1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.007Z\t1970-01-01T00:00:00.005Z\t1970-01-01T00:00:00.007Z\t1970-01-01T00:00:00.005Z\t1970-01-01T00:00:00.007Z\t1970-01-01T00:00:00.005Z
                            """),
                    "select ts, i, j, d, " +
                            "lead(j, 2, j + 1) over (), " +
                            "lag(j, 2, j + 1) over (), " +
                            "lead(j, 2, j + 1) ignore nulls over (), " +
                            "lag(j, 2, j + 1) ignore nulls over (), " +
                            "lead(j, 2, j + 1) respect nulls over (), " +
                            "lag(j, 2, j + 1) respect nulls over (), " +
                            "lead(d, 2, d + 1) over (), " +
                            "lag(d, 2, d + 1) over (), " +
                            "lead(d, 2, j + 1) ignore nulls over (), " +
                            "lag(d, 2, j + 1) ignore nulls over (), " +
                            "lead(d, 2, j + 1) respect nulls over (), " +
                            "lag(d, 2, j + 1) respect nulls over (), " +
                            "lead(ts, 2, ts + 10) over (), " +
                            "lag(ts, 2, ts + 10) over (), " +
                            "lead(ts, 2, ts + 10) ignore nulls over (), " +
                            "lag(ts, 2, ts + 10) ignore nulls over (), " +
                            "lead(ts, 2, ts + 10) respect nulls over (), " +
                            "lag(ts, 2, ts + 10) respect nulls over (), " +
                            "lead(m, 2, m) over (), " +
                            "lag(m, 2, m) over (), " +
                            "lead(m, 2, m) ignore nulls over (), " +
                            "lag(m, 2, m) ignore nulls over (), " +
                            "lead(m, 2, m) respect nulls over (), " +
                            "lag(m, 2, m) respect nulls over (), " +
                            "from tab ",
                    "ts",
                    true,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix1("""
                            ts\ti\tj\td\tlead\tlag\tlead_ignore_nulls\tlag_ignore_nulls\tlead1\tlag1\tlead2\tlag2\tlead_ignore_nulls1\tlag_ignore_nulls1\tlead3\tlag3\tlead4\tlag4\tlead_ignore_nulls2\tlag_ignore_nulls2\tlead5\tlag5\tlead6\tlag6\tlead_ignore_nulls3\tlag_ignore_nulls3\tlead7\tlag7
                            1970-01-01T00:00:00.000001Z\t0\t1\t1.0\t2\tnull\t2\t4\t2\tnull\tnull\t2.0\t2.0\t4.0\t2.0\tnull\t1970-01-01T00:00:00.000003Z\t1970-01-01T00:00:00.000011Z\t1970-01-01T00:00:00.000011Z\t1970-01-01T00:00:00.000003Z\t1970-01-01T00:00:00.000011Z\t1970-01-01T00:00:00.000003Z\t1970-01-01T00:00:00.003Z\t1970-01-01T00:00:00.001Z\t1970-01-01T00:00:00.001Z\t1970-01-01T00:00:00.003Z\t1970-01-01T00:00:00.001Z\t1970-01-01T00:00:00.003Z
                            1970-01-01T00:00:00.000002Z\t0\t2\t2.0\t3\t4\t3\t0\t3\t4\t4.0\t3.0\t3.0\t0.0\t3.0\t4.0\t1970-01-01T00:00:00.000004Z\t1970-01-01T00:00:00.000012Z\t1970-01-01T00:00:00.000012Z\t1970-01-01T00:00:00.000004Z\t1970-01-01T00:00:00.000012Z\t1970-01-01T00:00:00.000004Z\t1970-01-01T00:00:00.004Z\t1970-01-01T00:00:00.002Z\t1970-01-01T00:00:00.002Z\t1970-01-01T00:00:00.004Z\t1970-01-01T00:00:00.002Z\t1970-01-01T00:00:00.004Z
                            1970-01-01T00:00:00.000003Z\t0\tnull\tnull\t1\t0\t1\t0\t1\t0\t0.0\t1.0\t1.0\t0.0\t1.0\t0.0\t1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.000001Z\t1970-01-01T00:00:00.000001Z\t1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.000001Z\t1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.005Z\t1970-01-01T00:00:00.001Z\t1970-01-01T00:00:00.001Z\t1970-01-01T00:00:00.005Z\t1970-01-01T00:00:00.001Z\t1970-01-01T00:00:00.005Z
                            1970-01-01T00:00:00.000004Z\t1\t4\t4.0\t2\tnull\t1\t2\t2\tnull\tnull\t2.0\t1.0\t2.0\t2.0\tnull\t1970-01-01T00:00:00.000006Z\t1970-01-01T00:00:00.000002Z\t1970-01-01T00:00:00.000002Z\t1970-01-01T00:00:00.000006Z\t1970-01-01T00:00:00.000002Z\t1970-01-01T00:00:00.000006Z\t1970-01-01T00:00:00.006Z\t1970-01-01T00:00:00.002Z\t1970-01-01T00:00:00.002Z\t1970-01-01T00:00:00.006Z\t1970-01-01T00:00:00.002Z\t1970-01-01T00:00:00.006Z
                            1970-01-01T00:00:00.000005Z\t1\t0\t0.0\tnull\t2\t2\t1\tnull\t2\t2.0\tnull\t2.0\t1.0\tnull\t2.0\t1970-01-01T00:00:00.000007Z\t1970-01-01T00:00:00.000003Z\t1970-01-01T00:00:00.000003Z\t1970-01-01T00:00:00.000007Z\t1970-01-01T00:00:00.000003Z\t1970-01-01T00:00:00.000007Z\t1970-01-01T00:00:00.007Z\t1970-01-01T00:00:00.003Z\t1970-01-01T00:00:00.003Z\t1970-01-01T00:00:00.007Z\t1970-01-01T00:00:00.003Z\t1970-01-01T00:00:00.007Z
                            1970-01-01T00:00:00.000006Z\t1\tnull\tnull\t4\tnull\t4\tnull\t4\tnull\tnull\t4.0\t4.0\tnull\t4.0\tnull\t1970-01-01T00:00:00.000016Z\t1970-01-01T00:00:00.000004Z\t1970-01-01T00:00:00.000004Z\t1970-01-01T00:00:00.000016Z\t1970-01-01T00:00:00.000004Z\t1970-01-01T00:00:00.000016Z\t1970-01-01T00:00:00.006Z\t1970-01-01T00:00:00.004Z\t1970-01-01T00:00:00.004Z\t1970-01-01T00:00:00.006Z\t1970-01-01T00:00:00.004Z\t1970-01-01T00:00:00.006Z
                            1970-01-01T00:00:00.000007Z\t1\t2\t2.0\t0\t3\t4\t3\t0\t3\t3.0\t0.0\t4.0\t3.0\t0.0\t3.0\t1970-01-01T00:00:00.000017Z\t1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.000017Z\t1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.000017Z\t1970-01-01T00:00:00.007Z\t1970-01-01T00:00:00.005Z\t1970-01-01T00:00:00.005Z\t1970-01-01T00:00:00.007Z\t1970-01-01T00:00:00.005Z\t1970-01-01T00:00:00.007Z
                            """),
                    "select ts, i, j, d, " +
                            "lead(j, 2, j + 1) over (order by ts desc), " +
                            "lag(j, 2, j + 1) over (order by ts desc), " +
                            "lead(j, 2, j + 1) ignore nulls over (order by ts desc), " +
                            "lag(j, 2, j + 1) ignore nulls over (order by ts desc), " +
                            "lead(j, 2, j + 1) respect nulls over (order by ts desc), " +
                            "lag(j, 2, j + 1) respect nulls over (order by ts desc), " +
                            "lead(d, 2, d + 1) over (), " +
                            "lag(d, 2, d + 1) over (), " +
                            "lead(d, 2, d + 1) ignore nulls over (order by ts desc), " +
                            "lag(d, 2, d + 1) ignore nulls over (order by ts desc), " +
                            "lead(d, 2, d + 1) respect nulls over (order by ts desc), " +
                            "lag(d, 2, d + 1) respect nulls over (order by ts desc), " +
                            "lead(ts, 2, ts + 10) over (), " +
                            "lag(ts, 2, ts + 10) over (), " +
                            "lead(ts, 2, ts + 10) ignore nulls over (order by ts desc), " +
                            "lag(ts, 2, ts + 10) ignore nulls over (order by ts desc), " +
                            "lead(ts, 2, ts + 10) respect nulls over (order by ts desc), " +
                            "lag(ts, 2, ts + 10) respect nulls over (order by ts desc), " +
                            "lead(m, 2, m) over (), " +
                            "lag(m, 2, m) over (), " +
                            "lead(m, 2, m) ignore nulls over (order by ts desc), " +
                            "lag(m, 2, m) ignore nulls over (order by ts desc), " +
                            "lead(m, 2, m) respect nulls over (order by ts desc), " +
                            "lag(m, 2, m) respect nulls over (order by ts desc), " +
                            "from tab order by ts asc",
                    "ts",
                    true,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix1("""
                            ts\ti\tj\td\tlead\tlag\tlead_ignore_nulls\tlag_ignore_nulls\tlead1\tlag1\tlead2\tlag2\tlead_ignore_nulls1\tlag_ignore_nulls1\tlead3\tlag3\tlead4\tlag4\tlead_ignore_nulls2\tlag_ignore_nulls2\tlead5\tlag5\tlead6\tlag6\tlead_ignore_nulls3\tlag_ignore_nulls3\tlead7\tlag7
                            1970-01-01T00:00:00.000001Z\t0\t1\t1.0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1970-01-01T00:00:00.000001Z\t1\t1970-01-01T00:00:00.000001Z\t1\t1970-01-01T00:00:00.000001Z\t1\t1970-01-01T00:00:00.001Z\t1\t1970-01-01T00:00:00.001Z\t1\t1970-01-01T00:00:00.001Z
                            1970-01-01T00:00:00.000002Z\t0\t2\t2.0\t2\t2\t2\t2\t2\t2\t2\t2\t2\t2\t2\t2\t2\t1970-01-01T00:00:00.000002Z\t2\t1970-01-01T00:00:00.000002Z\t2\t1970-01-01T00:00:00.000002Z\t2\t1970-01-01T00:00:00.002Z\t2\t1970-01-01T00:00:00.002Z\t2\t1970-01-01T00:00:00.002Z
                            1970-01-01T00:00:00.000003Z\t0\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t3\t1970-01-01T00:00:00.000003Z\t3\t1970-01-01T00:00:00.000003Z\t3\t1970-01-01T00:00:00.000003Z\t3\t1970-01-01T00:00:00.003Z\t3\t1970-01-01T00:00:00.003Z\t3\t1970-01-01T00:00:00.003Z
                            1970-01-01T00:00:00.000004Z\t1\t4\t4.0\t4\t4\t4\t4\t4\t4\t4\t4\t4\t4\t4\t4\t4\t1970-01-01T00:00:00.000004Z\t4\t1970-01-01T00:00:00.000004Z\t4\t1970-01-01T00:00:00.000004Z\t4\t1970-01-01T00:00:00.004Z\t4\t1970-01-01T00:00:00.004Z\t4\t1970-01-01T00:00:00.004Z
                            1970-01-01T00:00:00.000005Z\t1\t0\t0.0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t5\t1970-01-01T00:00:00.000005Z\t5\t1970-01-01T00:00:00.000005Z\t5\t1970-01-01T00:00:00.000005Z\t5\t1970-01-01T00:00:00.005Z\t5\t1970-01-01T00:00:00.005Z\t5\t1970-01-01T00:00:00.005Z
                            1970-01-01T00:00:00.000006Z\t1\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t6\t1970-01-01T00:00:00.000006Z\t6\t1970-01-01T00:00:00.000006Z\t6\t1970-01-01T00:00:00.000006Z\t6\t1970-01-01T00:00:00.006Z\t6\t1970-01-01T00:00:00.006Z\t6\t1970-01-01T00:00:00.006Z
                            1970-01-01T00:00:00.000007Z\t1\t2\t2.0\t2\t2\t2\t2\t2\t2\t2\t2\t2\t2\t2\t2\t7\t1970-01-01T00:00:00.000007Z\t7\t1970-01-01T00:00:00.000007Z\t7\t1970-01-01T00:00:00.000007Z\t7\t1970-01-01T00:00:00.007Z\t7\t1970-01-01T00:00:00.007Z\t7\t1970-01-01T00:00:00.007Z
                            """),
                    "select ts, i, j, d, " +
                            "lead(j, 0) over (order by ts desc), " +
                            "lag(j, 0) over (order by ts desc), " +
                            "lead(j, 0) ignore nulls over (order by ts desc), " +
                            "lag(j, 0) ignore nulls over (order by ts desc), " +
                            "lead(j, 0) respect nulls over (order by ts desc), " +
                            "lag(j, 0) respect nulls over (order by ts desc), " +
                            "lead(j, 0) over (), " +
                            "lag(j, 0) over (), " +
                            "lead(j, 0) ignore nulls over (order by ts desc), " +
                            "lag(j, 0) ignore nulls over (order by ts desc), " +
                            "lead(j, 0) respect nulls over (order by ts desc), " +
                            "lag(j, 0) respect nulls over (order by ts desc), " +
                            "lead(ts, 0) over (), " +
                            "lag(ts, 0) over (), " +
                            "lead(ts, 0) ignore nulls over (order by ts desc), " +
                            "lag(ts, 0) ignore nulls over (order by ts desc), " +
                            "lead(ts, 0) respect nulls over (order by ts desc), " +
                            "lag(ts, 0) respect nulls over (order by ts desc), " +
                            "lead(m, 0) over (), " +
                            "lag(m, 0) over (), " +
                            "lead(m, 0) ignore nulls over (order by ts desc), " +
                            "lag(m, 0) ignore nulls over (order by ts desc), " +
                            "lead(m, 0) respect nulls over (order by ts desc), " +
                            "lag(m, 0) respect nulls over (order by ts desc), " +
                            "from tab order by ts asc",
                    "ts",
                    true,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix1("""
                            ts\ti\tj\td\tlag\tlag_ignore_nulls\tlag1\tlag2\tlag3\tlag4
                            1970-01-01T00:00:00.000001Z\t0\t1\t1.0\tnull\tnull\tnull\tnull\t\t
                            1970-01-01T00:00:00.000002Z\t0\t2\t2.0\t1\t1\t1\t1.0\t1970-01-01T00:00:00.000001Z\t1970-01-01T00:00:00.001Z
                            1970-01-01T00:00:00.000003Z\t0\tnull\tnull\t2\t2\t2\t2.0\t1970-01-01T00:00:00.000002Z\t1970-01-01T00:00:00.002Z
                            1970-01-01T00:00:00.000004Z\t1\t4\t4.0\tnull\t2\tnull\tnull\t1970-01-01T00:00:00.000003Z\t1970-01-01T00:00:00.003Z
                            1970-01-01T00:00:00.000005Z\t1\t0\t0.0\t4\t4\t4\t4.0\t1970-01-01T00:00:00.000004Z\t1970-01-01T00:00:00.004Z
                            1970-01-01T00:00:00.000006Z\t1\tnull\tnull\t0\t0\t0\t0.0\t1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.005Z
                            1970-01-01T00:00:00.000007Z\t1\t2\t2.0\tnull\t0\tnull\tnull\t1970-01-01T00:00:00.000006Z\t1970-01-01T00:00:00.006Z
                            """),
                    "select ts, i, j, d, " +
                            "lag(j) over (), " +
                            "lag(j) ignore nulls over (), " +
                            "lag(j) respect nulls over (), " +
                            "lag(d) over (), " +
                            "lag(ts) over (), " +
                            "lag(m) over () " +
                            "from tab ",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix1("""
                            ts\ti\tj\td\tlag\tlag_ignore_nulls\tlag1\tlag2\tlag3\tlag4
                            1970-01-01T00:00:00.000001Z\t0\t1\t1.0\tnull\tnull\tnull\tnull\t\t
                            1970-01-01T00:00:00.000002Z\t0\t2\t2.0\tnull\tnull\tnull\tnull\t\t
                            1970-01-01T00:00:00.000003Z\t0\tnull\tnull\tnull\tnull\tnull\tnull\t\t
                            1970-01-01T00:00:00.000004Z\t1\t4\t4.0\t1\tnull\t1\t1.0\t1970-01-01T00:00:00.000001Z\t1970-01-01T00:00:00.001Z
                            1970-01-01T00:00:00.000005Z\t1\t0\t0.0\t2\t1\t2\t2.0\t1970-01-01T00:00:00.000002Z\t1970-01-01T00:00:00.002Z
                            1970-01-01T00:00:00.000006Z\t1\tnull\tnull\tnull\t2\tnull\tnull\t1970-01-01T00:00:00.000003Z\t1970-01-01T00:00:00.003Z
                            1970-01-01T00:00:00.000007Z\t1\t2\t2.0\t4\t2\t4\t4.0\t1970-01-01T00:00:00.000004Z\t1970-01-01T00:00:00.004Z
                            """),
                    "select ts, i, j, d, " +
                            "lag(j, 3) over (), " +
                            "lag(j, 3) ignore nulls over (), " +
                            "lag(j, 3) respect nulls over (), " +
                            "lag(d, 3) over (), " +
                            "lag(ts, 3) over (), " +
                            "lag(m, 3) over () " +
                            "from tab ",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix1("""
                            ts\ti\tj\td\tlag\tlag_ignore_nulls\tlag1\tlag2\tlag3\tlag4
                            1970-01-01T00:00:00.000001Z\t0\t1\t1.0\t2\t2\t2\t2.0\t1970-01-01T00:00:00.000011Z\t1970-01-01T00:00:00.001Z
                            1970-01-01T00:00:00.000002Z\t0\t2\t2.0\t3\t3\t3\t3.0\t1970-01-01T00:00:00.000012Z\t1970-01-01T00:00:00.002Z
                            1970-01-01T00:00:00.000003Z\t0\tnull\tnull\t1\t1\t1\t1.0\t1970-01-01T00:00:00.000001Z\t1970-01-01T00:00:00.001Z
                            1970-01-01T00:00:00.000004Z\t1\t4\t4.0\t2\t1\t2\t2.0\t1970-01-01T00:00:00.000002Z\t1970-01-01T00:00:00.002Z
                            1970-01-01T00:00:00.000005Z\t1\t0\t0.0\tnull\t2\tnull\tnull\t1970-01-01T00:00:00.000003Z\t1970-01-01T00:00:00.003Z
                            1970-01-01T00:00:00.000006Z\t1\tnull\tnull\t4\t4\t4\t4.0\t1970-01-01T00:00:00.000004Z\t1970-01-01T00:00:00.004Z
                            1970-01-01T00:00:00.000007Z\t1\t2\t2.0\t0\t4\t0\t0.0\t1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.005Z
                            """),
                    "select ts, i, j, d, " +
                            "lag(j, 2, j + 1) over (), " +
                            "lag(j, 2, j + 1) ignore nulls over (), " +
                            "lag(j, 2, j + 1) respect nulls over (), " +
                            "lag(d, 2, d + 1) over (), " +
                            "lag(ts, 2, ts + 10) over (), " +
                            "lag(m, 2, m) over () " +
                            "from tab ",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testLagLeadOverPartitionBy() throws Exception {

        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, j long, d double, s symbol, c VARCHAR, m date) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab select x::" + timestampType.getTypeName() + ", x/4, case when x % 3 = 0 THEN NULL ELSE x%5 END, " +
                    "case when x::double % 3 = 0 THEN NULL ELSE x::double%5 END, 'k' || (x%5) ::symbol, 'k' || x, x::date from long_sequence(7)");

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix1("""
                            ts\ti\tj\td\tlead\tlag\tlead_ignore_nulls\tlag_ignore_nulls\tlead1\tlag1\tlead2\tlag2\tlead_ignore_nulls1\tlag_ignore_nulls1\tlead3\tlag3\tlead4\tlag4\tlead_ignore_nulls2\tlag_ignore_nulls2\tlead5\tlag5\tlead6\tlag6\tlead_ignore_nulls3\tlag_ignore_nulls3\tlead7\tlag7
                            1970-01-01T00:00:00.000001Z\t0\t1\t1.0\t2\tnull\t2\tnull\t2\tnull\t2.0\tnull\t2.0\tnull\t2.0\tnull\t1970-01-01T00:00:00.000002Z\t\t1970-01-01T00:00:00.000002Z\t\t1970-01-01T00:00:00.000002Z\t\t1970-01-01T00:00:00.002Z\t\t1970-01-01T00:00:00.002Z\t\t1970-01-01T00:00:00.002Z\t
                            1970-01-01T00:00:00.000002Z\t0\t2\t2.0\tnull\t1\tnull\t1\tnull\t1\tnull\t1.0\tnull\t1.0\tnull\t1.0\t1970-01-01T00:00:00.000003Z\t1970-01-01T00:00:00.000001Z\t1970-01-01T00:00:00.000003Z\t1970-01-01T00:00:00.000001Z\t1970-01-01T00:00:00.000003Z\t1970-01-01T00:00:00.000001Z\t1970-01-01T00:00:00.003Z\t1970-01-01T00:00:00.001Z\t1970-01-01T00:00:00.003Z\t1970-01-01T00:00:00.001Z\t1970-01-01T00:00:00.003Z\t1970-01-01T00:00:00.001Z
                            1970-01-01T00:00:00.000003Z\t0\tnull\tnull\tnull\t2\tnull\t2\tnull\t2\tnull\t2.0\tnull\t2.0\tnull\t2.0\t\t1970-01-01T00:00:00.000002Z\t\t1970-01-01T00:00:00.000002Z\t\t1970-01-01T00:00:00.000002Z\t\t1970-01-01T00:00:00.002Z\t\t1970-01-01T00:00:00.002Z\t\t1970-01-01T00:00:00.002Z
                            1970-01-01T00:00:00.000004Z\t1\t4\t4.0\t0\tnull\t0\tnull\t0\tnull\t0.0\tnull\t0.0\tnull\t0.0\tnull\t1970-01-01T00:00:00.000005Z\t\t1970-01-01T00:00:00.000005Z\t\t1970-01-01T00:00:00.000005Z\t\t1970-01-01T00:00:00.005Z\t\t1970-01-01T00:00:00.005Z\t\t1970-01-01T00:00:00.005Z\t
                            1970-01-01T00:00:00.000005Z\t1\t0\t0.0\tnull\t4\t2\t4\tnull\t4\tnull\t4.0\t2.0\t4.0\tnull\t4.0\t1970-01-01T00:00:00.000006Z\t1970-01-01T00:00:00.000004Z\t1970-01-01T00:00:00.000006Z\t1970-01-01T00:00:00.000004Z\t1970-01-01T00:00:00.000006Z\t1970-01-01T00:00:00.000004Z\t1970-01-01T00:00:00.006Z\t1970-01-01T00:00:00.004Z\t1970-01-01T00:00:00.006Z\t1970-01-01T00:00:00.004Z\t1970-01-01T00:00:00.006Z\t1970-01-01T00:00:00.004Z
                            1970-01-01T00:00:00.000006Z\t1\tnull\tnull\t2\t0\t2\t0\t2\t0\t2.0\t0.0\t2.0\t0.0\t2.0\t0.0\t1970-01-01T00:00:00.000007Z\t1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.000007Z\t1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.000007Z\t1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.007Z\t1970-01-01T00:00:00.005Z\t1970-01-01T00:00:00.007Z\t1970-01-01T00:00:00.005Z\t1970-01-01T00:00:00.007Z\t1970-01-01T00:00:00.005Z
                            1970-01-01T00:00:00.000007Z\t1\t2\t2.0\tnull\tnull\tnull\t0\tnull\tnull\tnull\tnull\tnull\t0.0\tnull\tnull\t\t1970-01-01T00:00:00.000006Z\t\t1970-01-01T00:00:00.000006Z\t\t1970-01-01T00:00:00.000006Z\t\t1970-01-01T00:00:00.006Z\t\t1970-01-01T00:00:00.006Z\t\t1970-01-01T00:00:00.006Z
                            """),
                    "select ts, i, j, d, " +
                            "lead(j) over (partition by i), " +
                            "lag(j) over (partition by i), " +
                            "lead(j) ignore nulls over (partition by i), " +
                            "lag(j) ignore nulls over (partition by i), " +
                            "lead(j) respect nulls over (partition by i), " +
                            "lag(j) respect nulls over (partition by i), " +
                            "lead(d) over (partition by i), " +
                            "lag(d) over (partition by i), " +
                            "lead(d) ignore nulls over (partition by i), " +
                            "lag(d) ignore nulls over (partition by i), " +
                            "lead(d) respect nulls over (partition by i), " +
                            "lag(d) respect nulls over (partition by i), " +
                            "lead(ts) over (partition by i), " +
                            "lag(ts) over (partition by i), " +
                            "lead(ts) ignore nulls over (partition by i), " +
                            "lag(ts) ignore nulls over (partition by i), " +
                            "lead(ts) respect nulls over (partition by i), " +
                            "lag(ts) respect nulls over (partition by i), " +
                            "lead(m) over (partition by i), " +
                            "lag(m) over (partition by i), " +
                            "lead(m) ignore nulls over (partition by i), " +
                            "lag(m) ignore nulls over (partition by i), " +
                            "lead(m) respect nulls over (partition by i), " +
                            "lag(m) respect nulls over (partition by i), " +
                            "from tab ",
                    "ts",
                    true,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix1("""
                            ts\ti\tj\td\tlead\tlag\tlead_ignore_nulls\tlag_ignore_nulls\tlead_ignore_nulls1\tlag_ignore_nulls1\tlead1\tlag1\tlead_ignore_nulls2\tlag_ignore_nulls2\tlead_ignore_nulls3\tlag_ignore_nulls3\tlead2\tlag2\tlead_ignore_nulls4\tlag_ignore_nulls4\tlead_ignore_nulls5\tlag_ignore_nulls5\tlead3\tlag3\tlead_ignore_nulls6\tlag_ignore_nulls6\tlead_ignore_nulls7\tlag_ignore_nulls7
                            1970-01-01T00:00:00.000001Z\t0\t1\t1.0\t1\t1\t1\t1\t1\t1\t1.0\t1.0\t1.0\t1.0\t1.0\t1.0\t1\t1970-01-01T00:00:00.000001Z\t1\t1970-01-01T00:00:00.000001Z\t1\t1970-01-01T00:00:00.000001Z\t1\t1970-01-01T00:00:00.001Z\t1\t1970-01-01T00:00:00.001Z\t1\t1970-01-01T00:00:00.001Z
                            1970-01-01T00:00:00.000002Z\t0\t2\t2.0\t2\t2\t2\t2\t2\t2\t2.0\t2.0\t2.0\t2.0\t2.0\t2.0\t2\t1970-01-01T00:00:00.000002Z\t2\t1970-01-01T00:00:00.000002Z\t2\t1970-01-01T00:00:00.000002Z\t2\t1970-01-01T00:00:00.002Z\t2\t1970-01-01T00:00:00.002Z\t2\t1970-01-01T00:00:00.002Z
                            1970-01-01T00:00:00.000003Z\t0\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t3\t1970-01-01T00:00:00.000003Z\t3\t1970-01-01T00:00:00.000003Z\t3\t1970-01-01T00:00:00.000003Z\t3\t1970-01-01T00:00:00.003Z\t3\t1970-01-01T00:00:00.003Z\t3\t1970-01-01T00:00:00.003Z
                            1970-01-01T00:00:00.000004Z\t1\t4\t4.0\t4\t4\t4\t4\t4\t4\t4.0\t4.0\t4.0\t4.0\t4.0\t4.0\t4\t1970-01-01T00:00:00.000004Z\t4\t1970-01-01T00:00:00.000004Z\t4\t1970-01-01T00:00:00.000004Z\t4\t1970-01-01T00:00:00.004Z\t4\t1970-01-01T00:00:00.004Z\t4\t1970-01-01T00:00:00.004Z
                            1970-01-01T00:00:00.000005Z\t1\t0\t0.0\t0\t0\t0\t0\t0\t0\t0.0\t0.0\t0.0\t0.0\t0.0\t0.0\t5\t1970-01-01T00:00:00.000005Z\t5\t1970-01-01T00:00:00.000005Z\t5\t1970-01-01T00:00:00.000005Z\t5\t1970-01-01T00:00:00.005Z\t5\t1970-01-01T00:00:00.005Z\t5\t1970-01-01T00:00:00.005Z
                            1970-01-01T00:00:00.000006Z\t1\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t6\t1970-01-01T00:00:00.000006Z\t6\t1970-01-01T00:00:00.000006Z\t6\t1970-01-01T00:00:00.000006Z\t6\t1970-01-01T00:00:00.006Z\t6\t1970-01-01T00:00:00.006Z\t6\t1970-01-01T00:00:00.006Z
                            1970-01-01T00:00:00.000007Z\t1\t2\t2.0\t2\t2\t2\t2\t2\t2\t2.0\t2.0\t2.0\t2.0\t2.0\t2.0\t7\t1970-01-01T00:00:00.000007Z\t7\t1970-01-01T00:00:00.000007Z\t7\t1970-01-01T00:00:00.000007Z\t7\t1970-01-01T00:00:00.007Z\t7\t1970-01-01T00:00:00.007Z\t7\t1970-01-01T00:00:00.007Z
                            """),
                    "select ts, i, j, d, " +
                            "lead(j, 0) over (partition by i), " +
                            "lag(j, 0) over (partition by i), " +
                            "lead(j, 0) ignore nulls over (partition by i), " +
                            "lag(j, 0) ignore nulls over (partition by i), " +
                            "lead(j, 0) ignore nulls over (partition by i), " +
                            "lag(j, 0) ignore nulls over (partition by i), " +
                            "lead(d, 0) over (partition by i), " +
                            "lag(d, 0) over (partition by i), " +
                            "lead(d, 0) ignore nulls over (partition by i), " +
                            "lag(d, 0) ignore nulls over (partition by i), " +
                            "lead(d, 0) ignore nulls over (partition by i), " +
                            "lag(d, 0) ignore nulls over (partition by i), " +
                            "lead(ts, 0) over (partition by i), " +
                            "lag(ts, 0) over (partition by i), " +
                            "lead(ts, 0) ignore nulls over (partition by i), " +
                            "lag(ts, 0) ignore nulls over (partition by i), " +
                            "lead(ts, 0) ignore nulls over (partition by i), " +
                            "lag(ts, 0) ignore nulls over (partition by i), " +
                            "lead(m, 0) over (partition by i), " +
                            "lag(m, 0) over (partition by i), " +
                            "lead(m, 0) ignore nulls over (partition by i), " +
                            "lag(m, 0) ignore nulls over (partition by i), " +
                            "lead(m, 0) ignore nulls over (partition by i), " +
                            "lag(m, 0) ignore nulls over (partition by i), " +
                            "from tab ",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix1("""
                            ts\ti\tj\td\tlead\tlag\tlead_ignore_nulls\tlag_ignore_nulls\tlead1\tlag1\tlead2\tlag2\tlead_ignore_nulls1\tlag_ignore_nulls1\tlead3\tlag3\tlead4\tlag4\tlead_ignore_nulls2\tlag_ignore_nulls2\tlead5\tlag5\tlead6\tlag6\tlead_ignore_nulls3\tlag_ignore_nulls3\tlead7\tlag7
                            1970-01-01T00:00:00.000001Z\t0\t1\t1.0\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t\t\t\t\t\t\t\t\t\t\t\t
                            1970-01-01T00:00:00.000002Z\t0\t2\t2.0\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t\t\t\t\t\t\t\t\t\t\t\t
                            1970-01-01T00:00:00.000003Z\t0\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t\t\t\t\t\t\t\t\t\t\t\t
                            1970-01-01T00:00:00.000004Z\t1\t4\t4.0\t2\tnull\tnull\tnull\t2\tnull\t2.0\tnull\tnull\tnull\t2.0\tnull\t1970-01-01T00:00:00.000007Z\t\t1970-01-01T00:00:00.000007Z\t\t1970-01-01T00:00:00.000007Z\t\t1970-01-01T00:00:00.007Z\t\t1970-01-01T00:00:00.007Z\t\t1970-01-01T00:00:00.007Z\t
                            1970-01-01T00:00:00.000005Z\t1\t0\t0.0\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t\t\t\t\t\t\t\t\t\t\t\t
                            1970-01-01T00:00:00.000006Z\t1\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t\t\t\t\t\t\t\t\t\t\t\t
                            1970-01-01T00:00:00.000007Z\t1\t2\t2.0\tnull\t4\tnull\tnull\tnull\t4\tnull\t4.0\tnull\tnull\tnull\t4.0\t\t1970-01-01T00:00:00.000004Z\t\t1970-01-01T00:00:00.000004Z\t\t1970-01-01T00:00:00.000004Z\t\t1970-01-01T00:00:00.004Z\t\t1970-01-01T00:00:00.004Z\t\t1970-01-01T00:00:00.004Z
                            """),
                    "select ts, i, j, d, " +
                            "lead(j, 3) over (partition by i), " +
                            "lag(j, 3) over (partition by i), " +
                            "lead(j, 3) ignore nulls over (partition by i), " +
                            "lag(j, 3) ignore nulls over (partition by i), " +
                            "lead(j, 3) respect nulls over (partition by i), " +
                            "lag(j, 3) respect nulls over (partition by i), " +
                            "lead(d, 3) over (partition by i), " +
                            "lag(d, 3) over (partition by i), " +
                            "lead(d, 3) ignore nulls over (partition by i), " +
                            "lag(d, 3) ignore nulls over (partition by i), " +
                            "lead(d, 3) respect nulls over (partition by i), " +
                            "lag(d, 3) respect nulls over (partition by i), " +
                            "lead(ts, 3) over (partition by i), " +
                            "lag(ts, 3) over (partition by i), " +
                            "lead(ts, 3) ignore nulls over (partition by i), " +
                            "lag(ts, 3) ignore nulls over (partition by i), " +
                            "lead(ts, 3) respect nulls over (partition by i), " +
                            "lag(ts, 3) respect nulls over (partition by i), " +
                            "lead(m, 3) over (partition by i), " +
                            "lag(m, 3) over (partition by i), " +
                            "lead(m, 3) ignore nulls over (partition by i), " +
                            "lag(m, 3) ignore nulls over (partition by i), " +
                            "lead(m, 3) respect nulls over (partition by i), " +
                            "lag(m, 3) respect nulls over (partition by i), " +
                            "from tab ",
                    "ts",
                    true,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix1("""
                            ts\ti\tj\td\tlead\tlag\tlead_ignore_nulls\tlag_ignore_nulls\tlead1\tlag1\tlead2\tlag2\tlead_ignore_nulls1\tlag_ignore_nulls1\tlead3\tlag3\tlead4\tlag4\tlead_ignore_nulls2\tlag_ignore_nulls2\tlead5\tlag5\tlead6\tlag6\tlead_ignore_nulls3\tlag_ignore_nulls3\tlead7\tlag7
                            1970-01-01T00:00:00.000001Z\t0\t1\t1.0\tnull\t2\t2\t2\tnull\t2\tnull\t2.0\t2.0\t2.0\tnull\t2.0\t1970-01-01T00:00:00.000003Z\t1970-01-01T00:00:00.000011Z\t1970-01-01T00:00:00.000003Z\t1970-01-01T00:00:00.000011Z\t1970-01-01T00:00:00.000003Z\t1970-01-01T00:00:00.000011Z\t1970-01-01T00:00:00.003Z\t1970-01-01T00:00:00.001Z\t1970-01-01T00:00:00.003Z\t1970-01-01T00:00:00.001Z\t1970-01-01T00:00:00.003Z\t1970-01-01T00:00:00.001Z
                            1970-01-01T00:00:00.000002Z\t0\t2\t2.0\t3\t3\t3\t3\t3\t3\t3.0\t3.0\t3.0\t3.0\t3.0\t3.0\t1970-01-01T00:00:00.000012Z\t1970-01-01T00:00:00.000012Z\t1970-01-01T00:00:00.000012Z\t1970-01-01T00:00:00.000012Z\t1970-01-01T00:00:00.000012Z\t1970-01-01T00:00:00.000012Z\t1970-01-01T00:00:00.002Z\t1970-01-01T00:00:00.002Z\t1970-01-01T00:00:00.002Z\t1970-01-01T00:00:00.002Z\t1970-01-01T00:00:00.002Z\t1970-01-01T00:00:00.002Z
                            1970-01-01T00:00:00.000003Z\t0\tnull\tnull\tnull\t1\tnull\t1\tnull\t1\tnull\t1.0\tnull\t1.0\tnull\t1.0\t1970-01-01T00:00:00.000013Z\t1970-01-01T00:00:00.000001Z\t1970-01-01T00:00:00.000013Z\t1970-01-01T00:00:00.000001Z\t1970-01-01T00:00:00.000013Z\t1970-01-01T00:00:00.000001Z\t1970-01-01T00:00:00.003Z\t1970-01-01T00:00:00.001Z\t1970-01-01T00:00:00.003Z\t1970-01-01T00:00:00.001Z\t1970-01-01T00:00:00.003Z\t1970-01-01T00:00:00.001Z
                            1970-01-01T00:00:00.000004Z\t1\t4\t4.0\tnull\t5\t2\t5\tnull\t5\tnull\t5.0\t2.0\t5.0\tnull\t5.0\t1970-01-01T00:00:00.000006Z\t1970-01-01T00:00:00.000014Z\t1970-01-01T00:00:00.000006Z\t1970-01-01T00:00:00.000014Z\t1970-01-01T00:00:00.000006Z\t1970-01-01T00:00:00.000014Z\t1970-01-01T00:00:00.006Z\t1970-01-01T00:00:00.004Z\t1970-01-01T00:00:00.006Z\t1970-01-01T00:00:00.004Z\t1970-01-01T00:00:00.006Z\t1970-01-01T00:00:00.004Z
                            1970-01-01T00:00:00.000005Z\t1\t0\t0.0\t2\t1\t1\t1\t2\t1\t2.0\t1.0\t1.0\t1.0\t2.0\t1.0\t1970-01-01T00:00:00.000007Z\t1970-01-01T00:00:00.000015Z\t1970-01-01T00:00:00.000007Z\t1970-01-01T00:00:00.000015Z\t1970-01-01T00:00:00.000007Z\t1970-01-01T00:00:00.000015Z\t1970-01-01T00:00:00.007Z\t1970-01-01T00:00:00.005Z\t1970-01-01T00:00:00.007Z\t1970-01-01T00:00:00.005Z\t1970-01-01T00:00:00.007Z\t1970-01-01T00:00:00.005Z
                            1970-01-01T00:00:00.000006Z\t1\tnull\tnull\tnull\t4\tnull\t4\tnull\t4\tnull\t4.0\tnull\t4.0\tnull\t4.0\t1970-01-01T00:00:00.000016Z\t1970-01-01T00:00:00.000004Z\t1970-01-01T00:00:00.000016Z\t1970-01-01T00:00:00.000004Z\t1970-01-01T00:00:00.000016Z\t1970-01-01T00:00:00.000004Z\t1970-01-01T00:00:00.006Z\t1970-01-01T00:00:00.004Z\t1970-01-01T00:00:00.006Z\t1970-01-01T00:00:00.004Z\t1970-01-01T00:00:00.006Z\t1970-01-01T00:00:00.004Z
                            1970-01-01T00:00:00.000007Z\t1\t2\t2.0\t3\t0\t3\t4\t3\t0\t3.0\t0.0\t3.0\t4.0\t3.0\t0.0\t1970-01-01T00:00:00.000017Z\t1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.000017Z\t1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.000017Z\t1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.007Z\t1970-01-01T00:00:00.005Z\t1970-01-01T00:00:00.007Z\t1970-01-01T00:00:00.005Z\t1970-01-01T00:00:00.007Z\t1970-01-01T00:00:00.005Z
                            """),
                    "select ts, i, j, d, " +
                            "lead(j, 2, j + 1) over (partition by i), " +
                            "lag(j, 2, j + 1) over (partition by i), " +
                            "lead(j, 2, j + 1) ignore nulls over (partition by i), " +
                            "lag(j, 2, j + 1) ignore nulls over (partition by i), " +
                            "lead(j, 2, j + 1) respect nulls over (partition by i), " +
                            "lag(j, 2, j + 1) respect nulls over (partition by i), " +
                            "lead(d, 2, d + 1) over (partition by i), " +
                            "lag(d, 2, d + 1) over (partition by i), " +
                            "lead(d, 2, d + 1) ignore nulls over (partition by i), " +
                            "lag(d, 2, d + 1) ignore nulls over (partition by i), " +
                            "lead(d, 2, d + 1) respect nulls over (partition by i), " +
                            "lag(d, 2, d + 1) respect nulls over (partition by i), " +
                            "lead(ts, 2, ts + 10) over (partition by i), " +
                            "lag(ts, 2, ts + 10) over (partition by i), " +
                            "lead(ts, 2, ts + 10) ignore nulls over (partition by i), " +
                            "lag(ts, 2, ts + 10) ignore nulls over (partition by i), " +
                            "lead(ts, 2, ts + 10) respect nulls over (partition by i), " +
                            "lag(ts, 2, ts + 10) respect nulls over (partition by i), " +
                            "lead(m, 2, m) over (partition by i), " +
                            "lag(m, 2, m) over (partition by i), " +
                            "lead(m, 2, m) ignore nulls over (partition by i), " +
                            "lag(m, 2, m) ignore nulls over (partition by i), " +
                            "lead(m, 2, m) respect nulls over (partition by i), " +
                            "lag(m, 2, m) respect nulls over (partition by i), " +
                            "from tab ",
                    "ts",
                    true,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix1("""
                            ts\ti\tj\td\tlead\tlag\tlead_ignore_nulls\tlag_ignore_nulls\tlead1\tlag1\tlead2\tlag2\tlead_ignore_nulls1\tlag_ignore_nulls1\tlead3\tlag3\tlead4\tlag4\tlead_ignore_nulls2\tlag_ignore_nulls2\tlead5\tlag5\tlead6\tlag6\tlead_ignore_nulls3\tlag_ignore_nulls3\tlead7\tlag7
                            1970-01-01T00:00:00.000001Z\t0\t1\t1.0\t2\tnull\t2\t2\t2\tnull\tnull\t2.0\t2.0\t2.0\t2.0\tnull\t1970-01-01T00:00:00.000003Z\t1970-01-01T00:00:00.000011Z\t1970-01-01T00:00:00.000011Z\t1970-01-01T00:00:00.000003Z\t1970-01-01T00:00:00.000011Z\t1970-01-01T00:00:00.000003Z\t1970-01-01T00:00:00.003Z\t1970-01-01T00:00:00.001Z\t1970-01-01T00:00:00.001Z\t1970-01-01T00:00:00.003Z\t1970-01-01T00:00:00.001Z\t1970-01-01T00:00:00.003Z
                            1970-01-01T00:00:00.000002Z\t0\t2\t2.0\t3\t3\t3\t3\t3\t3\t3.0\t3.0\t3.0\t3.0\t3.0\t3.0\t1970-01-01T00:00:00.000012Z\t1970-01-01T00:00:00.000012Z\t1970-01-01T00:00:00.000012Z\t1970-01-01T00:00:00.000012Z\t1970-01-01T00:00:00.000012Z\t1970-01-01T00:00:00.000012Z\t1970-01-01T00:00:00.002Z\t1970-01-01T00:00:00.002Z\t1970-01-01T00:00:00.002Z\t1970-01-01T00:00:00.002Z\t1970-01-01T00:00:00.002Z\t1970-01-01T00:00:00.002Z
                            1970-01-01T00:00:00.000003Z\t0\tnull\tnull\t1\tnull\t1\tnull\t1\tnull\tnull\t1.0\t1.0\tnull\t1.0\tnull\t1970-01-01T00:00:00.000013Z\t1970-01-01T00:00:00.000001Z\t1970-01-01T00:00:00.000001Z\t1970-01-01T00:00:00.000013Z\t1970-01-01T00:00:00.000001Z\t1970-01-01T00:00:00.000013Z\t1970-01-01T00:00:00.003Z\t1970-01-01T00:00:00.001Z\t1970-01-01T00:00:00.001Z\t1970-01-01T00:00:00.003Z\t1970-01-01T00:00:00.001Z\t1970-01-01T00:00:00.003Z
                            1970-01-01T00:00:00.000004Z\t1\t4\t4.0\t5\tnull\t5\t2\t5\tnull\tnull\t5.0\t5.0\t2.0\t5.0\tnull\t1970-01-01T00:00:00.000006Z\t1970-01-01T00:00:00.000014Z\t1970-01-01T00:00:00.000014Z\t1970-01-01T00:00:00.000006Z\t1970-01-01T00:00:00.000014Z\t1970-01-01T00:00:00.000006Z\t1970-01-01T00:00:00.006Z\t1970-01-01T00:00:00.004Z\t1970-01-01T00:00:00.004Z\t1970-01-01T00:00:00.006Z\t1970-01-01T00:00:00.004Z\t1970-01-01T00:00:00.006Z
                            1970-01-01T00:00:00.000005Z\t1\t0\t0.0\t1\t2\t1\t1\t1\t2\t2.0\t1.0\t1.0\t1.0\t1.0\t2.0\t1970-01-01T00:00:00.000007Z\t1970-01-01T00:00:00.000015Z\t1970-01-01T00:00:00.000015Z\t1970-01-01T00:00:00.000007Z\t1970-01-01T00:00:00.000015Z\t1970-01-01T00:00:00.000007Z\t1970-01-01T00:00:00.007Z\t1970-01-01T00:00:00.005Z\t1970-01-01T00:00:00.005Z\t1970-01-01T00:00:00.007Z\t1970-01-01T00:00:00.005Z\t1970-01-01T00:00:00.007Z
                            1970-01-01T00:00:00.000006Z\t1\tnull\tnull\t4\tnull\t4\tnull\t4\tnull\tnull\t4.0\t4.0\tnull\t4.0\tnull\t1970-01-01T00:00:00.000016Z\t1970-01-01T00:00:00.000004Z\t1970-01-01T00:00:00.000004Z\t1970-01-01T00:00:00.000016Z\t1970-01-01T00:00:00.000004Z\t1970-01-01T00:00:00.000016Z\t1970-01-01T00:00:00.006Z\t1970-01-01T00:00:00.004Z\t1970-01-01T00:00:00.004Z\t1970-01-01T00:00:00.006Z\t1970-01-01T00:00:00.004Z\t1970-01-01T00:00:00.006Z
                            1970-01-01T00:00:00.000007Z\t1\t2\t2.0\t0\t3\t4\t3\t0\t3\t3.0\t0.0\t4.0\t3.0\t0.0\t3.0\t1970-01-01T00:00:00.000017Z\t1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.000017Z\t1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.000017Z\t1970-01-01T00:00:00.007Z\t1970-01-01T00:00:00.005Z\t1970-01-01T00:00:00.005Z\t1970-01-01T00:00:00.007Z\t1970-01-01T00:00:00.005Z\t1970-01-01T00:00:00.007Z
                            """),
                    "select ts, i, j, d, " +
                            "lead(j, 2, j + 1) over (partition by i order by ts desc), " +
                            "lag(j, 2, j + 1) over (partition by i order by ts desc), " +
                            "lead(j, 2, j + 1) ignore nulls over (partition by i order by ts desc), " +
                            "lag(j, 2, j + 1) ignore nulls over (partition by i order by ts desc), " +
                            "lead(j, 2, j + 1) respect nulls over (partition by i order by ts desc), " +
                            "lag(j, 2, j + 1) respect nulls over (partition by i order by ts desc), " +
                            "lead(d, 2, d + 1) over (partition by i), " +
                            "lag(d, 2, d + 1) over (partition by i), " +
                            "lead(d, 2, d + 1) ignore nulls over (partition by i order by ts desc), " +
                            "lag(d, 2, d + 1) ignore nulls over (partition by i order by ts desc), " +
                            "lead(d, 2, d + 1) respect nulls over (partition by i order by ts desc), " +
                            "lag(d, 2, d + 1) respect nulls over (partition by i order by ts desc), " +
                            "lead(ts, 2, ts + 10) over (partition by i), " +
                            "lag(ts, 2, ts + 10) over (partition by i), " +
                            "lead(ts, 2, ts + 10) ignore nulls over (partition by i order by ts desc), " +
                            "lag(ts, 2, ts + 10) ignore nulls over (partition by i order by ts desc), " +
                            "lead(ts, 2, ts + 10) respect nulls over (partition by i order by ts desc), " +
                            "lag(ts, 2, ts + 10) respect nulls over (partition by i order by ts desc), " +
                            "lead(m, 2, m) over (partition by i), " +
                            "lag(m, 2, m) over (partition by i), " +
                            "lead(m, 2, m) ignore nulls over (partition by i order by ts desc), " +
                            "lag(m, 2, m) ignore nulls over (partition by i order by ts desc), " +
                            "lead(m, 2, m) respect nulls over (partition by i order by ts desc), " +
                            "lag(m, 2, m) respect nulls over (partition by i order by ts desc), " +
                            "from tab order by ts asc",
                    "ts",
                    true,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix1("""
                            ts\ti\tj\td\tlead\tlag\tlead_ignore_nulls\tlag_ignore_nulls\tlead1\tlag1\tlead2\tlag2\tlead_ignore_nulls1\tlag_ignore_nulls1\tlead3\tlag3\tlead4\tlag4\tlead_ignore_nulls2\tlag_ignore_nulls2\tlead5\tlag5\tlead6\tlag6\tlead_ignore_nulls3\tlag_ignore_nulls3\tlead7\tlag7
                            1970-01-01T00:00:00.000001Z\t0\t1\t1.0\t1\t1\t1\t1\t1\t1\t1.0\t1.0\t1.0\t1.0\t1.0\t1.0\t1\t1970-01-01T00:00:00.000001Z\t1\t1970-01-01T00:00:00.000001Z\t1\t1970-01-01T00:00:00.000001Z\t1\t1970-01-01T00:00:00.001Z\t1\t1970-01-01T00:00:00.001Z\t1\t1970-01-01T00:00:00.001Z
                            1970-01-01T00:00:00.000002Z\t0\t2\t2.0\t2\t2\t2\t2\t2\t2\t2.0\t2.0\t2.0\t2.0\t2.0\t2.0\t2\t1970-01-01T00:00:00.000002Z\t2\t1970-01-01T00:00:00.000002Z\t2\t1970-01-01T00:00:00.000002Z\t2\t1970-01-01T00:00:00.002Z\t2\t1970-01-01T00:00:00.002Z\t2\t1970-01-01T00:00:00.002Z
                            1970-01-01T00:00:00.000003Z\t0\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t3\t1970-01-01T00:00:00.000003Z\t3\t1970-01-01T00:00:00.000003Z\t3\t1970-01-01T00:00:00.000003Z\t3\t1970-01-01T00:00:00.003Z\t3\t1970-01-01T00:00:00.003Z\t3\t1970-01-01T00:00:00.003Z
                            1970-01-01T00:00:00.000004Z\t1\t4\t4.0\t4\t4\t4\t4\t4\t4\t4.0\t4.0\t4.0\t4.0\t4.0\t4.0\t4\t1970-01-01T00:00:00.000004Z\t4\t1970-01-01T00:00:00.000004Z\t4\t1970-01-01T00:00:00.000004Z\t4\t1970-01-01T00:00:00.004Z\t4\t1970-01-01T00:00:00.004Z\t4\t1970-01-01T00:00:00.004Z
                            1970-01-01T00:00:00.000005Z\t1\t0\t0.0\t0\t0\t0\t0\t0\t0\t0.0\t0.0\t0.0\t0.0\t0.0\t0.0\t5\t1970-01-01T00:00:00.000005Z\t5\t1970-01-01T00:00:00.000005Z\t5\t1970-01-01T00:00:00.000005Z\t5\t1970-01-01T00:00:00.005Z\t5\t1970-01-01T00:00:00.005Z\t5\t1970-01-01T00:00:00.005Z
                            1970-01-01T00:00:00.000006Z\t1\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t6\t1970-01-01T00:00:00.000006Z\t6\t1970-01-01T00:00:00.000006Z\t6\t1970-01-01T00:00:00.000006Z\t6\t1970-01-01T00:00:00.006Z\t6\t1970-01-01T00:00:00.006Z\t6\t1970-01-01T00:00:00.006Z
                            1970-01-01T00:00:00.000007Z\t1\t2\t2.0\t2\t2\t2\t2\t2\t2\t2.0\t2.0\t2.0\t2.0\t2.0\t2.0\t7\t1970-01-01T00:00:00.000007Z\t7\t1970-01-01T00:00:00.000007Z\t7\t1970-01-01T00:00:00.000007Z\t7\t1970-01-01T00:00:00.007Z\t7\t1970-01-01T00:00:00.007Z\t7\t1970-01-01T00:00:00.007Z
                            """),
                    "select ts, i, j, d, " +
                            "lead(j, 0) over (partition by i order by ts desc), " +
                            "lag(j, 0) over (partition by i order by ts desc), " +
                            "lead(j, 0) ignore nulls over (partition by i order by ts desc), " +
                            "lag(j, 0) ignore nulls over (partition by i order by ts desc), " +
                            "lead(j, 0) respect nulls over (partition by i order by ts desc), " +
                            "lag(j, 0) respect nulls over (partition by i order by ts desc), " +
                            "lead(d, 0) over (partition by i order by ts desc), " +
                            "lag(d, 0) over (partition by i order by ts desc), " +
                            "lead(d, 0) ignore nulls over (partition by i order by ts desc), " +
                            "lag(d, 0) ignore nulls over (partition by i order by ts desc), " +
                            "lead(d, 0) respect nulls over (partition by i order by ts desc), " +
                            "lag(d, 0) respect nulls over (partition by i order by ts desc), " +
                            "lead(ts, 0) over (partition by i order by ts desc), " +
                            "lag(ts, 0) over (partition by i order by ts desc), " +
                            "lead(ts, 0) ignore nulls over (partition by i order by ts desc), " +
                            "lag(ts, 0) ignore nulls over (partition by i order by ts desc), " +
                            "lead(ts, 0) respect nulls over (partition by i order by ts desc), " +
                            "lag(ts, 0) respect nulls over (partition by i order by ts desc), " +
                            "lead(m, 0) over (partition by i order by ts desc), " +
                            "lag(m, 0) over (partition by i order by ts desc), " +
                            "lead(m, 0) ignore nulls over (partition by i order by ts desc), " +
                            "lag(m, 0) ignore nulls over (partition by i order by ts desc), " +
                            "lead(m, 0) respect nulls over (partition by i order by ts desc), " +
                            "lag(m, 0) respect nulls over (partition by i order by ts desc), " +
                            "from tab order by ts asc",
                    "ts",
                    true,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix1("""
                            ts\ti\tj\td\tlag\tlag_ignore_nulls\tlag1\tlag2\tlag3\tlag4
                            1970-01-01T00:00:00.000001Z\t0\t1\t1.0\tnull\tnull\tnull\tnull\t\t
                            1970-01-01T00:00:00.000002Z\t0\t2\t2.0\t1\t1\t1\t1.0\t1970-01-01T00:00:00.000001Z\t1970-01-01T00:00:00.001Z
                            1970-01-01T00:00:00.000003Z\t0\tnull\tnull\t2\t2\t2\t2.0\t1970-01-01T00:00:00.000002Z\t1970-01-01T00:00:00.002Z
                            1970-01-01T00:00:00.000004Z\t1\t4\t4.0\tnull\tnull\tnull\tnull\t\t
                            1970-01-01T00:00:00.000005Z\t1\t0\t0.0\t4\t4\t4\t4.0\t1970-01-01T00:00:00.000004Z\t1970-01-01T00:00:00.004Z
                            1970-01-01T00:00:00.000006Z\t1\tnull\tnull\t0\t0\t0\t0.0\t1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.005Z
                            1970-01-01T00:00:00.000007Z\t1\t2\t2.0\tnull\t0\tnull\tnull\t1970-01-01T00:00:00.000006Z\t1970-01-01T00:00:00.006Z
                            """),
                    "select ts, i, j, d, " +
                            "lag(j) over (partition by i), " +
                            "lag(j) ignore nulls over (partition by i), " +
                            "lag(j) respect nulls over (partition by i), " +
                            "lag(d) over (partition by i), " +
                            "lag(ts) over (partition by i), " +
                            "lag(m) over (partition by i) " +
                            "from tab ",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix1("""
                            ts\ti\tj\td\tlag\tlag_ignore_nulls\tlag1\tlag2\tlag3\tlag4
                            1970-01-01T00:00:00.000001Z\t0\t1\t1.0\tnull\tnull\tnull\tnull\t\t
                            1970-01-01T00:00:00.000002Z\t0\t2\t2.0\tnull\tnull\tnull\tnull\t\t
                            1970-01-01T00:00:00.000003Z\t0\tnull\tnull\tnull\tnull\tnull\tnull\t\t
                            1970-01-01T00:00:00.000004Z\t1\t4\t4.0\tnull\tnull\tnull\tnull\t\t
                            1970-01-01T00:00:00.000005Z\t1\t0\t0.0\tnull\tnull\tnull\tnull\t\t
                            1970-01-01T00:00:00.000006Z\t1\tnull\tnull\tnull\tnull\tnull\tnull\t\t
                            1970-01-01T00:00:00.000007Z\t1\t2\t2.0\t4\tnull\t4\t4.0\t1970-01-01T00:00:00.000004Z\t1970-01-01T00:00:00.004Z
                            """),
                    "select ts, i, j, d, " +
                            "lag(j, 3) over (partition by i), " +
                            "lag(j, 3) ignore nulls over (partition by i), " +
                            "lag(j, 3) respect nulls over (partition by i), " +
                            "lag(d, 3) over (partition by i), " +
                            "lag(ts, 3) over (partition by i), " +
                            "lag(m, 3) over (partition by i) " +
                            "from tab ",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix1("""
                            ts\ti\tj\td\tlag\tlag_ignore_nulls\tlag1\tlag2\tlag3\tlag4
                            1970-01-01T00:00:00.000001Z\t0\t1\t1.0\t2\t2\t2\t2.0\t1970-01-01T00:00:00.000002Z\t1970-01-01T00:00:00.001Z
                            1970-01-01T00:00:00.000002Z\t0\t2\t2.0\t3\t3\t3\t3.0\t1970-01-01T00:00:00.000003Z\t1970-01-01T00:00:00.002Z
                            1970-01-01T00:00:00.000003Z\t0\tnull\tnull\t1\t1\t1\t1.0\t1970-01-01T00:00:00.000001Z\t1970-01-01T00:00:00.001Z
                            1970-01-01T00:00:00.000004Z\t1\t4\t4.0\t5\t5\t5\t5.0\t1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.004Z
                            1970-01-01T00:00:00.000005Z\t1\t0\t0.0\t1\t1\t1\t1.0\t1970-01-01T00:00:00.000006Z\t1970-01-01T00:00:00.005Z
                            1970-01-01T00:00:00.000006Z\t1\tnull\tnull\t4\t4\t4\t4.0\t1970-01-01T00:00:00.000004Z\t1970-01-01T00:00:00.004Z
                            1970-01-01T00:00:00.000007Z\t1\t2\t2.0\t0\t4\t0\t0.0\t1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.005Z
                            """),
                    "select ts, i, j, d, " +
                            "lag(j, 2, j + 1) over (partition by i), " +
                            "lag(j, 2, j + 1) ignore nulls over (partition by i), " +
                            "lag(j, 2, j + 1) respect nulls over (partition by i), " +
                            "lag(d, 2, d + 1) over (partition by i), " +
                            "lag(ts, 2, ts + 1) over (partition by i), " +
                            "lag(m, 2, m) over (partition by i) " +
                            "from tab ",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testLeadException() throws Exception {
        executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, j long, d double, s symbol, c VARCHAR) timestamp(ts)", timestampType.getTypeName());
        assertExceptionNoLeakCheck(
                "select lead() over () from tab",
                7,
                "function `lead` requires arguments"
        );

        assertExceptionNoLeakCheck(
                "select lead(d, -1) over () from tab",
                15,
                "offset must be a positive integer"
        );

        assertExceptionNoLeakCheck(
                "select lead(d, 1, s) over () from tab",
                18,
                "default value must be can cast to double"
        );

        assertExceptionNoLeakCheck(
                "select lead(j, 1, s) over () from tab",
                18,
                "default value must be can cast to long"
        );

        assertExceptionNoLeakCheck(
                "select lead(d, 1, d + 1, d) over () from tab",
                25,
                "too many arguments"
        );

        assertExceptionNoLeakCheck(
                "select lead(d, i, d + 1) over () from tab",
                15,
                "offset must be a constant"
        );

        assertExceptionNoLeakCheck(
                "select lead(d, 1, sum(d)) over () from tab",
                18,
                "default value can not be a window function"
        );

        assertExceptionNoLeakCheck(
                "select lead(d, 1, sum(d)) over () from tab",
                18,
                "default value can not be a window function"
        );

        assertExceptionNoLeakCheck(
                "select lead(d, 1) ignore over () from tab",
                25,
                "'nulls' expected after 'ignore'"
        );

        assertExceptionNoLeakCheck(
                "select lead(d, 1) respect over () from tab",
                26,
                "'nulls' expected after 'respect'"
        );

        assertExceptionNoLeakCheck(
                "select lead(d, 1) ignore null over () from tab",
                25,
                "'nulls' expected, not 'null'"
        );
    }

    @Test
    public void testLeadLagTimestampMixedDefaultValue() throws Exception {
        assertMemoryLeak(() ->
                {
                    execute("create table x as (" +
                            "select " +
                            "timestamp_sequence(0, 1000000) as ts, " +
                            "timestamp_sequence_ns(0, 2000000000) as ts_ns " +
                            "from long_sequence(5)" +
                            ") timestamp(ts)");
                    assertQuery(
                            """
                                    ts\tts_ns\tlead\tlead1\tlag\tlag1
                                    1970-01-01T00:00:00.000000Z\t1970-01-01T00:00:00.000000000Z\t1970-01-01T00:00:02.000000Z\t1970-01-01T00:00:04.000000000Z\t1970-01-01T00:00:00.000000Z\t1970-01-01T00:00:00.000000000Z
                                    1970-01-01T00:00:01.000000Z\t1970-01-01T00:00:02.000000000Z\t1970-01-01T00:00:03.000000Z\t1970-01-01T00:00:06.000000000Z\t1970-01-01T00:00:02.000000Z\t1970-01-01T00:00:01.000000000Z
                                    1970-01-01T00:00:02.000000Z\t1970-01-01T00:00:04.000000000Z\t1970-01-01T00:00:04.000000Z\t1970-01-01T00:00:08.000000000Z\t1970-01-01T00:00:00.000000Z\t1970-01-01T00:00:00.000000000Z
                                    1970-01-01T00:00:03.000000Z\t1970-01-01T00:00:06.000000000Z\t1970-01-01T00:00:06.000000Z\t1970-01-01T00:00:03.000000000Z\t1970-01-01T00:00:01.000000Z\t1970-01-01T00:00:02.000000000Z
                                    1970-01-01T00:00:04.000000Z\t1970-01-01T00:00:08.000000000Z\t1970-01-01T00:00:08.000000Z\t1970-01-01T00:00:04.000000000Z\t1970-01-01T00:00:02.000000Z\t1970-01-01T00:00:04.000000000Z
                                    """,
                            "select ts, ts_ns, lead(ts, 2, ts_ns) over(), lead(ts_ns, 2, ts) over(), lag(ts, 2, ts_ns) over(), lag(ts_ns, 2, ts) over() from x;",
                            "ts",
                            true,
                            true
                    );
                }
        );
    }

    @Test
    public void testMaxDoubleOverPartitionedRangeWithLargeFrame() throws Exception {
        assertMemoryLeak(() -> {
            // Test similar to testMaxTimestampOverPartitionedRangeWithLargeFrame but for max(double)
            // This tests boundary conditions with large ranges that trigger buffer growth
            execute("create table tab (ts timestamp, i long, j long, s symbol, d double) timestamp(ts)");

            // Trigger per-partition buffers growth and free list usage
            execute("insert into tab select " +
                    "x::timestamp, " +
                    "x/10000, " +
                    "case when x % 3 = 0 THEN NULL ELSE x END, " +
                    "'k' || (x%5) ::symbol, " +
                    "(40000-x)*2::double " +
                    "from long_sequence(39999)");

            // Trigger removal of rows below lo boundary AND resize of buffer
            execute("insert into tab select " +
                    "(100000+x)::timestamp, " +
                    "(100000+x)%4, " +
                    "case when x % 3 = 0 THEN NULL ELSE 100000 + x END, " +
                    "'k' || (x%20) ::symbol, " +
                    "(360000-x)*2::double " +
                    "from long_sequence(360000)");

            assertSql(
                    """
                            i\tts\td\tmax_d_window
                            0\t1970-01-01T00:00:00.460000Z\t0.0\t160000.0
                            1\t1970-01-01T00:00:00.459997Z\t6.0\t160006.0
                            2\t1970-01-01T00:00:00.459998Z\t4.0\t160004.0
                            3\t1970-01-01T00:00:00.459999Z\t2.0\t160002.0
                            """,
                    "select i, ts, d, max_d_window from (" +
                            "select i, ts, d, " +
                            "max(d) over (partition by i order by ts range between 80000 preceding and current row) as max_d_window, " +
                            "row_number() over (partition by i order by ts desc) as rn " +
                            "from tab " +
                            "where i < 4" +
                            ") where rn = 1 " +
                            "order by i"
            );

            assertSql(
                    """
                            max_d\ti
                            160000.0\t0
                            160006.0\t1
                            160004.0\t2
                            160002.0\t3
                            """,
                    "select max(d) as max_d, i " +
                            "from ( " +
                            "  select data.d, data.i " +
                            "  from (select i, max(ts) as max from tab group by i) cnt " +
                            "  join tab data on cnt.i = data.i and data.ts >= (cnt.max - 80000) " +
                            "  order by data.i, ts " +
                            ") " +
                            "where i in (0,1,2,3) " +
                            "group by i " +
                            "order by i"
            );
        });
    }

    @Test
    public void testMaxDoubleOverPartitionedRangeWithLargeFrameNanos() throws Exception {
        assertMemoryLeak(() -> {
            // Test similar to testMaxTimestampOverPartitionedRangeWithLargeFrame but for max(double)
            // This tests boundary conditions with large ranges that trigger buffer growth
            execute("create table tab (ts timestamp_ns, i long, j long, s symbol, d double) timestamp(ts)");

            // Trigger per-partition buffers growth and free list usage
            execute("insert into tab select " +
                    "x::timestamp_ns, " +
                    "x/10000, " +
                    "case when x % 3 = 0 THEN NULL ELSE x END, " +
                    "'k' || (x%5) ::symbol, " +
                    "(40000-x)*2::double " +
                    "from long_sequence(39999)"
            );

            // Trigger removal of rows below lo boundary AND resize of buffer
            execute("insert into tab select " +
                    "(100000+x)::timestamp_ns, " +
                    "(100000+x)%4, " +
                    "case when x % 3 = 0 THEN NULL ELSE 100000 + x END, " +
                    "'k' || (x%20) ::symbol, " +
                    "(360000-x)*2::double " +
                    "from long_sequence(360000)"
            );

            assertSql(
                    """
                            i\tts\td\tmax_d_window
                            0\t1970-01-01T00:00:00.000460000Z\t0.0\t160000.0
                            1\t1970-01-01T00:00:00.000459997Z\t6.0\t160006.0
                            2\t1970-01-01T00:00:00.000459998Z\t4.0\t160004.0
                            3\t1970-01-01T00:00:00.000459999Z\t2.0\t160002.0
                            """,
                    "select i, ts, d, max_d_window from (" +
                            "select i, ts, d, " +
                            "max(d) over (partition by i order by ts range between 80000 preceding and current row) as max_d_window, " +
                            "row_number() over (partition by i order by ts desc) as rn " +
                            "from tab " +
                            "where i < 4" +
                            ") where rn = 1 " +
                            "order by i"
            );

            // Cross-check: aggregate query equivalent to the window function
            assertSql(
                    """
                            max_d\ti
                            160000.0\t0
                            160006.0\t1
                            160004.0\t2
                            160002.0\t3
                            """,
                    "select max(d) as max_d, i " +
                            "from ( " +
                            "  select data.d, data.i " +
                            "  from (select i, max(ts) as max from tab group by i) cnt " +
                            "  join tab data on cnt.i = data.i and data.ts >= (cnt.max - 80000) " +
                            "  order by data.i, ts " +
                            ") " +
                            "where i in (0,1,2,3) " +
                            "group by i " +
                            "order by i"
            );
        });
    }

    @Test
    public void testMaxNonDesignatedTimestampWithManyNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, other_ts timestamp, val long, grp symbol) timestamp(ts)");

            // Create large dataset with many nulls to test null handling in max()
            execute("insert into tab select " +
                    "dateadd('s', x::int, '2021-01-01T00:00:00.000000Z')::timestamp as ts, " +
                    "case when x % 3 = 0 then null else dateadd('h', (x % 24)::int, '2021-01-01T00:00:00.000000Z') end as other_ts, " +
                    "x as val, " +
                    "case when x % 2 = 0 then 'A' else 'B' end as grp " +
                    "from long_sequence(10000)");

            // Verify max() correctly handles nulls in large dataset
            assertQueryNoLeakCheck(
                    """
                            grp\tnon_null_count\tmax_other_ts
                            A\t3334\t2021-01-01T22:00:00.000000Z
                            B\t3333\t2021-01-01T23:00:00.000000Z
                            """,
                    "SELECT grp, " +
                            "count(other_ts) as non_null_count, " +
                            "max(other_ts) as max_other_ts " +
                            "FROM tab GROUP BY grp ORDER BY grp",
                    null,
                    true,
                    true
            );

            // Test window function with nulls
            assertQueryNoLeakCheck(
                    """
                            grp\tmax_window_ts
                            A\t2021-01-01T22:00:00.000000Z
                            B\t2021-01-01T23:00:00.000000Z
                            """,
                    "SELECT DISTINCT grp, max_window_ts FROM (" +
                            "SELECT grp, max(other_ts) OVER (PARTITION BY grp) as max_window_ts FROM tab" +
                            ") ORDER BY grp",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testMaxNonDesignatedTimestampWithManyNullsNanos() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp_ns, other_ts timestamp_ns, val long, grp symbol) timestamp(ts)");

            // Create large dataset with many nulls to test null handling in max()
            execute("insert into tab select " +
                    "dateadd('s', x::int, '2021-01-01T00:00:00.000000Z')::timestamp_ns as ts, " +
                    "case when x % 3 = 0 then null else dateadd('h', (x % 24)::int, '2021-01-01T00:00:00.000000Z') end as other_ts, " +
                    "x as val, " +
                    "case when x % 2 = 0 then 'A' else 'B' end as grp " +
                    "from long_sequence(10000)");

            // Verify max() correctly handles nulls in large dataset
            assertQueryNoLeakCheck(
                    """
                            grp\tnon_null_count\tmax_other_ts
                            A\t3334\t2021-01-01T22:00:00.000000000Z
                            B\t3333\t2021-01-01T23:00:00.000000000Z
                            """,
                    "SELECT grp, " +
                            "count(other_ts) as non_null_count, " +
                            "max(other_ts) as max_other_ts " +
                            "FROM tab GROUP BY grp ORDER BY grp",
                    null,
                    true,
                    true
            );

            // Test window function with nulls
            assertQueryNoLeakCheck(
                    """
                            grp\tmax_window_ts
                            A\t2021-01-01T22:00:00.000000000Z
                            B\t2021-01-01T23:00:00.000000000Z
                            """,
                    "SELECT DISTINCT grp, max_window_ts FROM (" +
                            "SELECT grp, max(other_ts) OVER (PARTITION BY grp) as max_window_ts FROM tab" +
                            ") ORDER BY grp",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testMaxNonDesignatedTimestampWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, other_ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', '2021-01-01T12:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', null, 2, 'A')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', '2021-01-03T08:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', null, 4, 'A')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', '2021-01-05T16:00:00.000000Z', 5, 'A')");
            execute("insert into tab values ('2021-01-06T00:00:00.000000Z', null, 6, 'A')");

            // Test max() on non-designated timestamp column containing nulls
            assertQueryNoLeakCheck(
                    """
                            ts\tother_ts\tval\tgrp\tmax_other_ts
                            2021-01-01T00:00:00.000000Z\t2021-01-01T12:00:00.000000Z\t1\tA\t2021-01-05T16:00:00.000000Z
                            2021-01-02T00:00:00.000000Z\t\t2\tA\t2021-01-05T16:00:00.000000Z
                            2021-01-03T00:00:00.000000Z\t2021-01-03T08:00:00.000000Z\t3\tA\t2021-01-05T16:00:00.000000Z
                            2021-01-04T00:00:00.000000Z\t\t4\tA\t2021-01-05T16:00:00.000000Z
                            2021-01-05T00:00:00.000000Z\t2021-01-05T16:00:00.000000Z\t5\tA\t2021-01-05T16:00:00.000000Z
                            2021-01-06T00:00:00.000000Z\t\t6\tA\t2021-01-05T16:00:00.000000Z
                            """,
                    "SELECT ts, other_ts, val, grp, max(other_ts) OVER (PARTITION BY grp) as max_other_ts FROM tab",
                    "ts",
                    true,
                    true
            );

            // Test with ORDER BY on non-designated timestamp with nulls
            assertQueryNoLeakCheck(
                    """
                            ts\tother_ts\tval\tgrp\tmax_other_ts_running
                            2021-01-01T00:00:00.000000Z\t2021-01-01T12:00:00.000000Z\t1\tA\t2021-01-01T12:00:00.000000Z
                            2021-01-02T00:00:00.000000Z\t\t2\tA\t2021-01-01T12:00:00.000000Z
                            2021-01-03T00:00:00.000000Z\t2021-01-03T08:00:00.000000Z\t3\tA\t2021-01-03T08:00:00.000000Z
                            2021-01-04T00:00:00.000000Z\t\t4\tA\t2021-01-03T08:00:00.000000Z
                            2021-01-05T00:00:00.000000Z\t2021-01-05T16:00:00.000000Z\t5\tA\t2021-01-05T16:00:00.000000Z
                            2021-01-06T00:00:00.000000Z\t\t6\tA\t2021-01-05T16:00:00.000000Z
                            """,
                    "SELECT ts, other_ts, val, grp, max(other_ts) OVER (PARTITION BY grp ORDER BY ts ROWS UNBOUNDED PRECEDING) as max_other_ts_running FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampOnNonDesignatedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, other_ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', '2021-01-05T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', '2021-01-04T00:00:00.000000Z', 2, 'B')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', '2021-01-03T00:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', '2021-01-02T00:00:00.000000Z', 4, 'B')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', '2021-01-01T00:00:00.000000Z', 5, 'C')");

            assertQueryNoLeakCheck(
                    """
                            ts\tother_ts\tval\tgrp\tmax_other_ts
                            2021-01-01T00:00:00.000000Z\t2021-01-05T00:00:00.000000Z\t1\tA\t2021-01-05T00:00:00.000000Z
                            2021-01-02T00:00:00.000000Z\t2021-01-04T00:00:00.000000Z\t2\tB\t2021-01-05T00:00:00.000000Z
                            2021-01-03T00:00:00.000000Z\t2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-05T00:00:00.000000Z
                            2021-01-04T00:00:00.000000Z\t2021-01-02T00:00:00.000000Z\t4\tB\t2021-01-05T00:00:00.000000Z
                            2021-01-05T00:00:00.000000Z\t2021-01-01T00:00:00.000000Z\t5\tC\t2021-01-05T00:00:00.000000Z
                            """,
                    "SELECT ts, other_ts, val, grp, max(other_ts) OVER () as max_other_ts FROM tab",
                    "ts",
                    true,
                    true
            );

            assertQueryNoLeakCheck(
                    """
                            ts\tother_ts\tval\tgrp\tmax_other_ts_by_grp
                            2021-01-01T00:00:00.000000Z\t2021-01-05T00:00:00.000000Z\t1\tA\t2021-01-05T00:00:00.000000Z
                            2021-01-02T00:00:00.000000Z\t2021-01-04T00:00:00.000000Z\t2\tB\t2021-01-04T00:00:00.000000Z
                            2021-01-03T00:00:00.000000Z\t2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-05T00:00:00.000000Z
                            2021-01-04T00:00:00.000000Z\t2021-01-02T00:00:00.000000Z\t4\tB\t2021-01-04T00:00:00.000000Z
                            2021-01-05T00:00:00.000000Z\t2021-01-01T00:00:00.000000Z\t5\tC\t2021-01-01T00:00:00.000000Z
                            """,
                    "SELECT ts, other_ts, val, grp, max(other_ts) OVER (PARTITION BY grp) as max_other_ts_by_grp FROM tab",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampOverPartitionedRangeWithLargeFrame() throws Exception {
        assertMemoryLeak(() -> {
            // Test similar to testFrameFunctionOverPartitionedRangeWithLargeFrame but for max(timestamp)
            // This tests boundary conditions with large ranges that trigger buffer growth
            execute("create table tab (ts timestamp, i long, j long, s symbol, d double) timestamp(ts)");

            // Trigger per-partition buffers growth and free list usage
            execute("insert into tab select " +
                    "x::timestamp, " +
                    "x/10000, " +
                    "case when x % 3 = 0 THEN NULL ELSE x END, " +
                    "'k' || (x%5) ::symbol, " +
                    "x*2::double " +
                    "from long_sequence(39999)");

            // Trigger removal of rows below lo boundary AND resize of buffer
            execute("insert into tab select " +
                    "(100000+x)::timestamp, " +
                    "(100000+x)%4, " +
                    "case when x % 3 = 0 THEN NULL ELSE 100000 + x END, " +
                    "'k' || (x%20) ::symbol, " +
                    "x*2::double " +
                    "from long_sequence(360000)");

            // Test max(timestamp) with window function over partitioned data with large frame
            // Test the last row of each partition to verify boundary conditions
            assertSql(
                    """
                            i\tts\tmax_ts_window
                            0\t1970-01-01T00:00:00.460000Z\t1970-01-01T00:00:00.460000Z
                            1\t1970-01-01T00:00:00.459997Z\t1970-01-01T00:00:00.459997Z
                            2\t1970-01-01T00:00:00.459998Z\t1970-01-01T00:00:00.459998Z
                            3\t1970-01-01T00:00:00.459999Z\t1970-01-01T00:00:00.459999Z
                            """,
                    "select i, ts, max_ts_window from (" +
                            "select i, ts, " +
                            "max(ts) over (partition by i order by ts range between 80000 preceding and current row) as max_ts_window, " +
                            "row_number() over (partition by i order by ts desc) as rn " +
                            "from tab " +
                            "where i < 4" +
                            ") where rn = 1 " +
                            "order by i"
            );

            // Cross-check: aggregate query equivalent to the window function
            assertSql(
                    """
                            max_ts\ti
                            1970-01-01T00:00:00.460000Z\t0
                            1970-01-01T00:00:00.459997Z\t1
                            1970-01-01T00:00:00.459998Z\t2
                            1970-01-01T00:00:00.459999Z\t3
                            """,
                    "select max(ts) as max_ts, i " +
                            "from ( " +
                            "  select data.ts, data.i " +
                            "  from (select i, max(ts) as max from tab group by i) cnt " +
                            "  join tab data on cnt.i = data.i and data.ts >= (cnt.max - 80000) " +
                            "  order by data.i, ts " +
                            ") " +
                            "where i in (0,1,2,3) " +
                            "group by i " +
                            "order by i"
            );
        });
    }

    @Test
    public void testMaxTimestampOverPartitionedRangeWithLargeFrameNanos() throws Exception {
        assertMemoryLeak(() -> {
            // Test similar to testMaxTimestampOverPartitionedRangeWithLargeFrame but for timestamp_ns type
            // This tests boundary conditions with large ranges that trigger buffer growth
            execute("create table tab (ts timestamp_ns, i long, j long, s symbol, d double) timestamp(ts)");

            // Trigger per-partition buffers growth and free list usage
            execute("insert into tab select " +
                    "x::timestamp_ns, " +
                    "x/10000, " +
                    "case when x % 3 = 0 THEN NULL ELSE x END, " +
                    "'k' || (x%5) ::symbol, " +
                    "x*2::double " +
                    "from long_sequence(39999)");

            // Trigger removal of rows below lo boundary AND resize of buffer
            execute("insert into tab select " +
                    "(100000+x)::timestamp_ns, " +
                    "(100000+x)%4, " +
                    "case when x % 3 = 0 THEN NULL ELSE 100000 + x END, " +
                    "'k' || (x%20) ::symbol, " +
                    "x*2::double " +
                    "from long_sequence(360000)");

            // Test max(timestamp_ns) with window function over partitioned data with large frame
            // Test the last row of each partition to verify boundary conditions
            assertSql(
                    """
                            i\tts\tmax_ts_window
                            0\t1970-01-01T00:00:00.000460000Z\t1970-01-01T00:00:00.000460000Z
                            1\t1970-01-01T00:00:00.000459997Z\t1970-01-01T00:00:00.000459997Z
                            2\t1970-01-01T00:00:00.000459998Z\t1970-01-01T00:00:00.000459998Z
                            3\t1970-01-01T00:00:00.000459999Z\t1970-01-01T00:00:00.000459999Z
                            """,
                    "select i, ts, max_ts_window from (" +
                            "select i, ts, " +
                            "max(ts) over (partition by i order by ts range between 80000 preceding and current row) as max_ts_window, " +
                            "row_number() over (partition by i order by ts desc) as rn " +
                            "from tab " +
                            "where i < 4" +
                            ") where rn = 1 " +
                            "order by i"
            );

            // Cross-check: aggregate query equivalent to the window function
            assertSql(
                    """
                            max_ts\ti
                            1970-01-01T00:00:00.000460000Z\t0
                            1970-01-01T00:00:00.000459997Z\t1
                            1970-01-01T00:00:00.000459998Z\t2
                            1970-01-01T00:00:00.000459999Z\t3
                            """,
                    "select max(ts) as max_ts, i " +
                            "from ( " +
                            "  select data.ts, data.i " +
                            "  from (select i, max(ts) as max from tab group by i) cnt " +
                            "  join tab data on cnt.i = data.i and data.ts >= (cnt.max - 80000) " +
                            "  order by data.i, ts " +
                            ") " +
                            "where i in (0,1,2,3) " +
                            "group by i " +
                            "order by i"
            );
        });
    }

    @Test
    public void testMaxTimestampOverWholeResultSet() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 2, 'B')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 4, 'B')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 5, 'C')");

            assertQueryNoLeakCheck(
                    """
                            ts\tval\tgrp\tmax_ts
                            2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-05T00:00:00.000000Z
                            2021-01-02T00:00:00.000000Z\t2\tB\t2021-01-05T00:00:00.000000Z
                            2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-05T00:00:00.000000Z
                            2021-01-04T00:00:00.000000Z\t4\tB\t2021-01-05T00:00:00.000000Z
                            2021-01-05T00:00:00.000000Z\t5\tC\t2021-01-05T00:00:00.000000Z
                            """,
                    "SELECT ts, val, grp, max(ts) OVER () as max_ts FROM tab",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampRangeFrameBufferOverflow() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val long, grp symbol) timestamp(ts)");

            // Create many partitions (>40) with dense timestamps to trigger deque resize
            // Each partition will have many overlapping frames requiring deque growth
            execute("insert into tab select " +
                    "dateadd('s', ((x-1) * 10)::int, '2021-01-01T00:00:00.000000Z'::timestamp) as ts, " +
                    "x as val, " +
                    "'grp_' || ((x-1) % 100) as grp " +  // 100 different partitions
                    "from long_sequence(50000)");

            // Test with wide time window that will cause deque to hold many max values simultaneously
            // With 10-second intervals and 1-hour window, each partition will have ~360 overlapping frames
            assertQueryNoLeakCheck(
                    """
                            cnt\tpartitions
                            50000\t100
                            """,
                    "SELECT count(*) as cnt, count(distinct grp)  as partitions FROM (" +
                            "SELECT ts, val, grp, " +
                            "max(ts) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN '1' HOUR PRECEDING AND CURRENT ROW) as max_ts_large_window " +
                            "FROM tab" +
                            ")",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampRangeFrameDequeOverflow() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val long, grp symbol) timestamp(ts)");

            // Create scenario that will trigger deque overflow in bounded range frames
            execute("insert into tab select " +
                    "dateadd('s', x::int, '2021-01-01T00:00:00.000000Z')::timestamp as ts, " +
                    "x as val, " +
                    "case when x % 5 = 0 then 'A' else 'B' end as grp " +
                    "from long_sequence(50000)");

            // Test bounded range frame that exercises dequeMem overflow handling
            assertQueryNoLeakCheck(
                    """
                            cnt\tmax_val
                            50000\t50000
                            """,
                    "SELECT count(*) as cnt, max(val) as max_val FROM (" +
                            "SELECT ts, val, grp, " +
                            "max(ts) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN '2' HOUR PRECEDING AND '1' HOUR PRECEDING) as max_ts_bounded " +
                            "FROM tab" +
                            ")",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampRangeFrameEdgeCases() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val long, grp symbol) timestamp(ts)");

            // Create edge case scenario: overlapping time ranges, identical timestamps
            execute("insert into tab values " +
                    "('2021-01-01T12:00:00.000000Z', 1, 'A'), " +
                    "('2021-01-01T12:00:00.000000Z', 2, 'A'), " + // duplicate timestamp
                    "('2021-01-01T12:00:01.000000Z', 3, 'A'), " +
                    "('2021-01-01T12:00:01.000000Z', 4, 'A'), " + // duplicate timestamp
                    "('2021-01-01T12:00:02.000000Z', 5, 'A'), " +
                    "('2021-01-01T12:00:03.000000Z', 6, 'A'), " +
                    "('2021-01-01T12:00:04.000000Z', 7, 'A'), " +
                    "('2021-01-01T12:00:05.000000Z', 8, 'A')");

            // Test precise boundary conditions in range frames
            assertQueryNoLeakCheck(
                    """
                            ts\tval\tmax_ts_precise
                            2021-01-01T12:00:00.000000Z\t1\t
                            2021-01-01T12:00:00.000000Z\t2\t
                            2021-01-01T12:00:01.000000Z\t3\t
                            2021-01-01T12:00:01.000000Z\t4\t
                            2021-01-01T12:00:02.000000Z\t5\t2021-01-01T12:00:00.000000Z
                            2021-01-01T12:00:03.000000Z\t6\t2021-01-01T12:00:01.000000Z
                            2021-01-01T12:00:04.000000Z\t7\t2021-01-01T12:00:02.000000Z
                            2021-01-01T12:00:05.000000Z\t8\t2021-01-01T12:00:03.000000Z
                            """,
                    "SELECT ts, val, max(ts) OVER (ORDER BY ts RANGE BETWEEN '4' SECOND preceding AND '2' SECOND preceding) as max_ts_precise FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampRangeFrameWithManyPartitions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val long, grp symbol) timestamp(ts)");

            // Create many partitions to test Map resizing and partition management
            execute("insert into tab select " +
                    "dateadd('s', x::int, '2021-01-01T00:00:00.000000Z')::timestamp as ts, " +
                    "x as val, " +
                    "'grp_' || (x % 1000) as grp " +
                    "from long_sequence(25000)");

            // Test with many partitions to exercise Map operations in MaxMinOverPartitionRangeFrameFunction
            assertQueryNoLeakCheck(
                    """
                            partition_count\ttotal_rows
                            1000\t25000
                            """,
                    "SELECT count(distinct grp) as partition_count, count(*) as total_rows FROM (" +
                            "SELECT ts, val, grp, " +
                            "max(ts) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN '30' MINUTE PRECEDING AND CURRENT ROW) as max_ts " +
                            "FROM tab" +
                            ")",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampRangeOnNonDesignatedTimestampFails() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, other_ts timestamp, val int) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', '2021-01-01T12:00:00.000000Z', 1)");

            // This should fail because we're using RANGE with ORDER BY on a non-designated timestamp
            assertExceptionNoLeakCheck(
                    "SELECT ts, other_ts, val, max(ts) OVER (ORDER BY other_ts RANGE BETWEEN '1' HOUR PRECEDING AND CURRENT ROW) FROM tab",
                    49,
                    "RANGE is supported only for queries ordered by designated timestamp"
            );
        });
    }

    @Test
    public void testMaxTimestampRangeWithBoundedFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-01T01:00:00.000000Z', 2, 'A')");
            execute("insert into tab values ('2021-01-01T02:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-01T03:00:00.000000Z', 4, 'A')");
            execute("insert into tab values ('2021-01-01T04:00:00.000000Z', 5, 'A')");
            execute("insert into tab values ('2021-01-01T05:00:00.000000Z', 6, 'A')");

            // Test range between x preceding and y preceding (bounded frame - not touching current row)
            assertQueryNoLeakCheck(
                    """
                            ts\tval\tgrp\tmax_ts_bounded
                            2021-01-01T00:00:00.000000Z\t1\tA\t
                            2021-01-01T01:00:00.000000Z\t2\tA\t2021-01-01T00:00:00.000000Z
                            2021-01-01T02:00:00.000000Z\t3\tA\t2021-01-01T01:00:00.000000Z
                            2021-01-01T03:00:00.000000Z\t4\tA\t2021-01-01T02:00:00.000000Z
                            2021-01-01T04:00:00.000000Z\t5\tA\t2021-01-01T03:00:00.000000Z
                            2021-01-01T05:00:00.000000Z\t6\tA\t2021-01-01T04:00:00.000000Z
                            """,
                    "SELECT ts, val, grp, max(ts) OVER (ORDER BY ts RANGE BETWEEN '2' HOUR PRECEDING AND '1' HOUR PRECEDING) as max_ts_bounded FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampRangeWithLargerTimeIntervals() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 2, 'A')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 4, 'A')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 5, 'A')");
            execute("insert into tab values ('2021-01-06T00:00:00.000000Z', 6, 'A')");
            execute("insert into tab values ('2021-01-07T00:00:00.000000Z', 7, 'A')");

            // Test with day intervals to exercise larger time ranges
            assertQueryNoLeakCheck(
                    """
                            ts\tval\tgrp\tmax_ts_3d
                            2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-01T00:00:00.000000Z
                            2021-01-02T00:00:00.000000Z\t2\tA\t2021-01-02T00:00:00.000000Z
                            2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-03T00:00:00.000000Z
                            2021-01-04T00:00:00.000000Z\t4\tA\t2021-01-04T00:00:00.000000Z
                            2021-01-05T00:00:00.000000Z\t5\tA\t2021-01-05T00:00:00.000000Z
                            2021-01-06T00:00:00.000000Z\t6\tA\t2021-01-06T00:00:00.000000Z
                            2021-01-07T00:00:00.000000Z\t7\tA\t2021-01-07T00:00:00.000000Z
                            """,
                    "SELECT ts, val, grp, max(ts) OVER (ORDER BY ts RANGE BETWEEN '3' DAY PRECEDING AND CURRENT ROW) as max_ts_3d FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampRangeWithMicrosecondPrecision() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-01T00:00:00.000100Z', 2, 'A')");
            execute("insert into tab values ('2021-01-01T00:00:00.000200Z', 3, 'A')");
            execute("insert into tab values ('2021-01-01T00:00:00.000300Z', 4, 'A')");
            execute("insert into tab values ('2021-01-01T00:00:00.000400Z', 5, 'A')");

            // Test with microsecond precision to exercise fine-grained time windows
            assertQueryNoLeakCheck(
                    """
                            ts\tval\tgrp\tmax_ts_micro
                            2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-01T00:00:00.000000Z
                            2021-01-01T00:00:00.000100Z\t2\tA\t2021-01-01T00:00:00.000100Z
                            2021-01-01T00:00:00.000200Z\t3\tA\t2021-01-01T00:00:00.000200Z
                            2021-01-01T00:00:00.000300Z\t4\tA\t2021-01-01T00:00:00.000300Z
                            2021-01-01T00:00:00.000400Z\t5\tA\t2021-01-01T00:00:00.000400Z
                            """,
                    "SELECT ts, val, grp, max(ts) OVER (ORDER BY ts RANGE BETWEEN 150 MICROSECOND PRECEDING AND CURRENT ROW) as max_ts_micro FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampRangeWithPartitionAndSpecificBounds() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-01T06:00:00.000000Z', 2, 'B')");
            execute("insert into tab values ('2021-01-01T12:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-01T18:00:00.000000Z', 4, 'B')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 5, 'A')");
            execute("insert into tab values ('2021-01-02T06:00:00.000000Z', 6, 'B')");

            // Test partitioned range with specific bounds - this exercises the MaxMinOverPartitionRangeFrameFunction
            assertQueryNoLeakCheck(
                    """
                            ts\tval\tgrp\tmax_ts_by_grp_6h
                            2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-01T00:00:00.000000Z
                            2021-01-01T06:00:00.000000Z\t2\tB\t2021-01-01T06:00:00.000000Z
                            2021-01-01T12:00:00.000000Z\t3\tA\t2021-01-01T12:00:00.000000Z
                            2021-01-01T18:00:00.000000Z\t4\tB\t2021-01-01T18:00:00.000000Z
                            2021-01-02T00:00:00.000000Z\t5\tA\t2021-01-02T00:00:00.000000Z
                            2021-01-02T06:00:00.000000Z\t6\tB\t2021-01-02T06:00:00.000000Z
                            """,
                    "SELECT ts, val, grp, max(ts) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN '6' HOUR PRECEDING AND CURRENT ROW) as max_ts_by_grp_6h FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampRangeWithPartitionedBoundedFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-01T01:00:00.000000Z', 2, 'B')");
            execute("insert into tab values ('2021-01-01T02:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-01T03:00:00.000000Z', 4, 'B')");
            execute("insert into tab values ('2021-01-01T04:00:00.000000Z', 5, 'A')");
            execute("insert into tab values ('2021-01-01T05:00:00.000000Z', 6, 'B')");

            // Test partitioned range with bounded frame - this exercises the dequeMem path
            assertQueryNoLeakCheck(
                    """
                            ts\tval\tgrp\tmax_ts_part_bounded
                            2021-01-01T00:00:00.000000Z\t1\tA\t
                            2021-01-01T01:00:00.000000Z\t2\tB\t
                            2021-01-01T02:00:00.000000Z\t3\tA\t2021-01-01T00:00:00.000000Z
                            2021-01-01T03:00:00.000000Z\t4\tB\t2021-01-01T01:00:00.000000Z
                            2021-01-01T04:00:00.000000Z\t5\tA\t2021-01-01T02:00:00.000000Z
                            2021-01-01T05:00:00.000000Z\t6\tB\t2021-01-01T03:00:00.000000Z
                            """,
                    "SELECT ts, val, grp, max(ts) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN '2' HOUR PRECEDING AND '1' HOUR PRECEDING) as max_ts_part_bounded FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampRangeWithSpecificBounds() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-01T06:00:00.000000Z', 2, 'A')");
            execute("insert into tab values ('2021-01-01T12:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-01T18:00:00.000000Z', 4, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 5, 'A')");
            execute("insert into tab values ('2021-01-02T06:00:00.000000Z', 6, 'A')");

            // Test range with 12 hours preceding to current row (not unbounded)
            assertQueryNoLeakCheck(
                    """
                            ts\tval\tgrp\tmax_ts_12h
                            2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-01T00:00:00.000000Z
                            2021-01-01T06:00:00.000000Z\t2\tA\t2021-01-01T06:00:00.000000Z
                            2021-01-01T12:00:00.000000Z\t3\tA\t2021-01-01T12:00:00.000000Z
                            2021-01-01T18:00:00.000000Z\t4\tA\t2021-01-01T18:00:00.000000Z
                            2021-01-02T00:00:00.000000Z\t5\tA\t2021-01-02T00:00:00.000000Z
                            2021-01-02T06:00:00.000000Z\t6\tA\t2021-01-02T06:00:00.000000Z
                            """,
                    "SELECT ts, val, grp, max(ts) OVER (ORDER BY ts RANGE BETWEEN '12' HOUR PRECEDING AND CURRENT ROW) as max_ts_12h FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampRowsCurrentRowOnly() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 2, 'A')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 4, 'A')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 5, 'A')");

            // Test ROWS BETWEEN CURRENT ROW AND CURRENT ROW - exercises MaxMinOverCurrentRowFunction
            assertQueryNoLeakCheck(
                    """
                            ts\tval\tgrp\tmax_ts_current_only
                            2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-01T00:00:00.000000Z
                            2021-01-02T00:00:00.000000Z\t2\tA\t2021-01-02T00:00:00.000000Z
                            2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-03T00:00:00.000000Z
                            2021-01-04T00:00:00.000000Z\t4\tA\t2021-01-04T00:00:00.000000Z
                            2021-01-05T00:00:00.000000Z\t5\tA\t2021-01-05T00:00:00.000000Z
                            """,
                    "SELECT ts, val, grp, max(ts) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) as max_ts_current_only FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampRowsFrameCombinations() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 2, 'A')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 4, 'A')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 5, 'A')");

            // Test multiple ROWS frame variations in one query to ensure different code paths work together
            assertQueryNoLeakCheck(
                    """
                            ts\tval\tgrp\tunbounded_to_current\tcurrent_only\twhole_partition
                            2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-01T00:00:00.000000Z\t2021-01-01T00:00:00.000000Z\t2021-01-05T00:00:00.000000Z
                            2021-01-02T00:00:00.000000Z\t2\tA\t2021-01-02T00:00:00.000000Z\t2021-01-02T00:00:00.000000Z\t2021-01-05T00:00:00.000000Z
                            2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-03T00:00:00.000000Z\t2021-01-03T00:00:00.000000Z\t2021-01-05T00:00:00.000000Z
                            2021-01-04T00:00:00.000000Z\t4\tA\t2021-01-04T00:00:00.000000Z\t2021-01-04T00:00:00.000000Z\t2021-01-05T00:00:00.000000Z
                            2021-01-05T00:00:00.000000Z\t5\tA\t2021-01-05T00:00:00.000000Z\t2021-01-05T00:00:00.000000Z\t2021-01-05T00:00:00.000000Z
                            """,
                    "SELECT ts, val, grp, " +
                            "max(ts) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as unbounded_to_current, " +
                            "max(ts) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) as current_only, " +
                            "max(ts) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as whole_partition " +
                            "FROM tab",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampRowsUnboundedPrecedingToCurrentRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 2, 'B')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 4, 'B')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 5, 'A')");

            // Test ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW - exercises MaxMinOverUnboundedPartitionRowsFrameFunction
            assertQueryNoLeakCheck(
                    """
                            ts\tval\tgrp\tmax_ts_unbounded_rows
                            2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-01T00:00:00.000000Z
                            2021-01-02T00:00:00.000000Z\t2\tB\t2021-01-02T00:00:00.000000Z
                            2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-03T00:00:00.000000Z
                            2021-01-04T00:00:00.000000Z\t4\tB\t2021-01-04T00:00:00.000000Z
                            2021-01-05T00:00:00.000000Z\t5\tA\t2021-01-05T00:00:00.000000Z
                            """,
                    "SELECT ts, val, grp, max(ts) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as max_ts_unbounded_rows FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampRowsUnboundedWithoutPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 2, 'B')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 4, 'B')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 5, 'A')");

            // Test without PARTITION BY to exercise different code path
            assertQueryNoLeakCheck(
                    """
                            ts\tval\tgrp\tmax_ts_no_partition
                            2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-01T00:00:00.000000Z
                            2021-01-02T00:00:00.000000Z\t2\tB\t2021-01-02T00:00:00.000000Z
                            2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-03T00:00:00.000000Z
                            2021-01-04T00:00:00.000000Z\t4\tB\t2021-01-04T00:00:00.000000Z
                            2021-01-05T00:00:00.000000Z\t5\tA\t2021-01-05T00:00:00.000000Z
                            """,
                    "SELECT ts, val, grp, max(ts) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as max_ts_no_partition FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampRowsWholePartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 2, 'B')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 4, 'B')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 5, 'A')");

            // Test ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING - exercises MaxMinOverPartitionFunction
            assertQueryNoLeakCheck(
                    """
                            ts\tval\tgrp\tmax_ts_whole_partition
                            2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-05T00:00:00.000000Z
                            2021-01-02T00:00:00.000000Z\t2\tB\t2021-01-04T00:00:00.000000Z
                            2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-05T00:00:00.000000Z
                            2021-01-04T00:00:00.000000Z\t4\tB\t2021-01-04T00:00:00.000000Z
                            2021-01-05T00:00:00.000000Z\t5\tA\t2021-01-05T00:00:00.000000Z
                            """,
                    "SELECT ts, val, grp, max(ts) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as max_ts_whole_partition FROM tab",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampWithComplexFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 2, 'A')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 4, 'B')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 5, 'B')");
            execute("insert into tab values ('2021-01-06T00:00:00.000000Z', 6, 'B')");
            execute("insert into tab values ('2021-01-07T00:00:00.000000Z', 7, 'C')");
            execute("insert into tab values ('2021-01-08T00:00:00.000000Z', 8, 'C')");

            assertQueryNoLeakCheck(
                    """
                            ts\tval\tgrp\tmax_ts_complex
                            2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-01T00:00:00.000000Z
                            2021-01-02T00:00:00.000000Z\t2\tA\t2021-01-02T00:00:00.000000Z
                            2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-03T00:00:00.000000Z
                            2021-01-04T00:00:00.000000Z\t4\tB\t2021-01-04T00:00:00.000000Z
                            2021-01-05T00:00:00.000000Z\t5\tB\t2021-01-05T00:00:00.000000Z
                            2021-01-06T00:00:00.000000Z\t6\tB\t2021-01-06T00:00:00.000000Z
                            2021-01-07T00:00:00.000000Z\t7\tC\t2021-01-07T00:00:00.000000Z
                            2021-01-08T00:00:00.000000Z\t8\tC\t2021-01-08T00:00:00.000000Z
                            """,
                    "SELECT ts, val, grp, max(ts) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) as max_ts_complex FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampWithDuplicateTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 2, 'B')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 4, 'B')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 5, 'C')");

            assertQueryNoLeakCheck(
                    """
                            ts\tval\tgrp\tmax_ts
                            2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-03T00:00:00.000000Z
                            2021-01-01T00:00:00.000000Z\t2\tB\t2021-01-03T00:00:00.000000Z
                            2021-01-02T00:00:00.000000Z\t3\tA\t2021-01-03T00:00:00.000000Z
                            2021-01-02T00:00:00.000000Z\t4\tB\t2021-01-03T00:00:00.000000Z
                            2021-01-03T00:00:00.000000Z\t5\tC\t2021-01-03T00:00:00.000000Z
                            """,
                    "SELECT ts, val, grp, max(ts) OVER () as max_ts FROM tab",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampWithEmptyPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");

            assertQueryNoLeakCheck(
                    "ts\tval\tgrp\tmax_ts\n",
                    "SELECT ts, val, grp, max(ts) OVER () as max_ts FROM tab",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampWithJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab1 (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("create table tab2 (ts timestamp, val int, grp symbol) timestamp(ts)");

            execute("insert into tab1 values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab1 values ('2021-01-02T00:00:00.000000Z', 2, 'B')");
            execute("insert into tab1 values ('2021-01-03T00:00:00.000000Z', 3, 'A')");

            execute("insert into tab2 values ('2021-01-02T00:00:00.000000Z', 10, 'A')");
            execute("insert into tab2 values ('2021-01-03T00:00:00.000000Z', 20, 'B')");
            execute("insert into tab2 values ('2021-01-04T00:00:00.000000Z', 30, 'A')");

            assertSql(
                    """
                            t1_ts\tt1_val\tt1_grp\tt2_ts\tt2_val\tttt
                            2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-02T00:00:00.000000Z\t10\t2021-01-02T00:00:00.000000Z
                            2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-04T00:00:00.000000Z\t30\t2021-01-04T00:00:00.000000Z
                            2021-01-02T00:00:00.000000Z\t2\tB\t2021-01-03T00:00:00.000000Z\t20\t2021-01-03T00:00:00.000000Z
                            2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-02T00:00:00.000000Z\t10\t2021-01-03T00:00:00.000000Z
                            2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-04T00:00:00.000000Z\t30\t2021-01-04T00:00:00.000000Z
                            """,
                    "SELECT t1.ts as t1_ts, t1.val as t1_val, t1.grp as t1_grp, t2.ts as t2_ts, t2.val as t2_val, " +
                            "CASE WHEN t1.ts > t2.ts THEN t1.ts ELSE t2.ts END as ttt " +
                            "FROM tab1 t1 JOIN tab2 t2 ON t1.grp = t2.grp ORDER BY t1.ts, t2.ts"
            );
            assertQueryNoLeakCheck(
                    """
                            t1_ts\tt1_val\tt1_grp\tt2_ts\tt2_val\tmax_ts
                            2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-02T00:00:00.000000Z\t10\t2021-01-04T00:00:00.000000Z
                            2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-04T00:00:00.000000Z\t30\t2021-01-04T00:00:00.000000Z
                            2021-01-02T00:00:00.000000Z\t2\tB\t2021-01-03T00:00:00.000000Z\t20\t2021-01-03T00:00:00.000000Z
                            2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-02T00:00:00.000000Z\t10\t2021-01-04T00:00:00.000000Z
                            2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-04T00:00:00.000000Z\t30\t2021-01-04T00:00:00.000000Z
                            """,
                    "SELECT t1.ts as t1_ts, t1.val as t1_val, t1.grp as t1_grp, t2.ts as t2_ts, t2.val as t2_val, " +
                            "max(CASE WHEN t1.ts > t2.ts THEN t1.ts ELSE t2.ts END) OVER (PARTITION BY t1.grp) as max_ts " +
                            "FROM tab1 t1 JOIN tab2 t2 ON t1.grp = t2.grp ORDER BY t1.ts, t2.ts",
                    "t1_ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampWithLargeDataset() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val long, grp symbol) timestamp(ts)");
            execute("insert into tab select timestamp_sequence(0, 1000000), x, rnd_symbol('A','B','C','D','E') from long_sequence(100000)");

            assertQueryNoLeakCheck(
                    """
                            cnt
                            100000
                            """,
                    "SELECT count(*) as cnt FROM (SELECT ts, max(ts) OVER (PARTITION BY grp) as max_ts FROM tab)",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampWithMixedFrames() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-01T06:00:00.000000Z', 2, 'A')");
            execute("insert into tab values ('2021-01-01T12:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-01T18:00:00.000000Z', 4, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 5, 'A')");
            execute("insert into tab values ('2021-01-02T06:00:00.000000Z', 6, 'A')");
            execute("insert into tab values ('2021-01-02T12:00:00.000000Z', 7, 'A')");

            assertQueryNoLeakCheck(
                    """
                            ts\tval\tgrp\tmax_rows\tmax_range
                            2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-01T00:00:00.000000Z\t2021-01-01T00:00:00.000000Z
                            2021-01-01T06:00:00.000000Z\t2\tA\t2021-01-01T06:00:00.000000Z\t2021-01-01T06:00:00.000000Z
                            2021-01-01T12:00:00.000000Z\t3\tA\t2021-01-01T12:00:00.000000Z\t2021-01-01T12:00:00.000000Z
                            2021-01-01T18:00:00.000000Z\t4\tA\t2021-01-01T18:00:00.000000Z\t2021-01-01T18:00:00.000000Z
                            2021-01-02T00:00:00.000000Z\t5\tA\t2021-01-02T00:00:00.000000Z\t2021-01-02T00:00:00.000000Z
                            2021-01-02T06:00:00.000000Z\t6\tA\t2021-01-02T06:00:00.000000Z\t2021-01-02T06:00:00.000000Z
                            2021-01-02T12:00:00.000000Z\t7\tA\t2021-01-02T12:00:00.000000Z\t2021-01-02T12:00:00.000000Z
                            """,
                    "SELECT ts, val, grp, " +
                            "max(ts) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) as max_rows, " +
                            "max(ts) OVER (ORDER BY ts RANGE BETWEEN '6' HOUR PRECEDING AND CURRENT ROW) as max_range " +
                            "FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampWithMultiplePartitions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp1 symbol, grp2 symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A', 'X')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 2, 'A', 'Y')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 3, 'B', 'X')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 4, 'B', 'Y')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 5, 'A', 'X')");
            execute("insert into tab values ('2021-01-06T00:00:00.000000Z', 6, 'B', 'X')");

            assertQueryNoLeakCheck(
                    """
                            ts\tval\tgrp1\tgrp2\tmax_ts_by_grps
                            2021-01-01T00:00:00.000000Z\t1\tA\tX\t2021-01-05T00:00:00.000000Z
                            2021-01-02T00:00:00.000000Z\t2\tA\tY\t2021-01-02T00:00:00.000000Z
                            2021-01-03T00:00:00.000000Z\t3\tB\tX\t2021-01-06T00:00:00.000000Z
                            2021-01-04T00:00:00.000000Z\t4\tB\tY\t2021-01-04T00:00:00.000000Z
                            2021-01-05T00:00:00.000000Z\t5\tA\tX\t2021-01-05T00:00:00.000000Z
                            2021-01-06T00:00:00.000000Z\t6\tB\tX\t2021-01-06T00:00:00.000000Z
                            """,
                    "SELECT ts, val, grp1, grp2, max(ts) OVER (PARTITION BY grp1, grp2) as max_ts_by_grps FROM tab",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol, other_ts timestamp) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A', '2021-01-01T12:00:00.000000Z')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 2, 'A', null)");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 3, 'A', '2021-01-03T12:00:00.000000Z')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 4, 'A', null)");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 5, 'A', '2021-01-05T12:00:00.000000Z')");

            assertQueryNoLeakCheck(
                    """
                            ts\tval\tgrp\tother_ts\tmax_other_ts
                            2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-01T12:00:00.000000Z\t2021-01-05T12:00:00.000000Z
                            2021-01-02T00:00:00.000000Z\t2\tA\t\t2021-01-05T12:00:00.000000Z
                            2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-03T12:00:00.000000Z\t2021-01-05T12:00:00.000000Z
                            2021-01-04T00:00:00.000000Z\t4\tA\t\t2021-01-05T12:00:00.000000Z
                            2021-01-05T00:00:00.000000Z\t5\tA\t2021-01-05T12:00:00.000000Z\t2021-01-05T12:00:00.000000Z
                            """,
                    "SELECT ts, val, grp, other_ts, max(other_ts) OVER () as max_other_ts FROM tab",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampWithOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 5, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 3, 'B')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 7, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 2, 'B')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 9, 'C')");

            assertQueryNoLeakCheck(
                    """
                            ts\tval\tgrp\tmax_ts_cumulative
                            2021-01-01T00:00:00.000000Z\t5\tA\t2021-01-01T00:00:00.000000Z
                            2021-01-02T00:00:00.000000Z\t3\tB\t2021-01-02T00:00:00.000000Z
                            2021-01-03T00:00:00.000000Z\t7\tA\t2021-01-03T00:00:00.000000Z
                            2021-01-04T00:00:00.000000Z\t2\tB\t2021-01-04T00:00:00.000000Z
                            2021-01-05T00:00:00.000000Z\t9\tC\t2021-01-05T00:00:00.000000Z
                            """,
                    "SELECT ts, val, grp, max(ts) OVER (ORDER BY ts) as max_ts_cumulative FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampWithOrderByDesc() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 2, 'B')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 4, 'B')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 5, 'C')");

            assertQueryNoLeakCheck(
                    """
                            ts\tval\tgrp\tmax_ts_desc
                            2021-01-05T00:00:00.000000Z\t5\tC\t2021-01-05T00:00:00.000000Z
                            2021-01-04T00:00:00.000000Z\t4\tB\t2021-01-05T00:00:00.000000Z
                            2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-05T00:00:00.000000Z
                            2021-01-02T00:00:00.000000Z\t2\tB\t2021-01-05T00:00:00.000000Z
                            2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-05T00:00:00.000000Z
                            """,
                    "SELECT ts, val, grp, max(ts) OVER (ORDER BY ts DESC) as max_ts_desc FROM tab ORDER BY ts DESC",
                    "ts###desc",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampWithPartitionBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 2, 'B')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 4, 'B')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 5, 'C')");

            assertQueryNoLeakCheck(
                    """
                            ts\tval\tgrp\tmax_ts_by_grp
                            2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-03T00:00:00.000000Z
                            2021-01-02T00:00:00.000000Z\t2\tB\t2021-01-04T00:00:00.000000Z
                            2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-03T00:00:00.000000Z
                            2021-01-04T00:00:00.000000Z\t4\tB\t2021-01-04T00:00:00.000000Z
                            2021-01-05T00:00:00.000000Z\t5\tC\t2021-01-05T00:00:00.000000Z
                            """,
                    "SELECT ts, val, grp, max(ts) OVER (PARTITION BY grp) as max_ts_by_grp FROM tab",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampWithPartitionByAndOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 2, 'B')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 4, 'B')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 5, 'A')");
            execute("insert into tab values ('2021-01-06T00:00:00.000000Z', 6, 'C')");

            assertQueryNoLeakCheck(
                    """
                            ts\tval\tgrp\tmax_ts_running
                            2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-01T00:00:00.000000Z
                            2021-01-02T00:00:00.000000Z\t2\tB\t2021-01-02T00:00:00.000000Z
                            2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-03T00:00:00.000000Z
                            2021-01-04T00:00:00.000000Z\t4\tB\t2021-01-04T00:00:00.000000Z
                            2021-01-05T00:00:00.000000Z\t5\tA\t2021-01-05T00:00:00.000000Z
                            2021-01-06T00:00:00.000000Z\t6\tC\t2021-01-06T00:00:00.000000Z
                            """,
                    "SELECT ts, val, grp, max(ts) OVER (PARTITION BY grp ORDER BY ts) as max_ts_running FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampWithRangeBetween() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-01T12:00:00.000000Z', 2, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-02T12:00:00.000000Z', 4, 'A')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 5, 'A')");

            assertQueryNoLeakCheck(
                    """
                            ts\tval\tgrp\tmax_ts_range
                            2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-01T00:00:00.000000Z
                            2021-01-01T12:00:00.000000Z\t2\tA\t2021-01-01T12:00:00.000000Z
                            2021-01-02T00:00:00.000000Z\t3\tA\t2021-01-02T00:00:00.000000Z
                            2021-01-02T12:00:00.000000Z\t4\tA\t2021-01-02T12:00:00.000000Z
                            2021-01-03T00:00:00.000000Z\t5\tA\t2021-01-03T00:00:00.000000Z
                            """,
                    "SELECT ts, val, grp, max(ts) OVER (ORDER BY ts RANGE BETWEEN '1' DAY PRECEDING AND CURRENT ROW) as max_ts_range FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampWithRowsBetween() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 2, 'A')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 4, 'A')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 5, 'A')");

            assertQueryNoLeakCheck(
                    """
                            ts\tval\tgrp\tmax_ts_window
                            2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-01T00:00:00.000000Z
                            2021-01-02T00:00:00.000000Z\t2\tA\t2021-01-02T00:00:00.000000Z
                            2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-03T00:00:00.000000Z
                            2021-01-04T00:00:00.000000Z\t4\tA\t2021-01-04T00:00:00.000000Z
                            2021-01-05T00:00:00.000000Z\t5\tA\t2021-01-05T00:00:00.000000Z
                            """,
                    "SELECT ts, val, grp, max(ts) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) as max_ts_window FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampWithRowsPrecedingOnly() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 2, 'A')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 4, 'A')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 5, 'A')");

            assertQueryNoLeakCheck(
                    """
                            ts\tval\tgrp\tmax_ts_preceding
                            2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-01T00:00:00.000000Z
                            2021-01-02T00:00:00.000000Z\t2\tA\t2021-01-02T00:00:00.000000Z
                            2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-03T00:00:00.000000Z
                            2021-01-04T00:00:00.000000Z\t4\tA\t2021-01-04T00:00:00.000000Z
                            2021-01-05T00:00:00.000000Z\t5\tA\t2021-01-05T00:00:00.000000Z
                            """,
                    "SELECT ts, val, grp, max(ts) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as max_ts_preceding FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampWithSingleRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");

            assertQueryNoLeakCheck(
                    """
                            ts\tval\tgrp\tmax_ts
                            2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-01T00:00:00.000000Z
                            """,
                    "SELECT ts, val, grp, max(ts) OVER () as max_ts FROM tab",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampWithSubquery() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 2, 'B')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 4, 'B')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 5, 'C')");

            assertQueryNoLeakCheck(
                    """
                            grp\tmax_ts
                            A\t2021-01-03T00:00:00.000000Z
                            B\t2021-01-04T00:00:00.000000Z
                            C\t2021-01-05T00:00:00.000000Z
                            """,
                    "SELECT DISTINCT grp, max_ts FROM (SELECT grp, max(ts) OVER (PARTITION BY grp) as max_ts FROM tab) ORDER BY grp",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampWithUnboundedPreceding() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 2, 'A')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 4, 'A')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 5, 'A')");

            assertQueryNoLeakCheck(
                    """
                            ts\tval\tgrp\tmax_ts_unbounded
                            2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-01T00:00:00.000000Z
                            2021-01-02T00:00:00.000000Z\t2\tA\t2021-01-02T00:00:00.000000Z
                            2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-03T00:00:00.000000Z
                            2021-01-04T00:00:00.000000Z\t4\tA\t2021-01-04T00:00:00.000000Z
                            2021-01-05T00:00:00.000000Z\t5\tA\t2021-01-05T00:00:00.000000Z
                            """,
                    "SELECT ts, val, grp, max(ts) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as max_ts_unbounded FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMinDoubleOverPartitionedRangeWithLargeFrame() throws Exception {
        assertMemoryLeak(() -> {
            // Test similar to testMaxDoubleOverPartitionedRangeWithLargeFrame but for min(double)
            // This tests boundary conditions with large ranges that trigger buffer growth
            execute("create table tab (ts timestamp, i long, j long, s symbol, d double) timestamp(ts)");

            // Trigger per-partition buffers growth and free list usage
            execute("insert into tab select " +
                    "x::timestamp, " +
                    "x/10000, " +
                    "case when x % 3 = 0 THEN NULL ELSE x END, " +
                    "'k' || (x%5) ::symbol, " +
                    "x*2::double " +
                    "from long_sequence(39999)");

            // Trigger removal of rows below lo boundary AND resize of buffer
            execute("insert into tab select " +
                    "(100000+x)::timestamp, " +
                    "(100000+x)%4, " +
                    "case when x % 3 = 0 THEN NULL ELSE 100000 + x END, " +
                    "'k' || (x%20) ::symbol, " +
                    "x*2::double " +
                    "from long_sequence(360000)");

            // Test min(double) with window function over partitioned data with large frame
            // Test the last row of each partition to verify boundary conditions
            assertSql(
                    """
                            i\tts\tmin_d_window
                            0\t1970-01-01T00:00:00.460000Z\t560000.0
                            1\t1970-01-01T00:00:00.459997Z\t559994.0
                            2\t1970-01-01T00:00:00.459998Z\t559996.0
                            3\t1970-01-01T00:00:00.459999Z\t559998.0
                            """,
                    "select i, ts, min_d_window from (" +
                            "select i, ts, " +
                            "min(d) over (partition by i order by ts range between 80000 preceding and current row) as min_d_window, " +
                            "row_number() over (partition by i order by ts desc) as rn " +
                            "from tab " +
                            "where i < 4" +
                            ") where rn = 1 " +
                            "order by i"
            );

            // Cross-check: aggregate query equivalent to the window function
            assertSql(
                    """
                            min_d\ti
                            560000.0\t0
                            559994.0\t1
                            559996.0\t2
                            559998.0\t3
                            """,
                    "select min(d) as min_d, i " +
                            "from ( " +
                            "  select data.d, data.i " +
                            "  from (select i, max(ts) as max from tab group by i) cnt " +
                            "  join tab data on cnt.i = data.i and data.ts >= (cnt.max - 80000) " +
                            "  order by data.i, ts " +
                            ") " +
                            "where i in (0,1,2,3) " +
                            "group by i " +
                            "order by i"
            );
        });
    }

    @Test
    public void testMinNonDesignatedTimestampWithManyNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, other_ts timestamp, val long, grp symbol) timestamp(ts)");

            execute("insert into tab select " +
                    "dateadd('s', x::int, '2021-01-01T00:00:00.000000Z')::timestamp as ts, " +
                    "case when x % 3 = 0 then null else dateadd('h', (x % 24)::int, '2021-01-01T00:00:00.000000Z') end as other_ts, " +
                    "x as val, " +
                    "case when x % 2 = 0 then 'A' else 'B' end as grp " +
                    "from long_sequence(10000)");

            assertQueryNoLeakCheck(
                    """
                            grp\tnon_null_count\tmin_other_ts
                            A\t3334\t2021-01-01T02:00:00.000000Z
                            B\t3333\t2021-01-01T01:00:00.000000Z
                            """,
                    "SELECT grp, " +
                            "count(other_ts) as non_null_count, " +
                            "min(other_ts) as min_other_ts " +
                            "FROM tab GROUP BY grp ORDER BY grp",
                    null,
                    true,
                    true
            );

            assertQueryNoLeakCheck(
                    """
                            grp\tmin_window_ts
                            A\t2021-01-01T02:00:00.000000Z
                            B\t2021-01-01T01:00:00.000000Z
                            """,
                    "SELECT DISTINCT grp, min_window_ts FROM (" +
                            "SELECT grp, min(other_ts) OVER (PARTITION BY grp) as min_window_ts FROM tab" +
                            ") ORDER BY grp",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testMinNonDesignatedTimestampWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, other_ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', '2021-01-01T12:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', null, 2, 'A')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', '2021-01-03T08:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', null, 4, 'A')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', '2021-01-05T16:00:00.000000Z', 5, 'A')");
            execute("insert into tab values ('2021-01-06T00:00:00.000000Z', null, 6, 'A')");

            assertQueryNoLeakCheck(
                    """
                            ts\tother_ts\tval\tgrp\tmin_other_ts
                            2021-01-01T00:00:00.000000Z\t2021-01-01T12:00:00.000000Z\t1\tA\t2021-01-01T12:00:00.000000Z
                            2021-01-02T00:00:00.000000Z\t\t2\tA\t2021-01-01T12:00:00.000000Z
                            2021-01-03T00:00:00.000000Z\t2021-01-03T08:00:00.000000Z\t3\tA\t2021-01-01T12:00:00.000000Z
                            2021-01-04T00:00:00.000000Z\t\t4\tA\t2021-01-01T12:00:00.000000Z
                            2021-01-05T00:00:00.000000Z\t2021-01-05T16:00:00.000000Z\t5\tA\t2021-01-01T12:00:00.000000Z
                            2021-01-06T00:00:00.000000Z\t\t6\tA\t2021-01-01T12:00:00.000000Z
                            """,
                    "SELECT ts, other_ts, val, grp, min(other_ts) OVER (PARTITION BY grp) as min_other_ts FROM tab",
                    "ts",
                    true,
                    true
            );

            assertQueryNoLeakCheck(
                    """
                            ts\tother_ts\tval\tgrp\tmin_other_ts_running
                            2021-01-01T00:00:00.000000Z\t2021-01-01T12:00:00.000000Z\t1\tA\t2021-01-01T12:00:00.000000Z
                            2021-01-02T00:00:00.000000Z\t\t2\tA\t2021-01-01T12:00:00.000000Z
                            2021-01-03T00:00:00.000000Z\t2021-01-03T08:00:00.000000Z\t3\tA\t2021-01-01T12:00:00.000000Z
                            2021-01-04T00:00:00.000000Z\t\t4\tA\t2021-01-01T12:00:00.000000Z
                            2021-01-05T00:00:00.000000Z\t2021-01-05T16:00:00.000000Z\t5\tA\t2021-01-01T12:00:00.000000Z
                            2021-01-06T00:00:00.000000Z\t\t6\tA\t2021-01-01T12:00:00.000000Z
                            """,
                    "SELECT ts, other_ts, val, grp, min(other_ts) OVER (PARTITION BY grp ORDER BY ts ROWS UNBOUNDED PRECEDING) as min_other_ts_running FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMinTimestampOnNonDesignatedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, other_ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', '2021-01-05T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', '2021-01-04T00:00:00.000000Z', 2, 'B')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', '2021-01-03T00:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', '2021-01-02T00:00:00.000000Z', 4, 'B')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', '2021-01-01T00:00:00.000000Z', 5, 'C')");

            assertQueryNoLeakCheck(
                    """
                            ts\tother_ts\tval\tgrp\tmin_other_ts
                            2021-01-01T00:00:00.000000Z\t2021-01-05T00:00:00.000000Z\t1\tA\t2021-01-01T00:00:00.000000Z
                            2021-01-02T00:00:00.000000Z\t2021-01-04T00:00:00.000000Z\t2\tB\t2021-01-01T00:00:00.000000Z
                            2021-01-03T00:00:00.000000Z\t2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-01T00:00:00.000000Z
                            2021-01-04T00:00:00.000000Z\t2021-01-02T00:00:00.000000Z\t4\tB\t2021-01-01T00:00:00.000000Z
                            2021-01-05T00:00:00.000000Z\t2021-01-01T00:00:00.000000Z\t5\tC\t2021-01-01T00:00:00.000000Z
                            """,
                    "SELECT ts, other_ts, val, grp, min(other_ts) OVER () as min_other_ts FROM tab",
                    "ts",
                    true,
                    true
            );

            assertQueryNoLeakCheck(
                    """
                            ts\tother_ts\tval\tgrp\tmin_other_ts_by_grp
                            2021-01-01T00:00:00.000000Z\t2021-01-05T00:00:00.000000Z\t1\tA\t2021-01-03T00:00:00.000000Z
                            2021-01-02T00:00:00.000000Z\t2021-01-04T00:00:00.000000Z\t2\tB\t2021-01-02T00:00:00.000000Z
                            2021-01-03T00:00:00.000000Z\t2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-03T00:00:00.000000Z
                            2021-01-04T00:00:00.000000Z\t2021-01-02T00:00:00.000000Z\t4\tB\t2021-01-02T00:00:00.000000Z
                            2021-01-05T00:00:00.000000Z\t2021-01-01T00:00:00.000000Z\t5\tC\t2021-01-01T00:00:00.000000Z
                            """,
                    "SELECT ts, other_ts, val, grp, min(other_ts) OVER (PARTITION BY grp) as min_other_ts_by_grp FROM tab",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testMinTimestampOverWholeResultSet() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 2, 'B')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 4, 'B')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 5, 'C')");

            assertQueryNoLeakCheck(
                    """
                            ts\tval\tgrp\tmin_ts
                            2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-01T00:00:00.000000Z
                            2021-01-02T00:00:00.000000Z\t2\tB\t2021-01-01T00:00:00.000000Z
                            2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-01T00:00:00.000000Z
                            2021-01-04T00:00:00.000000Z\t4\tB\t2021-01-01T00:00:00.000000Z
                            2021-01-05T00:00:00.000000Z\t5\tC\t2021-01-01T00:00:00.000000Z
                            """,
                    "SELECT ts, val, grp, min(ts) OVER () as min_ts FROM tab",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testMinTimestampRangeFrameBufferOverflow() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val long, grp symbol) timestamp(ts)");

            execute("insert into tab select " +
                    "dateadd('s', ((x-1) * 10)::int, '2021-01-01T00:00:00.000000Z'::timestamp) as ts, " +
                    "x as val, " +
                    "'grp_' || ((x-1) % 100) as grp " +
                    "from long_sequence(50000)");

            assertQueryNoLeakCheck(
                    """
                            cnt\tpartitions
                            50000\t100
                            """,
                    "SELECT count(*) as cnt, count(distinct grp)  as partitions FROM (" +
                            "SELECT ts, val, grp, " +
                            "min(ts) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN '1' HOUR PRECEDING AND CURRENT ROW) as min_ts_large_window " +
                            "FROM tab" +
                            ")",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testMinTimestampRangeFrameDequeOverflow() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val long, grp symbol) timestamp(ts)");

            execute("insert into tab select " +
                    "dateadd('s', x::int, '2021-01-01T00:00:00.000000Z')::timestamp as ts, " +
                    "x as val, " +
                    "case when x % 5 = 0 then 'A' else 'B' end as grp " +
                    "from long_sequence(50000)");

            assertQueryNoLeakCheck(
                    """
                            cnt\tmax_val
                            50000\t50000
                            """,
                    "SELECT count(*) as cnt, max(val) as max_val FROM (" +
                            "SELECT ts, val, grp, " +
                            "min(ts) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN '2' HOUR PRECEDING AND '1' HOUR PRECEDING) as min_ts_bounded " +
                            "FROM tab" +
                            ")",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testMinTimestampRangeFrameEdgeCases() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val long, grp symbol) timestamp(ts)");

            execute("insert into tab values " +
                    "('2021-01-01T12:00:00.000000Z', 1, 'A'), " +
                    "('2021-01-01T12:00:00.000000Z', 2, 'A'), " +
                    "('2021-01-01T12:00:01.000000Z', 3, 'A'), " +
                    "('2021-01-01T12:00:01.000000Z', 4, 'A'), " +
                    "('2021-01-01T12:00:02.000000Z', 5, 'A'), " +
                    "('2021-01-01T12:00:03.000000Z', 6, 'A'), " +
                    "('2021-01-01T12:00:04.000000Z', 7, 'A'), " +
                    "('2021-01-01T12:00:05.000000Z', 8, 'A')");

            assertQueryNoLeakCheck(
                    """
                            ts\tval\tmin_ts_precise
                            2021-01-01T12:00:00.000000Z\t1\t
                            2021-01-01T12:00:00.000000Z\t2\t
                            2021-01-01T12:00:01.000000Z\t3\t
                            2021-01-01T12:00:01.000000Z\t4\t
                            2021-01-01T12:00:02.000000Z\t5\t2021-01-01T12:00:00.000000Z
                            2021-01-01T12:00:03.000000Z\t6\t2021-01-01T12:00:00.000000Z
                            2021-01-01T12:00:04.000000Z\t7\t2021-01-01T12:00:00.000000Z
                            2021-01-01T12:00:05.000000Z\t8\t2021-01-01T12:00:01.000000Z
                            """,
                    "SELECT ts, val, min(ts) OVER (ORDER BY ts RANGE BETWEEN '4' SECOND preceding AND '2' SECOND preceding) as min_ts_precise FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMinTimestampRangeFrameWithManyPartitions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val long, grp symbol) timestamp(ts)");

            execute("insert into tab select " +
                    "dateadd('s', x::int, '2021-01-01T00:00:00.000000Z')::timestamp as ts, " +
                    "x as val, " +
                    "'grp_' || (x % 1000) as grp " +
                    "from long_sequence(25000)");

            assertQueryNoLeakCheck(
                    """
                            partition_count\ttotal_rows
                            1000\t25000
                            """,
                    "SELECT count(distinct grp) as partition_count, count(*) as total_rows FROM (" +
                            "SELECT ts, val, grp, " +
                            "min(ts) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN '30' MINUTE PRECEDING AND CURRENT ROW) as min_ts " +
                            "FROM tab" +
                            ")",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testMinTimestampRangeOnNonDesignatedTimestampFails() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, other_ts timestamp, val int) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', '2021-01-01T12:00:00.000000Z', 1)");

            assertExceptionNoLeakCheck(
                    "SELECT ts, other_ts, val, min(ts) OVER (ORDER BY other_ts RANGE BETWEEN '1' HOUR PRECEDING AND CURRENT ROW) FROM tab",
                    49,
                    "RANGE is supported only for queries ordered by designated timestamp"
            );
        });
    }

    @Test
    public void testMinTimestampRangeWithBoundedFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-01T01:00:00.000000Z', 2, 'A')");
            execute("insert into tab values ('2021-01-01T02:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-01T03:00:00.000000Z', 4, 'A')");
            execute("insert into tab values ('2021-01-01T04:00:00.000000Z', 5, 'A')");
            execute("insert into tab values ('2021-01-01T05:00:00.000000Z', 6, 'A')");

            assertQueryNoLeakCheck(
                    """
                            ts\tval\tgrp\tmin_ts_bounded
                            2021-01-01T00:00:00.000000Z\t1\tA\t
                            2021-01-01T01:00:00.000000Z\t2\tA\t2021-01-01T00:00:00.000000Z
                            2021-01-01T02:00:00.000000Z\t3\tA\t2021-01-01T00:00:00.000000Z
                            2021-01-01T03:00:00.000000Z\t4\tA\t2021-01-01T01:00:00.000000Z
                            2021-01-01T04:00:00.000000Z\t5\tA\t2021-01-01T02:00:00.000000Z
                            2021-01-01T05:00:00.000000Z\t6\tA\t2021-01-01T03:00:00.000000Z
                            """,
                    "SELECT ts, val, grp, min(ts) OVER (ORDER BY ts RANGE BETWEEN '2' HOUR PRECEDING AND '1' HOUR PRECEDING) as min_ts_bounded FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMinTimestampRangeWithPartitionAndSpecificBounds() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-01T06:00:00.000000Z', 2, 'B')");
            execute("insert into tab values ('2021-01-01T12:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-01T18:00:00.000000Z', 4, 'B')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 5, 'A')");
            execute("insert into tab values ('2021-01-02T06:00:00.000000Z', 6, 'B')");

            assertQueryNoLeakCheck(
                    """
                            ts\tval\tgrp\tmin_ts_by_grp_6h
                            2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-01T00:00:00.000000Z
                            2021-01-01T06:00:00.000000Z\t2\tB\t2021-01-01T06:00:00.000000Z
                            2021-01-01T12:00:00.000000Z\t3\tA\t2021-01-01T12:00:00.000000Z
                            2021-01-01T18:00:00.000000Z\t4\tB\t2021-01-01T18:00:00.000000Z
                            2021-01-02T00:00:00.000000Z\t5\tA\t2021-01-02T00:00:00.000000Z
                            2021-01-02T06:00:00.000000Z\t6\tB\t2021-01-02T06:00:00.000000Z
                            """,
                    "SELECT ts, val, grp, min(ts) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN '6' HOUR PRECEDING AND CURRENT ROW) as min_ts_by_grp_6h FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMinTimestampRowsCurrentRowOnly() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 2, 'A')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 4, 'A')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 5, 'A')");

            assertQueryNoLeakCheck(
                    """
                            ts\tval\tgrp\tmin_ts_current_only
                            2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-01T00:00:00.000000Z
                            2021-01-02T00:00:00.000000Z\t2\tA\t2021-01-02T00:00:00.000000Z
                            2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-03T00:00:00.000000Z
                            2021-01-04T00:00:00.000000Z\t4\tA\t2021-01-04T00:00:00.000000Z
                            2021-01-05T00:00:00.000000Z\t5\tA\t2021-01-05T00:00:00.000000Z
                            """,
                    "SELECT ts, val, grp, min(ts) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) as min_ts_current_only FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMinTimestampRowsUnboundedPrecedingToCurrentRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 2, 'B')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 4, 'B')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 5, 'A')");

            assertQueryNoLeakCheck(
                    """
                            ts\tval\tgrp\tmin_ts_unbounded_rows
                            2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-01T00:00:00.000000Z
                            2021-01-02T00:00:00.000000Z\t2\tB\t2021-01-02T00:00:00.000000Z
                            2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-01T00:00:00.000000Z
                            2021-01-04T00:00:00.000000Z\t4\tB\t2021-01-02T00:00:00.000000Z
                            2021-01-05T00:00:00.000000Z\t5\tA\t2021-01-01T00:00:00.000000Z
                            """,
                    "SELECT ts, val, grp, min(ts) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as min_ts_unbounded_rows FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMinTimestampRowsWholePartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 2, 'B')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 4, 'B')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 5, 'A')");

            assertQueryNoLeakCheck(
                    """
                            ts\tval\tgrp\tmin_ts_whole_partition
                            2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-01T00:00:00.000000Z
                            2021-01-02T00:00:00.000000Z\t2\tB\t2021-01-02T00:00:00.000000Z
                            2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-01T00:00:00.000000Z
                            2021-01-04T00:00:00.000000Z\t4\tB\t2021-01-02T00:00:00.000000Z
                            2021-01-05T00:00:00.000000Z\t5\tA\t2021-01-01T00:00:00.000000Z
                            """,
                    "SELECT ts, val, grp, min(ts) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as min_ts_whole_partition FROM tab",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testMinTimestampWithPartitionBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 2, 'B')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 4, 'B')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 5, 'C')");

            assertQueryNoLeakCheck(
                    """
                            ts\tval\tgrp\tmin_ts_by_grp
                            2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-01T00:00:00.000000Z
                            2021-01-02T00:00:00.000000Z\t2\tB\t2021-01-02T00:00:00.000000Z
                            2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-01T00:00:00.000000Z
                            2021-01-04T00:00:00.000000Z\t4\tB\t2021-01-02T00:00:00.000000Z
                            2021-01-05T00:00:00.000000Z\t5\tC\t2021-01-05T00:00:00.000000Z
                            """,
                    "SELECT ts, val, grp, min(ts) OVER (PARTITION BY grp) as min_ts_by_grp FROM tab",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testNegativeLimitWindowOrderedByNotTimestamp() throws Exception {
        // https://github.com/questdb/questdb/issues/4748
        assertMemoryLeak(() -> assertQuery("""
                        x\trow_number
                        1\t1
                        2\t2
                        3\t3
                        4\t4
                        5\t5
                        6\t6
                        7\t7
                        8\t8
                        9\t9
                        10\t10
                        """,
                """
                        SELECT x, row_number() OVER (
                            ORDER BY x asc
                            RANGE UNBOUNDED PRECEDING
                        )
                        FROM long_sequence(10)
                        limit -10""",
                true)
        );
    }

    @Test
    public void testNestedWindowFunctionArithmeticWithColumnAndAlias() throws Exception {
        // Test arithmetic of two nested window functions where one uses column and other uses alias.
        // SELECT x as a, sum(sum(x) OVER ()) OVER () + sum(sum(a) OVER ()) OVER () FROM x
        // a is an alias for column x.
        // sum(x) OVER () = 1+2+3 = 6 for each row
        // sum(6) OVER () = 18 for each row
        // sum(a) OVER () = sum(x) OVER () = 6 for each row
        // sum(6) OVER () = 18 for each row
        // 18 + 18 = 36 for each row
        assertQuery(
                """
                        a\tcolumn
                        1\t36.0
                        2\t36.0
                        3\t36.0
                        """,
                "SELECT x as a, sum(sum(x) OVER ()) OVER () + sum(sum(a) OVER ()) OVER () FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT x, timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(3)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                true,
                true
        );
    }

    @Test
    public void testNestedWindowFunctionInCaseWhenWithColumnAlias() throws Exception {
        // Test nested window function inside CASE WHEN where inner function references an output column alias.
        // SELECT x as a, CASE WHEN sum(sum(a) OVER ()) OVER () > 5 THEN 1 ELSE 0 END FROM x
        // a is an alias for column x, and sum(a) OVER () should resolve a to the original column x.
        // sum(x) OVER () = 1+2+3 = 6 for each row
        // sum(6) OVER () = 6+6+6 = 18 for each row
        // 18 > 5 is true, so CASE returns 1 for each row
        assertQuery(
                """
                        a\tcase
                        1\t1
                        2\t1
                        3\t1
                        """,
                "SELECT x as a, CASE WHEN sum(sum(a) OVER ()) OVER () > 5 THEN 1 ELSE 0 END FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT x, timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(3)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                true,
                true
        );
    }

    @Test
    public void testNestedWindowFunctionWithColumnAliasReference() throws Exception {
        // Test nested window function where inner function references an output column alias.
        // SELECT x as x0, sum(sum(x0) OVER ()) OVER () FROM x
        // x0 is an alias for column x, and sum(x0) OVER () should resolve x0 to the original column x.
        // sum(x) OVER () = 1+2+3 = 6 for each row
        // sum(6) OVER () = 6+6+6 = 18 for each row
        assertQuery(
                """
                        x0\tsum
                        1\t18.0
                        2\t18.0
                        3\t18.0
                        """,
                "SELECT x as x0, sum(sum(x0) OVER ()) OVER () as sum FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT x, timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(3)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                true,
                true
        );
    }

    @Test
    public void testNestedWindowFunctionWithTwoAliasesForSameColumn() throws Exception {
        // Test nested window function where two output aliases reference the same column.
        // SELECT x as a, x as b, sum(sum(a) OVER () + sum(b) OVER ()) OVER () FROM x
        // The table only has column x. Aliases 'a' and 'b' are output aliases for x.
        // The inner sum(a) and sum(b) should fail to resolve because a and b are not table columns.
        // The validation triggers for alias 'b' first because when there are multiple aliases
        // for the same column (x as a, x as b), the second alias 'b' references the first alias 'a'
        // in innerVirtualModel, and 'a' is not a table column.
        assertException(
                "SELECT x as a, x as b, sum(sum(a) OVER () + sum(b) OVER ()) OVER () FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT x, timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(3)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                48,
                "Invalid column: b"
        );
    }

    @Test
    public void testNestedWindowFunctionsIntermediateValues() throws Exception {
        // Diagnostic test to understand what values the intermediate expressions produce
        // Note: rank() OVER () without ORDER BY returns 1 for all rows (all ties)
        assertQuery(
                """
                        x\trn\trk\tinner_sum
                        1\t1\t1\t2
                        2\t2\t1\t3
                        3\t3\t1\t4
                        """,
                "SELECT x, row_number() OVER () as rn, rank() OVER () as rk, row_number() OVER () + rank() OVER () as inner_sum FROM t",
                "CREATE TABLE t AS (" +
                        "SELECT x, timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(3)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,  // supportsRandomAccess
                true   // expectSize
        );
    }

    @Test
    public void testNestedWindowFunctionsWithColumnAliases() throws Exception {
        // Test nested window functions where output column aliases conflict with table column names.
        // The query aliases column 'id' as 'a' and 'b', but the table also has columns 'a' and 'b'.
        // The inner window functions sum(a) OVER () and sum(b) OVER () should reference the
        // actual table columns 'a' and 'b', not the output aliases.
        // sum(a) OVER () = 10 + 20 + 30 = 60 for each row
        // sum(b) OVER () = 100 + 200 + 300 = 600 for each row
        // sum(60 + 600) OVER () = 660 * 3 = 1980 for each row
        assertQuery(
                """
                        a\tb\tsum
                        1\t1\t1980.0
                        2\t2\t1980.0
                        3\t3\t1980.0
                        """,
                "SELECT id as a, id as b, sum(sum(a) OVER () + sum(b) OVER ()) OVER () as sum FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT x AS id, " +
                        "  x * 10 AS a, " +
                        "  x * 100 AS b, " +
                        "  timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(3)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                true,
                true
        );
    }

    @Test
    public void testNestedWindowFunctionsWithOrderByColumnReference() throws Exception {
        // Test nested window functions where the outer window function's ORDER BY
        // references a column that must be propagated through multiple inner window models.
        // This tests that collectReferencedAliases properly traverses WindowExpression's orderBy.
        //
        // Query: SELECT x, sum(row_number() OVER () + rank() OVER ()) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t
        // - row_number() OVER () creates first inner window model
        // - rank() OVER () creates second inner window model
        // - sum(...) OVER (ORDER BY x ROWS...) is the outer window function
        // - x must be propagated through both inner window models
        //
        // row_number() OVER () = 1, 2, 3 for each row
        // rank() OVER () = 1, 1, 1 for each row (no ORDER BY, so all ties)
        // row_number() + rank() = 2, 3, 4 for each row
        // Running sum: 2, 2+3=5, 5+4=9
        assertQuery(
                """
                        x\tsum
                        1\t2.0
                        2\t5.0
                        3\t9.0
                        """,
                "SELECT x, sum(row_number() OVER () + rank() OVER ()) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as sum FROM t",
                "CREATE TABLE t AS (" +
                        "SELECT x, timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(3)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                true,  // supportsRandomAccess
                true   // expectSize
        );
    }

    @Test
    public void testNestedWindowFunctionsWithPartitionByColumnReference() throws Exception {
        // Test nested window functions where the outer window function's PARTITION BY
        // references a column that must be propagated through multiple inner window models.
        // This tests that collectReferencedAliases properly traverses WindowExpression's partitionBy.
        //
        // Query: SELECT x, sum(row_number() OVER () + rank() OVER ()) OVER (PARTITION BY x) FROM t
        // - row_number() OVER () creates first inner window model
        // - rank() OVER () creates second inner window model
        // - sum(...) OVER (PARTITION BY x) is the outer window function
        // - x must be propagated through both inner window models
        //
        // row_number() OVER () = 1, 2, 3 for each row
        // rank() OVER () = 1, 1, 1 for each row (no ORDER BY, so all ties)
        // row_number() + rank() = 2, 3, 4 for each row
        // Since each row has unique x (1, 2, 3), each partition has one row
        // sum(2) for x=1, sum(3) for x=2, sum(4) for x=3
        assertQuery(
                """
                        x\tsum
                        1\t2.0
                        2\t3.0
                        3\t4.0
                        """,
                "SELECT x, sum(row_number() OVER () + rank() OVER ()) OVER (PARTITION BY x) as sum FROM t",
                "CREATE TABLE t AS (" +
                        "SELECT x, timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(3)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                true,  // supportsRandomAccess
                true   // expectSize
        );
    }

    @Test
    public void testPartitionByAndOrderByColumnPushdown() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, j long, d double, s symbol, c VARCHAR) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab select x::timestamp, x/4, x%5, x*2::double, 'k' || (x%5) ::symbol, 'k' || x from long_sequence(7)");

            // row_number()
            assertQueryNoLeakCheck(
                    """
                            row_number
                            3
                            2
                            1
                            4
                            3
                            2
                            1
                            """,
                    "select row_number() over (partition by i order by ts desc) " +
                            "from tab " +
                            "order by ts asc",
                    null,
                    true, // cached window factory
                    true
            );

            assertQueryNoLeakCheck(
                    """
                            row_number
                            1
                            2
                            3
                            4
                            1
                            2
                            3
                            """,
                    "select row_number() over (partition by i order by ts desc)" +
                            "from tab " +
                            "order by ts desc",
                    null,
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    """
                            row_number
                            1
                            2
                            3
                            1
                            2
                            3
                            4
                            """,
                    "select row_number() over (partition by i order by ts asc) " +
                            "from tab " +
                            "order by ts asc",
                    null,
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    """
                            row_number
                            4
                            3
                            2
                            1
                            3
                            2
                            1
                            """,
                    "select row_number() over (partition by i order by ts asc) " +
                            "from tab " +
                            "order by ts desc",
                    null,
                    true, // cached window factory
                    true
            );

            assertQueryNoLeakCheck(
                    """
                            row_number
                            3
                            2
                            1
                            4
                            3
                            2
                            1
                            """,
                    "select row_number() over (partition by i order by i, j asc) " +
                            "from tab " +
                            "order by ts desc",
                    null,
                    true, // cached window factory
                    true
            );

            assertPlanNoLeakCheck(
                    "select row_number() over (partition by i order by ts asc), " +
                            "   avg(j) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   sum(j) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   first_value(j) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   first_value(j) ignore nulls over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   last_value(j) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   last_value(j) ignore nulls over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   count(*) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   count(j) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   count(s) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   count(d) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   count(c) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   max(j) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   min(j) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   rank() over (partition by i order by j asc), " +
                            "   dense_rank() over (partition by i order by j asc), " +
                            "   lag(j) over (partition by i order by j asc), " +
                            "   lead(j) over (partition by i order by j asc), " +
                            "   lag(j) ignore nulls over (partition by i order by j asc), " +
                            "   lead(j) ignore nulls over (partition by i order by j asc) " +
                            "from tab " +
                            "order by ts asc",
                    """
                            SelectedRecord
                                CachedWindow
                                  orderedFunctions: [[j desc] => [lead(j, 1, NULL) over (partition by [i]),lead(j, 1, NULL) ignore nulls over (partition by [i])],[ts desc] => [avg(j) over (partition by [i] rows between unbounded preceding and current row),sum(j) over (partition by [i] rows between unbounded preceding and current row),first_value(j) over (partition by [i] rows between unbounded preceding and current row),first_value(j) ignore nulls over (partition by [i] rows between unbounded preceding and current row),last_value(j) over (partition by [i] rows between unbounded preceding and current row),last_value(j) ignore nulls over (partition by [i] rows between unbounded preceding and current row),count(*) over (partition by [i] rows between unbounded preceding and current row),count(j) over (partition by [i] rows between unbounded preceding and current row),count(s) over (partition by [i] rows between unbounded preceding and current row),count(d) over (partition by [i] rows between unbounded preceding and current row),count(c) over (partition by [i] rows between unbounded preceding and current row),max(j) over (partition by [i] rows between unbounded preceding and current row),min(j) over (partition by [i] rows between unbounded preceding and current row)],[j] => [rank() over (partition by [i]),dense_rank() over (partition by [i]),lag(j, 1, NULL) over (partition by [i]),lag(j, 1, NULL) ignore nulls over (partition by [i])]]
                                  unorderedFunctions: [row_number() over (partition by [i])]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tab
                            """
            );

            assertQueryNoLeakCheck(
                    """
                            row_number\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\trank\tdense_rank\tlag\tlead\tlag_ignore_nulls\tlead_ignore_nulls
                            1\t2.0\t6.0\t3\t3\t1\t1\t3\t3\t3\t3\t3\t3\t1\t1\t1\tnull\t2\tnull\t2
                            2\t2.5\t5.0\t3\t3\t2\t2\t2\t2\t2\t2\t2\t3\t2\t2\t2\t1\t3\t1\t3
                            3\t3.0\t3.0\t3\t3\t3\t3\t1\t1\t1\t1\t1\t3\t3\t3\t3\t2\tnull\t2\tnull
                            1\t1.75\t7.0\t2\t2\t4\t4\t4\t4\t4\t4\t4\t4\t0\t4\t4\t2\tnull\t2\tnull
                            2\t1.0\t3.0\t2\t2\t0\t0\t3\t3\t3\t3\t3\t2\t0\t1\t1\tnull\t1\tnull\t1
                            3\t1.5\t3.0\t2\t2\t1\t1\t2\t2\t2\t2\t2\t2\t1\t2\t2\t0\t2\t0\t2
                            4\t2.0\t2.0\t2\t2\t2\t2\t1\t1\t1\t1\t1\t2\t2\t3\t3\t1\t4\t1\t4
                            """,
                    "select row_number() over (partition by i order by ts asc), " +
                            "   avg(j) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   sum(j) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   first_value(j) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   first_value(j) ignore nulls over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   last_value(j) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   last_value(j) ignore nulls over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   count(*) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   count(j) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   count(s) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   count(d) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   count(c) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   max(j) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   min(j) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   rank() over (partition by i order by j asc), " +
                            "   dense_rank() over (partition by i order by j asc), " +
                            "   lag(j) over (partition by i order by j asc), " +
                            "   lead(j) over (partition by i order by j asc), " +
                            "   lag(j) ignore nulls over (partition by i order by j asc), " +
                            "   lead(j) ignore nulls over (partition by i order by j asc) " +
                            "from tab " +
                            "order by ts asc",
                    null,
                    true, // cached window factory
                    true
            );

            assertQueryNoLeakCheck(
                    """
                            row_number\tavg\tsum\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\trank\tdense_rank\tlag\tlead\tlag_ignore_nulls\tlead_ignore_nulls
                            4\t2.0\t2.0\t2\t2\t2\t2\t1\t1\t1\t1\t1\t2\t2\t3\t3\t1\t4\t1\t4
                            3\t1.5\t3.0\t2\t2\t1\t1\t2\t2\t2\t2\t2\t2\t1\t2\t2\t0\t2\t0\t2
                            2\t1.0\t3.0\t2\t2\t0\t0\t3\t3\t3\t3\t3\t2\t0\t1\t1\tnull\t1\tnull\t1
                            1\t1.75\t7.0\t2\t2\t4\t4\t4\t4\t4\t4\t4\t4\t0\t4\t4\t2\tnull\t2\tnull
                            3\t3.0\t3.0\t3\t3\t3\t3\t1\t1\t1\t1\t1\t3\t3\t3\t3\t2\tnull\t2\tnull
                            2\t2.5\t5.0\t3\t3\t2\t2\t2\t2\t2\t2\t2\t3\t2\t2\t2\t1\t3\t1\t3
                            1\t2.0\t6.0\t3\t3\t1\t1\t3\t3\t3\t3\t3\t3\t1\t1\t1\tnull\t2\tnull\t2
                            """,
                    "select row_number() over (partition by i order by ts asc), " +
                            "   avg(j) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   sum(j) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   first_value(j) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   first_value(j) ignore nulls over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   last_value(j) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   last_value(j) ignore nulls over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   count(*) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   count(j) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   count(s) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   count(d) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   count(c) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   max(j) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   min(j) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   rank() over (partition by i order by j asc), " +
                            "   dense_rank() over (partition by i order by j asc), " +
                            "   lag(j) over (partition by i order by j asc), " +
                            "   lead(j) over (partition by i order by j asc), " +
                            "   lag(j) ignore nulls over (partition by i order by j asc), " +
                            "   lead(j) ignore nulls over (partition by i order by j asc) " +
                            "from tab " +
                            "order by ts desc",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testPartitionByWithMultiArgFunction() throws Exception {
        // Regression test for https://github.com/questdb/questdb/issues/6695
        // Window function type inference regression with multi-arg functions in PARTITION BY
        // Using split_part with 3 args - without the fix, this fails with:
        // "there is no matching function `split_part` with the argument types: (INT, CHAR, VARCHAR)"
        assertMemoryLeak(() -> {
            execute("create table t (sym varchar, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values ('A-1', '2024-01-01T00:00:00.000000Z')");
            execute("insert into t values ('A-2', '2024-01-01T00:00:01.000000Z')");
            execute("insert into t values ('B-1', '2024-01-01T00:00:02.000000Z')");

            assertQueryNoLeakCheck(
                    """
                            sym\tmax
                            A-1\t2024-01-01T00:00:01.000000Z
                            A-2\t2024-01-01T00:00:01.000000Z
                            B-1\t2024-01-01T00:00:02.000000Z
                            """,
                    "SELECT sym, max(ts) OVER (PARTITION BY split_part(sym, '-', 1)) as max FROM t",
                    null,
                    true,
                    true
            );
            assertQueryNoLeakCheck(
                    """
                            sym\tmax
                            A-1\t2024-01-01T00:00:01.000000Z
                            A-2\t2024-01-01T00:00:01.000000Z
                            B-1\t2024-01-01T00:00:02.000000Z
                            """,
                    "SELECT sym, max(ts) OVER (PARTITION BY split_part(sym, '-', 1), substring(sym, 1, 1)) as max FROM t",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testRankFunction() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, s symbol) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab select (x/4)::timestamp, x/2, 'k' || (x%2) ::symbol from long_sequence(12)");

            // rank()/dense_rank() over(partition by)
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\trank\tdense_rank
                            1970-01-01T00:00:00.000000Z\t1\t1
                            1970-01-01T00:00:00.000000Z\t1\t1
                            1970-01-01T00:00:00.000000Z\t1\t1
                            1970-01-01T00:00:00.000001Z\t1\t1
                            1970-01-01T00:00:00.000001Z\t1\t1
                            1970-01-01T00:00:00.000001Z\t1\t1
                            1970-01-01T00:00:00.000001Z\t1\t1
                            1970-01-01T00:00:00.000002Z\t1\t1
                            1970-01-01T00:00:00.000002Z\t1\t1
                            1970-01-01T00:00:00.000002Z\t1\t1
                            1970-01-01T00:00:00.000002Z\t1\t1
                            1970-01-01T00:00:00.000003Z\t1\t1
                            """),
                    "select ts, " +
                            "rank() over (partition by s), " +
                            "dense_rank() over (partition by s) " +
                            "from tab ",
                    "ts",
                    false,
                    true
            );

            // rank()/dense_rank() over()
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\trank\tdense_rank
                            1970-01-01T00:00:00.000000Z\t1\t1
                            1970-01-01T00:00:00.000000Z\t1\t1
                            1970-01-01T00:00:00.000000Z\t1\t1
                            1970-01-01T00:00:00.000001Z\t1\t1
                            1970-01-01T00:00:00.000001Z\t1\t1
                            1970-01-01T00:00:00.000001Z\t1\t1
                            1970-01-01T00:00:00.000001Z\t1\t1
                            1970-01-01T00:00:00.000002Z\t1\t1
                            1970-01-01T00:00:00.000002Z\t1\t1
                            1970-01-01T00:00:00.000002Z\t1\t1
                            1970-01-01T00:00:00.000002Z\t1\t1
                            1970-01-01T00:00:00.000003Z\t1\t1
                            """),
                    "select ts, " +
                            "rank() over (), " +
                            "dense_rank() over () " +
                            "from tab ",
                    "ts",
                    false,
                    true
            );

            // rank()/dense_rank() over(partition by xxx order by xxx)
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ts\trank\tdense_rank
                            1970-01-01T00:00:00.000000Z\tk1\t1\t1
                            1970-01-01T00:00:00.000000Z\tk0\t1\t1
                            1970-01-01T00:00:00.000000Z\tk1\t1\t1
                            1970-01-01T00:00:00.000001Z\tk0\t2\t2
                            1970-01-01T00:00:00.000001Z\tk1\t3\t2
                            1970-01-01T00:00:00.000001Z\tk0\t2\t2
                            1970-01-01T00:00:00.000001Z\tk1\t3\t2
                            1970-01-01T00:00:00.000002Z\tk0\t4\t3
                            1970-01-01T00:00:00.000002Z\tk1\t5\t3
                            1970-01-01T00:00:00.000002Z\tk0\t4\t3
                            1970-01-01T00:00:00.000002Z\tk1\t5\t3
                            1970-01-01T00:00:00.000003Z\tk0\t6\t4
                            """),
                    "select ts, s," +
                            "rank() over (partition by s order by ts), " +
                            "dense_rank() over (partition by s order by ts) " +
                            "from tab ",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ts\trank\tdense_rank
                            1970-01-01T00:00:00.000000Z\tk0\t1\t1
                            1970-01-01T00:00:00.000001Z\tk0\t2\t2
                            1970-01-01T00:00:00.000001Z\tk0\t2\t2
                            1970-01-01T00:00:00.000002Z\tk0\t4\t3
                            1970-01-01T00:00:00.000002Z\tk0\t4\t3
                            1970-01-01T00:00:00.000003Z\tk0\t6\t4
                            1970-01-01T00:00:00.000000Z\tk1\t1\t1
                            1970-01-01T00:00:00.000000Z\tk1\t1\t1
                            1970-01-01T00:00:00.000001Z\tk1\t3\t2
                            1970-01-01T00:00:00.000001Z\tk1\t3\t2
                            1970-01-01T00:00:00.000002Z\tk1\t5\t3
                            1970-01-01T00:00:00.000002Z\tk1\t5\t3
                            """),
                    "select ts, s," +
                            "rank() over (partition by s order by ts), " +
                            "dense_rank() over (partition by s order by ts) " +
                            "from tab order by s, ts",
                    "",
                    true,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ts\trank\trank1\tdense_rank\tdense_rank1
                            1970-01-01T00:00:00.000000Z\tk0\t1\t6\t1\t4
                            1970-01-01T00:00:00.000001Z\tk0\t2\t4\t2\t3
                            1970-01-01T00:00:00.000001Z\tk0\t2\t4\t2\t3
                            1970-01-01T00:00:00.000002Z\tk0\t4\t2\t3\t2
                            1970-01-01T00:00:00.000002Z\tk0\t4\t2\t3\t2
                            1970-01-01T00:00:00.000003Z\tk0\t6\t1\t4\t1
                            1970-01-01T00:00:00.000000Z\tk1\t1\t5\t1\t3
                            1970-01-01T00:00:00.000000Z\tk1\t1\t5\t1\t3
                            1970-01-01T00:00:00.000001Z\tk1\t3\t3\t2\t2
                            1970-01-01T00:00:00.000001Z\tk1\t3\t3\t2\t2
                            1970-01-01T00:00:00.000002Z\tk1\t5\t1\t3\t1
                            1970-01-01T00:00:00.000002Z\tk1\t5\t1\t3\t1
                            """),
                    "select ts, s," +
                            "rank() over (partition by s order by ts), " +
                            "rank() over (partition by s order by ts desc), " +
                            "dense_rank() over (partition by s order by ts), " +
                            "dense_rank() over (partition by s order by ts desc) " +
                            "from tab order by s, ts",
                    "",
                    true,
                    true
            );

            // rank()/dense_rank() over(order by xxx)
            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\trank\tdense_rank
                            1970-01-01T00:00:00.000000Z\t1\t1
                            1970-01-01T00:00:00.000000Z\t1\t1
                            1970-01-01T00:00:00.000000Z\t1\t1
                            1970-01-01T00:00:00.000001Z\t4\t2
                            1970-01-01T00:00:00.000001Z\t4\t2
                            1970-01-01T00:00:00.000001Z\t4\t2
                            1970-01-01T00:00:00.000001Z\t4\t2
                            1970-01-01T00:00:00.000002Z\t8\t3
                            1970-01-01T00:00:00.000002Z\t8\t3
                            1970-01-01T00:00:00.000002Z\t8\t3
                            1970-01-01T00:00:00.000002Z\t8\t3
                            1970-01-01T00:00:00.000003Z\t12\t4
                            """),
                    "select ts," +
                            "rank() over (order by ts), " +
                            "dense_rank() over (order by ts) " +
                            "from tab order by ts",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ts\trank\tdense_rank
                            1970-01-01T00:00:00.000000Z\tk0\t1\t1
                            1970-01-01T00:00:00.000001Z\tk0\t4\t2
                            1970-01-01T00:00:00.000001Z\tk0\t4\t2
                            1970-01-01T00:00:00.000002Z\tk0\t8\t3
                            1970-01-01T00:00:00.000002Z\tk0\t8\t3
                            1970-01-01T00:00:00.000003Z\tk0\t12\t4
                            1970-01-01T00:00:00.000000Z\tk1\t1\t1
                            1970-01-01T00:00:00.000000Z\tk1\t1\t1
                            1970-01-01T00:00:00.000001Z\tk1\t4\t2
                            1970-01-01T00:00:00.000001Z\tk1\t4\t2
                            1970-01-01T00:00:00.000002Z\tk1\t8\t3
                            1970-01-01T00:00:00.000002Z\tk1\t8\t3
                            """),
                    "select ts, s," +
                            "rank() over (order by ts), " +
                            "dense_rank() over (order by ts) " +
                            "from tab order by s",
                    "",
                    true,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\trank\tdense_rank
                            1970-01-01T00:00:00.000000Z\t1\t1
                            1970-01-01T00:00:00.000000Z\t1\t1
                            1970-01-01T00:00:00.000000Z\t1\t1
                            1970-01-01T00:00:00.000001Z\t4\t2
                            1970-01-01T00:00:00.000001Z\t4\t2
                            1970-01-01T00:00:00.000001Z\t4\t2
                            1970-01-01T00:00:00.000001Z\t4\t2
                            1970-01-01T00:00:00.000002Z\t8\t3
                            1970-01-01T00:00:00.000002Z\t8\t3
                            1970-01-01T00:00:00.000002Z\t8\t3
                            1970-01-01T00:00:00.000002Z\t8\t3
                            1970-01-01T00:00:00.000003Z\t12\t4
                            """),
                    "select ts," +
                            "rank() over (order by ts), " +
                            "dense_rank() over (order by ts) " +
                            "from tab",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\trank\tdense_rank\trank1\tdense_rank1
                            1970-01-01T00:00:00.000000Z\t1\t1\t10\t4
                            1970-01-01T00:00:00.000000Z\t1\t1\t10\t4
                            1970-01-01T00:00:00.000000Z\t1\t1\t10\t4
                            1970-01-01T00:00:00.000001Z\t4\t2\t6\t3
                            1970-01-01T00:00:00.000001Z\t4\t2\t6\t3
                            1970-01-01T00:00:00.000001Z\t4\t2\t6\t3
                            1970-01-01T00:00:00.000001Z\t4\t2\t6\t3
                            1970-01-01T00:00:00.000002Z\t8\t3\t2\t2
                            1970-01-01T00:00:00.000002Z\t8\t3\t2\t2
                            1970-01-01T00:00:00.000002Z\t8\t3\t2\t2
                            1970-01-01T00:00:00.000002Z\t8\t3\t2\t2
                            1970-01-01T00:00:00.000003Z\t12\t4\t1\t1
                            """),
                    "select ts," +
                            "rank() over (order by ts), " +
                            "dense_rank() over (order by ts), " +
                            "rank() over (order by ts desc), " +
                            "dense_rank() over (order by ts desc) " +
                            "from tab",
                    "ts",
                    true,
                    true
            );

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix("""
                            ts\ts\trank\tdense_rank
                            1970-01-01T00:00:00.000000Z\tk0\t6\t4
                            1970-01-01T00:00:00.000001Z\tk0\t4\t3
                            1970-01-01T00:00:00.000001Z\tk0\t4\t3
                            1970-01-01T00:00:00.000002Z\tk0\t2\t2
                            1970-01-01T00:00:00.000002Z\tk0\t2\t2
                            1970-01-01T00:00:00.000003Z\tk0\t1\t1
                            1970-01-01T00:00:00.000000Z\tk1\t5\t3
                            1970-01-01T00:00:00.000000Z\tk1\t5\t3
                            1970-01-01T00:00:00.000001Z\tk1\t3\t2
                            1970-01-01T00:00:00.000001Z\tk1\t3\t2
                            1970-01-01T00:00:00.000002Z\tk1\t1\t1
                            1970-01-01T00:00:00.000002Z\tk1\t1\t1
                            """),
                    "select ts, s," +
                            "rank() over (partition by concat(s,'foobar') order by ts desc)," +
                            "dense_rank() over (partition by concat(s,'foobar') order by ts desc)" +
                            "from tab order by s, ts",
                    "",
                    true,
                    true
            );
        });
    }

    @Test
    public void testRankWithNoPartitionByAndNoOrderByWildcardLast() throws Exception {
        assertQuery(
                replaceTimestampSuffix("""
                        rank\tdense_rank\tprice\tsymbol\tts
                        1\t1\t1\tBB\t1970-01-01T00:00:00.000000Z
                        1\t1\t2\tCC\t1970-01-02T03:46:40.000000Z
                        1\t1\t2\tAA\t1970-01-03T07:33:20.000000Z
                        1\t1\t1\tCC\t1970-01-04T11:20:00.000000Z
                        1\t1\t2\tBB\t1970-01-05T15:06:40.000000Z
                        1\t1\t1\tBB\t1970-01-06T18:53:20.000000Z
                        1\t1\t1\tBB\t1970-01-07T22:40:00.000000Z
                        1\t1\t1\tCC\t1970-01-09T02:26:40.000000Z
                        1\t1\t1\tCC\t1970-01-10T06:13:20.000000Z
                        1\t1\t2\tAA\t1970-01-11T10:00:00.000000Z
                        """),
                "select rank() over (), dense_rank() over (), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_int(1,2,3) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        (timestampType == TestTimestampType.MICRO ? " timestamp_sequence(0, 100000000000) ts" : " timestamp_sequence_ns(0, 100000000000000) ts") +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                "ts",
                false,
                true
        );
    }

    @Test
    public void testRankWithNoPartitionByAndOrderBySymbolWildcardLast() throws Exception {
        assertQuery(
                replaceTimestampSuffix("""
                        rank\tdense_rank\tprice\tsymbol\tts
                        3\t2\t1\tBB\t1970-01-01T00:00:00.000000Z
                        7\t3\t2\tCC\t1970-01-02T03:46:40.000000Z
                        1\t1\t2\tAA\t1970-01-03T07:33:20.000000Z
                        7\t3\t1\tCC\t1970-01-04T11:20:00.000000Z
                        3\t2\t2\tBB\t1970-01-05T15:06:40.000000Z
                        3\t2\t1\tBB\t1970-01-06T18:53:20.000000Z
                        3\t2\t1\tBB\t1970-01-07T22:40:00.000000Z
                        7\t3\t1\tCC\t1970-01-09T02:26:40.000000Z
                        7\t3\t1\tCC\t1970-01-10T06:13:20.000000Z
                        1\t1\t2\tAA\t1970-01-11T10:00:00.000000Z
                        """),
                "select rank() over (order by symbol), dense_rank() over (order by symbol), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_int(1,2,3) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        (timestampType == TestTimestampType.MICRO ? " timestamp_sequence(0, 100000000000) ts" : " timestamp_sequence_ns(0, 100000000000000) ts") +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                "ts",
                true,
                true
        );
    }

    @Test
    public void testRankWithPartitionAndOrderByNonSymbol() throws Exception {
        assertQuery(
                replaceTimestampSuffix("""
                        rank\tdense_rank\tprice\tts
                        1\t1\t42\t1970-01-01T00:00:00.000000Z
                        2\t2\t42\t1970-01-02T03:46:40.000000Z
                        3\t3\t42\t1970-01-03T07:33:20.000000Z
                        4\t4\t42\t1970-01-04T11:20:00.000000Z
                        5\t5\t42\t1970-01-05T15:06:40.000000Z
                        6\t6\t42\t1970-01-06T18:53:20.000000Z
                        7\t7\t42\t1970-01-07T22:40:00.000000Z
                        8\t8\t42\t1970-01-09T02:26:40.000000Z
                        9\t9\t42\t1970-01-10T06:13:20.000000Z
                        10\t10\t42\t1970-01-11T10:00:00.000000Z
                        """),
                "select rank() over (partition by price order by ts), dense_rank() over (partition by price order by ts), price, ts from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " 42 price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        (timestampType == TestTimestampType.MICRO ? " timestamp_sequence(0, 100000000000) ts" : " timestamp_sequence_ns(0, 100000000000000) ts") +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                "ts",
                false,
                true
        );
    }

    @Test
    public void testRankWithPartitionAndOrderBySymbolNoWildcard() throws Exception {
        assertQuery(
                """
                        rank\tdense_rank
                        1\t1
                        1\t1
                        1\t1
                        1\t1
                        1\t1
                        1\t1
                        1\t1
                        1\t1
                        1\t1
                        1\t1
                        """,
                "select rank() over (partition by symbol order by symbol), dense_rank() over (partition by symbol order by symbol) from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(42) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        (timestampType == TestTimestampType.MICRO ? " timestamp_sequence(0, 100000000000) ts" : " timestamp_sequence_ns(0, 100000000000000) ts") +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                null,
                true,
                true
        );
    }

    @Test
    public void testRankWithPartitionAndOrderBySymbolWildcardFirst() throws Exception {
        assertQuery(
                replaceTimestampSuffix("""
                        price\tsymbol\tts\trank\tdense_rank
                        0.8043224099968393\tCC\t1970-01-01T00:00:00.000000Z\t1\t1
                        0.2845577791213847\tBB\t1970-01-02T03:46:40.000000Z\t1\t1
                        0.9344604857394011\tCC\t1970-01-03T07:33:20.000000Z\t1\t1
                        0.7905675319675964\tAA\t1970-01-04T11:20:00.000000Z\t1\t1
                        0.8899286912289663\tBB\t1970-01-05T15:06:40.000000Z\t1\t1
                        0.11427984775756228\tCC\t1970-01-06T18:53:20.000000Z\t1\t1
                        0.4217768841969397\tBB\t1970-01-07T22:40:00.000000Z\t1\t1
                        0.7261136209823622\tBB\t1970-01-09T02:26:40.000000Z\t1\t1
                        0.6693837147631712\tBB\t1970-01-10T06:13:20.000000Z\t1\t1
                        0.8756771741121929\tBB\t1970-01-11T10:00:00.000000Z\t1\t1
                        """),
                "select *, rank() over (partition by symbol order by symbol), dense_rank() over (partition by symbol order by symbol) from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(42) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        (timestampType == TestTimestampType.MICRO ? " timestamp_sequence(0, 100000000000) ts" : " timestamp_sequence_ns(0, 100000000000000) ts") +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                "ts",
                true,
                true
        );
    }

    @Test
    public void testRankWithPartitionAndOrderBySymbolWildcardLast() throws Exception {
        assertQuery(
                replaceTimestampSuffix("""
                        rank\tdense_rank\tprice\tsymbol\tts
                        1\t1\t0.8043224099968393\tCC\t1970-01-01T00:00:00.000000Z
                        1\t1\t0.2845577791213847\tBB\t1970-01-02T03:46:40.000000Z
                        1\t1\t0.9344604857394011\tCC\t1970-01-03T07:33:20.000000Z
                        1\t1\t0.7905675319675964\tAA\t1970-01-04T11:20:00.000000Z
                        1\t1\t0.8899286912289663\tBB\t1970-01-05T15:06:40.000000Z
                        1\t1\t0.11427984775756228\tCC\t1970-01-06T18:53:20.000000Z
                        1\t1\t0.4217768841969397\tBB\t1970-01-07T22:40:00.000000Z
                        1\t1\t0.7261136209823622\tBB\t1970-01-09T02:26:40.000000Z
                        1\t1\t0.6693837147631712\tBB\t1970-01-10T06:13:20.000000Z
                        1\t1\t0.8756771741121929\tBB\t1970-01-11T10:00:00.000000Z
                        """),
                "select rank() over (partition by symbol order by symbol), dense_rank() over (partition by symbol order by symbol), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(42) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        (timestampType == TestTimestampType.MICRO ? " timestamp_sequence(0, 100000000000) ts" : " timestamp_sequence_ns(0, 100000000000000) ts") +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                "ts",
                true,
                true
        );
    }

    @Test
    public void testRankWithPartitionBySymbolAndMultiOrderWildcardLast() throws Exception {
        assertQuery(
                replaceTimestampSuffix("""
                        rank\tdense_rank\tprice\tsymbol\tts
                        1\t1\t1\tBB\t1970-01-01T00:00:00.000000Z
                        4\t2\t2\tCC\t1970-01-02T03:46:40.000000Z
                        1\t1\t2\tAA\t1970-01-03T07:33:20.000000Z
                        1\t1\t1\tCC\t1970-01-04T11:20:00.000000Z
                        4\t2\t2\tBB\t1970-01-05T15:06:40.000000Z
                        1\t1\t1\tBB\t1970-01-06T18:53:20.000000Z
                        1\t1\t1\tBB\t1970-01-07T22:40:00.000000Z
                        1\t1\t1\tCC\t1970-01-09T02:26:40.000000Z
                        1\t1\t1\tCC\t1970-01-10T06:13:20.000000Z
                        1\t1\t2\tAA\t1970-01-11T10:00:00.000000Z
                        """),
                "select rank() over (partition by symbol order by symbol, price), dense_rank() over (partition by symbol order by symbol, price),  * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_int(1,2,3) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        (timestampType == TestTimestampType.MICRO ? " timestamp_sequence(0, 100000000000) ts" : " timestamp_sequence_ns(0, 100000000000000) ts") +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                "ts",
                true,
                true
        );
    }

    @Test
    public void testRankWithPartitionBySymbolAndNoOrderWildcardLast() throws Exception {
        assertQuery(
                replaceTimestampSuffix("""
                        rank\tdense_rank\tprice\tsymbol\tts
                        1\t1\t1\tBB\t1970-01-01T00:00:00.000000Z
                        1\t1\t2\tCC\t1970-01-02T03:46:40.000000Z
                        1\t1\t2\tAA\t1970-01-03T07:33:20.000000Z
                        1\t1\t1\tCC\t1970-01-04T11:20:00.000000Z
                        1\t1\t2\tBB\t1970-01-05T15:06:40.000000Z
                        1\t1\t1\tBB\t1970-01-06T18:53:20.000000Z
                        1\t1\t1\tBB\t1970-01-07T22:40:00.000000Z
                        1\t1\t1\tCC\t1970-01-09T02:26:40.000000Z
                        1\t1\t1\tCC\t1970-01-10T06:13:20.000000Z
                        1\t1\t2\tAA\t1970-01-11T10:00:00.000000Z
                        """),
                "select rank() over (partition by symbol), dense_rank() over (partition by symbol), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_int(1,2,3) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        (timestampType == TestTimestampType.MICRO ? " timestamp_sequence(0, 100000000000) ts" : " timestamp_sequence_ns(0, 100000000000000) ts") +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                "ts",
                false,
                true
        );
    }

    @Test
    public void testRankWithPartitionBySymbolAndOrderByIntPriceDescWildcardLast() throws Exception {
        assertQuery(
                replaceTimestampSuffix("""
                        rank\tdense_rank\tprice\tsymbol\tts
                        2\t2\t1\tBB\t1970-01-01T00:00:00.000000Z
                        1\t1\t2\tCC\t1970-01-02T03:46:40.000000Z
                        1\t1\t2\tAA\t1970-01-03T07:33:20.000000Z
                        2\t2\t1\tCC\t1970-01-04T11:20:00.000000Z
                        1\t1\t2\tBB\t1970-01-05T15:06:40.000000Z
                        2\t2\t1\tBB\t1970-01-06T18:53:20.000000Z
                        2\t2\t1\tBB\t1970-01-07T22:40:00.000000Z
                        2\t2\t1\tCC\t1970-01-09T02:26:40.000000Z
                        2\t2\t1\tCC\t1970-01-10T06:13:20.000000Z
                        1\t1\t2\tAA\t1970-01-11T10:00:00.000000Z
                        """),
                "select rank() over (partition by symbol order by price desc), dense_rank() over (partition by symbol order by price desc), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_int(1,2,3) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        (timestampType == TestTimestampType.MICRO ? " timestamp_sequence(0, 100000000000) ts" : " timestamp_sequence_ns(0, 100000000000000) ts") +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                "ts",
                true,
                true
        );
    }

    @Test
    public void testRankWithPartitionBySymbolAndOrderByIntPriceWildcardLast() throws Exception {
        assertQuery(
                replaceTimestampSuffix("""
                        rank\tdense_rank\tprice\tsymbol\tts
                        1\t1\t1\tBB\t1970-01-01T00:00:00.000000Z
                        4\t2\t2\tCC\t1970-01-02T03:46:40.000000Z
                        1\t1\t2\tAA\t1970-01-03T07:33:20.000000Z
                        1\t1\t1\tCC\t1970-01-04T11:20:00.000000Z
                        4\t2\t2\tBB\t1970-01-05T15:06:40.000000Z
                        1\t1\t1\tBB\t1970-01-06T18:53:20.000000Z
                        1\t1\t1\tBB\t1970-01-07T22:40:00.000000Z
                        1\t1\t1\tCC\t1970-01-09T02:26:40.000000Z
                        1\t1\t1\tCC\t1970-01-10T06:13:20.000000Z
                        1\t1\t2\tAA\t1970-01-11T10:00:00.000000Z
                        """, timestampType.getTypeName()),
                "select rank() over (partition by symbol order by price), dense_rank() over (partition by symbol order by price), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_int(1,2,3) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        (timestampType == TestTimestampType.MICRO ? " timestamp_sequence(0, 100000000000) ts" : " timestamp_sequence_ns(0, 100000000000000) ts") +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                "ts",
                true,
                true
        );
    }

    @Test
    public void testRankWithPartitionBySymbolAndOrderByPriceWildcardLast() throws Exception {
        assertQuery(
                replaceTimestampSuffix("""
                        rank\tdense_rank\tprice\tsymbol\tts
                        2\t2\t0.8043224099968393\tCC\t1970-01-01T00:00:00.000000Z
                        1\t1\t0.2845577791213847\tBB\t1970-01-02T03:46:40.000000Z
                        3\t3\t0.9344604857394011\tCC\t1970-01-03T07:33:20.000000Z
                        1\t1\t0.7905675319675964\tAA\t1970-01-04T11:20:00.000000Z
                        6\t6\t0.8899286912289663\tBB\t1970-01-05T15:06:40.000000Z
                        1\t1\t0.11427984775756228\tCC\t1970-01-06T18:53:20.000000Z
                        2\t2\t0.4217768841969397\tBB\t1970-01-07T22:40:00.000000Z
                        4\t4\t0.7261136209823622\tBB\t1970-01-09T02:26:40.000000Z
                        3\t3\t0.6693837147631712\tBB\t1970-01-10T06:13:20.000000Z
                        5\t5\t0.8756771741121929\tBB\t1970-01-11T10:00:00.000000Z
                        """),
                "select rank() over (partition by symbol order by price), dense_rank() over (partition by symbol order by price), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(42) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        (timestampType == TestTimestampType.MICRO ? " timestamp_sequence(0, 100000000000) ts" : " timestamp_sequence_ns(0, 100000000000000) ts") +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                "ts",
                true,
                true
        );
    }

    @Test
    public void testRowHiLessThanRowHi() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, j long, d double, s symbol, c VARCHAR) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab select x::timestamp, x/4, case when x % 3 = 0 THEN NULL ELSE x%5 END, x%5, 'k' || (x%5) ::symbol, 'k' || x from long_sequence(7)");

            assertQueryAndPlan(
                    "select ts, i, j, " +
                            "avg(d) over (order by ts range between 2 preceding and 4 preceding)," +
                            "sum(d) over (order by ts rows between 2 preceding and 4 preceding), " +
                            "first_value(j) over (order by ts rows between 2 preceding and 4 preceding), " +
                            "last_value(j) over (order by ts rows between 2 preceding and 4 preceding), " +
                            "count(*) over (order by ts rows between 2 preceding and 4 preceding), " +
                            "count(d) over (order by ts rows between 2 preceding and 4 preceding), " +
                            "count(s) over (order by ts rows between 2 preceding and 4 preceding), " +
                            "count(c) over (order by ts rows between 2 preceding and 4 preceding), " +
                            "max(d) over (order by ts rows between 2 preceding and 4 preceding), " +
                            "min(d) over (order by ts rows between 2 preceding and 4 preceding), " +
                            "lead(ts) over (order by ts rows between 2 preceding and 4 preceding), " +
                            "lag(ts) over (order by ts rows between 2 preceding and 4 preceding), " +
                            "from tab",
                    """
                            CachedWindow
                              unorderedFunctions: [avg(d) over ( range between 2 preceding and 4 preceding),sum(d) over ( rows between 2 preceding and 4 preceding),first_value(j) over ( rows between 2 preceding and 4 preceding),last_value(j) over ( rows between 2 preceding and 4 preceding),count(*) over ( rows between 2 preceding and 4 preceding),count(d) over ( rows between 2 preceding and 4 preceding),count(s) over ( rows between 2 preceding and 4 preceding),count(c) over ( rows between 2 preceding and 4 preceding),max(d) over ( rows between 2 preceding and 4 preceding),min(d) over ( rows between 2 preceding and 4 preceding),lead(ts, 1, NULL) over (),lag(ts, 1, NULL) over ()]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tab
                            """,
                    replaceTimestampSuffix("""
                            ts\ti\tj\tavg\tsum\tfirst_value\tlast_value\tcount\tcount1\tcount2\tcount3\tmax\tmin\tlead\tlag
                            1970-01-01T00:00:00.000001Z\t0\t1\tnull\tnull\tnull\tnull\t0\t0\t0\t0\tnull\tnull\t1970-01-01T00:00:00.000002Z\t
                            1970-01-01T00:00:00.000002Z\t0\t2\tnull\tnull\tnull\tnull\t0\t0\t0\t0\tnull\tnull\t1970-01-01T00:00:00.000003Z\t1970-01-01T00:00:00.000001Z
                            1970-01-01T00:00:00.000003Z\t0\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\tnull\tnull\t1970-01-01T00:00:00.000004Z\t1970-01-01T00:00:00.000002Z
                            1970-01-01T00:00:00.000004Z\t1\t4\tnull\tnull\tnull\tnull\t0\t0\t0\t0\tnull\tnull\t1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.000003Z
                            1970-01-01T00:00:00.000005Z\t1\t0\tnull\tnull\tnull\tnull\t0\t0\t0\t0\tnull\tnull\t1970-01-01T00:00:00.000006Z\t1970-01-01T00:00:00.000004Z
                            1970-01-01T00:00:00.000006Z\t1\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\tnull\tnull\t1970-01-01T00:00:00.000007Z\t1970-01-01T00:00:00.000005Z
                            1970-01-01T00:00:00.000007Z\t1\t2\tnull\tnull\tnull\tnull\t0\t0\t0\t0\tnull\tnull\t\t1970-01-01T00:00:00.000006Z
                            """),
                    "ts",
                    true,
                    true
            );

            assertQueryAndPlan(
                    "select ts, i, j, " +
                            "avg(d) over (partition by s order by ts range between 2 preceding and 4 preceding)," +
                            "sum(d) over (partition by s order by ts rows between 2 preceding and 4 preceding), " +
                            "first_value(j) over (partition by s order by ts rows between 2 preceding and 4 preceding), " +
                            "last_value(j) over (partition by s order by ts rows between 2 preceding and 4 preceding), " +
                            "count(*) over (partition by s order by ts rows between 2 preceding and 4 preceding), " +
                            "count(d) over (partition by s order by ts rows between 2 preceding and 4 preceding), " +
                            "count(s) over (partition by s order by ts rows between 2 preceding and 4 preceding), " +
                            "count(c) over (partition by s order by ts rows between 2 preceding and 4 preceding), " +
                            "max(d) over (partition by s order by ts rows between 2 preceding and 4 preceding), " +
                            "min(d) over (partition by s order by ts rows between 2 preceding and 4 preceding), " +
                            "lead(ts) over (partition by s order by ts rows between 2 preceding and 4 preceding), " +
                            "lag(ts) over (partition by s order by ts rows between 2 preceding and 4 preceding), " +
                            "from tab",
                    """
                            CachedWindow
                              unorderedFunctions: [avg(d) over (partition by [s] range between 2 preceding and 4 preceding),sum(d) over (partition by [s] rows between 2 preceding and 4 preceding),first_value(j) over (partition by [s] rows between 2 preceding and 4 preceding),last_value(j) over (partition by [s] rows between 2 preceding and 4 preceding),count(*) over (partition by [s] rows between 2 preceding and 4 preceding),count(d) over (partition by [s] rows between 2 preceding and 4 preceding),count(s) over (partition by [s] rows between 2 preceding and 4 preceding),count(c) over (partition by [s] rows between 2 preceding and 4 preceding),max(d) over (partition by [s] rows between 2 preceding and 4 preceding),min(d) over (partition by [s] rows between 2 preceding and 4 preceding),lead(ts, 1, NULL) over (partition by [s]),lag(ts, 1, NULL) over (partition by [s])]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tab
                            """,
                    replaceTimestampSuffix("""
                            ts\ti\tj\tavg\tsum\tfirst_value\tlast_value\tcount\tcount1\tcount2\tcount3\tmax\tmin\tlead\tlag
                            1970-01-01T00:00:00.000001Z\t0\t1\tnull\tnull\tnull\tnull\t0\t0\t0\t0\tnull\tnull\t1970-01-01T00:00:00.000006Z\t
                            1970-01-01T00:00:00.000002Z\t0\t2\tnull\tnull\tnull\tnull\t0\t0\t0\t0\tnull\tnull\t1970-01-01T00:00:00.000007Z\t
                            1970-01-01T00:00:00.000003Z\t0\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\tnull\tnull\t\t
                            1970-01-01T00:00:00.000004Z\t1\t4\tnull\tnull\tnull\tnull\t0\t0\t0\t0\tnull\tnull\t\t
                            1970-01-01T00:00:00.000005Z\t1\t0\tnull\tnull\tnull\tnull\t0\t0\t0\t0\tnull\tnull\t\t
                            1970-01-01T00:00:00.000006Z\t1\tnull\tnull\tnull\tnull\tnull\t0\t0\t0\t0\tnull\tnull\t\t1970-01-01T00:00:00.000001Z
                            1970-01-01T00:00:00.000007Z\t1\t2\tnull\tnull\tnull\tnull\t0\t0\t0\t0\tnull\tnull\t\t1970-01-01T00:00:00.000002Z
                            """),
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testRowNumberFailsInNonWindowContext() throws Exception {
        assertException(
                "select row_number(), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(42) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        (timestampType == TestTimestampType.MICRO ? " timestamp_sequence(0, 100000000000) ts" : " timestamp_sequence_ns(0, 100000000000000) ts") +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                7,
                "window function called in non-window context, make sure to add OVER clause"
        );
    }

    @Test
    public void testRowNumberWithFilter() throws Exception {
        assertQuery(
                """
                        author\tsym\tcommits\trk
                        user1\tETH\t3\t1
                        user2\tETH\t3\t2
                        """,
                "with active_devs as (" +
                        "    select author, sym, count() as commits" +
                        "    from dev_stats" +
                        "    where author is not null and author != 'github-actions[bot]'" +
                        "    order by commits desc" +
                        "    limit 100" +
                        "), " +
                        "active_ranked as (" +
                        "    select author, sym, commits, row_number() over (partition by sym order by commits desc) as rk" +
                        "    from active_devs" +
                        ") " +
                        "select * from active_ranked where sym = 'ETH' order by author, sym, commits",
                "create table dev_stats as " +
                        "(" +
                        "select" +
                        " rnd_symbol('ETH','BTC') sym," +
                        " rnd_symbol('user1','user2') author," +
                        (timestampType == TestTimestampType.MICRO ? " timestamp_sequence(0, 100000000000) ts" : " timestamp_sequence_ns(0, 100000000000000) ts") +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                null,
                true,
                false
        );
    }

    @Test
    public void testRowNumberWithNoPartitionAndDifferentOrder() throws Exception {
        assertQuery(
                """
                        x\ty\trn
                        1\t1\t10
                        2\t0\t5
                        3\t1\t9
                        4\t0\t4
                        5\t1\t8
                        6\t0\t3
                        7\t1\t7
                        8\t0\t2
                        9\t1\t6
                        10\t0\t1
                        """,
                "select *, row_number() over (order by y asc, x desc) as rn from tab order by x asc",
                "create table tab as (select x, x%2 y from long_sequence(10))",
                null,
                true,
                true
        );
    }

    @Test
    public void testRowNumberWithNoPartitionAndNoOrderInSubQuery() throws Exception {
        assertQuery(
                """
                        symbol\trn
                        CC\t2
                        BB\t3
                        CC\t4
                        AA\t5
                        BB\t6
                        CC\t7
                        BB\t8
                        BB\t9
                        BB\t10
                        BB\t11
                        """,
                "select symbol, rn + 1 as rn from (select symbol, row_number() over() as rn from trades)",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(42) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        (timestampType == TestTimestampType.MICRO ? " timestamp_sequence(0, 100000000000) ts" : " timestamp_sequence_ns(0, 100000000000000) ts") +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                null,
                false,
                true
        );
    }

    @Test
    public void testRowNumberWithNoPartitionAndNoOrderWildcardLast() throws Exception {
        assertQuery(
                replaceTimestampSuffix("""
                        row_number\tprice\tsymbol\tts
                        1\t0.8043224099968393\tCC\t1970-01-01T00:00:00.000000Z
                        2\t0.2845577791213847\tBB\t1970-01-02T03:46:40.000000Z
                        3\t0.9344604857394011\tCC\t1970-01-03T07:33:20.000000Z
                        4\t0.7905675319675964\tAA\t1970-01-04T11:20:00.000000Z
                        5\t0.8899286912289663\tBB\t1970-01-05T15:06:40.000000Z
                        6\t0.11427984775756228\tCC\t1970-01-06T18:53:20.000000Z
                        7\t0.4217768841969397\tBB\t1970-01-07T22:40:00.000000Z
                        8\t0.7261136209823622\tBB\t1970-01-09T02:26:40.000000Z
                        9\t0.6693837147631712\tBB\t1970-01-10T06:13:20.000000Z
                        10\t0.8756771741121929\tBB\t1970-01-11T10:00:00.000000Z
                        """),
                "select row_number() over (), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(42) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        (timestampType == TestTimestampType.MICRO ? " timestamp_sequence(0, 100000000000) ts" : " timestamp_sequence_ns(0, 100000000000000) ts") +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                "ts",
                false,
                true
        );
    }

    @Test
    public void testRowNumberWithNoPartitionAndOrderBySymbolWildcardLast() throws Exception {
        assertQuery(
                replaceTimestampSuffix("""
                        row_number\tprice\tsymbol\tts
                        8\t0.8043224099968393\tCC\t1970-01-01T00:00:00.000000Z
                        2\t0.2845577791213847\tBB\t1970-01-02T03:46:40.000000Z
                        9\t0.9344604857394011\tCC\t1970-01-03T07:33:20.000000Z
                        1\t0.7905675319675964\tAA\t1970-01-04T11:20:00.000000Z
                        3\t0.8899286912289663\tBB\t1970-01-05T15:06:40.000000Z
                        10\t0.11427984775756228\tCC\t1970-01-06T18:53:20.000000Z
                        4\t0.4217768841969397\tBB\t1970-01-07T22:40:00.000000Z
                        5\t0.7261136209823622\tBB\t1970-01-09T02:26:40.000000Z
                        6\t0.6693837147631712\tBB\t1970-01-10T06:13:20.000000Z
                        7\t0.8756771741121929\tBB\t1970-01-11T10:00:00.000000Z
                        """),
                "select row_number() over (order by symbol), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(42) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        (timestampType == TestTimestampType.MICRO ? " timestamp_sequence(0, 100000000000) ts" : " timestamp_sequence_ns(0, 100000000000000) ts") +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                "ts",
                true,
                true
        );
    }

    @Test
    public void testRowNumberWithNoPartitionAndSameOrderFollowedByBaseFactory() throws Exception {
        assertQuery(
                replaceTimestampSuffix("""
                        ts\ts\trn
                        1970-01-01T00:00:00.000000Z\ta\t1
                        1970-01-02T03:46:40.000000Z\ta\t2
                        1970-01-10T06:13:20.000000Z\ta\t3
                        1970-01-03T07:33:20.000000Z\tb\t4
                        1970-01-09T02:26:40.000000Z\tb\t5
                        1970-01-11T10:00:00.000000Z\tb\t6
                        1970-01-04T11:20:00.000000Z\tc\t7
                        1970-01-05T15:06:40.000000Z\tc\t8
                        1970-01-06T18:53:20.000000Z\tc\t9
                        1970-01-07T22:40:00.000000Z\tc\t10
                        """),
                "select *, row_number() over (order by s) as rn from tab where ts in ('1970-01') order by s",
                "create table tab as " +
                        "(" +
                        "select" +
                        (timestampType == TestTimestampType.MICRO ? " timestamp_sequence(0, 100000000000) ts," : " timestamp_sequence_ns(0, 100000000000000) ts,") +
                        " rnd_symbol('a','b','c') s" +
                        " from long_sequence(10)" +
                        "), index(s) timestamp(ts) partition by month",
                null,
                false,
                false
        );
    }

    @Test
    public void testRowNumberWithNoPartitionAndSameOrderNotFollowedByBaseFactory() throws Exception {
        assertQuery(
                replaceTimestampSuffix("""
                        ts\ts\trn
                        1970-01-01T00:00:00.000000Z\ta\t1
                        1970-01-02T03:46:40.000000Z\ta\t2
                        1970-01-10T06:13:20.000000Z\ta\t3
                        1970-01-03T07:33:20.000000Z\tb\t4
                        1970-01-09T02:26:40.000000Z\tb\t5
                        1970-01-11T10:00:00.000000Z\tb\t6
                        1970-01-04T11:20:00.000000Z\tc\t7
                        1970-01-05T15:06:40.000000Z\tc\t8
                        1970-01-06T18:53:20.000000Z\tc\t9
                        1970-01-07T22:40:00.000000Z\tc\t10
                        """),
                "select *, row_number() over (order by s) as rn from tab where ts in ('1970-01') order by s",
                "create table tab as " +
                        "(" +
                        "select" +
                        (timestampType == TestTimestampType.MICRO ? " timestamp_sequence(0, 100000000000) ts," : " timestamp_sequence_ns(0, 100000000000000) ts,") +
                        " rnd_symbol('a','b','c') s" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by month",
                null,
                true,
                true
        );
    }

    @Test
    public void testRowNumberWithPartitionAndOrderByNonSymbol() throws Exception {
        assertQuery(
                replaceTimestampSuffix("""
                        row_number\tprice\tts
                        1\t42\t1970-01-01T00:00:00.000000Z
                        2\t42\t1970-01-02T03:46:40.000000Z
                        3\t42\t1970-01-03T07:33:20.000000Z
                        4\t42\t1970-01-04T11:20:00.000000Z
                        5\t42\t1970-01-05T15:06:40.000000Z
                        6\t42\t1970-01-06T18:53:20.000000Z
                        7\t42\t1970-01-07T22:40:00.000000Z
                        8\t42\t1970-01-09T02:26:40.000000Z
                        9\t42\t1970-01-10T06:13:20.000000Z
                        10\t42\t1970-01-11T10:00:00.000000Z
                        """),
                "select row_number() over (partition by price order by ts), price, ts from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " 42 price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        (timestampType == TestTimestampType.MICRO ? " timestamp_sequence(0, 100000000000) ts," : " timestamp_sequence_ns(0, 100000000000000) ts,") +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                "ts",
                false,
                true
        );
    }

    @Test
    public void testRowNumberWithPartitionAndOrderBySymbolNoWildcard() throws Exception {
        assertQuery(
                """
                        row_number
                        1
                        1
                        2
                        1
                        2
                        3
                        3
                        4
                        5
                        6
                        """,
                "select row_number() over (partition by symbol order by symbol) from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(42) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        (timestampType == TestTimestampType.MICRO ? " timestamp_sequence(0, 100000000000) ts" : " timestamp_sequence_ns(0, 100000000000000) ts") +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                null,
                true,
                true
        );
    }

    @Test
    public void testRowNumberWithPartitionAndOrderBySymbolWildcardFirst() throws Exception {
        assertQuery(
                replaceTimestampSuffix("""
                        price\tsymbol\tts\trow_number
                        0.8043224099968393\tCC\t1970-01-01T00:00:00.000000Z\t1
                        0.2845577791213847\tBB\t1970-01-02T03:46:40.000000Z\t1
                        0.9344604857394011\tCC\t1970-01-03T07:33:20.000000Z\t2
                        0.7905675319675964\tAA\t1970-01-04T11:20:00.000000Z\t1
                        0.8899286912289663\tBB\t1970-01-05T15:06:40.000000Z\t2
                        0.11427984775756228\tCC\t1970-01-06T18:53:20.000000Z\t3
                        0.4217768841969397\tBB\t1970-01-07T22:40:00.000000Z\t3
                        0.7261136209823622\tBB\t1970-01-09T02:26:40.000000Z\t4
                        0.6693837147631712\tBB\t1970-01-10T06:13:20.000000Z\t5
                        0.8756771741121929\tBB\t1970-01-11T10:00:00.000000Z\t6
                        """),
                "select *, row_number() over (partition by symbol order by symbol) from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(42) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        (timestampType == TestTimestampType.MICRO ? " timestamp_sequence(0, 100000000000) ts" : " timestamp_sequence_ns(0, 100000000000000) ts") +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                "ts",
                true,
                true
        );
    }

    @Test
    public void testRowNumberWithPartitionAndOrderBySymbolWildcardLast() throws Exception {
        assertQuery(
                replaceTimestampSuffix("""
                        row_number\tprice\tsymbol\tts
                        1\t0.8043224099968393\tCC\t1970-01-01T00:00:00.000000Z
                        1\t0.2845577791213847\tBB\t1970-01-02T03:46:40.000000Z
                        2\t0.9344604857394011\tCC\t1970-01-03T07:33:20.000000Z
                        1\t0.7905675319675964\tAA\t1970-01-04T11:20:00.000000Z
                        2\t0.8899286912289663\tBB\t1970-01-05T15:06:40.000000Z
                        3\t0.11427984775756228\tCC\t1970-01-06T18:53:20.000000Z
                        3\t0.4217768841969397\tBB\t1970-01-07T22:40:00.000000Z
                        4\t0.7261136209823622\tBB\t1970-01-09T02:26:40.000000Z
                        5\t0.6693837147631712\tBB\t1970-01-10T06:13:20.000000Z
                        6\t0.8756771741121929\tBB\t1970-01-11T10:00:00.000000Z
                        """),
                "select row_number() over (partition by symbol order by symbol), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(42) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        (timestampType == TestTimestampType.MICRO ? " timestamp_sequence(0, 100000000000) ts" : " timestamp_sequence_ns(0, 100000000000000) ts") +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                "ts",
                true,
                true
        );
    }

    @Test
    public void testRowNumberWithPartitionByScalarFunction() throws Exception {
        assertQuery(
                replaceTimestampSuffix("""
                        row_number\tts
                        1\t1970-01-01T00:00:00.000000Z
                        2\t1970-01-02T03:46:40.000000Z
                        1\t1970-01-03T07:33:20.000000Z
                        1\t1970-01-04T11:20:00.000000Z
                        2\t1970-01-05T15:06:40.000000Z
                        3\t1970-01-06T18:53:20.000000Z
                        4\t1970-01-07T22:40:00.000000Z
                        2\t1970-01-09T02:26:40.000000Z
                        3\t1970-01-10T06:13:20.000000Z
                        3\t1970-01-11T10:00:00.000000Z
                        """),
                "select row_number() over (partition by concat(symbol, '_foo') order by symbol), ts from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        (timestampType == TestTimestampType.MICRO ? " timestamp_sequence(0, 100000000000) ts" : " timestamp_sequence_ns(0, 100000000000000) ts") +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                "ts",
                true,
                true
        );
    }

    @Test
    public void testTwoWindowColumns() throws Exception {
        // Test two window functions with different specifications
        assertQuery(
                """
                        id\trn\ttotal
                        1\t1\t15.0
                        2\t2\t15.0
                        3\t3\t15.0
                        4\t4\t15.0
                        5\t5\t15.0
                        """,
                "SELECT id, " +
                        "row_number() OVER (ORDER BY ts) AS rn, " +
                        "sum(id) OVER () AS total " +
                        "FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT x AS id, timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(5)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                true,
                true
        );
    }

    @Test
    public void testTwoWindowFunctionsInArithmetic() throws Exception {
        // Test arithmetic between two window functions: sum() OVER - lag() OVER
        assertQuery(
                """
                        id_diff
                        null
                        2.0
                        4.0
                        7.0
                        11.0
                        """,
                "SELECT sum(id) OVER (ORDER BY ts) - lag(id) OVER (ORDER BY ts) AS id_diff FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT x AS id, timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(5)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,
                true
        );
    }

    @Test
    public void testUnSupportImplicitCast() throws Exception {
        assertException(
                "SELECT ts, side, lead(side) OVER ( PARTITION BY symbol ORDER BY ts ) " +
                        "AS next_price FROM trades ",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(100) price," +
                        " rnd_symbol('XX','YY','ZZ') side," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        (timestampType == TestTimestampType.MICRO ? " timestamp_sequence(0, 100000000000) ts" : " timestamp_sequence_ns(0, 100000000000000) ts") +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                0,
                "inconvertible value: `ZZ` [SYMBOL -> TIMESTAMP_NS]"
        );

        assertException(
                "SELECT ts, side, first_value(side) OVER ( PARTITION BY symbol ORDER BY ts ) " +
                        "AS next_price FROM trades ",
                0,
                "inconvertible value: `ZZ` [SYMBOL -> TIMESTAMP_NS]"
        );
    }

    @Test
    public void testWindowAvg() throws Exception {
        // Test avg() window function
        assertQuery(
                """
                        id\tavg_val
                        1\t1.0
                        2\t1.5
                        3\t2.0
                        4\t2.5
                        5\t3.0
                        """,
                "SELECT id, avg(id) OVER (ORDER BY ts) AS avg_val FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT x AS id, timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(5)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,
                true
        );
    }

    @Test
    public void testWindowBufferExceedsLimit() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_STORE_PAGE_SIZE, 4096);
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_STORE_MAX_PAGES, 10);

        try {
            assertMemoryLeak(() -> {
                executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, j long, d double, s symbol, c VARCHAR) timestamp(ts)", timestampType.getTypeName());
                execute("insert into tab select x::timestamp, 1, x, x*2::double, 'k' || (x%5) ::symbol, 'k' || x from long_sequence(100000)");

                //TODO: improve error message and position
                assertExceptionNoLeakCheck(
                        "select avg(j) over (partition by i rows between 100001 preceding and current row) from tab",
                        0,
                        "Maximum number of pages (10) breached in VirtualMemory"
                );

                assertExceptionNoLeakCheck(
                        "select sum(j) over (partition by i rows between 100001 preceding and current row) from tab",
                        0,
                        "Maximum number of pages (10) breached in VirtualMemory"
                );

                assertExceptionNoLeakCheck(
                        "select first_value(j) over (partition by i rows between 100001 preceding and current row) from tab",
                        0,
                        "Maximum number of pages (10) breached in VirtualMemory"
                );

                assertExceptionNoLeakCheck(
                        "select first_value(j) ignore nulls over (partition by i rows between 100001 preceding and current row) from tab",
                        0,
                        "Maximum number of pages (10) breached in VirtualMemory"
                );

                assertExceptionNoLeakCheck(
                        "select last_value(j) ignore nulls over (partition by i rows between 100001 preceding and current row) from tab",
                        0,
                        "Maximum number of pages (10) breached in VirtualMemory"
                );

                assertExceptionNoLeakCheck(
                        "select count(j) over (partition by i rows between 100001 preceding and current row) from tab",
                        0,
                        "Maximum number of pages (10) breached in VirtualMemory"
                );
                assertExceptionNoLeakCheck(
                        "select count(s) over (partition by i rows between 100001 preceding and current row) from tab",
                        0,
                        "Maximum number of pages (10) breached in VirtualMemory"
                );
                assertExceptionNoLeakCheck(
                        "select count(d) over (partition by i rows between 100001 preceding and current row) from tab",
                        0,
                        "Maximum number of pages (10) breached in VirtualMemory"
                );
                assertExceptionNoLeakCheck(
                        "select count(c) over (partition by i rows between 100001 preceding and current row) from tab",
                        0,
                        "Maximum number of pages (10) breached in VirtualMemory"
                );
                assertExceptionNoLeakCheck(
                        "select max(j) over (partition by i rows between 100001 preceding and current row) from tab",
                        0,
                        "Maximum number of pages (10) breached in VirtualMemory"
                );
                assertExceptionNoLeakCheck(
                        "select min(j) over (partition by i rows between 100001 preceding and current row) from tab",
                        0,
                        "Maximum number of pages (10) breached in VirtualMemory"
                );
                assertExceptionNoLeakCheck(
                        "select lag(j, 1000001) over(partition by i) from tab",
                        0,
                        "Maximum number of pages (10) breached in VirtualMemory"
                );
                assertExceptionNoLeakCheck(
                        "select lead(j, 1000001) over(partition by i) from tab",
                        0,
                        "Maximum number of pages (10) breached in VirtualMemory"
                );
                assertExceptionNoLeakCheck(
                        "select lag(j, 1000001) ignore nulls over(partition by i) from tab",
                        0,
                        "Maximum number of pages (10) breached in VirtualMemory"
                );
                assertExceptionNoLeakCheck(
                        "select lead(j, 1000001) ignore nulls over(partition by i) from tab",
                        0,
                        "Maximum number of pages (10) breached in VirtualMemory"
                );
            });
        } finally {
            // disable
            node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_STORE_PAGE_SIZE, 0);
            node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_STORE_MAX_PAGES, 0);
        }
    }

    @Test
    public void testWindowCount() throws Exception {
        // Test count() window function
        assertQuery(
                """
                        id\tcnt
                        1\t1
                        2\t2
                        3\t3
                        4\t4
                        5\t5
                        """,
                "SELECT id, count(*) OVER (ORDER BY ts) AS cnt FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT x AS id, timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(5)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,
                true
        );
    }

    @Test
    public void testWindowCumulativeSum() throws Exception {
        // Test cumulative sum using ROWS UNBOUNDED PRECEDING
        assertQuery(
                """
                        id\tcum_sum
                        1\t1.0
                        2\t3.0
                        3\t6.0
                        4\t10.0
                        5\t15.0
                        """,
                "SELECT id, sum(id) OVER (ORDER BY ts ROWS UNBOUNDED PRECEDING) AS cum_sum FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT x AS id, timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(5)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,
                true
        );
    }

    @Test
    public void testWindowDenseRank() throws Exception {
        // Test dense_rank() window function with ties
        assertQuery(
                """
                        val\tdrnk
                        1\t1
                        1\t1
                        2\t2
                        3\t3
                        3\t3
                        """,
                "SELECT val, dense_rank() OVER (ORDER BY val) AS drnk FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT " +
                        "  CASE WHEN x IN (1, 2) THEN 1 WHEN x = 3 THEN 2 ELSE 3 END AS val, " +
                        "  timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(5)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                true,
                true
        );
    }

    @Test
    public void testWindowFactoryRetainsTimestampMetadata() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, j long, c VARCHAR, sym symbol index) timestamp(ts)", timestampType.getTypeName());

            // table scans
            assertQueryAndPlan(
                    "select ts, i, j, " +
                            "first_value(j) over(), " +
                            "first_value(j) ignore nulls over(), " +
                            "last_value(j) over(), " +
                            "last_value(j) ignore nulls over(), " +
                            "avg(j) over (), " +
                            "sum(j) over (), " +
                            "count(j) over (), " +
                            "count(*) over (), " +
                            "count(c) over (), " +
                            "count(sym) over (), " +
                            "max(j) over (), " +
                            "min(j) over (), " +
                            "row_number() over (), " +
                            "rank() over (), " +
                            "dense_rank() over (), " +
                            "lag(j) over (), " +
                            "lead(j) over (), " +
                            "lag(j) ignore nulls over (), " +
                            "lead(j) ignore nulls over () " +
                            "from tab",
                    replacePlanTimestamp("""
                            CachedWindow
                              unorderedFunctions: [first_value(j) over (),first_value(j) ignore nulls over (),last_value(j) over (),last_value(j) ignore nulls over (),avg(j) over (),sum(j) over (),count(j) over (),count(*) over (),count(c) over (),count(sym) over (),max(j) over (),min(j) over (),row_number(),rank() over (),dense_rank() over (),lag(j, 1, NULL) over (),lead(j, 1, NULL) over (),lag(j, 1, NULL) ignore nulls over (),lead(j, 1, NULL) ignore nulls over ()]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tab
                            """),
                    "ts\ti\tj\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tavg\tsum\tcount\tcount1\tcount2\tcount3\tmax\tmin\trow_number\trank\tdense_rank\tlag\tlead\tlag_ignore_nulls\tlead_ignore_nulls\n",
                    "ts",
                    true,
                    false
            );

            assertQueryAndPlan(
                    "select ts, i, j, first_value(j) over(), first_value(j) ignore nulls over(), last_value(j) over(), last_value(j) ignore nulls over(), avg(j) over (), sum(j) over (), count(*) over (), count(j) over (), count(sym) over (), count(c) over (), " +
                            "max(j) over (), min(j) over () from tab order by ts desc",
                    """
                            CachedWindow
                              unorderedFunctions: [first_value(j) over (),first_value(j) ignore nulls over (),last_value(j) over (),last_value(j) ignore nulls over (),avg(j) over (),sum(j) over (),count(*) over (),count(j) over (),count(sym) over (),count(c) over (),max(j) over (),min(j) over ()]
                                PageFrame
                                    Row backward scan
                                    Frame backward scan on: tab
                            """,
                    "ts\ti\tj\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tavg\tsum\tcount\tcount1\tcount2\tcount3\tmax\tmin\n",
                    "ts###desc",
                    true,
                    false
            );

            assertQueryAndPlan(
                    "select ts, i, j, " +
                            "first_value(j) over(order by ts), " +
                            "first_value(j) ignore nulls over(order by ts), " +
                            "last_value(j) over(order by ts), " +
                            "last_value(j) ignore nulls over(order by ts), " +
                            "avg(j) over (order by ts), " +
                            "sum(j) over (order by ts), " +
                            "count(*) over (order by ts), " +
                            "count(j) over (order by ts), " +
                            "count(sym) over (order by ts), " +
                            "count(c) over (order by ts), " +
                            "max(j) over (order by ts), " +
                            "min(j) over (order by ts) " +
                            "from tab",
                    """
                            CachedWindow
                              unorderedFunctions: [first_value(j) over (),first_value(j) ignore nulls over (),last_value(j) over (range between unbounded preceding and current row),last_value(j) ignore nulls over (rows between unbounded preceding and current row),avg(j) over (rows between unbounded preceding and current row),sum(j) over (rows between unbounded preceding and current row),count(*) over (rows between unbounded preceding and current row),count(j) over (rows between unbounded preceding and current row),count(sym) over (rows between unbounded preceding and current row),count(c) over (rows between unbounded preceding and current row),max(j) over (rows between unbounded preceding and current row),min(j) over (rows between unbounded preceding and current row)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tab
                            """,
                    "ts\ti\tj\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tavg\tsum\tcount\tcount1\tcount2\tcount3\tmax\tmin\n",
                    "ts",
                    true,
                    false
            );

            assertQueryAndPlan(
                    "select ts, i, j, " +
                            "first_value(j) over (order by ts desc), " +
                            "first_value(j) ignore nulls over (order by ts desc), " +
                            "last_value(j) over (order by ts desc), " +
                            "last_value(j) ignore nulls over (order by ts desc), " +
                            "avg(j) over (order by ts desc), " +
                            "sum(j) over (order by ts desc), " +
                            "count(*) over (order by ts desc), " +
                            "count(j) over (order by ts desc), " +
                            "count(sym) over (order by ts desc), " +
                            "count(c) over (order by ts desc), " +
                            "max(j) over (order by ts desc), " +
                            "min(j) over (order by ts desc) " +
                            "from tab order by ts desc",
                    """
                            CachedWindow
                              unorderedFunctions: [first_value(j) over (),first_value(j) ignore nulls over (),last_value(j) over (range between unbounded preceding and current row),last_value(j) ignore nulls over (rows between unbounded preceding and current row),avg(j) over (rows between unbounded preceding and current row),sum(j) over (rows between unbounded preceding and current row),count(*) over (rows between unbounded preceding and current row),count(j) over (rows between unbounded preceding and current row),count(sym) over (rows between unbounded preceding and current row),count(c) over (rows between unbounded preceding and current row),max(j) over (rows between unbounded preceding and current row),min(j) over (rows between unbounded preceding and current row)]
                                PageFrame
                                    Row backward scan
                                    Frame backward scan on: tab
                            """,
                    "ts\ti\tj\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tavg\tsum\tcount\tcount1\tcount2\tcount3\tmax\tmin\n",
                    "ts###desc",
                    true,
                    false
            );

            assertQueryAndPlan(
                    "select ts, i, j, " +
                            "first_value(j) over (partition by i), " +
                            "first_value(j) ignore nulls over (partition by i), " +
                            "last_value(j) over (partition by i), " +
                            "last_value(j) ignore nulls over (partition by i), " +
                            "avg(j) over (partition by i), " +
                            "sum(j) over (partition by i), " +
                            "count(*) over (partition by i), " +
                            "count(j) over (partition by i), " +
                            "count(sym) over (partition by i), " +
                            "count(c) over (partition by i), " +
                            "max(j) over (partition by i), " +
                            "min(j) over (partition by i) " +
                            "from tab",
                    """
                            CachedWindow
                              unorderedFunctions: [first_value(j) over (partition by [i]),first_value(j) ignore nulls over (partition by [i]),last_value(j) over (partition by [i]),last_value(j) ignore nulls over (partition by [i]),avg(j) over (partition by [i]),sum(j) over (partition by [i]),count(*) over (partition by [i]),count(j) over (partition by [i]),count(sym) over (partition by [i]),count(c) over (partition by [i]),max(j) over (partition by [i]),min(j) over (partition by [i])]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tab
                            """,
                    "ts\ti\tj\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tavg\tsum\tcount\tcount1\tcount2\tcount3\tmax\tmin\n",
                    "ts",
                    true,
                    false
            );

            assertQueryAndPlan(
                    "select ts, i, j, " +
                            "first_value(j) over (partition by i), " +
                            "first_value(j) ignore nulls over (partition by i), " +
                            "last_value(j) over (partition by i), " +
                            "last_value(j) ignore nulls over (partition by i), " +
                            "avg(j) over (partition by i), " +
                            "sum(j) over (partition by i), " +
                            "count(*) over (partition by i), " +
                            "count(j) over (partition by i), " +
                            "count(sym) over (partition by i), " +
                            "count(c) over (partition by i), " +
                            "max(j) over (partition by i), " +
                            "min(j) over (partition by i) " +
                            "from tab order by ts desc",
                    """
                            CachedWindow
                              unorderedFunctions: [first_value(j) over (partition by [i]),first_value(j) ignore nulls over (partition by [i]),last_value(j) over (partition by [i]),last_value(j) ignore nulls over (partition by [i]),avg(j) over (partition by [i]),sum(j) over (partition by [i]),count(*) over (partition by [i]),count(j) over (partition by [i]),count(sym) over (partition by [i]),count(c) over (partition by [i]),max(j) over (partition by [i]),min(j) over (partition by [i])]
                                PageFrame
                                    Row backward scan
                                    Frame backward scan on: tab
                            """,
                    "ts\ti\tj\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tavg\tsum\tcount\tcount1\tcount2\tcount3\tmax\tmin\n",
                    "ts###desc",
                    true,
                    false
            );

            assertQueryAndPlan(
                    "select ts, i, j, " +
                            "first_value(j) over (partition by i order by ts), " +
                            "first_value(j) ignore nulls over (partition by i order by ts), " +
                            "last_value(j) over (partition by i order by ts), " +
                            "last_value(j) ignore nulls over (partition by i order by ts), " +
                            "avg(j) over (partition by i order by ts), " +
                            "sum(j) over (partition by i order by ts), " +
                            "count(*) over (partition by i order by ts), " +
                            "count(j) over (partition by i order by ts), " +
                            "count(sym) over (partition by i order by ts), " +
                            "count(c) over (partition by i order by ts), " +
                            "max(j) over (partition by i order by ts), " +
                            "min(j) over (partition by i order by ts) " +
                            "from tab",
                    """
                            Window
                              functions: [first_value(j) over (partition by [i] rows between unbounded preceding and current row),first_value(j) ignore nulls over (partition by [i] rows between unbounded preceding and current row),last_value(j) over (partition by [i] range between unbounded preceding and current row),last_value(j) ignore nulls over (partition by [i] rows between unbounded preceding and current row),avg(j) over (partition by [i] rows between unbounded preceding and current row),sum(j) over (partition by [i] rows between unbounded preceding and current row),count(*) over (partition by [i] rows between unbounded preceding and current row),count(j) over (partition by [i] rows between unbounded preceding and current row),count(sym) over (partition by [i] rows between unbounded preceding and current row),count(c) over (partition by [i] rows between unbounded preceding and current row),max(j) over (partition by [i] rows between unbounded preceding and current row),min(j) over (partition by [i] rows between unbounded preceding and current row)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tab
                            """,
                    "ts\ti\tj\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tavg\tsum\tcount\tcount1\tcount2\tcount3\tmax\tmin\n",
                    "ts",
                    false,
                    true
            );

            assertQueryAndPlan(
                    "select ts, i, j, " +
                            "first_value(j) over (partition by i order by ts desc), " +
                            "first_value(j) ignore nulls over (partition by i order by ts desc), " +
                            "last_value(j) over (partition by i order by ts desc), " +
                            "last_value(j) ignore nulls over (partition by i order by ts desc), " +
                            "avg(j) over (partition by i order by ts desc), " +
                            "sum(j) over (partition by i order by ts desc), " +
                            "count(*) over (partition by i order by ts desc), " +
                            "count(j) over (partition by i order by ts desc), " +
                            "count(sym) over (partition by i order by ts desc), " +
                            "count(c) over (partition by i order by ts desc), " +
                            "max(j) over (partition by i order by ts desc), " +
                            "min(j) over (partition by i order by ts desc) " +
                            "from tab " +
                            "order by ts desc",
                    """
                            Window
                              functions: [first_value(j) over (partition by [i] rows between unbounded preceding and current row),first_value(j) ignore nulls over (partition by [i] rows between unbounded preceding and current row),last_value(j) over (partition by [i] range between unbounded preceding and current row),last_value(j) ignore nulls over (partition by [i] rows between unbounded preceding and current row),avg(j) over (partition by [i] rows between unbounded preceding and current row),sum(j) over (partition by [i] rows between unbounded preceding and current row),count(*) over (partition by [i] rows between unbounded preceding and current row),count(j) over (partition by [i] rows between unbounded preceding and current row),count(sym) over (partition by [i] rows between unbounded preceding and current row),count(c) over (partition by [i] rows between unbounded preceding and current row),max(j) over (partition by [i] rows between unbounded preceding and current row),min(j) over (partition by [i] rows between unbounded preceding and current row)]
                                PageFrame
                                    Row backward scan
                                    Frame backward scan on: tab
                            """,
                    "ts\ti\tj\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tavg\tsum\tcount\tcount1\tcount2\tcount3\tmax\tmin\n",
                    "ts###desc",
                    false,
                    true
            );

            assertQueryAndPlan(
                    "select i, j, " +
                            "first_value(j) over (partition by i order by ts), " +
                            "first_value(j) ignore nulls over (partition by i order by ts), " +
                            "last_value(j) over (partition by i order by ts), " +
                            "last_value(j) ignore nulls over (partition by i order by ts), " +
                            "avg(j) over (partition by i order by ts), " +
                            "sum(j) over (partition by i order by ts), " +
                            "count(*) over (partition by i order by ts), " +
                            "count(j) over (partition by i order by ts), " +
                            "count(sym) over (partition by i order by ts), " +
                            "count(c) over (partition by i order by ts), " +
                            "max(j) over (partition by i order by ts), " +
                            "min(j) over (partition by i order by ts) " +
                            "from tab " +
                            "order by ts",
                    """
                            SelectedRecord
                                Window
                                  functions: [first_value(j) over (partition by [i] rows between unbounded preceding and current row),first_value(j) ignore nulls over (partition by [i] rows between unbounded preceding and current row),last_value(j) over (partition by [i] range between unbounded preceding and current row),last_value(j) ignore nulls over (partition by [i] rows between unbounded preceding and current row),avg(j) over (partition by [i] rows between unbounded preceding and current row),sum(j) over (partition by [i] rows between unbounded preceding and current row),count(*) over (partition by [i] rows between unbounded preceding and current row),count(j) over (partition by [i] rows between unbounded preceding and current row),count(sym) over (partition by [i] rows between unbounded preceding and current row),count(c) over (partition by [i] rows between unbounded preceding and current row),max(j) over (partition by [i] rows between unbounded preceding and current row),min(j) over (partition by [i] rows between unbounded preceding and current row)]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tab
                            """,
                    "i\tj\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tavg\tsum\tcount\tcount1\tcount2\tcount3\tmax\tmin\n",
                    null,
                    false,
                    true
            );

            assertQueryAndPlan(
                    "select i, j, " +
                            "first_value(j) over (partition by i order by ts desc), " +
                            "first_value(j) ignore nulls over (partition by i order by ts desc), " +
                            "last_value(j) over (partition by i order by ts desc), " +
                            "last_value(j) ignore nulls over (partition by i order by ts desc), " +
                            "avg(j) over (partition by i order by ts desc), " +
                            "sum(j) over (partition by i order by ts desc), " +
                            "count(*) over (partition by i order by ts desc), " +
                            "count(j) over (partition by i order by ts desc), " +
                            "count(sym) over (partition by i order by ts desc), " +
                            "count(c) over (partition by i order by ts desc), " +
                            "max(j) over (partition by i order by ts desc), " +
                            "min(j) over (partition by i order by ts desc) " +
                            "from tab " +
                            "order by ts desc",
                    """
                            SelectedRecord
                                Window
                                  functions: [first_value(j) over (partition by [i] rows between unbounded preceding and current row),first_value(j) ignore nulls over (partition by [i] rows between unbounded preceding and current row),last_value(j) over (partition by [i] range between unbounded preceding and current row),last_value(j) ignore nulls over (partition by [i] rows between unbounded preceding and current row),avg(j) over (partition by [i] rows between unbounded preceding and current row),sum(j) over (partition by [i] rows between unbounded preceding and current row),count(*) over (partition by [i] rows between unbounded preceding and current row),count(j) over (partition by [i] rows between unbounded preceding and current row),count(sym) over (partition by [i] rows between unbounded preceding and current row),count(c) over (partition by [i] rows between unbounded preceding and current row),max(j) over (partition by [i] rows between unbounded preceding and current row),min(j) over (partition by [i] rows between unbounded preceding and current row)]
                                    PageFrame
                                        Row backward scan
                                        Frame backward scan on: tab
                            """,
                    "i\tj\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tavg\tsum\tcount\tcount1\tcount2\tcount3\tmax\tmin\n",
                    null,
                    false,
                    true
            );

            assertQueryAndPlan(
                    "select i, j, " +
                            "first_value(j) over (partition by i order by ts range between 10 seconds preceding and current row), " +
                            "first_value(j) ignore nulls over (partition by i order by ts range between 10 seconds preceding and current row), " +
                            "last_value(j) over (partition by i order by ts range between 10 seconds preceding and current row), " +
                            "last_value(j) ignore nulls over (partition by i order by ts range between 10 seconds preceding and current row), " +
                            "avg(j) over (partition by i order by ts range between 10 seconds preceding and current row), " +
                            "sum(j) over (partition by i order by ts range between 10 seconds preceding and current row), " +
                            "count(*) over (partition by i order by ts range between 10 seconds preceding and current row), " +
                            "count(j) over (partition by i order by ts range between 10 seconds preceding and current row), " +
                            "count(sym) over (partition by i order by ts range between 10 seconds preceding and current row), " +
                            "count(c) over (partition by i order by ts range between 10 seconds preceding and current row), " +
                            "max(j) over (partition by i order by ts range between 10 seconds preceding and current row), " +
                            "min(j) over (partition by i order by ts range between 10 seconds preceding and current row), " +
                            "ts from tab",
                    replacePlanTimestamp("""
                            Window
                              functions: [first_value(j) over (partition by [i] range between 10000000 preceding and current row),first_value(j) ignore nulls over (partition by [i] range between 10000000 preceding and current row),last_value(j) over (partition by [i] range between 10000000 preceding and current row),last_value(j) ignore nulls over (partition by [i] range between 10000000 preceding and 0 preceding),avg(j) over (partition by [i] range between 10000000 preceding and current row),sum(j) over (partition by [i] range between 10000000 preceding and current row),count(*) over (partition by [i] range between 10000000 preceding and current row),count(j) over (partition by [i] range between 10000000 preceding and current row),count(sym) over (partition by [i] range between 10000000 preceding and current row),count(c) over (partition by [i] range between 10000000 preceding and current row),max(j) over (partition by [i] range between 10000000 preceding and current row),min(j) over (partition by [i] range between 10000000 preceding and current row)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tab
                            """),
                    "i\tj\tfirst_value\tfirst_value_ignore_nulls\tlast_value\tlast_value_ignore_nulls\tavg\tsum\tcount\tcount1\tcount2\tcount3\tmax\tmin\tts\n",
                    "ts",
                    false,
                    true
            );

            // index scans
            assertQueryAndPlan(
                    "select ts, i, j, row_number() over () from tab where sym = 'X'",
                    """
                            Window
                              functions: [row_number()]
                                DeferredSingleSymbolFilterPageFrame
                                    Index forward scan on: sym deferred: true
                                      filter: sym='X'
                                    Frame forward scan on: tab
                            """,
                    "ts\ti\tj\trow_number\n",
                    "ts",
                    false,
                    false
            );

            assertQueryAndPlan(
                    "select ts, i, j, row_number() over () from tab where sym = 'X' order by ts desc",
                    """
                            Window
                              functions: [row_number()]
                                DeferredSingleSymbolFilterPageFrame
                                    Index backward scan on: sym deferred: true
                                      filter: sym='X'
                                    Frame backward scan on: tab
                            """,
                    "ts\ti\tj\trow_number\n",
                    "ts###desc",
                    false,
                    false
            );

            assertQueryAndPlan(
                    "select ts, i, j, row_number() over () from tab where sym IN ('X', 'Y') order by sym",
                    """
                            SelectedRecord
                                Window
                                  functions: [row_number()]
                                    FilterOnValues symbolOrder: asc
                                        Cursor-order scan
                                            Index forward scan on: sym deferred: true
                                              filter: sym='X'
                                            Index forward scan on: sym deferred: true
                                              filter: sym='Y'
                                        Frame forward scan on: tab
                            """,
                    "ts\ti\tj\trow_number\n",
                    null,
                    false,
                    false
            );

            // lead(j) over() and lead(j) respect nulls over() are identical (respect nulls is default)
            // so they are deduplicated, resulting in 4 functions instead of 6
            assertQueryAndPlan(
                    "select ts, i, j, lead(j) over(), lag(j) over (), lead(j) ignore nulls over(), lag(j) ignore nulls over (), lead(j) respect nulls over(), lag(j) respect nulls over () from tab where sym = 'X'",
                    """
                            SelectedRecord
                                CachedWindow
                                  unorderedFunctions: [lead(j, 1, NULL) over (),lag(j, 1, NULL) over (),lead(j, 1, NULL) ignore nulls over (),lag(j, 1, NULL) ignore nulls over ()]
                                    DeferredSingleSymbolFilterPageFrame
                                        Index forward scan on: sym deferred: true
                                          filter: sym='X'
                                        Frame forward scan on: tab
                            """,
                    "ts\ti\tj\tlead\tlag\tlead_ignore_nulls\tlag_ignore_nulls\tlead1\tlag1\n",
                    "ts",
                    true,
                    false
            );

            // lead/lag with respect nulls (default) deduplicated with lead/lag without specifier
            assertQueryAndPlan(
                    "select ts, i, j, lead(j) over(), lag(j) over (), lead(j) ignore nulls over(), lag(j) ignore nulls over (), lead(j) respect nulls over(), lag(j) respect nulls over () from tab where sym = 'X' order by ts desc",
                    """
                            SelectedRecord
                                CachedWindow
                                  unorderedFunctions: [lead(j, 1, NULL) over (),lag(j, 1, NULL) over (),lead(j, 1, NULL) ignore nulls over (),lag(j, 1, NULL) ignore nulls over ()]
                                    DeferredSingleSymbolFilterPageFrame
                                        Index backward scan on: sym deferred: true
                                          filter: sym='X'
                                        Frame backward scan on: tab
                            """,
                    "ts\ti\tj\tlead\tlag\tlead_ignore_nulls\tlag_ignore_nulls\tlead1\tlag1\n",
                    "ts###desc",
                    true,
                    false
            );

            // lead/lag with respect nulls (default) deduplicated with lead/lag without specifier
            // Double SelectedRecord: one from ORDER BY sym, one from window function deduplication
            assertQueryAndPlan(
                    "select ts, i, j, lead(j) over(), lag(j) over (), lead(j) ignore nulls over(), lag(j) ignore nulls over (), lead(j) respect nulls over(), lag(j) respect nulls over () from tab where sym IN ('X', 'Y') order by sym",
                    """
                            SelectedRecord
                                SelectedRecord
                                    CachedWindow
                                      unorderedFunctions: [lead(j, 1, NULL) over (),lag(j, 1, NULL) over (),lead(j, 1, NULL) ignore nulls over (),lag(j, 1, NULL) ignore nulls over ()]
                                        FilterOnValues symbolOrder: asc
                                            Cursor-order scan
                                                Index forward scan on: sym deferred: true
                                                  filter: sym='X'
                                                Index forward scan on: sym deferred: true
                                                  filter: sym='Y'
                                            Frame forward scan on: tab
                            """,
                    "ts\ti\tj\tlead\tlag\tlead_ignore_nulls\tlag_ignore_nulls\tlead1\tlag1\n",
                    null,
                    true,
                    false
            );
        });
    }

    @Test
    public void testWindowFirstValue() throws Exception {
        // Test first_value() window function
        assertQuery(
                """
                        id\tfirst_val
                        1\t1
                        2\t1
                        3\t1
                        4\t1
                        5\t1
                        """,
                "SELECT id, first_value(id) OVER (ORDER BY ts) AS first_val FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT x AS id, timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(5)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,
                true
        );
    }

    @Test
    public void testWindowFrameRowsBetween() throws Exception {
        // Test window function with ROWS BETWEEN frame specification
        // Note: QuestDB only supports PRECEDING and CURRENT ROW for frame end
        assertQuery(
                """
                        id\tmoving_sum
                        1\t1.0
                        2\t3.0
                        3\t5.0
                        4\t7.0
                        5\t9.0
                        """,
                "SELECT id, sum(id) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS moving_sum FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT x AS id, timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(5)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,
                true
        );
    }

    @Test
    public void testWindowFunctionArithmeticAsArgumentToAggregate() throws Exception {
        // Test arithmetic of window functions as argument to regular aggregate
        // sum(sum(id) OVER () + sum(id) OVER ()) should:
        // 1. Compute sum(id) OVER () for each row: 6 (1+2+3)
        // 2. Add them: 6 + 6 = 12 for each row
        // 3. Sum all those values: 12+12+12 = 36
        assertQuery(
                """
                        result
                        36.0
                        """,
                "SELECT sum(sum(id) OVER () + sum(id) OVER ()) AS result FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT x as id, timestamp_sequence('2024-01-01', 1000000) AS ts FROM long_sequence(3)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,
                true
        );
    }

    @Test
    public void testWindowFunctionArithmeticInsideFunction() throws Exception {
        // Test two window functions in arithmetic expression inside abs()
        // This is the original bug case
        assertQuery(
                """
                        result
                        0.0
                        0.0
                        0.0
                        """,
                "SELECT abs(sum(x) OVER () - sum(x) OVER ()) AS result FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT x, timestamp_sequence('2024-01-01', 1000000) AS ts FROM long_sequence(3)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                true,
                true
        );
    }

    @Test
    public void testWindowFunctionArithmeticWithOrderBy() throws Exception {
        // Test window function with ORDER BY and arithmetic: row_number() + 1
        assertQuery(
                """
                        adjusted_rank
                        2
                        3
                        4
                        5
                        6
                        """,
                "SELECT row_number() OVER (ORDER BY ts) + 1 AS adjusted_rank FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(5)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,
                true
        );
    }

    @Test
    public void testWindowFunctionAsArgumentToAggregate() throws Exception {
        // Test window function as argument to regular aggregate (not window function)
        // sum(row_number() OVER (order by ts)) should:
        // 1. Compute row_number() for each row ordered by ts: 1, 2, 3
        // 2. Sum all those values: 1+2+3 = 6
        // 3. Return single aggregated result
        assertQuery(
                """
                        result
                        6
                        """,
                "SELECT sum(row_number() OVER (order by ts)) AS result FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT timestamp_sequence('2024-01-01', 1000000) AS ts FROM long_sequence(3)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,
                true
        );
    }

    @Test
    public void testWindowFunctionAsArgumentToCast() throws Exception {
        // Test window function as direct argument to cast() function
        assertQuery(
                """
                        result
                        1
                        2
                        3
                        """,
                "SELECT cast(row_number() OVER (ORDER BY ts) AS int) AS result FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT timestamp_sequence('2024-01-01', 1000000) AS ts FROM long_sequence(3)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,
                true
        );
    }

    @Test
    public void testWindowFunctionAsArgumentToFunction() throws Exception {
        // Test window function as direct argument to abs() function
        assertQuery(
                """
                        result
                        1
                        2
                        3
                        """,
                "SELECT abs(row_number() OVER (ORDER BY ts)) AS result FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT timestamp_sequence('2024-01-01', 1000000) AS ts FROM long_sequence(3)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,
                true
        );
    }

    @Test
    public void testWindowFunctionAsArgumentToWindowFunction() throws Exception {
        // Test window function as argument to another window function
        // sum(row_number() OVER ()) OVER () should:
        // 1. Compute row_number() for each row: 1, 2, 3
        // 2. Sum all those values: 1+2+3 = 6
        // 3. Return 6 for each row
        assertQuery(
                """
                        result
                        6.0
                        6.0
                        6.0
                        """,
                "SELECT sum(row_number() OVER ()) OVER () AS result FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT timestamp_sequence('2024-01-01', 1000000) AS ts FROM long_sequence(3)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                true,
                true
        );
    }

    @Test
    public void testWindowFunctionCastToInt() throws Exception {
        // Test window function cast to int
        assertQuery(
                """
                        rn
                        1
                        2
                        3
                        """,
                "SELECT row_number() OVER (ORDER BY ts)::int AS rn FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT timestamp_sequence('2024-01-01', 1000000) AS ts FROM long_sequence(3)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,
                true
        );
    }

    @Test
    public void testWindowFunctionContextCleanup() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table trades as " +
                    "(" +
                    "select" +
                    " rnd_int(1,2,3) price," +
                    " rnd_symbol('AA','BB','CC') symbol," +
                    (timestampType == TestTimestampType.MICRO ? " timestamp_sequence(0, 100000000000) ts" : " timestamp_sequence_ns(0, 100000000000000) ts") +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by day", sqlExecutionContext);

            assertQueryNoLeakCheck(
                    """
                            symbol\tprice\trow_number
                            BB\t1\t1
                            CC\t2\t2
                            AA\t2\t1
                            CC\t1\t1
                            BB\t2\t2
                            """,
                    "select symbol, price, row_number() over (partition by symbol order by price) " +
                            "from trades",
                    null,
                    true,
                    true
            );

            // WindowContext should be properly clean up when we try to execute the next query.
            for (String function : WINDOW_ONLY_FUNCTIONS) {
                if (function.contains("ignore nulls") || function.contains("respect nulls")) {
                    try {
                        execute("select #FUNCTION from trades".replace("#FUNCTION", function), sqlExecutionContext);
                        Assert.fail();
                    } catch (SqlException e) {
                        Assert.assertEquals(38, e.getPosition());
                        TestUtils.assertContains(e.getFlyweightMessage(), "'over' expected after 'nulls'");
                    }
                } else {
                    try {
                        execute("select #FUNCTION from trades".replace("#FUNCTION", function), sqlExecutionContext);
                        Assert.fail();
                    } catch (SqlException e) {
                        Assert.assertEquals(7, e.getPosition());
                        TestUtils.assertContains(e.getFlyweightMessage(), "window function called in non-window context, make sure to add OVER clause");
                    }
                }
            }
        });
    }

    @Test
    public void testWindowFunctionDoesSortIfOrderByIsNotCompatibleWithBaseQuery() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, j long, sym symbol index) timestamp(ts)", timestampType.getTypeName());

            for (String func : FRAME_FUNCTIONS) {
                String replace = func.trim().replace("#COLUMN", "1");
                if (replace.equals("count(1)")) {
                    replace = "count(*)";
                }
                replace = replace.replace(" respect nulls", "");

                assertPlanNoLeakCheck(
                        "select ts, i, j, #FUNCT_NAME over (partition by i order by ts desc rows between 1 preceding and current row) from tab".replace("#FUNCT_NAME", func).replace("#COLUMN", "1"),
                        "CachedWindow\n" +
                                "  orderedFunctions: [[ts desc] => [#FUNCT_NAME(1) over (partition by [i] rows between 1 preceding and current row)]]\n".replace("#FUNCT_NAME(1)", replace) +
                                "    PageFrame\n" +
                                "        Row forward scan\n" +
                                "        Frame forward scan on: tab\n"
                );

                assertPlanNoLeakCheck(
                        "select ts, i, j, #FUNCT_NAME over (partition by i order by ts asc rows between 1 preceding and current row)  from tab order by ts desc".replace("#FUNCT_NAME", func).replace("#COLUMN", "1"),
                        "CachedWindow\n" +
                                "  orderedFunctions: [[ts] => [#FUNCT_NAME(1) over (partition by [i] rows between 1 preceding and current row)]]\n".replace("#FUNCT_NAME(1)", replace) +
                                "    PageFrame\n" +
                                "        Row backward scan\n" +
                                "        Frame backward scan on: tab\n"
                );

                //TODO: inspect
                assertPlanNoLeakCheck(
                        "select ts, i, j, #FUNCT_NAME over (partition by i order by ts asc rows between 1 preceding and current row) from tab where sym in ( 'A', 'B') ".replace("#FUNCT_NAME", func).replace("#COLUMN", "1"),
                        func.contains("first_value") || func.contains("last_value") ?
                                "Window\n" +
                                        "  functions: [#FUNCT_NAME(1) over (partition by [i] rows between 1 preceding and current row)]\n".replace("#FUNCT_NAME(1)", replace) +
                                        "    FilterOnValues\n" +
                                        "        Table-order scan\n" +
                                        "            Index forward scan on: sym deferred: true\n" +
                                        "              filter: sym='B'\n" +
                                        "            Index forward scan on: sym deferred: true\n" +
                                        "              filter: sym='A'\n" +
                                        "        Frame forward scan on: tab\n"
                                :
                                "CachedWindow\n" +
                                        "  orderedFunctions: [[ts] => [#FUNCT_NAME(1) over (partition by [i] rows between 1 preceding and current row)]]\n".replace("#FUNCT_NAME(1)", replace) +
                                        "    FilterOnValues symbolOrder: desc\n" +
                                        "        Cursor-order scan\n" +
                                        "            Index forward scan on: sym deferred: true\n" +
                                        "              filter: sym='B'\n" +
                                        "            Index forward scan on: sym deferred: true\n" +
                                        "              filter: sym='A'\n" +
                                        "        Frame forward scan on: tab\n"

                );

                assertPlanNoLeakCheck(
                        "select ts, i, j, #FUNCT_NAME over (partition by i order by ts desc rows between 1 preceding and current row)  from tab where sym = 'A'".replace("#FUNCT_NAME", func).replace("#COLUMN", "1"),
                        "CachedWindow\n" +
                                "  orderedFunctions: [[ts desc] => [#FUNCT_NAME(1) over (partition by [i] rows between 1 preceding and current row)]]\n".replace("#FUNCT_NAME(1)", replace) +
                                "    DeferredSingleSymbolFilterPageFrame\n" +
                                "        Index forward scan on: sym deferred: true\n" +
                                "          filter: sym='A'\n" +
                                "        Frame forward scan on: tab\n"
                );
            }
        });
    }

    @Test
    public void testWindowFunctionDoesntSortIfOrderByIsCompatibleWithBaseQuery() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, j long, sym symbol index) timestamp(ts)", timestampType.getTypeName());

            for (String func : FRAME_FUNCTIONS) {
                String replace = func.trim().replace("#COLUMN", "1");
                if (replace.equals("count(1)")) {
                    replace = "count(*)";
                }
                replace = replace.replace(" respect nulls", "");

                assertPlanNoLeakCheck(
                        "select ts, i, j, #FUNCT_NAME over (partition by i order by ts rows between 1 preceding and current row) from tab".replace("#FUNCT_NAME", func).replace("#COLUMN", "1"),
                        "Window\n" +
                                "  functions: [#FUNCT_NAME(1) over (partition by [i] rows between 1 preceding and current row)]\n".replace("#FUNCT_NAME(1)", replace) +
                                "    PageFrame\n" +
                                "        Row forward scan\n" +
                                "        Frame forward scan on: tab\n"
                );

                assertPlanNoLeakCheck(
                        "select ts, i, j, #FUNCT_NAME over (partition by i order by ts rows between 1 preceding and current row)  from tab order by ts asc".replace("#FUNCT_NAME", func).replace("#COLUMN", "1"),
                        "Window\n" +
                                "  functions: [#FUNCT_NAME(1) over (partition by [i] rows between 1 preceding and current row)]\n".replace("#FUNCT_NAME(1)", replace) +
                                "    PageFrame\n" +
                                "        Row forward scan\n" +
                                "        Frame forward scan on: tab\n"
                );

                assertPlanNoLeakCheck(
                        "select ts, i, j, #FUNCT_NAME over (partition by i order by ts desc rows between 1 preceding and current row)  from tab order by ts desc".replace("#FUNCT_NAME", func).replace("#COLUMN", "1"),
                        "Window\n" +
                                "  functions: [#FUNCT_NAME(1) over (partition by [i] rows between 1 preceding and current row)]\n".replace("#FUNCT_NAME(1)", replace) +
                                "    PageFrame\n" +
                                "        Row backward scan\n" +
                                "        Frame backward scan on: tab\n"
                );

                assertPlanNoLeakCheck(
                        "select ts, i, j, #FUNCT_NAME over (partition by i order by ts asc rows between 1 preceding and current row)  from tab where sym = 'A'".replace("#FUNCT_NAME", func).replace("#COLUMN", "1"),
                        "Window\n" +
                                "  functions: [#FUNCT_NAME(1) over (partition by [i] rows between 1 preceding and current row)]\n".replace("#FUNCT_NAME(1)", replace) +
                                "    DeferredSingleSymbolFilterPageFrame\n" +
                                "        Index forward scan on: sym deferred: true\n" +
                                "          filter: sym='A'\n" +
                                "        Frame forward scan on: tab\n"
                );

                assertPlanNoLeakCheck(
                        "select ts, i, j, #FUNCT_NAME over (partition by i order by ts asc rows between 1 preceding and current row) ".replace("#FUNCT_NAME", func).replace("#COLUMN", "1") +
                                "from tab where sym in ( 'A', 'B') order by ts asc",
                        "Window\n" +
                                "  functions: [#FUNCT_NAME(1) over (partition by [i] rows between 1 preceding and current row)]\n".replace("#FUNCT_NAME(1)", replace) +
                                "    FilterOnValues\n" +
                                "        Table-order scan\n" +
                                "            Index forward scan on: sym deferred: true\n" +
                                "              filter: sym='A'\n" +
                                "            Index forward scan on: sym deferred: true\n" +
                                "              filter: sym='B'\n" +
                                "        Frame forward scan on: tab\n"
                );
            }
        });
    }

    @Test
    public void testWindowFunctionFailsInNonWindowContext() throws Exception {
        assertMemoryLeak(() -> {
            Class<?>[] factories = new Class<?>[]{
                    RankFunctionFactory.class,
                    DenseRankFunctionFactory.class,
                    RowNumberFunctionFactory.class,
                    AvgDoubleWindowFunctionFactory.class,
                    SumDoubleWindowFunctionFactory.class,
                    KSumDoubleWindowFunctionFactory.class,
                    CountConstWindowFunctionFactory.class,
                    CountDoubleWindowFunctionFactory.class,
                    CountSymbolWindowFunctionFactory.class,
                    CountVarcharWindowFunctionFactory.class,
                    MaxDoubleWindowFunctionFactory.class,
                    MinDoubleWindowFunctionFactory.class,
                    FirstValueDoubleWindowFunctionFactory.class,
                    LastValueDoubleWindowFunctionFactory.class,
                    LagDoubleFunctionFactory.class,
                    LeadDoubleFunctionFactory.class,
                    LagLongFunctionFactory.class,
                    LeadLongFunctionFactory.class,
                    LagTimestampFunctionFactory.class,
                    LeadTimestampFunctionFactory.class,
                    LagDateFunctionFactory.class,
                    LeadDateFunctionFactory.class
            };

            int position = -1;
            ObjList<Function> args = new ObjList<>();
            IntList argPositions = new IntList();

            for (Class<?> _class : factories) {
                FunctionFactory factory = (FunctionFactory) _class.getDeclaredConstructor().newInstance();

                try {
                    factory.newInstance(position, args, argPositions, configuration, sqlExecutionContext);
                    Assert.fail();
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getMessage(), "window function called in non-window context, make sure to add OVER clause");
                }
            }
        });
    }

    @Test
    public void testWindowFunctionInCaseDifferentOverSpecs() throws Exception {
        // Window functions with different OVER specs in THEN vs ELSE branches
        assertQuery(
                """
                        id\tresult
                        1\t1.0
                        2\t3.0
                        3\t6.0
                        4\t4.0
                        5\t5.0
                        """,
                "SELECT id, CASE " +
                        "  WHEN id <= 3 THEN sum(id) OVER (ORDER BY ts) " +
                        "  ELSE sum(id) OVER (PARTITION BY id ORDER BY ts) " +
                        "END AS result " +
                        "FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT x AS id, timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(5)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,
                true
        );
    }

    @Test
    public void testWindowFunctionInCaseExpression() throws Exception {
        // Test window function directly inside CASE WHEN condition
        assertQuery(
                """
                        category
                        first
                        middle
                        last
                        """,
                "SELECT CASE " +
                        "  WHEN row_number() OVER (ORDER BY ts) = 1 THEN 'first' " +
                        "  WHEN row_number() OVER (ORDER BY ts) = 3 THEN 'last' " +
                        "  ELSE 'middle' " +
                        "END AS category " +
                        "FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT timestamp_sequence('2024-01-01', 1000000) AS ts FROM long_sequence(3)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,
                true
        );
    }

    @Test
    public void testWindowFunctionInCaseThenAndElse() throws Exception {
        // Window functions in both THEN and ELSE with different functions
        assertQuery(
                """
                        id\tresult
                        1\t1
                        2\t2
                        3\t3
                        4\t3
                        5\t4
                        """,
                "SELECT id, CASE " +
                        "  WHEN id <= 3 THEN row_number() OVER (ORDER BY ts) " +
                        "  ELSE lag(id) OVER (ORDER BY ts) " +
                        "END AS result " +
                        "FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT x AS id, timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(5)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,
                true
        );
    }

    @Test
    public void testWindowFunctionInCaseWithCast() throws Exception {
        // Window function with cast inside CASE expression
        assertQuery(
                """
                        id\tresult
                        1\t1
                        2\t2
                        3\t3
                        4\tN/A
                        5\tN/A
                        """,
                "SELECT id, CASE " +
                        "  WHEN id <= 3 THEN row_number() OVER (ORDER BY ts)::string " +
                        "  ELSE 'N/A' " +
                        "END AS result " +
                        "FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT x AS id, timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(5)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,
                true
        );
    }

    @Test
    public void testWindowFunctionInPartitionByFails() throws Exception {
        assertException(
                """
                        SELECT pickup_datetime, row_number() OVER (PARTITION BY row_number())
                        FROM trips
                        WHERE pickup_datetime >= '2018-12-30' and pickup_datetime <= '2018-12-31'""",
                "create table trips as " +
                        "(" +
                        "select" +
                        " rnd_double(42) total_amount," +
                        (timestampType == TestTimestampType.MICRO ? " timestamp_sequence(0, 100000000000) pickup_datetime" : " timestamp_sequence_ns(0, 100000000000000) pickup_datetime") +
                        " from long_sequence(10)" +
                        ") timestamp(pickup_datetime) partition by day",
                56,
                "window function called in non-window context, make sure to add OVER clause"
        );
    }

    @Test
    public void testWindowFunctionLagMinusLead() throws Exception {
        // Test lag() - lead() window function arithmetic
        assertQuery(
                """
                        diff
                        null
                        -2
                        -2
                        null
                        """,
                "SELECT lag(id) OVER (ORDER BY ts) - lead(id) OVER (ORDER BY ts) AS diff FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT x AS id, timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(4)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                true,
                true
        );
    }

    @Test
    public void testWindowFunctionMultipleInSelect() throws Exception {
        // Test multiple window functions in select with different operations
        assertQuery(
                """
                        id\trunning_sum\tprev_id\tdiff
                        1\t1.0\tnull\tnull
                        2\t3.0\t1\t2.0
                        3\t6.0\t2\t4.0
                        4\t10.0\t3\t7.0
                        5\t15.0\t4\t11.0
                        """,
                "SELECT " +
                        "  id, " +
                        "  sum(id) OVER (ORDER BY ts) AS running_sum, " +
                        "  lag(id) OVER (ORDER BY ts) AS prev_id, " +
                        "  sum(id) OVER (ORDER BY ts) - lag(id) OVER (ORDER BY ts) AS diff " +
                        "FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT x AS id, timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(5)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,
                true
        );
    }

    @Test
    public void testWindowFunctionNestedParenthesesWithCast() throws Exception {
        // Deeply nested parentheses with cast
        assertQuery(
                """
                        rn
                        1
                        2
                        3
                        """,
                "SELECT (((row_number() OVER (ORDER BY ts))))::int AS rn FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT timestamp_sequence('2024-01-01', 1000000) AS ts FROM long_sequence(3)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,
                true
        );
    }

    @Test
    public void testWindowFunctionReleaseNativeMemory() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, val long) timestamp(ts)", timestampType.getTypeName());
            execute("insert into tab select x::timestamp, x from long_sequence(10)");

            final String EXPRESSION = "rnd_str(100,100,100,10)";//chosen because it allocates native memory that needs to be freed

            List<String> frameTypes = Arrays.asList("rows", "range");
            List<String> frameVariants = Arrays.asList(
                    // without partition keys
                    "over ()", // whole result set
                    "over ( #ORDERBY #FRAME current row)",
                    "over ( #ORDERBY #FRAME between unbounded preceding and 1 preceding)",
                    "over ( #ORDERBY #FRAME between unbounded preceding and current row)",
                    "over ( #ORDERBY #FRAME between 10 preceding and 1 preceding)",
                    "over ( #ORDERBY #FRAME between 10 preceding and current row)",
                    // with partition keys
                    "over ( partition by #EXPRESSION )", //whole partition
                    "over ( partition by #EXPRESSION #ORDERBY #FRAME current row)",
                    "over ( partition by #EXPRESSION #ORDERBY #FRAME between unbounded preceding and 1 preceding)",
                    "over ( partition by #EXPRESSION #ORDERBY #FRAME between unbounded preceding and current row)",
                    "over ( partition by #EXPRESSION #ORDERBY #FRAME between 10 preceding and 1 preceding)",
                    "over ( partition by #EXPRESSION #ORDERBY #FRAME between 10 preceding and current row)"
            );

            List<String> orderByVariants = Arrays.asList(
                    "", //only allowed for rows frame
                    "order by val",
                    "order by val, ts",
                    "order by ts",
                    "order by ts desc"
                    //, "order by " + EXPRESSION
            );

            for (String function : FRAME_FUNCTIONS) {
                for (String frameType : frameTypes) {
                    for (String frameVariant : frameVariants) {
                        for (String orderBy : orderByVariants) {
                            if ("range".equals(frameType) && !orderBy.startsWith("order by ts")) {
                                continue;//range frame requires order by timestamp
                            }

                            String query = "select count(*) from (" +
                                    ("select *, #FUNCTION " + frameVariant + " from tab ")
                                            .replace("#FUNCTION", function)
                                            .replace("#COLUMN", "length(#EXPRESSION)")
                                            .replace("#EXPRESSION", EXPRESSION)
                                            .replace("#FRAME", frameType)
                                            .replace("#ORDERBY", orderBy);

                            if (Chars.equals("order by ts desc", orderBy)) {
                                query += orderBy + ")";
                            } else {
                                query += ")";
                            }

                            try {
                                assertSql("count\n10\n", query);
                            } catch (Exception e) {
                                throw new AssertionError("Failed for query: " + query, e);
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testWindowFunctionUnsupportNulls() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table tab (ts #TIMESTAMP, i long, j long, s symbol, d double, c VARCHAR) timestamp(ts)", timestampType.getTypeName());

            for (int i = 0, size = FRAME_FUNCTIONS.size(); i < size; i++) {
                String func = FRAME_FUNCTIONS.get(i);
                for (String column : FRAME_FUNCTIONS_PARAMETER_COLUMN_NAME[i]) {
                    if (!func.contains("first_value") && !func.contains("last_value")) {
                        String query1 = "select #FUNCT_NAME IGNORE NULLS over () from tab".replace("#FUNCT_NAME", func).replace("#COLUMN", column);
                        assertExceptionNoLeakCheck(
                                query1,
                                36,
                                "RESPECT/IGNORE NULLS is not supported for current window function"
                        );

                        String query2 = "select #FUNCT_NAME RESPECT NULLS over () from tab".replace("#FUNCT_NAME", func).replace("#COLUMN", column);
                        assertExceptionNoLeakCheck(
                                query2,
                                36,
                                "RESPECT/IGNORE NULLS is not supported for current window function"
                        );
                    }
                }
            }
        });
    }

    @Test
    public void testWindowFunctionWithArithmeticInCaseBranches() throws Exception {
        // Window function with arithmetic operations in different CASE branches
        assertQuery(
                """
                        id\tresult
                        1\t2
                        2\t3
                        3\t4
                        4\t3
                        5\t4
                        """,
                "SELECT id, CASE " +
                        "  WHEN id <= 3 THEN row_number() OVER (ORDER BY ts) + 1 " +
                        "  ELSE row_number() OVER (ORDER BY ts) - 1 " +
                        "END AS result " +
                        "FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT x AS id, timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(5)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,
                true
        );
    }

    @Test
    public void testWindowFunctionWithCast() throws Exception {
        // Test window function with :: cast operator
        assertQuery(
                """
                        rn
                        1
                        2
                        3
                        """,
                "SELECT row_number() OVER (ORDER BY ts)::string AS rn FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT timestamp_sequence('2024-01-01', 1000000) AS ts FROM long_sequence(3)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,  // window functions don't support random access
                true
        );
    }

    @Test
    public void testWindowFunctionWithCastInParentheses() throws Exception {
        // Test window function with cast and outer parentheses
        assertQuery(
                """
                        rn
                        1
                        2
                        3
                        """,
                "SELECT (row_number() OVER (ORDER BY ts))::string AS rn FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT timestamp_sequence('2024-01-01', 1000000) AS ts FROM long_sequence(3)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,
                true
        );
    }

    @Test
    public void testWindowFunctionWithLimit() throws Exception {
        // Test window function with LIMIT clause
        assertQuery(
                """
                        rn
                        1
                        2
                        """,
                "SELECT row_number() OVER (ORDER BY ts) AS rn FROM x LIMIT 2",
                "CREATE TABLE x AS (" +
                        "SELECT timestamp_sequence('2024-01-01', 1000000) AS ts FROM long_sequence(5)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,
                true
        );
    }

    @Test
    public void testWindowFunctionWithNullHandling() throws Exception {
        // Test window functions with null values in arithmetic
        assertQuery(
                """
                        id\tprev\tdiff
                        1\tnull\tnull
                        null\t1\tnull
                        3\tnull\tnull
                        4\t3\t1
                        """,
                "SELECT id, lag(id) OVER (ORDER BY ts) AS prev, id - lag(id) OVER (ORDER BY ts) AS diff FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT " +
                        "  CASE WHEN x = 2 THEN NULL ELSE x END AS id, " +
                        "  timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(4)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,
                true
        );
    }

    @Test
    public void testWindowFunctionWithPartitionBy() throws Exception {
        // Test window function with PARTITION BY and cast
        assertQuery(
                """
                        sym\trn
                        A\t1
                        A\t2
                        B\t1
                        B\t2
                        """,
                "SELECT sym, row_number() OVER (PARTITION BY sym ORDER BY ts)::string AS rn FROM x ORDER BY sym, ts",
                "CREATE TABLE x AS (" +
                        "SELECT " +
                        "  CASE WHEN x <= 2 THEN 'A' ELSE 'B' END AS sym, " +
                        "  timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(4)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                true,
                true
        );
    }

    @Test
    public void testWindowMultiplePartitionBy() throws Exception {
        // Test window function with multiple PARTITION BY columns
        assertQuery(
                """
                        a\tb\trn
                        1\tX\t1
                        1\tX\t2
                        1\tY\t1
                        2\tX\t1
                        2\tY\t1
                        """,
                "SELECT a, b, row_number() OVER (PARTITION BY a, b ORDER BY ts) AS rn FROM x ORDER BY a, b, ts",
                "CREATE TABLE x AS (" +
                        "SELECT " +
                        "  CASE WHEN x <= 3 THEN 1 ELSE 2 END AS a, " +
                        "  CASE WHEN x IN (1, 2, 4) THEN 'X' ELSE 'Y' END AS b, " +
                        "  timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(5)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                true,
                true
        );
    }

    @Test
    public void testWindowOnNestWithMultiArgsFunction() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE x ( timestamp #TIMESTAMP, ticker SYMBOL, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, true_range DOUBLE ) TIMESTAMP(timestamp) PARTITION BY MONTH;", timestampType.getTypeName());
            execute("INSERT INTO x VALUES " +
                    "('2021-01-01 09:30:00', 'SPY', 370.00, 370.50, 369.50, 370.25, 100)," +
                    "('2021-01-02 09:30:00', 'SPY', 370.25, 370.75, 369.75, 370.50, 200)," +
                    "('2021-01-03 09:30:00', 'SPY', 370.50, 371.00, 370.00, 370.75, 300)," +
                    "('2021-01-04 09:30:00', 'SPY', 370.75, 371.25, 370.25, 371.00, 400)," +
                    "('2021-01-05 09:30:00', 'SPY', 371.00, 371.50, 370.50, 371.25, 500)," +
                    "('2021-01-06 09:30:00', 'SPY', 371.25, 371.75, 370.75, 371.50, 600)," +
                    "('2021-01-07 09:30:00', 'SPY', 371.50, 372.00, 371.00, 371.75, 700)," +
                    "('2021-01-08 09:30:00', 'SPY', 371.75, 372.25, 371.25, 372.00, 800)," +
                    "('2021-01-09 09:30:00', 'SPY', 372.00, 372.50, 371.50, 372.25, 900)," +
                    "('2021-01-10 09:30:00', 'SPY', 372.25, 372.75, 371.75, 372.50, 1000)," +
                    "('2021-01-11 09:30:00', 'SPY', 372.50, 373.00, 372.00, 372.75, 1100)," +
                    "('2021-01-12 09:30:00', 'SPY', 372.75, 373.25, 372.25, 373.00, 1200)," +
                    "('2021-01-13 09:30:00', 'SPY', 373.00, 373.50, 372.50, 373.25, 1300)," +
                    "('2021-01-14 09:30:00', 'SPY', 373.00, 373.50, 372.50, 373.25, 1400);");
            drainWalQueue();
            assertSql(replaceTimestampSuffix("""
                            rn\tticker\ttimestamp\topen\thigh\tlow\tclose\tatr
                            14\tSPY\t2021-01-14T09:30:00.000000Z\t373.0\t373.5\t372.5\t373.25\t1.0
                            13\tSPY\t2021-01-13T09:30:00.000000Z\t373.0\t373.5\t372.5\t373.25\t1.0
                            12\tSPY\t2021-01-12T09:30:00.000000Z\t372.75\t373.25\t372.25\t373.0\t1.0
                            11\tSPY\t2021-01-11T09:30:00.000000Z\t372.5\t373.0\t372.0\t372.75\t1.0
                            10\tSPY\t2021-01-10T09:30:00.000000Z\t372.25\t372.75\t371.75\t372.5\t1.0
                            9\tSPY\t2021-01-09T09:30:00.000000Z\t372.0\t372.5\t371.5\t372.25\t1.0
                            8\tSPY\t2021-01-08T09:30:00.000000Z\t371.75\t372.25\t371.25\t372.0\t1.0
                            7\tSPY\t2021-01-07T09:30:00.000000Z\t371.5\t372.0\t371.0\t371.75\t1.0
                            6\tSPY\t2021-01-06T09:30:00.000000Z\t371.25\t371.75\t370.75\t371.5\t1.0
                            5\tSPY\t2021-01-05T09:30:00.000000Z\t371.0\t371.5\t370.5\t371.25\t1.0
                            4\tSPY\t2021-01-04T09:30:00.000000Z\t370.75\t371.25\t370.25\t371.0\t1.0
                            3\tSPY\t2021-01-03T09:30:00.000000Z\t370.5\t371.0\t370.0\t370.75\t1.0
                            2\tSPY\t2021-01-02T09:30:00.000000Z\t370.25\t370.75\t369.75\t370.5\t1.0
                            1\tSPY\t2021-01-01T09:30:00.000000Z\t370.0\t370.5\t369.5\t370.25\t1.0
                            """),
                    "WITH true_ranges AS " +
                            "( SELECT rn, ticker, timestamp, open, high, low, close, high-low AS day_range, avg_14_bar_range, " +
                            "greatest(high-low, abs(high-prev_close), abs(low-prev_close)) as true_range FROM " +
                            "( SELECT timestamp, ticker, open, high, low, close, row_number() OVER (PARTITION BY ticker ORDER BY timestamp) as rn, " +
                            "avg(high - low) OVER (PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) as avg_14_bar_range, " +
                            "LAG(close) OVER (PARTITION BY ticker ORDER BY timestamp) AS prev_close, FROM x WHERE ticker = 'SPY' ))" +
                            "SELECT rn, ticker, timestamp, open, high, low, close, atr FROM ( " +
                            "SELECT rn, ticker, timestamp, open, high, low, close, avg(true_range) OVER " +
                            "(PARTITION BY ticker ORDER BY timestamp ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS atr FROM true_ranges ) ORDER BY ticker, timestamp DESC;");
        });
    }

    @Test
    public void testWindowOnlyFunctionFailsInNonWindowContext() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table trades as " +
                    "(" +
                    "select" +
                    " rnd_int(1,2,3) price," +
                    " rnd_symbol('AA','BB','CC') symbol," +
                    (timestampType == TestTimestampType.MICRO ?
                            " timestamp_sequence(0, 100000000000) ts," : " timestamp_sequence_ns(0, 100000000000000) ts,") +
                    " from long_sequence(10)" +
                    ") timestamp(ts) partition by day");


            for (String function : WINDOW_ONLY_FUNCTIONS) {
                if (function.contains("ignore nulls") || function.contains("respect nulls")) {
                    try {
                        execute("select #FUNCTION from trades".replace("#FUNCTION", function), sqlExecutionContext);
                        Assert.fail();
                    } catch (SqlException e) {
                        Assert.assertEquals(38, e.getPosition());
                        TestUtils.assertContains(e.getFlyweightMessage(), "'over' expected");
                    }
                } else {
                    assertExceptionNoLeakCheck(
                            "select #FUNCT_NAME, * from trades".replace("#FUNCT_NAME", function),
                            7,
                            "window function called in non-window context, make sure to add OVER clause"
                    );
                }
            }
        });
    }

    @Test
    public void testWindowOrderByDesc() throws Exception {
        // Test window function with ORDER BY DESC
        assertQuery(
                """
                        id\trn
                        1\t5
                        2\t4
                        3\t3
                        4\t2
                        5\t1
                        """,
                "SELECT id, row_number() OVER (ORDER BY ts DESC) AS rn FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT x AS id, timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(5)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                true,
                true
        );
    }

    @Test
    public void testWindowRank() throws Exception {
        // Test rank() window function with ties
        assertQuery(
                """
                        val\trnk
                        1\t1
                        1\t1
                        2\t3
                        3\t4
                        3\t4
                        """,
                "SELECT val, rank() OVER (ORDER BY val) AS rnk FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT " +
                        "  CASE WHEN x IN (1, 2) THEN 1 WHEN x = 3 THEN 2 ELSE 3 END AS val, " +
                        "  timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(5)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                true,
                true
        );
    }

    private static void normalizeSuffix(List<String> values) {
        int maxLength = 0;
        for (int i = 0, n = values.size(); i < n; i++) {
            int len = values.get(i).length();
            if (len > maxLength) {
                maxLength = len;
            }
        }

        for (int i = 0, n = values.size(); i < n; i++) {
            String function = values.get(i);
            if (function.length() < maxLength) {
                StringSink sink = Misc.getThreadLocalSink();
                sink.put(function);
                for (int j = 0, k = maxLength - function.length(); j < k; j++) {
                    sink.put(' ');
                }
                function = sink.toString();
            }
            values.set(i, function);
        }
    }

    private void assertWindowException(String query, int position, CharSequence errorMessage) throws Exception {
        for (String frameType : FRAME_TYPES) {
            assertExceptionNoLeakCheck(query.replace("#FRAME", frameType), position, errorMessage);
        }
    }

    private String replacePlanTimestamp(String expected) {
        return timestampType == TestTimestampType.NANO ? expected.replaceAll("10000000", "10000000000") : expected;
    }

    private String replaceTimestampSuffix(String expected) {
        return timestampType == TestTimestampType.NANO ? expected.replaceAll("Z\t", "000Z\t").replaceAll("Z\n", "000Z\n") : expected;
    }

    private String replaceTimestampSuffix1(String expected) {
        return timestampType == TestTimestampType.NANO ? expected.replaceAll("0.0000", "0.0000000") : expected;
    }

    protected void assertQueryAndPlan(String query, String plan, String expectedResult, String expectedTimestamp, boolean supportsRandomAccess, boolean expectSize) throws Exception {
        assertPlanNoLeakCheck(query, plan);

        assertQueryNoLeakCheck(
                expectedResult,
                query,
                expectedTimestamp,
                supportsRandomAccess,
                expectSize
        );
    }

    // =====================================================
    // WINDOW clause tests (named window definitions)
    // =====================================================

    @Test
    public void testNamedWindowBasic() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x int, y int, ts timestamp) timestamp(ts)");
            execute("insert into t values (1, 10, 0)");
            execute("insert into t values (2, 20, 1000000)");
            execute("insert into t values (3, 30, 2000000)");

            // Basic named window - empty window (whole table as partition)
            assertQueryNoLeakCheck(
                    "x\ty\trow_num\n" +
                            "1\t10\t1\n" +
                            "2\t20\t2\n" +
                            "3\t30\t3\n",
                    "SELECT x, y, row_number() OVER w as row_num " +
                            "FROM t " +
                            "WINDOW w AS ()",
                    null,
                    false,  // window functions don't support random access
                    true
            );

            // Verify inline window works correctly (baseline)
            assertQueryNoLeakCheck(
                    "x\ty\tsum\n" +
                            "1\t10\t10.0\n" +
                            "2\t20\t30.0\n" +
                            "3\t30\t60.0\n",
                    "SELECT x, y, sum(y) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as sum " +
                            "FROM t",
                    null,
                    true,
                    true
            );

            // Named window with ORDER BY using SELECT column
            assertQueryNoLeakCheck(
                    "x\ty\tsum\n" +
                            "1\t10\t10.0\n" +
                            "2\t20\t30.0\n" +
                            "3\t30\t60.0\n",
                    "SELECT x, y, sum(y) OVER w as sum " +
                            "FROM t " +
                            "WINDOW w AS (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
                    null,
                    true,  // ORDER BY window functions support random access
                    true
            );
        });
    }

    @Test
    public void testNamedWindowWithPartitionBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x int, y int, category symbol, ts timestamp) timestamp(ts)");
            execute("insert into t values (1, 10, 'A', 0)");
            execute("insert into t values (2, 20, 'B', 1000000)");
            execute("insert into t values (3, 30, 'A', 2000000)");
            execute("insert into t values (4, 40, 'B', 3000000)");

            // Use ORDER BY x since x is in SELECT list
            // Note: ORDER BY columns not in SELECT list requires additional column propagation fix
            assertQueryNoLeakCheck(
                    "x\ty\tcategory\tsum\n" +
                            "1\t10\tA\t10.0\n" +
                            "2\t20\tB\t20.0\n" +
                            "3\t30\tA\t40.0\n" +
                            "4\t40\tB\t60.0\n",
                    "SELECT x, y, category, sum(y) OVER w as sum " +
                            "FROM t " +
                            "WINDOW w AS (PARTITION BY category ORDER BY x)",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testNamedWindowWithFrameSpec() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x int, ts timestamp) timestamp(ts)");
            execute("insert into t values (1, 0)");
            execute("insert into t values (2, 1000000)");
            execute("insert into t values (3, 2000000)");
            execute("insert into t values (4, 3000000)");
            execute("insert into t values (5, 4000000)");

            // Named window with ROWS frame specification - use ORDER BY x (in SELECT)
            assertQueryNoLeakCheck(
                    "x\tsum\n" +
                            "1\t1.0\n" +
                            "2\t3.0\n" +
                            "3\t6.0\n" +
                            "4\t9.0\n" +
                            "5\t12.0\n",
                    "SELECT x, sum(x) OVER w as sum " +
                            "FROM t " +
                            "WINDOW w AS (ORDER BY x ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testNamedWindowReuseByMultipleFunctions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x int, ts timestamp) timestamp(ts)");
            execute("insert into t values (1, 0)");
            execute("insert into t values (2, 1000000)");
            execute("insert into t values (3, 2000000)");

            // Multiple functions using the same named window - use ORDER BY x (in SELECT)
            assertQueryNoLeakCheck(
                    "x\tsum\tavg\tcount\n" +
                            "1\t1.0\t1.0\t1\n" +
                            "2\t3.0\t1.5\t2\n" +
                            "3\t6.0\t2.0\t3\n",
                    "SELECT x, sum(x) OVER w as sum, avg(x) OVER w as avg, count(*) OVER w as count " +
                            "FROM t " +
                            "WINDOW w AS (ORDER BY x)",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testMultipleNamedWindows() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x int, category symbol, ts timestamp) timestamp(ts)");
            execute("insert into t values (1, 'A', 0)");
            execute("insert into t values (2, 'B', 1000000)");
            execute("insert into t values (3, 'A', 2000000)");
            execute("insert into t values (4, 'B', 3000000)");

            // Two different named windows - use ORDER BY x (in SELECT)
            assertQueryNoLeakCheck(
                    "x\tcategory\tsum1\tsum2\n" +
                            "1\tA\t1.0\t1.0\n" +
                            "2\tB\t3.0\t2.0\n" +
                            "3\tA\t6.0\t4.0\n" +
                            "4\tB\t10.0\t6.0\n",
                    "SELECT x, category, " +
                            "sum(x) OVER w1 as sum1, " +
                            "sum(x) OVER w2 as sum2 " +
                            "FROM t " +
                            "WINDOW w1 AS (ORDER BY x), w2 AS (PARTITION BY category ORDER BY x)",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testNamedWindowUndefinedError() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x int, ts timestamp) timestamp(ts)");
            execute("insert into t values (1, 0)");

            // Reference to undefined window name
            assertExceptionNoLeakCheck(
                    "SELECT x, sum(x) OVER undefined_window FROM t",
                    22,
                    "window 'undefined_window' is not defined"
            );
        });
    }

    @Test
    public void testNamedWindowDuplicateNameError() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x int, ts timestamp) timestamp(ts)");
            execute("insert into t values (1, 0)");

            // Duplicate window name
            assertExceptionNoLeakCheck(
                    "SELECT x, sum(x) OVER w FROM t WINDOW w AS (ORDER BY ts), w AS (ORDER BY ts DESC)",
                    58,
                    "duplicate window name"
            );
        });
    }

    @Test
    public void testNamedWindowEmptySpec() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x int, ts timestamp) timestamp(ts)");
            execute("insert into t values (1, 0)");
            execute("insert into t values (2, 1000000)");
            execute("insert into t values (3, 2000000)");

            // Empty window specification (whole table as one partition)
            assertQueryNoLeakCheck(
                    "x\trow_num\n" +
                            "1\t1\n" +
                            "2\t2\n" +
                            "3\t3\n",
                    "SELECT x, row_number() OVER w as row_num " +
                            "FROM t " +
                            "WINDOW w AS ()",
                    null,
                    false,  // row_number() without ORDER BY doesn't support random access
                    true
            );
        });
    }

    @Test
    public void testNamedWindowMixedWithInline() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x int, ts timestamp) timestamp(ts)");
            execute("insert into t values (1, 0)");
            execute("insert into t values (2, 1000000)");
            execute("insert into t values (3, 2000000)");

            // Use named window for cumulative sum
            assertQueryNoLeakCheck(
                    "x\tsum_cumulative\n" +
                            "1\t1.0\n" +
                            "2\t3.0\n" +
                            "3\t6.0\n",
                    "SELECT x, sum(x) OVER w as sum_cumulative " +
                            "FROM t " +
                            "WINDOW w AS (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testNamedWindowCaseInsensitive() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x int, ts timestamp) timestamp(ts)");
            execute("insert into t values (1, 0)");
            execute("insert into t values (2, 1000000)");

            // Window name is case-insensitive - use ORDER BY x
            assertQueryNoLeakCheck(
                    "x\tsum\n" +
                            "1\t1.0\n" +
                            "2\t3.0\n",
                    "SELECT x, sum(x) OVER MyWindow as sum " +
                            "FROM t " +
                            "WINDOW mywindow AS (ORDER BY x)",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testNamedWindowRowNumber() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x int, category symbol, ts timestamp) timestamp(ts)");
            execute("insert into t values (1, 'A', 0)");
            execute("insert into t values (2, 'B', 1000000)");
            execute("insert into t values (3, 'A', 2000000)");
            execute("insert into t values (4, 'B', 3000000)");

            // row_number() with named window - use ORDER BY x
            assertQueryNoLeakCheck(
                    "x\tcategory\trow_num\n" +
                            "1\tA\t1\n" +
                            "2\tB\t1\n" +
                            "3\tA\t2\n" +
                            "4\tB\t2\n",
                    "SELECT x, category, row_number() OVER w as row_num " +
                            "FROM t " +
                            "WINDOW w AS (PARTITION BY category ORDER BY x)",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testMultipleNamedWindowsPartitionedFirst() throws Exception {
        // This test reproduces a bug where defining the partitioned window FIRST
        // caused both windows to use the same partition specification
        assertMemoryLeak(() -> {
            execute("create table t (x int, side symbol, ts timestamp) timestamp(ts)");
            execute("insert into t values (1, 'sell', 0)");
            execute("insert into t values (2, 'buy', 1000000)");
            execute("insert into t values (3, 'buy', 2000000)");
            execute("insert into t values (4, 'sell', 3000000)");

            // ws has partition by side, wt has no partition
            // If bug exists: both would show partitioned results
            // Correct: sum_ws partitions by side, sum_wt is global cumulative
            assertQueryNoLeakCheck(
                    "x\tside\tsum_ws\tsum_wt\n" +
                            "1\tsell\t1.0\t1.0\n" +
                            "2\tbuy\t2.0\t3.0\n" +
                            "3\tbuy\t5.0\t6.0\n" +
                            "4\tsell\t5.0\t10.0\n",
                    "SELECT x, side, " +
                            "sum(x) OVER ws as sum_ws, " +
                            "sum(x) OVER wt as sum_wt " +
                            "FROM t " +
                            "WINDOW ws AS (PARTITION BY side ORDER BY x), " +
                            "wt AS (ORDER BY x)",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testInlineWindowOrderByNotInSelect() throws Exception {
        // Test that ORDER BY columns in INLINE windows don't need to be in SELECT list
        // This is the baseline - if this works, then named windows should also work
        assertMemoryLeak(() -> {
            execute("create table t (x int, y int, ts timestamp) timestamp(ts)");
            execute("insert into t values (10, 1, 0)");
            execute("insert into t values (20, 2, 1000000)");
            execute("insert into t values (30, 3, 2000000)");
            execute("insert into t values (40, 4, 3000000)");

            // Inline window: ORDER BY y, but y is not in SELECT
            assertQueryNoLeakCheck(
                    "x\tsum_x\n" +
                            "10\t10.0\n" +
                            "20\t30.0\n" +
                            "30\t60.0\n" +
                            "40\t100.0\n",
                    "SELECT x, sum(x) OVER (ORDER BY y) as sum_x FROM t",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testNamedWindowOrderByNotInSelect() throws Exception {
        // Test that ORDER BY columns in named windows don't need to be in SELECT list
        assertMemoryLeak(() -> {
            execute("create table t (x int, y int, ts timestamp) timestamp(ts)");
            execute("insert into t values (10, 1, 0)");
            execute("insert into t values (20, 2, 1000000)");
            execute("insert into t values (30, 3, 2000000)");
            execute("insert into t values (40, 4, 3000000)");

            // ORDER BY y, but y is not in SELECT - this should work
            assertQueryNoLeakCheck(
                    "x\tsum_x\n" +
                            "10\t10.0\n" +
                            "20\t30.0\n" +
                            "30\t60.0\n" +
                            "40\t100.0\n",
                    "SELECT x, sum(x) OVER w as sum_x " +
                            "FROM t " +
                            "WINDOW w AS (ORDER BY y)",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testNamedWindowWithCte() throws Exception {
        // Test that WINDOW clause works with CTEs
        assertMemoryLeak(() -> {
            execute("create table t (x int, y int, ts timestamp) timestamp(ts)");
            execute("insert into t values (10, 1, 0)");
            execute("insert into t values (20, 2, 1000000)");
            execute("insert into t values (30, 3, 2000000)");

            assertQueryNoLeakCheck(
                    "x\tsum_x\n" +
                            "10\t10.0\n" +
                            "20\t30.0\n" +
                            "30\t60.0\n",
                    "WITH a AS (SELECT * FROM t) " +
                            "SELECT x, sum(x) OVER w as sum_x FROM a WINDOW w AS (ORDER BY ts)",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testNamedWindowWithSubquery() throws Exception {
        // Test that WINDOW clause works with subqueries (similar structure to CTE)
        assertMemoryLeak(() -> {
            execute("create table t (x int, y int, ts timestamp) timestamp(ts)");
            execute("insert into t values (10, 1, 0)");
            execute("insert into t values (20, 2, 1000000)");
            execute("insert into t values (30, 3, 2000000)");

            assertQueryNoLeakCheck(
                    "x\tsum_x\n" +
                            "10\t10.0\n" +
                            "20\t30.0\n" +
                            "30\t60.0\n",
                    "SELECT x, sum(x) OVER w as sum_x FROM (SELECT * FROM t) WINDOW w AS (ORDER BY ts)",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testNamedWindowPartitionByNotInSelect() throws Exception {
        // Test that PARTITION BY columns in named windows don't need to be in SELECT list
        assertMemoryLeak(() -> {
            execute("create table t (x int, category symbol, ts timestamp) timestamp(ts)");
            execute("insert into t values (1, 'A', 0)");
            execute("insert into t values (2, 'B', 1000000)");
            execute("insert into t values (3, 'A', 2000000)");
            execute("insert into t values (4, 'B', 3000000)");

            // PARTITION BY category, but category is not in SELECT - this should work
            assertQueryNoLeakCheck(
                    "x\tsum_x\n" +
                            "1\t1.0\n" +
                            "2\t2.0\n" +
                            "3\t4.0\n" +
                            "4\t6.0\n",
                    "SELECT x, sum(x) OVER w as sum_x " +
                            "FROM t " +
                            "WINDOW w AS (PARTITION BY category ORDER BY x)",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testNamedWindowRankDenseRank() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x int, ts timestamp) timestamp(ts)");
            execute("insert into t values (1, 0)");
            execute("insert into t values (1, 1000000)");
            execute("insert into t values (2, 2000000)");
            execute("insert into t values (3, 3000000)");

            // rank() and dense_rank() with named window
            assertQueryNoLeakCheck(
                    "x\tr\tdr\n" +
                            "1\t1\t1\n" +
                            "1\t1\t1\n" +
                            "2\t3\t2\n" +
                            "3\t4\t3\n",
                    "SELECT x, rank() OVER w as r, dense_rank() OVER w as dr " +
                            "FROM t " +
                            "WINDOW w AS (ORDER BY x)",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testNamedWindowWithCteAndWhere() throws Exception {
        // Test that WINDOW clause works with CTEs and WHERE clause
        assertMemoryLeak(() -> {
            execute("create table t (x int, y int, ts timestamp) timestamp(ts)");
            execute("insert into t values (10, 1, 0)");
            execute("insert into t values (20, 2, 1000000)");
            execute("insert into t values (30, 1, 2000000)");
            execute("insert into t values (40, 2, 3000000)");

            assertQueryNoLeakCheck(
                    "x\tsum_x\n" +
                            "10\t10.0\n" +
                            "30\t40.0\n",
                    "WITH a AS (SELECT * FROM t WHERE y = 1) " +
                            "SELECT x, sum(x) OVER w as sum_x FROM a WINDOW w AS (ORDER BY ts)",
                    null,
                    false,
                    false
            );
        });
    }

    @Test
    public void testNamedWindowWithMultipleCtes() throws Exception {
        // Test that WINDOW clause works with multiple CTEs
        assertMemoryLeak(() -> {
            execute("create table t1 (x int, ts timestamp) timestamp(ts)");
            execute("create table t2 (y int, ts timestamp) timestamp(ts)");
            execute("insert into t1 values (10, 0)");
            execute("insert into t1 values (20, 1000000)");
            execute("insert into t2 values (100, 0)");
            execute("insert into t2 values (200, 1000000)");

            // Use first CTE with window function
            assertQueryNoLeakCheck(
                    "x\tsum_x\n" +
                            "10\t10.0\n" +
                            "20\t30.0\n",
                    "WITH a AS (SELECT * FROM t1), b AS (SELECT * FROM t2) " +
                            "SELECT x, sum(x) OVER w as sum_x FROM a WINDOW w AS (ORDER BY ts)",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testNamedWindowWithNestedSubqueries() throws Exception {
        // Test that WINDOW clause works with deeply nested subqueries
        assertMemoryLeak(() -> {
            execute("create table t (x int, y int, ts timestamp) timestamp(ts)");
            execute("insert into t values (10, 1, 0)");
            execute("insert into t values (20, 2, 1000000)");
            execute("insert into t values (30, 3, 2000000)");

            assertQueryNoLeakCheck(
                    "x\tsum_x\n" +
                            "10\t10.0\n" +
                            "20\t30.0\n" +
                            "30\t60.0\n",
                    "SELECT x, sum(x) OVER w as sum_x FROM (SELECT * FROM (SELECT * FROM t)) WINDOW w AS (ORDER BY ts)",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testNamedWindowWithCteAndPartitionBy() throws Exception {
        // Test that WINDOW clause with PARTITION BY works with CTEs
        assertMemoryLeak(() -> {
            execute("create table t (x int, category symbol, ts timestamp) timestamp(ts)");
            execute("insert into t values (10, 'A', 0)");
            execute("insert into t values (20, 'B', 1000000)");
            execute("insert into t values (30, 'A', 2000000)");
            execute("insert into t values (40, 'B', 3000000)");

            assertQueryNoLeakCheck(
                    "x\tcategory\tsum_x\n" +
                            "10\tA\t10.0\n" +
                            "20\tB\t20.0\n" +
                            "30\tA\t40.0\n" +
                            "40\tB\t60.0\n",
                    "WITH a AS (SELECT * FROM t) " +
                            "SELECT x, category, sum(x) OVER w as sum_x FROM a WINDOW w AS (PARTITION BY category ORDER BY ts)",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testNamedWindowWithSubqueryAndJoin() throws Exception {
        // Test that WINDOW clause works when subquery is joined with another table
        assertMemoryLeak(() -> {
            execute("create table t1 (x int, ts timestamp) timestamp(ts)");
            execute("create table t2 (y int, ts timestamp) timestamp(ts)");
            execute("insert into t1 values (10, 0)");
            execute("insert into t1 values (20, 1000000)");
            execute("insert into t2 values (100, 0)");
            execute("insert into t2 values (200, 1000000)");

            // Named window with a subquery that we then use - the window is on the outer query
            assertQueryNoLeakCheck(
                    "x\tsum_x\n" +
                            "10\t10.0\n" +
                            "20\t30.0\n",
                    "SELECT x, sum(x) OVER w as sum_x FROM (SELECT x, ts FROM t1) WINDOW w AS (ORDER BY ts)",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testNamedWindowWithCteReferenceInExpression() throws Exception {
        // Test that WINDOW clause works when CTE is used in a more complex expression
        assertMemoryLeak(() -> {
            execute("create table t (x int, ts timestamp) timestamp(ts)");
            execute("insert into t values (10, 0)");
            execute("insert into t values (20, 1000000)");
            execute("insert into t values (30, 2000000)");

            assertQueryNoLeakCheck(
                    "x\tdouble_x\tsum_x\n" +
                            "10\t20\t10.0\n" +
                            "20\t40\t30.0\n" +
                            "30\t60\t60.0\n",
                    "WITH a AS (SELECT x, x * 2 as double_x, ts FROM t) " +
                            "SELECT x, double_x, sum(x) OVER w as sum_x FROM a WINDOW w AS (ORDER BY ts)",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testNamedWindowWithInlineWindowInSameQuery() throws Exception {
        // Test that both named window and inline window can be used in the same query with subquery
        assertMemoryLeak(() -> {
            execute("create table t (x int, ts timestamp) timestamp(ts)");
            execute("insert into t values (10, 0)");
            execute("insert into t values (20, 1000000)");
            execute("insert into t values (30, 2000000)");

            assertQueryNoLeakCheck(
                    "x\tsum_x\tcount_x\n" +
                            "10\t10.0\t1\n" +
                            "20\t30.0\t2\n" +
                            "30\t60.0\t3\n",
                    "SELECT x, sum(x) OVER w as sum_x, count(*) OVER (ORDER BY ts) as count_x " +
                            "FROM (SELECT * FROM t) " +
                            "WINDOW w AS (ORDER BY ts)",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testNamedWindowExplainPlanMatchesInline() throws Exception {
        // Verify that named windows produce identical execution plans to inline windows
        assertMemoryLeak(() -> {
            execute("create table t (x int, ts timestamp) timestamp(ts)");

            // Expected plan for window function with ORDER BY ts
            // Note: Default frame is ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            String expectedPlan = "Window\n" +
                    "  functions: [sum(x) over (rows between unbounded preceding and current row)]\n" +
                    "    PageFrame\n" +
                    "        Row forward scan\n" +
                    "        Frame forward scan on: t\n";

            // Inline window
            assertPlanNoLeakCheck(
                    "SELECT sum(x) OVER (ORDER BY ts) FROM t",
                    expectedPlan
            );

            // Named window - should produce identical plan
            assertPlanNoLeakCheck(
                    "SELECT sum(x) OVER w FROM t WINDOW w AS (ORDER BY ts)",
                    expectedPlan
            );
        });
    }

    @Test
    public void testNamedWindowWithPartitionByExplainPlan() throws Exception {
        // Verify explain plan for named window with PARTITION BY
        assertMemoryLeak(() -> {
            execute("create table t (x int, category symbol, ts timestamp) timestamp(ts)");

            // Note: Default frame is ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            String expectedPlan = "Window\n" +
                    "  functions: [sum(x) over (partition by [category] rows between unbounded preceding and current row)]\n" +
                    "    PageFrame\n" +
                    "        Row forward scan\n" +
                    "        Frame forward scan on: t\n";

            // Inline window with PARTITION BY
            assertPlanNoLeakCheck(
                    "SELECT sum(x) OVER (PARTITION BY category ORDER BY ts) FROM t",
                    expectedPlan
            );

            // Named window with PARTITION BY - should produce identical plan
            assertPlanNoLeakCheck(
                    "SELECT sum(x) OVER w FROM t WINDOW w AS (PARTITION BY category ORDER BY ts)",
                    expectedPlan
            );
        });
    }

    static {
        FRAME_FUNCTIONS = Arrays.asList("avg(#COLUMN)", "sum(#COLUMN)", "ksum(#COLUMN)", "first_value(#COLUMN)", "first_value(#COLUMN) ignore nulls",
                "first_value(#COLUMN) respect nulls", "count(#COLUMN)", "max(#COLUMN)", "min(#COLUMN)",
                "last_value(#COLUMN)", "last_value(#COLUMN) ignore nulls", "last_value(#COLUMN) respect nulls");

        WINDOW_ONLY_FUNCTIONS = Arrays.asList("rank()", "dense_rank()", "row_number()", "first_value(1.0)", "last_value(1.0)", "lag(1.0)", "lead(1.0)",
                "lag(1.0) ignore nulls", "lead(1.0) ignore nulls", "lag(1.0) respect nulls", "lead(1.0) respect nulls",
                "first_value(1.0) ignore nulls", "last_value(1.0) ignore nulls", "first_value(1.0) respect nulls", "last_value(1.0) respect nulls");

        normalizeSuffix(FRAME_FUNCTIONS);
        normalizeSuffix(WINDOW_ONLY_FUNCTIONS);
    }
}
