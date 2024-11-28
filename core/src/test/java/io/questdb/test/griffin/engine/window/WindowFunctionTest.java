/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.griffin.engine.functions.window.*;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class WindowFunctionTest extends AbstractCairoTest {
    private static final List<String> FRAME_FUNCTIONS;
    private final static List<String> FRAME_TYPES = Arrays.asList("rows  ", "groups", "range ");
    private static final List<String> WINDOW_ONLY_FUNCTIONS;
    private static final String[][] FRAME_FUNCTIONS_PARAMETER_COLUMN_NAME = new String[][]{
            {
                    "i", "j" // avg
            },
            {
                    "i", "j" // sum
            },
            {
                    "i", "j", // first_value
            },
            {
                    "*", "j", "s", "d", "c" // count
            },
            {
                    "j" // max
            },
            {
                    "j" // min
            }
    };

    @Test
    public void testAggregateFunctionInPartitionByFails() throws Exception {
        assertException(
                "SELECT pickup_datetime, avg(total_amount) OVER (PARTITION BY avg(total_amount)\n" +
                        "  ORDER BY pickup_datetime\n" +
                        "  RANGE BETWEEN '7' PRECEDING AND CURRENT ROW) moving_average_1w\n" +
                        "FROM trips\n" +
                        "WHERE pickup_datetime >= '2018-12-30' and pickup_datetime <= '2018-12-31'\n" +
                        "SAMPLE BY 1d",
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
            execute("create table nodts_tab (ts timestamp, val int)");
            execute("insert into nodts_tab values (0, 1)");
            execute("insert into nodts_tab values (0, 2)");

            String noDtsResult = "ts\tval\tavg\tcount\tcount1\tcount2\tcount3\tmax\tmin\n" +
                    "1970-01-01T00:00:00.000000Z\t1\t1.0\t1\t1\t1\t1\t1.0\t1.0\n" +
                    "1970-01-01T00:00:00.000000Z\t1\t1.0\t2\t2\t2\t2\t1.0\t1.0\n" +
                    "1970-01-01T00:00:00.000000Z\t2\t1.3333333333333333\t3\t3\t3\t3\t2.0\t1.0\n" +
                    "1970-01-01T00:00:00.000000Z\t2\t1.5\t4\t4\t4\t4\t2.0\t1.0\n";

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
                    false
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
                    false
            );
        });
    }

    @Test
    public void testCachedWindowFactoryMaintainsOrderOfRecordsWithSameTimestamp2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int) timestamp(ts)");
            execute("insert into tab values (0, 1)");
            execute("insert into tab values (0, 1)");
            execute("insert into tab values (0, 2)");
            execute("insert into tab values (0, 2)");

            assertQueryNoLeakCheck(
                    "ts\tval\tavg\tcount\tmax\tmin\n" +
                            "1970-01-01T00:00:00.000000Z\t1\t1.0\t1\t1.0\t1.0\n" +
                            "1970-01-01T00:00:00.000000Z\t1\t1.0\t2\t1.0\t1.0\n" +
                            "1970-01-01T00:00:00.000000Z\t2\t1.3333333333333333\t3\t2.0\t1.0\n" +
                            "1970-01-01T00:00:00.000000Z\t2\t1.5\t4\t2.0\t1.0\n",
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
                    "ts\tval\tavg\tcount\tmax\tmin\n" +
                            "1970-01-01T00:00:00.000000Z\t2\t2.0\t1\t2.0\t2.0\n" +
                            "1970-01-01T00:00:00.000000Z\t2\t2.0\t2\t2.0\t2.0\n" +
                            "1970-01-01T00:00:00.000000Z\t1\t1.6666666666666667\t3\t2.0\t1.0\n" +
                            "1970-01-01T00:00:00.000000Z\t1\t1.5\t4\t2.0\t1.0\n",
                    "SELECT ts, val, avg(val) OVER (PARTITION BY 1=1 ORDER BY ts DESC), " +
                            "count(val) OVER (PARTITION BY 1=1 ORDER BY ts DESC), " +
                            "max(val) OVER (PARTITION BY 1=1 ORDER BY ts DESC), " +
                            "min(val) OVER (PARTITION BY 1=1 ORDER BY ts DESC) " +
                            "FROM tab " +
                            "ORDER BY ts DESC",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testFrameFunctionDoesNotAcceptFollowingInNonDefaultFrameDefinition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, i long, j long, s symbol, d double, c VARCHAR) timestamp(ts)");

            for (int i = 0, size = FRAME_FUNCTIONS.size(); i < size; i++) {
                String func = FRAME_FUNCTIONS.get(i);
                for (String column : FRAME_FUNCTIONS_PARAMETER_COLUMN_NAME[i]) {
                    assertExceptionNoLeakCheck(
                            "select #FUNCT_NAME(#COLUMN) over (partition by i rows between 10 following and 20 following) from tab".replace("#FUNCT_NAME", func).replace("#COLUMN", column),
                            59,
                            "frame start supports UNBOUNDED PRECEDING, _number_ PRECEDING and CURRENT ROW only"
                    );
                    assertExceptionNoLeakCheck(
                            "select #FUNCT_NAME(#COLUMN) over (partition by i rows between current row and 10 following) from tab".replace("#FUNCT_NAME", func).replace("#COLUMN", column),
                            75,
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
            execute("create table tab (ts timestamp, i long, j long, s symbol, d double, c VARCHAR) timestamp(ts)");
            execute("insert into tab select x::timestamp, x/4, x, 'k' || (x%5) ::symbol, x*2::double, 'k' || x from long_sequence(40000)");
            //trigger removal of rows below lo boundary AND resize of buffer
            execute("insert into tab select (100000+x)::timestamp, x/4, x, 'k' || (x%5) ::symbol, x*2::double, 'k' || x from long_sequence(90000)");

            assertQueryNoLeakCheck(
                    "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\n" +
                            "1970-01-01T00:00:00.189996Z\t22499\t89996\t49996.0\t3.999729996E9\t9996.0\t80001\t80001\t80001\t80001\t80001\t89996.0\t9996.0\n" +
                            "1970-01-01T00:00:00.189997Z\t22499\t89997\t49997.0\t3.999809997E9\t9997.0\t80001\t80001\t80001\t80001\t80001\t89997.0\t9997.0\n" +
                            "1970-01-01T00:00:00.189998Z\t22499\t89998\t49998.0\t3.999889998E9\t9998.0\t80001\t80001\t80001\t80001\t80001\t89998.0\t9998.0\n" +
                            "1970-01-01T00:00:00.189999Z\t22499\t89999\t49999.0\t3.999969999E9\t9999.0\t80001\t80001\t80001\t80001\t80001\t89999.0\t9999.0\n" +
                            "1970-01-01T00:00:00.190000Z\t22500\t90000\t50000.0\t4.00005E9\t10000.0\t80001\t80001\t80001\t80001\t80001\t90000.0\t10000.0\n",
                    "select * from (" +
                            "select ts, i, j, " +
                            "avg(j) over (order by ts range between 80000 preceding and current row), " +
                            "sum(j) over (order by ts range between 80000 preceding and current row), " +
                            "first_value(j) over (order by ts range between 80000 preceding and current row), " +
                            "count(*) over (order by ts range between 80000 preceding and current row), " +
                            "count(j) over (order by ts range between 80000 preceding and current row), " +
                            "count(s) over (order by ts range between 80000 preceding and current row), " +
                            "count(d) over (order by ts range between 80000 preceding and current row), " +
                            "count(c) over (order by ts range between 80000 preceding and current row), " +
                            "max(j) over (order by ts range between 80000 preceding and current row), " +
                            "min(j) over (order by ts range between 80000 preceding and current row) " +
                            "from tab), " +
                            " limit -5",
                    "ts",
                    false,
                    false,
                    false
            );

            execute("truncate table tab");
            // trigger buffer resize
            execute("insert into tab select (100000+x)::timestamp, x/4, x, 'k' || (x%5) ::symbol, x*2::double, 'k' || x from long_sequence(90000)");

            assertQueryNoLeakCheck(
                    "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\n" +
                            "1970-01-01T00:00:00.189996Z\t22499\t89996\t49996.0\t3.999729996E9\t9996.0\t80001\t80001\t80001\t80001\t80001\t89996.0\t9996.0\n" +
                            "1970-01-01T00:00:00.189997Z\t22499\t89997\t49997.0\t3.999809997E9\t9997.0\t80001\t80001\t80001\t80001\t80001\t89997.0\t9997.0\n" +
                            "1970-01-01T00:00:00.189998Z\t22499\t89998\t49998.0\t3.999889998E9\t9998.0\t80001\t80001\t80001\t80001\t80001\t89998.0\t9998.0\n" +
                            "1970-01-01T00:00:00.189999Z\t22499\t89999\t49999.0\t3.999969999E9\t9999.0\t80001\t80001\t80001\t80001\t80001\t89999.0\t9999.0\n" +
                            "1970-01-01T00:00:00.190000Z\t22500\t90000\t50000.0\t4.00005E9\t10000.0\t80001\t80001\t80001\t80001\t80001\t90000.0\t10000.0\n",
                    "select * from (select ts, i, j, " +
                            "avg(j) over (order by ts range between 80000 preceding and current row), " +
                            "sum(j) over (order by ts range between 80000 preceding and current row), " +
                            "first_value(j) over (order by ts range between 80000 preceding and current row), " +
                            "count(*) over (order by ts range between 80000 preceding and current row), " +
                            "count(j) over (order by ts range between 80000 preceding and current row), " +
                            "count(s) over (order by ts range between 80000 preceding and current row), " +
                            "count(d) over (order by ts range between 80000 preceding and current row), " +
                            "count(c) over (order by ts range between 80000 preceding and current row), " +
                            "max(j) over (order by ts range between 80000 preceding and current row), " +
                            "min(j) over (order by ts range between 80000 preceding and current row) " +
                            "from tab) limit -5",
                    "ts",
                    false,
                    false,
                    false
            );
        });
    }

    @Test
    public void testFrameFunctionOverNonPartitionedRowsWithLargeFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, i long, j long, s symbol, d double, c VARCHAR) timestamp(ts)");

            execute("insert into tab select x::timestamp, x/10000, x, 'k' || (x%10) ::symbol, x::double, 'k' || x from long_sequence(39999)");
            execute("insert into tab select (100000+x)::timestamp, (100000+x)%4, (100000+x), 'k' || (x%10) ::symbol, x::double, 'k' || x from long_sequence(4*90000)");

            String expected = "ts\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\n" +
                    "1970-01-01T00:00:00.460000Z\t460000\t420000.0\t3.360042E10\t380000.0\t80001\t80001\t80001\t80001\t80001\t460000.0\t380000.0\n";

            // cross-check with re-write using aggregate functions
            assertSql(
                    expected,
                    " select max(ts) as ts, max(j) j, avg(j) as avg, sum(j::double) as sum, last(j::double) as first_value, " +
                            "count(*) as count, count(j::double) as count1, count(s) as count2, count(d) as count3, count(c) as count4, " +
                            "max(j::double) as max, min(j::double) as min " +
                            "from " +
                            "( select ts, i, j, s, d, c, row_number() over (order by ts desc) as rn from tab order by ts desc) " +
                            "where rn between 1 and 80001 "
            );

            assertQueryNoLeakCheck(
                    expected,
                    "select * from (" +
                            "select * from " +
                            "(select ts, j, " +
                            "avg(j) over (order by ts rows between 80000 preceding and current row), " +
                            "sum(j) over (order by ts rows between 80000 preceding and current row), " +
                            "first_value(j) over (order by ts rows between 80000 preceding and current row), " +
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
                    false,
                    false
            );
        });
    }

    @Test
    public void testFrameFunctionOverNonPartitionedRowsWithLargeFrameRandomData() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, i long, j long, s symbol, d double, c VARCHAR) timestamp(ts)");

            execute("insert into tab select x::timestamp, x/10000, x, 'k' || (x%5) ::symbol, x*2::double, 'k' || x from long_sequence(39999)");
            execute("insert into tab select (100000+x)::timestamp, rnd_long(1,10000,10), rnd_long(1,100000,10), 'k' || (x%5) ::symbol, x*2::double, 'k' || x from long_sequence(1000000)");

            String expected = "ts\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\n" +
                    "1970-01-01T00:00:01.100000Z\t49980.066958378644\t3.815028491E9\t2073.0\t80001\t76331\t80001\t80001\t80001\t100000.0\t3.0\n";

            // cross-check with re-write using aggregate functions
            assertSql(
                    expected,
                    " select max(ts) as ts, avg(j) as avg, sum(j::double) as sum, last(j::double) as first_value, " +
                            "count(*) as count, count(j::double) as count1, count(s) as count2, count(d) as count3, count(c) as count4, " +
                            "max(j::double) as max, min(j::double) as min " +
                            "from " +
                            "( select ts, i, j, s, d, c, row_number() over (order by ts desc) as rn from tab order by ts desc) " +
                            "where rn between 1 and 80001 "
            );

            assertQueryNoLeakCheck(
                    expected,
                    "select * from (" +
                            "select * from (select ts, " +
                            "avg(j) over (order by ts rows between 80000 preceding and current row), " +
                            "sum(j) over (order by ts rows between 80000 preceding and current row), " +
                            "first_value(j) over (order by ts rows between 80000 preceding and current row), " +
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
                    false,
                    false
            );
        });
    }

    @Test
    public void testFrameFunctionOverPartitionedRangeWithLargeFrame() throws Exception {
        assertMemoryLeak(() -> {
            // default buffer size holds 65k entries in total, 32 per partition, see CairoConfiguration.getSqlWindowInitialRangeBufferSize()
            execute("create table tab (ts timestamp, i long, j long, s symbol, d double, c VARCHAR) timestamp(ts)");

            // trigger per-partition buffers growth and free list usage
            execute("insert into tab select x::timestamp, x/10000, x, 'k' || (x%5) ::symbol, x*2::double, 'k' || x from long_sequence(39999)");
            // trigger removal of rows below lo boundary AND resize of buffer
            execute("insert into tab select (100000+x)::timestamp, (100000+x)%4, (100000+x), 'k' || (x%20) ::symbol, x*2::double, 'k' || x from long_sequence(4*90000)");

            String expected = "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\n" +
                    "1970-01-01T00:00:00.460000Z\t0\t460000\t420000.0\t8.40042E9\t380000.0\t20001\t20001\t20001\t20001\t20001\t460000.0\t380000.0\n" +
                    "1970-01-01T00:00:00.459997Z\t1\t459997\t419997.0\t8.400359997E9\t379997.0\t20001\t20001\t20001\t20001\t20001\t459997.0\t379997.0\n" +
                    "1970-01-01T00:00:00.459998Z\t2\t459998\t419998.0\t8.400379998E9\t379998.0\t20001\t20001\t20001\t20001\t20001\t459998.0\t379998.0\n" +
                    "1970-01-01T00:00:00.459999Z\t3\t459999\t419999.0\t8.400399999E9\t379999.0\t20001\t20001\t20001\t20001\t20001\t459999.0\t379999.0\n";

            // cross-check with re-write using aggregate functions
            assertSql(
                    expected,
                    "select max(ts) as ts, i, max(j) as j, avg(j) as avg, sum(j::double) as sum, first(j::double) as first_value, " +
                            "count(*) as count, count(j::double) as count1, count(s) as count2, count(d) as count3, count(c) as count4, " +
                            "max(j::double) as max, min(j::double) as min " +
                            "from (" +
                            "  select data.ts, data.i, data.j, data.s, data.d, data.c" +
                            "  from ( select i, max(ts) as max from tab group by i) cnt " +
                            "  join tab data on cnt.i = data.i and data.ts >= (cnt.max - 80000) " +
                            "  order by data.i, ts " +
                            ") " +
                            "group by i " +
                            "order by i"
            );

            assertQueryNoLeakCheck(
                    expected,
                    "select * from " +
                            "(select * from (select ts, i, j, " +
                            "avg(j) over (partition by i order by ts range between 80000 preceding and current row), " +
                            "sum(j) over (partition by i order by ts range between 80000 preceding and current row), " +
                            "first_value(j) over (partition by i order by ts range between 80000 preceding and current row), " +
                            "count(*) over (partition by i order by ts range between 80000 preceding and current row), " +
                            "count(j) over (partition by i order by ts range between 80000 preceding and current row), " +
                            "count(s) over (partition by i order by ts range between 80000 preceding and current row), " +
                            "count(d) over (partition by i order by ts range between 80000 preceding and current row), " +
                            "count(c) over (partition by i order by ts range between 80000 preceding and current row), " +
                            "max(j) over (partition by i order by ts range between 80000 preceding and current row), " +
                            "min(j) over (partition by i order by ts range between 80000 preceding and current row) " +
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
            execute("create table tab (ts timestamp, i long, j long, s symbol, d double, c VARCHAR) timestamp(ts)");
            execute("insert into tab " +
                    "select (100000+x)::timestamp, " +
                    "rnd_long(1,20,10), " +
                    "rnd_long(1,1000,5), " +
                    "rnd_symbol('a', 'b', 'c', 'd'), " +
                    "rnd_long(1,1000,5)::double, " +
                    "rnd_varchar('aaa', 'vvvv', 'quest') " +
                    "from long_sequence(1000000)");

            String expected = "ts\ti\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\n" +
                    "1970-01-01T00:00:01.099993Z\tnull\t500.195891634415\t1680158.0\t201.0\t3664\t3359\t3664\t3377\t3664\t1000.0\t1.0\n" +
                    "1970-01-01T00:00:01.099950Z\t1\t495.24524012503554\t1742768.0\t915.0\t3845\t3519\t3845\t3520\t3845\t1000.0\t1.0\n" +
                    "1970-01-01T00:00:01.099955Z\t2\t495.3698069046226\t1693174.0\t80.0\t3781\t3418\t3781\t3475\t3781\t1000.0\t1.0\n" +
                    "1970-01-01T00:00:01.099983Z\t3\t505.02330264672037\t1755461.0\t807.0\t3786\t3476\t3786\t3452\t3786\t1000.0\t1.0\n" +
                    "1970-01-01T00:00:01.099989Z\t4\t507.0198750709824\t1785724.0\t423.0\t3834\t3522\t3834\t3528\t3834\t1000.0\t1.0\n" +
                    "1970-01-01T00:00:01.099999Z\t5\t505.02770562770564\t1749921.0\t986.0\t3786\t3465\t3786\t3467\t3786\t1000.0\t1.0\n" +
                    "1970-01-01T00:00:01.099992Z\t6\t500.087528604119\t1748306.0\t455.0\t3847\t3496\t3847\t3565\t3847\t1000.0\t1.0\n" +
                    "1970-01-01T00:00:01.100000Z\t7\t504.07134703196346\t1766266.0\t598.0\t3810\t3504\t3810\t3517\t3810\t1000.0\t2.0\n" +
                    "1970-01-01T00:00:01.099981Z\t8\t507.53068086298686\t1811377.0\t89.0\t3894\t3569\t3894\t3612\t3894\t1000.0\t1.0\n" +
                    "1970-01-01T00:00:01.099925Z\t9\t509.7903642099226\t1777639.0\t999.0\t3789\t3487\t3789\t3441\t3789\t999.0\t1.0\n" +
                    "1970-01-01T00:00:01.099947Z\t10\t499.44085417252035\t1777510.0\tnull\t3878\t3559\t3878\t3564\t3878\t1000.0\t2.0\n" +
                    "1970-01-01T00:00:01.099995Z\t11\t503.51796493245183\t1751739.0\t257.0\t3819\t3479\t3819\t3506\t3819\t1000.0\t1.0\n" +
                    "1970-01-01T00:00:01.099998Z\t12\t502.48197940503434\t1756677.0\t270.0\t3820\t3496\t3820\t3498\t3820\t1000.0\t1.0\n" +
                    "1970-01-01T00:00:01.099963Z\t13\t495.9894586894587\t1740923.0\t478.0\t3825\t3510\t3825\t3484\t3825\t1000.0\t1.0\n" +
                    "1970-01-01T00:00:01.099997Z\t14\t502.76085680751174\t1713409.0\t60.0\t3691\t3408\t3691\t3399\t3691\t1000.0\t1.0\n" +
                    "1970-01-01T00:00:01.099990Z\t15\t497.3836206896552\t1730895.0\t750.0\t3796\t3480\t3796\t3475\t3796\t1000.0\t1.0\n" +
                    "1970-01-01T00:00:01.099996Z\t16\t509.6849587716804\t1792562.0\t141.0\t3826\t3517\t3826\t3517\t3826\t1000.0\t1.0\n" +
                    "1970-01-01T00:00:01.099968Z\t17\t504.3433173212772\t1784871.0\t659.0\t3855\t3539\t3855\t3522\t3855\t1000.0\t1.0\n" +
                    "1970-01-01T00:00:01.099994Z\t18\t503.6875531613269\t1776506.0\t485.0\t3860\t3527\t3860\t3518\t3860\t1000.0\t1.0\n" +
                    "1970-01-01T00:00:01.099986Z\t19\t503.60588901472255\t1778736.0\t855.0\t3845\t3532\t3845\t3542\t3845\t1000.0\t1.0\n" +
                    "1970-01-01T00:00:01.099988Z\t20\t505.3122460824144\t1741306.0\t37.0\t3767\t3446\t3767\t3443\t3767\t1000.0\t1.0\n";

            // cross-check with re-write using aggregate functions
            assertSql(
                    expected,
                    "select max(ts) as ts, i, avg(j) as avg, sum(j::double) as sum, first(j::double) as first_value, " +
                            "count(*) as count, count(j::double) as count1, count(s) as count2, count(d) as count3, count(c) as count4, " +
                            "max(j::double) as max, min(j::double) as min " +
                            "from (" +
                            "  select data.ts, data.i, data.j, data.s, data.d, data.c" +
                            "  from (select i, max(ts) as max from tab group by i) cnt " +
                            "  join tab data on cnt.i = data.i and data.ts >= (cnt.max - 80000) " +
                            "  order by data.i, ts " +
                            ") " +
                            "group by i " +
                            "order by i "
            );

            assertQueryNoLeakCheck(
                    expected,
                    "select last(ts) as ts, " +
                            "i, " +
                            "last(avg) as avg, " +
                            "last(sum) as sum, " +
                            "last(first_value) as first_value, " +
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
                            "    avg(j) over (partition by i order by ts range between 80000 preceding and current row) avg, " +
                            "    sum(j) over (partition by i order by ts range between 80000 preceding and current row) sum, " +
                            "    first_value(j) over (partition by i order by ts range between 80000 preceding and current row) first_value, " +
                            "    count(*) over (partition by i order by ts range between 80000 preceding and current row) count, " +
                            "    count(j) over (partition by i order by ts range between 80000 preceding and current row) count1, " +
                            "    count(s) over (partition by i order by ts range between 80000 preceding and current row) count2, " +
                            "    count(d) over (partition by i order by ts range between 80000 preceding and current row) count3, " +
                            "    count(c) over (partition by i order by ts range between 80000 preceding and current row) count4, " +
                            "    max(j) over (partition by i order by ts range between 80000 preceding and current row) max, " +
                            "    min(j) over (partition by i order by ts range between 80000 preceding and current row) min " +
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
            execute("create table tab (ts timestamp, i long, j long, s symbol, d double, c VARCHAR) timestamp(ts)");

            execute("insert into tab select x::timestamp, x/10000, x, 'k' || (x%5) ::symbol, x*2::double, 'k' || x from long_sequence(39999)");
            execute("insert into tab select (100000+x)::timestamp, (100000+x)%4, (100000+x), 'k' || (x%20) ::symbol, x*2::double, 'k' || x from long_sequence(4*90000)");

            String expected = "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\n" +
                    "1970-01-01T00:00:00.460000Z\t0\t460000\t300000.0\t2.40003E10\t140000.0\t80001\t80001\t80001\t80001\t80001\t460000.0\t140000.0\n" +
                    "1970-01-01T00:00:00.459997Z\t1\t459997\t299997.0\t2.4000059997E10\t139997.0\t80001\t80001\t80001\t80001\t80001\t459997.0\t139997.0\n" +
                    "1970-01-01T00:00:00.459998Z\t2\t459998\t299998.0\t2.4000139998E10\t139998.0\t80001\t80001\t80001\t80001\t80001\t459998.0\t139998.0\n" +
                    "1970-01-01T00:00:00.459999Z\t3\t459999\t299999.0\t2.4000219999E10\t139999.0\t80001\t80001\t80001\t80001\t80001\t459999.0\t139999.0\n";

            // cross-check with re-write using aggregate functions
            assertSql(
                    expected,
                    " select max(ts) as ts, i, max(j) j, avg(j::double) as avg, sum(j::double) as sum, last(j::double) as first_value, " +
                            "count(*) as count, count(j::double) as count1, count(s) as count2, count(d) as count3, count(c) as count4, " +
                            "max(j::double) as max, min(j::double) as min " +
                            "from " +
                            "( select ts, i, j, s, d, c, row_number() over (partition by i order by ts desc) as rn from tab order by ts desc) " +
                            "where rn between 1 and 80001 " +
                            "group by i " +
                            "order by i"
            );

            assertQueryNoLeakCheck(
                    expected,
                    "select * from (" +
                            "select * from (select ts, i, j, " +
                            "avg(j) over (partition by i order by ts rows between 80000 preceding and current row), " +
                            "sum(j) over (partition by i order by ts rows between 80000 preceding and current row), " +
                            "first_value(j) over (partition by i order by ts rows between 80000 preceding and current row), " +
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
            execute("create table tab_big (ts timestamp, i long, j long, s symbol, d double, c VARCHAR) timestamp(ts)");
            execute("insert into tab_big select (x*1000000)::timestamp, x/4, x%5, " +
                    "'k' || (x%5) ::symbol, x*2::double, 'k' || x  from long_sequence(10)");

            // tests when frame doesn't end on current row and time gaps between values are bigger than hi bound
            assertQueryNoLeakCheck(
                    "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\n" +
                            "1970-01-01T00:00:01.000000Z\t0\t1\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:02.000000Z\t0\t2\t1.0\t1.0\t1.0\t1\t1\t1\t1\t1\t1.0\t1.0\n" +
                            "1970-01-01T00:00:03.000000Z\t0\t3\t1.5\t3.0\t1.0\t2\t2\t2\t2\t2\t2.0\t1.0\n" +
                            "1970-01-01T00:00:04.000000Z\t1\t4\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:05.000000Z\t1\t0\t4.0\t4.0\t4.0\t1\t1\t1\t1\t1\t4.0\t4.0\n" +
                            "1970-01-01T00:00:06.000000Z\t1\t1\t2.0\t4.0\t4.0\t2\t2\t2\t2\t2\t4.0\t0.0\n" +
                            "1970-01-01T00:00:07.000000Z\t1\t2\t1.6666666666666667\t5.0\t4.0\t3\t3\t3\t3\t3\t4.0\t0.0\n" +
                            "1970-01-01T00:00:08.000000Z\t2\t3\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:09.000000Z\t2\t4\t3.0\t3.0\t3.0\t1\t1\t1\t1\t1\t3.0\t3.0\n" +
                            "1970-01-01T00:00:10.000000Z\t2\t0\t3.5\t7.0\t3.0\t2\t2\t2\t2\t2\t4.0\t3.0\n",
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts range between unbounded preceding and 1 preceding), " +
                            "sum(j) over (partition by i order by ts range between unbounded preceding and 1 preceding), " +
                            "first_value(j) over (partition by i order by ts range between unbounded preceding and 1 preceding), " +
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
                    "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\n" +
                            "1970-01-01T00:00:10.000000Z\t2\t0\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:09.000000Z\t2\t4\t0.0\t0.0\t0.0\t1\t1\t1\t1\t1\t0.0\t0.0\n" +
                            "1970-01-01T00:00:08.000000Z\t2\t3\t2.0\t4.0\t0.0\t2\t2\t2\t2\t2\t4.0\t0.0\n" +
                            "1970-01-01T00:00:07.000000Z\t1\t2\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:06.000000Z\t1\t1\t2.0\t2.0\t2.0\t1\t1\t1\t1\t1\t2.0\t2.0\n" +
                            "1970-01-01T00:00:05.000000Z\t1\t0\t1.5\t3.0\t2.0\t2\t2\t2\t2\t2\t2.0\t1.0\n" +
                            "1970-01-01T00:00:04.000000Z\t1\t4\t1.0\t3.0\t2.0\t3\t3\t3\t3\t3\t2.0\t0.0\n" +
                            "1970-01-01T00:00:03.000000Z\t0\t3\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:02.000000Z\t0\t2\t3.0\t3.0\t3.0\t1\t1\t1\t1\t1\t3.0\t3.0\n" +
                            "1970-01-01T00:00:01.000000Z\t0\t1\t2.5\t5.0\t3.0\t2\t2\t2\t2\t2\t3.0\t2.0\n",
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts desc range between unbounded preceding and 1 preceding), " +
                            "sum(j) over (partition by i order by ts desc range between unbounded preceding and 1 preceding), " +
                            "first_value(j) over (partition by i order by ts desc range between unbounded preceding and 1 preceding), " +
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
                    "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\n" +
                            "1970-01-01T00:00:01.000000Z\t0\t1\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:02.000000Z\t0\t2\t1.0\t1.0\t1.0\t1\t1\t1\t1\t1\t1.0\t1.0\n" +
                            "1970-01-01T00:00:03.000000Z\t0\t3\t1.5\t3.0\t1.0\t2\t2\t2\t2\t2\t2.0\t1.0\n" +
                            "1970-01-01T00:00:04.000000Z\t1\t4\t2.0\t6.0\t1.0\t3\t3\t3\t3\t3\t3.0\t1.0\n" +
                            "1970-01-01T00:00:05.000000Z\t1\t0\t2.5\t10.0\t1.0\t4\t4\t4\t4\t4\t4.0\t1.0\n" +
                            "1970-01-01T00:00:06.000000Z\t1\t1\t2.0\t10.0\t1.0\t5\t5\t5\t5\t5\t4.0\t0.0\n" +
                            "1970-01-01T00:00:07.000000Z\t1\t2\t1.8333333333333333\t11.0\t1.0\t6\t6\t6\t6\t6\t4.0\t0.0\n" +
                            "1970-01-01T00:00:08.000000Z\t2\t3\t1.8571428571428572\t13.0\t1.0\t7\t7\t7\t7\t7\t4.0\t0.0\n" +
                            "1970-01-01T00:00:09.000000Z\t2\t4\t2.0\t16.0\t1.0\t8\t8\t8\t8\t8\t4.0\t0.0\n" +
                            "1970-01-01T00:00:10.000000Z\t2\t0\t2.2222222222222223\t20.0\t1.0\t9\t9\t9\t9\t9\t4.0\t0.0\n",
                    "select ts, i, j, " +
                            "avg(j) over (order by ts range between unbounded preceding and 1 preceding), " +
                            "sum(j) over (order by ts range between unbounded preceding and 1 preceding), " +
                            "first_value(j) over (order by ts range between unbounded preceding and 1 preceding), " +
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
                    "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\n" +
                            "1970-01-01T00:00:10.000000Z\t2\t0\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:09.000000Z\t2\t4\t0.0\t0.0\t0.0\t1\t1\t1\t1\t1\t0.0\t0.0\n" +
                            "1970-01-01T00:00:08.000000Z\t2\t3\t2.0\t4.0\t0.0\t2\t2\t2\t2\t2\t4.0\t0.0\n" +
                            "1970-01-01T00:00:07.000000Z\t1\t2\t2.3333333333333335\t7.0\t0.0\t3\t3\t3\t3\t3\t4.0\t0.0\n" +
                            "1970-01-01T00:00:06.000000Z\t1\t1\t2.25\t9.0\t0.0\t4\t4\t4\t4\t4\t4.0\t0.0\n" +
                            "1970-01-01T00:00:05.000000Z\t1\t0\t2.0\t10.0\t0.0\t5\t5\t5\t5\t5\t4.0\t0.0\n" +
                            "1970-01-01T00:00:04.000000Z\t1\t4\t1.6666666666666667\t10.0\t0.0\t6\t6\t6\t6\t6\t4.0\t0.0\n" +
                            "1970-01-01T00:00:03.000000Z\t0\t3\t2.0\t14.0\t0.0\t7\t7\t7\t7\t7\t4.0\t0.0\n" +
                            "1970-01-01T00:00:02.000000Z\t0\t2\t2.125\t17.0\t0.0\t8\t8\t8\t8\t8\t4.0\t0.0\n" +
                            "1970-01-01T00:00:01.000000Z\t0\t1\t2.111111111111111\t19.0\t0.0\t9\t9\t9\t9\t9\t4.0\t0.0\n",
                    "select ts, i, j, " +
                            "avg(j) over (order by ts desc range between unbounded preceding and 1 preceding), " +
                            "sum(j) over (order by ts desc range between unbounded preceding and 1 preceding), " +
                            "first_value(j) over (order by ts desc range between unbounded preceding and 1 preceding), " +
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

            execute("create table tab (ts timestamp, i long, j long, s symbol, d double, c VARCHAR) timestamp(ts)");
            execute("insert into tab select x::timestamp, x/4, x%5, 'k' || (x%5) ::symbol, x::double, " +
                    "'k' || x  from long_sequence(7)");

            // tests for between X preceding and [Y preceding | current row]
            assertQueryNoLeakCheck(
                    "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t1.8571428571428572\t13.0\t1.0\t7\t7\t7\t7\t7\t4.0\t0.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\t1.8571428571428572\t13.0\t1.0\t7\t7\t7\t7\t7\t4.0\t0.0\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t1.8571428571428572\t13.0\t1.0\t7\t7\t7\t7\t7\t4.0\t0.0\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\t1.8571428571428572\t13.0\t1.0\t7\t7\t7\t7\t7\t4.0\t0.0\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t1.8571428571428572\t13.0\t1.0\t7\t7\t7\t7\t7\t4.0\t0.0\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t1.8571428571428572\t13.0\t1.0\t7\t7\t7\t7\t7\t4.0\t0.0\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\t1.8571428571428572\t13.0\t1.0\t7\t7\t7\t7\t7\t4.0\t0.0\n",
                    "select ts, i, j, " +
                            "avg(j) over (), " +
                            "sum(j) over (), " +
                            "first_value(j) over (), " +
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
                    false
            );

            assertQueryNoLeakCheck(
                    "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t2.0\t6.0\t1.0\t3\t3\t3\t3\t3\t3.0\t1.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\t2.0\t6.0\t1.0\t3\t3\t3\t3\t3\t3.0\t1.0\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t2.0\t6.0\t1.0\t3\t3\t3\t3\t3\t3.0\t1.0\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\t1.75\t7.0\t4.0\t4\t4\t4\t4\t4\t4.0\t0.0\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t1.75\t7.0\t4.0\t4\t4\t4\t4\t4\t4.0\t0.0\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t1.75\t7.0\t4.0\t4\t4\t4\t4\t4\t4.0\t0.0\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\t1.75\t7.0\t4.0\t4\t4\t4\t4\t4\t4.0\t0.0\n",
                    "select ts, i, j, " +
                            "avg(j) over (partition by i), " +
                            "sum(j) over (partition by i), " +
                            "first_value(j) over (partition by i), " +
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
                    false
            );

            // separate test for first_value() only to use it with non-caching factory
            // this variant doesn't need to scan whole partition (while sum() or avg() do)
            assertQueryNoLeakCheck(
                    "ts\ti\tj\tfirst_value\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t1.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\t1.0\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t1.0\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\t4.0\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t4.0\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t4.0\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\t4.0\n",
                    "select ts, i, j, first_value(j) over (partition by i) " +
                            "from tab",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t1.0\t1.0\t1.0\t1\t1\t1\t1\t1\t1.0\t1.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\t1.5\t3.0\t1.0\t2\t2\t2\t2\t2\t2.0\t1.0\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t2.5\t5.0\t2.0\t2\t2\t2\t2\t2\t3.0\t2.0\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\t4.0\t4.0\t4.0\t1\t1\t1\t1\t1\t4.0\t4.0\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t2.0\t4.0\t4.0\t2\t2\t2\t2\t2\t4.0\t0.0\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t0.5\t1.0\t0.0\t2\t2\t2\t2\t2\t1.0\t0.0\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\t1.5\t3.0\t1.0\t2\t2\t2\t2\t2\t2.0\t1.0\n",
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts range between 1 microsecond preceding and current row), " +
                            "sum(j) over (partition by i order by ts range between 1 microsecond preceding and current row), " +
                            "first_value(j) over (partition by i order by ts range between 1 microsecond preceding and current row), " +
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
                    "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t1.0\t1.0\t1.0\t1\t1\t1\t1\t1\t1.0\t1.0\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t4.0\t4.0\t4.0\t1\t1\t1\t1\t1\t4.0\t4.0\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\t2.0\t4.0\t4.0\t2\t2\t2\t2\t2\t4.0\t0.0\n",
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts rows between 4 preceding and 2 preceding), " +
                            "sum(j) over (partition by i order by ts rows between 4 preceding and 2 preceding), " +
                            "first_value(j) over (partition by i order by ts rows between 4 preceding and 2 preceding), " +
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
                    "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n",
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts rows between 20 preceding and 10 preceding), " +
                            "sum(j) over (partition by i order by ts rows between 20 preceding and 10 preceding), " +
                            "first_value(j) over (partition by i order by ts rows between 20 preceding and 10 preceding), " +
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
                    "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t2.0\t2.0\t2.0\t1\t1\t1\t1\t1\t2.0\t2.0\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\t1.5\t3.0\t2.0\t2\t2\t2\t2\t2\t2.0\t1.0\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t3.0\t3.0\t3.0\t1\t1\t1\t1\t1\t3.0\t3.0\n",
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts desc range between 4 microseconds preceding and 2 preceding), " +
                            "sum(j) over (partition by i order by ts desc range between 4 microseconds preceding and 2 preceding), " +
                            "first_value(j) over (partition by i order by ts desc range between 4 microseconds preceding and 2 preceding), " +
                            "count(*) over (partition by i order by ts desc range between 4 microseconds preceding and 2 preceding), " +
                            "count(j) over (partition by i order by ts desc range between 4 microseconds preceding and 2 preceding), " +
                            "count(s) over (partition by i order by ts desc range between 4 microseconds preceding and 2 preceding), " +
                            "count(d) over (partition by i order by ts desc range between 4 microseconds preceding and 2 preceding), " +
                            "count(c) over (partition by i order by ts desc range between 4 microseconds preceding and 2 preceding), " +
                            "max(j) over (partition by i order by ts desc range between 4 microseconds preceding and 2 preceding), " +
                            "min(j) over (partition by i order by ts desc range between 4 microseconds preceding and 2 preceding) " +
                            "from tab order by ts desc",
                    "ts###DESC",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\t2.0\t2.0\t2.0\t1\t1\t1\t1\t1\t1.0\t2.0\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t1.5\t3.0\t2.0\t2\t2\t2\t2\t2\t1.0\t1.0\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t1.0\t3.0\t2.0\t3\t3\t3\t3\t3\t1.0\t0.0\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\t1.75\t7.0\t2.0\t4\t4\t4\t4\t4\t1.0\t0.0\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t3.0\t3.0\t3.0\t1\t1\t1\t1\t1\t0.0\t3.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\t2.5\t5.0\t3.0\t2\t2\t2\t2\t2\t0.0\t2.0\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t2.0\t6.0\t3.0\t3\t3\t3\t3\t3\t0.0\t1.0\n",
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts desc range between 4 preceding and current row), " +
                            "sum(j) over (partition by i order by ts desc range between 4 preceding and current row), " +
                            "first_value(j) over (partition by i order by ts desc range between 4 preceding and current row), " +
                            "count(*) over (partition by i order by ts desc range between 4 preceding and current row), " +
                            "count(j) over (partition by i order by ts desc range between 4 preceding and current row), " +
                            "count(s) over (partition by i order by ts desc range between 4 preceding and current row), " +
                            "count(d) over (partition by i order by ts desc range between 4 preceding and current row), " +
                            "count(c) over (partition by i order by ts desc range between 4 preceding and current row), " +
                            "max(i) over (partition by i order by ts desc range between 4 preceding and current row), " +
                            "min(j) over (partition by i order by ts desc range between 4 preceding and current row) " +
                            "from tab order by ts desc",
                    "ts###DESC",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\t2.0\t2.0\t2.0\t1\t1\t1\t1\t1\t2.0\t2.0\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t1.0\t1.0\t1.0\t1\t1\t1\t1\t1\t1.0\t1.0\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t0.0\t0.0\t0.0\t1\t1\t1\t1\t1\t0.0\t0.0\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\t4.0\t4.0\t4.0\t1\t1\t1\t1\t1\t4.0\t4.0\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t3.0\t3.0\t3.0\t1\t1\t1\t1\t1\t3.0\t3.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\t2.0\t2.0\t2.0\t1\t1\t1\t1\t1\t2.0\t2.0\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t1.0\t1.0\t1.0\t1\t1\t1\t1\t1\t1.0\t1.0\n",
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts desc range between 0 preceding and current row), " +
                            "sum(j) over (partition by i order by ts desc range between 0 preceding and current row), " +
                            "first_value(j) over (partition by i order by ts desc range between 0 preceding and current row), " +
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
                    "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t1.0\t1.0\t1.0\t1\t1\t1\t1\t1\t1.0\t1.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\t2.0\t2.0\t2.0\t1\t1\t1\t1\t1\t2.0\t2.0\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t3.0\t3.0\t3.0\t1\t1\t1\t1\t1\t3.0\t3.0\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\t4.0\t4.0\t4.0\t1\t1\t1\t1\t1\t4.0\t4.0\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t0.0\t0.0\t0.0\t1\t1\t1\t1\t1\t0.0\t0.0\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t1.0\t1.0\t1.0\t1\t1\t1\t1\t1\t1.0\t1.0\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\t2.0\t2.0\t2.0\t1\t1\t1\t1\t1\t2.0\t2.0\n",
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts asc range between 0 preceding and current row), " +
                            "sum(j) over (partition by i order by ts asc range between 0 preceding and current row), " +
                            "first_value(j) over (partition by i order by ts asc range between 0 preceding and current row), " +
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
                    "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t1.0\t1.0\t1.0\t1\t1\t1\t1\t1\t1.0\t1.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\t1.5\t3.0\t1.0\t2\t2\t2\t2\t2\t2.0\t1.0\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t2.0\t6.0\t1.0\t3\t3\t3\t3\t3\t3.0\t1.0\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\t4.0\t4.0\t4.0\t1\t1\t1\t1\t1\t4.0\t4.0\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t2.0\t4.0\t4.0\t2\t2\t2\t2\t2\t4.0\t0.0\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t1.6666666666666667\t5.0\t4.0\t3\t3\t3\t3\t3\t4.0\t0.0\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\t1.75\t7.0\t4.0\t4\t4\t4\t4\t4\t4.0\t0.0\n",
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts asc range between unbounded preceding and current row), " +
                            "sum(j) over (partition by i order by ts asc range between unbounded preceding and current row), " +
                            "first_value(j) over (partition by i order by ts asc range between unbounded preceding and current row), " +
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
                    "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\t2.0\t2.0\t2.0\t1\t1\t1\t1\t1\t2.0\t2.0\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t1.5\t3.0\t2.0\t2\t2\t2\t2\t2\t2.0\t1.0\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t1.0\t3.0\t2.0\t3\t3\t3\t3\t3\t2.0\t0.0\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\t1.75\t7.0\t2.0\t4\t4\t4\t4\t4\t4.0\t0.0\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t3.0\t3.0\t3.0\t1\t1\t1\t1\t1\t3.0\t3.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\t2.5\t5.0\t3.0\t2\t2\t2\t2\t2\t3.0\t2.0\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t2.0\t6.0\t3.0\t3\t3\t3\t3\t3\t3.0\t1.0\n",
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts desc range between unbounded preceding and current row), " +
                            "sum(j) over (partition by i order by ts desc range between unbounded preceding and current row), " +
                            "first_value(j) over (partition by i order by ts desc range between unbounded preceding and current row), " +
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
                    "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\t1.0\t1.0\t1.0\t1\t1\t1\t1\t1\t1.0\t1.0\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t1.5\t3.0\t1.0\t2\t2\t2\t2\t2\t2.0\t1.0\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t4.0\t4.0\t4.0\t1\t1\t1\t1\t1\t4.0\t4.0\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t2.0\t4.0\t4.0\t2\t2\t2\t2\t2\t4.0\t0.0\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\t1.6666666666666667\t5.0\t4.0\t3\t3\t3\t3\t3\t4.0\t0.0\n",
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts asc range between unbounded preceding and 1 preceding), " +
                            "sum(j) over (partition by i order by ts asc range between unbounded preceding and 1 preceding), " +
                            "first_value(j) over (partition by i order by ts asc range between unbounded preceding and 1 preceding), " +
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
                    "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t2.0\t2.0\t2.0\t1\t1\t1\t1\t1\t2.0\t2.0\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t1.5\t3.0\t2.0\t2\t2\t2\t2\t2\t2.0\t1.0\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\t1.0\t3.0\t2.0\t3\t3\t3\t3\t3\t2.0\t0.0\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\t3.0\t3.0\t3.0\t1\t1\t1\t1\t1\t3.0\t3.0\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t2.5\t5.0\t3.0\t2\t2\t2\t2\t2\t3.0\t2.0\n",
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts desc range between unbounded preceding and 1 preceding), " +
                            "sum(j) over (partition by i order by ts desc range between unbounded preceding and 1 preceding), " +
                            "first_value(j) over (partition by i order by ts desc range between unbounded preceding and 1 preceding), " +
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
                    "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n",
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts asc range between unbounded preceding and 10 preceding), " +
                            "sum(j) over (partition by i order by ts asc range between unbounded preceding and 10 preceding), " +
                            "first_value(j) over (partition by i order by ts asc range between unbounded preceding and 10 preceding), " +
                            "count(*) over (partition by i order by ts asc range between unbounded preceding and 10 preceding), " +
                            "count(j) over (partition by i order by ts asc range between unbounded preceding and 10 preceding), " +
                            "count(s) over (partition by i order by ts asc range between unbounded preceding and 10 preceding), " +
                            "count(d) over (partition by i order by ts asc range between unbounded preceding and 10 preceding), " +
                            "count(c) over (partition by i order by ts asc range between unbounded preceding and 10 preceding), " +
                            "max(j) over (partition by i order by ts asc range between unbounded preceding and 10 preceding), " +
                            "min(j) over (partition by i order by ts asc range between unbounded preceding and 10 preceding) " +
                            "from tab",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n",
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts desc range between unbounded preceding and 10 preceding), " +
                            "sum(j) over (partition by i order by ts desc range between unbounded preceding and 10 preceding), " +
                            "first_value(j) over (partition by i order by ts desc range between unbounded preceding and 10 preceding), " +
                            "count(*) over (partition by i order by ts desc range between unbounded preceding and 10 preceding), " +
                            "count(j) over (partition by i order by ts desc range between unbounded preceding and 10 preceding), " +
                            "count(s) over (partition by i order by ts desc range between unbounded preceding and 10 preceding), " +
                            "count(d) over (partition by i order by ts desc range between unbounded preceding and 10 preceding), " +
                            "count(c) over (partition by i order by ts desc range between unbounded preceding and 10 preceding), " +
                            "max(j) over (partition by i order by ts desc range between unbounded preceding and 10 preceding), " +
                            "min(j) over (partition by i order by ts desc range between unbounded preceding and 10 preceding) " +
                            "from tab " +
                            "order by ts desc",
                    "ts###DESC",
                    false,
                    true
            );

            // with duplicate timestamp values (but still unique within partition)

            execute("create table dups(ts timestamp, i long, j long, s symbol, d double, c VARCHAR) timestamp(ts) partition by year");
            execute("insert into dups select (x/2)::timestamp, x%2, x%5, 'k' || (x%5) ::symbol, x*2::double," +
                    " 'k' || x from long_sequence(10)");

            assertQueryNoLeakCheck(
                    "ts\ti\tj\ts\td\tc\n" +
                            "1970-01-01T00:00:00.000000Z\t1\t1\tk1\t2.0\tk1\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t2\tk2\t4.0\tk2\n" +
                            "1970-01-01T00:00:00.000001Z\t1\t3\tk3\t6.0\tk3\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t4\tk4\t8.0\tk4\n" +
                            "1970-01-01T00:00:00.000002Z\t1\t0\tk0\t10.0\tk5\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t1\tk1\t12.0\tk6\n" +
                            "1970-01-01T00:00:00.000003Z\t1\t2\tk2\t14.0\tk7\n" +
                            "1970-01-01T00:00:00.000004Z\t0\t3\tk3\t16.0\tk8\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\tk4\t18.0\tk9\n" +
                            "1970-01-01T00:00:00.000005Z\t0\t0\tk0\t20.0\tk10\n",
                    "select * from dups",
                    "ts",
                    true,
                    true
            );

            String dupResult = "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\n" +
                    "1970-01-01T00:00:00.000000Z\t1\t1\t1.0\t1.0\t1.0\t1\t1\t1\t1\t1\t1.0\t1.0\n" +
                    "1970-01-01T00:00:00.000001Z\t0\t2\t2.0\t2.0\t2.0\t1\t1\t1\t1\t1\t2.0\t2.0\n" +
                    "1970-01-01T00:00:00.000001Z\t1\t3\t2.0\t4.0\t1.0\t2\t2\t2\t2\t2\t3.0\t1.0\n" +
                    "1970-01-01T00:00:00.000002Z\t0\t4\t3.0\t6.0\t2.0\t2\t2\t2\t2\t2\t4.0\t2.0\n" +
                    "1970-01-01T00:00:00.000002Z\t1\t0\t1.3333333333333333\t4.0\t1.0\t3\t3\t3\t3\t3\t3.0\t0.0\n" +
                    "1970-01-01T00:00:00.000003Z\t0\t1\t2.3333333333333335\t7.0\t2.0\t3\t3\t3\t3\t3\t4.0\t1.0\n" +
                    "1970-01-01T00:00:00.000003Z\t1\t2\t1.5\t6.0\t1.0\t4\t4\t4\t4\t4\t3.0\t0.0\n" +
                    "1970-01-01T00:00:00.000004Z\t0\t3\t2.5\t10.0\t2.0\t4\t4\t4\t4\t4\t4.0\t1.0\n" +
                    "1970-01-01T00:00:00.000004Z\t1\t4\t2.0\t10.0\t1.0\t5\t5\t5\t5\t5\t4.0\t0.0\n" +
                    "1970-01-01T00:00:00.000005Z\t0\t0\t2.0\t10.0\t2.0\t5\t5\t5\t5\t5\t4.0\t0.0\n";

            assertQueryNoLeakCheck(
                    dupResult,
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts range between 4 preceding and current row), " +
                            "sum(j) over (partition by i order by ts range between 4 preceding and current row), " +
                            "first_value(j) over (partition by i order by ts range between 4 preceding and current row), " +
                            "count(j) over (partition by i order by ts range between 4 preceding and current row), " +
                            "count(*) over (partition by i order by ts range between 4 preceding and current row), " +
                            "count(s) over (partition by i order by ts range between 4 preceding and current row), " +
                            "count(d) over (partition by i order by ts range between 4 preceding and current row), " +
                            "count(c) over (partition by i order by ts range between 4 preceding and current row), " +
                            "max(j) over (partition by i order by ts range between 4 preceding and current row), " +
                            "min(j) over (partition by i order by ts range between 4 preceding and current row) " +
                            "from dups",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    dupResult,
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts range between 4 preceding and current row), " +
                            "sum(j) over (partition by i order by ts range between 4 preceding and current row), " +
                            "first_value(j) over (partition by i order by ts range between 4 preceding and current row), " +
                            "count(*) over (partition by i order by ts range between 4 preceding and current row), " +
                            "count(j) over (partition by i order by ts range between 4 preceding and current row), " +
                            "count(s) over (partition by i order by ts range between 4 preceding and current row), " +
                            "count(d) over (partition by i order by ts range between 4 preceding and current row), " +
                            "count(c) over (partition by i order by ts range between 4 preceding and current row), " +
                            "max(j) over (partition by i order by ts range between 4 preceding and current row), " +
                            "min(j) over (partition by i order by ts range between 4 preceding and current row) " +
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

            String dupResult2 = "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\n" +
                    "1970-01-01T00:00:00.000005Z\t0\t0\t0.0\t0.0\t0.0\t1\t1\t1\t1\t1\t0.0\t0.0\n" +
                    "1970-01-01T00:00:00.000004Z\t1\t4\t4.0\t4.0\t4.0\t1\t1\t1\t1\t1\t4.0\t4.0\n" +
                    "1970-01-01T00:00:00.000004Z\t0\t3\t1.5\t3.0\t0.0\t2\t2\t2\t2\t2\t3.0\t0.0\n" +
                    "1970-01-01T00:00:00.000003Z\t1\t2\t3.0\t6.0\t4.0\t2\t2\t2\t2\t2\t4.0\t2.0\n" +
                    "1970-01-01T00:00:00.000003Z\t0\t1\t1.3333333333333333\t4.0\t0.0\t3\t3\t3\t3\t3\t3.0\t0.0\n" +
                    "1970-01-01T00:00:00.000002Z\t1\t0\t2.0\t6.0\t4.0\t3\t3\t3\t3\t3\t4.0\t0.0\n" +
                    "1970-01-01T00:00:00.000002Z\t0\t4\t2.0\t8.0\t0.0\t4\t4\t4\t4\t4\t4.0\t0.0\n" +
                    "1970-01-01T00:00:00.000001Z\t1\t3\t2.25\t9.0\t4.0\t4\t4\t4\t4\t4\t4.0\t0.0\n" +
                    "1970-01-01T00:00:00.000001Z\t0\t2\t2.0\t10.0\t0.0\t5\t5\t5\t5\t5\t4.0\t0.0\n" +
                    "1970-01-01T00:00:00.000000Z\t1\t1\t2.0\t10.0\t4.0\t5\t5\t5\t5\t5\t4.0\t0.0\n";

            assertQueryNoLeakCheck(
                    dupResult2,
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts desc range between 4 preceding and current row), " +
                            "sum(j) over (partition by i order by ts desc range between 4 preceding and current row), " +
                            "first_value(j) over (partition by i order by ts desc range between 4 preceding and current row), " +
                            "count(*) over (partition by i order by ts desc range between 4 preceding and current row), " +
                            "count(j) over (partition by i order by ts desc range between 4 preceding and current row), " +
                            "count(s) over (partition by i order by ts desc range between 4 preceding and current row), " +
                            "count(d) over (partition by i order by ts desc range between 4 preceding and current row), " +
                            "count(c) over (partition by i order by ts desc range between 4 preceding and current row), " +
                            "max(j) over (partition by i order by ts desc range between 4 preceding and current row), " +
                            "min(j) over (partition by i order by ts desc range between 4 preceding and current row) " +
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
            execute("create table dups2(ts timestamp, i long, j long, n long, s symbol, d double, c VARCHAR) timestamp(ts) partition by year");
            execute("insert into dups2 select (x/4)::timestamp, x%2, x%5, x, 'k' || (x%5) ::symbol, x*2::double," +
                    " 'k' || x from long_sequence(10)");

            assertSql(
                    "ts\ti\tj\tn\ts\td\tc\n" +
                            "1970-01-01T00:00:00.000000Z\t0\t2\t2\tk2\t4.0\tk2\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t4\t4\tk4\t8.0\tk4\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t6\tk1\t12.0\tk6\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t3\t8\tk3\t16.0\tk8\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t0\t10\tk0\t20.0\tk10\n" +
                            "1970-01-01T00:00:00.000000Z\t1\t1\t1\tk1\t2.0\tk1\n" +
                            "1970-01-01T00:00:00.000000Z\t1\t3\t3\tk3\t6.0\tk3\n" +
                            "1970-01-01T00:00:00.000001Z\t1\t0\t5\tk0\t10.0\tk5\n" +
                            "1970-01-01T00:00:00.000001Z\t1\t2\t7\tk2\t14.0\tk7\n" +
                            "1970-01-01T00:00:00.000002Z\t1\t4\t9\tk4\t18.0\tk9\n",
                    "select * from dups2 order by i, n"
            );

            assertQueryNoLeakCheck(
                    "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\n" +
                            "1970-01-01T00:00:00.000000Z\t0\t2\t2.0\t2.0\t2.0\t1\t1\t1\t1\t1\t2.0\t2.0\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t4\t4.0\t4.0\t4.0\t1\t1\t1\t1\t1\t4.0\t4.0\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t2.5\t5.0\t4.0\t2\t2\t2\t2\t2\t4.0\t1.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t3\t3.0\t3.0\t3.0\t1\t1\t1\t1\t1\t3.0\t3.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t0\t1.5\t3.0\t3.0\t2\t2\t2\t2\t2\t3.0\t0.0\n" +
                            "1970-01-01T00:00:00.000000Z\t1\t1\t1.0\t1.0\t1.0\t1\t1\t1\t1\t1\t1.0\t1.0\n" +
                            "1970-01-01T00:00:00.000000Z\t1\t3\t2.0\t4.0\t1.0\t2\t2\t2\t2\t2\t3.0\t1.0\n" +
                            "1970-01-01T00:00:00.000001Z\t1\t0\t0.0\t0.0\t0.0\t1\t1\t1\t1\t1\t0.0\t0.0\n" +
                            "1970-01-01T00:00:00.000001Z\t1\t2\t1.0\t2.0\t0.0\t2\t2\t2\t2\t2\t2.0\t0.0\n" +
                            "1970-01-01T00:00:00.000002Z\t1\t4\t4.0\t4.0\t4.0\t1\t1\t1\t1\t1\t4.0\t4.0\n",
                    "select ts, i, j, avg, sum, first_value, count, count1, count2, count3, count4, max, min " +
                            "from ( " +
                            "select ts, i, j, n, " +
                            "avg(j) over (partition by i order by ts range between 0 preceding and current row) as avg, " +
                            "sum(j) over (partition by i order by ts range between 0 preceding and current row) as sum, " +
                            "first_value(j) over (partition by i order by ts range between 0 preceding and current row) as first_value, " +
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
                    "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\n" +
                            "1970-01-01T00:00:00.000002Z\t1\t4\t4.0\t4.0\t4.0\t1\t1\t1\t1\t1\t4.0\t4.0\n" +
                            "1970-01-01T00:00:00.000001Z\t1\t2\t2.0\t2.0\t2.0\t1\t1\t1\t1\t1\t2.0\t2.0\n" +
                            "1970-01-01T00:00:00.000001Z\t1\t0\t1.0\t2.0\t2.0\t2\t2\t2\t2\t2\t2.0\t0.0\n" +
                            "1970-01-01T00:00:00.000000Z\t1\t3\t3.0\t3.0\t3.0\t1\t1\t1\t1\t1\t3.0\t3.0\n" +
                            "1970-01-01T00:00:00.000000Z\t1\t1\t2.0\t4.0\t3.0\t2\t2\t2\t2\t2\t3.0\t1.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t0\t0.0\t0.0\t0.0\t1\t1\t1\t1\t1\t0.0\t0.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t3\t1.5\t3.0\t0.0\t2\t2\t2\t2\t2\t3.0\t0.0\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t1.0\t1.0\t1.0\t1\t1\t1\t1\t1\t1.0\t1.0\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t4\t2.5\t5.0\t1.0\t2\t2\t2\t2\t2\t4.0\t1.0\n" +
                            "1970-01-01T00:00:00.000000Z\t0\t2\t2.0\t2.0\t2.0\t1\t1\t1\t1\t1\t2.0\t2.0\n",
                    "select ts, i, j, avg, sum, first_value, count, count1, count2, count3, count4, max, min from ( " +
                            "select ts, i, j, n, " +
                            "avg(j) over (partition by i order by ts desc range between 0 preceding and current row) as avg, " +
                            "sum(j) over (partition by i order by ts desc range between 0 preceding and current row) as sum, " +
                            "first_value(j) over (partition by i order by ts desc range between 0 preceding and current row) as first_value, " +
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
                    "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\n" +
                            "1970-01-01T00:00:00.000000Z\t0\t2\t2.0\t2.0\t2.0\t1\t1\t1\t1\t1\t2.0\t2.0\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t4\t3.0\t6.0\t2.0\t2\t2\t2\t2\t2\t4.0\t2.0\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t2.3333333333333335\t7.0\t2.0\t3\t3\t3\t3\t3\t4.0\t1.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t3\t2.6666666666666665\t8.0\t4.0\t3\t3\t3\t3\t3\t4.0\t1.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t0\t2.0\t8.0\t4.0\t4\t4\t4\t4\t4\t4.0\t0.0\n" +
                            "1970-01-01T00:00:00.000000Z\t1\t1\t1.0\t1.0\t1.0\t1\t1\t1\t1\t1\t1.0\t1.0\n" +
                            "1970-01-01T00:00:00.000000Z\t1\t3\t2.0\t4.0\t1.0\t2\t2\t2\t2\t2\t3.0\t1.0\n" +
                            "1970-01-01T00:00:00.000001Z\t1\t0\t1.3333333333333333\t4.0\t1.0\t3\t3\t3\t3\t3\t3.0\t0.0\n" +
                            "1970-01-01T00:00:00.000001Z\t1\t2\t1.5\t6.0\t1.0\t4\t4\t4\t4\t4\t3.0\t0.0\n" +
                            "1970-01-01T00:00:00.000002Z\t1\t4\t2.0\t6.0\t0.0\t3\t3\t3\t3\t3\t4.0\t0.0\n",
                    "select ts, i, j, avg, sum, first_value, count, count1, count2, count3, count4, max, min " +
                            "from ( " +
                            "select ts, i, j,n, " +
                            "avg(j) over (partition by i order by ts range between 1 preceding and current row) as avg, " +
                            "sum(j) over (partition by i order by ts range between 1 preceding and current row) as sum, " +
                            "first_value(j) over (partition by i order by ts range between 1 preceding and current row) as first_value, " +
                            "count(*) over (partition by i order by ts range between 1 preceding and current row) as count, " +
                            "count(j) over (partition by i order by ts range between 1 preceding and current row) as count1, " +
                            "count(s) over (partition by i order by ts range between 1 preceding and current row) as count2, " +
                            "count(d) over (partition by i order by ts range between 1 preceding and current row) as count3, " +
                            "count(c) over (partition by i order by ts range between 1 preceding and current row) as count4, " +
                            "max(j) over (partition by i order by ts range between 1 preceding and current row) as max, " +
                            "min(j) over (partition by i order by ts range between 1 preceding and current row) as min " +
                            "from dups2 " +
                            "limit 10" +
                            ") order by i, n",
                    null,
                    true,
                    true
            );

            assertQueryNoLeakCheck(
                    "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\n" +
                            "1970-01-01T00:00:00.000002Z\t1\t4\t4.0\t4.0\t4.0\t1\t1\t1\t1\t1\t4.0\t4.0\n" +
                            "1970-01-01T00:00:00.000001Z\t1\t2\t3.0\t6.0\t4.0\t2\t2\t2\t2\t2\t4.0\t2.0\n" +
                            "1970-01-01T00:00:00.000001Z\t1\t0\t2.0\t6.0\t4.0\t3\t3\t3\t3\t3\t4.0\t0.0\n" +
                            "1970-01-01T00:00:00.000000Z\t1\t3\t1.6666666666666667\t5.0\t2.0\t3\t3\t3\t3\t3\t3.0\t0.0\n" +
                            "1970-01-01T00:00:00.000000Z\t1\t1\t1.5\t6.0\t2.0\t4\t4\t4\t4\t4\t3.0\t0.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t0\t0.0\t0.0\t0.0\t1\t1\t1\t1\t1\t0.0\t0.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t3\t1.5\t3.0\t0.0\t2\t2\t2\t2\t2\t3.0\t0.0\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t1.3333333333333333\t4.0\t0.0\t3\t3\t3\t3\t3\t3.0\t0.0\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t4\t2.0\t8.0\t0.0\t4\t4\t4\t4\t4\t4.0\t0.0\n" +
                            "1970-01-01T00:00:00.000000Z\t0\t2\t2.3333333333333335\t7.0\t1.0\t3\t3\t3\t3\t3\t4.0\t1.0\n",
                    "select ts, i, j, avg, sum, first_value, count, count1, count2, count3, count4, max, min " +
                            "from ( " +
                            "select ts, i, j,n, " +
                            "avg(j) over (partition by i order by ts desc range between 1 preceding and current row) as avg, " +
                            "sum(j) over (partition by i order by ts desc range between 1 preceding and current row) as sum, " +
                            "first_value(j) over (partition by i order by ts desc range between 1 preceding and current row) as first_value, " +
                            "count(*) over (partition by i order by ts desc range between 1 preceding and current row) as count, " +
                            "count(j) over (partition by i order by ts desc range between 1 preceding and current row) as count1, " +
                            "count(s) over (partition by i order by ts desc range between 1 preceding and current row) as count2, " +
                            "count(d) over (partition by i order by ts desc range between 1 preceding and current row) as count3, " +
                            "count(c) over (partition by i order by ts desc range between 1 preceding and current row) as count4, " +
                            "max(j) over (partition by i order by ts desc range between 1 preceding and current row) as max, " +
                            "min(j) over (partition by i order by ts desc range between 1 preceding and current row) as min " +
                            "from dups2 " +
                            "order by ts " +
                            "desc limit 10" +
                            ") order by i desc, n desc",
                    null,
                    true,
                    true
            );

            String dupResult3 = "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\n" +
                    "1970-01-01T00:00:00.000000Z\t0\t2\t2.0\t2.0\t2.0\t1\t1\t1\t1\t1\t2.0\t2.0\n" +
                    "1970-01-01T00:00:00.000001Z\t0\t4\t3.0\t6.0\t2.0\t2\t2\t2\t2\t2\t4.0\t2.0\n" +
                    "1970-01-01T00:00:00.000001Z\t0\t1\t2.3333333333333335\t7.0\t2.0\t3\t3\t3\t3\t3\t4.0\t1.0\n" +
                    "1970-01-01T00:00:00.000002Z\t0\t3\t2.5\t10.0\t2.0\t4\t4\t4\t4\t4\t4.0\t1.0\n" +
                    "1970-01-01T00:00:00.000002Z\t0\t0\t2.0\t10.0\t2.0\t5\t5\t5\t5\t5\t4.0\t0.0\n" +
                    "1970-01-01T00:00:00.000000Z\t1\t1\t1.0\t1.0\t1.0\t1\t1\t1\t1\t1\t1.0\t1.0\n" +
                    "1970-01-01T00:00:00.000000Z\t1\t3\t2.0\t4.0\t1.0\t2\t2\t2\t2\t2\t3.0\t1.0\n" +
                    "1970-01-01T00:00:00.000001Z\t1\t0\t1.3333333333333333\t4.0\t1.0\t3\t3\t3\t3\t3\t3.0\t0.0\n" +
                    "1970-01-01T00:00:00.000001Z\t1\t2\t1.5\t6.0\t1.0\t4\t4\t4\t4\t4\t3.0\t0.0\n" +
                    "1970-01-01T00:00:00.000002Z\t1\t4\t2.0\t10.0\t1.0\t5\t5\t5\t5\t5\t4.0\t0.0\n";

            assertQueryNoLeakCheck(
                    dupResult3,
                    "select ts, i, j, avg, sum, first_value, count, count1, count2, count3, count4, max, min " +
                            "from ( " +
                            "select ts, i, j, n, " +
                            "avg(j) over (partition by i order by ts range between 4 preceding and current row) avg, " +
                            "sum(j) over (partition by i order by ts range between 4 preceding and current row) sum, " +
                            "first_value(j) over (partition by i order by ts range between 4 preceding and current row) first_value, " +
                            "count(*) over (partition by i order by ts range between 4 preceding and current row) count, " +
                            "count(j) over (partition by i order by ts range between 4 preceding and current row) count1, " +
                            "count(s) over (partition by i order by ts range between 4 preceding and current row) count2, " +
                            "count(d) over (partition by i order by ts range between 4 preceding and current row) count3, " +
                            "count(c) over (partition by i order by ts range between 4 preceding and current row) count4, " +
                            "max(j) over (partition by i order by ts range between 4 preceding and current row) max, " +
                            "min(j) over (partition by i order by ts range between 4 preceding and current row) min " +
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
                    "select ts, i, j,avg, sum, first_value, count, count1, count2, count3, count4, max, min " +
                            "from ( " +
                            "select ts, i, j, n, " +
                            "avg(j) over (partition by i order by ts range between unbounded preceding and current row) avg, " +
                            "sum(j) over (partition by i order by ts range between unbounded preceding and current row) sum, " +
                            "first_value(j) over (partition by i order by ts range between unbounded preceding and current row) first_value, " +
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
                    "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\n" +
                            "1970-01-01T00:00:00.000000Z\t0\t2\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t4\t2.0\t2.0\t2.0\t1\t1\t1\t1\t1\t2.0\t2.0\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t2.0\t2.0\t2.0\t1\t1\t1\t1\t1\t2.0\t2.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t3\t2.3333333333333335\t7.0\t2.0\t3\t3\t3\t3\t3\t4.0\t1.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t0\t2.3333333333333335\t7.0\t2.0\t3\t3\t3\t3\t3\t4.0\t1.0\n" +
                            "1970-01-01T00:00:00.000000Z\t1\t1\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000000Z\t1\t3\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000001Z\t1\t0\t2.0\t4.0\t1.0\t2\t2\t2\t2\t2\t3.0\t1.0\n" +
                            "1970-01-01T00:00:00.000001Z\t1\t2\t2.0\t4.0\t1.0\t2\t2\t2\t2\t2\t3.0\t1.0\n" +
                            "1970-01-01T00:00:00.000002Z\t1\t4\t1.5\t6.0\t1.0\t4\t4\t4\t4\t4\t3.0\t0.0\n",
                    "select ts, i, j, avg, sum, first_value, count, count1, count2, count3, count4, max, min from ( " +
                            "select ts, i, j, n, " +
                            "avg(j) over (partition by i order by ts range between unbounded preceding and 1 preceding) avg, " +
                            "sum(j) over (partition by i order by ts range between unbounded preceding and 1 preceding) sum, " +
                            "first_value(j) over (partition by i order by ts range between unbounded preceding and 1 preceding) first_value, " +
                            "count(*) over (partition by i order by ts range between unbounded preceding and 1 preceding) count, " +
                            "count(j) over (partition by i order by ts range between unbounded preceding and 1 preceding) count1, " +
                            "count(s) over (partition by i order by ts range between unbounded preceding and 1 preceding) count2, " +
                            "count(d) over (partition by i order by ts range between unbounded preceding and 1 preceding) count3, " +
                            "count(c) over (partition by i order by ts range between unbounded preceding and 1 preceding) count4, " +
                            "max(j) over (partition by i order by ts range between unbounded preceding and 1 preceding) max, " +
                            "min(j) over (partition by i order by ts range between unbounded preceding and 1 preceding) min " +
                            "from dups2 " +
                            "order by ts " +
                            "limit 10" +
                            ") order by i, n",
                    null,
                    true,
                    true
            );

            String dupResult4 = "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\n" +
                    "1970-01-01T00:00:00.000002Z\t1\t4\t4.0\t4.0\t4.0\t1\t1\t1\t1\t1\t4.0\t4.0\n" +
                    "1970-01-01T00:00:00.000001Z\t1\t2\t3.0\t6.0\t4.0\t2\t2\t2\t2\t2\t4.0\t2.0\n" +
                    "1970-01-01T00:00:00.000001Z\t1\t0\t2.0\t6.0\t4.0\t3\t3\t3\t3\t3\t4.0\t0.0\n" +
                    "1970-01-01T00:00:00.000000Z\t1\t3\t2.25\t9.0\t4.0\t4\t4\t4\t4\t4\t4.0\t0.0\n" +
                    "1970-01-01T00:00:00.000000Z\t1\t1\t2.0\t10.0\t4.0\t5\t5\t5\t5\t5\t4.0\t0.0\n" +
                    "1970-01-01T00:00:00.000002Z\t0\t0\t0.0\t0.0\t0.0\t1\t1\t1\t1\t1\t0.0\t0.0\n" +
                    "1970-01-01T00:00:00.000002Z\t0\t3\t1.5\t3.0\t0.0\t2\t2\t2\t2\t2\t3.0\t0.0\n" +
                    "1970-01-01T00:00:00.000001Z\t0\t1\t1.3333333333333333\t4.0\t0.0\t3\t3\t3\t3\t3\t3.0\t0.0\n" +
                    "1970-01-01T00:00:00.000001Z\t0\t4\t2.0\t8.0\t0.0\t4\t4\t4\t4\t4\t4.0\t0.0\n" +
                    "1970-01-01T00:00:00.000000Z\t0\t2\t2.0\t10.0\t0.0\t5\t5\t5\t5\t5\t4.0\t0.0\n";

            assertQueryNoLeakCheck(
                    dupResult4,
                    "select ts,i,j,avg, sum, first_value, count, count1, count2, count3, count4, max, min " +
                            "from ( " +
                            "select ts, i, j, n, " +
                            "avg(j) over (partition by i order by ts desc range between 4 preceding and current row) avg, " +
                            "sum(j) over (partition by i order by ts desc range between 4 preceding and current row) sum, " +
                            "first_value(j) over (partition by i order by ts desc range between 4 preceding and current row) first_value, " +
                            "count(*) over (partition by i order by ts desc range between 4 preceding and current row) count, " +
                            "count(j) over (partition by i order by ts desc range between 4 preceding and current row) count1, " +
                            "count(s) over (partition by i order by ts desc range between 4 preceding and current row) count2, " +
                            "count(d) over (partition by i order by ts desc range between 4 preceding and current row) count3, " +
                            "count(c) over (partition by i order by ts desc range between 4 preceding and current row) count4, " +
                            "max(j) over (partition by i order by ts desc range between 4 preceding and current row) max, " +
                            "min(j) over (partition by i order by ts desc range between 4 preceding and current row) min " +
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
                    "select ts,i,j,avg, sum, first_value, count, count1, count2, count3, count4, max, min " +
                            "from ( " +
                            "select ts, i, j, n, " +
                            "avg(j) over (partition by i order by ts desc range between unbounded preceding and current row) avg, " +
                            "sum(j) over (partition by i order by ts desc range between unbounded preceding and current row) sum, " +
                            "first_value(j) over (partition by i order by ts desc range between unbounded preceding and current row) first_value, " +
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
                    "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\n" +
                            "1970-01-01T00:00:00.000002Z\t1\t4\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000001Z\t1\t2\t4.0\t4.0\t4.0\t1\t1\t1\t1\t1\t4.0\t4.0\n" +
                            "1970-01-01T00:00:00.000001Z\t1\t0\t4.0\t4.0\t4.0\t1\t1\t1\t1\t1\t4.0\t4.0\n" +
                            "1970-01-01T00:00:00.000000Z\t1\t3\t2.0\t6.0\t4.0\t3\t3\t3\t3\t3\t4.0\t0.0\n" +
                            "1970-01-01T00:00:00.000000Z\t1\t1\t2.0\t6.0\t4.0\t3\t3\t3\t3\t3\t4.0\t0.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t0\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t3\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t1.5\t3.0\t0.0\t2\t2\t2\t2\t2\t3.0\t0.0\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t4\t1.5\t3.0\t0.0\t2\t2\t2\t2\t2\t3.0\t0.0\n" +
                            "1970-01-01T00:00:00.000000Z\t0\t2\t2.0\t8.0\t0.0\t4\t4\t4\t4\t4\t4.0\t0.0\n",
                    "select ts,i,j,avg, sum, first_value, count, count1, count2, count3, count4, max, min " +
                            "from ( " +
                            "select ts, i, j, n, " +
                            "avg(j) over (partition by i order by ts desc range between unbounded preceding and 1 preceding) avg, " +
                            "sum(j) over (partition by i order by ts desc range between unbounded preceding and 1 preceding) sum, " +
                            "first_value(j) over (partition by i order by ts desc range between unbounded preceding and 1 preceding) first_value, " +
                            "count(*) over (partition by i order by ts desc range between unbounded preceding and 1 preceding) count, " +
                            "count(j) over (partition by i order by ts desc range between unbounded preceding and 1 preceding) count1, " +
                            "count(s) over (partition by i order by ts desc range between unbounded preceding and 1 preceding) count2, " +
                            "count(d) over (partition by i order by ts desc range between unbounded preceding and 1 preceding) count3, " +
                            "count(c) over (partition by i order by ts desc range between unbounded preceding and 1 preceding) count4, " +
                            "max(j) over (partition by i order by ts desc range between unbounded preceding and 1 preceding) max, " +
                            "min(j) over (partition by i order by ts desc range between unbounded preceding and 1 preceding) min " +
                            "from dups2 " +
                            "order by ts desc " +
                            "limit 10" +
                            ") order by i desc, n desc",
                    null,
                    true,
                    true
            );

            // table without designated timestamp
            execute("create table nodts(ts timestamp, i long, j long, s symbol, d double, c VARCHAR)");
            execute("insert into nodts select (x/2)::timestamp, x%2, x%5, 'k' || (x%5) ::symbol, x*2::double, 'k' || x from long_sequence(10)");

            // timestamp ascending order is declared using timestamp(ts) clause
            assertQueryNoLeakCheck(
                    dupResult,
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts range between 4 preceding and current row), " +
                            "sum(j) over (partition by i order by ts range between 4 preceding and current row), " +
                            "first_value(j) over (partition by i order by ts range between 4 preceding and current row), " +
                            "count(*) over (partition by i order by ts range between 4 preceding and current row), " +
                            "count(j) over (partition by i order by ts range between 4 preceding and current row), " +
                            "count(s) over (partition by i order by ts range between 4 preceding and current row), " +
                            "count(d) over (partition by i order by ts range between 4 preceding and current row), " +
                            "count(c) over (partition by i order by ts range between 4 preceding and current row), " +
                            "max(j) over (partition by i order by ts range between 4 preceding and current row), " +
                            "min(j) over (partition by i order by ts range between 4 preceding and current row) " +
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
                    "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t1.0\t1.0\t1.0\t1\t1\t1\t1\t1\t1.0\t1.0\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t4.0\t4.0\t4.0\t1\t1\t1\t1\t1\t4.0\t4.0\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\t2.0\t4.0\t4.0\t2\t2\t2\t2\t2\t4.0\t0.0\n",
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts range between 1 second preceding and 2 microsecond preceding), " +
                            "sum(j) over (partition by i order by ts range between 1 second preceding and 2 microsecond preceding), " +
                            "first_value(j) over (partition by i order by ts range between 1 second preceding and 2 microsecond preceding), " +
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
            execute("create table nodts(ts timestamp, i long, j long, s symbol, d double, c VARCHAR)");

            //table with designated timestamp
            execute("create table tab (ts timestamp, i long, j long, s symbol, d double, c VARCHAR, otherTs timestamp) timestamp(ts) partition by month");

            for (int i = 0, size = FRAME_FUNCTIONS.size(); i < size; i++) {
                String func = FRAME_FUNCTIONS.get(i);
                for (String column : FRAME_FUNCTIONS_PARAMETER_COLUMN_NAME[i]) {
                    assertExceptionNoLeakCheck(
                            "select ts, i, j, #FUNCT_NAME(#COLUMN) over (order by ts range between 4 preceding and current row) from nodts".replace("#FUNCT_NAME", func).replace("#COLUMN", column),
                            47,
                            "RANGE is supported only for queries ordered by designated timestamp"
                    );

                    assertExceptionNoLeakCheck(
                            "select ts, i, j, #FUNCT_NAME(#COLUMN) over (partition by i order by ts range between 4 preceding and current row) from nodts".replace("#FUNCT_NAME", func).replace("#COLUMN", column),
                            62,
                            "RANGE is supported only for queries ordered by designated timestamp"
                    );

                    // while it's possible to declare ascending designated timestamp order, it's not possible to declare descending order
                    assertExceptionNoLeakCheck(
                            "select ts, i, j, #FUNCT_NAME(#COLUMN) over (partition by i order by ts desc range between 4 preceding and current row) from nodts timestamp(ts)".replace("#FUNCT_NAME", func).replace("#COLUMN", column),
                            62,
                            "RANGE is supported only for queries ordered by designated timestamp"
                    );

                    assertExceptionNoLeakCheck(
                            "select ts, i, j, #FUNCT_NAME(#COLUMN) over (partition by i order by j desc range between unbounded preceding and 10 microsecond preceding) ".replace("#FUNCT_NAME", func).replace("#COLUMN", column) +
                                    "from tab order by ts desc",
                            62,
                            "RANGE is supported only for queries ordered by designated timestamp"
                    );

                    assertExceptionNoLeakCheck(
                            "select ts, i, j, #FUNCT_NAME(#COLUMN) over (partition by i order by j range 10 microsecond preceding) from tab".replace("#FUNCT_NAME", func).replace("#COLUMN", column),
                            62,
                            "RANGE is supported only for queries ordered by designated timestamp"
                    );

                    // order by column_number doesn't work with in over clause so 1 is treated as integer constant
                    assertExceptionNoLeakCheck(
                            "select ts, i, j, #FUNCT_NAME(#COLUMN) over (partition by i order by 1 range 10 microsecond preceding) from tab".replace("#FUNCT_NAME", func).replace("#COLUMN", column),
                            62,
                            "RANGE is supported only for queries ordered by designated timestamp"
                    );

                    assertExceptionNoLeakCheck(
                            "select ts, i, j, #FUNCT_NAME(#COLUMN) over (partition by i order by ts+i range 10 microsecond preceding) from tab".replace("#FUNCT_NAME", func).replace("#COLUMN", column),
                            64,
                            "RANGE is supported only for queries ordered by designated timestamp"
                    );

                    assertExceptionNoLeakCheck(
                            "select ts, i, j, #FUNCT_NAME(#COLUMN) over (partition by i order by otherTs range 10 microsecond preceding) from tab".replace("#FUNCT_NAME", func).replace("#COLUMN", column),
                            62,
                            "RANGE is supported only for queries ordered by designated timestamp"
                    );

                    assertExceptionNoLeakCheck(
                            "select ts, i, j, #FUNCT_NAME(#COLUMN) over (partition by i order by ts range 10 microsecond preceding) from tab timestamp(otherTs)".replace("#FUNCT_NAME", func).replace("#COLUMN", column),
                            62,
                            "RANGE is supported only for queries ordered by designated timestamp"
                    );

                    assertExceptionNoLeakCheck(
                            "select ts, i, j, #FUNCT_NAME(#COLUMN) over (partition by i order by otherTs desc range 10 microsecond preceding) from tab timestamp(otherTs)".replace("#FUNCT_NAME", func).replace("#COLUMN", column),
                            62,
                            "RANGE is supported only for queries ordered by designated timestamp"
                    );
                }
            }
        });
    }

    @Test
    public void testFrameFunctionOverRowsRejectsCurrentRowFrameExcludingCurrentRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, i long, j long, s symbol, d double, c VARCHAR) timestamp(ts)");

            for (int i = 0, size = FRAME_FUNCTIONS.size(); i < size; i++) {
                String func = FRAME_FUNCTIONS.get(i);
                for (String column : FRAME_FUNCTIONS_PARAMETER_COLUMN_NAME[i]) {
                    assertExceptionNoLeakCheck(
                            "select ts, i, j, #FUNCT_NAME(#COLUMN) over (partition by i order by ts rows current row exclude current row) from tab".replace("#FUNCT_NAME", func).replace("#COLUMN", column),
                            90,
                            "end of window is higher than start of window due to exclusion mode"
                    );
                }
            }
        });
    }

    @Test
    public void testFrameFunctionRejectsExclusionModesOtherThanDefault() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table xyz (a int, b int, c int, ts timestamp) timestamp(ts)");

            for (String function : FRAME_FUNCTIONS) {
                for (String exclusionMode : new String[]{"GROUP", "TIES"}) {
                    assertWindowException(
                            "select a,b, #FUNCT_NAME(c) over (partition by b order by ts #FRAME UNBOUNDED PRECEDING EXCLUDE #mode) from xyz"
                                    .replace("#FUNCT_NAME", function)
                                    .replace("#mode", exclusionMode),
                            95,
                            "only EXCLUDE NO OTHERS and EXCLUDE CURRENT ROW exclusion modes are supported"
                    );

                    assertWindowException(
                            "select a,b, #FUNCT_NAME(c) over (partition by b order by ts #FRAME BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE #mode) from xyz"
                                    .replace("#FUNCT_NAME", function)
                                    .replace("#mode", exclusionMode),
                            119,
                            "only EXCLUDE NO OTHERS and EXCLUDE CURRENT ROW exclusion modes are supported"
                    );
                }

                assertWindowException(
                        "select a,b, #FUNCT_NAME(c) over (partition by b order by ts #FRAME BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) from xyz"
                                .replace("#FUNCT_NAME", function),
                        127,
                        "EXCLUDE CURRENT ROW not supported with UNBOUNDED FOLLOWING frame boundary"
                );
            }
        });
    }

    @Test
    public void testFrameFunctionRejectsFramesThatUseFollowing() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, i long, j long, s symbol, d double, c VARCHAR) timestamp(ts)");
            execute("insert into tab select x::timestamp, x/4, x%5, 'k' || (x%5) ::symbol, x*2::double, 'k' || x from long_sequence(7)");

            for (int i = 0, size = FRAME_FUNCTIONS.size(); i < size; i++) {
                String func = FRAME_FUNCTIONS.get(i);
                for (String column : FRAME_FUNCTIONS_PARAMETER_COLUMN_NAME[i]) {
                    assertExceptionNoLeakCheck(
                            "select ts, i, j, #FUNCT_NAME(#COLUMN) over (partition by i order by ts rows between 10 following and 20 following) from tab".replace("#FUNCT_NAME", func).replace("#COLUMN", column),
                            81,
                            "frame start supports UNBOUNDED PRECEDING, _number_ PRECEDING and CURRENT ROW only"
                    );
                    assertExceptionNoLeakCheck(
                            "select ts, i, j, #FUNCT_NAME(#COLUMN) over (partition by i order by ts rows between 10 preceding and 1 following) from tab".replace("#FUNCT_NAME", func).replace("#COLUMN", column),
                            97,
                            "frame end supports _number_ PRECEDING and CURRENT ROW only"
                    );
                    assertExceptionNoLeakCheck(
                            "select ts, i, j, #FUNCT_NAME(#COLUMN) over (partition by i order by ts rows between 10 preceding and unbounded following) from tab".replace("#FUNCT_NAME", func).replace("#COLUMN", column),
                            105, "frame end supports UNBOUNDED FOLLOWING only when frame start is UNBOUNDED PRECEDING"
                    );

                    assertExceptionNoLeakCheck(
                            "select ts, i, j, #FUNCT_NAME(#COLUMN) over (partition by i order by ts groups between 10 following and 20 following) from tab".replace("#FUNCT_NAME", func).replace("#COLUMN", column),
                            83,
                            "frame start supports UNBOUNDED PRECEDING, _number_ PRECEDING and CURRENT ROW only"
                    );
                    assertExceptionNoLeakCheck(
                            "select ts, i, j, #FUNCT_NAME(#COLUMN) over (partition by i order by ts groups between 10 preceding and 1 following) from tab".replace("#FUNCT_NAME", func).replace("#COLUMN", column),
                            99,
                            "frame end supports _number_ PRECEDING and CURRENT ROW only"
                    );
                    assertExceptionNoLeakCheck(
                            "select ts, i, j, #FUNCT_NAME(#COLUMN) over (partition by i order by ts groups between 10 preceding and unbounded following) from tab".replace("#FUNCT_NAME", func).replace("#COLUMN", column),
                            107,
                            "frame end supports UNBOUNDED FOLLOWING only when frame start is UNBOUNDED PRECEDING"
                    );

                    assertExceptionNoLeakCheck(
                            "select ts, i, j, #FUNCT_NAME(i) over (partition by i order by ts range between 10 following and 20 following) from tab".replace("#FUNCT_NAME", func).replace("#COLUMN", column),
                            82,
                            "frame start supports UNBOUNDED PRECEDING, _number_ PRECEDING and CURRENT ROW only"
                    );
                    assertExceptionNoLeakCheck(
                            "select ts, i, j, #FUNCT_NAME(i) over (partition by i order by ts range between 10 preceding and 1 following) from tab".replace("#FUNCT_NAME", func).replace("#COLUMN", column),
                            98,
                            "frame end supports _number_ PRECEDING and CURRENT ROW only"
                    );
                    assertExceptionNoLeakCheck(
                            "select ts, i, j, #FUNCT_NAME(i) over (partition by i order by ts range between 10 preceding and unbounded following) from tab".replace("#FUNCT_NAME", func).replace("#COLUMN", column),
                            106,
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
                    "hostname\tusage_system\tavg\tsum\tfirst_value\tcount\tcount1\tmax\tmin\n" +
                            "A\t1.0\t1.0\t1.0\t1.0\t1\t1\t1.0\t1.0\n" +
                            "A\t2.0\t1.5\t3.0\t1.0\t2\t2\t2.0\t1.0\n" +
                            "B\t3.0\t3.0\t3.0\t3.0\t1\t1\t3.0\t3.0\n" +
                            "C\t4.0\t4.0\t4.0\t4.0\t1\t1\t4.0\t4.0\n" +
                            "C\t5.0\t4.5\t9.0\t4.0\t2\t2\t5.0\t4.0\n" +
                            "C\t6.0\t5.0\t15.0\t4.0\t3\t3\t6.0\t4.0\n" +
                            "C\t7.0\t5.5\t22.0\t4.0\t4\t4\t7.0\t4.0\n" +
                            "B\t8.0\t5.5\t11.0\t3.0\t2\t2\t8.0\t3.0\n" +
                            "A\t9.0\t4.0\t12.0\t1.0\t3\t3\t9.0\t1.0\n" +
                            "B\t10.0\t7.0\t21.0\t3.0\t3\t3\t10.0\t3.0\n",
                    "select hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname rows between 50 preceding and current row), " +
                            "sum(usage_system) over(partition by hostname rows between 50 preceding and current row), " +
                            "first_value(usage_system) over(partition by hostname rows between 50 preceding and current row), " +
                            "count(*) over(partition by hostname rows between 50 preceding and current row), " +
                            "count(usage_system) over(partition by hostname rows between 50 preceding and current row), " +
                            "max(usage_system) over(partition by hostname rows between 50 preceding and current row), " +
                            "min(usage_system) over(partition by hostname rows between 50 preceding and current row) " +
                            "from cpu " +
                            "limit 10",
                    null,
                    false,
                    false
            );
        });
    }

    @Test
    public void testFrameFunctionResolvesSymbolTablesInPartitionByCachedWindow() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (sym symbol, i int);");
            execute("insert into x values ('aaa', 1);");
            execute("insert into x values ('aaa', 2);");

            assertQueryNoLeakCheck(
                    "sym\tavg\tsum\tfirst_value\tcount\tcount1\tmax\tmin\n" +
                            "aaa\t1.5\t3.0\t1.0\t2\t2\t2.0\t1.0\n" +
                            "aaa\t1.5\t3.0\t1.0\t2\t2\t2.0\t1.0\n",
                    "SELECT sym, " +
                            "avg(i) OVER(PARTITION BY sym LIKE '%aaa%'), " +
                            "sum(i) OVER(PARTITION BY sym LIKE '%aaa%'), " +
                            "first_value(i) OVER(PARTITION BY sym LIKE '%aaa%'), " +
                            "count(i) OVER(PARTITION BY sym LIKE '%aaa%'), " +
                            "count(sym) OVER(PARTITION BY sym LIKE '%aaa%'), " +
                            "max(i) OVER(PARTITION BY sym LIKE '%aaa%'), " +
                            "min(i) OVER(PARTITION BY sym LIKE '%aaa%'), " +
                            "FROM x",
                    null,
                    true, // cached window factory
                    false
            );
        });
    }

    @Test
    public void testFrameFunctionResolvesSymbolTablesInPartitionByNonCachedWindow() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (sym symbol, i int, ts timestamp) timestamp(ts) partition by day;");
            execute("insert into x values ('aaa', 1, '2023-11-09T00:00:00.000000');");
            execute("insert into x values ('aaa', 2, '2023-11-09T01:00:00.000000');");

            assertQueryNoLeakCheck(
                    "ts\tsym\tavg\tsum\tfirst_value\tcount\tcount1\tmax\tmin\n" +
                            "2023-11-09T00:00:00.000000Z\taaa\t1.0\t1.0\t1.0\t1\t1\t1.0\t1.0\n" +
                            "2023-11-09T01:00:00.000000Z\taaa\t1.5\t3.0\t1.0\t2\t2\t2.0\t1.0\n",
                    "SELECT ts, sym, " +
                            "avg(i) OVER(PARTITION BY sym LIKE '%aaa%' ORDER BY ts), " +
                            "sum(i) OVER(PARTITION BY sym LIKE '%aaa%' ORDER BY ts), " +
                            "first_value(i) OVER(PARTITION BY sym LIKE '%aaa%' ORDER BY ts), " +
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
            execute("create table tab (ts timestamp, i long, j long, s symbol, d double, c VARCHAR) timestamp(ts)");
            execute("insert into tab select x::timestamp, x/4, x%5, 'k' || (x%5) ::symbol, x*2::double, 'k' || x from long_sequence(7)");

            for (int i = 0, size = FRAME_FUNCTIONS.size(); i < size; i++) {
                String func = FRAME_FUNCTIONS.get(i);
                for (String column : FRAME_FUNCTIONS_PARAMETER_COLUMN_NAME[i]) {
                    assertExceptionNoLeakCheck(
                            "select ts, i, j, #FUNCT_NAME(#COLUMN) over (partition by i order by ts groups unbounded preceding) from tab".replace("#FUNCT_NAME", func).replace("#COLUMN", column),
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
            execute("create table tab (ts timestamp, i long, j long, d double, s symbol, c VARCHAR) timestamp(ts)");
            execute("insert into tab select x::timestamp, x/4, x%5, x%5, 'k' || (x%5) ::symbol, 'k' || x from long_sequence(7)");

            assertSql(
                    "ts\ti\tj\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\n",
                    "select ts, i, j from tab"
            );

            assertQueryNoLeakCheck(
                    "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tmax\tmin\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t1.0\t1.0\t1.0\t1\t1\t1\t1\t1.0\t1.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\t1.5\t3.0\t1.0\t2\t2\t2\t2\t2.0\t1.0\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t2.0\t6.0\t1.0\t3\t3\t3\t3\t3.0\t1.0\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\t2.5\t10.0\t1.0\t4\t4\t4\t4\t4.0\t1.0\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t2.0\t10.0\t1.0\t5\t5\t5\t5\t4.0\t0.0\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t1.8333333333333333\t11.0\t1.0\t6\t6\t6\t6\t4.0\t0.0\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\t1.8571428571428572\t13.0\t1.0\t7\t7\t7\t7\t4.0\t0.0\n",
                    "select ts, i, j, " +
                            "avg(d) over (order by ts rows unbounded preceding)," +
                            "sum(d) over (order by ts rows unbounded preceding), " +
                            "first_value(d) over (order by ts rows unbounded preceding), " +
                            "count(*) over (order by ts rows unbounded preceding), " +
                            "count(d) over (order by ts rows unbounded preceding), " +
                            "count(s) over (order by ts rows unbounded preceding), " +
                            "count(c) over (order by ts rows unbounded preceding), " +
                            "max(d) over (order by ts rows unbounded preceding), " +
                            "min(d) over (order by ts rows unbounded preceding) " +
                            "from tab",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tmax\tmin\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t1.0\t1.0\t1.0\t1\t1\t1\t1\t1.0\t1.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\t1.5\t3.0\t1.0\t2\t2\t2\t2\t2.0\t1.0\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t2.0\t6.0\t1.0\t3\t3\t3\t3\t3.0\t1.0\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\t1.8571428571428572\t13.0\t1.0\t7\t7\t7\t7\t4.0\t0.0\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t1.5\t6.0\t1.0\t4\t4\t4\t4\t3.0\t0.0\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t1.4\t7.0\t1.0\t5\t5\t5\t5\t3.0\t0.0\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\t1.5\t9.0\t1.0\t6\t6\t6\t6\t3.0\t0.0\n",
                    "select ts, i, j, " +
                            "avg(j) over (order by i, j rows unbounded preceding), " +
                            "sum(j) over (order by i, j rows unbounded preceding), " +
                            "first_value(j) over (order by i, j rows unbounded preceding), " +
                            "count(*) over (order by i, j rows unbounded preceding), " +
                            "count(s) over (order by i, j rows unbounded preceding), " +
                            "count(d) over (order by i, j rows unbounded preceding), " +
                            "count(c) over (order by i, j rows unbounded preceding), " +
                            "max(j) over (order by i, j rows unbounded preceding), " +
                            "min(j) over (order by i, j rows unbounded preceding) " +
                            "from tab",
                    "ts",
                    true,//cached window factory
                    false
            );

            assertQueryNoLeakCheck(
                    "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tmax\tmin\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t1.0\t1.0\t1.0\t1\t1\t1\t1\t1.0\t1.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\t2.0\t2.0\t2.0\t1\t1\t1\t1\t2.0\t2.0\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t3.0\t3.0\t3.0\t1\t1\t1\t1\t3.0\t3.0\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\t4.0\t4.0\t4.0\t1\t1\t1\t1\t4.0\t4.0\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t0.0\t0.0\t0.0\t1\t1\t1\t1\t0.0\t0.0\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t1.0\t1.0\t1.0\t1\t1\t1\t1\t1.0\t1.0\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\t2.0\t2.0\t2.0\t1\t1\t1\t1\t2.0\t2.0\n",
                    "select ts, i, j, " +
                            "avg(d) over (order by ts rows current row), " +
                            "sum(d) over (order by ts rows current row), " +
                            "first_value(d) over (order by ts rows current row), " +
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
                    "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tmax\tmin\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t1.0\t1.0\t1.0\t0\t0\t0\t0\t1.0\t1.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\t2.0\t2.0\t2.0\t0\t0\t0\t0\t2.0\t2.0\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t3.0\t3.0\t3.0\t0\t0\t0\t0\t3.0\t3.0\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\t4.0\t4.0\t4.0\t0\t0\t0\t0\t4.0\t4.0\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t0.0\t0.0\t0.0\t0\t0\t0\t0\t0.0\t0.0\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t1.0\t1.0\t1.0\t0\t0\t0\t0\t1.0\t1.0\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\t2.0\t2.0\t2.0\t0\t0\t0\t0\t2.0\t2.0\n",
                    "select ts, i, j, " +
                            "avg(d) over (order by ts desc rows current row), " +
                            "sum(d) over (order by ts desc rows current row), " +
                            "first_value(d) over (order by ts desc rows current row), " +
                            "count(*) over (order by ts desc rows current row), " +
                            "count(s) over (order by ts desc rows current row), " +
                            "count(d) over (order by ts desc rows current row), " +
                            "count(c) over (order by ts desc rows current row), " +
                            "max(d) over (order by ts desc rows current row), " +
                            "min(d) over (order by ts desc rows current row) " +
                            "from tab",
                    "ts",
                    true, // cached window factory
                    false
            );

            assertQueryNoLeakCheck(
                    "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tmax\tmin\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\tnull\tnull\tnull\t1\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\t1.0\t1.0\t1.0\t2\t1\t1\t1\t1.0\t1.0\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t1.5\t3.0\t1.0\t3\t2\t2\t2\t2.0\t1.0\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\t2.0\t6.0\t1.0\t4\t3\t3\t3\t3.0\t1.0\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t2.5\t10.0\t1.0\t5\t4\t4\t4\t4.0\t1.0\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t2.0\t10.0\t1.0\t6\t5\t5\t5\t4.0\t0.0\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\t1.8333333333333333\t11.0\t1.0\t7\t6\t6\t6\t4.0\t0.0\n",
                    "select ts, i, j, " +
                            "avg(d) over (order by ts rows between unbounded preceding and 1 preceding), " +
                            "sum(d) over (order by ts rows between unbounded preceding and 1 preceding), " +
                            "first_value(d) over (order by ts rows between unbounded preceding and 1 preceding), " +
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
                    "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tmax\tmin\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\tnull\tnull\tnull\t1\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\tnull\tnull\tnull\t2\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t1.0\t1.0\t1.0\t3\t1\t1\t1\t1.0\t1.0\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\t1.5\t3.0\t1.0\t3\t2\t2\t2\t2.0\t1.0\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t2.0\t6.0\t1.0\t3\t3\t3\t3\t3.0\t1.0\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t3.0\t9.0\t2.0\t3\t3\t3\t3\t4.0\t2.0\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\t2.3333333333333335\t7.0\t3.0\t3\t3\t3\t3\t4.0\t0.0\n",
                    "select ts, i, j, " +
                            "avg(d) over (order by ts rows between 4 preceding and 2 preceding), " +
                            "sum(d) over (order by ts rows between 4 preceding and 2 preceding), " +
                            "first_value(d) over (order by ts rows between 4 preceding and 2 preceding), " +
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
                    "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tmax\tmin\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t2.3333333333333335\t7.0\t0.0\t3\t3\t3\t3\t4.0\t0.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\t1.6666666666666667\t5.0\t1.0\t3\t3\t3\t3\t4.0\t0.0\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t1.0\t3.0\t2.0\t3\t3\t3\t3\t2.0\t0.0\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\t1.5\t3.0\t2.0\t3\t2\t2\t2\t2.0\t1.0\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t2.0\t2.0\t2.0\t3\t1\t1\t1\t2.0\t2.0\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\tnull\tnull\tnull\t2\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\tnull\tnull\tnull\t1\t0\t0\t0\tnull\tnull\n",
                    "select ts, i, j, " +
                            "avg(d) over (order by ts desc rows between 4 preceding and 2 preceding), " +
                            "sum(d) over (order by ts desc rows between 4 preceding and 2 preceding), " +
                            "first_value(d) over (order by ts desc rows between 4 preceding and 2 preceding), " +
                            "count(*) over (order by ts desc rows between 4 preceding and 2 preceding), " +
                            "count(s) over (order by ts desc rows between 4 preceding and 2 preceding), " +
                            "count(d) over (order by ts desc rows between 4 preceding and 2 preceding), " +
                            "count(c) over (order by ts desc rows between 4 preceding and 2 preceding), " +
                            "max(d) over (order by ts desc rows between 4 preceding and 2 preceding), " +
                            "min(d) over (order by ts desc rows between 4 preceding and 2 preceding) " +
                            "from tab",
                    "ts",
                    true, //c ached window factory
                    false
            );

            assertQueryNoLeakCheck(
                    "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tmax\tmin\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t1.8571428571428572\t13.0\t1.0\t7\t7\t7\t7\t4.0\t0.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\t1.8571428571428572\t13.0\t1.0\t7\t7\t7\t7\t4.0\t0.0\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t1.8571428571428572\t13.0\t1.0\t7\t7\t7\t7\t4.0\t0.0\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\t1.8571428571428572\t13.0\t1.0\t7\t7\t7\t7\t4.0\t0.0\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t1.8571428571428572\t13.0\t1.0\t7\t7\t7\t7\t4.0\t0.0\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t1.8571428571428572\t13.0\t1.0\t7\t7\t7\t7\t4.0\t0.0\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\t1.8571428571428572\t13.0\t1.0\t7\t7\t7\t7\t4.0\t0.0\n",
                    "select ts, i, j, " +
                            "avg(d) over (order by i rows between unbounded preceding and unbounded following), " +
                            "sum(d) over (order by i rows between unbounded preceding and unbounded following), " +
                            "first_value(d) over (order by i rows between unbounded preceding and unbounded following), " +
                            "count(*) over (order by i rows between unbounded preceding and unbounded following), " +
                            "count(s) over (order by i rows between unbounded preceding and unbounded following), " +
                            "count(d) over (order by i rows between unbounded preceding and unbounded following), " +
                            "count(c) over (order by i rows between unbounded preceding and unbounded following), " +
                            "max(d) over (order by i rows between unbounded preceding and unbounded following), " +
                            "min(d) over (order by i rows between unbounded preceding and unbounded following) " +
                            "from tab",
                    "ts",
                    true, // cached window factory
                    false
            );

            assertQueryNoLeakCheck(
                    "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tmax\tmin\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t2.0\t6.0\t1.0\t3\t3\t3\t3\t3.0\t1.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\t2.0\t6.0\t1.0\t3\t3\t3\t3\t3.0\t1.0\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t2.0\t6.0\t1.0\t3\t3\t3\t3\t3.0\t1.0\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\t1.75\t7.0\t4.0\t4\t4\t4\t4\t4.0\t0.0\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t1.75\t7.0\t4.0\t4\t4\t4\t4\t4.0\t0.0\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t1.75\t7.0\t4.0\t4\t4\t4\t4\t4.0\t0.0\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\t1.75\t7.0\t4.0\t4\t4\t4\t4\t4.0\t0.0\n",
                    "select ts, i, j, " +
                            "avg(d) over (partition by i rows between unbounded preceding and unbounded following), " +
                            "sum(d) over (partition by i rows between unbounded preceding and unbounded following), " +
                            "first_value(d) over (partition by i rows between unbounded preceding and unbounded following), " +
                            "count(*) over (partition by i rows between unbounded preceding and unbounded following), " +
                            "count(s) over (partition by i rows between unbounded preceding and unbounded following), " +
                            "count(d) over (partition by i rows between unbounded preceding and unbounded following), " +
                            "count(c) over (partition by i rows between unbounded preceding and unbounded following), " +
                            "max(d) over (partition by i rows between unbounded preceding and unbounded following), " +
                            "min(d) over (partition by i rows between unbounded preceding and unbounded following) " +
                            "from tab",
                    "ts",
                    true,//cached window factory
                    false
            );

            String rowsResult1 = "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tmax\tmin\n" +
                    "1970-01-01T00:00:00.000001Z\t0\t1\t1.0\t1.0\t1.0\t1\t1\t1\t1\t1.0\t1.0\n" +
                    "1970-01-01T00:00:00.000002Z\t0\t2\t1.5\t3.0\t1.0\t2\t2\t2\t2\t2.0\t1.0\n" +
                    "1970-01-01T00:00:00.000003Z\t0\t3\t2.0\t6.0\t1.0\t3\t3\t3\t3\t3.0\t1.0\n" +
                    "1970-01-01T00:00:00.000004Z\t1\t4\t4.0\t4.0\t4.0\t1\t1\t1\t1\t4.0\t4.0\n" +
                    "1970-01-01T00:00:00.000005Z\t1\t0\t2.0\t4.0\t4.0\t2\t2\t2\t2\t4.0\t0.0\n" +
                    "1970-01-01T00:00:00.000006Z\t1\t1\t1.6666666666666667\t5.0\t4.0\t3\t3\t3\t3\t4.0\t0.0\n" +
                    "1970-01-01T00:00:00.000007Z\t1\t2\t1.75\t7.0\t4.0\t4\t4\t4\t4\t4.0\t0.0\n";

            assertQueryNoLeakCheck(
                    rowsResult1,
                    "select ts, i, j, " +
                            "avg(d) over (partition by i order by ts rows unbounded preceding), " +
                            "sum(d) over (partition by i order by ts rows unbounded preceding), " +
                            "first_value(d) over (partition by i order by ts rows unbounded preceding), " +
                            "count(*) over (partition by i order by ts rows unbounded preceding), " +
                            "count(s) over (partition by i order by ts rows unbounded preceding), " +
                            "count(d) over (partition by i order by ts rows unbounded preceding), " +
                            "count(c) over (partition by i order by ts rows unbounded preceding), " +
                            "max(d) over (partition by i order by ts rows unbounded preceding), " +
                            "min(d) over (partition by i order by ts rows unbounded preceding) " +
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
                    "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t1.0\t1.0\t1.0\t1\t1\t1\t1\t1\t1.0\t1.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\t1.5\t3.0\t1.0\t2\t2\t2\t2\t2\t2.0\t1.0\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t2.5\t5.0\t2.0\t2\t2\t2\t2\t2\t3.0\t2.0\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\t4.0\t4.0\t4.0\t1\t1\t1\t1\t1\t4.0\t4.0\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t2.0\t4.0\t4.0\t2\t2\t2\t2\t2\t4.0\t0.0\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t0.5\t1.0\t0.0\t2\t2\t2\t2\t2\t1.0\t0.0\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\t1.5\t3.0\t1.0\t2\t2\t2\t2\t2\t2.0\t1.0\n",
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts rows between 1 preceding and current row), " +
                            "sum(j) over (partition by i order by ts rows between 1 preceding and current row), " +
                            "first_value(j) over (partition by i order by ts rows between 1 preceding and current row), " +
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
                    "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t1.0\t1.0\t1.0\t1\t1\t1\t1\t1\t1.0\t1.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\t1.5\t3.0\t1.0\t2\t2\t2\t2\t2\t2.0\t1.0\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t2.0\t6.0\t1.0\t3\t3\t3\t3\t3\t3.0\t1.0\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\t4.0\t4.0\t4.0\t1\t1\t1\t1\t1\t4.0\t4.0\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t2.0\t4.0\t4.0\t2\t2\t2\t2\t2\t4.0\t0.0\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t1.6666666666666667\t5.0\t4.0\t3\t3\t3\t3\t3\t4.0\t0.0\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\t1.0\t3.0\t0.0\t3\t3\t3\t3\t3\t2.0\t0.0\n",
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts rows between 2 preceding and current row), " +
                            "sum(j) over (partition by i order by ts rows between 2 preceding and current row), " +
                            "first_value(j) over (partition by i order by ts rows between 2 preceding and current row), " +
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

            String result2 = "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\n" +
                    "1970-01-01T00:00:00.000001Z\t0\t1\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                    "1970-01-01T00:00:00.000002Z\t0\t2\t1.0\t1.0\t1.0\t1\t1\t1\t1\t1\t1.0\t1.0\n" +
                    "1970-01-01T00:00:00.000003Z\t0\t3\t1.5\t3.0\t1.0\t2\t2\t2\t2\t2\t2.0\t1.0\n" +
                    "1970-01-01T00:00:00.000004Z\t1\t4\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                    "1970-01-01T00:00:00.000005Z\t1\t0\t4.0\t4.0\t4.0\t1\t1\t1\t1\t1\t4.0\t4.0\n" +
                    "1970-01-01T00:00:00.000006Z\t1\t1\t2.0\t4.0\t4.0\t2\t2\t2\t2\t2\t4.0\t0.0\n" +
                    "1970-01-01T00:00:00.000007Z\t1\t2\t0.5\t1.0\t0.0\t2\t2\t2\t2\t2\t1.0\t0.0\n";

            assertQueryNoLeakCheck(
                    result2,
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts rows between 2 preceding and 1 preceding), " +
                            "sum(j) over (partition by i order by ts rows between 2 preceding and 1 preceding), " +
                            "first_value(j) over (partition by i order by ts rows between 2 preceding and 1 preceding), " +
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
                    "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n",
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts rows between 20 preceding and 10 preceding), " +
                            "sum(j) over (partition by i order by ts rows between 20 preceding and 10 preceding), " +
                            "first_value(j) over (partition by i order by ts rows between 20 preceding and 10 preceding), " +
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

            String result3 = "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\n" +
                    "1970-01-01T00:00:00.000001Z\t0\t1\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                    "1970-01-01T00:00:00.000002Z\t0\t2\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                    "1970-01-01T00:00:00.000003Z\t0\t3\t1.0\t1.0\t1.0\t1\t1\t1\t1\t1\t1.0\t1.0\n" +
                    "1970-01-01T00:00:00.000004Z\t1\t4\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                    "1970-01-01T00:00:00.000005Z\t1\t0\tnull\tnull\tnull\t0\t0\t0\t0\t0\tnull\tnull\n" +
                    "1970-01-01T00:00:00.000006Z\t1\t1\t4.0\t4.0\t4.0\t1\t1\t1\t1\t1\t4.0\t4.0\n" +
                    "1970-01-01T00:00:00.000007Z\t1\t2\t2.0\t4.0\t4.0\t2\t2\t2\t2\t2\t4.0\t0.0\n";

            assertQueryNoLeakCheck(
                    result3,
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts rows between unbounded preceding and 2 preceding), " +
                            "sum(j) over (partition by i order by ts rows between unbounded preceding and 2 preceding), " +
                            "first_value(j) over (partition by i order by ts rows between unbounded preceding and 2 preceding), " +
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
                    "ts\ti\tj\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t1.0\t1.0\t1.0\t1\t1\t1\t1\t1\t1.0\t1.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\t2.0\t2.0\t2.0\t1\t1\t1\t1\t1\t2.0\t2.0\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t3.0\t3.0\t3.0\t1\t1\t1\t1\t1\t3.0\t3.0\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\t4.0\t4.0\t4.0\t1\t1\t1\t1\t1\t4.0\t4.0\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t0.0\t0.0\t0.0\t1\t1\t1\t1\t1\t0.0\t0.0\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t1.0\t1.0\t1.0\t1\t1\t1\t1\t1\t1.0\t1.0\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\t2.0\t2.0\t2.0\t1\t1\t1\t1\t1\t2.0\t2.0\n",
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts rows current row), " +
                            "sum(j) over (partition by i order by ts rows current row), " +
                            "first_value(j) over (partition by i order by ts rows current row), " +
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
                    "avg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\tts\ti\tj\n" +
                            "1.0\t1.0\t1.0\t1\t1\t1\t1\t1\t1.0\t1.0\t1970-01-01T00:00:00.000001Z\t0\t1\n" +
                            "1.5\t3.0\t1.0\t2\t2\t2\t2\t2\t2.0\t1.0\t1970-01-01T00:00:00.000002Z\t0\t2\n" +
                            "2.5\t5.0\t2.0\t2\t2\t2\t2\t2\t3.0\t2.0\t1970-01-01T00:00:00.000003Z\t0\t3\n" +
                            "4.0\t4.0\t4.0\t1\t1\t1\t1\t1\t4.0\t4.0\t1970-01-01T00:00:00.000004Z\t1\t4\n" +
                            "2.0\t4.0\t4.0\t2\t2\t2\t2\t2\t4.0\t0.0\t1970-01-01T00:00:00.000005Z\t1\t0\n" +
                            "0.5\t1.0\t0.0\t2\t2\t2\t2\t2\t1.0\t0.0\t1970-01-01T00:00:00.000006Z\t1\t1\n" +
                            "1.5\t3.0\t1.0\t2\t2\t2\t2\t2\t2.0\t1.0\t1970-01-01T00:00:00.000007Z\t1\t2\n",
                    "select avg(j) over (partition by i order by ts rows between 1 preceding and current row), " +
                            "sum(j) over (partition by i order by ts rows between 1 preceding and current row), " +
                            "first_value(j) over (partition by i order by ts rows between 1 preceding and current row), " +
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
                    "avg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\ti\tj\n" +
                            "1.0\t1.0\t1.0\t1\t1\t1\t1\t1\t1.0\t1.0\t0\t1\n" +
                            "1.5\t3.0\t1.0\t2\t2\t2\t2\t2\t2.0\t1.0\t0\t2\n" +
                            "2.5\t5.0\t2.0\t2\t2\t2\t2\t2\t3.0\t2.0\t0\t3\n" +
                            "4.0\t4.0\t4.0\t1\t1\t1\t1\t1\t4.0\t4.0\t1\t4\n" +
                            "2.0\t4.0\t4.0\t2\t2\t2\t2\t2\t4.0\t0.0\t1\t0\n" +
                            "0.5\t1.0\t0.0\t2\t2\t2\t2\t2\t1.0\t0.0\t1\t1\n" +
                            "1.5\t3.0\t1.0\t2\t2\t2\t2\t2\t2.0\t1.0\t1\t2\n",
                    "select avg(j) over (partition by i order by ts rows between 1 preceding and current row), " +
                            "sum(j) over (partition by i order by ts rows between 1 preceding and current row), " +
                            "first_value(j) over (partition by i order by ts rows between 1 preceding and current row), " +
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

            String result4 = "avg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\n" +
                    "1.5\t3.0\t2.0\t2\t2\t2\t2\t2\t2.0\t1.0\n" +
                    "2.5\t5.0\t3.0\t2\t2\t2\t2\t2\t3.0\t2.0\n" +
                    "3.0\t3.0\t3.0\t1\t1\t1\t1\t1\t3.0\t3.0\n" +
                    "2.0\t4.0\t0.0\t2\t2\t2\t2\t2\t4.0\t0.0\n" +
                    "0.5\t1.0\t1.0\t2\t2\t2\t2\t2\t1.0\t0.0\n" +
                    "1.5\t3.0\t2.0\t2\t2\t2\t2\t2\t2.0\t1.0\n" +
                    "2.0\t2.0\t2.0\t1\t1\t1\t1\t1\t2.0\t2.0\n";
            assertQueryNoLeakCheck(
                    result4,
                    "select avg(j) over (partition by i order by ts desc rows between 1 preceding and current row), " +
                            "sum(j) over (partition by i order by ts desc rows between 1 preceding and current row), " +
                            "first_value(j) over (partition by i order by ts desc rows between 1 preceding and current row), " +
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
                    false
            );

            assertQueryNoLeakCheck(
                    result4,
                    "select avg(j) over (partition by i order by ts desc rows between 1 preceding and current row), " +
                            "sum(j) over (partition by i order by ts desc rows between 1 preceding and current row), " +
                            "first_value(j) over (partition by i order by ts desc rows between 1 preceding and current row), " +
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
                    false
            );

            assertQueryNoLeakCheck(
                    "avg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\ti\tj\n" +
                            "1.0\t1.0\t1.0\t1\t1\t1\t1\t1\t1.0\t1.0\t0\t1\n" +
                            "1.5\t3.0\t1.0\t2\t2\t2\t2\t2\t2.0\t1.0\t0\t2\n" +
                            "2.5\t5.0\t2.0\t2\t2\t2\t2\t2\t3.0\t2.0\t0\t3\n" +
                            "0.0\t0.0\t0.0\t1\t1\t1\t1\t1\t0.0\t0.0\t1\t0\n" +
                            "0.5\t1.0\t0.0\t2\t2\t2\t2\t2\t1.0\t0.0\t1\t1\n" +
                            "1.5\t3.0\t1.0\t2\t2\t2\t2\t2\t2.0\t1.0\t1\t2\n" +
                            "3.0\t6.0\t2.0\t2\t2\t2\t2\t2\t4.0\t2.0\t1\t4\n",
                    "select avg(j) over (partition by i order by j, i  desc rows between 1 preceding and current row), " +
                            "sum(j) over (partition by i order by j, i  desc rows between 1 preceding and current row), " +
                            "first_value(j) over (partition by i order by j, i  desc rows between 1 preceding and current row), " +
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
                    false
            );
        });
    }

    @Test
    public void testNegativeLimitWindowOrderedByNotTimestamp() throws Exception {
        // https://github.com/questdb/questdb/issues/4748
        assertMemoryLeak(() -> assertQuery("x\trow_number\n" +
                        "1\t1\n" +
                        "2\t2\n" +
                        "3\t3\n" +
                        "4\t4\n" +
                        "5\t5\n" +
                        "6\t6\n" +
                        "7\t7\n" +
                        "8\t8\n" +
                        "9\t9\n" +
                        "10\t10\n",
                "SELECT x, row_number() OVER (\n" +
                        "    ORDER BY x asc\n" +
                        "    RANGE UNBOUNDED PRECEDING\n" +
                        ")\n" +
                        "FROM long_sequence(10)\n" +
                        "limit -10", false));
    }

    @Test
    public void testPartitionByAndOrderByColumnPushdown() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, i long, j long, d double, s symbol, c VARCHAR) timestamp(ts)");
            execute("insert into tab select x::timestamp, x/4, x%5, x*2::double, 'k' || (x%5) ::symbol, 'k' || x from long_sequence(7)");

            // row_number()
            assertQueryNoLeakCheck(
                    "row_number\n" +
                            "3\n" +
                            "2\n" +
                            "1\n" +
                            "4\n" +
                            "3\n" +
                            "2\n" +
                            "1\n",
                    "select row_number() over (partition by i order by ts desc) " +
                            "from tab " +
                            "order by ts asc",
                    null,
                    true, // cached window factory
                    false
            );

            assertQueryNoLeakCheck(
                    "row_number\n" +
                            "1\n" +
                            "2\n" +
                            "3\n" +
                            "4\n" +
                            "1\n" +
                            "2\n" +
                            "3\n",
                    "select row_number() over (partition by i order by ts desc)" +
                            "from tab " +
                            "order by ts desc",
                    null,
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    "row_number\n" +
                            "1\n" +
                            "2\n" +
                            "3\n" +
                            "1\n" +
                            "2\n" +
                            "3\n" +
                            "4\n",
                    "select row_number() over (partition by i order by ts asc) " +
                            "from tab " +
                            "order by ts asc",
                    null,
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    "row_number\n" +
                            "4\n" +
                            "3\n" +
                            "2\n" +
                            "1\n" +
                            "3\n" +
                            "2\n" +
                            "1\n",
                    "select row_number() over (partition by i order by ts asc) " +
                            "from tab " +
                            "order by ts desc",
                    null,
                    true, // cached window factory
                    false
            );

            assertQueryNoLeakCheck(
                    "row_number\n" +
                            "3\n" +
                            "2\n" +
                            "1\n" +
                            "4\n" +
                            "3\n" +
                            "2\n" +
                            "1\n",
                    "select row_number() over (partition by i order by i, j asc) " +
                            "from tab " +
                            "order by ts desc",
                    null,
                    true, // cached window factory
                    false
            );

            assertPlanNoLeakCheck(
                    "select row_number() over (partition by i order by ts asc), " +
                            "   avg(j) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   sum(j) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   first_value(j) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   count(*) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   count(j) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   count(s) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   count(d) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   count(c) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   max(j) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   min(j) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   rank() over (partition by i order by j asc) " +
                            "from tab " +
                            "order by ts asc",
                    "SelectedRecord\n" +
                            "    CachedWindow\n" +
                            "      orderedFunctions: [[j] => [rank() over (partition by [i])],[ts desc] => [avg(j) over (partition by [i] rows between unbounded preceding and current row ),sum(j) over (partition by [i] rows between unbounded preceding and current row ),first_value(j) over (partition by [i] rows between unbounded preceding and current row ),count(*) over (partition by [i] rows between unbounded preceding and current row ),count(j) over (partition by [i] rows between unbounded preceding and current row ),count(s) over (partition by [i] rows between unbounded preceding and current row ),count(d) over (partition by [i] rows between unbounded preceding and current row ),count(c) over (partition by [i] rows between unbounded preceding and current row ),max(j) over (partition by [i] rows between unbounded preceding and current row ),min(j) over (partition by [i] rows between unbounded preceding and current row )]]\n" +
                            "      unorderedFunctions: [row_number() over (partition by [i])]\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tab\n"
            );

            assertQueryNoLeakCheck(
                    "row_number\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\trank\n" +
                            "1\t2.0\t6.0\t3.0\t3\t3\t3\t3\t3\t3.0\t1.0\t1\n" +
                            "2\t2.5\t5.0\t3.0\t2\t2\t2\t2\t2\t3.0\t2.0\t2\n" +
                            "3\t3.0\t3.0\t3.0\t1\t1\t1\t1\t1\t3.0\t3.0\t3\n" +
                            "1\t1.75\t7.0\t2.0\t4\t4\t4\t4\t4\t4.0\t0.0\t4\n" +
                            "2\t1.0\t3.0\t2.0\t3\t3\t3\t3\t3\t2.0\t0.0\t1\n" +
                            "3\t1.5\t3.0\t2.0\t2\t2\t2\t2\t2\t2.0\t1.0\t2\n" +
                            "4\t2.0\t2.0\t2.0\t1\t1\t1\t1\t1\t2.0\t2.0\t3\n",
                    "select row_number() over (partition by i order by ts asc), " +
                            "   avg(j) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   sum(j) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   first_value(j) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   count(*) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   count(j) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   count(s) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   count(d) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   count(c) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   max(j) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   min(j) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   rank() over (partition by i order by j asc) " +
                            "from tab " +
                            "order by ts asc",
                    null,
                    true, // cached window factory
                    false
            );

            assertQueryNoLeakCheck(
                    "row_number\tavg\tsum\tfirst_value\tcount\tcount1\tcount2\tcount3\tcount4\tmax\tmin\trank\n" +
                            "4\t2.0\t2.0\t2.0\t1\t1\t1\t1\t1\t2.0\t2.0\t3\n" +
                            "3\t1.5\t3.0\t2.0\t2\t2\t2\t2\t2\t2.0\t1.0\t2\n" +
                            "2\t1.0\t3.0\t2.0\t3\t3\t3\t3\t3\t2.0\t0.0\t1\n" +
                            "1\t1.75\t7.0\t2.0\t4\t4\t4\t4\t4\t4.0\t0.0\t4\n" +
                            "3\t3.0\t3.0\t3.0\t1\t1\t1\t1\t1\t3.0\t3.0\t3\n" +
                            "2\t2.5\t5.0\t3.0\t2\t2\t2\t2\t2\t3.0\t2.0\t2\n" +
                            "1\t2.0\t6.0\t3.0\t3\t3\t3\t3\t3\t3.0\t1.0\t1\n",
                    "select row_number() over (partition by i order by ts asc), " +
                            "   avg(j) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   sum(j) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   first_value(j) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   count(*) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   count(j) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   count(s) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   count(d) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   count(c) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   max(j) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   min(j) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   rank() over (partition by i order by j asc) " +
                            "from tab " +
                            "order by ts desc",
                    null,
                    true,
                    false
            );
        });
    }

    @Test
    public void testRankWithNoPartitionByAndNoOrderByWildcardLast() throws Exception {
        assertQuery(
                "rank\tprice\tsymbol\tts\n" +
                        "1\t1\tBB\t1970-01-01T00:00:00.000000Z\n" +
                        "1\t2\tCC\t1970-01-02T03:46:40.000000Z\n" +
                        "1\t2\tAA\t1970-01-03T07:33:20.000000Z\n" +
                        "1\t1\tCC\t1970-01-04T11:20:00.000000Z\n" +
                        "1\t2\tBB\t1970-01-05T15:06:40.000000Z\n" +
                        "1\t1\tBB\t1970-01-06T18:53:20.000000Z\n" +
                        "1\t1\tBB\t1970-01-07T22:40:00.000000Z\n" +
                        "1\t1\tCC\t1970-01-09T02:26:40.000000Z\n" +
                        "1\t1\tCC\t1970-01-10T06:13:20.000000Z\n" +
                        "1\t2\tAA\t1970-01-11T10:00:00.000000Z\n",
                "select rank() over (), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_int(1,2,3) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                "ts",
                true,
                false
        );
    }

    @Test
    public void testRankWithNoPartitionByAndOrderBySymbolWildcardLast() throws Exception {
        assertQuery(
                "rank\tprice\tsymbol\tts\n" +
                        "3\t1\tBB\t1970-01-01T00:00:00.000000Z\n" +
                        "7\t2\tCC\t1970-01-02T03:46:40.000000Z\n" +
                        "1\t2\tAA\t1970-01-03T07:33:20.000000Z\n" +
                        "7\t1\tCC\t1970-01-04T11:20:00.000000Z\n" +
                        "3\t2\tBB\t1970-01-05T15:06:40.000000Z\n" +
                        "3\t1\tBB\t1970-01-06T18:53:20.000000Z\n" +
                        "3\t1\tBB\t1970-01-07T22:40:00.000000Z\n" +
                        "7\t1\tCC\t1970-01-09T02:26:40.000000Z\n" +
                        "7\t1\tCC\t1970-01-10T06:13:20.000000Z\n" +
                        "1\t2\tAA\t1970-01-11T10:00:00.000000Z\n",
                "select rank() over (order by symbol), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_int(1,2,3) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                "ts",
                true,
                false
        );
    }

    @Test
    public void testRankWithPartitionAndOrderByNonSymbol() throws Exception {
        assertQuery(
                "rank\tprice\tts\n" +
                        "1\t42\t1970-01-01T00:00:00.000000Z\n" +
                        "2\t42\t1970-01-02T03:46:40.000000Z\n" +
                        "3\t42\t1970-01-03T07:33:20.000000Z\n" +
                        "4\t42\t1970-01-04T11:20:00.000000Z\n" +
                        "5\t42\t1970-01-05T15:06:40.000000Z\n" +
                        "6\t42\t1970-01-06T18:53:20.000000Z\n" +
                        "7\t42\t1970-01-07T22:40:00.000000Z\n" +
                        "8\t42\t1970-01-09T02:26:40.000000Z\n" +
                        "9\t42\t1970-01-10T06:13:20.000000Z\n" +
                        "10\t42\t1970-01-11T10:00:00.000000Z\n",
                "select rank() over (partition by price order by ts), price, ts from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " 42 price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
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
                "rank\n" +
                        "1\n" +
                        "1\n" +
                        "1\n" +
                        "1\n" +
                        "1\n" +
                        "1\n" +
                        "1\n" +
                        "1\n" +
                        "1\n" +
                        "1\n",
                "select rank() over (partition by symbol order by symbol) from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(42) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                null,
                true,
                false
        );
    }

    @Test
    public void testRankWithPartitionAndOrderBySymbolWildcardFirst() throws Exception {
        assertQuery(
                "price\tsymbol\tts\trank\n" +
                        "0.8043224099968393\tCC\t1970-01-01T00:00:00.000000Z\t1\n" +
                        "0.2845577791213847\tBB\t1970-01-02T03:46:40.000000Z\t1\n" +
                        "0.9344604857394011\tCC\t1970-01-03T07:33:20.000000Z\t1\n" +
                        "0.7905675319675964\tAA\t1970-01-04T11:20:00.000000Z\t1\n" +
                        "0.8899286912289663\tBB\t1970-01-05T15:06:40.000000Z\t1\n" +
                        "0.11427984775756228\tCC\t1970-01-06T18:53:20.000000Z\t1\n" +
                        "0.4217768841969397\tBB\t1970-01-07T22:40:00.000000Z\t1\n" +
                        "0.7261136209823622\tBB\t1970-01-09T02:26:40.000000Z\t1\n" +
                        "0.6693837147631712\tBB\t1970-01-10T06:13:20.000000Z\t1\n" +
                        "0.8756771741121929\tBB\t1970-01-11T10:00:00.000000Z\t1\n",
                "select *, rank() over (partition by symbol order by symbol) from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(42) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                "ts",
                true,
                false
        );
    }

    @Test
    public void testRankWithPartitionAndOrderBySymbolWildcardLast() throws Exception {
        assertQuery(
                "rank\tprice\tsymbol\tts\n" +
                        "1\t0.8043224099968393\tCC\t1970-01-01T00:00:00.000000Z\n" +
                        "1\t0.2845577791213847\tBB\t1970-01-02T03:46:40.000000Z\n" +
                        "1\t0.9344604857394011\tCC\t1970-01-03T07:33:20.000000Z\n" +
                        "1\t0.7905675319675964\tAA\t1970-01-04T11:20:00.000000Z\n" +
                        "1\t0.8899286912289663\tBB\t1970-01-05T15:06:40.000000Z\n" +
                        "1\t0.11427984775756228\tCC\t1970-01-06T18:53:20.000000Z\n" +
                        "1\t0.4217768841969397\tBB\t1970-01-07T22:40:00.000000Z\n" +
                        "1\t0.7261136209823622\tBB\t1970-01-09T02:26:40.000000Z\n" +
                        "1\t0.6693837147631712\tBB\t1970-01-10T06:13:20.000000Z\n" +
                        "1\t0.8756771741121929\tBB\t1970-01-11T10:00:00.000000Z\n",
                "select rank() over (partition by symbol order by symbol), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(42) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                "ts",
                true,
                false
        );
    }

    @Test
    public void testRankWithPartitionBySymbolAndMultiOrderWildcardLast() throws Exception {
        assertQuery(
                "rank\tprice\tsymbol\tts\n" +
                        "1\t1\tBB\t1970-01-01T00:00:00.000000Z\n" +
                        "4\t2\tCC\t1970-01-02T03:46:40.000000Z\n" +
                        "1\t2\tAA\t1970-01-03T07:33:20.000000Z\n" +
                        "1\t1\tCC\t1970-01-04T11:20:00.000000Z\n" +
                        "4\t2\tBB\t1970-01-05T15:06:40.000000Z\n" +
                        "1\t1\tBB\t1970-01-06T18:53:20.000000Z\n" +
                        "1\t1\tBB\t1970-01-07T22:40:00.000000Z\n" +
                        "1\t1\tCC\t1970-01-09T02:26:40.000000Z\n" +
                        "1\t1\tCC\t1970-01-10T06:13:20.000000Z\n" +
                        "1\t2\tAA\t1970-01-11T10:00:00.000000Z\n",
                "select rank() over (partition by symbol order by symbol, price), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_int(1,2,3) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                "ts",
                true,
                false
        );
    }

    @Test
    public void testRankWithPartitionBySymbolAndNoOrderWildcardLast() throws Exception {
        assertQuery(
                "rank\tprice\tsymbol\tts\n" +
                        "1\t1\tBB\t1970-01-01T00:00:00.000000Z\n" +
                        "1\t2\tCC\t1970-01-02T03:46:40.000000Z\n" +
                        "1\t2\tAA\t1970-01-03T07:33:20.000000Z\n" +
                        "2\t1\tCC\t1970-01-04T11:20:00.000000Z\n" +
                        "2\t2\tBB\t1970-01-05T15:06:40.000000Z\n" +
                        "3\t1\tBB\t1970-01-06T18:53:20.000000Z\n" +
                        "4\t1\tBB\t1970-01-07T22:40:00.000000Z\n" +
                        "3\t1\tCC\t1970-01-09T02:26:40.000000Z\n" +
                        "4\t1\tCC\t1970-01-10T06:13:20.000000Z\n" +
                        "2\t2\tAA\t1970-01-11T10:00:00.000000Z\n",
                "select rank() over (partition by symbol), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_int(1,2,3) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
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
                "rank\tprice\tsymbol\tts\n" +
                        "2\t1\tBB\t1970-01-01T00:00:00.000000Z\n" +
                        "1\t2\tCC\t1970-01-02T03:46:40.000000Z\n" +
                        "1\t2\tAA\t1970-01-03T07:33:20.000000Z\n" +
                        "2\t1\tCC\t1970-01-04T11:20:00.000000Z\n" +
                        "1\t2\tBB\t1970-01-05T15:06:40.000000Z\n" +
                        "2\t1\tBB\t1970-01-06T18:53:20.000000Z\n" +
                        "2\t1\tBB\t1970-01-07T22:40:00.000000Z\n" +
                        "2\t1\tCC\t1970-01-09T02:26:40.000000Z\n" +
                        "2\t1\tCC\t1970-01-10T06:13:20.000000Z\n" +
                        "1\t2\tAA\t1970-01-11T10:00:00.000000Z\n",
                "select rank() over (partition by symbol order by price desc), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_int(1,2,3) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                "ts",
                true,
                false
        );
    }

    @Test
    public void testRankWithPartitionBySymbolAndOrderByIntPriceWildcardLast() throws Exception {
        assertQuery(
                "rank\tprice\tsymbol\tts\n" +
                        "1\t1\tBB\t1970-01-01T00:00:00.000000Z\n" +
                        "4\t2\tCC\t1970-01-02T03:46:40.000000Z\n" +
                        "1\t2\tAA\t1970-01-03T07:33:20.000000Z\n" +
                        "1\t1\tCC\t1970-01-04T11:20:00.000000Z\n" +
                        "4\t2\tBB\t1970-01-05T15:06:40.000000Z\n" +
                        "1\t1\tBB\t1970-01-06T18:53:20.000000Z\n" +
                        "1\t1\tBB\t1970-01-07T22:40:00.000000Z\n" +
                        "1\t1\tCC\t1970-01-09T02:26:40.000000Z\n" +
                        "1\t1\tCC\t1970-01-10T06:13:20.000000Z\n" +
                        "1\t2\tAA\t1970-01-11T10:00:00.000000Z\n",
                "select rank() over (partition by symbol order by price), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_int(1,2,3) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                "ts",
                true,
                false
        );
    }

    @Test
    public void testRankWithPartitionBySymbolAndOrderByPriceWildcardLast() throws Exception {
        assertQuery(
                "rank\tprice\tsymbol\tts\n" +
                        "2\t0.8043224099968393\tCC\t1970-01-01T00:00:00.000000Z\n" +
                        "1\t0.2845577791213847\tBB\t1970-01-02T03:46:40.000000Z\n" +
                        "3\t0.9344604857394011\tCC\t1970-01-03T07:33:20.000000Z\n" +
                        "1\t0.7905675319675964\tAA\t1970-01-04T11:20:00.000000Z\n" +
                        "6\t0.8899286912289663\tBB\t1970-01-05T15:06:40.000000Z\n" +
                        "1\t0.11427984775756228\tCC\t1970-01-06T18:53:20.000000Z\n" +
                        "2\t0.4217768841969397\tBB\t1970-01-07T22:40:00.000000Z\n" +
                        "4\t0.7261136209823622\tBB\t1970-01-09T02:26:40.000000Z\n" +
                        "3\t0.6693837147631712\tBB\t1970-01-10T06:13:20.000000Z\n" +
                        "5\t0.8756771741121929\tBB\t1970-01-11T10:00:00.000000Z\n",
                "select rank() over (partition by symbol order by price), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(42) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                "ts",
                true,
                false
        );
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
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                7,
                "window function called in non-window context, make sure to add OVER clause"
        );
    }

    @Test
    public void testRowNumberWithFilter() throws Exception {
        assertQuery(
                "author\tsym\tcommits\trk\n" +
                        "user1\tETH\t3\t1\n" +
                        "user2\tETH\t3\t2\n",
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
                        " timestamp_sequence(0, 100000000000) ts" +
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
                "x\ty\trn\n" +
                        "1\t1\t10\n" +
                        "2\t0\t5\n" +
                        "3\t1\t9\n" +
                        "4\t0\t4\n" +
                        "5\t1\t8\n" +
                        "6\t0\t3\n" +
                        "7\t1\t7\n" +
                        "8\t0\t2\n" +
                        "9\t1\t6\n" +
                        "10\t0\t1\n",
                "select *, row_number() over (order by y asc, x desc) as rn from tab order by x asc",
                "create table tab as (select x, x%2 y from long_sequence(10))",
                null,
                true,
                false
        );
    }

    @Test
    public void testRowNumberWithNoPartitionAndNoOrderInSubQuery() throws Exception {
        assertQuery(
                "symbol\trn\n" +
                        "CC\t2\n" +
                        "BB\t3\n" +
                        "CC\t4\n" +
                        "AA\t5\n" +
                        "BB\t6\n" +
                        "CC\t7\n" +
                        "BB\t8\n" +
                        "BB\t9\n" +
                        "BB\t10\n" +
                        "BB\t11\n",
                "select symbol, rn + 1 as rn from (select symbol, row_number() over() as rn from trades)",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(42) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
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
                "row_number\tprice\tsymbol\tts\n" +
                        "1\t0.8043224099968393\tCC\t1970-01-01T00:00:00.000000Z\n" +
                        "2\t0.2845577791213847\tBB\t1970-01-02T03:46:40.000000Z\n" +
                        "3\t0.9344604857394011\tCC\t1970-01-03T07:33:20.000000Z\n" +
                        "4\t0.7905675319675964\tAA\t1970-01-04T11:20:00.000000Z\n" +
                        "5\t0.8899286912289663\tBB\t1970-01-05T15:06:40.000000Z\n" +
                        "6\t0.11427984775756228\tCC\t1970-01-06T18:53:20.000000Z\n" +
                        "7\t0.4217768841969397\tBB\t1970-01-07T22:40:00.000000Z\n" +
                        "8\t0.7261136209823622\tBB\t1970-01-09T02:26:40.000000Z\n" +
                        "9\t0.6693837147631712\tBB\t1970-01-10T06:13:20.000000Z\n" +
                        "10\t0.8756771741121929\tBB\t1970-01-11T10:00:00.000000Z\n",
                "select row_number() over (), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(42) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
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
                "row_number\tprice\tsymbol\tts\n" +
                        "8\t0.8043224099968393\tCC\t1970-01-01T00:00:00.000000Z\n" +
                        "2\t0.2845577791213847\tBB\t1970-01-02T03:46:40.000000Z\n" +
                        "9\t0.9344604857394011\tCC\t1970-01-03T07:33:20.000000Z\n" +
                        "1\t0.7905675319675964\tAA\t1970-01-04T11:20:00.000000Z\n" +
                        "3\t0.8899286912289663\tBB\t1970-01-05T15:06:40.000000Z\n" +
                        "10\t0.11427984775756228\tCC\t1970-01-06T18:53:20.000000Z\n" +
                        "4\t0.4217768841969397\tBB\t1970-01-07T22:40:00.000000Z\n" +
                        "5\t0.7261136209823622\tBB\t1970-01-09T02:26:40.000000Z\n" +
                        "6\t0.6693837147631712\tBB\t1970-01-10T06:13:20.000000Z\n" +
                        "7\t0.8756771741121929\tBB\t1970-01-11T10:00:00.000000Z\n",
                "select row_number() over (order by symbol), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(42) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                "ts",
                true,
                false
        );
    }

    @Test
    public void testRowNumberWithNoPartitionAndSameOrderFollowedByBaseFactory() throws Exception {
        assertQuery(
                "ts\ts\trn\n" +
                        "1970-01-01T00:00:00.000000Z\ta\t1\n" +
                        "1970-01-02T03:46:40.000000Z\ta\t2\n" +
                        "1970-01-10T06:13:20.000000Z\ta\t3\n" +
                        "1970-01-03T07:33:20.000000Z\tb\t4\n" +
                        "1970-01-09T02:26:40.000000Z\tb\t5\n" +
                        "1970-01-11T10:00:00.000000Z\tb\t6\n" +
                        "1970-01-04T11:20:00.000000Z\tc\t7\n" +
                        "1970-01-05T15:06:40.000000Z\tc\t8\n" +
                        "1970-01-06T18:53:20.000000Z\tc\t9\n" +
                        "1970-01-07T22:40:00.000000Z\tc\t10\n",
                "select *, row_number() over (order by s) as rn from tab where ts in ('1970-01') order by s",
                "create table tab as " +
                        "(" +
                        "select" +
                        " timestamp_sequence(0, 100000000000) ts," +
                        " rnd_symbol('a','b','c') s" +
                        " from long_sequence(10)" +
                        "), index(s) timestamp(ts) partition by month",
                null,
                false,
                true
        );
    }

    @Test
    public void testRowNumberWithNoPartitionAndSameOrderNotFollowedByBaseFactory() throws Exception {
        assertQuery(
                "ts\ts\trn\n" +
                        "1970-01-01T00:00:00.000000Z\ta\t1\n" +
                        "1970-01-02T03:46:40.000000Z\ta\t2\n" +
                        "1970-01-10T06:13:20.000000Z\ta\t3\n" +
                        "1970-01-03T07:33:20.000000Z\tb\t4\n" +
                        "1970-01-09T02:26:40.000000Z\tb\t5\n" +
                        "1970-01-11T10:00:00.000000Z\tb\t6\n" +
                        "1970-01-04T11:20:00.000000Z\tc\t7\n" +
                        "1970-01-05T15:06:40.000000Z\tc\t8\n" +
                        "1970-01-06T18:53:20.000000Z\tc\t9\n" +
                        "1970-01-07T22:40:00.000000Z\tc\t10\n",
                "select *, row_number() over (order by s) as rn from tab where ts in ('1970-01') order by s",
                "create table tab as " +
                        "(" +
                        "select" +
                        " timestamp_sequence(0, 100000000000) ts," +
                        " rnd_symbol('a','b','c') s" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by month",
                null,
                true,
                false
        );
    }

    @Test
    public void testRowNumberWithPartitionAndOrderByNonSymbol() throws Exception {
        assertQuery(
                "row_number\tprice\tts\n" +
                        "1\t42\t1970-01-01T00:00:00.000000Z\n" +
                        "2\t42\t1970-01-02T03:46:40.000000Z\n" +
                        "3\t42\t1970-01-03T07:33:20.000000Z\n" +
                        "4\t42\t1970-01-04T11:20:00.000000Z\n" +
                        "5\t42\t1970-01-05T15:06:40.000000Z\n" +
                        "6\t42\t1970-01-06T18:53:20.000000Z\n" +
                        "7\t42\t1970-01-07T22:40:00.000000Z\n" +
                        "8\t42\t1970-01-09T02:26:40.000000Z\n" +
                        "9\t42\t1970-01-10T06:13:20.000000Z\n" +
                        "10\t42\t1970-01-11T10:00:00.000000Z\n",
                "select row_number() over (partition by price order by ts), price, ts from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " 42 price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
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
                "row_number\n" +
                        "1\n" +
                        "1\n" +
                        "2\n" +
                        "1\n" +
                        "2\n" +
                        "3\n" +
                        "3\n" +
                        "4\n" +
                        "5\n" +
                        "6\n",
                "select row_number() over (partition by symbol order by symbol) from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(42) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                null,
                true,
                false
        );
    }

    @Test
    public void testRowNumberWithPartitionAndOrderBySymbolWildcardFirst() throws Exception {
        assertQuery(
                "price\tsymbol\tts\trow_number\n" +
                        "0.8043224099968393\tCC\t1970-01-01T00:00:00.000000Z\t1\n" +
                        "0.2845577791213847\tBB\t1970-01-02T03:46:40.000000Z\t1\n" +
                        "0.9344604857394011\tCC\t1970-01-03T07:33:20.000000Z\t2\n" +
                        "0.7905675319675964\tAA\t1970-01-04T11:20:00.000000Z\t1\n" +
                        "0.8899286912289663\tBB\t1970-01-05T15:06:40.000000Z\t2\n" +
                        "0.11427984775756228\tCC\t1970-01-06T18:53:20.000000Z\t3\n" +
                        "0.4217768841969397\tBB\t1970-01-07T22:40:00.000000Z\t3\n" +
                        "0.7261136209823622\tBB\t1970-01-09T02:26:40.000000Z\t4\n" +
                        "0.6693837147631712\tBB\t1970-01-10T06:13:20.000000Z\t5\n" +
                        "0.8756771741121929\tBB\t1970-01-11T10:00:00.000000Z\t6\n",
                "select *, row_number() over (partition by symbol order by symbol) from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(42) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                "ts",
                true,
                false
        );
    }

    @Test
    public void testRowNumberWithPartitionAndOrderBySymbolWildcardLast() throws Exception {
        assertQuery(
                "row_number\tprice\tsymbol\tts\n" +
                        "1\t0.8043224099968393\tCC\t1970-01-01T00:00:00.000000Z\n" +
                        "1\t0.2845577791213847\tBB\t1970-01-02T03:46:40.000000Z\n" +
                        "2\t0.9344604857394011\tCC\t1970-01-03T07:33:20.000000Z\n" +
                        "1\t0.7905675319675964\tAA\t1970-01-04T11:20:00.000000Z\n" +
                        "2\t0.8899286912289663\tBB\t1970-01-05T15:06:40.000000Z\n" +
                        "3\t0.11427984775756228\tCC\t1970-01-06T18:53:20.000000Z\n" +
                        "3\t0.4217768841969397\tBB\t1970-01-07T22:40:00.000000Z\n" +
                        "4\t0.7261136209823622\tBB\t1970-01-09T02:26:40.000000Z\n" +
                        "5\t0.6693837147631712\tBB\t1970-01-10T06:13:20.000000Z\n" +
                        "6\t0.8756771741121929\tBB\t1970-01-11T10:00:00.000000Z\n",
                "select row_number() over (partition by symbol order by symbol), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(42) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                "ts",
                true,
                false
        );
    }

    @Test
    public void testWindowBufferExceedsLimit() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_STORE_PAGE_SIZE, 4096);
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_STORE_MAX_PAGES, 10);

        try {
            assertMemoryLeak(() -> {
                execute("create table tab (ts timestamp, i long, j long, d double, s symbol, c VARCHAR) timestamp(ts)");
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
            });
        } finally {
            // disable
            node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_STORE_PAGE_SIZE, 0);
            node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_STORE_MAX_PAGES, 0);
        }
    }

    @Test
    public void testWindowFactoryRetainsTimestampMetadata() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, i long, j long, c VARCHAR, sym symbol index) timestamp(ts)");

            // table scans
            assertQueryAndPlan(
                    "select ts, i, j, " +
                            "avg(j) over (), " +
                            "sum(j) over (), " +
                            "count(j) over (), " +
                            "count(*) over (), " +
                            "count(c) over (), " +
                            "count(sym) over (), " +
                            "max(j) over (), " +
                            "min(j) over (), " +
                            "row_number() over (), " +
                            "rank() over () " +
                            "from tab",
                    "CachedWindow\n" +
                            "  unorderedFunctions: [avg(j) over (),sum(j) over (),count(j) over (),count(*) over (),count(c) over (),count(sym) over (),max(j) over (),min(j) over (),row_number(),rank()]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: tab\n",
                    "ts\ti\tj\tavg\tsum\tcount\tcount1\tcount2\tcount3\tmax\tmin\trow_number\trank\n",
                    "ts",
                    true,
                    false
            );

            assertQueryAndPlan(
                    "select ts, i, j, avg(j) over (), sum(j) over (), count(*) over (), count(j) over (), count(sym) over (), count(c) over (), " +
                            "max(j) over (), min(j) over () from tab order by ts desc",
                    "CachedWindow\n" +
                            "  unorderedFunctions: [avg(j) over (),sum(j) over (),count(*) over (),count(j) over (),count(sym) over (),count(c) over (),max(j) over (),min(j) over ()]\n" +
                            "    PageFrame\n" +
                            "        Row backward scan\n" +
                            "        Frame backward scan on: tab\n",
                    "ts\ti\tj\tavg\tsum\tcount\tcount1\tcount2\tcount3\tmax\tmin\n",
                    "ts",
                    true,
                    false
            );

            assertQueryAndPlan(
                    "select ts, i, j, " +
                            "avg(j) over (order by ts), " +
                            "sum(j) over (order by ts), " +
                            "count(*) over (order by ts), " +
                            "count(j) over (order by ts), " +
                            "count(sym) over (order by ts), " +
                            "count(c) over (order by ts), " +
                            "max(j) over (order by ts), " +
                            "min(j) over (order by ts) " +
                            "from tab",
                    "Window\n" +
                            "  functions: [avg(j) over (rows between unbounded preceding and current row),sum(j) over (rows between unbounded preceding and current row),count(*) over (rows between unbounded preceding and current row),count(j) over (rows between unbounded preceding and current row),count(sym) over (rows between unbounded preceding and current row),count(c) over (rows between unbounded preceding and current row),max(j) over (rows between unbounded preceding and current row),min(j) over (rows between unbounded preceding and current row)]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: tab\n",
                    "ts\ti\tj\tavg\tsum\tcount\tcount1\tcount2\tcount3\tmax\tmin\n",
                    "ts",
                    false,
                    true
            );

            assertQueryAndPlan(
                    "select ts, i, j, " +
                            "avg(j) over (order by ts desc), " +
                            "sum(j) over (order by ts desc), " +
                            "count(*) over (order by ts desc), " +
                            "count(j) over (order by ts desc), " +
                            "count(sym) over (order by ts desc), " +
                            "count(c) over (order by ts desc), " +
                            "max(j) over (order by ts desc), " +
                            "min(j) over (order by ts desc) " +
                            "from tab order by ts desc",
                    "Window\n" +
                            "  functions: [avg(j) over (rows between unbounded preceding and current row),sum(j) over (rows between unbounded preceding and current row),count(*) over (rows between unbounded preceding and current row),count(j) over (rows between unbounded preceding and current row),count(sym) over (rows between unbounded preceding and current row),count(c) over (rows between unbounded preceding and current row),max(j) over (rows between unbounded preceding and current row),min(j) over (rows between unbounded preceding and current row)]\n" +
                            "    PageFrame\n" +
                            "        Row backward scan\n" +
                            "        Frame backward scan on: tab\n",
                    "ts\ti\tj\tavg\tsum\tcount\tcount1\tcount2\tcount3\tmax\tmin\n",
                    "ts",
                    false,
                    true
            );

            assertQueryAndPlan(
                    "select ts, i, j, " +
                            "avg(j) over (partition by i), " +
                            "sum(j) over (partition by i), " +
                            "count(*) over (partition by i), " +
                            "count(j) over (partition by i), " +
                            "count(sym) over (partition by i), " +
                            "count(c) over (partition by i), " +
                            "max(j) over (partition by i), " +
                            "min(j) over (partition by i) " +
                            "from tab",
                    "CachedWindow\n" +
                            "  unorderedFunctions: [avg(j) over (partition by [i]),sum(j) over (partition by [i]),count(*) over (partition by [i]),count(j) over (partition by [i]),count(sym) over (partition by [i]),count(c) over (partition by [i]),max(j) over (partition by [i]),min(j) over (partition by [i])]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: tab\n",
                    "ts\ti\tj\tavg\tsum\tcount\tcount1\tcount2\tcount3\tmax\tmin\n",
                    "ts",
                    true,
                    false
            );

            assertQueryAndPlan(
                    "select ts, i, j, " +
                            "avg(j) over (partition by i), " +
                            "sum(j) over (partition by i), " +
                            "count(*) over (partition by i), " +
                            "count(j) over (partition by i), " +
                            "count(sym) over (partition by i), " +
                            "count(c) over (partition by i), " +
                            "max(j) over (partition by i), " +
                            "min(j) over (partition by i) " +
                            "from tab order by ts desc",
                    "CachedWindow\n" +
                            "  unorderedFunctions: [avg(j) over (partition by [i]),sum(j) over (partition by [i]),count(*) over (partition by [i]),count(j) over (partition by [i]),count(sym) over (partition by [i]),count(c) over (partition by [i]),max(j) over (partition by [i]),min(j) over (partition by [i])]\n" +
                            "    PageFrame\n" +
                            "        Row backward scan\n" +
                            "        Frame backward scan on: tab\n",
                    "ts\ti\tj\tavg\tsum\tcount\tcount1\tcount2\tcount3\tmax\tmin\n",
                    "ts",
                    true,
                    false
            );

            assertQueryAndPlan(
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts), " +
                            "sum(j) over (partition by i order by ts), " +
                            "count(*) over (partition by i order by ts), " +
                            "count(j) over (partition by i order by ts), " +
                            "count(sym) over (partition by i order by ts), " +
                            "count(c) over (partition by i order by ts), " +
                            "max(j) over (partition by i order by ts), " +
                            "min(j) over (partition by i order by ts) " +
                            "from tab",
                    "Window\n" +
                            "  functions: [avg(j) over (partition by [i] rows between unbounded preceding and current row ),sum(j) over (partition by [i] rows between unbounded preceding and current row ),count(*) over (partition by [i] rows between unbounded preceding and current row ),count(j) over (partition by [i] rows between unbounded preceding and current row ),count(sym) over (partition by [i] rows between unbounded preceding and current row ),count(c) over (partition by [i] rows between unbounded preceding and current row ),max(j) over (partition by [i] rows between unbounded preceding and current row ),min(j) over (partition by [i] rows between unbounded preceding and current row )]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: tab\n",
                    "ts\ti\tj\tavg\tsum\tcount\tcount1\tcount2\tcount3\tmax\tmin\n",
                    "ts",
                    false,
                    true
            );

            assertQueryAndPlan(
                    "select ts, i, j, " +
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
                    "Window\n" +
                            "  functions: [avg(j) over (partition by [i] rows between unbounded preceding and current row ),sum(j) over (partition by [i] rows between unbounded preceding and current row ),count(*) over (partition by [i] rows between unbounded preceding and current row ),count(j) over (partition by [i] rows between unbounded preceding and current row ),count(sym) over (partition by [i] rows between unbounded preceding and current row ),count(c) over (partition by [i] rows between unbounded preceding and current row ),max(j) over (partition by [i] rows between unbounded preceding and current row ),min(j) over (partition by [i] rows between unbounded preceding and current row )]\n" +
                            "    PageFrame\n" +
                            "        Row backward scan\n" +
                            "        Frame backward scan on: tab\n",
                    "ts\ti\tj\tavg\tsum\tcount\tcount1\tcount2\tcount3\tmax\tmin\n",
                    "ts",
                    false,
                    true
            );

            assertQueryAndPlan(
                    "select i, j, " +
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
                    "SelectedRecord\n" +
                            "    Window\n" +
                            "      functions: [avg(j) over (partition by [i] rows between unbounded preceding and current row ),sum(j) over (partition by [i] rows between unbounded preceding and current row ),count(*) over (partition by [i] rows between unbounded preceding and current row ),count(j) over (partition by [i] rows between unbounded preceding and current row ),count(sym) over (partition by [i] rows between unbounded preceding and current row ),count(c) over (partition by [i] rows between unbounded preceding and current row ),max(j) over (partition by [i] rows between unbounded preceding and current row ),min(j) over (partition by [i] rows between unbounded preceding and current row )]\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tab\n",
                    "i\tj\tavg\tsum\tcount\tcount1\tcount2\tcount3\tmax\tmin\n",
                    null,
                    false,
                    true
            );

            assertQueryAndPlan(
                    "select i, j, " +
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
                    "SelectedRecord\n" +
                            "    Window\n" +
                            "      functions: [avg(j) over (partition by [i] rows between unbounded preceding and current row ),sum(j) over (partition by [i] rows between unbounded preceding and current row ),count(*) over (partition by [i] rows between unbounded preceding and current row ),count(j) over (partition by [i] rows between unbounded preceding and current row ),count(sym) over (partition by [i] rows between unbounded preceding and current row ),count(c) over (partition by [i] rows between unbounded preceding and current row ),max(j) over (partition by [i] rows between unbounded preceding and current row ),min(j) over (partition by [i] rows between unbounded preceding and current row )]\n" +
                            "        PageFrame\n" +
                            "            Row backward scan\n" +
                            "            Frame backward scan on: tab\n",
                    "i\tj\tavg\tsum\tcount\tcount1\tcount2\tcount3\tmax\tmin\n",
                    null,
                    false,
                    true
            );

            assertQueryAndPlan(
                    "select i, j, " +
                            "avg(j) over (partition by i order by ts range between 10 seconds preceding and current row), " +
                            "sum(j) over (partition by i order by ts range between 10 seconds preceding and current row), " +
                            "count(*) over (partition by i order by ts range between 10 seconds preceding and current row), " +
                            "count(j) over (partition by i order by ts range between 10 seconds preceding and current row), " +
                            "count(sym) over (partition by i order by ts range between 10 seconds preceding and current row), " +
                            "count(c) over (partition by i order by ts range between 10 seconds preceding and current row), " +
                            "max(j) over (partition by i order by ts range between 10 seconds preceding and current row), " +
                            "min(j) over (partition by i order by ts range between 10 seconds preceding and current row), " +
                            "ts from tab",
                    "Window\n" +
                            "  functions: [avg(j) over (partition by [i] range between 10000000 preceding and current row),sum(j) over (partition by [i] range between 10000000 preceding and current row),count(*) over (partition by [i] range between 10000000 preceding and current row),count(j) over (partition by [i] range between 10000000 preceding and current row),count(sym) over (partition by [i] range between 10000000 preceding and current row),count(c) over (partition by [i] range between 10000000 preceding and current row),max(j) over (partition by [i] range between 10000000 preceding and current row),min(j) over (partition by [i] range between 10000000 preceding and current row)]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: tab\n",
                    "i\tj\tavg\tsum\tcount\tcount1\tcount2\tcount3\tmax\tmin\tts\n",
                    "ts",
                    false,
                    true
            );

            // index scans
            assertQueryAndPlan(
                    "select ts, i, j, row_number() over () from tab where sym = 'X'",
                    "Window\n" +
                            "  functions: [row_number()]\n" +
                            "    DeferredSingleSymbolFilterPageFrame\n" +
                            "        Index forward scan on: sym deferred: true\n" +
                            "          filter: sym='X'\n" +
                            "        Frame forward scan on: tab\n",
                    "ts\ti\tj\trow_number\n",
                    "ts",
                    false,
                    false
            );

            assertQueryAndPlan(
                    "select ts, i, j, row_number() over () from tab where sym = 'X' order by ts desc",
                    "Window\n" +
                            "  functions: [row_number()]\n" +
                            "    DeferredSingleSymbolFilterPageFrame\n" +
                            "        Index backward scan on: sym deferred: true\n" +
                            "          filter: sym='X'\n" +
                            "        Frame backward scan on: tab\n",
                    "ts\ti\tj\trow_number\n",
                    "ts",
                    false,
                    false
            );

            assertQueryAndPlan(
                    "select ts, i, j, row_number() over () from tab where sym IN ('X', 'Y') order by sym",
                    "SelectedRecord\n" +
                            "    Window\n" +
                            "      functions: [row_number()]\n" +
                            "        FilterOnValues symbolOrder: asc\n" +
                            "            Cursor-order scan\n" +
                            "                Index forward scan on: sym deferred: true\n" +
                            "                  filter: sym='X'\n" +
                            "                Index forward scan on: sym deferred: true\n" +
                            "                  filter: sym='Y'\n" +
                            "            Frame forward scan on: tab\n",
                    "ts\ti\tj\trow_number\n",
                    null,
                    false,
                    false
            );
        });
    }

    @Test
    public void testWindowFunctionContextCleanup() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table trades as " +
                    "(" +
                    "select" +
                    " rnd_int(1,2,3) price," +
                    " rnd_symbol('AA','BB','CC') symbol," +
                    " timestamp_sequence(0, 100000000000) ts" +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by day", sqlExecutionContext);

            assertQueryNoLeakCheck(
                    "symbol\tprice\trow_number\n" +
                            "BB\t1\t1\n" +
                            "CC\t2\t2\n" +
                            "AA\t2\t1\n" +
                            "CC\t1\t1\n" +
                            "BB\t2\t2\n",
                    "select symbol, price, row_number() over (partition by symbol order by price) " +
                            "from trades",
                    null,
                    true,
                    false
            );

            // WindowContext should be properly clean up when we try to execute the next query.
            for (String function : WINDOW_ONLY_FUNCTIONS) {
                try {
                    execute("select #FUNCTION from trades".replace("#FUNCTION", function), sqlExecutionContext);
                    Assert.fail();
                } catch (SqlException e) {
                    Assert.assertEquals(7, e.getPosition());
                    TestUtils.assertContains(e.getFlyweightMessage(), "window function called in non-window context, make sure to add OVER clause");
                }
            }
        });
    }

    @Test
    public void testWindowFunctionDoesSortIfOrderByIsNotCompatibleWithBaseQuery() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, i long, j long, sym symbol index) timestamp(ts)");

            for (String func : FRAME_FUNCTIONS) {
                String replace = func.trim() + "(1)";
                if (replace.equals("count(1)")) {
                    replace = "count(*)";
                }

                assertPlanNoLeakCheck(
                        "select ts, i, j, #FUNCT_NAME(1) over (partition by i order by ts desc rows between 1 preceding and current row) from tab".replace("#FUNCT_NAME", func),
                        "CachedWindow\n" +
                                "  orderedFunctions: [[ts desc] => [#FUNCT_NAME(1) over (partition by [i] rows between 1 preceding and current row)]]\n".replace("#FUNCT_NAME(1)", replace) +
                                "    PageFrame\n" +
                                "        Row forward scan\n" +
                                "        Frame forward scan on: tab\n"
                );

                assertPlanNoLeakCheck(
                        "select ts, i, j, #FUNCT_NAME(1) over (partition by i order by ts asc rows between 1 preceding and current row)  from tab order by ts desc".replace("#FUNCT_NAME", func),
                        "CachedWindow\n" +
                                "  orderedFunctions: [[ts] => [#FUNCT_NAME(1) over (partition by [i] rows between 1 preceding and current row)]]\n".replace("#FUNCT_NAME(1)", replace) +
                                "    PageFrame\n" +
                                "        Row backward scan\n" +
                                "        Frame backward scan on: tab\n"
                );

                //TODO: inspect
                assertPlanNoLeakCheck(
                        "select ts, i, j, #FUNCT_NAME(1) over (partition by i order by ts asc rows between 1 preceding and current row) from tab where sym in ( 'A', 'B') ".replace("#FUNCT_NAME", func),
                        "first_value".equals(func) ?
                                "Window\n" +
                                        "  functions: [first_value(1) over (partition by [i] rows between 1 preceding and current row)]\n" +
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
                        "select ts, i, j, #FUNCT_NAME(1) over (partition by i order by ts desc rows between 1 preceding and current row)  from tab where sym = 'A'".replace("#FUNCT_NAME", func),
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
            execute("create table tab (ts timestamp, i long, j long, sym symbol index) timestamp(ts)");

            for (String func : FRAME_FUNCTIONS) {
                String replace = func.trim() + "(1)";
                if (replace.equals("count(1)")) {
                    replace = "count(*)";
                }
                assertPlanNoLeakCheck(
                        "select ts, i, j, #FUNCT_NAME(1) over (partition by i order by ts rows between 1 preceding and current row) from tab".replace("#FUNCT_NAME", func),
                        "Window\n" +
                                "  functions: [#FUNCT_NAME(1) over (partition by [i] rows between 1 preceding and current row)]\n".replace("#FUNCT_NAME(1)", replace) +
                                "    PageFrame\n" +
                                "        Row forward scan\n" +
                                "        Frame forward scan on: tab\n"
                );

                assertPlanNoLeakCheck(
                        "select ts, i, j, #FUNCT_NAME(1) over (partition by i order by ts rows between 1 preceding and current row)  from tab order by ts asc".replace("#FUNCT_NAME", func),
                        "Window\n" +
                                "  functions: [#FUNCT_NAME(1) over (partition by [i] rows between 1 preceding and current row)]\n".replace("#FUNCT_NAME(1)", replace) +
                                "    PageFrame\n" +
                                "        Row forward scan\n" +
                                "        Frame forward scan on: tab\n"
                );

                assertPlanNoLeakCheck(
                        "select ts, i, j, #FUNCT_NAME(1) over (partition by i order by ts desc rows between 1 preceding and current row)  from tab order by ts desc".replace("#FUNCT_NAME", func),
                        "Window\n" +
                                "  functions: [#FUNCT_NAME(1) over (partition by [i] rows between 1 preceding and current row)]\n".replace("#FUNCT_NAME(1)", replace) +
                                "    PageFrame\n" +
                                "        Row backward scan\n" +
                                "        Frame backward scan on: tab\n"
                );

                assertPlanNoLeakCheck(
                        "select ts, i, j, #FUNCT_NAME(1) over (partition by i order by ts asc rows between 1 preceding and current row)  from tab where sym = 'A'".replace("#FUNCT_NAME", func),
                        "Window\n" +
                                "  functions: [#FUNCT_NAME(1) over (partition by [i] rows between 1 preceding and current row)]\n".replace("#FUNCT_NAME(1)", replace) +
                                "    DeferredSingleSymbolFilterPageFrame\n" +
                                "        Index forward scan on: sym deferred: true\n" +
                                "          filter: sym='A'\n" +
                                "        Frame forward scan on: tab\n"
                );

                assertPlanNoLeakCheck(
                        "select ts, i, j, #FUNCT_NAME(1) over (partition by i order by ts asc rows between 1 preceding and current row) ".replace("#FUNCT_NAME", func) +
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
            Class<?>[] factories = new Class<?>[]{RankFunctionFactory.class,
                    RowNumberFunctionFactory.class,
                    AvgDoubleWindowFunctionFactory.class,
                    SumDoubleWindowFunctionFactory.class,
                    CountConstWindowFunctionFactory.class,
                    CountDoubleWindowFunctionFactory.class,
                    CountSymbolWindowFunctionFactory.class,
                    CountVarcharWindowFunctionFactory.class,
                    MaxDoubleWindowFunctionFactory.class,
                    MinDoubleWindowFunctionFactory.class,
                    FirstValueDoubleWindowFunctionFactory.class};

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
    public void testWindowFunctionInPartitionByFails() throws Exception {
        assertException(
                "SELECT pickup_datetime, row_number() OVER (PARTITION BY row_number())\n" +
                        "FROM trips\n" +
                        "WHERE pickup_datetime >= '2018-12-30' and pickup_datetime <= '2018-12-31'\n" +
                        "SAMPLE BY 1d",
                "create table trips as " +
                        "(" +
                        "select" +
                        " rnd_double(42) total_amount," +
                        " timestamp_sequence(0, 100000000000) pickup_datetime" +
                        " from long_sequence(10)" +
                        ") timestamp(pickup_datetime) partition by day",
                56,
                "window function called in non-window context, make sure to add OVER clause"
        );
    }

    @Test
    public void testWindowFunctionReleaseNativeMemory() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val long) timestamp(ts)");
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
                                    ("select *, #FUNCTION(length(#EXPRESSION)) " + frameVariant + " from tab ")
                                            .replace("#FUNCTION", function)
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
    public void testWindowOnlyFunctionFailsInNonWindowContext() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table trades as " +
                    "(" +
                    "select" +
                    " rnd_int(1,2,3) price," +
                    " rnd_symbol('AA','BB','CC') symbol," +
                    " timestamp_sequence(0, 100000000000) ts" +
                    " from long_sequence(10)" +
                    ") timestamp(ts) partition by day");

            for (String function : WINDOW_ONLY_FUNCTIONS) {
                assertExceptionNoLeakCheck(
                        "select #FUNCT_NAME, * from trades".replace("#FUNCT_NAME", function),
                        7,
                        "window function called in non-window context, make sure to add OVER clause"
                );
            }
        });
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

    private void assertQueryAndPlan(String query, String plan, String expectedResult, String expectedTimestamp, boolean supportsRandomAccess, boolean expectSize) throws Exception {
        assertPlanNoLeakCheck(query, plan);

        assertQueryNoLeakCheck(
                expectedResult,
                query,
                expectedTimestamp,
                supportsRandomAccess,
                expectSize
        );
    }

    private void assertWindowException(String query, int position, CharSequence errorMessage) throws Exception {
        for (String frameType : FRAME_TYPES) {
            assertExceptionNoLeakCheck(query.replace("#FRAME", frameType), position, errorMessage);
        }
    }

    static {
        FRAME_FUNCTIONS = Arrays.asList("avg", "sum", "first_value", "count", "max", "min");

        WINDOW_ONLY_FUNCTIONS = Arrays.asList("rank()", "row_number()", "first_value(1.0)");

        normalizeSuffix(FRAME_FUNCTIONS);
        normalizeSuffix(WINDOW_ONLY_FUNCTIONS);
    }
}
