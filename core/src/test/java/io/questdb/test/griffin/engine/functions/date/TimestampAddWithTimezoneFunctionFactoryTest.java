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

package io.questdb.test.griffin.engine.functions.date;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.date.TimestampAddWithTimezoneFunctionFactory;
import io.questdb.jit.JitUtil;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class TimestampAddWithTimezoneFunctionFactoryTest extends AbstractFunctionFactoryTest {

    public static char[] units = {'y', 'M', 'w', 'd', 'h', 'm', 's', 'T', 'u'};

    @Test
    public void testAllVariables() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table test_tab as (" +
                            "select" +
                            " timestamp_sequence(0, 10000) ts," +
                            " rnd_symbol('h', 'M', 'y', 'x', null) period," +
                            " rnd_symbol('08:00', 'Europe/Bratislava', 'unknown/unknown', null) tz," +
                            " rnd_int(0,10,1) stride from long_sequence(100)" +
                            ")"
            );

            assertExceptionNoLeakCheck(
                    "select dateadd(period, stride, ts, tz) val from test_tab",
                    23,
                    "`null` is not a valid stride"
            );

            assertExceptionNoLeakCheck(
                    "select dateadd(period, stride, ts, tz) val from test_tab where stride is not null",
                    35,
                    "invalid timezone [timezone=unknown/unknown]"
            );

            assertExceptionNoLeakCheck(
                    "select dateadd(period, stride, ts, tz) val from test_tab where stride is not null and tz <> 'unknown/unknown'",
                    15,
                    "invalid period [period=\u0000]"
            );

            assertExceptionNoLeakCheck(
                    "select dateadd(period, stride, ts, tz) val from test_tab where stride is not null and tz <> 'unknown/unknown' and period is not null",
                    35,
                    "NULL timezone"
            );

            assertExceptionNoLeakCheck(
                    "select dateadd(period, stride, ts, tz) val from test_tab where stride is not null and tz <> 'unknown/unknown' and period is not null and tz is not null",
                    15,
                    "invalid period [period=x]"
            );

            assertSql(
                    "val\tval2\tts\tperiod\tstride\ttz\n" +
                            "1970-11-01T00:00:00.010000Z\t\t1970-01-01T00:00:00.010000Z\tM\t10\tEurope/Bratislava\n" +
                            "1975-01-01T00:00:00.060000Z\t\t1970-01-01T00:00:00.060000Z\ty\t5\t08:00\n" +
                            "1975-01-01T00:00:00.140000Z\t\t1970-01-01T00:00:00.140000Z\ty\t5\t08:00\n" +
                            "1970-08-01T00:00:00.160000Z\t\t1970-01-01T00:00:00.160000Z\tM\t7\t08:00\n" +
                            "1977-01-01T00:00:00.210000Z\t\t1970-01-01T00:00:00.210000Z\ty\t7\tEurope/Bratislava\n" +
                            "1970-01-01T02:00:00.230000Z\t\t1970-01-01T00:00:00.230000Z\th\t2\tEurope/Bratislava\n" +
                            "1970-03-01T00:00:00.270000Z\t\t1970-01-01T00:00:00.270000Z\tM\t2\tEurope/Bratislava\n" +
                            "1970-04-01T00:00:00.290000Z\t\t1970-01-01T00:00:00.290000Z\tM\t3\t08:00\n" +
                            "1970-02-01T00:00:00.300000Z\t\t1970-01-01T00:00:00.300000Z\tM\t1\tEurope/Bratislava\n" +
                            "1978-01-01T00:00:00.450000Z\t\t1970-01-01T00:00:00.450000Z\ty\t8\t08:00\n" +
                            "1970-09-01T00:00:00.460000Z\t\t1970-01-01T00:00:00.460000Z\tM\t8\tEurope/Bratislava\n" +
                            "1970-11-01T00:00:00.540000Z\t\t1970-01-01T00:00:00.540000Z\tM\t10\tEurope/Bratislava\n" +
                            "1978-01-01T00:00:00.580000Z\t\t1970-01-01T00:00:00.580000Z\ty\t8\tEurope/Bratislava\n" +
                            "1970-04-01T00:00:00.590000Z\t\t1970-01-01T00:00:00.590000Z\tM\t3\t08:00\n" +
                            "1970-11-01T00:00:00.610000Z\t\t1970-01-01T00:00:00.610000Z\tM\t10\t08:00\n" +
                            "1972-01-01T00:00:00.650000Z\t\t1970-01-01T00:00:00.650000Z\ty\t2\t08:00\n" +
                            "1970-07-01T00:00:00.660000Z\t\t1970-01-01T00:00:00.660000Z\tM\t6\t08:00\n" +
                            "1970-01-01T00:00:00.670000Z\t\t1970-01-01T00:00:00.670000Z\tM\t0\t08:00\n" +
                            "1970-08-01T00:00:00.680000Z\t\t1970-01-01T00:00:00.680000Z\tM\t7\tEurope/Bratislava\n" +
                            "1970-01-01T04:00:00.720000Z\t\t1970-01-01T00:00:00.720000Z\th\t4\tEurope/Bratislava\n" +
                            "1970-01-01T00:00:00.780000Z\t\t1970-01-01T00:00:00.780000Z\tM\t0\t08:00\n" +
                            "1970-10-01T00:00:00.810000Z\t\t1970-01-01T00:00:00.810000Z\tM\t9\tEurope/Bratislava\n" +
                            "1970-01-01T04:00:00.860000Z\t\t1970-01-01T00:00:00.860000Z\th\t4\tEurope/Bratislava\n" +
                            "1970-01-01T02:00:00.910000Z\t\t1970-01-01T00:00:00.910000Z\th\t2\tEurope/Bratislava\n" +
                            "1972-01-01T00:00:00.920000Z\t\t1970-01-01T00:00:00.920000Z\ty\t2\t08:00\n",
                    "select dateadd(period, stride, ts, tz) val," +
                            " dateadd(period, stride, null, tz) val2," +
                            " ts," +
                            " period," +
                            " stride," +
                            " tz" +
                            " from test_tab" +
                            " where stride is not null" +
                            " and tz <> 'unknown/unknown'" +
                            " and period is not null" +
                            " and tz is not null" +
                            " and period <> 'x'"
            );

            if (JitUtil.isJitSupported()) {
                assertSql(
                        "QUERY PLAN\n" +
                                "VirtualRecord\n" +
                                "  functions: [dateadd('period',stride,ts,tz),dateadd('period',stride,null,tz),ts,period,stride,tz]\n" +
                                "    Async JIT Filter workers: 1\n" +
                                "      filter: ((stride!=null and tz!='unknown/unknown' and period is not null and tz is not null) and period!='x')\n" +
                                "        PageFrame\n" +
                                "            Row forward scan\n" +
                                "            Frame forward scan on: test_tab\n",
                        "explain select dateadd(period, stride, ts, tz) val," +
                                " dateadd(period, stride, null, tz) val2," +
                                " ts," +
                                " period," +
                                " stride," +
                                " tz" +
                                " from test_tab" +
                                " where stride is not null" +
                                " and tz <> 'unknown/unknown'" +
                                " and period is not null" +
                                " and tz is not null" +
                                " and period <> 'x'"
                );

            } else {
                assertSql(
                        "QUERY PLAN\n" +
                                "VirtualRecord\n" +
                                "  functions: [dateadd('period',stride,ts,tz),dateadd('period',stride,null,tz),ts,period,stride,tz]\n" +
                                "    Async Filter workers: 1\n" +
                                "      filter: ((stride!=null and tz!='unknown/unknown' and period is not null and tz is not null) and period!='x')\n" +
                                "        PageFrame\n" +
                                "            Row forward scan\n" +
                                "            Frame forward scan on: test_tab\n",
                        "explain select dateadd(period, stride, ts, tz) val," +
                                " dateadd(period, stride, null, tz) val2," +
                                " ts," +
                                " period," +
                                " stride," +
                                " tz" +
                                " from test_tab" +
                                " where stride is not null" +
                                " and tz <> 'unknown/unknown'" +
                                " and period is not null" +
                                " and tz is not null" +
                                " and period <> 'x'"
                );
            }
        });
    }

    @Test
    public void testDSTTransitionToUTC() throws Exception {
        // Input query performing DATEADD and converting to UTC
        String query = "select to_utc(dateadd('w', 1, '2024-10-21'), 'Europe/Bratislava') as utc_time";

        // Validate the result accounts for DST transition correctly
        assertQuery(
                "utc_time\n" +
                        "2024-10-27T23:00:00.000000Z\n",
                query
        );
    }

    @Test
    public void testDateAddEquivalenceWithUTC() throws Exception {
        // Direct calculation with DATEADD
        String directQuery = "select dateadd('w', 1, '2024-10-21', 'Europe/Bratislava') as direct";
        // Equivalent calculation with intermediate conversions
        String conversionQuery = "select to_utc(dateadd('w', 1, to_timezone('2024-10-21', 'Europe/Bratislava')), 'Europe/Bratislava') as via_conversion";

        // Validate the two produce the same result
        assertQuery(
                "direct\tvia_conversion\n" +
                        "2024-10-28T01:00:00.000000Z\t2024-10-28T01:00:00.000000Z\n",
                String.format("select direct, via_conversion from (%s) cross join (%s)", directQuery, conversionQuery)
        );
    }

    @Test
    public void testDateAddWithTimezonePlan() throws Exception {
        assertMemoryLeak(() -> assertPlanNoLeakCheck(
                // Input query testing DATEADD behavior
                "select dateadd('w', 1, '2024-10-21', 'Europe/Bratislava')",
                // Updated expected plan
                "VirtualRecord\n" +
                        "  functions: [1730077200000000]\n" +
                        "    long_sequence count: 1\n"
        ));
    }

    @Test
    public void testNullStride() throws Exception {
        for (int i = 0; i < units.length; i++) {
            assertException("select dateadd('" + units[i] + "', null, 1587275359886758L, 'Europe/Bratislava')", 20, "`null` is not a valid stride");
        }
    }

    @Test
    public void testNullTimestamp() throws Exception {
        for (int i = 0; i < units.length; i++) {
            assertSqlWithTypes("dateadd\n:TIMESTAMP\n", "select dateadd('" + units[i] + "', 5, null, 'Europe/Bratislava')");
        }
    }

    @Test
    public void testPeriodNullChar() throws Exception {
        assertException("select dateadd('\0', 5, 1587275359886758L, 'Europe/Bratislava')", 15, "invalid time period [unit=");
    }

    @Test
    public void testSimple() throws Exception {
        assertSqlWithTypes("dateadd\n2025-04-19T05:49:19.886758Z:TIMESTAMP\n", "select dateadd('y', 5, 1587275359886758L, 'Europe/Bratislava')");
        assertSqlWithTypes("dateadd\n2015-04-19T05:49:19.886758Z:TIMESTAMP\n", "select dateadd('y', -5, 1587275359886758L, 'Europe/Bratislava')");
        assertSqlWithTypes("dateadd\n2020-09-19T05:49:19.886758Z:TIMESTAMP\n", "select dateadd('M', 5, 1587275359886758L, 'Europe/Bratislava')");
        assertSqlWithTypes("dateadd\n2019-11-19T06:49:19.886758Z:TIMESTAMP\n", "select dateadd('M', -5, 1587275359886758L, 'Europe/Bratislava')");
        assertSqlWithTypes("dateadd\n2020-05-24T05:49:19.886758Z:TIMESTAMP\n", "select dateadd('w', 5, 1587275359886758L, 'Europe/Bratislava')");
        assertSqlWithTypes("dateadd\n2020-03-15T06:49:19.886758Z:TIMESTAMP\n", "select dateadd('w', -5, 1587275359886758L, 'Europe/Bratislava')");
        assertSqlWithTypes("dateadd\n2020-04-24T05:49:19.886758Z:TIMESTAMP\n", "select dateadd('d', 5, 1587275359886758L, 'Europe/Bratislava')");
        assertSqlWithTypes("dateadd\n2020-04-14T05:49:19.886758Z:TIMESTAMP\n", "select dateadd('d', -5, 1587275359886758L, 'Europe/Bratislava')");
        assertSqlWithTypes("dateadd\n2020-04-19T10:49:19.886758Z:TIMESTAMP\n", "select dateadd('h', 5, 1587275359886758L, 'Europe/Bratislava')");
        assertSqlWithTypes("dateadd\n2020-04-19T00:49:19.886758Z:TIMESTAMP\n", "select dateadd('h', -5, 1587275359886758L, 'Europe/Bratislava')");
        assertSqlWithTypes("dateadd\n2020-04-19T05:54:19.886758Z:TIMESTAMP\n", "select dateadd('m', 5, 1587275359886758L, 'Europe/Bratislava')");
        assertSqlWithTypes("dateadd\n2020-04-19T05:44:19.886758Z:TIMESTAMP\n", "select dateadd('m', -5, 1587275359886758L, 'Europe/Bratislava')");
        assertSqlWithTypes("dateadd\n2020-04-19T05:49:24.886758Z:TIMESTAMP\n", "select dateadd('s', 5, 1587275359886758L, 'Europe/Bratislava')");
        assertSqlWithTypes("dateadd\n2020-04-19T05:49:14.886758Z:TIMESTAMP\n", "select dateadd('s', -5, 1587275359886758L, 'Europe/Bratislava')");
        assertSqlWithTypes("dateadd\n2020-04-19T05:49:19.891758Z:TIMESTAMP\n", "select dateadd('T', 5, 1587275359886758L, 'Europe/Bratislava')");
        assertSqlWithTypes("dateadd\n2020-04-19T05:49:19.881758Z:TIMESTAMP\n", "select dateadd('T', -5, 1587275359886758L, 'Europe/Bratislava')");
        assertSqlWithTypes("dateadd\n2020-04-19T05:49:19.886763Z:TIMESTAMP\n", "select dateadd('u', 5, 1587275359886758L, 'Europe/Bratislava')");
        assertSqlWithTypes("dateadd\n2020-04-19T05:49:19.886753Z:TIMESTAMP\n", "select dateadd('u', -5, 1587275359886758L, 'Europe/Bratislava')");
    }

    @Test
    public void testSimplePlan() throws SqlException {
        assertSql(
                "QUERY PLAN\n" +
                        "VirtualRecord\n" +
                        "  functions: [dateadd('u',-5,now(),'Europe/Bratislava')]\n" +
                        "    long_sequence count: 1\n",
                "explain select dateadd('u', -5, now, 'Europe/Bratislava') from long_sequence(1)"
        );
    }

    @Test
    public void testStrideConstantPeriodVariableWithNulls() throws Exception {
        String timezone = "Europe/Bratislava"; // Dynamically set this at runtime

        assertSqlWithTypes(
                "dateadd\n:TIMESTAMP\n:TIMESTAMP\n",
                String.format("select dateadd('y', cast(x as int), null, '%s') from long_sequence(2)",
                        timezone));
        assertException(
                "select dateadd('y', case when x = 1 then cast(x as int) else null end, 1587275359886758L, '%s') from long_sequence(2)",
                90,
                "invalid timezone [timezone=%s]"
        );

        assertException(
                "select dateadd('y', case when x = 1 then cast(x as int) else null end, 1587275359886758L, '03:00') from long_sequence(2)",
                20,
                "`null` is not a valid stride"
        );

        assertSql(
                "QUERY PLAN\n" +
                        "VirtualRecord\n" +
                        "  functions: [dateadd('y',1587275359886758L,case([x::int,null,x]),'03:00')]\n" +
                        "    long_sequence count: 2\n",
                "explain select dateadd('y', case when x = 1 then cast(x as int) else null end, 1587275359886758L, '03:00') from long_sequence(2)"
        );

        assertSqlWithTypes("dateadd\n:TIMESTAMP\n:TIMESTAMP\n",
                String.format("select dateadd('M', cast(x as int), null, '%s') from long_sequence(2)",
                        timezone));

        assertSqlWithTypes("dateadd\n:TIMESTAMP\n:TIMESTAMP\n",
                String.format("select dateadd('w', cast(x as int), null, '%s') from long_sequence(2)",
                        timezone));

        assertSqlWithTypes("dateadd\n:TIMESTAMP\n:TIMESTAMP\n",
                String.format("select dateadd('d', cast(x as int), null, '%s') from long_sequence(2)",
                        timezone));

        assertSqlWithTypes("dateadd\n:TIMESTAMP\n:TIMESTAMP\n",
                String.format("select dateadd('h', cast(x as int), null, '%s') from long_sequence(2)",
                        timezone));

        assertSqlWithTypes("dateadd\n:TIMESTAMP\n:TIMESTAMP\n",
                String.format("select dateadd('m', cast(x as int), null, '%s') from long_sequence(2)",
                        timezone));

        assertSqlWithTypes("dateadd\n:TIMESTAMP\n:TIMESTAMP\n",
                String.format("select dateadd('s', cast(x as int), null, '%s') from long_sequence(2)",
                        timezone));
    }

    @Test
    public void testUnknownPeriod() throws Exception {
        assertException("select dateadd('q', -5, 1587275359886758L, 'Europe/Bratislava')", 15, "invalid time period [unit=q]");
    }

    @Test
    public void testUnknownTimezone() throws Exception {
        assertException("select dateadd('d', 2, 1603580400000000L, 'Random/Time')", 42, "invalid timezone [timezone=Random/Time]");
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new TimestampAddWithTimezoneFunctionFactory();
    }
}