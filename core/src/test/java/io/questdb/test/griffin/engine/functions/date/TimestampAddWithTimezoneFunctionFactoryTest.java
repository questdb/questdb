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
import io.questdb.std.str.StringSink;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class TimestampAddWithTimezoneFunctionFactoryTest extends AbstractFunctionFactoryTest {
    private static final char[] UNITS = {'y', 'M', 'w', 'd', 'h', 'm', 's', 'T', 'u', 'n'};

    @Test
    public void testAllVariables() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table test_tab as (" +
                            "select" +
                            " timestamp_sequence(0, 10000) ts," +
                            " timestamp_sequence(0::timestamp_ns, 10000000) ts_ns," +
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
                    "invalid timezone: unknown/unknown"
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
                    "select dateadd(period, stride, ts_ns, tz) val from test_tab where stride is not null and tz <> 'unknown/unknown' and period is not null and tz is not null",
                    15,
                    "invalid period [period=x]"
            );

            assertSql(
                    "val\tval2\tval3\tts\tperiod\tstride\ttz\n" +
                            "1970-11-01T00:00:00.010000Z\t\t1970-11-01T00:00:00.010000000Z\t1970-01-01T00:00:00.010000Z\tM\t10\tEurope/Bratislava\n" +
                            "1975-01-01T00:00:00.060000Z\t\t1975-01-01T00:00:00.060000000Z\t1970-01-01T00:00:00.060000Z\ty\t5\t08:00\n" +
                            "1975-01-01T00:00:00.140000Z\t\t1975-01-01T00:00:00.140000000Z\t1970-01-01T00:00:00.140000Z\ty\t5\t08:00\n" +
                            "1970-08-01T00:00:00.160000Z\t\t1970-08-01T00:00:00.160000000Z\t1970-01-01T00:00:00.160000Z\tM\t7\t08:00\n" +
                            "1977-01-01T00:00:00.210000Z\t\t1977-01-01T00:00:00.210000000Z\t1970-01-01T00:00:00.210000Z\ty\t7\tEurope/Bratislava\n" +
                            "1970-01-01T02:00:00.230000Z\t\t1970-01-01T02:00:00.230000000Z\t1970-01-01T00:00:00.230000Z\th\t2\tEurope/Bratislava\n" +
                            "1970-03-01T00:00:00.270000Z\t\t1970-03-01T00:00:00.270000000Z\t1970-01-01T00:00:00.270000Z\tM\t2\tEurope/Bratislava\n" +
                            "1970-04-01T00:00:00.290000Z\t\t1970-04-01T00:00:00.290000000Z\t1970-01-01T00:00:00.290000Z\tM\t3\t08:00\n" +
                            "1970-02-01T00:00:00.300000Z\t\t1970-02-01T00:00:00.300000000Z\t1970-01-01T00:00:00.300000Z\tM\t1\tEurope/Bratislava\n" +
                            "1978-01-01T00:00:00.450000Z\t\t1978-01-01T00:00:00.450000000Z\t1970-01-01T00:00:00.450000Z\ty\t8\t08:00\n" +
                            "1970-09-01T00:00:00.460000Z\t\t1970-09-01T00:00:00.460000000Z\t1970-01-01T00:00:00.460000Z\tM\t8\tEurope/Bratislava\n" +
                            "1970-11-01T00:00:00.540000Z\t\t1970-11-01T00:00:00.540000000Z\t1970-01-01T00:00:00.540000Z\tM\t10\tEurope/Bratislava\n" +
                            "1978-01-01T00:00:00.580000Z\t\t1978-01-01T00:00:00.580000000Z\t1970-01-01T00:00:00.580000Z\ty\t8\tEurope/Bratislava\n" +
                            "1970-04-01T00:00:00.590000Z\t\t1970-04-01T00:00:00.590000000Z\t1970-01-01T00:00:00.590000Z\tM\t3\t08:00\n" +
                            "1970-11-01T00:00:00.610000Z\t\t1970-11-01T00:00:00.610000000Z\t1970-01-01T00:00:00.610000Z\tM\t10\t08:00\n" +
                            "1972-01-01T00:00:00.650000Z\t\t1972-01-01T00:00:00.650000000Z\t1970-01-01T00:00:00.650000Z\ty\t2\t08:00\n" +
                            "1970-07-01T00:00:00.660000Z\t\t1970-07-01T00:00:00.660000000Z\t1970-01-01T00:00:00.660000Z\tM\t6\t08:00\n" +
                            "1970-01-01T00:00:00.670000Z\t\t1970-01-01T00:00:00.670000000Z\t1970-01-01T00:00:00.670000Z\tM\t0\t08:00\n" +
                            "1970-08-01T00:00:00.680000Z\t\t1970-08-01T00:00:00.680000000Z\t1970-01-01T00:00:00.680000Z\tM\t7\tEurope/Bratislava\n" +
                            "1970-01-01T04:00:00.720000Z\t\t1970-01-01T04:00:00.720000000Z\t1970-01-01T00:00:00.720000Z\th\t4\tEurope/Bratislava\n" +
                            "1970-01-01T00:00:00.780000Z\t\t1970-01-01T00:00:00.780000000Z\t1970-01-01T00:00:00.780000Z\tM\t0\t08:00\n" +
                            "1970-10-01T00:00:00.810000Z\t\t1970-10-01T00:00:00.810000000Z\t1970-01-01T00:00:00.810000Z\tM\t9\tEurope/Bratislava\n" +
                            "1970-01-01T04:00:00.860000Z\t\t1970-01-01T04:00:00.860000000Z\t1970-01-01T00:00:00.860000Z\th\t4\tEurope/Bratislava\n" +
                            "1970-01-01T02:00:00.910000Z\t\t1970-01-01T02:00:00.910000000Z\t1970-01-01T00:00:00.910000Z\th\t2\tEurope/Bratislava\n" +
                            "1972-01-01T00:00:00.920000Z\t\t1972-01-01T00:00:00.920000000Z\t1970-01-01T00:00:00.920000Z\ty\t2\t08:00\n",
                    "select dateadd(period, stride, ts, tz) val," +
                            " dateadd(period, stride, null, tz) val2," +
                            " dateadd(period, stride, ts_ns, tz) val3," +
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

            assertPlanNoLeakCheck(
                    "select dateadd(period, stride, ts, tz) val," +
                            " dateadd(period, stride, null, tz) val2," +
                            " dateadd(period, stride, ts_ns, tz) val3," +
                            " ts," +
                            " period," +
                            " stride," +
                            " tz" +
                            " from test_tab" +
                            " where stride is not null" +
                            " and tz <> 'unknown/unknown'" +
                            " and period is not null" +
                            " and tz is not null" +
                            " and period <> 'x'",
                    "VirtualRecord\n" +
                            "  functions: [dateadd('period',stride,ts,tz),dateadd('period',stride,null,tz),dateadd('period',stride,ts_ns,tz),ts,period,stride,tz]\n" +
                            "    Async JIT Filter workers: 1\n" +
                            "      filter: ((stride!=null and tz!='unknown/unknown' and period is not null and tz is not null) and period!='x') [pre-touch]\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: test_tab\n"
            );
        });
    }

    @Test
    public void testDSTTransitionToUTC() throws Exception {
        assertMemoryLeak(() -> {
            // Input query performing DATEADD and converting to UTC
            // Validate the result accounts for DST transition correctly
            assertQueryNoLeakCheck(
                    "utc_time\n" +
                            "2024-10-27T23:00:00.000000Z\n",
                    "select to_utc(dateadd('w', 1, '2024-10-21'), 'Europe/Bratislava') as utc_time"
            );

            assertQueryNoLeakCheck(
                    "utc_time\n" +
                            "2021-03-28T01:00:00.000000Z\n",
                    "select to_utc(dateadd('h', 2, '2021-03-28'), 'Europe/Berlin') as utc_time"
            );

            assertQueryNoLeakCheck(
                    "utc_time\n" +
                            "2021-10-31T00:00:00.000000Z\n",
                    "select to_utc(dateadd('h', 2, '2021-10-31'), 'Europe/Berlin') as utc_time"
            );

            assertQueryNoLeakCheck(
                    "utc_time\n" +
                            "2024-10-27T23:00:00.000000000Z\n",
                    "select to_utc(dateadd('w', 1, '2024-10-21'::timestamp_ns), 'Europe/Bratislava') as utc_time"
            );

            assertQueryNoLeakCheck(
                    "utc_time\n" +
                            "2021-03-28T01:00:00.000000000Z\n",
                    "select to_utc(dateadd('h', 2, '2021-03-28'::timestamp_ns), 'Europe/Berlin') as utc_time"
            );

            assertQueryNoLeakCheck(
                    "utc_time\n" +
                            "2021-10-31T00:00:00.000000000Z\n",
                    "select to_utc(dateadd('h', 2, '2021-10-31'::timestamp_ns), 'Europe/Berlin') as utc_time"
            );
        });
    }

    @Test
    public void testDateAddEquivalenceWithUTC() throws Exception {
        assertMemoryLeak(() -> {
            final StringSink sink2 = new StringSink();
            testDateAddEquivalenceWithUTC('w', 1, "2024-10-21", "timestamp", "Europe/Bratislava", sink2);

            // CET to CEST
            testDateAddEquivalenceWithUTC('h', 0, "2021-03-28", "timestamp", "Europe/Berlin", sink2);
            testDateAddEquivalenceWithUTC('h', 1, "2021-03-28", "timestamp", "Europe/Berlin", sink2);
            testDateAddEquivalenceWithUTC('h', 2, "2021-03-28", "timestamp", "Europe/Berlin", sink2);
            testDateAddEquivalenceWithUTC('h', 3, "2021-03-28", "timestamp", "Europe/Berlin", sink2);

            // CEST to CET
            testDateAddEquivalenceWithUTC('h', 0, "2021-10-31", "timestamp", "Europe/Berlin", sink2);
            testDateAddEquivalenceWithUTC('h', 1, "2021-10-31", "timestamp", "Europe/Berlin", sink2);
            testDateAddEquivalenceWithUTC('h', 2, "2021-10-31", "timestamp", "Europe/Berlin", sink2);
            testDateAddEquivalenceWithUTC('h', 3, "2021-10-31", "timestamp", "Europe/Berlin", sink2);

            testDateAddEquivalenceWithUTC('w', 1, "2024-10-21", "timestamp_ns", "Europe/Bratislava", sink2);

            // CET to CEST
            testDateAddEquivalenceWithUTC('h', 0, "2021-03-28", "timestamp_ns", "Europe/Berlin", sink2);
            testDateAddEquivalenceWithUTC('h', 1, "2021-03-28", "timestamp_ns", "Europe/Berlin", sink2);
            testDateAddEquivalenceWithUTC('h', 2, "2021-03-28", "timestamp_ns", "Europe/Berlin", sink2);
            testDateAddEquivalenceWithUTC('h', 3, "2021-03-28", "timestamp_ns", "Europe/Berlin", sink2);

            // CEST to CET
            testDateAddEquivalenceWithUTC('h', 0, "2021-10-31", "timestamp_ns", "Europe/Berlin", sink2);
            testDateAddEquivalenceWithUTC('h', 1, "2021-10-31", "timestamp_ns", "Europe/Berlin", sink2);
            testDateAddEquivalenceWithUTC('h', 2, "2021-10-31", "timestamp_ns", "Europe/Berlin", sink2);
            testDateAddEquivalenceWithUTC('h', 3, "2021-10-31", "timestamp_ns", "Europe/Berlin", sink2);
        });
    }

    @Test
    public void testDateAddWithTimezonePlan() throws Exception {
        assertMemoryLeak(() -> {
            assertPlanNoLeakCheck(
                    // Input query testing DATEADD behavior
                    "select dateadd('w', 1, '2024-10-21', 'Europe/Bratislava')",
                    // Updated expected plan
                    "VirtualRecord\n" +
                            "  functions: [2024-10-28T01:00:00.000000Z]\n" +
                            "    long_sequence count: 1\n"
            );
            assertPlanNoLeakCheck(
                    "select dateadd('w', 1, '2024-10-21T00:00:11.123456789', 'Europe/Bratislava')",
                    "VirtualRecord\n" +
                            "  functions: [2024-10-28T01:00:11.123456789Z]\n" +
                            "    long_sequence count: 1\n"
            );
        });
    }

    @Test
    public void testNullStride() throws Exception {
        for (int i = 0; i < UNITS.length; i++) {
            assertException("select dateadd('" + UNITS[i] + "', null, 1587275359886758L, 'Europe/Bratislava')", 20, "`null` is not a valid stride");
        }
    }

    @Test
    public void testNullTimestamp() throws Exception {
        for (int i = 0; i < UNITS.length; i++) {
            assertSqlWithTypes("dateadd\n:TIMESTAMP\n", "select dateadd('" + UNITS[i] + "', 5, null, 'Europe/Bratislava')");
        }
        for (int i = 0; i < UNITS.length; i++) {
            assertSqlWithTypes("dateadd\n:TIMESTAMP_NS\n", "select dateadd('" + UNITS[i] + "', 5, null::timestamp_ns, 'Europe/Bratislava')");
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
        assertSqlWithTypes("dateadd\n2020-04-19T05:49:19.886763Z:TIMESTAMP\n", "select dateadd('U', 5, 1587275359886758L, 'Europe/Bratislava')");
        assertSqlWithTypes("dateadd\n2020-04-19T05:49:19.886753Z:TIMESTAMP\n", "select dateadd('U', -5, 1587275359886758L, 'Europe/Bratislava')");
        assertSqlWithTypes("dateadd\n2020-04-19T05:49:19.886758Z:TIMESTAMP\n", "select dateadd('n', 5, 1587275359886758L, 'Europe/Bratislava')");
        assertSqlWithTypes("dateadd\n2020-04-19T05:49:19.886758Z:TIMESTAMP\n", "select dateadd('n', -5, 1587275359886758L, 'Europe/Bratislava')");

        assertSqlWithTypes("dateadd\n2025-04-19T05:49:19.886758123Z:TIMESTAMP_NS\n", "select dateadd('y', 5, 1587275359886758123L::timestamp_ns, 'Europe/Bratislava')");
        assertSqlWithTypes("dateadd\n2015-04-19T05:49:19.886758123Z:TIMESTAMP_NS\n", "select dateadd('y', -5, 1587275359886758123L::timestamp_ns, 'Europe/Bratislava')");
        assertSqlWithTypes("dateadd\n2020-09-19T05:49:19.886758123Z:TIMESTAMP_NS\n", "select dateadd('M', 5, 1587275359886758123L::timestamp_ns, 'Europe/Bratislava')");
        assertSqlWithTypes("dateadd\n2019-11-19T06:49:19.886758123Z:TIMESTAMP_NS\n", "select dateadd('M', -5, 1587275359886758123L::timestamp_ns, 'Europe/Bratislava')");
        assertSqlWithTypes("dateadd\n2020-05-24T05:49:19.886758123Z:TIMESTAMP_NS\n", "select dateadd('w', 5, 1587275359886758123L::timestamp_ns, 'Europe/Bratislava')");
        assertSqlWithTypes("dateadd\n2020-03-15T06:49:19.886758123Z:TIMESTAMP_NS\n", "select dateadd('w', -5, 1587275359886758123L::timestamp_ns, 'Europe/Bratislava')");
        assertSqlWithTypes("dateadd\n2020-04-24T05:49:19.886758123Z:TIMESTAMP_NS\n", "select dateadd('d', 5, 1587275359886758123L::timestamp_ns, 'Europe/Bratislava')");
        assertSqlWithTypes("dateadd\n2020-04-14T05:49:19.886758123Z:TIMESTAMP_NS\n", "select dateadd('d', -5, 1587275359886758123L::timestamp_ns, 'Europe/Bratislava')");
        assertSqlWithTypes("dateadd\n2020-04-19T10:49:19.886758123Z:TIMESTAMP_NS\n", "select dateadd('h', 5, 1587275359886758123L::timestamp_ns, 'Europe/Bratislava')");
        assertSqlWithTypes("dateadd\n2020-04-19T00:49:19.886758123Z:TIMESTAMP_NS\n", "select dateadd('h', -5, 1587275359886758123L::timestamp_ns, 'Europe/Bratislava')");
        assertSqlWithTypes("dateadd\n2020-04-19T05:54:19.886758123Z:TIMESTAMP_NS\n", "select dateadd('m', 5, 1587275359886758123L::timestamp_ns, 'Europe/Bratislava')");
        assertSqlWithTypes("dateadd\n2020-04-19T05:44:19.886758123Z:TIMESTAMP_NS\n", "select dateadd('m', -5, 1587275359886758123L::timestamp_ns, 'Europe/Bratislava')");
        assertSqlWithTypes("dateadd\n2020-04-19T05:49:24.886758123Z:TIMESTAMP_NS\n", "select dateadd('s', 5, 1587275359886758123L::timestamp_ns, 'Europe/Bratislava')");
        assertSqlWithTypes("dateadd\n2020-04-19T05:49:14.886758123Z:TIMESTAMP_NS\n", "select dateadd('s', -5, 1587275359886758123L::timestamp_ns, 'Europe/Bratislava')");
        assertSqlWithTypes("dateadd\n2020-04-19T05:49:19.891758123Z:TIMESTAMP_NS\n", "select dateadd('T', 5, 1587275359886758123L::timestamp_ns, 'Europe/Bratislava')");
        assertSqlWithTypes("dateadd\n2020-04-19T05:49:19.881758123Z:TIMESTAMP_NS\n", "select dateadd('T', -5, 1587275359886758123L::timestamp_ns, 'Europe/Bratislava')");
        assertSqlWithTypes("dateadd\n2020-04-19T05:49:19.886763123Z:TIMESTAMP_NS\n", "select dateadd('U', 5, 1587275359886758123L::timestamp_ns, 'Europe/Bratislava')");
        assertSqlWithTypes("dateadd\n2020-04-19T05:49:19.886753123Z:TIMESTAMP_NS\n", "select dateadd('U', -5, 1587275359886758123L::timestamp_ns, 'Europe/Bratislava')");
        assertSqlWithTypes("dateadd\n2020-04-19T05:49:19.886758128Z:TIMESTAMP_NS\n", "select dateadd('n', 5, 1587275359886758123L::timestamp_ns, 'Europe/Bratislava')");
        assertSqlWithTypes("dateadd\n2020-04-19T05:49:19.886758118Z:TIMESTAMP_NS\n", "select dateadd('n', -5, 1587275359886758123L::timestamp_ns, 'Europe/Bratislava')");
    }

    @Test
    public void testSimplePlan() throws SqlException {
        assertSql(
                "QUERY PLAN\n" +
                        "VirtualRecord\n" +
                        "  functions: [dateadd('U',-5,now(),'Europe/Bratislava')]\n" +
                        "    long_sequence count: 1\n",
                "explain select dateadd('U', -5, now, 'Europe/Bratislava') from long_sequence(1)"
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
                "invalid timezone: %s"
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

        assertSql(
                "QUERY PLAN\n" +
                        "VirtualRecord\n" +
                        "  functions: [dateadd('y',1587275359886758000L::timestamp_ns,case([x::int,null,x]),'03:00')]\n" +
                        "    long_sequence count: 2\n",
                "explain select dateadd('y', case when x = 1 then cast(x as int) else null end, 1587275359886758000L::timestamp_ns, '03:00') from long_sequence(2)"
        );

        assertSqlWithTypes("dateadd\n:TIMESTAMP\n:TIMESTAMP\n",
                String.format("select dateadd('M', cast(x as int), null, '%s') from long_sequence(2)",
                        timezone));
        assertSqlWithTypes("dateadd\n:TIMESTAMP_NS\n:TIMESTAMP_NS\n",
                String.format("select dateadd('M', cast(x as int), null::timestamp_ns, '%s') from long_sequence(2)",
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
        assertException("select dateadd('d', 2, 1603580400000000L, 'Random/Time')", 42, "invalid timezone: Random/Time");
    }

    private void testDateAddEquivalenceWithUTC(char unit, int interval, String date, String dateType, String tz, StringSink sink2) throws Exception {
        // Direct calculation with DATEADD
        String directQuery = "select dateadd('" + unit + "', " + interval + ", '" + date + "'::" + dateType + ", '" + tz + "') as d";
        // Equivalent calculation with intermediate conversions
        String conversionQuery = "select to_utc(dateadd('" + unit + "', " + interval + ", to_timezone('" + date + "'::" + dateType + ", '" + tz + "')), '" + tz + "') as d";

        // Validate the two produce the same result
        printSql(directQuery, sink);
        printSql(conversionQuery, sink2);
        TestUtils.assertEquals(sink2, sink);
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new TimestampAddWithTimezoneFunctionFactory();
    }
}