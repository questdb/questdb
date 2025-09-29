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
import io.questdb.griffin.engine.functions.date.TimestampAddFunctionFactory;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class TimestampAddFunctionFactoryTest extends AbstractFunctionFactoryTest {

    public static char[] units = {'y', 'M', 'w', 'd', 'h', 'm', 's', 'T', 'U', 'n'};

    @Test
    public void testDynamicFunction() throws Exception {
        execute("create table x as (" +
                "select " +
                "x::int as x1," +
                "rnd_symbol('u', 'n', 'T', 's') as s," +
                "timestamp_sequence(0, 1000000) as ts, " +
                "timestamp_sequence(0::timestamp_ns, 1000000000) as ts_ns " +
                "from long_sequence(20)" +
                ") timestamp(ts)");
        assertQuery(
                "date1\tdate2\tdatediff\n" +
                        "1970-01-01T00:00:01.000000Z\t1970-01-01T00:00:01.000000Z\t0\n" +
                        "1970-01-01T00:00:02.000000Z\t1970-01-01T00:00:02.000000Z\t0\n" +
                        "1970-01-01T00:00:03.000000Z\t1970-01-01T00:00:03.000000Z\t0\n" +
                        "1970-01-01T00:00:04.000000Z\t1970-01-01T00:00:04.000000Z\t0\n" +
                        "1970-01-01T00:00:05.000000Z\t1970-01-01T00:00:05.000000Z\t0\n" +
                        "1970-01-01T00:00:06.000000Z\t1970-01-01T00:00:06.000000Z\t0\n" +
                        "1970-01-01T00:00:07.000000Z\t1970-01-01T00:00:07.000000Z\t0\n" +
                        "1970-01-01T00:00:08.000000Z\t1970-01-01T00:00:08.000000Z\t0\n" +
                        "1970-01-01T00:00:09.000000Z\t1970-01-01T00:00:09.000000Z\t0\n" +
                        "1970-01-01T00:00:10.000000Z\t1970-01-01T00:00:10.000000Z\t0\n" +
                        "1970-01-01T00:00:11.000000Z\t1970-01-01T00:00:11.000000Z\t0\n" +
                        "1970-01-01T00:00:12.000000Z\t1970-01-01T00:00:12.000000Z\t0\n" +
                        "1970-01-01T00:00:13.000000Z\t1970-01-01T00:00:13.000000Z\t0\n" +
                        "1970-01-01T00:00:14.000000Z\t1970-01-01T00:00:14.000000Z\t0\n" +
                        "1970-01-01T00:00:15.000000Z\t1970-01-01T00:00:15.000000Z\t0\n" +
                        "1970-01-01T00:00:16.000000Z\t1970-01-01T00:00:16.000000Z\t0\n" +
                        "1970-01-01T00:00:17.000000Z\t1970-01-01T00:00:17.000000Z\t0\n" +
                        "1970-01-01T00:00:18.000000Z\t1970-01-01T00:00:18.000000Z\t0\n" +
                        "1970-01-01T00:00:19.000000Z\t1970-01-01T00:00:19.000000Z\t0\n" +
                        "1970-01-01T00:00:20.000000Z\t1970-01-01T00:00:20.000000Z\t0\n",
                "select dateadd('u', 1000000, ts) date1, dateadd('u', 1000000, ts) date2, datediff('T', date1, date2) from x;",
                null,
                true,
                true
        );

        assertQuery(
                "s\tdate1\tdate2\tdatediff\n" +
                        "u\t1970-01-01T00:00:02.000000Z\t1970-01-01T00:00:02.000000000Z\t0\n" +
                        "T\t1970-01-01T00:16:41.000000Z\t1970-01-01T00:16:41.000000000Z\t0\n" +
                        "n\t1970-01-01T00:00:01.001000Z\t1970-01-01T00:00:01.001000000Z\t0\n" +
                        "s\t1970-01-12T13:46:41.000000Z\t1970-01-12T13:46:41.000000000Z\t0\n" +
                        "n\t1970-01-01T00:00:01.001000Z\t1970-01-01T00:00:01.001000000Z\t0\n" +
                        "s\t1970-01-12T13:46:41.000000Z\t1970-01-12T13:46:41.000000000Z\t0\n" +
                        "T\t1970-01-01T00:16:41.000000Z\t1970-01-01T00:16:41.000000000Z\t0\n" +
                        "s\t1970-01-12T13:46:41.000000Z\t1970-01-12T13:46:41.000000000Z\t0\n" +
                        "T\t1970-01-01T00:16:41.000000Z\t1970-01-01T00:16:41.000000000Z\t0\n" +
                        "u\t1970-01-01T00:00:02.000000Z\t1970-01-01T00:00:02.000000000Z\t0\n" +
                        "u\t1970-01-01T00:00:02.000000Z\t1970-01-01T00:00:02.000000000Z\t0\n" +
                        "u\t1970-01-01T00:00:02.000000Z\t1970-01-01T00:00:02.000000000Z\t0\n" +
                        "T\t1970-01-01T00:16:41.000000Z\t1970-01-01T00:16:41.000000000Z\t0\n" +
                        "s\t1970-01-12T13:46:41.000000Z\t1970-01-12T13:46:41.000000000Z\t0\n" +
                        "n\t1970-01-01T00:00:01.001000Z\t1970-01-01T00:00:01.001000000Z\t0\n" +
                        "u\t1970-01-01T00:00:02.000000Z\t1970-01-01T00:00:02.000000000Z\t0\n" +
                        "u\t1970-01-01T00:00:02.000000Z\t1970-01-01T00:00:02.000000000Z\t0\n" +
                        "n\t1970-01-01T00:00:01.001000Z\t1970-01-01T00:00:01.001000000Z\t0\n" +
                        "n\t1970-01-01T00:00:01.001000Z\t1970-01-01T00:00:01.001000000Z\t0\n" +
                        "T\t1970-01-01T00:16:41.000000Z\t1970-01-01T00:16:41.000000000Z\t0\n",
                "select s, dateadd(s, 1000000, 1000000::timestamp) date1, dateadd(s, 1000000, 1000000000::timestamp_ns) date2, datediff('n', date1, date2) from x;",
                null,
                true,
                true
        );

        assertQuery(
                "s\tdate1\tdate2\tdatediff\n" +
                        "u\t1970-01-01T00:00:01.000000Z\t1970-01-01T00:00:01.000000000Z\t0\n" +
                        "T\t1970-01-01T00:16:41.000000Z\t1970-01-01T00:16:41.000000000Z\t0\n" +
                        "n\t1970-01-01T00:00:02.001000Z\t1970-01-01T00:00:02.001000000Z\t0\n" +
                        "s\t1970-01-12T13:46:43.000000Z\t1970-01-12T13:46:43.000000000Z\t0\n" +
                        "n\t1970-01-01T00:00:04.001000Z\t1970-01-01T00:00:04.001000000Z\t0\n" +
                        "s\t1970-01-12T13:46:45.000000Z\t1970-01-12T13:46:45.000000000Z\t0\n" +
                        "T\t1970-01-01T00:16:46.000000Z\t1970-01-01T00:16:46.000000000Z\t0\n" +
                        "s\t1970-01-12T13:46:47.000000Z\t1970-01-12T13:46:47.000000000Z\t0\n" +
                        "T\t1970-01-01T00:16:48.000000Z\t1970-01-01T00:16:48.000000000Z\t0\n" +
                        "u\t1970-01-01T00:00:10.000000Z\t1970-01-01T00:00:10.000000000Z\t0\n" +
                        "u\t1970-01-01T00:00:11.000000Z\t1970-01-01T00:00:11.000000000Z\t0\n" +
                        "u\t1970-01-01T00:00:12.000000Z\t1970-01-01T00:00:12.000000000Z\t0\n" +
                        "T\t1970-01-01T00:16:52.000000Z\t1970-01-01T00:16:52.000000000Z\t0\n" +
                        "s\t1970-01-12T13:46:53.000000Z\t1970-01-12T13:46:53.000000000Z\t0\n" +
                        "n\t1970-01-01T00:00:14.001000Z\t1970-01-01T00:00:14.001000000Z\t0\n" +
                        "u\t1970-01-01T00:00:16.000000Z\t1970-01-01T00:00:16.000000000Z\t0\n" +
                        "u\t1970-01-01T00:00:17.000000Z\t1970-01-01T00:00:17.000000000Z\t0\n" +
                        "n\t1970-01-01T00:00:17.001000Z\t1970-01-01T00:00:17.001000000Z\t0\n" +
                        "n\t1970-01-01T00:00:18.001000Z\t1970-01-01T00:00:18.001000000Z\t0\n" +
                        "T\t1970-01-01T00:16:59.000000Z\t1970-01-01T00:16:59.000000000Z\t0\n",
                "select s, dateadd(s, 1000000, ts) date1, dateadd(s, 1000000, ts_ns) date2, datediff('n', date1, date2) from x;",
                null,
                true,
                true
        );
    }

    @Test
    public void testNullStride() throws Exception {
        for (int i = 0; i < units.length; i++) {
            assertException("select dateadd('" + units[i] + "', null, 1587275359886758L)", 20, "`null` is not a valid stride");
        }
    }

    @Test
    public void testNullTimestamp() throws Exception {
        for (int i = 0; i < units.length; i++) {
            assertSqlWithTypes("dateadd\n:TIMESTAMP\n", "select dateadd('" + units[i] + "', 5, null)");
        }
    }

    @Test
    public void testPeriodNullChar() throws Exception {
        assertException("select dateadd('\0', 5, 1587275359886758L)", 15, "invalid time period [unit=\u0000]");
    }

    @Test
    public void testSimple() throws Exception {
        assertSqlWithTypes("dateadd\n2025-04-19T05:49:19.886758Z:TIMESTAMP\n", "select dateadd('y', 5, 1587275359886758L)");
        assertSqlWithTypes("dateadd\n2015-04-19T05:49:19.886758Z:TIMESTAMP\n", "select dateadd('y', -5, 1587275359886758L)");
        assertSqlWithTypes("dateadd\n2020-09-19T05:49:19.886758Z:TIMESTAMP\n", "select dateadd('M', 5, 1587275359886758L)");
        assertSqlWithTypes("dateadd\n2019-11-19T05:49:19.886758Z:TIMESTAMP\n", "select dateadd('M', -5, 1587275359886758L)");
        assertSqlWithTypes("dateadd\n2020-05-24T05:49:19.886758Z:TIMESTAMP\n", "select dateadd('w', 5, 1587275359886758L)");
        assertSqlWithTypes("dateadd\n2020-03-15T05:49:19.886758Z:TIMESTAMP\n", "select dateadd('w', -5, 1587275359886758L)");
        assertSqlWithTypes("dateadd\n2020-04-24T05:49:19.886758Z:TIMESTAMP\n", "select dateadd('d', 5, 1587275359886758L)");
        assertSqlWithTypes("dateadd\n2020-04-14T05:49:19.886758Z:TIMESTAMP\n", "select dateadd('d', -5, 1587275359886758L)");
        assertSqlWithTypes("dateadd\n2020-04-19T10:49:19.886758Z:TIMESTAMP\n", "select dateadd('h', 5, 1587275359886758L)");
        assertSqlWithTypes("dateadd\n2020-04-19T00:49:19.886758Z:TIMESTAMP\n", "select dateadd('h', -5, 1587275359886758L)");
        assertSqlWithTypes("dateadd\n2020-04-19T05:54:19.886758Z:TIMESTAMP\n", "select dateadd('m', 5, 1587275359886758L)");
        assertSqlWithTypes("dateadd\n2020-04-19T05:44:19.886758Z:TIMESTAMP\n", "select dateadd('m', -5, 1587275359886758L)");
        assertSqlWithTypes("dateadd\n2020-04-19T05:49:24.886758Z:TIMESTAMP\n", "select dateadd('s', 5, 1587275359886758L)");
        assertSqlWithTypes("dateadd\n2020-04-19T05:49:14.886758Z:TIMESTAMP\n", "select dateadd('s', -5, 1587275359886758L)");
        assertSqlWithTypes("dateadd\n2020-04-19T05:49:19.891758Z:TIMESTAMP\n", "select dateadd('T', 5, 1587275359886758L)");
        assertSqlWithTypes("dateadd\n2020-04-19T05:49:19.881758Z:TIMESTAMP\n", "select dateadd('T', -5, 1587275359886758L)");
        assertSqlWithTypes("dateadd\n2020-04-19T05:49:19.886763Z:TIMESTAMP\n", "select dateadd('U', 5, 1587275359886758L)");
        assertSqlWithTypes("dateadd\n2020-04-19T05:49:19.886763Z:TIMESTAMP\n", "select dateadd('n', 5000, 1587275359886758L)");
        assertSqlWithTypes("dateadd\n2020-04-19T05:49:19.886753Z:TIMESTAMP\n", "select dateadd('U', -5, 1587275359886758L)");

        assertSqlWithTypes("dateadd\n2025-04-19T05:49:19.886758123Z:TIMESTAMP_NS\n", "select dateadd('y', 5, 1587275359886758123L::timestamp_ns)");
        assertSqlWithTypes("dateadd\n2015-04-19T05:49:19.886758123Z:TIMESTAMP_NS\n", "select dateadd('y', -5, 1587275359886758123L::timestamp_ns)");
        assertSqlWithTypes("dateadd\n2020-09-19T05:49:19.886758123Z:TIMESTAMP_NS\n", "select dateadd('M', 5, 1587275359886758123L::timestamp_ns)");
        assertSqlWithTypes("dateadd\n2019-11-19T05:49:19.886758123Z:TIMESTAMP_NS\n", "select dateadd('M', -5, 1587275359886758123L::timestamp_ns)");
        assertSqlWithTypes("dateadd\n2020-05-24T05:49:19.886758123Z:TIMESTAMP_NS\n", "select dateadd('w', 5, 1587275359886758123L::timestamp_ns)");
        assertSqlWithTypes("dateadd\n2020-03-15T05:49:19.886758123Z:TIMESTAMP_NS\n", "select dateadd('w', -5, 1587275359886758123L::timestamp_ns)");
        assertSqlWithTypes("dateadd\n2020-04-24T05:49:19.886758123Z:TIMESTAMP_NS\n", "select dateadd('d', 5, 1587275359886758123L::timestamp_ns)");
        assertSqlWithTypes("dateadd\n2020-04-14T05:49:19.886758123Z:TIMESTAMP_NS\n", "select dateadd('d', -5, 1587275359886758123L::timestamp_ns)");
        assertSqlWithTypes("dateadd\n2020-04-19T10:49:19.886758123Z:TIMESTAMP_NS\n", "select dateadd('h', 5, 1587275359886758123L::timestamp_ns)");
        assertSqlWithTypes("dateadd\n2020-04-19T00:49:19.886758123Z:TIMESTAMP_NS\n", "select dateadd('h', -5, 1587275359886758123L::timestamp_ns)");
        assertSqlWithTypes("dateadd\n2020-04-19T05:54:19.886758123Z:TIMESTAMP_NS\n", "select dateadd('m', 5, 1587275359886758123L::timestamp_ns)");
        assertSqlWithTypes("dateadd\n2020-04-19T05:44:19.886758123Z:TIMESTAMP_NS\n", "select dateadd('m', -5, 1587275359886758123L::timestamp_ns)");
        assertSqlWithTypes("dateadd\n2020-04-19T05:49:24.886758123Z:TIMESTAMP_NS\n", "select dateadd('s', 5, 1587275359886758123L::timestamp_ns)");
        assertSqlWithTypes("dateadd\n2020-04-19T05:49:14.886758123Z:TIMESTAMP_NS\n", "select dateadd('s', -5, 1587275359886758123L::timestamp_ns)");
        assertSqlWithTypes("dateadd\n2020-04-19T05:49:19.891758123Z:TIMESTAMP_NS\n", "select dateadd('T', 5, 1587275359886758123L::timestamp_ns)");
        assertSqlWithTypes("dateadd\n2020-04-19T05:49:19.881758123Z:TIMESTAMP_NS\n", "select dateadd('T', -5, 1587275359886758123L::timestamp_ns)");
        assertSqlWithTypes("dateadd\n2020-04-19T05:49:19.886763123Z:TIMESTAMP_NS\n", "select dateadd('U', 5, 1587275359886758123L::timestamp_ns)");
        assertSqlWithTypes("dateadd\n2020-04-19T05:49:19.886758100Z:TIMESTAMP_NS\n", "select dateadd('n', -23, 1587275359886758123L::timestamp_ns)");
        assertSqlWithTypes("dateadd\n2020-04-19T05:49:19.886753123Z:TIMESTAMP_NS\n", "select dateadd('U', -5, 1587275359886758123L::timestamp_ns)");
    }

    @Test
    public void testStrideConstantPeriodVariableWithNulls() throws Exception {
        assertSqlWithTypes("dateadd\n:TIMESTAMP\n:TIMESTAMP\n", "select dateadd('y', cast(x as int), null) from long_sequence(2)");
        assertSqlWithTypes("dateadd\n2021-04-19T05:49:19.886758Z:TIMESTAMP\n:TIMESTAMP\n", "select dateadd('y', case when x = 1 then cast(x as int) else null end, 1587275359886758L) from long_sequence(2)");
        assertSqlWithTypes("dateadd\n:TIMESTAMP\n:TIMESTAMP\n", "select dateadd('M', cast(x as int), null) from long_sequence(2)");
        assertSqlWithTypes("dateadd\n2020-05-19T05:49:19.886758Z:TIMESTAMP\n:TIMESTAMP\n", "select dateadd('M', case when x = 1 then cast(x as int) else null end, 1587275359886758L) from long_sequence(2)");
        assertSqlWithTypes("dateadd\n:TIMESTAMP\n:TIMESTAMP\n", "select dateadd('w', cast(x as int), null) from long_sequence(2)");
        assertSqlWithTypes("dateadd\n2020-04-26T05:49:19.886758Z:TIMESTAMP\n:TIMESTAMP\n", "select dateadd('w', case when x = 1 then cast(x as int) else null end, 1587275359886758L) from long_sequence(2)");
        assertSqlWithTypes("dateadd\n:TIMESTAMP\n:TIMESTAMP\n", "select dateadd('d', cast(x as int), null) from long_sequence(2)");
        assertSqlWithTypes("dateadd\n2020-04-20T05:49:19.886758Z:TIMESTAMP\n:TIMESTAMP\n", "select dateadd('d', case when x = 1 then cast(x as int) else null end, 1587275359886758L) from long_sequence(2)");
        assertSqlWithTypes("dateadd\n:TIMESTAMP\n:TIMESTAMP\n", "select dateadd('h', cast(x as int), null) from long_sequence(2)");
        assertSqlWithTypes("dateadd\n2020-04-19T06:49:19.886758Z:TIMESTAMP\n:TIMESTAMP\n", "select dateadd('h', case when x = 1 then cast(x as int) else null end, 1587275359886758L) from long_sequence(2)");
        assertSqlWithTypes("dateadd\n:TIMESTAMP\n:TIMESTAMP\n", "select dateadd('m', cast(x as int), null) from long_sequence(2)");
        assertSqlWithTypes("dateadd\n2020-04-19T05:50:19.886758Z:TIMESTAMP\n:TIMESTAMP\n", "select dateadd('m', case when x = 1 then cast(x as int) else null end, 1587275359886758L) from long_sequence(2)");
        assertSqlWithTypes("dateadd\n:TIMESTAMP\n:TIMESTAMP\n", "select dateadd('s', cast(x as int), null) from long_sequence(2)");
        assertSqlWithTypes("dateadd\n2020-04-19T05:49:20.886758Z:TIMESTAMP\n:TIMESTAMP\n", "select dateadd('s', case when x = 1 then cast(x as int) else null end, 1587275359886758L) from long_sequence(2)");

        assertSqlWithTypes("dateadd\n:TIMESTAMP_NS\n:TIMESTAMP_NS\n", "select dateadd('y', cast(x as int), null::timestamp_ns) from long_sequence(2)");
        assertSqlWithTypes("dateadd\n2021-04-19T05:49:19.886758000Z:TIMESTAMP_NS\n:TIMESTAMP_NS\n", "select dateadd('y', case when x = 1 then cast(x as int) else null end, 1587275359886758000L::timestamp_ns) from long_sequence(2)");
        assertSqlWithTypes("dateadd\n:TIMESTAMP_NS\n:TIMESTAMP_NS\n", "select dateadd('M', cast(x as int), null::timestamp_ns) from long_sequence(2)");
        assertSqlWithTypes("dateadd\n2020-05-19T05:49:19.886758000Z:TIMESTAMP_NS\n:TIMESTAMP_NS\n", "select dateadd('M', case when x = 1 then cast(x as int) else null end, 1587275359886758000L::timestamp_ns) from long_sequence(2)");
        assertSqlWithTypes("dateadd\n:TIMESTAMP_NS\n:TIMESTAMP_NS\n", "select dateadd('w', cast(x as int), null::timestamp_ns) from long_sequence(2)");
        assertSqlWithTypes("dateadd\n2020-04-26T05:49:19.886758000Z:TIMESTAMP_NS\n:TIMESTAMP_NS\n", "select dateadd('w', case when x = 1 then cast(x as int) else null end, 1587275359886758000L::timestamp_ns) from long_sequence(2)");
        assertSqlWithTypes("dateadd\n:TIMESTAMP_NS\n:TIMESTAMP_NS\n", "select dateadd('d', cast(x as int), null::timestamp_ns) from long_sequence(2)");
        assertSqlWithTypes("dateadd\n2020-04-20T05:49:19.886758000Z:TIMESTAMP_NS\n:TIMESTAMP_NS\n", "select dateadd('d', case when x = 1 then cast(x as int) else null end, 1587275359886758000L::timestamp_ns) from long_sequence(2)");
        assertSqlWithTypes("dateadd\n:TIMESTAMP_NS\n:TIMESTAMP_NS\n", "select dateadd('h', cast(x as int), null::timestamp_ns) from long_sequence(2)");
        assertSqlWithTypes("dateadd\n2020-04-19T06:49:19.886758000Z:TIMESTAMP_NS\n:TIMESTAMP_NS\n", "select dateadd('h', case when x = 1 then cast(x as int) else null end, 1587275359886758000L::timestamp_ns) from long_sequence(2)");
        assertSqlWithTypes("dateadd\n:TIMESTAMP_NS\n:TIMESTAMP_NS\n", "select dateadd('m', cast(x as int), null::timestamp_ns) from long_sequence(2)");
        assertSqlWithTypes("dateadd\n2020-04-19T05:50:19.886758000Z:TIMESTAMP_NS\n:TIMESTAMP_NS\n", "select dateadd('m', case when x = 1 then cast(x as int) else null end, 1587275359886758000L::timestamp_ns) from long_sequence(2)");
        assertSqlWithTypes("dateadd\n:TIMESTAMP_NS\n:TIMESTAMP_NS\n", "select dateadd('s', cast(x as int), null::timestamp_ns) from long_sequence(2)");
        assertSqlWithTypes("dateadd\n2020-04-19T05:49:20.886758000Z:TIMESTAMP_NS\n:TIMESTAMP_NS\n", "select dateadd('s', case when x = 1 then cast(x as int) else null end, 1587275359886758000L::timestamp_ns) from long_sequence(2)");
    }

    @Test
    public void testUnknownPeriod() throws Exception {
        assertException("select dateadd('q', -5, 1587275359886758L)", 15, "invalid time period [unit=q]");
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new TimestampAddFunctionFactory();
    }
}
