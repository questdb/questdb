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

import io.questdb.std.Interval;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class TodayTomorrowYesterdayTest extends AbstractCairoTest {

    @Test
    public void testIntervalStartEnd() throws Exception {
        assertSql("column\n" +
                "true\n", "select interval_start(today()) = date_trunc('day', now()) from long_sequence(1)");
        assertSql("column\n" +
                "true\n", "select interval_end(today()) = dateadd('d', 1, date_trunc('day', now()))-1 from long_sequence(1)\n");
    }

    @Test
    public void testIntrinsics1() throws Exception {
        assertSql("bool\n" +
                        "true\n",
                "select true as bool from long_sequence(1) where now() in today()");
        // no interval scan with now()
        assertPlanNoLeakCheck("select true as bool from long_sequence(1) where now() in today()"
                , "VirtualRecord\n" +
                        "  functions: [true]\n" +
                        "    Filter filter: now() in [1726704000000000,1726790399999999]\n" +
                        "        long_sequence count: 1\n");
    }

    @Test
    public void testIntrinsics2() throws Exception {

        assertMemoryLeak(() -> {
            ddl("CREATE TABLE x (ts TIMESTAMP) timestamp(ts) PARTITION BY DAY WAL;");
            drainWalQueue();
            // should have interval scans despite use of function
            // due to optimisation step to convert it to a constant
            long today = Timestamps.today();
            long tomorrow = Timestamps.tomorrow();
            long yesterday = Timestamps.yesterday();
            long tomorrowAndOne = Timestamps.addDays(tomorrow, 1);
            StringSink sink = new StringSink();
            buildPlan(sink, today, tomorrow - 1);
            assertPlanNoLeakCheck("select true as bool from x where ts in today()",
                    sink.toString());
            sink.clear();
            buildPlan(sink, tomorrow, tomorrowAndOne - 1);
            assertPlanNoLeakCheck("select true as bool from x where ts in tomorrow()",
                    sink.toString());
            sink.clear();
            buildPlan(sink, yesterday, today - 1);
            assertPlanNoLeakCheck("select true as bool from x where ts in yesterday()",
                    sink.toString());
            sink.clear();
        });
    }

    @Test
    public void testTimestampInInterval() throws Exception {
        assertSql("result\n", "select true as result from long_sequence(1)\n" +
                "where now() in tomorrow()");
        assertSql("result\n" +
                "true\n", "select true as result from long_sequence(1)\n" +
                "where now() in today()");
//        assertSql("result\n", "select true as result from long_sequence(1)\n" +
//                "where now() in yesterday()");
    }

    @Test
    public void testToday() throws Exception {
        long todayStart = Timestamps.today();
        long todayEnd = Timestamps.tomorrow() - 1;
        final Interval interval = new Interval(todayStart, todayEnd);
        assertSql("today\n" + interval + "\n", "select today()");
    }

    @Test
    public void testTodayWithTimezone() throws Exception {
        assertSql("column\n" +
                        "true\n",
                "select today('Antarctica/McMurdo') = interval(date_trunc('day', to_timezone(now(), 'Antarctica/McMurdo')), date_trunc('day', to_timezone(dateadd('d', 1, now()), 'Antarctica/McMurdo')) - 1)\n");
    }

    @Test
    public void testTomorrow() throws Exception {
        long tomorrowStart = Timestamps.tomorrow();
        long tomorrowEnd = Timestamps.addDays(tomorrowStart, 1) - 1;
        final Interval interval = new Interval(tomorrowStart, tomorrowEnd);
        assertSql("tomorrow\n" + interval + "\n", "select tomorrow()");
    }

    @Test
    public void testTomorrowWithTimezone() throws Exception {
        assertSql("column\n" +
                        "true\n",
                "select tomorrow('Antarctica/McMurdo') = interval(date_trunc('day', to_timezone(dateadd('d', 1, now()), 'Antarctica/McMurdo')), date_trunc('day', to_timezone(dateadd('d', 2, now()), 'Antarctica/McMurdo')) - 1)\n");
    }

    @Test
    public void testYesterday() throws Exception {
        long yesterdayStart = Timestamps.yesterday();
        long yesterdayEnd = Timestamps.today() - 1;
        final Interval interval = new Interval(yesterdayStart, yesterdayEnd);
        assertSql("yesterday\n" + interval + "\n", "select yesterday()");
    }

    @Test
    public void testYesterdayWithTimezone() throws Exception {
        assertSql("column\n" +
                        "true\n",
                "select yesterday('Antarctica/McMurdo') = interval(date_trunc('day', to_timezone(dateadd('d', -1, now()), 'Antarctica/McMurdo')), date_trunc('day', to_timezone(now(), 'Antarctica/McMurdo')) - 1)\n");
    }

    private void buildPlan(StringSink sink, long lo, long hi) {
        sink.put("VirtualRecord\n" +
                "  functions: [true]\n" +
                "    PageFrame\n" +
                "        Row forward scan\n" +
                "        Interval forward scan on: x\n" +
                "          intervals: [(\"");
        sink.putISODate(lo);
        sink.put("\",\"");
        sink.putISODate(hi);
        sink.put("\")]\n");
    }

}
