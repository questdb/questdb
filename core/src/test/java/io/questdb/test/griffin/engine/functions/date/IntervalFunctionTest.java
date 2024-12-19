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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.Interval;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class IntervalFunctionTest extends AbstractCairoTest {

    @Test
    public void testInterval() throws Exception {
        assertMemoryLeak(() -> {
            assertSql(
                    "interval\n" +
                            "('2000-01-01T01:00:00.000Z', '2000-01-02T01:00:00.000Z')\n",
                    "select interval('2000-01-01T01:00:00.000Z', '2000-01-02T01:00:00.000Z')"
            );

            bindVariableService.clear();
            bindVariableService.setStr("lo", "2000-01-03T01:00:00.000Z");
            bindVariableService.setStr("hi", "2000-01-04T01:00:00.000Z");
            assertSql(
                    "interval\n" +
                            "('2000-01-03T01:00:00.000Z', '2000-01-04T01:00:00.000Z')\n",
                    "select interval(:lo, :hi)"
            );

            assertSql(
                    "interval\n" +
                            "('1970-01-01T00:00:00.000Z', '1970-01-01T00:00:00.010Z')\n",
                    "select interval(x,x+10000) from long_sequence(1)"
            );
        });
    }

    @Test
    public void testIntervalStartEnd() throws Exception {
        assertMemoryLeak(() -> {
            assertSql(
                    "column\n" +
                            "true\n",
                    "select interval_start(today()) = date_trunc('day', now()) from long_sequence(1)"
            );
            assertSql(
                    "column\n" +
                            "true\n",
                    "select interval_end(today()) = dateadd('d', 1, date_trunc('day', now()))-1 from long_sequence(1)\n"
            );
        });
    }

    @Test
    public void testIntrinsics1() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP) timestamp(ts) PARTITION BY DAY WAL;");
            // should have interval scans despite use of function
            // due to optimisation step to convert it to a constant
            long today = today(sqlExecutionContext.getNow());
            long tomorrow = tomorrow(sqlExecutionContext.getNow());
            long yesterday = yesterday(sqlExecutionContext.getNow());
            long tomorrowAndOne = Timestamps.addDays(tomorrow, 1);
            long todayAndOne = Timestamps.addDays(today, 1);

            StringSink sink = new StringSink();
            buildInPlan(sink, today, tomorrow - 1);
            assertPlanNoLeakCheck(
                    "select true as bool from x where ts in today()",
                    sink.toString()
            );

            sink.clear();
            buildInPlan(sink, tomorrow, tomorrowAndOne - 1);
            assertPlanNoLeakCheck(
                    "select true as bool from x where ts in tomorrow()",
                    sink.toString()
            );

            sink.clear();
            buildInPlan(sink, yesterday, today - 1);
            assertPlanNoLeakCheck(
                    "select true as bool from x where ts in yesterday()",
                    sink.toString()
            );

            sink.clear();
            buildNotInPlan(sink, today, todayAndOne);
            assertPlanNoLeakCheck(
                    "select true as bool from x where ts not in today()",
                    sink.toString()
            );

            assertPlanNoLeakCheck(
                    "select true as bool from x where ts in yesterday() and ts in today() and ts in tomorrow()",
                    "VirtualRecord\n" +
                            "  functions: [true]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Interval forward scan on: x\n" +
                            "          intervals: []\n"
            );

            assertPlanNoLeakCheck(
                    "select true as bool from x where ts in null::interval",
                    "VirtualRecord\n" +
                            "  functions: [true]\n" +
                            "    Async Filter workers: 1\n" +
                            "      filter: ts in (null, null)\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: x\n"
            );
        });
    }

    @Test
    public void testIntrinsics2() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(Timestamps.DAY_MICROS);
            execute("CREATE TABLE x as (select x::timestamp ts from long_sequence(10)) timestamp(ts) PARTITION BY DAY WAL;");
            drainWalQueue();

            assertSql(
                    "ts\n" +
                            "1970-01-01T00:00:00.000001Z\n" +
                            "1970-01-01T00:00:00.000002Z\n" +
                            "1970-01-01T00:00:00.000003Z\n" +
                            "1970-01-01T00:00:00.000004Z\n" +
                            "1970-01-01T00:00:00.000005Z\n" +
                            "1970-01-01T00:00:00.000006Z\n" +
                            "1970-01-01T00:00:00.000007Z\n" +
                            "1970-01-01T00:00:00.000008Z\n" +
                            "1970-01-01T00:00:00.000009Z\n" +
                            "1970-01-01T00:00:00.000010Z\n",
                    "select * from x where ts in yesterday()"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts in yesterday()",
                    "PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            "      intervals: [(\"1970-01-01T00:00:00.000000Z\",\"1970-01-01T23:59:59.999999Z\")]\n"
            );

            assertSql(
                    "ts\n",
                    "select * from x where ts in today()"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts in today()",
                    "PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            "      intervals: [(\"1970-01-02T00:00:00.000000Z\",\"1970-01-02T23:59:59.999999Z\")]\n"
            );

            assertSql(
                    "ts\n",
                    "select * from x where ts in tomorrow()"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts in tomorrow()",
                    "PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            "      intervals: [(\"1970-01-03T00:00:00.000000Z\",\"1970-01-03T23:59:59.999999Z\")]\n"
            );
        });
    }

    @Test
    public void testIntrinsicsAllVirtual() throws Exception {
        assertMemoryLeak(() -> {
            assertSql(
                    "bool\n" +
                            "true\n",
                    "select true as bool from long_sequence(1) where now() in today()"
            );
            // no interval scan with now()
            assertPlanNoLeakCheck(
                    "select true as bool from long_sequence(1) where now() in today()",
                    "VirtualRecord\n" +
                            "  functions: [true]\n" +
                            "    Filter filter: now() in today()\n" +
                            "        long_sequence count: 1\n"
            );
        });
    }

    @Test
    public void testIntrinsicsNonPartitioned() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (k int, ts timestamp);");
            assertPlanNoLeakCheck(
                    "select * from x where ts in today() or ts in tomorrow() or ts in yesterday();",
                    "Async Filter workers: 1\n" +
                            "  filter: ((ts in today() or ts in tomorrow()) or ts in yesterday())\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: x\n"
            );
        });
    }

    @Test
    public void testInvalidIntervalBoundaries() throws Exception {
        assertMemoryLeak(() -> {
            assertExceptionNoLeakCheck(
                    "select interval(2,1)",
                    7,
                    "invalid interval boundaries"
            );

            try {
                try (
                        RecordCursorFactory factory = select("select interval(x+1,x) from long_sequence(1)");
                        RecordCursor cursor = factory.getCursor(sqlExecutionContext)
                ) {
                    Assert.assertTrue(cursor.hasNext());
                    cursor.getRecord().getInterval(0);
                }
                Assert.fail();
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "invalid interval boundaries");
                Assert.assertEquals(0, e.getPosition());
            }
        });
    }

    @Test
    public void testNonConstantTimezone() throws Exception {
        setCurrentMicros(7 * Timestamps.DAY_MICROS + Timestamps.HOUR_MICROS); // 1970-01-08T01:00:00.000000Z
        assertMemoryLeak(() -> {
            execute("create table x as (select 'Europe/Sofia' tz from long_sequence(1))");

            assertSql(
                    "yesterday\ttoday\ttomorrow\n" +
                            "('1970-01-06T22:00:00.000Z', '1970-01-07T21:59:59.999Z')\t('1970-01-07T22:00:00.000Z', '1970-01-08T21:59:59.999Z')\t('1970-01-08T22:00:00.000Z', '1970-01-09T21:59:59.999Z')\n",
                    "select yesterday(tz), today(tz), tomorrow(tz) from x"
            );
        });
    }

    @Test
    public void testTimestampInInterval() throws Exception {
        assertMemoryLeak(() -> {
            assertSql(
                    "result\n",
                    "select true as result from long_sequence(1)\n" +
                            "where now() in tomorrow()"
            );
            assertSql(
                    "result\n" +
                            "true\n",
                    "select true as result from long_sequence(1)\n" +
                            "where now() in today()"
            );
            assertSql(
                    "result\n",
                    "select true as result from long_sequence(1)\n" +
                            "where now() in yesterday()"
            );
        });
    }

    @Test
    public void testTimezoneDSTSwitch() throws Exception {
        // Last Sunday of March, e.g. 2024-03-31, is the DST (daylight saving time) switch day in Bulgaria (UTC+02 to UTC+03).
        assertMemoryLeak(() -> {
            setCurrentMicros(IntervalUtils.parseFloorPartialTimestamp("2024-04-01T06:00:00.000000Z"));
            String expected = "yesterday\n" +
                    "('2024-03-30T22:00:00.000Z', '2024-03-31T20:59:59.999Z')\n";
            assertSql(expected, "select yesterday('Europe/Sofia')");
            bindVariableService.clear();
            bindVariableService.setStr("tz", "Europe/Sofia");
            assertSql(expected, "select yesterday(:tz)");

            setCurrentMicros(IntervalUtils.parseFloorPartialTimestamp("2024-03-31T06:00:00.000000Z"));
            expected = "today\n" +
                    "('2024-03-30T22:00:00.000Z', '2024-03-31T20:59:59.999Z')\n";
            assertSql(expected, "select today('Europe/Sofia')");
            bindVariableService.clear();
            bindVariableService.setStr("tz", "Europe/Sofia");
            assertSql(expected, "select today(:tz)");

            setCurrentMicros(IntervalUtils.parseFloorPartialTimestamp("2024-03-30T06:00:00.000000Z"));
            expected = "tomorrow\n" +
                    "('2024-03-30T22:00:00.000Z', '2024-03-31T20:59:59.999Z')\n";
            assertSql(expected, "select tomorrow('Europe/Sofia')");
            bindVariableService.clear();
            bindVariableService.setStr("tz", "Europe/Sofia");
            assertSql(expected, "select tomorrow(:tz)");
        });
    }

    @Test
    public void testToday1() throws Exception {
        assertMemoryLeak(() -> {
            long todayStart = today(sqlExecutionContext.getNow());
            long todayEnd = tomorrow(sqlExecutionContext.getNow()) - 1;
            final Interval interval = new Interval(todayStart, todayEnd);
            assertSql("today\n" + intervalAsString(interval) + "\n", "select today()");
        });
    }

    @Test
    public void testTodayWithTimezone() throws Exception {
        setCurrentMicros(Timestamps.DAY_MICROS + Timestamps.HOUR_MICROS); // 1970-01-02T01:00:00.000000Z
        assertMemoryLeak(() -> {
            String expected = "today\n" +
                    "('1970-01-02T00:00:00.000Z', '1970-01-02T23:59:59.999Z')\n";
            assertSql(expected, "select today(null)");
            bindVariableService.clear();
            bindVariableService.setStr("tz", null);
            assertSql(expected, "select today(:tz)");

            expected = "today\n" +
                    "('1970-01-01T01:30:00.000Z', '1970-01-02T01:29:59.999Z')\n";
            assertSql(expected, "select today('UTC-01:30')");
            bindVariableService.clear();
            bindVariableService.setStr("tz", "UTC-01:30");
            assertSql(expected, "select today(:tz)");

            expected = "today\n" +
                    "('1970-01-01T22:30:00.000Z', '1970-01-02T22:29:59.999Z')\n";
            assertSql(expected, "select today('UTC+01:30')");
            bindVariableService.clear();
            bindVariableService.setStr("tz", "UTC+01:30");
            assertSql(expected, "select today(:tz)");

            expected = "today\n" +
                    "('1970-01-01T22:00:00.000Z', '1970-01-02T21:59:59.999Z')\n";
            assertSql(expected, "select today('Europe/Sofia')");
            bindVariableService.clear();
            bindVariableService.setStr("tz", "Europe/Sofia");
            assertSql(expected, "select today(:tz)");

            expected = "today\n" +
                    "('1970-01-01T05:00:00.000Z', '1970-01-02T04:59:59.999Z')\n";
            assertSql(expected, "select today('America/Toronto')");
            bindVariableService.clear();
            bindVariableService.setStr("tz", "America/Toronto");
            assertSql(expected, "select today(:tz)");
        });
    }

    @Test
    public void testTomorrow() throws Exception {
        assertMemoryLeak(() -> {
            long tomorrowStart = tomorrow(sqlExecutionContext.getNow());
            long tomorrowEnd = Timestamps.addDays(tomorrowStart, 1) - 1;
            final Interval interval = new Interval(tomorrowStart, tomorrowEnd);
            assertSql("tomorrow\n" + intervalAsString(interval) + "\n", "select tomorrow()");
        });
    }

    @Test
    public void testTomorrowWithTimezone() throws Exception {
        setCurrentMicros(2 * Timestamps.DAY_MICROS + Timestamps.HOUR_MICROS); // 1970-01-03T01:00:00.000000Z
        assertMemoryLeak(() -> {
            String expected = "tomorrow\n" +
                    "('1970-01-04T00:00:00.000Z', '1970-01-04T23:59:59.999Z')\n";
            assertSql(expected, "select tomorrow(null)");
            bindVariableService.clear();
            bindVariableService.setStr("tz", null);
            assertSql(expected, "select tomorrow(:tz)");

            expected = "tomorrow\n" +
                    "('1970-01-03T01:30:00.000Z', '1970-01-04T01:29:59.999Z')\n";
            assertSql(expected, "select tomorrow('UTC-01:30')");
            bindVariableService.clear();
            bindVariableService.setStr("tz", "UTC-01:30");
            assertSql(expected, "select tomorrow(:tz)");

            expected = "tomorrow\n" +
                    "('1970-01-03T22:30:00.000Z', '1970-01-04T22:29:59.999Z')\n";
            assertSql(expected, "select tomorrow('UTC+01:30')");
            bindVariableService.clear();
            bindVariableService.setStr("tz", "UTC+01:30");
            assertSql(expected, "select tomorrow(:tz)");

            expected = "tomorrow\n" +
                    "('1970-01-03T22:00:00.000Z', '1970-01-04T21:59:59.999Z')\n";
            assertSql(expected, "select tomorrow('Europe/Sofia')");
            bindVariableService.clear();
            bindVariableService.setStr("tz", "Europe/Sofia");
            assertSql(expected, "select tomorrow(:tz)");

            expected = "tomorrow\n" +
                    "('1970-01-03T05:00:00.000Z', '1970-01-04T04:59:59.999Z')\n";
            assertSql(expected, "select tomorrow('America/Toronto')");
            bindVariableService.clear();
            bindVariableService.setStr("tz", "America/Toronto");
            assertSql(expected, "select tomorrow(:tz)");
        });
    }

    @Test
    public void testYesterday() throws Exception {
        assertMemoryLeak(() -> {
            long yesterdayStart = yesterday(sqlExecutionContext.getNow());
            long yesterdayEnd = today(sqlExecutionContext.getNow()) - 1;
            final Interval interval = new Interval(yesterdayStart, yesterdayEnd);
            assertSql("yesterday\n" + intervalAsString(interval) + "\n", "select yesterday()");
        });
    }

    @Test
    public void testYesterdayWithTimezone() throws Exception {
        setCurrentMicros(7 * Timestamps.DAY_MICROS + Timestamps.HOUR_MICROS); // 1970-01-08T01:00:00.000000Z
        assertMemoryLeak(() -> {
            String expected = "yesterday\n" +
                    "('1970-01-07T00:00:00.000Z', '1970-01-07T23:59:59.999Z')\n";
            assertSql(expected, "select yesterday(null)");
            bindVariableService.clear();
            bindVariableService.setStr("tz", null);
            assertSql(expected, "select yesterday(:tz)");

            expected = "yesterday\n" +
                    "('1970-01-06T01:30:00.000Z', '1970-01-07T01:29:59.999Z')\n";
            assertSql(expected, "select yesterday('UTC-01:30')");
            bindVariableService.clear();
            bindVariableService.setStr("tz", "UTC-01:30");
            assertSql(expected, "select yesterday(:tz)");

            expected = "yesterday\n" +
                    "('1970-01-06T22:30:00.000Z', '1970-01-07T22:29:59.999Z')\n";
            assertSql(expected, "select yesterday('UTC+01:30')");
            bindVariableService.clear();
            bindVariableService.setStr("tz", "UTC+01:30");
            assertSql(expected, "select yesterday(:tz)");

            expected = "yesterday\n" +
                    "('1970-01-06T22:00:00.000Z', '1970-01-07T21:59:59.999Z')\n";
            assertSql(expected, "select yesterday('Europe/Sofia')");
            bindVariableService.clear();
            bindVariableService.setStr("tz", "Europe/Sofia");
            assertSql(expected, "select yesterday(:tz)");

            expected = "yesterday\n" +
                    "('1970-01-06T05:00:00.000Z', '1970-01-07T04:59:59.999Z')\n";
            assertSql(expected, "select yesterday('America/Toronto')");
            bindVariableService.clear();
            bindVariableService.setStr("tz", "America/Toronto");
            assertSql(expected, "select yesterday(:tz)");
        });
    }

    private static void buildNotInPlan(StringSink sink, long lo, long hi) {
        sink.put("VirtualRecord\n" +
                "  functions: [true]\n" +
                "    PageFrame\n" +
                "        Row forward scan\n" +
                "        Interval forward scan on: x\n" +
                "          intervals: [(\"");
        sink.put("MIN");
        sink.put("\",\"");
        sink.putISODate(lo - 1);
        sink.put("\"),(\"");
        sink.putISODate(hi);
        sink.put("\",\"");
        sink.put("MAX");
        sink.put("\")]\n");
    }

    private static String intervalAsString(Interval interval) {
        return new StringSink().put(interval).toString();
    }

    private static long today(long nowMicros) {
        return Timestamps.floorDD(nowMicros);
    }

    private static long tomorrow(long nowMicros) {
        return Timestamps.floorDD(Timestamps.addDays(nowMicros, 1));
    }

    private static long yesterday(long nowMicros) {
        return Timestamps.floorDD(Timestamps.addDays(nowMicros, -1));
    }

    private void buildInPlan(StringSink sink, long lo, long hi) {
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
