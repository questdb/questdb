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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.functions.IntervalFunction;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.Interval;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

public class IntervalFunctionTest extends AbstractCairoTest {
    private static final IntervalFunction function = new IntervalFunction(ColumnType.INTERVAL_TIMESTAMP_MICRO) {
        @Override
        public @NotNull Interval getInterval(Record rec) {
            return Interval.NULL;
        }
    };

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDecimal128() {
        function.getDecimal128(null, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDecimal16() {
        function.getDecimal16(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDecimal256() {
        function.getDecimal256(null, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDecimal32() {
        function.getDecimal32(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDecimal64() {
        function.getDecimal64(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDecimal8() {
        function.getDecimal8(null);
    }

    @Test
    public void testInterval() throws Exception {
        assertMemoryLeak(() -> {
            assertSql(
                    """
                            interval
                            ('2000-01-01T01:00:00.000Z', '2000-01-02T01:00:00.000Z')
                            """,
                    "select interval('2000-01-01T01:00:00.000Z', '2000-01-02T01:00:00.000Z')"
            );

            bindVariableService.clear();
            bindVariableService.setStr("lo", "2000-01-03T01:00:00.000Z");
            bindVariableService.setStr("hi", "2000-01-04T01:00:00.000Z");
            assertSql(
                    """
                            interval
                            ('2000-01-03T01:00:00.000Z', '2000-01-04T01:00:00.000Z')
                            """,
                    "select interval(:lo, :hi)"
            );

            assertSql(
                    """
                            interval
                            ('1970-01-01T00:00:00.000Z', '1970-01-01T00:00:00.010Z')
                            """,
                    "select interval(x,x+10000) from long_sequence(1)"
            );

            assertSql(
                    """
                            interval
                            ('2000-01-01T01:00:00.000Z', '2000-01-02T01:00:00.000Z')
                            """,
                    "select interval('2000-01-01T01:00:00.000123123Z', '2000-01-02T01:00:00.000123Z')"
            );

            bindVariableService.clear();
            bindVariableService.setStr("lo", "2000-01-03T01:00:00.000123123Z");
            bindVariableService.setStr("hi", "2000-01-04T01:00:00.000123Z");
            assertSql(
                    """
                            interval
                            ('2000-01-03T01:00:00.000Z', '2000-01-04T01:00:00.000Z')
                            """,
                    "select interval(:lo, :hi)"
            );

            assertSql(
                    """
                            interval
                            ('1970-01-01T00:00:00.000Z', '1970-01-01T00:00:00.010Z')
                            """,
                    "select interval(x,x+10000) from long_sequence(1)"
            );

            assertSql(
                    """
                            interval
                            ('1970-01-01T00:00:00.000Z', '1970-01-01T00:00:00.010Z')
                            """,
                    "select interval(x::timestamp_ns,(x+10000000)::timestamp_ns) from long_sequence(1)"
            );
        });
    }

    @Test
    public void testIntervalMixedTimestampTypes() throws Exception {
        assertMemoryLeak(() -> {
            assertQueryNoLeakCheck("""
                            interval
                            ('1970-01-01T00:00:00.000Z', '1970-01-01T00:00:00.010Z')
                            """,
                    "select interval(x::timestamp, (x+10_000_000)::timestamp_ns) from long_sequence(1)");
            assertQueryNoLeakCheck(
                    """
                            interval
                            ('1970-01-01T00:00:00.000Z', '1970-01-01T00:00:00.010Z')
                            """,
                    "select interval(x::timestamp, (x+10_000_000)::timestamp_ns) from long_sequence(1)"
            );

            assertQueryNoLeakCheck(
                    """
                            interval
                            ('1970-01-01T00:00:00.000Z', '1970-01-01T00:00:00.010Z')
                            """,
                    "select interval(x::timestamp_ns, (x+10_000)::timestamp) from long_sequence(1)"
            );

            assertQueryNoLeakCheck(
                    """
                            interval
                            
                            """,
                    "select interval(null::timestamp_ns, x::timestamp) from long_sequence(1)"
            );

            assertQueryNoLeakCheck(
                    """
                            interval
                            
                            """,
                    "select interval(x, null::timestamp) from long_sequence(1)"
            );


            execute("create table test_mixed (ts_micro timestamp, ts_nano timestamp_ns)");
            execute("insert into test_mixed values ('2000-01-01T00:00:00.000Z', '2000-01-01T00:00:10.000Z')");

            assertQueryNoLeakCheck(
                    """
                            interval
                            ('2000-01-01T00:00:00.000Z', '2000-01-01T00:00:10.000Z')
                            """,
                    "select interval(ts_micro, ts_nano) from test_mixed"
            );

            assertException("select interval(ts_nano, ts_micro) from test_mixed", 0, "invalid interval boundaries");
            assertException("select interval(ts_micro + 11_000_000, ts_nano) from test_mixed", 0, "invalid interval boundaries");

            assertQueryNoLeakCheck(
                    """
                            interval
                            ('2000-01-01T00:00:10.000Z', '2000-01-01T00:00:10.000Z')
                            """,
                    "select interval(ts_nano, ts_micro + 10_000_000) from test_mixed"
            );
        });
    }

    @Test
    public void testIntervalStartEnd() throws Exception {
        assertMemoryLeak(() -> {
            assertSql(
                    """
                            column
                            true
                            """,
                    "select interval_start(today()) = date_trunc('day', now()) from long_sequence(1)"
            );
            assertSql(
                    """
                            column
                            true
                            """,
                    "select interval_start(today()) = date_trunc('day', now()::timestamp_ns) from long_sequence(1)"
            );
            assertSql(
                    """
                            column
                            true
                            """,
                    "select interval_end(today()) = dateadd('d', 1, date_trunc('day', now()))-1 from long_sequence(1)\n"
            );
            sqlExecutionContext.setIntervalFunctionType(ColumnType.INTERVAL_TIMESTAMP_NANO);
            assertSql(
                    """
                            column
                            true
                            """,
                    "select interval_end(today()) = dateadd('d', 1, date_trunc('day', now()::timestamp_ns))-1 from long_sequence(1)\n"
            );
            sqlExecutionContext.setIntervalFunctionType(ColumnType.INTERVAL_TIMESTAMP_MICRO);
        });
    }

    @Test
    public void testIntrinsics1() throws Exception {
        testIntrinsics1(ColumnType.TIMESTAMP_MICRO);
    }

    @Test
    public void testIntrinsics2() throws Exception {
        testIntrinsics1(ColumnType.TIMESTAMP_NANO);
    }

    @Test
    public void testIntrinsics3() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosTimestampDriver.INSTANCE.fromDays(1));
            execute("CREATE TABLE x as (select x::timestamp ts from long_sequence(10)) timestamp(ts) PARTITION BY DAY WAL;");
            drainWalQueue();

            assertSql(
                    """
                            ts
                            1970-01-01T00:00:00.000001Z
                            1970-01-01T00:00:00.000002Z
                            1970-01-01T00:00:00.000003Z
                            1970-01-01T00:00:00.000004Z
                            1970-01-01T00:00:00.000005Z
                            1970-01-01T00:00:00.000006Z
                            1970-01-01T00:00:00.000007Z
                            1970-01-01T00:00:00.000008Z
                            1970-01-01T00:00:00.000009Z
                            1970-01-01T00:00:00.000010Z
                            """,
                    "select * from x where ts in yesterday()"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts in yesterday()",
                    """
                            PageFrame
                                Row forward scan
                                Interval forward scan on: x
                                  intervals: [("1970-01-01T00:00:00.000000Z","1970-01-01T23:59:59.999999Z")]
                            """
            );

            assertSql(
                    "ts\n",
                    "select * from x where ts in today()"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts in today()",
                    """
                            PageFrame
                                Row forward scan
                                Interval forward scan on: x
                                  intervals: [("1970-01-02T00:00:00.000000Z","1970-01-02T23:59:59.999999Z")]
                            """
            );

            assertSql(
                    "ts\n",
                    "select * from x where ts in tomorrow()"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts in tomorrow()",
                    """
                            PageFrame
                                Row forward scan
                                Interval forward scan on: x
                                  intervals: [("1970-01-03T00:00:00.000000Z","1970-01-03T23:59:59.999999Z")]
                            """
            );
        });
    }

    @Test
    public void testIntrinsics4() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosTimestampDriver.INSTANCE.fromDays(1));
            execute("CREATE TABLE x as (select x::timestamp_ns ts from long_sequence(10)) timestamp(ts) PARTITION BY DAY WAL;");
            drainWalQueue();

            assertSql(
                    """
                            ts
                            1970-01-01T00:00:00.000000001Z
                            1970-01-01T00:00:00.000000002Z
                            1970-01-01T00:00:00.000000003Z
                            1970-01-01T00:00:00.000000004Z
                            1970-01-01T00:00:00.000000005Z
                            1970-01-01T00:00:00.000000006Z
                            1970-01-01T00:00:00.000000007Z
                            1970-01-01T00:00:00.000000008Z
                            1970-01-01T00:00:00.000000009Z
                            1970-01-01T00:00:00.000000010Z
                            """,
                    "select * from x where ts in yesterday()"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts in yesterday()",
                    """
                            PageFrame
                                Row forward scan
                                Interval forward scan on: x
                                  intervals: [("1970-01-01T00:00:00.000000000Z","1970-01-01T23:59:59.999999999Z")]
                            """
            );

            assertSql(
                    "ts\n",
                    "select * from x where ts in today()"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts in today()",
                    """
                            PageFrame
                                Row forward scan
                                Interval forward scan on: x
                                  intervals: [("1970-01-02T00:00:00.000000000Z","1970-01-02T23:59:59.999999999Z")]
                            """
            );

            assertSql(
                    "ts\n",
                    "select * from x where ts in tomorrow()"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts in tomorrow()",
                    """
                            PageFrame
                                Row forward scan
                                Interval forward scan on: x
                                  intervals: [("1970-01-03T00:00:00.000000000Z","1970-01-03T23:59:59.999999999Z")]
                            """
            );
        });
    }

    @Test
    public void testIntrinsicsAllVirtual() throws Exception {
        assertMemoryLeak(() -> {
            assertSql(
                    """
                            bool
                            true
                            """,
                    "select true as bool from long_sequence(1) where now() in today()"
            );
            // no interval scan with now()
            assertPlanNoLeakCheck(
                    "select true as bool from long_sequence(1) where now() in today()",
                    """
                            VirtualRecord
                              functions: [true]
                                Filter filter: now() in today()
                                    long_sequence count: 1
                            """
            );
        });
    }

    @Test
    public void testIntrinsicsNonPartitioned() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (k int, ts timestamp);");
            assertPlanNoLeakCheck(
                    "select * from x where ts in today() or ts in tomorrow() or ts in yesterday();",
                    """
                            Async Filter workers: 1
                              filter: ((ts in today() or ts in tomorrow()) or ts in yesterday())
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """
            );

            execute("create table x1 (k int, ts timestamp_ns);");
            assertPlanNoLeakCheck(
                    "select * from x where ts in today() or ts in tomorrow() or ts in yesterday();",
                    """
                            Async Filter workers: 1
                              filter: ((ts in today() or ts in tomorrow()) or ts in yesterday())
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """
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
        setCurrentMicros(MicrosTimestampDriver.INSTANCE.fromDays(7) + MicrosTimestampDriver.INSTANCE.fromHours(1)); // 1970-01-08T01:00:00.000000Z
        assertMemoryLeak(() -> {
            execute("create table x as (select 'Europe/Sofia' tz from long_sequence(1))");

            assertSql(
                    """
                            yesterday\ttoday\ttomorrow
                            ('1970-01-06T22:00:00.000Z', '1970-01-07T21:59:59.999Z')\t('1970-01-07T22:00:00.000Z', '1970-01-08T21:59:59.999Z')\t('1970-01-08T22:00:00.000Z', '1970-01-09T21:59:59.999Z')
                            """,
                    "select yesterday(tz), today(tz), tomorrow(tz) from x"
            );
        });
    }

    @Test
    public void testNowNsInInterval() throws Exception {
        assertMemoryLeak(() -> {
            assertSql(
                    "result\n",
                    "select true as result from long_sequence(1)\n" +
                            "where now_ns() in tomorrow()"
            );
            assertSql(
                    """
                            result
                            true
                            """,
                    "select true as result from long_sequence(1)\n" +
                            "where now_ns() in today()"
            );
            assertSql(
                    "result\n",
                    "select true as result from long_sequence(1)\n" +
                            "where now_ns() in yesterday()"
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
                    """
                            result
                            true
                            """,
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
            setCurrentMicros(MicrosTimestampDriver.floor("2024-04-01T06:00:00.000000Z"));
            String expected = """
                    yesterday
                    ('2024-03-30T22:00:00.000Z', '2024-03-31T20:59:59.999Z')
                    """;
            assertSql(expected, "select yesterday('Europe/Sofia')");
            bindVariableService.clear();
            bindVariableService.setStr("tz", "Europe/Sofia");
            assertSql(expected, "select yesterday(:tz)");

            setCurrentMicros(MicrosTimestampDriver.floor("2024-03-31T06:00:00.000000Z"));
            expected = """
                    today
                    ('2024-03-30T22:00:00.000Z', '2024-03-31T20:59:59.999Z')
                    """;
            assertSql(expected, "select today('Europe/Sofia')");
            bindVariableService.clear();
            bindVariableService.setStr("tz", "Europe/Sofia");
            assertSql(expected, "select today(:tz)");

            setCurrentMicros(MicrosTimestampDriver.floor("2024-03-30T06:00:00.000000Z"));
            expected = """
                    tomorrow
                    ('2024-03-30T22:00:00.000Z', '2024-03-31T20:59:59.999Z')
                    """;
            assertSql(expected, "select tomorrow('Europe/Sofia')");
            bindVariableService.clear();
            bindVariableService.setStr("tz", "Europe/Sofia");
            assertSql(expected, "select tomorrow(:tz)");
        });
    }

    @Test
    public void testToday1() throws Exception {
        testToday(ColumnType.TIMESTAMP_MICRO);
    }

    @Test
    public void testToday2() throws Exception {
        testToday(ColumnType.TIMESTAMP_NANO);
    }

    @Test
    public void testTodayWithTimezone() throws Exception {
        setCurrentMicros(MicrosTimestampDriver.INSTANCE.fromDays(1) + MicrosTimestampDriver.INSTANCE.fromHours(1)); // 1970-01-02T01:00:00.000000Z
        assertMemoryLeak(() -> {
            String expected = """
                    today
                    ('1970-01-02T00:00:00.000Z', '1970-01-02T23:59:59.999Z')
                    """;
            assertSql(expected, "select today(null)");
            bindVariableService.clear();
            bindVariableService.setStr("tz", null);
            assertSql(expected, "select today(:tz)");

            expected = """
                    today
                    ('1970-01-01T01:30:00.000Z', '1970-01-02T01:29:59.999Z')
                    """;
            assertSql(expected, "select today('UTC-01:30')");
            bindVariableService.clear();
            bindVariableService.setStr("tz", "UTC-01:30");
            assertSql(expected, "select today(:tz)");

            expected = """
                    today
                    ('1970-01-01T22:30:00.000Z', '1970-01-02T22:29:59.999Z')
                    """;
            assertSql(expected, "select today('UTC+01:30')");
            bindVariableService.clear();
            bindVariableService.setStr("tz", "UTC+01:30");
            assertSql(expected, "select today(:tz)");

            expected = """
                    today
                    ('1970-01-01T22:00:00.000Z', '1970-01-02T21:59:59.999Z')
                    """;
            assertSql(expected, "select today('Europe/Sofia')");
            bindVariableService.clear();
            bindVariableService.setStr("tz", "Europe/Sofia");
            assertSql(expected, "select today(:tz)");

            expected = """
                    today
                    ('1970-01-01T05:00:00.000Z', '1970-01-02T04:59:59.999Z')
                    """;
            assertSql(expected, "select today('America/Toronto')");
            bindVariableService.clear();
            bindVariableService.setStr("tz", "America/Toronto");
            assertSql(expected, "select today(:tz)");
        });
    }

    @Test
    public void testTomorrow() throws Exception {
        testTomorrow(ColumnType.TIMESTAMP_MICRO);
    }

    @Test
    public void testTomorrow2() throws Exception {
        testTomorrow(ColumnType.TIMESTAMP_NANO);
    }

    @Test
    public void testTomorrowWithTimezone() throws Exception {
        setCurrentMicros(MicrosTimestampDriver.INSTANCE.fromDays(2) + MicrosTimestampDriver.INSTANCE.fromHours(1)); // 1970-01-03T01:00:00.000000Z
        assertMemoryLeak(() -> {
            String expected = """
                    tomorrow
                    ('1970-01-04T00:00:00.000Z', '1970-01-04T23:59:59.999Z')
                    """;
            assertSql(expected, "select tomorrow(null)");
            bindVariableService.clear();
            bindVariableService.setStr("tz", null);
            assertSql(expected, "select tomorrow(:tz)");

            expected = """
                    tomorrow
                    ('1970-01-03T01:30:00.000Z', '1970-01-04T01:29:59.999Z')
                    """;
            assertSql(expected, "select tomorrow('UTC-01:30')");
            bindVariableService.clear();
            bindVariableService.setStr("tz", "UTC-01:30");
            assertSql(expected, "select tomorrow(:tz)");

            expected = """
                    tomorrow
                    ('1970-01-03T22:30:00.000Z', '1970-01-04T22:29:59.999Z')
                    """;
            assertSql(expected, "select tomorrow('UTC+01:30')");
            bindVariableService.clear();
            bindVariableService.setStr("tz", "UTC+01:30");
            assertSql(expected, "select tomorrow(:tz)");

            expected = """
                    tomorrow
                    ('1970-01-03T22:00:00.000Z', '1970-01-04T21:59:59.999Z')
                    """;
            assertSql(expected, "select tomorrow('Europe/Sofia')");
            bindVariableService.clear();
            bindVariableService.setStr("tz", "Europe/Sofia");
            assertSql(expected, "select tomorrow(:tz)");

            expected = """
                    tomorrow
                    ('1970-01-03T05:00:00.000Z', '1970-01-04T04:59:59.999Z')
                    """;
            assertSql(expected, "select tomorrow('America/Toronto')");
            bindVariableService.clear();
            bindVariableService.setStr("tz", "America/Toronto");
            assertSql(expected, "select tomorrow(:tz)");
        });
    }

    @Test
    public void testYesterday1() throws Exception {
        testYesterday(ColumnType.TIMESTAMP_MICRO);
    }

    @Test
    public void testYesterday2() throws Exception {
        testYesterday(ColumnType.TIMESTAMP_NANO);
    }

    @Test
    public void testYesterdayWithTimezone() throws Exception {
        setCurrentMicros(MicrosTimestampDriver.INSTANCE.fromDays(7) + MicrosTimestampDriver.INSTANCE.fromHours(1)); // 1970-01-08T01:00:00.000000Z
        assertMemoryLeak(() -> {
            String expected = """
                    yesterday
                    ('1970-01-07T00:00:00.000Z', '1970-01-07T23:59:59.999Z')
                    """;
            assertSql(expected, "select yesterday(null)");
            bindVariableService.clear();
            bindVariableService.setStr("tz", null);
            assertSql(expected, "select yesterday(:tz)");

            expected = """
                    yesterday
                    ('1970-01-06T01:30:00.000Z', '1970-01-07T01:29:59.999Z')
                    """;
            assertSql(expected, "select yesterday('UTC-01:30')");
            bindVariableService.clear();
            bindVariableService.setStr("tz", "UTC-01:30");
            assertSql(expected, "select yesterday(:tz)");

            expected = """
                    yesterday
                    ('1970-01-06T22:30:00.000Z', '1970-01-07T22:29:59.999Z')
                    """;
            assertSql(expected, "select yesterday('UTC+01:30')");
            bindVariableService.clear();
            bindVariableService.setStr("tz", "UTC+01:30");
            assertSql(expected, "select yesterday(:tz)");

            expected = """
                    yesterday
                    ('1970-01-06T22:00:00.000Z', '1970-01-07T21:59:59.999Z')
                    """;
            assertSql(expected, "select yesterday('Europe/Sofia')");
            bindVariableService.clear();
            bindVariableService.setStr("tz", "Europe/Sofia");
            assertSql(expected, "select yesterday(:tz)");

            expected = """
                    yesterday
                    ('1970-01-06T05:00:00.000Z', '1970-01-07T04:59:59.999Z')
                    """;
            assertSql(expected, "select yesterday('America/Toronto')");
            bindVariableService.clear();
            bindVariableService.setStr("tz", "America/Toronto");
            assertSql(expected, "select yesterday(:tz)");
        });
    }

    private static void buildNotInPlan(TimestampDriver driver, StringSink sink, long lo, long hi) {
        sink.put("""
                VirtualRecord
                  functions: [true]
                    PageFrame
                        Row forward scan
                        Interval forward scan on: x
                          intervals: [(\"""");
        sink.put("MIN");
        sink.put("\",\"");
        sink.putISODate(driver, lo - 1);
        sink.put("\"),(\"");
        sink.putISODate(driver, hi);
        sink.put("\",\"");
        sink.put("MAX");
        sink.put("\")]\n");
    }

    private static String intervalAsString(Interval interval, int columnType) {
        StringSink sink = new StringSink();
        interval.toSink(sink, columnType);
        return sink.toString();
    }

    private static long today(TimestampDriver driver, long nowMicros) {
        return driver.startOfDay(nowMicros, 0);
    }

    private static long tomorrow(TimestampDriver driver, long nowMicros) {
        return driver.startOfDay(nowMicros, 1);
    }

    private static long yesterday(TimestampDriver driver, long nowMicros) {
        return driver.startOfDay(nowMicros, -1);
    }

    private void buildInPlan(TimestampDriver driver, StringSink sink, long lo, long hi) {
        sink.put("""
                VirtualRecord
                  functions: [true]
                    PageFrame
                        Row forward scan
                        Interval forward scan on: x
                          intervals: [(\"""");
        sink.putISODate(driver, lo);
        sink.put("\",\"");
        sink.putISODate(driver, hi);
        sink.put("\")]\n");
    }

    private void testIntrinsics1(int columnType) throws Exception {
        assertMemoryLeak(() -> {
            String timestampTypeName = ColumnType.nameOf(columnType);
            TimestampDriver driver = ColumnType.getTimestampDriver(columnType);
            executeWithRewriteTimestamp("CREATE TABLE x (ts #TIMESTAMP) timestamp(ts) PARTITION BY DAY WAL;", timestampTypeName);
            // should have interval scans despite use of function
            // due to optimisation step to convert it to a constant
            long today = today(driver, sqlExecutionContext.getNow(columnType));
            long tomorrow = tomorrow(driver, sqlExecutionContext.getNow(columnType));
            long yesterday = yesterday(driver, sqlExecutionContext.getNow(columnType));

            long tomorrowAndOne = driver.addDays(tomorrow, 1);
            long todayAndOne = driver.addDays(today, 1);

            StringSink sink = new StringSink();
            buildInPlan(driver, sink, today, tomorrow - 1);
            assertPlanNoLeakCheck(
                    "select true as bool from x where ts in today()",
                    sink.toString()
            );

            sink.clear();
            buildInPlan(driver, sink, tomorrow, tomorrowAndOne - 1);
            assertPlanNoLeakCheck(
                    "select true as bool from x where ts in tomorrow()",
                    sink.toString()
            );

            sink.clear();
            buildInPlan(driver, sink, yesterday, today - 1);
            assertPlanNoLeakCheck(
                    "select true as bool from x where ts in yesterday()",
                    sink.toString()
            );

            sink.clear();
            buildNotInPlan(driver, sink, today, todayAndOne);
            assertPlanNoLeakCheck(
                    "select true as bool from x where ts not in today()",
                    sink.toString()
            );

            assertPlanNoLeakCheck(
                    "select true as bool from x where ts in yesterday() and ts in today() and ts in tomorrow()",
                    """
                            VirtualRecord
                              functions: [true]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: x
                                      intervals: []
                            """
            );

            assertPlanNoLeakCheck(
                    "select true as bool from x where ts in null::interval",
                    """
                            VirtualRecord
                              functions: [true]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: x
                                      intervals: [("MIN","MIN")]
                            """
            );
        });
    }

    private void testToday(int timestampType) throws Exception {
        assertMemoryLeak(() -> {
            TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
            long todayStart = today(driver, sqlExecutionContext.getNow(timestampType));
            long todayEnd = tomorrow(driver, sqlExecutionContext.getNow(timestampType)) - 1;
            final Interval interval = new Interval(todayStart, todayEnd);
            assertSql("today\n" + intervalAsString(interval, IntervalUtils.getIntervalType(timestampType)) + "\n", "select today()");
        });
    }

    private void testTomorrow(int timestampType) throws Exception {
        assertMemoryLeak(() -> {
            TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
            long tomorrowStart = tomorrow(driver, sqlExecutionContext.getNow(timestampType));
            long tomorrowEnd = driver.addDays(tomorrowStart, 1) - 1;
            final Interval interval = new Interval(tomorrowStart, tomorrowEnd);
            assertSql("tomorrow\n" + intervalAsString(interval, IntervalUtils.getIntervalType(timestampType)) + "\n", "select tomorrow()");
        });
    }

    private void testYesterday(int timestampType) throws Exception {
        assertMemoryLeak(() -> {
            TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
            long yesterdayStart = yesterday(driver, sqlExecutionContext.getNow(timestampType));
            long yesterdayEnd = today(driver, sqlExecutionContext.getNow(timestampType)) - 1;
            final Interval interval = new Interval(yesterdayStart, yesterdayEnd);
            assertSql("yesterday\n" + intervalAsString(interval, IntervalUtils.getIntervalType(timestampType)) + "\n", "select yesterday()");
        });
    }
}
