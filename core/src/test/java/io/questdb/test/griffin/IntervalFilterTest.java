/*+*****************************************************************************
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

package io.questdb.test.griffin;

import io.questdb.DefaultFactoryProvider;
import io.questdb.FactoryProvider;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.TickCalendarServiceFactory;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.CompiledTickExpression;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.jit.JitUtil;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.cairo.TestTickCalendarService;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@RunWith(Parameterized.class)
public class IntervalFilterTest extends AbstractCairoTest {
    private final TestTimestampType timestampType;

    public IntervalFilterTest(TestTimestampType timestampType) {
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> testParams() {
        return Arrays.asList(new Object[][]{
                {TestTimestampType.MICRO}, {TestTimestampType.NANO}
        });
    }

    @Test
    public void testBetween() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as " +
                            "(" +
                            "  select" +
                            "    rnd_double(0)*100 a," +
                            "    timestamp_sequence(0, 1000000)::" + timestampType.getTypeName() + " ts" +
                            "  from long_sequence(3)" +
                            ") timestamp(ts) partition by day"
            );

            String expected = replaceTimestampSuffix("""
                    a\tts
                    8.486964232560668\t1970-01-01T00:00:01.000000Z
                    8.43832076262595\t1970-01-01T00:00:02.000000Z
                    """, timestampType.getTypeName());
            assertQuery("select * from x where ts between '1970-01-01T00:00:01.000000Z' and '1970-01-01T00:00:02.000000Z'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan(replaceTimestampSuffix("""
                            PageFrame
                                Row forward scan
                                Interval forward scan on: x
                                  intervals: [("1970-01-01T00:00:01.000000Z","1970-01-01T00:00:02.000000Z")]
                            """, timestampType.getTypeName()))
                    .returns(expected);
            assertQuery("select * from x where ts between '1970-01-01T00:00:01.000000000Z' and '1970-01-01T00:00:02.000000000Z'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan(replaceTimestampSuffix("""
                            PageFrame
                                Row forward scan
                                Interval forward scan on: x
                                  intervals: [("1970-01-01T00:00:01.000000Z","1970-01-01T00:00:02.000000Z")]
                            """, timestampType.getTypeName()))
                    .returns(expected);
            bindVariableService.clear();
            bindVariableService.setStr("ts1", "1970-01-01T00:00:01.000000Z");
            bindVariableService.setStr("ts2", "1970-01-01T00:00:02.000000Z");
            bindVariableService.setStr("ts3", "1970-01-01T00:00:01.000000000Z");
            bindVariableService.setStr("ts4", "1970-01-01T00:00:02.000000000Z");
            assertQuery("select * from x where ts between :ts1 and :ts2")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);
            assertQuery("select * from x where ts between :ts3 and :ts4")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);
        });
    }

    @Test
    public void testEqualTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as " +
                            "(" +
                            "  select" +
                            "    rnd_double(0)*100 a," +
                            "    timestamp_sequence(0, 10000)::" + timestampType.getTypeName() + " ts" +
                            "  from long_sequence(3)" +
                            ") timestamp(ts) partition by day"
            );

            String expected = replaceTimestampSuffix1("""
                    a\tts
                    8.486964232560668\t1970-01-01T00:00:00.010000Z
                    """, timestampType.getTypeName());
            assertQuery("select * from x where ts = '1970-01-01T00:00:00.010000Z'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan(replaceTimestampSuffix1("""
                            PageFrame
                                Row forward scan
                                Interval forward scan on: x
                                  intervals: [("1970-01-01T00:00:00.010000Z","1970-01-01T00:00:00.010000Z")]
                            """, timestampType.getTypeName()))
                    .returns(expected);
            assertQuery("select * from x where ts = '1970-01-01T00:00:00.010000000Z'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan(replaceTimestampSuffix1("""
                            PageFrame
                                Row forward scan
                                Interval forward scan on: x
                                  intervals: [("1970-01-01T00:00:00.010000Z","1970-01-01T00:00:00.010000Z")]
                            """, timestampType.getTypeName()))
                    .returns(expected);
            bindVariableService.clear();
            bindVariableService.setStr("ts", "1970-01-01T00:00:00.010000Z");
            bindVariableService.setStr("ts1", "1970-01-01T00:00:00.010000000Z");
            assertQuery("select * from x where ts = :ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);
            assertQuery("select * from x where ts = :ts1")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);

            expected = replaceTimestampSuffix1("""
                    a\tts
                    80.43224099968394\t1970-01-01T00:00:00.000000Z
                    8.43832076262595\t1970-01-01T00:00:00.020000Z
                    """, timestampType.getTypeName());
            assertQuery("select * from x where ts != '1970-01-01T00:00:00.010000Z'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"MIN\",\"1970-01-01T00:00:00.009999Z\"),(\"1970-01-01T00:00:00.010001Z\",\"MAX\")]\n" :
                                    "      intervals: [(\"MIN\",\"1970-01-01T00:00:00.009999999Z\"),(\"1970-01-01T00:00:00.010000001Z\",\"MAX\")]\n"))
                    .returns(expected);
            assertQuery("select * from x where ts != '1970-01-01T00:00:00.010000000Z'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"MIN\",\"1970-01-01T00:00:00.009999Z\"),(\"1970-01-01T00:00:00.010001Z\",\"MAX\")]\n" :
                                    "      intervals: [(\"MIN\",\"1970-01-01T00:00:00.009999999Z\"),(\"1970-01-01T00:00:00.010000001Z\",\"MAX\")]\n"))
                    .returns(expected);
            bindVariableService.clear();
            bindVariableService.setStr("ts", "1970-01-01T00:00:00.010000Z");
            bindVariableService.setStr("ts1", "1970-01-01T00:00:00.010000000Z");
            assertQuery("select * from x where ts != :ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);
            assertQuery("select * from x where ts != :ts1")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);
            expected = replaceTimestampSuffix1("""
                    a\tts
                    80.43224099968394\t1970-01-01T00:00:00.000000Z
                    8.43832076262595\t1970-01-01T00:00:00.020000Z
                    """, timestampType.getTypeName());
            assertQuery("select * from x where ts in '1970-01-01' and ts != '1970-01-01T00:00:00.010000Z'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"1970-01-01T00:00:00.000000Z\",\"1970-01-01T00:00:00.009999Z\"),(\"1970-01-01T00:00:00.010001Z\",\"1970-01-01T23:59:59.999999Z\")]\n" :
                                    "      intervals: [(\"1970-01-01T00:00:00.000000000Z\",\"1970-01-01T00:00:00.009999999Z\"),(\"1970-01-01T00:00:00.010000001Z\",\"1970-01-01T23:59:59.999999999Z\")]\n"))
                    .returns(expected);
            assertQuery("select * from x where ts in '1970-01-01' and ts != '1970-01-01T00:00:00.010000000Z'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"1970-01-01T00:00:00.000000Z\",\"1970-01-01T00:00:00.009999Z\"),(\"1970-01-01T00:00:00.010001Z\",\"1970-01-01T23:59:59.999999Z\")]\n" :
                                    "      intervals: [(\"1970-01-01T00:00:00.000000000Z\",\"1970-01-01T00:00:00.009999999Z\"),(\"1970-01-01T00:00:00.010000001Z\",\"1970-01-01T23:59:59.999999999Z\")]\n"))
                    .returns(expected);
            bindVariableService.clear();
            bindVariableService.setStr("date", "1970-01-01");
            bindVariableService.setStr("ts", "1970-01-01T00:00:00.010000Z");
            bindVariableService.setStr("ts1", "1970-01-01T00:00:00.010000000Z");
            assertQuery("select * from x where ts in :date and ts != :ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);
            assertQuery("select * from x where ts in :date and ts != :ts1")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);

            expected = "a\tts\n";
            assertQuery("select * from x where ts = '1970-01-01T00:00:00.010000Z' and ts = '1970-01-01T00:00:00.020000Z' and ts = '1970-01-01T00:00:00.030000Z'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("Empty table\n")
                    .returns(expected);
            assertQuery("select * from x where ts = '1970-01-01T00:00:00.010000000Z' and ts = '1970-01-01T00:00:00.020000000Z' and ts = '1970-01-01T00:00:00.030000000Z'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("Empty table\n")
                    .returns(expected);
            bindVariableService.clear();
            bindVariableService.setStr("ts", "1970-01-01T00:00:00.010000Z");
            bindVariableService.setStr("ts1", "1970-01-01T00:00:00.010000Z");
            assertQuery("select * from x where ts = :ts and ts = '1970-01-01T00:00:00.020000Z' and ts = '1970-01-01T00:00:00.030000Z'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);
            assertQuery("select * from x where ts = :ts1 and ts = '1970-01-01T00:00:00.020000000Z' and ts = '1970-01-01T00:00:00.030000000Z'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);

            expected = "a\tts\n";
            assertQuery("select * from x where ts != '1970-01-01T00:00:00.010000Z' and ts = '1970-01-01T00:00:00.020000Z' and ts = '1970-01-01T00:00:00.030000Z'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("Empty table\n")
                    .returns(expected);
            assertQuery("select * from x where ts != '1970-01-01T00:00:00.010000000Z' and ts = '1970-01-01T00:00:00.020000000Z' and ts = '1970-01-01T00:00:00.030000000Z'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("Empty table\n")
                    .returns(expected);
            bindVariableService.clear();
            bindVariableService.setStr("ts", "1970-01-01T00:00:00.010000Z");
            bindVariableService.setStr("ts1", "1970-01-01T00:00:00.010000000Z");
            assertQuery("select * from x where ts != :ts and ts = '1970-01-01T00:00:00.020000Z' and ts = '1970-01-01T00:00:00.030000Z'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);
            assertQuery("select * from x where ts != :ts1 and ts = '1970-01-01T00:00:00.020000000Z' and ts = '1970-01-01T00:00:00.030000000Z'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);

            assertQuery("select * from x where ts = (-9223372036854775808)::timestamp")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("a\tts\n");
            assertQuery("select * from x where ts = (-9223372036854775808)::timestamp_ns")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("a\tts\n");
        });
    }

    @Test
    public void testEqualTimestampCursor() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (" +
                            "  select" +
                            "    rnd_double(0)*100 a," +
                            "    timestamp_sequence(0, 10000)::" + timestampType.getTypeName() + " ts" +
                            "  from long_sequence(3)" +
                            ") timestamp(ts) partition by day"
            );

            assertQuery("select * from x where ts = (select max(ts) from x)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan(replaceTimestampSuffix1("""
                            PageFrame
                                Row forward scan
                                Interval forward scan on: x
                                  intervals: [("1970-01-01T00:00:00.020000Z","1970-01-01T00:00:00.020000Z")]
                            """, timestampType.getTypeName()))
                    .returns(replaceTimestampSuffix1("""
                            a\tts
                            8.43832076262595\t1970-01-01T00:00:00.020000Z
                            """, timestampType.getTypeName()));
        });
    }

    @Test
    public void testEqualTimestampOrWithNullBindVariable() throws Exception {
        // End-to-end guard for the OR-anchor union folding in WhereClauseParser: a nullable timestamp
        // bound in the FIRST disjunct of `ts = $1 or ts = $2` must drop only its own disjunct, not the
        // whole disjunction. The plan assertion pins that the OR group is still consumed as a runtime
        // interval union (no residual filter fallback), so the interval model alone decides the answer -
        // an anchor that intersected a NULL bound would return zero rows here instead of $2's row.
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (" +
                            "  select" +
                            "    x a," +
                            "    timestamp_sequence(0, 10000)::" + timestampType.getTypeName() + " ts" +
                            "  from long_sequence(3)" +
                            ") timestamp(ts) partition by day"
            );

            final long ts2 = timestampType.getDriver().parseFloorLiteral("1970-01-01T00:00:00.020000Z");
            final String expected = replaceTimestampSuffix1("""
                    a	ts
                    3	1970-01-01T00:00:00.020000Z
                    """, timestampType.getTypeName());

            final String query = "select * from x where ts = $1 or ts = $2";

            // NULL on the left (the anchor disjunct)
            bindVariableService.clear();
            bindVariableService.setTimestampWithType(0, timestampType.getTimestampType(), Numbers.LONG_NULL);
            bindVariableService.setTimestampWithType(1, timestampType.getTimestampType(), ts2);
            assertQuery(query).noLeakCheck().assertsPlanContaining("Interval forward scan on: x");
            assertQuery(query).noLeakCheck().timestamp("ts").returns(expected);

            // NULL on the right (mirror: pins the symmetry)
            bindVariableService.clear();
            bindVariableService.setTimestampWithType(0, timestampType.getTimestampType(), ts2);
            bindVariableService.setTimestampWithType(1, timestampType.getTimestampType(), Numbers.LONG_NULL);
            assertQuery(query).noLeakCheck().assertsPlanContaining("Interval forward scan on: x");
            assertQuery(query).noLeakCheck().timestamp("ts").returns(expected);
        });
    }

    @Test
    public void testGreaterTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as " +
                            "(" +
                            "  select" +
                            "    rnd_double(0)*100 a," +
                            "    timestamp_sequence(0, 10000)::" + timestampType.getTypeName() + " ts" +
                            "  from long_sequence(3)" +
                            ") timestamp(ts) partition by day"
            );

            String expected = replaceTimestampSuffix1("""
                    a\tts
                    8.43832076262595\t1970-01-01T00:00:00.020000Z
                    """, timestampType.getTypeName());
            assertQuery("select * from x where ts > '1970-01-01T00:00:00.010000Z'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"1970-01-01T00:00:00.010001Z\",\"MAX\")]\n" :
                                    "      intervals: [(\"1970-01-01T00:00:00.010000001Z\",\"MAX\")]\n"))
                    .returns(expected);
            assertQuery("select * from x where ts > '1970-01-01T00:00:00.010000000Z'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"1970-01-01T00:00:00.010001Z\",\"MAX\")]\n" :
                                    "      intervals: [(\"1970-01-01T00:00:00.010000001Z\",\"MAX\")]\n"))
                    .returns(expected);
            bindVariableService.clear();
            bindVariableService.setStr("ts", "1970-01-01T00:00:00.010000Z");
            bindVariableService.setStr("ts1", "1970-01-01T00:00:00.010000000Z");
            assertQuery("select * from x where ts > :ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);
            assertQuery("select * from x where ts > :ts1")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);

            expected = replaceTimestampSuffix1("""
                    a\tts
                    80.43224099968394\t1970-01-01T00:00:00.000000Z
                    8.486964232560668\t1970-01-01T00:00:00.010000Z
                    """, timestampType.getTypeName());
            assertQuery("select * from x where ts <= '1970-01-01T00:00:00.010000Z'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan(replaceTimestampSuffix1("""
                            PageFrame
                                Row forward scan
                                Interval forward scan on: x
                                  intervals: [("MIN","1970-01-01T00:00:00.010000Z")]
                            """, timestampType.getTypeName()))
                    .returns(expected);
            assertQuery("select * from x where ts <= '1970-01-01T00:00:00.010000000Z'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan(replaceTimestampSuffix1("""
                            PageFrame
                                Row forward scan
                                Interval forward scan on: x
                                  intervals: [("MIN","1970-01-01T00:00:00.010000Z")]
                            """, timestampType.getTypeName()))
                    .returns(expected);
            bindVariableService.clear();
            bindVariableService.setStr("ts", "1970-01-01T00:00:00.010000Z");
            bindVariableService.setStr("ts1", "1970-01-01T00:00:00.010000000Z");
            assertQuery("select * from x where ts <= :ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);
            assertQuery("select * from x where ts <= :ts1")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);

            expected = replaceTimestampSuffix1("""
                    a\tts
                    80.43224099968394\t1970-01-01T00:00:00.000000Z
                    8.486964232560668\t1970-01-01T00:00:00.010000Z
                    """, timestampType.getTypeName());
            assertQuery("select * from x where ts in '1970-01-01' and ts <= '1970-01-01T00:00:00.010000Z'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan(replaceTimestampSuffix1("""
                            PageFrame
                                Row forward scan
                                Interval forward scan on: x
                                  intervals: [("1970-01-01T00:00:00.000000Z","1970-01-01T00:00:00.010000Z")]
                            """, timestampType.getTypeName()))
                    .returns(expected);
            assertQuery("select * from x where ts in '1970-01-01' and ts <= '1970-01-01T00:00:00.010000000Z'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan(replaceTimestampSuffix1("""
                            PageFrame
                                Row forward scan
                                Interval forward scan on: x
                                  intervals: [("1970-01-01T00:00:00.000000Z","1970-01-01T00:00:00.010000Z")]
                            """, timestampType.getTypeName()))
                    .returns(expected);
            bindVariableService.clear();
            bindVariableService.setStr("date", "1970-01-01");
            bindVariableService.setStr("ts", "1970-01-01T00:00:00.010000Z");
            bindVariableService.setStr("ts1", "1970-01-01T00:00:00.010000000Z");
            assertQuery("select * from x where ts in :date and ts <= :ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);
            assertQuery("select * from x where ts in :date and ts <= :ts1")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);

            expected = "a\tts\n";
            assertQuery("select * from x where ts > '1970-01-01T00:00:00.010000Z' and ts in '1970-01-01T00:00:01' and ts in '1970-01-01T00:00:02'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("Empty table\n")
                    .returns(expected);
            assertQuery("select * from x where ts > '1970-01-01T00:00:00.010000000Z' and ts in '1970-01-01T00:00:01' and ts in '1970-01-01T00:00:02'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("Empty table\n")
                    .returns(expected);
            bindVariableService.clear();
            bindVariableService.setStr("ts", "1970-01-01T00:00:00.010000Z");
            bindVariableService.setStr("ts1", "1970-01-01T00:00:00.010000000Z");
            assertQuery("select * from x where ts > :ts and ts in '1970-01-01T00:00:01' and ts in '1970-01-01T00:00:02'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);
            assertQuery("select * from x where ts > :ts1 and ts in '1970-01-01T00:00:01' and ts in '1970-01-01T00:00:02'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);
        });
    }

    @Test
    public void testGreaterTimestampCursor() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (" +
                            "  select" +
                            "    rnd_double(0)*100 a," +
                            "    timestamp_sequence(0, 10000)::" + timestampType.getTypeName() + " ts" +
                            "  from long_sequence(3)" +
                            ") timestamp(ts) partition by day"
            );

            assertQuery("select * from x where ts > (select min(ts) from x)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"1970-01-01T00:00:00.000001Z\",\"MAX\")]\n" :
                                    "      intervals: [(\"1970-01-01T00:00:00.000000001Z\",\"MAX\")]\n"))
                    .returns(replaceTimestampSuffix1("""
                            a\tts
                            8.486964232560668\t1970-01-01T00:00:00.010000Z
                            8.43832076262595\t1970-01-01T00:00:00.020000Z
                            """, timestampType.getTypeName()));

            assertQuery("select * from x where ts >= (select min(ts) from x)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan(replaceTimestampSuffix1("""
                            PageFrame
                                Row forward scan
                                Interval forward scan on: x
                                  intervals: [("1970-01-01T00:00:00.000000Z","MAX")]
                            """, timestampType.getTypeName()))
                    .returns(replaceTimestampSuffix1("""
                            a\tts
                            80.43224099968394\t1970-01-01T00:00:00.000000Z
                            8.486964232560668\t1970-01-01T00:00:00.010000Z
                            8.43832076262595\t1970-01-01T00:00:00.020000Z
                            """, timestampType.getTypeName()));
        });
    }

    @Test
    public void testInConstantInterval() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (" +
                            "  select" +
                            "    rnd_double(0)*100 a," +
                            "    timestamp_sequence(0, 1000000)::" + timestampType.getTypeName() + " ts" +
                            "  from long_sequence(4)" +
                            ") timestamp(ts) partition by day"
            );
            assertQuery("select * from x where ts in interval('1970-01-01T00:00:01', '1970-01-01T00:00:03')")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"1970-01-01T00:00:01.000000Z\",\"1970-01-01T00:00:03.000000Z\")]\n" :
                                    "      intervals: [(\"1970-01-01T00:00:01.000000000Z\",\"1970-01-01T00:00:03.000000000Z\")]\n"))
                    .returns(replaceTimestampSuffix("""
                            a\tts
                            8.486964232560668\t1970-01-01T00:00:01.000000Z
                            8.43832076262595\t1970-01-01T00:00:02.000000Z
                            65.08594025855301\t1970-01-01T00:00:03.000000Z
                            """, timestampType.getTypeName()));
            assertQuery("select * from x where ts not in interval('1970-01-01T00:00:01', '1970-01-01T00:00:03')")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"MIN\",\"1970-01-01T00:00:00.999999Z\"),(\"1970-01-01T00:00:03.000001Z\",\"MAX\")]\n" :
                                    "      intervals: [(\"MIN\",\"1970-01-01T00:00:00.999999999Z\"),(\"1970-01-01T00:00:03.000000001Z\",\"MAX\")]\n"))
                    .returns(replaceTimestampSuffix("""
                            a\tts
                            80.43224099968394\t1970-01-01T00:00:00.000000Z
                            """, timestampType.getTypeName()));
        });
    }

    @Test
    public void testInDateListCoveredElementOverMaxIntervals() throws Exception {
        // The later element of each list holds more intervals than the cap (1,024), and they never
        // touch, but they all lie in the first element's day. With no time zone, expanding day
        // filter or tick calendar of its own, the element adds them to the merge range of the list,
        // whose merges union them into that day before they count. One row every 7 minutes from
        // 2023-12-31 to 2024-01-02, 206 of them on 2024-01-01.
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE t (tm #TIMESTAMP, d DATE, ts #TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", timestampType.getTypeName());
            execute("""
                    INSERT INTO t
                    SELECT '2023-12-31'::TIMESTAMP + (x - 1) * 420_000_000L,
                           ('2023-12-31'::TIMESTAMP + (x - 1) * 420_000_000L)::DATE,
                           '2023-12-31'::TIMESTAMP + (x - 1) * 420_000_000L
                    FROM long_sequence(618)
                    """);
            final String day = "year(ts) = 2024 AND month(ts) = 1 AND day(ts) = 1";
            final String dayRows = "SELECT ts FROM t WHERE " + day;
            // 1,200 slots of 10 ms, as numeric brackets and as a time list, 6,000 slots of 1 ms in one
            // time list value, and 1,200 slots in nested time lists
            final String n1 = "[2024-01-01,2024-01-01T[09..10]:00:[00..59].[0..9]0]";
            final String n2 = "[2024-01-01,2024-01-01T[09:00,10:00]:[00..59].[0..9]0]";
            final String n3 = "[2024-01-01,2024-01-01T[09:00]:[00..59].[0..9][0..9]0]";
            for (String tick : new String[]{n1, n2, n3, "[2024-01-01,2024-01-01T[09:,10:][00:,01:][00..59].[0..9]0]"}) {
                assertInOnEveryColumn(tick, day, 206);
                bindVariableService.clear();
                bindVariableService.setStr("p", tick);
                for (String column : new String[]{"ts", "tm"}) {
                    assertSqlCursors(dayRows, "SELECT ts FROM t WHERE " + column + " IN :p");
                }
            }
            assertSqlCursors("SELECT ts FROM t WHERE NOT (" + day + ")", "SELECT ts FROM t WHERE ts NOT IN '" + n1 + "'");
            // the interval scan reads the one merged day
            assertQuery("SELECT ts FROM t WHERE ts IN '" + n1 + "'")
                    .noLeakCheck()
                    .assertsPlanContaining(timestampType == TestTimestampType.NANO
                            ? "intervals: [(\"2024-01-01T00:00:00.000000000Z\",\"2024-01-01T23:59:59.999999999Z\")]"
                            : "intervals: [(\"2024-01-01T00:00:00.000000Z\",\"2024-01-01T23:59:59.999999Z\")]");
            // a tick string in every row, which the filter parses per row
            executeWithRewriteTimestamp("CREATE TABLE u (ts #TIMESTAMP, s STRING, v VARCHAR) TIMESTAMP(ts) PARTITION BY DAY", timestampType.getTypeName());
            execute("INSERT INTO u SELECT ts, '" + n1 + "', '" + n1 + "' FROM t");
            for (String column : new String[]{"s", "v"}) {
                assertSqlCursors("SELECT ts FROM u WHERE " + day, "SELECT ts FROM u WHERE ts IN " + column);
            }

            // A later element with a time zone keeps a merge range of its own, because the zone
            // converts only its intervals, so its 1,200 slots fail.
            final String overCapError = "Bracket expansion produces too many intervals (max 1024)";
            final String n2z = n2 + "@+05:00";
            for (String column : new String[]{"ts", "tm"}) {
                final String sql = "SELECT ts FROM t WHERE " + column + " IN '" + n2z + "'";
                assertExceptionNoLeakCheck(sql, sql.indexOf("'["), overCapError);
            }
            bindVariableService.clear();
            bindVariableService.setStr("p", n2z);
            assertExceptionNoLeakCheck("SELECT ts FROM t WHERE tm IN :p", 0, overCapError);
            // the interval scan holds no rows for a bind value it cannot parse
            assertQuery("SELECT ts FROM t WHERE ts IN :p").noLeakCheck().timestamp("ts").returns("ts\n");
            // one factory, run again with new bind values
            try (RecordCursorFactory factory = select("SELECT count() FROM t WHERE tm IN :p")) {
                bindVariableService.setStr("p", n1);
                assertFactory(factory).withContext(sqlExecutionContext).expectSize().noRandomAccess().returns("count\n206\n");
                bindVariableService.setStr("p", n2z);
                try (RecordCursor ignore = factory.getCursor(sqlExecutionContext)) {
                    Assert.fail("Expected the cap error for " + n2z);
                } catch (SqlException e) {
                    Assert.assertEquals(0, e.getPosition());
                    TestUtils.assertContains(e.getFlyweightMessage(), overCapError);
                }
                bindVariableService.setStr("p", n3);
                assertFactory(factory).withContext(sqlExecutionContext).expectSize().noRandomAccess().returns("count\n206\n");
            }
        });
    }

    @Test
    public void testInDateListDayFilterAfterLargeElement() throws Exception {
        // The first element expands to 336 intervals, above the incremental merge threshold (256),
        // and the second element's day filter must judge only the Mondays of March and April 2023.
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "CREATE TABLE x (k INT, tm #TIMESTAMP, ts #TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY",
                    timestampType.getTypeName()
            );
            execute("""
                    INSERT INTO x VALUES
                    (1, '2000-06-01T12:00:00.000000Z', '2000-06-01T12:00:00.000000Z'),
                    (2, '2023-03-06T12:00:00.000000Z', '2023-03-06T12:00:00.000000Z'),
                    (3, '2023-03-07T12:00:00.000000Z', '2023-03-07T12:00:00.000000Z'),
                    (4, '2023-06-01T09:00:30.000000Z', '2023-06-01T09:00:30.000000Z'),
                    (5, '2023-11-15T12:00:00.000000Z', '2023-11-15T12:00:00.000000Z')
                    """);
            // the Monday 2023-03-06 and the 09:00 minute of 2023-06-01
            final String expected = """
                    k
                    2
                    4
                    """;
            for (String column : new String[]{"ts", "tm"}) {
                assertQuery("SELECT k FROM x WHERE " + column + " IN '[2023-[01..12]-[01..28]T09:00,2023-[03..04]#mon]'")
                        .noLeakCheck()
                        .returns(expected);
                assertQuery("SELECT k FROM x WHERE " + column + " IN '[2023-[03..04]#mon,2023-[01..12]-[01..28]T09:00]'")
                        .noLeakCheck()
                        .returns(expected);
                assertQuery("SELECT k FROM x WHERE " + column + " IN '[2023-[01..12]-[01..28]T09:00,2023-03#mon,2023-04#mon]'")
                        .noLeakCheck()
                        .returns(expected);
                assertQuery("SELECT k FROM x WHERE " + column + " IN '[2023-[01..12]-[01..28]T09:00,2023-03-[01..31]#mon]'")
                        .noLeakCheck()
                        .returns(expected);
                // a day filter on the whole list
                assertQuery("SELECT k FROM x WHERE " + column + " IN '[2023-[01..12]-[01..28]T09:00,2023-[03..04]]#mon'")
                        .noLeakCheck()
                        .returns("k\n2\n");
            }
        });
    }

    @Test
    public void testInDateListDayFilterOverMaxIntervalsFailsEarly() throws Exception {
        // Each 100-year element filters to about 5,218 Mondays, so too-many-intervals wins over the
        // unparsable third element.
        assertMemoryLeak(() -> {
            createDayFilterTable();
            final String sql = "SELECT k FROM x WHERE ts IN '[20[00..99],20[00..99],2024-13-45]#mon'";
            assertExceptionNoLeakCheck(sql, sql.indexOf("'["), "Bracket expansion produces too many intervals (max 1024)");
        });
    }

    @Test
    public void testInDateListDayFilterRepeatedElements() throws Exception {
        // Each element holds 240 days, so together they exceed the list's incremental merge
        // threshold (256) when counted unfiltered. Each element filters its own days before any
        // union, so only the Mondays 2024-01-08 (row 3) and 2024-09-02 (row 6) match.
        assertMemoryLeak(() -> {
            createDayFilterTable();
            final String dayFilterPredicate = "ts IN '[2024-[01..12]-[01..20],2024-[01..12]-[01..20]]#mon'";
            final String mondays = """
                    k
                    3
                    6
                    """;
            assertQuery("SELECT k FROM x WHERE " + dayFilterPredicate).noLeakCheck().returns(mondays);
            assertInBothOrders(dayFilterPredicate, "ts > dateadd('y', -30, now())", mondays);
        });
    }

    @Test
    public void testInDateListMonthDayFilterAfterLargeFilteredElement() throws Exception {
        // The first element filters 2,016 bracket values to 288 Mondays, above the incremental merge
        // threshold (256). The month elements after it expand their own day filter to single days,
        // which must not reach into the Mondays.
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "CREATE TABLE x (k INT, tm #TIMESTAMP, ts #TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY",
                    timestampType.getTypeName()
            );
            // 1 and 4 are Mondays, 2 a Saturday, 3 and 5 Tuesdays, 6 a Monday and 7 a Sunday
            execute("""
                    INSERT INTO x VALUES
                    (6, '2019-01-07T12:00:00.000000Z', '2019-01-07T12:00:00.000000Z'),
                    (1, '2024-09-02T12:00:00.000000Z', '2024-09-02T12:00:00.000000Z'),
                    (2, '2024-09-07T12:00:00.000000Z', '2024-09-07T12:00:00.000000Z'),
                    (7, '2024-09-29T12:00:00.000000Z', '2024-09-29T12:00:00.000000Z'),
                    (3, '2024-10-15T12:00:00.000000Z', '2024-10-15T12:00:00.000000Z'),
                    (4, '2024-11-04T12:00:00.000000Z', '2024-11-04T12:00:00.000000Z'),
                    (5, '2024-11-05T12:00:00.000000Z', '2024-11-05T12:00:00.000000Z')
                    """);
            final String mondaysAndWorkdays = """
                    k
                    6
                    1
                    3
                    4
                    """;
            for (String column : new String[]{"ts", "tm"}) {
                final String prefix = "SELECT k FROM x WHERE " + column + " IN ";
                assertQuery(prefix + "'[[2019..2024]-[01..12]-[01..28]#mon,2024-[09..10]#workday]'")
                        .noLeakCheck()
                        .returns(mondaysAndWorkdays);
                assertQuery(prefix + "'[[2019..2024]-[01..12]-[01..28]#mon,2024-09#workday,2024-10#workday]'")
                        .noLeakCheck()
                        .returns(mondaysAndWorkdays);
                assertQuery(prefix + "'[[2019..2024]-[01..12]-[01..28]#mon,2024#workday]'")
                        .noLeakCheck()
                        .returns(mondaysAndWorkdays + "5\n");
                assertQuery(prefix + "'[[2019..2024]-[01..12]-[01..28],2024-[09..10]]#mon'")
                        .noLeakCheck()
                        .returns("""
                                k
                                6
                                1
                                4
                                """);
            }
        });
    }

    @Test
    public void testInDateListOverMaxIntervalsFailsEarly() throws Exception {
        // The one-minute values of the second element lie an hour apart and never touch. Its merges
        // union them with the first element's day, and the list's 1,025th interval is the 16th
        // value of February 16, so too-many-intervals wins over month 13.
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "CREATE TABLE x (k INT, tm #TIMESTAMP, ts #TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY",
                    timestampType.getTypeName()
            );
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:30.000000Z', '2024-01-01T00:00:30.000000Z'),
                    (2, '2024-01-21T00:00:30.000000Z', '2024-01-21T00:00:30.000000Z')
                    """);
            for (String column : new String[]{"ts", "tm"}) {
                assertOverMaxIntervalsError("SELECT k FROM x WHERE " + column + " IN '[2024-01-01,2024-[01..13]-[01..28]T[00..23]:00]'");
                // The failed compilation leaves the next query intact. The second element repeats
                // days 1-11 of January, and its merges union its values into the first element's
                // 960 before they count them against the cap.
                assertQuery("SELECT k FROM x WHERE " + column + " IN '[2024-[01..02]-[01..20]T[00..23]:00,2024-01-[01..11]T[00..23]:00]'")
                        .noLeakCheck()
                        .returns("k\n1\n");
            }
        });
    }

    @Test
    public void testInDateListTimeZoneAboveMergeThreshold() throws Exception {
        // One row per hour from 2023-12-31T00:00Z to 2025-01-08T23:00Z, so every local day of 2024
        // at +05:00 holds 24 rows. January's 31 days and the 308 days 1 to 28 of February to
        // December make 339 bracket values, above the incremental merge threshold (256). A day
        // that keeps UTC instead of its local time keeps the count, so each case also compares
        // the rows with a reference that selects them by their local time l at +05:00, which
        // Asia/Karachi keeps all year.
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE t (tm #TIMESTAMP, ts #TIMESTAMP) TIMESTAMP(ts) PARTITION BY MONTH", timestampType.getTypeName());
            execute("""
                    INSERT INTO t
                    SELECT '2023-12-31'::TIMESTAMP + (x - 1) * 3_600_000_000L, '2023-12-31'::TIMESTAMP + (x - 1) * 3_600_000_000L
                    FROM long_sequence(9_000)
                    """);
            final String days = "year(l) = 2024 AND (month(l) = 1 OR day(l) <= 28)";
            final String januaryAndMondays = "year(l) = 2024 AND (month(l) = 1 OR (day(l) <= 28 AND day_of_week(l) = 1))";
            final String[][] cases = {
                    {"'[2024-01-[01..31],2024-[02..12]-[01..28]]@+05:00'", String.valueOf((31 + 11 * 28) * 24), days},
                    {"'[2024-01-[01..31]@+05:00,2024-[02..12]-[01..28]@+05:00]'", String.valueOf((31 + 11 * 28) * 24), days},
                    {"'[2024-01-[01..31]@Asia/Karachi,2024-[02..12]-[01..28]@Asia/Karachi]'", String.valueOf((31 + 11 * 28) * 24), days},
                    {
                            "'[2024-01-01@+05:00,2024-[02..12]-[01..28]@+05:00]'",
                            String.valueOf((1 + 11 * 28) * 24),
                            "year(l) = 2024 AND ((month(l) = 1 AND day(l) = 1) OR (month(l) >= 2 AND day(l) <= 28))"
                    },
                    {
                            "'[2024-01-[01..31]@+05:00,2024-[02..05]-[01..28]@+05:00]'",
                            String.valueOf((31 + 4 * 28) * 24),
                            "year(l) = 2024 AND (month(l) = 1 OR (month(l) <= 5 AND day(l) <= 28))"
                    },
                    {"'2024-[02..12]-[01..28]@+05:00'", String.valueOf(11 * 28 * 24), "year(l) = 2024 AND month(l) >= 2 AND day(l) <= 28"},
                    // the Mondays among them: 5 in January and 4 in each of the 11 later months
                    {"'[2024-01-[01..31],2024-[02..12]-[01..28]]@+05:00#mon'", String.valueOf((5 + 11 * 4) * 24), days + " AND day_of_week(l) = 1"},
                    // January and the Mondays of the later months
                    {"'[2024-01-[01..31]@+05:00,2024-[02..12]-[01..28]@+05:00#mon]'", String.valueOf((31 + 11 * 4) * 24), januaryAndMondays},
                    {"'[2024-01-[01..31],2024-[02..12]-[01..28]#mon]@+05:00'", String.valueOf((31 + 11 * 4) * 24), januaryAndMondays},
                    // 09:00 to 10:00 local on each day
                    {"'[2024-01-[01..31],2024-[02..12]-[01..28]]T09:00@+05:00;1h'", String.valueOf(31 + 11 * 28), days + " AND hour(l) = 9"},
                    // Every hour of days 1-20 of January and February, one minute each. The second
                    // element repeats days 1-11 of January, and its merges count only its own
                    // intervals against the cap (1,024), not the first element's 960 as well.
                    {
                            "'[2024-[01..02]-[01..20]T[00..23]:00,2024-01-[01..11]T[00..23]:00]@+05:00'",
                            String.valueOf(2 * 20 * 24),
                            "year(l) = 2024 AND month(l) <= 2 AND day(l) <= 20"
                    },
            };
            for (String[] c : cases) {
                final String reference = "SELECT ts FROM (SELECT ts, dateadd('h', 5, ts) l FROM t) WHERE " + c[2];
                assertQuery("SELECT count() FROM (" + reference + ")")
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns("count\n" + c[1] + "\n");
                for (String column : new String[]{"ts", "tm"}) {
                    assertQuery("SELECT count() FROM t WHERE " + column + " IN " + c[0])
                            .noLeakCheck()
                            .expectSize()
                            .noRandomAccess()
                            .returns("count\n" + c[1] + "\n");
                    assertSqlCursors(reference, "SELECT ts FROM t WHERE " + column + " IN " + c[0]);
                    bindVariableService.clear();
                    bindVariableService.setStr("p", c[0].substring(1, c[0].length() - 1));
                    assertSqlCursors(reference, "SELECT ts FROM t WHERE " + column + " IN :p");
                }
            }
        });
    }

    @Test
    public void testInDateVariableRangeElementDayFilter() throws Exception {
        // the six days from $today, the Monday 2024-01-08, hold one Monday (row 3)
        assertMemoryLeak(() -> {
            createDayFilterTable();
            final String monday = """
                    k
                    3
                    """;
            bindVariableService.clear();
            bindVariableService.setStr("p", "[$today..$today+5d#mon]");
            assertQuery("SELECT k FROM x WHERE ts IN :p").noLeakCheck().returns(monday);
            assertQuery("SELECT k FROM x WHERE ts NOT IN :p").noLeakCheck().returns("""
                    k
                    1
                    2
                    4
                    5
                    6
                    """);
            // the element filter overrides the global one
            bindVariableService.setStr("p", "[$today..$today+5d#mon]#tue");
            assertQuery("SELECT k FROM x WHERE ts IN :p").noLeakCheck().returns(monday);
            bindVariableService.setStr("p", "[$today..$today+5d]#mon");
            assertQuery("SELECT k FROM x WHERE ts IN :p").noLeakCheck().returns(monday);
        });
    }

    @Test
    public void testInDayFilterBracketExpansionAboveMergeThreshold() throws Exception {
        // 336 bracket values reach the incremental merge threshold (256) of the bracket expansion.
        // In 2024, days 1-28 of each month hold exactly 4 Mondays, and the table holds the Monday
        // rows 3 (2024-01-08) and 6 (2024-09-02); row 1 is a Friday, 2 a Saturday, 4 a Tuesday and
        // 5 a Sunday.
        assertMemoryLeak(() -> {
            createDayFilterTable();
            bindVariableService.clear();
            bindVariableService.setStr("i", "2024-01-07#workday");
            final String mondays = """
                    k
                    3
                    6
                    """;
            final String runtimePredicate = "ts > dateadd('y', -10, now())";
            assertInBothOrders("ts IN '2024-[01..12]-[01..28]#mon'", runtimePredicate, mondays);
            assertInBothOrders("ts IN '2024-[01..12]-[01..28]#mon'", "ts NOT IN :i", mondays);
            assertInBothOrders(
                    "ts NOT IN '2024-[01..12]-[01..28]#mon'",
                    runtimePredicate,
                    """
                            k
                            1
                            2
                            4
                            5
                            """
            );
            assertInBothOrders(
                    "ts IN '2024-[01..12]-[01..28]#workday'",
                    runtimePredicate,
                    """
                            k
                            1
                            3
                            4
                            6
                            """
            );
            // the filter judges a Monday at +05:00 by its local day
            assertInBothOrders("ts IN '2024-[01..12]-[01..28]@+05:00#mon'", runtimePredicate, mondays);
            // a single-element date list and a per-element filter
            assertInBothOrders("ts IN '[2024-[01..12]-[01..28]]#mon'", runtimePredicate, mondays);
            assertInBothOrders(
                    "ts IN '[2024-01-05,2024-[01..12]-[01..28]#mon]'",
                    runtimePredicate,
                    """
                            k
                            1
                            3
                            6
                            """
            );
            // Monday 18:00 for 6 hours joins Tuesday 00:00 for 6 hours, but the Tuesday interval
            // fails the filter, so Tuesday 04:30 (row 4) must not match
            assertInBothOrders("ts IN '2024-[01..12]-[01..28]T[00:00,18:00]#mon;6h'", runtimePredicate, "k\n");
            assertQuery("SELECT k FROM x WHERE ts IN '2024-[01..12]-[01..28]#mon'").noLeakCheck().returns(mondays);
            // $today is the Monday 2024-01-08
            assertQuery("SELECT k FROM x WHERE ts IN '$today' OR ts IN '2024-[01..12]-[01..28]#mon'").noLeakCheck().returns(mondays);
            assertQuery("SELECT k FROM x WHERE ts IN '2024-[01..12]-[01..28]#mon' OR ts IN '$today'").noLeakCheck().returns(mondays);
        });
    }

    @Test
    public void testInDayFilterBracketExpansionBelowMergeThreshold() throws Exception {
        // control: 252 bracket values stay below the incremental merge threshold
        assertMemoryLeak(() -> {
            createDayFilterTable();
            final String mondays = """
                    k
                    3
                    6
                    """;
            assertInBothOrders("ts IN '2024-[01..09]-[01..28]#mon'", "ts > dateadd('y', -10, now())", mondays);
            assertQuery("SELECT k FROM x WHERE ts IN '$today' OR ts IN '2024-[01..09]-[01..28]#mon'").noLeakCheck().returns(mondays);
            assertInBothOrders("ts IN '2024-[01..04]-[01..28]T[00:00,18:00]#mon;6h'", "ts > dateadd('y', -10, now())", "k\n");
        });
    }

    @Test
    public void testInDayFilterBracketExpansionMaxIntervals() throws Exception {
        // Days 1-28 of a month hold exactly 4 Mondays and 20 workdays. 4 years of Mondays at 12:00
        // make 192 intervals; 5 years of workdays at 09:00 make 1,200, above the default
        // cairo.sql.interval.max.intervals.after.merge of 1,024.
        assertMemoryLeak(() -> {
            createDayFilterTable();
            final String runtimePredicate = "ts > dateadd('y', -30, now())";
            assertInBothOrders(
                    "ts IN '[2021..2024]-[01..12]-[01..28]T12:00#mon;1h'",
                    runtimePredicate,
                    """
                            k
                            3
                            6
                            """
            );
            final String overCap = "ts IN '[2001..2005]-[01..12]-[01..28]T09:00#workday;1h'";
            assertOverMaxIntervalsError("SELECT k FROM x WHERE " + runtimePredicate + " AND " + overCap);
            assertOverMaxIntervalsError("SELECT k FROM x WHERE " + overCap + " AND " + runtimePredicate);
        });
    }

    @Test
    public void testInDayFilterBracketExpansionOverMaxIntervals() throws Exception {
        // 22 years x 12 months x 4 Mondays among days 1-28 make 1,056 intervals, above the cap of
        // 1,024; one year less makes 1,008
        assertMemoryLeak(() -> {
            createDayFilterTable();
            final String overCap = "'[2003..2024]-[01..12]-[01..28]#mon'";
            final String atCap = "'[2004..2024]-[01..12]-[01..28]#mon'";
            final String runtimePredicate = "ts > dateadd('y', -30, now())";
            assertOverMaxIntervalsError("SELECT k FROM x WHERE ts IN " + overCap);
            assertOverMaxIntervalsError("SELECT k FROM x WHERE ts IN " + overCap + " AND " + runtimePredicate);
            assertOverMaxIntervalsError("SELECT k FROM x WHERE " + runtimePredicate + " AND ts IN " + overCap);
            assertOverMaxIntervalsError("SELECT k FROM x WHERE ts NOT IN " + overCap + " AND " + runtimePredicate);
            assertOverMaxIntervalsError("SELECT k FROM x WHERE " + runtimePredicate + " AND ts NOT IN " + overCap);
            assertOverMaxIntervalsError("SELECT k FROM x WHERE ts IN '$today' OR ts IN " + overCap);
            assertOverMaxIntervalsError("SELECT k FROM x WHERE ts IN " + overCap + " OR ts IN '$today'");
            // a date variable compiles the list, and the static element checks its own count
            assertOverMaxIntervalsError("SELECT k FROM x WHERE ts IN '[$today,20[03..24]-[01..12]-[01..28]]#mon'");
            // the monotonic chain falls back to the row filter, which parses the string again
            assertOverMaxIntervalsError("SELECT k FROM x WHERE dateadd('h', 1, ts) IN " + overCap);
            // a bind variable on the designated timestamp swallows the error: IN keeps no row, and
            // NOT IN keeps every row
            bindVariableService.clear();
            bindVariableService.setStr("p", "[2003..2024]-[01..12]-[01..28]#mon");
            assertQuery("SELECT k FROM x WHERE ts IN :p").noLeakCheck().returns("k\n");
            assertQuery("SELECT k FROM x WHERE ts NOT IN :p").noLeakCheck().returns("""
                    k
                    1
                    2
                    3
                    4
                    5
                    6
                    """);
            final String mondays = """
                    k
                    3
                    6
                    """;
            assertQuery("SELECT k FROM x WHERE ts IN " + atCap).noLeakCheck().returns(mondays);
            assertInBothOrders("ts IN " + atCap, runtimePredicate, mondays);
            bindVariableService.setStr("p", "[2004..2024]-[01..12]-[01..28]#mon");
            assertQuery("SELECT k FROM x WHERE ts IN :p").noLeakCheck().returns(mondays);
        });
    }

    @Test
    public void testInDayFilterBracketExpansionWithDateVariableAboveMergeThreshold() throws Exception {
        assertMemoryLeak(() -> {
            createDayFilterTable();
            final String mondays = """
                    k
                    3
                    6
                    """;
            // a date variable compiles the list; its static element still expands 336 values
            assertQuery("SELECT k FROM x WHERE ts IN '[$today,2024-[01..12]-[01..28]]#mon'").noLeakCheck().returns(mondays);
            // a bind variable string resolves when the cursor opens
            bindVariableService.clear();
            bindVariableService.setStr("p", "2024-[01..12]-[01..28]#mon");
            assertQuery("SELECT k FROM x WHERE ts IN :p").noLeakCheck().returns(mondays);
        });
    }

    @Test
    public void testInDayFilterBracketListWithRuntimeInterval() throws Exception {
        // The predicate written last reaches the interval model builder first. A day-filtered
        // bracket list that follows a run-time interval must intersect with it as one union of
        // its values.
        assertMemoryLeak(() -> {
            createDayFilterTable();
            bindVariableService.clear();
            bindVariableService.setStr("i", "2024-01-07#workday");

            final String query = "SELECT k FROM x WHERE ts IN '2024-01-[05,08]#workday' AND ts NOT IN :i";
            assertQuery(query)
                    .noLeakCheck()
                    .returns("""
                            k
                            1
                            3
                            """);
            // the same query with a bind value that subtracts the Monday
            bindVariableService.setStr("i", "2024-01-08");
            assertQuery(query)
                    .noLeakCheck()
                    .returns("""
                            k
                            1
                            """);
        });
    }

    @Test
    public void testInDayFilterImpreciseBracketExpansionWithDateVariable() throws Exception {
        // A date variable compiles the list. Its month and week elements have no date precision, so
        // the compiler expands them into their matching days: filtering each month by its first day
        // would keep January whole, as 2024-01-01 is a Monday. One row per hour from Monday
        // 2023-12-25 to Sunday 2024-04-07; the references select the rows by their calendar fields.
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE t (tm #TIMESTAMP, d DATE, ts #TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", timestampType.getTypeName());
            execute("""
                    INSERT INTO t
                    SELECT '2023-12-25'::TIMESTAMP + (x - 1) * 3_600_000_000L,
                           ('2023-12-25'::TIMESTAMP + (x - 1) * 3_600_000_000L)::DATE,
                           '2023-12-25'::TIMESTAMP + (x - 1) * 3_600_000_000L
                    FROM long_sequence(2_520)
                    """);
            final String q1Mondays = "year(ts) = 2024 AND month(ts) <= 3 AND day_of_week(ts) = 1";
            // $today, the Monday 2024-01-08, is one of the static days
            setCurrentMicros(Micros.toMicros(2024, 1, 8, 1, 0));
            assertInOnEveryColumn("[$today,2024-[01..03]]#mon", q1Mondays, 13 * 24);
            // ISO week 1 of 2024 starts on Monday 2024-01-01, and week 13 ends on Sunday 2024-03-31
            assertInOnEveryColumn("[$today,2024-W[01..13]]#mon", q1Mondays, 13 * 24);
            // 23 + 21 + 21 weekdays, the leap day 2024-02-29 among them
            assertInOnEveryColumn("[$today,2024-[01..03]]#workday", "year(ts) = 2024 AND month(ts) <= 3 AND day_of_week(ts) <= 5", (23 + 21 + 21) * 24);
            final String janFebMondays = "[$today,2024-[01..02]]#mon";
            assertInOnEveryColumn(janFebMondays, "year(ts) = 2024 AND month(ts) <= 2 AND day_of_week(ts) = 1", 9 * 24);
            // The 9 Mondays and $today make 10 range checks, the default limit of the JIT for an IN
            // list, so the JIT filters tm and d. The 14 range checks of the Q1 lists exceed it, and
            // the Java filter runs them.
            final int callerJitMode = sqlExecutionContext.getJitMode();
            try {
                sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
                for (String column : new String[]{"tm", "d"}) {
                    assertQuery("SELECT ts FROM t WHERE " + column + " IN '" + janFebMondays + "'")
                            .noLeakCheck()
                            .assertsPlanContaining("Async JIT Filter");
                }
            } finally {
                sqlExecutionContext.setJitMode(callerJitMode);
            }
            assertQuery("SELECT ts FROM t WHERE ts IN '" + janFebMondays + "'")
                    .noLeakCheck()
                    .assertsPlanContaining("Interval forward scan on: t");
            // $today, the Monday 2024-04-01, adds a day after the static ones
            setCurrentMicros(Micros.toMicros(2024, 4, 1, 3, 0));
            assertInOnEveryColumn(
                    "[$today,2024-[01..03]]#mon",
                    "year(ts) = 2024 AND ((month(ts) <= 3 AND day_of_week(ts) = 1) OR (month(ts) = 4 AND day(ts) = 1))",
                    14 * 24
            );
        });
    }

    @Test
    public void testInDayFilterWithNoIntervalsAndRuntimeInterval() throws Exception {
        // 2024-01-06 is a Saturday, so '2024-01-06#workday' holds no interval and an AND with it
        // holds no row, whichever run-time interval it meets and in either order.
        assertMemoryLeak(() -> {
            createDayFilterTable();
            bindVariableService.clear();
            bindVariableService.setStr("i", "2024-01-07#workday");
            bindVariableService.setStr("d6", "2024-01-06");
            bindVariableService.setStr("d8", "2024-01-08");

            assertInBothOrders("ts IN '2024-01-06#workday'", "ts IN :d6", "k\n");
            assertInBothOrders("ts IN '2024-01-06#workday'", "ts < now()", "k\n");
            assertInBothOrders("ts IN '2024-01-06#workday'", "ts > dateadd('y', -10, now())", "k\n");
            assertInBothOrders("ts IN '2024-01-06#workday'", "ts IN '$today'", "k\n");
            assertInBothOrders("ts IN '2024-01-06#workday'", "ts NOT IN :d8", "k\n");
            assertInBothOrders("ts IN '2024-01-06#workday'", "ts NOT IN :i", "k\n");
            assertInBothOrders("ts IN '2024-01-06#workday'", "ts NOT IN '$today#weekend'", "k\n");
            assertInBothOrders("ts IN '2024-01-[06,07]#workday'", "ts > dateadd('y', -10, now())", "k\n");
        });
    }

    @Test
    public void testInDayFilterWithNoIntervalsAndRuntimeNotIn() throws Exception {
        // 2024-01-06 is a Saturday, so '2024-01-06#workday' holds no interval. The run-time NOT IN
        // holds no interval either: 2024-01-07 is a Sunday, and $today is the Monday 2024-01-08.
        // Subtracting no interval keeps every row, but the AND with an empty IN keeps none.
        assertMemoryLeak(() -> {
            createDayFilterTable();
            bindVariableService.clear();
            bindVariableService.setStr("i", "2024-01-07#workday");

            // the empty IN holds no row in the static-first order
            assertQuery("SELECT k FROM x WHERE ts NOT IN :i AND ts IN '2024-01-06#workday'").noLeakCheck().returns("k\n");
            assertQuery("SELECT k FROM x WHERE ts NOT IN '$today#weekend' AND ts IN '2024-01-06#workday'").noLeakCheck().returns("k\n");

            // a day-filtered IN that holds an interval keeps its rows
            final String friday = """
                    k
                    1
                    """;
            assertQuery("SELECT k FROM x WHERE ts IN '2024-01-05#workday' AND ts NOT IN :i").noLeakCheck().returns(friday);
            assertQuery("SELECT k FROM x WHERE ts IN '2024-01-05#workday' AND ts NOT IN '$today#weekend'").noLeakCheck().returns(friday);

            // a day-filtered NOT IN combined with an empty run-time NOT IN
            assertQuery("SELECT k FROM x WHERE ts NOT IN '2024-01-06#weekend' AND ts NOT IN :i")
                    .noLeakCheck()
                    .returns("""
                            k
                            1
                            3
                            4
                            5
                            6
                            """);
            assertQuery("SELECT k FROM x WHERE ts NOT IN '2024-01-06#workday' AND ts NOT IN :i")
                    .noLeakCheck()
                    .returns("""
                            k
                            1
                            2
                            3
                            4
                            5
                            6
                            """);

            // the run-time NOT IN reaches the interval model builder first
            assertQuery("SELECT k FROM x WHERE ts IN '2024-01-06#workday' AND ts NOT IN :i").noLeakCheck().returns("k\n");
            assertQuery("SELECT k FROM x WHERE ts IN '2024-01-06#workday' AND ts NOT IN '$today#weekend'").noLeakCheck().returns("k\n");
        });
    }

    @Test
    public void testInDayFilterWithRuntimeIntervalIsOrderIndependent() throws Exception {
        // Each day-filtered expression holds an interval although its first day fails the filter,
        // so the rows it keeps must not depend on the order of the AND operands.
        assertMemoryLeak(() -> {
            createDayFilterTable();
            bindVariableService.clear();
            bindVariableService.setStr("i", "2024-01-07#workday");

            // 2024-09-01 is a Sunday; Monday 2024-09-02 is the only matching row
            assertInBothOrders("ts IN '2024-09#workday'", "ts NOT IN :i", "k\n6\n");
            assertInBothOrders("ts IN '2024-09#workday'", "ts > dateadd('y', -10, now())", "k\n6\n");
            // Saturday, Sunday and Monday repetitions; only Monday's hour matches
            assertInBothOrders("ts IN '2024-01-06T12:00#workday;1h;1d;3'", "ts NOT IN :i", "k\n3\n");
            assertInBothOrders("ts IN '2024-01-06T12:00#workday;1h;1d;3'", "ts > dateadd('y', -10, now())", "k\n3\n");
            // 2024-01-01 is a Monday, but the weekend inside January must not match
            assertInBothOrders("ts IN '2024-01#workday'", "ts NOT IN :i", "k\n1\n3\n4\n");
            assertInBothOrders("ts IN '2024-01#workday'", "ts > dateadd('y', -10, now())", "k\n1\n3\n4\n");
            // the Saturday bracket value holds no interval, the Monday one does
            assertInBothOrders("ts IN '2024-01-[06,08]#workday'", "ts NOT IN :i", "k\n3\n");
            assertInBothOrders("ts IN '2024-01-[06,08]#workday'", "ts > dateadd('y', -10, now())", "k\n3\n");
            // Monday 23:00 at -05:00 starts on Tuesday 04:00 UTC
            assertInBothOrders("ts IN '2024-01-08T23:00@-05:00#mon;2h'", "ts NOT IN :i", "k\n4\n");
            assertInBothOrders("ts IN '2024-01-08T23:00@-05:00#mon;2h'", "ts > dateadd('y', -10, now())", "k\n4\n");
            // the Saturday list element carries its own filter and holds no interval
            assertInBothOrders("ts IN '[2024-01-06#workday,2024-01-08]'", "ts NOT IN :i", "k\n3\n");
        });
    }

    @Test
    public void testInDayFilterWithRuntimeOr() throws Exception {
        // The OR of a run-time and a static interval reaches the interval model builder before
        // the day-filtered IN, so the day-filtered IN must intersect the whole OR.
        assertMemoryLeak(() -> {
            createDayFilterTable();
            // 2024-01-06 is a Saturday, so '2024-01-06#workday' holds no interval
            assertInBothOrders("ts IN '2024-01-06#workday'", "(ts IN '$today' OR ts IN '2024-01-06')", "k\n");
            // only the Monday of the Saturday-to-Monday range is a workday, and $today is that Monday
            assertInBothOrders("ts IN '2024-01-[06..08]#workday'", "(ts IN '$today' OR ts IN '2024-01-06')", "k\n3\n");
        });
    }

    @Test
    public void testInExchangeCalendarWithRuntimeInterval() throws Exception {
        // TestTickCalendarService holds the 2025 XNYS sessions: 14:30 to 21:00 UTC in January.
        final FactoryProvider previousFactoryProvider = factoryProvider;
        factoryProvider = new DefaultFactoryProvider() {
            @Override
            public @NotNull TickCalendarServiceFactory getTickCalendarServiceFactory() {
                return () -> TestTickCalendarService.INSTANCE;
            }
        };
        try {
            assertMemoryLeak(() -> {
                executeWithRewriteTimestamp("CREATE TABLE x (k INT, ts #TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", timestampType.getTypeName());
                execute("""
                        INSERT INTO x VALUES
                        (1, '2025-01-24T10:00:00.000000Z'),
                        (2, '2025-01-24T15:00:00.000000Z'),
                        (3, '2025-01-25T15:00:00.000000Z'),
                        (4, '2025-01-27T15:00:00.000000Z')
                        """);
                // 2025-01-24 is a Friday, 2025-01-25 a Saturday and 2025-01-27 a Monday
                setCurrentMicros(Micros.toMicros(2025, 1, 27, 1, 0));

                final String runtimePredicate = "ts > dateadd('y', -10, now())";
                final String fridaySession = """
                        k
                        2
                        """;
                final String fridayAndMondaySessions = """
                        k
                        2
                        4
                        """;
                assertInBothOrders("ts IN '2025-01-24#XNYS'", runtimePredicate, fridaySession);
                assertInBothOrders("ts IN '2025-01-24T15:00#XNYS;1h'", runtimePredicate, fridaySession);
                assertInBothOrders("ts IN '2025-01-[24..27]#XNYS'", runtimePredicate, fridayAndMondaySessions);
                assertInBothOrders(
                        "ts NOT IN '2025-01-24#XNYS'",
                        runtimePredicate,
                        """
                                k
                                1
                                3
                                4
                                """
                );
                // the calendar applies to the Friday list element only
                assertInBothOrders(
                        "ts IN '[2025-01-24#XNYS,2025-01-25]'",
                        runtimePredicate,
                        """
                                k
                                2
                                3
                                """
                );
                // the OR leaf written first reaches the interval model builder first
                assertQuery("SELECT k FROM x WHERE ts IN '2025-01-24#XNYS' OR ts IN '$today'").noLeakCheck().returns(fridayAndMondaySessions);
                assertQuery("SELECT k FROM x WHERE ts IN '$today' OR ts IN '2025-01-24#XNYS'").noLeakCheck().returns(fridayAndMondaySessions);
            });
        } finally {
            factoryProvider = previousFactoryProvider;
        }
    }

    @Test
    public void testInInterval() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (" +
                            "  select" +
                            "    rnd_double(0)*100 a," +
                            "    timestamp_sequence(0, 1000000)::" + timestampType.getTypeName() + " ts" +
                            "  from long_sequence(3)" +
                            ") timestamp(ts) partition by day"
            );

            String expected = replaceTimestampSuffix("""
                    a\tts
                    8.486964232560668\t1970-01-01T00:00:01.000000Z
                    """, timestampType.getTypeName());
            assertQuery("select * from x where ts in '1970-01-01T00:00:01'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"1970-01-01T00:00:01.000000Z\",\"1970-01-01T00:00:01.999999Z\")]\n" :
                                    "      intervals: [(\"1970-01-01T00:00:01.000000000Z\",\"1970-01-01T00:00:01.999999999Z\")]\n"))
                    .returns(expected);
            bindVariableService.clear();
            bindVariableService.setStr("i", "1970-01-01T00:00:01");
            assertQuery("select * from x where ts in :i")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);

            expected = replaceTimestampSuffix1("""
                    a\tts
                    80.43224099968394\t1970-01-01T00:00:00.000000Z
                    8.43832076262595\t1970-01-01T00:00:02.000000Z
                    """, timestampType.getTypeName());
            assertQuery("select * from x where ts not in '1970-01-01T00:00:01'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"MIN\",\"1970-01-01T00:00:00.999999Z\"),(\"1970-01-01T00:00:02.000000Z\",\"MAX\")]\n" :
                                    "      intervals: [(\"MIN\",\"1970-01-01T00:00:00.999999999Z\"),(\"1970-01-01T00:00:02.000000000Z\",\"MAX\")]\n"))
                    .returns(expected);
            bindVariableService.clear();
            bindVariableService.setStr("i", "1970-01-01T00:00:01");
            assertQuery("select * from x where ts not in :i")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);

            expected = replaceTimestampSuffix1("""
                    a\tts
                    80.43224099968394\t1970-01-01T00:00:00.000000Z
                    8.43832076262595\t1970-01-01T00:00:02.000000Z
                    """, timestampType.getTypeName());
            assertQuery("select * from x where ts in '1970-01-01' and ts not in '1970-01-01T00:00:01'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"1970-01-01T00:00:00.000000Z\",\"1970-01-01T00:00:00.999999Z\"),(\"1970-01-01T00:00:02.000000Z\",\"1970-01-01T23:59:59.999999Z\")]\n" :
                                    "      intervals: [(\"1970-01-01T00:00:00.000000000Z\",\"1970-01-01T00:00:00.999999999Z\"),(\"1970-01-01T00:00:02.000000000Z\",\"1970-01-01T23:59:59.999999999Z\")]\n"))
                    .returns(expected);
            bindVariableService.clear();
            bindVariableService.setStr("d", "1970-01-01");
            bindVariableService.setStr("i", "1970-01-01T00:00:01");
            assertQuery("select * from x where ts in :d and ts not in :i")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);

            expected = "a\tts\n";
            assertQuery("select * from x where ts in '1970-01-01T00:00:01' and ts in '1970-01-01T00:00:02' and ts in '1970-01-01T00:00:03'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("Empty table\n")
                    .returns(expected);
            bindVariableService.clear();
            bindVariableService.setStr("i", "1970-01-01T00:00:01");
            assertQuery("select * from x where ts in :i and ts in '1970-01-01T00:00:02' and ts in '1970-01-01T00:00:03'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);

            expected = "a\tts\n";
            assertQuery("select * from x where ts not in '1970-01-01T00:00:01' and ts in '1970-01-01T00:00:02' and ts in '1970-01-01T00:00:03'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("Empty table\n")
                    .returns(expected);
            bindVariableService.clear();
            bindVariableService.setStr("i", "1970-01-01T00:00:01");
            assertQuery("select * from x where ts not in :i and ts in '1970-01-01T00:00:02' and ts in '1970-01-01T00:00:03'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);

            expected = replaceTimestampSuffix1("""
                    a\tts
                    8.486964232560668\t1970-01-01T00:00:01.000000Z
                    """, timestampType.getTypeName());
            assertQuery("select * from x where ts in '1970-01-01T00:00' and ts in '1970-01-01T00:00:01'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"1970-01-01T00:00:01.000000Z\",\"1970-01-01T00:00:01.999999Z\")]\n" :
                                    "      intervals: [(\"1970-01-01T00:00:01.000000000Z\",\"1970-01-01T00:00:01.999999999Z\")]\n"))
                    .returns(expected);
            bindVariableService.clear();
            bindVariableService.setStr("i1", "1970-01-01T00:00");
            bindVariableService.setStr("i2", "1970-01-01T00:00:01");
            assertQuery("select * from x where ts in :i1 and ts in :i2")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);

            assertQuery("select * from x where ts in ('1970-01-01T00:00:01', '1970-01-01T00:00:02')")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(replaceTimestampSuffix1("""
                            a\tts
                            8.486964232560668\t1970-01-01T00:00:01.000000Z
                            8.43832076262595\t1970-01-01T00:00:02.000000Z
                            """, timestampType.getTypeName()));

            assertQuery("select * from x where ts in '1970-01-01T00:00:01' or ts in '1970-01-01T00:00:02'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(replaceTimestampSuffix1("""
                            a\tts
                            8.486964232560668\t1970-01-01T00:00:01.000000Z
                            8.43832076262595\t1970-01-01T00:00:02.000000Z
                            """, timestampType.getTypeName()));

            assertQuery("select * from x where ts in null")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("a\tts\n");

            assertQuery("select * from x where ts not in null")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(replaceTimestampSuffix1("""
                            a\tts
                            80.43224099968394\t1970-01-01T00:00:00.000000Z
                            8.486964232560668\t1970-01-01T00:00:01.000000Z
                            8.43832076262595\t1970-01-01T00:00:02.000000Z
                            """, timestampType.getTypeName()));

            assertQuery("select * from x where ts not in null and ts in '1970-01-01T00:00:01' and ts in '1970-01-01T00:00:02'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("a\tts\n");
        });
    }

    @Test
    public void testInIntervalWithFractions() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (" +
                            "  select" +
                            "    rnd_double(0)*100 a," +
                            "    timestamp_sequence(0, 110000)::" + timestampType.getTypeName() + " ts" +
                            "  from long_sequence(3)" +
                            ") timestamp(ts) partition by day"
            );

            String expected = replaceTimestampSuffix1("""
                    a\tts
                    8.486964232560668\t1970-01-01T00:00:00.110000Z
                    """, timestampType.getTypeName());

            assertQuery("select * from x where ts in '1970-01-01T00:00:00.1'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"1970-01-01T00:00:00.100000Z\",\"1970-01-01T00:00:00.199999Z\")]\n" :
                                    "      intervals: [(\"1970-01-01T00:00:00.100000000Z\",\"1970-01-01T00:00:00.199999999Z\")]\n"))
                    .returns(expected);
            bindVariableService.clear();
            bindVariableService.setStr("i", "1970-01-01T00:00:00.1");
            assertQuery("select * from x where ts in :i")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);

            expected = replaceTimestampSuffix1("""
                    a\tts
                    80.43224099968394\t1970-01-01T00:00:00.000000Z
                    8.43832076262595\t1970-01-01T00:00:00.220000Z
                    """, timestampType.getTypeName());
            assertQuery("select * from x where ts not in '1970-01-01T00:00:00.1'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"MIN\",\"1970-01-01T00:00:00.099999Z\"),(\"1970-01-01T00:00:00.200000Z\",\"MAX\")]\n" :
                                    "      intervals: [(\"MIN\",\"1970-01-01T00:00:00.099999999Z\"),(\"1970-01-01T00:00:00.200000000Z\",\"MAX\")]\n"))
                    .returns(expected);
            bindVariableService.clear();
            bindVariableService.setStr("i", "1970-01-01T00:00:00.1");
            assertQuery("select * from x where ts not in :i")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);

            expected = replaceTimestampSuffix1("""
                    a\tts
                    80.43224099968394\t1970-01-01T00:00:00.000000Z
                    8.43832076262595\t1970-01-01T00:00:00.220000Z
                    """, timestampType.getTypeName());
            assertQuery("select * from x where ts in '1970-01-01' and ts not in '1970-01-01T00:00:00.1'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"1970-01-01T00:00:00.000000Z\",\"1970-01-01T00:00:00.099999Z\"),(\"1970-01-01T00:00:00.200000Z\",\"1970-01-01T23:59:59.999999Z\")]\n" :
                                    "      intervals: [(\"1970-01-01T00:00:00.000000000Z\",\"1970-01-01T00:00:00.099999999Z\"),(\"1970-01-01T00:00:00.200000000Z\",\"1970-01-01T23:59:59.999999999Z\")]\n"))
                    .returns(expected);
            bindVariableService.clear();
            bindVariableService.setStr("d", "1970-01-01");
            bindVariableService.setStr("i", "1970-01-01T00:00:00.1");
            assertQuery("select * from x where ts in :d and ts not in :i")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);

            expected = "a\tts\n";
            assertQuery("select * from x where ts in '1970-01-01T00:00:00.1' and ts in '1970-01-01T00:00:00.2' and ts in '1970-01-01T00:00:00.3'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("Empty table\n")
                    .returns(expected);
            bindVariableService.clear();
            bindVariableService.setStr("i", "1970-01-01T00:00:00.1");
            assertQuery("select * from x where ts in :i and ts in '1970-01-01T00:00:00.2' and ts in '1970-01-01T00:00:00.3'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);

            expected = "a\tts\n";
            assertQuery("select * from x where ts not in '1970-01-01T00:00:00.1' and ts in '1970-01-01T00:00:00.2' and ts in '1970-01-01T00:00:00.3'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("Empty table\n")
                    .returns(expected);
            bindVariableService.clear();
            bindVariableService.setStr("i", "1970-01-01T00:00:00.1");
            assertQuery("select * from x where ts not in :i and ts in '1970-01-01T00:00:00.2' and ts in '1970-01-01T00:00:00.3'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);

            expected = replaceTimestampSuffix1("""
                    a\tts
                    8.486964232560668\t1970-01-01T00:00:00.110000Z
                    """, timestampType.getTypeName());
            assertQuery("select * from x where ts in '1970-01-01T00:00' and ts in '1970-01-01T00:00:00.1'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"1970-01-01T00:00:00.100000Z\",\"1970-01-01T00:00:00.199999Z\")]\n" :
                                    "      intervals: [(\"1970-01-01T00:00:00.100000000Z\",\"1970-01-01T00:00:00.199999999Z\")]\n"))
                    .returns(expected);
            bindVariableService.clear();
            bindVariableService.setStr("i1", "1970-01-01T00:00");
            bindVariableService.setStr("i2", "1970-01-01T00:00:00.1");
            assertQuery("select * from x where ts in :i1 and ts in :i2")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);

            assertQuery("select * from x where ts in ('1970-01-01T00:00:00.11', '1970-01-01T00:00:00.22')")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(replaceTimestampSuffix1("""
                            a\tts
                            8.486964232560668\t1970-01-01T00:00:00.110000Z
                            8.43832076262595\t1970-01-01T00:00:00.220000Z
                            """, timestampType.getTypeName()));

            assertQuery("select * from x where ts in '1970-01-01T00:00:00.1' or ts in '1970-01-01T00:00:00.2'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(replaceTimestampSuffix1("""
                            a\tts
                            8.486964232560668\t1970-01-01T00:00:00.110000Z
                            8.43832076262595\t1970-01-01T00:00:00.220000Z
                            """, timestampType.getTypeName()));

            assertQuery("select * from x where ts not in null and ts in '1970-01-01T00:00:00.1' and ts in '1970-01-01T00:00:00.2'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("a\tts\n");
        });
    }

    @Test
    public void testInTickListWithRuntimeInterval() throws Exception {
        // The predicate written last reaches the interval model builder first. A tick string with
        // several values that follows a run-time interval must intersect with it as one union of its
        // values. $today and :d8 are the Monday 2024-01-08, and now() is 01:00 on that day.
        assertMemoryLeak(() -> {
            createDayFilterTable();
            bindVariableService.clear();
            bindVariableService.setStr("d8", "2024-01-08");
            final String fri = "k\n1\n";
            final String mon = "k\n3\n";

            // a date variable
            assertInBothOrders("ts IN '2024-01-[08,09]'", "ts IN '$today'", mon);
            assertInBothOrders("ts IN '[2024-01-08,2024-01-09]'", "ts IN '$today'", mon);
            assertInBothOrders("ts IN '2024-01-[08..09]'", "ts IN '$today'", mon);
            assertInBothOrders("ts IN '2024-01-[08,09]T00:00;13h'", "ts IN '$today'", mon);
            assertInBothOrders("ts IN '2024-01-[05,08]'", "ts NOT IN '$today'", fri);
            // now()
            assertInBothOrders("ts IN '2024-01-[05,09]'", "ts < now()", fri);
            assertInBothOrders("ts IN '[2024-01-05,2024-01-09]'", "ts < now()", fri);
            assertInBothOrders("ts IN '2024-01-[08,09]'", "ts < dateadd('h', 12, now())", mon);
            // 2024-01-09 at +05:00 starts at 2024-01-08T19:00Z and holds the Tuesday 04:30 (row 4)
            assertInBothOrders("ts IN '2024-01-[05,09]@+05:00'", "ts < now()", fri);
            // the 12:00 hour of 2024-01-05, 06, 08 and 09
            assertInBothOrders("ts IN '2024-01-[05,08]T12:00;1h;1d;2'", "ts < now()", "k\n1\n2\n");
            // a run-time interval that holds both values
            assertInBothOrders("ts IN '2024-01-[05,09]'", "ts > dateadd('y', -1, now())", "k\n1\n4\n");
            // a bind variable
            assertInBothOrders("ts IN '2024-01-[08,09]'", "ts IN :d8", mon);
            assertInBothOrders("ts IN '2024-01-[05,08]'", "ts NOT IN :d8", fri);
            // a run-time OR
            assertInBothOrders("ts IN '2024-01-[06,09]'", "(ts IN '$today' OR ts IN '2024-01-06')", "k\n2\n");
            assertQuery("SELECT k FROM x WHERE ts IN '2024-01-[05,06]' AND (ts IN '$today' OR ts IN '2024-01-[05,09]')")
                    .noLeakCheck()
                    .returns(fri);
            // a static interval ahead of the run-time one
            assertQuery("SELECT k FROM x WHERE ts IN '2024-01-[05,09]' AND ts < now() AND ts IN '2024-01-[05..09]'")
                    .noLeakCheck()
                    .returns(fri);
            // a single value and a single periodic value
            assertInBothOrders("ts IN '2024-01-05'", "ts < now()", fri);
            assertInBothOrders("ts IN '2024-01-05T12:00;1h;1d;5'", "ts < now()", "k\n1\n2\n");
        });
    }

    @Test
    public void testInTickListWithRuntimeIntervalFollowsClockAndBindValues() throws Exception {
        // Compiles each query once and runs it again after the clock or a bind value changes, as a
        // plan from the query cache runs. The run-time interval follows the change, and the tick
        // string stays intersected with it.
        assertMemoryLeak(() -> {
            createDayFilterTable();
            try (RecordCursorFactory factory = select("SELECT k FROM x WHERE ts IN '2024-01-[05,09]' AND ts < now()")) {
                assertFactory(factory).withContext(sqlExecutionContext).returns("k\n1\n");
                setCurrentMicros(Micros.toMicros(2024, 1, 9, 5, 0));
                assertFactory(factory).withContext(sqlExecutionContext).returns("k\n1\n4\n");
                setCurrentMicros(Micros.toMicros(2024, 1, 5, 0, 0));
                assertFactory(factory).withContext(sqlExecutionContext).returns("k\n");
            }

            setCurrentMicros(Micros.toMicros(2024, 1, 8, 1, 0));
            try (RecordCursorFactory factory = select("SELECT k FROM x WHERE ts IN '2024-01-[08,09]' AND ts IN '$today'")) {
                assertFactory(factory).withContext(sqlExecutionContext).returns("k\n3\n");
                setCurrentMicros(Micros.toMicros(2024, 1, 9, 1, 0));
                assertFactory(factory).withContext(sqlExecutionContext).returns("k\n4\n");
                setCurrentMicros(Micros.toMicros(2024, 1, 10, 1, 0));
                assertFactory(factory).withContext(sqlExecutionContext).returns("k\n");
            }

            bindVariableService.clear();
            bindVariableService.setStr("p", "2024-01-08");
            try (RecordCursorFactory factory = select("SELECT k FROM x WHERE ts IN '2024-01-[08,09]' AND ts IN :p")) {
                assertFactory(factory).withContext(sqlExecutionContext).returns("k\n3\n");
                bindVariableService.setStr("p", "2024-01-09");
                assertFactory(factory).withContext(sqlExecutionContext).returns("k\n4\n");
                bindVariableService.setStr("p", "2024-01-[05,08]");
                assertFactory(factory).withContext(sqlExecutionContext).returns("k\n3\n");
                bindVariableService.setStr("p", "2024-01-05");
                assertFactory(factory).withContext(sqlExecutionContext).returns("k\n");
            }
        });
    }

    @Test
    public void testInTickListWithRuntimeIntervalInQueryContexts() throws Exception {
        // now() is 01:00 on 2024-01-08, so only the Friday 2024-01-05 (row 1) of the list matches
        assertMemoryLeak(() -> {
            createDayFilterTable();
            final String fri = "k\n1\n";
            final String plan = timestampType == TestTimestampType.NANO
                    ? "intervals: [(\"2024-01-05T00:00:00.000000000Z\",\"2024-01-05T23:59:59.999999999Z\")]"
                    : "intervals: [(\"2024-01-05T00:00:00.000000Z\",\"2024-01-05T23:59:59.999999Z\")]";
            assertQuery("SELECT k FROM x WHERE ts IN '2024-01-[05,09]' AND ts < now()")
                    .noLeakCheck()
                    .withPlanContaining(plan)
                    .returns(fri);
            assertQuery("WITH c AS (SELECT k, ts FROM x WHERE ts < now()) SELECT k FROM c WHERE ts IN '2024-01-[05,09]'")
                    .noLeakCheck()
                    .returns(fri);
            assertQuery("SELECT k FROM (SELECT k, ts FROM x WHERE ts IN '2024-01-[05,09]') WHERE ts < now()")
                    .noLeakCheck()
                    .returns(fri);
            assertQuery("SELECT count() FROM x WHERE ts IN '2024-01-[05,09]' AND ts < now()")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n1\n");
            assertQuery("SELECT k FROM x WHERE ts IN '2024-01-[05,09]' AND ts < now() ORDER BY ts DESC")
                    .noLeakCheck()
                    .returns(fri);
            // $today is the Monday 2024-01-08 (row 3)
            assertQuery("SELECT k FROM x WHERE ts IN '2024-01-[05,09]' AND ts < now() UNION ALL SELECT k FROM x WHERE ts IN '2024-01-[08,09]' AND ts IN '$today'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("k\n1\n3\n");
        });
    }

    @Test
    public void testInTickListWithRuntimeIntervalMaxIntervals() throws Exception {
        // 5 years x 12 months x 28 days at 09:00 for an hour make 1,680 intervals, above the default
        // cairo.sql.interval.max.intervals.after.merge of 1,024. 3 years at 12:00 make 1,008.
        assertMemoryLeak(() -> {
            createDayFilterTable();
            final String overCap = "'[2001..2005]-[01..12]-[01..28]T09:00;1h'";
            final String runtimePredicate = "ts > dateadd('y', -30, now())";
            assertOverMaxIntervalsError("SELECT k FROM x WHERE ts IN " + overCap + " AND " + runtimePredicate);
            assertOverMaxIntervalsError("SELECT k FROM x WHERE " + runtimePredicate + " AND ts IN " + overCap);
            assertOverMaxIntervalsError("SELECT k FROM x WHERE ts IN " + overCap + " AND ts IN '$today'");
            assertOverMaxIntervalsError("SELECT k FROM x WHERE ts NOT IN " + overCap + " AND " + runtimePredicate);
            assertOverMaxIntervalsError("SELECT k FROM x WHERE " + runtimePredicate + " AND ts NOT IN " + overCap);
            assertOverMaxIntervalsError("SELECT k FROM x WHERE ts IN '$today' OR ts IN " + overCap);
            assertOverMaxIntervalsError("SELECT k FROM x WHERE ts IN " + overCap + " OR ts IN '$today'");

            // the 12:00 rows on days 1 to 28 before now()
            assertInBothOrders("ts IN '[2022..2024]-[01..12]-[01..28]T12:00;1h'", "ts < now()", "k\n1\n2\n");
            assertInBothOrders("ts IN '[2022..2024]-[01..12]-[01..28]T12:00;1h'", runtimePredicate, "k\n1\n2\n3\n5\n6\n");
        });
    }

    @Test
    public void testInTickListWithRuntimeIntervalNoMatch() throws Exception {
        // The reproducer of #8112: neither day is today, and both lie more than a year back.
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE x (k INT, ts #TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", timestampType.getTypeName());
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-05T12:00:00.000000Z'),
                    (2, '2024-01-08T12:00:00.000000Z')
                    """);
            setCurrentMicros(Micros.toMicros(2026, 11, 2, 10, 0));
            bindVariableService.clear();
            bindVariableService.setStr("p", "2026-11-02");
            assertInBothOrders("ts IN '2024-01-[05,08]'", "ts IN '$today'", "k\n");
            assertInBothOrders("ts IN '2024-01-[05,08]'", "ts > dateadd('y', -1, now())", "k\n");
            assertInBothOrders("ts IN '[2024-01-05,2024-01-08]'", "ts > dateadd('y', -1, now())", "k\n");
            assertInBothOrders("ts IN '2024-01-[05,08]T12:00;1h'", "ts > dateadd('y', -1, now())", "k\n");
            assertInBothOrders("ts IN '2024-01-[05,08]'", "ts IN :p", "k\n");
            // both static
            assertInBothOrders("ts IN '2024-01-[05,08]'", "ts IN '2024-01-05'", "k\n1\n");
        });
    }

    @Test
    public void testInTickListWithRuntimeIntervalParseError() throws Exception {
        // The tick string fails with the same error whichever predicate reaches the builder first.
        assertMemoryLeak(() -> {
            createDayFilterTable();
            final String[][] cases = {
                    {"'2024-01-[05,40]'", "Invalid date: 2024-01-40"},
                    {"'[2024-01-05,2024-13-01]'", "Invalid date: 2024-13-01"},
                    {"'2024-01-[05,08'", "Unclosed '[' in interval"},
                    {"'2024-01-[]'", "Empty bracket expansion"},
                    {"'2024-01-[05,08]T12:00;1x'", "Invalid duration unit: x"},
                    {"'2024-01-[05,08]@Foo/Bar'", "invalid timezone: Foo/Bar"},
            };
            for (String[] c : cases) {
                final String tick = c[0];
                for (String sql : new String[]{
                        "SELECT k FROM x WHERE ts < now() AND ts IN " + tick,
                        "SELECT k FROM x WHERE ts IN " + tick + " AND ts < now()"
                }) {
                    assertExceptionNoLeakCheck(sql, sql.indexOf(tick), c[1]);
                }
            }
            // the failed compilations leave the next query intact
            assertInBothOrders("ts IN '2024-01-[05,09]'", "ts < now()", "k\n1\n");
        });
    }

    @Test
    public void testInTimeListSuffixBracketsAboveMergeThreshold() throws Exception {
        // Time list values with bracketed suffixes merge between them once the list has grown by
        // the incremental merge threshold (256), and the merges count against the cap (1,024).
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "CREATE TABLE x (k INT, tm #TIMESTAMP, ts #TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY",
                    timestampType.getTypeName()
            );
            // 2024-01-01 and 2024-01-08 are Mondays, 2024-01-06 a Saturday
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T04:00:00.005000Z', '2024-01-01T04:00:00.005000Z'),
                    (2, '2024-01-01T04:00:00.015000Z', '2024-01-01T04:00:00.015000Z'),
                    (3, '2024-01-01T09:00:00.505000Z', '2024-01-01T09:00:00.505000Z'),
                    (4, '2024-01-01T09:00:01.505000Z', '2024-01-01T09:00:01.505000Z'),
                    (5, '2024-01-01T09:00:59.905000Z', '2024-01-01T09:00:59.905000Z'),
                    (6, '2024-01-01T09:00:59.995000Z', '2024-01-01T09:00:59.995000Z'),
                    (7, '2024-01-06T04:00:10.000000Z', '2024-01-06T04:00:10.000000Z'),
                    (8, '2024-01-08T04:00:05.000000Z', '2024-01-08T04:00:05.000000Z'),
                    (9, '2024-01-08T04:00:10.000000Z', '2024-01-08T04:00:10.000000Z'),
                    (10, '2024-01-08T09:00:05.000000Z', '2024-01-08T09:00:05.000000Z'),
                    (11, '2024-01-08T09:00:10.000000Z', '2024-01-08T09:00:10.000000Z'),
                    (12, '2024-01-09T04:00:10.000000Z', '2024-01-09T04:00:10.000000Z')
                    """);
            // The 600 slots of 10 ms of the minute 09:00 from 10 duplicate values hold rows 3, 4 and
            // 5; rows 2 and 6 lie between slots. At +05:00 the minute is 04:00Z and holds row 1.
            // The slots never touch, so the JIT would need 600 range checks, more than its IR memory
            // (cairo.sql.jit.ir.memory.max.pages) holds, and the Java filter runs tm.
            final String values = "09:00,".repeat(9) + "09:00";
            assertInTickRows("2024-01-01T[" + values + "]:[00..59].[0..9]0", "k\n3\n4\n5\n", false);
            assertInTickRows("2024-01-01T[" + values + "]:[00..59].[0..9]0@+05:00", "k\n1\n", false);
            assertInTickRows("2024-01-01T[" + "09:00@+05:00,".repeat(9) + "09:00@+05:00]:[00..59].[0..9]0", "k\n1\n", false);
            assertInTickRows("2024-01-01T[" + "09:00@+05:00,04:00,".repeat(4) + "09:00@+05:00,04:00]:[00..59].[0..9]0", "k\n1\n", false);
            // 100 outer values of the same 100 seconds: the second 09:00:00 holds row 3. The 10,000
            // leaves make that one second, a single range check on the JIT.
            assertInTickRows("2024-01-01T[" + "09:,".repeat(99) + "09:][" + "00:00,".repeat(99) + "00:00]", "k\n3\n", true);
            // Sunday 2024-01-07 and Monday 2024-01-08 at 09:00 at +05:00, 04:00Z on the same day: the
            // first 16 seconds of the minutes 04:00 to 04:16 on Monday hold rows 8 and 9. The
            // dropped Sunday leaves never move Monday's leaves below the start of their zoned value,
            // which would leave them at 09:00Z, local time read as UTC (rows 10 and 11). The 17 runs
            // of 16 seconds make 17 range checks on the JIT.
            final StringBuilder minutes = new StringBuilder();
            final StringBuilder seconds = new StringBuilder();
            for (int k = 0; k < 17; k++) {
                minutes.append(k == 0 ? "" : ",").append(':').append(twoDigits(k));
                if (k < 16) {
                    seconds.append(k == 0 ? "" : ",").append(':').append(twoDigits(k));
                }
            }
            assertInTickRows("2024-01-[07..08]T[09@+05:00][" + minutes + "][" + seconds + "]#mon", "k\n8\n9\n", true);
            // One zoned value per day, 04:00:00 to 04:00:50Z on each Monday to Friday among days 1-28
            // of each month: rows 1, 2, 8, 9 and 12, not the Saturday (row 7) or 09:00Z (rows 10 and 11)
            final String workdayTick = "2024-[01..12]-[01..28]T[09:00@+05:00]:[00..50].[0..9]#workday";
            final String workdayRows = "SELECT k FROM x WHERE year(ts) = 2024 AND day(ts) <= 28 AND day_of_week(ts) <= 5 AND hour(ts) = 4 AND minute(ts) = 0 AND second(ts) <= 50";
            assertQuery(workdayRows).noLeakCheck().returns("k\n1\n2\n8\n9\n12\n");
            // Days 1-28 of a month hold 20 workdays, and the touching tenths of a second make one
            // interval per day, so the JIT checks 240 intervals
            assertInTickRows(workdayTick, "k\n1\n2\n8\n9\n12\n", true);

            // The first two values hold 1,200 intervals that never touch, so too-many-intervals
            // wins over the invalid third value.
            for (String tick : new String[]{
                    "2024-01-01T[00:00,00:01,25:99]:[00..59].[0..9]0",
                    "2024-01-01T[00:00,00:01,25:99]:[00..59].[0..9]0@+05:00",
                    "2024-01-01T[00:00@+05:00,00:01@+05:00,25:99@+05:00]:[00..59].[0..9]0",
                    "2024-01-01T[09:,10:,25:][" + evenSeconds(600) + "]",
            }) {
                for (String column : new String[]{"ts", "tm"}) {
                    final String sql = "SELECT k FROM x WHERE " + column + " IN '" + tick + "'";
                    assertExceptionNoLeakCheck(sql, sql.indexOf("'2024"), "Bracket expansion produces too many intervals (max 1024)");
                }
            }
        });
    }

    @Test
    public void testInTimeListTimeZoneAboveMergeThreshold() throws Exception {
        // Each time list value expands the first minute of its hour to 600 tenths of a second, above
        // the incremental merge threshold (256), and then converts them from its own time zone.
        // 20:00 at -05:00 is 01:00Z on the next day, and 10:00 at +05:00 is 05:00Z.
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "CREATE TABLE x (k INT, tm #TIMESTAMP, ts #TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY",
                    timestampType.getTypeName()
            );
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T05:00:30.000000Z', '2024-01-01T05:00:30.000000Z'),
                    (2, '2024-01-01T10:00:30.000000Z', '2024-01-01T10:00:30.000000Z'),
                    (5, '2024-01-01T15:00:30.000000Z', '2024-01-01T15:00:30.000000Z'),
                    (3, '2024-01-01T20:00:30.000000Z', '2024-01-01T20:00:30.000000Z'),
                    (4, '2024-01-02T01:00:30.000000Z', '2024-01-02T01:00:30.000000Z')
                    """);
            for (String column : new String[]{"ts", "tm"}) {
                assertQuery("SELECT k FROM x WHERE " + column + " IN '2024-01-01T[20:00@-05:00,10:00@+05:00]:[00..59].[0..9]'")
                        .noLeakCheck()
                        .returns("""
                                k
                                1
                                4
                                """);
                // both values at -05:00: 10:00 is 15:00Z
                assertQuery("SELECT k FROM x WHERE " + column + " IN '2024-01-01T[20:00,10:00]:[00..59].[0..9]@-05:00'")
                        .noLeakCheck()
                        .returns("""
                                k
                                5
                                4
                                """);
            }
        });
    }

    @Test
    public void testIncrementalMergeMatchesUnmergedParse() throws Exception {
        // testIntervalScanMatchesRowFilter compares two consumers of the same parse, so it cannot
        // see a wrong incremental merge. A merge threshold above the leaf count of every tick
        // string that randomTickString builds leaves only the final union, which must give the
        // intervals of the default parse.
        final CairoConfiguration unmergedConfiguration = new DefaultCairoConfiguration(".") {
            @Override
            public int getSqlIntervalIncrementalMergeThreshold() {
                return 5_000_000;
            }

            @Override
            public int getSqlIntervalMaxIntervalsAfterMerge() {
                return 10_000_000;
            }
        };
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            final TimestampDriver timestampDriver = timestampType.getDriver();
            final StringSink tickSink = new StringSink();
            final StringSink sink = new StringSink();
            final LongList expected = new LongList();
            final LongList actual = new LongList();
            for (int i = 0; i < 4_500; i++) {
                final String tick = randomTickString(rnd, tickSink, true, false);
                expected.clear();
                actual.clear();
                IntervalUtils.parseTickExpr(timestampDriver, unmergedConfiguration, tick, 0, tick.length(), 0, expected, sink);
                IntervalUtils.parseTickExpr(timestampDriver, configuration, tick, 0, tick.length(), 0, actual, sink);
                Assert.assertEquals(tick, expected, actual);
            }
        });
    }

    @Test
    public void testIntervalBwdNoLeak() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (" +
                            "  select" +
                            "    rnd_double(0)*100 a," +
                            "    timestamp_sequence(0, 10000)::" + timestampType.getTypeName() + " ts" +
                            "  from long_sequence(3)" +
                            ") timestamp(ts) partition by day"
            );

            assertException(
                    "select * from x where ts > now() - '3 day' order by ts desc",
                    0,
                    "inconvertible value: `3 day` [STRING -> LONG]"
            );
        });
    }

    @Test
    public void testIntervalFwdNoLeak() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (" +
                            "  select" +
                            "    rnd_double(0)*100 a," +
                            "    timestamp_sequence(0, 10000)::" + timestampType.getTypeName() + " ts" +
                            "  from long_sequence(3)" +
                            ") timestamp(ts) partition by day"
            );

            assertException(
                    "select * from x where ts > now() - '3 day'",
                    0,
                    "inconvertible value: `3 day` [STRING -> LONG]"
            );
        });
    }

    @Test
    public void testIntervalScanMatchesRowFilter() throws Exception {
        // Compares the interval scan on the designated timestamp ts with the row filter on tm, a
        // non-designated copy of ts, for random AND chains of tick strings and run-time intervals
        // in random order.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "CREATE TABLE x (k INT, tm #TIMESTAMP, ts #TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY",
                    timestampType.getTypeName()
            );
            // one row every 37 minutes from 2023-12-28 to 2024-01-20
            execute("""
                    INSERT INTO x
                    SELECT x::INT, '2023-12-28'::TIMESTAMP + (x - 1) * 2_220_000_000L, '2023-12-28'::TIMESTAMP + (x - 1) * 2_220_000_000L
                    FROM long_sequence(900)
                    """);
            final StringSink sink = new StringSink();
            final StringSink tickSink = new StringSink();
            for (int i = 0; i < 450; i++) {
                final long now = Micros.toMicros(2023, 12, 28, 0, 0) + rnd.nextLong(25 * Micros.DAY_MICROS);
                setCurrentMicros(now);
                final String bindValue = randomTickString(rnd, tickSink, true, false);
                bindVariableService.clear();
                bindVariableService.setStr("p", bindValue);

                sink.clear();
                final int predicateCount = 2 + rnd.nextInt(3);
                for (int j = 0; j < predicateCount; j++) {
                    if (j > 0) {
                        sink.put(" AND ");
                    }
                    putRandomPredicate(rnd, sink, tickSink);
                }
                final String predicate = sink.toString();
                final String rowFilterQuery = "SELECT k FROM x WHERE " + predicate.replace("%c", "tm");
                final String intervalScanQuery = "SELECT k FROM x WHERE " + predicate.replace("%c", "ts");
                try {
                    assertSqlCursors(rowFilterQuery, intervalScanQuery);
                } catch (AssertionError e) {
                    throw new AssertionError(intervalScanQuery + " [now=" + now + ", p=" + bindValue + ']', e);
                }
            }
        });
    }

    @Test
    public void testLessTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (" +
                            "  select" +
                            "    rnd_double(0)*100 a," +
                            "    timestamp_sequence(0, 10000)::" + timestampType.getTypeName() + " ts" +
                            "  from long_sequence(3)" +
                            ") timestamp(ts) partition by day"
            );

            String expected = replaceTimestampSuffix1("""
                    a\tts
                    80.43224099968394\t1970-01-01T00:00:00.000000Z
                    """, timestampType.getTypeName());
            assertQuery("select * from x where ts < '1970-01-01T00:00:00.010000Z'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"MIN\",\"1970-01-01T00:00:00.009999Z\")]\n" :
                                    "      intervals: [(\"MIN\",\"1970-01-01T00:00:00.009999999Z\")]\n"))
                    .returns(expected);
            assertQuery("select * from x where ts < '1970-01-01T00:00:00.010000000Z'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"MIN\",\"1970-01-01T00:00:00.009999Z\")]\n" :
                                    "      intervals: [(\"MIN\",\"1970-01-01T00:00:00.009999999Z\")]\n"))
                    .returns(expected);
            bindVariableService.clear();
            bindVariableService.setStr("ts", "1970-01-01T00:00:00.010000Z");
            bindVariableService.setStr("ts1", "1970-01-01T00:00:00.010000Z");
            assertQuery("select * from x where ts < :ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);
            assertQuery("select * from x where ts < :ts1")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);

            expected = replaceTimestampSuffix1("""
                    a\tts
                    8.486964232560668\t1970-01-01T00:00:00.010000Z
                    8.43832076262595\t1970-01-01T00:00:00.020000Z
                    """, timestampType.getTypeName());
            assertQuery("select * from x where ts >= '1970-01-01T00:00:00.010000Z'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan(replaceTimestampSuffix1("""
                            PageFrame
                                Row forward scan
                                Interval forward scan on: x
                                  intervals: [("1970-01-01T00:00:00.010000Z","MAX")]
                            """, timestampType.getTypeName()))
                    .returns(expected);
            assertQuery("select * from x where ts >= '1970-01-01T00:00:00.010000000Z'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan(replaceTimestampSuffix1("""
                            PageFrame
                                Row forward scan
                                Interval forward scan on: x
                                  intervals: [("1970-01-01T00:00:00.010000Z","MAX")]
                            """, timestampType.getTypeName()))
                    .returns(expected);
            bindVariableService.clear();
            bindVariableService.setStr("ts", "1970-01-01T00:00:00.010000Z");
            bindVariableService.setStr("ts1", "1970-01-01T00:00:00.010000Z");
            assertQuery("select * from x where ts >= :ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);
            assertQuery("select * from x where ts >= :ts1")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);

            expected = replaceTimestampSuffix1("""
                    a\tts
                    8.486964232560668\t1970-01-01T00:00:00.010000Z
                    8.43832076262595\t1970-01-01T00:00:00.020000Z
                    """, timestampType.getTypeName());
            assertQuery("select * from x where ts in '1970-01-01' and ts >= '1970-01-01T00:00:00.010000Z'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"1970-01-01T00:00:00.010000Z\",\"1970-01-01T23:59:59.999999Z\")]\n" :
                                    "      intervals: [(\"1970-01-01T00:00:00.010000000Z\",\"1970-01-01T23:59:59.999999999Z\")]\n"))
                    .returns(expected);
            assertQuery("select * from x where ts in '1970-01-01' and ts >= '1970-01-01T00:00:00.010000000Z'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"1970-01-01T00:00:00.010000Z\",\"1970-01-01T23:59:59.999999Z\")]\n" :
                                    "      intervals: [(\"1970-01-01T00:00:00.010000000Z\",\"1970-01-01T23:59:59.999999999Z\")]\n"))
                    .returns(expected);
            bindVariableService.clear();
            bindVariableService.setStr("date", "1970-01-01");
            bindVariableService.setStr("ts", "1970-01-01T00:00:00.010000Z");
            bindVariableService.setStr("ts1", "1970-01-01T00:00:00.010000000Z");
            assertQuery("select * from x where ts in :date and ts >= :ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);
            assertQuery("select * from x where ts in :date and ts >= :ts1")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);
            expected = "a\tts\n";
            assertQuery("select * from x where ts < '1970-01-01T00:00:00.010000Z' and ts in '1970-01-01T00:00:01' and ts in '1970-01-01T00:00:02'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("Empty table\n")
                    .returns(expected);
            assertQuery("select * from x where ts < '1970-01-01T00:00:00.010000000Z' and ts in '1970-01-01T00:00:01' and ts in '1970-01-01T00:00:02'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("Empty table\n")
                    .returns(expected);
            bindVariableService.clear();
            bindVariableService.setStr("ts", "1970-01-01T00:00:00.010000Z");
            bindVariableService.setStr("ts1", "1970-01-01T00:00:00.010000000Z");
            assertQuery("select * from x where ts < :ts and ts in '1970-01-01T00:00:01' and ts in '1970-01-01T00:00:02'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);
            assertQuery("select * from x where ts < :ts1 and ts in '1970-01-01T00:00:01' and ts in '1970-01-01T00:00:02'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);
        });
    }

    @Test
    public void testLessTimestampCursor() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (" +
                            "  select" +
                            "    rnd_double(0)*100 a," +
                            "    timestamp_sequence(0, 10000)::" + timestampType.getTypeName() + " ts" +
                            "  from long_sequence(3)" +
                            ") timestamp(ts) partition by day"
            );

            assertQuery("select * from x where ts < (select max(ts) from x)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"MIN\",\"1970-01-01T00:00:00.019999Z\")]\n" :
                                    "      intervals: [(\"MIN\",\"1970-01-01T00:00:00.019999999Z\")]\n"))
                    .returns(replaceTimestampSuffix1("""
                            a\tts
                            80.43224099968394\t1970-01-01T00:00:00.000000Z
                            8.486964232560668\t1970-01-01T00:00:00.010000Z
                            """, timestampType.getTypeName()));

            assertQuery("select * from x where ts <= (select max(ts) from x)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan(replaceTimestampSuffix1("""
                            PageFrame
                                Row forward scan
                                Interval forward scan on: x
                                  intervals: [("MIN","1970-01-01T00:00:00.020000Z")]
                            """, timestampType.getTypeName()))
                    .returns(replaceTimestampSuffix1("""
                            a\tts
                            80.43224099968394\t1970-01-01T00:00:00.000000Z
                            8.486964232560668\t1970-01-01T00:00:00.010000Z
                            8.43832076262595\t1970-01-01T00:00:00.020000Z
                            """, timestampType.getTypeName()));
        });
    }

    @Test
    public void testNotInDayFilterWithRuntimeNotIn() throws Exception {
        // 2024-09-01 is a Sunday, so the first day of '2024-09#workday' fails the filter, but the
        // month still holds the workdays from Monday 2024-09-02 on. The run-time NOT IN holds no
        // interval, because 2024-01-07 is a Sunday.
        assertMemoryLeak(() -> {
            createDayFilterTable();
            bindVariableService.clear();
            bindVariableService.setStr("i", "2024-01-07#workday");
            assertInBothOrders(
                    "ts NOT IN '2024-09#workday'",
                    "ts NOT IN :i",
                    """
                            k
                            1
                            2
                            3
                            4
                            5
                            """
            );
            // WhereClauseParser subtracts a not-equals string the same way as NOT IN.
            assertInBothOrders(
                    "ts != '2024-09#workday'",
                    "ts NOT IN :i",
                    """
                            k
                            1
                            2
                            3
                            4
                            5
                            """
            );
        });
    }

    @Test
    public void testNotInIntervalWithNoIntervals() throws Exception {
        // 2024-01-06 is a Saturday, so '2024-01-06#workday' holds no interval, and neither does
        // '$today#workday' on that day. Subtracting no interval used to leave no row.
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE x (k INT, ts #TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", timestampType.getTypeName());
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-05T12:00:00.000000Z'),
                    (2, '2024-01-06T12:00:00.000000Z'),
                    (3, '2024-01-08T12:00:00.000000Z')
                    """);
            setCurrentMicros(Micros.toMicros(2024, 1, 6, 1, 0));
            bindVariableService.clear();
            bindVariableService.setStr("i", "2024-01-06#workday");
            final String all = """
                    k
                    1
                    2
                    3
                    """;
            assertQuery("SELECT k FROM x WHERE ts NOT IN '2024-01-06#workday'")
                    .noLeakCheck()
                    .withPlanContaining("intervals: [(\"MIN\",\"MAX\")]")
                    .returns(all);
            assertQuery("SELECT k FROM x WHERE ts NOT IN '$today#workday'").noLeakCheck().returns(all);
            assertQuery("SELECT k FROM x WHERE NOT ts IN '$today#workday'").noLeakCheck().returns(all);
            assertQuery("SELECT k FROM x WHERE ts NOT IN :i").noLeakCheck().returns(all);

            final String fifthAndSixth = """
                    k
                    1
                    2
                    """;
            assertQuery("SELECT k FROM x WHERE ts IN '[2024-01-05, 2024-01-06]' AND ts NOT IN '2024-01-06#workday'").noLeakCheck().returns(fifthAndSixth);
            assertQuery("SELECT k FROM x WHERE ts IN '[2024-01-05, 2024-01-06]' AND ts NOT IN '$today#workday'").noLeakCheck().returns(fifthAndSixth);
            assertQuery("SELECT k FROM x WHERE ts IN '[2024-01-05, 2024-01-06]' AND ts NOT IN :i").noLeakCheck().returns(fifthAndSixth);
            assertQuery("SELECT k FROM x WHERE ts NOT IN '$today#workday' AND ts NOT IN '2024-01-08'").noLeakCheck().returns(fifthAndSixth);
            assertQuery("SELECT k FROM x WHERE ts NOT IN :i AND ts NOT IN '2024-01-08'").noLeakCheck().returns(fifthAndSixth);

            // An empty set stays empty.
            assertQuery("SELECT k FROM x WHERE ts IN '$today#workday' AND ts NOT IN '2024-01-07#workday'").noLeakCheck().returns("k\n");
            assertQuery("SELECT k FROM x WHERE ts NOT IN '2024-01-07#workday' AND ts IN '$today#workday'").noLeakCheck().returns("k\n");
            assertQuery("SELECT k FROM x WHERE ts IN '$today#workday' AND ts NOT IN '$today#workday'").noLeakCheck().returns("k\n");
            assertQuery("SELECT k FROM x WHERE ts NOT IN '$today#workday' AND ts IN '$today#workday'").noLeakCheck().returns("k\n");
            assertQuery("SELECT k FROM x WHERE ts IN '$today#workday' AND ts NOT IN :i").noLeakCheck().returns("k\n");
            assertQuery("SELECT k FROM x WHERE ts NOT IN :i AND ts IN '$today#workday'").noLeakCheck().returns("k\n");
        });
    }

    @Test
    public void testNotInTickListWithRuntimeInterval() throws Exception {
        // NOT IN subtracts every value of the tick string from the run-time interval. $today and
        // :d8 are the Monday 2024-01-08, and now() is 01:00 on that day.
        assertMemoryLeak(() -> {
            createDayFilterTable();
            bindVariableService.clear();
            bindVariableService.setStr("d8", "2024-01-08");
            assertInBothOrders("ts NOT IN '2024-01-[05,09]'", "ts < now()", "k\n2\n");
            assertInBothOrders("ts != '2024-01-[05,09]'", "ts < now()", "k\n2\n");
            assertInBothOrders("ts NOT IN '[2024-01-05,2024-01-09]'", "ts < now()", "k\n2\n");
            assertInBothOrders("ts NOT IN '2024-01-[05,06]'", "ts IN '$today'", "k\n3\n");
            assertInBothOrders("ts NOT IN '2024-01-[08,09]'", "ts IN '$today'", "k\n");
            assertInBothOrders("ts NOT IN '2024-01-[05,09]'", "ts IN :d8", "k\n3\n");
            assertInBothOrders("ts NOT IN '[2024-01-08,2024-01-09]'", "ts NOT IN :d8", "k\n1\n2\n5\n6\n");
            assertInBothOrders("ts NOT IN '[2024-01-08,2024-01-09]'", "ts NOT IN '$today'", "k\n1\n2\n5\n6\n");
            // 2024-01-09 at +05:00 starts at 2024-01-08T19:00Z and holds the Tuesday 04:30 (row 4)
            assertInBothOrders("ts NOT IN '2024-01-[05,09]@+05:00'", "ts > dateadd('y', -1, now())", "k\n2\n3\n5\n6\n");
            // the 12:00 hour of 2024-01-05, 06, 08 and 09
            assertInBothOrders("ts NOT IN '2024-01-[05,08]T12:00;1h;1d;2'", "ts > dateadd('y', -1, now())", "k\n4\n5\n6\n");
        });
    }

    @Test
    public void testOrInDayFilterMonthWithRuntimeInterval() throws Exception {
        // $today is the Monday 2024-01-08. 2024-09-01 is a Sunday, so the first day of
        // '2024-09#workday' fails the filter, but the month still holds Monday 2024-09-02.
        assertMemoryLeak(() -> {
            createDayFilterTable();
            final String expected = """
                    k
                    3
                    6
                    """;
            // the OR leaf written first reaches the interval model builder first
            assertQuery("SELECT k FROM x WHERE ts IN '2024-09#workday' OR ts IN '$today'").noLeakCheck().returns(expected);
            assertQuery("SELECT k FROM x WHERE ts IN '$today' OR ts IN '2024-09#workday'").noLeakCheck().returns(expected);
        });
    }

    @Test
    public void testOrInDayFilterWithRuntimeInterval() throws Exception {
        // $today is the Monday 2024-01-08; the Saturday bracket value holds no interval
        assertMemoryLeak(() -> {
            createDayFilterTable();
            final String fridayAndMonday = """
                    k
                    1
                    3
                    """;
            assertQuery("SELECT k FROM x WHERE ts IN '$today' OR ts IN '2024-01-[05,06]#workday'").noLeakCheck().returns(fridayAndMonday);
            assertQuery("SELECT k FROM x WHERE ts IN '2024-01-[05,06]#workday' OR ts IN '$today'").noLeakCheck().returns(fridayAndMonday);
            assertQuery("SELECT k FROM x WHERE ts IN '$today' OR ts IN '2024-01-06#workday'")
                    .noLeakCheck()
                    .returns("""
                            k
                            3
                            """);
        });
    }

    @Test
    public void testOrInTickListWithRuntimeInterval() throws Exception {
        // $today and :d8 are the Monday 2024-01-08
        assertMemoryLeak(() -> {
            createDayFilterTable();
            bindVariableService.clear();
            bindVariableService.setStr("d8", "2024-01-08");
            final String expected = """
                    k
                    1
                    3
                    4
                    """;
            assertOrInBothOrders("ts IN '2024-01-[05,09]'", "ts IN '$today'", expected);
            assertOrInBothOrders("ts IN '[2024-01-05,2024-01-09]'", "ts IN '$today'", expected);
            assertOrInBothOrders("ts IN '2024-01-[05,09]'", "ts IN :d8", expected);
            // 2024-01-09 at +05:00 starts at 2024-01-08T19:00Z and holds the Tuesday 04:30 (row 4)
            assertOrInBothOrders("ts IN '2024-01-[05,09]@+05:00'", "ts IN '$today'", expected);
            assertOrInBothOrders(
                    "ts IN '2024-01-[05,06]T12:00;1h'",
                    "ts IN '$today'",
                    """
                            k
                            1
                            2
                            3
                            """
            );
        });
    }

    @Test
    public void testRandomTickStringCoversTimeListsAndCoveredElements() throws Exception {
        // testIncrementalMergeMatchesUnmergedParse and testIntervalScanMatchesRowFilter reach the
        // merges between time list values and the merge range of a date list only through the
        // tick strings that randomTickString builds, so it must build each shape that runs them.
        // The merge threshold is 256 intervals.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        // case 6: the date or day range, the values, the tenths, the global zone and the day filter
        final Pattern timeListPattern = Pattern.compile("2024-01-(\\d\\d|\\[(\\d\\d)\\.\\.(\\d\\d)])T\\[([^]]+)]:\\[00\\.\\.59](\\.\\[0\\.\\.9])?(@[^#;]+)?(#[^;]+)?(;\\d+m)?");
        // case 7: the covering day, the zoned element in between, the time of the later element
        // and the day filter
        final Pattern coveredPattern = Pattern.compile("\\[2024-01-(\\d\\d)(?:,(2024-01-\\[\\d\\d\\.\\.\\d\\d]@[^,]+))?,2024-01-\\d\\dT(.+)](#.+)?");
        // case 8: the outer values, the inner values, the seconds bracket and the day filter
        final Pattern nestedPattern = Pattern.compile("2024-01-\\d\\dT\\[([^]]+)]\\[([^]]+)](\\[00\\.\\.59])?(#.+)?");
        final String overCapError = "Bracket expansion produces too many intervals (max 1024)";
        assertMemoryLeak(() -> {
            final TimestampDriver timestampDriver = timestampType.getDriver();
            final StringSink tickSink = new StringSink();
            final StringSink sink = new StringSink();
            final LongList intervals = new LongList();
            boolean hasZonedTenthValues = false;
            boolean hasZonedSecondValues = false;
            boolean hasZonelessValues = false;
            boolean hasFilteredValues = false;
            boolean hasDayRangeOfSmallCalls = false;
            final boolean[] hasCoveredSpelling = new boolean[3];
            boolean hasCoveredElementAfterZonedElement = false;
            boolean hasNestedMinutes = false;
            boolean hasNestedSeconds = false;
            boolean hasNestedZoneAndFilter = false;
            boolean hasNestedMergeBeforeLastValue = false;
            for (int i = 0; i < 4_500; i++) {
                final String tick = randomTickString(rnd, tickSink, true, false);
                final Matcher timeList = timeListPattern.matcher(tick);
                if (timeList.matches()) {
                    final int valueCount = countValues(timeList.group(4));
                    final boolean hasValueZone = timeList.group(4).indexOf('@') >= 0;
                    final boolean hasZone = hasValueZone || timeList.group(6) != null;
                    final boolean hasTenths = timeList.group(5) != null;
                    final boolean hasDayFilter = timeList.group(7) != null;
                    // 600 leaves per value: the values merge from the third on
                    hasZonedTenthValues |= hasValueZone && hasTenths && valueCount >= 3;
                    // 60 leaves per value: only the merges between values run
                    hasZonedSecondValues |= hasZone && !hasTenths && valueCount >= 5;
                    hasZonelessValues |= !hasZone && valueCount >= 3;
                    hasFilteredValues |= hasDayFilter;
                    // Each day runs the time list, whose 60 leaves per value stay below the threshold,
                    // while the days together reach it: the merges between values join values of
                    // several days.
                    if (timeList.group(2) != null && !hasTenths && hasZone && hasDayFilter) {
                        final int dayCount = Integer.parseInt(timeList.group(3)) - Integer.parseInt(timeList.group(2)) + 1;
                        hasDayRangeOfSmallCalls |= valueCount * 60 < 256 && dayCount * valueCount * 60 >= 256;
                    }
                    continue;
                }
                final Matcher covered = coveredPattern.matcher(tick);
                if (covered.matches()) {
                    // the list parses, while its later element fails alone
                    final String laterTime = covered.group(3);
                    // [HH..HH]:00:..., [HH:MM,HH:MM]:... or [HH:MM]:...
                    final int spelling = laterTime.charAt(3) == '.' ? 0 : laterTime.charAt(6) == ',' ? 1 : 2;
                    intervals.clear();
                    IntervalUtils.parseTickExpr(timestampDriver, configuration, tick, 0, tick.length(), 0, intervals, sink);
                    final String laterElement = "2024-01-" + covered.group(1) + 'T' + laterTime;
                    final String dayFilter = covered.group(4) != null ? covered.group(4) : "";
                    final String laterElementAlone = laterElement + dayFilter;
                    intervals.clear();
                    try {
                        IntervalUtils.parseTickExpr(timestampDriver, configuration, laterElementAlone, 0, laterElementAlone.length(), 0, intervals, sink);
                    } catch (SqlException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), overCapError);
                        hasCoveredSpelling[spelling] = true;
                    }
                    if (covered.group(2) != null) {
                        // The later element merges after the zoned element restored the merge range
                        // of the list. Without the covering day the list fails, so merges from the
                        // start of the zoned element would fail as well.
                        final String uncoveredList = '[' + covered.group(2) + ',' + laterElement + ']' + dayFilter;
                        intervals.clear();
                        try {
                            IntervalUtils.parseTickExpr(timestampDriver, configuration, uncoveredList, 0, uncoveredList.length(), 0, intervals, sink);
                        } catch (SqlException e) {
                            TestUtils.assertContains(e.getFlyweightMessage(), overCapError);
                            hasCoveredElementAfterZonedElement = true;
                        }
                    }
                    continue;
                }
                final Matcher nested = nestedPattern.matcher(tick);
                if (nested.matches()) {
                    final boolean isMinutes = nested.group(3) != null;
                    hasNestedMinutes |= isMinutes;
                    hasNestedSeconds |= !isMinutes;
                    hasNestedZoneAndFilter |= nested.group(1).indexOf('@') >= 0 && nested.group(4) != null;
                    // the outer values before the last one add 256 one-second leaves or more
                    hasNestedMergeBeforeLastValue |= !isMinutes && (countValues(nested.group(1)) - 1) * countValues(nested.group(2)) >= 256;
                }
            }
            Assert.assertTrue("zoned values of 600 leaves", hasZonedTenthValues);
            Assert.assertTrue("zoned values of 60 leaves", hasZonedSecondValues);
            Assert.assertTrue("zone-less values", hasZonelessValues);
            Assert.assertTrue("values under a day filter", hasFilteredValues);
            Assert.assertTrue("a day range of time lists below the merge threshold", hasDayRangeOfSmallCalls);
            Assert.assertTrue("covered element of hour brackets", hasCoveredSpelling[0]);
            Assert.assertTrue("covered element of two time list values", hasCoveredSpelling[1]);
            Assert.assertTrue("covered element of one time list value", hasCoveredSpelling[2]);
            Assert.assertTrue("covered element after a zoned element", hasCoveredElementAfterZonedElement);
            Assert.assertTrue("nested minutes", hasNestedMinutes);
            Assert.assertTrue("nested seconds", hasNestedSeconds);
            Assert.assertTrue("nested values with a zone under a day filter", hasNestedZoneAndFilter);
            Assert.assertTrue("nested values that merge before the last outer value", hasNestedMergeBeforeLastValue);
        });
    }

    @Test
    public void testRandomTickStringCoversDateVariableListWithTouchingLeavesOnJit() throws Exception {
        // testIntervalScanMatchesRowFilter runs the JIT bound of a list that holds a date variable
        // and static elements, which counts each run of touching static intervals once, only on the
        // lists that randomTickString builds. So it must build such a list whose static leaves
        // touch, as the 600 tenths of a second of a time list value do, and that the JIT runs while
        // the compiled list holds more IR entries than the IN list threshold (10): a bound of one
        // interval per entry would send it to the Java filter.
        Assume.assumeTrue(JitUtil.isJitSupported());
        final Rnd rnd = TestUtils.generateRandom(LOG);
        // case 6 with tenths of a second
        final Pattern tenthsPattern = Pattern.compile("\\[\\$today,2024-01-(\\d\\d|\\[\\d\\d\\.\\.\\d\\d])T\\[[^]]+]:\\[00\\.\\.59]\\.\\[0\\.\\.9]].*");
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "CREATE TABLE x (k INT, tm #TIMESTAMP, ts #TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY",
                    timestampType.getTypeName()
            );
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            final TimestampDriver timestampDriver = timestampType.getDriver();
            final int threshold = configuration.getSqlJitMaxInListSizeThreshold();
            final StringSink tickSink = new StringSink();
            boolean hasTouchingTenthsOnJit = false;
            for (int i = 0; i < 4_500 && !hasTouchingTenthsOnJit; i++) {
                final String tick = randomTickString(rnd, tickSink, true, true);
                if (!tenthsPattern.matcher(tick).matches()) {
                    continue;
                }
                try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(timestampDriver, configuration, tick, 0, tick.length(), 0)) {
                    if (compiled.getElementCount() <= threshold) {
                        continue;
                    }
                }
                try (RecordCursorFactory factory = select("SELECT k FROM x WHERE tm IN '" + tick + "'")) {
                    hasTouchingTenthsOnJit = factory.usesCompiledFilter();
                }
            }
            Assert.assertTrue("$today and touching tenths of more IR entries than the IN list threshold on the JIT", hasTouchingTenthsOnJit);
        });
    }

    @Test
    public void testRandomTickStringExceedsMaxIntervalsFromListStart() throws Exception {
        // testIncrementalMergeMatchesUnmergedParse catches an incremental merge that counts the
        // earlier elements' intervals against the cap (1,024) only if randomTickString builds
        // lists that hold more than the cap while a later element merges. A later element with a
        // time zone keeps a merge range of its own, whose merges count only its own intervals while
        // the list holds more: each element order must reach such a merge. A later element without
        // a zone adds its intervals to the merge range of the list, whose merges union them with the
        // earlier elements' before they count, so no read of the cap sees more than the union of the
        // list, at most 960 intervals. Without a day filter, no other read of the cap sees such a
        // list: the assertion on entry sees the empty list, and the other checks follow a union of
        // the whole list.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            final TimestampDriver timestampDriver = timestampType.getDriver();
            final StringSink tickSink = new StringSink();
            final StringSink sink = new StringSink();
            final LongList intervals = new LongList();
            final ListSizeAtCapCheckConfiguration listSizeConfiguration = new ListSizeAtCapCheckConfiguration(intervals);
            final int maxIntervals = listSizeConfiguration.getSqlIntervalMaxIntervalsAfterMerge();
            // the configuration of testIncrementalMergeMatchesUnmergedParse
            Assert.assertEquals(configuration.getSqlIntervalIncrementalMergeThreshold(), listSizeConfiguration.getSqlIntervalIncrementalMergeThreshold());
            Assert.assertEquals(configuration.getSqlIntervalMaxIntervalsAfterMerge(), maxIntervals);
            boolean hasLargerFirstOverCap = false;
            boolean hasSmallerFirstOverCap = false;
            for (int i = 0; i < 4_500; i++) {
                final String tick = randomTickString(rnd, tickSink, true, false);
                final boolean isLargerFirst = tick.startsWith("[2024-[01..02]");
                if (tick.indexOf('#') >= 0 || (!isLargerFirst && !tick.contains(",2024-[01..02]"))) {
                    continue;
                }
                intervals.clear();
                listSizeConfiguration.maxListIntervalCount = 0;
                IntervalUtils.parseTickExpr(timestampDriver, listSizeConfiguration, tick, 0, tick.length(), 0, intervals, sink);
                if (tick.indexOf('@') < 0) {
                    Assert.assertTrue(tick + ": " + listSizeConfiguration.maxListIntervalCount, listSizeConfiguration.maxListIntervalCount <= maxIntervals);
                } else if (listSizeConfiguration.maxListIntervalCount > maxIntervals) {
                    if (isLargerFirst) {
                        hasLargerFirstOverCap = true;
                    } else {
                        hasSmallerFirstOverCap = true;
                    }
                }
            }
            Assert.assertTrue("larger element first", hasLargerFirstOverCap);
            Assert.assertTrue("smaller element first", hasSmallerFirstOverCap);
        });
    }

    @Test
    public void testTimestampCursorMixed() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (" +
                            "  select" +
                            "    rnd_double(0)*100 a," +
                            "    timestamp_sequence(0, 10000)::" + timestampType.getTypeName() + " ts" +
                            "  from long_sequence(3)" +
                            ") timestamp(ts) partition by day"
            );

            assertQuery("select * from x where ts > (select min(ts) from x) and ts < (select max(ts) from x)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"1970-01-01T00:00:00.000001Z\",\"1970-01-01T00:00:00.019999Z\")]\n" :
                                    "      intervals: [(\"1970-01-01T00:00:00.000000001Z\",\"1970-01-01T00:00:00.019999999Z\")]\n"))
                    .returns(replaceTimestampSuffix1("""
                            a\tts
                            8.486964232560668\t1970-01-01T00:00:00.010000Z
                            """, timestampType.getTypeName()));
        });
    }

    // The error points at the tick string literal.
    private static void assertOverMaxIntervalsError(String sql) throws Exception {
        assertExceptionNoLeakCheck(sql, sql.indexOf("'["), "Bracket expansion produces too many intervals (max 1024)");
    }

    // Expects the query to run its filter on the JIT when isJitExpected, the JIT mode of the context
    // is on and the platform has a JIT, and on the Java filter or without a row filter otherwise.
    private static void assertUsesCompiledFilter(String sql, boolean isJitExpected) throws SqlException {
        final int jitMode = sqlExecutionContext.getJitMode();
        try (RecordCursorFactory factory = select(sql)) {
            Assert.assertEquals(
                    sql + ", jit mode " + jitMode,
                    isJitExpected && jitMode != SqlJitMode.JIT_MODE_DISABLED && JitUtil.isJitSupported(),
                    factory.usesCompiledFilter()
            );
        }
    }

    private static void putRandomPredicate(Rnd rnd, StringSink sink, StringSink tickSink) {
        // %c stands for the timestamp column
        switch (rnd.nextInt(14)) {
            case 0 -> sink.put("%c < now()");
            case 1 -> sink.put("%c >= now()");
            case 2 -> sink.put("%c > dateadd('h', ").put(-rnd.nextInt(100)).put(", now())");
            case 3 -> sink.put("%c <= dateadd('d', ").put(rnd.nextInt(5)).put(", now())");
            case 4 -> sink.put("%c BETWEEN dateadd('d', -").put(rnd.nextInt(5)).put(", now()) AND now()");
            case 5 -> sink.put(rnd.nextBoolean() ? "%c IN '$today'" : "%c IN '$yesterday'");
            case 6 -> sink.put("%c NOT IN '$today'");
            case 7 -> sink.put(rnd.nextBoolean() ? "%c IN :p" : "%c NOT IN :p");
            case 8 -> {
                // The OR leaf written first reaches the interval model builder first. A first leaf
                // that holds no interval drops the other leaves (#8115), so a tick string written
                // first carries no day filter, which is what can leave it without intervals.
                final String runtime = rnd.nextBoolean() ? "%c IN '$today'" : "%c IN :p";
                final boolean isRuntimeFirst = rnd.nextBoolean();
                final String tick = "%c IN '" + randomTickString(rnd, tickSink, isRuntimeFirst, false) + '\'';
                sink.put('(').put(isRuntimeFirst ? runtime : tick).put(" OR ").put(isRuntimeFirst ? tick : runtime).put(')');
            }
            case 9 -> {
                final String tick = randomTickString(rnd, tickSink, true, false);
                sink.put("%c NOT IN '").put(tick).put('\'');
            }
            // $today in one list with static elements. On tm, the JIT checks as many intervals as
            // $today and the runs of the static intervals can yield.
            case 10 -> {
                final String tick = randomTickString(rnd, tickSink, true, true);
                sink.put("%c IN '").put(tick).put('\'');
            }
            case 11 -> {
                final String tick = randomTickString(rnd, tickSink, true, true);
                sink.put("%c NOT IN '").put(tick).put('\'');
            }
            default -> {
                final String tick = randomTickString(rnd, tickSink, true, false);
                sink.put("%c IN '").put(tick).put('\'');
            }
        }
    }

    // The number of comma-separated values
    private static int countValues(String values) {
        int valueCount = 1;
        for (int i = 0, n = values.length(); i < n; i++) {
            if (values.charAt(i) == ',') {
                valueCount++;
            }
        }
        return valueCount;
    }

    // The time list values MM:SS of the first count even seconds from 00:00: 00:00,00:02,...
    private static String evenSeconds(int count) {
        final StringBuilder values = new StringBuilder();
        for (int k = 0; k < count; k++) {
            values.append(k == 0 ? "" : ",").append(twoDigits(2 * k / 60)).append(':').append(twoDigits(2 * k % 60));
        }
        return values.toString();
    }

    // A time zone of the time list value, with probability 1/3. Hours 06-19 at either offset stay
    // on the same day in UTC, so a day filter judges a leaf by its local and its UTC day alike.
    private static void putRandomValueZone(Rnd rnd, StringSink sink) {
        switch (rnd.nextInt(6)) {
            case 0 -> sink.put("@+05:00");
            case 1 -> sink.put("@-03:30");
            default -> {
            }
        }
    }

    // A tick string over the days of January 2024, in one case also of February, that both the
    // interval scan and the row filter resolve: lists, ranges, times, durations, repetitions,
    // fixed offsets, day filters, time lists with bracketed suffixes and nested time lists. With
    // hasDateVariable, $today opens the list of the elements, and both evaluate the list through a
    // CompiledTickExpression. That compiles each static element on its own and takes no
    // repetition, so the list leaves out case 7, whose later element exceeds the cap (1,024)
    // without the day that covers it, and the repetition of a time.
    private static String randomTickString(Rnd rnd, StringSink sink, boolean isDayFilterAllowed, boolean hasDateVariable) {
        sink.clear();
        final int day = 1 + rnd.nextInt(20);
        boolean hasTime = rnd.nextBoolean();
        boolean isZoneAllowed = true;
        int listDurationMinutes = 0;
        int shape = rnd.nextInt(hasDateVariable ? 8 : 9);
        if (hasDateVariable && shape == 7) {
            shape = 8;
        }
        switch (shape) {
            case 0 -> sink.put("2024-01-").put(twoDigits(day));
            case 1 -> {
                sink.put("2024-01-[").put(twoDigits(day));
                for (int i = 0, n = 1 + rnd.nextInt(3); i < n; i++) {
                    sink.put(',').put(twoDigits(1 + rnd.nextInt(20)));
                }
                sink.put(']');
            }
            case 2 -> sink.put("2024-01-[").put(twoDigits(day)).put("..").put(twoDigits(day + rnd.nextInt(5))).put(']');
            case 3 -> {
                // The hourly values of the two elements together reach the incremental merge
                // threshold (256), mostly within the second element. The elements carry the time.
                final int split = 2 + rnd.nextInt(17);
                sink.put("[2024-01-[01..").put(twoDigits(split)).put("]T[00..23]:").put(twoDigits(rnd.nextInt(60)))
                        .put(",2024-01-[").put(twoDigits(split + 1)).put("..20]T[00..23]:").put(twoDigits(rnd.nextInt(60)))
                        .put(']');
                hasTime = false;
                listDurationMinutes = rnd.nextBoolean() ? 1 + rnd.nextInt(90) : 0;
            }
            case 4 -> {
                // Two overlapping elements of hourly values at one minute, in random order: days
                // 1-20 of January and February, and 3 to 20 of those days in January. The union
                // is the larger element's 960 values. Under a time zone, the later element merges a
                // range of its own while the list holds more than the cap (1,024): 960 + 256
                // intervals at the first merge of a smaller second element of 11 days or more, and
                // 72 + 960 or more at the last merge of a larger second element; each merge must
                // count only the later element's intervals. Without a zone, the later element's
                // merges union its values with the earlier element's before they count.
                final String minute = twoDigits(rnd.nextInt(60));
                final int smallerDayCount = 3 + rnd.nextInt(18);
                final int smallerFirstDay = 1 + rnd.nextInt(21 - smallerDayCount);
                final String larger = "2024-[01..02]-[01..20]T[00..23]:" + minute;
                final String smaller = "2024-01-[" + twoDigits(smallerFirstDay) + ".." + twoDigits(smallerFirstDay + smallerDayCount - 1)
                        + "]T[00..23]:" + minute;
                final boolean isLargerFirst = rnd.nextBoolean();
                sink.put('[').put(isLargerFirst ? larger : smaller).put(',').put(isLargerFirst ? smaller : larger).put(']');
                hasTime = false;
                // durations up to 58 minutes keep the values apart
                listDurationMinutes = rnd.nextBoolean() ? 1 + rnd.nextInt(58) : 0;
            }
            case 6 -> {
                // Time list values with bracketed suffixes: 60 touching seconds or 600 touching
                // tenths of a second each, some with a time zone of their own, on one day or on a
                // range of days, each of which runs the time list. Values above the incremental
                // merge threshold (256) merge within, and the values merge between them once they
                // have added that many, also across the days.
                final int lastDay = day + rnd.nextInt(4);
                sink.put("2024-01-");
                if (lastDay > day) {
                    sink.put('[').put(twoDigits(day)).put("..").put(twoDigits(lastDay)).put(']');
                } else {
                    sink.put(twoDigits(day));
                }
                sink.put("T[");
                for (int i = 0, n = 1 + rnd.nextInt(6); i < n; i++) {
                    sink.put(i == 0 ? "" : ",").put(twoDigits(6 + rnd.nextInt(14))).put(':').put(twoDigits(rnd.nextInt(60)));
                    putRandomValueZone(rnd, sink);
                }
                sink.put(rnd.nextBoolean() ? "]:[00..59]" : "]:[00..59].[0..9]");
                hasTime = false;
                listDurationMinutes = rnd.nextBoolean() ? 1 + rnd.nextInt(58) : 0;
            }
            case 7 -> {
                // A later element of 1,200 or 6,000 intervals that never touch, more than the cap
                // (1,024), all in the first element's day. With no time zone of its own it adds
                // them to the merge range of the list, whose merges union them into that day, also
                // after an element in between whose time zone gives it a merge range of its own,
                // which it closes by restoring the merge range of the list. No time zone on the
                // later element, with which it fails by design, and no duration, which would
                // shorten the covering day.
                final int hour = 6 + rnd.nextInt(13);
                sink.put("[2024-01-").put(twoDigits(day));
                if (rnd.nextBoolean()) {
                    final int zonedDay = 1 + rnd.nextInt(20);
                    sink.put(",2024-01-[").put(twoDigits(zonedDay)).put("..").put(twoDigits(zonedDay + rnd.nextInt(3))).put(']')
                            .put(rnd.nextBoolean() ? "@+05:00" : "@-03:30");
                }
                sink.put(",2024-01-").put(twoDigits(day)).put("T[").put(twoDigits(hour));
                switch (rnd.nextInt(3)) {
                    case 0 -> sink.put("..").put(twoDigits(hour + 1)).put("]:00:[00..59].[0..9]0]");
                    case 1 -> {
                        final String minute = twoDigits(rnd.nextInt(60));
                        sink.put(':').put(minute).put(',').put(twoDigits(hour + 1)).put(':').put(minute).put("]:[00..59].[0..9]0]");
                    }
                    default -> sink.put(':').put(twoDigits(rnd.nextInt(60))).put("]:[00..59].[0..9][0..9]0]");
                }
                hasTime = false;
                isZoneAllowed = false;
            }
            case 8 -> {
                // Nested time lists: the seconds of whole minutes, or single seconds, which the
                // outer values merge between them once they have added 256 intervals. A global
                // zone would convert the inner times twice, so only the outer values carry zones.
                sink.put("2024-01-").put(twoDigits(day)).put("T[");
                final boolean isMinutes = rnd.nextBoolean();
                for (int i = 0, n = 2 + rnd.nextInt(isMinutes ? 3 : 5); i < n; i++) {
                    sink.put(i == 0 ? "" : ",").put(twoDigits(6 + rnd.nextInt(14))).put(':');
                    putRandomValueZone(rnd, sink);
                }
                sink.put("][");
                if (isMinutes) {
                    for (int i = 0, n = 2 + rnd.nextInt(5); i < n; i++) {
                        sink.put(i == 0 ? "" : ",").put(twoDigits(rnd.nextInt(60))).put(':');
                    }
                    sink.put("][00..59]");
                } else {
                    for (int i = 0, n = 20 + rnd.nextInt(61); i < n; i++) {
                        sink.put(i == 0 ? "" : ",").put(twoDigits(rnd.nextInt(60))).put(':').put(twoDigits(rnd.nextInt(60)));
                    }
                    sink.put(']');
                }
                hasTime = false;
                isZoneAllowed = false;
            }
            default -> {
                sink.put("[2024-01-").put(twoDigits(day));
                for (int i = 0, n = 1 + rnd.nextInt(3); i < n; i++) {
                    sink.put(",2024-01-").put(twoDigits(1 + rnd.nextInt(20)));
                }
                sink.put(']');
            }
        }
        if (hasDateVariable) {
            // $today joins the list, or makes a list with the single element
            final String elements = sink.toString();
            sink.clear();
            sink.put("[$today,");
            if (elements.charAt(0) == '[') {
                sink.put(elements, 1, elements.length());
            } else {
                sink.put(elements).put(']');
            }
        }
        if (hasTime) {
            sink.put('T').put(twoDigits(rnd.nextInt(24))).put(":00");
        }
        switch (isZoneAllowed ? rnd.nextInt(4) : 3) {
            case 0 -> sink.put("@+05:00");
            case 1 -> sink.put("@-03:30");
            default -> {
            }
        }
        if (isDayFilterAllowed) {
            switch (rnd.nextInt(5)) {
                case 0 -> sink.put("#workday");
                case 1 -> sink.put("#mon,wed");
                default -> {
                }
            }
        }
        if (hasTime) {
            switch (rnd.nextInt(3)) {
                case 0 -> sink.put(';').put(1 + rnd.nextInt(30)).put('h');
                case 1 -> {
                    sink.put(';').put(1 + rnd.nextInt(5)).put('h');
                    if (!hasDateVariable) {
                        sink.put(";1d;").put(1 + rnd.nextInt(4));
                    }
                }
                default -> {
                }
            }
        } else if (listDurationMinutes > 0) {
            sink.put(';').put(listDurationMinutes).put('m');
        }
        return sink.toString();
    }

    private static String twoDigits(int value) {
        return value < 10 ? "0" + value : String.valueOf(value);
    }

    private void assertInBothOrders(String staticPredicate, String runtimePredicate, String expected) throws Exception {
        // the predicate written last reaches the interval model builder first
        assertQuery("SELECT k FROM x WHERE " + runtimePredicate + " AND " + staticPredicate).noLeakCheck().returns(expected);
        assertQuery("SELECT k FROM x WHERE " + staticPredicate + " AND " + runtimePredicate).noLeakCheck().returns(expected);
    }

    // Expects the rows of the reference predicate from the tick string on each column of table t:
    // the designated ts, the non-designated tm and the DATE d, with the JIT on and off.
    private void assertInOnEveryColumn(String tick, String referencePredicate, int expectedRowCount) throws Exception {
        final String reference = "SELECT ts FROM t WHERE " + referencePredicate;
        assertQuery("SELECT count() FROM (" + reference + ")")
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("count\n" + expectedRowCount + "\n");
        final int callerJitMode = sqlExecutionContext.getJitMode();
        try {
            for (int jitMode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                sqlExecutionContext.setJitMode(jitMode);
                for (String column : new String[]{"ts", "tm", "d"}) {
                    assertSqlCursors(reference, "SELECT ts FROM t WHERE " + column + " IN '" + tick + "'");
                }
            }
        } finally {
            sqlExecutionContext.setJitMode(callerJitMode);
        }
    }

    // Expects the rows of the tick string on the designated ts and the non-designated tm of table
    // x, as a literal and as a bind variable, with the JIT mode on and off, and the filter that
    // runs them. The designated ts runs an interval scan with no row filter, and the JIT never
    // compiles a timestamp IN over a bind variable. So only the literal on tm can run on the JIT,
    // and isJitExpected says whether it does when the JIT is on.
    private void assertInTickRows(String tick, String expected, boolean isJitExpected) throws Exception {
        final int callerJitMode = sqlExecutionContext.getJitMode();
        try {
            for (int jitMode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                sqlExecutionContext.setJitMode(jitMode);
                bindVariableService.clear();
                bindVariableService.setStr("p", tick);
                for (String column : new String[]{"ts", "tm"}) {
                    final String literalSql = "SELECT k FROM x WHERE " + column + " IN '" + tick + "'";
                    assertQuery(literalSql).noLeakCheck().returns(expected);
                    assertUsesCompiledFilter(literalSql, isJitExpected && "tm".equals(column));
                    final String bindSql = "SELECT k FROM x WHERE " + column + " IN :p";
                    assertQuery(bindSql).noLeakCheck().returns(expected);
                    assertUsesCompiledFilter(bindSql, false);
                }
            }
        } finally {
            sqlExecutionContext.setJitMode(callerJitMode);
        }
    }

    private void assertOrInBothOrders(String staticPredicate, String runtimePredicate, String expected) throws Exception {
        // the OR leaf written first reaches the interval model builder first
        assertQuery("SELECT k FROM x WHERE " + runtimePredicate + " OR " + staticPredicate).noLeakCheck().returns(expected);
        assertQuery("SELECT k FROM x WHERE " + staticPredicate + " OR " + runtimePredicate).noLeakCheck().returns(expected);
    }

    private void createDayFilterTable() throws Exception {
        executeWithRewriteTimestamp("CREATE TABLE x (k INT, ts #TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", timestampType.getTypeName());
        execute("""
                INSERT INTO x VALUES
                (1, '2024-01-05T12:00:00.000000Z'),
                (2, '2024-01-06T12:00:00.000000Z'),
                (3, '2024-01-08T12:00:00.000000Z'),
                (4, '2024-01-09T04:30:00.000000Z'),
                (5, '2024-09-01T12:00:00.000000Z'),
                (6, '2024-09-02T12:00:00.000000Z')
                """);
        // Monday
        setCurrentMicros(Micros.toMicros(2024, 1, 8, 1, 0));
    }

    // IntervalUtils reads the cap through this getter in an assertion on entry and wherever it
    // checks an interval count. Records the largest list, counted from its start, at such a read.
    private static class ListSizeAtCapCheckConfiguration extends DefaultCairoConfiguration {
        private final LongList intervals;
        private int maxListIntervalCount;

        ListSizeAtCapCheckConfiguration(LongList intervals) {
            super(".");
            this.intervals = intervals;
        }

        @Override
        public int getSqlIntervalMaxIntervalsAfterMerge() {
            maxListIntervalCount = Math.max(maxListIntervalCount, intervals.size() / 2);
            return super.getSqlIntervalMaxIntervalsAfterMerge();
        }
    }
}
