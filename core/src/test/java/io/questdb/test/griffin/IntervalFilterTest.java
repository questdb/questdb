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
import io.questdb.cairo.TickCalendarServiceFactory;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.model.IntervalOperation;
import io.questdb.griffin.model.IntervalUtils;
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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

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
            for (int i = 0; i < 3_000; i++) {
                final String tick = randomTickString(rnd, tickSink, true);
                expected.clear();
                actual.clear();
                IntervalUtils.parseTickExpr(timestampDriver, unmergedConfiguration, tick, 0, tick.length(), 0, expected, IntervalOperation.INTERSECT, sink, true);
                IntervalUtils.parseTickExpr(timestampDriver, configuration, tick, 0, tick.length(), 0, actual, IntervalOperation.INTERSECT, sink, true);
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
            for (int i = 0; i < 300; i++) {
                final long now = Micros.toMicros(2023, 12, 28, 0, 0) + rnd.nextLong(25 * Micros.DAY_MICROS);
                setCurrentMicros(now);
                final String bindValue = randomTickString(rnd, tickSink, true);
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

    private static void putRandomPredicate(Rnd rnd, StringSink sink, StringSink tickSink) {
        // %c stands for the timestamp column
        switch (rnd.nextInt(12)) {
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
                final String tick = "%c IN '" + randomTickString(rnd, tickSink, isRuntimeFirst) + '\'';
                sink.put('(').put(isRuntimeFirst ? runtime : tick).put(" OR ").put(isRuntimeFirst ? tick : runtime).put(')');
            }
            case 9 -> {
                final String tick = randomTickString(rnd, tickSink, true);
                sink.put("%c NOT IN '").put(tick).put('\'');
            }
            default -> {
                final String tick = randomTickString(rnd, tickSink, true);
                sink.put("%c IN '").put(tick).put('\'');
            }
        }
    }

    // A tick string over the days of January 2024 that both the interval scan and the row filter
    // resolve: lists, ranges, times, durations, repetitions, fixed offsets and day filters.
    private static String randomTickString(Rnd rnd, StringSink sink, boolean canHaveDayFilter) {
        sink.clear();
        final int day = 1 + rnd.nextInt(20);
        boolean hasTime = rnd.nextBoolean();
        int listDurationMinutes = 0;
        switch (rnd.nextInt(5)) {
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
            default -> {
                sink.put("[2024-01-").put(twoDigits(day));
                for (int i = 0, n = 1 + rnd.nextInt(3); i < n; i++) {
                    sink.put(",2024-01-").put(twoDigits(1 + rnd.nextInt(20)));
                }
                sink.put(']');
            }
        }
        if (hasTime) {
            sink.put('T').put(twoDigits(rnd.nextInt(24))).put(":00");
        }
        switch (rnd.nextInt(4)) {
            case 0 -> sink.put("@+05:00");
            case 1 -> sink.put("@-03:30");
            default -> {
            }
        }
        if (canHaveDayFilter) {
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
                case 1 -> sink.put(';').put(1 + rnd.nextInt(5)).put("h;1d;").put(1 + rnd.nextInt(4));
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
}
