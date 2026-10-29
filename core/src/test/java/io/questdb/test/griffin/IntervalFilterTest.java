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
import io.questdb.cairo.TickCalendarServiceFactory;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.cairo.TestTickCalendarService;
import org.jetbrains.annotations.NotNull;
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
            assertInDayFilterWithRuntimeInterval(dayFilterPredicate, "ts > dateadd('y', -30, now())", mondays);
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
            assertInDayFilterWithRuntimeInterval("ts IN '2024-[01..12]-[01..28]#mon'", runtimePredicate, mondays);
            assertInDayFilterWithRuntimeInterval("ts IN '2024-[01..12]-[01..28]#mon'", "ts NOT IN :i", mondays);
            assertInDayFilterWithRuntimeInterval(
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
            assertInDayFilterWithRuntimeInterval(
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
            assertInDayFilterWithRuntimeInterval("ts IN '2024-[01..12]-[01..28]@+05:00#mon'", runtimePredicate, mondays);
            // a single-element date list and a per-element filter
            assertInDayFilterWithRuntimeInterval("ts IN '[2024-[01..12]-[01..28]]#mon'", runtimePredicate, mondays);
            assertInDayFilterWithRuntimeInterval(
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
            assertInDayFilterWithRuntimeInterval("ts IN '2024-[01..12]-[01..28]T[00:00,18:00]#mon;6h'", runtimePredicate, "k\n");
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
            assertInDayFilterWithRuntimeInterval("ts IN '2024-[01..09]-[01..28]#mon'", "ts > dateadd('y', -10, now())", mondays);
            assertQuery("SELECT k FROM x WHERE ts IN '$today' OR ts IN '2024-[01..09]-[01..28]#mon'").noLeakCheck().returns(mondays);
            assertInDayFilterWithRuntimeInterval("ts IN '2024-[01..04]-[01..28]T[00:00,18:00]#mon;6h'", "ts > dateadd('y', -10, now())", "k\n");
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
            assertInDayFilterWithRuntimeInterval(
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
            assertInDayFilterWithRuntimeInterval("ts IN " + atCap, runtimePredicate, mondays);
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

            assertInDayFilterWithRuntimeInterval("ts IN '2024-01-06#workday'", "ts IN :d6", "k\n");
            assertInDayFilterWithRuntimeInterval("ts IN '2024-01-06#workday'", "ts < now()", "k\n");
            assertInDayFilterWithRuntimeInterval("ts IN '2024-01-06#workday'", "ts > dateadd('y', -10, now())", "k\n");
            assertInDayFilterWithRuntimeInterval("ts IN '2024-01-06#workday'", "ts IN '$today'", "k\n");
            assertInDayFilterWithRuntimeInterval("ts IN '2024-01-06#workday'", "ts NOT IN :d8", "k\n");
            assertInDayFilterWithRuntimeInterval("ts IN '2024-01-06#workday'", "ts NOT IN :i", "k\n");
            assertInDayFilterWithRuntimeInterval("ts IN '2024-01-06#workday'", "ts NOT IN '$today#weekend'", "k\n");
            assertInDayFilterWithRuntimeInterval("ts IN '2024-01-[06,07]#workday'", "ts > dateadd('y', -10, now())", "k\n");
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
            assertInDayFilterWithRuntimeInterval("ts IN '2024-09#workday'", "ts NOT IN :i", "k\n6\n");
            assertInDayFilterWithRuntimeInterval("ts IN '2024-09#workday'", "ts > dateadd('y', -10, now())", "k\n6\n");
            // Saturday, Sunday and Monday repetitions; only Monday's hour matches
            assertInDayFilterWithRuntimeInterval("ts IN '2024-01-06T12:00#workday;1h;1d;3'", "ts NOT IN :i", "k\n3\n");
            assertInDayFilterWithRuntimeInterval("ts IN '2024-01-06T12:00#workday;1h;1d;3'", "ts > dateadd('y', -10, now())", "k\n3\n");
            // 2024-01-01 is a Monday, but the weekend inside January must not match
            assertInDayFilterWithRuntimeInterval("ts IN '2024-01#workday'", "ts NOT IN :i", "k\n1\n3\n4\n");
            assertInDayFilterWithRuntimeInterval("ts IN '2024-01#workday'", "ts > dateadd('y', -10, now())", "k\n1\n3\n4\n");
            // the Saturday bracket value holds no interval, the Monday one does
            assertInDayFilterWithRuntimeInterval("ts IN '2024-01-[06,08]#workday'", "ts NOT IN :i", "k\n3\n");
            assertInDayFilterWithRuntimeInterval("ts IN '2024-01-[06,08]#workday'", "ts > dateadd('y', -10, now())", "k\n3\n");
            // Monday 23:00 at -05:00 starts on Tuesday 04:00 UTC
            assertInDayFilterWithRuntimeInterval("ts IN '2024-01-08T23:00@-05:00#mon;2h'", "ts NOT IN :i", "k\n4\n");
            assertInDayFilterWithRuntimeInterval("ts IN '2024-01-08T23:00@-05:00#mon;2h'", "ts > dateadd('y', -10, now())", "k\n4\n");
            // the Saturday list element carries its own filter and holds no interval
            assertInDayFilterWithRuntimeInterval("ts IN '[2024-01-06#workday,2024-01-08]'", "ts NOT IN :i", "k\n3\n");
        });
    }

    @Test
    public void testInDayFilterWithRuntimeOr() throws Exception {
        // The OR of a run-time and a static interval reaches the interval model builder before
        // the day-filtered IN, so the day-filtered IN must intersect the whole OR.
        assertMemoryLeak(() -> {
            createDayFilterTable();
            // 2024-01-06 is a Saturday, so '2024-01-06#workday' holds no interval
            assertInDayFilterWithRuntimeInterval("ts IN '2024-01-06#workday'", "(ts IN '$today' OR ts IN '2024-01-06')", "k\n");
            // only the Monday of the Saturday-to-Monday range is a workday, and $today is that Monday
            assertInDayFilterWithRuntimeInterval("ts IN '2024-01-[06..08]#workday'", "(ts IN '$today' OR ts IN '2024-01-06')", "k\n3\n");
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
                assertInDayFilterWithRuntimeInterval("ts IN '2025-01-24#XNYS'", runtimePredicate, fridaySession);
                assertInDayFilterWithRuntimeInterval("ts IN '2025-01-24T15:00#XNYS;1h'", runtimePredicate, fridaySession);
                assertInDayFilterWithRuntimeInterval("ts IN '2025-01-[24..27]#XNYS'", runtimePredicate, fridayAndMondaySessions);
                assertInDayFilterWithRuntimeInterval(
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
                assertInDayFilterWithRuntimeInterval(
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
            assertInDayFilterWithRuntimeInterval(
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
            assertInDayFilterWithRuntimeInterval(
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

    private void assertInDayFilterWithRuntimeInterval(String dayFilterPredicate, String runtimePredicate, String expected) throws Exception {
        // the predicate written last reaches the interval model builder first
        assertQuery("SELECT k FROM x WHERE " + runtimePredicate + " AND " + dayFilterPredicate).noLeakCheck().returns(expected);
        assertQuery("SELECT k FROM x WHERE " + dayFilterPredicate + " AND " + runtimePredicate).noLeakCheck().returns(expected);
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
