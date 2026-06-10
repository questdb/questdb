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

import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
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
                    "inconvertible value: `3 day` [STRING -> TIMESTAMP_NS]"
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
                    "inconvertible value: `3 day` [STRING -> TIMESTAMP_NS]"
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
}
