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

            String expected = replaceTimestampSuffix("a\tts\n" +
                    "8.486964232560668\t1970-01-01T00:00:01.000000Z\n" +
                    "8.43832076262595\t1970-01-01T00:00:02.000000Z\n", timestampType.getTypeName());
            assertSql(
                    expected,
                    "select * from x where ts between '1970-01-01T00:00:01.000000Z' and '1970-01-01T00:00:02.000000Z'"
            );
            assertSql(
                    expected,
                    "select * from x where ts between '1970-01-01T00:00:01.000000000Z' and '1970-01-01T00:00:02.000000000Z'"
            );
            bindVariableService.clear();
            bindVariableService.setStr("ts1", "1970-01-01T00:00:01.000000Z");
            bindVariableService.setStr("ts2", "1970-01-01T00:00:02.000000Z");
            bindVariableService.setStr("ts3", "1970-01-01T00:00:01.000000000Z");
            bindVariableService.setStr("ts4", "1970-01-01T00:00:02.000000000Z");
            assertSql(
                    expected,
                    "select * from x where ts between :ts1 and :ts2"
            );
            assertSql(
                    expected,
                    "select * from x where ts between :ts3 and :ts4"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts between '1970-01-01T00:00:01.000000Z' and '1970-01-01T00:00:02.000000Z'",
                    replaceTimestampSuffix("PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            "      intervals: [(\"1970-01-01T00:00:01.000000Z\",\"1970-01-01T00:00:02.000000Z\")]\n", timestampType.getTypeName())
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts between '1970-01-01T00:00:01.000000000Z' and '1970-01-01T00:00:02.000000000Z'",
                    replaceTimestampSuffix("PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            "      intervals: [(\"1970-01-01T00:00:01.000000Z\",\"1970-01-01T00:00:02.000000Z\")]\n", timestampType.getTypeName())
            );
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

            String expected = replaceTimestampSuffix1("a\tts\n" +
                    "8.486964232560668\t1970-01-01T00:00:00.010000Z\n", timestampType.getTypeName());
            assertSql(
                    expected,
                    "select * from x where ts = '1970-01-01T00:00:00.010000Z'"
            );
            assertSql(
                    expected,
                    "select * from x where ts = '1970-01-01T00:00:00.010000000Z'"
            );
            bindVariableService.clear();
            bindVariableService.setStr("ts", "1970-01-01T00:00:00.010000Z");
            bindVariableService.setStr("ts1", "1970-01-01T00:00:00.010000000Z");
            assertSql(
                    expected,
                    "select * from x where ts = :ts"
            );
            assertSql(
                    expected,
                    "select * from x where ts = :ts1"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts = '1970-01-01T00:00:00.010000Z'",
                    replaceTimestampSuffix1("PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            "      intervals: [(\"1970-01-01T00:00:00.010000Z\",\"1970-01-01T00:00:00.010000Z\")]\n", timestampType.getTypeName())
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts = '1970-01-01T00:00:00.010000000Z'",
                    replaceTimestampSuffix1("PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            "      intervals: [(\"1970-01-01T00:00:00.010000Z\",\"1970-01-01T00:00:00.010000Z\")]\n", timestampType.getTypeName())
            );

            expected = replaceTimestampSuffix1("a\tts\n" +
                    "80.43224099968394\t1970-01-01T00:00:00.000000Z\n" +
                    "8.43832076262595\t1970-01-01T00:00:00.020000Z\n", timestampType.getTypeName());
            assertSql(
                    expected,
                    "select * from x where ts != '1970-01-01T00:00:00.010000Z'"
            );
            assertSql(
                    expected,
                    "select * from x where ts != '1970-01-01T00:00:00.010000000Z'"
            );
            bindVariableService.clear();
            bindVariableService.setStr("ts", "1970-01-01T00:00:00.010000Z");
            bindVariableService.setStr("ts1", "1970-01-01T00:00:00.010000000Z");
            assertSql(
                    expected,
                    "select * from x where ts != :ts"
            );
            assertSql(
                    expected,
                    "select * from x where ts != :ts1"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts != '1970-01-01T00:00:00.010000Z'",
                    "PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"MIN\",\"1970-01-01T00:00:00.009999Z\"),(\"1970-01-01T00:00:00.010001Z\",\"MAX\")]\n" :
                                    "      intervals: [(\"MIN\",\"1970-01-01T00:00:00.009999999Z\"),(\"1970-01-01T00:00:00.010000001Z\",\"MAX\")]\n")
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts != '1970-01-01T00:00:00.010000000Z'",
                    "PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"MIN\",\"1970-01-01T00:00:00.009999Z\"),(\"1970-01-01T00:00:00.010001Z\",\"MAX\")]\n" :
                                    "      intervals: [(\"MIN\",\"1970-01-01T00:00:00.009999999Z\"),(\"1970-01-01T00:00:00.010000001Z\",\"MAX\")]\n")
            );
            expected = replaceTimestampSuffix1("a\tts\n" +
                    "80.43224099968394\t1970-01-01T00:00:00.000000Z\n" +
                    "8.43832076262595\t1970-01-01T00:00:00.020000Z\n", timestampType.getTypeName());
            assertSql(
                    expected,
                    "select * from x where ts in '1970-01-01' and ts != '1970-01-01T00:00:00.010000Z'"
            );
            assertSql(
                    expected,
                    "select * from x where ts in '1970-01-01' and ts != '1970-01-01T00:00:00.010000000Z'"
            );
            bindVariableService.clear();
            bindVariableService.setStr("date", "1970-01-01");
            bindVariableService.setStr("ts", "1970-01-01T00:00:00.010000Z");
            bindVariableService.setStr("ts1", "1970-01-01T00:00:00.010000000Z");
            assertSql(
                    expected,
                    "select * from x where ts in :date and ts != :ts"
            );
            assertSql(
                    expected,
                    "select * from x where ts in :date and ts != :ts1"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts in '1970-01-01' and ts != '1970-01-01T00:00:00.010000Z'",
                    "PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"1970-01-01T00:00:00.000000Z\",\"1970-01-01T00:00:00.009999Z\"),(\"1970-01-01T00:00:00.010001Z\",\"1970-01-01T23:59:59.999999Z\")]\n" :
                                    "      intervals: [(\"1970-01-01T00:00:00.000000000Z\",\"1970-01-01T00:00:00.009999999Z\"),(\"1970-01-01T00:00:00.010000001Z\",\"1970-01-01T23:59:59.999999999Z\")]\n")
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts in '1970-01-01' and ts != '1970-01-01T00:00:00.010000000Z'",
                    "PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"1970-01-01T00:00:00.000000Z\",\"1970-01-01T00:00:00.009999Z\"),(\"1970-01-01T00:00:00.010001Z\",\"1970-01-01T23:59:59.999999Z\")]\n" :
                                    "      intervals: [(\"1970-01-01T00:00:00.000000000Z\",\"1970-01-01T00:00:00.009999999Z\"),(\"1970-01-01T00:00:00.010000001Z\",\"1970-01-01T23:59:59.999999999Z\")]\n")
            );

            expected = "a\tts\n";
            assertSql(
                    expected,
                    "select * from x where ts = '1970-01-01T00:00:00.010000Z' and ts = '1970-01-01T00:00:00.020000Z' and ts = '1970-01-01T00:00:00.030000Z'"
            );
            assertSql(
                    expected,
                    "select * from x where ts = '1970-01-01T00:00:00.010000000Z' and ts = '1970-01-01T00:00:00.020000000Z' and ts = '1970-01-01T00:00:00.030000000Z'"
            );
            bindVariableService.clear();
            bindVariableService.setStr("ts", "1970-01-01T00:00:00.010000Z");
            bindVariableService.setStr("ts1", "1970-01-01T00:00:00.010000Z");
            assertSql(
                    expected,
                    "select * from x where ts = :ts and ts = '1970-01-01T00:00:00.020000Z' and ts = '1970-01-01T00:00:00.030000Z'"
            );
            assertSql(
                    expected,
                    "select * from x where ts = :ts1 and ts = '1970-01-01T00:00:00.020000000Z' and ts = '1970-01-01T00:00:00.030000000Z'"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts = '1970-01-01T00:00:00.010000Z' and ts = '1970-01-01T00:00:00.020000Z' and ts = '1970-01-01T00:00:00.030000Z'",
                    "Empty table\n"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts = '1970-01-01T00:00:00.010000000Z' and ts = '1970-01-01T00:00:00.020000000Z' and ts = '1970-01-01T00:00:00.030000000Z'",
                    "Empty table\n"
            );

            expected = "a\tts\n";
            assertSql(
                    expected,
                    "select * from x where ts != '1970-01-01T00:00:00.010000Z' and ts = '1970-01-01T00:00:00.020000Z' and ts = '1970-01-01T00:00:00.030000Z'"
            );
            assertSql(
                    expected,
                    "select * from x where ts != '1970-01-01T00:00:00.010000000Z' and ts = '1970-01-01T00:00:00.020000000Z' and ts = '1970-01-01T00:00:00.030000000Z'"
            );
            bindVariableService.clear();
            bindVariableService.setStr("ts", "1970-01-01T00:00:00.010000Z");
            bindVariableService.setStr("ts1", "1970-01-01T00:00:00.010000000Z");
            assertSql(
                    expected,
                    "select * from x where ts != :ts and ts = '1970-01-01T00:00:00.020000Z' and ts = '1970-01-01T00:00:00.030000Z'"
            );
            assertSql(
                    expected,
                    "select * from x where ts != :ts1 and ts = '1970-01-01T00:00:00.020000000Z' and ts = '1970-01-01T00:00:00.030000000Z'"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts != '1970-01-01T00:00:00.010000Z' and ts = '1970-01-01T00:00:00.020000Z' and ts = '1970-01-01T00:00:00.030000Z'",
                    "Empty table\n"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts != '1970-01-01T00:00:00.010000000Z' and ts = '1970-01-01T00:00:00.020000000Z' and ts = '1970-01-01T00:00:00.030000000Z'",
                    "Empty table\n"
            );

            assertSql(
                    "a\tts\n",
                    "select * from x where ts = (-9223372036854775808)::timestamp"
            );
            assertSql(
                    "a\tts\n",
                    "select * from x where ts = (-9223372036854775808)::timestamp_ns"
            );
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

            assertSql(
                    replaceTimestampSuffix1("a\tts\n" +
                            "8.43832076262595\t1970-01-01T00:00:00.020000Z\n", timestampType.getTypeName()),
                    "select * from x where ts = (select max(ts) from x)"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts = (select max(ts) from x)",
                    replaceTimestampSuffix1("PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            "      intervals: [(\"1970-01-01T00:00:00.020000Z\",\"1970-01-01T00:00:00.020000Z\")]\n", timestampType.getTypeName())
            );
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

            String expected = replaceTimestampSuffix1("a\tts\n" +
                    "8.43832076262595\t1970-01-01T00:00:00.020000Z\n", timestampType.getTypeName());
            assertSql(
                    expected,
                    "select * from x where ts > '1970-01-01T00:00:00.010000Z'"
            );
            assertSql(
                    expected,
                    "select * from x where ts > '1970-01-01T00:00:00.010000000Z'"
            );
            bindVariableService.clear();
            bindVariableService.setStr("ts", "1970-01-01T00:00:00.010000Z");
            bindVariableService.setStr("ts1", "1970-01-01T00:00:00.010000000Z");
            assertSql(
                    expected,
                    "select * from x where ts > :ts"
            );
            assertSql(
                    expected,
                    "select * from x where ts > :ts1"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts > '1970-01-01T00:00:00.010000Z'",
                    "PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"1970-01-01T00:00:00.010001Z\",\"MAX\")]\n" :
                                    "      intervals: [(\"1970-01-01T00:00:00.010000001Z\",\"MAX\")]\n")

            );
            assertPlanNoLeakCheck(
                    "select * from x where ts > '1970-01-01T00:00:00.010000000Z'",
                    "PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"1970-01-01T00:00:00.010001Z\",\"MAX\")]\n" :
                                    "      intervals: [(\"1970-01-01T00:00:00.010000001Z\",\"MAX\")]\n")

            );

            expected = replaceTimestampSuffix1("a\tts\n" +
                    "80.43224099968394\t1970-01-01T00:00:00.000000Z\n" +
                    "8.486964232560668\t1970-01-01T00:00:00.010000Z\n", timestampType.getTypeName());
            assertSql(
                    expected,
                    "select * from x where ts <= '1970-01-01T00:00:00.010000Z'"
            );
            assertSql(
                    expected,
                    "select * from x where ts <= '1970-01-01T00:00:00.010000000Z'"
            );
            bindVariableService.clear();
            bindVariableService.setStr("ts", "1970-01-01T00:00:00.010000Z");
            bindVariableService.setStr("ts1", "1970-01-01T00:00:00.010000000Z");
            assertSql(
                    expected,
                    "select * from x where ts <= :ts"
            );
            assertSql(
                    expected,
                    "select * from x where ts <= :ts1"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts <= '1970-01-01T00:00:00.010000Z'",
                    replaceTimestampSuffix1("PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            "      intervals: [(\"MIN\",\"1970-01-01T00:00:00.010000Z\")]\n", timestampType.getTypeName())
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts <= '1970-01-01T00:00:00.010000000Z'",
                    replaceTimestampSuffix1("PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            "      intervals: [(\"MIN\",\"1970-01-01T00:00:00.010000Z\")]\n", timestampType.getTypeName())
            );

            expected = replaceTimestampSuffix1("a\tts\n" +
                    "80.43224099968394\t1970-01-01T00:00:00.000000Z\n" +
                    "8.486964232560668\t1970-01-01T00:00:00.010000Z\n", timestampType.getTypeName());
            assertSql(
                    expected,
                    "select * from x where ts in '1970-01-01' and ts <= '1970-01-01T00:00:00.010000Z'"
            );
            assertSql(
                    expected,
                    "select * from x where ts in '1970-01-01' and ts <= '1970-01-01T00:00:00.010000000Z'"
            );
            bindVariableService.clear();
            bindVariableService.setStr("date", "1970-01-01");
            bindVariableService.setStr("ts", "1970-01-01T00:00:00.010000Z");
            bindVariableService.setStr("ts1", "1970-01-01T00:00:00.010000000Z");
            assertSql(
                    expected,
                    "select * from x where ts in :date and ts <= :ts"
            );
            assertSql(
                    expected,
                    "select * from x where ts in :date and ts <= :ts1"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts in '1970-01-01' and ts <= '1970-01-01T00:00:00.010000Z'",
                    replaceTimestampSuffix1("PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            "      intervals: [(\"1970-01-01T00:00:00.000000Z\",\"1970-01-01T00:00:00.010000Z\")]\n", timestampType.getTypeName())
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts in '1970-01-01' and ts <= '1970-01-01T00:00:00.010000000Z'",
                    replaceTimestampSuffix1("PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            "      intervals: [(\"1970-01-01T00:00:00.000000Z\",\"1970-01-01T00:00:00.010000Z\")]\n", timestampType.getTypeName())
            );

            expected = "a\tts\n";
            assertSql(
                    expected,
                    "select * from x where ts > '1970-01-01T00:00:00.010000Z' and ts in '1970-01-01T00:00:01' and ts in '1970-01-01T00:00:02'"
            );
            assertSql(
                    expected,
                    "select * from x where ts > '1970-01-01T00:00:00.010000000Z' and ts in '1970-01-01T00:00:01' and ts in '1970-01-01T00:00:02'"
            );
            bindVariableService.clear();
            bindVariableService.setStr("ts", "1970-01-01T00:00:00.010000Z");
            bindVariableService.setStr("ts1", "1970-01-01T00:00:00.010000000Z");
            assertSql(
                    expected,
                    "select * from x where ts > :ts and ts in '1970-01-01T00:00:01' and ts in '1970-01-01T00:00:02'"
            );
            assertSql(
                    expected,
                    "select * from x where ts > :ts1 and ts in '1970-01-01T00:00:01' and ts in '1970-01-01T00:00:02'"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts > '1970-01-01T00:00:00.010000Z' and ts in '1970-01-01T00:00:01' and ts in '1970-01-01T00:00:02'",
                    "Empty table\n"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts > '1970-01-01T00:00:00.010000000Z' and ts in '1970-01-01T00:00:01' and ts in '1970-01-01T00:00:02'",
                    "Empty table\n"
            );
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

            assertSql(
                    replaceTimestampSuffix1("a\tts\n" +
                            "8.486964232560668\t1970-01-01T00:00:00.010000Z\n" +
                            "8.43832076262595\t1970-01-01T00:00:00.020000Z\n", timestampType.getTypeName()),
                    "select * from x where ts > (select min(ts) from x)"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts > (select min(ts) from x)",
                    "PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"1970-01-01T00:00:00.000001Z\",\"MAX\")]\n" :
                                    "      intervals: [(\"1970-01-01T00:00:00.000000001Z\",\"MAX\")]\n")
            );

            assertSql(
                    replaceTimestampSuffix1("a\tts\n" +
                            "80.43224099968394\t1970-01-01T00:00:00.000000Z\n" +
                            "8.486964232560668\t1970-01-01T00:00:00.010000Z\n" +
                            "8.43832076262595\t1970-01-01T00:00:00.020000Z\n", timestampType.getTypeName()),
                    "select * from x where ts >= (select min(ts) from x)"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts >= (select min(ts) from x)",
                    replaceTimestampSuffix1("PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            "      intervals: [(\"1970-01-01T00:00:00.000000Z\",\"MAX\")]\n", timestampType.getTypeName())
            );
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

            String expected = replaceTimestampSuffix("a\tts\n" +
                    "8.486964232560668\t1970-01-01T00:00:01.000000Z\n", timestampType.getTypeName());
            assertSql(
                    expected,
                    "select * from x where ts in '1970-01-01T00:00:01'"
            );
            bindVariableService.clear();
            bindVariableService.setStr("i", "1970-01-01T00:00:01");
            assertSql(
                    expected,
                    "select * from x where ts in :i"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts in '1970-01-01T00:00:01'",
                    "PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"1970-01-01T00:00:01.000000Z\",\"1970-01-01T00:00:01.999999Z\")]\n" :
                                    "      intervals: [(\"1970-01-01T00:00:01.000000000Z\",\"1970-01-01T00:00:01.999999999Z\")]\n")
            );

            expected = replaceTimestampSuffix1("a\tts\n" +
                    "80.43224099968394\t1970-01-01T00:00:00.000000Z\n" +
                    "8.43832076262595\t1970-01-01T00:00:02.000000Z\n", timestampType.getTypeName());
            assertSql(
                    expected,
                    "select * from x where ts not in '1970-01-01T00:00:01'"
            );
            bindVariableService.clear();
            bindVariableService.setStr("i", "1970-01-01T00:00:01");
            assertSql(
                    expected,
                    "select * from x where ts not in :i"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts not in '1970-01-01T00:00:01'",
                    "PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"MIN\",\"1970-01-01T00:00:00.999999Z\"),(\"1970-01-01T00:00:02.000000Z\",\"MAX\")]\n" :
                                    "      intervals: [(\"MIN\",\"1970-01-01T00:00:00.999999999Z\"),(\"1970-01-01T00:00:02.000000000Z\",\"MAX\")]\n")
            );

            expected = replaceTimestampSuffix1("a\tts\n" +
                    "80.43224099968394\t1970-01-01T00:00:00.000000Z\n" +
                    "8.43832076262595\t1970-01-01T00:00:02.000000Z\n", timestampType.getTypeName());
            assertSql(
                    expected,
                    "select * from x where ts in '1970-01-01' and ts not in '1970-01-01T00:00:01'"
            );
            bindVariableService.clear();
            bindVariableService.setStr("d", "1970-01-01");
            bindVariableService.setStr("i", "1970-01-01T00:00:01");
            assertSql(
                    expected,
                    "select * from x where ts in :d and ts not in :i"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts in '1970-01-01' and ts not in '1970-01-01T00:00:01'",
                    "PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"1970-01-01T00:00:00.000000Z\",\"1970-01-01T00:00:00.999999Z\"),(\"1970-01-01T00:00:02.000000Z\",\"1970-01-01T23:59:59.999999Z\")]\n" :
                                    "      intervals: [(\"1970-01-01T00:00:00.000000000Z\",\"1970-01-01T00:00:00.999999999Z\"),(\"1970-01-01T00:00:02.000000000Z\",\"1970-01-01T23:59:59.999999999Z\")]\n")
            );

            expected = "a\tts\n";
            assertSql(
                    expected,
                    "select * from x where ts in '1970-01-01T00:00:01' and ts in '1970-01-01T00:00:02' and ts in '1970-01-01T00:00:03'"
            );
            bindVariableService.clear();
            bindVariableService.setStr("i", "1970-01-01T00:00:01");
            assertSql(
                    expected,
                    "select * from x where ts in :i and ts in '1970-01-01T00:00:02' and ts in '1970-01-01T00:00:03'"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts in '1970-01-01T00:00:01' and ts in '1970-01-01T00:00:02' and ts in '1970-01-01T00:00:03'",
                    "Empty table\n"
            );

            expected = "a\tts\n";
            assertSql(
                    expected,
                    "select * from x where ts not in '1970-01-01T00:00:01' and ts in '1970-01-01T00:00:02' and ts in '1970-01-01T00:00:03'"
            );
            bindVariableService.clear();
            bindVariableService.setStr("i", "1970-01-01T00:00:01");
            assertSql(
                    expected,
                    "select * from x where ts not in :i and ts in '1970-01-01T00:00:02' and ts in '1970-01-01T00:00:03'"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts not in '1970-01-01T00:00:01' and ts in '1970-01-01T00:00:02' and ts in '1970-01-01T00:00:03'",
                    "Empty table\n"
            );

            expected = replaceTimestampSuffix1("a\tts\n" +
                    "8.486964232560668\t1970-01-01T00:00:01.000000Z\n", timestampType.getTypeName());
            assertSql(
                    expected,
                    "select * from x where ts in '1970-01-01T00:00' and ts in '1970-01-01T00:00:01'"
            );
            bindVariableService.clear();
            bindVariableService.setStr("i1", "1970-01-01T00:00");
            bindVariableService.setStr("i2", "1970-01-01T00:00:01");
            assertSql(
                    expected,
                    "select * from x where ts in :i1 and ts in :i2"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts in '1970-01-01T00:00' and ts in '1970-01-01T00:00:01'",
                    "PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"1970-01-01T00:00:01.000000Z\",\"1970-01-01T00:00:01.999999Z\")]\n" :
                                    "      intervals: [(\"1970-01-01T00:00:01.000000000Z\",\"1970-01-01T00:00:01.999999999Z\")]\n")
            );

            assertSql(
                    replaceTimestampSuffix1("a\tts\n" +
                            "8.486964232560668\t1970-01-01T00:00:01.000000Z\n" +
                            "8.43832076262595\t1970-01-01T00:00:02.000000Z\n", timestampType.getTypeName()),
                    "select * from x where ts in ('1970-01-01T00:00:01', '1970-01-01T00:00:02')"
            );

            assertSql(
                    replaceTimestampSuffix1("a\tts\n" +
                            "8.486964232560668\t1970-01-01T00:00:01.000000Z\n" +
                            "8.43832076262595\t1970-01-01T00:00:02.000000Z\n", timestampType.getTypeName()),
                    "select * from x where ts in '1970-01-01T00:00:01' or ts in '1970-01-01T00:00:02'"
            );

            assertSql(
                    "a\tts\n",
                    "select * from x where ts in null"
            );

            assertSql(
                    replaceTimestampSuffix1("a\tts\n" +
                            "80.43224099968394\t1970-01-01T00:00:00.000000Z\n" +
                            "8.486964232560668\t1970-01-01T00:00:01.000000Z\n" +
                            "8.43832076262595\t1970-01-01T00:00:02.000000Z\n", timestampType.getTypeName()),
                    "select * from x where ts not in null"
            );

            assertSql(
                    "a\tts\n",
                    "select * from x where ts not in null and ts in '1970-01-01T00:00:01' and ts in '1970-01-01T00:00:02'"
            );
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

            String expected = replaceTimestampSuffix1("a\tts\n" +
                    "8.486964232560668\t1970-01-01T00:00:00.110000Z\n", timestampType.getTypeName());

            assertSql(
                    expected,
                    "select * from x where ts in '1970-01-01T00:00:00.1'"
            );
            bindVariableService.clear();
            bindVariableService.setStr("i", "1970-01-01T00:00:00.1");
            assertSql(
                    expected,
                    "select * from x where ts in :i"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts in '1970-01-01T00:00:00.1'",
                    "PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"1970-01-01T00:00:00.100000Z\",\"1970-01-01T00:00:00.199999Z\")]\n" :
                                    "      intervals: [(\"1970-01-01T00:00:00.100000000Z\",\"1970-01-01T00:00:00.199999999Z\")]\n")
            );

            expected = replaceTimestampSuffix1("a\tts\n" +
                    "80.43224099968394\t1970-01-01T00:00:00.000000Z\n" +
                    "8.43832076262595\t1970-01-01T00:00:00.220000Z\n", timestampType.getTypeName());
            assertSql(
                    expected,
                    "select * from x where ts not in '1970-01-01T00:00:00.1'"
            );
            bindVariableService.clear();
            bindVariableService.setStr("i", "1970-01-01T00:00:00.1");
            assertSql(
                    expected,
                    "select * from x where ts not in :i"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts not in '1970-01-01T00:00:00.1'",
                    "PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"MIN\",\"1970-01-01T00:00:00.099999Z\"),(\"1970-01-01T00:00:00.200000Z\",\"MAX\")]\n" :
                                    "      intervals: [(\"MIN\",\"1970-01-01T00:00:00.099999999Z\"),(\"1970-01-01T00:00:00.200000000Z\",\"MAX\")]\n")
            );

            expected = replaceTimestampSuffix1("a\tts\n" +
                    "80.43224099968394\t1970-01-01T00:00:00.000000Z\n" +
                    "8.43832076262595\t1970-01-01T00:00:00.220000Z\n", timestampType.getTypeName());
            assertSql(
                    expected,
                    "select * from x where ts in '1970-01-01' and ts not in '1970-01-01T00:00:00.1'"
            );
            bindVariableService.clear();
            bindVariableService.setStr("d", "1970-01-01");
            bindVariableService.setStr("i", "1970-01-01T00:00:00.1");
            assertSql(
                    expected,
                    "select * from x where ts in :d and ts not in :i"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts in '1970-01-01' and ts not in '1970-01-01T00:00:00.1'",
                    "PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"1970-01-01T00:00:00.000000Z\",\"1970-01-01T00:00:00.099999Z\"),(\"1970-01-01T00:00:00.200000Z\",\"1970-01-01T23:59:59.999999Z\")]\n" :
                                    "      intervals: [(\"1970-01-01T00:00:00.000000000Z\",\"1970-01-01T00:00:00.099999999Z\"),(\"1970-01-01T00:00:00.200000000Z\",\"1970-01-01T23:59:59.999999999Z\")]\n")
            );

            expected = "a\tts\n";
            assertSql(
                    expected,
                    "select * from x where ts in '1970-01-01T00:00:00.1' and ts in '1970-01-01T00:00:00.2' and ts in '1970-01-01T00:00:00.3'"
            );
            bindVariableService.clear();
            bindVariableService.setStr("i", "1970-01-01T00:00:00.1");
            assertSql(
                    expected,
                    "select * from x where ts in :i and ts in '1970-01-01T00:00:00.2' and ts in '1970-01-01T00:00:00.3'"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts in '1970-01-01T00:00:00.1' and ts in '1970-01-01T00:00:00.2' and ts in '1970-01-01T00:00:00.3'",
                    "Empty table\n"
            );

            expected = "a\tts\n";
            assertSql(
                    expected,
                    "select * from x where ts not in '1970-01-01T00:00:00.1' and ts in '1970-01-01T00:00:00.2' and ts in '1970-01-01T00:00:00.3'"
            );
            bindVariableService.clear();
            bindVariableService.setStr("i", "1970-01-01T00:00:00.1");
            assertSql(
                    expected,
                    "select * from x where ts not in :i and ts in '1970-01-01T00:00:00.2' and ts in '1970-01-01T00:00:00.3'"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts not in '1970-01-01T00:00:00.1' and ts in '1970-01-01T00:00:00.2' and ts in '1970-01-01T00:00:00.3'",
                    "Empty table\n"
            );

            expected = replaceTimestampSuffix1("a\tts\n" +
                    "8.486964232560668\t1970-01-01T00:00:00.110000Z\n", timestampType.getTypeName());
            assertSql(
                    expected,
                    "select * from x where ts in '1970-01-01T00:00' and ts in '1970-01-01T00:00:00.1'"
            );
            bindVariableService.clear();
            bindVariableService.setStr("i1", "1970-01-01T00:00");
            bindVariableService.setStr("i2", "1970-01-01T00:00:00.1");
            assertSql(
                    expected,
                    "select * from x where ts in :i1 and ts in :i2"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts in '1970-01-01T00:00' and ts in '1970-01-01T00:00:00.1'",
                    "PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"1970-01-01T00:00:00.100000Z\",\"1970-01-01T00:00:00.199999Z\")]\n" :
                                    "      intervals: [(\"1970-01-01T00:00:00.100000000Z\",\"1970-01-01T00:00:00.199999999Z\")]\n")
            );

            assertSql(
                    replaceTimestampSuffix1("a\tts\n" +
                            "8.486964232560668\t1970-01-01T00:00:00.110000Z\n" +
                            "8.43832076262595\t1970-01-01T00:00:00.220000Z\n", timestampType.getTypeName()),
                    "select * from x where ts in ('1970-01-01T00:00:00.11', '1970-01-01T00:00:00.22')"
            );

            assertSql(
                    replaceTimestampSuffix1("a\tts\n" +
                            "8.486964232560668\t1970-01-01T00:00:00.110000Z\n" +
                            "8.43832076262595\t1970-01-01T00:00:00.220000Z\n", timestampType.getTypeName()),
                    "select * from x where ts in '1970-01-01T00:00:00.1' or ts in '1970-01-01T00:00:00.2'"
            );

            assertSql(
                    "a\tts\n",
                    "select * from x where ts not in null and ts in '1970-01-01T00:00:00.1' and ts in '1970-01-01T00:00:00.2'"
            );
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

            String expected = replaceTimestampSuffix1("a\tts\n" +
                    "80.43224099968394\t1970-01-01T00:00:00.000000Z\n", timestampType.getTypeName());
            assertSql(
                    expected,
                    "select * from x where ts < '1970-01-01T00:00:00.010000Z'"
            );
            assertSql(
                    expected,
                    "select * from x where ts < '1970-01-01T00:00:00.010000000Z'"
            );
            bindVariableService.clear();
            bindVariableService.setStr("ts", "1970-01-01T00:00:00.010000Z");
            bindVariableService.setStr("ts1", "1970-01-01T00:00:00.010000Z");
            assertSql(
                    expected,
                    "select * from x where ts < :ts"
            );
            assertSql(
                    expected,
                    "select * from x where ts < :ts1"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts < '1970-01-01T00:00:00.010000Z'",
                    "PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"MIN\",\"1970-01-01T00:00:00.009999Z\")]\n" :
                                    "      intervals: [(\"MIN\",\"1970-01-01T00:00:00.009999999Z\")]\n")
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts < '1970-01-01T00:00:00.010000000Z'",
                    "PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"MIN\",\"1970-01-01T00:00:00.009999Z\")]\n" :
                                    "      intervals: [(\"MIN\",\"1970-01-01T00:00:00.009999999Z\")]\n")
            );

            expected = replaceTimestampSuffix1("a\tts\n" +
                    "8.486964232560668\t1970-01-01T00:00:00.010000Z\n" +
                    "8.43832076262595\t1970-01-01T00:00:00.020000Z\n", timestampType.getTypeName());
            assertSql(
                    expected,
                    "select * from x where ts >= '1970-01-01T00:00:00.010000Z'"
            );
            assertSql(
                    expected,
                    "select * from x where ts >= '1970-01-01T00:00:00.010000000Z'"
            );
            bindVariableService.clear();
            bindVariableService.setStr("ts", "1970-01-01T00:00:00.010000Z");
            bindVariableService.setStr("ts1", "1970-01-01T00:00:00.010000Z");
            assertSql(
                    expected,
                    "select * from x where ts >= :ts"
            );
            assertSql(
                    expected,
                    "select * from x where ts >= :ts1"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts >= '1970-01-01T00:00:00.010000Z'",
                    replaceTimestampSuffix1("PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            "      intervals: [(\"1970-01-01T00:00:00.010000Z\",\"MAX\")]\n", timestampType.getTypeName())
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts >= '1970-01-01T00:00:00.010000000Z'",
                    replaceTimestampSuffix1("PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            "      intervals: [(\"1970-01-01T00:00:00.010000Z\",\"MAX\")]\n", timestampType.getTypeName())
            );

            expected = replaceTimestampSuffix1("a\tts\n" +
                    "8.486964232560668\t1970-01-01T00:00:00.010000Z\n" +
                    "8.43832076262595\t1970-01-01T00:00:00.020000Z\n", timestampType.getTypeName());
            assertSql(
                    expected,
                    "select * from x where ts in '1970-01-01' and ts >= '1970-01-01T00:00:00.010000Z'"
            );
            assertSql(
                    expected,
                    "select * from x where ts in '1970-01-01' and ts >= '1970-01-01T00:00:00.010000000Z'"
            );
            bindVariableService.clear();
            bindVariableService.setStr("date", "1970-01-01");
            bindVariableService.setStr("ts", "1970-01-01T00:00:00.010000Z");
            bindVariableService.setStr("ts1", "1970-01-01T00:00:00.010000000Z");
            assertSql(
                    expected,
                    "select * from x where ts in :date and ts >= :ts"
            );
            assertSql(
                    expected,
                    "select * from x where ts in :date and ts >= :ts1"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts in '1970-01-01' and ts >= '1970-01-01T00:00:00.010000Z'",
                    "PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"1970-01-01T00:00:00.010000Z\",\"1970-01-01T23:59:59.999999Z\")]\n" :
                                    "      intervals: [(\"1970-01-01T00:00:00.010000000Z\",\"1970-01-01T23:59:59.999999999Z\")]\n")

            );
            assertPlanNoLeakCheck(
                    "select * from x where ts in '1970-01-01' and ts >= '1970-01-01T00:00:00.010000000Z'",
                    "PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"1970-01-01T00:00:00.010000Z\",\"1970-01-01T23:59:59.999999Z\")]\n" :
                                    "      intervals: [(\"1970-01-01T00:00:00.010000000Z\",\"1970-01-01T23:59:59.999999999Z\")]\n")

            );
            expected = "a\tts\n";
            assertSql(
                    expected,
                    "select * from x where ts < '1970-01-01T00:00:00.010000Z' and ts in '1970-01-01T00:00:01' and ts in '1970-01-01T00:00:02'"
            );
            assertSql(
                    expected,
                    "select * from x where ts < '1970-01-01T00:00:00.010000000Z' and ts in '1970-01-01T00:00:01' and ts in '1970-01-01T00:00:02'"
            );
            bindVariableService.clear();
            bindVariableService.setStr("ts", "1970-01-01T00:00:00.010000Z");
            bindVariableService.setStr("ts1", "1970-01-01T00:00:00.010000000Z");
            assertSql(
                    expected,
                    "select * from x where ts < :ts and ts in '1970-01-01T00:00:01' and ts in '1970-01-01T00:00:02'"
            );
            assertSql(
                    expected,
                    "select * from x where ts < :ts1 and ts in '1970-01-01T00:00:01' and ts in '1970-01-01T00:00:02'"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts < '1970-01-01T00:00:00.010000Z' and ts in '1970-01-01T00:00:01' and ts in '1970-01-01T00:00:02'",
                    "Empty table\n"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts < '1970-01-01T00:00:00.010000000Z' and ts in '1970-01-01T00:00:01' and ts in '1970-01-01T00:00:02'",
                    "Empty table\n"
            );
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

            assertSql(
                    replaceTimestampSuffix1("a\tts\n" +
                            "80.43224099968394\t1970-01-01T00:00:00.000000Z\n" +
                            "8.486964232560668\t1970-01-01T00:00:00.010000Z\n", timestampType.getTypeName()),
                    "select * from x where ts < (select max(ts) from x)"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts < (select max(ts) from x)",
                    "PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"MIN\",\"1970-01-01T00:00:00.019999Z\")]\n" :
                                    "      intervals: [(\"MIN\",\"1970-01-01T00:00:00.019999999Z\")]\n")
            );

            assertSql(
                    replaceTimestampSuffix1("a\tts\n" +
                            "80.43224099968394\t1970-01-01T00:00:00.000000Z\n" +
                            "8.486964232560668\t1970-01-01T00:00:00.010000Z\n" +
                            "8.43832076262595\t1970-01-01T00:00:00.020000Z\n", timestampType.getTypeName()),
                    "select * from x where ts <= (select max(ts) from x)"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts <= (select max(ts) from x)",
                    replaceTimestampSuffix1("PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            "      intervals: [(\"MIN\",\"1970-01-01T00:00:00.020000Z\")]\n", timestampType.getTypeName())
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

            assertSql(
                    replaceTimestampSuffix1("a\tts\n" +
                            "8.486964232560668\t1970-01-01T00:00:00.010000Z\n", timestampType.getTypeName()),
                    "select * from x where ts > (select min(ts) from x) and ts < (select max(ts) from x)"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts > (select min(ts) from x) and ts < (select max(ts) from x)",
                    "PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"1970-01-01T00:00:00.000001Z\",\"1970-01-01T00:00:00.019999Z\")]\n" :
                                    "      intervals: [(\"1970-01-01T00:00:00.000000001Z\",\"1970-01-01T00:00:00.019999999Z\")]\n")
            );
        });
    }
}
