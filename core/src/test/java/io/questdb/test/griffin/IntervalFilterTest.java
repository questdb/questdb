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
import org.junit.Test;

public class IntervalFilterTest extends AbstractCairoTest {

    @Test
    public void testBetween() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as " +
                            "(" +
                            "  select" +
                            "    rnd_double(0)*100 a," +
                            "    timestamp_sequence(0, 1000000) ts" +
                            "  from long_sequence(3)" +
                            ") timestamp(ts) partition by day"
            );

            String expected = "a\tts\n" +
                    "8.486964232560668\t1970-01-01T00:00:01.000000Z\n" +
                    "8.43832076262595\t1970-01-01T00:00:02.000000Z\n";
            assertSql(
                    expected,
                    "select * from x where ts between '1970-01-01T00:00:01.000000Z' and '1970-01-01T00:00:02.000000Z'"
            );
            bindVariableService.clear();
            bindVariableService.setStr("ts1", "1970-01-01T00:00:01.000000Z");
            bindVariableService.setStr("ts2", "1970-01-01T00:00:02.000000Z");
            assertSql(
                    expected,
                    "select * from x where ts between :ts1 and :ts2"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts between '1970-01-01T00:00:01.000000Z' and '1970-01-01T00:00:02.000000Z'",
                    "PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            "      intervals: [(\"1970-01-01T00:00:01.000000Z\",\"1970-01-01T00:00:02.000000Z\")]\n"
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
                            "    timestamp_sequence(0, 10000) ts" +
                            "  from long_sequence(3)" +
                            ") timestamp(ts) partition by day"
            );

            String expected = "a\tts\n" +
                    "8.486964232560668\t1970-01-01T00:00:00.010000Z\n";
            assertSql(
                    expected,
                    "select * from x where ts = '1970-01-01T00:00:00.010000Z'"
            );
            bindVariableService.clear();
            bindVariableService.setStr("ts", "1970-01-01T00:00:00.010000Z");
            assertSql(
                    expected,
                    "select * from x where ts = :ts"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts = '1970-01-01T00:00:00.010000Z'",
                    "PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            "      intervals: [(\"1970-01-01T00:00:00.010000Z\",\"1970-01-01T00:00:00.010000Z\")]\n"
            );

            expected = "a\tts\n" +
                    "80.43224099968394\t1970-01-01T00:00:00.000000Z\n" +
                    "8.43832076262595\t1970-01-01T00:00:00.020000Z\n";
            assertSql(
                    expected,
                    "select * from x where ts != '1970-01-01T00:00:00.010000Z'"
            );
            bindVariableService.clear();
            bindVariableService.setStr("ts", "1970-01-01T00:00:00.010000Z");
            assertSql(
                    expected,
                    "select * from x where ts != :ts"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts != '1970-01-01T00:00:00.010000Z'",
                    "PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            "      intervals: [(\"MIN\",\"1970-01-01T00:00:00.009999Z\"),(\"1970-01-01T00:00:00.010001Z\",\"MAX\")]\n"
            );

            expected = "a\tts\n" +
                    "80.43224099968394\t1970-01-01T00:00:00.000000Z\n" +
                    "8.43832076262595\t1970-01-01T00:00:00.020000Z\n";
            assertSql(
                    expected,
                    "select * from x where ts in '1970-01-01' and ts != '1970-01-01T00:00:00.010000Z'"
            );
            bindVariableService.clear();
            bindVariableService.setStr("date", "1970-01-01");
            bindVariableService.setStr("ts", "1970-01-01T00:00:00.010000Z");
            assertSql(
                    expected,
                    "select * from x where ts in :date and ts != :ts"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts in '1970-01-01' and ts != '1970-01-01T00:00:00.010000Z'",
                    "PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            "      intervals: [(\"1970-01-01T00:00:00.000000Z\",\"1970-01-01T00:00:00.009999Z\"),(\"1970-01-01T00:00:00.010001Z\",\"1970-01-01T23:59:59.999999Z\")]\n"
            );

            expected = "a\tts\n";
            assertSql(
                    expected,
                    "select * from x where ts = '1970-01-01T00:00:00.010000Z' and ts = '1970-01-01T00:00:00.020000Z' and ts = '1970-01-01T00:00:00.030000Z'"
            );
            bindVariableService.clear();
            bindVariableService.setStr("ts", "1970-01-01T00:00:00.010000Z");
            assertSql(
                    expected,
                    "select * from x where ts = :ts and ts = '1970-01-01T00:00:00.020000Z' and ts = '1970-01-01T00:00:00.030000Z'"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts = '1970-01-01T00:00:00.010000Z' and ts = '1970-01-01T00:00:00.020000Z' and ts = '1970-01-01T00:00:00.030000Z'",
                    "Empty table\n"
            );

            expected = "a\tts\n";
            assertSql(
                    expected,
                    "select * from x where ts != '1970-01-01T00:00:00.010000Z' and ts = '1970-01-01T00:00:00.020000Z' and ts = '1970-01-01T00:00:00.030000Z'"
            );
            bindVariableService.clear();
            bindVariableService.setStr("ts", "1970-01-01T00:00:00.010000Z");
            assertSql(
                    expected,
                    "select * from x where ts != :ts and ts = '1970-01-01T00:00:00.020000Z' and ts = '1970-01-01T00:00:00.030000Z'"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts != '1970-01-01T00:00:00.010000Z' and ts = '1970-01-01T00:00:00.020000Z' and ts = '1970-01-01T00:00:00.030000Z'",
                    "Empty table\n"
            );

            assertSql(
                    "a\tts\n",
                    "select * from x where ts = (-9223372036854775808)::timestamp"
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
                            "    timestamp_sequence(0, 10000) ts" +
                            "  from long_sequence(3)" +
                            ") timestamp(ts) partition by day"
            );

            assertSql(
                    "a\tts\n" +
                            "8.43832076262595\t1970-01-01T00:00:00.020000Z\n",
                    "select * from x where ts = (select max(ts) from x)"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts = (select max(ts) from x)",
                    "PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            "      intervals: [(\"1970-01-01T00:00:00.020000Z\",\"1970-01-01T00:00:00.020000Z\")]\n"
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
                            "    timestamp_sequence(0, 10000) ts" +
                            "  from long_sequence(3)" +
                            ") timestamp(ts) partition by day"
            );

            String expected = "a\tts\n" +
                    "8.43832076262595\t1970-01-01T00:00:00.020000Z\n";
            assertSql(
                    expected,
                    "select * from x where ts > '1970-01-01T00:00:00.010000Z'"
            );
            bindVariableService.clear();
            bindVariableService.setStr("ts", "1970-01-01T00:00:00.010000Z");
            assertSql(
                    expected,
                    "select * from x where ts > :ts"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts > '1970-01-01T00:00:00.010000Z'",
                    "PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            "      intervals: [(\"1970-01-01T00:00:00.010001Z\",\"MAX\")]\n"
            );

            expected = "a\tts\n" +
                    "80.43224099968394\t1970-01-01T00:00:00.000000Z\n" +
                    "8.486964232560668\t1970-01-01T00:00:00.010000Z\n";
            assertSql(
                    expected,
                    "select * from x where ts <= '1970-01-01T00:00:00.010000Z'"
            );
            bindVariableService.clear();
            bindVariableService.setStr("ts", "1970-01-01T00:00:00.010000Z");
            assertSql(
                    expected,
                    "select * from x where ts <= :ts"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts <= '1970-01-01T00:00:00.010000Z'",
                    "PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            "      intervals: [(\"MIN\",\"1970-01-01T00:00:00.010000Z\")]\n"
            );

            expected = "a\tts\n" +
                    "80.43224099968394\t1970-01-01T00:00:00.000000Z\n" +
                    "8.486964232560668\t1970-01-01T00:00:00.010000Z\n";
            assertSql(
                    expected,
                    "select * from x where ts in '1970-01-01' and ts <= '1970-01-01T00:00:00.010000Z'"
            );
            bindVariableService.clear();
            bindVariableService.setStr("date", "1970-01-01");
            bindVariableService.setStr("ts", "1970-01-01T00:00:00.010000Z");
            assertSql(
                    expected,
                    "select * from x where ts in :date and ts <= :ts"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts in '1970-01-01' and ts <= '1970-01-01T00:00:00.010000Z'",
                    "PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            "      intervals: [(\"1970-01-01T00:00:00.000000Z\",\"1970-01-01T00:00:00.010000Z\")]\n"
            );

            expected = "a\tts\n";
            assertSql(
                    expected,
                    "select * from x where ts > '1970-01-01T00:00:00.010000Z' and ts in '1970-01-01T00:00:01' and ts in '1970-01-01T00:00:02'"
            );
            bindVariableService.clear();
            bindVariableService.setStr("ts", "1970-01-01T00:00:00.010000Z");
            assertSql(
                    expected,
                    "select * from x where ts > :ts and ts in '1970-01-01T00:00:01' and ts in '1970-01-01T00:00:02'"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts > '1970-01-01T00:00:00.010000Z' and ts in '1970-01-01T00:00:01' and ts in '1970-01-01T00:00:02'",
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
                            "    timestamp_sequence(0, 10000) ts" +
                            "  from long_sequence(3)" +
                            ") timestamp(ts) partition by day"
            );

            assertSql(
                    "a\tts\n" +
                            "8.486964232560668\t1970-01-01T00:00:00.010000Z\n" +
                            "8.43832076262595\t1970-01-01T00:00:00.020000Z\n",
                    "select * from x where ts > (select min(ts) from x)"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts > (select min(ts) from x)",
                    "PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            "      intervals: [(\"1970-01-01T00:00:00.000001Z\",\"MAX\")]\n"
            );

            assertSql(
                    "a\tts\n" +
                            "80.43224099968394\t1970-01-01T00:00:00.000000Z\n" +
                            "8.486964232560668\t1970-01-01T00:00:00.010000Z\n" +
                            "8.43832076262595\t1970-01-01T00:00:00.020000Z\n",
                    "select * from x where ts >= (select min(ts) from x)"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts >= (select min(ts) from x)",
                    "PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            "      intervals: [(\"1970-01-01T00:00:00.000000Z\",\"MAX\")]\n"
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
                            "    timestamp_sequence(0, 1000000) ts" +
                            "  from long_sequence(3)" +
                            ") timestamp(ts) partition by day"
            );

            String expected = "a\tts\n" +
                    "8.486964232560668\t1970-01-01T00:00:01.000000Z\n";
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
                            "      intervals: [(\"1970-01-01T00:00:01.000000Z\",\"1970-01-01T00:00:01.999999Z\")]\n"
            );

            expected = "a\tts\n" +
                    "80.43224099968394\t1970-01-01T00:00:00.000000Z\n" +
                    "8.43832076262595\t1970-01-01T00:00:02.000000Z\n";
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
                            "      intervals: [(\"MIN\",\"1970-01-01T00:00:00.999999Z\"),(\"1970-01-01T00:00:02.000000Z\",\"MAX\")]\n"
            );

            expected = "a\tts\n" +
                    "80.43224099968394\t1970-01-01T00:00:00.000000Z\n" +
                    "8.43832076262595\t1970-01-01T00:00:02.000000Z\n";
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
                            "      intervals: [(\"1970-01-01T00:00:00.000000Z\",\"1970-01-01T00:00:00.999999Z\"),(\"1970-01-01T00:00:02.000000Z\",\"1970-01-01T23:59:59.999999Z\")]\n"
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

            expected = "a\tts\n" +
                    "8.486964232560668\t1970-01-01T00:00:01.000000Z\n";
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
                            "      intervals: [(\"1970-01-01T00:00:01.000000Z\",\"1970-01-01T00:00:01.999999Z\")]\n"
            );

            assertSql(
                    "a\tts\n" +
                            "8.486964232560668\t1970-01-01T00:00:01.000000Z\n" +
                            "8.43832076262595\t1970-01-01T00:00:02.000000Z\n",
                    "select * from x where ts in ('1970-01-01T00:00:01', '1970-01-01T00:00:02')"
            );

            assertSql(
                    "a\tts\n" +
                            "8.486964232560668\t1970-01-01T00:00:01.000000Z\n" +
                            "8.43832076262595\t1970-01-01T00:00:02.000000Z\n",
                    "select * from x where ts in '1970-01-01T00:00:01' or ts in '1970-01-01T00:00:02'"
            );

            assertSql(
                    "a\tts\n",
                    "select * from x where ts in null"
            );

            assertSql(
                    "a\tts\n" +
                            "80.43224099968394\t1970-01-01T00:00:00.000000Z\n" +
                            "8.486964232560668\t1970-01-01T00:00:01.000000Z\n" +
                            "8.43832076262595\t1970-01-01T00:00:02.000000Z\n",
                    "select * from x where ts not in null"
            );

            assertSql(
                    "a\tts\n",
                    "select * from x where ts not in null and ts in '1970-01-01T00:00:01' and ts in '1970-01-01T00:00:02'"
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
                            "    timestamp_sequence(0, 10000) ts" +
                            "  from long_sequence(3)" +
                            ") timestamp(ts) partition by day"
            );

            assertException(
                    "select * from x where ts > now() - '3 day' order by ts desc",
                    0,
                    "inconvertible value: `3 day` [STRING -> TIMESTAMP]"
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
                            "    timestamp_sequence(0, 10000) ts" +
                            "  from long_sequence(3)" +
                            ") timestamp(ts) partition by day"
            );

            assertException(
                    "select * from x where ts > now() - '3 day'",
                    0,
                    "inconvertible value: `3 day` [STRING -> TIMESTAMP]"
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
                            "    timestamp_sequence(0, 10000) ts" +
                            "  from long_sequence(3)" +
                            ") timestamp(ts) partition by day"
            );

            String expected = "a\tts\n" +
                    "80.43224099968394\t1970-01-01T00:00:00.000000Z\n";
            assertSql(
                    expected,
                    "select * from x where ts < '1970-01-01T00:00:00.010000Z'"
            );
            bindVariableService.clear();
            bindVariableService.setStr("ts", "1970-01-01T00:00:00.010000Z");
            assertSql(
                    expected,
                    "select * from x where ts < :ts"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts < '1970-01-01T00:00:00.010000Z'",
                    "PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            "      intervals: [(\"MIN\",\"1970-01-01T00:00:00.009999Z\")]\n"
            );

            expected = "a\tts\n" +
                    "8.486964232560668\t1970-01-01T00:00:00.010000Z\n" +
                    "8.43832076262595\t1970-01-01T00:00:00.020000Z\n";
            assertSql(
                    expected,
                    "select * from x where ts >= '1970-01-01T00:00:00.010000Z'"
            );
            bindVariableService.clear();
            bindVariableService.setStr("ts", "1970-01-01T00:00:00.010000Z");
            assertSql(
                    expected,
                    "select * from x where ts >= :ts"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts >= '1970-01-01T00:00:00.010000Z'",
                    "PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            "      intervals: [(\"1970-01-01T00:00:00.010000Z\",\"MAX\")]\n"
            );

            expected = "a\tts\n" +
                    "8.486964232560668\t1970-01-01T00:00:00.010000Z\n" +
                    "8.43832076262595\t1970-01-01T00:00:00.020000Z\n";
            assertSql(
                    expected,
                    "select * from x where ts in '1970-01-01' and ts >= '1970-01-01T00:00:00.010000Z'"
            );
            bindVariableService.clear();
            bindVariableService.setStr("date", "1970-01-01");
            bindVariableService.setStr("ts", "1970-01-01T00:00:00.010000Z");
            assertSql(
                    expected,
                    "select * from x where ts in :date and ts >= :ts"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts in '1970-01-01' and ts >= '1970-01-01T00:00:00.010000Z'",
                    "PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            "      intervals: [(\"1970-01-01T00:00:00.010000Z\",\"1970-01-01T23:59:59.999999Z\")]\n"
            );

            expected = "a\tts\n";
            assertSql(
                    expected,
                    "select * from x where ts < '1970-01-01T00:00:00.010000Z' and ts in '1970-01-01T00:00:01' and ts in '1970-01-01T00:00:02'"
            );
            bindVariableService.clear();
            bindVariableService.setStr("ts", "1970-01-01T00:00:00.010000Z");
            assertSql(
                    expected,
                    "select * from x where ts < :ts and ts in '1970-01-01T00:00:01' and ts in '1970-01-01T00:00:02'"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts < '1970-01-01T00:00:00.010000Z' and ts in '1970-01-01T00:00:01' and ts in '1970-01-01T00:00:02'",
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
                            "    timestamp_sequence(0, 10000) ts" +
                            "  from long_sequence(3)" +
                            ") timestamp(ts) partition by day"
            );

            assertSql(
                    "a\tts\n" +
                            "80.43224099968394\t1970-01-01T00:00:00.000000Z\n" +
                            "8.486964232560668\t1970-01-01T00:00:00.010000Z\n",
                    "select * from x where ts < (select max(ts) from x)"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts < (select max(ts) from x)",
                    "PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            "      intervals: [(\"MIN\",\"1970-01-01T00:00:00.019999Z\")]\n"
            );

            assertSql(
                    "a\tts\n" +
                            "80.43224099968394\t1970-01-01T00:00:00.000000Z\n" +
                            "8.486964232560668\t1970-01-01T00:00:00.010000Z\n" +
                            "8.43832076262595\t1970-01-01T00:00:00.020000Z\n",
                    "select * from x where ts <= (select max(ts) from x)"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts <= (select max(ts) from x)",
                    "PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            "      intervals: [(\"MIN\",\"1970-01-01T00:00:00.020000Z\")]\n"
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
                            "    timestamp_sequence(0, 10000) ts" +
                            "  from long_sequence(3)" +
                            ") timestamp(ts) partition by day"
            );

            assertSql(
                    "a\tts\n" +
                            "8.486964232560668\t1970-01-01T00:00:00.010000Z\n",
                    "select * from x where ts > (select min(ts) from x) and ts < (select max(ts) from x)"
            );
            assertPlanNoLeakCheck(
                    "select * from x where ts > (select min(ts) from x) and ts < (select max(ts) from x)",
                    "PageFrame\n" +
                            "    Row forward scan\n" +
                            "    Interval forward scan on: x\n" +
                            "      intervals: [(\"1970-01-01T00:00:00.000001Z\",\"1970-01-01T00:00:00.019999Z\")]\n"
            );
        });
    }
}
