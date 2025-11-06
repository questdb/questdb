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

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class WindowJoinTest extends AbstractCairoTest {
    private final TestTimestampType leftTableTimestampType;
    private final TestTimestampType rightTableTimestampType;

    public WindowJoinTest(TestTimestampType leftTimestampType, TestTimestampType rightTimestampType) {
        this.leftTableTimestampType = leftTimestampType;
        this.rightTableTimestampType = rightTimestampType;
    }

    @Parameterized.Parameters(name = "{0}-{1}")
    public static Collection<Object[]> testParams() {
        return Arrays.asList(new Object[][]{
                {TestTimestampType.MICRO, TestTimestampType.MICRO}, {TestTimestampType.MICRO, TestTimestampType.NANO},
                {TestTimestampType.NANO, TestTimestampType.MICRO}, {TestTimestampType.NANO, TestTimestampType.NANO}
        });
    }

    @Test
    public void testBasicWindowJoin() throws Exception {
        assertMemoryLeak(() -> {
            prepareTable();
            String expect = replaceTimestampSuffix("sym\tprice\tts\twindow_price\n" +
                    "AAPL\t100.0\t2023-01-01T09:00:00.000000Z\t301.5\n" +
                    "AAPL\t101.0\t2023-01-01T09:01:00.000000Z\t202.0\n" +
                    "AAPL\t102.0\t2023-01-01T09:02:00.000000Z\t101.5\n" +
                    "MSFT\t200.0\t2023-01-01T09:03:00.000000Z\t400.0\n" +
                    "MSFT\t201.0\t2023-01-01T09:04:00.000000Z\t200.5\n" +
                    "GOOGL\t300.0\t2023-01-01T09:05:00.000000Z\t600.0\n" +
                    "GOOGL\t301.0\t2023-01-01T09:06:00.000000Z\t300.5\n" +
                    "AAPL\t103.0\t2023-01-01T09:07:00.000000Z\t102.5\n" +
                    "MSFT\t202.0\t2023-01-01T09:08:00.000000Z\t201.5\n" +
                    "GOOGL\t302.0\t2023-01-01T09:09:00.000000Z\t301.5\n", leftTableTimestampType.getTypeName());
            assertQueryAndPlan(
                    expect,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Async Window Fast Join workers: 1\n" +
                            "      join filter: sym=sym\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding\n" +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " following\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: trades\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: prices\n",
                    "select t.*, sum(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on (t.sym = p.sym) " +
                            " range between 1 minute preceding and 1 minute following " +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );

            // verify result
            assertQuery(
                    expect,
                    "select t.*, sum(p.price) window_price " +
                            "from trades t " +
                            "left join prices p " +
                            "on (t.sym = p.sym) " +
                            " and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) " +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testFastJoinWithMasterFilter() throws Exception {
        Assume.assumeTrue(leftTableTimestampType.getTimestampType() == rightTableTimestampType.getTimestampType());
        assertMemoryLeak(() -> {
            prepareTable();
            String expect = replaceTimestampSuffix("sym\tprice\tts\twindow_price\n" +
                    "AAPL\t100.0\t2023-01-01T09:00:00.000000Z\t100.5\n" +
                    "AAPL\t101.0\t2023-01-01T09:01:00.000000Z\t100.5\n" +
                    "AAPL\t102.0\t2023-01-01T09:02:00.000000Z\t101.0\n" +
                    "MSFT\t200.0\t2023-01-01T09:03:00.000000Z\t200.0\n" +
                    "MSFT\t201.0\t2023-01-01T09:04:00.000000Z\t200.0\n" +
                    "AAPL\t103.0\t2023-01-01T09:07:00.000000Z\t102.5\n" +
                    "MSFT\t202.0\t2023-01-01T09:08:00.000000Z\t201.5\n", leftTableTimestampType.getTypeName());
            assertQueryAndPlan(
                    expect,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Async Window Fast Join workers: 1\n" +
                            "      join filter: sym=sym\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " preceding\n" +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " following\n" +
                            "      master filter: price<300\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: trades\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: prices\n",
                    "select t.*, avg(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on (t.sym = p.sym) and  t.price < 300 " +
                            " range between 2 minute preceding and 2 minute following " +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );

            // verify result
            assertQuery(
                    expect,
                    "select t.*, avg(p.price) window_price " +
                            "from (select * from trades where price < 300) t " +
                            "left join prices p " +
                            "on (t.sym = p.sym) " +
                            " and p.ts >= dateadd('m', -2, t.ts) AND p.ts <= dateadd('m', 2, t.ts) " +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    true
            );

            expect = replaceTimestampSuffix("ts\tmax\n" +
                    "2023-01-01T09:00:00.000000Z\t2023-01-01T09:01:00.000000Z\n" +
                    "2023-01-01T09:01:00.000000Z\t2023-01-01T09:01:00.000000Z\n" +
                    "2023-01-01T09:02:00.000000Z\t2023-01-01T09:01:00.000000Z\n" +
                    "2023-01-01T09:03:00.000000Z\t2023-01-01T09:03:00.000000Z\n" +
                    "2023-01-01T09:04:00.000000Z\t2023-01-01T09:03:00.000000Z\n" +
                    "2023-01-01T09:07:00.000000Z\t2023-01-01T09:06:00.000000Z\n" +
                    "2023-01-01T09:08:00.000000Z\t2023-01-01T09:07:00.000000Z\n", leftTableTimestampType.getTypeName());
            assertQueryAndPlan(
                    expect,
                    "SelectedRecord\n" +
                            "    Sort\n" +
                            "      keys: [ts, sym]\n" +
                            "        Async Window Fast Join workers: 1\n" +
                            "          join filter: sym=sym\n" +
                            "          window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " preceding\n" +
                            "          window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " following\n" +
                            "          master filter: price<300\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: trades\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: prices\n",
                    "select t.ts, max(p.ts) " +
                            "from trades t " +
                            "window join prices p " +
                            "on (t.sym = p.sym) and  t.price < 300 " +
                            " range between 2 minute preceding and 2 minute following " +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );

            // verify result
            assertQuery(
                    expect,
                    "select t.ts, max(p.ts) " +
                            "from (select * from trades where price < 300) t " +
                            "left join prices p " +
                            "on (t.sym = p.sym) " +
                            " and p.ts >= dateadd('m', -2, t.ts) AND p.ts <= dateadd('m', 2, t.ts) " +
                            "order by t.ts;",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testVectorizedWindowJoin() throws Exception {
        assertMemoryLeak(() -> {
            prepareTable();
            String expect = replaceTimestampSuffix("sym\tprice\tts\twindow_price\n" +
                    "AAPL\t100.0\t2023-01-01T09:00:00.000000Z\t100.5\n" +
                    "AAPL\t101.0\t2023-01-01T09:01:00.000000Z\t101.0\n" +
                    "AAPL\t102.0\t2023-01-01T09:02:00.000000Z\t101.5\n" +
                    "MSFT\t200.0\t2023-01-01T09:03:00.000000Z\t200.0\n" +
                    "MSFT\t201.0\t2023-01-01T09:04:00.000000Z\t200.5\n" +
                    "GOOGL\t300.0\t2023-01-01T09:05:00.000000Z\t300.0\n" +
                    "GOOGL\t301.0\t2023-01-01T09:06:00.000000Z\t300.5\n" +
                    "AAPL\t103.0\t2023-01-01T09:07:00.000000Z\t102.5\n" +
                    "MSFT\t202.0\t2023-01-01T09:08:00.000000Z\t201.5\n" +
                    "GOOGL\t302.0\t2023-01-01T09:09:00.000000Z\t301.5\n", leftTableTimestampType.getTypeName());
            assertQueryAndPlan(
                    expect,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Async Window Fast Join workers: 1\n" +
                            "      join filter: sym=sym\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding\n" +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " following\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: trades\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: prices\n",
                    "select t.*, avg(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on (t.sym = p.sym) " +
                            " range between 1 minute preceding and 1 minute following " +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );

            // verify result
            assertQuery(
                    expect,
                    "select t.*, avg(p.price) window_price " +
                            "from trades t " +
                            "left join prices p " +
                            "on (t.sym = p.sym) " +
                            " and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) " +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testWindowJoinBinarySearch() throws Exception {
        assertMemoryLeak(() -> {
            prepareTable(true);
            String expect = replaceTimestampSuffix("sym\tprice\tts\twindow_price\n" +
                    "AAPL\t100.0\t2023-01-01T09:00:00.000000Z\tnull\n" +
                    "AAPL\t101.0\t2023-01-01T09:01:00.000000Z\tnull\n" +
                    "AAPL\t102.0\t2023-01-01T09:02:00.000000Z\tnull\n" +
                    "MSFT\t200.0\t2023-01-01T09:03:00.000000Z\tnull\n" +
                    "MSFT\t201.0\t2023-01-01T09:04:00.000000Z\tnull\n" +
                    "GOOGL\t300.0\t2023-01-01T09:05:00.000000Z\tnull\n" +
                    "GOOGL\t301.0\t2023-01-01T09:06:00.000000Z\tnull\n" +
                    "AAPL\t103.0\t2023-01-01T09:07:00.000000Z\tnull\n" +
                    "MSFT\t202.0\t2023-01-01T09:08:00.000000Z\tnull\n" +
                    "GOOGL\t302.0\t2023-01-01T09:09:00.000000Z\tnull\n" +
                    "TSLA\t400.0\t2023-01-01T09:10:00.000000Z\tnull\n" +
                    "TSLA\t401.0\t2023-01-01T09:11:00.000000Z\tnull\n" +
                    "AMZN\t500.0\t2023-01-01T09:12:00.000000Z\tnull\n" +
                    "AMZN\t501.0\t2023-01-01T09:13:00.000000Z\t99.5\n" +
                    "META\t600.0\t2023-01-01T09:14:00.000000Z\t200.0\n" +
                    "META\t601.0\t2023-01-01T09:15:00.000000Z\t202.0\n" +
                    "TSLA\t402.0\t2023-01-01T09:16:00.000000Z\t301.0\n" +
                    "AMZN\t502.0\t2023-01-01T09:17:00.000000Z\t400.0\n" +
                    "META\t602.0\t2023-01-01T09:18:00.000000Z\t500.0\n" +
                    "NFLX\t700.0\t2023-01-01T09:19:00.000000Z\t600.0\n", leftTableTimestampType.getTypeName());
            assertQueryAndPlan(
                    expect,
                    "Sort\n" +
                            "  keys: [ts]\n" +
                            "    Async Window Join workers: 1\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "900000000" : "900000000000") + " preceding\n" +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "840000000" : "840000000000") + " preceding\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: trades\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: prices\n",
                    "select t.*, sum(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            " range between 15 minute preceding and 14 minute preceding " +
                            "order by t.ts;",
                    "ts",
                    true,
                    false
            );

            // verify result
            assertQuery(
                    expect,
                    "select t.*, sum(p.price) window_price " +
                            "from trades t " +
                            "left join prices p " +
                            "on p.ts >= dateadd('m', -15, t.ts) AND p.ts <= dateadd('m', -14, t.ts) " +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    true
            );

            expect = replaceTimestampSuffix("sym\tprice\tts\twindow_price\n" +
                    "AAPL\t100.0\t2023-01-01T09:00:00.000000Z\tnull\n" +
                    "AAPL\t101.0\t2023-01-01T09:01:00.000000Z\tnull\n" +
                    "AAPL\t102.0\t2023-01-01T09:02:00.000000Z\tnull\n" +
                    "MSFT\t200.0\t2023-01-01T09:03:00.000000Z\tnull\n" +
                    "MSFT\t201.0\t2023-01-01T09:04:00.000000Z\tnull\n" +
                    "GOOGL\t300.0\t2023-01-01T09:05:00.000000Z\tnull\n" +
                    "GOOGL\t301.0\t2023-01-01T09:06:00.000000Z\tnull\n" +
                    "AAPL\t103.0\t2023-01-01T09:07:00.000000Z\tnull\n" +
                    "MSFT\t202.0\t2023-01-01T09:08:00.000000Z\t200.5\n" +
                    "GOOGL\t302.0\t2023-01-01T09:09:00.000000Z\t600.0\n" +
                    "TSLA\t400.0\t2023-01-01T09:10:00.000000Z\tnull\n" +
                    "TSLA\t401.0\t2023-01-01T09:11:00.000000Z\tnull\n" +
                    "AMZN\t500.0\t2023-01-01T09:12:00.000000Z\tnull\n" +
                    "AMZN\t501.0\t2023-01-01T09:13:00.000000Z\tnull\n" +
                    "META\t600.0\t2023-01-01T09:14:00.000000Z\tnull\n" +
                    "META\t601.0\t2023-01-01T09:15:00.000000Z\tnull\n" +
                    "TSLA\t402.0\t2023-01-01T09:16:00.000000Z\tnull\n" +
                    "AMZN\t502.0\t2023-01-01T09:17:00.000000Z\t500.5\n" +
                    "META\t602.0\t2023-01-01T09:18:00.000000Z\t1200.0\n" +
                    "NFLX\t700.0\t2023-01-01T09:19:00.000000Z\tnull\n", leftTableTimestampType.getTypeName());
            assertQueryAndPlan(
                    expect,
                    "Sort\n" +
                            "  keys: [ts]\n" +
                            "    Async Window Fast Join workers: 1\n" +
                            "      join filter: sym=sym\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "300000000" : "300000000000") + " preceding\n" +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "240000000" : "240000000000") + " preceding\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: trades\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: prices\n",
                    "select t.*, sum(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p on t.sym = p.sym" +
                            " range between 5 minute preceding and 4 minute preceding " +
                            "order by t.ts;",
                    "ts",
                    true,
                    false
            );

            // verify result
            assertQuery(
                    expect,
                    "select t.*, sum(p.price) window_price " +
                            "from trades t " +
                            "left join prices p " +
                            "on t.sym = p.sym and p.ts >= dateadd('m', -5, t.ts) AND p.ts <= dateadd('m', -4, t.ts) " +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testWindowJoinNoOtherCondition() throws Exception {
        assertMemoryLeak(() -> {
            prepareTable();
            String expect = replaceTimestampSuffix("sym\tprice\tts\twindow_price\n" +
                    "AAPL\t100.0\t2023-01-01T09:00:00.000000Z\t99.5\n" +
                    "AAPL\t101.0\t2023-01-01T09:01:00.000000Z\t200.0\n" +
                    "AAPL\t102.0\t2023-01-01T09:02:00.000000Z\t202.0\n" +
                    "MSFT\t200.0\t2023-01-01T09:03:00.000000Z\t301.0\n" +
                    "MSFT\t201.0\t2023-01-01T09:04:00.000000Z\t400.0\n" +
                    "GOOGL\t300.0\t2023-01-01T09:05:00.000000Z\t500.0\n" +
                    "GOOGL\t301.0\t2023-01-01T09:06:00.000000Z\t600.0\n" +
                    "AAPL\t103.0\t2023-01-01T09:07:00.000000Z\t403.0\n" +
                    "MSFT\t202.0\t2023-01-01T09:08:00.000000Z\t304.0\n" +
                    "GOOGL\t302.0\t2023-01-01T09:09:00.000000Z\t503.0\n", leftTableTimestampType.getTypeName());
            assertQueryAndPlan(
                    expect,
                    "Sort\n" +
                            "  keys: [ts]\n" +
                            "    Async Window Join workers: 1\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " preceding\n" +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: trades\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: prices\n",
                    "select t.*, sum(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            " range between 2 minute preceding and 1 minute preceding " +
                            "order by t.ts;",
                    "ts",
                    true,
                    false
            );

            // verify result
            assertQuery(
                    expect,
                    "select t.*, sum(p.price) window_price " +
                            "from trades t " +
                            "left join prices p " +
                            "on p.ts >= dateadd('m', -2, t.ts) AND p.ts <= dateadd('m', -1, t.ts) " +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    true
            );

            expect = replaceTimestampSuffix("sym\tprice\tts\twindow_price\n" +
                    "AAPL\t100.0\t2023-01-01T09:00:00.000000Z\tnull\n" +
                    "AAPL\t101.0\t2023-01-01T09:01:00.000000Z\t99.5\n" +
                    "AAPL\t102.0\t2023-01-01T09:02:00.000000Z\t200.0\n" +
                    "MSFT\t200.0\t2023-01-01T09:03:00.000000Z\t202.0\n" +
                    "MSFT\t201.0\t2023-01-01T09:04:00.000000Z\t301.0\n" +
                    "GOOGL\t300.0\t2023-01-01T09:05:00.000000Z\t400.0\n" +
                    "GOOGL\t301.0\t2023-01-01T09:06:00.000000Z\t500.0\n" +
                    "AAPL\t103.0\t2023-01-01T09:07:00.000000Z\t600.0\n" +
                    "MSFT\t202.0\t2023-01-01T09:08:00.000000Z\t403.0\n" +
                    "GOOGL\t302.0\t2023-01-01T09:09:00.000000Z\t304.0\n", leftTableTimestampType.getTypeName());
            assertQueryAndPlan(
                    expect,
                    "Sort\n" +
                            "  keys: [ts]\n" +
                            "    Async Window Join workers: 1\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "180000000" : "180000000000") + " preceding\n" +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " preceding\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: trades\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: prices\n",
                    "select t.*, sum(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            " range between 3 minute preceding and 2 minute preceding " +
                            "order by t.ts;",
                    "ts",
                    true,
                    false
            );

            // verify result
            assertQuery(
                    expect,
                    "select t.*, sum(p.price) window_price " +
                            "from trades t " +
                            "left join prices p " +
                            "on p.ts >= dateadd('m', -3, t.ts) AND p.ts <= dateadd('m', -2, t.ts) " +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    true
            );

/*            expect = replaceTimestampSuffix("sym\tprice\tts\twindow_price\n" +
                    "AAPL\t100.0\t2023-01-01T09:00:00.000000Z\t301.5\n" +
                    "AAPL\t101.0\t2023-01-01T09:01:00.000000Z\t401.5\n" +
                    "AAPL\t102.0\t2023-01-01T09:02:00.000000Z\t501.5\n" +
                    "MSFT\t200.0\t2023-01-01T09:03:00.000000Z\t699.5\n" +
                    "MSFT\t201.0\t2023-01-01T09:04:00.000000Z\t800.5\n" +
                    "AAPL\t103.0\t2023-01-01T09:07:00.000000Z\t605.5\n" +
                    "MSFT\t202.0\t2023-01-01T09:08:00.000000Z\t503.0\n", leftTableTimestampType.getTypeName());
            assertQueryAndPlan(
                    expect,
                    "Sort\n" +
                            "  keys: [ts]\n" +
                            "    Async Window Join workers: 1\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding\n" +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " following\n" +
                            "      master filter: price<300\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: trades\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: prices\n",
                    "select t.*, sum(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p on t.price < 300" +
                            " range between 1 minute preceding and 1 minute following " +
                            "order by t.ts;",
                    "ts",
                    true,
                    false
            );

            // verify result
            assertQuery(
                    expect,
                    "select t.*, sum(p.price) window_price " +
                            "from (select * from trades where price < 300) t " +
                            "left join prices p " +
                            "on p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts)" +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    true
            );*/
        });
    }

    @Test
    public void testWindowJoinProjection() throws Exception {
        assertMemoryLeak(() -> {
            prepareTable();
            String expect = replaceTimestampSuffix("window_price\tcolumn\tsym\tts\n" +
                    "303.5\t101.0\tAAPL\t2023-01-01T09:00:00.000000Z\n" +
                    "204.0\t102.0\tAAPL\t2023-01-01T09:01:00.000000Z\n" +
                    "103.5\t103.0\tAAPL\t2023-01-01T09:02:00.000000Z\n" +
                    "402.0\t201.0\tMSFT\t2023-01-01T09:03:00.000000Z\n" +
                    "202.5\t202.0\tMSFT\t2023-01-01T09:04:00.000000Z\n" +
                    "602.0\t301.0\tGOOGL\t2023-01-01T09:05:00.000000Z\n" +
                    "302.5\t302.0\tGOOGL\t2023-01-01T09:06:00.000000Z\n" +
                    "104.5\t104.0\tAAPL\t2023-01-01T09:07:00.000000Z\n" +
                    "203.5\t203.0\tMSFT\t2023-01-01T09:08:00.000000Z\n" +
                    "303.5\t303.0\tGOOGL\t2023-01-01T09:09:00.000000Z\n", leftTableTimestampType.getTypeName());
            assertQueryAndPlan(
                    expect,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    VirtualRecord\n" +
                            "      functions: [sum+2,price+1,sym,ts]\n" +
                            "        Async Window Fast Join workers: 1\n" +
                            "          join filter: sym=sym\n" +
                            "          window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding\n" +
                            "          window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " following\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: trades\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: prices\n",
                    "select sum(p.price) + 2 as window_price, t.price + 1, t.sym, t.ts " +
                            "from trades t " +
                            "window join prices p " +
                            "on (t.sym = p.sym) " +
                            " range between 1 minute preceding and 1 minute following " +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );

            // verify result
            assertQuery(
                    expect,
                    "select sum(p.price) + 2 window_price, t.price + 1, t.sym, t.ts " +
                            "from trades t " +
                            "left join prices p " +
                            "on (t.sym = p.sym) " +
                            " and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) " +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    true
            );

            expect = replaceTimestampSuffix("window_price\tcolumn\tsym\tts\n" +
                    "null\t101.0\tAAPL\t2023-01-01T09:00:00.000000Z\n" +
                    "201.5\t102.0\tAAPL\t2023-01-01T09:01:00.000000Z\n" +
                    "402.0\t103.0\tAAPL\t2023-01-01T09:02:00.000000Z\n" +
                    "301.5\t201.0\tMSFT\t2023-01-01T09:03:00.000000Z\n" +
                    "602.0\t202.0\tMSFT\t2023-01-01T09:04:00.000000Z\n" +
                    "104.5\t301.0\tGOOGL\t2023-01-01T09:05:00.000000Z\n" +
                    "306.0\t302.0\tGOOGL\t2023-01-01T09:06:00.000000Z\n" +
                    "505.0\t104.0\tAAPL\t2023-01-01T09:07:00.000000Z\n" +
                    "303.5\t203.0\tMSFT\t2023-01-01T09:08:00.000000Z\n" +
                    "null\t303.0\tGOOGL\t2023-01-01T09:09:00.000000Z\n", leftTableTimestampType.getTypeName());
            assertQueryAndPlan(
                    expect,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    VirtualRecord\n" +
                            "      functions: [sum+2,price+1,sym,ts]\n" +
                            "        Async Window Join workers: 1\n" +
                            "          join filter: (t.sym!=p.sym and 100<p.price)\n" +
                            "          window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding\n" +
                            "          window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " following\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: trades\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: prices\n",
                    "select sum(p.price) + 2 as window_price, t.price + 1, t.sym, t.ts " +
                            "from trades t " +
                            "window join prices p " +
                            "on (t.sym != p.sym and p.price > 100) " +
                            " range between 1 minute preceding and 1 minute following " +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );

            // verify result
            assertQuery(
                    expect,
                    "select sum(p.price) + 2 window_price, t.price + 1, t.sym, t.ts " +
                            "from trades t " +
                            "left join prices p " +
                            "on (t.sym != p.sym and p.price > 100) " +
                            " and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) " +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    true
            );
        });
    }

    private void prepareTable(boolean extraData) throws SqlException {
        executeWithRewriteTimestamp(
                "create table trades (" +
                        "  sym symbol," +
                        "  price double," +
                        "  ts #TIMESTAMP" +
                        ") timestamp(ts) partition by day;",
                leftTableTimestampType.getTypeName()
        );
        executeWithRewriteTimestamp(
                "create table prices (" +
                        "  sym symbol," +
                        "  price double," +
                        "  ts #TIMESTAMP" +
                        ") timestamp(ts) partition by day;",
                rightTableTimestampType.getTypeName()
        );

        executeWithRewriteTimestamp(
                "insert into trades values " +
                        "('AAPL', 100.0, cast('2023-01-01T09:00:00.000000Z' as #TIMESTAMP))," +
                        "('AAPL', 101.0, cast('2023-01-01T09:01:00.000000Z' as #TIMESTAMP))," +
                        "('AAPL', 102.0, cast('2023-01-01T09:02:00.000000Z' as #TIMESTAMP))," +
                        "('MSFT', 200.0, cast('2023-01-01T09:03:00.000000Z' as #TIMESTAMP))," +
                        "('MSFT', 201.0, cast('2023-01-01T09:04:00.000000Z' as #TIMESTAMP))," +
                        "('GOOGL', 300.0, cast('2023-01-01T09:05:00.000000Z' as #TIMESTAMP))," +
                        "('GOOGL', 301.0, cast('2023-01-01T09:06:00.000000Z' as #TIMESTAMP))," +
                        "('AAPL', 103.0, cast('2023-01-01T09:07:00.000000Z' as #TIMESTAMP))," +
                        "('MSFT', 202.0, cast('2023-01-01T09:08:00.000000Z' as #TIMESTAMP))," +
                        "('GOOGL', 302.0, cast('2023-01-01T09:09:00.000000Z' as #TIMESTAMP));",
                leftTableTimestampType.getTypeName()
        );

        executeWithRewriteTimestamp(
                "insert into prices values " +
                        "('AAPL', 99.5, cast('2023-01-01T08:59:00.000000Z' as #TIMESTAMP))," +
                        "('AAPL', 100.5, cast('2023-01-01T09:00:00.000000Z' as #TIMESTAMP))," +
                        "('AAPL', 101.5, cast('2023-01-01T09:01:00.000000Z' as #TIMESTAMP))," +
                        "('MSFT', 199.5, cast('2023-01-01T09:02:00.000000Z' as #TIMESTAMP))," +
                        "('MSFT', 200.5, cast('2023-01-01T09:03:00.000000Z' as #TIMESTAMP))," +
                        "('GOOGL', 299.5, cast('2023-01-01T09:04:00.000000Z' as #TIMESTAMP))," +
                        "('GOOGL', 300.5, cast('2023-01-01T09:05:00.000000Z' as #TIMESTAMP))," +
                        "('AAPL', 102.5, cast('2023-01-01T09:06:00.000000Z' as #TIMESTAMP))," +
                        "('MSFT', 201.5, cast('2023-01-01T09:07:00.000000Z' as #TIMESTAMP))," +
                        "('GOOGL', 301.5, cast('2023-01-01T09:08:00.000000Z' as #TIMESTAMP));",
                rightTableTimestampType.getTypeName()
        );

        if (extraData) {
            executeWithRewriteTimestamp(
                    "insert into trades values " +
                            "('TSLA', 400.0, cast('2023-01-01T09:10:00.000000Z' as #TIMESTAMP))," +
                            "('TSLA', 401.0, cast('2023-01-01T09:11:00.000000Z' as #TIMESTAMP))," +
                            "('AMZN', 500.0, cast('2023-01-01T09:12:00.000000Z' as #TIMESTAMP))," +
                            "('AMZN', 501.0, cast('2023-01-01T09:13:00.000000Z' as #TIMESTAMP))," +
                            "('META', 600.0, cast('2023-01-01T09:14:00.000000Z' as #TIMESTAMP))," +
                            "('META', 601.0, cast('2023-01-01T09:15:00.000000Z' as #TIMESTAMP))," +
                            "('TSLA', 402.0, cast('2023-01-01T09:16:00.000000Z' as #TIMESTAMP))," +
                            "('AMZN', 502.0, cast('2023-01-01T09:17:00.000000Z' as #TIMESTAMP))," +
                            "('META', 602.0, cast('2023-01-01T09:18:00.000000Z' as #TIMESTAMP))," +
                            "('NFLX', 700.0, cast('2023-01-01T09:19:00.000000Z' as #TIMESTAMP));",
                    leftTableTimestampType.getTypeName()
            );

            executeWithRewriteTimestamp(
                    "insert into prices values " +
                            "('TSLA', 399.5, cast('2023-01-01T09:09:00.000000Z' as #TIMESTAMP))," +
                            "('TSLA', 400.5, cast('2023-01-01T09:10:00.000000Z' as #TIMESTAMP))," +
                            "('AMZN', 499.5, cast('2023-01-01T09:11:00.000000Z' as #TIMESTAMP))," +
                            "('AMZN', 500.5, cast('2023-01-01T09:12:00.000000Z' as #TIMESTAMP))," +
                            "('META', 599.5, cast('2023-01-01T09:13:00.000000Z' as #TIMESTAMP))," +
                            "('META', 600.5, cast('2023-01-01T09:14:00.000000Z' as #TIMESTAMP))," +
                            "('TSLA', 401.5, cast('2023-01-01T09:15:00.000000Z' as #TIMESTAMP))," +
                            "('AMZN', 501.5, cast('2023-01-01T09:16:00.000000Z' as #TIMESTAMP))," +
                            "('META', 601.5, cast('2023-01-01T09:17:00.000000Z' as #TIMESTAMP))," +
                            "('NFLX', 699.5, cast('2023-01-01T09:18:00.000000Z' as #TIMESTAMP));",
                    rightTableTimestampType.getTypeName()
            );
        }
    }

    private void prepareTable() throws SqlException {
        prepareTable(false);
    }
}
