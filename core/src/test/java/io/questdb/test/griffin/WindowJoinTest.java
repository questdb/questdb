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
        });
    }

    private void prepareTable() throws SqlException {
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
    }
}
