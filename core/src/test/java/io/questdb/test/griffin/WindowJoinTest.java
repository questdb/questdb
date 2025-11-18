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

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Ignore;
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

    @Override
    @Before
    public void setUp() {
        super.setUp();
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MIN_ROWS, 4);
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MAX_ROWS, 8);
    }

    @Test
    public void testAggregateNotTrivialColumn() throws Exception {
        assertMemoryLeak(() -> {
            prepareTable();
            String expect = replaceTimestampSuffix("sym\tprice\tts\twindow_price\n" +
                    "AAPL\t100.0\t2023-01-01T09:00:00.000000Z\t304.5\n" +
                    "AAPL\t101.0\t2023-01-01T09:01:00.000000Z\t204.0\n" +
                    "AAPL\t102.0\t2023-01-01T09:02:00.000000Z\t102.5\n" +
                    "MSFT\t200.0\t2023-01-01T09:03:00.000000Z\t402.0\n" +
                    "MSFT\t201.0\t2023-01-01T09:04:00.000000Z\t201.5\n" +
                    "GOOGL\t300.0\t2023-01-01T09:05:00.000000Z\t602.0\n" +
                    "GOOGL\t301.0\t2023-01-01T09:06:00.000000Z\t301.5\n" +
                    "AAPL\t103.0\t2023-01-01T09:07:00.000000Z\t103.5\n" +
                    "MSFT\t202.0\t2023-01-01T09:08:00.000000Z\t202.5\n" +
                    "GOOGL\t302.0\t2023-01-01T09:09:00.000000Z\t302.5\n", leftTableTimestampType.getTypeName());
            assertQueryAndPlan(
                    expect,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Async Window Fast Join workers: 1\n" +
                            "      vectorized: true\n" +
                            "      symbol: sym=sym\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding\n" +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " following\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: trades\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: prices\n",
                    "select t.*, sum(p.price + 1) as window_price " +
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
                    "select t.*, sum(p.price + 1) window_price " +
                            "from trades t " +
                            "left join prices p " +
                            "on (t.sym = p.sym) " +
                            " and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) " +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    true
            );

            expect = replaceTimestampSuffix("sym\tprice\tts\twindow_price\n" +
                    "AAPL\t100.0\t2023-01-01T09:00:00.000000Z\t304.5\n" +
                    "AAPL\t101.0\t2023-01-01T09:01:00.000000Z\t404.5\n" +
                    "AAPL\t102.0\t2023-01-01T09:02:00.000000Z\t504.5\n" +
                    "MSFT\t200.0\t2023-01-01T09:03:00.000000Z\t702.5\n" +
                    "MSFT\t201.0\t2023-01-01T09:04:00.000000Z\t803.5\n" +
                    "GOOGL\t300.0\t2023-01-01T09:05:00.000000Z\t705.5\n" +
                    "GOOGL\t301.0\t2023-01-01T09:06:00.000000Z\t607.5\n" +
                    "AAPL\t103.0\t2023-01-01T09:07:00.000000Z\t608.5\n" +
                    "MSFT\t202.0\t2023-01-01T09:08:00.000000Z\t505.0\n" +
                    "GOOGL\t302.0\t2023-01-01T09:09:00.000000Z\t302.5\n", leftTableTimestampType.getTypeName());
            assertQueryAndPlan(
                    expect,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Async Window Join workers: 1\n" +
                            "      vectorized: true\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding\n" +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " following\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: trades\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: prices\n",
                    "select t.*, sum(p.price + 1) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            " range between 1 minute preceding and 1 minute following " +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );

            // verify result
            assertQuery(
                    expect,
                    "select t.*, sum(p.price + 1) window_price " +
                            "from trades t " +
                            "left join prices p " +
                            "on p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) " +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    true
            );

            expect = replaceTimestampSuffix("sym\tprice\tts\tcount\n" +
                    "AAPL\t100.0\t2023-01-01T09:00:00.000000Z\t3\n" +
                    "AAPL\t101.0\t2023-01-01T09:01:00.000000Z\t3\n" +
                    "AAPL\t102.0\t2023-01-01T09:02:00.000000Z\t3\n" +
                    "MSFT\t200.0\t2023-01-01T09:03:00.000000Z\t3\n" +
                    "MSFT\t201.0\t2023-01-01T09:04:00.000000Z\t3\n" +
                    "GOOGL\t300.0\t2023-01-01T09:05:00.000000Z\t3\n" +
                    "GOOGL\t301.0\t2023-01-01T09:06:00.000000Z\t3\n" +
                    "AAPL\t103.0\t2023-01-01T09:07:00.000000Z\t3\n" +
                    "MSFT\t202.0\t2023-01-01T09:08:00.000000Z\t2\n" +
                    "GOOGL\t302.0\t2023-01-01T09:09:00.000000Z\t1\n", leftTableTimestampType.getTypeName());
            assertQueryAndPlan(
                    expect,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Async Window Join workers: 1\n" +
                            "      vectorized: false\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding\n" +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " following\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: trades\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: prices\n",
                    "select t.*, count() " +
                            "from trades t " +
                            "window join prices p " +
                            " range between 1 minute preceding and 1 minute following " +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );

            // verify result
            assertQuery(
                    expect,
                    "select t.*, count()  " +
                            "from trades t " +
                            "left join prices p " +
                            "on p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) " +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testAggregateNotTrivialColumnCastRequired() throws Exception {
        // timestamp types don't matter for this test
        Assume.assumeTrue(leftTableTimestampType == TestTimestampType.MICRO);
        Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);
        assertMemoryLeak(() -> {
            execute(
                    "create table x (" +
                            "  s symbol," +
                            "  ts timestamp" +
                            ") timestamp(ts) partition by day;"
            );
            execute(
                    "create table y (" +
                            "  s symbol," +
                            "  x long," +
                            "  ts timestamp" +
                            ") timestamp(ts) partition by day;"
            );

            execute(
                    "insert into x values " +
                            "('sym0', '2023-01-01T09:00:00.000000Z');"
            );
            execute(
                    "insert into y values " +
                            "('sym0', null, '2023-01-01T08:59:58.000000Z')," +
                            "('sym0', null, '2023-01-01T08:59:59.000000Z')," +
                            "('sym0', 1, '2023-01-01T09:00:00.000000Z')," +
                            "('sym0', 2, '2023-01-01T09:00:01.000000Z')," +
                            "('sym0', 3, '2023-01-01T09:00:02.000000Z');"
            );

            assertQueryAndPlan(
                    """
                            s\tts\tavg_y
                            sym0\t2023-01-01T09:00:00.000000Z\t1.0
                            """,
                    """
                            Sort
                              keys: [ts, s]
                                Async Window Fast Join workers: 1
                                  vectorized: true
                                  symbol: s=s
                                  window lo: 1000000 preceding
                                  window hi: 1000000 following
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: x
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: y
                            """,
                    "select x.*, avg(y.x) as avg_y " +
                            "from x " +
                            "window join y " +
                            "on (x.s = y.s) " +
                            " range between 1 second preceding and 1 second following " +
                            "order by x.ts, x.s;",
                    "ts",
                    true,
                    false
            );
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
                            "      vectorized: true\n" +
                            "      symbol: sym=sym\n" +
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
    public void testCalcSize() throws Exception {
        assertMemoryLeak(() -> {
            prepareTable(true);
            assertSkipToAndCalculateSize("select t.*, sum(t.price) as window_price " +
                    "from trades t " +
                    "window join prices p " +
                    "on (t.sym = p.sym) " +
                    " range between 1 minute preceding and 1 minute following " +
                    "order by t.ts, t.sym", 20, true);
            assertSkipToAndCalculateSize("select t.*, sum(t.price) as window_price " +
                    "from trades t " +
                    "window join prices p " +
                    " range between 1 minute preceding and 1 minute following " +
                    "order by t.ts, t.sym", 20, true);
            assertSkipToAndCalculateSize("select t.*, sum(t.price) as window_price " +
                    "from trades t " +
                    "window join prices p " +
                    "on t.sym = p.sym " +
                    " range between 1 minute preceding and 1 minute following " +
                    " where t.price < 400 " +
                    "order by t.ts, t.sym", 10, true);
            assertSkipToAndCalculateSize("select t.*, sum(t.price) as window_price " +
                    "from trades t " +
                    "window join prices p " +
                    " range between 1 minute preceding and 1 minute following " +
                    " where t.price < 400 " +
                    "order by t.ts, t.sym", 10, true);
        });
    }

    @Test
    public void testFastJoinWithJoinFilter() throws Exception {
        assertMemoryLeak(() -> {
            prepareTable();
            String expect = replaceTimestampSuffix("sym\tprice\tts\twindow_price\n" +
                    "AAPL\t100.0\t2023-01-01T09:00:00.000000Z\t100.5\n" +
                    "AAPL\t101.0\t2023-01-01T09:01:00.000000Z\t100.5\n" +
                    "AAPL\t102.0\t2023-01-01T09:02:00.000000Z\t101.0\n" +
                    "MSFT\t200.0\t2023-01-01T09:03:00.000000Z\t200.0\n" +
                    "MSFT\t201.0\t2023-01-01T09:04:00.000000Z\t200.0\n" +
                    "GOOGL\t300.0\t2023-01-01T09:05:00.000000Z\t299.5\n" +
                    "GOOGL\t301.0\t2023-01-01T09:06:00.000000Z\t299.5\n" +
                    "AAPL\t103.0\t2023-01-01T09:07:00.000000Z\t102.5\n" +
                    "MSFT\t202.0\t2023-01-01T09:08:00.000000Z\t201.5\n" +
                    "GOOGL\t302.0\t2023-01-01T09:09:00.000000Z\tnull\n", leftTableTimestampType.getTypeName());
            assertQueryAndPlan(
                    expect,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Async Window Fast Join workers: 1\n" +
                            "      vectorized: false\n" +
                            "      symbol: sym=sym\n" +
                            "      join filter: p.price<300\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " preceding\n" +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " following\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: trades\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: prices\n",
                    "select t.*, avg(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on (t.sym = p.sym) and p.price < 300 " +
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
                            "from trades t " +
                            "left join prices p " +
                            "on (t.sym = p.sym)" +
                            " and p.ts >= dateadd('m', -2, t.ts) AND p.ts <= dateadd('m', 2, t.ts) and p.price < 300 " +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    true
            );

            expect = replaceTimestampSuffix("sym\tprice\tts\twindow_price\n" +
                    "AAPL\t100.0\t2023-01-01T09:00:00.000000Z\t100.5\n" +
                    "AAPL\t101.0\t2023-01-01T09:01:00.000000Z\t100.5\n" +
                    "AAPL\t102.0\t2023-01-01T09:02:00.000000Z\t101.0\n" +
                    "MSFT\t200.0\t2023-01-01T09:03:00.000000Z\t200.0\n" +
                    "MSFT\t201.0\t2023-01-01T09:04:00.000000Z\t200.0\n" +
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
                            "      vectorized: true\n" +
                            "      symbol: sym=sym\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " preceding\n" +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " following\n" +
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
                            "from trades t " +
                            "left join prices p " +
                            "on (t.sym = p.sym)" +
                            " and p.ts >= dateadd('m', -2, t.ts) AND p.ts <= dateadd('m', 2, t.ts)  " +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    true
            );

            expect = replaceTimestampSuffix("sym\tprice\tts\twindow_price\n" +
                    "AAPL\t100.0\t2023-01-01T09:00:00.000000Z\t100.5\n" +
                    "AAPL\t101.0\t2023-01-01T09:01:00.000000Z\t100.5\n" +
                    "AAPL\t102.0\t2023-01-01T09:02:00.000000Z\t101.0\n" +
                    "MSFT\t200.0\t2023-01-01T09:03:00.000000Z\t199.5\n" +
                    "MSFT\t201.0\t2023-01-01T09:04:00.000000Z\t199.5\n" +
                    "GOOGL\t300.0\t2023-01-01T09:05:00.000000Z\t300.5\n" +
                    "GOOGL\t301.0\t2023-01-01T09:06:00.000000Z\t301.0\n" +
                    "AAPL\t103.0\t2023-01-01T09:07:00.000000Z\t102.5\n" +
                    "MSFT\t202.0\t2023-01-01T09:08:00.000000Z\tnull\n" +
                    "GOOGL\t302.0\t2023-01-01T09:09:00.000000Z\t301.5\n", leftTableTimestampType.getTypeName());
            assertQueryAndPlan(
                    expect,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Async Window Fast Join workers: 1\n" +
                            "      vectorized: false\n" +
                            "      symbol: sym=sym\n" +
                            "      join filter: (p.price<200 or 300<p.price)\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " preceding\n" +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " following\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: trades\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: prices\n",
                    "select t.*, avg(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on (p.price < 200 or p.price > 300 ) AND (t.sym = p.sym) " +
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
                            "from trades t " +
                            "left join prices p " +
                            "on (t.sym = p.sym)" +
                            " and p.ts >= dateadd('m', -2, t.ts) AND p.ts <= dateadd('m', 2, t.ts) and (p.price < 200 or p.price > 300 ) " +
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
                    "MSFT\t202.0\t2023-01-01T09:08:00.000000Z\tnull\n" +
                    "GOOGL\t302.0\t2023-01-01T09:09:00.000000Z\tnull\n", leftTableTimestampType.getTypeName());
            assertQueryAndPlan(
                    expect,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Async Window Fast Join workers: 1\n" +
                            "      vectorized: false\n" +
                            "      symbol: sym=sym\n" +
                            "      join filter: (p.price<200 and 300<p.price)\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " preceding\n" +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " following\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: trades\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: prices\n",
                    "select t.*, avg(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on p.price < 200 and p.price > 300 AND (t.sym = p.sym) " +
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
                            "from trades t " +
                            "left join prices p " +
                            "on (t.sym = p.sym)" +
                            " and p.ts >= dateadd('m', -2, t.ts) AND p.ts <= dateadd('m', 2, t.ts) and (p.price < 200 and p.price > 300 ) " +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    true
            );


            expect = replaceTimestampSuffix("sym\tprice\tts\twindow_price\n" +
                    "AAPL\t100.0\t2023-01-01T09:00:00.000000Z\t100.5\n" +
                    "AAPL\t101.0\t2023-01-01T09:01:00.000000Z\t100.5\n" +
                    "AAPL\t102.0\t2023-01-01T09:02:00.000000Z\t101.0\n" +
                    "MSFT\t200.0\t2023-01-01T09:03:00.000000Z\t200.0\n" +
                    "MSFT\t201.0\t2023-01-01T09:04:00.000000Z\t200.0\n" +
                    "GOOGL\t300.0\t2023-01-01T09:05:00.000000Z\t299.5\n" +
                    "GOOGL\t301.0\t2023-01-01T09:06:00.000000Z\t299.5\n" +
                    "AAPL\t103.0\t2023-01-01T09:07:00.000000Z\t102.5\n" +
                    "MSFT\t202.0\t2023-01-01T09:08:00.000000Z\t201.5\n" +
                    "GOOGL\t302.0\t2023-01-01T09:09:00.000000Z\tnull\n", leftTableTimestampType.getTypeName());
            assertQueryAndPlan(
                    expect,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Async Window Fast Join workers: 1\n" +
                            "      vectorized: false\n" +
                            "      symbol: sym=sym\n" +
                            "      join filter: (1000<p.ts and p.price<300 and p.sym!='AAAAAA')\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " preceding\n" +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " following\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Interval forward scan on: trades\n" +
                            "              intervals: [(\"" + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "1970-01-01T00:00:00.001001Z" : "1970-01-01T00:00:00.000001001Z") + "\",\"MAX\")]\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Interval forward scan on: prices\n" +
                            "              intervals: [(\"" + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? (ColumnType.isTimestampMicro(rightTableTimestampType.getTimestampType()) ? "1969-12-31T23:58:00.001001Z" : "1969-12-31T23:58:00.001001000Z")
                            : (ColumnType.isTimestampMicro(rightTableTimestampType.getTimestampType()) ? "1969-12-31T23:58:00.000001Z" : "1969-12-31T23:58:00.000001001Z")) + "\",\"MAX\")]\n",
                    "select t.*, avg(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on p.ts > 1000 AND (t.sym = p.sym) and p.price < 300 and p.sym != 'AAAAAA'" +
                            " range between 2 minute preceding and 2 minute following " +
                            " where t.ts > 1000 " +
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
                            "on t.ts > 1000 and p.ts > 1000 AND (t.sym = p.sym) and p.price < 300 and p.sym != 'AAAAAA'" +
                            " and p.ts >= dateadd('m', -2, t.ts) AND p.ts <= dateadd('m', 2, t.ts) " +
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
                            "      vectorized: true\n" +
                            "      symbol: sym=sym\n" +
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
                            "on (t.sym = p.sym) " +
                            " range between 2 minute preceding and 2 minute following " +
                            " where t.price < 300 " +
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
                            "          vectorized: true\n" +
                            "          symbol: sym=sym\n" +
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
                            "on (t.sym = p.sym) " +
                            " range between 2 minute preceding and 2 minute following " +
                            " where t.price < 300 " +
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
    public void testMasterHasIntervalFilter() throws Exception {
        assertMemoryLeak(() -> {
            prepareTable();
            String expect = replaceTimestampSuffix("sym\tprice\tts\twindow_price\n" +
                    "AAPL\t100.0\t2023-01-01T09:00:00.000000Z\t301.5\n" +
                    "AAPL\t101.0\t2023-01-01T09:01:00.000000Z\t202.0\n" +
                    "AAPL\t102.0\t2023-01-01T09:02:00.000000Z\t101.5\n" +
                    "MSFT\t200.0\t2023-01-01T09:03:00.000000Z\t400.0\n" +
                    "MSFT\t201.0\t2023-01-01T09:04:00.000000Z\t200.5\n", leftTableTimestampType.getTypeName());
            assertQueryAndPlan(
                    expect,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Async Window Fast Join workers: 1\n" +
                            "      vectorized: true\n" +
                            "      symbol: sym=sym\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding\n" +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " following\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Interval forward scan on: trades\n" +
                            "              intervals: [(\"MIN\",\"2023-01-01T09:04:00." + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "000000Z" : "000000000Z") + "\")]\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Interval forward scan on: prices\n" +
                            "              intervals: [(\"MIN\",\"" + (ColumnType.isTimestampMicro(rightTableTimestampType.getTimestampType()) ? "2023-01-01T09:05:00.000000Z" : "2023-01-01T09:05:00.000000000Z") + "\")]\n",
                    "select t.*, sum(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on (t.sym = p.sym) " +
                            " range between 1 minute preceding and 1 minute following " +
                            " where t.ts <= '2023-01-01T09:04:00.000000Z' " +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );

            assertQueryAndPlan(
                    expect,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Async Window Fast Join workers: 1\n" +
                            "      vectorized: true\n" +
                            "      symbol: sym=sym\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding\n" +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " following\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Interval forward scan on: trades\n" +
                            "              intervals: [(\"MIN\",\"2023-01-01T09:04:00." + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "000000Z" : "000000000Z") + "\")]\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Interval forward scan on: prices\n" +
                            "              intervals: [(\"MIN\",\"" + (ColumnType.isTimestampMicro(rightTableTimestampType.getTimestampType()) ? "2023-01-01T09:05:00.000000Z" : "2023-01-01T09:05:00.000000000Z") + "\")]\n",
                    "declare @x := '2023-01-01T09:04:00.000000Z' select t.*, sum(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on (t.sym = p.sym)  " +
                            " range between 1 minute preceding and 1 minute following " +
                            " where t.ts <= @x " +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );

            // verify result
            assertQuery(
                    expect,
                    "select t.*, sum(p.price) window_price " +
                            "from (select * from trades where ts <= '2023-01-01T09:04:00.000000Z') t " +
                            "left join prices p " +
                            "on (t.sym = p.sym) " +
                            " and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) " +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    true
            );

            expect = replaceTimestampSuffix("sym\tprice\tts\twindow_price\n" +
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
                            "      vectorized: true\n" +
                            "      symbol: sym=sym\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding\n" +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " following\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Interval forward scan on: trades\n" +
                            "              intervals: [(\"2023-01-01T09:04:00." + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "000001Z" : "000000001Z") + "\",\"MAX\")]\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Interval forward scan on: prices\n" +
                            "              intervals: [(\"" + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? (ColumnType.isTimestampMicro(rightTableTimestampType.getTimestampType()) ? "2023-01-01T09:03:00.000001Z" : "2023-01-01T09:03:00.000001000Z")
                            : (ColumnType.isTimestampMicro(rightTableTimestampType.getTimestampType()) ? "2023-01-01T09:03:00.000000Z" : "2023-01-01T09:03:00.000000001Z")) + "\",\"MAX\")]\n",
                    "select t.*, sum(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on (t.sym = p.sym)  " +
                            " range between 1 minute preceding and 1 minute following " +
                            " where t.ts > '2023-01-01T09:04:00.000000Z'" +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );

            // verify result
            assertQuery(
                    expect,
                    "select t.*, sum(p.price) window_price " +
                            "from (select * from trades where ts > '2023-01-01T09:04:00.000000Z') t " +
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
    public void testMultiAggregateColumns() throws Exception {
        assertMemoryLeak(() -> {
            Assume.assumeTrue(leftTableTimestampType.getTimestampType() == rightTableTimestampType.getTimestampType());
            prepareTable();
            String expect = replaceTimestampSuffix("sym\tprice\tts\tsum\tmax\tavg\tmin\n" +
                    "AAPL\t100.0\t2023-01-01T09:00:00.000000Z\t301.5\t2023-01-01T09:01:00.000000Z\t100.5\t2023-01-01T08:59:00.000000Z\n" +
                    "AAPL\t101.0\t2023-01-01T09:01:00.000000Z\t202.0\t2023-01-01T09:01:00.000000Z\t101.0\t2023-01-01T09:00:00.000000Z\n" +
                    "AAPL\t102.0\t2023-01-01T09:02:00.000000Z\t101.5\t2023-01-01T09:01:00.000000Z\t101.5\t2023-01-01T09:01:00.000000Z\n" +
                    "MSFT\t200.0\t2023-01-01T09:03:00.000000Z\t400.0\t2023-01-01T09:03:00.000000Z\t200.0\t2023-01-01T09:02:00.000000Z\n" +
                    "MSFT\t201.0\t2023-01-01T09:04:00.000000Z\t200.5\t2023-01-01T09:03:00.000000Z\t200.5\t2023-01-01T09:03:00.000000Z\n" +
                    "GOOGL\t300.0\t2023-01-01T09:05:00.000000Z\t600.0\t2023-01-01T09:05:00.000000Z\t300.0\t2023-01-01T09:04:00.000000Z\n" +
                    "GOOGL\t301.0\t2023-01-01T09:06:00.000000Z\t300.5\t2023-01-01T09:05:00.000000Z\t300.5\t2023-01-01T09:05:00.000000Z\n" +
                    "AAPL\t103.0\t2023-01-01T09:07:00.000000Z\t102.5\t2023-01-01T09:06:00.000000Z\t102.5\t2023-01-01T09:06:00.000000Z\n" +
                    "MSFT\t202.0\t2023-01-01T09:08:00.000000Z\t201.5\t2023-01-01T09:07:00.000000Z\t201.5\t2023-01-01T09:07:00.000000Z\n" +
                    "GOOGL\t302.0\t2023-01-01T09:09:00.000000Z\t301.5\t2023-01-01T09:08:00.000000Z\t301.5\t2023-01-01T09:08:00.000000Z\n", leftTableTimestampType.getTypeName());
            assertQueryAndPlan(
                    expect,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Async Window Fast Join workers: 1\n" +
                            "      vectorized: true\n" +
                            "      symbol: sym=sym\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding\n" +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " following\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: trades\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: prices\n",
                    "select t.*, sum(p.price), max(p.ts), avg(p.price), min(p.ts) " +
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
                    "select t.*, sum(p.price), max(p.ts), avg(p.price), min(p.ts) " +
                            "from trades t " +
                            "left join prices p " +
                            "on (t.sym = p.sym) " +
                            " and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) " +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    true
            );

            expect = replaceTimestampSuffix("sym\tprice\tts\tmax\tsum\tavg\tmin\n" +
                    "AAPL\t100.0\t2023-01-01T09:00:00.000000Z\t2023-01-01T09:01:00.000000Z\t301.5\t100.5\t2023-01-01T08:59:00.000000Z\n" +
                    "AAPL\t101.0\t2023-01-01T09:01:00.000000Z\t2023-01-01T09:02:00.000000Z\t401.5\t133.83333333333334\t2023-01-01T09:00:00.000000Z\n" +
                    "AAPL\t102.0\t2023-01-01T09:02:00.000000Z\t2023-01-01T09:03:00.000000Z\t501.5\t167.16666666666666\t2023-01-01T09:01:00.000000Z\n" +
                    "MSFT\t200.0\t2023-01-01T09:03:00.000000Z\t2023-01-01T09:04:00.000000Z\t699.5\t233.16666666666666\t2023-01-01T09:02:00.000000Z\n" +
                    "MSFT\t201.0\t2023-01-01T09:04:00.000000Z\t2023-01-01T09:05:00.000000Z\t800.5\t266.8333333333333\t2023-01-01T09:03:00.000000Z\n" +
                    "GOOGL\t300.0\t2023-01-01T09:05:00.000000Z\t2023-01-01T09:06:00.000000Z\t702.5\t234.16666666666666\t2023-01-01T09:04:00.000000Z\n" +
                    "GOOGL\t301.0\t2023-01-01T09:06:00.000000Z\t2023-01-01T09:07:00.000000Z\t604.5\t201.5\t2023-01-01T09:05:00.000000Z\n" +
                    "AAPL\t103.0\t2023-01-01T09:07:00.000000Z\t2023-01-01T09:08:00.000000Z\t605.5\t201.83333333333334\t2023-01-01T09:06:00.000000Z\n" +
                    "MSFT\t202.0\t2023-01-01T09:08:00.000000Z\t2023-01-01T09:08:00.000000Z\t503.0\t251.5\t2023-01-01T09:07:00.000000Z\n" +
                    "GOOGL\t302.0\t2023-01-01T09:09:00.000000Z\t2023-01-01T09:08:00.000000Z\t301.5\t301.5\t2023-01-01T09:08:00.000000Z\n", leftTableTimestampType.getTypeName());
            assertQueryAndPlan(
                    expect,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Async Window Join workers: 1\n" +
                            "      vectorized: true\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding\n" +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " following\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: trades\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: prices\n",
                    "select t.*, max(p.ts), sum(p.price), avg(p.price), min(p.ts) " +
                            "from trades t " +
                            "window join prices p " +
                            " range between 1 minute preceding and 1 minute following " +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );

            // verify result
            assertQuery(
                    expect,
                    "select t.*, max(p.ts), sum(p.price), avg(p.price), min(p.ts) " +
                            "from trades t " +
                            "left join prices p " +
                            " on p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) " +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    true
            );

            expect = replaceTimestampSuffix("sym\tprice\tts\tmax\tsum\tavg\tmin\tavg1\n" +
                    "AAPL\t100.0\t2023-01-01T09:00:00.000000Z\t2023-01-01T09:01:00.000000Z\t601.5\t200.5\t2023-01-01T08:59:00.000000Z\t201.5\n" +
                    "AAPL\t101.0\t2023-01-01T09:01:00.000000Z\t2023-01-01T09:02:00.000000Z\t701.5\t233.83333333333334\t2023-01-01T09:00:00.000000Z\t234.83333333333334\n" +
                    "AAPL\t102.0\t2023-01-01T09:02:00.000000Z\t2023-01-01T09:03:00.000000Z\t801.5\t267.1666666666667\t2023-01-01T09:01:00.000000Z\t268.1666666666667\n" +
                    "MSFT\t200.0\t2023-01-01T09:03:00.000000Z\t2023-01-01T09:04:00.000000Z\t999.5\t333.1666666666667\t2023-01-01T09:02:00.000000Z\t334.1666666666667\n" +
                    "MSFT\t201.0\t2023-01-01T09:04:00.000000Z\t2023-01-01T09:05:00.000000Z\t1100.5\t366.8333333333333\t2023-01-01T09:03:00.000000Z\t367.8333333333333\n" +
                    "GOOGL\t300.0\t2023-01-01T09:05:00.000000Z\t2023-01-01T09:06:00.000000Z\t1002.5\t334.1666666666667\t2023-01-01T09:04:00.000000Z\t335.1666666666667\n" +
                    "GOOGL\t301.0\t2023-01-01T09:06:00.000000Z\t2023-01-01T09:07:00.000000Z\t904.5\t301.5\t2023-01-01T09:05:00.000000Z\t302.5\n" +
                    "AAPL\t103.0\t2023-01-01T09:07:00.000000Z\t2023-01-01T09:08:00.000000Z\t905.5\t301.8333333333333\t2023-01-01T09:06:00.000000Z\t302.8333333333333\n" +
                    "MSFT\t202.0\t2023-01-01T09:08:00.000000Z\t2023-01-01T09:08:00.000000Z\t703.0\t351.5\t2023-01-01T09:07:00.000000Z\t352.5\n" +
                    "GOOGL\t302.0\t2023-01-01T09:09:00.000000Z\t2023-01-01T09:08:00.000000Z\t401.5\t401.5\t2023-01-01T09:08:00.000000Z\t402.5\n", leftTableTimestampType.getTypeName());
            assertQueryAndPlan(
                    expect,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Async Window Join workers: 1\n" +
                            "      vectorized: true\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding\n" +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " following\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: trades\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: prices\n",
                    "select t.*, max(p.ts), sum(p.price + 100), avg(p.price + 100), min(p.ts), avg(p.price + 101) " +
                            "from trades t " +
                            "window join prices p " +
                            " range between 1 minute preceding and 1 minute following " +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
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
                            "      vectorized: true\n" +
                            "      symbol: sym=sym\n" +
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
                    "Async Window Join workers: 1\n" +
                            "  vectorized: true\n" +
                            "  window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "900000000" : "900000000000") + " preceding\n" +
                            "  window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "840000000" : "840000000000") + " preceding\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: trades\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: prices\n",
                    "select t.*, sum(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            " range between 15 minute preceding and 14 minute preceding " +
                            "order by t.ts;",
                    "ts",
                    false,
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
                    "Async Window Fast Join workers: 1\n" +
                            "  vectorized: true\n" +
                            "  symbol: sym=sym\n" +
                            "  window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "300000000" : "300000000000") + " preceding\n" +
                            "  window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "240000000" : "240000000000") + " preceding\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: trades\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: prices\n",
                    "select t.*, sum(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p on t.sym = p.sym" +
                            " range between 5 minute preceding and 4 minute preceding " +
                            "order by t.ts;",
                    "ts",
                    false,
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
    public void testWindowJoinChain() throws Exception {
        assertMemoryLeak(() -> {
            prepareTable();
            String expect = replaceTimestampSuffix("ts\tsym\twindow_price1\twindow_price2\n" +
                    "2023-01-01T09:00:00.000000Z\tAAPL\t301.5\t301.5\n" +
                    "2023-01-01T09:01:00.000000Z\tAAPL\t202.0\t301.5\n" +
                    "2023-01-01T09:02:00.000000Z\tAAPL\t101.5\t202.0\n" +
                    "2023-01-01T09:03:00.000000Z\tMSFT\t400.0\t400.0\n" +
                    "2023-01-01T09:04:00.000000Z\tMSFT\t200.5\t400.0\n" +
                    "2023-01-01T09:05:00.000000Z\tGOOGL\t600.0\t600.0\n" +
                    "2023-01-01T09:06:00.000000Z\tGOOGL\t300.5\t901.5\n" +
                    "2023-01-01T09:07:00.000000Z\tAAPL\t102.5\t102.5\n" +
                    "2023-01-01T09:08:00.000000Z\tMSFT\t201.5\t201.5\n" +
                    "2023-01-01T09:09:00.000000Z\tGOOGL\t301.5\t301.5\n", leftTableTimestampType.getTypeName());
            assertQueryAndPlan(
                    expect,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Window Fast Join\n" +
                            "      symbol: t.sym=sym\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " preceding\n" +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " following\n" +
                            "        Async Window Fast Join workers: 1\n" +
                            "          vectorized: true\n" +
                            "          symbol: sym=sym\n" +
                            "          window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding\n" +
                            "          window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " following\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: trades\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: prices\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: prices\n",
                    "select t.ts, t.sym, sum(p.price) as window_price1, sum(p1.price) as window_price2 " +
                            "from trades t " +
                            "window join prices p " +
                            "on (t.sym = p.sym) " +
                            " range between 1 minute preceding and 1 minute following " +
                            "window join prices p1 " +
                            "on (t.sym = p1.sym) " +
                            " range between 2 minute preceding and 2 minute following " +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );

            expect = replaceTimestampSuffix("ts\tsym\twindow_price2\n" +
                    "2023-01-01T09:00:00.000000Z\tAAPL\t301.5\n" +
                    "2023-01-01T09:01:00.000000Z\tAAPL\t301.5\n" +
                    "2023-01-01T09:02:00.000000Z\tAAPL\t202.0\n" +
                    "2023-01-01T09:03:00.000000Z\tMSFT\t400.0\n" +
                    "2023-01-01T09:04:00.000000Z\tMSFT\t400.0\n" +
                    "2023-01-01T09:05:00.000000Z\tGOOGL\t600.0\n" +
                    "2023-01-01T09:06:00.000000Z\tGOOGL\t901.5\n" +
                    "2023-01-01T09:07:00.000000Z\tAAPL\t102.5\n" +
                    "2023-01-01T09:08:00.000000Z\tMSFT\t201.5\n" +
                    "2023-01-01T09:09:00.000000Z\tGOOGL\t301.5\n", leftTableTimestampType.getTypeName());
            assertQueryAndPlan(
                    expect,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Window Fast Join\n" +
                            "      symbol: t.sym=sym\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " preceding\n" +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " following\n" +
                            "        Async Window Fast Join workers: 1\n" +
                            "          vectorized: false\n" +
                            "          symbol: sym=sym\n" +
                            "          window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding\n" +
                            "          window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " following\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: trades\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: prices\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: prices\n",
                    "select t.ts, t.sym,  sum(p1.price) as window_price2 " +
                            "from trades t " +
                            "window join prices p " +
                            "on (t.sym = p.sym) " +
                            " range between 1 minute preceding and 1 minute following " +
                            "window join prices p1 " +
                            "on (t.sym = p1.sym) " +
                            " range between 2 minute preceding and 2 minute following " +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );

            assertExceptionNoLeakCheck(
                    "select t.ts, t.sym,  sum(p1.price), p.price as window_price2 " +
                            "from trades t " +
                            "window join prices p " +
                            "on (t.sym = p.sym) " +
                            " range between 1 minute preceding and 1 minute following " +
                            "window join prices p1 " +
                            "on (t.sym = p1.sym) " +
                            " range between 2 minute preceding and 2 minute following " +
                            "order by t.ts, t.sym;",
                    36,
                    "WINDOW join cannot reference right table non-aggregate column: p.price"
            );

            assertExceptionNoLeakCheck(
                    "select t.ts, t.sym,  sum(p1.price + p.price) as window_price2 " +
                            "from trades t " +
                            "window join prices p " +
                            "on (t.sym = p.sym) " +
                            " range between 1 minute preceding and 1 minute following " +
                            "window join prices p1 " +
                            "on (t.sym = p1.sym) " +
                            " range between 2 minute preceding and 2 minute following " +
                            "order by t.ts, t.sym;",
                    21,
                    "WINDOW join aggregate function cannot reference columns from multiple models"
            );
        });
    }

    @Test
    public void testWindowJoinFailsOnConstantNonBooleanJoinFilter() throws Exception {
        // timestamp types don't matter for this test
        Assume.assumeTrue(leftTableTimestampType == TestTimestampType.MICRO);
        Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);
        assertMemoryLeak(() -> {
            prepareTable();

            assertExceptionNoLeakCheck(
                    "select t.*, sum(p.price) sum_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on t.price-p.price " +
                            " range between 1 second preceding and 1 second following;",
                    80,
                    "boolean expression expected"
            );
        });
    }

    @Test
    public void testWindowJoinFailsOnIncludePrevailing() throws Exception {
        // timestamp types don't matter for this test
        Assume.assumeTrue(leftTableTimestampType == TestTimestampType.MICRO);
        Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);
        assertMemoryLeak(() -> {
            prepareTable();

            assertExceptionNoLeakCheck(
                    "select t.*, sum(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on (t.sym = p.sym) " +
                            " range between 2 minute preceding and 2 minute following including prevailing;",
                    162,
                    "including prevailing is not supported in WINDOW joins"
            );
        });
    }

    @Test
    public void testWindowJoinFailsOnInvalidBoundaries() throws Exception {
        // timestamp types don't matter for this test
        Assume.assumeTrue(leftTableTimestampType == TestTimestampType.MICRO);
        Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);
        assertMemoryLeak(() -> {
            prepareTable();

            assertExceptionNoLeakCheck(
                    "select t.*, sum(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on (t.sym = p.sym) " +
                            " range between 2 minute preceding and 4 minute preceding;",
                    133,
                    "WINDOW join hi value cannot be less than lo value"
            );
        });
    }

    @Test
    public void testWindowJoinFailsOnMasterColumnAggregate() throws Exception {
        // timestamp types don't matter for this test
        Assume.assumeTrue(leftTableTimestampType == TestTimestampType.MICRO);
        Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);
        assertMemoryLeak(() -> {
            prepareTable();

            assertExceptionNoLeakCheck(
                    "select t.*, p.price " +
                            "from trades t " +
                            "window join prices p " +
                            "on (t.sym = p.sym) " +
                            " range between 1 second preceding and 1 second following;",
                    12,
                    "WINDOW join cannot reference right table non-aggregate column: p.price"
            );
        });
    }

    @Test
    public void testWindowJoinFailsWhenSlaveDoesNotSupportTimeFrames() throws Exception {
        // timestamp types don't matter for this test
        Assume.assumeTrue(leftTableTimestampType == TestTimestampType.MICRO);
        Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);
        assertMemoryLeak(() -> {
            prepareTable();

            assertExceptionNoLeakCheck(
                    "select t.*, sum(p.price) as window_price " +
                            "from (trades limit 5) t " +
                            "window join (select * from prices where ts in '2023-01-01T09:03:00.000000Z' or ts = '2023-01-01T09:07:00.000000Z' or ts = '2023-01-01T09:08:00.000000Z') p " +
                            "on (t.sym = p.sym) " +
                            " range between 1 minute preceding and 1 minute following " +
                            "order by t.ts, t.sym;",
                    65,
                    "right side of window join must be a table, not sub-query"
            );
        });
    }

    @Test
    public void testWindowJoinFailsWhenUnboundedIsUsed() throws Exception {
        // timestamp types don't matter for this test
        Assume.assumeTrue(leftTableTimestampType == TestTimestampType.MICRO);
        Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);
        assertMemoryLeak(() -> {
            prepareTable();

            assertExceptionNoLeakCheck(
                    "select t.*, sum(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on t.sym = p.sym " +
                            " range between unbounded preceding and 1 day following;",
                    118,
                    "unbounded preceding/following is not supported in WINDOW joins"
            );

            assertExceptionNoLeakCheck(
                    "select t.*, sum(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on t.sym = p.sym " +
                            " range between 1 second preceding and unbounded following;",
                    141,
                    "unbounded preceding/following is not supported in WINDOW joins"
            );

            assertExceptionNoLeakCheck(
                    "select t.*, sum(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on t.sym = p.sym " +
                            " range between unbounded preceding and unbounded following;",
                    118,
                    "unbounded preceding/following is not supported in WINDOW joins"
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
                    "Async Window Join workers: 1\n" +
                            "  vectorized: true\n" +
                            "  window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " preceding\n" +
                            "  window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: trades\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: prices\n",
                    "select t.*, sum(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            " range between 2 minute preceding and 1 minute preceding " +
                            "order by t.ts;",
                    "ts",
                    false,
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
                    "Async Window Join workers: 1\n" +
                            "  vectorized: true\n" +
                            "  window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "180000000" : "180000000000") + " preceding\n" +
                            "  window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " preceding\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: trades\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: prices\n",
                    "select t.*, sum(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            " range between 3 minute preceding and 2 minute preceding " +
                            "order by t.ts;",
                    "ts",
                    false,
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

            expect = replaceTimestampSuffix("sym\tprice\tts\twindow_price\n" +
                    "AAPL\t100.0\t2023-01-01T09:00:00.000000Z\t301.5\n" +
                    "AAPL\t101.0\t2023-01-01T09:01:00.000000Z\t401.5\n" +
                    "AAPL\t102.0\t2023-01-01T09:02:00.000000Z\t501.5\n" +
                    "MSFT\t200.0\t2023-01-01T09:03:00.000000Z\t699.5\n" +
                    "MSFT\t201.0\t2023-01-01T09:04:00.000000Z\t800.5\n" +
                    "AAPL\t103.0\t2023-01-01T09:07:00.000000Z\t605.5\n" +
                    "MSFT\t202.0\t2023-01-01T09:08:00.000000Z\t503.0\n", leftTableTimestampType.getTypeName());
            assertQueryAndPlan(
                    expect,
                    "Async Window Join workers: 1\n" +
                            "  vectorized: true\n" +
                            "  window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding\n" +
                            "  window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " following\n" +
                            "  master filter: price<300\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: trades\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: prices\n",
                    "select t.*, sum(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            " range between 1 minute preceding and 1 minute following " +
                            " where t.price < 300 " +
                            "order by t.ts;",
                    "ts",
                    false,
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
            );
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
                            "          vectorized: true\n" +
                            "          symbol: sym=sym\n" +
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
                            "          vectorized: false\n" +
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

    @Test
    public void testWindowJoinWithConstantFilter() throws Exception {
        // timestamp types don't matter for this test
        Assume.assumeTrue(leftTableTimestampType == TestTimestampType.MICRO);
        Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);
        assertMemoryLeak(() -> {
            prepareTable();

            assertQueryNoLeakCheck(
                    """
                            sym\tprice\tts\tsum_price
                            AAPL\t100.0\t2023-01-01T09:00:00.000000Z\t100.5
                            AAPL\t101.0\t2023-01-01T09:01:00.000000Z\t101.5
                            AAPL\t102.0\t2023-01-01T09:02:00.000000Z\tnull
                            """,
                    "select t.*, sum(p.price) sum_price " +
                            "from (trades limit 3) t " +
                            "window join prices p " +
                            "on (t.sym=p.sym) " +
                            " range between 1 second preceding and 1 second following " +
                            "where 42=42;",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    """
                            sym	price	ts	sum_price
                            """,
                    "select t.*, sum(p.price) sum_price " +
                            "from (trades limit 3) t " +
                            "window join prices p " +
                            "on (t.sym=p.sym) " +
                            " range between 1 second preceding and 1 second following " +
                            "where 42=43;",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testWindowJoinWithConstantJoinFilter() throws Exception {
        // timestamp types don't matter for this test
        Assume.assumeTrue(leftTableTimestampType == TestTimestampType.MICRO);
        Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);
        assertMemoryLeak(() -> {
            prepareTable();

            assertQueryNoLeakCheck(
                    """
                            sym\tprice\tts\tsum_price
                            AAPL\t100.0\t2023-01-01T09:00:00.000000Z\t100.5
                            AAPL\t101.0\t2023-01-01T09:01:00.000000Z\t101.5
                            AAPL\t102.0\t2023-01-01T09:02:00.000000Z\t199.5
                            """,
                    "select t.*, sum(p.price) sum_price " +
                            "from (trades limit 3) t " +
                            "window join prices p " +
                            "on (42=42) " +
                            " range between 1 second preceding and 1 second following;",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testWindowJoinWithMasterLimit() throws Exception {
        assertMemoryLeak(() -> {
            prepareTable();
            String expect = replaceTimestampSuffix("sym\tprice\tts\twindow_price\n" +
                    "AAPL\t100.0\t2023-01-01T09:00:00.000000Z\t301.5\n" +
                    "AAPL\t101.0\t2023-01-01T09:01:00.000000Z\t202.0\n" +
                    "AAPL\t102.0\t2023-01-01T09:02:00.000000Z\t101.5\n" +
                    "MSFT\t200.0\t2023-01-01T09:03:00.000000Z\t400.0\n" +
                    "MSFT\t201.0\t2023-01-01T09:04:00.000000Z\t200.5\n", leftTableTimestampType.getTypeName());
            assertQueryAndPlan(
                    expect,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Window Fast Join\n" +
                            "      symbol: sym=sym\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding\n" +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " following\n" +
                            "        Limit lo: 5 skip-over-rows: 0 limit: 5\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: trades\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: prices\n",
                    "select t.*, sum(p.price) as window_price " +
                            "from (trades limit 5) t " +
                            "window join prices p " +
                            "on (t.sym = p.sym) " +
                            " range between 1 minute preceding and 1 minute following " +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    true
            );

            assertQuery(
                    expect,
                    "select t.*, sum(p.price) window_price " +
                            "from (trades limit 5) t " +
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
    public void testWindowJoinWithMasterLimitOffsetFilter() throws Exception {
        assertMemoryLeak(() -> {
            prepareTable();
            String expect = replaceTimestampSuffix("sym\tprice\tts\twindow_price\n" +
                    "AAPL\t102.0\t2023-01-01T09:02:00.000000Z\t101.5\n" +
                    "MSFT\t200.0\t2023-01-01T09:03:00.000000Z\t400.0\n" +
                    "MSFT\t201.0\t2023-01-01T09:04:00.000000Z\t200.5\n", leftTableTimestampType.getTypeName());

            // fast factory
            assertQueryAndPlan(
                    expect,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Window Fast Join\n" +
                            "      symbol: sym=sym\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding\n" +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " following\n" +
                            "        Limit lo: 1 hi: 4 skip-over-rows: 1 limit: 3\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Interval forward scan on: trades\n" +
                            "                  intervals: [(\"2023-01-01T09:00:00." + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "000001" : "000000001") + "Z\",\"MAX\")]\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Interval forward scan on: prices\n" +
                            "              intervals: [(\"" + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? (ColumnType.isTimestampMicro(rightTableTimestampType.getTimestampType()) ? "2023-01-01T08:59:00.000001Z" : "2023-01-01T08:59:00.000001000Z")
                            : (ColumnType.isTimestampMicro(rightTableTimestampType.getTimestampType()) ? "2023-01-01T08:59:00.000000Z" : "2023-01-01T08:59:00.000000001Z")) + "\",\"MAX\")]\n",
                    "select t.*, sum(p.price) as window_price " +
                            "from (trades where ts > '2023-01-01T09:00:00Z' limit 1, 4) t " +
                            "window join prices p " +
                            "on (t.sym = p.sym) " +
                            " range between 1 minute preceding and 1 minute following " +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    true
            );

            assertQuery(
                    expect,
                    "select t.*, sum(p.price) window_price " +
                            "from (trades where ts > '2023-01-01T09:00:00Z' limit 1, 4) t " +
                            "left join prices p " +
                            "on (t.sym = p.sym) " +
                            " and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) " +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    true
            );

            // non-fast factory
            assertQueryAndPlan(
                    expect,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Window Join\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding\n" +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " following\n" +
                            "      join filter: concat([t.sym,'_0'])=concat([p.sym,'_0'])\n" +
                            "        Limit lo: 1 hi: 4 skip-over-rows: 1 limit: 3\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Interval forward scan on: trades\n" +
                            "                  intervals: [(\"2023-01-01T09:00:00." + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "000001" : "000000001") + "Z\",\"MAX\")]\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Interval forward scan on: prices\n" +
                            "              intervals: [(\"" + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? (ColumnType.isTimestampMicro(rightTableTimestampType.getTimestampType()) ? "2023-01-01T08:59:00.000001Z" : "2023-01-01T08:59:00.000001000Z")
                            : (ColumnType.isTimestampMicro(rightTableTimestampType.getTimestampType()) ? "2023-01-01T08:59:00.000000Z" : "2023-01-01T08:59:00.000000001Z")) + "\",\"MAX\")]\n",
                    "select t.*, sum(p.price) as window_price " +
                            "from (trades where ts > '2023-01-01T09:00:00Z' limit 1, 4) t " +
                            "window join prices p " +
                            "on (concat(t.sym, '_0') = concat(p.sym, '_0')) " +
                            " range between 1 minute preceding and 1 minute following " +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    true
            );

            assertQuery(
                    expect,
                    "select t.*, sum(p.price) window_price " +
                            "from (trades where ts > '2023-01-01T09:00:00Z' limit 1, 4) t " +
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

    @Ignore // FIXME window join should support this type filter
    @Test
    public void testWindowJoinWithPostFilter() throws Exception {
        // timestamp types don't matter for this test
        Assume.assumeTrue(leftTableTimestampType == TestTimestampType.MICRO);
        Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);
        assertMemoryLeak(() -> {
            prepareTable();

            assertQueryNoLeakCheck(
                    """
                            sym\tprice\tts\tsum_price
                            AAPL\t100.0\t2023-01-01T09:00:00.000000Z\t100.5
                            AAPL\t101.0\t2023-01-01T09:01:00.000000Z\t101.5
                            AAPL\t102.0\t2023-01-01T09:02:00.000000Z\tnull
                            """,
                    "select t.price, sum(p.price) sum_price " +
                            "from (trades limit 3) t " +
                            "window join prices p " +
                            "on (t.sym=p.sym) " +
                            " range between 1 second preceding and 1 second following " +
                            "where sum_price = 100.5;",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testWindowJoinWithRndFilter() throws Exception {
        // timestamp types don't matter for this test
        Assume.assumeTrue(leftTableTimestampType == TestTimestampType.MICRO);
        Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);
        assertMemoryLeak(() -> {
            prepareTable();

            assertQueryNoLeakCheck(
                    """
                            sym	sum_price
                            """,
                    "select t.sym, sum(p.price) sum_price " +
                            "from (trades limit 3) t " +
                            "window join prices p " +
                            "on (t.sym=p.sym) " +
                            " range between 1 second preceding and 1 second following " +
                            "where rnd_long() = 42;",
                    null,
                    false,
                    false
            );
        });
    }

    @Test
    public void testWithConstantJoinFilter() throws Exception {
        assertMemoryLeak(() -> {
            prepareTable();
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
                    "GOOGL\t302.0\t2023-01-01T09:09:00.000000Z\tnull\n", leftTableTimestampType.getTypeName());
            assertQueryAndPlan(
                    expect,
                    "Sort light\n" +
                            "  keys: [ts, sym]\n" +
                            "    ExtraNullColumnRecord\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: trades\n",
                    "select t.*, sum(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on (0 = 1) " +
                            " range between 1 minute preceding and 1 minute following " +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    true
            );

            // verify result
            assertQuery(
                    expect,
                    "select t.*, sum(p.price) window_price " +
                            "from trades t " +
                            "left join prices p " +
                            "on (0 = 1) " +
                            " and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) " +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    true
            );

            expect = replaceTimestampSuffix("sum\tt_price\tavg\tsym\n" +
                    "null\t100.0\tnull\tAAPL\n" +
                    "null\t101.0\tnull\tAAPL\n" +
                    "null\t102.0\tnull\tAAPL\n" +
                    "null\t103.0\tnull\tAAPL\n" +
                    "null\t300.0\tnull\tGOOGL\n" +
                    "null\t301.0\tnull\tGOOGL\n" +
                    "null\t302.0\tnull\tGOOGL\n" +
                    "null\t200.0\tnull\tMSFT\n" +
                    "null\t201.0\tnull\tMSFT\n" +
                    "null\t202.0\tnull\tMSFT\n", leftTableTimestampType.getTypeName());
            assertQueryAndPlan(
                    expect,
                    "Sort light\n" +
                            "  keys: [sym]\n" +
                            "    SelectedRecord\n" +
                            "        ExtraNullColumnRecord\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: trades\n",
                    "select sum(p.price), t.price t_price, avg(p.price), t.sym " +
                            "from trades t " +
                            "window join prices p " +
                            "on (0 = 1) " +
                            " range between 1 minute preceding and 1 minute following " +
                            "order by t.sym;",
                    null,
                    true,
                    true
            );

            // verify result
            assertQuery(
                    expect,
                    "select  sum(p.price), t.price t_price, avg(p.price), t.sym " +
                            "from trades t " +
                            "left join prices p " +
                            "on (0 = 1) " +
                            " and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) " +
                            "order by t.sym;",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testWithLimit() throws Exception {
        assertMemoryLeak(() -> {
            prepareTable();
            String expect = replaceTimestampSuffix("sym\tprice\tts\twindow_price\n" +
                    "AAPL\t100.0\t2023-01-01T09:00:00.000000Z\t100.5\n" +
                    "AAPL\t101.0\t2023-01-01T09:01:00.000000Z\t101.0\n" +
                    "AAPL\t102.0\t2023-01-01T09:02:00.000000Z\t101.5\n", leftTableTimestampType.getTypeName());
            assertQueryAndPlan(
                    expect,
                    "Limit lo: 3 skip-over-rows: 0 limit: 3\n" +
                            "    Async Window Fast Join workers: 1\n" +
                            "      vectorized: true\n" +
                            "      symbol: sym=sym\n" +
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
                            " limit 3",
                    "ts",
                    false,
                    true
            );

            // verify result
            assertQuery(
                    expect,
                    "select t.*, avg(p.price) window_price " +
                            "from trades t " +
                            "left join prices p " +
                            "on (t.sym = p.sym) " +
                            " and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) " +
                            "order by t.ts limit 3;",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testWithOnlyAggregateLeftTableColumn() throws Exception {
        assertMemoryLeak(() -> {
            prepareTable();
            String expect = replaceTimestampSuffix("sym\tprice\tts\twindow_price\n" +
                    "AAPL\t100.0\t2023-01-01T09:00:00.000000Z\t300.0\n" +
                    "AAPL\t101.0\t2023-01-01T09:01:00.000000Z\t202.0\n" +
                    "AAPL\t102.0\t2023-01-01T09:02:00.000000Z\t102.0\n" +
                    "MSFT\t200.0\t2023-01-01T09:03:00.000000Z\t400.0\n" +
                    "MSFT\t201.0\t2023-01-01T09:04:00.000000Z\t201.0\n" +
                    "GOOGL\t300.0\t2023-01-01T09:05:00.000000Z\t600.0\n" +
                    "GOOGL\t301.0\t2023-01-01T09:06:00.000000Z\t301.0\n" +
                    "AAPL\t103.0\t2023-01-01T09:07:00.000000Z\t103.0\n" +
                    "MSFT\t202.0\t2023-01-01T09:08:00.000000Z\t202.0\n" +
                    "GOOGL\t302.0\t2023-01-01T09:09:00.000000Z\t302.0\n", leftTableTimestampType.getTypeName());
            assertQueryAndPlan(
                    expect,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Async Window Fast Join workers: 1\n" +
                            "      vectorized: false\n" +
                            "      symbol: sym=sym\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding\n" +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " following\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: trades\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: prices\n",
                    "select t.*, sum(t.price) as window_price " +
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
                    "select t.*, sum(t.price) window_price " +
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

    private void assertSkipToAndCalculateSize(String select, int size, boolean expectedSize) throws Exception {
        assertQueryNoLeakCheck("count\n" + size + "\n", "select count(*) from (" + select + ")", null, false, expectedSize);
        RecordCursor.Counter counter = new RecordCursor.Counter();

        try (RecordCursorFactory factory = select(select)) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                Assert.assertEquals(size, counter.get());
                for (int i = 0; i < size + 2; i++) {
                    cursor.toTop();
                    counter.set(i);
                    cursor.skipRows(counter);
                    Assert.assertEquals(Math.max(i - size, 0), counter.get());
                    counter.clear();
                    cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                    Assert.assertEquals(Math.max(size - i, 0), counter.get());
                    cursor.toTop();
                    for (int j = 0; j < i; j++) {
                        if (!cursor.hasNext()) {
                            break;
                        }
                    }
                    counter.clear();
                    cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                    Assert.assertEquals(Math.max(size - i, 0), counter.get());
                }
            }
        }
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
