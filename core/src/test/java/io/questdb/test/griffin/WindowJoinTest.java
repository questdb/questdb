/*******************************************************************************
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

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.join.AsyncWindowJoinAtom;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class WindowJoinTest extends AbstractCairoTest {
    private final TestTimestampType leftTableTimestampType;
    private final TestTimestampType rightTableTimestampType;
    private final StringSink sink = new StringSink();
    private boolean includePrevailing;
    private boolean leftConvertParquet;
    private boolean rightConvertParquet;

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
        final Rnd rnd = TestUtils.generateRandom(LOG);
        AsyncWindowJoinAtom.GROUP_BY_VALUE_USE_COMPACT_DIRECT_MAP = rnd.nextBoolean();
        sink.clear();
        includePrevailing = rnd.nextBoolean();
        leftConvertParquet = rnd.nextBoolean();
        rightConvertParquet = rnd.nextBoolean();
    }

    @Test
    public void testAggregateNotTrivialColumn() throws Exception {
        assertMemoryLeak(() -> {
            prepareTable();
            if (!includePrevailing) {
                printSql("select t.sym, t.price, t.ts, sum(p.price + 1) window_price " +
                        "from trades t " +
                        "left join prices p " +
                        "on (t.sym = p.sym) " +
                        " and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) " +
                        "order by t.ts, t.sym;", sink);
            } else {
                printSql("""
                                select sym,price,ts, sum(price1 + 1) window_price from
                                (
                                        select * from (
                                            select t.sym, t.price, t.ts, p.price
                                            from trades t
                                            left join prices p
                                            on (t.sym = p.sym)
                                            and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts)
                                        union
                                            select sym,price,ts,price1  from (select t.sym, t.price, t.ts, p.price price1, p.ts as ts1
                                            from trades t
                                            join prices p
                                            on (t.sym = p.sym) and p.ts <= dateadd('m', -1, t.ts)) LATEST ON ts1 PARTITION BY ts, sym
                                        ) order by ts
                                )
                                order by ts, sym;
                                """,
                        sink);
            }

            assertQueryAndPlan(
                    sink,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Async Window Fast Join workers: 1\n" +
                            "      vectorized: true\n" +
                            "      symbol: sym=sym\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding " + (includePrevailing ? "(include prevailing)\n" : "(exclude prevailing)\n") +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " following\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: trades\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: prices\n",
                    "select t.sym, t.price, t.ts, sum(p.price + 1) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on (t.sym = p.sym) " +
                            " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );

            if (!includePrevailing) {
                printSql("select t.sym, t.price, t.ts, sum(p.price + 1) window_price " +
                        "from trades t " +
                        "left join prices p " +
                        "on p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) " +
                        "order by t.ts, t.sym;", sink);
            } else {
                printSql("""
                                select sym,price,ts, sum(price1 + 1) window_price from
                                (
                                        select * from (
                                            select t.sym, t.price, t.ts, p.price
                                            from trades t
                                            left join prices p
                                            on p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts)
                                        union
                                            select sym,price,ts,price1  from (select t.sym, t.price, t.ts, p.price price1, p.ts as ts1
                                            from trades t
                                            join prices p
                                            on p.ts <= dateadd('m', -1, t.ts)) LATEST ON ts1 PARTITION BY ts
                                        ) order by ts
                                )
                                order by ts, sym;
                                """,
                        sink);
            }
            assertQueryAndPlan(
                    sink,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Async Window Join workers: 1\n" +
                            "      vectorized: true\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding " + (includePrevailing ? "(include prevailing)\n" : "(exclude prevailing)\n") +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " following\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: trades\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: prices\n",
                    "select t.sym, t.price, t.ts, sum(p.price + 1) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );

            if (!includePrevailing) {
                printSql("select t.sym, t.price, t.ts, count(p.ts)  " +
                        "from trades t " +
                        "left join prices p " +
                        "on p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) " +
                        "order by t.ts, t.sym;", sink);
            } else {
                printSql("""
                                select sym,price,ts, count(pts) from
                                (
                                        select * from (
                                            select t.sym, t.price, t.ts, p.price, p.ts pts
                                            from trades t
                                            left join prices p
                                            on p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts)
                                        union
                                            select sym,price,ts,price1, ts1  from (select t.sym, t.price, t.ts, p.price price1, p.ts as ts1
                                            from trades t
                                            join prices p
                                            on p.ts <= dateadd('m', -1, t.ts)) LATEST ON ts1 PARTITION BY ts
                                        ) order by ts
                                )
                                order by ts, sym;
                                """,
                        sink);
            }

            assertQueryAndPlan(
                    sink,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Async Window Join workers: 1\n" +
                            "      vectorized: true\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding " + (includePrevailing ? "(include prevailing)\n" : "(exclude prevailing)\n") +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " following\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: trades\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: prices\n",
                    "select t.sym, t.price, t.ts, count() " +
                            "from trades t " +
                            "window join prices p " +
                            " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
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
                            sym0\t2023-01-01T09:00:00.000000Z\t1.5
                            """,
                    """
                            Sort
                              keys: [ts, s]
                                Async Window Fast Join workers: 1
                                  vectorized: true
                                  symbol: s=s
                            """ +
                            "      window lo: 1000000 preceding" + (includePrevailing ? " (include prevailing)\n" : " (exclude prevailing)\n")
                            +
                            """
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
                            " range between 1 second preceding and 1 second following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
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
            // window around current row
            if (!includePrevailing) {
                printSql("select t.sym, t.price, t.ts, sum(p.price) window_price " +
                        "from trades t " +
                        "left join prices p " +
                        "on (t.sym = p.sym) " +
                        " and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) " +
                        "order by t.ts, t.sym;", sink);
            } else {
                printSql("""
                                select sym,price,ts, sum(price1) window_price from
                                (
                                        select * from (
                                            select t.sym, t.price, t.ts, p.price
                                            from trades t
                                            left join prices p
                                            on (t.sym = p.sym)
                                            and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts)
                                        union
                                            select sym,price,ts,price1  from (select t.sym, t.price, t.ts, p.price price1, p.ts as ts1
                                            from trades t
                                            join prices p
                                            on (t.sym = p.sym) and p.ts <= dateadd('m', -1, t.ts)) LATEST ON ts1 PARTITION BY ts, sym
                                        ) order by ts
                                )
                                order by ts, sym;
                                """,
                        sink);
            }
            assertQueryAndPlan(
                    sink,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Async Window Fast Join workers: 1\n" +
                            "      vectorized: true\n" +
                            "      symbol: sym=sym\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding " + (includePrevailing ? "(include prevailing)\n" : "(exclude prevailing)\n") +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " following\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: trades\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: prices\n",
                    "select t.sym, t.price, t.ts, sum(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on (t.sym = p.sym) " +
                            " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );

            // window in the past
            if (!includePrevailing) {
                printSql("select t.sym, t.price, t.ts, sum(p.price) as window_price " +
                        "from trades t " +
                        "window join prices p " +
                        "on (t.sym = p.sym) " +
                        " range between 2 minute preceding and 1 minute preceding " +
                        "order by t.ts, t.sym;", sink);
            } else {
                printSql("""
                                select sym,price,ts, sum(price1) window_price from
                                (
                                        select * from (
                                            select t.sym, t.price, t.ts, p.price
                                            from trades t
                                            left join prices p
                                            on (t.sym = p.sym)
                                            and p.ts >= dateadd('m', -2, t.ts) AND p.ts <= dateadd('m', -1, t.ts)
                                        union
                                            select sym,price,ts,price1  from (select t.sym, t.price, t.ts, p.price price1, p.ts as ts1
                                            from trades t
                                            join prices p
                                            on (t.sym = p.sym) and p.ts <= dateadd('m', -2, t.ts)) LATEST ON ts1 PARTITION BY ts, sym
                                        ) order by ts
                                )
                                order by ts, sym;
                                """,
                        sink);
            }
            assertQuery(
                    sink.toString(),
                    "select t.sym, t.price, t.ts, sum(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on (t.sym = p.sym) " +
                            " range between 2 minute preceding and 1 minute preceding " +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );

            // window in the future
            if (!includePrevailing) {
                printSql("select t.sym, t.price, t.ts, sum(p.price) window_price " +
                        "from trades t " +
                        "left join prices p " +
                        "on (t.sym = p.sym) " +
                        " and p.ts >= dateadd('m', 1, t.ts) AND p.ts <= dateadd('m', 2, t.ts) " +
                        "order by t.ts, t.sym;", sink);
            } else {
                printSql("""
                                select sym,price,ts, sum(price1) window_price from
                                (
                                        select * from (
                                            select t.sym, t.price, t.ts, p.price
                                            from trades t
                                            left join prices p
                                            on (t.sym = p.sym)
                                            and p.ts >= dateadd('m', 1, t.ts) AND p.ts <= dateadd('m', 2, t.ts)
                                        union
                                            select sym,price,ts,price1  from (select t.sym, t.price, t.ts, p.price price1, p.ts as ts1
                                            from trades t
                                            join prices p
                                            on (t.sym = p.sym) and p.ts <= dateadd('m', 1, t.ts)) LATEST ON ts1 PARTITION BY ts, sym
                                        ) order by ts
                                )
                                order by ts, sym;
                                """,
                        sink);
            }

            assertQuery(
                    sink,
                    "select t.sym, t.price, t.ts, sum(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on (t.sym = p.sym) " +
                            " range between 1 minute following and 2 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testCalcSize() throws Exception {
        assertMemoryLeak(() -> {
            prepareTable();
            assertSkipToAndCalculateSize(
                    "select t.sym, t.price, t.ts, sum(t.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on (t.sym = p.sym) " +
                            " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by t.ts, t.sym",
                    20
            );
            assertSkipToAndCalculateSize(
                    "select t.sym, t.price, t.ts, sum(t.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by t.ts, t.sym",
                    20
            );
            assertSkipToAndCalculateSize(
                    "select t.sym, t.price, t.ts, sum(t.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on t.sym = p.sym " +
                            " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            " where t.price < 450 " +
                            "order by t.ts, t.sym",
                    3
            );
            assertSkipToAndCalculateSize(
                    "select t.sym, t.price, t.ts, sum(t.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            " where t.price < 450 " +
                            "order by t.ts, t.sym",
                    3
            );
        });
    }

    @Test
    public void testCountOnlyWindowJoin() throws Exception {
        assertMemoryLeak(() -> {
            prepareTable();
            assertQueryAndPlan(
                    """
                            count
                            3
                            3
                            3
                            3
                            3
                            3
                            3
                            3
                            2
                            1
                            2
                            1
                            2
                            1
                            2
                            1
                            1
                            1
                            1
                            1
                            """,
                    "Async Window Fast Join workers: 1\n" +
                            "  vectorized: true\n" +
                            "  symbol: sym=sym\n" +
                            "  window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding " + (includePrevailing ? "(include prevailing)\n" : "(exclude prevailing)\n") +
                            "  window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " following\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: trades\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: prices\n",
                    "select count() " +
                            "from trades t " +
                            "window join prices p " +
                            "on (t.sym = p.sym) " +
                            " range between 1 minute preceding and 1 minute following" + (includePrevailing ? " include prevailing" : " exclude prevailing"),
                    null,
                    false,
                    false
            );
        });
    }

    @Test
    public void testFastJoinWithJoinFilter() throws Exception {
        assertMemoryLeak(() -> {
            prepareTable();
            if (!includePrevailing) {
                printSql("select t.sym, t.price, t.ts, avg(p.price) window_price " +
                        "from trades t " +
                        "left join prices p " +
                        "on (t.sym = p.sym)" +
                        " and p.ts >= dateadd('m', -2, t.ts) AND p.ts <= dateadd('m', 2, t.ts) and p.price < 300 " +
                        "order by t.ts, t.sym;", sink);
            } else {
                printSql("""
                                select sym,price,ts, avg(price1) window_price from
                                (
                                        select * from (
                                            select t.sym, t.price, t.ts, p.price
                                            from trades t
                                            left join prices p
                                            on (t.sym = p.sym)
                                            and p.ts >= dateadd('m', -2, t.ts) AND p.ts <= dateadd('m', 2, t.ts) AND p.price < 300
                                        union
                                            select sym,price,ts,price1  from (select t.sym, t.price, t.ts, p.price price1, p.ts as ts1
                                            from trades t
                                            join prices p
                                            on (t.sym = p.sym) and p.ts <= dateadd('m', -2, t.ts) and p.price < 300) LATEST ON ts1 PARTITION BY ts, sym
                                        ) order by ts
                                )
                                order by ts, sym;
                                """,
                        sink);
            }
            assertQueryAndPlan(
                    sink,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Async Window Fast Join workers: 1\n" +
                            "      vectorized: false\n" +
                            "      symbol: sym=sym\n" +
                            "      join filter: p.price<300\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " preceding " + (includePrevailing ? "(include prevailing)\n" : "(exclude prevailing)\n") +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " following\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: trades\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: prices\n",
                    "select t.sym, t.price, t.ts, avg(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on (t.sym = p.sym) and p.price < 300 " +
                            " range between 2 minute preceding and 2 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );

            if (!includePrevailing) {
                printSql("select t.sym, t.price, t.ts, avg(p.price) window_price " +
                        "from trades t " +
                        "left join prices p " +
                        "on (t.sym = p.sym)" +
                        " and p.ts >= dateadd('m', -2, t.ts) AND p.ts <= dateadd('m', 2, t.ts)  " +
                        "order by t.ts, t.sym;", sink);
            } else {
                printSql("""
                                select sym,price,ts, avg(price1) window_price from
                                (
                                        select * from (
                                            select t.sym, t.price, t.ts, p.price
                                            from trades t
                                            left join prices p
                                            on (t.sym = p.sym)
                                            and p.ts >= dateadd('m', -2, t.ts) AND p.ts <= dateadd('m', 2, t.ts)
                                        union
                                            select sym,price,ts,price1  from (select t.sym, t.price, t.ts, p.price price1, p.ts as ts1
                                            from trades t
                                            join prices p
                                            on (t.sym = p.sym) and p.ts <= dateadd('m', -2, t.ts)) LATEST ON ts1 PARTITION BY ts, sym
                                        ) order by ts
                                )
                                order by ts, sym;
                                """,
                        sink);
            }
            assertQueryAndPlan(
                    sink,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Async Window Fast Join workers: 1\n" +
                            "      vectorized: true\n" +
                            "      symbol: sym=sym\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " preceding " + (includePrevailing ? "(include prevailing)\n" : "(exclude prevailing)\n") +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " following\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: trades\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: prices\n",
                    "select t.sym, t.price, t.ts, avg(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on (t.sym = p.sym) " +
                            " range between 2 minute preceding and 2 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );

            if (!includePrevailing) {
                printSql("select t.sym, t.price, t.ts, avg(p.price) window_price " +
                        "from trades t " +
                        "left join prices p " +
                        "on (t.sym = p.sym)" +
                        " and p.ts >= dateadd('m', -2, t.ts) AND p.ts <= dateadd('m', 2, t.ts) and (p.price < 200 or p.price > 300 ) " +
                        "order by t.ts, t.sym;", sink);
            } else {
                printSql("""
                                select sym,price,ts, avg(price1) window_price from
                                (
                                        select * from (
                                            select t.sym, t.price, t.ts, p.price
                                            from trades t
                                            left join prices p
                                            on (t.sym = p.sym)
                                            and p.ts >= dateadd('m', -2, t.ts) AND p.ts <= dateadd('m', 2, t.ts) and (p.price < 200 or p.price > 300 )
                                        union
                                            select sym,price,ts,price1  from (select t.sym, t.price, t.ts, p.price price1, p.ts as ts1
                                            from trades t
                                            join prices p
                                            on (t.sym = p.sym) and p.ts <= dateadd('m', -2, t.ts) and (p.price < 200 or p.price > 300)) LATEST ON ts1 PARTITION BY ts, sym
                                        ) order by ts
                                )
                                order by ts, sym;
                                """,
                        sink);
            }
            assertQueryAndPlan(
                    sink,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Async Window Fast Join workers: 1\n" +
                            "      vectorized: false\n" +
                            "      symbol: sym=sym\n" +
                            "      join filter: (p.price<200 or 300<p.price)\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " preceding " + (includePrevailing ? "(include prevailing)\n" : "(exclude prevailing)\n") +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " following\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: trades\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: prices\n",
                    "select t.sym, t.price, t.ts, avg(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on (p.price < 200 or p.price > 300 ) AND (t.sym = p.sym) " +
                            " range between 2 minute preceding and 2 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );

            if (!includePrevailing) {
                printSql("select t.sym, t.price, t.ts, avg(p.price) window_price " +
                        "from trades t " +
                        "left join prices p " +
                        "on (t.sym = p.sym)" +
                        " and p.ts >= dateadd('m', -2, t.ts) AND p.ts <= dateadd('m', 2, t.ts) and (p.price < 200 and p.price > 300 ) " +
                        "order by t.ts, t.sym;", sink);
            } else {
                printSql("""
                                select sym,price,ts, avg(price1) window_price from
                                (
                                        select * from (
                                            select t.sym, t.price, t.ts, p.price
                                            from trades t
                                            left join prices p
                                            on (t.sym = p.sym)
                                            and p.ts >= dateadd('m', -2, t.ts) AND p.ts <= dateadd('m', 2, t.ts) and (p.price < 200 and p.price > 300 )
                                        union
                                            select sym,price,ts,price1  from (select t.sym, t.price, t.ts, p.price price1, p.ts as ts1
                                            from trades t
                                            join prices p
                                            on (t.sym = p.sym) and p.ts <= dateadd('m', -2, t.ts) and (p.price < 200 and p.price > 300)) LATEST ON ts1 PARTITION BY ts, sym
                                        ) order by ts
                                )
                                order by ts, sym;
                                """,
                        sink);
            }
            assertQueryAndPlan(
                    sink,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Async Window Fast Join workers: 1\n" +
                            "      vectorized: false\n" +
                            "      symbol: sym=sym\n" +
                            "      join filter: (p.price<200 and 300<p.price)\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " preceding " + (includePrevailing ? "(include prevailing)\n" : "(exclude prevailing)\n") +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " following\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: trades\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: prices\n",
                    "select t.sym, t.price, t.ts, avg(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on p.price < 200 and p.price > 300 AND (t.sym = p.sym) " +
                            " range between 2 minute preceding and 2 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );

            // with join filter and master filter
            if (!includePrevailing) {
                printSql("select t.sym, t.price, t.ts, avg(p.price) window_price " +
                        "from trades t " +
                        "left join prices p " +
                        "on t.ts > 1000 and p.ts > 1000 AND (t.sym = p.sym) and p.price < 300 and p.sym != 'AAAAAA'" +
                        " and p.ts >= dateadd('m', -2, t.ts) AND p.ts <= dateadd('m', 2, t.ts) " +
                        "order by t.ts, t.sym;", sink);
            } else {
                printSql("""
                                select sym,price,ts, avg(price1) window_price from
                                (
                                        select * from (
                                            select t.sym, t.price, t.ts, p.price
                                            from trades t
                                            left join prices p
                                            on (t.sym = p.sym)
                                            and p.ts >= dateadd('m', -2, t.ts) AND p.ts <= dateadd('m', 2, t.ts) and (t.ts > 1000 and p.ts > 1000 AND (t.sym = p.sym) and p.price < 300 and p.sym != 'AAAAAA' )
                                        union
                                            select sym,price,ts,price1  from (select t.sym, t.price, t.ts, p.price price1, p.ts as ts1
                                            from trades t
                                            join prices p
                                            on (t.sym = p.sym) and p.ts <= dateadd('m', -2, t.ts) and (t.ts > 1000 and p.ts > 1000 AND (t.sym = p.sym) and p.price < 300 and p.sym != 'AAAAAA')) LATEST ON ts1 PARTITION BY ts, sym
                                        ) order by ts
                                )
                                order by ts, sym;
                                """,
                        sink);
            }
            assertQueryAndPlan(
                    sink,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Async Window Fast Join workers: 1\n" +
                            "      vectorized: false\n" +
                            "      symbol: sym=sym\n" +
                            "      join filter: (1000<p.ts and p.price<300 and p.sym!='AAAAAA')\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " preceding " + (includePrevailing ? "(include prevailing)\n" : "(exclude prevailing)\n") +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " following\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Interval forward scan on: trades\n" +
                            "              intervals: [(\"" + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "1970-01-01T00:00:00.001001Z" : "1970-01-01T00:00:00.000001001Z") + "\",\"MAX\")]\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            (includePrevailing ? "            Frame forward scan on: prices\n" :
                                    "            Interval forward scan on: prices\n" +
                                            "              intervals: [(\"" + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? (ColumnType.isTimestampMicro(rightTableTimestampType.getTimestampType()) ? "1969-12-31T23:58:00.001001Z" : "1969-12-31T23:58:00.001001000Z")
                                            : (ColumnType.isTimestampMicro(rightTableTimestampType.getTimestampType()) ? "1969-12-31T23:58:00.000001Z" : "1969-12-31T23:58:00.000001001Z")) + "\",\"MAX\")]\n"),
                    "select t.sym, t.price, t.ts, avg(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on p.ts > 1000 AND (t.sym = p.sym) and p.price < 300 and p.sym != 'AAAAAA'" +
                            " range between 2 minute preceding and 2 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            " where t.ts > 1000 " +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testFastJoinWithMasterFilter() throws Exception {
        Assume.assumeTrue(leftTableTimestampType.getTimestampType() == rightTableTimestampType.getTimestampType());
        assertMemoryLeak(() -> {
            prepareTable();
            if (!includePrevailing) {
                printSql("select t.sym, t.price, t.ts, avg(p.price) window_price " +
                        "from (select * from trades where price < 300) t " +
                        "left join prices p " +
                        "on (t.sym = p.sym) " +
                        " and p.ts >= dateadd('m', -2, t.ts) AND p.ts <= dateadd('m', 2, t.ts) " +
                        "order by t.ts, t.sym;", sink);
            } else {
                printSql("""
                                select sym,price,ts, avg(price1) window_price from
                                (
                                        select * from (
                                            select t.sym, t.price, t.ts, p.price
                                            from (select * from trades where price < 300) t
                                            left join prices p
                                            on (t.sym = p.sym)
                                            and p.ts >= dateadd('m', -2, t.ts) AND p.ts <= dateadd('m', 2, t.ts) AND p.price < 300
                                        union
                                            select sym,price,ts,price1  from (select t.sym, t.price, t.ts, p.price price1, p.ts as ts1
                                            from (select * from trades where price < 300) t
                                            join prices p
                                            on (t.sym = p.sym) and p.ts <= dateadd('m', -2, t.ts) and p.price < 300) LATEST ON ts1 PARTITION BY ts, sym
                                        ) order by ts
                                )
                                order by ts, sym;
                                """,
                        sink);
            }
            assertQueryAndPlan(
                    sink,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Async Window Fast Join workers: 1\n" +
                            "      vectorized: true\n" +
                            "      symbol: sym=sym\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " preceding " + (includePrevailing ? "(include prevailing)\n" : "(exclude prevailing)\n") +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " following\n" +
                            "      master filter: price<300\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: trades\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: prices\n",
                    "select t.sym, t.price, t.ts, avg(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on (t.sym = p.sym) " +
                            " range between 2 minute preceding and 2 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            " where t.price < 300 " +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );

            if (!includePrevailing) {
                printSql("select t.sym, t.ts, count(), max(p.ts) " +
                        "from (select * from trades where price < 300) t " +
                        "left join prices p " +
                        "on (t.sym = p.sym) " +
                        " and p.ts >= dateadd('m', -2, t.ts) AND p.ts <= dateadd('m', 2, t.ts) " +
                        "order by t.ts;", sink);
            } else {
                printSql("""
                                select sym,ts, count(), max(pts) from
                                (
                                        select * from (
                                            select t.sym, t.price, t.ts, p.price, p.ts pts
                                            from (select * from trades where price < 300) t
                                            left join prices p
                                            on (t.sym = p.sym)
                                            and p.ts >= dateadd('m', -2, t.ts) AND p.ts <= dateadd('m', 2, t.ts) AND p.price < 300
                                        union
                                            select sym,price,ts,price1, ts1 pts   from (select t.sym, t.price, t.ts, p.price price1, p.ts as ts1
                                            from (select * from trades where price < 300) t
                                            join prices p
                                            on (t.sym = p.sym) and p.ts <= dateadd('m', -2, t.ts) and p.price < 300) LATEST ON ts1 PARTITION BY ts, sym
                                        ) order by ts
                                )
                                order by ts, sym;
                                """,
                        sink);
            }
            assertQueryAndPlan(
                    sink,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Async Window Fast Join workers: 1\n" +
                            "      vectorized: true\n" +
                            "      symbol: sym=sym\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " preceding " + (includePrevailing ? "(include prevailing)\n" : "(exclude prevailing)\n") +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " following\n" +
                            "      master filter: price<300\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: trades\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: prices\n",
                    "select t.sym, t.ts, count(), max(p.ts) " +
                            "from trades t " +
                            "window join prices p " +
                            "on (t.sym = p.sym) " +
                            " range between 2 minute preceding and 2 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            " where t.price < 300 " +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testFrameNoIntersection() throws Exception {
        Assume.assumeTrue(leftTableTimestampType == TestTimestampType.MICRO);
        Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);
        assertMemoryLeak(() -> {
            execute(
                    "create table trades (" +
                            "  sym symbol," +
                            "  price double," +
                            "  ts timestamp" +
                            ") timestamp(ts) partition by day;"
            );
            execute(
                    "create table prices (" +
                            "  sym symbol," +
                            "  bid double," +
                            "  ts timestamp" +
                            ") timestamp(ts) partition by day;"
            );

            execute(
                    "insert into trades values " +
                            "('sym0', 1.0, '2023-01-01T08:59:56.000000Z')," +
                            "('sym0', 2.0, '2023-01-01T08:59:57.000000Z')," +
                            "('sym0', 3.0, '2023-01-03T08:59:58.000000Z')," +
                            "('sym0', 4.0, '2023-01-03T08:59:59.000000Z')," +
                            "('sym0', 5.0, '2023-01-05T09:00:00.000000Z')," +
                            "('sym0', 6.0, '2023-01-05T09:00:01.000000Z')," +
                            "('sym0', 7.0, '2023-01-07T09:00:02.000000Z')," +
                            "('sym0', 8.0, '2023-01-07T09:00:03.000000Z')," +
                            "('sym0', 9.0, '2023-01-09T09:00:04.000000Z');"
            );

            execute(
                    "insert into prices values " +
                            "('sym0', 1.0, '2023-01-02T08:59:56.000000Z')," +
                            "('sym0', 2.0, '2023-01-02T08:59:57.000000Z')," +
                            "('sym0', 3.0, '2023-01-02T08:59:58.000000Z')," +
                            "('sym0', 4.0, '2023-01-04T08:59:59.000000Z')," +
                            "('sym0', 5.0, '2023-01-04T09:00:00.000000Z')," +
                            "('sym0', 6.0, '2023-01-04T09:00:01.000000Z')," +
                            "('sym0', 7.0, '2023-01-06T09:00:02.000000Z')," +
                            "('sym0', 8.0, '2023-01-06T09:00:03.000000Z')," +
                            "('sym0', 9.0, '2023-01-08T09:00:04.000000Z');"
            );

            assertQueryAndPlan(
                    includePrevailing ? """
                            sym	price	ts	first	avg
                            sym0	1.0	2023-01-01T08:59:56.000000Z	null	null
                            sym0	2.0	2023-01-01T08:59:57.000000Z	null	null
                            sym0	3.0	2023-01-03T08:59:58.000000Z	3.0	3.0
                            sym0	4.0	2023-01-03T08:59:59.000000Z	3.0	3.0
                            sym0	5.0	2023-01-05T09:00:00.000000Z	6.0	6.0
                            sym0	6.0	2023-01-05T09:00:01.000000Z	6.0	6.0
                            sym0	7.0	2023-01-07T09:00:02.000000Z	8.0	8.0
                            sym0	8.0	2023-01-07T09:00:03.000000Z	8.0	8.0
                            sym0	9.0	2023-01-09T09:00:04.000000Z	9.0	9.0
                            """
                            : """
                            sym	price	ts	first	avg
                            sym0	1.0	2023-01-01T08:59:56.000000Z	null	null
                            sym0	2.0	2023-01-01T08:59:57.000000Z	null	null
                            sym0	3.0	2023-01-03T08:59:58.000000Z	null	null
                            sym0	4.0	2023-01-03T08:59:59.000000Z	null	null
                            sym0	5.0	2023-01-05T09:00:00.000000Z	null	null
                            sym0	6.0	2023-01-05T09:00:01.000000Z	null	null
                            sym0	7.0	2023-01-07T09:00:02.000000Z	null	null
                            sym0	8.0	2023-01-07T09:00:03.000000Z	null	null
                            sym0	9.0	2023-01-09T09:00:04.000000Z	null	null
                            """,
                    """
                            Sort
                              keys: [ts, sym]
                                Async Window Fast Join workers: 1
                                  vectorized: true
                                  symbol: sym=sym
                            """ +
                            "      window lo: 60000000 preceding" + (includePrevailing ? " (include prevailing)\n" : " (exclude prevailing)\n") +
                            """
                                          window hi: 60000000 following
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: trades
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: prices
                                    """,
                    "select t.sym, t.price, t.ts, first(p.bid) as first, avg(p.bid) as avg " +
                            "from trades t " +
                            "window join prices p " +
                            "on (t.sym = p.sym) " +
                            " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testHasJoinFilterWithDuplicatedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            Assume.assumeTrue(leftTableTimestampType == TestTimestampType.MICRO);
            Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);
            execute(
                    "create table trades (" +
                            "  ts timestamp, " +
                            "  sym symbol, " +
                            "  price double " +
                            ") timestamp(ts) partition by day;"
            );
            execute(
                    "create table prices (" +
                            "  ts timestamp, " +
                            "  sym symbol, " +
                            "  price double" +
                            ") timestamp(ts) partition by day;"
            );

            execute(
                    "insert into trades(sym, price, ts) values " +
                            "('TSLA', 400.0, cast('2023-01-01T09:10:00.000000Z' as TIMESTAMP))," +
                            "('TSLA', 401.0, cast('2023-01-01T09:11:00.000000Z' as TIMESTAMP))," +
                            "('AMZN', 500.0, cast('2023-01-01T09:12:00.000000Z' as TIMESTAMP))," +
                            "('AMZN', 501.0, cast('2023-01-01T09:12:00.000000Z' as TIMESTAMP))," +
                            "('META', 600.0, cast('2023-01-01T09:15:00.000000Z' as TIMESTAMP))," +
                            "('META', 601.0, cast('2023-01-01T09:15:00.000000Z' as TIMESTAMP))," +
                            "('TSLA', 402.0, cast('2023-01-01T09:15:00.000000Z' as TIMESTAMP))," +
                            "('AMZN', 502.0, cast('2023-01-01T09:16:00.000000Z' as TIMESTAMP))," +
                            "('META', 602.0, cast('2023-01-01T09:17:00.000000Z' as TIMESTAMP));"
            );
            execute(
                    "insert into prices(sym, price, ts) values " +
                            "('TSLA', 399.5, cast('2023-01-01T09:09:00.000000Z' as TIMESTAMP))," +
                            "('TSLA', 400.5, cast('2023-01-01T09:10:00.000000Z' as TIMESTAMP))," +
                            "('AMZN', 499.5, cast('2023-01-01T09:11:00.000000Z' as TIMESTAMP))," +
                            "('AMZN', 500.5, cast('2023-01-01T09:11:00.000000Z' as TIMESTAMP))," +
                            "('META', 599.5, cast('2023-01-01T09:14:00.000000Z' as TIMESTAMP))," +
                            "('META', 600.5, cast('2023-01-01T09:14:00.000000Z' as TIMESTAMP))," +
                            "('TSLA', 401.5, cast('2023-01-01T09:14:00.000000Z' as TIMESTAMP))," +
                            "('AMZN', 501.5, cast('2023-01-01T09:15:00.000000Z' as TIMESTAMP))," +
                            "('META', 601.5, cast('2023-01-01T09:16:00.000000Z' as TIMESTAMP));"
            );

            if (!includePrevailing) {
                printSql("select t.sym, t.price, t.ts, count(p.price) window_price " +
                        "from trades t " +
                        "left join prices p " +
                        " on p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) AND p.price > 0 " +
                        "order by t.ts, t.sym;", sink);
            } else {
                printSql("""
                                select sym,price,ts, count(price1) window_price from
                                (
                                        select * from (
                                            select t.sym, t.price, t.ts, p.price, p.ts pts
                                            from trades t
                                            left join prices p
                                            on p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) and p.price > 0
                                        union
                                            select sym,price,ts,price1,ts1  from (select t.sym, t.price, t.ts, p.price price1, p.ts as ts1
                                            from trades t
                                            join prices p
                                            on p.ts <= dateadd('m', 1, t.ts) and p.price > 0) LATEST ON ts1 PARTITION BY ts, sym
                                        ) order by ts
                                )
                                order by ts, sym;
                                """,
                        sink);
            }

            assertQuery(
                    sink,
                    String.format("""
                                    SELECT t.sym, t.price, t.ts, count(p.price) AS window_price
                                    FROM trades t
                                    WINDOW JOIN prices p
                                    ON p.price > 0
                                       RANGE BETWEEN 1 MINUTE PRECEDING AND 1 MINUTE FOLLOWING %s PREVAILING
                                    ORDER BY t.ts, t.sym
                                    """,
                            includePrevailing ? "include" : "exclude"),
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testHasMaterAndJoinFilterWithDuplicatedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            Assume.assumeTrue(leftTableTimestampType == TestTimestampType.MICRO);
            Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);
            execute(
                    "create table trades (" +
                            "  ts timestamp, " +
                            "  sym symbol, " +
                            "  price double " +
                            ") timestamp(ts) partition by day;"
            );
            execute(
                    "create table prices (" +
                            "  ts timestamp, " +
                            "  sym symbol, " +
                            "  price double" +
                            ") timestamp(ts) partition by day;"
            );

            execute(
                    "insert into trades(sym, price, ts) values " +
                            "('TSLA', 400.0, cast('2023-01-01T09:10:00.000000Z' as TIMESTAMP))," +
                            "('TSLA', 401.0, cast('2023-01-01T09:11:00.000000Z' as TIMESTAMP))," +
                            "('AMZN', 500.0, cast('2023-01-01T09:12:00.000000Z' as TIMESTAMP))," +
                            "('AMZN', 501.0, cast('2023-01-01T09:12:00.000000Z' as TIMESTAMP))," +
                            "('META', 600.0, cast('2023-01-01T09:15:00.000000Z' as TIMESTAMP))," +
                            "('META', 601.0, cast('2023-01-01T09:15:00.000000Z' as TIMESTAMP))," +
                            "('TSLA', 402.0, cast('2023-01-01T09:15:00.000000Z' as TIMESTAMP))," +
                            "('AMZN', 502.0, cast('2023-01-01T09:16:00.000000Z' as TIMESTAMP))," +
                            "('META', 602.0, cast('2023-01-01T09:17:00.000000Z' as TIMESTAMP));"
            );
            execute(
                    "insert into prices(sym, price, ts) values " +
                            "('TSLA', 399.5, cast('2023-01-01T09:09:00.000000Z' as TIMESTAMP))," +
                            "('TSLA', 400.5, cast('2023-01-01T09:10:00.000000Z' as TIMESTAMP))," +
                            "('AMZN', 499.5, cast('2023-01-01T09:11:00.000000Z' as TIMESTAMP))," +
                            "('AMZN', 500.5, cast('2023-01-01T09:11:00.000000Z' as TIMESTAMP))," +
                            "('META', 599.5, cast('2023-01-01T09:14:00.000000Z' as TIMESTAMP))," +
                            "('META', 600.5, cast('2023-01-01T09:14:00.000000Z' as TIMESTAMP))," +
                            "('TSLA', 401.5, cast('2023-01-01T09:14:00.000000Z' as TIMESTAMP))," +
                            "('AMZN', 501.5, cast('2023-01-01T09:15:00.000000Z' as TIMESTAMP))," +
                            "('META', 601.5, cast('2023-01-01T09:16:00.000000Z' as TIMESTAMP));"
            );

            if (!includePrevailing) {
                printSql("select t.sym, t.price, t.ts, count(p.price) window_price " +
                        "from (select * from trades where price > 0) t " +
                        "left join prices p " +
                        " on p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) AND p.price > 0 " +
                        "order by t.ts, t.sym;", sink);
            } else {
                printSql("""
                                select sym,price,ts, count(price1) window_price from
                                (
                                        select * from (
                                            select t.sym, t.price, t.ts, p.price, p.ts pts
                                            from (select * from trades where price > 0) t
                                            left join prices p
                                            on p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) and p.price > 0
                                        union
                                            select sym,price,ts,price1,ts1  from (select t.sym, t.price, t.ts, p.price price1, p.ts as ts1
                                            from (select * from trades where price > 0) t
                                            join prices p
                                            on p.ts <= dateadd('m', 1, t.ts) and p.price > 0) LATEST ON ts1 PARTITION BY ts, sym
                                        ) order by ts
                                )
                                order by ts, sym;
                                """,
                        sink);
            }

            assertQuery(
                    sink,
                    String.format("""
                                    SELECT t.sym, t.price, t.ts, count(p.price) AS window_price
                                    FROM (select * from trades where price > 0) t
                                    WINDOW JOIN prices p
                                    ON p.price > 0
                                       RANGE BETWEEN 1 MINUTE PRECEDING AND 1 MINUTE FOLLOWING %s PREVAILING
                                    ORDER BY t.ts, t.sym
                                    """,
                            includePrevailing ? "include" : "exclude"),
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testJoinFilterMiddleRowMatchesAtBoundary() throws Exception {
        // Tests join filter with multiple rows where only a middle row matches.

        // timestamp types don't matter for this test
        Assume.assumeTrue(leftTableTimestampType == TestTimestampType.MICRO);
        Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);

        assertMemoryLeak(() -> {
            execute(
                    "create table x (" +
                            "  ts timestamp," +
                            "  sym symbol" +
                            ") timestamp(ts) partition by day;"
            );
            execute(
                    "create table y (" +
                            "  ts timestamp," +
                            "  sym symbol," +
                            "  val int" +
                            ") timestamp(ts) partition by day;"
            );

            execute("insert into x values ('2023-01-01T09:01:00.000000Z', 'A');");

            // Three rows at the boundary timestamp: only the middle one (val=5) matches the filter
            execute(
                    "insert into y values " +
                            "('2023-01-01T09:00:00.000000Z', 'A', 1)," +
                            "('2023-01-01T09:00:00.000000Z', 'A', 5)," +
                            "('2023-01-01T09:00:00.000000Z', 'A', 2)," +
                            "('2023-01-01T09:01:00.000000Z', 'A', 5);"
            );

            assertQueryNoLeakCheck(
                    """
                            ts\tsym\tsum
                            2023-01-01T09:01:00.000000Z\tA\t10
                            """,
                    "select x.ts, x.sym, sum(y.val) " +
                            "from x " +
                            "window join y on (x.sym = y.sym and y.val = 5) " +
                            "range between 1 minute preceding and 1 minute following " +
                            (includePrevailing ? "include prevailing" : "exclude prevailing"),
                    "ts",
                    false,
                    false
            );
        });
    }

    @Test
    public void testJoinFilterWithMultipleRowsAtSameTimestamp() throws Exception {
        // Tests join filter with multiple rows at the same timestamp boundary.
        // The fix ensures all rows at slaveTimestampLo are checked against the join filter,
        // not just the first one.

        // timestamp types don't matter for this test
        Assume.assumeTrue(leftTableTimestampType == TestTimestampType.MICRO);
        Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);

        assertMemoryLeak(() -> {
            execute(
                    "create table x (" +
                            "  ts timestamp," +
                            "  sym symbol" +
                            ") timestamp(ts) partition by day;"
            );
            execute(
                    "create table y (" +
                            "  ts timestamp," +
                            "  sym symbol," +
                            "  val int" +
                            ") timestamp(ts) partition by day;"
            );

            // Insert x row
            execute("insert into x values ('2023-01-01T09:01:00.000000Z', 'A');");

            // Insert multiple y rows at the same timestamp (the window boundary).
            // The join filter will exclude the first row (val=1) but match the second (val=2).
            execute(
                    "insert into y values " +
                            "('2023-01-01T09:00:00.000000Z', 'A', 1)," +
                            "('2023-01-01T09:00:00.000000Z', 'A', 2)," +
                            "('2023-01-01T09:01:00.000000Z', 'A', 3);"
            );

            assertQueryNoLeakCheck(
                    """
                            ts\tsym\tsum
                            2023-01-01T09:01:00.000000Z\tA\t5
                            """,
                    "select x.ts, x.sym, sum(y.val) " +
                            "from x " +
                            "window join y on (x.sym = y.sym and y.val > 1) " +
                            "range between 1 minute preceding and 1 minute following " +
                            (includePrevailing ? "include prevailing" : "exclude prevailing"),
                    "ts",
                    false,
                    false
            );
        });
    }

    @Test
    public void testJoinFilterWithMultipleRowsAtSameTimestampNoSymbol() throws Exception {
        // Tests join filter with multiple rows at the same timestamp boundary
        // when using the non-fast path (no symbol equality).

        // timestamp types don't matter for this test
        Assume.assumeTrue(leftTableTimestampType == TestTimestampType.MICRO);
        Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);

        assertMemoryLeak(() -> {
            execute(
                    "create table x (" +
                            "  ts timestamp," +
                            "  id int" +
                            ") timestamp(ts) partition by day;"
            );
            execute(
                    "create table y (" +
                            "  ts timestamp," +
                            "  id int," +
                            "  val int" +
                            ") timestamp(ts) partition by day;"
            );

            // Insert x row
            execute("insert into x values ('2023-01-01T09:01:00.000000Z', 1);");

            // Insert multiple y rows at the same timestamp.
            // First row at the boundary doesn't match the filter, second one does.
            execute(
                    "insert into y values " +
                            "('2023-01-01T09:00:00.000000Z', 1, 10)," +
                            "('2023-01-01T09:00:00.000000Z', 1, 20)," +
                            "('2023-01-01T09:01:00.000000Z', 1, 30);"
            );

            assertQueryNoLeakCheck(
                    """
                            ts\tid\tsum
                            2023-01-01T09:01:00.000000Z\t1\t50
                            """,
                    "select x.ts, x.id, sum(y.val) " +
                            "from x " +
                            "window join y on (x.id = y.id and y.val > 10) " +
                            "range between 1 minute preceding and 1 minute following " +
                            (includePrevailing ? "include prevailing" : "exclude prevailing"),
                    "ts",
                    false,
                    false
            );
        });
    }

    @Test
    public void testMasterFilterLimit() throws Exception {
        // timestamp types don't matter for this test
        Assume.assumeTrue(leftTableTimestampType == TestTimestampType.MICRO);
        Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);
        assertMemoryLeak(() -> {
            execute(
                    "create table trades (" +
                            "  sym symbol," +
                            "  price double," +
                            "  ts timestamp" +
                            ") timestamp(ts) partition by day;"
            );
            execute(
                    "create table prices (" +
                            "  sym symbol," +
                            "  bid double," +
                            "  ts timestamp" +
                            ") timestamp(ts) partition by day;"
            );

            execute(
                    "insert into trades values " +
                            "('sym0', 1.0, '2023-01-01T08:59:56.000000Z')," +
                            "('sym0', 2.0, '2023-01-01T08:59:57.000000Z')," +
                            "('sym0', 3.0, '2023-01-01T08:59:58.000000Z')," +
                            "('sym0', 4.0, '2023-01-01T08:59:59.000000Z')," +
                            "('sym0', 5.0, '2023-01-01T09:00:00.000000Z')," +
                            "('sym0', 6.0, '2023-01-01T09:00:01.000000Z')," +
                            "('sym0', 7.0, '2023-01-01T09:00:02.000000Z')," +
                            "('sym0', 8.0, '2023-01-01T09:00:03.000000Z')," +
                            "('sym0', 9.0, '2023-01-01T09:00:04.000000Z');"
            );

            execute(
                    "insert into prices values " +
                            "('sym0', 1.0, '2023-01-01T08:59:56.000000Z')," +
                            "('sym0', 2.0, '2023-01-01T08:59:57.000000Z')," +
                            "('sym0', 3.0, '2023-01-01T08:59:58.000000Z')," +
                            "('sym0', 4.0, '2023-01-01T08:59:59.000000Z')," +
                            "('sym0', 5.0, '2023-01-01T09:00:00.000000Z')," +
                            "('sym0', 6.0, '2023-01-01T09:00:01.000000Z')," +
                            "('sym0', 7.0, '2023-01-01T09:00:02.000000Z')," +
                            "('sym0', 8.0, '2023-01-01T09:00:03.000000Z')," +
                            "('sym0', 9.0, '2023-01-01T09:00:04.000000Z');"
            );

            String whatToDoWithPrevailing = includePrevailing ? "include" : "exclude";
            assertQueryAndPlan(
                    """
                            sym\tprice\tts\tfirst\tavg
                            """,
                    String.format("""
                            Sort
                              keys: [ts, sym]
                                Window Fast Join
                                  vectorized: true
                                  symbol: sym=sym
                                  window lo: 60000000 preceding (%s prevailing)
                                  window hi: 60000000 following
                                    Limit left: 5 right: 9 skip-rows-max: 5 take-rows-max: 4
                                        Async JIT Filter workers: 1
                                          filter: 5<price
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: trades
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: prices
                            """, whatToDoWithPrevailing),
                    String.format("""
                                    SELECT t.sym, t.price, t.ts, first(p.bid) AS first, avg(p.bid) AS avg
                                    FROM (trades WHERE price > 5 LIMIT 5, 9) t
                                    WINDOW JOIN prices p
                                    ON (t.sym = p.sym)
                                        RANGE BETWEEN 1 MINUTE PRECEDING AND 1 MINUTE FOLLOWING %s PREVAILING
                                    ORDER BY t.ts, t.sym
                                    """,
                            whatToDoWithPrevailing),
                    "ts",
                    true,
                    false
            );
        });

    }

    @Test
    public void testMasterHasIntervalFilter() throws Exception {
        assertMemoryLeak(() -> {
            prepareTable();
            if (!includePrevailing) {
                printSql("select t.sym, t.price, t.ts, sum(p.price) window_price " +
                        "from (select * from trades where ts <= '2023-01-01T09:04:00.000000Z') t " +
                        "left join prices p " +
                        "on (t.sym = p.sym) " +
                        " and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) " +
                        "order by t.ts, t.sym;", sink);
            } else {
                printSql("""
                                with t as (select * from trades where ts <= '2023-01-01T09:04:00.000000Z')
                                select sym,price,ts, sum(price1) window_price from
                                (
                                        select * from (
                                            select t.sym, t.price, t.ts, p.price
                                            from t
                                            left join prices p
                                            on (t.sym = p.sym)
                                            and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts)
                                        union
                                            select sym,price,ts,price1  from (select t.sym, t.price, t.ts, p.price price1, p.ts as ts1
                                            from t
                                            join prices p
                                            on (t.sym = p.sym) and p.ts <= dateadd('m', -1, t.ts)) LATEST ON ts1 PARTITION BY ts, sym
                                        ) order by ts
                                )
                                order by ts, sym;
                                """,
                        sink);
            }
            assertQueryAndPlan(
                    sink,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Async Window Fast Join workers: 1\n" +
                            "      vectorized: true\n" +
                            "      symbol: sym=sym\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding " + (includePrevailing ? "(include prevailing)\n" : "(exclude prevailing)\n") +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " following\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Interval forward scan on: trades\n" +
                            "              intervals: [(\"MIN\",\"2023-01-01T09:04:00." + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "000000Z" : "000000000Z") + "\")]\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Interval forward scan on: prices\n" +
                            "              intervals: [(\"MIN\",\"" + (ColumnType.isTimestampMicro(rightTableTimestampType.getTimestampType()) ? "2023-01-01T09:05:00.000000Z" : "2023-01-01T09:05:00.000000000Z") + "\")]\n",
                    "select t.sym, t.price, t.ts, sum(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on (t.sym = p.sym) " +
                            " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            " where t.ts <= '2023-01-01T09:04:00.000000Z' " +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );

            assertQueryAndPlan(
                    sink,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Async Window Fast Join workers: 1\n" +
                            "      vectorized: true\n" +
                            "      symbol: sym=sym\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding " + (includePrevailing ? "(include prevailing)\n" : "(exclude prevailing)\n") +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " following\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Interval forward scan on: trades\n" +
                            "              intervals: [(\"MIN\",\"2023-01-01T09:04:00." + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "000000Z" : "000000000Z") + "\")]\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Interval forward scan on: prices\n" +
                            "              intervals: [(\"MIN\",\"" + (ColumnType.isTimestampMicro(rightTableTimestampType.getTimestampType()) ? "2023-01-01T09:05:00.000000Z" : "2023-01-01T09:05:00.000000000Z") + "\")]\n",
                    "declare @x := '2023-01-01T09:04:00.000000Z' select t.sym, t.price, t.ts, sum(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on (t.sym = p.sym)  " +
                            " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            " where t.ts <= @x " +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );

            if (!includePrevailing) {
                printSql("select t.sym, t.price, t.ts, sum(p.price) window_price " +
                        "from (select * from trades where ts > '2023-01-01T09:04:00.000000Z') t " +
                        "left join prices p " +
                        "on (t.sym = p.sym) " +
                        " and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) " +
                        "order by t.ts, t.sym;", sink);
            } else {
                printSql("""
                                with t as (select * from trades where ts > '2023-01-01T09:04:00.000000Z')
                                select sym,price,ts, sum(price1) window_price from
                                (
                                        select * from (
                                            select t.sym, t.price, t.ts, p.price
                                            from t
                                            left join prices p
                                            on (t.sym = p.sym)
                                            and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts)
                                        union
                                            select sym,price,ts,price1  from (select t.sym, t.price, t.ts, p.price price1, p.ts as ts1
                                            from t
                                            join prices p
                                            on (t.sym = p.sym) and p.ts <= dateadd('m', -1, t.ts)) LATEST ON ts1 PARTITION BY ts, sym
                                        ) order by ts
                                )
                                order by ts, sym;
                                """,
                        sink);
            }
            assertQueryAndPlan(
                    sink,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Async Window Fast Join workers: 1\n" +
                            "      vectorized: true\n" +
                            "      symbol: sym=sym\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding " + (includePrevailing ? "(include prevailing)\n" : "(exclude prevailing)\n") +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " following\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Interval forward scan on: trades\n" +
                            "              intervals: [(\"2023-01-01T09:04:00." + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "000001Z" : "000000001Z") + "\",\"MAX\")]\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            (includePrevailing ?
                                    "            Frame forward scan on: prices\n"
                                    : "            Interval forward scan on: prices\n" +
                                    "              intervals: [(\"" + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? (ColumnType.isTimestampMicro(rightTableTimestampType.getTimestampType()) ? "2023-01-01T09:03:00.000001Z" : "2023-01-01T09:03:00.000001000Z")
                                    : (ColumnType.isTimestampMicro(rightTableTimestampType.getTimestampType()) ? "2023-01-01T09:03:00.000000Z" : "2023-01-01T09:03:00.000000001Z")) + "\",\"MAX\")]\n"),
                    "select t.sym, t.price, t.ts, sum(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on (t.sym = p.sym)  " +
                            " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            " where t.ts > '2023-01-01T09:04:00.000000Z'" +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testMixedColumnReused() throws Exception {
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
                            "  x float," +
                            "  ts timestamp" +
                            ") timestamp(ts) partition by day;"
            );

            execute(
                    "insert into x values " +
                            "('sym0', '2023-01-01T09:00:00.000000Z');"
            );
            execute(
                    "insert into y values " +
                            "('sym0', 1.0f, '2023-01-01T08:59:58.000000Z')," +
                            "('sym0', 2.0f, '2023-01-01T08:59:59.000000Z')," +
                            "('sym0', 3.0f, '2023-01-01T09:00:00.000000Z')," +
                            "('sym0', 4.0f, '2023-01-01T09:00:01.000000Z')," +
                            "('sym0', 5.0f, '2023-01-01T09:00:02.000000Z');"
            );

            assertQueryAndPlan(
                    """
                            s\tts\tfirst\tavg
                            sym0\t2023-01-01T09:00:00.000000Z\t2.0\t3.0
                            """,
                    """
                            Sort
                              keys: [ts, s]
                                Async Window Fast Join workers: 1
                                  vectorized: true
                                  symbol: s=s
                            """ +
                            "      window lo: 1000000 preceding" + (includePrevailing ? " (include prevailing)\n" : " (exclude prevailing)\n")
                            +
                            """
                                          window hi: 1000000 following
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: x
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: y
                                    """,
                    "select x.*, first(y.x) as first, avg(y.x) as avg " +
                            "from x " +
                            "window join y " +
                            "on (x.s = y.s) " +
                            " range between 1 second preceding and 1 second following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by x.ts, x.s;",
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testMultiAggregateColumns() throws Exception {
        assertMemoryLeak(() -> {
            Assume.assumeTrue(leftTableTimestampType.getTimestampType() == rightTableTimestampType.getTimestampType());
            prepareTable();
            if (!includePrevailing) {
                printSql("select t.sym, t.price, t.ts, sum(p.price), max(p.ts), avg(p.price), min(p.ts) " +
                        "from trades t " +
                        "left join prices p " +
                        "on (t.sym = p.sym) " +
                        " and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) " +
                        "order by t.ts, t.sym;", sink);
            } else {
                printSql("""
                                select sym,price,ts, sum(price1),max(pts), avg(price1), min(pts) from
                                (
                                        select * from (
                                            select t.sym, t.price, t.ts, p.price, p.ts pts
                                            from trades t
                                            left join prices p
                                            on (t.sym = p.sym)
                                            and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts)
                                        union
                                            select sym,price,ts,price1,ts1  from (select t.sym, t.price, t.ts, p.price price1, p.ts as ts1
                                            from trades t
                                            join prices p
                                            on (t.sym = p.sym) and p.ts <= dateadd('m', -1, t.ts)) LATEST ON ts1 PARTITION BY ts, sym
                                        ) order by ts
                                )
                                order by ts, sym;
                                """,
                        sink);
            }
            assertQueryAndPlan(
                    sink,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Async Window Fast Join workers: 1\n" +
                            "      vectorized: true\n" +
                            "      symbol: sym=sym\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding " + (includePrevailing ? "(include prevailing)\n" : "(exclude prevailing)\n") +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " following\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: trades\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: prices\n",
                    "select t.sym, t.price, t.ts, sum(p.price), max(p.ts), avg(p.price), min(p.ts) " +
                            "from trades t " +
                            "window join prices p " +
                            "on (t.sym = p.sym) " +
                            " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );

            if (!includePrevailing) {
                printSql("select t.sym, t.price, t.ts, max(p.ts), sum(p.price), avg(p.price), count(p.price), min(p.ts) " +
                        "from trades t " +
                        "left join prices p " +
                        " on p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) " +
                        "order by t.ts, t.sym;", sink);
            } else {
                printSql("""
                                select sym,price,ts, max(pts), sum(price1), avg(price1), count(price1), min(pts) from
                                (
                                        select * from (
                                            select t.sym, t.price, t.ts, p.price, p.ts pts
                                            from trades t
                                            left join prices p
                                            on p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts)
                                        union
                                            select sym,price,ts,price1,ts1  from (select t.sym, t.price, t.ts, p.price price1, p.ts as ts1
                                            from trades t
                                            join prices p
                                            on p.ts <= dateadd('m', -1, t.ts)) LATEST ON ts1 PARTITION BY ts, sym
                                        ) order by ts
                                )
                                order by ts, sym;
                                """,
                        sink);
            }

            assertQueryAndPlan(
                    sink,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Async Window Join workers: 1\n" +
                            "      vectorized: true\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding " + (includePrevailing ? "(include prevailing)\n" : "(exclude prevailing)\n") +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " following\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: trades\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: prices\n",
                    "select t.sym, t.price, t.ts, max(p.ts), sum(p.price), avg(p.price), count(p.price), min(p.ts) " +
                            "from trades t " +
                            "window join prices p " +
                            " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );

            printSql("select t.sym, t.price, t.ts, max(p.ts), sum(p.price + 100), avg(p.price + 100), min(p.ts), avg(p.price + 101) " +
                    "from trades t " +
                    "left join prices p " +
                    " on p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) " +
                    "order by t.ts, t.sym;", sink);
            assertQueryAndPlan(
                    sink,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Async Window Join workers: 1\n" +
                            "      vectorized: true\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding " + (includePrevailing ? "(include prevailing)\n" : "(exclude prevailing)\n") +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " following\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: trades\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: prices\n",
                    "select t.sym, t.price, t.ts, max(p.ts), sum(p.price + 100), avg(p.price + 100), min(p.ts), avg(p.price + 101) " +
                            "from trades t " +
                            "window join prices p " +
                            " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testNotThreadSafeFunction() throws Exception {
        Assume.assumeTrue(leftTableTimestampType == TestTimestampType.MICRO);
        Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);
        assertMemoryLeak(() -> {
            prepareTable();
            if (!includePrevailing) {
                printSql(" SELECT t.ts, t.price price, t.sym, max(cast(concat(p.price, '0') as double)) max_price, count(p.ts) cnt " +
                        "from trades t " +
                        "left join prices p " +
                        "on (t.sym = p.sym) " +
                        " and p.ts >= dateadd('s', -1, t.ts) AND p.ts <= dateadd('s', 1, t.ts) " +
                        "order by t.ts, t.sym;", sink);
            } else {
                printSql("""
                                select ts, price, sym, max(cast(concat(price1, '0') as double)) max_price, count(pts) cnt from
                                (
                                        select * from (
                                            select t.sym, t.price, t.ts, p.price, p.ts pts
                                            from trades t
                                            left join prices p
                                            on (t.sym = p.sym)
                                            and p.ts >= dateadd('s', -1, t.ts) AND p.ts <= dateadd('s', 1, t.ts)
                                        union
                                            select sym,price,ts,price1,ts1  from (select t.sym, t.price, t.ts, p.price price1, p.ts as ts1
                                            from trades t
                                            join prices p
                                            on (t.sym = p.sym) and p.ts <= dateadd('s', -1, t.ts)) LATEST ON ts1 PARTITION BY ts, sym
                                        ) order by ts
                                )
                                order by ts, sym;
                                """,
                        sink);
            }

            assertQueryAndPlan(
                    sink,
                    """
                            Async Window Fast Join workers: 1
                              vectorized: true
                              symbol: sym=sym
                            """ +
                            "  window lo: 1000000 preceding" + (includePrevailing ? " (include prevailing)\n" : " (exclude prevailing)\n")
                            +
                            """
                                      window hi: 1000000 following
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: trades
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: prices
                                    """,
                    "  SELECT t.ts, t.price price,t.sym, max(cast(concat(p.price, '0') as double)) max_price, count() cnt " +
                            "  FROM trades t " +
                            "  WINDOW JOIN prices p ON t.sym = p.sym " +
                            "  RANGE BETWEEN 1 second PRECEDING AND 1 second FOLLOWING " + (includePrevailing ? " INCLUDE PREVAILING " : " EXCLUDE PREVAILING "),
                    "ts",
                    false,
                    false
            );

            if (!includePrevailing) {
                printSql(" SELECT t.ts, t.price price, t.sym, max(cast(concat(p.price, '2') as double)) max_price, count(p.ts) cnt " +
                        "from trades t " +
                        "left join prices p " +
                        "on (t.sym = p.sym) " +
                        " and p.ts >= dateadd('s', -1, t.ts) AND p.ts <= dateadd('s', 1, t.ts) " +
                        " where cast(concat(t.price, '2') as double) > 200 " +
                        " order by t.ts, t.sym;", sink);
            } else {
                printSql("""
                                with t as (select * from trades where cast(concat(price, '2') as double) > 200)
                                select ts, price, sym, max(cast(concat(price1, '2') as double)) max_price, count(pts) cnt from
                                (
                                        select * from (
                                            select t.sym, t.price, t.ts, p.price, p.ts pts
                                            from t
                                            left join prices p
                                            on (t.sym = p.sym)
                                            and p.ts >= dateadd('s', -1, t.ts) AND p.ts <= dateadd('s', 1, t.ts)
                                        union
                                            select sym,price,ts,price1,ts1  from (select t.sym, t.price, t.ts, p.price price1, p.ts as ts1
                                            from t
                                            join prices p
                                            on (t.sym = p.sym) and p.ts <= dateadd('s', -1, t.ts)) LATEST ON ts1 PARTITION BY ts, sym
                                        ) order by ts
                                )
                                order by ts, sym;
                                """,
                        sink);
            }

            assertQueryAndPlan(
                    sink,
                    """
                            Async Window Fast Join workers: 1
                              vectorized: true
                              symbol: sym=sym
                            """ +
                            "  window lo: 1000000 preceding" + (includePrevailing ? " (include prevailing)\n" : " (exclude prevailing)\n")
                            +
                            """
                                      window hi: 1000000 following
                                      master filter: 200<concat([price,'2'])::double
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: trades
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: prices
                                    """,
                    "  SELECT t.ts, t.price, t.sym, max(cast(concat(p.price, '2') as double)) max_price, count() cnt " +
                            "  FROM trades t " +
                            "  WINDOW JOIN prices p ON t.sym = p.sym " +
                            "  RANGE BETWEEN 1 second PRECEDING AND 1 second FOLLOWING " + (includePrevailing ? " INCLUDE PREVAILING " : " EXCLUDE PREVAILING ") +
                            "  WHERE cast(concat(t.price, '2') as double) > 200",
                    "ts",
                    false,
                    false
            );
        });
    }

    @Test
    public void testVectorizedWindowJoin() throws Exception {
        assertMemoryLeak(() -> {
            prepareTable();
            if (!includePrevailing) {
                printSql("select t.sym, t.price, t.ts, avg(p.price) window_price " +
                        "from trades t " +
                        "left join prices p " +
                        "on (t.sym = p.sym) " +
                        " and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) " +
                        "order by t.ts, t.sym;", sink);
            } else {
                printSql("""
                                select sym,price,ts, avg(price1) window_price from
                                (
                                        select * from (
                                            select t.sym, t.price, t.ts, p.price, p.ts pts
                                            from trades t
                                            left join prices p
                                            on (t.sym = p.sym)
                                            and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts)
                                        union
                                            select sym,price,ts,price1,ts1  from (select t.sym, t.price, t.ts, p.price price1, p.ts as ts1
                                            from trades t
                                            join prices p
                                            on (t.sym = p.sym) and p.ts <= dateadd('m', -1, t.ts)) LATEST ON ts1 PARTITION BY ts, sym
                                        ) order by ts
                                )
                                order by ts, sym;
                                """,
                        sink);
            }
            assertQueryAndPlan(
                    sink,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Async Window Fast Join workers: 1\n" +
                            "      vectorized: true\n" +
                            "      symbol: sym=sym\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding " + (includePrevailing ? "(include prevailing)\n" : "(exclude prevailing)\n") +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " following\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: trades\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: prices\n",
                    "select t.sym, t.price, t.ts, avg(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on (t.sym = p.sym) " +
                            " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testWindowJoinBinarySearch() throws Exception {
        assertMemoryLeak(() -> {
            prepareTable();
            if (!includePrevailing) {
                printSql("select t.sym, t.price, t.ts, sum(p.price) window_price " +
                        "from trades t " +
                        "left join prices p " +
                        "on p.ts >= dateadd('m', -15, t.ts) AND p.ts <= dateadd('m', -14, t.ts) " +
                        "order by t.ts, t.sym;", sink);
            } else {
                printSql("""
                                select sym,price,ts, sum(price1) window_price from
                                (
                                        select * from (
                                            select t.sym, t.price, t.ts, p.price, p.ts pts
                                            from trades t
                                            left join prices p
                                            on p.ts >= dateadd('m', -15, t.ts) AND p.ts <= dateadd('m', -14, t.ts)
                                        union
                                            select sym,price,ts,price1,ts1  from (select t.sym, t.price, t.ts, p.price price1, p.ts as ts1
                                            from trades t
                                            join prices p
                                            on p.ts <= dateadd('m', -15, t.ts)) LATEST ON ts1 PARTITION BY ts, sym
                                        ) order by ts
                                )
                                order by ts, sym;
                                """,
                        sink);
            }
            assertQueryAndPlan(
                    sink,
                    "Async Window Join workers: 1\n" +
                            "  vectorized: true\n" +
                            "  window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "900000000" : "900000000000") + " preceding " + (includePrevailing ? "(include prevailing)\n" : "(exclude prevailing)\n") +
                            "  window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "840000000" : "840000000000") + " preceding\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: trades\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: prices\n",
                    "select t.sym, t.price, t.ts, sum(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            " range between 15 minute preceding and 14 minute preceding " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by t.ts;",
                    "ts",
                    false,
                    false
            );

            if (!includePrevailing) {
                printSql("select t.sym, t.price, t.ts, sum(p.price) window_price " +
                        "from trades t " +
                        "left join prices p " +
                        "on t.sym = p.sym and p.ts >= dateadd('m', -5, t.ts) AND p.ts <= dateadd('m', -4, t.ts) " +
                        "order by t.ts, t.sym;", sink);
            } else {
                printSql("""
                                select sym,price,ts, sum(price1) window_price from
                                (
                                        select * from (
                                            select t.sym, t.price, t.ts, p.price, p.ts pts
                                            from trades t
                                            left join prices p
                                            on (t.sym = p.sym)
                                            and p.ts >= dateadd('m', -5, t.ts) AND p.ts <= dateadd('m', -4, t.ts)
                                        union
                                            select sym,price,ts,price1,ts1  from (select t.sym, t.price, t.ts, p.price price1, p.ts as ts1
                                            from trades t
                                            join prices p
                                            on (t.sym = p.sym) and p.ts <= dateadd('m', -5, t.ts)) LATEST ON ts1 PARTITION BY ts, sym
                                        ) order by ts
                                )
                                order by ts, sym;
                                """,
                        sink);
            }
            assertQueryAndPlan(
                    sink,
                    "Async Window Fast Join workers: 1\n" +
                            "  vectorized: true\n" +
                            "  symbol: sym=sym\n" +
                            "  window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "300000000" : "300000000000") + " preceding " + (includePrevailing ? "(include prevailing)\n" : "(exclude prevailing)\n") +
                            "  window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "240000000" : "240000000000") + " preceding\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: trades\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: prices\n",
                    "select t.sym, t.price, t.ts, sum(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p on t.sym = p.sym" +
                            " range between 5 minute preceding and 4 minute preceding " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by t.ts;",
                    "ts",
                    false,
                    false
            );
        });
    }

    @Test
    public void testWindowJoinChain() throws Exception {
        assertMemoryLeak(() -> {
            prepareTable();
            String expect = includePrevailing ?
                    replaceTimestampSuffix("""
                            ts\tsym\twindow_price1\twindow_price2
                            2023-01-01T09:00:00.000000Z\t\tnull\tnull
                            2023-01-01T09:01:00.000000Z\t\tnull\tnull
                            2023-01-01T09:02:00.000000Z\t\tnull\tnull
                            2023-01-01T09:03:00.000000Z\t\tnull\tnull
                            2023-01-01T09:04:00.000000Z\t\tnull\tnull
                            2023-01-01T09:05:00.000000Z\t\tnull\tnull
                            2023-01-01T09:06:00.000000Z\t\tnull\tnull
                            2023-01-01T09:07:00.000000Z\t\tnull\tnull
                            2023-01-01T09:08:00.000000Z\t\tnull\tnull
                            2023-01-01T09:09:00.000000Z\t\tnull\tnull
                            2023-01-01T09:10:00.000000Z\tTSLA\t800.0\t800.0
                            2023-01-01T09:11:00.000000Z\tTSLA\t400.5\t800.0
                            2023-01-01T09:12:00.000000Z\tAMZN\t1000.0\t1000.0
                            2023-01-01T09:13:00.000000Z\tAMZN\t500.5\t1501.5
                            2023-01-01T09:14:00.000000Z\tMETA\t1200.0\t1801.5
                            2023-01-01T09:15:00.000000Z\tMETA\t600.5\t1801.5
                            2023-01-01T09:16:00.000000Z\tTSLA\t401.5\t802.0
                            2023-01-01T09:17:00.000000Z\tAMZN\t501.5\t1002.0
                            2023-01-01T09:18:00.000000Z\tMETA\t601.5\t1202.0
                            2023-01-02T09:19:00.000000Z\tNFLX\t699.5\t699.5
                            """, leftTableTimestampType.getTypeName())
                    : replaceTimestampSuffix("""
                    ts	sym	window_price1	window_price2
                    2023-01-01T09:00:00.000000Z		null	null
                    2023-01-01T09:01:00.000000Z		null	null
                    2023-01-01T09:02:00.000000Z		null	null
                    2023-01-01T09:03:00.000000Z		null	null
                    2023-01-01T09:04:00.000000Z		null	null
                    2023-01-01T09:05:00.000000Z		null	null
                    2023-01-01T09:06:00.000000Z		null	null
                    2023-01-01T09:07:00.000000Z		null	null
                    2023-01-01T09:08:00.000000Z		null	null
                    2023-01-01T09:09:00.000000Z		null	null
                    2023-01-01T09:10:00.000000Z	TSLA	800.0	800.0
                    2023-01-01T09:11:00.000000Z	TSLA	400.5	800.0
                    2023-01-01T09:12:00.000000Z	AMZN	1000.0	1000.0
                    2023-01-01T09:13:00.000000Z	AMZN	500.5	1501.5
                    2023-01-01T09:14:00.000000Z	META	1200.0	1801.5
                    2023-01-01T09:15:00.000000Z	META	600.5	1801.5
                    2023-01-01T09:16:00.000000Z	TSLA	401.5	401.5
                    2023-01-01T09:17:00.000000Z	AMZN	501.5	501.5
                    2023-01-01T09:18:00.000000Z	META	601.5	601.5
                    2023-01-02T09:19:00.000000Z	NFLX	699.5	699.5
                    """, leftTableTimestampType.getTypeName());
            assertQueryAndPlan(
                    expect,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Window Fast Join\n" +
                            "      vectorized: true\n" +
                            "      symbol: t.sym=sym\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "180000000" : "180000000000") + " preceding " + (includePrevailing ? "(include prevailing)\n" : "(exclude prevailing)\n") +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "180000000" : "180000000000") + " following\n" +
                            "        Async Window Fast Join workers: 1\n" +
                            "          vectorized: true\n" +
                            "          symbol: sym=sym\n" +
                            "          window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding " + (includePrevailing ? "(include prevailing)\n" : "(exclude prevailing)\n") +
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
                            " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "window join prices p1 " +
                            "on (t.sym = p1.sym) " +
                            " range between 3 minute preceding and 3 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );

            expect = includePrevailing ?
                    replaceTimestampSuffix("""
                            ts\tsym\twindow_price2
                            2023-01-01T09:00:00.000000Z\t\tnull
                            2023-01-01T09:01:00.000000Z\t\tnull
                            2023-01-01T09:02:00.000000Z\t\tnull
                            2023-01-01T09:03:00.000000Z\t\tnull
                            2023-01-01T09:04:00.000000Z\t\tnull
                            2023-01-01T09:05:00.000000Z\t\tnull
                            2023-01-01T09:06:00.000000Z\t\tnull
                            2023-01-01T09:07:00.000000Z\t\tnull
                            2023-01-01T09:08:00.000000Z\t\tnull
                            2023-01-01T09:09:00.000000Z\t\tnull
                            2023-01-01T09:10:00.000000Z\tTSLA\t800.0
                            2023-01-01T09:11:00.000000Z\tTSLA\t800.0
                            2023-01-01T09:12:00.000000Z\tAMZN\t1000.0
                            2023-01-01T09:13:00.000000Z\tAMZN\t1501.5
                            2023-01-01T09:14:00.000000Z\tMETA\t1801.5
                            2023-01-01T09:15:00.000000Z\tMETA\t1801.5
                            2023-01-01T09:16:00.000000Z\tTSLA\t802.0
                            2023-01-01T09:17:00.000000Z\tAMZN\t1002.0
                            2023-01-01T09:18:00.000000Z\tMETA\t1202.0
                            2023-01-02T09:19:00.000000Z\tNFLX\t699.5
                            """, leftTableTimestampType.getTypeName())
                    : replaceTimestampSuffix("""
                    ts	sym	window_price2
                    2023-01-01T09:00:00.000000Z		null
                    2023-01-01T09:01:00.000000Z		null
                    2023-01-01T09:02:00.000000Z		null
                    2023-01-01T09:03:00.000000Z		null
                    2023-01-01T09:04:00.000000Z		null
                    2023-01-01T09:05:00.000000Z		null
                    2023-01-01T09:06:00.000000Z		null
                    2023-01-01T09:07:00.000000Z		null
                    2023-01-01T09:08:00.000000Z		null
                    2023-01-01T09:09:00.000000Z		null
                    2023-01-01T09:10:00.000000Z	TSLA	800.0
                    2023-01-01T09:11:00.000000Z	TSLA	800.0
                    2023-01-01T09:12:00.000000Z	AMZN	1000.0
                    2023-01-01T09:13:00.000000Z	AMZN	1501.5
                    2023-01-01T09:14:00.000000Z	META	1801.5
                    2023-01-01T09:15:00.000000Z	META	1801.5
                    2023-01-01T09:16:00.000000Z	TSLA	401.5
                    2023-01-01T09:17:00.000000Z	AMZN	501.5
                    2023-01-01T09:18:00.000000Z	META	601.5
                    2023-01-02T09:19:00.000000Z	NFLX	699.5
                    """, leftTableTimestampType.getTypeName());
            assertQueryAndPlan(
                    expect,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Window Fast Join\n" +
                            "      vectorized: true\n" +
                            "      symbol: t.sym=sym\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "180000000" : "180000000000") + " preceding " + (includePrevailing ? "(include prevailing)\n" : "(exclude prevailing)\n") +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "180000000" : "180000000000") + " following\n" +
                            "        Async Window Fast Join workers: 1\n" +
                            "          vectorized: false\n" +
                            "          symbol: sym=sym\n" +
                            "          window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding " + (includePrevailing ? "(include prevailing)\n" : "(exclude prevailing)\n") +
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
                    "select t.ts, t.sym, sum(p1.price) as window_price2 " +
                            "from trades t " +
                            "window join prices p " +
                            "on (t.sym = p.sym) " +
                            " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "window join prices p1 " +
                            "on (t.sym = p1.sym) " +
                            " range between 3 minute preceding and 3 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );

            expect = replaceTimestampSuffix("""
                    ts	sym	window_price1	window_price2
                    2023-01-01T09:00:00.000000Z		null	null
                    2023-01-01T09:01:00.000000Z		null	null
                    2023-01-01T09:02:00.000000Z		null	null
                    2023-01-01T09:03:00.000000Z		null	null
                    2023-01-01T09:04:00.000000Z		null	null
                    2023-01-01T09:05:00.000000Z		null	null
                    2023-01-01T09:06:00.000000Z		null	null
                    2023-01-01T09:07:00.000000Z		null	null
                    2023-01-01T09:08:00.000000Z		399.5	null
                    2023-01-01T09:09:00.000000Z		800.0	null
                    2023-01-01T09:10:00.000000Z	TSLA	1299.5	null
                    2023-01-01T09:11:00.000000Z	TSLA	1400.5	null
                    2023-01-01T09:12:00.000000Z	AMZN	1599.5	null
                    2023-01-01T09:13:00.000000Z	AMZN	1700.5	null
                    2023-01-01T09:14:00.000000Z	META	1601.5	null
                    2023-01-01T09:15:00.000000Z	META	1503.5	null
                    2023-01-01T09:16:00.000000Z	TSLA	1504.5	null
                    2023-01-01T09:17:00.000000Z	AMZN	1103.0	null
                    2023-01-01T09:18:00.000000Z	META	601.5	null
                    2023-01-02T09:19:00.000000Z	NFLX	699.5	null
                    """, leftTableTimestampType.getTypeName());
            assertQueryAndPlan(
                    expect,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    ExtraNullColumnRecord\n" +
                            "        Async Window Join workers: 1\n" +
                            "          vectorized: true\n" +
                            "          window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding " + (includePrevailing ? "(include prevailing)\n" : "(exclude prevailing)\n") +
                            "          window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " following\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: trades\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: prices\n",
                    "select t.ts, t.sym, sum(p.price) window_price1, sum(p1.price) as window_price2 " +
                            "from trades t " +
                            "window join prices p " +
                            "on (1 = 1) " +
                            " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "window join prices p1 " +
                            "on (0 = 1) " +
                            " range between 2 minute preceding and 2 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );

            expect = includePrevailing ?
                    replaceTimestampSuffix("""
                            ts	sym	window_price1	window_price2
                            2023-01-01T09:00:00.000000Z		null	null
                            2023-01-01T09:01:00.000000Z		null	null
                            2023-01-01T09:02:00.000000Z		null	null
                            2023-01-01T09:03:00.000000Z		null	null
                            2023-01-01T09:04:00.000000Z		null	null
                            2023-01-01T09:05:00.000000Z		null	null
                            2023-01-01T09:06:00.000000Z		null	null
                            2023-01-01T09:07:00.000000Z		null	null
                            2023-01-01T09:08:00.000000Z		null	null
                            2023-01-01T09:09:00.000000Z		null	null
                            2023-01-01T09:10:00.000000Z	TSLA	null	800.0
                            2023-01-01T09:11:00.000000Z	TSLA	null	800.0
                            2023-01-01T09:12:00.000000Z	AMZN	null	1000.0
                            2023-01-01T09:13:00.000000Z	AMZN	null	1000.0
                            2023-01-01T09:14:00.000000Z	META	null	1200.0
                            2023-01-01T09:15:00.000000Z	META	null	1801.5
                            2023-01-01T09:16:00.000000Z	TSLA	null	802.0
                            2023-01-01T09:17:00.000000Z	AMZN	null	1002.0
                            2023-01-01T09:18:00.000000Z	META	null	1202.0
                            2023-01-02T09:19:00.000000Z	NFLX	null	699.5
                            """, leftTableTimestampType.getTypeName())
                    : replaceTimestampSuffix("""
                    ts	sym	window_price1	window_price2
                    2023-01-01T09:00:00.000000Z		null	null
                    2023-01-01T09:01:00.000000Z		null	null
                    2023-01-01T09:02:00.000000Z		null	null
                    2023-01-01T09:03:00.000000Z		null	null
                    2023-01-01T09:04:00.000000Z		null	null
                    2023-01-01T09:05:00.000000Z		null	null
                    2023-01-01T09:06:00.000000Z		null	null
                    2023-01-01T09:07:00.000000Z		null	null
                    2023-01-01T09:08:00.000000Z		null	null
                    2023-01-01T09:09:00.000000Z		null	null
                    2023-01-01T09:10:00.000000Z	TSLA	null	800.0
                    2023-01-01T09:11:00.000000Z	TSLA	null	800.0
                    2023-01-01T09:12:00.000000Z	AMZN	null	1000.0
                    2023-01-01T09:13:00.000000Z	AMZN	null	1000.0
                    2023-01-01T09:14:00.000000Z	META	null	1200.0
                    2023-01-01T09:15:00.000000Z	META	null	1801.5
                    2023-01-01T09:16:00.000000Z	TSLA	null	401.5
                    2023-01-01T09:17:00.000000Z	AMZN	null	501.5
                    2023-01-01T09:18:00.000000Z	META	null	601.5
                    2023-01-02T09:19:00.000000Z	NFLX	null	699.5
                    """, leftTableTimestampType.getTypeName());
            assertQueryAndPlan(
                    expect,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Async Window Fast Join workers: 1\n" +
                            "      vectorized: true\n" +
                            "      symbol: t.sym=sym\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " preceding " + (includePrevailing ? "(include prevailing)\n" : "(exclude prevailing)\n") +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " following\n" +
                            "        ExtraNullColumnRecord\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: trades\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: prices\n",
                    "select t.ts, t.sym, sum(p.price) window_price1, sum(p1.price) as window_price2 " +
                            "from trades t " +
                            "window join prices p " +
                            "on (1 = 0 and t.sym = p.sym) " +
                            " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "window join prices p1 " +
                            "on (1 = 1 and t.sym = p1.sym) " +
                            " range between 2 minute preceding and 2 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );

            // count is resolved to the first slave
            expect = replaceTimestampSuffix(
                    """
                            ts	sym	window_price1	window_price2	cnt
                            2023-01-01T09:00:00.000000Z		null	null	3
                            2023-01-01T09:01:00.000000Z		null	null	3
                            2023-01-01T09:02:00.000000Z		null	null	3
                            2023-01-01T09:03:00.000000Z		null	null	3
                            2023-01-01T09:04:00.000000Z		null	null	3
                            2023-01-01T09:05:00.000000Z		null	null	3
                            2023-01-01T09:06:00.000000Z		null	null	3
                            2023-01-01T09:07:00.000000Z		null	null	3
                            2023-01-01T09:08:00.000000Z		null	null	2
                            2023-01-01T09:09:00.000000Z		null	null	1
                            2023-01-01T09:10:00.000000Z	TSLA	399.5	400.5	2
                            2023-01-01T09:11:00.000000Z	TSLA	400.5	400.5	1
                            2023-01-01T09:12:00.000000Z	AMZN	499.5	500.5	2
                            2023-01-01T09:13:00.000000Z	AMZN	500.5	501.5	1
                            2023-01-01T09:14:00.000000Z	META	599.5	601.5	2
                            2023-01-01T09:15:00.000000Z	META	600.5	601.5	1
                            2023-01-01T09:16:00.000000Z	TSLA	401.5	401.5	1
                            2023-01-01T09:17:00.000000Z	AMZN	501.5	501.5	1
                            2023-01-01T09:18:00.000000Z	META	601.5	601.5	1
                            2023-01-02T09:19:00.000000Z	NFLX	699.5	699.5	1
                            """,
                    leftTableTimestampType.getTypeName()
            );
            assertQueryAndPlan(
                    expect,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Window Fast Join\n" +
                            "      vectorized: true\n" +
                            "      symbol: t.sym=sym\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "180000000" : "180000000000") + " preceding " + (includePrevailing ? "(include prevailing)\n" : "(exclude prevailing)\n") +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "180000000" : "180000000000") + " following\n" +
                            "        Async Window Fast Join workers: 1\n" +
                            "          vectorized: true\n" +
                            "          symbol: sym=sym\n" +
                            "          window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding " + (includePrevailing ? "(include prevailing)\n" : "(exclude prevailing)\n") +
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
                    "select t.ts, t.sym, min(p.price) window_price1, max(p1.price) as window_price2, count() as cnt " +
                            "from trades t " +
                            "window join prices p " +
                            "on (t.sym = p.sym) " +
                            " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "window join prices p1 " +
                            "on (t.sym = p1.sym) " +
                            " range between 3 minute preceding and 3 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );

            assertExceptionNoLeakCheck(
                    "select t.ts, t.sym, sum(p1.price), p.price as window_price2 " +
                            "from trades t " +
                            "window join prices p " +
                            "on (t.sym = p.sym) " +
                            " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "window join prices p1 " +
                            "on (t.sym = p1.sym) " +
                            " range between 2 minute preceding and 2 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by t.ts, t.sym;",
                    35,
                    "WINDOW join cannot reference right table non-aggregate column: p.price"
            );

            assertExceptionNoLeakCheck(
                    "select t.ts, t.sym, sum(p1.price + p.price) as window_price2 " +
                            "from trades t " +
                            "window join prices p " +
                            "on (t.sym = p.sym) " +
                            " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "window join prices p1 " +
                            "on (t.sym = p1.sym) " +
                            " range between 2 minute preceding and 2 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by t.ts, t.sym;",
                    20,
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
                    "select t.sym, t.price, t.ts, sum(p.price) sum_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on t.price-p.price " +
                            " range between 1 second preceding and 1 second following" + (includePrevailing ? " include prevailing;" : " exclude prevailing;"),
                    97,
                    "boolean expression expected"
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
                    "select t.sym, t.price, t.ts, sum(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on (t.sym = p.sym) " +
                            " range between 2 minute preceding and 4 minute preceding;",
                    150,
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
                    "select t.sym, t.price, t.ts, p.price " +
                            "from trades t " +
                            "window join prices p " +
                            "on (t.sym = p.sym) " +
                            " range between 1 second preceding and 1 second following" + (includePrevailing ? " include prevailing;" : " exclude prevailing;"),
                    29,
                    "WINDOW join cannot reference right table non-aggregate column: p.price"
            );
        });
    }

    @Test
    public void testWindowJoinFailsOnSlaveColumnsInFilter() throws Exception {
        assertMemoryLeak(() -> {
            prepareTable();
            assertExceptionNoLeakCheck(
                    "select t.sym, t.price, t.ts, sum(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on (t.sym = p.sym) " +
                            " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "where p.price > 10 " +
                            "order by t.ts, t.sym;",
                    195,
                    "Invalid column: p.price"
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
                    "select t.sym, t.price, t.ts, sum(p.price) as window_price " +
                            "from (trades limit 5) t " +
                            "window join (select * from prices where ts in '2023-01-01T09:03:00.000000Z' or ts = '2023-01-01T09:07:00.000000Z' and ts = '2023-01-01T09:08:00.000000Z') p " +
                            "on (t.sym = p.sym) " +
                            " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by t.ts, t.sym;",
                    82,
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
                    "select t.sym, t.price, t.ts, sum(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on t.sym = p.sym " +
                            " range between unbounded preceding and 1 day following" + (includePrevailing ? " include prevailing;" : " exclude prevailing;"),
                    135,
                    "unbounded preceding/following is not supported in WINDOW joins"
            );

            assertExceptionNoLeakCheck(
                    "select t.sym, t.price, t.ts, sum(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on t.sym = p.sym " +
                            " range between 1 second preceding and unbounded following" + (includePrevailing ? " include prevailing;" : " exclude prevailing;"),
                    158,
                    "unbounded preceding/following is not supported in WINDOW joins"
            );

            assertExceptionNoLeakCheck(
                    "select t.sym, t.price, t.ts, sum(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on t.sym = p.sym " +
                            " range between unbounded preceding and unbounded following" + (includePrevailing ? " include prevailing;" : " exclude prevailing;"),
                    135,
                    "unbounded preceding/following is not supported in WINDOW joins"
            );
        });
    }

    @Test
    public void testWindowJoinNoOtherCondition() throws Exception {
        assertMemoryLeak(() -> {
            prepareTable();
            if (!includePrevailing) {
                printSql("select t.sym, t.price, t.ts, sum(p.price) window_price " +
                        "from trades t " +
                        "left join prices p " +
                        "on p.ts >= dateadd('m', -2, t.ts) AND p.ts <= dateadd('m', -1, t.ts) " +
                        "order by t.ts, t.sym;", sink);
            } else {
                printSql("""
                                select sym,price,ts, sum(price1) window_price from
                                (
                                        select * from (
                                            select t.sym, t.price, t.ts, p.price, p.ts pts
                                            from trades t
                                            left join prices p
                                            on p.ts >= dateadd('m', -2, t.ts) AND p.ts <= dateadd('m', -1, t.ts)
                                        union
                                            select sym,price,ts,price1,ts1  from (select t.sym, t.price, t.ts, p.price price1, p.ts as ts1
                                            from trades t
                                            join prices p
                                            on p.ts <= dateadd('m', -2, t.ts)) LATEST ON ts1 PARTITION BY ts, sym
                                        ) order by ts
                                )
                                order by ts, sym;
                                """,
                        sink);
            }

            assertQueryAndPlan(
                    sink,
                    "Async Window Join workers: 1\n" +
                            "  vectorized: true\n" +
                            "  window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " preceding " + (includePrevailing ? "(include prevailing)\n" : "(exclude prevailing)\n") +
                            "  window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: trades\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: prices\n",
                    "select t.sym, t.price, t.ts, sum(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            " range between 2 minute preceding and 1 minute preceding " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by t.ts;",
                    "ts",
                    false,
                    false
            );

            if (!includePrevailing) {
                printSql("select t.sym, t.price, t.ts, sum(p.price) window_price " +
                        "from trades t " +
                        "left join prices p " +
                        "on p.ts >= dateadd('m', -3, t.ts) AND p.ts <= dateadd('m', -2, t.ts) " +
                        "order by t.ts, t.sym;", sink);
            } else {
                printSql("""
                                select sym,price,ts, sum(price1) window_price from
                                (
                                        select * from (
                                            select t.sym, t.price, t.ts, p.price, p.ts pts
                                            from trades t
                                            left join prices p
                                            on p.ts >= dateadd('m', -3, t.ts) AND p.ts <= dateadd('m', -2, t.ts)
                                        union
                                            select sym,price,ts,price1,ts1  from (select t.sym, t.price, t.ts, p.price price1, p.ts as ts1
                                            from trades t
                                            join prices p
                                            on p.ts <= dateadd('m', -3, t.ts)) LATEST ON ts1 PARTITION BY ts, sym
                                        ) order by ts
                                )
                                order by ts, sym;
                                """,
                        sink);
            }
            assertQueryAndPlan(
                    sink,
                    "Async Window Join workers: 1\n" +
                            "  vectorized: true\n" +
                            "  window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "180000000" : "180000000000") + " preceding " + (includePrevailing ? "(include prevailing)\n" : "(exclude prevailing)\n") +
                            "  window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " preceding\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: trades\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: prices\n",
                    "select t.sym, t.price, t.ts, sum(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            " range between 3 minute preceding and 2 minute preceding " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by t.ts;",
                    "ts",
                    false,
                    false
            );

            if (!includePrevailing) {
                printSql("select t.sym, t.price, t.ts, sum(p.price) window_price, count() as cnt " +
                        "from (select * from trades where price < 300) t " +
                        "left join prices p " +
                        "on p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) " +
                        "order by t.ts, t.sym;", sink);
            } else {
                printSql("""
                                with t as (select * from trades where price < 300)
                                select sym,price,ts, sum(price1) window_price,count() as cnt from
                                (
                                        select * from (
                                            select t.sym, t.price, t.ts, p.price, p.ts pts
                                            from t
                                            left join prices p
                                            on p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts)
                                        union
                                            select sym,price,ts,price1,ts1  from (select t.sym, t.price, t.ts, p.price price1, p.ts as ts1
                                            from t
                                            join prices p
                                            on p.ts <= dateadd('m', -1, t.ts)) LATEST ON ts1 PARTITION BY ts, sym
                                        ) order by ts
                                )
                                order by ts, sym;
                                """,
                        sink);
            }
            assertQueryAndPlan(
                    sink,
                    "Async Window Join workers: 1\n" +
                            "  vectorized: true\n" +
                            "  window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding " + (includePrevailing ? "(include prevailing)\n" : "(exclude prevailing)\n") +
                            "  window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " following\n" +
                            "  master filter: price<300\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: trades\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: prices\n",
                    "select t.sym, t.price, t.ts, sum(p.price) as window_price, count() as cnt " +
                            "from trades t " +
                            "window join prices p " +
                            " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            " where t.price < 300 " +
                            "order by t.ts;",
                    "ts",
                    false,
                    false
            );
        });
    }

    @Test
    public void testWindowJoinOrderByDescAsyncFast() throws Exception {
        // Tests AsyncWindowJoinFastRecordCursorFactory with ORDER BY ts DESC
        // Factory selection: parallel enabled (default), symbol join condition, no LIMIT
        assertMemoryLeak(() -> {
            prepareTable();
            final String nanoZeros = ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "" : "000";
            final String prevailing = includePrevailing ? "include" : "exclude";

            if (!includePrevailing) {
                printSql(
                        "select t.sym, t.price, t.ts, sum(p.price) window_price " +
                                "from trades t " +
                                "left join prices p " +
                                "on (t.sym = p.sym) " +
                                " and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) " +
                                "order by t.ts desc;",
                        sink
                );
            } else {
                printSql(
                        """
                                select sym,price,ts, sum(price1) window_price from
                                (
                                        select * from (
                                            select t.sym, t.price, t.ts, p.price
                                            from trades t
                                            left join prices p
                                            on (t.sym = p.sym)
                                            and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts)
                                        union
                                            select sym,price,ts,price1  from (select t.sym, t.price, t.ts, p.price price1, p.ts as ts1
                                            from trades t
                                            join prices p
                                            on (t.sym = p.sym) and p.ts <= dateadd('m', -1, t.ts)) LATEST ON ts1 PARTITION BY ts, sym
                                        ) order by ts
                                )
                                order by ts desc;
                                """,
                        sink
                );
            }
            assertQueryAndPlan(
                    sink,
                    String.format("""
                                    Sort
                                      keys: [ts desc]
                                        Async Window Fast Join workers: 1
                                          vectorized: true
                                          symbol: sym=sym
                                          window lo: 60000000%1$s preceding (%2$s prevailing)
                                          window hi: 60000000%1$s following
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: trades
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: prices
                                    """,
                            nanoZeros,
                            prevailing
                    ),
                    String.format(
                            """
                                    SELECT t.sym, t.price, t.ts, sum(p.price) AS window_price
                                    FROM trades t
                                    WINDOW JOIN prices p
                                    ON (t.sym = p.sym)
                                      RANGE BETWEEN 1 MINUTE PRECEDING AND 1 MINUTE FOLLOWING %s PREVAILING
                                    ORDER BY t.ts DESC
                                    """,
                            prevailing
                    ),
                    "ts###DESC",
                    true,
                    false
            );
        });
    }

    @Test
    public void testWindowJoinOrderByDescAsyncNonFast() throws Exception {
        // Tests AsyncWindowJoinRecordCursorFactory with ORDER BY ts DESC
        // Factory selection: parallel enabled (default), no symbol join condition, no LIMIT
        assertMemoryLeak(() -> {
            prepareTable();
            final String nanoZeros = ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "" : "000";
            final String prevailing = includePrevailing ? "include" : "exclude";

            if (!includePrevailing) {
                printSql(
                        "select t.sym, t.price, t.ts, sum(p.price) window_price " +
                                "from trades t " +
                                "left join prices p " +
                                "on p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) " +
                                "order by t.ts desc;",
                        sink
                );
            } else {
                printSql(
                        """
                                select sym,price,ts, sum(price1) window_price from
                                (
                                        select * from (
                                            select t.sym, t.price, t.ts, p.price
                                            from trades t
                                            left join prices p
                                            on p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts)
                                        union
                                            select sym,price,ts,price1  from (select t.sym, t.price, t.ts, p.price price1, p.ts as ts1
                                            from trades t
                                            join prices p
                                            on p.ts <= dateadd('m', -1, t.ts)) LATEST ON ts1 PARTITION BY ts
                                        ) order by ts
                                )
                                order by ts desc;
                                """,
                        sink
                );
            }
            assertQueryAndPlan(
                    sink,
                    String.format(
                            """
                                    Sort
                                      keys: [ts desc, sym]
                                        Async Window Join workers: 1
                                          vectorized: true
                                          window lo: 60000000%1$s preceding (%2$s prevailing)
                                          window hi: 60000000%1$s following
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: trades
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: prices
                                    """,
                            nanoZeros,
                            prevailing
                    ),
                    String.format(
                            """
                                    SELECT t.sym, t.price, t.ts, sum(p.price) AS window_price
                                    FROM trades t
                                    WINDOW JOIN prices p
                                      RANGE BETWEEN 1 MINUTE PRECEDING AND 1 MINUTE FOLLOWING %s PREVAILING
                                    ORDER BY t.ts DESC, t.sym
                                    """,
                            prevailing
                    ),
                    "ts###DESC",
                    true,
                    false
            );
        });
    }

    @Test
    public void testWindowJoinOrderByDescNonAsyncFast() throws Exception {
        // Tests WindowJoinFastRecordCursorFactory with ORDER BY ts DESC
        // Factory selection: LIMIT on master forces non-async, symbol join condition
        assertMemoryLeak(() -> {
            prepareTable();
            final String nanoZeros = ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "" : "000";
            final String prevailing = includePrevailing ? "include" : "exclude";

            if (!includePrevailing) {
                printSql(
                        "select t.sym, t.price, t.ts, sum(p.price) window_price " +
                                "from (select * from trades limit 10) t " +
                                "left join prices p " +
                                "on (t.sym = p.sym) " +
                                " and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) " +
                                "order by t.ts desc;",
                        sink
                );
            } else {
                printSql(
                        """
                                with t as (select * from trades limit 10)
                                select sym,price,ts, sum(price1) window_price from
                                (
                                        select * from (
                                            select t.sym, t.price, t.ts, p.price
                                            from t
                                            left join prices p
                                            on (t.sym = p.sym)
                                            and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts)
                                        union
                                            select sym,price,ts,price1  from (select t.sym, t.price, t.ts, p.price price1, p.ts as ts1
                                            from t
                                            join prices p
                                            on (t.sym = p.sym) and p.ts <= dateadd('m', -1, t.ts)) LATEST ON ts1 PARTITION BY ts, sym
                                        ) order by ts
                                )
                                order by ts desc;
                                """,
                        sink
                );
            }
            assertQueryAndPlan(
                    sink,
                    String.format(
                            """
                                    Sort
                                      keys: [ts desc]
                                        Window Fast Join
                                          vectorized: true
                                          symbol: sym=sym
                                          window lo: 60000000%1$s preceding (%2$s prevailing)
                                          window hi: 60000000%1$s following
                                            Limit value: 10 skip-rows: 0 take-rows: 10
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: trades
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: prices
                                    """,
                            nanoZeros,
                            prevailing
                    ),
                    String.format(
                            """
                                    SELECT t.sym, t.price, t.ts, sum(p.price) AS window_price
                                    FROM (SELECT * FROM trades LIMIT 10) t
                                    WINDOW JOIN prices p
                                    ON (t.sym = p.sym)
                                      RANGE BETWEEN 1 MINUTE PRECEDING AND 1 MINUTE FOLLOWING %s PREVAILING
                                    ORDER BY t.ts DESC
                                    """,
                            prevailing
                    ),
                    "ts###DESC",
                    true,
                    true
            );
        });
    }

    @Test
    public void testWindowJoinOrderByDescNonAsyncNonFast() throws Exception {
        // Tests WindowJoinRecordCursorFactory with ORDER BY DESC
        // Factory selection: LIMIT on master forces non-async, expression-based join (no symbol match)
        assertMemoryLeak(() -> {
            prepareTable();
            final String nanoZeros = ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "" : "000";
            final String prevailing = includePrevailing ? "include" : "exclude";

            if (!includePrevailing) {
                printSql(
                        "select t.sym, t.price, t.ts, sum(p.price) window_price " +
                                "from (select * from trades limit 10) t " +
                                "left join prices p " +
                                "on concat(t.sym, '_0') = concat(p.sym, '_0') " +
                                " and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) " +
                                "order by t.ts desc;",
                        sink
                );
            } else {
                printSql(
                        """
                                with t as (select * from trades limit 10)
                                select sym,price,ts, sum(price1) window_price from
                                (
                                        select * from (
                                            select t.sym, t.price, t.ts, p.price
                                            from t
                                            left join prices p
                                            on concat(t.sym, '_0') = concat(p.sym, '_0')
                                            and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts)
                                        union
                                            select sym,price,ts,price1  from (select t.sym, t.price, t.ts, p.price price1, p.ts as ts1
                                            from t
                                            join prices p
                                            on concat(t.sym, '_0') = concat(p.sym, '_0') and p.ts <= dateadd('m', -1, t.ts)) LATEST ON ts1 PARTITION BY ts, sym
                                        ) order by ts
                                )
                                order by ts desc;
                                """,
                        sink
                );
            }
            assertQueryAndPlan(
                    sink,
                    String.format(
                            """
                                    Sort
                                      keys: [ts desc]
                                        Window Join
                                          window lo: 60000000%1$s preceding (%2$s prevailing)
                                          window hi: 60000000%1$s following
                                          join filter: concat([t.sym,'_0'])=concat([p.sym,'_0'])
                                            Limit value: 10 skip-rows: 0 take-rows: 10
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: trades
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: prices
                                    """,
                            nanoZeros,
                            prevailing
                    ),
                    String.format(
                            """
                                    SELECT t.sym, t.price, t.ts, sum(p.price) AS window_price
                                    FROM (SELECT * FROM trades LIMIT 10) t
                                    WINDOW JOIN prices p
                                    ON (concat(t.sym, '_0') = concat(p.sym, '_0'))
                                      RANGE BETWEEN 1 MINUTE PRECEDING AND 1 MINUTE FOLLOWING %s PREVAILING
                                    ORDER BY t.ts DESC
                                    """,
                            prevailing
                    ),
                    "ts###DESC",
                    true,
                    true
            );
        });
    }

    @Test
    public void testWindowJoinProjection() throws Exception {
        assertMemoryLeak(() -> {
            prepareTable();
            if (!includePrevailing) {
                printSql("select sum(p.price) + 2 window_price, t.price + 1, t.sym, t.ts " +
                        "from trades t " +
                        "left join prices p " +
                        "on (t.sym = p.sym) " +
                        " and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) " +
                        "order by t.ts, t.sym;", sink);
            } else {
                printSql("""
                                select sum(price1) + 2 window_price, price + 1, sym, ts from
                                (
                                        select * from (
                                            select t.sym, t.price, t.ts, p.price, p.ts pts
                                            from trades t
                                            left join prices p
                                            on (t.sym = p.sym)
                                            and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts)
                                        union
                                            select sym,price,ts,price1,ts1  from (select t.sym, t.price, t.ts, p.price price1, p.ts as ts1
                                            from trades t
                                            join prices p
                                            on (t.sym = p.sym) and p.ts <= dateadd('m', -1, t.ts)) LATEST ON ts1 PARTITION BY ts, sym
                                        ) order by ts
                                )
                                order by ts, sym;
                                """,
                        sink);
            }
            assertQueryAndPlan(
                    sink,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    VirtualRecord\n" +
                            "      functions: [sum+2,price+1,sym,ts]\n" +
                            "        Async Window Fast Join workers: 1\n" +
                            "          vectorized: true\n" +
                            "          symbol: sym=sym\n" +
                            "          window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding " + (includePrevailing ? "(include prevailing)\n" : "(exclude prevailing)\n") +
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
                            " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );

            if (!includePrevailing) {
                printSql("select sum(p.price) + 2 window_price, t.price + 1, t.sym, t.ts " +
                        "from trades t " +
                        "left join prices p " +
                        "on (t.sym != p.sym and p.price > 100) " +
                        " and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) " +
                        "order by t.ts, t.sym;", sink);
            } else {
                printSql("""
                                select sum(price1) + 2 window_price, price + 1, sym, ts from
                                (
                                        select * from (
                                            select t.sym, t.price, t.ts, p.price, p.ts pts
                                            from trades t
                                            left join prices p
                                            on (t.sym != p.sym and p.price > 100)
                                            and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts)
                                        union
                                            select sym,price,ts,price1,ts1  from (select t.sym, t.price, t.ts, p.price price1, p.ts as ts1
                                            from trades t
                                            join prices p
                                            on (t.sym != p.sym and p.price > 100) and p.ts <= dateadd('m', -1, t.ts)) LATEST ON ts1 PARTITION BY ts, sym
                                        ) order by ts
                                )
                                order by ts, sym;
                                """,
                        sink);
            }
            assertQueryAndPlan(
                    sink,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    VirtualRecord\n" +
                            "      functions: [sum+2,price+1,sym,ts]\n" +
                            "        Async Window Join workers: 1\n" +
                            "          vectorized: false\n" +
                            "          join filter: (t.sym!=p.sym and 100<p.price)\n" +
                            "          window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding " + (includePrevailing ? "(include prevailing)\n" : "(exclude prevailing)\n") +
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
                            " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testWindowJoinWithComplicityAggFunctions() throws Exception {
        // timestamp types don't matter for this test
        Assume.assumeTrue(leftTableTimestampType == TestTimestampType.MICRO);
        Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);

        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE trades (
                                ts TIMESTAMP,
                                sym SYMBOL,
                                price DOUBLE
                            ) timestamp(ts);
                            """
            );
            execute(
                    """
                            CREATE TABLE prices (
                                ts TIMESTAMP,
                                sym SYMBOL,
                                val0 DOUBLE,
                                val1 DOUBLE
                            ) timestamp(ts);
                            """
            );

            assertQueryAndPlan(
                    "ts\tsym\tprice\tagg0\tagg1\tagg2\tagg3\n",
                    """
                            Sort
                              keys: [ts, sym]
                                VirtualRecord
                                  functions: [ts,sym,price,agg0,agg1,agg2,agg1]
                                    Async Window Join workers: 1
                                      vectorized: true
                            """ +
                            "          window lo: 773 preceding" + (includePrevailing ? " (include prevailing)\n" : " (exclude prevailing)\n") +
                            """
                                              window hi: 773 following
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: trades
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: prices
                                    """,
                    """
                            SELECT t.ts, t.sym, t.price, first(val0) agg0, last(val1) agg1, sum(val1) agg2, last(val1) agg3
                            FROM trades t
                            WINDOW JOIN prices p
                            """ +
                            "RANGE BETWEEN 773 microseconds PRECEDING AND 773 microseconds FOLLOWING" + (includePrevailing ? " INCLUDE PREVAILING " : " EXCLUDE PREVAILING ") +
                            """
                                    ORDER BY t.ts, t.sym
                                    """,
                    "ts",
                    true,
                    false
            );

            assertQueryAndPlan(
                    "ts\tsym\tprice\tagg0\tagg1\tagg2\tagg3\n",
                    """
                            Sort
                              keys: [ts, sym]
                                VirtualRecord
                                  functions: [ts,sym,price,agg0,agg1,agg2,agg1]
                                    Async Window Join workers: 1
                                      vectorized: true
                            """ +
                            "          window lo: 773 preceding" + (includePrevailing ? " (include prevailing)\n" : " (exclude prevailing)\n") +
                            """
                                              window hi: 773 following
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: trades
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: prices
                                    """,
                    """
                            SELECT t.ts, t.sym, t.price, first(val0) agg0, last(val1) agg1, sum(val1) agg2, last(val1) agg3
                            FROM trades t
                            WINDOW JOIN prices p
                            """ +
                            " RANGE BETWEEN 773 microseconds PRECEDING AND 773 microseconds FOLLOWING" + (includePrevailing ? " INCLUDE PREVAILING " : " EXCLUDE PREVAILING ") +
                            """
                                    ORDER BY ts, sym;""",
                    "ts",
                    true,
                    false
            );

            assertQueryAndPlan(
                    "ts\tsym\tprice\tagg1\tagg2\n",
                    """
                            Sort
                              keys: [ts, sym]
                                VirtualRecord
                                  functions: [ts,sym,price,first-last,sum-last1]
                                    Async Window Join workers: 1
                                      vectorized: true
                            """ +
                            "          window lo: 773 preceding" + (includePrevailing ? " (include prevailing)\n" : " (exclude prevailing)\n") +
                            """
                                              window hi: 773 following
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: trades
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: prices
                                    """,
                    """
                            SELECT t.ts, t.sym, t.price, first(val0 + 1) - last(val1 + 1) agg1, sum(val1 + 2) - last(val1 + 2) agg2
                            FROM trades t
                            WINDOW JOIN prices p
                            """ +
                            "RANGE BETWEEN 773 microseconds PRECEDING AND 773 microseconds FOLLOWING" + (includePrevailing ? " INCLUDE PREVAILING " : " EXCLUDE PREVAILING ") +
                            """
                                    ORDER BY t.ts, t.sym""",
                    "ts",
                    true,
                    false
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
                    includePrevailing ?
                            """
                                    sym	price	ts	sum_price
                                    TSLA	400.0	2023-01-01T09:10:00.000000Z	800.0
                                    TSLA	401.0	2023-01-01T09:11:00.000000Z	400.5
                                    AMZN	500.0	2023-01-01T09:12:00.000000Z	1000.0
                                    """
                            : """
                            sym	price	ts	sum_price
                            TSLA	400.0	2023-01-01T09:10:00.000000Z	400.5
                            TSLA	401.0	2023-01-01T09:11:00.000000Z	null
                            AMZN	500.0	2023-01-01T09:12:00.000000Z	500.5
                            """,
                    "select t.sym, t.price, t.ts, sum(p.price) sum_price " +
                            "from (trades limit 10, 13) t " +
                            "window join prices p " +
                            "on (t.sym=p.sym) " +
                            " range between 1 second preceding and 1 second following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "where 42=42;",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    """
                            sym	price	ts	sum_price
                            """,
                    "select t.sym, t.price, t.ts, sum(p.price) sum_price " +
                            "from (trades limit 10, 13) t " +
                            "window join prices p " +
                            "on (t.sym=p.sym) " +
                            " range between 1 second preceding and 1 second following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
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
                    includePrevailing ?
                            """
                                    sym	price	ts	sum_price
                                    	null	2023-01-01T09:00:00.000000Z	null
                                    	null	2023-01-01T09:01:00.000000Z	null
                                    	null	2023-01-01T09:02:00.000000Z	null
                                    	null	2023-01-01T09:03:00.000000Z	null
                                    	null	2023-01-01T09:04:00.000000Z	null
                                    	null	2023-01-01T09:05:00.000000Z	null
                                    	null	2023-01-01T09:06:00.000000Z	null
                                    	null	2023-01-01T09:07:00.000000Z	null
                                    	null	2023-01-01T09:08:00.000000Z	null
                                    	null	2023-01-01T09:09:00.000000Z	399.5
                                    TSLA	400.0	2023-01-01T09:10:00.000000Z	800.0
                                    TSLA	401.0	2023-01-01T09:11:00.000000Z	900.0
                                    AMZN	500.0	2023-01-01T09:12:00.000000Z	1000.0
                                    """
                            : """
                            sym	price	ts	sum_price
                            	null	2023-01-01T09:00:00.000000Z	null
                            	null	2023-01-01T09:01:00.000000Z	null
                            	null	2023-01-01T09:02:00.000000Z	null
                            	null	2023-01-01T09:03:00.000000Z	null
                            	null	2023-01-01T09:04:00.000000Z	null
                            	null	2023-01-01T09:05:00.000000Z	null
                            	null	2023-01-01T09:06:00.000000Z	null
                            	null	2023-01-01T09:07:00.000000Z	null
                            	null	2023-01-01T09:08:00.000000Z	null
                            	null	2023-01-01T09:09:00.000000Z	399.5
                            TSLA	400.0	2023-01-01T09:10:00.000000Z	400.5
                            TSLA	401.0	2023-01-01T09:11:00.000000Z	499.5
                            AMZN	500.0	2023-01-01T09:12:00.000000Z	500.5
                            """,
                    "select t.sym, t.price, t.ts, sum(p.price) sum_price " +
                            "from (trades limit 13) t " +
                            "window join prices p " +
                            "on (42=42) " +
                            " range between 1 second preceding and 1 second following" + (includePrevailing ? " include prevailing;" : " exclude prevailing;"),
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
            if (!includePrevailing) {
                printSql("select t.sym, t.price, t.ts, sum(p.price) window_price " +
                        "from (trades limit 5) t " +
                        "left join prices p " +
                        "on (t.sym = p.sym) " +
                        " and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) " +
                        "order by t.ts, t.sym;", sink);
            } else {
                printSql("""
                                with t as (select * from trades limit 5)
                                select sym,price,ts, sum(price1) window_price from
                                (
                                        select * from (
                                            select t.sym, t.price, t.ts, p.price, p.ts pts
                                            from t
                                            left join prices p
                                            on t.sym = p.sym and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts)
                                        union
                                            select sym,price,ts,price1,ts1  from (select t.sym, t.price, t.ts, p.price price1, p.ts as ts1
                                            from t
                                            join prices p
                                            on t.sym = p.sym and p.ts <= dateadd('m', 1, t.ts)) LATEST ON ts1 PARTITION BY ts, sym
                                        ) order by ts
                                )
                                order by ts, sym;
                                """,
                        sink);
            }
            String nanoZeros = ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "" : "000";
            String whatToDoWithPrevailing = includePrevailing ? "include" : "exclude";
            assertQueryAndPlan(
                    sink,
                    String.format("""
                            Sort
                              keys: [ts, sym]
                                Window Fast Join
                                  vectorized: true
                                  symbol: sym=sym
                                  window lo: 60000000%1$s preceding (%2$s prevailing)
                                  window hi: 60000000%1$s following
                                    Limit value: 5 skip-rows: 0 take-rows: 5
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: trades
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: prices
                            """, nanoZeros, whatToDoWithPrevailing),
                    String.format("""
                                    SELECT t.sym, t.price, t.ts, sum(p.price) AS window_price
                                    FROM (trades LIMIT 5) t
                                    WINDOW JOIN prices p
                                    ON (t.sym = p.sym)
                                       RANGE BETWEEN 1 MINUTE PRECEDING AND 1 MINUTE FOLLOWING %s PREVAILING
                                    ORDER BY t.ts, t.sym
                                    """,
                            whatToDoWithPrevailing),
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
            // fast factory
            if (!includePrevailing) {
                printSql("select t.sym, t.price, t.ts, max(concat(p.price, '000')) f " +
                        "from (trades where ts > '2023-01-01T09:00:00Z' limit 1, 4) t " +
                        "left join prices p " +
                        "on (t.sym = p.sym) " +
                        " and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) " +
                        "order by t.ts, t.sym;", sink);
            } else {
                printSql("""
                                with t as (trades where ts > '2023-01-01T09:00:00Z' limit 1, 4)
                                select sym,price,ts, max(concat(price1, '000')) f from
                                (
                                        select * from (
                                            select t.sym, t.price, t.ts, p.price, p.ts pts
                                            from t
                                            left join prices p
                                            on t.sym = p.sym and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts)
                                        union
                                            select sym,price,ts,price1,ts1  from (select t.sym, t.price, t.ts, p.price price1, p.ts as ts1
                                            from t
                                            join prices p
                                            on t.sym = p.sym and p.ts <= dateadd('m', 1, t.ts)) LATEST ON ts1 PARTITION BY ts, sym
                                        ) order by ts
                                )
                                order by ts, sym;
                                """,
                        sink);
            }
            String whatToDoWithPrevailing = includePrevailing ? "include" : "exclude";
            String nanoZeros = ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "" : "000";
            boolean isLeftMicroTs = ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType());
            boolean isRightMicroTs = ColumnType.isTimestampMicro(rightTableTimestampType.getTimestampType());
            String planFragment = includePrevailing
                    ? "Frame forward scan on: prices"
                    : String.format("""
                            Interval forward scan on: prices
                                          intervals: [("2023-01-01T08:59:00.00000%sZ","MAX")]""",
                    isLeftMicroTs ? (isRightMicroTs ? "1" : "1000") : (isRightMicroTs ? "0" : "0001"));
            assertQueryAndPlan(
                    sink,
                    String.format("""
                            Sort
                              keys: [ts, sym]
                                Window Fast Join
                                  vectorized: false
                                  symbol: sym=sym
                                  window lo: 60000000%1$s preceding (%2$s prevailing)
                                  window hi: 60000000%1$s following
                                    Limit left: 1 right: 4 skip-rows-max: 1 take-rows-max: 3
                                        PageFrame
                                            Row forward scan
                                            Interval forward scan on: trades
                                              intervals: [("2023-01-01T09:00:00.%1$s000001Z","MAX")]
                                    PageFrame
                                        Row forward scan
                                        %3$s
                            """, nanoZeros, whatToDoWithPrevailing, planFragment),
                    String.format("""
                            SELECT t.sym, t.price, t.ts, max(concat(p.price, '000')) f
                            FROM (trades WHERE ts > '2023-01-01T09:00:00Z' LIMIT 1, 4) t
                            WINDOW JOIN prices p
                            ON (t.sym = p.sym)
                              RANGE BETWEEN 1 MINUTE PRECEDING AND 1 MINUTE FOLLOWING %s PREVAILING
                            ORDER BY t.ts, t.sym
                            """, whatToDoWithPrevailing),
                    "ts",
                    true,
                    false
            );

            // fast factory, vectorized
            if (!includePrevailing) {
                printSql("select t.sym, t.price, t.ts, sum(p.price) window_price, count(p.ts) as cnt " +
                        "from (trades where ts > '2023-01-01T09:00:00Z' limit 1, 4) t " +
                        "left join prices p " +
                        "on (t.sym = p.sym) " +
                        " and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) " +
                        "order by t.ts, t.sym;", sink);
            } else {
                printSql("""
                                with t as (trades where ts > '2023-01-01T09:00:00Z' limit 1, 4)
                                select sym,price,ts, sum(price1) window_price, count(pts) as cnt from
                                (
                                        select * from (
                                            select t.sym, t.price, t.ts, p.price, p.ts pts
                                            from t
                                            left join prices p
                                            on t.sym = p.sym and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts)
                                        union
                                            select sym,price,ts,price1,ts1  from (select t.sym, t.price, t.ts, p.price price1, p.ts as ts1
                                            from t
                                            join prices p
                                            on t.sym = p.sym and p.ts <= dateadd('m', 1, t.ts)) LATEST ON ts1 PARTITION BY ts, sym
                                        ) order by ts
                                )
                                order by ts, sym;
                                """,
                        sink);
            }
            assertQueryAndPlan(
                    sink,
                    String.format("""
                            Sort
                              keys: [ts, sym]
                                Window Fast Join
                                  vectorized: true
                                  symbol: sym=sym
                                  window lo: 60000000%1$s preceding (%2$s prevailing)
                                  window hi: 60000000%1$s following
                                    Limit left: 1 right: 4 skip-rows-max: 1 take-rows-max: 3
                                        PageFrame
                                            Row forward scan
                                            Interval forward scan on: trades
                                              intervals: [("2023-01-01T09:00:00.%1$s000001Z","MAX")]
                                    PageFrame
                                        Row forward scan
                                        %3$s
                            """, nanoZeros, whatToDoWithPrevailing, planFragment),
                    String.format("""
                            SELECT t.sym, t.price, t.ts, sum(p.price) AS window_price, count() AS cnt
                            FROM (trades WHERE ts > '2023-01-01T09:00:00Z' LIMIT 1, 4) t
                            WINDOW JOIN prices p
                            ON (t.sym = p.sym)
                              RANGE BETWEEN 1 MINUTE PRECEDING AND 1 MINUTE FOLLOWING %s PREVAILING
                            ORDER BY t.ts, t.sym
                            """, whatToDoWithPrevailing),
                    "ts",
                    true,
                    false
            );

            // non-fast factory
            assertQueryAndPlan(
                    sink,
                    String.format("""
                            Sort
                              keys: [ts, sym]
                                Window Join
                                  window lo: 60000000%1$s preceding (%2$s prevailing)
                                  window hi: 60000000%1$s following
                                  join filter: concat([t.sym,'_0'])=concat([p.sym,'_0'])
                                    Limit left: 1 right: 4 skip-rows-max: 1 take-rows-max: 3
                                        PageFrame
                                            Row forward scan
                                            Interval forward scan on: trades
                                              intervals: [("2023-01-01T09:00:00.00000%1$s1Z","MAX")]
                                    PageFrame
                                        Row forward scan
                                        %3$s
                            """, nanoZeros, whatToDoWithPrevailing, planFragment),
                    String.format("""
                            SELECT t.sym, t.price, t.ts, sum(p.price) AS window_price, count AS cnt
                            FROM (trades WHERE ts > '2023-01-01T09:00:00Z' LIMIT 1, 4) t
                            WINDOW JOIN prices p
                            ON (concat(t.sym, '_0') = concat(p.sym, '_0'))
                             RANGE BETWEEN 1 MINUTE PRECEDING AND 1 MINUTE FOLLOWING %s PREVAILING
                            ORDER BY t.ts, t.sym
                            """, whatToDoWithPrevailing),
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testWindowJoinWithPostFilter() throws Exception {
        // timestamp types don't matter for this test
        Assume.assumeTrue(leftTableTimestampType == TestTimestampType.MICRO);
        Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);
        assertMemoryLeak(() -> {
            prepareTable();
            assertExceptionNoLeakCheck(
                    "select t.ts, t.sym, sum(p.price) " +
                            "from trades t " +
                            "window join prices p " +
                            "on (t.sym = p.sym) " +
                            " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            " where p.price > 100;",
                    171,
                    "Invalid column: p.price"
            );

            assertQueryNoLeakCheck(
                    includePrevailing ?
                            """
                                    sym	price	sum_price
                                    TSLA	400.0	800.0
                                    TSLA	401.0	400.5
                                    """ :
                            """
                                    sym	price	sum_price
                                    TSLA	400.0	400.5
                                    TSLA	401.0	null
                                    """,
                    "select t.sym, t.price, sum(p.price) sum_price " +
                            "from (trades limit 12) t " +
                            "window join prices p " +
                            "on (t.sym=p.sym) " +
                            " range between 1 second preceding and 1 second following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "where 42 = 42 and t.price > 101;",
                    null,
                    false,
                    false
            );
        });
    }

    // Regression test for https://github.com/questdb/questdb/issues/6661
    // SIGSEGV when closing cursor while workers still access slave frame cache.
    @Test
    public void testWindowJoinWithPrevailingOnEmptyResultSetRegression() throws Exception {
        // The bug was in AsyncWindowJoinRecordCursor.close() freeing slaveTimeFrameAddressCache
        // before awaiting worker threads. With small page frames (4-8 rows), parallel execution
        // is triggered even with moderate data sizes.
        Assume.assumeTrue(leftTableTimestampType == TestTimestampType.MICRO);
        Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);

        assertMemoryLeak(() -> {
            // Create trades table (master)
            execute(
                    "CREATE TABLE trades (" +
                            "    symbol SYMBOL," +
                            "    side SYMBOL," +
                            "    price DOUBLE," +
                            "    amount DOUBLE," +
                            "    timestamp TIMESTAMP" +
                            ") timestamp(timestamp) PARTITION BY HOUR"
            );

            // Insert trades - enough rows to create multiple page frames for parallel execution
            execute(
                    "INSERT INTO trades SELECT " +
                            "    rnd_symbol(100, 4, 4, 0) AS symbol," +
                            "    rnd_symbol('buy', 'sell') as side," +
                            "    rnd_double() * 20 + 10 AS price," +
                            "    rnd_double() * 20 + 10 AS amount," +
                            "    timestamp_sequence('2025-01-01', 10000) as timestamp " +
                            "FROM long_sequence(1000)"
            );

            // Create prices table (slave)
            execute(
                    "CREATE TABLE prices (" +
                            "    ts TIMESTAMP," +
                            "    sym SYMBOL," +
                            "    bid DOUBLE," +
                            "    ask DOUBLE" +
                            ") timestamp(ts) PARTITION BY HOUR BYPASS WAL"
            );

            // Insert prices - enough to keep workers busy during close()
            execute(
                    "INSERT INTO prices " +
                            "SELECT " +
                            "    timestamp_sequence('2024-12-31T23', 1000) as ts," +
                            "    rnd_symbol(100, 4, 4, 0)," +
                            "    rnd_double() * 10.0 + 5.0," +
                            "    rnd_double() * 10.0 + 5.0 " +
                            "FROM long_sequence(100000)"
            );

            // Query exercises the window join with parallel execution
            assertSql(
                    """
                            symbol\tside\tprice\tamount\ttimestamp\tavg_bid\tavg_ask
                            IBBT\tsell\t27.62658038426882\t25.755174211876263\t2025-01-01T00:00:00.000000Z\tnull\tnull
                            SRGO\tsell\t23.775851060898002\t10.159709099174506\t2025-01-01T00:00:00.010000Z\tnull\tnull
                            SSMP\tbuy\t11.565604136302905\t16.244392764407145\t2025-01-01T00:00:00.020000Z\tnull\tnull
                            XKUI\tsell\t10.048915397521615\t13.947353449965911\t2025-01-01T00:00:00.030000Z\tnull\tnull
                            HMLL\tsell\t14.021324739768806\t16.009749043773716\t2025-01-01T00:00:00.040000Z\tnull\tnull
                            FLPB\tbuy\t19.839800343262496\t27.863620707156855\t2025-01-01T00:00:00.050000Z\tnull\tnull
                            VLJU\tsell\t15.83019601640128\t29.917959191564314\t2025-01-01T00:00:00.060000Z\tnull\tnull
                            OUOJ\tsell\t24.934027336260215\t26.80592941625909\t2025-01-01T00:00:00.070000Z\tnull\tnull
                            IBBT\tsell\t20.61751353375695\t21.524088094210946\t2025-01-01T00:00:00.080000Z\tnull\tnull
                            CTGQ\tsell\t13.49968514450758\t11.026703113256238\t2025-01-01T00:00:00.090000Z\tnull\tnull
                            OUOJ\tbuy\t16.843632751428714\t15.84854969504557\t2025-01-01T00:00:00.100000Z\tnull\tnull
                            LTJC\tsell\t29.22396570248674\t29.09283466161919\t2025-01-01T00:00:00.110000Z\tnull\tnull
                            GSHO\tsell\t25.88504507239604\t28.117800596816146\t2025-01-01T00:00:00.120000Z\tnull\tnull
                            DOTS\tsell\t16.41868117763792\t24.86294443626393\t2025-01-01T00:00:00.130000Z\tnull\tnull
                            DYOP\tsell\t26.435305077197874\t15.413707089338455\t2025-01-01T00:00:00.140000Z\tnull\tnull
                            YSBE\tsell\t10.2792159091968\t27.31259131836934\t2025-01-01T00:00:00.150000Z\tnull\tnull
                            ZIMN\tsell\t13.384768613590621\t17.457309579815103\t2025-01-01T00:00:00.160000Z\tnull\tnull
                            SHRU\tsell\t16.3235720755334\t17.701813396489623\t2025-01-01T00:00:00.170000Z\tnull\tnull
                            HYHB\tbuy\t28.86493132935254\t22.759984186895146\t2025-01-01T00:00:00.180000Z\tnull\tnull
                            RGII\tbuy\t19.221926182810602\t18.537842800419824\t2025-01-01T00:00:00.190000Z\tnull\tnull
                            CXZO\tbuy\t10.591679371443165\t21.557635704613368\t2025-01-01T00:00:00.200000Z\tnull\tnull
                            OXPK\tbuy\t28.96576151757136\t20.050827613754144\t2025-01-01T00:00:00.210000Z\tnull\tnull
                            LTJC\tsell\t26.92242339501047\t20.899941634158836\t2025-01-01T00:00:00.220000Z\tnull\tnull
                            DEYY\tsell\t27.533817292847473\t16.116016640182213\t2025-01-01T00:00:00.230000Z\tnull\tnull
                            TJRS\tbuy\t22.959234881347033\t22.10100638570894\t2025-01-01T00:00:00.240000Z\tnull\tnull
                            CTGQ\tsell\t12.921049998677834\t25.435105535889953\t2025-01-01T00:00:00.250000Z\tnull\tnull
                            EDYY\tbuy\t16.30669914546051\t10.06503983223096\t2025-01-01T00:00:00.260000Z\tnull\tnull
                            RXGZ\tsell\t11.786917367439454\t14.001364901858706\t2025-01-01T00:00:00.270000Z\tnull\tnull
                            ZVQE\tsell\t27.791831657324227\t21.39546636906466\t2025-01-01T00:00:00.280000Z\tnull\tnull
                            WCKY\tsell\t13.198423008539908\t13.339624263396804\t2025-01-01T00:00:00.290000Z\tnull\tnull
                            TKVV\tsell\t13.688551244044207\t24.597321155459404\t2025-01-01T00:00:00.300000Z\tnull\tnull
                            LGMX\tsell\t15.941031673027107\t11.20557575211648\t2025-01-01T00:00:00.310000Z\tnull\tnull
                            EPIH\tbuy\t24.17402366454753\t12.73092931821326\t2025-01-01T00:00:00.320000Z\tnull\tnull
                            YFFD\tbuy\t10.880800171783589\t18.085020227121333\t2025-01-01T00:00:00.330000Z\tnull\tnull
                            NWIF\tsell\t20.729207504030697\t26.766120445035824\t2025-01-01T00:00:00.340000Z\tnull\tnull
                            OOZZ\tbuy\t24.795632981855434\t12.15981147992586\t2025-01-01T00:00:00.350000Z\tnull\tnull
                            VTJW\tbuy\t29.12355658415776\t10.22198531343937\t2025-01-01T00:00:00.360000Z\tnull\tnull
                            RFBV\tsell\t22.00243118985623\t24.48473748327055\t2025-01-01T00:00:00.370000Z\tnull\tnull
                            ROMN\tsell\t20.47740062300111\t20.671907152614516\t2025-01-01T00:00:00.380000Z\tnull\tnull
                            FFYU\tbuy\t17.667129080102733\t27.707351259388567\t2025-01-01T00:00:00.390000Z\tnull\tnull
                            SSMP\tsell\t24.67675977610084\t10.033065601247618\t2025-01-01T00:00:00.400000Z\tnull\tnull
                            MSSU\tbuy\t29.953792861511868\t16.978557147036504\t2025-01-01T00:00:00.410000Z\tnull\tnull
                            CTGQ\tsell\t11.476692834981783\t24.030822777492983\t2025-01-01T00:00:00.420000Z\tnull\tnull
                            EDYY\tsell\t27.796452822328803\t23.881835850296664\t2025-01-01T00:00:00.430000Z\tnull\tnull
                            RFBV\tbuy\t19.129335075801645\t12.257218521256053\t2025-01-01T00:00:00.440000Z\tnull\tnull
                            HRIP\tsell\t25.566703507780534\t20.67048768117076\t2025-01-01T00:00:00.450000Z\tnull\tnull
                            ZSRY\tbuy\t25.454640764755734\t16.248916021224627\t2025-01-01T00:00:00.460000Z\tnull\tnull
                            HYHB\tsell\t25.886371535000865\t18.84310317447792\t2025-01-01T00:00:00.470000Z\tnull\tnull
                            HYHB\tbuy\t17.30854044094422\t11.534495828195148\t2025-01-01T00:00:00.480000Z\tnull\tnull
                            QULO\tsell\t26.818160509651435\t13.024224060779295\t2025-01-01T00:00:00.490000Z\tnull\tnull
                            VDZJ\tbuy\t13.167957482341812\t23.085119757130766\t2025-01-01T00:00:00.500000Z\tnull\tnull
                            LPDX\tbuy\t13.212893502033927\t20.129161502324173\t2025-01-01T00:00:00.510000Z\tnull\tnull
                            LPDX\tbuy\t15.728020543309029\t19.826684208375337\t2025-01-01T00:00:00.520000Z\tnull\tnull
                            HRIP\tsell\t18.082032632105324\t10.407817683892535\t2025-01-01T00:00:00.530000Z\tnull\tnull
                            ZLUO\tsell\t23.888298107508575\t21.953229092823626\t2025-01-01T00:00:00.540000Z\tnull\tnull
                            NWIF\tsell\t19.52077225629195\t11.134476656172474\t2025-01-01T00:00:00.550000Z\tnull\tnull
                            ELLK\tbuy\t14.184340811274318\t15.094057822706276\t2025-01-01T00:00:00.560000Z\tnull\tnull
                            DGLO\tsell\t27.738795234919074\t11.621840472934776\t2025-01-01T00:00:00.570000Z\tnull\tnull
                            SLUQ\tbuy\t22.491335936372202\t16.485053950897814\t2025-01-01T00:00:00.580000Z\tnull\tnull
                            LNVT\tsell\t23.98781919191839\t22.137131832694806\t2025-01-01T00:00:00.590000Z\tnull\tnull
                            RXGZ\tsell\t26.29758525834465\t10.423559548894774\t2025-01-01T00:00:00.600000Z\tnull\tnull
                            TMHG\tbuy\t22.723475346083802\t19.901230470039927\t2025-01-01T00:00:00.610000Z\tnull\tnull
                            XKUI\tsell\t25.272695529329088\t20.789125031105968\t2025-01-01T00:00:00.620000Z\tnull\tnull
                            FLRB\tbuy\t15.026396384114975\t21.647820237948338\t2025-01-01T00:00:00.630000Z\tnull\tnull
                            IPHZ\tsell\t24.519935543823234\t14.916923042764703\t2025-01-01T00:00:00.640000Z\tnull\tnull
                            CTGQ\tsell\t17.84229695014247\t28.479029586850537\t2025-01-01T00:00:00.650000Z\tnull\tnull
                            ZSRY\tsell\t25.75285961005529\t14.176304090055979\t2025-01-01T00:00:00.660000Z\tnull\tnull
                            DSWU\tsell\t15.425868155389564\t15.603843765010279\t2025-01-01T00:00:00.670000Z\tnull\tnull
                            CTGQ\tsell\t12.35375275769121\t11.065736133015472\t2025-01-01T00:00:00.680000Z\tnull\tnull
                            ZFKW\tbuy\t25.201101771231546\t23.768298047455954\t2025-01-01T00:00:00.690000Z\tnull\tnull
                            HNIM\tsell\t19.42157626936415\t12.705119479615949\t2025-01-01T00:00:00.700000Z\tnull\tnull
                            MYIC\tsell\t22.493764775978917\t19.71468597797314\t2025-01-01T00:00:00.710000Z\tnull\tnull
                            GXHF\tsell\t27.775586730199752\t21.832838972867037\t2025-01-01T00:00:00.720000Z\tnull\tnull
                            LTJC\tsell\t21.725613069659403\t26.5554305057099\t2025-01-01T00:00:00.730000Z\tnull\tnull
                            GYVF\tsell\t18.092419400178116\t17.60003057461975\t2025-01-01T00:00:00.740000Z\tnull\tnull
                            OQMY\tbuy\t21.85982392034898\t13.26416715258983\t2025-01-01T00:00:00.750000Z\tnull\tnull
                            GLUO\tbuy\t14.657105656174414\t14.424549589606041\t2025-01-01T00:00:00.760000Z\tnull\tnull
                            SRGO\tbuy\t20.719868855416763\t19.693053535393567\t2025-01-01T00:00:00.770000Z\tnull\tnull
                            TJRS\tsell\t14.734617548001221\t16.456405634856537\t2025-01-01T00:00:00.780000Z\tnull\tnull
                            HFOW\tsell\t15.392189805885586\t21.571290760949427\t2025-01-01T00:00:00.790000Z\tnull\tnull
                            VTJW\tbuy\t12.64531233173071\t24.779545760438296\t2025-01-01T00:00:00.800000Z\tnull\tnull
                            FMBE\tbuy\t12.543125456431344\t17.731290506506138\t2025-01-01T00:00:00.810000Z\tnull\tnull
                            VTJW\tsell\t29.532569717902796\t11.232934355723161\t2025-01-01T00:00:00.820000Z\tnull\tnull
                            HFOW\tbuy\t27.844068772068546\t23.87333982916651\t2025-01-01T00:00:00.830000Z\tnull\tnull
                            XKUI\tbuy\t21.038381932392795\t29.348705762370983\t2025-01-01T00:00:00.840000Z\tnull\tnull
                            ZZRM\tbuy\t12.638088085987135\t16.65230824310371\t2025-01-01T00:00:00.850000Z\tnull\tnull
                            FJGE\tsell\t28.043043693990846\t16.15244013383554\t2025-01-01T00:00:00.860000Z\tnull\tnull
                            GSHO\tbuy\t25.175720049547856\t10.225270236798849\t2025-01-01T00:00:00.870000Z\tnull\tnull
                            DGLO\tbuy\t16.787019028000493\t17.96374488715091\t2025-01-01T00:00:00.880000Z\tnull\tnull
                            EDYY\tbuy\t15.648153791985521\t26.109490964090764\t2025-01-01T00:00:00.890000Z\tnull\tnull
                            GIFO\tbuy\t27.17935642395738\t27.285600063219317\t2025-01-01T00:00:00.900000Z\tnull\tnull
                            OQMY\tsell\t10.526727955566603\t27.317233833129286\t2025-01-01T00:00:00.910000Z\tnull\tnull
                            HYHB\tbuy\t10.946844747851184\t22.078349323344476\t2025-01-01T00:00:00.920000Z\tnull\tnull
                            ZSQL\tbuy\t28.53385914328215\t21.02368330902948\t2025-01-01T00:00:00.930000Z\tnull\tnull
                            OLYX\tbuy\t15.30448399238092\t26.809100994112733\t2025-01-01T00:00:00.940000Z\tnull\tnull
                            ROMN\tsell\t21.527383568112796\t20.700330943529384\t2025-01-01T00:00:00.950000Z\tnull\tnull
                            LTJC\tbuy\t11.12198693638817\t13.962977138980627\t2025-01-01T00:00:00.960000Z\tnull\tnull
                            HNZH\tbuy\t28.820793409876465\t13.814646966480208\t2025-01-01T00:00:00.970000Z\tnull\tnull
                            IPHZ\tbuy\t10.37439621579069\t16.06275240804409\t2025-01-01T00:00:00.980000Z\tnull\tnull
                            OXPK\tbuy\t25.128429718796674\t27.43678869894423\t2025-01-01T00:00:00.990000Z\tnull\tnull
                            """,
                    "SELECT t.*, avg(p.bid) avg_bid, avg(p.ask) avg_ask " +
                            "FROM trades t " +
                            "WINDOW JOIN prices p ON p.sym = t.symbol " +
                            "RANGE BETWEEN 1 second PRECEDING and 1 second FOLLOWING LIMIT 100");
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
                            " range between 1 second preceding and 1 second following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
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
            if (!includePrevailing) {
                printSql("select t.sym, t.price, t.ts, sum(p.price) window_price " +
                        "from trades t " +
                        "left join prices p " +
                        "on (0 = 1) " +
                        " and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) " +
                        "order by t.ts, t.sym;", sink);
            } else {
                printSql("""
                                select sym,price,ts, sum(price1) window_price from
                                (
                                        select * from (
                                            select t.sym, t.price, t.ts, p.price, p.ts pts
                                            from trades t
                                            left join prices p
                                            on (0 = 1) and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts)
                                        union
                                            select sym,price,ts,price1,ts1  from (select t.sym, t.price, t.ts, p.price price1, p.ts as ts1
                                            from trades t
                                            join prices p
                                            on (0 = 1) and p.ts <= dateadd('m', -1, t.ts)) LATEST ON ts1 PARTITION BY ts, sym
                                        ) order by ts
                                )
                                order by ts, sym;
                                """,
                        sink);
            }
            assertQueryAndPlan(
                    sink,
                    """
                            Sort light
                              keys: [ts, sym]
                                ExtraNullColumnRecord
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """,
                    "select t.sym, t.price, t.ts, sum(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on (0 = 1) " +
                            " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    true
            );

            if (!includePrevailing) {
                printSql("select  sum(p.price), t.price t_price, avg(p.price), t.sym, t.ts " +
                        "from trades t " +
                        "left join prices p " +
                        "on (0 = 1) " +
                        " and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) " +
                        "order by t.sym;", sink);
            } else {
                printSql("""
                                select sum(price1), price t_price, avg(price1), sym, ts from
                                (
                                        select * from (
                                            select t.sym, t.price, t.ts, p.price, p.ts pts
                                            from trades t
                                            left join prices p
                                            on (0 = 1) and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts)
                                        union
                                            select sym,price,ts,price1,ts1  from (select t.sym, t.price, t.ts, p.price price1, p.ts as ts1
                                            from trades t
                                            join prices p
                                            on (0 = 1) and p.ts <= dateadd('m', -1, t.ts)) LATEST ON ts1 PARTITION BY ts, sym
                                        ) order by ts
                                )
                                order by sym;
                                """,
                        sink);
            }
            assertQueryAndPlan(
                    sink,
                    """
                            Sort light
                              keys: [sym]
                                SelectedRecord
                                    ExtraNullColumnRecord
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: trades
                            """,
                    "select sum(p.price), t.price t_price, avg(p.price), t.sym, t.ts " +
                            "from trades t " +
                            "window join prices p " +
                            "on (0 = 1) " +
                            " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
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
            if (!includePrevailing) {
                printSql("select t.sym, t.price, t.ts, avg(p.price) window_price " +
                        "from trades t " +
                        "left join prices p " +
                        "on (t.sym = p.sym) " +
                        " and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) " +
                        "order by t.ts limit 3;", sink);
            } else {
                printSql("""
                                select sym,price,ts, avg(price1) window_price from
                                (
                                    select * from (
                                        select t.sym, t.price, t.ts, p.price, p.ts pts
                                        from trades t
                                        left join prices p
                                        on (t.sym = p.sym) and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts)
                                    union
                                        select sym,price,ts,price1,ts1  from (select t.sym, t.price, t.ts, p.price price1, p.ts as ts1
                                        from trades t
                                        join prices p
                                        on (t.sym = p.sym) and p.ts <= dateadd('m', -1, t.ts)) LATEST ON ts1 PARTITION BY ts, sym
                                    ) order by ts
                                )
                                order by ts limit 3;
                                """,
                        sink);
            }
            String nanoZeros = ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "" : "000";
            String whatToDoWithPrevailing = includePrevailing ? "include" : "exclude";
            assertQueryAndPlan(
                    sink,
                    String.format("""
                            Limit value: 3 skip-rows-max: 0 take-rows-max: 3
                                Async Window Fast Join workers: 1
                                  vectorized: true
                                  symbol: sym=sym
                                  window lo: 60000000%1$s preceding (%2$s prevailing)
                                  window hi: 60000000%1$s following
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: prices
                            """, nanoZeros, whatToDoWithPrevailing),
                    String.format("""
                                    SELECT t.sym, t.price, t.ts, avg(p.price) AS window_price
                                    FROM trades t
                                    WINDOW JOIN prices p
                                    ON (t.sym = p.sym)
                                        RANGE BETWEEN 1 MINUTE PRECEDING AND 1 MINUTE FOLLOWING %s PREVAILING
                                    LIMIT 3
                                    """,
                            whatToDoWithPrevailing),
                    "ts",
                    false,
                    false
            );
        });
    }

    @Test
    public void testWithOnlyAggregateLeftTableColumn() throws Exception {
        assertMemoryLeak(() -> {
            prepareTable();
            if (!includePrevailing) {
                printSql("select t.sym, t.price, t.ts, sum(t.price) window_price " +
                        "from trades t " +
                        "left join prices p " +
                        "on (t.sym = p.sym) " +
                        " and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) " +
                        "order by t.ts, t.sym;", sink);
            } else {
                printSql("""
                                select sym,price,ts, sum(price) window_price from
                                (
                                        select * from (
                                            select t.sym, t.price, t.ts, p.price, p.ts pts
                                            from trades t
                                            left join prices p
                                            on (t.sym = p.sym) and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts)
                                        union
                                            select sym,price,ts,price1,ts1  from (select t.sym, t.price, t.ts, p.price price1, p.ts as ts1
                                            from trades t
                                            join prices p
                                            on (t.sym = p.sym) and p.ts <= dateadd('m', -1, t.ts)) LATEST ON ts1 PARTITION BY ts, sym
                                        ) order by ts
                                )
                                order by ts, sym;
                                """,
                        sink);
            }
            assertQueryAndPlan(
                    sink,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Async Window Fast Join workers: 1\n" +
                            "      vectorized: false\n" +
                            "      symbol: sym=sym\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding " + (includePrevailing ? "(include prevailing)\n" : "(exclude prevailing)\n") +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " following\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: trades\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: prices\n",
                    "select t.sym, t.price, t.ts, sum(t.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on (t.sym = p.sym) " +
                            " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testWithSymbolEqualConditionInSameTable() throws Exception {
        // timestamp types don't matter for this test
        Assume.assumeTrue(leftTableTimestampType == TestTimestampType.MICRO);
        Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);
        assertMemoryLeak(() -> {
            execute(
                    "create table x (" +
                            "  s symbol," +
                            "  s1 symbol," +
                            "  ts timestamp" +
                            ") timestamp(ts) partition by day;"
            );
            execute(
                    "create table y (" +
                            "  s symbol," +
                            "  s1 symbol," +
                            "  ts timestamp" +
                            ") timestamp(ts) partition by day;"
            );

            execute(
                    "insert into x values " +
                            "('sym0', 'sym1', '2023-01-01T09:00:00.000000Z'), ('sym2', 'sym2', '2023-01-01T09:00:00.000000Z');"
            );
            execute(
                    "insert into y values " +
                            "('sym0', 'sym0', '2023-01-01T08:59:58.000000Z')," +
                            "('sym1', 'sym1', '2023-01-01T08:59:59.000000Z')," +
                            "('sym2', 'sym2', '2023-01-01T09:00:00.000000Z')," +
                            "('sym3', 'sym33', '2023-01-01T09:00:01.000000Z')," +
                            "('sym4', 'sym44', '2023-01-01T09:00:02.000000Z');"
            );

            assertQueryAndPlan(
                    """
                            s	s1	ts	count
                            sym0	sym1	2023-01-01T09:00:00.000000Z	0
                            sym2	sym2	2023-01-01T09:00:00.000000Z	1
                            """,
                    """
                            Sort
                              keys: [ts, s]
                                Async Window Fast Join workers: 1
                                  vectorized: false
                                  symbol: s=s
                                  join filter: x.s=x.s1
                            """ +
                            "      window lo: 1000000 preceding" + (includePrevailing ? " (include prevailing)\n" : " (exclude prevailing)\n")
                            +
                            """
                                          window hi: 1000000 following
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: x
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: y
                                    """,
                    "select x.*, count() " +
                            "from x " +
                            "window join y " +
                            "on (x.s = x.s1 and x.s = y.s) " +
                            " range between 1 second preceding and 1 second following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by x.ts, x.s;",
                    "ts",
                    true,
                    false
            );

            assertQueryAndPlan(
                    includePrevailing ? """
                            s	s1	ts	count
                            sym0	sym1	2023-01-01T09:00:00.000000Z	1
                            sym2	sym2	2023-01-01T09:00:00.000000Z	1
                            """
                            :
                            """
                                    s	s1	ts	count
                                    sym0	sym1	2023-01-01T09:00:00.000000Z	0
                                    sym2	sym2	2023-01-01T09:00:00.000000Z	1
                                    """,
                    """
                            Sort
                              keys: [ts, s]
                                Async Window Fast Join workers: 1
                                  vectorized: false
                                  symbol: s=s1
                                  join filter: y.s=y.s1
                            """ +
                            "      window lo: 1000000 preceding" + (includePrevailing ? " (include prevailing)\n" : " (exclude prevailing)\n")
                            +
                            """
                                          window hi: 1000000 following
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: x
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: y
                                    """,
                    "select x.*, count() " +
                            "from x " +
                            "window join y " +
                            "on (x.s = y.s1 and y.s = y.s1) " +
                            " range between 1 second preceding and 1 second following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by x.ts, x.s;",
                    "ts",
                    true,
                    false
            );

            assertQueryAndPlan(
                    """
                            s	s1	ts	count
                            sym0	sym1	2023-01-01T09:00:00.000000Z	0
                            sym2	sym2	2023-01-01T09:00:00.000000Z	1
                            """,
                    """
                            Sort
                              keys: [ts, s]
                                Async Window Fast Join workers: 1
                                  vectorized: false
                                  symbol: s=s1
                                  join filter: (x.s1=y.s1 and x.s1=y.s and x.s=y.s)
                            """ +
                            "      window lo: 1000000 preceding" + (includePrevailing ? " (include prevailing)\n" : " (exclude prevailing)\n")
                            +
                            """
                                          window hi: 1000000 following
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: x
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: y
                                    """,
                    "select x.*, count() " +
                            "from x " +
                            "window join y " +
                            "on (x.s = y.s1 and x.s1 = y.s1 and x.s1 = y.s and x.s = y.s) " +
                            " range between 1 second preceding and 1 second following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by x.ts, x.s;",
                    "ts",
                    true,
                    false
            );
        });
    }

    private void assertSkipToAndCalculateSize(String select, int size) throws Exception {
        assertQueryNoLeakCheck("count\n" + size + "\n", "select count(*) from (" + select + ")", null, false, true);
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

    @Test
    public void testWindowJoinSelfJoinWithAggregatesInSelectAndWhere() throws Exception {
        // Reproducer for: https://demo.questdb.io error "Invalid column: price" at position 0
        // Query: SELECT t.timestamp, t.order_id, t.symbol, t.side, t.price AS fill_price,
        //        sum(w.price * w.quantity) / sum(w.quantity) AS vwap_5m, ...
        //        FROM fx_trades t WINDOW JOIN fx_trades w ON (t.symbol = w.symbol)
        //        RANGE BETWEEN 5 minutes PRECEDING AND 1 microseconds PRECEDING EXCLUDE PREVAILING
        //        WHERE t.symbol = 'EURUSD' ORDER BY t.timestamp LIMIT 100
        assertMemoryLeak(() -> {
            execute("CREATE TABLE fx_trades (" +
                    "timestamp TIMESTAMP, " +
                    "symbol SYMBOL, " +
                    "side SYMBOL, " +
                    "price DOUBLE, " +
                    "quantity DOUBLE, " +
                    "order_id UUID" +
                    ") TIMESTAMP(timestamp) PARTITION BY DAY WAL");
            execute("INSERT INTO fx_trades VALUES " +
                    "('2025-01-01T00:00:00.000000Z', 'EURUSD', 'buy', 1.05, 1000, rnd_uuid4())," +
                    "('2025-01-01T00:01:00.000000Z', 'EURUSD', 'sell', 1.051, 500, rnd_uuid4())," +
                    "('2025-01-01T00:02:00.000000Z', 'EURUSD', 'buy', 1.052, 750, rnd_uuid4())," +
                    "('2025-01-01T00:03:00.000000Z', 'GBPUSD', 'buy', 1.25, 1000, rnd_uuid4())," +
                    "('2025-01-01T00:04:00.000000Z', 'EURUSD', 'sell', 1.053, 250, rnd_uuid4())," +
                    "('2025-01-01T00:05:00.000000Z', 'EURUSD', 'buy', 1.054, 600, rnd_uuid4())");
            drainWalQueue();

            // Self-join with aggregates and WHERE clause - this was reproducing the "Invalid column: price" error
            assertQueryNoLeakCheck(
                    """
                            timestamp\torder_id\tsymbol\tside\tfill_price\tvwap_5m\tslippage_bps
                            2025-01-01T00:00:00.000000Z\t0010cde8-12ce-40ee-8010-a928bb8b9650\tEURUSD\tbuy\t1.05\tnull\tnull
                            2025-01-01T00:01:00.000000Z\t9f9b2131-d49f-4d1d-ab81-39815c50d341\tEURUSD\tsell\t1.051\t1.05\t9.523809523808474
                            2025-01-01T00:02:00.000000Z\t7bcd48d8-c77a-4655-b2a2-15ba0462ad15\tEURUSD\tbuy\t1.052\t1.0503333333333333\t15.867978419549715
                            2025-01-01T00:04:00.000000Z\te8beef38-cd7b-43d8-9b2d-34586f6275fa\tEURUSD\tsell\t1.053\t1.050888888888889\t20.088813702684046
                            2025-01-01T00:05:00.000000Z\t322a2198-864b-4b14-b97f-a69eb8fec6cc\tEURUSD\tbuy\t1.054\t1.0511\t27.590143659025067
                            """,
                    "SELECT " +
                            "t.timestamp, " +
                            "t.order_id, " +
                            "t.symbol, " +
                            "t.side, " +
                            "t.price AS fill_price, " +
                            "sum(w.price * w.quantity) / sum(w.quantity) AS vwap_5m, " +
                            "(t.price - sum(w.price * w.quantity) / sum(w.quantity)) " +
                            "    / (sum(w.price * w.quantity) / sum(w.quantity)) * 10000 AS slippage_bps " +
                            "FROM fx_trades t " +
                            "WINDOW JOIN fx_trades w " +
                            "    ON (t.symbol = w.symbol) " +
                            "    RANGE BETWEEN 5 minutes PRECEDING AND 1 microseconds PRECEDING " +
                            "    EXCLUDE PREVAILING " +
                            "WHERE t.symbol = 'EURUSD' " +
                            "ORDER BY t.timestamp " +
                            "LIMIT 100",
                    "timestamp",
                    false,
                    false
            );
        });
    }

    private void prepareTable() throws SqlException {
        executeWithRewriteTimestamp(
                "create table trades (" +
                        "  ts #TIMESTAMP" +
                        ") timestamp(ts) partition by day;",
                leftTableTimestampType.getTypeName()
        );
        executeWithRewriteTimestamp(
                "create table prices (" +
                        "  ts #TIMESTAMP" +
                        ") timestamp(ts) partition by day;",
                rightTableTimestampType.getTypeName()
        );

        executeWithRewriteTimestamp(
                "insert into trades values " +
                        "(cast('2023-01-01T09:00:00.000000Z' as #TIMESTAMP))," +
                        "(cast('2023-01-01T09:01:00.000000Z' as #TIMESTAMP))," +
                        "(cast('2023-01-01T09:02:00.000000Z' as #TIMESTAMP))," +
                        "(cast('2023-01-01T09:03:00.000000Z' as #TIMESTAMP))," +
                        "(cast('2023-01-01T09:04:00.000000Z' as #TIMESTAMP))," +
                        "(cast('2023-01-01T09:05:00.000000Z' as #TIMESTAMP))," +
                        "(cast('2023-01-01T09:06:00.000000Z' as #TIMESTAMP))," +
                        "(cast('2023-01-01T09:07:00.000000Z' as #TIMESTAMP))," +
                        "(cast('2023-01-01T09:08:00.000000Z' as #TIMESTAMP))," +
                        "(cast('2023-01-01T09:09:00.000000Z' as #TIMESTAMP));",
                leftTableTimestampType.getTypeName()
        );

        executeWithRewriteTimestamp(
                "insert into prices values " +
                        "(cast('2023-01-01T08:59:00.000000Z' as #TIMESTAMP))," +
                        "(cast('2023-01-01T09:00:00.000000Z' as #TIMESTAMP))," +
                        "(cast('2023-01-01T09:01:00.000000Z' as #TIMESTAMP))," +
                        "(cast('2023-01-01T09:02:00.000000Z' as #TIMESTAMP))," +
                        "(cast('2023-01-01T09:03:00.000000Z' as #TIMESTAMP))," +
                        "(cast('2023-01-01T09:04:00.000000Z' as #TIMESTAMP))," +
                        "(cast('2023-01-01T09:05:00.000000Z' as #TIMESTAMP))," +
                        "(cast('2023-01-01T09:06:00.000000Z' as #TIMESTAMP))," +
                        "(cast('2023-01-01T09:07:00.000000Z' as #TIMESTAMP))," +
                        "(cast('2023-01-01T09:08:00.000000Z' as #TIMESTAMP));",
                rightTableTimestampType.getTypeName()
        );

        execute("alter table trades add column sym symbol");
        execute("alter table trades add column price double");
        execute("alter table prices add column sym symbol");
        execute("alter table prices add column price double");
        executeWithRewriteTimestamp(
                "insert into trades(sym, price, ts) values " +
                        "('TSLA', 400.0, cast('2023-01-01T09:10:00.000000Z' as #TIMESTAMP))," +
                        "('TSLA', 401.0, cast('2023-01-01T09:11:00.000000Z' as #TIMESTAMP))," +
                        "('AMZN', 500.0, cast('2023-01-01T09:12:00.000000Z' as #TIMESTAMP))," +
                        "('AMZN', 501.0, cast('2023-01-01T09:13:00.000000Z' as #TIMESTAMP))," +
                        "('META', 600.0, cast('2023-01-01T09:14:00.000000Z' as #TIMESTAMP))," +
                        "('META', 601.0, cast('2023-01-01T09:15:00.000000Z' as #TIMESTAMP))," +
                        "('TSLA', 402.0, cast('2023-01-01T09:16:00.000000Z' as #TIMESTAMP))," +
                        "('AMZN', 502.0, cast('2023-01-01T09:17:00.000000Z' as #TIMESTAMP))," +
                        "('META', 602.0, cast('2023-01-01T09:18:00.000000Z' as #TIMESTAMP))," +
                        "('NFLX', 700.0, cast('2023-01-02T09:19:00.000000Z' as #TIMESTAMP));",
                leftTableTimestampType.getTypeName()
        );
        executeWithRewriteTimestamp(
                "insert into prices(sym, price, ts) values " +
                        "('TSLA', 399.5, cast('2023-01-01T09:09:00.000000Z' as #TIMESTAMP))," +
                        "('TSLA', 400.5, cast('2023-01-01T09:10:00.000000Z' as #TIMESTAMP))," +
                        "('AMZN', 499.5, cast('2023-01-01T09:11:00.000000Z' as #TIMESTAMP))," +
                        "('AMZN', 500.5, cast('2023-01-01T09:12:00.000000Z' as #TIMESTAMP))," +
                        "('META', 599.5, cast('2023-01-01T09:13:00.000000Z' as #TIMESTAMP))," +
                        "('META', 600.5, cast('2023-01-01T09:14:00.000000Z' as #TIMESTAMP))," +
                        "('TSLA', 401.5, cast('2023-01-01T09:15:00.000000Z' as #TIMESTAMP))," +
                        "('AMZN', 501.5, cast('2023-01-01T09:16:00.000000Z' as #TIMESTAMP))," +
                        "('META', 601.5, cast('2023-01-01T09:17:00.000000Z' as #TIMESTAMP))," +
                        "('NFLX', 699.5, cast('2023-01-02T09:18:00.000000Z' as #TIMESTAMP));",
                rightTableTimestampType.getTypeName()
        );
        if (leftConvertParquet) {
            execute("ALTER TABLE trades CONVERT PARTITION TO PARQUET WHERE ts >= 0");
        }
        if (rightConvertParquet) {
            execute("ALTER TABLE prices CONVERT PARTITION TO PARQUET WHERE ts >= 0");
        }
        drainWalQueue();
    }
}
