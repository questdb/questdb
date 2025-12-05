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
import io.questdb.griffin.engine.join.AsyncWindowJoinAtom;
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
        AsyncWindowJoinAtom.GROUP_BY_VALUE_USE_COMPACT_DIRECT_MAP = TestUtils.generateRandom(LOG).nextBoolean();
        sink.clear();
        //includePrevailing = TestUtils.generateRandom(null).nextBoolean();
        includePrevailing = false;
    }

    @Test
    public void testAggregateNotTrivialColumn() throws Exception {
        assertMemoryLeak(() -> {
            prepareTable();
            if (!includePrevailing) {
                printSql("select t.*, sum(p.price + 1) window_price " +
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
                                            select t.*, p.price
                                            from trades t 
                                            left join prices p 
                                            on (t.sym = p.sym) 
                                            and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts)
                                        union 
                                            select sym,price,ts,price1  from (select t.*, p.price price1, p.ts as ts1
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
                    "select t.*, sum(p.price + 1) as window_price " +
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
                printSql("select t.*, sum(p.price + 1) window_price " +
                        "from trades t " +
                        "left join prices p " +
                        "on p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) " +
                        "order by t.ts, t.sym;", sink);
            } else {
                printSql("""
                                select sym,price,ts, sum(price1 + 1) window_price from
                                (
                                        select * from (
                                            select t.*, p.price
                                            from trades t 
                                            left join prices p 
                                            on p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts)
                                        union 
                                            select sym,price,ts,price1  from (select t.*, p.price price1, p.ts as ts1
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
                    "select t.*, sum(p.price + 1) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );

            if (!includePrevailing) {
                printSql("select t.*, count()  " +
                        "from trades t " +
                        "left join prices p " +
                        "on p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) " +
                        "order by t.ts, t.sym;", sink);
            } else {
                printSql("""
                                select sym,price,ts, count() from
                                (
                                        select * from (
                                            select t.*, p.price
                                            from trades t 
                                            left join prices p 
                                            on p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts)
                                        union 
                                            select sym,price,ts,price1  from (select t.*, p.price price1, p.ts as ts1
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
                    "select t.*, count() " +
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
                            sym0\t2023-01-01T09:00:00.000000Z\t1.0
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
                printSql("select t.*, sum(p.price) window_price " +
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
                                            select t.*, p.price
                                            from trades t 
                                            left join prices p 
                                            on (t.sym = p.sym) 
                                            and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts)
                                        union 
                                            select sym,price,ts,price1  from (select t.*, p.price price1, p.ts as ts1
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
                    "select t.*, sum(p.price) as window_price " +
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
                printSql("select t.*, sum(p.price) as window_price " +
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
                                            select t.*, p.price
                                            from trades t 
                                            left join prices p 
                                            on (t.sym = p.sym) 
                                            and p.ts >= dateadd('m', -2, t.ts) AND p.ts <= dateadd('m', -1, t.ts)
                                        union 
                                            select sym,price,ts,price1  from (select t.*, p.price price1, p.ts as ts1
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
                    "select t.*, sum(p.price) as window_price " +
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
                printSql("select t.*, sum(p.price) window_price " +
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
                                            select t.*, p.price
                                            from trades t 
                                            left join prices p 
                                            on (t.sym = p.sym) 
                                            and p.ts >= dateadd('m', 1, t.ts) AND p.ts <= dateadd('m', 2, t.ts)
                                        union 
                                            select sym,price,ts,price1  from (select t.*, p.price price1, p.ts as ts1
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
                    "select t.*, sum(p.price) as window_price " +
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
            assertSkipToAndCalculateSize("select t.*, sum(t.price) as window_price " +
                    "from trades t " +
                    "window join prices p " +
                    "on (t.sym = p.sym) " +
                    " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                    "order by t.ts, t.sym", 21);
            assertSkipToAndCalculateSize("select t.*, sum(t.price) as window_price " +
                    "from trades t " +
                    "window join prices p " +
                    " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                    "order by t.ts, t.sym", 21);
            assertSkipToAndCalculateSize("select t.*, sum(t.price) as window_price " +
                    "from trades t " +
                    "window join prices p " +
                    "on t.sym = p.sym " +
                    " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                    " where t.price < 400 " +
                    "order by t.ts, t.sym", 11);
            assertSkipToAndCalculateSize("select t.*, sum(t.price) as window_price " +
                    "from trades t " +
                    "window join prices p " +
                    " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                    " where t.price < 400 " +
                    "order by t.ts, t.sym", 11);
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
                            2
                            1
                            1
                            2
                            1
                            2
                            1
                            1
                            1
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
                printSql("select t.*, avg(p.price) window_price " +
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
                                            select t.*, p.price
                                            from trades t 
                                            left join prices p 
                                            on (t.sym = p.sym) 
                                            and p.ts >= dateadd('m', -2, t.ts) AND p.ts <= dateadd('m', 2, t.ts) AND p.price < 300 
                                        union 
                                            select sym,price,ts,price1  from (select t.*, p.price price1, p.ts as ts1
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
                    "select t.*, avg(p.price) as window_price " +
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
                printSql("select t.*, avg(p.price) window_price " +
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
                                            select t.*, p.price
                                            from trades t 
                                            left join prices p 
                                            on (t.sym = p.sym) 
                                            and p.ts >= dateadd('m', -2, t.ts) AND p.ts <= dateadd('m', 2, t.ts) 
                                        union 
                                            select sym,price,ts,price1  from (select t.*, p.price price1, p.ts as ts1
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
                    "select t.*, avg(p.price) as window_price " +
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
                printSql("select t.*, avg(p.price) window_price " +
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
                                            select t.*, p.price
                                            from trades t 
                                            left join prices p 
                                            on (t.sym = p.sym) 
                                            and p.ts >= dateadd('m', -2, t.ts) AND p.ts <= dateadd('m', 2, t.ts) and (p.price < 200 or p.price > 300 )  
                                        union 
                                            select sym,price,ts,price1  from (select t.*, p.price price1, p.ts as ts1
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
                    "select t.*, avg(p.price) as window_price " +
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
                printSql("select t.*, avg(p.price) window_price " +
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
                                            select t.*, p.price
                                            from trades t 
                                            left join prices p 
                                            on (t.sym = p.sym) 
                                            and p.ts >= dateadd('m', -2, t.ts) AND p.ts <= dateadd('m', 2, t.ts) and (p.price < 200 and p.price > 300 )  
                                        union 
                                            select sym,price,ts,price1  from (select t.*, p.price price1, p.ts as ts1
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
                    "select t.*, avg(p.price) as window_price " +
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
                printSql("select t.*, avg(p.price) window_price " +
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
                                            select t.*, p.price
                                            from trades t 
                                            left join prices p 
                                            on (t.sym = p.sym) 
                                            and p.ts >= dateadd('m', -2, t.ts) AND p.ts <= dateadd('m', 2, t.ts) and (t.ts > 1000 and p.ts > 1000 AND (t.sym = p.sym) and p.price < 300 and p.sym != 'AAAAAA' )  
                                        union 
                                            select sym,price,ts,price1  from (select t.*, p.price price1, p.ts as ts1
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
                    "select t.*, avg(p.price) as window_price " +
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
                printSql("select t.*, avg(p.price) window_price " +
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
                                            select t.*, p.price
                                            from (select * from trades where price < 300) t 
                                            left join prices p 
                                            on (t.sym = p.sym) 
                                            and p.ts >= dateadd('m', -2, t.ts) AND p.ts <= dateadd('m', 2, t.ts) AND p.price < 300 
                                        union 
                                            select sym,price,ts,price1  from (select t.*, p.price price1, p.ts as ts1
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
                    "select t.*, avg(p.price) as window_price " +
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
                                            select t.*, p.price, p.ts pts
                                            from (select * from trades where price < 300) t 
                                            left join prices p 
                                            on (t.sym = p.sym) 
                                            and p.ts >= dateadd('m', -2, t.ts) AND p.ts <= dateadd('m', 2, t.ts) AND p.price < 300 
                                        union 
                                            select sym,price,ts,price1, ts1 pts   from (select t.*, p.price price1, p.ts as ts1
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

            assertQueryAndPlan(
                    """
                            sym\tprice\tts\tfirst\tavg
                            """,
                    """
                            Sort
                              keys: [ts, sym]
                                Window Fast Join
                                  vectorized: true
                                  symbol: sym=sym
                            """ +
                            "      window lo: 60000000 preceding" + (includePrevailing ? " (include prevailing)\n" : " (exclude prevailing)\n") +
                            """
                                          window hi: 60000000 following
                                            Limit lo: 5 hi: 9 skip-over-rows: 0 limit: 0
                                                Async JIT Filter workers: 1
                                                  filter: 5<price
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: trades
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: prices
                                    """,
                    "select t.*, first(p.bid) as first, avg(p.bid) as avg " +
                            "from (trades where price > 5 limit 5, 9) t " +
                            "window join prices p " +
                            "on (t.sym = p.sym) " +
                            " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by t.ts, t.sym;",
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
            if (!includePrevailing) {
                printSql("select t.*, sum(p.price) window_price " +
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
                                            select t.*, p.price
                                            from t 
                                            left join prices p 
                                            on (t.sym = p.sym) 
                                            and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts)
                                        union 
                                            select sym,price,ts,price1  from (select t.*, p.price price1, p.ts as ts1
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
                    "select t.*, sum(p.price) as window_price " +
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
                    "declare @x := '2023-01-01T09:04:00.000000Z' select t.*, sum(p.price) as window_price " +
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
                printSql("select t.*, sum(p.price) window_price " +
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
                                            select t.*, p.price
                                            from t 
                                            left join prices p 
                                            on (t.sym = p.sym) 
                                            and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts)
                                        union 
                                            select sym,price,ts,price1  from (select t.*, p.price price1, p.ts as ts1
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
                    "select t.*, sum(p.price) as window_price " +
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
                printSql("select t.*, sum(p.price), max(p.ts), avg(p.price), min(p.ts) " +
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
                                            select t.*, p.price, p.ts pts
                                            from trades t 
                                            left join prices p 
                                            on (t.sym = p.sym) 
                                            and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts)
                                        union 
                                            select sym,price,ts,price1,ts1  from (select t.*, p.price price1, p.ts as ts1
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
                    "select t.*, sum(p.price), max(p.ts), avg(p.price), min(p.ts) " +
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
                printSql("select t.*, max(p.ts), sum(p.price), avg(p.price), min(p.ts) " +
                        "from trades t " +
                        "left join prices p " +
                        " on p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) " +
                        "order by t.ts, t.sym;", sink);
            } else {
                printSql("""
                                select sym,price,ts, max(pts), sum(price1), avg(price1), min(pts) from
                                (
                                        select * from (
                                            select t.*, p.price, p.ts pts
                                            from trades t 
                                            left join prices p 
                                            on p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts)
                                        union 
                                            select sym,price,ts,price1,ts1  from (select t.*, p.price price1, p.ts as ts1
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
                    "select t.*, max(p.ts), sum(p.price), avg(p.price), min(p.ts) " +
                            "from trades t " +
                            "window join prices p " +
                            " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );

            printSql("select t.*, max(p.ts), sum(p.price + 100), avg(p.price + 100), min(p.ts), avg(p.price + 101) " +
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
                    "select t.*, max(p.ts), sum(p.price + 100), avg(p.price + 100), min(p.ts), avg(p.price + 101) " +
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
                printSql(" SELECT t.ts, t.price price, t.sym, max(cast(concat(p.price, '0') as double)) max_price, count(p.price) cnt " +
                        "from trades t " +
                        "left join prices p " +
                        "on (t.sym = p.sym) " +
                        " and p.ts >= dateadd('s', -1, t.ts) AND p.ts <= dateadd('s', 1, t.ts) " +
                        "order by t.ts, t.sym;", sink);
            } else {
                printSql("""
                                select ts, price, sym, max(cast(concat(price1, '0') as double)) max_price, count(price1) cnt from
                                (
                                        select * from (
                                            select t.*, p.price, p.ts pts
                                            from trades t 
                                            left join prices p 
                                            on (t.sym = p.sym) 
                                            and p.ts >= dateadd('s', -1, t.ts) AND p.ts <= dateadd('s', 1, t.ts)
                                        union 
                                            select sym,price,ts,price1,ts1  from (select t.*, p.price price1, p.ts as ts1
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
                printSql(" SELECT t.ts, t.price price, t.sym, max(cast(concat(p.price, '2') as double)) max_price, count(p.price) cnt " +
                        "from trades t " +
                        "left join prices p " +
                        "on (t.sym = p.sym) " +
                        " and p.ts >= dateadd('s', -1, t.ts) AND p.ts <= dateadd('s', 1, t.ts) " +
                        " where cast(concat(t.price, '2') as double) > 200 " +
                        " order by t.ts, t.sym;", sink);
            } else {
                printSql("""
                                with t as (select * from trades where cast(concat(price, '2') as double) > 200)
                                select ts, price, sym, max(cast(concat(price1, '2') as double)) max_price, count(price1) cnt from
                                (
                                        select * from (
                                            select t.*, p.price, p.ts pts
                                            from t 
                                            left join prices p 
                                            on (t.sym = p.sym) 
                                            and p.ts >= dateadd('s', -1, t.ts) AND p.ts <= dateadd('s', 1, t.ts)
                                        union 
                                            select sym,price,ts,price1,ts1  from (select t.*, p.price price1, p.ts as ts1
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
                printSql("select t.*, avg(p.price) window_price " +
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
                                            select t.*, p.price, p.ts pts
                                            from trades t 
                                            left join prices p 
                                            on (t.sym = p.sym) 
                                            and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts)
                                        union 
                                            select sym,price,ts,price1,ts1  from (select t.*, p.price price1, p.ts as ts1
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
                    "select t.*, avg(p.price) as window_price " +
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
                printSql("select t.*, sum(p.price) window_price " +
                        "from trades t " +
                        "left join prices p " +
                        "on p.ts >= dateadd('m', -15, t.ts) AND p.ts <= dateadd('m', -14, t.ts) " +
                        "order by t.ts, t.sym;", sink);
            } else {
                printSql("""
                                select sym,price,ts, sum(price1) window_price from
                                (
                                        select * from (
                                            select t.*, p.price, p.ts pts
                                            from trades t 
                                            left join prices p 
                                            on p.ts >= dateadd('m', -15, t.ts) AND p.ts <= dateadd('m', -14, t.ts)
                                        union 
                                            select sym,price,ts,price1,ts1  from (select t.*, p.price price1, p.ts as ts1
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
                    "select t.*, sum(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            " range between 15 minute preceding and 14 minute preceding " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by t.ts;",
                    "ts",
                    false,
                    false
            );

            if (!includePrevailing) {
                printSql("select t.*, sum(p.price) window_price " +
                        "from trades t " +
                        "left join prices p " +
                        "on t.sym = p.sym and p.ts >= dateadd('m', -5, t.ts) AND p.ts <= dateadd('m', -4, t.ts) " +
                        "order by t.ts, t.sym;", sink);
            } else {
                printSql("""
                                select sym,price,ts, sum(price1) window_price from
                                (
                                        select * from (
                                            select t.*, p.price, p.ts pts
                                            from trades t 
                                            left join prices p 
                                            on (t.sym = p.sym) 
                                            and p.ts >= dateadd('m', -5, t.ts) AND p.ts <= dateadd('m', -4, t.ts)
                                        union 
                                            select sym,price,ts,price1,ts1  from (select t.*, p.price price1, p.ts as ts1
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
                    "select t.*, sum(p.price) as window_price " +
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
                            ts	sym	window_price1	window_price2
                            ts	sym	window_price1	window_price2
                            2023-01-01T09:00:00.000000Z	AAPL	301.5	301.5
                            2023-01-01T09:01:00.000000Z	AAPL	202.0	301.5
                            2023-01-01T09:02:00.000000Z		199.0	199.0
                            2023-01-01T09:02:00.000000Z	AAPL	101.5	202.0
                            2023-01-01T09:03:00.000000Z	MSFT	400.0	400.0
                            2023-01-01T09:04:00.000000Z	MSFT	200.5	400.0
                            2023-01-01T09:05:00.000000Z	GOOGL	600.0	600.0
                            2023-01-01T09:06:00.000000Z	GOOGL	300.5	901.5
                            2023-01-01T09:07:00.000000Z	AAPL	102.5	102.5
                            2023-01-01T09:08:00.000000Z	MSFT	201.5	201.5
                            2023-01-01T09:09:00.000000Z	GOOGL	301.5	301.5
                            2023-01-01T09:10:00.000000Z	TSLA	800.0	800.0
                            2023-01-01T09:11:00.000000Z	TSLA	400.5	800.0
                            2023-01-01T09:12:00.000000Z	AMZN	1000.0	1000.0
                            2023-01-01T09:13:00.000000Z	AMZN	500.5	1000.0
                            2023-01-01T09:14:00.000000Z	META	1200.0	1200.0
                            2023-01-01T09:15:00.000000Z	META	600.5	1801.5
                            2023-01-01T09:16:00.000000Z	TSLA	401.5	401.5
                            2023-01-01T09:17:00.000000Z	AMZN	501.5	501.5
                            2023-01-01T09:18:00.000000Z	META	601.5	601.5
                            2023-01-01T09:19:00.000000Z	NFLX	699.5	699.5
                            """, leftTableTimestampType.getTypeName())
                    : replaceTimestampSuffix("""
                    ts\tsym\twindow_price1\twindow_price2
                    2023-01-01T09:00:00.000000Z\tAAPL\t301.5\t301.5
                    2023-01-01T09:01:00.000000Z\tAAPL\t202.0\t301.5
                    2023-01-01T09:02:00.000000Z\t\t199.0\t199.0
                    2023-01-01T09:02:00.000000Z\tAAPL\t101.5\t202.0
                    2023-01-01T09:03:00.000000Z\tMSFT\t400.0\t400.0
                    2023-01-01T09:04:00.000000Z\tMSFT\t200.5\t400.0
                    2023-01-01T09:05:00.000000Z\tGOOGL\t600.0\t600.0
                    2023-01-01T09:06:00.000000Z\tGOOGL\t300.5\t901.5
                    2023-01-01T09:07:00.000000Z\tAAPL\t102.5\t102.5
                    2023-01-01T09:08:00.000000Z\tMSFT\t201.5\t201.5
                    2023-01-01T09:09:00.000000Z\tGOOGL\t301.5\t301.5
                    """, leftTableTimestampType.getTypeName());
            assertQueryAndPlan(
                    expect,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Window Fast Join\n" +
                            "      vectorized: true\n" +
                            "      symbol: t.sym=sym\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " preceding " + (includePrevailing ? "(include prevailing)\n" : "(exclude prevailing)\n") +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " following\n" +
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
                            " range between 2 minute preceding and 2 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );

            expect = includePrevailing ?
                    replaceTimestampSuffix("""
                            ts	sym	window_price2
                            2023-01-01T09:00:00.000000Z	AAPL	301.5
                            2023-01-01T09:01:00.000000Z	AAPL	301.5
                            2023-01-01T09:02:00.000000Z		199.0
                            2023-01-01T09:02:00.000000Z	AAPL	202.0
                            2023-01-01T09:03:00.000000Z	MSFT	400.0
                            2023-01-01T09:04:00.000000Z	MSFT	400.0
                            2023-01-01T09:05:00.000000Z	GOOGL	600.0
                            2023-01-01T09:06:00.000000Z	GOOGL	901.5
                            2023-01-01T09:07:00.000000Z	AAPL	204.0
                            2023-01-01T09:08:00.000000Z	MSFT	402.0
                            2023-01-01T09:09:00.000000Z	GOOGL	602.0
                            """, leftTableTimestampType.getTypeName())
                    : replaceTimestampSuffix("""
                    ts	sym	window_price2
                    2023-01-01T09:00:00.000000Z	AAPL	301.5
                    2023-01-01T09:01:00.000000Z	AAPL	301.5
                    2023-01-01T09:02:00.000000Z		199.0
                    2023-01-01T09:02:00.000000Z	AAPL	202.0
                    2023-01-01T09:03:00.000000Z	MSFT	400.0
                    2023-01-01T09:04:00.000000Z	MSFT	400.0
                    2023-01-01T09:05:00.000000Z	GOOGL	600.0
                    2023-01-01T09:06:00.000000Z	GOOGL	901.5
                    2023-01-01T09:07:00.000000Z	AAPL	102.5
                    2023-01-01T09:08:00.000000Z	MSFT	201.5
                    2023-01-01T09:09:00.000000Z	GOOGL	301.5
                    """, leftTableTimestampType.getTypeName());
            assertQueryAndPlan(
                    expect,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Window Fast Join\n" +
                            "      vectorized: true\n" +
                            "      symbol: t.sym=sym\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " preceding " + (includePrevailing ? "(include prevailing)\n" : "(exclude prevailing)\n") +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " following\n" +
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
                    "select t.ts, t.sym,  sum(p1.price) as window_price2 " +
                            "from trades t " +
                            "window join prices p " +
                            "on (t.sym = p.sym) " +
                            " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "window join prices p1 " +
                            "on (t.sym = p1.sym) " +
                            " range between 2 minute preceding and 2 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    false
            );

            expect = replaceTimestampSuffix("""
                    ts	sym	window_price1	window_price2
                    2023-01-01T09:00:00.000000Z	AAPL	301.5	null
                    2023-01-01T09:01:00.000000Z	AAPL	600.5	null
                    2023-01-01T09:02:00.000000Z		700.5	null
                    2023-01-01T09:02:00.000000Z	AAPL	700.5	null
                    2023-01-01T09:03:00.000000Z	MSFT	898.5	null
                    2023-01-01T09:04:00.000000Z	MSFT	800.5	null
                    2023-01-01T09:05:00.000000Z	GOOGL	702.5	null
                    2023-01-01T09:06:00.000000Z	GOOGL	604.5	null
                    2023-01-01T09:07:00.000000Z	AAPL	605.5	null
                    2023-01-01T09:08:00.000000Z	MSFT	503.0	null
                    2023-01-01T09:09:00.000000Z	GOOGL	301.5	null
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
                            2023-01-01T09:00:00.000000Z	AAPL	null	301.5
                            2023-01-01T09:01:00.000000Z	AAPL	null	301.5
                            2023-01-01T09:02:00.000000Z		null	199.0
                            2023-01-01T09:02:00.000000Z	AAPL	null	202.0
                            2023-01-01T09:03:00.000000Z	MSFT	null	400.0
                            2023-01-01T09:04:00.000000Z	MSFT	null	400.0
                            2023-01-01T09:05:00.000000Z	GOOGL	null	600.0
                            2023-01-01T09:06:00.000000Z	GOOGL	null	901.5
                            2023-01-01T09:07:00.000000Z	AAPL	null	204.0
                            2023-01-01T09:08:00.000000Z	MSFT	null	402.0
                            2023-01-01T09:09:00.000000Z	GOOGL	null	602.0
                            """, leftTableTimestampType.getTypeName())
                    : replaceTimestampSuffix("""
                    ts	sym	window_price1	window_price2
                    2023-01-01T09:00:00.000000Z	AAPL	null	301.5
                    2023-01-01T09:01:00.000000Z	AAPL	null	301.5
                    2023-01-01T09:02:00.000000Z		null	199.0
                    2023-01-01T09:02:00.000000Z	AAPL	null	202.0
                    2023-01-01T09:03:00.000000Z	MSFT	null	400.0
                    2023-01-01T09:04:00.000000Z	MSFT	null	400.0
                    2023-01-01T09:05:00.000000Z	GOOGL	null	600.0
                    2023-01-01T09:06:00.000000Z	GOOGL	null	901.5
                    2023-01-01T09:07:00.000000Z	AAPL	null	102.5
                    2023-01-01T09:08:00.000000Z	MSFT	null	201.5
                    2023-01-01T09:09:00.000000Z	GOOGL	null	301.5
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
            expect = replaceTimestampSuffix("""
                    ts	sym	window_price1	window_price2	cnt
                    2023-01-01T09:00:00.000000Z	AAPL	99.5	101.5	3
                    2023-01-01T09:01:00.000000Z	AAPL	100.5	101.5	2
                    2023-01-01T09:02:00.000000Z		199.0	199.0	1
                    2023-01-01T09:02:00.000000Z	AAPL	101.5	101.5	1
                    2023-01-01T09:03:00.000000Z	MSFT	199.5	200.5	2
                    2023-01-01T09:04:00.000000Z	MSFT	200.5	200.5	1
                    2023-01-01T09:05:00.000000Z	GOOGL	299.5	300.5	2
                    2023-01-01T09:06:00.000000Z	GOOGL	300.5	301.5	1
                    2023-01-01T09:07:00.000000Z	AAPL	102.5	102.5	1
                    2023-01-01T09:08:00.000000Z	MSFT	201.5	201.5	1
                    2023-01-01T09:09:00.000000Z	GOOGL	301.5	301.5	1
                    """, leftTableTimestampType.getTypeName());
            assertQueryAndPlan(
                    expect,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Window Fast Join\n" +
                            "      vectorized: true\n" +
                            "      symbol: t.sym=sym\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " preceding " + (includePrevailing ? "(include prevailing)\n" : "(exclude prevailing)\n") +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "120000000" : "120000000000") + " following\n" +
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
                            " range between 2 minute preceding and 2 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
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
                    "select t.*, sum(p.price) sum_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on t.price-p.price " +
                            " range between 1 second preceding and 1 second following" + (includePrevailing ? " include prevailing;" : " exclude prevailing;"),
                    80,
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
                            " range between 1 second preceding and 1 second following" + (includePrevailing ? " include prevailing;" : " exclude prevailing;"),
                    12,
                    "WINDOW join cannot reference right table non-aggregate column: p.price"
            );
        });
    }

    @Test
    public void testWindowJoinFailsOnSlaveColumnsInFilter() throws Exception {
        assertMemoryLeak(() -> {
            prepareTable();
            assertExceptionNoLeakCheck(
                    "select t.*, sum(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on (t.sym = p.sym) " +
                            " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "where p.price > 10 " +
                            "order by t.ts, t.sym;",
                    178,
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
                    "select t.*, sum(p.price) as window_price " +
                            "from (trades limit 5) t " +
                            "window join (select * from prices where ts in '2023-01-01T09:03:00.000000Z' or ts = '2023-01-01T09:07:00.000000Z' or ts = '2023-01-01T09:08:00.000000Z') p " +
                            "on (t.sym = p.sym) " +
                            " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
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
                            " range between unbounded preceding and 1 day following" + (includePrevailing ? " include prevailing;" : " exclude prevailing;"),
                    118,
                    "unbounded preceding/following is not supported in WINDOW joins"
            );

            assertExceptionNoLeakCheck(
                    "select t.*, sum(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on t.sym = p.sym " +
                            " range between 1 second preceding and unbounded following" + (includePrevailing ? " include prevailing;" : " exclude prevailing;"),
                    141,
                    "unbounded preceding/following is not supported in WINDOW joins"
            );

            assertExceptionNoLeakCheck(
                    "select t.*, sum(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on t.sym = p.sym " +
                            " range between unbounded preceding and unbounded following" + (includePrevailing ? " include prevailing;" : " exclude prevailing;"),
                    118,
                    "unbounded preceding/following is not supported in WINDOW joins"
            );
        });
    }

    @Test
    public void testWindowJoinNoOtherCondition() throws Exception {
        assertMemoryLeak(() -> {
            prepareTable();
            if (!includePrevailing) {
                printSql("select t.*, sum(p.price) window_price " +
                        "from trades t " +
                        "left join prices p " +
                        "on p.ts >= dateadd('m', -2, t.ts) AND p.ts <= dateadd('m', -1, t.ts) " +
                        "order by t.ts, t.sym;", sink);
            } else {
                printSql("""
                                select sym,price,ts, sum(price1) window_price from
                                (
                                        select * from (
                                            select t.*, p.price, p.ts pts
                                            from trades t 
                                            left join prices p 
                                            on p.ts >= dateadd('m', -2, t.ts) AND p.ts <= dateadd('m', -1, t.ts)
                                        union 
                                            select sym,price,ts,price1,ts1  from (select t.*, p.price price1, p.ts as ts1
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
                    "select t.*, sum(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            " range between 2 minute preceding and 1 minute preceding " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by t.ts;",
                    "ts",
                    false,
                    false
            );

            if (!includePrevailing) {
                printSql("select t.*, sum(p.price) window_price " +
                        "from trades t " +
                        "left join prices p " +
                        "on p.ts >= dateadd('m', -3, t.ts) AND p.ts <= dateadd('m', -2, t.ts) " +
                        "order by t.ts, t.sym;", sink);
            } else {
                printSql("""
                                select sym,price,ts, sum(price1) window_price from
                                (
                                        select * from (
                                            select t.*, p.price, p.ts pts
                                            from trades t 
                                            left join prices p 
                                            on p.ts >= dateadd('m', -3, t.ts) AND p.ts <= dateadd('m', -2, t.ts)
                                        union 
                                            select sym,price,ts,price1,ts1  from (select t.*, p.price price1, p.ts as ts1
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
                    "select t.*, sum(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            " range between 3 minute preceding and 2 minute preceding " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by t.ts;",
                    "ts",
                    false,
                    false
            );

            if (!includePrevailing) {
                printSql("select t.*, sum(p.price) window_price, count() as cnt " +
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
                                            select t.*, p.price, p.ts pts
                                            from t 
                                            left join prices p 
                                            on p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts)
                                        union 
                                            select sym,price,ts,price1,ts1  from (select t.*, p.price price1, p.ts as ts1
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
                    "select t.*, sum(p.price) as window_price, count() as cnt " +
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
                                            select t.*, p.price, p.ts pts
                                            from trades t 
                                            left join prices p 
                                            on (t.sym = p.sym) 
                                            and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts)
                                        union 
                                            select sym,price,ts,price1,ts1  from (select t.*, p.price price1, p.ts as ts1
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
                                            select t.*, p.price, p.ts pts
                                            from trades t 
                                            left join prices p 
                                            on (t.sym != p.sym and p.price > 100) 
                                            and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts)
                                        union 
                                            select sym,price,ts,price1,ts1  from (select t.*, p.price price1, p.ts as ts1
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
                                    ORDER BY t.ts, t.sym""",
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
                                    AAPL	100.0	2023-01-01T09:00:00.000000Z	200.0
                                    AAPL	101.0	2023-01-01T09:01:00.000000Z	202.0
                                    	102.0	2023-01-01T09:02:00.000000Z	199.0
                                    """
                            : """
                            sym\tprice\tts\tsum_price
                            AAPL\t100.0\t2023-01-01T09:00:00.000000Z\t100.5
                            AAPL\t101.0\t2023-01-01T09:01:00.000000Z\t101.5
                            \t102.0\t2023-01-01T09:02:00.000000Z\t199.0
                            """,
                    "select t.*, sum(p.price) sum_price " +
                            "from (trades limit 3) t " +
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
                    "select t.*, sum(p.price) sum_price " +
                            "from (trades limit 3) t " +
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
                                    AAPL	100.0	2023-01-01T09:00:00.000000Z	200.0
                                    AAPL	101.0	2023-01-01T09:01:00.000000Z	202.0
                                    	102.0	2023-01-01T09:02:00.000000Z	500.0
                                    """
                            : """
                            sym\tprice\tts\tsum_price
                            AAPL\t100.0\t2023-01-01T09:00:00.000000Z\t100.5
                            AAPL\t101.0\t2023-01-01T09:01:00.000000Z\t101.5
                            \t102.0\t2023-01-01T09:02:00.000000Z\t398.5
                            """,
                    "select t.ts, t.sym, t.price, sum(p.price) sum_price " +
                            "from (trades limit 3) t " +
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
                printSql("select t.*, sum(p.price) window_price " +
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
                                            select t.*, p.price, p.ts pts
                                            from t 
                                            left join prices p 
                                            on t.sym = p.sym and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts)
                                        union 
                                            select sym,price,ts,price1,ts1  from (select t.*, p.price price1, p.ts as ts1
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
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Window Fast Join\n" +
                            "      vectorized: true\n" +
                            "      symbol: sym=sym\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding " + (includePrevailing ? "(include prevailing)\n" : "(exclude prevailing)\n") +
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
                            " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
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
            // fast factory
            if (!includePrevailing) {
                printSql("select t.*, max(concat(p.price, '000')) f " +
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
                                            select t.*, p.price, p.ts pts
                                            from t 
                                            left join prices p 
                                            on t.sym = p.sym and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts)
                                        union 
                                            select sym,price,ts,price1,ts1  from (select t.*, p.price price1, p.ts as ts1
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
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Window Fast Join\n" +
                            "      vectorized: false\n" +
                            "      symbol: sym=sym\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding " + (includePrevailing ? "(include prevailing)\n" : "(exclude prevailing)\n") +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " following\n" +
                            "        Limit lo: 1 hi: 4 skip-over-rows: 1 limit: 3\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Interval forward scan on: trades\n" +
                            "                  intervals: [(\"2023-01-01T09:00:00." + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "000001" : "000000001") + "Z\",\"MAX\")]\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            (includePrevailing ? "            Frame forward scan on: prices\n" :
                                    "            Interval forward scan on: prices\n" +
                                            "              intervals: [(\"" + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? (ColumnType.isTimestampMicro(rightTableTimestampType.getTimestampType()) ? "2023-01-01T08:59:00.000001Z" : "2023-01-01T08:59:00.000001000Z")
                                            : (ColumnType.isTimestampMicro(rightTableTimestampType.getTimestampType()) ? "2023-01-01T08:59:00.000000Z" : "2023-01-01T08:59:00.000000001Z")) + "\",\"MAX\")]\n"),
                    "select t.*, max(concat(p.price, '000')) f " +
                            "from (trades where ts > '2023-01-01T09:00:00Z' limit 1, 4) t " +
                            "window join prices p " +
                            "on (t.sym = p.sym) " +
                            " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    true
            );

            // fast factory, vectorized
            if (!includePrevailing) {
                printSql("select t.*, sum(p.price) window_price, count(p.price) as cnt " +
                        "from (trades where ts > '2023-01-01T09:00:00Z' limit 1, 4) t " +
                        "left join prices p " +
                        "on (t.sym = p.sym) " +
                        " and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts) " +
                        "order by t.ts, t.sym;", sink);
            } else {
                printSql("""
                                with t as (trades where ts > '2023-01-01T09:00:00Z' limit 1, 4)
                                select sym,price,ts, sum(price1) window_price, count(price1) as cnt from
                                (
                                        select * from (
                                            select t.*, p.price, p.ts pts
                                            from t 
                                            left join prices p 
                                            on t.sym = p.sym and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts)
                                        union 
                                            select sym,price,ts,price1,ts1  from (select t.*, p.price price1, p.ts as ts1
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
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Window Fast Join\n" +
                            "      vectorized: true\n" +
                            "      symbol: sym=sym\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding " + (includePrevailing ? "(include prevailing)\n" : "(exclude prevailing)\n") +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " following\n" +
                            "        Limit lo: 1 hi: 4 skip-over-rows: 1 limit: 3\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Interval forward scan on: trades\n" +
                            "                  intervals: [(\"2023-01-01T09:00:00." + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "000001" : "000000001") + "Z\",\"MAX\")]\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            (includePrevailing ? "            Frame forward scan on: prices\n" :
                                    "            Interval forward scan on: prices\n" +
                                            "              intervals: [(\"" + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? (ColumnType.isTimestampMicro(rightTableTimestampType.getTimestampType()) ? "2023-01-01T08:59:00.000001Z" : "2023-01-01T08:59:00.000001000Z")
                                            : (ColumnType.isTimestampMicro(rightTableTimestampType.getTimestampType()) ? "2023-01-01T08:59:00.000000Z" : "2023-01-01T08:59:00.000000001Z")) + "\",\"MAX\")]\n"),
                    "select t.*, sum(p.price) as window_price, count() as cnt " +
                            "from (trades where ts > '2023-01-01T09:00:00Z' limit 1, 4) t " +
                            "window join prices p " +
                            "on (t.sym = p.sym) " +
                            " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    true
            );

            // non-fast factory
            assertQueryAndPlan(
                    sink,
                    "Sort\n" +
                            "  keys: [ts, sym]\n" +
                            "    Window Join\n" +
                            "      window lo: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " preceding " + (includePrevailing ? "(include prevailing)\n" : "(exclude prevailing)\n") +
                            "      window hi: " + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "60000000" : "60000000000") + " following\n" +
                            "      join filter: concat([t.sym,'_0'])=concat([p.sym,'_0'])\n" +
                            "        Limit lo: 1 hi: 4 skip-over-rows: 1 limit: 3\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Interval forward scan on: trades\n" +
                            "                  intervals: [(\"2023-01-01T09:00:00." + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? "000001" : "000000001") + "Z\",\"MAX\")]\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            (includePrevailing ? "            Frame forward scan on: prices\n" :
                                    "            Interval forward scan on: prices\n" +
                                            "              intervals: [(\"" + (ColumnType.isTimestampMicro(leftTableTimestampType.getTimestampType()) ? (ColumnType.isTimestampMicro(rightTableTimestampType.getTimestampType()) ? "2023-01-01T08:59:00.000001Z" : "2023-01-01T08:59:00.000001000Z")
                                            : (ColumnType.isTimestampMicro(rightTableTimestampType.getTimestampType()) ? "2023-01-01T08:59:00.000000Z" : "2023-01-01T08:59:00.000000001Z")) + "\",\"MAX\")]\n"),
                    "select t.*, sum(p.price) as window_price, count as cnt " +
                            "from (trades where ts > '2023-01-01T09:00:00Z' limit 1, 4) t " +
                            "window join prices p " +
                            "on (concat(t.sym, '_0') = concat(p.sym, '_0')) " +
                            " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            "order by t.ts, t.sym;",
                    "ts",
                    true,
                    true
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
                    """
                            sym\tprice\tsum_price
                            \t102.0\t199.0
                            """,
                    "select t.sym, t.price, sum(p.price) sum_price " +
                            "from (trades limit 3) t " +
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
                printSql("select t.*, sum(p.price) window_price " +
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
                                            select t.*, p.price, p.ts pts
                                            from trades t 
                                            left join prices p 
                                            on (0 = 1) and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts)
                                        union 
                                            select sym,price,ts,price1,ts1  from (select t.*, p.price price1, p.ts as ts1
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
                    "select t.*, sum(p.price) as window_price " +
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
                                            select t.*, p.price, p.ts pts
                                            from trades t 
                                            left join prices p 
                                            on (0 = 1) and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts)
                                        union 
                                            select sym,price,ts,price1,ts1  from (select t.*, p.price price1, p.ts as ts1
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
                printSql("select t.*, avg(p.price) window_price " +
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
                                            select t.*, p.price, p.ts pts
                                            from trades t 
                                            left join prices p 
                                            on (t.sym = p.sym) and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts)
                                        union 
                                            select sym,price,ts,price1,ts1  from (select t.*, p.price price1, p.ts as ts1
                                            from trades t 
                                            join prices p 
                                            on (t.sym = p.sym) and p.ts <= dateadd('m', -1, t.ts)) LATEST ON ts1 PARTITION BY ts, sym
                                        ) order by ts
                                )
                                order by ts limit 3;
                                """,
                        sink);
            }
            assertQueryAndPlan(
                    sink,
                    "Limit lo: 3 skip-over-rows: 0 limit: 3\n" +
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
                    "select t.*, avg(p.price) as window_price " +
                            "from trades t " +
                            "window join prices p " +
                            "on (t.sym = p.sym) " +
                            " range between 1 minute preceding and 1 minute following " + (includePrevailing ? " include prevailing " : " exclude prevailing ") +
                            " limit 3",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testWithOnlyAggregateLeftTableColumn() throws Exception {
        assertMemoryLeak(() -> {
            prepareTable();
            if (!includePrevailing) {
                printSql("select t.*, sum(t.price) window_price " +
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
                                            select t.*, p.price, p.ts pts
                                            from trades t 
                                            left join prices p 
                                            on (t.sym = p.sym) and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts)
                                        union 
                                            select sym,price,ts,price1,ts1  from (select t.*, p.price price1, p.ts as ts1
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
                    "select t.*, sum(t.price) as window_price " +
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
                        "('NFLX', 700.0, cast('2023-01-01T09:19:00.000000Z' as #TIMESTAMP));",
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
                        "('NFLX', 699.5, cast('2023-01-01T09:18:00.000000Z' as #TIMESTAMP));",
                rightTableTimestampType.getTypeName()
        );
    }
}
