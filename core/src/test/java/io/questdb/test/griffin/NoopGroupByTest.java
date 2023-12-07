/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

public class NoopGroupByTest extends AbstractCairoTest {

    @Test
    public void testMissingGroupByWithHourFunction() throws Exception {
        assertQuery(
                "hour\tavgBid\n",
                "select hour(ts), avg(bid) avgBid from x order by hour",
                "create table x (\n" +
                        "    sym1 symbol,\n" +
                        "    sym2 symbol,\n" +
                        "    bid double,\n" +
                        "    ask double,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                null,
                "insert into x select * from (select " +
                        "         rnd_symbol('A', 'B', 'C') sym1, \n" +
                        "         rnd_symbol('D', 'E', 'F') sym2, \n" +
                        "        rnd_double() bid, \n" +
                        "        rnd_double() ask, \n" +
                        "        timestamp_sequence(172800000000, 360000000) ts \n" +
                        "    from long_sequence(20)) timestamp (ts)",
                "hour\tavgBid\n" +
                        "0\t0.47607185409853914\n" +
                        "1\t0.6861237948732989\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testNoopGroupByBindVariable() throws Exception {
        ddl("create table y (id int, ref int, val double)");
        engine.releaseAllWriters();
        assertException(
                "select x.id, x.ref, y.ref, sum(val) from x join y on (id) group by x.id, :var, y.ref",
                "create table x (id int, ref int, ref3 int)",
                73,
                "literal expected"
        );
    }

    // with where clause
    @Test
    public void testNoopGroupByFailureWhenUsing1KeyInSelectStatementBut2InGroupBy() throws Exception {
        ddl("create table x ( " +
                "    sym1 symbol," +
                "    sym2 symbol," +
                "    bid int," +
                "    ask int )");
        engine.releaseAllWriters();

        String query = "select sym1, avg(bid) avgBid " +
                "from x " +
                "where sym1 in ('AA', 'BB' ) " +
                "group by sym1, sym2";
        assertPlan(
                query,
                "VirtualRecord\n" +
                        "  functions: [sym1,avgBid]\n" +
                        "    Async Group By workers: 1\n" +
                        "      keys: [sym1,sym2]\n" +
                        "      values: [avg(bid)]\n" +
                        "      filter: sym1 in [AA,BB]\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: x\n"
        );
        assertQuery("sym1\tavgBid\n", query, null, true, false);
    }

    @Test
    public void testNoopGroupByFailureWhenUsing2KeysInSelectStatementButOnlyOneInGroupByV1() throws Exception {
        assertException(
                "select sym1, sym2, avg(bid) avgBid from x where sym1 in ('AA', 'BB' ) group by sym1",
                "create table x (\n" +
                        "    sym1 symbol,\n" +
                        "    sym2 symbol,\n" +
                        "    bid int,\n" +
                        "    ask int\n" +
                        ")",
                13,
                "column must appear in GROUP BY clause or aggregate function"
        );
    }

    @Test
    public void testNoopGroupByFailureWhenUsing2KeysInSelectStatementButOnlyOneInGroupByV2() throws Exception {
        assertException(
                "select sym1, sym2, avg(bid) avgBid from x where sym1 in ('AA', 'BB' ) group by sym2",
                "create table x (\n" +
                        "    sym1 symbol,\n" +
                        "    sym2 symbol,\n" +
                        "    bid int,\n" +
                        "    ask int\n" +
                        ")",
                7,
                "column must appear in GROUP BY clause or aggregate function"
        );
    }

    @Test
    public void testNoopGroupByFailureWhenUsingAliasedColumnAndWrongTableAlias() throws Exception {
        assertException(
                "select sym ccy, avg(bid) avgBid from x a where sym in ('AA', 'BB' ) group by b.ccy",
                "create table x (\n" +
                        "    sym symbol,\n" +
                        "    bid int,\n" +
                        "    ask int\n" +
                        ")",
                77,
                "Invalid table name or alias"
        );
    }

    @Test
    public void testNoopGroupByFailureWhenUsingFunctionColumn() throws Exception {
        assertException(
                "select sym, avg(bid) avgBid from x where sym in ('AA', 'BB' ) group by avgBid",
                "create table x (\n" +
                        "    sym symbol,\n" +
                        "    bid int,\n" +
                        "    ask int\n" +
                        ")",
                71,
                "aggregate functions are not allowed in GROUP BY"
        );
    }

    @Test
    public void testNoopGroupByFailureWhenUsingInvalidColumn() throws Exception {
        assertException(
                "select sym, avg(bid) avgBid from x where sym in ('AA', 'BB' ) group by badColumn",
                "create table x (\n" +
                        "    sym symbol,\n" +
                        "    bid int,\n" +
                        "    ask int\n" +
                        ")",
                71,
                "Invalid column: badColumn"
        );
    }

    @Test
    public void testNoopGroupByInvalidColumnName1() throws Exception {
        assertException(
                "select a.sym1, avg(bid) avgBid from x a group by b.rubbish",
                "create table x (\n" +
                        "    sym1 symbol,\n" +
                        "    sym2 symbol,\n" +
                        "    bid double,\n" +
                        "    ask double,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                49,
                "Invalid table name or alias"
        );
    }

    @Test
    public void testNoopGroupByInvalidColumnName2() throws Exception {
        assertException(
                "select a.sym1, avg(bid) avgBid from x a group by b.sym1",
                "create table x (\n" +
                        "    sym1 symbol,\n" +
                        "    sym2 symbol,\n" +
                        "    bid double,\n" +
                        "    ask double,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                49,
                "Invalid table name or alias"
        );
    }

    @Test
    public void testNoopGroupByJoinArithmetic() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table x (id int, ref int, ref3 int)");
            insert("insert into x values (1,1,1), (1,2,2);");
            compile("create table y (id int, ref int, val double)");
            insert("insert into y values (1,1,1), (1,2,2);");
            engine.releaseAllWriters();
            assertQuery(
                    "id\tcolumn\tsum\n" +
                            "1\t2\t1.0\n" +
                            "1\t3\t3.0\n" +
                            "1\t4\t2.0\n",
                    "select x.id, x.ref + y.ref, sum(val) from x join y on (id) group by x.id, x.ref + y.ref",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testNoopGroupByJoinBadArithmetic() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table y(id int, ref int, val double)");
            compile("create table x (id int, ref int, ref3 int)");
            engine.releaseAllWriters();
            assertException(
                    "select x.id, x.ref - y.ref, sum(val) from x join y on (id) group by x.id, y.ref - x.ref",
                    21,
                    "column must appear in GROUP BY clause or aggregate function"
            );
        });
    }

    @Test
    public void testNoopGroupByJoinConst() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table x (id int, ref int, ref3 int)");
            compile("create table y(id int, ref int, val double)");
            engine.releaseAllWriters();
            assertQuery(
                    "z\tsum\n",
                    "select 'x' z, sum(val) from x join y on (id) group by 'x'",
                    null,
                    true
            );
        });
    }

    @Test
    public void testNoopGroupByJoinReference() throws Exception {
        ddl("create table y(id int, ref int, val double)");
        engine.releaseAllWriters();
        assertQuery(
                "id\tref\tref1\tsum\n",
                "select x.id, x.ref, y.ref, sum(val) from x join y on (id) group by x.id, x.ref, y.ref",
                "create table x (id int, ref int, ref3 int)",
                null,
                true,
                true
        );
    }

    @Test
    public void testNoopGroupByJoinReferenceNonSelected() throws Exception {
        ddl("create table y(id int, ref int, val double)");
        engine.releaseAllWriters();
        assertException(
                "select x.id, x.ref, y.ref, sum(val) from x join y on (id) group by x.id, x.ref3, y.ref",
                "create table x (id int, ref int, ref3 int)",
                13,
                "column must appear in GROUP BY clause or aggregate function"
        );
    }

    @Test
    public void testNoopGroupByJoinStringConst() throws Exception {
        ddl("create table x (id int, ref int, ref3 int)");
        ddl("create table y(id int, ref int, val double)");
        engine.releaseAllWriters();
        String query = "select 'x' z, sum(val) from x join y on (id) group by 'y'";
        assertQuery("z\tsum\n", query, null, true);
    }

    @Test
    public void testNoopGroupByMissingColumnWithTableAlias1() throws Exception {
        assertException(
                "select a.sym1, a.sym2, avg(bid) avgBid from x a group by a.sym1", //a.sym2 is missing in group by clause
                "create table x (\n" +
                        "    sym1 symbol,\n" +
                        "    sym2 symbol,\n" +
                        "    bid double,\n" +
                        "    ask double,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                15,
                "column must appear in GROUP BY clause or aggregate function"
        );
    }

    @Test
    public void testNoopGroupByValidColumnName() throws Exception {
        assertQuery(
                "sym1\tavgBid\n",
                "select a.sym1, avg(bid) avgBid from x a group by a.sym1 order by a.sym1",
                "create table x (\n" +
                        "    sym1 symbol,\n" +
                        "    sym2 symbol,\n" +
                        "    bid double,\n" +
                        "    ask double,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                null,
                "insert into x select * from (select " +
                        "         rnd_symbol('A', 'B', 'C') sym1, \n" +
                        "         rnd_symbol('D', 'E', 'F') sym2, \n" +
                        "        rnd_double() bid, \n" +
                        "        rnd_double() ask, \n" +
                        "        timestamp_sequence(172800000000, 360000000) ts \n" +
                        "    from long_sequence(20)) timestamp (ts)",
                "sym1\tavgBid\n" +
                        "A\t0.5942181417903911\n" +
                        "B\t0.7080299543021055\n" +
                        "C\t0.4760584891454253\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testNoopGroupByValidColumnNameWithHourFunction() throws Exception {
        assertQuery(
                "hour\tavgBid\n",
                //select hour(pickup_datetime), sum(passenger_count) from trips group by hour(pickup_datetime);
                "select hour(ts), avg(bid) avgBid from x group by hour(ts) order by hour",
                "create table x (\n" +
                        "    sym1 symbol,\n" +
                        "    sym2 symbol,\n" +
                        "    bid double,\n" +
                        "    ask double,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                null,
                "insert into x select * from (select " +
                        "         rnd_symbol('A', 'B', 'C') sym1, \n" +
                        "         rnd_symbol('D', 'E', 'F') sym2, \n" +
                        "        rnd_double() bid, \n" +
                        "        rnd_double() ask, \n" +
                        "        timestamp_sequence(172800000000, 360000000) ts \n" +
                        "    from long_sequence(20)) timestamp (ts)",
                "hour\tavgBid\n" +
                        "0\t0.47607185409853914\n" +
                        "1\t0.6861237948732989\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testNoopGroupByValidColumnNameWithHourFunctionAndAliasedTable() throws Exception {
        assertQuery(
                "hour\tavgBid\n",
                "select hour(a.ts), avg(bid) avgBid from x a group by hour(a.ts) order by hour",
                "create table x (\n" +
                        "    sym1 symbol,\n" +
                        "    sym2 symbol,\n" +
                        "    bid double,\n" +
                        "    ask double,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                null,
                "insert into x select * from (select " +
                        "         rnd_symbol('A', 'B', 'C') sym1, \n" +
                        "         rnd_symbol('D', 'E', 'F') sym2, \n" +
                        "        rnd_double() bid, \n" +
                        "        rnd_double() ask, \n" +
                        "        timestamp_sequence(172800000000, 360000000) ts \n" +
                        "    from long_sequence(20)) timestamp (ts)",
                "hour\tavgBid\n" +
                        "0\t0.47607185409853914\n" +
                        "1\t0.6861237948732989\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testNoopGroupByWhenUsingAliasedColumn() throws Exception {
        assertQuery(
                "ccy\tavgBid\n",
                "select sym1 ccy, avg(bid) avgBid from x where sym1 in ('A', 'B' ) group by ccy",
                "create table x (\n" +
                        "    sym1 symbol,\n" +
                        "    sym2 symbol,\n" +
                        "    bid double,\n" +
                        "    ask double,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                null,
                "insert into x select * from (select " +
                        "         rnd_symbol('A', 'B', 'C') sym1, \n" +
                        "         rnd_symbol('D', 'E', 'F') sym2, \n" +
                        "        rnd_double() bid, \n" +
                        "        rnd_double() ask, \n" +
                        "        timestamp_sequence(172800000000, 360000000) ts \n" +
                        "    from long_sequence(20)) timestamp (ts)",
                "ccy\tavgBid\n" +
                        "A\t0.5942181417903911\n" +
                        "B\t0.7080299543021055\n",
                true,
                true,
                false
        );
    }

    @Test // sym1 is aliased as ccy at stage later than group by
    public void testNoopGroupByWhenUsingAliasedColumnAndAliasedTable() throws Exception {
        assertException("select sym1 as ccy, avg(bid) avgBid from x a where sym1 in ('A', 'B' ) group by a.ccy",
                "create table x (\n" +
                        "    sym1 symbol,\n" +
                        "    sym2 symbol,\n" +
                        "    bid double,\n" +
                        "    ask double,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY", 80, "Invalid column: a.ccy"
        );
    }

    @Test
    public void testNoopGroupByWhenUsingAliasedColumnAndAliasedTable2() throws Exception {
        assertException(
                "select sym1 ccy, avg(bid) avgBid from x a where sym1 in ('A', 'B' ) group by b.ccy",
                "create table x (\n" +
                        "    sym1 symbol,\n" +
                        "    sym2 symbol,\n" +
                        "    bid double,\n" +
                        "    ask double,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                77,
                "Invalid table name or alias"
        );
    }

    @Test
    public void testNoopGroupByWhenUsingOriginalColumnAndAliasedTable() throws Exception {
        assertQuery(
                "ccy\tavgBid\n",
                "select sym1 as ccy, avg(bid) avgBid from x a where sym1 in ('A', 'B' ) group by a.sym1",
                "create table x (\n" +
                        "    sym1 symbol,\n" +
                        "    sym2 symbol,\n" +
                        "    bid double,\n" +
                        "    ask double,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                null,
                "insert into x select * from (select " +
                        "         rnd_symbol('A', 'B', 'C') sym1, \n" +
                        "         rnd_symbol('D', 'E', 'F') sym2, \n" +
                        "        rnd_double() bid, \n" +
                        "        rnd_double() ask, \n" +
                        "        timestamp_sequence(172800000000, 360000000) ts \n" +
                        "    from long_sequence(20)) timestamp (ts)",
                "ccy\tavgBid\n" +
                        "A\t0.5942181417903911\n" +
                        "B\t0.7080299543021055\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testNoopGroupByWith1Syms() throws Exception {
        assertQuery(
                "sym1\tavgBid\n",
                "select sym1, avg(bid) avgBid from x where sym1 in ('A', 'B' ) group by sym1",
                "create table x (\n" +
                        "    sym1 symbol,\n" +
                        "    sym2 symbol,\n" +
                        "    bid double,\n" +
                        "    ask double,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                null,
                "insert into x select * from (select " +
                        "         rnd_symbol('A', 'B', 'C') sym1, \n" +
                        "         rnd_symbol('D', 'E', 'F') sym2, \n" +
                        "        rnd_double() bid, \n" +
                        "        rnd_double() ask, \n" +
                        "        timestamp_sequence(172800000000, 360000000) ts \n" +
                        "    from long_sequence(20)) timestamp (ts)",
                "sym1\tavgBid\n" +
                        "A\t0.5942181417903911\n" +
                        "B\t0.7080299543021055\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testNoopGroupByWith2Syms() throws Exception {
        assertQuery(
                "sym1\tsym2\tavgBid\n",
                "select sym1, sym2, avg(bid) avgBid from x where sym1 in ('A', 'B' ) group by sym1, sym2",
                "create table x (\n" +
                        "    sym1 symbol,\n" +
                        "    sym2 symbol,\n" +
                        "    bid double,\n" +
                        "    ask double,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                null,
                "insert into x select * from (select " +
                        "         rnd_symbol('A', 'B', 'C') sym1, \n" +
                        "         rnd_symbol('D', 'E', 'F') sym2, \n" +
                        "        rnd_double() bid, \n" +
                        "        rnd_double() ask, \n" +
                        "        timestamp_sequence(172800000000, 360000000) ts \n" +
                        "    from long_sequence(20)) timestamp (ts)",
                "sym1\tsym2\tavgBid\n" +
                        "A\tD\t0.47381585528154324\n" +
                        "B\tE\t0.6403134139386097\n" +
                        "A\tE\t0.5837537495691357\n" +
                        "B\tD\t0.8434630350290969\n" +
                        "A\tF\t0.8664158914718532\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testNoopGroupByWithAlias() throws Exception {
        assertQuery(
                "sym1\tavgBid\n",
                "select sym1, avg(bid) avgBid from x a where sym1 in ('A', 'B' ) group by a.sym1",
                "create table x (\n" +
                        "    sym1 symbol,\n" +
                        "    sym2 symbol,\n" +
                        "    bid double,\n" +
                        "    ask double,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                null,
                "insert into x select * from (select " +
                        "         rnd_symbol('A', 'B', 'C') sym1, \n" +
                        "         rnd_symbol('D', 'E', 'F') sym2, \n" +
                        "        rnd_double() bid, \n" +
                        "        rnd_double() ask, \n" +
                        "        timestamp_sequence(172800000000, 360000000) ts \n" +
                        "    from long_sequence(20)) timestamp (ts)",
                "sym1\tavgBid\n" +
                        "A\t0.5942181417903911\n" +
                        "B\t0.7080299543021055\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testNoopGroupByWithCrossJoinAndFilterOnSymbolColumn() throws Exception {
        assertQuery(
                "ts\tsym2\ttotalCost\n",
                "SELECT A.ts, A.sym2, A.totalCost\n" +
                        "FROM (\n" +
                        "    SELECT checkpoint.minutestamp AS ts, T.sym2, sum(T.cost) AS totalCost\n" +
                        "    FROM x AS T,\n" +
                        "      (\n" +
                        "        SELECT DISTINCT R.tsMs / 60000 * 60000 as minutestamp\n" +
                        "        FROM x AS R\n" +
                        "        WHERE\n" +
                        "          R.ts BETWEEN '1970-01-03T00:00:33.303Z' AND '1970-01-04T09:03:33.303Z'\n" +
                        "          AND R.sym1 = 'A'\n" +
                        "        ORDER BY minutestamp ASC\n" +
                        "      ) checkpoint\n" +
                        "    WHERE\n" +
                        "      T.ts BETWEEN '1970-01-03T00:00:33.303Z' AND '1970-01-04T09:03:33.303Z'\n" +
                        "      AND T.sym1 = 'A'\n" +
                        "      AND T.sym2 like '%E%'\n" +
                        "    GROUP BY ts, sym2\n" +
                        "    ORDER BY ts ASC\n" +
                        "  ) as A\n" +
                        "GROUP BY ts, sym2, totalCost\n" +
                        "ORDER BY ts ASC",
                "create table x (\n" +
                        "  ts timestamp,\n" +
                        "  tsMs long,\n" +
                        "  sym1 symbol,\n" +
                        "  sym2 symbol,\n" +
                        "  cost float\n" +
                        ") timestamp(ts) partition by day",
                null,
                "insert into x select * from (select " +
                        "        timestamp_sequence(172800000000, 360000000) ts, \n" +
                        "        (x * 60000) tsMs, \n" +
                        "        rnd_symbol('A', 'B', 'C') sym1, \n" +
                        "        rnd_symbol('D', 'E', 'F') sym2, \n" +
                        "        rnd_double() cost \n" +
                        "    from long_sequence(20)) timestamp (ts)",
                "ts\tsym2\ttotalCost\n" +
                        "180000\tE\t1.7202\n" +
                        "300000\tE\t1.7202\n" +
                        "420000\tE\t1.7202\n" +
                        "1200000\tE\t1.7202\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testNoopGroupByWithFunction1() throws Exception {
        assertQuery(
                "column\tavg\n",
                "select b+a, avg(c) from x group by b+a",
                "create table x (\n" +
                        "    sym1 symbol,\n" +
                        "    sym2 symbol,\n" +
                        "    a double,\n" +
                        "    b double,\n" +
                        "    c double,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                null,
                "insert into x select * from (select " +
                        "         rnd_symbol('A', 'B', 'C') sym1, \n" +
                        "         rnd_symbol('D', 'E', 'F') sym2, \n" +
                        "        rnd_double() a, \n" +
                        "        rnd_double() b, \n" +
                        "        rnd_double() c, \n" +
                        "        timestamp_sequence(172800000000, 360000000) ts \n" +
                        "    from long_sequence(20)) timestamp (ts)",
                "column\tavg\n" +
                        "0.30949977657533256\t0.299199045961845\n" +
                        "1.4932004946738646\t0.9856290845874263\n" +
                        "1.1347848544029424\t0.7611029514995744\n" +
                        "0.5966743012271949\t0.2390529010846525\n" +
                        "0.9879110542701665\t0.38539947865244994\n" +
                        "0.6649002464931092\t0.7675673070796104\n" +
                        "0.7795990267808574\t0.6381607531178513\n" +
                        "1.4831535123369082\t0.12026122412833129\n" +
                        "1.234827286954693\t0.42281342727402726\n" +
                        "1.2962662695358191\t0.5522494170511608\n" +
                        "0.8268723676824133\t0.8847591603509142\n" +
                        "1.757029498695562\t0.8001121139739173\n" +
                        "1.0843141424360652\t0.456344569609078\n" +
                        "0.8195064672447426\t0.5659429139861241\n" +
                        "1.405167662413488\t0.9644183832564398\n" +
                        "0.693754621013657\t0.8164182592467494\n" +
                        "0.9820924616701128\t0.769238189433781\n" +
                        "0.9144934765891063\t0.6551335839796312\n" +
                        "0.7675889012481835\t0.9540069089049732\n" +
                        "0.9257619753148886\t0.19751370382305056\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testNoopGroupReferenceAggregate() throws Exception {
        assertException(
                "select a.sym1, avg(bid) avgBid from x a group by a.sym1, avgBid order by a.sym1",
                "create table x (\n" +
                        "    sym1 symbol,\n" +
                        "    sym2 symbol,\n" +
                        "    bid double,\n" +
                        "    ask double,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                57,
                "aggregate functions are not allowed in GROUP BY"
        );
    }

    @Test
    public void testNoopGroupReferenceNonKeyColumn() throws Exception {
        assertException(
                "select a.sym1, avg(bid) avgBid from x a group by a.sym2 order by a.sym1",
                "create table x (\n" +
                        "    sym1 symbol,\n" +
                        "    sym2 symbol,\n" +
                        "    bid double,\n" +
                        "    ask double,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                7,
                "column must appear in GROUP BY clause or aggregate function"
        );
    }

    @Test
    public void testSubQuery() throws Exception {
        assertQuery(
                "bkt\tavg\n",
                "select bkt, avg(bid) from (select abs(id % 10) bkt, bid from x) group by bkt",
                "create table x (\n" +
                        "    id long,\n" +
                        "    bid double\n" +
                        ") ",
                null,
                "insert into x select * from (select " +
                        "         rnd_long(), \n" +
                        "        rnd_double() \n" +
                        "    from long_sequence(20))",
                "bkt\tavg\n" +
                        "6\t0.7275909062911847\n" +
                        "5\t0.08486964232560668\n" +
                        "8\t0.5773046624150107\n" +
                        "4\t0.413662826357355\n" +
                        "2\t0.22452340856088226\n" +
                        "1\t0.33762525947485594\n" +
                        "3\t0.7715455271652294\n" +
                        "7\t0.47335449523280454\n" +
                        "0\t0.1911234617573182\n" +
                        "9\t0.5793466326862211\n",
                true,
                true,
                false
        );
    }
}
