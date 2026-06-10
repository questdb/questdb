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
import org.junit.Test;

public class NoOpGroupByTest extends AbstractCairoTest {

    @Test
    public void testMissingGroupByWithHourFunction() throws Exception {
        assertQuery("select hour(ts), avg(bid) avgBid from x order by hour")
                .ddl("""
                        create table x (
                            sym1 symbol,
                            sym2 symbol,
                            bid double,
                            ask double,
                            ts timestamp
                        ) timestamp(ts) partition by DAY""")
                .mutateWith("""
                        insert into x select * from (select \
                                 rnd_symbol('A', 'B', 'C') sym1,\s
                                 rnd_symbol('D', 'E', 'F') sym2,\s
                                rnd_double() bid,\s
                                rnd_double() ask,\s
                                timestamp_sequence(172800000000, 360000000) ts\s
                            from long_sequence(20)) timestamp (ts)""")
                .expectSize()
                .returns("hour\tavgBid\n", """
                        hour\tavgBid
                        0\t0.47607185409853914
                        1\t0.6861237948732989
                        """);
    }

    @Test
    public void testNoopGroupByBindVariable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y (id int, ref int, val double)");
            engine.releaseAllWriters();
            assertQuery("select x.id, x.ref, y.ref, sum(val) from x join y on (id) group by x.id, :var, y.ref")
                    .ddl("create table x (id int, ref int, ref3 int)")
                    .noLeakCheck()
                    .fails(73, "literal expected");
        });
    }

    // with where clause
    @Test
    public void testNoopGroupByFailureWhenUsing1KeyInSelectStatementBut2InGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x ( " +
                    "    sym1 symbol," +
                    "    sym2 symbol," +
                    "    bid int," +
                    "    ask int )");
            engine.releaseAllWriters();

            String query = "select sym1, avg(bid) avgBid " +
                    "from x " +
                    "where sym1 in ('AA', 'BB' ) " +
                    "group by sym1, sym2";
            assertQuery(query)
                    .noLeakCheck()
                    .withPlan("""
                            VirtualRecord
                              functions: [sym1,avgBid]
                                Async JIT Group By workers: 1
                                  keys: [sym1,sym2]
                                  values: [avg(bid)]
                                  filter: sym1 in [AA,BB]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: x
                            """)
                    .returns("sym1\tavgBid\n");
        });
    }

    @Test
    public void testNoopGroupByFailureWhenUsing2KeysInSelectStatementButOnlyOneInGroupByV1() throws Exception {
        assertQuery("select sym1, sym2, avg(bid) avgBid from x where sym1 in ('AA', 'BB' ) group by sym1")
                .ddl("""
                        create table x (
                            sym1 symbol,
                            sym2 symbol,
                            bid int,
                            ask int
                        )""")
                .fails(13, "column must appear in GROUP BY clause or aggregate function");
    }

    @Test
    public void testNoopGroupByFailureWhenUsing2KeysInSelectStatementButOnlyOneInGroupByV2() throws Exception {
        assertQuery("select sym1, sym2, avg(bid) avgBid from x where sym1 in ('AA', 'BB' ) group by sym2")
                .ddl("""
                        create table x (
                            sym1 symbol,
                            sym2 symbol,
                            bid int,
                            ask int
                        )""")
                .fails(7, "column must appear in GROUP BY clause or aggregate function");
    }

    @Test
    public void testNoopGroupByFailureWhenUsingAliasedColumnAndWrongTableAlias() throws Exception {
        assertQuery("select sym ccy, avg(bid) avgBid from x a where sym in ('AA', 'BB' ) group by b.ccy")
                .ddl("""
                        create table x (
                            sym symbol,
                            bid int,
                            ask int
                        )""")
                .fails(77, "Invalid table name or alias");
    }

    @Test
    public void testNoopGroupByFailureWhenUsingFunctionColumn() throws Exception {
        assertQuery("select sym, avg(bid) avgBid from x where sym in ('AA', 'BB' ) group by avgBid")
                .ddl("""
                        create table x (
                            sym symbol,
                            bid int,
                            ask int
                        )""")
                .fails(71, "aggregate functions are not allowed in GROUP BY");
    }

    @Test
    public void testNoopGroupByFailureWhenUsingInvalidColumn() throws Exception {
        assertQuery("select sym, avg(bid) avgBid from x where sym in ('AA', 'BB' ) group by badColumn")
                .ddl("""
                        create table x (
                            sym symbol,
                            bid int,
                            ask int
                        )""")
                .fails(71, "Invalid column: badColumn");
    }

    @Test
    public void testNoopGroupByInvalidColumnName1() throws Exception {
        assertQuery("select a.sym1, avg(bid) avgBid from x a group by b.rubbish")
                .ddl("""
                        create table x (
                            sym1 symbol,
                            sym2 symbol,
                            bid double,
                            ask double,
                            ts timestamp
                        ) timestamp(ts) partition by DAY""")
                .fails(49, "Invalid table name or alias");
    }

    @Test
    public void testNoopGroupByInvalidColumnName2() throws Exception {
        assertQuery("select a.sym1, avg(bid) avgBid from x a group by b.sym1")
                .ddl("""
                        create table x (
                            sym1 symbol,
                            sym2 symbol,
                            bid double,
                            ask double,
                            ts timestamp
                        ) timestamp(ts) partition by DAY""")
                .fails(49, "Invalid table name or alias");
    }

    @Test
    public void testNoopGroupByJoinArithmetic() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (id int, ref int, ref3 int)");
            execute("insert into x values (1,1,1), (1,2,2);");
            execute("create table y (id int, ref int, val double)");
            execute("insert into y values (1,1,1), (1,2,2);");
            engine.releaseAllWriters();
            assertQuery("select x.id, x.ref + y.ref, sum(val) from x join y on (id) group by x.id, x.ref + y.ref order by 1, 2")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tcolumn\tsum
                            1\t2\t1.0
                            1\t3\t3.0
                            1\t4\t2.0
                            """);
        });
    }

    @Test
    public void testNoopGroupByJoinBadArithmetic() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y(id int, ref int, val double)");
            execute("create table x (id int, ref int, ref3 int)");
            engine.releaseAllWriters();
            assertQuery("select x.id, x.ref - y.ref, sum(val) from x join y on (id) group by x.id, y.ref - x.ref")
                    .noLeakCheck()
                    .fails(21, "column must appear in GROUP BY clause or aggregate function");
        });
    }

    @Test
    public void testNoopGroupByJoinConst() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (id int, ref int, ref3 int)");
            execute("create table y(id int, ref int, val double)");
            engine.releaseAllWriters();
            assertQuery("select 'x' z, sum(val) from x join y on (id) group by 'x'")
                    .noLeakCheck()
                    .returns("z\tsum\n");
        });
    }

    @Test
    public void testNoopGroupByJoinReference() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y(id int, ref int, val double)");
            engine.releaseAllWriters();
            assertQuery("select x.id, x.ref, y.ref, sum(val) from x join y on (id) group by x.id, x.ref, y.ref")
                    .noLeakCheck()
                    .ddl("create table x (id int, ref int, ref3 int)")
                    .expectSize()
                    .returns("id\tref\tref1\tsum\n");
        });
    }

    @Test
    public void testNoopGroupByJoinReferenceNonSelected() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y(id int, ref int, val double)");
            engine.releaseAllWriters();
            assertQuery("select x.id, x.ref, y.ref, sum(val) from x join y on (id) group by x.id, x.ref3, y.ref")
                    .ddl("create table x (id int, ref int, ref3 int)")
                    .noLeakCheck()
                    .fails(13, "column must appear in GROUP BY clause or aggregate function");
        });
    }

    @Test
    public void testNoopGroupByJoinStringConst() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (id int, ref int, ref3 int)");
            execute("create table y(id int, ref int, val double)");
            engine.releaseAllWriters();
            String query = "select 'x' z, sum(val) from x join y on (id) group by 'y'";
            assertQuery(query)
                    .noLeakCheck()
                    .returns("z\tsum\n");
        });
    }

    @Test
    public void testNoopGroupByMissingColumnWithTableAlias1() throws Exception {
        assertQuery("select a.sym1, a.sym2, avg(bid) avgBid from x a group by a.sym1")
                .ddl(//a.sym2 is missing in group by clause
                        """
                                create table x (
                                    sym1 symbol,
                                    sym2 symbol,
                                    bid double,
                                    ask double,
                                    ts timestamp
                                ) timestamp(ts) partition by DAY""")
                .fails(15, "column must appear in GROUP BY clause or aggregate function");
    }

    @Test
    public void testNoopGroupByValidColumnName() throws Exception {
        assertQuery("select a.sym1, avg(bid) avgBid from x a group by a.sym1 order by a.sym1")
                .ddl("""
                        create table x (
                            sym1 symbol,
                            sym2 symbol,
                            bid double,
                            ask double,
                            ts timestamp
                        ) timestamp(ts) partition by DAY""")
                .mutateWith("""
                        insert into x select * from (select \
                                 rnd_symbol('A', 'B', 'C') sym1,\s
                                 rnd_symbol('D', 'E', 'F') sym2,\s
                                rnd_double() bid,\s
                                rnd_double() ask,\s
                                timestamp_sequence(172800000000, 360000000) ts\s
                            from long_sequence(20)) timestamp (ts)""")
                .expectSize()
                .returns("sym1\tavgBid\n", """
                        sym1\tavgBid
                        A\t0.5942181417903911
                        B\t0.7080299543021055
                        C\t0.4760584891454253
                        """);
    }

    @Test
    public void testNoopGroupByValidColumnNameWithHourFunction() throws Exception {
        assertQuery(// select hour(pickup_datetime), sum(passenger_count) from trips group by hour(pickup_datetime);
                "select hour(ts), avg(bid) avgBid from x group by hour(ts) order by hour")
                .ddl("""
                        create table x (
                            sym1 symbol,
                            sym2 symbol,
                            bid double,
                            ask double,
                            ts timestamp
                        ) timestamp(ts) partition by DAY""")
                .mutateWith("""
                        insert into x select * from (select \
                                 rnd_symbol('A', 'B', 'C') sym1,\s
                                 rnd_symbol('D', 'E', 'F') sym2,\s
                                rnd_double() bid,\s
                                rnd_double() ask,\s
                                timestamp_sequence(172800000000, 360000000) ts\s
                            from long_sequence(20)) timestamp (ts)""")
                .expectSize()
                .returns("hour\tavgBid\n", """
                        hour\tavgBid
                        0\t0.47607185409853914
                        1\t0.6861237948732989
                        """);
    }

    @Test
    public void testNoopGroupByValidColumnNameWithHourFunctionAndAliasedTable() throws Exception {
        assertQuery("select hour(a.ts), avg(bid) avgBid from x a group by hour(a.ts) order by hour")
                .ddl("""
                        create table x (
                            sym1 symbol,
                            sym2 symbol,
                            bid double,
                            ask double,
                            ts timestamp
                        ) timestamp(ts) partition by DAY""")
                .mutateWith("""
                        insert into x select * from (select \
                                 rnd_symbol('A', 'B', 'C') sym1,\s
                                 rnd_symbol('D', 'E', 'F') sym2,\s
                                rnd_double() bid,\s
                                rnd_double() ask,\s
                                timestamp_sequence(172800000000, 360000000) ts\s
                            from long_sequence(20)) timestamp (ts)""")
                .expectSize()
                .returns("hour\tavgBid\n", """
                        hour\tavgBid
                        0\t0.47607185409853914
                        1\t0.6861237948732989
                        """);
    }

    @Test
    public void testNoopGroupByWhenUsingAliasedColumn() throws Exception {
        assertQuery("select sym1 ccy, avg(bid) avgBid from x where sym1 in ('A', 'B' ) group by ccy order by ccy")
                .ddl("""
                        create table x (
                            sym1 symbol,
                            sym2 symbol,
                            bid double,
                            ask double,
                            ts timestamp
                        ) timestamp(ts) partition by DAY""")
                .mutateWith("""
                        insert into x select * from (select \
                                 rnd_symbol('A', 'B', 'C') sym1,\s
                                 rnd_symbol('D', 'E', 'F') sym2,\s
                                rnd_double() bid,\s
                                rnd_double() ask,\s
                                timestamp_sequence(172800000000, 360000000) ts\s
                            from long_sequence(20)) timestamp (ts)""")
                .expectSize()
                .returns("ccy\tavgBid\n", """
                        ccy\tavgBid
                        A\t0.5942181417903911
                        B\t0.7080299543021055
                        """);
    }

    @Test // sym1 is aliased as ccy at stage later than group by
    public void testNoopGroupByWhenUsingAliasedColumnAndAliasedTable() throws Exception {
        assertQuery("select sym1 as ccy, avg(bid) avgBid from x a where sym1 in ('A', 'B' ) group by a.ccy")
                .ddl("""
                        create table x (
                            sym1 symbol,
                            sym2 symbol,
                            bid double,
                            ask double,
                            ts timestamp
                        ) timestamp(ts) partition by DAY""")
                .fails(80, "Invalid column: a.ccy");
    }

    @Test
    public void testNoopGroupByWhenUsingAliasedColumnAndAliasedTable2() throws Exception {
        assertQuery("select sym1 ccy, avg(bid) avgBid from x a where sym1 in ('A', 'B' ) group by b.ccy")
                .ddl("""
                        create table x (
                            sym1 symbol,
                            sym2 symbol,
                            bid double,
                            ask double,
                            ts timestamp
                        ) timestamp(ts) partition by DAY""")
                .fails(77, "Invalid table name or alias");
    }

    @Test
    public void testNoopGroupByWhenUsingOriginalColumnAndAliasedTable() throws Exception {
        assertQuery("select sym1 as ccy, avg(bid) avgBid from x a where sym1 in ('A', 'B' ) group by a.sym1 order by 1")
                .ddl("""
                        create table x (
                            sym1 symbol,
                            sym2 symbol,
                            bid double,
                            ask double,
                            ts timestamp
                        ) timestamp(ts) partition by DAY""")
                .mutateWith("""
                        insert into x select * from (select \
                                 rnd_symbol('A', 'B', 'C') sym1,\s
                                 rnd_symbol('D', 'E', 'F') sym2,\s
                                rnd_double() bid,\s
                                rnd_double() ask,\s
                                timestamp_sequence(172800000000, 360000000) ts\s
                            from long_sequence(20)) timestamp (ts)""")
                .expectSize()
                .returns("ccy\tavgBid\n", """
                        ccy\tavgBid
                        A\t0.5942181417903911
                        B\t0.7080299543021055
                        """);
    }

    @Test
    public void testNoopGroupByWith1Syms() throws Exception {
        assertQuery("select sym1, avg(bid) avgBid from x where sym1 in ('A', 'B' ) group by sym1 order by sym1")
                .ddl("""
                        create table x (
                            sym1 symbol,
                            sym2 symbol,
                            bid double,
                            ask double,
                            ts timestamp
                        ) timestamp(ts) partition by DAY""")
                .mutateWith("""
                        insert into x select * from (select \
                                 rnd_symbol('A', 'B', 'C') sym1,\s
                                 rnd_symbol('D', 'E', 'F') sym2,\s
                                rnd_double() bid,\s
                                rnd_double() ask,\s
                                timestamp_sequence(172800000000, 360000000) ts\s
                            from long_sequence(20)) timestamp (ts)""")
                .expectSize()
                .returns("sym1\tavgBid\n", """
                        sym1\tavgBid
                        A\t0.5942181417903911
                        B\t0.7080299543021055
                        """);
    }

    @Test
    public void testNoopGroupByWith2Syms() throws Exception {
        assertQuery("select sym1, sym2, avg(bid) avgBid from x where sym1 in ('A', 'B' ) group by sym1, sym2 order by sym1, sym2")
                .ddl("""
                        create table x (
                            sym1 symbol,
                            sym2 symbol,
                            bid double,
                            ask double,
                            ts timestamp
                        ) timestamp(ts) partition by DAY""")
                .mutateWith("""
                        insert into x select * from (select \
                                 rnd_symbol('A', 'B', 'C') sym1,\s
                                 rnd_symbol('D', 'E', 'F') sym2,\s
                                rnd_double() bid,\s
                                rnd_double() ask,\s
                                timestamp_sequence(172800000000, 360000000) ts\s
                            from long_sequence(20)) timestamp (ts)""")
                .expectSize()
                .returns("sym1\tsym2\tavgBid\n", """
                        sym1\tsym2\tavgBid
                        A\tD\t0.47381585528154324
                        A\tE\t0.5837537495691357
                        A\tF\t0.8664158914718532
                        B\tD\t0.8434630350290969
                        B\tE\t0.6403134139386097
                        """);
    }

    @Test
    public void testNoopGroupByWithAlias() throws Exception {
        assertQuery("select sym1, avg(bid) avgBid from x a where sym1 in ('A', 'B' ) group by a.sym1 order by sym1")
                .ddl("""
                        create table x (
                            sym1 symbol,
                            sym2 symbol,
                            bid double,
                            ask double,
                            ts timestamp
                        ) timestamp(ts) partition by DAY""")
                .mutateWith("""
                        insert into x select * from (select \
                                 rnd_symbol('A', 'B', 'C') sym1,\s
                                 rnd_symbol('D', 'E', 'F') sym2,\s
                                rnd_double() bid,\s
                                rnd_double() ask,\s
                                timestamp_sequence(172800000000, 360000000) ts\s
                            from long_sequence(20)) timestamp (ts)""")
                .expectSize()
                .returns("sym1\tavgBid\n", """
                        sym1\tavgBid
                        A\t0.5942181417903911
                        B\t0.7080299543021055
                        """);
    }

    @Test
    public void testNoopGroupByWithCrossJoinAndFilterOnSymbolColumn() throws Exception {
        assertQuery("""
                SELECT A.ts, A.sym2, A.totalCost
                FROM (
                    SELECT checkpoint.minutestamp AS ts, T.sym2, sum(T.cost) AS totalCost
                    FROM x AS T,
                      (
                        SELECT DISTINCT R.tsMs / 60000 * 60000 as minutestamp
                        FROM x AS R
                        WHERE
                          R.ts BETWEEN '1970-01-03T00:00:33.303Z' AND '1970-01-04T09:03:33.303Z'
                          AND R.sym1 = 'A'
                        ORDER BY minutestamp ASC
                      ) checkpoint
                    WHERE
                      T.ts BETWEEN '1970-01-03T00:00:33.303Z' AND '1970-01-04T09:03:33.303Z'
                      AND T.sym1 = 'A'
                      AND T.sym2 like '%E%'
                    GROUP BY ts, sym2
                    ORDER BY ts ASC
                  ) as A
                GROUP BY ts, sym2, totalCost
                ORDER BY ts ASC""")
                .ddl("""
                        create table x (
                          ts timestamp,
                          tsMs long,
                          sym1 symbol,
                          sym2 symbol,
                          cost float
                        ) timestamp(ts) partition by day""")
                .mutateWith("""
                        insert into x select * from (select \
                                timestamp_sequence(172800000000, 360000000) ts,\s
                                (x * 60000) tsMs,\s
                                rnd_symbol('A', 'B', 'C') sym1,\s
                                rnd_symbol('D', 'E', 'F') sym2,\s
                                rnd_double() cost\s
                            from long_sequence(20)) timestamp (ts)""")
                .expectSize()
                .returns("ts\tsym2\ttotalCost\n", """
                        ts\tsym2\ttotalCost
                        180000\tE\t1.7201583
                        300000\tE\t1.7201583
                        420000\tE\t1.7201583
                        1200000\tE\t1.7201583
                        """);
    }

    @Test
    public void testNoopGroupByWithFunction1() throws Exception {
        assertQuery("select b+a, avg(c) from x group by b+a order by b+a")
                .ddl("""
                        create table x (
                            sym1 symbol,
                            sym2 symbol,
                            a double,
                            b double,
                            c double,
                            ts timestamp
                        ) timestamp(ts) partition by DAY""")
                .mutateWith("""
                        insert into x select * from (select \
                                 rnd_symbol('A', 'B', 'C') sym1,\s
                                 rnd_symbol('D', 'E', 'F') sym2,\s
                                rnd_double() a,\s
                                rnd_double() b,\s
                                rnd_double() c,\s
                                timestamp_sequence(172800000000, 360000000) ts\s
                            from long_sequence(20)) timestamp (ts)""")
                .expectSize()
                .returns("column\tavg\n", """
                        column\tavg
                        0.30949977657533256\t0.299199045961845
                        0.5966743012271949\t0.2390529010846525
                        0.6649002464931092\t0.7675673070796104
                        0.693754621013657\t0.8164182592467494
                        0.7675889012481835\t0.9540069089049732
                        0.7795990267808574\t0.6381607531178513
                        0.8195064672447426\t0.5659429139861241
                        0.8268723676824133\t0.8847591603509142
                        0.9144934765891063\t0.6551335839796312
                        0.9257619753148886\t0.19751370382305056
                        0.9820924616701128\t0.769238189433781
                        0.9879110542701665\t0.38539947865244994
                        1.0843141424360652\t0.456344569609078
                        1.1347848544029424\t0.7611029514995744
                        1.234827286954693\t0.42281342727402726
                        1.2962662695358191\t0.5522494170511608
                        1.405167662413488\t0.9644183832564398
                        1.4831535123369082\t0.12026122412833129
                        1.4932004946738646\t0.9856290845874263
                        1.757029498695562\t0.8001121139739173
                        """);
    }

    @Test
    public void testNoopGroupReferenceAggregate() throws Exception {
        assertQuery("select a.sym1, avg(bid) avgBid from x a group by a.sym1, avgBid order by a.sym1")
                .ddl("""
                        create table x (
                            sym1 symbol,
                            sym2 symbol,
                            bid double,
                            ask double,
                            ts timestamp
                        ) timestamp(ts) partition by DAY""")
                .fails(57, "aggregate functions are not allowed in GROUP BY");
    }

    @Test
    public void testNoopGroupReferenceNonKeyColumn() throws Exception {
        assertQuery("select a.sym1, avg(bid) avgBid from x a group by a.sym2 order by a.sym1")
                .ddl("""
                        create table x (
                            sym1 symbol,
                            sym2 symbol,
                            bid double,
                            ask double,
                            ts timestamp
                        ) timestamp(ts) partition by DAY""")
                .fails(7, "column must appear in GROUP BY clause or aggregate function");
    }

    @Test
    public void testSubQuery() throws Exception {
        assertQuery("select bkt, avg(bid) from (select abs(id % 10) bkt, bid from x) group by bkt order by bkt")
                .ddl("""
                        create table x (
                            id long,
                            bid double
                        )\s""")
                .mutateWith("""
                        insert into x select * from (select \
                                 rnd_long(),\s
                                rnd_double()\s
                            from long_sequence(20))""")
                .expectSize()
                .returns("bkt\tavg\n", """
                        bkt\tavg
                        0\t0.1911234617573182
                        1\t0.33762525947485594
                        2\t0.22452340856088226
                        3\t0.7715455271652294
                        4\t0.413662826357355
                        5\t0.08486964232560668
                        6\t0.7275909062911847
                        7\t0.47335449523280454
                        8\t0.5773046624150107
                        9\t0.5793466326862211
                        """);
    }
}
