/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin;

import org.junit.Test;

public class NoopGroupByTest extends AbstractGriffinTest {

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
                true
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
                true
        );
    }

    @Test
    public void testNoopGroupByBindVariable() throws Exception {
        compiler.compile("create table y(id int, ref int, val double)", sqlExecutionContext);
        engine.releaseAllWriters();
        assertFailure(
                "select x.id, x.ref, y.ref, sum(val) from x join y on (id) group by x.id, :var, y.ref",
                "create table x (id int, ref int, ref3 int)",
                73, "literal expected"
        );
    }

    //with where clause
    @Test
    public void testNoopGroupByFailureWhenUsing1KeyInSelectStatementBut2InGroupBy() throws Exception {
        assertFailure(
                "select sym1, avg(bid) avgBid from x where sym1 in ('AA', 'BB' ) group by sym1, sym2",
                "create table x (\n" +
                        "    sym1 symbol,\n" +
                        "    sym2 symbol,\n" +
                        "    bid int,\n" +
                        "    ask int\n" +
                        ")",
                79,
                "group by column does not match any key column is select statement"
        );
    }

    @Test
    public void testNoopGroupByFailureWhenUsing2KeysInSelectStatementButOnlyOneInGroupByV1() throws Exception {
        assertFailure(
                "select sym1, sym2, avg(bid) avgBid from x where sym1 in ('AA', 'BB' ) group by sym1",
                "create table x (\n" +
                        "    sym1 symbol,\n" +
                        "    sym2 symbol,\n" +
                        "    bid int,\n" +
                        "    ask int\n" +
                        ")",
                7,
                "not enough columns in group by"
        );
    }

    @Test
    public void testNoopGroupByFailureWhenUsing2KeysInSelectStatementButOnlyOneInGroupByV2() throws Exception {
        assertFailure(
                "select sym1, sym2, avg(bid) avgBid from x where sym1 in ('AA', 'BB' ) group by sym2",
                "create table x (\n" +
                        "    sym1 symbol,\n" +
                        "    sym2 symbol,\n" +
                        "    bid int,\n" +
                        "    ask int\n" +
                        ")",
                7,
                "not enough columns in group by"
        );
    }

    @Test
    public void testNoopGroupByFailureWhenUsingAliasedColumnAndWrongTableAlias() throws Exception {
        assertFailure(
                "select sym ccy, avg(bid) avgBid from x a where sym in ('AA', 'BB' ) group by b.ccy",
                "create table x (\n" +
                        "    sym symbol,\n" +
                        "    bid int,\n" +
                        "    ask int\n" +
                        ")",
                77,
                "invalid column reference"
        );
    }

    @Test
    public void testNoopGroupByFailureWhenUsingFunctionColumn() throws Exception {
        assertFailure(
                "select sym, avg(bid) avgBid from x where sym in ('AA', 'BB' ) group by avgBid",
                "create table x (\n" +
                        "    sym symbol,\n" +
                        "    bid int,\n" +
                        "    ask int\n" +
                        ")",
                71,
                "group by column references aggregate expression"
        );
    }

    @Test
    public void testNoopGroupByFailureWhenUsingInvalidColumn() throws Exception {
        assertFailure(
                "select sym, avg(bid) avgBid from x where sym in ('AA', 'BB' ) group by badColumn",
                "create table x (\n" +
                        "    sym symbol,\n" +
                        "    bid int,\n" +
                        "    ask int\n" +
                        ")",
                71,
                "group by column does not match any key column is select statement"
        );
    }

    @Test
    public void testNoopGroupByInvalidColumnName1() throws Exception {
        assertFailure(
                "select a.sym1, avg(bid) avgBid from x a group by b.rubbish",
                "create table x (\n" +
                        "    sym1 symbol,\n" +
                        "    sym2 symbol,\n" +
                        "    bid double,\n" +
                        "    ask double,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                49,
                "invalid column reference"
        );
    }

    @Test
    public void testNoopGroupByInvalidColumnName2() throws Exception {
        assertFailure(
                "select a.sym1, avg(bid) avgBid from x a group by b.sym1",
                "create table x (\n" +
                        "    sym1 symbol,\n" +
                        "    sym2 symbol,\n" +
                        "    bid double,\n" +
                        "    ask double,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                49,
                "invalid column reference"
        );
    }

    @Test
    public void testNoopGroupByJoinArithmetic() throws Exception {
        compiler.compile("create table y(id int, ref int, val double)", sqlExecutionContext);
        engine.releaseAllWriters();
        assertQuery(
                "id\tcolumn\tsum\n",
                "select x.id, x.ref + y.ref, sum(val) from x join y on (id) group by x.id, x.ref + y.ref",
                "create table x (id int, ref int, ref3 int)",
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testNoopGroupByJoinBadArithmetic() throws Exception {
        compiler.compile("create table y(id int, ref int, val double)", sqlExecutionContext);
        engine.releaseAllWriters();
        assertFailure(
                "select x.id, x.ref - y.ref, sum(val) from x join y on (id) group by x.id, y.ref - x.ref",
                "create table x (id int, ref int, ref3 int)",
                80,
                "group by expression does not match anything select in statement"
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
                true
        );
    }

    @Test
    public void testNoopGroupByValidColumnNameWithHourFunctionAndAliasedTable() throws Exception {
        assertQuery(
                "hour\tavgBid\n",
                //select hour(pickup_datetime), sum(passenger_count) from trips group by hour(pickup_datetime);
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
                true
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
                true
        );
    }

    @Test
    public void testNoopGroupByWhenUsingAliasedColumnAndAliasedTable() throws Exception {
        assertQuery(
                "ccy\tavgBid\n",
                "select sym1 ccy, avg(bid) avgBid from x a where sym1 in ('A', 'B' ) group by a.ccy",
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
                true
        );
    }

    @Test
    public void testNoopGroupByJoinConst() throws Exception {
        compiler.compile("create table y(id int, ref int, val double)", sqlExecutionContext);
        engine.releaseAllWriters();
        assertFailure(
                "select 'x' z, sum(val) from x join y on (id) group by 'x'",
                "create table x (id int, ref int, ref3 int)",
                54,
                "group by expression does not match anything select in statement"
        );
    }

    @Test
    public void testNoopGroupByJoinInvalidConst() throws Exception {
        compiler.compile("create table y(id int, ref int, val double)", sqlExecutionContext);
        engine.releaseAllWriters();
        assertFailure(
                "select 'x' z, sum(val) from x join y on (id) group by 'y'",
                "create table x (id int, ref int, ref3 int)",
                54,
                "group by expression does not match anything select in statement"
        );
    }

    @Test
    public void testNoopGroupByJoinReference() throws Exception {
        compiler.compile("create table y(id int, ref int, val double)", sqlExecutionContext);
        engine.releaseAllWriters();
        assertQuery(
                "id\tref\tref1\tsum\n",
                "select x.id, x.ref, y.ref, sum(val) from x join y on (id) group by x.id, x.ref, y.ref",
                "create table x (id int, ref int, ref3 int)",
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testNoopGroupByJoinReferenceNonSelected() throws Exception {
        compiler.compile("create table y(id int, ref int, val double)", sqlExecutionContext);
        engine.releaseAllWriters();
        assertFailure(
                "select x.id, x.ref, y.ref, sum(val) from x join y on (id) group by x.id, x.ref3, y.ref",
                "create table x (id int, ref int, ref3 int)",
                73, "invalid column reference"
        );
    }

    @Test
    public void testNoopGroupByMissingColumnWithTableAlias1() throws Exception {
        assertFailure(
                "select a.sym1, a.sym2, avg(bid) avgBid from x a group by a.sym1", //a.sym2 is missing in group by clause
                "create table x (\n" +
                        "    sym1 symbol,\n" +
                        "    sym2 symbol,\n" +
                        "    bid double,\n" +
                        "    ask double,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                7,
                "not enough columns in group by"
        );
    }

    @Test
    public void testNoopGroupByWhenUsingAliasedColumnAndAliasedTable2() throws Exception {
        assertFailure(
                "select sym1 ccy, avg(bid) avgBid from x a where sym1 in ('A', 'B' ) group by b.ccy",
                "create table x (\n" +
                        "    sym1 symbol,\n" +
                        "    sym2 symbol,\n" +
                        "    bid double,\n" +
                        "    ask double,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                77,
                "invalid column reference"
        );
    }

    @Test
    public void testNoopGroupReferenceAggregate() throws Exception {
        assertFailure(
                "select a.sym1, avg(bid) avgBid from x a group by a.sym1, avgBid order by a.sym1",
                "create table x (\n" +
                        "    sym1 symbol,\n" +
                        "    sym2 symbol,\n" +
                        "    bid double,\n" +
                        "    ask double,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                57,
                "group by column references aggregate expression"
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
                true
        );
    }

    @Test
    public void testNoopGroupReferenceNonKeyColumn() throws Exception {
        assertFailure(
                "select a.sym1, avg(bid) avgBid from x a group by a.sym2 order by a.sym1",
                "create table x (\n" +
                        "    sym1 symbol,\n" +
                        "    sym2 symbol,\n" +
                        "    bid double,\n" +
                        "    ask double,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                49,
                "group by column does not match any key column is select statement"
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
                true
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
                true
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
                true
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
                true
        );
    }
}
