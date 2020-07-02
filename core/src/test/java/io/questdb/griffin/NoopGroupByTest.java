/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.Rnd;
import org.junit.Before;
import org.junit.Test;

public class NoopGroupByTest extends AbstractGriffinTest {

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testNoopGroupByFailureWhenUsing1KeyInSelectStatementBut2InGroupBy() throws Exception {
        assertFailure(
                "select sym1, avg(bid) avgBid from x where sym1 in ('AA', 'BB' ) group by sym1, sym2",
                "create table x (\n" +
                        "    sym1 symbol,\n" +
                        "    sym2 symbol,\n" +
                        "    bid int,\n" +
                        "    ask int\n" +
                        ")  partition by NONE",
                0,
                "group by column does not match key column is select statement "
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
                        ")  partition by NONE",
                0,
                "group by column does not match key column is select statement "
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
                        ")  partition by NONE",
                0,
                "group by column does not match key column is select statement "
        );
    }

    @Test
    public void testNoopGroupByFailureWhenUsingAliasedColumn() throws Exception {
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
                true
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
                        ")  partition by NONE",
                0,
                "group by column does not match key column is select statement "
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
                        ")  partition by NONE",
                0,
                "group by column does not match key column is select statement "
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
                true
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
                true
        );
    }
}
