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

/**
 * These tests cover distinct variations.
 */
public class DistinctTest extends AbstractCairoTest {

    @Test
    public void testDistinctImplementsLimitLoPositive() throws Exception {
        execute(
                "create table x as (" +
                        "  select" +
                        "    rnd_symbol('foo') sym," +
                        "    rnd_int() origin," +
                        "    rnd_int() event," +
                        "    timestamp_sequence(0, 0) created" +
                        "  from long_sequence(100)" +
                        ") timestamp(created);"
        );
        assertQueryAndPlan(
                "sym\torigin\tlag\n" +
                        "foo\t315515118\tnull\n" +
                        "foo\t73575701\t315515118\n" +
                        "foo\t592859671\t73575701\n" +
                        "foo\t-1191262516\t592859671\n" +
                        "foo\t-1575378703\t-1191262516\n",
                "Limit lo: 5 skip-over-rows: 0 limit: 5\n" +
                        "    Distinct\n" +
                        "      keys: sym,origin,lag\n" +
                        "      earlyExit: 5\n" +
                        "        Window\n" +
                        "          functions: [lag(origin, 1, NULL) over ()]\n" +
                        "            PageFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: x\n",
                "SELECT DISTINCT sym, origin, lag(origin) over() from x limit 5",
                null,
                false,
                true
        );

        assertQueryAndPlan(
                "sym\torigin\tlag\n" +
                        "foo\t2137969456\t264240638\n" +
                        "foo\t68265578\t2137969456\n" +
                        "foo\t44173540\t68265578\n" +
                        "foo\t-2144581835\t44173540\n" +
                        "foo\t-1162267908\t-2144581835\n" +
                        "foo\t-1575135393\t-1162267908\n" +
                        "foo\t326010667\t-1575135393\n" +
                        "foo\t-2034804966\t326010667\n",
                "Limit lo: 20 hi: 28 skip-over-rows: 20 limit: 8\n" +
                        "    Distinct\n" +
                        "      keys: sym,origin,lag\n" +
                        "      earlyExit: 28\n" +
                        "        Window\n" +
                        "          functions: [lag(origin, 1, NULL) over ()]\n" +
                        "            PageFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: x\n",
                "SELECT DISTINCT sym, origin, lag(origin) over() from x limit 20, 28",
                null,
                false,
                true
        );

        // no early exit
        assertQueryAndPlan(
                "sym\torigin\tlag\n" +
                        "foo\t874367915\t-1775036711\n" +
                        "foo\t1431775887\t874367915\n" +
                        "foo\t-1822590290\t1431775887\n" +
                        "foo\t957075831\t-1822590290\n" +
                        "foo\t-2043541236\t957075831\n",
                "Limit lo: -5 skip-over-rows: 95 limit: 5\n" +
                        "    Distinct\n" +
                        "      keys: sym,origin,lag\n" +
                        "        Window\n" +
                        "          functions: [lag(origin, 1, NULL) over ()]\n" +
                        "            PageFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: x\n",
                "SELECT DISTINCT sym, origin, lag(origin) over() from x limit -5",
                null,
                false,
                true
        );

        // no early exit
        assertQueryAndPlan(
                "sym\torigin\tlag\n" +
                        "foo\t315515118\tnull\n" +
                        "foo\t73575701\t315515118\n" +
                        "foo\t592859671\t73575701\n" +
                        "foo\t-1191262516\t592859671\n" +
                        "foo\t-1575378703\t-1191262516\n",
                "Limit lo: 5 skip-over-rows: 0 limit: 5\n" +
                        "    Sort\n" +
                        "      keys: [sym]\n" +
                        "        Distinct\n" +
                        "          keys: sym,origin,lag\n" +
                        "            Window\n" +
                        "              functions: [lag(origin, 1, NULL) over ()]\n" +
                        "                PageFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: x\n",
                "SELECT DISTINCT sym, origin, lag(origin) over() from x order by 1 limit 5",
                null,
                true,
                true
        );
    }

    @Test
    public void testDuplicateColumn() throws Exception {
        assertQuery(
                "e1\te2\n" +
                        "24814\t24814\n" +
                        "-13027\t-13027\n" +
                        "-22955\t-22955\n",
                "SELECT DISTINCT event e1, event e2 FROM x order by 1 desc;",
                "create table x as (" +
                        "  select" +
                        "    rnd_short() origin," +
                        "    rnd_short() event," +
                        "    timestamp_sequence(0, 0) created" +
                        "  from long_sequence(3)" +
                        ") timestamp(created);",
                null,
                true,
                true
        );
    }

    @Test
    public void testDuplicateColumnInWhereClauseSubQuery() throws Exception {
        assertQuery(
                "origin\tevent\tcreated\n" +
                        "-27056\ta\t1970-01-01T00:00:00.000000Z\n" +
                        "-11455\tc\t1970-01-01T00:00:00.000000Z\n" +
                        "-21227\tc\t1970-01-01T00:00:00.000000Z\n",
                "SELECT * FROM x WHERE event IN (SELECT * FROM (SELECT DISTINCT event, event FROM x));",
                "create table x as (" +
                        "  select" +
                        "    rnd_short() origin," +
                        "    rnd_symbol('a','b','c') event," +
                        "    timestamp_sequence(0, 0) created" +
                        "  from long_sequence(3)" +
                        ") timestamp(created);",
                "created",
                true,
                false
        );
    }

    @Test
    public void testDuplicateColumnWithSubQuery() throws Exception {
        assertQuery(
                "e1\te2\n" +
                        "-24814\t-24814\n" +
                        "13027\t13027\n" +
                        "22955\t22955\n",
                "SELECT DISTINCT event e1, event e2 FROM (SELECT origin, (-event) event FROM x) order by 1;",
                "create table x as (" +
                        "  select" +
                        "    rnd_short() origin," +
                        "    rnd_short() event," +
                        "    timestamp_sequence(0, 0) created" +
                        "  from long_sequence(3)" +
                        ") timestamp(created);",
                null,
                true,
                true
        );
    }

    @Test
    public void testDuplicateColumnWithUnion() throws Exception {
        assertQuery(
                "e1\te2\n" +
                        "42\t42\n" +
                        "-22955\t-22955\n" +
                        "-13027\t-13027\n" +
                        "24814\t24814\n",
                "(SELECT 42 e1, 42 e2) UNION (SELECT DISTINCT event e1, event e2 FROM x order by 1);",
                "create table x as (" +
                        "  select" +
                        "    rnd_short() origin," +
                        "    rnd_short() event," +
                        "    timestamp_sequence(0, 0) created" +
                        "  from long_sequence(3)" +
                        ") timestamp(created);",
                null,
                false,
                false
        );
    }

    @Test
    public void testDuplicateCount() throws Exception {
        assertQuery(
                "count\tcount1\n" +
                        "10\t10\n",
                "SELECT DISTINCT count(*), count(*) FROM x;",
                "create table x as (" +
                        "  select" +
                        "    rnd_short() origin," +
                        "    rnd_short() event," +
                        "    timestamp_sequence(0, 0) created" +
                        "  from long_sequence(10)" +
                        ") timestamp(created);",
                null,
                false,
                true
        );
    }

    @Test
    public void testDuplicateCount2() throws Exception {
        assertQuery(
                "sym\tcount\tcount1\n" +
                        "foo\t10\t10\n",
                "SELECT DISTINCT sym, count(*), count(*) FROM x;",
                "create table x as (" +
                        "  select" +
                        "    rnd_symbol('foo') sym," +
                        "    rnd_short() origin," +
                        "    rnd_short() event," +
                        "    timestamp_sequence(0, 0) created" +
                        "  from long_sequence(10)" +
                        ") timestamp(created);",
                null,
                false,
                true
        );
    }

    @Test
    public void testDuplicateCountNested() throws Exception {
        assertQuery(
                "count\tcount1\n" +
                        "10\t10\n",
                "SELECT * FROM (SELECT DISTINCT count(*), count(*) FROM x);",
                "create table x as (" +
                        "  select" +
                        "    rnd_short() origin," +
                        "    rnd_short() event," +
                        "    timestamp_sequence(0, 0) created" +
                        "  from long_sequence(10)" +
                        ") timestamp(created);",
                null,
                false,
                true
        );
    }
}
