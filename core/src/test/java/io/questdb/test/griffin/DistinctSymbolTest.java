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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class DistinctSymbolTest extends AbstractCairoTest {

    @Test
    public void testDistinctSymbolIndexed() throws Exception {
        assertQuery(
                "sym\n" +
                        "2020-01-01\n" +
                        "2020-01-02\n" +
                        "2020-01-03\n" +
                        "2020-01-04\n" +
                        "2020-01-05\n" +
                        "2020-01-06\n" +
                        "2020-01-07\n" +
                        "2020-01-08\n" +
                        "2020-01-09\n" +
                        "2020-01-10\n",
                "select DISTINCT(sym) from tab order by 1 LIMIT 10",
                "create table tab as (select timestamp_sequence('2020-01-01', 10 * 60 * 1000000L) ts, cast(" +
                        "to_str(timestamp_sequence('2020-01-01', 10 * 60 * 1000000L), 'yyyy-MM-dd')" +
                        " as symbol) sym from long_sequence(10000)), index(sym) timestamp(ts) PARTITION BY MONTH",
                null,
                "alter table tab drop partition list '2020-01'",
                "sym\n" +
                        "2020-02-01\n" +
                        "2020-02-02\n" +
                        "2020-02-03\n" +
                        "2020-02-04\n" +
                        "2020-02-05\n" +
                        "2020-02-06\n" +
                        "2020-02-07\n" +
                        "2020-02-08\n" +
                        "2020-02-09\n" +
                        "2020-02-10\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testDistinctSymbolIndexedWithUpdates() throws Exception {
        assertQuery(
                "sym\n" +
                        "2020-01-01\n" +
                        "2020-01-02\n" +
                        "2020-01-03\n" +
                        "2020-01-04\n" +
                        "2020-01-05\n",
                "select DISTINCT(sym) from tab order by 1 LIMIT 5",
                "create table tab as (select timestamp_sequence('2020-01-01', 10 * 60 * 1000000L) ts, cast(" +
                        "to_str(timestamp_sequence('2020-01-01', 10 * 60 * 1000000L), 'yyyy-MM-dd')" +
                        " as symbol) sym from long_sequence(10000)), index(sym) timestamp(ts) PARTITION BY MONTH",
                null,
                "update tab set sym = 'abracadabra' where sym = '2020-01-01'",
                "sym\n" +
                        "2020-01-02\n" +
                        "2020-01-03\n" +
                        "2020-01-04\n" +
                        "2020-01-05\n" +
                        "2020-01-06\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testDistinctSymbolNonIndexed() throws Exception {
        assertQuery(
                "sym\n" +
                        "2020-01-01\n" +
                        "2020-01-02\n" +
                        "2020-01-03\n",
                "select DISTINCT sym from tab order by 1 LIMIT 3",
                "create table tab as (select timestamp_sequence('2020-01-01', 10 * 60 * 1000000L) ts, cast(" +
                        "to_str(timestamp_sequence('2020-01-01', 10 * 60 * 1000000L), 'yyyy-MM-dd')" +
                        " as symbol) sym from long_sequence(10000)) timestamp(ts) PARTITION BY MONTH",
                null,
                "alter table tab drop partition list '2020-01'",
                "sym\n" +
                        "2020-02-01\n" +
                        "2020-02-02\n" +
                        "2020-02-03\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testDistinctSymbolNonIndexedFilteredFramed() throws Exception {
        assertQuery(
                "sym\n" +
                        "2020-03-01\n" +
                        "2020-03-02\n" +
                        "2020-03-03\n" +
                        "2020-03-04\n",
                "select DISTINCT sym from tab WHERE ts in '2020-03' order by 1 LIMIT 4",
                "create table tab as (select timestamp_sequence('2020-01-01', 10 * 60 * 1000000L) ts, cast(" +
                        "to_str(timestamp_sequence('2020-01-01', 10 * 60 * 1000000L), 'yyyy-MM-dd')" +
                        " as symbol) sym from long_sequence(10000)) timestamp(ts) PARTITION BY MONTH",
                null,
                true,
                true
        );
    }

    @Test
    public void testDistinctSymbolNonIndexedFilteredNonFramed() throws Exception {
        // Filter framing not possible
        assertQuery(
                "sym\n" +
                        "2020-02-01\n" +
                        "2020-02-02\n" +
                        "2020-02-03\n" +
                        "2020-02-04\n",
                "select DISTINCT sym from tab WHERE ts > '2020-01-15' and left(sym, 7) = '2020-02' order by 1 LIMIT 4",
                "create table tab as (select timestamp_sequence('2020-01-01', 10 * 60 * 1000000L) ts, cast(" +
                        "to_str(timestamp_sequence('2020-01-01', 10 * 60 * 1000000L), 'yyyy-MM-dd')" +
                        " as symbol) sym from long_sequence(10000)) timestamp(ts) PARTITION BY MONTH",
                null,
                true,
                true
        );
    }

    @Test
    public void testDistinctSymbolNonIndexedWithNulls() throws Exception {
        assertQuery(
                "sym\n" +
                        "\n" +
                        "a\n" +
                        "b\n" +
                        "c\n",
                "select DISTINCT sym from tab order by 1",
                "create table tab as (select timestamp_sequence('2020-01-01', 10 * 60 * 1000000L) ts, rnd_symbol('a','b','c',null) sym from long_sequence(10000))" +
                        " timestamp(ts) PARTITION BY MONTH",
                null,
                true,
                true
        );
    }

    @Test
    public void testDistinctSymbolWithAnotherCol() throws Exception {
        assertQuery(
                "sym\tmonth\n" +
                        "2020-01-01\t1\n" +
                        "2020-01-02\t1\n" +
                        "2020-01-03\t1\n" +
                        "2020-01-04\t1\n" +
                        "2020-01-05\t1\n" +
                        "2020-01-06\t1\n" +
                        "2020-01-07\t1\n" +
                        "2020-01-08\t1\n" +
                        "2020-01-09\t1\n" +
                        "2020-01-10\t1\n",
                "select DISTINCT sym, month(ts) from tab order by 1 LIMIT 10",
                "create table tab as (select timestamp_sequence('2020-01-01', 10 * 60 * 1000000L) ts, cast(" +
                        "to_str(timestamp_sequence('2020-01-01', 10 * 60 * 1000000L), 'yyyy-MM-dd')" +
                        " as symbol) sym from long_sequence(10000)) timestamp(ts) PARTITION BY MONTH",
                null,
                true,
                true
        );
    }
}
