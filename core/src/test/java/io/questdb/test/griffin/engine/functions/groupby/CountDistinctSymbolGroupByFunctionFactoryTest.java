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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class CountDistinctSymbolGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testConstant() throws Exception {
        assertQuery(
                "a\tcount_distinct\n" +
                        "a\t1\n" +
                        "b\t1\n" +
                        "c\t1\n",
                "select a, count_distinct('a'::symbol) from x order by a",
                "create table x as (select * from (select rnd_symbol('a','b','c') a from long_sequence(20)))",
                null,
                true,
                true
        );
    }

    @Test
    public void testExpression() throws Exception {
        final String expected = "a\tcount_distinct\n" +
                "1\t1\n" +
                "3\t1\n" +
                "4\t2\n" +
                "5\t2\n" +
                "6\t2\n" +
                "7\t3\n" +
                "8\t2\n";
        assertQuery(
                expected,
                "select a, count_distinct(concat(s, 'foobar')::symbol) from x order by a",
                "create table x as (select * from (select rnd_int(1, 8, 0) a, rnd_symbol('a','b','c') s from long_sequence(20)))",
                null,
                true,
                true
        );
        // concatenation shouldn't affect the number of distinct values,
        // so the result should stay the same
        assertSql(expected, "select a, count_distinct(s) from x order by a");
    }

    @Test
    public void testGroupKeyed() throws Exception {
        assertQuery(
                "a\tcount_distinct\n" +
                        "0\t3\n" +
                        "1\t3\n" +
                        "3\t3\n" +
                        "4\t3\n" +
                        "5\t1\n" +
                        "6\t2\n" +
                        "8\t3\n",
                "select a, count_distinct(s) from x order by a",
                "create table x as (select * from (select rnd_int(0, 9, 0) a, rnd_symbol('a','b','c','d','e','f') s, timestamp_sequence(0, 100000) ts from long_sequence(20)) timestamp(ts))",
                null,
                true,
                true
        );
    }

    @Test
    public void testGroupNotKeyed() throws Exception {
        assertQuery(
                "count_distinct\n" +
                        "922\n",
                "select count_distinct(s) from x",
                "create table x as (select * from (select rnd_symbol(1000, 1, 10, 0) s, timestamp_sequence(0, 100000) ts from long_sequence(10000)) timestamp(ts))",
                null,
                false,
                true
        );
    }

    @Test
    public void testGroupNotKeyedMultipleFunctions() throws Exception {
        assertQuery(
                "count_distinct\tcount_distinct1\n" +
                        "96\t921\n",
                "select count_distinct(s1), count_distinct(s2) from x",
                "create table x as (select * from (select rnd_symbol(100, 1, 10, 0) s1, rnd_symbol(1000, 1, 10, 0) s2, timestamp_sequence(0, 100000) ts from long_sequence(10000)) timestamp(ts))",
                null,
                false,
                true
        );
    }

    @Test
    public void testGroupNotKeyedWithNulls() throws Exception {
        String expected = "count_distinct\n" +
                "62\n";
        assertQuery(
                expected,
                "select count_distinct(s) from x",
                "create table x as (select * from (select rnd_symbol(100, 10, 10, 0) s, timestamp_sequence(10, 100000) ts from long_sequence(100)) timestamp(ts)) timestamp(ts) PARTITION BY YEAR",
                null,
                false,
                true
        );

        insert("insert into x values(cast(null as SYMBOL), '2021-05-21')");
        insert("insert into x values(cast(null as SYMBOL), '1970-01-01')");
        assertSql(expected, "select count_distinct(s) from x");
    }

    @Test
    public void testNullConstant() throws Exception {
        assertQuery(
                "s\tcount_distinct\n" +
                        "a\t0\n" +
                        "b\t0\n" +
                        "c\t0\n",
                "select s, count_distinct(cast(null as SYMBOL)) from x order by s",
                "create table x as (select * from (select rnd_symbol('a','b','c') s from long_sequence(20)))",
                null,
                true,
                true
        );
    }

    @Test
    public void testSampleKeyed() throws Exception {
        assertQuery(
                "count_distinct\tts\n" +
                        "6\t1970-01-01T00:00:00.000000Z\n" +
                        "6\t1970-01-01T00:00:05.000000Z\n",
                "select count_distinct(s), ts from x sample by 5s",
                "create table x as (select * from (select rnd_symbol('a','b','c','d','e','f') s, timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))",
                "ts",
                false
        );
    }
}
