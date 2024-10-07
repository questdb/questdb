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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class StringDistinctAggSymbolGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testConstantNull() throws Exception {
        assertQuery(
                "string_distinct_agg\n" +
                        "\n",
                "select string_distinct_agg(null::symbol, ',') from x",
                "create table x as (select * from (select timestamp_sequence(0, 100000) ts from long_sequence(5)) timestamp(ts))",
                null,
                false,
                true
        );
    }

    @Test
    public void testConstantString() throws Exception {
        assertQuery(
                "string_distinct_agg\n" +
                        "aaa\n",
                "select string_distinct_agg('aaa'::symbol, ',') from x",
                "create table x as (select * from (select timestamp_sequence(0, 100000) ts from long_sequence(5)) timestamp(ts))",
                null,
                false,
                true
        );
    }

    @Test
    public void testGroupKeyed() throws Exception {
        assertQuery(
                "a\tstring_distinct_agg\n" +
                        "a\tbbb,abc,aaa\n" +
                        "b\tccc,abc,bbb\n" +
                        "c\tccc,abc,aaa\n" +
                        "d\tbbb,abc\n" +
                        "e\tabc,ccc\n" +
                        "f\tccc,abc,bbb\n",
                "select a, string_distinct_agg(s, ',') from x order by a",
                "create table x as (" +
                        "select * from (" +
                        "   select " +
                        "       rnd_symbol('a','b','c','d','e','f') a," +
                        "       rnd_symbol('abc', 'aaa', 'bbb', 'ccc') s, " +
                        "       timestamp_sequence(0, 100000) ts " +
                        "   from long_sequence(30)" +
                        ") timestamp(ts))",
                null,
                true,
                true
        );
    }

    @Test
    public void testGroupKeyedAllNulls() throws Exception {
        assertQuery(
                "a\tstring_distinct_agg\n" +
                        "a\t\n" +
                        "b\t\n" +
                        "c\t\n",
                "select a, string_distinct_agg(s, ',') from x order by a",
                "create table x as (" +
                        "select * from (" +
                        "   select " +
                        "       rnd_symbol('a','b','c') a," +
                        "       null::symbol s, " +
                        "       timestamp_sequence(0, 100000) ts " +
                        "   from long_sequence(5)" +
                        ") timestamp(ts))",
                null,
                true,
                true
        );
    }

    @Test
    public void testGroupKeyedManyRows() throws Exception {
        assertQuery(
                "max\n" +
                        "15\n",
                "select max(length(agg)) from (select a, string_distinct_agg(s, ',') agg from x)",
                "create table x as (" +
                        "select * from (" +
                        "   select " +
                        "       rnd_symbol(200,10,10,0) a," +
                        "       rnd_symbol('abc', 'aaa', 'bbb', 'ccc') s, " +
                        "       timestamp_sequence(0, 100000) ts " +
                        "   from long_sequence(1000)" +
                        ") timestamp(ts))",
                null,
                false,
                true
        );
    }

    @Test
    public void testGroupKeyedSomeNulls() throws Exception {
        assertQuery(
                "a\tstring_distinct_agg\n" +
                        "\taaa\n" +
                        "a\taaa\n",
                "select a, string_distinct_agg(s, ',') from x",
                "create table x as (" +
                        "select * from (" +
                        "   select " +
                        "       rnd_symbol(null, 'a') a," +
                        "       rnd_symbol(null, 'aaa') s, " +
                        "       timestamp_sequence(0, 100000) ts " +
                        "   from long_sequence(20)" +
                        ") timestamp(ts))",
                null,
                true,
                true
        );
    }

    @Test
    public void testGroupNotKeyed() throws Exception {
        assertQuery(
                "string_distinct_agg\n" +
                        "abc,bbb,aaa,ccc\n",
                "select string_distinct_agg(s, ',') from x",
                "create table x as (select * from (select rnd_symbol('abc', 'aaa', 'bbb', 'ccc') s, timestamp_sequence(0, 100000) ts from long_sequence(5)) timestamp(ts))",
                null,
                false,
                true
        );
    }

    @Test
    public void testSkipNull() throws Exception {
        assertQuery(
                "string_distinct_agg\n" +
                        "\n",
                "select string_distinct_agg(s, ',') from x",
                "create table x as (select * from (select cast(null as symbol) s from long_sequence(5)))",
                null,
                "insert into x select 'abc' from long_sequence(1)",
                "string_distinct_agg\n" +
                        "abc\n",
                false,
                true,
                false
        );
    }
}
