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


public class StringAggGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testConstantNull() throws Exception {
        assertQuery(
                "string_agg\n" +
                        "\n",
                "select string_agg(null, ',') from x",
                "create table x as (select * from (select timestamp_sequence(0, 100000) ts from long_sequence(5)) timestamp(ts))",
                null,
                false,
                true
        );
    }

    @Test
    public void testConstantString() throws Exception {
        assertQuery(
                "string_agg\n" +
                        "aaa,aaa,aaa,aaa,aaa\n",
                "select string_agg('aaa', ',') from x",
                "create table x as (select * from (select timestamp_sequence(0, 100000) ts from long_sequence(5)) timestamp(ts))",
                null,
                false,
                true
        );
    }

    @Test
    public void testDistinctColumnNameNotQuoted() throws Exception {
        assertException(
                "select string_agg(distinct, ',') from x",
                "create table x as (select * from (select rnd_str('abc', 'aaa', 'bbb', 'ccc') \"distinct\", timestamp_sequence(0, 100000) ts from long_sequence(5)) timestamp(ts))",
                18,
                "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"distinct\""
        );

        assertException("select string_agg(distinct::varchar, ',') from x", 18, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"distinct\"");
    }

    @Test
    public void testDistinctColumnNameQuoted() throws Exception {
        String expected = "string_agg\n" +
                "abc,bbb,aaa,ccc,aaa\n";
        assertQuery(
                "string_agg\n" +
                        "abc,bbb,aaa,ccc,aaa\n",
                "select string_agg(\"distinct\", ',') from x",
                "create table x as (select * from (select rnd_str('abc', 'aaa', 'bbb', 'ccc') \"distinct\", timestamp_sequence(0, 100000) ts from long_sequence(5)) timestamp(ts))",
                null,
                false,
                true
        );

        assertSql(expected, "select string_agg(\"distinct\"::varchar, ',') from x");
        assertSql(expected, "select string_agg(cast (\"distinct\" as string)::varchar, ',') from x");
    }

    @Test
    public void testGroupKeyed() throws Exception {
        assertQuery(
                "a\tstring_agg\n" +
                        "a\tbbb,abc,aaa\n" +
                        "b\tccc,abc\n" +
                        "c\tccc\n" +
                        "d\tbbb\n" +
                        "e\tabc,ccc\n" +
                        "f\tccc\n",
                "select a, string_agg(s, ',') from x order by a",
                "create table x as (" +
                        "select * from (" +
                        "   select " +
                        "       rnd_symbol('a','b','c','d','e','f') a," +
                        "       rnd_str('abc', 'aaa', 'bbb', 'ccc') s, " +
                        "       timestamp_sequence(0, 100000) ts " +
                        "   from long_sequence(10)" +
                        ") timestamp(ts))",
                null,
                true,
                true
        );
    }

    @Test
    public void testGroupKeyedAllNulls() throws Exception {
        assertQuery(
                "a\tstring_agg\n" +
                        "a\t\n" +
                        "b\t\n" +
                        "c\t\n",
                "select a, string_agg(s, ',') from x order by a",
                "create table x as (" +
                        "select * from (" +
                        "   select " +
                        "       rnd_symbol('a','b','c') a," +
                        "       null::string s, " +
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
                        "47\n",
                "select max(length(agg)) from (select a, string_agg(s, ',') agg from x)",
                "create table x as (" +
                        "select * from (" +
                        "   select " +
                        "       rnd_symbol(200,10,10,0) a," +
                        "       rnd_str('abc', 'aaa', 'bbb', 'ccc') s, " +
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
                "a\tstring_agg\n" +
                        "\taaa,aaa,aaa,aaa,aaa,aaa\n" +
                        "a\taaa,aaa,aaa,aaa\n",
                "select a, string_agg(s, ',') from x",
                "create table x as (" +
                        "select * from (" +
                        "   select " +
                        "       rnd_symbol(null, 'a') a," +
                        "       rnd_str(null, 'aaa') s, " +
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
                "string_agg\n" +
                        "abc,bbb,aaa,ccc,aaa\n",
                "select string_agg(s, ',') from x",
                "create table x as (select * from (select rnd_str('abc', 'aaa', 'bbb', 'ccc') s, timestamp_sequence(0, 100000) ts from long_sequence(5)) timestamp(ts))",
                null,
                false,
                true
        );
    }

    @Test
    public void testSkipNull() throws Exception {
        assertQuery(
                "string_agg\n" +
                        "\n",
                "select string_agg(s, ',') from x",
                "create table x as (select * from (select cast(null as string) s from long_sequence(5)))",
                null,
                "insert into x select 'abc' from long_sequence(1)",
                "string_agg\n" +
                        "abc\n",
                false,
                true,
                false
        );
    }
}
