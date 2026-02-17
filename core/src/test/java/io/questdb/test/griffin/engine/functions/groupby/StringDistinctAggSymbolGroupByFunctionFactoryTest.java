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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class StringDistinctAggSymbolGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testConstantNull() throws Exception {
        String expected = """
                string_distinct_agg
                
                """;
        assertQuery(
                expected,
                "select string_distinct_agg(null::symbol, ',') from x",
                "create table x as (select * from (select timestamp_sequence(0, 100000) ts from long_sequence(5)) timestamp(ts))",
                null,
                false,
                true
        );
        assertSql(expected, "select string_agg(distinct null, ',') from x");
    }

    @Test
    public void testConstantString() throws Exception {
        String expected = """
                string_distinct_agg
                aaa
                """;
        assertQuery(
                expected,
                "select string_distinct_agg('aaa'::symbol, ',') from x",
                "create table x as (select * from (select timestamp_sequence(0, 100000) ts from long_sequence(5)) timestamp(ts))",
                null,
                false,
                true
        );
        assertSql(expected, "select string_agg(distinct 'aaa'::symbol, ',') from x");
    }

    @Test
    public void testGroupKeyed() throws Exception {
        String expected = """
                a\tstring_distinct_agg
                a\tbbb,abc,aaa
                b\tccc,abc,bbb
                c\tccc,abc,aaa
                d\tbbb,abc
                e\tabc,ccc
                f\tccc,abc,bbb
                """;
        assertQuery(
                expected,
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
        assertSql(expected, "select a, string_agg(distinct s, ',') from x group by a order by a");
    }

    @Test
    public void testGroupKeyedAllNulls() throws Exception {
        String expected = """
                a\tstring_distinct_agg
                a\t
                b\t
                c\t
                """;
        assertQuery(
                expected,
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
        assertSql(expected, "select a, string_agg(distinct s, ',') from x group by a order by a");
    }

    @Test
    public void testGroupKeyedManyRows() throws Exception {
        String expected = """
                max
                15
                """;
        assertQuery(
                expected,
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
        assertSql(expected, "select max(length(agg)) from (select a, string_agg(distinct s, ',') agg from x)");
    }

    @Test
    public void testGroupKeyedSomeNulls() throws Exception {
        String expected = """
                a\tstring_distinct_agg
                \taaa
                a\taaa
                """;
        assertQuery(
                expected,
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
        assertSql(expected, "select a, string_agg(distinct s, ',') from x group by a order by a");
    }

    @Test
    public void testGroupNotKeyed() throws Exception {
        String expected = """
                string_distinct_agg
                abc,bbb,aaa,ccc
                """;
        assertQuery(
                expected,
                "select string_distinct_agg(s, ',') from x",
                "create table x as (select * from (select rnd_symbol('abc', 'aaa', 'bbb', 'ccc') s, timestamp_sequence(0, 100000) ts from long_sequence(5)) timestamp(ts))",
                null,
                false,
                true
        );
        assertSql(expected, "select string_agg(distinct s, ',') from x");
    }

    @Test
    public void testSkipNull() throws Exception {
        String expected = """
                string_distinct_agg
                
                """;
        String ddl = "create table x as (select * from (select cast(null as symbol) s from long_sequence(5)))";
        String ddl2 = "insert into x select 'abc' from long_sequence(1)";
        String expected2 = """
                string_distinct_agg
                abc
                """;

        assertQuery(
                expected,
                "select string_distinct_agg(s, ',') from x",
                ddl,
                null,
                ddl2,
                expected2,
                false,
                true,
                false
        );

        execute("drop table x");

        assertQuery(
                expected,
                "select string_agg(distinct s, ',') from x",
                ddl,
                null,
                ddl2,
                expected2,
                false,
                true,
                false
        );
    }
}
