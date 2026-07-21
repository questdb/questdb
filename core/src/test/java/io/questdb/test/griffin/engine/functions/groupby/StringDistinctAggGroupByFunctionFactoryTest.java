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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class StringDistinctAggGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testConstantNull() throws Exception {
        String expected = """
                string_distinct_agg
                
                """;
        assertQuery("select string_distinct_agg(null, ',') from x")
                .ddl("create table x as (select * from (select timestamp_sequence(0, 100000) ts from long_sequence(5)) timestamp(ts))")
                .noRandomAccess()
                .expectSize()
                .returns(expected);
        assertQuery("select string_agg(distinct null, ',') from x")
                .noRandomAccess()
                .expectSize()
                .returns(expected);
    }

    @Test
    public void testConstantString() throws Exception {
        String expected = """
                string_distinct_agg
                aaa
                """;
        assertQuery("select string_distinct_agg('aaa', ',') from x")
                .ddl("create table x as (select * from (select timestamp_sequence(0, 100000) ts from long_sequence(5)) timestamp(ts))")
                .noRandomAccess()
                .expectSize()
                .returns(expected);
        assertQuery("select string_agg(distinct 'aaa', ',') from x")
                .noRandomAccess()
                .expectSize()
                .returns(expected);
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
        assertQuery("select a, string_distinct_agg(s, ',') from x order by a")
                .ddl("create table x as (" +
                        "select * from (" +
                        "   select " +
                        "       rnd_symbol('a','b','c','d','e','f') a," +
                        "       rnd_str('abc', 'aaa', 'bbb', 'ccc') s, " +
                        "       timestamp_sequence(0, 100000) ts " +
                        "   from long_sequence(30)" +
                        ") timestamp(ts))")
                .expectSize()
                .returns(expected);
        assertQuery("select a, string_agg(distinct s, ',') from x group by a order by a")
                .expectSize()
                .returns(expected);
    }

    @Test
    public void testGroupKeyedAllNulls() throws Exception {
        String expected = """
                a\tstring_distinct_agg
                a\t
                b\t
                c\t
                """;
        assertQuery("select a, string_distinct_agg(s, ',') from x order by a")
                .ddl("create table x as (" +
                        "select * from (" +
                        "   select " +
                        "       rnd_symbol('a','b','c') a," +
                        "       null::string s, " +
                        "       timestamp_sequence(0, 100000) ts " +
                        "   from long_sequence(5)" +
                        ") timestamp(ts))")
                .expectSize()
                .returns(expected);
        assertQuery("select a, string_agg(distinct s, ',') from x group by a order by a")
                .expectSize()
                .returns(expected);
    }

    @Test
    public void testGroupKeyedManyRows() throws Exception {
        String expected = """
                max
                15
                """;
        assertQuery("select max(length(agg)) from (select a, string_distinct_agg(s, ',') agg from x)")
                .ddl("create table x as (" +
                        "select * from (" +
                        "   select " +
                        "       rnd_symbol(200,10,10,0) a," +
                        "       rnd_str('abc', 'aaa', 'bbb', 'ccc') s, " +
                        "       timestamp_sequence(0, 100000) ts " +
                        "   from long_sequence(1000)" +
                        ") timestamp(ts))")
                .noRandomAccess()
                .expectSize()
                .returns(expected);
        assertQuery("select max(length(agg)) from (select a, string_agg(distinct s, ',') agg from x)")
                .noRandomAccess()
                .expectSize()
                .returns(expected);
    }

    @Test
    public void testGroupKeyedSomeNulls() throws Exception {
        String expected = """
                a\tstring_distinct_agg
                \taaa
                a\taaa
                """;
        assertQuery("select a, string_distinct_agg(s, ',') from x")
                .ddl("create table x as (" +
                        "select * from (" +
                        "   select " +
                        "       rnd_symbol(null, 'a') a," +
                        "       rnd_str(null, 'aaa') s, " +
                        "       timestamp_sequence(0, 100000) ts " +
                        "   from long_sequence(20)" +
                        ") timestamp(ts))")
                .expectSize()
                .returns(expected);
        assertQuery("select a, string_agg(distinct s, ',') from x")
                .expectSize()
                .returns(expected);
    }

    @Test
    public void testGroupNotKeyed() throws Exception {
        String expected = """
                string_distinct_agg
                abc,bbb,aaa,ccc
                """;
        assertQuery("select string_distinct_agg(s, ',') from x")
                .ddl("create table x as (select * from (select rnd_str('abc', 'aaa', 'bbb', 'ccc') s, timestamp_sequence(0, 100000) ts from long_sequence(5)) timestamp(ts))")
                .noRandomAccess()
                .expectSize()
                .returns(expected);
        assertQuery("select string_agg(distinct s, ',') from x")
                .noRandomAccess()
                .expectSize()
                .returns(expected);
    }

    @Test
    public void testSkipNull() throws Exception {
        String ddl = "create table x as (select * from (select cast(null as string) s from long_sequence(5)))";
        String expected = """
                string_distinct_agg
                
                """;
        String ddl2 = "insert into x select 'abc' from long_sequence(1)";
        String expected2 = """
                string_distinct_agg
                abc
                """;
        assertQuery("select string_distinct_agg(s, ',') from x")
                .ddl(ddl)
                .mutateWith(ddl2)
                .noRandomAccess()
                .expectSize()
                .returns(expected, expected2);

        execute("drop table x");

        assertQuery("select string_agg(distinct s, ',') from x")
                .ddl(ddl)
                .mutateWith(ddl2)
                .noRandomAccess()
                .expectSize()
                .returns(expected, expected2);
    }
}
