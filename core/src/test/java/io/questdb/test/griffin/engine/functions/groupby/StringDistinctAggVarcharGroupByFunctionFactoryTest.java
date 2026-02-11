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

public class StringDistinctAggVarcharGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testConstantNull() throws Exception {
        assertQuery(
                """
                        string_distinct_agg
                        
                        """,
                "select string_distinct_agg(null::varchar, ',') from x",
                "create table x as (select * from (select timestamp_sequence(0, 100000) ts from long_sequence(5)) timestamp(ts))",
                null,
                false,
                true
        );
    }

    @Test
    public void testConstantString() throws Exception {
        assertQuery(
                """
                        string_distinct_agg
                        aaa
                        """,
                "select string_distinct_agg('aaa'::varchar, ',') from x",
                "create table x as (select * from (select timestamp_sequence(0, 100000) ts from long_sequence(5)) timestamp(ts))",
                null,
                false,
                true
        );
    }

    @Test
    public void testGroupKeyed() throws Exception {
        assertQuery(
                """
                        a\tstring_distinct_agg
                        a\tbbb,abc,aaa
                        b\tccc,abc,bbb
                        c\tccc,abc,aaa
                        d\tbbb,abc
                        e\tabc,ccc
                        f\tccc,abc,bbb
                        """,
                "select a, string_distinct_agg(s, ',') from x order by a",
                "create table x as (" +
                        "select * from (" +
                        "   select " +
                        "       rnd_symbol('a','b','c','d','e','f') a," +
                        "       rnd_varchar('abc', 'aaa', 'bbb', 'ccc') s, " +
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
                """
                        a\tstring_distinct_agg
                        a\t
                        b\t
                        c\t
                        """,
                "select a, string_distinct_agg(s, ',') from x order by a",
                "create table x as (" +
                        "select * from (" +
                        "   select " +
                        "       rnd_symbol('a','b','c') a," +
                        "       null::varchar s, " +
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
                """
                        max
                        15
                        """,
                "select max(length(agg)) from (select a, string_distinct_agg(s, ',') agg from x)",
                "create table x as (" +
                        "select * from (" +
                        "   select " +
                        "       rnd_symbol(200,10,10,0) a," +
                        "       rnd_varchar('abc', 'aaa', 'bbb', 'ccc') s, " +
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
                """
                        a\tstring_distinct_agg
                        \taaa
                        a\taaa
                        """,
                "select a, string_distinct_agg(s, ',') from x",
                "create table x as (" +
                        "select * from (" +
                        "   select " +
                        "       rnd_symbol(null, 'a') a," +
                        "       rnd_varchar(null, 'aaa') s, " +
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
                """
                        string_distinct_agg
                        abc,bbb,aaa,ccc
                        """,
                "select string_distinct_agg(s, ',') from x",
                "create table x as (select * from (select rnd_varchar('abc', 'aaa', 'bbb', 'ccc') s, timestamp_sequence(0, 100000) ts from long_sequence(5)) timestamp(ts))",
                null,
                false,
                true
        );
    }

    @Test
    public void testOrderByNotSupported() throws Exception {
        execute("create table x as (select * from (select timestamp_sequence(0, 100000) ts from long_sequence(5)) timestamp(ts))");
        assertException("select string_distinct_agg(ts, ',' ORDER BY ts) from x", 35, "ORDER BY not supported for string_distinct_agg");
        assertException("select string_distinct_agg(ts, ',' order by ts) from x", 35, "ORDER BY not supported for string_distinct_agg");
    }

    @Test
    public void testSkipNull() throws Exception {
        assertQuery(
                """
                        string_distinct_agg
                        
                        """,
                "select string_distinct_agg(s, ',') from x",
                "create table x as (select * from (select cast(null as varchar) s from long_sequence(5)))",
                null,
                "insert into x select 'abc' from long_sequence(1)",
                """
                        string_distinct_agg
                        abc
                        """,
                false,
                true,
                false
        );
    }
}
