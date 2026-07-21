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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;


public class StringAggGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testBufferLimitCompliance() throws Exception {
        assertMemoryLeak(() -> {
            engine.execute("create table break as (select rnd_str(25,25,0) a from long_sequence(100000));", sqlExecutionContext);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                try (RecordCursorFactory fact = compiler.compile("select string_agg(a, ',') from break", sqlExecutionContext).getRecordCursorFactory()) {
                    // execute factory a couple of times to make sure nothing breaks
                    testBufferLimitCompliance0(fact);
                    testBufferLimitCompliance0(fact);

                    engine.execute("truncate table break", sqlExecutionContext);
                    engine.execute("insert into break select rnd_str(25,25,0) a from long_sequence(10)", sqlExecutionContext);
                    // execute factory few times to make sure nothing accumulates
                    testBufferLimitCompliance1(fact);
                    testBufferLimitCompliance1(fact);
                    testBufferLimitCompliance1(fact);
                }
            }
        });
    }

    @Test
    public void testConstantNull() throws Exception {
        assertQuery("select string_agg(null, ',') from x")
                .ddl("create table x as (select * from (select timestamp_sequence(0, 100000) ts from long_sequence(5)) timestamp(ts))")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        string_agg
                        
                        """);
    }

    @Test
    public void testConstantString() throws Exception {
        assertQuery("select string_agg('aaa', ',') from x")
                .ddl("create table x as (select * from (select timestamp_sequence(0, 100000) ts from long_sequence(5)) timestamp(ts))")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        string_agg
                        aaa,aaa,aaa,aaa,aaa
                        """);
    }

    @Test
    public void testDistinctColumnNameNotQuoted() throws Exception {
        assertQuery("select string_agg(distinct, ',') from x")
                .ddl("create table x as (select * from (select rnd_str('abc', 'aaa', 'bbb', 'ccc') \"distinct\", timestamp_sequence(0, 100000) ts from long_sequence(5)) timestamp(ts))")
                .fails(18, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"distinct\"");

        assertQuery("select string_agg(distinct::varchar, ',') from x")
                .fails(18, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"distinct\"");
    }

    @Test
    public void testDistinctColumnNameQuoted() throws Exception {
        String expected = """
                string_agg
                abc,bbb,aaa,ccc,aaa
                """;
        assertQuery("select string_agg(\"distinct\", ',') from x")
                .ddl("create table x as (select * from (select rnd_str('abc', 'aaa', 'bbb', 'ccc') \"distinct\", timestamp_sequence(0, 100000) ts from long_sequence(5)) timestamp(ts))")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        string_agg
                        abc,bbb,aaa,ccc,aaa
                        """);

        assertQuery("select string_agg(\"distinct\"::varchar, ',') from x")
                .noRandomAccess()
                .expectSize()
                .returns(expected);
        assertQuery("select string_agg(cast (\"distinct\" as string)::varchar, ',') from x")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns(expected);
    }

    @Test
    public void testGroupKeyed() throws Exception {
        assertQuery("select a, string_agg(s, ',') from x order by a")
                .ddl("create table x as (" +
                        "select * from (" +
                        "   select " +
                        "       rnd_symbol('a','b','c','d','e','f') a," +
                        "       rnd_str('abc', 'aaa', 'bbb', 'ccc') s, " +
                        "       timestamp_sequence(0, 100000) ts " +
                        "   from long_sequence(10)" +
                        ") timestamp(ts))")
                .expectSize()
                .returns("""
                        a\tstring_agg
                        a\tbbb,abc,aaa
                        b\tccc,abc
                        c\tccc
                        d\tbbb
                        e\tabc,ccc
                        f\tccc
                        """);
    }

    @Test
    public void testGroupKeyedAllNulls() throws Exception {
        assertQuery("select a, string_agg(s, ',') from x order by a")
                .ddl("create table x as (" +
                        "select * from (" +
                        "   select " +
                        "       rnd_symbol('a','b','c') a," +
                        "       null::string s, " +
                        "       timestamp_sequence(0, 100000) ts " +
                        "   from long_sequence(5)" +
                        ") timestamp(ts))")
                .expectSize()
                .returns("""
                        a\tstring_agg
                        a\t
                        b\t
                        c\t
                        """);
    }

    @Test
    public void testGroupKeyedManyRows() throws Exception {
        assertQuery("select max(length(agg)) from (select a, string_agg(s, ',') agg from x)")
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
                .returns("""
                        max
                        47
                        """);
    }

    @Test
    public void testGroupKeyedSomeNulls() throws Exception {
        assertQuery("select a, string_agg(s, ',') from x")
                .ddl("create table x as (" +
                        "select * from (" +
                        "   select " +
                        "       rnd_symbol(null, 'a') a," +
                        "       rnd_str(null, 'aaa') s, " +
                        "       timestamp_sequence(0, 100000) ts " +
                        "   from long_sequence(20)" +
                        ") timestamp(ts))")
                .expectSize()
                .returns("""
                        a\tstring_agg
                        \taaa,aaa,aaa,aaa,aaa,aaa
                        a\taaa,aaa,aaa,aaa
                        """);
    }

    @Test
    public void testGroupNotKeyed() throws Exception {
        assertQuery("select string_agg(s, ',') from x")
                .ddl("create table x as (select * from (select rnd_str('abc', 'aaa', 'bbb', 'ccc') s, timestamp_sequence(0, 100000) ts from long_sequence(5)) timestamp(ts))")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        string_agg
                        abc,bbb,aaa,ccc,aaa
                        """);
    }

    @Test
    public void testSkipNull() throws Exception {
        assertQuery("select string_agg(s, ',') from x")
                .ddl("create table x as (select * from (select cast(null as string) s from long_sequence(5)))")
                .mutateWith("insert into x select 'abc' from long_sequence(1)")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        string_agg
                        
                        """, """
                        string_agg
                        abc
                        """);
    }

    private static void testBufferLimitCompliance0(RecordCursorFactory fact) throws SqlException {
        try (RecordCursor cursor = fact.getCursor(sqlExecutionContext)) {
            try {
                TestUtils.assertCursor(null, cursor, fact.getMetadata(), true, sink);
                Assert.fail();
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "string_agg() result exceeds max size of");
                Assert.assertEquals(18, e.getPosition());
            }
        }
    }

    private static void testBufferLimitCompliance1(RecordCursorFactory fact) throws SqlException {
        try (RecordCursor cursor = fact.getCursor(sqlExecutionContext)) {
            TestUtils.assertCursor(
                    """
                            string_agg
                            FGSLQDYOCTKSCTOHWMRHGLXBI,HWIIOHDDLYDZEUTOQEZOODZRO,NSTFHKVDEKTQJGFGVRMVZVRCO,HRFVCWZYHGIEIEJEIDVRHYFXF,LEZBTJQRMHTRYZEUDDOTUHDOC,IMSSDLJUVTBCCPKOOEYICCHMH,NSBDCJLEPQPOVFLEJCELDSOWJ,TIMPLKXPJEVHPWLCKDUTWKOSZ,HBJLFGZSKNBUVJNXJUUKCSJHM,GDKSKLTEBHKVQXNELZCHSPSYD
                            """,
                    cursor,
                    fact.getMetadata(),
                    true,
                    sink
            );
        }
    }
}
