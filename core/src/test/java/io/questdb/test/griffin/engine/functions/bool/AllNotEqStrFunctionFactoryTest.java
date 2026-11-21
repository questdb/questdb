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

package io.questdb.test.griffin.engine.functions.bool;

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class AllNotEqStrFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testConstant() throws Exception {
        assertQuery("select * from long_sequence(1) where 'aaa' <> all('{abc,xyz}'::text[])")
                .ddl(null)
                .expectSize()
                .returns("""
                        x
                        1
                        """);
    }

    @Test
    public void testEmptyArray() throws Exception {
        assertQuery("select * from tab where a <> all('{}'::text[])")
                .ddl("create table tab as (select rnd_str('aaa', 'bbb', 'ccc') a from long_sequence(5));")
                .expectSize()
                .returns("""
                        a
                        aaa
                        aaa
                        bbb
                        ccc
                        ccc
                        """);
    }

    @Test
    public void testMatch() throws Exception {
        assertQuery("select * from tab where a <> all('{abc,xyz}'::text[])")
                .ddl("create table tab as (select rnd_str('aaa', 'bbb', 'ccc') a from long_sequence(5));")
                .returns("""
                        a
                        aaa
                        aaa
                        bbb
                        ccc
                        ccc
                        """);
    }

    @Test
    public void testMatchVarcharColumn() throws Exception {
        assertQuery("select * from tab where a <> all('{добрий,вечір}'::text[])")
                .ddl("create table tab as (select rnd_varchar('ганьба','слава','добрий','вечір') a from long_sequence(5));")
                .returns("""
                        a
                        ганьба
                        слава
                        слава
                        """);
    }

    @Test
    public void testNoMatch() throws Exception {
        assertQuery("select * from tab where a <> all('{aaa,bbb,ccc}'::text[])")
                .ddl("create table tab as (select rnd_str('aaa', 'bbb', 'ccc') a from long_sequence(5));")
                .returns("a\n");
    }

    @Test
    public void testNull() throws Exception {
        assertQuery("select * from tab where a <> all('{aaa,bbb,ccc}'::text[])")
                .ddl("create table tab as (select cast(null as string) a from long_sequence(5));")
                .returns("a\n");
    }

    @Test
    public void testNullConstant() throws Exception {
        assertQuery("select * from long_sequence(1) where null <> all('{abc,xyz}'::text[])")
                .ddl(null)
                .expectSize()
                .returns("x\n");
    }

    @Test
    public void testNullVarchar() throws Exception {
        assertQuery("select * from tab where a <> all('{добрий,вечір}'::text[])")
                .ddl("create table tab as (select cast(null as varchar) a from long_sequence(15));")
                .returns("a\n");
    }

    @Test
    public void testSymbolCastUsesKeyComparison() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (SELECT x::INT k, (CASE x % 4 WHEN 0 THEN 'aaa' WHEN 1 THEN 'bbb' WHEN 2 THEN '' END)::SYMBOL s FROM long_sequence(8))");
            for (String lhs : new String[]{"s", "s::STRING"}) {
                assertQuery("SELECT k FROM tab WHERE " + lhs + " <> ALL('{aaa,xyz}'::text[])")
                        .noLeakCheck()
                        .withPlanContaining("filter: s <> all [aaa,xyz]")
                        .returns("""
                                k
                                1
                                2
                                5
                                6
                                """);
            }
        });
    }

    @Test
    public void testSymbolCastWithWrongArgumentNamesString() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (s SYMBOL, str STRING)");
            assertExceptionNoLeakCheck("SELECT * FROM tab WHERE str <> ALL ('b')", 28, "there is no matching function `<>all` with the argument types: (STRING, CHAR)");
            assertExceptionNoLeakCheck("SELECT * FROM tab WHERE s::STRING <> ALL ('b')", 34, "there is no matching function `<>all` with the argument types: (STRING, CHAR)");
        });
    }

    @Test
    public void testSymbolConstant() throws Exception {
        assertQuery("SELECT 'aaa'::SYMBOL <> ALL('{abc}'::text[]) a, 'aaa'::SYMBOL <> ALL('{aaa}'::text[]) b, NULL::SYMBOL <> ALL('{abc}'::text[]) c")
                .expectSize()
                .returns("""
                        a\tb\tc
                        true\tfalse\tfalse
                        """);
    }

    @Test
    public void testSymbolReusedFactoryResolvesKeysPerExecution() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (s SYMBOL)");
            execute("INSERT INTO tab VALUES ('aaa'), ('bbb')");
            try (RecordCursorFactory factory = select("SELECT s FROM tab WHERE s <> ALL('{aaa,new}'::text[])")) {
                assertFactoryRows("s\nbbb\n", factory);
                execute("INSERT INTO tab VALUES ('new'), ('ccc')");
                assertFactoryRows("s\nbbb\nccc\n", factory);
            }
        });
    }

    private void assertFactoryRows(String expected, RecordCursorFactory factory) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            assertCursor(expected, cursor, factory.getMetadata(), true);
        }
    }
}
