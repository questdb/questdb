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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class InSymbolCursorFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testNullInCursorInFilter() throws Exception {
        assertQuery("SELECT x FROM long_sequence(3) WHERE NULL IN (SELECT 'abc'::symbol);")
                .ddl(null)
                .returns("x\n");
    }

    @Test
    public void testNullInCursorInSelect() throws Exception {
        assertQuery("SELECT NULL IN (SELECT 'abc'::symbol);")
                .ddl(null)
                .expectSize()
                .returns("""
                        column
                        false
                        """);
    }

    @Test
    public void testNullInNullCursorInFilter() throws Exception {
        assertQuery("SELECT x FROM long_sequence(3) WHERE NULL IN (SELECT 'abc'::symbol);")
                .ddl(null)
                .returns("x\n");
    }

    @Test
    public void testNullInNullCursorInSelect() throws Exception {
        assertQuery("SELECT NULL IN (SELECT NULL);")
                .ddl(null)
                .expectSize()
                .returns("""
                        column
                        true
                        """);
    }

    @Test
    public void testNullInNullInFilter() throws Exception {
        assertQuery("SELECT x FROM long_sequence(3) WHERE NULL IN NULL;")
                .ddl(null)
                .expectSize()
                .returns("""
                        x
                        1
                        2
                        3
                        """);
    }

    @Test
    public void testNullInNullInSelect() throws Exception {
        assertQuery("SELECT NULL IN NULL;")
                .ddl(null)
                .expectSize()
                .returns("""
                        column
                        true
                        """);
    }

    @Test
    public void testSymbolColumnInCursorInFilter() throws Exception {
        assertQuery("select * from x where b in (select 'RXGZ'::symbol from long_sequence(3))")
                .ddl("create table x as (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from long_sequence(5)" +
                        ") timestamp(k) partition by DAY")
                .timestamp("k")
                .returns("""
                        a\tb\tk
                        23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z
                        """);
    }

    @Test
    public void testSymbolColumnInCursorInSelect() throws Exception {
        assertQuery("select b in (select 'RXGZ'::symbol from long_sequence(3)), b from x")
                .ddl("create table x as (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from long_sequence(5)" +
                        ") timestamp(k) partition by DAY")
                .expectSize()
                .returns("""
                        column\tb
                        false\t
                        false\tVTJW
                        true\tRXGZ
                        false\tPEHN
                        false\t
                        """);
    }

    @Test
    public void testSymbolColumnInNullInFilter() throws Exception {
        assertQuery("select * from x where b in (select null from long_sequence(10))")
                .ddl("create table x as (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from long_sequence(5)" +
                        ") timestamp(k) partition by DAY")
                .timestamp("k")
                .returns("""
                        a\tb\tk
                        11.427984775756228\t\t1970-01-01T00:00:00.000000Z
                        87.99634725391621\t\t1970-01-05T15:06:40.000000Z
                        """);
    }

    @Test
    public void testSymbolColumnInNullInSelect() throws Exception {
        assertQuery("select b in (select null from long_sequence(10)), b from x")
                .ddl("create table x as (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from long_sequence(5)" +
                        ") timestamp(k) partition by DAY")
                .expectSize()
                .returns("""
                        column\tb
                        true\t
                        false\tVTJW
                        false\tRXGZ
                        false\tPEHN
                        true\t
                        """);
    }

    @Test
    public void testSymbolExprInNullInFilter() throws Exception {
        assertQuery("select * from x where substring(b, 1, 2)::symbol in (select null from long_sequence(10))")
                .ddl("create table x as (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from long_sequence(5)" +
                        ") timestamp(k) partition by DAY")
                .timestamp("k")
                .returns("""
                        a\tb\tk
                        11.427984775756228\t\t1970-01-01T00:00:00.000000Z
                        87.99634725391621\t\t1970-01-05T15:06:40.000000Z
                        """);
    }

    @Test
    public void testSymbolExprInNullInSelect() throws Exception {
        assertQuery("select substring(b, 1, 2)::symbol in (select null from long_sequence(10)), b from x")
                .ddl("create table x as (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from long_sequence(5)" +
                        ") timestamp(k) partition by DAY")
                .expectSize()
                .returns("""
                        column\tb
                        true\t
                        false\tVTJW
                        false\tRXGZ
                        false\tPEHN
                        true\t
                        """);
    }

    @Test
    public void testSymbolInCursorInFilter() throws Exception {
        assertQuery("SELECT x FROM long_sequence(3) WHERE 'abc'::symbol IN (SELECT 'abc'::symbol);")
                .ddl(null)
                .returns("""
                        x
                        1
                        2
                        3
                        """);
    }

    @Test
    public void testSymbolInCursorInSelect() throws Exception {
        assertQuery("SELECT 'abc'::symbol IN (SELECT 'abc'::symbol);")
                .ddl(null)
                .expectSize()
                .returns("""
                        column
                        true
                        """);
    }

    @Test
    public void testSymbolInNullCursorInFilter() throws Exception {
        assertQuery("SELECT x FROM long_sequence(3) WHERE 'foobar'::symbol IN (SELECT NULL);")
                .ddl(null)
                .returns("x\n");
    }

    @Test
    public void testSymbolInNullCursorInSelect() throws Exception {
        assertQuery("SELECT 'foobar'::symbol IN (SELECT NULL);")
                .ddl(null)
                .expectSize()
                .returns("""
                        column
                        false
                        """);
    }

    @Test
    public void testSymbolInNullInFilter() throws Exception {
        assertQuery("SELECT x FROM long_sequence(3) WHERE 'foobar'::symbol IN NULL;")
                .ddl(null)
                .returns("x\n");
    }

    @Test
    public void testSymbolInNullInSelect() throws Exception {
        assertQuery("SELECT 'foobar'::symbol IN NULL;")
                .ddl(null)
                .expectSize()
                .returns("""
                        column
                        false
                        """);
    }

    @Test
    public void testSymbolInSymbolInFilter() throws Exception {
        assertQuery("SELECT x FROM long_sequence(3) WHERE 'foobar'::symbol IN 'foobar'::symbol;")
                .ddl(null)
                .expectSize()
                .returns("""
                        x
                        1
                        2
                        3
                        """);
    }

    @Test
    public void testSymbolInSymbolInSelect() throws Exception {
        assertQuery("SELECT 'foo'::symbol IN 'bar'::symbol;")
                .ddl(null)
                .expectSize()
                .returns("""
                        column
                        false
                        """);
    }

    @Test
    public void testUnsupportedColumnType() throws Exception {
        assertQuery("select * from x where b in (select 12, rnd_str('RXGZ', 'HYRX', null) a from long_sequence(10))")
                .ddl("create table x as (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from long_sequence(20)" +
                        ") timestamp(k) partition by DAY")
                .fails(24, "supported column types are VARCHAR, SYMBOL and STRING, found: INT");
    }
}
