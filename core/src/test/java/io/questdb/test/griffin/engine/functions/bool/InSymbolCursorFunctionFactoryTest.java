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

package io.questdb.test.griffin.engine.functions.bool;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class InSymbolCursorFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testNullInCursorInFilter() throws Exception {
        assertQuery(
                "x\n",
                "SELECT x FROM long_sequence(3) WHERE NULL IN (SELECT 'abc'::symbol);",
                null,
                null,
                true,
                false
        );
    }

    @Test
    public void testNullInCursorInSelect() throws Exception {
        assertQuery(
                "column\n" +
                        "false\n",
                "SELECT NULL IN (SELECT 'abc'::symbol);",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testNullInNullCursorInFilter() throws Exception {
        assertQuery(
                "x\n",
                "SELECT x FROM long_sequence(3) WHERE NULL IN (SELECT 'abc'::symbol);",
                null,
                null,
                true,
                false
        );
    }

    @Test
    public void testNullInNullCursorInSelect() throws Exception {
        assertQuery(
                "column\n" +
                        "true\n",
                "SELECT NULL IN (SELECT NULL);",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testNullInNullInFilter() throws Exception {
        assertQuery(
                "x\n" +
                        "1\n" +
                        "2\n" +
                        "3\n",
                "SELECT x FROM long_sequence(3) WHERE NULL IN NULL;",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testNullInNullInSelect() throws Exception {
        assertQuery(
                "column\n" +
                        "true\n",
                "SELECT NULL IN NULL;",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testSymbolColumnInCursorInFilter() throws Exception {
        assertQuery(
                "a\tb\tk\n" +
                        "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n",
                "select * from x where b in (select 'RXGZ'::symbol from long_sequence(3))",
                "create table x as (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from long_sequence(5)" +
                        ") timestamp(k) partition by DAY",
                "k",
                true,
                false
        );
    }

    @Test
    public void testSymbolColumnInCursorInSelect() throws Exception {
        assertQuery(
                "column\tb\n" +
                        "false\t\n" +
                        "false\tVTJW\n" +
                        "true\tRXGZ\n" +
                        "false\tPEHN\n" +
                        "false\t\n",
                "select b in (select 'RXGZ'::symbol from long_sequence(3)), b from x",
                "create table x as (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from long_sequence(5)" +
                        ") timestamp(k) partition by DAY",
                null,
                true,
                true
        );
    }

    @Test
    public void testSymbolColumnInNullInFilter() throws Exception {
        assertQuery(
                "a\tb\tk\n" +
                        "11.427984775756228\t\t1970-01-01T00:00:00.000000Z\n" +
                        "87.99634725391621\t\t1970-01-05T15:06:40.000000Z\n",
                "select * from x where b in (select null from long_sequence(10))",
                "create table x as (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from long_sequence(5)" +
                        ") timestamp(k) partition by DAY",
                "k",
                true,
                false
        );
    }

    @Test
    public void testSymbolColumnInNullInSelect() throws Exception {
        assertQuery(
                "column\tb\n" +
                        "true\t\n" +
                        "false\tVTJW\n" +
                        "false\tRXGZ\n" +
                        "false\tPEHN\n" +
                        "true\t\n",
                "select b in (select null from long_sequence(10)), b from x",
                "create table x as (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from long_sequence(5)" +
                        ") timestamp(k) partition by DAY",
                null,
                true,
                true
        );
    }

    @Test
    public void testSymbolExprInNullInFilter() throws Exception {
        assertQuery(
                "a\tb\tk\n" +
                        "11.427984775756228\t\t1970-01-01T00:00:00.000000Z\n" +
                        "87.99634725391621\t\t1970-01-05T15:06:40.000000Z\n",
                "select * from x where substring(b, 1, 2)::symbol in (select null from long_sequence(10))",
                "create table x as (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from long_sequence(5)" +
                        ") timestamp(k) partition by DAY",
                "k",
                true,
                false
        );
    }

    @Test
    public void testSymbolExprInNullInSelect() throws Exception {
        assertQuery(
                "column\tb\n" +
                        "true\t\n" +
                        "false\tVTJW\n" +
                        "false\tRXGZ\n" +
                        "false\tPEHN\n" +
                        "true\t\n",
                "select substring(b, 1, 2)::symbol in (select null from long_sequence(10)), b from x",
                "create table x as (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from long_sequence(5)" +
                        ") timestamp(k) partition by DAY",
                null,
                true,
                true
        );
    }

    @Test
    public void testSymbolInCursorInFilter() throws Exception {
        assertQuery(
                "x\n" +
                        "1\n" +
                        "2\n" +
                        "3\n",
                "SELECT x FROM long_sequence(3) WHERE 'abc'::symbol IN (SELECT 'abc'::symbol);",
                null,
                null,
                true,
                false
        );
    }

    @Test
    public void testSymbolInCursorInSelect() throws Exception {
        assertQuery(
                "column\n" +
                        "true\n",
                "SELECT 'abc'::symbol IN (SELECT 'abc'::symbol);",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testSymbolInNullCursorInFilter() throws Exception {
        assertQuery(
                "x\n",
                "SELECT x FROM long_sequence(3) WHERE 'foobar'::symbol IN (SELECT NULL);",
                null,
                null,
                false,
                false
        );
    }

    @Test
    public void testSymbolInNullCursorInSelect() throws Exception {
        assertQuery(
                "column\n" +
                        "false\n",
                "SELECT 'foobar'::symbol IN (SELECT NULL);",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testSymbolInNullInFilter() throws Exception {
        assertQuery(
                "x\n",
                "SELECT x FROM long_sequence(3) WHERE 'foobar'::symbol IN NULL;",
                null,
                null,
                false,
                false
        );
    }

    @Test
    public void testSymbolInNullInSelect() throws Exception {
        assertQuery(
                "column\n" +
                        "false\n",
                "SELECT 'foobar'::symbol IN NULL;",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testSymbolInSymbolInFilter() throws Exception {
        assertQuery(
                "x\n" +
                        "1\n" +
                        "2\n" +
                        "3\n",
                "SELECT x FROM long_sequence(3) WHERE 'foobar'::symbol IN 'foobar'::symbol;",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testSymbolInSymbolInSelect() throws Exception {
        assertQuery(
                "column\n" +
                        "false\n",
                "SELECT 'foo'::symbol IN 'bar'::symbol;",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testUnsupportedColumnType() throws Exception {
        assertException(
                "select * from x where b in (select 12, rnd_str('RXGZ', 'HYRX', null) a from long_sequence(10))",
                "create table x as (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                24,
                "supported column types are VARCHAR, SYMBOL and STRING, found: INT"
        );
    }
}
