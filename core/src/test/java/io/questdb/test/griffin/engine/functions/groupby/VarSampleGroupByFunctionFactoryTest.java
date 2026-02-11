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

public class VarSampleGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testVarSampleAllNull() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "var_samp\nnull\n", "select var_samp(x) from (select cast(null as double) x from long_sequence(100))"
        ));
    }

    @Test
    public void testVarSampleAllSameValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select 17.2151921 x from long_sequence(100))");
            assertSql(
                    "var_samp\n0.0\n", "select var_samp(x) from tbl1"
            );
        });
    }

    @Test
    public void testVarSampleDoubleValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select cast(x as double) x from long_sequence(100))");
            assertSql(
                    "var_samp\n841.6666666666666\n", "select var_samp(x) from tbl1"
            );
        });
    }

    @Test
    public void testVarSampleFirstNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1(x double)");
            execute("insert into 'tbl1' VALUES (null)");
            execute("insert into 'tbl1' select x from long_sequence(100)");
            assertSql(
                    "var_samp\n841.6666666666666\n", "select var_samp(x) from tbl1"
            );
        });
    }

    @Test
    public void testVarSampleFloatValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select cast(x as float) x from long_sequence(100))");
            assertSql(
                    "var_samp\n841.6666666666666\n", "select var_samp(x) from tbl1"
            );
        });
    }

    @Test
    public void testVarSampleIntValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select cast(x as int) x from long_sequence(100))");
            assertSql(
                    "var_samp\n841.6666666666666\n", "select var_samp(x) from tbl1"
            );
        });
    }

    @Test
    public void testVarSampleNoValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1(x int)");
            assertSql(
                    "var_samp\nnull\n", "select var_samp(x) from tbl1"
            );
        });
    }

    @Test
    public void testVarSampleOneValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1(x int)");
            execute("insert into 'tbl1' VALUES " +
                    "(17.2151920)");
            assertSql(
                    "var_samp\nnull\n", "select var_samp(x) from tbl1"
            );
        });
    }

    @Test
    public void testVarSampleOverflow() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select 100000000 x from long_sequence(1000000))");
            assertSql(
                    "var_samp\n0.0\n", "select var_samp(x) from tbl1"
            );
        });
    }

    @Test
    public void testVarSampleSomeNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select cast(x as double) x from long_sequence(100))");
            execute("insert into 'tbl1' VALUES (null)");
            assertSql(
                    "var_samp\n841.6666666666666\n", "select var_samp(x) from tbl1"
            );
        });
    }
}
