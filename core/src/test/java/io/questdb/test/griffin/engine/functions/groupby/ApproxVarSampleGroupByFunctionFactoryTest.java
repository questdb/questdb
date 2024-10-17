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

public class ApproxVarSampleGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testApproxVarSampleAllNull() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "approx_var_samp\nnull\n", "select approx_var_samp(x) from (select cast(null as double) x from long_sequence(100))"
        ));
    }

    @Test
    public void testApproxVarSampleAllSameValues() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tbl1 as (select 17.2151921 x from long_sequence(100))");
            assertSql(
                    "approx_var_samp\n0.0\n", "select approx_var_samp(x) from tbl1"
            );
        });
    }

    @Test
    public void testApproxVarSampleDoubleValues() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tbl1 as (select cast(x as double) x from long_sequence(100))");
            assertSql(
                    "approx_var_samp\n841.6666666666666\n", "select approx_var_samp(x) from tbl1"
            );
        });
    }

    @Test
    public void testApproxVarSampleFirstNull() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tbl1(x double)");
            insert("insert into 'tbl1' VALUES (null)");
            insert("insert into 'tbl1' select x from long_sequence(100)");
            assertSql(
                    "approx_var_samp\n841.6666666666666\n", "select approx_var_samp(x) from tbl1"
            );
        });
    }

    @Test
    public void testApproxVarSampleFloatValues() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tbl1 as (select cast(x as float) x from long_sequence(100))");
            assertSql(
                    "approx_var_samp\n841.6666666666666\n", "select approx_var_samp(x) from tbl1"
            );
        });
    }

    @Test
    public void testApproxVarSampleIntValues() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tbl1 as (select cast(x as int) x from long_sequence(100))");
            assertSql(
                    "approx_var_samp\n841.6666666666666\n", "select approx_var_samp(x) from tbl1"
            );
        });
    }

    @Test
    public void testApproxVarSampleLong256Values() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tbl1 as (select x cast(x as long256) from long_sequence(100))");
            assertSql(
                    "approx_var_samp\n841.6666666666666\n", "select approx_var_samp(x) from tbl1"
            );
        });
    }

    @Test
    public void testApproxVarSampleNoValues() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tbl1(x int)");
            assertSql(
                    "approx_var_samp\nnull\n", "select approx_var_samp(x) from tbl1"
            );
        });
    }

    @Test
    public void testApproxVarSampleOneValue() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tbl1(x int)");
            insert("insert into 'tbl1' VALUES " +
                    "(17.2151920)");
            assertSql(
                    "approx_var_samp\nnull\n", "select approx_var_samp(x) from tbl1"
            );
        });
    }

    @Test
    public void testApproxVarSampleOverflow() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tbl1 as (select 100000000 x from long_sequence(1000000))");
            assertSql(
                    "approx_var_samp\n0.0\n", "select approx_var_samp(x) from tbl1"
            );
        });
    }

    @Test
    public void testApproxVarSampleSomeNull() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tbl1 as (select cast(x as double) x from long_sequence(100))");
            insert("insert into 'tbl1' VALUES (null)");
            assertSql(
                    "approx_var_samp\n841.6666666666666\n", "select approx_var_samp(x) from tbl1"
            );
        });
    }
}