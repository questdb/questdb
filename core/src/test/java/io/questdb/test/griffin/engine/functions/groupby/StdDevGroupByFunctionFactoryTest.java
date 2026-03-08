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

public class StdDevGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testStddevAllNull() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "stddev\nnull\n", "select stddev(x) from (select cast(null as double) x from long_sequence(100))"
        ));
    }

    @Test
    public void testStddevAllSameValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select 17.2151921 x from long_sequence(100))");
            assertSql(
                    "stddev\n0.0\n", "select stddev(x) from tbl1"
            );
        });
    }

    @Test
    public void testStddevDoubleValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select cast(x as double) x from long_sequence(100))");
            assertSql(
                    "stddev\n29.011491975882016\n", "select stddev(x) from tbl1"
            );
        });
    }

    @Test
    public void testStddevFirstNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1(x double)");
            execute("insert into 'tbl1' VALUES (null)");
            execute("insert into 'tbl1' select x from long_sequence(100)");
            assertSql(
                    "stddev\n29.011491975882016\n", "select stddev(x) from tbl1"
            );
        });
    }

    @Test
    public void testStddevFloatValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select cast(x as float) x from long_sequence(100))");
            assertSql(
                    "stddev\n29.011491975882016\n", "select stddev(x) from tbl1"
            );
        });
    }

    @Test
    public void testStddevIntValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select cast(x as int) x from long_sequence(100))");
            assertSql(
                    "stddev\n29.011491975882016\n", "select stddev(x) from tbl1"
            );
        });
    }

    @Test
    public void testStddevNoValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1(x int)");
            assertSql(
                    "stddev\nnull\n", "select stddev(x) from tbl1"
            );
        });
    }

    @Test
    public void testStddevOneValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1(x int)");
            execute("insert into 'tbl1' VALUES " +
                    "(17.2151920)");
            assertSql(
                    "stddev\nnull\n", "select stddev(x) from tbl1"
            );
        });
    }

    @Test
    public void testStddevOverflow() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select 100000000 x from long_sequence(1000000))");
            assertSql(
                    "stddev\n0.0\n", "select stddev(x) from tbl1"
            );
        });
    }

    @Test
    public void testStddevSomeNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select cast(x as double) x from long_sequence(100))");
            execute("insert into 'tbl1' VALUES (null)");
            assertSql(
                    "stddev\n29.011491975882016\n", "select stddev(x) from tbl1"
            );
        });
    }
}
