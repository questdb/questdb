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

public class StdDevPopGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testStddevPopAllNull() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "stddev_pop\nnull\n", "select stddev_pop(x) from (select cast(null as double) x from long_sequence(100))"
        ));
    }

    @Test
    public void testStddevPopAllSameValues() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tbl1 as (select 17.2151921 x from long_sequence(100))");
            assertSql(
                    "stddev_pop\n0.0\n", "select stddev_pop(x) from tbl1"
            );
        });
    }

    @Test
    public void testStddevPopDoubleValues() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tbl1 as (select cast(x as double) x from long_sequence(100))");
            assertSql(
                    "stddev_pop\n28.86607004772212\n", "select stddev_pop(x) from tbl1"
            );
        });
    }

    @Test
    public void testStddevPopFirstNull() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tbl1(x double)");
            insert("insert into 'tbl1' VALUES (null)");
            insert("insert into 'tbl1' select x from long_sequence(100)");
            assertSql(
                    "stddev_pop\n28.86607004772212\n", "select stddev_pop(x) from tbl1"
            );
        });
    }

    @Test
    public void testStddevPopFloatValues() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tbl1 as (select cast(x as float) x from long_sequence(100))");
            assertSql(
                    "stddev_pop\n28.86607004772212\n", "select stddev_pop(x) from tbl1"
            );
        });
    }

    @Test
    public void testStddevPopIntValues() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tbl1 as (select cast(x as int) x from long_sequence(100))");
            assertSql(
                    "stddev_pop\n28.86607004772212\n", "select stddev_pop(x) from tbl1"
            );
        });
    }

    @Test
    public void testStddevPopLong256Values() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tbl1 as (select x cast(x as long256) from long_sequence(100))");
            assertSql(
                    "stddev_pop\n28.86607004772212\n", "select stddev_pop(x) from tbl1"
            );
        });
    }

    @Test
    public void testStddevPopNoValues() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tbl1(x int)");
            assertSql(
                    "stddev_pop\nnull\n", "select stddev_pop(x) from tbl1"
            );
        });
    }

    @Test
    public void testStddevPopOneValue() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tbl1(x int)");
            insert("insert into 'tbl1' VALUES " +
                    "(17.2151920)");
            assertSql(
                    "stddev_pop\n0.0\n", "select stddev_pop(x) from tbl1"
            );
        });
    }

    @Test
    public void testStddevPopOverflow() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tbl1 as (select 100000000 x from long_sequence(1000000))");
            assertSql(
                    "stddev_pop\n0.0\n", "select stddev_pop(x) from tbl1"
            );
        });
    }

    @Test
    public void testStddevPopSomeNull() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tbl1 as (select cast(x as double) x from long_sequence(100))");
            insert("insert into 'tbl1' VALUES (null)");
            assertSql(
                    "stddev_pop\n28.86607004772212\n", "select stddev_pop(x) from tbl1"
            );
        });
    }
}