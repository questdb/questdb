/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

public class StdDevSampleGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testStddevSampAllNull() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "stddev_samp\nNaN\n", "select stddev_samp(x) from (select cast(null as double) x from long_sequence(100))"
        ));
    }

    @Test
    public void testStddevSampAllSameValues() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tbl1 as (select 17.2151921 x from long_sequence(100))");
            assertSql(
                    "stddev_samp\n0.0\n", "select stddev_samp(x) from tbl1"
            );
        });
    }

    @Test
    public void testStddevSampDoubleValues() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tbl1 as (select cast(x as double) x from long_sequence(100))");
            assertSql(
                    "stddev_samp\n29.011491975882016\n", "select stddev_samp(x) from tbl1"
            );
        });
    }

    @Test
    public void testStddevSampFirstNull() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tbl1(x double)");
            insert("insert into 'tbl1' VALUES (null)");
            insert("insert into 'tbl1' select x from long_sequence(100)");
            assertSql(
                    "stddev_samp\n29.011491975882016\n", "select stddev_samp(x) from tbl1"
            );
        });
    }

    @Test
    public void testStddevSampFloatValues() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tbl1 as (select cast(x as float) x from long_sequence(100))");
            assertSql(
                    "stddev_samp\n29.011491975882016\n", "select stddev_samp(x) from tbl1"
            );
        });
    }

    @Test
    public void testStddevSampIntValues() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tbl1 as (select cast(x as int) x from long_sequence(100))");
            assertSql(
                    "stddev_samp\n29.011491975882016\n", "select stddev_samp(x) from tbl1"
            );
        });
    }

    @Test
    public void testStddevSampLong256Values() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tbl1 as (select x cast(x as long256) from long_sequence(100))");
            assertSql(
                    "stddev_samp\n29.011491975882016\n", "select stddev_samp(x) from tbl1"
            );
        });
    }

    @Test
    public void testStddevSampNoValues() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tbl1(x int)");
            assertSql(
                    "stddev_samp\nNaN\n", "select stddev_samp(x) from tbl1"
            );
        });
    }

    @Test
    public void testStddevSampOneValue() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tbl1(x int)");
            insert("insert into 'tbl1' VALUES " +
                    "(17.2151920)");
            assertSql(
                    "stddev_samp\nNaN\n", "select stddev_samp(x) from tbl1"
            );
        });
    }

    @Test
    public void testStddevSampOverflow() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tbl1 as (select 100000000 x from long_sequence(1000000))");
            assertSql(
                    "stddev_samp\n0.0\n", "select stddev_samp(x) from tbl1"
            );
        });
    }

    @Test
    public void testStddevSampSomeNull() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tbl1 as (select cast(x as double) x from long_sequence(100))");
            insert("insert into 'tbl1' VALUES (null)");
            assertSql(
                    "stddev_samp\n29.011491975882016\n", "select stddev_samp(x) from tbl1"
            );
        });
    }
}