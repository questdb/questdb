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

public class RegressionTStatsFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testRegrTStatsAllNull() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "regr_tstats\nnull\n", "select regr_tstats(y, x) from (select cast(null as double) x, cast(null as double) y from long_sequence(100))"
        ));
    }

    @Test
    public void testRegrTStatsNoValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1(x int, y int)");
            assertSql(
                    "regr_tstats\nnull\n", "select regr_tstats(x, y) from tbl1"
            );
        });
    }

    @Test
    public void testRegrTStatsAllSameValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select 17.2151921 x, 17.2151921 y from long_sequence(100))");
            assertSql(
                    "regr_tstats\nnull\n", "select regr_tstats(x, y) from tbl1"
            );
        });
    }

    @Test
    public void testRegrTStatsPerfectFit() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select cast(x as double) x, cast(x as double) y from long_sequence(100))");
            // For a perfect fit with slope=1 and no error, t-stat should be infinite (displayed as null)
            assertSql(
                    "regr_tstats\nnull\n", "select regr_tstats(x, y) from tbl1"
            );
        });
    }

    @Test
    public void testRegrTStatsWithNullValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1(x double, y double)");
            execute("insert into 'tbl1' VALUES (null, null)");
            execute("insert into 'tbl1' select x, x as y from long_sequence(100)");
            assertSql(
                    "regr_tstats\nnull\n", "select regr_tstats(x, y) from tbl1"
            );
        });
    }

    @Test
    public void testRegrTStatsFloatValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select cast(x as float) x, cast(x as float) y from long_sequence(100))");
            // For a perfect float fit, t-stat is infinite (displayed as null in QuestDB)
            assertSql(
                    "regr_tstats\nnull\n", "select regr_tstats(x, y) from tbl1"
            );
        });
    }

    @Test
    public void testRegrTStatsIntValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select cast(x as int) x, cast(x as int) y from long_sequence(100))");
            // For a perfect int fit, t-stat is infinite (displayed as null in QuestDB)
            assertSql(
                    "regr_tstats\nnull\n", "select regr_tstats(x, y) from tbl1"
            );
        });
    }

    @Test
    public void testRegrTStatsOneColumnAllNull() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "regr_tstats\nnull\n", "select regr_tstats(x, y) from (select cast(null as double) x, x as y from long_sequence(100))"
        ));
    }

    @Test
    public void testRegrTStatsOneValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1(x int, y int)");
            execute("insert into 'tbl1' VALUES (17.2151920, 17.2151920)");
            assertSql(
                    "regr_tstats\nnull\n", "select regr_tstats(x, y) from tbl1"
            );
        });
    }

    @Test
    public void testRegrTStatsNoOverflow() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select 100000000 x, 100000000 y from long_sequence(1000000))");
            assertSql(
                    "regr_tstats\nnull\n", "select regr_tstats(x, y) from tbl1"
            );
        });
    }

    @Test
    public void testRegrTStatsSomeNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select cast(x as double) x, cast(x as double) y from long_sequence(100))");
            execute("insert into 'tbl1' VALUES (null, null)");
            assertSql(
                    "regr_tstats\nnull\n", "select regr_tstats(x, y) from tbl1"
            );
        });
    }

    @Test
    public void testRegrTStatsLinearWithIntercept() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select x, 2 * x + 5 as y from long_sequence(100))");
            // For a perfect linear relationship with slope=2 and no error, t-stat is infinite (displayed as null in QuestDB)
            assertSql(
                    "regr_tstats\nnull\n", "select regr_tstats(x, y) from tbl1"
            );
        });
    }

    @Test
    public void testRegrTStatsTwoValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1(x double, y double)");
            execute("insert into 'tbl1' VALUES (1.0, 2.0)");
            execute("insert into 'tbl1' VALUES (2.0, 4.0)");
            // With 2 values and perfect fit, t-stat is infinite (displayed as null in QuestDB)
            assertSql(
                    "regr_tstats\nnull\n", "select regr_tstats(x, y) from tbl1"
            );
        });
    }

}
