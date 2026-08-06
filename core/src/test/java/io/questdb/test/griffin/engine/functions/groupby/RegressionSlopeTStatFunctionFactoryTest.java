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

public class RegressionSlopeTStatFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testRegrSlopeTStatAllNull() throws Exception {
        assertMemoryLeak(() -> assertQuery("select regr_slope_tstat(y, x) from (select cast(null as double) x, cast(null as double) y from long_sequence(100))")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("regr_slope_tstat\nnull\n"));
    }

    @Test
    public void testRegrSlopeTStatConstantX() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select 5.0 x, cast(x as double) y from long_sequence(100))");
            assertQuery("select regr_slope_tstat(y, x) from tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("regr_slope_tstat\nnull\n");
        });
    }

    @Test
    public void testRegrSlopeTStatExplainPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1 (x double, y double)");
            assertQuery("select regr_slope_tstat(y, x) from tbl1")
                    .noLeakCheck()
                    .assertsPlanContaining("regr_slope_tstat(y,x)");
        });
    }

    @Test
    public void testRegrSlopeTStatPerfectFit() throws Exception {
        // A perfect fit leaves the standard error at zero, so the t-statistic is NULL.
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select cast(x as double) x, cast(2 * x + 1 as double) y from long_sequence(100))");
            assertQuery("select regr_slope_tstat(y, x) from tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("regr_slope_tstat\nnull\n");
        });
    }

    @Test
    public void testRegrSlopeTStatScatter() throws Exception {
        // Points (1,1),(2,3),(3,2),(4,5),(5,4): slope=0.8, SSE=3.6, n=5.
        // SE(slope) = sqrt((3.6/3)/10) = sqrt(0.12); t = 0.8/sqrt(0.12) = 2.309401.
        assertMemoryLeak(() -> {
            execute("create table tbl1(x double, y double)");
            execute("insert into tbl1 values (1,1),(2,3),(3,2),(4,5),(5,4)");
            assertQuery("select round(regr_slope_tstat(y, x), 6) regr_slope_tstat from tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("regr_slope_tstat\n2.309401\n");
        });
    }

    @Test
    public void testRegrSlopeTStatTooFewRows() throws Exception {
        // Fewer than three points gives no residual degrees of freedom.
        assertMemoryLeak(() -> {
            execute("create table tbl1(x double, y double)");
            execute("insert into tbl1 values (1,1),(2,3)");
            assertQuery("select regr_slope_tstat(y, x) from tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("regr_slope_tstat\nnull\n");
        });
    }
}
