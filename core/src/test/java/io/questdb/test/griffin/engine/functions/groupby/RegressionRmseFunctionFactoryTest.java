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

public class RegressionRmseFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testRegrRmseAllNull() throws Exception {
        assertMemoryLeak(() -> assertQuery("select regr_rmse(y, x) from (select null::double x, null::double y from long_sequence(100))")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("regr_rmse\nnull\n"));
    }

    @Test
    public void testRegrRmseConstantX() throws Exception {
        // X does not vary, so no regression line exists and the result is NULL.
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select 5.0 x, x::double y from long_sequence(100))");
            assertQuery("select regr_rmse(y, x) from tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("regr_rmse\nnull\n");
        });
    }

    @Test
    public void testRegrRmseExplainPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1 (x double, y double)");
            assertQuery("select regr_rmse(y, x) from tbl1")
                    .noLeakCheck()
                    .assertsPlanContaining("regr_rmse(y,x)");
        });
    }

    @Test
    public void testRegrRmseNoValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1(x double, y double)");
            assertQuery("select regr_rmse(y, x) from tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("regr_rmse\nnull\n");
        });
    }

    @Test
    public void testRegrRmsePerfectFit() throws Exception {
        // y = 2x + 1 has zero residuals, so RMSE is exactly 0.
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select x::double x, (2 * x + 1)::double y from long_sequence(100))");
            assertQuery("select regr_rmse(y, x) from tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("regr_rmse\n0.0\n");
        });
    }

    @Test
    public void testRegrRmseScatter() throws Exception {
        // Points (1,1),(2,3),(3,2),(4,5),(5,4): Sxx=10, Syy=10, Sxy=8.
        // SSE = Syy - Sxy^2/Sxx = 10 - 6.4 = 3.6, RMSE = sqrt(3.6/5) = sqrt(0.72).
        assertMemoryLeak(() -> {
            execute("create table tbl1(x double, y double)");
            execute("insert into tbl1 values (1,1),(2,3),(3,2),(4,5),(5,4)");
            assertQuery("select round(regr_rmse(y, x), 6) regr_rmse from tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("regr_rmse\n0.848528\n");
        });
    }
}
