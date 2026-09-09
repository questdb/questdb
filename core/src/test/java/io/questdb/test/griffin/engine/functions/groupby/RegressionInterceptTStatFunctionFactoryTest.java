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

public class RegressionInterceptTStatFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testRegrInterceptTStatAllNull() throws Exception {
        assertMemoryLeak(() -> assertQuery("select regr_intercept_tstat(y, x) from (select null::double x, null::double y from long_sequence(100))")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("regr_intercept_tstat\nnull\n"));
    }

    @Test
    public void testRegrInterceptTStatConstantX() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select 5.0 x, x::double y from long_sequence(100))");
            assertQuery("select regr_intercept_tstat(y, x) from tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("regr_intercept_tstat\nnull\n");
        });
    }

    @Test
    public void testRegrInterceptTStatExplainPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1 (x double, y double)");
            assertQuery("select regr_intercept_tstat(y, x) from tbl1")
                    .noLeakCheck()
                    .assertsPlanContaining("regr_intercept_tstat(y,x)");
        });
    }

    @Test
    public void testRegrInterceptTStatPerfectFit() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select x::double x, (2 * x + 1)::double y from long_sequence(100))");
            assertQuery("select regr_intercept_tstat(y, x) from tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("regr_intercept_tstat\nnull\n");
        });
    }

    @Test
    public void testRegrInterceptTStatScatter() throws Exception {
        // Points (1,1),(2,3),(3,2),(4,5),(5,4): intercept=0.6, SSE=3.6, n=5, meanX=3, Sxx=10.
        // SE(intercept) = sqrt((3.6/3)*(1/5 + 9/10)) = sqrt(1.32);
        // t = 0.6/sqrt(1.32) = 0.522233.
        assertMemoryLeak(() -> {
            execute("create table tbl1(x double, y double)");
            execute("insert into tbl1 values (1,1),(2,3),(3,2),(4,5),(5,4)");
            assertQuery("select round(regr_intercept_tstat(y, x), 6) regr_intercept_tstat from tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("regr_intercept_tstat\n0.522233\n");
        });
    }

    @Test
    public void testRegrInterceptTStatTooFewRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1(x double, y double)");
            execute("insert into tbl1 values (1,1),(2,3)");
            assertQuery("select regr_intercept_tstat(y, x) from tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("regr_intercept_tstat\nnull\n");
        });
    }
}
