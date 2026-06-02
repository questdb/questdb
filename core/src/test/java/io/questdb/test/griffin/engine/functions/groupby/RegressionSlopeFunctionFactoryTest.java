/*+*****************************************************************************
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

public class RegressionSlopeFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testRegrSlopeAllNull() throws Exception {
        assertMemoryLeak(() -> assertQuery("select regr_slope(y, x) from (select cast(null as double) x, cast(null as double) y from long_sequence(100))")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("regr_slope\nnull\n"));
    }

    @Test
    public void testRegrSlopeAllSameValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select 17.2151921 x, 17.2151921 y from long_sequence(100))");
            assertQuery("select regr_slope(x, y) from tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("regr_slope\nnull\n");
        });
    }

    @Test
    public void testRegrSlopeDoubleValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select cast(x as double) x, cast(x as double) y from long_sequence(100))");
            assertQuery("select regr_slope(x, y) from tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("regr_slope\n1.0\n");
        });
    }

    @Test
    public void testRegrSlopeExplainPlan() throws Exception {
        // EXPLAIN renders the function via getName(), which is otherwise not
        // invoked by the data-path tests. Pins the function name shown in
        // query plans and SHOW FUNCTIONS output.
        assertMemoryLeak(() -> {
            execute("create table tbl1 (x double, y double)");
            assertPlanNoLeakCheck(
                    "select regr_slope(y, x) from tbl1",
                    """
                            Async Group By workers: 1
                              vectorized: false
                              values: [regr_slope(y,x)]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tbl1
                            """
            );
        });
    }

    @Test
    public void testRegrSlopeFloatValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select cast(x as float) x, cast(x as float) y from long_sequence(100))");
            assertQuery("select regr_slope(x, y) from tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("regr_slope\n1.0\n");
        });
    }

    @Test
    public void testRegrSlopeIntValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select cast(x as int) x, cast(x as int) y from long_sequence(100))");
            assertQuery("select regr_slope(x, y) from tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("regr_slope\n1.0\n");
        });
    }

    @Test
    public void testRegrSlopeLargeValues() throws Exception {
        // Varying X with large magnitude over many rows exercises the formula
        // path under values where the running accumulators reach magnitudes a
        // constant-X dataset never produces. For y = 2x + 1e9 the slope is
        // 2.0; round() absorbs any 1-ulp drift from the Welford updates.
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select cast(x * 1e8 as double) x, cast(x * 2e8 + 1e9 as double) y from long_sequence(1_000_000))");
            assertQuery("select round(regr_slope(y, x), 10) regr_slope from tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("regr_slope\n2.0\n");
        });
    }

    @Test
    public void testRegrSlopeNaNAndInfinityIgnored() throws Exception {
        // Numbers.isFinite() treats NaN, +Infinity, -Infinity, and QuestDB's
        // double NULL identically, so non-finite (y, x) pairs are skipped.
        // The remaining 100 rows form a perfect line y = x, so slope = 1.0.
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select cast(x as double) x, cast(x as double) y from long_sequence(100))");
            execute("insert into tbl1 values ('NaN'::double, 1.0), (1.0, 'NaN'::double), ('+Infinity'::double, 2.0), (2.0, '-Infinity'::double)");
            assertQuery("select regr_slope(y, x) from tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("regr_slope\n1.0\n");
        });
    }

    @Test
    public void testRegrSlopeNegative() throws Exception {
        // y = -2x + 5: regressing y on x gives slope = -2.0. Catches a
        // sign-flip bug in the Sxy accumulator that y = x tests cannot.
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select cast(x as double) x, cast(-2 * x + 5 as double) y from long_sequence(100))");
            assertQuery("select regr_slope(y, x) from tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("regr_slope\n-2.0\n");
        });
    }

    @Test
    public void testRegrSlopeNoValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1(x int, y int)");
            assertQuery("select regr_slope(x, y) from tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("regr_slope\nnull\n");
        });
    }

    @Test
    public void testRegrSlopeOneColumnAllNull() throws Exception {
        assertMemoryLeak(() -> assertQuery("select regr_slope(x, y) from (select cast(null as double) x, x as y from long_sequence(100))")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("regr_slope\nnull\n"));
    }

    @Test
    public void testRegrSlopeOneValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1(x int, y int)");
            execute("insert into 'tbl1' VALUES (17.2151920, 17.2151920)");
            assertQuery("select regr_slope(x, y) from tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("regr_slope\nnull\n");
        });
    }

    @Test
    public void testRegrSlopeSomeNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select cast(x as double) x, cast(x as double) y from long_sequence(100))");
            execute("insert into 'tbl1' VALUES (null, null)");
            assertQuery("select regr_slope(x, y) from tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("regr_slope\n1.0\n");
        });
    }

    @Test
    public void testRegrSlopeWithNullValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1(x double, y double)");
            execute("insert into 'tbl1' VALUES (null, null)");
            execute("insert into 'tbl1' select x, x as y from long_sequence(100)");
            assertQuery("select regr_slope(x, y) from tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("regr_slope\n1.0\n");
        });
    }

}