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

public class RegressionR2FunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testRegrR2AllNull() throws Exception {
        assertMemoryLeak(() -> assertQuery("select regr_r2(y, x) from (select cast(null as double) x, cast(null as double) y from long_sequence(100))")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("regr_r2\nnull\n"));
    }

    @Test
    public void testRegrR2AllSameValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select 17.2151921 x, 17.2151921 y from long_sequence(100))");
            assertQuery("select regr_r2(y, x) from tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("regr_r2\nnull\n");
        });
    }

    @Test
    public void testRegrR2AllSameX() throws Exception {
        // X is constant, Y varies. Sxx = 0 => regr_r2 is NULL per SQL:2003.
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select 5.0 x, cast(x as double) y from long_sequence(100))");
            assertQuery("select regr_r2(y, x) from tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("regr_r2\nnull\n");
        });
    }

    @Test
    public void testRegrR2AllSameY() throws Exception {
        // X varies, Y is constant. Sxx != 0, Syy = 0 => regr_r2 = 1.0 per SQL:2003
        // (a horizontal line is a perfect fit of constant Y).
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select cast(x as double) x, 5.0 y from long_sequence(100))");
            assertQuery("select regr_r2(y, x) from tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("regr_r2\n1.0\n");
        });
    }

    @Test
    public void testRegrR2DoubleValues() throws Exception {
        // Perfect linear relationship y = x => r^2 = 1.0
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select cast(x as double) x, cast(x as double) y from long_sequence(100))");
            assertQuery("select regr_r2(y, x) from tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("regr_r2\n1.0\n");
        });
    }

    @Test
    public void testRegrR2EqualsCorrSquared() throws Exception {
        // For any non-degenerate dataset, regr_r2(y, x) must equal corr(y, x)^2
        // (up to floating-point rounding). The two code paths compute the result
        // differently (direct ratio vs sqrt-then-square) so we allow tiny tolerance.
        assertMemoryLeak(() -> {
            execute(
                    "create table tbl1 as (" +
                            "select cast(x as double) x, " +
                            "cast(x * 2.5 + 7 + rnd_double() * 0.1 as double) y " +
                            "from long_sequence(1000))"
            );
            assertQuery("select round(regr_r2(y, x) - corr(y, x) * corr(y, x), 12) diff from tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("diff\n0.0\n");
        });
    }

    @Test
    public void testRegrR2ExplainPlan() throws Exception {
        // EXPLAIN renders the function via getName(), which is otherwise
        // not invoked by the data-path tests. Pins the function name shown
        // in query plans and SHOW FUNCTIONS output.
        assertMemoryLeak(() -> {
            execute("create table tbl1 (x double, y double)");
            assertQuery("select regr_r2(y, x) from tbl1")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async Group By workers: 1
                              vectorized: false
                              values: [regr_r2(y,x)]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tbl1
                            """);
        });
    }

    @Test
    public void testRegrR2FloatValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select cast(x as float) x, cast(x as float) y from long_sequence(100))");
            assertQuery("select regr_r2(y, x) from tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("regr_r2\n1.0\n");
        });
    }

    @Test
    public void testRegrR2GroupByMultipleGroups() throws Exception {
        // Single-threaded multi-group GROUP BY: confirms the per-key aggregate
        // state is kept separate across groups. With 1000 rows and the default
        // page-frame size the whole table fits in one frame, so this test does
        // NOT exercise the parallel merge() path; that coverage lives in
        // RegressionR2ParallelGroupByTest. Group A is a perfect line y = x
        // (r^2 = 1.0); group B has constant Y with varying X (Syy = 0, Sxx > 0
        // returns 1.0 by SQL:2003 §10.9).
        assertMemoryLeak(() -> {
            execute(
                    "create table tbl1 as (" +
                            "select " +
                            "  case when x % 2 = 0 then 'A' else 'B' end g, " +
                            "  cast(x as double) x, " +
                            "  case when x % 2 = 0 then cast(x as double) else 42.0 end y " +
                            "from long_sequence(1000))"
            );
            assertQuery("select g, regr_r2(y, x) from tbl1 order by g")
                    .noLeakCheck()
                    .expectSize()
                    .returns("g\tregr_r2\nA\t1.0\nB\t1.0\n");
        });
    }

    @Test
    public void testRegrR2IntValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select cast(x as int) x, cast(x as int) y from long_sequence(100))");
            assertQuery("select regr_r2(y, x) from tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("regr_r2\n1.0\n");
        });
    }

    @Test
    public void testRegrR2KnownDataset() throws Exception {
        // Hand-computed reference, also verified with PostgreSQL:
        //   X = {1, 2, 3, 4, 5}, Y = {2, 4, 5, 4, 5}
        //   mean(X) = 3, mean(Y) = 4
        //   Sxx = 10, Syy = 6, Sxy = 6
        //   regr_r2 = Sxy^2 / (Sxx * Syy) = 36 / 60 = 0.6
        assertMemoryLeak(() -> {
            execute("create table tbl1 (x double, y double)");
            execute("insert into tbl1 values (1, 2), (2, 4), (3, 5), (4, 4), (5, 5)");
            assertQuery("select round(regr_r2(y, x), 10) regr_r2 from tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("regr_r2\n0.6\n");
        });
    }

    @Test
    public void testRegrR2LargeValues() throws Exception {
        // Varying X with large magnitude over many rows exercises the actual
        // formula path (Sxy^2 / (Sxx * Syy)) under values where the running
        // accumulators reach magnitudes a constant-X dataset never produces.
        // For a perfectly linear y = 2x + 1e9 the result is r^2 = 1.0;
        // round() absorbs any 1-ulp drift from the Welford updates.
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select cast(x * 1e8 as double) x, cast(x * 2e8 + 1e9 as double) y from long_sequence(1_000_000))");
            assertQuery("select round(regr_r2(y, x), 10) regr_r2 from tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("regr_r2\n1.0\n");
        });
    }

    @Test
    public void testRegrR2NaNAndInfinityIgnored() throws Exception {
        // Numbers.isFinite() treats NaN, +Infinity, -Infinity, and QuestDB's
        // double NULL identically, so non-finite (y, x) pairs are skipped.
        // The remaining 100 rows form a perfect line y = x, so r^2 = 1.0.
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select cast(x as double) x, cast(x as double) y from long_sequence(100))");
            execute("insert into tbl1 values ('NaN'::double, 1.0), (1.0, 'NaN'::double), ('+Infinity'::double, 2.0), (2.0, '-Infinity'::double)");
            assertQuery("select regr_r2(y, x) from tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("regr_r2\n1.0\n");
        });
    }

    @Test
    public void testRegrR2NegativeSlope() throws Exception {
        // y = -2x + 5 is a perfect negative linear fit, so r^2 = 1.0.
        // A sign-flip bug in the Sxy accumulator would survive the r^2
        // squaring only if its magnitude were preserved; in practice a
        // wrong-sign update produces a magnitude collapse and r^2 drops
        // far from 1.0.
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select cast(x as double) x, cast(-2 * x + 5 as double) y from long_sequence(100))");
            assertQuery("select regr_r2(y, x) from tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("regr_r2\n1.0\n");
        });
    }

    @Test
    public void testRegrR2NoValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1(x int, y int)");
            assertQuery("select regr_r2(y, x) from tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("regr_r2\nnull\n");
        });
    }

    @Test
    public void testRegrR2OneColumnAllNull() throws Exception {
        assertMemoryLeak(() -> assertQuery("select regr_r2(y, x) from (select cast(null as double) x, x as y from long_sequence(100))")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("regr_r2\nnull\n"));
    }

    @Test
    public void testRegrR2OneValue() throws Exception {
        // Single pair => count = 1, Sxx = 0 => NULL.
        assertMemoryLeak(() -> {
            execute("create table tbl1(x double, y double)");
            execute("insert into tbl1 VALUES (17.2151920, 17.2151920)");
            assertQuery("select regr_r2(y, x) from tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("regr_r2\nnull\n");
        });
    }

    @Test
    public void testRegrR2OverflowDenominator() throws Exception {
        // Values near ±1e153: sumX and sumY are each ~2e306, their product overflows to
        // +Infinity. Before the fix sumXY² also overflows to +Inf, giving Inf/Inf = NaN.
        // The split-sqrt fallback correctly returns 1.0 for a perfect positive correlation.
        assertMemoryLeak(() -> {
            execute("create table tbl1(x double, y double)");
            execute("insert into tbl1 values (1e153, 1e153), (-1e153, -1e153)");
            assertQuery("select regr_r2(y, x) from tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("regr_r2\n1.0\n");
        });
    }

    @Test
    public void testRegrR2UnderflowDenominator() throws Exception {
        // Values near ±1e-150: sumX and sumY are each ~2e-300, their product underflows
        // to 0.0. Before the fix (sumXY * sumXY) / 0.0 = Infinity or NaN.
        // The split-sqrt fallback uses two sqrt roundings so the result can be
        // 0.9999999999999998 (within 2 ULP of 1.0); round() to 10 places confirms 1.0.
        assertMemoryLeak(() -> {
            execute("create table tbl1(x double, y double)");
            execute("insert into tbl1 values (1e-150, 1e-150), (-1e-150, -1e-150)");
            assertQuery("select round(regr_r2(y, x), 10) regr_r2 from tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("regr_r2\n1.0\n");
        });
    }

    @Test
    public void testRegrR2ClampAboveOne() throws Exception {
        // The split-sqrt path can produce r² marginally above 1.0 due to rounding drift.
        // For the overflow inputs in testRegrR2OverflowDenominator, the unclamped value
        // is 1.0000000000000004; the clamp (r2 > 1.0 ? 1.0 : r2) brings it to exactly 1.0.
        // Verify by asserting the result equals 1.0, not 1.0000000000000004.
        assertMemoryLeak(() -> {
            execute("create table tbl1(x double, y double)");
            // Same overflow-range inputs: without the clamp this would return 1.0000000000000004.
            execute("insert into tbl1 values (1e153, 1e153), (-1e153, -1e153)");
            assertQuery("select regr_r2(y, x) = 1.0 is_clamped from tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("is_clamped\ntrue\n");
        });
    }

    @Test
    public void testRegrR2SomeNull() throws Exception {
        // Null pairs are skipped; remaining 100 rows form a perfect line => r^2 = 1.0
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select cast(x as double) x, cast(x as double) y from long_sequence(100))");
            execute("insert into tbl1 VALUES (null, null)");
            assertQuery("select regr_r2(y, x) from tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("regr_r2\n1.0\n");
        });
    }
}
