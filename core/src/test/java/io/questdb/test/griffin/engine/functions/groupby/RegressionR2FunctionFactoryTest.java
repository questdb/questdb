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
        assertMemoryLeak(() -> assertSql(
                "regr_r2\nnull\n",
                "select regr_r2(y, x) from (select cast(null as double) x, cast(null as double) y from long_sequence(100))"
        ));
    }

    @Test
    public void testRegrR2AllSameValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select 17.2151921 x, 17.2151921 y from long_sequence(100))");
            assertSql(
                    "regr_r2\nnull\n",
                    "select regr_r2(y, x) from tbl1"
            );
        });
    }

    @Test
    public void testRegrR2AllSameX() throws Exception {
        // X is constant, Y varies. Sxx = 0 => regr_r2 is NULL per SQL:2003.
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select 5.0 x, cast(x as double) y from long_sequence(100))");
            assertSql(
                    "regr_r2\nnull\n",
                    "select regr_r2(y, x) from tbl1"
            );
        });
    }

    @Test
    public void testRegrR2AllSameY() throws Exception {
        // X varies, Y is constant. Sxx != 0, Syy = 0 => regr_r2 = 1.0 per SQL:2003
        // (a horizontal line is a perfect fit of constant Y).
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select cast(x as double) x, 5.0 y from long_sequence(100))");
            assertSql(
                    "regr_r2\n1.0\n",
                    "select regr_r2(y, x) from tbl1"
            );
        });
    }

    @Test
    public void testRegrR2DoubleValues() throws Exception {
        // Perfect linear relationship y = x => r^2 = 1.0
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select cast(x as double) x, cast(x as double) y from long_sequence(100))");
            assertSql(
                    "regr_r2\n1.0\n",
                    "select regr_r2(y, x) from tbl1"
            );
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
            assertSql(
                    "diff\n0.0\n",
                    "select round(regr_r2(y, x) - corr(y, x) * corr(y, x), 12) diff from tbl1"
            );
        });
    }

    @Test
    public void testRegrR2FloatValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select cast(x as float) x, cast(x as float) y from long_sequence(100))");
            assertSql(
                    "regr_r2\n1.0\n",
                    "select regr_r2(y, x) from tbl1"
            );
        });
    }

    @Test
    public void testRegrR2GroupByMultipleGroups() throws Exception {
        // Multiple groups exercise the merge() path. Each group has y = x => r^2 = 1.0
        // for group A (perfect line) and constant Y for group B (r^2 = 1.0 by SQL:2003).
        assertMemoryLeak(() -> {
            execute(
                    "create table tbl1 as (" +
                    "select " +
                    "  case when x % 2 = 0 then 'A' else 'B' end g, " +
                    "  cast(x as double) x, " +
                    "  case when x % 2 = 0 then cast(x as double) else 42.0 end y " +
                    "from long_sequence(1000))"
            );
            assertSql(
                    "g\tregr_r2\nA\t1.0\nB\t1.0\n",
                    "select g, regr_r2(y, x) from tbl1 order by g"
            );
        });
    }

    @Test
    public void testRegrR2IntValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select cast(x as int) x, cast(x as int) y from long_sequence(100))");
            assertSql(
                    "regr_r2\n1.0\n",
                    "select regr_r2(y, x) from tbl1"
            );
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
            assertSql(
                    "regr_r2\n0.6\n",
                    "select round(regr_r2(y, x), 10) regr_r2 from tbl1"
            );
        });
    }

    @Test
    public void testRegrR2NoOverflow() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select 100000000 x, 100000000 y from long_sequence(1_000_000))");
            assertSql(
                    "regr_r2\nnull\n",
                    "select regr_r2(y, x) from tbl1"
            );
        });
    }

    @Test
    public void testRegrR2NoValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1(x int, y int)");
            assertSql(
                    "regr_r2\nnull\n",
                    "select regr_r2(y, x) from tbl1"
            );
        });
    }

    @Test
    public void testRegrR2OneColumnAllNull() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "regr_r2\nnull\n",
                "select regr_r2(y, x) from (select cast(null as double) x, x as y from long_sequence(100))"
        ));
    }

    @Test
    public void testRegrR2OneValue() throws Exception {
        // Single pair => count = 1, Sxx = 0 => NULL.
        assertMemoryLeak(() -> {
            execute("create table tbl1(x double, y double)");
            execute("insert into tbl1 VALUES (17.2151920, 17.2151920)");
            assertSql(
                    "regr_r2\nnull\n",
                    "select regr_r2(y, x) from tbl1"
            );
        });
    }

    @Test
    public void testRegrR2SomeNull() throws Exception {
        // Null pairs are skipped; remaining 100 rows form a perfect line => r^2 = 1.0
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select cast(x as double) x, cast(x as double) y from long_sequence(100))");
            execute("insert into tbl1 VALUES (null, null)");
            assertSql(
                    "regr_r2\n1.0\n",
                    "select regr_r2(y, x) from tbl1"
            );
        });
    }
}
