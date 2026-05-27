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

package io.questdb.test.griffin.engine.functions.groupby.regression;

import io.questdb.PropertyKey;
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * Shared deterministic parallel GROUP BY scenarios for regression aggregates
 * ({@code regr_slope}, {@code regr_intercept}, {@code regr_r2}). Subclasses
 * supply the SQL function name and the per-function expected values; the base
 * class drives identical data shapes through every implementation so that the
 * {@code merge()} path in {@link io.questdb.griffin.engine.functions.groupby.AbstractRegressionGroupByFunction}
 * is exercised with the same inputs across the family.
 * <p>
 * The {@link #setUp()} forces parallel GROUP BY on with a small page-frame
 * size and a sharding threshold of 1, so even a 4000-row table is split into
 * many frames distributed across worker partials. Multi-shard merges run
 * inside the keyed test; non-keyed (single-output) merges run inside the
 * sparse-null test.
 */
public abstract class AbstractRegressionParallelGroupByTest extends AbstractCairoTest {

    protected abstract String expectedKeyedSparseNulls();

    protected abstract String expectedPerfectLine();

    protected abstract String expectedSparseNulls();

    protected abstract String funcName();

    @Override
    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 1);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WORK_STEALING_THRESHOLD, 1);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 64);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT, 2);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 2);
        super.setUp();
    }

    /**
     * Every row has NULL for both arguments. Every worker partial ends with
     * count = 0 and isNew = false, so {@code merge()} traverses the
     * srcCount == 0 / destCount == 0 fast paths exclusively. The aggregate
     * must return NULL.
     */
    @Test
    public void testParallelAllNull() throws Exception {
        assertMemoryLeak(() -> {
            try (WorkerPool pool = new WorkerPool(() -> 4)) {
                TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                    execute(
                            compiler,
                            "create table tbl as (" +
                                    "select cast(null as double) x, cast(null as double) y from long_sequence(4_000))",
                            sqlExecutionContext
                    );
                    assertQueryNoLeakCheck(
                            compiler,
                            funcName() + "\nnull\n",
                            "select " + funcName() + "(y, x) from tbl",
                            null,
                            sqlExecutionContext,
                            false,
                            true
                    );
                }, configuration, LOG);
            }
        });
    }

    /**
     * Keyed GROUP BY with three keys. Key 'A' carries a clean y = 2x + 5 line
     * (slope = 2.0, intercept = 5.0, r&sup2; = 1.0). Keys 'B' and 'C' carry
     * only NULL pairs. With sharding threshold = 1, the per-shard merge fires
     * on every group, and the all-NULL keys exercise the empty-partial guards
     * in {@code merge()} from inside the sharded merger. Results are rounded
     * to 10 places to absorb the harmless rounding drift that parallel
     * pairwise merging introduces relative to a single-threaded run.
     */
    @Test
    public void testParallelKeyedSparseNulls() throws Exception {
        assertMemoryLeak(() -> {
            try (WorkerPool pool = new WorkerPool(() -> 4)) {
                TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                    execute(
                            compiler,
                            "create table tbl as (" +
                                    "select " +
                                    "  case when x % 3 = 0 then 'A' when x % 3 = 1 then 'B' else 'C' end g, " +
                                    "  case when x % 3 = 0 then cast(x as double) else cast(null as double) end x_v, " +
                                    "  case when x % 3 = 0 then cast(2 * x + 5 as double) else cast(null as double) end y_v " +
                                    "from long_sequence(4_000))",
                            sqlExecutionContext
                    );
                    assertQueryNoLeakCheck(
                            compiler,
                            expectedKeyedSparseNulls(),
                            "select g, round(" + funcName() + "(y_v, x_v), 10) " + funcName() + " from tbl order by g",
                            null,
                            sqlExecutionContext,
                            true,
                            true
                    );
                }, configuration, LOG);
            }
        });
    }

    /**
     * Dense y = 2x + 5 over the whole table, no NULLs. Every worker partial
     * has count &gt; 0, so {@code merge()} runs the full Chan formula on every
     * iteration. Pins basic parallel correctness for the family. Rounded to
     * 10 places to absorb parallel-merge rounding drift.
     */
    @Test
    public void testParallelPerfectLine() throws Exception {
        assertMemoryLeak(() -> {
            try (WorkerPool pool = new WorkerPool(() -> 4)) {
                TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                    execute(
                            compiler,
                            "create table tbl as (" +
                                    "select cast(x as double) x, cast(2 * x + 5 as double) y from long_sequence(4_000))",
                            sqlExecutionContext
                    );
                    assertQueryNoLeakCheck(
                            compiler,
                            funcName() + "\n" + expectedPerfectLine() + "\n",
                            "select round(" + funcName() + "(y, x), 10) " + funcName() + " from tbl",
                            null,
                            sqlExecutionContext,
                            false,
                            true
                    );
                }, configuration, LOG);
            }
        });
    }

    /**
     * 3 990 NULL rows plus 10 real (y = 2x + 5) rows clustered in one frame.
     * Multiple worker partials end with count = 0 while a single worker
     * carries the real data, so {@code merge()} hits the
     * {@code srcCount == 0 → return} and {@code destCount == 0 → copy} guards
     * before integrating the real partial. Reproduces the NaN-poisoning bug
     * the guards fix; without them this query returned NULL. Rounded to 10
     * places to absorb parallel-merge rounding drift.
     * <p>
     * The query runs eight times because the {@code destCount == 0} guard
     * fires only when slot 0 is assigned an all-NULL worker, which is roughly
     * a 75% event per run; eight independent attempts drive the coverage
     * miss rate below 0.01%.
     */
    @Test
    public void testParallelSparseNulls() throws Exception {
        assertMemoryLeak(() -> {
            try (WorkerPool pool = new WorkerPool(() -> 4)) {
                TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                    execute(
                            compiler,
                            "create table tbl as (" +
                                    "select " +
                                    "  case when x between 1500 and 1509 then cast(x as double) else cast(null as double) end x, " +
                                    "  case when x between 1500 and 1509 then cast(2 * x + 5 as double) else cast(null as double) end y " +
                                    "from long_sequence(4_000))",
                            sqlExecutionContext
                    );
                    for (int run = 0; run < 8; run++) {
                        assertQueryNoLeakCheck(
                                compiler,
                                funcName() + "\n" + expectedSparseNulls() + "\n",
                                "select round(" + funcName() + "(y, x), 10) " + funcName() + " from tbl",
                                null,
                                sqlExecutionContext,
                                false,
                                true
                        );
                    }
                }, configuration, LOG);
            }
        });
    }
}
