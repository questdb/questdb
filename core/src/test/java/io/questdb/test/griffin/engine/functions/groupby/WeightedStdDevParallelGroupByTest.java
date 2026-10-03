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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

// Exercises AbstractWeightedStdDevGroupByFunction.merge() with negative weights. A group that holds a negative
// weight returns NULL, whatever the row order and however the rows split into partial results, so serial and
// parallel GROUP BY agree. Before the fix the result depended on both, because the running weight sum could
// reach zero, which the merge read as "no data" and computeNext() divided by.
public class WeightedStdDevParallelGroupByTest extends AbstractCairoTest {
    private static final String FUNCTIONS = """
            round(weighted_stddev_rel(v, w), 11) rel,
            round(weighted_stddev(v, w), 11) alias,
            round(weighted_stddev_freq(v, w), 11) freq""";

    @Test
    public void testNegativeWeightKeyed() throws Exception {
        runWithPool((db, ctx) -> {
            createTable(db, ctx);
            for (int threshold : new int[]{Integer.MAX_VALUE, 1}) {
                // Threshold 1 shards the keyed maps, so partial results also merge per shard.
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, threshold);
                assertSerialAndParallel(
                        db,
                        ctx,
                        "SELECT k, " + FUNCTIONS + " FROM t ORDER BY k",
                        true,
                        """
                                k\trel\talias\tfreq
                                0\t3.7495149808\t3.7495149808\t3.74726099389
                                1\tnull\tnull\tnull
                                2\tnull\tnull\tnull
                                """
                );
            }
        });
    }

    @Test
    public void testNegativeWeightNotKeyed() throws Exception {
        runWithPool((db, ctx) -> {
            createTable(db, ctx);
            assertSerialAndParallel(
                    db,
                    ctx,
                    "SELECT " + FUNCTIONS + " FROM t",
                    false,
                    """
                            rel\talias\tfreq
                            null\tnull\tnull
                            """
            );
            // Key 0 holds positive weights only.
            assertSerialAndParallel(
                    db,
                    ctx,
                    "SELECT " + FUNCTIONS + " FROM t WHERE k = 0",
                    false,
                    """
                            rel\talias\tfreq
                            3.7495149808\t3.7495149808\t3.74726099389
                            """
            );
        });
    }

    private static void createTable(CairoEngine db, SqlExecutionContext ctx) throws Exception {
        // Keys 1 and 2 hold weights that repeat 10, -3, -6, 6, so partial weight sums reach zero whenever
        // a partial result holds just the -6 and the 6. Key 0 holds weights 1 to 4. One row per minute in
        // hourly partitions, and page frames of at most 2 rows, split every key over hundreds of partial
        // results.
        db.execute(
                """
                        CREATE TABLE t AS (
                            SELECT
                                (x % 3)::INT k,
                                (x % 13 - 6)::INT v,
                                CASE
                                    WHEN x % 3 = 0 THEN (x % 4 + 1)::INT
                                    WHEN x % 4 = 0 THEN 10
                                    WHEN x % 4 = 1 THEN -3
                                    WHEN x % 4 = 2 THEN -6
                                    ELSE 6
                                END w,
                                timestamp_sequence(0, 60_000_000L) ts
                            FROM long_sequence(2_000)
                        ) TIMESTAMP(ts) PARTITION BY HOUR""",
                ctx
        );
        ctx.changePageFrameSizes(1, 2);
    }

    private void assertSerialAndParallel(
            CairoEngine db,
            SqlExecutionContext ctx,
            String query,
            boolean hasRandomAccess,
            String expected
    ) throws Exception {
        try {
            ctx.setParallelGroupByEnabled(false);
            assertQuery(query)
                    .noLeakCheck()
                    .withEngine(db)
                    .withContext(ctx)
                    .withPlanContaining("GroupBy")
                    .supportsRandomAccess(hasRandomAccess)
                    .expectSize()
                    .returns(expected);
            ctx.setParallelGroupByEnabled(true);
            assertQuery(query)
                    .noLeakCheck()
                    .withEngine(db)
                    .withContext(ctx)
                    .withPlanContaining("Group By workers: 4")
                    .supportsRandomAccess(hasRandomAccess)
                    .expectSize()
                    .returns(expected);
        } finally {
            ctx.setParallelGroupByEnabled(true);
        }
    }

    private void runWithPool(PoolRunnable body) throws Exception {
        assertMemoryLeak(() -> {
            try (WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)))) {
                TestUtils.execute(pool, (db, _, sqlExecutionContext) ->
                        body.run(db, sqlExecutionContext), configuration, LOG);
            }
        });
    }

    @FunctionalInterface
    private interface PoolRunnable {
        void run(CairoEngine db, SqlExecutionContext ctx) throws Exception;
    }
}
