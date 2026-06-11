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
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

// Locks in the fix for https://github.com/questdb/questdb/issues/7160 — corr() returning NULL
// under parallel GROUP BY when many worker partials see only NULL (y, x) pairs.
public class CorrParallelGroupByTest extends AbstractCairoTest {

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

    @Test
    public void testParallelAllNull() throws Exception {
        runWithPool((compiler, ctx) -> {
            execute(
                    compiler,
                    "CREATE TABLE tbl AS (" +
                            "    SELECT" +
                            "        cast(null AS double) x," +
                            "        cast(null AS double) y" +
                            "    FROM long_sequence(4_000)" +
                            ")",
                    ctx
            );
            assertQuery("SELECT corr(y, x) FROM tbl")
                    .noLeakCheck()
                    .withCompiler(compiler)
                    .withContext(ctx)
                    .noRandomAccess()
                    .expectSize()
                    .returns("corr\nnull\n");
        });
    }

    @Test
    public void testParallelKeyedSparseNulls() throws Exception {
        runWithPool((compiler, ctx) -> {
            execute(
                    compiler,
                    "CREATE TABLE tbl AS (" +
                            "    SELECT" +
                            "        x % 4 AS grp," +
                            "        CASE WHEN x BETWEEN 1500 AND 1509 THEN cast(x AS double) ELSE cast(null AS double) END x_val," +
                            "        CASE WHEN x BETWEEN 1500 AND 1509 THEN cast(2 * x + 5 AS double) ELSE cast(null AS double) END y_val" +
                            "    FROM long_sequence(4_000)" +
                            ")",
                    ctx
            );
            assertQuery("SELECT grp, corr(y_val, x_val) FROM tbl ORDER BY grp")
                    .noLeakCheck()
                    .withCompiler(compiler)
                    .withContext(ctx)
                    .expectSize()
                    .returns("""
                            grp\tcorr
                            0\t1.0
                            1\t1.0
                            2\t1.0
                            3\t1.0
                            """);
        });
    }

    @Test
    public void testParallelSparseNulls() throws Exception {
        runWithPool((compiler, ctx) -> {
            execute(
                    compiler,
                    "CREATE TABLE tbl AS (" +
                            "    SELECT" +
                            "        CASE WHEN x BETWEEN 1500 AND 1509 THEN cast(x AS double) ELSE cast(null AS double) END x," +
                            "        CASE WHEN x BETWEEN 1500 AND 1509 THEN cast(2 * x + 5 AS double) ELSE cast(null AS double) END y" +
                            "    FROM long_sequence(4_000)" +
                            ")",
                    ctx
            );
            assertQuery("SELECT corr(y, x) FROM tbl")
                    .noLeakCheck()
                    .withCompiler(compiler)
                    .withContext(ctx)
                    .noRandomAccess()
                    .expectSize()
                    .returns("corr\n1.0\n");
        });
    }

    private void runWithPool(PoolRunnable body) throws Exception {
        assertMemoryLeak(() -> {
            try (WorkerPool pool = new WorkerPool(() -> 4)) {
                TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) ->
                        body.run(compiler, sqlExecutionContext), configuration, LOG);
            }
        });
    }

    @FunctionalInterface
    private interface PoolRunnable {
        void run(SqlCompiler compiler, SqlExecutionContext ctx) throws Exception;
    }
}
