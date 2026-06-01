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

// Exercises the parallel-merge path for the Welford-based variance/stddev family
// (var_pop, var_samp, stddev_pop, stddev_samp), all sharing AbstractStdDevGroupByFunction.merge().
// Same empty-partial NaN-poisoning bug as https://github.com/questdb/questdb/issues/7160 for corr().
public class StdDevParallelGroupByTest extends AbstractCairoTest {

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
    public void testParallelKeyedSparseNulls() throws Exception {
        runWithPool((compiler, ctx) -> {
            execute(
                    compiler,
                    "CREATE TABLE tbl AS (" +
                            "    SELECT" +
                            "        x % 4 AS grp," +
                            "        CASE WHEN x BETWEEN 1500 AND 1509 THEN cast(x AS double) ELSE cast(null AS double) END val" +
                            "    FROM long_sequence(4_000)" +
                            ")",
                    ctx
            );
            assertQuery("SELECT grp, var_pop(val), var_samp(val), stddev_pop(val), stddev_samp(val) FROM tbl ORDER BY grp")
                    .noLeakCheck()
                    .withCompiler(compiler)
                    .withContext(ctx)
                    .expectSize()
                    .returns("""
                            grp\tvar_pop\tvar_samp\tstddev_pop\tstddev_samp
                            0\t10.666666666666666\t16.0\t3.265986323710904\t4.0
                            1\t10.666666666666666\t16.0\t3.265986323710904\t4.0
                            2\t4.0\t8.0\t2.0\t2.8284271247461903
                            3\t4.0\t8.0\t2.0\t2.8284271247461903
                            """);
        });
    }

    @Test
    public void testParallelStdDevPopSparseNulls() throws Exception {
        runWithPool((compiler, ctx) -> {
            createSparseTable(compiler, ctx);
            assertQuery("SELECT stddev_pop(x) FROM tbl")
                    .noLeakCheck()
                    .withCompiler(compiler)
                    .withContext(ctx)
                    .noRandomAccess()
                    .expectSize()
                    .returns("stddev_pop\n2.8722813232690143\n");
        });
    }

    @Test
    public void testParallelStdDevSampSparseNulls() throws Exception {
        runWithPool((compiler, ctx) -> {
            createSparseTable(compiler, ctx);
            assertQuery("SELECT stddev_samp(x) FROM tbl")
                    .noLeakCheck()
                    .withCompiler(compiler)
                    .withContext(ctx)
                    .noRandomAccess()
                    .expectSize()
                    .returns("stddev_samp\n3.0276503540974917\n");
        });
    }

    @Test
    public void testParallelVarPopSparseNulls() throws Exception {
        runWithPool((compiler, ctx) -> {
            createSparseTable(compiler, ctx);
            assertQuery("SELECT var_pop(x) FROM tbl")
                    .noLeakCheck()
                    .withCompiler(compiler)
                    .withContext(ctx)
                    .noRandomAccess()
                    .expectSize()
                    .returns("var_pop\n8.25\n");
        });
    }

    @Test
    public void testParallelVarSampSparseNulls() throws Exception {
        runWithPool((compiler, ctx) -> {
            createSparseTable(compiler, ctx);
            assertQuery("SELECT var_samp(x) FROM tbl")
                    .noLeakCheck()
                    .withCompiler(compiler)
                    .withContext(ctx)
                    .noRandomAccess()
                    .expectSize()
                    .returns("var_samp\n9.166666666666666\n");
        });
    }

    private void createSparseTable(SqlCompiler compiler, SqlExecutionContext ctx) throws Exception {
        execute(
                compiler,
                "CREATE TABLE tbl AS (" +
                        "    SELECT" +
                        "        CASE WHEN x BETWEEN 1500 AND 1509 THEN cast(x AS double) ELSE cast(null AS double) END x" +
                        "    FROM long_sequence(4_000)" +
                        ")",
                ctx
        );
    }

    private void runWithPool(PoolRunnable body) throws Exception {
        assertMemoryLeak(() -> {
            try (WorkerPool pool = new WorkerPool(() -> 4)) {
                TestUtils.execute(pool, (_, compiler, sqlExecutionContext) ->
                        body.run(compiler, sqlExecutionContext), configuration, LOG);
            }
        });
    }

    @FunctionalInterface
    private interface PoolRunnable {
        void run(SqlCompiler compiler, SqlExecutionContext ctx) throws Exception;
    }
}
