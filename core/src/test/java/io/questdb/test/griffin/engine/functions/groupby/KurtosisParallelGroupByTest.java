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

public class KurtosisParallelGroupByTest extends AbstractCairoTest {

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
    public void testParallelKurtosisPopSparseNulls() throws Exception {
        runWithPool((compiler, ctx) -> {
            createSparseTable(compiler, ctx);
            // arithmetic sequence {1500..1509}, n=10: g2_pop = 10*M4/M2^2 - 3 = -202/165
            assertQuery("SELECT kurtosis_pop(x) FROM tbl")
                    .noLeakCheck()
                    .withCompiler(compiler)
                    .withContext(ctx)
                    .noRandomAccess()
                    .expectSize()
                    .returns("kurtosis_pop\n-1.2242424242424241\n");
        });
    }

    @Test
    public void testParallelKurtosisSampSparseNulls() throws Exception {
        runWithPool((compiler, ctx) -> {
            createSparseTable(compiler, ctx);
            // arithmetic sequence of any length >= 4 has Fisher-Pearson sample kurtosis exactly -6/5
            assertQuery("SELECT kurtosis_samp(x) FROM tbl")
                    .noLeakCheck()
                    .withCompiler(compiler)
                    .withContext(ctx)
                    .noRandomAccess()
                    .expectSize()
                    .returns("kurtosis_samp\n-1.1999999999999997\n");
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
