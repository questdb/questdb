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

package io.questdb.test.griffin.engine.functions.cast;

import io.questdb.PropertyKey;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

// A CHAR -> DECIMAL cast reads its argument through Function.getStrA(), which CharColumn serves
// from a per-instance sink, so workers must get their own copy of the key or filter function.
public class CastCharToDecimalParallelGroupByTest extends AbstractCairoTest {

    private static final String CREATE_TABLE = "CREATE TABLE tbl AS (SELECT (48 + (x % 10))::char c FROM long_sequence(4_000))";
    private static final String EXPECTED_GROUPS = """
            k\tcount
            0.00\t400
            1.00\t400
            2.00\t400
            3.00\t400
            4.00\t400
            5.00\t400
            6.00\t400
            7.00\t400
            8.00\t400
            9.00\t400
            """;

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
    public void testParallelFilter() throws Exception {
        runWithPool((compiler, ctx) -> {
            execute(compiler, CREATE_TABLE, ctx);
            assertQuery("SELECT count() FROM tbl WHERE c::DECIMAL(30,2) = '7.00'::DECIMAL(30,2)")
                    .noLeakCheck()
                    .withCompiler(compiler)
                    .withContext(ctx)
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n400\n");
        });
    }

    @Test
    public void testParallelGroupByDecimal128Key() throws Exception {
        runWithPool((compiler, ctx) -> {
            execute(compiler, CREATE_TABLE, ctx);
            assertQuery("SELECT c::DECIMAL(30,2) k, count() FROM tbl ORDER BY k")
                    .noLeakCheck()
                    .withCompiler(compiler)
                    .withContext(ctx)
                    .expectSize()
                    .returns(EXPECTED_GROUPS);
        });
    }

    @Test
    public void testParallelGroupByDecimal256Key() throws Exception {
        runWithPool((compiler, ctx) -> {
            execute(compiler, CREATE_TABLE, ctx);
            assertQuery("SELECT c::DECIMAL(40,2) k, count() FROM tbl ORDER BY k")
                    .noLeakCheck()
                    .withCompiler(compiler)
                    .withContext(ctx)
                    .expectSize()
                    .returns(EXPECTED_GROUPS);
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
