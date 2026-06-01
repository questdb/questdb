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

package io.questdb.test.griffin.engine.orderby;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.mp.WorkerPool;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class OrderByMemoryCapTest extends AbstractCairoTest {

    @Test
    public void testParallelTopKPerWorkerCapFires() throws Exception {
        // Each AsyncTopK worker builds its own LimitedSizeLongTreeChain carrying the full sort.key
        // budget, so a 64-byte cap fires per worker. Confirms AsyncTopKAtom threads the (raise ...)
        // hint correctly, not just the single-threaded path.
        node1.setProperty(PropertyKey.CAIRO_SQL_PARALLEL_TOP_K_ENABLED, true);
        node1.setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1000);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_KEY_PAGE_SIZE, 64);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_KEY_MAX_BYTES, 64);

        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                engine.execute(
                        "CREATE TABLE tab AS (" +
                                "SELECT (x * 1_000_000L)::TIMESTAMP AS ts, (x % 1_000)::INT AS g, x::INT AS v" +
                                " FROM long_sequence(100_000)) TIMESTAMP(ts) PARTITION BY DAY",
                        sqlExecutionContext
                );

                // Multi-column ORDER BY routes through the tree chain rather than the long top-K path.
                final String query = "SELECT ts, g, v FROM tab ORDER BY g, v LIMIT 40000";

                // Guard against a silent serial fallback: the plan must be the parallel top-K factory.
                final StringSink sink = new StringSink();
                TestUtils.printSql(engine, sqlExecutionContext, "EXPLAIN " + query, sink);
                TestUtils.assertContains(sink, "Async");
                TestUtils.assertContains(sink, "Top K");

                try {
                    TestUtils.printSql(engine, sqlExecutionContext, query, sink);
                    Assert.fail("expected LimitOverflowException from constrained sort.key budget");
                } catch (CairoException ex) {
                    TestUtils.assertContains(ex.getFlyweightMessage(),
                            "memory exceeded in RedBlackTree (raise cairo.sql.sort.key.max.bytes)");
                }
            }, configuration, LOG);
        });
    }

    @Test
    public void testSerialOrderByLimitCapFires() throws Exception {
        // Parallel top-K disabled routes ORDER BY ... LIMIT through the serial
        // LimitedSizeSortedLightRecordCursorFactory. A 64-byte sort.key budget makes its
        // LimitedSizeLongTreeChain key heap overflow, naming the sort.key config key.
        node1.setProperty(PropertyKey.CAIRO_SQL_PARALLEL_TOP_K_ENABLED, false);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_KEY_PAGE_SIZE, 64);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_KEY_MAX_BYTES, 64);

        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (" +
                    "SELECT (x % 1_000)::INT AS g, x::INT AS v" +
                    " FROM long_sequence(50_000))");

            assertExceptionNoLeakCheck(
                    "SELECT g, v FROM tab ORDER BY g, v LIMIT 40000",
                    0,
                    "memory exceeded in RedBlackTree (raise cairo.sql.sort.key.max.bytes)"
            );
        });
    }
}
