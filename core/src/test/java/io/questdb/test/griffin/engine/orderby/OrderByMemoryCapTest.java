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
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.mp.WorkerPool;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class OrderByMemoryCapTest extends AbstractCairoTest {

    @Test
    public void testParallelTopKEncodedPerWorkerCapFires() throws Exception {
        // Each AsyncTopK worker builds its own EncodedTopKBuffer carrying the full sort.key
        // budget, so a 64-byte cap fires per worker. Confirms AsyncTopKAtom threads the (raise ...)
        // hint correctly, not just the single-threaded path.
        node1.setProperty(PropertyKey.CAIRO_SQL_PARALLEL_TOP_K_ENABLED, true);
        node1.setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1000);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_KEY_PAGE_SIZE, 64);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_KEY_MAX_BYTES, 64);

        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(pool, (engine, _, sqlExecutionContext) -> {
                engine.execute(
                        "CREATE TABLE tab AS (" +
                                "SELECT (x * 1_000_000L)::TIMESTAMP AS ts, (x % 1_000)::INT AS g, x::INT AS v" +
                                " FROM long_sequence(100_000)) TIMESTAMP(ts) PARTITION BY DAY",
                        sqlExecutionContext
                );

                // Fixed-width INT keys take the encoded per-worker buffers rather than the tree chain.
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
                            "memory exceeded in EncodedSort (raise cairo.sql.sort.key.max.bytes or cairo.sql.sort.light.value.max.bytes)");
                }
            }, configuration, LOG);
        });
    }

    @Test
    public void testParallelTopKTreeChainPerWorkerCapFires() throws Exception {
        // A VARCHAR sort key is not encodable, so AsyncTopK falls back to the per-worker
        // LimitedSizeLongTreeChain; a 64-byte sort.key cap fires per worker with the
        // RedBlackTree (raise ...) hint.
        node1.setProperty(PropertyKey.CAIRO_SQL_PARALLEL_TOP_K_ENABLED, true);
        node1.setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1000);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_KEY_PAGE_SIZE, 64);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_KEY_MAX_BYTES, 64);

        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(pool, (engine, _, sqlExecutionContext) -> {
                engine.execute(
                        "CREATE TABLE tab AS (" +
                                "SELECT (x * 1_000_000L)::TIMESTAMP AS ts, (x % 1_000)::VARCHAR AS s, x::INT AS v" +
                                " FROM long_sequence(100_000)) TIMESTAMP(ts) PARTITION BY DAY",
                        sqlExecutionContext
                );

                final String query = "SELECT ts, s, v FROM tab ORDER BY s, v LIMIT 40000";

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
    public void testSerialEncodedOrderByLimitCapFires() throws Exception {
        // The encoded top-K honors the key cap on its own: a 64-byte budget fits only
        // a few entries, so the scan overflows even with the value cap unset.
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_KEY_MAX_BYTES, 64);

        assertMemoryLeak(() -> {
            // The shared context snapshots the parallel top-K flag at construction,
            // so it has to be flipped on the context itself.
            final SqlExecutionContextImpl context = (SqlExecutionContextImpl) sqlExecutionContext;
            final boolean wasParallelTopKEnabled = context.isParallelTopKEnabled();
            context.setParallelTopKEnabled(false);
            try {
                execute("CREATE TABLE tab AS (" +
                        "SELECT (x % 1_000)::INT AS g, x::INT AS v" +
                        " FROM long_sequence(50_000))");

                assertExceptionNoLeakCheck(
                        "SELECT g, v FROM tab ORDER BY g, v LIMIT 40000",
                        0,
                        "memory exceeded in EncodedSort"
                );
            } finally {
                context.setParallelTopKEnabled(wasParallelTopKEnabled);
            }
        });
    }

    @Test
    public void testSerialEncodedOrderByLimitValueCapFires() throws Exception {
        // The value cap must bind on its own: a 64-byte budget fits eight rowId
        // words, so the scan overflows even with the key cap unset.
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_LIGHT_VALUE_MAX_BYTES, 64);

        assertMemoryLeak(() -> {
            // The shared context snapshots the parallel top-K flag at construction,
            // so it has to be flipped on the context itself.
            final SqlExecutionContextImpl context = (SqlExecutionContextImpl) sqlExecutionContext;
            final boolean wasParallelTopKEnabled = context.isParallelTopKEnabled();
            context.setParallelTopKEnabled(false);
            try {
                execute("CREATE TABLE tab AS (" +
                        "SELECT (x % 1_000)::INT AS g, x::INT AS v" +
                        " FROM long_sequence(50_000))");

                assertExceptionNoLeakCheck(
                        "SELECT g, v FROM tab ORDER BY g, v LIMIT 40000",
                        0,
                        "memory exceeded in EncodedSort"
                );
            } finally {
                context.setParallelTopKEnabled(wasParallelTopKEnabled);
            }
        });
    }

    @Test
    public void testSerialOrderByLimitCapFires() throws Exception {
        // With parallel top-K and the encoded sort disabled, ORDER BY ... LIMIT routes
        // through the serial LimitedSizeSortedLightRecordCursorFactory. A 64-byte
        // sort.key budget makes its LimitedSizeLongTreeChain key heap overflow, naming
        // the sort.key config key.
        node1.setProperty(PropertyKey.CAIRO_SQL_ORDER_BY_SORT_ENABLED, false);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_KEY_PAGE_SIZE, 64);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_KEY_MAX_BYTES, 64);

        assertMemoryLeak(() -> {
            // The shared context snapshots the parallel top-K flag at construction,
            // so it has to be flipped on the context itself.
            final SqlExecutionContextImpl context = (SqlExecutionContextImpl) sqlExecutionContext;
            final boolean wasParallelTopKEnabled = context.isParallelTopKEnabled();
            context.setParallelTopKEnabled(false);
            try {
                execute("CREATE TABLE tab AS (" +
                        "SELECT (x % 1_000)::INT AS g, x::INT AS v" +
                        " FROM long_sequence(50_000))");

                assertExceptionNoLeakCheck(
                        "SELECT g, v FROM tab ORDER BY g, v LIMIT 40000",
                        0,
                        "memory exceeded in RedBlackTree (raise cairo.sql.sort.key.max.bytes)"
                );
            } finally {
                context.setParallelTopKEnabled(wasParallelTopKEnabled);
            }
        });
    }
}
