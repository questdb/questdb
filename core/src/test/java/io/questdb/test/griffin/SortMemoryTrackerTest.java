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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * SQL-level tests that exercise the per-query memory limit through the
 * tracker-aware sort buffer family wired by PR 2.3
 * ({@link io.questdb.griffin.engine.AbstractRedBlackTree},
 * {@link io.questdb.griffin.engine.orderby.LongTreeChain},
 * {@link io.questdb.griffin.engine.orderby.LimitedSizeLongTreeChain}).
 * <p>
 * {@link io.questdb.griffin.engine.orderby.SortedLightRecordCursorFactory},
 * {@link io.questdb.griffin.engine.orderby.LimitedSizeSortedLightRecordCursorFactory},
 * {@link io.questdb.griffin.engine.window.CachedWindowRecordCursorFactory},
 * and {@link io.questdb.griffin.engine.table.AsyncTopKAtom} construct their
 * tree chains with deferred allocation (lazy mode) and bind the active
 * workload's MemoryTracker before the first cursor open. The malloc/free
 * pairs that follow are charged symmetrically to the per-query counter;
 * a runaway sort crosses the limit and fails at the offending allocation
 * site.
 * <p>
 * The per-query limit is set in {@link #beforeClass()} because
 * {@code CairoEngine#getMemoryTrackerProvider} caches the
 * {@code PerQueryMemoryTrackerProvider} on first access with the limit then in
 * effect. Per-test overrides via {@code node1.setProperty} land too late on a
 * shared engine. Tests that should breach the limit use large workloads;
 * tests that should succeed use small ones.
 */
public class SortMemoryTrackerTest extends AbstractCairoTest {

    @BeforeClass
    public static void beforeClass() {
        // 512 KiB: large enough for the 128 KiB key + 128 KiB value initial
        // sort allocations on small inputs, small enough that a runaway sort
        // breaches after one or two heap doublings.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 512 * 1024L);
    }

    @Test
    public void testCachedWindowFailsOnLargePartition() throws Exception {
        // Window functions over an unordered base feed a CachedWindowRecordCursor
        // that builds one tree per ordered group. The tree key/value heaps grow
        // with the partition; a large partition breaches the per-query limit.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (SELECT x AS k FROM long_sequence(50_000))");
            drainWalQueue();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final CompiledQuery cq = compiler.compile(
                        "SELECT k, row_number() OVER (ORDER BY k) AS rn FROM tab",
                        sqlExecutionContext
                );
                try (RecordCursorFactory factory = cq.getRecordCursorFactory();
                     RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    while (cursor.hasNext()) {
                        // drain until breach
                    }
                    Assert.fail("expected per-query memory breach");
                } catch (CairoException e) {
                    Assert.assertTrue("expected isOutOfMemory(), got: " + e.getFlyweightMessage(), e.isOutOfMemory());
                    TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                    TestUtils.assertContains(e.getFlyweightMessage(), "workload=QUERY");
                }
            }
        });
    }

    @Test
    public void testCachedWindowSucceedsOnSmallPartition() throws Exception {
        // Same window function on a small partition fits the limit and returns
        // the expected row numbers.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (SELECT x AS k FROM long_sequence(5))");
            drainWalQueue();
            assertSql(
                    "k\trn\n" +
                            "1\t1\n" +
                            "2\t2\n" +
                            "3\t3\n" +
                            "4\t4\n" +
                            "5\t5\n",
                    "SELECT k, row_number() OVER (ORDER BY k) AS rn FROM tab ORDER BY k"
            );
        });
    }

    @Test
    public void testOrderByFailsOnLargeInput() throws Exception {
        // ORDER BY on a non-encodable column (varchar) routes through
        // SortedLightRecordCursorFactory which builds a LongTreeChain over the
        // full result set. The chain's key and value heaps grow with the input;
        // a large input breaches the per-query limit.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (SELECT cast(x AS varchar) k FROM long_sequence(50_000))");
            drainWalQueue();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final CompiledQuery cq = compiler.compile(
                        "SELECT * FROM tab ORDER BY k DESC",
                        sqlExecutionContext
                );
                try (RecordCursorFactory factory = cq.getRecordCursorFactory();
                     RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    while (cursor.hasNext()) {
                        // drain until breach
                    }
                    Assert.fail("expected per-query memory breach");
                } catch (CairoException e) {
                    Assert.assertTrue("expected isOutOfMemory(), got: " + e.getFlyweightMessage(), e.isOutOfMemory());
                    TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                    TestUtils.assertContains(e.getFlyweightMessage(), "workload=QUERY");
                }
            }
        });
    }

    @Test
    public void testOrderByLimitFailsOnLargeTopN() throws Exception {
        // ORDER BY ... LIMIT N on a non-encodable column with a large positive
        // N routes through LimitedSizeSortedLightRecordCursorFactory which
        // builds a LimitedSizeLongTreeChain holding up to N records. With N
        // close to the input size the chain's key/value heaps grow past the
        // per-query limit and breach at the offending realloc.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (SELECT cast(x AS varchar) k FROM long_sequence(50_000))");
            drainWalQueue();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final CompiledQuery cq = compiler.compile(
                        "SELECT * FROM tab ORDER BY k DESC LIMIT 50000",
                        sqlExecutionContext
                );
                try (RecordCursorFactory factory = cq.getRecordCursorFactory();
                     RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    while (cursor.hasNext()) {
                        // drain until breach
                    }
                    Assert.fail("expected per-query memory breach");
                } catch (CairoException e) {
                    Assert.assertTrue("expected isOutOfMemory(), got: " + e.getFlyweightMessage(), e.isOutOfMemory());
                    TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                    TestUtils.assertContains(e.getFlyweightMessage(), "workload=QUERY");
                }
            }
        });
    }

    @Test
    public void testOrderByLimitSucceedsOnSmallInput() throws Exception {
        // ORDER BY ... LIMIT N on a non-encodable column with a small input
        // goes through LimitedSizeSortedLightRecordCursorFactory; the chain
        // stays well below the limit and the query returns the expected top
        // rows.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (SELECT cast(x AS varchar) k FROM long_sequence(20))");
            drainWalQueue();
            assertSql(
                    "k\n" +
                            "9\n" +
                            "8\n" +
                            "7\n" +
                            "6\n" +
                            "5\n",
                    "SELECT * FROM tab ORDER BY k DESC LIMIT 5"
            );
        });
    }

    @Test
    public void testOrderBySucceedsOnSmallInput() throws Exception {
        // ORDER BY without LIMIT on a non-encodable column with a small input
        // goes through SortedLightRecordCursorFactory; the LongTreeChain's
        // initial heaps cover the full dataset with no growth and the query
        // returns the expected sorted rows.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (SELECT cast(x AS varchar) k FROM long_sequence(5))");
            drainWalQueue();
            assertSql(
                    "k\n" +
                            "5\n" +
                            "4\n" +
                            "3\n" +
                            "2\n" +
                            "1\n",
                    "SELECT * FROM tab ORDER BY k DESC"
            );
        });
    }

    @Test
    public void testRepeatedCursorRunsReleaseAllocations() throws Exception {
        // Repeat the same ORDER BY many times to verify that close/reopen cycles
        // through the tree chain release every byte they allocated. The load-bearing
        // check is assertMemoryLeak around the loop: a malloc/free asymmetry would
        // manifest as a residual native allocation count at the end of the test.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (SELECT cast(x AS varchar) k FROM long_sequence(50))");
            drainWalQueue();
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile("SELECT * FROM tab ORDER BY k DESC", sqlExecutionContext).getRecordCursorFactory()) {
                for (int i = 0; i < 20; i++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        long rows = 0;
                        while (cursor.hasNext()) {
                            rows++;
                        }
                        Assert.assertEquals(50, rows);
                    }
                }
            }
        });
    }
}
