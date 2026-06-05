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
import org.junit.Before;
import org.junit.Test;

/**
 * SQL-level tests that exercise the per-query memory limit through the
 * tracker-aware GROUP BY function-state allocator:
 * {@link io.questdb.griffin.engine.groupby.FastGroupByAllocator} and the
 * {@link io.questdb.std.DirectLongLongHashMap} chunk index it owns.
 * <p>
 * The synchronous GROUP BY factories
 * ({@link io.questdb.griffin.engine.groupby.GroupByRecordCursorFactory} and
 * {@link io.questdb.griffin.engine.groupby.GroupByNotKeyedRecordCursorFactory})
 * construct their allocator in lazy mode and bind the active workload's
 * MemoryTracker before the first cursor open. GROUP BY functions that hold
 * per-group state allocate it through the bound allocator, so a runaway
 * aggregate crosses the per-query limit at the offending allocation site.
 * <p>
 * {@code count_distinct} is the vehicle here because its set state
 * ({@link io.questdb.griffin.engine.groupby.GroupByLongHashSet}) is allocated
 * through the GROUP BY allocator. The non-keyed variant is the cleanest proof:
 * it carries no tracker-aware map (the single aggregate value lives in a
 * {@code SimpleMapValue}), so the only allocations charged to the per-query
 * counter come from the allocator.
 * <p>
 * The per-query limit and the allocator's default chunk size are applied per
 * test in {@link #setUp()} via {@code setProperty}, re-applied because
 * {@code AbstractCairoTest.tearDown} clears property overrides between tests;
 * the provider reads the limit live on each tracker acquisition. The shrunk
 * chunk size keeps small workloads comfortably under the limit so a runaway
 * aggregate breaches only after a few set doublings.
 * <p>
 * Parallel GROUP BY is disabled per test via
 * {@code sqlExecutionContext.setParallelGroupByEnabled(false)} to pin the breach
 * to a single deterministic allocation site. The parallel path
 * ({@code AsyncGroupByAtom} / {@code GroupByMapFragment}) is tracker-aware too,
 * but a breach inside its shard-merge job surfaces as a cancellation rather than
 * the per-query OOM.
 */
public class GroupByAllocatorMemoryTrackerTest extends AbstractCairoTest {

    @Before
    public void setUp() {
        super.setUp();
        // 512 KiB: large enough for the small initial set allocations to fit
        // comfortably, small enough that a runaway count_distinct breaches
        // after a few set doublings.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 512 * 1024L);
        // Shrink the GROUP BY allocator's default chunk size so the initial
        // per-group set allocations sit well under the 512 KiB per-query limit
        // and the breach tests can grow the set by doubling. The default
        // (128 KiB) would round small initial allocations up to a coarse chunk.
        setProperty(PropertyKey.CAIRO_SQL_GROUPBY_ALLOCATOR_DEFAULT_CHUNK_SIZE, 4 * 1024L);
    }

    @Test
    public void testKeyedCountDistinctFailsOnLargeSets() throws Exception {
        // Sync keyed GROUP BY with few keys but many distinct values per key.
        // The per-key count_distinct sets are allocated through the GROUP BY
        // allocator; their combined growth trips the per-query limit. The data
        // map carries only two keys, so the breach is attributable to the
        // allocator rather than the (tracker-aware, but tiny) map.
        assertMemoryLeak(() -> {
            sqlExecutionContext.setParallelGroupByEnabled(false);
            execute("CREATE TABLE tab AS (SELECT x % 2 AS k, x AS v FROM long_sequence(100_000))");
            drainWalQueue();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final CompiledQuery cq = compiler.compile("SELECT k, count_distinct(v) FROM tab GROUP BY k", sqlExecutionContext);
                try (RecordCursorFactory factory = cq.getRecordCursorFactory();
                     RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    //noinspection StatementWithEmptyBody
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
    public void testKeyedCountDistinctSucceedsOnSmallSets() throws Exception {
        // Sync keyed GROUP BY on a small distinct set fits the per-query limit;
        // the allocator accounting holds malloc/free in balance and the query
        // returns the expected aggregates.
        assertMemoryLeak(() -> {
            sqlExecutionContext.setParallelGroupByEnabled(false);
            execute("CREATE TABLE tab AS (SELECT x % 2 AS k, x % 10 AS v FROM long_sequence(100))");
            drainWalQueue();
            assertQuery("SELECT k, count_distinct(v) cnt FROM tab GROUP BY k ORDER BY k")
                    .noLeakCheck()
                    .expectSize()
                    .returns("k\tcnt\n" +
                            "0\t5\n" +
                            "1\t5\n");
        });
    }

    @Test
    public void testNotKeyedCountDistinctFailsOnLargeSet() throws Exception {
        // Sync non-keyed GROUP BY: count_distinct over many distinct values
        // grows a single GroupByLongHashSet through the allocator. There is no
        // tracker-aware map on this path (the aggregate lives in a
        // SimpleMapValue), so the breach is charged solely to the allocator -
        // the cleanest demonstration of the non-keyed allocator wiring.
        assertMemoryLeak(() -> {
            sqlExecutionContext.setParallelGroupByEnabled(false);
            execute("CREATE TABLE tab AS (SELECT x AS v FROM long_sequence(50_000))");
            drainWalQueue();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final CompiledQuery cq = compiler.compile("SELECT count_distinct(v) FROM tab", sqlExecutionContext);
                try (RecordCursorFactory factory = cq.getRecordCursorFactory();
                     RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    //noinspection StatementWithEmptyBody
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
    public void testNotKeyedCountDistinctOpenFailureReleasesAllocations() throws Exception {
        // getCursor() must null cursor.baseCursor on a breach so a reused open does not read a freed
        // cursor. A tiny limit breaches the first open at allocator.reopen(); raising it must recover.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 64L);
        assertMemoryLeak(() -> {
            sqlExecutionContext.setParallelGroupByEnabled(false);
            execute("CREATE TABLE tab AS (SELECT x % 1_000 AS v FROM long_sequence(10_000))");
            drainWalQueue();
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile("SELECT count_distinct(v) FROM tab", sqlExecutionContext).getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.fail("expected a per-query memory breach during cursor open");
                } catch (CairoException e) {
                    Assert.assertTrue("expected isOutOfMemory(), got: " + e.getFlyweightMessage(), e.isOutOfMemory());
                    TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                }
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0L);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(1_000, cursor.getRecord().getLong(0));
                }
            }
        });
    }

    @Test
    public void testNotKeyedCountDistinctSucceedsOnSmallSet() throws Exception {
        // Sync non-keyed GROUP BY on a small distinct set fits the per-query
        // limit and returns the expected count.
        assertMemoryLeak(() -> {
            sqlExecutionContext.setParallelGroupByEnabled(false);
            execute("CREATE TABLE tab AS (SELECT x % 5 AS v FROM long_sequence(100))");
            drainWalQueue();
            assertQuery("SELECT count_distinct(v) cnt FROM tab")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("cnt\n5\n");
        });
    }

    @Test
    public void testRepeatedCursorRunsReleaseAllocations() throws Exception {
        // Repeat the same non-keyed count_distinct many times to verify that
        // close/reopen cycles through the allocator release every byte they
        // allocated. assertMemoryLeak around the loop is the load-bearing
        // check; a malloc/free asymmetry would manifest as a residual native
        // allocation count at the end of the test.
        assertMemoryLeak(() -> {
            sqlExecutionContext.setParallelGroupByEnabled(false);
            execute("CREATE TABLE tab AS (SELECT x % 1_000 AS v FROM long_sequence(10_000))");
            drainWalQueue();
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile("SELECT count_distinct(v) FROM tab", sqlExecutionContext).getRecordCursorFactory()) {
                for (int i = 0; i < 20; i++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        long rows = 0;
                        while (cursor.hasNext()) {
                            rows++;
                        }
                        Assert.assertEquals(1, rows);
                    }
                }
            }
        });
    }
}
