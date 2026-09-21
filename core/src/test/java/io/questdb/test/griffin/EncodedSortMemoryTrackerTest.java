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
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.engine.orderby.EncodedSortLightRecordCursorFactory;
import io.questdb.griffin.engine.orderby.EncodedSortRecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * SQL-level tests that exercise the per-query memory limit through the
 * tracker-aware encoded-sort cursors in {@link io.questdb.griffin.engine.orderby}.
 * A full ORDER BY on an encodable (fixed-width) sort key over a random-access
 * base routes to {@link EncodedSortLightRecordCursorFactory}, whose entry buffer
 * ({@code DirectLongList}) holds one (key, rowId) entry per input row; over a
 * non-random-access base (e.g. UNION ALL) it routes to
 * {@link EncodedSortRecordCursorFactory}, which additionally materializes every
 * record into a {@code RecordChain}. Both buffers grow one entry per row and,
 * with the production default of {@code cairo.sql.sort.*.max.pages} =
 * Integer.MAX_VALUE, are bounded only by the heap, so a runaway sort is a
 * genuine per-query memory vector. The cursors bind the active workload's
 * MemoryTracker on the entry buffer (and record chain) in of() before the first
 * allocation, so malloc/free stay symmetric and a runaway sort breaches the
 * limit at the offending allocation.
 * <p>
 * The per-query limit and the record-chain page size are applied per test in
 * {@link #setUpPageSizes()} (re-applied because tearDown clears property
 * overrides) so the non-light path's materialized pages fit under the limit;
 * the provider reads the limit live on each tracker acquisition.
 */
public class EncodedSortMemoryTrackerTest extends AbstractCairoTest {

    @Before
    public void setUpPageSizes() {
        // The entry buffer reopens to a fixed 128 KiB DirectLongList, so the
        // limit must clear that for the small-input / leak-loop cases while a
        // 100k-row sort that grows the buffer past it breaches. Re-applied per
        // test because tearDown clears property overrides; read live by the
        // provider on each acquisition.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 256 * 1024L);
        // Shrink the record-chain page size so the non-light EncodedSort's
        // materialized chain fits a couple of pages under the per-query limit.
        // maxEntryMemBytes keys off sort.key.page.size, not the value page size,
        // so this does not lower the LimitOverflow ceiling.
        setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 16 * 1024L);
    }

    @Test
    public void testEncodedSortFailsOnLargeInput() throws Exception {
        // A UNION ALL base does not support random access, so the sort
        // materializes records into a RecordChain alongside the entry buffer.
        // UNION ALL itself allocates nothing tracker-charged, so the breach is
        // attributable to the encoded-sort buffers.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a AS (SELECT x AS k FROM long_sequence(100_000))");
            execute("CREATE TABLE b AS (SELECT x AS k FROM long_sequence(100_000))");
            drainWalQueue();
            final String sql = "SELECT k FROM (SELECT k FROM a UNION ALL SELECT k FROM b) ORDER BY k";
            assertUsesFactory(sql, EncodedSortRecordCursorFactory.class);
            assertQueryBreaches(sql);
        });
    }

    @Test
    public void testEncodedSortLightBuildBreachDoesNotLeakReader() throws Exception {
        // EncodedSort sizes its (key, rowId) entry buffer in the build pass on the first
        // hasNext() (entryMem.setCapacity to the row count), not in of(), so the breach
        // lands mid-build with the base cursor held. close() after a breaching build must
        // free that base cursor; tests keep leaked readers, so a leak shows as a busy reader.
        assertMemoryLeak(() -> {
            // 100k rows size the entry buffer well past the 256 KiB limit, so the build
            // breaches at setCapacity on the first hasNext().
            execute("CREATE TABLE tab AS (SELECT x AS k FROM long_sequence(100_000))");
            drainWalQueue();
            final String sql = "SELECT * FROM tab ORDER BY k";
            assertUsesFactory(sql, EncodedSortLightRecordCursorFactory.class);
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                for (int i = 0; i < 5; i++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        //noinspection StatementWithEmptyBody
                        while (cursor.hasNext()) {
                            // drain until the build-pass breach
                        }
                        Assert.fail("expected per-query memory breach during build at iteration " + i);
                    } catch (CairoException e) {
                        Assert.assertTrue("expected isOutOfMemory(), got: " + e.getFlyweightMessage(), e.isOutOfMemory());
                        TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                        TestUtils.assertContains(e.getFlyweightMessage(), "workload=QUERY");
                    }
                }
            }
            // Every breaching build must have returned its base cursor's reader.
            Assert.assertEquals("busy reader count", 0, engine.getBusyReaderCount());
        });
    }

    @Test
    public void testEncodedSortLightFailsOnLargeInput() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (SELECT x AS k FROM long_sequence(100_000))");
            drainWalQueue();
            final String sql = "SELECT * FROM tab ORDER BY k";
            assertUsesFactory(sql, EncodedSortLightRecordCursorFactory.class);
            assertQueryBreaches(sql);
        });
    }

    @Test
    public void testEncodedSortLightRepeatedCursorRunsReleaseAllocations() throws Exception {
        // Repeat the same encodable sort many times; the assertMemoryLeak around
        // the loop is the load-bearing check that the entry buffer's reopen/close
        // cycles release every byte. With the per-query limit active,
        // recordPerQueryMemAlloc's assert also guards malloc/free symmetry on the
        // per-query counter.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (SELECT (50 - x) AS k FROM long_sequence(50))");
            drainWalQueue();
            final String sql = "SELECT * FROM tab ORDER BY k";
            assertUsesFactory(sql, EncodedSortLightRecordCursorFactory.class);
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
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

    @Test
    public void testEncodedSortLightSucceedsOnSmallInput() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (SELECT (5 - x) AS k FROM long_sequence(5))");
            drainWalQueue();
            assertUsesFactory("SELECT * FROM tab ORDER BY k", EncodedSortLightRecordCursorFactory.class);
            assertQuery("SELECT * FROM tab ORDER BY k")
                    .noLeakCheck()
                    .expectSize()
                    .returns("k\n0\n1\n2\n3\n4\n");
        });
    }

    @Test
    public void testEncodedSortRepeatedCursorRunsReleaseAllocations() throws Exception {
        // Non-light variant: the leak loop also cycles the materialized
        // RecordChain, so it covers the record-chain tracker binding in addition
        // to the entry buffer.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a AS (SELECT x AS k FROM long_sequence(30))");
            execute("CREATE TABLE b AS (SELECT x AS k FROM long_sequence(30))");
            drainWalQueue();
            final String sql = "SELECT k FROM (SELECT k FROM a UNION ALL SELECT k FROM b) ORDER BY k";
            assertUsesFactory(sql, EncodedSortRecordCursorFactory.class);
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                for (int i = 0; i < 20; i++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        long rows = 0;
                        while (cursor.hasNext()) {
                            rows++;
                        }
                        Assert.assertEquals(60, rows);
                    }
                }
            }
        });
    }

    private void assertQueryBreaches(String sql) throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
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
    }

    private void assertUsesFactory(String sql, Class<?> factoryClass) throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler();
             RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
            RecordCursorFactory cur = factory;
            while (cur != null) {
                if (factoryClass.isInstance(cur)) {
                    return;
                }
                RecordCursorFactory next = cur.getBaseFactory();
                if (next == cur) {
                    break;
                }
                cur = next;
            }
            Assert.fail("expected " + factoryClass.getSimpleName() + " in base chain of " + factory.getClass().getSimpleName());
        }
    }
}
