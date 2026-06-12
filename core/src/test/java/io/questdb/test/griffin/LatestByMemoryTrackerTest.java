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
 * SQL-level tests that exercise the per-query memory limit through the LATEST BY
 * rowid lists ({@link io.questdb.std.DirectLongList}), together
 * with the LATEST BY hash maps wired alongside them (the dominant allocator on
 * the map-backed paths).
 * <p>
 * Three families are covered:
 * <ul>
 *     <li>indexed LATEST BY ({@code LatestByAllIndexedRecordCursor}) - a
 *     {@code rows} list sized to the symbol cardinality;</li>
 *     <li>non-indexed LATEST BY ({@code LatestByAllRecordCursor}) - a per-key
 *     map plus a {@code rows} list, both scaling with key cardinality;</li>
 *     <li>LATEST BY over a sub-query ({@code LatestByRecordCursorFactory}) - a
 *     per-key map plus a {@code rowIndexes} list.</li>
 * </ul>
 * Each owning cursor binds the active workload's MemoryTracker before the first
 * allocation (lazy construction + reopen) and frees against the same tracker at
 * cursor close, so the malloc/free pairs are charged symmetrically and a runaway
 * query fails at the offending allocation site.
 * <p>
 * The per-query limit is applied per test in {@link #setUpSortPageSize()} via
 * {@code setProperty} so it survives the per-test override reset; the provider
 * reads it live on each tracker acquisition. Tests that should breach the limit
 * use a high-cardinality key; tests that should succeed use a handful of keys.
 */
public class LatestByMemoryTrackerTest extends AbstractCairoTest {

    private static final int HIGH_CARDINALITY = 50_000;

    @Before
    public void setUpSortPageSize() {
        // The per-query limit, re-applied per test because tearDown clears
        // property overrides; the provider reads it live on each acquisition.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 128 * 1024L);
        // The deterministic ORDER BY in the success queries routes to the
        // non-light EncodedSort over the non-random-access LATEST BY sub-query,
        // whose RecordChain now charges the per-query tracker with a 16 MB value
        // page by default. Shrink it so the sort fits under the limit; the LATEST
        // BY structures the breach tests target are unaffected. Re-applied per
        // test because tearDown clears property overrides.
        setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 16 * 1024L);
    }

    @Test
    public void testLatestByIndexedFailsOnHighCardinality() throws Exception {
        // Indexed symbol with HIGH_CARDINALITY distinct values: LatestByAllIndexedRecordCursor
        // sizes its rows list to the symbol count, so the single setCapacity() realloc
        // (~400 KiB) crosses the 128 KiB per-query limit.
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE tab_idx AS (" +
                            "  SELECT (x * 1_000_000L)::timestamp ts, ('s' || x)::symbol sym, x v" +
                            "  FROM long_sequence(" + HIGH_CARDINALITY + ")" +
                            "), INDEX(sym) TIMESTAMP(ts) PARTITION BY DAY"
            );
            drainWalQueue();
            assertBreach("SELECT * FROM tab_idx LATEST ON ts PARTITION BY sym");
        });
    }

    @Test
    public void testLatestByIndexedSucceedsOnLowCardinality() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab_idx_small (ts TIMESTAMP, sym SYMBOL INDEX, v LONG) TIMESTAMP(ts) PARTITION BY DAY");
            execute(
                    "INSERT INTO tab_idx_small VALUES" +
                            "  ('2024-01-01T00:00:00.000000Z', 'a', 1)," +
                            "  ('2024-01-01T00:00:01.000000Z', 'b', 2)," +
                            "  ('2024-01-01T00:00:02.000000Z', 'a', 3)," +
                            "  ('2024-01-01T00:00:03.000000Z', 'b', 4)"
            );
            drainWalQueue();
            // Raise the per-query limit for this success case so returns()'s extra cursor passes
            // (a second read, calculateSize(), the variable-column and factory-property checks) fit
            // alongside the ORDER BY sort's key buffer. The breach tests keep the tight 128 KiB
            // limit from setUpSortPageSize(); the LATEST BY structures here stay well under 1 MiB.
            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 1024 * 1024L);
            assertQuery("SELECT sym, v FROM tab_idx_small LATEST ON ts PARTITION BY sym ORDER BY sym")
                    .noLeakCheck()
                    .expectSize()
                    .returns("sym\tv\n" +
                            "a\t3\n" +
                            "b\t4\n");
        });
    }

    @Test
    public void testLatestByNonIndexedFailsOnHighCardinality() throws Exception {
        // Non-indexed LONG key: LatestByAllRecordCursor grows both a per-key map and the
        // rows list to the key cardinality; both are tracker-bound and breach the limit.
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE tab_noidx AS (" +
                            "  SELECT (x * 1_000_000L)::timestamp ts, x k, x v" +
                            "  FROM long_sequence(" + HIGH_CARDINALITY + ")" +
                            ") TIMESTAMP(ts) PARTITION BY DAY"
            );
            drainWalQueue();
            assertBreach("SELECT * FROM tab_noidx LATEST ON ts PARTITION BY k");
        });
    }

    @Test
    public void testLatestByNonIndexedSucceedsOnLowCardinality() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab_noidx_small (ts TIMESTAMP, k LONG, v LONG) TIMESTAMP(ts) PARTITION BY DAY");
            execute(
                    "INSERT INTO tab_noidx_small VALUES" +
                            "  ('2024-01-01T00:00:00.000000Z', 1, 10)," +
                            "  ('2024-01-01T00:00:01.000000Z', 2, 20)," +
                            "  ('2024-01-01T00:00:02.000000Z', 1, 30)," +
                            "  ('2024-01-01T00:00:03.000000Z', 2, 40)"
            );
            drainWalQueue();
            // Raise the per-query limit for this success case so returns()'s extra cursor passes fit
            // alongside the ORDER BY sort's key buffer; see testLatestByIndexedSucceedsOnLowCardinality.
            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 1024 * 1024L);
            assertQuery("SELECT k, v FROM tab_noidx_small LATEST ON ts PARTITION BY k ORDER BY k")
                    .noLeakCheck()
                    .expectSize()
                    .returns("k\tv\n" +
                            "1\t30\n" +
                            "2\t40\n");
        });
    }

    @Test
    public void testLatestBySubQueryFailsOnHighCardinality() throws Exception {
        // LATEST BY over a SAMPLE BY base (no random access) routes to
        // LatestByRecordCursorFactory, whose latestByMap and rowIndexes are tracker-bound.
        // SAMPLE BY is not tracker-wired, so it feeds the rows through without breaching;
        // the LATEST BY map/list growth trips the limit.
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE tab_sub AS (" +
                            "  SELECT (x * 1_000_000L)::timestamp ts, x k, x v" +
                            "  FROM long_sequence(" + HIGH_CARDINALITY + ")" +
                            ") TIMESTAMP(ts) PARTITION BY DAY"
            );
            drainWalQueue();
            assertBreach(
                    "WITH yy AS (SELECT ts, k, max(v) v FROM tab_sub SAMPLE BY 1s ALIGN TO FIRST OBSERVATION) " +
                            "SELECT * FROM yy LATEST ON ts PARTITION BY k"
            );
        });
    }

    @Test
    public void testLatestBySubQuerySucceedsOnLowCardinality() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab_sub_small (ts TIMESTAMP, k LONG, v LONG) TIMESTAMP(ts) PARTITION BY DAY");
            execute(
                    "INSERT INTO tab_sub_small VALUES" +
                            "  ('2024-01-01T00:00:00.000000Z', 1, 10)," +
                            "  ('2024-01-01T00:00:01.000000Z', 2, 20)," +
                            "  ('2024-01-01T00:00:02.000000Z', 1, 30)," +
                            "  ('2024-01-01T00:00:03.000000Z', 2, 40)"
            );
            drainWalQueue();
            // Raise the per-query limit for this success case so returns()'s extra cursor passes fit
            // alongside the ORDER BY sort's key buffer; see testLatestByIndexedSucceedsOnLowCardinality.
            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 1024 * 1024L);
            assertQuery("WITH yy AS (SELECT ts, k, max(v) v FROM tab_sub_small SAMPLE BY 1s ALIGN TO FIRST OBSERVATION) " +
                    "SELECT k, v FROM yy LATEST ON ts PARTITION BY k ORDER BY k")
                    .noLeakCheck()
                    .expectSize()
                    .returns("k\tv\n" +
                            "1\t30\n" +
                            "2\t40\n");
        });
    }

    @Test
    public void testRepeatedCursorRunsReleaseAllocations() throws Exception {
        // Repeat the same non-indexed LATEST BY many times: close/reopen cycles through the
        // map and the rows list must release every byte they allocate. assertMemoryLeak
        // around the loop is the load-bearing check - a malloc/free asymmetry would show up
        // as a residual native allocation count at the end of the test.
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE tab_loop AS (" +
                            "  SELECT (x * 1_000_000L)::timestamp ts, x % 10 k, x v" +
                            "  FROM long_sequence(1000)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY"
            );
            drainWalQueue();
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile(
                         "SELECT * FROM tab_loop LATEST ON ts PARTITION BY k", sqlExecutionContext
                 ).getRecordCursorFactory()) {
                for (int i = 0; i < 20; i++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        long rows = 0;
                        while (cursor.hasNext()) {
                            rows++;
                        }
                        Assert.assertEquals(10, rows);
                    }
                }
            }
        });
    }

    @Test
    public void testOpenFailureReleasesAllocations() throws Exception {
        // Drive the breach during cursor open() instead of during the scan: with the rows list
        // sized far above the limit, of() first allocates the LATEST BY map and then rows.reopen()
        // breaches, orphaning the map unless the failed open frees it under the still-bound tracker.
        // The key cardinality is small so the scan itself never breaches - only open does. Reusing
        // one factory across opens under assertMemoryLeak catches the resulting leak / counter desync.
        setProperty(PropertyKey.CAIRO_SQL_LATEST_BY_ROW_COUNT, 100_000);
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE tab_open AS (" +
                            "  SELECT (x * 1_000_000L)::timestamp ts, x % 100 k, x v" +
                            "  FROM long_sequence(1000)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY"
            );
            drainWalQueue();
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile(
                         "SELECT * FROM tab_open LATEST ON ts PARTITION BY k", sqlExecutionContext
                 ).getRecordCursorFactory()) {
                for (int i = 0; i < 5; i++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertNotNull(cursor);
                        Assert.fail("expected a per-query memory breach during cursor open");
                    } catch (CairoException e) {
                        Assert.assertTrue("expected isOutOfMemory(), got: " + e.getFlyweightMessage(), e.isOutOfMemory());
                        TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                    }
                }
            }
        });
    }

    private static void assertBreach(String sql) throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            final CompiledQuery cq = compiler.compile(sql, sqlExecutionContext);
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
    }
}
