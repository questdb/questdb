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
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.engine.table.LatestByAllFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.LatestByAllIndexedRecordCursorFactory;
import io.questdb.griffin.engine.table.LatestByAllSymbolsFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.LatestByRecordCursorFactory;
import io.questdb.jit.JitUtil;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
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
    public void testLatestByJitBatchesAcrossManySmallFrames() throws Exception {
        assertJitBatchesAfterSmallFrames(16, 32);
    }

    @Test
    public void testLatestByJitBuffersAllocatedOnFirstBatch() throws Exception {
        Assume.assumeTrue(JitUtil.isJitSupported());
        assertMemoryLeak(() -> {
            sqlExecutionContext.changePageFrameSizes(1, 32);
            try {
                setProperty(PropertyKey.CAIRO_PAGE_FRAME_COLUMN_LIST_CAPACITY, 32768);
                execute("CREATE TABLE jit_lazy AS (SELECT (x % 3)::STRING::SYMBOL s, x v,"
                        + " (x * 1000000)::TIMESTAMP ts FROM long_sequence(100)) TIMESTAMP(ts) PARTITION BY DAY");
                sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
                for (String predicate : new String[]{"v > 0", "v < 50"}) {
                    try (RecordCursorFactory factory = select("SELECT v FROM jit_lazy WHERE " + predicate
                            + " LATEST ON ts PARTITION BY s")) {
                        Assert.assertTrue(factory.usesCompiledFilter());
                        for (int attempt = 0; attempt < 3; attempt++) {
                            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                long count = 0;
                                while (cursor.hasNext()) {
                                    count++;
                                }
                                Assert.fail("expected JIT buffer allocation on the first batch");
                            } catch (CairoException e) {
                                Assert.assertTrue(e.isOutOfMemory());
                                TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                            }
                            Assert.assertNull(sqlExecutionContext.getMemoryTracker());
                        }
                    }
                }
            } finally {
                sqlExecutionContext.restoreToDefaultPageFrameSizes();
            }
        });
    }

    @Test
    public void testLatestByJitResultBufferBoundedAcrossLargeFrames() throws Exception {
        Assume.assumeTrue(JitUtil.isJitSupported());
        assertMemoryLeak(() -> {
            sqlExecutionContext.changePageFrameSizes(65_537, 65_537);
            try {
                setProperty(PropertyKey.CAIRO_PAGE_FRAME_ROWID_LIST_CAPACITY, 131_072);
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 1024 * 1024L);
                execute("CREATE TABLE jit_bound AS (SELECT (x % 2)::STRING::SYMBOL s,"
                        + " (x % 2)::INT k, x v, timestamp_sequence(0, 1) ts"
                        + " FROM long_sequence(65_537)) TIMESTAMP(ts) PARTITION BY DAY");
                sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
                for (String keys : new String[]{"k", "s"}) {
                    try (RecordCursorFactory factory = select("SELECT v FROM jit_bound WHERE v > 0"
                            + " LATEST ON ts PARTITION BY " + keys)) {
                        Assert.assertTrue(factory.usesCompiledFilter());
                        for (int attempt = 0; attempt < 3; attempt++) {
                            long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_OFFLOAD);
                            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                long count = 0;
                                long sum = 0;
                                while (cursor.hasNext()) {
                                    count++;
                                    sum += cursor.getRecord().getLong(0);
                                }
                                Assert.assertEquals(2, count);
                                Assert.assertEquals(131_073, sum);
                                long allocated = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_OFFLOAD) - before;
                                long pointerBytes = 2L * configuration.getPageFrameReduceColumnListCapacity() * Long.BYTES;
                                Assert.assertTrue("native batch buffers: " + allocated,
                                        allocated > pointerBytes && allocated <= 2048L * Long.BYTES + pointerBytes);
                                cursor.toTop();
                                Assert.assertTrue(cursor.hasNext());
                            }
                            Assert.assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_OFFLOAD));
                            Assert.assertNull(sqlExecutionContext.getMemoryTracker());
                        }
                    }
                }
            } finally {
                sqlExecutionContext.restoreToDefaultPageFrameSizes();
            }
        });
    }

    @Test
    public void testLatestByJitOpenFailureReleasesBuffers() throws Exception {
        Assume.assumeTrue(JitUtil.isJitSupported());
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_JIT_BIND_VARS_MEMORY_PAGE_SIZE, 256 * 1024);
            execute("CREATE TABLE jit_open AS (SELECT (x % 3)::STRING::SYMBOL s, (x % 5)::STRING::SYMBOL t,"
                    + " x v, (x * 1000000)::TIMESTAMP ts FROM long_sequence(100)) TIMESTAMP(ts) PARTITION BY DAY");
            bindVariableService.setStr("key", "1");
            bindVariableService.setLong("min", 0);
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            for (String clause : new String[]{"", " AND s='1'", " AND s=:key", " AND s IN ('1','2')"}) {
                try (RecordCursorFactory factory = select("SELECT v FROM jit_open WHERE v > :min" + clause
                        + " LATEST ON ts PARTITION BY s")) {
                    Assert.assertTrue(factory.usesCompiledFilter());
                    for (int i = 0; i < 3; i++) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            Assert.fail("expected JIT buffer allocation to fail");
                        } catch (CairoException e) {
                            Assert.assertTrue(e.isOutOfMemory());
                            TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                        }
                        Assert.assertNull(sqlExecutionContext.getMemoryTracker());
                    }
                }
            }
        });
    }

    @Test
    public void testLatestByJitReleasesBuffersOnClose() throws Exception {
        Assume.assumeTrue(JitUtil.isJitSupported());
        assertMemoryLeak(() -> {
            sqlExecutionContext.changePageFrameSizes(1, 512);
            try {
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 4 * 1024 * 1024L);
                execute("CREATE TABLE jit_memory AS (SELECT (x % 11)::STRING::SYMBOL s, (x % 13)::STRING::SYMBOL t,"
                        + " (x % 17)::STRING::SYMBOL u, x v, (x * 1000000)::TIMESTAMP ts"
                        + " FROM long_sequence(10000)) TIMESTAMP(ts) PARTITION BY DAY");
                bindVariableService.setLong("min", 100);
                sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
                for (String keys : new String[]{"s", "s,t", "s,t,u"}) {
                    try (RecordCursorFactory factory = select("SELECT v FROM jit_memory WHERE v > :min AND v < 9000 LATEST ON ts PARTITION BY " + keys)) {
                        Assert.assertTrue(factory.usesCompiledFilter());
                        for (int iteration = 0; iteration < 6; iteration++) {
                            final MemoryTracker tracker;
                            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                tracker = sqlExecutionContext.getMemoryTracker();
                                Assert.assertNotNull(tracker);
                                if (iteration % 3 == 1) {
                                    Assert.assertTrue(cursor.hasNext());
                                } else if (iteration % 3 == 2) {
                                    while (cursor.hasNext()) {
                                    }
                                }
                                Assert.assertTrue(tracker.getUsed() > 0);
                            }
                            Assert.assertEquals(0, tracker.getUsed());
                            Assert.assertNull(sqlExecutionContext.getMemoryTracker());
                        }
                    }
                }
            } finally {
                sqlExecutionContext.restoreToDefaultPageFrameSizes();
            }
        });
    }

    @Test
    public void testLatestByJitBatchesAcrossMultipleSmallFrames() throws Exception {
        assertJitBatchesAfterSmallFrames(4, 32);
    }

    @Test
    public void testLatestByJitBatchesAfterSmallNewestFrame() throws Exception {
        assertJitBatchesAfterSmallFrames(1, 32);
    }

    @Test
    public void testLatestByJitBatchesForLargeFirstParquetFrame() throws Exception {
        Assume.assumeTrue(JitUtil.isJitSupported());
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 1024 * 1024L);
            setProperty(PropertyKey.CAIRO_PAGE_FRAME_ROWID_LIST_CAPACITY, 262_144);
            setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4096);
            execute("CREATE TABLE jit_large_parquet AS (SELECT (x % 2)::STRING::SYMBOL s,"
                    + " 'a'::SYMBOL t, 'b'::SYMBOL u, x v, timestamp_sequence(0, 1) ts"
                    + " FROM long_sequence(4096)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO jit_large_parquet VALUES ('0', 'a', 'b', 0, '1970-01-02')");
            execute("ALTER TABLE jit_large_parquet CONVERT PARTITION TO PARQUET WHERE ts < '1970-01-02'");
            try {
                // Exclude the active native partition so the first frame is a Parquet row group.
                // Its newest two rows complete every query, even when maxRows is only one or two.
                for (int maxRows : new int[]{1, 2}) {
                    sqlExecutionContext.changePageFrameSizes(1, maxRows);
                    for (String interval : new String[]{"ts < '1970-01-02'",
                            "ts >= '1970-01-01T00:00:00.002048' AND ts < '1970-01-02'"}) {
                        String prefix = "SELECT v FROM jit_large_parquet WHERE v > 0 AND " + interval;
                        ObjList<String> queries = new ObjList<>();
                        queries.add(prefix + " LATEST ON ts PARTITION BY s");
                        queries.add(prefix + " LATEST ON ts PARTITION BY s,t");
                        queries.add(prefix + " LATEST ON ts PARTITION BY s,t,u");
                        queries.add(prefix + " AND s IN ('0','1') LATEST ON ts PARTITION BY s");
                        queries.add(prefix + " AND s = '1' LATEST ON ts PARTITION BY s");
                        queries.add(prefix + " AND s != '0' LATEST ON ts PARTITION BY s");
                        queries.add(prefix + " AND s IN (SELECT '1'::STRING) LATEST ON ts PARTITION BY s");
                        for (int mode : new int[]{SqlJitMode.JIT_MODE_DISABLED, SqlJitMode.JIT_MODE_FORCE_SCALAR, SqlJitMode.JIT_MODE_ENABLED}) {
                            sqlExecutionContext.setJitMode(mode);
                            for (int queryIndex = 0; queryIndex < queries.size(); queryIndex++) {
                                try (RecordCursorFactory factory = select(queries.getQuick(queryIndex))) {
                                    Assert.assertEquals(mode != SqlJitMode.JIT_MODE_DISABLED, factory.usesCompiledFilter());
                                    for (int attempt = 0; attempt < 3; attempt++) {
                                        assertFactory(factory).withContext(sqlExecutionContext).sizeMayVary()
                                                .returns(queryIndex < 4 ? "v\n4095\n4096\n" : "v\n4095\n");
                                        Assert.assertNull(sqlExecutionContext.getMemoryTracker());
                                    }
                                }
                            }
                        }
                    }
                }
            } finally {
                sqlExecutionContext.restoreToDefaultPageFrameSizes();
                sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            }
        });
    }

    @Test
    public void testLatestByJitBatchesWhenNewestFrameCannotCoverTargets() throws Exception {
        assertJitBatchesAfterSmallFrames(1, 1);
    }

    @Test
    public void testLatestBySymbolSetsFailOnHighCardinality() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE symbol_sets AS (SELECT ('s' || x)::SYMBOL a, ('t' || x)::SYMBOL b,"
                    + " ('u' || x)::SYMBOL c, x v, (x * 1000000L)::TIMESTAMP ts FROM long_sequence("
                    + HIGH_CARDINALITY + ")) TIMESTAMP(ts) PARTITION BY DAY");
            for (String keys : new String[]{"a, b", "a, b, c"}) {
                assertBreach("SELECT * FROM symbol_sets LATEST ON ts PARTITION BY " + keys,
                        LatestByAllSymbolsFilteredRecordCursorFactory.class);
            }
        });
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
            assertBreach("SELECT * FROM tab_idx LATEST ON ts PARTITION BY sym", LatestByAllIndexedRecordCursorFactory.class);
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
            assertBreach("SELECT * FROM tab_noidx LATEST ON ts PARTITION BY k", LatestByAllFilteredRecordCursorFactory.class);
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
                            "SELECT * FROM yy LATEST ON ts PARTITION BY k",
                    LatestByRecordCursorFactory.class
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
                assertInTree(factory, LatestByAllFilteredRecordCursorFactory.class);
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
                assertInTree(factory, LatestByAllFilteredRecordCursorFactory.class);
                for (int i = 0; i < 5; i++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertNotNull(cursor);
                        Assert.fail("expected a per-query memory breach during cursor open");
                    } catch (CairoException e) {
                        Assert.assertTrue("expected isOutOfMemory(), got: " + e.getFlyweightMessage(), e.isOutOfMemory());
                        TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                        TestUtils.assertContains(e.getFlyweightMessage(), "workload=QUERY");
                    }
                }
            }
        });
    }

    private static void assertBreach(String sql, Class<?> expectedFactory) throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            final CompiledQuery cq = compiler.compile(sql, sqlExecutionContext);
            try (RecordCursorFactory factory = cq.getRecordCursorFactory();
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                assertInTree(factory, expectedFactory);
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

    private static void assertInTree(RecordCursorFactory factory, Class<?> factoryClass) {
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

    private void assertJitBatchesAfterSmallFrames(int smallPartitionCount, int smallPartitionRows) throws Exception {
        Assume.assumeTrue(JitUtil.isJitSupported());
        assertMemoryLeak(() -> {
            sqlExecutionContext.changePageFrameSizes(1, 512);
            try {
                // The result buffer stays bounded even with an oversized global initial capacity.
                // These queries need the tiny partitions and two older rows.
                setProperty(PropertyKey.CAIRO_PAGE_FRAME_ROWID_LIST_CAPACITY, 32_768);
                execute("CREATE TABLE jit_small_frames AS (SELECT (x % 2)::STRING::SYMBOL s,"
                        + " 'a'::SYMBOL t, 'b'::SYMBOL u, x v, timestamp_sequence(0, 1) ts"
                        + " FROM long_sequence(4096)) TIMESTAMP(ts) PARTITION BY DAY");
                for (int i = 0; i < smallPartitionCount; i++) {
                    execute("INSERT INTO jit_small_frames SELECT '0', 'a', 'b', x + "
                            + (4096 + i * smallPartitionRows) + ", timestamp_sequence("
                            + (i + 1) * 86_400_000_000L + ", 1) FROM long_sequence(" + smallPartitionRows + ")");
                }
                final String expected = "v\n4095\n" + (4096 + smallPartitionCount * smallPartitionRows) + "\n";
                ObjList<String> queries = new ObjList<>();
                queries.add("SELECT v FROM jit_small_frames WHERE v > 0 LATEST ON ts PARTITION BY s");
                queries.add("SELECT v FROM jit_small_frames WHERE v > 0 LATEST ON ts PARTITION BY s,t");
                queries.add("SELECT v FROM jit_small_frames WHERE v > 0 LATEST ON ts PARTITION BY s,t,u");
                queries.add("SELECT v FROM jit_small_frames WHERE v > 0 AND s IN ('0','1') LATEST ON ts PARTITION BY s");
                queries.add("SELECT v FROM jit_small_frames WHERE v > 0 AND s = '1' LATEST ON ts PARTITION BY s");
                queries.add("SELECT v FROM jit_small_frames WHERE v > 0 AND s != '0' LATEST ON ts PARTITION BY s");
                queries.add("SELECT v FROM jit_small_frames WHERE v > 0 AND s IN (SELECT '1'::STRING) LATEST ON ts PARTITION BY s");
                for (int mode : new int[]{SqlJitMode.JIT_MODE_DISABLED, SqlJitMode.JIT_MODE_FORCE_SCALAR, SqlJitMode.JIT_MODE_ENABLED}) {
                    sqlExecutionContext.setJitMode(mode);
                    for (int queryIndex = 0; queryIndex < queries.size(); queryIndex++) {
                        try (RecordCursorFactory factory = select(queries.getQuick(queryIndex))) {
                            Assert.assertEquals(mode != SqlJitMode.JIT_MODE_DISABLED, factory.usesCompiledFilter());
                            for (int attempt = 0; attempt < 3; attempt++) {
                                assertFactory(factory).withContext(sqlExecutionContext).sizeMayVary()
                                        .returns(queryIndex < 4 ? expected : "v\n4095\n");
                                Assert.assertNull(sqlExecutionContext.getMemoryTracker());
                            }
                        }
                    }
                }
            } finally {
                sqlExecutionContext.restoreToDefaultPageFrameSizes();
                sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            }
        });
    }
}
