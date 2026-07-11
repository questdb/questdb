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
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.engine.join.AsOfJoinDenseRecordCursorFactory;
import io.questdb.griffin.engine.join.AsOfJoinDenseSingleSymbolRecordCursorFactory;
import io.questdb.griffin.engine.join.AsOfJoinFastRecordCursorFactory;
import io.questdb.griffin.engine.join.AsOfJoinLightRecordCursorFactory;
import io.questdb.griffin.engine.join.AsOfJoinMemoizedRecordCursorFactory;
import io.questdb.griffin.engine.join.HashJoinLightRecordCursorFactory;
import io.questdb.griffin.engine.join.HashOuterJoinFilteredLightRecordCursorFactory;
import io.questdb.griffin.engine.join.HashOuterJoinFilteredRecordCursorFactory;
import io.questdb.griffin.engine.join.HashOuterJoinLightRecordCursorFactory;
import io.questdb.griffin.engine.join.HashOuterJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.LtJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.NestedLoopFullJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.SpliceJoinLightRecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * SQL-level tests that exercise the per-query memory limit through the
 * tracker-aware secondary join cursors in {@link io.questdb.griffin.engine.join}.
 * Light outer joins grow a join-key {@link io.questdb.cairo.map.Map} plus a
 * {@link io.questdb.griffin.engine.join.LongChain} of every slave rowid; the
 * keyed merge joins (ASOF / LT / SPLICE) grow a per-key map that never evicts
 * without TOLERANCE; the nested-loop full join grows a matched-rowid map. All
 * scale with join-key cardinality (or slave row count) and bind the active
 * workload's MemoryTracker in the cursor's of() before the first reopen(), so a
 * runaway join breaches the limit at the offending allocation.
 * <p>
 * The per-query limit is set to 512 KiB per test in {@link #setUp()} (so it
 * survives the per-test override reset; the provider reads it live on each
 * tracker acquisition), enough to clear the LongChain's fixed 128 KiB initial
 * page (plus the small join maps) for the small-input / leak-loop cases while a
 * high-cardinality join breaches.
 */
public class JoinMemoryTrackerTest extends AbstractCairoTest {

    @Before
    public void setUp() {
        super.setUp();
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 512 * 1024L);
    }

    @Test
    public void testAsOfJoinDenseFailsOnLargeInput() throws Exception {
        // asof_dense + multi-key routes to AsOfJoinDenseRecordCursorFactory. Its scan maps memorize every
        // distinct join key (no eviction without TOLERANCE), so a high-cardinality join grows them past the limit.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m AS (SELECT x::SYMBOL k1, x::SYMBOL k2, (x * 1_000_000L)::timestamp ts FROM long_sequence(40_000)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE s AS (SELECT x::SYMBOL k1, x::SYMBOL k2, (x * 1_000_000L)::timestamp ts FROM long_sequence(40_000)) TIMESTAMP(ts) PARTITION BY DAY");
            drainWalQueue();
            final String sql = "SELECT /*+ asof_dense(m s) */ m.k1 FROM m ASOF JOIN s ON (m.k1 = s.k1 AND m.k2 = s.k2)";
            assertUsesFactory(sql, AsOfJoinDenseRecordCursorFactory.class);
            assertQueryBreaches(sql);
        });
    }

    @Test
    public void testAsOfJoinDenseOpenFailureReleasesAllocations() throws Exception {
        // asof_dense + multi-key routes to AsOfJoinDenseRecordCursorFactory, whose of() reopens two
        // tracker-bound SingleRecordSinks; a tiny limit breaches that reopen. The reuse loop asserts the
        // open-error path frees each cursor once and leaves the factory reusable (assertMemoryLeak guards leaks).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m AS (SELECT x::SYMBOL k1, x::SYMBOL k2, (x * 1_000_000L)::timestamp ts FROM long_sequence(4)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE s AS (SELECT x::SYMBOL k1, x::SYMBOL k2, (x * 1_000_000L)::timestamp ts FROM long_sequence(4)) TIMESTAMP(ts) PARTITION BY DAY");
            drainWalQueue();
            // Set the tiny limit only after table creation so the populating SELECT does not breach it.
            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 4L);
            assertOpenFailureReleasesAllocations(
                    "SELECT /*+ asof_dense(m s) */ m.k1 FROM m ASOF JOIN s ON (m.k1 = s.k1 AND m.k2 = s.k2)",
                    AsOfJoinDenseRecordCursorFactory.class
            );
        });
    }

    @Test
    public void testAsOfJoinDenseRepeatedCursorRunsReleaseAllocations() throws Exception {
        // The reuse loop cycles the dense cursor's sinks and scan maps; assertMemoryLeak is the load-bearing
        // check that their malloc/free stays symmetric on the per-query counter after the of() reorder.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m AS (SELECT x::SYMBOL k1, x::SYMBOL k2, (x * 1_000_000L)::timestamp ts FROM long_sequence(50)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE s AS (SELECT x::SYMBOL k1, x::SYMBOL k2, (x * 1_000_000L)::timestamp ts FROM long_sequence(50)) TIMESTAMP(ts) PARTITION BY DAY");
            drainWalQueue();
            final String sql = "SELECT /*+ asof_dense(m s) */ m.k1 FROM m ASOF JOIN s ON (m.k1 = s.k1 AND m.k2 = s.k2)";
            assertUsesFactory(sql, AsOfJoinDenseRecordCursorFactory.class);
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
    public void testAsOfJoinDenseSingleSymbolFailsOnLargeInput() throws Exception {
        // asof_dense + single SYMBOL routes to AsOfJoinDenseSingleSymbolRecordCursorFactory (no sinks; scan
        // maps bound via the base setMemoryTracker). A high-cardinality join grows the maps past the limit.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m AS (SELECT x::SYMBOL k, (x * 1_000_000L)::timestamp ts FROM long_sequence(40_000)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE s AS (SELECT x::SYMBOL k, (x * 1_000_000L)::timestamp ts FROM long_sequence(40_000)) TIMESTAMP(ts) PARTITION BY DAY");
            drainWalQueue();
            final String sql = "SELECT /*+ asof_dense(m s) */ m.k FROM m ASOF JOIN s ON k";
            assertUsesFactory(sql, AsOfJoinDenseSingleSymbolRecordCursorFactory.class);
            assertQueryBreaches(sql);
        });
    }

    @Test
    public void testAsOfJoinDenseSingleSymbolOpenFailureReleasesAllocations() throws Exception {
        // Single-symbol dense has no sinks, so of() breaches on the first scan-map reopen() under a tiny
        // limit. The reuse loop asserts the open-error path frees each cursor once (the map reopen runs
        // before super.of() adopts the cursors, so close() finds null cursors) and leaves the factory reusable.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m AS (SELECT x::SYMBOL k, (x * 1_000_000L)::timestamp ts FROM long_sequence(4)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE s AS (SELECT x::SYMBOL k, (x * 1_000_000L)::timestamp ts FROM long_sequence(4)) TIMESTAMP(ts) PARTITION BY DAY");
            drainWalQueue();
            // Tiny limit set after table creation so the populating SELECT does not breach it.
            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 4L);
            assertOpenFailureReleasesAllocations(
                    "SELECT /*+ asof_dense(m s) */ m.k FROM m ASOF JOIN s ON k",
                    AsOfJoinDenseSingleSymbolRecordCursorFactory.class
            );
        });
    }

    @Test
    public void testAsOfJoinDenseSingleSymbolRepeatedCursorRunsReleaseAllocations() throws Exception {
        // The reuse loop cycles the single-symbol dense cursor's scan maps; assertMemoryLeak is the
        // load-bearing check that their malloc/free stays symmetric on the per-query counter.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m AS (SELECT x::SYMBOL k, (x * 1_000_000L)::timestamp ts FROM long_sequence(50)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE s AS (SELECT x::SYMBOL k, (x * 1_000_000L)::timestamp ts FROM long_sequence(50)) TIMESTAMP(ts) PARTITION BY DAY");
            drainWalQueue();
            final String sql = "SELECT /*+ asof_dense(m s) */ m.k FROM m ASOF JOIN s ON k";
            assertUsesFactory(sql, AsOfJoinDenseSingleSymbolRecordCursorFactory.class);
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
    public void testAsOfJoinFastOpenFailureReleasesAllocations() throws Exception {
        // A keyed ASOF join over a time-frame slave routes to AsOfJoinFastRecordCursorFactory, whose of()
        // reopens two tracker-bound SingleRecordSinks; a tiny limit breaches that reopen. The reuse loop
        // asserts the open-error path frees each cursor once and leaves the factory reusable.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m AS (SELECT x::SYMBOL k, (x * 1_000_000L)::timestamp ts FROM long_sequence(4)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE s AS (SELECT x::SYMBOL k, (x * 1_000_000L)::timestamp ts FROM long_sequence(4)) TIMESTAMP(ts) PARTITION BY DAY");
            drainWalQueue();
            // Tiny limit set after table creation so the populating SELECT does not breach it.
            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 4L);
            assertOpenFailureReleasesAllocations(
                    "SELECT m.k FROM m ASOF JOIN s ON k",
                    AsOfJoinFastRecordCursorFactory.class
            );
        });
    }

    @Test
    public void testAsOfJoinFastRepeatedCursorRunsReleaseAllocations() throws Exception {
        // The reuse loop cycles the fast cursor's sinks; assertMemoryLeak is the load-bearing check that
        // their malloc/free stays symmetric on the per-query counter after the of() reorder.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m AS (SELECT x::SYMBOL k, (x * 1_000_000L)::timestamp ts FROM long_sequence(50)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE s AS (SELECT x::SYMBOL k, (x * 1_000_000L)::timestamp ts FROM long_sequence(50)) TIMESTAMP(ts) PARTITION BY DAY");
            drainWalQueue();
            final String sql = "SELECT m.k FROM m ASOF JOIN s ON k";
            assertUsesFactory(sql, AsOfJoinFastRecordCursorFactory.class);
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
    public void testAsOfJoinLightFailsOnLargeInput() throws Exception {
        // The asof_linear hint forces the AsOfJoinLight fallback (skipping the
        // Fast/Indexed paths); its join-key map never evicts without TOLERANCE.
        // The result is iterated (not count(*), which would short-circuit via
        // calculateSize without building the map) so the map actually grows.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m AS (SELECT x::SYMBOL k, (x * 1_000_000L)::timestamp ts FROM long_sequence(40_000)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE s AS (SELECT x::SYMBOL k, (x * 1_000_000L)::timestamp ts FROM long_sequence(40_000)) TIMESTAMP(ts) PARTITION BY DAY");
            drainWalQueue();
            final String sql = "SELECT /*+ asof_linear(m s) */ m.k FROM m ASOF JOIN s ON k";
            assertUsesFactory(sql, AsOfJoinLightRecordCursorFactory.class);
            assertQueryBreaches(sql);
        });
    }

    @Test
    public void testAsOfJoinLightOpenFailureReleasesAllocations() throws Exception {
        // Inflate the join-key map far above the limit so AsOfJoinLight's of() breaches on
        // joinKeyToRowId.reopen() with isOpen already set; the asof_linear hint forces the
        // AsOfJoinLight fallback. A small input keeps every other allocation under the limit.
        setProperty(PropertyKey.CAIRO_SQL_SMALL_MAP_KEY_CAPACITY, 50_000);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m AS (SELECT x AS k, (x * 1_000_000L)::timestamp ts FROM long_sequence(20)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE s AS (SELECT x AS k, (x * 1_000_000L)::timestamp ts FROM long_sequence(20)) TIMESTAMP(ts) PARTITION BY DAY");
            drainWalQueue();
            assertOpenFailureReleasesAllocations(
                    "SELECT /*+ asof_linear(m s) */ m.k FROM m ASOF JOIN s ON k",
                    AsOfJoinLightRecordCursorFactory.class
            );
        });
    }

    @Test
    public void testAsOfJoinMemoizedFailsOnLargeInput() throws Exception {
        // asof_memoized + single SYMBOL routes to AsOfJoinMemoizedRecordCursorFactory. Its rememberedSymbols
        // map caches one entry per distinct symbol (no eviction), so a high-cardinality join grows it past the limit.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m AS (SELECT x::SYMBOL k, (x * 1_000_000L)::timestamp ts FROM long_sequence(40_000)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE s AS (SELECT x::SYMBOL k, (x * 1_000_000L)::timestamp ts FROM long_sequence(40_000)) TIMESTAMP(ts) PARTITION BY DAY");
            drainWalQueue();
            final String sql = "SELECT /*+ asof_memoized(m s) */ m.k FROM m ASOF JOIN s ON k";
            assertUsesFactory(sql, AsOfJoinMemoizedRecordCursorFactory.class);
            assertQueryBreaches(sql);
        });
    }

    @Test
    public void testAsOfJoinMemoizedOpenFailureReleasesAllocations() throws Exception {
        // Memoized has no sinks, so of() breaches on rememberedSymbols.reopen() under a tiny limit. The
        // reuse loop asserts the open-error path frees each cursor once (the reopen runs before super.of()
        // adopts the cursors, so close() finds null cursors) and leaves the factory reusable.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m AS (SELECT x::SYMBOL k, (x * 1_000_000L)::timestamp ts FROM long_sequence(4)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE s AS (SELECT x::SYMBOL k, (x * 1_000_000L)::timestamp ts FROM long_sequence(4)) TIMESTAMP(ts) PARTITION BY DAY");
            drainWalQueue();
            // Tiny limit set after table creation so the populating SELECT does not breach it.
            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 4L);
            assertOpenFailureReleasesAllocations(
                    "SELECT /*+ asof_memoized(m s) */ m.k FROM m ASOF JOIN s ON k",
                    AsOfJoinMemoizedRecordCursorFactory.class
            );
        });
    }

    @Test
    public void testAsOfJoinMemoizedRepeatedCursorRunsReleaseAllocations() throws Exception {
        // The reuse loop cycles the memoized cursor's rememberedSymbols map; assertMemoryLeak is the
        // load-bearing check that its malloc/free stays symmetric on the per-query counter.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m AS (SELECT x::SYMBOL k, (x * 1_000_000L)::timestamp ts FROM long_sequence(50)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE s AS (SELECT x::SYMBOL k, (x * 1_000_000L)::timestamp ts FROM long_sequence(50)) TIMESTAMP(ts) PARTITION BY DAY");
            drainWalQueue();
            final String sql = "SELECT /*+ asof_memoized(m s) */ m.k FROM m ASOF JOIN s ON k";
            assertUsesFactory(sql, AsOfJoinMemoizedRecordCursorFactory.class);
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
    public void testHashJoinLightOpenFailureReleasesAllocations() throws Exception {
        // Inflate the slave LongChain page above the limit so HashJoinLight's of() allocates
        // the join-key map (region 1) and then breaches on the slave chain reopen (region 2),
        // orphaning the map unless the failed open frees the cursor under the still-bound
        // tracker. A small input keeps the join-key map well under the limit.
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_LIGHT_VALUE_PAGE_SIZE, 2 * 1024 * 1024L);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m AS (SELECT x::SYMBOL k FROM long_sequence(20))");
            execute("CREATE TABLE s AS (SELECT x::SYMBOL k, x AS v FROM long_sequence(20))");
            drainWalQueue();
            assertOpenFailureReleasesAllocations(
                    "SELECT m.k, s.v FROM m JOIN s ON k",
                    HashJoinLightRecordCursorFactory.class
            );
        });
    }

    @Test
    public void testHashOuterJoinFilteredFullFatOpenFailureReleasesAllocations() throws Exception {
        // A LEFT JOIN with a non-equi filter forces the full-fat HashOuterJoinFilteredRecordCursorFactory.
        // Its LEFT cursor has no match-ids map, so of() breaches cleanly on joinKeyMap.reopen() with
        // isOpen already set once the inflated map crosses the limit. Reusing the factory catches a
        // failed open that leaves isOpen stuck.
        setProperty(PropertyKey.CAIRO_SQL_SMALL_MAP_KEY_CAPACITY, 50_000);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m AS (SELECT x AS k, x AS v FROM long_sequence(20))");
            execute("CREATE TABLE s AS (SELECT x AS k, x AS v FROM long_sequence(20))");
            drainWalQueue();
            assertFullFatOpenFailureReleasesAllocations(
                    "SELECT m.k FROM m LEFT JOIN s ON m.k = s.k AND m.v <> s.v",
                    HashOuterJoinFilteredRecordCursorFactory.class
            );
        });
    }

    @Test
    public void testHashOuterJoinFilteredFullFatRepeatedCursorRunsReleaseAllocations() throws Exception {
        // A full-fat FULL OUTER JOIN with a non-equi filter cycles the join-key map, the match-ids map
        // and the slave RecordChain - all now tracker-bound. assertMemoryLeak around the reuse loop is
        // the load-bearing check that each structure's malloc/free nets to zero on the per-query counter.
        // Shrink the slave-chain page so its default 16 MiB page does not breach the 512 KiB limit on
        // the first record; a small input then fits comfortably under the limit.
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_PAGE_SIZE, 64 * 1024L);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m AS (SELECT x AS k, x AS v FROM long_sequence(50))");
            execute("CREATE TABLE s AS (SELECT x AS k, (2 * x) AS v FROM long_sequence(50))");
            drainWalQueue();
            final String sql = "SELECT m.k, s.v FROM m FULL OUTER JOIN s ON m.k = s.k AND m.v <> s.v";
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.setFullFatJoins(true);
                try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                    Assert.assertTrue(
                            "expected HashOuterJoinFilteredRecordCursorFactory, got: " + factory.getClass().getSimpleName(),
                            isFactoryInChain(factory, HashOuterJoinFilteredRecordCursorFactory.class)
                    );
                    for (int i = 0; i < 20; i++) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            long rows = 0;
                            while (cursor.hasNext()) {
                                rows++;
                            }
                            Assert.assertTrue("iteration " + i + " produced no rows", rows > 0);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testHashOuterJoinFilteredFullFatSymbolFilterResolves() throws Exception {
        // A full-fat FULL OUTER JOIN whose non-equi filter compares SYMBOL columns from both sides.
        // filter.init() resolves those columns through the join cursor's getSymbolTable(), which reads
        // the adopted master/slave cursors, so of() must adopt them BEFORE filter.init(). count=5 (one
        // matched pair, two unmatched master, two unmatched slave) proves the symbol filter both inited
        // without NPE and evaluated correctly; a fix that adopts after filter.init() would NPE here.
        // Shrink the slave-chain page so its default 16 MiB page does not breach the 512 KiB limit.
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_PAGE_SIZE, 64 * 1024L);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (g INT, k SYMBOL)");
            execute("INSERT INTO m VALUES (1, 'a'), (2, 'b'), (3, 'c')");
            execute("CREATE TABLE s (g INT, k SYMBOL)");
            execute("INSERT INTO s VALUES (1, 'a'), (2, 'x'), (3, 'c')");
            drainWalQueue();
            final String sql = "SELECT count(*) FROM (SELECT m.k mk, s.k sk FROM m FULL OUTER JOIN s ON m.g = s.g AND m.k <> s.k)";
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.setFullFatJoins(true);
                try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                    Assert.assertTrue(
                            "expected HashOuterJoinFilteredRecordCursorFactory, got: " + factory.getClass().getSimpleName(),
                            isFactoryInChain(factory, HashOuterJoinFilteredRecordCursorFactory.class)
                    );
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(5, cursor.getRecord().getLong(0));
                        Assert.assertFalse(cursor.hasNext());
                    }
                }
            }
        });
    }

    @Test
    public void testHashOuterJoinFilteredLightOpenFailureReleasesAllocations() throws Exception {
        // A FULL OUTER JOIN with a non-equi filter routes to the filtered light factory whose
        // full-outer cursor reopens a match-ids map (region 1) before the slave chain (region 2).
        // Inflating the slave LongChain page makes of() breach on the chain, orphaning the
        // match-ids map unless the failed open frees the cursor under the still-bound tracker.
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_LIGHT_VALUE_PAGE_SIZE, 2 * 1024 * 1024L);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m AS (SELECT x::SYMBOL k, x AS v FROM long_sequence(20))");
            execute("CREATE TABLE s AS (SELECT x::SYMBOL k, x AS v FROM long_sequence(20))");
            drainWalQueue();
            assertOpenFailureReleasesAllocations(
                    "SELECT * FROM m FULL OUTER JOIN s ON m.k = s.k AND m.v <> s.v",
                    HashOuterJoinFilteredLightRecordCursorFactory.class
            );
        });
    }

    @Test
    public void testHashOuterJoinFilteredLightSymbolFilterResolves() throws Exception {
        // Light-path counterpart of testHashOuterJoinFilteredFullFatSymbolFilterResolves: a FULL OUTER
        // JOIN whose non-equi filter compares SYMBOL columns routes to the filtered light factory. Its
        // of() adopts master/slave before filter.init(), which resolves the symbols via getSymbolTable().
        // count=5 proves correct resolution; a fix that adopts after filter.init() would NPE here.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (g INT, k SYMBOL)");
            execute("INSERT INTO m VALUES (1, 'a'), (2, 'b'), (3, 'c')");
            execute("CREATE TABLE s (g INT, k SYMBOL)");
            execute("INSERT INTO s VALUES (1, 'a'), (2, 'x'), (3, 'c')");
            drainWalQueue();
            final String sql = "SELECT count(*) FROM (SELECT m.k mk, s.k sk FROM m FULL OUTER JOIN s ON m.g = s.g AND m.k <> s.k)";
            assertUsesFactory(sql, HashOuterJoinFilteredLightRecordCursorFactory.class);
            assertQuery(sql).noLeakCheck().noRandomAccess().expectSize().returns("count\n5\n");
        });
    }

    @Test
    public void testHashOuterJoinFullFatOpenFailureReleasesAllocations() throws Exception {
        // Force the full-fat HashOuterJoinRecordCursorFactory and inflate the join-key map above the
        // limit so its of() breaches on joinKeyMap.reopen() with isOpen already set. A small input
        // keeps every other allocation under the limit; reusing one factory across opens catches a
        // failed open that leaves isOpen stuck (a later open would skip reopen() and stop breaching).
        setProperty(PropertyKey.CAIRO_SQL_SMALL_MAP_KEY_CAPACITY, 50_000);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m AS (SELECT x AS k FROM long_sequence(20))");
            execute("CREATE TABLE s AS (SELECT x AS k, x AS v FROM long_sequence(20))");
            drainWalQueue();
            assertFullFatOpenFailureReleasesAllocations(
                    "SELECT m.k, s.v FROM m LEFT JOIN s ON k",
                    HashOuterJoinRecordCursorFactory.class
            );
        });
    }

    @Test
    public void testHashOuterJoinLightFailsOnLargeInput() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m AS (SELECT x::SYMBOL k FROM long_sequence(40_000))");
            execute("CREATE TABLE s AS (SELECT x::SYMBOL k, x AS v FROM long_sequence(40_000))");
            drainWalQueue();
            final String sql = "SELECT m.k, s.v FROM m LEFT JOIN s ON k";
            assertUsesFactory(sql, HashOuterJoinLightRecordCursorFactory.class);
            assertQueryBreaches(sql);
        });
    }

    @Test
    public void testHashOuterJoinLightOpenFailureReleasesAllocations() throws Exception {
        // Inflate the slave LongChain page above the limit so HashOuterJoinLight's of() breaches
        // on the slave chain reopen with isOpen already set. A small input keeps the join-key map
        // under the limit; reusing one factory across opens catches the isOpen desync.
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_LIGHT_VALUE_PAGE_SIZE, 2 * 1024 * 1024L);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m AS (SELECT x::SYMBOL k FROM long_sequence(20))");
            execute("CREATE TABLE s AS (SELECT x::SYMBOL k, x AS v FROM long_sequence(20))");
            drainWalQueue();
            assertOpenFailureReleasesAllocations(
                    "SELECT m.k, s.v FROM m LEFT JOIN s ON k",
                    HashOuterJoinLightRecordCursorFactory.class
            );
        });
    }

    @Test
    public void testHashOuterJoinLightRepeatedCursorRunsReleaseAllocations() throws Exception {
        // The leak loop cycles both the join-key map and the LongChain slave
        // chain; the assertMemoryLeak around it is the load-bearing check that
        // the new LongChain tracker binding keeps malloc/free symmetric.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m AS (SELECT x::SYMBOL k FROM long_sequence(50))");
            execute("CREATE TABLE s AS (SELECT x::SYMBOL k, x AS v FROM long_sequence(50))");
            drainWalQueue();
            final String sql = "SELECT m.k, s.v FROM m LEFT JOIN s ON k";
            assertUsesFactory(sql, HashOuterJoinLightRecordCursorFactory.class);
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
    public void testHashOuterJoinLightSucceedsOnSmallInput() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m AS (SELECT x::SYMBOL k FROM long_sequence(20))");
            execute("CREATE TABLE s AS (SELECT x::SYMBOL k, x AS v FROM long_sequence(20))");
            drainWalQueue();
            assertQuery("SELECT count(*) FROM (SELECT m.k, s.v FROM m LEFT JOIN s ON k)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n20\n");
        });
    }

    @Test
    public void testLtJoinFullFatOpenFailureReleasesAllocations() throws Exception {
        // Force the full-fat LtJoinRecordCursorFactory and inflate the join-key map above
        // the limit so its of() breaches on joinKeyMapA.reopen() with isOpen set; TOLERANCE
        // adds the second evacuation map. Reusing the factory catches a failed open that
        // leaves isOpen stuck (a later open would skip reopen() and stop breaching).
        setProperty(PropertyKey.CAIRO_SQL_SMALL_MAP_KEY_CAPACITY, 50_000);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m AS (SELECT x AS k, (x * 1_000_000L)::timestamp ts FROM long_sequence(20)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE s AS (SELECT x AS k, (x * 1_000_000L)::timestamp ts FROM long_sequence(20)) TIMESTAMP(ts) PARTITION BY DAY");
            drainWalQueue();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.setFullFatJoins(true);
                try (RecordCursorFactory factory = compiler.compile(
                        "SELECT m.k FROM m LT JOIN s ON k TOLERANCE 2s",
                        sqlExecutionContext
                ).getRecordCursorFactory()) {
                    Assert.assertTrue(
                            "expected LtJoinRecordCursorFactory, got: " + factory.getClass().getSimpleName(),
                            isFactoryInChain(factory, LtJoinRecordCursorFactory.class)
                    );
                    for (int i = 0; i < 5; i++) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            Assert.fail("expected a per-query memory breach during cursor open at iteration " + i);
                        } catch (CairoException e) {
                            Assert.assertTrue("expected isOutOfMemory(), got: " + e.getFlyweightMessage(), e.isOutOfMemory());
                            TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                            TestUtils.assertContains(e.getFlyweightMessage(), "workload=QUERY");
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testMarkoutHorizonCompileWithoutOpenDoesNotLeak() throws Exception {
        // The markout cross-join optimization holds a RecordArray for the slave offset grid. It is
        // lazy (no native backing until the first of() materializes the slave), so a factory compiled
        // but never opened frees nothing on close(). assertMemoryLeak catches a regression that makes
        // the RecordArray allocate eagerly in the constructor.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, order_ts TIMESTAMP) TIMESTAMP(order_ts)");
            execute("INSERT INTO orders VALUES (1, 0::timestamp), (2, 1_000_000::timestamp)");
            final String sql = "SELECT /*+ markout_horizon(orders offsets) */ id, order_ts + usec_offs AS ts " +
                    "FROM orders CROSS JOIN (SELECT 1_000_000 * (x-1) AS usec_offs FROM long_sequence(100)) offsets " +
                    "ORDER BY order_ts + usec_offs";
            assertUsesMarkoutHorizon(sql);
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory ignored = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                // intentionally never call getCursor()
            }
        });
    }

    @Test
    public void testMarkoutHorizonFailsOnLargeSlave() throws Exception {
        // The markout cross-join optimization materializes the entire slave (the long_sequence offset
        // grid) into a RecordArray during the cursor's of(). A huge offset grid grows that RecordArray
        // (now tracker-bound) past the limit, so getCursor() breaches with isOutOfMemory() set. Without
        // the binding the RecordArray escapes the limit and the open completes, firing Assert.fail.
        // assertMemoryLeak additionally guards the of()-throw path: the partial RecordArray must be freed.
        // Shrink the RecordArray page so the breach comes from accumulating ~32K offset rows under the
        // 512 KiB limit rather than from the default 16 MiB first page.
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_PAGE_SIZE, 64 * 1024L);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, order_ts TIMESTAMP) TIMESTAMP(order_ts)");
            execute("INSERT INTO orders VALUES (1, 0::timestamp), (2, 1_000_000::timestamp)");
            final String sql = "SELECT /*+ markout_horizon(orders offsets) */ id, order_ts + usec_offs AS ts " +
                    "FROM orders CROSS JOIN (SELECT 1_000_000 * (x-1) AS usec_offs FROM long_sequence(5_000_000)) offsets " +
                    "ORDER BY order_ts + usec_offs";
            // No assertUsesMarkoutHorizon here: EXPLAIN opens the underlying cursor and would
            // materialize the whole 5M-row slave. Routing is proven by the small-slave sibling tests.
            assertQueryBreaches(sql);
        });
    }

    @Test
    public void testMarkoutHorizonOpenFailureReleasesAllocations() throws Exception {
        // A tiny limit breaches the markout cursor's of() on the first slave RecordArray page (the
        // materialization runs before the cursor adopts master/slave). Reusing the factory across opens
        // asserts the open-error path frees the partial RecordArray once and leaves the factory reusable;
        // assertMemoryLeak guards leaks across the reuse loop.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, order_ts TIMESTAMP) TIMESTAMP(order_ts)");
            execute("INSERT INTO orders VALUES (1, 0::timestamp), (2, 1_000_000::timestamp)");
            // Tiny limit set after table creation so the populating INSERT does not breach it.
            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 4L);
            final String sql = "SELECT /*+ markout_horizon(orders offsets) */ id, order_ts + usec_offs AS ts " +
                    "FROM orders CROSS JOIN (SELECT 1_000_000 * (x-1) AS usec_offs FROM long_sequence(100)) offsets " +
                    "ORDER BY order_ts + usec_offs";
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                for (int i = 0; i < 5; i++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.fail("expected a per-query memory breach during cursor open at iteration " + i);
                    } catch (CairoException e) {
                        Assert.assertTrue("expected isOutOfMemory(), got: " + e.getFlyweightMessage(), e.isOutOfMemory());
                        TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                        TestUtils.assertContains(e.getFlyweightMessage(), "workload=QUERY");
                    }
                }
            }
        });
    }

    @Test
    public void testMarkoutHorizonRepeatedCursorRunsReleaseAllocations() throws Exception {
        // The reuse loop cycles the markout cursor's slave RecordArray (now tracker-bound) across
        // of()/close(); assertMemoryLeak is the load-bearing check that its malloc/free stays symmetric
        // on the per-query counter.
        // Shrink the RecordArray page so the small offset grid stays well under the 512 KiB limit
        // (the default 16 MiB first page would breach on the first row).
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_PAGE_SIZE, 64 * 1024L);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, order_ts TIMESTAMP) TIMESTAMP(order_ts)");
            execute("INSERT INTO orders VALUES (1, 0::timestamp), (2, 1_000_000::timestamp), (3, 2_000_000::timestamp)");
            final String sql = "SELECT /*+ markout_horizon(orders offsets) */ id, order_ts + usec_offs AS ts " +
                    "FROM orders CROSS JOIN (SELECT 1_000_000 * (x-1) AS usec_offs FROM long_sequence(100)) offsets " +
                    "ORDER BY order_ts + usec_offs";
            assertUsesMarkoutHorizon(sql);
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                for (int i = 0; i < 20; i++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        long rows = 0;
                        while (cursor.hasNext()) {
                            rows++;
                        }
                        Assert.assertEquals(300, rows);
                    }
                }
            }
        });
    }

    @Test
    public void testNestedLoopFullJoinFailsOnLargeInput() throws Exception {
        // A one-row master keeps the nested loop cheap while the non-equi FULL
        // OUTER condition matches nearly every slave row, growing the matched-id
        // map past the limit.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m AS (SELECT 1L AS k FROM long_sequence(1))");
            execute("CREATE TABLE s AS (SELECT x AS k FROM long_sequence(100_000))");
            drainWalQueue();
            final String sql = "SELECT count(*) FROM m FULL OUTER JOIN s ON m.k <> s.k";
            assertUsesFactory(sql, NestedLoopFullJoinRecordCursorFactory.class);
            assertQueryBreaches(sql);
        });
    }

    @Test
    public void testNestedLoopFullJoinOpenFailureReleasesAllocations() throws Exception {
        // Inflate the matched-id map far above the limit so the nested-loop full join's of()
        // breaches on matchIdsMap.reopen() with isOpen already set. A small input keeps every other
        // allocation under the limit; reusing one factory across opens catches the isOpen desync.
        setProperty(PropertyKey.CAIRO_SQL_SMALL_MAP_KEY_CAPACITY, 50_000);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m AS (SELECT x AS k FROM long_sequence(20))");
            execute("CREATE TABLE s AS (SELECT x AS k FROM long_sequence(20))");
            drainWalQueue();
            assertOpenFailureReleasesAllocations(
                    "SELECT * FROM m FULL OUTER JOIN s ON m.k <> s.k",
                    NestedLoopFullJoinRecordCursorFactory.class
            );
        });
    }

    @Test
    public void testNestedLoopFullJoinSymbolFilterResolves() throws Exception {
        // A FULL OUTER JOIN with a non-equi SYMBOL condition (no equi key) routes to the nested-loop
        // full join. Its of() adopts master/slave before filter.init(), which resolves the symbol
        // columns via getSymbolTable(). count=7 (seven matched pairs, no unmatched rows) proves the
        // symbol filter inited and evaluated correctly; a fix that adopts after filter.init() would NPE.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (k SYMBOL)");
            execute("INSERT INTO m VALUES ('a'), ('b'), ('c')");
            execute("CREATE TABLE s (k SYMBOL)");
            execute("INSERT INTO s VALUES ('a'), ('x'), ('c')");
            drainWalQueue();
            final String sql = "SELECT count(*) FROM (SELECT m.k mk, s.k sk FROM m FULL OUTER JOIN s ON m.k <> s.k)";
            assertUsesFactory(sql, NestedLoopFullJoinRecordCursorFactory.class);
            assertQuery(sql).noLeakCheck().noRandomAccess().expectSize().returns("count\n7\n");
        });
    }

    @Test
    public void testSpliceJoinFailsOnLargeInput() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m AS (SELECT x::SYMBOL k, (x * 1_000_000L)::timestamp ts FROM long_sequence(40_000)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE s AS (SELECT x::SYMBOL k, (x * 1_000_000L)::timestamp ts FROM long_sequence(40_000)) TIMESTAMP(ts) PARTITION BY DAY");
            drainWalQueue();
            final String sql = "SELECT count(*) FROM m SPLICE JOIN s ON k";
            assertUsesFactory(sql, SpliceJoinLightRecordCursorFactory.class);
            assertQueryBreaches(sql);
        });
    }

    @Test
    public void testSpliceJoinOpenFailureReleasesAllocations() throws Exception {
        // Inflate the join-key map far above the limit so SpliceJoinLight's of() breaches on
        // joinKeyMap.reopen() with isOpen already set. A small input keeps every other
        // allocation under the limit; reusing one factory across opens catches the isOpen desync.
        setProperty(PropertyKey.CAIRO_SQL_SMALL_MAP_KEY_CAPACITY, 50_000);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m AS (SELECT x AS k, (x * 1_000_000L)::timestamp ts FROM long_sequence(20)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE s AS (SELECT x AS k, (x * 1_000_000L)::timestamp ts FROM long_sequence(20)) TIMESTAMP(ts) PARTITION BY DAY");
            drainWalQueue();
            assertOpenFailureReleasesAllocations(
                    "SELECT * FROM m SPLICE JOIN s ON k",
                    SpliceJoinLightRecordCursorFactory.class
            );
        });
    }

    private void assertFullFatOpenFailureReleasesAllocations(String sql, Class<?> factoryClass) throws Exception {
        // setFullFatJoins forces the full-fat outer-join factory (the same path the optimizer picks for a
        // non-random-access slave). The query is configured to breach during the cursor's of() (the
        // inflated join-key map crosses the per-query limit on the first reopen). Reusing one factory
        // across opens catches a failed open that does not free the cursor: the stuck isOpen flag would
        // let a later open skip reopen() and stop breaching, tripping the Assert.fail below.
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            compiler.setFullFatJoins(true);
            try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                Assert.assertTrue(
                        "expected " + factoryClass.getSimpleName() + ", got: " + factory.getClass().getSimpleName(),
                        isFactoryInChain(factory, factoryClass)
                );
                for (int i = 0; i < 5; i++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.fail("expected a per-query memory breach during cursor open at iteration " + i);
                    } catch (CairoException e) {
                        Assert.assertTrue("expected isOutOfMemory(), got: " + e.getFlyweightMessage(), e.isOutOfMemory());
                        TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                        TestUtils.assertContains(e.getFlyweightMessage(), "workload=QUERY");
                    }
                }
            }
        }
    }

    private void assertOpenFailureReleasesAllocations(String sql, Class<?> factoryClass) throws Exception {
        // The query is configured to breach during the join cursor's of() (an inflated
        // structure crosses the per-query limit on the first reopen). Reusing one factory
        // across opens catches a failed-open that does not free the cursor: the orphaned
        // tracker-bound map and the stuck isOpen flag would let a later open skip reopen()
        // and stop breaching, tripping the Assert.fail below.
        assertUsesFactory(sql, factoryClass);
        try (SqlCompiler compiler = engine.getSqlCompiler();
             RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
            for (int i = 0; i < 5; i++) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.fail("expected a per-query memory breach during cursor open at iteration " + i);
                } catch (CairoException e) {
                    Assert.assertTrue("expected isOutOfMemory(), got: " + e.getFlyweightMessage(), e.isOutOfMemory());
                    TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                    TestUtils.assertContains(e.getFlyweightMessage(), "workload=QUERY");
                }
            }
        }
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
            if (!isFactoryInChain(factory, factoryClass)) {
                Assert.fail("expected " + factoryClass.getSimpleName() + " in base chain of " + factory.getClass().getSimpleName());
            }
        }
    }

    private void assertUsesMarkoutHorizon(String sql) throws Exception {
        // MarkoutHorizonRecordCursorFactory is wrapped by the projection, so check the plan text
        // (as MarkoutHorizonCrossJoinTest does) rather than walking the base-factory chain.
        try (RecordCursorFactory factory = select("EXPLAIN " + sql);
             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            sink.clear();
            CursorPrinter.println(cursor, factory.getMetadata(), sink);
            TestUtils.assertContains(sink, "Markout Horizon Join");
        }
    }

    private boolean isFactoryInChain(RecordCursorFactory factory, Class<?> factoryClass) {
        RecordCursorFactory cur = factory;
        while (cur != null) {
            if (factoryClass.isInstance(cur)) {
                return true;
            }
            RecordCursorFactory next = cur.getBaseFactory();
            if (next == cur) {
                break;
            }
            cur = next;
        }
        return false;
    }
}
