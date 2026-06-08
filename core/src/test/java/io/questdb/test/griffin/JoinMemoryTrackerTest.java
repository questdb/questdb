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
import io.questdb.griffin.engine.join.AsOfJoinDenseRecordCursorFactory;
import io.questdb.griffin.engine.join.AsOfJoinFastRecordCursorFactory;
import io.questdb.griffin.engine.join.AsOfJoinLightRecordCursorFactory;
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
    public void testAsOfJoinDenseOpenFailureReleasesAllocations() throws Exception {
        // asof_dense + multi-key routes to AsOfJoinDenseRecordCursorFactory, whose of() reopens two
        // tracker-bound SingleRecordSinks; a tiny limit breaches that reopen. The reuse loop asserts the
        // open-error path frees each cursor once and leaves the factory reusable (assertMemoryLeak guards leaks).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m AS (SELECT cast(x AS SYMBOL) k1, cast(x AS SYMBOL) k2, (x * 1_000_000L)::timestamp ts FROM long_sequence(4)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE s AS (SELECT cast(x AS SYMBOL) k1, cast(x AS SYMBOL) k2, (x * 1_000_000L)::timestamp ts FROM long_sequence(4)) TIMESTAMP(ts) PARTITION BY DAY");
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
            execute("CREATE TABLE m AS (SELECT cast(x AS SYMBOL) k1, cast(x AS SYMBOL) k2, (x * 1_000_000L)::timestamp ts FROM long_sequence(50)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE s AS (SELECT cast(x AS SYMBOL) k1, cast(x AS SYMBOL) k2, (x * 1_000_000L)::timestamp ts FROM long_sequence(50)) TIMESTAMP(ts) PARTITION BY DAY");
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
    public void testAsOfJoinFastOpenFailureReleasesAllocations() throws Exception {
        // A keyed ASOF join over a time-frame slave routes to AsOfJoinFastRecordCursorFactory, whose of()
        // reopens two tracker-bound SingleRecordSinks; a tiny limit breaches that reopen. The reuse loop
        // asserts the open-error path frees each cursor once and leaves the factory reusable.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m AS (SELECT cast(x AS SYMBOL) k, (x * 1_000_000L)::timestamp ts FROM long_sequence(4)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE s AS (SELECT cast(x AS SYMBOL) k, (x * 1_000_000L)::timestamp ts FROM long_sequence(4)) TIMESTAMP(ts) PARTITION BY DAY");
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
            execute("CREATE TABLE m AS (SELECT cast(x AS SYMBOL) k, (x * 1_000_000L)::timestamp ts FROM long_sequence(50)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE s AS (SELECT cast(x AS SYMBOL) k, (x * 1_000_000L)::timestamp ts FROM long_sequence(50)) TIMESTAMP(ts) PARTITION BY DAY");
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
            execute("CREATE TABLE m AS (SELECT cast(x AS SYMBOL) k, (x * 1_000_000L)::timestamp ts FROM long_sequence(100_000)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE s AS (SELECT cast(x AS SYMBOL) k, (x * 1_000_000L)::timestamp ts FROM long_sequence(100_000)) TIMESTAMP(ts) PARTITION BY DAY");
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
    public void testHashJoinLightOpenFailureReleasesAllocations() throws Exception {
        // Inflate the slave LongChain page above the limit so HashJoinLight's of() allocates
        // the join-key map (region 1) and then breaches on the slave chain reopen (region 2),
        // orphaning the map unless the failed open frees the cursor under the still-bound
        // tracker. A small input keeps the join-key map well under the limit.
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_LIGHT_VALUE_PAGE_SIZE, 2 * 1024 * 1024L);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m AS (SELECT cast(x AS SYMBOL) k FROM long_sequence(20))");
            execute("CREATE TABLE s AS (SELECT cast(x AS SYMBOL) k, x AS v FROM long_sequence(20))");
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
    public void testHashOuterJoinFilteredLightOpenFailureReleasesAllocations() throws Exception {
        // A FULL OUTER JOIN with a non-equi filter routes to the filtered light factory whose
        // full-outer cursor reopens a match-ids map (region 1) before the slave chain (region 2).
        // Inflating the slave LongChain page makes of() breach on the chain, orphaning the
        // match-ids map unless the failed open frees the cursor under the still-bound tracker.
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_LIGHT_VALUE_PAGE_SIZE, 2 * 1024 * 1024L);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m AS (SELECT cast(x AS SYMBOL) k, x AS v FROM long_sequence(20))");
            execute("CREATE TABLE s AS (SELECT cast(x AS SYMBOL) k, x AS v FROM long_sequence(20))");
            drainWalQueue();
            assertOpenFailureReleasesAllocations(
                    "SELECT * FROM m FULL OUTER JOIN s ON m.k = s.k AND m.v <> s.v",
                    HashOuterJoinFilteredLightRecordCursorFactory.class
            );
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
            execute("CREATE TABLE m AS (SELECT cast(x AS SYMBOL) k FROM long_sequence(100_000))");
            execute("CREATE TABLE s AS (SELECT cast(x AS SYMBOL) k, x AS v FROM long_sequence(100_000))");
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
            execute("CREATE TABLE m AS (SELECT cast(x AS SYMBOL) k FROM long_sequence(20))");
            execute("CREATE TABLE s AS (SELECT cast(x AS SYMBOL) k, x AS v FROM long_sequence(20))");
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
            execute("CREATE TABLE m AS (SELECT cast(x AS SYMBOL) k FROM long_sequence(50))");
            execute("CREATE TABLE s AS (SELECT cast(x AS SYMBOL) k, x AS v FROM long_sequence(50))");
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
            execute("CREATE TABLE m AS (SELECT cast(x AS SYMBOL) k FROM long_sequence(20))");
            execute("CREATE TABLE s AS (SELECT cast(x AS SYMBOL) k, x AS v FROM long_sequence(20))");
            drainWalQueue();
            assertSql("count\n20\n", "SELECT count(*) FROM (SELECT m.k, s.v FROM m LEFT JOIN s ON k)");
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
                        }
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
    public void testSpliceJoinFailsOnLargeInput() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m AS (SELECT cast(x AS SYMBOL) k, (x * 1_000_000L)::timestamp ts FROM long_sequence(100_000)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE s AS (SELECT cast(x AS SYMBOL) k, (x * 1_000_000L)::timestamp ts FROM long_sequence(100_000)) TIMESTAMP(ts) PARTITION BY DAY");
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
