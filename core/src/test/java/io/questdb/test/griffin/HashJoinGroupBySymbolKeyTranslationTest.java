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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.table.AsyncHashJoinGroupByAtom;
import io.questdb.griffin.engine.table.AsyncHashJoinGroupByRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.std.MemoryTracker;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.test.griffin.AsyncHashJoinSymbolKeyTranslationTest.*;
import static io.questdb.test.griffin.ParallelHashJoinTestUtils.plan;
import static io.questdb.test.griffin.ParallelHashJoinTestUtils.result;

/**
 * The SYMBOL key translation of the fused GROUP BY over a hash join, by the rows that each
 * execution's probe reads, all of its frame rows: a probe of at least one row per 256 texts of the
 * probe column's dictionary translates through one cache that every slot shares, of four bytes per
 * text, and any other through a lazy cache of each slot's own. The tables are those of
 * {@link AsyncHashJoinSymbolKeyTranslationTest}: f.sym holds 300,000 texts, so the shared cache opens
 * for 1,172 probe rows or more. Every query compares its rows with the plan without the fused GROUP
 * BY and without the parallel hash join, and asserts the mode and the shared cache's size while the
 * cursor is open.
 */
public class HashJoinGroupBySymbolKeyTranslationTest extends AbstractCairoTest {
    private static final int WORKER_COUNT = 4;

    @Override
    @Before
    public void setUp() {
        super.setUp();
        sqlExecutionContext.setParallelGroupByEnabled(true);
        sqlExecutionContext.setParallelHashJoinGroupByEnabled(true);
    }

    @After
    public void restoreContext() {
        sqlExecutionContext.restoreToDefaultPageFrameSizes();
        sqlExecutionContext.setJitMode(configuration.getSqlJitMode());
    }

    @Test
    public void testCachedFactoryAcrossInsertTruncateAndAlter() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String sql = "SELECT d.w % 10 g, count() c, count(d.w) n FROM f LEFT JOIN d ON f.sym = d.sym" + SELECTIVE + " ORDER BY g";
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinGroupByRecordCursorFactory fused = findFused(factory);
                Assert.assertNotNull(fused);
                assertExecution(factory, fused, nonFused(sql, sqlExecutionContext), HashJoinGroupBySymbolKeyTranslationTest::assertSlotCached);
                // 2,000 more March rows: 3,000 >= 1,172, so the same factory opens the shared cache.
                execute("INSERT INTO f SELECT 'k' || x, 's3_' || (x % 50), (x % 100)::INT, x, timestamp_sequence('2024-03-01T01', 1_000_000) FROM long_sequence(2_000)");
                assertExecution(factory, fused, nonFused(sql, sqlExecutionContext), HashJoinGroupBySymbolKeyTranslationTest::assertSharedCache);
                // TRUNCATE drops the texts, which come back in another order, under other keys.
                execute("TRUNCATE TABLE f");
                execute(marchRows("f", "2024-03-01"));
                execute("INSERT INTO f SELECT 'k' || (299_948 - x), 's3_' || (x % 50), (x % 100)::INT, x, timestamp_sequence('2024-01-01', 1_000_000) FROM long_sequence(299_947)");
                assertExecution(factory, fused, nonFused(sql, sqlExecutionContext), HashJoinGroupBySymbolKeyTranslationTest::assertSlotCached);
            }
            // A column added after the rows changes the table's structure, so the query compiles anew.
            execute("ALTER TABLE f ADD COLUMN z INT");
            assertTranslation(sql, HashJoinGroupBySymbolKeyTranslationTest::assertSlotCached);
        });
    }

    @Test
    public void testCancellationAtEachCheckWithSlotCaches() throws Exception {
        assertMemoryLeak(() -> TestUtils.execute(new TestWorkerPool(WORKER_COUNT, WorkerPoolMode.LEGACY), (db, compiler, ignore) -> {
            try (SqlExecutionContextImpl context = newContext(db)) {
                AsyncHashJoinSymbolKeyTranslationTest.createTables(db, context);
                context.changePageFrameSizes(64, 64);
                final String sql = "SELECT count() c, sum(d.w) s, count(d.w) n FROM f LEFT JOIN d ON f.sym = d.sym" + SELECTIVE;
                final String expected = nonFused(sql, context);
                try (RecordCursorFactory factory = db.select(sql, context)) {
                    final AsyncHashJoinGroupByRecordCursorFactory fused = findFused(factory);
                    Assert.assertNotNull(fused);
                    assertExecution(factory, fused, expected, HashJoinGroupBySymbolKeyTranslationTest::assertSlotCached, context);
                    final SqlExecutionCircuitBreaker previous = context.getCircuitBreaker();
                    final TrippingCircuitBreaker counting = new TrippingCircuitBreaker(previous, Long.MAX_VALUE);
                    context.with(counting);
                    try {
                        Assert.assertEquals(expected, result(factory, context));
                    } finally {
                        context.with(previous);
                    }
                    final long checks = counting.checks.get();
                    Assert.assertTrue("checks: " + checks, checks > 2);
                    int cancellations = 0;
                    // The cancellations of an open cursor, whose tracker tells what the execution left.
                    int trackedCancellations = 0;
                    for (long tripAt = 1; tripAt <= checks; tripAt++) {
                        final TrippingCircuitBreaker breaker = new TrippingCircuitBreaker(previous, tripAt);
                        context.with(breaker);
                        // Stays null when the check cancels the cursor's opening, before any slot translates.
                        MemoryTracker tracker = null;
                        try (RecordCursor cursor = factory.getCursor(context)) {
                            tracker = context.getMemoryTracker();
                            Assert.assertNotNull(tracker);
                            //noinspection StatementWithEmptyBody
                            while (cursor.hasNext()) {
                            }
                            // The owner's checks vary with the workers' pace, so an execution may end before
                            // the check.
                            Assert.assertTrue("ended past check " + tripAt, breaker.checks.get() < tripAt);
                        } catch (CairoException e) {
                            Assert.assertTrue(e.getMessage(), e.isCancellation());
                            cancellations++;
                            if (tracker != null) {
                                trackedCancellations++;
                            }
                        } finally {
                            context.with(previous);
                        }
                        // The closed cursor released every byte that its execution charged, the caches that
                        // the slots grew before the cancellation included.
                        if (tracker != null) {
                            Assert.assertEquals("tracked after check " + tripAt, 0, tracker.getUsed());
                        }
                        Assert.assertNull(fused.getAtom().getFrozenBuild());
                        Assert.assertEquals(0, fused.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                        Assert.assertEquals(0, db.getBusyReaderCount());
                    }
                    Assert.assertTrue("cancellations: " + cancellations, cancellations > 2);
                    Assert.assertTrue("tracked cancellations: " + trackedCancellations, trackedCancellations > 0);
                    Assert.assertEquals(expected, result(factory, context));
                }
            }
        }, configuration, LOG));
    }

    @Test
    public void testDataShapes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // No probe row: an INNER join over a build with rows probes, and translates through the
            // slots, which meet no key.
            for (String join : new String[]{" JOIN ", " LEFT JOIN "}) {
                assertTranslation("SELECT count() c, sum(d.w) s FROM f" + join + "d ON f.sym = d.sym WHERE f.ts IN '2024-02-01'",
                        HashJoinGroupBySymbolKeyTranslationTest::assertSlotCached);
            }
            // One probe row, and the 10 rows of a day whose keys are all NULL, which match d's NULL row.
            assertTranslation("SELECT d.w, count() c FROM f JOIN d ON f.sym = d.sym WHERE f.ts = '2024-03-01T00:02:00' ORDER BY d.w",
                    HashJoinGroupBySymbolKeyTranslationTest::assertSlotCached);
            execute("INSERT INTO f SELECT NULL, 's3_' || (x % 50), (x % 100)::INT, x, timestamp_sequence('2024-02-15', 1_000_000) FROM long_sequence(10)");
            final String nullRows = assertTranslation("SELECT d.w, count() c FROM f JOIN d ON f.sym = d.sym WHERE f.ts IN '2024-02-15' ORDER BY d.w",
                    HashJoinGroupBySymbolKeyTranslationTest::assertSlotCached);
            // d's NULL key is its row 904.
            TestUtils.assertContains(nullRows, "904:LONG\t10:LONG");
            // A probe column of NULLs alone has an empty dictionary, which the shared cache covers with
            // no byte. A LEFT join probes it whatever its size, and its NULL keys match d's NULL row.
            execute("CREATE TABLE fnull AS (SELECT NULL::SYMBOL sym, x v, timestamp_sequence('2024-01-01', 1_000_000) ts FROM long_sequence(100)) TIMESTAMP(ts) PARTITION BY DAY");
            TestUtils.assertContains(assertTranslation("SELECT count() c, sum(d.w) s FROM fnull LEFT JOIN d ON fnull.sym = d.sym WHERE fnull.ts IN '2024-01-01'",
                    fused -> assertSharedCache(fused, 0)), "100:LONG\t90400:LONG");
            // An INNER join whose build has no row probes nothing and binds nothing; a LEFT one translates
            // its probe's keys, all of which miss.
            assertTranslation("SELECT count() c, sum(d.w) s FROM f JOIN (SELECT * FROM d WHERE w < 0) d ON f.sym = d.sym" + SELECTIVE, fused -> {
                final AsyncHashJoinGroupByAtom atom = fused.getAtom();
                Assert.assertEquals(0, atom.getFrozenBuild().getRowCount());
                Assert.assertFalse(atom.isSymbolKeySlotCached(0));
                Assert.assertFalse(atom.isSymbolKeyCacheFilledFromBuild(0));
                Assert.assertEquals(0, atom.getBuildingStage().getSymbolKeyCacheSizeInBytes(0));
            });
            assertTranslation("SELECT count() c, count(d.w) n FROM f LEFT JOIN (SELECT * FROM d WHERE w < 0) d ON f.sym = d.sym" + SELECTIVE,
                    HashJoinGroupBySymbolKeyTranslationTest::assertSlotCached);
            // Frames of 64 rows, whose keys repeat across frames, keyed and scalar.
            sqlExecutionContext.changePageFrameSizes(64, 64);
            assertTranslation("SELECT f.sym, count() c FROM f JOIN d ON f.sym = d.sym" + SELECTIVE + " ORDER BY f.sym",
                    HashJoinGroupBySymbolKeyTranslationTest::assertSlotCached);
            assertTranslation("SELECT count() c, sum(f.v) s FROM f JOIN d ON f.sym = d.sym" + SELECTIVE,
                    HashJoinGroupBySymbolKeyTranslationTest::assertSlotCached);
            sqlExecutionContext.restoreToDefaultPageFrameSizes();
            // The slots' caches at a capacity of 4 and of 0 entries, over the March partition's 954 keys.
            for (int capacity : new int[]{4, 0}) {
                setProperty(PropertyKey.CAIRO_SQL_JOIN_SYMBOL_TRANSLATION_CACHE_CAPACITY, capacity);
                assertTranslation("SELECT f.sym, count() c, count(d.w) n FROM f LEFT JOIN d ON f.sym = d.sym" + SELECTIVE + " ORDER BY f.sym",
                        HashJoinGroupBySymbolKeyTranslationTest::assertSlotCached);
                assertTranslation("SELECT count() c, count(d.w) n FROM f LEFT JOIN d ON f.sym = d.sym" + SELECTIVE,
                        HashJoinGroupBySymbolKeyTranslationTest::assertSlotCached);
            }
        });
    }

    @Test
    public void testExecutionModes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // JIT on and off, under a probe filter whose frames still hold 1,000 rows.
            for (int jitMode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                sqlExecutionContext.setJitMode(jitMode);
                assertTranslation("SELECT d.w % 10 g, count() c FROM f JOIN d ON f.sym = d.sym" + SELECTIVE + " AND f.v % 3 = 0 ORDER BY g",
                        HashJoinGroupBySymbolKeyTranslationTest::assertSlotCached);
            }
            sqlExecutionContext.setJitMode(configuration.getSqlJitMode());
            // The owner's build, and the build in rounds, which shrinks the frames to a few rows.
            for (HashJoinBuildMode mode : HashJoinBuildMode.values()) {
                mode.apply(node1.getConfigurationOverrides(), sqlExecutionContext);
                assertTranslation("SELECT d.w % 10 g, count() c, count(d.w) n FROM f LEFT JOIN d ON f.sym = d.sym" + SELECTIVE + " ORDER BY g", fused -> {
                    assertSlotCached(fused);
                    Assert.assertEquals(mode == HashJoinBuildMode.PARALLEL, fused.getAtom().isBuiltInRounds());
                });
                sqlExecutionContext.restoreToDefaultPageFrameSizes();
            }
            // A Parquet March partition: a later row keeps it from being the last one.
            execute("INSERT INTO f VALUES ('k1', 's3_1', 1, 1, '2024-04-01')");
            execute("ALTER TABLE f CONVERT PARTITION TO PARQUET LIST '2024-03-01'");
            assertTranslation("SELECT f.sym, count() c, sum(d.w) s FROM f LEFT JOIN d ON f.sym = d.sym" + SELECTIVE + " ORDER BY f.sym",
                    HashJoinGroupBySymbolKeyTranslationTest::assertSlotCached);
        });
    }

    @Test
    public void testFullScanFillsTheSharedCacheFromTheBuild() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // 300,947 probe rows: the shared cache opens, filled from d's 903 texts.
            for (String join : new String[]{" JOIN ", " LEFT JOIN "}) {
                assertTranslation("SELECT d.w % 10 g, count() c FROM f" + join + "d ON f.sym = d.sym ORDER BY g", fused -> {
                    assertSharedCache(fused);
                    Assert.assertTrue(fused.getAtom().isSymbolKeyCacheFilledFromBuild(0));
                });
            }
        });
    }

    @Test
    public void testLayouts() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // A staged key with an INT column.
            assertTranslation("SELECT d.w % 10 g, count() c FROM f JOIN d ON f.sym = d.sym AND f.k = d.k" + SELECTIVE + " ORDER BY g",
                    HashJoinGroupBySymbolKeyTranslationTest::assertSlotCached);
            // Two SYMBOL pairs, which the planner may list in either order: f.sym's 300,000 texts
            // translate through the slots, and f.sym3's 50 through the shared cache.
            assertTranslation("SELECT d.w % 10 g, count() c, count(d.w) n FROM f LEFT JOIN d ON f.sym = d.sym AND f.sym3 = d.sym3" + SELECTIVE + " ORDER BY g", fused -> {
                final AsyncHashJoinGroupByAtom atom = fused.getAtom();
                final int slotCachedKey = atom.isSymbolKeySlotCached(0) ? 0 : 1;
                Assert.assertTrue(atom.isSymbolKeySlotCached(slotCachedKey));
                Assert.assertEquals(0, atom.getBuildingStage().getSymbolKeyCacheSizeInBytes(slotCachedKey));
                Assert.assertFalse(atom.isSymbolKeySlotCached(1 - slotCachedKey));
                Assert.assertEquals(4L * 50, atom.getBuildingStage().getSymbolKeyCacheSizeInBytes(1 - slotCachedKey));
            });
        });
    }

    @Test
    public void testLimitOfTheGroupsTakesNoReach() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // A LIMIT applies to the groups, after every probe row: the shared cache opens for f's
            // 300,947 rows. The join under the same LIMIT reads one row first, and translates through
            // the slots.
            assertTranslation("SELECT d.w % 10 g, count() c FROM f JOIN d ON f.sym = d.sym ORDER BY g LIMIT 1",
                    HashJoinGroupBySymbolKeyTranslationTest::assertSharedCache);
            final boolean wasJoin = sqlExecutionContext.isParallelHashJoinEnabled();
            sqlExecutionContext.setParallelHashJoinEnabled(true);
            try (RecordCursorFactory factory = select("SELECT f.ts, d.w FROM f JOIN d ON f.sym = d.sym LIMIT 1")) {
                final AsyncHashJoinRecordCursorFactory join = ParallelHashJoinTestUtils.find(factory);
                Assert.assertNotNull(join);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertTrue(join.getAtom().getStage(0).isSymbolKeySlotCached(0));
                }
            } finally {
                sqlExecutionContext.setParallelHashJoinEnabled(wasJoin);
            }
        });
    }

    @Test
    public void testSelectiveGroupByTranslatesThroughSlotCaches() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // Keyed by the build's and by the probe's columns, scalar, and a count alone, INNER and LEFT.
            for (String join : new String[]{" JOIN ", " LEFT JOIN "}) {
                for (String sql : new String[]{
                        "SELECT d.w % 10 g, count() c, sum(f.v) s FROM f" + join + "d ON f.sym = d.sym" + SELECTIVE + " ORDER BY g",
                        "SELECT f.sym, count() c, count(d.w) n FROM f" + join + "d ON f.sym = d.sym" + SELECTIVE + " ORDER BY f.sym",
                        "SELECT count() c, sum(d.w) s, count(d.w) n FROM f" + join + "d ON f.sym = d.sym" + SELECTIVE,
                        "SELECT count() FROM f" + join + "d ON f.sym = d.sym" + SELECTIVE,
                }) {
                    assertTranslation(sql, HashJoinGroupBySymbolKeyTranslationTest::assertSlotCached);
                }
            }
            // The count over the INNER join: 1,092 pairs, see AsyncHashJoinSymbolKeyTranslationTest.
            TestUtils.assertContains(nonFused("SELECT count() FROM f JOIN d ON f.sym = d.sym" + SELECTIVE, sqlExecutionContext), "1092");
        });
    }

    @Test
    public void testSelectiveGroupByTracksLessThanTheSharedCache() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String sql = "SELECT f.sym, count() c, sum(d.w) s FROM f JOIN d ON f.sym = d.sym" + SELECTIVE + " ORDER BY f.sym";
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinGroupByRecordCursorFactory fused = findFused(factory);
                Assert.assertNotNull(fused);
                for (int run = 0; run < 2; run++) {
                    final MemoryTracker tracker;
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(cursor.hasNext());
                        tracker = sqlExecutionContext.getMemoryTracker();
                        Assert.assertNotNull(tracker);
                        // The build, the GROUP BY's maps and the owner's cache of the keys it met, a quarter
                        // of the 1,200,000 bytes that the shared cache alone takes, at most.
                        Assert.assertTrue("tracked " + tracker.getUsed(), tracker.getUsed() < SHARED_CACHE_SIZE / 4);
                        assertSlotCached(fused);
                    }
                    Assert.assertEquals(0, tracker.getUsed());
                }
            }
        });
    }

    @Test
    public void testSharedCacheOpensAtOneProbeRowPer256Symbols() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // ceil(300,000 / 256) = 1,172: an interval of 1,171 rows translates through the slots, and one
            // of 1,172 rows opens the shared cache.
            assertTranslation("SELECT d.w % 10 g, count() c FROM f JOIN d ON f.sym = d.sym WHERE f.ts >= '2024-01-01' AND f.ts < '2024-01-01T00:19:31' ORDER BY g",
                    HashJoinGroupBySymbolKeyTranslationTest::assertSlotCached);
            assertTranslation("SELECT d.w % 10 g, count() c FROM f JOIN d ON f.sym = d.sym WHERE f.ts >= '2024-01-01' AND f.ts < '2024-01-01T00:19:32' ORDER BY g",
                    HashJoinGroupBySymbolKeyTranslationTest::assertSharedCache);
        });
    }

    @Test
    public void testSqlContexts() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String groupBy = "SELECT d.w % 10 g, count() c FROM f JOIN d ON f.sym = d.sym" + SELECTIVE;
            assertTranslation("SELECT * FROM (" + groupBy + ") WHERE c > 50 ORDER BY g", HashJoinGroupBySymbolKeyTranslationTest::assertSlotCached);
            assertTranslation("WITH j AS (" + groupBy + ") SELECT g, c FROM j ORDER BY g", HashJoinGroupBySymbolKeyTranslationTest::assertSlotCached);
            assertTranslation(groupBy + " ORDER BY g LIMIT 3", HashJoinGroupBySymbolKeyTranslationTest::assertSlotCached);
            // Set operations, whose sides run the fused GROUP BY.
            final String other = "SELECT d.w % 10 g, count() c FROM f JOIN d ON f.sym = d.sym WHERE f.ts IN '2024-03-01T00:05'";
            for (String operation : new String[]{" UNION ", " UNION ALL ", " EXCEPT ", " INTERSECT "}) {
                final String sql = "SELECT * FROM ((" + groupBy + ")" + operation + "(" + other + ")) ORDER BY g, c";
                final String expected = nonFused(sql, sqlExecutionContext);
                try (RecordCursorFactory factory = select(sql)) {
                    TestUtils.assertContains(plan(factory, sqlExecutionContext), "Async Hash Join Group By");
                    Assert.assertEquals(sql, expected, result(factory, sqlExecutionContext));
                    Assert.assertEquals(sql, expected, result(factory, sqlExecutionContext));
                }
            }
            // EXPLAIN shows what it showed before.
            try (RecordCursorFactory factory = select(groupBy)) {
                final String plan = plan(factory, sqlExecutionContext);
                TestUtils.assertContains(plan, "Async Hash Join Group By");
                TestUtils.assertContains(plan, "symbolKeyJoin: true");
                TestUtils.assertContains(plan, "Interval forward scan on: f");
            }
        });
    }

    @Test
    public void testWorkerPoolsOnFiberWorkers() throws Exception {
        assertWorkerPools(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testWorkerPoolsOnThreadWorkers() throws Exception {
        assertWorkerPools(WorkerPoolMode.LEGACY);
    }

    private static void assertExecution(RecordCursorFactory factory, AsyncHashJoinGroupByRecordCursorFactory fused, String expected, GroupByTranslationCheck check) throws SqlException {
        assertExecution(factory, fused, expected, check, sqlExecutionContext);
    }

    // Checks the translation of an execution while its cursor is open, once it read every row, then
    // the rows of two more executions; every execution releases what it took.
    private static void assertExecution(
            RecordCursorFactory factory,
            AsyncHashJoinGroupByRecordCursorFactory fused,
            String expected,
            GroupByTranslationCheck check,
            SqlExecutionContext context
    ) throws SqlException {
        final MemoryTracker tracker;
        try (RecordCursor cursor = factory.getCursor(context)) {
            //noinspection StatementWithEmptyBody
            while (cursor.hasNext()) {
            }
            tracker = context.getMemoryTracker();
            Assert.assertNotNull(tracker);
            check.check(fused);
        }
        Assert.assertEquals(0, tracker.getUsed());
        Assert.assertEquals(0, fused.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
        Assert.assertEquals(expected, result(factory, context));
        Assert.assertEquals(expected, result(factory, context));
    }

    // The first SYMBOL key translates through the shared cache of f.sym's 300,000 texts.
    private static void assertSharedCache(AsyncHashJoinGroupByRecordCursorFactory fused) {
        assertSharedCache(fused, SHARED_CACHE_SIZE);
    }

    private static void assertSharedCache(AsyncHashJoinGroupByRecordCursorFactory fused, long size) {
        Assert.assertFalse(fused.getAtom().isSymbolKeySlotCached(0));
        Assert.assertEquals(size, fused.getAtom().getBuildingStage().getSymbolKeyCacheSizeInBytes(0));
    }

    // No shared cache: the slots translate the first SYMBOL key through caches of their own.
    private static void assertSlotCached(AsyncHashJoinGroupByRecordCursorFactory fused) {
        final AsyncHashJoinGroupByAtom atom = fused.getAtom();
        Assert.assertTrue(atom.isSymbolKeySlotCached(0));
        Assert.assertFalse(atom.isSymbolKeyCacheFilledFromBuild(0));
        Assert.assertEquals(0, atom.getBuildingStage().getSymbolKeyCacheSizeInBytes(0));
    }

    // Compiles the query with the fused GROUP BY, checks one execution, compares two more with the plan
    // without it, and returns that plan's result.
    private static String assertTranslation(String sql, GroupByTranslationCheck check) throws SqlException {
        final String expected = nonFused(sql, sqlExecutionContext);
        try (RecordCursorFactory factory = select(sql)) {
            final AsyncHashJoinGroupByRecordCursorFactory fused = findFused(factory);
            Assert.assertNotNull(sql + '\n' + plan(factory, sqlExecutionContext), fused);
            assertExecution(factory, fused, expected, check);
        }
        return expected;
    }

    // Twenty executions of keyed and scalar GROUP BYs of f's March rows in frames of 64 rows, on a pool
    // of four workers of the mode, whose slots each translate through a cache of their own.
    private static void assertWorkerPools(WorkerPoolMode mode) throws Exception {
        assertMemoryLeak(() -> TestUtils.execute(new TestWorkerPool(WORKER_COUNT, mode), (db, compiler, ignore) -> {
            try (SqlExecutionContextImpl context = newContext(db)) {
                AsyncHashJoinSymbolKeyTranslationTest.createTables(db, context);
                context.changePageFrameSizes(64, 64);
                for (String sql : new String[]{
                        "SELECT f.sym, count() c, sum(d.w) s FROM f LEFT JOIN d ON f.sym = d.sym WHERE f.ts IN '2024-03-01' ORDER BY f.sym",
                        "SELECT count() c, count(d.w) n, sum(f.v) s FROM f JOIN d ON f.sym = d.sym WHERE f.ts IN '2024-03-01'",
                }) {
                    final String expected = nonFused(sql, context);
                    try (RecordCursorFactory factory = db.select(sql, context)) {
                        final AsyncHashJoinGroupByRecordCursorFactory fused = findFused(factory);
                        Assert.assertNotNull(fused);
                        for (int execution = 0; execution < 20; execution++) {
                            assertExecution(factory, fused, expected, HashJoinGroupBySymbolKeyTranslationTest::assertSlotCached, context);
                        }
                    }
                }
            }
        }, configuration, LOG));
    }

    private static void createTables() throws SqlException {
        AsyncHashJoinSymbolKeyTranslationTest.createTables(engine, sqlExecutionContext);
    }

    @Nullable
    private static AsyncHashJoinGroupByRecordCursorFactory findFused(RecordCursorFactory factory) {
        for (RecordCursorFactory current = factory; current != null; current = current.getBaseFactory()) {
            if (current instanceof AsyncHashJoinGroupByRecordCursorFactory fused) {
                return fused;
            }
        }
        return null;
    }

    private static SqlExecutionContextImpl newContext(CairoEngine db) {
        final SqlExecutionContextImpl context = new SqlExecutionContextImpl(db, WORKER_COUNT).with(AllowAllSecurityContext.INSTANCE, bindVariableService, null, -1, null);
        context.setParallelGroupByEnabled(true);
        context.setParallelHashJoinGroupByEnabled(true);
        return context;
    }

    // The result of the query's plan without the fused GROUP BY and without the parallel hash join.
    private static String nonFused(String sql, SqlExecutionContext context) throws SqlException {
        final boolean wasFused = context.isParallelHashJoinGroupByEnabled();
        final boolean wasJoin = context.isParallelHashJoinEnabled();
        context.setParallelHashJoinGroupByEnabled(false);
        context.setParallelHashJoinEnabled(false);
        try (RecordCursorFactory factory = context.getCairoEngine().select(sql, context)) {
            Assert.assertNull(sql, findFused(factory));
            return result(factory, context);
        } finally {
            context.setParallelHashJoinGroupByEnabled(wasFused);
            context.setParallelHashJoinEnabled(wasJoin);
        }
    }

    @FunctionalInterface
    private interface GroupByTranslationCheck {
        void check(AsyncHashJoinGroupByRecordCursorFactory fused);
    }
}
