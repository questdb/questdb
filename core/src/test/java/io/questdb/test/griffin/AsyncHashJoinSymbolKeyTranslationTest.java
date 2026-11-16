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
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.HashJoinBuildStage;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.std.MemoryTracker;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.CountingSqlExecutionCircuitBreaker;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.test.griffin.ParallelHashJoinTestUtils.*;

/**
 * The SYMBOL key translation of the parallel hash join, by the rows that each execution's probe
 * reads. A join whose probe reads at least one row per 256 texts of the probe column's dictionary
 * translates through one cache that every slot shares, of four bytes per text, which the execution
 * clears, or fills from the build; any other translates through a lazy cache of each slot's own, as
 * the ordinary plan's SymbolTranslatingRecord does, which costs nothing until the slot meets a key.
 * f.sym holds 300,000 texts, so the shared cache opens for 1,172 probe rows or more: the probe's frame
 * rows, or a LIMIT's rows when fewer. Every query compares its rows, in order, with the ordinary
 * plan's, and asserts each SYMBOL key's mode and the shared cache's size while the cursor is open.
 */
public class AsyncHashJoinSymbolKeyTranslationTest extends AbstractCairoTest {
    // f.sym's texts: k1 to k299947, and the 53 texts of the March rows that the January rows lack.
    static final int PROBE_KEY_COUNT = 300_000;
    // The selective probe: the 1,000 rows of f's March partition, 1,000 < ceil(300,000 / 256) = 1,172.
    static final String SELECTIVE = " WHERE f.ts IN '2024-03-01'";
    // The shared cache of f.sym: four bytes per text.
    static final long SHARED_CACHE_SIZE = 4L * PROBE_KEY_COUNT;
    private static final String EMOJI = "\uD83D\uDE00a";
    private static final String LONG_TEXT = "L".repeat(1_000);
    private static final int WORKER_COUNT = 4;

    @Override
    @Before
    public void setUp() {
        super.setUp();
        sqlExecutionContext.setParallelHashJoinEnabled(true);
    }

    @After
    public void restoreContext() {
        sqlExecutionContext.restoreToDefaultPageFrameSizes();
        sqlExecutionContext.setJitMode(configuration.getSqlJitMode());
    }

    @Test
    public void testBindVariablesPickTheModeOfEachExecution() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // One factory, whose LIMIT decides each execution's reach: 1,171 rows translate through the
            // slots, 1,172 open the shared cache, and 1,171 go back to the slots.
            final String limitSql = "SELECT f.ts, f.sym, d.w FROM f JOIN d ON f.sym = d.sym LIMIT $1";
            bindVariableService.clear();
            bindVariableService.setLong(0, 1_171);
            try (RecordCursorFactory factory = select(limitSql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                for (long limit : new long[]{1_171, 1_172, 1_171}) {
                    bindVariableService.setLong(0, limit);
                    final String expected = ordinary(limitSql, sqlExecutionContext);
                    Assert.assertEquals(limit, rowCount(expected));
                    assertExecution(factory, join, expected, limit < 1_172 ? AsyncHashJoinSymbolKeyTranslationTest::assertSlotCached : AsyncHashJoinSymbolKeyTranslationTest::assertSharedCache);
                }
            }
            // An interval of bind variables: the March partition's 1,000 rows, every row, then the March
            // partition again.
            final String intervalSql = "SELECT f.ts, f.sym, d.w FROM f JOIN d ON f.sym = d.sym WHERE f.ts BETWEEN $1 AND $2";
            final String[][] intervals = {
                    {"2024-03-01T00:00:00.000000Z", "2024-03-01T23:59:59.999999Z"},
                    {"2024-01-01T00:00:00.000000Z", "2024-03-02T00:00:00.000000Z"},
                    {"2024-03-01T00:00:00.000000Z", "2024-03-01T23:59:59.999999Z"},
            };
            bindVariableService.clear();
            bindVariableService.setTimestamp(0, MicrosFormatUtils.parseTimestamp(intervals[0][0]));
            bindVariableService.setTimestamp(1, MicrosFormatUtils.parseTimestamp(intervals[0][1]));
            try (RecordCursorFactory factory = select(intervalSql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                for (String[] interval : intervals) {
                    bindVariableService.setTimestamp(0, MicrosFormatUtils.parseTimestamp(interval[0]));
                    bindVariableService.setTimestamp(1, MicrosFormatUtils.parseTimestamp(interval[1]));
                    final boolean isMarch = interval[0].startsWith("2024-03");
                    assertExecution(factory, join, ordinary(intervalSql, sqlExecutionContext),
                            isMarch ? AsyncHashJoinSymbolKeyTranslationTest::assertSlotCached : AsyncHashJoinSymbolKeyTranslationTest::assertSharedCache);
                }
            }
            bindVariableService.clear();
        });
    }

    @Test
    public void testCachedFactoryAcrossInsertTruncateAndAlter() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String sql = "SELECT f.ts, f.sym, d.w FROM f LEFT JOIN d ON f.sym = d.sym" + SELECTIVE;
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                assertExecution(factory, join, ordinary(sql, sqlExecutionContext), AsyncHashJoinSymbolKeyTranslationTest::assertSlotCached);
                // 2,000 more March rows: 3,000 >= 1,172, so the same factory opens the shared cache.
                execute("INSERT INTO f SELECT 'k' || x, 's3_' || (x % 50), (x % 100)::INT, x, timestamp_sequence('2024-03-01T01', 1_000_000) FROM long_sequence(2_000)");
                assertExecution(factory, join, ordinary(sql, sqlExecutionContext), AsyncHashJoinSymbolKeyTranslationTest::assertSharedCache);
                // TRUNCATE drops the texts, which come back in another order, under other keys.
                execute("TRUNCATE TABLE f");
                execute(marchRows("f", "2024-03-01"));
                execute("INSERT INTO f SELECT 'k' || (299_948 - x), 's3_' || (x % 50), (x % 100)::INT, x, timestamp_sequence('2024-01-01', 1_000_000) FROM long_sequence(299_947)");
                assertProbeKeyCount("f", PROBE_KEY_COUNT);
                assertExecution(factory, join, ordinary(sql, sqlExecutionContext), AsyncHashJoinSymbolKeyTranslationTest::assertSlotCached);
            }
            // A column added after the rows changes the table's structure, so the query compiles anew.
            execute("ALTER TABLE f ADD COLUMN z INT");
            assertTranslation(sql, AsyncHashJoinSymbolKeyTranslationTest::assertSlotCached);
        });
    }

    @Test
    public void testCancellationAtEachCheckWithSlotCaches() throws Exception {
        assertMemoryLeak(() -> TestUtils.execute(new TestWorkerPool(WORKER_COUNT, WorkerPoolMode.LEGACY), (db, compiler, ignore) -> {
            try (SqlExecutionContextImpl context = newContext(db)) {
                createTables(db, context);
                context.changePageFrameSizes(64, 64);
                final String sql = "SELECT f.ts, f.sym, d.w FROM f LEFT JOIN d ON f.sym = d.sym" + SELECTIVE;
                final String expected = ordinary(sql, context);
                context.setParallelHashJoinEnabled(true);
                try (RecordCursorFactory factory = db.select(sql, context)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(join);
                    assertExecution(factory, join, expected, AsyncHashJoinSymbolKeyTranslationTest::assertSlotCached, context);
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
                    for (long tripAt = 1; tripAt <= checks; tripAt++) {
                        final TrippingCircuitBreaker breaker = new TrippingCircuitBreaker(previous, tripAt);
                        context.with(breaker);
                        try (RecordCursor cursor = factory.getCursor(context)) {
                            //noinspection StatementWithEmptyBody
                            while (cursor.hasNext()) {
                            }
                            // The owner's checks vary with the workers' pace, so an execution may end before
                            // the check.
                            Assert.assertTrue("ended past check " + tripAt, breaker.checks.get() < tripAt);
                        } catch (CairoException e) {
                            Assert.assertTrue(e.getMessage(), e.isCancellation());
                            cancellations++;
                        } finally {
                            context.with(previous);
                        }
                        Assert.assertNull(join.getAtom().getStage(0).getFrozenBuild());
                        Assert.assertEquals(0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                        Assert.assertEquals(0, join.getBuildRoundAtom().getPerWorkerLocks().getAcquiredSlotCount());
                        Assert.assertEquals(0, db.getBusyReaderCount());
                    }
                    Assert.assertTrue("cancellations: " + cancellations, cancellations > 2);
                    Assert.assertEquals(expected, result(factory, context));
                }
            }
        }, configuration, LOG));
    }

    @Test
    public void testCapacityAndContinuationsOnTheOwnersSlot() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // A cap of one tuple, which a task lifts to its frame's rows: the frames whose rows meet d's
            // repeated keys continue on the owner's slot, which translates the rest of their keys.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1);
            sqlExecutionContext.changePageFrameSizes(64, 64);
            final String sql = "SELECT f.ts, f.sym, d.w FROM f LEFT JOIN d ON f.sym = d.sym AND d.w > f.v - 1_000_000" + SELECTIVE;
            for (int capacity : new int[]{1_000_000, 4, 0}) {
                setProperty(PropertyKey.CAIRO_SQL_JOIN_SYMBOL_TRANSLATION_CACHE_CAPACITY, capacity);
                assertTranslation(sql, join -> {
                    assertSlotCached(join);
                    Assert.assertTrue("continuations: " + join.getAtom().getContinuationCount(), join.getAtom().getContinuationCount() > 0);
                    Assert.assertTrue("frames: " + join.getProbeFrameCount(), join.getProbeFrameCount() > 1);
                });
            }
        });
    }

    @Test
    public void testDataShapes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // No probe row: a reach of 0 translates through the slots, which meet no key.
            for (String join : new String[]{" JOIN ", " LEFT JOIN "}) {
                final String empty = "SELECT f.ts, f.sym, d.w FROM f" + join + "d ON f.sym = d.sym WHERE f.ts IN '2024-02-01'";
                Assert.assertEquals(0, rowCount(assertTranslation(empty, AsyncHashJoinSymbolKeyTranslationTest::assertSlotCached)));
            }
            // One probe row, k121's, which d holds once, and the 10 rows of a day whose keys are all NULL,
            // which match d's NULL row.
            Assert.assertEquals(1, rowCount(assertTranslation("SELECT f.ts, f.sym, d.w FROM f JOIN d ON f.sym = d.sym WHERE f.ts = '2024-03-01T00:02:00'",
                    AsyncHashJoinSymbolKeyTranslationTest::assertSlotCached)));
            execute("INSERT INTO f SELECT NULL, 's3_' || (x % 50), (x % 100)::INT, x, timestamp_sequence('2024-02-15', 1_000_000) FROM long_sequence(10)");
            Assert.assertEquals(10, rowCount(assertTranslation("SELECT f.ts, f.sym, d.w FROM f JOIN d ON f.sym = d.sym WHERE f.ts IN '2024-02-15'",
                    AsyncHashJoinSymbolKeyTranslationTest::assertSlotCached)));
            // A probe column of NULLs alone has an empty dictionary, which the shared cache covers with
            // no byte: ceil(0 / 256) = 0 rows reach it.
            execute("CREATE TABLE fnull AS (SELECT NULL::SYMBOL sym, x v, timestamp_sequence('2024-01-01', 1_000_000) ts FROM long_sequence(100)) TIMESTAMP(ts) PARTITION BY DAY");
            assertProbeKeyCount("fnull", 0);
            Assert.assertEquals(100, rowCount(assertTranslation("SELECT fnull.ts, fnull.v, d.w FROM fnull JOIN d ON fnull.sym = d.sym WHERE fnull.ts IN '2024-01-01'",
                    join -> assertSharedCache(join.getAtom().getStage(0), 0, 0))));
            // An INNER join whose build has no row returns none and binds nothing; a LEFT one translates
            // its probe's keys, all of which miss.
            final String noBuild = "SELECT f.ts, f.sym, d.w FROM f JOIN (SELECT * FROM d WHERE w < 0) d ON f.sym = d.sym" + SELECTIVE;
            Assert.assertEquals(0, rowCount(assertTranslation(noBuild, join -> {
                final HashJoinBuildStage stage = join.getAtom().getStage(0);
                Assert.assertEquals(0, stage.getFrozenBuild().getRowCount());
                Assert.assertFalse(stage.isSymbolKeySlotCached(0));
                Assert.assertFalse(stage.isSymbolKeyCacheFilledFromBuild(0));
                Assert.assertEquals(0, stage.getSymbolKeyCacheSizeInBytes(0));
            })));
            final String missingBuild = "SELECT f.ts, f.sym, d.w FROM f LEFT JOIN (SELECT * FROM d WHERE w < 0) d ON f.sym = d.sym" + SELECTIVE;
            Assert.assertEquals(1_000, rowCount(assertTranslation(missingBuild, AsyncHashJoinSymbolKeyTranslationTest::assertSlotCached)));
            // Frames of 64 rows, whose keys repeat across frames.
            sqlExecutionContext.changePageFrameSizes(64, 64);
            assertTranslation("SELECT f.ts, f.sym, d.w FROM f JOIN d ON f.sym = d.sym" + SELECTIVE, join -> {
                assertSlotCached(join);
                Assert.assertTrue("frames: " + join.getProbeFrameCount(), join.getProbeFrameCount() > 1);
            });
        });
    }

    @Test
    public void testExecutionModes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // JIT on and off, under a probe filter whose frames still hold 1,000 rows.
            for (int jitMode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                sqlExecutionContext.setJitMode(jitMode);
                assertTranslation("SELECT f.ts, f.sym, d.w FROM f JOIN d ON f.sym = d.sym" + SELECTIVE + " AND f.v % 3 = 0",
                        AsyncHashJoinSymbolKeyTranslationTest::assertSlotCached);
            }
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            // The owner's build, and builds in rounds, which shrink the frames to a few rows.
            for (HashJoinBuildMode mode : HashJoinBuildMode.values()) {
                mode.apply(node1.getConfigurationOverrides(), sqlExecutionContext);
                assertTranslation("SELECT f.ts, f.sym, d.w FROM f LEFT JOIN d ON f.sym = d.sym" + SELECTIVE,
                        AsyncHashJoinSymbolKeyTranslationTest::assertSlotCached);
                sqlExecutionContext.restoreToDefaultPageFrameSizes();
            }
            // A Parquet March partition: a later row keeps it from being the last one.
            execute("INSERT INTO f VALUES ('k1', 's3_1', 1, 1, '2024-04-01')");
            execute("ALTER TABLE f CONVERT PARTITION TO PARQUET LIST '2024-03-01'");
            assertTranslation("SELECT f.ts, f.sym, f.v, d.w FROM f LEFT JOIN d ON f.sym = d.sym" + SELECTIVE,
                    AsyncHashJoinSymbolKeyTranslationTest::assertSlotCached);
            // A probe key column added after the rows: the March rows hold no s2, which reads NULL and
            // matches d's NULL row, and 300,000 later rows give s2 a dictionary of 300,000 texts.
            execute("ALTER TABLE f ADD COLUMN s2 SYMBOL CAPACITY 524288");
            execute("INSERT INTO f (sym, s2, v, ts) SELECT 'k1', 'n' || x, x, timestamp_sequence('2024-05-01', 1_000_000) FROM long_sequence(300_000)");
            assertProbeKeyCount("f", 5, PROBE_KEY_COUNT);
            Assert.assertEquals(1_000, rowCount(assertTranslation("SELECT f.ts, f.sym, d.w FROM f JOIN d ON f.s2 = d.sym" + SELECTIVE,
                    AsyncHashJoinSymbolKeyTranslationTest::assertSlotCached)));
            // A WAL table.
            execute("CREATE TABLE fw (sym SYMBOL CAPACITY 524288, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO fw SELECT sym, v, ts FROM f WHERE ts < '2024-04-01'");
            drainWalQueue();
            assertProbeKeyCount("fw", 0, PROBE_KEY_COUNT);
            assertTranslation("SELECT fw.ts, fw.sym, d.w FROM fw JOIN d ON fw.sym = d.sym WHERE fw.ts IN '2024-03-01'",
                    AsyncHashJoinSymbolKeyTranslationTest::assertSlotCached);
            // A table without partitions, read under a LIMIT of 100 rows.
            execute("CREATE TABLE f2 AS (SELECT sym, v FROM f WHERE ts < '2024-04-01')");
            assertProbeKeyCount("f2", 0, PROBE_KEY_COUNT);
            assertTranslation("SELECT f2.sym, f2.v, d.w FROM f2 JOIN d ON f2.sym = d.sym LIMIT 100",
                    AsyncHashJoinSymbolKeyTranslationTest::assertSlotCached);
        });
    }

    @Test
    public void testFullScanFillsTheSharedCacheFromTheBuild() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // 300,947 probe rows: the shared cache opens, filled from d's 903 texts.
            for (String join : new String[]{" JOIN ", " LEFT JOIN "}) {
                assertTranslation("SELECT f.ts, f.sym, d.w FROM f" + join + "d ON f.sym = d.sym", join1 -> {
                    assertSharedCache(join1);
                    Assert.assertTrue(join1.getAtom().getStage(0).isSymbolKeyCacheFilledFromBuild(0));
                });
            }
        });
    }

    @Test
    public void testMaterializedViewRefreshOverASelectiveProbe() throws Exception {
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_MAT_VIEW_PARALLEL_SQL_ENABLED, "true");
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE fw (sym SYMBOL CAPACITY 524288, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO fw SELECT sym, v, ts FROM f");
            drainWalQueue();
            // first() keeps the GROUP BY off the fused hash join, so the refresh runs the join.
            final String view = "SELECT fw.ts, d.w, first(fw.v) fv, count() c FROM fw JOIN d ON fw.sym = d.sym SAMPLE BY 1h";
            execute("CREATE MATERIALIZED VIEW mv WITH BASE fw AS (" + view + ") PARTITION BY DAY");
            drainWalAndMatViewQueues();
            Assert.assertTrue(isOperatorCached("mv"));
            // One hour of 360 rows, which the next refresh reads alone: 360 < 1,172.
            execute("INSERT INTO fw SELECT 'k' || x, x, timestamp_sequence('2024-03-02T05', 10_000_000) FROM long_sequence(360)");
            drainWalAndMatViewQueues();
            Assert.assertTrue(isOperatorCached("mv"));
            assertProbeKeyCount("fw", 0, PROBE_KEY_COUNT);
            final StringSink actual = new StringSink();
            printSql("SELECT * FROM mv ORDER BY ts, w", actual);
            sqlExecutionContext.setParallelHashJoinEnabled(false);
            try {
                final StringSink expected = new StringSink();
                printSql("SELECT * FROM (" + view + ") ORDER BY ts, w", expected);
                TestUtils.assertEquals(expected, actual);
            } finally {
                sqlExecutionContext.setParallelHashJoinEnabled(true);
            }
        });
    }

    @Test
    public void testProbeDictionaryShapes() throws Exception {
        // Without auto-scaling, a column keeps its declared capacity, and f_small's 300,000 texts at a
        // capacity of 65,536, more than two per unit, make its dictionary ineligible for a fill from the
        // build.
        setProperty(PropertyKey.CAIRO_AUTO_SCALE_SYMBOL_CAPACITY, "false");
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE f_nocache (sym SYMBOL CAPACITY 524288 NOCACHE, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE f_small (sym SYMBOL CAPACITY 65536, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE f_index (sym SYMBOL CAPACITY 524288 INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            for (String table : new String[]{"f_nocache", "f_small", "f_index"}) {
                execute("INSERT INTO " + table + " SELECT sym, v, ts FROM f");
                assertProbeKeyCount(table, 0, PROBE_KEY_COUNT);
                for (String join : new String[]{" JOIN ", " LEFT JOIN "}) {
                    assertTranslation("SELECT p.ts, p.sym, d.w FROM " + table + " p" + join + "d ON p.sym = d.sym WHERE p.ts IN '2024-03-01'",
                            AsyncHashJoinSymbolKeyTranslationTest::assertSlotCached);
                }
            }
            // The full scan of the ineligible dictionary opens the shared cache unresolved.
            assertTranslation("SELECT p.ts, p.sym, d.w FROM f_small p JOIN d ON p.sym = d.sym", join -> {
                assertSharedCache(join);
                Assert.assertFalse(join.getAtom().getStage(0).isSymbolKeyCacheFilledFromBuild(0));
            });
        });
    }

    @Test
    public void testSelectiveProbeTracksLessThanTheSharedCache() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String sql = "SELECT f.ts, f.sym, d.w FROM f JOIN d ON f.sym = d.sym" + SELECTIVE;
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                for (int run = 0; run < 2; run++) {
                    final MemoryTracker tracker;
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(cursor.hasNext());
                        tracker = sqlExecutionContext.getMemoryTracker();
                        Assert.assertNotNull(tracker);
                        // The build, the ring, the payload copy and the owner's cache of the 954 keys it met,
                        // a quarter of the 1,200,000 bytes that the shared cache alone takes, at most.
                        Assert.assertTrue("tracked " + tracker.getUsed(), tracker.getUsed() < SHARED_CACHE_SIZE / 4);
                        assertSlotCached(join);
                    }
                    Assert.assertEquals(0, tracker.getUsed());
                }
            }
        });
    }

    @Test
    public void testSelectiveProbeTranslatesThroughSlotCaches() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // The March rows i = 0..953 hold k(i + 1) up to i = 899, then NULL, '', the 4-byte UTF-8 text,
            // the 1,000-character text and absent1 to absent50, and rows 954..999 hold k1 to k46 again. d
            // holds k1 to k900 once, k1 to k96 once more, '', the two long texts and a NULL key, so the
            // INNER join returns 96 * 2 + 804 + 4 rows for the first 954 rows and 46 * 2 for the rest, 1,092,
            // and the LEFT join adds the 50 absent rows.
            for (String join : new String[]{" JOIN ", " LEFT JOIN "}) {
                final String sql = "SELECT f.ts, f.sym, f.v, d.w, d.sym ds FROM f" + join + "d ON f.sym = d.sym" + SELECTIVE;
                final String rows = assertTranslation(sql, join1 -> {
                    assertSlotCached(join1);
                    // The stage has one SYMBOL key pair, and no second one to translate.
                    Assert.assertFalse(join1.getAtom().getStage(0).isSymbolKeySlotCached(1));
                });
                Assert.assertEquals(sql, join.contains("LEFT") ? 1_142 : 1_092, rowCount(rows));
            }
        });
    }

    @Test
    public void testSharedCacheOpensAtOneProbeRowPer256Symbols() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // ceil(300,000 / 256) = 1,172: a LIMIT, and an interval, of 1,171 rows translate through the
            // slots, and of 1,172 rows open the shared cache. The interval's rows hold k1 to k1171 or k1172,
            // and d holds k1 to k96 twice, so the LEFT join returns 96 more rows than the interval holds.
            for (int rows : new int[]{1_171, 1_172}) {
                final TranslationCheck check = rows < 1_172 ? AsyncHashJoinSymbolKeyTranslationTest::assertSlotCached : AsyncHashJoinSymbolKeyTranslationTest::assertSharedCache;
                Assert.assertEquals(rows, rowCount(assertTranslation("SELECT f.ts, f.sym, d.w FROM f JOIN d ON f.sym = d.sym LIMIT " + rows, check)));
                final String bound = rows == 1_171 ? "2024-01-01T00:19:31" : "2024-01-01T00:19:32";
                Assert.assertEquals(rows + 96, rowCount(assertTranslation("SELECT f.ts, f.sym, d.w FROM f LEFT JOIN d ON f.sym = d.sym WHERE f.ts >= '2024-01-01' AND f.ts < '" + bound + "'", check)));
            }
        });
    }

    @Test
    public void testSlotCacheCapacityIsReadPerExecution() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // LIMIT 1,171 translates through the slots, and f's January rows past k900 miss d, so the join
            // reads every January row, 299,947 keys, past its reach. The owner runs every task, on its own
            // slot, so the executions differ in the owner's cache alone.
            final String sql = "SELECT f.ts, f.sym, d.w FROM f JOIN d ON f.sym = d.sym LIMIT 1171";
            final String expected = ordinary(sql, sqlExecutionContext);
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                final int[] capacities = {1_000_000, 1_000, 4, 0};
                final long[] used = new long[capacities.length];
                for (int i = 0; i < capacities.length; i++) {
                    setProperty(PropertyKey.CAIRO_SQL_JOIN_SYMBOL_TRANSLATION_CACHE_CAPACITY, capacities[i]);
                    final int index = i;
                    assertExecution(factory, join, expected, join1 -> {
                        assertSlotCached(join1);
                        used[index] = sqlExecutionContext.getMemoryTracker().getUsed();
                    });
                }
                // At the default, the owner's cache keeps every key it met, at four bytes each at least.
                Assert.assertTrue("default: " + (used[0] - used[3]), used[0] - used[3] >= 4L * 299_947);
                // At 1,000 entries it stops allocating: at most 64 bytes per entry and the initial 2 KiB,
                // per slot, of which the context's worker and the owner make two.
                Assert.assertTrue("1,000: " + (used[1] - used[3]), used[1] - used[3] > 0 && used[1] - used[3] <= 2 * (64L * 1_000 + 2_048));
                // At 4 entries, the page table and the page of the first keys, and at 0, nothing.
                Assert.assertEquals(256 + 1_024, used[2] - used[3]);
            }
        });
    }

    @Test
    public void testSqlContexts() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String join = "SELECT f.ts, f.sym, f.v, d.w FROM f JOIN d ON f.sym = d.sym" + SELECTIVE;
            assertTranslation("SELECT * FROM (" + join + ") WHERE w > 100", AsyncHashJoinSymbolKeyTranslationTest::assertSlotCached);
            assertTranslation("WITH j AS (" + join + ") SELECT ts, sym, w FROM j", AsyncHashJoinSymbolKeyTranslationTest::assertSlotCached);
            assertTranslation("SELECT f.ts, f.sym, d.w FROM f LEFT JOIN d ON f.sym = d.sym" + SELECTIVE + " ORDER BY f.ts DESC LIMIT 5",
                    AsyncHashJoinSymbolKeyTranslationTest::assertSlotCached);
            assertTranslation(join + " LIMIT -5", AsyncHashJoinSymbolKeyTranslationTest::assertSlotCached);
            assertTranslation("SELECT f.ts, d.w, row_number() OVER (PARTITION BY d.w) rn FROM f JOIN d ON f.sym = d.sym" + SELECTIVE,
                    AsyncHashJoinSymbolKeyTranslationTest::assertSlotCached);
            // A GROUP BY over the join, with the fused GROUP BY off.
            sqlExecutionContext.setParallelHashJoinGroupByEnabled(false);
            try {
                assertTranslation("SELECT d.w % 10 g, count() c FROM f JOIN d ON f.sym = d.sym" + SELECTIVE + " ORDER BY g",
                        AsyncHashJoinSymbolKeyTranslationTest::assertSlotCached);
            } finally {
                sqlExecutionContext.setParallelHashJoinGroupByEnabled(configuration.isSqlParallelHashJoinGroupByEnabled());
            }
            // Set operations, whose sides run the operator.
            final String other = "SELECT f.ts, f.sym, f.v, d.w FROM f JOIN d ON f.sym = d.sym WHERE f.ts IN '2024-03-01T00:05'";
            for (String operation : new String[]{" UNION ", " UNION ALL ", " EXCEPT ", " INTERSECT "}) {
                assertParallelHashJoin("(" + join + ")" + operation + "(" + other + ")", sqlExecutionContext);
            }
            // EXPLAIN shows what it showed before.
            try (RecordCursorFactory factory = select(join)) {
                final String plan = plan(factory, sqlExecutionContext);
                TestUtils.assertContains(plan, "symbolKeyJoin: true");
                TestUtils.assertContains(plan, "Interval forward scan on: f");
            }
            // A star of two SYMBOL stages: f.sym's 300,000 texts translate through the slots, and f.sym3's
            // 50 through the shared cache.
            execute("CREATE TABLE e AS (SELECT ('s3_' || x)::SYMBOL sym3, x u FROM long_sequence(25))");
            assertTranslation("SELECT f.ts, d.w, e.u FROM f JOIN d ON f.sym = d.sym JOIN e ON f.sym3 = e.sym3" + SELECTIVE, join1 -> {
                assertSlotCached(join1);
                assertSharedCache(join1.getAtom().getStage(1), 0, 4L * 50);
            });
            // A WHERE on the indexed probe column: one that the index serves reads the probe through the
            // index, which the ordinary plan keeps, and one that it does not filters the frames.
            execute("ALTER TABLE f ALTER COLUMN sym ADD INDEX");
            assertOrdinaryPlan("SELECT f.ts, f.sym, d.w FROM f JOIN d ON f.sym = d.sym" + SELECTIVE + " AND f.sym IN ('k5', 'k6')", sqlExecutionContext);
            assertTranslation("SELECT f.ts, f.sym, d.w FROM f JOIN d ON f.sym = d.sym" + SELECTIVE + " AND f.sym != 'k5'",
                    AsyncHashJoinSymbolKeyTranslationTest::assertSlotCached);
        });
    }

    @Test
    public void testSwappedExecutionsTranslateTheLeftInputsKeys() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // r holds 1,000 rows over a dictionary of 300,000 texts, l 10 rows: the INNER join of l and r
            // swaps, so the first stage builds l and probes r, and 1,000 < ceil(300,000 / 256) rows.
            createPaddedTable("r", 1_000);
            execute("CREATE TABLE l AS (SELECT * FROM f WHERE ts IN '2024-03-01' LIMIT 10)");
            assertTranslation("SELECT l.v, r.v rv, r.sym FROM l JOIN r ON l.sym = r.sym", join -> {
                Assert.assertTrue(join.getAtom().isSwapped());
                assertSlotCached(join);
            });
            // l2 holds 10 rows over a dictionary of 300,000 texts, and a later stage reads its keys, which a
            // swapped execution takes from the first stage's build rows: r2's 1,000 probe rows bound them.
            createPaddedTable("l2", 10);
            execute("CREATE TABLE r2 AS (SELECT x::INT k, x w2 FROM long_sequence(1_000))");
            assertTranslation("SELECT l2.v, r2.w2, d.w FROM l2 JOIN r2 ON l2.k = r2.k JOIN d ON l2.sym = d.sym", join -> {
                Assert.assertTrue(join.getAtom().isSwapped());
                assertSlotCached(join.getAtom().getStage(1), 0);
            });
        });
    }

    @Test
    public void testTypesAndLayouts() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // A staged key with an INT column.
            assertTranslation("SELECT f.ts, f.sym, f.k, d.w FROM f JOIN d ON f.sym = d.sym AND f.k = d.k" + SELECTIVE,
                    AsyncHashJoinSymbolKeyTranslationTest::assertSlotCached);
            // Two SYMBOL pairs of one stage, which the planner may list in either order: f.sym's 300,000
            // texts translate through the slots, and f.sym3's 50 through the shared cache.
            assertTranslation("SELECT f.ts, f.sym, f.sym3, d.w FROM f LEFT JOIN d ON f.sym = d.sym AND f.sym3 = d.sym3" + SELECTIVE, join -> {
                final HashJoinBuildStage stage = join.getAtom().getStage(0);
                final int slotCachedKey = stage.isSymbolKeySlotCached(0) ? 0 : 1;
                assertSlotCached(stage, slotCachedKey);
                assertSharedCache(stage, 1 - slotCachedKey, 4L * 50);
            });
            // SYMBOL against VARCHAR and STRING translates nothing.
            for (String column : new String[]{"sv", "ss"}) {
                final String plan = assertParallelHashJoin("SELECT f.ts, f.sym, d.w FROM f JOIN d ON f.sym = d." + column + SELECTIVE, sqlExecutionContext);
                Assert.assertFalse(plan, plan.contains("symbolKeyJoin"));
            }
        });
    }

    @Test
    public void testWorkerPoolsAndAConcurrentWriterOnFiberWorkers() throws Exception {
        assertWorkerPoolsAndAConcurrentWriter(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testWorkerPoolsAndAConcurrentWriterOnThreadWorkers() throws Exception {
        assertWorkerPoolsAndAConcurrentWriter(WorkerPoolMode.LEGACY);
    }

    // f.sym holds exactly 300,000 texts: k1 to k299947, one per January row, and the 53 texts of the
    // 1,000 March rows that the January rows lack. d holds 1,000 rows: k900 down to k1, '', the two long
    // texts, a NULL key and k1 to k96 once more, under keys of its own, and its texts as VARCHAR and
    // STRING too.
    static void createTables(CairoEngine db, SqlExecutionContext context) throws SqlException {
        db.execute("CREATE TABLE f (sym SYMBOL CAPACITY 524288, sym3 SYMBOL, k INT, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", context);
        db.execute("INSERT INTO f SELECT 'k' || x, 's3_' || (x % 50), (x % 100)::INT, x, timestamp_sequence('2024-01-01', 1_000_000) FROM long_sequence(299_947)", context);
        db.execute(marchRows("f", "2024-03-01"), context);
        db.execute("CREATE TABLE d (sym SYMBOL, sym3 SYMBOL, k INT, w LONG, sv VARCHAR, ss STRING)", context);
        db.execute("""
                INSERT INTO d SELECT s, 's3_' || (x %% 50), (x %% 100)::INT, x, s::VARCHAR, s::STRING FROM (
                    SELECT x, CASE WHEN x <= 900 THEN 'k' || (901 - x) WHEN x = 901 THEN '' WHEN x = 902 THEN '%s'
                        WHEN x = 903 THEN '%s' WHEN x = 904 THEN NULL ELSE 'k' || (x - 904) END s
                    FROM long_sequence(1_000)
                )
                """.formatted(EMOJI, LONG_TEXT), context);
        try (TableReader reader = db.getReader("f")) {
            Assert.assertEquals(PROBE_KEY_COUNT, reader.getSymbolMapReader(0).getSymbolCount());
            Assert.assertEquals(50, reader.getSymbolMapReader(1).getSymbolCount());
        }
    }

    // 1,000 rows of the day: row i = (x - 1) % 954 holds k(i + 1) for i < 900, then NULL, '', the 4-byte
    // UTF-8 text, the 1,000-character text and absent1 to absent50.
    static String marchRows(String table, String day) {
        return """
                INSERT INTO %s SELECT CASE WHEN i < 900 THEN 'k' || (i + 1) WHEN i = 900 THEN NULL WHEN i = 901 THEN ''
                    WHEN i = 902 THEN '%s' WHEN i = 903 THEN '%s' ELSE 'absent' || (i - 903) END,
                    's3_' || (x %% 50), (x %% 100)::INT, x, timestamp_sequence('%s', 1_000_000)
                FROM (SELECT x, (x - 1) %% 954 i FROM long_sequence(1_000))
                """.formatted(table, EMOJI, LONG_TEXT, day);
    }

    private static void assertExecution(RecordCursorFactory factory, AsyncHashJoinRecordCursorFactory join, String expected, TranslationCheck check) throws SqlException {
        assertExecution(factory, join, expected, check, sqlExecutionContext);
    }

    // Checks the translation of an execution while its cursor is open, once it read every row, then
    // the rows of two more executions; every execution releases what it took.
    private static void assertExecution(
            RecordCursorFactory factory,
            AsyncHashJoinRecordCursorFactory join,
            String expected,
            TranslationCheck check,
            SqlExecutionContext context
    ) throws SqlException {
        final MemoryTracker tracker;
        try (RecordCursor cursor = factory.getCursor(context)) {
            //noinspection StatementWithEmptyBody
            while (cursor.hasNext()) {
            }
            tracker = context.getMemoryTracker();
            Assert.assertNotNull(tracker);
            check.check(join);
        }
        Assert.assertEquals(0, tracker.getUsed());
        Assert.assertEquals(0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
        Assert.assertEquals(expected, result(factory, context));
        Assert.assertEquals(expected, result(factory, context));
    }

    private static void assertProbeKeyCount(String table, int keyCount) {
        assertProbeKeyCount(table, 0, keyCount);
    }

    private static void assertProbeKeyCount(String table, int column, int keyCount) {
        try (TableReader reader = getReader(table)) {
            Assert.assertEquals(keyCount, reader.getSymbolMapReader(column).getSymbolCount());
        }
    }

    // The first stage's first SYMBOL key translates through the shared cache of f.sym's 300,000 texts.
    private static void assertSharedCache(AsyncHashJoinRecordCursorFactory join) {
        assertSharedCache(join.getAtom().getStage(0), 0, SHARED_CACHE_SIZE);
    }

    // The shared cache is open, at four bytes per text of the probe dictionary.
    private static void assertSharedCache(HashJoinBuildStage stage, int key, long size) {
        Assert.assertFalse(stage.isSymbolKeySlotCached(key));
        Assert.assertEquals(size, stage.getSymbolKeyCacheSizeInBytes(key));
    }

    // The first stage's first SYMBOL key translates through the slots' caches.
    private static void assertSlotCached(AsyncHashJoinRecordCursorFactory join) {
        assertSlotCached(join.getAtom().getStage(0), 0);
    }

    // No shared cache: the slots translate through caches of their own.
    private static void assertSlotCached(HashJoinBuildStage stage, int key) {
        Assert.assertTrue(stage.isSymbolKeySlotCached(key));
        Assert.assertFalse(stage.isSymbolKeyCacheFilledFromBuild(key));
        Assert.assertEquals(0, stage.getSymbolKeyCacheSizeInBytes(key));
    }

    // Compiles the query with the operator on, checks one execution, compares two more with the
    // ordinary plan, and returns the ordinary plan's result.
    private static String assertTranslation(String sql, TranslationCheck check) throws SqlException {
        final String expected = ordinary(sql, sqlExecutionContext);
        try (RecordCursorFactory factory = select(sql)) {
            final AsyncHashJoinRecordCursorFactory join = find(factory);
            Assert.assertNotNull(sql, join);
            assertExecution(factory, join, expected, check);
        }
        return expected;
    }

    // Twenty executions of a LEFT join of f's March rows in frames of 64 rows, on a pool of four workers
    // of the mode, whose slots each translate through a cache of their own, then an execution during
    // which a writer commits new texts, and the executions after it.
    private static void assertWorkerPoolsAndAConcurrentWriter(WorkerPoolMode mode) throws Exception {
        assertMemoryLeak(() -> TestUtils.execute(new TestWorkerPool(WORKER_COUNT, mode), (db, compiler, ignore) -> {
            try (SqlExecutionContextImpl context = newContext(db)) {
                createTables(db, context);
                // Frames of 64 rows spread the March rows over the workers' slots, each of which
                // translates through a cache of its own; the slots move between threads and carriers.
                context.changePageFrameSizes(64, 64);
                final String sql = "SELECT f.ts, f.sym, d.w FROM f LEFT JOIN d ON f.sym = d.sym WHERE f.ts IN '2024-03'";
                final String expected = ordinary(sql, context);
                context.setParallelHashJoinEnabled(true);
                try (RecordCursorFactory factory = db.select(sql, context)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(join);
                    for (int execution = 0; execution < 20; execution++) {
                        assertExecution(factory, join, expected, AsyncHashJoinSymbolKeyTranslationTest::assertSlotCached, context);
                    }
                    // A writer commits 5,000 new texts, and rows of them, to a later March day while a
                    // cursor is open: the cursor reads the rows of its snapshot.
                    final String snapshot = printRows(sql, false, context);
                    final StringSink rows = new StringSink();
                    try (RecordCursor cursor = factory.getCursor(context)) {
                        final Record record = cursor.getRecord();
                        Assert.assertTrue(cursor.hasNext());
                        CursorPrinter.println(record, factory.getMetadata(), rows, true);
                        assertSlotCached(join);
                        db.execute("INSERT INTO f SELECT 'n' || x, 's3_' || (x % 50), (x % 100)::INT, x, timestamp_sequence('2024-03-05', 1_000_000) FROM long_sequence(5_000)", context);
                        while (cursor.hasNext()) {
                            CursorPrinter.println(record, factory.getMetadata(), rows, true);
                        }
                    }
                    TestUtils.assertEquals(snapshot, rows);
                    Assert.assertEquals(0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                    try (TableReader reader = db.getReader("f")) {
                        Assert.assertEquals(PROBE_KEY_COUNT + 5_000, reader.getSymbolMapReader(0).getSymbolCount());
                    }
                    // The next execution reads 6,000 rows over a dictionary of 305,000 texts, which open
                    // the shared cache: 6,000 >= ceil(305,000 / 256) = 1,192.
                    final String grown = ordinary(sql, context);
                    Assert.assertEquals(rowCount(expected) + 5_000, rowCount(grown));
                    assertExecution(factory, join, grown, join1 -> assertSharedCache(join1.getAtom().getStage(0), 0, 4L * (PROBE_KEY_COUNT + 5_000)), context);
                }
                // The March partition alone still translates through the slots: 1,000 < 1,192.
                final String march = "SELECT f.ts, f.sym, d.w FROM f LEFT JOIN d ON f.sym = d.sym WHERE f.ts IN '2024-03-01'";
                final String marchExpected = ordinary(march, context);
                context.setParallelHashJoinEnabled(true);
                try (RecordCursorFactory factory = db.select(march, context)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(join);
                    assertExecution(factory, join, marchExpected, AsyncHashJoinSymbolKeyTranslationTest::assertSlotCached, context);
                }
            }
        }, configuration, LOG));
    }

    // A table of f's columns that holds the first rows of f's March partition, and whose sym dictionary
    // also holds the 299,047 texts of a day that the table then dropped, which leaves its texts behind:
    // 300,000 texts for 1,000 rows, and 299,057 for 10.
    private static void createPaddedTable(String table, int rows) throws SqlException {
        execute("CREATE TABLE " + table + " (sym SYMBOL CAPACITY 524288, sym3 SYMBOL, k INT, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("INSERT INTO " + table + " SELECT 'pad' || x, 's3_0', 0, x, timestamp_sequence('2023-12-01', 1_000_000) FROM long_sequence(299_047)");
        execute(marchRows(table, "2024-03-01").replace("long_sequence(1_000)", "long_sequence(" + rows + ")"));
        execute("ALTER TABLE " + table + " DROP PARTITION WHERE ts < '2024-01-01'");
        assertProbeKeyCount(table, rows == 1_000 ? PROBE_KEY_COUNT : 299_047 + rows);
        try (TableReader reader = getReader(table)) {
            Assert.assertEquals(rows, reader.size());
        }
    }

    private static void createTables() throws SqlException {
        createTables(engine, sqlExecutionContext);
    }

    // Whether the factory that the last refresh of the view cached runs the operator.
    private static boolean isOperatorCached(String view) {
        final TableToken token = engine.verifyTableName(view);
        final MatViewState state = engine.getMatViewStateStore().getViewState(token);
        Assert.assertNotNull(state);
        Assert.assertTrue(state.tryLock());
        try (RecordCursorFactory factory = state.acquireRecordFactory()) {
            Assert.assertNotNull(factory);
            return find(factory) != null;
        } finally {
            state.unlock();
        }
    }

    private static SqlExecutionContextImpl newContext(CairoEngine db) {
        return new SqlExecutionContextImpl(db, WORKER_COUNT).with(AllowAllSecurityContext.INSTANCE, bindVariableService, null, -1, null);
    }

    // The rows of the query, as CursorPrinter prints them, under the operator or under the ordinary plan.
    private static String printRows(String sql, boolean isParallel, SqlExecutionContext context) throws SqlException {
        final boolean wasParallel = context.isParallelHashJoinEnabled();
        context.setParallelHashJoinEnabled(isParallel);
        try (RecordCursorFactory factory = context.getCairoEngine().select(sql, context); RecordCursor cursor = factory.getCursor(context)) {
            Assert.assertEquals(sql, isParallel, find(factory) != null);
            final StringSink rows = new StringSink();
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                CursorPrinter.println(record, factory.getMetadata(), rows, true);
            }
            return rows.toString();
        } finally {
            context.setParallelHashJoinEnabled(wasParallel);
        }
    }

    // The rows of a result: after its two lines of metadata, two lines per row, its values and the
    // values that its SYMBOL, STRING and VARCHAR columns read through other paths.
    private static long rowCount(CharSequence result) {
        long lines = 0;
        for (int i = 0, n = result.length(); i < n; i++) {
            if (result.charAt(i) == '\n') {
                lines++;
            }
        }
        return (lines - 2) / 2;
    }

    // Counts the stateful checks of the breaker, which throw on a trip, and cancels at the given one.
    static final class TrippingCircuitBreaker extends CountingSqlExecutionCircuitBreaker {
        final AtomicLong checks = new AtomicLong();
        private final long tripAt;

        TrippingCircuitBreaker(SqlExecutionCircuitBreaker delegate, long tripAt) {
            super(delegate);
            this.tripAt = tripAt;
        }

        @Override
        public void statefulThrowExceptionIfTripped() {
            super.statefulThrowExceptionIfTripped();
            onCheck();
        }

        @Override
        public void statefulThrowExceptionIfTrippedNoThrottle() {
            super.statefulThrowExceptionIfTrippedNoThrottle();
            onCheck();
        }

        @Override
        public void statefulThrowExceptionIfTrippedTimeThrottled() {
            super.statefulThrowExceptionIfTrippedTimeThrottled();
            onCheck();
        }

        private void onCheck() {
            if (checks.incrementAndGet() == tripAt) {
                throw CairoException.queryCancelled(1);
            }
        }
    }

    @FunctionalInterface
    private interface TranslationCheck {
        void check(AsyncHashJoinRecordCursorFactory join);
    }
}
