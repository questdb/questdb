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

import io.questdb.DefaultFactoryProvider;
import io.questdb.FactoryProvider;
import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.sql.async.AdaptiveWorkStealingStrategy;
import io.questdb.cairo.sql.async.WorkStealingStrategy;
import io.questdb.cairo.sql.async.WorkStealingStrategyFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.table.AsyncHashJoinGroupByAtom;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.context;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.fused;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.plan;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.result;

/**
 * Covers a fused hash join GROUP BY over the life of one factory and its cursors:
 * <ul>
 *     <li>writes that land between a cursor's open and its first read, a window that the lazy
 *     build widens, since the build runs on the first read;</li>
 *     <li>executions of one factory that build on the owner and in rounds, or merge with and
 *     without shards, as the tables change between them;</li>
 *     <li>the adaptive work stealing strategy deciding inside the build's rounds;</li>
 *     <li>a real pool of a single worker.</li>
 * </ul>
 * Every result is compared with the ordinary plan's.
 */
public class HashJoinGroupByLifecycleTest extends AbstractCairoTest {
    // b holds fewer rows than p, so every INNER join builds b, as LEFT and RIGHT joins do.
    private static final String[] SHAPES = {
            "SELECT p.g, count() c, sum(b.w) w FROM p JOIN b ON p.k = b.k GROUP BY p.g ORDER BY p.g",
            // The lone count of an INNER join, which counts the pairs from each key's row count.
            "SELECT count() FROM p JOIN b ON p.k = b.k",
            "SELECT p.g, count() c, count(b.w) m, sum(b.w) w FROM p LEFT JOIN b ON p.k = b.k GROUP BY p.g ORDER BY p.g",
            // The planner swaps the inputs of a RIGHT join and runs it as a LEFT one.
            "SELECT p.g, count() c, sum(b.w) w FROM b RIGHT JOIN p ON p.k = b.k GROUP BY p.g ORDER BY p.g",
            // A SYMBOL key, which the probe translates into the build's dictionary.
            "SELECT p.s, b.s bs, count() c, sum(b.w) w FROM p LEFT JOIN b ON p.s = b.s GROUP BY p.s, b.s ORDER BY p.s, bs",
            // A composite key, which stages into the build's map, with a SYMBOL pair inside it.
            "SELECT count() c, sum(b.w) w, sum(p.v) v FROM p JOIN b ON p.k = b.k AND p.s = b.s"
    };
    // Keyed by p.g alone, so the group count follows p.g's distinct values.
    private static final String[] SHARDING_SHAPES = {
            "SELECT p.g, count() c, sum(b.w) w FROM p JOIN b ON p.k = b.k GROUP BY p.g ORDER BY p.g",
            "SELECT p.g, count() c, count(b.w) m FROM p LEFT JOIN b ON p.k = b.k GROUP BY p.g ORDER BY p.g",
            "SELECT p.g, count() c, count(b.w) m FROM p LEFT JOIN b ON p.s = b.s GROUP BY p.g ORDER BY p.g"
    };

    // The adaptive strategy, which the default factory picks for 4 workers once the stealing
    // threshold is 1, waits on the workers that took a round's tasks rather than stealing them.
    // The owner of a build in rounds consults it whenever the reduce queue of 4 slots is full.
    @Test
    public void testAdaptiveWorkStealingDecidesInsideBuildRoundsWithLegacyWorkers() throws Exception {
        assertAdaptiveWorkStealingInsideBuildRounds(WorkerPoolMode.LEGACY);
    }

    @Test
    public void testAdaptiveWorkStealingDecidesInsideBuildRoundsWithoutWorkers() throws Exception {
        assertAdaptiveWorkStealingInsideBuildRounds(null);
    }

    @Test
    public void testDropPartitionBetweenOpenAndFirstReadKeepsTheSnapshot() throws Exception {
        assertWritesBetweenOpenAndFirstRead(
                false,
                null,
                SHAPES,
                "ALTER TABLE b DROP PARTITION LIST '2020-01-02'",
                "ALTER TABLE p DROP PARTITION LIST '2020-01-02'"
        );
    }

    @Test
    public void testInsertBetweenOpenAndFirstReadKeepsTheSnapshot() throws Exception {
        // The build gets an out-of-order row, which rewrites a partition the open cursor reads, and
        // texts new to its dictionary. The probe gets k45, a text only the build held so far, and a
        // text neither dictionary holds. The ordinary plan loses k23's matches on the probe's write,
        // see testInsertIntoProbeBetweenOpenAndFirstReadKeepsOrdinarySymbolMatches(), so the fused
        // cursor is compared with the result before the write.
        assertWritesBetweenOpenAndFirstRead(
                false,
                null,
                SHAPES,
                "INSERT INTO b VALUES (1, 'k1', 1000.0, '2020-01-02T12'), (45, 'n45', 2000.0, '2020-01-04'), (NULL, NULL, 3000.0, '2020-01-04T01')",
                "INSERT INTO p VALUES (45, 'k45', 6, 1.5, '2020-01-02T12'), (46, 'n46', 0, 2.5, '2020-01-04'), (3, 'k3', 1, 3.5, '2020-01-04T01')"
        );
    }

    // The fused probe looks p's texts up in b's dictionary, which the write grows.
    @Ignore("Known bug #7837: an open reader's SYMBOL lookup misses existing texts after a concurrent append")
    @Test
    public void testInsertIntoBuildBetweenOpenAndFirstReadKeepsSymbolMatches() throws Exception {
        assertSymbolMatchesSurviveInsertBetweenOpenAndFirstRead(true, "INSERT INTO b VALUES (45, 'k45', 1.0, '2020-01-04')");
    }

    // The ordinary join translates its SYMBOL key through p's dictionary, which the write grows. The
    // fused plan passes this case, see testInsertBetweenOpenAndFirstReadKeepsTheSnapshot().
    @Ignore("Known bug #7837: an open reader's SYMBOL lookup misses existing texts after a concurrent append")
    @Test
    public void testInsertIntoProbeBetweenOpenAndFirstReadKeepsOrdinarySymbolMatches() throws Exception {
        assertSymbolMatchesSurviveInsertBetweenOpenAndFirstRead(false, "INSERT INTO p VALUES (45, 'k45', 6, 1.5, '2020-01-04')");
    }

    @Test
    public void testOneFactoryBuildsOnTheOwnerAndInRoundsWithFiberWorkers() throws Exception {
        assertBuildSwitchesBetweenOwnerAndRounds(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testOneFactoryBuildsOnTheOwnerAndInRoundsWithLegacyWorkers() throws Exception {
        assertBuildSwitchesBetweenOwnerAndRounds(WorkerPoolMode.LEGACY);
    }

    @Test
    public void testOneFactoryBuildsOnTheOwnerAndInRoundsWithoutWorkers() throws Exception {
        assertBuildSwitchesBetweenOwnerAndRounds(null);
    }

    @Test
    public void testOneFactoryMergesWithAndWithoutShardsWithFiberWorkers() throws Exception {
        assertMergeSwitchesBetweenShardedAndUnsharded(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testOneFactoryMergesWithAndWithoutShardsWithLegacyWorkers() throws Exception {
        assertMergeSwitchesBetweenShardedAndUnsharded(WorkerPoolMode.LEGACY);
    }

    @Test
    public void testOneFactoryMergesWithAndWithoutShardsWithoutWorkers() throws Exception {
        assertMergeSwitchesBetweenShardedAndUnsharded(null);
    }

    @Test
    public void testSingleFiberWorkerPool() throws Exception {
        assertSingleWorkerPool(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testSingleLegacyWorkerPool() throws Exception {
        assertSingleWorkerPool(WorkerPoolMode.LEGACY);
    }

    @Test
    public void testTruncateIsRefusedBetweenOpenAndFirstRead() throws Exception {
        // The open cursor holds readers of both tables from its open on, although it builds only on
        // its first read, so a non-WAL table refuses the TRUNCATE as it does for the ordinary plan.
        assertWritesBetweenOpenAndFirstRead(false, "there is an active query against", SHAPES, "TRUNCATE TABLE b", "TRUNCATE TABLE p");
    }

    @Test
    public void testUpdateBetweenOpenAndFirstReadKeepsTheSnapshot() throws Exception {
        // An UPDATE writes new versions of the columns that the open cursor reads.
        assertWritesBetweenOpenAndFirstRead(
                false,
                null,
                SHAPES,
                "UPDATE b SET w = w + 1000 WHERE k < 10",
                "UPDATE p SET g = g + 10 WHERE k < 10"
        );
    }

    @Test
    public void testWalApplyBetweenOpenAndFirstReadKeepsTheSnapshot() throws Exception {
        // The WAL apply job runs each write while the cursor holds the readers. Three shapes cover the
        // INT layout, the SYMBOL key's translation and the staged composite key.
        assertWritesBetweenOpenAndFirstRead(
                true,
                null,
                new String[]{SHAPES[0], SHAPES[4], SHAPES[5]},
                "INSERT INTO b VALUES (1, 'k1', 1000.0, '2020-01-02T12'), (45, 'n45', 2000.0, '2020-01-04'), (NULL, NULL, 3000.0, '2020-01-04T01')",
                "INSERT INTO p VALUES (45, 'k45', 6, 1.5, '2020-01-02T12'), (46, 'n46', 0, 2.5, '2020-01-04'), (3, 'k3', 1, 3.5, '2020-01-04T01')",
                "ALTER TABLE b DROP PARTITION LIST '2020-01-02'",
                "ALTER TABLE p DROP PARTITION LIST '2020-01-02'",
                "UPDATE b SET w = w + 1000 WHERE k < 10",
                "TRUNCATE TABLE b",
                "TRUNCATE TABLE p"
        );
    }

    private static void assertMatchesOrdinaryPlan(String label, CairoEngine db, String sql, SqlExecutionContextImpl context, String actual) throws Exception {
        context.setParallelHashJoinGroupByEnabled(false);
        try (RecordCursorFactory baseline = db.select(sql, context)) {
            Assert.assertFalse(label, plan(baseline, context).contains("Hash Join Group By"));
            Assert.assertEquals(label, result(baseline, context), actual);
        } finally {
            context.setParallelHashJoinGroupByEnabled(true);
        }
    }

    private static void createTables(CairoEngine db, SqlExecutionContext context, boolean isWal, int buildRows) throws SqlException {
        db.execute("DROP TABLE IF EXISTS p", context);
        db.execute("DROP TABLE IF EXISTS b", context);
        final String walClause = isWal ? " WAL" : " BYPASS WAL";
        // p probes with 100 rows a day over three days, of 40 keys, and b builds with 40 rows a day,
        // of 50 keys, so ten build keys find no probe row.
        db.execute("CREATE TABLE p (k INT, s SYMBOL, g INT, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY" + walClause, context);
        db.execute("CREATE TABLE b (k INT, s SYMBOL, w DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY" + walClause, context);
        db.execute("""
                INSERT INTO p
                SELECT (x % 40)::INT, ('k' || (x % 40))::SYMBOL, (x % 7)::INT, x * 0.5, timestamp_sequence('2020-01-01', 864_000_000)
                FROM long_sequence(300)""", context);
        db.execute("INSERT INTO b SELECT (x % 50)::INT, ('k' || (x % 50))::SYMBOL, x::DOUBLE, timestamp_sequence('2020-01-01', 2_160_000_000) "
                + "FROM long_sequence(" + buildRows + ")", context);
    }

    private static String read(RecordCursor cursor, RecordMetadata metadata) {
        final StringSink sink = new StringSink();
        try {
            CursorPrinter.println(cursor, metadata, sink, true, true);
            return sink.toString();
        } catch (CairoException e) {
            return "error: " + e.getFlyweightMessage();
        }
    }

    // Runs the factory once more, or, when the build table outgrew what its plan allows, compiles
    // the query again, as every holder of a compiled factory does on that exception.
    private static String readAgain(CairoEngine db, String sql, RecordCursorFactory factory, SqlExecutionContextImpl context) throws Exception {
        try {
            return result(factory, context);
        } catch (TableReferenceOutOfDateException e) {
            try (RecordCursorFactory recompiled = db.select(sql, context)) {
                return result(recompiled, context);
            }
        } catch (CairoException e) {
            return "error: " + e.getFlyweightMessage();
        }
    }

    private void assertAdaptiveWorkStealingInsideBuildRounds(@Nullable WorkerPoolMode mode) throws Exception {
        HashJoinBuildMode.PARALLEL.applyProperties(node1.getConfigurationOverrides());
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WORK_STEALING_THRESHOLD, 1);
        setProperty(PropertyKey.CAIRO_UNORDERED_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 4);
        final AtomicInteger roundDecisions = new AtomicInteger();
        final FactoryProvider previousFactoryProvider = factoryProvider;
        factoryProvider = new DefaultFactoryProvider() {
            @Override
            public @NotNull WorkStealingStrategy getWorkStealingStrategy(
                    @NotNull CairoConfiguration configuration,
                    int workerCount,
                    @NotNull StatefulAtom atom
            ) {
                final WorkStealingStrategy strategy = super.getWorkStealingStrategy(configuration, workerCount, atom);
                if (!(atom instanceof AsyncHashJoinGroupByAtom fusedAtom)) {
                    return strategy;
                }
                Assert.assertTrue(strategy instanceof AdaptiveWorkStealingStrategy);
                // Counts the decisions taken while the open cursor's build is not yet published,
                // which only the build's rounds take.
                return new AdaptiveWorkStealingStrategy(
                        configuration.getSqlParallelWorkStealingThreshold(),
                        configuration.getSqlParallelWorkStealingSpinTimeout()
                ) {
                    @Override
                    public boolean shouldSteal(int finishedCount) {
                        if (fusedAtom.getFrozenBuild() == null) {
                            roundDecisions.incrementAndGet();
                        }
                        return super.shouldSteal(finishedCount);
                    }
                };
            }
        };
        try {
            assertMemoryLeak(() -> {
                final TestWorkerPool pool = mode == null ? null : new TestWorkerPool(4, mode);
                TestUtils.execute(pool, (db, compiler, ignored) -> {
                    Assert.assertTrue(WorkStealingStrategyFactory.getInstance(configuration, 4) instanceof AdaptiveWorkStealingStrategy);
                    for (String sql : SHAPES) {
                        createTables(db, ignored, false, 120);
                        try (SqlExecutionContextImpl context = context(db, 4)) {
                            // Frames of up to 3 rows give the build's rounds dozens of tasks each.
                            context.changePageFrameSizes(2, 3);
                            final int decisionsBefore = roundDecisions.get();
                            try (RecordCursorFactory factory = db.select(sql, context)) {
                                final String label = sql + "\n" + plan(factory, context);
                                Assert.assertTrue(label, label.contains("Hash Join Group By"));
                                final AsyncHashJoinGroupByAtom atom = fused(factory).getAtom();
                                for (int run = 0; run < 2; run++) {
                                    final String actual;
                                    try (RecordCursor cursor = factory.getCursor(context)) {
                                        actual = read(cursor, factory.getMetadata());
                                        Assert.assertTrue(label, atom.isBuiltInRounds());
                                    }
                                    assertMatchesOrdinaryPlan(label, db, sql, context, actual);
                                    Assert.assertEquals(label, 0, atom.getPerWorkerLocks().getAcquiredSlotCount());
                                }
                            }
                            // Without workers, the queue fills after its fourth task, and with them,
                            // long before the owner publishes a round's last one.
                            if (mode == null) {
                                Assert.assertTrue(sql, roundDecisions.get() > decisionsBefore);
                            }
                            Assert.assertNull(sql, context.getMemoryTracker());
                        }
                    }
                    Assert.assertTrue(roundDecisions.get() > 0);
                }, configuration, LOG);
            });
        } finally {
            factoryProvider = previousFactoryProvider;
        }
    }

    // One factory executes as its build table grows past the parallel build's row threshold and
    // shrinks below it again, never past twice the probe's rows, which would recompile it.
    private void assertBuildSwitchesBetweenOwnerAndRounds(@Nullable WorkerPoolMode mode) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 100);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 1);
        final String[] steps = {
                null,
                // 120 build rows
                "INSERT INTO b SELECT (x % 50)::INT, ('n' || (x % 50))::SYMBOL, x::DOUBLE, timestamp_sequence('2020-01-02T12', 2_160_000_000) FROM long_sequence(60)",
                // 80 build rows
                "ALTER TABLE b DROP PARTITION LIST '2020-01-01'",
                // 120 build rows
                "INSERT INTO b SELECT (x % 50)::INT, ('k' || (x % 50))::SYMBOL, x::DOUBLE, timestamp_sequence('2020-01-04', 2_160_000_000) FROM long_sequence(40)"
        };
        final boolean[] isBuiltInRounds = {false, true, false, true};
        assertMemoryLeak(() -> {
            final TestWorkerPool pool = mode == null ? null : new TestWorkerPool(4, mode);
            TestUtils.execute(pool, (db, compiler, ignored) -> {
                for (String sql : SHAPES) {
                    createTables(db, ignored, false, 60);
                    try (SqlExecutionContextImpl context = context(db, 4)) {
                        context.changePageFrameSizes(2, 3);
                        try (RecordCursorFactory factory = db.select(sql, context)) {
                            final String label = sql + "\n" + plan(factory, context);
                            Assert.assertTrue(label, label.contains("Hash Join Group By"));
                            final AsyncHashJoinGroupByAtom atom = fused(factory).getAtom();
                            for (int step = 0; step < steps.length; step++) {
                                if (steps[step] != null) {
                                    db.execute(steps[step], context);
                                }
                                final String stepLabel = label + "\nstep " + step;
                                final String actual;
                                try (RecordCursor cursor = factory.getCursor(context)) {
                                    actual = read(cursor, factory.getMetadata());
                                    Assert.assertEquals(stepLabel, isBuiltInRounds[step], atom.isBuiltInRounds());
                                    Assert.assertEquals(stepLabel, isBuiltInRounds[step], atom.getBuildPartitionCount() > 1);
                                }
                                assertMatchesOrdinaryPlan(stepLabel, db, sql, context, actual);
                                Assert.assertEquals(stepLabel, 0, atom.getPerWorkerLocks().getAcquiredSlotCount());
                            }
                        }
                        Assert.assertNull(sql, context.getMemoryTracker());
                    }
                }
            }, configuration, LOG);
        });
    }

    // One factory executes as its probe's group count grows past the sharding threshold and
    // shrinks below it again. The run after a sharded one shards from its start, on the hint the
    // sharded run left, and the run after that merges without shards again.
    private void assertMergeSwitchesBetweenShardedAndUnsharded(@Nullable WorkerPoolMode mode) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 20);
        final String[] steps = {
                null,
                // 150 more groups, in a day partition of their own.
                "INSERT INTO p SELECT (x % 40)::INT, ('k' || (x % 40))::SYMBOL, (100 + x % 150)::INT, x * 0.5, "
                        + "timestamp_sequence('2020-01-05', 400_000_000) FROM long_sequence(200)",
                "ALTER TABLE p DROP PARTITION LIST '2020-01-05'",
                null
        };
        final boolean[] isSharded = {false, true, true, false};
        assertMemoryLeak(() -> {
            final TestWorkerPool pool = mode == null ? null : new TestWorkerPool(4, mode);
            TestUtils.execute(pool, (db, compiler, ignored) -> {
                for (String sql : SHARDING_SHAPES) {
                    createTables(db, ignored, false, 120);
                    try (SqlExecutionContextImpl context = context(db, 4)) {
                        context.changePageFrameSizes(2, 3);
                        try (RecordCursorFactory factory = db.select(sql, context)) {
                            final String label = sql + "\n" + plan(factory, context);
                            Assert.assertTrue(label, label.contains("Hash Join Group By"));
                            final AsyncHashJoinGroupByAtom atom = fused(factory).getAtom();
                            for (int step = 0; step < steps.length; step++) {
                                if (steps[step] != null) {
                                    db.execute(steps[step], context);
                                }
                                final String stepLabel = label + "\nstep " + step;
                                final String actual;
                                try (RecordCursor cursor = factory.getCursor(context)) {
                                    actual = read(cursor, factory.getMetadata());
                                    Assert.assertEquals(stepLabel, isSharded[step], atom.isSharded());
                                }
                                assertMatchesOrdinaryPlan(stepLabel, db, sql, context, actual);
                                Assert.assertEquals(stepLabel, 0, atom.getPerWorkerLocks().getAcquiredSlotCount());
                            }
                        }
                        Assert.assertNull(sql, context.getMemoryTracker());
                    }
                }
            }, configuration, LOG);
        });
    }

    // A pool of one worker gives the operator one worker slot next to the owner's, the fewest that
    // a parallel build's partitions and a sharded merge spread over.
    private void assertSingleWorkerPool(WorkerPoolMode mode) throws Exception {
        assertMemoryLeak(() -> {
            final TestWorkerPool pool = new TestWorkerPool(1, mode);
            TestUtils.execute(pool, (db, compiler, ignored) -> {
                createTables(db, ignored, false, 120);
                for (HashJoinBuildMode buildMode : HashJoinBuildMode.values()) {
                    buildMode.applyProperties(node1.getConfigurationOverrides());
                    for (int threshold : new int[]{Integer.MAX_VALUE, 1}) {
                        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, threshold);
                        for (String sql : SHAPES) {
                            try (SqlExecutionContextImpl context = context(db, 1)) {
                                context.changePageFrameSizes(2, 3);
                                try (RecordCursorFactory factory = db.select(sql, context)) {
                                    final String label = buildMode + ", threshold " + threshold + ": " + sql + "\n" + plan(factory, context);
                                    Assert.assertTrue(label, label.contains("Hash Join Group By"));
                                    final AsyncHashJoinGroupByAtom atom = fused(factory).getAtom();
                                    for (int run = 0; run < 2; run++) {
                                        final String actual;
                                        try (RecordCursor cursor = factory.getCursor(context)) {
                                            actual = read(cursor, factory.getMetadata());
                                            Assert.assertEquals(label, buildMode == HashJoinBuildMode.PARALLEL, atom.isBuiltInRounds());
                                            // Only a keyed query merges maps, and every keyed shape here has
                                            // more than one group.
                                            Assert.assertEquals(label, threshold == 1 && sql.contains("GROUP BY"), atom.isSharded());
                                        }
                                        assertMatchesOrdinaryPlan(label, db, sql, context, actual);
                                        Assert.assertEquals(label, 0, atom.getPerWorkerLocks().getAcquiredSlotCount());
                                    }
                                }
                                Assert.assertNull(sql, context.getMemoryTracker());
                            }
                        }
                    }
                }
            }, configuration, LOG);
        });
    }

    /**
     * Opens a cursor of the given plan over tables whose build holds k0 to k39, inserts a row with
     * k45 into one of them, then reads the cursor. k45 lands in another bucket of the SYMBOL column's
     * index than k23, yet the open reader's lookup of k23 finds nothing once the writer has added it;
     * see PR7618_BUGS_HANDOFF.md. The cursor must still pair p's k23 rows with b's.
     */
    private void assertSymbolMatchesSurviveInsertBetweenOpenAndFirstRead(boolean isFused, String write) throws Exception {
        assertMemoryLeak(() -> {
            createTables(engine, sqlExecutionContext, false, 120);
            execute("DROP TABLE b");
            execute("CREATE TABLE b (k INT, s SYMBOL, w DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO b SELECT (x % 40)::INT, ('k' || (x % 40))::SYMBOL, x::DOUBLE, timestamp_sequence('2020-01-01', 2_160_000_000) FROM long_sequence(120)");
            final String sql = "SELECT p.s, b.s bs, count() c, sum(b.w) w FROM p LEFT JOIN b ON p.s = b.s GROUP BY p.s, b.s ORDER BY p.s, bs";
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.setParallelHashJoinGroupByEnabled(isFused);
                try (RecordCursorFactory factory = engine.select(sql, context)) {
                    Assert.assertEquals(isFused, plan(factory, context).contains("Hash Join Group By"));
                    final String expected = result(factory, context);
                    Assert.assertTrue(expected, expected.contains("k23:SYMBOL\tk23:SYMBOL\t21:LONG\t1323.0:DOUBLE"));
                    final String actual;
                    try (RecordCursor cursor = factory.getCursor(context)) {
                        execute(write);
                        actual = read(cursor, factory.getMetadata());
                    }
                    Assert.assertEquals(expected, actual);
                }
            }
        });
    }

    /**
     * Opens a fused cursor, runs the write, then reads the cursor. The cursor holds its tables as
     * they were when it opened, although it builds only on its first read, so it returns what the
     * ordinary plan returned just before the write, or, when the open cursor makes the write fail
     * with the given refusal, as nothing happened. The factory's next execution then reads the
     * tables as the write left them.
     */
    private void assertWritesBetweenOpenAndFirstRead(boolean isWal, @Nullable String refusal, String[] shapes, String... writes) throws Exception {
        assertMemoryLeak(() -> {
            for (HashJoinBuildMode buildMode : HashJoinBuildMode.values()) {
                buildMode.applyProperties(node1.getConfigurationOverrides());
                for (String write : writes) {
                    for (String sql : shapes) {
                        createTables(engine, sqlExecutionContext, isWal, 120);
                        if (isWal) {
                            drainWalQueue();
                        }
                        try (SqlExecutionContextImpl context = context(engine, 4)) {
                            context.changePageFrameSizes(2, 3);
                            final String label = buildMode + ": " + write + "\n" + sql;
                            context.setParallelHashJoinGroupByEnabled(false);
                            final String expected;
                            try (RecordCursorFactory ordinary = engine.select(sql, context)) {
                                expected = result(ordinary, context);
                            } finally {
                                context.setParallelHashJoinGroupByEnabled(true);
                            }
                            try (RecordCursorFactory factory = engine.select(sql, context)) {
                                Assert.assertTrue(label, plan(factory, context).contains("Hash Join Group By"));
                                final AsyncHashJoinGroupByAtom atom = fused(factory).getAtom();
                                final String actual;
                                try (RecordCursor cursor = factory.getCursor(context)) {
                                    // The open cursor has built nothing yet.
                                    Assert.assertNull(label, atom.getFrozenBuild());
                                    if (refusal == null) {
                                        execute(write, sqlExecutionContext);
                                        if (isWal) {
                                            drainWalQueue();
                                        }
                                    } else {
                                        try {
                                            execute(write, sqlExecutionContext);
                                            Assert.fail(label);
                                        } catch (SqlException | CairoException e) {
                                            TestUtils.assertContains(e.getMessage(), refusal);
                                        }
                                    }
                                    actual = read(cursor, factory.getMetadata());
                                    Assert.assertEquals(label, buildMode == HashJoinBuildMode.PARALLEL, atom.isBuiltInRounds());
                                }
                                Assert.assertEquals(label, expected, actual);
                                Assert.assertEquals(label, 0, atom.getPerWorkerLocks().getAcquiredSlotCount());
                                assertMatchesOrdinaryPlan(label + "\nafter the write", engine, sql, context,
                                        readAgain(engine, sql, factory, context));
                            }
                            Assert.assertNull(label, context.getMemoryTracker());
                        }
                    }
                }
            }
        });
    }
}
