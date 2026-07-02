/*******************************************************************************
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

package io.questdb.test.cairo.lv;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewState;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.mp.Job;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.Files;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * SQL {@code CHECKPOINT CREATE} / restore (database backup) coverage for live views.
 * <p>
 * This exercises the operator-facing backup feature driven by {@code DatabaseCheckpointAgent}
 * end to end: create a live view with data, run the real {@code CHECKPOINT CREATE} statement,
 * simulate a restore by dropping the {@code _restore} trigger file and calling
 * {@code engine.checkpointRecover()} + re-hydration, then drive the refresh worker and assert
 * the restored view converges to a from-scratch recompute over the (restored) base table.
 * <p>
 * It is deliberately distinct from {@link LiveViewCheckpointTest}, which is a pure unit test of
 * the internal per-view head {@code .cp} file format and never runs a SQL statement or the
 * checkpoint agent.
 */
public class LiveViewCheckpointRestoreTest extends AbstractCairoTest {

    // > FLUSH EVERY 100ms, so a single driveRefreshToQuiescence pass crosses the flush window.
    private static final long CLOCK_ADVANCE_MICROS = 250_000;
    private static final String SNAPSHOT_ID = "test-checkpoint-instance";
    // Installed before the engine is built (setUpStatic) so DatabaseCheckpointAgent captures it. A pure
    // pass-through unless a test arms lvStateCopyHook; testCheckpointWhileBaseAdvancesConverges uses it to
    // land a deterministic base advance during the _lv.s copy.
    private static final LvCheckpointFilesFacade testFilesFacade = new LvCheckpointFilesFacade();
    private static Path checkpointPath;
    private static Path triggerFilePath;
    private int checkpointRootLen;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        checkpointPath = new Path();
        triggerFilePath = new Path();
        ff = testFilesFacade;
        AbstractCairoTest.setUpStatic();
    }

    @AfterClass
    public static void tearDownStatic() {
        checkpointPath = Misc.free(checkpointPath);
        triggerFilePath = Misc.free(triggerFilePath);
        AbstractCairoTest.tearDownStatic();
    }

    @Before
    public void setUp() {
        // CHECKPOINT relies on the sync() syscall, unavailable on Windows; skip the whole suite there.
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        super.setUp();
        ff = testFilesFacade;
        testFilesFacade.reset();
        checkpointPath.of(configuration.getCheckpointRoot()).concat(configuration.getDbDirectory()).slash();
        checkpointRootLen = checkpointPath.size();
        triggerFilePath.of(configuration.getDbRoot()).parent().concat(TableUtils.RESTORE_FROM_CHECKPOINT_TRIGGER_FILE_NAME).$();
        // Pin the clock below all (future-dated) test data: a non-BACKFILL view's lower bound is
        // the CREATE wall-clock moment and the refresh path drops rows below it.
        setCurrentMicros(0L);
        // Stable snapshot instance id so CHECKPOINT CREATE records it and restore reads the same value.
        setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, SNAPSHOT_ID);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        // Reset the checkpoint in-progress flag in case a test failed before its own RELEASE, and
        // wipe the checkpoint dir so it does not leak into the next test.
        execute("CHECKPOINT RELEASE");
        checkpointPath.trimTo(checkpointRootLen);
        configuration.getFilesFacade().rmdir(checkpointPath.slash());
    }

    @Test
    public void testCheckpointMidRefreshIsConsistent() throws Exception {
        // Checkpoint a view that has not yet caught up to the base: batch2 is applied to the base but
        // never refreshed into the view before CHECKPOINT CREATE, so the frozen view lags the base.
        // The freeze must capture a mutually consistent (persisted watermark, on-disk data) pair;
        // restore then resumes the refresh and converges over the full base. The concurrent
        // freeze-vs-running-worker race is covered by
        // LiveViewConcurrencyTest#testConcurrentCheckpointDuringRefresh; here the interest is the
        // restore consistency of a mid-catch-up view.
        final String viewSql = "SELECT ts, sym, x, sum(x) OVER (PARTITION BY sym ORDER BY ts " +
                "ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS s FROM base";
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " + viewSql);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-01-01T00:00:01.000000Z', 'a', 1.0), " +
                        "('2026-01-01T00:00:02.000000Z', 'b', 2.0)");
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(viewSql);

                // Apply batch2 to the base but do NOT refresh the view: it now lags the base, and the
                // pending commits are captured inside the checkpoint's own base data.
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-01-01T00:00:03.000000Z', 'a', 3.0), " +
                        "('2026-01-01T00:00:04.000000Z', 'b', 4.0)");
                drainWalQueue();

                execute("CHECKPOINT CREATE");
            }

            restoreFromCheckpoint();

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            assertViewMatchesRecompute(viewSql);

            execute("CHECKPOINT RELEASE");
        });
    }

    @Test
    public void testCheckpointMultipleLiveViewsOverOneBase() throws Exception {
        // Multiple views with different window shapes over one base are frozen and restored together;
        // each must converge independently to its own recompute after restore.
        final String viewSql1 = "SELECT ts, sym, x, sum(x) OVER (PARTITION BY sym ORDER BY ts " +
                "ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS s FROM base";
        final String viewSql2 = "SELECT ts, sym, x, max(x) OVER (PARTITION BY sym ORDER BY ts " +
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS m FROM base";
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv1 FLUSH EVERY 100ms AS " + viewSql1);
            execute("CREATE LIVE VIEW lv2 FLUSH EVERY 100ms AS " + viewSql2);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-01-01T00:00:01.000000Z', 'a', 1.0), " +
                        "('2026-01-01T00:00:02.000000Z', 'b', 2.0), " +
                        "('2026-01-01T00:00:03.000000Z', 'a', 3.0), " +
                        "('2026-01-01T00:00:04.000000Z', 'b', 4.0)");
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(viewSql1, "lv1");
                assertViewMatchesRecompute(viewSql2, "lv2");

                execute("CHECKPOINT CREATE");
            }

            restoreFromCheckpoint();
            drainWalQueue();

            // New base commits after restore; both resumed views must pick them up.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-01-01T00:00:05.000000Z', 'a', 5.0), " +
                        "('2026-01-01T00:00:06.000000Z', 'b', 6.0)");
                driveRefreshToQuiescence(job);
            }

            assertViewMatchesRecompute(viewSql1, "lv1");
            assertViewMatchesRecompute(viewSql2, "lv2");

            execute("CHECKPOINT RELEASE");
        });
    }

    @Test
    public void testCheckpointOfInvalidatedViewRestoresInvalid() throws Exception {
        // An invalidated view is still a WAL-backed table with its own materialized data. Checkpoint
        // must capture the terminal INVALID state (with its reason) and the frozen data; restore must
        // bring both back - the view stays invalid, keeps its reason, and remains queryable from its
        // own on-disk tier.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");

            final String expectedRows = "ts\tx\trn\n" +
                    "2026-01-01T00:00:01.000000Z\t1\t1\n" +
                    "2026-01-01T00:00:02.000000Z\t2\t2\n";

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('2026-01-01T00:00:01.000000Z', 1), " +
                        "('2026-01-01T00:00:02.000000Z', 2)");
                driveRefreshToQuiescence(job);

                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                Assert.assertFalse("LV must start valid", instance.isInvalid());
                assertQuery("SELECT ts, x, rn FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns(expectedRows);

                // Rename the base out from under the view: it flips INVALID immediately.
                execute("RENAME TABLE base TO base2");
                drainWalQueue();
                Assert.assertTrue("LV must be invalid after base rename", instance.isInvalid());
            }

            execute("CHECKPOINT CREATE");
            restoreFromCheckpoint();

            // Restored view stays invalid, keeps its reason, and its frozen data is still queryable.
            assertQuery("SELECT view_status, invalidation_reason FROM live_views() WHERE view_name = 'lv'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("view_status\tinvalidation_reason\n" +
                            "invalid\tbase table rename\n");
            assertQuery("SELECT ts, x, rn FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns(expectedRows);

            execute("CHECKPOINT RELEASE");
        });
    }

    @Test
    public void testCheckpointOverDedupBaseRestores() throws Exception {
        // Checkpoint/restore composes with a DEDUP base. A below-frontier UPSERT replaces an
        // already-emitted row before the checkpoint (exercising the dedup replay path), and the
        // recompute oracle reads the applied (post-dedup) base, so the keep-last winner is what the
        // restored view must equal.
        final String viewSql = "SELECT ts, sym, val, sum(val) OVER (PARTITION BY sym ORDER BY ts " +
                "ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS s FROM base";
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, val DOUBLE) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " + viewSql);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, sym, val) VALUES " +
                        "('2026-01-01T00:00:01.000000Z', 'a', 1.0), " +
                        "('2026-01-01T00:00:02.000000Z', 'b', 2.0), " +
                        "('2026-01-01T00:00:03.000000Z', 'a', 3.0)");
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(viewSql);

                // A below-frontier UPSERT re-points an already-emitted (ts, sym) to a new value; the
                // dedup-aware refresh must replay and replace the stale row.
                execute("INSERT INTO base (ts, sym, val) VALUES ('2026-01-01T00:00:01.000000Z', 'a', 100.0)");
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(viewSql);

                execute("CHECKPOINT CREATE");
            }

            restoreFromCheckpoint();
            drainWalQueue();

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            assertViewMatchesRecompute(viewSql);

            execute("CHECKPOINT RELEASE");
        });
    }

    @Test
    public void testCheckpointRestoresNonEmptyLiveView() throws Exception {
        // Create a live view with data and let it fully flush to disk, CHECKPOINT CREATE, then
        // advance the base past the checkpoint (rows that restore must roll back), restore, and
        // assert the view equals a from-scratch recompute over the restored base. The recompute
        // oracle inherently proves the rollback: if the view retained the post-checkpoint rows, or
        // the base kept them, the two cursors would diverge.
        final String viewSql = "SELECT ts, sym, x, sum(x) OVER (PARTITION BY sym ORDER BY ts " +
                "ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS s FROM base";
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " + viewSql);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-01-01T00:00:01.000000Z', 'a', 1.0), " +
                        "('2026-01-01T00:00:02.000000Z', 'b', 2.0), " +
                        "('2026-01-01T00:00:03.000000Z', 'a', 3.0), " +
                        "('2026-01-01T00:00:04.000000Z', 'b', 4.0), " +
                        "('2026-01-01T00:00:05.000000Z', 'a', 5.0)");
                // Converge and flush the lead to disk so the checkpoint captures the full view.
                driveRefreshToQuiescence(job);

                assertViewMatchesRecompute(viewSql);

                execute("CHECKPOINT CREATE");

                // Advance the base beyond the checkpoint. Applied to the base table (its seqTxn
                // advances) but never refreshed into the view; restore must discard all of it.
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-01-01T00:00:06.000000Z', 'a', 60.0), " +
                        "('2026-01-01T00:00:07.000000Z', 'b', 70.0)");
                drainWalQueue();
            }

            restoreFromCheckpoint();

            // Immediately after restore, before any refresh: the disk state alone must match the
            // recompute over the rolled-back base (everything was flushed pre-checkpoint).
            drainWalQueue();
            assertViewMatchesRecompute(viewSql);

            // And a refresh over the restored (rolled-back) base must be a no-op, staying converged.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            assertViewMatchesRecompute(viewSql);

            execute("CHECKPOINT RELEASE");
        });
    }

    @Test
    public void testCheckpointWhileBaseAdvancesConverges() throws Exception {
        // Convergence lock for the "an LV can lead the base in the checkpoint" hazard.
        // DatabaseCheckpointAgent orders tables dependent-first / base-last
        // (DependentViewGraph.orderByDependentViews), so it freezes and copies the live view BEFORE it
        // snapshots the base. This test forces the base to advance in exactly that window - after the
        // LV's _lv.s state file has been frozen and copied, but before the base snapshot is taken - via
        // a deterministic FilesFacade copy hook: the first time the checkpoint copies _lv.s, a helper
        // thread inserts and applies two fresh base rows, and the copy blocks until that lands. So the
        // checkpoint captures the LV at its older consumed watermark and the base at the newer state.
        //
        // Because the base is copied last (at a seqTxn >= the LV's consumed point), restore leaves the
        // LV at or behind the base, and the forward refresh catches it up: the restored view converges
        // to the recompute over the restored (advanced) base. Under a base-FIRST ordering, the LV's
        // _lv.s would instead reference base rows newer than the base snapshot and restore would keep
        // ghost rows; this lock is expected to PASS on the current (base-last) code. The hook makes the
        // interleaving deterministic - the advance always lands at the _lv.s copy - so this is a
        // behaviour lock, not a timing race.
        final String viewSql = "SELECT ts, sym, x, sum(x) OVER (PARTITION BY sym ORDER BY ts " +
                "ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS s FROM base";

        final AtomicBoolean hookFired = new AtomicBoolean(false);
        final ConcurrentLinkedQueue<Throwable> hookErrors = new ConcurrentLinkedQueue<>();

        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " + viewSql);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-01-01T00:00:01.000000Z', 'a', 1.0), " +
                        "('2026-01-01T00:00:02.000000Z', 'b', 2.0), " +
                        "('2026-01-01T00:00:03.000000Z', 'a', 3.0), " +
                        "('2026-01-01T00:00:04.000000Z', 'b', 4.0), " +
                        "('2026-01-01T00:00:05.000000Z', 'a', 5.0)");
                // Converge and flush the lead to disk so the checkpoint captures the full view at 1..5.
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(viewSql);
            }
            // The job is closed: the LV stays frozen at rows 1..5 for the whole checkpoint. Only the hook
            // moves the base.

            // Arm the one-shot _lv.s copy hook: when the checkpoint freezes and copies the LV state file,
            // advance the base (ordered last, so not yet copied) on a helper thread and block until it
            // lands. The base is free here - the checkpoint has not yet opened a reader on it.
            testFilesFacade.lvStateCopyHook = () -> {
                final SOCountDownLatch advanceDone = new SOCountDownLatch(1);
                final Thread advancer = new Thread(() -> {
                    try {
                        final TableToken baseToken = engine.verifyTableName("base");
                        try (WalWriter ww = engine.getWalWriter(baseToken)) {
                            appendBaseRow(ww, MicrosFormatUtils.parseUTCTimestamp("2026-01-01T00:00:06.000000Z"), "a", 60.0);
                            appendBaseRow(ww, MicrosFormatUtils.parseUTCTimestamp("2026-01-01T00:00:07.000000Z"), "b", 70.0);
                            ww.commit();
                        }
                        drainWalQueue(engine);
                    } catch (Throwable th) {
                        hookErrors.add(th);
                    } finally {
                        Path.clearThreadLocals();
                        advanceDone.countDown();
                    }
                }, "lv-base-advancer");
                advancer.start();
                advanceDone.await();
                hookFired.set(true);
            };

            // The hook advances the base to rows 1..7 during the _lv.s copy.
            execute("CHECKPOINT CREATE");
            if (!hookErrors.isEmpty()) {
                throw new RuntimeException("base advance hook failed", hookErrors.peek());
            }
            Assert.assertTrue("the _lv.s copy hook must have fired", hookFired.get());

            restoreFromCheckpoint();
            drainWalQueue();

            // The restored base holds rows 1..7 (the base was copied last, after the advance). The LV was
            // frozen at 1..5; the forward refresh must catch it up to the recompute over the advanced base.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            assertViewMatchesRecompute(viewSql);

            execute("CHECKPOINT RELEASE");
        });
    }

    @Test
    public void testCheckpointWithUnflushedLeadRebuildsAfterRestore() throws Exception {
        // At checkpoint time the newest rows sit in the non-durable in-mem tier (the lead), not on
        // disk. startCheckpoint freezes the view without flushing, so CHECKPOINT CREATE captures only
        // the flushed disk prefix, the head .cp accumulator snapshot, and the base data. Restore must
        // therefore rebuild the lead by replaying the retained base forward from the head .cp, landing
        // exactly the rows the lead held. The recompute oracle confirms the rebuilt lead is correct.
        final String viewSql = "SELECT ts, sym, x, sum(x) OVER (PARTITION BY sym ORDER BY ts " +
                "ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS s FROM base";
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 60s AS " + viewSql);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Flushed prefix on disk.
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-01-01T00:00:01.000000Z', 'a', 1.0), " +
                        "('2026-01-01T00:00:02.000000Z', 'b', 2.0), " +
                        "('2026-01-01T00:00:03.000000Z', 'a', 3.0)");
                driveRefreshToQuiescence(job);

                // Build an un-flushed lead: pin the flush clock to now, then refresh a forward batch
                // without advancing the clock past FLUSH EVERY, so the rows publish into the tier as
                // the lead and never reach disk.
                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                instance.setLastFlushTimeUs(currentMicros);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-01-01T00:00:04.000000Z', 'a', 4.0), " +
                        "('2026-01-01T00:00:05.000000Z', 'b', 5.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                // Precondition: a genuine un-flushed lead exists at the moment of checkpoint.
                Assert.assertTrue("test must build an un-flushed lead", instance.getLeadRowCount() > 0);
                // The tier serves the lead, so the live view already reflects every row.
                assertViewMatchesRecompute(viewSql);

                execute("CHECKPOINT CREATE");
            }

            restoreFromCheckpoint();

            // Rebuild the lead from the checkpoint's head .cp + base data, then assert convergence.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                LiveViewInstance restored = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull("view must be restored", restored);
                restored.setLastFlushTimeUs(currentMicros);
                driveRefreshToQuiescence(job);
            }

            assertViewMatchesRecompute(viewSql);

            execute("CHECKPOINT RELEASE");
        });
    }

    @Test
    public void testCheckpointWithVarSizeLeadRebuildsAfterRestore() throws Exception {
        // Cross-feature (tier x checkpoint): the in-mem tier stores every persisted type, so an
        // un-flushed lead can hold var-size (VARCHAR) and DECIMAL payloads. Restore must rebuild that
        // lead byte-for-byte from the retained base, not just the fixed-width columns.
        final String viewSql = "SELECT ts, sym, v, d, sum(x) OVER (PARTITION BY sym ORDER BY ts " +
                "ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS s FROM base";
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE, v VARCHAR, d DECIMAL(18, 3)) " +
                    "TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 60s AS " + viewSql);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, sym, x, v, d) VALUES " +
                        "('2026-01-01T00:00:01.000000Z', 'a', 1.0, 'alpha', 1.111m), " +
                        "('2026-01-01T00:00:02.000000Z', 'b', 2.0, 'beta', 2.222m), " +
                        "('2026-01-01T00:00:03.000000Z', 'a', 3.0, 'gamma', 3.333m)");
                driveRefreshToQuiescence(job);

                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                instance.setLastFlushTimeUs(currentMicros);
                execute("INSERT INTO base (ts, sym, x, v, d) VALUES " +
                        "('2026-01-01T00:00:04.000000Z', 'a', 4.0, 'delta-a-longer-varchar-value', 4.444m), " +
                        "('2026-01-01T00:00:05.000000Z', 'b', 5.0, 'epsilon', 5.555m)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                Assert.assertTrue("test must build an un-flushed var-size lead", instance.getLeadRowCount() > 0);
                assertViewMatchesRecompute(viewSql);

                execute("CHECKPOINT CREATE");
            }

            restoreFromCheckpoint();

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                LiveViewInstance restored = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull("view must be restored", restored);
                restored.setLastFlushTimeUs(currentMicros);
                driveRefreshToQuiescence(job);
            }

            assertViewMatchesRecompute(viewSql);

            execute("CHECKPOINT RELEASE");
        });
    }

    @Test
    public void testRestoredLiveViewContinuesRefreshingToConvergence() throws Exception {
        // After restore the view must resume from its persisted consumed-seqTxn watermark and
        // materialize brand-new base commits, converging to the recompute over the grown base.
        final String viewSql = "SELECT ts, sym, x, sum(x) OVER (PARTITION BY sym ORDER BY ts " +
                "ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS s FROM base";
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " + viewSql);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-01-01T00:00:01.000000Z', 'a', 1.0), " +
                        "('2026-01-01T00:00:02.000000Z', 'b', 2.0), " +
                        "('2026-01-01T00:00:03.000000Z', 'a', 3.0)");
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(viewSql);

                execute("CHECKPOINT CREATE");
            }

            restoreFromCheckpoint();
            drainWalQueue();

            // New base commits arrive after the restore; the resumed view must pick them up.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-01-01T00:00:04.000000Z', 'a', 4.0), " +
                        "('2026-01-01T00:00:05.000000Z', 'b', 5.0), " +
                        "('2026-01-01T00:00:06.000000Z', 'a', 6.0)");
                driveRefreshToQuiescence(job);
            }

            assertViewMatchesRecompute(viewSql);

            execute("CHECKPOINT RELEASE");
        });
    }

    // Appends one row to the (ts, sym, x) base table via a direct WalWriter. Used by the base-advance
    // FilesFacade hook, which runs off the SQL execution context and so cannot use execute(INSERT ...).
    private static void appendBaseRow(WalWriter walWriter, long ts, CharSequence sym, double x) {
        TableWriter.Row row = walWriter.newRow(ts);
        row.putSym(1, sym);
        row.putDouble(2, x);
        row.append();
    }

    // The live view must equal the same window recomputed directly over the base table. The view's
    // stored columns are exactly the projection it was created from, so (lv) and (viewSql) share a
    // schema. ORDER BY 2, 1 (sym, ts) gives both sides a total order; genericStringMatch tolerates
    // the SYMBOL-vs-STRING passthrough difference.
    private void assertViewMatchesRecompute(String viewSql) throws Exception {
        assertViewMatchesRecompute(viewSql, "lv");
    }

    private void assertViewMatchesRecompute(String viewSql, String viewName) throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + viewSql + ") ORDER BY 2, 1",
                "(" + viewName + ") ORDER BY 2, 1",
                LOG,
                true
        );
    }

    private void createTriggerFile() {
        Files.touch(triggerFilePath.$());
    }

    // Pumps the refresh job until no further LV WAL work is produced, advancing the clock each pass
    // so deferred flushes land, and applying the LV's own WAL after each burst. Mirrors the helper
    // in LiveViewFuzzTest.
    private void driveRefreshToQuiescence(LiveViewRefreshJob job) {
        for (int i = 0; i < 512; i++) {
            setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
            drainWalQueue();
            boolean progressed = drainJob(job);
            drainWalQueue();
            if (!progressed) {
                break;
            }
        }
    }

    // Simulates a restore-from-checkpoint restart in-process: releases all readers/writers, drops
    // the _restore trigger file, runs checkpoint recovery (which copies the snapshot metadata back
    // over the db root), then re-hydrates the name registry, metadata cache and view graphs. Mirrors
    // the in-process restore sequence in CheckpointTest#testCheckpointRestoresLiveView.
    private void restoreFromCheckpoint() {
        engine.clear();
        engine.closeNameRegistry();
        createTriggerFile();
        engine.checkpointRecover();
        engine.reloadTableNames();
        engine.getMetadataCache().onStartupAsyncHydrator();
        engine.buildViewGraphs();
    }

    private static boolean drainJob(Job job) {
        boolean any = false;
        for (int i = 0; i < 64 && job.run(); i++) {
            any = true;
        }
        return any;
    }

    // A pass-through FilesFacade with a one-shot hook that fires when the checkpoint copies a live
    // view's _lv.s state file. Installed before the engine is built so DatabaseCheckpointAgent captures
    // it (the agent snapshots configuration.getFilesFacade() into a final field at construction, so a
    // per-test assertMemoryLeak(ff, ...) swap would arrive too late for the copy loop).
    private static class LvCheckpointFilesFacade extends TestFilesFacadeImpl {
        // Armed by a test; run once, on the first _lv.s copy after arming. Runs on the checkpoint thread.
        Runnable lvStateCopyHook;

        @Override
        public int copy(LPSZ from, LPSZ to) {
            final Runnable hook = lvStateCopyHook;
            if (hook != null && Utf8s.endsWithAscii(from, LiveViewState.LIVE_VIEW_STATE_FILE_NAME)) {
                lvStateCopyHook = null; // one-shot
                hook.run();
            }
            return super.copy(from, to);
        }

        void reset() {
            lvStateCopyHook = null;
        }
    }
}
