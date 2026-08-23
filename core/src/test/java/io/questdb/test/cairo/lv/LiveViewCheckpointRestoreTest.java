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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewState;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Os;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.LogCapture;
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
 * It is deliberately distinct from the checkpoint timeline suites, which unit-test the
 * internal per-view {@code _checkpoints/} format and never run a SQL statement or the
 * checkpoint agent.
 */
public class LiveViewCheckpointRestoreTest extends AbstractLiveViewTest {

    // > FLUSH EVERY 100ms, so a single driveRefreshToQuiescence pass crosses the flush window.
    private static final String SNAPSHOT_ID = "test-checkpoint-instance";
    // Installed before the engine is built (setUpStatic) so DatabaseCheckpointAgent captures it. A pure
    // pass-through unless a test arms lvStateCopyHook; testCheckpointWhileBaseAdvancesConverges uses it to
    // land a deterministic base advance during the _lv.s copy.
    private static final LvCheckpointFilesFacade testFilesFacade = new LvCheckpointFilesFacade();
    // One-shot fault injection for testCheckpointRetryDoesNotLeakLiveViewFreeze: fires the first time
    // the checkpoint agent opens a reader for the named live view, simulating a reader retry that lands
    // in the narrow DROP window. retryFired records whether it actually fired.
    private static final AtomicBoolean retryFired = new AtomicBoolean(false);
    private static volatile String retryLvName;
    private static Path checkpointPath;
    private static Path triggerFilePath;
    private int checkpointRootLen;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        checkpointPath = new Path();
        triggerFilePath = new Path();
        ff = testFilesFacade;
        // Engine whose getReaderWithRepair injects a one-shot reader retry for the armed live view.
        // Inert unless retryLvName is set, so every other test in this class sees a stock engine.
        AbstractCairoTest.engineFactory = conf -> new CairoEngine(conf) {
            @Override
            public TableReader getReaderWithRepair(TableToken tableToken) {
                final String lvName = retryLvName;
                if (lvName != null
                        && tableToken.isLiveView()
                        && lvName.equals(tableToken.getTableName())
                        && retryFired.compareAndSet(false, true)) {
                    // Simulate the narrow DROP window: the view has left the registry's viewsByName
                    // (so the agent's retry re-fetch of the instance returns null) while the table
                    // token is not yet marked dropped. Throwing here forces the agent's for(;;) loop
                    // to re-enter the isLiveView branch a second time.
                    getLiveViewRegistry().removeView(tableToken.getTableName());
                    throw TableReferenceOutOfDateException.of(tableToken);
                }
                return super.getReaderWithRepair(tableToken);
            }
        };
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
        // Disarm the reader-retry injection so it never leaks into another test.
        retryLvName = null;
        retryFired.set(false);
        checkpointPath.of(configuration.getCheckpointRoot()).concat(configuration.getDbDirectory()).slash();
        checkpointRootLen = checkpointPath.size();
        triggerFilePath.of(configuration.getDbRoot()).parent().concat(TableUtils.RESTORE_FROM_CHECKPOINT_TRIGGER_FILE_NAME).$();
        // Pin the clock below all (future-dated) test data: a non-SEED view's lower bound is
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
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " + viewSql);

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
    public void testSnapshotExcludesTimelineAndRestoreRebuildsDerivedState() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        final String viewSql = "SELECT ts, sym, x, sum(x) OVER (PARTITION BY sym ORDER BY ts " +
                "ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS running FROM base";
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " + viewSql);
            final TableToken lvToken = engine.verifyTableName("lv");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-01-01T00:00:01.000000Z', 'a', 1.0), " +
                        "('2026-01-01T00:00:02.000000Z', 'a', 2.0)");
                driveRefreshToQuiescence(job);
                assertTimelineExists(lvToken);

                execute("CHECKPOINT CREATE");
                assertSnapshotDoesNotContainCheckpoints(lvToken);

                // Advance both authoritative data and the local timeline beyond the
                // snapshot. Restore must not let these newer derived roots survive.
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-01-01T00:00:03.000000Z', 'a', 3.0), " +
                        "('2026-01-01T00:00:04.000000Z', 'a', 4.0)");
                driveRefreshToQuiescence(job);
                assertTimelineExists(lvToken);
            }

            // Inspect the restore boundary before graph reload can reconcile or a
            // refresh can publish a replacement timeline. Its publication point and
            // nested metadata/data directories must be gone; only an empty container remains.
            engine.clear();
            engine.closeNameRegistry();
            createTriggerFile();
            engine.checkpointRecover();
            assertCheckpointStateCleared(lvToken);
            engine.reloadTableNames();
            engine.getMetadataCache().onStartupAsyncHydrator();
            engine.buildViewGraphs();
            assertCheckpointStateCleared(lvToken);

            // The first post-restore turn performs the forced applied-base rebuild.
            // Forced replay does not seal a timeline generation, so a following
            // ordinary in-order commit exercises normal local timeline publication.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, sym, x) VALUES ('2026-01-01T00:00:05.000000Z', 'a', 10.0)");
                driveRefreshToQuiescence(job);
                execute("INSERT INTO base (ts, sym, x) VALUES ('2026-01-01T00:00:06.000000Z', 'a', 6.0)");
                driveRefreshToQuiescence(job);
            }
            assertTimelineExists(engine.verifyTableName("lv"));

            assertQuery("SELECT ts, sym, x, running FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tsym\tx\trunning\n" +
                            "2026-01-01T00:00:01.000000Z\ta\t1.0\t1.0\n" +
                            "2026-01-01T00:00:02.000000Z\ta\t2.0\t3.0\n" +
                            "2026-01-01T00:00:05.000000Z\ta\t10.0\t13.0\n" +
                            "2026-01-01T00:00:06.000000Z\ta\t6.0\t19.0\n");
            assertViewMatchesRecompute(viewSql);

            execute("CHECKPOINT RELEASE");
        });
    }

    @Test
    public void testCheckpointsDirDoesNotLogInvalidPartition() throws Exception {
        // Regression: the live view's _checkpoints directory (LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME)
        // lives inside the view's table folder. Both partition-purge scans used to try to parse it as a
        // partition timestamp, fail, and log a spurious "invalid partition directory inside table folder"
        // ERROR. The directory was always left intact, so the only observable symptom is the log line.
        // Two scans are exercised here: TableSnapshotRestore's scan during restore, and
        // TableWriter.removePartitionDirsNotAttached when the restored LV table writer reopens fresh.
        final String viewSql = "SELECT ts, sym, x, sum(x) OVER (PARTITION BY sym ORDER BY ts " +
                "ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS s FROM base";
        final LogCapture capture = new LogCapture();
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " + viewSql);

            capture.start();
            try {
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    execute("INSERT INTO base (ts, sym, x) VALUES " +
                            "('2026-01-01T00:00:01.000000Z', 'a', 1.0), " +
                            "('2026-01-02T00:00:02.000000Z', 'b', 2.0)");
                    driveRefreshToQuiescence(job);
                    assertViewMatchesRecompute(viewSql);
                    execute("CHECKPOINT CREATE");
                }

                // The flush cycle must have written the _checkpoints dir, otherwise neither scan would
                // encounter it and this test would trivially pass.
                assertCheckpointsDirExists("lv");

                // Restore re-scans the restored table folder (TableSnapshotRestore site), then the
                // post-restore refresh reopens the LV table writer fresh with the restored _checkpoints
                // present (TableWriter.purgeUnusedPartitions site). Both used to log the ERROR.
                restoreFromCheckpoint();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveRefreshToQuiescence(job);
                }
                assertViewMatchesRecompute(viewSql);
                assertCheckpointsDirExists("lv");

                // Flush barrier on the same async log path: once this sentinel reaches the captured
                // sink, any earlier ERROR (FIFO) is already present, so assertNotLogged is reliable.
                LOG.info().$("live view checkpoints purge test flush barrier").$();
                capture.waitForRegex("live view checkpoints purge test flush barrier");
                capture.assertNotLogged("invalid partition directory");
            } finally {
                capture.stop();
            }

            execute("CHECKPOINT RELEASE");
        });
    }

    @Test
    public void testCheckpointMidSeedReSweepsAfterRestore() throws Exception {
        // A checkpoint taken mid-SEED, then a restore, must not leave the sweep resuming from a
        // timeline that got AHEAD of the checkpoint. After CHECKPOINT CREATE returns, the (unfrozen)
        // sweep keeps advancing: its LV table and its seed boundaries both move past the checkpoint.
        // Restore rolls the LV's _txn / partitions / _lv.s back to the checkpoint (R_cp rows on
        // disk); a surviving generation whose newest root sits at R_bcp > R_cp would jump the data
        // cursor past the base rows that produced R_cp..R_bcp while lvRowsTotal starts at R_bcp - a
        // permanent silent gap over [R_cp, R_bcp).
        //
        // Restore closes that by clearing the derived timeline outright, so the resumed sweep finds
        // no resume point and re-runs from offset 0 behind its skip-write floor, converging to the
        // recompute over the restored base. CHECKPOINT_ROWS=1 seals a boundary every swept row so
        // the ahead window is reached deterministically after a single turn.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        final String viewSql = "SELECT ts, sym, x, sum(x) OVER (PARTITION BY sym ORDER BY ts " +
                "ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS s FROM base";
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base (ts, sym, x) VALUES " +
                    "('2026-01-01T00:00:01.000000Z', 'a', 1.0), " +
                    "('2026-01-01T00:00:02.000000Z', 'b', 2.0), " +
                    "('2026-01-01T00:00:03.000000Z', 'a', 3.0), " +
                    "('2026-01-01T00:00:04.000000Z', 'b', 4.0), " +
                    "('2026-01-01T00:00:05.000000Z', 'a', 5.0), " +
                    "('2026-01-01T00:00:06.000000Z', 'b', 6.0)");
            drainWalQueue();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " + viewSql);

            final long seedOffsetAtCheckpoint;
            final TableToken lvToken = engine.verifyTableName("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // One seed turn: exactly one row lands on disk (R_cp = 1) and a seed boundary is
                // sealed. The view stays SEEDING.
                job.run();
                drainWalQueue();
                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                Assert.assertEquals(
                        "view must still be SEEDING at checkpoint time",
                        LiveViewState.SEED_STATE_SEEDING,
                        instance.getStateReader().getSeedState()
                );
                seedOffsetAtCheckpoint = instance.getSeedCheckpointDataOffset();
                Assert.assertNotEquals("a seed boundary must have been sealed before the checkpoint",
                        Numbers.LONG_NULL, seedOffsetAtCheckpoint);

                execute("CHECKPOINT CREATE");

                // The view is unfrozen now: advance the sweep past the checkpoint so the LV table and
                // the seed boundaries both move ahead (R_bcp > R_cp), while staying SEEDING (not
                // every base row is swept yet, so completion does not retire the timeline).
                for (int i = 0; i < 3; i++) {
                    job.run();
                    drainWalQueue();
                }
                instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                Assert.assertEquals(
                        "the sweep must still be SEEDING (the ahead timeline must not be retired by completion)",
                        LiveViewState.SEED_STATE_SEEDING,
                        instance.getStateReader().getSeedState()
                );
                Assert.assertTrue(
                        "the seed cursor must have advanced past the checkpoint",
                        instance.getSeedCheckpointDataOffset() > seedOffsetAtCheckpoint
                );
            }

            restoreFromCheckpoint();
            drainWalQueue();
            assertCheckpointStateCleared(lvToken);

            // Resume the sweep: with no timeline to resume from, it re-runs from offset 0 and the
            // view converges to the full recompute over the restored base.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
            }
            Assert.assertEquals(
                    LiveViewState.SEED_STATE_ACTIVE,
                    engine.getLiveViewRegistry().getViewInstance("lv").getStateReader().getSeedState()
            );
            assertLiveViewRowCount(6);
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
            execute("CREATE LIVE VIEW lv1 FLUSH EVERY 100ms START FROM NOW AS " + viewSql1);
            execute("CREATE LIVE VIEW lv2 FLUSH EVERY 100ms START FROM NOW AS " + viewSql2);

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
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, g SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " +
                    "SELECT ts, x, count(*) OVER (PARTITION BY g ORDER BY ts " +
                    "ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base");

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
    public void testCheckpointOverUnreadableLiveViewStateSkipsItAndCompletes() throws Exception {
        // A live view whose _lv.s is gone but whose _lv (the CREATE commit marker) survives comes
        // up as a droppable state_unreadable stub: the loader refuses it rather than resume from a
        // -1 floor and replay the whole base. The checkpoint agent hard-threw when it could not
        // copy that missing file, and its per-table loop has no per-table catch - so ONE damaged
        // view failed CHECKPOINT CREATE for the ENTIRE database, every healthy table included,
        // until an operator found it and dropped it. The copy must skip what is not there.
        //
        // Skipping does not paper the damage over: the snapshot reproduces the view exactly as
        // broken as it is here, and the restored database re-derives the same stub from the same
        // missing file.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, g SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " +
                    "SELECT ts, x, count(*) OVER (PARTITION BY g ORDER BY ts " +
                    "ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            // A healthy, unrelated table. It is what the whole-database checkpoint was losing.
            execute("CREATE TABLE other (ts TIMESTAMP, y INT) TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, x) VALUES ('2026-01-01T00:00:01.000000Z', 1)");
                execute("INSERT INTO other (ts, y) VALUES ('2026-01-01T00:00:01.000000Z', 7)");
                driveRefreshToQuiescence(job);
            }
            drainWalQueue();

            // Damage the view the way a restart onto the damaged directory would find it.
            removeLiveViewStateFile("lv");
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            assertQuery("SELECT view_name, view_status FROM live_views()")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("view_name\tview_status\n" +
                            "lv\tstate_unreadable\n");

            // The database-wide checkpoint must complete despite the damaged view.
            execute("CHECKPOINT CREATE");
            restoreFromCheckpoint();

            // The healthy table came through the snapshot intact.
            assertQuery("SELECT ts, y FROM other")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\ty\n" +
                            "2026-01-01T00:00:01.000000Z\t7\n");
            // And the damaged view is still surfaced as damaged, not silently resurrected.
            assertQuery("SELECT view_name, view_status FROM live_views()")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("view_name\tview_status\n" +
                            "lv\tstate_unreadable\n");

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
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " + viewSql);

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
    public void testCheckpointRestoresFirstValueIgnoreNullsLaggingRangeDouble() throws Exception {
        // first_value(DOUBLE) IGNORE NULLS over a RANGE frame whose upper bound lags the current row.
        // The window function accumulator is snapshotted into the head checkpoint; restore must reload
        // it so post-restore rows still report the captured value. See the helper for what this used to
        // cover and why its lower bound is now a wide finite look-behind.
        assertFirstValueIgnoreNullsLaggingRangeRestore("DOUBLE", "1.0", "2.0", "3.0");
    }

    @Test
    public void testCheckpointRestoresFirstValueIgnoreNullsLaggingRangeLong() throws Exception {
        // As testCheckpointRestoresFirstValueIgnoreNullsLaggingRangeDouble but for first_value(LONG),
        // which carries its own inline snapshot/restore in FirstValueLongWindowFunctionFactory.
        assertFirstValueIgnoreNullsLaggingRangeRestore("LONG", "10", "20", "30");
    }

    @Test
    public void testCheckpointRestoresLiveViewAdvancedPastCheckpoint() throws Exception {
        // Regression for routing live views through the full table restore path (copyMetadataFiles +
        // resetTodoLog + rebuildTableFiles) rather than a metadata-only copy. A live view keeps
        // refreshing after CHECKPOINT CREATE returns - the freeze is released once the checkpoint
        // finishes - so its own _txn / _cv / _lv.s and partition data advance past the checkpoint in
        // the normal live-ingestion window. Restore must roll the view back to the checkpoint just
        // like its base. Pre-fix, restore copied only _meta / _name / _lv for a live view and skipped
        // _txn / _cv / _lv.s plus the partition rebuild, so the view kept its post-checkpoint rows
        // while the base rolled back, diverging from the recompute.
        final String viewSql = "SELECT ts, sym, x, sum(x) OVER (PARTITION BY sym ORDER BY ts " +
                "ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS s FROM base";
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " + viewSql);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-01-01T00:00:01.000000Z', 'a', 1.0), " +
                        "('2026-01-01T00:00:02.000000Z', 'b', 2.0), " +
                        "('2026-01-01T00:00:03.000000Z', 'a', 3.0), " +
                        "('2026-01-01T00:00:04.000000Z', 'b', 4.0), " +
                        "('2026-01-01T00:00:05.000000Z', 'a', 5.0)");
                // Converge and flush the lead to disk so the checkpoint captures the view at 1..5.
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(viewSql);
                assertLiveViewRowCount(5);

                execute("CHECKPOINT CREATE");

                // Advance the base past the checkpoint AND materialize it into the live view's own
                // table, so the view's _txn / _cv / _lv.s and partition data all move past the
                // checkpoint. CHECKPOINT CREATE has returned, so the view is unfrozen and the refresh
                // worker is free to run here.
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-01-01T00:00:06.000000Z', 'a', 6.0), " +
                        "('2026-01-01T00:00:07.000000Z', 'b', 7.0)");
                driveRefreshToQuiescence(job);
                // The view really did advance past the checkpoint before the restore.
                assertViewMatchesRecompute(viewSql);
                assertLiveViewRowCount(7);
            }

            restoreFromCheckpoint();

            // Immediately after restore, before any refresh: the restored view must be rolled back to
            // the checkpoint (5 rows), matching the recompute over the rolled-back base. Pre-fix the
            // view kept its 7 post-checkpoint rows and this diverged.
            drainWalQueue();
            assertLiveViewRowCount(5);
            assertViewMatchesRecompute(viewSql);

            // A refresh over the restored (rolled-back) base is a no-op: the base has no rows past the
            // checkpoint, so the view stays converged at 5 rows.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            assertLiveViewRowCount(5);
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
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " + viewSql);

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
    public void testCheckpointRetryDoesNotLeakLiveViewFreeze() throws Exception {
        // Regression for the DatabaseCheckpointAgent freeze/unfreeze pairing. The inner for(;;)
        // reader-retry loop must freeze the LiveViewInstance once and keep that reference across
        // retries. If a concurrent DROP removes the view from the registry between the initial
        // freeze and a reader retry, getViewInstance returns null on the retry; pre-fix the loop
        // overwrote freezeLvInstance with that null, losing the reference to the frozen instance,
        // so the finally never called endCheckpoint() and the view stayed frozen forever (a later
        // base-table invalidation would then park in waitForUnfrozen() and hang).
        //
        // The engineFactory installed in setUpStatic injects exactly that: on the LV's first
        // getReaderWithRepair during CHECKPOINT CREATE it removes the view from the registry and
        // throws a retriable exception, forcing the agent to re-enter the isLiveView branch with
        // getViewInstance now returning null.
        final String viewSql = "SELECT ts, sym, x, sum(x) OVER (PARTITION BY sym ORDER BY ts " +
                "ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS s FROM base";
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " + viewSql);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-01-01T00:00:01.000000Z', 'a', 1.0), " +
                        "('2026-01-01T00:00:02.000000Z', 'b', 2.0)");
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(viewSql);

                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                Assert.assertFalse("view must not start frozen", instance.isFreezeInProgress());

                // Arm the one-shot reader-retry injection, then run the real CHECKPOINT CREATE.
                retryFired.set(false);
                retryLvName = "lv";
                execute("CHECKPOINT CREATE");
                retryLvName = null;

                // The injection must actually have fired, otherwise the retry path was never taken
                // and the assertion below would pass vacuously.
                Assert.assertTrue("reader-retry injection never fired", retryFired.get());
                // The freeze must be released despite the retry that saw a null instance.
                Assert.assertFalse(
                        "CHECKPOINT leaked the live view freeze across a reader retry",
                        instance.isFreezeArmed()
                );

                // The injection removed the instance from the registry; re-register it so normal
                // teardown frees its native state and the DROP below can find it.
                engine.getLiveViewRegistry().registerView(instance);
            }

            execute("CHECKPOINT RELEASE");
            execute("DROP LIVE VIEW lv");
            execute("DROP TABLE base");
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
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " + viewSql);

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
        // the flushed disk prefix, the head checkpoint accumulator snapshot, and the base data. Restore must
        // therefore rebuild the lead by replaying the retained base forward from the head checkpoint, landing
        // exactly the rows the lead held. The recompute oracle confirms the rebuilt lead is correct.
        final String viewSql = "SELECT ts, sym, x, sum(x) OVER (PARTITION BY sym ORDER BY ts " +
                "ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS s FROM base";
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 60s START FROM NOW AS " + viewSql);

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

            // Rebuild the lead from the checkpoint's head checkpoint + base data, then assert convergence.
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
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 60s START FROM NOW AS " + viewSql);

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
    public void testRestoredLiveViewWithDecimalWindowConvergesAfterRestore() throws Exception {
        // DECIMAL reaches CHECKPOINT CREATE / restore only as a passthrough projection column
        // (testCheckpointWithVarSizeLeadRebuildsAfterRestore), never under a window function - so the
        // DECIMAL window state, which is where the interesting per-width ring/deque serialization
        // lives, was never carried through a real database checkpoint. Same shape as the convergence
        // test above, with max/min/sum over a DECIMAL(38, 6) so the window state has to survive.
        //
        // The values are deliberately non-monotonic, so max and min over the sliding frame both have
        // to expire an extremum that has left it.
        final String viewSql = "SELECT ts, sym, d, " +
                "  max(d) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS mx, " +
                "  min(d) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS mn, " +
                "  sum(d) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS s " +
                "FROM base";
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, d DECIMAL(38, 6)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " + viewSql);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, sym, d) VALUES " +
                        "('2026-01-01T00:00:01.000000Z', 'a', 10.000000m), " +
                        "('2026-01-01T00:00:02.000000Z', 'b', 6.000000m), " +
                        "('2026-01-01T00:00:03.000000Z', 'a', 30.000000m), " +
                        "('2026-01-01T00:00:04.000000Z', 'a', 5.000000m)");
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(viewSql);

                execute("CHECKPOINT CREATE");
            }

            restoreFromCheckpoint();
            drainWalQueue();

            // The post-restore rows slide both frames past the pre-restore extrema, so converging
            // needs the restored DECIMAL window state, not just the restored rows.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, sym, d) VALUES " +
                        "('2026-01-01T00:00:05.000000Z', 'a', 20.000000m), " +
                        "('2026-01-01T00:00:06.000000Z', 'b', 18.000000m), " +
                        "('2026-01-01T00:00:07.000000Z', 'a', 15.000000m)");
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
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " + viewSql);

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

    private void assertCheckpointStateCleared(TableToken token) {
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(token).concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME).$();
            Assert.assertTrue("restore must leave an empty checkpoint container at " + path, ff.exists(path.$()));
            final int checkpointsDirLen = path.size();
            path.concat(LiveViewCheckpointLayout.TIMELINE_FILE_NAME).$();
            Assert.assertFalse("stale timeline publication must be removed at " + path, ff.exists(path.$()));
            path.trimTo(checkpointsDirLen).concat(LiveViewCheckpointLayout.META_DIR_NAME).$();
            Assert.assertFalse("stale timeline metadata must be removed at " + path, ff.exists(path.$()));
            path.trimTo(checkpointsDirLen).concat(LiveViewCheckpointLayout.DATA_DIR_NAME).$();
            Assert.assertFalse("stale timeline data must be removed at " + path, ff.exists(path.$()));
        }
    }

    private void assertSnapshotDoesNotContainCheckpoints(TableToken token) {
        try (Path path = new Path()) {
            path.of(configuration.getCheckpointRoot())
                    .concat(configuration.getDbDirectory())
                    .concat(token)
                    .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME)
                    .$();
            Assert.assertFalse("checkpoint snapshot must exclude derived live-view state at " + path, configuration.getFilesFacade().exists(path.$()));
        }
    }

    private void assertTimelineExists(TableToken token) {
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot())
                    .concat(token)
                    .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME)
                    .concat(LiveViewCheckpointLayout.TIMELINE_FILE_NAME)
                    .$();
            Assert.assertTrue("expected durable live-view timeline at " + path, configuration.getFilesFacade().exists(path.$()));
        }
    }

    private void assertCheckpointsDirExists(String viewName) {
        final TableToken token = engine.verifyTableName(viewName);
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(token).concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME).$();
            Assert.assertTrue(
                    "expected _checkpoints dir at " + path,
                    configuration.getFilesFacade().exists(path.$())
            );
        }
    }

    // Drives a first_value(valueType) IGNORE NULLS live view over a RANGE frame whose upper bound
    // lags the current row through checkpoint/restore and asserts convergence to a from-scratch
    // recompute. The three pre-checkpoint rows per partition are (null, v, w): the null exercises
    // IGNORE NULLS, and w lands more than the '2' SECOND upper offset past v. After restore, a fourth
    // row (x) still holds v in its frame, so the reloaded accumulator - not a fresh one - decides its
    // first_value.
    // The lower bound is a wide finite look-behind rather than UNBOUNDED PRECEDING. This began as a
    // regression for the frameLoBounded == false accumulator, whose snapshot walked logical order over
    // a value parked at physical ring index 0 and so read a never-written slot; a live view can no
    // longer create that shape, since the finite-influence gate rejects an unbounded frame start. Over
    // this data the wide look-behind selects the same rows, so the assertions are unchanged and what
    // they now cover is the bounded-lo accumulator.
    private void assertFirstValueIgnoreNullsLaggingRangeRestore(
            String valueType,
            String v,
            String w,
            String x
    ) throws Exception {
        final String viewSql = "SELECT ts, sym, first_value(val) IGNORE NULLS OVER w AS fv FROM base " +
                "WINDOW w AS (PARTITION BY sym ORDER BY ts RANGE BETWEEN '24' HOUR PRECEDING AND '2' SECOND PRECEDING)";
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, val " + valueType + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " + viewSql);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, sym, val) VALUES " +
                        "('2026-01-01T00:00:00.000000Z', 'a', null), " +
                        "('2026-01-01T00:00:01.000000Z', 'a', " + v + "), " +
                        "('2026-01-01T00:00:10.000000Z', 'a', " + w + "), " +
                        "('2026-01-01T00:00:00.000000Z', 'b', null), " +
                        "('2026-01-01T00:00:01.000000Z', 'b', " + v + "), " +
                        "('2026-01-01T00:00:10.000000Z', 'b', " + w + ")");
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(viewSql);

                execute("CHECKPOINT CREATE");
            }

            restoreFromCheckpoint();
            drainWalQueue();

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, sym, val) VALUES " +
                        "('2026-01-01T00:00:20.000000Z', 'a', " + x + "), " +
                        "('2026-01-01T00:00:20.000000Z', 'b', " + x + ")");
                driveRefreshToQuiescence(job);
            }
            assertViewMatchesRecompute(viewSql);

            execute("CHECKPOINT RELEASE");
        });
    }

    // Asserts the live view currently holds exactly the given row count. Complements the recompute
    // oracle with an unambiguous check that restore rolled the view's own table back to the
    // checkpoint rather than keeping its post-checkpoint rows.
    private void assertLiveViewRowCount(long expected) throws Exception {
        assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n" + expected + "\n");
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
        // A refresh fault self-heals into a full recompute from the applied base, which this
        // oracle would match either way; assert no cycle faulted so an incremental-path
        // regression (including on the post-restore path) cannot hide behind the recovery.
        assertNoRefreshFaults(viewName);
    }

    private void createTriggerFile() {
        Files.touch(triggerFilePath.$());
    }

    // Unlinks the view's _lv.s, leaving its _lv (the CREATE commit marker) in place. That is the
    // on-disk shape the loader reports as state_unreadable: a committed definition with no state to
    // resume from.
    private void removeLiveViewStateFile(String viewName) {
        final TableToken token = engine.verifyTableName(viewName);
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(token).concat(LiveViewState.LIVE_VIEW_STATE_FILE_NAME).$();
            Assert.assertTrue(
                    "expected an _lv.s to remove at " + path,
                    configuration.getFilesFacade().removeQuiet(path.$())
            );
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
