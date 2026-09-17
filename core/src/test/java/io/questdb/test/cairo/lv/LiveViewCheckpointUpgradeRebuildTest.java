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

package io.questdb.test.cairo.lv;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointRecoveryPhase;
import io.questdb.cairo.lv.LiveViewCheckpointRestoreRoute;
import io.questdb.cairo.lv.LiveViewCheckpointSuperblock;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRebuildRestatementGuard;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewState;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import java.util.zip.CRC32;

/**
 * The upgrade rebuild of a live view carried over from an older checkpoint format, on the paths the
 * released fixtures cannot reach: crashes between its steps, faults in its retire, a base whose
 * apply lags the view, a view still seeding, a view that never materialized a row, and a shutdown
 * in the middle of its scan.
 * <p>
 * Every case builds a view with this build, stops the process, and stamps the view's
 * {@code _timeline} with an older format version in both fields that carry one, checksum and all -
 * the shape a released 10.0.x slot has. Nothing below the superblock is rewritten, and nothing
 * needs to be: the upgrade never decodes a byte under an older superblock.
 * {@link LiveViewCheckpointReleaseCompatTest} runs the same route over trees 10.0.1 really wrote.
 * <p>
 * The crash-safety argument the cases pin: the older superblock is the only record that the
 * rebuild is owed, and it stays on disk until the rebuild's replacement has committed. A restart
 * before that point meets it and rebuilds again; a restart after the retire meets no timeline and
 * takes the ordinary missing-timeline route over rows that already equal a recompute.
 */
public class LiveViewCheckpointUpgradeRebuildTest extends AbstractLiveViewCheckpointCompatTest {
    private static final String ALL_ROWS = """
            created_at\taccount_id\tcumulative_sum\tcumulative_count
            2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
            2026-01-01T09:10:00.000000Z\tacct-2\t2.0\t1
            2026-01-02T09:00:00.000000Z\tacct-1\t4.0\t1
            2026-01-02T09:10:00.000000Z\tacct-1\t12.0\t2
            2026-01-03T09:00:00.000000Z\tacct-1\t16.0\t1
            2026-01-03T09:10:00.000000Z\tacct-2\t32.0\t1
            """;
    private static final int OLDER_FORMAT_VERSION = LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION - 1;
    private static final String UPGRADE_CAUSE = "timeline format upgrade";
    private static final String VIEW_ROWS_QUERY = "SELECT created_at, account_id, cumulative_sum, cumulative_count FROM lv";
    private static final LogCapture capture = new LogCapture();
    private final UpgradeFault fault = new UpgradeFault();

    @After
    public void resetClock() {
        capture.stop();
        setCurrentMicros(-1);
    }

    @Before
    public void setUpCadence() {
        // One logical boundary per commit, so every flush seals and a case can count seals.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setCurrentMicros(0);
        capture.start();
    }

    @Test
    public void testACancelledUpgradeRebuildStaysPendingForTheNextStart() throws Exception {
        assertMemoryLeak(() -> {
            seedSixRows();
            carryOverToOlderFormat();
            final LiveViewInstance instance = instance("lv");
            Assert.assertTrue(instance.isCheckpointUpgradeRebuildPending());

            // What DROP, an invalidation and engine shutdown do to a scan in flight: trip the
            // breaker the refresh turn's rebuild consults. A shutdown is the one that matters here,
            // because every view carried over from an older format runs this scan on its first
            // start, and a large base makes it a long one.
            instance.cancelRefresh();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            capture.drain();
            capture.assertLogged("live view restart rebuilding from applied base [view=lv, cause=" + UPGRADE_CAUSE);
            capture.assertLogged("live view refresh cancelled [view=lv");
            capture.assertNotLogged("live view restart applied-base rebuild failed");
            Assert.assertFalse("a cancelled rebuild must not invalidate the view", instance.isInvalid());
            Assert.assertFalse(instance.hasPendingInvalidationReason());
            Assert.assertEquals(
                    "a cancelled rebuild names no route",
                    LiveViewCheckpointRestoreRoute.NONE,
                    instance.getCheckpointRestoreRoute()
            );
            Assert.assertTrue("the older directory is untouched, so the rebuild is still owed", instance.isCheckpointUpgradeRebuildPending());
            Assert.assertEquals(OLDER_FORMAT_VERSION, readSuperblockFormatVersion(checkpointsRootByDirName()));
            assertViewRows(ALL_ROWS);

            // The next start meets the older superblock and runs the rebuild to the end.
            shutdown();
            restart();
            assertUpgradeRebuilt("lv");
            Assert.assertFalse(instance("lv").isInvalid());
            assertViewRows(ALL_ROWS);
            assertNoRefreshFaults("lv");
            Assert.assertEquals(LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION, readSuperblockFormatVersion(checkpointsRootByDirName()));
        });
    }

    @Test
    public void testACrashBetweenTheCommitAndTheRetireRebuildsAgainOnRestart() throws Exception {
        assertMemoryLeak(fault, () -> {
            seedSixRows();
            carryOverToOlderFormat();

            // The retire that follows the replacement commit cannot remove _timeline, which is the
            // disk a crash between the two leaves: the rebuilt rows, beside the older superblock.
            fault.armTimelineRemove();
            final LiveViewRebuildRestatementGuard guard = drive();
            Assert.assertTrue(fault.hasTimelineRemoveFired());
            final LiveViewInstance instance = instance("lv");
            capture.drain();
            capture.assertLogged("live view could not retire its older-format checkpoint timeline, the upgrade stays pending [view=lv]");
            // The rebuild's own seal met the older superblock and refused, once.
            capture.assertOnlyOnce(Pattern.quote("could not write live view head checkpoint [view=lv"));
            Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_FORMAT_UPGRADE, guard.getAbstention());
            Assert.assertEquals("upgrade_rebuild", LiveViewCheckpointRestoreRoute.name(instance.getCheckpointRestoreRoute()));
            Assert.assertTrue("the older superblock survived, so the upgrade is still owed", instance.isCheckpointUpgradeRebuildPending());
            Assert.assertFalse(instance.isInvalid());
            Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
            Assert.assertEquals(OLDER_FORMAT_VERSION, readSuperblockFormatVersion(checkpointsRootByDirName()));
            assertViewRows(ALL_ROWS);
            assertNoRefreshFaults("lv");

            // The restart reaches the upgrade route again, off the same superblock, and runs one
            // redundant rebuild over the output the first one committed: same rows, and the older
            // directory goes this time.
            shutdown();
            capture.start();
            final LiveViewRebuildRestatementGuard second = restart();
            capture.drain();
            capture.assertLogged("live view checkpoint timeline was written by an older format, rebuilding from base [view=");
            assertUpgradeRebuilt("lv");
            Assert.assertEquals(1, instance("lv").getCheckpointRebuildAttempts());
            Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_FORMAT_UPGRADE, second.getAbstention());
            capture.assertNotLogged("could not write live view head checkpoint");
            assertViewRows(ALL_ROWS);
            Assert.assertEquals(LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION, readSuperblockFormatVersion(checkpointsRootByDirName()));
            assertNoRefreshFaults("lv");

            shutdown();
            restart();
            assertRestoredFromTimeline("lv");
            assertViewRows(ALL_ROWS);
        });
    }

    @Test
    public void testAFailedUpgradeRebuildInvalidatesTheView() throws Exception {
        assertMemoryLeak(fault, () -> {
            seedSixRows();
            carryOverToOlderFormat();
            fault.of(engine.verifyTableName("tx").getDirName());

            // A failure other than apply lag and cancellation keeps the disposition the
            // missing-timeline route has: an IO fault or a dropped base needs an operator either
            // way, and the view says so durably rather than retrying a scan forever.
            engine.releaseInactive();
            fault.armAppliedScan();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            Assert.assertTrue("the rebuild's scan must have been failed", fault.hasAppliedScanFired());
            capture.drain();
            capture.assertLogged("live view restart applied-base rebuild failed [view=lv");
            final LiveViewInstance instance = instance("lv");
            Assert.assertTrue(instance.isInvalid());
            Assert.assertEquals("blocked", LiveViewCheckpointRestoreRoute.name(instance.getCheckpointRestoreRoute()));
            TestUtils.assertContains(instance.getInvalidationReason(), "live view restart timeline recovery failed");
            // Nothing committed, so the rows are the ones the view had.
            assertViewRows(ALL_ROWS);
        });
    }

    @Test
    public void testANeverMaterializedViewRetiresTheOlderTimelineWithoutRebuilding() throws Exception {
        assertMemoryLeak(() -> {
            seedSixRows();
            // A view over a base that has never committed: ACTIVE at CREATE, no row, and a processed
            // watermark below the commit it subscribes from.
            execute("CREATE TABLE tx_empty (created_at TIMESTAMP, account_id SYMBOL, amount DOUBLE) "
                    + "TIMESTAMP(created_at) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv_empty FLUSH EVERY 100ms START FROM BEGINNING AS "
                    + "SELECT created_at, account_id, sum(amount) OVER w AS s "
                    + "FROM tx_empty WINDOW w AS (PARTITION BY account_id ORDER BY created_at ANCHOR DAILY '00:00')");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            final LiveViewInstance empty = instance("lv_empty");
            Assert.assertEquals(LiveViewState.SEED_STATE_ACTIVE, empty.getStateReader().getSeedState());
            Assert.assertTrue(empty.getStateReader().getLastProcessedSeqTxn() < empty.getStateReader().getSubscribeFromSeqTxn());
            shutdown();

            // Such a view never seals, so this build never leaves it a timeline. An older build's
            // directory is what gives it one here: the other view's, carried over and stamped.
            final File emptyCheckpoints = checkpointsRootByDirName("lv_empty");
            try (
                    Path src = new Path().of(checkpointsRootByDirName("lv").getAbsolutePath()).slash();
                    Path dst = new Path().of(emptyCheckpoints.getAbsolutePath()).slash()
            ) {
                TestUtils.copyDirectory(src, dst, engine.getConfiguration().getMkDirMode());
            }
            stampOlderFormat(emptyCheckpoints);
            loadCatalogue();
            Assert.assertTrue(instance("lv_empty").isCheckpointUpgradeRebuildPending());

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            capture.drain();
            capture.assertLogged("live view carried over from an older checkpoint format holds no output, retiring its timeline [view=lv_empty]");
            capture.assertNotLogged("live view restart rebuilding from applied base [view=lv_empty");
            final LiveViewInstance reloaded = instance("lv_empty");
            Assert.assertFalse(reloaded.isCheckpointUpgradeRebuildPending());
            Assert.assertEquals("nothing needed rebuilding", 0, reloaded.getCheckpointRebuildAttempts());
            Assert.assertEquals(LiveViewCheckpointRestoreRoute.NONE, reloaded.getCheckpointRestoreRoute());
            Assert.assertFalse(reloaded.isInvalid());
            Assert.assertFalse(
                    "the older directory must be gone, or it would refuse the view's first seal",
                    new File(emptyCheckpoints, LiveViewCheckpointLayout.TIMELINE_FILE_NAME).exists()
            );

            // And the view materializes normally from its first commit, sealing this build's format.
            execute("INSERT INTO tx_empty VALUES ('2026-01-05T09:00:00.000000Z', 'acct-1', 5.0)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            capture.drain();
            capture.assertNotLogged("could not write live view head checkpoint [view=lv_empty");
            assertQuery("SELECT created_at, account_id, s FROM lv_empty")
                    .noLeakCheck()
                    .timestamp("created_at")
                    .expectSize()
                    .returns("""
                            created_at\taccount_id\ts
                            2026-01-05T09:00:00.000000Z\tacct-1\t5.0
                            """);
            Assert.assertEquals(LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION, readSuperblockFormatVersion(emptyCheckpoints));
            assertNoRefreshFaults("lv_empty");
        });
    }

    @Test
    public void testARestartBeforeTheFirstRefreshStillOwesTheUpgradeRebuild() throws Exception {
        assertMemoryLeak(() -> {
            seedSixRows();
            final File checkpointsRoot = checkpointsRootByDirName();
            carryOverToOlderFormat();
            final long filesBefore = countFiles(checkpointsRoot);

            // Pending, not blocked: the view is active, queryable, and says what it owes.
            final LiveViewInstance instance = instance("lv");
            Assert.assertTrue(instance.isCheckpointUpgradeRebuildPending());
            Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
            Assert.assertFalse(instance.isInvalid());
            Assert.assertEquals(LiveViewCheckpointRecoveryPhase.NONE, instance.getCheckpointRecoveryPhase());
            assertViewRows(ALL_ROWS);
            assertQuery("SELECT view_status, checkpoint_recovery_phase, invalidation_reason FROM live_views() WHERE view_name = 'lv'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            view_status\tcheckpoint_recovery_phase\tinvalidation_reason
                            active\tupgrade_rebuild_pending\t
                            """);

            // A stop before the first refresh turn, which is the earliest crash there is. Nothing
            // moved, so the next start reaches the same disposition off the same superblock.
            shutdown();
            loadCatalogue();
            Assert.assertTrue(instance("lv").isCheckpointUpgradeRebuildPending());
            Assert.assertEquals(filesBefore, countFiles(checkpointsRoot));
            Assert.assertEquals(OLDER_FORMAT_VERSION, readSuperblockFormatVersion(checkpointsRoot));

            final LiveViewRebuildRestatementGuard guard;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                guard = job.rebuildRestatementGuardForTest();
            }
            assertUpgradeRebuilt("lv");
            Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_FORMAT_UPGRADE, guard.getAbstention());
            assertViewRows(ALL_ROWS);
            Assert.assertEquals(LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION, readSuperblockFormatVersion(checkpointsRoot));
            assertNoRefreshFaults("lv");

            shutdown();
            restart();
            assertRestoredFromTimeline("lv");
            assertViewRows(ALL_ROWS);
        });
    }

    @Test
    public void testASeedingViewRetiresTheOlderTimelineAndReSweeps() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL, amount DOUBLE) "
                    + "TIMESTAMP(created_at) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO tx (created_at, account_id, amount) VALUES
                    ('2026-01-01T09:00:00.000000Z', 'acct-1', 1.0),
                    ('2026-01-01T09:10:00.000000Z', 'acct-2', 2.0),
                    ('2026-01-02T09:00:00.000000Z', 'acct-1', 4.0),
                    ('2026-01-02T09:10:00.000000Z', 'acct-1', 8.0),
                    ('2026-01-03T09:00:00.000000Z', 'acct-1', 16.0),
                    ('2026-01-03T09:10:00.000000Z', 'acct-2', 32.0)""");
            drainWalQueue();
            createView();

            // Two seed turns, one row and one seal each, then a stop with the view still SEEDING and
            // a mid-sweep generation on disk.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance seeding = instance("lv");
                for (int i = 0; i < 100 && seeding.getSeedDataOffset() < 2; i++) {
                    job.run();
                    drainWalQueue();
                }
                Assert.assertEquals(2, seeding.getSeedDataOffset());
                Assert.assertEquals(LiveViewState.SEED_STATE_SEEDING, seeding.getStateReader().getSeedState());
                Assert.assertNotEquals(Numbers.LONG_NULL, seeding.getSeedCheckpointDataOffset());
            }
            carryOverToOlderFormat();
            final LiveViewInstance instance = instance("lv");
            Assert.assertEquals(LiveViewState.SEED_STATE_SEEDING, instance.getStateReader().getSeedState());
            Assert.assertTrue(instance.isCheckpointUpgradeRebuildPending());

            // The seed sweep's own resume copes with a timeline it cannot read: it retires it and
            // re-sweeps from offset zero, skip-writing the rows already on disk. No whole-view
            // rebuild runs and no guard is consulted - a seed restates nothing - and the retire is
            // what clears the pending upgrade.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);
                Assert.assertEquals(
                        LiveViewRebuildRestatementGuard.ABSTAIN_NOT_EVALUATED,
                        job.rebuildRestatementGuardForTest().getAbstention()
                );
            }
            capture.drain();
            capture.assertLogged("live view retired its older-format checkpoint timeline, the next seal writes the supported format [view=lv");
            capture.assertNotLogged("live view restart rebuilding from applied base");
            capture.assertNotLogged("could not write live view head checkpoint");
            Assert.assertFalse(instance.isCheckpointUpgradeRebuildPending());
            Assert.assertEquals(0, instance.getCheckpointRebuildAttempts());
            Assert.assertEquals(2, instance.getSeedSkipWriteFloor());
            Assert.assertEquals(LiveViewState.SEED_STATE_ACTIVE, instance.getStateReader().getSeedState());
            assertViewRows(ALL_ROWS);
            Assert.assertEquals(LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION, readSuperblockFormatVersion(checkpointsRootByDirName()));
            assertNoRefreshFaults("lv");

            shutdown();
            restart();
            assertRestoredFromTimeline("lv");
            assertViewRows(ALL_ROWS);
        });
    }

    @Test
    public void testAnUnlinkFaultOnASegmentDirectoryStillClearsThePendingUpgrade() throws Exception {
        assertMemoryLeak(fault, () -> {
            seedSixRows();
            carryOverToOlderFormat();

            // The retire removes _timeline first, so a fault further down leaves a directory that
            // declares no format at all: nothing a restart would rebuild over, and nothing a seal
            // refuses. The pending upgrade clears on _timeline alone, and the leftover segments are
            // orphans the next seal's reconciliation collects.
            fault.armMetaDirRemove();
            drive();
            Assert.assertTrue(fault.hasMetaDirRemoveFired());
            capture.drain();
            capture.assertLogged("live view retired its older-format checkpoint timeline, the next seal writes the supported format [view=lv");
            capture.assertNotLogged("could not write live view head checkpoint");
            assertUpgradeRebuilt("lv");
            assertViewRows(ALL_ROWS);
            Assert.assertEquals(LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION, readSuperblockFormatVersion(checkpointsRootByDirName()));
            assertNoRefreshFaults("lv");

            shutdown();
            restart();
            assertRestoredFromTimeline("lv");
            assertViewRows(ALL_ROWS);
            assertNoRefreshFaults("lv");
        });
    }

    @Test
    public void testAnUnlinkFaultOnTheOlderTimelineEndsThroughRepeatedSealFailures() throws Exception {
        assertMemoryLeak(fault, () -> {
            seedSixRows();
            carryOverToOlderFormat();
            fault.armTimelineRemove();
            final LiveViewInstance instance = instance("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                Assert.assertTrue(fault.hasTimelineRemoveFired());
                Assert.assertTrue(instance.isCheckpointUpgradeRebuildPending());

                // No retry loop: the view keeps refreshing, and every seal refuses to publish over
                // the older superblock that survived. The third refusal in a row retires the
                // timeline through the ordinary seal-failure path, and that retire clears the
                // pending upgrade.
                insertAndRefresh(job, "('2026-01-03T10:00:00.000000Z', 'acct-1', 64.0)");
                Assert.assertTrue(instance.isCheckpointUpgradeRebuildPending());
                insertAndRefresh(job, "('2026-01-03T10:10:00.000000Z', 'acct-2', 128.0)");
                capture.drain();
                capture.assertLogged("live view checkpoint seal keeps failing, retiring the timeline and backing off [view=lv, consecutiveFailures=3");
                Assert.assertFalse("the seal-failure retire must end the pending upgrade", instance.isCheckpointUpgradeRebuildPending());
                Assert.assertFalse(instance.isInvalid());
                Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
                Assert.assertFalse(new File(checkpointsRootByDirName(), LiveViewCheckpointLayout.TIMELINE_FILE_NAME).exists());

                // Past the seal cooldown, the next flush seals this build's format.
                setCurrentMicros(currentMicros + 2 * Micros.MINUTE_MICROS);
                insertAndRefresh(job, "('2026-01-03T10:20:00.000000Z', 'acct-1', 256.0)");
            }
            Assert.assertEquals(LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION, readSuperblockFormatVersion(checkpointsRootByDirName()));
            Assert.assertEquals("no refresh turn may have faulted", 0, instance.getRefreshFaultCount());
            Assert.assertEquals("the upgrade rebuild runs once", 1, instance.getCheckpointRebuildAttempts());
            assertViewRows(ALL_ROWS
                    + "2026-01-03T10:00:00.000000Z\tacct-1\t80.0\t2\n"
                    + "2026-01-03T10:10:00.000000Z\tacct-2\t160.0\t2\n"
                    + "2026-01-03T10:20:00.000000Z\tacct-1\t336.0\t3\n");

            shutdown();
            restart();
            assertRestoredFromTimeline("lv");
            assertNoRefreshFaults("lv");
        });
    }

    @Test
    public void testAnUpgradeRebuildOverABaseThatLostAMiddleDayRestatesItWithoutALogLine() throws Exception {
        assertMemoryLeak(() -> {
            seedSixRows();
            // The view keeps its rows for the day, as incremental refresh always has.
            execute("ALTER TABLE tx DROP PARTITION LIST '2026-01-02'");
            drainWalQueue();
            drive();
            assertViewRows(ALL_ROWS);
            carryOverToOlderFormat();

            final LiveViewRebuildRestatementGuard guard = drive();

            // On any other route the guard's row shortfall refuses this rebuild. The upgrade route
            // compares only the history floor, which a middle day leaves intact, so the rows go and
            // no line says so. This is the documented cost of rebuilding every carried-over view.
            capture.drain();
            capture.assertNotLogged("live view upgrade rebuild restates rows the view retained");
            capture.assertNotLogged("live view rebuild from the applied base refused");
            assertUpgradeRebuilt("lv");
            Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_FORMAT_UPGRADE, guard.getAbstention());
            Assert.assertEquals(LiveViewRebuildRestatementGuard.VERDICT_NONE, guard.getVerdict());
            assertViewRows("""
                    created_at\taccount_id\tcumulative_sum\tcumulative_count
                    2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
                    2026-01-01T09:10:00.000000Z\tacct-2\t2.0\t1
                    2026-01-03T09:00:00.000000Z\tacct-1\t16.0\t1
                    2026-01-03T09:10:00.000000Z\tacct-2\t32.0\t1
                    """);
            assertNoRefreshFaults("lv");
        });
    }

    @Test
    public void testAnUpgradeRebuildOverABaseThatLostItsOldestDayLogsTheRestatement() throws Exception {
        assertMemoryLeak(() -> {
            seedSixRows();
            execute("ALTER TABLE tx DROP PARTITION LIST '2026-01-01'");
            drainWalQueue();
            drive();
            assertViewRows(ALL_ROWS);
            carryOverToOlderFormat();

            final LiveViewRebuildRestatementGuard guard = drive();

            // Refused on the missing-timeline route, off the same two transaction files; logged and
            // run on this one.
            capture.drain();
            capture.assertLogged("live view upgrade rebuild restates rows the view retained [view=lv"
                    + ", viewMinTs=2026-01-01T09:00:00.000000Z, baseRows=4, baseMinTs=2026-01-02T09:00:00.000000Z]");
            capture.assertNotLogged("live view rebuild from the applied base refused");
            assertUpgradeRebuilt("lv");
            Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_FORMAT_UPGRADE, guard.getAbstention());
            assertViewRows("""
                    created_at\taccount_id\tcumulative_sum\tcumulative_count
                    2026-01-02T09:00:00.000000Z\tacct-1\t4.0\t1
                    2026-01-02T09:10:00.000000Z\tacct-1\t12.0\t2
                    2026-01-03T09:00:00.000000Z\tacct-1\t16.0\t1
                    2026-01-03T09:10:00.000000Z\tacct-2\t32.0\t1
                    """);
            assertNoRefreshFaults("lv");

            // A restart restores from the rebuilt view's own seal: the view no longer holds the day
            // the base lost, so nothing asks about it again.
            shutdown();
            restart();
            assertRestoredFromTimeline("lv");
            Assert.assertFalse(instance("lv").isCheckpointRecoveryBlocked());
        });
    }

    @Test
    public void testAnUpgradeRebuildWaitsForTheBaseApplyInsteadOfInvalidating() throws Exception {
        assertMemoryLeak(() -> {
            seedSixRows();
            final TableToken baseToken = engine.verifyTableName("tx");
            final long baseApplied = instance("lv").getLastProcessedSeqTxn();
            // The view flushes a commit the base has not applied - it drains the raw WAL - and the
            // base's apply then stops, which is the backlog a first start after an upgrade can
            // meet.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(instance("lv").getLastFlushTimeUs() + CLOCK_ADVANCE_MICROS);
                execute("INSERT INTO tx VALUES ('2026-01-03T10:00:00.000000Z', 'acct-1', 64.0)");
                drainJob(job);
            }
            Assert.assertEquals(baseApplied + 1, instance("lv").getLastProcessedSeqTxn());
            Assert.assertEquals(baseApplied, engine.getTableSequencerAPI().getTxnTracker(baseToken).getWriterTxn());
            execute("ALTER TABLE tx SUSPEND WAL");
            carryOverToOlderFormat();

            // The restart route would wait for the apply in place and invalidate the view durably
            // once the flush-retry budget ran out. The upgrade defers instead, ahead of the restore
            // attempt, so the attempt is still unmade on the turn that retries it.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int i = 0; i < 3; i++) {
                    setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                    drainJob(job);
                }
                final LiveViewInstance instance = instance("lv");
                capture.drain();
                capture.assertOnlyOnce(Pattern.quote("live view rebuild from the applied base waits for the base table to apply what the view consumed "
                        + "[view=lv, cause=" + UPGRADE_CAUSE + ", rebuildSeqTxn=" + (baseApplied + 1)
                        + ", appliedSeqTxn=" + baseApplied + "]"));
                capture.assertNotLogged("live view restart rebuilding from applied base");
                Assert.assertFalse("the attempt must not burn on a deferral", instance.isCheckpointRestoreAttempted());
                Assert.assertTrue(instance.isCheckpointUpgradeRebuildPending());
                Assert.assertTrue(instance.isCheckpointRebuildDeferred());
                Assert.assertFalse(instance.isInvalid());
                Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
                Assert.assertEquals(0, instance.getRefreshFaultCount());
                Assert.assertEquals(0, instance.getFlushRetryCount());
                Assert.assertEquals(0, instance.getCheckpointRebuildAttempts());
                Assert.assertEquals(OLDER_FORMAT_VERSION, readSuperblockFormatVersion(checkpointsRootByDirName()));
                // The deferral outranks the pending upgrade in live_views(), and its reason names it.
                assertQuery("SELECT view_status, checkpoint_recovery_phase, base_apply_wait_seqtxn "
                        + "FROM live_views() WHERE view_name = 'lv'")
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("view_status\tcheckpoint_recovery_phase\tbase_apply_wait_seqtxn\n"
                                + "active\trebuild_deferred\t" + (baseApplied + 1) + "\n");
                TestUtils.assertContains(instance.getCheckpointRecoveryReason(), "[cause=" + UPGRADE_CAUSE);

                // The apply resumes and the next turn past the back-off runs the rebuild.
                execute("ALTER TABLE tx RESUME WAL");
                driveRefreshToQuiescence(job);
                Assert.assertEquals(
                        LiveViewRebuildRestatementGuard.ABSTAIN_FORMAT_UPGRADE,
                        job.rebuildRestatementGuardForTest().getAbstention()
                );
            }
            assertUpgradeRebuilt("lv");
            final LiveViewInstance instance = instance("lv");
            Assert.assertFalse(instance.isCheckpointRebuildDeferred());
            Assert.assertEquals(Numbers.LONG_NULL, instance.getApplyLagDeferTargetSeqTxn());
            assertViewRows(ALL_ROWS + "2026-01-03T10:00:00.000000Z\tacct-1\t80.0\t2\n");
            assertQuery("SELECT view_status, checkpoint_recovery_phase, checkpoint_recovery_reason "
                    + "FROM live_views() WHERE view_name = 'lv'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("view_status\tcheckpoint_recovery_phase\tcheckpoint_recovery_reason\n"
                            + "active\t\t\n");
            assertNoRefreshFaults("lv");
        });
    }

    private static void putLeInt(byte[] bytes, int offset, int value) {
        bytes[offset] = (byte) value;
        bytes[offset + 1] = (byte) (value >>> 8);
        bytes[offset + 2] = (byte) (value >>> 16);
        bytes[offset + 3] = (byte) (value >>> 24);
    }

    private static void putLeLong(byte[] bytes, int offset, long value) {
        putLeInt(bytes, offset, (int) value);
        putLeInt(bytes, offset + 4, (int) (value >>> 32));
    }

    /**
     * Rewrites both superblock slots to name the older format version the way a build of that
     * version stamps one - in the version field and in the magic's trailing nibble, checksum and
     * all - which is the shape of a released 10.0.x slot.
     */
    private static void stampOlderFormat(File checkpointsRoot) throws IOException {
        final File timeline = new File(checkpointsRoot, LiveViewCheckpointLayout.TIMELINE_FILE_NAME);
        final byte[] bytes = Files.readAllBytes(timeline.toPath());
        Assert.assertTrue("the view must have sealed a timeline to carry over", bytes.length >= 2 * LiveViewCheckpointSuperblock.SLOT_SIZE);
        for (int slot = 0; slot < 2; slot++) {
            final int base = slot * LiveViewCheckpointSuperblock.SLOT_SIZE;
            putLeLong(bytes, base + LiveViewCheckpointSuperblock.SLOT_MAGIC_OFFSET, LiveViewCheckpointSuperblock.SLOT_MAGIC_FAMILY | OLDER_FORMAT_VERSION);
            putLeInt(bytes, base + LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION_OFFSET, OLDER_FORMAT_VERSION);
            final CRC32 crc = new CRC32();
            crc.update(bytes, base, LiveViewCheckpointSuperblock.SLOT_CRC_COVERAGE);
            putLeInt(bytes, base + LiveViewCheckpointSuperblock.SLOT_CRC_OFFSET, (int) crc.getValue());
        }
        Files.write(timeline.toPath(), bytes);
    }

    private void assertViewRows(String expected) throws Exception {
        assertQuery(VIEW_ROWS_QUERY)
                .noLeakCheck()
                .timestamp("created_at")
                .expectSize()
                .returns(expected);
    }

    /**
     * Stops the process, stamps the view's timeline with the older format, and loads the catalogue
     * again, which is where the upgrade is decided. No refresh turn runs.
     */
    private void carryOverToOlderFormat() throws IOException {
        shutdown();
        stampOlderFormat(checkpointsRootByDirName());
        loadCatalogue();
    }

    private File checkpointsRootByDirName() {
        return checkpointsRootByDirName("lv");
    }

    private File checkpointsRootByDirName(String viewName) {
        return new File(
                new File(engine.getConfiguration().getDbRoot(), engine.verifyTableName(viewName).getDirName()),
                LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME
        );
    }

    private void createView() throws Exception {
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                + "SELECT created_at, account_id, sum(amount) OVER w AS cumulative_sum, "
                + "count(account_id) OVER w AS cumulative_count "
                + "FROM tx WINDOW w AS (PARTITION BY account_id ORDER BY created_at ANCHOR DAILY '00:00')");
    }

    /**
     * Drives the refresh job to quiescence and returns what the last whole-view rebuild's guard
     * found.
     */
    private LiveViewRebuildRestatementGuard drive() {
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveRefreshToQuiescence(job);
            return job.rebuildRestatementGuardForTest();
        }
    }

    private void insertAndRefresh(LiveViewRefreshJob job, String values) throws Exception {
        execute("INSERT INTO tx VALUES " + values);
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    private void loadCatalogue() {
        engine.buildViewGraphs();
    }

    private LiveViewRebuildRestatementGuard restart() {
        loadCatalogue();
        return drive();
    }

    /**
     * Six rows over three days, one commit each, so the timeline holds one boundary per row and
     * every base partition holds rows the view has derived output from.
     */
    private void seedSixRows() throws Exception {
        execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL, amount DOUBLE) "
                + "TIMESTAMP(created_at) PARTITION BY DAY WAL");
        createView();
        final String[] rows = {
                "'2026-01-01T09:00:00.000000Z', 'acct-1', 1.0",
                "'2026-01-01T09:10:00.000000Z', 'acct-2', 2.0",
                "'2026-01-02T09:00:00.000000Z', 'acct-1', 4.0",
                "'2026-01-02T09:10:00.000000Z', 'acct-1', 8.0",
                "'2026-01-03T09:00:00.000000Z', 'acct-1', 16.0",
                "'2026-01-03T09:10:00.000000Z', 'acct-2', 32.0"
        };
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveSeedToCompletion(job, "lv");
            for (String row : rows) {
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES (" + row + ")");
                drainWalQueue();
                driveRefreshToQuiescence(job);
            }
        }
        assertViewRows(ALL_ROWS);
        assertNoRefreshFaults("lv");
    }

    /**
     * Releases everything that maps the view's and the base's files, the way a stopped process
     * would, so the next catalogue load starts from disk.
     */
    private void shutdown() {
        engine.getLiveViewRegistry().clear();
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        engine.releaseInactive();
    }

    /**
     * Fails, once each and only when armed: the unlink of a view's {@code _timeline}, the removal of
     * its checkpoint {@code meta} directory, and the open of the base's {@code amount} column that
     * the whole-view rebuild's scan of the applied base makes and the raw-WAL drain never does.
     */
    private static final class UpgradeFault extends TestFilesFacadeImpl {
        private final AtomicBoolean isAppliedScanArmed = new AtomicBoolean();
        private final AtomicBoolean isAppliedScanFired = new AtomicBoolean();
        private final AtomicBoolean isMetaDirRemoveArmed = new AtomicBoolean();
        private final AtomicBoolean isMetaDirRemoveFired = new AtomicBoolean();
        private final AtomicBoolean isTimelineRemoveArmed = new AtomicBoolean();
        private final AtomicBoolean isTimelineRemoveFired = new AtomicBoolean();
        private volatile String baseDir;

        @Override
        public long openRO(LPSZ name) {
            final String dir = baseDir;
            if (dir != null
                    && isAppliedScanArmed.get()
                    && Utf8s.containsAscii(name, dir)
                    && !Utf8s.containsAscii(name, "wal")
                    && Utf8s.endsWithAscii(name, "amount.d")
                    && isAppliedScanArmed.compareAndSet(true, false)) {
                isAppliedScanFired.set(true);
                return -1;
            }
            return super.openRO(name);
        }

        @Override
        public boolean removeQuiet(LPSZ name) {
            if (isTimelineRemoveArmed.get()
                    && Utf8s.endsWithAscii(name, LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME + File.separator + LiveViewCheckpointLayout.TIMELINE_FILE_NAME)
                    && isTimelineRemoveArmed.compareAndSet(true, false)) {
                isTimelineRemoveFired.set(true);
                return false;
            }
            return super.removeQuiet(name);
        }

        @Override
        public boolean rmdir(Path name, boolean haltOnError) {
            if (isMetaDirRemoveArmed.get()
                    && Utf8s.endsWithAscii(name, LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME + File.separator + LiveViewCheckpointLayout.META_DIR_NAME)
                    && isMetaDirRemoveArmed.compareAndSet(true, false)) {
                isMetaDirRemoveFired.set(true);
                return false;
            }
            return super.rmdir(name, haltOnError);
        }

        void armAppliedScan() {
            isAppliedScanFired.set(false);
            isAppliedScanArmed.set(true);
        }

        void armMetaDirRemove() {
            isMetaDirRemoveFired.set(false);
            isMetaDirRemoveArmed.set(true);
        }

        void armTimelineRemove() {
            isTimelineRemoveFired.set(false);
            isTimelineRemoveArmed.set(true);
        }

        boolean hasAppliedScanFired() {
            return isAppliedScanFired.get();
        }

        boolean hasMetaDirRemoveFired() {
            return isMetaDirRemoveFired.get();
        }

        boolean hasTimelineRemoveFired() {
            return isTimelineRemoveFired.get();
        }

        void of(String baseDir) {
            this.baseDir = baseDir;
        }
    }
}
