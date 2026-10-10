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
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointSuperblock;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRebuildRestatementGuard;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * What this build does with a checkpoint tree the released 10.0.1 build wrote: it rebuilds the
 * view from its base table, on the view's first refresh, with no operator action.
 * <p>
 * 10.0.1 sealed an anchored view's state as a separate anchor root ({@code PAGE_KIND = 0x1b})
 * plus a function root per window call ({@code 0x18}). This build publishes one fused
 * {@code LiveViewCheckpointWindowRoot} ({@code 0x1d}) instead and carries no decoder for the
 * older shape at all, so it declares a higher {@code SLOT_FORMAT_VERSION}. Live views were beta in
 * 10.0.x, so a view that build left behind is not stopped for the operator to re-create: this
 * build runs the same recompute a DROP and re-create would, from the base rows available today,
 * and retires the released directory once the rebuild's replacement has committed.
 * <p>
 * That recompute is also what it costs. A base that lost rows the view retained - TTL,
 * DROP/DETACH PARTITION, TRUNCATE - comes back without them, and the restatement guard that
 * refuses such a rebuild on every other route stands down for this one; the view logs what it can
 * see of the loss and keeps refreshing. One case here pins that ending too.
 * <p>
 * {@link LiveViewCheckpointUpgradeRebuildTest} pins the crash, fault and deferral behaviour of the
 * same route against a synthetic older version - a superblock that suite stamped. This class pins
 * it against a version no test wrote: a whole database root emitted by an unmodified 10.0.1
 * checkout and never touched by a writer from this branch.
 * <p>
 * To regenerate the fixture, copy {@code /lv/LiveViewReleaseFixtureGenerator.java.txt} into a
 * clean {@code 10.0.1} checkout's {@code io.questdb.test.cairo.lv} package and run it; the
 * constants below are the values it prints.
 */
public class LiveViewCheckpointReleaseCompatTest extends AbstractLiveViewCheckpointCompatTest {

    private static final String DAILY_ANCHOR = "2026-01-01T";
    // The simulated clock the fixture's own run left behind. This one starts above it, so the
    // flush cadence reads a forward-moving clock rather than one that jumped backwards.
    private static final long FIXTURE_END_MICROS = 2_500_000L;
    private static final String FIXTURE_RESOURCE = "/lv/lv_checkpoint_10_0_1.zip";
    // The layout version 10.0.1 stamped. Pinned rather than derived from this build's own
    // constant, so a later bump cannot quietly redefine what the fixture is.
    private static final int RELEASED_FORMAT_VERSION = 1;
    private static final LogCapture capture = new LogCapture();

    @After
    public void resetClock() {
        capture.stop();
        setCurrentMicros(-1);
    }

    @Before
    public void setUpCadence() {
        // Matches the cadence the fixture was sealed under, so a commit made after the upgrade
        // seals a boundary of its own rather than waiting for a row budget to fill.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setCurrentMicros(2 * FIXTURE_END_MICROS);
        capture.start();
    }

    @Test
    public void testAReleasedCheckpointTreeRebuildsTheViewFromItsBase() throws Exception {
        assertMemoryLeak(() -> {
            final long checkpointFilesBefore = openFixture();
            final File checkpointsRoot = checkpointsRoot("lv");

            // The premise, read out of the released bytes rather than out of the decision under
            // test: this really is a directory an older build wrote in a version this one does not
            // implement.
            Assert.assertEquals(
                    "the fixture must declare the version 10.0.1 stamped",
                    RELEASED_FORMAT_VERSION,
                    readSuperblockFormatVersion(checkpointsRoot)
            );
            Assert.assertTrue(
                    "a fixture that does not declare an older version cannot exercise the upgrade",
                    RELEASED_FORMAT_VERSION < LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION
            );

            // The upgrade arm, not the block and not the reset. The catalogue load decides it off
            // the superblock alone, and rebuilds nothing yet.
            capture.drain();
            capture.assertLogged("live view checkpoint timeline was written by an older format, rebuilding from base [view=");
            capture.assertNotLogged("live view checkpoint timeline declares an unsupported format version");
            capture.assertNotLogged("live view checkpoint timeline carries a foreign layout version");
            capture.assertNotLogged("live view restart rebuilding from applied base");

            final LiveViewInstance instance = instance("lv");
            Assert.assertTrue(instance.isCheckpointUpgradeRebuildPending());
            Assert.assertFalse("an upgrade is not a block", instance.isCheckpointRecoveryBlocked());
            Assert.assertFalse(instance.isInvalid());
            Assert.assertEquals(
                    "the catalogue load must not move a file of the released directory",
                    checkpointFilesBefore,
                    countFiles(checkpointsRoot)
            );
            // Until its first refresh turn the view serves the rows the released build
            // materialized, and reports itself active with the rebuild it owes.
            assertReleasedRows();
            assertQuery("SELECT view_status, checkpoint_recovery_phase, invalidation_reason, checkpoint_recovery_reason "
                    + "FROM live_views() WHERE view_name = 'lv'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("view_status\tcheckpoint_recovery_phase\tinvalidation_reason\tcheckpoint_recovery_reason\n"
                            + "active\tupgrade_rebuild_pending\t\t" + instance.getCheckpointUpgradeRebuildReason() + "\n");
            TestUtils.assertContains(
                    instance.getCheckpointUpgradeRebuildReason(),
                    "checkpoint timeline format version " + RELEASED_FORMAT_VERSION + " was written by an older build"
            );

            final LiveViewRebuildRestatementGuard guard;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                guard = job.rebuildRestatementGuardForTest();
            }
            capture.drain();
            capture.assertLogged("live view restart rebuilding from applied base [view=lv, cause=timeline format upgrade");
            capture.assertLogged("live view retired its older-format checkpoint timeline, the next seal writes the supported format [view=lv");
            // The rebuild retired the released directory before its own post-replay seal, so no
            // seal ever met the older superblock.
            capture.assertNotLogged("could not write live view head checkpoint");
            // Nothing below the superblock was ever decoded.
            capture.assertNotLogged("could not restore live view from checkpoint timeline");
            // The base still holds every row the view was built from, so nothing was restated.
            capture.assertNotLogged("live view upgrade rebuild restates rows the view retained");
            assertUpgradeRebuilt("lv");
            Assert.assertEquals(1, instance.getCheckpointRebuildAttempts());
            Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_FORMAT_UPGRADE, guard.getAbstention());
            Assert.assertEquals(LiveViewRebuildRestatementGuard.VERDICT_NONE, guard.getVerdict());
            assertNoRefreshFaults("lv");
            assertReleasedRows();
            assertViewMatchesRecompute();

            // The rebuild's seal wrote this build's format in place of the released one.
            Assert.assertEquals(
                    LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION,
                    readSuperblockFormatVersion(checkpointsRoot)
            );
            try (LiveViewCheckpointMetaStore store = openStore(instance)) {
                Assert.assertTrue("the rebuilt view must publish a generation this build adopts", store.isValid());
            }
            assertQuery("SELECT view_status, checkpoint_recovery_phase, checkpoint_recovery_reason "
                    + "FROM live_views() WHERE view_name = 'lv'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("view_status\tcheckpoint_recovery_phase\tcheckpoint_recovery_reason\n"
                            + "active\t\t\n");

            // An ordinary view from here: a base commit refreshes incrementally, with no second
            // rebuild, and a restart restores from the timeline the upgrade sealed.
            execute("INSERT INTO tx VALUES ('" + timestamp(50) + "', 'acct-1', 100.0)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            Assert.assertEquals("the new commit must not rebuild", 1, instance.getCheckpointRebuildAttempts());
            assertViewMatchesRecompute();
            assertNoRefreshFaults("lv");

            restartCycle();
            Assert.assertFalse(instance("lv").isCheckpointUpgradeRebuildPending());
            assertRestoredFromTimeline("lv");
            assertViewMatchesRecompute();
            assertNoRefreshFaults("lv");
        });
    }

    @Test
    public void testAReleasedViewWhoseBaseLostRowsRebuildsWithoutThem() throws Exception {
        assertMemoryLeak(() -> {
            openFixture();
            Assert.assertTrue(instance("lv").isCheckpointUpgradeRebuildPending());

            // The base loses the hour the view's every row came from before the view's first
            // refresh turn, which is what an upgrade meets after TTL or DROP PARTITION ran on a
            // base whose view kept its rows. A later hour's row keeps the base from being empty.
            execute("INSERT INTO tx VALUES ('2026-01-01T10:00:00.000000Z', 'acct-1', 100.0)");
            drainWalQueue();
            execute("ALTER TABLE tx DROP PARTITION LIST '2026-01-01T09'");
            drainWalQueue();
            assertReleasedRows();

            final LiveViewRebuildRestatementGuard guard;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                guard = job.rebuildRestatementGuardForTest();
            }

            // The rebuild ran rather than refused - the guard would have refused it on any other
            // route, off the same two transaction files - and the one thing that says the view
            // lost rows is the advisory line.
            capture.drain();
            capture.assertLogged("live view upgrade rebuild restates rows the view retained [view=lv"
                    + ", viewMinTs=2026-01-01T09:00:00.000000Z, baseRows=1, baseMinTs=2026-01-01T10:00:00.000000Z]");
            capture.assertNotLogged("live view rebuild from the applied base refused");
            assertUpgradeRebuilt("lv");
            Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_FORMAT_UPGRADE, guard.getAbstention());
            assertNoRefreshFaults("lv");

            // Fewer rows than the released view served: a recompute from what the base holds now.
            assertQuery("SELECT created_at, account_id, cumulative_sum, cumulative_count FROM lv")
                    .noLeakCheck()
                    .timestamp("created_at")
                    .expectSize()
                    .returns("created_at\taccount_id\tcumulative_sum\tcumulative_count\n" +
                            "2026-01-01T10:00:00.000000Z\tacct-1\t100.0\t1\n");
            assertViewMatchesRecompute();
            Assert.assertEquals(
                    LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION,
                    readSuperblockFormatVersion(checkpointsRoot("lv"))
            );

            // Settled: a restart restores from the rebuilt view's own seal, with no guard asking
            // about the hour the base lost, because the view no longer holds it.
            restartCycle();
            assertRestoredFromTimeline("lv");
            assertViewMatchesRecompute();
            assertNoRefreshFaults("lv");
        });
    }

    @Test
    public void testTheOperatorsReCreateStillWorksOnAReleasedView() throws Exception {
        assertMemoryLeak(() -> {
            openFixture();
            Assert.assertTrue(instance("lv").isCheckpointUpgradeRebuildPending());

            // The exit a 10.0.x view used to need, still available to an operator who would
            // rather re-create than wait for the first refresh turn. SHOW CREATE LIVE VIEW has to
            // work on a view that has not rebuilt yet, and its output has to re-execute.
            printSql("SHOW CREATE LIVE VIEW lv;");
            final String releasedDdl = sink.toString().replace("ddl\n", "");
            execute("DROP LIVE VIEW lv");
            execute(releasedDdl);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);
            }

            final LiveViewInstance recreated = instance("lv");
            Assert.assertFalse("the released directory went with the dropped view", recreated.isCheckpointUpgradeRebuildPending());
            Assert.assertFalse(recreated.isCheckpointRecoveryBlocked());
            Assert.assertFalse(recreated.isInvalid());
            Assert.assertEquals("a re-created view seeds rather than rebuilds", 0, recreated.getCheckpointRebuildAttempts());
            assertNoRefreshFaults("lv");

            // A fresh directory, declaring this build's version.
            Assert.assertEquals(
                    LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION,
                    readSuperblockFormatVersion(checkpointsRoot("lv"))
            );
            assertViewMatchesRecompute();

            execute("INSERT INTO tx VALUES ('" + timestamp(50) + "', 'acct-1', 100.0)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            assertViewMatchesRecompute();
            restartCycle();
            assertRestoredFromTimeline("lv");
            assertViewMatchesRecompute();
            assertNoRefreshFaults("lv");
        });
    }

    private static String timestamp(int secondOfDay) {
        return DAILY_ANCHOR + String.format("09:%02d:%02d.000000Z", secondOfDay / 60, secondOfDay % 60);
    }

    /**
     * Asserts the view serves exactly the rows the released build materialized. Read through the
     * ordinary cursor, so it covers what a user querying the view gets.
     */
    private void assertReleasedRows() throws Exception {
        assertQuery("SELECT created_at, account_id, cumulative_sum, cumulative_count FROM lv")
                .noLeakCheck()
                .timestamp("created_at")
                .expectSize()
                .returns("created_at\taccount_id\tcumulative_sum\tcumulative_count\n" +
                        "2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1\n" +
                        "2026-01-01T09:00:10.000000Z\tacct-2\t11.0\t1\n" +
                        "2026-01-01T09:00:20.000000Z\tacct-1\t22.0\t2\n" +
                        "2026-01-01T09:00:30.000000Z\tacct-2\t42.0\t2\n" +
                        "2026-01-01T09:00:40.000000Z\tacct-1\t63.0\t3\n");
    }

    /**
     * Compares the view against a from-base recompute of the same window. ANCHOR is live-view
     * syntax, so the daily bucket is written out as an ordinary partition term.
     */
    private void assertViewMatchesRecompute() throws Exception {
        final String bucket = "timestamp_floor('1d', created_at, '1970-01-01T00:00:00.000000Z'::timestamp)";
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(select created_at, account_id, "
                        + "sum(amount) over (partition by account_id, bucket order by created_at "
                        + "rows between unbounded preceding and current row) as cumulative_sum, "
                        + "count(account_id) over (partition by account_id, bucket order by created_at "
                        + "rows between unbounded preceding and current row) as cumulative_count "
                        + "from (select created_at, account_id, amount, " + bucket + " as bucket from tx)"
                        + ") order by 2, 1",
                "(lv) order by 2, 1",
                LOG,
                true
        );
    }

    /**
     * Unpacks the fixture and loads its catalogue, which is where the upgrade is decided, and
     * reports the file count the released tree arrived with. The count is taken before the
     * catalogue load so it is the released inventory rather than one this build has already had
     * an opportunity to change.
     */
    private long openFixture() throws IOException {
        replaceDbContent(FIXTURE_RESOURCE);
        final File checkpointsRoot = new File(
                new File(engine.getConfiguration().getDbRoot(), engine.getTableTokenIfExists("lv").getDirName()),
                LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME
        );
        final long files = countFiles(checkpointsRoot);
        engine.buildViewGraphs();
        return files;
    }

    private void restartCycle() throws Exception {
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
        try (LiveViewRefreshJob resumed = new LiveViewRefreshJob(0, engine, 1)) {
            driveRefreshToQuiescence(resumed);
        }
    }
}
