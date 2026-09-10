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
import io.questdb.cairo.lv.LiveViewCheckpointRestoreRoute;
import io.questdb.cairo.lv.LiveViewCheckpointSuperblock;
import io.questdb.cairo.lv.LiveViewInstance;
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
 * What this build does with a checkpoint tree the released 10.0.1 build wrote: it blocks.
 * <p>
 * 10.0.1 sealed an anchored view's state as a separate anchor root ({@code PAGE_KIND = 0x1b})
 * plus a function root per window call ({@code 0x18}). This build publishes one fused
 * {@code LiveViewCheckpointWindowRoot} ({@code 0x1d}) instead and carries no decoder for the
 * older shape at all, so it declares a higher {@code SLOT_FORMAT_VERSION} and stops at any
 * directory that declares another one. The tree is preserved to the file, the rows it
 * materialized are still served, and the way out is the operator's.
 * <p>
 * That is a deliberate reversal of what this class used to assert. The earlier cut restored the
 * released roots through a retained decoder and converted them on the next seal; the decoder is
 * gone with the layout it read. Blocking rather than rebuilding is the point: this build cannot
 * show that replaying today's surviving base rows reproduces the output those roots stand for,
 * because TTL, DROP/DETACH PARTITION and TRUNCATE all take source rows a live view keeps its own
 * output for. So it stops and says so, and the operator decides.
 * <p>
 * {@link LiveViewCheckpointForwardCompatTest} pins the same block against a synthetic version -
 * a superblock this suite stamped. This class is the one that pins it against a version no test
 * wrote: a whole database root emitted by an unmodified 10.0.1 checkout and never touched by a
 * writer from this branch. A block that only ever fired on bytes the suite forged would prove
 * nothing about a real upgrade.
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
    public void testAReleasedCheckpointTreeBlocksTheViewAndKeepsEveryFile() throws Exception {
        assertMemoryLeak(() -> {
            final long checkpointFilesBefore = openFixture();
            final File checkpointsRoot = checkpointsRoot("lv");

            // The premise, read out of the released bytes rather than out of the decision under
            // test: this really is a directory another build wrote in a version this one does not
            // implement.
            Assert.assertEquals(
                    "the fixture must declare the version 10.0.1 stamped",
                    RELEASED_FORMAT_VERSION,
                    readSuperblockFormatVersion(checkpointsRoot)
            );
            Assert.assertNotEquals(
                    "a fixture that declares this build's own version cannot exercise the boundary",
                    LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION,
                    RELEASED_FORMAT_VERSION
            );

            // The boundary gate, not the reset one. A version this build does not implement is
            // another build's generation announcing itself, and the directory is held for it.
            capture.drain();
            capture.assertLogged("live view checkpoint timeline declares an unsupported format version");
            capture.assertNotLogged("live view checkpoint timeline carries a foreign layout version");
            capture.assertNotLogged("live view restart rebuilding from applied base");

            final LiveViewInstance instance = instance("lv");
            Assert.assertTrue(instance.isCheckpointRecoveryBlocked());
            // Not a durable invalidation - _lv.s.invalid stays clear, so a build that does read
            // the released layout would resume this view with no operator action.
            Assert.assertFalse("blocking must not write _lv.s.invalid", instance.isInvalid());
            TestUtils.assertContains(
                    instance.getCheckpointRecoveryReason(),
                    "checkpoint timeline format version " + RELEASED_FORMAT_VERSION
                            + " is not supported by this build"
            );

            // The refresh turn declined ahead of every other guard, so nothing opened a root.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            Assert.assertEquals(
                    "upgrade_blocked",
                    LiveViewCheckpointRestoreRoute.name(instance.getCheckpointRestoreRoute())
            );
            Assert.assertFalse(instance.isCheckpointRestoreAttempted());
            Assert.assertEquals(0, instance.getCheckpointRebuildAttempts());
            Assert.assertEquals(0, instance.getCheckpointTimelineResets());
            Assert.assertEquals(
                    "not one file of the released directory may move",
                    checkpointFilesBefore,
                    countFiles(checkpointsRoot)
            );
            // And nothing below the superblock is reachable from here, which is what makes the
            // file count the whole of what this build can say about the tree: the slots declare a
            // version it does not implement, so its own meta store adopts no generation and the
            // ladder underneath is neither read nor retired.
            try (LiveViewCheckpointMetaStore store = openStore(instance)) {
                Assert.assertFalse(
                        "this build must not adopt a generation out of a blocked timeline",
                        store.isValid()
                );
            }
            assertNoRefreshFaults("lv");

            // The rows the released build materialized are still served, and a new base commit
            // does not move them: refresh is stopped, not merely restore.
            final long processedBefore = instance.getLastProcessedSeqTxn();
            assertReleasedRows();
            execute("INSERT INTO tx VALUES ('" + timestamp(50) + "', 'acct-1', 100.0)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int i = 0; i < 4; i++) {
                    drainJob(job);
                    drainWalQueue();
                }
            }
            assertReleasedRows();
            Assert.assertEquals(
                    "a blocked view must not advance its watermark",
                    processedBefore,
                    instance("lv").getLastProcessedSeqTxn()
            );
            assertNoRefreshFaults("lv");

            // live_views() reports the status operators already search for, with the phase telling
            // a format block apart from a durable invalidation.
            assertQuery("SELECT view_status, checkpoint_recovery_phase, " +
                    "invalidation_reason = checkpoint_recovery_reason AS reason_mirrored " +
                    "FROM live_views() WHERE view_name = 'lv'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("view_status\tcheckpoint_recovery_phase\treason_mirrored\n" +
                            "invalid\tblocked\ttrue\n");

            // The disposition is derived from the superblock on every start, so a restart reaches
            // it again with no marker of its own, and is as harmless as the first pass.
            restartCycle();
            Assert.assertTrue(instance("lv").isCheckpointRecoveryBlocked());
            Assert.assertEquals(checkpointFilesBefore, countFiles(checkpointsRoot));
            Assert.assertEquals(RELEASED_FORMAT_VERSION, readSuperblockFormatVersion(checkpointsRoot));
            assertReleasedRows();
        });
    }

    @Test
    public void testTheOperatorsReCreateIsTheWayOutOfABlockedReleasedTimeline() throws Exception {
        assertMemoryLeak(() -> {
            openFixture();
            Assert.assertTrue(instance("lv").isCheckpointRecoveryBlocked());

            // The documented exit, run as an operator would run it. SHOW CREATE LIVE VIEW has to
            // work on a blocked view - the definition is what the re-create is built from - and
            // its output has to re-execute, or the procedure the block points at is not one.
            printSql("SHOW CREATE LIVE VIEW lv;");
            final String releasedDdl = sink.toString().replace("ddl\n", "");
            execute("DROP LIVE VIEW lv");
            execute(releasedDdl);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);
            }

            final LiveViewInstance recreated = instance("lv");
            Assert.assertFalse("the re-created view must not be blocked", recreated.isCheckpointRecoveryBlocked());
            Assert.assertFalse(recreated.isInvalid());
            assertNoRefreshFaults("lv");

            // A fresh directory, declaring this build's version - the released one went with the
            // dropped view rather than being converted in place.
            Assert.assertEquals(
                    LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION,
                    readSuperblockFormatVersion(checkpointsRoot("lv"))
            );

            // The rows are a recomputation from the base rows available today, which is what makes
            // this a separate operation rather than completion of an upgrade: had the base since
            // lost history to TTL, DROP/DETACH PARTITION or TRUNCATE, they would differ from what
            // the released view served - deliberately, and by the operator's own hand.
            assertViewMatchesRecompute();

            // And it is an ordinary view from here: it seals its own timeline and a restart comes
            // back on it.
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
     * Asserts the view still serves exactly the rows the released build materialized. Read
     * through the ordinary cursor, so it covers what a user querying a blocked view gets.
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
     * Unpacks the fixture and loads its catalogue, which is where the block is decided, and
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
