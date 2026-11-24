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
 * The released shape whose per-function state layout also moved, and what the timeline-wide
 * format upgrade does with it.
 * <p>
 * An anchored cumulative {@code max}/{@code min} compiles to
 * {@code MaxDoubleWindowFunctionFactory.MaxMinOverUnboundedPartitionRowsFrameFunction} and its
 * LONG twin, and this branch took the redundant "initialized" byte out of both images - a
 * nine-byte freeze became eight - and bumped {@code checkpointStateFormatVersion()} 1 -&gt; 2 to
 * say so. The version rides inside the codec identity, which is the function directory's lookup
 * key, so every root {@code 10.0.1} wrote for these functions stops resolving.
 * <p>
 * Before the timeline-wide format version existed, that was the whole case, and its ending was
 * uncomfortable: the restore tried the released roots, failed to resolve the functions, and fell
 * back to a rebuild that threw the ladder away - silently, by way of a decode error. The format
 * version now decides first. The released directory declares a layout version older than this
 * build implements, so reconciliation stops at the superblock, and the refresh worker takes the
 * upgrade rebuild without ever opening the function directory: no restore is attempted, so no
 * identity mismatch is reached, and the released directory is retired whole once the rebuild's
 * replacement commits.
 * <p>
 * The rows cannot tell the two endings apart - a from-base replay lands on the same numbers either
 * way - so the witnesses are the route and the <b>absence</b> of the decode path's log lines.
 * <p>
 * To regenerate the fixture, copy {@code /lv/LiveViewReleaseExtremaFixtureGenerator.java.txt}
 * into a clean {@code 10.0.1} checkout's {@code io.questdb.test.cairo.lv} package and run it;
 * the constants below are the values it prints.
 */
public class LiveViewCheckpointReleaseExtremaCompatTest extends AbstractLiveViewCheckpointCompatTest {

    /**
     * The daily anchor the released view was declared with, written out as an ordinary
     * partition term for the recompute oracle. ANCHOR is live-view syntax.
     */
    private static final String CUMULATIVE_FRAME =
            "PARTITION BY account_id, bucket ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW";
    // The simulated clock the fixture's own run left behind. This one starts above it, so the
    // flush cadence reads a forward-moving clock rather than one that jumped backwards.
    private static final long FIXTURE_END_MICROS = 2_500_000L;
    private static final String FIXTURE_RESOURCE = "/lv/lv_checkpoint_10_0_1_extrema.zip";
    // The layout version 10.0.1 stamped, pinned rather than derived from this build's own
    // constant so a later bump cannot quietly redefine what the fixture is.
    private static final int RELEASED_FORMAT_VERSION = 1;
    private static final String VIEW_NAME = "lv_extrema";
    // A valid view holding correct rows is the ending of both a decode-path fallback and the
    // upgrade rebuild, so the state a case can read afterwards does not say which one ran. The
    // log and the route do.
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
    public void testTheUpgradeRebuildNeverDecodesTheReleasedRoots() throws Exception {
        assertMemoryLeak(() -> {
            final long checkpointFilesBefore = openFixture();
            final File checkpointsRoot = checkpointsRoot(VIEW_NAME);
            Assert.assertEquals(
                    "the fixture must declare the version 10.0.1 stamped",
                    RELEASED_FORMAT_VERSION,
                    readSuperblockFormatVersion(checkpointsRoot)
            );
            final LiveViewInstance instance = instance(VIEW_NAME);
            Assert.assertTrue(instance.isCheckpointUpgradeRebuildPending());
            Assert.assertEquals(checkpointFilesBefore, countFiles(checkpointsRoot));

            // The upgrade's first refresh cycle, which is where the silent retirement used to
            // happen.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }

            // The route that ran, and - the point of the case - the decode path that did not. The
            // function directory is never opened, so the identity mismatch the state-version bump
            // creates is never reached.
            capture.drain();
            capture.assertLogged("live view checkpoint timeline was written by an older format, rebuilding from base");
            capture.assertLogged("live view restart rebuilding from applied base [view=" + VIEW_NAME + ", cause=timeline format upgrade");
            capture.assertNotLogged("could not restore live view from checkpoint timeline, rebuilding derived state");
            capture.assertNotLogged("root is missing a compiled function");
            capture.assertNotLogged("could not write live view head checkpoint");
            assertUpgradeRebuilt(VIEW_NAME);
            Assert.assertEquals(1, instance.getCheckpointRebuildAttempts());
            Assert.assertEquals("the rebuild retires the released directory once", 1, instance.getCheckpointTimelineResets());
            assertNoRefreshFaults(VIEW_NAME);
            assertViewMatchesRecompute("after the upgrade rebuild");

            // The released roots are gone: what the directory holds now is this build's own seal,
            // under this build's format and this build's state version.
            Assert.assertEquals(
                    LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION,
                    readSuperblockFormatVersion(checkpointsRoot)
            );

            // And the next restart restores from that seal rather than rebuilding again: the cost
            // is paid once, on the first start after the upgrade.
            capture.start();
            restartCycle();
            assertRestoredFromTimeline(VIEW_NAME);
            capture.drain();
            capture.assertNotLogged("live view restart rebuilding from applied base");
            assertViewMatchesRecompute("after a restart off the rebuilt view's own seal");
        });
    }

    @Test
    public void testTheOperatorsReCreateRebuildsTheExtremaViewUnderThisBuildsStateVersion() throws Exception {
        assertMemoryLeak(() -> {
            openFixture();
            Assert.assertTrue(instance(VIEW_NAME).isCheckpointUpgradeRebuildPending());

            // The exit a 10.0.x view used to need. What it produces here is what the upgrade
            // rebuild produces - a window recomputed from the base rows available today.
            printSql("SHOW CREATE LIVE VIEW " + VIEW_NAME + ';');
            final String releasedDdl = sink.toString().replace("ddl\n", "");
            execute("DROP LIVE VIEW " + VIEW_NAME);
            execute(releasedDdl);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, VIEW_NAME);
                driveRefreshToQuiescence(job);
                insertExtrema(job, 60);
            }
            final LiveViewInstance recreated = instance(VIEW_NAME);
            Assert.assertFalse(recreated.isCheckpointRecoveryBlocked());
            Assert.assertFalse(recreated.isCheckpointUpgradeRebuildPending());
            Assert.assertFalse(recreated.isInvalid());
            Assert.assertEquals(
                    LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION,
                    readSuperblockFormatVersion(checkpointsRoot(VIEW_NAME))
            );
            assertViewMatchesRecompute("after the operator's re-create");

            // And the ladder it seals is this build's, so the next restart restores rather than
            // recomputing.
            capture.start();
            restartCycle();
            assertRestoredFromTimeline(VIEW_NAME);
            capture.drain();
            capture.assertNotLogged("live view restart rebuilding from applied base");
            assertViewMatchesRecompute("after a restart off the re-created view's own seal");
        });
    }

    private void assertViewMatchesRecompute(String at) throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(SELECT created_at, account_id, "
                        + "max(amount) OVER (" + CUMULATIVE_FRAME + ") AS running_max, "
                        + "min(amount) OVER (" + CUMULATIVE_FRAME + ") AS running_min, "
                        + "max(qty) OVER (" + CUMULATIVE_FRAME + ") AS running_qty_max, "
                        + "min(qty) OVER (" + CUMULATIVE_FRAME + ") AS running_qty_min "
                        + "FROM (SELECT created_at, account_id, amount, qty, "
                        + "timestamp_floor('1d', created_at, '1970-01-01T00:00:00.000000Z'::timestamp) AS bucket "
                        + "FROM ext)) ORDER BY 2, 1",
                '(' + VIEW_NAME + ") ORDER BY 2, 1",
                LOG,
                true
        );
        assertNoRefreshFaults(VIEW_NAME);
        LOG.info().$("released extrema view matches its from-base recompute [at=").$(at).$(']').$();
    }

    private void insertExtrema(LiveViewRefreshJob job, int second) throws Exception {
        execute("INSERT INTO ext VALUES ('" + timestamp(second) + "', 'acct-1', "
                + (second + 1.5) + ", " + (second * 1_000L) + ')');
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    /**
     * Unpacks the fixture and loads its catalogue, which is where the upgrade is decided, and
     * reports the file count the released tree arrived with. The count is taken before the
     * catalogue load, so it is the released inventory rather than one this build has already
     * had an opportunity to change.
     */
    private long openFixture() throws IOException {
        replaceDbContent(FIXTURE_RESOURCE);
        final File checkpointsRoot = new File(
                new File(engine.getConfiguration().getDbRoot(), engine.getTableTokenIfExists(VIEW_NAME).getDirName()),
                LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME
        );
        final long files = countFiles(checkpointsRoot);
        engine.buildViewGraphs();
        Assert.assertFalse(
                VIEW_NAME + ": the fixture must not carry an invalid view",
                instance(VIEW_NAME).isInvalid()
        );
        return files;
    }

    private void restartCycle() throws Exception {
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
        try (LiveViewRefreshJob resumed = new LiveViewRefreshJob(0, engine, 1)) {
            driveRefreshToQuiescence(resumed);
        }
    }

    private static String timestamp(int secondOfDay) {
        return String.format("2026-01-01T09:%02d:%02d.000000Z", secondOfDay / 60, secondOfDay % 60);
    }
}
