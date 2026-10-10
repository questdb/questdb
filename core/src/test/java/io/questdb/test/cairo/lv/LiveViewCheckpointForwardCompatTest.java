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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointRestoreRoute;
import io.questdb.cairo.lv.LiveViewCheckpointRoot;
import io.questdb.cairo.lv.LiveViewCheckpointSuperblock;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineReader;
import io.questdb.cairo.lv.LiveViewCheckpointWindowRoot;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.zip.CRC32;

/**
 * Forward compatibility: what this build does when the {@code _checkpoints} tree on disk was
 * written by a <b>newer</b> one.
 * <p>
 * This is not hypothetical, and it is not the same question as the cross-version restore in
 * {@link LiveViewCheckpointReleaseCompatTest}. That case reads a tree an older build wrote,
 * and the answer there is "rebuild the view from its base table and retire the older tree".
 * This case is the other direction: a user upgrades,
 * the newer build seals through its own writers, and the user then rolls back - or a mixed-
 * version cluster puts an older binary in front of a newer node's files. The answer there
 * cannot be "restore it", because this build does not know the shape. It has to be "notice,
 * discard, and rebuild the derived state from the base table", with the view still valid and
 * still correct at the end.
 * <p>
 * What makes it a live concern is what this branch itself did. It added a fused window root
 * ({@code PAGE_KIND = 0x1d}) and a {@code _retirements} file long before it bumped
 * {@code LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION}, because neither addition made an
 * old page unreadable - the bump came later, with the removal of the decoder that had kept
 * them readable. A future release has every reason to extend the format the same way, so the
 * interesting failures are still the ones the superblock version does not announce.
 * <p>
 * Three gates decide the outcome, in this order, and the cases below cover all three:
 * <ol>
 *     <li>the superblock's magic and layout version - a version declared in both fields that
 *     carry one blocks the view and keeps the directory, a slot whose two disagree is damage the
 *     other slot recovers from, and a directory holding neither a declaration nor a readable slot
 *     is reset whole;</li>
 *     <li>an unrecognized top-level entry in {@code _checkpoints/} - the same reset, which is
 *     the gate {@code _retirements} would have tripped on a 10.0.x binary;</li>
 *     <li>neither of those moved, but the metadata pages inside are newer. Nothing at the
 *     lifecycle level sees this one. It has to be the page decoders that refuse, and the
 *     restore's own catch that turns the refusal into a rebuild.</li>
 * </ol>
 * Every injection here rewrites the page checksum after the edit, so the bytes are a
 * <b>well-formed page of a shape this build does not know</b> rather than a damaged one. That
 * distinction is the whole point of the case: the suite's existing corruption tests flip a bit
 * and expect {@code metadata page checksum mismatch}, which proves nothing about a format that
 * is intact and merely newer.
 */
public class LiveViewCheckpointForwardCompatTest extends AbstractLiveViewCheckpointCompatTest {

    // One boundary per commit, at ten-second intervals from the daily anchor.
    private static final int BOUNDARIES = 5;
    private static final String DAILY_ANCHOR = "2026-01-01T";
    // The next metadata page kind a future release would allocate: this branch's own tags run
    // 0x11..0x1d, so 0x1e is what an extension of the format looks like from here.
    private static final int FUTURE_PAGE_KIND = 0x1e;
    // Every audited structure is at format version 1, so 2 is a future revision of one of them.
    private static final int FUTURE_STRUCTURE_FORMAT_VERSION = 2;
    // A plausible top-level file a future release adds beside _timeline, the way this branch
    // added _retirements.
    private static final String FUTURE_TOP_LEVEL_ARTEFACT = "_lineage";
    // What the restore logs when it leaves a fallback to the rebuild rather than heal the roots it
    // stepped over.
    private static final String HEAL_DECLINED =
            "live view checkpoint heal declined, a boundary's timestamp group grew after its seal";
    // What the restore logs when the base rows the heal folded are not the rows the view's output
    // holds over the same interval.
    private static final String HEAL_DECLINED_ROWS =
            "live view checkpoint heal declined, the base table does not hold the rows the view materialized";
    // Three gates can produce the same outcome here - a valid view holding correct rows - and
    // the state a case can read afterwards does not say which one fired. The log does, so every
    // case names its own gate rather than settling for the shared ending.
    private static final LogCapture capture = new LogCapture();

    @After
    public void resetClock() {
        capture.stop();
        setCurrentMicros(-1);
    }

    @Before
    public void setUpCadence() {
        // One logical boundary per commit, so the timeline carries a ladder deep enough for the
        // head-only case to have a predecessor to fall back to.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setCurrentMicros(0);
        capture.start();
    }

    @Test
    public void testAFlippedBitInTheNewestSlotsFormatVersionFallsBackToTheOtherSlot() throws Exception {
        assertMemoryLeak(() -> assertAFlippedFormatBitFallsBack(LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION_OFFSET));
    }

    @Test
    public void testAFlippedBitInTheNewestSlotsMagicNibbleFallsBackToTheOtherSlot() throws Exception {
        assertMemoryLeak(() -> assertAFlippedFormatBitFallsBack(LiveViewCheckpointSuperblock.SLOT_MAGIC_OFFSET));
    }

    @Test
    public void testAFutureHeadAloneFallsBackToTheBoundariesThisBuildUnderstands() throws Exception {
        assertMemoryLeak(() -> {
            seedFiveBoundaries();
            final File checkpointsRoot = checkpointsRoot();
            final ObjList<PageSite> stateRoots = stateRootSites();
            final PageSite head = stateRoots.getQuick(stateRoots.size() - 1);
            shutdown();

            // A rollback taken right after the newer build sealed a single boundary: the head is
            // in a shape this build cannot read, everything below it is still its own. The
            // bounded predecessor fallback is what this exercises - the restore is not forced all
            // the way back to the base table for damage scoped to one root version.
            rewriteMetaPageInt(checkpointsRoot, head, LiveViewCheckpointLayout.PAGE_KIND_OFFSET, FUTURE_PAGE_KIND);

            restart();

            // The fallback gate, not the rebuild one: the restore walked past the head it could
            // not read and landed on the newest boundary it could.
            capture.drain();
            capture.assertLogged("live view checkpoint restore fell back past corrupt roots, reconstructing");
            capture.assertNotLogged("live view restart rebuilding from applied base");

            final LiveViewInstance instance = instance("lv");
            Assert.assertFalse("a newer head must not invalidate the view", instance.isInvalid());
            Assert.assertTrue("the restore must have run", instance.isCheckpointRestoreAttempted());
            assertNoRefreshFaults("lv");
            // The lineage survives: the fallback restored off the newest boundary it understood
            // and then healed the one it skipped in place, rather than retiring the timeline.
            Assert.assertEquals(
                    "a fallback must keep every boundary the ladder held",
                    BOUNDARIES,
                    countSealedBoundaries("lv")
            );
            Assert.assertTrue(
                    "the heal must republish the skipped boundary in this build's own shape",
                    isFusedHead("lv")
            );
            assertViewMatchesRecompute();

            // The healed generation is a normal one: a further commit and restart restore off it.
            assertRestartsCleanlyAfterwards();
        });
    }

    @Test
    public void testAFutureHeadOverAnUnconsumedOutOfOrderRowIsHealedOverALossyBase() throws Exception {
        assertMemoryLeak(() -> {
            // The base loses an hour of the previous day that the view keeps, so a rebuild from the
            // applied base is refused. It then applies an out-of-order row strictly inside the head's
            // interval before the view consumes it. The heal folds that row, which sits below the
            // durable frontier: the drain meets its commit as out of order and repairs it from a
            // boundary below it, so the heal stands and the view keeps refreshing.
            seedFiveBoundaries("('2025-12-31T23:00:00.000000Z', 'acct-1', 500.0)", -1, null);
            execute("ALTER TABLE tx DROP PARTITION LIST '2025-12-31T23'");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            execute("INSERT INTO tx VALUES ('" + timestamp(35) + "', 'acct-1', 1000.0)");
            drainWalQueue();
            rollBackTheHead();

            capture.drain();
            capture.assertLogged("reconstructed corrupt live view checkpoint roots");
            capture.assertNotLogged(HEAL_DECLINED_ROWS);
            capture.assertNotLogged("live view rebuild from the applied base refused");
            assertRestoredFromTimeline("lv");
            Assert.assertFalse("the heal must not leave the view blocked", instance("lv").isCheckpointRecoveryBlocked());
            Assert.assertTrue("the drain must repair the late row's commit", repairedRows() > 0);
            assertNoRefreshFaults("lv");
            final String viewRows = """
                    created_at\taccount_id\tcumulative_sum\tcumulative_count
                    2025-12-31T23:00:00.000000Z\tacct-1\t500.0\t1
                    2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
                    2026-01-01T09:00:10.000000Z\tacct-2\t11.0\t1
                    2026-01-01T09:00:20.000000Z\tacct-1\t22.0\t2
                    2026-01-01T09:00:30.000000Z\tacct-2\t42.0\t2
                    2026-01-01T09:00:35.000000Z\tacct-1\t1022.0\t3
                    2026-01-01T09:00:40.000000Z\tacct-1\t1063.0\t4
                    """;
            assertViewRows(viewRows);

            // New rows keep arriving, and across a restart.
            commitAndRefresh("('" + timestamp(50) + "', 'acct-1', 100.0), ('" + timestamp(55) + "', 'acct-2', 7.0)");
            final String grownRows = viewRows + """
                    2026-01-01T09:00:50.000000Z\tacct-1\t1163.0\t5
                    2026-01-01T09:00:55.000000Z\tacct-2\t49.0\t3
                    """;
            assertViewRows(grownRows);
            shutdown();
            restart();
            assertRestoredFromTimeline("lv");
            Assert.assertFalse(instance("lv").isCheckpointRecoveryBlocked());
            assertNoRefreshFaults("lv");
            assertViewRows(grownRows);
            commitAndRefresh("('" + timestamp(59) + "', 'acct-1', 3.0)");
            assertViewRows(grownRows + "2026-01-01T09:00:59.000000Z\tacct-1\t1166.0\t6\n");
        });
    }

    @Test
    public void testAFutureHeadOverAPredecessorWhoseTimestampGroupGrewIsRebuiltRatherThanHealed() throws Exception {
        assertMemoryLeak(() -> {
            // A tie on the head's predecessor after it sealed; the head sealed above it holds the
            // tie. The heal warms up from the predecessor, which does not, and positions the roots
            // it rebuilds from the durable table, which does - so the restore's row count would
            // pass over accumulators short by the tie.
            seedFiveBoundaries(null, 30, "acct-1");
            rollBackTheHead();

            // A row of the tied account in the same anchor day reads the accumulators back. Without
            // the tie they would answer 163.0 over four rows.
            commitAndRefresh("('" + timestamp(50) + "', 'acct-1', 100.0)");
            assertViewRows("""
                    created_at\taccount_id\tcumulative_sum\tcumulative_count
                    2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
                    2026-01-01T09:00:10.000000Z\tacct-2\t11.0\t1
                    2026-01-01T09:00:20.000000Z\tacct-1\t22.0\t2
                    2026-01-01T09:00:30.000000Z\tacct-2\t42.0\t2
                    2026-01-01T09:00:30.000000Z\tacct-1\t1022.0\t3
                    2026-01-01T09:00:40.000000Z\tacct-1\t1063.0\t4
                    2026-01-01T09:00:50.000000Z\tacct-1\t1163.0\t5
                    """);
            assertViewMatchesRecompute();
            capture.drain();
            capture.assertLogged(HEAL_DECLINED + " [view=lv, boundary=2026-01-01T09:00:30.000000Z, "
                    + "recordedRows=4, durableRows=5]");
            capture.assertNotLogged("reconstructed corrupt live view checkpoint roots");
            assertRebuiltFromAppliedBase("lv");
            assertNoRefreshFaults("lv");
        });
    }

    @Test
    public void testAFutureHeadWhoseTieTheViewHasNotConsumedIsRebuiltRatherThanHealed() throws Exception {
        assertMemoryLeak(() -> {
            // The base applies a tie on the head before the view consumes it, and the restart then
            // finds the head unreadable. The view's output and applied watermark stop below the
            // tie, while the base table the heal reads already holds it. A head healed with the
            // tie folded in would meet the tie again when the drain consumes its commit, so the
            // heal declines and the restart rebuilds the view from the applied base.
            seedFiveBoundaries();
            execute("INSERT INTO tx VALUES ('" + timestamp(40) + "', 'acct-2', 1000.0)");
            drainWalQueue();
            rollBackTheHead();

            assertViewRows("""
                    created_at\taccount_id\tcumulative_sum\tcumulative_count
                    2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
                    2026-01-01T09:00:10.000000Z\tacct-2\t11.0\t1
                    2026-01-01T09:00:20.000000Z\tacct-1\t22.0\t2
                    2026-01-01T09:00:30.000000Z\tacct-2\t42.0\t2
                    2026-01-01T09:00:40.000000Z\tacct-1\t63.0\t3
                    2026-01-01T09:00:40.000000Z\tacct-2\t1042.0\t3
                    """);
            assertViewMatchesRecompute();
            capture.drain();
            capture.assertLogged("live view checkpoint restore fell back past corrupt roots, reconstructing");
            capture.assertLogged(HEAL_DECLINED_ROWS);
            capture.assertNotLogged("reconstructed corrupt live view checkpoint roots");
            assertRebuiltFromAppliedBase("lv");
            assertNoRefreshFaults("lv");
        });
    }

    @Test
    public void testAFutureHeadWhoseTimestampGroupGrewIsHealedAtTheAppliedBase() throws Exception {
        assertMemoryLeak(() -> {
            // A tie on the head after it sealed, and no row above it: the durable frontier is the
            // head's own timestamp. The heal rebuilds the head from the base table, tie included,
            // and publishes it at the view's applied base seqTxn rather than at the head's own, so
            // the restore replays nothing onto it and folds the tie exactly once.
            seedFiveBoundaries(null, 40, "acct-2");
            rollBackTheHead();

            capture.drain();
            capture.assertLogged("live view checkpoint restore fell back past corrupt roots, reconstructing");
            capture.assertLogged("reconstructed corrupt live view checkpoint roots");
            capture.assertNotLogged(HEAL_DECLINED);
            capture.assertNotLogged("live view checkpoint timeline rebuild does not match durable materialization");
            assertRestoredFromTimeline("lv");
            Assert.assertFalse(instance("lv").isInvalid());
            assertNoRefreshFaults("lv");
            Assert.assertEquals(
                    "the heal re-versions the head in place and seals no boundary of its own",
                    BOUNDARIES,
                    countSealedBoundaries("lv")
            );
            final String viewRows = """
                    created_at\taccount_id\tcumulative_sum\tcumulative_count
                    2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
                    2026-01-01T09:00:10.000000Z\tacct-2\t11.0\t1
                    2026-01-01T09:00:20.000000Z\tacct-1\t22.0\t2
                    2026-01-01T09:00:30.000000Z\tacct-2\t42.0\t2
                    2026-01-01T09:00:40.000000Z\tacct-1\t63.0\t3
                    2026-01-01T09:00:40.000000Z\tacct-2\t1042.0\t3
                    """;
            assertViewRows(viewRows);
            assertViewMatchesRecompute();

            // A row of the tied account reads the healed accumulators back: a head that missed the
            // tie would answer 142.0 over three rows, one that folded it twice 2142.0 over five.
            commitAndRefresh("('" + timestamp(50) + "', 'acct-2', 100.0)");
            final String grownRows = viewRows + "2026-01-01T09:00:50.000000Z\tacct-2\t1142.0\t4\n";
            assertViewRows(grownRows);
            shutdown();
            restart();
            assertRestoredFromTimeline("lv");
            assertNoRefreshFaults("lv");
            assertViewRows(grownRows);
            assertViewMatchesRecompute();
        });
    }

    @Test
    public void testAFutureHeadWhoseTimestampGroupGrewIsHealedOverALossyBase() throws Exception {
        assertMemoryLeak(() -> {
            // The first commit carries a row an hour below the boundaries, and the base loses that
            // hour. The view keeps the row, so the rebuild from the applied base is refused and
            // would block the view. The heal needs no row below the predecessor it warms up from,
            // so it brings the view back with its whole history.
            seedFiveBoundaries("('2026-01-01T08:00:00.000000Z', 'acct-1', 500.0)", 40, "acct-2");
            execute("ALTER TABLE tx DROP PARTITION LIST '2026-01-01T08'");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            assertNoRefreshFaults("lv");
            final String viewRows = """
                    created_at\taccount_id\tcumulative_sum\tcumulative_count
                    2026-01-01T08:00:00.000000Z\tacct-1\t500.0\t1
                    2026-01-01T09:00:00.000000Z\tacct-1\t501.0\t2
                    2026-01-01T09:00:10.000000Z\tacct-2\t11.0\t1
                    2026-01-01T09:00:20.000000Z\tacct-1\t522.0\t3
                    2026-01-01T09:00:30.000000Z\tacct-2\t42.0\t2
                    2026-01-01T09:00:40.000000Z\tacct-1\t563.0\t4
                    2026-01-01T09:00:40.000000Z\tacct-2\t1042.0\t3
                    """;
            assertViewRows(viewRows);
            rollBackTheHead();

            capture.drain();
            capture.assertLogged("reconstructed corrupt live view checkpoint roots");
            capture.assertNotLogged(HEAL_DECLINED);
            capture.assertNotLogged("live view rebuild from the applied base refused");
            assertRestoredFromTimeline("lv");
            Assert.assertFalse("the heal must not leave the view blocked", instance("lv").isCheckpointRecoveryBlocked());
            assertNoRefreshFaults("lv");
            assertViewRows(viewRows);

            // New rows keep arriving, over the tie folded exactly once, and across a restart.
            commitAndRefresh("('" + timestamp(50) + "', 'acct-2', 100.0)");
            final String grownRows = viewRows + "2026-01-01T09:00:50.000000Z\tacct-2\t1142.0\t4\n";
            assertViewRows(grownRows);
            shutdown();
            restart();
            assertRestoredFromTimeline("lv");
            Assert.assertFalse(instance("lv").isCheckpointRecoveryBlocked());
            assertNoRefreshFaults("lv");
            assertViewRows(grownRows);
        });
    }

    @Test
    public void testAFutureHeadWhoseTimestampGroupGrewIsHealedOverAnUnconsumedOutOfOrderRow() throws Exception {
        assertMemoryLeak(() -> {
            // The head's group grew after its seal, the base lost an hour of the previous day, and
            // the base then applied an out-of-order row below the head that the view has not
            // consumed. The heal folds the tie, which the view's table holds, and the late row,
            // which the base does, and publishes at the applied base seqTxn. The drain then repairs
            // the late row's commit from the boundary below it, leaving the tie folded once.
            seedFiveBoundaries("('2025-12-31T23:00:00.000000Z', 'acct-1', 500.0)", 40, "acct-2");
            execute("ALTER TABLE tx DROP PARTITION LIST '2025-12-31T23'");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            execute("INSERT INTO tx VALUES ('" + timestamp(35) + "', 'acct-1', 300.0)");
            drainWalQueue();
            rollBackTheHead();

            capture.drain();
            capture.assertLogged("reconstructed corrupt live view checkpoint roots");
            capture.assertNotLogged(HEAL_DECLINED);
            capture.assertNotLogged(HEAL_DECLINED_ROWS);
            assertRestoredFromTimeline("lv");
            Assert.assertFalse("the heal must not leave the view blocked", instance("lv").isCheckpointRecoveryBlocked());
            Assert.assertTrue("the drain must repair the late row's commit", repairedRows() > 0);
            assertNoRefreshFaults("lv");
            final String viewRows = """
                    created_at\taccount_id\tcumulative_sum\tcumulative_count
                    2025-12-31T23:00:00.000000Z\tacct-1\t500.0\t1
                    2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
                    2026-01-01T09:00:10.000000Z\tacct-2\t11.0\t1
                    2026-01-01T09:00:20.000000Z\tacct-1\t22.0\t2
                    2026-01-01T09:00:30.000000Z\tacct-2\t42.0\t2
                    2026-01-01T09:00:35.000000Z\tacct-1\t322.0\t3
                    2026-01-01T09:00:40.000000Z\tacct-1\t363.0\t4
                    2026-01-01T09:00:40.000000Z\tacct-2\t1042.0\t3
                    """;
            assertViewRows(viewRows);

            // The tie's account reads its accumulators back: 1142.0 over four rows, where a tie
            // folded twice would answer 2142.0 over five.
            commitAndRefresh("('" + timestamp(50) + "', 'acct-2', 100.0)");
            final String grownRows = viewRows + "2026-01-01T09:00:50.000000Z\tacct-2\t1142.0\t4\n";
            assertViewRows(grownRows);
            shutdown();
            restart();
            assertRestoredFromTimeline("lv");
            Assert.assertFalse(instance("lv").isCheckpointRecoveryBlocked());
            assertNoRefreshFaults("lv");
            assertViewRows(grownRows);
        });
    }

    @Test
    public void testAFutureHeadWhoseTimestampGroupGrewUnderAHigherFrontierIsHealedAndSealedThere() throws Exception {
        assertMemoryLeak(() -> {
            seedFiveBoundaries();
            // One commit puts a tie on the head and a row above it, and the cadence seals neither,
            // so the durable frontier runs past the head boundary that holds part of its group.
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000);
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ADAPTIVE_CADENCE_ENABLED, "false");
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_MAX_DURATION_MICROS, 86_400_000_000L);
            commitAndRefresh("('" + timestamp(40) + "', 'acct-2', 1000.0), ('" + timestamp(45) + "', 'acct-1', 100.0)");
            Assert.assertEquals("the commit must not seal a boundary", BOUNDARIES, countSealedBoundaries("lv"));
            rollBackTheHead();

            // The heal folds the head from the base table, tie included, and then seals the
            // frontier above it at the view's applied base seqTxn. The restore lands on that root
            // and replays nothing, so no row above the head is lost and the tie is not folded twice.
            capture.drain();
            capture.assertLogged("live view checkpoint restore fell back past corrupt roots, reconstructing");
            capture.assertLogged("reconstructed corrupt live view checkpoint roots");
            capture.assertNotLogged(HEAL_DECLINED);
            assertRestoredFromTimeline("lv");
            assertNoRefreshFaults("lv");
            Assert.assertEquals(
                    "the heal must seal one boundary at the durable frontier",
                    BOUNDARIES + 1,
                    countSealedBoundaries("lv")
            );
            Assert.assertEquals(ts(timestamp(45)), instance("lv").getHeadCheckpointMaxTs());
            Assert.assertTrue(isFusedHead("lv"));
            final String viewRows = """
                    created_at\taccount_id\tcumulative_sum\tcumulative_count
                    2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
                    2026-01-01T09:00:10.000000Z\tacct-2\t11.0\t1
                    2026-01-01T09:00:20.000000Z\tacct-1\t22.0\t2
                    2026-01-01T09:00:30.000000Z\tacct-2\t42.0\t2
                    2026-01-01T09:00:40.000000Z\tacct-1\t63.0\t3
                    2026-01-01T09:00:40.000000Z\tacct-2\t1042.0\t3
                    2026-01-01T09:00:45.000000Z\tacct-1\t163.0\t4
                    """;
            assertViewRows(viewRows);

            // Both accounts read their healed accumulators back, before and after a restart.
            commitAndRefresh("('" + timestamp(50) + "', 'acct-2', 100.0), ('" + timestamp(55) + "', 'acct-1', 1.0)");
            final String grownRows = viewRows + """
                    2026-01-01T09:00:50.000000Z\tacct-2\t1142.0\t4
                    2026-01-01T09:00:55.000000Z\tacct-1\t164.0\t5
                    """;
            assertViewRows(grownRows);
            assertViewMatchesRecompute();
            shutdown();
            restart();
            assertRestoredFromTimeline("lv");
            assertNoRefreshFaults("lv");
            assertViewRows(grownRows);
            assertViewMatchesRecompute();
        });
    }

    @Test
    public void testAFutureStateRootFormatVersionRebuildsFromTheBase() throws Exception {
        assertMemoryLeak(() -> {
            seedFiveBoundaries();
            final File checkpointsRoot = checkpointsRoot();
            final ObjList<PageSite> stateRoots = stateRootSites();
            shutdown();

            // A future revision of the fused root itself: same page kind, same framing, a
            // structure format version this build does not write.
            for (int i = 0, n = stateRoots.size(); i < n; i++) {
                rewriteMetaPageInt(
                        checkpointsRoot,
                        stateRoots.getQuick(i),
                        LiveViewCheckpointLayout.PAGE_HEADER_SIZE,
                        FUTURE_STRUCTURE_FORMAT_VERSION
                );
            }
            assertAFutureFormatVersionIsRejected(checkpointsRoot, stateRoots.getQuick(stateRoots.size() - 1));

            restart();
            assertTheDecoderGateRefused(
                    "window state root format version mismatch [expected=1, actual="
                            + FUTURE_STRUCTURE_FORMAT_VERSION + ']'
            );
            assertRebuiltFromTheBase();
            assertRestartsCleanlyAfterwards();
        });
    }

    @Test
    public void testABlockedViewReleasesItsBaseWalFloor() throws Exception {
        // One WAL segment per commit, so countWalSegments reads the purge floor rather than the
        // rollover threshold - the same knob LiveViewRefreshDisabledTest uses for the same reading.
        setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 1);
        // WalPurgeJob.runSerially is interval-gated off the millisecond clock, which this class
        // freezes. Without both of these the sweep below silently does nothing.
        setProperty(PropertyKey.CAIRO_WAL_PURGE_INTERVAL, 0);
        assertMemoryLeak(() -> {
            seedFiveBoundaries();
            final File checkpointsRoot = checkpointsRoot();
            final String baseDirName = engine.getTableTokenIfExists("tx").getDirName();
            shutdown();

            setSuperblockFormatVersion(checkpointsRoot, LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION + 1);
            restart();
            Assert.assertTrue(instance("lv").isCheckpointRecoveryBlocked());

            // A base commit the blocked view will not consume. It is what gives the purge job
            // something above the view's frozen watermark to reclaim.
            execute("INSERT INTO tx VALUES ('" + timestamp(50) + "', 'acct-1', 100.0)");
            drainWalQueue();

            // The floor a blocked view does NOT hold. Its own watermark never advances, so any
            // floor it publishes is frozen, and a frozen floor grows the base WAL without bound -
            // on a base table every other writer and view shares. Releasing is the same rule an
            // invalid view follows, for the same reason.
            final long walSegmentsBefore = countWalSegments(baseDirName);
            engine.releaseInactive();
            setCurrentMicros(60_000_000L);
            try (WalPurgeJob purgeJob = new WalPurgeJob(engine)) {
                purgeJob.drain(0);
            }
            Assert.assertTrue(
                    "a blocked view must release its base WAL floor, not pin it",
                    countWalSegments(baseDirName) < walSegmentsBefore
            );

            // What that release costs, stated rather than hidden, because it is the reason to reach
            // for the exit rather than to sit on a block. The restore replays the base WAL between
            // the head checkpoint's boundary and the applied watermark; the sweep above took it, so
            // a build that DOES read the format cannot resume off the roots the block preserved. It
            // spends the flush-retry budget on the missing segment and lands in the base-WAL-loss
            // re-derive, which recomputes the view from the base rows available today.
            shutdown();
            setSuperblockFormatVersion(checkpointsRoot, LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION);
            restart();
            Assert.assertFalse(instance("lv").isCheckpointRecoveryBlocked());
            capture.drain();
            capture.assertLogged("live view re-derived from the applied base after base WAL loss");

            // The rows are right here only because this base still holds every row the view was
            // built from. A base that had since lost history to TTL, DROP/DETACH PARTITION or
            // TRUNCATE would be recomputed from whatever survives - silently, and differently -
            // which is the outcome the block exists to avoid and the reason the way out is the
            // operator's re-CREATE rather than an indefinite wait.
            assertViewMatchesRecompute();
        });
    }

    @Test
    public void testAFutureStateRootPageKindRebuildsFromTheBase() throws Exception {
        assertMemoryLeak(() -> {
            seedFiveBoundaries();
            final File checkpointsRoot = checkpointsRoot();
            final ObjList<PageSite> stateRoots = stateRootSites();
            shutdown();

            // Every boundary, not only the head: a tree a newer build actually wrote carries the
            // newer shape all the way down, so leaving a predecessor readable would test the
            // fallback path instead of the one this case is about.
            for (int i = 0, n = stateRoots.size(); i < n; i++) {
                rewriteMetaPageInt(
                        checkpointsRoot,
                        stateRoots.getQuick(i),
                        LiveViewCheckpointLayout.PAGE_KIND_OFFSET,
                        FUTURE_PAGE_KIND
                );
            }
            assertAFuturePageKindIsRejectedRatherThanMisread(
                    checkpointsRoot,
                    stateRoots.getQuick(stateRoots.size() - 1)
            );

            restart();
            assertTheDecoderGateRefused("window state root page kind unknown, kind=" + FUTURE_PAGE_KIND);
            assertRebuiltFromTheBase();
            assertRestartsCleanlyAfterwards();
        });
    }

    @Test
    public void testAFutureSuperblockFormatVersionBlocksTheViewAndKeepsWhatItHas() throws Exception {
        assertMemoryLeak(() -> {
            seedFiveBoundaries();
            final File checkpointsRoot = checkpointsRoot();
            final long checkpointFilesBefore = countFiles(checkpointsRoot);
            final long processedBefore = instance("lv").getLastProcessedSeqTxn();
            shutdown();

            // The gate that does announce itself. Both slots, which is what a build that
            // owned this directory would have left, though one declaring slot is enough to
            // block. Declaring takes both fields that carry a version - see
            // assertAFlippedFormatBitFallsBack for what one of them alone gets.
            setSuperblockFormatVersion(checkpointsRoot, LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION + 1);

            restart();

            // The boundary gate, not the reset one. The distinction is the whole change: a
            // version this build does not implement is another build's generation announcing
            // itself, and this build cannot show that replaying today's surviving base rows
            // reproduces the output those roots stand for - TTL, DROP PARTITION and TRUNCATE
            // all take source rows a live view keeps its own output for.
            capture.drain();
            capture.assertLogged("live view checkpoint timeline declares an unsupported format version");
            capture.assertNotLogged("live view checkpoint timeline carries a foreign layout version");
            capture.assertNotLogged("live view restart rebuilding from applied base");

            final LiveViewInstance instance = instance("lv");
            Assert.assertTrue(instance.isCheckpointRecoveryBlocked());
            // Not a durable invalidation - _lv.s.invalid stays clear, which is what lets a build
            // that reads the format resume the view with no operator action - even though the view
            // reports itself invalid and releases its base WAL floor like any other stopped view.
            Assert.assertFalse("blocking must not write _lv.s.invalid", instance.isInvalid());
            TestUtils.assertContains(
                    instance.getCheckpointRecoveryReason(),
                    "checkpoint timeline format version " + (LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION + 1)
                            + " is not supported by this build"
            );
            // The route names a decision that was taken rather than an attempt that ran: the
            // refresh worker declined the view, so no restore and no rebuild happened.
            Assert.assertEquals(
                    "upgrade_blocked",
                    LiveViewCheckpointRestoreRoute.name(instance.getCheckpointRestoreRoute())
            );
            Assert.assertFalse(instance.isCheckpointRestoreAttempted());
            Assert.assertEquals(0, instance.getCheckpointRebuildAttempts());
            Assert.assertEquals(0, instance.getCheckpointTimelineResets());
            Assert.assertEquals(
                    "not one file of the other build's directory may move",
                    checkpointFilesBefore,
                    countFiles(checkpointsRoot)
            );

            // The rows the other build materialized are still served, and a new base commit
            // does not move them: refresh is stopped, not merely restore.
            assertBlockedViewRows();
            execute("INSERT INTO tx VALUES ('" + timestamp(50) + "', 'acct-1', 100.0)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int i = 0; i < 4; i++) {
                    drainJob(job);
                    drainWalQueue();
                }
            }
            assertBlockedViewRows();
            Assert.assertEquals(
                    "a blocked view must not advance its watermark",
                    processedBefore,
                    instance("lv").getLastProcessedSeqTxn()
            );
            assertNoRefreshFaults("lv");

            // live_views() carries the phase and the reason, so an operator can see why the
            // view stopped without reading the log - and it reports the status those operators
            // already search for, with invalidation_reason mirroring the recovery reason so a
            // query written for durable invalidations needs no new column to explain this one.
            // The phase is what says this is a format block rather than a terminal invalidation.
            assertQuery("SELECT view_status, checkpoint_recovery_phase, " +
                    "invalidation_reason = checkpoint_recovery_reason AS reason_mirrored " +
                    "FROM live_views() WHERE view_name = 'lv'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("view_status\tcheckpoint_recovery_phase\treason_mirrored\n" +
                            "invalid\tblocked\ttrue\n");

            // The disposition is derived from the superblock, so it survives a restart with no
            // marker of its own - and the second restart is as harmless as the first.
            shutdown();
            restart();
            Assert.assertTrue(instance("lv").isCheckpointRecoveryBlocked());
            Assert.assertEquals(checkpointFilesBefore, countFiles(checkpointsRoot));
            assertBlockedViewRows();

            // A build that does implement the version meets no boundary: the view resumes off
            // the roots that were held for it, rather than off a rebuild.
            shutdown();
            setSuperblockFormatVersion(checkpointsRoot, LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION);
            restart();
            Assert.assertFalse(instance("lv").isCheckpointRecoveryBlocked());
            assertRestoredFromTimeline("lv");
            Assert.assertTrue(
                    "the ladder the block preserved must come back whole, plus whatever the resume seals",
                    countSealedBoundaries("lv") >= BOUNDARIES
            );
            assertNoRefreshFaults("lv");
            // The commit that landed while the view was blocked is not lost either: no purge sweep
            // ran over this block, so the base WAL still held it, and the resumed view materializes
            // it on top of the restored roots rather than recomputing the window that carries it.
            // A block that outlives a purge sweep does not get this - see
            // testABlockedViewReleasesItsBaseWalFloor.
            assertQuery("SELECT created_at, account_id, cumulative_sum, cumulative_count FROM lv")
                    .noLeakCheck()
                    .timestamp("created_at")
                    .expectSize()
                    .returns("created_at\taccount_id\tcumulative_sum\tcumulative_count\n" +
                            "2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1\n" +
                            "2026-01-01T09:00:10.000000Z\tacct-2\t11.0\t1\n" +
                            "2026-01-01T09:00:20.000000Z\tacct-1\t22.0\t2\n" +
                            "2026-01-01T09:00:30.000000Z\tacct-2\t42.0\t2\n" +
                            "2026-01-01T09:00:40.000000Z\tacct-1\t63.0\t3\n" +
                            "2026-01-01T09:00:50.000000Z\tacct-1\t163.0\t4\n");
            assertViewMatchesRecompute();

            // And the generation the resume published is a normal one: a further restart comes
            // back on it.
            shutdown();
            restart();
            assertRestoredFromTimeline("lv");
            assertNoRefreshFaults("lv");
            assertViewMatchesRecompute();
        });
    }

    @Test
    public void testAFutureTopLevelArtefactResetsTheCheckpointDirectory() throws Exception {
        assertMemoryLeak(() -> {
            seedFiveBoundaries();
            final File checkpointsRoot = checkpointsRoot();
            shutdown();

            // This is the gate a 10.0.x binary would have met on a tree this branch wrote, since
            // _retirements is exactly such an addition. It is here to keep answering the same way
            // for whatever the next release adds.
            Assert.assertTrue(
                    "cannot create the future artefact",
                    new File(checkpointsRoot, FUTURE_TOP_LEVEL_ARTEFACT).createNewFile()
            );

            restart();
            capture.drain();
            capture.assertLogged(
                    "live view checkpoint directory holds an entry outside the current layout"
            );
            assertRebuiltFromTheBase();
            Assert.assertFalse(
                    "the reset must remove the whole directory, not recover the half it can read",
                    new File(checkpointsRoot, FUTURE_TOP_LEVEL_ARTEFACT).exists()
            );
            assertRestartsCleanlyAfterwards();
        });
    }

    @Test
    public void testAHealDeclinesWhenTheBaseLostRowsInsideTheHealedInterval() throws Exception {
        assertMemoryLeak(() -> {
            // The base loses the hour every boundary sits in, and the view keeps its rows. The
            // heal warms up from the predecessor and finds no base row between it and the head
            // it rebuilds, so a healed head would hold the predecessor's state under the head's
            // position: one acct-1 row short, with nothing in the restore's row count to say so.
            seedFiveBoundaries("('2026-01-01T08:00:00.000000Z', 'acct-1', 500.0)", -1, null);
            execute("ALTER TABLE tx DROP PARTITION LIST '2026-01-01T09'");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            final String viewRows = """
                    created_at\taccount_id\tcumulative_sum\tcumulative_count
                    2026-01-01T08:00:00.000000Z\tacct-1\t500.0\t1
                    2026-01-01T09:00:00.000000Z\tacct-1\t501.0\t2
                    2026-01-01T09:00:10.000000Z\tacct-2\t11.0\t1
                    2026-01-01T09:00:20.000000Z\tacct-1\t522.0\t3
                    2026-01-01T09:00:30.000000Z\tacct-2\t42.0\t2
                    2026-01-01T09:00:40.000000Z\tacct-1\t563.0\t4
                    """;
            assertViewRows(viewRows);
            final int pageKind = rollBackTheHead();

            // The rebuild from the applied base would drop the lost hour's rows, so the view stops
            // on the rows it holds rather than serve one computed over a head short of a row: the
            // healed head would have answered 622.0 over four rows here, against 663.0 over five.
            commitAndRefresh("('" + timestamp(50) + "', 'acct-1', 100.0)");
            assertViewRows(viewRows);
            Assert.assertTrue(instance("lv").isCheckpointRecoveryBlocked());
            capture.drain();
            capture.assertLogged("live view checkpoint restore fell back past corrupt roots, reconstructing");
            capture.assertLogged(HEAL_DECLINED_ROWS + " [view=lv, lowTsExclusive=2026-01-01T09:00:30.000000Z, "
                    + "highTsInclusive=2026-01-01T09:00:40.000000Z, durableRows=1, baseRows=0]");
            capture.assertNotLogged("reconstructed corrupt live view checkpoint roots");

            // A declined heal leaves the timeline as the build that sealed it left it, so that
            // build comes back on the head it sealed, and the row above it reads 663.0 over five.
            rollForwardTheNewestHeads(1, pageKind);
            assertRestoredFromTimeline("lv");
            Assert.assertFalse(instance("lv").isCheckpointRecoveryBlocked());
            assertNoRefreshFaults("lv");
            assertViewRows(viewRows + "2026-01-01T09:00:50.000000Z\tacct-1\t663.0\t5\n");
        });
    }

    @Test
    public void testAHealDeclinesWhenTheBaseLostRowsBelowAnUnconsumedRowInAHigherInterval() throws Exception {
        assertMemoryLeak(() -> {
            // The two newest heads are unreadable, so the heal folds two intervals above their
            // predecessor. The base lost the hour the lower interval starts in, and applied an
            // out-of-order row into the upper interval that the view has not consumed: a row short
            // in one interval, a row over in the other, and in total exactly the rows the view's
            // table holds. Healed, the lower head would lack 08:59:55's row while its position
            // counts it, and the drain's repair of the late row resumes from that head.
            seedFiveBoundariesAcrossAnHour();
            execute("ALTER TABLE tx DROP PARTITION LIST '2026-01-01T08'");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            final String viewRows = """
                    created_at\taccount_id\tcumulative_sum\tcumulative_count
                    2026-01-01T08:59:30.000000Z\tacct-2\t1.0\t1
                    2026-01-01T08:59:40.000000Z\tacct-1\t2.0\t1
                    2026-01-01T08:59:50.000000Z\tacct-2\t5.0\t2
                    2026-01-01T08:59:55.000000Z\tacct-1\t10.0\t2
                    2026-01-01T09:00:00.000000Z\tacct-2\t21.0\t3
                    2026-01-01T09:00:10.000000Z\tacct-1\t42.0\t3
                    """;
            assertViewRows(viewRows);
            execute("INSERT INTO tx VALUES ('" + timestamp(5) + "', 'acct-1', 1000.0)");
            drainWalQueue();
            final int pageKind = rollBackTheNewestHeads(2);

            // The rebuild from the applied base would drop the lost hour's rows, so the view stops on
            // the rows it holds. Healed, it would have answered 1002.0 over two rows at 09:00:05 and
            // 1034.0 over three at 09:00:10.
            assertViewRows(viewRows);
            Assert.assertTrue(instance("lv").isCheckpointRecoveryBlocked());
            capture.drain();
            capture.assertLogged("live view checkpoint restore fell back past corrupt roots, reconstructing");
            capture.assertLogged(HEAL_DECLINED_ROWS + " [view=lv, lowTsExclusive=2026-01-01T08:59:50.000000Z, "
                    + "highTsInclusive=2026-01-01T09:00:00.000000Z, durableRows=2, baseRows=1]");
            capture.assertNotLogged("reconstructed corrupt live view checkpoint roots");

            // The build that sealed both heads comes back on them, and its repair of the late row
            // resumes from the lower head it sealed, which holds 08:59:55's row.
            rollForwardTheNewestHeads(2, pageKind);
            assertRestoredFromTimeline("lv");
            Assert.assertFalse(instance("lv").isCheckpointRecoveryBlocked());
            Assert.assertTrue("the drain must repair the late row's commit", repairedRows() > 0);
            assertNoRefreshFaults("lv");
            assertViewRows("""
                    created_at\taccount_id\tcumulative_sum\tcumulative_count
                    2026-01-01T08:59:30.000000Z\tacct-2\t1.0\t1
                    2026-01-01T08:59:40.000000Z\tacct-1\t2.0\t1
                    2026-01-01T08:59:50.000000Z\tacct-2\t5.0\t2
                    2026-01-01T08:59:55.000000Z\tacct-1\t10.0\t2
                    2026-01-01T09:00:00.000000Z\tacct-2\t21.0\t3
                    2026-01-01T09:00:05.000000Z\tacct-1\t1010.0\t3
                    2026-01-01T09:00:10.000000Z\tacct-1\t1042.0\t4
                    """);
        });
    }

    @Test
    public void testTwoFutureHeadsAreHealedOverAnUnconsumedOutOfOrderRowInTheLowerInterval() throws Exception {
        assertMemoryLeak(() -> assertTwoFutureHeadsAreHealedOverAnUnconsumedOutOfOrderRow(25, """
                created_at\taccount_id\tcumulative_sum\tcumulative_count
                2025-12-31T23:00:00.000000Z\tacct-1\t500.0\t1
                2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
                2026-01-01T09:00:10.000000Z\tacct-2\t11.0\t1
                2026-01-01T09:00:20.000000Z\tacct-1\t22.0\t2
                2026-01-01T09:00:25.000000Z\tacct-1\t1022.0\t3
                2026-01-01T09:00:30.000000Z\tacct-2\t42.0\t2
                2026-01-01T09:00:40.000000Z\tacct-1\t1063.0\t4
                """));
    }

    @Test
    public void testTwoFutureHeadsAreHealedOverAnUnconsumedOutOfOrderRowInTheUpperInterval() throws Exception {
        assertMemoryLeak(() -> assertTwoFutureHeadsAreHealedOverAnUnconsumedOutOfOrderRow(35, """
                created_at\taccount_id\tcumulative_sum\tcumulative_count
                2025-12-31T23:00:00.000000Z\tacct-1\t500.0\t1
                2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
                2026-01-01T09:00:10.000000Z\tacct-2\t11.0\t1
                2026-01-01T09:00:20.000000Z\tacct-1\t22.0\t2
                2026-01-01T09:00:30.000000Z\tacct-2\t42.0\t2
                2026-01-01T09:00:35.000000Z\tacct-1\t1022.0\t3
                2026-01-01T09:00:40.000000Z\tacct-1\t1063.0\t4
                """));
    }

    private static int crc32(byte[] bytes, int offset, int length) {
        final CRC32 crc = new CRC32();
        crc.update(bytes, offset, length);
        return (int) crc.getValue();
    }

    private static int leInt(byte[] bytes, int offset) {
        return (bytes[offset] & 0xff)
                | ((bytes[offset + 1] & 0xff) << 8)
                | ((bytes[offset + 2] & 0xff) << 16)
                | ((bytes[offset + 3] & 0xff) << 24);
    }

    private static long leLong(byte[] bytes, int offset) {
        return (leInt(bytes, offset) & 0xffff_ffffL) | ((long) leInt(bytes, offset + 4) << 32);
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

    private static String timestamp(int secondOfDay) {
        return DAILY_ANCHOR + String.format("09:%02d:%02d.000000Z", secondOfDay / 60, secondOfDay % 60);
    }

    /**
     * Flips one bit of one format field in the newest superblock slot - rot rather than a write,
     * so the checksum is left stale - and asserts the view neither blocks nor rebuilds: it
     * restores off the generation the other slot names, replays the commit the damaged slot
     * covered, and a later seal overwrites the damage.
     * <p>
     * Bit 0 of the field's low byte turns this build's 2 into 3, which read alone is the next
     * format version. A flip in the version field used to block the view on exactly that reading,
     * costing a DROP and re-CREATE; one in the magic's nibble used to reset the directory and
     * rebuild the view from the base rows that survive today.
     */
    private void assertAFlippedFormatBitFallsBack(int fieldOffset) throws Exception {
        seedFiveBoundaries();
        final File checkpointsRoot = checkpointsRoot();
        shutdown();

        final File timeline = new File(checkpointsRoot, LiveViewCheckpointLayout.TIMELINE_FILE_NAME);
        final byte[] bytes = Files.readAllBytes(timeline.toPath());
        final int generationOffset = LiveViewCheckpointSuperblock.SLOT_GENERATION_OFFSET;
        final long generation0 = leLong(bytes, generationOffset);
        final long generation1 = leLong(bytes, LiveViewCheckpointSuperblock.SLOT_SIZE + generationOffset);
        final int newestSlot = generation1 > generation0 ? 1 : 0;
        final long intactGeneration = Math.min(generation0, generation1);
        bytes[newestSlot * LiveViewCheckpointSuperblock.SLOT_SIZE + fieldOffset] ^= 1;
        Files.write(timeline.toPath(), bytes);

        restart();
        capture.drain();
        capture.assertNotLogged("live view checkpoint timeline declares an unsupported format version");
        capture.assertNotLogged("live view checkpoint timeline carries a foreign layout version");
        capture.assertNotLogged("live view checkpoint directory was written by another format");
        capture.assertNotLogged("live view restart rebuilding from applied base");

        final LiveViewInstance instance = instance("lv");
        Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
        Assert.assertFalse(instance.isInvalid());
        assertRestoredFromTimeline("lv");
        Assert.assertEquals(
                "the restore must come back on the generation the intact slot names",
                intactGeneration,
                instance.getCheckpointRestoreGeneration()
        );
        assertNoRefreshFaults("lv");
        assertViewMatchesRecompute();

        // A commit seals over the slot selection passed over, and a restart comes back on the
        // generation that seal published.
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            execute("INSERT INTO tx VALUES ('" + timestamp(50) + "', 'acct-1', 100.0)");
            drainWalQueue();
            driveRefreshToQuiescence(job);
        }
        shutdown();
        restart();
        assertRestoredFromTimeline("lv");
        Assert.assertTrue(
                "the restart must restore off a generation sealed after the damage",
                instance("lv").getCheckpointRestoreGeneration() > intactGeneration
        );
        assertNoRefreshFaults("lv");
        assertViewMatchesRecompute();
        assertQuery("SELECT created_at, account_id, cumulative_sum, cumulative_count FROM lv")
                .noLeakCheck()
                .timestamp("created_at")
                .expectSize()
                .returns("created_at\taccount_id\tcumulative_sum\tcumulative_count\n" +
                        "2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1\n" +
                        "2026-01-01T09:00:10.000000Z\tacct-2\t11.0\t1\n" +
                        "2026-01-01T09:00:20.000000Z\tacct-1\t22.0\t2\n" +
                        "2026-01-01T09:00:30.000000Z\tacct-2\t42.0\t2\n" +
                        "2026-01-01T09:00:40.000000Z\tacct-1\t63.0\t3\n" +
                        "2026-01-01T09:00:50.000000Z\tacct-1\t163.0\t4\n");
        final byte[] healed = Files.readAllBytes(timeline.toPath());
        for (int slot = 0; slot < 2; slot++) {
            final int base = slot * LiveViewCheckpointSuperblock.SLOT_SIZE;
            Assert.assertEquals(
                    "slot " + slot + " must carry this build's magic again",
                    LiveViewCheckpointSuperblock.SLOT_MAGIC,
                    leLong(healed, base + LiveViewCheckpointSuperblock.SLOT_MAGIC_OFFSET)
            );
            Assert.assertEquals(
                    "slot " + slot + " must carry this build's version again",
                    LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION,
                    leInt(healed, base + LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION_OFFSET)
            );
            Assert.assertEquals(
                    "slot " + slot + " must checksum again",
                    crc32(healed, base, LiveViewCheckpointSuperblock.SLOT_CRC_COVERAGE),
                    leInt(healed, base + LiveViewCheckpointSuperblock.SLOT_CRC_OFFSET)
            );
        }
    }

    /**
     * Asserts a future revision of a structure this build does know is refused by version rather
     * than decoded on the old field offsets. The expected version is pinned rather than derived,
     * so a later change to this branch's own {@code FORMAT_VERSION} has to come back through
     * here.
     */
    private void assertAFutureFormatVersionIsRejected(File checkpointsRoot, PageSite site) {
        try (
                Path dir = new Path().of(checkpointsRoot.getAbsolutePath());
                LiveViewCheckpointWindowRoot windowRoot = new LiveViewCheckpointWindowRoot(engine.getConfiguration())
        ) {
            try {
                windowRoot.ofIfWindowRoot(dir, site.ref());
                Assert.fail("a future structure format version must not decode");
            } catch (CairoException e) {
                TestUtils.assertContains(
                        e.getFlyweightMessage(),
                        "window state root format version mismatch [expected=1, actual="
                                + FUTURE_STRUCTURE_FORMAT_VERSION + ']'
                );
            } finally {
                windowRoot.detach();
            }
        }
    }

    /**
     * Asserts the state-root decoder refuses a page kind this build does not know, rather than
     * either claiming it or reporting it as damage.
     * <p>
     * Both halves matter. The probe answering yes would hand a newer shape to this build's
     * decoder on the old field offsets, which is the misread a tagged union used to make
     * reachable. The strict decode answering {@code metadata page checksum mismatch} would mean
     * the build cannot tell a newer format from a corrupt one - the page's checksum agrees with
     * its body here, so the only honest complaint is about the kind.
     */
    private void assertAFuturePageKindIsRejectedRatherThanMisread(File checkpointsRoot, PageSite site) {
        try (
                Path dir = new Path().of(checkpointsRoot.getAbsolutePath());
                LiveViewCheckpointWindowRoot windowRoot = new LiveViewCheckpointWindowRoot(engine.getConfiguration())
        ) {
            Assert.assertFalse(
                    "the probe must decline a page kind this build does not know",
                    windowRoot.ofIfWindowRoot(dir, site.ref())
            );
            try {
                windowRoot.of(dir, site.ref());
                Assert.fail("a page kind this build does not know must not decode as a state root");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "window state root page kind unknown");
            } finally {
                windowRoot.detach();
            }
        }
    }

    /**
     * Asserts the third gate fired, and that the operator can tell which kind of unreadable it
     * met. The walk's own exhaustion message describes only the walk, so the refusal that
     * started it is quoted into it - the difference between "this directory is damaged" and
     * "this directory is newer than me", over bytes whose every checksum agrees.
     */
    private void assertTheDecoderGateRefused(String refusal) {
        capture.drain();
        capture.assertNotLogged("live view checkpoint timeline carries a foreign layout version");
        capture.assertNotLogged("live view checkpoint directory holds an entry outside the current layout");
        capture.assertLogged("could not restore live view from checkpoint timeline, rebuilding derived state");
        capture.assertLogged("newestRefusal=live view checkpoint " + refusal);
    }

    /**
     * Asserts the outcome every case here shares: the view survived a tree it could not read, it
     * is correct, and it discarded the tree rather than adopting part of it.
     * <p>
     * {@code isCheckpointRestoreSucceeded()} is deliberately not the witness. The rebuild path
     * sets it too - it reports that the restart resolved its derived state, not which way - so
     * the discriminator has to be the lineage: a rebuild retires the timeline first, which takes
     * the boundary ladder back to what a single replay seals.
     */
    private void assertRebuiltFromTheBase() throws Exception {
        final LiveViewInstance instance = instance("lv");
        Assert.assertFalse("a checkpoint tree this build cannot read must not invalidate the view", instance.isInvalid());
        Assert.assertTrue("the restore must have run", instance.isCheckpointRestoreAttempted());
        assertNoRefreshFaults("lv");
        Assert.assertTrue(
                "the unreadable timeline must be retired, not carried forward",
                countSealedBoundaries("lv") < BOUNDARIES
        );
        assertViewMatchesRecompute();
    }

    /**
     * Drives a commit and a further restart over whatever the recovery left behind, so a case
     * proves the view came back on a healthy generation rather than one that merely happened to
     * hold the right rows once.
     */
    private void assertRestartsCleanlyAfterwards() throws Exception {
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            execute("INSERT INTO tx VALUES ('" + timestamp(50) + "', 'acct-1', 100.0)");
            drainWalQueue();
            driveRefreshToQuiescence(job);
        }
        assertViewMatchesRecompute();

        shutdown();
        restart();
        final LiveViewInstance instance = instance("lv");
        Assert.assertFalse("the recovered view must stay valid across a restart", instance.isInvalid());
        Assert.assertTrue(
                "the restart must restore off the generation the recovery published",
                instance.isCheckpointRestoreSucceeded()
        );
        assertNoRefreshFaults("lv");
        assertViewMatchesRecompute();

        assertQuery("SELECT created_at, account_id, cumulative_sum, cumulative_count FROM lv")
                .timestamp("created_at")
                .expectSize()
                .returns("created_at\taccount_id\tcumulative_sum\tcumulative_count\n" +
                        "2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1\n" +
                        "2026-01-01T09:00:10.000000Z\tacct-2\t11.0\t1\n" +
                        "2026-01-01T09:00:20.000000Z\tacct-1\t22.0\t2\n" +
                        "2026-01-01T09:00:30.000000Z\tacct-2\t42.0\t2\n" +
                        "2026-01-01T09:00:40.000000Z\tacct-1\t63.0\t3\n" +
                        "2026-01-01T09:00:50.000000Z\tacct-1\t163.0\t4\n");
    }

    /**
     * The two newest heads are unreadable over a base that lost an hour of the previous day, so a
     * rebuild from the applied base is refused, and the base applied an out-of-order row at
     * {@code lateSecond}, which the view has not consumed, into one of the two intervals the heal
     * folds. The other interval folds exactly the rows the view's table holds. A pure surplus loses
     * the view no row, so the heal stands on both heads, and the drain repairs the row's commit from
     * a boundary below it.
     */
    private void assertTwoFutureHeadsAreHealedOverAnUnconsumedOutOfOrderRow(int lateSecond, String expectedRows) throws Exception {
        seedFiveBoundaries("('2025-12-31T23:00:00.000000Z', 'acct-1', 500.0)", -1, null);
        execute("ALTER TABLE tx DROP PARTITION LIST '2025-12-31T23'");
        drainWalQueue();
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveRefreshToQuiescence(job);
        }
        execute("INSERT INTO tx VALUES ('" + timestamp(lateSecond) + "', 'acct-1', 1000.0)");
        drainWalQueue();
        rollBackTheNewestHeads(2);

        capture.drain();
        capture.assertLogged("reconstructed corrupt live view checkpoint roots [view=lv, "
                + "predecessorMaxTs=2026-01-01T09:00:20.000000Z, corruptCeilingMaxTs=2026-01-01T09:00:40.000000Z, roots=2");
        capture.assertNotLogged(HEAL_DECLINED_ROWS);
        assertRestoredFromTimeline("lv");
        Assert.assertFalse("the heal must not leave the view blocked", instance("lv").isCheckpointRecoveryBlocked());
        Assert.assertTrue("the drain must repair the late row's commit", repairedRows() > 0);
        assertNoRefreshFaults("lv");
        assertViewRows(expectedRows);

        // New rows keep arriving, and across a restart.
        commitAndRefresh("('" + timestamp(50) + "', 'acct-1', 100.0)");
        final String grownRows = expectedRows + "2026-01-01T09:00:50.000000Z\tacct-1\t1163.0\t5\n";
        assertViewRows(grownRows);
        shutdown();
        restart();
        assertRestoredFromTimeline("lv");
        Assert.assertFalse(instance("lv").isCheckpointRecoveryBlocked());
        assertNoRefreshFaults("lv");
        assertViewRows(grownRows);
    }

    private void assertViewRows(String expected) throws Exception {
        assertQuery("SELECT created_at, account_id, cumulative_sum, cumulative_count FROM lv")
                .noLeakCheck()
                .timestamp("created_at")
                .expectSize()
                .returns(expected);
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
     * Asserts the view still serves exactly the rows the other build materialized. Read through
     * the ordinary cursor, so it covers what a user querying a blocked view gets.
     */
    private void assertBlockedViewRows() throws Exception {
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
     * Counts the base table's WAL segment directories - what the purge job reclaims once no
     * consumer's floor holds them.
     */
    private long countWalSegments(String tableDirName) throws IOException {
        final File tableDir = new File(engine.getConfiguration().getDbRoot(), tableDirName);
        final File[] walDirs = tableDir.listFiles(f -> f.isDirectory() && f.getName().startsWith(WalUtils.WAL_NAME_BASE));
        if (walDirs == null) {
            return 0;
        }
        long segments = 0;
        for (File walDir : walDirs) {
            final File[] segmentDirs = walDir.listFiles(File::isDirectory);
            if (segmentDirs != null) {
                segments += segmentDirs.length;
            }
        }
        return segments;
    }

    /**
     * Rewrites both superblock slots to name {@code formatVersion} the way a build of that
     * version stamps one - in the version field and in the magic's trailing nibble, checksum and
     * all - so the slots are a real generation of that format rather than a torn write or a
     * flipped field. Both slots, and both directions: the same helper stamps a version this build
     * does not implement and stamps its own back, which is how a case can show that the block
     * held the directory intact for the build that does read it.
     */
    private void setSuperblockFormatVersion(File checkpointsRoot, int formatVersion) throws IOException {
        final File file = new File(checkpointsRoot, LiveViewCheckpointLayout.TIMELINE_FILE_NAME);
        final byte[] bytes = Files.readAllBytes(file.toPath());
        for (int slot = 0; slot < 2; slot++) {
            final int base = slot * LiveViewCheckpointSuperblock.SLOT_SIZE;
            putLeLong(
                    bytes,
                    base + LiveViewCheckpointSuperblock.SLOT_MAGIC_OFFSET,
                    LiveViewCheckpointSuperblock.SLOT_MAGIC_FAMILY | formatVersion
            );
            putLeInt(
                    bytes,
                    base + LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION_OFFSET,
                    formatVersion
            );
            putLeInt(
                    bytes,
                    base + LiveViewCheckpointSuperblock.SLOT_CRC_OFFSET,
                    crc32(bytes, base, LiveViewCheckpointSuperblock.SLOT_CRC_COVERAGE)
            );
        }
        Files.write(file.toPath(), bytes);
    }

    private File checkpointsRoot() {
        return new File(
                new File(engine.getConfiguration().getDbRoot(), instance("lv").getLiveViewToken().getDirName()),
                LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME
        );
    }

    private void commitAndRefresh(String values) throws Exception {
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            commitAndRefresh(job, values);
        }
    }

    private void commitAndRefresh(LiveViewRefreshJob job, String values) throws Exception {
        execute("INSERT INTO tx VALUES " + values);
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    private void createView() throws Exception {
        execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL, amount DOUBLE) "
                + "TIMESTAMP(created_at) PARTITION BY HOUR WAL");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                + "SELECT created_at, account_id, sum(amount) OVER w AS cumulative_sum, "
                + "count(account_id) OVER w AS cumulative_count "
                + "FROM tx WINDOW w AS (PARTITION BY account_id ORDER BY created_at ANCHOR DAILY '00:00')");
    }

    private File metaSegmentFile(File checkpointsRoot, long segmentId) {
        final StringBuilder name = new StringBuilder(LiveViewCheckpointLayout.META_SEGMENT_PREFIX);
        final String digits = Long.toString(segmentId);
        for (int i = digits.length(); i < LiveViewCheckpointLayout.ID_PAD_LEN; i++) {
            name.append('0');
        }
        return new File(new File(checkpointsRoot, LiveViewCheckpointLayout.META_DIR_NAME), name.append(digits).toString());
    }

    private int readMetaPageInt(File checkpointsRoot, PageSite site, int fieldOffset) throws IOException {
        final byte[] bytes = Files.readAllBytes(metaSegmentFile(checkpointsRoot, site.segmentId).toPath());
        return leInt(bytes, (int) site.offset + fieldOffset);
    }

    // Base rows the drain's out-of-order repairs replayed since the restart, through either
    // disposition. In-order appends leave both at zero.
    private long repairedRows() {
        final LiveViewInstance instance = instance("lv");
        return instance.getO3BoundaryReplayRows() + instance.getO3ResumeReplayRows();
    }

    private void restart() {
        engine.buildViewGraphs();
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveRefreshToQuiescence(job);
        }
    }

    /**
     * Rewrites one INT of one metadata page and repairs the page checksum over it.
     * <p>
     * Repairing the checksum is what separates this from the suite's corruption cases. Leaving
     * it stale would make every injection here fail as {@code metadata page checksum mismatch}
     * before any decoder looked at the field, which proves the CRC works and nothing about what
     * this build does with an intact page it does not understand.
     */
    private void rewriteMetaPageInt(File checkpointsRoot, PageSite site, int fieldOffset, int value)
            throws IOException {
        final File file = metaSegmentFile(checkpointsRoot, site.segmentId);
        final byte[] bytes = Files.readAllBytes(file.toPath());
        final int pageStart = (int) site.offset;
        // crc INT, payloadLength INT, pageKind INT, payload - and the CRC covers everything from
        // the length field on.
        Assert.assertEquals(
                "the page must be the length the reference that reached it claims",
                site.length - LiveViewCheckpointLayout.PAGE_HEADER_SIZE,
                leInt(bytes, pageStart + LiveViewCheckpointLayout.PAGE_LENGTH_OFFSET)
        );
        putLeInt(bytes, pageStart + fieldOffset, value);
        putLeInt(
                bytes,
                pageStart + LiveViewCheckpointLayout.PAGE_CRC_OFFSET,
                crc32(bytes, pageStart + Integer.BYTES, site.length - Integer.BYTES)
        );
        Files.write(file.toPath(), bytes);
    }

    /**
     * Rewrites the head boundary's state root into the page kind a newer build would write - the
     * rollback taken right after that build sealed the head - and restarts.
     *
     * @return the page kind the head carried, for {@link #rollForwardTheNewestHeads(int, int)}
     */
    private int rollBackTheHead() throws IOException {
        return rollBackTheNewestHeads(1);
    }

    /**
     * {@link #rollBackTheHead()} for the newer build having sealed the {@code heads} newest
     * boundaries before the rollback.
     *
     * @return the page kind those boundaries carried, which this build sealed them with
     */
    private int rollBackTheNewestHeads(int heads) throws IOException {
        final File checkpointsRoot = checkpointsRoot();
        final ObjList<PageSite> stateRoots = stateRootSites();
        final int pageKind = readMetaPageInt(
                checkpointsRoot,
                stateRoots.getQuick(stateRoots.size() - 1),
                LiveViewCheckpointLayout.PAGE_KIND_OFFSET
        );
        shutdown();
        for (int i = stateRoots.size() - heads, n = stateRoots.size(); i < n; i++) {
            rewriteMetaPageInt(checkpointsRoot, stateRoots.getQuick(i), LiveViewCheckpointLayout.PAGE_KIND_OFFSET, FUTURE_PAGE_KIND);
        }
        restart();
        return pageKind;
    }

    /**
     * Undoes {@link #rollBackTheNewestHeads(int)} - the build that reads those heads comes back -
     * and restarts.
     */
    private void rollForwardTheNewestHeads(int heads, int pageKind) throws IOException {
        final File checkpointsRoot = checkpointsRoot();
        final ObjList<PageSite> stateRoots = stateRootSites();
        shutdown();
        for (int i = stateRoots.size() - heads, n = stateRoots.size(); i < n; i++) {
            rewriteMetaPageInt(checkpointsRoot, stateRoots.getQuick(i), LiveViewCheckpointLayout.PAGE_KIND_OFFSET, pageKind);
        }
        restart();
    }

    /**
     * Builds a live view whose sealed shape is the fused window root, one boundary per commit.
     */
    private void seedFiveBoundaries() throws Exception {
        seedFiveBoundaries(null, -1, null);
    }

    /**
     * Five fused boundaries, one per commit, the two newest above the hour the three oldest sit
     * in. The commit that seals the fourth boundary carries a row in that earlier hour as well,
     * so the interval between the third and fourth boundaries straddles the hour.
     */
    private void seedFiveBoundariesAcrossAnHour() throws Exception {
        createView();
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveSeedToCompletion(job, "lv");
            commitAndRefresh(job, "('2026-01-01T08:59:30.000000Z', 'acct-2', 1.0)");
            commitAndRefresh(job, "('2026-01-01T08:59:40.000000Z', 'acct-1', 2.0)");
            commitAndRefresh(job, "('2026-01-01T08:59:50.000000Z', 'acct-2', 4.0)");
            commitAndRefresh(job, "('2026-01-01T08:59:55.000000Z', 'acct-1', 8.0), ('" + timestamp(0) + "', 'acct-2', 16.0)");
            commitAndRefresh(job, "('" + timestamp(10) + "', 'acct-1', 32.0)");
        }
        Assert.assertEquals("the seed must leave one boundary per commit", BOUNDARIES, countSealedBoundaries("lv"));
        Assert.assertTrue(isFusedHead("lv"));
        assertNoRefreshFaults("lv");
    }

    /**
     * {@link #seedFiveBoundaries()}, with two optional extras. {@code earlierRow} rides in the
     * first commit, below the first boundary. {@code tieAccount} commits a row of 1000.0 on the
     * boundary at {@code tieSecond} right after that boundary sealed: the cadence seals no second
     * boundary on one timestamp, so the tie leaves that boundary describing part of its group.
     */
    private void seedFiveBoundaries(String earlierRow, int tieSecond, String tieAccount) throws Exception {
        createView();
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveSeedToCompletion(job, "lv");
            for (int second = 0; second <= 40; second += 10) {
                final String row = "('" + timestamp(second) + "', '"
                        + (second % 20 == 0 ? "acct-1" : "acct-2") + "', " + (second + 1.0) + ")";
                execute("INSERT INTO tx VALUES " + (second == 0 && earlierRow != null ? earlierRow + ", " + row : row));
                drainWalQueue();
                driveRefreshToQuiescence(job);
                if (second == tieSecond) {
                    execute("INSERT INTO tx VALUES ('" + timestamp(second) + "', '" + tieAccount + "', 1000.0)");
                    drainWalQueue();
                    driveRefreshToQuiescence(job);
                }
            }
        }

        Assert.assertEquals("the seed must leave one boundary per commit", BOUNDARIES, countSealedBoundaries("lv"));
        Assert.assertTrue("this build must seal the fused shape it is being asked to outgrow", isFusedHead("lv"));
        assertNoRefreshFaults("lv");
    }

    /**
     * Releases every mapped file the view holds, so the injections below rewrite bytes nothing is
     * reading. Pairs with {@link #restart()}, which is the restart itself.
     */
    private void shutdown() {
        engine.getLiveViewRegistry().clear();
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        engine.releaseInactive();
    }

    /**
     * Locates the state root page of every sealed boundary, oldest first.
     * <p>
     * Two passes rather than one: the timeline visitor hands out a flyweight entry that the next
     * page overwrites, so descending into the boundary root from inside the callback would be
     * reading the reference it is about to invalidate.
     */
    private ObjList<PageSite> stateRootSites() {
        final LiveViewInstance instance = instance("lv");
        final LongList boundaryRoots = new LongList();
        try (
                Path checkpointsDir = checkpointsDir(instance);
                LiveViewCheckpointMetaStore store = openStore(instance);
                LiveViewCheckpointTimelineReader timeline = openTimelineReader(instance);
                LiveViewCheckpointGenerationPin pin = store.pin()
        ) {
            timeline.iterateAll(pin.getTimelineRootRef(), entry -> {
                boundaryRoots.add(entry.rootRef.getSegmentId());
                boundaryRoots.add(entry.rootRef.getOffset());
                boundaryRoots.add(entry.rootRef.getLength());
            });
        }

        final ObjList<PageSite> sites = new ObjList<>();
        try (
                Path checkpointsDir = checkpointsDir(instance);
                LiveViewCheckpointRoot root = new LiveViewCheckpointRoot(engine.getConfiguration())
        ) {
            final LiveViewCheckpointPageRef boundaryRef = new LiveViewCheckpointPageRef();
            final LiveViewCheckpointPageRef stateRootRef = new LiveViewCheckpointPageRef();
            for (int i = 0, n = boundaryRoots.size(); i < n; i += 3) {
                boundaryRef.of(
                        boundaryRoots.getQuick(i),
                        boundaryRoots.getQuick(i + 1),
                        (int) boundaryRoots.getQuick(i + 2)
                );
                root.of(checkpointsDir, boundaryRef);
                root.getStateRootRef(stateRootRef);
                Assert.assertFalse("every sealed boundary must name a state root", stateRootRef.isNull());
                sites.add(new PageSite(
                        stateRootRef.getSegmentId(),
                        stateRootRef.getOffset(),
                        stateRootRef.getLength()
                ));
            }
        }
        Assert.assertEquals("one state root per sealed boundary", BOUNDARIES, sites.size());
        return sites;
    }

    /**
     * Where one metadata page sits: the segment file that holds it, its offset in that file and
     * its total framed length.
     */
    private static final class PageSite {
        final int length;
        final long offset;
        final long segmentId;

        PageSite(long segmentId, long offset, int length) {
            this.segmentId = segmentId;
            this.offset = offset;
            this.length = length;
        }

        LiveViewCheckpointPageRef ref() {
            return new LiveViewCheckpointPageRef().of(segmentId, offset, length);
        }
    }
}
