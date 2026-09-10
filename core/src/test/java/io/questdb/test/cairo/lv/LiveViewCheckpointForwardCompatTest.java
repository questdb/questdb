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
 * and the answer there is "restore it". This case is the other direction: a user upgrades,
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
 *     <li>the superblock's magic and layout version - {@code isForeignFormat} resets the whole
 *     directory;</li>
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
            // owned this directory would have left: one declaring slot is enough to block,
            // by the same rule that lets one foreign slot condemn the file.
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

    private static void putLeInt(byte[] bytes, int offset, int value) {
        bytes[offset] = (byte) value;
        bytes[offset + 1] = (byte) (value >>> 8);
        bytes[offset + 2] = (byte) (value >>> 16);
        bytes[offset + 3] = (byte) (value >>> 24);
    }

    private static String timestamp(int secondOfDay) {
        return DAILY_ANCHOR + String.format("09:%02d:%02d.000000Z", secondOfDay / 60, secondOfDay % 60);
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
     * Rewrites both superblock slots to name {@code formatVersion}, checksum and all, so the
     * slots are a real generation of that format rather than a torn write. Both slots, and both
     * directions: the same helper stamps a version this build does not implement and stamps its
     * own back, which is how a case can show that the block held the directory intact for the
     * build that does read it.
     */
    private void setSuperblockFormatVersion(File checkpointsRoot, int formatVersion) throws IOException {
        final File file = new File(checkpointsRoot, LiveViewCheckpointLayout.TIMELINE_FILE_NAME);
        final byte[] bytes = Files.readAllBytes(file.toPath());
        for (int slot = 0; slot < 2; slot++) {
            final int base = slot * LiveViewCheckpointSuperblock.SLOT_SIZE;
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

    private File metaSegmentFile(File checkpointsRoot, long segmentId) {
        final StringBuilder name = new StringBuilder(LiveViewCheckpointLayout.META_SEGMENT_PREFIX);
        final String digits = Long.toString(segmentId);
        for (int i = digits.length(); i < LiveViewCheckpointLayout.ID_PAD_LEN; i++) {
            name.append('0');
        }
        return new File(new File(checkpointsRoot, LiveViewCheckpointLayout.META_DIR_NAME), name.append(digits).toString());
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
     * Builds a live view whose sealed shape is the fused window root, one boundary per commit.
     */
    private void seedFiveBoundaries() throws Exception {
        execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL, amount DOUBLE) "
                + "TIMESTAMP(created_at) PARTITION BY HOUR WAL");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                + "SELECT created_at, account_id, sum(amount) OVER w AS cumulative_sum, "
                + "count(account_id) OVER w AS cumulative_count "
                + "FROM tx WINDOW w AS (PARTITION BY account_id ORDER BY created_at ANCHOR DAILY '00:00')");

        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveSeedToCompletion(job, "lv");
            for (int second = 0; second <= 40; second += 10) {
                execute("INSERT INTO tx VALUES ('" + timestamp(second) + "', '"
                        + (second % 20 == 0 ? "acct-1" : "acct-2") + "', " + (second + 1.0) + ")");
                drainWalQueue();
                driveRefreshToQuiescence(job);
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
