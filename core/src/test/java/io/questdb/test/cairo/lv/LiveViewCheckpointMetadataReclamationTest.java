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
import io.questdb.cairo.lv.LiveViewCheckpointFunctionDirectory;
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointLifecycle;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointRoot;
import io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectory;
import io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectoryReader;
import io.questdb.cairo.lv.LiveViewCheckpointSuperblock;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineReader;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreWriter;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.std.Chars;
import io.questdb.std.Numbers;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Coverage for metadata segment reclamation, in the two units the catalogue keeps.
 * <p>
 * The three trees the superblock roots - the timeline, the row-position delta
 * index and the segment catalogue itself - have one live version at a time, so
 * their segments count <em>pages</em>: a publication adds the pages it wrote and
 * releases the ones its path copy replaced, and a B+ tree page is named exactly
 * once, by its parent or by the superblock's root reference, which is what makes
 * that exact. Boundary metadata - checkpoint roots, anchor roots, function roots
 * and the partition-map pages below them - has one live version per surviving
 * boundary instead, and boundaries retire in bulk, so its segments count
 * <em>roots</em> exactly as data segments do: each root states the segments its
 * whole closure names, and a repair splice or a truncate releases them in one
 * reference transaction.
 * <p>
 * Before this, nothing ever removed a file from {@code meta/}. Every seal writes
 * one segment for its timeline path copy and one for the catalogue path copy, and
 * both are superseded by the next seal, so a view sealing on a five-minute cadence
 * left 576 dead files a day behind with no mechanism able to reclaim them; beside
 * them, every boundary a repair re-versioned or a truncate dropped left its whole
 * closure behind for good.
 * <p>
 * What a cadence seal leaves is retained state rather than garbage - its boundary
 * stays live - so it is still there after these cases run, and only a retention
 * horizon can retire it. Each case therefore pairs its structural assertion with a
 * restart and the from-base recompute oracle, so a count that retired a segment
 * one page or one root too early surfaces as a failed restore rather than as a
 * saving.
 */
public class LiveViewCheckpointMetadataReclamationTest extends AbstractLiveViewTest {

    // ANCHOR compiles only inside a live view, and every row these cases commit
    // lands on the same calendar day, so a plain row_number() over the same
    // partition and order is exactly what the anchored view must produce.
    private static final String ANCHORED_RECOMPUTE_SQL =
            "SELECT ts, sym, row_number() OVER (PARTITION BY sym ORDER BY ts) AS s FROM base";
    private static final String ANCHORED_VIEW_SQL = "SELECT ts, sym, row_number() OVER w AS s FROM base " +
            "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')";
    private static final int KEYS = 4;
    // Two measurement points far enough apart that unbounded growth cannot hide.
    private static final int SEALS_EARLY = 8;
    private static final int SEALS_LATE = 32;
    private static final String VIEW_SQL = "SELECT ts, sym, sum(x) OVER (" +
            "PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW" +
            ") AS s FROM base";
    private String viewSql = VIEW_SQL;

    @After
    public void resetClock() {
        setCurrentMicros(-1);
    }

    @Before
    public void setUpCadence() {
        // One logical boundary per commit, so a seal is exactly a commit and the
        // per-seal metadata the case measures is not averaged over a cadence window.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setCurrentMicros(0);
    }

    @Test
    public void testCrashBeforeSuperblockPublicationLeavesTheCatalogueConsistent() throws Exception {
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int seal = 1; seal <= 4; seal++) {
                    commit(job, seal);
                }
                final LiveViewInstance instance = viewInstance();

                // A publication that wrote its metadata segments and then died
                // leaves them unreferenced by any durable slot. The retry allocates
                // fresh ids past them, and neither the retry's accounting nor the
                // sweep may confuse the orphans with the segments the surviving
                // generation still reaches.
                job.setCheckpointTimelineTestFailureStage(
                        LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH
                );
                commit(job, 5);
                job.setCheckpointTimelineTestFailureStage(0);
                commit(job, 6);
                driveRefreshToQuiescence(job);

                purgeCycle(instance);
                assertCatalogueMatchesDisk(instance);

                restartCycle();
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testDeferredDirectoryRegistrationExecutesOnePublicationLate() throws Exception {
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                commit(job, 1);
                final LiveViewInstance instance = viewInstance();

                // The tree cannot carry an entry naming the file it is being
                // written into, so the segment it landed in is pending rather than
                // catalogued.
                final long firstPending = pendingDirectorySegmentId(instance);
                Assert.assertNotEquals(
                        "a seal that staged a catalogue mutation writes a directory segment",
                        Numbers.LONG_NULL,
                        firstPending
                );
                Assert.assertFalse(
                        "the directory segment must not be in the catalogue it carries",
                        catalogue(instance).contains(firstPending)
                );

                commit(job, 2);
                final long secondPending = pendingDirectorySegmentId(instance);
                Assert.assertNotEquals(firstPending, secondPending);
                Assert.assertTrue(
                        "the next publication must register what the previous one left pending",
                        catalogue(instance).contains(firstPending)
                );
                Assert.assertFalse(catalogue(instance).contains(secondPending));

                // And the registration is not a formality: the same publication
                // path-copied that segment's root away, so it retired immediately.
                Assert.assertEquals(
                        "a directory segment whose every page the next publication replaced retires at once",
                        0,
                        referenceCount(instance, firstPending)
                );

                driveRefreshToQuiescence(job);
                restartCycle();
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testBoundaryPagesSharedAcrossSealsOutliveTheBoundaryThatWroteThem() throws Exception {
        assertMemoryLeak(() -> {
            // An anchored window is the sharing case: an anchor value moves once a
            // day, so consecutive seals put a byte-identical entry and the map
            // writer drops it - leaving the newer boundary's anchor root pointing
            // at a map page an older seal wrote, in an older segment.
            createAnchoredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int seal = 1; seal <= 8; seal++) {
                    commit(job, seal);
                }
                driveRefreshToQuiescence(job);
                final LiveViewInstance instance = viewInstance();
                purgeCycle(instance);

                final Set<Long> shared = sharedBoundarySegmentIds(instance);
                Assert.assertFalse(
                        "no boundary metadata segment is named by more than one root, so this case"
                                + " proves nothing about cross-boundary sharing",
                        shared.isEmpty()
                );
                // A segment several boundaries reach must survive the sweep for as
                // long as the last of them does; releasing on the first would take
                // pages a live root still names.
                final Set<Long> metaFiles = metaSegmentIds(instance);
                for (long segmentId : shared) {
                    Assert.assertTrue(
                            "a shared boundary metadata segment was unlinked, segmentId=" + segmentId,
                            metaFiles.contains(segmentId)
                    );
                }

                assertCatalogueMatchesDisk(instance);
                restartCycle();
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testRepairReclaimsTheBoundaryMetadataItReVersions() throws Exception {
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int seal = 1; seal <= 12; seal++) {
                    commit(job, seal);
                }
                driveRefreshToQuiescence(job);
                final LiveViewInstance instance = viewInstance();
                purgeCycle(instance);
                final Set<Long> beforeBoundaries = boundarySegmentIds(instance);
                final Set<Long> beforeKeys = timelineKeys(instance);

                // A correction just below the head converges inside the history the
                // view still holds, so the repair splices: it re-versions the
                // boundaries it replays over and keeps every logical key. Their old
                // checkpoint, function and anchor roots stop being reachable from any
                // surviving root, which is half of where the repair-driven garbage is.
                setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                execute("INSERT INTO base (ts, sym, x) VALUES ('" + timestamp(10 + 12 * 10 - 3) + "', 'k1', 500)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
                driveRefreshToQuiescence(job);
                Assert.assertTrue(
                        "the correction must have taken a repair",
                        instance.getO3BoundaryReplayRows() + instance.getO3ResumeReplayRows() > 0
                );
                // A splice preserves every logical key - which is what makes this the
                // re-versioning case rather than the truncate one below.
                Assert.assertTrue(
                        "a splice must keep every logical boundary it re-versions",
                        timelineKeys(instance).containsAll(beforeKeys)
                );

                // Two more publications, so the fallback A/B slot advances past the
                // generation the repair retired those segments at and the sweep is
                // allowed to act on them.
                commit(job, 13);
                commit(job, 14);
                driveRefreshToQuiescence(job);
                purgeCycle(instance);

                assertReclaimedSomeOf(instance, beforeBoundaries, "the repair");
                assertCatalogueMatchesDisk(instance);
                restartCycle();
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testRepairReclaimsTheMetadataItSupersedes() throws Exception {
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int seal = 1; seal <= 12; seal++) {
                    commit(job, seal);
                }
                driveRefreshToQuiescence(job);
                final LiveViewInstance instance = viewInstance();

                // An out-of-order correction re-versions boundaries through a
                // splice and moves the row-position delta index, so it supersedes
                // timeline and delta pages the ordinary cadence never touches.
                setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                execute("INSERT INTO base (ts, sym, x) VALUES ('" + timestamp(6) + "', 'k0', 500)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
                driveRefreshToQuiescence(job);
                Assert.assertTrue(
                        "the correction must have taken a repair",
                        instance.getO3BoundaryReplayRows() + instance.getO3ResumeReplayRows() > 0
                );

                purgeCycle(instance);
                assertCatalogueMatchesDisk(instance);

                restartCycle();
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testTruncateReclaimsTheBoundaryMetadataOfTheEntriesItDrops() throws Exception {
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int seal = 1; seal <= 12; seal++) {
                    commit(job, seal);
                }
                driveRefreshToQuiescence(job);
                final LiveViewInstance instance = viewInstance();
                purgeCycle(instance);
                final Set<Long> beforeBoundaries = boundarySegmentIds(instance);
                final Set<Long> beforeKeys = timelineKeys(instance);

                // A correction deep enough that the repair cannot classify a
                // converged suffix has no tail worth keeping, so it truncates above
                // the repair floor and re-seals a fresh head over the surviving
                // prefix. Every dropped entry releases its whole closure in one
                // reference transaction.
                setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                execute("INSERT INTO base (ts, sym, x) VALUES ('" + timestamp(65) + "', 'k2', 900)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
                driveRefreshToQuiescence(job);
                Assert.assertTrue(
                        "the correction must have taken a repair",
                        instance.getO3BoundaryReplayRows() + instance.getO3ResumeReplayRows() > 0
                );
                // The discriminator against the splice case: a truncate is the only
                // publication that drops a logical key rather than re-versioning it.
                final Set<Long> dropped = new HashSet<>(beforeKeys);
                dropped.removeAll(timelineKeys(instance));
                Assert.assertFalse(
                        "the correction spliced rather than truncated, so this case tests the wrong publication",
                        dropped.isEmpty()
                );

                commit(job, 13);
                commit(job, 14);
                driveRefreshToQuiescence(job);
                purgeCycle(instance);

                assertReclaimedSomeOf(instance, beforeBoundaries, "the truncate");
                assertCatalogueMatchesDisk(instance);
                restartCycle();
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testSupersededTimelineAndCatalogueSegmentsAreReclaimed() throws Exception {
        assertMemoryLeak(() -> {
            // Reconciliation is the sweep this case measures, so the cadence sweep
            // beside it is off: with both running, purgeCycle finds the files
            // already gone and the case would read as reclaiming nothing.
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_PURGE_INTERVAL, 0);
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int seal = 1; seal <= SEALS_EARLY; seal++) {
                    commit(job, seal);
                }
                driveRefreshToQuiescence(job);
                final LiveViewInstance instance = viewInstance();
                purgeCycle(instance);
                final int earlyLiveMetaSegments = liveTreeMetadataSegmentCount(instance);
                final int earlyMetaFiles = metaSegmentIds(instance).size();

                int purged = 0;
                for (int seal = SEALS_EARLY + 1; seal <= SEALS_LATE; seal++) {
                    commit(job, seal);
                    purged += purgeCycle(instance);
                }
                driveRefreshToQuiescence(job);
                purged += purgeCycle(instance);

                // The headline: what a seal supersedes is reclaimed, so the live
                // metadata a generation reaches stops following the seal count.
                //
                // It is not flat, and the residual is worth naming: the catalogue
                // keeps an entry for every segment ever written - a purge unlinks
                // the file and leaves the entry - so the catalogue tree gains a leaf
                // every leafCapacity entries, and each such leaf is a live metadata
                // page of its own, in whichever segment last rewrote it. That is a
                // pre-existing property of the data-side catalogue, and cataloguing
                // both kinds of metadata segment beside the data ones now puts about
                // five entries per seal through it rather than one. Bounding the
                // catalogue itself needs an entry-retirement path this phase does
                // not add; what this asserts is only that the residual stays well
                // below one segment per seal.
                final int lateLiveMetaSegments = liveTreeMetadataSegmentCount(instance);
                final int extraSeals = SEALS_LATE - SEALS_EARLY;
                Assert.assertTrue(
                        "live metadata segments went " + earlyLiveMetaSegments + " -> " + lateLiveMetaSegments
                                + " over " + extraSeals + " further seals",
                        lateLiveMetaSegments - earlyLiveMetaSegments <= extraSeals / 4
                );

                // And the files really go. Each of the extra seals writes a timeline
                // segment and a catalogue segment that the seal after it supersedes,
                // so the sweep has to reclaim about two per seal - against the three
                // per seal of boundary metadata it correctly leaves alone.
                Assert.assertTrue(
                        "the sweep reclaimed " + purged + " segments over " + extraSeals + " seals",
                        purged >= extraSeals
                );
                final int metaFileGrowth = metaSegmentIds(instance).size() - earlyMetaFiles;
                Assert.assertTrue(
                        "meta/ grew by " + metaFileGrowth + " files over " + extraSeals
                                + " seals, which is no better than keeping every one of them",
                        metaFileGrowth < 4 * extraSeals
                );

                assertCatalogueMatchesDisk(instance);
                restartCycle();
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testTheCatalogueRetiresTheEntriesOfSweptSegments() throws Exception {
        assertMemoryLeak(() -> {
            // The hand-off this case measures is the one a restart's reconciliation
            // makes, so the cadence sweep is off: with it on, the seal after each
            // sweep carries the proposal away and no run of seals ever accumulates
            // the dead entries the case needs to see retired in one publication.
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_PURGE_INTERVAL, 0);
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int seal = 1; seal <= SEALS_LATE; seal++) {
                    commit(job, seal);
                }
                driveRefreshToQuiescence(job);
            }
            final LiveViewInstance swept = viewInstance();
            purgeCycle(swept);

            // The sweep unlinks a superseded segment and leaves its entry behind:
            // the catalogue is copy-on-write and named by the superblock, so only a
            // publication may rewrite it, and the sweep publishes none of its own.
            // That is the last term that grew with the view's age rather than with
            // what it holds - one entry per segment ever written, and a leaf of the
            // catalogue's own tree per leafCapacity of them.
            final Set<Long> deadEntries = deadCatalogueEntries(swept);
            final int catalogueBefore = catalogue(swept).size();
            Assert.assertTrue(
                    "the sweep must leave entries naming unlinked files, dead=" + deadEntries.size()
                            + " of " + catalogueBefore,
                    deadEntries.size() > SEALS_LATE / 2
            );

            // A worker reconciles before its first seal of a directory, so it is
            // that seal which carries the sweep's proposal into the tree.
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                commit(job, SEALS_LATE + 1);
                driveRefreshToQuiescence(job);
            }

            final LiveViewInstance instance = viewInstance();
            final Set<Long> catalogueAfter = catalogue(instance);
            for (long segmentId : deadEntries) {
                Assert.assertFalse(
                        "an entry naming an unlinked segment must be gone, segmentId=" + segmentId,
                        catalogueAfter.contains(segmentId)
                );
            }
            Assert.assertTrue(
                    "the catalogue went " + catalogueBefore + " -> " + catalogueAfter.size()
                            + " over the seal that follows a sweep, so it retired nothing",
                    catalogueAfter.size() < catalogueBefore
            );
            Assert.assertEquals(
                    "the seal itself unlinks nothing, so every surviving entry must name a file",
                    0,
                    deadCatalogueEntries(instance).size()
            );

            assertCatalogueMatchesDisk(instance);
            restartCycle();
            assertViewMatchesRecompute();
        });
    }

    @Test
    public void testTheCadenceSweepCollectsWithoutARestart() throws Exception {
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int seal = 1; seal <= SEALS_EARLY; seal++) {
                    commit(job, seal);
                }
                driveRefreshToQuiescence(job);
                final LiveViewInstance instance = viewInstance();
                final Set<Long> earlyMetaFiles = metaSegmentIds(instance);

                // No purgeCycle and no restart from here on. A worker reconciles a
                // directory once, at its first seal of it, so before the purge
                // cadence existed every segment these seals superseded stayed on
                // disk until the process ended - whatever this run reclaims, the
                // cadence sweep reclaimed.
                for (int seal = SEALS_EARLY + 1; seal <= SEALS_LATE; seal++) {
                    commit(job, seal);
                }
                driveRefreshToQuiescence(job);

                final Set<Long> lateMetaFiles = metaSegmentIds(instance);
                int reclaimed = 0;
                for (long segmentId : earlyMetaFiles) {
                    if (!lateMetaFiles.contains(segmentId)) {
                        reclaimed++;
                    }
                }
                Assert.assertTrue(
                        "not one of the " + earlyMetaFiles.size() + " files present at seal " + SEALS_EARLY
                                + " was unlinked over the seals that followed, so nothing swept inside"
                                + " the process",
                        reclaimed > 0
                );

                // The headline, and what a growing view actually feels: a segment
                // whose reference count reached zero and whose file is still there
                // is garbage waiting for a sweep. Each seal supersedes its
                // predecessor's timeline and catalogue segments, so without the
                // cadence that queue grows by about two per seal and only a restart
                // ever drains it. With it, the queue holds no more than what the
                // fallback slot still protects.
                final int extraSeals = SEALS_LATE - SEALS_EARLY;
                final int uncollected = uncollectedSegmentCount(instance);
                Assert.assertTrue(
                        "the catalogue holds " + uncollected + " zero-reference segments whose files are"
                                + " still on disk after " + SEALS_LATE + " seals",
                        uncollected <= extraSeals / 4
                );

                // The other half of the cadence: the entries those sweeps left
                // naming nothing ride the next seal out of the tree, so the
                // catalogue no longer waits for a restart either.
                final Set<Long> pending = deadCatalogueEntries(instance);
                Assert.assertFalse(
                        "the last sweep left no entry naming an unlinked file, so the hand-off"
                                + " below proves nothing",
                        pending.isEmpty()
                );
                commit(job, SEALS_LATE + 1);
                driveRefreshToQuiescence(job);
                final Set<Long> catalogueAfter = catalogue(instance);
                for (long segmentId : pending) {
                    Assert.assertFalse(
                            "an entry the cadence sweep proposed must be gone after the next seal,"
                                    + " segmentId=" + segmentId,
                            catalogueAfter.contains(segmentId)
                    );
                }

                assertCatalogueMatchesDisk(instance);
                restartCycle();
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testTheCadenceSweepStaysOffAtIntervalZero() throws Exception {
        assertMemoryLeak(() -> {
            // The control that gives the case above its meaning, and the statement
            // of what the interval buys: at zero, reclamation is exactly what it
            // was before the cadence existed - a reconciliation and nothing else.
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_PURGE_INTERVAL, 0);
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int seal = 1; seal <= SEALS_EARLY; seal++) {
                    commit(job, seal);
                }
                driveRefreshToQuiescence(job);
                final LiveViewInstance instance = viewInstance();
                final Set<Long> earlyMetaFiles = metaSegmentIds(instance);

                for (int seal = SEALS_EARLY + 1; seal <= SEALS_LATE; seal++) {
                    commit(job, seal);
                }
                driveRefreshToQuiescence(job);

                final Set<Long> lateMetaFiles = metaSegmentIds(instance);
                for (long segmentId : earlyMetaFiles) {
                    Assert.assertTrue(
                            "a segment was unlinked with the purge cadence disabled, segmentId=" + segmentId,
                            lateMetaFiles.contains(segmentId)
                    );
                }
                // And every segment those seals superseded is still queued, which is
                // the growth the cadence exists to stop: the reconciliation sweep
                // finds the lot of it in one pass.
                final int extraSeals = SEALS_LATE - SEALS_EARLY;
                Assert.assertTrue(
                        "only " + uncollectedSegmentCount(instance) + " zero-reference segments were"
                                + " waiting after " + SEALS_LATE + " seals with no cadence sweep",
                        uncollectedSegmentCount(instance) >= extraSeals
                );
                Assert.assertTrue(
                        "reconciliation reclaimed nothing, so the run left no garbage behind at all",
                        purgeCycle(instance) >= extraSeals
                );

                assertCatalogueMatchesDisk(instance);
                restartCycle();
                assertViewMatchesRecompute();
            }
        });
    }

    private static Path checkpointsDir(LiveViewInstance instance) {
        return new Path().of(configuration.getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    // The metadata segment ids with a final-name file on disk. A purge unlinks the
    // file but leaves the catalogue entry, so on-disk presence is the reclaim test.
    private static Set<Long> metaSegmentIds(LiveViewInstance instance) {
        final Set<Long> ids = new HashSet<>();
        try (Path checkpointsDir = checkpointsDir(instance); Path metaDir = new Path()) {
            LiveViewCheckpointLayout.metaDirPath(metaDir, checkpointsDir);
            final String[] names = new File(metaDir.toString()).list();
            if (names != null) {
                for (String name : names) {
                    if (name.endsWith(LiveViewCheckpointLayout.TMP_SUFFIX)
                            || !Chars.startsWith(name, LiveViewCheckpointLayout.META_SEGMENT_PREFIX)) {
                        continue;
                    }
                    try {
                        ids.add(Long.parseLong(name.substring(LiveViewCheckpointLayout.META_SEGMENT_PREFIX.length())));
                    } catch (NumberFormatException ignore) {
                        // A name that is not m.<number> is not a segment we track.
                    }
                }
            }
        }
        return ids;
    }

    private static String timestamp(int secondOfDay) {
        return String.format(
                "2026-01-01T%02d:%02d:%02d.000000Z",
                secondOfDay / 3600,
                (secondOfDay % 3600) / 60,
                secondOfDay % 60
        );
    }

    /**
     * Every catalogued segment the selected generation still references must have
     * its file, whichever directory its kind puts it in. This is the assertion a
     * reference count that retired one page too early fails.
     */
    private void assertCatalogueMatchesDisk(LiveViewInstance instance) {
        final Set<Long> metaFiles = metaSegmentIds(instance);
        try (
                Path dir = checkpointsDir(instance);
                LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(configuration)
        ) {
            metaStore.of(dir);
            Assert.assertTrue("the generation must be readable", metaStore.isValid());
            try (
                    LiveViewCheckpointGenerationPin pin = metaStore.pin();
                    LiveViewCheckpointSegmentDirectoryReader directory =
                            new LiveViewCheckpointSegmentDirectoryReader(configuration)
            ) {
                directory.of(dir, pin.getSegmentDirectoryRootRef());
                final List<String> missing = new ArrayList<>();
                directory.iterateAll(entry -> {
                    if (entry.referenceCount <= 0) {
                        return;
                    }
                    final boolean exists = entry.isMetadata()
                            ? metaFiles.contains(entry.segmentId)
                            : dataSegmentFileExists(instance, entry.segmentId);
                    if (!exists) {
                        missing.add((entry.isMetadata() ? "m." : "d.") + entry.segmentId);
                    }
                });
                Assert.assertEquals(
                        "the selected generation references unlinked segments " + missing,
                        0,
                        missing.size()
                );
                // The root the superblock names is the one page nothing may have
                // reclaimed; a pending directory segment is not catalogued at all,
                // so the check above cannot speak for it.
                final long pending = metaStore.getSuperblock().pendingDirectorySegmentId;
                if (pending != Numbers.LONG_NULL) {
                    Assert.assertTrue(
                            "the unregistered directory segment must survive, segmentId=" + pending,
                            metaFiles.contains(pending)
                    );
                }
            }
        }
    }

    /**
     * At least one of the boundary-metadata segments live before the event is gone
     * from disk after it, and nothing a surviving root still names went with it.
     * Both halves matter: the first is what the reclamation buys and the second is
     * what a count that released one root too early would fail.
     */
    private void assertReclaimedSomeOf(LiveViewInstance instance, Set<Long> before, String what) {
        final Set<Long> metaFiles = metaSegmentIds(instance);
        final Set<Long> reclaimed = new HashSet<>(before);
        reclaimed.removeAll(metaFiles);
        Assert.assertFalse(
                what + " reclaimed none of the " + before.size() + " boundary metadata segments it superseded",
                reclaimed.isEmpty()
        );
        for (long segmentId : boundarySegmentIds(instance)) {
            Assert.assertTrue(
                    "a surviving root names an unlinked metadata segment, segmentId=" + segmentId,
                    metaFiles.contains(segmentId)
            );
        }
    }

    private void assertViewMatchesRecompute() throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + viewSql + ") ORDER BY 2, 1",
                "(lv) ORDER BY 2, 1",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");
    }

    /**
     * Boundary-metadata segments the live timeline names through its roots: one
     * per checkpoint root, one per anchor root and one per function root. A
     * boundary's partition-map pages can sit in older segments than these, so the
     * set is a lower bound on what must survive - which is all a "nothing live was
     * unlinked" check needs, and exactly what a "something dead went" check wants
     * to draw its candidates from.
     */
    private Set<Long> boundarySegmentIds(LiveViewInstance instance) {
        final Set<Long> ids = new HashSet<>();
        try (
                Path dir = checkpointsDir(instance);
                LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(configuration)
        ) {
            metaStore.of(dir);
            Assert.assertTrue("the generation must be readable", metaStore.isValid());
            try (
                    LiveViewCheckpointGenerationPin pin = metaStore.pin();
                    LiveViewCheckpointTimelineReader timeline = new LiveViewCheckpointTimelineReader(configuration);
                    LiveViewCheckpointRoot root = new LiveViewCheckpointRoot(configuration);
                    LiveViewCheckpointFunctionDirectory functions = new LiveViewCheckpointFunctionDirectory(configuration)
            ) {
                timeline.of(dir);
                final LiveViewCheckpointPageRef ref = new LiveViewCheckpointPageRef();
                timeline.iterateAll(pin.getTimelineRootRef(), entry -> {
                    ids.add(entry.rootRef.getSegmentId());
                    root.of(dir, entry.rootRef);
                    root.getAnchorRootRef(ref);
                    if (!ref.isNull()) {
                        ids.add(ref.getSegmentId());
                    }
                    root.getFunctionDirectoryRef(ref);
                    functions.of(dir, ref);
                    for (int i = 0, n = functions.size(); i < n; i++) {
                        functions.getRootRef(i, ref);
                        ids.add(ref.getSegmentId());
                    }
                });
            }
        }
        return ids;
    }

    // The catalogued segment ids of the selected generation, whatever their kind.
    private Set<Long> catalogue(LiveViewInstance instance) {
        final Set<Long> ids = new HashSet<>();
        try (
                Path dir = checkpointsDir(instance);
                LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(configuration)
        ) {
            metaStore.of(dir);
            try (
                    LiveViewCheckpointGenerationPin pin = metaStore.pin();
                    LiveViewCheckpointSegmentDirectoryReader directory =
                            new LiveViewCheckpointSegmentDirectoryReader(configuration)
            ) {
                directory.of(dir, pin.getSegmentDirectoryRootRef());
                directory.iterateAll(entry -> ids.add(entry.segmentId));
            }
        }
        return ids;
    }

    // One row for one key, plus a refresh turn. The key rotates so the boundary
    // metadata keeps moving rather than settling into a single reused shape.
    private void commit(LiveViewRefreshJob job, int seal) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        execute("INSERT INTO base (ts, sym, x) VALUES ('" + timestamp(10 + seal * 10)
                + "', 'k" + (seal % KEYS) + "', " + seal + ")");
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
    }

    private void createAnchoredView() throws Exception {
        viewSql = ANCHORED_RECOMPUTE_SQL;
        createView(ANCHORED_VIEW_SQL);
    }

    private void createView() throws Exception {
        viewSql = VIEW_SQL;
        createView(VIEW_SQL);
    }

    private void createView(String sql) throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " + sql);
    }

    /**
     * Catalogued segments with no file on disk. Every one is dead weight: a sweep
     * has already unlinked what the entry names, so nothing can ever read it and
     * no count decides anything about it.
     */
    private Set<Long> deadCatalogueEntries(LiveViewInstance instance) {
        final Set<Long> metaFiles = metaSegmentIds(instance);
        final Set<Long> dead = new HashSet<>();
        try (
                Path dir = checkpointsDir(instance);
                LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(configuration)
        ) {
            metaStore.of(dir);
            Assert.assertTrue("the generation must be readable", metaStore.isValid());
            try (
                    LiveViewCheckpointGenerationPin pin = metaStore.pin();
                    LiveViewCheckpointSegmentDirectoryReader directory =
                            new LiveViewCheckpointSegmentDirectoryReader(configuration)
            ) {
                directory.of(dir, pin.getSegmentDirectoryRootRef());
                directory.iterateAll(entry -> {
                    final boolean exists = entry.isMetadata()
                            ? metaFiles.contains(entry.segmentId)
                            : dataSegmentFileExists(instance, entry.segmentId);
                    if (!exists) {
                        dead.add(entry.segmentId);
                    }
                });
            }
        }
        return dead;
    }

    private boolean dataSegmentFileExists(LiveViewInstance instance, long segmentId) {
        try (Path checkpointsDir = checkpointsDir(instance); Path path = new Path()) {
            LiveViewCheckpointLayout.dataSegmentPath(path, checkpointsDir, segmentId);
            return configuration.getFilesFacade().exists(path.$());
        }
    }

    /**
     * Live segments of the three superblock-rooted trees. Boundary metadata is
     * deliberately excluded: it grows with the boundary count by design and only a
     * retention horizon retires it, so folding it in would hide what the per-page
     * accounting bounds.
     */
    private int liveTreeMetadataSegmentCount(LiveViewInstance instance) {
        final int[] count = {0};
        try (
                Path dir = checkpointsDir(instance);
                LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(configuration)
        ) {
            metaStore.of(dir);
            try (
                    LiveViewCheckpointGenerationPin pin = metaStore.pin();
                    LiveViewCheckpointSegmentDirectoryReader directory =
                            new LiveViewCheckpointSegmentDirectoryReader(configuration)
            ) {
                directory.of(dir, pin.getSegmentDirectoryRootRef());
                directory.iterateAll(entry -> {
                    if (entry.kind == LiveViewCheckpointSegmentDirectory.SEGMENT_KIND_META
                            && entry.referenceCount > 0) {
                        count[0]++;
                    }
                });
            }
        }
        return count[0];
    }

    private long pendingDirectorySegmentId(LiveViewInstance instance) {
        try (
                Path dir = checkpointsDir(instance);
                LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(configuration)
        ) {
            metaStore.of(dir);
            Assert.assertTrue(metaStore.isValid());
            final LiveViewCheckpointSuperblock superblock = metaStore.getSuperblock();
            return superblock.pendingDirectorySegmentId;
        }
    }

    private int purgeCycle(LiveViewInstance instance) {
        try (Path dir = checkpointsDir(instance)) {
            final LiveViewCheckpointLifecycle.ReconcileResult result = LiveViewCheckpointLifecycle.reconcile(
                    configuration,
                    dir,
                    instance.getLiveViewToken().getTableId(),
                    0,
                    true
            );
            Assert.assertFalse("the definition and epoch are fixed for the case", result.isEpochReplaced());
            Assert.assertFalse("this build wrote the directory it is reconciling", result.isFormatReset());
            Assert.assertEquals("no obsolete segment may fail to unlink", 0, result.getFailedPurgeCount());
            Assert.assertEquals("no orphan may fail removal", 0, result.getFailedOrphanCount());
            return result.getPurgedSegmentCount();
        }
    }

    private long referenceCount(LiveViewInstance instance, long segmentId) {
        try (
                Path dir = checkpointsDir(instance);
                LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(configuration)
        ) {
            metaStore.of(dir);
            try (
                    LiveViewCheckpointGenerationPin pin = metaStore.pin();
                    LiveViewCheckpointSegmentDirectoryReader directory =
                            new LiveViewCheckpointSegmentDirectoryReader(configuration)
            ) {
                directory.of(dir, pin.getSegmentDirectoryRootRef());
                return directory.getReferenceCount(segmentId);
            }
        }
    }

    private void restartCycle() {
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
    }

    /**
     * Boundary-metadata segments more than one root names - the pages one seal
     * wrote and a later one reused rather than copying.
     */
    private Set<Long> sharedBoundarySegmentIds(LiveViewInstance instance) {
        final Set<Long> ids = new HashSet<>();
        try (
                Path dir = checkpointsDir(instance);
                LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(configuration)
        ) {
            metaStore.of(dir);
            Assert.assertTrue("the generation must be readable", metaStore.isValid());
            try (
                    LiveViewCheckpointGenerationPin pin = metaStore.pin();
                    LiveViewCheckpointSegmentDirectoryReader directory =
                            new LiveViewCheckpointSegmentDirectoryReader(configuration)
            ) {
                directory.of(dir, pin.getSegmentDirectoryRootRef());
                directory.iterateAll(entry -> {
                    if (entry.kind == LiveViewCheckpointSegmentDirectory.SEGMENT_KIND_BOUNDARY
                            && entry.referenceCount > 1) {
                        ids.add(entry.segmentId);
                    }
                });
            }
        }
        return ids;
    }

    /**
     * The {@code maxTimestamp} of every logical boundary the timeline holds. A
     * splice preserves the whole set and a truncate drops a suffix of it, which is
     * what tells the two publications apart from the outside.
     */
    private Set<Long> timelineKeys(LiveViewInstance instance) {
        final Set<Long> keys = new HashSet<>();
        try (
                Path dir = checkpointsDir(instance);
                LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(configuration)
        ) {
            metaStore.of(dir);
            Assert.assertTrue("the generation must be readable", metaStore.isValid());
            try (
                    LiveViewCheckpointGenerationPin pin = metaStore.pin();
                    LiveViewCheckpointTimelineReader timeline = new LiveViewCheckpointTimelineReader(configuration)
            ) {
                timeline.of(dir);
                timeline.iterateAll(pin.getTimelineRootRef(), entry -> keys.add(entry.maxTimestamp));
            }
        }
        return keys;
    }

    /**
     * Catalogued segments no root of the selected generation references any more,
     * whose file is nevertheless still on disk. Each one is collectable garbage
     * waiting for a sweep - modulo the handful the fallback slot or a reader pin
     * still protects, which is what keeps the bound above zero rather than at it.
     */
    private int uncollectedSegmentCount(LiveViewInstance instance) {
        final Set<Long> metaFiles = metaSegmentIds(instance);
        final int[] count = {0};
        try (
                Path dir = checkpointsDir(instance);
                LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(configuration)
        ) {
            metaStore.of(dir);
            Assert.assertTrue("the generation must be readable", metaStore.isValid());
            try (
                    LiveViewCheckpointGenerationPin pin = metaStore.pin();
                    LiveViewCheckpointSegmentDirectoryReader directory =
                            new LiveViewCheckpointSegmentDirectoryReader(configuration)
            ) {
                directory.of(dir, pin.getSegmentDirectoryRootRef());
                directory.iterateAll(entry -> {
                    if (entry.referenceCount != 0) {
                        return;
                    }
                    final boolean exists = entry.isMetadata()
                            ? metaFiles.contains(entry.segmentId)
                            : dataSegmentFileExists(instance, entry.segmentId);
                    if (exists) {
                        count[0]++;
                    }
                });
            }
        }
        return count[0];
    }

    private LiveViewInstance viewInstance() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull("live view 'lv' must be registered", instance);
        return instance;
    }
}
