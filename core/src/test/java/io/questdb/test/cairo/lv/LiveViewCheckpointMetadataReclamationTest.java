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
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointLifecycle;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectoryReader;
import io.questdb.cairo.lv.LiveViewCheckpointSuperblock;
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
 * Coverage for metadata segment reclamation: the three trees the superblock roots
 * - the timeline, the row-position delta index and the segment catalogue itself -
 * catalogue the metadata segments their pages live in, release the pages a path
 * copy supersedes, and let the ordinary purge sweep unlink a segment whose last
 * reachable page is gone.
 * <p>
 * Before this, nothing ever removed a file from {@code meta/}. Every seal writes
 * one segment for its timeline path copy and one for the catalogue path copy, and
 * both are superseded by the next seal, so a view sealing on a five-minute cadence
 * left 576 dead files a day behind with no mechanism able to reclaim them. The
 * boundary metadata beside them - checkpoint roots, function roots, anchor roots
 * and partition maps - is retained state rather than garbage while its boundary
 * lives, and stays out of the catalogue until a retention horizon can retire the
 * boundaries naming it.
 * <p>
 * What makes the accounting exact is that a B+ tree page is named exactly once, by
 * its parent or by the superblock's root reference, so a metadata segment's
 * reference count is the number of its pages the current generation still reaches:
 * the publication adds the pages it wrote and subtracts the ones it replaced. Each
 * case therefore pairs its structural assertion with a restart and the from-base
 * recompute oracle, so a count that retired a segment one page too early surfaces
 * as a failed restore rather than as a saving.
 */
public class LiveViewCheckpointMetadataReclamationTest extends AbstractLiveViewTest {

    private static final int KEYS = 4;
    // Two measurement points far enough apart that unbounded growth cannot hide.
    private static final int SEALS_EARLY = 8;
    private static final int SEALS_LATE = 32;
    private static final String VIEW_SQL = "SELECT ts, sym, sum(x) OVER (" +
            "PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW" +
            ") AS s FROM base";

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
    public void testSupersededTimelineAndCatalogueSegmentsAreReclaimed() throws Exception {
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int seal = 1; seal <= SEALS_EARLY; seal++) {
                    commit(job, seal);
                }
                driveRefreshToQuiescence(job);
                final LiveViewInstance instance = viewInstance();
                purgeCycle(instance);
                final int earlyLiveMetaSegments = liveMetadataSegmentCount(instance);
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
                // page of its own. That is a pre-existing property of the data-side
                // catalogue which cataloguing metadata segments beside them makes
                // about three times as fast. Bounding the catalogue itself needs an
                // entry-retirement path this phase does not add.
                final int lateLiveMetaSegments = liveMetadataSegmentCount(instance);
                final int extraSeals = SEALS_LATE - SEALS_EARLY;
                Assert.assertTrue(
                        "live metadata segments went " + earlyLiveMetaSegments + " -> " + lateLiveMetaSegments
                                + " over " + extraSeals + " further seals",
                        lateLiveMetaSegments <= earlyLiveMetaSegments + 2
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

    private void assertViewMatchesRecompute() throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + VIEW_SQL + ") ORDER BY 2, 1",
                "(lv) ORDER BY 2, 1",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");
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

    private void createView() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " + VIEW_SQL);
    }

    private boolean dataSegmentFileExists(LiveViewInstance instance, long segmentId) {
        try (Path checkpointsDir = checkpointsDir(instance); Path path = new Path()) {
            LiveViewCheckpointLayout.dataSegmentPath(path, checkpointsDir, segmentId);
            return configuration.getFilesFacade().exists(path.$());
        }
    }

    private int liveMetadataSegmentCount(LiveViewInstance instance) {
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
                    if (entry.isMetadata() && entry.referenceCount > 0) {
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

    private LiveViewInstance viewInstance() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull("live view 'lv' must be registered", instance);
        return instance;
    }
}
