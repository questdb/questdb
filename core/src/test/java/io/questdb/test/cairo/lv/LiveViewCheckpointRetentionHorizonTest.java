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
import io.questdb.cairo.lv.LiveViewCheckpointRepairPlan;
import io.questdb.cairo.lv.LiveViewCheckpointSuperblock;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineReader;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreWriter;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.std.Chars;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

/**
 * Coverage for the retention horizon - the only publication that bounds what a
 * continuously sealing live view retains.
 * <p>
 * Every earlier reclamation phase closed garbage: pages and segments no surviving
 * root reaches. What was left is retained state, one closure per boundary the
 * timeline still names, and the timeline named every boundary it had ever sealed.
 * The horizon retires the oldest of them by event time, which turns the store's
 * footprint from a function of the view's age into a function of the horizon.
 * <p>
 * The cases pin the four properties that make that safe rather than merely small.
 * The footprint has to stop growing while the view keeps sealing, and the view has
 * to keep restoring from its head afterwards - both against the from-base recompute
 * oracle, so a horizon that retired a boundary something still needed surfaces as a
 * wrong answer rather than as a saving. The WAL purge floor must not move with the
 * horizon, because it is generation-scoped and releasing base WAL a restart still
 * needs would be silent data loss. And a correction below the horizon must land on
 * the same answer as a recompute whether or not the view's window functions can
 * localize it - the horizon costs reach, not correctness.
 */
public class LiveViewCheckpointRetentionHorizonTest extends AbstractLiveViewTest {

    // Event-time spacing between commits. One boundary per commit, so the horizon
    // below keeps a fixed count of them once the history is long enough.
    private static final int COMMIT_SPACING_SECONDS = 10;
    private static final int KEYS = 4;
    // Retention horizon in event time. At the commit spacing above it keeps the
    // head plus the six boundaries under it.
    private static final long RETENTION_MICROS = 60 * 1_000_000L;
    private static final int SEALS_EARLY = 10;
    private static final int SEALS_LATE = 30;
    // A bounded RANGE dependency supplies its own lower bound, so the same
    // correction stays localized with no sealed anchor under it at all.
    private static final String RANGE_VIEW_SQL = "SELECT ts, sym, sum(x) OVER (" +
            "PARTITION BY sym ORDER BY ts RANGE BETWEEN '30' SECOND PRECEDING AND CURRENT ROW" +
            ") AS s FROM base";
    private static final String ROWS_VIEW_SQL = "SELECT ts, sym, sum(x) OVER (" +
            "PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW" +
            ") AS s FROM base";
    private String viewSql = ROWS_VIEW_SQL;

    @After
    public void resetClock() {
        setCurrentMicros(-1);
    }

    @Before
    public void setUpCadence() {
        // One logical boundary per commit, so the horizon's effect is measured in
        // boundaries rather than averaged over a cadence window.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_RETENTION_MICROS, RETENTION_MICROS);
        setCurrentMicros(0);
    }

    @Test
    public void testByteCountersStayCumulativeAcrossRetention() throws Exception {
        assertMemoryLeak(() -> {
            createView(ROWS_VIEW_SQL);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int seal = 1; seal <= SEALS_EARLY; seal++) {
                    commit(job, seal);
                }
                final LiveViewInstance instance = viewInstance();
                final long metadataBytesEarly = superblockLong(instance, Field.METADATA_BYTES);
                final long dataBytesEarly = superblockLong(instance, Field.DATA_BYTES);
                final long logicalStateBytesEarly = superblockLong(instance, Field.LOGICAL_STATE_BYTES);
                Assert.assertTrue("the horizon must have retired something by now", retiredCount(instance) > 0);

                for (int seal = SEALS_EARLY + 1; seal <= SEALS_LATE; seal++) {
                    commit(job, seal);
                }
                driveRefreshToQuiescence(job);

                // The physical byte counters price what the timeline produced under
                // this history epoch, not what currently occupies the directory, so
                // reclamation must never subtract from them.
                Assert.assertTrue(
                        "metadataBytes must not fall when boundaries retire",
                        superblockLong(instance, Field.METADATA_BYTES) > metadataBytesEarly
                );
                Assert.assertTrue(
                        "dataBytes must not fall when boundaries retire",
                        superblockLong(instance, Field.DATA_BYTES) > dataBytesEarly
                );
                // The logical total is the opposite: it describes the live closure,
                // so it sheds what the retired boundaries held and stays bounded.
                final long logicalStateBytesLate = superblockLong(instance, Field.LOGICAL_STATE_BYTES);
                Assert.assertTrue(
                        "logicalStateBytes went " + logicalStateBytesEarly + " -> " + logicalStateBytesLate
                                + " while the horizon held the boundary count flat",
                        logicalStateBytesLate <= 2 * logicalStateBytesEarly
                );

                restartCycle();
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testCorrectionBelowTheHorizonRebuildsToTheSameAnswer() throws Exception {
        assertMemoryLeak(() -> {
            // A DEDUP base is what makes a change set unprovable as insert-only, and
            // a ROWS dependency is discovered rather than derived, so it cannot be
            // localized without that proof. This is the population section 6.4 of the
            // retention design says pays for the horizon: nothing bounds the repair
            // from below any more, so it rebuilds the view's whole history.
            createDedupView(ROWS_VIEW_SQL);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int seal = 1; seal <= SEALS_LATE; seal++) {
                    commit(job, seal);
                }
                final LiveViewInstance instance = viewInstance();
                final long oldestKept = oldestBoundaryTimestamp(instance);
                Assert.assertNotEquals("the horizon must have retired something", Numbers.LONG_NULL, oldestKept);

                // An upsert onto a row the first commit wrote, well below the oldest
                // surviving boundary: no sealed anchor sits under the change and the
                // dependency cannot supply a floor either.
                final long correctionTs = COMMIT_SPACING_SECONDS * 2 * 1_000_000L;
                Assert.assertTrue(
                        "the correction must sit below every surviving boundary",
                        correctionTs < oldestKept
                );
                insertOutOfOrder(job, COMMIT_SPACING_SECONDS * 2, "k1", 777);
                driveRefreshToQuiescence(job);

                Assert.assertEquals(
                        "a correction below the horizon with no finite dependency must rebuild",
                        LiveViewCheckpointRepairPlan.DISPOSITION_BOUNDARY_REBUILD,
                        instance.getCheckpointRepairLastDisposition()
                );
                Assert.assertNotEquals(
                        "the rebuild must be the denied one, not a dependency-localized rebuild",
                        LiveViewCheckpointRepairPlan.DENIAL_NONE,
                        instance.getCheckpointRepairLastDenialReason()
                );
                assertViewMatchesRecompute();

                restartCycle();
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testHorizonHoldsTheFootprintFlatWhileTheViewKeepsSealing() throws Exception {
        assertMemoryLeak(() -> {
            createView(ROWS_VIEW_SQL);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int seal = 1; seal <= SEALS_EARLY; seal++) {
                    commit(job, seal);
                }
                final LiveViewInstance instance = viewInstance();
                purgeCycle(instance);
                final int earlyBoundaries = boundaryTimestamps(instance).size();
                final int earlyMetaFiles = metaSegmentIds(instance).size();
                final int earlyDataFiles = dataSegmentIds(instance).size();
                Assert.assertTrue(
                        "the horizon must already have capped the boundary count, was " + earlyBoundaries,
                        earlyBoundaries < SEALS_EARLY
                );

                for (int seal = SEALS_EARLY + 1; seal <= SEALS_LATE; seal++) {
                    commit(job, seal);
                }
                driveRefreshToQuiescence(job);
                purgeCycle(instance);

                // The boundary count is a function of the horizon and the commit
                // spacing, not of how long the view has been running.
                final int lateBoundaries = boundaryTimestamps(instance).size();
                Assert.assertEquals(
                        "the boundary count must not move with the view's age",
                        earlyBoundaries,
                        lateBoundaries
                );
                assertHorizonBounds(instance);

                // And the files behind those boundaries go with them. What is left
                // growing is the catalogue's own tree, which retires no entry and is
                // not this phase's to bound - so the bar is well under the three
                // metadata files and one data file a seal writes.
                final int extraSeals = SEALS_LATE - SEALS_EARLY;
                final int metaFileGrowth = metaSegmentIds(instance).size() - earlyMetaFiles;
                final int dataFileGrowth = dataSegmentIds(instance).size() - earlyDataFiles;
                Assert.assertTrue(
                        "meta/ grew by " + metaFileGrowth + " files over " + extraSeals + " further seals",
                        metaFileGrowth <= extraSeals / 4
                );
                Assert.assertTrue(
                        "data/ grew by " + dataFileGrowth + " files over " + extraSeals + " further seals",
                        dataFileGrowth <= extraSeals / 4
                );

                // The published entry count follows the live set rather than the id
                // counter, which keeps climbing.
                Assert.assertEquals(
                        "the entry count must report what the timeline holds",
                        lateBoundaries,
                        superblockLong(instance, Field.NEXT_CHECKPOINT_ID) - retiredCount(instance)
                );

                restartCycle();
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testHorizonPreservesTheWalPurgeFloor() throws Exception {
        assertMemoryLeak(() -> {
            createView(ROWS_VIEW_SQL);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int seal = 1; seal <= SEALS_EARLY; seal++) {
                    commit(job, seal);
                }
                final LiveViewInstance instance = viewInstance();
                final long baseSeqTxnBefore = superblockLong(instance, Field.NORMALIZED_BASE_SEQTXN);
                final long generationBefore = superblockLong(instance, Field.GENERATION);
                final long retiredBefore = retiredCount(instance);

                // A retention pass on its own, with no seal beside it to move a
                // watermark: whatever the floor was, it must still be that.
                final long floorTs = headBoundaryTimestamp(instance) - RETENTION_MICROS + 1;
                Assert.assertTrue(
                        "the horizon must retire something for this to mean anything",
                        publishHorizon(instance, floorTs)
                );

                Assert.assertTrue(
                        "the retention publication must advance the generation",
                        superblockLong(instance, Field.GENERATION) > generationBefore
                );
                Assert.assertTrue(
                        "the retention publication must retire boundaries",
                        retiredCount(instance) > retiredBefore
                );
                Assert.assertEquals(
                        "the base watermark must not move when the horizon does",
                        baseSeqTxnBefore,
                        superblockLong(instance, Field.NORMALIZED_BASE_SEQTXN)
                );

                restartCycle();
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testHorizonRefusesToRetireTheHead() throws Exception {
        assertMemoryLeak(() -> {
            createView(ROWS_VIEW_SQL);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int seal = 1; seal <= SEALS_EARLY; seal++) {
                    commit(job, seal);
                }
                final LiveViewInstance instance = viewInstance();

                // A floor above every boundary would leave the view with no head to
                // restore from, so the publication refuses outright rather than
                // publishing a headless timeline.
                final long generationBefore = superblockLong(instance, Field.GENERATION);
                Assert.assertFalse(publishHorizon(instance, headBoundaryTimestamp(instance) + 1));
                Assert.assertEquals(
                        "a refused retention pass must publish nothing at all",
                        generationBefore,
                        superblockLong(instance, Field.GENERATION)
                );
                Assert.assertTrue(boundaryTimestamps(instance).size() > 0);

                restartCycle();
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testLocalizableViewStaysLocalizedBelowTheHorizon() throws Exception {
        assertMemoryLeak(() -> {
            createView(RANGE_VIEW_SQL);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int seal = 1; seal <= SEALS_LATE; seal++) {
                    commit(job, seal);
                }
                final LiveViewInstance instance = viewInstance();
                Assert.assertTrue("the horizon must have retired something", retiredCount(instance) > 0);

                // The paired control for the rebuild case: the same correction depth,
                // against a view whose bounded RANGE dependency supplies its own
                // lower bound. Checkpoint availability is not what localizes a
                // repair, so this one must stay local with nothing sealed under it.
                insertOutOfOrder(job, 25, "k1", 777);
                driveRefreshToQuiescence(job);

                Assert.assertEquals(
                        "a bounded RANGE dependency must localize below the horizon",
                        LiveViewCheckpointRepairPlan.DENIAL_NONE,
                        instance.getCheckpointRepairLastDenialReason()
                );
                Assert.assertNotEquals(
                        "the repair must have planned something rather than never run",
                        0,
                        instance.getCheckpointRepairLastDisposition()
                );
                assertViewMatchesRecompute();

                restartCycle();
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testSeedResumePointSurvivesTheHorizon() throws Exception {
        assertMemoryLeak(() -> {
            createView(ROWS_VIEW_SQL);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int seal = 1; seal <= SEALS_EARLY; seal++) {
                    commit(job, seal);
                }
                final LiveViewInstance instance = viewInstance();

                // Stamp a mid-sweep resume point onto the published generation. A
                // high-side truncate clears it, because it discards the head the
                // sweep was resuming into; the horizon drops boundaries the sweep is
                // long past, so it has to carry the same value forward.
                final long seedCursorOffset = 4_242;
                try (
                        Path dir = checkpointsDir(instance);
                        LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(configuration)
                ) {
                    metaStore.of(dir);
                    Assert.assertTrue(metaStore.isValid());
                    final LiveViewCheckpointSuperblock superblock = metaStore.getSuperblock();
                    superblock.generation++;
                    superblock.seedCursorOffset = seedCursorOffset;
                    metaStore.publish();
                }

                final long floorTs = headBoundaryTimestamp(instance) - RETENTION_MICROS + 1;
                Assert.assertTrue(publishHorizon(instance, floorTs));
                Assert.assertEquals(
                        "the horizon must carry the seed resume point forward",
                        seedCursorOffset,
                        superblockLong(instance, Field.SEED_CURSOR_OFFSET)
                );
            }
        });
    }

    private static Path checkpointsDir(LiveViewInstance instance) {
        return new Path().of(configuration.getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    private static Set<Long> dataSegmentIds(LiveViewInstance instance) {
        try (Path checkpointsDir = checkpointsDir(instance); Path dataDir = new Path()) {
            LiveViewCheckpointLayout.dataDirPath(dataDir, checkpointsDir);
            return segmentIds(dataDir, LiveViewCheckpointLayout.DATA_SEGMENT_PREFIX);
        }
    }

    private static Set<Long> metaSegmentIds(LiveViewInstance instance) {
        try (Path checkpointsDir = checkpointsDir(instance); Path metaDir = new Path()) {
            LiveViewCheckpointLayout.metaDirPath(metaDir, checkpointsDir);
            return segmentIds(metaDir, LiveViewCheckpointLayout.META_SEGMENT_PREFIX);
        }
    }

    // Final-name segment ids on disk. A purge unlinks the file but leaves the
    // catalogue entry, so on-disk presence is what says a segment was reclaimed.
    private static Set<Long> segmentIds(Path dir, String prefix) {
        final Set<Long> ids = new HashSet<>();
        final String[] names = new File(dir.toString()).list();
        if (names != null) {
            for (String name : names) {
                if (name.endsWith(LiveViewCheckpointLayout.TMP_SUFFIX) || !Chars.startsWith(name, prefix)) {
                    continue;
                }
                try {
                    ids.add(Long.parseLong(name.substring(prefix.length())));
                } catch (NumberFormatException ignore) {
                    // A name that is not <prefix><number> is not a segment we track.
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
     * Every surviving boundary sits at or above the horizon the head implies, and
     * the boundary that would sit just below it is gone. Together these say the
     * retention pass cut exactly where the configured window puts the floor.
     */
    private void assertHorizonBounds(LiveViewInstance instance) {
        final LongList timestamps = boundaryTimestamps(instance);
        Assert.assertTrue("the timeline must keep a head", timestamps.size() > 0);
        final long head = timestamps.getQuick(timestamps.size() - 1);
        for (int i = 0, n = timestamps.size(); i < n; i++) {
            Assert.assertTrue(
                    "boundary " + timestamps.getQuick(i) + " sits below the horizon under head " + head,
                    timestamps.getQuick(i) >= head - RETENTION_MICROS
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

    private LongList boundaryTimestamps(LiveViewInstance instance) {
        final LongList timestamps = new LongList();
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
                timeline.iterateAll(pin.getTimelineRootRef(), entry -> timestamps.add(entry.maxTimestamp));
            }
        }
        return timestamps;
    }

    // One row for one key, plus a refresh turn. The key rotates so each boundary's
    // closure keeps moving rather than settling into a single reused shape.
    private void commit(LiveViewRefreshJob job, int seal) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        execute("INSERT INTO base (ts, sym, x) VALUES ('" + timestamp(COMMIT_SPACING_SECONDS * (seal + 1))
                + "', 'k" + (seal % KEYS) + "', " + seal + ")");
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
    }

    private void createDedupView(String sql) throws Exception {
        viewSql = sql;
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL " +
                "DEDUP UPSERT KEYS(ts, sym)");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " + sql);
    }

    private void createView(String sql) throws Exception {
        viewSql = sql;
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " + sql);
    }

    private long headBoundaryTimestamp(LiveViewInstance instance) {
        final LongList timestamps = boundaryTimestamps(instance);
        Assert.assertTrue("the timeline must hold a boundary", timestamps.size() > 0);
        return timestamps.getQuick(timestamps.size() - 1);
    }

    private void insertOutOfOrder(LiveViewRefreshJob job, int secondOfDay, String key, long value) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        execute("INSERT INTO base (ts, sym, x) VALUES ('" + timestamp(secondOfDay) + "', '" + key + "', " + value + ")");
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
    }

    private long oldestBoundaryTimestamp(LiveViewInstance instance) {
        final LongList timestamps = boundaryTimestamps(instance);
        return timestamps.size() == 0 ? Numbers.LONG_NULL : timestamps.getQuick(0);
    }

    /**
     * Runs one retention pass directly, so a case can pin what the publication
     * itself does rather than what a seal happens to trigger beside it.
     */
    private boolean publishHorizon(LiveViewInstance instance, long floorTs) {
        try (
                Path dir = checkpointsDir(instance);
                LiveViewCheckpointTimelineStoreWriter writer =
                        new LiveViewCheckpointTimelineStoreWriter(configuration)
        ) {
            return writer.publishTruncateBelow(
                    dir,
                    instance.getLiveViewToken().getTableId(),
                    0,
                    floorTs,
                    true
            ).isPublished();
        }
    }

    private void purgeCycle(LiveViewInstance instance) {
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
        }
    }

    private long retiredCount(LiveViewInstance instance) {
        return superblockLong(instance, Field.RETIRED_CHECKPOINT_COUNT);
    }

    private void restartCycle() {
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
    }

    private long superblockLong(LiveViewInstance instance, Field field) {
        try (
                Path dir = checkpointsDir(instance);
                LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(configuration)
        ) {
            metaStore.of(dir);
            Assert.assertTrue("the generation must be readable", metaStore.isValid());
            final LiveViewCheckpointSuperblock superblock = metaStore.getSuperblock();
            return switch (field) {
                case DATA_BYTES -> superblock.dataBytes;
                case GENERATION -> superblock.generation;
                case LOGICAL_STATE_BYTES -> superblock.logicalStateBytes;
                case METADATA_BYTES -> superblock.metadataBytes;
                case NEXT_CHECKPOINT_ID -> superblock.nextCheckpointId;
                case NORMALIZED_BASE_SEQTXN -> superblock.normalizedBaseSeqTxn;
                case RETIRED_CHECKPOINT_COUNT -> superblock.retiredCheckpointCount;
                case SEED_CURSOR_OFFSET -> superblock.seedCursorOffset;
            };
        }
    }

    private LiveViewInstance viewInstance() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull("live view 'lv' must be registered", instance);
        return instance;
    }

    private enum Field {
        DATA_BYTES,
        GENERATION,
        LOGICAL_STATE_BYTES,
        METADATA_BYTES,
        NEXT_CHECKPOINT_ID,
        NORMALIZED_BASE_SEQTXN,
        RETIRED_CHECKPOINT_COUNT,
        SEED_CURSOR_OFFSET
    }
}
