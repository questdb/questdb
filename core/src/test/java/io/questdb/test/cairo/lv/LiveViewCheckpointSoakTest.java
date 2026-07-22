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
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.lv.LiveViewCheckpointDataStore;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionDirectory;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionRoot;
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointLifecycle;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointPartitionMapReader;
import io.questdb.cairo.lv.LiveViewCheckpointRoot;
import io.questdb.cairo.lv.LiveViewCheckpointStatePageRef;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineEntry;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineReader;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.std.Files;
import io.questdb.std.LongHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Soak coverage for the versioned checkpoint timeline: a long RANGE and a long
 * ROWS workload driven through repeated out-of-order corrections and through
 * every lifecycle event that can reach a live timeline between them.
 * <p>
 * Each of the {@link #ROUNDS} rounds commits {@link #COMMITS_PER_ROUND} in-order
 * groups, then two corrections whose dispositions differ: one five seconds under
 * the head, whose influence reaches the runtime frontier, and one deep in
 * history, which sits below every logical boundary in the first rounds and far
 * below them afterwards, so the planner prices a resume against the localized
 * rebuild the finite dependency bounds. A round closes on one lifecycle cycle,
 * rotating through purge, restart, compaction and {@code CHECKPOINT
 * CREATE}/restore, so every round faces a timeline the previous cycle left
 * behind rather than one it built itself.
 * <p>
 * The oracle throughout is the same window recomputed from the base table, with
 * the fault count asserted zero beside it: a faulting refresh cycle self-heals
 * into exactly that recompute, so equality alone would pass whether the
 * incremental, repair and restore paths worked or rebuilt themselves on every
 * commit. Timestamps are unique per {@code (sym, ts)} pair - in-order groups
 * land on multiples of ten seconds, head-adjacent corrections on multiples of
 * ten minus five, historical ones on multiples of ten plus three - so the
 * designated timestamp totally orders both sides and a row-level diff is
 * meaningful. {@code sum} over small LONG values is exact in a double, so the
 * comparison needs no floating tolerance.
 * <p>
 * This suite complements rather than replaces {@link LiveViewFuzzTest}: the fuzz
 * randomizes window shape and ingestion order against the same oracle, while
 * this one pins both and varies what happens to the durable timeline between
 * corrections.
 */
public class LiveViewCheckpointSoakTest extends AbstractLiveViewTest {

    // In-order commits per round. Every commit seals one logical checkpoint boundary,
    // so a round contributes this many roots plus one per correction.
    private static final int COMMITS_PER_ROUND = 12;
    // Distance between the compaction cycle's target segment id and the id the next
    // ordinary seal would allocate. Any id above the segment directory's last is legal;
    // this gap keeps an abandoned target from ever colliding with a final name the
    // soak's own seals reach.
    private static final int COMPACTION_TARGET_ID_GAP = 1024;
    // Four lifecycle cycles rotate over the rounds, so this is a multiple of four.
    private static final int ROUNDS = 40;
    private static final String SNAPSHOT_ID = "test-live-view-soak";
    // CHECKPOINT CREATE relies on the sync() syscall, which is unavailable on Windows.
    // The soak keeps running there; the restore cycle is substituted with a restart so
    // the remaining three cycles and the whole out-of-order workload still get exercised.
    private static final boolean isCheckpointSupported = Os.type != Os.WINDOWS;

    @Before
    @Override
    public void setUp() {
        super.setUp();
        // Pin the clock below the (2026) data so a START FROM NOW view resolves its lower
        // bound under every row it will ever see, including the corrections that land
        // below the first logical boundary.
        setCurrentMicros(0L);
        // Stable instance id so CHECKPOINT CREATE records it and restore reads the same value.
        setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, SNAPSHOT_ID);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        // Clear the in-progress flag in case a round failed between CREATE and RELEASE,
        // and wipe the checkpoint directory so it cannot leak into the next test.
        execute("CHECKPOINT RELEASE");
        try (Path path = new Path()) {
            path.of(configuration.getCheckpointRoot()).concat(configuration.getDbDirectory()).slash();
            configuration.getFilesFacade().rmdir(path);
        }
        setCurrentMicros(-1);
    }

    @Test
    public void testRangeWindowSoak() throws Exception {
        // A 30 second look-behind over rows spaced 10 seconds apart, so a frame holds the
        // current group and three predecessors. RANGE derives both ends of a repair by
        // timestamp arithmetic, which makes the historical correction's [L, H) independent
        // of how much history sits below it.
        runSoak("PARTITION BY sym ORDER BY ts RANGE BETWEEN '30' SECOND PRECEDING AND CURRENT ROW");
    }

    @Test
    public void testRowsWindowSoak() throws Exception {
        // The same frame extent reached by counting rows instead. ROWS discovers its ends
        // by walking each affected key either side of the change, so the soak also prices
        // that discovery against a timeline that keeps growing under it.
        runSoak("PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW");
    }

    // The live view must equal the same window recomputed directly over the base table.
    // The view's stored columns are exactly the projection it was created from, so (lv)
    // and (viewSql) share a schema. ORDER BY 2, 1 (sym, ts) gives both sides a total
    // order; genericStringMatch tolerates the SYMBOL-vs-STRING passthrough difference. A
    // refresh fault self-heals into a full recompute this oracle would match either way,
    // so the fault count guards that the view converged through the incremental, repair
    // and restore paths rather than through a recovery rebuild.
    private void assertViewMatchesRecompute(String viewSql) throws Exception {
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

    // Takes an OSS checkpoint of the whole database and restores it in process: releases
    // readers/writers, drops the _restore trigger file, runs checkpoint recovery and
    // re-hydrates the name registry, metadata cache and view graphs. The snapshot excludes
    // derived checkpoint state, so restore clears the local timeline and the next refresh
    // turn rebuilds it from the restored base.
    private void checkpointRestoreCycle() throws Exception {
        execute("CHECKPOINT CREATE");
        engine.clear();
        engine.closeNameRegistry();
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).parent().concat(TableUtils.RESTORE_FROM_CHECKPOINT_TRIGGER_FILE_NAME).$();
            Files.touch(path.$());
        }
        engine.checkpointRecover();
        engine.reloadTableNames();
        engine.getMetadataCache().onStartupAsyncHydrator();
        engine.buildViewGraphs();
        execute("CHECKPOINT RELEASE");
    }

    // Commits one (sym, ts) group of two rows and gives the refresh job a turn on it. The
    // clock steps past the view's 100ms flush window first, so the group reaches disk
    // before the next commit rather than lingering as an unflushed lead.
    private void commit(LiveViewRefreshJob job, int second, long xa, long xb) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        final String rowTs = secondsTs(second);
        execute("INSERT INTO base (ts, sym, x) VALUES " +
                "('" + rowTs + "', 'a', " + xa + "), " +
                "('" + rowTs + "', 'b', " + xb + ")");
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
    }

    /**
     * One compaction cycle over the newest logical root: every state page it references
     * is repacked into a fresh immutable segment while a purge runs beside it.
     * <p>
     * The candidate is abandoned rather than published, because no path yet redirects
     * published roots onto compacted pages - the repack protocol is reachable only as
     * {@link LiveViewCheckpointDataStore.Candidate}. What the soak drives is the half
     * that already composes with a live timeline: candidate ownership holding both the
     * sources and the target off a concurrent purge queue, and the abandon leaving the
     * published generation byte-identical for the next round to keep repairing.
     */
    private void compactionCycle(LiveViewInstance instance) {
        final ObjList<LiveViewCheckpointStatePageRef> sourceRefs = new ObjList<>();
        final ObjList<LiveViewCheckpointStatePageRef> targetRefs = new ObjList<>();
        final LongHashSet sourceSegmentIds = new LongHashSet();
        try (
                Path checkpointsDir = checkpointsDir(instance);
                LiveViewCheckpointMetaStore metaStore = openMetaStore(checkpointsDir)
        ) {
            // A round that reached this cycle has sealed a boundary per commit, so a
            // published generation with a newest root and state pages under it is not a
            // precondition to skip past - it is the state the cycle is here to compact.
            Assert.assertTrue("the soak must reach compaction with a published generation", metaStore.isValid());
            final long targetSegmentId = metaStore.getSuperblock().nextSegmentId + COMPACTION_TARGET_ID_GAP;
            try (
                    LiveViewCheckpointGenerationPin pin = metaStore.pin();
                    LiveViewCheckpointTimelineReader timeline = new LiveViewCheckpointTimelineReader(configuration);
                    LiveViewCheckpointRoot root = new LiveViewCheckpointRoot(configuration);
                    LiveViewCheckpointFunctionDirectory functions = new LiveViewCheckpointFunctionDirectory(configuration);
                    LiveViewCheckpointFunctionRoot functionRoot = new LiveViewCheckpointFunctionRoot(configuration);
                    LiveViewCheckpointPartitionMapReader partitions = new LiveViewCheckpointPartitionMapReader(configuration)
            ) {
                timeline.of(checkpointsDir);
                final LiveViewCheckpointTimelineEntry newest = new LiveViewCheckpointTimelineEntry();
                Assert.assertTrue(
                        "the published timeline must hold at least one logical entry",
                        timeline.last(pin.getTimelineRootRef(), newest)
                );
                root.of(checkpointsDir, newest.rootRef);
                final LiveViewCheckpointPageRef directoryRef = new LiveViewCheckpointPageRef();
                root.getFunctionDirectoryRef(directoryRef);
                functions.of(checkpointsDir, directoryRef);
                partitions.of(checkpointsDir);

                final LiveViewCheckpointPageRef functionRef = new LiveViewCheckpointPageRef();
                final LiveViewCheckpointPageRef partitionMapRef = new LiveViewCheckpointPageRef();
                final LiveViewCheckpointStatePageRef scalarRef = new LiveViewCheckpointStatePageRef();
                for (int i = 0, n = functions.size(); i < n; i++) {
                    functions.getRootRef(i, functionRef);
                    functionRoot.of(checkpointsDir, functionRef);
                    functionRoot.getScalarStateRef(scalarRef);
                    if (!scalarRef.isNull()) {
                        sourceRefs.add(copyOf(scalarRef));
                    }
                    functionRoot.getPartitionMapRootRef(partitionMapRef);
                    partitions.iterateAll(partitionMapRef, entry -> {
                        for (int p = 0, m = entry.getStatePageCount(); p < m; p++) {
                            sourceRefs.add(copyOf(entry.getStatePageRef(p)));
                        }
                    });
                }
            }

            Assert.assertTrue(
                    "the newest root must reference the window state the compaction repacks",
                    sourceRefs.size() > 0
            );
            for (int i = 0, n = sourceRefs.size(); i < n; i++) {
                sourceSegmentIds.add(sourceRefs.getQuick(i).getSegmentId());
            }
            try (LiveViewCheckpointDataStore dataStore = new LiveViewCheckpointDataStore(configuration, metaStore)) {
                dataStore.of(checkpointsDir);
                try (LiveViewCheckpointDataStore.Candidate candidate = dataStore.beginCandidate()) {
                    candidate.repack(targetSegmentId, sourceRefs, targetRefs);
                    Assert.assertEquals(
                            "a repack redirects every source page it was given",
                            sourceRefs.size(),
                            targetRefs.size()
                    );
                    Assert.assertTrue(dataSegmentExists(checkpointsDir, targetSegmentId));

                    // The purge beside an open candidate may reclaim whatever the round's
                    // repairs superseded, but never a segment the candidate owns.
                    final LiveViewCheckpointDataStore.PurgeResult purge = dataStore.purge();
                    Assert.assertEquals("a purge must not fail to unlink", 0, purge.getFailedSegmentCount());
                    assertSegmentsExist(checkpointsDir, sourceSegmentIds);
                    Assert.assertTrue(dataSegmentExists(checkpointsDir, targetSegmentId));
                }
                Assert.assertFalse(
                        "an abandoned candidate owes the directory no target",
                        dataSegmentExists(checkpointsDir, targetSegmentId)
                );
                assertSegmentsExist(checkpointsDir, sourceSegmentIds);
            }
        }
    }

    private LiveViewCheckpointMetaStore openMetaStore(Path checkpointsDir) {
        final LiveViewCheckpointMetaStore store = new LiveViewCheckpointMetaStore(configuration);
        store.of(checkpointsDir);
        return store;
    }

    // One purge cycle: the primary-owned lifecycle reconciliation a publication or a
    // startup would run, driven here at a quiescent point so the obsolete segments the
    // round's repairs left behind are reclaimed against a timeline nothing else is
    // touching. The definition txn and history epoch are the ones the engine passes, so
    // this must never look like an epoch change and retire the timeline.
    private void purgeCycle(LiveViewInstance instance) {
        try (Path checkpointsDir = checkpointsDir(instance)) {
            final LiveViewCheckpointLifecycle.ReconcileResult result = LiveViewCheckpointLifecycle.reconcile(
                    configuration,
                    checkpointsDir,
                    instance.getLiveViewToken().getTableId(),
                    0,
                    true
            );
            Assert.assertFalse("the soak must not reset a directory this build wrote", result.isFormatReset());
            Assert.assertFalse("the definition and epoch are fixed for the whole soak", result.isEpochReplaced());
            Assert.assertEquals("no orphan may fail removal", 0, result.getFailedOrphanCount());
            Assert.assertEquals("no obsolete segment may fail to unlink", 0, result.getFailedPurgeCount());
            Assert.assertEquals("no repair descriptor may fail removal", 0, result.getFailedRepairCount());
            Assert.assertEquals(
                    "a quiescent round leaves no crashed repair behind",
                    0,
                    result.getDiscardedRepairCount()
            );
        }
    }

    // Simulates a restart: drops the in-memory registry and rebuilds every view from its
    // on-disk state, which reconciles the timeline, selects a generation and restores a
    // root before the next turn resumes.
    private void restartCycle() {
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
    }

    private void runSoak(String windowFrame) throws Exception {
        // One logical root per commit: the fastest cadence the view can seal, so the soak
        // accumulates the most boundaries and every repair has roots to splice.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        final String viewSql = "SELECT ts, sym, sum(x) OVER (" + windowFrame + ") AS s FROM base";
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " + viewSql);

            int compactions = 0;
            int purges = 0;
            int restarts = 0;
            int restores = 0;
            for (int round = 1; round <= ROUNDS; round++) {
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull("live view 'lv' must be registered in round " + round, instance);
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    for (int i = 0; i < COMMITS_PER_ROUND; i++) {
                        commit(job, inOrderSecond(round, i), 100L * round + i, 200L * round + i);
                    }
                    // Just below the head, so the correction's influence reaches the runtime
                    // frontier rather than converging under it.
                    commit(job, headAdjacentO3Second(round), 9000L + round, 9100L + round);

                    // Deep in history: below every boundary in the first rounds and far below
                    // them afterwards, and always below the durable frontier, so it is always
                    // repaired rather than appended and the finite dependency - not the
                    // nearest surviving boundary - is what keeps the work off START FROM.
                    final long repairedBefore = repairedRows(instance);
                    commit(job, historicalO3Second(round), 8000L + round, 8100L + round);
                    Assert.assertTrue(
                            "round " + round + " must repair its historical correction rather than append it",
                            repairedRows(instance) > repairedBefore
                    );

                    driveRefreshToQuiescence(job);
                }
                assertViewMatchesRecompute(viewSql);

                switch (round % 4) {
                    case 1 -> {
                        purgeCycle(instance);
                        purges++;
                    }
                    case 2 -> {
                        restartCycle();
                        restarts++;
                    }
                    case 3 -> {
                        compactionCycle(instance);
                        compactions++;
                    }
                    default -> {
                        if (isCheckpointSupported) {
                            checkpointRestoreCycle();
                            restores++;
                        } else {
                            restartCycle();
                            restarts++;
                        }
                    }
                }

                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveRefreshToQuiescence(job);
                }
                assertViewMatchesRecompute(viewSql);
            }

            // A soak whose cycles silently stopped running would still converge, so the
            // rotation is asserted rather than assumed.
            Assert.assertEquals("every round must close on one lifecycle cycle",
                    ROUNDS, compactions + purges + restarts + restores);
            Assert.assertEquals(ROUNDS / 4, compactions);
            Assert.assertEquals(ROUNDS / 4, purges);
            Assert.assertEquals(isCheckpointSupported ? ROUNDS / 4 : 0, restores);
            Assert.assertEquals(isCheckpointSupported ? ROUNDS / 4 : ROUNDS / 2, restarts);

            execute("DROP LIVE VIEW lv");
        });
    }

    private static void assertSegmentsExist(Path checkpointsDir, LongHashSet segmentIds) {
        for (int i = 0, n = segmentIds.size(); i < n; i++) {
            final long segmentId = segmentIds.get(i);
            Assert.assertTrue(
                    "a segment the published generation references must survive, segmentId=" + segmentId,
                    dataSegmentExists(checkpointsDir, segmentId)
            );
        }
    }

    private static Path checkpointsDir(LiveViewInstance instance) {
        return new Path().of(configuration.getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    // The page-reference flyweights the readers hand out are reused across entries, so a
    // reference that has to outlive its cursor is copied out by value.
    private static LiveViewCheckpointStatePageRef copyOf(LiveViewCheckpointStatePageRef src) {
        return new LiveViewCheckpointStatePageRef().of(
                src.getSegmentId(),
                src.getOffset(),
                src.getStoredLength(),
                src.getDecodedLength(),
                src.getPageKind(),
                src.getCodec(),
                src.getRowCount(),
                src.getFlags()
        );
    }

    private static boolean dataSegmentExists(Path checkpointsDir, long segmentId) {
        try (Path path = new Path()) {
            LiveViewCheckpointLayout.dataSegmentPath(path, checkpointsDir, segmentId);
            return configuration.getFilesFacade().exists(path.$());
        }
    }

    // Second-of-day of the round's head-adjacent correction: five seconds under the group
    // the round's last in-order commit wrote, so it lands between two existing groups
    // without colliding with either.
    private static int headAdjacentO3Second(int round) {
        return 10 * round * COMMITS_PER_ROUND - 5;
    }

    // Second-of-day of the round's historical correction. Three seconds above the group
    // the round's ordinal names, so it never collides with an in-order group (a multiple
    // of ten) or a head-adjacent one (a multiple of ten minus five), and stays inside the
    // history the first two rounds wrote however long the soak runs.
    private static int historicalO3Second(int round) {
        return 10 * (round - 1) + 3;
    }

    private static int inOrderSecond(int round, int index) {
        return 10 * ((round - 1) * COMMITS_PER_ROUND + index + 1);
    }

    // Base rows a repair replayed over this instance's lifetime, through either
    // disposition: the resume from a boundary below the change, or the localized rebuild
    // over the change's own dependency interval. In-order appends leave both at zero.
    private static long repairedRows(LiveViewInstance instance) {
        return instance.getO3BoundaryReplayRows() + instance.getO3ResumeReplayRows();
    }

    // Builds a 2026-11-01 microsecond timestamp literal at the given second-of-day offset.
    // Every soak row shares one calendar day, so the base's DAY partition never enters the
    // picture.
    private static String secondsTs(int secondOfDay) {
        return String.format(
                "2026-11-01T%02d:%02d:%02d.000000Z",
                secondOfDay / 3600,
                (secondOfDay % 3600) / 60,
                secondOfDay % 60
        );
    }
}
