/*******************************************************************************
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
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointRepairRoute;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineReader;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewRefreshStats;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.str.Path;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A live view over a base whose commits overlap - each commit reaches below the newest
 * timestamp the previous one wrote - repairs every commit, and each repair's closing seal
 * owes a checkpoint root at the view's frontier. The cadence does not call for those roots,
 * so each is a repair root, and the seal that appends the second root above one removes it:
 * the timeline keeps the cadence's roots plus the two newest, and the checkpoint files stop
 * growing, rather than gaining a root and about three files per commit.
 * <p>
 * Unless a case sets a cadence of its own, every stream runs under the default cadence
 * (1,000,000 rows, 5 minutes) on the simulated clock, one commit a second, so no cadence root
 * falls inside it: the commit that follows the seeded rows by more than the duration cadence
 * seals the stream's only cadence root. The expected counts follow from the rule. Commit 1 seals a cadence root over the seeded root,
 * commit 2 a repair root over both, commit 3 a second one, and from commit 4 on every seal
 * removes the second-newest root, the repair root two commits back: four roots throughout.
 */
public class LiveViewCheckpointRepairRootTest extends AbstractLiveViewTest {
    private static final String ANCHOR_REF = "SELECT ts, sym, x, sum(x) OVER (PARTITION BY sym ORDER BY ts) s FROM %s";
    // Every row of the streams sits in the first day, so the daily anchor's sum is the running sum.
    private static final String ANCHOR_VIEW = "SELECT ts, sym, x, sum(x) OVER w s FROM %s WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')";
    private static final int KEYS = 100;
    private static final String RANGE_VIEW = "SELECT ts, sym, x, sum(x) OVER (PARTITION BY sym ORDER BY ts RANGE BETWEEN '2' SECOND PRECEDING AND CURRENT ROW) s FROM %s";
    private static final int ROWS_PER_COMMIT = 20;
    private static final String ROWS_FILTER_VIEW = "SELECT ts, sym, x, sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) s FROM %s WHERE x > 0";
    private static final String ROWS_VIEW = "SELECT ts, sym, x, sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) s FROM %s";
    private static final long SECOND = 1_000_000L;
    private static final int SHAPE_ALTERNATING_FLUSHED_APART = 2;
    private static final int SHAPE_ALTERNATING_FOLDED = 1;
    private static final int SHAPE_JITTER = 0;
    private static final int STEPS = 40;
    private static final long TS0 = 1_767_225_600_000_000L; // 2026-01-01T00:00:00Z
    private static final long CHAIN_TS0 = TS0 + 600 * SECOND;
    // Refresh faults a stream expects: a base schema change is one.
    private long expectedRefreshFaults;
    private String symbolColumnType = "SYMBOL";

    @Test
    public void testBaseAlterMidStreamKeepsTheRepairRoots() throws Exception {
        assertMemoryLeak(() -> {
            // The base's schema change makes the view put its runtime back from its newest root in
            // process. The repair roots it tracks are still the two newest, so the stream goes on
            // removing them.
            expectedRefreshFaults = 1;
            final Stream s = stream(ROWS_FILTER_VIEW, ROWS_FILTER_VIEW, true, false, SHAPE_JITTER, 20, 0, (commit, job, instance) -> {
                if (commit == 11) {
                    execute("ALTER TABLE base ADD COLUMN y INT");
                    drainWalQueue();
                    quiesce(job);
                }
            });
            Assert.assertEquals(
                    "[1, 2, 3, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4]",
                    Arrays.toString(s.entries)
            );
            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertEquals(1, instance.getCheckpointRuntimeRestores());
            // The recovery's turn folds a commit into the next repair, so the ids depend on how many
            // repairs ran: the seeded root and the cadence root stay, and the two newest are the
            // repair roots the view tracks.
            final LongList ids = checkpointTimelineIds(instance);
            Assert.assertEquals(4, ids.size());
            Assert.assertEquals(0, ids.getQuick(0));
            Assert.assertEquals(1, ids.getQuick(1));
            Assert.assertEquals("[" + ids.getQuick(2) + "," + ids.getQuick(3) + "]", s.finalRepairRootIds);
            // Every root appended from the fourth on removed one.
            Assert.assertEquals(ids.getQuick(3) - 3, s.removals);
        });
    }

    @Test
    public void testClosingSealThatCannotReadTheRootItRemovesFailsAndTheNextSealRemovesIt() throws Exception {
        final AtomicReference<String> failedSegment = new AtomicReference<>();
        final TestFilesFacadeImpl ff = new TestFilesFacadeImpl() {
            @Override
            public long length(LPSZ name) {
                final String needle = failedSegment.get();
                if (needle != null && Utf8s.endsWithAscii(name, needle)) {
                    return 0;
                }
                return super.length(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            final Stream s = stream(ROWS_FILTER_VIEW, ROWS_FILTER_VIEW, true, false, SHAPE_JITTER, 12, 0, (commit, job, instance) -> {
                if (commit == 7) {
                    // [0, 1, 5, 6]: the resume restores its anchor, 5, and the closing seal then
                    // removes it, which reads its root, which the fault makes unreadable.
                    Assert.assertEquals("[0,1,5,6]", checkpointTimelineIds(instance).toString());
                    final String path = rootSegmentPath(instance, 5);
                    hooks(job).setSimulateResumeHeadSealStart(() -> failedSegment.set(path));
                } else if (commit == 8) {
                    // The seal failed and published nothing: the splice's generation stands, and its
                    // marker stays live until a root lands at the frontier.
                    failedSegment.set(null);
                    Assert.assertEquals(1, instance.getSealFailureStreak());
                    Assert.assertTrue(isRepairMarkerPresent(instance));
                    Assert.assertEquals("[0,1,5,6]", checkpointTimelineIds(instance).toString());
                    Assert.assertEquals("[5,6]", instance.getCheckpointRepairRootIds().toString());
                } else if (commit == 9) {
                    // The next commit's seal landed at the frontier, took 7, the id the failed seal
                    // did not publish, removed 5 and cleared the marker.
                    Assert.assertEquals(0, instance.getSealFailureStreak());
                    Assert.assertFalse(isRepairMarkerPresent(instance));
                    Assert.assertEquals("[0,1,6,7]", checkpointTimelineIds(instance).toString());
                }
            });
            Assert.assertEquals("[1, 2, 3, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4]", Arrays.toString(s.entries));
            Assert.assertEquals("[0,1,10,11]", s.finalIds);
            Assert.assertEquals("[10,11]", s.finalRepairRootIds);
            Assert.assertEquals(8, s.removals);
            restartLiveViewsAndRecover();
            assertRestoredFromTimeline("lv");
            assertSqlCursors(
                    "SELECT * FROM (" + String.format(ROWS_FILTER_VIEW, "base") + ") ORDER BY ts, sym",
                    "SELECT * FROM lv ORDER BY ts, sym"
            );
        });
    }

    @Test
    public void testCrashAheadOfARemovingCadenceSealRestoresTheOlderGeneration() throws Exception {
        final AtomicReference<String> failedSegment = new AtomicReference<>();
        final TestFilesFacadeImpl ff = new TestFilesFacadeImpl() {
            @Override
            public long length(LPSZ name) {
                final String needle = failedSegment.get();
                if (needle != null && Utf8s.endsWithAscii(name, needle)) {
                    return 0;
                }
                return super.length(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            final Stream s = stream(ROWS_FILTER_VIEW, ROWS_FILTER_VIEW, true, false, SHAPE_JITTER, 8, 0);
            Assert.assertEquals("[0,1,7,8]", s.finalIds);
            final String ref = "SELECT * FROM (" + String.format(ROWS_FILTER_VIEW, "base") + ") ORDER BY ts, sym";
            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // The jitter stops. Past the duration cadence, an in-order commit seals a cadence root,
                // which removes the repair root 7 and fails ahead of its publication on its root.
                failedSegment.set(rootSegmentPath(instance, 7));
                setCurrentMicros(currentMicros + 301 * SECOND);
                execute("INSERT INTO base (ts, sym, x) VALUES ((" + (CHAIN_TS0 + 10 * SECOND) + ")::timestamp, 'a', 1.0)");
                drainWalQueue();
                quiesce(job);
                failedSegment.set(null);
                Assert.assertEquals(1, instance.getSealFailureStreak());
                Assert.assertEquals("[0,1,7,8]", checkpointTimelineIds(instance).toString());
            }
            // The restart restores the generation the failed seal left, 7 among its roots, and replays
            // the in-order commit above its newest root.
            restartLiveViewsAndRecover();
            assertRestoredFromTimeline("lv");
            assertSqlCursors(ref, "SELECT * FROM lv ORDER BY ts, sym");
            instance = engine.getLiveViewRegistry().getViewInstance("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // The restart forgot the repair roots, so the next seal, a cadence root, keeps 7.
                execute("INSERT INTO base (ts, sym, x) VALUES ((" + (CHAIN_TS0 + 11 * SECOND) + ")::timestamp, 'a', 1.0)");
                drainWalQueue();
                quiesce(job);
                Assert.assertEquals("[0,1,7,8,9]", checkpointTimelineIds(instance).toString());
            }
            assertSqlCursors(ref, "SELECT * FROM lv ORDER BY ts, sym");
        });
    }

    @Test
    public void testJitteryKeyedResumeKeepsTwoRootsAboveTheCadence() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        symbolColumnType = "SYMBOL INDEX";
        assertMemoryLeak(() -> {
            // The keyed resume serves an anchored view over a base that does not deduplicate, whose
            // commits only insert.
            final Stream s = stream(ANCHOR_VIEW, ANCHOR_REF, false, false, SHAPE_JITTER, STEPS, 0, (commit, job, instance) -> {
                if (commit == 1) {
                    hooks(job).setForceOpenSegmentKeyedReplay(true);
                }
            });
            assertFourRoots(s);
            Assert.assertEquals(STEPS - 1, s.routeCount(LiveViewCheckpointRepairRoute.ROUTE_KEYED_RESUME_FROM_ANCHOR));
        });
    }

    @Test
    public void testAPreExistingDenseLadderIsNotThinned() throws Exception {
        // Twenty commits under a rows cadence of one leave a cadence root each. Under the default
        // cadence, the repairs after them seal repair roots, and only those go: the dense ladder a
        // timeline written before this rule holds stays.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            final Stream s = stream(ROWS_FILTER_VIEW, ROWS_FILTER_VIEW, true, false, SHAPE_JITTER, 30, 0, (commit, job, instance) -> {
                if (commit == 21) {
                    Assert.assertEquals(21, checkpointTimelineIds(instance).size());
                    setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, (String) null);
                }
            });
            final StringBuilder ids = new StringBuilder("[");
            for (int i = 0; i <= 20; i++) {
                ids.append(i).append(',');
            }
            ids.append("29,30]");
            Assert.assertEquals(ids.toString(), s.finalIds);
            Assert.assertEquals("[29,30]", s.finalRepairRootIds);
            // From commit 23 on, each seal removes the repair root two commits back.
            Assert.assertEquals(8, s.removals);
        });
    }

    @Test
    public void testDropLiveViewMidStreamReleasesItsRoots() throws Exception {
        assertMemoryLeak(() -> {
            final Stream s = stream(ROWS_FILTER_VIEW, ROWS_FILTER_VIEW, true, false, SHAPE_JITTER, 8, 0);
            Assert.assertEquals("[7,8]", s.finalRepairRootIds);
            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            final java.io.File checkpointsDir = new java.io.File(
                    new java.io.File(configuration.getDbRoot(), instance.getLiveViewToken().getDirName()),
                    LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME
            );
            Assert.assertTrue(checkpointsDir.exists());
            execute("DROP LIVE VIEW lv");
            // The apply job and the purge job reclaim a dropped live view's files, as they do a
            // dropped WAL table's.
            try (WalPurgeJob purgeJob = new WalPurgeJob(engine)) {
                for (int i = 0; i < 8; i++) {
                    drainWalQueue();
                    purgeJob.drain(0);
                }
            }
            Assert.assertTrue(instance.isDropped());
            Assert.assertFalse("the dropped view's checkpoint files must go", checkpointsDir.exists());
            // The base table takes rows after the view went.
            execute("INSERT INTO base (ts, sym, x) VALUES ((" + (CHAIN_TS0 + 10 * SECOND) + ")::timestamp, 'a', 2.0)");
            drainWalQueue();
            assertQuery("SELECT count() FROM base WHERE x = 2.0").noRandomAccess().expectSize().returns("count\n1\n");
        });
    }

    @Test
    public void testJitteryStreamKeepsACadenceRootPerDuration() throws Exception {
        // A duration cadence of 60.5 s over a commit a second calls for a cadence root every 61
        // commits, whatever the milliseconds each commit's refresh takes. The model replays the rule:
        // a root is a cadence root when the cadence is due, a repair root otherwise, and each append
        // removes the second-newest root when that one is a repair root.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_MAX_DURATION_MICROS, 60_500_000);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ADAPTIVE_CADENCE_ENABLED, "false");
        final int steps = 130;
        assertMemoryLeak(() -> {
            final Stream s = stream(ROWS_FILTER_VIEW, ROWS_FILTER_VIEW, true, false, SHAPE_JITTER, steps, 0);
            final LongList timeline = new LongList();
            final LongList repairRoots = new LongList();
            timeline.add(0);
            // The seeded root, 302 s before commit 1, is the last cadence root.
            long lastCadenceRootSeconds = -301;
            long removals = 0;
            for (int k = 1; k <= steps; k++) {
                final boolean isCadenceRoot = k - lastCadenceRootSeconds >= 60.5;
                final int n = timeline.size();
                if (n > 1 && repairRoots.indexOf(timeline.getQuick(n - 2)) > -1) {
                    repairRoots.remove(timeline.getQuick(n - 2));
                    timeline.removeIndex(n - 2);
                    removals++;
                }
                timeline.add(k);
                if (isCadenceRoot) {
                    lastCadenceRootSeconds = k;
                } else {
                    repairRoots.add(k);
                }
                Assert.assertEquals("entries after commit " + k, timeline.size(), s.entries[k]);
            }
            Assert.assertEquals("[0,1,62,123,129,130]", timeline.toString());
            Assert.assertEquals(timeline.toString(), s.finalIds);
            Assert.assertEquals(repairRoots.toString(), s.finalRepairRootIds);
            Assert.assertEquals(removals, s.removals);
        });
    }

    @Test
    public void testDurationCadenceOfZeroKeepsEveryRoot() throws Exception {
        // Every seal is due under a zero duration cadence, so every root is a cadence root.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_MAX_DURATION_MICROS, 0);
        assertMemoryLeak(() -> assertEveryRootKept(stream(ROWS_FILTER_VIEW, ROWS_FILTER_VIEW, true, false, SHAPE_JITTER, 12, 0)));
    }

    @Test
    public void testRetireInsideTheStreamForgetsEveryRepairRoot() throws Exception {
        assertMemoryLeak(() -> {
            // A row below every root makes the repair rebuild the view and retire the timeline. The
            // retire forgets the repair roots and the cadence, so the fresh history's first root,
            // id 0 again, is a cadence root.
            final Stream s = stream(ROWS_FILTER_VIEW, ROWS_FILTER_VIEW, true, false, SHAPE_JITTER, 9, 0, (commit, job, instance) -> {
                if (commit == 9) {
                    Assert.assertEquals("[0,1,7,8]", checkpointTimelineIds(instance).toString());
                    Assert.assertEquals("[7,8]", instance.getCheckpointRepairRootIds().toString());
                    execute("INSERT INTO base (ts, sym, x) VALUES ((" + (TS0 + 50) + ")::timestamp, 'below', 1.0)");
                }
            });
            Assert.assertEquals("[0]", s.finalIds);
            Assert.assertEquals("[]", s.finalRepairRootIds);
            Assert.assertEquals(5, s.removals);
        });
    }

    @Test
    public void testRowCadenceOfOneKeepsEverySealWithRows() throws Exception {
        // Each repair puts rows through the window, so the rows cadence calls for every root.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> assertEveryRootKept(stream(ROWS_FILTER_VIEW, ROWS_FILTER_VIEW, true, false, SHAPE_JITTER, 12, 0)));
    }

    @Test
    public void testTimelineRemovedUnderTheViewForgetsTheRepairRoots() throws Exception {
        assertMemoryLeak(() -> {
            final Stream s = stream(ROWS_FILTER_VIEW, ROWS_FILTER_VIEW, true, false, SHAPE_JITTER, 8, 0);
            Assert.assertEquals("[7,8]", s.finalRepairRootIds);
            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // The timeline goes under the view, and an in-order cadence seal opens a fresh
                // history, whose ids restart at 0. No retire ran in this process, so only the id, at
                // or below the newest tracked one, tells the view that 7 and 8 named roots of the
                // history that went.
                retireSeedCheckpointTimeline(instance);
                setCurrentMicros(currentMicros + 301 * SECOND);
                execute("INSERT INTO base (ts, sym, x) VALUES ((" + (CHAIN_TS0 + 10 * SECOND) + ")::timestamp, 'a', 1.0)");
                drainWalQueue();
                quiesce(job);
                Assert.assertEquals("[0]", checkpointTimelineIds(instance).toString());
                Assert.assertEquals("[]", instance.getCheckpointRepairRootIds().toString());
            }
            assertSqlCursors(
                    "SELECT * FROM (" + String.format(ROWS_FILTER_VIEW, "base") + ") ORDER BY ts, sym",
                    "SELECT * FROM lv ORDER BY ts, sym"
            );
        });
    }

    @Test
    public void testTruncateInsideTheStreamForgetsTheRootsItDropped() throws Exception {
        assertMemoryLeak(() -> {
            // With no chain, each resume truncates the roots above its anchor, 7, and the view forgets
            // the repair roots the truncate dropped: [0, 1, 7, k] after commit k from 9 to 12. Once the
            // chain is back, commit 13 keeps 12 and removes 7.
            final Stream s = stream(ROWS_FILTER_VIEW, ROWS_FILTER_VIEW, true, false, SHAPE_JITTER, 20, 0, (commit, job, instance) -> {
                if (commit == 9) {
                    setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 0);
                } else if (commit == 13) {
                    Assert.assertEquals("[0,1,7,12]", checkpointTimelineIds(instance).toString());
                    Assert.assertEquals("[7,12]", instance.getCheckpointRepairRootIds().toString());
                    setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, (String) null);
                }
            });
            Assert.assertEquals(
                    "[1, 2, 3, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4]",
                    Arrays.toString(s.entries)
            );
            Assert.assertEquals("[0,1,19,20]", s.finalIds);
            Assert.assertEquals("[19,20]", s.finalRepairRootIds);
            Assert.assertEquals(5 + 8, s.removals);
        });
    }

    @Test
    public void testEveryFifthCommitOfAJitteryStreamRestartsAndRestores() throws Exception {
        assertMemoryLeak(() -> {
            // A restart forgets the repair roots, so the two the restart left among the newest stay,
            // and the first seal after it is a cadence root: three more roots per restart. Between
            // restarts the seals remove repair roots, and each restart restores from a timeline the
            // removals thinned.
            final Stream s = stream(ROWS_FILTER_VIEW, ROWS_FILTER_VIEW, true, false, SHAPE_JITTER, 20, 5);
            Assert.assertEquals(
                    "[1, 2, 3, 4, 4, 4, 5, 6, 7, 7, 7, 8, 9, 10, 10, 10, 11, 12, 13, 13, 13]",
                    Arrays.toString(s.entries)
            );
            Assert.assertEquals("[0,1,4,5,6,9,10,11,14,15,16,19,20]", s.finalIds);
            Assert.assertEquals("[19,20]", s.finalRepairRootIds);
            Assert.assertEquals(8, s.removals);
            Assert.assertEquals(3, s.restarts);
        });
    }

    @Test
    public void testHeadMissRowsCountTowardTheRowsCadence() throws Exception {
        // The head miss's closing seal is passed no row, since the lifetime counter already holds
        // its replay's. The rows cadence counts them all the same, so under a rows cadence of one
        // every closing seal's root is a cadence root, and the view tracks no repair root.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            final Stream s = stream(RANGE_VIEW, RANGE_VIEW, true, false, SHAPE_JITTER, 12, 0);
            for (int i = 1; i <= 12; i++) {
                Assert.assertEquals("entries after commit " + i, 2, s.entries[i]);
            }
            Assert.assertEquals(11, s.routeCount(LiveViewCheckpointRepairRoute.ROUTE_RANGE_REBUILD));
            Assert.assertEquals("[0,12]", s.finalIds);
            Assert.assertEquals("[]", s.finalRepairRootIds);
            Assert.assertEquals(0, s.removals);
        });
    }

    @Test
    public void testJitteryAnchoredIncrementalSealsKeepTwoRootsAboveTheCadence() throws Exception {
        assertMemoryLeak(() -> assertFourRoots(stream(ANCHOR_VIEW, ANCHOR_REF, true, false, SHAPE_JITTER)));
    }

    @Test
    public void testJitteryRangeViewReplacesItsNewestRoot() throws Exception {
        assertMemoryLeak(() -> {
            // The head miss's closing seal replaces the newest root every commit: its replay does not
            // restart the batch minimum, so the root's timestamp group reads as grown. The replace
            // drops the replaced root's id from the repair roots, so only the newest stays tracked.
            final Stream s = stream(RANGE_VIEW, RANGE_VIEW, true, false, SHAPE_JITTER);
            for (int i = 1; i <= STEPS; i++) {
                Assert.assertEquals("entries after commit " + i, 2, s.entries[i]);
            }
            for (int i = 2; i <= STEPS; i++) {
                Assert.assertEquals("files after commit " + i, s.files[2], s.files[i]);
            }
            Assert.assertEquals(STEPS - 1, s.routeCount(LiveViewCheckpointRepairRoute.ROUTE_RANGE_REBUILD));
            Assert.assertEquals("[0," + STEPS + "]", s.finalIds);
            Assert.assertEquals("[" + STEPS + "]", s.finalRepairRootIds);
            Assert.assertEquals(0, s.removals);
        });
    }

    @Test
    public void testJitteryResumeOverANonDedupBaseKeepsTwoRootsAboveTheCadence() throws Exception {
        assertMemoryLeak(() -> {
            final Stream s = stream(ROWS_VIEW, ROWS_VIEW, false, false, SHAPE_JITTER);
            assertFourRoots(s);
            Assert.assertEquals(STEPS - 1, s.routeCount(LiveViewCheckpointRepairRoute.ROUTE_RESUME_FROM_ANCHOR));
        });
    }

    @Test
    public void testJitteryWholeRangeKeepsTwoRootsAboveTheCadence() throws Exception {
        assertMemoryLeak(() -> {
            final Stream s = stream(ROWS_FILTER_VIEW, ROWS_FILTER_VIEW, true, false, SHAPE_JITTER);
            assertFourRoots(s);
            Assert.assertEquals(STEPS - 1, s.routeCount(LiveViewCheckpointRepairRoute.ROUTE_RESUME_FROM_ANCHOR));
            // From the fourth commit on, each resume anchors on the previous repair root, one commit
            // below the late rows, so it replays the late commit and the one before it at most.
            for (int i = 4; i <= STEPS; i++) {
                Assert.assertTrue("replayed rows of commit " + i + ": " + s.scanRows[i], s.scanRows[i] <= 2 * ROWS_PER_COMMIT);
            }
        });
    }

    @Test
    public void testJitteryWholeRangeOverANanosecondBaseKeepsTwoRootsAboveTheCadence() throws Exception {
        assertMemoryLeak(() -> assertFourRoots(stream(ROWS_FILTER_VIEW, ROWS_FILTER_VIEW, true, true, SHAPE_JITTER)));
    }

    @Test
    public void testKeyedResumeRowsCountTowardTheRowsCadence() throws Exception {
        // A keyed resume's closing seal is passed only the rows its commit inserted, 20, while its
        // replay re-emits the touched keys' stored rows above the anchor too: at least the key a
        // whose row tops the previous commit. Counting those, a rows cadence of 21 calls for every
        // closing seal's root, which the seal's own count would not.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, ROWS_PER_COMMIT + 1);
        symbolColumnType = "SYMBOL INDEX";
        assertMemoryLeak(() -> {
            final Stream s = stream(ANCHOR_VIEW, ANCHOR_REF, false, false, SHAPE_JITTER, 12, 0, (commit, job, instance) -> {
                if (commit == 1) {
                    hooks(job).setForceOpenSegmentKeyedReplay(true);
                }
            });
            assertEveryRootKept(s);
            Assert.assertEquals(11, s.routeCount(LiveViewCheckpointRepairRoute.ROUTE_KEYED_RESUME_FROM_ANCHOR));
        });
    }

    @Test
    public void testLateRowsAboveTheNewestRootKeepTwoRootsAboveTheCadence() throws Exception {
        // Each in-order batch flushes alone, then its late row lands above the newest root, which the
        // previous repair sealed at its frontier: the resume crosses no root, and only its closing
        // seal adds one.
        assertMemoryLeak(() -> assertFourRoots(stream(ROWS_FILTER_VIEW, ROWS_FILTER_VIEW, true, false, SHAPE_ALTERNATING_FLUSHED_APART)));
    }

    @Test
    public void testRepairsThatFoldTheNextBatchKeepTwoRootsAboveTheCadence() throws Exception {
        // Each late row waits in the lead and is repaired together with the next in-order batch. The
        // last one waits for the pass that ends the stream, whose repair stops at the newest root's
        // frontier and so only re-stamps it.
        assertMemoryLeak(() -> assertFourRoots(stream(ROWS_FILTER_VIEW, ROWS_FILTER_VIEW, true, false, SHAPE_ALTERNATING_FOLDED)));
    }

    @Test
    public void testRestartMidStreamKeepsTheRepairRootsItForgets() throws Exception {
        assertMemoryLeak(() -> {
            // Ten commits keep [0, 1, 9, 10], with 9 and 10 repair roots. The restart forgets that,
            // so they stay, and the first seal after it, 11, is a cadence root. 12 and 13 are repair
            // roots again, and from 14 on each seal removes the repair root two commits back.
            final Stream s = stream(ROWS_FILTER_VIEW, ROWS_FILTER_VIEW, true, false, SHAPE_JITTER, 20, 10);
            Assert.assertEquals(
                    "[1, 2, 3, 4, 4, 4, 4, 4, 4, 4, 4, 5, 6, 7, 7, 7, 7, 7, 7, 7, 7]",
                    Arrays.toString(s.entries)
            );
            Assert.assertEquals("[0,1,9,10,11,19,20]", s.finalIds);
            Assert.assertEquals("[19,20]", s.finalRepairRootIds);
            Assert.assertEquals(7 + 7, s.removals);
            Assert.assertEquals(1, s.restarts);
        });
    }

    private static void assertEveryRootKept(Stream s) {
        for (int i = 0; i < s.entries.length; i++) {
            Assert.assertEquals("entries after commit " + i, i + 1, s.entries[i]);
        }
        Assert.assertEquals("[]", s.finalRepairRootIds);
        Assert.assertEquals(0, s.removals);
    }

    private static void assertFourRoots(Stream s) {
        Assert.assertEquals("entries at the start", 1, s.entries[0]);
        Assert.assertEquals("entries after commit 1", 2, s.entries[1]);
        Assert.assertEquals("entries after commit 2", 3, s.entries[2]);
        for (int i = 3; i <= STEPS; i++) {
            Assert.assertEquals("entries after commit " + i, 4, s.entries[i]);
        }
        Assert.assertEquals("entries after the stream settled", 4, s.finalEntries);
        // Each commit adds a root and its segments and removes the root two commits back with
        // its own, so the files stop growing; the catalogue's own pages come and go with the
        // sweep, by a file or two.
        for (int i = 20; i <= STEPS; i++) {
            Assert.assertTrue("files after commit " + i + ": " + Arrays.toString(s.files), Math.abs(s.files[i] - s.files[20]) <= 2);
        }
        Assert.assertTrue("files after the stream settled: " + s.finalFiles + ", " + Arrays.toString(s.files), Math.abs(s.finalFiles - s.files[20]) <= 2);
        // The seeded root and the stream's one cadence root stay; ids grow by one per appended root.
        Assert.assertEquals("[0,1," + (STEPS - 1) + "," + STEPS + "]", s.finalIds);
        Assert.assertEquals("[" + (STEPS - 1) + "," + STEPS + "]", s.finalRepairRootIds);
        // From commit 4 on, every seal removes one.
        Assert.assertEquals(STEPS - 3, s.removals);
    }

    private static Path checkpointsDir(LiveViewInstance instance) {
        return new Path().of(configuration.getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    private static boolean isRepairMarkerPresent(LiveViewInstance instance) {
        try (Path dir = checkpointsDir(instance)) {
            return configuration.getFilesFacade().exists(dir.concat(LiveViewCheckpointLayout.REPAIRING_MARKER_FILE_NAME).$());
        }
    }

    /**
     * @return the path of the metadata segment holding the root page of the root {@code checkpointId}
     */
    private static String rootSegmentPath(LiveViewInstance instance, long checkpointId) {
        final long[] segmentId = {Numbers.LONG_NULL};
        try (
                Path dir = checkpointsDir(instance);
                Path path = new Path();
                LiveViewCheckpointMetaStore store = new LiveViewCheckpointMetaStore(configuration);
                LiveViewCheckpointTimelineReader reader = new LiveViewCheckpointTimelineReader(configuration)
        ) {
            store.of(dir);
            reader.of(dir);
            try (LiveViewCheckpointGenerationPin pin = store.pin()) {
                reader.iterateAll(pin.getTimelineRootRef(), entry -> {
                    if (entry.checkpointId == checkpointId) {
                        segmentId[0] = entry.rootRef.getSegmentId();
                    }
                });
            }
            Assert.assertNotEquals("root " + checkpointId + " is not in the timeline", Numbers.LONG_NULL, segmentId[0]);
            return LiveViewCheckpointLayout.metaSegmentPath(path, dir, segmentId[0]).toString();
        }
    }

    private static long liveViewsLong(String view, String column) throws Exception {
        try (RecordCursorFactory factory = select("SELECT " + column + " FROM live_views() WHERE view_name = '" + view + "'")) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(cursor.hasNext());
                return cursor.getRecord().getLong(0);
            }
        }
    }

    private static void quiesce(LiveViewRefreshJob job) {
        for (int pass = 0; pass < REFRESH_QUIESCENCE_PASSES; pass++) {
            setCurrentMicros(currentMicros + 1_000);
            drainWalQueue();
            final boolean progressed = drainJob(job);
            drainWalQueue();
            if (!progressed) {
                return;
            }
        }
        Assert.fail("the live view refresh did not quiesce");
    }

    private Stream stream(String viewTemplate, String refTemplate, boolean isDedup, boolean isNanos, int shape) throws Exception {
        return stream(viewTemplate, refTemplate, isDedup, isNanos, shape, STEPS, 0, null);
    }

    private Stream stream(String viewTemplate, String refTemplate, boolean isDedup, boolean isNanos, int shape, int steps, int restartEvery) throws Exception {
        return stream(viewTemplate, refTemplate, isDedup, isNanos, shape, steps, restartEvery, null);
    }

    /**
     * Runs {@code steps} overlapping commits of {@code shape} against a view of
     * {@code viewTemplate}, restarting the view after every {@code restartEvery} commits (0: never),
     * and checks the view against {@code refTemplate} recomputed on the base after every restart
     * and at the end. {@code hook}, when set, runs before each commit.
     */
    private Stream stream(
            String viewTemplate,
            String refTemplate,
            boolean isDedup,
            boolean isNanos,
            int shape,
            int steps,
            int restartEvery,
            @Nullable CommitHook hook
    ) throws Exception {
        final long unit = isNanos ? 1_000L : 1L;
        final String cast = isNanos ? "::timestamp_ns" : "::timestamp";
        final String ref = "SELECT * FROM (" + String.format(refTemplate, "base") + ") ORDER BY ts, sym";
        setCurrentMicros(0L);
        execute("CREATE TABLE base (ts " + (isNanos ? "TIMESTAMP_NS" : "TIMESTAMP") + ", sym " + symbolColumnType + ", x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL"
                + (isDedup ? " DEDUP UPSERT KEYS(ts, sym)" : ""));
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " + String.format(viewTemplate, "base"));
        final Stream s = new Stream(steps);
        LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1);
        try {
            driveSeedToCompletion(job, "lv");
            execute("INSERT INTO base (ts, sym, x) SELECT ((" + TS0 + " + x) * " + unit + ")" + cast + ", ('s' || x)::symbol, 1.0 FROM long_sequence(" + KEYS + ")");
            drainWalQueue();
            quiesce(job);
            // Past the duration cadence, so the stream's first commit seals a cadence root.
            setCurrentMicros(currentMicros + 301 * SECOND);
            quiesce(job);
            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            s.entries[0] = liveViewsLong("lv", "checkpoint_timeline_entries");
            s.files[0] = checkpointFileCount(instance);
            final long clockStart = currentMicros + SECOND;
            for (int i = 1; i <= steps; i++) {
                final long ts = CHAIN_TS0 + i * SECOND;
                setCurrentMicros(Math.max(currentMicros, clockStart + i * SECOND));
                if (hook != null) {
                    hook.beforeCommit(i, job, instance);
                }
                final long scanBefore = liveViewsLong("lv", "o3_replay_scan_rows");
                if (shape == SHAPE_JITTER) {
                    // Each commit's rows spread over the 1.5 s below its newest row, so it reaches
                    // 0.5 s below the previous commit's newest row.
                    final long spread = 1_500_000L;
                    final long spacing = (spread - 1) / ROWS_PER_COMMIT;
                    final long offset = (long) (i - 1) * (ROWS_PER_COMMIT - 1);
                    execute("INSERT INTO base (ts, sym, x) SELECT ((" + (ts - spread) + " + x * " + spacing + ") * " + unit + ")" + cast
                            + ", ('s' || (((" + offset + " + x - 1) % " + KEYS + ") + 1))::symbol, 1.0 FROM long_sequence(" + (ROWS_PER_COMMIT - 1) + ")");
                    execute("INSERT INTO base (ts, sym, x) VALUES ((" + ts + " * " + unit + ")" + cast + ", 'a', 1.0)");
                } else {
                    // An in-order batch up to ts - 0.5 s, then one late row 0.25 s below its top.
                    final long top = ts - SECOND / 2;
                    execute("INSERT INTO base (ts, sym, x) SELECT ((" + (top - SECOND / 2) + " + x * " + (SECOND / 2 / ROWS_PER_COMMIT) + ") * " + unit + ")" + cast
                            + ", ('s' || (((" + (long) i * ROWS_PER_COMMIT + " + x - 1) % " + KEYS + ") + 1))::symbol, 1.0 FROM long_sequence(" + ROWS_PER_COMMIT + ")");
                    drainWalQueue();
                    if (shape == SHAPE_ALTERNATING_FLUSHED_APART) {
                        // Past FLUSH EVERY, so the in-order batch flushes before the late row exists.
                        setCurrentMicros(currentMicros + 200_000);
                    }
                    quiesce(job);
                    execute("INSERT INTO base (ts, sym, x) VALUES ((" + (top - SECOND / 4) + " * " + unit + ")" + cast + ", 'late', 1.0)");
                    if (shape == SHAPE_ALTERNATING_FLUSHED_APART) {
                        drainWalQueue();
                        setCurrentMicros(currentMicros + 200_000);
                    }
                }
                drainWalQueue();
                quiesce(job);
                s.entries[i] = liveViewsLong("lv", "checkpoint_timeline_entries");
                s.files[i] = checkpointFileCount(instance);
                s.scanRows[i] = liveViewsLong("lv", "o3_replay_scan_rows") - scanBefore;
                assertRepairRootsAmongTheTwoNewest(instance);
                if (restartEvery > 0 && i % restartEvery == 0 && i < steps) {
                    s.collect(job);
                    job = Misc.free(job);
                    restartLiveViewsAndRecover();
                    assertRestoredFromTimeline("lv");
                    instance = engine.getLiveViewRegistry().getViewInstance("lv");
                    Assert.assertEquals("a restart forgets the repair roots", 0, instance.getCheckpointRepairRootIds().size());
                    Assert.assertEquals(Numbers.LONG_NULL, instance.getLastCadenceRootWrittenUs());
                    assertSqlCursors(ref, "SELECT * FROM lv ORDER BY ts, sym");
                    s.restarts++;
                    job = new LiveViewRefreshJob(0, engine, 1);
                }
            }
            // A pass past FLUSH EVERY flushes a late row the last commit left in the lead.
            setCurrentMicros(currentMicros + 10 * SECOND);
            quiesce(job);
            s.finalEntries = liveViewsLong("lv", "checkpoint_timeline_entries");
            s.finalFiles = checkpointFileCount(instance);
            s.finalIds = checkpointTimelineIds(instance).toString();
            s.finalRepairRootIds = instance.getCheckpointRepairRootIds().toString();
            s.collect(job);
            assertRepairRootsAmongTheTwoNewest(instance);
            Assert.assertEquals("refresh faults", expectedRefreshFaults, instance.getRefreshFaultCount());
            assertCatalogueMatchesDisk(instance);
            assertSqlCursors(ref, "SELECT * FROM lv ORDER BY ts, sym");
        } finally {
            Misc.free(job);
        }
        return s;
    }

    @FunctionalInterface
    private interface CommitHook {
        void beforeCommit(int commit, LiveViewRefreshJob job, LiveViewInstance instance) throws Exception;
    }

    private static final class Stream {
        final long[] entries;
        final int[] files;
        final long[] routes = new long[LiveViewCheckpointRepairRoute.ROUTE_COUNT];
        final long[] scanRows;
        long finalEntries;
        int finalFiles;
        String finalIds;
        String finalRepairRootIds;
        long removals;
        int restarts;

        Stream(int steps) {
            entries = new long[steps + 1];
            files = new int[steps + 1];
            scanRows = new long[steps + 1];
        }

        void collect(LiveViewRefreshJob job) {
            final LiveViewRefreshStats stats = job.getStatsForTest();
            removals += stats.get(LiveViewRefreshStats.REPAIR_ROOT_REMOVAL_COUNT);
            for (int r = 0; r < LiveViewCheckpointRepairRoute.ROUTE_COUNT; r++) {
                routes[r] += stats.getRouteCount(r);
            }
        }

        long routeCount(int route) {
            return routes[route];
        }
    }
}
