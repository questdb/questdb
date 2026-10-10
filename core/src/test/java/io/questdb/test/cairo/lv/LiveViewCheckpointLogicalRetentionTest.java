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
import io.questdb.cairo.TableReader;
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointLifecycle;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointRepairMarker;
import io.questdb.cairo.lv.LiveViewCheckpointRestoreRoute;
import io.questdb.cairo.lv.LiveViewCheckpointRoot;
import io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectoryReader;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineEntry;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineReader;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.Zip;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Acceptance coverage for the retention rule the versioned timeline replaced the
 * retained checkpoint ring with: inside one history epoch a logical checkpoint
 * entry is never removed. The ring bounded retention by count and bytes, so an
 * out-of-order row older than the surviving horizon fell back to a replay from
 * {@code START FROM}; the timeline instead keeps every boundary it ever sealed
 * and versions - rather than deletes - the roots a repair corrects.
 * <p>
 * The oracle throughout is the epoch's complete logical entry set rather than
 * its size alone. Every case reads the published generation's checkpoint ids and
 * asserts they are exactly {@code [0, nextCheckpointId)}: the epoch allocates ids
 * from zero and monotonically, so a contiguous run ending one below the next id
 * to allocate proves no entry went missing anywhere in the timeline, not merely
 * that the total happened to hold. A count comparison alone would pass a
 * publication that dropped an old boundary and appended a new one in the same
 * generation.
 * <p>
 * Each case then pins one way the count could plausibly drop: ordinary cadence
 * past the ring's former count bound, a localized repair re-versioning a
 * historical interval, and the physical lifecycle - purge and restart - running
 * over a live timeline. A fourth case pins the prefix-preservation rule for a
 * repair whose influence reaches the runtime frontier: it has no converged suffix
 * to keep, but the roots below the repair floor are still correct, so a truncate
 * preserves that prefix and re-seals a fresh head above it rather than retiring
 * the whole timeline. The generation advances and the id space carries forward -
 * a pruned tail of the same epoch, not a new one restarting at zero.
 */
public class LiveViewCheckpointLogicalRetentionTest extends AbstractLiveViewTest {

    // Deep historical corrections one case drives, each three seconds above the group
    // its ordinal names, so it lands between two in-order groups without colliding.
    private static final int CORRECTIONS = 6;
    // The prefix every heal decline over rows the base lost logs. What follows it names the check
    // that declined: " [view=" the per-interval floor, " on its frontier" the frontier group's
    // count, " on a timestamp" the per-timestamp walk.
    private static final String HEAL_DECLINED_ROWS =
            "live view checkpoint heal declined, the base table does not hold the rows the view materialized";
    // What seedCorruptNewestRootUnderAHigherFrontierOverALossyBase leaves the view holding.
    private static final String HIGHER_FRONTIER_SEEDED_ROWS = """
            ts\tsym\ts
            2025-12-31T23:59:00.000000Z\ta\t1000.0
            2026-01-01T00:00:10.000000Z\ta\t1.0
            2026-01-01T00:00:20.000000Z\ta\t3.0
            2026-01-01T00:00:30.000000Z\ta\t6.0
            2026-01-01T00:00:40.000000Z\ta\t10.0
            2026-01-01T00:00:50.000000Z\ta\t14.0
            2026-01-01T00:01:00.000000Z\ta\t18.0
            2026-01-01T00:01:00.000000Z\tb\t100.0
            2026-01-01T00:01:10.000000Z\ta\t22.0
            """;
    // In-order commits every case builds its history from. At one logical root per
    // commit this is also the epoch's logical entry count - eight times the retained
    // ring's former default count bound, which is the retention the timeline replaced.
    private static final int SEALS = 64;
    private static final String VIEW_SQL = "SELECT ts, sym, sum(x) OVER (" +
            "PARTITION BY sym ORDER BY ts RANGE BETWEEN '30' SECOND PRECEDING AND CURRENT ROW" +
            ") AS s FROM base";

    @After
    public void resetClock() {
        setCurrentMicros(-1);
    }

    @Before
    public void setUpCadence() {
        // The fastest cadence the view can seal: one logical root per commit, so the
        // history accumulates the most boundaries a repair or a purge can drop.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        // Pin the clock below the (2026) data so a START FROM NOW view resolves its
        // lower bound under every row it will ever see, corrections included.
        setCurrentMicros(0);
    }

    @Test
    public void testCadenceRetainsEveryLogicalEntryPastTheRetiredRingBound() throws Exception {
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                Assert.assertNull(job.getCheckpointTimelineLifecycleStateForTest());
                long entries = 0;
                for (int commit = 1; commit <= SEALS; commit++) {
                    appendAndRefresh(job, commit * 10, commit);
                    if (commit == 1) {
                        Assert.assertSame(
                                engine.getLiveViewCheckpointLifecycleState(),
                                job.getCheckpointTimelineLifecycleStateForTest()
                        );
                    }
                    final LiveViewInstance instance = viewInstance();
                    entries = assertEpochRetainsEveryEntry(instance, entries, "after commit " + commit);
                    Assert.assertEquals("one cadence seal appends one logical entry", commit, entries);
                }

                // The oldest boundary stays addressable at its original coordinate after
                // every later event, which is what makes an old O3 row's predecessor
                // lookup a search rather than a fallback to START FROM.
                final LiveViewInstance instance = viewInstance();
                final LiveViewCheckpointTimelineEntry oldest = new LiveViewCheckpointTimelineEntry();
                assertPredecessorIs(instance, ts(timestamp(20)), 0, oldest);
                Assert.assertEquals(
                        "the oldest boundary keeps the coordinate it was sealed at",
                        ts(timestamp(10)),
                        oldest.maxTimestamp
                );
                Assert.assertTrue(findsEntry(instance, ts(timestamp(10)), 0, oldest));
            }
        });
    }

    @Test
    public void testEofRepairPreservedTimelineRestoresAfterRestart() throws Exception {
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = buildHistory(job);
                // EOF repair: preserves the prefix, re-seals a fresh head, clears the marker.
                correct(job, instance, SEALS * 10 - 5, 800);
                Assert.assertTrue("the repair preserved the timeline", generation(instance) > SEALS);
            }

            // Restart: drop the in-memory registry and rebuild it from disk.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            final LiveViewInstance restored = viewInstance();

            // The cleared marker means a restart restores from the preserved timeline
            // rather than rebuilding: its advanced generation and prefix survive.
            Assert.assertTrue("the preserved generation survives the restart", generation(restored) > SEALS);
            final LiveViewCheckpointTimelineEntry entry = new LiveViewCheckpointTimelineEntry();
            Assert.assertTrue(
                    "the oldest boundary survives the restart",
                    findsEntry(restored, ts(timestamp(10)), 0, entry)
            );

            // A refresh after the restart restores from the preserved timeline (no
            // rebuild would have reset the generation to a fresh epoch) and the view
            // matches a direct recompute.
            try (LiveViewRefreshJob resumed = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(resumed);
            }
            Assert.assertTrue(
                    "a restore must not reset the generation to a rebuilt epoch",
                    generation(viewInstance()) > SEALS
            );
            assertViewMatchesRecompute();
        });
    }

    @Test
    public void testLifecycleCyclesRetainEveryLogicalEntry() throws Exception {
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                LiveViewInstance instance = buildHistory(job);
                // One correction first, so the purge below has superseded physical
                // versions to reclaim rather than an untouched steady-state timeline.
                correct(job, instance, historicalSecond(1), 900);
                long entries = assertEpochRetainsEveryEntry(instance, SEALS, "after the correction");

                purgeCycle(instance);
                entries = assertEpochRetainsEveryEntry(instance, entries, "after the purge");

                engine.getLiveViewRegistry().clear();
                engine.buildViewGraphs();
                instance = viewInstance();
                entries = assertEpochRetainsEveryEntry(instance, entries, "after the restart");
                Assert.assertEquals("the physical lifecycle owns no logical entry", SEALS, entries);

                // The restarted view keeps sealing into the same epoch rather than
                // starting a new one under the reconciled timeline.
                try (LiveViewRefreshJob resumed = new LiveViewRefreshJob(0, engine, 1)) {
                    appendAndRefresh(resumed, (SEALS + 1) * 10, SEALS + 1);
                    driveRefreshToQuiescence(resumed);
                }
                entries = assertEpochRetainsEveryEntry(viewInstance(), entries, "after resuming the cadence");
                Assert.assertEquals(SEALS + 1, entries);
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testLiveRepairMarkerForcesRebuildOnRestart() throws Exception {
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = buildHistory(job);
                // Simulate a crash in the middle of a prefix-preserving repair: a live
                // marker (its base generation is the current one, so no later generation
                // sealed over it) sits on disk with the timeline still present.
                writeRepairMarker(instance, generation(instance));
            }

            // Restart and refresh: the live marker must force a rebuild from the applied
            // base rather than trust a timeline whose head may have been truncated.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            try (LiveViewRefreshJob resumed = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(resumed);
            }

            final LiveViewInstance restored = viewInstance();
            Assert.assertEquals("a forced rebuild resets the generation", 1, generation(restored));
            try (Path dir = checkpointsDir(restored)) {
                Assert.assertFalse(
                        "the rebuild must remove the repair marker",
                        LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir)
                );
            }
            assertViewMatchesRecompute();
        });
    }

    @Test
    public void testLocalizedRepairsReVersionRootsWithoutDroppingOne() throws Exception {
        assertMemoryLeak(() -> {
            createView();
            final LiveViewInstance instance;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                instance = buildHistory(job);
            }
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                Assert.assertNull(job.getCheckpointTimelineLifecycleStateForTest());
                long entries = SEALS;
                for (int correction = 1; correction <= CORRECTIONS; correction++) {
                    final long generationBefore = generation(instance);
                    correct(job, instance, historicalSecond(correction), 900 + correction);
                    if (correction == 1) {
                        Assert.assertSame(
                                engine.getLiveViewCheckpointLifecycleState(),
                                job.getCheckpointTimelineLifecycleStateForTest()
                        );
                    }

                    final String when = "after correction " + correction;
                    Assert.assertTrue(
                            "a localized repair publishes a new generation " + when,
                            generation(instance) > generationBefore
                    );
                    entries = assertEpochRetainsEveryEntry(instance, entries, when);
                    Assert.assertEquals(
                            "a converged repair re-versions roots in [C, H) and creates no boundary " + when,
                            SEALS,
                            entries
                    );
                    assertViewMatchesRecompute();
                }
            }
        });
    }

    @Test
    public void testLoneStagedMarkerForcesOneRebuildAndIsThenRemoved() throws Exception {
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = buildHistory(job);
                // A staged marker with no record in it, and no final name: what a
                // crash inside the staged write itself can leave. With no generation to
                // test, it has to read as a live repair, not as "none in flight".
                writeRepairMarker(instance, generation(instance));
                try (Path dir = checkpointsDir(instance); Path path = new Path()) {
                    LiveViewCheckpointLayout.repairingMarkerPath(path, dir);
                    path.put(LiveViewCheckpointLayout.TMP_SUFFIX);
                    Assert.assertTrue(configuration.getFilesFacade().touch(path.$()));
                    LiveViewCheckpointLayout.repairingMarkerPath(path, dir);
                    Assert.assertTrue(configuration.getFilesFacade().removeQuiet(path.$()));
                }
            }

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            try (LiveViewRefreshJob resumed = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(resumed);
            }

            final LiveViewInstance restored = viewInstance();
            Assert.assertEquals("a staged-only marker forces a rebuild", 1, generation(restored));
            // The rebuild must clear the sibling too, or every later restart rebuilds.
            try (Path dir = checkpointsDir(restored)) {
                Assert.assertFalse(
                        "the rebuild must remove the staged marker sibling",
                        LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir)
                );
            }
            assertViewMatchesRecompute();
        });
    }

    @Test
    public void testRestartReconstructsCorruptNewestRoot() throws Exception {
        assertMemoryLeak(() -> {
            createView();
            final long generationBefore;
            final long nextIdBefore;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = buildHistory(job);
                generationBefore = generation(instance);
                nextIdBefore = nextCheckpointId(instance);
                // Corrupt the newest logical root's data segment while the view is
                // quiescent: one truncated byte makes its state page fail the reader's
                // length check, exactly as a torn write would.
                corruptNewestRootDataSegment(instance);
            }

            // Restart: drop the in-memory registry and rebuild it from disk.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            // Restore: the floor selects the corrupt newest root, the reader falls back
            // to its predecessor, and the restart reconstructs the corrupt id in place
            // before restoring cleanly from the healed generation.
            try (LiveViewRefreshJob resumed = new LiveViewRefreshJob(0, engine, 1)) {
                Assert.assertNull(resumed.getCheckpointTimelineLifecycleStateForTest());
                driveRefreshToQuiescence(resumed);
                Assert.assertSame(
                        engine.getLiveViewCheckpointLifecycleState(),
                        resumed.getCheckpointTimelineLifecycleStateForTest()
                );
            }

            final LiveViewInstance restored = viewInstance();
            // Reconstruction, not a rebuild: a rebuild would retire the timeline and
            // reset the generation to a fresh epoch. Here the generation advances over
            // the preserved epoch and the id space is unchanged.
            Assert.assertTrue(
                    "reconstruction must advance the generation, not reset it [generation="
                            + generation(restored) + ']',
                    generation(restored) > generationBefore
            );
            Assert.assertEquals(
                    "reconstruction re-versions ids in place and mints none",
                    nextIdBefore,
                    nextCheckpointId(restored)
            );
            Assert.assertEquals(
                    "every logical entry survives the corrupt-root reconstruction",
                    SEALS,
                    assertEpochRetainsEveryEntry(restored, SEALS, "after reconstruction")
            );

            final LiveViewCheckpointTimelineEntry entry = new LiveViewCheckpointTimelineEntry();
            // The healed newest boundary keeps its original coordinate and id, so it is
            // addressable again rather than a permanently skipped corrupt version.
            Assert.assertTrue(
                    "the healed newest boundary must be addressable at its original id",
                    findsEntry(restored, ts(timestamp(SEALS * 10)), SEALS - 1, entry)
            );
            // The oldest boundary - an unrelated root far below the corruption - survives.
            Assert.assertTrue(
                    "an unrelated root must survive the corruption",
                    findsEntry(restored, ts(timestamp(10)), 0, entry)
            );
            assertViewMatchesRecompute();

            // An in-order row above the frontier folds directly into the reconstructed
            // window state: a wrong retained RANGE frame would make its sum diverge from
            // a fresh recompute, so this exercises the reconstructed state, not just the
            // durable table the restore left untouched.
            try (LiveViewRefreshJob resumed = new LiveViewRefreshJob(0, engine, 1)) {
                appendAndRefresh(resumed, (SEALS + 1) * 10, SEALS + 1);
                driveRefreshToQuiescence(resumed);
            }
            assertViewMatchesRecompute();

            // A deep O3 correction after the heal localizes against the surviving roots,
            // proving the reconstructed timeline is fully usable for later repair.
            try (LiveViewRefreshJob resumed = new LiveViewRefreshJob(0, engine, 1)) {
                correct(resumed, restored, historicalSecond(2), 950);
            }
            assertViewMatchesRecompute();
        });
    }

    @Test
    public void testRestartHealsACorruptNewestRootWhoseTimestampGroupGrewOverALossyBase() throws Exception {
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // A row a day below the history, which the base loses further down while the view
                // keeps it: a rebuild from the applied base would have to drop it, and refuses to.
                commitAndRefresh(job, "('2025-12-31T23:59:00.000000Z', 'a', 1000)");
                for (int commit = 1; commit <= 8; commit++) {
                    appendAndRefresh(job, commit * 10, commit);
                }
                // An in-order row on the newest root's own timestamp, after it sealed: the
                // cadence seals no second root there, so the root holds part of its group.
                commitAndRefresh(job, "('" + timestamp(80) + "', 'b', 100)");
                driveRefreshToQuiescence(job);
                Assert.assertEquals(ts(timestamp(80)), viewInstance().getHeadCheckpointMaxTs());
                execute("ALTER TABLE base DROP PARTITION LIST '2025-12-31'");
                drainWalQueue();
                driveRefreshToQuiescence(job);
                corruptNewestRootDataSegment(viewInstance());
            }

            restartAndRefresh();
            assertRestoredFromTimeline("lv");
            Assert.assertFalse(
                    "the heal must not leave the view blocked over the lost day",
                    viewInstance().isCheckpointRecoveryBlocked()
            );
            final String viewRows = """
                    ts\tsym\ts
                    2025-12-31T23:59:00.000000Z\ta\t1000.0
                    2026-01-01T00:00:10.000000Z\ta\t1.0
                    2026-01-01T00:00:20.000000Z\ta\t3.0
                    2026-01-01T00:00:30.000000Z\ta\t6.0
                    2026-01-01T00:00:40.000000Z\ta\t10.0
                    2026-01-01T00:00:50.000000Z\ta\t14.0
                    2026-01-01T00:01:00.000000Z\ta\t18.0
                    2026-01-01T00:01:10.000000Z\ta\t22.0
                    2026-01-01T00:01:20.000000Z\ta\t26.0
                    2026-01-01T00:01:20.000000Z\tb\t100.0
                    """;
            assertViewRows(viewRows);

            // The tie's account reads the healed frame back: a head that missed the tie would
            // sum 7, one that folded it twice 207.
            try (LiveViewRefreshJob resumed = new LiveViewRefreshJob(0, engine, 1)) {
                commitAndRefresh(resumed, "('" + timestamp(90) + "', 'b', 7)");
                driveRefreshToQuiescence(resumed);
            }
            final String grownRows = viewRows + "2026-01-01T00:01:30.000000Z\tb\t107.0\n";
            assertViewRows(grownRows);
            assertNoRefreshFaults("lv");

            restartAndRefresh();
            assertRestoredFromTimeline("lv");
            assertNoRefreshFaults("lv");
            assertViewRows(grownRows);
        });
    }

    @Test
    public void testRestartHealsACorruptNewestRootUnderAHigherFrontierOverALossyBase() throws Exception {
        setCadenceForAGroupSplitAcrossTheFrontier();
        assertMemoryLeak(() -> {
            seedCorruptNewestRootUnderAHigherFrontierOverALossyBase(null);

            restartAndRefresh();
            assertRestoredFromTimeline("lv");
            Assert.assertFalse(viewInstance().isCheckpointRecoveryBlocked());
            Assert.assertEquals(
                    "the heal must seal the durable frontier above the healed root",
                    ts(timestamp(70)),
                    viewInstance().getHeadCheckpointMaxTs()
            );
            assertViewRows(HIGHER_FRONTIER_SEEDED_ROWS);

            try (LiveViewRefreshJob resumed = new LiveViewRefreshJob(0, engine, 1)) {
                commitAndRefresh(resumed, "('" + timestamp(80) + "', 'b', 7), ('" + timestamp(80) + "', 'a', 8)");
                driveRefreshToQuiescence(resumed);
            }
            final String grownRows = HIGHER_FRONTIER_SEEDED_ROWS + """
                    2026-01-01T00:01:20.000000Z\tb\t107.0
                    2026-01-01T00:01:20.000000Z\ta\t26.0
                    """;
            assertViewRows(grownRows);
            assertNoRefreshFaults("lv");

            restartAndRefresh();
            assertRestoredFromTimeline("lv");
            assertNoRefreshFaults("lv");
            assertViewRows(grownRows);
        });
    }

    @Test
    public void testRestartHealsACorruptNewestRootUnderAHigherFrontierOverAnUnconsumedOutOfOrderRow() throws Exception {
        setCadenceForAGroupSplitAcrossTheFrontier();
        assertMemoryLeak(() -> {
            // The base applies a row between the grown root and the frontier before the view
            // consumes it. The heal folds it into the root it seals at the frontier. The row sits
            // below the frontier, so the drain meets its commit as out of order and repairs it from
            // the healed root beneath it.
            seedCorruptNewestRootUnderAHigherFrontierOverALossyBase("('" + timestamp(65) + "', 'a', 50)");

            restartAndRefresh();
            assertRestoredFromTimeline("lv");
            Assert.assertFalse(
                    "the heal must not leave the view blocked over the lost day",
                    viewInstance().isCheckpointRecoveryBlocked()
            );
            Assert.assertTrue("the drain must repair the late row's commit", repairedRows(viewInstance()) > 0);
            assertNoRefreshFaults("lv");
            final String viewRows = """
                    ts\tsym\ts
                    2025-12-31T23:59:00.000000Z\ta\t1000.0
                    2026-01-01T00:00:10.000000Z\ta\t1.0
                    2026-01-01T00:00:20.000000Z\ta\t3.0
                    2026-01-01T00:00:30.000000Z\ta\t6.0
                    2026-01-01T00:00:40.000000Z\ta\t10.0
                    2026-01-01T00:00:50.000000Z\ta\t14.0
                    2026-01-01T00:01:00.000000Z\ta\t18.0
                    2026-01-01T00:01:00.000000Z\tb\t100.0
                    2026-01-01T00:01:05.000000Z\ta\t65.0
                    2026-01-01T00:01:10.000000Z\ta\t72.0
                    """;
            assertViewRows(viewRows);

            try (LiveViewRefreshJob resumed = new LiveViewRefreshJob(0, engine, 1)) {
                commitAndRefresh(resumed, "('" + timestamp(80) + "', 'b', 7), ('" + timestamp(80) + "', 'a', 8)");
                driveRefreshToQuiescence(resumed);
            }
            final String grownRows = viewRows + """
                    2026-01-01T00:01:20.000000Z\tb\t107.0
                    2026-01-01T00:01:20.000000Z\ta\t76.0
                    """;
            assertViewRows(grownRows);
            assertNoRefreshFaults("lv");

            restartAndRefresh();
            assertRestoredFromTimeline("lv");
            assertNoRefreshFaults("lv");
            assertViewRows(grownRows);
        });
    }

    @Test
    public void testRestartHealsACorruptNewestRootBelowTheFrontierOverAnUnconsumedRowOnItsTimestamp() throws Exception {
        setCadenceForAGroupSplitAcrossTheFrontier();
        assertMemoryLeak(() -> {
            // The newest root at 00:01:00 sealed its whole group, and a row above it at 00:01:10
            // stays unsealed. The base then applies a row on the root's own timestamp that the view
            // has not consumed. The heal folds it into the root, which sits below the frontier, so
            // the drain meets its commit as out of order rather than as a tie, and repairs it from
            // the root below.
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                commitAndRefresh(job, "('2025-12-31T23:59:00.000000Z', 'a', 1000)");
                for (int commit = 1; commit <= 7; commit++) {
                    appendAndRefresh(job, commit * 10, commit);
                }
                driveRefreshToQuiescence(job);
                Assert.assertEquals(ts(timestamp(60)), viewInstance().getHeadCheckpointMaxTs());
                execute("ALTER TABLE base DROP PARTITION LIST '2025-12-31'");
                drainWalQueue();
                driveRefreshToQuiescence(job);
                execute("INSERT INTO base VALUES ('" + timestamp(60) + "', 'b', 5)");
                drainWalQueue();
                corruptNewestRootDataSegment(viewInstance());
            }

            restartAndRefresh();
            assertRestoredFromTimeline("lv");
            Assert.assertFalse(
                    "the heal must not leave the view blocked over the lost day",
                    viewInstance().isCheckpointRecoveryBlocked()
            );
            Assert.assertTrue("the drain must repair the late row's commit", repairedRows(viewInstance()) > 0);
            assertNoRefreshFaults("lv");
            final String viewRows = """
                    ts\tsym\ts
                    2025-12-31T23:59:00.000000Z\ta\t1000.0
                    2026-01-01T00:00:10.000000Z\ta\t1.0
                    2026-01-01T00:00:20.000000Z\ta\t3.0
                    2026-01-01T00:00:30.000000Z\ta\t6.0
                    2026-01-01T00:00:40.000000Z\ta\t10.0
                    2026-01-01T00:00:50.000000Z\ta\t14.0
                    2026-01-01T00:01:00.000000Z\ta\t18.0
                    2026-01-01T00:01:00.000000Z\tb\t5.0
                    2026-01-01T00:01:10.000000Z\ta\t22.0
                    """;
            assertViewRows(viewRows);

            try (LiveViewRefreshJob resumed = new LiveViewRefreshJob(0, engine, 1)) {
                commitAndRefresh(resumed, "('" + timestamp(80) + "', 'b', 7), ('" + timestamp(80) + "', 'a', 8)");
                driveRefreshToQuiescence(resumed);
            }
            final String grownRows = viewRows + """
                    2026-01-01T00:01:20.000000Z\tb\t12.0
                    2026-01-01T00:01:20.000000Z\ta\t26.0
                    """;
            assertViewRows(grownRows);
            assertNoRefreshFaults("lv");

            restartAndRefresh();
            assertRestoredFromTimeline("lv");
            assertNoRefreshFaults("lv");
            assertViewRows(grownRows);
        });
    }

    @Test
    public void testRestartHealsACorruptNewestRootOverAnUnconsumedOutOfOrderRowOnALossyBase() throws Exception {
        assertMemoryLeak(() -> assertRestartHealsOverAnUnconsumedOutOfOrderRow(
                VIEW_SQL,
                "('" + timestamp(75) + "', 'a', 50)"
        ));
    }

    @Test
    public void testRestartHealsAFilteredViewOverAnUnconsumedOutOfOrderRowOnALossyBase() throws Exception {
        // The commit the view has not consumed also carries a row on the frontier that the view's
        // filter drops. The heal counts the rows the window folds, so that row is no tie on the
        // frontier the drain would fold a second time.
        assertMemoryLeak(() -> assertRestartHealsOverAnUnconsumedOutOfOrderRow(
                VIEW_SQL + " WHERE sym != 'z'",
                "('" + timestamp(75) + "', 'a', 50), ('" + timestamp(80) + "', 'z', 999)"
        ));
    }

    @Test
    public void testRestartDeclinesToHealOverATieOnTheFrontierTheViewHasNotConsumed() throws Exception {
        setCadenceForAGroupSplitAcrossTheFrontier();
        assertMemoryLeak(() -> {
            // The base applies an in-order tie on the frontier before the view consumes it. The
            // drain will fold that row as it consumes the commit, so a root the heal sealed with the
            // row already in it would count it twice. The heal declines, and the rebuild the lost
            // day refuses leaves the view on the rows it holds.
            seedCorruptNewestRootUnderAHigherFrontierOverALossyBase("('" + timestamp(70) + "', 'b', 5)");

            restartAndRefresh();
            Assert.assertTrue(viewInstance().isCheckpointRecoveryBlocked());
            Assert.assertEquals(
                    "rebuild_blocked",
                    LiveViewCheckpointRestoreRoute.name(viewInstance().getCheckpointRestoreRoute())
            );
            assertViewRows(HIGHER_FRONTIER_SEEDED_ROWS);
        });
    }

    @Test
    public void testRestartDeclinesToHealAFilteredViewsLostRowUnderARowItsFilterDropsOnTheSameTimestamp() throws Exception {
        // One commit the view has not consumed puts a row into the interval, which cancels the loss
        // in the interval's count. Another puts a row back on the lost row's timestamp, which the
        // view's filter drops. The floor counts the rows the heal folds, which the filter decides,
        // so that row does not stand in for the lost one.
        setCadenceForAGroupSplitAcrossTheFrontier();
        assertMemoryLeak(() -> assertRestartRebuildsOverALostRowAmongUnconsumedRowsInItsInterval(
                VIEW_SQL + " WHERE sym != 'z'",
                """
                        ts\tsym\ts
                        2026-01-01T00:00:00.000000Z\ta\t1.0
                        2026-01-01T00:00:10.000000Z\ta\t2.0
                        2026-01-01T00:00:20.000000Z\ta\t3.0
                        2026-01-01T00:00:30.000000Z\ta\t4.0
                        2026-01-01T00:58:00.000000Z\tb\t100.0
                        2026-01-01T00:59:00.000000Z\ta\t4.0
                        2026-01-01T02:00:00.000000Z\ta\t6.0
                        2026-01-01T02:00:10.000000Z\ta\t13.0
                        2026-01-01T02:00:20.000000Z\ta\t21.0
                        """,
                "('2026-01-01T00:58:00.000000Z', 'b', 100)",
                "('2026-01-01T01:59:50.000000Z', 'z', 999)"
        ));
    }

    @Test
    public void testRestartDeclinesToHealALostRowAloneInItsInterval() throws Exception {
        // No commit the view has not consumed puts a row into the newest root's interval, so the
        // interval's count shows the lost row and the per-interval floor is the only check that sees
        // it: the per-timestamp walk reads an interval only when its count shows a surplus or the base
        // applied commits the view has not consumed. The rebuild the declined heal falls back to would
        // drop the lost row the view keeps, so the restatement guard refuses it and the view stays on
        // the rows it holds. A healed head would have answered 21.0 at 02:00:20 - whose window reaches
        // back over the lost row - from a runtime that never held it, where the build that sealed the
        // head answers 26.0.
        setCadenceForAGroupSplitAcrossTheFrontier();
        assertMemoryLeak(() -> {
            seedALostRowAmongUnconsumedRowsInItsInterval(VIEW_SQL, null);
            Assert.assertEquals(
                    "no commit may be left unconsumed",
                    engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("base")),
                    viewInstance().getLastProcessedSeqTxn()
            );
            final LogCapture capture = restartAndRefreshCapturingLog();
            try (LiveViewRefreshJob resumed = new LiveViewRefreshJob(0, engine, 1)) {
                commitAndRefresh(resumed, "('2026-01-01T02:00:20.000000Z', 'a', 8)");
                driveRefreshToQuiescence(resumed);
            }
            Assert.assertTrue(
                    "the declined heal must leave the view blocked over the lost row",
                    viewInstance().isCheckpointRecoveryBlocked()
            );
            Assert.assertEquals(
                    "rebuild_blocked",
                    LiveViewCheckpointRestoreRoute.name(viewInstance().getCheckpointRestoreRoute())
            );
            assertViewRows("""
                    ts\tsym\ts
                    2026-01-01T00:00:00.000000Z\ta\t1.0
                    2026-01-01T00:00:10.000000Z\ta\t2.0
                    2026-01-01T00:00:20.000000Z\ta\t3.0
                    2026-01-01T00:00:30.000000Z\ta\t4.0
                    2026-01-01T00:59:00.000000Z\ta\t4.0
                    2026-01-01T01:59:50.000000Z\ta\t5.0
                    2026-01-01T02:00:00.000000Z\ta\t11.0
                    2026-01-01T02:00:10.000000Z\ta\t18.0
                    """);
            capture.assertLogged(HEAL_DECLINED_ROWS + " [view=lv, lowTsExclusive=2026-01-01T00:00:30.000000Z, "
                    + "highTsInclusive=2026-01-01T02:00:00.000000Z, durableRows=3, baseRows=2]");
            capture.assertNotLogged("reconstructed corrupt live view checkpoint roots");
        });
    }

    @Test
    public void testRestartDeclinesToHealALostRowOnItsIntervalsLastTimestamp() throws Exception {
        // The newest root at 01:59:50 sits alone in its hour, which the base loses while the view
        // keeps its row. The base then applies a row the view has not consumed into the root's
        // interval, which cancels the loss in the interval's count. The lost row is on the interval's
        // last timestamp, so no view timestamp above it closes its group inside the per-timestamp
        // walk, and only the walk's check after its last group declines. The rebuild from the applied
        // base restates the lost row away and consumes the unconsumed one. A healed head would have
        // kept the lost row's output and 15.0 at 02:00:10, which counts it, then answered 17.0 at
        // 02:00:20 from a runtime that never held it, where the build that sealed the head answers 24.0.
        setCadenceForAGroupSplitAcrossTheFrontier();
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " + VIEW_SQL);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int second = 0; second <= 30; second += 10) {
                    appendAndRefresh(job, second, 1);
                }
                commitAndRefresh(job, "('2026-01-01T00:59:00.000000Z', 'a', 5)");
                commitAndRefresh(job, "('2026-01-01T00:59:30.000000Z', 'a', 6)");
                commitAndRefresh(job, "('2026-01-01T01:59:50.000000Z', 'a', 7)");
                commitAndRefresh(job, "('2026-01-01T02:00:10.000000Z', 'a', 8)");
                driveRefreshToQuiescence(job);
                Assert.assertEquals(ts("2026-01-01T01:59:50.000000Z"), viewInstance().getHeadCheckpointMaxTs());
                execute("ALTER TABLE base DROP PARTITION LIST '2026-01-01T01'");
                drainWalQueue();
                driveRefreshToQuiescence(job);
                execute("INSERT INTO base VALUES ('2026-01-01T00:58:00.000000Z', 'b', 100)");
                drainWalQueue();
                corruptNewestRootDataSegment(viewInstance());
            }

            final LogCapture capture = restartAndRefreshCapturingLog();
            assertRebuiltFromAppliedBase("lv");
            Assert.assertFalse(
                    "the rebuild from the applied base must not be refused",
                    viewInstance().isCheckpointRecoveryBlocked()
            );
            try (LiveViewRefreshJob resumed = new LiveViewRefreshJob(0, engine, 1)) {
                commitAndRefresh(resumed, "('2026-01-01T02:00:20.000000Z', 'a', 9)");
                driveRefreshToQuiescence(resumed);
            }
            assertViewRows("""
                    ts\tsym\ts
                    2026-01-01T00:00:00.000000Z\ta\t1.0
                    2026-01-01T00:00:10.000000Z\ta\t2.0
                    2026-01-01T00:00:20.000000Z\ta\t3.0
                    2026-01-01T00:00:30.000000Z\ta\t4.0
                    2026-01-01T00:58:00.000000Z\tb\t100.0
                    2026-01-01T00:59:00.000000Z\ta\t5.0
                    2026-01-01T00:59:30.000000Z\ta\t11.0
                    2026-01-01T02:00:10.000000Z\ta\t8.0
                    2026-01-01T02:00:20.000000Z\ta\t17.0
                    """);
            assertViewMatchesRecompute();
            capture.assertLogged(HEAL_DECLINED_ROWS + " on a timestamp [view=lv, "
                    + "timestamp=2026-01-01T01:59:50.000000Z, durableRows=1, baseRows=0]");
            capture.assertNotLogged("reconstructed corrupt live view checkpoint roots");
        });
    }

    @Test
    public void testRestartDeclinesToHealALostRowUnderAsManyUnconsumedRowsInItsInterval() throws Exception {
        // One unconsumed row cancels the lost one in the interval's count.
        setCadenceForAGroupSplitAcrossTheFrontier();
        assertMemoryLeak(() -> assertRestartRebuildsOverALostRowAmongUnconsumedRowsInItsInterval(
                VIEW_SQL,
                """
                        ts\tsym\ts
                        2026-01-01T00:00:00.000000Z\ta\t1.0
                        2026-01-01T00:00:10.000000Z\ta\t2.0
                        2026-01-01T00:00:20.000000Z\ta\t3.0
                        2026-01-01T00:00:30.000000Z\ta\t4.0
                        2026-01-01T00:58:00.000000Z\tb\t100.0
                        2026-01-01T00:59:00.000000Z\ta\t4.0
                        2026-01-01T02:00:00.000000Z\ta\t6.0
                        2026-01-01T02:00:10.000000Z\ta\t13.0
                        2026-01-01T02:00:20.000000Z\ta\t21.0
                        """,
                "('2026-01-01T00:58:00.000000Z', 'b', 100)"
        ));
    }

    @Test
    public void testRestartDeclinesToHealALostRowUnderMoreUnconsumedRowsInItsInterval() throws Exception {
        // Two unconsumed rows outnumber the lost one, so the interval's count shows a surplus.
        setCadenceForAGroupSplitAcrossTheFrontier();
        assertMemoryLeak(() -> assertRestartRebuildsOverALostRowAmongUnconsumedRowsInItsInterval(
                VIEW_SQL,
                """
                        ts\tsym\ts
                        2026-01-01T00:00:00.000000Z\ta\t1.0
                        2026-01-01T00:00:10.000000Z\ta\t2.0
                        2026-01-01T00:00:20.000000Z\ta\t3.0
                        2026-01-01T00:00:30.000000Z\ta\t4.0
                        2026-01-01T00:58:00.000000Z\tb\t100.0
                        2026-01-01T00:58:30.000000Z\tb\t300.0
                        2026-01-01T00:59:00.000000Z\ta\t4.0
                        2026-01-01T02:00:00.000000Z\ta\t6.0
                        2026-01-01T02:00:10.000000Z\ta\t13.0
                        2026-01-01T02:00:20.000000Z\ta\t21.0
                        """,
                "('2026-01-01T00:58:00.000000Z', 'b', 100), ('2026-01-01T00:58:30.000000Z', 'b', 200)"
        ));
    }

    @Test
    public void testRestartDeclinesToHealALostRowUnderMoreUnconsumedRowsInItsIntervalOverALossyBase() throws Exception {
        setCadenceForAGroupSplitAcrossTheFrontier();
        assertMemoryLeak(() -> {
            // The base also loses a row below the history, so the rebuild the declined heal falls back
            // to is refused, and the view stays on the rows it holds.
            seedALostRowAmongUnconsumedRowsInItsInterval(
                    VIEW_SQL,
                    "('2025-12-31T23:59:00.000000Z', 'a', 1000)",
                    "('2026-01-01T00:58:00.000000Z', 'b', 100), ('2026-01-01T00:58:30.000000Z', 'b', 200)"
            );
            restartAndRefresh();
            try (LiveViewRefreshJob resumed = new LiveViewRefreshJob(0, engine, 1)) {
                commitAndRefresh(resumed, "('2026-01-01T02:00:20.000000Z', 'a', 8)");
                driveRefreshToQuiescence(resumed);
            }
            assertViewRows("""
                    ts\tsym\ts
                    2025-12-31T23:59:00.000000Z\ta\t1000.0
                    2026-01-01T00:00:00.000000Z\ta\t1.0
                    2026-01-01T00:00:10.000000Z\ta\t2.0
                    2026-01-01T00:00:20.000000Z\ta\t3.0
                    2026-01-01T00:00:30.000000Z\ta\t4.0
                    2026-01-01T00:59:00.000000Z\ta\t4.0
                    2026-01-01T01:59:50.000000Z\ta\t5.0
                    2026-01-01T02:00:00.000000Z\ta\t11.0
                    2026-01-01T02:00:10.000000Z\ta\t18.0
                    """);
            Assert.assertTrue(viewInstance().isCheckpointRecoveryBlocked());
            Assert.assertEquals(
                    "rebuild_blocked",
                    LiveViewCheckpointRestoreRoute.name(viewInstance().getCheckpointRestoreRoute())
            );
        });
    }

    @Test
    public void testRestartRefusesAHealedGenerationWhoseFrontierSealNeverLanded() throws Exception {
        // The heal of a grown ceiling under a higher frontier publishes twice: the splice that
        // re-versions the ceiling at the applied base seqTxn, then the root at the frontier. This
        // fails the second publication's data segment, which leaves on disk exactly what a crash
        // between the two leaves: a healed generation whose newest root sits below the frontier,
        // at a base seqTxn no commit above it replays from. No restore may come back on it.
        setCadenceForAGroupSplitAcrossTheFrontier();
        final AtomicBoolean isArmed = new AtomicBoolean();
        final AtomicInteger dataSegmentPublications = new AtomicInteger();
        final String dataSegmentNeedle = LiveViewCheckpointLayout.DATA_DIR_NAME
                + Files.SEPARATOR
                + LiveViewCheckpointLayout.DATA_SEGMENT_PREFIX;
        final TestFilesFacadeImpl ff = new TestFilesFacadeImpl() {
            @Override
            public int rename(LPSZ from, LPSZ to) {
                if (isArmed.get()
                        && Utf8s.containsAscii(to, dataSegmentNeedle)
                        && dataSegmentPublications.incrementAndGet() == 2) {
                    return Files.FILES_RENAME_ERR_OTHER;
                }
                return super.rename(from, to);
            }
        };
        assertMemoryLeak(ff, () -> {
            seedCorruptNewestRootUnderAHigherFrontierOverALossyBase(null);

            isArmed.set(true);
            restartAndRefresh();
            isArmed.set(false);
            Assert.assertEquals("the splice and then the frontier seal publish a data segment each", 2, dataSegmentPublications.get());
            // The heal fails, so the restart takes the rebuild, which the lost day refuses: the
            // outcome a declined heal has. The splice it did publish stays on disk.
            Assert.assertTrue(viewInstance().isCheckpointRecoveryBlocked());
            assertViewRows(HIGHER_FRONTIER_SEEDED_ROWS);

            // A restart over that generation finds no corrupt root left to heal. The restore lands
            // on the healed ceiling and replays nothing above the applied base seqTxn, so its row
            // count falls short of the view's table and the restore refuses it, rather than coming
            // back on a runtime that never saw the row above the ceiling.
            restartAndRefresh();
            Assert.assertEquals(
                    "rebuild_blocked",
                    LiveViewCheckpointRestoreRoute.name(viewInstance().getCheckpointRestoreRoute())
            );
            assertViewRows(HIGHER_FRONTIER_SEEDED_ROWS);
        });
    }

    @Test
    public void testEofRepairKeepsTheWholeLadderInsteadOfTruncatingIt() throws Exception {
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = buildHistory(job);
                final long generationBefore = generation(instance);
                Assert.assertEquals("the epoch allocated one id per seal", SEALS, nextCheckpointId(instance));
                final LongList idsBefore = logicalCheckpointIds(instance);

                // Five seconds under the head, so the correction's influence reaches the
                // runtime frontier instead of converging under it. There is no proven
                // converged suffix to keep untouched, but every root above the repair
                // floor describes output the replay is about to reproduce - so the repair
                // re-versions them in place rather than dropping them.
                correct(job, instance, SEALS * 10 - 5, 800);

                // Preserved, not retired: the generation advances rather than restarting
                // at 1, and the checkpoint id space carries forward rather than resetting.
                Assert.assertTrue(
                        "an EOF repair must preserve the timeline, not reset its generation [generation="
                                + generation(instance) + ']',
                        generation(instance) > generationBefore
                );

                // The ladder is what the next correction resumes from, so the repair has
                // to leave it standing. Every id survives, in place: a splice re-versions
                // a boundary's payload and keeps its logical coordinate, where the
                // truncate this replaced dropped every entry above the repair floor and
                // left the newest usable anchor pinned wherever the last in-order commit
                // had put it.
                final LongList ids = logicalCheckpointIds(instance);
                Assert.assertEquals("the repair must drop no logical entry", idsBefore.size(), ids.size());
                for (int i = 0, n = ids.size(); i < n; i++) {
                    Assert.assertEquals("the ladder keeps its ids in place", idsBefore.getQuick(i), ids.getQuick(i));
                }
                // A splice appends no root, and the replay stops at the end of the base
                // table - which is the newest boundary it just re-versioned - so nothing
                // above it needs sealing and no fresh id is minted.
                Assert.assertEquals(
                        "a splice that reaches its own newest root mints no boundary [nextCheckpointId="
                                + nextCheckpointId(instance) + ']',
                        SEALS,
                        nextCheckpointId(instance)
                );

                // The oldest boundary keeps the exact coordinate it was sealed at, which
                // is what makes an old O3 row's predecessor lookup a search rather than a
                // fallback to the view boundary.
                final LiveViewCheckpointTimelineEntry entry = new LiveViewCheckpointTimelineEntry();
                Assert.assertTrue(
                        "the oldest boundary must survive the near-head repair",
                        findsEntry(instance, ts(timestamp(10)), 0, entry)
                );
                assertViewMatchesRecompute();

                // A subsequent, deeper O3 correction reuses one of the surviving
                // predecessors: it localizes against them (a splice that re-versions in
                // place and mints no new boundary) instead of falling back to a full
                // rebuild from the view boundary, which a retired timeline would have
                // forced.
                final long generationBeforeDeep = generation(instance);
                final long nextIdBeforeDeep = nextCheckpointId(instance);
                correct(job, instance, 103, 700); // between the 100 and 110 groups
                Assert.assertTrue(
                        "the deeper repair must advance the generation",
                        generation(instance) > generationBeforeDeep
                );
                Assert.assertEquals(
                        "a localized repair reusing the preserved prefix mints no new boundary",
                        nextIdBeforeDeep,
                        nextCheckpointId(instance)
                );
                Assert.assertTrue(
                        "the preserved prefix stays addressable after the deeper repair",
                        findsEntry(instance, ts(timestamp(10)), 0, entry)
                );
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testRefreshJobTruncateUsesEngineLifecycleState() throws Exception {
        assertMemoryLeak(() -> {
            createView();
            final LiveViewInstance instance;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                instance = buildHistory(job);
            }
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                Assert.assertNull(job.getCheckpointTimelineLifecycleStateForTest());
                Assert.assertTrue(job.truncateOrRetireTimelineOnO3ForTest(
                        instance,
                        ts(timestamp(SEALS * 10 - 5))
                ));
                Assert.assertSame(
                        engine.getLiveViewCheckpointLifecycleState(),
                        job.getCheckpointTimelineLifecycleStateForTest()
                );
            }
        });
    }

    @Test
    public void testEofRepairTruncatesThePrefixWhenTheChainIsDeclined() throws Exception {
        // The budget that governs how many boundaries one repair may re-version. Zero
        // declines the chain outright, which is the fallback a correction deeper than the
        // budget takes in the field - and the only way left to reach the truncate, since
        // an ordinary EOF correction now keeps its ladder.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 0);
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = buildHistory(job);
                final long generationBefore = generation(instance);
                Assert.assertEquals("the epoch allocated one id per seal", SEALS, nextCheckpointId(instance));

                correct(job, instance, SEALS * 10 - 5, 800);

                // Preserved, not retired: the roots below the repair floor are still
                // correct whatever happens above them, so the truncate keeps them and the
                // generation and id space carry forward rather than restarting.
                Assert.assertTrue(
                        "a truncating repair must preserve the prefix, not reset its generation [generation="
                                + generation(instance) + ']',
                        generation(instance) > generationBefore
                );
                Assert.assertTrue(
                        "the checkpoint id space must carry forward, not reset [nextCheckpointId="
                                + nextCheckpointId(instance) + ']',
                        nextCheckpointId(instance) > SEALS
                );

                // The logical entry set is the preserved prefix plus the newly sealed
                // head: a contiguous run from id 0 and then the head at the preserved
                // next id. The tail roots the repair rewrote are gone, so this is a
                // pruned tail of the same epoch, not a fresh history restarting at zero.
                final LongList ids = logicalCheckpointIds(instance);
                Assert.assertTrue("more than the head must survive the repair", ids.size() > 1);
                Assert.assertEquals("the preserved prefix keeps id 0", 0, ids.getQuick(0));
                for (int i = 0, n = ids.size() - 1; i < n; i++) {
                    Assert.assertEquals("the preserved prefix is contiguous from zero", i, ids.getQuick(i));
                }
                // The head the truncate re-sealed takes the next free id, and the run
                // below it stops one short of it - which is the entry the truncate
                // dropped, and the discriminator against the splice this case exists to
                // avoid taking.
                Assert.assertEquals(
                        "the sealed head takes the preserved next id",
                        SEALS,
                        ids.getQuick(ids.size() - 1)
                );
                Assert.assertEquals(
                        "the truncate must have dropped the tail it rewrote",
                        SEALS - 1,
                        ids.getQuick(ids.size() - 2) + 1
                );

                // The oldest boundary keeps the exact coordinate it was sealed at, which
                // is what makes an old O3 row's predecessor lookup a search rather than a
                // fallback to the view boundary.
                final LiveViewCheckpointTimelineEntry entry = new LiveViewCheckpointTimelineEntry();
                Assert.assertTrue(
                        "the oldest boundary must survive the near-head repair",
                        findsEntry(instance, ts(timestamp(10)), 0, entry)
                );
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testEofRepairRetiresTheTimelineWhenTheTruncateCannotWriteItsMarker() throws Exception {
        // The truncate above, on a filesystem that refuses the repair marker. The marker
        // has to be durable before the truncate publishes, because it alone holds a restart
        // off the truncated head until the post-replay seal re-anchors it. A truncate that
        // cannot write one keeps nothing: truncateOrRetireTimelineOnO3 catches the failure
        // and retires the whole timeline ahead of the replacement commit, so no root above
        // the floor outlives the output it describes. The repair itself goes through, and
        // the post-replay seal opens a fresh history that a restart restores from.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 0);
        final AtomicBoolean isArmed = new AtomicBoolean();
        final AtomicInteger markerAttempts = new AtomicInteger();
        final TestFilesFacadeImpl ff = new TestFilesFacadeImpl() {
            @Override
            public int rename(LPSZ from, LPSZ to) {
                if (isArmed.get() && Utf8s.containsAscii(to, LiveViewCheckpointLayout.REPAIRING_MARKER_FILE_NAME)) {
                    markerAttempts.incrementAndGet();
                    return Files.FILES_RENAME_ERR_OTHER;
                }
                return super.rename(from, to);
            }
        };
        assertMemoryLeak(ff, () -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = buildHistory(job);
                final long resetsBefore = instance.getCheckpointTimelineResets();

                isArmed.set(true);
                correct(job, instance, SEALS * 10 - 5, 800);
                isArmed.set(false);

                Assert.assertEquals("the truncate attempts its marker once", 1, markerAttempts.get());
                Assert.assertEquals(
                        "a truncate that cannot write its marker must retire the timeline",
                        resetsBefore + 1,
                        instance.getCheckpointTimelineResets()
                );
                try (Path dir = checkpointsDir(instance)) {
                    Assert.assertFalse(
                            "a retired timeline owes no marker",
                            LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir)
                    );
                }
                // Retired, not preserved: the post-replay seal opens a fresh history instead
                // of carrying the generation and the id space forward, and the oldest boundary
                // goes with the timeline it belonged to.
                Assert.assertEquals("the post-replay seal must open a fresh history", 1, generation(instance));
                final long entries = assertEpochRetainsEveryEntry(instance, 0, "after the retire");
                Assert.assertTrue(
                        "no entry of the retired history may survive [entries=" + entries + ']',
                        entries < SEALS
                );
                Assert.assertFalse(
                        "the oldest boundary must go with the retired timeline",
                        findsEntry(instance, ts(timestamp(10)), 0, new LiveViewCheckpointTimelineEntry())
                );
                assertViewMatchesRecompute();
            }

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            try (LiveViewRefreshJob resumed = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(resumed);
            }
            assertRestoredFromTimeline("lv");
            assertViewMatchesRecompute();
        });
    }

    @Test
    public void testStaleRepairMarkerIsIgnoredOnRestart() throws Exception {
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = buildHistory(job);
                // A marker a completed repair left behind: a later generation (the
                // current one) sealed well past the marker's recorded base generation,
                // so a restart must ignore it and restore normally.
                writeRepairMarker(instance, generation(instance) - 3);
            }

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            try (LiveViewRefreshJob resumed = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(resumed);
            }

            final LiveViewInstance restored = viewInstance();
            // The stale marker is ignored and cleared; the timeline restores rather than
            // rebuilding, so its generation is not reset to a fresh epoch.
            Assert.assertTrue("a stale marker must not force a rebuild", generation(restored) > 1);
            try (Path dir = checkpointsDir(restored)) {
                Assert.assertFalse(
                        "a stale marker must be cleared on restore",
                        LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir)
                );
            }
            assertViewMatchesRecompute();
        });
    }

    @Test
    public void testVersionOneRepairMarkerUnderItsOwnGenerationForcesRebuildOnRestart() throws Exception {
        // A version 1 marker, the layout builds before the seqTxn was recorded wrote, reads
        // back with no seqTxn. Under its own generation only the seqTxn can prove the repair
        // moved nothing durable, so a marker without one must read as live and send the
        // restart to the rebuild from the applied base.
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = buildHistory(job);
                // The control: the same marker in version 2, recording the seqTxn the view's
                // WAL ends at and its table applied, reads as stale. That proves the view
                // meets every other staleness condition, so the version 1 marker below reads
                // as live on its missing seqTxn alone.
                writeRepairMarker(
                        instance,
                        generation(instance),
                        engine.getTableSequencerAPI().lastTxn(instance.getLiveViewToken())
                );
            }

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            try (LiveViewRefreshJob resumed = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(resumed);
            }
            assertRestoredFromTimeline("lv");
            final LiveViewInstance restored = viewInstance();
            final long generation = generation(restored);
            Assert.assertTrue("the control must restore rather than rebuild", generation > 1);
            try (Path dir = checkpointsDir(restored)) {
                Assert.assertFalse(
                        "the restart must clear the stale control marker",
                        LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir)
                );
                writeVersionOneRepairMarker(restored, generation);
                // The premise: a valid version 1 record under the view's own generation, not a
                // torn one, whose unreadable base generation would force the rebuild by itself.
                Assert.assertEquals(generation, LiveViewCheckpointRepairMarker.readBaseGeneration(configuration, dir));
                Assert.assertEquals(Numbers.LONG_NULL, LiveViewCheckpointRepairMarker.readLvSeqTxn(configuration, dir));
            }
            final long lvSeqTxn = engine.getTableSequencerAPI().lastTxn(restored.getLiveViewToken());
            try (TableReader reader = engine.getReader(restored.getLiveViewToken())) {
                Assert.assertEquals("the view's table must have applied its whole WAL", lvSeqTxn, reader.getSeqTxn());
            }

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            try (LiveViewRefreshJob resumed = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(resumed);
            }
            assertRebuiltFromAppliedBase("lv");
            final LiveViewInstance rebuilt = viewInstance();
            Assert.assertEquals("the live marker must force a rebuild that resets the generation", 1, generation(rebuilt));
            try (Path dir = checkpointsDir(rebuilt)) {
                Assert.assertFalse(
                        "the rebuild must remove the repair marker",
                        LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir)
                );
            }
            assertViewMatchesRecompute();
        });
    }

    private static Path checkpointsDir(LiveViewInstance instance) {
        return new Path().of(configuration.getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    // Second-of-day of the n-th historical correction: three seconds above the group
    // its ordinal names, so it collides with no in-order group (a multiple of ten) and
    // stays deep enough below the head for its influence to converge under the frontier.
    private static int historicalSecond(int correction) {
        return 10 * correction + 3;
    }

    // Base rows a repair replayed over this instance's lifetime, through either
    // disposition: the resume from a boundary below the change, or the localized
    // rebuild over the change's own dependency interval. In-order appends leave both
    // at zero.
    private static long repairedRows(LiveViewInstance instance) {
        return instance.getO3BoundaryReplayRows() + instance.getO3ResumeReplayRows();
    }

    // Three rows per seal and no duration trigger, so the commit carrying a tie and a row above
    // it, two rows, seals nothing.
    private static void setCadenceForAGroupSplitAcrossTheFrontier() {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 3);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ADAPTIVE_CADENCE_ENABLED, "false");
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_MAX_DURATION_MICROS, 86_400_000_000L);
    }

    private static String timestamp(int secondOfDay) {
        return String.format("2026-01-01T00:%02d:%02d.000000Z", secondOfDay / 60, secondOfDay % 60);
    }

    private void appendAndRefresh(LiveViewRefreshJob job, int second, long value) throws Exception {
        commitAndRefresh(job, "('" + timestamp(second) + "', 'a', " + value + ")");
    }

    /**
     * Asserts the published generation still holds every logical entry the epoch ever
     * allocated - the checkpoint ids {@code [0, nextCheckpointId)}, in order - and that
     * the count has not fallen below {@code previousEntries}. Returns the current count
     * so a caller can thread it through the next step.
     */
    private long assertEpochRetainsEveryEntry(LiveViewInstance instance, long previousEntries, String when) {
        final LongList ids = logicalCheckpointIds(instance);
        final long nextCheckpointId = nextCheckpointId(instance);
        Assert.assertEquals(
                "the epoch must hold every checkpoint id it allocated " + when,
                nextCheckpointId,
                ids.size()
        );
        for (int i = 0, n = ids.size(); i < n; i++) {
            Assert.assertEquals("logical entry at index " + i + " " + when, i, ids.getQuick(i));
        }
        Assert.assertTrue(
                "the logical entry count must never drop " + when
                        + " [before=" + previousEntries + ", after=" + ids.size() + ']',
                ids.size() >= previousEntries
        );
        return ids.size();
    }

    private void assertPredecessorIs(
            LiveViewInstance instance,
            long correctionTimestamp,
            long checkpointId,
            LiveViewCheckpointTimelineEntry out
    ) {
        try (
                LiveViewCheckpointMetaStore store = openStore(instance);
                LiveViewCheckpointGenerationPin pin = store.pin();
                LiveViewCheckpointTimelineReader reader = openTimelineReader(instance)
        ) {
            Assert.assertTrue(
                    "a correction at " + correctionTimestamp + " must find a predecessor boundary",
                    reader.predecessor(pin.getTimelineRootRef(), correctionTimestamp, out)
            );
        }
        Assert.assertEquals(checkpointId, out.checkpointId);
    }

    // The newest interval holds a row the base lost among rows the view has not consumed, over a base
    // that holds every other row the view does. The heal declines, so the restart rebuilds from the
    // applied base, which restates the lost row away and consumes the unconsumed ones. A healed head
    // would have kept the lost row's output and the two above it that count it, then answered 21.0 at
    // 02:00:20 - whose window reaches back over the lost row - from a runtime that never held it,
    // where the build that sealed the head answers 26.0.
    private void assertRestartRebuildsOverALostRowAmongUnconsumedRowsInItsInterval(
            String viewSql,
            String expectedRows,
            String... unconsumedCommits
    ) throws Exception {
        seedALostRowAmongUnconsumedRowsInItsInterval(viewSql, null, unconsumedCommits);
        restartAndRefresh();
        try (LiveViewRefreshJob resumed = new LiveViewRefreshJob(0, engine, 1)) {
            commitAndRefresh(resumed, "('2026-01-01T02:00:20.000000Z', 'a', 8)");
            driveRefreshToQuiescence(resumed);
        }
        assertViewRows(expectedRows);
        assertRebuiltFromAppliedBase("lv");
        Assert.assertFalse(viewInstance().isCheckpointRecoveryBlocked());
        assertViewMatchesRecompute(viewSql);

        restartAndRefresh();
        assertRestoredFromTimeline("lv");
        assertNoRefreshFaults("lv");
        assertViewRows(expectedRows);
    }

    // A RANGE view whose newest root, at 00:01:20, is torn after the base applied unconsumedRows -
    // which sit strictly inside the root's interval, below the durable frontier - and before the
    // view consumed them. The first commit carries a row a day below the history that the base
    // then loses while the view keeps it, so a rebuild from the applied base is refused. The heal
    // folds the unconsumed rows, and the drain then repairs their commit as out of order, so the
    // view keeps refreshing over the right values, and across a restart.
    private void assertRestartHealsOverAnUnconsumedOutOfOrderRow(String viewSql, String unconsumedRows) throws Exception {
        createView(viewSql);
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            commitAndRefresh(job, "('2025-12-31T23:59:00.000000Z', 'a', 1000)");
            for (int commit = 1; commit <= 8; commit++) {
                appendAndRefresh(job, commit * 10, commit);
            }
            driveRefreshToQuiescence(job);
            execute("ALTER TABLE base DROP PARTITION LIST '2025-12-31'");
            drainWalQueue();
            driveRefreshToQuiescence(job);
            Assert.assertEquals(ts(timestamp(80)), viewInstance().getHeadCheckpointMaxTs());
            execute("INSERT INTO base VALUES " + unconsumedRows);
            drainWalQueue();
            corruptNewestRootDataSegment(viewInstance());
        }

        restartAndRefresh();
        assertRestoredFromTimeline("lv");
        Assert.assertFalse(
                "the heal must not leave the view blocked over the lost day",
                viewInstance().isCheckpointRecoveryBlocked()
        );
        Assert.assertTrue("the drain must repair the late row's commit", repairedRows(viewInstance()) > 0);
        assertNoRefreshFaults("lv");
        final String viewRows = """
                ts\tsym\ts
                2025-12-31T23:59:00.000000Z\ta\t1000.0
                2026-01-01T00:00:10.000000Z\ta\t1.0
                2026-01-01T00:00:20.000000Z\ta\t3.0
                2026-01-01T00:00:30.000000Z\ta\t6.0
                2026-01-01T00:00:40.000000Z\ta\t10.0
                2026-01-01T00:00:50.000000Z\ta\t14.0
                2026-01-01T00:01:00.000000Z\ta\t18.0
                2026-01-01T00:01:10.000000Z\ta\t22.0
                2026-01-01T00:01:15.000000Z\ta\t68.0
                2026-01-01T00:01:20.000000Z\ta\t76.0
                """;
        assertViewRows(viewRows);

        try (LiveViewRefreshJob resumed = new LiveViewRefreshJob(0, engine, 1)) {
            commitAndRefresh(resumed, "('" + timestamp(90) + "', 'a', 9), ('" + timestamp(90) + "', 'b', 11)");
            commitAndRefresh(resumed, "('" + timestamp(100) + "', 'a', 10), ('" + timestamp(100) + "', 'b', 12)");
            driveRefreshToQuiescence(resumed);
        }
        final String grownRows = viewRows + """
                2026-01-01T00:01:30.000000Z\ta\t80.0
                2026-01-01T00:01:30.000000Z\tb\t11.0
                2026-01-01T00:01:40.000000Z\ta\t84.0
                2026-01-01T00:01:40.000000Z\tb\t23.0
                """;
        assertViewRows(grownRows);
        assertNoRefreshFaults("lv");

        restartAndRefresh();
        assertRestoredFromTimeline("lv");
        assertNoRefreshFaults("lv");
        assertViewRows(grownRows);
        try (LiveViewRefreshJob resumed = new LiveViewRefreshJob(0, engine, 1)) {
            commitAndRefresh(resumed, "('" + timestamp(105) + "', 'a', 1), ('" + timestamp(105) + "', 'b', 1)");
            driveRefreshToQuiescence(resumed);
        }
        assertViewRows(grownRows + """
                2026-01-01T00:01:45.000000Z\ta\t78.0
                2026-01-01T00:01:45.000000Z\tb\t24.0
                """);
        assertNoRefreshFaults("lv");
    }

    // The live view must equal the same window recomputed directly over the base table.
    // A refresh fault self-heals into exactly that recompute, so the fault count guards
    // that the view converged through the incremental and repair paths rather than
    // through a recovery rebuild that would also have thrown the timeline away.
    private void assertViewMatchesRecompute() throws Exception {
        assertViewMatchesRecompute(VIEW_SQL);
    }

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

    private void assertViewRows(String expected) throws Exception {
        assertQuery("SELECT ts, sym, s FROM lv")
                .noLeakCheck()
                .timestamp("ts")
                .expectSize()
                .returns(expected);
    }

    private LiveViewInstance buildHistory(LiveViewRefreshJob job) throws Exception {
        for (int commit = 1; commit <= SEALS; commit++) {
            appendAndRefresh(job, commit * 10, commit);
        }
        driveRefreshToQuiescence(job);
        final LiveViewInstance instance = viewInstance();
        Assert.assertEquals(SEALS, assertEpochRetainsEveryEntry(instance, 0, "after building the history"));
        return instance;
    }

    private void commitAndRefresh(LiveViewRefreshJob job, String values) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        execute("INSERT INTO base VALUES " + values);
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
    }

    // Commits one out-of-order row and drives the refresh job to quiescence over it,
    // asserting the change was actually repaired: a case whose corrections quietly
    // degenerated into appends would leave every retention assertion beside them
    // testing nothing but ordinary cadence.
    private void correct(LiveViewRefreshJob job, LiveViewInstance instance, int second, long value) throws Exception {
        final long repairedBefore = repairedRows(instance);
        appendAndRefresh(job, second, value);
        driveRefreshToQuiescence(job);
        Assert.assertTrue(
                "the row at second " + second + " must be repaired rather than appended",
                repairedRows(instance) > repairedBefore
        );
    }

    // Truncates the newest logical root's first data segment by one byte, so the
    // reader rejects its state page on a length check - the cheapest structural
    // corruption a torn write can leave. Mirrors the seal test's corruption helper.
    private void corruptNewestRootDataSegment(LiveViewInstance instance) {
        final long segmentId;
        final long fileLength;
        try (
                LiveViewCheckpointMetaStore store = openStore(instance);
                LiveViewCheckpointGenerationPin pin = store.pin();
                LiveViewCheckpointTimelineReader timeline = openTimelineReader(instance);
                LiveViewCheckpointRoot root = new LiveViewCheckpointRoot(configuration);
                LiveViewCheckpointSegmentDirectoryReader directory =
                        new LiveViewCheckpointSegmentDirectoryReader(configuration);
                Path checkpointsDir = checkpointsDir(instance)
        ) {
            final LiveViewCheckpointTimelineEntry newest = new LiveViewCheckpointTimelineEntry();
            Assert.assertTrue("the timeline must hold a newest root", timeline.last(pin.getTimelineRootRef(), newest));
            root.of(checkpointsDir, newest.rootRef);
            segmentId = root.getSegmentId(0);
            directory.of(checkpointsDir, pin.getSegmentDirectoryRootRef());
            fileLength = directory.getFileLength(segmentId);
        }
        try (Path checkpointsDir = checkpointsDir(instance); Path dataPath = new Path()) {
            LiveViewCheckpointLayout.dataSegmentPath(dataPath, checkpointsDir, segmentId);
            final FilesFacade ff = configuration.getFilesFacade();
            final long fd = ff.openRW(dataPath.$(), 0);
            try {
                Assert.assertTrue("truncating the data segment must succeed", ff.truncate(fd, fileLength - 1));
            } finally {
                ff.close(fd);
            }
        }
    }

    private void createView() throws Exception {
        createView(VIEW_SQL);
    }

    private void createView(String viewSql) throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " + viewSql);
    }

    /**
     * Point lookup of one logical boundary by its full {@code (maxTimestamp, checkpointId)} key.
     */
    private boolean findsEntry(
            LiveViewInstance instance,
            long maxTimestamp,
            long checkpointId,
            LiveViewCheckpointTimelineEntry out
    ) {
        try (
                LiveViewCheckpointMetaStore store = openStore(instance);
                LiveViewCheckpointGenerationPin pin = store.pin();
                LiveViewCheckpointTimelineReader reader = openTimelineReader(instance)
        ) {
            return reader.findExact(pin.getTimelineRootRef(), maxTimestamp, checkpointId, out);
        }
    }

    private long generation(LiveViewInstance instance) {
        try (LiveViewCheckpointMetaStore store = openStore(instance)) {
            return store.getSuperblock().generation;
        }
    }

    private LongList logicalCheckpointIds(LiveViewInstance instance) {
        final LongList ids = new LongList();
        try (
                LiveViewCheckpointMetaStore store = openStore(instance);
                LiveViewCheckpointGenerationPin pin = store.pin();
                LiveViewCheckpointTimelineReader reader = openTimelineReader(instance)
        ) {
            reader.iterateAll(pin.getTimelineRootRef(), entry -> ids.add(entry.checkpointId));
        }
        return ids;
    }

    private long nextCheckpointId(LiveViewInstance instance) {
        try (LiveViewCheckpointMetaStore store = openStore(instance)) {
            return store.getSuperblock().nextCheckpointId;
        }
    }

    private LiveViewCheckpointMetaStore openStore(LiveViewInstance instance) {
        final LiveViewCheckpointMetaStore store = new LiveViewCheckpointMetaStore(configuration);
        try (Path dir = checkpointsDir(instance)) {
            store.of(dir);
        }
        return store;
    }

    private LiveViewCheckpointTimelineReader openTimelineReader(LiveViewInstance instance) {
        final LiveViewCheckpointTimelineReader reader = new LiveViewCheckpointTimelineReader(configuration);
        try (Path dir = checkpointsDir(instance)) {
            reader.of(dir);
        }
        return reader;
    }

    // The primary-owned lifecycle reconciliation a publication or a startup would run,
    // driven here at a quiescent point so the correction's obsolete segments are
    // reclaimed against a timeline nothing else is touching. The definition txn and
    // history epoch are the ones the engine passes, so this must never look like an
    // epoch change and retire the timeline.
    private void purgeCycle(LiveViewInstance instance) {
        try (Path dir = checkpointsDir(instance)) {
            final LiveViewCheckpointLifecycle.ReconcileResult result = LiveViewCheckpointLifecycle.reconcile(
                    configuration,
                    dir,
                    instance.getLiveViewToken().getTableId(),
                    0,
                    true
            );
            Assert.assertFalse("the definition and epoch are fixed for the whole case", result.isEpochReplaced());
            Assert.assertFalse("this build wrote the directory it is reconciling", result.isFormatReset());
            Assert.assertEquals("no obsolete segment may fail to unlink", 0, result.getFailedPurgeCount());
            Assert.assertEquals("no orphan may fail removal", 0, result.getFailedOrphanCount());
        }
    }

    // Drops the in-memory registry, rebuilds it from disk and drives the restart's recovery
    // to quiescence, as a process restart would.
    private void restartAndRefresh() {
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
        try (LiveViewRefreshJob resumed = new LiveViewRefreshJob(0, engine, 1)) {
            driveRefreshToQuiescence(resumed);
        }
    }

    // Restarts as restartAndRefresh does, with the log captured, so a case whose outcome several
    // checks can produce asserts the one that fired. The capture stops before it returns, and keeps
    // what it captured for the assertions.
    private LogCapture restartAndRefreshCapturingLog() {
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            restartAndRefresh();
            capture.drain();
        } finally {
            capture.stop();
        }
        return capture;
    }

    // A RANGE view over a base partitioned by hour, whose newest root at 02:00:00 sits under an
    // unsealed frontier row at 02:00:10. The root's interval above its predecessor holds 00:59:00,
    // 01:59:50 and 02:00:00. The base then loses 01:59:50's hour, which the view walks past and keeps,
    // and applies unconsumedCommits, if any, into the same interval before the view consumes them,
    // which can leave the interval holding no fewer base rows than view rows. earlierRow, when not
    // null, rides in the first commit below the history, and the base loses its hour too. The newest
    // root's data segment is torn last.
    private void seedALostRowAmongUnconsumedRowsInItsInterval(
            String viewSql,
            @Nullable String earlierRow,
            String... unconsumedCommits
    ) throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY HOUR WAL");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " + viewSql);
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            commitAndRefresh(job, (earlierRow != null ? earlierRow + ", " : "") + "('" + timestamp(0) + "', 'a', 1)");
            for (int second = 10; second <= 30; second += 10) {
                appendAndRefresh(job, second, 1);
            }
            commitAndRefresh(job, "('2026-01-01T00:59:00.000000Z', 'a', 4)");
            commitAndRefresh(job, "('2026-01-01T01:59:50.000000Z', 'a', 5)");
            commitAndRefresh(job, "('2026-01-01T02:00:00.000000Z', 'a', 6)");
            commitAndRefresh(job, "('2026-01-01T02:00:10.000000Z', 'a', 7)");
            driveRefreshToQuiescence(job);
            final LiveViewInstance instance = viewInstance();
            Assert.assertEquals(ts("2026-01-01T02:00:00.000000Z"), instance.getHeadCheckpointMaxTs());
            final LiveViewCheckpointTimelineEntry predecessor = new LiveViewCheckpointTimelineEntry();
            try (
                    LiveViewCheckpointMetaStore store = openStore(instance);
                    LiveViewCheckpointGenerationPin pin = store.pin();
                    LiveViewCheckpointTimelineReader reader = openTimelineReader(instance)
            ) {
                Assert.assertTrue(reader.predecessor(pin.getTimelineRootRef(), instance.getHeadCheckpointMaxTs(), predecessor));
            }
            Assert.assertTrue(
                    "the lost row and the unconsumed rows must share the newest root's interval",
                    predecessor.maxTimestamp < ts("2026-01-01T00:58:00.000000Z")
            );
            execute("ALTER TABLE base DROP PARTITION LIST '2026-01-01T01'" + (earlierRow != null ? ", '2025-12-31T23'" : ""));
            drainWalQueue();
            driveRefreshToQuiescence(job);
            for (String unconsumedRows : unconsumedCommits) {
                execute("INSERT INTO base VALUES " + unconsumedRows);
            }
            drainWalQueue();
            corruptNewestRootDataSegment(instance);
        }
    }

    // A RANGE view whose newest root, at 00:01:00, holds part of its timestamp group: one
    // commit carries a tie on it and a row above it at 00:01:10, and the cadence seals neither.
    // The first commit carries a row a day below the history that the base then loses while the
    // view keeps it, so a rebuild from the applied base is refused. When unconsumedRows is not
    // null, the base applies them after the view went quiescent, and the view never consumes
    // them. The newest root's data segment is torn last.
    private void seedCorruptNewestRootUnderAHigherFrontierOverALossyBase(@Nullable String unconsumedRows) throws Exception {
        createView();
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            commitAndRefresh(job, "('2025-12-31T23:59:00.000000Z', 'a', 1000)");
            for (int commit = 1; commit <= 6; commit++) {
                appendAndRefresh(job, commit * 10, commit);
            }
            commitAndRefresh(job, "('" + timestamp(60) + "', 'b', 100), ('" + timestamp(70) + "', 'a', 7)");
            driveRefreshToQuiescence(job);
            Assert.assertEquals(ts(timestamp(60)), viewInstance().getHeadCheckpointMaxTs());
            execute("ALTER TABLE base DROP PARTITION LIST '2025-12-31'");
            drainWalQueue();
            driveRefreshToQuiescence(job);
            if (unconsumedRows != null) {
                execute("INSERT INTO base VALUES " + unconsumedRows);
                drainWalQueue();
            }
            corruptNewestRootDataSegment(viewInstance());
        }
    }

    private LiveViewInstance viewInstance() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull("live view 'lv' must be registered", instance);
        return instance;
    }

    // Stamps a repair marker on disk to stand in for a crash in the middle of a
    // prefix-preserving repair. The identity fields mirror what the repair writes. The
    // recorded seqTxn sits one below the view's newest commit, as a repair whose
    // replacement committed leaves it, so only the base generation drives the restart
    // decision.
    private void writeRepairMarker(LiveViewInstance instance, long baseGeneration) {
        writeRepairMarker(
                instance,
                baseGeneration,
                engine.getTableSequencerAPI().lastTxn(instance.getLiveViewToken()) - 1
        );
    }

    private void writeRepairMarker(LiveViewInstance instance, long baseGeneration, long lvSeqTxn) {
        try (Path dir = checkpointsDir(instance)) {
            LiveViewCheckpointRepairMarker.write(
                    configuration,
                    dir,
                    instance.getLiveViewToken().getTableId(),
                    0,
                    baseGeneration,
                    ts(timestamp(10)),
                    lvSeqTxn
            );
        }
    }

    // Stamps the version 1 marker layout the builds before the seqTxn was recorded wrote:
    // the identity fields writeRepairMarker records, up to the floor timestamp, then the CRC
    // of everything before it. The record carries no seqTxn.
    private void writeVersionOneRepairMarker(LiveViewInstance instance, long baseGeneration) {
        final FilesFacade ff = configuration.getFilesFacade();
        final int size = LiveViewCheckpointRepairMarker.V1_SIZE;
        try (Path dir = checkpointsDir(instance); Path markerPath = new Path()) {
            LiveViewCheckpointLayout.repairingMarkerPath(markerPath, dir);
            final long fd = ff.openRW(markerPath.$(), configuration.getWriterFileOpenOpts());
            Assert.assertTrue(fd > -1);
            final long buf = Unsafe.calloc(size, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().putLong(buf + LiveViewCheckpointRepairMarker.MAGIC_OFFSET, LiveViewCheckpointRepairMarker.MARKER_MAGIC);
                Unsafe.getUnsafe().putInt(buf + LiveViewCheckpointRepairMarker.FORMAT_VERSION_OFFSET, LiveViewCheckpointRepairMarker.V1_FORMAT_VERSION);
                Unsafe.getUnsafe().putLong(buf + LiveViewCheckpointRepairMarker.DEFINITION_TXN_OFFSET, instance.getLiveViewToken().getTableId());
                Unsafe.getUnsafe().putLong(buf + LiveViewCheckpointRepairMarker.HISTORY_EPOCH_OFFSET, 0);
                Unsafe.getUnsafe().putLong(buf + LiveViewCheckpointRepairMarker.BASE_GENERATION_OFFSET, baseGeneration);
                Unsafe.getUnsafe().putLong(buf + LiveViewCheckpointRepairMarker.FLOOR_TIMESTAMP_OFFSET, ts(timestamp(10)));
                Unsafe.getUnsafe().putInt(
                        buf + LiveViewCheckpointRepairMarker.V1_CRC_OFFSET,
                        Zip.crc32(0, buf, LiveViewCheckpointRepairMarker.V1_CRC_OFFSET)
                );
                Assert.assertEquals(size, ff.write(fd, buf, size, 0));
            } finally {
                ff.close(fd);
                Unsafe.free(buf, size, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }
}
