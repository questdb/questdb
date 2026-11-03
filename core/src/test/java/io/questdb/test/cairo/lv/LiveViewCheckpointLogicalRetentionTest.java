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
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointLifecycle;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointRecoveryPhase;
import io.questdb.cairo.lv.LiveViewCheckpointRepairMarker;
import io.questdb.cairo.lv.LiveViewCheckpointRepairRoute;
import io.questdb.cairo.lv.LiveViewCheckpointRoot;
import io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectoryReader;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineEntry;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineReader;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreWriter;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewRefreshStats;
import io.questdb.cairo.lv.LiveViewState;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.griffin.SqlException;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.Zip;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.IntSupplier;

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
    // What a departure of the newest of two roots above the day the base loses leaves.
    private static final String DEPARTURE_ROWS_OVER_THE_LOST_DAY = """
            ts\tsym\tx\ts
            2025-12-31T12:00:00.000000Z\ta\t0.5\t0.5
            2026-01-01T00:01:00.000000Z\ta\t1.5\t2.0
            """;
    private static final String DEPARTURE_ROWS_QUERY = "SELECT ts, sym, x, s FROM lv";
    // One upsert over the head-miss departure fixture: 00:57 to a new value, 00:58 and 00:59 out
    // of the filter.
    private static final String HEAD_MISS_KEPT_ROW_DEPARTURE = "('2026-01-01T00:57:00.000000Z', 'a', 5.0), "
            + "('2026-01-01T00:58:00.000000Z', 'a', -1.0), ('2026-01-01T00:59:00.000000Z', 'a', -1.0)";
    // What HEAD_MISS_KEPT_ROW_DEPARTURE leaves: the kept row's one-minute frame holds 00:56 too.
    private static final String HEAD_MISS_KEPT_ROW_DEPARTURE_ROWS = headMissDepartureRows(true, 56)
            + "2026-01-01T00:57:00.000000Z\ta\t5.0\t6.0\n";
    // A filtered view over a deduplicating base: an upsert of a value the filter rejects moves
    // a row out of it.
    private static final String DEPARTURE_VIEW_SQL = "SELECT ts, sym, x, sum(x) OVER (PARTITION BY sym ORDER BY ts "
            + "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) s FROM base WHERE x > 0";
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
    // The checkpoint files a splicing EOF repair and the seals around it write: the repair's
    // marker and descriptor, the seals' segments and retirement queue, and the superblock.
    private static final ObjList<String> SPLICE_SYNCED_FILE_KINDS = new ObjList<>(
            LiveViewCheckpointLayout.REPAIRING_MARKER_FILE_NAME,
            LiveViewCheckpointLayout.REPAIR_DIR_NAME,
            LiveViewCheckpointLayout.DATA_DIR_NAME,
            LiveViewCheckpointLayout.META_DIR_NAME,
            LiveViewCheckpointLayout.RETIREMENT_QUEUE_FILE_NAME,
            LiveViewCheckpointLayout.TIMELINE_FILE_NAME
    );
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
                Assert.assertNull(job.getCheckpointTimelinePurgeStateForTest());
                long entries = 0;
                for (int commit = 1; commit <= SEALS; commit++) {
                    appendAndRefresh(job, commit * 10, commit);
                    if (commit == 1) {
                        Assert.assertSame(
                                engine.getLiveViewCheckpointPurgeState(),
                                job.getCheckpointTimelinePurgeStateForTest()
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
    public void testEofRepairPreservedTimelineRestoresAfterRestartInAsyncCommitMode() throws Exception {
        assertCheckpointFilesSyncInCommitMode(
                "async",
                SPLICE_SYNCED_FILE_KINDS,
                this::testEofRepairPreservedTimelineRestoresAfterRestart
        );
    }

    @Test
    public void testEofRepairPreservedTimelineRestoresAfterRestartInSyncCommitMode() throws Exception {
        assertCheckpointFilesSyncInCommitMode(
                "sync",
                SPLICE_SYNCED_FILE_KINDS,
                this::testEofRepairPreservedTimelineRestoresAfterRestart
        );
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
                Assert.assertNull(job.getCheckpointTimelinePurgeStateForTest());
                long entries = SEALS;
                for (int correction = 1; correction <= CORRECTIONS; correction++) {
                    final long generationBefore = generation(instance);
                    correct(job, instance, historicalSecond(correction), 900 + correction);
                    if (correction == 1) {
                        Assert.assertSame(
                                engine.getLiveViewCheckpointPurgeState(),
                                job.getCheckpointTimelinePurgeStateForTest()
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
                Assert.assertNull(resumed.getCheckpointTimelinePurgeStateForTest());
                driveRefreshToQuiescence(resumed);
                Assert.assertSame(
                        engine.getLiveViewCheckpointPurgeState(),
                        resumed.getCheckpointTimelinePurgeStateForTest()
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
                    LiveViewCheckpointRecoveryPhase.routeName(viewInstance().getCheckpointRestoreRoute())
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
                    LiveViewCheckpointRecoveryPhase.routeName(viewInstance().getCheckpointRestoreRoute())
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
    public void testRestartDeclinesToHealALostRowInACompositePartitionOfTheViewsTable() throws Exception {
        // testRestartDeclinesToHealALostRowOnItsIntervalsLastTimestamp, over a view's table
        // partitioned by DAY. A late row of another symbol, which the view repairs in, makes the
        // day composite: the repair rewrites the day from the late row up at the tail of the column
        // files, and every row after it appends there too. So the lost row sits past the day's
        // first liveRows file rows, where the per-timestamp walk only finds it by walking the day's
        // pieces. A walk of those file rows reads the dead rows the repair superseded instead,
        // never meets the lost row, and the heal goes ahead.
        setCadenceForAGroupSplitAcrossTheFrontier();
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms PARTITION BY DAY START FROM NOW AS " + VIEW_SQL);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int second = 0; second <= 30; second += 10) {
                    appendAndRefresh(job, second, 1);
                }
                final long repairedBefore = repairedRows(viewInstance());
                commitAndRefresh(job, "('2026-01-01T00:00:05.000000Z', 'c', 1)");
                driveRefreshToQuiescence(job);
                Assert.assertTrue("the late row must be repaired in", repairedRows(viewInstance()) > repairedBefore);
                commitAndRefresh(job, "('2026-01-01T00:59:00.000000Z', 'a', 5)");
                commitAndRefresh(job, "('2026-01-01T00:59:30.000000Z', 'a', 6)");
                commitAndRefresh(job, "('2026-01-01T01:59:50.000000Z', 'a', 7)");
                commitAndRefresh(job, "('2026-01-01T02:00:10.000000Z', 'a', 8)");
                driveRefreshToQuiescence(job);
                Assert.assertEquals(ts("2026-01-01T01:59:50.000000Z"), viewInstance().getHeadCheckpointMaxTs());
                assertViewRowPastLiveFilePrefix("lv", "2026-01-01T01:59:50.000000Z");
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
                    2026-01-01T00:00:05.000000Z\tc\t1.0
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
                    LiveViewCheckpointRecoveryPhase.routeName(viewInstance().getCheckpointRestoreRoute())
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
                    LiveViewCheckpointRecoveryPhase.routeName(viewInstance().getCheckpointRestoreRoute())
            );
            assertViewRows(HIGHER_FRONTIER_SEEDED_ROWS);
        });
    }

    @Test
    public void testDepartureCrashAheadOfTheFrontierTruncateRebuildsWhenTheSpliceKeptARootAboveARow() throws Exception {
        // A crash between the marker the frontier truncate writes and its publication leaves the
        // splice's generation under a marker recording that generation and the view's seqTxn,
        // neither of which moved: the marker reads as stale and the restart restores from the
        // splice. The splice kept the departed row's root above the row that became the frontier,
        // holding the state at that row, so the restore fails its row count and rebuilds. The base
        // lost nothing here, and the rebuild gives what a recompute gives.
        final DepartureMarkerCrash crash = new DepartureMarkerCrash();
        assertMemoryLeak(crash, () -> {
            crashAheadOfTheFrontierTruncateAndRestart(crash, false, false);
            assertRebuiltFromAppliedBase("lv");
            Assert.assertFalse(viewInstance().isCheckpointRecoveryBlocked());
            assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
            restartAndRefresh();
            assertRestoredFromTimeline("lv");
            assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
        });
    }

    @Test
    public void testDepartureCrashAheadOfTheFrontierTruncateRestoresWhenTheSpliceKeptTheFrontierRoot() throws Exception {
        // The same crash after a departure whose resume replayed nothing: the splice kept the root
        // of the row that became the frontier, so the stale marker's restart restores from the
        // splice, over a base that lost the day the view keeps.
        final DepartureMarkerCrash crash = new DepartureMarkerCrash();
        assertMemoryLeak(crash, () -> {
            crashAheadOfTheFrontierTruncateAndRestart(crash, true, true);
            assertRestoredFromTimeline("lv");
            assertViewRows(DEPARTURE_ROWS_OVER_THE_LOST_DAY, DEPARTURE_ROWS_QUERY);
            assertNoRefreshFaults("lv");
            restartAndRefresh();
            assertRestoredFromTimeline("lv");
            assertViewRows(DEPARTURE_ROWS_OVER_THE_LOST_DAY, DEPARTURE_ROWS_QUERY);
        });
    }

    @Test
    public void testDepartureCrashAheadOfTheFrontierTruncateRestoresWhenTheSpliceReVersionedTheFrontierRoot() throws Exception {
        // The same crash after a late row and a departure in one commit, whose replay stopped on a
        // root the splice re-versioned at the new frontier.
        final DepartureMarkerCrash crash = new DepartureMarkerCrash();
        assertMemoryLeak(crash, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTheDayTheBaseLoses(job);
                commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'a', 1.5)");
                commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', 2.5)");
                commitDeparture(job, "('2026-01-01T00:03:00.000000Z', 'a', 4.5)");
                crashAtTheFrontierTruncateMarker(
                        crash,
                        job,
                        "('2026-01-01T00:01:30.000000Z', 'a', 3.0), ('2026-01-01T00:03:00.000000Z', 'a', -4.5)"
                );
            }
            restoreTheCrashImageAndRestart(crash);
            assertRestoredFromTimeline("lv");
            final String rows = """
                    ts\tsym\tx\ts
                    2025-12-31T12:00:00.000000Z\ta\t0.5\t0.5
                    2026-01-01T00:01:00.000000Z\ta\t1.5\t2.0
                    2026-01-01T00:01:30.000000Z\ta\t3.0\t5.0
                    2026-01-01T00:02:00.000000Z\ta\t2.5\t7.0
                    """;
            assertViewRows(rows, DEPARTURE_ROWS_QUERY);
            assertNoRefreshFaults("lv");
            restartAndRefresh();
            assertRestoredFromTimeline("lv");
            assertViewRows(rows, DEPARTURE_ROWS_QUERY);
        });
    }

    @Test
    public void testDepartureCrashBetweenTheFrontierTruncateAndItsSealRebuildsOnRestart() throws Exception {
        // A crash after the truncate that drops the roots above the new frontier and before the
        // seal there leaves a generation whose newest root sits below the frontier, under the
        // marker the truncate wrote. The marker reads as live, so the restart rebuilds from the
        // applied base rather than restore from that generation.
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(() -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'a', 1.5)");
                    commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', 2.5), ('2026-01-01T00:03:00.000000Z', 'a', 9.5)");
                    final LiveViewInstance instance = viewInstance();
                    final long generationBefore = generation(instance);
                    final AtomicBoolean hasCrashed = new AtomicBoolean();
                    hooks(job).setSimulateResumeHeadSealStart(() -> {
                        hasCrashed.set(true);
                        throw CairoException.critical(0).put("simulated crash ahead of the frontier seal");
                    });
                    execute("INSERT INTO base VALUES ('2026-01-01T00:03:00.000000Z', 'a', -9.5)");
                    drainWalQueue();
                    for (int pass = 0; pass < REFRESH_QUIESCENCE_PASSES && !hasCrashed.get(); pass++) {
                        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                        drainWalQueue();
                        job.processNotificationsForTest();
                    }
                    Assert.assertTrue("the resume must reach its head seal", hasCrashed.get());
                    Assert.assertEquals("the splice and the truncate", generationBefore + 2, generation(instance));
                    assertCheckpointLadder(instance, false, """
                            2026-01-01T00:01:00.000000Z -> 1
                            """);
                    assertMarkerNames(generationBefore + 1);
                }
                restartAndRefresh();
                capture.drain();
                capture.assertLogged("live view restart rebuilding from applied base [view=lv, cause=prefix preservation repair marker present");
                assertRebuiltFromAppliedBase("lv");
                assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testDepartureFrontierSealThatFailsOverALossyBaseBlocksTheRestart() throws Exception {
        // The truncate's own rule for a seal that does not land: the truncated generation's
        // newest root sits below the frontier, so the timeline retires. A restart then has nothing
        // to restore from, and over a base that lost a day the view keeps, the restatement guard
        // refuses the rebuild.
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            failTheDepartureFrontierSeal(fault, true, false);
            restartAndRefresh();
            final LiveViewInstance instance = viewInstance();
            Assert.assertEquals(
                    "rebuild_blocked",
                    LiveViewCheckpointRecoveryPhase.routeName(instance.getCheckpointRestoreRoute())
            );
            Assert.assertTrue(instance.isCheckpointRecoveryBlocked());
            assertViewRows(singleDepartureRows(true), DEPARTURE_ROWS_QUERY);
        });
    }

    @Test
    public void testDepartureFrontierSealThatFailsRebuildsOnRestart() throws Exception {
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            failTheDepartureFrontierSeal(fault, false, false);
            restartAndRefresh();
            assertRebuiltFromAppliedBase("lv");
            Assert.assertFalse(viewInstance().isCheckpointRecoveryBlocked());
            assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
        });
    }

    @Test
    public void testDepartureFrontierSealThatFailsReopensTheTimelineOnTheNextRow() throws Exception {
        // The retired timeline reopens on the next row's seal, which a restart restores from
        // even over the base that lost a day.
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            failTheDepartureFrontierSeal(fault, true, true);
            final String rows = singleDepartureRows(true) + "2026-01-01T00:04:00.000000Z\ta\t8.0\t12.0\n";
            for (int restart = 0; restart < 2; restart++) {
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(rows, DEPARTURE_ROWS_QUERY);
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testDepartureFrontierSealsOverAPurgeAndAReadBetweenTheTruncateAndTheSeal() throws Exception {
        // Between the truncate and the seal at the new frontier the view's head is cleared, and
        // the generation on disk holds no root at the frontier. A reader there sees the repaired
        // rows and a WAL purge there runs, and the seal lands after both. The base WAL above the
        // departure, which a restart replays from, stays through a purge after the seal.
        setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 1);
        setProperty(PropertyKey.CAIRO_WAL_PURGE_INTERVAL, 0);
        assertMemoryLeak(() -> {
            createDepartureView();
            final StringSink rowsAtTheSeal = new StringSink();
            final AtomicBoolean hasPurged = new AtomicBoolean();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTheDayTheBaseLoses(job);
                commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'a', 1.5)");
                commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', 2.5), ('2026-01-01T00:03:00.000000Z', 'a', 9.5)");
                final LiveViewInstance instance = viewInstance();
                hooks(job).setSimulateResumeHeadSealStart(() -> {
                    Assert.assertEquals("the truncate clears the head", Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                    try {
                        printSql(DEPARTURE_ROWS_QUERY, rowsAtTheSeal);
                    } catch (SqlException e) {
                        throw new AssertionError(e);
                    }
                    try (WalPurgeJob purgeJob = new WalPurgeJob(engine)) {
                        purgeJob.drain(0);
                    }
                    hasPurged.set(true);
                });
                // seqTxn 5; the base's seqTxn 2 is the DROP PARTITION.
                commitDeparture(job, "('2026-01-01T00:03:00.000000Z', 'a', -9.5)");
                Assert.assertTrue("the purge must run between the truncate and the seal", hasPurged.get());
                Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT));
                assertCheckpointLadder(instance, false, """
                        2025-12-31T12:00:00.000000Z -> 1
                        2026-01-01T00:01:00.000000Z -> 2
                        2026-01-01T00:02:00.000000Z -> 3
                        """);
                Assert.assertEquals(5, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(5, instance.getCheckpointTimelineWalPurgeFloor());
            }
            TestUtils.assertEquals(singleDepartureRows(true), rowsAtTheSeal);
            // seqTxn 6, which the view consumes only after the restart.
            execute("INSERT INTO base VALUES ('2026-01-01T00:04:00.000000Z', 'a', 8.0)");
            drainWalQueue();
            releaseSavePointHolds("base");
            engine.releaseInactive();
            setCurrentMicros(currentMicros + 60_000_000L);
            try (WalPurgeJob purgeJob = new WalPurgeJob(engine)) {
                purgeJob.drain(0);
            }
            Assert.assertEquals("the purge must keep the segment above the departure", 1, countBaseWalSegments());
            restartAndRefresh();
            assertRestoredFromTimeline("lv");
            final String rows = singleDepartureRows(true) + "2026-01-01T00:04:00.000000Z\ta\t8.0\t12.0\n";
            assertViewRows(rows, DEPARTURE_ROWS_QUERY);
            assertNoRefreshFaults("lv");
        });
    }

    @Test
    public void testDepartureFrontierTruncateThatFailsToPublishOpensAFreshHistory() throws Exception {
        // The truncate fails after writing its metadata and before the superblock names it, so
        // the timeline retires and the seal at the new frontier opens a fresh history. The fault
        // is armed at the truncate's own marker, the turn's second, and disarmed ahead of the
        // seal, which would otherwise fail on it as well.
        final AtomicReference<LiveViewRefreshJob> faultedJob = new AtomicReference<>();
        final AtomicInteger markerRenames = new AtomicInteger();
        final TestFilesFacadeImpl ff = new TestFilesFacadeImpl() {
            @Override
            public int rename(LPSZ from, LPSZ to) {
                final int result = super.rename(from, to);
                final LiveViewRefreshJob job = faultedJob.get();
                if (job != null
                        && Utf8s.endsWithAscii(to, LiveViewCheckpointLayout.REPAIRING_MARKER_FILE_NAME)
                        && markerRenames.incrementAndGet() == 2) {
                    job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH);
                }
                return result;
            }
        };
        assertMemoryLeak(ff, () -> assertDepartureTruncateFaultOpensAFreshHistory(
                job -> {
                    faultedJob.set(job);
                    hooks(job).setSimulateResumeHeadSealStart(() -> job.setCheckpointTimelineTestFailureStage(0));
                },
                () -> {
                    faultedJob.set(null);
                    Assert.assertEquals("the splice's marker and the truncate's", 2, markerRenames.get());
                }
        ));
    }

    @Test
    public void testDepartureFrontierTruncateWithoutItsMarkerOpensAFreshHistory() throws Exception {
        // The truncate writes its marker before it publishes, the turn's second marker after the
        // splice's. One it cannot write retires the timeline, and the seal at the new frontier
        // opens a fresh history there.
        final AtomicBoolean isArmed = new AtomicBoolean();
        final AtomicInteger markerRenames = new AtomicInteger();
        final TestFilesFacadeImpl ff = new TestFilesFacadeImpl() {
            @Override
            public int rename(LPSZ from, LPSZ to) {
                if (isArmed.get()
                        && Utf8s.endsWithAscii(to, LiveViewCheckpointLayout.REPAIRING_MARKER_FILE_NAME)
                        && markerRenames.incrementAndGet() == 2) {
                    return Files.FILES_RENAME_ERR_OTHER;
                }
                return super.rename(from, to);
            }
        };
        assertMemoryLeak(ff, () -> assertDepartureTruncateFaultOpensAFreshHistory(
                job -> isArmed.set(true),
                () -> {
                    isArmed.set(false);
                    Assert.assertEquals("the splice's marker and the truncate's", 2, markerRenames.get());
                }
        ));
    }

    @Test
    public void testDepartureMarkerThatCannotBeClearedAfterAFrontierRestampIsLive() throws Exception {
        // A truncate that leaves its newest root at the frontier publishes one generation past
        // the splice its marker names, and the seal only re-stamps the head. A marker whose clear
        // failed reads as live there, so the restart rebuilds, which the restatement guard
        // refuses over the day the base lost.
        final LiveViewMarkerClearFault fault = new LiveViewMarkerClearFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTheDayTheBaseLoses(job);
                commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'a', 1.5)");
                commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', 2.5)");
                final long generationBefore = generation(viewInstance());
                fault.arm();
                commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', -2.5)");
                fault.disarm();
                Assert.assertEquals(1, fault.getRefusedClears());
                Assert.assertEquals("the splice and the truncate", generationBefore + 2, generation(viewInstance()));
                assertMarkerNames(generationBefore + 1);
            }
            restartAndRefresh();
            final LiveViewInstance instance = viewInstance();
            Assert.assertEquals(
                    "rebuild_blocked",
                    LiveViewCheckpointRecoveryPhase.routeName(instance.getCheckpointRestoreRoute())
            );
            Assert.assertTrue(instance.isCheckpointRecoveryBlocked());
            assertViewRows(DEPARTURE_ROWS_OVER_THE_LOST_DAY, DEPARTURE_ROWS_QUERY);
        });
    }

    @Test
    public void testDepartureMarkerThatCannotBeClearedAfterAFrontierSealIsStale() throws Exception {
        // The frontier seal publishes two generations past the splice its truncate's marker
        // names, so a marker whose clear failed reads as stale and the restart restores.
        final LiveViewMarkerClearFault fault = new LiveViewMarkerClearFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTheDayTheBaseLoses(job);
                commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'a', 1.5)");
                commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', 2.5), ('2026-01-01T00:03:00.000000Z', 'a', 9.5)");
                final long generationBefore = generation(viewInstance());
                fault.arm();
                commitDeparture(job, "('2026-01-01T00:03:00.000000Z', 'a', -9.5)");
                fault.disarm();
                Assert.assertEquals(1, fault.getRefusedClears());
                Assert.assertEquals("the splice, the truncate and the seal", generationBefore + 3, generation(viewInstance()));
                assertMarkerNames(generationBefore + 1);
            }
            restartAndRefresh();
            assertRestoredFromTimeline("lv");
            assertViewRows(singleDepartureRows(true), DEPARTURE_ROWS_QUERY);
            try (Path dir = checkpointsDir(viewInstance())) {
                Assert.assertFalse(
                        "the restart must clear the stale marker",
                        LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir)
                );
            }
            restartAndRefresh();
            assertRestoredFromTimeline("lv");
            assertViewRows(singleDepartureRows(true), DEPARTURE_ROWS_QUERY);
        });
    }

    @Test
    public void testDepartureRestampThatTheSealCooldownDeclinesKeepsTheTruncatedTimeline() throws Exception {
        // The truncate leaves its newest root at the new frontier, so the seal there would only
        // re-stamp the head, and a seal on cooldown declines even that. The truncated generation
        // is restorable without it: its newest root sits at the frontier, so the marker clears
        // and the timeline stays, and each restart restores from it over the day the base lost.
        assertMemoryLeak(() -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTheDayTheBaseLoses(job);
                commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'a', 1.5)");
                commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', 2.5)");
                final LiveViewInstance instance = viewInstance();
                final long generationBefore = generation(instance);
                final long resetsBefore = instance.getCheckpointTimelineResets();
                final long sealFailuresBefore = instance.getCheckpointSealFailures();
                final AtomicBoolean hasReachedTheSeal = new AtomicBoolean();
                // A cooldown that ends one microsecond on, so it holds for this seal alone.
                hooks(job).setSimulateResumeHeadSealStart(() -> {
                    instance.setSealCooldownUntilUs(currentMicros + 1);
                    hasReachedTheSeal.set(true);
                });
                commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', -2.5)");
                Assert.assertTrue("the resume must reach its head seal", hasReachedTheSeal.get());
                Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT));
                Assert.assertEquals("the splice and the truncate, and no seal", generationBefore + 2, generation(instance));
                Assert.assertEquals("the declined seal is not a failed one", sealFailuresBefore, instance.getCheckpointSealFailures());
                Assert.assertEquals("the truncated timeline stays", resetsBefore, instance.getCheckpointTimelineResets());
                Assert.assertEquals("the declined re-stamp leaves the head the truncate cleared", Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                assertCheckpointLadder(instance, false, """
                        2025-12-31T12:00:00.000000Z -> 1
                        2026-01-01T00:01:00.000000Z -> 2
                        """);
                try (Path dir = checkpointsDir(instance)) {
                    Assert.assertFalse(
                            "a truncate whose newest root sits at the frontier resolves its marker",
                            LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir)
                    );
                }
            }
            assertViewRows(DEPARTURE_ROWS_OVER_THE_LOST_DAY, DEPARTURE_ROWS_QUERY);
            assertNoRefreshFaults("lv");
            for (int restart = 0; restart < 2; restart++) {
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                Assert.assertFalse(viewInstance().isCheckpointRecoveryBlocked());
                assertViewRows(DEPARTURE_ROWS_OVER_THE_LOST_DAY, DEPARTURE_ROWS_QUERY);
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testDepartureRestampThatTheSealCooldownDeclinesHoldsBaseWalUntilARestart() throws Exception {
        // The truncated timeline of the case above keeps its newest root at the new frontier
        // 00:01, with the head clear and the newest timestamp the view has seen still at the
        // departed 00:02. Each cycle without rows then declines a first root there, above the
        // newest row the view holds, and holds the base WAL where the repair left it. The restart
        // restores from the root at the frontier, which puts the head back on it, and the cycles
        // after it move its base coordinate again.
        setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 1);
        setProperty(PropertyKey.CAIRO_WAL_PURGE_INTERVAL, 0);
        assertMemoryLeak(() -> {
            createDepartureView();
            final String ladder = """
                    2025-12-31T12:00:00.000000Z -> 1
                    2026-01-01T00:01:00.000000Z -> 2
                    """;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTheDayTheBaseLoses(job);
                commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'a', 1.5)");
                commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', 2.5)");
                final LiveViewInstance instance = viewInstance();
                final long sealFailuresBefore = instance.getCheckpointSealFailures();
                hooks(job).setSimulateResumeHeadSealStart(() -> instance.setSealCooldownUntilUs(currentMicros + 1));
                commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', -2.5)");
                hooks(job).setSimulateResumeHeadSealStart(null);
                Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT));
                Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(MicrosTimestampDriver.floor("2026-01-01T00:02:00.000000Z"), instance.getLatestSeenTs());
                assertCheckpointLadder(instance, false, ladder);
                final long floorAfterTheRepair = instance.getCheckpointTimelineWalPurgeFloor();
                for (int minute = 3; minute <= 5; minute++) {
                    setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                    commitDeparture(job, "('2026-01-01T00:0" + minute + ":00.000000Z', 'a', -1.0)");
                    Assert.assertEquals(
                            "one decline per cycle",
                            minute - 2,
                            job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT)
                    );
                }
                Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                assertCheckpointLadder(instance, false, ladder);
                Assert.assertEquals("the declines are no failed seals", sealFailuresBefore, instance.getCheckpointSealFailures());
                Assert.assertEquals(floorAfterTheRepair, instance.getCheckpointTimelineWalPurgeFloor());
                Assert.assertEquals(8, instance.getStateReader().getLvConsumedSeqTxn());
                Assert.assertEquals(
                        "the purge must keep the segments above the floor the repair left",
                        8 - floorAfterTheRepair,
                        purgeBaseWalAndCountSegments()
                );
            }
            assertViewRows(DEPARTURE_ROWS_OVER_THE_LOST_DAY, DEPARTURE_ROWS_QUERY);
            assertNoRefreshFaults("lv");

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // seqTxn 9 to 11: the first publishes at once, for a head this process restored
                // rather than wrote, and each one after it is past the cadence.
                for (int minute = 6; minute <= 8; minute++) {
                    setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                    commitDeparture(job, "('2026-01-01T00:0" + minute + ":00.000000Z', 'a', -1.0)");
                    Assert.assertEquals(minute + 3, viewInstance().getHeadCheckpointBaseSeqTxn());
                }
                assertRestoredFromTimeline("lv");
                final LiveViewInstance instance = viewInstance();
                Assert.assertEquals(MicrosTimestampDriver.floor("2026-01-01T00:01:00.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(0, instance.getCheckpointSealFailures());
                Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT));
                assertCheckpointLadder(instance, false, ladder);
                Assert.assertEquals(10, instance.getCheckpointTimelineWalPurgeFloor());
                Assert.assertEquals("the purge must keep only the segments above the floor", 11 - 10, purgeBaseWalAndCountSegments());
            }
            restartAndRefresh();
            assertRestoredFromTimeline("lv");
            Assert.assertFalse(viewInstance().isCheckpointRecoveryBlocked());
            assertViewRows(DEPARTURE_ROWS_OVER_THE_LOST_DAY, DEPARTURE_ROWS_QUERY);
            assertNoRefreshFaults("lv");
        });
    }

    @Test
    public void testDepartureThatEmptiesTheViewRetiresItsTimeline() throws Exception {
        // A root that holds no row of the view: the head-miss rebuild that deletes the view's
        // first row re-versions that row's root at position 0 and the root above it at 1. The
        // departure of the row above it then resumes from the rowless root, replays nothing and
        // leaves the view empty, with no frontier to seal at. The truncate keeps no root and
        // retires the timeline, and a restart has nothing to restore: it comes back with no rows,
        // still refreshing.
        assertMemoryLeak(() -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'a', 1.5)");
                commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'a', 2.5)");
                final LiveViewInstance instance = viewInstance();
                assertCheckpointLadder(instance, false, """
                        2026-01-01T00:01:00.000000Z -> 1
                        2026-01-01T00:05:00.000000Z -> 2
                        """);
                replaceBaseRange(
                        MicrosTimestampDriver.floor("2026-01-01T00:01:00.000000Z"),
                        MicrosTimestampDriver.floor("2026-01-01T00:01:00.000001Z"),
                        Double.NaN
                );
                drainWalQueue();
                driveRefreshToQuiescence(job);
                assertCheckpointLadder(instance, false, """
                        2026-01-01T00:01:00.000000Z -> 0
                        2026-01-01T00:05:00.000000Z -> 1
                        """);
                Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT));
                final long resetsBefore = instance.getCheckpointTimelineResets();
                final long resumesBefore = job.getStatsForTest().getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RESUME_FROM_ANCHOR);
                final long headMissRepairsBefore = countHeadMissRepairs(job);
                commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'a', -2.5)");
                Assert.assertEquals(
                        "the resume from the rowless root repairs the departure",
                        resumesBefore + 1,
                        job.getStatsForTest().getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RESUME_FROM_ANCHOR)
                );
                Assert.assertEquals(headMissRepairsBefore, countHeadMissRepairs(job));
                Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT));
                Assert.assertEquals(resetsBefore + 1, instance.getCheckpointTimelineResets());
                try (Path dir = checkpointsDir(instance); Path timelinePath = new Path()) {
                    LiveViewCheckpointLayout.timelinePath(timelinePath, dir);
                    Assert.assertFalse("an empty view keeps no timeline", configuration.getFilesFacade().exists(timelinePath.$()));
                    Assert.assertFalse(LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir));
                }
            }
            final String noRows = "ts\tsym\tx\ts\n";
            assertViewRows(noRows, DEPARTURE_ROWS_QUERY);
            restartAndRefresh();
            final LiveViewInstance instance = viewInstance();
            Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
            Assert.assertFalse(instance.isInvalid());
            assertViewRows(noRows, DEPARTURE_ROWS_QUERY);
            assertNoRefreshFaults("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                commitDeparture(job, "('2026-01-01T00:06:00.000000Z', 'a', 4.0)");
            }
            assertViewRows(noRows + "2026-01-01T00:06:00.000000Z\ta\t4.0\t4.0\n", DEPARTURE_ROWS_QUERY);
            assertViewMatchesRecompute(HEAD_MISS_DEPARTURE_VIEW_SQL);
        });
    }

    @Test
    public void testHeadMissDepartureCrashAheadOfTheFrontierTruncateRebuildsWhenTheSpliceKeptARootAboveTheRow() throws Exception {
        // A crash between the marker the head-miss rebuild's frontier truncate writes and the
        // truncate's publication leaves the splice's generation under a marker recording that
        // generation and the view's seqTxn, neither of which moved: the marker reads as stale and
        // the restart restores from the splice. The splice kept the departed rows' root above the
        // kept row, so the restore fails its row count and rebuilds. The base lost nothing here,
        // and the rebuild gives what a recompute gives.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final DepartureMarkerCrash crash = new DepartureMarkerCrash();
        assertMemoryLeak(crash, () -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitHeadMissDepartureFixture(job, false, 59);
                crashAtTheHeadMissFrontierTruncateMarker(crash, job, HEAD_MISS_KEPT_ROW_DEPARTURE);
            }
            restoreTheCrashImageAndRestart(crash);
            assertRebuiltFromAppliedBase("lv");
            Assert.assertFalse(viewInstance().isCheckpointRecoveryBlocked());
            assertViewRows(HEAD_MISS_KEPT_ROW_DEPARTURE_ROWS, DEPARTURE_ROWS_QUERY);
            assertViewMatchesRecompute(HEAD_MISS_DEPARTURE_VIEW_SQL);
            restartAndRefresh();
            assertRestoredFromTimeline("lv");
            assertViewMatchesRecompute(HEAD_MISS_DEPARTURE_VIEW_SQL);
        });
    }

    @Test
    public void testHeadMissDepartureCrashAheadOfTheFrontierTruncateRestoresWhenTheRootAtTheFrontierCountsEveryRow() throws Exception {
        // The same crash after a departure that emitted nothing above a root at the row that
        // became the frontier, which counts every row the view holds: the stale marker's restart
        // restores from the splice's generation, floored at that root, over the day the base lost.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final DepartureMarkerCrash crash = new DepartureMarkerCrash();
        assertMemoryLeak(crash, () -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitHeadMissRootAtTheFrontierThenRowsAboveIt(job, true, true);
                crashAtTheHeadMissFrontierTruncateMarker(crash, job, "('2026-01-01T00:59:00.000000Z', 'a', -1.0)");
            }
            restoreTheCrashImageAndRestart(crash);
            final String rows = headMissDepartureRows(true, 56);
            assertRestoredFromTimeline("lv");
            assertViewRows(rows, DEPARTURE_ROWS_QUERY);
            assertNoRefreshFaults("lv");
            restartAndRefresh();
            assertRestoredFromTimeline("lv");
            assertViewRows(rows, DEPARTURE_ROWS_QUERY);
        });
    }

    @Test
    public void testHeadMissDepartureCrashBetweenTheFrontierTruncateAndItsSealRebuildsOnRestart() throws Exception {
        // A crash after the truncate that drops the roots above the new frontier and before the
        // seal there leaves a generation whose newest root sits below the frontier, under the
        // marker the truncate wrote. The marker reads as live, so the restart rebuilds from the
        // applied base rather than restore from that generation.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(() -> {
                createHeadMissDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    commitHeadMissDepartureFixture(job, false, 59);
                    final LiveViewInstance instance = viewInstance();
                    final long generationBefore = generation(instance);
                    final AtomicBoolean hasCrashed = new AtomicBoolean();
                    hooks(job).setSimulateHeadMissHeadSealStart(() -> {
                        hasCrashed.set(true);
                        throw CairoException.critical(0).put("simulated crash ahead of the frontier seal");
                    });
                    execute("INSERT INTO base VALUES " + HEAD_MISS_KEPT_ROW_DEPARTURE);
                    drainWalQueue();
                    for (int pass = 0; pass < REFRESH_QUIESCENCE_PASSES && !hasCrashed.get(); pass++) {
                        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                        drainWalQueue();
                        job.processNotificationsForTest();
                    }
                    Assert.assertTrue("the head-miss rebuild must reach its head seal", hasCrashed.get());
                    Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_FRONTIER_TRUNCATE_COUNT));
                    Assert.assertEquals("the splice and the truncate", generationBefore + 2, generation(instance));
                    assertCheckpointLadder(instance, false, "2025-12-31T23:55:00.000000Z -> 1\n");
                    assertMarkerNames(generationBefore + 1);
                }
                restartAndRefresh();
                capture.drain();
                capture.assertLogged("live view restart rebuilding from applied base [view=lv, cause=prefix preservation repair marker present");
                assertRebuiltFromAppliedBase("lv");
                assertViewMatchesRecompute(HEAD_MISS_DEPARTURE_VIEW_SQL);
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewMatchesRecompute(HEAD_MISS_DEPARTURE_VIEW_SQL);
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testHeadMissDepartureFrontierLookupThatFailsRetiresTheTimeline() throws Exception {
        // The departure emits nothing above a root at the row that becomes the frontier, which
        // would keep the timeline, but the read of that root fails: a root the repair cannot vet
        // is no root to keep a generation over, so it retires the timeline. Over a base that lost
        // nothing the restart rebuilds what a recompute gives.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final AtomicBoolean isArmed = new AtomicBoolean();
        final AtomicBoolean isConsumedSeqTxnAdvanced = new AtomicBoolean();
        final AtomicInteger refusedReads = new AtomicInteger();
        // Once armed, refuses the first open of the timeline's superblock after the repair writes
        // the view's state file to advance its consumed seqTxn past the splice it published: the
        // lookup of the root at the frontier, which runs next.
        final TestFilesFacadeImpl ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (isArmed.get() && Utf8s.endsWithAscii(name, LiveViewState.LIVE_VIEW_STATE_FILE_NAME)) {
                    isArmed.set(false);
                    isConsumedSeqTxnAdvanced.set(true);
                } else if (isConsumedSeqTxnAdvanced.get() && Utf8s.endsWithAscii(name, LiveViewCheckpointLayout.TIMELINE_FILE_NAME)) {
                    isConsumedSeqTxnAdvanced.set(false);
                    refusedReads.incrementAndGet();
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(ff, () -> {
                createHeadMissDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    commitHeadMissRootAtTheFrontierThenRowsAboveIt(job, false, false);
                    final LiveViewInstance instance = viewInstance();
                    final long resetsBefore = instance.getCheckpointTimelineResets();
                    isArmed.set(true);
                    commitDeparture(job, "('2026-01-01T00:59:00.000000Z', 'a', -1.0)");
                    Assert.assertEquals(1, refusedReads.get());
                    capture.drain();
                    capture.assertLogged("could not read the live view checkpoint root at the frontier [view=lv");
                    Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_ZERO_ROW_RETIRE_COUNT));
                    Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_ZERO_ROW_KEEP_COUNT));
                    Assert.assertEquals("the repair retires the timeline", resetsBefore + 1, instance.getCheckpointTimelineResets());
                    try (Path dir = checkpointsDir(instance); Path timelinePath = new Path()) {
                        LiveViewCheckpointLayout.timelinePath(timelinePath, dir);
                        Assert.assertFalse(configuration.getFilesFacade().exists(timelinePath.$()));
                        Assert.assertFalse(LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir));
                    }
                }
                restartAndRefresh();
                assertRebuiltFromAppliedBase("lv");
                assertViewRows(headMissDepartureRows(true, 56), DEPARTURE_ROWS_QUERY);
                assertViewMatchesRecompute(HEAD_MISS_DEPARTURE_VIEW_SQL);
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testHeadMissDepartureFrontierSealThatFailsRetiresTheTimeline() throws Exception {
        // The truncate kept only the root below the kept row, so the frontier is owed a root of
        // its own, and the seal that would put it there fails on its data segment: the truncate's
        // rule retires the timeline. Over a base that lost nothing the restart rebuilds what a
        // recompute gives, and the next row's seal opens a timeline the restart after it restores.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitHeadMissDepartureFixture(job, false, 59);
                final LiveViewInstance instance = viewInstance();
                final long resetsBefore = instance.getCheckpointTimelineResets();
                final long sealFailuresBefore = instance.getCheckpointSealFailures();
                hooks(job).setSimulateHeadMissHeadSealStart(
                        () -> fault.arm(LiveViewCheckpointDiskFullFault.OP_TRUNCATE, LiveViewCheckpointLayout.DATA_DIR_NAME, 1)
                );
                commitDeparture(job, HEAD_MISS_KEPT_ROW_DEPARTURE);
                fault.disarm();
                Assert.assertNotNull("the seal at the new frontier must reach its data segment", fault.getFailedFile());
                Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_FRONTIER_TRUNCATE_COUNT));
                Assert.assertEquals(sealFailuresBefore + 1, instance.getCheckpointSealFailures());
                Assert.assertEquals(
                        "the truncated timeline goes with the seal that did not land",
                        resetsBefore + 1,
                        instance.getCheckpointTimelineResets()
                );
                try (Path dir = checkpointsDir(instance); Path timelinePath = new Path()) {
                    LiveViewCheckpointLayout.timelinePath(timelinePath, dir);
                    Assert.assertFalse(configuration.getFilesFacade().exists(timelinePath.$()));
                    Assert.assertFalse(LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir));
                }
            }
            assertViewRows(HEAD_MISS_KEPT_ROW_DEPARTURE_ROWS, DEPARTURE_ROWS_QUERY);
            assertNoRefreshFaults("lv");
            restartAndRefresh();
            assertRebuiltFromAppliedBase("lv");
            Assert.assertFalse(viewInstance().isCheckpointRecoveryBlocked());
            assertViewMatchesRecompute(HEAD_MISS_DEPARTURE_VIEW_SQL);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                commitDeparture(job, "('2026-01-01T01:05:00.000000Z', 'a', 4.0)");
            }
            restartAndRefresh();
            assertRestoredFromTimeline("lv");
            assertViewRows(HEAD_MISS_KEPT_ROW_DEPARTURE_ROWS + "2026-01-01T01:05:00.000000Z\ta\t4.0\t4.0\n", DEPARTURE_ROWS_QUERY);
            assertViewMatchesRecompute(HEAD_MISS_DEPARTURE_VIEW_SQL);
        });
    }

    @Test
    public void testHeadMissDepartureFrontierSealsOverAPurgeAndAReadBetweenTheTruncateAndTheSeal() throws Exception {
        // Between the frontier truncate and the seal at the new frontier the view's head is
        // cleared, and the generation on disk holds no root at the frontier. A reader there sees
        // the repaired rows and a WAL purge there runs, and the seal lands after both, standing on
        // the departure's commit. A restart restores from it over the day the base lost.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        setProperty(PropertyKey.CAIRO_WAL_PURGE_INTERVAL, 0);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView();
            final StringSink rowsAtTheSeal = new StringSink();
            final AtomicBoolean hasPurged = new AtomicBoolean();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitHeadMissDepartureFixture(job, true, 59);
                final LiveViewInstance instance = viewInstance();
                hooks(job).setSimulateHeadMissHeadSealStart(() -> {
                    Assert.assertEquals("the truncate clears the head", Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                    try {
                        printSql(DEPARTURE_ROWS_QUERY, rowsAtTheSeal);
                    } catch (SqlException e) {
                        throw new AssertionError(e);
                    }
                    try (WalPurgeJob purgeJob = new WalPurgeJob(engine)) {
                        purgeJob.drain(0);
                    }
                    hasPurged.set(true);
                });
                commitDeparture(job, HEAD_MISS_KEPT_ROW_DEPARTURE);
                Assert.assertTrue("the purge must run between the truncate and the seal", hasPurged.get());
                Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_FRONTIER_TRUNCATE_COUNT));
                assertCheckpointLadder(instance, false, """
                        2025-12-31T23:55:00.000000Z -> 1
                        2026-01-01T00:57:00.000000Z -> 59
                        """);
                Assert.assertEquals(ts("2026-01-01T00:57:00.000000Z"), instance.getHeadCheckpointMaxTs());
                // The base's seqTxns: the 23:55 row, the DROP PARTITION, the two batches and the
                // departure.
                Assert.assertEquals(5, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(5, instance.getCheckpointTimelineWalPurgeFloor());
            }
            TestUtils.assertEquals(HEAD_MISS_KEPT_ROW_DEPARTURE_ROWS, rowsAtTheSeal);
            restartAndRefresh();
            assertRestoredFromTimeline("lv");
            assertViewRows(HEAD_MISS_KEPT_ROW_DEPARTURE_ROWS, DEPARTURE_ROWS_QUERY);
            assertNoRefreshFaults("lv");
        });
    }

    @Test
    public void testHeadMissDepartureSpliceThatFailsToPublishRetiresTheTimeline() throws Exception {
        // The splice of the kept-row departure fails after writing its metadata and before the
        // superblock names it: the output moved under every root it re-versioned, so the timeline
        // retires on the way out, with the seal the repair put on it. Over a base that lost nothing
        // the restart rebuilds what a recompute gives.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final AtomicReference<LiveViewRefreshJob> faultedJob = new AtomicReference<>();
        final AtomicInteger markerRenames = new AtomicInteger();
        final TestFilesFacadeImpl ff = new TestFilesFacadeImpl() {
            @Override
            public int rename(LPSZ from, LPSZ to) {
                final int result = super.rename(from, to);
                final LiveViewRefreshJob job = faultedJob.get();
                if (job != null
                        && Utf8s.endsWithAscii(to, LiveViewCheckpointLayout.REPAIRING_MARKER_FILE_NAME)
                        && markerRenames.incrementAndGet() == 1) {
                    job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH);
                }
                return result;
            }
        };
        assertMemoryLeak(ff, () -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitHeadMissDepartureFixture(job, false, 59);
                final LiveViewInstance instance = viewInstance();
                final long resetsBefore = instance.getCheckpointTimelineResets();
                final long repairFailuresBefore = instance.getCheckpointRepairFailures();
                faultedJob.set(job);
                hooks(job).setSimulateHeadMissHeadSealStart(() -> job.setCheckpointTimelineTestFailureStage(0));
                commitDeparture(job, HEAD_MISS_KEPT_ROW_DEPARTURE);
                faultedJob.set(null);
                Assert.assertEquals("the splice's marker alone", 1, markerRenames.get());
                Assert.assertEquals(repairFailuresBefore + 1, instance.getCheckpointRepairFailures());
                Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_FRONTIER_TRUNCATE_COUNT));
                Assert.assertEquals("the unpublished splice retires the timeline", resetsBefore + 1, instance.getCheckpointTimelineResets());
                try (Path dir = checkpointsDir(instance); Path timelinePath = new Path()) {
                    LiveViewCheckpointLayout.timelinePath(timelinePath, dir);
                    Assert.assertFalse(configuration.getFilesFacade().exists(timelinePath.$()));
                    Assert.assertFalse(LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir));
                }
            }
            assertViewRows(HEAD_MISS_KEPT_ROW_DEPARTURE_ROWS, DEPARTURE_ROWS_QUERY);
            assertNoRefreshFaults("lv");
            restartAndRefresh();
            assertRebuiltFromAppliedBase("lv");
            assertViewMatchesRecompute(HEAD_MISS_DEPARTURE_VIEW_SQL);
        });
    }

    @Test
    public void testHeadMissDepartureFrontierTruncateThatFailsToPublishOpensAFreshHistory() throws Exception {
        // The head-miss rebuild's frontier truncate fails after writing its metadata and before
        // the superblock names it, so the timeline retires and the seal at the new frontier opens
        // a fresh history. The fault is armed at the truncate's own marker, the turn's second, and
        // disarmed ahead of the seal, which would otherwise fail on it as well.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final AtomicReference<LiveViewRefreshJob> faultedJob = new AtomicReference<>();
        final AtomicInteger markerRenames = new AtomicInteger();
        final TestFilesFacadeImpl ff = new TestFilesFacadeImpl() {
            @Override
            public int rename(LPSZ from, LPSZ to) {
                final int result = super.rename(from, to);
                final LiveViewRefreshJob job = faultedJob.get();
                if (job != null
                        && Utf8s.endsWithAscii(to, LiveViewCheckpointLayout.REPAIRING_MARKER_FILE_NAME)
                        && markerRenames.incrementAndGet() == 2) {
                    job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH);
                }
                return result;
            }
        };
        assertMemoryLeak(ff, () -> assertHeadMissDepartureTruncateFaultOpensAFreshHistory(
                job -> {
                    faultedJob.set(job);
                    hooks(job).setSimulateHeadMissHeadSealStart(() -> job.setCheckpointTimelineTestFailureStage(0));
                },
                () -> {
                    faultedJob.set(null);
                    Assert.assertEquals("the splice's marker and the truncate's", 2, markerRenames.get());
                }
        ));
    }

    @Test
    public void testHeadMissDepartureFrontierTruncateWithoutItsMarkerAfterAZeroRowRepairLeavesNoHead() throws Exception {
        // A departure that emits nothing above a root at the row that becomes the frontier
        // truncates the root above it away, and the head goes back on the root at the frontier.
        // A truncate that cannot write its marker retires the timeline instead, which leaves no
        // root for the head to go back on: it stays clear, and each cycle without rows declines a
        // first root at the newest timestamp the view has seen, above its newest row, rather than
        // fail a publication over a timeline that is gone.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final AtomicBoolean isArmed = new AtomicBoolean();
        final AtomicInteger markerRenames = new AtomicInteger();
        final TestFilesFacadeImpl ff = new TestFilesFacadeImpl() {
            @Override
            public int rename(LPSZ from, LPSZ to) {
                if (isArmed.get()
                        && Utf8s.endsWithAscii(to, LiveViewCheckpointLayout.REPAIRING_MARKER_FILE_NAME)
                        && markerRenames.incrementAndGet() == 2) {
                    return Files.FILES_RENAME_ERR_OTHER;
                }
                return super.rename(from, to);
            }
        };
        assertMemoryLeak(ff, () -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitHeadMissRootAtTheFrontierThenRowsAboveIt(job, true, true);
                final LiveViewInstance instance = viewInstance();
                final long resetsBefore = instance.getCheckpointTimelineResets();
                isArmed.set(true);
                commitDeparture(job, "('2026-01-01T00:59:00.000000Z', 'a', -1.0)");
                isArmed.set(false);
                Assert.assertEquals("the splice's marker and the truncate's", 2, markerRenames.get());
                Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_FRONTIER_TRUNCATE_COUNT));
                Assert.assertEquals("the failed truncate retires the timeline", resetsBefore + 1, instance.getCheckpointTimelineResets());
                Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                for (int minute = 1; minute <= 3; minute++) {
                    setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                    commitDeparture(job, "('2026-01-01T01:0" + minute + ":00.000000Z', 'a', -1.0)");
                    Assert.assertEquals(
                            "one decline per cycle",
                            minute,
                            job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT)
                    );
                }
                Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(0, instance.getCheckpointSealFailures());
                Assert.assertFalse(instance.isSealOnCooldown(currentMicros));
                try (Path dir = checkpointsDir(instance); Path timelinePath = new Path()) {
                    LiveViewCheckpointLayout.timelinePath(timelinePath, dir);
                    Assert.assertFalse("the retired timeline stays gone", configuration.getFilesFacade().exists(timelinePath.$()));
                }
            }
            assertViewRows(headMissDepartureRows(true, 56), DEPARTURE_ROWS_QUERY);
            assertNoRefreshFaults("lv");
        });
    }

    @Test
    public void testHeadMissDepartureFrontierTruncateWithoutItsMarkerOpensAFreshHistory() throws Exception {
        // The head-miss rebuild's frontier truncate writes its marker before it publishes, the
        // turn's second marker after the splice's. One it cannot write retires the timeline, and
        // the seal at the new frontier opens a fresh history there.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final AtomicBoolean isArmed = new AtomicBoolean();
        final AtomicInteger markerRenames = new AtomicInteger();
        final TestFilesFacadeImpl ff = new TestFilesFacadeImpl() {
            @Override
            public int rename(LPSZ from, LPSZ to) {
                if (isArmed.get()
                        && Utf8s.endsWithAscii(to, LiveViewCheckpointLayout.REPAIRING_MARKER_FILE_NAME)
                        && markerRenames.incrementAndGet() == 2) {
                    return Files.FILES_RENAME_ERR_OTHER;
                }
                return super.rename(from, to);
            }
        };
        assertMemoryLeak(ff, () -> assertHeadMissDepartureTruncateFaultOpensAFreshHistory(
                job -> isArmed.set(true),
                () -> {
                    isArmed.set(false);
                    Assert.assertEquals("the splice's marker and the truncate's", 2, markerRenames.get());
                }
        ));
    }

    @Test
    public void testHeadMissDepartureMarkerThatCannotBeClearedAfterAFrontierSealIsStale() throws Exception {
        // The frontier seal publishes two generations past the splice its truncate's marker
        // names, so a marker whose clear failed reads as stale and the restart restores.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewMarkerClearFault fault = new LiveViewMarkerClearFault();
        assertMemoryLeak(fault, () -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitHeadMissDepartureFixture(job, true, 59);
                final long generationBefore = generation(viewInstance());
                fault.arm();
                commitDeparture(job, HEAD_MISS_KEPT_ROW_DEPARTURE);
                fault.disarm();
                Assert.assertEquals(1, fault.getRefusedClears());
                Assert.assertEquals("the splice, the truncate and the seal", generationBefore + 3, generation(viewInstance()));
                assertMarkerNames(generationBefore + 1);
            }
            for (int restart = 0; restart < 2; restart++) {
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(HEAD_MISS_KEPT_ROW_DEPARTURE_ROWS, DEPARTURE_ROWS_QUERY);
                try (Path dir = checkpointsDir(viewInstance())) {
                    Assert.assertFalse(
                            "the restart must clear the stale marker",
                            LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir)
                    );
                }
            }
        });
    }

    @Test
    public void testHeadMissDepartureMarkerThatCannotBeClearedOverAKeptFrontierRootIsLive() throws Exception {
        // A departure that emitted nothing above a root at the row that became the frontier
        // truncates the root above it away and keeps the timeline with no seal: one generation
        // past the splice the truncate's marker names. A marker whose clear failed reads as live
        // there, so the restart rebuilds, which the restatement guard refuses over the day the
        // base lost.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewMarkerClearFault fault = new LiveViewMarkerClearFault();
        assertMemoryLeak(fault, () -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitHeadMissRootAtTheFrontierThenRowsAboveIt(job, true, true);
                final long generationBefore = generation(viewInstance());
                fault.arm();
                commitDeparture(job, "('2026-01-01T00:59:00.000000Z', 'a', -1.0)");
                fault.disarm();
                Assert.assertEquals(1, fault.getRefusedClears());
                Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_FRONTIER_TRUNCATE_COUNT));
                Assert.assertEquals("the splice and the truncate", generationBefore + 2, generation(viewInstance()));
                assertMarkerNames(generationBefore + 1);
            }
            restartAndRefresh();
            final LiveViewInstance instance = viewInstance();
            Assert.assertEquals(
                    "rebuild_blocked",
                    LiveViewCheckpointRecoveryPhase.routeName(instance.getCheckpointRestoreRoute())
            );
            Assert.assertTrue(instance.isCheckpointRecoveryBlocked());
            assertViewRows(headMissDepartureRows(true, 56), DEPARTURE_ROWS_QUERY);
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
                Assert.assertNull(job.getCheckpointTimelinePurgeStateForTest());
                Assert.assertTrue(job.truncateOrRetireTimelineOnO3ForTest(
                        instance,
                        ts(timestamp(SEALS * 10 - 5))
                ));
                Assert.assertSame(
                        engine.getLiveViewCheckpointPurgeState(),
                        job.getCheckpointTimelinePurgeStateForTest()
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
        assertTruncateThatCannotWriteItsMarkerRetiresTheTimeline(
                ff,
                () -> isArmed.set(true),
                () -> isArmed.set(false),
                markerAttempts::get
        );
    }

    @Test
    public void testEofRepairRetiresTheTimelineWhenTheTruncateRunsOutOfDiskSpaceForItsMarker() throws Exception {
        // The same truncate on a full disk: the marker's staged file opens, and reserving the
        // blocks its body needs fails. The write leaves that empty staged file behind, and a
        // staged marker with no final name reads as a live repair, so the retire has to take it
        // with the timeline or every restart after it rebuilds.
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertTruncateThatCannotWriteItsMarkerRetiresTheTimeline(
                fault,
                () -> fault.arm(
                        LiveViewCheckpointDiskFullFault.OP_ALLOCATE,
                        LiveViewCheckpointLayout.REPAIRING_MARKER_FILE_NAME,
                        1
                ),
                fault::disarm,
                () -> fault.getFailedFile() != null ? 1 : 0
        );
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

    private static int countBaseWalSegments() {
        final File tableDir = new File(configuration.getDbRoot(), engine.verifyTableName("base").getDirName());
        final File[] walDirs = tableDir.listFiles(f -> f.isDirectory() && f.getName().startsWith(WalUtils.WAL_NAME_BASE));
        Assert.assertNotNull(walDirs);
        int segments = 0;
        for (File walDir : walDirs) {
            final File[] segmentDirs = walDir.listFiles(File::isDirectory);
            if (segmentDirs != null) {
                segments += segmentDirs.length;
            }
        }
        return segments;
    }

    private static Path crashImageDir() {
        return new Path().of(configuration.getDbRoot()).concat("departure_crash_image").slash();
    }

    // The rows the single-departure shape leaves: a root per commit, the newest of which sealed
    // two rows under the newer one's timestamp, and that row upserted out of the filter.
    private static String singleDepartureRows(boolean isDayLost) {
        return isDayLost
                ? """
                ts\tsym\tx\ts
                2025-12-31T12:00:00.000000Z\ta\t0.5\t0.5
                2026-01-01T00:01:00.000000Z\ta\t1.5\t2.0
                2026-01-01T00:02:00.000000Z\ta\t2.5\t4.5
                """
                : """
                ts\tsym\tx\ts
                2026-01-01T00:01:00.000000Z\ta\t1.5\t1.5
                2026-01-01T00:02:00.000000Z\ta\t2.5\t4.0
                """;
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
     * The newest of two roots above the day the base loses departs, with {@code arm} making the
     * frontier truncate fail: the timeline retires, and the seal at the new frontier opens a
     * fresh history there that each restart restores from.
     */
    private void assertDepartureTruncateFaultOpensAFreshHistory(Consumer<LiveViewRefreshJob> arm, Runnable disarm) throws Exception {
        createDepartureView();
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveSeedToCompletion(job, "lv");
            commitTheDayTheBaseLoses(job);
            commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'a', 1.5)");
            commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', 2.5)");
            final LiveViewInstance instance = viewInstance();
            final long resetsBefore = instance.getCheckpointTimelineResets();
            arm.accept(job);
            commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', -2.5)");
            disarm.run();
            Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT));
            Assert.assertEquals("the failed truncate retires the timeline", resetsBefore + 1, instance.getCheckpointTimelineResets());
            Assert.assertEquals("the seal at the new frontier opens a fresh history", 1, generation(instance));
            assertCheckpointLadder(instance, false, """
                    2026-01-01T00:01:00.000000Z -> 2
                    """);
            try (Path dir = checkpointsDir(instance)) {
                Assert.assertFalse("a retired timeline owes no marker", LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir));
            }
        }
        assertViewRows(DEPARTURE_ROWS_OVER_THE_LOST_DAY, DEPARTURE_ROWS_QUERY);
        assertNoRefreshFaults("lv");
        for (int restart = 0; restart < 2; restart++) {
            restartAndRefresh();
            assertRestoredFromTimeline("lv");
            assertViewRows(DEPARTURE_ROWS_OVER_THE_LOST_DAY, DEPARTURE_ROWS_QUERY);
            assertNoRefreshFaults("lv");
        }
    }

    /**
     * The head-miss departure fixture over the day the base loses, whose kept-row departure
     * runs with {@code arm} making the frontier truncate fail: the timeline retires, and the
     * seal at the new frontier opens a fresh history there that each restart restores from.
     */
    private void assertHeadMissDepartureTruncateFaultOpensAFreshHistory(Consumer<LiveViewRefreshJob> arm, Runnable disarm) throws Exception {
        createHeadMissDepartureView();
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveSeedToCompletion(job, "lv");
            commitHeadMissDepartureFixture(job, true, 59);
            final LiveViewInstance instance = viewInstance();
            final long resetsBefore = instance.getCheckpointTimelineResets();
            arm.accept(job);
            commitDeparture(job, HEAD_MISS_KEPT_ROW_DEPARTURE);
            disarm.run();
            Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_FRONTIER_TRUNCATE_COUNT));
            Assert.assertEquals("the failed truncate retires the timeline", resetsBefore + 1, instance.getCheckpointTimelineResets());
            Assert.assertEquals("the seal at the new frontier opens a fresh history", 1, generation(instance));
            assertCheckpointLadder(instance, false, "2026-01-01T00:57:00.000000Z -> 59\n");
            try (Path dir = checkpointsDir(instance)) {
                Assert.assertFalse("a retired timeline owes no marker", LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir));
            }
        }
        assertViewRows(HEAD_MISS_KEPT_ROW_DEPARTURE_ROWS, DEPARTURE_ROWS_QUERY);
        assertNoRefreshFaults("lv");
        for (int restart = 0; restart < 2; restart++) {
            restartAndRefresh();
            assertRestoredFromTimeline("lv");
            assertViewRows(HEAD_MISS_KEPT_ROW_DEPARTURE_ROWS, DEPARTURE_ROWS_QUERY);
            assertNoRefreshFaults("lv");
        }
    }

    // The marker on disk is the one the frontier truncate wrote over the splice's generation.
    private void assertMarkerNames(long baseGeneration) {
        try (Path dir = checkpointsDir(viewInstance())) {
            Assert.assertTrue("the marker must be on disk", LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir));
            Assert.assertEquals(
                    "the frontier truncate's marker names the splice's generation",
                    baseGeneration,
                    LiveViewCheckpointRepairMarker.readBaseGeneration(configuration, dir)
            );
        }
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

    /**
     * Runs the EOF correction whose repair the chain budget sends to the truncate, with
     * {@code arm} making the truncate's marker write fail on {@code ff}, and asserts the
     * truncate retires the whole timeline ahead of its commit and the repair's post-replay seal
     * opens a fresh history that a restart restores from. {@code markerAttempts} reports how
     * many marker writes the fault met.
     */
    private void assertTruncateThatCannotWriteItsMarkerRetiresTheTimeline(
            FilesFacade ff,
            Runnable arm,
            Runnable disarm,
            IntSupplier markerAttempts
    ) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 0);
        assertMemoryLeak(ff, () -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = buildHistory(job);
                final long resetsBefore = instance.getCheckpointTimelineResets();

                arm.run();
                correct(job, instance, SEALS * 10 - 5, 800);
                disarm.run();

                Assert.assertEquals("the truncate attempts its marker once", 1, markerAttempts.getAsInt());
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

            restartAndRefresh();
            assertRestoredFromTimeline("lv");
            assertViewMatchesRecompute();
        });
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
        assertViewRows(expected, "SELECT ts, sym, s FROM lv");
    }

    private void assertViewRows(String expected, String query) throws Exception {
        assertQuery(query)
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

    private void commitDeparture(LiveViewRefreshJob job, String values) throws Exception {
        execute("INSERT INTO base VALUES " + values);
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    // The one row of a day the base then drops, which the view walks past and keeps: a rebuild
    // from the applied base would lose it.
    private void commitTheDayTheBaseLoses(LiveViewRefreshJob job) throws Exception {
        commitDeparture(job, "('2025-12-31T12:00:00.000000Z', 'a', 0.5)");
        execute("ALTER TABLE base DROP PARTITION LIST '2025-12-31'");
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    /**
     * The head-miss departure fixture: the row 2025-12-31T23:55, whose day the base then drops
     * when {@code isDayLost}, a row a minute from 00:00 to 00:30 the cadence leaves unsealed, then
     * one from 00:31 to {@code lastMinute}, which seals a root at its newest row.
     */
    private void commitHeadMissDepartureFixture(LiveViewRefreshJob job, boolean isDayLost, int lastMinute) throws Exception {
        commitDeparture(job, "('2025-12-31T23:55:00.000000Z', 'a', 0.5)");
        if (isDayLost) {
            execute("ALTER TABLE base DROP PARTITION LIST '2025-12-31'");
            drainWalQueue();
            driveRefreshToQuiescence(job);
        }
        commitDeparture(job, minuteRowsOfA(0, 30, "1.0"));
        setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
        commitDeparture(job, minuteRowsOfA(31, lastMinute, "1.0"));
    }

    /**
     * Over the day the base loses when {@code isDayLost}, a root at the row 00:56 that the
     * departure of 00:59 makes the frontier, then two rows the view's filter drops and the row
     * 00:59, which a root of its own seals when {@code isAboveSealed}. A rebuild from a minute
     * below 00:59 reads none of the dropped rows a resume from the root at 00:56 would read, so it
     * is the cheaper repair.
     */
    private void commitHeadMissRootAtTheFrontierThenRowsAboveIt(LiveViewRefreshJob job, boolean isDayLost, boolean isAboveSealed) throws Exception {
        commitHeadMissDepartureFixture(job, isDayLost, 56);
        if (isAboveSealed) {
            setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
        }
        commitDeparture(
                job,
                "('2026-01-01T00:57:00.000000Z', 'a', -1.0), ('2026-01-01T00:57:30.000000Z', 'a', -1.0), ('2026-01-01T00:59:00.000000Z', 'a', 1.0)"
        );
        assertCheckpointLadder(
                viewInstance(),
                false,
                "2025-12-31T23:55:00.000000Z -> 1\n2026-01-01T00:56:00.000000Z -> 58\n"
                        + (isAboveSealed ? "2026-01-01T00:59:00.000000Z -> 59\n" : "")
        );
    }

    /**
     * Builds a departure, the newest of two roots ({@code isZeroRowDeparture}) or a root over two
     * rows, optionally above the day the base loses, crashes at the frontier truncate's marker,
     * and restarts from what the crash left.
     */
    private void crashAheadOfTheFrontierTruncateAndRestart(
            DepartureMarkerCrash crash,
            boolean isDayLost,
            boolean isZeroRowDeparture
    ) throws Exception {
        createDepartureView();
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveSeedToCompletion(job, "lv");
            if (isDayLost) {
                commitTheDayTheBaseLoses(job);
            }
            commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'a', 1.5)");
            if (isZeroRowDeparture) {
                commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', 2.5)");
                crashAtTheFrontierTruncateMarker(crash, job, "('2026-01-01T00:02:00.000000Z', 'a', -2.5)");
            } else {
                commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', 2.5), ('2026-01-01T00:03:00.000000Z', 'a', 9.5)");
                crashAtTheFrontierTruncateMarker(crash, job, "('2026-01-01T00:03:00.000000Z', 'a', -9.5)");
            }
        }
        restoreTheCrashImageAndRestart(crash);
    }

    /**
     * Commits {@code departure} and copies the view's checkpoint directory to a crash image the
     * moment the frontier truncate's marker lands: the marker records the splice's generation and
     * the view's seqTxn, and the truncate has not published yet. The turn then runs on to its end.
     */
    private void crashAtTheFrontierTruncateMarker(DepartureMarkerCrash crash, LiveViewRefreshJob job, String departure) throws Exception {
        final LiveViewInstance instance = viewInstance();
        try (Path checkpoints = checkpointsDir(instance).slash(); Path crashImage = crashImageDir()) {
            crash.arm(() -> TestUtils.copyDirectory(checkpoints, crashImage, configuration.getMkDirMode()));
            commitDeparture(job, departure);
            crash.disarm();
        }
        Assert.assertTrue("the frontier truncate must write its marker", crash.hasCrashed());
        Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT));
    }

    /**
     * Commits {@code departure} and copies the view's checkpoint directory to a crash image the
     * moment the head-miss rebuild's frontier truncate writes its marker: the marker records the
     * splice's generation and the view's seqTxn, and the truncate has not published yet. The turn
     * then runs on to its end.
     */
    private void crashAtTheHeadMissFrontierTruncateMarker(DepartureMarkerCrash crash, LiveViewRefreshJob job, String departure) throws Exception {
        final LiveViewInstance instance = viewInstance();
        try (Path checkpoints = checkpointsDir(instance).slash(); Path crashImage = crashImageDir()) {
            crash.arm(() -> TestUtils.copyDirectory(checkpoints, crashImage, configuration.getMkDirMode()));
            commitDeparture(job, departure);
            crash.disarm();
        }
        Assert.assertTrue("the frontier truncate must write its marker", crash.hasCrashed());
        Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_FRONTIER_TRUNCATE_COUNT));
    }

    private void createDepartureView() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, sym)");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " + DEPARTURE_VIEW_SQL);
    }

    // A filtered view with a one-minute RANGE frame over a deduplicating base: under the default
    // cadence a correction of its newest rows reaches the head-miss end-of-table splice.
    private void createHeadMissDepartureView() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, sym)");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " + HEAD_MISS_DEPARTURE_VIEW_SQL);
    }

    /**
     * A root over two rows, optionally above the day the base loses, whose newer row departs,
     * with the seal at the new frontier failing on its data segment. The truncate before it left
     * the root below that frontier as the newest, so the timeline retires.
     * {@code isRowAfterTheFault} commits a forward row after it, whose seal reopens the timeline.
     */
    private void failTheDepartureFrontierSeal(
            LiveViewCheckpointDiskFullFault fault,
            boolean isDayLost,
            boolean isRowAfterTheFault
    ) throws Exception {
        createDepartureView();
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveSeedToCompletion(job, "lv");
            if (isDayLost) {
                commitTheDayTheBaseLoses(job);
            }
            commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'a', 1.5)");
            commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', 2.5), ('2026-01-01T00:03:00.000000Z', 'a', 9.5)");
            final LiveViewInstance instance = viewInstance();
            final long resetsBefore = instance.getCheckpointTimelineResets();
            final long sealFailuresBefore = instance.getCheckpointSealFailures();
            hooks(job).setSimulateResumeHeadSealStart(
                    () -> fault.arm(LiveViewCheckpointDiskFullFault.OP_TRUNCATE, LiveViewCheckpointLayout.DATA_DIR_NAME, 1)
            );
            commitDeparture(job, "('2026-01-01T00:03:00.000000Z', 'a', -9.5)");
            fault.disarm();
            Assert.assertNotNull("the seal at the new frontier must reach its data segment", fault.getFailedFile());
            Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT));
            Assert.assertEquals(sealFailuresBefore + 1, instance.getCheckpointSealFailures());
            Assert.assertEquals(
                    "the truncated timeline goes with the seal that did not land",
                    resetsBefore + 1,
                    instance.getCheckpointTimelineResets()
            );
            try (Path dir = checkpointsDir(instance); Path timelinePath = new Path()) {
                LiveViewCheckpointLayout.timelinePath(timelinePath, dir);
                Assert.assertFalse(configuration.getFilesFacade().exists(timelinePath.$()));
                Assert.assertFalse(LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir));
            }
            if (isRowAfterTheFault) {
                commitDeparture(job, "('2026-01-01T00:04:00.000000Z', 'a', 8.0)");
                Assert.assertEquals("the next row's seal opens a fresh history", 1, generation(instance));
                assertCheckpointLadder(instance, false, "2026-01-01T00:04:00.000000Z -> " + (isDayLost ? 4 : 3) + "\n");
            }
        }
        assertViewRows(
                singleDepartureRows(isDayLost) + (isRowAfterTheFault ? "2026-01-01T00:04:00.000000Z\ta\t8.0\t12.0\n" : ""),
                DEPARTURE_ROWS_QUERY
        );
        assertNoRefreshFaults("lv");
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

    /**
     * Stops the process, puts the crash image back as the view's checkpoint directory, and
     * restarts from it.
     */
    private void restoreTheCrashImageAndRestart(DepartureMarkerCrash crash) {
        Assert.assertTrue(crash.hasCrashed());
        final TableToken viewToken = engine.verifyTableName("lv");
        engine.getLiveViewRegistry().clear();
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        engine.releaseInactive();
        try (
                Path checkpoints = new Path().of(configuration.getDbRoot()).concat(viewToken).concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME).slash();
                Path crashImage = crashImageDir()
        ) {
            final FilesFacade ff = configuration.getFilesFacade();
            Assert.assertTrue(ff.rmdir(checkpoints));
            TestUtils.copyDirectory(crashImage, checkpoints, configuration.getMkDirMode());
            Assert.assertTrue(ff.rmdir(crashImage));
        }
        restartAndRefresh();
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

    /**
     * Once armed, runs an action right after the turn's second repair marker rename lands: the
     * marker the frontier truncate writes ahead of its publication, the splice's being the first.
     */
    private static final class DepartureMarkerCrash extends TestFilesFacadeImpl {
        private final AtomicInteger markerRenames = new AtomicInteger();
        private volatile boolean hasCrashed;
        private volatile Runnable onCrash;

        @Override
        public int rename(LPSZ from, LPSZ to) {
            final int result = super.rename(from, to);
            final Runnable action = onCrash;
            if (action != null
                    && result == Files.FILES_RENAME_OK
                    && Utf8s.endsWithAscii(to, LiveViewCheckpointLayout.REPAIRING_MARKER_FILE_NAME)
                    && markerRenames.incrementAndGet() == 2) {
                onCrash = null;
                action.run();
                hasCrashed = true;
            }
            return result;
        }

        void arm(Runnable onCrash) {
            markerRenames.set(0);
            hasCrashed = false;
            this.onCrash = onCrash;
        }

        void disarm() {
            onCrash = null;
        }

        boolean hasCrashed() {
            return hasCrashed;
        }
    }
}
