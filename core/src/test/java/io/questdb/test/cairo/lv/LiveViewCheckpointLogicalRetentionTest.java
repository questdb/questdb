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
import io.questdb.cairo.NanosTimestampDriver;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointLifecycle;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointRecoveryPhase;
import io.questdb.cairo.lv.LiveViewCheckpointRepairMarker;
import io.questdb.cairo.lv.LiveViewCheckpointRepairRoute;
import io.questdb.cairo.lv.LiveViewCheckpointRoot;
import io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectoryReader;
import io.questdb.cairo.lv.LiveViewCheckpointSuperblock;
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
import io.questdb.std.datetime.microtime.Micros;
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
    // Too many open files: a fault a retry may get past.
    private static final int ERRNO_EMFILE = 24;
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
    // The evidence the restatement guard refuses a rebuild of the head-miss owed-root fixture on:
    // the view keeps the row 2025-12-31T23:55, whose day the base lost.
    private static final String HEAD_MISS_OWED_ROOT_HISTORY_FLOOR = "the view holds rows from 2025-12-31T23:55:00.000000Z "
            + "but the base table's earliest row is at 2026-01-01T00:00:00.000000Z";
    // What an upsert of the owed-root fixture's row 00:04 out of the filter leaves: 00:03's sum
    // takes 00:01 and 00:02, by hand.
    private static final String OWED_ROOT_DEPARTED_FRONTIER_ROWS = """
            ts\tsym\tx\ts
            2025-12-31T12:00:00.000000Z\ta\t0.5\t0.5
            2026-01-01T00:01:00.000000Z\ta\t1.5\t2.0
            2026-01-01T00:02:00.000000Z\ta\t2.5\t4.5
            2026-01-01T00:03:00.000000Z\ta\t9.5\t13.5
            """;
    // The evidence the restatement guard refuses a rebuild of the owed-root fixture on: the view
    // keeps the row 2025-12-31T12:00, whose day the base lost.
    private static final String OWED_ROOT_HISTORY_FLOOR = "the view holds rows from 2025-12-31T12:00:00.000000Z "
            + "but the base table's earliest row is at 2026-01-01T00:01:00.000000Z";
    // What the owed-root fixture and its late row 00:01:30 leave: s is the sum of x over the row
    // and the two rows of 'a' below it, by hand.
    private static final String OWED_ROOT_ROWS = """
            ts\tsym\tx\ts
            2025-12-31T12:00:00.000000Z\ta\t0.5\t0.5
            2026-01-01T00:01:00.000000Z\ta\t1.5\t2.0
            2026-01-01T00:01:30.000000Z\ta\t1.0\t3.0
            2026-01-01T00:02:00.000000Z\ta\t2.5\t5.0
            2026-01-01T00:03:00.000000Z\ta\t9.5\t13.0
            2026-01-01T00:04:00.000000Z\ta\t1.0\t13.0
            """;
    // The in-order rows of 1.0 the owed-root cases add above OWED_ROOT_ROWS: 9.5 + 1.0 + 1.0 at
    // 00:05, then 1.0 + 1.0 + 1.0.
    private static final String OWED_ROOT_ROW_0005 = "2026-01-01T00:05:00.000000Z\ta\t1.0\t11.5\n";
    private static final String OWED_ROOT_ROW_0006 = "2026-01-01T00:06:00.000000Z\ta\t1.0\t3.0\n";
    private static final String OWED_ROOT_ROW_0007 = "2026-01-01T00:07:00.000000Z\ta\t1.0\t3.0\n";
    private static final String OWED_ROOT_ROW_0008 = "2026-01-01T00:08:00.000000Z\ta\t1.0\t3.0\n";
    // What the owed-root fixture leaves, by hand: s is the sum of x over the row and the two rows
    // of 'a' below it.
    private static final String PAUSE_FIXTURE_ROWS = OWED_ROOT_DEPARTED_FRONTIER_ROWS + "2026-01-01T00:04:00.000000Z\ta\t1.0\t13.0\n";
    // OWED_ROOT_ROWS with a second late row, 00:01:45, which shifts the two sums above it.
    private static final String OWED_ROOT_SECOND_LATE_ROW_ROWS = """
            ts\tsym\tx\ts
            2025-12-31T12:00:00.000000Z\ta\t0.5\t0.5
            2026-01-01T00:01:00.000000Z\ta\t1.5\t2.0
            2026-01-01T00:01:30.000000Z\ta\t1.0\t3.0
            2026-01-01T00:01:45.000000Z\ta\t1.0\t3.5
            2026-01-01T00:02:00.000000Z\ta\t2.5\t4.5
            2026-01-01T00:03:00.000000Z\ta\t9.5\t13.0
            2026-01-01T00:04:00.000000Z\ta\t1.0\t13.0
            """;
    // What the owed-root fixture and its late rows 00:01:10, 00:01:20 and 00:01:40 leave, by hand.
    private static final String THREE_LATE_ROWS = """
            ts\tsym\tx\ts
            2025-12-31T12:00:00.000000Z\ta\t0.5\t0.5
            2026-01-01T00:01:00.000000Z\ta\t1.5\t2.0
            2026-01-01T00:01:10.000000Z\ta\t1.0\t3.0
            2026-01-01T00:01:20.000000Z\ta\t1.0\t3.5
            2026-01-01T00:01:40.000000Z\ta\t1.0\t3.0
            2026-01-01T00:02:00.000000Z\ta\t2.5\t4.5
            2026-01-01T00:03:00.000000Z\ta\t9.5\t13.0
            2026-01-01T00:04:00.000000Z\ta\t1.0\t13.0
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
    public void testRestartPastACorruptNewestRootLogsBothRootsInTheViewsUnit() throws Exception {
        assertRestartPastACorruptNewestRootReconstructsIt(false);
    }

    @Test
    public void testRestartPastACorruptNewestRootLogsBothRootsInTheViewsUnitOverANanosecondBase() throws Exception {
        assertRestartPastACorruptNewestRootReconstructsIt(true);
    }

    @Test
    public void testRestartPastACorruptNewestRootWhoseHealCannotWriteLogsItInTheViewsUnitOverANanosecondBase() throws Exception {
        // The case above with the first allocation of a data segment refused, as a full disk
        // refuses it: the heal cannot write the root it reconstructs, and logs the torn root 00:02
        // in nanoseconds. The restore fails with it, and the restart rebuilds the view from the
        // applied base, which still holds every row.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final String day = "2026-01-01";
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView(true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                corruptNewestRootDataSegment(commitTwoRowsAboveTheNewestRoot(job, day, false));
            }
            fault.arm(LiveViewCheckpointDiskFullFault.OP_ALLOCATE, LiveViewCheckpointLayout.DATA_DIR_NAME, 1);
            final LogCapture capture = restartAndRefreshCapturingLog();
            fault.disarm();
            Assert.assertNotNull("the heal must reach a data segment", fault.getFailedFile());
            capture.assertLogged("live view checkpoint restore fell back past corrupt roots, reconstructing [view=lv, "
                    + "predecessorMaxTs=2026-01-01T00:01:00.000000000Z, corruptCeilingMaxTs=2026-01-01T00:02:00.000000000Z]");
            capture.assertLogged("could not reconstruct corrupt live view checkpoint roots [view=lv, "
                    + "corruptCeilingMaxTs=2026-01-01T00:02:00.000000000Z, error=");
            capture.assertNotLogged("reconstructed corrupt live view checkpoint roots");
            assertRebuiltFromAppliedBase("lv");
            assertViewRows(owedRootText(PAUSE_FIXTURE_ROWS, day, true), DEPARTURE_ROWS_QUERY);
            assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
        });
    }

    @Test
    public void testRestartLogsTheRootItRestoresAndTheFrontierInTheViewsUnit() throws Exception {
        assertRestartLogsTheRootItRestoresAndTheFrontier(false);
    }

    @Test
    public void testRestartLogsTheRootItRestoresAndTheFrontierInTheViewsUnitOverANanosecondBase() throws Exception {
        // Every restart of a view over a nanosecond base logs this line: both timestamps print in
        // nanoseconds, with the digits below the microsecond the rows carry.
        assertRestartLogsTheRootItRestoresAndTheFrontier(true);
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
        // The base applies an in-order tie on the frontier before the view consumes it. The drain
        // will fold that row as it consumes the commit, so a root the heal sealed with the row
        // already in it would count it twice. The heal declines, and the rebuild the lost day
        // refuses leaves the view on the rows it holds.
        assertRestartDeclinesToHealOverATieOnTheFrontierTheViewHasNotConsumed(false);
    }

    @Test
    public void testRestartDeclinesToHealOverATieOnTheFrontierTheViewHasNotConsumedOverANanosecondBase() throws Exception {
        // The same heal over a base whose designated timestamp has nanosecond precision: its line
        // prints the frontier in nanoseconds.
        assertRestartDeclinesToHealOverATieOnTheFrontierTheViewHasNotConsumed(true);
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
        assertRestartDeclinesToHealALostRowAloneInItsInterval(false);
    }

    @Test
    public void testRestartDeclinesToHealALostRowAloneInItsIntervalOverANanosecondBase() throws Exception {
        // The same heal over a base whose designated timestamp has nanosecond precision: its line
        // prints the interval's bounds in nanoseconds.
        assertRestartDeclinesToHealALostRowAloneInItsInterval(true);
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
        assertRestartDeclinesToHealALostRowOnItsIntervalsLastTimestamp(false);
    }

    @Test
    public void testRestartDeclinesToHealALostRowOnItsIntervalsLastTimestampOverANanosecondBase() throws Exception {
        // The same heal over a base whose designated timestamp has nanosecond precision: its line
        // prints the lost row's timestamp in nanoseconds.
        assertRestartDeclinesToHealALostRowOnItsIntervalsLastTimestamp(true);
    }

    @Test
    public void testRestartDeclinesToHealALostRowUnderAsManyUnconsumedRowsInItsInterval() throws Exception {
        // One unconsumed row cancels the lost one in the interval's count, so the per-timestamp
        // walk declines, at the lost row's timestamp, inside the interval.
        assertRestartDeclinesToHealALostRowUnderAsManyUnconsumedRowsInItsInterval(false);
    }

    @Test
    public void testRestartDeclinesToHealALostRowUnderAsManyUnconsumedRowsInItsIntervalOverANanosecondBase() throws Exception {
        // The same heal over a base whose designated timestamp has nanosecond precision: its line
        // prints the lost row's timestamp in nanoseconds.
        assertRestartDeclinesToHealALostRowUnderAsManyUnconsumedRowsInItsInterval(true);
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
    public void testDepartureCrashAheadOfTheFrontierTruncateKeepsTheRowsOfTheDayTheBaseLost() throws Exception {
        // The crash between the marker the frontier truncate writes and its publication, after a
        // departure whose resume emitted the row below it, over the day the base lost. The splice's
        // generation holds no root at that row, the new frontier, so a restore from it would fail
        // its row count, and a rebuild that stood the restatement guard down on that would drop the
        // 2025-12-31 row only the view still holds. The marker reads as live instead: each restart
        // rebuilds with the guard armed, which refuses, and the view stops with every row.
        final DepartureMarkerCrash crash = new DepartureMarkerCrash();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(crash, () -> {
                crashAheadOfTheFrontierTruncateAndRestart(crash, true, false);
                assertRebuildBlockedByTheRepairMarker(singleDepartureRows(true), OWED_ROOT_HISTORY_FLOOR);
                assertRestartsBlockedByTheRepairMarker(singleDepartureRows(true), OWED_ROOT_HISTORY_FLOOR);
                capture.drain();
                capture.assertNotLogged("does not match durable materialization");
                capture.assertNotLogged("runs without the restatement guard");
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testDepartureCrashAheadOfTheFrontierTruncateRebuildsAndSealsBelowTheDepartedRow() throws Exception {
        // The same crash after a departure whose resume replayed nothing, over a base that lost
        // nothing: the splice kept the root of the row that became the frontier, 00:01, and the
        // departed row's root, 00:02, above it. The restart rebuilds from the applied base rather
        // than restore that generation, so no root of the departed row is left above the frontier,
        // and each row committed in order below where it was then seals. The restart after them
        // restores.
        final DepartureMarkerCrash crash = new DepartureMarkerCrash();
        assertMemoryLeak(crash, () -> {
            crashAheadOfTheFrontierTruncateAndRestart(crash, false, true);
            assertRebuiltFromAppliedBase("lv");
            final String rowsAtTheRestart = """
                    ts\tsym\tx\ts
                    2026-01-01T00:01:00.000000Z\ta\t1.5\t1.5
                    """;
            assertViewRows(rowsAtTheRestart, DEPARTURE_ROWS_QUERY);
            assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
            final LiveViewInstance instance = viewInstance();
            final long resetsBefore = instance.getCheckpointTimelineResets();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int second = 10; second <= 30; second += 10) {
                    setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                    commitDeparture(job, "('2026-01-01T00:01:" + second + ".000000Z', 'a', 1.0)");
                }
            }
            Assert.assertEquals("no seal meets a root above the frontier", 0, instance.getCheckpointSealFailures());
            Assert.assertEquals(resetsBefore, instance.getCheckpointTimelineResets());
            // The view held one row ahead of them, and each seals a root of its own.
            assertNewestRoots(
                    instance,
                    "2026-01-01T00:01:10.000000Z", 2,
                    "2026-01-01T00:01:20.000000Z", 3,
                    "2026-01-01T00:01:30.000000Z", 4
            );
            // s sums x over the row and the two rows of 'a' below it.
            final String rows = rowsAtTheRestart + """
                    2026-01-01T00:01:10.000000Z\ta\t1.0\t2.5
                    2026-01-01T00:01:20.000000Z\ta\t1.0\t3.5
                    2026-01-01T00:01:30.000000Z\ta\t1.0\t3.0
                    """;
            assertViewRows(rows, DEPARTURE_ROWS_QUERY);
            assertNoRefreshFaults("lv");
            restartAndRefresh();
            assertRestoredFromTimeline("lv");
            assertViewRows(rows, DEPARTURE_ROWS_QUERY);
        });
    }

    @Test
    public void testDepartureCrashAheadOfTheFrontierTruncateRebuildsWhenTheSpliceKeptARootAboveARow() throws Exception {
        // A crash between the marker the frontier truncate writes and its publication leaves the
        // splice's generation under that marker. The truncate runs after the replacement commit,
        // so its marker records no seqTxn and reads as live: the restart rebuilds from the
        // applied base without trying the splice's generation, whose root of the departed row
        // sits above the row that became the frontier. The base lost nothing here, and the
        // rebuild gives what a recompute gives.
        final DepartureMarkerCrash crash = new DepartureMarkerCrash();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(crash, () -> {
                crashAheadOfTheFrontierTruncateAndRestart(crash, false, false);
                capture.drain();
                capture.assertLogged("live view restart rebuilding from applied base [view=lv, cause=prefix preservation repair marker present");
                capture.assertNotLogged("does not match durable materialization");
                assertRebuiltFromAppliedBase("lv");
                Assert.assertFalse(viewInstance().isCheckpointRecoveryBlocked());
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
    public void testDepartureCrashAheadOfTheFrontierTruncateRefusesTheRebuildWhenTheSpliceKeptTheFrontierRoot() throws Exception {
        // The same crash after a departure whose resume replayed nothing: the splice kept the root
        // of the row that became the frontier, and the departed row's root above it, where a
        // restore would leave it to refuse every seal below it. The marker reads as live, so each
        // restart rebuilds from the applied base, which the restatement guard refuses over the day
        // the base lost: the view stops with every row.
        final DepartureMarkerCrash crash = new DepartureMarkerCrash();
        assertMemoryLeak(crash, () -> {
            crashAheadOfTheFrontierTruncateAndRestart(crash, true, true);
            assertRebuildBlockedByTheRepairMarker(DEPARTURE_ROWS_OVER_THE_LOST_DAY, OWED_ROOT_HISTORY_FLOOR);
            assertRestartsBlockedByTheRepairMarker(DEPARTURE_ROWS_OVER_THE_LOST_DAY, OWED_ROOT_HISTORY_FLOOR);
        });
    }

    @Test
    public void testDepartureCrashAheadOfTheFrontierTruncateRefusesTheRebuildWhenTheSpliceReVersionedTheFrontierRoot() throws Exception {
        // The same crash after a late row and a departure in one commit, whose replay stopped on a
        // root the splice re-versioned at the new frontier: the marker reads as live, and the
        // restatement guard refuses each restart's rebuild over the day the base lost.
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
            final String rows = """
                    ts\tsym\tx\ts
                    2025-12-31T12:00:00.000000Z\ta\t0.5\t0.5
                    2026-01-01T00:01:00.000000Z\ta\t1.5\t2.0
                    2026-01-01T00:01:30.000000Z\ta\t3.0\t5.0
                    2026-01-01T00:02:00.000000Z\ta\t2.5\t7.0
                    """;
            assertRebuildBlockedByTheRepairMarker(rows, OWED_ROOT_HISTORY_FLOOR);
            assertRestartsBlockedByTheRepairMarker(rows, OWED_ROOT_HISTORY_FLOOR);
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
    public void testDepartureFrontierTruncateWhosePublicationAndRetireFailKeepsTheRowsOfTheDayTheBaseLost() throws Exception {
        // The frontier truncate fails after writing its marker and its metadata, and the retire
        // that answers it removes nothing, so the splice's generation stays on disk under the
        // truncate's marker in a running process. The marker records no seqTxn, so the in-place
        // recovery a base column brings declines that generation, whose newest root sits above the
        // row that became the frontier, and the restatement guard refuses the rebuild over the day
        // the base lost: the view stops with every row, and so does each restart.
        assertFrontierTruncateWhosePublicationAndRetireFailKeepsTheRows(false);
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
    public void testDepartureRestampPassesTheSealCooldownAndKeepsTheTruncatedTimeline() throws Exception {
        // The truncate leaves its newest root at the new frontier, so the seal there only re-stamps
        // the head, which writes nothing, and a seal on cooldown lets it through: the head goes back
        // on the root at the frontier. The truncated generation is restorable as it stands: its
        // newest root sits at the frontier, so the marker clears and the timeline stays, and each
        // restart restores from it over the day the base lost.
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
                Assert.assertEquals("the splice and the truncate, and no root", generationBefore + 2, generation(instance));
                Assert.assertEquals("the re-stamp is no failed seal", sealFailuresBefore, instance.getCheckpointSealFailures());
                Assert.assertEquals("the truncated timeline stays", resetsBefore, instance.getCheckpointTimelineResets());
                Assert.assertEquals("the re-stamp puts the head on the root at the frontier", ts("2026-01-01T00:01:00.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals("at the departure's commit", lastBaseSeqTxn(), instance.getHeadCheckpointBaseSeqTxn());
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
    public void testDepartureRestampUnderTheSealCooldownReleasesBaseWal() throws Exception {
        // The truncated timeline of the case above keeps its newest root at the new frontier
        // 00:01, with the newest timestamp the view has seen still at the departed 00:02. The
        // re-stamp the cooldown lets through puts the head on that root, so each cycle without rows
        // past the cadence republishes it at its own commit and moves the base WAL floor up, rather
        // than ask for a root at 00:02, above the newest row the view holds.
        configureWalPurge();
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
                Assert.assertEquals(MicrosTimestampDriver.floor("2026-01-01T00:01:00.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(5, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(MicrosTimestampDriver.floor("2026-01-01T00:02:00.000000Z"), instance.getLatestSeenTs());
                assertCheckpointLadder(instance, false, ladder);
                for (int minute = 3; minute <= 5; minute++) {
                    final long generationBefore = generation(instance);
                    setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                    commitDeparture(job, "('2026-01-01T00:0" + minute + ":00.000000Z', 'a', -1.0)");
                    Assert.assertEquals("one publication per cycle", generationBefore + 1, generation(instance));
                    Assert.assertEquals("at the cycle's commit", minute + 3, instance.getHeadCheckpointBaseSeqTxn());
                }
                Assert.assertEquals(MicrosTimestampDriver.floor("2026-01-01T00:01:00.000000Z"), instance.getHeadCheckpointMaxTs());
                assertCheckpointLadder(instance, false, ladder);
                Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT));
                Assert.assertEquals("the publications are no failed seals", sealFailuresBefore, instance.getCheckpointSealFailures());
                // The older of the two superblock slots, which the publication at seqTxn 8 left at 7.
                Assert.assertEquals(7, instance.getCheckpointTimelineWalPurgeFloor());
                Assert.assertEquals("the purge must keep only the segments above the floor", 8 - 7, purgeBaseWalAndCountSegments());
            }
            assertViewRows(DEPARTURE_ROWS_OVER_THE_LOST_DAY, DEPARTURE_ROWS_QUERY);
            assertNoRefreshFaults("lv");
            assertRowlessCyclesBetweenTwoRestartsKeepTheTimeline();
        });
    }

    @Test
    public void testDepartureRestampThatTheViewTableReadDeclinesHoldsBaseWalUntilARestart() throws Exception {
        // The truncated timeline of the cases above keeps its newest root at the new frontier
        // 00:01, and the re-stamp's read of the view's table fails, which declines it: the head
        // stays clear, with the newest timestamp the view has seen still at the departed 00:02.
        // Each cycle without rows then declines a first root there, above the newest row the view
        // holds, and holds the base WAL where the repair left it. The restart restores from the
        // root at the frontier, which puts the head back on it, and the cycles after it move its
        // base coordinate again.
        configureWalPurge();
        final AtomicReference<String> failedOpenDir = new AtomicReference<>();
        final AtomicInteger failedOpens = new AtomicInteger();
        final TestFilesFacadeImpl ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                final String dirName = failedOpenDir.get();
                if (dirName != null
                        && Utf8s.containsAscii(name, Files.SEPARATOR + dirName + Files.SEPARATOR)
                        && Utf8s.endsWithAscii(name, Files.SEPARATOR + TableUtils.TXN_FILE_NAME)
                        && failedOpenDir.compareAndSet(dirName, null)) {
                    failedOpens.incrementAndGet();
                    return -1;
                }
                return super.openRO(name);
            }
        };
        assertMemoryLeak(ff, () -> {
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
                hooks(job).setSimulateResumeHeadSealStart(() -> {
                    // No pooled reader may serve the read, so it opens the table's files. The
                    // resume holds a reader of the base, so only the view's are released.
                    Assert.assertTrue(engine.lockReadersByTableToken(instance.getLiveViewToken()));
                    engine.unlockReaders(instance.getLiveViewToken());
                    failedOpenDir.set(instance.getLiveViewToken().getDirName());
                });
                commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', -2.5)");
                hooks(job).setSimulateResumeHeadSealStart(null);
                Assert.assertEquals("the re-stamp's read of the view's table fails", 1, failedOpens.get());
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
            assertRowlessCyclesBetweenTwoRestartsKeepTheTimeline();
        });
    }

    @Test
    public void testDepartureThatEmptiesTheViewCrashAheadOfTheFrontierTruncateRebuildsNoRows() throws Exception {
        // The crash between the marker the frontier truncate writes and its publication, after the
        // departure that empties the view: the truncate would keep no root and retire the
        // timeline. Its marker reads as live, so the restart rebuilds from the applied base rather
        // than restore the splice's generation, and comes back with no rows, still refreshing: the
        // view retains no row a rebuild could drop. The rows after it seal a fresh history, which
        // the restart after them restores.
        final DepartureMarkerCrash crash = new DepartureMarkerCrash();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(crash, () -> {
                createHeadMissDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'a', 1.5)");
                    commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'a', 2.5)");
                    replaceBaseRange(
                            MicrosTimestampDriver.floor("2026-01-01T00:01:00.000000Z"),
                            MicrosTimestampDriver.floor("2026-01-01T00:01:00.000001Z"),
                            Double.NaN
                    );
                    drainWalQueue();
                    driveRefreshToQuiescence(job);
                    crashAtTheFrontierTruncateMarker(crash, job, "('2026-01-01T00:05:00.000000Z', 'a', -2.5)");
                }
                restoreTheCrashImageAndRestart(crash);
                capture.drain();
                capture.assertLogged("live view restart rebuilding from applied base [view=lv, cause=prefix preservation repair marker present");
                capture.assertNotLogged("could not restore live view from checkpoint timeline");
                assertRebuiltFromAppliedBase("lv");
                final LiveViewInstance instance = viewInstance();
                Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
                Assert.assertFalse(instance.isInvalid());
                final String noRows = "ts\tsym\tx\ts\n";
                assertViewRows(noRows, DEPARTURE_ROWS_QUERY);
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    commitDeparture(job, "('2026-01-01T00:03:00.000000Z', 'a', 4.0)");
                    commitDeparture(job, "('2026-01-01T00:04:00.000000Z', 'a', 4.0)");
                }
                assertCheckpointLadder(instance, false, """
                        2026-01-01T00:03:00.000000Z -> 1
                        2026-01-01T00:04:00.000000Z -> 2
                        """);
                // 00:04's one-minute frame holds 00:03.
                final String rows = noRows + """
                        2026-01-01T00:03:00.000000Z\ta\t4.0\t4.0
                        2026-01-01T00:04:00.000000Z\ta\t4.0\t8.0
                        """;
                assertViewRows(rows, DEPARTURE_ROWS_QUERY);
                assertNoRefreshFaults("lv");
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(rows, DEPARTURE_ROWS_QUERY);
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testDepartureThatEmptiesTheViewWithTheChainDeclinedRetiresItsTimeline() throws Exception {
        // As below, with the resume's chain declined: it truncates at its rowless anchor ahead of
        // its commit and replays nothing, and the view it leaves holds no row, so no root can
        // stand at its frontier. The truncated timeline retires, and a restart has nothing to
        // restore: it comes back with no rows, still refreshing.
        assertMemoryLeak(() -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'a', 1.5)");
                commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'a', 2.5)");
                final LiveViewInstance instance = viewInstance();
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
                setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 0);
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
                Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.TRUNCATE_ZERO_ROW_KEEP_COUNT));
                Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT));
                Assert.assertEquals(resetsBefore + 1, instance.getCheckpointTimelineResets());
                assertTimelineAndMarkerRetired(instance);
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
    public void testZeroRowResumeCrashAfterItsFrontierTruncatePublishesOverALostDayBlocksTheRestart() throws Exception {
        // The crash lands after the frontier truncate published and before the watermark: the
        // generation stands one past the one the truncate's marker names, which reads as live, so
        // the restart rebuilds, and the restatement guard refuses it over the day the base lost.
        final ZeroRowFrontierTruncateCrash crash = new ZeroRowFrontierTruncateCrash(true);
        assertMemoryLeak(crash, () -> {
            crashInTheZeroRowFrontierTruncateAndRestart(crash, true);
            assertRebuildBlockedByTheRepairMarker(DEPARTURE_ROWS_OVER_THE_LOST_DAY, OWED_ROOT_HISTORY_FLOOR);
            assertRestartsBlockedByTheRepairMarker(DEPARTURE_ROWS_OVER_THE_LOST_DAY, OWED_ROOT_HISTORY_FLOOR);
        });
    }

    @Test
    public void testZeroRowResumeCrashAfterItsFrontierTruncatePublishesRebuilds() throws Exception {
        // As above, over a base that lost nothing: the rebuild gives what a recompute gives.
        final ZeroRowFrontierTruncateCrash crash = new ZeroRowFrontierTruncateCrash(true);
        assertMemoryLeak(crash, () -> {
            crashInTheZeroRowFrontierTruncateAndRestart(crash, false);
            assertRebuiltFromAppliedBase("lv");
            Assert.assertFalse(viewInstance().isCheckpointRecoveryBlocked());
            assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
        });
    }

    @Test
    public void testZeroRowResumeCrashAtItsFrontierTruncateMarkerOverALostDayBlocksTheRestart() throws Exception {
        // The crash lands between the frontier truncate's marker and its publication. The marker
        // names the generation the resume's first truncate published and records no seqTxn, so it
        // reads as live, as at the neighbouring crash points: the restart rebuilds, and the
        // restatement guard refuses it over the day the base lost.
        final ZeroRowFrontierTruncateCrash crash = new ZeroRowFrontierTruncateCrash(false);
        assertMemoryLeak(crash, () -> {
            crashInTheZeroRowFrontierTruncateAndRestart(crash, true);
            assertRebuildBlockedByTheRepairMarker(DEPARTURE_ROWS_OVER_THE_LOST_DAY, OWED_ROOT_HISTORY_FLOOR);
            assertRestartsBlockedByTheRepairMarker(DEPARTURE_ROWS_OVER_THE_LOST_DAY, OWED_ROOT_HISTORY_FLOOR);
        });
    }

    @Test
    public void testZeroRowResumeCrashAtItsFrontierTruncateMarkerRebuilds() throws Exception {
        final ZeroRowFrontierTruncateCrash crash = new ZeroRowFrontierTruncateCrash(false);
        assertMemoryLeak(crash, () -> {
            crashInTheZeroRowFrontierTruncateAndRestart(crash, false);
            assertRebuiltFromAppliedBase("lv");
            Assert.assertFalse(viewInstance().isCheckpointRecoveryBlocked());
            assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
        });
    }

    @Test
    public void testZeroRowResumeWhoseFrontierTruncateFailsOpensAFreshHistoryAtTheFrontier() throws Exception {
        // The chain declined, the resume anchors on a rowless root and replays nothing, and its
        // frontier truncate fails on a metadata segment, which retires the timeline. The runtime
        // still holds the state at the view's new frontier, so the seal there opens a fresh
        // history, which each restart restores from over the day the base lost.
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTheDayTheBaseLoses(job);
                commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'a', 1.5)");
                commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', 2.5)");
                commitDeparture(job, "('2026-01-01T00:03:00.000000Z', 'a', 3.5)");
                commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', -2.5)");
                final LiveViewInstance instance = viewInstance();
                assertCheckpointLadder(instance, false, """
                        2025-12-31T12:00:00.000000Z -> 1
                        2026-01-01T00:01:00.000000Z -> 2
                        2026-01-01T00:02:00.000000Z -> 2
                        2026-01-01T00:03:00.000000Z -> 3
                        """);
                setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 0);
                final long resetsBefore = instance.getCheckpointTimelineResets();
                hooks(job).setSimulateResumeHeadSealStart(
                        () -> fault.arm(LiveViewCheckpointDiskFullFault.OP_ALLOCATE, LiveViewCheckpointLayout.META_DIR_NAME, 1)
                );
                commitDeparture(job, "('2026-01-01T00:03:00.000000Z', 'a', -3.5)");
                hooks(job).setSimulateResumeHeadSealStart(null);
                fault.disarm();
                Assert.assertNotNull("the frontier truncate must reach its metadata segment", fault.getFailedFile());
                Assert.assertEquals("the failed truncate retires the timeline", resetsBefore + 1, instance.getCheckpointTimelineResets());
                Assert.assertEquals(0, instance.getCheckpointSealFailures());
                Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.TRUNCATE_ZERO_ROW_KEEP_COUNT));
                Assert.assertEquals("the seal opens a fresh history", 1, generation(instance));
                assertCheckpointLadder(instance, false, "2026-01-01T00:01:00.000000Z -> 2\n");
                Assert.assertEquals(ts("2026-01-01T00:01:00.000000Z"), instance.getHeadCheckpointMaxTs());
                assertNoRepairMarker(instance);
            }
            assertViewRows(DEPARTURE_ROWS_OVER_THE_LOST_DAY, DEPARTURE_ROWS_QUERY);
            assertNoRefreshFaults("lv");
            for (int restart = 0; restart < 2; restart++) {
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(DEPARTURE_ROWS_OVER_THE_LOST_DAY, DEPARTURE_ROWS_QUERY);
            }
        });
    }

    @Test
    public void testZeroRowResumeWhoseFrontierSealFailsRetiresItsTruncatedTimeline() throws Exception {
        // The chain declined, the resume anchors on a rowless root and replays nothing, so its
        // frontier truncate drops that root and its seal at the new frontier fails on its data
        // segment, after the truncate published. The truncated timeline retires with the
        // truncate's marker, and the next row opens a fresh history, which each restart restores.
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTheDayTheBaseLoses(job);
                commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'a', 1.5)");
                commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', 2.5), ('2026-01-01T00:03:00.000000Z', 'a', 4.5)");
                commitDeparture(job, "('2026-01-01T00:04:00.000000Z', 'a', 8.5)");
                commitDeparture(job, "('2026-01-01T00:03:00.000000Z', 'a', -4.5)");
                final LiveViewInstance instance = viewInstance();
                assertCheckpointLadder(instance, false, """
                        2025-12-31T12:00:00.000000Z -> 1
                        2026-01-01T00:01:00.000000Z -> 2
                        2026-01-01T00:03:00.000000Z -> 3
                        2026-01-01T00:04:00.000000Z -> 4
                        """);
                setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 0);
                final long resetsBefore = instance.getCheckpointTimelineResets();
                final long sealFailuresBefore = instance.getCheckpointSealFailures();
                hooks(job).setSimulateResumeHeadSealStart(
                        () -> fault.arm(LiveViewCheckpointDiskFullFault.OP_ALLOCATE, LiveViewCheckpointLayout.DATA_DIR_NAME, 1)
                );
                commitDeparture(job, "('2026-01-01T00:04:00.000000Z', 'a', -8.5)");
                hooks(job).setSimulateResumeHeadSealStart(null);
                fault.disarm();
                Assert.assertNotNull("the seal at the frontier must reach its data segment", fault.getFailedFile());
                Assert.assertEquals(sealFailuresBefore + 1, instance.getCheckpointSealFailures());
                Assert.assertEquals("the failed seal retires the truncated timeline", resetsBefore + 1, instance.getCheckpointTimelineResets());
                Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.TRUNCATE_ZERO_ROW_KEEP_COUNT));
                assertTimelineAndMarkerRetired(instance);
                commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'a', 4.5)");
                Assert.assertEquals("the next row opens a fresh history", 1, generation(instance));
                assertCheckpointLadder(instance, false, "2026-01-01T00:05:00.000000Z -> 4\n");
            }
            final String rows = singleDepartureRows(true) + "2026-01-01T00:05:00.000000Z\ta\t4.5\t8.5\n";
            assertViewRows(rows, DEPARTURE_ROWS_QUERY);
            assertNoRefreshFaults("lv");
            for (int restart = 0; restart < 2; restart++) {
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(rows, DEPARTURE_ROWS_QUERY);
            }
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
    public void testHeadMissDepartureCrashAheadOfTheFrontierTruncateKeepsTheRowsOfTheDayTheBaseLost() throws Exception {
        // The crash between the marker the head-miss rebuild's frontier truncate writes and its
        // publication, after the kept-row departure over the day the base lost. The splice's
        // generation holds no root at the kept row, the new frontier, so a restore from it would
        // fail its row count, and a rebuild that stood the restatement guard down on that would
        // drop the 2025-12-31 row only the view still holds. The marker reads as live instead:
        // each restart rebuilds with the guard armed, which refuses, and the view stops with every
        // row. Once the day goes back into the base, the restart's rebuild passes the guard, and
        // the restart after it restores.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final DepartureMarkerCrash crash = new DepartureMarkerCrash();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(crash, () -> {
                createHeadMissDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    commitHeadMissDepartureFixture(job, true, 59);
                    crashAtTheHeadMissFrontierTruncateMarker(crash, job, HEAD_MISS_KEPT_ROW_DEPARTURE);
                }
                restoreTheCrashImageAndRestart(crash);
                assertRebuildBlockedByTheRepairMarker(HEAD_MISS_KEPT_ROW_DEPARTURE_ROWS, HEAD_MISS_OWED_ROOT_HISTORY_FLOOR);
                assertRestartsBlockedByTheRepairMarker(HEAD_MISS_KEPT_ROW_DEPARTURE_ROWS, HEAD_MISS_OWED_ROOT_HISTORY_FLOOR);
                capture.drain();
                capture.assertNotLogged("does not match durable materialization");
                capture.assertNotLogged("runs without the restatement guard");
                execute("INSERT INTO base VALUES ('2025-12-31T23:55:00.000000Z', 'a', 0.5)");
                drainWalQueue();
                restartAndRefresh();
                assertRebuiltFromAppliedBase("lv");
                Assert.assertFalse(viewInstance().isCheckpointRecoveryBlocked());
                assertViewRows(HEAD_MISS_KEPT_ROW_DEPARTURE_ROWS, DEPARTURE_ROWS_QUERY);
                assertViewMatchesRecompute(HEAD_MISS_DEPARTURE_VIEW_SQL);
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(HEAD_MISS_KEPT_ROW_DEPARTURE_ROWS, DEPARTURE_ROWS_QUERY);
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testHeadMissDepartureCrashAheadOfTheFrontierTruncateRebuildsAndSealsBelowTheDepartedRow() throws Exception {
        // The same crash after a departure that emitted nothing above the root at the row that
        // became the frontier, 00:56, over a base that lost nothing: the splice's generation keeps
        // the departed row's root, 00:59, above it. The restart rebuilds from the applied base
        // rather than restore that generation, so rows committed in order below where the departed
        // row was seal with no root of it in their way, and the restart after them restores.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final DepartureMarkerCrash crash = new DepartureMarkerCrash();
        assertMemoryLeak(crash, () -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitHeadMissRootAtTheFrontierThenRowsAboveIt(job, false, true);
                crashAtTheHeadMissFrontierTruncateMarker(crash, job, "('2026-01-01T00:59:00.000000Z', 'a', -1.0)");
            }
            restoreTheCrashImageAndRestart(crash);
            assertRebuiltFromAppliedBase("lv");
            assertViewRows(headMissDepartureRows(true, 56), DEPARTURE_ROWS_QUERY);
            assertViewMatchesRecompute(HEAD_MISS_DEPARTURE_VIEW_SQL);
            final LiveViewInstance instance = viewInstance();
            final long resetsBefore = instance.getCheckpointTimelineResets();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int second = 10; second <= 30; second += 10) {
                    setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                    commitDeparture(job, "('2026-01-01T00:58:" + second + ".000000Z', 'a', 1.0)");
                }
            }
            Assert.assertEquals("no seal meets a root above the frontier", 0, instance.getCheckpointSealFailures());
            Assert.assertEquals(resetsBefore, instance.getCheckpointTimelineResets());
            // The view held 58 rows ahead of them, 23:55 and 00:00 to 00:56, and each seals a root
            // of its own.
            assertNewestRoots(
                    instance,
                    "2026-01-01T00:58:10.000000Z", 59,
                    "2026-01-01T00:58:20.000000Z", 60,
                    "2026-01-01T00:58:30.000000Z", 61
            );
            // Each row's one-minute frame reaches 00:57:30 at the lowest, whose row the filter
            // drops, so it holds only the rows committed after the restart.
            final String rows = headMissDepartureRows(true, 56) + """
                    2026-01-01T00:58:10.000000Z\ta\t1.0\t1.0
                    2026-01-01T00:58:20.000000Z\ta\t1.0\t2.0
                    2026-01-01T00:58:30.000000Z\ta\t1.0\t3.0
                    """;
            assertViewRows(rows, DEPARTURE_ROWS_QUERY);
            assertNoRefreshFaults("lv");
            restartAndRefresh();
            assertRestoredFromTimeline("lv");
            assertViewRows(rows, DEPARTURE_ROWS_QUERY);
        });
    }

    @Test
    public void testHeadMissDepartureCrashAheadOfTheFrontierTruncateRebuildsWhenTheSpliceKeptARootAboveTheRow() throws Exception {
        // A crash between the marker the head-miss rebuild's frontier truncate writes and the
        // truncate's publication leaves the splice's generation under that marker. The truncate
        // runs after the replacement commit, so its marker records no seqTxn and reads as live:
        // the restart rebuilds from the applied base without trying the splice's generation, whose
        // root of the departed rows sits above the kept row. The base lost nothing here, and the
        // rebuild gives what a recompute gives.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final DepartureMarkerCrash crash = new DepartureMarkerCrash();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(crash, () -> {
                createHeadMissDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    commitHeadMissDepartureFixture(job, false, 59);
                    crashAtTheHeadMissFrontierTruncateMarker(crash, job, HEAD_MISS_KEPT_ROW_DEPARTURE);
                }
                restoreTheCrashImageAndRestart(crash);
                capture.drain();
                capture.assertLogged("live view restart rebuilding from applied base [view=lv, cause=prefix preservation repair marker present");
                capture.assertNotLogged("does not match durable materialization");
                assertRebuiltFromAppliedBase("lv");
                Assert.assertFalse(viewInstance().isCheckpointRecoveryBlocked());
                assertViewRows(HEAD_MISS_KEPT_ROW_DEPARTURE_ROWS, DEPARTURE_ROWS_QUERY);
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
    public void testHeadMissDepartureCrashAheadOfTheFrontierTruncateRefusesTheRebuildWhenTheRootAtTheFrontierCountsEveryRow() throws Exception {
        // The same crash after a departure that emitted nothing above a root at the row that
        // became the frontier, which counts every row the view holds. The splice's generation
        // keeps the departed row's root above it, where a restore would leave it to refuse every
        // seal below it. The marker reads as live, so each restart rebuilds from the applied base,
        // which the restatement guard refuses over the day the base lost: the view stops with
        // every row.
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
            assertRebuildBlockedByTheRepairMarker(rows, HEAD_MISS_OWED_ROOT_HISTORY_FLOOR);
            assertRestartsBlockedByTheRepairMarker(rows, HEAD_MISS_OWED_ROOT_HISTORY_FLOOR);
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
        assertHeadMissDepartureFrontierLookupThatFailsRetiresTheTimeline(false);
    }

    @Test
    public void testHeadMissDepartureFrontierLookupThatFailsRetiresTheTimelineOverANanosecondBase() throws Exception {
        // The same departure over a base whose designated timestamp has nanosecond precision: the
        // line the failed read logs prints the frontier in nanoseconds.
        assertHeadMissDepartureFrontierLookupThatFailsRetiresTheTimeline(true);
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
    public void testHeadMissDepartureFrontierTruncateWhosePublicationAndRetireFailKeepsTheRowsOfTheDayTheBaseLost() throws Exception {
        // The head-miss rebuild's frontier truncate fails after writing its marker and its
        // metadata, and the retire that answers it removes nothing, so the splice's generation
        // stays on disk under the truncate's marker in a running process. The marker records no
        // seqTxn, so the in-place recovery a base column brings declines that generation, which
        // holds no root at the kept row, and the restatement guard refuses the rebuild over the
        // day the base lost: the view stops with every row, and so does each restart.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertFrontierTruncateWhosePublicationAndRetireFailKeepsTheRows(true);
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

    @Test
    public void testASecondRepairInsideTheOwedWindowResolvesItsOwnMarker() throws Exception {
        // A second late row before the owed root lands repairs over the live marker: its own
        // marker replaces it, over the generation the first splice published, and its own closing
        // seal lands the root both repairs owed, which resolves it.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                final long generationBefore = generation(instance);
                failTheOwedResumeSeal(job, fault, LiveViewCheckpointDiskFullFault.OP_ALLOCATE, "('2026-01-01T00:01:30.000000Z', 'a', 1.0)");
                assertRepairMarkerLive(instance, generationBefore);
                commitDeparture(job, "('2026-01-01T00:01:45.000000Z', 'a', 1.0)");
                Assert.assertEquals("the first splice, the second, and the second's seal", generationBefore + 3, generation(instance));
                Assert.assertEquals(ts("2026-01-01T00:04:00.000000Z"), instance.getHeadCheckpointMaxTs());
                assertNoRepairMarker(instance);
                assertNoRefreshFaults("lv");
            }
            for (int restart = 0; restart < 2; restart++) {
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(OWED_ROOT_SECOND_LATE_ROW_ROWS, DEPARTURE_ROWS_QUERY);
            }
        });
    }

    @Test
    public void testASecondRepairWhoseSealFailsInsideTheOwedWindowKeepsItsOwnMarker() throws Exception {
        // The second repair's closing seal fails too: its marker, over the generation the first
        // splice published, stays live under its own splice, and a restart stops the view with
        // every row over the day the base lost.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                final long generationBefore = generation(instance);
                failTheOwedResumeSeal(job, fault, LiveViewCheckpointDiskFullFault.OP_ALLOCATE, "('2026-01-01T00:01:30.000000Z', 'a', 1.0)");
                failTheOwedResumeSeal(job, fault, LiveViewCheckpointDiskFullFault.OP_ALLOCATE, "('2026-01-01T00:01:45.000000Z', 'a', 1.0)");
                assertRepairMarkerLive(instance, generationBefore + 1);
                assertNoRefreshFaults("lv");
            }
            assertRestartsBlockedByTheRepairMarker(OWED_ROOT_SECOND_LATE_ROW_ROWS, OWED_ROOT_HISTORY_FLOOR);
        });
    }

    @Test
    public void testHeadMissOwedRootLandsWithTheNextRowsFirstCheckpoint() throws Exception {
        // The head-miss rebuild clears the head, so the next row seals a first checkpoint at once.
        // It lands at the frontier two generations past the marker's, which reads the marker as
        // stale, and the restart clears it and restores.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitFiveRowsAboveTheHeadMissRoot(job);
                final long generationBefore = generation(instance);
                failTheOwedHeadMissSeal(job, fault);
                assertRepairMarkerLive(instance, generationBefore);
                commitDeparture(job, minuteRowsOfA(56, 56, "1.0"));
                Assert.assertEquals(ts("2026-01-01T00:56:00.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals("the splice and the first checkpoint", generationBefore + 2, generation(instance));
                final LongList ladder = snapshotCheckpointLadder(instance);
                Assert.assertEquals("the first checkpoint seals the frontier", ts("2026-01-01T00:56:00.000000Z"), ladder.getQuick(ladder.size() - 2));
                Assert.assertEquals("and counts every row of the view", 59, ladder.getQuick(ladder.size() - 1));
                assertRepairMarkerStale(instance, generationBefore);
                assertNoRefreshFaults("lv");
            }
            for (int restart = 0; restart < 2; restart++) {
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertNoRepairMarker(viewInstance());
                assertViewRows(headMissRowsWithTheLateRow(56), DEPARTURE_ROWS_QUERY);
            }
        });
    }

    @Test
    public void testHeadMissSealThatFailsAgainOnTheNextRowBlocksTheRestart() throws Exception {
        // The fault outlasts the head-miss rebuild's closing seal and fails the next row's first
        // checkpoint too: no root reaches the frontier, the marker stays live, and the restart
        // stops the view with every row over the day the base lost.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitFiveRowsAboveTheHeadMissRoot(job);
                final long generationBefore = generation(instance);
                final long sealFailuresBefore = instance.getCheckpointSealFailures();
                failTheOwedHeadMissSeal(job, fault);
                fault.arm(LiveViewCheckpointDiskFullFault.OP_ALLOCATE, LiveViewCheckpointLayout.DATA_DIR_NAME, 1);
                commitDeparture(job, minuteRowsOfA(56, 56, "1.0"));
                fault.disarm();
                Assert.assertNotNull("the first checkpoint must reach its data segment", fault.getFailedFile());
                Assert.assertEquals(sealFailuresBefore + 2, instance.getCheckpointSealFailures());
                Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                assertRepairMarkerLive(instance, generationBefore);
                assertNoRefreshFaults("lv");
            }
            assertRestartsBlockedByTheRepairMarker(headMissRowsWithTheLateRow(56), HEAD_MISS_OWED_ROOT_HISTORY_FLOOR);
        });
    }

    @Test
    public void testHeadMissThatDeclinesItsSpliceLogsItsFloorInTheViewsUnit() throws Exception {
        assertHeadMissThatDeclinesItsSpliceTruncates(false);
    }

    @Test
    public void testHeadMissThatDeclinesItsSpliceLogsItsFloorInTheViewsUnitOverANanosecondBase() throws Exception {
        assertHeadMissThatDeclinesItsSpliceTruncates(true);
    }

    @Test
    public void testHeadMissSpliceAboveTheFrontierLogsItsTruncateInTheViewsUnit() throws Exception {
        assertHeadMissSpliceAboveTheFrontierTruncatesAtIt(false);
    }

    @Test
    public void testHeadMissSpliceAboveTheFrontierLogsItsTruncateInTheViewsUnitOverANanosecondBase() throws Exception {
        assertHeadMissSpliceAboveTheFrontierTruncatesAtIt(true);
    }

    @Test
    public void testHeadMissSealThatFailsOverAnOwedRootBlocksARestartBeforeAnyRow() throws Exception {
        // The head-miss rebuild's end-of-table splice keeps its newest root at 00:50, below the
        // frontier 00:55, and its closing seal fails: the marker stays live, and the rebuild logs the
        // frontier it owes a root and the splice's newest root once. A restart before any row
        // rebuilds from the applied base, which the restatement guard refuses over the day the base
        // lost, keeping every row.
        assertHeadMissSealThatFailsOverAnOwedRootBlocksARestartBeforeAnyRow(false);
    }

    @Test
    public void testHeadMissSealThatFailsOverAnOwedRootBlocksARestartBeforeAnyRowOverANanosecondBase() throws Exception {
        // The same rebuild over a base whose designated timestamp has nanosecond precision: the line
        // it logs once prints the frontier and the splice's newest root in nanoseconds.
        assertHeadMissSealThatFailsOverAnOwedRootBlocksARestartBeforeAnyRow(true);
    }

    @Test
    public void testHeadMissOwedRootRetiredByTheFirstFailureAfterThePauseLogsNoKeptMarker() throws Exception {
        // Past the cadence, three rows the view's filter drops fail their seals at the frontier
        // 00:55, which pauses the seal. The fault persists past the pause: the late row 00:49:30
        // after it reaches the head-miss rebuild, whose closing seal fails. That fourth failure
        // retires the timeline and its marker, so the rebuild has no marker left to report it kept,
        // and the newest row is the newest timestamp the view has seen, so it owes no root either.
        // Each restart then finds no timeline, and the rebuild from the applied base is refused over
        // the day the base lost: the view stops with every row it holds.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        final LogCapture capture = new LogCapture();
        try {
            assertMemoryLeak(fault, () -> {
                createHeadMissDepartureView(false);
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    final LiveViewInstance instance = commitFiveRowsAboveTheHeadMissRoot(job, false);
                    final long sealFailuresBefore = instance.getCheckpointSealFailures();
                    final long resetsBefore = instance.getCheckpointTimelineResets();
                    setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                    for (int second = 10; second <= 30; second += 10) {
                        fault.arm(LiveViewCheckpointDiskFullFault.OP_ALLOCATE, LiveViewCheckpointLayout.DATA_DIR_NAME, 1);
                        commitDeparture(job, "('2026-01-01T00:55:" + second + ".000000Z', 'b', -1.0)");
                        fault.disarm();
                        Assert.assertNotNull("the seal at the frontier must reach its data segment", fault.getFailedFile());
                    }
                    Assert.assertEquals(sealFailuresBefore + 3, instance.getCheckpointSealFailures());
                    Assert.assertTrue("the third failure pauses the seal", instance.isSealOnCooldown(currentMicros));
                    Assert.assertEquals("and retires nothing", resetsBefore, instance.getCheckpointTimelineResets());

                    setCurrentMicros(currentMicros + 61_000_000L);
                    final long headMissRepairsBefore = countHeadMissRepairs(job);
                    hooks(job).setSimulateHeadMissHeadSealStart(
                            () -> fault.arm(LiveViewCheckpointDiskFullFault.OP_ALLOCATE, LiveViewCheckpointLayout.DATA_DIR_NAME, 1)
                    );
                    capture.start();
                    commitDeparture(job, "('2026-01-01T00:49:30.000000Z', 'a', 1.0)");
                    capture.drain();
                    capture.stop();
                    hooks(job).setSimulateHeadMissHeadSealStart(null);
                    fault.disarm();
                    Assert.assertEquals("the head-miss rebuild repairs the late row", headMissRepairsBefore + 1, countHeadMissRepairs(job));
                    Assert.assertNotNull("the closing seal after the pause is attempted", fault.getFailedFile());
                    Assert.assertEquals(sealFailuresBefore + 4, instance.getCheckpointSealFailures());
                    Assert.assertEquals("one retire", resetsBefore + 1, instance.getCheckpointTimelineResets());
                    assertTimelineAndMarkerRetired(instance);
                    Assert.assertEquals(1, capture.countOccurrences("live view checkpoint seal keeps failing, retiring the timeline and backing off "
                            + "[view=lv, consecutiveFailures=4, backoffMicros=120000000]"));
                    Assert.assertEquals(0, capture.countOccurrences("live view O3 repair kept its repair marker"));
                    Assert.assertEquals(0, capture.countOccurrences("live view O3 repair owes a root no seal can land"));
                    assertNoRefreshFaults("lv");
                }
                assertRestartsBlocked(headMissRowsWithTheLateRow(55), HEAD_MISS_OWED_ROOT_HISTORY_FLOOR, "timeline is absent");
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testHeadMissSpliceWhoseNewestRootSitsAtTheFrontierClearsItsMarkerWhenTheRestampPassesACooldown() throws Exception {
        // The splice's newest root, 00:50, is the frontier: the generation is restorable as it
        // stands, so the marker clears, and each restart restores over the day the base lost. The
        // closing seal there only re-stamps the head, which writes nothing, so a cooldown lets it
        // through, and the head goes back on the root at the frontier.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitHeadMissDepartureFixture(job, true, 50);
                final LiveViewInstance instance = viewInstance();
                final long generationBefore = generation(instance);
                final long sealFailuresBefore = instance.getCheckpointSealFailures();
                final long headMissRepairsBefore = countHeadMissRepairs(job);
                final AtomicBoolean hasReachedTheSeal = new AtomicBoolean();
                // A cooldown that ends one microsecond on, so it holds for this seal alone.
                hooks(job).setSimulateHeadMissHeadSealStart(() -> {
                    instance.setSealCooldownUntilUs(currentMicros + 1);
                    hasReachedTheSeal.set(true);
                });
                commitDeparture(job, "('2026-01-01T00:49:30.000000Z', 'a', 1.0)");
                hooks(job).setSimulateHeadMissHeadSealStart(null);
                Assert.assertTrue("the head-miss rebuild must reach its head seal", hasReachedTheSeal.get());
                Assert.assertEquals(headMissRepairsBefore + 1, countHeadMissRepairs(job));
                Assert.assertEquals("the re-stamp is no failed seal", sealFailuresBefore, instance.getCheckpointSealFailures());
                Assert.assertEquals(ts("2026-01-01T00:50:00.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals("at the repair's commit", lastBaseSeqTxn(), instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals("the splice alone", generationBefore + 1, generation(instance));
                assertCheckpointLadder(instance, false, "2025-12-31T23:55:00.000000Z -> 1\n2026-01-01T00:50:00.000000Z -> 53\n");
                assertNoRepairMarker(instance);
                assertNoRefreshFaults("lv");
            }
            for (int restart = 0; restart < 2; restart++) {
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(headMissRowsWithTheLateRow(50), DEPARTURE_ROWS_QUERY);
            }
        });
    }

    @Test
    public void testResumeAtACadenceOfZeroOwesNoRoot() throws Exception {
        assertResumeUnderASealPerFlushOwesNoRoot(0);
    }

    @Test
    public void testResumeAtANegativeCadenceOwesNoRoot() throws Exception {
        assertResumeUnderASealPerFlushOwesNoRoot(-1);
    }

    @Test
    public void testResumeOwedRootAboveTheNewestRowLandsWithTheNextRow() throws Exception {
        // The owed state of the case below, then a row past the cadence: its seal lands at its own
        // timestamp, the newest row, two generations past the marker's, and the restart restores.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                final long generationBefore = generation(instance);
                failTheOwedResumeSeal(job, fault, LiveViewCheckpointDiskFullFault.OP_ALLOCATE, "('2026-01-01T00:04:00.000000Z', 'a', -1.0)");
                assertRepairMarkerLive(instance, generationBefore);
                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'a', 1.0)");
                Assert.assertEquals(ts("2026-01-01T00:05:00.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals("the splice and the row's seal", generationBefore + 2, generation(instance));
                assertRepairMarkerStale(instance, generationBefore);
                assertNoRefreshFaults("lv");
            }
            for (int restart = 0; restart < 2; restart++) {
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(OWED_ROOT_DEPARTED_FRONTIER_ROWS + "2026-01-01T00:05:00.000000Z\ta\t1.0\t13.0\n", DEPARTURE_ROWS_QUERY);
            }
        });
    }

    @Test
    public void testResumeOwedRootAboveTheNewestRowWaitsForARow() throws Exception {
        // An upsert moves the unsealed row 00:04 out of the filter: the resume from the parked
        // head's own root emits 00:03, the new frontier, which the failed closing seal owed a root.
        // The replay's row lowered the batch minimum, so each cycle without rows asks for a root at
        // the newest timestamp the view has seen, 00:04, above the newest row the table holds, which
        // the seal declines. The marker stays live, and a restart stops the view with every row over
        // the day the base lost.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                final long generationBefore = generation(instance);
                failTheOwedResumeSeal(job, fault, LiveViewCheckpointDiskFullFault.OP_ALLOCATE, "('2026-01-01T00:04:00.000000Z', 'a', -1.0)");
                Assert.assertEquals(ts("2026-01-01T00:04:00.000000Z"), instance.getLatestSeenTs());
                assertRepairMarkerLive(instance, generationBefore);
                for (int minute = 5; minute <= 6; minute++) {
                    final long declinesBefore = job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT);
                    setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                    commitDeparture(job, "('2026-01-01T00:0" + minute + ":00.000000Z', 'b', -1.0)");
                    Assert.assertEquals(
                            "one decline per cycle",
                            declinesBefore + 1,
                            job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT)
                    );
                    Assert.assertEquals(ts("2026-01-01T00:02:00.000000Z"), instance.getHeadCheckpointMaxTs());
                    assertRepairMarkerLive(instance, generationBefore);
                }
                assertNoRefreshFaults("lv");
            }
            assertRestartsBlockedByTheRepairMarker(OWED_ROOT_DEPARTED_FRONTIER_ROWS, OWED_ROOT_HISTORY_FLOOR);
        });
    }

    @Test
    public void testResumeOwedRootFaultThroughThePauseRetiresTheMarkerWithTheTimeline() throws Exception {
        // The fault outlasts the closing seal and the next two seals the cadence asks for: the third
        // failure in a row pauses the seal over the intact timeline, marker included. The fault
        // outlasts the pause too: the first row after it attempts once, and that fourth failure
        // retires the timeline, which takes the marker with it, and arms a two-minute cooldown. The
        // first row past that cooldown opens a fresh history, which each restart restores over the
        // day the base lost.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                final long generationBefore = generation(instance);
                final long sealFailuresBefore = instance.getCheckpointSealFailures();
                final long resetsBefore = instance.getCheckpointTimelineResets();
                failTheOwedResumeSeal(job, fault, LiveViewCheckpointDiskFullFault.OP_ALLOCATE, "('2026-01-01T00:01:30.000000Z', 'a', 1.0)");
                assertRepairMarkerLive(instance, generationBefore);

                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                fault.arm(LiveViewCheckpointDiskFullFault.OP_ALLOCATE, LiveViewCheckpointLayout.DATA_DIR_NAME, 1);
                commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'a', 1.0)");
                Assert.assertNotNull("the cadence seal must reach its data segment", fault.getFailedFile());
                Assert.assertEquals(sealFailuresBefore + 2, instance.getCheckpointSealFailures());
                Assert.assertEquals(resetsBefore, instance.getCheckpointTimelineResets());
                assertRepairMarkerLive(instance, generationBefore);

                fault.arm(LiveViewCheckpointDiskFullFault.OP_ALLOCATE, LiveViewCheckpointLayout.DATA_DIR_NAME, 1);
                commitDeparture(job, "('2026-01-01T00:06:00.000000Z', 'a', 1.0)");
                fault.disarm();
                Assert.assertNotNull("the next seal must reach its data segment", fault.getFailedFile());
                Assert.assertEquals(sealFailuresBefore + 3, instance.getCheckpointSealFailures());
                Assert.assertEquals("the third failure pauses", resetsBefore, instance.getCheckpointTimelineResets());
                Assert.assertEquals(ts("2026-01-01T00:02:00.000000Z"), instance.getHeadCheckpointMaxTs());
                assertRepairMarkerLive(instance, generationBefore);
                assertTimelineOnDisk(instance, true);
                Assert.assertTrue("the pause holds a minute", instance.isSealOnCooldown(currentMicros + 59_000_000L));
                Assert.assertFalse(instance.isSealOnCooldown(currentMicros + 61_000_000L));

                setCurrentMicros(currentMicros + 61_000_000L);
                fault.arm(LiveViewCheckpointDiskFullFault.OP_ALLOCATE, LiveViewCheckpointLayout.DATA_DIR_NAME, 1);
                commitDeparture(job, "('2026-01-01T00:07:00.000000Z', 'a', 1.0)");
                fault.disarm();
                Assert.assertNotNull("the first row after the pause attempts the seal", fault.getFailedFile());
                Assert.assertEquals(sealFailuresBefore + 4, instance.getCheckpointSealFailures());
                Assert.assertEquals("the fourth failure retires the timeline", resetsBefore + 1, instance.getCheckpointTimelineResets());
                Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                assertTimelineAndMarkerRetired(instance);
                Assert.assertTrue("the cooldown holds two minutes", instance.isSealOnCooldown(currentMicros + 119_000_000L));
                Assert.assertFalse(instance.isSealOnCooldown(currentMicros + 121_000_000L));

                setCurrentMicros(currentMicros + 121_000_000L);
                commitDeparture(job, "('2026-01-01T00:08:00.000000Z', 'a', 1.0)");
                Assert.assertEquals("the row past the cooldown opens a fresh history", 1, generation(instance));
                assertCheckpointLadder(instance, false, "2026-01-01T00:08:00.000000Z -> 10\n");
                assertNoRefreshFaults("lv");
            }
            for (int restart = 0; restart < 2; restart++) {
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(
                        OWED_ROOT_ROWS + OWED_ROOT_ROW_0005 + OWED_ROOT_ROW_0006 + OWED_ROOT_ROW_0007 + OWED_ROOT_ROW_0008,
                        DEPARTURE_ROWS_QUERY
                );
            }
        });
    }

    @Test
    public void testANewFailureAfterAnIdlePauseRetires() throws Exception {
        // The fault clears inside the pause, and the base then idles for ten minutes: nothing
        // attempts a seal, so nothing ends the failure streak. A single fault on the next row then
        // fails the first seal after the pause, the fourth in a row, which retires the timeline.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                final long sealFailuresBefore = instance.getCheckpointSealFailures();
                final long resetsBefore = instance.getCheckpointTimelineResets();
                failThreeCadenceSeals(job, fault, instance);
                setCurrentMicros(currentMicros + 10 * Micros.MINUTE_MICROS);
                fault.arm(LiveViewCheckpointDiskFullFault.OP_ALLOCATE, LiveViewCheckpointLayout.DATA_DIR_NAME, 1);
                commitDeparture(job, "('2026-01-01T00:08:00.000000Z', 'a', 1.0)");
                fault.disarm();
                Assert.assertNotNull("the row's seal must reach its data segment", fault.getFailedFile());
                Assert.assertEquals(sealFailuresBefore + 4, instance.getCheckpointSealFailures());
                Assert.assertEquals("the streak never ended, so the fourth failure retires", resetsBefore + 1, instance.getCheckpointTimelineResets());
                assertTimelineAndMarkerRetired(instance);
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testASealFailureThatIsNoCairoExceptionPausesAtTheThirdFailure() throws Exception {
        // A seal that fails on an unchecked exception rather than a CairoException counts as a
        // failure all the same: the third in a row pauses the seal over the intact timeline. The
        // fault clears inside the pause, and the first row after it lands the seal.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final AtomicBoolean isArmed = new AtomicBoolean();
        final AtomicInteger failures = new AtomicInteger();
        final String dataSegmentPrefix = Files.SEPARATOR + LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME
                + Files.SEPARATOR + LiveViewCheckpointLayout.DATA_DIR_NAME + Files.SEPARATOR;
        final TestFilesFacadeImpl ff = new TestFilesFacadeImpl() {
            @Override
            public boolean exists(LPSZ path) {
                if (Utf8s.containsAscii(path, dataSegmentPrefix)
                        && !Utf8s.endsWithAscii(path, LiveViewCheckpointLayout.TMP_SUFFIX)
                        && isArmed.compareAndSet(true, false)) {
                    failures.incrementAndGet();
                    throw new UnsupportedOperationException("test failure on a checkpoint data segment");
                }
                return super.exists(path);
            }
        };
        assertMemoryLeak(ff, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                final long generationBefore = generation(instance);
                final long sealFailuresBefore = instance.getCheckpointSealFailures();
                final long resetsBefore = instance.getCheckpointTimelineResets();
                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                for (int minute = 5; minute <= 7; minute++) {
                    isArmed.set(true);
                    commitDeparture(job, "('2026-01-01T00:0" + minute + ":00.000000Z', 'a', 1.0)");
                    Assert.assertFalse("the seal must reach its data segment", isArmed.get());
                    Assert.assertEquals(sealFailuresBefore + minute - 4, instance.getCheckpointSealFailures());
                }
                Assert.assertEquals(3, failures.get());
                Assert.assertEquals("the third failure pauses", resetsBefore, instance.getCheckpointTimelineResets());
                Assert.assertEquals(ts("2026-01-01T00:02:00.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(generationBefore, generation(instance));
                Assert.assertTrue(instance.isSealOnCooldown(currentMicros));

                setCurrentMicros(currentMicros + 61_000_000L);
                commitDeparture(job, "('2026-01-01T00:08:00.000000Z', 'a', 1.0)");
                Assert.assertEquals(ts("2026-01-01T00:08:00.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(generationBefore + 1, generation(instance));
                Assert.assertEquals(sealFailuresBefore + 3, instance.getCheckpointSealFailures());
                assertNoRefreshFaults("lv");
            }
            for (int restart = 0; restart < 2; restart++) {
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(PAUSE_FIXTURE_ROWS + OWED_ROOT_ROW_0005 + OWED_ROOT_ROW_0006 + OWED_ROOT_ROW_0007 + OWED_ROOT_ROW_0008, DEPARTURE_ROWS_QUERY);
            }
        });
    }

    @Test
    public void testBaseSchemaChangeInsideThePauseRestoresInPlace() throws Exception {
        // A base column added inside the pause has the view recover in place, which restores its
        // runtime from the timeline the pause kept, over the day the base lost: the view stays
        // active, keeps every row and keeps refreshing.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                failThreeCadenceSeals(job, fault, instance);
                final long runtimeRestoresBefore = instance.getCheckpointRuntimeRestores();
                final long rebuildAttemptsBefore = instance.getCheckpointRebuildAttempts();
                execute("ALTER TABLE base ADD COLUMN y INT");
                drainWalQueue();
                driveRefreshToQuiescence(job);
                Assert.assertEquals("the recovery restores the runtime in place", runtimeRestoresBefore + 1, instance.getCheckpointRuntimeRestores());
                Assert.assertEquals(rebuildAttemptsBefore, instance.getCheckpointRebuildAttempts());
                commitDeparture(job, "('2026-01-01T00:08:00.000000Z', 'a', 1.0, NULL)");
                Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
                Assert.assertFalse(instance.isInvalid());
                Assert.assertEquals(rebuildAttemptsBefore, instance.getCheckpointRebuildAttempts());
            }
            assertViewRows(PAUSE_FIXTURE_ROWS + OWED_ROOT_ROW_0005 + OWED_ROOT_ROW_0006 + OWED_ROOT_ROW_0007 + OWED_ROOT_ROW_0008, DEPARTURE_ROWS_QUERY);
        });
    }

    @Test
    public void testCadenceSealFaultSpanningThreeFlushesKeepsTheTimelineForARestart() throws Exception {
        assertCadenceSealFaultSpanningThreeFlushesKeepsTheTimeline(true);
    }

    @Test
    public void testCadenceSealFaultSpanningThreeFlushesOverACompleteBaseKeepsTheTimelineForARestart() throws Exception {
        assertCadenceSealFaultSpanningThreeFlushesKeepsTheTimeline(false);
    }

    @Test
    public void testDepartureInsideThePauseRestampsItsFrontierRootAndReleasesBaseWalAfterIt() throws Exception {
        // Three failed publications pause the seal over the head at 00:02, and the fault clears. A
        // departure of 00:02 inside the pause truncates the timeline at the new frontier 00:01,
        // which keeps its root, and the re-stamp there writes nothing, so the pause lets it
        // through: the head goes back on that root. A rejected row inside the pause publishes
        // nothing. The first one after it republishes the root at its commit though the cadence
        // is not due, which moves the base WAL floor, and each restart restores.
        configureWalPurge();
        final LogCapture capture = new LogCapture();
        try {
            assertMemoryLeak(() -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    final LiveViewInstance instance = commitThreeRoots(job);
                    final long resetsBefore = instance.getCheckpointTimelineResets();
                    capture.start();
                    failThreePublications(job, instance);
                    capture.drain();
                    capture.stop();
                    capture.assertLogged("live view checkpoint seal keeps failing, keeping the timeline and backing off "
                            + "[view=lv, consecutiveFailures=3, backoffMicros=60000000]");
                    Assert.assertEquals(0, capture.countOccurrences("retiring the timeline"));
                    job.setCheckpointTimelineTestFailureStage(0);

                    departTheNewestRootInsideThePause(job, instance);
                    final long floorAfterTheRepair = instance.getCheckpointTimelineWalPurgeFloor();

                    final long generationBefore = generation(instance);
                    commitDeparture(job, "('2026-01-01T00:02:40.000000Z', 'b', -1.0)");
                    Assert.assertEquals("the pause holds the publication", generationBefore, generation(instance));
                    Assert.assertEquals(8, instance.getHeadCheckpointBaseSeqTxn());

                    setCurrentMicros(currentMicros + 61_000_000L);
                    commitDeparture(job, "('2026-01-01T00:02:50.000000Z', 'b', -1.0)");
                    Assert.assertEquals("the first cycle after the pause publishes", generationBefore + 1, generation(instance));
                    Assert.assertEquals(10, instance.getHeadCheckpointBaseSeqTxn());
                    Assert.assertEquals(ts("2026-01-01T00:01:00.000000Z"), instance.getHeadCheckpointMaxTs());
                    Assert.assertEquals(resetsBefore, instance.getCheckpointTimelineResets());
                    Assert.assertFalse(instance.isSealOnCooldown(currentMicros));
                    // The older superblock slot is the repair's generation until a second publication.
                    Assert.assertEquals(floorAfterTheRepair, instance.getCheckpointTimelineWalPurgeFloor());
                    setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                    commitDeparture(job, "('2026-01-01T00:02:55.000000Z', 'b', -1.0)");
                    Assert.assertEquals(11, instance.getHeadCheckpointBaseSeqTxn());
                    Assert.assertEquals("the floor rises above the one the repair left", 10, instance.getCheckpointTimelineWalPurgeFloor());
                    Assert.assertTrue(floorAfterTheRepair < 10);
                    Assert.assertEquals("the purge must keep only the segments above the floor", 11 - 10, purgeBaseWalAndCountSegments());
                    assertNoRefreshFaults("lv");
                }
                for (int restart = 0; restart < 2; restart++) {
                    restartAndRefresh();
                    assertRestoredFromTimeline("lv");
                    assertViewRows(DEPARTURE_ROWS_OVER_THE_LOST_DAY, DEPARTURE_ROWS_QUERY);
                }
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testDepartureInsideThePauseUnderAPersistingFaultRetiresAtTheFirstPublicationAfterIt() throws Exception {
        // As above, with the fault back after the departure: the pause holds the publication, and
        // the first one after it fails, the fourth failure in a row, which retires the timeline and
        // releases both base WAL arms. The first kept row past the two-minute cooldown opens a fresh
        // history, which each restart restores over the day the base lost.
        final LogCapture capture = new LogCapture();
        try {
            assertMemoryLeak(() -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    final LiveViewInstance instance = commitThreeRoots(job);
                    final long sealFailuresBefore = instance.getCheckpointSealFailures();
                    final long resetsBefore = instance.getCheckpointTimelineResets();
                    failThreePublications(job, instance);
                    job.setCheckpointTimelineTestFailureStage(0);
                    departTheNewestRootInsideThePause(job, instance);

                    job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH);
                    commitDeparture(job, "('2026-01-01T00:02:40.000000Z', 'b', -1.0)");
                    Assert.assertEquals("the pause holds the publication", sealFailuresBefore + 3, instance.getCheckpointSealFailures());

                    setCurrentMicros(currentMicros + 61_000_000L);
                    capture.start();
                    commitDeparture(job, "('2026-01-01T00:02:50.000000Z', 'b', -1.0)");
                    capture.drain();
                    capture.stop();
                    job.setCheckpointTimelineTestFailureStage(0);
                    Assert.assertEquals(sealFailuresBefore + 4, instance.getCheckpointSealFailures());
                    Assert.assertEquals("the fourth failure retires", resetsBefore + 1, instance.getCheckpointTimelineResets());
                    Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                    assertTimelineAndMarkerRetired(instance);
                    capture.assertLogged("live view checkpoint seal keeps failing, retiring the timeline and backing off "
                            + "[view=lv, consecutiveFailures=4, backoffMicros=120000000]");
                    Assert.assertTrue(instance.isSealOnCooldown(currentMicros + 119_000_000L));
                    Assert.assertFalse(instance.isSealOnCooldown(currentMicros + 121_000_000L));

                    setCurrentMicros(currentMicros + 121_000_000L);
                    commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'a', 1.0)");
                    Assert.assertEquals("the kept row opens a fresh history", 1, generation(instance));
                    assertCheckpointLadder(instance, false, "2026-01-01T00:05:00.000000Z -> 3\n");
                    assertNoRefreshFaults("lv");
                }
                for (int restart = 0; restart < 2; restart++) {
                    restartAndRefresh();
                    assertRestoredFromTimeline("lv");
                    assertViewRows(DEPARTURE_ROWS_OVER_THE_LOST_DAY + "2026-01-01T00:05:00.000000Z\ta\t1.0\t3.0\n", DEPARTURE_ROWS_QUERY);
                }
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testDropAndRecreateInsideThePauseStartsWithoutACooldown() throws Exception {
        // The pause is the instance's own state: a view dropped and created again under the same
        // name inside it seals on its first flush, with no failure on its streak.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                failThreeCadenceSeals(job, fault, instance);
                execute("DROP LIVE VIEW lv");
                drainWalQueue();
                driveRefreshToQuiescence(job);
                execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " + DEPARTURE_VIEW_SQL);
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);
                final LiveViewInstance recreated = viewInstance();
                Assert.assertNotSame(instance, recreated);
                Assert.assertEquals(0, recreated.getCheckpointSealFailures());
                Assert.assertEquals(0, recreated.getSealFailureStreak());
                Assert.assertFalse(recreated.isSealOnCooldown(currentMicros));
                Assert.assertEquals("the first flush seals", ts("2026-01-01T00:07:00.000000Z"), recreated.getHeadCheckpointMaxTs());
                Assert.assertEquals(1, generation(recreated));
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testHeadMissSealFaultThroughTwoFirstCheckpointsPausesWithTheMarkerLive() throws Exception {
        // The head-miss rebuild's closing seal fails, then the first checkpoints of the next two
        // rows: the third failure in a row pauses the seal with the head clear, the timeline and the
        // marker live. A row inside the pause attempts nothing. The fault clears, and the first row
        // after the pause lands its first checkpoint at the frontier, two generations past the
        // marker's, so each restart restores.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitFiveRowsAboveTheHeadMissRoot(job);
                final long generationBefore = generation(instance);
                final long sealFailuresBefore = instance.getCheckpointSealFailures();
                final long resetsBefore = instance.getCheckpointTimelineResets();
                failTheOwedHeadMissSeal(job, fault);
                for (int minute = 56; minute <= 57; minute++) {
                    fault.arm(LiveViewCheckpointDiskFullFault.OP_ALLOCATE, LiveViewCheckpointLayout.DATA_DIR_NAME, 1);
                    commitDeparture(job, minuteRowsOfA(minute, minute, "1.0"));
                    fault.disarm();
                    Assert.assertNotNull("the first checkpoint must reach its data segment", fault.getFailedFile());
                }
                Assert.assertEquals(sealFailuresBefore + 3, instance.getCheckpointSealFailures());
                Assert.assertEquals("the third failure pauses", resetsBefore, instance.getCheckpointTimelineResets());
                Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                assertTimelineOnDisk(instance, true);
                assertRepairMarkerLive(instance, generationBefore);

                fault.arm(LiveViewCheckpointDiskFullFault.OP_ALLOCATE, LiveViewCheckpointLayout.DATA_DIR_NAME, 1);
                commitDeparture(job, minuteRowsOfA(58, 58, "1.0"));
                fault.disarm();
                Assert.assertNull("a row inside the pause attempts nothing", fault.getFailedFile());
                Assert.assertEquals(sealFailuresBefore + 3, instance.getCheckpointSealFailures());
                assertRepairMarkerLive(instance, generationBefore);

                setCurrentMicros(currentMicros + 61_000_000L);
                commitDeparture(job, minuteRowsOfA(59, 59, "1.0"));
                Assert.assertEquals(ts("2026-01-01T00:59:00.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals("the splice and the first checkpoint", generationBefore + 2, generation(instance));
                assertRepairMarkerStale(instance, generationBefore);
                assertNoRefreshFaults("lv");
            }
            for (int restart = 0; restart < 2; restart++) {
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(headMissRowsWithTheLateRow(59), DEPARTURE_ROWS_QUERY);
            }
        });
    }

    @Test
    public void testLateRowsInsideThePauseHoldTheirClosingSealsAndTheFirstRowAfterItRetires() throws Exception {
        // Three failed cadence seals pause the seal. Two late rows inside the pause each splice the
        // timeline, and the pause holds each one's closing seal: the fault armed for it stays unmet,
        // and the marker stays live, as the newest timestamp the view has seen is the newest row,
        // where a seal can land, and each repair logs that it kept it. The fault persists: the first
        // row after the pause attempts once, and that fourth failure retires the timeline and the
        // marker with it. The first row past the two-minute cooldown opens a fresh history, which
        // each restart restores.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        final LogCapture capture = new LogCapture();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                    final long sealFailuresBefore = instance.getCheckpointSealFailures();
                    final long resetsBefore = instance.getCheckpointTimelineResets();
                    failThreeCadenceSeals(job, fault, instance);
                    for (String lateRow : new String[]{"00:01:10", "00:01:20"}) {
                        final long generationBefore = generation(instance);
                        final long resumesBefore = job.getStatsForTest().getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RESUME_FROM_ANCHOR);
                        hooks(job).setSimulateResumeHeadSealStart(
                                () -> fault.arm(LiveViewCheckpointDiskFullFault.OP_ALLOCATE, LiveViewCheckpointLayout.DATA_DIR_NAME, 1)
                        );
                        capture.start();
                        commitDeparture(job, "('2026-01-01T" + lateRow + ".000000Z', 'a', 1.0)");
                        capture.drain();
                        capture.stop();
                        hooks(job).setSimulateResumeHeadSealStart(null);
                        fault.disarm();
                        Assert.assertEquals(resumesBefore + 1, job.getStatsForTest().getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RESUME_FROM_ANCHOR));
                        Assert.assertNull("the pause holds the closing seal", fault.getFailedFile());
                        Assert.assertEquals(
                                1,
                                capture.countOccurrences("live view O3 repair kept its repair marker, no root at the frontier [view=lv, "
                                        + "frontier=2026-01-01T00:07:00.000000Z, spliceHeadRootMaxTs=2026-01-01T00:02:00.000000Z]")
                        );
                        Assert.assertEquals(sealFailuresBefore + 3, instance.getCheckpointSealFailures());
                        Assert.assertEquals(resetsBefore, instance.getCheckpointTimelineResets());
                        Assert.assertEquals(ts("2026-01-01T00:07:00.000000Z"), instance.getLatestSeenTs());
                        Assert.assertEquals(ts("2026-01-01T00:02:00.000000Z"), instance.getHeadCheckpointMaxTs());
                        assertRepairMarkerLive(instance, generationBefore);
                        assertTimelineOnDisk(instance, true);
                    }

                    setCurrentMicros(currentMicros + 61_000_000L);
                    fault.arm(LiveViewCheckpointDiskFullFault.OP_ALLOCATE, LiveViewCheckpointLayout.DATA_DIR_NAME, 1);
                    capture.start();
                    commitDeparture(job, "('2026-01-01T00:08:00.000000Z', 'a', 1.0)");
                    capture.drain();
                    capture.stop();
                    fault.disarm();
                    Assert.assertNotNull("the first row after the pause attempts the seal", fault.getFailedFile());
                    Assert.assertEquals("exactly one attempt", sealFailuresBefore + 4, instance.getCheckpointSealFailures());
                    Assert.assertEquals(resetsBefore + 1, instance.getCheckpointTimelineResets());
                    Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                    assertTimelineAndMarkerRetired(instance);
                    capture.assertLogged("live view checkpoint seal keeps failing, retiring the timeline and backing off "
                            + "[view=lv, consecutiveFailures=4, backoffMicros=120000000]");

                    setCurrentMicros(currentMicros + 121_000_000L);
                    commitDeparture(job, "('2026-01-01T00:09:00.000000Z', 'a', 1.0)");
                    Assert.assertEquals("the row past the cooldown opens a fresh history", 1, generation(instance));
                    assertCheckpointLadder(instance, false, "2026-01-01T00:09:00.000000Z -> 12\n");
                    assertNoRefreshFaults("lv");
                }
                final String rows = """
                        ts\tsym\tx\ts
                        2025-12-31T12:00:00.000000Z\ta\t0.5\t0.5
                        2026-01-01T00:01:00.000000Z\ta\t1.5\t2.0
                        2026-01-01T00:01:10.000000Z\ta\t1.0\t3.0
                        2026-01-01T00:01:20.000000Z\ta\t1.0\t3.5
                        2026-01-01T00:02:00.000000Z\ta\t2.5\t4.5
                        2026-01-01T00:03:00.000000Z\ta\t9.5\t13.0
                        2026-01-01T00:04:00.000000Z\ta\t1.0\t13.0
                        """ + OWED_ROOT_ROW_0005 + OWED_ROOT_ROW_0006 + OWED_ROOT_ROW_0007 + OWED_ROOT_ROW_0008
                        + "2026-01-01T00:09:00.000000Z\ta\t1.0\t3.0\n";
                for (int restart = 0; restart < 2; restart++) {
                    restartAndRefresh();
                    assertRestoredFromTimeline("lv");
                    assertViewRows(rows, DEPARTURE_ROWS_QUERY);
                }
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testANoCaptureResumeInsideThePauseRetiresItsTruncatedTimeline() throws Exception {
        // With no chained boundary allowed, the resume of a late row inside the pause captures
        // nothing and truncates the timeline at its output floor. The pause holds the closing seal
        // that would have put a fresh head over the truncated prefix, so the route retires the
        // truncated timeline. The first row after the pause opens a fresh history.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 0);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                final long sealFailuresBefore = instance.getCheckpointSealFailures();
                final long resetsBefore = instance.getCheckpointTimelineResets();
                failThreeCadenceSeals(job, fault, instance);
                final long resumesBefore = job.getStatsForTest().getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RESUME_FROM_ANCHOR);
                commitDeparture(job, "('2026-01-01T00:01:30.000000Z', 'a', 1.0)");
                Assert.assertEquals(resumesBefore + 1, job.getStatsForTest().getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RESUME_FROM_ANCHOR));
                Assert.assertEquals("the route retires the truncated timeline", resetsBefore + 1, instance.getCheckpointTimelineResets());
                Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                assertTimelineAndMarkerRetired(instance);
                Assert.assertEquals("the pause held the seal", sealFailuresBefore + 3, instance.getCheckpointSealFailures());
                Assert.assertTrue(instance.isSealOnCooldown(currentMicros));

                setCurrentMicros(currentMicros + 61_000_000L);
                commitDeparture(job, "('2026-01-01T00:08:00.000000Z', 'a', 1.0)");
                Assert.assertEquals("the row after the pause opens a fresh history", 1, generation(instance));
                assertCheckpointLadder(instance, false, "2026-01-01T00:08:00.000000Z -> 10\n");
                Assert.assertEquals("and ends the failure streak", 0, instance.getSealFailureStreak());
                Assert.assertEquals(sealFailuresBefore + 3, instance.getCheckpointSealFailures());
                assertNoRefreshFaults("lv");
            }
            for (int restart = 0; restart < 2; restart++) {
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(OWED_ROOT_ROWS + OWED_ROOT_ROW_0005 + OWED_ROOT_ROW_0006 + OWED_ROOT_ROW_0007 + OWED_ROOT_ROW_0008, DEPARTURE_ROWS_QUERY);
            }
        });
    }

    @Test
    public void testOwedRootThatADepartureStrandsAfterThePauseRetiresOnce() throws Exception {
        // Three failed seals pause the seal, and the fault persists past the pause. The departure of
        // 00:04 after it attempts its closing seal, which fails: that fourth failure retires the
        // timeline and its marker, so the repair finds no timeline left to retire for the root it
        // owes, and no marker left to report it kept.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        final LogCapture capture = new LogCapture();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                    final long sealFailuresBefore = instance.getCheckpointSealFailures();
                    final long resetsBefore = instance.getCheckpointTimelineResets();
                    failThreeRowlessSealsAtTheFrontier(job, fault, instance);
                    setCurrentMicros(currentMicros + 61_000_000L);
                    hooks(job).setSimulateResumeHeadSealStart(
                            () -> fault.arm(LiveViewCheckpointDiskFullFault.OP_ALLOCATE, LiveViewCheckpointLayout.DATA_DIR_NAME, 1)
                    );
                    capture.start();
                    commitDeparture(job, "('2026-01-01T00:04:00.000000Z', 'a', -1.0)");
                    capture.drain();
                    capture.stop();
                    hooks(job).setSimulateResumeHeadSealStart(null);
                    fault.disarm();
                    Assert.assertNotNull("the closing seal after the pause is attempted", fault.getFailedFile());
                    Assert.assertEquals(sealFailuresBefore + 4, instance.getCheckpointSealFailures());
                    Assert.assertEquals("one retire", resetsBefore + 1, instance.getCheckpointTimelineResets());
                    Assert.assertEquals(1, capture.countOccurrences("live view checkpoint seal keeps failing, retiring the timeline and backing off "
                            + "[view=lv, consecutiveFailures=4, backoffMicros=120000000]"));
                    Assert.assertEquals(0, capture.countOccurrences("live view O3 repair owes a root no seal can land"));
                    Assert.assertEquals(0, capture.countOccurrences("live view O3 repair kept its repair marker"));
                    assertTimelineAndMarkerRetired(instance);
                    assertNoRefreshFaults("lv");
                }
                assertRestartsBlocked(OWED_ROOT_DEPARTED_FRONTIER_ROWS, OWED_ROOT_HISTORY_FLOOR, "timeline is absent");
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testOwedRootThatADepartureStrandsAtTheThirdFailureRetiresTheTimeline() throws Exception {
        // Two failed seals at the frontier 00:04, then an upsert moves 00:04 out of the filter: the
        // resume owes a root at the new frontier 00:03, and its closing seal fails, the third
        // failure in a row, which pauses the seal. The newest timestamp the view has seen, 00:04,
        // sits above the newest row, where no seal of a cycle without rows can land, so the owed
        // root would hold the base WAL until a newer row: the repair retires the timeline and its
        // marker instead. Each restart then finds no timeline, and the rebuild from the applied
        // base is refused over the day the base lost: the view stops with every row it holds.
        final LogCapture capture = new LogCapture();
        try {
            assertOwedRootThatADepartureStrandsAtTheThirdFailureRetiresTheTimeline(false, capture);
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testOwedRootThatADepartureStrandsAtTheThirdFailureRetiresTheTimelineOverANanosecondBase() throws Exception {
        // The same departure over a base whose designated timestamp has nanosecond precision: the
        // repair weighs the owed frontier 00:03 against the newest timestamp the view has seen,
        // 00:04, both in the view's own unit, and retires the timeline and its marker at the third
        // failure as it does over a microsecond base. Its line prints both in nanoseconds.
        final LogCapture capture = new LogCapture();
        try {
            assertOwedRootThatADepartureStrandsAtTheThirdFailureRetiresTheTimeline(true, capture);
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testOwedRootThatADepartureStrandsInsideThePauseRetiresTheTimeline() throws Exception {
        // Three failed seals at the frontier 00:04 pause the seal. An upsert inside the pause moves
        // 00:04 out of the filter: the resume owes a root at the new frontier 00:03, and the pause
        // holds its closing seal. No seal of a cycle without rows can land that root before a newer
        // row, and the failures have spent their budget, so the repair retires the timeline and its
        // marker. After the pause the cycles without rows hold nothing, and the first kept row opens
        // a fresh history, which each restart restores over the day the base lost.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                final long sealFailuresBefore = instance.getCheckpointSealFailures();
                final long resetsBefore = instance.getCheckpointTimelineResets();
                failThreeRowlessSealsAtTheFrontier(job, fault, instance);
                hooks(job).setSimulateResumeHeadSealStart(
                        () -> fault.arm(LiveViewCheckpointDiskFullFault.OP_ALLOCATE, LiveViewCheckpointLayout.DATA_DIR_NAME, 1)
                );
                commitDeparture(job, "('2026-01-01T00:04:00.000000Z', 'a', -1.0)");
                hooks(job).setSimulateResumeHeadSealStart(null);
                fault.disarm();
                Assert.assertNull("the pause holds the closing seal", fault.getFailedFile());
                Assert.assertEquals(sealFailuresBefore + 3, instance.getCheckpointSealFailures());
                Assert.assertEquals(resetsBefore + 1, instance.getCheckpointTimelineResets());
                Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                assertTimelineAndMarkerRetired(instance);

                setCurrentMicros(currentMicros + 61_000_000L);
                for (int minute = 5; minute <= 6; minute++) {
                    final long declinesBefore = job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT);
                    commitDeparture(job, "('2026-01-01T00:0" + minute + ":00.000000Z', 'b', -1.0)");
                    Assert.assertEquals(
                            "one decline per cycle",
                            declinesBefore + 1,
                            job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT)
                    );
                    Assert.assertEquals(sealFailuresBefore + 3, instance.getCheckpointSealFailures());
                    Assert.assertEquals("nothing holds the base WAL", Numbers.LONG_NULL, instance.getHeadCheckpointBaseSeqTxn());
                    Assert.assertEquals(Numbers.LONG_NULL, instance.getCheckpointTimelineWalPurgeFloor());
                }
                commitDeparture(job, "('2026-01-01T00:07:00.000000Z', 'a', 1.0)");
                Assert.assertEquals("the kept row opens a fresh history", 1, generation(instance));
                assertCheckpointLadder(instance, false, "2026-01-01T00:07:00.000000Z -> 5\n");
                assertNoRefreshFaults("lv");
            }
            for (int restart = 0; restart < 2; restart++) {
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(OWED_ROOT_DEPARTED_FRONTIER_ROWS + "2026-01-01T00:07:00.000000Z\ta\t1.0\t13.0\n", DEPARTURE_ROWS_QUERY);
            }
        });
    }

    @Test
    public void testPersistentSealFaultRetiresAtTheFirstFailureAfterThePause() throws Exception {
        // The fault outlasts the pause. A row inside it attempts nothing; the first row after it
        // attempts once, and that fourth failure retires the timeline, releases both base WAL arms
        // and backs off two minutes. The first row past that opens a fresh history, which each
        // restart restores over the day the base lost.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        final LogCapture capture = new LogCapture();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                    final long sealFailuresBefore = instance.getCheckpointSealFailures();
                    final long resetsBefore = instance.getCheckpointTimelineResets();
                    failThreeCadenceSeals(job, fault, instance);
                    fault.arm(LiveViewCheckpointDiskFullFault.OP_ALLOCATE, LiveViewCheckpointLayout.DATA_DIR_NAME, 1);
                    commitDeparture(job, "('2026-01-01T00:08:00.000000Z', 'a', 1.0)");
                    Assert.assertNull("a row inside the pause attempts nothing", fault.getFailedFile());
                    Assert.assertEquals(sealFailuresBefore + 3, instance.getCheckpointSealFailures());

                    setCurrentMicros(currentMicros + 61_000_000L);
                    capture.start();
                    commitDeparture(job, "('2026-01-01T00:09:00.000000Z', 'a', 1.0)");
                    capture.drain();
                    capture.stop();
                    fault.disarm();
                    Assert.assertNotNull("the first row after the pause attempts the seal", fault.getFailedFile());
                    Assert.assertEquals(sealFailuresBefore + 4, instance.getCheckpointSealFailures());
                    Assert.assertEquals(resetsBefore + 1, instance.getCheckpointTimelineResets());
                    Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                    assertTimelineAndMarkerRetired(instance);
                    capture.assertLogged("live view checkpoint seal keeps failing, retiring the timeline and backing off "
                            + "[view=lv, consecutiveFailures=4, backoffMicros=120000000]");
                    Assert.assertTrue(instance.isSealOnCooldown(currentMicros + 119_000_000L));
                    Assert.assertFalse(instance.isSealOnCooldown(currentMicros + 121_000_000L));

                    setCurrentMicros(currentMicros + 121_000_000L);
                    commitDeparture(job, "('2026-01-01T00:10:00.000000Z', 'a', 1.0)");
                    Assert.assertEquals("the row past the cooldown opens a fresh history", 1, generation(instance));
                    assertCheckpointLadder(instance, false, "2026-01-01T00:10:00.000000Z -> 11\n");
                    assertNoRefreshFaults("lv");
                }
                for (int restart = 0; restart < 2; restart++) {
                    restartAndRefresh();
                    assertRestoredFromTimeline("lv");
                    assertViewRows(
                            PAUSE_FIXTURE_ROWS + OWED_ROOT_ROW_0005 + OWED_ROOT_ROW_0006 + OWED_ROOT_ROW_0007 + OWED_ROOT_ROW_0008
                                    + "2026-01-01T00:09:00.000000Z\ta\t1.0\t3.0\n"
                                    + "2026-01-01T00:10:00.000000Z\ta\t1.0\t3.0\n",
                            DEPARTURE_ROWS_QUERY
                    );
                }
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testRestampBetweenFailedPublicationsKeepsTheFailureStreak() throws Exception {
        // Two failed publications, then a departure whose re-stamp puts the head on the root at the
        // new frontier: the re-stamp writes nothing, so it proves nothing about the fault and leaves
        // the failure streak where it was. The next failed publication is the third in a row.
        final LogCapture capture = new LogCapture();
        try {
            assertMemoryLeak(() -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    final LiveViewInstance instance = commitThreeRoots(job);
                    final long sealFailuresBefore = instance.getCheckpointSealFailures();
                    job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH);
                    setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                    commitDeparture(job, "('2026-01-01T00:02:10.000000Z', 'b', -1.0)");
                    commitDeparture(job, "('2026-01-01T00:02:20.000000Z', 'b', -1.0)");
                    Assert.assertEquals(sealFailuresBefore + 2, instance.getCheckpointSealFailures());
                    job.setCheckpointTimelineTestFailureStage(0);
                    commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', -2.5)");
                    Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT));
                    Assert.assertEquals("the re-stamp lands", ts("2026-01-01T00:01:00.000000Z"), instance.getHeadCheckpointMaxTs());
                    Assert.assertEquals(lastBaseSeqTxn(), instance.getHeadCheckpointBaseSeqTxn());
                    Assert.assertEquals("and keeps the streak", 2, instance.getSealFailureStreak());

                    job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH);
                    setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                    capture.start();
                    commitDeparture(job, "('2026-01-01T00:02:30.000000Z', 'b', -1.0)");
                    capture.drain();
                    capture.stop();
                    job.setCheckpointTimelineTestFailureStage(0);
                    Assert.assertEquals(sealFailuresBefore + 3, instance.getCheckpointSealFailures());
                    capture.assertLoggedRE("could not advance live view head checkpoint base \\[view=lv, baseSeqTxn=\\d+, consecutiveFailures=3,");
                    capture.assertLogged("live view checkpoint seal keeps failing, keeping the timeline and backing off "
                            + "[view=lv, consecutiveFailures=3, backoffMicros=60000000]");
                    assertNoRefreshFaults("lv");
                }
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testRestampRightAfterThePauseDoesNotEndTheFailureStreak() throws Exception {
        // Three failed publications pause the seal. A departure right after the pause re-stamps the
        // head on the root at the new frontier, which writes nothing and leaves the streak at three.
        // So the first publication after it is due though the cadence is not, and with the fault
        // back it fails: the fourth failure in a row, which retires the timeline.
        assertMemoryLeak(() -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = commitThreeRoots(job);
                final long sealFailuresBefore = instance.getCheckpointSealFailures();
                final long resetsBefore = instance.getCheckpointTimelineResets();
                failThreePublications(job, instance);
                setCurrentMicros(currentMicros + 61_000_000L);
                job.setCheckpointTimelineTestFailureStage(0);
                commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', -2.5)");
                Assert.assertEquals("the re-stamp lands", ts("2026-01-01T00:01:00.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(lastBaseSeqTxn(), instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(3, instance.getSealFailureStreak());

                job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH);
                commitDeparture(job, "('2026-01-01T00:02:40.000000Z', 'b', -1.0)");
                job.setCheckpointTimelineTestFailureStage(0);
                Assert.assertEquals("the publication is due and fails", sealFailuresBefore + 4, instance.getCheckpointSealFailures());
                Assert.assertEquals(resetsBefore + 1, instance.getCheckpointTimelineResets());
                assertTimelineAndMarkerRetired(instance);
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testRestartInsideThePauseOverAnOwedRootStopsTheViewWithEveryRow() throws Exception {
        // Three late rows whose closing seals fail pause the seal with the owed root's marker live,
        // and a restart inside the pause refuses the timeline the marker names: the rebuild from the
        // applied base is refused over the day the base lost, and the view stops with every row.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                failThreeLateRowSeals(job, fault, instance);
                assertNoRefreshFaults("lv");
            }
            assertRestartsBlockedByTheRepairMarker(THREE_LATE_ROWS, OWED_ROOT_HISTORY_FLOOR);
        });
    }

    @Test
    public void testRestoredHeadSealFaultSpanningThreeFlushesKeepsTheTimeline() throws Exception {
        // A restart leaves the restored head with no write time, so each flush after it seals, and
        // each fails: the third failure in a row pauses the seal over the intact timeline, and the
        // restarts restore from it.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTwoRowsAboveTheNewestRoot(job);
            }
            restartAndRefresh();
            assertRestoredFromTimeline("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = viewInstance();
                final long generationBefore = generation(instance);
                for (int minute = 5; minute <= 7; minute++) {
                    fault.arm(LiveViewCheckpointDiskFullFault.OP_ALLOCATE, LiveViewCheckpointLayout.DATA_DIR_NAME, 1);
                    commitDeparture(job, "('2026-01-01T00:0" + minute + ":00.000000Z', 'a', 1.0)");
                    fault.disarm();
                    Assert.assertNotNull("the restored head's flush must seal", fault.getFailedFile());
                    Assert.assertEquals(minute - 4, instance.getCheckpointSealFailures());
                }
                Assert.assertEquals("the third failure pauses", 0, instance.getCheckpointTimelineResets());
                Assert.assertEquals(ts("2026-01-01T00:02:00.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(generationBefore, generation(instance));
                Assert.assertTrue(instance.isSealOnCooldown(currentMicros));
                assertTimelineOnDisk(instance, true);
                assertNoRefreshFaults("lv");
            }
            for (int restart = 0; restart < 2; restart++) {
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(PAUSE_FIXTURE_ROWS + OWED_ROOT_ROW_0005 + OWED_ROOT_ROW_0006 + OWED_ROOT_ROW_0007, DEPARTURE_ROWS_QUERY);
            }
        });
    }

    @Test
    public void testSealFaultClearingInsideThePauseLandsTheNextSealOnTheKeptLadder() throws Exception {
        // The fault clears inside the pause. The first row after it seals, as the cadence that was
        // due never moved, on the ladder the pause kept, which ends the failure streak: a failure
        // after it is the first of a new one. Each restart restores over the day the base lost.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        final LogCapture capture = new LogCapture();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                    landTheSealAfterThePause(job, fault, instance);

                    setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                    fault.arm(LiveViewCheckpointDiskFullFault.OP_ALLOCATE, LiveViewCheckpointLayout.DATA_DIR_NAME, 1);
                    capture.start();
                    commitDeparture(job, "('2026-01-01T00:09:00.000000Z', 'a', 1.0)");
                    capture.drain();
                    capture.stop();
                    fault.disarm();
                    Assert.assertNotNull("the cadence seal must reach its data segment", fault.getFailedFile());
                    capture.assertLoggedRE("could not write live view head checkpoint \\[view=lv, lvSeqTxn=\\d+, consecutiveFailures=1,");
                    assertNoRefreshFaults("lv");
                }
                for (int restart = 0; restart < 2; restart++) {
                    restartAndRefresh();
                    assertRestoredFromTimeline("lv");
                    assertViewRows(
                            PAUSE_FIXTURE_ROWS + OWED_ROOT_ROW_0005 + OWED_ROOT_ROW_0006 + OWED_ROOT_ROW_0007 + OWED_ROOT_ROW_0008
                                    + "2026-01-01T00:09:00.000000Z\ta\t1.0\t3.0\n",
                            DEPARTURE_ROWS_QUERY
                    );
                }
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testSealFaultPausesAtTheThirdFailureUnderACadenceOfZero() throws Exception {
        assertSealFaultPausesWhateverTheCadence(0, 1_000_000);
    }

    @Test
    public void testSealFaultPausesAtTheThirdFailureUnderAnHourCadence() throws Exception {
        assertSealFaultPausesWhateverTheCadence(Micros.HOUR_MICROS, 1_000_000);
    }

    @Test
    public void testSealFaultPausesAtTheThirdFailureUnderANegativeCadence() throws Exception {
        assertSealFaultPausesWhateverTheCadence(-1, 1_000_000);
    }

    @Test
    public void testSealFaultPausesAtTheThirdFailureUnderARowCadenceOfOne() throws Exception {
        assertSealFaultPausesWhateverTheCadence(5 * Micros.MINUTE_MICROS, 1);
    }

    @Test
    public void testSecondFaultAfterALandedSealGetsItsOwnPause() throws Exception {
        // A seal that lands after the pause ends the failure streak and its cooldown. A new fault
        // then gets three attempts and a one-minute pause of its own, not the backoff the first
        // streak had reached.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        final LogCapture capture = new LogCapture();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                    final long sealFailuresBefore = instance.getCheckpointSealFailures();
                    final long resetsBefore = instance.getCheckpointTimelineResets();
                    landTheSealAfterThePause(job, fault, instance);
                    setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                    capture.start();
                    for (int minute = 9; minute <= 11; minute++) {
                        fault.arm(LiveViewCheckpointDiskFullFault.OP_ALLOCATE, LiveViewCheckpointLayout.DATA_DIR_NAME, 1);
                        commitDeparture(job, "('2026-01-01T00:" + (minute < 10 ? "0" : "") + minute + ":00.000000Z', 'a', 1.0)");
                        fault.disarm();
                        Assert.assertNotNull("the seal must reach its data segment", fault.getFailedFile());
                    }
                    capture.drain();
                    capture.stop();
                    Assert.assertEquals(sealFailuresBefore + 6, instance.getCheckpointSealFailures());
                    Assert.assertEquals("no retire", resetsBefore, instance.getCheckpointTimelineResets());
                    capture.assertLogged("live view checkpoint seal keeps failing, keeping the timeline and backing off "
                            + "[view=lv, consecutiveFailures=3, backoffMicros=60000000]");
                    Assert.assertEquals(0, capture.countOccurrences("retiring the timeline"));
                    Assert.assertEquals(ts("2026-01-01T00:08:00.000000Z"), instance.getHeadCheckpointMaxTs());
                    assertNoRefreshFaults("lv");
                }
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testThreeLateRowsWhoseClosingSealsFailPauseAndKeepTheLadder() throws Exception {
        // Three late rows, each repaired by the resume, whose closing seals fail: the third failure
        // in a row pauses the seal with the head parked on 00:02 and the owed root's marker live.
        // The fault clears. The first row after the pause comes inside the duration cadence and
        // seals all the same, which lands a root at the frontier two generations past the marker's:
        // the ladder stays, and each restart restores over the day the base lost.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                final long markerBaseGeneration = failThreeLateRowSeals(job, fault, instance);
                setCurrentMicros(currentMicros + 61_000_000L);
                commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'a', 1.0)");
                Assert.assertEquals(ts("2026-01-01T00:05:00.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals("the splice and the row's seal", markerBaseGeneration + 2, generation(instance));
                assertRepairMarkerStale(instance, markerBaseGeneration);
                assertCheckpointLadder(instance, false, """
                        2025-12-31T12:00:00.000000Z -> 1
                        2026-01-01T00:01:00.000000Z -> 2
                        2026-01-01T00:02:00.000000Z -> 6
                        2026-01-01T00:05:00.000000Z -> 9
                        """);
                Assert.assertEquals(0, instance.getSealFailureStreak());
                assertNoRefreshFaults("lv");
            }
            for (int restart = 0; restart < 2; restart++) {
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(THREE_LATE_ROWS + OWED_ROOT_ROW_0005, DEPARTURE_ROWS_QUERY);
            }
        });
    }

    @Test
    public void testThreeLateRowsWhoseClosingSealsKeepFailingRetireAtTheFirstRowAfterThePause() throws Exception {
        // As above, with the fault persisting: the first row after the pause attempts the seal once,
        // inside the duration cadence, and that fourth failure retires the timeline and the owed
        // root's marker with it, releasing both base WAL arms about a minute after the third.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        final LogCapture capture = new LogCapture();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                    final long sealFailuresBefore = instance.getCheckpointSealFailures();
                    final long resetsBefore = instance.getCheckpointTimelineResets();
                    failThreeLateRowSeals(job, fault, instance);
                    setCurrentMicros(currentMicros + 61_000_000L);
                    fault.arm(LiveViewCheckpointDiskFullFault.OP_ALLOCATE, LiveViewCheckpointLayout.DATA_DIR_NAME, 1);
                    capture.start();
                    commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'a', 1.0)");
                    capture.drain();
                    capture.stop();
                    fault.disarm();
                    Assert.assertNotNull("the first row after the pause attempts the seal", fault.getFailedFile());
                    Assert.assertEquals(sealFailuresBefore + 4, instance.getCheckpointSealFailures());
                    Assert.assertEquals(resetsBefore + 1, instance.getCheckpointTimelineResets());
                    assertTimelineAndMarkerRetired(instance);
                    capture.assertLogged("live view checkpoint seal keeps failing, retiring the timeline and backing off "
                            + "[view=lv, consecutiveFailures=4, backoffMicros=120000000]");
                    assertNoRefreshFaults("lv");
                }
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testWalPurgeAfterThePauseFollowsTheConsumedCommit() throws Exception {
        // The fault outlasts the pause, and the fourth failure releases both base WAL arms: the
        // purge then keeps nothing a restart of this view would replay.
        configureWalPurge();
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                failThreeCadenceSeals(job, fault, instance);
                setCurrentMicros(currentMicros + 61_000_000L);
                fault.arm(LiveViewCheckpointDiskFullFault.OP_ALLOCATE, LiveViewCheckpointLayout.DATA_DIR_NAME, 1);
                commitDeparture(job, "('2026-01-01T00:08:00.000000Z', 'a', 1.0)");
                fault.disarm();
                Assert.assertNotNull(fault.getFailedFile());
                assertTimelineAndMarkerRetired(instance);
                Assert.assertEquals(lastBaseSeqTxn(), instance.getStateReader().getLvConsumedSeqTxn());
                Assert.assertEquals("the purge keeps no segment the view holds", 0, purgeBaseWalAndCountSegments());
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testWalPurgeInsideThePauseKeepsTheBaseWalARestartReplays() throws Exception {
        // The pause keeps both base WAL arms where the failed seals left them, so the purge keeps
        // every base commit above the older of the two, which the restart replays.
        configureWalPurge();
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                final long floor = Math.min(instance.getHeadCheckpointBaseSeqTxn(), instance.getCheckpointTimelineWalPurgeFloor());
                failThreeCadenceSeals(job, fault, instance);
                Assert.assertEquals(
                        "the purge must keep the segments above the floor the failed seals left",
                        lastBaseSeqTxn() - floor,
                        purgeBaseWalAndCountSegments()
                );
                assertNoRefreshFaults("lv");
            }
            restartAndRefresh();
            assertRestoredFromTimeline("lv");
            assertViewRows(PAUSE_FIXTURE_ROWS + OWED_ROOT_ROW_0005 + OWED_ROOT_ROW_0006 + OWED_ROOT_ROW_0007, DEPARTURE_ROWS_QUERY);
        });
    }

    @Test
    public void testResumeOwedRootHoldsTheBaseWalUntilItLands() throws Exception {
        // While the marker is live, the timeline's WAL floor stays at the base coordinate of the
        // head the repair left parked, and the purge keeps every base commit above it. The owed
        // root moves the floor up once it lands, and a restart restores from the base WAL the
        // purges kept.
        configureWalPurge();
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                final long generationBefore = generation(instance);
                final long headBaseSeqTxn = instance.getHeadCheckpointBaseSeqTxn();
                failTheOwedResumeSeal(job, fault, LiveViewCheckpointDiskFullFault.OP_ALLOCATE, "('2026-01-01T00:01:30.000000Z', 'a', 1.0)");
                assertRepairMarkerLive(instance, generationBefore);
                Assert.assertEquals("the head stays parked", headBaseSeqTxn, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals("the floor stays at the parked head's base", headBaseSeqTxn, instance.getCheckpointTimelineWalPurgeFloor());
                Assert.assertEquals(lastBaseSeqTxn() - headBaseSeqTxn, purgeBaseWalAndCountSegments());

                commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'a', 1.0)");
                assertRepairMarkerLive(instance, generationBefore);
                Assert.assertEquals(headBaseSeqTxn, instance.getCheckpointTimelineWalPurgeFloor());
                Assert.assertEquals(lastBaseSeqTxn() - headBaseSeqTxn, purgeBaseWalAndCountSegments());

                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                commitDeparture(job, "('2026-01-01T00:06:00.000000Z', 'a', 1.0)");
                Assert.assertEquals(ts("2026-01-01T00:06:00.000000Z"), instance.getHeadCheckpointMaxTs());
                final long floorAfterTheRoot = instance.getCheckpointTimelineWalPurgeFloor();
                Assert.assertTrue("the owed root moves the floor up", floorAfterTheRoot > headBaseSeqTxn);
                Assert.assertEquals(lastBaseSeqTxn() - floorAfterTheRoot, purgeBaseWalAndCountSegments());
                assertNoRefreshFaults("lv");
            }
            for (int restart = 0; restart < 2; restart++) {
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(OWED_ROOT_ROWS + OWED_ROOT_ROW_0005 + OWED_ROOT_ROW_0006, DEPARTURE_ROWS_QUERY);
            }
        });
    }

    @Test
    public void testResumeOwedRootLandsAtTheRowCadence() throws Exception {
        // The failed closing seal leaves the rows it owed on the row cadence's count, so the next
        // row crosses the count inside the duration cadence and lands the owed root.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 3);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                final long generationBefore = generation(instance);
                failTheOwedResumeSeal(job, fault, LiveViewCheckpointDiskFullFault.OP_ALLOCATE, "('2026-01-01T00:01:30.000000Z', 'a', 1.0)");
                assertRepairMarkerLive(instance, generationBefore);
                commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'a', 1.0)");
                Assert.assertEquals(ts("2026-01-01T00:05:00.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals("the splice and the row's seal", generationBefore + 2, generation(instance));
                assertRepairMarkerStale(instance, generationBefore);
                assertNoRefreshFaults("lv");
            }
            for (int restart = 0; restart < 2; restart++) {
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(OWED_ROOT_ROWS + OWED_ROOT_ROW_0005, DEPARTURE_ROWS_QUERY);
            }
        });
    }

    @Test
    public void testResumeOwedRootLandsOnARowlessCycleAtTheCadence() throws Exception {
        // On the whole-range route the replay lowered the batch minimum, so a cycle without rows
        // seals at the frontier once the cadence is due: a row the filter rejects inside the cadence
        // seals nothing, and one past it lands the owed root two generations past the marker's,
        // which reads the marker as stale.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                landTheOwedRootOnARowlessCycle(job, fault, instance);
                assertNoRefreshFaults("lv");
            }
            for (int restart = 0; restart < 2; restart++) {
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(OWED_ROOT_ROWS, DEPARTURE_ROWS_QUERY);
            }
        });
    }

    @Test
    public void testResumeThatDeclinesItsChainLogsItsAnchorInTheViewsUnit() throws Exception {
        assertResumeThatDeclinesItsChainTruncates(false);
    }

    @Test
    public void testResumeThatDeclinesItsChainLogsItsAnchorInTheViewsUnitOverANanosecondBase() throws Exception {
        assertResumeThatDeclinesItsChainTruncates(true);
    }

    @Test
    public void testResumeSpliceAboveTheFrontierLogsItsTruncateInTheViewsUnit() throws Exception {
        assertResumeSpliceAboveTheFrontierTruncatesAtIt(false);
    }

    @Test
    public void testResumeSpliceAboveTheFrontierLogsItsTruncateInTheViewsUnitOverANanosecondBase() throws Exception {
        assertResumeSpliceAboveTheFrontierTruncatesAtIt(true);
    }

    @Test
    public void testResumeWhoseAnchorIsGoneLogsTheAnchorInTheViewsUnit() throws Exception {
        assertResumeWhoseAnchorRestoreFails(CairoException.ERRNO_FILE_DOES_NOT_EXIST, false);
    }

    @Test
    public void testResumeWhoseAnchorIsGoneLogsTheAnchorInTheViewsUnitOverANanosecondBase() throws Exception {
        assertResumeWhoseAnchorRestoreFails(CairoException.ERRNO_FILE_DOES_NOT_EXIST, true);
    }

    @Test
    public void testResumeWhoseAnchorRestoreFaultsLogsTheAnchorInTheViewsUnit() throws Exception {
        assertResumeWhoseAnchorRestoreFails(ERRNO_EMFILE, false);
    }

    @Test
    public void testResumeWhoseAnchorRestoreFaultsLogsTheAnchorInTheViewsUnitOverANanosecondBase() throws Exception {
        assertResumeWhoseAnchorRestoreFails(ERRNO_EMFILE, true);
    }

    @Test
    public void testResumeWhoseMarkerCannotBeWrittenLogsItsFloorInTheViewsUnit() throws Exception {
        assertResumeWhoseMarkerCannotBeWrittenRetiresTheTimeline(false);
    }

    @Test
    public void testResumeWhoseMarkerCannotBeWrittenLogsItsFloorInTheViewsUnitOverANanosecondBase() throws Exception {
        assertResumeWhoseMarkerCannotBeWrittenRetiresTheTimeline(true);
    }

    @Test
    public void testResumeOwedRootThatLandsClearsItsMarker() throws Exception {
        // A closing seal that lands puts the owed root at the frontier, which makes the splice
        // restorable at once: the marker clears, and the runtime that froze the root stays the
        // head's, so the next resume from that root reuses it rather than restoring it.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                final long generationBefore = generation(instance);
                commitDeparture(job, "('2026-01-01T00:01:30.000000Z', 'a', 1.0)");
                Assert.assertEquals(ts("2026-01-01T00:04:00.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals("the splice and the closing seal", generationBefore + 2, generation(instance));
                assertCheckpointLadder(instance, false, """
                        2025-12-31T12:00:00.000000Z -> 1
                        2026-01-01T00:01:00.000000Z -> 2
                        2026-01-01T00:02:00.000000Z -> 4
                        2026-01-01T00:04:00.000000Z -> 6
                        """);
                assertNoRepairMarker(instance);
                final long reusesBefore = job.getStatsForTest().get(LiveViewRefreshStats.RUNTIME_ANCHOR_REUSE_COUNT);
                // Out of order within itself and wholly above the head, so the resume anchors on the
                // head's own root.
                commitDeparture(job, "('2026-01-01T00:06:00.000000Z', 'a', 1.0), ('2026-01-01T00:05:00.000000Z', 'a', 1.0)");
                Assert.assertEquals(
                        "the resume must reuse the runtime",
                        reusesBefore + 1,
                        job.getStatsForTest().get(LiveViewRefreshStats.RUNTIME_ANCHOR_REUSE_COUNT)
                );
                assertNoRefreshFaults("lv");
            }
            final String rows = OWED_ROOT_ROWS + OWED_ROOT_ROW_0005 + OWED_ROOT_ROW_0006;
            assertViewRows(rows, DEPARTURE_ROWS_QUERY);
            restartAndRefresh();
            assertRestoredFromTimeline("lv");
            assertViewRows(rows, DEPARTURE_ROWS_QUERY);
        });
    }

    @Test
    public void testResumeOwedRootThatLandsOnItsThirdAttemptRestores() throws Exception {
        // The fault outlasts the closing seal and the next cadence seal, and clears before the
        // third: that seal lands the owed root inside the strike budget, which reads the marker as
        // stale and ends the failure streak, so a failure after it is the first of a new one.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        final LogCapture capture = new LogCapture();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                    final long generationBefore = generation(instance);
                    final long sealFailuresBefore = instance.getCheckpointSealFailures();
                    final long resetsBefore = instance.getCheckpointTimelineResets();
                    failTheOwedResumeSeal(job, fault, LiveViewCheckpointDiskFullFault.OP_ALLOCATE, "('2026-01-01T00:01:30.000000Z', 'a', 1.0)");

                    setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                    fault.arm(LiveViewCheckpointDiskFullFault.OP_ALLOCATE, LiveViewCheckpointLayout.DATA_DIR_NAME, 1);
                    commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'a', 1.0)");
                    fault.disarm();
                    Assert.assertNotNull("the cadence seal must reach its data segment", fault.getFailedFile());
                    Assert.assertEquals(sealFailuresBefore + 2, instance.getCheckpointSealFailures());
                    assertRepairMarkerLive(instance, generationBefore);

                    commitDeparture(job, "('2026-01-01T00:06:00.000000Z', 'a', 1.0)");
                    Assert.assertEquals(ts("2026-01-01T00:06:00.000000Z"), instance.getHeadCheckpointMaxTs());
                    Assert.assertEquals("the splice and the seal that landed", generationBefore + 2, generation(instance));
                    Assert.assertEquals(sealFailuresBefore + 2, instance.getCheckpointSealFailures());
                    Assert.assertEquals(resetsBefore, instance.getCheckpointTimelineResets());
                    assertCheckpointLadder(instance, false, """
                            2025-12-31T12:00:00.000000Z -> 1
                            2026-01-01T00:01:00.000000Z -> 2
                            2026-01-01T00:02:00.000000Z -> 4
                            2026-01-01T00:06:00.000000Z -> 8
                            """);
                    assertRepairMarkerStale(instance, generationBefore);

                    setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                    fault.arm(LiveViewCheckpointDiskFullFault.OP_ALLOCATE, LiveViewCheckpointLayout.DATA_DIR_NAME, 1);
                    capture.start();
                    commitDeparture(job, "('2026-01-01T00:07:00.000000Z', 'a', 1.0)");
                    capture.drain();
                    capture.stop();
                    fault.disarm();
                    Assert.assertNotNull("the cadence seal must reach its data segment", fault.getFailedFile());
                    capture.assertLoggedRE("could not write live view head checkpoint \\[view=lv, lvSeqTxn=\\d+, consecutiveFailures=1,");
                    assertNoRefreshFaults("lv");
                }
                for (int restart = 0; restart < 2; restart++) {
                    restartAndRefresh();
                    assertRestoredFromTimeline("lv");
                    assertViewRows(OWED_ROOT_ROWS + OWED_ROOT_ROW_0005 + OWED_ROOT_ROW_0006 + OWED_ROOT_ROW_0007, DEPARTURE_ROWS_QUERY);
                }
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testResumeSealThatFailsOnItsDataMappingOverAnOwedRootBlocksTheRestart() throws Exception {
        assertOwedResumeSealFaultBlocksTheRestarts(LiveViewCheckpointDiskFullFault.OP_MMAP, false, false, true);
    }

    @Test
    public void testResumeSealThatFailsOnItsDataSyncOverAnOwedRootBlocksTheRestart() throws Exception {
        // A syncing commit mode is what has the seal msync its data segment at all.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        assertOwedResumeSealFaultBlocksTheRestarts(LiveViewCheckpointDiskFullFault.OP_MSYNC, false, false, true);
    }

    @Test
    public void testResumeSealThatFailsOnItsDataTruncateOverAnOwedRootBlocksTheRestart() throws Exception {
        assertOwedResumeSealFaultBlocksTheRestarts(LiveViewCheckpointDiskFullFault.OP_TRUNCATE, false, false, true);
    }

    @Test
    public void testResumeSealThatFailsOverAnOwedRootBlocksARestartBeforeAnyRow() throws Exception {
        assertOwedResumeSealFaultBlocksTheRestarts(LiveViewCheckpointDiskFullFault.OP_ALLOCATE, false, false, false);
    }

    @Test
    public void testResumeSealThatFailsOverAnOwedRootBlocksTheMidDrainRecovery() throws Exception {
        // The fault that broke the closing seal breaks the next repair's replay too, inside the owed
        // window. Its in-place recovery restores from the timeline only behind no live marker, so it
        // falls back to the rebuild from the applied base, which the restatement guard refuses over
        // the day the base lost: the view stops with every row it holds, and the second late row
        // waits unconsumed.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                    final long generationBefore = generation(instance);
                    failTheOwedResumeSeal(job, fault, LiveViewCheckpointDiskFullFault.OP_ALLOCATE, "('2026-01-01T00:01:30.000000Z', 'a', 1.0)");
                    final long faultsBefore = instance.getRefreshFaultCount();
                    final long runtimeRestoresBefore = instance.getCheckpointRuntimeRestores();
                    final AtomicBoolean isThrown = new AtomicBoolean();
                    hooks(job).setSimulateResumeReplayStart(() -> {
                        if (isThrown.compareAndSet(false, true)) {
                            throw CairoException.critical(0).put("simulated fault in the second repair's replay");
                        }
                    });
                    commitDeparture(job, "('2026-01-01T00:01:45.000000Z', 'a', 1.0)");
                    hooks(job).setSimulateResumeReplayStart(null);
                    Assert.assertTrue("the second repair must reach its replay", isThrown.get());
                    Assert.assertEquals(faultsBefore + 1, instance.getRefreshFaultCount());
                    Assert.assertEquals("nothing restores behind the live marker", runtimeRestoresBefore, instance.getCheckpointRuntimeRestores());
                    Assert.assertTrue("the refused rebuild stops the view", instance.isCheckpointRecoveryBlocked());
                    Assert.assertFalse(instance.isInvalid());
                    TestUtils.assertContains(
                            instance.getCheckpointRecoveryReason(),
                            "would drop rows it retains [cause=mid-drain refresh failure]: " + OWED_ROOT_HISTORY_FLOOR
                    );
                    assertRepairMarkerLive(instance, generationBefore);
                }
                capture.drain();
                capture.assertLogged("live view cannot restore its runtime from the checkpoint timeline, rebuilding from the applied base "
                        + "[view=lv, cause=mid-drain refresh failure, reason=prefix preservation repair marker present]");
                assertViewRows(OWED_ROOT_ROWS, DEPARTURE_ROWS_QUERY);
                assertRestartsBlockedByTheRepairMarker(OWED_ROOT_ROWS, OWED_ROOT_HISTORY_FLOOR);
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testResumeSealThatFailsOverAnOwedRootBlocksTheRestartOverANanosecondBase() throws Exception {
        // The same repair over a base whose designated timestamp has nanosecond precision: the
        // line it logs once prints the frontier and the splice's newest root in nanoseconds.
        assertOwedResumeSealFaultLogsTheKeptMarkerOnce(true);
    }

    @Test
    public void testResumeSealThatFailsOverAnOwedRootBlocksTheRestartOverATtlBase() throws Exception {
        assertOwedResumeSealFaultBlocksTheRestarts(LiveViewCheckpointDiskFullFault.OP_ALLOCATE, false, true, true);
    }

    @Test
    public void testResumeSealThatFailsOverAnOwedRootBlocksTheRestartUnderAnHourCadence() throws Exception {
        // An hour's cadence seals no root at 00:01 or 00:02, so the resume anchors on the root at
        // 2025-12-31 and its splice owes every row above it. A row ten minutes after the failed seal
        // is well inside the cadence and seals nothing: the marker stays live however long the
        // cadence leaves the window open.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_MAX_DURATION_MICROS, 3_600_000_000L);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ADAPTIVE_CADENCE_ENABLED, "false");
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                assertCheckpointLadder(instance, false, "2025-12-31T12:00:00.000000Z -> 1\n");
                final long generationBefore = generation(instance);
                failTheOwedResumeSeal(job, fault, LiveViewCheckpointDiskFullFault.OP_ALLOCATE, "('2026-01-01T00:01:30.000000Z', 'a', 1.0)");
                Assert.assertEquals(ts("2025-12-31T12:00:00.000000Z"), instance.getHeadCheckpointMaxTs());
                assertRepairMarkerLive(instance, generationBefore);
                setCurrentMicros(currentMicros + 600_000_000L);
                commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'a', 1.0)");
                Assert.assertEquals(ts("2025-12-31T12:00:00.000000Z"), instance.getHeadCheckpointMaxTs());
                assertRepairMarkerLive(instance, generationBefore);
                assertNoRefreshFaults("lv");
            }
            assertRestartsBlockedByTheRepairMarker(OWED_ROOT_ROWS + OWED_ROOT_ROW_0005, OWED_ROOT_HISTORY_FLOOR);
        });
    }

    @Test
    public void testResumeSealThatFailsOverAnOwedRootKeepsEveryRowAcrossRestarts() throws Exception {
        // The resume's splice keeps its newest root at 00:02, below the frontier 00:04, and its
        // closing seal fails: the marker stays live, and the repair logs the frontier it owes a root
        // and the splice's newest root once. A row inside the cadence seals nothing, and each restart
        // rebuilds from the applied base, which the restatement guard refuses over the day the base
        // lost. The view stops with every row it holds.
        assertOwedResumeSealFaultLogsTheKeptMarkerOnce(false);
    }

    @Test
    public void testResumeSpliceEndingOnItsNewestRootClearsItsMarkerWhenTheRestampPassesACooldown() throws Exception {
        // The replay ends on the newest root the splice re-versioned, which already sits at the
        // frontier: the generation is restorable without the closing seal, which only re-stamps the
        // head, so the marker clears. The re-stamp writes nothing, so a cooldown lets it through:
        // the head goes back on that root at the repair's commit, with nothing fed since.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTheDayTheBaseLoses(job);
                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'a', 1.5)");
                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', 2.5)");
                final LiveViewInstance instance = viewInstance();
                final long generationBefore = generation(instance);
                final long sealFailuresBefore = instance.getCheckpointSealFailures();
                final AtomicBoolean hasReachedTheSeal = new AtomicBoolean();
                // A cooldown that ends one microsecond on, so it holds for this seal alone.
                hooks(job).setSimulateResumeHeadSealStart(() -> {
                    instance.setSealCooldownUntilUs(currentMicros + 1);
                    hasReachedTheSeal.set(true);
                });
                commitDeparture(job, "('2026-01-01T00:01:30.000000Z', 'a', 1.0)");
                hooks(job).setSimulateResumeHeadSealStart(null);
                Assert.assertTrue("the resume must reach its head seal", hasReachedTheSeal.get());
                Assert.assertEquals("the re-stamp is no failed seal", sealFailuresBefore, instance.getCheckpointSealFailures());
                Assert.assertEquals(ts("2026-01-01T00:02:00.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals("at the repair's commit", lastBaseSeqTxn(), instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals("with nothing fed since", Long.MAX_VALUE, instance.getMinSeenTsSinceCheckpoint());
                Assert.assertEquals("the splice alone", generationBefore + 1, generation(instance));
                assertCheckpointLadder(instance, false, """
                        2025-12-31T12:00:00.000000Z -> 1
                        2026-01-01T00:01:00.000000Z -> 2
                        2026-01-01T00:02:00.000000Z -> 4
                        """);
                assertNoRepairMarker(instance);
                assertNoRefreshFaults("lv");
            }
            final String rows = OWED_ROOT_ROWS.substring(0, OWED_ROOT_ROWS.indexOf("2026-01-01T00:03"));
            for (int restart = 0; restart < 2; restart++) {
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(rows, DEPARTURE_ROWS_QUERY);
            }
        });
    }

    @Test
    public void testResumeThatDeclinesTheChainRetiresItsTruncatedTimelineWhenItsSealFails() throws Exception {
        // A resume that declines the checkpoint chain truncates the timeline at its replacement
        // commit instead of splicing it. A truncated timeline is restorable only under a fresh head,
        // so a closing seal that fails over it retires it, which takes the truncate's marker with
        // it. The next row opens a fresh history at its own root, which each restart restores from
        // over the day the base lost.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 0);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                final long resetsBefore = instance.getCheckpointTimelineResets();
                final long sealFailuresBefore = instance.getCheckpointSealFailures();
                hooks(job).setSimulateResumeHeadSealStart(
                        () -> fault.arm(LiveViewCheckpointDiskFullFault.OP_ALLOCATE, LiveViewCheckpointLayout.DATA_DIR_NAME, 1)
                );
                commitDeparture(job, "('2026-01-01T00:01:30.000000Z', 'a', 1.0)");
                hooks(job).setSimulateResumeHeadSealStart(null);
                fault.disarm();
                Assert.assertNotNull("the closing seal must reach its data segment", fault.getFailedFile());
                Assert.assertEquals(sealFailuresBefore + 1, instance.getCheckpointSealFailures());
                Assert.assertEquals("the failed seal retires the truncated timeline", resetsBefore + 1, instance.getCheckpointTimelineResets());
                try (Path dir = checkpointsDir(instance); Path timelinePath = new Path()) {
                    LiveViewCheckpointLayout.timelinePath(timelinePath, dir);
                    Assert.assertFalse("the timeline is gone", configuration.getFilesFacade().exists(timelinePath.$()));
                    Assert.assertFalse("and the truncate's marker with it", LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir));
                }
                commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'a', 1.0)");
                Assert.assertEquals("the next row opens a fresh history", 1, generation(instance));
                assertCheckpointLadder(instance, false, "2026-01-01T00:05:00.000000Z -> 7\n");
                assertNoRefreshFaults("lv");
            }
            for (int restart = 0; restart < 2; restart++) {
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(OWED_ROOT_ROWS + OWED_ROOT_ROW_0005, DEPARTURE_ROWS_QUERY);
            }
        });
    }

    @Test
    public void testResumeWithoutACaptureRecordsTheSeqTxnItsReplacementFollows() throws Exception {
        // A resume that declines the checkpoint chain truncates the timeline immediately ahead of
        // its replacement commit, so its marker records the view's seqTxn that commit follows: a
        // crash between the marker and the commit leaves a timeline the restart can still restore
        // from, and the marker reads as stale there. Only the frontier truncates, which run after
        // the replacement commit, record none.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 0);
        // The view's committed WAL seqTxn at each marker publication, and the seqTxn the
        // published marker recorded.
        final LongList lvSeqTxnAtMarkerWrites = new LongList();
        final LongList recordedLvSeqTxns = new LongList();
        final TestFilesFacadeImpl ff = new TestFilesFacadeImpl() {
            @Override
            public int rename(LPSZ from, LPSZ to) {
                final int result = super.rename(from, to);
                if (result == Files.FILES_RENAME_OK && Utf8s.endsWithAscii(to, LiveViewCheckpointLayout.REPAIRING_MARKER_FILE_NAME)) {
                    final LiveViewInstance instance = viewInstance();
                    lvSeqTxnAtMarkerWrites.add(engine.getTableSequencerAPI().lastTxn(instance.getLiveViewToken()));
                    try (Path dir = checkpointsDir(instance)) {
                        recordedLvSeqTxns.add(LiveViewCheckpointRepairMarker.readLvSeqTxn(configuration, dir));
                    }
                }
                return result;
            }
        };
        assertMemoryLeak(ff, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                Assert.assertEquals("no repair has written a marker yet", 0, lvSeqTxnAtMarkerWrites.size());
                final long lvSeqTxnBefore = engine.getTableSequencerAPI().lastTxn(instance.getLiveViewToken());
                final long resumesBefore = job.getStatsForTest().getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RESUME_FROM_ANCHOR);
                commitDeparture(job, "('2026-01-01T00:01:30.000000Z', 'a', 1.0)");
                Assert.assertEquals(
                        "the resume from the root at 00:01 repairs the late row",
                        resumesBefore + 1,
                        job.getStatsForTest().getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RESUME_FROM_ANCHOR)
                );
                Assert.assertEquals("the truncate writes the turn's one marker", 1, lvSeqTxnAtMarkerWrites.size());
                Assert.assertEquals("the marker lands ahead of the replacement commit", lvSeqTxnBefore, lvSeqTxnAtMarkerWrites.getQuick(0));
                Assert.assertEquals("the marker records the seqTxn the replacement commit follows", lvSeqTxnBefore, recordedLvSeqTxns.getQuick(0));
                Assert.assertEquals(
                        "the replacement commits after the marker",
                        lvSeqTxnBefore + 1,
                        engine.getTableSequencerAPI().lastTxn(instance.getLiveViewToken())
                );
                assertNoRepairMarker(instance);
                assertNoRefreshFaults("lv");
            }
            restartAndRefresh();
            assertRestoredFromTimeline("lv");
            assertViewRows(OWED_ROOT_ROWS, DEPARTURE_ROWS_QUERY);
        });
    }

    @Test
    public void testResumeThatFaultsBetweenItsSpliceAndItsSealBlocksTheRestart() throws Exception {
        // A fault between the splice and the closing seal ends the turn ahead of the marker's
        // resolution, so the marker stays live as a failed seal leaves it: a row inside the cadence
        // seals nothing, and the restart stops the view with every row over the day the base lost.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                final long generationBefore = generation(instance);
                final AtomicBoolean isThrown = new AtomicBoolean();
                hooks(job).setSimulateResumeHeadSealStart(() -> {
                    if (isThrown.compareAndSet(false, true)) {
                        throw CairoException.critical(0).put("simulated fault between the splice and the seal");
                    }
                });
                commitDeparture(job, "('2026-01-01T00:01:30.000000Z', 'a', 1.0)");
                hooks(job).setSimulateResumeHeadSealStart(null);
                Assert.assertTrue("the resume must reach its head seal", isThrown.get());
                Assert.assertEquals(1, instance.getRefreshFaultCount());
                Assert.assertEquals(ts("2026-01-01T00:02:00.000000Z"), instance.getHeadCheckpointMaxTs());
                assertRepairMarkerLive(instance, generationBefore);
                commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'a', 1.0)");
                Assert.assertEquals(ts("2026-01-01T00:02:00.000000Z"), instance.getHeadCheckpointMaxTs());
                assertRepairMarkerLive(instance, generationBefore);
            }
            assertRestartsBlockedByTheRepairMarker(OWED_ROOT_ROWS + OWED_ROOT_ROW_0005, OWED_ROOT_HISTORY_FLOOR);
        });
    }

    @Test
    public void testRowlessCyclesAfterADepartureAdvanceTheHeadBaseWithoutARoot() throws Exception {
        // A departure leaves the head on the root at the view's new frontier, its newest row, under
        // the newest timestamp the view has seen. Each cycle without rows past the cadence
        // republishes that root at its own commit, as the table's newest row says, rather than ask
        // for a root at the timestamp above it, which the seal would decline.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createDepartureView();
            final String ladder = """
                    2025-12-31T12:00:00.000000Z -> 1
                    2026-01-01T00:01:00.000000Z -> 2
                    """;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTheDayTheBaseLoses(job);
                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'a', 1.5)");
                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', 2.5)");
                commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', -2.5)");
                final LiveViewInstance instance = viewInstance();
                Assert.assertEquals(ts("2026-01-01T00:01:00.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(ts("2026-01-01T00:02:00.000000Z"), instance.getLatestSeenTs());
                assertCheckpointLadder(instance, false, ladder);
                assertAdvancesTheHeadBaseOnEachRowlessCycle(job, instance, ts("2026-01-01T00:01:00.000000Z"), ladder, 3);
                Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT));
                assertNoRefreshFaults("lv");
            }
            restartAndRefresh();
            assertRestoredFromTimeline("lv");
            assertViewRows(DEPARTURE_ROWS_OVER_THE_LOST_DAY, DEPARTURE_ROWS_QUERY);
        });
    }

    @Test
    public void testRowlessCyclesAfterTheOwedRootLandsAdvanceItsBase() throws Exception {
        // Once the owed root sits at the frontier the head is at the newest row the view's table
        // holds, and each cycle without rows past the cadence republishes that root at its own
        // commit: no new root, and the head's base coordinate and the WAL floor move up.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                landTheOwedRootOnARowlessCycle(job, fault, instance);
                assertAdvancesTheHeadBaseOnEachRowlessCycle(job, instance, ts("2026-01-01T00:04:00.000000Z"), """
                        2025-12-31T12:00:00.000000Z -> 1
                        2026-01-01T00:01:00.000000Z -> 2
                        2026-01-01T00:02:00.000000Z -> 4
                        2026-01-01T00:04:00.000000Z -> 6
                        """, 7);
                assertNoRefreshFaults("lv");
            }
            restartAndRefresh();
            assertRestoredFromTimeline("lv");
            assertViewRows(OWED_ROOT_ROWS, DEPARTURE_ROWS_QUERY);
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

    // The head-miss owed-root fixture's rows through 00:lastMinute with the late row 00:49:30: its
    // one-minute frame holds 00:49 and itself, and 00:50's holds both and itself.
    private static String headMissRowsWithTheLateRow(int lastMinute) {
        return headMissDepartureRows(true, lastMinute).replace(
                "2026-01-01T00:50:00.000000Z\ta\t1.0\t2.0\n",
                "2026-01-01T00:49:30.000000Z\ta\t1.0\t2.0\n2026-01-01T00:50:00.000000Z\ta\t1.0\t3.0\n"
        );
    }

    private static long lastBaseSeqTxn() {
        return engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("base"));
    }

    // text, written for the owed-root fixture on 2026-01-01 over a microsecond base, for the
    // fixture on day, over a nanosecond base when isNanos. The rows of 2025-12-31 stay where they
    // are.
    private static String owedRootText(String text, String day, boolean isNanos) {
        final String onDay = text.replace("2026-01-01T", day + "T");
        return isNanos ? onDay.replace(".000000Z", ".000000000Z") : onDay;
    }

    // The instant text names, in nanoseconds when isNanos and in microseconds otherwise.
    private static long timestampOf(String text, boolean isNanos) {
        return isNanos ? NanosTimestampDriver.floor(text) : MicrosTimestampDriver.floor(text);
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
     * Over the day the base loses, the head-miss departure fixture's kept-row departure when
     * {@code isHeadMiss}, and otherwise the departure of the newer of two rows under one root,
     * with the frontier truncate failing after its marker and its metadata, and the retire that
     * answers it, and the seal after it, removing nothing. The splice's generation stays on disk
     * under the truncate's marker, which records no seqTxn. A base column then has the view
     * recover in place, which declines that generation, and the restatement guard refuses the
     * rebuild: the view stops with every row, in place and on each restart.
     */
    private void assertFrontierTruncateWhosePublicationAndRetireFailKeepsTheRows(boolean isHeadMiss) throws Exception {
        final FrontierTruncateDoubleFault fault = new FrontierTruncateDoubleFault();
        final String rows = isHeadMiss ? HEAD_MISS_KEPT_ROW_DEPARTURE_ROWS : singleDepartureRows(true);
        final String historyFloor = isHeadMiss ? HEAD_MISS_OWED_ROOT_HISTORY_FLOOR : OWED_ROOT_HISTORY_FLOOR;
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                final String departure;
                if (isHeadMiss) {
                    createHeadMissDepartureView();
                    departure = HEAD_MISS_KEPT_ROW_DEPARTURE;
                } else {
                    createDepartureView();
                    departure = "('2026-01-01T00:03:00.000000Z', 'a', -9.5)";
                }
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    if (isHeadMiss) {
                        commitHeadMissDepartureFixture(job, true, 59);
                    } else {
                        commitTheDayTheBaseLoses(job);
                        commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'a', 1.5)");
                        commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', 2.5), ('2026-01-01T00:03:00.000000Z', 'a', 9.5)");
                    }
                    final LiveViewInstance instance = viewInstance();
                    final long generationBefore = generation(instance);
                    fault.arm(() -> job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH));
                    try {
                        commitDeparture(job, departure);
                    } finally {
                        fault.disarm();
                        job.setCheckpointTimelineTestFailureStage(0);
                    }
                    Assert.assertTrue("the frontier truncate must write its marker", fault.hasFired());
                    Assert.assertEquals(
                            1,
                            job.getStatsForTest().get(isHeadMiss
                                    ? LiveViewRefreshStats.HEAD_MISS_SPLICE_FRONTIER_TRUNCATE_COUNT
                                    : LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT)
                    );
                    Assert.assertEquals("the truncate never published", generationBefore + 1, generation(instance));
                    assertTimelineOnDisk(instance, true);
                    assertMarkerNames(generationBefore + 1);
                    try (Path dir = checkpointsDir(instance)) {
                        Assert.assertEquals(
                                "a marker written after the replacement commit records no seqTxn",
                                Numbers.LONG_NULL,
                                LiveViewCheckpointRepairMarker.readLvSeqTxn(configuration, dir)
                        );
                    }
                    assertViewRows(rows, DEPARTURE_ROWS_QUERY);
                    final long runtimeRestoresBefore = instance.getCheckpointRuntimeRestores();
                    execute("ALTER TABLE base ADD COLUMN y INT");
                    drainWalQueue();
                    driveRefreshToQuiescence(job);
                    Assert.assertEquals("nothing restores behind the live marker", runtimeRestoresBefore, instance.getCheckpointRuntimeRestores());
                    Assert.assertTrue("the refused rebuild stops the view", instance.isCheckpointRecoveryBlocked());
                    Assert.assertFalse(instance.isInvalid());
                    TestUtils.assertContains(
                            instance.getCheckpointRecoveryReason(),
                            "would drop rows it retains [cause=base table metadata change]: " + historyFloor
                    );
                }
                capture.drain();
                capture.assertLogged("live view cannot restore its runtime from the checkpoint timeline, rebuilding from the applied base "
                        + "[view=lv, cause=base table metadata change, reason=prefix preservation repair marker present]");
                assertViewRows(rows, DEPARTURE_ROWS_QUERY);
                assertRestartsBlockedByTheRepairMarker(rows, historyFloor);
                capture.drain();
                capture.assertNotLogged("does not match durable materialization");
                capture.assertNotLogged("runs without the restatement guard");
            });
        } finally {
            capture.stop();
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

    // The marker on disk names baseGeneration: the generation the repair that wrote it started
    // from, which for a frontier truncate is the splice's.
    private void assertMarkerNames(long baseGeneration) {
        try (Path dir = checkpointsDir(viewInstance())) {
            Assert.assertTrue("the marker must be on disk", LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir));
            Assert.assertEquals(
                    "the marker must name the generation its repair started from",
                    baseGeneration,
                    LiveViewCheckpointRepairMarker.readBaseGeneration(configuration, dir)
            );
        }
    }

    // The three newest roots of instance's ladder, each as its timestamp and its effective row
    // position, oldest first: no root sits above the third.
    private void assertNewestRoots(
            LiveViewInstance instance,
            String firstTs,
            long firstPosition,
            String secondTs,
            long secondPosition,
            String thirdTs,
            long thirdPosition
    ) {
        final LongList ladder = snapshotCheckpointLadder(instance);
        final int n = ladder.size();
        Assert.assertTrue("the ladder must hold three roots at least", n >= 6);
        Assert.assertEquals(ts(firstTs), ladder.getQuick(n - 6));
        Assert.assertEquals(firstPosition, ladder.getQuick(n - 5));
        Assert.assertEquals(ts(secondTs), ladder.getQuick(n - 4));
        Assert.assertEquals(secondPosition, ladder.getQuick(n - 3));
        Assert.assertEquals(ts(thirdTs), ladder.getQuick(n - 2));
        Assert.assertEquals(thirdPosition, ladder.getQuick(n - 1));
    }

    // No repair marker is on disk.
    private void assertNoRepairMarker(LiveViewInstance instance) {
        try (Path dir = checkpointsDir(instance)) {
            Assert.assertFalse("the repair marker must be gone", LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir));
        }
    }

    /**
     * The owed-root fixture under a duration cadence of {@code durationMicros} and a row cadence
     * of {@code rowsCadence}, then three rows past both, whose seals fail: the third failure in a
     * row pauses the seal. Three more rows, each due under any cadence, attempt nothing inside the
     * pause, and a restart restores every row.
     */
    private void assertSealFaultPausesWhateverTheCadence(long durationMicros, long rowsCadence) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, rowsCadence);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_MAX_DURATION_MICROS, durationMicros);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                final long sealFailuresBefore = instance.getCheckpointSealFailures();
                final long resetsBefore = instance.getCheckpointTimelineResets();
                setCurrentMicros(currentMicros + Math.max(durationMicros, 0) + Micros.MINUTE_MICROS);
                for (int minute = 5; minute <= 10; minute++) {
                    fault.arm(LiveViewCheckpointDiskFullFault.OP_ALLOCATE, LiveViewCheckpointLayout.DATA_DIR_NAME, 1);
                    commitDeparture(job, "('2026-01-01T00:" + (minute < 10 ? "0" : "") + minute + ":00.000000Z', 'a', 1.0)");
                    fault.disarm();
                    if (minute <= 7) {
                        Assert.assertNotNull("the seal must reach its data segment", fault.getFailedFile());
                    } else {
                        Assert.assertNull("the pause holds the seal although it is due", fault.getFailedFile());
                    }
                    Assert.assertEquals(sealFailuresBefore + Math.min(minute - 4, 3), instance.getCheckpointSealFailures());
                }
                Assert.assertEquals("the third failure pauses", resetsBefore, instance.getCheckpointTimelineResets());
                Assert.assertTrue(instance.isSealOnCooldown(currentMicros));
                assertTimelineOnDisk(instance, true);
                assertNoRefreshFaults("lv");
            }
            restartAndRefresh();
            assertRestoredFromTimeline("lv");
            assertViewRows(
                    PAUSE_FIXTURE_ROWS + OWED_ROOT_ROW_0005 + OWED_ROOT_ROW_0006 + OWED_ROOT_ROW_0007 + OWED_ROOT_ROW_0008
                            + "2026-01-01T00:09:00.000000Z\ta\t1.0\t3.0\n"
                            + "2026-01-01T00:10:00.000000Z\ta\t1.0\t3.0\n",
                    DEPARTURE_ROWS_QUERY
            );
        });
    }

    /**
     * The owed-root fixture, over a base that lost the day 2025-12-31 when {@code isDayDropped},
     * then three rows past the duration cadence whose seals fail: the third failure pauses the seal
     * and keeps the timeline, which a row inside the pause does not seal over, and each restart
     * restores from it with every row.
     */
    private void assertCadenceSealFaultSpanningThreeFlushesKeepsTheTimeline(boolean isDayDropped) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        final LogCapture capture = new LogCapture();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job, "2026-01-01", isDayDropped);
                    final long sealFailuresBefore = instance.getCheckpointSealFailures();
                    capture.start();
                    failThreeCadenceSeals(job, fault, instance);
                    capture.drain();
                    capture.stop();
                    capture.assertLogged("live view checkpoint seal keeps failing, keeping the timeline and backing off "
                            + "[view=lv, consecutiveFailures=3, backoffMicros=60000000]");
                    Assert.assertEquals(0, capture.countOccurrences("retiring the timeline"));
                    assertQuery("SELECT view_status, checkpoint_seal_failures FROM live_views() WHERE view_name = 'lv'")
                            .noLeakCheck()
                            .noRandomAccess()
                            .returns("view_status\tcheckpoint_seal_failures\nactive\t" + (sealFailuresBefore + 3) + "\n");

                    final long generationBefore = generation(instance);
                    commitDeparture(job, "('2026-01-01T00:08:00.000000Z', 'a', 1.0)");
                    Assert.assertEquals("a row inside the pause attempts nothing", sealFailuresBefore + 3, instance.getCheckpointSealFailures());
                    Assert.assertEquals("so nothing lands", generationBefore, generation(instance));
                    Assert.assertEquals(ts("2026-01-01T00:02:00.000000Z"), instance.getHeadCheckpointMaxTs());
                    assertNoRefreshFaults("lv");
                }
                for (int restart = 0; restart < 2; restart++) {
                    restartAndRefresh();
                    assertRestoredFromTimeline("lv");
                    assertViewRows(
                            PAUSE_FIXTURE_ROWS + OWED_ROOT_ROW_0005 + OWED_ROOT_ROW_0006 + OWED_ROOT_ROW_0007 + OWED_ROOT_ROW_0008,
                            DEPARTURE_ROWS_QUERY
                    );
                }
            });
        } finally {
            capture.stop();
        }
    }

    /**
     * Restarts twice. Each restart rebuilds from the applied base because of {@code cause}, which
     * the restatement guard refuses over the day the base lost, on {@code historyFloor}: the view
     * stops with {@code rows}, every row it held, and {@code live_views()} reports the block.
     */
    private void assertRestartsBlocked(String rows, String historyFloor, String cause) throws Exception {
        for (int restart = 0; restart < 2; restart++) {
            restartAndRefresh();
            assertRebuildBlocked(rows, historyFloor, cause);
        }
    }

    /**
     * The restart behind rebuilt from the applied base because of {@code cause}, which the
     * restatement guard refused over the day the base lost, on {@code historyFloor}: the view
     * stopped with {@code rows}, every row it held, and {@code live_views()} reports the block.
     */
    private void assertRebuildBlocked(String rows, String historyFloor, String cause) throws Exception {
        final LiveViewInstance instance = viewInstance();
        Assert.assertEquals(
                "rebuild_blocked",
                LiveViewCheckpointRecoveryPhase.routeName(instance.getCheckpointRestoreRoute())
        );
        Assert.assertTrue(instance.isCheckpointRecoveryBlocked());
        Assert.assertFalse("a refused rebuild writes no invalidation", instance.isInvalid());
        TestUtils.assertContains(
                instance.getCheckpointRecoveryReason(),
                "would drop rows it retains [cause=" + cause + "]: " + historyFloor
        );
        assertQuery("SELECT view_status, checkpoint_recovery_phase, "
                + "invalidation_reason = checkpoint_recovery_reason AS reason_mirrored "
                + "FROM live_views() WHERE view_name = 'lv'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("""
                        view_status\tcheckpoint_recovery_phase\treason_mirrored
                        invalid\trebuild_blocked\ttrue
                        """);
        assertViewRows(rows, DEPARTURE_ROWS_QUERY);
    }

    // The restart behind met a live repair marker: see assertRebuildBlocked.
    private void assertRebuildBlockedByTheRepairMarker(String rows, String historyFloor) throws Exception {
        assertRebuildBlocked(rows, historyFloor, "prefix preservation repair marker present");
    }

    /**
     * Over the day the base loses (base commits 1 and 2), a root at 00:01 (3) and one at 00:02 (4),
     * each sealed by its own commit under the class's row cadence of one.
     */
    private LiveViewInstance commitThreeRoots(LiveViewRefreshJob job) throws Exception {
        driveSeedToCompletion(job, "lv");
        commitTheDayTheBaseLoses(job);
        commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'a', 1.5)");
        commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', 2.5)");
        final LiveViewInstance instance = viewInstance();
        assertCheckpointLadder(instance, false, """
                2025-12-31T12:00:00.000000Z -> 1
                2026-01-01T00:01:00.000000Z -> 2
                2026-01-01T00:02:00.000000Z -> 3
                """);
        return instance;
    }

    /**
     * Over {@link #commitThreeRoots}, inside the seal's pause, the departure of 00:02 (base commit
     * 8): the resume truncates the timeline at the new frontier 00:01, which keeps its root, and
     * the seal there only re-stamps the head, which writes nothing, so the pause lets it through.
     */
    private void departTheNewestRootInsideThePause(LiveViewRefreshJob job, LiveViewInstance instance) throws Exception {
        final long generationBefore = generation(instance);
        final long sealFailuresBefore = instance.getCheckpointSealFailures();
        commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', -2.5)");
        Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT));
        Assert.assertEquals("the splice and the truncate, and no root", generationBefore + 2, generation(instance));
        assertCheckpointLadder(instance, false, """
                2025-12-31T12:00:00.000000Z -> 1
                2026-01-01T00:01:00.000000Z -> 2
                """);
        assertNoRepairMarker(instance);
        Assert.assertEquals("the re-stamp passes the pause", ts("2026-01-01T00:01:00.000000Z"), instance.getHeadCheckpointMaxTs());
        Assert.assertEquals("at the departure's commit", 8, instance.getHeadCheckpointBaseSeqTxn());
        Assert.assertEquals("and counts as no failure", sealFailuresBefore, instance.getCheckpointSealFailures());
        Assert.assertEquals("nor as a success", 3, instance.getSealFailureStreak());
        Assert.assertTrue("so the pause stands", instance.isSealOnCooldown(currentMicros));
    }

    /**
     * Commits the rows 00:05 to 00:07 of 1.0 past the duration cadence, with the data segment of
     * each one's seal failing: three failures in a row, which pause the seal over the intact
     * timeline. Asserts the pause kept the head, both base WAL arms, the generation and the ladder,
     * and the timeline on disk, retired nothing, and holds the seal for a minute.
     */
    private void failThreeCadenceSeals(
            LiveViewRefreshJob job,
            LiveViewCheckpointDiskFullFault fault,
            LiveViewInstance instance
    ) throws Exception {
        final long headMaxTs = instance.getHeadCheckpointMaxTs();
        final long headBaseSeqTxn = instance.getHeadCheckpointBaseSeqTxn();
        final long timelineFloor = instance.getCheckpointTimelineWalPurgeFloor();
        final long generationBefore = generation(instance);
        final String ladder = snapshotCheckpointLadder(instance).toString();
        final long sealFailuresBefore = instance.getCheckpointSealFailures();
        final long resetsBefore = instance.getCheckpointTimelineResets();
        setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
        for (int minute = 5; minute <= 7; minute++) {
            fault.arm(LiveViewCheckpointDiskFullFault.OP_ALLOCATE, LiveViewCheckpointLayout.DATA_DIR_NAME, 1);
            commitDeparture(job, "('2026-01-01T00:0" + minute + ":00.000000Z', 'a', 1.0)");
            fault.disarm();
            Assert.assertNotNull("the seal must reach its data segment", fault.getFailedFile());
            Assert.assertEquals(sealFailuresBefore + minute - 4, instance.getCheckpointSealFailures());
        }
        Assert.assertEquals("the third failure pauses", resetsBefore, instance.getCheckpointTimelineResets());
        Assert.assertEquals(headMaxTs, instance.getHeadCheckpointMaxTs());
        Assert.assertEquals(headBaseSeqTxn, instance.getHeadCheckpointBaseSeqTxn());
        Assert.assertEquals(timelineFloor, instance.getCheckpointTimelineWalPurgeFloor());
        assertTimelineOnDisk(instance, true);
        Assert.assertEquals(generationBefore, generation(instance));
        TestUtils.assertEquals(ladder, snapshotCheckpointLadder(instance).toString());
        Assert.assertTrue("the pause holds a minute", instance.isSealOnCooldown(currentMicros + 59_000_000L));
        Assert.assertFalse(instance.isSealOnCooldown(currentMicros + 61_000_000L));
    }

    /**
     * Over the owed-root fixture, the late rows 00:01:10, 00:01:20 and 00:01:40, each repaired by
     * the resume from the root at 00:01, whose closing seals fail: the third failure in a row
     * pauses the seal with the head parked on 00:02 and the owed root's marker live. Returns the
     * generation the marker names.
     */
    private long failThreeLateRowSeals(
            LiveViewRefreshJob job,
            LiveViewCheckpointDiskFullFault fault,
            LiveViewInstance instance
    ) throws Exception {
        final long sealFailuresBefore = instance.getCheckpointSealFailures();
        long markerBaseGeneration = Numbers.LONG_NULL;
        for (String lateRow : new String[]{"00:01:10", "00:01:20", "00:01:40"}) {
            markerBaseGeneration = generation(instance);
            failTheOwedResumeSeal(job, fault, LiveViewCheckpointDiskFullFault.OP_ALLOCATE, "('2026-01-01T" + lateRow + ".000000Z', 'a', 1.0)");
        }
        Assert.assertEquals(sealFailuresBefore + 3, instance.getCheckpointSealFailures());
        Assert.assertEquals("the head stays parked", ts("2026-01-01T00:02:00.000000Z"), instance.getHeadCheckpointMaxTs());
        Assert.assertTrue("the third failure pauses", instance.isSealOnCooldown(currentMicros));
        assertRepairMarkerLive(instance, markerBaseGeneration);
        assertCheckpointLadder(instance, false, """
                2025-12-31T12:00:00.000000Z -> 1
                2026-01-01T00:01:00.000000Z -> 2
                2026-01-01T00:02:00.000000Z -> 6
                """);
        return markerBaseGeneration;
    }

    /**
     * Over {@link #commitThreeRoots}, past the duration cadence, commits rows the view's filter
     * rejects at 00:02:10 to 00:02:30 (base commits 5 to 7), with the writer failing every
     * publication: three failures in a row pause the seal over the head at 00:02. Leaves the
     * writer failing.
     */
    private void failThreePublications(LiveViewRefreshJob job, LiveViewInstance instance) throws Exception {
        final long headBaseSeqTxn = instance.getHeadCheckpointBaseSeqTxn();
        final long timelineFloor = instance.getCheckpointTimelineWalPurgeFloor();
        final long sealFailuresBefore = instance.getCheckpointSealFailures();
        final long resetsBefore = instance.getCheckpointTimelineResets();
        job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH);
        setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
        for (int second = 10; second <= 30; second += 10) {
            commitDeparture(job, "('2026-01-01T00:02:" + second + ".000000Z', 'b', -1.0)");
            Assert.assertEquals(sealFailuresBefore + second / 10, instance.getCheckpointSealFailures());
        }
        Assert.assertEquals("the third failure pauses", resetsBefore, instance.getCheckpointTimelineResets());
        Assert.assertEquals(ts("2026-01-01T00:02:00.000000Z"), instance.getHeadCheckpointMaxTs());
        Assert.assertEquals(headBaseSeqTxn, instance.getHeadCheckpointBaseSeqTxn());
        Assert.assertEquals(timelineFloor, instance.getCheckpointTimelineWalPurgeFloor());
        assertTimelineOnDisk(instance, true);
        Assert.assertTrue(instance.isSealOnCooldown(currentMicros));
    }

    /**
     * Past the duration cadence, commits rows the view's filter rejects at 00:04:10 to 00:04:30,
     * whose cycles each seal the owed-root fixture's unsealed rows at the frontier 00:04, and fail:
     * the third failure in a row pauses the seal with the head on 00:02.
     */
    private void failThreeRowlessSealsAtTheFrontier(
            LiveViewRefreshJob job,
            LiveViewCheckpointDiskFullFault fault,
            LiveViewInstance instance
    ) throws Exception {
        final long sealFailuresBefore = instance.getCheckpointSealFailures();
        final long resetsBefore = instance.getCheckpointTimelineResets();
        setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
        for (int second = 10; second <= 30; second += 10) {
            fault.arm(LiveViewCheckpointDiskFullFault.OP_ALLOCATE, LiveViewCheckpointLayout.DATA_DIR_NAME, 1);
            commitDeparture(job, "('2026-01-01T00:04:" + second + ".000000Z', 'b', -1.0)");
            fault.disarm();
            Assert.assertNotNull("the seal at the frontier must reach its data segment", fault.getFailedFile());
        }
        Assert.assertEquals(sealFailuresBefore + 3, instance.getCheckpointSealFailures());
        Assert.assertEquals("the third failure pauses", resetsBefore, instance.getCheckpointTimelineResets());
        Assert.assertEquals(ts("2026-01-01T00:02:00.000000Z"), instance.getHeadCheckpointMaxTs());
        assertTimelineOnDisk(instance, true);
        Assert.assertTrue(instance.isSealOnCooldown(currentMicros));
    }

    /**
     * Pauses the seal with {@link #failThreeCadenceSeals}, then, with the fault cleared, commits the
     * row 00:08 past the pause: the cadence that was due never moved, so the row seals, on the
     * ladder the pause kept, and ends the failure streak and its cooldown.
     */
    private void landTheSealAfterThePause(
            LiveViewRefreshJob job,
            LiveViewCheckpointDiskFullFault fault,
            LiveViewInstance instance
    ) throws Exception {
        final long sealFailuresBefore = instance.getCheckpointSealFailures();
        failThreeCadenceSeals(job, fault, instance);
        final long generationBefore = generation(instance);
        final long headBaseSeqTxn = instance.getHeadCheckpointBaseSeqTxn();
        final long timelineFloor = instance.getCheckpointTimelineWalPurgeFloor();
        setCurrentMicros(currentMicros + 61_000_000L);
        commitDeparture(job, "('2026-01-01T00:08:00.000000Z', 'a', 1.0)");
        Assert.assertEquals(ts("2026-01-01T00:08:00.000000Z"), instance.getHeadCheckpointMaxTs());
        Assert.assertEquals(generationBefore + 1, generation(instance));
        assertCheckpointLadder(instance, false, """
                2025-12-31T12:00:00.000000Z -> 1
                2026-01-01T00:01:00.000000Z -> 2
                2026-01-01T00:02:00.000000Z -> 3
                2026-01-01T00:08:00.000000Z -> 9
                """);
        Assert.assertTrue("the head arm moves up", instance.getHeadCheckpointBaseSeqTxn() > headBaseSeqTxn);
        Assert.assertTrue("and the timeline arm", instance.getCheckpointTimelineWalPurgeFloor() > timelineFloor);
        Assert.assertEquals("the failure count is a lifetime total", sealFailuresBefore + 3, instance.getCheckpointSealFailures());
        Assert.assertEquals(0, instance.getSealFailureStreak());
        Assert.assertFalse(instance.isSealOnCooldown(currentMicros));
    }

    // A retire took the timeline, its repair marker and any staged marker off the disk, and
    // released both base WAL arms the view held. Reads the files' existence alone: the superblock
    // helpers create an empty timeline where none is.
    private void assertTimelineAndMarkerRetired(LiveViewInstance instance) {
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path dir = checkpointsDir(instance); Path filePath = new Path()) {
            LiveViewCheckpointLayout.timelinePath(filePath, dir);
            Assert.assertFalse("the timeline is gone", ff.exists(filePath.$()));
            Assert.assertFalse("and the marker with it", LiveViewCheckpointRepairMarker.exists(ff, dir));
            LiveViewCheckpointLayout.repairingMarkerPath(filePath, dir).put(LiveViewCheckpointLayout.TMP_SUFFIX);
            Assert.assertFalse("and no staged marker", ff.exists(filePath.$()));
        }
        Assert.assertEquals("the retire releases the head arm", Numbers.LONG_NULL, instance.getHeadCheckpointBaseSeqTxn());
        Assert.assertEquals("and the timeline arm", Numbers.LONG_NULL, instance.getCheckpointTimelineWalPurgeFloor());
    }

    // The view's timeline file is on disk when isPresent, and is not otherwise.
    private void assertTimelineOnDisk(LiveViewInstance instance, boolean isPresent) {
        try (Path dir = checkpointsDir(instance); Path timelinePath = new Path()) {
            LiveViewCheckpointLayout.timelinePath(timelinePath, dir);
            Assert.assertEquals(
                    isPresent ? "the timeline stays" : "the timeline is gone",
                    isPresent,
                    configuration.getFilesFacade().exists(timelinePath.$())
            );
        }
    }

    /**
     * Commits two rows the view's filter rejects, at 00:{@code firstMinute} and the minute after
     * it, each past the cadence, and asserts each cycle republishes the head's root at its own
     * commit: one generation and no root, the head on {@code headMaxTs} at that commit's base
     * coordinate, and the WAL floor at the coordinate the head held before, which the generation
     * the publication keeps beside its own names.
     */
    private void assertAdvancesTheHeadBaseOnEachRowlessCycle(
            LiveViewRefreshJob job,
            LiveViewInstance instance,
            long headMaxTs,
            String ladder,
            int firstMinute
    ) throws Exception {
        for (int minute = firstMinute; minute <= firstMinute + 1; minute++) {
            final long generationBefore = generation(instance);
            final long headBaseBefore = instance.getHeadCheckpointBaseSeqTxn();
            setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
            commitDeparture(job, "('2026-01-01T00:0" + minute + ":00.000000Z', 'b', -1.0)");
            Assert.assertEquals("one publication per cycle", generationBefore + 1, generation(instance));
            Assert.assertEquals(headMaxTs, instance.getHeadCheckpointMaxTs());
            Assert.assertEquals("the head moves to the cycle's commit", lastBaseSeqTxn(), instance.getHeadCheckpointBaseSeqTxn());
            Assert.assertEquals("the WAL floor follows it", headBaseBefore, instance.getCheckpointTimelineWalPurgeFloor());
            assertCheckpointLadder(instance, false, ladder);
        }
    }

    /**
     * Restarts over the truncated timeline of the departure of 00:02, whose newest root sits at
     * the new frontier 00:01, at base commit 8, and commits a row the view's filter rejects past
     * the cadence at 00:06 to 00:08 (seqTxn 9 to 11): the restore puts the head on the root at the
     * frontier, and each cycle republishes it at its own commit. A second restart restores too.
     */
    private void assertRowlessCyclesBetweenTwoRestartsKeepTheTimeline() throws Exception {
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
            assertCheckpointLadder(instance, false, """
                    2025-12-31T12:00:00.000000Z -> 1
                    2026-01-01T00:01:00.000000Z -> 2
                    """);
            Assert.assertEquals(10, instance.getCheckpointTimelineWalPurgeFloor());
            Assert.assertEquals("the purge must keep only the segments above the floor", 11 - 10, purgeBaseWalAndCountSegments());
        }
        restartAndRefresh();
        assertRestoredFromTimeline("lv");
        Assert.assertFalse(viewInstance().isCheckpointRecoveryBlocked());
        assertViewRows(DEPARTURE_ROWS_OVER_THE_LOST_DAY, DEPARTURE_ROWS_QUERY);
        assertNoRefreshFaults("lv");
    }

    /**
     * {@link #assertHeadMissSpliceAboveTheFrontierTruncatesAtIt} with no chained boundary allowed:
     * the head-miss rebuild declines to splice the root at 00:59 above its output floor 00:57, logs
     * that floor in the view's own unit, and truncates the timeline there instead. Its closing seal
     * puts a root at the frontier 00:57, which a restart restores.
     */
    private void assertHeadMissThatDeclinesItsSpliceTruncates(boolean isNanos) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 0);
        final String day = "2026-01-01";
        final LogCapture capture = new LogCapture();
        try {
            assertMemoryLeak(() -> {
                createHeadMissDepartureView(isNanos);
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    commitHeadMissDepartureFixture(job, false, 59);
                    final LiveViewInstance instance = viewInstance();
                    capture.start();
                    commitDeparture(job, HEAD_MISS_KEPT_ROW_DEPARTURE);
                    capture.drain();
                    capture.stop();
                    capture.assertLogged(owedRootText("live view O3 head miss declined the checkpoint splice, truncating instead "
                            + "[view=lv, boundaries=1, max=0, outputLowTs=2026-01-01T00:57:00.000000Z]", day, isNanos));
                    Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_FRONTIER_TRUNCATE_COUNT));
                    assertCheckpointLadder(
                            instance,
                            isNanos,
                            owedRootText("2025-12-31T23:55:00.000000Z -> 1\n2026-01-01T00:57:00.000000Z -> 59\n", day, isNanos)
                    );
                    assertNoRepairMarker(instance);
                    assertNoRefreshFaults("lv");
                }
                assertViewRows(owedRootText(HEAD_MISS_KEPT_ROW_DEPARTURE_ROWS, day, isNanos), DEPARTURE_ROWS_QUERY);
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(owedRootText(HEAD_MISS_KEPT_ROW_DEPARTURE_ROWS, day, isNanos), DEPARTURE_ROWS_QUERY);
            });
        } finally {
            capture.stop();
        }
    }

    /**
     * The owed-root fixture, on a base whose designated timestamp has nanosecond precision when
     * {@code isNanos}, with no chained boundary allowed: the resume of the late row 00:01:30 from
     * the root at 00:01 declines to re-version the root at 00:02 above it, logs its anchor in the
     * view's own unit, and truncates the timeline above the anchor instead. Its closing seal puts a
     * root at the frontier 00:04, which a restart restores with every row.
     */
    private void assertResumeThatDeclinesItsChainTruncates(boolean isNanos) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 0);
        final String day = "2026-01-01";
        final LogCapture capture = new LogCapture();
        try {
            assertMemoryLeak(() -> {
                createDepartureView(isNanos);
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                    capture.start();
                    commitDeparture(job, "('2026-01-01T00:01:30.000000Z', 'a', 1.0)");
                    capture.drain();
                    capture.stop();
                    capture.assertLogged(owedRootText("live view O3 resume declined the checkpoint chain, truncating instead "
                            + "[view=lv, boundaries=1, max=0, anchorMaxTs=2026-01-01T00:01:00.000000Z]", day, isNanos));
                    assertCheckpointLadder(instance, isNanos, owedRootText("""
                            2025-12-31T12:00:00.000000Z -> 1
                            2026-01-01T00:01:00.000000Z -> 2
                            2026-01-01T00:04:00.000000Z -> 6
                            """, day, isNanos));
                    assertNoRepairMarker(instance);
                    assertNoRefreshFaults("lv");
                }
                assertViewRows(owedRootText(OWED_ROOT_ROWS, day, isNanos), DEPARTURE_ROWS_QUERY);
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(owedRootText(OWED_ROOT_ROWS, day, isNanos), DEPARTURE_ROWS_QUERY);
            });
        } finally {
            capture.stop();
        }
    }

    /**
     * The head-miss departure fixture over the day the base keeps, on a base whose designated
     * timestamp has nanosecond precision when {@code isNanos}: roots at 23:55 and 00:59, then one
     * upsert keeps 00:57 at a new value and moves 00:58 and 00:59 out of the filter. The head-miss
     * rebuild emits the one row 00:57, below the root at 00:59 its splice kept, so it logs the
     * frontier 00:57 and that root in the view's own unit, truncates the timeline at the frontier
     * and seals a root there, which a restart restores.
     */
    private void assertHeadMissSpliceAboveTheFrontierTruncatesAtIt(boolean isNanos) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final String day = "2026-01-01";
        final LogCapture capture = new LogCapture();
        try {
            assertMemoryLeak(() -> {
                createHeadMissDepartureView(isNanos);
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    commitHeadMissDepartureFixture(job, false, 59);
                    final LiveViewInstance instance = viewInstance();
                    assertCheckpointLadder(
                            instance,
                            isNanos,
                            owedRootText("2025-12-31T23:55:00.000000Z -> 1\n2026-01-01T00:59:00.000000Z -> 61\n", day, isNanos)
                    );
                    capture.start();
                    commitDeparture(job, HEAD_MISS_KEPT_ROW_DEPARTURE);
                    capture.drain();
                    capture.stop();
                    Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_FRONTIER_TRUNCATE_COUNT));
                    capture.assertLogged(owedRootText("live view O3 head miss output ended below its spliced roots, truncating at the "
                            + "frontier [view=lv, frontier=2026-01-01T00:57:00.000000Z, spliceHeadRootMaxTs=2026-01-01T00:59:00.000000Z, "
                            + "rowsEmitted=1]", day, isNanos));
                    assertCheckpointLadder(
                            instance,
                            isNanos,
                            owedRootText("2025-12-31T23:55:00.000000Z -> 1\n2026-01-01T00:57:00.000000Z -> 59\n", day, isNanos)
                    );
                    assertNoRepairMarker(instance);
                    assertNoRefreshFaults("lv");
                }
                assertViewRows(owedRootText(HEAD_MISS_KEPT_ROW_DEPARTURE_ROWS, day, isNanos), DEPARTURE_ROWS_QUERY);
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(owedRootText(HEAD_MISS_KEPT_ROW_DEPARTURE_ROWS, day, isNanos), DEPARTURE_ROWS_QUERY);
            });
        } finally {
            capture.stop();
        }
    }

    /**
     * Over the departure view at a root per commit, on a base whose designated timestamp has
     * nanosecond precision when {@code isNanos}: roots at 00:01 and 00:03, the second over the rows
     * 00:02 and 00:03, then an upsert moves 00:03 out of the filter. The resume from the root at
     * 00:01 emits the one row 00:02, below the root at 00:03 its splice kept, so it logs the frontier
     * 00:02 and that root in the view's own unit, truncates the timeline at the frontier and seals a
     * root there, which a restart restores.
     */
    private void assertResumeSpliceAboveTheFrontierTruncatesAtIt(boolean isNanos) throws Exception {
        final String day = "2026-01-01";
        final LogCapture capture = new LogCapture();
        try {
            assertMemoryLeak(() -> {
                createDepartureView(isNanos);
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'a', 1.5)");
                    commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', 2.5), ('2026-01-01T00:03:00.000000Z', 'a', 9.5)");
                    final LiveViewInstance instance = viewInstance();
                    assertCheckpointLadder(
                            instance,
                            isNanos,
                            owedRootText("2026-01-01T00:01:00.000000Z -> 1\n2026-01-01T00:03:00.000000Z -> 3\n", day, isNanos)
                    );
                    capture.start();
                    commitDeparture(job, "('2026-01-01T00:03:00.000000Z', 'a', -9.5)");
                    capture.drain();
                    capture.stop();
                    Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT));
                    capture.assertLogged(owedRootText("live view O3 resume output ended below its spliced roots, truncating at the "
                            + "frontier [view=lv, frontier=2026-01-01T00:02:00.000000Z, spliceHeadRootMaxTs=2026-01-01T00:03:00.000000Z, "
                            + "rowsEmitted=1]", day, isNanos));
                    assertCheckpointLadder(
                            instance,
                            isNanos,
                            owedRootText("2026-01-01T00:01:00.000000Z -> 1\n2026-01-01T00:02:00.000000Z -> 2\n", day, isNanos)
                    );
                    assertNoRepairMarker(instance);
                    assertNoRefreshFaults("lv");
                }
                assertViewRows(owedRootText(singleDepartureRows(false), day, isNanos), DEPARTURE_ROWS_QUERY);
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(owedRootText(singleDepartureRows(false), day, isNanos), DEPARTURE_ROWS_QUERY);
            });
        } finally {
            capture.stop();
        }
    }

    /**
     * The owed-root fixture over the day the base keeps, on a base whose designated timestamp has
     * nanosecond precision when {@code isNanos}, with the restore of the root at 00:01, which the
     * resume of the late row 00:01:30 anchors on, failing its first open with {@code errno}. EMFILE
     * is a fault a retry may get past: the turn fails, keeps the timeline, and the gate of the turn
     * after it restores the runtime in place from the newest root at or below the frontier 00:04,
     * the third seal's root at 00:02, then replays the two rows above it out of the four base
     * commits the view consumed. The retry then resumes from the same anchor and lands the owed
     * root. A file that is gone is not a fault a retry gets past: the resume retires the timeline
     * and the view rebuilds from the applied base. Either way the lines name the roots in the
     * view's own unit, the view ends with every row, and a restart restores it from the timeline.
     */
    private void assertResumeWhoseAnchorRestoreFails(int errno, boolean isNanos) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final String day = "2026-01-01";
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        final LogCapture capture = new LogCapture();
        try {
            assertMemoryLeak(fault.facade(), () -> {
                createDepartureView(isNanos);
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job, day, false);
                    final long resetsBefore = instance.getCheckpointTimelineResets();
                    final long faultsBefore = instance.getRefreshFaultCount();
                    fault.armAnchorRestoreOpen(errno);
                    capture.start();
                    commitDeparture(job, "('" + day + "T00:01:30.000000Z', 'a', 1.0)");
                    capture.drain();
                    capture.stop();
                    Assert.assertEquals("the restore of the anchor must reach its first open", 1, fault.getAnchorRestoreOpenFailures());
                    if (errno == ERRNO_EMFILE) {
                        capture.assertLogged(owedRootText("could not restore live view O3 resume anchor, retrying [view=lv, "
                                + "anchorMaxTs=2026-01-01T00:01:00.000000Z, anchorCheckpointId=1, keyed=false, error=", day, isNanos));
                        Assert.assertEquals(
                                "the gate of the turn after the fault restores the runtime once",
                                1,
                                capture.countOccurrences(owedRootText("live view restored its runtime from the checkpoint timeline "
                                        + "[view=lv, cause=mid-drain refresh failure, generation=3, checkpointId=2, "
                                        + "boundary=2026-01-01T00:02:00.000000Z, frontier=2026-01-01T00:04:00.000000Z, baseSeqTxn=4, "
                                        + "replayedRows=2]", day, isNanos))
                        );
                        Assert.assertEquals("the fault fails the turn", faultsBefore + 1, instance.getRefreshFaultCount());
                        Assert.assertEquals("a fault a retry gets past keeps the timeline", resetsBefore, instance.getCheckpointTimelineResets());
                        Assert.assertEquals(
                                "the retry lands the owed root",
                                timestampOf(day + "T00:04:00.000000Z", isNanos),
                                instance.getHeadCheckpointMaxTs()
                        );
                    } else {
                        capture.assertLogged(owedRootText("could not restore live view O3 resume anchor [view=lv, "
                                + "anchorMaxTs=2026-01-01T00:01:00.000000Z, anchorCheckpointId=1, error=", day, isNanos));
                        // The resume retires the timeline, and the recovery of its turn rebuilds
                        // the view from the applied base, which retires what is left again.
                        Assert.assertEquals("a root that is gone retires the timeline", resetsBefore + 2, instance.getCheckpointTimelineResets());
                        capture.assertLogged("live view cannot restore its runtime from the checkpoint timeline, rebuilding from the applied base "
                                + "[view=lv, cause=mid-drain refresh failure, reason=timeline is absent]");
                    }
                }
                assertViewRows(owedRootText(OWED_ROOT_ROWS, day, isNanos), DEPARTURE_ROWS_QUERY);
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(owedRootText(OWED_ROOT_ROWS, day, isNanos), DEPARTURE_ROWS_QUERY);
            });
        } finally {
            capture.stop();
        }
    }

    /**
     * The owed-root fixture, on a base whose designated timestamp has nanosecond precision when
     * {@code isNanos}, over a filesystem that refuses the repair marker of the resume repairing the
     * late row 00:01:30. The resume logs the marker's floor in the view's own unit, and retires the
     * timeline ahead of its commit instead of splicing unprotected. Its closing seal opens a fresh
     * history, which a restart restores with every row.
     */
    private void assertResumeWhoseMarkerCannotBeWrittenRetiresTheTimeline(boolean isNanos) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final String day = "2026-01-01";
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
        final LogCapture capture = new LogCapture();
        try {
            assertMemoryLeak(ff, () -> {
                createDepartureView(isNanos);
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                    final long resetsBefore = instance.getCheckpointTimelineResets();
                    isArmed.set(true);
                    capture.start();
                    commitDeparture(job, "('" + day + "T00:01:30.000000Z', 'a', 1.0)");
                    capture.drain();
                    capture.stop();
                    isArmed.set(false);
                    Assert.assertEquals("the resume attempts its marker once", 1, markerAttempts.get());
                    // The resume replays every row strictly above its anchor 00:01, so its output
                    // floor is one unit of the base above it.
                    capture.assertLogged("could not write the live view checkpoint repair marker [view=lv, floorTs="
                            + (isNanos ? "2026-01-01T00:01:00.000000001Z" : "2026-01-01T00:01:00.000001Z") + ", error=");
                    Assert.assertEquals("no marker, no splice: the timeline retires", resetsBefore + 1, instance.getCheckpointTimelineResets());
                    assertNoRepairMarker(instance);
                    assertNoRefreshFaults("lv");
                }
                assertViewRows(owedRootText(OWED_ROOT_ROWS, day, isNanos), DEPARTURE_ROWS_QUERY);
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(owedRootText(OWED_ROOT_ROWS, day, isNanos), DEPARTURE_ROWS_QUERY);
            });
        } finally {
            capture.stop();
        }
    }

    /**
     * The owed-root fixture over the day the base keeps, on a base whose designated timestamp has
     * nanosecond precision when {@code isNanos}, with the data segment of its newest root, 00:02,
     * torn. The restart's restore falls back to the root at 00:01 and heals the torn one in place,
     * logging both in the view's own unit, and the view keeps every row.
     */
    private void assertRestartPastACorruptNewestRootReconstructsIt(boolean isNanos) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final String day = "2026-01-01";
        assertMemoryLeak(() -> {
            createDepartureView(isNanos);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                corruptNewestRootDataSegment(commitTwoRowsAboveTheNewestRoot(job, day, false));
            }
            final LogCapture capture = restartAndRefreshCapturingLog();
            capture.assertLogged(owedRootText("live view checkpoint restore fell back past corrupt roots, reconstructing [view=lv, "
                    + "predecessorMaxTs=2026-01-01T00:01:00.000000Z, corruptCeilingMaxTs=2026-01-01T00:02:00.000000Z]", day, isNanos));
            capture.assertLogged(owedRootText("reconstructed corrupt live view checkpoint roots [view=lv, "
                    + "predecessorMaxTs=2026-01-01T00:01:00.000000Z, corruptCeilingMaxTs=2026-01-01T00:02:00.000000Z, roots=", day, isNanos));
            assertViewRows(owedRootText(PAUSE_FIXTURE_ROWS, day, isNanos), DEPARTURE_ROWS_QUERY);
            assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
        });
    }

    // testRestartDeclinesToHealOverATieOnTheFrontierTheViewHasNotConsumed over a base whose
    // designated timestamp has nanosecond precision when isNanos. The view holds one row at the
    // frontier 00:01:10, and the base two once it applied the tie.
    private void assertRestartDeclinesToHealOverATieOnTheFrontierTheViewHasNotConsumed(boolean isNanos) throws Exception {
        setCadenceForAGroupSplitAcrossTheFrontier();
        assertMemoryLeak(() -> {
            seedCorruptNewestRootUnderAHigherFrontierOverALossyBase(isNanos, "('" + timestamp(70) + "', 'b', 5)");

            final LogCapture capture = restartAndRefreshCapturingLog();
            Assert.assertTrue(viewInstance().isCheckpointRecoveryBlocked());
            Assert.assertEquals(
                    "rebuild_blocked",
                    LiveViewCheckpointRecoveryPhase.routeName(viewInstance().getCheckpointRestoreRoute())
            );
            assertViewRows(owedRootText(HIGHER_FRONTIER_SEEDED_ROWS, "2026-01-01", isNanos));
            capture.assertLogged(owedRootText(HEAL_DECLINED_ROWS + " on its frontier [view=lv, "
                    + "frontier=2026-01-01T00:01:10.000000Z, durableRows=1, baseRows=2]", "2026-01-01", isNanos));
        });
    }

    // testRestartDeclinesToHealALostRowUnderAsManyUnconsumedRowsInItsInterval over a base whose
    // designated timestamp has nanosecond precision when isNanos. The view's table holds the lost
    // row 01:59:50, and the base no row there.
    private void assertRestartDeclinesToHealALostRowUnderAsManyUnconsumedRowsInItsInterval(boolean isNanos) throws Exception {
        setCadenceForAGroupSplitAcrossTheFrontier();
        assertMemoryLeak(() -> assertRestartRebuildsOverALostRowAmongUnconsumedRowsInItsInterval(
                isNanos,
                HEAL_DECLINED_ROWS + " on a timestamp [view=lv, timestamp=2026-01-01T01:59:50.000000Z, durableRows=1, baseRows=0]",
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

    // testRestartDeclinesToHealALostRowAloneInItsInterval over a base whose designated timestamp
    // has nanosecond precision when isNanos.
    private void assertRestartDeclinesToHealALostRowAloneInItsInterval(boolean isNanos) throws Exception {
        setCadenceForAGroupSplitAcrossTheFrontier();
        assertMemoryLeak(() -> {
            seedALostRowAmongUnconsumedRowsInItsInterval(isNanos, VIEW_SQL, null);
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
            assertViewRows(owedRootText("""
                    ts\tsym\ts
                    2026-01-01T00:00:00.000000Z\ta\t1.0
                    2026-01-01T00:00:10.000000Z\ta\t2.0
                    2026-01-01T00:00:20.000000Z\ta\t3.0
                    2026-01-01T00:00:30.000000Z\ta\t4.0
                    2026-01-01T00:59:00.000000Z\ta\t4.0
                    2026-01-01T01:59:50.000000Z\ta\t5.0
                    2026-01-01T02:00:00.000000Z\ta\t11.0
                    2026-01-01T02:00:10.000000Z\ta\t18.0
                    """, "2026-01-01", isNanos));
            capture.assertLogged(owedRootText(HEAL_DECLINED_ROWS + " [view=lv, lowTsExclusive=2026-01-01T00:00:30.000000Z, "
                    + "highTsInclusive=2026-01-01T02:00:00.000000Z, durableRows=3, baseRows=2]", "2026-01-01", isNanos));
            capture.assertNotLogged("reconstructed corrupt live view checkpoint roots");
        });
    }

    // testRestartDeclinesToHealALostRowOnItsIntervalsLastTimestamp over a base whose designated
    // timestamp has nanosecond precision when isNanos.
    private void assertRestartDeclinesToHealALostRowOnItsIntervalsLastTimestamp(boolean isNanos) throws Exception {
        setCadenceForAGroupSplitAcrossTheFrontier();
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts " + (isNanos ? "TIMESTAMP_NS" : "TIMESTAMP") + ", sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY HOUR WAL");
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
                Assert.assertEquals(timestampOf("2026-01-01T01:59:50.000000Z", isNanos), viewInstance().getHeadCheckpointMaxTs());
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
            assertViewRows(owedRootText("""
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
                    """, "2026-01-01", isNanos));
            assertViewMatchesRecompute();
            capture.assertLogged(owedRootText(HEAL_DECLINED_ROWS + " on a timestamp [view=lv, "
                    + "timestamp=2026-01-01T01:59:50.000000Z, durableRows=1, baseRows=0]", "2026-01-01", isNanos));
            capture.assertNotLogged("reconstructed corrupt live view checkpoint roots");
        });
    }

    /**
     * Over the departure view, on a base whose designated timestamp has nanosecond precision when
     * {@code isNanos}: the view's first row 00:01 seals the first root, the row 00:02 past the
     * duration cadence the second, and the row 00:04 inside the cadence none. Over a nanosecond base
     * each row carries digits below the microsecond. The restart restores from the root at 00:02 and
     * replays the row 00:04 above it, and logs both timestamps in the view's own unit, beside the
     * generation and the id of the second root and the base seqTxn of the third commit.
     */
    private void assertRestartLogsTheRootItRestoresAndTheFrontier(boolean isNanos) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final String firstRootTs = isNanos ? "2026-01-01T00:01:00.000000123Z" : "2026-01-01T00:01:00.000000Z";
        final String boundaryTs = isNanos ? "2026-01-01T00:02:00.000000456Z" : "2026-01-01T00:02:00.000000Z";
        final String frontierTs = isNanos ? "2026-01-01T00:04:00.000000789Z" : "2026-01-01T00:04:00.000000Z";
        assertMemoryLeak(() -> {
            createDepartureView(isNanos);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitDeparture(job, "('" + firstRootTs + "', 'a', 1.5)");
                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                commitDeparture(job, "('" + boundaryTs + "', 'a', 2.5)");
                commitDeparture(job, "('" + frontierTs + "', 'a', 9.5)");
                assertCheckpointLadder(viewInstance(), isNanos, firstRootTs + " -> 1\n" + boundaryTs + " -> 2\n");
                assertNoRefreshFaults("lv");
            }
            final LogCapture capture = restartAndRefreshCapturingLog();
            assertRestoredFromTimeline("lv");
            Assert.assertEquals(
                    1,
                    capture.countOccurrences("restored live view from checkpoint timeline [view=lv, generation=2, checkpointId=1, "
                            + "boundary=" + boundaryTs + ", frontier=" + frontierTs + ", baseSeqTxn=3, replayedRows=1]")
            );
            assertViewRows(
                    "ts\tsym\tx\ts\n"
                            + firstRootTs + "\ta\t1.5\t1.5\n"
                            + boundaryTs + "\ta\t2.5\t4.0\n"
                            + frontierTs + "\ta\t9.5\t13.5\n",
                    DEPARTURE_ROWS_QUERY
            );
        });
    }

    /**
     * The head-miss departure of 00:59 over a root at 00:56, which becomes the frontier, on a base
     * whose designated timestamp has nanosecond precision when {@code isNanos}: the read of that
     * root fails, and the repair logs the frontier in the view's own unit and retires the timeline.
     */
    private void assertHeadMissDepartureFrontierLookupThatFailsRetiresTheTimeline(boolean isNanos) throws Exception {
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
                createHeadMissDepartureView(isNanos);
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    commitHeadMissRootAtTheFrontierThenRowsAboveIt(job, false, false, isNanos);
                    final LiveViewInstance instance = viewInstance();
                    final long resetsBefore = instance.getCheckpointTimelineResets();
                    isArmed.set(true);
                    commitDeparture(job, "('2026-01-01T00:59:00.000000Z', 'a', -1.0)");
                    Assert.assertEquals(1, refusedReads.get());
                    capture.drain();
                    capture.assertLogged(owedRootText(
                            "could not read the live view checkpoint root at the frontier [view=lv, frontier=2026-01-01T00:56:00.000000Z, error=",
                            "2026-01-01",
                            isNanos
                    ));
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
                assertViewRows(owedRootText(headMissDepartureRows(true, 56), "2026-01-01", isNanos), DEPARTURE_ROWS_QUERY);
                assertViewMatchesRecompute(HEAD_MISS_DEPARTURE_VIEW_SQL);
            });
        } finally {
            capture.stop();
        }
    }

    /**
     * The head-miss owed-root fixture, over a base whose designated timestamp has nanosecond
     * precision when {@code isNanos}: the closing seal of the rebuild repairing the late row
     * 00:49:30 fails, the marker stays live, and the rebuild logs once that it kept it, naming the
     * frontier 00:55 and the splice's newest root 00:50 in the view's own unit. Each restart before
     * any row then stops the view with every row it holds.
     */
    private void assertHeadMissSealThatFailsOverAnOwedRootBlocksARestartBeforeAnyRow(boolean isNanos) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final String day = "2026-01-01";
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createHeadMissDepartureView(isNanos);
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    final LiveViewInstance instance = commitFiveRowsAboveTheHeadMissRoot(job, isNanos);
                    final long generationBefore = generation(instance);
                    failTheOwedHeadMissSeal(job, fault);
                    assertCheckpointLadder(
                            instance,
                            isNanos,
                            owedRootText("2025-12-31T23:55:00.000000Z -> 1\n2026-01-01T00:50:00.000000Z -> 53\n", day, isNanos)
                    );
                    assertRepairMarkerLive(instance, generationBefore);
                    assertNoRefreshFaults("lv");
                }
                assertRestartsBlockedByTheRepairMarker(
                        owedRootText(headMissRowsWithTheLateRow(55), day, isNanos),
                        owedRootText(HEAD_MISS_OWED_ROOT_HISTORY_FLOOR, day, isNanos)
                );
            });
            capture.drain();
            Assert.assertEquals(
                    1,
                    capture.countOccurrences(owedRootText("live view O3 repair kept its repair marker, no root at the frontier [view=lv, "
                            + "frontier=2026-01-01T00:55:00.000000Z, spliceHeadRootMaxTs=2026-01-01T00:50:00.000000Z]", day, isNanos))
            );
        } finally {
            capture.stop();
        }
    }

    /**
     * {@link #assertOwedResumeSealFaultBlocksTheRestarts} under an allocation fault with a row after
     * it, over a nanosecond base when {@code isNanos}: the repair logs once that it kept its marker,
     * naming the frontier 00:04 and the splice's newest root 00:02 in the view's own unit.
     */
    private void assertOwedResumeSealFaultLogsTheKeptMarkerOnce(boolean isNanos) throws Exception {
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertOwedResumeSealFaultBlocksTheRestarts(LiveViewCheckpointDiskFullFault.OP_ALLOCATE, isNanos, false, true);
            capture.drain();
            Assert.assertEquals(
                    1,
                    capture.countOccurrences(owedRootText("live view O3 repair kept its repair marker, no root at the frontier [view=lv, "
                            + "frontier=2026-01-01T00:04:00.000000Z, spliceHeadRootMaxTs=2026-01-01T00:02:00.000000Z]", "2026-01-01", isNanos))
            );
        } finally {
            capture.stop();
        }
    }

    /**
     * The owed-root fixture, on a base whose designated timestamp has nanosecond precision when
     * {@code isNanos}, and that loses the day 2025-12-31 through a DROP PARTITION, or through its
     * one-day TTL when {@code isTtl}, which moves the fixture's rows to 2026-01-02. The closing seal
     * of the resume repairing the late row 00:01:30 fails on {@code faultOp}, and the row 00:05,
     * when {@code isRowAfterTheFault}, commits inside the cadence. The marker stays live through
     * both, and each restart stops the view with every row it holds.
     */
    private void assertOwedResumeSealFaultBlocksTheRestarts(
            int faultOp,
            boolean isNanos,
            boolean isTtl,
            boolean isRowAfterTheFault
    ) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        if (isTtl) {
            // Past the data, so the TTL measures from the base's newest row, and the first commit on
            // 2026-01-02 expires the day 2025-12-31.
            setCurrentMicros(MicrosTimestampDriver.floor("2026-01-03T00:00:00.000000Z"));
        }
        final String day = isTtl ? "2026-01-02" : "2026-01-01";
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            execute("CREATE TABLE base (ts " + (isNanos ? "TIMESTAMP_NS" : "TIMESTAMP") + ", sym SYMBOL, x DOUBLE) TIMESTAMP(ts) "
                    + "PARTITION BY DAY" + (isTtl ? " TTL 1 DAY" : "") + " WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " + DEPARTURE_VIEW_SQL);
            final long parkedHeadTs = isNanos
                    ? NanosTimestampDriver.floor(day + "T00:02:00.000000Z")
                    : MicrosTimestampDriver.floor(day + "T00:02:00.000000Z");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job, day, !isTtl);
                assertCheckpointLadder(instance, isNanos, owedRootText("""
                        2025-12-31T12:00:00.000000Z -> 1
                        2026-01-01T00:01:00.000000Z -> 2
                        2026-01-01T00:02:00.000000Z -> 3
                        """, day, isNanos));
                final long generationBefore = generation(instance);
                failTheOwedResumeSeal(job, fault, faultOp, "('" + day + "T00:01:30.000000Z', 'a', 1.0)");
                Assert.assertEquals("the failed seal leaves the head parked", parkedHeadTs, instance.getHeadCheckpointMaxTs());
                assertCheckpointLadder(instance, isNanos, owedRootText("""
                        2025-12-31T12:00:00.000000Z -> 1
                        2026-01-01T00:01:00.000000Z -> 2
                        2026-01-01T00:02:00.000000Z -> 4
                        """, day, isNanos));
                assertRepairMarkerLive(instance, generationBefore);
                if (isRowAfterTheFault) {
                    commitDeparture(job, "('" + day + "T00:05:00.000000Z', 'a', 1.0)");
                    Assert.assertEquals("a row inside the cadence seals nothing", parkedHeadTs, instance.getHeadCheckpointMaxTs());
                    assertRepairMarkerLive(instance, generationBefore);
                }
                assertNoRefreshFaults("lv");
            }
            assertRestartsBlockedByTheRepairMarker(
                    owedRootText(OWED_ROOT_ROWS + (isRowAfterTheFault ? OWED_ROOT_ROW_0005 : ""), day, isNanos),
                    owedRootText(OWED_ROOT_HISTORY_FLOOR, day, isNanos)
            );
        });
    }

    /**
     * Over the owed-root fixture, on a base whose designated timestamp has nanosecond precision
     * when {@code isNanos}: past the duration cadence, two failed seals at the frontier 00:04, then
     * an upsert of 00:04 out of the filter, whose resume owes a root at the new frontier 00:03 and
     * whose closing seal fails, the third failure in a row. The seal pauses, and the repair retires
     * the timeline and its marker, logging the owed frontier and the newest timestamp the view has
     * seen in the view's own unit. Each restart then stops the view with every row it holds.
     * {@code capture} holds the log of the upsert's refresh.
     */
    private void assertOwedRootThatADepartureStrandsAtTheThirdFailureRetiresTheTimeline(
            boolean isNanos,
            LogCapture capture
    ) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final String day = "2026-01-01";
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView(isNanos);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                final long sealFailuresBefore = instance.getCheckpointSealFailures();
                final long resetsBefore = instance.getCheckpointTimelineResets();
                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                for (int second = 10; second <= 20; second += 10) {
                    fault.arm(LiveViewCheckpointDiskFullFault.OP_ALLOCATE, LiveViewCheckpointLayout.DATA_DIR_NAME, 1);
                    commitDeparture(job, "('" + day + "T00:04:" + second + ".000000Z', 'b', -1.0)");
                    fault.disarm();
                    Assert.assertNotNull("the seal at the frontier must reach its data segment", fault.getFailedFile());
                }
                Assert.assertEquals(sealFailuresBefore + 2, instance.getCheckpointSealFailures());
                hooks(job).setSimulateResumeHeadSealStart(
                        () -> fault.arm(LiveViewCheckpointDiskFullFault.OP_ALLOCATE, LiveViewCheckpointLayout.DATA_DIR_NAME, 1)
                );
                capture.start();
                commitDeparture(job, "('" + day + "T00:04:00.000000Z', 'a', -1.0)");
                capture.drain();
                capture.stop();
                hooks(job).setSimulateResumeHeadSealStart(null);
                fault.disarm();
                Assert.assertNotNull("the closing seal must reach its data segment", fault.getFailedFile());
                Assert.assertEquals(sealFailuresBefore + 3, instance.getCheckpointSealFailures());
                Assert.assertEquals(resetsBefore + 1, instance.getCheckpointTimelineResets());
                Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                assertTimelineAndMarkerRetired(instance);
                capture.assertLogged("live view checkpoint seal keeps failing, keeping the timeline and backing off "
                        + "[view=lv, consecutiveFailures=3, backoffMicros=60000000]");
                capture.assertLogged(owedRootText("live view O3 repair owes a root no seal can land before a newer row while its "
                        + "checkpoint seal backs off, retiring the timeline [view=lv, frontier=2026-01-01T00:03:00.000000Z, "
                        + "latestSeenTs=2026-01-01T00:04:00.000000Z]", day, isNanos));
                Assert.assertEquals(0, capture.countOccurrences("live view O3 repair kept its repair marker"));
                Assert.assertTrue(instance.isSealOnCooldown(currentMicros + 59_000_000L));
                Assert.assertFalse(instance.isSealOnCooldown(currentMicros + 61_000_000L));
                assertNoRefreshFaults("lv");
            }
            assertRestartsBlocked(
                    owedRootText(OWED_ROOT_DEPARTED_FRONTIER_ROWS, day, isNanos),
                    owedRootText(OWED_ROOT_HISTORY_FLOOR, day, isNanos),
                    "timeline is absent"
            );
        });
    }

    /**
     * Asserts the repair marker reads as live: on disk, naming {@code markerBaseGeneration}, under
     * the splice's generation one past it, so a restore refuses the timeline.
     */
    private void assertRepairMarkerLive(LiveViewInstance instance, long markerBaseGeneration) {
        assertMarkerNames(markerBaseGeneration);
        Assert.assertEquals("no root has landed past the splice", markerBaseGeneration + 1, generation(instance));
    }

    /**
     * Asserts the repair marker is still on disk, naming {@code markerBaseGeneration}, under a
     * generation two or more past it: a restore reads it as stale and clears it.
     */
    private void assertRepairMarkerStale(LiveViewInstance instance, long markerBaseGeneration) {
        assertMarkerNames(markerBaseGeneration);
        Assert.assertTrue("a root has landed past the splice", generation(instance) > markerBaseGeneration + 1);
    }

    /**
     * Restarts twice over a live repair marker. Each restart rebuilds from the applied base, which
     * the restatement guard refuses over the day the base lost, on {@code historyFloor}: the view
     * stops with {@code rows}, every row it held, and {@code live_views()} reports the block.
     */
    private void assertRestartsBlockedByTheRepairMarker(String rows, String historyFloor) throws Exception {
        assertRestartsBlocked(rows, historyFloor, "prefix preservation repair marker present");
    }

    /**
     * Under a duration cadence of {@code durationCadenceMicros}, at or below zero, every flush
     * seals, so the owed-root fixture leaves a root at the frontier 00:04, which the late row's
     * splice re-versions: the closing seal only re-stamps the head, writing no data segment, so
     * the fault armed for one stays unmet and the marker clears.
     */
    private void assertResumeUnderASealPerFlushOwesNoRoot(long durationCadenceMicros) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_MAX_DURATION_MICROS, durationCadenceMicros);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                assertCheckpointLadder(instance, false, """
                        2025-12-31T12:00:00.000000Z -> 1
                        2026-01-01T00:01:00.000000Z -> 2
                        2026-01-01T00:02:00.000000Z -> 3
                        2026-01-01T00:04:00.000000Z -> 5
                        """);
                final long generationBefore = generation(instance);
                hooks(job).setSimulateResumeHeadSealStart(
                        () -> fault.arm(LiveViewCheckpointDiskFullFault.OP_ALLOCATE, LiveViewCheckpointLayout.DATA_DIR_NAME, 1)
                );
                commitDeparture(job, "('2026-01-01T00:01:30.000000Z', 'a', 1.0)");
                hooks(job).setSimulateResumeHeadSealStart(null);
                fault.disarm();
                Assert.assertNull("a re-stamp writes no data segment", fault.getFailedFile());
                Assert.assertEquals(ts("2026-01-01T00:04:00.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals("the splice alone", generationBefore + 1, generation(instance));
                assertCheckpointLadder(instance, false, """
                        2025-12-31T12:00:00.000000Z -> 1
                        2026-01-01T00:01:00.000000Z -> 2
                        2026-01-01T00:02:00.000000Z -> 4
                        2026-01-01T00:04:00.000000Z -> 6
                        """);
                assertNoRepairMarker(instance);
                commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'a', 1.0)");
                Assert.assertEquals(ts("2026-01-01T00:05:00.000000Z"), instance.getHeadCheckpointMaxTs());
                assertNoRefreshFaults("lv");
            }
            for (int restart = 0; restart < 2; restart++) {
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(OWED_ROOT_ROWS + OWED_ROOT_ROW_0005, DEPARTURE_ROWS_QUERY);
            }
        });
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
        assertRestartRebuildsOverALostRowAmongUnconsumedRowsInItsInterval(false, null, viewSql, expectedRows, unconsumedCommits);
    }

    // As above, over a base whose designated timestamp has nanosecond precision when isNanos,
    // with expectedRows and declineLine, when not null, written for a microsecond base: the
    // first restart logs declineLine in the view's own unit.
    private void assertRestartRebuildsOverALostRowAmongUnconsumedRowsInItsInterval(
            boolean isNanos,
            @Nullable String declineLine,
            String viewSql,
            String expectedRows,
            String... unconsumedCommits
    ) throws Exception {
        seedALostRowAmongUnconsumedRowsInItsInterval(isNanos, viewSql, null, unconsumedCommits);
        final LogCapture capture = restartAndRefreshCapturingLog();
        if (declineLine != null) {
            capture.assertLogged(owedRootText(declineLine, "2026-01-01", isNanos));
        }
        try (LiveViewRefreshJob resumed = new LiveViewRefreshJob(0, engine, 1)) {
            commitAndRefresh(resumed, "('2026-01-01T02:00:20.000000Z', 'a', 8)");
            driveRefreshToQuiescence(resumed);
        }
        assertViewRows(owedRootText(expectedRows, "2026-01-01", isNanos));
        assertRebuiltFromAppliedBase("lv");
        Assert.assertFalse(viewInstance().isCheckpointRecoveryBlocked());
        assertViewMatchesRecompute(viewSql);

        restartAndRefresh();
        assertRestoredFromTimeline("lv");
        assertNoRefreshFaults("lv");
        assertViewRows(owedRootText(expectedRows, "2026-01-01", isNanos));
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
        commitHeadMissRootAtTheFrontierThenRowsAboveIt(job, isDayLost, isAboveSealed, false);
    }

    // As above, over the view createHeadMissDepartureView(isNanos) created.
    private void commitHeadMissRootAtTheFrontierThenRowsAboveIt(
            LiveViewRefreshJob job,
            boolean isDayLost,
            boolean isAboveSealed,
            boolean isNanos
    ) throws Exception {
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
                isNanos,
                owedRootText(
                        "2025-12-31T23:55:00.000000Z -> 1\n2026-01-01T00:56:00.000000Z -> 58\n"
                                + (isAboveSealed ? "2026-01-01T00:59:00.000000Z -> 59\n" : ""),
                        "2026-01-01",
                        isNanos
                )
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
     * Over the day the base loses when {@code isDayLost}, rows at 00:01, 00:02 and 00:03, a root
     * each. A chained correction moves 00:02 out of the filter while 00:03 stays, so its splice
     * re-versions the root at 00:02 with the row below it, a root that holds no row of its own.
     * With the chain declined, 00:03 then departs: the resume anchors on the rowless root and
     * replays nothing, so a frontier truncate drops the anchor ahead of the watermark at 00:01.
     * The crash image the armed {@code crash} takes in that departure's turn is then restored and
     * the process restarted from it.
     */
    private void crashInTheZeroRowFrontierTruncateAndRestart(ZeroRowFrontierTruncateCrash crash, boolean isDayLost) throws Exception {
        createDepartureView();
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveSeedToCompletion(job, "lv");
            if (isDayLost) {
                commitTheDayTheBaseLoses(job);
            }
            commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'a', 1.5)");
            commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', 2.5)");
            commitDeparture(job, "('2026-01-01T00:03:00.000000Z', 'a', 3.5)");
            commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', -2.5)");
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 0);
            final LiveViewInstance instance = viewInstance();
            final long generationBefore = generation(instance);
            try (Path checkpoints = checkpointsDir(instance).slash(); Path crashImage = crashImageDir()) {
                crash.arm(() -> TestUtils.copyDirectory(checkpoints, crashImage, configuration.getMkDirMode()));
                commitDeparture(job, "('2026-01-01T00:03:00.000000Z', 'a', -3.5)");
                crash.disarm();
            }
            Assert.assertTrue("the frontier truncate must write its marker", crash.hasCrashed());
            Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.TRUNCATE_ZERO_ROW_KEEP_COUNT));
            Assert.assertEquals("the truncate, the frontier truncate and the watermark", generationBefore + 3, generation(instance));
            try (
                    Path crashImage = crashImageDir();
                    LiveViewCheckpointSuperblock superblock = new LiveViewCheckpointSuperblock(configuration)
            ) {
                superblock.of(crashImage);
                Assert.assertTrue(superblock.isValid());
                Assert.assertEquals(
                        crash.isAfterThePublication() ? "the frontier truncate has published" : "the frontier truncate has not published",
                        generationBefore + (crash.isAfterThePublication() ? 2 : 1),
                        superblock.generation
                );
                Assert.assertEquals(
                        "the frontier truncate's marker names the first truncate's generation",
                        generationBefore + 1,
                        LiveViewCheckpointRepairMarker.readBaseGeneration(configuration, crashImage)
                );
                Assert.assertEquals(
                        "a marker written after the replacement commit records no seqTxn",
                        Numbers.LONG_NULL,
                        LiveViewCheckpointRepairMarker.readLvSeqTxn(configuration, crashImage)
                );
            }
        }
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

    // The crash image crashAtThe*FrontierTruncateMarker took: the superblock still names the
    // splice's generation, the truncate's marker names it too, and the marker records no seqTxn,
    // because the truncate wrote it after the replacement commit.
    private void assertTheCrashImageMarkerRecordsNoSeqTxn(long spliceGeneration) {
        try (
                Path crashImage = crashImageDir();
                LiveViewCheckpointSuperblock superblock = new LiveViewCheckpointSuperblock(configuration)
        ) {
            superblock.of(crashImage);
            Assert.assertTrue(superblock.isValid());
            Assert.assertEquals("the truncate has not published", spliceGeneration, superblock.generation);
            Assert.assertEquals(
                    "the truncate's marker names the splice's generation",
                    spliceGeneration,
                    LiveViewCheckpointRepairMarker.readBaseGeneration(configuration, crashImage)
            );
            Assert.assertEquals(
                    "a marker written after the replacement commit records no seqTxn",
                    Numbers.LONG_NULL,
                    LiveViewCheckpointRepairMarker.readLvSeqTxn(configuration, crashImage)
            );
        }
    }

    /**
     * Commits {@code departure} and copies the view's checkpoint directory to a crash image the
     * moment the frontier truncate's marker lands: the marker records the splice's generation and
     * no seqTxn, and the truncate has not published yet. The turn then runs on to its end.
     */
    private void crashAtTheFrontierTruncateMarker(DepartureMarkerCrash crash, LiveViewRefreshJob job, String departure) throws Exception {
        final LiveViewInstance instance = viewInstance();
        final long generationBefore = generation(instance);
        try (Path checkpoints = checkpointsDir(instance).slash(); Path crashImage = crashImageDir()) {
            crash.arm(() -> TestUtils.copyDirectory(checkpoints, crashImage, configuration.getMkDirMode()));
            commitDeparture(job, departure);
            crash.disarm();
        }
        Assert.assertTrue("the frontier truncate must write its marker", crash.hasCrashed());
        Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT));
        assertTheCrashImageMarkerRecordsNoSeqTxn(generationBefore + 1);
    }

    /**
     * Commits {@code departure} and copies the view's checkpoint directory to a crash image the
     * moment the head-miss rebuild's frontier truncate writes its marker: the marker records the
     * splice's generation and no seqTxn, and the truncate has not published yet. The turn then
     * runs on to its end.
     */
    private void crashAtTheHeadMissFrontierTruncateMarker(DepartureMarkerCrash crash, LiveViewRefreshJob job, String departure) throws Exception {
        final LiveViewInstance instance = viewInstance();
        final long generationBefore = generation(instance);
        try (Path checkpoints = checkpointsDir(instance).slash(); Path crashImage = crashImageDir()) {
            crash.arm(() -> TestUtils.copyDirectory(checkpoints, crashImage, configuration.getMkDirMode()));
            commitDeparture(job, departure);
            crash.disarm();
        }
        Assert.assertTrue("the frontier truncate must write its marker", crash.hasCrashed());
        Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_FRONTIER_TRUNCATE_COUNT));
        assertTheCrashImageMarkerRecordsNoSeqTxn(generationBefore + 1);
    }

    /**
     * The head-miss owed-root fixture: over the day the base loses, a root at 00:50 under the rows
     * 00:51 to 00:55, which the cadence leaves unsealed. A late row at 00:49:30 then reaches the
     * head-miss rebuild's end-of-table splice, whose newest root, 00:50, sits below the frontier.
     */
    private LiveViewInstance commitFiveRowsAboveTheHeadMissRoot(LiveViewRefreshJob job) throws Exception {
        return commitFiveRowsAboveTheHeadMissRoot(job, false);
    }

    // As above, over the view createHeadMissDepartureView(isNanos) created.
    private LiveViewInstance commitFiveRowsAboveTheHeadMissRoot(LiveViewRefreshJob job, boolean isNanos) throws Exception {
        commitHeadMissDepartureFixture(job, true, 50);
        commitDeparture(job, minuteRowsOfA(51, 55, "1.0"));
        final LiveViewInstance instance = viewInstance();
        assertCheckpointLadder(
                instance,
                isNanos,
                owedRootText("2025-12-31T23:55:00.000000Z -> 1\n2026-01-01T00:50:00.000000Z -> 52\n", "2026-01-01", isNanos)
        );
        return instance;
    }

    private LiveViewInstance commitTwoRowsAboveTheNewestRoot(LiveViewRefreshJob job) throws Exception {
        return commitTwoRowsAboveTheNewestRoot(job, "2026-01-01", true);
    }

    /**
     * The owed-root fixture on {@code day}: the row 2025-12-31T12:00, whose day the base loses, by
     * a DROP PARTITION when {@code isDayDropped}, then roots at 00:01 and 00:02, each sealed once
     * the clock passed the duration cadence, then the rows 00:03 and 00:04 inside it, which no root
     * holds. A late row below 00:02 resumes from the root at 00:01, whose splice re-versions the
     * root at 00:02, below the frontier 00:04: the repair's closing seal owes the frontier a root.
     */
    private LiveViewInstance commitTwoRowsAboveTheNewestRoot(LiveViewRefreshJob job, String day, boolean isDayDropped) throws Exception {
        commitDeparture(job, "('2025-12-31T12:00:00.000000Z', 'a', 0.5)");
        if (isDayDropped) {
            execute("ALTER TABLE base DROP PARTITION LIST '2025-12-31'");
            drainWalQueue();
            driveRefreshToQuiescence(job);
        }
        setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
        commitDeparture(job, "('" + day + "T00:01:00.000000Z', 'a', 1.5)");
        setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
        commitDeparture(job, "('" + day + "T00:02:00.000000Z', 'a', 2.5)");
        commitDeparture(job, "('" + day + "T00:03:00.000000Z', 'a', 9.5), ('" + day + "T00:04:00.000000Z', 'a', 1.0)");
        return viewInstance();
    }

    private void createDepartureView() throws Exception {
        createDepartureView(false);
    }

    // The departure view, over a base whose designated timestamp has nanosecond precision when
    // isNanos.
    private void createDepartureView(boolean isNanos) throws Exception {
        execute("CREATE TABLE base (ts " + (isNanos ? "TIMESTAMP_NS" : "TIMESTAMP")
                + ", sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, sym)");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " + DEPARTURE_VIEW_SQL);
    }

    // A filtered view with a one-minute RANGE frame over a deduplicating base: under the default
    // cadence a correction of its newest rows reaches the head-miss end-of-table splice.
    private void createHeadMissDepartureView() throws Exception {
        createHeadMissDepartureView(false);
    }

    // As above, over a base whose designated timestamp has nanosecond precision when isNanos.
    private void createHeadMissDepartureView(boolean isNanos) throws Exception {
        execute("CREATE TABLE base (ts " + (isNanos ? "TIMESTAMP_NS" : "TIMESTAMP")
                + ", sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, sym)");
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

    /**
     * Commits the late row 00:49:30 over the head-miss owed-root fixture, with the data segment of
     * the head-miss rebuild's closing seal failing, and asserts the rebuild repaired it, cleared
     * the head, and failed its seal once.
     */
    private void failTheOwedHeadMissSeal(LiveViewRefreshJob job, LiveViewCheckpointDiskFullFault fault) throws Exception {
        final LiveViewInstance instance = viewInstance();
        final long headMissRepairsBefore = countHeadMissRepairs(job);
        final long sealFailuresBefore = instance.getCheckpointSealFailures();
        hooks(job).setSimulateHeadMissHeadSealStart(
                () -> fault.arm(LiveViewCheckpointDiskFullFault.OP_ALLOCATE, LiveViewCheckpointLayout.DATA_DIR_NAME, 1)
        );
        commitDeparture(job, "('2026-01-01T00:49:30.000000Z', 'a', 1.0)");
        hooks(job).setSimulateHeadMissHeadSealStart(null);
        fault.disarm();
        Assert.assertEquals("the head-miss rebuild repairs the late row", headMissRepairsBefore + 1, countHeadMissRepairs(job));
        Assert.assertNotNull("the closing seal must reach its data segment", fault.getFailedFile());
        Assert.assertEquals(sealFailuresBefore + 1, instance.getCheckpointSealFailures());
        Assert.assertEquals("the rebuild cleared the head", Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
    }

    /**
     * Commits {@code values}, a correction the resume from an anchor repairs, with {@code faultOp}
     * failing the data segment of the resume's closing seal, and asserts the seal failed once and
     * retired nothing.
     */
    private void failTheOwedResumeSeal(
            LiveViewRefreshJob job,
            LiveViewCheckpointDiskFullFault fault,
            int faultOp,
            String values
    ) throws Exception {
        final LiveViewInstance instance = viewInstance();
        final long resumesBefore = job.getStatsForTest().getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RESUME_FROM_ANCHOR);
        final long sealFailuresBefore = instance.getCheckpointSealFailures();
        final long resetsBefore = instance.getCheckpointTimelineResets();
        hooks(job).setSimulateResumeHeadSealStart(() -> fault.arm(faultOp, LiveViewCheckpointLayout.DATA_DIR_NAME, 1));
        commitDeparture(job, values);
        hooks(job).setSimulateResumeHeadSealStart(null);
        fault.disarm();
        Assert.assertEquals(
                "the resume from an anchor repairs the correction",
                resumesBefore + 1,
                job.getStatsForTest().getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RESUME_FROM_ANCHOR)
        );
        Assert.assertNotNull("the closing seal must reach its data segment", fault.getFailedFile());
        Assert.assertEquals(LiveViewCheckpointLayout.DATA_DIR_NAME, LiveViewCheckpointDiskFullFault.kindOf(fault.getFailedFile()));
        Assert.assertEquals(sealFailuresBefore + 1, instance.getCheckpointSealFailures());
        Assert.assertEquals("one failure retires nothing", resetsBefore, instance.getCheckpointTimelineResets());
    }

    /**
     * Fails the closing seal of the owed-root fixture's late row 00:01:30, then commits a row the
     * view's filter rejects inside the cadence, which seals nothing, and one past it, whose cycle
     * lands the owed root at the frontier 00:04.
     */
    private void landTheOwedRootOnARowlessCycle(
            LiveViewRefreshJob job,
            LiveViewCheckpointDiskFullFault fault,
            LiveViewInstance instance
    ) throws Exception {
        final long generationBefore = generation(instance);
        failTheOwedResumeSeal(job, fault, LiveViewCheckpointDiskFullFault.OP_ALLOCATE, "('2026-01-01T00:01:30.000000Z', 'a', 1.0)");
        commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'b', -1.0)");
        Assert.assertEquals("a cycle inside the cadence seals nothing", ts("2026-01-01T00:02:00.000000Z"), instance.getHeadCheckpointMaxTs());
        assertRepairMarkerLive(instance, generationBefore);
        setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
        commitDeparture(job, "('2026-01-01T00:06:00.000000Z', 'b', -1.0)");
        Assert.assertEquals("a cycle past the cadence seals the frontier", ts("2026-01-01T00:04:00.000000Z"), instance.getHeadCheckpointMaxTs());
        Assert.assertEquals("the splice and the owed root", generationBefore + 2, generation(instance));
        assertCheckpointLadder(instance, false, """
                2025-12-31T12:00:00.000000Z -> 1
                2026-01-01T00:01:00.000000Z -> 2
                2026-01-01T00:02:00.000000Z -> 4
                2026-01-01T00:04:00.000000Z -> 6
                """);
        assertRepairMarkerStale(instance, generationBefore);
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
        createView(viewSql, false);
    }

    // As above, over a base whose designated timestamp has nanosecond precision when isNanos.
    private void createView(String viewSql, boolean isNanos) throws Exception {
        execute("CREATE TABLE base (ts " + (isNanos ? "TIMESTAMP_NS" : "TIMESTAMP") + ", sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
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
        seedALostRowAmongUnconsumedRowsInItsInterval(false, viewSql, earlierRow, unconsumedCommits);
    }

    // As above, over a base whose designated timestamp has nanosecond precision when isNanos.
    private void seedALostRowAmongUnconsumedRowsInItsInterval(
            boolean isNanos,
            String viewSql,
            @Nullable String earlierRow,
            String... unconsumedCommits
    ) throws Exception {
        execute("CREATE TABLE base (ts " + (isNanos ? "TIMESTAMP_NS" : "TIMESTAMP") + ", sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY HOUR WAL");
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
            Assert.assertEquals(timestampOf("2026-01-01T02:00:00.000000Z", isNanos), instance.getHeadCheckpointMaxTs());
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
                    predecessor.maxTimestamp < timestampOf("2026-01-01T00:58:00.000000Z", isNanos)
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
        seedCorruptNewestRootUnderAHigherFrontierOverALossyBase(false, unconsumedRows);
    }

    // As above, over a base whose designated timestamp has nanosecond precision when isNanos.
    private void seedCorruptNewestRootUnderAHigherFrontierOverALossyBase(boolean isNanos, @Nullable String unconsumedRows) throws Exception {
        createView(VIEW_SQL, isNanos);
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            commitAndRefresh(job, "('2025-12-31T23:59:00.000000Z', 'a', 1000)");
            for (int commit = 1; commit <= 6; commit++) {
                appendAndRefresh(job, commit * 10, commit);
            }
            commitAndRefresh(job, "('" + timestamp(60) + "', 'b', 100), ('" + timestamp(70) + "', 'a', 7)");
            driveRefreshToQuiescence(job);
            Assert.assertEquals(timestampOf(timestamp(60), isNanos), viewInstance().getHeadCheckpointMaxTs());
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

    /**
     * Once armed, runs an action in the turn of a zero-row resume that held no capture, once its
     * frontier truncate's marker lands - the turn's second, the first truncate's being the first.
     * Unless {@code isAfterThePublication}, right after that marker's rename, ahead of the
     * truncate's publication. Otherwise at the second open of the superblock after it: the first
     * is the truncate's own, the second the read of the root the truncate left at the frontier,
     * once the truncate has published and before the watermark does.
     */
    private static final class ZeroRowFrontierTruncateCrash extends TestFilesFacadeImpl {
        private final boolean isAfterThePublication;
        private final AtomicInteger markerRenames = new AtomicInteger();
        private final AtomicInteger superblockOpens = new AtomicInteger();
        private volatile boolean hasCrashed;
        private volatile Runnable onCrash;

        ZeroRowFrontierTruncateCrash(boolean isAfterThePublication) {
            this.isAfterThePublication = isAfterThePublication;
        }

        @Override
        public long openRO(LPSZ name) {
            onSuperblockOpen(name);
            return super.openRO(name);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            onSuperblockOpen(name);
            return super.openRW(name, opts);
        }

        @Override
        public int rename(LPSZ from, LPSZ to) {
            final int result = super.rename(from, to);
            if (onCrash != null
                    && result == Files.FILES_RENAME_OK
                    && Utf8s.endsWithAscii(to, LiveViewCheckpointLayout.REPAIRING_MARKER_FILE_NAME)
                    && markerRenames.incrementAndGet() == 2
                    && !isAfterThePublication) {
                crash();
            }
            return result;
        }

        void arm(Runnable onCrash) {
            markerRenames.set(0);
            superblockOpens.set(0);
            hasCrashed = false;
            this.onCrash = onCrash;
        }

        void disarm() {
            onCrash = null;
        }

        boolean hasCrashed() {
            return hasCrashed;
        }

        boolean isAfterThePublication() {
            return isAfterThePublication;
        }

        private void crash() {
            final Runnable action = onCrash;
            onCrash = null;
            action.run();
            hasCrashed = true;
        }

        private void onSuperblockOpen(LPSZ name) {
            if (onCrash != null
                    && isAfterThePublication
                    && markerRenames.get() == 2
                    && Utf8s.endsWithAscii(name, LiveViewCheckpointLayout.TIMELINE_FILE_NAME)
                    && superblockOpens.incrementAndGet() == 2) {
                crash();
            }
        }
    }

    /**
     * Once armed, runs an action right after the turn's second repair marker rename lands, the
     * frontier truncate's, and from then until disarmed fails every removal under the checkpoint
     * directory, so a retire removes nothing.
     */
    private static final class FrontierTruncateDoubleFault extends TestFilesFacadeImpl {
        private final AtomicInteger markerRenames = new AtomicInteger();
        private volatile boolean hasFired;
        private volatile boolean isFailingRemovals;
        private volatile Runnable onFire;

        @Override
        public boolean removeQuiet(LPSZ name) {
            if (isFailingRemovals && Utf8s.containsAscii(name, LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME)) {
                return false;
            }
            return super.removeQuiet(name);
        }

        @Override
        public int rename(LPSZ from, LPSZ to) {
            final int result = super.rename(from, to);
            final Runnable action = onFire;
            if (action != null
                    && result == Files.FILES_RENAME_OK
                    && Utf8s.endsWithAscii(to, LiveViewCheckpointLayout.REPAIRING_MARKER_FILE_NAME)
                    && markerRenames.incrementAndGet() == 2) {
                onFire = null;
                action.run();
                isFailingRemovals = true;
                hasFired = true;
            }
            return result;
        }

        @Override
        public boolean rmdir(Path name, boolean haltOnError) {
            if (isFailingRemovals && Utf8s.containsAscii(name, LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME)) {
                return false;
            }
            return super.rmdir(name, haltOnError);
        }

        void arm(Runnable onFire) {
            markerRenames.set(0);
            hasFired = false;
            this.onFire = onFire;
        }

        void disarm() {
            onFire = null;
            isFailingRemovals = false;
        }

        boolean hasFired() {
            return hasFired;
        }
    }
}
