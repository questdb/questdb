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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.NanosTimestampDriver;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
import java.util.regex.Pattern;

/**
 * Acceptance coverage for the retention rule the versioned timeline replaced the
 * retained checkpoint ring with: inside one history epoch a logical checkpoint
 * entry is never removed, with one exception: a repair root - one a repair's
 * closing seal wrote only because it owed a root at its frontier, which the
 * checkpoint cadence did not call for - goes once it falls below the two newest
 * roots (see LiveViewCheckpointRepairRootTest). The ring bounded retention by count and bytes, so an
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
    // What the upsert of 00:01 to 100 over the rows 00:01 to 00:04, x 1 to 4, leaves: s sums x over
    // the row and the two rows of 'a' below it, by hand.
    private static final String CHANGE_BELOW_EVERY_ROOT_ROWS = """
            ts\tsym\tx\ts
            2026-01-01T00:01:00.000000Z\ta\t100.0\t100.0
            2026-01-01T00:02:00.000000Z\ta\t2.0\t102.0
            2026-01-01T00:03:00.000000Z\ta\t3.0\t105.0
            2026-01-01T00:04:00.000000Z\ta\t4.0\t9.0
            """;
    // CHANGE_BELOW_EVERY_ROOT_ROWS with the late row 00:01:30 of 0.5, which shifts the two sums above it.
    private static final String CHANGE_BELOW_EVERY_ROOT_LATE_ROW_ROWS = """
            ts\tsym\tx\ts
            2026-01-01T00:01:00.000000Z\ta\t100.0\t100.0
            2026-01-01T00:01:30.000000Z\ta\t0.5\t100.5
            2026-01-01T00:02:00.000000Z\ta\t2.0\t102.5
            2026-01-01T00:03:00.000000Z\ta\t3.0\t5.5
            2026-01-01T00:04:00.000000Z\ta\t4.0\t9.0
            """;
    // The forward row 00:05 of 5.0 above them: 5.0 + 4.0 + 3.0.
    private static final String CHANGE_BELOW_EVERY_ROOT_ROW_0005 = "2026-01-01T00:05:00.000000Z\ta\t5.0\t12.0\n";
    // What the upsert of 00:01 to 100 over commitFourRootsOverTheDayTheBaseLoses leaves, the row
    // of the day the base lost kept: s sums x over the row and the two rows of 'a' below it, by
    // hand; the rebuild's state reads the base, which no longer holds 12:00.
    private static final String CHANGE_BELOW_EVERY_ROOT_OVER_THE_LOST_DAY_ROWS = """
            ts\tsym\tx\ts
            2025-12-31T12:00:00.000000Z\ta\t0.5\t0.5
            2026-01-01T00:01:00.000000Z\ta\t100.0\t100.0
            2026-01-01T00:02:00.000000Z\ta\t2.0\t102.0
            2026-01-01T00:03:00.000000Z\ta\t3.0\t105.0
            2026-01-01T00:04:00.000000Z\ta\t4.0\t9.0
            """;
    // Too many open files: a fault a retry may get past.
    private static final int ERRNO_EMFILE = 24;
    // What a departure of the newest of two roots above the day the base loses leaves.
    private static final String DEPARTURE_ROWS_OVER_THE_LOST_DAY = """
            ts\tsym\tx\ts
            2025-12-31T12:00:00.000000Z\ta\t0.5\t0.5
            2026-01-01T00:01:00.000000Z\ta\t1.5\t2.0
            """;
    private static final String DEPARTURE_ROWS_QUERY = "SELECT ts, sym, x, s FROM lv";
    // The rows of 01-01 crashInTheAnchoredFrontierTruncateAndRestart's departures leave the view: the
    // daily running sum of a.
    private static final String ANCHORED_DEPARTURE_KEPT_ROWS = """
            2026-01-01T23:57:00.000000Z\ta\t1.5\t1.5
            2026-01-01T23:58:00.000000Z\ta\t2.5\t4.0
            """;
    // What the restatement guard logs when a restore that failed its count stands it down, so the
    // rebuild from the applied base runs without it.
    private static final String GUARD_STAND_DOWN = "live view rebuild from the applied base runs without the restatement guard";
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
    // The head-miss owed-root fixture's rows with the late rows 00:49:30 and 00:49:45, by hand: the
    // one-minute frame of each holds 00:49 and the late rows up to itself, and 00:50's holds all
    // four.
    private static final String HEAD_MISS_ROWS_WITH_BOTH_LATE_ROWS = headMissDepartureRows(true, 55).replace(
            "2026-01-01T00:50:00.000000Z\ta\t1.0\t2.0\n",
            "2026-01-01T00:49:30.000000Z\ta\t1.0\t2.0\n"
                    + "2026-01-01T00:49:45.000000Z\ta\t1.0\t3.0\n"
                    + "2026-01-01T00:50:00.000000Z\ta\t1.0\t4.0\n"
    );
    // The evidence the restatement guard refuses a rebuild on after the late row 00:00:30 went
    // into the base under the departure fixture: the view keeps the row 2025-12-31T12:00, whose
    // day the base lost.
    private static final String LATE_ROW_HISTORY_FLOOR = "the view holds rows from 2025-12-31T12:00:00.000000Z "
            + "but the base table's earliest row is at 2026-01-01T00:00:30.000000Z";
    // What the retire logs when its check of _timeline after the unlink fails: it cannot tell whether
    // the file went, and the view refuses the timeline as one the retire kept.
    private static final String KEPT_TIMELINE_CHECK_FAILED = "could not check the live view checkpoint timeline after a retire [view=lv";
    // What the retire logs when it keeps _timeline and cannot write the marker beside it.
    private static final String KEPT_TIMELINE_MARKER_WRITE_FAILED = "could not write the live view checkpoint repair marker "
            + "beside a timeline its retire could not remove [view=lv";
    // What the retire logs when it keeps _timeline and the view refuses no timeline yet: once for
    // each file a retire keeps, until a retire removes it.
    private static final String KEPT_TIMELINE_NEWLY_KEPT = "live view could not remove the checkpoint timeline it retired, "
            + "refusing it until a retire removes it [view=lv";
    // What a read or a write of the checkpoint timeline fails with while a retire's kept _timeline
    // stands.
    private static final String KEPT_TIMELINE_REFUSAL = "live view checkpoint timeline is retired, its _timeline could not be removed";
    // What a failed head seal logs.
    private static final String HEAD_SEAL_FAILED = "could not write live view head checkpoint [view=lv";
    // What a seal that would open a runtime history fails with while it cannot remove the repair
    // marker an older history left.
    private static final String LEFTOVER_MARKER_REFUSAL = "could not remove the repair marker an earlier live view checkpoint timeline left";
    // The cause the in-place recovery after a faulted refresh turn reports.
    private static final String MID_DRAIN_CAUSE = "mid-drain refresh failure";
    // What an upsert of the owed-root fixture's row 00:04 out of the filter leaves: 00:03's sum
    // takes 00:01 and 00:02, by hand.
    private static final String OWED_ROOT_DEPARTED_FRONTIER_ROWS = """
            ts\tsym\tx\ts
            2025-12-31T12:00:00.000000Z\ta\t0.5\t0.5
            2026-01-01T00:01:00.000000Z\ta\t1.5\t2.0
            2026-01-01T00:02:00.000000Z\ta\t2.5\t4.5
            2026-01-01T00:03:00.000000Z\ta\t9.5\t13.5
            """;
    // The ladder that upsert leaves once the root its repair owes the new frontier 00:03 lands: the
    // roots below it, and that root, which counts the four rows at or below 00:03.
    private static final String OWED_ROOT_DEPARTED_FRONTIER_LADDER = """
            2025-12-31T12:00:00.000000Z -> 1
            2026-01-01T00:01:00.000000Z -> 2
            2026-01-01T00:02:00.000000Z -> 3
            2026-01-01T00:03:00.000000Z -> 4
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
    // The ladder the departure of 00:02 from the roots at 00:01 and 00:02 above the day the base
    // loses leaves: its resume truncates the spliced timeline at the new frontier 00:01 and keeps
    // the root there, which counts the two rows at or below it.
    private static final String RESTAMP_DEPARTURE_LADDER = """
            2025-12-31T12:00:00.000000Z -> 1
            2026-01-01T00:01:00.000000Z -> 2
            """;
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
    // The rows the seeding view's base holds ahead of CREATE, one a minute from 00:01.
    private static final int SEED_ROWS = 8;
    // What the anchored view over tx computes, recomputed in plain SQL: each account's running sum
    // over a partition of the day the anchor resets on.
    private static final String ANCHORED_RECOMPUTE_SQL = "SELECT created_at, account_id, sum(amount) OVER (PARTITION BY account_id, bucket "
            + "ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sum FROM (SELECT created_at, "
            + "account_id, amount, timestamp_floor('1d', created_at, '1970-01-01T00:00:00.000000Z'::timestamp) AS bucket FROM tx)";
    private static final String ANCHORED_VIEW_SQL = "SELECT created_at, account_id, sum(amount) OVER w AS cumulative_sum FROM tx "
            + "WINDOW w AS (PARTITION BY account_id ORDER BY created_at ANCHOR DAILY '00:00')";
    // What a seed sweep's resume logs when it abandons the timeline for a re-sweep from offset 0.
    private static final String SEED_RESUME_ABANDONED = "could not restore live view from seed checkpoint [view=lv";
    // What a seed sweep's seal logs when it fails.
    private static final String SEED_SEAL_FAILED = "could not write live view seed checkpoint [view=lv";
    private static final String SEED_VIEW_SQL = "SELECT ts, sym, x, sum(x) OVER (PARTITION BY sym ORDER BY ts "
            + "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) s FROM base";
    // What the departure of 00:03 from the rows 00:01, 00:02 and 00:03 leaves, with the forward row
    // 00:04 of 1.0 and the late row 00:03:30 of 2.0: s sums x over the row and the two rows of 'a'
    // below it, by hand.
    private static final String SPLICED_DEPARTURE_LATE_ROW_ROWS = """
            ts\tsym\tx\ts
            2026-01-01T00:01:00.000000Z\ta\t1.5\t1.5
            2026-01-01T00:02:00.000000Z\ta\t2.5\t4.0
            2026-01-01T00:03:30.000000Z\ta\t2.0\t6.0
            2026-01-01T00:04:00.000000Z\ta\t1.0\t5.5
            """;
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
    public void testRestartPastACorruptNewestRootCancelledInItsHealKeepsTheTimelineForTheNextStart() throws Exception {
        // A DROP, an invalidation or an engine shutdown trips the breaker while the restart heals
        // the torn root 00:02. Nothing failed, so the heal hands the cancellation on rather than
        // return the failure the restart answers with a whole-view rebuild: the turn ends with the
        // timeline as the build left it, and the next start heals the root and restores from it.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final String day = "2026-01-01";
        assertMemoryLeak(() -> {
            createDepartureView(false);
            final long generationBefore;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job, day, false);
                generationBefore = generation(instance);
                corruptNewestRootDataSegment(instance);
            }
            final LogCapture capture = new LogCapture();
            capture.start();
            try {
                engine.getLiveViewRegistry().clear();
                engine.buildViewGraphs();
                final LiveViewInstance cancelled = viewInstance();
                final AtomicBoolean isCancelled = new AtomicBoolean();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    hooks(job).setSimulateCorruptRootHealScanStart(() -> {
                        cancelled.cancelRefresh();
                        isCancelled.set(true);
                    });
                    driveRefreshToQuiescence(job);
                }
                Assert.assertTrue("the restart must have reached the heal", isCancelled.get());
                capture.drain();
                capture.assertLogged("live view checkpoint restore fell back past corrupt roots, reconstructing [view=lv, "
                        + "predecessorMaxTs=2026-01-01T00:01:00.000000Z, corruptCeilingMaxTs=2026-01-01T00:02:00.000000Z]");
                capture.assertLogged("live view refresh cancelled [view=lv");
                capture.assertNotLogged("could not reconstruct corrupt live view checkpoint roots");
                capture.assertNotLogged("reconstructed corrupt live view checkpoint roots");
                capture.assertNotLogged("could not restore live view from checkpoint timeline");
                Assert.assertEquals("a cancelled heal must not start a rebuild", 0, cancelled.getCheckpointRebuildAttempts());
                Assert.assertFalse(cancelled.isInvalid());
                Assert.assertFalse(cancelled.isCheckpointRecoveryBlocked());
                Assert.assertEquals("a cancelled heal must not publish a generation", generationBefore, generation(cancelled));
                assertViewRows(owedRootText(PAUSE_FIXTURE_ROWS, day, false), DEPARTURE_ROWS_QUERY);

                restartAndRefresh();
                capture.drain();
                capture.assertLogged("reconstructed corrupt live view checkpoint roots [view=lv, "
                        + "predecessorMaxTs=2026-01-01T00:01:00.000000Z, corruptCeilingMaxTs=2026-01-01T00:02:00.000000Z, roots=");
            } finally {
                capture.stop();
            }
            assertRestoredFromTimeline("lv");
            assertViewRows(owedRootText(PAUSE_FIXTURE_ROWS, day, false), DEPARTURE_ROWS_QUERY);
            assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
        });
    }

    @Test
    public void testRestartPastACorruptNewestRootWhoseHealCannotWriteRetriesTheRestoreOverANanosecondBase() throws Exception {
        // testRestartPastACorruptNewestRootLogsBothRootsInTheViewsUnitOverANanosecondBase with the
        // first allocation of a data segment refused, as a full disk refuses it: the heal cannot
        // write the root it reconstructs, and logs the torn root 00:02 in nanoseconds. A full disk
        // says nothing about the roots, so the heal hands the fault on,
        // and the restart retries the restore under the flush-retry budget rather than rebuild the
        // view from the applied base. The base owes the view no commit, so the next one drives the
        // retry, whose heal now writes, and the view restores in place.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final String day = "2026-01-01";
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView(true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                corruptNewestRootDataSegment(commitTwoRowsAboveTheNewestRoot(job, day, false));
            }
            // s sums x over the row and the two rows of 'a' below it: 9.5 + 1.0 + 2.0.
            final String rowsWithTheNextCommit = owedRootText(
                    PAUSE_FIXTURE_ROWS + "2026-01-01T00:05:00.000000Z\ta\t2.0\t12.5\n",
                    day,
                    true
            );
            final LogCapture capture = new LogCapture();
            capture.start();
            try {
                fault.arm(LiveViewCheckpointDiskFullFault.OP_ALLOCATE, LiveViewCheckpointLayout.DATA_DIR_NAME, 1);
                engine.getLiveViewRegistry().clear();
                engine.buildViewGraphs();
                final LiveViewInstance instance = viewInstance();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveRefreshToQuiescence(job);
                    fault.disarm();
                    Assert.assertNotNull("the heal must reach a data segment", fault.getFailedFile());
                    capture.drain();
                    capture.assertLogged("live view checkpoint restore fell back past corrupt roots, reconstructing [view=lv, "
                            + "predecessorMaxTs=2026-01-01T00:01:00.000000000Z, corruptCeilingMaxTs=2026-01-01T00:02:00.000000000Z]");
                    capture.assertLogged("could not reconstruct corrupt live view checkpoint roots, retrying [view=lv, "
                            + "corruptCeilingMaxTs=2026-01-01T00:02:00.000000000Z, error=");
                    capture.assertLogged("could not restore live view from checkpoint timeline, retrying [view=lv");
                    capture.assertNotLogged("could not reconstruct corrupt live view checkpoint roots [view=");
                    capture.assertNotLogged("reconstructed corrupt live view checkpoint roots");
                    capture.assertNotLogged("live view restart rebuilding from applied base");
                    Assert.assertEquals("a full disk must not start a rebuild", 0, instance.getCheckpointRebuildAttempts());
                    Assert.assertFalse(instance.isInvalid());
                    Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
                    Assert.assertTrue("the failed restore must leave the window-state debt", instance.isWindowStateDirty());
                    Assert.assertEquals(0, instance.getCheckpointRuntimeRestores());
                    assertViewRows(owedRootText(PAUSE_FIXTURE_ROWS, day, true), DEPARTURE_ROWS_QUERY);

                    commitDeparture(job, "('" + day + "T00:05:00.000000Z', 'a', 2.0)");
                }
                capture.drain();
                capture.assertLogged("reconstructed corrupt live view checkpoint roots [view=lv, "
                        + "predecessorMaxTs=2026-01-01T00:01:00.000000000Z, corruptCeilingMaxTs=2026-01-01T00:02:00.000000000Z, roots=");
                capture.assertNotLogged("live view restart rebuilding from applied base");
                capture.assertNotLogged("live view recomputed window state from applied base");
                Assert.assertEquals(0, instance.getCheckpointRebuildAttempts());
                Assert.assertEquals("the retry must have restored from the timeline", 1, instance.getCheckpointRuntimeRestores());
                Assert.assertEquals("the full disk is the one fault", 1, instance.getRefreshFaultCount());
                Assert.assertFalse("the retry must have settled the window-state debt", instance.isWindowStateDirty());
                Assert.assertFalse(instance.isInvalid());
                assertViewRows(rowsWithTheNextCommit, DEPARTURE_ROWS_QUERY);
            } finally {
                capture.stop();
            }
            restartAndRefresh();
            assertRestoredFromTimeline("lv");
            assertViewRows(rowsWithTheNextCommit, DEPARTURE_ROWS_QUERY);
            assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
        });
    }

    @Test
    public void testRestartPastACorruptNewestRootWhoseHealFailsLogsItInTheViewsUnitOverANanosecondBase() throws Exception {
        // testRestartPastACorruptNewestRootLogsBothRootsInTheViewsUnitOverANanosecondBase with the
        // heal's splice failing past its data on an error that names no cause: the heal logs the
        // torn root 00:02 in nanoseconds and fails the restore, and the restart rebuilds the view
        // from the applied base, which still holds every row.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final String day = "2026-01-01";
        assertMemoryLeak(() -> {
            createDepartureView(true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                corruptNewestRootDataSegment(commitTwoRowsAboveTheNewestRoot(job, day, false));
            }
            final LogCapture capture = new LogCapture();
            capture.start();
            try {
                engine.getLiveViewRegistry().clear();
                engine.buildViewGraphs();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_DATA_PUBLISH);
                    // The restart's turn: the heal, its failed restore and the rebuild that answers it.
                    advanceClockToNextRefreshPass();
                    job.run();
                    job.setCheckpointTimelineTestFailureStage(0);
                    driveRefreshToQuiescence(job);
                }
                capture.drain();
                capture.assertLogged("live view checkpoint restore fell back past corrupt roots, reconstructing [view=lv, "
                        + "predecessorMaxTs=2026-01-01T00:01:00.000000000Z, corruptCeilingMaxTs=2026-01-01T00:02:00.000000000Z]");
                capture.assertLogged("could not reconstruct corrupt live view checkpoint roots [view=lv, "
                        + "corruptCeilingMaxTs=2026-01-01T00:02:00.000000000Z, error=");
                capture.assertLogged("test failure after live view checkpoint data publication");
                capture.assertNotLogged("could not reconstruct corrupt live view checkpoint roots, retrying");
                capture.assertNotLogged("reconstructed corrupt live view checkpoint roots");
            } finally {
                capture.stop();
            }
            assertRebuiltFromAppliedBase("lv");
            assertViewRows(owedRootText(PAUSE_FIXTURE_ROWS, day, true), DEPARTURE_ROWS_QUERY);
            assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
        });
    }

    @Test
    public void testRestartPastACorruptNewestRootWhoseHealHitsABugRebuildsFromTheAppliedBase() throws Exception {
        // A heal that fails on a throwable no CairoException stands for, a bug in its own code,
        // fails the restore as an error naming no cause does: the heal logs it, and the restart
        // rebuilds the view from the applied base, which still holds every row.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final String day = "2026-01-01";
        assertMemoryLeak(() -> {
            createDepartureView(false);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                corruptNewestRootDataSegment(commitTwoRowsAboveTheNewestRoot(job, day, false));
            }
            final LogCapture capture = new LogCapture();
            capture.start();
            try {
                engine.getLiveViewRegistry().clear();
                engine.buildViewGraphs();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    hooks(job).setSimulateCorruptRootHealScanStart(() -> {
                        throw new IllegalStateException("simulated corrupt-root heal bug");
                    });
                    driveRefreshToQuiescence(job);
                }
                capture.drain();
                capture.assertLogged("could not reconstruct corrupt live view checkpoint roots [view=lv, "
                        + "corruptCeilingMaxTs=2026-01-01T00:02:00.000000Z, "
                        + "error=java.lang.IllegalStateException: simulated corrupt-root heal bug");
                capture.assertNotLogged("could not reconstruct corrupt live view checkpoint roots, retrying");
                capture.assertNotLogged("reconstructed corrupt live view checkpoint roots");
            } finally {
                capture.stop();
            }
            assertRebuiltFromAppliedBase("lv");
            assertViewRows(owedRootText(PAUSE_FIXTURE_ROWS, day, false), DEPARTURE_ROWS_QUERY);
            assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
        });
    }

    @Test
    public void testRestartPastACorruptNewestRootWhoseHealMeetsABaseAlterRestoresAfterTheRecompile() throws Exception {
        // A base ALTER lands while the restart heals the torn root 00:02, after the restart compiled
        // the view's plan, so the heal's scan of the base refuses the newer metadata. That says
        // nothing about the roots, so the heal hands the refusal on rather than return the failure
        // the restart answers with a whole-view rebuild, which would run the same plan into the same
        // refusal. The drift recovery recompiles, heals the root and restores from the timeline.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final String day = "2026-01-01";
        assertMemoryLeak(() -> {
            createDepartureView(false);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                corruptNewestRootDataSegment(commitTwoRowsAboveTheNewestRoot(job, day, false));
            }
            final LogCapture capture = new LogCapture();
            capture.start();
            try {
                engine.getLiveViewRegistry().clear();
                engine.buildViewGraphs();
                final LiveViewInstance instance = viewInstance();
                final AtomicBoolean isAltered = new AtomicBoolean();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    hooks(job).setSimulateCorruptRootHealScanStart(() -> {
                        try {
                            execute("ALTER TABLE base ADD COLUMN y INT");
                        } catch (SqlException e) {
                            throw new AssertionError(e);
                        }
                        drainWalQueue();
                        isAltered.set(true);
                    });
                    driveRefreshToQuiescence(job);
                }
                Assert.assertTrue("the restart must have reached the heal", isAltered.get());
                capture.drain();
                capture.assertNotLogged("could not reconstruct corrupt live view checkpoint roots");
                capture.assertNotLogged("live view restart rebuilding from applied base");
                capture.assertNotLogged("live view recomputed window state from applied base");
                capture.assertLogged("reconstructed corrupt live view checkpoint roots [view=lv, "
                        + "predecessorMaxTs=2026-01-01T00:01:00.000000Z, corruptCeilingMaxTs=2026-01-01T00:02:00.000000Z, roots=");
                Assert.assertEquals("a base ALTER must not start a rebuild", 0, instance.getCheckpointRebuildAttempts());
                Assert.assertEquals("the drift recovery must have restored from the timeline", 1, instance.getCheckpointRuntimeRestores());
                Assert.assertEquals("the base ALTER is the one fault", 1, instance.getRefreshFaultCount());
                Assert.assertFalse(instance.isInvalid());
                Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
                Assert.assertFalse(instance.isWindowStateDirty());
                assertViewRows(owedRootText(PAUSE_FIXTURE_ROWS, day, false), DEPARTURE_ROWS_QUERY);
            } finally {
                capture.stop();
            }
            restartAndRefresh();
            assertRestoredFromTimeline("lv");
            assertViewRows(owedRootText(PAUSE_FIXTURE_ROWS, day, false), DEPARTURE_ROWS_QUERY);
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
    public void testDepartureFrontierSealThatFailsAndWhoseRetireKeepsOnlyTheTimelineBlocksTheRestart() throws Exception {
        // As above, with the retire that answers the failed seal failing on _timeline and the
        // segment directories: the truncated generation stays on disk, its newest root below the
        // frontier, and so does the truncate's marker beside it. The marker reads as live, so each
        // restart rebuilds rather than restore that generation, and the restatement guard refuses
        // the rebuild over the day the base lost: the view stops with every row.
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    failTheFrontierSealWithItsTimelineLocked(fault, job, false, true, false);
                }
                assertRestartsBlockedByTheRepairMarker(singleDepartureRows(true), OWED_ROOT_HISTORY_FLOOR);
                capture.drain();
                capture.assertNotLogged("does not match durable materialization");
                capture.assertNotLogged(GUARD_STAND_DOWN);
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testDepartureFrontierSealThatFailsAndWhoseRetireKeepsOnlyTheTimelineRebuildsOverACompleteBase() throws Exception {
        // As above, over a base that lost nothing: the restart rebuilds over the marker, and that
        // rebuild's retire removes the generation and the marker with it. The rebuild gives what a
        // recompute gives, and the restart after it restores the fresh history.
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    failTheFrontierSealWithItsTimelineLocked(fault, job, false, false, false);
                }
                restartAndRefresh();
                capture.drain();
                capture.assertLogged("live view restart rebuilding from applied base [view=lv, cause=prefix preservation repair marker present");
                assertRebuiltFromAppliedBase("lv");
                final LiveViewInstance instance = viewInstance();
                Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
                Assert.assertEquals("the rebuild opens a fresh history", 1, generation(instance));
                assertNoRepairMarker(instance);
                assertViewRows(singleDepartureRows(false), DEPARTURE_ROWS_QUERY);
                assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(singleDepartureRows(false), DEPARTURE_ROWS_QUERY);
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testDepartureFrontierSealThatFailsWithOnlyItsTimelineLockedOpensAFreshHistoryAtTheNextSeal() throws Exception {
        // A lock on the _timeline file alone: the retire that answers the failed seal removes the
        // segment directories and every file but _timeline, and keeps the truncate's marker beside
        // it. The view refuses the generation the superblock still names, whose segments are gone.
        // The lock goes with the departure's turn, so the next row's seal retries the retire, which
        // removes _timeline and the marker with it, and opens a fresh history at the row: no seal
        // meets the truncated generation's missing segments. A base column then has the view
        // restore its runtime in place from that history, and each restart restores from it. The
        // view keeps every row throughout.
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        final String rows = singleDepartureRows(true) + "2026-01-01T00:04:00.000000Z\ta\t8.0\t12.0\n";
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    failTheFrontierSealWithItsTimelineLocked(fault, job, false, true, true);
                    final LiveViewInstance instance = viewInstance();
                    final FilesFacade ff = configuration.getFilesFacade();
                    try (Path dir = checkpointsDir(instance); Path path = new Path()) {
                        Assert.assertFalse("the retire removes the metadata segments", ff.exists(LiveViewCheckpointLayout.metaDirPath(path, dir).$()));
                        Assert.assertFalse("and the data segments", ff.exists(LiveViewCheckpointLayout.dataDirPath(path, dir).$()));
                    }
                    Assert.assertTrue("and the view refuses the generation it kept", instance.isCheckpointTimelineRetirePending());
                    final long sealFailuresBefore = instance.getCheckpointSealFailures();
                    commitDeparture(job, "('2026-01-01T00:04:00.000000Z', 'a', 8.0)");
                    Assert.assertEquals("the next seal lands", sealFailuresBefore, instance.getCheckpointSealFailures());
                    Assert.assertFalse("its retry of the retire removes the kept file", instance.isCheckpointTimelineRetirePending());
                    Assert.assertEquals("and it opens a fresh history", 1, generation(instance));
                    assertCheckpointLadder(instance, false, "2026-01-01T00:04:00.000000Z -> 4\n");
                    assertNoRepairMarker(instance);
                    final long runtimeRestoresBefore = instance.getCheckpointRuntimeRestores();
                    execute("ALTER TABLE base ADD COLUMN y INT");
                    drainWalQueue();
                    driveRefreshToQuiescence(job);
                    Assert.assertEquals("the base column restores the runtime in place", runtimeRestoresBefore + 1, instance.getCheckpointRuntimeRestores());
                    Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
                    Assert.assertFalse(instance.isInvalid());
                }
                assertViewRows(rows, DEPARTURE_ROWS_QUERY);
                for (int restart = 0; restart < 2; restart++) {
                    restartAndRefresh();
                    assertRestoredFromTimeline("lv");
                    assertViewRows(rows, DEPARTURE_ROWS_QUERY);
                }
                capture.drain();
                capture.assertNotLogged("reset live view checkpoint directory");
                capture.assertNotLogged("does not match durable materialization");
                capture.assertNotLogged(GUARD_STAND_DOWN);
            });
        } finally {
            capture.stop();
        }
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
        assertFrontierTruncateWhosePublicationAndRetireFailKeepsTheRows(false, false);
    }

    @Test
    public void testDepartureFrontierTruncateWhoseRetireKeepsOnlyTheTimelineKeepsTheRowsOfTheDayTheBaseLost() throws Exception {
        // As above, with the retire failing only on _timeline and the segment directories: it
        // removes every other file, and keeps the truncate's marker beside the generation it could
        // not remove, so the in-place recovery and each restart decline that generation as they do
        // above, and the view stops with every row.
        assertFrontierTruncateWhosePublicationAndRetireFailKeepsTheRows(false, true);
    }

    @Test
    public void testDepartureFrontierTruncateWhoseMarkerRewriteFailsAfterItsUnlinkCrashAheadOfItsRetireKeepsTheRows() throws Exception {
        // The frontier truncate writes its marker over the splice's, and the rewrite fails the way
        // Windows fails it: the splice's record unlinked, the retried rename refused. The staged
        // record is the only one left - the truncate's, naming the splice's generation and recording
        // no seqTxn - and the write keeps it. The truncate then retires the timeline, and a crash as
        // that retire reaches _timeline leaves the splice's generation under the staged record. It
        // reads as live, so each restart rebuilds rather than restore that generation, and the
        // restatement guard refuses the rebuild over the day the base lost: the view stops with
        // every row.
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView();
                final long spliceGeneration;
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    commitTheDayTheBaseLoses(job);
                    commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'a', 1.5)");
                    commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', 2.5), ('2026-01-01T00:03:00.000000Z', 'a', 9.5)");
                    final LiveViewInstance instance = viewInstance();
                    spliceGeneration = generation(instance) + 1;
                    final AtomicBoolean isImageTaken = new AtomicBoolean();
                    try (Path checkpoints = checkpointsDir(instance).slash(); Path crashImage = crashImageDir()) {
                        fault.failTheNextMarkerRewrite();
                        fault.beforeTheNextTimelineUnlink(() -> {
                            TestUtils.copyDirectory(checkpoints, crashImage, configuration.getMkDirMode());
                            isImageTaken.set(true);
                        });
                        commitDeparture(job, "('2026-01-01T00:03:00.000000Z', 'a', -9.5)");
                    }
                    Assert.assertEquals("the truncate's marker rewrite fails", 1, fault.getFailedMarkerRewrites());
                    Assert.assertTrue("the crash image must be taken", isImageTaken.get());
                    Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT));
                }
                assertTheCrashImageMarkerRecordsNoSeqTxn(spliceGeneration);
                try (Path crashImage = crashImageDir(); Path path = new Path()) {
                    final FilesFacade ff = configuration.getFilesFacade();
                    LiveViewCheckpointLayout.repairingMarkerPath(path, crashImage);
                    Assert.assertFalse("the rewrite unlinked the splice's record", ff.exists(path.$()));
                    path.put(LiveViewCheckpointLayout.TMP_SUFFIX);
                    Assert.assertTrue("and kept the truncate's staged one", ff.exists(path.$()));
                }
                putTheCrashImageBack();
                restartAndRefresh();
                assertRebuildBlockedByTheRepairMarker(singleDepartureRows(true), OWED_ROOT_HISTORY_FLOOR);
                assertRestartsBlockedByTheRepairMarker(singleDepartureRows(true), OWED_ROOT_HISTORY_FLOOR);
                capture.drain();
                capture.assertNotLogged("does not match durable materialization");
                capture.assertNotLogged(GUARD_STAND_DOWN);
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testDepartureFrontierTruncateWhoseRetireKeepsOnlyTheTimelineOpensAFreshHistoryAtTheNextSeal() throws Exception {
        // The frontier truncate fails after its marker and its metadata, and the retire that answers
        // it keeps _timeline and the marker beside it: the splice's generation, whose root of the
        // departed row sits above the frontier, and which the view refuses. Once the fault is gone,
        // the next row's seal retries the retire, which removes that generation and the marker with
        // it, and opens a fresh history at the row. Each restart restores from it with every row,
        // over the day the base lost too.
        final FrontierTruncateDoubleFault fault = new FrontierTruncateDoubleFault(true);
        final String rows = singleDepartureRows(true) + "2026-01-01T00:04:00.000000Z\ta\t8.0\t12.0\n";
        assertMemoryLeak(fault, () -> {
            failTheFrontierTruncateWithItsTimelineLockedThenSeal(fault, 1);
            for (int restart = 0; restart < 2; restart++) {
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertNoRepairMarker(viewInstance());
                assertViewRows(rows, DEPARTURE_ROWS_QUERY);
            }
        });
    }

    @Test
    public void testDepartureFrontierTruncateWhoseRetireKeepsOnlyTheTimelineRestoresAfterTwoSeals() throws Exception {
        // As above, with two rows sealed in the fresh history the first one's seal opened. Each
        // restart restores from the newest root, which the running view sealed.
        final FrontierTruncateDoubleFault fault = new FrontierTruncateDoubleFault(true);
        // s sums x over the row and the two rows of 'a' below it.
        final String rows = singleDepartureRows(true) + """
                2026-01-01T00:04:00.000000Z\ta\t8.0\t12.0
                2026-01-01T00:05:00.000000Z\ta\t1.0\t11.5
                """;
        assertMemoryLeak(fault, () -> {
            failTheFrontierTruncateWithItsTimelineLockedThenSeal(fault, 2);
            for (int restart = 0; restart < 2; restart++) {
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertNoRepairMarker(viewInstance());
                assertViewRows(rows, DEPARTURE_ROWS_QUERY);
            }
        });
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
    public void testDepartureRestampThatTheViewTableReadDeclinesIsPublishedOnTheNextRowlessCycle() throws Exception {
        // The truncated timeline of the cases above keeps its newest root at the new frontier
        // 00:01, and the re-stamp's read of the view's table fails, which declines it: the head
        // stays clear, with the newest timestamp the view has seen still at the departed 00:02. The
        // first cycle without rows puts the head back on the root at the frontier at its own
        // commit, at once, since a cleared head carries no write time, rather than declining a
        // first root at 00:02. Each cycle past the cadence after it republishes the root at its
        // commit, which moves the base WAL floor up, and the restarts restore from it.
        assertDepartureRestampThatTheViewTableReadDeclinesIsPublishedOnTheNextRowlessCycle(false);
    }

    @Test
    public void testDepartureRestampThatTheViewTableReadDeclinesIsPublishedOnTheNextRowlessCycleOverANanosecondBase() throws Exception {
        // The same departure over a base whose designated timestamp has nanosecond precision: the
        // cycle reads the root at the frontier, and logs it, in the view's own unit.
        assertDepartureRestampThatTheViewTableReadDeclinesIsPublishedOnTheNextRowlessCycle(true);
    }

    @Test
    public void testClearedHeadPublicationThatFailsOnceLandsOnTheNextRowlessCycle() throws Exception {
        // The first cycle without rows after the declined re-stamp finds the root at the frontier
        // and its publication fails: the failure counts on the seal failure streak, as a failed
        // seal does, and leaves the head clear and the timeline as the repair left it. The head
        // still carries no write time, so the next cycle, with the clock where it was, publishes
        // the root at its own commit and ends the streak.
        final LiveViewTableReadFault fault = new LiveViewTableReadFault();
        final LogCapture capture = new LogCapture();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    final LiveViewInstance instance = departWithTheRestampsViewTableReadFailing(job, fault, false);
                    final long generationBefore = generation(instance);
                    final long sealFailuresBefore = instance.getCheckpointSealFailures();
                    final long resetsBefore = instance.getCheckpointTimelineResets();
                    hooks(job).setSimulateNoRowHeadCheckpointStart(
                            () -> job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH)
                    );
                    capture.start();
                    try {
                        // seqTxn 6.
                        commitDeparture(job, "('2026-01-01T00:03:00.000000Z', 'a', -1.0)");
                    } finally {
                        hooks(job).setSimulateNoRowHeadCheckpointStart(null);
                        job.setCheckpointTimelineTestFailureStage(0);
                    }
                    capture.drain();
                    capture.stop();
                    capture.assertLogged("could not advance live view head checkpoint base [view=lv, baseSeqTxn=6, consecutiveFailures=1, error=");
                    Assert.assertEquals("the publication fails", sealFailuresBefore + 1, instance.getCheckpointSealFailures());
                    Assert.assertEquals(1, instance.getSealFailureStreak());
                    Assert.assertEquals("and retires nothing", resetsBefore, instance.getCheckpointTimelineResets());
                    Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                    Assert.assertEquals(Numbers.LONG_NULL, instance.getLastCheckpointWrittenUs());
                    Assert.assertEquals(generationBefore, generation(instance));
                    Assert.assertEquals(5, instance.getCheckpointTimelineWalPurgeFloor());
                    Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.CLEARED_HEAD_PUBLICATION_COUNT));
                    Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT));

                    // seqTxn 7, with the clock where it was.
                    commitDeparture(job, "('2026-01-01T00:04:00.000000Z', 'a', -1.0)");
                    Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.CLEARED_HEAD_PUBLICATION_COUNT));
                    Assert.assertEquals("the next cycle publishes the root at the frontier", ts("2026-01-01T00:01:00.000000Z"), instance.getHeadCheckpointMaxTs());
                    Assert.assertEquals("at its commit", 7, instance.getHeadCheckpointBaseSeqTxn());
                    Assert.assertEquals(generationBefore + 1, generation(instance));
                    Assert.assertEquals("the older superblock slot holds the truncate", 5, instance.getCheckpointTimelineWalPurgeFloor());
                    Assert.assertEquals("the publication that lands is no failure", sealFailuresBefore + 1, instance.getCheckpointSealFailures());
                    Assert.assertEquals("and ends the streak", 0, instance.getSealFailureStreak());
                    assertCheckpointLadder(instance, false, RESTAMP_DEPARTURE_LADDER);
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
    public void testClearedHeadPublicationThatFailsThreeTimesInARowLandsAfterThePause() throws Exception {
        assertClearedHeadPublicationThatFailsThreeTimesInARow(false);
    }

    @Test
    public void testClearedHeadPublicationThatFailsThreeTimesInARowRetiresAtTheFirstFailureAfterThePause() throws Exception {
        assertClearedHeadPublicationThatFailsThreeTimesInARow(true);
    }

    @Test
    public void testClearedHeadPublicationInsideTheSealPauseWaitsItOut() throws Exception {
        // The first cycle without rows after the declined re-stamp runs inside a seal pause, which
        // holds the publication, as it holds every checkpoint write, without counting a failure:
        // the head stays clear. The first cycle after the pause publishes the root at the frontier.
        final LiveViewTableReadFault fault = new LiveViewTableReadFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = departWithTheRestampsViewTableReadFailing(job, fault, false);
                final long generationBefore = generation(instance);
                final long sealFailuresBefore = instance.getCheckpointSealFailures();
                // A pause that ends one microsecond on, so it holds this cycle alone.
                hooks(job).setSimulateNoRowHeadCheckpointStart(() -> instance.setSealCooldownUntilUs(currentMicros + 1));
                try {
                    // seqTxn 6.
                    commitDeparture(job, "('2026-01-01T00:03:00.000000Z', 'a', -1.0)");
                } finally {
                    hooks(job).setSimulateNoRowHeadCheckpointStart(null);
                }
                Assert.assertEquals("the pause holds the publication", Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(generationBefore, generation(instance));
                Assert.assertEquals("which is no failure", sealFailuresBefore, instance.getCheckpointSealFailures());
                Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.CLEARED_HEAD_PUBLICATION_COUNT));
                Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT));

                setCurrentMicros(currentMicros + 1);
                // seqTxn 7.
                commitDeparture(job, "('2026-01-01T00:04:00.000000Z', 'a', -1.0)");
                Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.CLEARED_HEAD_PUBLICATION_COUNT));
                Assert.assertEquals("the cycle after the pause publishes", ts("2026-01-01T00:01:00.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(7, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(generationBefore + 1, generation(instance));
                Assert.assertEquals(sealFailuresBefore, instance.getCheckpointSealFailures());
                assertCheckpointLadder(instance, false, RESTAMP_DEPARTURE_LADDER);
                assertNoRefreshFaults("lv");
            }
            restartAndRefresh();
            assertRestoredFromTimeline("lv");
            assertViewRows(DEPARTURE_ROWS_OVER_THE_LOST_DAY, DEPARTURE_ROWS_QUERY);
        });
    }

    @Test
    public void testClearedHeadPublicationOverADriftedCountRetiresTheTimeline() throws Exception {
        // The first cycle without rows after the declined re-stamp reads the view's table before it
        // looks for the root at the frontier, and meets a lifetime row count that drifted from the
        // table: it re-seats the count on the table's size, retires the timeline and publishes
        // nothing, as a seal over a drifted count does, which is neither a failure nor a decline.
        // With no timeline, the next cycle declines a first root at the newest timestamp the view
        // has seen and holds no base WAL, and each restart finds no timeline: the rebuild from the
        // applied base is refused over the day the base lost, and the view stops with every row.
        configureWalPurge();
        final LiveViewTableReadFault fault = new LiveViewTableReadFault();
        final LogCapture capture = new LogCapture();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    final LiveViewInstance instance = departWithTheRestampsViewTableReadFailing(job, fault, false);
                    final long mismatchesBefore = instance.getCheckpointRowCountMismatches();
                    final long sealFailuresBefore = instance.getCheckpointSealFailures();
                    hooks(job).setSimulateNoRowHeadCheckpointStart(() -> instance.addLvRowsTotal(1));
                    capture.start();
                    try {
                        // seqTxn 6.
                        commitDeparture(job, "('2026-01-01T00:03:00.000000Z', 'a', -1.0)");
                    } finally {
                        hooks(job).setSimulateNoRowHeadCheckpointStart(null);
                    }
                    capture.drain();
                    capture.stop();
                    capture.assertLogged("live view emitted row count does not match its durable output, retiring the timeline [view=lv");
                    Assert.assertEquals("the cycle meets the drift", mismatchesBefore + 1, instance.getCheckpointRowCountMismatches());
                    Assert.assertEquals("and re-seats the count on the table's size", 2, instance.getLvRowsTotal());
                    Assert.assertEquals("a drift is no failure", sealFailuresBefore, instance.getCheckpointSealFailures());
                    Assert.assertEquals(
                            "nor a decline",
                            0,
                            job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT)
                    );
                    Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.CLEARED_HEAD_PUBLICATION_COUNT));
                    Assert.assertEquals("nothing publishes", Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                    assertTimelineAndMarkerRetired(instance);

                    // seqTxn 7.
                    setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                    commitDeparture(job, "('2026-01-01T00:04:00.000000Z', 'a', -1.0)");
                    Assert.assertEquals(
                            "with no timeline the next cycle declines a first root above the newest row",
                            1,
                            job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT)
                    );
                    Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.CLEARED_HEAD_PUBLICATION_COUNT));
                    Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointBaseSeqTxn());
                    Assert.assertEquals(Numbers.LONG_NULL, instance.getCheckpointTimelineWalPurgeFloor());
                    Assert.assertEquals("and holds no base WAL", 0, purgeBaseWalAndCountSegments());
                    assertNoRefreshFaults("lv");
                }
                assertRestartsBlocked(DEPARTURE_ROWS_OVER_THE_LOST_DAY, OWED_ROOT_HISTORY_FLOOR, "timeline is absent");
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testClearedHeadPublicationWhoseTimelineReadFailsLeavesTheCycleToTheSeal() throws Exception {
        // The first cycle without rows after the declined re-stamp cannot read the timeline for the
        // root at the frontier: a root it cannot vet is no root to publish, so it leaves the cycle
        // to the seal, which declines a first root at the newest timestamp the view has seen, above
        // the newest row the view holds, and counts no failure. The next cycle reads the timeline
        // and publishes the root at its commit.
        final LiveViewTableReadFault fault = new LiveViewTableReadFault();
        final LogCapture capture = new LogCapture();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    final LiveViewInstance instance = departWithTheRestampsViewTableReadFailing(job, fault, false);
                    final long generationBefore = generation(instance);
                    final long sealFailuresBefore = instance.getCheckpointSealFailures();
                    hooks(job).setSimulateNoRowHeadCheckpointStart(() -> fault.failTheNextTimelineRead(instance));
                    capture.start();
                    try {
                        // seqTxn 6.
                        commitDeparture(job, "('2026-01-01T00:03:00.000000Z', 'a', -1.0)");
                    } finally {
                        hooks(job).setSimulateNoRowHeadCheckpointStart(null);
                    }
                    capture.drain();
                    capture.stop();
                    Assert.assertEquals("the read of the timeline fails", 1, fault.getFailedTimelineOpens());
                    capture.assertLogged("could not read the live view checkpoint root at the frontier [view=lv, frontier=2026-01-01T00:01:00.000000Z, error=");
                    Assert.assertEquals(
                            "the seal declines a first root above the newest row",
                            1,
                            job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT)
                    );
                    Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.CLEARED_HEAD_PUBLICATION_COUNT));
                    Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                    Assert.assertEquals(generationBefore, generation(instance));
                    Assert.assertEquals("no failure", sealFailuresBefore, instance.getCheckpointSealFailures());

                    // seqTxn 7.
                    commitDeparture(job, "('2026-01-01T00:04:00.000000Z', 'a', -1.0)");
                    Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.CLEARED_HEAD_PUBLICATION_COUNT));
                    Assert.assertEquals("the next cycle publishes", ts("2026-01-01T00:01:00.000000Z"), instance.getHeadCheckpointMaxTs());
                    Assert.assertEquals(7, instance.getHeadCheckpointBaseSeqTxn());
                    Assert.assertEquals(generationBefore + 1, generation(instance));
                    Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT));
                    Assert.assertEquals(sealFailuresBefore, instance.getCheckpointSealFailures());
                    assertCheckpointLadder(instance, false, RESTAMP_DEPARTURE_LADDER);
                    assertNoRefreshFaults("lv");
                }
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(DEPARTURE_ROWS_OVER_THE_LOST_DAY, DEPARTURE_ROWS_QUERY);
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testClearedHeadPublicationWhoseViewTableReadFailsLandsOnTheNextRowlessCycle() throws Exception {
        // The first cycle without rows after the declined re-stamp cannot read the view's table,
        // which says what row the root it publishes has to sit at: it logs the failed read and
        // publishes nothing, which counts neither a failure nor a decline. The next cycle reads the
        // table and publishes the root at the frontier at its commit.
        final LiveViewTableReadFault fault = new LiveViewTableReadFault();
        final LogCapture capture = new LogCapture();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    final LiveViewInstance instance = departWithTheRestampsViewTableReadFailing(job, fault, false);
                    final long generationBefore = generation(instance);
                    final long sealFailuresBefore = instance.getCheckpointSealFailures();
                    hooks(job).setSimulateNoRowHeadCheckpointStart(() -> fault.failTheNextViewTableRead(engine, instance));
                    capture.start();
                    try {
                        // seqTxn 6.
                        commitDeparture(job, "('2026-01-01T00:03:00.000000Z', 'a', -1.0)");
                    } finally {
                        hooks(job).setSimulateNoRowHeadCheckpointStart(null);
                    }
                    Assert.assertEquals("the cycle's read of the view's table fails", 2, fault.getFailedTxnOpens());
                    Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                    Assert.assertEquals(generationBefore, generation(instance));
                    Assert.assertEquals("a failed read is no failure", sealFailuresBefore, instance.getCheckpointSealFailures());
                    Assert.assertEquals(
                            "nor a decline",
                            0,
                            job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT)
                    );
                    Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.CLEARED_HEAD_PUBLICATION_COUNT));

                    // seqTxn 7.
                    commitDeparture(job, "('2026-01-01T00:04:00.000000Z', 'a', -1.0)");
                    capture.drain();
                    capture.stop();
                    Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.CLEARED_HEAD_PUBLICATION_COUNT));
                    Assert.assertEquals("the next cycle publishes", ts("2026-01-01T00:01:00.000000Z"), instance.getHeadCheckpointMaxTs());
                    Assert.assertEquals(7, instance.getHeadCheckpointBaseSeqTxn());
                    Assert.assertEquals(generationBefore + 1, generation(instance));
                    Assert.assertEquals(sealFailuresBefore, instance.getCheckpointSealFailures());
                    Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT));
                    // The failed read logs once, and the read that lands logs nothing.
                    Assert.assertEquals(
                            1,
                            capture.countOccurrences("could not measure live view rows before a checkpoint seal [view=lv, error=")
                    );
                    assertCheckpointLadder(instance, false, RESTAMP_DEPARTURE_LADDER);
                    assertNoRefreshFaults("lv");
                }
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(DEPARTURE_ROWS_OVER_THE_LOST_DAY, DEPARTURE_ROWS_QUERY);
            });
        } finally {
            capture.stop();
        }
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
                // The view holds no row below the rowless anchor, so the resume derives no state.
                Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.RESUME_FRONTIER_STATE_RESTORE_COUNT));
                Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.RESUME_FRONTIER_STATE_WARM_UP_COUNT));
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
    public void testAnchoredZeroRowResumeCrashAfterItsFrontierTruncatePublishesOverALostDayBlocksTheRestart() throws Exception {
        // As below, over a base that lost a day the view keeps: the restatement guard refuses the
        // rebuild, and the view stops with every row it holds.
        final ZeroRowFrontierTruncateCrash crash = new ZeroRowFrontierTruncateCrash(true);
        assertMemoryLeak(crash, () -> {
            crashInTheAnchoredFrontierTruncateAndRestart(crash, true);
            final String rows = "ts\tsym\tx\ts\n2025-12-31T12:00:00.000000Z\ta\t0.5\t0.5\n" + ANCHORED_DEPARTURE_KEPT_ROWS;
            final String historyFloor = "the view holds rows from 2025-12-31T12:00:00.000000Z "
                    + "but the base table's earliest row is at 2026-01-01T23:57:00.000000Z";
            assertRebuildBlockedByTheRepairMarker(rows, historyFloor);
            assertRestartsBlockedByTheRepairMarker(rows, historyFloor);
        });
    }

    @Test
    public void testAnchoredZeroRowResumeCrashAfterItsFrontierTruncatePublishesRebuilds() throws Exception {
        // An anchored view's newest day departs in two corrections, the second with the chain
        // declined: its resume derives the state at the frontier 23:58, below its anchor, and its
        // truncate at the frontier drops every root at or above 23:58, the seal at the frontier to
        // follow. The crash lands after that truncate published and before the seal: the truncate's
        // marker records no seqTxn and reads as live, so the restart rebuilds from the applied base,
        // which lost nothing, and the late row at 23:59 folds over the rebuilt state.
        final ZeroRowFrontierTruncateCrash crash = new ZeroRowFrontierTruncateCrash(true);
        assertMemoryLeak(crash, () -> {
            crashInTheAnchoredFrontierTruncateAndRestart(crash, false);
            assertRebuiltFromAppliedBase("lv");
            Assert.assertFalse(viewInstance().isCheckpointRecoveryBlocked());
            final String rows = "ts\tsym\tx\ts\n" + ANCHORED_DEPARTURE_KEPT_ROWS;
            assertViewRows(rows, DEPARTURE_ROWS_QUERY);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                commitDeparture(job, "('2026-01-01T23:59:00.000000Z', 'a', 1.0)");
            }
            assertViewRows(rows + "2026-01-01T23:59:00.000000Z\ta\t1.0\t5.0\n", DEPARTURE_ROWS_QUERY);
            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    "SELECT ts, sym, x, sum(x) OVER (PARTITION BY sym, timestamp_floor('d', ts) ORDER BY ts) s FROM base WHERE x > 0 ORDER BY ts, sym",
                    "(lv) ORDER BY ts, sym",
                    LOG,
                    true
            );
            assertNoRefreshFaults("lv");
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
    public void testDepartureThatLeavesOnlyRowsBelowEveryRootCrashAheadOfItsRetireBlocksTheRestart() throws Exception {
        // As below, over a row of the day the base loses that rode in with 00:01: the head-miss
        // rebuild that deletes 00:01 leaves that root holding the lost day's row alone, and the
        // departure of 00:05 leaves the view holding that row, below every root. The frontier
        // truncate keeps no prefix, so it retires the timeline, and its marker, written after the
        // replacement commit, names the splice's generation and records no seqTxn. A crash as that
        // retire reaches _timeline leaves the generation under the marker, which reads as live:
        // each restart rebuilds rather than restore it, and the restatement guard refuses the
        // rebuild over the day the base lost. The view stops with its row.
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        final String rows = """
                ts\tsym\tx\ts
                2025-12-31T12:00:00.000000Z\ta\t0.5\t0.5
                """;
        final String historyFloor = "the view holds rows from 2025-12-31T12:00:00.000000Z "
                + "but the base table's earliest row is at 2026-01-01T00:05:00.000000Z";
        assertMemoryLeak(fault, () -> {
            createHeadMissDepartureView();
            final long spliceGeneration;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitDeparture(job, "('2025-12-31T12:00:00.000000Z', 'a', 0.5), ('2026-01-01T00:01:00.000000Z', 'a', 1.5)");
                execute("ALTER TABLE base DROP PARTITION LIST '2025-12-31'");
                drainWalQueue();
                driveRefreshToQuiescence(job);
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
                        2026-01-01T00:01:00.000000Z -> 1
                        2026-01-01T00:05:00.000000Z -> 2
                        """);
                spliceGeneration = generation(instance) + 1;
                final AtomicBoolean isImageTaken = new AtomicBoolean();
                try (Path checkpoints = checkpointsDir(instance).slash(); Path crashImage = crashImageDir()) {
                    fault.beforeTheNextTimelineUnlink(() -> {
                        TestUtils.copyDirectory(checkpoints, crashImage, configuration.getMkDirMode());
                        isImageTaken.set(true);
                    });
                    commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'a', -2.5)");
                }
                Assert.assertTrue("the crash image must be taken", isImageTaken.get());
                Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT));
                // Past the crash point the running view goes on: the retire removes the timeline
                // and the marker, and the seal at the new frontier opens a fresh history there.
                Assert.assertEquals("the seal at the new frontier opens a fresh history", 1, generation(instance));
                assertCheckpointLadder(instance, false, "2025-12-31T12:00:00.000000Z -> 1\n");
                assertNoRepairMarker(instance);
            }
            assertViewRows(rows, DEPARTURE_ROWS_QUERY);
            assertTheCrashImageMarkerRecordsNoSeqTxn(spliceGeneration);
            putTheCrashImageBack();
            restartAndRefresh();
            assertRebuildBlockedByTheRepairMarker(rows, historyFloor);
            assertRestartsBlockedByTheRepairMarker(rows, historyFloor);
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
    public void testHeadMissDepartureFrontierSealThatFailsAndWhoseRetireKeepsOnlyTheTimelineBlocksTheRestart() throws Exception {
        // The head-miss rebuild's frontier truncate published and its seal at the new frontier
        // failed, so the truncated generation retires, with _timeline and the segment directories
        // locked: the generation stays on disk, its only root below the kept row, and the
        // truncate's marker stays beside it. Each restart rebuilds over the marker, and the
        // restatement guard refuses the rebuild over the day the base lost: the view stops with
        // every row.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createHeadMissDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    failTheFrontierSealWithItsTimelineLocked(fault, job, true, true, false);
                }
                assertRestartsBlockedByTheRepairMarker(HEAD_MISS_KEPT_ROW_DEPARTURE_ROWS, HEAD_MISS_OWED_ROOT_HISTORY_FLOOR);
                capture.drain();
                capture.assertNotLogged("does not match durable materialization");
                capture.assertNotLogged(GUARD_STAND_DOWN);
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
        // retires on the way out, and neither a head nor a base WAL arm the view held survives it.
        // Over a base that lost nothing the restart rebuilds what a recompute gives.
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
                Assert.assertEquals("no head survives the retire", Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                Assert.assertEquals("nor the head's base WAL arm", Numbers.LONG_NULL, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals("nor the timeline's", Numbers.LONG_NULL, instance.getCheckpointTimelineWalPurgeFloor());
                Assert.assertFalse(instance.isCheckpointTimelineRetirePending());
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
        assertFrontierTruncateWhosePublicationAndRetireFailKeepsTheRows(true, false);
    }

    @Test
    public void testHeadMissDepartureFrontierTruncateWhoseRetireKeepsOnlyTheTimelineKeepsTheRowsOfTheDayTheBaseLost() throws Exception {
        // As above, with the retire failing only on _timeline and the segment directories: the
        // truncate's marker stays beside the generation the retire could not remove.
        assertFrontierTruncateWhosePublicationAndRetireFailKeepsTheRows(true, true);
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
    public void testTornStagedMarkerBesideATimelineItsRebuildCannotRemoveRebuildsEachRestart() throws Exception {
        // A torn staged marker names no generation, so nothing can read it as stale: every restart
        // rebuilds over it, and the rebuild's retire is what removes it, with the timeline. While
        // _timeline is locked that retire keeps both, and the rows sealed after the rebuild land on
        // top of the generation it kept, so the next restart rebuilds again rather than restore.
        // Once the lock is gone, the rebuild's retire removes both, and the restart after it
        // restores the fresh history.
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        final String markerRebuild = "live view restart rebuilding from applied base [view=lv, cause=prefix preservation repair marker present";
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    final LiveViewInstance instance = buildHistory(job);
                    try (Path dir = checkpointsDir(instance); Path path = new Path()) {
                        LiveViewCheckpointLayout.repairingMarkerPath(path, dir).put(LiveViewCheckpointLayout.TMP_SUFFIX);
                        Assert.assertTrue(configuration.getFilesFacade().touch(path.$()));
                    }
                }
                fault.lock(false);
                try {
                    restartAndRefresh();
                    assertRebuiltFromAppliedBase("lv");
                    assertTornStagedMarkerBesideTheTimeline(viewInstance());
                    assertViewMatchesRecompute();
                    try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                        appendAndRefresh(job, SEALS * 10 + 10, SEALS + 1);
                        appendAndRefresh(job, SEALS * 10 + 20, SEALS + 2);
                        driveRefreshToQuiescence(job);
                    }
                    restartAndRefresh();
                    assertRebuiltFromAppliedBase("lv");
                    assertTornStagedMarkerBesideTheTimeline(viewInstance());
                    assertViewMatchesRecompute();
                } finally {
                    fault.unlock();
                }
                restartAndRefresh();
                assertRebuiltFromAppliedBase("lv");
                final LiveViewInstance instance = viewInstance();
                Assert.assertEquals("the rebuild opens a fresh history", 1, generation(instance));
                assertNoRepairMarker(instance);
                assertViewMatchesRecompute();
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewMatchesRecompute();
                capture.drain();
                Assert.assertEquals(3, capture.countOccurrences(markerRebuild));
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testTruncateWithNoPrefixCrashAheadOfItsRetireRestoresTheTimeline() throws Exception {
        // The crash image of the truncate below, taken as its retire reaches _timeline: the
        // generation the truncate found, and its marker naming that generation and the view's last
        // seqTxn. The view committed nothing past that seqTxn, so the restart reads the marker as
        // stale, clears it, and restores the generation, which still describes the view's output.
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        assertMemoryLeak(fault, () -> {
            createView();
            final LiveViewInstance instance;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                instance = buildHistory(job);
            }
            final long generationBefore = generation(instance);
            final long lvSeqTxn = engine.getTableSequencerAPI().lastTxn(instance.getLiveViewToken());
            final AtomicBoolean isImageTaken = new AtomicBoolean();
            try (
                    LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1);
                    Path checkpoints = checkpointsDir(instance).slash();
                    Path crashImage = crashImageDir()
            ) {
                fault.beforeTheNextTimelineUnlink(() -> {
                    TestUtils.copyDirectory(checkpoints, crashImage, configuration.getMkDirMode());
                    isImageTaken.set(true);
                });
                Assert.assertFalse("no root sits below the floor", job.truncateOrRetireTimelineOnO3ForTest(instance, ts(timestamp(5))));
            }
            Assert.assertTrue("the crash image must be taken", isImageTaken.get());
            assertTheCrashImage(generationBefore, generationBefore, lvSeqTxn);
            putTheCrashImageBack();
            restartAndRefresh();
            assertRestoredFromTimeline("lv");
            final LiveViewInstance restored = viewInstance();
            Assert.assertEquals("the restore keeps the generation", generationBefore, generation(restored));
            assertNoRepairMarker(restored);
            assertViewMatchesRecompute();
        });
    }

    @Test
    public void testTruncateWithNoPrefixWhoseRetireKeepsTheTimelineOpensAFreshHistoryAtTheNextSeal() throws Exception {
        // A truncate whose floor sits below every root keeps no prefix and retires the timeline
        // instead. Its marker, written ahead of the truncate, names the generation and the view's
        // last seqTxn, and the retire, which fails on _timeline and the segment directories, keeps
        // the file and refuses the generation it names. The view has committed nothing past that
        // seqTxn, so the truncate's marker reads as stale there, and the retire writes its own in
        // its place, which names no generation and so reads as live. The lock goes, and the next
        // row's seal retries the retire, which removes the file and the marker with it, and opens a
        // fresh history at the row, which the restart restores.
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        assertMemoryLeak(fault, () -> {
            createView();
            final LiveViewInstance instance;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                instance = buildHistory(job);
            }
            final long generationBefore = generation(instance);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                fault.lock(false);
                try {
                    Assert.assertFalse("no root sits below the floor", job.truncateOrRetireTimelineOnO3ForTest(instance, ts(timestamp(5))));
                } finally {
                    fault.unlock();
                }
                Assert.assertEquals("the retire meets the lock once", 1, fault.getRefusedTimelineUnlinks());
                assertTimelineRetirePending(instance, generationBefore, Numbers.LONG_NULL);
                try (Path dir = checkpointsDir(instance)) {
                    Assert.assertEquals(Numbers.LONG_NULL, LiveViewCheckpointRepairMarker.readLvSeqTxn(configuration, dir));
                }
                appendAndRefresh(job, SEALS * 10 + 10, SEALS + 1);
                driveRefreshToQuiescence(job);
                Assert.assertFalse("the seal's retry of the retire removed the file", instance.isCheckpointTimelineRetirePending());
                Assert.assertEquals("and the seal opens a fresh history", 1, generation(instance));
                assertCheckpointLadder(instance, false, timestamp(SEALS * 10 + 10) + " -> " + (SEALS + 1) + "\n");
                assertNoRepairMarker(instance);
            }
            restartAndRefresh();
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

    @Test
    public void testASecondRepairInsideTheOwedWindowResolvesItsOwnMarker() throws Exception {
        // A second late row before the owed root lands repairs over the live marker: its own
        // marker replaces it, over the generation the first splice published, and its own closing
        // seal lands the root both repairs owed, which resolves it. The marker it writes over the
        // live one records no seqTxn, so it reads as live until that seal lands.
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
                final LiveViewReplacementCommitWitness witness = new LiveViewReplacementCommitWitness(
                        engine,
                        instance,
                        generationBefore + 1,
                        LiveViewReplacementCommitWitness.FAIL_NONE
                );
                commitDepartureUnder(witness, job, "('2026-01-01T00:01:45.000000Z', 'a', 1.0)");
                assertMarkersRecorded(witness, Numbers.LONG_NULL);
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
        // every row over the day the base lost. Written over a live marker, it records no seqTxn.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                final long generationBefore = generation(instance);
                failTheOwedResumeSeal(job, fault, LiveViewCheckpointDiskFullFault.OP_ALLOCATE, "('2026-01-01T00:01:30.000000Z', 'a', 1.0)");
                final LiveViewReplacementCommitWitness witness = new LiveViewReplacementCommitWitness(
                        engine,
                        instance,
                        generationBefore + 1,
                        LiveViewReplacementCommitWitness.FAIL_NONE
                );
                CairoEngine.setRoleSwitchMintObserver(witness::onMint);
                try {
                    failTheOwedResumeSeal(job, fault, LiveViewCheckpointDiskFullFault.OP_ALLOCATE, "('2026-01-01T00:01:45.000000Z', 'a', 1.0)");
                } finally {
                    CairoEngine.setRoleSwitchMintObserver(null);
                }
                assertMarkersRecorded(witness, Numbers.LONG_NULL);
                assertRepairMarkerLive(instance, generationBefore + 1);
                assertNoRefreshFaults("lv");
            }
            assertRestartsBlockedByTheRepairMarker(OWED_ROOT_SECOND_LATE_ROW_ROWS, OWED_ROOT_HISTORY_FLOOR);
        });
    }

    @Test
    public void testAThirdRepairWhoseCommitFailsOverTheSecondsLiveMarkerKeepsEveryRow() throws Exception {
        // Both late rows' closing seals fail, so the second's marker stays live under its own
        // splice. A third late row's resume writes its marker over that one, naming the second
        // splice's generation, and its replacement commit fails. The marker records no seqTxn, so
        // the in-place recovery declines the timeline, and the rebuild from the applied base is
        // refused over the day the base lost: the view stops with every row it holds, the third
        // late row unconsumed, and so does each restart.
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
                    failTheOwedResumeSeal(job, fault, LiveViewCheckpointDiskFullFault.OP_ALLOCATE, "('2026-01-01T00:01:45.000000Z', 'a', 1.0)");
                    assertRepairMarkerLive(instance, generationBefore + 1);
                    final long runtimeRestoresBefore = instance.getCheckpointRuntimeRestores();
                    final LiveViewReplacementCommitWitness witness = new LiveViewReplacementCommitWitness(
                            engine,
                            instance,
                            generationBefore + 2,
                            LiveViewReplacementCommitWitness.FAIL_CRITICAL
                    );
                    commitDepartureUnder(witness, job, "('2026-01-01T00:01:50.000000Z', 'a', 1.0)");
                    assertMarkersRecorded(witness, Numbers.LONG_NULL);
                    Assert.assertEquals(1, instance.getRefreshFaultCount());
                    Assert.assertEquals("nothing restores behind the live marker", runtimeRestoresBefore, instance.getCheckpointRuntimeRestores());
                    assertMidDrainRebuildBlocked(instance, OWED_ROOT_HISTORY_FLOOR);
                    Assert.assertEquals("the third splice never published", generationBefore + 2, generation(instance));
                    assertMarkerNames(generationBefore + 2);
                }
                capture.drain();
                capture.assertLogged(runtimeRestoreDeclinedOverTheMarker(MID_DRAIN_CAUSE));
                assertViewRows(OWED_ROOT_SECOND_LATE_ROW_ROWS, DEPARTURE_ROWS_QUERY);
                assertRestartsBlockedByTheRepairMarker(OWED_ROOT_SECOND_LATE_ROW_ROWS, OWED_ROOT_HISTORY_FLOOR);
                capture.drain();
                capture.assertNotLogged(GUARD_STAND_DOWN);
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testASecondRepairWhoseCommitFailsInsideTheOwedWindowKeepsTheRowsOfTheDayTheBaseLost() throws Exception {
        // The first late row's closing seal fails, so its splice's marker stays live over a
        // generation that owes the view's frontier a root. A second late row's resume writes its
        // own marker over that one, naming the same generation, and its replacement commit fails.
        // The marker keeps the verdict of the one it replaced: it records no seqTxn, so the
        // in-place recovery declines the timeline instead of restoring a generation that holds no
        // root at the frontier, and the rebuild from the applied base is refused over the day the
        // base lost. The view stops with every row it holds, and so does each restart.
        assertASecondRepairWhoseCommitFailsInsideTheOwedWindowKeepsEveryRow(false);
    }

    @Test
    public void testASecondRepairWhoseCommitFailsInsideTheOwedWindowKeepsTheRowsOfTheDayTheBaseLostOverANanosecondBase() throws Exception {
        assertASecondRepairWhoseCommitFailsInsideTheOwedWindowKeepsEveryRow(true);
    }

    @Test
    public void testASecondRepairWhoseMarkerRewriteFailsAfterItsUnlinkInsideTheOwedWindowBlocksTheRestart() throws Exception {
        // The first late row's closing seal fails, so its splice's marker stays live. The second
        // late row's repair writes its own marker over it, recording no seqTxn, and the rewrite
        // fails the way Windows fails it: the live record unlinked, the retried rename refused. The
        // write keeps the staged record, the only one left, and the repair retires the timeline
        // ahead of its commit, with _timeline and the segment directories locked: the splice's
        // generation stays on disk with the staged record beside it, which reads as live there. The
        // closing seal retries the retire, meets the lock and is refused, so nothing lands on the
        // generation the record names: each restart reads it as live and rebuilds, which the
        // restatement guard refuses over the day the base lost. The view stops with every row.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                    final long generationBefore = generation(instance);
                    failTheOwedResumeSealAtItsPublication(job, "('2026-01-01T00:01:30.000000Z', 'a', 1.0)");
                    assertRepairMarkerLive(instance, generationBefore);
                    final long resetsBefore = instance.getCheckpointTimelineResets();
                    final long sealFailuresBefore = instance.getCheckpointSealFailures();
                    fault.failTheNextMarkerRewrite();
                    fault.lock(false);
                    try {
                        commitDeparture(job, "('2026-01-01T00:01:45.000000Z', 'a', 1.0)");
                    } finally {
                        fault.unlock();
                    }
                    Assert.assertEquals("the second repair's marker rewrite fails", 1, fault.getFailedMarkerRewrites());
                    Assert.assertEquals("the repair retires the timeline ahead of its commit", resetsBefore + 1, instance.getCheckpointTimelineResets());
                    Assert.assertEquals("the retire, then the closing seal's retry of it", 2, fault.getRefusedTimelineUnlinks());
                    Assert.assertEquals("which refuses the seal", sealFailuresBefore + 1, instance.getCheckpointSealFailures());
                    Assert.assertTrue("the view refuses the generation the retire kept", instance.isCheckpointTimelineRetirePending());
                    assertTimelineOnDisk(instance, true);
                    try (Path dir = checkpointsDir(instance); Path path = new Path()) {
                        final FilesFacade ff = configuration.getFilesFacade();
                        LiveViewCheckpointLayout.repairingMarkerPath(path, dir);
                        Assert.assertFalse("the rewrite unlinked the live record", ff.exists(path.$()));
                        path.put(LiveViewCheckpointLayout.TMP_SUFFIX);
                        Assert.assertTrue("and kept the staged one", ff.exists(path.$()));
                        Assert.assertEquals(
                                "a marker written over a live one records no seqTxn",
                                Numbers.LONG_NULL,
                                LiveViewCheckpointRepairMarker.readLvSeqTxn(configuration, dir)
                        );
                    }
                    assertMarkerNames(generationBefore + 1);
                    Assert.assertEquals("nothing lands on the generation the retire kept", generationBefore + 1, generation(instance));
                    Assert.assertEquals("no head survives the retire", Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                    assertViewRows(OWED_ROOT_SECOND_LATE_ROW_ROWS, DEPARTURE_ROWS_QUERY);
                }
                assertRestartsBlockedByTheRepairMarker(OWED_ROOT_SECOND_LATE_ROW_ROWS, OWED_ROOT_HISTORY_FLOOR);
                capture.drain();
                capture.assertNotLogged("does not match durable materialization");
                capture.assertNotLogged(GUARD_STAND_DOWN);
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testASecondRepairWhoseCommitFailsInsideTheOwedWindowRebuildsInPlaceOverACompleteBase() throws Exception {
        // The second repair's marker records no seqTxn over the live one, so the in-place recovery
        // declines the timeline and rebuilds from the applied base. The base lost nothing, so the
        // restatement guard lets the rebuild through: the view keeps refreshing, holds both late
        // rows, and opens a fresh history at its frontier, which each restart restores from.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job, "2026-01-01", false);
                    final long generationBefore = generation(instance);
                    failTheOwedResumeSeal(job, fault, LiveViewCheckpointDiskFullFault.OP_ALLOCATE, "('2026-01-01T00:01:30.000000Z', 'a', 1.0)");
                    assertRepairMarkerLive(instance, generationBefore);
                    final long runtimeRestoresBefore = instance.getCheckpointRuntimeRestores();
                    final LiveViewReplacementCommitWitness witness = new LiveViewReplacementCommitWitness(
                            engine,
                            instance,
                            generationBefore + 1,
                            LiveViewReplacementCommitWitness.FAIL_CRITICAL
                    );
                    commitDepartureUnder(witness, job, "('2026-01-01T00:01:45.000000Z', 'a', 1.0)");
                    assertMarkersRecorded(witness, Numbers.LONG_NULL);
                    Assert.assertEquals(1, instance.getRefreshFaultCount());
                    Assert.assertEquals("nothing restores behind the live marker", runtimeRestoresBefore, instance.getCheckpointRuntimeRestores());
                    Assert.assertFalse("the rebuild over a complete base goes through", instance.isCheckpointRecoveryBlocked());
                    Assert.assertFalse(instance.isInvalid());
                    Assert.assertEquals("the rebuild opens a fresh history", 1, generation(instance));
                    Assert.assertEquals("at the view's frontier", ts("2026-01-01T00:04:00.000000Z"), instance.getHeadCheckpointMaxTs());
                    assertNoRepairMarker(instance);
                    assertViewRows(OWED_ROOT_SECOND_LATE_ROW_ROWS, DEPARTURE_ROWS_QUERY);
                }
                capture.drain();
                capture.assertLogged(runtimeRestoreDeclinedOverTheMarker(MID_DRAIN_CAUSE));
                for (int restart = 0; restart < 2; restart++) {
                    restartAndRefresh();
                    assertRestoredFromTimeline("lv");
                    assertViewRows(OWED_ROOT_SECOND_LATE_ROW_ROWS, DEPARTURE_ROWS_QUERY);
                }
                capture.drain();
                capture.assertNotLogged(GUARD_STAND_DOWN);
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testASecondRepairThatStopsBeforeItsCommitInsideTheOwedWindowBlocksTheRestart() throws Exception {
        // The process stops between the second repair's marker and its replacement commit, as a
        // crash there stops it: nothing moved past the marker. The marker records no seqTxn over
        // the live one it replaced, so each restart rebuilds from the applied base instead of
        // restoring a generation that holds no root at the frontier, and the restatement guard
        // refuses the rebuild over the day the base lost: the view stops with every row it held.
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
                    assertRepairMarkerLive(instance, generationBefore);
                    final long lvSeqTxnBefore = engine.getTableSequencerAPI().lastTxn(instance.getLiveViewToken());
                    final LiveViewReplacementCommitWitness witness = new LiveViewReplacementCommitWitness(
                            engine,
                            instance,
                            generationBefore + 1,
                            LiveViewReplacementCommitWitness.FAIL_CANCELLATION
                    );
                    commitAndStopAtTheReplacementCommit(witness, job, "('2026-01-01T00:01:45.000000Z', 'a', 1.0)");
                    assertMarkersRecorded(witness, Numbers.LONG_NULL);
                    Assert.assertEquals("the marker lands ahead of the replacement commit", lvSeqTxnBefore, witness.getViewLvSeqTxn(0));
                    Assert.assertEquals("the replacement never commits", lvSeqTxnBefore, engine.getTableSequencerAPI().lastTxn(instance.getLiveViewToken()));
                    Assert.assertEquals("the splice never publishes", generationBefore + 1, generation(instance));
                    assertMarkerNames(generationBefore + 1);
                    Assert.assertEquals("no recovery runs", 0, instance.getCheckpointRuntimeRestores());
                    Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
                }
                assertRestartsBlockedByTheRepairMarker(OWED_ROOT_ROWS, OWED_ROOT_HISTORY_FLOOR);
                capture.drain();
                capture.assertNotLogged(GUARD_STAND_DOWN);
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testASecondRepairThatTruncatesInsideTheOwedWindowAndStopsAfterItsMarkerBlocksTheRestart() throws Exception {
        // With the chain declined, the second repair truncates the timeline ahead of its
        // replacement commit, and its marker, written over the live one, records no seqTxn. A
        // crash right after that marker lands leaves the truncate unpublished and the replacement
        // uncommitted: each restart from that image rebuilds from the applied base, which the
        // restatement guard refuses over the day the base lost, and the view stops with every row.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final RepairMarkerWitness markers = new RepairMarkerWitness();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(markers, () -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                    final long generationBefore = generation(instance);
                    failTheOwedResumeSealAtItsPublication(job, "('2026-01-01T00:01:30.000000Z', 'a', 1.0)");
                    assertRepairMarkerLive(instance, generationBefore);
                    setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 0);
                    stopAfterTheTruncateMarker(markers, job, instance, generationBefore + 1, "('2026-01-01T00:01:45.000000Z', 'a', 1.0)");
                    Assert.assertEquals("the truncate writes the turn's one marker", 1, markers.size());
                    Assert.assertEquals("over the live one, it names the splice's generation", generationBefore + 1, markers.getGeneration(0));
                    Assert.assertEquals("and records no seqTxn", Numbers.LONG_NULL, markers.getLvSeqTxn(0));
                    assertTheCrashImage(generationBefore + 1, generationBefore + 1, Numbers.LONG_NULL);
                    capture.drain();
                    capture.assertLogged("live view O3 resume declined the checkpoint chain, truncating instead [view=lv");
                }
                putTheCrashImageBack();
                assertRestartsBlockedByTheRepairMarker(OWED_ROOT_ROWS, OWED_ROOT_HISTORY_FLOOR);
                capture.drain();
                capture.assertNotLogged(GUARD_STAND_DOWN);
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testARepairAfterTheOwedRootLandedWhoseCommitFailsRestoresInPlace() throws Exception {
        // A row past the cadence lands the root the first late row's splice owed, so the marker
        // left on disk names a generation two below the superblock's: a stale leftover. A later
        // late row's repair reads it as stale, clears it and records the view's seqTxn in its own
        // marker. Its replacement commit fails, and the in-place recovery reads that marker as
        // stale too, restores the runtime from the timeline and repairs the late row on the retry:
        // the view keeps every row over the day the base lost and keeps refreshing.
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
                    setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                    commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'a', 1.0)");
                    Assert.assertEquals("the owed root lands", ts("2026-01-01T00:05:00.000000Z"), instance.getHeadCheckpointMaxTs());
                    assertRepairMarkerStale(instance, generationBefore);
                    Assert.assertEquals("the splice and the owed root", generationBefore + 2, generation(instance));
                    final long runtimeRestoresBefore = instance.getCheckpointRuntimeRestores();
                    final LiveViewReplacementCommitWitness witness = new LiveViewReplacementCommitWitness(
                            engine,
                            instance,
                            generationBefore + 2,
                            LiveViewReplacementCommitWitness.FAIL_CRITICAL
                    );
                    commitDepartureUnder(witness, job, "('2026-01-01T00:01:45.000000Z', 'a', 1.0)");
                    Assert.assertEquals("the faulted commit and its retry", 2, witness.size());
                    Assert.assertEquals("the marker records the seqTxn its commit follows", witness.getViewLvSeqTxn(0), witness.getRecordedLvSeqTxn(0));
                    Assert.assertNotEquals(Numbers.LONG_NULL, witness.getRecordedLvSeqTxn(0));
                    Assert.assertEquals(1, instance.getRefreshFaultCount());
                    Assert.assertEquals("the recovery restores in place", runtimeRestoresBefore + 1, instance.getCheckpointRuntimeRestores());
                    Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
                    Assert.assertFalse(instance.isInvalid());
                    assertNoRepairMarker(instance);
                    assertViewRows(OWED_ROOT_SECOND_LATE_ROW_ROWS + OWED_ROOT_ROW_0005, DEPARTURE_ROWS_QUERY);
                }
                capture.drain();
                capture.assertLogged("live view restored its runtime from the checkpoint timeline [view=lv, cause=mid-drain refresh failure");
                for (int restart = 0; restart < 2; restart++) {
                    restartAndRefresh();
                    assertRestoredFromTimeline("lv");
                    assertViewRows(OWED_ROOT_SECOND_LATE_ROW_ROWS + OWED_ROOT_ROW_0005, DEPARTURE_ROWS_QUERY);
                }
                capture.drain();
                capture.assertNotLogged(GUARD_STAND_DOWN);
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testATruncatingRepairAfterTheOwedRootLandedThatStopsAfterItsMarkerRestores() throws Exception {
        // The owed root has landed, so the marker left on disk is a stale leftover. With the chain
        // declined, a later late row's repair truncates ahead of its replacement commit: it reads
        // the leftover as stale, clears it, and records the view's seqTxn in the marker it writes.
        // A crash right after that marker lands leaves the truncate unpublished and the
        // replacement uncommitted, so the restart from that image reads the marker as stale,
        // restores from the timeline and repairs the late row: every row stays.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final RepairMarkerWitness markers = new RepairMarkerWitness();
        assertMemoryLeak(markers, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                final long generationBefore = generation(instance);
                failTheOwedResumeSealAtItsPublication(job, "('2026-01-01T00:01:30.000000Z', 'a', 1.0)");
                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'a', 1.0)");
                assertRepairMarkerStale(instance, generationBefore);
                final long owedRootGeneration = generation(instance);
                Assert.assertEquals("the splice and the owed root", generationBefore + 2, owedRootGeneration);
                setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 0);
                final long lvSeqTxnBefore = engine.getTableSequencerAPI().lastTxn(instance.getLiveViewToken());
                stopAfterTheTruncateMarker(markers, job, instance, owedRootGeneration, "('2026-01-01T00:01:45.000000Z', 'a', 1.0)");
                Assert.assertEquals("the truncate writes the turn's one marker", 1, markers.size());
                Assert.assertEquals("it names the owed root's generation", owedRootGeneration, markers.getGeneration(0));
                Assert.assertEquals("and records the seqTxn its commit follows", lvSeqTxnBefore, markers.getLvSeqTxn(0));
                assertTheCrashImage(owedRootGeneration, owedRootGeneration, lvSeqTxnBefore);
            }
            putTheCrashImageBack();
            for (int restart = 0; restart < 2; restart++) {
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(OWED_ROOT_SECOND_LATE_ROW_ROWS + OWED_ROOT_ROW_0005, DEPARTURE_ROWS_QUERY);
            }
        });
    }

    @Test
    public void testAHeadMissSecondRepairThatStopsBeforeItsCommitInsideTheOwedWindowBlocksTheRestart() throws Exception {
        // The head-miss rebuild's closing seal fails, so its splice's marker stays live. A second
        // late row's head-miss rebuild writes its own marker over it, naming the splice's
        // generation, and the process stops ahead of its replacement commit. That marker records no
        // seqTxn, so each restart rebuilds from the applied base, which the restatement guard
        // refuses over the day the base lost: the view stops with every row it held.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createHeadMissDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    final LiveViewInstance instance = commitFiveRowsAboveTheHeadMissRoot(job);
                    final long generationBefore = generation(instance);
                    failTheOwedHeadMissSeal(job, fault);
                    assertRepairMarkerLive(instance, generationBefore);
                    stopAtTheSecondHeadMissCommit(job, instance, generationBefore);
                }
                assertRestartsBlockedByTheRepairMarker(headMissRowsWithTheLateRow(55), HEAD_MISS_OWED_ROOT_HISTORY_FLOOR);
                capture.drain();
                capture.assertNotLogged(GUARD_STAND_DOWN);
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testAHeadMissSecondRepairStoppedBeforeItsCommitThenABaseSchemaChangeKeepsEveryRow() throws Exception {
        // As above, but the process lives on and the base's schema changes: the view recovers in
        // place, which declines the timeline under the marker that records no seqTxn, and the
        // rebuild from the applied base is refused over the day the base lost. The view stops with
        // every row it holds, and so does each restart.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createHeadMissDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    final LiveViewInstance instance = commitFiveRowsAboveTheHeadMissRoot(job);
                    final long generationBefore = generation(instance);
                    failTheOwedHeadMissSeal(job, fault);
                    stopAtTheSecondHeadMissCommit(job, instance, generationBefore);
                    final long runtimeRestoresBefore = instance.getCheckpointRuntimeRestores();
                    execute("ALTER TABLE base ADD COLUMN y INT");
                    drainWalQueue();
                    driveRefreshToQuiescence(job);
                    final LiveViewInstance recovered = viewInstance();
                    Assert.assertEquals("nothing restores behind the live marker", runtimeRestoresBefore, recovered.getCheckpointRuntimeRestores());
                    Assert.assertTrue("the refused rebuild stops the view", recovered.isCheckpointRecoveryBlocked());
                    Assert.assertFalse(recovered.isInvalid());
                    TestUtils.assertContains(
                            recovered.getCheckpointRecoveryReason(),
                            "would drop rows it retains [cause=base table metadata change]: " + HEAD_MISS_OWED_ROOT_HISTORY_FLOOR
                    );
                    Assert.assertEquals("the second splice never published", generationBefore + 1, generation(recovered));
                    assertMarkerNames(generationBefore + 1);
                }
                capture.drain();
                capture.assertLogged(runtimeRestoreDeclinedOverTheMarker("base table metadata change"));
                assertViewRows(headMissRowsWithTheLateRow(55), DEPARTURE_ROWS_QUERY);
                assertRestartsBlockedByTheRepairMarker(headMissRowsWithTheLateRow(55), HEAD_MISS_OWED_ROOT_HISTORY_FLOOR);
                capture.drain();
                capture.assertNotLogged(GUARD_STAND_DOWN);
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testAHeadMissSecondRepairWhoseCommitFailsRetriesUnderItsOwnLiveMarker() throws Exception {
        // The second late row's head-miss rebuild converges below the frontier, so it keeps the
        // primary runtime when its replacement commit fails and no recovery runs: the retry repairs
        // the row on the next turn. Each attempt writes its marker over a live one, the kept marker
        // and then its own, so neither records a seqTxn. The retry's splice and closing seal land,
        // which clears the marker, and each restart restores with both late rows.
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
                final long headMissRepairsBefore = countHeadMissRepairs(job);
                final long runtimeRestoresBefore = instance.getCheckpointRuntimeRestores();
                final LiveViewReplacementCommitWitness witness = new LiveViewReplacementCommitWitness(
                        engine,
                        instance,
                        generationBefore + 1,
                        LiveViewReplacementCommitWitness.FAIL_CRITICAL
                );
                commitDepartureUnder(witness, job, "('2026-01-01T00:49:45.000000Z', 'a', 1.0)");
                assertMarkersRecorded(witness, Numbers.LONG_NULL, Numbers.LONG_NULL);
                Assert.assertEquals("the retry repairs the late row", headMissRepairsBefore + 1, countHeadMissRepairs(job));
                Assert.assertEquals(1, instance.getRefreshFaultCount());
                Assert.assertEquals("the head miss keeps the primary runtime", runtimeRestoresBefore, instance.getCheckpointRuntimeRestores());
                Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
                Assert.assertEquals("the retry's splice and its closing seal", generationBefore + 3, generation(instance));
                Assert.assertEquals(ts("2026-01-01T00:55:00.000000Z"), instance.getHeadCheckpointMaxTs());
                assertNoRepairMarker(instance);
            }
            for (int restart = 0; restart < 2; restart++) {
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(HEAD_MISS_ROWS_WITH_BOTH_LATE_ROWS, DEPARTURE_ROWS_QUERY);
            }
        });
    }

    @Test
    public void testAHeadMissRepairWhoseCommitFailsRewritesItsOwnStaleMarkerWithItsSeqTxn() throws Exception {
        // No marker stands before the late row, so the head-miss rebuild's marker records the
        // view's seqTxn. Its replacement commit fails and the rebuild, which converged below the
        // frontier, keeps the primary runtime, so no recovery reads that marker. The retry finds
        // it under the generation it names, with the view's WAL still ending at the seqTxn it
        // recorded: a stale leftover, which it clears, and its own marker records that seqTxn
        // again. The retry's splice and closing seal land and clear it, and each restart restores.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final RepairMarkerWitness markers = new RepairMarkerWitness();
        assertMemoryLeak(markers, () -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitFiveRowsAboveTheHeadMissRoot(job);
                final long generationBefore = generation(instance);
                final long headMissRepairsBefore = countHeadMissRepairs(job);
                final long lvSeqTxnBefore = engine.getTableSequencerAPI().lastTxn(instance.getLiveViewToken());
                final LiveViewReplacementCommitWitness witness = new LiveViewReplacementCommitWitness(
                        engine,
                        instance,
                        generationBefore,
                        LiveViewReplacementCommitWitness.FAIL_CRITICAL
                );
                markers.clear();
                commitDepartureUnder(witness, job, "('2026-01-01T00:49:30.000000Z', 'a', 1.0)");
                Assert.assertEquals("the retry repairs the late row", headMissRepairsBefore + 1, countHeadMissRepairs(job));
                Assert.assertEquals("each attempt writes its marker", 2, markers.size());
                for (int attempt = 0; attempt < 2; attempt++) {
                    Assert.assertEquals(generationBefore, markers.getGeneration(attempt));
                    Assert.assertEquals("each records the seqTxn its commit follows", lvSeqTxnBefore, markers.getLvSeqTxn(attempt));
                }
                Assert.assertEquals(1, instance.getRefreshFaultCount());
                Assert.assertEquals("the head miss keeps the primary runtime", 0, instance.getCheckpointRuntimeRestores());
                Assert.assertEquals("the retry's splice and its closing seal", generationBefore + 2, generation(instance));
                assertNoRepairMarker(instance);
            }
            for (int restart = 0; restart < 2; restart++) {
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(headMissRowsWithTheLateRow(55), DEPARTURE_ROWS_QUERY);
            }
        });
    }

    @Test
    public void testAnEndOfTableHeadMissSecondRepairWhoseCommitFailsInsideTheOwedWindowKeepsEveryRow() throws Exception {
        // The head-miss rebuild's closing seal fails, so its splice's marker stays live. A second
        // late row whose frame reaches past the view's frontier sends the head-miss rebuild to the
        // end of the table, which does not keep the primary runtime: its replacement commit fails,
        // and the in-place recovery reads the marker the rebuild wrote over the live one. It
        // records no seqTxn, so the recovery declines the timeline, and the rebuild from the
        // applied base is refused over the day the base lost. The view stops with every row it
        // holds, and so does each restart.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createHeadMissDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    final LiveViewInstance instance = commitFiveRowsAboveTheHeadMissRoot(job);
                    final long generationBefore = generation(instance);
                    failTheOwedHeadMissSeal(job, fault);
                    assertRepairMarkerLive(instance, generationBefore);
                    final long headMissRepairsBefore = countHeadMissRepairs(job);
                    final long runtimeRestoresBefore = instance.getCheckpointRuntimeRestores();
                    final LiveViewReplacementCommitWitness witness = new LiveViewReplacementCommitWitness(
                            engine,
                            instance,
                            generationBefore + 1,
                            LiveViewReplacementCommitWitness.FAIL_CRITICAL
                    );
                    commitDepartureUnder(witness, job, "('2026-01-01T00:54:30.000000Z', 'a', 1.0)");
                    assertMarkersRecorded(witness, Numbers.LONG_NULL);
                    Assert.assertEquals("the faulted repair publishes nothing", headMissRepairsBefore, countHeadMissRepairs(job));
                    Assert.assertEquals(1, instance.getRefreshFaultCount());
                    Assert.assertEquals("nothing restores behind the live marker", runtimeRestoresBefore, instance.getCheckpointRuntimeRestores());
                    assertMidDrainRebuildBlocked(instance, HEAD_MISS_OWED_ROOT_HISTORY_FLOOR);
                    Assert.assertEquals("the second splice never published", generationBefore + 1, generation(instance));
                    assertMarkerNames(generationBefore + 1);
                }
                capture.drain();
                // The late row's frame reaches past the frontier, so its rebuild has no upper bound.
                capture.assertLoggedRE("live view O3 replay \\[view=lv, lateRowTs=" + ts("2026-01-01T00:54:30.000000Z")
                        + ", .*, highTsExclusive=null, resumeFromAnchor=false, ");
                capture.assertLogged(runtimeRestoreDeclinedOverTheMarker(MID_DRAIN_CAUSE));
                assertViewRows(headMissRowsWithTheLateRow(55), DEPARTURE_ROWS_QUERY);
                assertRestartsBlockedByTheRepairMarker(headMissRowsWithTheLateRow(55), HEAD_MISS_OWED_ROOT_HISTORY_FLOOR);
                capture.drain();
                capture.assertNotLogged(GUARD_STAND_DOWN);
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testARepairOverAMarkerWhoseClearFailedRebuildsWhenItsCommitFails() throws Exception {
        // A departure's truncate leaves its newest root at the frontier, and the marker clear after
        // the seal that re-stamps the head fails: the leftover names the splice's generation, one
        // below the superblock's, and reads as live, as it does to a restart. A late row's repair
        // then writes its marker over it, which records no seqTxn, and its replacement commit
        // fails. The in-place recovery declines the timeline as a restart would, and the rebuild
        // from the applied base is refused over the day the base lost: the view stops with every
        // row it holds, the late row unconsumed, and so does each restart.
        final LiveViewMarkerClearFault fault = new LiveViewMarkerClearFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    commitTheDayTheBaseLoses(job);
                    commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'a', 1.5)");
                    commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', 2.5)");
                    final LiveViewInstance instance = viewInstance();
                    final long generationBefore = generation(instance);
                    fault.arm();
                    commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', -2.5)");
                    fault.disarm();
                    Assert.assertEquals(1, fault.getRefusedClears());
                    Assert.assertEquals("the splice and the truncate", generationBefore + 2, generation(instance));
                    assertMarkerNames(generationBefore + 1);
                    final LiveViewReplacementCommitWitness witness = new LiveViewReplacementCommitWitness(
                            engine,
                            instance,
                            generationBefore + 2,
                            LiveViewReplacementCommitWitness.FAIL_CRITICAL
                    );
                    commitDepartureUnder(witness, job, "('2026-01-01T00:00:30.000000Z', 'a', 1.0)");
                    assertMarkersRecorded(witness, Numbers.LONG_NULL);
                    Assert.assertEquals(1, instance.getRefreshFaultCount());
                    Assert.assertEquals("nothing restores behind the live marker", 0, instance.getCheckpointRuntimeRestores());
                    assertMidDrainRebuildBlocked(instance, LATE_ROW_HISTORY_FLOOR);
                    assertMarkerNames(generationBefore + 2);
                }
                capture.drain();
                capture.assertLogged(runtimeRestoreDeclinedOverTheMarker(MID_DRAIN_CAUSE));
                assertViewRows(DEPARTURE_ROWS_OVER_THE_LOST_DAY, DEPARTURE_ROWS_QUERY);
                assertRestartsBlockedByTheRepairMarker(DEPARTURE_ROWS_OVER_THE_LOST_DAY, LATE_ROW_HISTORY_FLOOR);
                capture.drain();
                capture.assertNotLogged(GUARD_STAND_DOWN);
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testASecondRepairThatCannotReadTheMarkerItReplacesRetiresTheTimelineAheadOfItsCommit() throws Exception {
        // The second repair cannot read the live marker it would write over, so it cannot tell
        // whether to record its seqTxn: it treats the marker as unwritable, drops its capture and
        // retires the timeline, the live marker with it, ahead of its replacement commit. The
        // commit lands, the closing seal opens a fresh history at the frontier, and each restart
        // restores every row from it. The resume replaces the output from just above its anchor,
        // the root at 00:01.
        assertASecondRepairThatCannotReadTheMarkerItReplacesRetiresTheTimeline(
                false,
                "could not write the live view checkpoint repair marker [view=lv, floorTs=2026-01-01T00:01:00.000001Z"
        );
    }

    @Test
    public void testASecondTruncatingRepairThatCannotReadTheMarkerItReplacesRetiresTheTimeline() throws Exception {
        // As above, with the chain declined: the truncate cannot read the live marker, and retires
        // the timeline instead of truncating it.
        assertASecondRepairThatCannotReadTheMarkerItReplacesRetiresTheTimeline(
                true,
                "could not preserve live view checkpoint timeline prefix, retiring [view=lv"
        );
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
    public void testResumeOwedRootAboveTheNewestRowLandsOnARowlessCycle() throws Exception {
        // An upsert moves the unsealed row 00:04 out of the filter: the resume from the parked
        // head's own root emits 00:03, the new frontier, which the failed closing seal owed a root.
        // The newest timestamp the view has seen stays at 00:04, above the newest row the table
        // holds. A cycle without rows inside the cadence seals nothing; the first one past it seals
        // the table's newest row, 00:03, rather than declining a root at 00:04: the owed root lands,
        // reads the marker as stale and moves the WAL floor up, and each restart restores over the
        // day the base lost.
        assertResumeOwedRootAboveTheNewestRowLandsOnARowlessCycle(false);
    }

    @Test
    public void testResumeOwedRootAboveTheNewestRowLandsOnARowlessCycleOverANanosecondBase() throws Exception {
        // The same departure over a base whose designated timestamp has nanosecond precision: the
        // cycle weighs the newest timestamp the view has seen against the table's newest row, and
        // stamps the root, in the view's own unit.
        assertResumeOwedRootAboveTheNewestRowLandsOnARowlessCycle(true);
    }

    @Test
    public void testResumeOwedRootAboveTheNewestRowLandsAtTheRowCadence() throws Exception {
        // Under a cadence of three rows, the departure's failed closing seal leaves the row its
        // resume owed on the row cadence's count beside the two rows no root holds, so the first
        // cycle without rows after it is due inside the duration cadence, and lands the owed root at
        // the table's newest row, 00:03.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 3);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                final long generationBefore = generation(instance);
                failTheOwedResumeSeal(job, fault, LiveViewCheckpointDiskFullFault.OP_ALLOCATE, "('2026-01-01T00:04:00.000000Z', 'a', -1.0)");
                assertRepairMarkerLive(instance, generationBefore);
                // seqTxn 7, with the clock where the departure left it.
                commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'b', -1.0)");
                Assert.assertEquals("the cycle seals the table's newest row", ts("2026-01-01T00:03:00.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals("at its commit", 7, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals("the splice and the owed root", generationBefore + 2, generation(instance));
                assertRepairMarkerStale(instance, generationBefore);
                assertCheckpointLadder(instance, false, OWED_ROOT_DEPARTED_FRONTIER_LADDER);
                assertNoRefreshFaults("lv");
            }
            for (int restart = 0; restart < 2; restart++) {
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(OWED_ROOT_DEPARTED_FRONTIER_ROWS, DEPARTURE_ROWS_QUERY);
            }
        });
    }

    @Test
    public void testResumeOwedRootAboveTheNewestRowLandsOnceAnHourCadenceElapses() throws Exception {
        // An hour's cadence seals no root at 00:01 or 00:02, so the departure's resume anchors on
        // the root at 2025-12-31 and owes its new frontier 00:03 a root, which its closing seal
        // fails to write. A cycle without rows ten minutes on is inside the cadence and seals
        // nothing; one past the hour seals the table's newest row, which lands the owed root.
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
                failTheOwedResumeSeal(job, fault, LiveViewCheckpointDiskFullFault.OP_ALLOCATE, "('2026-01-01T00:04:00.000000Z', 'a', -1.0)");
                Assert.assertEquals(ts("2025-12-31T12:00:00.000000Z"), instance.getHeadCheckpointMaxTs());
                assertRepairMarkerLive(instance, generationBefore);
                setCurrentMicros(currentMicros + 600_000_000L);
                commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'b', -1.0)");
                Assert.assertEquals("a cycle inside the hour seals nothing", ts("2025-12-31T12:00:00.000000Z"), instance.getHeadCheckpointMaxTs());
                assertRepairMarkerLive(instance, generationBefore);
                setCurrentMicros(currentMicros + 3_600_000_000L);
                commitDeparture(job, "('2026-01-01T00:06:00.000000Z', 'b', -1.0)");
                Assert.assertEquals("a cycle past it seals the table's newest row", ts("2026-01-01T00:03:00.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals("at its commit", 8, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals("the splice and the owed root", generationBefore + 2, generation(instance));
                assertRepairMarkerStale(instance, generationBefore);
                assertCheckpointLadder(instance, false, "2025-12-31T12:00:00.000000Z -> 1\n2026-01-01T00:03:00.000000Z -> 4\n");
                assertNoRefreshFaults("lv");
            }
            for (int restart = 0; restart < 2; restart++) {
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(OWED_ROOT_DEPARTED_FRONTIER_ROWS, DEPARTURE_ROWS_QUERY);
            }
        });
    }

    @Test
    public void testResumeOwedRootAboveTheNewestRowWhoseLandingFailsOnceLandsOnTheNextDueCycle() throws Exception {
        assertResumeOwedRootAboveTheNewestRowWhoseLandingFailsOnceLandsOnTheNextDueCycle(false);
    }

    @Test
    public void testResumeOwedRootAboveTheNewestRowWhoseLandingPublicationFailsOnceLandsOnTheNextDueCycle() throws Exception {
        assertResumeOwedRootAboveTheNewestRowWhoseLandingFailsOnceLandsOnTheNextDueCycle(true);
    }

    @Test
    public void testResumeOwedRootAboveTheNewestRowWhoseViewTableReadFailsOnceLandsOnTheNextDueCycle() throws Exception {
        // The first cycle without rows past the cadence cannot read the view's table, which the seal
        // of the table's newest row reads first: it logs the failed read, counts neither a failure
        // nor a decline and seals nothing, so the marker stays live. The cadence stays due, and the
        // next cycle reads the table and lands the owed root.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
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
        final LogCapture capture = new LogCapture();
        try {
            assertMemoryLeak(ff, () -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                    final long generationBefore = generation(instance);
                    // A cooldown that ends one microsecond on, so it holds the closing seal alone.
                    hooks(job).setSimulateResumeHeadSealStart(() -> instance.setSealCooldownUntilUs(currentMicros + 1));
                    commitDeparture(job, "('2026-01-01T00:04:00.000000Z', 'a', -1.0)");
                    hooks(job).setSimulateResumeHeadSealStart(null);
                    Assert.assertEquals("the cooldown holds the closing seal", ts("2026-01-01T00:02:00.000000Z"), instance.getHeadCheckpointMaxTs());
                    assertRepairMarkerLive(instance, generationBefore);
                    final long sealFailuresBefore = instance.getCheckpointSealFailures();
                    final long declinesBefore = job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT);
                    hooks(job).setSimulateNoRowHeadCheckpointStart(() -> {
                        // No pooled reader may serve the read, so it opens the table's files.
                        Assert.assertTrue(engine.lockReadersByTableToken(instance.getLiveViewToken()));
                        engine.unlockReaders(instance.getLiveViewToken());
                        failedOpenDir.set(instance.getLiveViewToken().getDirName());
                    });
                    setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                    capture.start();
                    commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'b', -1.0)");
                    hooks(job).setSimulateNoRowHeadCheckpointStart(null);
                    Assert.assertEquals("the seal's read of the view's table fails", 1, failedOpens.get());
                    Assert.assertEquals("and seals nothing", ts("2026-01-01T00:02:00.000000Z"), instance.getHeadCheckpointMaxTs());
                    Assert.assertEquals("a failed read is not a failed seal", sealFailuresBefore, instance.getCheckpointSealFailures());
                    Assert.assertEquals(
                            "nor a decline",
                            declinesBefore,
                            job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT)
                    );
                    assertRepairMarkerLive(instance, generationBefore);

                    commitDeparture(job, "('2026-01-01T00:06:00.000000Z', 'b', -1.0)");
                    capture.drain();
                    capture.stop();
                    Assert.assertEquals("the next cycle lands the owed root", ts("2026-01-01T00:03:00.000000Z"), instance.getHeadCheckpointMaxTs());
                    Assert.assertEquals("at its commit", 8, instance.getHeadCheckpointBaseSeqTxn());
                    Assert.assertEquals("the splice and the owed root", generationBefore + 2, generation(instance));
                    assertRepairMarkerStale(instance, generationBefore);
                    Assert.assertEquals(sealFailuresBefore, instance.getCheckpointSealFailures());
                    Assert.assertEquals(
                            declinesBefore,
                            job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT)
                    );
                    // The failed read logs once, and the read that lands logs nothing.
                    Assert.assertEquals(
                            1,
                            capture.countOccurrences("could not measure live view rows before a checkpoint seal [view=lv, error=")
                    );
                    assertNoRefreshFaults("lv");
                }
                for (int restart = 0; restart < 2; restart++) {
                    restartAndRefresh();
                    assertRestoredFromTimeline("lv");
                    assertViewRows(OWED_ROOT_DEPARTED_FRONTIER_ROWS, DEPARTURE_ROWS_QUERY);
                }
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testResumeOwedRootAboveTheNewestRowPublishesOnceACadence() throws Exception {
        // Three cycles without rows inside the cadence after the departure's failed closing seal
        // attempt no seal - the fault armed on the data segment stays unfired - and publish nothing.
        // Then three cadences, two such cycles each: the first cycle of the first cadence lands the
        // owed root, the first of each later one republishes it at its commit, and the second of
        // each publishes nothing, so the generation grows by one a cadence.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                failTheOwedResumeSeal(job, fault, LiveViewCheckpointDiskFullFault.OP_ALLOCATE, "('2026-01-01T00:04:00.000000Z', 'a', -1.0)");
                final long spliceGeneration = generation(instance);
                fault.arm(LiveViewCheckpointDiskFullFault.OP_ALLOCATE, LiveViewCheckpointLayout.DATA_DIR_NAME, 1);
                for (int minute = 5; minute <= 7; minute++) {
                    commitDeparture(job, "('" + String.format("2026-01-01T00:%02d:00.000000Z", minute) + "', 'b', -1.0)");
                }
                fault.disarm();
                Assert.assertNull("no cycle inside the cadence reaches a data segment", fault.getFailedFile());
                Assert.assertEquals("nor publishes", spliceGeneration, generation(instance));
                for (int cadence = 1; cadence <= 3; cadence++) {
                    setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                    commitDeparture(job, "('" + String.format("2026-01-01T00:%02d:00.000000Z", 6 + 2 * cadence) + "', 'b', -1.0)");
                    Assert.assertEquals("one publication a cadence", spliceGeneration + cadence, generation(instance));
                    Assert.assertEquals("at the cycle's commit", lastBaseSeqTxn(), instance.getHeadCheckpointBaseSeqTxn());
                    commitDeparture(job, "('" + String.format("2026-01-01T00:%02d:00.000000Z", 7 + 2 * cadence) + "', 'b', -1.0)");
                    Assert.assertEquals("a cycle inside the cadence publishes nothing", spliceGeneration + cadence, generation(instance));
                }
                Assert.assertEquals(ts("2026-01-01T00:03:00.000000Z"), instance.getHeadCheckpointMaxTs());
                assertCheckpointLadder(instance, false, OWED_ROOT_DEPARTED_FRONTIER_LADDER);
                Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT));
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testResumeOwedRootOneMicrosecondAboveTheEpochLandsOnARowlessCycle() throws Exception {
        // The smallest designated timestamps: a root at the epoch, then rows one and two
        // microseconds above it, which no root holds. An upsert moves the newer of them out of the
        // filter, and the closing seal of the resume from the epoch's root, which owes the new
        // frontier a root, fails. The first cycle without rows past the cadence lands that root one
        // microsecond above the epoch, the table's newest row, and each restart restores from it.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitDeparture(job, "('1970-01-01T00:00:00.000000Z', 'a', 0.5)");
                final LiveViewInstance instance = viewInstance();
                Assert.assertEquals("the first commit seals the epoch", 0, instance.getHeadCheckpointMaxTs());
                commitDeparture(job, "('1970-01-01T00:00:00.000001Z', 'a', 1.5), ('1970-01-01T00:00:00.000002Z', 'a', 2.5)");
                Assert.assertEquals("no root seals the rows above it inside the cadence", 0, instance.getHeadCheckpointMaxTs());
                final long generationBefore = generation(instance);
                failTheOwedResumeSeal(job, fault, LiveViewCheckpointDiskFullFault.OP_ALLOCATE, "('1970-01-01T00:00:00.000002Z', 'a', -1.0)");
                Assert.assertEquals(2, instance.getLatestSeenTs());
                assertRepairMarkerLive(instance, generationBefore);
                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                commitDeparture(job, "('1970-01-01T00:00:00.000003Z', 'b', -1.0)");
                Assert.assertEquals("the cycle seals the table's newest row", 1, instance.getHeadCheckpointMaxTs());
                Assert.assertEquals("at its commit", 4, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals("the splice and the owed root", generationBefore + 2, generation(instance));
                assertRepairMarkerStale(instance, generationBefore);
                assertCheckpointLadder(instance, false, "1970-01-01T00:00:00.000000Z -> 1\n1970-01-01T00:00:00.000001Z -> 2\n");
                assertNoRefreshFaults("lv");
            }
            final String rows = """
                    ts\tsym\tx\ts
                    1970-01-01T00:00:00.000000Z\ta\t0.5\t0.5
                    1970-01-01T00:00:00.000001Z\ta\t1.5\t2.0
                    """;
            for (int restart = 0; restart < 2; restart++) {
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(rows, DEPARTURE_ROWS_QUERY);
            }
            assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
        });
    }

    @Test
    public void testDepartureOfADaysFirstRowWhoseClosingSealFailsLandsOnThePreviousDay() throws Exception {
        // The departed row is the first of 2026-01-01, so the view's new frontier, which the failed
        // closing seal owed a root, is the last row of the day before. The first cycle without rows
        // past the cadence lands the root there, below the newest timestamp the view has seen on the
        // next day, and each restart restores from it.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitDeparture(job, "('2025-12-31T23:58:00.000000Z', 'a', 1.5)");
                commitDeparture(job, "('2025-12-31T23:59:00.000000Z', 'a', 2.5), ('2026-01-01T00:00:00.000000Z', 'a', 4.5)");
                final LiveViewInstance instance = viewInstance();
                Assert.assertEquals(ts("2025-12-31T23:58:00.000000Z"), instance.getHeadCheckpointMaxTs());
                final long generationBefore = generation(instance);
                failTheOwedResumeSeal(job, fault, LiveViewCheckpointDiskFullFault.OP_ALLOCATE, "('2026-01-01T00:00:00.000000Z', 'a', -1.0)");
                Assert.assertEquals(ts("2026-01-01T00:00:00.000000Z"), instance.getLatestSeenTs());
                assertRepairMarkerLive(instance, generationBefore);
                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'b', -1.0)");
                Assert.assertEquals("the cycle seals the day before's last row", ts("2025-12-31T23:59:00.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals("at its commit", 4, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals("the splice and the owed root", generationBefore + 2, generation(instance));
                assertRepairMarkerStale(instance, generationBefore);
                assertCheckpointLadder(instance, false, "2025-12-31T23:58:00.000000Z -> 1\n2025-12-31T23:59:00.000000Z -> 2\n");
                assertNoRefreshFaults("lv");
            }
            final String rows = """
                    ts\tsym\tx\ts
                    2025-12-31T23:58:00.000000Z\ta\t1.5\t1.5
                    2025-12-31T23:59:00.000000Z\ta\t2.5\t4.0
                    """;
            for (int restart = 0; restart < 2; restart++) {
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(rows, DEPARTURE_ROWS_QUERY);
            }
            assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
        });
    }

    @Test
    public void testLateRowAfterADepartureWhoseClosingSealFailsKeepsTheBaseWalBounded() throws Exception {
        // A seal per row: roots at 00:01, 00:02 and 00:03 (base commits 1 to 3), then an upsert
        // moves 00:03 out of the filter (4), which truncates the timeline at the new frontier 00:02
        // and leaves the newest timestamp the view has seen at 00:03. A late row at 00:02:30 (5)
        // resumes from the root at 00:02, and its closing seal, which owes the new frontier
        // 00:02:30 a root, fails. Then only rows the filter rejects arrive, above 00:03, three to a
        // duration cadence, each followed by a purge. The first lands the owed root at the table's
        // newest row, and the first of each cadence after it republishes that root at its commit,
        // so the purge keeps the commits above the older superblock slot's: never more than five.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        configureWalPurge();
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'a', 1.0)");
                commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', 2.0)");
                commitDeparture(job, "('2026-01-01T00:03:00.000000Z', 'a', 3.0)");
                commitDeparture(job, "('2026-01-01T00:03:00.000000Z', 'a', -3.0)");
                final LiveViewInstance instance = viewInstance();
                Assert.assertEquals(ts("2026-01-01T00:02:00.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(ts("2026-01-01T00:03:00.000000Z"), instance.getLatestSeenTs());
                final long generationBefore = generation(instance);
                failTheOwedResumeSeal(job, fault, LiveViewCheckpointDiskFullFault.OP_ALLOCATE, "('2026-01-01T00:02:30.000000Z', 'a', 2.5)");
                assertRepairMarkerLive(instance, generationBefore);
                Assert.assertEquals("the head stays parked at the departure's commit", 4, instance.getHeadCheckpointBaseSeqTxn());
                for (int i = 0; i < 24; i++) {
                    if (i % 3 == 0) {
                        setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                    }
                    commitDeparture(job, "('" + String.format("2026-01-01T00:%02d:00.000000Z", 4 + i) + "', 'b', -1.0)");
                    final long seqTxn = 6 + i;
                    // The commit of the cadence's first cycle, which published the head, and the
                    // slot the publication before it left: the splice's, at the late row's commit,
                    // under the first cadence.
                    final long headBaseSeqTxn = 6 + 3 * (i / 3);
                    final long olderSlotSeqTxn = i < 3 ? 5 : headBaseSeqTxn - 3;
                    Assert.assertEquals(ts("2026-01-01T00:02:30.000000Z"), instance.getHeadCheckpointMaxTs());
                    Assert.assertEquals("commit " + seqTxn, headBaseSeqTxn, instance.getHeadCheckpointBaseSeqTxn());
                    Assert.assertEquals("commit " + seqTxn, olderSlotSeqTxn, instance.getCheckpointTimelineWalPurgeFloor());
                    Assert.assertEquals("commit " + seqTxn, seqTxn - olderSlotSeqTxn, purgeBaseWalAndCountSegments());
                }
                assertRepairMarkerStale(instance, generationBefore);
                assertCheckpointLadder(instance, false, """
                        2026-01-01T00:01:00.000000Z -> 1
                        2026-01-01T00:02:00.000000Z -> 2
                        2026-01-01T00:02:30.000000Z -> 3
                        """);
                Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT));
                assertNoRefreshFaults("lv");
            }
            final String rows = """
                    ts\tsym\tx\ts
                    2026-01-01T00:01:00.000000Z\ta\t1.0\t1.0
                    2026-01-01T00:02:00.000000Z\ta\t2.0\t3.0
                    2026-01-01T00:02:30.000000Z\ta\t2.5\t5.5
                    """;
            for (int restart = 0; restart < 2; restart++) {
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(rows, DEPARTURE_ROWS_QUERY);
            }
            assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
        });
    }

    @Test
    public void testHeadMissOwedRootAboveTheNewestRowLandsOnTheFirstRowlessCycle() throws Exception {
        // One commit carries the late row 00:49:30 and an upsert of the newest row 00:55 out of the
        // filter: the head-miss rebuild's end-of-table splice keeps its newest root at 00:50, below
        // the new frontier 00:54, clears the head, and its closing seal fails. The newest timestamp
        // the view has seen stays at 00:55. The head is clear, so the first cycle without rows is due
        // at once, and it seals the table's newest row, 00:54: the owed root lands, reads the marker
        // as stale and moves the WAL floor up, and each restart restores over the day the base lost.
        assertHeadMissOwedRootAboveTheNewestRowLandsOnTheFirstRowlessCycle(false);
    }

    @Test
    public void testHeadMissOwedRootAboveTheNewestRowLandsOnTheFirstRowlessCycleOverANanosecondBase() throws Exception {
        assertHeadMissOwedRootAboveTheNewestRowLandsOnTheFirstRowlessCycle(true);
    }

    @Test
    public void testHeadMissOwedRootAboveTheNewestRowLandsOnTheNextLateRowsRebuildToTheEndOfTheTable() throws Exception {
        // One commit carries the late row 00:49:30 and an upsert of the newest row 00:55 out of the
        // filter, and the head-miss rebuild's closing seal fails: its splice's marker stays live,
        // the head clear, and the newest timestamp the view has seen stays at 00:55, above the
        // table's newest row, 00:54. A second late row, 00:49:45, arrives before any cycle without
        // rows. Over a frontier above the table's newest row the plan derives no convergence bound,
        // so the rebuild keeps no primary runtime: it runs to the end of the table, and its closing
        // seal stamps the last row it emitted, the table's newest, rather than the newest timestamp
        // seen. The owed root lands there, the marker goes, the WAL floor moves to the repair's
        // commit, and each restart restores both late rows over the day the base lost.
        configureWalPurge();
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createHeadMissDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    final LiveViewInstance instance = commitFiveRowsAboveTheHeadMissRoot(job);
                    final long generationBefore = generation(instance);
                    final long sealFailuresBefore = instance.getCheckpointSealFailures();
                    hooks(job).setSimulateHeadMissHeadSealStart(
                            () -> fault.arm(LiveViewCheckpointDiskFullFault.OP_ALLOCATE, LiveViewCheckpointLayout.DATA_DIR_NAME, 1)
                    );
                    commitDeparture(job, "('2026-01-01T00:49:30.000000Z', 'a', 1.0), ('2026-01-01T00:55:00.000000Z', 'a', -1.0)");
                    hooks(job).setSimulateHeadMissHeadSealStart(null);
                    fault.disarm();
                    Assert.assertNotNull("the closing seal must reach its data segment", fault.getFailedFile());
                    Assert.assertEquals(sealFailuresBefore + 1, instance.getCheckpointSealFailures());
                    Assert.assertEquals("the rebuild cleared the head", Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                    Assert.assertEquals(ts("2026-01-01T00:55:00.000000Z"), instance.getLatestSeenTs());
                    assertRepairMarkerLive(instance, generationBefore);

                    final long headMissRepairsBefore = countHeadMissRepairs(job);
                    commitDeparture(job, "('2026-01-01T00:49:45.000000Z', 'a', 1.0)");
                    capture.drain();
                    capture.assertLoggedRE("live view O3 replay \\[view=lv, lateRowTs=" + ts("2026-01-01T00:49:45.000000Z")
                            + ", .*, highTsExclusive=null, resumeFromAnchor=false, ");
                    Assert.assertEquals("the head-miss rebuild repairs the late row", headMissRepairsBefore + 1, countHeadMissRepairs(job));
                    Assert.assertEquals(
                            "its closing seal stamps the table's newest row",
                            ts("2026-01-01T00:54:00.000000Z"),
                            instance.getHeadCheckpointMaxTs()
                    );
                    Assert.assertEquals("at the repair's commit", 7, instance.getHeadCheckpointBaseSeqTxn());
                    Assert.assertEquals(
                            "and lands, declining nothing",
                            0,
                            job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT)
                    );
                    Assert.assertEquals(sealFailuresBefore + 1, instance.getCheckpointSealFailures());
                    Assert.assertEquals("the landing ends the failure streak", 0, instance.getSealFailureStreak());
                    Assert.assertEquals(ts("2026-01-01T00:55:00.000000Z"), instance.getLatestSeenTs());
                    Assert.assertEquals("the second splice and its closing seal", generationBefore + 3, generation(instance));
                    assertNoRepairMarker(instance);
                    // The replay fed the late row 00:49:45, below the root at 00:50 the splice
                    // re-versioned, so the root at 00:54 takes its place.
                    assertCheckpointLadder(instance, false, "2025-12-31T23:55:00.000000Z -> 1\n2026-01-01T00:54:00.000000Z -> 58\n");
                    Assert.assertEquals("both superblock slots stand at the repair's commit", 7, instance.getCheckpointTimelineWalPurgeFloor());
                    Assert.assertEquals("the purge keeps no commit", 0, purgeBaseWalAndCountSegments());
                    assertNoRefreshFaults("lv");
                }
                // Both late rows, through the table's newest row: 00:55 left the filter.
                final String rows = HEAD_MISS_ROWS_WITH_BOTH_LATE_ROWS.replace("2026-01-01T00:55:00.000000Z\ta\t1.0\t2.0\n", "");
                for (int restart = 0; restart < 2; restart++) {
                    restartAndRefresh();
                    assertRestoredFromTimeline("lv");
                    assertViewRows(rows, DEPARTURE_ROWS_QUERY);
                    // The base lost 2025-12-31, and no one-minute frame of a later row reaches it.
                    TestUtils.assertSqlCursors(
                            engine,
                            sqlExecutionContext,
                            "(" + HEAD_MISS_DEPARTURE_VIEW_SQL + ") ORDER BY 2, 1",
                            "(SELECT * FROM lv WHERE ts >= '2026-01-01') ORDER BY 2, 1",
                            LOG,
                            true
                    );
                    assertNoRefreshFaults("lv");
                }
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testABaseColumnAddedAfterTheDepartedOwedRootLandsRestoresInPlace() throws Exception {
        // The owed root the departure's failed closing seal left lands on a cycle without rows past
        // the cadence, so the base schema change that follows has the view restore its runtime from
        // the timeline in place, over the day the base lost: the view keeps every row, a kept row
        // after it folds onto the restored state, and each restart restores.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            final String rows = OWED_ROOT_DEPARTED_FRONTIER_ROWS + "2026-01-01T00:06:00.000000Z\ta\t1.0\t13.0\n";
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                final long generationBefore = generation(instance);
                failTheOwedResumeSeal(job, fault, LiveViewCheckpointDiskFullFault.OP_ALLOCATE, "('2026-01-01T00:04:00.000000Z', 'a', -1.0)");
                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'b', -1.0)");
                Assert.assertEquals(ts("2026-01-01T00:03:00.000000Z"), instance.getHeadCheckpointMaxTs());
                assertRepairMarkerStale(instance, generationBefore);
                final long runtimeRestoresBefore = instance.getCheckpointRuntimeRestores();
                final long rebuildAttemptsBefore = instance.getCheckpointRebuildAttempts();
                final long faultsBefore = instance.getRefreshFaultCount();
                execute("ALTER TABLE base ADD COLUMN y INT");
                drainWalQueue();
                driveRefreshToQuiescence(job);
                final LiveViewInstance recovered = viewInstance();
                Assert.assertEquals("the recovery restores the runtime in place", runtimeRestoresBefore + 1, recovered.getCheckpointRuntimeRestores());
                Assert.assertEquals("and rebuilds nothing", rebuildAttemptsBefore, recovered.getCheckpointRebuildAttempts());
                Assert.assertFalse(recovered.isCheckpointRecoveryBlocked());
                Assert.assertFalse(recovered.isInvalid());
                Assert.assertEquals("the schema change faults one turn", faultsBefore + 1, recovered.getRefreshFaultCount());
                assertViewRows(OWED_ROOT_DEPARTED_FRONTIER_ROWS, DEPARTURE_ROWS_QUERY);
                commitDeparture(job, "('2026-01-01T00:06:00.000000Z', 'a', 1.0, NULL)");
                assertViewRows(rows, DEPARTURE_ROWS_QUERY);
                Assert.assertEquals("and no turn after it", faultsBefore + 1, recovered.getRefreshFaultCount());
            }
            for (int restart = 0; restart < 2; restart++) {
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(rows, DEPARTURE_ROWS_QUERY);
            }
        });
    }

    @Test
    public void testWalPurgeAroundTheLandingOfADepartedOwedRootKeepsTheRestoreWal() throws Exception {
        // The departure's closing seal is the third failure in a row (base commits 6 to 8), which
        // pauses the seal with the head parked at commit 4 and the older superblock slot there too.
        // A purge before any cycle after the pause keeps every commit above it, as does one run on
        // the next cycle just ahead of its seal. That seal lands the owed root, the arms follow it,
        // and each restart restores from what the purges kept, with every row.
        configureWalPurge();
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                final long generationBefore = departAtTheThirdFailureInARow(job, fault, instance, false, false);
                Assert.assertEquals(4, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(4, instance.getCheckpointTimelineWalPurgeFloor());
                Assert.assertEquals("the purge keeps every commit above the parked head", 8 - 4, purgeBaseWalAndCountSegments());
                final AtomicLong headBaseAtTheHook = new AtomicLong(-1);
                final AtomicLong segmentsAtTheHook = new AtomicLong(-1);
                hooks(job).setSimulateNoRowHeadCheckpointStart(() -> {
                    headBaseAtTheHook.set(instance.getHeadCheckpointBaseSeqTxn());
                    segmentsAtTheHook.set(purgeBaseWalAndCountSegments());
                });
                commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'b', -1.0)");
                hooks(job).setSimulateNoRowHeadCheckpointStart(null);
                Assert.assertEquals("the head is still parked ahead of the seal", 4, headBaseAtTheHook.get());
                Assert.assertEquals("so the purge keeps every commit above it", 9 - 4, segmentsAtTheHook.get());
                Assert.assertEquals("the seal lands the owed root", ts("2026-01-01T00:03:00.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(9, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals("the older superblock slot holds the splice", 8, instance.getCheckpointTimelineWalPurgeFloor());
                assertRepairMarkerStale(instance, generationBefore);
                Assert.assertEquals(9 - 8, purgeBaseWalAndCountSegments());
                assertNoRefreshFaults("lv");
            }
            for (int restart = 0; restart < 2; restart++) {
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(OWED_ROOT_DEPARTED_FRONTIER_ROWS, DEPARTURE_ROWS_QUERY);
            }
        });
    }

    @Test
    public void testDropInsideThePauseOverADepartedOwedRootReleasesTheBaseWal() throws Exception {
        // The departure's closing seal is the third failure in a row (base commits 6 to 8): the
        // pause keeps the timeline, the live marker and both base WAL arms at the parked head's
        // commit 4. DROP LIVE VIEW inside the pause releases all of it.
        configureWalPurge();
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                departAtTheThirdFailureInARow(job, fault, instance, false, false);
                Assert.assertTrue(instance.isSealOnCooldown(currentMicros));
                Assert.assertEquals("the pause holds the head arm", 4, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals("and the timeline arm", 4, instance.getCheckpointTimelineWalPurgeFloor());
                execute("DROP LIVE VIEW lv");
                drainJob(job);
                Assert.assertNull(engine.getLiveViewRegistry().getViewInstance("lv"));
                Assert.assertEquals("a dropped view holds no base WAL", 0, purgeBaseWalAndCountSegments());
            }
        });
    }

    @Test
    public void testResumeOwedRootAboveTheNewestRowOverADriftedCountRetiresTheTimeline() throws Exception {
        // The cycle without rows past the cadence that would land the owed root at the table's
        // newest row meets a lifetime row count that drifted from the table: the seal re-seats the
        // count on the table's size, retires the timeline and its marker and stamps nothing, as a
        // seal over a drifted count does, which is neither a failure nor a decline. With no
        // timeline, the next cycle without rows declines a root at the newest timestamp the view
        // has seen and holds no base WAL, and each restart finds no timeline: the rebuild from the
        // applied base is refused over the day the base lost, and the view stops with every row.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        final LogCapture capture = new LogCapture();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                    failTheOwedResumeSeal(job, fault, LiveViewCheckpointDiskFullFault.OP_ALLOCATE, "('2026-01-01T00:04:00.000000Z', 'a', -1.0)");
                    final long mismatchesBefore = instance.getCheckpointRowCountMismatches();
                    final long sealFailuresBefore = instance.getCheckpointSealFailures();
                    final long declinesBefore = job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT);
                    hooks(job).setSimulateNoRowHeadCheckpointStart(() -> instance.addLvRowsTotal(1));
                    setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                    capture.start();
                    commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'b', -1.0)");
                    capture.drain();
                    capture.stop();
                    hooks(job).setSimulateNoRowHeadCheckpointStart(null);
                    Assert.assertEquals("the seal meets the drift", mismatchesBefore + 1, instance.getCheckpointRowCountMismatches());
                    Assert.assertEquals("and re-seats the count on the table's size", 4, instance.getLvRowsTotal());
                    Assert.assertEquals("a drift is no failed seal", sealFailuresBefore, instance.getCheckpointSealFailures());
                    Assert.assertEquals(
                            "nor a decline",
                            declinesBefore,
                            job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT)
                    );
                    Assert.assertEquals("no root lands", Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                    assertTimelineAndMarkerRetired(instance);
                    capture.assertLogged("live view emitted row count does not match its durable output, retiring the timeline [view=lv");

                    setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                    commitDeparture(job, "('2026-01-01T00:06:00.000000Z', 'b', -1.0)");
                    Assert.assertEquals(
                            "with no timeline the next cycle declines a root above the newest row",
                            declinesBefore + 1,
                            job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT)
                    );
                    Assert.assertEquals("and holds no base WAL", Numbers.LONG_NULL, instance.getHeadCheckpointBaseSeqTxn());
                    Assert.assertEquals(Numbers.LONG_NULL, instance.getCheckpointTimelineWalPurgeFloor());
                    assertNoRefreshFaults("lv");
                }
                assertRestartsBlocked(OWED_ROOT_DEPARTED_FRONTIER_ROWS, OWED_ROOT_HISTORY_FLOOR, "timeline is absent");
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testResumeOwedRootAboveTheNewestRowWaitsForTheApplyOfTheViewsBlock() throws Exception {
        // After the departure's closing seal failed, a kept row past the cadence commits its block
        // to the view's WAL while the view table's writer is held, so its apply lands nothing. Its
        // seal, and the seal of a cycle without rows after it, which stamps the table's newest row,
        // find the block outstanding and stamp nothing, which counts neither a failure nor a
        // decline, so the marker stays live. Once the writer goes, the block lands, and the next
        // cycle without rows, still due, lands the root at the kept row, the table's newest, which
        // reads the marker as stale. Each restart restores over the day the base lost.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            final String rows = OWED_ROOT_DEPARTED_FRONTIER_ROWS + "2026-01-01T00:05:00.000000Z\ta\t1.0\t13.0\n";
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                final long generationBefore = generation(instance);
                failTheOwedResumeSeal(job, fault, LiveViewCheckpointDiskFullFault.OP_ALLOCATE, "('2026-01-01T00:04:00.000000Z', 'a', -1.0)");
                final TableToken viewToken = instance.getLiveViewToken();
                final long sealFailuresBefore = instance.getCheckpointSealFailures();
                final long declinesBefore = job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT);
                // The lock reason is the WAL apply's own, which the inline apply takes for a
                // concurrent holder and returns from without suspending the table.
                try (TableWriter ignore = engine.getWriterUnsafe(viewToken, TableUtils.WAL_2_TABLE_WRITE_REASON)) {
                    setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                    commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'a', 1.0)");
                    commitDeparture(job, "('2026-01-01T00:06:00.000000Z', 'b', -1.0)");
                    Assert.assertEquals(
                            "the kept row's block waits for its apply",
                            1,
                            engine.getTableSequencerAPI().getTxnTracker(viewToken).getSeqTxn()
                                    - engine.getTableSequencerAPI().getTxnTracker(viewToken).getWriterTxn()
                    );
                    Assert.assertEquals("so no seal stamps a root", ts("2026-01-01T00:02:00.000000Z"), instance.getHeadCheckpointMaxTs());
                    Assert.assertEquals("which is no failed seal", sealFailuresBefore, instance.getCheckpointSealFailures());
                    Assert.assertEquals(
                            "nor a decline",
                            declinesBefore,
                            job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT)
                    );
                    assertRepairMarkerLive(instance, generationBefore);
                }
                driveRefreshToQuiescence(job);
                Assert.assertEquals(
                        "the block lands once the writer goes",
                        engine.getTableSequencerAPI().getTxnTracker(viewToken).getSeqTxn(),
                        engine.getTableSequencerAPI().getTxnTracker(viewToken).getWriterTxn()
                );
                commitDeparture(job, "('2026-01-01T00:07:00.000000Z', 'b', -1.0)");
                Assert.assertEquals("the next cycle lands the root at the kept row", ts("2026-01-01T00:05:00.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals("the splice and the root", generationBefore + 2, generation(instance));
                assertRepairMarkerStale(instance, generationBefore);
                assertCheckpointLadder(instance, false, """
                        2025-12-31T12:00:00.000000Z -> 1
                        2026-01-01T00:01:00.000000Z -> 2
                        2026-01-01T00:02:00.000000Z -> 3
                        2026-01-01T00:05:00.000000Z -> 5
                        """);
                Assert.assertEquals(sealFailuresBefore, instance.getCheckpointSealFailures());
                assertViewRows(rows, DEPARTURE_ROWS_QUERY);
                assertNoRefreshFaults("lv");
            }
            for (int restart = 0; restart < 2; restart++) {
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(rows, DEPARTURE_ROWS_QUERY);
            }
        });
    }

    @Test
    public void testRowsFedOverNoTimelineSealTheirFirstRootOnARowlessCycle() throws Exception {
        // The view's first seal, over its first two rows, fails on its data segment, so the view
        // holds no timeline, and the rows wait above no head. A cycle without rows asks for a first
        // root at the newest timestamp the view has seen, which with no timeline the seal does not
        // move: that timestamp is the newest row the table holds, so the first root lands there,
        // counting both rows, and each restart restores from it.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                fault.arm(LiveViewCheckpointDiskFullFault.OP_ALLOCATE, LiveViewCheckpointLayout.DATA_DIR_NAME, 1);
                commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'a', 1.5), ('2026-01-01T00:02:00.000000Z', 'a', 2.5)");
                fault.disarm();
                Assert.assertNotNull("the first seal must reach its data segment", fault.getFailedFile());
                final LiveViewInstance instance = viewInstance();
                Assert.assertEquals(1, instance.getCheckpointSealFailures());
                Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                Assert.assertEquals("no timeline", Numbers.LONG_NULL, instance.getCheckpointTimelineWalPurgeFloor());
                Assert.assertEquals("the rows wait above no head", ts("2026-01-01T00:01:00.000000Z"), instance.getMinSeenTsSinceCheckpoint());
                Assert.assertEquals(ts("2026-01-01T00:02:00.000000Z"), instance.getLatestSeenTs());

                commitDeparture(job, "('2026-01-01T00:03:00.000000Z', 'b', -1.0)");
                Assert.assertEquals("the cycle seals the first root", ts("2026-01-01T00:02:00.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals("at its commit", 2, instance.getHeadCheckpointBaseSeqTxn());
                assertCheckpointLadder(instance, false, "2026-01-01T00:02:00.000000Z -> 2\n");
                Assert.assertEquals(1, generation(instance));
                Assert.assertEquals(1, instance.getCheckpointSealFailures());
                Assert.assertEquals(0, instance.getSealFailureStreak());
                Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT));
                Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.CLEARED_HEAD_PUBLICATION_COUNT));
                assertNoRefreshFaults("lv");
            }
            final String rows = """
                    ts\tsym\tx\ts
                    2026-01-01T00:01:00.000000Z\ta\t1.5\t1.5
                    2026-01-01T00:02:00.000000Z\ta\t2.5\t4.0
                    """;
            for (int restart = 0; restart < 2; restart++) {
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(rows, DEPARTURE_ROWS_QUERY);
            }
            assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
        });
    }

    @Test
    public void testDropAfterTheDepartedOwedRootLandsReleasesTheBaseWal() throws Exception {
        // The owed root the departure's failed closing seal left (base commit 6) lands on the first
        // cycle without rows past the cadence (7), and a cycle inside the next cadence (8) publishes
        // nothing: the purge keeps the commits above the splice's slot. DROP LIVE VIEW in the middle
        // of that stretch releases all of it.
        configureWalPurge();
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                failTheOwedResumeSeal(job, fault, LiveViewCheckpointDiskFullFault.OP_ALLOCATE, "('2026-01-01T00:04:00.000000Z', 'a', -1.0)");
                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'b', -1.0)");
                Assert.assertEquals(ts("2026-01-01T00:03:00.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(7, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(6, instance.getCheckpointTimelineWalPurgeFloor());
                commitDeparture(job, "('2026-01-01T00:06:00.000000Z', 'b', -1.0)");
                Assert.assertEquals(7, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(8 - 6, purgeBaseWalAndCountSegments());
                execute("DROP LIVE VIEW lv");
                drainJob(job);
                Assert.assertNull(engine.getLiveViewRegistry().getViewInstance("lv"));
                Assert.assertEquals("a dropped view holds no base WAL", 0, purgeBaseWalAndCountSegments());
            }
        });
    }

    @Test
    public void testResumeOwedRootFaultThroughThePauseWhoseRetireKeepsOnlyTheTimelineBlocksTheRestart() throws Exception {
        // As below, with _timeline and the segment directories locked for the retire at the fourth
        // failure: the splice's generation stays on disk, one past the one its live marker names,
        // and the marker stays beside it. Each restart rebuilds over the marker rather than restore
        // that generation, and the restatement guard refuses the rebuild over the day the base
        // lost: the view stops with every row.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        final String rows = OWED_ROOT_ROWS + OWED_ROOT_ROW_0005 + OWED_ROOT_ROW_0006 + OWED_ROOT_ROW_0007;
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                    final long generationBefore = generation(instance);
                    final long lvSeqTxnBefore = engine.getTableSequencerAPI().lastTxn(instance.getLiveViewToken());
                    final long sealFailuresBefore = instance.getCheckpointSealFailures();
                    final long resetsBefore = instance.getCheckpointTimelineResets();
                    failTheOwedResumeSealAtItsPublication(job, "('2026-01-01T00:01:30.000000Z', 'a', 1.0)");
                    assertRepairMarkerLive(instance, generationBefore);

                    setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                    job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH);
                    try {
                        commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'a', 1.0)");
                        commitDeparture(job, "('2026-01-01T00:06:00.000000Z', 'a', 1.0)");
                    } finally {
                        job.setCheckpointTimelineTestFailureStage(0);
                    }
                    Assert.assertEquals(sealFailuresBefore + 3, instance.getCheckpointSealFailures());
                    Assert.assertEquals("the third failure pauses", resetsBefore, instance.getCheckpointTimelineResets());

                    setCurrentMicros(currentMicros + 61_000_000L);
                    job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH);
                    fault.lock(false);
                    try {
                        commitDeparture(job, "('2026-01-01T00:07:00.000000Z', 'a', 1.0)");
                    } finally {
                        fault.unlock();
                        job.setCheckpointTimelineTestFailureStage(0);
                    }
                    Assert.assertEquals(sealFailuresBefore + 4, instance.getCheckpointSealFailures());
                    Assert.assertEquals("the fourth failure retires the timeline", resetsBefore + 1, instance.getCheckpointTimelineResets());
                    Assert.assertEquals("the retire meets the lock once", 1, fault.getRefusedTimelineUnlinks());
                    Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                    assertTimelineOnDisk(instance, true);
                    assertRepairMarkerLive(instance, generationBefore);
                    try (Path dir = checkpointsDir(instance)) {
                        Assert.assertEquals(
                                "the marker records the seqTxn its repair's commit followed",
                                lvSeqTxnBefore,
                                LiveViewCheckpointRepairMarker.readLvSeqTxn(configuration, dir)
                        );
                    }
                    assertViewRows(rows, DEPARTURE_ROWS_QUERY);
                }
                assertRestartsBlockedByTheRepairMarker(rows, OWED_ROOT_HISTORY_FLOOR);
                capture.drain();
                capture.assertNotLogged("does not match durable materialization");
                capture.assertNotLogged(GUARD_STAND_DOWN);
            });
        } finally {
            capture.stop();
        }
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
    public void testOwedRootOfADepartureWhoseClosingSealFailsAfterThePauseRetiresOnce() throws Exception {
        // Three failed seals pause the seal, and the fault persists past the pause. The departure of
        // 00:04 after it attempts its closing seal, which fails: that fourth failure retires the
        // timeline and its marker, so the repair finds no marker left to report it kept.
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
    public void testOwedRootOfADepartureAtTheThirdFailureLandsAfterThePause() throws Exception {
        // Two failed seals at the frontier 00:04, then an upsert moves 00:04 out of the filter: the
        // resume owes a root at the new frontier 00:03, and its closing seal fails, the third
        // failure in a row, which pauses the seal and keeps the timeline. The repair keeps its
        // marker. A cycle without rows inside the pause attempts nothing; the first one after it
        // seals the table's newest row, 00:03, whatever the cadence, which lands the owed root,
        // reads the marker as stale and ends the failure streak. Each restart restores over the day
        // the base lost.
        assertOwedRootOfADepartureAtTheThirdFailureLandsAfterThePause(false, false);
    }

    @Test
    public void testOwedRootOfADepartureAtTheThirdFailureLandsAfterThePauseOverANanosecondBase() throws Exception {
        // The same departure over a base whose designated timestamp has nanosecond precision: the
        // repair logs the frontier it keeps its marker for and the splice's newest root in
        // nanoseconds, and the cycle after the pause stamps the root in the view's own unit.
        assertOwedRootOfADepartureAtTheThirdFailureLandsAfterThePause(true, false);
    }

    @Test
    public void testOwedRootOfADepartureAtTheThirdFailureOnItsPublicationLandsAfterThePause() throws Exception {
        // As above, with every failed seal failing ahead of its superblock rather than on the
        // allocation of its data segment.
        assertOwedRootOfADepartureAtTheThirdFailureLandsAfterThePause(false, true);
    }

    @Test
    public void testOwedRootOfADepartureAtTheThirdFailureRetiresAtTheFirstFailureAfterThePause() throws Exception {
        // The departure's closing seal is the third failure in a row, which pauses the seal and
        // keeps the timeline and the marker. The fault outlasts the pause: the first cycle without
        // rows after it seals the table's newest row and fails, and that fourth failure retires the
        // timeline and its marker, releases both base WAL arms and backs off two minutes. Past the
        // backoff each cycle without rows declines a first root at the newest timestamp the view
        // has seen and holds nothing, and each restart finds no timeline: the rebuild from the
        // applied base is refused over the day the base lost, and the view stops with every row.
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
                    departAtTheThirdFailureInARow(job, fault, instance, false, false);

                    setCurrentMicros(currentMicros + 61_000_000L);
                    hooks(job).setSimulateNoRowHeadCheckpointStart(
                            () -> fault.arm(LiveViewCheckpointDiskFullFault.OP_ALLOCATE, LiveViewCheckpointLayout.DATA_DIR_NAME, 1)
                    );
                    capture.start();
                    commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'b', -1.0)");
                    capture.drain();
                    capture.stop();
                    hooks(job).setSimulateNoRowHeadCheckpointStart(null);
                    fault.disarm();
                    Assert.assertNotNull("the first cycle after the pause attempts the seal", fault.getFailedFile());
                    Assert.assertEquals("the fourth failure in a row", sealFailuresBefore + 4, instance.getCheckpointSealFailures());
                    Assert.assertEquals("retires the timeline", resetsBefore + 1, instance.getCheckpointTimelineResets());
                    Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                    assertTimelineAndMarkerRetired(instance);
                    capture.assertLogged("live view checkpoint seal keeps failing, retiring the timeline and backing off "
                            + "[view=lv, consecutiveFailures=4, backoffMicros=120000000]");
                    Assert.assertTrue(instance.isSealOnCooldown(currentMicros + 119_000_000L));
                    Assert.assertFalse(instance.isSealOnCooldown(currentMicros + 121_000_000L));

                    setCurrentMicros(currentMicros + 121_000_000L);
                    for (int minute = 6; minute <= 7; minute++) {
                        final long declinesBefore = job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT);
                        commitDeparture(job, "('2026-01-01T00:0" + minute + ":00.000000Z', 'b', -1.0)");
                        Assert.assertEquals(
                                "one decline per cycle",
                                declinesBefore + 1,
                                job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT)
                        );
                        Assert.assertEquals(sealFailuresBefore + 4, instance.getCheckpointSealFailures());
                        Assert.assertEquals("nothing holds the base WAL", Numbers.LONG_NULL, instance.getHeadCheckpointBaseSeqTxn());
                        Assert.assertEquals(Numbers.LONG_NULL, instance.getCheckpointTimelineWalPurgeFloor());
                    }
                    assertNoRefreshFaults("lv");
                }
                assertRestartsBlocked(OWED_ROOT_DEPARTED_FRONTIER_ROWS, OWED_ROOT_HISTORY_FLOOR, "timeline is absent");
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testOwedRootOfADepartureWhoseFaultOutlastsThePauseAndWhoseRetireKeepsOnlyTheTimelineBlocksTheRestart() throws Exception {
        // As above, with every seal failing ahead of its superblock, and with _timeline and the
        // segment directories locked for the retire at the fourth failure, on the first cycle
        // without rows after the pause: the splice's generation stays on disk, one past the one
        // its live marker names, and the marker stays beside it. Each restart then rebuilds over
        // the marker, and the restatement guard refuses the rebuild over the day the base lost:
        // the view stops with every row it holds.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                    final long sealFailuresBefore = instance.getCheckpointSealFailures();
                    final long resetsBefore = instance.getCheckpointTimelineResets();
                    setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                    job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH);
                    try {
                        commitDeparture(job, "('2026-01-01T00:04:10.000000Z', 'b', -1.0)");
                        commitDeparture(job, "('2026-01-01T00:04:20.000000Z', 'b', -1.0)");
                    } finally {
                        job.setCheckpointTimelineTestFailureStage(0);
                    }
                    Assert.assertEquals("the seals at the frontier fail", sealFailuresBefore + 2, instance.getCheckpointSealFailures());
                    final long generationBefore = generation(instance);
                    hooks(job).setSimulateResumeHeadSealStart(
                            () -> job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH)
                    );
                    try {
                        commitDeparture(job, "('2026-01-01T00:04:00.000000Z', 'a', -1.0)");
                    } finally {
                        hooks(job).setSimulateResumeHeadSealStart(null);
                        job.setCheckpointTimelineTestFailureStage(0);
                    }
                    Assert.assertEquals("the third failure in a row", sealFailuresBefore + 3, instance.getCheckpointSealFailures());
                    Assert.assertEquals("pauses the seal and retires nothing", resetsBefore, instance.getCheckpointTimelineResets());
                    assertRepairMarkerLive(instance, generationBefore);

                    setCurrentMicros(currentMicros + 61_000_000L);
                    hooks(job).setSimulateNoRowHeadCheckpointStart(() -> {
                        job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH);
                        fault.lock(false);
                    });
                    try {
                        commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'b', -1.0)");
                    } finally {
                        hooks(job).setSimulateNoRowHeadCheckpointStart(null);
                        fault.unlock();
                        job.setCheckpointTimelineTestFailureStage(0);
                    }
                    Assert.assertEquals("the fourth failure in a row", sealFailuresBefore + 4, instance.getCheckpointSealFailures());
                    Assert.assertEquals("retires the timeline", resetsBefore + 1, instance.getCheckpointTimelineResets());
                    Assert.assertEquals("the retire meets the lock once", 1, fault.getRefusedTimelineUnlinks());
                    Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                    assertTimelineOnDisk(instance, true);
                    assertRepairMarkerLive(instance, generationBefore);
                    assertViewRows(OWED_ROOT_DEPARTED_FRONTIER_ROWS, DEPARTURE_ROWS_QUERY);
                }
                capture.drain();
                capture.assertLogged("live view O3 repair kept its repair marker, no root at the frontier [view=lv, "
                        + "frontier=2026-01-01T00:03:00.000000Z, spliceHeadRootMaxTs=2026-01-01T00:02:00.000000Z]");
                capture.assertLogged("live view checkpoint seal keeps failing, retiring the timeline and backing off "
                        + "[view=lv, consecutiveFailures=4, backoffMicros=120000000]");
                assertRestartsBlockedByTheRepairMarker(OWED_ROOT_DEPARTED_FRONTIER_ROWS, OWED_ROOT_HISTORY_FLOOR);
                capture.drain();
                capture.assertNotLogged("does not match durable materialization");
                capture.assertNotLogged(GUARD_STAND_DOWN);
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testOwedRootOfADepartureInsideThePauseLandsOnTheFirstRowlessCycleAfterIt() throws Exception {
        // Three failed seals at the frontier 00:04 pause the seal (base commits 6 to 8). An upsert
        // inside the pause moves 00:04 out of the filter (9): the resume owes a root at the new
        // frontier 00:03, and the pause holds its closing seal, which counts no failure; the repair
        // keeps its marker and the timeline. The first cycle without rows after the pause (10)
        // seals the table's newest row, 00:03, whatever the cadence: the owed root lands, reads the
        // marker as stale and ends the failure streak. The next one past the cadence (11)
        // republishes that root at its commit, and a kept row inside the cadence after it (12)
        // stays unsealed. Each restart restores over the day the base lost.
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
                final long generationBefore = generation(instance);
                final long declinesBefore = job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT);
                hooks(job).setSimulateResumeHeadSealStart(
                        () -> fault.arm(LiveViewCheckpointDiskFullFault.OP_ALLOCATE, LiveViewCheckpointLayout.DATA_DIR_NAME, 1)
                );
                commitDeparture(job, "('2026-01-01T00:04:00.000000Z', 'a', -1.0)");
                hooks(job).setSimulateResumeHeadSealStart(null);
                fault.disarm();
                Assert.assertNull("the pause holds the closing seal", fault.getFailedFile());
                Assert.assertEquals(sealFailuresBefore + 3, instance.getCheckpointSealFailures());
                Assert.assertEquals("nothing retires", resetsBefore, instance.getCheckpointTimelineResets());
                Assert.assertEquals(ts("2026-01-01T00:02:00.000000Z"), instance.getHeadCheckpointMaxTs());
                assertTimelineOnDisk(instance, true);
                assertRepairMarkerLive(instance, generationBefore);

                setCurrentMicros(currentMicros + 61_000_000L);
                commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'b', -1.0)");
                Assert.assertEquals("the first cycle after the pause lands the owed root", ts("2026-01-01T00:03:00.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals("at its commit", 10, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals("the splice and the owed root", generationBefore + 2, generation(instance));
                assertRepairMarkerStale(instance, generationBefore);
                assertCheckpointLadder(instance, false, OWED_ROOT_DEPARTED_FRONTIER_LADDER);
                Assert.assertEquals("the root that lands ends the streak", 0, instance.getSealFailureStreak());
                Assert.assertFalse(instance.isSealOnCooldown(currentMicros));
                Assert.assertEquals(sealFailuresBefore + 3, instance.getCheckpointSealFailures());

                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                commitDeparture(job, "('2026-01-01T00:06:00.000000Z', 'b', -1.0)");
                Assert.assertEquals(ts("2026-01-01T00:03:00.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals("the next cycle past the cadence republishes the root", 11, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals("one publication", generationBefore + 3, generation(instance));
                Assert.assertEquals("the WAL floor follows it", 10, instance.getCheckpointTimelineWalPurgeFloor());
                commitDeparture(job, "('2026-01-01T00:07:00.000000Z', 'a', 1.0)");
                Assert.assertEquals("a kept row inside the cadence stays unsealed", ts("2026-01-01T00:03:00.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(
                        "no cycle declines a root",
                        declinesBefore,
                        job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT)
                );
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
    public void testRestartInsideThePauseOverADepartedOwedRootStopsTheViewWithEveryRow() throws Exception {
        // The departure's closing seal is the third failure in a row: the seal pauses, and the repair
        // keeps its marker and the timeline. A restart inside the pause refuses the timeline the
        // live marker names, and the rebuild from the applied base is refused over the day the base
        // lost: the view stops with every row it holds.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                departAtTheThirdFailureInARow(job, fault, instance, false, false);
                Assert.assertTrue(instance.isSealOnCooldown(currentMicros));
                assertNoRefreshFaults("lv");
            }
            assertRestartsBlockedByTheRepairMarker(OWED_ROOT_DEPARTED_FRONTIER_ROWS, OWED_ROOT_HISTORY_FLOOR);
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

    @Test
    public void testResumeWhoseSpliceFailsAndWhoseRetireKeepsTheTimelineOpensAFreshHistoryAtTheNextSeal() throws Exception {
        // The departure of 00:03 resumes from the root at 00:01, and its splice fails after its data:
        // the output moved under every root above the anchor, so the timeline retires, under a lock
        // on _timeline that keeps the file, and the splice's live marker beside it. The lock goes
        // ahead of the closing seal, which retires the kept file first and opens a fresh history at
        // the frontier 00:02. No root of the departed row, whose state still holds its 9.5, is left
        // to anchor the late row 00:03:30, which resumes from the fresh root.
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = commitTwoRootsUnderASplicedDeparture(job);
                final long sealFailuresBefore = instance.getCheckpointSealFailures();
                final long resetsBefore = instance.getCheckpointTimelineResets();
                departWithTheSpliceFailingUnderALock(job, fault, true);
                Assert.assertEquals("the splice's retire meets the lock alone", 1, fault.getRefusedTimelineUnlinks());
                Assert.assertEquals("the seal's retry of the retire counts no reset", resetsBefore + 1, instance.getCheckpointTimelineResets());
                Assert.assertFalse("the seal's retry removed the kept timeline", instance.isCheckpointTimelineRetirePending());
                Assert.assertEquals("and the seal lands", sealFailuresBefore, instance.getCheckpointSealFailures());
                Assert.assertEquals("on a fresh history", 1, generation(instance));
                assertCheckpointLadder(instance, false, "2026-01-01T00:02:00.000000Z -> 2\n");
                assertNoRepairMarker(instance);
                commitDeparture(job, "('2026-01-01T00:04:00.000000Z', 'a', 1.0)");
                final long resumesBefore = job.getStatsForTest().getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RESUME_FROM_ANCHOR);
                commitDeparture(job, "('2026-01-01T00:03:30.000000Z', 'a', 2.0)");
                Assert.assertEquals(
                        "the late row resumes from the fresh root at 00:02",
                        resumesBefore + 1,
                        job.getStatsForTest().getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RESUME_FROM_ANCHOR)
                );
            }
            assertViewRows(SPLICED_DEPARTURE_LATE_ROW_ROWS, DEPARTURE_ROWS_QUERY);
            assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
        });
    }

    @Test
    public void testResumeWhoseSpliceFailsWhileItsTimelineStaysLockedRepairsLateRowsWithoutAnAnchor() throws Exception {
        // As above, with the lock held through a forward row and a late row. Every seal retries the
        // retire first and is refused, the third refusal pausing the seal; the late row finds no
        // anchor, since the timeline it would search is the retired one, and the head-miss rebuild
        // repairs it from scratch. Once the lock goes, the first seal past the pause retires the
        // kept file and opens a fresh history, which a restart restores.
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    final LiveViewInstance instance = commitTwoRootsUnderASplicedDeparture(job);
                    final long spliceBaseGeneration = superblockGeneration(instance);
                    final long sealFailuresBefore = instance.getCheckpointSealFailures();
                    final long resetsBefore = instance.getCheckpointTimelineResets();
                    departWithTheSpliceFailingUnderALock(job, fault, false);
                    Assert.assertEquals("the retire, then the closing seal's retry of it", 2, fault.getRefusedTimelineUnlinks());
                    Assert.assertEquals("the closing seal is refused", sealFailuresBefore + 1, instance.getCheckpointSealFailures());
                    assertTimelineRetirePending(instance, spliceBaseGeneration, spliceBaseGeneration);
                    assertSpliceMarkerLive(instance);

                    commitDeparture(job, "('2026-01-01T00:04:00.000000Z', 'a', 1.0)");
                    Assert.assertEquals(3, fault.getRefusedTimelineUnlinks());
                    Assert.assertEquals("the forward row's seal is refused", sealFailuresBefore + 2, instance.getCheckpointSealFailures());

                    final long resumesBefore = job.getStatsForTest().getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RESUME_FROM_ANCHOR);
                    final long headMissRepairsBefore = countHeadMissRepairs(job);
                    capture.drain();
                    commitDeparture(job, "('2026-01-01T00:03:30.000000Z', 'a', 2.0)");
                    Assert.assertEquals(
                            "the late row takes no anchor",
                            resumesBefore,
                            job.getStatsForTest().getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RESUME_FROM_ANCHOR)
                    );
                    Assert.assertEquals("the head-miss rebuild repairs it", headMissRepairsBefore + 1, countHeadMissRepairs(job));
                    capture.drain();
                    capture.assertLogged("live view checkpoint timeline holds no resume anchor [view=lv");
                    capture.assertLogged(KEPT_TIMELINE_REFUSAL);
                    Assert.assertEquals("the rebuild's retire, then its closing seal's retry of it", 5, fault.getRefusedTimelineUnlinks());
                    Assert.assertEquals("the splice's retire and the rebuild's", resetsBefore + 2, instance.getCheckpointTimelineResets());
                    Assert.assertEquals("the third refusal", sealFailuresBefore + 3, instance.getCheckpointSealFailures());
                    Assert.assertTrue("pauses the seal", instance.isSealOnCooldown(currentMicros));
                    assertTimelineRetirePending(instance, spliceBaseGeneration, spliceBaseGeneration);
                    assertSpliceMarkerLive(instance);
                    assertViewRows(SPLICED_DEPARTURE_LATE_ROW_ROWS, DEPARTURE_ROWS_QUERY);
                    assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);

                    fault.unlock();
                    setCurrentMicros(currentMicros + 61_000_000L);
                    commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'a', 1.0)");
                    Assert.assertFalse("the seal past the pause retires the kept timeline", instance.isCheckpointTimelineRetirePending());
                    Assert.assertEquals("and opens a fresh history", 1, generation(instance));
                    assertCheckpointLadder(instance, false, "2026-01-01T00:05:00.000000Z -> 5\n");
                    assertNoRepairMarker(instance);
                    Assert.assertEquals("the seal that lands ends the streak", 0, instance.getSealFailureStreak());
                    Assert.assertFalse(instance.isSealOnCooldown(currentMicros));
                }
                // s sums x over the row and the two rows of 'a' below it.
                final String rows = SPLICED_DEPARTURE_LATE_ROW_ROWS + "2026-01-01T00:05:00.000000Z\ta\t1.0\t4.0\n";
                assertViewRows(rows, DEPARTURE_ROWS_QUERY);
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(rows, DEPARTURE_ROWS_QUERY);
                assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testChangeBelowEveryRootWhoseRetireCannotCheckTheTimelineIsRefusedOnEveryWorker() throws Exception {
        assertChangeBelowEveryRootWhoseTimelineStaysLockedIsRefusedOnEveryWorker(true);
    }

    @Test
    public void testChangeBelowEveryRootWhoseRetireCannotCheckTheTimelineOpensAFreshHistoryAtTheNextSeal() throws Exception {
        assertChangeBelowEveryRootWhoseRetireKeepsTheTimelineOpensAFreshHistory(false, true);
    }

    @Test
    public void testChangeBelowEveryRootWhoseRetireCannotCheckTheTimelineOpensAFreshHistoryAtTheNextSealOverANanosecondBase() throws Exception {
        assertChangeBelowEveryRootWhoseRetireKeepsTheTimelineOpensAFreshHistory(true, true);
    }

    @Test
    public void testChangeBelowEveryRootWhoseRetireCannotCheckTheTimelineRebuildsOnRestart() throws Exception {
        assertChangeBelowEveryRootWhoseTimelineStaysLockedRebuildsOnRestart(true);
    }

    @Test
    public void testChangeBelowEveryRootWhoseRetireCannotCheckTheTimelineRepairsLateRowsWithoutAnAnchor() throws Exception {
        assertChangeBelowEveryRootWhileItsTimelineStaysLockedRepairsLateRowsWithoutAnAnchor(true);
    }

    @Test
    public void testChangeBelowEveryRootWhoseRetireCannotCheckTheTimelineStopsTheRestartOverTheDayTheBaseLost() throws Exception {
        assertChangeBelowEveryRootWhoseTimelineStaysLockedStopsTheRestartOverTheDayTheBaseLost(true);
    }

    @Test
    public void testChangeBelowEveryRootWhoseRetireKeepsTheTimelineOpensAFreshHistoryAtTheNextSeal() throws Exception {
        assertChangeBelowEveryRootWhoseRetireKeepsTheTimelineOpensAFreshHistory(false, false);
    }

    @Test
    public void testChangeBelowEveryRootWhoseRetireKeepsTheTimelineOpensAFreshHistoryAtTheNextSealOverANanosecondBase() throws Exception {
        assertChangeBelowEveryRootWhoseRetireKeepsTheTimelineOpensAFreshHistory(true, false);
    }

    @Test
    public void testChangeBelowEveryRootWhileItsTimelineStaysLockedRepairsLateRowsWithoutAnAnchor() throws Exception {
        assertChangeBelowEveryRootWhileItsTimelineStaysLockedRepairsLateRowsWithoutAnAnchor(false);
    }

    @Test
    public void testChangeBelowEveryRootWhoseTimelineStaysLockedIsRefusedOnEveryWorker() throws Exception {
        assertChangeBelowEveryRootWhoseTimelineStaysLockedIsRefusedOnEveryWorker(false);
    }

    @Test
    public void testChangeBelowEveryRootWhoseTimelineStaysLockedRebuildsOnRestart() throws Exception {
        assertChangeBelowEveryRootWhoseTimelineStaysLockedRebuildsOnRestart(false);
    }

    @Test
    public void testChangeBelowEveryRootWhoseTimelineStaysLockedStopsTheRestartOverTheDayTheBaseLost() throws Exception {
        assertChangeBelowEveryRootWhoseTimelineStaysLockedStopsTheRestartOverTheDayTheBaseLost(false);
    }

    @Test
    public void testSealsOverAKeptTimelineBackOffOnTheSealFailureBudget() throws Exception {
        // While the lock holds, every seal retries the retire before it appends and is refused when
        // the retry meets the lock, which spends the seal failure budget as any failed seal does: the
        // third refusal pauses the seal a minute, the first after the pause retires once more and
        // pauses it two, and the first after that four. No retry runs inside a pause, so the unlinks
        // the lock refuses follow that schedule. Once the lock goes, the first seal past the pause
        // opens a fresh history and ends the streak.
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    final LiveViewInstance instance = commitFourRoots(job, false);
                    final long generationBefore = superblockGeneration(instance);
                    final long sealFailuresBefore = instance.getCheckpointSealFailures();
                    final long resetsBefore = instance.getCheckpointTimelineResets();
                    changeBelowEveryRootUnderALock(job, fault);
                    assertSealRetries(instance, sealFailuresBefore + 1, fault, 2, resetsBefore + 1);

                    commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'a', 1.0)");
                    assertSealRetries(instance, sealFailuresBefore + 2, fault, 3, resetsBefore + 1);
                    commitDeparture(job, "('2026-01-01T00:06:00.000000Z', 'a', 1.0)");
                    assertSealRetries(instance, sealFailuresBefore + 3, fault, 4, resetsBefore + 1);
                    Assert.assertTrue("the third refusal pauses the seal a minute", instance.isSealOnCooldown(currentMicros + 59_000_000L));
                    Assert.assertFalse(instance.isSealOnCooldown(currentMicros + 61_000_000L));

                    setCurrentMicros(currentMicros + 30_000_000L);
                    commitDeparture(job, "('2026-01-01T00:07:00.000000Z', 'a', 1.0)");
                    assertSealRetries(instance, sealFailuresBefore + 3, fault, 4, resetsBefore + 1);

                    setCurrentMicros(currentMicros + 31_000_000L);
                    commitDeparture(job, "('2026-01-01T00:08:00.000000Z', 'a', 1.0)");
                    // The seal's retry, then the retire the failure after the pause runs.
                    assertSealRetries(instance, sealFailuresBefore + 4, fault, 6, resetsBefore + 2);
                    Assert.assertTrue("which pauses the seal two minutes", instance.isSealOnCooldown(currentMicros + 119_000_000L));
                    Assert.assertFalse(instance.isSealOnCooldown(currentMicros + 121_000_000L));

                    setCurrentMicros(currentMicros + 60_000_000L);
                    commitDeparture(job, "('2026-01-01T00:09:00.000000Z', 'a', 1.0)");
                    assertSealRetries(instance, sealFailuresBefore + 4, fault, 6, resetsBefore + 2);

                    setCurrentMicros(currentMicros + 61_000_000L);
                    commitDeparture(job, "('2026-01-01T00:10:00.000000Z', 'a', 1.0)");
                    assertSealRetries(instance, sealFailuresBefore + 5, fault, 8, resetsBefore + 3);
                    Assert.assertTrue("and the next one four", instance.isSealOnCooldown(currentMicros + 239_000_000L));
                    Assert.assertFalse(instance.isSealOnCooldown(currentMicros + 241_000_000L));
                    assertTimelineRetirePending(instance, generationBefore, Numbers.LONG_NULL);
                    capture.drain();
                    capture.assertLogged("live view checkpoint seal keeps failing, keeping the timeline and backing off "
                            + "[view=lv, consecutiveFailures=3, backoffMicros=60000000]");
                    capture.assertLogged("live view checkpoint seal keeps failing, retiring the timeline and backing off "
                            + "[view=lv, consecutiveFailures=4, backoffMicros=120000000]");
                    capture.assertLogged("live view checkpoint seal keeps failing, retiring the timeline and backing off "
                            + "[view=lv, consecutiveFailures=5, backoffMicros=240000000]");
                    assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);

                    fault.unlock();
                    setCurrentMicros(currentMicros + 241_000_000L);
                    commitDeparture(job, "('2026-01-01T00:11:00.000000Z', 'a', 1.0)");
                    Assert.assertFalse(instance.isCheckpointTimelineRetirePending());
                    Assert.assertEquals("the seal past the pause opens a fresh history", 1, generation(instance));
                    assertCheckpointLadder(instance, false, "2026-01-01T00:11:00.000000Z -> 11\n");
                    Assert.assertEquals("and ends the streak", 0, instance.getSealFailureStreak());
                    Assert.assertFalse(instance.isSealOnCooldown(currentMicros));
                }
                assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testOnlyTheRetireThatFirstKeepsTheTimelineLogsItsRefusal() throws Exception {
        // The change below every root keeps _timeline under the lock, and the closing seal's and
        // the forward row's retries of the retire meet the lock too: only the retire that first kept
        // the file logs that the view refuses it. With the lock gone the next seal's retry removes
        // the file. A retire that keeps it again under a new lock then logs once more, and the
        // seal's retry after it does not.
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    final LiveViewInstance instance = commitFourRoots(job, false);
                    changeBelowEveryRootUnderALock(job, fault);
                    commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'a', 5.0)");
                    Assert.assertEquals("the retire, then two retries of it", 3, fault.getRefusedTimelineUnlinks());
                    capture.drain();
                    Assert.assertEquals("only the retire that kept the file first logs it", 1, capture.countOccurrences(KEPT_TIMELINE_NEWLY_KEPT));

                    fault.unlock();
                    commitDeparture(job, "('2026-01-01T00:06:00.000000Z', 'a', 6.0)");
                    Assert.assertFalse("the seal's retry removes the file", instance.isCheckpointTimelineRetirePending());
                    Assert.assertEquals("and opens a fresh history", 1, generation(instance));

                    keepTheTimelineUnderALock(job, fault);
                    commitDeparture(job, "('2026-01-01T00:07:00.000000Z', 'a', 7.0)");
                    Assert.assertEquals("the truncate's retire, then the seal's retry of it", 2, fault.getRefusedTimelineUnlinks());
                    Assert.assertTrue(instance.isCheckpointTimelineRetirePending());
                    capture.drain();
                    Assert.assertEquals("the retire that keeps the file again logs it once more", 2, capture.countOccurrences(KEPT_TIMELINE_NEWLY_KEPT));
                    fault.unlock();
                }
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testRetireThatCannotCheckTheTimelineAndCannotWriteItsMarkerStillRefusesIt() throws Exception {
        assertRetireThatKeepsTheTimelineAndCannotWriteItsMarkerStillRefusesIt(true);
    }

    @Test
    public void testRetireThatKeepsTheTimelineAndCannotWriteItsMarkerStillRefusesTheTimeline() throws Exception {
        assertRetireThatKeepsTheTimelineAndCannotWriteItsMarkerStillRefusesIt(false);
    }

    @Test
    public void testRetireRetryThatCannotCheckTheTimelineKeepsRefusingItUntilTheNextSeal() throws Exception {
        // The change below every root keeps _timeline under the lock, and the lock goes. The next
        // seal's retry of the retire removes the file and the marker beside it, but the check of
        // _timeline after the unlink fails: the retire cannot tell whether the file went, so the
        // view goes on refusing it, writes the marker a kept file needs, beside no file, and the
        // seal is refused. The seal after it retries again, finds the file gone, removes the marker
        // and opens a fresh history.
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    final LiveViewInstance instance = commitFourRoots(job, false);
                    final long sealFailuresBefore = instance.getCheckpointSealFailures();
                    final long resetsBefore = instance.getCheckpointTimelineResets();
                    try {
                        changeBelowEveryRootUnderALock(job, fault);
                    } finally {
                        fault.unlock();
                    }
                    Assert.assertEquals("the closing seal is refused", sealFailuresBefore + 1, instance.getCheckpointSealFailures());

                    fault.failTheCheckAfterTheNextTimelineUnlink();
                    capture.drain();
                    commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'a', 5.0)");
                    Assert.assertEquals("the retry's check of the timeline fails", 1, fault.getFailedTimelineChecks());
                    capture.drain();
                    capture.assertLogged("could not check the live view checkpoint timeline after a retire [view=lv");
                    assertTimelineOnDisk(instance, false);
                    assertMarkerNames(Numbers.LONG_NULL);
                    Assert.assertTrue("the view goes on refusing the timeline it could not check", instance.isCheckpointTimelineRetirePending());
                    Assert.assertEquals("so the seal is refused", sealFailuresBefore + 2, instance.getCheckpointSealFailures());
                    Assert.assertEquals("and the retry counts no reset", resetsBefore + 1, instance.getCheckpointTimelineResets());
                    Assert.assertEquals("no head stands", Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());

                    commitDeparture(job, "('2026-01-01T00:06:00.000000Z', 'a', 6.0)");
                    capture.drain();
                    capture.assertLogged("live view removed the checkpoint timeline an earlier retire left behind [view=lv");
                    Assert.assertFalse("the next retry finds the file gone", instance.isCheckpointTimelineRetirePending());
                    assertNoRepairMarker(instance);
                    Assert.assertEquals("and its seal opens a fresh history", 1, generation(instance));
                    assertCheckpointLadder(instance, false, "2026-01-01T00:06:00.000000Z -> 6\n");
                    Assert.assertEquals("which lands", sealFailuresBefore + 2, instance.getCheckpointSealFailures());
                    Assert.assertEquals("and ends the streak", 0, instance.getSealFailureStreak());
                    Assert.assertEquals(resetsBefore + 1, instance.getCheckpointTimelineResets());
                }
                // 00:06 sums 6.0 + 5.0 + 4.0.
                assertViewRows(
                        CHANGE_BELOW_EVERY_ROOT_ROWS + CHANGE_BELOW_EVERY_ROOT_ROW_0005 + "2026-01-01T00:06:00.000000Z\ta\t6.0\t15.0\n",
                        DEPARTURE_ROWS_QUERY
                );
                assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testRetireThatRemovesTheTimelineButCannotCheckItRefusesItUntilTheClosingSeal() throws Exception {
        // No lock: the head-miss rebuild's retire removes _timeline, and the check of it after the
        // unlink fails, so the retire cannot tell whether the file went. The view refuses the
        // timeline as one the retire kept, and writes the marker such a file needs, beside no file.
        // The closing seal's retry of the retire finds the file gone, which takes the marker with
        // it, and opens a fresh history at the frontier. The late row then finds no root below it.
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    final LiveViewInstance instance = commitFourRoots(job, false);
                    final long sealFailuresBefore = instance.getCheckpointSealFailures();
                    final long resetsBefore = instance.getCheckpointTimelineResets();
                    final AtomicBoolean isPendingAtTheSeal = new AtomicBoolean();
                    final AtomicBoolean isTimelineAtTheSeal = new AtomicBoolean(true);
                    final AtomicBoolean isMarkerAtTheSeal = new AtomicBoolean();
                    final AtomicReference<Long> markerBaseAtTheSeal = new AtomicReference<>();
                    hooks(job).setSimulateHeadMissHeadSealStart(() -> {
                        isPendingAtTheSeal.set(instance.isCheckpointTimelineRetirePending());
                        try (Path dir = checkpointsDir(instance); Path timelinePath = new Path()) {
                            final FilesFacade ff = configuration.getFilesFacade();
                            isTimelineAtTheSeal.set(ff.exists(LiveViewCheckpointLayout.timelinePath(timelinePath, dir).$()));
                            isMarkerAtTheSeal.set(LiveViewCheckpointRepairMarker.exists(ff, dir));
                            markerBaseAtTheSeal.set(LiveViewCheckpointRepairMarker.readBaseGeneration(configuration, dir));
                        }
                    });
                    fault.failTheCheckAfterTheNextTimelineUnlink();
                    try {
                        commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'a', 100.0)");
                    } finally {
                        hooks(job).setSimulateHeadMissHeadSealStart(null);
                    }
                    Assert.assertEquals("the retire's check of the timeline fails", 1, fault.getFailedTimelineChecks());
                    Assert.assertTrue("the view refuses the timeline it could not check", isPendingAtTheSeal.get());
                    Assert.assertFalse("which the retire removed", isTimelineAtTheSeal.get());
                    Assert.assertTrue("with the marker a kept file needs beside it", isMarkerAtTheSeal.get());
                    Assert.assertEquals("naming no generation", Long.valueOf(Numbers.LONG_NULL), markerBaseAtTheSeal.get());
                    capture.drain();
                    Assert.assertEquals(1, capture.countOccurrences(KEPT_TIMELINE_CHECK_FAILED));
                    Assert.assertEquals(1, capture.countOccurrences(KEPT_TIMELINE_NEWLY_KEPT));
                    capture.assertLogged("live view removed the checkpoint timeline an earlier retire left behind [view=lv");
                    Assert.assertFalse("the closing seal's retry finds the file gone", instance.isCheckpointTimelineRetirePending());
                    assertNoRepairMarker(instance);
                    Assert.assertEquals("and its seal opens a fresh history", 1, generation(instance));
                    assertCheckpointLadder(instance, false, "2026-01-01T00:04:00.000000Z -> 4\n");
                    Assert.assertEquals("which lands", sealFailuresBefore, instance.getCheckpointSealFailures());
                    Assert.assertEquals("the rebuild retires once", resetsBefore + 1, instance.getCheckpointTimelineResets());

                    final long resumesBefore = job.getStatsForTest().getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RESUME_FROM_ANCHOR);
                    commitDeparture(job, "('2026-01-01T00:01:30.000000Z', 'a', 0.5)");
                    Assert.assertEquals(
                            "no root of the fresh history sits below the late row",
                            resumesBefore,
                            job.getStatsForTest().getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RESUME_FROM_ANCHOR)
                    );
                }
                assertViewRows(CHANGE_BELOW_EVERY_ROOT_LATE_ROW_ROWS, DEPARTURE_ROWS_QUERY);
                assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testRestartBetweenARetireThatCannotCheckItsRemovedTimelineAndItsClosingSealRebuilds() throws Exception {
        // No lock: the head-miss rebuild's retire removes _timeline and cannot check it, so it
        // writes the marker a kept file needs, beside no file, and the process stops at the change's
        // replacement commit. The restart meets the marker and rebuilds from the applied base, whose
        // retire takes the marker. The next row's seal lands, and the restart after it restores.
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = commitFourRoots(job, false);
                final LiveViewReplacementCommitWitness witness = new LiveViewReplacementCommitWitness(
                        engine,
                        instance,
                        Numbers.LONG_NULL,
                        LiveViewReplacementCommitWitness.FAIL_CANCELLATION
                );
                fault.failTheCheckAfterTheNextTimelineUnlink();
                commitAndStopAtTheReplacementCommit(witness, job, "('2026-01-01T00:01:00.000000Z', 'a', 100.0)");
                Assert.assertEquals("the retire's check of the timeline fails", 1, fault.getFailedTimelineChecks());
                assertMarkersRecorded(witness, Numbers.LONG_NULL);
                Assert.assertTrue(instance.isCheckpointTimelineRetirePending());
                assertTimelineOnDisk(instance, false);
                assertMarkerNames(Numbers.LONG_NULL);
            }
            restartAndRefresh();
            assertRebuiltFromAppliedBase("lv");
            final LiveViewInstance restarted = viewInstance();
            Assert.assertFalse(restarted.isCheckpointTimelineRetirePending());
            assertViewRows(CHANGE_BELOW_EVERY_ROOT_ROWS, DEPARTURE_ROWS_QUERY);
            assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final long sealFailuresBefore = restarted.getCheckpointSealFailures();
                commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'a', 5.0)");
                Assert.assertEquals("the next row's seal lands", sealFailuresBefore, restarted.getCheckpointSealFailures());
                assertNoRepairMarker(restarted);
            }
            final String rows = CHANGE_BELOW_EVERY_ROOT_ROWS + CHANGE_BELOW_EVERY_ROOT_ROW_0005;
            assertViewRows(rows, DEPARTURE_ROWS_QUERY);
            restartAndRefresh();
            assertRestoredFromTimeline("lv");
            assertViewRows(rows, DEPARTURE_ROWS_QUERY);
            assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
        });
    }

    @Test
    public void testRetireRemovesAndChecksTheTimelineWithoutAllocating() throws Exception {
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = commitFourRoots(job, false);
                assertRetiresWithoutAllocating(job, fault, instance);
                assertCheckpointFilesRetired(instance);
            }
        });
    }

    @Test
    public void testRetireOfAViewWhoseCheckpointPathsOutgrowAPathAllocatesOnlyTheFirstTime() throws Exception {
        // The long view's directory name takes 225 bytes, 75 characters of three bytes each, so its
        // checkpoint paths outgrow the 255 bytes a Path starts with, and its first retire on the job
        // grows the buffers the job builds them in. Every retire after it allocates nothing, and a
        // retire of a view with a short name on the same job removes that view's files.
        final String longName = "\u4e2d".repeat(75);
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            execute("CREATE LIVE VIEW \"" + longName + "\" FLUSH EVERY 100ms START FROM BEGINNING AS " + DEPARTURE_VIEW_SQL);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, longName);
                final LiveViewInstance shortView = commitFourRoots(job, false);
                final LiveViewInstance longView = engine.getLiveViewRegistry().getViewInstance(longName);
                Assert.assertNotNull(longView);
                assertCheckpointLadder(longView, false, """
                        2026-01-01T00:01:00.000000Z -> 1
                        2026-01-01T00:02:00.000000Z -> 2
                        2026-01-01T00:03:00.000000Z -> 3
                        2026-01-01T00:04:00.000000Z -> 4
                        """);
                try (Path dir = checkpointsDir(longView); Path path = new Path()) {
                    LiveViewCheckpointLayout.timelinePath(path, dir);
                    Assert.assertTrue("the long view's checkpoint paths outgrow a Path", path.size() > 255);
                }

                Assert.assertFalse(
                        "a truncate below every root retires the timeline",
                        job.truncateOrRetireTimelineOnO3ForTest(longView, ts("2026-01-01T00:00:00.000000Z"))
                );
                Assert.assertFalse("the check finds _timeline gone", longView.isCheckpointTimelineRetirePending());
                assertCheckpointFilesRetired(longView);
                commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'a', 5.0)");
                Assert.assertEquals("the next seal opens a fresh history", 1, generation(longView));
                assertCheckpointLadder(longView, false, "2026-01-01T00:05:00.000000Z -> 5\n");

                assertRetiresWithoutAllocating(job, fault, longView);
                assertCheckpointFilesRetired(longView);

                Assert.assertFalse(
                        "a truncate below every root of the short view retires its timeline",
                        job.truncateOrRetireTimelineOnO3ForTest(shortView, ts("2026-01-01T00:00:00.000000Z"))
                );
                Assert.assertFalse(shortView.isCheckpointTimelineRetirePending());
                assertCheckpointFilesRetired(shortView);
            }
        });
    }

    @Test
    public void testRetireWhoseRemovalThrowsAfterTheTimelineWentChecksTheTimelineAfresh() throws Exception {
        // No lock: the head-miss rebuild's retire removes _timeline, and the removal of the meta
        // directory after it throws, which ends the retire's removals. The check after it builds
        // the path of _timeline afresh and finds the file gone, so the view refuses nothing, and the
        // closing seal opens a fresh history over the directories the retire left.
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    final LiveViewInstance instance = commitFourRoots(job, false);
                    final long sealFailuresBefore = instance.getCheckpointSealFailures();
                    fault.throwAtTheNextMetaDirRemoval();
                    commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'a', 100.0)");
                    Assert.assertEquals("the retire's removal of the meta directory throws", 1, fault.getThrownMetaDirRemovals());
                    capture.drain();
                    Assert.assertEquals(1, capture.countOccurrences("could not retire live view checkpoint timeline [view=lv"));
                    capture.assertNotLogged(KEPT_TIMELINE_CHECK_FAILED);
                    capture.assertNotLogged(KEPT_TIMELINE_NEWLY_KEPT);
                    Assert.assertFalse("the check finds _timeline gone", instance.isCheckpointTimelineRetirePending());
                    assertNoRepairMarker(instance);
                    Assert.assertEquals("the closing seal opens a fresh history", 1, generation(instance));
                    assertCheckpointLadder(instance, false, "2026-01-01T00:04:00.000000Z -> 4\n");
                    Assert.assertEquals("which lands", sealFailuresBefore, instance.getCheckpointSealFailures());
                }
                assertViewRows(CHANGE_BELOW_EVERY_ROOT_ROWS, DEPARTURE_ROWS_QUERY);
                assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testRestartAfterARetireThatKeptTheTimelineWithoutItsMarkerReplaysTheCorrection() throws Exception {
        // Every marker write of the change's turn fails, the retire's and the closing seal's retry's,
        // so nothing durable refuses the kept generation: a restart before any later retire restores
        // it. Its replay meets the change's commit out of order and hands it to the repair, which
        // rebuilds the view around it and retires the kept generation on its way. With one
        // correction in the replay's gap, the view comes back as a recompute leaves it.
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = commitFourRoots(job, false);
                final long sealFailuresBefore = instance.getCheckpointSealFailures();
                fault.failMarkerWrites(-1);
                try {
                    changeBelowEveryRootUnderALock(job, fault);
                } finally {
                    fault.failMarkerWrites(0);
                    fault.unlock();
                }
                Assert.assertEquals("the retire's marker write and the retry's", 2, fault.getRefusedMarkerWrites());
                Assert.assertTrue(instance.isCheckpointTimelineRetirePending());
                Assert.assertEquals("the closing seal is refused", sealFailuresBefore + 1, instance.getCheckpointSealFailures());
                assertTimelineOnDisk(instance, true);
                assertNoRepairMarker(instance);
            }
            assertViewRows(CHANGE_BELOW_EVERY_ROOT_ROWS, DEPARTURE_ROWS_QUERY);
            restartAndRefresh();
            final LiveViewInstance restarted = viewInstance();
            Assert.assertEquals(
                    "timeline_restore",
                    LiveViewCheckpointRecoveryPhase.routeName(restarted.getCheckpointRestoreRoute())
            );
            Assert.assertEquals("the repair the replay handed the change to retires the kept generation", 1, restarted.getCheckpointTimelineResets());
            assertViewRows(CHANGE_BELOW_EVERY_ROOT_ROWS, DEPARTURE_ROWS_QUERY);
            assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
        });
    }

    @Test
    public void testCrashAfterARetireThatCannotCheckTheTimelineRebuildsOnRestart() throws Exception {
        // As testCrashAfterARetireThatKeptTheTimelineRebuildsOnRestart, with the retire's check of
        // _timeline after the unlink failing: the retire writes the marker all the same.
        assertCrashAfterARetireThatKeptTheTimeline(false, true);
    }

    @Test
    public void testCrashAfterARetireThatCannotCheckTheTimelineStopsTheRestartOverTheDayTheBaseLost() throws Exception {
        assertCrashAfterARetireThatKeptTheTimeline(true, true);
    }

    @Test
    public void testCrashAfterARetireThatKeptTheTimelineRebuildsOnRestart() throws Exception {
        // A crash at the change's replacement commit, after the retire kept _timeline and wrote its
        // marker: the view's table still holds the rows from before the change, and the marker sends
        // the restart to the rebuild from the applied base rather than to the kept generation.
        assertCrashAfterARetireThatKeptTheTimeline(false, false);
    }

    @Test
    public void testCrashAfterARetireThatKeptTheTimelineStopsTheRestartOverTheDayTheBaseLost() throws Exception {
        // As above, over a row of the day the base lost: the restatement guard refuses the rebuild,
        // and the view stops with every row it held.
        assertCrashAfterARetireThatKeptTheTimeline(true, false);
    }

    @Test
    public void testDropOfAViewWhoseRetireCannotCheckTheTimeline() throws Exception {
        assertDropOfAViewWhoseRetireKeptTheTimeline(true);
    }

    @Test
    public void testDropOfAViewWhoseRetireKeptTheTimeline() throws Exception {
        assertDropOfAViewWhoseRetireKeptTheTimeline(false);
    }

    @Test
    public void testWalPurgeBetweenAKeptRetireAndTheRestartRebuilds() throws Exception {
        // The retire released both base WAL arms the view held, as every retire does, so a purge
        // after it removes the base WAL the kept generation would replay from. The restart meets
        // the marker the retire wrote and rebuilds from the applied base, which reads no base WAL,
        // without trying a restore whose replay the purge removed.
        setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 1);
        setProperty(PropertyKey.CAIRO_WAL_PURGE_INTERVAL, 0);
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    final LiveViewInstance instance = commitFourRoots(job, false);
                    final long generationBefore = superblockGeneration(instance);
                    try {
                        changeBelowEveryRootUnderALock(job, fault);
                    } finally {
                        fault.unlock();
                    }
                    assertTimelineRetirePending(instance, generationBefore, Numbers.LONG_NULL);
                }
                releaseSavePointHolds("base");
                engine.releaseInactive();
                setCurrentMicros(currentMicros + 60_000_000L);
                try (WalPurgeJob purgeJob = new WalPurgeJob(engine)) {
                    purgeJob.drain(0);
                }
                Assert.assertEquals("the purge removes every base WAL segment the view consumed", 0, countBaseWalSegments());
                restartAndRefresh();
                capture.drain();
                capture.assertLogged("live view restart rebuilding from applied base [view=lv, cause=prefix preservation repair marker present");
                capture.assertNotLogged("could not restore live view from checkpoint timeline");
                assertRebuiltFromAppliedBase("lv");
                assertViewRows(CHANGE_BELOW_EVERY_ROOT_ROWS, DEPARTURE_ROWS_QUERY);
                assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testTruncateOverAKeptTimelinePublishesNothing() throws Exception {
        // A truncate that meets the kept generation is refused at the writer, ahead of the marker it
        // would write over the retire's: nothing publishes, and its own failure path retires once
        // more, which meets the lock too.
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    final LiveViewInstance instance = commitFourRoots(job, false);
                    final long generationBefore = superblockGeneration(instance);
                    changeBelowEveryRootUnderALock(job, fault);
                    final long resetsBefore = instance.getCheckpointTimelineResets();
                    final int markerRenamesBefore = fault.getMarkerRenames();
                    capture.drain();
                    Assert.assertFalse(
                            "the truncate retires instead of keeping the roots below 00:03",
                            job.truncateOrRetireTimelineOnO3ForTest(instance, ts("2026-01-01T00:03:00.000000Z"))
                    );
                    Assert.assertEquals("its retire", resetsBefore + 1, instance.getCheckpointTimelineResets());
                    Assert.assertEquals("meets the lock", 3, fault.getRefusedTimelineUnlinks());
                    Assert.assertEquals("no truncate marker replaces the retire's", markerRenamesBefore, fault.getMarkerRenames());
                    assertTimelineRetirePending(instance, generationBefore, Numbers.LONG_NULL);
                    capture.drain();
                    capture.assertLogged("could not preserve live view checkpoint timeline prefix, retiring [view=lv");
                    capture.assertLogged(KEPT_TIMELINE_REFUSAL);
                    fault.unlock();
                }
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testKeptTimelineMarkerThatOutlivesItsTimelineOpensNoHistoryBesideIt() throws Exception {
        // The change below every root keeps _timeline under a lock, with the marker naming no base
        // generation beside it, over a base that lost a day the view keeps. The lock then moves from
        // _timeline to the marker: the next seal's retry of the retire removes _timeline but not the
        // marker, and the seal opens no history beside the marker, which would read as live against
        // it. Once the marker can go, the next seal removes it and opens the history: a base column
        // added in place restores it, and so do two restarts, instead of stopping at the rebuild
        // the restatement guard refuses over the day the base lost.
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView();
                // s sums x over the row and the two rows of 'a' below it, by hand: 5.0 + 4.0 + 3.0 at
                // 00:05, then 6.0 + 5.0 + 4.0.
                final String rows = CHANGE_BELOW_EVERY_ROOT_OVER_THE_LOST_DAY_ROWS + CHANGE_BELOW_EVERY_ROOT_ROW_0005
                        + "2026-01-01T00:06:00.000000Z\ta\t6.0\t15.0\n";
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    final LiveViewInstance instance = commitFourRootsOverTheDayTheBaseLoses(job);
                    changeBelowEveryRootUnderALock(job, fault);
                    assertMarkerNames(Numbers.LONG_NULL);
                    final long sealFailuresBefore = instance.getCheckpointSealFailures();
                    fault.unlock();
                    fault.lockMarker(true);
                    commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'a', 5.0)");
                    Assert.assertFalse("the seal's retry of the retire removed _timeline", instance.isCheckpointTimelineRetirePending());
                    Assert.assertEquals("the retry and the seal's append each meet the marker's lock", 2, fault.getRefusedMarkerUnlinks());
                    Assert.assertEquals("the seal is refused", sealFailuresBefore + 1, instance.getCheckpointSealFailures());
                    assertNoHistory(instance);
                    assertMarkerNames(Numbers.LONG_NULL);
                    capture.drain();
                    capture.assertLoggedRE(Pattern.quote(HEAD_SEAL_FAILED) + "[^\\n]*" + Pattern.quote(LEFTOVER_MARKER_REFUSAL));

                    fault.lockMarker(false);
                    commitDeparture(job, "('2026-01-01T00:06:00.000000Z', 'a', 6.0)");
                    Assert.assertEquals("the next seal opens the history", 1, generation(instance));
                    assertCheckpointLadder(instance, false, "2026-01-01T00:06:00.000000Z -> 7\n");
                    assertNoRepairMarker(instance);
                    Assert.assertEquals("and ends the streak", 0, instance.getSealFailureStreak());

                    final long runtimeRestoresBefore = instance.getCheckpointRuntimeRestores();
                    execute("ALTER TABLE base ADD COLUMN y INT");
                    drainWalQueue();
                    driveRefreshToQuiescence(job);
                    Assert.assertEquals("the base column restores the history in place", runtimeRestoresBefore + 1, instance.getCheckpointRuntimeRestores());
                    Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
                }
                assertViewRows(rows, DEPARTURE_ROWS_QUERY);
                for (int restart = 1; restart <= 2; restart++) {
                    restartAndRefresh();
                    assertRestoredFromTimeline("lv");
                    assertViewRows(rows, DEPARTURE_ROWS_QUERY);
                }
                capture.drain();
                capture.assertNotLogged("prefix preservation repair marker present");
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testKeptTimelineMarkerWhoseUnlinkGoesThroughOnTheRetryOpensTheHistoryAtOnce() throws Exception {
        // As above, with the marker refusing only the unlink of the seal's retry of the retire: the
        // seal's own removal goes through, and the seal opens the history at once, without the
        // marker, which the restart restores over the day the base lost.
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            final String rows = CHANGE_BELOW_EVERY_ROOT_OVER_THE_LOST_DAY_ROWS + CHANGE_BELOW_EVERY_ROOT_ROW_0005;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = commitFourRootsOverTheDayTheBaseLoses(job);
                changeBelowEveryRootUnderALock(job, fault);
                final long sealFailuresBefore = instance.getCheckpointSealFailures();
                fault.unlock();
                fault.refuseTheNextMarkerUnlinks(1);
                commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'a', 5.0)");
                Assert.assertFalse(instance.isCheckpointTimelineRetirePending());
                Assert.assertEquals("the retry of the retire meets the refusal", 1, fault.getRefusedMarkerUnlinks());
                Assert.assertEquals("the seal lands", sealFailuresBefore, instance.getCheckpointSealFailures());
                Assert.assertEquals("on the history it opens", 1, generation(instance));
                assertCheckpointLadder(instance, false, "2026-01-01T00:05:00.000000Z -> 6\n");
                assertNoRepairMarker(instance);
            }
            assertViewRows(rows, DEPARTURE_ROWS_QUERY);
            restartAndRefresh();
            assertRestoredFromTimeline("lv");
            assertViewRows(rows, DEPARTURE_ROWS_QUERY);
        });
    }

    @Test
    public void testRetireOverALiveTruncateMarkerThatKeepsTheMarkerOpensNoHistoryBesideIt() throws Exception {
        // A truncate's live marker names the generation it truncated. The head-miss rebuild that
        // follows retires the timeline under a lock on the marker alone: _timeline goes and the
        // marker stays, where it would read as live against the first generations of a fresh
        // history. The rebuild's closing seal opens no history beside it. Once the marker can go,
        // the next seal removes it and opens the history, which a restart restores instead of
        // rebuilding.
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            final String rows = CHANGE_BELOW_EVERY_ROOT_ROWS + CHANGE_BELOW_EVERY_ROOT_ROW_0005;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = commitFourRoots(job, false);
                final long sealFailuresBefore = instance.getCheckpointSealFailures();
                final long markerGeneration = retireOverALiveTruncateMarkerThatKeepsTheMarker(job, fault);
                Assert.assertEquals("the retire and the closing seal each meet the marker's lock", 2, fault.getRefusedMarkerUnlinks());
                Assert.assertEquals("the closing seal is refused", sealFailuresBefore + 1, instance.getCheckpointSealFailures());
                Assert.assertFalse("the retire removed _timeline", instance.isCheckpointTimelineRetirePending());
                assertNoHistory(instance);
                assertMarkerNames(markerGeneration);

                fault.lockMarker(false);
                commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'a', 5.0)");
                Assert.assertEquals("the next seal opens the history", 1, generation(instance));
                assertCheckpointLadder(instance, false, "2026-01-01T00:05:00.000000Z -> 5\n");
                assertNoRepairMarker(instance);
            }
            assertViewRows(rows, DEPARTURE_ROWS_QUERY);
            restartAndRefresh();
            assertRestoredFromTimeline("lv");
            assertViewRows(rows, DEPARTURE_ROWS_QUERY);
            assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
        });
    }

    @Test
    public void testCrashAtTheHistoryOpeningMarkerRemovalLeavesNoGeneration() throws Exception {
        // As above, with the marker refusing the retire's unlink alone, and the process stopped right
        // before the closing seal's unlink of it: the image holds the marker beside no generation, as
        // an image taken anywhere ahead of the publication does. The restart reads the marker as live
        // and rebuilds from the applied base, which the intact base allows, and the rebuild's closing
        // seal opens a history without the marker, which the next restart restores.
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView();
                final long markerGeneration;
                final AtomicBoolean isImageTaken = new AtomicBoolean();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    final LiveViewInstance instance = commitFourRoots(job, false);
                    markerGeneration = superblockGeneration(instance);
                    Assert.assertTrue(job.truncateOrRetireTimelineOnO3ForTest(instance, ts("2026-01-01T00:03:00.000000Z")));
                    try (Path checkpoints = checkpointsDir(instance).slash(); Path crashImage = crashImageDir()) {
                        fault.refuseTheNextMarkerUnlinks(1);
                        fault.beforeTheNextMarkerUnlink(() -> {
                            TestUtils.copyDirectory(checkpoints, crashImage, configuration.getMkDirMode());
                            isImageTaken.set(true);
                        });
                        commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'a', 100.0)");
                    }
                    Assert.assertTrue("the crash image must be taken", isImageTaken.get());
                    Assert.assertEquals("the retire meets the refusal", 1, fault.getRefusedMarkerUnlinks());
                    Assert.assertEquals("the closing seal removes the marker and opens the history", 1, generation(instance));
                    assertNoRepairMarker(instance);
                }
                try (
                        Path crashImage = crashImageDir();
                        LiveViewCheckpointSuperblock superblock = new LiveViewCheckpointSuperblock(configuration)
                ) {
                    superblock.of(crashImage);
                    Assert.assertFalse("the image holds no generation", superblock.isValid());
                    Assert.assertEquals("no slot passes its checksum", Long.MAX_VALUE, superblock.getOldestValidGeneration());
                    Assert.assertEquals(
                            "and the marker the retire kept",
                            markerGeneration,
                            LiveViewCheckpointRepairMarker.readBaseGeneration(configuration, crashImage)
                    );
                }
                putTheCrashImageBack();
                restartAndRefresh();
                capture.drain();
                capture.assertLogged("live view restart rebuilding from applied base [view=lv, cause=prefix preservation repair marker present");
                assertRebuiltFromAppliedBase("lv");
                final LiveViewInstance restarted = viewInstance();
                Assert.assertEquals("the rebuild's closing seal opens the history", 1, generation(restarted));
                assertNoRepairMarker(restarted);
                assertViewRows(CHANGE_BELOW_EVERY_ROOT_ROWS, DEPARTURE_ROWS_QUERY);
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'a', 5.0)");
                }
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(CHANGE_BELOW_EVERY_ROOT_ROWS + CHANGE_BELOW_EVERY_ROOT_ROW_0005, DEPARTURE_ROWS_QUERY);
                assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testRestartWhileTheMarkerStaysLockedRebuildsUntilTheLockGoes() throws Exception {
        // A restart while the marker's lock holds meets the marker beside no generation and rebuilds
        // from the applied base. The rebuild's retire cannot remove the marker either, so its closing
        // seal opens no history. Once the lock goes, the next seal opens one, which the next restart
        // restores.
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView();
                final long markerGeneration;
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    commitFourRoots(job, false);
                    markerGeneration = retireOverALiveTruncateMarkerThatKeepsTheMarker(job, fault);
                }
                restartAndRefresh();
                capture.drain();
                capture.assertLogged("live view restart rebuilding from applied base [view=lv, cause=prefix preservation repair marker present");
                assertRebuiltFromAppliedBase("lv");
                final LiveViewInstance restarted = viewInstance();
                Assert.assertEquals("the rebuild's closing seal is refused", 1, restarted.getCheckpointSealFailures());
                assertNoHistory(restarted);
                assertMarkerNames(markerGeneration);
                assertViewRows(CHANGE_BELOW_EVERY_ROOT_ROWS, DEPARTURE_ROWS_QUERY);

                fault.lockMarker(false);
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'a', 5.0)");
                }
                Assert.assertEquals("the next seal opens the history", 1, generation(restarted));
                assertNoRepairMarker(restarted);
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(CHANGE_BELOW_EVERY_ROOT_ROWS + CHANGE_BELOW_EVERY_ROOT_ROW_0005, DEPARTURE_ROWS_QUERY);
                assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testRepairAfterTheHistoryOpensRecordsItsSeqTxn() throws Exception {
        // The marker the retire that kept _timeline wrote outlives _timeline under a lock, and the
        // history opens once the marker goes. The late row 00:07:30 then resumes from the root at
        // 00:07, and the marker its repair writes ahead of its commit records the view's last
        // committed seqTxn: no marker an older history left stands beside the history to make it
        // record none.
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = commitFourRoots(job, false);
                changeBelowEveryRootUnderALock(job, fault);
                fault.unlock();
                fault.lockMarker(true);
                commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'a', 5.0)");
                fault.lockMarker(false);
                for (int minute = 6; minute <= 8; minute++) {
                    commitDeparture(job, "('2026-01-01T00:0" + minute + ":00.000000Z', 'a', " + minute + ".0)");
                }
                // The seqTxn the marker records, then the view sequencer's last txn, both read right
                // after the marker lands, ahead of the repair's commit.
                final LongList seqTxns = new LongList();
                fault.afterTheNextMarker(() -> {
                    try (Path dir = checkpointsDir(instance)) {
                        seqTxns.add(LiveViewCheckpointRepairMarker.readLvSeqTxn(configuration, dir));
                    }
                    seqTxns.add(engine.getTableSequencerAPI().lastTxn(instance.getLiveViewToken()));
                });
                final long resumesBefore = job.getStatsForTest().getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RESUME_FROM_ANCHOR);
                commitDeparture(job, "('2026-01-01T00:07:30.000000Z', 'a', 0.5)");
                Assert.assertEquals(
                        "the late row resumes from the root at 00:07",
                        resumesBefore + 1,
                        job.getStatsForTest().getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RESUME_FROM_ANCHOR)
                );
                Assert.assertEquals("the repair writes its marker", 2, seqTxns.size());
                Assert.assertNotEquals("the marker records a seqTxn", Numbers.LONG_NULL, seqTxns.getQuick(0));
                Assert.assertEquals("the view's last committed one", seqTxns.getQuick(1), seqTxns.getQuick(0));
            }
            // s sums x over the row and the two rows of 'a' below it, by hand.
            assertViewRows(
                    CHANGE_BELOW_EVERY_ROOT_ROWS + CHANGE_BELOW_EVERY_ROOT_ROW_0005
                            + "2026-01-01T00:06:00.000000Z\ta\t6.0\t15.0\n"
                            + "2026-01-01T00:07:00.000000Z\ta\t7.0\t18.0\n"
                            + "2026-01-01T00:07:30.000000Z\ta\t0.5\t13.5\n"
                            + "2026-01-01T00:08:00.000000Z\ta\t8.0\t15.5\n",
                    DEPARTURE_ROWS_QUERY
            );
            assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
        });
    }

    @Test
    public void testSealsRefusedOverALockedMarkerBackOffOnTheSealFailureBudget() throws Exception {
        // The marker the retire that kept _timeline wrote outlives _timeline under a lock that holds:
        // every seal that would open the history is refused, which spends the seal failure budget as
        // any failed seal does. The third refusal pauses the seal a minute, the first after the pause
        // retires once more, which meets the lock too, and pauses it two, and the first after that
        // four. No seal runs inside a pause, so the unlinks the lock refuses follow that schedule; no
        // generation is published, and no base WAL arm stands. Once the lock goes, the first seal
        // past the pause opens the history and ends the streak.
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    final LiveViewInstance instance = commitFourRoots(job, false);
                    final long sealFailuresBefore = instance.getCheckpointSealFailures();
                    final long resetsBefore = instance.getCheckpointTimelineResets();
                    changeBelowEveryRootUnderALock(job, fault);
                    fault.unlock();
                    fault.lockMarker(true);
                    assertMarkerSealRetries(instance, sealFailuresBefore + 1, fault, 0, resetsBefore + 1);

                    // The seal's retry of the retire, then its own removal.
                    commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'a', 1.0)");
                    assertMarkerSealRetries(instance, sealFailuresBefore + 2, fault, 2, resetsBefore + 1);
                    commitDeparture(job, "('2026-01-01T00:06:00.000000Z', 'a', 1.0)");
                    assertMarkerSealRetries(instance, sealFailuresBefore + 3, fault, 3, resetsBefore + 1);
                    Assert.assertTrue("the third refusal pauses the seal a minute", instance.isSealOnCooldown(currentMicros + 59_000_000L));
                    Assert.assertFalse(instance.isSealOnCooldown(currentMicros + 61_000_000L));

                    setCurrentMicros(currentMicros + 30_000_000L);
                    commitDeparture(job, "('2026-01-01T00:07:00.000000Z', 'a', 1.0)");
                    assertMarkerSealRetries(instance, sealFailuresBefore + 3, fault, 3, resetsBefore + 1);

                    setCurrentMicros(currentMicros + 31_000_000L);
                    commitDeparture(job, "('2026-01-01T00:08:00.000000Z', 'a', 1.0)");
                    // The seal's removal, then the retire the failure after the pause runs.
                    assertMarkerSealRetries(instance, sealFailuresBefore + 4, fault, 5, resetsBefore + 2);
                    Assert.assertTrue("which pauses the seal two minutes", instance.isSealOnCooldown(currentMicros + 119_000_000L));
                    Assert.assertFalse(instance.isSealOnCooldown(currentMicros + 121_000_000L));

                    setCurrentMicros(currentMicros + 60_000_000L);
                    commitDeparture(job, "('2026-01-01T00:09:00.000000Z', 'a', 1.0)");
                    assertMarkerSealRetries(instance, sealFailuresBefore + 4, fault, 5, resetsBefore + 2);

                    setCurrentMicros(currentMicros + 61_000_000L);
                    commitDeparture(job, "('2026-01-01T00:10:00.000000Z', 'a', 1.0)");
                    assertMarkerSealRetries(instance, sealFailuresBefore + 5, fault, 7, resetsBefore + 3);
                    Assert.assertTrue("and the next one four", instance.isSealOnCooldown(currentMicros + 239_000_000L));
                    Assert.assertFalse(instance.isSealOnCooldown(currentMicros + 241_000_000L));
                    assertNoHistory(instance);
                    assertMarkerNames(Numbers.LONG_NULL);
                    Assert.assertEquals("no head arm stands", Numbers.LONG_NULL, instance.getHeadCheckpointBaseSeqTxn());
                    Assert.assertEquals("nor a timeline arm", Numbers.LONG_NULL, instance.getCheckpointTimelineWalPurgeFloor());
                    capture.drain();
                    capture.assertLogged("live view checkpoint seal keeps failing, keeping the timeline and backing off "
                            + "[view=lv, consecutiveFailures=3, backoffMicros=60000000]");
                    capture.assertLogged("live view checkpoint seal keeps failing, retiring the timeline and backing off "
                            + "[view=lv, consecutiveFailures=4, backoffMicros=120000000]");
                    capture.assertLogged("live view checkpoint seal keeps failing, retiring the timeline and backing off "
                            + "[view=lv, consecutiveFailures=5, backoffMicros=240000000]");
                    assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);

                    fault.lockMarker(false);
                    setCurrentMicros(currentMicros + 241_000_000L);
                    commitDeparture(job, "('2026-01-01T00:11:00.000000Z', 'a', 1.0)");
                    Assert.assertEquals("the seal past the pause opens the history", 1, generation(instance));
                    assertCheckpointLadder(instance, false, "2026-01-01T00:11:00.000000Z -> 11\n");
                    assertNoRepairMarker(instance);
                    Assert.assertEquals("and ends the streak", 0, instance.getSealFailureStreak());
                    Assert.assertFalse(instance.isSealOnCooldown(currentMicros));
                }
                assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testHistoryOpeningOverALockedMarkerIsRefusedOnEveryWorker() throws Exception {
        // Two refresh workers, each with a timeline writer of its own. The first runs the change
        // below every root, whose retire keeps _timeline and the marker beside it, and the lock then
        // moves to the marker. Each worker's seal is refused while the marker stays: what refuses it
        // is on disk, not in either worker. Once the marker can go, past the pause the third refusal
        // armed, the second worker's next seal opens the history.
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (
                    LiveViewRefreshJob first = new LiveViewRefreshJob(0, 2, engine, 1);
                    LiveViewRefreshJob second = new LiveViewRefreshJob(1, 2, engine, 1)
            ) {
                final LiveViewInstance instance = commitFourRoots(first, false);
                final long sealFailuresBefore = instance.getCheckpointSealFailures();
                changeBelowEveryRootUnderALock(first, fault);
                fault.unlock();
                fault.lockMarker(true);
                commitDeparture(second, "('2026-01-01T00:05:00.000000Z', 'a', 5.0)");
                Assert.assertEquals("the second worker's seal is refused", sealFailuresBefore + 2, instance.getCheckpointSealFailures());
                Assert.assertEquals("its retry of the retire and its removal meet the marker's lock", 2, fault.getRefusedMarkerUnlinks());
                commitDeparture(first, "('2026-01-01T00:06:00.000000Z', 'a', 6.0)");
                Assert.assertEquals("so is the first worker's", sealFailuresBefore + 3, instance.getCheckpointSealFailures());
                Assert.assertEquals("whose removal meets the lock too", 3, fault.getRefusedMarkerUnlinks());
                Assert.assertTrue("the third refusal pauses the seal", instance.isSealOnCooldown(currentMicros));
                assertNoHistory(instance);
                assertMarkerNames(Numbers.LONG_NULL);

                fault.lockMarker(false);
                setCurrentMicros(currentMicros + 61_000_000L);
                commitDeparture(second, "('2026-01-01T00:07:00.000000Z', 'a', 7.0)");
                Assert.assertEquals("the second worker's next seal opens the history", 1, generation(instance));
                assertCheckpointLadder(instance, false, "2026-01-01T00:07:00.000000Z -> 7\n");
                assertNoRepairMarker(instance);
            }
            assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
        });
    }

    @Test
    public void testDepartureThatEmptiesTheViewBesideALeftoverMarkerSealsNothing() throws Exception {
        // The truncate's marker stands beside no generation under a lock, and an upsert then moves
        // every row out of the filter: a view with no row seals no boundary, so no seal is refused.
        // Once the lock goes, the forward row's seal removes the marker and opens the history.
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = commitFourRoots(job, false);
                final long markerGeneration = retireOverALiveTruncateMarkerThatKeepsTheMarker(job, fault);
                final long sealFailuresBefore = instance.getCheckpointSealFailures();
                commitDeparture(
                        job,
                        "('2026-01-01T00:01:00.000000Z', 'a', -1.0), ('2026-01-01T00:02:00.000000Z', 'a', -1.0), "
                                + "('2026-01-01T00:03:00.000000Z', 'a', -1.0), ('2026-01-01T00:04:00.000000Z', 'a', -1.0)"
                );
                assertViewRows("ts\tsym\tx\ts\n", DEPARTURE_ROWS_QUERY);
                Assert.assertEquals("no seal is attempted over the empty view", sealFailuresBefore, instance.getCheckpointSealFailures());
                assertNoHistory(instance);
                assertMarkerNames(markerGeneration);

                fault.lockMarker(false);
                commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'a', 5.0)");
                Assert.assertEquals("the forward row's seal opens the history", 1, generation(instance));
                assertCheckpointLadder(instance, false, "2026-01-01T00:05:00.000000Z -> 1\n");
                assertNoRepairMarker(instance);
            }
            assertViewRows("ts\tsym\tx\ts\n2026-01-01T00:05:00.000000Z\ta\t5.0\t5.0\n", DEPARTURE_ROWS_QUERY);
            assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
        });
    }

    @Test
    public void testOnlyHistoryOpeningsProbeTheRepairMarker() throws Exception {
        // The marker removal costs a check of each marker name where a history opens, and nothing on
        // any other seal: the first seed seal and the seed completion's seal each open one, beside
        // the retire ahead of each, which checks both names as it always has, and no later seed seal
        // or cadence seal looks for a marker. With no marker on disk, nothing unlinks one.
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        assertMemoryLeak(fault, () -> {
            createSeedingView(SEED_ROWS - 2, 2);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                int checksBefore = fault.getMarkerExistsChecks();
                final LiveViewInstance instance = runSeedTurns(job, 1);
                Assert.assertEquals("the resume's retire, then the seed history's opening", checksBefore + 4, fault.getMarkerExistsChecks());
                Assert.assertEquals("the first seed seal opens the seed history", 1, generation(instance));

                checksBefore = fault.getMarkerExistsChecks();
                runSeedTurns(job, SEED_ROWS - 1);
                Assert.assertEquals("no later seed seal checks", checksBefore, fault.getMarkerExistsChecks());
                Assert.assertEquals("each turn sealed a seed root", SEED_ROWS, instance.getSeedCheckpointDataOffset());

                job.run();
                drainWalQueue();
                Assert.assertEquals(LiveViewState.SEED_STATE_ACTIVE, instance.getStateReader().getSeedState());
                Assert.assertEquals("the completion's retire, then the runtime history's opening", checksBefore + 4, fault.getMarkerExistsChecks());
                Assert.assertEquals("the completion seal opens the runtime history", 1, generation(instance));

                checksBefore = fault.getMarkerExistsChecks();
                for (int minute = 3; minute <= 22; minute++) {
                    commitDeparture(job, "('2026-01-02T00:" + (minute < 10 ? "0" : "") + minute + ":00.000000Z', 'a', " + minute + ".0)");
                }
                Assert.assertEquals("each cadence seal sealed its row", ts("2026-01-02T00:22:00.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals("no cadence seal checks", checksBefore, fault.getMarkerExistsChecks());
                Assert.assertEquals("nothing unlinks a marker", 0, fault.getMarkerUnlinks());
            }
            assertViewMatchesRecompute(SEED_VIEW_SQL);
        });
    }

    @Test
    public void testSeedSweepOpensItsHistoryWithoutTheMarkerItsResumeRetireCouldNotRemove() throws Exception {
        // A marker naming no base generation, as the retire that keeps a timeline writes it, outlives
        // the retire of the first seed turn's resume, whose unlink of it is refused once. The turn's
        // seed seal opens the seed history and removes the marker first, so no marker stands beside
        // the sweep's roots, nor beside the history its completion opens, which a restart restores.
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        assertMemoryLeak(fault, () -> {
            createSeedingView(SEED_ROWS - 2, 2);
            stampKeptBranchMarker(viewInstance());
            fault.refuseTheNextMarkerUnlinks(1);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = runSeedTurns(job, 1);
                Assert.assertEquals("the resume's retire meets the refusal", 1, fault.getRefusedMarkerUnlinks());
                Assert.assertEquals("the seed seal opens the seed history", 1, generation(instance));
                Assert.assertEquals("at the row the turn fed", 1, seedCursorOffset(instance));
                assertNoRepairMarker(instance);
                driveSeedToCompletion(job, "lv");
            }
            Assert.assertEquals(LiveViewState.SEED_STATE_ACTIVE, viewInstance().getStateReader().getSeedState());
            assertNoRepairMarker(viewInstance());
            assertViewMatchesRecompute(SEED_VIEW_SQL);
            restartAndRefresh();
            assertRestoredFromTimeline("lv");
            assertViewMatchesRecompute(SEED_VIEW_SQL);
        });
    }

    @Test
    public void testSeedSweepKeepsItsResumePointsBesideAMarkerItCannotRemove() throws Exception {
        // The restarted sweep cannot read its torn seed root, and the retire that abandons the resume
        // keeps _timeline under a lock, with the marker naming no base generation beside it. The lock
        // then moves to the marker. The next seed seal's retry of the retire removes _timeline, its
        // removal of the marker is refused, and the seed history opens all the same: no reader of a
        // seed history reads the marker. Its roots keep landing, and a restart while the lock holds
        // resumes from the newest of them. The completion's seal opens the runtime history, which a
        // restart reads the marker against, so it is refused while the marker stays, and the forward
        // row's seal opens it once the marker can go.
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createSeedingView(SEED_ROWS - 2, 2);
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    Assert.assertEquals("each turn seals a seed root", 3, runSeedTurns(job, 3).getSeedCheckpointDataOffset());
                }
                engine.getLiveViewRegistry().clear();
                engine.buildViewGraphs();
                final LiveViewInstance instance = viewInstance();
                corruptNewestRootDataSegment(instance);
                fault.lock(false);
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    runSeedTurns(job, 1);
                    Assert.assertEquals("the abandon's two retires, then the turn's seal", 3, fault.getRefusedTimelineUnlinks());
                    Assert.assertTrue(instance.isCheckpointTimelineRetirePending());
                    assertMarkerNames(Numbers.LONG_NULL);
                    capture.drain();
                    Assert.assertEquals("the turn's seed seal is refused", 1, capture.countOccurrences(SEED_SEAL_FAILED));

                    fault.unlock();
                    fault.lockMarker(true);
                    runSeedTurns(job, 1);
                    Assert.assertFalse("the seed seal's retry of the retire removed _timeline", instance.isCheckpointTimelineRetirePending());
                    Assert.assertEquals("the retry and the seed history's opening each meet the marker's lock", 2, fault.getRefusedMarkerUnlinks());
                    Assert.assertEquals("the seed history opens all the same", 1, generation(instance));
                    Assert.assertEquals("at the second row of the re-sweep", 2, seedCursorOffset(instance));
                    for (int turn = 2; turn <= 3; turn++) {
                        runSeedTurns(job, 1);
                        Assert.assertEquals(turn, generation(instance));
                        Assert.assertEquals(turn + 1, seedCursorOffset(instance));
                    }
                    Assert.assertEquals("no later seed seal tries the removal", 2, fault.getRefusedMarkerUnlinks());
                    assertMarkerNames(Numbers.LONG_NULL);
                    capture.drain();
                    Assert.assertEquals("no seed seal fails over the marker", 1, capture.countOccurrences(SEED_SEAL_FAILED));
                }
                engine.getLiveViewRegistry().clear();
                engine.buildViewGraphs();
                final LiveViewInstance restarted = viewInstance();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    runSeedTurns(job, 1);
                    Assert.assertEquals("the restarted sweep resumes from the root at 4", 5, restarted.getSeedDataOffset());
                    final long sealFailuresBefore = restarted.getCheckpointSealFailures();
                    driveSeedToCompletion(job, "lv");
                    Assert.assertEquals(LiveViewState.SEED_STATE_ACTIVE, restarted.getStateReader().getSeedState());
                    Assert.assertEquals("the completion seal is refused", sealFailuresBefore + 1, restarted.getCheckpointSealFailures());
                    capture.drain();
                    capture.assertLoggedRE(Pattern.quote(HEAD_SEAL_FAILED) + "[^\\n]*" + Pattern.quote(LEFTOVER_MARKER_REFUSAL));
                    assertNoHistory(restarted);
                    assertMarkerNames(Numbers.LONG_NULL);
                    assertViewMatchesRecompute(SEED_VIEW_SQL);

                    fault.lockMarker(false);
                    commitDeparture(job, "('2026-01-02T00:03:00.000000Z', 'a', 9.0)");
                    Assert.assertEquals("the forward row's seal opens the runtime history", 1, generation(restarted));
                    Assert.assertEquals("a steady root", Numbers.LONG_NULL, seedCursorOffset(restarted));
                    assertNoRepairMarker(restarted);
                }
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewMatchesRecompute(SEED_VIEW_SQL);
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testSeedRearmBesideAMarkerItCannotRemoveResumesFromTheNewestRoot() throws Exception {
        // A marker naming no base generation stands from CREATE on under a lock: the first seed
        // turn's resume retire cannot remove it, nor can the seed history's opening, and the seed
        // roots land beside it, two rows a turn. The turn that feeds the first day's last row and
        // then reads the second day faults on that read, which re-arms the sweep's resume in the same
        // process: it resumes from the newest seed root, which no marker keeps it off. The
        // completion's seal is refused while the marker stays, and the forward row's seal opens the
        // runtime history once it can go.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 2);
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createSeedingView(7, 2);
                stampKeptBranchMarker(viewInstance());
                fault.lockMarker(true);
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    final LiveViewInstance instance = runSeedTurns(job, 3);
                    Assert.assertEquals("a seed root per turn beside the marker", 3, generation(instance));
                    Assert.assertEquals(6, seedCursorOffset(instance));
                    Assert.assertEquals("the resume's retire and the seed history's opening", 2, fault.getRefusedMarkerUnlinks());
                    fault.failTheNextBaseColumnOpen(engine.verifyTableName("base").getDirName(), "2026-01-02");
                    for (int pass = 0; pass < REFRESH_QUIESCENCE_PASSES && fault.getRefusedBaseColumnOpens() == 0; pass++) {
                        advanceClockToNextRefreshPass();
                        runSeedTurns(job, 1);
                    }
                    Assert.assertEquals("the sweep's read of the second day faults", 1, fault.getRefusedBaseColumnOpens());
                    Assert.assertEquals("which is the view's one refresh fault", 1, instance.getRefreshFaultCount());
                    capture.drain();
                    capture.assertLogged("live view mid-seed refresh failure, sweep will resume [view=lv");
                    advanceClockToNextRefreshPass();
                    runSeedTurns(job, 1);
                    capture.drain();
                    capture.assertNotLogged(SEED_RESUME_ABANDONED);
                    Assert.assertEquals("the re-armed sweep resumes from the root at 6 and feeds two rows", 8, instance.getSeedDataOffset());
                    Assert.assertEquals(4, generation(instance));
                    Assert.assertEquals(8, seedCursorOffset(instance));
                    Assert.assertEquals("no later seed seal tries the removal", 2, fault.getRefusedMarkerUnlinks());

                    final long sealFailuresBefore = instance.getCheckpointSealFailures();
                    driveSeedToCompletion(job, "lv");
                    Assert.assertEquals(LiveViewState.SEED_STATE_ACTIVE, instance.getStateReader().getSeedState());
                    Assert.assertEquals("the completion seal is refused", sealFailuresBefore + 1, instance.getCheckpointSealFailures());
                    assertNoHistory(instance);
                    assertMarkerNames(Numbers.LONG_NULL);

                    fault.lockMarker(false);
                    commitDeparture(job, "('2026-01-02T00:03:00.000000Z', 'a', 10.0)");
                    Assert.assertEquals("the forward row's seal opens the runtime history", 1, generation(instance));
                    assertNoRepairMarker(instance);
                }
                // The injected fault is the view's one refresh fault, asserted above.
                TestUtils.assertSqlCursors(
                        engine,
                        sqlExecutionContext,
                        "(" + SEED_VIEW_SQL + ") ORDER BY 2, 1",
                        "(lv) ORDER BY 2, 1",
                        LOG,
                        true
                );
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testSeedThatEmitsNoRowsLeavesTheMarkerToTheFirstForwardSeal() throws Exception {
        // A seed that qualifies no row seals nothing, so a marker the seed's two retires could not
        // remove - its resume's and its completion's - stays beside no history once the view goes
        // ACTIVE. The first forward row's seal is the first runtime history opening: it removes the
        // marker and opens the history, which a restart restores.
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        assertMemoryLeak(fault, () -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final StringSink rows = new StringSink();
            for (int row = 1; row <= SEED_ROWS; row++) {
                rows.put(row > 1 ? ", " : "").put("('2026-01-01T00:0").put(row).put(":00.000000Z', 'a', ").put(row).put(".0)");
            }
            execute("INSERT INTO base VALUES " + rows);
            drainWalQueue();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM '2027-01-01T00:00:00.000000Z' AS " + SEED_VIEW_SQL);
            stampKeptBranchMarker(viewInstance());
            fault.refuseTheNextMarkerUnlinks(2);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = viewInstance();
                Assert.assertEquals(LiveViewState.SEED_STATE_ACTIVE, instance.getStateReader().getSeedState());
                Assert.assertEquals("the resume's retire and the completion's", 2, fault.getRefusedMarkerUnlinks());
                assertNoHistory(instance);
                assertMarkerNames(Numbers.LONG_NULL);

                commitDeparture(job, "('2027-01-01T00:01:00.000000Z', 'a', 1.0)");
                Assert.assertEquals("the first forward row's seal opens the history", 1, generation(instance));
                assertNoRepairMarker(instance);
            }
            final String viewRows = "ts\tsym\tx\ts\n2027-01-01T00:01:00.000000Z\ta\t1.0\t1.0\n";
            assertViewRows(viewRows, DEPARTURE_ROWS_QUERY);
            restartAndRefresh();
            assertRestoredFromTimeline("lv");
            assertViewRows(viewRows, DEPARTURE_ROWS_QUERY);
        });
    }

    @Test
    public void testCountDriftRetireThatKeepsTheTimelineKeepsTheFrontierTruncateMarker() throws Exception {
        // The departure of 00:02 resumes from the root at 00:01 and replays nothing: its frontier
        // truncate keeps that root, now at the frontier, under a marker naming the splice's
        // generation, and the closing seal only re-stamps the head on it. The re-stamp meets a count
        // that drifted and retires the timeline, under a lock that keeps the file. The resolution
        // clears the marker over a root kept at the frontier, but not beside a timeline a retire
        // could not remove: the marker stays live, and the restart rebuilds.
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'a', 1.5)");
                    commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', 2.5)");
                    final LiveViewInstance instance = viewInstance();
                    final long spliceGeneration = superblockGeneration(instance) + 1;
                    final long mismatchesBefore = instance.getCheckpointRowCountMismatches();
                    hooks(job).setSimulateResumeHeadSealStart(() -> {
                        fault.lock(false);
                        instance.addLvRowsTotal(1);
                    });
                    try {
                        commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', -2.5)");
                    } finally {
                        hooks(job).setSimulateResumeHeadSealStart(null);
                        fault.unlock();
                    }
                    Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT));
                    Assert.assertEquals("the re-stamp meets the drift", mismatchesBefore + 1, instance.getCheckpointRowCountMismatches());
                    Assert.assertEquals("whose retire meets the lock", 1, fault.getRefusedTimelineUnlinks());
                    // The truncate published one past the splice, so the marker naming the splice's
                    // generation reads as live.
                    assertTimelineRetirePending(instance, spliceGeneration + 1, spliceGeneration);
                    try (Path dir = checkpointsDir(instance)) {
                        Assert.assertEquals(Numbers.LONG_NULL, LiveViewCheckpointRepairMarker.readLvSeqTxn(configuration, dir));
                    }
                }
                capture.drain();
                capture.assertLogged("live view emitted row count does not match its durable output, retiring the timeline [view=lv");
                restartAndRefresh();
                capture.drain();
                capture.assertLogged("live view restart rebuilding from applied base [view=lv, cause=prefix preservation repair marker present");
                assertRebuiltFromAppliedBase("lv");
                assertViewRows("ts\tsym\tx\ts\n2026-01-01T00:01:00.000000Z\ta\t1.5\t1.5\n", DEPARTURE_ROWS_QUERY);
                assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testHeadMissSpliceThatFailsToPublishUnderALockPublishesNothingOverTheKeptTimeline() throws Exception {
        // The head-miss rebuild's splice fails to publish, and its closing seal lands on the
        // un-spliced generation, at the frontier 01:05 above every root that generation keeps. The
        // tail then retires that generation under a lock that keeps the file, and the retire takes
        // the head the seal set with it, so no cycle without rows publishes over the kept file: each
        // one's seal retries the retire and is refused, the third pausing the seal, and the first
        // after the pause retiring once more. The splice's marker stays live through all of it, and a
        // restart rebuilds over it.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createHeadMissDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    final LiveViewInstance instance = commitSixUnsealedRowsAboveTheHeadMissRoots(job);
                    final long spliceBaseGeneration = superblockGeneration(instance);
                    final long sealFailuresBefore = instance.getCheckpointSealFailures();
                    failTheHeadMissSpliceUnderALock(job, fault, true);
                    Assert.assertEquals("the closing seal lands", sealFailuresBefore, instance.getCheckpointSealFailures());
                    assertCheckpointLadder(instance, false, "2025-12-31T23:55:00.000000Z -> 1\n2026-01-01T01:05:00.000000Z -> 67\n");
                    Assert.assertEquals("the tail's retire meets the lock", 1, fault.getRefusedTimelineUnlinks());
                    assertTimelineRetirePending(instance, spliceBaseGeneration + 1, spliceBaseGeneration);
                    for (int cycle = 0; cycle < 4; cycle++) {
                        setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                        commitDeparture(job, "('2026-01-01T01:1" + cycle + ":00.000000Z', 'a', -1.0)");
                        Assert.assertEquals(
                                "each cycle's seal retries the retire and is refused",
                                sealFailuresBefore + cycle + 1,
                                instance.getCheckpointSealFailures()
                        );
                        // The fourth, past the pause the third armed, retires once more on its way.
                        Assert.assertEquals(cycle < 3 ? cycle + 2 : 6, fault.getRefusedTimelineUnlinks());
                        assertTimelineRetirePending(instance, spliceBaseGeneration + 1, spliceBaseGeneration);
                    }
                    capture.drain();
                    capture.assertLogged("live view checkpoint seal keeps failing, keeping the timeline and backing off "
                            + "[view=lv, consecutiveFailures=3, backoffMicros=60000000]");
                    capture.assertLogged("live view checkpoint seal keeps failing, retiring the timeline and backing off "
                            + "[view=lv, consecutiveFailures=4, backoffMicros=120000000]");
                    fault.unlock();
                }
                restartAndRefresh();
                capture.drain();
                capture.assertLogged("live view restart rebuilding from applied base [view=lv, cause=prefix preservation repair marker present");
                assertRebuiltFromAppliedBase("lv");
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    commitDeparture(job, "('2026-01-01T00:58:30.000000Z', 'a', 2.0)");
                }
                // 00:58:30's one-minute frame holds 00:58 at 3.0, and 00:59's holds both.
                assertQuery("SELECT ts, s FROM lv WHERE ts BETWEEN '2026-01-01T00:58:30.000000Z' AND '2026-01-01T00:59:00.000000Z'")
                        .noLeakCheck()
                        .timestamp("ts")
                        .sizeMayVary()
                        .returns("""
                                ts\ts
                                2026-01-01T00:58:30.000000Z\t5.0
                                2026-01-01T00:59:00.000000Z\t6.0
                                """);
                assertViewMatchesRecompute(HEAD_MISS_DEPARTURE_VIEW_SQL);
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testHeadMissSpliceThatFailsToPublishLeavesNoHeadBehindItsRetire() throws Exception {
        // As above with no lock: the tail's retire removes the generation the closing seal landed
        // on, and the head that seal set goes with it. So the first cycle without rows seals a fresh
        // history at the frontier 01:05 on the first-checkpoint path, rather than publish over a
        // timeline that is gone, the next ones advance its base, and nothing fails; the forward row
        // then seals at its cadence, and a restart restores.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        assertMemoryLeak(fault, () -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = commitSixUnsealedRowsAboveTheHeadMissRoots(job);
                final long sealFailuresBefore = instance.getCheckpointSealFailures();
                failTheHeadMissSpliceUnderALock(job, fault, false);
                Assert.assertEquals("no head survives the retire", Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                Assert.assertEquals("nor its base WAL arm", Numbers.LONG_NULL, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals("nor the timeline's", Numbers.LONG_NULL, instance.getCheckpointTimelineWalPurgeFloor());
                Assert.assertFalse(instance.isCheckpointTimelineRetirePending());
                assertTimelineAndMarkerRetired(instance);
                Assert.assertEquals("the closing seal landed ahead of the retire", sealFailuresBefore, instance.getCheckpointSealFailures());
                for (int cycle = 0; cycle < 4; cycle++) {
                    setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                    commitDeparture(job, "('2026-01-01T01:1" + cycle + ":00.000000Z', 'a', -1.0)");
                    Assert.assertEquals(
                            "the first cycle seals a fresh history, each later one advances its base",
                            cycle + 1,
                            generation(instance)
                    );
                    Assert.assertEquals(ts("2026-01-01T01:05:00.000000Z"), instance.getHeadCheckpointMaxTs());
                    Assert.assertEquals("at the cycle's commit", lastBaseSeqTxn(), instance.getHeadCheckpointBaseSeqTxn());
                }
                assertCheckpointLadder(instance, false, "2026-01-01T01:05:00.000000Z -> 67\n");
                Assert.assertEquals("nothing fails", sealFailuresBefore, instance.getCheckpointSealFailures());
                Assert.assertFalse(instance.isSealOnCooldown(currentMicros));
                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                commitDeparture(job, "('2026-01-01T01:20:00.000000Z', 'a', 1.0)");
                Assert.assertEquals("the forward row seals at its cadence", ts("2026-01-01T01:20:00.000000Z"), instance.getHeadCheckpointMaxTs());
                assertCheckpointLadder(instance, false, "2026-01-01T01:05:00.000000Z -> 67\n2026-01-01T01:20:00.000000Z -> 68\n");
                assertNoRefreshFaults("lv");
            }
            restartAndRefresh();
            assertRestoredFromTimeline("lv");
            assertViewMatchesRecompute(HEAD_MISS_DEPARTURE_VIEW_SQL);
        });
    }

    @Test
    public void testLocalizedHeadMissOverAKeptTimelinePublishesNoSplice() throws Exception {
        // A localized correction while the view refuses the generation a retire kept: the repair
        // finds no anchor in it, its capture is refused, and so is the truncate it falls back to,
        // whose own failure path retires once more. Nothing splices into the kept generation and
        // nothing publishes over it.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createHeadMissDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    final LiveViewInstance instance = commitSixUnsealedRowsAboveTheHeadMissRoots(job);
                    final long spliceBaseGeneration = superblockGeneration(instance);
                    failTheHeadMissSpliceUnderALock(job, fault, true);
                    final long rootsVersionedBefore = instance.getCheckpointRepairRootsVersioned();
                    final long resetsBefore = instance.getCheckpointTimelineResets();
                    final long sealFailuresBefore = instance.getCheckpointSealFailures();
                    final long headMissRepairsBefore = countHeadMissRepairs(job);
                    capture.drain();
                    commitDeparture(job, "('2026-01-01T00:30:00.000000Z', 'a', 4.0)");
                    Assert.assertEquals("the head-miss rebuild repairs the change", headMissRepairsBefore + 1, countHeadMissRepairs(job));
                    capture.drain();
                    capture.assertLogged("live view checkpoint timeline repair capture unavailable, truncating instead [view=lv");
                    capture.assertLogged("could not preserve live view checkpoint timeline prefix, retiring [view=lv");
                    Assert.assertEquals("no splice re-versions a root", rootsVersionedBefore, instance.getCheckpointRepairRootsVersioned());
                    Assert.assertEquals("the truncate's failure path retires", resetsBefore + 1, instance.getCheckpointTimelineResets());
                    // The tail's retire, then the truncate's and the closing seal's retry of it.
                    Assert.assertEquals(3, fault.getRefusedTimelineUnlinks());
                    Assert.assertEquals("the closing seal is refused", sealFailuresBefore + 1, instance.getCheckpointSealFailures());
                    assertTimelineRetirePending(instance, spliceBaseGeneration + 1, spliceBaseGeneration);
                    fault.unlock();
                }
                assertViewMatchesRecompute(HEAD_MISS_DEPARTURE_VIEW_SQL);
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testHeadMissSpliceThatFailsWhileItsTimelineRefusesEveryOpenStillWritesItsMarker() throws Exception {
        // The kept-row departure's splice fails after its metadata, and from the closing seal on
        // _timeline refuses its unlink and every open. The closing seal fails on the open, and the
        // tail's retire keeps the file beside the splice's marker. Judging that marker reads the
        // superblock, whose open is refused too, so the retire writes its own marker in its place,
        // which names no generation and so reads as live whatever the superblock says. The turn
        // ends with no refresh fault, the capture the repair held freed on its way out, and a
        // restart rebuilds over the marker.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createHeadMissDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    commitHeadMissDepartureFixture(job, false, 59);
                    final LiveViewInstance instance = viewInstance();
                    final long sealFailuresBefore = instance.getCheckpointSealFailures();
                    final long repairFailuresBefore = instance.getCheckpointRepairFailures();
                    fault.afterTheNextMarker(() -> job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH));
                    hooks(job).setSimulateHeadMissHeadSealStart(() -> {
                        job.setCheckpointTimelineTestFailureStage(0);
                        fault.lock(false);
                        fault.refuseTimelineOpens(true);
                    });
                    try {
                        commitDeparture(job, HEAD_MISS_KEPT_ROW_DEPARTURE);
                    } finally {
                        hooks(job).setSimulateHeadMissHeadSealStart(null);
                        fault.afterTheNextMarker(null);
                        job.setCheckpointTimelineTestFailureStage(0);
                    }
                    Assert.assertEquals("the splice fails to publish", repairFailuresBefore + 1, instance.getCheckpointRepairFailures());
                    Assert.assertEquals("the closing seal fails on the open", sealFailuresBefore + 1, instance.getCheckpointSealFailures());
                    Assert.assertEquals("the tail's retire meets the lock", 1, fault.getRefusedTimelineUnlinks());
                    assertNoRefreshFaults("lv");
                    Assert.assertTrue("the retire keeps the timeline", instance.isCheckpointTimelineRetirePending());
                    assertTimelineOnDisk(instance, true);
                    assertMarkerNames(Numbers.LONG_NULL);
                    try (Path dir = checkpointsDir(instance)) {
                        Assert.assertEquals(Numbers.LONG_NULL, LiveViewCheckpointRepairMarker.readLvSeqTxn(configuration, dir));
                    }
                    capture.drain();
                    capture.assertLogged("could not read the live view checkpoint repair marker beside a timeline its retire could not remove, writing one [view=lv");
                    fault.refuseTimelineOpens(false);
                    fault.unlock();
                }
                assertViewRows(HEAD_MISS_KEPT_ROW_DEPARTURE_ROWS, DEPARTURE_ROWS_QUERY);
                restartAndRefresh();
                capture.drain();
                capture.assertLogged("live view restart rebuilding from applied base [view=lv, cause=prefix preservation repair marker present");
                assertRebuiltFromAppliedBase("lv");
                assertViewMatchesRecompute(HEAD_MISS_DEPARTURE_VIEW_SQL);
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testDepartureThatEmptiesTheViewWhileItsTimelineIsLockedSealsNothingAndKeepsItsMarker() throws Exception {
        // The departure that empties the view retires its timeline under a lock that keeps the file
        // and the frontier truncate's marker beside it. A view with no row seals no boundary, so no
        // seal retries the retire: the closing seal has no frontier to stamp, and each cycle without
        // rows declines its boundary over the empty table. The restart rebuilds over the marker,
        // with no row, and the forward row after it opens a fresh history.
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
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
                    final long spliceGeneration = superblockGeneration(instance) + 1;
                    final long sealFailuresBefore = instance.getCheckpointSealFailures();
                    fault.lock(false);
                    commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'a', -2.5)");
                    Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT));
                    Assert.assertEquals("the truncate's retire alone meets the lock", 1, fault.getRefusedTimelineUnlinks());
                    Assert.assertEquals("no seal is attempted", sealFailuresBefore, instance.getCheckpointSealFailures());
                    assertTimelineRetirePending(instance, spliceGeneration, spliceGeneration);
                    final long declinesBefore = job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT);
                    for (int minute = 6; minute <= 7; minute++) {
                        setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                        commitDeparture(job, "('2026-01-01T00:0" + minute + ":00.000000Z', 'a', -1.0)");
                        Assert.assertEquals(
                                "each cycle declines its boundary over the empty table",
                                declinesBefore + minute - 5,
                                job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT)
                        );
                    }
                    Assert.assertEquals("and retries no retire", 1, fault.getRefusedTimelineUnlinks());
                    Assert.assertEquals(sealFailuresBefore, instance.getCheckpointSealFailures());
                    assertTimelineRetirePending(instance, spliceGeneration, spliceGeneration);
                    fault.unlock();
                }
                final String noRows = "ts\tsym\tx\ts\n";
                assertViewRows(noRows, DEPARTURE_ROWS_QUERY);
                restartAndRefresh();
                capture.drain();
                capture.assertLogged("live view restart rebuilding from applied base [view=lv, cause=prefix preservation repair marker present");
                assertRebuiltFromAppliedBase("lv");
                assertViewRows(noRows, DEPARTURE_ROWS_QUERY);
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    commitDeparture(job, "('2026-01-01T00:08:00.000000Z', 'a', 4.0)");
                }
                final LiveViewInstance instance = viewInstance();
                Assert.assertEquals("the forward row's seal opens a fresh history", 1, generation(instance));
                assertCheckpointLadder(instance, false, "2026-01-01T00:08:00.000000Z -> 1\n");
                assertViewRows(noRows + "2026-01-01T00:08:00.000000Z\ta\t4.0\t4.0\n", DEPARTURE_ROWS_QUERY);
                assertViewMatchesRecompute(HEAD_MISS_DEPARTURE_VIEW_SQL);
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testBaseColumnAddedOverAKeptTimelineRebuildsInPlace() throws Exception {
        // A base column added while the view refuses the generation a retire kept: the in-place
        // recovery reads the marker the retire wrote, and rebuilds from the applied base rather than
        // restore the kept generation.
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    final LiveViewInstance instance = commitFourRoots(job, false);
                    changeBelowEveryRootUnderALock(job, fault);
                    final long runtimeRestoresBefore = instance.getCheckpointRuntimeRestores();
                    execute("ALTER TABLE base ADD COLUMN y INT");
                    drainWalQueue();
                    driveRefreshToQuiescence(job);
                    Assert.assertEquals("nothing restores behind the live marker", runtimeRestoresBefore, instance.getCheckpointRuntimeRestores());
                    Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
                    Assert.assertFalse(instance.isInvalid());
                    fault.unlock();
                }
                capture.drain();
                capture.assertLogged(runtimeRestoreDeclinedOverTheMarker("base table metadata change"));
                assertViewRows(CHANGE_BELOW_EVERY_ROOT_ROWS, DEPARTURE_ROWS_QUERY);
                TestUtils.assertSqlCursors(
                        engine,
                        sqlExecutionContext,
                        "(" + DEPARTURE_VIEW_SQL + ") ORDER BY 2, 1",
                        "(" + DEPARTURE_ROWS_QUERY + ") ORDER BY 2, 1",
                        LOG,
                        true
                );
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testBaseColumnAddedOverAKeptTimelineWithoutItsMarkerRebuildsInPlace() throws Exception {
        // testBaseColumnAddedOverAKeptTimelineRebuildsInPlace with no marker beside the kept file:
        // the in-place recovery's read of the kept generation is refused instead, and it rebuilds
        // from the applied base all the same.
        assertBaseColumnAddedOverAKeptTimelineWithoutItsMarker(false);
    }

    @Test
    public void testBaseColumnAddedOverAKeptTimelineWithoutItsMarkerStopsTheViewOverTheDayTheBaseLost() throws Exception {
        // As above, over the day the base lost: the restatement guard refuses the rebuild, and the
        // view stops at once with every row, without retrying the restore that was refused.
        assertBaseColumnAddedOverAKeptTimelineWithoutItsMarker(true);
    }

    @Test
    public void testSeedCompletionOverAKeptTimelineSealsNoRootAndRebuildsOnRestart() throws Exception {
        // The seed sweep's completion retires the seed roots under a lock that keeps _timeline. The
        // seal of the finished state retries the retire, meets the lock and is refused, so the view
        // goes ACTIVE with no head, and nothing publishes over the seed generation. A restart meets
        // the marker the retire wrote and rebuilds rather than restore that generation.
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createSeedingView(SEED_ROWS - 2, 2);
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    final LiveViewInstance instance = runSeedTurns(job, SEED_ROWS);
                    Assert.assertEquals("each turn sealed a seed root", SEED_ROWS, instance.getSeedCheckpointDataOffset());
                    final long generationBefore = superblockGeneration(instance);
                    final long sealFailuresBefore = instance.getCheckpointSealFailures();
                    fault.lock(false);
                    job.run();
                    drainWalQueue();
                    Assert.assertEquals(LiveViewState.SEED_STATE_ACTIVE, instance.getStateReader().getSeedState());
                    Assert.assertEquals("the completion's retire, then its seal's retry of it", 2, fault.getRefusedTimelineUnlinks());
                    Assert.assertEquals("the completion seal is refused", sealFailuresBefore + 1, instance.getCheckpointSealFailures());
                    assertTimelineRetirePending(instance, generationBefore, Numbers.LONG_NULL);
                    fault.unlock();
                }
                assertViewMatchesRecompute(SEED_VIEW_SQL);
                restartAndRefresh();
                capture.drain();
                capture.assertLogged("live view restart rebuilding from applied base [view=lv, cause=prefix preservation repair marker present");
                assertRebuiltFromAppliedBase("lv");
                assertViewMatchesRecompute(SEED_VIEW_SQL);
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testSeedSweepOverAKeptTimelineRetriesItsSealOnEveryTurn() throws Exception {
        // Once the lock goes, the first seed seal retires the kept timeline and seals its root in a
        // fresh history, and the sweep completes as a recompute does.
        assertSeedSweepOverAKeptTimeline(false);
    }

    @Test
    public void testSeedSweepOverAKeptTimelineRestartedWhileTheLockHoldsResweeps() throws Exception {
        // A restart while the lock holds: the restarted sweep's resume reads the kept seed root,
        // which it cannot read either, so it retires again, under the lock, and re-sweeps from offset
        // 0 behind its skip-write floor.
        assertSeedSweepOverAKeptTimeline(true);
    }

    @Test
    public void testSeedRearmOverAKeptTimelineResweeps() throws Exception {
        // The restarted sweep cannot read its torn seed root, and the retire that abandons the resume
        // keeps _timeline under the lock. Two rows a turn: the turn that feeds the first day's last
        // row and then reads the second day faults on that read, which re-arms the sweep's resume in
        // the same process. The resume reads the timeline again and is refused the kept generation,
        // so it is abandoned for a re-sweep from offset 0 behind the skip-write floor, which
        // completes as a recompute does once the lock goes.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 2);
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createSeedingView(7, 2);
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    Assert.assertEquals("the turn seals a seed root", 2, runSeedTurns(job, 1).getSeedCheckpointDataOffset());
                }
                engine.getLiveViewRegistry().clear();
                engine.buildViewGraphs();
                final LiveViewInstance instance = viewInstance();
                corruptNewestRootDataSegment(instance);
                final long generationBefore = superblockGeneration(instance);
                fault.lock(false);
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    runSeedTurns(job, 1);
                    Assert.assertEquals("the abandon's two retires, then the turn's seal", 3, fault.getRefusedTimelineUnlinks());
                    capture.drain();
                    Assert.assertEquals("the torn root abandons the resume", 1, capture.countOccurrences(SEED_RESUME_ABANDONED));
                    fault.failTheNextBaseColumnOpen(engine.verifyTableName("base").getDirName(), "2026-01-02");
                    for (int pass = 0; pass < REFRESH_QUIESCENCE_PASSES && capture.countOccurrences(SEED_RESUME_ABANDONED) < 2; pass++) {
                        advanceClockToNextRefreshPass();
                        runSeedTurns(job, 1);
                        capture.drain();
                    }
                    Assert.assertEquals("the sweep's read of the second day faults", 1, fault.getRefusedBaseColumnOpens());
                    Assert.assertEquals("which is the view's one refresh fault", 1, instance.getRefreshFaultCount());
                    capture.assertLogged("live view mid-seed refresh failure, sweep will resume [view=lv");
                    Assert.assertEquals("the re-armed resume is abandoned too", 2, capture.countOccurrences(SEED_RESUME_ABANDONED));
                    capture.assertLoggedRE(Pattern.quote(SEED_RESUME_ABANDONED) + "[^\\n]*" + Pattern.quote(KEPT_TIMELINE_REFUSAL));
                    assertTimelineRetirePending(instance, generationBefore, Numbers.LONG_NULL);
                    Assert.assertEquals("and no seed root landed", Numbers.LONG_NULL, instance.getSeedCheckpointDataOffset());
                    fault.unlock();
                    driveSeedToCompletion(job, "lv");
                    Assert.assertFalse(instance.isCheckpointTimelineRetirePending());
                }
                Assert.assertEquals(LiveViewState.SEED_STATE_ACTIVE, viewInstance().getStateReader().getSeedState());
                // The injected fault is the view's one refresh fault, asserted above.
                TestUtils.assertSqlCursors(
                        engine,
                        sqlExecutionContext,
                        "(" + SEED_VIEW_SQL + ") ORDER BY 2, 1",
                        "(lv) ORDER BY 2, 1",
                        LOG,
                        true
                );
                restartAndRefresh();
                assertViewMatchesRecompute(SEED_VIEW_SQL);
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testKeyedClosedSegmentRepairOverAKeptTimelineReadsTheSegmentWhole() throws Exception {
        // A correction of acct-2 in the closed day 2026-01-02, which a keyed replay would repair
        // along that account alone once it has vetted the roots it re-versions. The timeline they
        // sit in is one a retire kept: the vetting is refused, so the repair reads the day whole,
        // and nothing splices into the kept generation.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_REPLAY_ENABLED, "true");
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                final StringSink seed = new StringSink();
                for (int day = 2; day <= 4; day++) {
                    for (int offset = 1; offset <= 16; offset++) {
                        if (seed.length() > 0) {
                            seed.put(", ");
                        }
                        seed.put(anchoredRow(day, 1, offset, "acct-" + ((offset - 1) % 4 + 1)));
                    }
                }
                createAnchoredView(seed);
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveRefreshToQuiescence(job);
                    commitToTx(job, anchoredRow(5, 1, 0, "acct-1"));
                    final LiveViewInstance instance = keepTheTimelineUnderALock(job, fault);
                    final long generationBefore = superblockGeneration(instance);
                    final long keyedRepairsBefore = job.getStatsForTest().getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_KEYED_CLOSED_SEGMENT);
                    final long rootsVersionedBefore = instance.getCheckpointRepairRootsVersioned();
                    capture.drain();
                    commitToTx(job, anchoredRow(2, 0, 30, "acct-2"));
                    capture.drain();
                    capture.assertLogged("live view could not vet the boundaries a keyed repair re-versions [view=lv");
                    Assert.assertEquals(
                            "the keyed route is declined",
                            keyedRepairsBefore,
                            job.getStatsForTest().getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_KEYED_CLOSED_SEGMENT)
                    );
                    Assert.assertEquals("nothing splices into the kept generation", rootsVersionedBefore, instance.getCheckpointRepairRootsVersioned());
                    assertTimelineRetirePending(instance, generationBefore, Numbers.LONG_NULL);
                    fault.unlock();
                }
                assertAnchoredViewMatchesRecompute();
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testSegmentLoopOverAKeptTimelineDoesNotRestoreTheFrontierRoot() throws Exception {
        // A late commit into the closed day 2026-01-09, whose repair seals a root at the frontier,
        // then a drain that feeds an in-order row and meets a late commit into 2026-01-08, which
        // it hands to a per-segment loop. Ahead of the loop the runtime goes back to the root at
        // its frontier, which drops the in-order row the drain fed, but every root the view could
        // go back to sits in a timeline a retire kept: the lookup is refused, and the union range
        // repairs the change instead.
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                final StringSink seed = new StringSink();
                for (int day = 2; day <= 9; day++) {
                    if (seed.length() > 0) {
                        seed.put(", ");
                    }
                    seed.put(anchoredRow(day, 1, 0, "acct-1")).put(", ").put(anchoredRow(day, 2, 0, "acct-2"));
                }
                createAnchoredView(seed);
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveRefreshToQuiescence(job);
                    commitToTx(job, anchoredRow(10, 1, 0, "acct-1"));
                    final LiveViewInstance instance = keepTheTimelineUnderALock(job, fault);
                    final long generationBefore = superblockGeneration(instance);
                    hooks(job).setSimulatePeerTurnBeforeRefreshLatch(() -> {
                        try {
                            execute("INSERT INTO tx VALUES " + anchoredRow(10, 2, 0, "acct-2"));
                            execute("INSERT INTO tx VALUES " + anchoredRow(8, 3, 0, "acct-1"));
                        } catch (SqlException e) {
                            throw new RuntimeException(e);
                        }
                        // Ahead of the pass that reads the two commits, which is the turn after this one.
                        hooks(job).setSimulatePeerTurnBeforeRefreshLatch(() -> drainWalQueue());
                    });
                    capture.drain();
                    try {
                        commitToTx(job, anchoredRow(9, 3, 0, "acct-2"));
                    } finally {
                        hooks(job).setSimulatePeerTurnBeforeRefreshLatch(null);
                    }
                    capture.drain();
                    capture.assertNotLogged("live view restored its runtime to the root at the frontier ahead of a segment loop");
                    assertTimelineRetirePending(instance, generationBefore, Numbers.LONG_NULL);
                    fault.unlock();
                }
                assertAnchoredViewMatchesRecompute();
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testZeroRowEndOfTableHeadMissOverAKeptTimelineWarmsTheFrontierState() throws Exception {
        // The departure of the rows above the frontier 2026-01-02T00:00:50 emits no row, and the
        // rebuild derives the state at that frontier ahead of its commit. A root at 00:00:10, inside
        // the frontier's frame, would let it restore that root and fold the rows above it, but the
        // timeline holding it is one a retire kept: the lookup is refused, and the state is folded
        // from the frame's floor instead.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        assertMemoryLeak(fault, () -> {
            createHeadMissDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitDeparture(job, "('2025-12-31T23:55:00.000000Z', 'a', 0.5)");
                commitDeparture(job, minuteRowsOfA(0, 30, "1.0") + ", ('2026-01-01T23:59:50.000000Z', 'a', 1.0)");
                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                commitDeparture(job, "('2026-01-02T00:00:10.000000Z', 'a', 1.0)");
                commitDeparture(
                        job,
                        "('2026-01-02T00:00:20.000000Z', 'a', -1.0), ('2026-01-02T00:00:30.000000Z', 'a', -1.0), "
                                + "('2026-01-02T00:00:40.000000Z', 'a', -1.0), ('2026-01-02T00:00:50.000000Z', 'a', 1.0), "
                                + "('2026-01-02T00:02:00.000000Z', 'a', 1.0), ('2026-01-02T00:02:10.000000Z', 'a', 1.0), "
                                + "('2026-01-02T00:02:20.000000Z', 'a', 1.0)"
                );
                final LiveViewInstance instance = viewInstance();
                assertCheckpointLadder(instance, false, """
                        2025-12-31T23:55:00.000000Z -> 1
                        2026-01-02T00:00:10.000000Z -> 34
                        """);
                final long generationBefore = superblockGeneration(instance);
                final LiveViewRefreshStats stats = job.getStatsForTest();
                final long warmUpsBefore = stats.get(LiveViewRefreshStats.HEAD_MISS_FRONTIER_STATE_WARM_UP_COUNT);
                final long restoresBefore = stats.get(LiveViewRefreshStats.HEAD_MISS_FRONTIER_STATE_RESTORE_COUNT);
                // A truncate floored below every root keeps none, so it retires the timeline, which
                // the lock keeps.
                fault.lock(false);
                Assert.assertFalse(job.truncateOrRetireTimelineOnO3ForTest(instance, ts("2025-12-31T23:55:00.000000Z")));
                replaceBaseRange(ts("2026-01-02T00:02:00.000000Z"), ts("2026-01-02T00:03:00.000000Z"), Double.NaN);
                drainWalQueue();
                driveRefreshToQuiescence(job);
                Assert.assertEquals("no root is restored at the frontier", restoresBefore, stats.get(LiveViewRefreshStats.HEAD_MISS_FRONTIER_STATE_RESTORE_COUNT));
                Assert.assertEquals("its state is folded from the frame's floor", warmUpsBefore + 1, stats.get(LiveViewRefreshStats.HEAD_MISS_FRONTIER_STATE_WARM_UP_COUNT));
                assertTimelineRetirePending(instance, generationBefore, Numbers.LONG_NULL);
                fault.unlock();
            }
            // Each one-minute frame holds the rows of 'a' the filter keeps within a minute below the
            // row: 01-01T23:59:50 alone, then 01-02T00:00:10 over it, then 00:00:50 over both.
            assertViewRows(headMissDepartureRows(true, 30) + """
                    2026-01-01T23:59:50.000000Z\ta\t1.0\t1.0
                    2026-01-02T00:00:10.000000Z\ta\t1.0\t2.0
                    2026-01-02T00:00:50.000000Z\ta\t1.0\t3.0
                    """, DEPARTURE_ROWS_QUERY);
            assertViewMatchesRecompute(HEAD_MISS_DEPARTURE_VIEW_SQL);
        });
    }

    // One row of account at hour:minute of 2026-01-day, holding 1.0, as an INSERT tuple.
    private static String anchoredRow(int day, int hour, int minute, String account) {
        return String.format("('2026-01-%02dT%02d:%02d:00.000000Z', '%s', 1.0)", day, hour, minute, account);
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

    // Arms the next seal to fail: on the allocation of its data segment, or, when
    // isPublicationFault, ahead of the superblock of its publication, which then fails every
    // publication until disarmTheSealFault.
    private static void armTheSealFault(LiveViewRefreshJob job, LiveViewCheckpointDiskFullFault fault, boolean isPublicationFault) {
        if (isPublicationFault) {
            job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH);
        } else {
            fault.arm(LiveViewCheckpointDiskFullFault.OP_ALLOCATE, LiveViewCheckpointLayout.DATA_DIR_NAME, 1);
        }
    }

    // Lifts whatever armTheSealFault armed.
    private static void disarmTheSealFault(LiveViewRefreshJob job, LiveViewCheckpointDiskFullFault fault) {
        job.setCheckpointTimelineTestFailureStage(0);
        fault.disarm();
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
     * answers it, and the seal after it, removing nothing - or, when
     * {@code isOnlyTheTimelineKept}, nothing but the files beside {@code _timeline} and the
     * segment directories, and the marker the retire keeps beside the timeline. The splice's
     * generation stays on disk under the truncate's marker, which records no seqTxn. A base
     * column then has the view recover in place, which declines that generation, and the
     * restatement guard refuses the rebuild: the view stops with every row, in place and on each
     * restart.
     */
    private void assertFrontierTruncateWhosePublicationAndRetireFailKeepsTheRows(boolean isHeadMiss, boolean isOnlyTheTimelineKept) throws Exception {
        final FrontierTruncateDoubleFault fault = new FrontierTruncateDoubleFault(isOnlyTheTimelineKept);
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

    // A retire removed every name of the view's timeline: _timeline, both repair marker names, the
    // retirement work set under both names, and the meta, data and repair directories. Reads the
    // names' existence alone.
    private void assertCheckpointFilesRetired(LiveViewInstance instance) {
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path dir = checkpointsDir(instance); Path path = new Path()) {
            Assert.assertFalse("_timeline", ff.exists(LiveViewCheckpointLayout.timelinePath(path, dir).$()));
            Assert.assertFalse("the repair marker", ff.exists(LiveViewCheckpointLayout.repairingMarkerPath(path, dir).$()));
            Assert.assertFalse("its staged sibling", ff.exists(path.put(LiveViewCheckpointLayout.TMP_SUFFIX).$()));
            Assert.assertFalse("the retirement work set", ff.exists(LiveViewCheckpointLayout.retirementQueuePath(path, dir).$()));
            Assert.assertFalse("its staged sibling", ff.exists(path.put(LiveViewCheckpointLayout.TMP_SUFFIX).$()));
            Assert.assertFalse("the meta directory", ff.exists(LiveViewCheckpointLayout.metaDirPath(path, dir).$()));
            Assert.assertFalse("the data directory", ff.exists(LiveViewCheckpointLayout.dataDirPath(path, dir).$()));
            Assert.assertFalse("the repair directory", ff.exists(LiveViewCheckpointLayout.repairDirPath(path, dir).$()));
        }
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

    // The view's timeline file is on disk, and beside it a staged repair marker with no published
    // one, which fails its checks: a torn record, which names no generation.
    private void assertTornStagedMarkerBesideTheTimeline(LiveViewInstance instance) {
        assertTimelineOnDisk(instance, true);
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path dir = checkpointsDir(instance); Path path = new Path()) {
            LiveViewCheckpointLayout.repairingMarkerPath(path, dir);
            Assert.assertFalse(ff.exists(path.$()));
            path.put(LiveViewCheckpointLayout.TMP_SUFFIX);
            Assert.assertTrue("the torn staged marker stays beside the timeline", ff.exists(path.$()));
            Assert.assertEquals(Numbers.LONG_NULL, LiveViewCheckpointRepairMarker.readBaseGeneration(configuration, dir));
        }
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

    // The anchored view over tx matches the plain-SQL recompute that partitions each account's rows
    // by the day the anchor resets on.
    private void assertAnchoredViewMatchesRecompute() throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + ANCHORED_RECOMPUTE_SQL + ") ORDER BY 2, 1",
                "(lv) ORDER BY 2, 1",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");
    }

    // The seal failures the view counted, the unlinks of _timeline the lock refused, and the timeline
    // resets the view counted, each at its expected total.
    private static void assertSealRetries(
            LiveViewInstance instance,
            long sealFailures,
            TimelineUnlinkFault fault,
            int refusedUnlinks,
            long resets
    ) {
        Assert.assertEquals("seal failures", sealFailures, instance.getCheckpointSealFailures());
        Assert.assertEquals("unlinks of _timeline the lock refused", refusedUnlinks, fault.getRefusedTimelineUnlinks());
        Assert.assertEquals("timeline resets", resets, instance.getCheckpointTimelineResets());
    }

    // The seal failures the view counted, the unlinks of either repair marker name the marker's lock
    // refused, and the timeline resets the view counted, each at its expected total.
    private static void assertMarkerSealRetries(
            LiveViewInstance instance,
            long sealFailures,
            TimelineUnlinkFault fault,
            int refusedMarkerUnlinks,
            long resets
    ) {
        Assert.assertEquals("seal failures", sealFailures, instance.getCheckpointSealFailures());
        Assert.assertEquals("unlinks of the marker the lock refused", refusedMarkerUnlinks, fault.getRefusedMarkerUnlinks());
        Assert.assertEquals("timeline resets", resets, instance.getCheckpointTimelineResets());
    }

    // No slot of the view's _timeline, if it has one, passes its checksum: no history stands, so no
    // generation a restart could restore or weigh a repair marker against.
    private static void assertNoHistory(LiveViewInstance instance) {
        try (Path dir = checkpointsDir(instance); Path timelinePath = new Path()) {
            LiveViewCheckpointLayout.timelinePath(timelinePath, dir);
            if (!configuration.getFilesFacade().exists(timelinePath.$())) {
                return;
            }
            try (LiveViewCheckpointSuperblock superblock = new LiveViewCheckpointSuperblock(configuration)) {
                superblock.of(dir);
                Assert.assertFalse("no generation is published", superblock.isValid());
                Assert.assertEquals("no slot passes its checksum", Long.MAX_VALUE, superblock.getOldestValidGeneration());
            }
        }
    }

    // The seed cursor the view's selected generation carries: the row offset of a seed root, or
    // LONG_NULL for a steady root.
    private static long seedCursorOffset(LiveViewInstance instance) {
        try (
                Path dir = checkpointsDir(instance);
                LiveViewCheckpointSuperblock superblock = new LiveViewCheckpointSuperblock(configuration)
        ) {
            superblock.of(dir);
            Assert.assertTrue(superblock.isValid());
            return superblock.seedCursorOffset;
        }
    }

    // Stamps the repair marker the retire that keeps a timeline writes beside it: no base generation,
    // no seqTxn, and the identity every publication of the view names.
    private static void stampKeptBranchMarker(LiveViewInstance instance) {
        try (Path dir = checkpointsDir(instance)) {
            LiveViewCheckpointRepairMarker.write(
                    configuration,
                    dir,
                    instance.getLiveViewToken().getTableId(),
                    0,
                    Numbers.LONG_NULL,
                    Numbers.LONG_NULL,
                    Numbers.LONG_NULL
            );
        }
    }

    // The splice's marker, under the generation it names, which the failed splice never moved past,
    // records a seqTxn the view's own WAL has committed past: the seqTxn rule reads it as live.
    private static void assertSpliceMarkerLive(LiveViewInstance instance) {
        try (Path dir = checkpointsDir(instance)) {
            final long markerLvSeqTxn = LiveViewCheckpointRepairMarker.readLvSeqTxn(configuration, dir);
            Assert.assertNotEquals("the splice's marker records the seqTxn ahead of its commit", Numbers.LONG_NULL, markerLvSeqTxn);
            Assert.assertTrue(
                    "and the replacement committed past it",
                    markerLvSeqTxn < engine.getTableSequencerAPI().lastTxn(instance.getLiveViewToken())
            );
        }
    }

    /**
     * Asserts the view refuses the generation a retire kept: the retire stands pending, the file is
     * on disk with its superblock still at keptGeneration, which nothing published over, no head and
     * neither base WAL arm survived the retire, and a marker naming markerBaseGeneration stands beside
     * the file. {@link Numbers#LONG_NULL} is the marker the retire writes itself, which names no
     * generation and so reads as live whatever the superblock says.
     */
    private void assertTimelineRetirePending(LiveViewInstance instance, long keptGeneration, long markerBaseGeneration) {
        Assert.assertTrue("the retire kept _timeline", instance.isCheckpointTimelineRetirePending());
        assertTimelineOnDisk(instance, true);
        Assert.assertEquals("nothing publishes over the kept timeline", keptGeneration, superblockGeneration(instance));
        Assert.assertEquals("no head survives the retire", Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
        Assert.assertEquals("nor the head's base WAL arm", Numbers.LONG_NULL, instance.getHeadCheckpointBaseSeqTxn());
        Assert.assertEquals("nor the timeline's", Numbers.LONG_NULL, instance.getCheckpointTimelineWalPurgeFloor());
        assertMarkerNames(markerBaseGeneration);
    }

    /**
     * The change below every root over {@link #commitFourRoots}, or over
     * {@link #commitFourRootsOverTheDayTheBaseLoses} when isDayLost, under a lock on _timeline that
     * stays on, with every marker write of the change's turn failing: the retire keeps the file
     * with no marker beside it. A base column added in the same process then has the view recover
     * in place, which no marker declines, so it reaches the restore, and the restore's read of the
     * kept generation is refused. The refusal says the timeline cannot be restored from, which a
     * retry would not change, so the recovery rebuilds from the applied base at once, and stops
     * the view at once, with every row it held, when the restatement guard refuses that rebuild
     * over the day the base lost.
     */
    private void assertBaseColumnAddedOverAKeptTimelineWithoutItsMarker(boolean isDayLost) throws Exception {
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        final String restoreRefused = "live view could not restore its runtime from the checkpoint timeline, rebuilding from "
                + "the applied base [view=lv, cause=base table metadata change, error=";
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    final LiveViewInstance instance = isDayLost ? commitFourRootsOverTheDayTheBaseLoses(job) : commitFourRoots(job, false);
                    fault.failMarkerWrites(-1);
                    try {
                        changeBelowEveryRootUnderALock(job, fault);
                    } finally {
                        fault.failMarkerWrites(0);
                    }
                    Assert.assertEquals("the retire's marker write and the retry's", 2, fault.getRefusedMarkerWrites());
                    Assert.assertTrue(instance.isCheckpointTimelineRetirePending());
                    assertTimelineOnDisk(instance, true);
                    assertNoRepairMarker(instance);
                    final long runtimeRestoresBefore = instance.getCheckpointRuntimeRestores();
                    final long faultsBefore = instance.getRefreshFaultCount();
                    execute("ALTER TABLE base ADD COLUMN y INT");
                    drainWalQueue();
                    driveRefreshToQuiescence(job);
                    Assert.assertEquals("nothing restores the kept generation", runtimeRestoresBefore, instance.getCheckpointRuntimeRestores());
                    Assert.assertEquals("the drift is the one refresh fault", faultsBefore + 1, instance.getRefreshFaultCount());
                    Assert.assertEquals("and the recovery charges no retry", 0, instance.getFlushRetryCount());
                    Assert.assertTrue("the view goes on refusing the kept generation", instance.isCheckpointTimelineRetirePending());
                    Assert.assertFalse(instance.isInvalid());
                    Assert.assertEquals("the refused rebuild stops the view", isDayLost, instance.isCheckpointRecoveryBlocked());
                    if (isDayLost) {
                        TestUtils.assertContains(
                                instance.getCheckpointRecoveryReason(),
                                "would drop rows it retains [cause=base table metadata change]: " + OWED_ROOT_HISTORY_FLOOR
                        );
                    }
                    fault.unlock();
                }
                capture.drain();
                Assert.assertEquals("one restore, refused", 1, capture.countOccurrences(restoreRefused));
                capture.assertLoggedRE(Pattern.quote(restoreRefused) + "[^\\n]*" + Pattern.quote(KEPT_TIMELINE_REFUSAL));
                capture.assertNotLogged("live view could not restore its runtime from the checkpoint timeline, retrying [view=lv");
                if (isDayLost) {
                    capture.assertLogged("live view rebuild from the applied base refused, it would drop rows the view retains "
                            + "[view=lv, cause=base table metadata change, reason=");
                    capture.assertNotLogged("live view restore failed and its rebuild would drop rows the view retains, retrying the restore");
                    capture.assertNotLogged(GUARD_STAND_DOWN);
                    assertViewRows(CHANGE_BELOW_EVERY_ROOT_OVER_THE_LOST_DAY_ROWS, DEPARTURE_ROWS_QUERY);
                } else {
                    capture.assertLogged("live view recomputed window state from applied base [view=lv, cause=base table metadata change]");
                    assertViewRows(CHANGE_BELOW_EVERY_ROOT_ROWS, DEPARTURE_ROWS_QUERY);
                    TestUtils.assertSqlCursors(
                            engine,
                            sqlExecutionContext,
                            "(" + DEPARTURE_VIEW_SQL + ") ORDER BY 2, 1",
                            "(" + DEPARTURE_ROWS_QUERY + ") ORDER BY 2, 1",
                            LOG,
                            true
                    );
                }
            });
        } finally {
            capture.stop();
        }
    }

    /**
     * The lock outlasts the change below every root: the forward row's seal and the late row's
     * closing seal retry the retire and are refused, the third refusal in a row pausing the seal,
     * and the late row finds no anchor. Nothing publishes over the kept generation. Past the pause,
     * with the lock gone, the next row's seal opens a fresh history. When isTheCheckFailing, the
     * check of _timeline after the change's retire fails, and so does the one after the forward
     * row's retry, over the file the lock keeps: the view goes on refusing it, logs that it does
     * once, and rewrites no marker over the live one beside it.
     */
    private void assertChangeBelowEveryRootWhileItsTimelineStaysLockedRepairsLateRowsWithoutAnAnchor(boolean isTheCheckFailing) throws Exception {
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    final LiveViewInstance instance = commitFourRoots(job, false);
                    final long generationBefore = superblockGeneration(instance);
                    final long sealFailuresBefore = instance.getCheckpointSealFailures();
                    changeBelowEveryRootUnderALock(job, fault, isTheCheckFailing);
                    final int markerRenamesBefore = fault.getMarkerRenames();
                    if (isTheCheckFailing) {
                        fault.failTheCheckAfterTheNextTimelineUnlink();
                    }
                    commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'a', 5.0)");
                    Assert.assertEquals("the forward row's seal retries the retire", 3, fault.getRefusedTimelineUnlinks());
                    Assert.assertEquals("and is refused", sealFailuresBefore + 2, instance.getCheckpointSealFailures());
                    Assert.assertEquals("the retry's check fails too", isTheCheckFailing ? 2 : 0, fault.getFailedTimelineChecks());
                    Assert.assertEquals("the retry rewrites no live marker", markerRenamesBefore, fault.getMarkerRenames());
                    assertTimelineRetirePending(instance, generationBefore, Numbers.LONG_NULL);
                    capture.drain();
                    Assert.assertEquals("checks that failed", isTheCheckFailing ? 2 : 0, capture.countOccurrences(KEPT_TIMELINE_CHECK_FAILED));
                    Assert.assertEquals("only the retire that kept the file first logs it", 1, capture.countOccurrences(KEPT_TIMELINE_NEWLY_KEPT));
                    final long resumesBefore = job.getStatsForTest().getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RESUME_FROM_ANCHOR);
                    commitDeparture(job, "('2026-01-01T00:01:30.000000Z', 'a', 0.5)");
                    Assert.assertEquals(
                            "the late row takes no anchor",
                            resumesBefore,
                            job.getStatsForTest().getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RESUME_FROM_ANCHOR)
                    );
                    Assert.assertEquals("the rebuild's retire, then its closing seal's retry of it", 5, fault.getRefusedTimelineUnlinks());
                    Assert.assertEquals("the third refusal", sealFailuresBefore + 3, instance.getCheckpointSealFailures());
                    Assert.assertTrue("pauses the seal", instance.isSealOnCooldown(currentMicros));
                    assertTimelineRetirePending(instance, generationBefore, Numbers.LONG_NULL);
                    assertViewRows(CHANGE_BELOW_EVERY_ROOT_LATE_ROW_ROWS + CHANGE_BELOW_EVERY_ROOT_ROW_0005, DEPARTURE_ROWS_QUERY);
                    assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);

                    fault.unlock();
                    setCurrentMicros(currentMicros + 61_000_000L);
                    commitDeparture(job, "('2026-01-01T00:06:00.000000Z', 'a', 6.0)");
                    Assert.assertFalse(instance.isCheckpointTimelineRetirePending());
                    Assert.assertEquals("the seal past the pause opens a fresh history", 1, generation(instance));
                    assertCheckpointLadder(instance, false, "2026-01-01T00:06:00.000000Z -> 7\n");
                    assertNoRepairMarker(instance);
                }
                // 6.0 + 5.0 + 4.0.
                assertViewRows(
                        CHANGE_BELOW_EVERY_ROOT_LATE_ROW_ROWS + CHANGE_BELOW_EVERY_ROOT_ROW_0005 + "2026-01-01T00:06:00.000000Z\ta\t6.0\t15.0\n",
                        DEPARTURE_ROWS_QUERY
                );
                assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
            });
        } finally {
            capture.stop();
        }
    }

    /**
     * Two refresh workers, each with a timeline writer and reader of its own. The first runs the
     * change below every root, whose retire keeps _timeline under the lock, or, when
     * isTheCheckFailing, cannot check it after the unlink. The second runs the late row and the
     * forward row with the lock held: the retire the first worker ran is the view's, so the second
     * neither finds an anchor in the kept generation nor binds a writer to it, and its seals are
     * refused. The third refusal on the view pauses the seal, and past the pause, with the lock
     * gone, the second worker's next seal opens a fresh history.
     */
    private void assertChangeBelowEveryRootWhoseTimelineStaysLockedIsRefusedOnEveryWorker(boolean isTheCheckFailing) throws Exception {
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (
                    LiveViewRefreshJob first = new LiveViewRefreshJob(0, 2, engine, 1);
                    LiveViewRefreshJob second = new LiveViewRefreshJob(1, 2, engine, 1)
            ) {
                final LiveViewInstance instance = commitFourRoots(first, false);
                final long generationBefore = superblockGeneration(instance);
                final long sealFailuresBefore = instance.getCheckpointSealFailures();
                changeBelowEveryRootUnderALock(first, fault, isTheCheckFailing);
                Assert.assertEquals(sealFailuresBefore + 1, instance.getCheckpointSealFailures());
                Assert.assertNull("the second worker has bound no timeline writer", second.getCheckpointTimelinePurgeStateForTest());

                commitDeparture(second, "('2026-01-01T00:01:30.000000Z', 'a', 0.5)");
                commitDeparture(second, "('2026-01-01T00:05:00.000000Z', 'a', 5.0)");
                Assert.assertEquals(
                        "the second worker's late row takes no anchor",
                        0,
                        second.getStatsForTest().getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RESUME_FROM_ANCHOR)
                );
                Assert.assertEquals("its head-miss rebuild repairs it", 1, countHeadMissRepairs(second));
                Assert.assertNull(
                        "the second worker never binds a writer to the kept generation",
                        second.getCheckpointTimelinePurgeStateForTest()
                );
                Assert.assertEquals("its two seals are refused", sealFailuresBefore + 3, instance.getCheckpointSealFailures());
                Assert.assertTrue("the third refusal on the view pauses the seal", instance.isSealOnCooldown(currentMicros));
                assertTimelineRetirePending(instance, generationBefore, Numbers.LONG_NULL);
                assertViewRows(CHANGE_BELOW_EVERY_ROOT_LATE_ROW_ROWS + CHANGE_BELOW_EVERY_ROOT_ROW_0005, DEPARTURE_ROWS_QUERY);
                assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);

                fault.unlock();
                setCurrentMicros(currentMicros + 61_000_000L);
                commitDeparture(second, "('2026-01-01T00:06:00.000000Z', 'a', 6.0)");
                Assert.assertNotNull("the seal binds the second worker's writer", second.getCheckpointTimelinePurgeStateForTest());
                Assert.assertFalse(instance.isCheckpointTimelineRetirePending());
                Assert.assertEquals("and opens a fresh history", 1, generation(instance));
                assertCheckpointLadder(instance, false, "2026-01-01T00:06:00.000000Z -> 7\n");
            }
            assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
        });
    }

    /**
     * A restart with the lock still held meets the marker the kept retire wrote, which a retire that
     * cannot check _timeline after the unlink, when isTheCheckFailing, writes too: it rebuilds from
     * the applied base rather than restore the kept generation, and the rebuild's own retire meets
     * the lock, so the restarted view refuses the generation too. A late row then finds no anchor.
     * Once the lock goes, the next seal opens a fresh history, which the next restart restores.
     */
    private void assertChangeBelowEveryRootWhoseTimelineStaysLockedRebuildsOnRestart(boolean isTheCheckFailing) throws Exception {
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    commitFourRoots(job, false);
                    changeBelowEveryRootUnderALock(job, fault, isTheCheckFailing);
                }
                restartAndRefresh();
                capture.drain();
                capture.assertLogged("live view restart rebuilding from applied base [view=lv, cause=prefix preservation repair marker present");
                assertRebuiltFromAppliedBase("lv");
                final LiveViewInstance restarted = viewInstance();
                Assert.assertTrue("the rebuild's retire meets the lock too", restarted.isCheckpointTimelineRetirePending());
                assertMarkerNames(Numbers.LONG_NULL);
                assertViewRows(CHANGE_BELOW_EVERY_ROOT_ROWS, DEPARTURE_ROWS_QUERY);
                assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    commitDeparture(job, "('2026-01-01T00:01:30.000000Z', 'a', 0.5)");
                    Assert.assertEquals(0, job.getStatsForTest().getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RESUME_FROM_ANCHOR));
                    assertViewRows(CHANGE_BELOW_EVERY_ROOT_LATE_ROW_ROWS, DEPARTURE_ROWS_QUERY);
                    assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
                    fault.unlock();
                    setCurrentMicros(currentMicros + 61_000_000L);
                    commitDeparture(job, "('2026-01-01T00:06:00.000000Z', 'a', 6.0)");
                    Assert.assertFalse(restarted.isCheckpointTimelineRetirePending());
                    Assert.assertEquals("the next seal opens a fresh history", 1, generation(restarted));
                    assertCheckpointLadder(restarted, false, "2026-01-01T00:06:00.000000Z -> 6\n");
                }
                // 6.0 + 4.0 + 3.0.
                final String rows = CHANGE_BELOW_EVERY_ROOT_LATE_ROW_ROWS + "2026-01-01T00:06:00.000000Z\ta\t6.0\t13.0\n";
                assertViewRows(rows, DEPARTURE_ROWS_QUERY);
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(rows, DEPARTURE_ROWS_QUERY);
                assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
            });
        } finally {
            capture.stop();
        }
    }

    /**
     * The row of the day the base then loses rode in with 00:01, so the oldest root sits at 00:01
     * and the upsert of 00:01 changes a row below every root. Its rebuild keeps the lost row, and
     * its retire keeps _timeline under the lock, or, when isTheCheckFailing, cannot check it after
     * the unlink, with the marker it writes beside it either way. Each restart then rebuilds rather
     * than restore the kept generation, and the restatement guard refuses the rebuild over the day
     * the base lost: the view stops with every row.
     */
    private void assertChangeBelowEveryRootWhoseTimelineStaysLockedStopsTheRestartOverTheDayTheBaseLost(boolean isTheCheckFailing) throws Exception {
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    final LiveViewInstance instance = commitFourRootsOverTheDayTheBaseLoses(job);
                    final long generationBefore = superblockGeneration(instance);
                    changeBelowEveryRootUnderALock(job, fault, isTheCheckFailing);
                    assertTimelineRetirePending(instance, generationBefore, Numbers.LONG_NULL);
                }
                assertViewRows(CHANGE_BELOW_EVERY_ROOT_OVER_THE_LOST_DAY_ROWS, DEPARTURE_ROWS_QUERY);
                assertRestartsBlockedByTheRepairMarker(CHANGE_BELOW_EVERY_ROOT_OVER_THE_LOST_DAY_ROWS, OWED_ROOT_HISTORY_FLOOR);
                capture.drain();
                capture.assertNotLogged(GUARD_STAND_DOWN);
                capture.assertNotLogged("does not match durable materialization");
            });
        } finally {
            capture.stop();
        }
    }

    /**
     * A view whose retire kept _timeline, or, when isTheCheckFailing, could not check it after the
     * unlink, dropped once the lock is gone and created again under the same name: the drop takes
     * the kept file with the view's directory, and the new view refuses nothing and opens a history
     * of its own at its seed's completion.
     */
    private void assertDropOfAViewWhoseRetireKeptTheTimeline(boolean isTheCheckFailing) throws Exception {
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = commitFourRoots(job, false);
                final long generationBefore = superblockGeneration(instance);
                try {
                    changeBelowEveryRootUnderALock(job, fault, isTheCheckFailing);
                } finally {
                    fault.unlock();
                }
                assertTimelineRetirePending(instance, generationBefore, Numbers.LONG_NULL);
                execute("DROP LIVE VIEW lv");
                drainWalQueue();
                driveRefreshToQuiescence(job);
                execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " + DEPARTURE_VIEW_SQL);
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);
                final LiveViewInstance recreated = viewInstance();
                Assert.assertNotSame(instance, recreated);
                Assert.assertFalse(recreated.isCheckpointTimelineRetirePending());
                Assert.assertEquals("the seed's completion seal opens the new view's history", 1, generation(recreated));
                assertCheckpointLadder(recreated, false, "2026-01-01T00:04:00.000000Z -> 4\n");
                assertNoRepairMarker(recreated);
                Assert.assertEquals(0, recreated.getCheckpointSealFailures());
            }
            assertViewRows(CHANGE_BELOW_EVERY_ROOT_ROWS, DEPARTURE_ROWS_QUERY);
            assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
        });
    }

    /**
     * The retire keeps _timeline under the lock, or, when isTheCheckFailing, cannot check it after
     * the unlink, and cannot write the marker beside it: the view refuses the kept generation all
     * the same, and the closing seal's retry of the retire, which meets the lock too, writes the
     * marker. The late row after it finds no anchor.
     */
    private void assertRetireThatKeepsTheTimelineAndCannotWriteItsMarkerStillRefusesIt(boolean isTheCheckFailing) throws Exception {
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    final LiveViewInstance instance = commitFourRoots(job, false);
                    final long generationBefore = superblockGeneration(instance);
                    final AtomicBoolean isMarkerAtTheSeal = new AtomicBoolean(true);
                    final AtomicBoolean isPendingAtTheSeal = new AtomicBoolean();
                    hooks(job).setSimulateHeadMissHeadSealStart(() -> {
                        try (Path dir = checkpointsDir(instance)) {
                            isMarkerAtTheSeal.set(LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir));
                        }
                        isPendingAtTheSeal.set(instance.isCheckpointTimelineRetirePending());
                    });
                    fault.failMarkerWrites(1);
                    try {
                        changeBelowEveryRootUnderALock(job, fault, isTheCheckFailing);
                    } finally {
                        hooks(job).setSimulateHeadMissHeadSealStart(null);
                        fault.failMarkerWrites(0);
                    }
                    Assert.assertEquals("the retire's marker write fails", 1, fault.getRefusedMarkerWrites());
                    Assert.assertTrue("the retire refuses the kept timeline all the same", isPendingAtTheSeal.get());
                    Assert.assertFalse("with no marker beside it", isMarkerAtTheSeal.get());
                    capture.drain();
                    capture.assertLogged(KEPT_TIMELINE_MARKER_WRITE_FAILED);
                    // The closing seal's retry of the retire writes it.
                    assertTimelineRetirePending(instance, generationBefore, Numbers.LONG_NULL);
                    try (Path dir = checkpointsDir(instance)) {
                        Assert.assertEquals(Numbers.LONG_NULL, LiveViewCheckpointRepairMarker.readLvSeqTxn(configuration, dir));
                    }
                    commitDeparture(job, "('2026-01-01T00:01:30.000000Z', 'a', 0.5)");
                    Assert.assertEquals(
                            "the late row takes no anchor",
                            0,
                            job.getStatsForTest().getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RESUME_FROM_ANCHOR)
                    );
                    assertTimelineRetirePending(instance, generationBefore, Numbers.LONG_NULL);
                    fault.unlock();
                }
                assertViewRows(CHANGE_BELOW_EVERY_ROOT_LATE_ROW_ROWS, DEPARTURE_ROWS_QUERY);
                assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
            });
        } finally {
            capture.stop();
        }
    }

    /**
     * Retires the view's timeline through a truncate floored below every root, under a lock on
     * _timeline that keeps the file, so the view refuses it. While it does, a truncate fails at once,
     * ahead of anything it builds, and its catch retires again: once under the lock and once without
     * it. Each of the two builds the paths it unlinks and checks in buffers the job owns, so no
     * {@link Path} stands open at the unlink of _timeline, and nothing allocates native memory from
     * the truncate's start to the check after that unlink: of _timeline under the lock, and of the
     * repair marker, ahead of the directory removals, once the unlink goes through. The view refuses
     * nothing after the last.
     */
    private void assertRetiresWithoutAllocating(LiveViewRefreshJob job, TimelineUnlinkFault fault, LiveViewInstance instance) {
        final long floorTs = ts("2026-01-01T00:00:00.000000Z");
        fault.lock(false);
        Assert.assertFalse("a truncate below every root retires the timeline", job.truncateOrRetireTimelineOnO3ForTest(instance, floorTs));
        Assert.assertEquals("and the lock keeps the file", 1, fault.getRefusedTimelineUnlinks());
        Assert.assertTrue(instance.isCheckpointTimelineRetirePending());
        for (boolean isLocked : new boolean[]{true, false}) {
            if (!isLocked) {
                fault.unlock();
            }
            fault.sampleTheNextRetire();
            final long pathBytes = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_PATH);
            final long allocations = Unsafe.getMallocCount() + Unsafe.getReallocCount();
            Assert.assertFalse("the refused truncate retires again", job.truncateOrRetireTimelineOnO3ForTest(instance, floorTs));
            Assert.assertEquals("no Path stands open at the unlink of _timeline", pathBytes, fault.getPathBytesAtTheUnlink());
            Assert.assertEquals("nothing allocates up to the check after the unlink", allocations, fault.getAllocationsAtTheCheck());
            Assert.assertEquals("the view refuses the timeline while the lock keeps it", isLocked, instance.isCheckpointTimelineRetirePending());
        }
        Assert.assertEquals("the lock refused two unlinks", 2, fault.getRefusedTimelineUnlinks());
    }

    /**
     * Roots at 00:01 to 00:04, then the upsert of 00:01 to 100 under a lock on _timeline for that
     * turn alone, over a base whose designated timestamp has nanosecond precision when isNanos. The
     * rebuild's retire keeps the file, or, when isTheCheckFailing, cannot check it after the unlink,
     * and either way writes a marker naming no generation beside it, and the closing seal retries
     * the retire, meets the lock and is refused. With the lock gone, the next row's seal retries the
     * retire again, removes the kept file and the marker with it, and opens a fresh history. The
     * late row 00:01:30 then finds no root below it in that history, and the rebuild gives what a
     * recompute gives.
     */
    private void assertChangeBelowEveryRootWhoseRetireKeepsTheTimelineOpensAFreshHistory(boolean isNanos, boolean isTheCheckFailing) throws Exception {
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView(isNanos);
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    final LiveViewInstance instance = commitFourRoots(job, isNanos);
                    final long generationBefore = superblockGeneration(instance);
                    final long sealFailuresBefore = instance.getCheckpointSealFailures();
                    final long resetsBefore = instance.getCheckpointTimelineResets();
                    try {
                        changeBelowEveryRootUnderALock(job, fault, isTheCheckFailing);
                    } finally {
                        fault.unlock();
                    }
                    capture.drain();
                    Assert.assertEquals("the retire's check fails", isTheCheckFailing ? 1 : 0, capture.countOccurrences(KEPT_TIMELINE_CHECK_FAILED));
                    Assert.assertEquals("the view refuses the timeline", 1, capture.countOccurrences(KEPT_TIMELINE_NEWLY_KEPT));
                    assertTimelineRetirePending(instance, generationBefore, Numbers.LONG_NULL);
                    try (Path dir = checkpointsDir(instance)) {
                        Assert.assertEquals("nor any seqTxn", Numbers.LONG_NULL, LiveViewCheckpointRepairMarker.readLvSeqTxn(configuration, dir));
                    }
                    Assert.assertEquals("the closing seal is refused", sealFailuresBefore + 1, instance.getCheckpointSealFailures());
                    Assert.assertEquals("the rebuild retires once", resetsBefore + 1, instance.getCheckpointTimelineResets());

                    commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'a', 5.0)");
                    Assert.assertFalse("the seal's retry removed the kept timeline", instance.isCheckpointTimelineRetirePending());
                    Assert.assertEquals("and counts no reset of its own", resetsBefore + 1, instance.getCheckpointTimelineResets());
                    Assert.assertEquals("the seal opens a fresh history", 1, generation(instance));
                    assertCheckpointLadder(instance, isNanos, owedRootText("2026-01-01T00:05:00.000000Z -> 5\n", "2026-01-01", isNanos));
                    assertNoRepairMarker(instance);
                    Assert.assertEquals("and lands", sealFailuresBefore + 1, instance.getCheckpointSealFailures());

                    commitDeparture(job, "('2026-01-01T00:01:30.000000Z', 'a', 0.5)");
                    Assert.assertEquals(
                            "no root of the fresh history sits below the late row",
                            0,
                            job.getStatsForTest().getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RESUME_FROM_ANCHOR)
                    );
                }
                assertViewRows(
                        owedRootText(CHANGE_BELOW_EVERY_ROOT_LATE_ROW_ROWS + CHANGE_BELOW_EVERY_ROOT_ROW_0005, "2026-01-01", isNanos),
                        DEPARTURE_ROWS_QUERY
                );
                assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
            });
        } finally {
            capture.stop();
        }
    }

    /**
     * The change below every root over {@link #commitFourRoots}, or over
     * {@link #commitFourRootsOverTheDayTheBaseLoses} when isDayLost, under a lock on _timeline, with
     * the process stopped at the change's replacement commit, after the retire kept the file, or,
     * when isTheCheckFailing, could not check it after the unlink, and wrote its marker beside it:
     * the view's table still holds the rows from before the change. The restart meets the marker and
     * rebuilds from the applied base, which the restatement guard refuses over the day the base lost.
     */
    private void assertCrashAfterARetireThatKeptTheTimeline(boolean isDayLost, boolean isTheCheckFailing) throws Exception {
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        // s sums x over the row and the two rows of 'a' below it, by hand.
        final String rowsBeforeTheChange = isDayLost
                ? """
                ts\tsym\tx\ts
                2025-12-31T12:00:00.000000Z\ta\t0.5\t0.5
                2026-01-01T00:01:00.000000Z\ta\t1.0\t1.5
                2026-01-01T00:02:00.000000Z\ta\t2.0\t3.5
                2026-01-01T00:03:00.000000Z\ta\t3.0\t6.0
                2026-01-01T00:04:00.000000Z\ta\t4.0\t9.0
                """
                : """
                ts\tsym\tx\ts
                2026-01-01T00:01:00.000000Z\ta\t1.0\t1.0
                2026-01-01T00:02:00.000000Z\ta\t2.0\t3.0
                2026-01-01T00:03:00.000000Z\ta\t3.0\t6.0
                2026-01-01T00:04:00.000000Z\ta\t4.0\t9.0
                """;
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    final LiveViewInstance instance = isDayLost ? commitFourRootsOverTheDayTheBaseLoses(job) : commitFourRoots(job, false);
                    final long generationBefore = superblockGeneration(instance);
                    final LiveViewReplacementCommitWitness witness = new LiveViewReplacementCommitWitness(
                            engine,
                            instance,
                            Numbers.LONG_NULL,
                            LiveViewReplacementCommitWitness.FAIL_CANCELLATION
                    );
                    fault.lock(false);
                    if (isTheCheckFailing) {
                        fault.failTheCheckAfterTheNextTimelineUnlink();
                    }
                    commitAndStopAtTheReplacementCommit(witness, job, "('2026-01-01T00:01:00.000000Z', 'a', 100.0)");
                    Assert.assertEquals("the retire's check of the timeline fails", isTheCheckFailing ? 1 : 0, fault.getFailedTimelineChecks());
                    assertMarkersRecorded(witness, Numbers.LONG_NULL);
                    Assert.assertEquals("the retire meets the lock ahead of the commit", 1, fault.getRefusedTimelineUnlinks());
                    assertTimelineRetirePending(instance, generationBefore, Numbers.LONG_NULL);
                    fault.unlock();
                }
                assertViewRows(rowsBeforeTheChange, DEPARTURE_ROWS_QUERY);
                restartAndRefresh();
                if (isDayLost) {
                    assertRebuildBlockedByTheRepairMarker(rowsBeforeTheChange, OWED_ROOT_HISTORY_FLOOR);
                    assertRestartsBlockedByTheRepairMarker(rowsBeforeTheChange, OWED_ROOT_HISTORY_FLOOR);
                    capture.drain();
                    capture.assertNotLogged(GUARD_STAND_DOWN);
                } else {
                    capture.drain();
                    capture.assertLogged("live view restart rebuilding from applied base [view=lv, cause=prefix preservation repair marker present");
                    assertRebuiltFromAppliedBase("lv");
                    assertViewRows(CHANGE_BELOW_EVERY_ROOT_ROWS, DEPARTURE_ROWS_QUERY);
                    assertViewMatchesRecompute(DEPARTURE_VIEW_SQL);
                }
            });
        } finally {
            capture.stop();
        }
    }

    /**
     * Seeds three rows, then restarts with the newest seed root's data segment torn, under a lock on
     * _timeline: the restarted sweep cannot read that root, so its resume retires the timeline and
     * retires it again to re-sweep from offset 0 behind its skip-write floor, the lock keeping the
     * file both times. Each of the three turns from the abandon on, the abandoning one included,
     * feeds one row and yields, and its seed seal retries the retire and is refused: the sweep keeps
     * no resume point while the lock holds, and nothing publishes over the kept generation. Then the
     * lock goes and the next seed seal opens a fresh history, or, when isRestartedWhileLocked, the
     * process restarts with the lock held, and the restarted resume meets the torn root again. The
     * view completes as a recompute does, and so does it after a restart.
     */
    private void assertSeedSweepOverAKeptTimeline(boolean isRestartedWhileLocked) throws Exception {
        final TimelineUnlinkFault fault = new TimelineUnlinkFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createSeedingView(SEED_ROWS - 2, 2);
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    Assert.assertEquals("each turn seals a seed root", 3, runSeedTurns(job, 3).getSeedCheckpointDataOffset());
                }
                engine.getLiveViewRegistry().clear();
                engine.buildViewGraphs();
                final LiveViewInstance instance = viewInstance();
                corruptNewestRootDataSegment(instance);
                final long generationBefore = superblockGeneration(instance);
                fault.lock(false);
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    for (int turn = 1; turn <= 3; turn++) {
                        runSeedTurns(job, 1);
                        Assert.assertEquals("the abandon's two retires, then one seal per turn", 2 + turn, fault.getRefusedTimelineUnlinks());
                        Assert.assertEquals("no seed root lands", Numbers.LONG_NULL, instance.getSeedCheckpointDataOffset());
                    }
                    assertTimelineRetirePending(instance, generationBefore, Numbers.LONG_NULL);
                    capture.drain();
                    Assert.assertEquals("the torn root abandons the resume", 1, capture.countOccurrences(SEED_RESUME_ABANDONED));
                    Assert.assertEquals("each turn's seed seal is refused", 3, capture.countOccurrences(SEED_SEAL_FAILED));
                    if (!isRestartedWhileLocked) {
                        fault.unlock();
                        runSeedTurns(job, 1);
                        Assert.assertFalse("the seed seal removed the kept timeline", instance.isCheckpointTimelineRetirePending());
                        Assert.assertEquals("and sealed its root in a fresh history", 1, generation(instance));
                        Assert.assertEquals("three rows re-swept under the lock, then the fourth", 4, instance.getSeedCheckpointDataOffset());
                        driveSeedToCompletion(job, "lv");
                    }
                }
                if (isRestartedWhileLocked) {
                    engine.getLiveViewRegistry().clear();
                    engine.buildViewGraphs();
                    final LiveViewInstance restarted = viewInstance();
                    try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                        runSeedTurns(job, 1);
                        capture.drain();
                        Assert.assertEquals("the restarted resume cannot read the kept root either", 2, capture.countOccurrences(SEED_RESUME_ABANDONED));
                        Assert.assertTrue("and its retire keeps the file", restarted.isCheckpointTimelineRetirePending());
                        fault.unlock();
                        driveSeedToCompletion(job, "lv");
                    }
                }
                Assert.assertEquals(LiveViewState.SEED_STATE_ACTIVE, viewInstance().getStateReader().getSeedState());
                assertViewMatchesRecompute(SEED_VIEW_SQL);
                restartAndRefresh();
                assertViewMatchesRecompute(SEED_VIEW_SQL);
            });
        } finally {
            capture.stop();
        }
    }

    /**
     * Upserts 00:01 to 100 over {@link #commitFourRoots} under a lock on _timeline, and leaves the
     * lock on. No root sits below the change, and a ROWS repair over a deduplicating base does not
     * localize, so the head-miss rebuild re-derives the whole view and retires the timeline ahead of
     * its commit, writing no marker of its own: the retire keeps the file, and the closing seal
     * retries the retire and meets the lock as well.
     */
    private void changeBelowEveryRootUnderALock(LiveViewRefreshJob job, TimelineUnlinkFault fault) throws Exception {
        changeBelowEveryRootUnderALock(job, fault, false);
    }

    /**
     * As {@link #changeBelowEveryRootUnderALock(LiveViewRefreshJob, TimelineUnlinkFault)}, with the
     * check of _timeline that follows the rebuild's retire failing when isTheCheckFailing: the retire
     * cannot tell whether the file went, and answers as it does for a file it kept.
     */
    private void changeBelowEveryRootUnderALock(LiveViewRefreshJob job, TimelineUnlinkFault fault, boolean isTheCheckFailing) throws Exception {
        final long headMissRepairsBefore = countHeadMissRepairs(job);
        final int failedChecksBefore = fault.getFailedTimelineChecks();
        fault.lock(false);
        if (isTheCheckFailing) {
            fault.failTheCheckAfterTheNextTimelineUnlink();
        }
        commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'a', 100.0)");
        Assert.assertEquals("the head-miss rebuild repairs the change", headMissRepairsBefore + 1, countHeadMissRepairs(job));
        Assert.assertEquals(
                "the retire's check of the timeline fails",
                failedChecksBefore + (isTheCheckFailing ? 1 : 0),
                fault.getFailedTimelineChecks()
        );
        Assert.assertEquals("the retire, then the closing seal's retry of it", 2, fault.getRefusedTimelineUnlinks());
    }

    /**
     * Over {@link #commitFourRoots}, a truncate floored at 00:03 keeps the roots below it under a
     * marker naming the generation it truncated, live beside the one it publishes. Then, with the
     * marker locked, the upsert of 00:01 to 100: no root sits below the change, so the head-miss
     * rebuild retires the timeline ahead of its commit, which removes _timeline and keeps the
     * marker. Returns the generation the marker names. The lock stays on.
     */
    private long retireOverALiveTruncateMarkerThatKeepsTheMarker(LiveViewRefreshJob job, TimelineUnlinkFault fault) throws Exception {
        final LiveViewInstance instance = viewInstance();
        final long truncatedGeneration = superblockGeneration(instance);
        Assert.assertTrue(
                "the truncate keeps the roots below 00:03",
                job.truncateOrRetireTimelineOnO3ForTest(instance, ts("2026-01-01T00:03:00.000000Z"))
        );
        Assert.assertEquals(truncatedGeneration + 1, superblockGeneration(instance));
        assertMarkerNames(truncatedGeneration);
        final long headMissRepairsBefore = countHeadMissRepairs(job);
        fault.lockMarker(true);
        commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'a', 100.0)");
        Assert.assertEquals("the head-miss rebuild repairs the change", headMissRepairsBefore + 1, countHeadMissRepairs(job));
        return truncatedGeneration;
    }

    // Commits values to tx, the anchored view's base, and drives the refresh to quiescence over it.
    private void commitToTx(LiveViewRefreshJob job, String values) throws Exception {
        execute("INSERT INTO tx VALUES " + values);
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    // The anchored view over tx, whose base holds the rows seed ahead of CREATE, so the view seeds
    // them. account_id carries an index, which a keyed repair reads its account's rows through.
    private void createAnchoredView(CharSequence seed) throws Exception {
        execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL NOCACHE INDEX CAPACITY 8, amount DOUBLE) "
                + "TIMESTAMP(created_at) PARTITION BY HOUR WAL");
        execute("INSERT INTO tx VALUES " + seed);
        drainWalQueue();
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " + ANCHORED_VIEW_SQL);
    }

    /**
     * Under a lock on _timeline, runs a truncate floored below every root the view's timeline
     * holds: it keeps no prefix, so it retires the timeline instead, and the lock keeps the file.
     * The lock stays on.
     */
    private LiveViewInstance keepTheTimelineUnderALock(LiveViewRefreshJob job, TimelineUnlinkFault fault) {
        final LiveViewInstance instance = viewInstance();
        fault.lock(false);
        Assert.assertFalse(
                "a truncate below every root retires the timeline",
                job.truncateOrRetireTimelineOnO3ForTest(instance, ts("2026-01-01T00:00:00.000000Z"))
        );
        Assert.assertEquals("and the lock keeps the file", 1, fault.getRefusedTimelineUnlinks());
        return instance;
    }

    // A root at each of the rows 00:01 to 00:04, x 1 to 4, each sealed by its own commit under the
    // class's row cadence of one, over the view createDepartureView(isNanos) created.
    private LiveViewInstance commitFourRoots(LiveViewRefreshJob job, boolean isNanos) throws Exception {
        driveSeedToCompletion(job, "lv");
        for (int minute = 1; minute <= 4; minute++) {
            commitDeparture(job, "('2026-01-01T00:0" + minute + ":00.000000Z', 'a', " + minute + ".0)");
        }
        final LiveViewInstance instance = viewInstance();
        assertCheckpointLadder(instance, isNanos, owedRootText("""
                2026-01-01T00:01:00.000000Z -> 1
                2026-01-01T00:02:00.000000Z -> 2
                2026-01-01T00:03:00.000000Z -> 3
                2026-01-01T00:04:00.000000Z -> 4
                """, "2026-01-01", isNanos));
        return instance;
    }

    // A root at 00:01 over the row 2025-12-31T12:00 that rides in with it, whose day the base then
    // drops while the view keeps it, and a root at each of 00:02 to 00:04: x 1 to 4.
    private LiveViewInstance commitFourRootsOverTheDayTheBaseLoses(LiveViewRefreshJob job) throws Exception {
        driveSeedToCompletion(job, "lv");
        commitDeparture(job, "('2025-12-31T12:00:00.000000Z', 'a', 0.5), ('2026-01-01T00:01:00.000000Z', 'a', 1.0)");
        execute("ALTER TABLE base DROP PARTITION LIST '2025-12-31'");
        drainWalQueue();
        driveRefreshToQuiescence(job);
        for (int minute = 2; minute <= 4; minute++) {
            commitDeparture(job, "('2026-01-01T00:0" + minute + ":00.000000Z', 'a', " + minute + ".0)");
        }
        final LiveViewInstance instance = viewInstance();
        assertCheckpointLadder(instance, false, """
                2026-01-01T00:01:00.000000Z -> 2
                2026-01-01T00:02:00.000000Z -> 3
                2026-01-01T00:03:00.000000Z -> 4
                2026-01-01T00:04:00.000000Z -> 5
                """);
        return instance;
    }

    // The head-miss departure fixture over a base that lost nothing, roots at 23:55 and 00:59, and
    // the rows 01:00 to 01:05 above it, which the cadence leaves unsealed.
    private LiveViewInstance commitSixUnsealedRowsAboveTheHeadMissRoots(LiveViewRefreshJob job) throws Exception {
        driveSeedToCompletion(job, "lv");
        commitHeadMissDepartureFixture(job, false, 59);
        commitDeparture(
                job,
                "('2026-01-01T01:00:00.000000Z', 'a', 1.0), ('2026-01-01T01:01:00.000000Z', 'a', 1.0), "
                        + "('2026-01-01T01:02:00.000000Z', 'a', 1.0), ('2026-01-01T01:03:00.000000Z', 'a', 1.0), "
                        + "('2026-01-01T01:04:00.000000Z', 'a', 1.0), ('2026-01-01T01:05:00.000000Z', 'a', 1.0)"
        );
        final LiveViewInstance instance = viewInstance();
        assertCheckpointLadder(instance, false, "2025-12-31T23:55:00.000000Z -> 1\n2026-01-01T00:59:00.000000Z -> 61\n");
        return instance;
    }

    // A root at 00:01, then one at 00:03 over the rows 00:02 and 00:03, the newer of which the
    // departure of departWithTheSpliceFailingUnderALock upserts out of the filter.
    private LiveViewInstance commitTwoRootsUnderASplicedDeparture(LiveViewRefreshJob job) throws Exception {
        driveSeedToCompletion(job, "lv");
        commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'a', 1.5)");
        commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', 2.5), ('2026-01-01T00:03:00.000000Z', 'a', 9.5)");
        final LiveViewInstance instance = viewInstance();
        assertCheckpointLadder(instance, false, """
                2026-01-01T00:01:00.000000Z -> 1
                2026-01-01T00:03:00.000000Z -> 3
                """);
        return instance;
    }

    // A view over rows of 'a' a minute apart from 00:01, firstDayRows of them on 2026-01-01 and
    // secondDayRows on 2026-01-02, in a day partition of their own, x numbering them from 1. The base
    // holds them ahead of CREATE, so the view seeds them: under the class's row cadence of one each
    // sweep turn feeds one row and seals a seed root at it.
    private void createSeedingView(int firstDayRows, int secondDayRows) throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
        final StringSink rows = new StringSink();
        for (int row = 1; row <= firstDayRows + secondDayRows; row++) {
            if (row > 1) {
                rows.put(", ");
            }
            final boolean isFirstDay = row <= firstDayRows;
            rows.put(isFirstDay ? "('2026-01-01T00:0" : "('2026-01-02T00:0")
                    .put(isFirstDay ? row : row - firstDayRows)
                    .put(":00.000000Z', 'a', ").put(row).put(".0)");
        }
        execute("INSERT INTO base VALUES " + rows);
        drainWalQueue();
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " + SEED_VIEW_SQL);
    }

    /**
     * Commits the departure of 00:03 over {@link #commitTwoRootsUnderASplicedDeparture}, whose resume
     * from the root at 00:01 splices the root at 00:03, with the splice failing after its data and
     * a lock on _timeline from the replay on: the retire that answers the failed splice keeps the
     * file, and the splice's marker beside it. The lock goes ahead of the closing seal when
     * isReleasedAtTheSeal, and stays otherwise.
     */
    private void departWithTheSpliceFailingUnderALock(
            LiveViewRefreshJob job,
            TimelineUnlinkFault fault,
            boolean isReleasedAtTheSeal
    ) throws Exception {
        final LiveViewInstance instance = viewInstance();
        final long repairFailuresBefore = instance.getCheckpointRepairFailures();
        final long resumesBefore = job.getStatsForTest().getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RESUME_FROM_ANCHOR);
        hooks(job).setSimulateResumeReplayStart(() -> {
            job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_DATA_PUBLISH);
            fault.lock(false);
        });
        hooks(job).setSimulateResumeHeadSealStart(() -> {
            job.setCheckpointTimelineTestFailureStage(0);
            if (isReleasedAtTheSeal) {
                fault.unlock();
            }
        });
        try {
            commitDeparture(job, "('2026-01-01T00:03:00.000000Z', 'a', -9.5)");
        } finally {
            hooks(job).setSimulateResumeReplayStart(null);
            hooks(job).setSimulateResumeHeadSealStart(null);
            job.setCheckpointTimelineTestFailureStage(0);
        }
        Assert.assertEquals(
                "the resume from 00:01 repairs the departure",
                resumesBefore + 1,
                job.getStatsForTest().getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RESUME_FROM_ANCHOR)
        );
        Assert.assertEquals("its splice fails to publish", repairFailuresBefore + 1, instance.getCheckpointRepairFailures());
    }

    /**
     * Commits the upsert of 00:58 to 3.0 over {@link #commitSixUnsealedRowsAboveTheHeadMissRoots}: a
     * value change below the root at 00:59, which the head-miss rebuild's end-of-table splice
     * re-versions, with the splice failing after its metadata, before the superblock names it. The
     * closing seal still appends onto the un-spliced generation, at the frontier 01:05 above every
     * root it keeps, and the tail then retires that generation, under a lock on _timeline taken at
     * the closing seal when isLocked.
     */
    private void failTheHeadMissSpliceUnderALock(LiveViewRefreshJob job, TimelineUnlinkFault fault, boolean isLocked) throws Exception {
        final LiveViewInstance instance = viewInstance();
        final long repairFailuresBefore = instance.getCheckpointRepairFailures();
        final long headMissRepairsBefore = countHeadMissRepairs(job);
        final int markerRenamesBefore = fault.getMarkerRenames();
        fault.afterTheNextMarker(() -> job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH));
        hooks(job).setSimulateHeadMissHeadSealStart(() -> {
            job.setCheckpointTimelineTestFailureStage(0);
            if (isLocked) {
                fault.lock(false);
            }
        });
        try {
            commitDeparture(job, "('2026-01-01T00:58:00.000000Z', 'a', 3.0)");
        } finally {
            hooks(job).setSimulateHeadMissHeadSealStart(null);
            fault.afterTheNextMarker(null);
            job.setCheckpointTimelineTestFailureStage(0);
        }
        Assert.assertEquals("the head-miss rebuild repairs the change", headMissRepairsBefore + 1, countHeadMissRepairs(job));
        Assert.assertEquals("the splice's marker alone", markerRenamesBefore + 1, fault.getMarkerRenames());
        Assert.assertEquals("the splice fails to publish", repairFailuresBefore + 1, instance.getCheckpointRepairFailures());
    }

    // Runs turns sweep turns of the seeding view, each feeding one row under the class's row cadence
    // of one, and returns the view, which still has rows to sweep.
    private LiveViewInstance runSeedTurns(LiveViewRefreshJob job, int turns) {
        for (int turn = 0; turn < turns; turn++) {
            job.run();
            drainWalQueue();
        }
        final LiveViewInstance instance = viewInstance();
        Assert.assertEquals("the sweep has rows left", LiveViewState.SEED_STATE_SEEDING, instance.getStateReader().getSeedState());
        return instance;
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
     * {@link #departWithTheRestampsViewTableReadFailing}, then three publications of the cleared
     * head that fail in a row (base commits 6 to 8), each counted on the seal failure streak, the
     * third of which pauses the seal over the timeline the repair left, and a cycle inside the
     * pause (9), which attempts nothing. The first cycle after the pause (10) publishes the root at
     * the frontier, or, when {@code isFailingAfterThePause}, fails a fourth time, which retires the
     * timeline and releases the base WAL it held: the next cycle past the backoff declines a first
     * root at the newest timestamp the view has seen, and each restart then finds no timeline, so
     * the rebuild from the applied base is refused over the day the base lost.
     */
    private void assertClearedHeadPublicationThatFailsThreeTimesInARow(boolean isFailingAfterThePause) throws Exception {
        configureWalPurge();
        final LiveViewTableReadFault fault = new LiveViewTableReadFault();
        final LogCapture capture = new LogCapture();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    final LiveViewInstance instance = departWithTheRestampsViewTableReadFailing(job, fault, false);
                    final long generationBefore = generation(instance);
                    final long sealFailuresBefore = instance.getCheckpointSealFailures();
                    final long resetsBefore = instance.getCheckpointTimelineResets();
                    job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH);
                    capture.start();
                    try {
                        // seqTxn 6 to 8, with the clock where the departure left it: the head carries
                        // no write time, so each cycle is due.
                        for (int minute = 3; minute <= 5; minute++) {
                            commitDeparture(job, "('2026-01-01T00:0" + minute + ":00.000000Z', 'a', -1.0)");
                            Assert.assertEquals("each publication fails", sealFailuresBefore + minute - 2, instance.getCheckpointSealFailures());
                            Assert.assertEquals(minute - 2, instance.getSealFailureStreak());
                        }
                        Assert.assertTrue("the third failure pauses the seal for a minute", instance.isSealOnCooldown(currentMicros + 59_000_000L));
                        Assert.assertFalse(instance.isSealOnCooldown(currentMicros + 61_000_000L));
                        Assert.assertEquals("and retires nothing", resetsBefore, instance.getCheckpointTimelineResets());
                        Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                        Assert.assertEquals(generationBefore, generation(instance));

                        // seqTxn 9, inside the pause.
                        commitDeparture(job, "('2026-01-01T00:06:00.000000Z', 'a', -1.0)");
                        Assert.assertEquals("the pause holds the publication", sealFailuresBefore + 3, instance.getCheckpointSealFailures());
                        Assert.assertEquals(3, instance.getSealFailureStreak());
                        Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                        Assert.assertEquals(generationBefore, generation(instance));
                        Assert.assertEquals("the timeline holds the base WAL where the repair left it", 5, instance.getCheckpointTimelineWalPurgeFloor());
                        // The purge steps the clock a minute on.
                        Assert.assertEquals(9 - 5, purgeBaseWalAndCountSegments());

                        setCurrentMicros(currentMicros + 61_000_000L);
                        if (!isFailingAfterThePause) {
                            job.setCheckpointTimelineTestFailureStage(0);
                        }
                        // seqTxn 10, the first cycle after the pause.
                        commitDeparture(job, "('2026-01-01T00:07:00.000000Z', 'a', -1.0)");
                    } finally {
                        job.setCheckpointTimelineTestFailureStage(0);
                    }
                    capture.drain();
                    capture.stop();
                    Assert.assertEquals(
                            "every failed publication logs",
                            isFailingAfterThePause ? 4 : 3,
                            capture.countOccurrences("could not advance live view head checkpoint base [view=lv, baseSeqTxn=")
                    );
                    if (isFailingAfterThePause) {
                        capture.assertLogged("live view checkpoint seal keeps failing, retiring the timeline and backing off [view=lv, consecutiveFailures=4");
                        Assert.assertEquals("the fourth failure", sealFailuresBefore + 4, instance.getCheckpointSealFailures());
                        Assert.assertEquals("retires the timeline", resetsBefore + 1, instance.getCheckpointTimelineResets());
                        Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                        assertTimelineAndMarkerRetired(instance);
                        Assert.assertEquals("which holds no base WAL", 0, purgeBaseWalAndCountSegments());
                        Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.CLEARED_HEAD_PUBLICATION_COUNT));

                        // seqTxn 11, past the backoff the retire armed.
                        setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                        commitDeparture(job, "('2026-01-01T00:08:00.000000Z', 'a', -1.0)");
                        Assert.assertEquals(
                                "with no timeline the cycle declines a first root above the newest row",
                                1,
                                job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT)
                        );
                        Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.CLEARED_HEAD_PUBLICATION_COUNT));
                        Assert.assertEquals(Numbers.LONG_NULL, instance.getCheckpointTimelineWalPurgeFloor());
                        Assert.assertEquals(0, purgeBaseWalAndCountSegments());
                    } else {
                        Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.CLEARED_HEAD_PUBLICATION_COUNT));
                        Assert.assertEquals("the first cycle after the pause publishes", ts("2026-01-01T00:01:00.000000Z"), instance.getHeadCheckpointMaxTs());
                        Assert.assertEquals("at its commit", 10, instance.getHeadCheckpointBaseSeqTxn());
                        Assert.assertEquals(generationBefore + 1, generation(instance));
                        Assert.assertEquals("and ends the streak", 0, instance.getSealFailureStreak());
                        Assert.assertEquals(sealFailuresBefore + 3, instance.getCheckpointSealFailures());
                        Assert.assertEquals(resetsBefore, instance.getCheckpointTimelineResets());
                        assertCheckpointLadder(instance, false, RESTAMP_DEPARTURE_LADDER);
                        Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT));
                    }
                    assertNoRefreshFaults("lv");
                }
                if (isFailingAfterThePause) {
                    assertRestartsBlocked(DEPARTURE_ROWS_OVER_THE_LOST_DAY, OWED_ROOT_HISTORY_FLOOR, "timeline is absent");
                } else {
                    for (int restart = 0; restart < 2; restart++) {
                        restartAndRefresh();
                        assertRestoredFromTimeline("lv");
                        assertViewRows(DEPARTURE_ROWS_OVER_THE_LOST_DAY, DEPARTURE_ROWS_QUERY);
                    }
                }
            });
        } finally {
            capture.stop();
        }
    }

    /**
     * {@link #departWithTheRestampsViewTableReadFailing}, then cycles without rows (base commits 6
     * to 8). The first, with the clock where the departure left it, finds the root at the frontier
     * 00:01 and publishes it at its commit, which puts the head back on it with the cycle's write
     * time; the second, inside the cadence that write starts, publishes nothing; the third, past
     * it, republishes the root at its commit as any head at the newest row is, and the base WAL
     * floor follows.
     */
    private void assertDepartureRestampThatTheViewTableReadDeclinesIsPublishedOnTheNextRowlessCycle(boolean isNanos) throws Exception {
        configureWalPurge();
        final String day = "2026-01-01";
        final LiveViewTableReadFault fault = new LiveViewTableReadFault();
        final LogCapture capture = new LogCapture();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView(isNanos);
                final long frontierTs = timestampOf("2026-01-01T00:01:00.000000Z", isNanos);
                final String ladder = owedRootText(RESTAMP_DEPARTURE_LADDER, day, isNanos);
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    final LiveViewInstance instance = departWithTheRestampsViewTableReadFailing(job, fault, isNanos);
                    final long generationBefore = generation(instance);
                    final long sealFailuresBefore = instance.getCheckpointSealFailures();

                    final AtomicLong clockAtTheCycle = new AtomicLong(Numbers.LONG_NULL);
                    hooks(job).setSimulateNoRowHeadCheckpointStart(() -> clockAtTheCycle.set(currentMicros));
                    capture.start();
                    try {
                        // seqTxn 6.
                        commitDeparture(job, "('2026-01-01T00:03:00.000000Z', 'a', -1.0)");
                    } finally {
                        hooks(job).setSimulateNoRowHeadCheckpointStart(null);
                    }
                    capture.drain();
                    capture.stop();
                    Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.CLEARED_HEAD_PUBLICATION_COUNT));
                    capture.assertLogged(owedRootText(
                            "live view put its cleared head checkpoint on the root at its newest row [view=lv, "
                                    + "root=2026-01-01T00:01:00.000000Z, baseSeqTxn=6]",
                            day,
                            isNanos
                    ));
                    Assert.assertEquals("the cycle puts the head back on the root at the frontier", frontierTs, instance.getHeadCheckpointMaxTs());
                    Assert.assertEquals("at its commit", 6, instance.getHeadCheckpointBaseSeqTxn());
                    Assert.assertNotEquals(Numbers.LONG_NULL, clockAtTheCycle.get());
                    Assert.assertEquals("with the cycle's write time", clockAtTheCycle.get(), instance.getLastCheckpointWrittenUs());
                    Assert.assertEquals("one publication", generationBefore + 1, generation(instance));
                    Assert.assertEquals("the older superblock slot holds the truncate", 5, instance.getCheckpointTimelineWalPurgeFloor());
                    assertCheckpointLadder(instance, isNanos, ladder);

                    // seqTxn 7, inside the cadence the publication started.
                    commitDeparture(job, "('2026-01-01T00:04:00.000000Z', 'a', -1.0)");
                    Assert.assertEquals("a cycle inside the cadence publishes nothing", generationBefore + 1, generation(instance));
                    Assert.assertEquals(6, instance.getHeadCheckpointBaseSeqTxn());

                    // seqTxn 8, past it.
                    setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                    commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'a', -1.0)");
                    Assert.assertEquals("the head's own publication", generationBefore + 2, generation(instance));
                    Assert.assertEquals(frontierTs, instance.getHeadCheckpointMaxTs());
                    Assert.assertEquals("at its commit", 8, instance.getHeadCheckpointBaseSeqTxn());
                    Assert.assertEquals("the older superblock slot holds the cleared head's", 6, instance.getCheckpointTimelineWalPurgeFloor());
                    Assert.assertEquals("the purge must keep only the segments above the floor", 8 - 6, purgeBaseWalAndCountSegments());
                    Assert.assertEquals("one publication of a cleared head", 1, job.getStatsForTest().get(LiveViewRefreshStats.CLEARED_HEAD_PUBLICATION_COUNT));
                    Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT));
                    Assert.assertEquals("no publication fails", sealFailuresBefore, instance.getCheckpointSealFailures());
                    assertCheckpointLadder(instance, isNanos, ladder);
                    assertNoRefreshFaults("lv");
                }
                final String rows = owedRootText(DEPARTURE_ROWS_OVER_THE_LOST_DAY, day, isNanos);
                assertViewRows(rows, DEPARTURE_ROWS_QUERY);
                if (isNanos) {
                    for (int restart = 0; restart < 2; restart++) {
                        restartAndRefresh();
                        assertRestoredFromTimeline("lv");
                        assertViewRows(rows, DEPARTURE_ROWS_QUERY);
                    }
                } else {
                    assertRowlessCyclesBetweenTwoRestartsKeepTheTimeline();
                }
            });
        } finally {
            capture.stop();
        }
    }

    /**
     * The departure of the re-stamp cases above, over a base whose designated timestamp has
     * nanosecond precision when {@code isNanos}: the row of the day the base loses (base commits 1
     * and 2), a root at 00:01 (3) and one at 00:02 (4), then the upsert of 00:02 out of the filter
     * (5). The resume truncates the spliced timeline at the new frontier 00:01 and keeps the root
     * there, so its closing seal only re-stamps the head on that root, and the re-stamp's read of
     * the view's table fails, which declines it. The head stays clear with no write time, the
     * marker resolves over the root at the frontier, the older superblock slot holds the splice,
     * and the newest timestamp the view has seen stays at the departed 00:02, with nothing fed
     * since.
     */
    private LiveViewInstance departWithTheRestampsViewTableReadFailing(
            LiveViewRefreshJob job,
            LiveViewTableReadFault fault,
            boolean isNanos
    ) throws Exception {
        driveSeedToCompletion(job, "lv");
        commitTheDayTheBaseLoses(job);
        commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'a', 1.5)");
        commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', 2.5)");
        final LiveViewInstance instance = viewInstance();
        // The resume holds a reader of the base, and none of the view's.
        hooks(job).setSimulateResumeHeadSealStart(() -> fault.failTheNextViewTableRead(engine, instance));
        try {
            commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', -2.5)");
        } finally {
            hooks(job).setSimulateResumeHeadSealStart(null);
        }
        Assert.assertEquals("the re-stamp's read of the view's table fails", 1, fault.getFailedTxnOpens());
        Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT));
        Assert.assertEquals("the head stays clear", Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
        Assert.assertEquals(Numbers.LONG_NULL, instance.getLastCheckpointWrittenUs());
        Assert.assertEquals(timestampOf("2026-01-01T00:02:00.000000Z", isNanos), instance.getLatestSeenTs());
        Assert.assertEquals("nothing fed since", Long.MAX_VALUE, instance.getMinSeenTsSinceCheckpoint());
        assertCheckpointLadder(instance, isNanos, owedRootText(RESTAMP_DEPARTURE_LADDER, "2026-01-01", isNanos));
        Assert.assertEquals(5, instance.getCheckpointTimelineWalPurgeFloor());
        try (Path dir = checkpointsDir(instance)) {
            Assert.assertFalse(
                    "the marker resolves over the root at the frontier",
                    LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir)
            );
        }
        Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.CLEARED_HEAD_PUBLICATION_COUNT));
        Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT));
        return instance;
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
     * when {@code isNanos}, the departure {@link #departAtTheThirdFailureInARow} makes, whose
     * closing seal is the third failure in a row: the seal pauses, keeping the timeline, and the
     * repair keeps its marker, logging the frontier 00:03 and the splice's newest root 00:02 in
     * the view's own unit. A cycle without rows inside the pause attempts nothing and declines
     * nothing. The first one after it seals the table's newest row, 00:03, which lands the owed
     * root, reads the marker as stale and ends the failure streak, and each restart restores over
     * the day the base lost. {@code isPublicationFault} fails each seal ahead of its superblock
     * rather than on the allocation of its data segment.
     */
    private void assertOwedRootOfADepartureAtTheThirdFailureLandsAfterThePause(boolean isNanos, boolean isPublicationFault) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final String day = "2026-01-01";
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        final LogCapture capture = new LogCapture();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView(isNanos);
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                    final long sealFailuresBefore = instance.getCheckpointSealFailures();
                    final long resetsBefore = instance.getCheckpointTimelineResets();
                    capture.start();
                    final long generationBefore = departAtTheThirdFailureInARow(job, fault, instance, isNanos, isPublicationFault);
                    capture.drain();
                    capture.stop();
                    capture.assertLogged("live view checkpoint seal keeps failing, keeping the timeline and backing off "
                            + "[view=lv, consecutiveFailures=3, backoffMicros=60000000]");
                    Assert.assertEquals(
                            1,
                            capture.countOccurrences(owedRootText("live view O3 repair kept its repair marker, no root at the frontier "
                                    + "[view=lv, frontier=2026-01-01T00:03:00.000000Z, spliceHeadRootMaxTs=2026-01-01T00:02:00.000000Z]", day, isNanos))
                    );
                    Assert.assertEquals(0, capture.countOccurrences("retiring the timeline"));

                    final long declinesBefore = job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT);
                    armTheSealFault(job, fault, isPublicationFault);
                    try {
                        commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'b', -1.0)");
                    } finally {
                        disarmTheSealFault(job, fault);
                    }
                    Assert.assertNull("a cycle inside the pause reaches no data segment", fault.getFailedFile());
                    Assert.assertEquals("and attempts no seal", sealFailuresBefore + 3, instance.getCheckpointSealFailures());
                    Assert.assertEquals(
                            "nor declines one",
                            declinesBefore,
                            job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT)
                    );
                    Assert.assertEquals(timestampOf("2026-01-01T00:02:00.000000Z", isNanos), instance.getHeadCheckpointMaxTs());
                    assertRepairMarkerLive(instance, generationBefore);

                    setCurrentMicros(currentMicros + 61_000_000L);
                    commitDeparture(job, "('2026-01-01T00:06:00.000000Z', 'b', -1.0)");
                    Assert.assertEquals(
                            "the first cycle after the pause seals the table's newest row",
                            timestampOf("2026-01-01T00:03:00.000000Z", isNanos),
                            instance.getHeadCheckpointMaxTs()
                    );
                    Assert.assertEquals("at its commit", 10, instance.getHeadCheckpointBaseSeqTxn());
                    Assert.assertEquals("the splice and the owed root", generationBefore + 2, generation(instance));
                    assertRepairMarkerStale(instance, generationBefore);
                    assertCheckpointLadder(instance, isNanos, owedRootText(OWED_ROOT_DEPARTED_FRONTIER_LADDER, day, isNanos));
                    Assert.assertEquals("the root that lands ends the streak", 0, instance.getSealFailureStreak());
                    Assert.assertFalse(instance.isSealOnCooldown(currentMicros));
                    Assert.assertEquals(sealFailuresBefore + 3, instance.getCheckpointSealFailures());
                    Assert.assertEquals(resetsBefore, instance.getCheckpointTimelineResets());
                    Assert.assertEquals(
                            declinesBefore,
                            job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT)
                    );
                    assertNoRefreshFaults("lv");
                }
                for (int restart = 0; restart < 2; restart++) {
                    restartAndRefresh();
                    assertRestoredFromTimeline("lv");
                    assertViewRows(owedRootText(OWED_ROOT_DEPARTED_FRONTIER_ROWS, day, isNanos), DEPARTURE_ROWS_QUERY);
                }
            });
        } finally {
            capture.stop();
        }
    }

    /**
     * Over the owed-root fixture, on a base whose designated timestamp has nanosecond precision
     * when {@code isNanos}: an upsert moves the unsealed row 00:04 out of the filter (base commit
     * 6), and the closing seal of the resume that repairs it, which owes the new frontier 00:03 a
     * root, fails. A cycle without rows inside the cadence (7) attempts no seal. The first one past
     * it (8) seals the table's newest row, 00:03, rather than declining a root at 00:04, the newest
     * timestamp the view has seen: the owed root lands, reads the marker as stale and leaves the
     * splice's commit 6 in the older superblock slot. Each cycle past the cadence after it
     * republishes that root at its own commit, and each restart restores over the day the base
     * lost.
     */
    private void assertResumeOwedRootAboveTheNewestRowLandsOnARowlessCycle(boolean isNanos) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final String day = "2026-01-01";
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView(isNanos);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                final long generationBefore = generation(instance);
                final long sealFailuresBefore = instance.getCheckpointSealFailures();
                final long parkedHeadTs = timestampOf("2026-01-01T00:02:00.000000Z", isNanos);
                final long frontierTs = timestampOf("2026-01-01T00:03:00.000000Z", isNanos);
                failTheOwedResumeSeal(job, fault, LiveViewCheckpointDiskFullFault.OP_ALLOCATE, "('2026-01-01T00:04:00.000000Z', 'a', -1.0)");
                Assert.assertEquals(timestampOf("2026-01-01T00:04:00.000000Z", isNanos), instance.getLatestSeenTs());
                Assert.assertEquals("the failed seal leaves the head parked", parkedHeadTs, instance.getHeadCheckpointMaxTs());
                assertRepairMarkerLive(instance, generationBefore);

                fault.arm(LiveViewCheckpointDiskFullFault.OP_ALLOCATE, LiveViewCheckpointLayout.DATA_DIR_NAME, 1);
                commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'b', -1.0)");
                fault.disarm();
                Assert.assertNull("a cycle inside the cadence attempts no seal", fault.getFailedFile());
                Assert.assertEquals(parkedHeadTs, instance.getHeadCheckpointMaxTs());
                assertRepairMarkerLive(instance, generationBefore);

                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                commitDeparture(job, "('2026-01-01T00:06:00.000000Z', 'b', -1.0)");
                Assert.assertEquals("the cycle seals the table's newest row", frontierTs, instance.getHeadCheckpointMaxTs());
                Assert.assertEquals("at its commit", 8, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals("the splice and the owed root", generationBefore + 2, generation(instance));
                assertRepairMarkerStale(instance, generationBefore);
                final String ladder = owedRootText(OWED_ROOT_DEPARTED_FRONTIER_LADDER, day, isNanos);
                assertCheckpointLadder(instance, isNanos, ladder);
                Assert.assertEquals("the older superblock slot holds the splice", 6, instance.getCheckpointTimelineWalPurgeFloor());
                Assert.assertEquals("the closing seal alone failed", sealFailuresBefore + 1, instance.getCheckpointSealFailures());
                Assert.assertEquals(0, instance.getSealFailureStreak());
                Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT));
                Assert.assertEquals(
                        "rows fed above a parked head take the seal",
                        0,
                        job.getStatsForTest().get(LiveViewRefreshStats.CLEARED_HEAD_PUBLICATION_COUNT)
                );

                for (int minute = 7; minute <= 8; minute++) {
                    final long generation = generation(instance);
                    final long headBaseSeqTxn = instance.getHeadCheckpointBaseSeqTxn();
                    setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                    commitDeparture(job, "('2026-01-01T00:0" + minute + ":00.000000Z', 'b', -1.0)");
                    Assert.assertEquals("one publication per cycle", generation + 1, generation(instance));
                    Assert.assertEquals(frontierTs, instance.getHeadCheckpointMaxTs());
                    Assert.assertEquals("the head moves to the cycle's commit", minute + 2, instance.getHeadCheckpointBaseSeqTxn());
                    Assert.assertEquals("the WAL floor follows it", headBaseSeqTxn, instance.getCheckpointTimelineWalPurgeFloor());
                    assertCheckpointLadder(instance, isNanos, ladder);
                }
                assertNoRefreshFaults("lv");
            }
            for (int restart = 0; restart < 2; restart++) {
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(owedRootText(OWED_ROOT_DEPARTED_FRONTIER_ROWS, day, isNanos), DEPARTURE_ROWS_QUERY);
            }
        });
    }

    /**
     * Over the owed-root fixture: the departure of 00:04 (base commit 6), whose closing seal fails,
     * then a cycle without rows past the cadence (7) whose seal of the table's newest row fails too,
     * on the allocation of its data segment or, when {@code isPublicationFault}, ahead of its
     * superblock: the second failure in a row, which leaves the head, both base WAL arms and the
     * live marker as they were. The failed seal leaves the cadence due, so the next cycle (8),
     * with the clock where it was, lands the owed root and ends the streak, and each restart
     * restores over the day the base lost.
     */
    private void assertResumeOwedRootAboveTheNewestRowWhoseLandingFailsOnceLandsOnTheNextDueCycle(boolean isPublicationFault) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createDepartureView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                final long generationBefore = generation(instance);
                final long resetsBefore = instance.getCheckpointTimelineResets();
                failTheOwedResumeSeal(job, fault, LiveViewCheckpointDiskFullFault.OP_ALLOCATE, "('2026-01-01T00:04:00.000000Z', 'a', -1.0)");
                final long sealFailuresBefore = instance.getCheckpointSealFailures();
                Assert.assertEquals(4, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(4, instance.getCheckpointTimelineWalPurgeFloor());

                hooks(job).setSimulateNoRowHeadCheckpointStart(() -> armTheSealFault(job, fault, isPublicationFault));
                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                try {
                    commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'b', -1.0)");
                } finally {
                    hooks(job).setSimulateNoRowHeadCheckpointStart(null);
                    disarmTheSealFault(job, fault);
                }
                if (!isPublicationFault) {
                    Assert.assertNotNull("the cycle's seal must reach its data segment", fault.getFailedFile());
                }
                Assert.assertEquals("the cycle's seal fails", sealFailuresBefore + 1, instance.getCheckpointSealFailures());
                Assert.assertEquals("the second failure in a row", 2, instance.getSealFailureStreak());
                Assert.assertEquals("retires nothing", resetsBefore, instance.getCheckpointTimelineResets());
                Assert.assertEquals(ts("2026-01-01T00:02:00.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(4, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(4, instance.getCheckpointTimelineWalPurgeFloor());
                assertRepairMarkerLive(instance, generationBefore);

                commitDeparture(job, "('2026-01-01T00:06:00.000000Z', 'b', -1.0)");
                Assert.assertEquals("the next cycle lands the owed root", ts("2026-01-01T00:03:00.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals("at its commit", 8, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals("the older superblock slot holds the splice", 6, instance.getCheckpointTimelineWalPurgeFloor());
                Assert.assertEquals("the splice and the owed root", generationBefore + 2, generation(instance));
                assertRepairMarkerStale(instance, generationBefore);
                assertCheckpointLadder(instance, false, OWED_ROOT_DEPARTED_FRONTIER_LADDER);
                Assert.assertEquals("the seal that lands is no failure", sealFailuresBefore + 1, instance.getCheckpointSealFailures());
                Assert.assertEquals("and ends the streak", 0, instance.getSealFailureStreak());
                assertNoRefreshFaults("lv");
            }
            for (int restart = 0; restart < 2; restart++) {
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(OWED_ROOT_DEPARTED_FRONTIER_ROWS, DEPARTURE_ROWS_QUERY);
            }
        });
    }

    /**
     * Over the head-miss owed-root fixture, on a base whose designated timestamp has nanosecond
     * precision when {@code isNanos} (base commits 1 to 5): one commit (6) carries the late row
     * 00:49:30 and an upsert of the newest row 00:55 out of the filter. The head-miss rebuild's
     * end-of-table splice keeps its newest root at 00:50, below the new frontier 00:54, clears the
     * head, and its closing seal fails on its data segment, which leaves the marker live and the
     * older superblock slot at the root at 00:50's commit 4. The head is clear, so the first cycle
     * without rows (7) is due with the clock where it was, and seals the table's newest row, 00:54:
     * the owed root lands in the place of the root at 00:50, reads the marker as stale and moves the
     * WAL floor to the splice's commit, and each restart restores over the day the base lost.
     */
    private void assertHeadMissOwedRootAboveTheNewestRowLandsOnTheFirstRowlessCycle(boolean isNanos) throws Exception {
        configureWalPurge();
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final String day = "2026-01-01";
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            createHeadMissDepartureView(isNanos);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = commitFiveRowsAboveTheHeadMissRoot(job, isNanos);
                final long generationBefore = generation(instance);
                final long headMissRepairsBefore = countHeadMissRepairs(job);
                final long sealFailuresBefore = instance.getCheckpointSealFailures();
                hooks(job).setSimulateHeadMissHeadSealStart(
                        () -> fault.arm(LiveViewCheckpointDiskFullFault.OP_ALLOCATE, LiveViewCheckpointLayout.DATA_DIR_NAME, 1)
                );
                commitDeparture(job, "('2026-01-01T00:49:30.000000Z', 'a', 1.0), ('2026-01-01T00:55:00.000000Z', 'a', -1.0)");
                hooks(job).setSimulateHeadMissHeadSealStart(null);
                fault.disarm();
                Assert.assertEquals("the head-miss rebuild repairs the commit", headMissRepairsBefore + 1, countHeadMissRepairs(job));
                Assert.assertNotNull("the closing seal must reach its data segment", fault.getFailedFile());
                Assert.assertEquals(sealFailuresBefore + 1, instance.getCheckpointSealFailures());
                Assert.assertEquals("the rebuild cleared the head", Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(timestampOf("2026-01-01T00:55:00.000000Z", isNanos), instance.getLatestSeenTs());
                assertRepairMarkerLive(instance, generationBefore);
                final String splicedLadder = owedRootText("2025-12-31T23:55:00.000000Z -> 1\n2026-01-01T00:50:00.000000Z -> 53\n", day, isNanos);
                assertCheckpointLadder(instance, isNanos, splicedLadder);
                Assert.assertEquals("the older superblock slot holds the root at 00:50", 4, instance.getCheckpointTimelineWalPurgeFloor());
                Assert.assertEquals("the purge keeps every commit above it", 6 - 4, purgeBaseWalAndCountSegments());

                commitDeparture(job, "('2026-01-01T00:56:00.000000Z', 'b', -1.0)");
                Assert.assertEquals(
                        "the cycle seals the table's newest row",
                        timestampOf("2026-01-01T00:54:00.000000Z", isNanos),
                        instance.getHeadCheckpointMaxTs()
                );
                Assert.assertEquals("at its commit", 7, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals("the splice and the owed root", generationBefore + 2, generation(instance));
                assertRepairMarkerStale(instance, generationBefore);
                // The replay fed the late row 00:49:30, below the root at 00:50, so the batch minimum
                // the seal hands the writer reads that root's timestamp group as grown since it was
                // sealed, and the root at 00:54 takes its place.
                assertCheckpointLadder(
                        instance,
                        isNanos,
                        owedRootText("2025-12-31T23:55:00.000000Z -> 1\n2026-01-01T00:54:00.000000Z -> 57\n", day, isNanos)
                );
                Assert.assertEquals("the older superblock slot holds the splice", 6, instance.getCheckpointTimelineWalPurgeFloor());
                Assert.assertEquals("the purge keeps the commit above it", 7 - 6, purgeBaseWalAndCountSegments());
                Assert.assertEquals(sealFailuresBefore + 1, instance.getCheckpointSealFailures());
                Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT));
                Assert.assertEquals(
                        "rows the rebuild fed since it cleared the head take the seal",
                        0,
                        job.getStatsForTest().get(LiveViewRefreshStats.CLEARED_HEAD_PUBLICATION_COUNT)
                );
                assertNoRefreshFaults("lv");
            }
            for (int restart = 0; restart < 2; restart++) {
                restartAndRefresh();
                assertRestoredFromTimeline("lv");
                assertViewRows(owedRootText(headMissRowsWithTheLateRow(54), day, isNanos), DEPARTURE_ROWS_QUERY);
            }
        });
    }

    /**
     * Over the owed-root fixture, past the duration cadence: two cycles without rows whose seals at
     * the frontier 00:04 fail (base commits 6 and 7), then an upsert of 00:04 out of the filter (8),
     * whose resume owes a root at the new frontier 00:03 and whose closing seal fails too, the
     * third failure in a row. The seal pauses for a minute and keeps the timeline, and the repair
     * keeps its marker, with the head parked on 00:02 at commit 4. Each seal fails on the
     * allocation of its data segment or, when {@code isPublicationFault}, ahead of its superblock,
     * over a base whose designated timestamp has nanosecond precision when {@code isNanos}. Returns
     * the generation the marker names.
     */
    private long departAtTheThirdFailureInARow(
            LiveViewRefreshJob job,
            LiveViewCheckpointDiskFullFault fault,
            LiveViewInstance instance,
            boolean isNanos,
            boolean isPublicationFault
    ) throws Exception {
        final long sealFailuresBefore = instance.getCheckpointSealFailures();
        final long resetsBefore = instance.getCheckpointTimelineResets();
        setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
        for (int second = 10; second <= 20; second += 10) {
            armTheSealFault(job, fault, isPublicationFault);
            try {
                commitDeparture(job, "('2026-01-01T00:04:" + second + ".000000Z', 'b', -1.0)");
            } finally {
                disarmTheSealFault(job, fault);
            }
            if (!isPublicationFault) {
                Assert.assertNotNull("the seal at the frontier must reach its data segment", fault.getFailedFile());
            }
        }
        Assert.assertEquals("the seals at the frontier fail", sealFailuresBefore + 2, instance.getCheckpointSealFailures());
        final long generationBefore = generation(instance);
        hooks(job).setSimulateResumeHeadSealStart(() -> armTheSealFault(job, fault, isPublicationFault));
        try {
            commitDeparture(job, "('2026-01-01T00:04:00.000000Z', 'a', -1.0)");
        } finally {
            hooks(job).setSimulateResumeHeadSealStart(null);
            disarmTheSealFault(job, fault);
        }
        if (!isPublicationFault) {
            Assert.assertNotNull("the closing seal must reach its data segment", fault.getFailedFile());
        }
        Assert.assertEquals("the closing seal fails, the third failure in a row", sealFailuresBefore + 3, instance.getCheckpointSealFailures());
        Assert.assertEquals("which pauses the seal and retires nothing", resetsBefore, instance.getCheckpointTimelineResets());
        Assert.assertTrue("the pause holds a minute", instance.isSealOnCooldown(currentMicros + 59_000_000L));
        Assert.assertFalse(instance.isSealOnCooldown(currentMicros + 61_000_000L));
        Assert.assertEquals("the head stays parked", timestampOf("2026-01-01T00:02:00.000000Z", isNanos), instance.getHeadCheckpointMaxTs());
        Assert.assertEquals(4, instance.getHeadCheckpointBaseSeqTxn());
        Assert.assertEquals(timestampOf("2026-01-01T00:04:00.000000Z", isNanos), instance.getLatestSeenTs());
        assertTimelineOnDisk(instance, true);
        assertRepairMarkerLive(instance, generationBefore);
        return generationBefore;
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
     * The first late row of the owed-root fixture, 00:01:30, with its closing seal failed, then a
     * second, 00:01:45, whose resume writes its marker over the first one's live marker and whose
     * replacement commit fails, over a base whose designated timestamp has nanosecond precision
     * when {@code isNanos}. The marker records no seqTxn, so the in-place recovery declines the
     * timeline, and the rebuild from the applied base is refused over the day the base lost, in
     * place and on each restart: the view keeps every row it held, and the guard never stands down.
     */
    private void assertASecondRepairWhoseCommitFailsInsideTheOwedWindowKeepsEveryRow(boolean isNanos) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final String day = "2026-01-01";
        final String rows = owedRootText(OWED_ROOT_ROWS, day, isNanos);
        final String historyFloor = owedRootText(OWED_ROOT_HISTORY_FLOOR, day, isNanos);
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView(isNanos);
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                    final long generationBefore = generation(instance);
                    failTheOwedResumeSeal(job, fault, LiveViewCheckpointDiskFullFault.OP_ALLOCATE, "('2026-01-01T00:01:30.000000Z', 'a', 1.0)");
                    assertRepairMarkerLive(instance, generationBefore);
                    final long lvSeqTxnBefore = engine.getTableSequencerAPI().lastTxn(instance.getLiveViewToken());
                    final LiveViewReplacementCommitWitness witness = new LiveViewReplacementCommitWitness(
                            engine,
                            instance,
                            generationBefore + 1,
                            LiveViewReplacementCommitWitness.FAIL_CRITICAL
                    );
                    commitDepartureUnder(witness, job, "('2026-01-01T00:01:45.000000Z', 'a', 1.0)");
                    assertMarkersRecorded(witness, Numbers.LONG_NULL);
                    Assert.assertEquals("the marker lands ahead of the replacement commit", lvSeqTxnBefore, witness.getViewLvSeqTxn(0));
                    Assert.assertEquals("the replacement never commits", lvSeqTxnBefore, engine.getTableSequencerAPI().lastTxn(instance.getLiveViewToken()));
                    Assert.assertEquals(1, instance.getRefreshFaultCount());
                    Assert.assertEquals("nothing restores behind the live marker", 0, instance.getCheckpointRuntimeRestores());
                    assertMidDrainRebuildBlocked(instance, historyFloor);
                    Assert.assertEquals("the second splice never published", generationBefore + 1, generation(instance));
                    assertMarkerNames(generationBefore + 1);
                }
                capture.drain();
                capture.assertLogged(runtimeRestoreDeclinedOverTheMarker(MID_DRAIN_CAUSE));
                assertViewRows(rows, DEPARTURE_ROWS_QUERY);
                assertRestartsBlockedByTheRepairMarker(rows, historyFloor);
                capture.drain();
                capture.assertNotLogged(GUARD_STAND_DOWN);
            });
        } finally {
            capture.stop();
        }
    }

    /**
     * The first late row of the owed-root fixture, 00:01:30, with its closing seal failed, then a
     * second, 00:01:45, whose repair cannot read the live marker it would write over: the
     * read-write open of {@code _repairing} fails once. With {@code isTruncating} the chain is
     * declined, so the read is the truncate's ahead of the replacement commit, and otherwise the
     * splice marker's. The repair logs {@code expectedLog} and retires the timeline, the live marker
     * with it, ahead of its commit; the commit lands, and the closing seal opens a fresh history at
     * the frontier 00:04, which each restart restores every row from.
     */
    private void assertASecondRepairThatCannotReadTheMarkerItReplacesRetiresTheTimeline(
            boolean isTruncating,
            String expectedLog
    ) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final RepairMarkerOpenFault fault = new RepairMarkerOpenFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(fault, () -> {
                createDepartureView();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    final LiveViewInstance instance = commitTwoRowsAboveTheNewestRoot(job);
                    final long generationBefore = generation(instance);
                    failTheOwedResumeSealAtItsPublication(job, "('2026-01-01T00:01:30.000000Z', 'a', 1.0)");
                    assertRepairMarkerLive(instance, generationBefore);
                    if (isTruncating) {
                        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 0);
                    }
                    final long resumesBefore = job.getStatsForTest().getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RESUME_FROM_ANCHOR);
                    final long resetsBefore = instance.getCheckpointTimelineResets();
                    fault.arm();
                    try {
                        commitDeparture(job, "('2026-01-01T00:01:45.000000Z', 'a', 1.0)");
                    } finally {
                        fault.disarm();
                    }
                    Assert.assertEquals("the repair reads the marker it would write over", 1, fault.getRefusedOpens());
                    Assert.assertEquals(
                            "the resume from the root at 00:01 repairs the late row",
                            resumesBefore + 1,
                            job.getStatsForTest().getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RESUME_FROM_ANCHOR)
                    );
                    Assert.assertEquals("the repair retires the timeline", resetsBefore + 1, instance.getCheckpointTimelineResets());
                    Assert.assertEquals("the closing seal opens a fresh history", 1, generation(instance));
                    Assert.assertEquals("at the frontier", ts("2026-01-01T00:04:00.000000Z"), instance.getHeadCheckpointMaxTs());
                    assertNoRepairMarker(instance);
                    assertNoRefreshFaults("lv");
                    assertViewRows(OWED_ROOT_SECOND_LATE_ROW_ROWS, DEPARTURE_ROWS_QUERY);
                }
                capture.drain();
                capture.assertLogged(expectedLog);
                for (int restart = 0; restart < 2; restart++) {
                    restartAndRefresh();
                    assertRestoredFromTimeline("lv");
                    assertViewRows(OWED_ROOT_SECOND_LATE_ROW_ROWS, DEPARTURE_ROWS_QUERY);
                }
            });
        } finally {
            capture.stop();
        }
    }

    // The seqTxn each marker the witness read recorded, in commit order.
    private static void assertMarkersRecorded(LiveViewReplacementCommitWitness witness, long... lvSeqTxns) {
        Assert.assertEquals("the commits made under the marker", lvSeqTxns.length, witness.size());
        for (int i = 0; i < lvSeqTxns.length; i++) {
            Assert.assertEquals("the seqTxn the marker records at commit " + i, lvSeqTxns[i], witness.getRecordedLvSeqTxn(i));
        }
    }

    // The in-place recovery after a faulted refresh turn declined to restore, rebuilt from the
    // applied base, and the restatement guard refused the rebuild on historyFloor: the view stops
    // with every row it holds, and writes no invalidation.
    private static void assertMidDrainRebuildBlocked(LiveViewInstance instance, String historyFloor) {
        Assert.assertTrue("the refused rebuild stops the view", instance.isCheckpointRecoveryBlocked());
        Assert.assertFalse(instance.isInvalid());
        TestUtils.assertContains(
                instance.getCheckpointRecoveryReason(),
                "would drop rows it retains [cause=" + MID_DRAIN_CAUSE + "]: " + historyFloor
        );
    }

    // The checkpoint directory the crash image holds: the superblock at superblockGeneration, under
    // a repair marker naming markerGeneration and recording markerLvSeqTxn.
    private static void assertTheCrashImage(long superblockGeneration, long markerGeneration, long markerLvSeqTxn) {
        try (
                Path crashImage = crashImageDir();
                LiveViewCheckpointSuperblock superblock = new LiveViewCheckpointSuperblock(configuration)
        ) {
            superblock.of(crashImage);
            Assert.assertTrue(superblock.isValid());
            Assert.assertEquals("the truncate has not published", superblockGeneration, superblock.generation);
            Assert.assertEquals(markerGeneration, LiveViewCheckpointRepairMarker.readBaseGeneration(configuration, crashImage));
            Assert.assertEquals(markerLvSeqTxn, LiveViewCheckpointRepairMarker.readLvSeqTxn(configuration, crashImage));
        }
    }

    // What a running recovery logs when a live repair marker sends it to the rebuild from the
    // applied base, for cause.
    private static String runtimeRestoreDeclinedOverTheMarker(String cause) {
        return "live view cannot restore its runtime from the checkpoint timeline, rebuilding from the applied base [view=lv, cause="
                + cause + ", reason=prefix preservation repair marker present]";
    }

    /**
     * Commits {@code values} with {@code witness} on the role-switch mint seam and runs refresh
     * passes only until it fails a commit with a cancellation: that turn committed nothing past
     * the marker and ran no recovery, so the process stops there, as a crash at that commit would.
     */
    private void commitAndStopAtTheReplacementCommit(LiveViewReplacementCommitWitness witness, LiveViewRefreshJob job, String values) throws Exception {
        execute("INSERT INTO base VALUES " + values);
        drainWalQueue();
        CairoEngine.setRoleSwitchMintObserver(witness::onMint);
        try {
            for (int pass = 0; pass < REFRESH_QUIESCENCE_PASSES && !witness.hasFailed(); pass++) {
                setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                drainWalQueue();
                job.processNotificationsForTest();
            }
        } finally {
            CairoEngine.setRoleSwitchMintObserver(null);
        }
        Assert.assertTrue("the repair must reach its replacement commit", witness.hasFailed());
    }

    // Commits values and drives the refresh to quiescence with witness on the role-switch mint seam.
    private void commitDepartureUnder(LiveViewReplacementCommitWitness witness, LiveViewRefreshJob job, String values) throws Exception {
        CairoEngine.setRoleSwitchMintObserver(witness::onMint);
        try {
            commitDeparture(job, values);
        } finally {
            CairoEngine.setRoleSwitchMintObserver(null);
        }
        Assert.assertTrue("the repair must reach its replacement commit", witness.size() > 0);
    }

    /**
     * Commits {@code values}, a late row the resume from the owed-root fixture's root at 00:01
     * repairs, with the writer stage that fails a publication ahead of its superblock set for the
     * resume's closing seal: the seal fails once and the splice's marker stays live. The case's
     * files facade is free for a witness of its own.
     */
    private void failTheOwedResumeSealAtItsPublication(LiveViewRefreshJob job, String values) throws Exception {
        final LiveViewInstance instance = viewInstance();
        final long resumesBefore = job.getStatsForTest().getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RESUME_FROM_ANCHOR);
        final long sealFailuresBefore = instance.getCheckpointSealFailures();
        final long resetsBefore = instance.getCheckpointTimelineResets();
        hooks(job).setSimulateResumeHeadSealStart(
                () -> job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH)
        );
        try {
            commitDeparture(job, values);
        } finally {
            hooks(job).setSimulateResumeHeadSealStart(null);
            job.setCheckpointTimelineTestFailureStage(0);
        }
        Assert.assertEquals(
                "the resume from an anchor repairs the correction",
                resumesBefore + 1,
                job.getStatsForTest().getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RESUME_FROM_ANCHOR)
        );
        Assert.assertEquals(sealFailuresBefore + 1, instance.getCheckpointSealFailures());
        Assert.assertEquals("one failure retires nothing", resetsBefore, instance.getCheckpointTimelineResets());
    }

    /**
     * Commits the late row 00:49:45 over the head-miss owed-root fixture whose first late row's
     * closing seal failed, and stops the process at its replacement commit: the head-miss
     * rebuild's marker, written over the live one, names the splice's generation and records no
     * seqTxn, and nothing moved past it.
     */
    private void stopAtTheSecondHeadMissCommit(LiveViewRefreshJob job, LiveViewInstance instance, long generationBefore) throws Exception {
        final long lvSeqTxnBefore = engine.getTableSequencerAPI().lastTxn(instance.getLiveViewToken());
        final LiveViewReplacementCommitWitness witness = new LiveViewReplacementCommitWitness(
                engine,
                instance,
                generationBefore + 1,
                LiveViewReplacementCommitWitness.FAIL_CANCELLATION
        );
        commitAndStopAtTheReplacementCommit(witness, job, "('2026-01-01T00:49:45.000000Z', 'a', 1.0)");
        assertMarkersRecorded(witness, Numbers.LONG_NULL);
        Assert.assertEquals("the replacement never commits", lvSeqTxnBefore, engine.getTableSequencerAPI().lastTxn(instance.getLiveViewToken()));
        Assert.assertEquals("the splice never publishes", generationBefore + 1, generation(instance));
        assertMarkerNames(generationBefore + 1);
        Assert.assertEquals("no recovery runs", 0, instance.getCheckpointRuntimeRestores());
        Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
    }

    /**
     * Commits {@code values}, a late row whose resume truncates the timeline ahead of its
     * replacement commit, copies the view's checkpoint directory to the crash image right after
     * the truncate's marker lands, before the truncate publishes, and stops the process at the
     * replacement commit, which the marker names {@code markerGeneration} for: nothing outside the
     * image moved past the marker either. {@code markers} records the turn's marker.
     */
    private void stopAfterTheTruncateMarker(
            RepairMarkerWitness markers,
            LiveViewRefreshJob job,
            LiveViewInstance instance,
            long markerGeneration,
            String values
    ) throws Exception {
        final long lvSeqTxnBefore = engine.getTableSequencerAPI().lastTxn(instance.getLiveViewToken());
        final LiveViewReplacementCommitWitness witness = new LiveViewReplacementCommitWitness(
                engine,
                instance,
                markerGeneration,
                LiveViewReplacementCommitWitness.FAIL_CANCELLATION
        );
        try (Path checkpoints = checkpointsDir(instance).slash(); Path crashImage = crashImageDir()) {
            markers.clear();
            markers.afterTheNextMarker(() -> TestUtils.copyDirectory(checkpoints, crashImage, configuration.getMkDirMode()));
            commitAndStopAtTheReplacementCommit(witness, job, values);
        }
        Assert.assertTrue("the crash image must be taken", markers.size() > 0);
        Assert.assertEquals("the truncate publishes ahead of the commit", markerGeneration + 1, generation(instance));
        Assert.assertEquals("the replacement never commits", lvSeqTxnBefore, engine.getTableSequencerAPI().lastTxn(instance.getLiveViewToken()));
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
     * Over the day the base loses when {@code isDayLost}, an anchored view's rows of a at
     * 01-01T23:57, then 23:58 with 01-02T00:01 in one commit, then 00:02, a root each. A correction
     * moves 00:01 out of the filter: a localized head miss re-versions the roots of 01-02 from that
     * day's start. With the chain declined, a second one moves 00:02 out: the resume anchors on the
     * root at 00:01 and replays nothing, so it derives the state at the view's new frontier 23:58
     * ahead of its commit, and once the commit lands a frontier truncate drops every root at or
     * above 23:58 ahead of the seal there. The crash image the armed {@code crash} takes in that
     * turn is then restored and the process restarted from it.
     */
    private void crashInTheAnchoredFrontierTruncateAndRestart(ZeroRowFrontierTruncateCrash crash, boolean isDayLost) throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, sym)");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS SELECT ts, sym, x, sum(x) OVER w s FROM base "
                + "WHERE x > 0 WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')");
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveSeedToCompletion(job, "lv");
            if (isDayLost) {
                commitTheDayTheBaseLoses(job);
            }
            commitDeparture(job, "('2026-01-01T23:57:00.000000Z', 'a', 1.5)");
            commitDeparture(job, "('2026-01-01T23:58:00.000000Z', 'a', 2.5), ('2026-01-02T00:01:00.000000Z', 'a', 4.5)");
            commitDeparture(job, "('2026-01-02T00:02:00.000000Z', 'a', 8.5)");
            commitDeparture(job, "('2026-01-02T00:01:00.000000Z', 'a', -4.5)");
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 0);
            final LiveViewInstance instance = viewInstance();
            final long generationBefore = generation(instance);
            try (Path checkpoints = checkpointsDir(instance).slash(); Path crashImage = crashImageDir()) {
                crash.arm(() -> TestUtils.copyDirectory(checkpoints, crashImage, configuration.getMkDirMode()));
                commitDeparture(job, "('2026-01-02T00:02:00.000000Z', 'a', -8.5)");
                crash.disarm();
            }
            Assert.assertTrue("the frontier truncate must publish ahead of the seal", crash.hasCrashed());
            Assert.assertEquals("the resume derived the state at the frontier", 1, job.getStatsForTest().get(LiveViewRefreshStats.RESUME_FRONTIER_STATE_RESTORE_COUNT));
            Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.TRUNCATE_ZERO_ROW_KEEP_COUNT));
            Assert.assertEquals("the truncate, the frontier truncate and the seal", generationBefore + 3, generation(instance));
            try (
                    Path crashImage = crashImageDir();
                    LiveViewCheckpointSuperblock superblock = new LiveViewCheckpointSuperblock(configuration)
            ) {
                superblock.of(crashImage);
                Assert.assertTrue(superblock.isValid());
                Assert.assertEquals("the frontier truncate has published", generationBefore + 2, superblock.generation);
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
     * Over the day the base loses when {@code isDayLost}, the head-miss departure fixture's kept-row
     * departure when {@code isHeadMiss}, and otherwise the departure of the newer of two rows under
     * one root. The frontier truncate publishes and the seal at the new frontier fails, so the
     * truncate's rule retires the truncated generation, under a lock on {@code _timeline} (see
     * {@link TimelineUnlinkFault#lock}): the generation stays on disk, two past the one the splice
     * started from, and the truncate's marker stays beside it, naming the splice's generation and
     * recording no seqTxn.
     */
    private void failTheFrontierSealWithItsTimelineLocked(
            TimelineUnlinkFault fault,
            LiveViewRefreshJob job,
            boolean isHeadMiss,
            boolean isDayLost,
            boolean isOnlyTheFileLocked
    ) throws Exception {
        driveSeedToCompletion(job, "lv");
        if (isHeadMiss) {
            commitHeadMissDepartureFixture(job, isDayLost, 59);
        } else {
            if (isDayLost) {
                commitTheDayTheBaseLoses(job);
            }
            commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'a', 1.5)");
            commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', 2.5), ('2026-01-01T00:03:00.000000Z', 'a', 9.5)");
        }
        final LiveViewInstance instance = viewInstance();
        final long spliceGeneration = generation(instance) + 1;
        final long sealFailuresBefore = instance.getCheckpointSealFailures();
        final long resetsBefore = instance.getCheckpointTimelineResets();
        final Runnable failTheSealUnderTheLock = () -> {
            job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH);
            fault.lock(isOnlyTheFileLocked);
        };
        if (isHeadMiss) {
            hooks(job).setSimulateHeadMissHeadSealStart(failTheSealUnderTheLock);
        } else {
            hooks(job).setSimulateResumeHeadSealStart(failTheSealUnderTheLock);
        }
        try {
            commitDeparture(job, isHeadMiss ? HEAD_MISS_KEPT_ROW_DEPARTURE : "('2026-01-01T00:03:00.000000Z', 'a', -9.5)");
        } finally {
            hooks(job).setSimulateHeadMissHeadSealStart(null);
            hooks(job).setSimulateResumeHeadSealStart(null);
            fault.unlock();
            job.setCheckpointTimelineTestFailureStage(0);
        }
        Assert.assertEquals(
                1,
                job.getStatsForTest().get(isHeadMiss
                        ? LiveViewRefreshStats.HEAD_MISS_SPLICE_FRONTIER_TRUNCATE_COUNT
                        : LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT)
        );
        Assert.assertEquals(sealFailuresBefore + 1, instance.getCheckpointSealFailures());
        Assert.assertEquals("the truncated timeline goes with the seal that did not land", resetsBefore + 1, instance.getCheckpointTimelineResets());
        Assert.assertEquals("the retire meets the lock once", 1, fault.getRefusedTimelineUnlinks());
        assertTimelineOnDisk(instance, true);
        Assert.assertEquals("the splice and the truncate", spliceGeneration + 1, superblockGeneration(instance));
        if (!isOnlyTheFileLocked) {
            // The truncate kept the roots below the new frontier.
            assertCheckpointLadder(
                    instance,
                    false,
                    isHeadMiss
                            ? "2025-12-31T23:55:00.000000Z -> 1\n"
                            : isDayLost
                              ? "2025-12-31T12:00:00.000000Z -> 1\n2026-01-01T00:01:00.000000Z -> 2\n"
                              : "2026-01-01T00:01:00.000000Z -> 1\n"
            );
        }
        assertMarkerNames(spliceGeneration);
        try (Path dir = checkpointsDir(instance)) {
            Assert.assertEquals(
                    "a marker written after the replacement commit records no seqTxn",
                    Numbers.LONG_NULL,
                    LiveViewCheckpointRepairMarker.readLvSeqTxn(configuration, dir)
            );
        }
        assertViewRows(isHeadMiss ? HEAD_MISS_KEPT_ROW_DEPARTURE_ROWS : singleDepartureRows(isDayLost), DEPARTURE_ROWS_QUERY);
        assertNoRefreshFaults("lv");
    }

    /**
     * Over the day the base loses, the departure of the newer of two rows under one root, with the
     * frontier truncate failing after its marker and its metadata, and the retire that answers it
     * failing on {@code _timeline} and the segment directories: the splice's generation stays on
     * disk with the truncate's marker beside it, the view refuses it, and the seal at the new
     * frontier, which retries the retire, fails too. Then, with the fault gone, {@code rowsAfter}
     * rows, one or two: the first one's seal retries the retire, which removes the splice's
     * generation and the marker with it, and opens a fresh history at the row, and each row seals a
     * root of it.
     */
    private void failTheFrontierTruncateWithItsTimelineLockedThenSeal(FrontierTruncateDoubleFault fault, int rowsAfter) throws Exception {
        createDepartureView();
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveSeedToCompletion(job, "lv");
            commitTheDayTheBaseLoses(job);
            commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'a', 1.5)");
            commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', 2.5), ('2026-01-01T00:03:00.000000Z', 'a', 9.5)");
            final LiveViewInstance instance = viewInstance();
            final long spliceGeneration = generation(instance) + 1;
            fault.arm(() -> job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH));
            try {
                commitDeparture(job, "('2026-01-01T00:03:00.000000Z', 'a', -9.5)");
            } finally {
                fault.disarm();
                job.setCheckpointTimelineTestFailureStage(0);
            }
            Assert.assertTrue("the frontier truncate must write its marker", fault.hasFired());
            Assert.assertEquals("the truncate never published", spliceGeneration, generation(instance));
            assertTimelineOnDisk(instance, true);
            assertMarkerNames(spliceGeneration);
            Assert.assertTrue("the view refuses the generation the retire kept", instance.isCheckpointTimelineRetirePending());
            // The splice re-versioned the departed row's root, above the new frontier 00:02.
            assertCheckpointLadder(instance, false, """
                    2025-12-31T12:00:00.000000Z -> 1
                    2026-01-01T00:01:00.000000Z -> 2
                    2026-01-01T00:03:00.000000Z -> 3
                    """);

            commitDeparture(job, "('2026-01-01T00:04:00.000000Z', 'a', 8.0)");
            Assert.assertFalse("the seal's retry of the retire removes the kept file", instance.isCheckpointTimelineRetirePending());
            Assert.assertEquals("and the seal opens a fresh history", 1, generation(instance));
            assertCheckpointLadder(instance, false, "2026-01-01T00:04:00.000000Z -> 4\n");
            assertNoRepairMarker(instance);
            if (rowsAfter > 1) {
                commitDeparture(job, "('2026-01-01T00:05:00.000000Z', 'a', 1.0)");
                assertCheckpointLadder(
                        instance,
                        false,
                        "2026-01-01T00:04:00.000000Z -> 4\n2026-01-01T00:05:00.000000Z -> 5\n"
                );
                Assert.assertEquals("each row seals one generation", 2, generation(instance));
                assertNoRepairMarker(instance);
            }
            assertNoRefreshFaults("lv");
        }
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

    // The generation the superblock's selected slot names, read off the slots alone: what a restart
    // weighs a repair marker against, whether or not the segments the generation names are on disk.
    private long superblockGeneration(LiveViewInstance instance) {
        try (
                Path dir = checkpointsDir(instance);
                LiveViewCheckpointSuperblock superblock = new LiveViewCheckpointSuperblock(configuration)
        ) {
            superblock.of(dir);
            Assert.assertTrue(superblock.isValid());
            return superblock.generation;
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
        putTheCrashImageBack();
        restartAndRefresh();
    }

    // Stops the process and puts the crash image back as the view's checkpoint directory, for the
    // restart that follows to come back from.
    private void putTheCrashImageBack() {
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
     * directory, so a retire removes nothing. With {@code isOnlyTheTimelineRefused} it fails only
     * the unlink of {@code _timeline} and the directory removals, so a retire removes every other
     * file: the repair marker too, unless the retire keeps it beside the timeline.
     */
    private static final class FrontierTruncateDoubleFault extends TestFilesFacadeImpl {
        private final boolean isOnlyTheTimelineRefused;
        private final AtomicInteger markerRenames = new AtomicInteger();
        private volatile boolean hasFired;
        private volatile boolean isFailingRemovals;
        private volatile Runnable onFire;

        FrontierTruncateDoubleFault(boolean isOnlyTheTimelineRefused) {
            this.isOnlyTheTimelineRefused = isOnlyTheTimelineRefused;
        }

        @Override
        public boolean removeQuiet(LPSZ name) {
            if (isFailingRemovals
                    && Utf8s.containsAscii(name, LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME)
                    && (!isOnlyTheTimelineRefused || Utf8s.endsWithAscii(name, LiveViewCheckpointLayout.TIMELINE_FILE_NAME))) {
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

    /**
     * The removals a lock on {@code _timeline} refuses. Once locked, fails the unlink of
     * {@code _timeline} and every directory removal under the checkpoint directory, or, with
     * {@code isOnlyTheFileLocked}, only the removal of the checkpoint directory that holds the
     * file, so a retire removes the segment directories. Every other unlink - the repair marker,
     * its staged sibling, the retirement work set - goes through. It can also run an action right
     * before the next unlink of {@code _timeline}, where a crash image of a crash ahead of a
     * retire is taken, and fail the next rewrite of the repair marker the way Windows fails it:
     * the rename over the previous record is refused, that record unlinked, and the retried
     * rename refused too. Apart from the lock it can refuse every open of {@code _timeline}, refuse
     * the staged write of the next repair markers before it creates a file, so no marker of theirs
     * reaches the disk, run an action right after the next repair marker lands, fail the check
     * of {@code _timeline} that follows its next unlink, and throw from the next removal of the
     * checkpoint {@code meta} directory. It can sample, once, the native memory that open
     * {@link Path}s hold at the next unlink of {@code _timeline}, and the native allocations made
     * so far at the first check after it of whether {@code _timeline} or a repair marker name exists.
     * <p>
     * A second lock stands on the repair marker alone: once locked, it refuses every unlink of
     * {@code _repairing} and of {@code _repairing.tmp}, or, unlocked, the next few of them. It counts
     * the unlinks it refuses, every unlink of either name, and every check of whether either name
     * exists, and can run an action right before the next unlink of either name it lets through,
     * where a crash image of a crash ahead of that unlink is taken.
     */
    private static final class TimelineUnlinkFault extends TestFilesFacadeImpl {
        private final AtomicInteger failedMarkerRewrites = new AtomicInteger();
        private final AtomicInteger failedTimelineChecks = new AtomicInteger();
        private final AtomicInteger markerExistsChecks = new AtomicInteger();
        private final AtomicInteger markerRenames = new AtomicInteger();
        private final AtomicInteger markerUnlinks = new AtomicInteger();
        private final AtomicInteger refusedBaseColumnOpens = new AtomicInteger();
        private final AtomicInteger refusedMarkerUnlinks = new AtomicInteger();
        private final AtomicInteger refusedMarkerWrites = new AtomicInteger();
        private final AtomicInteger refusedTimelineUnlinks = new AtomicInteger();
        private final AtomicInteger thrownMetaDirRemovals = new AtomicInteger();
        private volatile Runnable afterTheNextMarker;
        // Unsafe's malloc and realloc count at the check the sampler armed, or -1.
        private volatile long allocationsAtTheCheck = -1;
        // The directory and partition of the base column open to fail once, or null.
        private volatile String baseColumnFaultDirName;
        private volatile String baseColumnFaultPartition;
        private volatile Runnable beforeTheNextMarkerUnlink;
        private volatile Runnable beforeTheNextTimelineUnlink;
        private volatile int injectedErrno;
        private volatile boolean isFailingTheCheckAfterTheNextTimelineUnlink;
        private volatile boolean isFailingTheNextMarkerRewrite;
        private volatile boolean isFailingTheNextTimelineCheck;
        private volatile boolean isLocked;
        private volatile boolean isMarkerLocked;
        private volatile boolean isMarkerRewriteRetried;
        private volatile boolean isOnlyTheFileLocked;
        private volatile boolean isRefusingTimelineOpens;
        private volatile boolean isSamplingTheNextCheck;
        private volatile boolean isSamplingTheNextUnlink;
        private volatile boolean isThrowingAtTheNextMetaDirRemoval;
        // Marker unlinks left to refuse while the marker is not locked.
        private volatile int markerUnlinksToRefuse;
        // Staged marker writes left to refuse, or a negative count to refuse every one.
        private volatile int markerWritesToRefuse;
        // The native memory tagged NATIVE_PATH at the unlink the sampler armed, or -1.
        private volatile long pathBytesAtTheUnlink = -1;

        @Override
        public int errno() {
            final int errno = injectedErrno;
            return errno != 0 ? errno : super.errno();
        }

        @Override
        public boolean exists(LPSZ path) {
            if (isSamplingTheNextCheck && (isTimelineFile(path) || isMarkerFile(path))) {
                isSamplingTheNextCheck = false;
                allocationsAtTheCheck = Unsafe.getMallocCount() + Unsafe.getReallocCount();
            }
            if (isMarkerFile(path)) {
                markerExistsChecks.incrementAndGet();
            }
            if (isFailingTheNextTimelineCheck && isTimelineFile(path)) {
                isFailingTheNextTimelineCheck = false;
                failedTimelineChecks.incrementAndGet();
                throw CairoException.critical(0).put("injected fault on the check of _timeline");
            }
            return super.exists(path);
        }

        @Override
        public long openRO(LPSZ name) {
            if (isRefusedTimelineOpen(name)) {
                return -1;
            }
            final String dirName = baseColumnFaultDirName;
            if (dirName != null
                    && Utf8s.containsAscii(name, dirName)
                    && Utf8s.containsAscii(name, baseColumnFaultPartition)
                    && Utf8s.endsWithAscii(name, "x.d")
                    && !Utf8s.containsAscii(name, "wal")) {
                baseColumnFaultDirName = null;
                refusedBaseColumnOpens.incrementAndGet();
                return -1;
            }
            return super.openRO(name);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            if (isRefusedTimelineOpen(name)) {
                return -1;
            }
            final int toRefuse = markerWritesToRefuse;
            if (toRefuse != 0
                    && Utf8s.endsWithAscii(name, LiveViewCheckpointLayout.REPAIRING_MARKER_FILE_NAME + LiveViewCheckpointLayout.TMP_SUFFIX)) {
                if (toRefuse > 0) {
                    markerWritesToRefuse = toRefuse - 1;
                }
                refusedMarkerWrites.incrementAndGet();
                return -1;
            }
            return super.openRW(name, opts);
        }

        @Override
        public boolean removeQuiet(LPSZ name) {
            if (isMarkerFile(name)) {
                markerUnlinks.incrementAndGet();
                if (isMarkerLocked) {
                    refusedMarkerUnlinks.incrementAndGet();
                    return false;
                }
                final int toRefuse = markerUnlinksToRefuse;
                if (toRefuse > 0) {
                    markerUnlinksToRefuse = toRefuse - 1;
                    refusedMarkerUnlinks.incrementAndGet();
                    return false;
                }
                final Runnable action = beforeTheNextMarkerUnlink;
                if (action != null) {
                    beforeTheNextMarkerUnlink = null;
                    action.run();
                }
            }
            if (isTimelineFile(name)) {
                if (isSamplingTheNextUnlink) {
                    isSamplingTheNextUnlink = false;
                    pathBytesAtTheUnlink = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_PATH);
                    isSamplingTheNextCheck = true;
                }
                final Runnable action = beforeTheNextTimelineUnlink;
                if (action != null) {
                    beforeTheNextTimelineUnlink = null;
                    action.run();
                }
                if (isFailingTheCheckAfterTheNextTimelineUnlink) {
                    isFailingTheCheckAfterTheNextTimelineUnlink = false;
                    isFailingTheNextTimelineCheck = true;
                }
                if (isLocked) {
                    refusedTimelineUnlinks.incrementAndGet();
                    return false;
                }
            }
            return super.removeQuiet(name);
        }

        @Override
        public int rename(LPSZ from, LPSZ to) {
            if (isFailingTheNextMarkerRewrite && Utf8s.endsWithAscii(to, LiveViewCheckpointLayout.REPAIRING_MARKER_FILE_NAME)) {
                if (exists(to)) {
                    // MoveFileW refuses an existing destination, so the rewrite unlinks the
                    // previous record and retries.
                    isMarkerRewriteRetried = true;
                    injectedErrno = CairoException.ERRNO_ALREADY_EXISTS_WIN;
                    return Files.FILES_RENAME_ERR_OTHER;
                }
                if (isMarkerRewriteRetried) {
                    isFailingTheNextMarkerRewrite = false;
                    isMarkerRewriteRetried = false;
                    failedMarkerRewrites.incrementAndGet();
                    injectedErrno = CairoException.ERRNO_EACCES_LINUX;
                    return Files.FILES_RENAME_ERR_OTHER;
                }
            }
            injectedErrno = 0;
            final int result = super.rename(from, to);
            if (result == Files.FILES_RENAME_OK && Utf8s.endsWithAscii(to, LiveViewCheckpointLayout.REPAIRING_MARKER_FILE_NAME)) {
                markerRenames.incrementAndGet();
                final Runnable action = afterTheNextMarker;
                if (action != null) {
                    afterTheNextMarker = null;
                    action.run();
                }
            }
            return result;
        }

        @Override
        public boolean rmdir(Path name, boolean haltOnError) {
            if (isThrowingAtTheNextMetaDirRemoval
                    && (Utf8s.endsWithAscii(name, LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME + Files.SEPARATOR + LiveViewCheckpointLayout.META_DIR_NAME)
                    || Utf8s.endsWithAscii(name, LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME + Files.SEPARATOR + LiveViewCheckpointLayout.META_DIR_NAME + Files.SEPARATOR))) {
                isThrowingAtTheNextMetaDirRemoval = false;
                thrownMetaDirRemovals.incrementAndGet();
                // What FilesFacadeImpl.rmdir rethrows when its walk of the tree fails.
                throw new CairoError("injected fault on the removal of the live view checkpoint meta directory");
            }
            if (isLocked
                    && Utf8s.containsAscii(name, LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME)
                    && (!isOnlyTheFileLocked
                    || Utf8s.endsWithAscii(name, LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME)
                    || Utf8s.endsWithAscii(name, LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME + Files.SEPARATOR))) {
                return false;
            }
            return super.rmdir(name, haltOnError);
        }

        void afterTheNextMarker(Runnable action) {
            afterTheNextMarker = action;
        }

        void beforeTheNextMarkerUnlink(Runnable action) {
            beforeTheNextMarkerUnlink = action;
        }

        void beforeTheNextTimelineUnlink(Runnable action) {
            beforeTheNextTimelineUnlink = action;
        }

        // Fails the check whether _timeline exists that follows its next unlink, refused or not,
        // once.
        void failTheCheckAfterTheNextTimelineUnlink() {
            isFailingTheCheckAfterTheNextTimelineUnlink = true;
        }

        void failTheNextMarkerRewrite() {
            isMarkerRewriteRetried = false;
            isFailingTheNextMarkerRewrite = true;
        }

        // Refuses the staged write of the next count repair markers, or of every one until called
        // again with 0 when count is negative.
        void failMarkerWrites(int count) {
            markerWritesToRefuse = count;
        }

        // Fails the next open of the column file x.d in the partition directory named partition of
        // the table whose directory is dirName, once.
        void failTheNextBaseColumnOpen(String dirName, String partition) {
            baseColumnFaultPartition = partition;
            baseColumnFaultDirName = dirName;
        }

        int getRefusedBaseColumnOpens() {
            return refusedBaseColumnOpens.get();
        }

        int getFailedMarkerRewrites() {
            return failedMarkerRewrites.get();
        }

        // Unsafe's malloc and realloc count at the check the sampler armed, or -1 when none ran.
        long getAllocationsAtTheCheck() {
            return allocationsAtTheCheck;
        }

        int getFailedTimelineChecks() {
            return failedTimelineChecks.get();
        }

        int getMarkerExistsChecks() {
            return markerExistsChecks.get();
        }

        int getMarkerRenames() {
            return markerRenames.get();
        }

        int getMarkerUnlinks() {
            return markerUnlinks.get();
        }

        int getRefusedMarkerUnlinks() {
            return refusedMarkerUnlinks.get();
        }

        int getRefusedMarkerWrites() {
            return refusedMarkerWrites.get();
        }

        // The native memory tagged NATIVE_PATH at the unlink the sampler armed, or -1 when none ran.
        long getPathBytesAtTheUnlink() {
            return pathBytesAtTheUnlink;
        }

        int getRefusedTimelineUnlinks() {
            return refusedTimelineUnlinks.get();
        }

        int getThrownMetaDirRemovals() {
            return thrownMetaDirRemovals.get();
        }

        void lock(boolean isOnlyTheFileLocked) {
            this.isOnlyTheFileLocked = isOnlyTheFileLocked;
            refusedTimelineUnlinks.set(0);
            isLocked = true;
        }

        // Locks or unlocks both repair marker names, refusing every unlink of either while locked.
        void lockMarker(boolean isLocked) {
            isMarkerLocked = isLocked;
        }

        // Refuses the next count unlinks of either repair marker name while the marker is not locked.
        void refuseTheNextMarkerUnlinks(int count) {
            markerUnlinksToRefuse = count;
        }

        void refuseTimelineOpens(boolean isRefusing) {
            isRefusingTimelineOpens = isRefusing;
        }

        // Samples, once, the native memory tagged NATIVE_PATH at the next unlink of _timeline, and
        // Unsafe's malloc and realloc count at the first check after it of whether _timeline or a
        // repair marker name exists.
        void sampleTheNextRetire() {
            allocationsAtTheCheck = -1;
            pathBytesAtTheUnlink = -1;
            isSamplingTheNextCheck = false;
            isSamplingTheNextUnlink = true;
        }

        // Throws, once, from the next removal of the checkpoint meta directory.
        void throwAtTheNextMetaDirRemoval() {
            isThrowingAtTheNextMetaDirRemoval = true;
        }

        void unlock() {
            isLocked = false;
        }

        private static boolean isMarkerFile(LPSZ name) {
            return Utf8s.containsAscii(name, LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME)
                    && (Utf8s.endsWithAscii(name, LiveViewCheckpointLayout.REPAIRING_MARKER_FILE_NAME)
                    || Utf8s.endsWithAscii(name, LiveViewCheckpointLayout.REPAIRING_MARKER_FILE_NAME + LiveViewCheckpointLayout.TMP_SUFFIX));
        }

        private static boolean isTimelineFile(LPSZ name) {
            return Utf8s.containsAscii(name, LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME)
                    && Utf8s.endsWithAscii(name, LiveViewCheckpointLayout.TIMELINE_FILE_NAME);
        }

        private boolean isRefusedTimelineOpen(LPSZ name) {
            return isRefusingTimelineOpens && isTimelineFile(name);
        }
    }

    /**
     * Once armed, fails the next read-write open of the published repair marker,
     * {@code _checkpoints/_repairing}, the way a full table of open files fails it. Only a read of
     * the marker opens that name: the write stages its record through {@code _repairing.tmp}.
     */
    private static final class RepairMarkerOpenFault extends TestFilesFacadeImpl {
        private final AtomicBoolean isArmed = new AtomicBoolean();
        private final AtomicInteger refusedOpens = new AtomicInteger();

        @Override
        public long openRW(LPSZ name, int opts) {
            if (Utf8s.endsWithAscii(name, LiveViewCheckpointLayout.REPAIRING_MARKER_FILE_NAME) && isArmed.compareAndSet(true, false)) {
                refusedOpens.incrementAndGet();
                return -1;
            }
            return super.openRW(name, opts);
        }

        void arm() {
            refusedOpens.set(0);
            isArmed.set(true);
        }

        void disarm() {
            isArmed.set(false);
        }

        int getRefusedOpens() {
            return refusedOpens.get();
        }
    }

    /**
     * Reads back each repair marker the view publishes, right after the rename that publishes
     * it: the generation it names and the seqTxn it records. Once armed, also runs an action right
     * after the next such rename, ahead of anything its repair does next, which is where a crash
     * image of a crash between the marker and the publication it guards is taken.
     */
    private static final class RepairMarkerWitness extends TestFilesFacadeImpl {
        private final LongList generations = new LongList();
        private final LongList lvSeqTxns = new LongList();
        private Runnable afterTheNextMarker;

        @Override
        public int rename(LPSZ from, LPSZ to) {
            final int result = super.rename(from, to);
            if (result == Files.FILES_RENAME_OK && Utf8s.endsWithAscii(to, LiveViewCheckpointLayout.REPAIRING_MARKER_FILE_NAME)) {
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                try (Path dir = checkpointsDir(instance)) {
                    generations.add(LiveViewCheckpointRepairMarker.readBaseGeneration(configuration, dir));
                    lvSeqTxns.add(LiveViewCheckpointRepairMarker.readLvSeqTxn(configuration, dir));
                }
                final Runnable action = afterTheNextMarker;
                if (action != null) {
                    afterTheNextMarker = null;
                    action.run();
                }
            }
            return result;
        }

        void afterTheNextMarker(Runnable action) {
            afterTheNextMarker = action;
        }

        void clear() {
            generations.clear();
            lvSeqTxns.clear();
            afterTheNextMarker = null;
        }

        long getGeneration(int index) {
            return generations.getQuick(index);
        }

        long getLvSeqTxn(int index) {
            return lvSeqTxns.getQuick(index);
        }

        int size() {
            return generations.size();
        }
    }
}
