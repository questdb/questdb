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
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointRecoveryPhase;
import io.questdb.cairo.lv.LiveViewCheckpointRepairMarker;
import io.questdb.cairo.lv.LiveViewCheckpointRestoreRoute;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRebuildRestatementGuard;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The restatement guard in front of the whole-view rebuild from the applied base: what it
 * refuses, what it lets through, and what a refused rebuild leaves behind.
 * <p>
 * Every refusal case starts the same way. A base partition the view has already derived
 * rows from is dropped, and the incremental path walks past the DROP PARTITION as it always
 * has - the view keeps its rows for that day, which is the frozen-prefix contract. Then a
 * route into the whole-view rebuild opens: a restart with no timeline, a restart behind a
 * live repair marker, a base schema change the view cannot restore its accumulators past in
 * place, a lost base WAL segment. Before the guard, each of them recomputed the view from the
 * surviving base rows and replaced its output - the dropped day's rows gone, silently, with
 * the view valid throughout. Now each of them stops the view instead, and the case asserts
 * that everything the rebuild would have replaced is still there.
 * <p>
 * A base schema change that can restore in place never gets that far: it puts the
 * accumulators back from the view's own timeline and keeps refreshing, with the dropped day
 * still in the view. That case is here too, because it is the refusal the restore removes;
 * the restore itself is {@link LiveViewRuntimeRestoreTest}'s subject.
 * <p>
 * The checks are witnessed apart. Dropping the OLDEST day moves the base's earliest row
 * above the view's, which the history floor sees before the rebuild reads a row. Dropping a
 * MIDDLE day leaves the base's earliest row where it was, so only the scan's row count can
 * see it. Where a backlog that may hold a dedup replacement stands that count down, the lost
 * partition check sees a dropped day wherever it sits, including an oldest day the history
 * floor misses because the base keeps an older row the view does not.
 * {@link LiveViewRebuildRestatementGuard#getVerdict()} names which one fired.
 * <p>
 * The pass cases matter as much: a rebuild over a base that still holds every row has to go
 * ahead exactly as before, and so does one whose backlog legitimately removes a row - both
 * are rebuilds that heal, and a guard that stopped them would trade one silent failure for a
 * loud one nobody asked for.
 */
public class LiveViewRebuildRestatementGuardTest extends AbstractLiveViewCheckpointCompatTest {
    // What the view holds once the fixture's six rows are in. ANCHOR DAILY resets each
    // account's accumulators at midnight, so day 2 carries acct-1 to 12.0 / 2.
    private static final String ALL_ROWS = """
            created_at\taccount_id\tcumulative_sum\tcumulative_count
            2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
            2026-01-01T09:10:00.000000Z\tacct-2\t2.0\t1
            2026-01-02T09:00:00.000000Z\tacct-1\t4.0\t1
            2026-01-02T09:10:00.000000Z\tacct-1\t12.0\t2
            2026-01-03T09:00:00.000000Z\tacct-1\t16.0\t1
            2026-01-03T09:10:00.000000Z\tacct-2\t32.0\t1
            """;
    // What the view holds once the late day-two row the parked-repair cases commit has been
    // repaired in, over a base that lost its oldest day. The view keeps that day, and the
    // correction re-accumulates day two alone: ANCHOR DAILY resets acct-1 at midnight.
    private static final String CORRECTED_ROWS = """
            created_at\taccount_id\tcumulative_sum\tcumulative_count
            2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
            2026-01-01T09:10:00.000000Z\tacct-2\t2.0\t1
            2026-01-02T09:00:00.000000Z\tacct-1\t4.0\t1
            2026-01-02T09:05:00.000000Z\tacct-1\t68.0\t2
            2026-01-02T09:10:00.000000Z\tacct-1\t76.0\t3
            2026-01-03T09:00:00.000000Z\tacct-1\t16.0\t1
            2026-01-03T09:10:00.000000Z\tacct-2\t32.0\t1
            """;
    // The same, for the late day-three row the resume-replay cases commit. The newest checkpoint
    // below it is day three's first row, so the repair resumes from that anchor rather than
    // replaying from the correction's own day.
    private static final String CORRECTED_DAY_THREE_ROWS = """
            created_at\taccount_id\tcumulative_sum\tcumulative_count
            2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
            2026-01-01T09:10:00.000000Z\tacct-2\t2.0\t1
            2026-01-02T09:00:00.000000Z\tacct-1\t4.0\t1
            2026-01-02T09:10:00.000000Z\tacct-1\t12.0\t2
            2026-01-03T09:00:00.000000Z\tacct-1\t16.0\t1
            2026-01-03T09:05:00.000000Z\tacct-1\t80.0\t2
            2026-01-03T09:10:00.000000Z\tacct-2\t32.0\t1
            """;
    // The same once ROWS_AHEAD sits above the late row too: acct-1 carries the correction's 64.0
    // through the rest of day three.
    private static final String CORRECTED_DAY_THREE_ROWS_AHEAD = """
            created_at\taccount_id\tcumulative_sum\tcumulative_count
            2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
            2026-01-01T09:10:00.000000Z\tacct-2\t2.0\t1
            2026-01-02T09:00:00.000000Z\tacct-1\t4.0\t1
            2026-01-02T09:10:00.000000Z\tacct-1\t12.0\t2
            2026-01-03T09:00:00.000000Z\tacct-1\t16.0\t1
            2026-01-03T09:05:00.000000Z\tacct-1\t80.0\t2
            2026-01-03T09:10:00.000000Z\tacct-2\t32.0\t1
            2026-01-03T10:00:00.000000Z\tacct-1\t144.0\t3
            2026-01-03T10:10:00.000000Z\tacct-2\t160.0\t2
            2026-01-03T10:20:00.000000Z\tacct-1\t400.0\t4
            2026-01-03T10:30:00.000000Z\tacct-2\t672.0\t3
            """;
    private static final String CRASH_IMAGE_DIR_NAME = "lv_checkpoints_crash_image";
    // What the view with no window dependency holds once the fixture's six rows are in, in one
    // commit. Nothing resets the accumulators, so each account carries its whole history.
    private static final String UNLOCALIZED_ROWS = """
            created_at\taccount_id\tcumulative_sum\tcumulative_count
            2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
            2026-01-01T09:10:00.000000Z\tacct-2\t2.0\t1
            2026-01-02T09:00:00.000000Z\tacct-1\t5.0\t2
            2026-01-02T09:10:00.000000Z\tacct-1\t13.0\t3
            2026-01-03T09:00:00.000000Z\tacct-1\t29.0\t4
            2026-01-03T09:10:00.000000Z\tacct-2\t34.0\t2
            """;
    // What that view holds once the late day-two row has been repaired in, over a base that lost
    // its oldest day. The repair replays the whole surviving base from the view boundary and
    // replaces the view from its first output row up, so the day the base lost stays in the view
    // and every row above it is recomputed without that day.
    private static final String UNLOCALIZED_CORRECTED_ROWS = """
            created_at\taccount_id\tcumulative_sum\tcumulative_count
            2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
            2026-01-01T09:10:00.000000Z\tacct-2\t2.0\t1
            2026-01-02T09:00:00.000000Z\tacct-1\t4.0\t1
            2026-01-02T09:05:00.000000Z\tacct-1\t68.0\t2
            2026-01-02T09:10:00.000000Z\tacct-1\t76.0\t3
            2026-01-03T09:00:00.000000Z\tacct-1\t92.0\t4
            2026-01-03T09:10:00.000000Z\tacct-2\t32.0\t1
            """;
    // What names a checkpoint data segment inside the view's checkpoint directory. The segment a
    // repair stages carries the temporary suffix until its splice publishes it.
    private static final String DATA_SEGMENT_PATH_PART = LiveViewCheckpointLayout.DATA_DIR_NAME
            + Files.SEPARATOR
            + LiveViewCheckpointLayout.DATA_SEGMENT_PREFIX;
    // What the filtering view the dedup-backlog cases create holds before and after the base
    // replaces its first row with one the filter rejects, and the row a later commit appends.
    private static final String FILTERED_APPEND = "INSERT INTO base (ts, sym, i) VALUES ('2026-01-01T00:10:00.000000Z', 'a', 1000)";
    private static final String FILTERED_APPEND_OUTPUT = "2026-01-01T00:10:00.000000Z\ta\t1000\t2400.0\n";
    // A commit that reaches the frontier of a view holding FILTERED_TWO_DAY_ROWS, with a row the
    // filter rejects. Over a base without dedup it only adds a row.
    private static final String FILTERED_FRONTIER_COMMIT = "INSERT INTO base (ts, sym, i) VALUES ('2026-01-02T00:05:00.000000Z', 'a', -1)";
    // The same for a view holding FILTERED_LATER_DAYS_ROWS.
    private static final String FILTERED_LATER_DAYS_FRONTIER_COMMIT = "INSERT INTO base (ts, sym, i) VALUES ('2026-01-03T00:05:00.000000Z', 'a', -1)";
    // The evidence a lost partition refusal reads once a view holding FILTERED_LATER_DAYS_ROWS
    // outlives its base's second day.
    private static final String FILTERED_LATER_DAYS_LOST_DAY_EVIDENCE = "the view holds a row at 2026-01-02T00:01:00.000000Z "
            + "but the base table holds no partition between 2026-01-02T00:00:00.000000Z and 2026-01-03T00:00:00.000000Z";
    // What the filtering view holds over rows on the second and third day, while its base also
    // holds an older row on the first day that the view does not: a base that loses the second
    // day then still has a row earlier than the view's first, and the history floor is blind to
    // the loss.
    private static final String FILTERED_LATER_DAYS_ROWS = """
            ts\tsym\ti\tv
            2026-01-02T00:01:00.000000Z\ta\t297\t297.0
            2026-01-03T00:05:00.000000Z\ta\t500\t500.0
            2026-01-03T00:09:00.000000Z\ta\t900\t1400.0
            """;
    private static final String FILTERED_LATER_DAYS_ROWS_INSERT = """
            INSERT INTO base (ts, sym, i) VALUES
                ('2026-01-02T00:01:00.000000Z', 'a', 297),
                ('2026-01-03T00:05:00.000000Z', 'a', 500),
                ('2026-01-03T00:09:00.000000Z', 'a', 900)""";
    // The evidence a history floor reads once a view holding FILTERED_TWO_DAY_ROWS outlives its
    // base's first day.
    private static final String FILTERED_LOST_DAY_EVIDENCE = "the view holds rows from 2026-01-01T00:01:00.000000Z "
            + "but the base table's earliest row is at 2026-01-02T00:05:00.000000Z";
    private static final String FILTERED_REPLACED_ROWS = """
            ts\tsym\ti\tv
            2026-01-01T00:05:00.000000Z\ta\t500\t500.0
            2026-01-01T00:09:00.000000Z\ta\t900\t1400.0
            """;
    private static final String FILTERED_REPLACEMENT = "INSERT INTO base (ts, sym, i) VALUES ('2026-01-01T00:01:00.000000Z', 'a', -108)";
    private static final String FILTERED_ROWS = """
            ts\tsym\ti\tv
            2026-01-01T00:01:00.000000Z\ta\t297\t297.0
            2026-01-01T00:05:00.000000Z\ta\t500\t797.0
            2026-01-01T00:09:00.000000Z\ta\t900\t1697.0
            """;
    private static final String FILTERED_ROWS_INSERT = """
            INSERT INTO base (ts, sym, i) VALUES
                ('2026-01-01T00:01:00.000000Z', 'a', 297),
                ('2026-01-01T00:05:00.000000Z', 'a', 500),
                ('2026-01-01T00:09:00.000000Z', 'a', 900)""";
    // The evidence a lost partition refusal reads once a view holding FILTERED_THREE_DAY_ROWS
    // outlives its base's middle day.
    private static final String FILTERED_THREE_DAY_LOST_DAY_EVIDENCE = "the view holds a row at 2026-01-02T00:05:00.000000Z "
            + "but the base table holds no partition between 2026-01-02T00:00:00.000000Z and 2026-01-03T00:00:00.000000Z";
    // The same view over one row a day for three days, so a base that loses the middle day still
    // holds the view's first row, and only the row shortfall sees the loss.
    private static final String FILTERED_THREE_DAY_ROWS = """
            ts\tsym\ti\tv
            2026-01-01T00:01:00.000000Z\ta\t297\t297.0
            2026-01-02T00:05:00.000000Z\ta\t500\t500.0
            2026-01-03T00:09:00.000000Z\ta\t900\t900.0
            """;
    private static final String FILTERED_THREE_DAY_ROWS_INSERT = """
            INSERT INTO base (ts, sym, i) VALUES
                ('2026-01-01T00:01:00.000000Z', 'a', 297),
                ('2026-01-02T00:05:00.000000Z', 'a', 500),
                ('2026-01-03T00:09:00.000000Z', 'a', 900)""";
    // The same view over rows spanning two days, so a base that loses its first day holds no row
    // as early as the view's first.
    private static final String FILTERED_TWO_DAY_ROWS = """
            ts\tsym\ti\tv
            2026-01-01T00:01:00.000000Z\ta\t297\t297.0
            2026-01-02T00:05:00.000000Z\ta\t500\t500.0
            2026-01-02T00:09:00.000000Z\ta\t900\t1400.0
            """;
    private static final String FILTERED_TWO_DAY_ROWS_INSERT = """
            INSERT INTO base (ts, sym, i) VALUES
                ('2026-01-01T00:01:00.000000Z', 'a', 297),
                ('2026-01-02T00:05:00.000000Z', 'a', 500),
                ('2026-01-02T00:09:00.000000Z', 'a', 900)""";
    // Commits made after the fixture's six rows, in the order the cases below make them, and the
    // view row each one produces on top of ALL_ROWS. They extend day three, so acct-1 and acct-2
    // keep accumulating from 16.0 and 32.0.
    private static final String[] ROWS_AHEAD = {
            "('2026-01-03T10:00:00.000000Z', 'acct-1', 64.0)",
            "('2026-01-03T10:10:00.000000Z', 'acct-2', 128.0)",
            "('2026-01-03T10:20:00.000000Z', 'acct-1', 256.0)",
            "('2026-01-03T10:30:00.000000Z', 'acct-2', 512.0)"
    };
    private static final String[] ROWS_AHEAD_OUTPUT = {
            "2026-01-03T10:00:00.000000Z\tacct-1\t80.0\t2\n",
            "2026-01-03T10:10:00.000000Z\tacct-2\t160.0\t2\n",
            "2026-01-03T10:20:00.000000Z\tacct-1\t336.0\t3\n",
            "2026-01-03T10:30:00.000000Z\tacct-2\t672.0\t3\n"
    };
    // The log line a rebuild writes when it stands the row shortfall down over a backlog that may
    // hold a dedup replacement, and keeps the history floor and the lost partition check.
    private static final String ROW_SHORTFALL_STAND_DOWN_LINE = "live view rebuild from the applied base checks only "
            + "the restatement guard's history floor and base partitions";
    // The two running doors into the whole-view rebuild, as their recoveries name themselves in
    // the log lines and the operator reasons a deferral or a refusal publishes.
    private static final String DRIFT_CAUSE = "base table metadata change";
    private static final String MID_DRAIN_CAUSE = "mid-drain refresh failure";
    private static final String VIEW_ROWS_QUERY = "SELECT created_at, account_id, cumulative_sum, cumulative_count FROM lv";
    private static final LogCapture capture = new LogCapture();

    @After
    public void resetClock() {
        capture.stop();
        setCurrentMicros(-1);
    }

    @Before
    public void setUpCadence() {
        // One logical boundary per commit, so the timeline a refusal must preserve holds a
        // ladder rather than a single root.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setCurrentMicros(0);
        capture.start();
    }

    @Test
    public void testABaseSchemaChangeBehindALiveRepairMarkerIsRefused() throws Exception {
        assertMemoryLeak(() -> {
            // A deduplicating base, because its drain reads the applied base through the
            // compiled factory, and that is where a base metadata change surfaces as drift. The
            // view has no filter, so dedup cannot drop an output row and the guard compares.
            seedSixRows("DEDUP UPSERT KEYS(created_at, account_id)");
            dropPartitionAndRefresh("2026-01-01");
            // A repair whose truncated head is not yet re-sealed. It is what keeps the drift's
            // own recovery - restoring the accumulators from the timeline in place - off the
            // timeline, and so what sends it to the whole-view rebuild.
            writeRepairMarker(instance("lv"));
            final int boundariesBefore = countSealedBoundaries("lv");
            final long generationBefore = newestGeneration(instance("lv"));
            final long processedBefore = instance("lv").getLastProcessedSeqTxn();

            // A schema change the view survives: the column is one it never reads. The raw WAL
            // drain resolves columns by name and would absorb it, so the commit after it carries
            // two rows the base collapses into one. That dedup is what routes the drain through
            // the applied base, whose reader the view's compiled plan now predates: the drain
            // meets the drift, recompiles and asks for the whole-view rebuild.
            execute("ALTER TABLE tx ADD COLUMN note INT");
            execute("INSERT INTO tx (created_at, account_id, amount) VALUES "
                    + "('2026-01-03T10:00:00.000000Z', 'acct-1', 30.0), "
                    + "('2026-01-03T10:00:00.000000Z', 'acct-1', 64.0)");
            drainWalQueue();
            final LiveViewRebuildRestatementGuard guard;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                guard = job.rebuildRestatementGuardForTest();
            }

            capture.drain();
            capture.assertLogged("live view cannot restore its runtime from the checkpoint timeline, rebuilding from the applied base "
                    + "[view=lv, cause=base table metadata change, reason=prefix preservation repair marker present]");
            capture.assertLogged("live view rebuild from the applied base refused, it would drop rows the view retains");
            capture.assertNotLogged("live view recomputed window state from applied base");
            final LiveViewInstance instance = instance("lv");
            assertRebuildBlocked(instance, "base table metadata change");
            Assert.assertEquals(0, instance.getCheckpointRuntimeRestores());
            Assert.assertEquals(LiveViewRebuildRestatementGuard.VERDICT_HISTORY_FLOOR, guard.getVerdict());
            TestUtils.assertContains(
                    instance.getCheckpointRecoveryReason(),
                    "the view holds rows from 2026-01-01T09:00:00.000000Z but the base table's earliest row is at 2026-01-02T09:00:00.000000Z"
            );

            // Nothing moved: not the rows, not the watermark, not the timeline, not the marker.
            assertViewRows(ALL_ROWS);
            Assert.assertEquals(processedBefore, instance.getLastProcessedSeqTxn());
            Assert.assertEquals(0, instance.getCheckpointTimelineResets());
            Assert.assertEquals(
                    "a refused rebuild must not retire the timeline a restart restores from",
                    generationBefore,
                    newestGeneration(instance)
            );
            Assert.assertEquals(boundariesBefore, countSealedBoundaries("lv"));
            try (Path dir = checkpointsDir(instance)) {
                Assert.assertTrue(LiveViewCheckpointRepairMarker.exists(engine.getConfiguration().getFilesFacade(), dir));
            }
            assertLiveViewsReportsTheBlock();

            // The block is not durable, and neither is it lifted by one: the restart runs the
            // recovery again, meets the same marker, and the same evidence refuses its rebuild.
            shutdown();
            restart();
            assertRebuildBlocked(instance("lv"), "prefix preservation repair marker present");
            assertViewRows(ALL_ROWS);
        });
    }

    @Test
    public void testABaseSchemaChangeRestoresInsteadOfRebuildingAndKeepsTheDayTheBaseLost() throws Exception {
        assertMemoryLeak(() -> {
            // The same drift as above, with nothing standing over the timeline. The recovery
            // restores the accumulators the recompile lost from the view's own newest root
            // rather than rebuilding the view from what its base holds today, so it never asks
            // the question the guard would have answered with a refusal.
            seedSixRows("DEDUP UPSERT KEYS(created_at, account_id)");
            dropPartitionAndRefresh("2026-01-01");

            execute("ALTER TABLE tx ADD COLUMN note INT");
            execute("INSERT INTO tx (created_at, account_id, amount) VALUES "
                    + "('2026-01-03T10:00:00.000000Z', 'acct-1', 30.0), "
                    + "('2026-01-03T10:00:00.000000Z', 'acct-1', 64.0)");
            drainWalQueue();
            final LiveViewRebuildRestatementGuard guard;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                guard = job.rebuildRestatementGuardForTest();
            }

            capture.drain();
            capture.assertLogged("live view restored its runtime from the checkpoint timeline [view=lv, cause=base table metadata change");
            capture.assertNotLogged("live view rebuild from the applied base refused");
            capture.assertNotLogged("live view recomputed window state from applied base");
            Assert.assertEquals(
                    "no whole-view rebuild may have been asked for",
                    LiveViewRebuildRestatementGuard.ABSTAIN_NOT_EVALUATED,
                    guard.getAbstention()
            );
            final LiveViewInstance instance = instance("lv");
            Assert.assertFalse("the view must keep refreshing", instance.isCheckpointRecoveryBlocked());
            Assert.assertEquals(1, instance.getCheckpointRuntimeRestores());
            Assert.assertEquals(0, instance.getCheckpointTimelineResets());
            Assert.assertFalse(instance.isInvalid());
            // The day the base lost is still in the view, and the commit that met the drift is
            // materialized on top of the accumulation the restore put back - with no restart.
            final String resumedRows = ALL_ROWS + "2026-01-03T10:00:00.000000Z\tacct-1\t80.0\t2\n";
            assertViewRows(resumedRows);

            shutdown();
            restart();
            assertRestoredFromTimeline("lv");
            assertNoRefreshFaults("lv");
            assertViewRows(resumedRows);
        });
    }

    @Test
    public void testALostBaseWalRederiveIsRefusedRatherThanInvalidated() throws Exception {
        assertMemoryLeak(() -> {
            seedSixRows("");
            // The base moves on while the view does not see it: a commit and the loss of the
            // oldest day, both applied to the base table only. This is the lag a backup
            // captures, and its restore brings back the table without the WAL that carried it.
            execute("INSERT INTO tx VALUES ('2026-01-03T10:00:00.000000Z', 'acct-1', 64.0)");
            execute("ALTER TABLE tx DROP PARTITION LIST '2026-01-01'");
            drainWalQueue();
            final long processedBefore = instance("lv").getLastProcessedSeqTxn();
            shutdown();
            removeBaseWal("tx");

            final LiveViewRebuildRestatementGuard guard = restart();

            capture.drain();
            capture.assertLogged("live view rebuild from the applied base refused, it would drop rows the view retains");
            capture.assertNotLogged("live view re-derived from the applied base after base WAL loss");
            final LiveViewInstance instance = instance("lv");
            // The re-derive is the last step before a durable invalidation. A refusal is not a
            // failure, so the view stops without being invalidated, and the restart the operator
            // runs after bringing the WAL back takes it up again.
            assertRebuildBlocked(instance, "base WAL segment missing");
            // The backlog the re-derive folds in is exactly what cannot be read. A base that is
            // neither a materialized view nor deduplicating under a filter has no commit there
            // that could remove a row legitimately, so the guard compares anyway - and refuses
            // on the floor, before the re-derive reads a row.
            Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_NONE, guard.getAbstention());
            Assert.assertEquals(LiveViewRebuildRestatementGuard.VERDICT_HISTORY_FLOOR, guard.getVerdict());
            assertViewRows(ALL_ROWS);
            Assert.assertEquals(processedBefore, instance.getLastProcessedSeqTxn());
            assertLiveViewsReportsTheBlock();
        });
    }

    @Test
    public void testALostBaseWalRederiveBehindTheViewsLeadComparesAndRefuses() throws Exception {
        assertMemoryLeak(() -> {
            seedSixRows("");
            dropPartitionAndRefresh("2026-01-01");
            final LiveViewInstance instance = instance("lv");
            final long processedBefore = instance.getLastProcessedSeqTxn();
            final LiveViewRebuildRestatementGuard guard;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // An un-flushed lead that runs one commit past the base's apply: the first commit
                // is applied before the view drains it, the second is not. The base's applied head
                // then sits strictly between the view's flushed watermark and its lead, which is
                // what lets the re-derive below run at all, and it pins that head.
                setCurrentMicros(instance.getLastFlushTimeUs());
                execute("INSERT INTO tx VALUES " + ROWS_AHEAD[0]);
                drainWalQueue();
                drainJob(job);
                execute("INSERT INTO tx VALUES " + ROWS_AHEAD[1]);
                drainJob(job);
                Assert.assertEquals(processedBefore, instance.getLastProcessedSeqTxn());
                Assert.assertEquals(processedBefore + 2, instance.getRefreshedUpToSeqTxn());
                // A third commit whose WAL segment is lost before anything applies or drains it.
                commitThroughASecondWalAndLoseIt(ROWS_AHEAD[2]);

                // The drain fails on the lost segment until the retry budget runs out, and the
                // re-derive that follows pins the base's applied head, one commit behind the lead.
                // That used to stand the guard down as a snapshot behind the view. The lead is not
                // in the view's table, though, and the re-derive drops it, so the snapshot holds
                // every commit the table does and the guard compares.
                final int drainsToExhaustTheBudget = engine.getConfiguration().getLiveViewFlushRetryMax() + 1;
                for (int i = 0; i < drainsToExhaustTheBudget; i++) {
                    failDrainOnTheLostSegment(job, processedBefore + 3);
                }
                guard = job.rebuildRestatementGuardForTest();
            }

            capture.drain();
            capture.assertLogged("live view rebuild from the applied base refused, it would drop rows the view retains");
            capture.assertNotLogged("live view re-derived from the applied base after base WAL loss");
            capture.assertNotLogged("live view rebuild from the applied base runs without the restatement guard");
            capture.assertNotLogged("live view rebuild from the applied base waits for the base table");
            assertRebuildBlocked(instance, "base WAL segment missing");
            Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_NONE, guard.getAbstention());
            Assert.assertEquals(LiveViewRebuildRestatementGuard.VERDICT_HISTORY_FLOOR, guard.getVerdict());
            Assert.assertEquals(6, guard.getDurableRows());
            Assert.assertEquals(processedBefore, instance.getLastProcessedSeqTxn());
            Assert.assertFalse("a refused re-derive must not invalidate the view", instance.isInvalid());
            // A view stopped at a running door keeps serving its in-memory lead.
            assertViewRows(ALL_ROWS + rowsAheadOutput(2));
        });
    }

    @Test
    public void testARebuildOverACompleteBasePassesTheGuard() throws Exception {
        assertMemoryLeak(() -> {
            seedSixRows("");
            shutdown();
            removeTimeline();

            final LiveViewRebuildRestatementGuard guard = restart();

            // The rebuild that heals a lost timeline over a base that still has every row goes
            // ahead exactly as it did before the guard - compared, and found to reproduce every
            // row the view held.
            assertRebuiltFromAppliedBase("lv");
            final LiveViewInstance instance = instance("lv");
            Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
            Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_NONE, guard.getAbstention());
            Assert.assertEquals(LiveViewRebuildRestatementGuard.VERDICT_NONE, guard.getVerdict());
            Assert.assertEquals(6, guard.getDurableRows());
            Assert.assertEquals(6, guard.getReproducedRows());
            assertNoRefreshFaults("lv");
            assertViewRows(ALL_ROWS);
            capture.drain();
            capture.assertNotLogged("live view rebuild from the applied base refused");
        });
    }

    @Test
    public void testARestartBehindARepairMarkerRefusesARebuildThatWouldDropAMiddleDay() throws Exception {
        assertMemoryLeak(() -> {
            seedSixRows("");
            // The MIDDLE day: the base's earliest row stays where it was, so the history floor
            // has nothing to see and only the recompute's own row count can.
            dropPartitionAndRefresh("2026-01-02");
            // A crash in the middle of a prefix-preserving repair leaves this marker, and the
            // restart that finds it live rebuilds rather than trust the timeline under it.
            writeRepairMarker(instance("lv"));
            final long generationBefore = newestGeneration(instance("lv"));
            final int boundariesBefore = countSealedBoundaries("lv");
            shutdown();

            final LiveViewRebuildRestatementGuard guard = restart();

            final LiveViewInstance instance = instance("lv");
            assertRebuildBlocked(instance, "prefix preservation repair marker present");
            Assert.assertEquals("rebuild_blocked", LiveViewCheckpointRestoreRoute.name(instance.getCheckpointRestoreRoute()));
            Assert.assertEquals(1, instance.getCheckpointRebuildAttempts());
            Assert.assertEquals(0, instance.getCheckpointTimelineResets());
            Assert.assertEquals(LiveViewRebuildRestatementGuard.VERDICT_ROW_SHORTFALL, guard.getVerdict());
            Assert.assertEquals(4, guard.getReproducedRows());
            TestUtils.assertContains(
                    instance.getCheckpointRecoveryReason(),
                    "the rebuild reproduces 4 of the 6 rows the view holds up to 2026-01-03T09:10:00.000000Z"
            );
            // The refusal came after the scan, with the replacement staged in the view's WAL
            // writer: closing the writer rolled it back, and the retire the rebuild owed never
            // ran, so the marker and the timeline under it are exactly as the crash left them.
            assertViewRows(ALL_ROWS);
            Assert.assertEquals(generationBefore, newestGeneration(instance));
            Assert.assertEquals(boundariesBefore, countSealedBoundaries("lv"));
            try (Path dir = checkpointsDir(instance)) {
                Assert.assertTrue(
                        "the marker must survive for the next restart to meet",
                        LiveViewCheckpointRepairMarker.exists(engine.getConfiguration().getFilesFacade(), dir)
                );
            }
            assertNoRefreshFaults("lv");
            capture.drain();
            capture.assertNotLogged("live view O3 head-miss replay completed");
        });
    }

    @Test
    public void testACrashDuringAParkedRepairRestoresOverTheDayTheBaseLost() throws Exception {
        assertMemoryLeak(() -> {
            seedSixRows("");
            dropPartitionAndRefresh("2026-01-01");
            parkRepairAndRestart(true);
        });
    }

    @Test
    public void testARestartDuringAParkedRepairRestoresOverTheDayTheBaseLost() throws Exception {
        assertMemoryLeak(() -> {
            seedSixRows("");
            dropPartitionAndRefresh("2026-01-01");
            parkRepairAndRestart(false);
        });
    }

    @Test
    public void testACrashDuringAParkedTruncatingRepairRestoresOverTheDayTheBaseLost() throws Exception {
        assertMemoryLeak(() -> {
            seedSixRows("");
            dropPartitionAndRefresh("2026-01-01");
            parkTruncatingRepairAndRestart(true);
        });
    }

    @Test
    public void testARestartDuringAParkedRepairWithNoCaptureRestoresOverTheDayTheBaseLost() throws Exception {
        // The other way into the truncate: the splice's capture cannot open, here because its
        // repair descriptor cannot be published, so the repair holds nothing to splice through.
        final AtomicBoolean isArmed = new AtomicBoolean();
        final TestFilesFacadeImpl ff = new TestFilesFacadeImpl() {
            @Override
            public int rename(LPSZ from, LPSZ to) {
                if (isArmed.get() && Utf8s.containsAscii(to, Files.SEPARATOR + LiveViewCheckpointLayout.REPAIR_DIR_NAME
                        + Files.SEPARATOR + LiveViewCheckpointLayout.REPAIR_DESCRIPTOR_PREFIX)) {
                    isArmed.set(false);
                    return Files.FILES_RENAME_ERR_OTHER;
                }
                return super.rename(from, to);
            }
        };
        assertMemoryLeak(ff, () -> {
            seedSixRows("");
            dropPartitionAndRefresh("2026-01-01");
            isArmed.set(true);
            parkRepairAndRestart(false);
            Assert.assertFalse("the descriptor fault must have fired", isArmed.get());
            capture.drain();
            capture.assertLogged("live view checkpoint timeline repair capture unavailable, retiring instead [view=lv");
            capture.assertLogged("live view O3 repair yielded on its turn budget [view=lv");
            capture.assertNotLogged("live view rebuild from the applied base refused");
        });
    }

    @Test
    public void testARestartDuringAParkedTruncatingRepairRestoresOverTheDayTheBaseLost() throws Exception {
        assertMemoryLeak(() -> {
            seedSixRows("");
            dropPartitionAndRefresh("2026-01-01");
            parkTruncatingRepairAndRestart(false);
        });
    }

    @Test
    public void testARestartDuringAParkedRepairRestoresOverTheDayTtlEvicted() throws Exception {
        assertMemoryLeak(() -> {
            seedSixRows("");
            // TTL measures a partition's age against the earlier of the table's newest row and
            // the wall clock, so the clock moves past the fixture's last day first.
            setCurrentMicros(ts("2026-01-05T00:00:00.000000Z"));
            execute("ALTER TABLE tx SET TTL 1 DAY");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            assertQuery("SELECT min(created_at), count() FROM tx")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            min\tcount
                            2026-01-02T09:00:00.000000Z\t4
                            """);
            assertViewRows(ALL_ROWS);
            parkRepairAndRestart(false);
        });
    }

    @Test
    public void testARestartDuringAnUnlocalizedRepairRestoresOverTheDayTheBaseLost() throws Exception {
        assertMemoryLeak(() -> {
            seedUnlocalizedView();
            dropPartitionAndRefresh("2026-01-01", UNLOCALIZED_ROWS);
            cancelUnlocalizedRepairAndRestart();
        });
    }

    @Test
    public void testABaseMetadataChangeUnderAnUnlocalizedRepairRestoresInPlaceOverTheDayTtlEvicted() throws Exception {
        // No shutdown this time. The TTL change moves the base's metadata version, which the view
        // first meets when its repair replay opens the base through the compiled SELECT, so that
        // replay throws and the drift recovery restores the runtime in place. It needs the timeline
        // the replay had not moved anything under: without one it rebuilds from the applied base,
        // meets the day TTL evicted and stops the view, with no restart involved.
        assertMemoryLeak(() -> {
            seedUnlocalizedView();
            // TTL measures a partition's age against the earlier of the table's newest row and
            // the wall clock, so the clock moves past the fixture's last day first.
            setCurrentMicros(ts("2026-01-05T00:00:00.000000Z"));
            execute("ALTER TABLE tx SET TTL 1 DAY");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            assertQuery("SELECT min(created_at), count() FROM tx")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            min\tcount
                            2026-01-02T09:00:00.000000Z\t4
                            """);
            assertViewRows(UNLOCALIZED_ROWS);
            final LiveViewInstance instance = instance("lv");
            final long processedBefore = instance.getLastProcessedSeqTxn();
            execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-02T09:05:00.000000Z', 'acct-1', 64.0)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }

            Assert.assertFalse("the view must keep refreshing", instance.isCheckpointRecoveryBlocked());
            capture.drain();
            capture.assertLogged("resumeFromAnchor=false");
            capture.assertLogged("live view restored its runtime from the checkpoint timeline [view=lv, cause=base table metadata change");
            capture.assertLogged("live view O3 head-miss replay completed [view=lv");
            capture.assertLogged("localized=false");
            capture.assertNotLogged("live view rebuild from the applied base refused");
            Assert.assertEquals(1, instance.getCheckpointRuntimeRestores());
            Assert.assertEquals("the drift is the one fault", 1, instance.getRefreshFaultCount());
            Assert.assertEquals("the retry must consume the correction", processedBefore + 1, instance.getLastProcessedSeqTxn());
            assertViewRows(UNLOCALIZED_CORRECTED_ROWS);

            shutdown();
            restart();
            assertRestoredFromTimeline("lv");
            Assert.assertFalse(instance("lv").isCheckpointRecoveryBlocked());
            assertViewRows(UNLOCALIZED_CORRECTED_ROWS);
            assertNoRefreshFaults("lv");
        });
    }

    @Test
    public void testARestartDuringAFilteredResumeWithNoCaptureRestoresOverTheDayTheBaseLost() throws Exception {
        // A resume that declines the checkpoint chain truncates the timeline instead of
        // re-versioning the roots above its anchor. Its own row loop never consults the circuit
        // breaker, but the filter cursor under it does on every row it pulls, so an engine
        // shutdown trips a filtered resume in the middle of its replay. The view's refresh is
        // cancelled once the resume has restored its anchor and opened its cursors, which
        // consult the breaker too, and before its replay pulls a row. The breaker's throttle
        // then lets a later row of the replay find the flag, which the four rows committed above
        // the anchor make sure it reaches.
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 0);
            seedSixRows("", "WHERE amount > 0");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (String row : ROWS_AHEAD) {
                    execute("INSERT INTO tx VALUES " + row);
                    drainWalQueue();
                    driveRefreshToQuiescence(job);
                }
            }
            final String rowsWithRowsAhead = ALL_ROWS + String.join("", ROWS_AHEAD_OUTPUT);
            dropPartitionAndRefresh("2026-01-01", rowsWithRowsAhead);
            final LiveViewInstance cancelled = instance("lv");
            final long generationBefore = newestGeneration(cancelled);
            final long processedBefore = cancelled.getLastProcessedSeqTxn();
            execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-03T09:05:00.000000Z', 'acct-1', 64.0)");
            drainWalQueue();
            final AtomicBoolean hasReplayStarted = new AtomicBoolean();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                job.setSimulateResumeReplayStartForTest(() -> {
                    hasReplayStarted.set(true);
                    cancelled.cancelRefresh();
                });
                for (int pass = 0; pass < REFRESH_QUIESCENCE_PASSES && cancelled.getRefreshFaultCount() == 0; pass++) {
                    setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                    drainWalQueue();
                    job.processNotificationsForTest();
                }
            }
            Assert.assertTrue("the resume replay must have started", hasReplayStarted.get());
            Assert.assertEquals("the breaker must have ended the replay exactly once", 1, cancelled.getRefreshFaultCount());
            capture.drain();
            capture.assertLogged("resumeFromAnchor=true");
            capture.assertLogged("live view O3 resume declined the checkpoint chain, truncating instead [view=lv");
            capture.assertLogged("live view refresh cancelled [view=lv");
            capture.assertNotLogged("live view O3 resume replay completed");
            Assert.assertEquals("a cancelled replay must not consume the correction", processedBefore, cancelled.getLastProcessedSeqTxn());
            assertViewRows(rowsWithRowsAhead);
            final long generationAfterCancel = newestGeneration(cancelled);
            final boolean isMarkerOnDiskAfterCancel;
            try (Path dir = checkpointsDir(cancelled)) {
                isMarkerOnDiskAfterCancel = LiveViewCheckpointRepairMarker.exists(engine.getConfiguration().getFilesFacade(), dir);
            }
            shutdown();

            restart();
            assertRestoredFromTimeline("lv");
            final LiveViewInstance restored = instance("lv");
            Assert.assertFalse("the view must keep refreshing", restored.isCheckpointRecoveryBlocked());
            Assert.assertEquals("the restart must consume the correction", processedBefore + 1, restored.getLastProcessedSeqTxn());
            assertViewRows(CORRECTED_DAY_THREE_ROWS_AHEAD);
            assertNoRefreshFaults("lv");
            capture.drain();
            capture.assertLogged("live view O3 resume replay completed [view=lv");
            capture.assertNotLogged("live view rebuild from the applied base refused");

            shutdown();
            restart();
            assertRestoredFromTimeline("lv");
            Assert.assertFalse(instance("lv").isCheckpointRecoveryBlocked());
            assertViewRows(CORRECTED_DAY_THREE_ROWS_AHEAD);
            assertNoRefreshFaults("lv");

            // What made both recoveries sound: the cancelled replay had moved nothing durable,
            // so the restart found the timeline it resumed from intact and no marker over it.
            Assert.assertFalse("a cancelled replay owes no repair marker", isMarkerOnDiskAfterCancel);
            Assert.assertEquals("a cancelled replay must not publish a generation", generationBefore, generationAfterCancel);
        });
    }

    @Test
    public void testACrashDuringAResumeReplayRestoresOverTheDayTheBaseLost() throws Exception {
        final StagedSegmentOpenFault fault = new StagedSegmentOpenFault();
        assertMemoryLeak(fault, () -> {
            seedSixRows("");
            dropPartitionAndRefresh("2026-01-01");
            failResumeReplayAndRestart(fault, true);
        });
    }

    @Test
    public void testAnUnwritableMarkerRetiresAResumeAheadOfItsCommit() throws Exception {
        // A resume that cannot write its repair marker must not splice unprotected. It learns that
        // at its replacement commit, after its replay has frozen the roots it re-versions, so it
        // drops them there, retires the timeline ahead of the commit and publishes as a resume
        // holding no capture. The retire costs the view its ladder and nothing else: the day the
        // base lost stays in the view, and the head seal opens a history the restart restores from.
        final AtomicBoolean isArmed = new AtomicBoolean();
        final AtomicInteger stagedSegmentOpens = new AtomicInteger();
        final LongList stagedSegmentOpensAtMarkerAttempts = new LongList();
        final LongList lvSeqTxnAtMarkerAttempts = new LongList();
        final TestFilesFacadeImpl ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (isArmed.get() && isStagedSegment(name)) {
                    stagedSegmentOpens.incrementAndGet();
                }
                return super.openRW(name, opts);
            }

            @Override
            public int rename(LPSZ from, LPSZ to) {
                if (isArmed.get() && Utf8s.containsAscii(to, LiveViewCheckpointLayout.REPAIRING_MARKER_FILE_NAME)) {
                    stagedSegmentOpensAtMarkerAttempts.add(stagedSegmentOpens.get());
                    lvSeqTxnAtMarkerAttempts.add(engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("lv")));
                    return Files.FILES_RENAME_ERR_OTHER;
                }
                return super.rename(from, to);
            }
        };
        assertMemoryLeak(ff, () -> {
            seedSixRows("");
            dropPartitionAndRefresh("2026-01-01");
            final LiveViewInstance instance = instance("lv");
            final long processedBefore = instance.getLastProcessedSeqTxn();
            final long lvSeqTxnBefore = engine.getTableSequencerAPI().lastTxn(instance.getLiveViewToken());
            execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-03T09:05:00.000000Z', 'acct-1', 64.0)");
            drainWalQueue();
            isArmed.set(true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            isArmed.set(false);

            capture.drain();
            capture.assertLogged("resumeFromAnchor=true");
            capture.assertLogged("could not write the live view checkpoint repair marker [view=lv");
            Assert.assertEquals("the resume attempts its marker once", 1, lvSeqTxnAtMarkerAttempts.size());
            Assert.assertTrue(
                    "the marker attempt must follow the replay's first staged root",
                    stagedSegmentOpensAtMarkerAttempts.getQuick(0) > 0
            );
            Assert.assertEquals(
                    "the marker attempt must precede the replacement commit",
                    lvSeqTxnBefore,
                    lvSeqTxnAtMarkerAttempts.getQuick(0)
            );
            Assert.assertTrue(
                    "the replacement must have committed after the attempt",
                    engine.getTableSequencerAPI().lastTxn(instance.getLiveViewToken()) > lvSeqTxnBefore
            );
            Assert.assertEquals("the correction must be consumed", processedBefore + 1, instance.getLastProcessedSeqTxn());
            Assert.assertTrue("the repair must resume from the anchor", instance.getO3ResumeReplayRows() > 0);
            Assert.assertEquals("no marker, no splice", 0, instance.getCheckpointRepairRootsVersioned());
            Assert.assertTrue("the retire must have run", instance.getCheckpointTimelineResets() > 0);
            Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
            try (Path dir = checkpointsDir(instance)) {
                Assert.assertFalse(
                        "a retired timeline owes no marker",
                        LiveViewCheckpointRepairMarker.exists(engine.getConfiguration().getFilesFacade(), dir)
                );
            }
            assertNoRefreshFaults("lv");
            assertViewRows(CORRECTED_DAY_THREE_ROWS);

            shutdown();
            restart();
            assertRestoredFromTimeline("lv");
            Assert.assertFalse(instance("lv").isCheckpointRecoveryBlocked());
            assertViewRows(CORRECTED_DAY_THREE_ROWS);
            assertNoRefreshFaults("lv");
        });
    }

    @Test
    public void testAResumeReplayFaultRestoresInPlaceOverTheDayTheBaseLost() throws Exception {
        final StagedSegmentOpenFault fault = new StagedSegmentOpenFault();
        assertMemoryLeak(fault, () -> {
            seedSixRows("");
            dropPartitionAndRefresh("2026-01-01");
            failResumeReplayAndRestart(fault, false);
        });
    }

    @Test
    public void testARestartWithNoTimelineRefusesARebuildThatWouldDropTheOldestDay() throws Exception {
        assertMemoryLeak(() -> {
            seedSixRows("");
            dropPartitionAndRefresh("2026-01-01");
            assertViewRows(ALL_ROWS);
            final long processedBefore = instance("lv").getLastProcessedSeqTxn();
            shutdown();
            // A restart with no timeline to restore from, which is what a directory reset for its
            // format, a history-epoch replacement or a timeline an earlier failure retired leaves.
            removeTimeline();

            final LiveViewRebuildRestatementGuard guard = restart();

            // Refused before the rebuild read a row: the view's first row is older than anything
            // the base still holds, which two transaction-file reads are enough to see.
            capture.drain();
            capture.assertLogged("live view restart rebuilding from applied base");
            capture.assertLogged("live view rebuild from the applied base refused, it would drop rows the view retains");
            capture.assertNotLogged("live view O3 head-miss replay completed");
            final LiveViewInstance instance = instance("lv");
            assertRebuildBlocked(instance, "timeline is absent");
            Assert.assertEquals("rebuild_blocked", LiveViewCheckpointRestoreRoute.name(instance.getCheckpointRestoreRoute()));
            Assert.assertFalse(
                    "a refused rebuild resolved no derived state, so it must not report success",
                    instance.isCheckpointRestoreSucceeded()
            );
            Assert.assertEquals(1, instance.getCheckpointRebuildAttempts());
            Assert.assertEquals(LiveViewRebuildRestatementGuard.VERDICT_HISTORY_FLOOR, guard.getVerdict());
            Assert.assertEquals("the floor needs no scan", 0, guard.getReproducedRows());
            assertViewRows(ALL_ROWS);
            assertLiveViewsReportsTheBlock();

            // Refresh is stopped, not merely the restore: a base commit moves neither the rows
            // nor the watermark, and nothing faults.
            execute("INSERT INTO tx VALUES ('2026-01-03T10:00:00.000000Z', 'acct-1', 64.0)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            assertViewRows(ALL_ROWS);
            Assert.assertEquals(processedBefore, instance("lv").getLastProcessedSeqTxn());
            assertNoRefreshFaults("lv");

            // Nothing on disk records the block, and nothing needs to: the next restart asks the
            // same question of the same evidence.
            shutdown();
            restart();
            assertRebuildBlocked(instance("lv"), "timeline is absent");
            assertViewRows(ALL_ROWS);

            // The operator's exit is a deliberate recomputation from what the base holds today.
            execute("DROP LIVE VIEW lv");
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);
            }
            Assert.assertFalse(instance("lv").isCheckpointRecoveryBlocked());
            assertViewRows("""
                    created_at\taccount_id\tcumulative_sum\tcumulative_count
                    2026-01-02T09:00:00.000000Z\tacct-1\t4.0\t1
                    2026-01-02T09:10:00.000000Z\tacct-1\t12.0\t2
                    2026-01-03T09:00:00.000000Z\tacct-1\t16.0\t1
                    2026-01-03T09:10:00.000000Z\tacct-2\t32.0\t1
                    2026-01-03T10:00:00.000000Z\tacct-1\t80.0\t2
                    """);
        });
    }

    @Test
    public void testARebuildAheadOfTheBaseApplyWaitsForItAndHealsACompleteBase() throws Exception {
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        assertMemoryLeak(fault.facade(), () -> {
            seedSixRows("");
            fault.of(engine.verifyTableName("tx").getDirName());
            final LiveViewInstance instance = instance("lv");
            final long baseApplied = instance.getLastProcessedSeqTxn();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                flushAheadOfTheBaseApply(job);
                // Keeps the mid-drain recovery's restore off the timeline, so it asks for the
                // whole-view rebuild.
                writeRepairMarker(instance);
                failMidDrainAheadOfTheBaseApply(job, fault, 1);
                // Pinned where the base had applied, the rebuild would hold the view's table, which
                // has the flushed commit's row, against a snapshot that lacks it: seven rows held
                // against six reproduced, and a refusal of a rebuild that restates nothing. The
                // previous code stood the guard down there instead and ran the rebuild unchecked.
                assertRebuildDeferred(job, instance, baseApplied);
                assertViewRows(ALL_ROWS + rowsAheadOutput(2));

                // One turn after the base applies the four commits. The deferred recovery's
                // rebuild pins a snapshot holding every commit the view's table has output of, so
                // the guard compares - and finds every row the view holds reproduced.
                drainWalQueue();
                drainJob(job);
                final LiveViewRebuildRestatementGuard guard = job.rebuildRestatementGuardForTest();
                Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_NONE, guard.getAbstention());
                Assert.assertEquals(LiveViewRebuildRestatementGuard.VERDICT_NONE, guard.getVerdict());
                Assert.assertEquals(7, guard.getDurableRows());
                Assert.assertEquals(7, guard.getReproducedRows());
                // The cost of the wait: the rebuild commits at the base's head, four commits past
                // the point the base had applied when the fault landed, so it materializes the
                // lead's commit and the two the fault interrupted itself rather than leaving them
                // to the next drain.
                Assert.assertEquals(baseApplied + 4, instance.getLastProcessedSeqTxn());
                Assert.assertFalse(instance.isWindowStateDirty());
                // The rebuild ran, so the view no longer waits for anything, and says so.
                assertLiveViewsReportsNoRecovery(instance);
                driveRefreshToQuiescence(job);
            }

            capture.drain();
            capture.assertLogged("live view recomputed window state from applied base [view=lv, cause=mid-drain refresh failure]");
            capture.assertNotLogged("live view rebuild from the applied base runs without the restatement guard");
            Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
            Assert.assertFalse(instance.isInvalid());
            assertViewRows(ALL_ROWS + rowsAheadOutput(4));
        });
    }

    @Test
    public void testARebuildAheadOfTheBaseApplyWaitsForItAndRefusesToDropTheOldestDay() throws Exception {
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        assertMemoryLeak(fault.facade(), () -> {
            seedSixRows("");
            fault.of(engine.verifyTableName("tx").getDirName());
            dropPartitionAndRefresh("2026-01-01");
            final LiveViewInstance instance = instance("lv");
            final long baseApplied = instance.getLastProcessedSeqTxn();
            final long generationBefore;
            final LiveViewRebuildRestatementGuard guard;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                flushAheadOfTheBaseApply(job);
                writeRepairMarker(instance);
                generationBefore = newestGeneration(instance);
                failMidDrainAheadOfTheBaseApply(job, fault, 1);
                // Pinned where the base had applied, the rebuild would have stood the guard down and
                // replaced the view with what the surviving days produce. It waits instead.
                assertRebuildDeferred(job, instance, baseApplied);
                final String deferralReason = instance.getCheckpointRecoveryReason();
                assertViewRows(ALL_ROWS + rowsAheadOutput(2));

                // The back-off only paces the retries. Once it has elapsed, a later commit's
                // notification brings the view back with the base still behind: the window-state
                // gate takes the debt, the restore declines again, and the rebuild defers again -
                // through the refresh turn's own apply-lag arm this time, with no fault counted and
                // no second log line for the same target.
                setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                engine.getLiveViewStateStore().notifyBaseTableCommit(engine.verifyTableName("tx"), baseApplied + 4);
                drainJob(job);
                capture.drain();
                // The retry reached the gate: the restore declined a second time.
                capture.assertLoggedRE("(?s)live view cannot restore its runtime from the checkpoint timeline.*"
                        + "live view cannot restore its runtime from the checkpoint timeline");
                capture.assertOnlyOnce("live view rebuild from the applied base waits for the base table to apply what the view consumed");
                Assert.assertTrue(instance.isWindowStateDirty());
                Assert.assertEquals(baseApplied + 1, instance.getApplyLagDeferTargetSeqTxn());
                Assert.assertEquals(1, instance.getRefreshFaultCount());
                Assert.assertEquals(0, instance.getFlushRetryCount());
                Assert.assertEquals(
                        LiveViewRebuildRestatementGuard.ABSTAIN_NOT_EVALUATED,
                        job.rebuildRestatementGuardForTest().getAbstention()
                );
                Assert.assertEquals(baseApplied + 1, instance.getLastProcessedSeqTxn());
                assertViewRows(ALL_ROWS + rowsAheadOutput(2));
                // Still waiting, and still saying so with the reason the first deferral published:
                // a retry on the same target builds nothing new.
                Assert.assertTrue(instance.isCheckpointRebuildDeferred());
                Assert.assertSame(deferralReason, instance.getCheckpointRecoveryReason());

                drainWalQueue();
                driveRefreshToQuiescence(job);
                guard = job.rebuildRestatementGuardForTest();
            }

            capture.drain();
            capture.assertLogged("live view rebuild from the applied base refused, it would drop rows the view retains");
            capture.assertNotLogged("live view rebuild from the applied base runs without the restatement guard");
            assertRebuildBlocked(instance, "mid-drain refresh failure");
            Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_NONE, guard.getAbstention());
            Assert.assertEquals(LiveViewRebuildRestatementGuard.VERDICT_HISTORY_FLOOR, guard.getVerdict());
            TestUtils.assertContains(
                    instance.getCheckpointRecoveryReason(),
                    "the view holds rows from 2026-01-01T09:00:00.000000Z but the base table's earliest row is at 2026-01-02T09:00:00.000000Z"
            );
            Assert.assertEquals(baseApplied + 1, instance.getLastProcessedSeqTxn());
            Assert.assertEquals(generationBefore, newestGeneration(instance));
            try (Path dir = checkpointsDir(instance)) {
                Assert.assertTrue(LiveViewCheckpointRepairMarker.exists(engine.getConfiguration().getFilesFacade(), dir));
            }
            assertLiveViewsReportsTheBlock();
            // A view stopped at a running door keeps serving its in-memory lead.
            assertViewRows(ALL_ROWS + rowsAheadOutput(2));
        });
    }

    @Test
    public void testARebuildWaitingOnASuspendedBaseApplyReportsTheWaitUntilTheApplyResumes() throws Exception {
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        assertMemoryLeak(fault.facade(), () -> {
            seedSixRows("");
            fault.of(engine.verifyTableName("tx").getDirName());
            final TableToken baseToken = engine.verifyTableName("tx");
            final LiveViewInstance instance = instance("lv");
            final long baseApplied = instance.getLastProcessedSeqTxn();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                flushAheadOfTheBaseApply(job);
                writeRepairMarker(instance);
                // The base's WAL apply stops with the view's table holding output of a commit it
                // never applied - an operator's SUSPEND WAL here, a failed apply in the wild. Nothing
                // on the view's side bounds the wait that follows: a deferral charges no retry, so
                // only the apply ends it.
                execute("ALTER TABLE tx SUSPEND WAL");
                failMidDrainAheadOfTheBaseApply(job, fault, 1);
                assertRebuildDeferred(job, instance, baseApplied);
                final String deferralReason = instance.getCheckpointRecoveryReason();

                // Retry after retry, the view waits and keeps saying what it waits for. Before the
                // phase existed, this stretch showed only as a lag behind the base.
                for (int i = 0; i < 3; i++) {
                    drainWalQueue();
                    setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                    engine.getLiveViewStateStore().notifyBaseTableCommit(baseToken, baseApplied + 4);
                    drainJob(job);
                }
                Assert.assertTrue(engine.isWalApplySuspended(baseToken));
                Assert.assertEquals(baseApplied, engine.getTableSequencerAPI().getTxnTracker(baseToken).getWriterTxn());
                capture.drain();
                capture.assertOnlyOnce("live view rebuild from the applied base waits for the base table to apply what the view consumed");
                Assert.assertTrue(instance.isCheckpointRebuildDeferred());
                Assert.assertSame(deferralReason, instance.getCheckpointRecoveryReason());
                // Three retries, one wait. base_apply_wait_micros measures from the deferral that
                // opened it, not from the last retry, which is what makes a suspended base look
                // different from a base that is merely a window behind.
                assertQuery("SELECT base_apply_wait_seqtxn, base_apply_wait_micros "
                        + "FROM live_views() WHERE view_name = 'lv'")
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("base_apply_wait_seqtxn\tbase_apply_wait_micros\n"
                                + (baseApplied + 1) + "\t" + (3 * CLOCK_ADVANCE_MICROS) + "\n");
                Assert.assertTrue(instance.isWindowStateDirty());
                Assert.assertEquals(1, instance.getRefreshFaultCount());
                Assert.assertEquals(0, instance.getFlushRetryCount());
                Assert.assertFalse(instance.isInvalid());
                Assert.assertEquals(baseApplied + 1, instance.getLastProcessedSeqTxn());
                assertViewRows(ALL_ROWS + rowsAheadOutput(2));

                // The apply resumes and lands the four commits, which ends the wait whatever the
                // rebuild then does. Here its first run fails on the applied base's scan: the view
                // owes the recovery still, and is charged for the failure, but no longer reports a
                // wait the base has already satisfied.
                execute("ALTER TABLE tx RESUME WAL");
                drainWalQueue();
                // A fresh base reader, so the rebuild's scan opens the column the fault fails. One
                // pass of the job rather than a drain, because the turn after the failure heals.
                engine.releaseInactive();
                fault.armAppliedScan();
                job.run();
                Assert.assertTrue("the rebuild's scan must have been failed once", fault.hasAppliedScanFired());
                capture.drain();
                capture.assertLogged("live view window-state recompute failed [view=lv, cause=mid-drain refresh failure");
                Assert.assertTrue(instance.isWindowStateDirty());
                Assert.assertEquals(2, instance.getRefreshFaultCount());
                Assert.assertEquals(1, instance.getFlushRetryCount());
                assertLiveViewsReportsNoRecovery(instance);

                // The next turn's rebuild pins a snapshot holding all four commits, compares,
                // finds every row the view holds, and heals.
                driveRefreshToQuiescence(job);
            }

            capture.drain();
            capture.assertLogged("live view recomputed window state from applied base [view=lv, cause=mid-drain refresh failure]");
            capture.assertNotLogged("live view rebuild from the applied base refused");
            assertLiveViewsReportsNoRecovery(instance);
            Assert.assertFalse(instance.isWindowStateDirty());
            Assert.assertEquals(baseApplied + 4, instance.getLastProcessedSeqTxn());
            assertViewRows(ALL_ROWS + rowsAheadOutput(4));
        });
    }

    @Test
    public void testARetryThatRestoresFromTheTimelineEndsTheWaitBeforeTheBaseApplies() throws Exception {
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        assertMemoryLeak(fault.facade(), () -> {
            seedSixRows("");
            fault.of(engine.verifyTableName("tx").getDirName());
            final TableToken baseToken = engine.verifyTableName("tx");
            final LiveViewInstance instance = instance("lv");
            final long baseApplied = instance.getLastProcessedSeqTxn();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                flushAheadOfTheBaseApply(job);
                writeRepairMarker(instance);
                failMidDrainAheadOfTheBaseApply(job, fault, 1);
                assertRebuildDeferred(job, instance, baseApplied);

                // What kept the recovery off the timeline goes away while the base is still behind.
                // A retry tries the restore before the rebuild, so this one restores in place, which
                // needs nothing from the base's apply, and the wait ends without the apply landing.
                try (Path dir = checkpointsDir(instance)) {
                    LiveViewCheckpointRepairMarker.clear(engine.getConfiguration().getFilesFacade(), dir);
                }
                setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                engine.getLiveViewStateStore().notifyBaseTableCommit(baseToken, baseApplied + 4);
                drainJob(job);

                capture.drain();
                capture.assertLogged("live view restored its runtime from the checkpoint timeline [view=lv, cause=mid-drain refresh failure");
                capture.assertNotLogged("live view recomputed window state from applied base");
                Assert.assertEquals(1, instance.getCheckpointRuntimeRestores());
                Assert.assertEquals(
                        "no whole-view rebuild may have run",
                        LiveViewRebuildRestatementGuard.ABSTAIN_NOT_EVALUATED,
                        job.rebuildRestatementGuardForTest().getAbstention()
                );
                Assert.assertEquals(baseApplied, engine.getTableSequencerAPI().getTxnTracker(baseToken).getWriterTxn());
                Assert.assertFalse(instance.isWindowStateDirty());
                assertLiveViewsReportsNoRecovery(instance);

                driveRefreshToQuiescence(job);
            }

            Assert.assertEquals("the mid-drain fault is the one fault", 1, instance.getRefreshFaultCount());
            assertLiveViewsReportsNoRecovery(instance);
            assertViewRows(ALL_ROWS + rowsAheadOutput(4));
        });
    }

    @Test
    public void testADroppedViewStopsWaitingForTheRebuildItWasDeferring() throws Exception {
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        assertMemoryLeak(fault.facade(), () -> {
            seedSixRows("");
            fault.of(engine.verifyTableName("tx").getDirName());
            final TableToken baseToken = engine.verifyTableName("tx");
            final LiveViewInstance instance = instance("lv");
            final long baseApplied = instance.getLastProcessedSeqTxn();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                flushAheadOfTheBaseApply(job);
                writeRepairMarker(instance);
                failMidDrainAheadOfTheBaseApply(job, fault, 1);
                assertRebuildDeferred(job, instance, baseApplied);
            }

            // The operator drops the view rather than waiting the base's apply out - the exit the
            // suspended-base case shows an operator needs, taken. DROP LIVE VIEW fences the
            // refresh worker and closes the instance on the SQL thread, and tryCloseIfDropped's
            // two clears run there under the refresh latch. Nothing else on that path clears
            // either one: close() clears neither, and no refresh turn runs again.
            //
            // Unlike the invalidation the two clears mirror, this one cannot be read through
            // live_views() - the row is gone before the clear could be reported - so the drop's
            // disposition is read off the instance, which the caller still holds.
            execute("DROP LIVE VIEW lv");
            Assert.assertTrue(instance.isDropped());
            Assert.assertEquals(LiveViewCheckpointRecoveryPhase.NONE, instance.getCheckpointRecoveryPhase());
            Assert.assertNull(instance.getCheckpointRecoveryReason());
            Assert.assertFalse(instance.isCheckpointRebuildDeferred());
            Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
            // A deferred rebuild is an apply-lag wait as well, so the drop ends both halves.
            Assert.assertEquals(Numbers.LONG_NULL, instance.getApplyLagDeferTargetSeqTxn());
            Assert.assertEquals(Numbers.LONG_NULL, instance.getApplyLagDeferSinceUs());
            Assert.assertEquals(Numbers.LONG_NULL, instance.getApplyLagDeferUntilUs());

            // The base is left holding the commits the view never consumed, and is a table like
            // any other once the view that lagged it is gone: its apply lands them, and nothing
            // is waiting on it.
            drainWalQueue();
            Assert.assertEquals(baseApplied + 4, engine.getTableSequencerAPI().getTxnTracker(baseToken).getWriterTxn());
            Assert.assertNull(engine.getLiveViewRegistry().getViewInstance("lv"));
        });
    }

    @Test
    public void testABaseSchemaChangeAheadOfTheBaseApplyDefersTheRebuildUntilTheApplyInvalidatesTheView() throws Exception {
        assertMemoryLeak(() -> {
            seedSixRows("");
            final TableToken baseToken = engine.verifyTableName("tx");
            final LiveViewInstance instance = instance("lv");
            final long baseApplied = instance.getLastProcessedSeqTxn();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                flushAheadOfTheBaseApply(job);
                // Keeps the drift's own recovery - restoring the accumulators from the timeline in
                // place - off the timeline, so it falls back to the whole-view rebuild.
                writeRepairMarker(instance);
                // The retype and the row that carries it into a fresh WAL segment are sequenced but
                // not applied. The sequencer notifies live views at COMMIT time, so the raw-WAL
                // drain reaches a segment whose 'amount' is no longer the DOUBLE the compiled
                // projection strides and bails with the drift, while the base's apply - and the
                // invalidation the retype earns there - is still behind the commit the view has
                // already flushed. That is what puts a drift in front of a rebuild the view's own
                // coordinate is ahead of; every other route to a drift reads the applied base and
                // so waits for the apply before it can drift at all.
                execute("ALTER TABLE tx ALTER COLUMN amount TYPE FLOAT");
                execute("INSERT INTO tx VALUES " + ROWS_AHEAD[1]);
                drainJob(job);

                // The drift door's own deferral arm. The rebuild it asked for would pin the base's
                // applied head, behind the commit the view's table already holds output of, so it
                // waits instead - and the wait reaches the back-off rather than escaping the turn's
                // failure handling, which has no other arm that would catch it.
                assertRebuildDeferred(job, instance, DRIFT_CAUSE, baseApplied);
                final String deferralReason = instance.getCheckpointRecoveryReason();
                Assert.assertFalse("the apply has not landed the retype yet", instance.isInvalid());
                assertViewRows(ALL_ROWS + rowsAheadOutput(1));

                // A retry once the back-off has elapsed, with the base still behind: the gate takes
                // the debt the drift left on the instance, the restore declines again, and the
                // rebuild defers again on the same target. The gate recovers any carried debt under
                // the mid-drain cause, so the retry's own line would name that one - there is no
                // second line, because the target has not moved.
                setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                engine.getLiveViewStateStore().notifyBaseTableCommit(baseToken, baseApplied + 3);
                drainJob(job);
                capture.drain();
                capture.assertOnlyOnce("live view rebuild from the applied base waits for the base table to apply what the view consumed");
                Assert.assertTrue(instance.isCheckpointRebuildDeferred());
                Assert.assertSame(deferralReason, instance.getCheckpointRecoveryReason());
                Assert.assertTrue(instance.isWindowStateDirty());
                Assert.assertEquals("the drift is the one fault", 1, instance.getRefreshFaultCount());
                Assert.assertEquals("a deferral charges no retry", 0, instance.getFlushRetryCount());
                Assert.assertEquals(baseApplied + 1, instance.getLastProcessedSeqTxn());
                Assert.assertEquals(
                        LiveViewRebuildRestatementGuard.ABSTAIN_NOT_EVALUATED,
                        job.rebuildRestatementGuardForTest().getAbstention()
                );

                // The apply lands the retype, which invalidates the view on the referenced column
                // it changed. That is where this drift was always going to end: the rebuild the
                // wait was for never runs, and a view that has stopped refreshing waits for
                // nothing, so both the phase and the two apply-wait columns clear.
                drainWalQueue();
                drainJob(job);
            }

            capture.drain();
            capture.assertNotLogged("live view recomputed window state from applied base");
            capture.assertNotLogged("live view rebuild from the applied base refused");
            Assert.assertTrue(instance.isInvalid());
            TestUtils.assertContains(instance.getInvalidationReason(), "change column type operation [column=amount]");
            Assert.assertFalse(instance.isCheckpointRebuildDeferred());
            Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
            Assert.assertEquals(LiveViewCheckpointRecoveryPhase.NONE, instance.getCheckpointRecoveryPhase());
            Assert.assertNull(instance.getCheckpointRecoveryReason());
            Assert.assertEquals(Numbers.LONG_NULL, instance.getApplyLagDeferTargetSeqTxn());
            Assert.assertEquals(Numbers.LONG_NULL, instance.getApplyLagDeferSinceUs());
            assertQuery("SELECT view_status, checkpoint_recovery_phase, checkpoint_recovery_reason, "
                    + "base_apply_wait_seqtxn, base_apply_wait_micros "
                    + "FROM live_views() WHERE view_name = 'lv'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            view_status\tcheckpoint_recovery_phase\tcheckpoint_recovery_reason\tbase_apply_wait_seqtxn\tbase_apply_wait_micros
                            invalid\t\t\tnull\tnull
                            """);
            // An invalidated view stays queryable, and the drift never let a row of the drifted
            // segment through: the view holds what it held before the retype was sequenced.
            assertViewRows(ALL_ROWS + rowsAheadOutput(1));
        });
    }

    @Test
    public void testARebuildBehindTheViewsLeadComparesAtOnceAndRefuses() throws Exception {
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        assertMemoryLeak(fault.facade(), () -> {
            seedSixRows("");
            fault.of(engine.verifyTableName("tx").getDirName());
            dropPartitionAndRefresh("2026-01-01");
            final LiveViewInstance instance = instance("lv");
            final long baseApplied = instance.getLastProcessedSeqTxn();
            final LiveViewRebuildRestatementGuard guard;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                writeRepairMarker(instance);
                // The view drains the first of three unapplied commits into its lead, so the lead
                // runs past the base's apply while its table does not. The rebuild pins where the
                // base had applied, which holds every commit the table has output of, and drops the
                // lead: the guard compares without waiting for anything.
                failMidDrainAheadOfTheBaseApply(job, fault, 0);
                guard = job.rebuildRestatementGuardForTest();
            }

            capture.drain();
            capture.assertLogged("live view rebuild from the applied base refused, it would drop rows the view retains");
            capture.assertNotLogged("live view rebuild from the applied base waits for the base table");
            capture.assertNotLogged("live view rebuild from the applied base runs without the restatement guard");
            assertRebuildBlocked(instance, "mid-drain refresh failure");
            Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_NONE, guard.getAbstention());
            Assert.assertEquals(LiveViewRebuildRestatementGuard.VERDICT_HISTORY_FLOOR, guard.getVerdict());
            Assert.assertEquals(6, guard.getDurableRows());
            Assert.assertEquals(baseApplied, instance.getLastProcessedSeqTxn());
            assertViewRows(ALL_ROWS + rowsAheadOutput(1));
        });
    }

    @Test
    public void testATurnedOffGuardLetsTheRebuildFollowTheBase() throws Exception {
        assertMemoryLeak(() -> {
            seedSixRows("");
            dropPartitionAndRefresh("2026-01-01");
            shutdown();
            removeTimeline();
            // The escape hatch: an operator who would rather the view track the base's retention
            // than stop gets the rebuild every release before the guard ran.
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_REBUILD_RESTATEMENT_GUARD_ENABLED, "false");

            final LiveViewRebuildRestatementGuard guard = restart();

            assertRebuiltFromAppliedBase("lv");
            Assert.assertFalse(instance("lv").isCheckpointRecoveryBlocked());
            Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_DISABLED, guard.getAbstention());
            // The restatement the guard exists to refuse, now asked for: the dropped day's rows
            // are gone from the view.
            assertViewRows("""
                    created_at\taccount_id\tcumulative_sum\tcumulative_count
                    2026-01-02T09:00:00.000000Z\tacct-1\t4.0\t1
                    2026-01-02T09:10:00.000000Z\tacct-1\t12.0\t2
                    2026-01-03T09:00:00.000000Z\tacct-1\t16.0\t1
                    2026-01-03T09:10:00.000000Z\tacct-2\t32.0\t1
                    """);
        });
    }

    @Test
    public void testABacklogThatLegitimatelyRemovesARowIsNotRefused() throws Exception {
        assertMemoryLeak(() -> {
            // A deduplicating base under a filtering view: a replacement that fails the filter
            // takes the replaced row out of the view, and incremental refresh would propagate
            // exactly that. So a rebuild whose snapshot holds such a replacement the view has not
            // consumed yet reproduces fewer rows, and is right to.
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG) "
                    + "TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                    + "SELECT ts, sym, i, sum(i) OVER (PARTITION BY sym ORDER BY ts "
                    + "RANGE BETWEEN '9' MINUTE PRECEDING AND CURRENT ROW) AS v FROM base WHERE i > 0");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                execute("INSERT INTO base (ts, sym, i) VALUES "
                        + "('2026-01-01T00:01:00.000000Z', 'a', 297), "
                        + "('2026-01-01T00:05:00.000000Z', 'a', 500), "
                        + "('2026-01-01T00:09:00.000000Z', 'a', 900)");
                drainWalQueue();
                driveRefreshToQuiescence(job);
            }
            // The replacement: applied to the base, never refreshed into the view.
            execute("INSERT INTO base (ts, sym, i) VALUES ('2026-01-01T00:01:00.000000Z', 'a', -108)");
            drainWalQueue();
            shutdown();
            removeTimeline();

            final LiveViewRebuildRestatementGuard guard = restart();

            assertRebuiltFromAppliedBase("lv");
            Assert.assertFalse(instance("lv").isCheckpointRecoveryBlocked());
            Assert.assertEquals(
                    "a backlog commit that can legitimately remove an output row must stand the row shortfall down",
                    LiveViewRebuildRestatementGuard.ABSTAIN_BACKLOG_MAY_REMOVE,
                    guard.getAbstention()
            );
            // A rebuild that stands the row shortfall down over rows the view could lose says so,
            // so a restatement found later has a line explaining why the row shortfall did not
            // stop it. The history floor and the lost partition check held: the replacement left
            // a base row at the view's first timestamp, in the partition it replaced a row of.
            capture.drain();
            capture.assertLogged(ROW_SHORTFALL_STAND_DOWN_LINE + " [view=lv, reason=backlog may remove rows]");
            capture.assertNotLogged("live view rebuild from the applied base refused");
            assertQuery("SELECT ts, sym, i FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tsym\ti
                            2026-01-01T00:05:00.000000Z\ta\t500
                            2026-01-01T00:09:00.000000Z\ta\t900
                            """);
        });
    }

    @Test
    public void testABacklogThatAddsAColumnAfterAFilteredCommitStillRefusesADayTtlEvicted() throws Exception {
        // The wall clock moves past the base's rows, so TTL measures against the newest one. The
        // backlog's commit reaches the frontier and pushes that newest row to day three, which
        // evicts day one: the loss rides on the commit itself, not on a partition operation.
        setCurrentMicros(ts("2026-01-05T00:00:00.000000Z"));
        assertLostDayRebuildRefused(
                "TTL 1 DAY WAL",
                true,
                false,
                "INSERT INTO base (ts, sym, i) VALUES ('2026-01-02T00:05:00.000000Z', 'a', -1), ('2026-01-03T12:00:00.000000Z', 'a', -2)",
                "ALTER TABLE base ADD COLUMN note INT"
        );
    }

    @Test
    public void testABacklogThatAddsAColumnAfterAFilteredCommitStillRefusesALostDay() throws Exception {
        // The commit ran without dedup, so it only added a row the filter rejects. A schema change
        // behind it that leaves dedup alone changes nothing about that, and the day the base lost
        // is the restatement the guard refuses.
        assertLostDayRebuildRefused(
                "WAL",
                true,
                false,
                "ALTER TABLE base DROP PARTITION LIST '2026-01-01'",
                FILTERED_FRONTIER_COMMIT,
                "ALTER TABLE base ADD COLUMN note INT"
        );
    }

    @Test
    public void testABacklogThatAddsAColumnAndDisablesDedupAfterARemovingReplacementIsNotRefused() throws Exception {
        // A schema change that leaves dedup alone sits between the replacement and the DEDUP
        // DISABLE. The walk looks past it to the change that did touch dedup.
        assertDedupBacklogRebuildFollowsTheBase(
                false,
                "DEDUP UPSERT KEYS(ts, sym)",
                FILTERED_REPLACEMENT,
                "ALTER TABLE base ADD COLUMN note INT",
                "ALTER TABLE base DEDUP DISABLE"
        );
    }

    @Test
    public void testABacklogThatChangesTheSchemaButNotDedupAfterAFilteredCommitStillRefusesALostDay() throws Exception {
        // Every structural change a WAL table takes that is not a DEDUP change, in one backlog.
        // None of them can switch dedup on or off: the designated timestamp, which every dedup
        // key set holds, can be neither renamed, retyped nor dropped. RENAME TABLE is the one
        // left out: renaming a view's base invalidates the view, so no rebuild follows it.
        assertLostDayRebuildRefused(
                "WAL",
                true,
                false,
                "ALTER TABLE base DROP PARTITION LIST '2026-01-01'",
                FILTERED_FRONTIER_COMMIT,
                "ALTER TABLE base ADD COLUMN note INT",
                "ALTER TABLE base RENAME COLUMN note TO memo",
                "ALTER TABLE base ALTER COLUMN memo TYPE LONG",
                "ALTER TABLE base DROP COLUMN memo"
        );
    }

    @Test
    public void testABacklogThatDisablesDedupAfterARemovingReplacementIsNotRefused() throws Exception {
        // The replacement ran under dedup, and the DEDUP DISABLE behind it leaves the snapshot
        // reporting a base without dedup keys. The snapshot's flag does not speak for a commit
        // an earlier schema governed, so the rebuild still follows the base.
        assertDedupBacklogRebuildFollowsTheBase(
                false,
                "DEDUP UPSERT KEYS(ts, sym)",
                FILTERED_REPLACEMENT,
                "ALTER TABLE base DEDUP DISABLE"
        );
    }

    @Test
    public void testABacklogThatDisablesDedupAheadOfAFilteredCommitAndAddsAColumnStillRefusesALostDay() throws Exception {
        // The base deduplicated until the backlog's DEDUP DISABLE, and the commit came after it,
        // so it ran without dedup as the snapshot says. The schema change behind the commit
        // leaves that alone.
        assertLostDayRebuildRefused(
                "WAL DEDUP UPSERT KEYS(ts, sym)",
                true,
                false,
                "ALTER TABLE base DROP PARTITION LIST '2026-01-01'",
                "ALTER TABLE base DEDUP DISABLE",
                FILTERED_FRONTIER_COMMIT,
                "ALTER TABLE base ADD COLUMN note INT"
        );
    }

    @Test
    public void testABacklogThatDisablesDedupTheBaseNeverHadAfterAFilteredCommitStillRefusesALostDay() throws Exception {
        // The base never deduplicated, so the DEDUP DISABLE changes nothing, but the sequencer
        // cannot say so: its record reads the same as one that ended dedup under a replacement.
        // The row shortfall stands down for that, and the history floor still sees the lost day.
        // A replacement keeps a base row at the timestamp it replaced, so no commit the backlog
        // may hold can move the base's earliest row past the view's.
        assertLostDayRebuildRefused(
                "WAL",
                true,
                false,
                LiveViewRebuildRestatementGuard.ABSTAIN_BACKLOG_MAY_REMOVE,
                "ALTER TABLE base DROP PARTITION LIST '2026-01-01'",
                FILTERED_FRONTIER_COMMIT,
                "ALTER TABLE base DEDUP DISABLE"
        );
    }

    @Test
    public void testABacklogThatEnablesDedupAheadOfARemovingReplacementIsNotRefused() throws Exception {
        // The mirror: the view consumed commits over a base without dedup keys, and the backlog
        // turns dedup on before the replacement. The snapshot deduplicates, as the replacement did.
        assertDedupBacklogRebuildFollowsTheBase(
                false,
                "",
                "ALTER TABLE base DEDUP ENABLE UPSERT KEYS(ts, sym)",
                FILTERED_REPLACEMENT
        );
    }

    @Test
    public void testABacklogThatReplacesARangeAfterADedupReplacementIsNotRefused() throws Exception {
        final TimelineOpenFault fault = new TimelineOpenFault();
        assertMemoryLeak(fault, () -> {
            // A dedup replacement the filter rejects stands only the row shortfall down, and the
            // walk goes on past it. The REPLACE_RANGE commit behind it deletes the base's first
            // day, which moves the base's earliest row past the view's, and incremental refresh
            // would propagate that deletion. So it stands the whole guard down, and the rebuild
            // follows the base rather than meeting the history floor. The commit goes through
            // WalWriter directly, as LiveViewFuzzTest's REPLACE_RANGE operation does.
            seedTwoDayView("WAL DEDUP UPSERT KEYS(ts, sym)", true, fault);
            final long processedBefore = instance("lv").getLastProcessedSeqTxn();
            execute(FILTERED_FRONTIER_COMMIT);
            drainWalQueue();
            try (WalWriter walWriter = engine.getWalWriter(engine.verifyTableName("base"))) {
                walWriter.commitWithParams(
                        ts("2026-01-01T00:00:00.000000Z"),
                        ts("2026-01-02T00:00:00.000000Z"),
                        WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE
                );
            }
            drainWalQueue();
            Assert.assertEquals("the view must not consume the backlog", processedBefore, instance("lv").getLastProcessedSeqTxn());
            fault.disarm();
            shutdown();

            final LiveViewRebuildRestatementGuard guard = restart();

            final LiveViewInstance instance = instance("lv");
            Assert.assertFalse(
                    "the rebuild must follow the base, not stop the view: " + instance.getCheckpointRecoveryReason(),
                    instance.isCheckpointRecoveryBlocked()
            );
            assertRebuiltFromAppliedBase("lv");
            Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_BACKLOG_MAY_REMOVE, guard.getAbstention());
            capture.drain();
            capture.assertLogged("live view rebuild from the applied base runs without the restatement guard [view=lv, reason=backlog may remove rows]");
            capture.assertNotLogged(ROW_SHORTFALL_STAND_DOWN_LINE);
            final String rows = """
                    ts\tsym\ti\tv
                    2026-01-02T00:09:00.000000Z\ta\t900\t900.0
                    """;
            assertFilteredViewRows(rows);
            assertLiveViewsReportsNoRecovery(instance);

            shutdown();
            restart();
            Assert.assertFalse(instance("lv").isCheckpointRecoveryBlocked());
            assertFilteredViewRows(rows);
        });
    }

    @Test
    public void testABacklogThatReplacesARowAfterTheBaseLostADayOfRowsTheFilterRejectedIsNotRefused() throws Exception {
        // The base loses a day the view holds no row of, then replaces a row the view holds with
        // one the filter rejects. Neither is a loss of the view's rows: the day took only rows the
        // filter rejected, and the replacement is what incremental refresh would propagate.
        assertRejectedDayLossRebuildFollowsTheBase("");
    }

    @Test
    public void testABacklogThatReplacesARowWhileTheViewsFrontierSitsOnAPartitionBoundaryIsNotRefused() throws Exception {
        final TimelineOpenFault fault = new TimelineOpenFault();
        assertMemoryLeak(fault, () -> {
            // Every row the view holds sits at midnight, so its newest is exactly where the base's
            // last partition begins. A replacement leaves a base row at the timestamp it replaced,
            // so the base still holds a partition over every row the view holds, and the walk over
            // the base's partitions has to cover the view's frontier rather than stop below it:
            // the rebuild follows the base.
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG) "
                    + "TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, sym)");
            createFilteredView();
            refreshIntoFilteredView("""
                    INSERT INTO base (ts, sym, i) VALUES
                        ('2026-01-01T00:00:00.000000Z', 'a', 297),
                        ('2026-01-02T00:00:00.000000Z', 'a', 500),
                        ('2026-01-03T00:00:00.000000Z', 'a', 900)""");
            Assert.assertTrue("the view's timeline open must have failed", fault.getFailures() > 0);
            // A day apart, every row leads its own window, so the sum is the row's own value.
            assertFilteredViewRows("""
                    ts\tsym\ti\tv
                    2026-01-01T00:00:00.000000Z\ta\t297\t297.0
                    2026-01-02T00:00:00.000000Z\ta\t500\t500.0
                    2026-01-03T00:00:00.000000Z\ta\t900\t900.0
                    """);
            applyUnconsumedBacklog(
                    "INSERT INTO base (ts, sym, i) VALUES ('2026-01-02T00:00:00.000000Z', 'a', -108)",
                    "ALTER TABLE base DEDUP DISABLE"
            );
            // The base holds a partition over each of the view's three rows: the replacement kept
            // the middle day, with the row that took the place of the one the view holds.
            assertQuery("SELECT name, numRows FROM table_partitions('base')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            name\tnumRows
                            2026-01-01\t1
                            2026-01-02\t1
                            2026-01-03\t1
                            """);
            fault.disarm();
            shutdown();

            final LiveViewRebuildRestatementGuard guard = restart();

            final LiveViewInstance instance = instance("lv");
            Assert.assertFalse(
                    "the rebuild must follow the base, not stop the view: " + instance.getCheckpointRecoveryReason(),
                    instance.isCheckpointRecoveryBlocked()
            );
            assertRebuiltFromAppliedBase("lv");
            Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_BACKLOG_MAY_REMOVE, guard.getAbstention());
            Assert.assertEquals(LiveViewRebuildRestatementGuard.VERDICT_NONE, guard.getVerdict());
            capture.drain();
            // The stand-down the lost partition check runs behind: the walk this case pins over the
            // base's partitions happens only under it.
            capture.assertLogged(ROW_SHORTFALL_STAND_DOWN_LINE + " [view=lv, reason="
                    + LiveViewRebuildRestatementGuard.abstentionName(LiveViewRebuildRestatementGuard.ABSTAIN_BACKLOG_MAY_REMOVE)
                    + "]");
            final String replacedRows = """
                    ts\tsym\ti\tv
                    2026-01-01T00:00:00.000000Z\ta\t297\t297.0
                    2026-01-03T00:00:00.000000Z\ta\t900\t900.0
                    """;
            assertFilteredViewRows(replacedRows);
            assertLiveViewsReportsNoRecovery(instance);

            shutdown();
            restart();
            Assert.assertFalse(instance("lv").isCheckpointRecoveryBlocked());
            assertFilteredViewRows(replacedRows);

            execute("INSERT INTO base (ts, sym, i) VALUES ('2026-01-03T00:05:00.000000Z', 'a', 1000)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            assertFilteredViewRows(replacedRows + "2026-01-03T00:05:00.000000Z\ta\t1000\t1900.0\n");
            assertLiveViewsReportsNoRecovery(instance("lv"));
        });
    }

    @Test
    public void testABacklogThatReplacesTheBasesEarliestRowWithOneTheFilterRejectsIsNotRefused() throws Exception {
        // The replacement takes the view's first row, the only row of the base's first day, out
        // of the view, and the DEDUP DISABLE behind it leaves the guard unable to tell it from a
        // plain commit. The history floor stays armed over it and must not mistake it for the
        // day the base lost: the base still holds a row at that timestamp, the one that replaced
        // it, so its earliest row does not move.
        assertDedupBacklogRebuildOverRowsFollowsTheBase(
                false,
                "DEDUP UPSERT KEYS(ts, sym)",
                FILTERED_TWO_DAY_ROWS_INSERT,
                FILTERED_TWO_DAY_ROWS,
                """
                        ts\tsym\ti\tv
                        2026-01-02T00:05:00.000000Z\ta\t500\t500.0
                        2026-01-02T00:09:00.000000Z\ta\t900\t1400.0
                        """,
                "INSERT INTO base (ts, sym, i) VALUES ('2026-01-02T00:10:00.000000Z', 'a', 1000)",
                "2026-01-02T00:10:00.000000Z\ta\t1000\t2400.0\n",
                FILTERED_REPLACEMENT,
                "ALTER TABLE base DEDUP DISABLE"
        );
    }

    @Test
    public void testABacklogThatTogglesDedupAheadOfAFilteredCommitStillRefusesALostDay() throws Exception {
        // The DEDUP changes precede the backlog's only commit, which ran without dedup like the
        // snapshot says. So the commit adds a row the filter rejects and removes nothing, and the
        // day the base lost is still the restatement the guard refuses.
        assertLostDayRebuildRefused(
                "WAL",
                true,
                false,
                "ALTER TABLE base DROP PARTITION LIST '2026-01-01'",
                "ALTER TABLE base DEDUP ENABLE UPSERT KEYS(ts, sym)",
                "ALTER TABLE base DEDUP DISABLE",
                FILTERED_FRONTIER_COMMIT
        );
    }

    @Test
    public void testABacklogThatTogglesDedupAfterAFilteredCommitStillRefusesALostDay() throws Exception {
        // The commit ran without dedup, and the DEDUP ENABLE and DISABLE behind it leave the
        // sequencer's record unable to say so. The history floor still sees the lost day.
        assertLostDayRebuildRefused(
                "WAL",
                true,
                false,
                LiveViewRebuildRestatementGuard.ABSTAIN_BACKLOG_MAY_REMOVE,
                "ALTER TABLE base DROP PARTITION LIST '2026-01-01'",
                FILTERED_FRONTIER_COMMIT,
                "ALTER TABLE base DEDUP ENABLE UPSERT KEYS(ts, sym)",
                "ALTER TABLE base DEDUP DISABLE"
        );
    }

    @Test
    public void testABacklogThatTogglesDedupAroundARemovingReplacementIsNotRefused() throws Exception {
        // Neither end of the backlog deduplicates, only the replacement between them did.
        assertDedupBacklogRebuildFollowsTheBase(
                false,
                "",
                "ALTER TABLE base DEDUP ENABLE UPSERT KEYS(ts, sym)",
                FILTERED_REPLACEMENT,
                "ALTER TABLE base DEDUP DISABLE"
        );
    }

    @Test
    public void testADedupDisableAfterAFilteredCommitStillRefusesALostNewestDay() throws Exception {
        final TimelineOpenFault fault = new TimelineOpenFault();
        assertMemoryLeak(fault, () -> {
            // The base loses the day that holds the view's frontier, which leaves its earliest row
            // where it was, and the backlog's DEDUP change stands the row shortfall down. The
            // view's last row now sits above every partition the base holds.
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            createFilteredView();
            refreshIntoFilteredView(FILTERED_THREE_DAY_ROWS_INSERT);
            Assert.assertTrue("the view's timeline open must have failed", fault.getFailures() > 0);
            assertFilteredViewRows(FILTERED_THREE_DAY_ROWS);
            applyUnconsumedBacklog(
                    "ALTER TABLE base DROP PARTITION LIST '2026-01-03'",
                    "INSERT INTO base (ts, sym, i) VALUES ('2026-01-02T00:07:00.000000Z', 'a', -1)",
                    "ALTER TABLE base DEDUP DISABLE"
            );
            fault.disarm();
            shutdown();

            final LiveViewRebuildRestatementGuard guard = restart();

            assertLostPartitionRefused(
                    guard,
                    "timeline is absent",
                    LiveViewRebuildRestatementGuard.ABSTAIN_BACKLOG_MAY_REMOVE,
                    "the view holds a row at 2026-01-03T00:09:00.000000Z but the base table holds no partition "
                            + "between 2026-01-03T00:00:00.000000Z and 2026-01-04T00:00:00.000000Z",
                    FILTERED_THREE_DAY_ROWS
            );

            shutdown();
            restart();
            assertRebuildBlocked(instance("lv"), "timeline is absent");
            assertFilteredViewRows(FILTERED_THREE_DAY_ROWS);
        });
    }

    @Test
    public void testADedupDisableAfterAFilteredCommitStillRefusesALostNewestDayWhoseRowSitsOnItsBoundary() throws Exception {
        final TimelineOpenFault fault = new TimelineOpenFault();
        assertMemoryLeak(fault, () -> {
            // The same loss, with the view's frontier exactly at the midnight the day the base lost
            // begins. The range above the base's last partition starts where that row sits, so a
            // refusal that stopped at the frontier rather than reaching it would let the row go.
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            createFilteredView();
            refreshIntoFilteredView("""
                    INSERT INTO base (ts, sym, i) VALUES
                        ('2026-01-01T00:01:00.000000Z', 'a', 297),
                        ('2026-01-02T00:05:00.000000Z', 'a', 500),
                        ('2026-01-03T00:00:00.000000Z', 'a', 900)""");
            Assert.assertTrue("the view's timeline open must have failed", fault.getFailures() > 0);
            final String rows = """
                    ts\tsym\ti\tv
                    2026-01-01T00:01:00.000000Z\ta\t297\t297.0
                    2026-01-02T00:05:00.000000Z\ta\t500\t500.0
                    2026-01-03T00:00:00.000000Z\ta\t900\t900.0
                    """;
            assertFilteredViewRows(rows);
            applyUnconsumedBacklog(
                    "ALTER TABLE base DROP PARTITION LIST '2026-01-03'",
                    "INSERT INTO base (ts, sym, i) VALUES ('2026-01-02T00:07:00.000000Z', 'a', -1)",
                    "ALTER TABLE base DEDUP DISABLE"
            );
            fault.disarm();
            shutdown();

            final LiveViewRebuildRestatementGuard guard = restart();

            assertLostPartitionRefused(
                    guard,
                    "timeline is absent",
                    LiveViewRebuildRestatementGuard.ABSTAIN_BACKLOG_MAY_REMOVE,
                    "the view holds a row at 2026-01-03T00:00:00.000000Z but the base table holds no partition "
                            + "between 2026-01-03T00:00:00.000000Z and 2026-01-04T00:00:00.000000Z",
                    rows
            );

            shutdown();
            restart();
            assertRebuildBlocked(instance("lv"), "timeline is absent");
            assertFilteredViewRows(rows);
        });
    }

    @Test
    public void testADedupDisableAfterAFilteredCommitStillRefusesAnOldestDayLostAboveARowTheFilterRejects() throws Exception {
        // The base loses the view's oldest day but keeps an older row the filter rejected, so its
        // earliest row stays below the view's and the history floor cannot see the loss. The
        // backlog's DEDUP change stands the row shortfall down.
        assertLostOldestDayRebuildRefused(
                "",
                false,
                "ALTER TABLE base DROP PARTITION LIST '2026-01-02'",
                FILTERED_LATER_DAYS_FRONTIER_COMMIT,
                "ALTER TABLE base DEDUP DISABLE"
        );
    }

    @Test
    public void testADedupDisableAfterAFilteredCommitStillRefusesAnOldestDayLostAboveTheStartFrom() throws Exception {
        // The same, with the older base row one the view's START FROM leaves out.
        assertLostOldestDayRebuildRefused(
                "",
                true,
                "ALTER TABLE base DROP PARTITION LIST '2026-01-02'",
                FILTERED_LATER_DAYS_FRONTIER_COMMIT,
                "ALTER TABLE base DEDUP DISABLE"
        );
    }

    @Test
    public void testADedupToggleAfterAFilteredCommitStillRefusesADetachedOldestDay() throws Exception {
        // A detached partition is as gone from the base as a dropped one.
        assertLostOldestDayRebuildRefused(
                "",
                false,
                "ALTER TABLE base DETACH PARTITION LIST '2026-01-02'",
                FILTERED_LATER_DAYS_FRONTIER_COMMIT,
                "ALTER TABLE base DEDUP ENABLE UPSERT KEYS(ts, sym)",
                "ALTER TABLE base DEDUP DISABLE"
        );
    }

    @Test
    public void testADeduplicatingBacklogThatReachesTheFrontierStillRefusesALostDay() throws Exception {
        // The commit replaces a row the view holds with one the filter rejects, and the base lost
        // its first day. The row shortfall cannot tell the two apart, the history floor can: only
        // the lost day moves the base's earliest row.
        assertLostDayRebuildRefused(
                "WAL DEDUP UPSERT KEYS(ts, sym)",
                true,
                false,
                LiveViewRebuildRestatementGuard.ABSTAIN_BACKLOG_MAY_REMOVE,
                "ALTER TABLE base DROP PARTITION LIST '2026-01-01'",
                FILTERED_FRONTIER_COMMIT
        );
    }

    @Test
    public void testADeduplicatingBacklogThatReachesTheFrontierStillRefusesALostMiddleDay() throws Exception {
        final TimelineOpenFault fault = new TimelineOpenFault();
        assertMemoryLeak(fault, () -> {
            // The commit may replace a row the view holds with one the filter rejects, so the row
            // shortfall stands down, and the middle day the base lost leaves its earliest row
            // where it was. The view's row from that day sits in no partition the base holds.
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG) "
                    + "TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, sym)");
            createFilteredView();
            refreshIntoFilteredView(FILTERED_THREE_DAY_ROWS_INSERT);
            Assert.assertTrue("the view's timeline open must have failed", fault.getFailures() > 0);
            assertFilteredViewRows(FILTERED_THREE_DAY_ROWS);
            applyUnconsumedBacklog(
                    "ALTER TABLE base DROP PARTITION LIST '2026-01-02'",
                    "INSERT INTO base (ts, sym, i) VALUES ('2026-01-03T00:05:00.000000Z', 'a', -1)"
            );
            fault.disarm();
            shutdown();

            final LiveViewRebuildRestatementGuard guard = restart();

            assertLostPartitionRefused(
                    guard,
                    "timeline is absent",
                    LiveViewRebuildRestatementGuard.ABSTAIN_BACKLOG_MAY_REMOVE,
                    FILTERED_THREE_DAY_LOST_DAY_EVIDENCE,
                    FILTERED_THREE_DAY_ROWS
            );

            shutdown();
            restart();
            assertRebuildBlocked(instance("lv"), "timeline is absent");
            assertFilteredViewRows(FILTERED_THREE_DAY_ROWS);
        });
    }

    @Test
    public void testAFilteredCommitAboveTheFrontierKeepsTheRowShortfallBehindADedupChange() throws Exception {
        final TimelineOpenFault fault = new TimelineOpenFault();
        assertMemoryLeak(fault, () -> {
            // The base loses the middle of the view's three days, which leaves its earliest row
            // where it was: only the row shortfall sees that loss. The backlog's commit lies above
            // the view's frontier, so a DEDUP change behind it cannot have taken a row out of the
            // view, and the shortfall must still compare.
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            createFilteredView();
            refreshIntoFilteredView(FILTERED_THREE_DAY_ROWS_INSERT);
            Assert.assertTrue("the view's timeline open must have failed", fault.getFailures() > 0);
            assertFilteredViewRows(FILTERED_THREE_DAY_ROWS);
            applyUnconsumedBacklog(
                    "ALTER TABLE base DROP PARTITION LIST '2026-01-02'",
                    "INSERT INTO base (ts, sym, i) VALUES ('2026-01-03T00:10:00.000000Z', 'a', -1)",
                    "ALTER TABLE base DEDUP DISABLE"
            );
            fault.disarm();
            shutdown();

            final LiveViewRebuildRestatementGuard guard = restart();

            final LiveViewInstance instance = instance("lv");
            assertRebuildBlocked(instance, "timeline is absent");
            TestUtils.assertContains(
                    instance.getCheckpointRecoveryReason(),
                    "the rebuild reproduces 2 of the 3 rows the view holds up to 2026-01-03T00:09:00.000000Z"
            );
            Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_NONE, guard.getAbstention());
            Assert.assertEquals(LiveViewRebuildRestatementGuard.VERDICT_ROW_SHORTFALL, guard.getVerdict());
            capture.drain();
            capture.assertNotLogged("live view rebuild from the applied base runs without the restatement guard");
            capture.assertNotLogged(ROW_SHORTFALL_STAND_DOWN_LINE);
            assertFilteredViewRows(FILTERED_THREE_DAY_ROWS);

            shutdown();
            restart();
            assertRebuildBlocked(instance("lv"), "timeline is absent");
            assertFilteredViewRows(FILTERED_THREE_DAY_ROWS);
        });
    }

    @Test
    public void testABaseDedupDisableAfterAFilteredCommitStillRefusesAnOldestDayLostAboveTheStartFromWithNoTimeline() throws Exception {
        final TimelineOpenFault fault = new TimelineOpenFault();
        assertMemoryLeak(fault, () -> {
            // The running route: a view with no timeline walks past the loss of its oldest day, and
            // the base keeps an older row the view's START FROM leaves out.
            seedLaterDaysView("", true);
            Assert.assertTrue("the view's timeline open must have failed", fault.getFailures() > 0);
            execute("ALTER TABLE base DROP PARTITION LIST '2026-01-02'");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            assertFilteredViewRows(FILTERED_LATER_DAYS_ROWS);
            fault.disarm();

            // A commit that reaches the frontier, then the DEDUP DISABLE, which is itself the schema
            // change the drain meets. With no timeline to restore from, it asks for the rebuild.
            execute(FILTERED_LATER_DAYS_FRONTIER_COMMIT);
            execute("ALTER TABLE base DEDUP DISABLE");
            drainWalQueue();
            final LiveViewRebuildRestatementGuard guard;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                guard = job.rebuildRestatementGuardForTest();
            }

            assertLostPartitionRefused(
                    guard,
                    DRIFT_CAUSE,
                    LiveViewRebuildRestatementGuard.ABSTAIN_BACKLOG_MAY_REMOVE,
                    FILTERED_LATER_DAYS_LOST_DAY_EVIDENCE,
                    FILTERED_LATER_DAYS_ROWS
            );
            assertDriftRestoreFoundNoGeneration();
        });
    }

    @Test
    public void testABaseSchemaChangeAfterAFilteredCommitStillRefusesALostDayWithNoTimeline() throws Exception {
        final TimelineOpenFault fault = new TimelineOpenFault();
        assertMemoryLeak(fault, () -> {
            // The running route: a view with no timeline walks past the loss of its base's first
            // day and keeps that day's row.
            seedTwoDayView("WAL", true, fault);
            execute("ALTER TABLE base DROP PARTITION LIST '2026-01-01'");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            assertFilteredViewRows(FILTERED_TWO_DAY_ROWS);
            fault.disarm();

            // A commit that reaches the frontier, then a schema change the view survives. The
            // drain meets the drift, and with no timeline to restore from, asks for the rebuild.
            execute(FILTERED_FRONTIER_COMMIT);
            execute("ALTER TABLE base ADD COLUMN note INT");
            drainWalQueue();
            final LiveViewRebuildRestatementGuard guard;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                guard = job.rebuildRestatementGuardForTest();
            }

            final LiveViewInstance instance = instance("lv");
            assertRebuildBlocked(instance, DRIFT_CAUSE);
            TestUtils.assertContains(instance.getCheckpointRecoveryReason(), FILTERED_LOST_DAY_EVIDENCE);
            Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_NONE, guard.getAbstention());
            Assert.assertEquals(LiveViewRebuildRestatementGuard.VERDICT_HISTORY_FLOOR, guard.getVerdict());
            assertFilteredViewRows(FILTERED_TWO_DAY_ROWS);
            capture.drain();
            assertDriftRestoreFoundNoGeneration();
            capture.assertNotLogged("live view rebuild from the applied base runs without the restatement guard");
        });
    }

    @Test
    public void testALostCommitBehindADedupDisableAfterARemovingReplacementIsNotRefused() throws Exception {
        final TimelineOpenFault fault = new TimelineOpenFault();
        assertMemoryLeak(fault, () -> {
            // The walk reads the replacement and the DEDUP DISABLE behind it, and goes on past
            // them to a commit whose WAL is gone. That commit ran without dedup and adds a row the
            // filter rejects, but the replacement the walk already read still stands the row
            // shortfall down: a comparison would refuse the rebuild, on every restart.
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG) "
                    + "TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, sym)");
            createFilteredView();
            refreshIntoFilteredView(FILTERED_ROWS_INSERT);
            Assert.assertTrue("the view's timeline open must have failed", fault.getFailures() > 0);
            applyUnconsumedBacklog(FILTERED_REPLACEMENT, "ALTER TABLE base DEDUP DISABLE");
            final TableToken baseToken = engine.verifyTableName("base");
            final long processedBefore = instance("lv").getLastProcessedSeqTxn();
            try (WalWriter held = engine.getWalWriter(baseToken)) {
                Assert.assertEquals("every earlier commit must sit in the first WAL", 1, held.getWalId());
                execute("INSERT INTO base (ts, sym, i) VALUES ('2026-01-01T00:07:00.000000Z', 'a', -3)");
            }
            drainWalQueue();
            Assert.assertEquals("the view must not consume the backlog", processedBefore, instance("lv").getLastProcessedSeqTxn());
            fault.disarm();
            shutdown();
            final File secondWal = new File(
                    new File(engine.getConfiguration().getDbRoot(), baseToken.getDirName()),
                    WalUtils.WAL_NAME_BASE + 2
            );
            Assert.assertTrue("the insert must have taken a second WAL", secondWal.isDirectory());
            try (Path p = new Path()) {
                p.of(secondWal.getAbsolutePath());
                Assert.assertTrue("could not remove " + secondWal, engine.getConfiguration().getFilesFacade().rmdir(p));
            }

            final LiveViewRebuildRestatementGuard guard = restart();

            final LiveViewInstance instance = instance("lv");
            Assert.assertFalse(
                    "the rebuild must follow the base, not stop the view: " + instance.getCheckpointRecoveryReason(),
                    instance.isCheckpointRecoveryBlocked()
            );
            assertRebuiltFromAppliedBase("lv");
            Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_BACKLOG_UNREADABLE, guard.getAbstention());
            capture.drain();
            capture.assertLogged("guardChecks=history floor and base partitions,");
            capture.assertLogged(ROW_SHORTFALL_STAND_DOWN_LINE + " [view=lv, reason=backlog unreadable]");
            assertFilteredViewRows(FILTERED_REPLACED_ROWS);
            assertLiveViewsReportsNoRecovery(instance);

            shutdown();
            restart();
            Assert.assertFalse(instance("lv").isCheckpointRecoveryBlocked());
            assertFilteredViewRows(FILTERED_REPLACED_ROWS);
        });
    }

    @Test
    public void testALostBaseWalRederiveOverABacklogThatDisablesDedupAfterARemovingReplacementIsNotRefused() throws Exception {
        assertMemoryLeak(() -> {
            // The view restores from its timeline, and its drain meets the lost segment. The
            // replacement's commit is exactly what the re-derive cannot read, and the DEDUP
            // DISABLE after it is on the sequencer, which the loss left readable. That commit may
            // have run under dedup, so the guard abstains and the re-derive follows the base.
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG) "
                    + "TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, sym)");
            createFilteredView();
            refreshIntoFilteredView(FILTERED_ROWS_INSERT);
            assertFilteredViewRows(FILTERED_ROWS);
            execute(FILTERED_REPLACEMENT);
            execute("ALTER TABLE base DEDUP DISABLE");
            drainWalQueue();
            shutdown();
            removeBaseWal("base");

            final LiveViewRebuildRestatementGuard guard = restart();

            final LiveViewInstance instance = instance("lv");
            Assert.assertFalse(
                    "the re-derive must follow the base, not stop the view: " + instance.getCheckpointRecoveryReason(),
                    instance.isCheckpointRecoveryBlocked()
            );
            Assert.assertFalse(instance.isInvalid());
            capture.drain();
            capture.assertLogged("live view re-derived from the applied base after base WAL loss");
            capture.assertNotLogged("live view rebuild from the applied base refused");
            Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_BACKLOG_UNREADABLE, guard.getAbstention());
            assertFilteredViewRows(FILTERED_REPLACED_ROWS);
            assertLiveViewsReportsNoRecovery(instance);

            execute(FILTERED_APPEND);
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            assertFilteredViewRows(FILTERED_REPLACED_ROWS + FILTERED_APPEND_OUTPUT);
        });
    }

    @Test
    public void testALostBaseWalBacklogOverADeduplicatingBaseStillRefusesALostDay() throws Exception {
        // The same, with the backlog's commits unreadable: over a base that deduplicates, any of
        // them may be a replacement, and none of them can move the base's earliest row.
        assertLostDayRebuildRefused(
                "WAL DEDUP UPSERT KEYS(ts, sym)",
                true,
                true,
                LiveViewRebuildRestatementGuard.ABSTAIN_BACKLOG_UNREADABLE,
                "ALTER TABLE base DROP PARTITION LIST '2026-01-01'",
                FILTERED_FRONTIER_COMMIT
        );
    }

    @Test
    public void testALostBaseWalBacklogThatAddsAColumnAndDisablesDedupAfterARemovingReplacementIsNotRefused() throws Exception {
        // The replacement's commit is what the rebuild cannot read. The sequencer still records
        // what followed it, and the check looks past the schema change that leaves dedup alone to
        // the DEDUP DISABLE that leaves the replacement's dedup unknown.
        assertDedupBacklogRebuildFollowsTheBase(
                true,
                "DEDUP UPSERT KEYS(ts, sym)",
                FILTERED_REPLACEMENT,
                "ALTER TABLE base ADD COLUMN note INT",
                "ALTER TABLE base DEDUP DISABLE"
        );
    }

    @Test
    public void testALostBaseWalBacklogThatDisablesDedupAfterAFilteredCommitStillRefusesALostDay() throws Exception {
        // The base never deduplicated, and the rebuild cannot read the backlog's commits. The
        // DEDUP DISABLE the sequencer records after them stands the row shortfall down, and the
        // history floor still sees the lost day.
        assertLostDayRebuildRefused(
                "WAL",
                true,
                true,
                LiveViewRebuildRestatementGuard.ABSTAIN_BACKLOG_UNREADABLE,
                "ALTER TABLE base DROP PARTITION LIST '2026-01-01'",
                FILTERED_FRONTIER_COMMIT,
                "ALTER TABLE base DEDUP DISABLE"
        );
    }

    @Test
    public void testAPurgedBacklogBehindARefusalStaysRefusedAcrossASchemaChange() throws Exception {
        // WalPurgeJob.runSerially is interval-gated off the millisecond clock, which this class
        // freezes. Without this the sweep below silently does nothing.
        setProperty(PropertyKey.CAIRO_WAL_PURGE_INTERVAL, 0);
        final TimelineOpenFault fault = new TimelineOpenFault();
        assertMemoryLeak(fault, () -> {
            // A filtering view over a base without dedup walks past the loss of the base's first
            // day, and the restart that finds no timeline refuses the rebuild.
            seedTwoDayView("WAL", true, fault);
            execute("ALTER TABLE base DROP PARTITION LIST '2026-01-01'");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            assertFilteredViewRows(FILTERED_TWO_DAY_ROWS);
            fault.disarm();
            shutdown();
            restart();
            assertRebuildBlocked(instance("lv"), "timeline is absent");

            // Ingestion goes on while the view is stopped, and a blocked view holds no WAL floor:
            // the routine sweep takes every base WAL the restart closed. The sequencer keeps the
            // record of each commit, not the commit.
            execute("INSERT INTO base (ts, sym, i) VALUES ('2026-01-03T00:05:00.000000Z', 'a', 7)");
            drainWalQueue();
            engine.releaseInactive();
            setCurrentMicros(currentMicros + 60_000_000L);
            try (WalPurgeJob purgeJob = new WalPurgeJob(engine)) {
                purgeJob.drain(0);
            }
            Assert.assertEquals("the sweep must have taken the base's WAL", 0, countBaseWalDirs("base"));
            // A schema change that leaves dedup alone, behind the commit the sweep took.
            execute("ALTER TABLE base ADD COLUMN note INT");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            shutdown();

            final LiveViewRebuildRestatementGuard guard = restart();

            // The commit ran without dedup, as the base always has, so it cannot have taken a row
            // out of the view. The guard compares and refuses again, as it did before the sweep.
            final LiveViewInstance instance = instance("lv");
            assertRebuildBlocked(instance, "timeline is absent");
            TestUtils.assertContains(instance.getCheckpointRecoveryReason(), FILTERED_LOST_DAY_EVIDENCE);
            Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_NONE, guard.getAbstention());
            Assert.assertEquals(LiveViewRebuildRestatementGuard.VERDICT_HISTORY_FLOOR, guard.getVerdict());
            assertFilteredViewRows(FILTERED_TWO_DAY_ROWS);
            capture.drain();
            capture.assertLogged("live view could not read the rebuild's base backlog [view=lv, fromSeqTxn=2, toSeqTxn=4, guardChecks=history floor and row shortfall,");
            capture.assertNotLogged("live view rebuild from the applied base runs without the restatement guard");

            shutdown();
            restart();
            assertRebuildBlocked(instance("lv"), "timeline is absent");
            assertFilteredViewRows(FILTERED_TWO_DAY_ROWS);
        });
    }

    @Test
    public void testAPurgedBacklogBehindARowShortfallRefusalStaysRefusedAcrossADedupDisable() throws Exception {
        assertPurgedMiddleDayRefusalSurvivesADedupDisable("WAL");
    }

    @Test
    public void testAPurgedBacklogOverADeduplicatingBaseStaysRefusedAcrossADedupDisable() throws Exception {
        assertPurgedMiddleDayRefusalSurvivesADedupDisable("WAL DEDUP UPSERT KEYS(ts, sym)");
    }

    @Test
    public void testARestartThatLostTheBaseWalBehindADedupDisableStillRefusesAnOldestDayLostAboveARowTheFilterRejects() throws Exception {
        assertMemoryLeak(() -> {
            // No fault: the view keeps its timeline and walks past the loss of its oldest day, and
            // the base keeps an older row the filter rejected. A restore that captured the applied
            // base and not its WAL leaves the timeline nothing to replay the backlog from, and the
            // rebuild cannot read the backlog either. The sequencer still records its DEDUP DISABLE.
            seedLaterDaysView("", false);
            execute("ALTER TABLE base DROP PARTITION LIST '2026-01-02'");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            assertFilteredViewRows(FILTERED_LATER_DAYS_ROWS);
            applyUnconsumedBacklog(FILTERED_LATER_DAYS_FRONTIER_COMMIT, "ALTER TABLE base DEDUP DISABLE");
            shutdown();
            removeBaseWal("base");

            final LiveViewRebuildRestatementGuard guard = restart();

            assertLostPartitionRefused(
                    guard,
                    "timeline restore failed",
                    LiveViewRebuildRestatementGuard.ABSTAIN_BACKLOG_UNREADABLE,
                    FILTERED_LATER_DAYS_LOST_DAY_EVIDENCE,
                    FILTERED_LATER_DAYS_ROWS
            );

            shutdown();
            restart();
            assertRebuildBlocked(instance("lv"), "timeline restore failed");
            assertFilteredViewRows(FILTERED_LATER_DAYS_ROWS);
        });
    }

    @Test
    public void testAViewPartitionedByMonthFollowsABaseThatLostADayOfRowsTheFilterRejected() throws Exception {
        // The view's one partition spans the day the base lost, and holds no row inside it.
        assertRejectedDayLossRebuildFollowsTheBase("PARTITION BY MONTH");
    }

    @Test
    public void testAViewPartitionedByMonthRefusesADayItsBaseLostBehindADedupDisable() throws Exception {
        // The view's one partition spans the day the base lost and the days around it, so only
        // the rows inside that partition can say whether the view holds any of the lost day.
        assertLostOldestDayRebuildRefused(
                "PARTITION BY MONTH",
                false,
                "ALTER TABLE base DROP PARTITION LIST '2026-01-02'",
                FILTERED_LATER_DAYS_FRONTIER_COMMIT,
                "ALTER TABLE base DEDUP DISABLE"
        );
    }

    @Test
    public void testAnUnfilteredViewRefusesALostDayBehindAFrontierCommitThatDedupDisableFollows() throws Exception {
        // A view without a filter loses no row to a replacement, whatever dedup did to it, so a
        // DEDUP DISABLE behind a commit that reached the frontier is no reason to stand down.
        assertLostDayRebuildRefused(
                "WAL DEDUP UPSERT KEYS(ts, sym)",
                false,
                false,
                "ALTER TABLE base DROP PARTITION LIST '2026-01-01'",
                FILTERED_FRONTIER_COMMIT,
                "ALTER TABLE base DEDUP DISABLE"
        );
    }

    @Test
    public void testAnUnfilteredViewRefusesALostDayBehindAnUnreadableCommitThatDedupDisableFollows() throws Exception {
        // The same, with the backlog's commits unreadable: without a filter, a commit the rebuild
        // cannot read still cannot take a row out of the view.
        assertLostDayRebuildRefused(
                "WAL DEDUP UPSERT KEYS(ts, sym)",
                false,
                true,
                "ALTER TABLE base DROP PARTITION LIST '2026-01-01'",
                FILTERED_FRONTIER_COMMIT,
                "ALTER TABLE base DEDUP DISABLE"
        );
    }

    @Test
    public void testAnUnreadableBacklogBehindADedupDisableStillRefusesALostDay() throws Exception {
        // The DEDUP DISABLE comes before the first commit the rebuild cannot read, and no commit
        // ahead of it reached the frontier. So the unreadable commit ran without dedup, as the
        // snapshot says, and only a DEDUP change after it could leave that in doubt.
        assertLostDayRebuildRefused(
                "WAL DEDUP UPSERT KEYS(ts, sym)",
                true,
                true,
                "ALTER TABLE base DEDUP DISABLE",
                FILTERED_FRONTIER_COMMIT,
                "ALTER TABLE base DROP PARTITION LIST '2026-01-01'"
        );
    }

    @Test
    public void testAnUnreadableBacklogIgnoresDedupChangesTheSnapshotHasNotApplied() throws Exception {
        final TimelineOpenFault fault = new TimelineOpenFault();
        assertMemoryLeak(fault, () -> {
            seedTwoDayView("WAL", true, fault);
            applyUnconsumedBacklog("ALTER TABLE base DROP PARTITION LIST '2026-01-01'", FILTERED_FRONTIER_COMMIT);
            // The base's apply stops, and DEDUP changes land on the sequencer past everything the
            // rebuild's snapshot holds. The base applied the backlog's commits without dedup, and
            // what the sequencer records after the snapshot has no say in that.
            execute("ALTER TABLE base SUSPEND WAL");
            applyUnconsumedBacklog("ALTER TABLE base DEDUP ENABLE UPSERT KEYS(ts, sym)", "ALTER TABLE base DEDUP DISABLE");
            final TableToken baseToken = engine.verifyTableName("base");
            Assert.assertEquals(
                    "the DEDUP changes must stay unapplied",
                    engine.getTableSequencerAPI().getTxnTracker(baseToken).getWriterTxn() + 2,
                    engine.getTableSequencerAPI().lastTxn(baseToken)
            );
            fault.disarm();

            assertLostDayRebuildRefusedOnRestart(true, LiveViewRebuildRestatementGuard.ABSTAIN_NONE);
        });
    }

    @Test
    public void testAnUnreadableDedupHistoryRefusesUntilARestartCanReadIt() throws Exception {
        final DedupHistoryOpenFault fault = new DedupHistoryOpenFault();
        assertMemoryLeak(fault, () -> {
            // The finding's backlog: a replacement under dedup, then DEDUP DISABLE. Whether the
            // replacement ran under dedup is on the sequencer's record of that DEDUP DISABLE, and
            // the restart cannot read it. With the history unknown, the guard compares, as it
            // would over a base that never deduplicated, and a restart retries the question.
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG) "
                    + "TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, sym)");
            createFilteredView();
            refreshIntoFilteredView(FILTERED_ROWS_INSERT);
            Assert.assertTrue("the view's timeline open must have failed", fault.getFailures() > 0);
            applyUnconsumedBacklog(FILTERED_REPLACEMENT, "ALTER TABLE base DEDUP DISABLE");
            fault.disarm();
            fault.armDedupHistory(engine.verifyTableName("base"));
            shutdown();

            LiveViewRebuildRestatementGuard guard = restart();

            assertRebuildBlocked(instance("lv"), "timeline is absent");
            Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_NONE, guard.getAbstention());
            Assert.assertEquals(LiveViewRebuildRestatementGuard.VERDICT_ROW_SHORTFALL, guard.getVerdict());
            assertFilteredViewRows(FILTERED_ROWS);
            Assert.assertTrue("the restart must have tried to read the dedup history", fault.getDedupHistoryFailures() > 0);
            capture.drain();
            capture.assertLogged("live view could not read a base schema change, the rebuild guard compares [view=lv");
            capture.assertNotLogged("live view rebuild from the applied base runs without the restatement guard");

            // The history reads again, and the next restart finds the replacement ran under dedup.
            fault.disarmDedupHistory();
            shutdown();

            guard = restart();

            final LiveViewInstance instance = instance("lv");
            Assert.assertFalse(
                    "the rebuild must follow the base, not stop the view: " + instance.getCheckpointRecoveryReason(),
                    instance.isCheckpointRecoveryBlocked()
            );
            Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_BACKLOG_MAY_REMOVE, guard.getAbstention());
            assertFilteredViewRows(FILTERED_REPLACED_ROWS);
            assertLiveViewsReportsNoRecovery(instance);

            execute(FILTERED_APPEND);
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            assertFilteredViewRows(FILTERED_REPLACED_ROWS + FILTERED_APPEND_OUTPUT);
        });
    }

    @Test
    public void testGuardAbstentionsCompareNothing() {
        final LiveViewRebuildRestatementGuard guard = new LiveViewRebuildRestatementGuard();
        Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_NOT_EVALUATED, guard.getAbstention());
        final int[] abstentions = {
                LiveViewRebuildRestatementGuard.ABSTAIN_DISABLED,
                LiveViewRebuildRestatementGuard.ABSTAIN_NOTHING_RETAINED,
                LiveViewRebuildRestatementGuard.ABSTAIN_BACKLOG_MAY_REMOVE,
                LiveViewRebuildRestatementGuard.ABSTAIN_BACKLOG_UNREADABLE,
                LiveViewRebuildRestatementGuard.ABSTAIN_FORMAT_UPGRADE
        };
        for (int abstention : abstentions) {
            // Armed over evidence both checks would refuse, then stood down: nothing it held
            // survives the disarm, and nothing it is shown afterwards counts.
            guard.arm(10, ts("2026-01-01T00:00:00.000000Z"), ts("2026-01-02T00:00:00.000000Z"), 0, 0);
            guard.disarm(abstention);
            guard.observe(ts("2026-01-01T12:00:00.000000Z"));
            Assert.assertEquals(abstention, guard.getAbstention());
            Assert.assertFalse(guard.isHistoryFloorBreached());
            Assert.assertFalse(guard.isRowShortfall());
            Assert.assertEquals(0, guard.getReproducedRows());
            Assert.assertNotEquals("not evaluated", LiveViewRebuildRestatementGuard.abstentionName(abstention));
        }
    }

    @Test
    public void testGuardHistoryFloorIsStrictAndCoversAnEmptyBase() {
        final LiveViewRebuildRestatementGuard guard = new LiveViewRebuildRestatementGuard();
        final long viewMin = ts("2026-01-01T09:00:00.000000Z");
        final long viewMax = ts("2026-01-03T09:00:00.000000Z");

        // A base row AT the view's earliest timestamp may be the one that produced it.
        guard.arm(6, viewMin, viewMax, 4, viewMin);
        Assert.assertFalse(guard.isHistoryFloorBreached());
        guard.arm(6, viewMin, viewMax, 4, viewMin - 1);
        Assert.assertFalse(guard.isHistoryFloorBreached());
        guard.arm(6, viewMin, viewMax, 4, viewMin + 1);
        Assert.assertTrue(guard.isHistoryFloorBreached());

        // An empty base has no earliest row to compare against, and every row the view holds is
        // below its floor - whatever the minimum the reader reports for no rows.
        guard.arm(6, viewMin, viewMax, 0, Long.MIN_VALUE);
        Assert.assertTrue(guard.isHistoryFloorBreached());
        guard.refuse(LiveViewRebuildRestatementGuard.VERDICT_HISTORY_FLOOR);
        final StringSink sink = new StringSink();
        guard.appendEvidence(sink, MicrosTimestampDriver.INSTANCE);
        TestUtils.assertEquals("the view holds rows from 2026-01-01T09:00:00.000000Z but the base table holds no rows", sink);
    }

    @Test
    public void testGuardLostPartitionRefusesOnlyWhatWasRecordedSinceArming() {
        final LiveViewRebuildRestatementGuard guard = new LiveViewRebuildRestatementGuard();
        final long viewMin = ts("2026-01-01T09:00:00.000000Z");
        final long viewMax = ts("2026-01-03T09:00:00.000000Z");
        final long lostRow = ts("2026-01-02T09:00:00.000000Z");
        final long lostPartitionLo = ts("2026-01-02T00:00:00.000000Z");
        final long lostPartitionHi = ts("2026-01-03T00:00:00.000000Z");
        final int[] abstentions = {
                LiveViewRebuildRestatementGuard.ABSTAIN_BACKLOG_MAY_REMOVE,
                LiveViewRebuildRestatementGuard.ABSTAIN_BACKLOG_UNREADABLE
        };
        for (int abstention : abstentions) {
            // Stood down to the history floor and the lost partition check over a floor that
            // holds: nothing is lost until a row is recorded.
            guard.arm(3, viewMin, viewMax, 3, viewMin);
            guard.disarmRowShortfall(abstention);
            Assert.assertFalse(guard.isBasePartitionLost());
            guard.observeLostPartition(lostRow, lostPartitionLo, lostPartitionHi);
            Assert.assertTrue(guard.isBasePartitionLost());
            Assert.assertFalse(guard.isHistoryFloorBreached());
            Assert.assertFalse(guard.isRowShortfall());
            guard.refuse(LiveViewRebuildRestatementGuard.VERDICT_LOST_PARTITION);
            Assert.assertEquals(LiveViewRebuildRestatementGuard.VERDICT_LOST_PARTITION, guard.getVerdict());
            final StringSink sink = new StringSink();
            guard.appendEvidence(sink, MicrosTimestampDriver.INSTANCE);
            TestUtils.assertEquals(
                    "the view holds a row at 2026-01-02T09:00:00.000000Z but the base table holds no partition "
                            + "between 2026-01-02T00:00:00.000000Z and 2026-01-03T00:00:00.000000Z",
                    sink
            );

            // The next rebuild's arming forgets the row and the verdict, and so does a full
            // stand-down.
            guard.arm(3, viewMin, viewMax, 3, viewMin);
            Assert.assertFalse(guard.isBasePartitionLost());
            Assert.assertEquals(LiveViewRebuildRestatementGuard.VERDICT_NONE, guard.getVerdict());
            guard.disarmRowShortfall(abstention);
            guard.observeLostPartition(lostRow, lostPartitionLo, lostPartitionHi);
            guard.disarm(abstention);
            Assert.assertFalse(guard.isBasePartitionLost());
        }
    }

    @Test
    public void testGuardRowShortfallCountsOnlyRowsAtOrBelowTheFrontier() {
        final LiveViewRebuildRestatementGuard guard = new LiveViewRebuildRestatementGuard();
        final long viewMax = ts("2026-01-03T09:00:00.000000Z");
        guard.arm(3, ts("2026-01-01T09:00:00.000000Z"), viewMax, 3, ts("2026-01-01T09:00:00.000000Z"));

        guard.observe(ts("2026-01-01T09:00:00.000000Z"));
        guard.observe(viewMax);
        // Above the frontier: a row from a base transaction the view had not consumed. It adds
        // to the recompute, not to what the recompute reproduces of the view.
        guard.observe(viewMax + 1);
        Assert.assertEquals(2, guard.getReproducedRows());
        Assert.assertTrue(guard.isRowShortfall());
        guard.refuse(LiveViewRebuildRestatementGuard.VERDICT_ROW_SHORTFALL);
        final StringSink sink = new StringSink();
        guard.appendEvidence(sink, MicrosTimestampDriver.INSTANCE);
        TestUtils.assertEquals("the rebuild reproduces 2 of the 3 rows the view holds up to 2026-01-03T09:00:00.000000Z", sink);

        guard.observe(viewMax);
        Assert.assertFalse("a recompute that reproduces every row is no shortfall", guard.isRowShortfall());
    }

    @Test
    public void testGuardRowShortfallDisarmKeepsTheHistoryFloor() {
        final LiveViewRebuildRestatementGuard guard = new LiveViewRebuildRestatementGuard();
        final long viewMin = ts("2026-01-01T09:00:00.000000Z");
        final long viewMax = ts("2026-01-03T09:00:00.000000Z");
        final int[] abstentions = {
                LiveViewRebuildRestatementGuard.ABSTAIN_BACKLOG_MAY_REMOVE,
                LiveViewRebuildRestatementGuard.ABSTAIN_BACKLOG_UNREADABLE
        };
        for (int abstention : abstentions) {
            // Armed over evidence both checks would refuse, then stood down over a backlog that
            // may hold a dedup replacement: the recompute's count no longer counts, and the
            // history floor still refuses.
            guard.arm(10, viewMin, viewMax, 4, viewMin + 1);
            guard.disarmRowShortfall(abstention);
            guard.observe(viewMin);
            Assert.assertEquals(abstention, guard.getAbstention());
            Assert.assertEquals(0, guard.getReproducedRows());
            Assert.assertFalse(guard.isRowShortfall());
            Assert.assertTrue(guard.isHistoryFloorBreached());
            guard.refuse(LiveViewRebuildRestatementGuard.VERDICT_HISTORY_FLOOR);
            Assert.assertEquals(LiveViewRebuildRestatementGuard.VERDICT_HISTORY_FLOOR, guard.getVerdict());
            final StringSink sink = new StringSink();
            guard.appendEvidence(sink, MicrosTimestampDriver.INSTANCE);
            TestUtils.assertEquals(
                    "the view holds rows from 2026-01-01T09:00:00.000000Z but the base table's earliest row is at 2026-01-01T09:00:00.000001Z",
                    sink
            );

            // A replacement at the view's earliest timestamp leaves the base's earliest row there.
            guard.arm(10, viewMin, viewMax, 4, viewMin);
            guard.disarmRowShortfall(abstention);
            Assert.assertFalse(guard.isHistoryFloorBreached());
            Assert.assertFalse(guard.isRowShortfall());
        }
    }

    /**
     * Asserts the disposition every refusal leaves: a stopped view that is not a durable
     * invalidation, blocked on the rebuild phase with a reason naming the route that asked for
     * the rebuild and the operator's ways out.
     */
    private static void assertRebuildBlocked(LiveViewInstance instance, String cause) {
        Assert.assertTrue("the view must be stopped", instance.isCheckpointRecoveryBlocked());
        Assert.assertFalse("a rebuild block is not a format block", instance.isCheckpointFormatBlocked());
        Assert.assertEquals(LiveViewCheckpointRecoveryPhase.REBUILD_BLOCKED, instance.getCheckpointRecoveryPhase());
        Assert.assertFalse("a refused rebuild must not write _lv.s.invalid", instance.isInvalid());
        final String reason = instance.getCheckpointRecoveryReason();
        TestUtils.assertContains(reason, "rebuilding the view from its base table would drop rows it retains [cause=" + cause + "]");
        TestUtils.assertContains(reason, "DROP and re-create the view");
        TestUtils.assertContains(reason, "cairo.live.view.rebuild.restatement.guard.enabled=false");
    }

    /**
     * The operator text a deferred rebuild publishes, waiting for the base to apply
     * {@code rebuildSeqTxn} on behalf of the recovery {@code cause} names.
     */
    private static String deferralReason(String cause, long rebuildSeqTxn) {
        return "rebuilding the view from its base table waits for the base table to apply what the view consumed "
                + "[cause=" + cause + ", baseTable=tx, rebuildSeqTxn=" + rebuildSeqTxn + "]: the view's table "
                + "holds output of base commits the base table has not applied yet, and nothing has moved. Refresh "
                + "resumes on its own once the base table applies seqTxn " + rebuildSeqTxn + "; a base table whose WAL "
                + "apply is suspended (see wal_tables()) keeps the view waiting until the apply resumes";
    }

    /**
     * Whether {@code name} is a checkpoint data segment still carrying its temporary suffix: the
     * file a repair's replay opens when it freezes a root, and a seal opens when it stages one.
     */
    private static boolean isStagedSegment(LPSZ name) {
        return Utf8s.containsAscii(name, LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME)
                && Utf8s.containsAscii(name, DATA_SEGMENT_PATH_PART)
                && Utf8s.endsWithAscii(name, LiveViewCheckpointLayout.TMP_SUFFIX);
    }

    /**
     * The view rows the first {@code count} of {@link #ROWS_AHEAD} produce, in order.
     */
    private static String rowsAheadOutput(int count) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(ROWS_AHEAD_OUTPUT[i]);
        }
        return sb.toString();
    }

    /**
     * Applies each of {@code backlog} to the base, one apply at a time, without refreshing the
     * view.
     */
    private void applyUnconsumedBacklog(String... backlog) throws Exception {
        final long processedBefore = instance("lv").getLastProcessedSeqTxn();
        for (String statement : backlog) {
            execute(statement);
            drainWalQueue();
        }
        Assert.assertEquals("the view must not consume the backlog", processedBefore, instance("lv").getLastProcessedSeqTxn());
    }

    /**
     * Refreshes three positive rows into a view filtering {@code i > 0} while every open of its
     * {@code _timeline} fails, so the view's table holds their output and no checkpoint describes
     * it. Then applies {@code backlog} to the base without refreshing the view - a replacement of
     * the first row with one the filter rejects, around DEDUP changes - and restarts, first
     * removing the base's WAL when {@code isBaseWalLost}. The restart finds no timeline and
     * rebuilds from the applied base, which legitimately holds one output row fewer: the rebuild
     * must follow the base, a second restart must keep it, and a later commit must still reach
     * the view.
     */
    private void assertDedupBacklogRebuildFollowsTheBase(
            boolean isBaseWalLost,
            String dedupClause,
            String... backlog
    ) throws Exception {
        assertDedupBacklogRebuildOverRowsFollowsTheBase(
                isBaseWalLost,
                dedupClause,
                FILTERED_ROWS_INSERT,
                FILTERED_ROWS,
                FILTERED_REPLACED_ROWS,
                FILTERED_APPEND,
                FILTERED_APPEND_OUTPUT,
                backlog
        );
    }

    /**
     * The same, over the rows {@code rowsInsert} commits, which the view holds as {@code rows}
     * and a rebuild that follows the base holds as {@code replacedRows}. {@code append} is the
     * later commit that must still reach the view, as {@code appendOutput}.
     */
    private void assertDedupBacklogRebuildOverRowsFollowsTheBase(
            boolean isBaseWalLost,
            String dedupClause,
            String rowsInsert,
            String rows,
            String replacedRows,
            String append,
            String appendOutput,
            String... backlog
    ) throws Exception {
        final TimelineOpenFault fault = new TimelineOpenFault();
        assertMemoryLeak(fault, () -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG) "
                    + "TIMESTAMP(ts) PARTITION BY DAY WAL " + dedupClause);
            createFilteredView();
            refreshIntoFilteredView(rowsInsert);
            Assert.assertTrue("the view's timeline open must have failed", fault.getFailures() > 0);
            assertFilteredViewRows(rows);
            applyUnconsumedBacklog(backlog);
            fault.disarm();
            shutdown();
            if (isBaseWalLost) {
                removeBaseWal("base");
            }

            final LiveViewRebuildRestatementGuard guard = restart();

            final LiveViewInstance instance = instance("lv");
            Assert.assertFalse(
                    "the rebuild must follow the base, not stop the view: " + instance.getCheckpointRecoveryReason(),
                    instance.isCheckpointRecoveryBlocked()
            );
            assertRebuiltFromAppliedBase("lv");
            Assert.assertEquals(
                    "a replacement that ran under dedup can legitimately remove an output row",
                    isBaseWalLost
                            ? LiveViewRebuildRestatementGuard.ABSTAIN_BACKLOG_UNREADABLE
                            : LiveViewRebuildRestatementGuard.ABSTAIN_BACKLOG_MAY_REMOVE,
                    guard.getAbstention()
            );
            assertFilteredViewRows(replacedRows);
            assertLiveViewsReportsNoRecovery(instance);

            shutdown();
            restart();
            Assert.assertFalse(instance("lv").isCheckpointRecoveryBlocked());
            assertFilteredViewRows(replacedRows);
            assertLiveViewsReportsNoRecovery(instance("lv"));

            execute(append);
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            assertFilteredViewRows(replacedRows + appendOutput);
            assertLiveViewsReportsNoRecovery(instance("lv"));
        });
    }

    /**
     * Asserts the base metadata drift recovery tried to restore the runtime from the view's
     * timeline and found no generation in it, which is what sends it to the rebuild the caller
     * asserts refused. The view never published a generation, and the anchor lookup of the repair
     * the drift interrupts opens the view's {@code _timeline}, which creates an empty one. That
     * repair retires the timeline only once its replay has ended, so the empty file is still there
     * when the recovery reads it.
     */
    private void assertDriftRestoreFoundNoGeneration() {
        capture.assertLogged("live view could not restore its runtime from the checkpoint timeline, rebuilding from the applied base "
                + "[view=lv, cause=base table metadata change, error=");
        capture.assertLogged("live view checkpoint has no valid generation to restore");
    }

    private void assertFilteredViewRows(String expected) throws Exception {
        assertQuery("SELECT ts, sym, i, v FROM lv")
                .noLeakCheck()
                .timestamp("ts")
                .expectSize()
                .returns(expected);
    }

    /**
     * Seeds {@link #FILTERED_TWO_DAY_ROWS} into a view over a base created with
     * {@code tableOptions}, filtering {@code i > 0} when {@code isFiltered}, while every open of
     * its {@code _timeline} fails. Then applies {@code backlog} to the base without refreshing
     * the view - a loss of the base's first day among it, and the base's WAL with it when
     * {@code isBaseWalLost} - and restarts. See {@link #assertLostDayRebuildRefusedOnRestart}.
     */
    private void assertLostDayRebuildRefused(
            String tableOptions,
            boolean isFiltered,
            boolean isBaseWalLost,
            String... backlog
    ) throws Exception {
        assertLostDayRebuildRefused(
                tableOptions,
                isFiltered,
                isBaseWalLost,
                LiveViewRebuildRestatementGuard.ABSTAIN_NONE,
                backlog
        );
    }

    /**
     * The same, for a guard that compares with {@code abstention} in force: either
     * {@link LiveViewRebuildRestatementGuard#ABSTAIN_NONE}, both checks, or the row shortfall's
     * stand-down over a backlog that may hold a dedup replacement, which keeps the history floor.
     */
    private void assertLostDayRebuildRefused(
            String tableOptions,
            boolean isFiltered,
            boolean isBaseWalLost,
            int abstention,
            String... backlog
    ) throws Exception {
        final TimelineOpenFault fault = new TimelineOpenFault();
        assertMemoryLeak(fault, () -> {
            seedTwoDayView(tableOptions, isFiltered, fault);
            applyUnconsumedBacklog(backlog);
            fault.disarm();
            assertLostDayRebuildRefusedOnRestart(isBaseWalLost, abstention);
        });
    }

    /**
     * Restarts a view that holds {@link #FILTERED_TWO_DAY_ROWS} and no timeline over a base that
     * lost its first day, first removing the base's WAL when {@code isBaseWalLost}. The restart
     * rebuilds from the applied base, whose history floor sees the lost day, so the guard must
     * refuse, on that restart and the next, and keep every row. {@code abstention} is what the
     * guard compares with: {@link LiveViewRebuildRestatementGuard#ABSTAIN_NONE} when no backlog
     * commit can have taken a row out of the view, and otherwise the row shortfall's stand-down,
     * which leaves the history floor to refuse.
     */
    private void assertLostDayRebuildRefusedOnRestart(boolean isBaseWalLost, int abstention) throws Exception {
        shutdown();
        if (isBaseWalLost) {
            removeBaseWal("base");
        }

        final LiveViewRebuildRestatementGuard guard = restart();

        final LiveViewInstance instance = instance("lv");
        assertRebuildBlocked(instance, "timeline is absent");
        TestUtils.assertContains(instance.getCheckpointRecoveryReason(), FILTERED_LOST_DAY_EVIDENCE);
        Assert.assertEquals(abstention, guard.getAbstention());
        Assert.assertEquals(LiveViewRebuildRestatementGuard.VERDICT_HISTORY_FLOOR, guard.getVerdict());
        capture.drain();
        capture.assertNotLogged("live view rebuild from the applied base runs without the restatement guard");
        if (abstention == LiveViewRebuildRestatementGuard.ABSTAIN_NONE) {
            capture.assertNotLogged(ROW_SHORTFALL_STAND_DOWN_LINE);
        } else {
            capture.assertLogged(ROW_SHORTFALL_STAND_DOWN_LINE + " [view=lv, reason="
                    + LiveViewRebuildRestatementGuard.abstentionName(abstention) + "]");
        }
        if (isBaseWalLost) {
            capture.assertLogged(abstention == LiveViewRebuildRestatementGuard.ABSTAIN_NONE
                    ? "guardChecks=history floor and row shortfall,"
                    : "guardChecks=history floor and base partitions,");
        } else {
            capture.assertNotLogged("live view could not read the rebuild's base backlog");
        }
        assertFilteredViewRows(FILTERED_TWO_DAY_ROWS);

        shutdown();
        restart();
        assertRebuildBlocked(instance("lv"), "timeline is absent");
        assertFilteredViewRows(FILTERED_TWO_DAY_ROWS);
    }

    /**
     * Seeds {@link #FILTERED_LATER_DAYS_ROWS} into a view created with {@code viewPartitionBy}
     * while every open of its {@code _timeline} fails, over a base without dedup keys that also
     * holds an older row the view does not - see {@link #seedLaterDaysView}. Then applies
     * {@code backlog} to the base without refreshing the view - a loss of the base's second day
     * and a DEDUP change behind a commit that reaches the frontier among it - and restarts. The
     * history floor cannot see the lost day and the DEDUP change stands the row shortfall down,
     * so the refusal has to come from the base partition the view's oldest row lost, on that
     * restart and the next.
     */
    private void assertLostOldestDayRebuildRefused(
            String viewPartitionBy,
            boolean isOlderRowBelowStartFrom,
            String... backlog
    ) throws Exception {
        final TimelineOpenFault fault = new TimelineOpenFault();
        assertMemoryLeak(fault, () -> {
            seedLaterDaysView(viewPartitionBy, isOlderRowBelowStartFrom);
            Assert.assertTrue("the view's timeline open must have failed", fault.getFailures() > 0);
            applyUnconsumedBacklog(backlog);
            fault.disarm();
            shutdown();

            final LiveViewRebuildRestatementGuard guard = restart();

            assertLostPartitionRefused(
                    guard,
                    "timeline is absent",
                    LiveViewRebuildRestatementGuard.ABSTAIN_BACKLOG_MAY_REMOVE,
                    FILTERED_LATER_DAYS_LOST_DAY_EVIDENCE,
                    FILTERED_LATER_DAYS_ROWS
            );
            capture.assertNotLogged("live view could not read the rebuild's base backlog");

            shutdown();
            restart();
            assertRebuildBlocked(instance("lv"), "timeline is absent");
            assertFilteredViewRows(FILTERED_LATER_DAYS_ROWS);
        });
    }

    /**
     * Asserts the refusal a whole-view rebuild meets when the view holds a row in a base partition
     * that no longer exists, behind a backlog that stood the row shortfall down with
     * {@code abstention}: a stopped view whose reason names the route {@code cause} and the
     * {@code evidence}, no rebuild that ran without the guard, and every one of {@code rows} kept.
     */
    private void assertLostPartitionRefused(
            LiveViewRebuildRestatementGuard guard,
            String cause,
            int abstention,
            String evidence,
            String rows
    ) throws Exception {
        final LiveViewInstance instance = instance("lv");
        assertRebuildBlocked(instance, cause);
        TestUtils.assertContains(instance.getCheckpointRecoveryReason(), evidence);
        Assert.assertEquals(abstention, guard.getAbstention());
        Assert.assertEquals(LiveViewRebuildRestatementGuard.VERDICT_LOST_PARTITION, guard.getVerdict());
        capture.drain();
        capture.assertLogged(ROW_SHORTFALL_STAND_DOWN_LINE + " [view=lv, reason="
                + LiveViewRebuildRestatementGuard.abstentionName(abstention) + "]");
        capture.assertNotLogged("live view rebuild from the applied base runs without the restatement guard");
        if (abstention == LiveViewRebuildRestatementGuard.ABSTAIN_BACKLOG_UNREADABLE) {
            capture.assertLogged("guardChecks=history floor and base partitions,");
        }
        assertFilteredViewRows(rows);
    }

    /**
     * Asserts a view whose deferred rebuild ran, or whose recovery otherwise finished, reports no
     * recovery at all: both recovery columns NULL beside an {@code active} status.
     */
    private void assertLiveViewsReportsNoRecovery(LiveViewInstance instance) throws Exception {
        Assert.assertFalse(instance.isCheckpointRebuildDeferred());
        Assert.assertEquals(LiveViewCheckpointRecoveryPhase.NONE, instance.getCheckpointRecoveryPhase());
        Assert.assertNull(instance.getCheckpointRecoveryReason());
        // The wait the rebuild was in goes with the phase: a view that owes nothing waits for
        // nothing, so the two base_apply_wait_* columns read NULL beside the two recovery ones.
        Assert.assertEquals(Numbers.LONG_NULL, instance.getApplyLagDeferSinceUs());
        Assert.assertEquals(Numbers.LONG_NULL, instance.getApplyLagDeferTargetSeqTxn());
        assertQuery("SELECT view_status, checkpoint_recovery_phase, invalidation_reason, checkpoint_recovery_reason, "
                + "base_apply_wait_seqtxn, base_apply_wait_micros "
                + "FROM live_views() WHERE view_name = 'lv'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("""
                        view_status\tcheckpoint_recovery_phase\tinvalidation_reason\tcheckpoint_recovery_reason\tbase_apply_wait_seqtxn\tbase_apply_wait_micros
                        active\t\t\t\tnull\tnull
                        """);
    }

    private void assertLiveViewsReportsTheBlock() throws Exception {
        assertQuery("SELECT view_status, checkpoint_recovery_phase, "
                + "invalidation_reason = checkpoint_recovery_reason AS reason_mirrored "
                + "FROM live_views() WHERE view_name = 'lv'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("""
                        view_status\tcheckpoint_recovery_phase\treason_mirrored
                        invalid\trebuild_blocked\ttrue
                        """);
    }

    /**
     * Refreshes {@link #FILTERED_THREE_DAY_ROWS} into a view with no timeline over a base created
     * with {@code tableOptions}, which then loses the middle day behind a commit above the view's
     * frontier, and restarts: the rebuild's row shortfall refuses. A blocked view holds no WAL
     * floor, so the routine sweep takes the base's WAL behind the refusal, and a DEDUP DISABLE
     * lands on the base after the commits the sweep took. The restart that follows cannot read
     * those commits, and the DEDUP DISABLE leaves it unable to say they ran without dedup, so the
     * row shortfall stands down. The day the base lost must still refuse, on that restart and the
     * next, and every row must stay.
     */
    private void assertPurgedMiddleDayRefusalSurvivesADedupDisable(String tableOptions) throws Exception {
        // WalPurgeJob.runSerially is interval-gated off the millisecond clock, which this class
        // freezes. Without this the sweep below silently does nothing.
        setProperty(PropertyKey.CAIRO_WAL_PURGE_INTERVAL, 0);
        final TimelineOpenFault fault = new TimelineOpenFault();
        assertMemoryLeak(fault, () -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG) TIMESTAMP(ts) PARTITION BY DAY " + tableOptions);
            createFilteredView();
            refreshIntoFilteredView(FILTERED_THREE_DAY_ROWS_INSERT);
            Assert.assertTrue("the view's timeline open must have failed", fault.getFailures() > 0);
            assertFilteredViewRows(FILTERED_THREE_DAY_ROWS);
            applyUnconsumedBacklog(
                    "ALTER TABLE base DROP PARTITION LIST '2026-01-02'",
                    "INSERT INTO base (ts, sym, i) VALUES ('2026-01-04T00:05:00.000000Z', 'a', 7)"
            );
            fault.disarm();
            shutdown();
            restart();
            assertRebuildBlocked(instance("lv"), "timeline is absent");
            TestUtils.assertContains(
                    instance("lv").getCheckpointRecoveryReason(),
                    "the rebuild reproduces 2 of the 3 rows the view holds up to 2026-01-03T00:09:00.000000Z"
            );

            engine.releaseInactive();
            setCurrentMicros(currentMicros + 60_000_000L);
            try (WalPurgeJob purgeJob = new WalPurgeJob(engine)) {
                purgeJob.drain(0);
            }
            Assert.assertEquals("the sweep must have taken the base's WAL", 0, countBaseWalDirs("base"));
            execute("ALTER TABLE base DEDUP DISABLE");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            shutdown();

            final LiveViewRebuildRestatementGuard guard = restart();

            assertLostPartitionRefused(
                    guard,
                    "timeline is absent",
                    LiveViewRebuildRestatementGuard.ABSTAIN_BACKLOG_UNREADABLE,
                    FILTERED_THREE_DAY_LOST_DAY_EVIDENCE,
                    FILTERED_THREE_DAY_ROWS
            );

            shutdown();
            restart();
            assertRebuildBlocked(instance("lv"), "timeline is absent");
            assertFilteredViewRows(FILTERED_THREE_DAY_ROWS);
        });
    }

    /**
     * Asserts the whole-view rebuild a mid-drain recovery asked for waited for the base's apply
     * instead of running: nothing pinned a snapshot, nothing was refused, rebuilt or charged to the
     * retry budget, and the window-state debt stands on the instance behind an apply-lag back-off
     * that names the commit the view flushed past the base's applied head. The wait is reported:
     * the view is not stopped, so {@code live_views()} keeps it {@code active} and says what it
     * waits for through the two recovery columns alone.
     */
    private void assertRebuildDeferred(LiveViewRefreshJob job, LiveViewInstance instance, long baseApplied) throws Exception {
        assertRebuildDeferred(job, instance, MID_DRAIN_CAUSE, baseApplied);
    }

    /**
     * The same, for a deferral a named recovery asked for. Both running doors reach the rebuild
     * the same way, so both report the same pair of columns under the same phase; only the cause
     * the reason and the log line carry tells them apart.
     */
    private void assertRebuildDeferred(
            LiveViewRefreshJob job,
            LiveViewInstance instance,
            String cause,
            long baseApplied
    ) throws Exception {
        capture.drain();
        capture.assertLogged("live view rebuild from the applied base waits for the base table to apply what the view consumed "
                + "[view=lv, cause=" + cause + ", rebuildSeqTxn=" + (baseApplied + 1)
                + ", appliedSeqTxn=" + baseApplied + "]");
        capture.assertNotLogged("live view recomputed window state from applied base");
        Assert.assertEquals(
                "no whole-view rebuild may have pinned a snapshot",
                LiveViewRebuildRestatementGuard.ABSTAIN_NOT_EVALUATED,
                job.rebuildRestatementGuardForTest().getAbstention()
        );
        Assert.assertTrue("the recovery the rebuild owes must carry to a later turn", instance.isWindowStateDirty());
        Assert.assertEquals(baseApplied + 1, instance.getApplyLagDeferTargetSeqTxn());
        Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
        Assert.assertFalse(instance.isInvalid());
        Assert.assertEquals("the fault that asked for the recovery is the one fault", 1, instance.getRefreshFaultCount());
        Assert.assertEquals("a deferral charges no retry", 0, instance.getFlushRetryCount());
        Assert.assertEquals(baseApplied + 1, instance.getLastProcessedSeqTxn());

        Assert.assertTrue("the wait must be reported", instance.isCheckpointRebuildDeferred());
        Assert.assertEquals(LiveViewCheckpointRecoveryPhase.REBUILD_DEFERRED, instance.getCheckpointRecoveryPhase());
        Assert.assertEquals(deferralReason(cause, baseApplied + 1), instance.getCheckpointRecoveryReason());
        // A deferred rebuild is an apply-lag wait like any other, so it reports through the two
        // base_apply_wait_* columns as well: the seqTxn the phase's reason names, and a duration
        // the frozen test clock pins to the stamp the deferral took.
        Assert.assertEquals(currentMicros, instance.getApplyLagDeferSinceUs());
        assertQuery("SELECT view_status, checkpoint_recovery_phase, invalidation_reason, checkpoint_recovery_reason, "
                + "base_apply_wait_seqtxn, base_apply_wait_micros "
                + "FROM live_views() WHERE view_name = 'lv'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("view_status\tcheckpoint_recovery_phase\tinvalidation_reason\tcheckpoint_recovery_reason\t"
                        + "base_apply_wait_seqtxn\tbase_apply_wait_micros\n"
                        + "active\trebuild_deferred\t\t" + deferralReason(cause, baseApplied + 1) + "\t"
                        + (baseApplied + 1) + "\t0\n");
    }

    /**
     * Refreshes rows on three days into a view created with {@code viewPartitionBy} while every
     * open of its {@code _timeline} fails, over a deduplicating base whose middle day holds only a
     * row the filter rejects. Then the base loses that day, replaces a row the view holds with one
     * the filter rejects, and disables dedup, all without refreshing the view, and the view
     * restarts. The base holds no partition for the middle day, but the view holds no row of it:
     * the rebuild must follow the base, a second restart must keep it, and a later commit must
     * still reach the view.
     */
    private void assertRejectedDayLossRebuildFollowsTheBase(String viewPartitionBy) throws Exception {
        final TimelineOpenFault fault = new TimelineOpenFault();
        assertMemoryLeak(fault, () -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG) "
                    + "TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, sym)");
            createBaseView(viewPartitionBy + " START FROM BEGINNING", true);
            refreshIntoFilteredView("""
                    INSERT INTO base (ts, sym, i) VALUES
                        ('2026-01-01T00:01:00.000000Z', 'a', 297),
                        ('2026-01-02T00:05:00.000000Z', 'a', -5),
                        ('2026-01-03T00:05:00.000000Z', 'a', 500),
                        ('2026-01-03T00:09:00.000000Z', 'a', 900)""");
            Assert.assertTrue("the view's timeline open must have failed", fault.getFailures() > 0);
            assertFilteredViewRows("""
                    ts\tsym\ti\tv
                    2026-01-01T00:01:00.000000Z\ta\t297\t297.0
                    2026-01-03T00:05:00.000000Z\ta\t500\t500.0
                    2026-01-03T00:09:00.000000Z\ta\t900\t1400.0
                    """);
            applyUnconsumedBacklog(
                    "ALTER TABLE base DROP PARTITION LIST '2026-01-02'",
                    "INSERT INTO base (ts, sym, i) VALUES ('2026-01-03T00:05:00.000000Z', 'a', -108)",
                    "ALTER TABLE base DEDUP DISABLE"
            );
            fault.disarm();
            shutdown();

            final LiveViewRebuildRestatementGuard guard = restart();

            final LiveViewInstance instance = instance("lv");
            Assert.assertFalse(
                    "the rebuild must follow the base, not stop the view: " + instance.getCheckpointRecoveryReason(),
                    instance.isCheckpointRecoveryBlocked()
            );
            assertRebuiltFromAppliedBase("lv");
            Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_BACKLOG_MAY_REMOVE, guard.getAbstention());
            Assert.assertEquals(LiveViewRebuildRestatementGuard.VERDICT_NONE, guard.getVerdict());
            final String replacedRows = """
                    ts\tsym\ti\tv
                    2026-01-01T00:01:00.000000Z\ta\t297\t297.0
                    2026-01-03T00:09:00.000000Z\ta\t900\t900.0
                    """;
            assertFilteredViewRows(replacedRows);
            assertLiveViewsReportsNoRecovery(instance);

            shutdown();
            restart();
            Assert.assertFalse(instance("lv").isCheckpointRecoveryBlocked());
            assertFilteredViewRows(replacedRows);

            execute("INSERT INTO base (ts, sym, i) VALUES ('2026-01-03T00:10:00.000000Z', 'a', 1000)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            assertFilteredViewRows(replacedRows + "2026-01-03T00:10:00.000000Z\ta\t1000\t1900.0\n");
            assertLiveViewsReportsNoRecovery(instance("lv"));
        });
    }

    private void assertViewRows(String expected) throws Exception {
        assertQuery(VIEW_ROWS_QUERY)
                .noLeakCheck()
                .timestamp("created_at")
                .expectSize()
                .returns(expected);
    }

    /**
     * A view over {@code base} with a bounded RANGE sum, filtering {@code i > 0} when
     * {@code isFiltered}.
     */
    private void createBaseView(boolean isFiltered) throws Exception {
        createBaseView("START FROM BEGINNING", isFiltered);
    }

    /**
     * The same, created with {@code viewOptions} - the clauses between FLUSH EVERY and AS.
     */
    private void createBaseView(String viewOptions, boolean isFiltered) throws Exception {
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms " + viewOptions + " AS\n" + """
                SELECT ts, sym, i,
                       sum(i) OVER (
                           PARTITION BY sym ORDER BY ts
                           RANGE BETWEEN '9' MINUTE PRECEDING AND CURRENT ROW
                       ) AS v
                FROM base""" + (isFiltered ? " WHERE i > 0" : ""));
    }

    /**
     * A view over {@code base} whose filter the cases' replacement row fails, with a bounded
     * RANGE sum the replacement changes for the rows after it.
     */
    private void createFilteredView() throws Exception {
        createBaseView(true);
    }

    /**
     * The same accumulators as {@link #createView()} over a ROWS frame wider than any account's
     * history in these cases, beside a lag that ignores nulls. That lag reaches back an unbounded
     * number of rows, so no dependency bounds the window: an out-of-order repair either resumes
     * from a sealed boundary below the change or replays the whole base from the view boundary.
     */
    private void createUnlocalizedView() throws Exception {
        execute("""
                CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS
                SELECT created_at, account_id,
                       sum(amount) OVER (PARTITION BY account_id ORDER BY created_at ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS cumulative_sum,
                       count(account_id) OVER (PARTITION BY account_id ORDER BY created_at ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS cumulative_count,
                       lag(amount, 1) IGNORE NULLS OVER (PARTITION BY account_id ORDER BY created_at ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS previous_amount
                FROM tx""");
    }

    private void createView() throws Exception {
        createView("");
    }

    /**
     * The same, with {@code whereClause} between the FROM and the WINDOW clauses, or none when
     * it is empty.
     */
    private void createView(String whereClause) throws Exception {
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                + "SELECT created_at, account_id, sum(amount) OVER w AS cumulative_sum, "
                + "count(account_id) OVER w AS cumulative_count "
                + "FROM tx" + (whereClause.isEmpty() ? "" : " " + whereClause)
                + " WINDOW w AS (PARTITION BY account_id ORDER BY created_at ANCHOR DAILY '00:00')");
    }

    /**
     * Drops one base partition the view has already derived rows from, and lets the view walk
     * past the DROP PARTITION the way it always has: it keeps those rows.
     */
    private void dropPartitionAndRefresh(String day) throws Exception {
        dropPartitionAndRefresh(day, ALL_ROWS);
    }

    private void dropPartitionAndRefresh(String day, String expectedViewRows) throws Exception {
        execute("ALTER TABLE tx DROP PARTITION LIST '" + day + "'");
        drainWalQueue();
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveRefreshToQuiescence(job);
        }
        Assert.assertFalse(instance("lv").isCheckpointRecoveryBlocked());
        assertViewRows(expectedViewRows);
        assertNoRefreshFaults("lv");
    }

    /**
     * Commits {@link #ROWS_AHEAD} {@code first} to {@code first + 2}, which the base table does not
     * apply yet, and has the view fail mid-drain over them. A view over a base without dedup keys
     * drains the raw WAL, so it runs ahead of the base's own apply: the first commit gets a refresh
     * task of its own and lands in the view's un-flushed lead, the next two coalesce behind it, and
     * the fault fails that pass's read of the third commit after the second has been fed. The
     * recovery that follows owes the view its accumulators while the base has applied none of the
     * three.
     * <p>
     * The clock stays on the view's last flush, so the lead is not flushed; the caller drives
     * everything after the fault.
     */
    private void failMidDrainAheadOfTheBaseApply(LiveViewRefreshJob job, LiveViewMidDrainFault fault, int first) throws Exception {
        setCurrentMicros(instance("lv").getLastFlushTimeUs());
        for (int i = first; i < first + 3; i++) {
            execute("INSERT INTO tx VALUES " + ROWS_AHEAD[i]);
        }
        fault.arm(2);
        drainJob(job);
        Assert.assertTrue("the mid-drain segment read must have been failed exactly once", fault.hasFired());
    }

    /**
     * Commits {@link #ROWS_AHEAD}'s first row, which the base table does not apply, and has the view
     * drain it from raw WAL and flush it, with the clock past {@code FLUSH EVERY}. The view's table
     * then holds output of a commit the base has not applied, which is what puts the view's own
     * coordinate past the base's applied head.
     */
    private void flushAheadOfTheBaseApply(LiveViewRefreshJob job) throws Exception {
        final LiveViewInstance instance = instance("lv");
        final long baseApplied = instance.getLastProcessedSeqTxn();
        setCurrentMicros(instance.getLastFlushTimeUs() + CLOCK_ADVANCE_MICROS);
        execute("INSERT INTO tx VALUES " + ROWS_AHEAD[0]);
        drainJob(job);
        Assert.assertEquals(
                "the view must have flushed a commit the base has not applied",
                baseApplied + 1,
                instance.getLastProcessedSeqTxn()
        );
        Assert.assertEquals(baseApplied, engine.getTableSequencerAPI().getTxnTracker(engine.verifyTableName("tx")).getWriterTxn());
    }

    /**
     * Re-publishes the base's head commit and drives the refresh, which is what a later commit
     * notification looks like to the view: it drains from its lead up to the lost segment and fails
     * there. The fallback scan would not retry the drain, because it drives a view only as far as
     * the base has applied, and here the base's apply is at or behind the lead.
     */
    private void failDrainOnTheLostSegment(LiveViewRefreshJob job, long baseHead) {
        engine.getLiveViewStateStore().notifyBaseTableCommit(engine.verifyTableName("tx"), baseHead);
        drainJob(job);
    }

    /**
     * Commits a late day-three row, whose repair resumes from the checkpoint anchored at day
     * three's first row, and fails that replay where it stages its first re-versioned root: past
     * the anchor restore and the scan, ahead of the replacement commit. When {@code isCrash}, the
     * fault first copies the view's checkpoint directory, and the process stops right after the
     * turn the fault ended - that copy goes back before the restart, which is all a crash inside
     * the replay would have left there. Otherwise the view recovers in place and repairs the
     * correction on its next turn. Then restarts, twice.
     * <p>
     * Nothing durable moved before the fault: the replacement sat uncommitted in the view's WAL
     * writer, which rolled it back, no generation named the staged roots, and the correction
     * stayed unconsumed. So the timeline the view had before the correction still describes the
     * output on disk, and the in-place recovery and each restart must restore from it. A recovery
     * that rebuilt from the applied base instead would meet the day the base lost and stop the
     * view, in place and on every restart after it.
     */
    private void failResumeReplayAndRestart(StagedSegmentOpenFault fault, boolean isCrash) throws Exception {
        final LiveViewInstance instance = instance("lv");
        final long processedBefore = instance.getLastProcessedSeqTxn();
        execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-03T09:05:00.000000Z', 'acct-1', 64.0)");
        drainWalQueue();
        final boolean[] isMarkerOnDiskAtFault = {false};
        try (
                Path crashImage = new Path().of(engine.getConfiguration().getDbRoot()).concat(CRASH_IMAGE_DIR_NAME).slash();
                Path checkpoints = checkpointsDir(instance).slash()
        ) {
            final FilesFacade ff = engine.getConfiguration().getFilesFacade();
            fault.arm(() -> {
                isMarkerOnDiskAtFault[0] = LiveViewCheckpointRepairMarker.exists(ff, checkpoints);
                if (isCrash) {
                    TestUtils.copyDirectory(checkpoints, crashImage, engine.getConfiguration().getMkDirMode());
                }
            });
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                if (isCrash) {
                    for (int pass = 0; pass < REFRESH_QUIESCENCE_PASSES && !fault.hasFired(); pass++) {
                        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                        drainWalQueue();
                        job.processNotificationsForTest();
                    }
                } else {
                    driveRefreshToQuiescence(job);
                }
            }
            Assert.assertTrue("the replay must have reached its first staged root", fault.hasFired());
            capture.drain();
            capture.assertLogged("resumeFromAnchor=true");
            if (isCrash) {
                // The turn the fault ended committed nothing, so the directory copied inside it is
                // the whole durable state a crash at that point leaves.
                Assert.assertEquals(processedBefore, instance.getLastProcessedSeqTxn());
                assertViewRows(ALL_ROWS);
            } else {
                Assert.assertFalse("the view must keep refreshing", instance.isCheckpointRecoveryBlocked());
                capture.assertLogged("live view restored its runtime from the checkpoint timeline [view=lv, cause=mid-drain refresh failure");
                capture.assertNotLogged("live view rebuild from the applied base refused");
                Assert.assertEquals(1, instance.getCheckpointRuntimeRestores());
                Assert.assertEquals("the fault is the one fault", 1, instance.getRefreshFaultCount());
                Assert.assertEquals("the retry must consume the correction", processedBefore + 1, instance.getLastProcessedSeqTxn());
                Assert.assertTrue("the retry must resume from the anchor", instance.getO3ResumeReplayRows() > 0);
                Assert.assertFalse("the completed repair owes no marker", LiveViewCheckpointRepairMarker.exists(ff, checkpoints));
                assertViewRows(CORRECTED_DAY_THREE_ROWS);
            }
            shutdown();
            if (isCrash) {
                Assert.assertTrue(ff.rmdir(checkpoints));
                TestUtils.copyDirectory(crashImage, checkpoints, engine.getConfiguration().getMkDirMode());
                Assert.assertTrue(ff.rmdir(crashImage));
            }
        }

        restart();
        assertRestoredFromTimeline("lv");
        final LiveViewInstance restored = instance("lv");
        Assert.assertFalse("the view must keep refreshing", restored.isCheckpointRecoveryBlocked());
        Assert.assertEquals("the correction must be consumed", processedBefore + 1, restored.getLastProcessedSeqTxn());
        assertViewRows(CORRECTED_DAY_THREE_ROWS);
        assertNoRefreshFaults("lv");

        shutdown();
        restart();
        assertRestoredFromTimeline("lv");
        Assert.assertFalse(instance("lv").isCheckpointRecoveryBlocked());
        assertViewRows(CORRECTED_DAY_THREE_ROWS);
        assertNoRefreshFaults("lv");

        // What made both recoveries sound: the replay faulted ahead of its replacement commit, so
        // no repair marker stood over the timeline yet.
        Assert.assertFalse(
                "a replay that has not reached its replacement commit owes no repair marker",
                isMarkerOnDiskAtFault[0]
        );
    }

    private long newestGeneration(LiveViewInstance instance) {
        try (
                LiveViewCheckpointMetaStore store = openStore(instance);
                LiveViewCheckpointGenerationPin pin = store.pin()
        ) {
            return pin.getGeneration();
        }
    }

    /**
     * Seeds the view {@link #createFilteredView()} created, then commits {@code insert} to its
     * base and refreshes the view over it.
     */
    private void refreshIntoFilteredView(String insert) throws Exception {
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveSeedToCompletion(job, "lv");
            execute(insert);
            drainWalQueue();
            driveRefreshToQuiescence(job);
        }
        assertNoRefreshFaults("lv");
    }

    /**
     * Removes every WAL directory of the base table, which is what a restore that captured the
     * applied table and not its WAL leaves behind.
     */
    private void removeBaseWal(String baseTableName) {
        final TableToken baseToken = engine.verifyTableName(baseTableName);
        final File baseDir = new File(engine.getConfiguration().getDbRoot(), baseToken.getDirName());
        final File[] walDirs = baseDir.listFiles(f -> f.isDirectory() && f.getName().startsWith(WalUtils.WAL_NAME_BASE));
        Assert.assertNotNull(walDirs);
        Assert.assertTrue("the base must have a WAL to lose", walDirs.length > 0);
        for (File walDir : walDirs) {
            try (Path p = new Path()) {
                p.of(walDir.getAbsolutePath());
                Assert.assertTrue("could not remove " + walDir, engine.getConfiguration().getFilesFacade().rmdir(p));
            }
        }
    }

    /**
     * Removes the view's {@code _timeline}, leaving the segments under it for the catalogue
     * load's orphan sweep. The restart then finds no timeline and asks for the rebuild.
     */
    private void removeTimeline() {
        final File timeline = new File(checkpointsRootByDirName(), LiveViewCheckpointLayout.TIMELINE_FILE_NAME);
        Assert.assertTrue("the fixture must have published a timeline to remove", timeline.delete());
    }

    private int countBaseWalDirs(String baseTableName) {
        final TableToken baseToken = engine.verifyTableName(baseTableName);
        final File baseDir = new File(engine.getConfiguration().getDbRoot(), baseToken.getDirName());
        final File[] walDirs = baseDir.listFiles(f -> f.isDirectory() && f.getName().startsWith(WalUtils.WAL_NAME_BASE));
        Assert.assertNotNull(walDirs);
        return walDirs.length;
    }

    private File checkpointsRootByDirName() {
        final TableToken viewToken = engine.verifyTableName("lv");
        return new File(
                new File(engine.getConfiguration().getDbRoot(), viewToken.getDirName()),
                LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME
        );
    }

    /**
     * Commits one row to the base through a WAL of its own - the insert takes a second WAL writer
     * while the test holds the first - and then removes that WAL, so this commit alone is lost to
     * everything that would read it: the view's drain and the base's own apply. Every earlier
     * commit stays readable in the first WAL.
     */
    private void commitThroughASecondWalAndLoseIt(String values) throws Exception {
        final TableToken baseToken = engine.verifyTableName("tx");
        try (WalWriter held = engine.getWalWriter(baseToken)) {
            Assert.assertEquals("every earlier commit must sit in the first WAL", 1, held.getWalId());
            execute("INSERT INTO tx VALUES " + values);
        }
        engine.releaseInactive();
        final File secondWal = new File(
                new File(engine.getConfiguration().getDbRoot(), baseToken.getDirName()),
                WalUtils.WAL_NAME_BASE + 2
        );
        Assert.assertTrue("the insert must have taken a second WAL", secondWal.isDirectory());
        try (Path p = new Path()) {
            p.of(secondWal.getAbsolutePath());
            Assert.assertTrue("could not remove " + secondWal, engine.getConfiguration().getFilesFacade().rmdir(p));
        }
    }

    /**
     * Commits a late day-two row, drives the refresh until the repair it opens parks on its turn
     * budget, and stops the process under the parked repair: gracefully, the way the engine's
     * teardown discards it, or - when {@code isCrash} - with the view's checkpoint directory put
     * back exactly as the park left it, which is all a crash would have left there. Then restarts,
     * twice.
     * <p>
     * Nothing durable moved while the repair was parked: its replacement sat uncommitted in the
     * view's WAL writer, no generation named its staged roots, and the correction stayed
     * unconsumed. So the timeline the view had before the correction still describes the output
     * on disk, and each restart must restore from it - the first then repairs the correction
     * again. A restart that rebuilt from the applied base instead would meet the day the base lost
     * and stop the view, on that restart and on every one after it.
     */
    private void parkRepairAndRestart(boolean isCrash) throws Exception {
        // One base row per repair turn, so the repair over day two spends several turns and
        // parks between them.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        final LiveViewInstance parked = instance("lv");
        final long generationBefore = newestGeneration(parked);
        final long processedBefore = parked.getLastProcessedSeqTxn();
        execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-02T09:05:00.000000Z', 'acct-1', 64.0)");
        drainWalQueue();
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        final long generationWhileParked;
        final boolean isMarkerOnDiskWhileParked;
        try (
                Path crashImage = new Path().of(engine.getConfiguration().getDbRoot()).concat(CRASH_IMAGE_DIR_NAME).slash();
                Path checkpoints = checkpointsDir(parked).slash()
        ) {
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveUntilParked(job, "lv");
                generationWhileParked = newestGeneration(parked);
                isMarkerOnDiskWhileParked = LiveViewCheckpointRepairMarker.exists(ff, checkpoints);
                Assert.assertEquals(processedBefore, parked.getLastProcessedSeqTxn());
                assertViewRows(ALL_ROWS);
                if (isCrash) {
                    TestUtils.copyDirectory(checkpoints, crashImage, engine.getConfiguration().getMkDirMode());
                }
                // CairoEngine.close()'s order: the parked repair goes before the registry does.
                engine.getLiveViewRegistry().discardSuspendedRepairs();
                Assert.assertNull(parked.getSuspendedRepair());
            }
            shutdown();
            if (isCrash) {
                Assert.assertTrue(ff.rmdir(checkpoints));
                TestUtils.copyDirectory(crashImage, checkpoints, engine.getConfiguration().getMkDirMode());
                Assert.assertTrue(ff.rmdir(crashImage));
            }
        }

        restart();
        assertRestoredFromTimeline("lv");
        final LiveViewInstance restored = instance("lv");
        Assert.assertFalse("the view must keep refreshing", restored.isCheckpointRecoveryBlocked());
        Assert.assertEquals("the restart must consume the correction", processedBefore + 1, restored.getLastProcessedSeqTxn());
        assertViewRows(CORRECTED_ROWS);
        assertNoRefreshFaults("lv");

        shutdown();
        restart();
        assertRestoredFromTimeline("lv");
        Assert.assertFalse(instance("lv").isCheckpointRecoveryBlocked());
        assertViewRows(CORRECTED_ROWS);
        assertNoRefreshFaults("lv");

        // What made both recoveries sound: the parked repair had moved nothing durable, so the
        // restart found the timeline it had pinned intact and no repair marker over it.
        Assert.assertEquals("a parked repair must not publish a generation", generationBefore, generationWhileParked);
        Assert.assertFalse("a parked repair owes no repair marker", isMarkerOnDiskWhileParked);
    }

    /**
     * {@link #parkRepairAndRestart(boolean)} for a head miss that declines the checkpoint splice
     * and truncates the timeline at its output floor instead, which is what a repair crossing
     * more sealed boundaries than {@code cairo.live.view.checkpoint.repair.max.chained.boundaries}
     * allows does. A limit of 0 declines every splice, so the fixture's one-boundary repair takes
     * the same branch the default limit of 256 sends a deeper one down.
     */
    private void parkTruncatingRepairAndRestart(boolean isCrash) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 0);
        parkRepairAndRestart(isCrash);
        capture.drain();
        capture.assertLogged("live view O3 head miss declined the checkpoint splice, truncating instead [view=lv");
        capture.assertLogged("live view O3 repair yielded on its turn budget [view=lv");
        capture.assertNotLogged("live view rebuild from the applied base refused");
    }

    /**
     * Cancels the view's refresh and drives the turn that meets the commit the caller has just
     * made, so the first circuit-breaker check of that turn throws, the way an engine shutdown
     * trips the same breaker. Nothing consults the breaker ahead of the repair: the commit is out
     * of order, so the forward drain hands it to the repair before reading a row of it, and the
     * check that throws is the repair's own, past what its first turn settles before it replays.
     * The instance keeps the flag, so the caller stops the process next.
     */
    private void cancelRefreshTurn(LiveViewInstance instance) {
        instance.cancelRefresh();
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            for (int pass = 0; pass < REFRESH_QUIESCENCE_PASSES && instance.getRefreshFaultCount() == 0; pass++) {
                setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                drainWalQueue();
                job.processNotificationsForTest();
            }
        }
        Assert.assertEquals("the breaker must have ended the replay exactly once", 1, instance.getRefreshFaultCount());
    }

    /**
     * Commits a late day-two row to the view {@link #seedUnlocalizedView()} created. No sealed
     * boundary sits below it and the window has no finite dependency, so the repair replays the
     * whole surviving base from the view boundary in one turn it cannot yield. The view's refresh
     * is cancelled ahead of that turn, so the replay's own circuit-breaker check throws on its
     * first row - which is where an engine shutdown trips the same breaker - and the process then
     * stops. Then restarts, twice.
     * <p>
     * Nothing durable moved before the breaker tripped: the replacement sat uncommitted in the
     * view's WAL writer, which rolled it back, and the correction stayed unconsumed. So the
     * timeline the view had before the correction still describes the output on disk, and the
     * first restart must restore from it and repair the correction again. A restart that found no
     * timeline would rebuild from the applied base instead, meet the day the base lost and stop
     * the view, on that restart and on every one after it.
     */
    private void cancelUnlocalizedRepairAndRestart() throws Exception {
        final LiveViewInstance cancelled = instance("lv");
        final long generationBefore = newestGeneration(cancelled);
        final long processedBefore = cancelled.getLastProcessedSeqTxn();
        execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-02T09:05:00.000000Z', 'acct-1', 64.0)");
        drainWalQueue();
        cancelRefreshTurn(cancelled);
        capture.drain();
        capture.assertLogged("resumeFromAnchor=false");
        capture.assertLogged("live view refresh cancelled [view=lv");
        capture.assertNotLogged("live view O3 head-miss replay completed");
        Assert.assertEquals("a cancelled replay must not consume the correction", processedBefore, cancelled.getLastProcessedSeqTxn());
        assertViewRows(UNLOCALIZED_ROWS);
        final boolean isTimelineOnDiskAfterCancel = new File(checkpointsRootByDirName(), LiveViewCheckpointLayout.TIMELINE_FILE_NAME).exists();
        final long generationAfterCancel = isTimelineOnDiskAfterCancel ? newestGeneration(cancelled) : Numbers.LONG_NULL;
        shutdown();

        restart();
        final LiveViewInstance restored = instance("lv");
        Assert.assertEquals(
                "live view 'lv' took the wrong restart recovery route",
                "timeline_restore",
                LiveViewCheckpointRestoreRoute.name(restored.getCheckpointRestoreRoute())
        );
        Assert.assertEquals("the restart must restore rather than rebuild", 0, restored.getCheckpointRebuildAttempts());
        Assert.assertFalse("the view must keep refreshing", restored.isCheckpointRecoveryBlocked());
        Assert.assertEquals("the restart must consume the correction", processedBefore + 1, restored.getLastProcessedSeqTxn());
        // The repair the restart re-ran retires the timeline it restored from at its replacement
        // commit, and its head seal opens the history the second restart restores from.
        Assert.assertEquals(1, restored.getCheckpointTimelineResets());
        assertViewRows(UNLOCALIZED_CORRECTED_ROWS);
        assertNoRefreshFaults("lv");
        capture.drain();
        capture.assertLogged("live view O3 head-miss replay completed [view=lv");
        capture.assertLogged("localized=false");
        capture.assertNotLogged("live view rebuild from the applied base refused");

        shutdown();
        restart();
        assertRestoredFromTimeline("lv");
        Assert.assertFalse(instance("lv").isCheckpointRecoveryBlocked());
        assertViewRows(UNLOCALIZED_CORRECTED_ROWS);
        assertNoRefreshFaults("lv");

        // What made both recoveries sound: the cancelled replay had moved nothing durable, so the
        // restart found the timeline it replayed over intact.
        Assert.assertTrue("a cancelled replay must leave its timeline on disk", isTimelineOnDiskAfterCancel);
        Assert.assertEquals("a cancelled replay must not publish a generation", generationBefore, generationAfterCancel);
    }

    /**
     * Creates a base without dedup keys and a view over it filtering {@code i > 0}, partitioned by
     * {@code viewPartitionBy}, and refreshes {@link #FILTERED_LATER_DAYS_ROWS} into the view. The
     * base also holds a row on the first day the view does not: one the filter rejects, or, when
     * {@code isOlderRowBelowStartFrom}, one the base held before the view was created with a START
     * FROM past it. Either way the base's earliest row sits below the view's.
     */
    private void seedLaterDaysView(String viewPartitionBy, boolean isOlderRowBelowStartFrom) throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
        if (isOlderRowBelowStartFrom) {
            execute("INSERT INTO base (ts, sym, i) VALUES ('2026-01-01T00:01:00.000000Z', 'a', 50)");
            drainWalQueue();
            createBaseView(viewPartitionBy + " START FROM '2026-01-02T00:00:00.000000Z'", true);
            refreshIntoFilteredView(FILTERED_LATER_DAYS_ROWS_INSERT);
        } else {
            createBaseView(viewPartitionBy + " START FROM BEGINNING", true);
            refreshIntoFilteredView(FILTERED_LATER_DAYS_ROWS_INSERT + ",\n    ('2026-01-01T00:01:00.000000Z', 'a', -5)");
        }
        assertFilteredViewRows(FILTERED_LATER_DAYS_ROWS);
        assertQuery("SELECT min(ts), count() FROM base")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("""
                        min\tcount
                        2026-01-01T00:01:00.000000Z\t4
                        """);
    }

    /**
     * Creates the base with {@code tableOptions} and the view over it, filtering {@code i > 0}
     * when {@code isFiltered}, and refreshes {@link #FILTERED_TWO_DAY_ROWS} into the view while
     * {@code fault} fails every open of its {@code _timeline}: the view's table holds their output
     * and no checkpoint describes it.
     */
    private void seedTwoDayView(String tableOptions, boolean isFiltered, TimelineOpenFault fault) throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG) TIMESTAMP(ts) PARTITION BY DAY " + tableOptions);
        createBaseView(isFiltered);
        refreshIntoFilteredView(FILTERED_TWO_DAY_ROWS_INSERT);
        Assert.assertTrue("the view's timeline open must have failed", fault.getFailures() > 0);
        assertFilteredViewRows(FILTERED_TWO_DAY_ROWS);
    }

    /**
     * The fixture's six rows over three days in one commit, into the view
     * {@link #createUnlocalizedView()} creates. One commit seals one boundary, at the newest row,
     * so a late row anywhere below it finds no boundary to resume from.
     */
    private void seedUnlocalizedView() throws Exception {
        execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL, amount DOUBLE) "
                + "TIMESTAMP(created_at) PARTITION BY DAY WAL");
        createUnlocalizedView();
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveSeedToCompletion(job, "lv");
            execute("""
                    INSERT INTO tx (created_at, account_id, amount) VALUES
                        ('2026-01-01T09:00:00.000000Z', 'acct-1', 1.0),
                        ('2026-01-01T09:10:00.000000Z', 'acct-2', 2.0),
                        ('2026-01-02T09:00:00.000000Z', 'acct-1', 4.0),
                        ('2026-01-02T09:10:00.000000Z', 'acct-1', 8.0),
                        ('2026-01-03T09:00:00.000000Z', 'acct-1', 16.0),
                        ('2026-01-03T09:10:00.000000Z', 'acct-2', 32.0)""");
            drainWalQueue();
            driveRefreshToQuiescence(job);
        }
        assertViewRows(UNLOCALIZED_ROWS);
        assertNoRefreshFaults("lv");
        Assert.assertEquals("one boundary for the one commit", 1, countSealedBoundaries("lv"));
    }

    /**
     * Rebuilds the view registry from disk and drives the first refresh turns, which is where a
     * restart runs its recovery. Returns what the last whole-view rebuild's guard found.
     */
    private LiveViewRebuildRestatementGuard restart() {
        engine.buildViewGraphs();
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveRefreshToQuiescence(job);
            // A plain heap object the job keeps no native resource in, so it stays readable
            // after the job closes.
            return job.rebuildRestatementGuardForTest();
        }
    }

    /**
     * Six rows over three days, one commit each, so the timeline holds one boundary per row and
     * every base partition holds rows the view has derived output from.
     */
    private void seedSixRows(String dedupClause) throws Exception {
        seedSixRows(dedupClause, "");
    }

    /**
     * The same, into a view filtered by {@code whereClause}. Every fixture row passes the filters
     * the cases use, so the view holds {@link #ALL_ROWS} either way.
     */
    private void seedSixRows(String dedupClause, String whereClause) throws Exception {
        execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL, amount DOUBLE) "
                + "TIMESTAMP(created_at) PARTITION BY DAY WAL " + dedupClause);
        createView(whereClause);
        final String[] rows = {
                "'2026-01-01T09:00:00.000000Z', 'acct-1', 1.0",
                "'2026-01-01T09:10:00.000000Z', 'acct-2', 2.0",
                "'2026-01-02T09:00:00.000000Z', 'acct-1', 4.0",
                "'2026-01-02T09:10:00.000000Z', 'acct-1', 8.0",
                "'2026-01-03T09:00:00.000000Z', 'acct-1', 16.0",
                "'2026-01-03T09:10:00.000000Z', 'acct-2', 32.0"
        };
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveSeedToCompletion(job, "lv");
            for (String row : rows) {
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES (" + row + ")");
                drainWalQueue();
                driveRefreshToQuiescence(job);
            }
        }
        assertViewRows(ALL_ROWS);
        assertNoRefreshFaults("lv");
        Assert.assertEquals("one boundary per commit", 6, countSealedBoundaries("lv"));
    }

    /**
     * Releases everything that maps the view's and the base's files, the way a stopped process
     * would, so the next {@link #restart()} starts from disk.
     */
    private void shutdown() {
        engine.getLiveViewRegistry().clear();
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        engine.releaseInactive();
    }

    /**
     * Stamps the durable marker a prefix-preserving repair writes before it truncates, over the
     * generation on disk, so the restart reads it as a repair that crashed rather than one a
     * later seal made stale.
     */
    private void writeRepairMarker(LiveViewInstance instance) {
        try (Path dir = checkpointsDir(instance)) {
            LiveViewCheckpointRepairMarker.write(
                    engine.getConfiguration(),
                    dir,
                    instance.getLiveViewToken().getTableId(),
                    0,
                    newestGeneration(instance),
                    ts("2026-01-02T00:00:00.000000Z")
            );
        }
    }

    /**
     * A {@link TimelineOpenFault} that can also fail every read-only open of one table's
     * sequencer metadata change index, {@code _txnlog.meta.i}. Nothing else opens that file
     * read-only once the base has applied its backlog: the change log is what records which
     * structural change a sequencer entry made, so the fault leaves a rebuild unable to tell
     * whether one changed dedup.
     */
    private static final class DedupHistoryOpenFault extends TimelineOpenFault {
        private final AtomicInteger dedupHistoryFailures = new AtomicInteger();
        private volatile String dedupHistoryDirName;

        @Override
        public long openRO(LPSZ name) {
            if (isDedupHistory(name)) {
                dedupHistoryFailures.incrementAndGet();
                return -1;
            }
            return super.openRO(name);
        }

        @Override
        public long openRONoCache(LPSZ name) {
            if (isDedupHistory(name)) {
                dedupHistoryFailures.incrementAndGet();
                return -1;
            }
            return super.openRONoCache(name);
        }

        private boolean isDedupHistory(LPSZ name) {
            final String dirName = dedupHistoryDirName;
            return dirName != null
                    && Utf8s.containsAscii(name, Files.SEPARATOR + dirName + Files.SEPARATOR)
                    && Utf8s.endsWithAscii(name, WalUtils.TXNLOG_FILE_NAME_META_INX);
        }

        void armDedupHistory(TableToken tableToken) {
            dedupHistoryDirName = tableToken.getDirName();
        }

        void disarmDedupHistory() {
            dedupHistoryDirName = null;
        }

        int getDedupHistoryFailures() {
            return dedupHistoryFailures.get();
        }
    }

    /**
     * Fails the next open of a checkpoint data segment the view stages, after running the armed
     * action. A repair's replay opens its temporary segment when it freezes the first root it
     * re-versions: after the capture and the descriptor, and ahead of the replacement commit. No
     * cadence seal runs between arming and that open in the cases above.
     */
    private static final class StagedSegmentOpenFault extends TestFilesFacadeImpl {
        private final AtomicBoolean hasFired = new AtomicBoolean();
        private final AtomicBoolean isArmed = new AtomicBoolean();
        private volatile Runnable onFault;

        @Override
        public long openRW(LPSZ name, int opts) {
            if (isStagedSegment(name) && isArmed.compareAndSet(true, false)) {
                onFault.run();
                hasFired.set(true);
                return -1;
            }
            return super.openRW(name, opts);
        }

        void arm(Runnable onFault) {
            this.onFault = onFault;
            hasFired.set(false);
            isArmed.set(true);
        }

        boolean hasFired() {
            return hasFired.get();
        }
    }

    /**
     * Fails every open of a live view's {@code _timeline} until disarmed. A seal that cannot open
     * the timeline leaves the output it follows durable and undescribed: the next restart finds no
     * timeline and rebuilds the view from its base.
     */
    private static class TimelineOpenFault extends TestFilesFacadeImpl {
        private final AtomicInteger failures = new AtomicInteger();
        private final AtomicBoolean isArmed = new AtomicBoolean(true);

        @Override
        public long openRW(LPSZ name, int opts) {
            if (isArmed.get() && Utf8s.endsWithAscii(name, LiveViewCheckpointLayout.TIMELINE_FILE_NAME)) {
                failures.incrementAndGet();
                return -1;
            }
            return super.openRW(name, opts);
        }

        void disarm() {
            isArmed.set(false);
        }

        int getFailures() {
            return failures.get();
        }
    }
}
