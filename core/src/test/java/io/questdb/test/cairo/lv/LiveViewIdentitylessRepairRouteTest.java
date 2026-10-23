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
import io.questdb.cairo.lv.LiveViewCheckpointOutputUniqueness;
import io.questdb.cairo.lv.LiveViewCheckpointRepairPlan;
import io.questdb.cairo.lv.LiveViewCheckpointRepairRoute;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewRefreshStats;
import io.questdb.griffin.SqlException;
import io.questdb.std.Numbers;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * How a view WITHOUT the (timestamp, key) upsert identity repairs out-of-order data that
 * the change-set decomposition can scope.
 * <p>
 * Such a view can never take the open segment's keyed resume, which publishes on that
 * identity, so nothing it repairs may pay for the key domain that route follows. The
 * decomposition does more than feed that route, though. For a correction inside the open
 * segment it also places the rows of the commits apply raced past it in their own closed
 * segments, drops the rows below the view's {@code START FROM} boundary, and hands the
 * residual repair a classified floor instead of the raw trigger. Every view the released
 * 10.0.x builds created is without the identity, and so is every view created with sparse
 * publication off or with a key the identity cannot name. These cases pin that such a view
 * keeps those three on the key shapes named below: each one repairs the corrected closed
 * segment alone, or resumes inside the open segment, instead of rebuilding from the
 * corrected day or from {@code START FROM} through the end of the table.
 * <p>
 * The raced closed row pins the placement and the classified floor on three key shapes: an
 * indexed SYMBOL key the view projects, a key the view does not project, and the unindexed
 * key of a view the 10.0.1 build created. Only the first shape pins the drop below
 * {@code START FROM} - every case that creates its view with a {@code START FROM} uses it -
 * and only that shape passes the keyed-scan gate. On these unfiltered views over a base
 * that does not deduplicate, a walk over a key that passes the gate reads the rows of a
 * commit lying wholly inside the open segment when that commit reaches below the boundary,
 * and drops the ones under it. A view with either of the other two keys folds such a commit
 * whole, from its own minimum, so a row of it below a {@code START FROM} inside the open
 * segment still sets the residual floor. No case here pins that path.
 * <p>
 * Each case reports one repair as a single line - segment repairs, the route deltas, the
 * base rows the repair scanned and the output rows it re-emitted at a segment boundary and
 * on a resume, and the plan's disposition and denial - and then checks the view against a
 * from-base recompute. The fixture holds 40 base rows per day: ten hourly commits of four
 * accounts each.
 */
public class LiveViewIdentitylessRepairRouteTest extends AbstractLiveViewCheckpointCompatTest {
    // The simulated clock the 10.0.1 fixture's own run left behind; the upgraded case starts
    // above it, so the flush cadence reads a forward-moving clock.
    private static final long FIXTURE_END_MICROS = 2_500_000L;
    private static final String FIXTURE_RESOURCE = "/lv/lv_checkpoint_10_0_1.zip";
    private static final long HOUR_MICROS = 3_600_000_000L;
    private static final String KEY_NOT_PROJECTED = "key_not_projected";
    // What the decomposition read for a view without the identity when the only commit
    // reaching below the open segment is the raced one: that commit's single row, and no
    // pricing of a keyed resume the view cannot take.
    private static final String RACED_ROW_WALKED_ALONE = "walkedRows=1|openSegmentPriced=0";
    // A repair of the closed segment the raced commit reached, then a resume of the open
    // segment above its newest root: one closed day plus the open day's tail, however many
    // days of history sit between them.
    private static final String SEGMENT_AND_RESUME = "segmentRepairs=1|routes=segment rebuild+1,resume from anchor+1"
            + "|baseRowsScanned=74|boundaryRowsEmitted=26|resumeRowsEmitted=33"
            + "|lastDisposition=resume from anchor|lastDenial=resume cheaper";
    // Positions in a snapshot(): the repair's own counters, then one route delta each.
    private static final int SNAPSHOT_BOUNDARY_ROWS = 1;
    private static final int SNAPSHOT_OPEN_SEGMENT_PRICED = 5;
    private static final int SNAPSHOT_RESUME_ROWS = 2;
    private static final int SNAPSHOT_ROUTES = 6;
    private static final int SNAPSHOT_SCAN_ROWS = 0;
    private static final int SNAPSHOT_SEGMENT_REPAIRS = 3;
    private static final int SNAPSHOT_WALKED_ROWS = 4;
    private static final String SPARSE_DISABLED = "sparse_disabled";
    private static final String VIEW_FLOOR = "2026-01-02T00:00:00.000000Z";
    private static final String WITH_IDENTITY = "identity";

    @After
    public void resetClock() {
        setCurrentMicros(-1);
    }

    @Test
    public void testAClosedRowARacedCommitCarriesIsRepairedInItsOwnSegmentWithTheIdentity() throws Exception {
        // The control: a view that carries the identity takes the same route. Its walk reads
        // the open segment's commit too, for the key domain it prices a keyed resume on.
        runRacedClosedRow(WITH_IDENTITY, 3, "walkedRows=2|openSegmentPriced=1");
    }

    @Test
    public void testAClosedRowARacedCommitCarriesIsRepairedInItsOwnSegmentWithoutTheIdentity() throws Exception {
        runRacedClosedRow(SPARSE_DISABLED, 3, RACED_ROW_WALKED_ALONE);
    }

    @Test
    public void testAClosedRowARacedCommitCarriesIsRepairedInItsOwnSegmentWithoutTheIdentityOnALongerHistory() throws Exception {
        // Four more days between the corrected one and the open one cost nothing more.
        runRacedClosedRow(SPARSE_DISABLED, 7, RACED_ROW_WALKED_ALONE);
    }

    @Test
    public void testAClosedRowARacedCommitCarriesIsRepairedInItsOwnSegmentWithoutTheIdentityOrAProjectedKey() throws Exception {
        runRacedClosedRow(KEY_NOT_PROJECTED, 3, RACED_ROW_WALKED_ALONE);
    }

    @Test
    public void testAClosedRowARacedCommitCarriesIsRepairedInItsOwnSegmentOnAViewTheReleasedBuildCreated() throws Exception {
        // The customer's population: a view 10.0.1 created carries no identity, before or
        // after the upgrade rebuild. Its key is unindexed, so the walk reads no key at all.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setCurrentMicros(2 * FIXTURE_END_MICROS);
        assertMemoryLeak(() -> {
            replaceDbContent(FIXTURE_RESOURCE);
            engine.buildViewGraphs();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                execute("INSERT INTO tx VALUES " + days(2, 4));
                drainWalQueue();
                driveRefreshToQuiescence(job);
                openDay(5, job, false);
                assertIdentity(false);
                assertRacedClosedRowRepair(job, 5, null, true, RACED_ROW_WALKED_ALONE);
            }
        });
    }

    @Test
    public void testARowBelowTheViewFloorInsideTheOpenSegmentIsDroppedWithTheIdentity() throws Exception {
        // The control: a view that carries the identity reads every row of the open
        // segment's commits for its key domain, and that row walk is what drops the row
        // under the boundary. Whatever route its pricing then picks, it plans from the same
        // floor as the view without the identity.
        runRowBelowTheViewFloorInsideTheOpenSegment(WITH_IDENTITY);
    }

    @Test
    public void testARowBelowTheViewFloorInsideTheOpenSegmentIsDroppedWithoutTheIdentity() throws Exception {
        runRowBelowTheViewFloorInsideTheOpenSegment(SPARSE_DISABLED);
    }

    @Test
    public void testASubFloorRowARacedCommitCarriesDoesNotRebuildTheViewWithoutTheIdentity() throws Exception {
        runRacedSubFloorRow(3);
    }

    @Test
    public void testASubFloorRowARacedCommitCarriesDoesNotRebuildTheViewWithoutTheIdentityOnALongerHistory() throws Exception {
        runRacedSubFloorRow(7);
    }

    @Test
    public void testASubFloorRowBehindAnUnflushedLeadDoesNotRebuildTheViewWithoutTheIdentity() throws Exception {
        // An un-flushed lead holds the newest in-order row, so the durable output sits below
        // the runtime frontier and no closed segment may be repaired on its own. The trigger
        // carries a row under START FROM and one inside the open day: the decomposition drops
        // the first, and the repair resumes inside the open day.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "false");
        assertMemoryLeak(() -> {
            seedBase(true, 3);
            createView(SPARSE_DISABLED, "1h", VIEW_FLOOR);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                openDay(5, job, true);
                execute("INSERT INTO tx VALUES " + row(5, 10, 10, "acct-1"));
                drainWalQueue();
                driveRefreshToQuiescence(job);
                assertIdentity(false);
                Assert.assertEquals("the lead must still be un-flushed", 1, viewInstance().getLeadRowCount());
                final long[] before = snapshot(job);

                execute("INSERT INTO tx VALUES " + row(1, 3, 35, "acct-1") + ", " + row(5, 2, 35, "acct-1"));
                drainWalQueue();
                driveRefreshToQuiescence(job);

                Assert.assertEquals(
                        "segmentRepairs=0|routes=resume from anchor+1|baseRowsScanned=34|boundaryRowsEmitted=0"
                                + "|resumeRowsEmitted=34|lastDisposition=resume from anchor|lastDenial=unflushed output",
                        observe(job, before)
                );
                // The trigger reaches below the open segment, so its rows are read; the lead's
                // commit sits wholly inside it and folds whole.
                Assert.assertEquals("walkedRows=2|openSegmentPriced=0", observeWalk(job, before));
                // Make the lead durable before comparing against the recompute.
                setCurrentMicros(currentMicros + HOUR_MICROS);
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(VIEW_FLOOR, true);
            }
        });
    }

    @Test
    public void testASubFloorRowBehindInOrderRowsOfTheSamePassDoesNotRebuildTheViewWithoutTheIdentity() throws Exception {
        // The drain feeds an in-order commit through the window functions and then meets the
        // trigger in the same pass, while the default checkpoint cadence leaves the newest
        // root below the frontier, so the window state is dirty and no closed segment may be
        // repaired on its own. The decomposition still drops the trigger's row under START
        // FROM, and the repair rebuilds the open day alone. The two commits land while a turn
        // runs, as a writer's do, so the pass after it reads them together.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "false");
        assertMemoryLeak(() -> {
            seedBase(true, 3);
            createView(SPARSE_DISABLED, "100ms", VIEW_FLOOR);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                openDay(5, job, false);
                assertIdentity(false);
                final long[][] before = {null};
                hooks(job).setSimulatePeerTurnBeforeRefreshLatch(() -> {
                    try {
                        execute("INSERT INTO tx VALUES " + row(5, 10, 10, "acct-1"));
                        execute("INSERT INTO tx VALUES " + row(1, 3, 35, "acct-1") + ", " + row(5, 2, 35, "acct-1"));
                    } catch (SqlException e) {
                        throw new RuntimeException(e);
                    }
                    hooks(job).setSimulatePeerTurnBeforeRefreshLatch(() -> {
                        drainWalQueue();
                        before[0] = snapshot(job);
                    });
                });

                // An in-order commit whose turn lets the writer land the two commits.
                execute("INSERT INTO tx VALUES " + row(5, 9, 50, "acct-1"));
                drainWalQueue();
                driveRefreshToQuiescence(job);

                Assert.assertNotNull("the pass that reads both commits never ran", before[0]);
                Assert.assertEquals(
                        "segmentRepairs=0|routes=range rebuild+1|baseRowsScanned=43|boundaryRowsEmitted=32"
                                + "|resumeRowsEmitted=0|lastDisposition=localized rebuild|lastDenial=null",
                        observe(job, before[0])
                );
                Assert.assertEquals("walkedRows=2|openSegmentPriced=0", observeWalk(job, before[0]));
                assertViewMatchesRecompute(VIEW_FLOOR, true);
            }
        });
    }

    @Test
    public void testASubFloorRowInACleanStateResumesWithoutTheIdentity() throws Exception {
        // The control: with nothing denying the closed-segment loop, the trigger that carries
        // a row under START FROM resumes the open day.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "false");
        assertMemoryLeak(() -> {
            seedBase(true, 3);
            createView(SPARSE_DISABLED, "100ms", VIEW_FLOOR);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                openDay(5, job, false);
                assertIdentity(false);
                final long[] before = snapshot(job);

                execute("INSERT INTO tx VALUES " + row(1, 3, 35, "acct-1") + ", " + row(5, 2, 35, "acct-1"));
                drainWalQueue();
                driveRefreshToQuiescence(job);

                Assert.assertEquals(
                        "segmentRepairs=0|routes=resume from anchor+1|baseRowsScanned=33|boundaryRowsEmitted=0"
                                + "|resumeRowsEmitted=33|lastDisposition=resume from anchor|lastDenial=resume cheaper",
                        observe(job, before)
                );
                Assert.assertEquals("walkedRows=2|openSegmentPriced=0", observeWalk(job, before));
                assertViewMatchesRecompute(VIEW_FLOOR, true);
            }
        });
    }

    private static String days(int firstDay, int lastDay) {
        final StringBuilder rows = new StringBuilder();
        for (int day = firstDay; day <= lastDay; day++) {
            for (int hour = 0; hour < 10; hour++) {
                for (int account = 1; account <= 4; account++) {
                    if (!rows.isEmpty()) {
                        rows.append(", ");
                    }
                    rows.append(row(day, hour, account * 10, "acct-" + account));
                }
            }
        }
        return rows.toString();
    }

    private static String row(int day, int hour, int minute, String account) {
        return "('2026-01-" + String.format("%02d", day) + "T" + String.format("%02d", hour)
                + ":" + String.format("%02d", minute) + ":00.000000Z', '" + account + "', 1.0)";
    }

    private void assertIdentity(boolean hasIdentity) {
        final LiveViewInstance instance = viewInstance();
        final int outputKeyColumnIndex = LiveViewCheckpointOutputUniqueness.outputKeyColumnIndex(instance.getCompiledPlan());
        Assert.assertEquals(
                "the fixture must " + (hasIdentity ? "carry" : "lack") + " the upsert identity",
                hasIdentity,
                instance.isDedupKeyed() && instance.getDedupKeyColumnIndex() == outputKeyColumnIndex
        );
        Assert.assertNull("the view must be unfiltered", instance.getCompiledPlan().getFilter());
    }

    /**
     * The trigger lies wholly inside the open day, below the frontier, and the commit apply
     * raced past it carries one row in the oldest closed day.
     */
    private void assertRacedClosedRowRepair(
            LiveViewRefreshJob job,
            int openDay,
            String startFrom,
            boolean isKeyProjected,
            String expectedWalk
    ) throws Exception {
        final long[] before = snapshot(job);

        execute("INSERT INTO tx VALUES " + row(openDay, 2, 35, "acct-1"));
        execute("INSERT INTO tx VALUES " + row(2, 3, 35, "acct-2"));
        drainWalQueue();
        driveRefreshToQuiescence(job);

        Assert.assertEquals(SEGMENT_AND_RESUME, observe(job, before));
        Assert.assertEquals(expectedWalk, observeWalk(job, before));
        assertViewMatchesRecompute(startFrom, isKeyProjected);
    }

    private void assertViewMatchesRecompute(String startFrom, boolean isKeyProjected) throws Exception {
        final String bucket = "timestamp_floor('1d', created_at, '1970-01-01T00:00:00.000000Z'::timestamp)";
        final String source = startFrom == null
                ? "tx"
                : "(SELECT * FROM tx WHERE created_at >= '" + startFrom + "'::timestamp)";
        final String recompute = "SELECT created_at, " + (isKeyProjected ? "account_id, " : "")
                + "sum(amount) OVER (PARTITION BY account_id, bucket ORDER BY created_at "
                + "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sum, "
                + "count(" + (isKeyProjected ? "account_id" : "amount") + ") OVER (PARTITION BY account_id, bucket ORDER BY created_at "
                + "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_count "
                + "FROM (SELECT created_at, account_id, amount, " + bucket + " AS bucket FROM " + source + ")";
        final String order = isKeyProjected ? " ORDER BY 2, 1" : " ORDER BY 1, 2";
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + recompute + ")" + order,
                "(lv)" + order,
                LOG,
                true
        );
        assertNoRefreshFaults("lv");
    }

    private void createView(String kind, String flushEvery, String startFrom) throws SqlException {
        final String projection = KEY_NOT_PROJECTED.equals(kind)
                ? "SELECT created_at, sum(amount) OVER w AS cumulative_sum, count(amount) OVER w AS cumulative_count "
                : "SELECT created_at, account_id, sum(amount) OVER w AS cumulative_sum, count(account_id) OVER w AS cumulative_count ";
        execute("CREATE LIVE VIEW lv FLUSH EVERY " + flushEvery + " START FROM "
                + (startFrom == null ? "BEGINNING" : "'" + startFrom + "'") + " AS "
                + projection
                + "FROM tx WINDOW w AS (PARTITION BY account_id ORDER BY created_at ANCHOR DAILY '00:00')");
    }

    private String observe(LiveViewRefreshJob job, long[] before) {
        final long[] after = snapshot(job);
        final StringBuilder routes = new StringBuilder();
        for (int route = 0; route < LiveViewCheckpointRepairRoute.ROUTE_COUNT; route++) {
            final long delta = after[SNAPSHOT_ROUTES + route] - before[SNAPSHOT_ROUTES + route];
            if (delta != 0) {
                if (!routes.isEmpty()) {
                    routes.append(',');
                }
                routes.append(LiveViewCheckpointRepairRoute.routeName(route)).append('+').append(delta);
            }
        }
        final LiveViewInstance instance = viewInstance();
        final int denial = instance.getCheckpointRepairLastDenialReason();
        return "segmentRepairs=" + (after[SNAPSHOT_SEGMENT_REPAIRS] - before[SNAPSHOT_SEGMENT_REPAIRS])
                + "|routes=" + routes
                + "|baseRowsScanned=" + (after[SNAPSHOT_SCAN_ROWS] - before[SNAPSHOT_SCAN_ROWS])
                + "|boundaryRowsEmitted=" + (after[SNAPSHOT_BOUNDARY_ROWS] - before[SNAPSHOT_BOUNDARY_ROWS])
                + "|resumeRowsEmitted=" + (after[SNAPSHOT_RESUME_ROWS] - before[SNAPSHOT_RESUME_ROWS])
                + "|lastDisposition=" + LiveViewCheckpointRepairPlan.dispositionName(instance.getCheckpointRepairLastDisposition(), denial)
                + "|lastDenial=" + LiveViewCheckpointRepairPlan.denialReasonName(denial);
    }

    /**
     * What the repair's decomposition read: the WAL rows its walk visited one by one, and
     * how many times it priced a keyed resume of the open segment, warm or cold.
     */
    private String observeWalk(LiveViewRefreshJob job, long[] before) {
        final long[] after = snapshot(job);
        return "walkedRows=" + (after[SNAPSHOT_WALKED_ROWS] - before[SNAPSHOT_WALKED_ROWS])
                + "|openSegmentPriced=" + (after[SNAPSHOT_OPEN_SEGMENT_PRICED] - before[SNAPSHOT_OPEN_SEGMENT_PRICED]);
    }

    private void openDay(int day, LiveViewRefreshJob job, boolean isHourlyFlush) throws SqlException {
        for (int hour = 0; hour < 10; hour++) {
            final StringBuilder rows = new StringBuilder();
            for (int account = 1; account <= 4; account++) {
                if (!rows.isEmpty()) {
                    rows.append(", ");
                }
                rows.append(row(day, hour, account * 10, "acct-" + account));
            }
            execute("INSERT INTO tx VALUES " + rows);
            drainWalQueue();
            if (isHourlyFlush) {
                setCurrentMicros(currentMicros + HOUR_MICROS);
            }
            driveRefreshToQuiescence(job);
        }
    }

    private void runRacedClosedRow(String kind, int closedDays, String expectedWalk) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        if (!WITH_IDENTITY.equals(kind)) {
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "false");
        }
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL NOCACHE INDEX CAPACITY 4, amount DOUBLE) "
                    + "TIMESTAMP(created_at) PARTITION BY HOUR WAL");
            execute("INSERT INTO tx VALUES " + days(2, 1 + closedDays));
            drainWalQueue();
            createView(kind, "100ms", null);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                final int openDay = 2 + closedDays;
                openDay(openDay, job, false);
                assertIdentity(WITH_IDENTITY.equals(kind));
                assertRacedClosedRowRepair(job, openDay, null, !KEY_NOT_PROJECTED.equals(kind), expectedWalk);
            }
        });
    }

    /**
     * The view is younger than its first anchor segment: START FROM sits inside the open
     * day, so a commit can reach below the view's boundary without leaving the open
     * segment. The row under the boundary produces no output, and the repair has to plan
     * from the lowest row above it, as the row walk that collects a key domain does - not
     * from the commit's own minimum, which would land the correction on START FROM and
     * rebuild the whole view.
     */
    private void runRowBelowTheViewFloorInsideTheOpenSegment(String kind) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        final boolean hasIdentity = WITH_IDENTITY.equals(kind);
        if (!hasIdentity) {
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "false");
        }
        final String viewFloor = "2026-01-05T05:00:00.000000Z";
        assertMemoryLeak(() -> {
            seedBase(false, 3);
            createView(kind, "100ms", viewFloor);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                openDay(5, job, false);
                assertIdentity(hasIdentity);
                final long[] residualFloor = {Numbers.LONG_NULL};
                hooks(job).setSimulateResumeReplayStart(
                        () -> residualFloor[0] = job.segmentChangeSetForTest().getResidualMinTs()
                );
                final long[] before = snapshot(job);

                execute("INSERT INTO tx VALUES " + row(5, 3, 35, "acct-1") + ", " + row(5, 6, 35, "acct-1"));
                drainWalQueue();
                driveRefreshToQuiescence(job);

                Assert.assertEquals(
                        "the residual must start at the lowest row above the view's boundary",
                        ts("2026-01-05T06:35:00.000000Z"),
                        residualFloor[0]
                );
                if (!hasIdentity) {
                    Assert.assertEquals(
                            "segmentRepairs=0|routes=resume from anchor+1|baseRowsScanned=17|boundaryRowsEmitted=0"
                                    + "|resumeRowsEmitted=17|lastDisposition=resume from anchor|lastDenial=resume cheaper",
                            observe(job, before)
                    );
                    // Both of the commit's rows are read, for their timestamps, and nothing else.
                    Assert.assertEquals("walkedRows=2|openSegmentPriced=0", observeWalk(job, before));
                }
                assertViewMatchesRecompute(viewFloor, true);
            }
        });
    }

    /**
     * The trigger lies wholly inside the open day, and the commit apply raced past it
     * carries one row under START FROM and nothing else.
     */
    private void runRacedSubFloorRow(int closedDays) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "false");
        assertMemoryLeak(() -> {
            seedBase(true, closedDays);
            createView(SPARSE_DISABLED, "100ms", VIEW_FLOOR);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                final int openDay = 2 + closedDays;
                openDay(openDay, job, false);
                assertIdentity(false);
                final long[] before = snapshot(job);

                execute("INSERT INTO tx VALUES " + row(openDay, 2, 35, "acct-1"));
                execute("INSERT INTO tx VALUES " + row(1, 3, 35, "acct-2"));
                drainWalQueue();
                driveRefreshToQuiescence(job);

                Assert.assertEquals(
                        "segmentRepairs=0|routes=resume from anchor+1|baseRowsScanned=33|boundaryRowsEmitted=0"
                                + "|resumeRowsEmitted=33|lastDisposition=resume from anchor|lastDenial=resume cheaper",
                        observe(job, before)
                );
                // The trigger sits wholly inside the open segment and folds whole, and the raced
                // commit lies wholly under START FROM, so no row is read at all.
                Assert.assertEquals("walkedRows=0|openSegmentPriced=0", observeWalk(job, before));
                assertViewMatchesRecompute(VIEW_FLOOR, true);
            }
        });
    }

    /**
     * An indexed SYMBOL key over closed days 2 through {@code 1 + closedDays}, plus a day
     * below the view's floor when {@code isSubFloorDaySeeded} holds.
     */
    private void seedBase(boolean isSubFloorDaySeeded, int closedDays) throws SqlException {
        execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL NOCACHE INDEX CAPACITY 4, amount DOUBLE) "
                + "TIMESTAMP(created_at) PARTITION BY HOUR WAL");
        execute("INSERT INTO tx VALUES " + days(isSubFloorDaySeeded ? 1 : 2, 1 + closedDays));
        drainWalQueue();
    }

    private long[] snapshot(LiveViewRefreshJob job) {
        final LiveViewInstance instance = viewInstance();
        final LiveViewRefreshStats stats = job.getStatsForTest();
        final long[] counts = new long[SNAPSHOT_ROUTES + LiveViewCheckpointRepairRoute.ROUTE_COUNT];
        counts[SNAPSHOT_SCAN_ROWS] = instance.getO3ReplayScanRows();
        counts[SNAPSHOT_BOUNDARY_ROWS] = instance.getO3BoundaryReplayRows();
        counts[SNAPSHOT_RESUME_ROWS] = instance.getO3ResumeReplayRows();
        counts[SNAPSHOT_SEGMENT_REPAIRS] = stats.get(LiveViewRefreshStats.SEGMENT_REPAIR_COUNT);
        counts[SNAPSHOT_WALKED_ROWS] = stats.get(LiveViewRefreshStats.SEGMENT_CLASSIFY_WALKED_ROWS);
        counts[SNAPSHOT_OPEN_SEGMENT_PRICED] = stats.get(LiveViewRefreshStats.OPEN_SEGMENT_KEYED_PRICED_COUNT)
                + stats.get(LiveViewRefreshStats.OPEN_SEGMENT_COLD_KEYED_PRICED_COUNT);
        for (int route = 0; route < LiveViewCheckpointRepairRoute.ROUTE_COUNT; route++) {
            counts[SNAPSHOT_ROUTES + route] = stats.getRouteCount(route);
        }
        return counts;
    }

    private LiveViewInstance viewInstance() {
        return instance("lv");
    }
}
