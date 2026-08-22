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
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Coverage for the repair of a correction that lands in the <b>open</b> anchor segment -
 * the one the runtime is still standing in, which the resume repairs by replaying every
 * base row above its anchor.
 * <p>
 * That resume is where the reported workload's volume is: under a daily anchor almost every
 * late commit is shallower than a day, so it lands in the open segment, and the per-segment
 * scoping, the keyed replay and the sparse publication all decline by construction. What
 * these cases pin is the route that reaches it - the open segment's own key domain, and the
 * pricing that decides whether following those keys reads less than reading every row above
 * the anchor.
 */
public class LiveViewOpenSegmentKeyedReplayTest extends AbstractLiveViewTest {

    @Test
    public void testACorrectionInTheOpenSegmentCollectsItsKeysAndPricesThem() throws Exception {
        // The measurement the route rests on: one account corrected inside the open day,
        // against a resume that reads every account's rows from its anchor to the end of the
        // base table.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        // Even the arithmetic path's measured setup price is larger than this tiny
        // fixture's whole range. Price the setup at one row so the case can exercise the
        // route; reported-density coverage validates the production crossover.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverTwoDays(), true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                openTheDayAboveARoot(job);
                Assert.assertEquals(0, job.openSegmentKeyedPricedCountForTest());

                // Below the frontier and inside the open day, and above the root the two
                // in-order rows above sealed - which is the shape that takes the anchor
                // resume and denies every route built for a closed segment.
                commit(row(4, 2, 35, "acct-1"), job);

                Assert.assertEquals(
                        "the open segment's resume must be priced exactly once",
                        1,
                        job.openSegmentKeyedPricedCountForTest()
                );
                Assert.assertEquals(0, job.openSegmentKeyedUnpricedCountForTest());
                Assert.assertEquals(
                        "one account of four is less to read than every row above the anchor",
                        1,
                        job.openSegmentKeyedCheaperCountForTest()
                );
                Assert.assertTrue(
                        "the keyed scan must read fewer rows than the whole range: posting="
                                + job.openSegmentKeyedPostingRowsForTest()
                                + " whole=" + job.openSegmentKeyedWholeRangeRowsForTest(),
                        job.openSegmentKeyedPostingRowsForTest() < job.openSegmentKeyedWholeRangeRowsForTest()
                );
                Assert.assertEquals(
                        "no closed segment was touched, so none may be repaired",
                        0,
                        job.segmentRepairCountForTest()
                );
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testACorrectionInTheOpenSegmentIsResumedByKeyAndPublishedSparsely() throws Exception {
        // The route end to end: the resume follows the corrected account through the base's
        // posting index, leaves every other account's stored rows exactly where they stand,
        // and the view still matches a from-base recompute afterwards.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        // The identity the publication upserts on. It is a CREATE-time schema property, so
        // it has to be on before the view exists.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "true");
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverTwoDays(), true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                openTheDayAboveARoot(job);

                commit(row(4, 2, 35, "acct-1"), job);

                Assert.assertEquals(
                        "the resume must follow the correction's own keys",
                        1,
                        job.openSegmentKeyedResumeCountForTest()
                );
                Assert.assertEquals(
                        "and publish only the rows it recomputed",
                        1,
                        job.openSegmentSparseResumeCountForTest()
                );
                Assert.assertEquals(
                        "checkpoint positions must come from the exact insert delta, not a stored-row scan",
                        1,
                        job.openSegmentArithmeticRowPositionCountForTest()
                );
                Assert.assertEquals(
                        "nothing may abandon its attempt on output that names each pair once",
                        0,
                        job.sparsePublicationFallbackCountForTest()
                );
                Assert.assertTrue(
                        "the publication must have left the other accounts' rows alone",
                        job.sparsePublicationRowsKeptForTest() > 0
                );
                Assert.assertTrue(
                        "the corrected keys must be handed back to the primary runtime",
                        job.transplantedKeyCountForTest() > 0
                );
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAKeyedResumeSurvivesARestartAndAFurtherCorrection() throws Exception {
        // The ladder a keyed resume leaves has to be restorable: its roots hold the
        // corrected keys' state and every other key's entry exactly as the old root wrote
        // it, and the row positions count the rows the publication left alone as well as
        // the ones it wrote. A restart is what reads all of that back.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "true");
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverTwoDays(), true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                openTheDayAboveARoot(job);
                commit(row(4, 2, 35, "acct-1"), job);
                Assert.assertEquals(1, job.openSegmentSparseResumeCountForTest());
                assertViewMatchesRecompute();
            }
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute();
                // A second correction, now against the ladder the first one spliced.
                commit(row(4, 3, 15, "acct-4"), job);
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAViewWithoutTheDedupKeysNeverResumesByKey() throws Exception {
        // The publication is an upsert on the view's own identity, so a view CREATEd
        // without it has nothing to upsert onto - and the block would otherwise have to
        // carry every stored row above the anchor, which is the whole range and no saving.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "false");
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverTwoDays(), true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                openTheDayAboveARoot(job);

                commit(row(4, 2, 35, "acct-1"), job);

                Assert.assertEquals(0, job.openSegmentKeyedResumeCountForTest());
                Assert.assertEquals(0, job.openSegmentSparseResumeCountForTest());
                // The pricing still runs and still says the keyed read is smaller, which is
                // what says the identity is what turned the route down.
                Assert.assertEquals(1, job.openSegmentKeyedCheaperCountForTest());
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testTheOpenSegmentIsNotPricedWithTheRouteDeclined() throws Exception {
        // The switch is what decides whether the decomposition walks every commit's rows at
        // all, so a declined route must leave the resume reading exactly what it always did.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_OPEN_SEGMENT_KEYED_REPLAY_ENABLED, "false");
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverTwoDays(), true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                openTheDayAboveARoot(job);

                commit(row(4, 2, 35, "acct-1"), job);

                Assert.assertEquals(0, job.openSegmentKeyedPricedCountForTest());
                Assert.assertEquals(0, job.openSegmentKeyedUnpricedCountForTest());
                Assert.assertEquals(0, job.openSegmentKeyedCheaperCountForTest());
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAnUnindexedKeyCollectsNoDomainAndPricesNothing() throws Exception {
        // The route turns an unindexed view down at the decomposition rather than at the
        // pricing: the keyed scan it would take needs the posting index, so there is nothing
        // to price and no reason to pay for the wider walk that collects a domain.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverTwoDays(), false);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                openTheDayAboveARoot(job);

                commit(row(4, 2, 35, "acct-1"), job);

                Assert.assertEquals(0, job.openSegmentKeyedPricedCountForTest());
                Assert.assertEquals(0, job.openSegmentKeyedUnpricedCountForTest());
                Assert.assertEquals(0, job.openSegmentKeyedCheaperCountForTest());
                assertViewMatchesRecompute();
            }
        });
    }

    /**
     * Drops the in-memory view registry and rebuilds it, which is what makes the next
     * refresh restore its runtime from the checkpoint timeline rather than continue from
     * the state this process happens to be holding.
     */
    private void restartCycle() {
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
    }

    private void assertViewMatchesRecompute() throws Exception {
        final String bucket = "timestamp_floor('1d', created_at, '1970-01-01T00:00:00.000000Z'::timestamp)";
        final String recompute = "select created_at, cod_acct_no, "
                + "sum(amt_txn) over (partition by cod_acct_no, bucket order by created_at "
                + "rows between unbounded preceding and current row) as cumulative_sum "
                + "from (select created_at, cod_acct_no, amt_txn, " + bucket + " as bucket from tx)";
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + recompute + ") order by 2, 1",
                "(lv) order by 2, 1",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");
    }

    /**
     * Drives the open day forward in order, one commit per hour, so the cadence seals a
     * checkpoint root inside it. Without one the plan finds no boundary strictly below a
     * correction there, denies the resume and rebuilds from the view's own floor instead -
     * which is a different repair with a different executor and none of this route in it.
     * <p>
     * The hours matter as much as the roots: the base is partitioned by hour, so a resume
     * spanning several of them is what lets a key's postings be counted against a range
     * wider than the one partition the floor sits in.
     */
    private void openTheDayAboveARoot(LiveViewRefreshJob job) throws Exception {
        for (int hour = 0; hour < 10; hour++) {
            final StringBuilder rows = new StringBuilder();
            for (int account = 1; account <= 4; account++) {
                if (rows.length() > 0) {
                    rows.append(", ");
                }
                rows.append(row(4, hour, account * 10, "acct-" + account));
            }
            commit(rows.toString(), job);
        }
    }

    private void commit(String values, LiveViewRefreshJob job) throws Exception {
        execute("insert into tx values " + values);
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    private void createView(String seedRows, boolean isKeyIndexed) throws Exception {
        execute("create table tx (created_at timestamp, cod_acct_no symbol nocache"
                + (isKeyIndexed ? " index capacity 4" : "") + ", "
                + "amt_txn double) timestamp(created_at) partition by hour wal");
        execute("insert into tx values " + seedRows);
        drainWalQueue();
        execute("create live view lv flush every 100ms start from beginning as "
                + "select created_at, cod_acct_no, sum(amt_txn) over w as cumulative_sum "
                + "from tx window w as (partition by cod_acct_no order by created_at anchor daily '00:00')");
    }

    /**
     * One row of {@code account} at {@code hour}:{@code minute} on 2026-01-{@code day}, as
     * an INSERT tuple. With a daily anchor the day is also the segment.
     */
    private String row(int day, int hour, int minute, String account) {
        return "('2026-01-" + String.format("%02d", day) + "T" + String.format("%02d", hour)
                + ":" + String.format("%02d", minute) + ":00.000000Z', '" + account + "', 1.0)";
    }

    /**
     * Ten rows of each of four accounts on each of 2026-01-02 and 2026-01-03, one per hour.
     * 2026-01-04 is left to {@link #openTheDayAboveARoot}, which drives it in order: it is
     * the open segment once the view has caught up, so a correction inside it is the shape
     * every route built for a closed segment declines.
     */
    private String seedFourAccountsOverTwoDays() {
        final StringBuilder rows = new StringBuilder();
        for (int day = 2; day <= 3; day++) {
            for (int hour = 0; hour < 10; hour++) {
                for (int account = 1; account <= 4; account++) {
                    if (rows.length() > 0) {
                        rows.append(", ");
                    }
                    rows.append(row(day, hour, account * 10, "acct-" + account));
                }
            }
        }
        return rows.toString();
    }
}
